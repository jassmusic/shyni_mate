import json
import threading
import time

import requests

from .model_scan import ModelScanItem
from .setup import *
from .task_scan import Task
from .task_trash_view import TaskTrashView
from .task_trash_selected import TaskTrashSelected
from .extensions import BrowserPage, vfs_refresh
from .shyni_backend import ShyniBackend

name = 'scan'
# 진행현황 수동 새로고침의 직접 계산을 프로세스당 10초에 한 번으로 제한하는 기록
_last_ondemand_summary = 0

class ModuleScan(PluginModuleBase):
    scan_queue = None
    scan_thread = None
    file_exist_thread = None
    current_scan_count = 0

    def __init__(self, P):
        super(ModuleScan, self).__init__(P, name=name, first_menu='setting')
        self.db_default = {
            f"{self.name}_db_version": "1",
            f"{self.name}_max_scan_count": "2",
            #f"{self.name}_incompleted_rescan": "False",
            f"{self.name}_max_wait_time": "5",
            f"{self.name}_probe_timeout_sec": "60",
            f"{self.name}_analyze_wait_min": "5",
            f"{self.name}_analyze_fail_min": "60",
            # 진행 중 표시 고착 정리 — 작업 번호를 받지 못했거나 비우기 도중
            # 재시작되는 등 확인할 방법이 없을 때 이 시간이 지나면 '중단됨'으로
            # 정리한다. 0이면 끈다 (값 읽기는 대시보드 쪽에서 한다)
            f"{self.name}_stuck_timeout_min": "30",
            # 시간 제한 탭(스캔 설정)의 입력 칸 — 키는 고급 설정 시절의 base_ 이름을
            # 그대로 쓴다 (DB 값과 shyni_backend.timeout_sec이 이 키를 읽는다)
            "base_timeout_query": "15",
            "base_timeout_send": "30",
            "base_timeout_poll": "15",
            "base_timeout_callback": "10",
            "base_timeout_vfs_rc": "600",
            f"{self.name}_manual_target": "",
            f"{self.name}_filecheck_thread_interval": "60",
            f"{self.name}_db_delete_day": "30",
            f"{self.name}_db_auto_delete": "True",
            f"{self.name}_use_vfs_refresh": "False",
            f"{self.name}_vfs_change_rule": "/mnt/gds2/GDRIVE|/GDRIVE|172.17.0.1:5574",
            f"{self.name}_shyni_use": "True",
            f"{self.name}_io_yield_use": "True",
            f"{self.name}_shyni_path_rule": "",
            f"{self.name}_trash_schedule_anchors": "",
        }
        self.web_list_model = ModelScanItem
        self.set_page_list([BrowserPage])

    def migration(self):
        # scan_item에 샤이니 대상 상태 컬럼 보장 — 실제 컬럼 존재로 판단(멱등).
        # binds URL 문자열 파싱 금지(웹훅 migration에서 상대경로 오해석 전례).
        try:
            from sqlalchemy import inspect, text
            with F.app.app_context():
                engine = F.db.engines[P.package_name] if hasattr(F.db, 'engines') else F.db.get_engine(F.app, bind=P.package_name)
                insp = inspect(engine)
                if 'scan_item' in insp.get_table_names():
                    cols = [c['name'] for c in insp.get_columns('scan_item')]
                    with engine.begin() as conn:
                        if 'shyni_status' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD shyni_status VARCHAR'))
                        if 'shyni_job_id' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD shyni_job_id VARCHAR'))
                        if 'shyni_started_at' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD shyni_started_at DATETIME'))
                        if 'send_attempt' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD send_attempt INTEGER'))
                            # 기존 행은 지금까지의 파일체크 횟수를 초기값으로 심는다 —
                            # 예전 키(파일체크 횟수 기반)와 다음 전송 키가 겹치지 않게 하기 위함.
                            conn.execute(text('UPDATE scan_item SET send_attempt = COALESCE(filecheck_count, 0) WHERE send_attempt IS NULL'))
                        if 'vfs_done' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD vfs_done BOOLEAN'))
                        if 'vfs_done_time' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD vfs_done_time DATETIME'))
                        if 'reset_done' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD reset_done BOOLEAN'))
                        if 'reset_time' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD reset_time DATETIME'))
                        if 'in_library' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD in_library BOOLEAN'))
                        if 'shyni_phase' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD shyni_phase VARCHAR'))
                        if 'probe_received' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD probe_received INTEGER'))
                        if 'probe_expected' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD probe_expected INTEGER'))
                        if 'held' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD held BOOLEAN'))
                        if 'fail_reason' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD fail_reason VARCHAR'))
                        if 'send_error_count' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD send_error_count INTEGER'))
                        if 'shyni_late_result' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD shyni_late_result VARCHAR'))
                        if 'shyni_late_finished_at' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD shyni_late_finished_at DATETIME'))
                        if 'shyni_late_checked_at' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD shyni_late_checked_at DATETIME'))
                    if 'shyni_status' not in cols:
                        P.logger.info('[Scan] migration: scan_item.shyni_status/shyni_job_id 컬럼 추가')
        except Exception as e:
            P.logger.error(f'[Scan] migration 오류: {e}')


    def process_menu(self, sub, req):
        if sub == 'trash':
            arg = P.ModelSetting.to_dict()
            try:
                from .mod_status import ModuleStatus
                sections, _ = ModuleStatus._sections()
                arg['library_list'] = sections or []
            except Exception:
                arg['library_list'] = []
            # 현황 카드의 '휴지통 상세'로 넘어오면 그 라이브러리를 미리 골라 둔다
            try:
                arg['trash_preselect'] = (req.args.get('key') or '') if req is not None else ''
            except Exception:
                arg['trash_preselect'] = ''
            # 캐시 현황과 휴지통 개수는 첫 화면부터 바로 보이게 페이지에
            # 함께 실어 보낸다. 서버 응답을 기다리면 표시가 늦어지고,
            # 그 사이에 캐시가 있는 줄 모르고 조회를 눌러 버릴 수 있다.
            # 둘 다 저장된 값을 읽는 것이라 페이지 표시를 늦추지 않는다.
            try:
                from .task_trash_view import cache_times
                arg['trash_cache_times'] = cache_times()
            except Exception:
                arg['trash_cache_times'] = {}
            try:
                from .task_status import read_trash_cache
                counts_cache = read_trash_cache() or {}
                arg['trash_counts_init'] = counts_cache.get('data') or {}
            except Exception:
                arg['trash_counts_init'] = {}
            return render_template(f'{P.package_name}_{self.name}_trash.html', arg=arg)
        return super().process_menu(sub, req)


    def process_command(self, command, arg1, arg2, arg3, req):
        ret = {'ret':'success'}
        if command == 'locate_item':
            # 진행현황에서 번호를 눌렀을 때 그 항목이 현재 목록 조건에서 몇
            # 페이지에 있는지 알려 준다 — 건수 조회 두 번뿐인 가벼운 명령이다.
            try:
                item_id = int(arg1)
                try:
                    ctx = json.loads(arg2 or '{}')
                except Exception:
                    ctx = {}
                order = ctx.get('order') or 'desc'
                query = P.ModelScanItem.make_query(
                    None, order=order, search=ctx.get('search') or '',
                    option1=ctx.get('option1') or 'all',
                    option2=ctx.get('option2') or 'all',
                    option3=ctx.get('option3') or 'all')
                if not query.filter(P.ModelScanItem.id == item_id).count():
                    return jsonify({'ret': 'warning', 'msg': '상태가 변경되어 해당 항목을 찾지 못했습니다.'})
                if order == 'asc':
                    above = query.filter(P.ModelScanItem.id < item_id).count()
                else:
                    above = query.filter(P.ModelScanItem.id > item_id).count()
                return jsonify({'ret': 'success', 'page': above // 30 + 1})
            except Exception as e:
                logger.error(f'[Scan] 항목 위치 조회 실패: {e}')
                return jsonify({'ret': 'warning'})
        if command == 'summary':
            try:
                # 진행현황 집계는 파일체크 워커가 주기마다 미리 계산해 둔 캐시를
                # 읽는 것이 기본이다. 웹 프로세스가 분류 쿼리를 직접 돌리면
                # 새로고침할 때마다 DB 경합이 생겨 화면 전체가 느려졌다
                # (2026-11-11). 캐시가 아직 없을 때만(재시작 직후 등) 한 번
                # 직접 계산해 캐시에 남긴다.
                summary = None
                raw_cache = P.ModelSetting.get('scan_summary_cache')
                if raw_cache:
                    try:
                        summary = json.loads(raw_cache)
                    except Exception:
                        summary = None
                # 수동 새로고침(arg1='force')인데 캐시가 15초보다 오래됐으면,
                # 그때만 웹이 직접 한 번 계산해 바로 보여 준다 (캐시에 쓰지는
                # 않는다 — 저장은 워커 몫이다). 버튼을 연달아 눌렀을 때를
                # 대비해 직접 계산은 프로세스당 10초에 한 번까지만 허용하고,
                # 계산이 실패하면 캐시 값을 그대로 쓴다.
                if arg1 == 'force':
                    now_ts = time.time()
                    cache_old = summary is None or (now_ts - float(summary.get('ts') or 0)) > 15
                    global _last_ondemand_summary
                    if cache_old and now_ts - _last_ondemand_summary >= 10:
                        try:
                            fresh = ModelScanItem.summary_counts()
                            fresh['ts'] = now_ts
                            _last_ondemand_summary = now_ts
                            summary = fresh
                        except Exception as e:
                            logger.error(f'[Scan] 진행현황 직접 계산 실패(캐시 사용): {e}')
                if summary is None:
                    summary = ModelScanItem.summary_counts()
                    summary['ts'] = time.time()
                    try:
                        P.ModelSetting.set('scan_summary_cache', json.dumps(summary))
                    except Exception as e:
                        logger.warning(f'[Scan] 진행현황 캐시 저장 실패: {e}')
                summary = dict(summary)
                summary['shyni_use'] = P.ModelSetting.get_bool('scan_shyni_use')
                ret['summary'] = summary
            except Exception as e:
                # 30초마다 폴링하므로 실패해도 알림을 띄우지 않는다 —
                # 화면은 마지막 숫자를 유지하고 (N초 전) 표시가 오래됐음을 알린다
                logger.error(f'[Scan] 진행현황 집계 실패: {e}')
                ret = {'ret': 'fail'}
        elif command == 'trash_counts':
            from . import db_support
            if not db_support.db_ready():
                return jsonify({'ret': 'fail', 'msg': 'DB 연결이 준비되지 않았습니다.'})
            try:
                # 현황·건강도와 같은 개수 캐시를 쓴다 — 라이브러리 선택
                # 팝업을 열 때마다 무거운 전체 읽기를 돌리지 않기 위해서다
                from .task_status import trash_counts_for_stats
                ret['data'] = trash_counts_for_stats()
            except Exception as e:
                return jsonify({'ret': 'fail', 'msg': f'휴지통 수를 읽지 못했습니다: {e}'})
        elif command == 'trash_query':
            # 조회 계산은 워커(task_trash_view)가 수행한다 — 웹은 요청만 남기고
            # 화면은 trash_state로 진행률을 폴링한다.
            from . import db_support
            if not db_support.db_ready():
                return jsonify({'ret': 'fail',
                                'msg': '휴지통 조회는 Relay에서 연결 테스트를 통과해야 사용할 수 있습니다.'})
            try:
                lib_id = int(arg1)
            except Exception:
                return jsonify({'ret': 'warning', 'msg': '라이브러리를 선택해 주세요.'})
            from .task_trash_view import request_view
            ret['data'] = request_view(lib_id, force=(arg2 == '1'))
        elif command == 'trash_query_all':
            # 전체 조회 접수 — 계산은 워커가 라이브러리 단위로 수행한다
            from . import db_support
            if not db_support.db_ready():
                return jsonify({'ret': 'fail',
                                'msg': '휴지통 조회는 Relay에서 연결 테스트를 통과해야 사용할 수 있습니다.'})
            try:
                lib_ids = json.loads(arg1 or '[]')
            except Exception:
                lib_ids = []
            from .task_trash_view import request_bulk
            res = request_bulk(lib_ids)
            if res.get('ret') == 'fail':
                return jsonify(res)
            ret['data'] = res
        elif command == 'trash_query_all_cancel':
            from .task_trash_view import cancel_bulk
            cancel_bulk()
        elif command == 'trash_query_cancel':
            from .task_trash_view import cancel_query
            cancel_query()
        elif command == 'trash_state':
            from .task_trash_view import cache_libs, cache_times, read_state, read_bulk
            ret['data'] = {'state': read_state(),
                           'cache_libs': cache_libs(),
                           'cache_times': cache_times(),
                           'bulk': read_bulk()}
        elif command == 'trash_list':
            # 워커가 계산해 둔 캐시를 검색·정렬·페이지 단위로 잘라 돌려준다.
            from . import db_support
            if not db_support.db_ready():
                return jsonify({'ret': 'fail',
                                'msg': '휴지통 조회는 Relay에서 연결 테스트를 통과해야 사용할 수 있습니다.'})
            try:
                lib_id = int(arg1)
            except Exception:
                return jsonify({'ret': 'warning', 'msg': '라이브러리를 선택해 주세요.'})
            try:
                opts = json.loads(arg2 or '{}')
                if not isinstance(opts, dict):
                    opts = {}
            except Exception:
                opts = {}
            from .task_trash_view import cached_data, page_from_cache
            data = cached_data(lib_id)
            if not data:
                return jsonify({'ret': 'fail',
                                'msg': '조회된 결과가 없습니다. 조회 버튼을 눌러 주세요.'})
            try:
                ret['data'] = page_from_cache(
                    data,
                    page=int(opts.get('page') or 1),
                    search=(opts.get('search') or '').strip(),
                    order=opts.get('order') or 'old')
            except Exception as e:
                logger.error(f'[휴지통] 조회 결과 표시 실패: {e}')
                return jsonify({'ret': 'fail', 'msg': f'휴지통을 표시하지 못했습니다: {e}'})
        elif command == 'trash_files':
            from . import db_support
            if not db_support.db_ready():
                return jsonify({'ret': 'fail', 'msg': 'DB 연결이 준비되지 않았습니다.'})
            try:
                lib_id = int(arg1)
                root_id = int(arg2)
            except Exception:
                return jsonify({'ret': 'warning', 'msg': '항목이 올바르지 않습니다.'})
            from .task_trash_view import files_for
            files = files_for(lib_id, root_id)
            if files is None:
                # 캐시에 없으면 웹에서 느린 조회를 하지 않는다 — 화면이 멈춘
                # 느낌을 주지 않도록 다시 조회하도록 안내한다.
                return jsonify({'ret': 'fail',
                                'msg': '조회 결과가 바뀌었습니다. 조회 버튼을 눌러 다시 불러와 주세요.'})
            ret['data'] = {'files': files[:500], 'total': len(files)}
        elif command == 'trash_empty_all':
            try:
                lib_id = int(arg1)
            except Exception:
                return jsonify({'ret': 'warning', 'msg': '라이브러리를 선택해 주세요.'})
            url, token = ShyniBackend.get_config()
            if not url or not token:
                return jsonify({'ret': 'fail', 'msg': '기본 설정에서 shyni 주소와 토큰을 먼저 설정해 주세요.'})
            try:
                res = requests.get(f'{url}/library/sections/{lib_id}/emptyTrash',
                                   headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                                   timeout=600)
                res.raise_for_status()
            except Exception as e:
                return jsonify({'ret': 'fail', 'msg': f'휴지통 비우기에 실패했습니다: {e}'})
            logger.info(f'[휴지통] 전체 비우기: 라이브러리 {lib_id}')
            try:
                from .task_trash_view import clear_cache
                clear_cache(lib_id)
            except Exception as e:
                logger.warning(f'[휴지통] 전체 비우기 후 상세 캐시 지우기 실패: {e}')
            try:
                # 전부 비웠으므로 이 라이브러리 개수는 0이 확정이다 —
                # 개수 캐시에도 바로 반영해 현황·선택 칸이 옛 값을 보지 않게 한다
                from .task_status import set_trash_count
                set_trash_count(lib_id, 0)
            except Exception as e:
                logger.warning(f'[휴지통] 전체 비우기 후 개수 반영 실패: {e}')
            try:
                from .task_status import request_stats
                request_stats()
            except Exception as e:
                logger.warning(f'[휴지통] 전체 비우기 후 통계 재계산 요청 실패: {e}')
            ret['msg'] = '휴지통을 비웠습니다.'
        elif command == 'trash_selected_action':
            # 선택한 작품 폴더에 새로고침·스캔·새로고침 후 스캔을 실행한다.
            # 실행은 워커(task_trash_selected)가 전담하고 웹은 요청만 남긴다 —
            # 웹 스레드로 돌리면 ff 재시작 때 실행이 끊기고 상태도 사라진다.
            # 선택 비우기는 관리자 세션이 필요해 이 범위에 넣지 않는다.
            try:
                lib_id = int(arg1)
            except Exception:
                return jsonify({'ret': 'warning', 'msg': '라이브러리를 선택해 주세요.'})
            try:
                payload = json.loads(arg2 or '{}')
                if not isinstance(payload, dict):
                    payload = {}
            except Exception:
                payload = {}
            mode = (payload.get('mode') or '').strip()
            folders = payload.get('folders')
            from .task_trash_selected import (MAX_FOLDERS,
                                               request_selected_action,
                                               validate_request)
            _, verr = validate_request(lib_id, mode, folders)
            if verr:
                return jsonify({'ret': 'warning', 'msg': verr})
            # 대상 폴더는 지금 조회된(캐시된) 작품의 공통 폴더여야 한다 —
            # 화면 밖의 임의 경로로 새로고침·스캔이 나가지 않게 한다.
            from .task_trash_view import cached_data
            data = cached_data(lib_id)
            if not data:
                return jsonify({'ret': 'fail',
                                'msg': '조회된 결과가 없습니다. 조회 버튼을 눌러 주세요.'})
            allowed = {g.get('folder') for g in (data.get('groups') or [])
                       if g.get('folder')}
            folders, _ = validate_request(lib_id, mode, folders)
            unknown = [f for f in folders if f not in allowed]
            if unknown:
                return jsonify({'ret': 'warning',
                                'msg': '조회 결과에 없는 항목이 섞여 있습니다. 조회 버튼을 눌러 다시 불러온 뒤 실행해 주세요.'})
            try:
                state = request_selected_action(lib_id, mode, folders)
            except ValueError as e:
                return jsonify({'ret': 'warning', 'msg': str(e)})
            if state is None:
                return jsonify({'ret': 'warning',
                                'msg': '이미 실행 중인 선택 작업이 있습니다. 끝난 뒤에 다시 시도해 주세요.'})
            logger.info(f'[휴지통] 선택 실행 요청: 라이브러리 {lib_id} '
                        f'동작 {mode} 폴더 {len(folders)}개 (최대 {MAX_FOLDERS}개)')
            ret['data'] = state
            ret['msg'] = '선택한 항목의 실행을 시작했습니다.'
        elif command == 'trash_selected_state':
            from .task_trash_selected import read_sel_state
            ret['data'] = read_sel_state()
        elif command == 'manual':
            if not (arg2 or '').strip():
                return jsonify({'ret': 'warning', 'msg': '스캔할 파일이나 폴더 경로를 입력해 주세요.'})
            P.ModelSetting.set(f"{self.name}_manual_target", arg2)
            ModelScanItem(arg2, mode=arg1).save()
            logger.info(f'[Scan] 수동 추가: mode={arg1} target={arg2}')
            ret['msg'] = "추가하였습니다."
        elif command == 'reset_items':
            ids = [x.strip() for x in (arg1 or '').split(',') if x.strip()]
            done, skipped = 0, 0
            for id_str in ids:
                try:
                    item = ModelScanItem.get_by_id(int(id_str))
                except Exception:
                    item = None
                if item is None:
                    continue
                if item.shyni_status == 'RUNNING':
                    skipped += 1
                    continue
                # 생성 직후 상태로 되돌린다 — filecheck가 처음부터 다시 처리한다
                item.shyni_status = None
                item.in_library = None
                item.shyni_job_id = None
                item.shyni_started_at = None
                item.shyni_phase = None
                item.probe_received = None
                item.probe_expected = None
                item.section_id = None
                item.section_type = None
                item.meta_info = None
                item.vfs_done = None
                item.vfs_done_time = None
                item.filecheck_time = None
                item.filecheck_count = 0
                item.process_start_time = None
                item.process_finish_time = None
                item.process_duration = None
                item.completed_time = None
                item.flag_filecheck = False
                item.flag_last_filecheck = False
                item.flag_db_include = False
                item.flag_cancel = False
                item.flag_finish = False
                item.scan_folder = None
                item.held = False
                item.fail_reason = None
                item.send_error_count = 0
                item.shyni_late_result = None
                item.shyni_late_finished_at = None
                item.shyni_late_checked_at = None
                item.reset_done = True
                item.reset_time = datetime.now()
                item.set_status('READY', save=True)
                done += 1
            logger.info(f'[Scan] 항목 리셋: {done}개 다시 시작 (요청 ids={ids}, 실행 중 건너뜀 {skipped}개)')
            ret['msg'] = f'{done}개 항목을 초기화했습니다. 처음부터 다시 시작합니다.'
            if skipped:
                ret['msg'] += f' (실행 중이라 건너뜀 {skipped}개)'
        elif command == 'retry_scan':
            item = ModelScanItem.get_by_id(arg1)
            if item is None:
                ret = {'ret': 'warning', 'msg': '항목을 찾지 못했습니다.'}
            else:
                if item.mode == 'REFRESH':
                    # 이전 404/실패 결과가 재요청을 가로막지 않도록 초기화한다.
                    # 이미 실행 중인 샤이니 job은 중복 발사하지 않는다.
                    item.section_id = None
                    item.section_type = None
                    if item.shyni_status != 'RUNNING':
                        item.shyni_status = None
                        item.shyni_job_id = None
                        item.shyni_started_at = None
                if item.shyni_status != 'RUNNING':
                    # 전송 오류 횟수와 뒤늦은 결과 기록은 새 시도와 함께 지운다
                    item.fail_reason = None
                    item.send_error_count = 0
                    item.shyni_late_result = None
                    item.shyni_late_finished_at = None
                    item.shyni_late_checked_at = None
                item.set_status('READY', save=True)
                logger.info(f'[Scan] 재시도(READY로 변경): item={arg1} mode={item.mode}')
                ret['msg'] = 'READY 상태로 변경했습니다.'
        elif command == 'release_hold':
            ids = [int(x) for x in (arg1 or '').split(',') if x.strip().isdigit()]
            done = 0
            for item_id in ids:
                try:
                    item = ModelScanItem.get_by_id(item_id)
                except Exception:
                    item = None
                if item is not None and getattr(item, 'held', None):
                    item.held = False
                    item.save()
                    done += 1
            logger.info(f'[Scan] 보류 해제: {done}개 (요청 ids={ids})')
            ret['msg'] = f'{done}개 항목의 보류를 해제했습니다. 파일체크 주기에 처리가 시작됩니다.'
        elif command == 'release_hold_all':
            done = ModelScanItem.release_all_held()
            logger.info(f'[Scan] 전체 보류 해제: {done}개')
            ret['msg'] = f'보류 중인 {done}개 항목을 전부 해제했습니다. 파일체크 주기에 처리가 시작됩니다.'
        elif command == 'meta_load_items':
            ids = [int(x) for x in (arg1 or '').split(',') if x.strip().isdigit()]
            if not ids:
                ret = {'ret': 'warning', 'msg': '선택된 항목이 없습니다.'}
            else:
                t = threading.Thread(target=ShyniBackend.load_meta_many, args=(ids,))
                t.daemon = True
                t.start()
                logger.info(f'[Scan] 선택 항목 메타 불러오기 시작: {len(ids)}개 ids={ids}')
                ret['msg'] = f'{len(ids)}개 항목의 메타 정보를 불러오는 중입니다. 잠시 뒤 목록에 표시됩니다.'
        elif command == 'meta_refresh_items':
            ids = [int(x) for x in (arg1 or '').split(',') if x.strip().isdigit()]
            if not ids:
                ret = {'ret': 'warning', 'msg': '선택된 항목이 없습니다.'}
            else:
                t = threading.Thread(target=ShyniBackend.meta_refresh_many, args=(ids,))
                t.daemon = True
                t.start()
                logger.info(f'[Scan] 선택 항목 메타 새로고침 시작: {len(ids)}개 ids={ids}')
                ret['msg'] = f'{len(ids)}개 항목의 메타 새로고침을 시작했습니다. 반영되면 미디어 정보가 갱신됩니다.'
        elif command == 'meta_load':
            item = ModelScanItem.get_by_id(arg1)
            if item is None:
                ret = {'ret': 'warning', 'msg': '항목을 찾지 못했습니다.'}
            else:
                t = threading.Thread(target=ShyniBackend.load_meta_now, args=(item.id,))
                t.daemon = True
                t.start()
                logger.info(f'[Scan] 라이브러리 메타 불러오기(수동) 시작: item={item.id} target={item.target}')
                ret['msg'] = '라이브러리 메타 정보를 불러오는 중입니다. 잠시 뒤 목록에 표시됩니다.'
        elif command == 'meta_refresh':
            item = ModelScanItem.get_by_id(arg1)
            if item is None:
                ret = {'ret': 'warning', 'msg': '항목을 찾지 못했습니다.'}
            else:
                t = threading.Thread(target=ShyniBackend.meta_refresh_now, args=(item.id,))
                t.daemon = True
                t.start()
                logger.info(f'[Scan] 메타 새로고침(수동) 시작: item={item.id} target={item.target}')
                ret['msg'] = '메타 새로고침을 요청했습니다. 반영되면 미디어 정보(이미지 포함)가 갱신됩니다.'
        return jsonify(ret)


    def process_api(self, sub, req):
        ret = {'ret':'success'}
        if sub == 'do_scan':
            target = req.form.get('target')
            if not target:
                return jsonify({'ret': 'fail', 'msg': '스캔할 대상(target)이 없습니다.'})
            target_section_id = req.form.get('target_section_id') or 0
            mode = req.form.get('mode') or 'ADD'
            callback_id = req.form.get('callback_id')
            callback_url = req.form.get('callback_url')
            ModelScanItem(
                target,
                mode = mode,
                target_section_id = target_section_id,
                callback_id = callback_id,
                callback_url = callback_url,
            ).save()
            logger.info(f'[Scan] 수신 등록: mode={mode} target={target} callback={callback_id}')
            ret['msg'] = f'{mode=} {target=}'
        elif sub == 'manual_refresh':
            meta_id = req.form.get('metadata_item_id')
            if not meta_id:
                return jsonify({'ret': 'fail', 'msg': '해당 항목을 찾지 못했습니다.'})
            url, token = ShyniBackend.get_config()
            if not url or not token:
                return jsonify({'ret': 'fail', 'msg': '기본 설정에서 shyni 주소와 토큰을 먼저 설정해 주세요.'})
            try:
                res = requests.put(f'{url}/library/metadata/{meta_id}/refresh',
                                   headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                                   timeout=30)
                res.raise_for_status()
                logger.info(f'[Scan] 메타데이터 새로고침 요청: metadata_id={meta_id}')
                ret = {'ret':'success', 'msg': f'{meta_id} 새로고침을 요청했습니다.'}
            except Exception as e:
                logger.error(f'[Scan] 메타데이터 새로고침 실패: metadata_id={meta_id} {e}')
                ret = {'ret': 'fail', 'msg': f'새로고침 요청에 실패했습니다: {e}'}
        elif sub == 'vfs_refresh':
            target = req.form.get('target')
            recursive = (req.form.get('recursive') == 'true') or False
            async_ = (req.form.get('async') == 'true') or False
            try:
                result = vfs_refresh(target, recursive, async_)
            except Exception as e:
                logger.error(f'[Scan] VFS 새로고침 실패: {target} {e}')
                return jsonify({'ret': 'fail', 'msg': f'VFS 새로고침에 실패했습니다: {e}'})
            ret.update(result)
            ret.update({
                'recursive': recursive,
                'async': async_,
                'target': target,
            })
        else:
            return {'ret':'fail', 'msg':'Bad request'}, 400
        return jsonify(ret)


    def plugin_load(self):
        def func():
            self.start_celery(Task.start)
            self.start_celery(TaskTrashView.start)
            self.start_celery(TaskTrashSelected.start)
        thread = threading.Thread(target=func, args=())
        thread.daemon = True
        thread.start()


    def setting_save_after(self, changes: list) -> None:
        '''override'''
        super().setting_save_after(changes)
        for page in self.page_list:
            page.setting_save_after(changes)
