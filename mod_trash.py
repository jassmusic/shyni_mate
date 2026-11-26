import json
import threading

import requests

from .model_trash import ModelTrashItem
from .mod_status import ModuleStatus
from .setup import *
from .shyni_backend import ShyniBackend
from .task_trash_view import TaskTrashView
from .task_trash_selected import TaskTrashSelected
from .task_periodic import annotate_total_labels, current_scan_progress
from .task_trash import (ACTION_LABEL, LIBRARY_BUSY_MESSAGE, TaskTrash,
                         _record_trash_start_error, _same_job_busy,
                         _same_library_busy, _trash_cancel_requested,
                         clear_trash_start_error, fail_trash_start,
                         get_last_trash_start_error, get_trash_currents,
                         get_trash_jobs, request_trash_cancel,
                         update_trash_current_job_idx)

name = 'trash'
logger = P.logger

# 라이브러리 선택 목록 정렬 — 주기적 스캔·휴지통 스캔이 같은 설정을 쓴다.
PICK_SORTS = ('default_asc', 'default_desc', 'name_asc', 'name_desc')
PICK_SORT_KEY = 'jobs_lib_pick_sort'
# 휴지통 조회 페이지 라이브러리 선택 팝업 정렬 (jobs 페이지와 별개)
TRASH_PICK_SORTS = ('default', 'name', 'trash_desc')
TRASH_PICK_SORT_KEY = 'trash_lib_pick_sort'
# 휴지통 조회 페이지 전체 현황 정렬 (선택 팝업과 별개, 기본은 휴지통 수 내림차순)
TRASH_OVERVIEW_SORTS = ('default', 'name', 'trash_desc')
TRASH_OVERVIEW_SORT_KEY = 'trash_overview_sort'
# 휴지통 조회 페이지 전체 현황 표시 선택 — 숨긴 라이브러리 키(쉼표 구분,
# 메타·대시보드·건강도와 별개). 메타의 meta_overview_hidden_libs와 같은 방식.
TRASH_OVERVIEW_HIDDEN_LIBS_KEY = 'trash_overview_hidden_libs'


def _parse_empty_stats(data):
    """비우기 응답에서 (삭제 수, 보관 기간 남아 남은 수)를 읽는다.
    관리자 API는 평평한 dict(removed_parts·retained_parts가 최상위),
    compat API는 stats 안에 넣어 주는 경우도 있어 양쪽을 본다.
    못 읽으면 None."""
    if not isinstance(data, dict):
        return None, None
    stats = data.get('stats')
    if not isinstance(stats, dict):
        stats = {}

    def _int(v):
        try:
            return int(v) if v is not None else None
        except Exception:
            return None
    removed = _int(stats.get('removed_parts'))
    if removed is None:
        removed = _int(data.get('removed_parts'))
    retained = _int(stats.get('retained_parts'))
    if retained is None:
        retained = _int(data.get('retained_parts'))
    return removed, retained


def _empty_result_message(removed, retained):
    """비우기 결과 문구 — 'N개 삭제, M개는 보관 기간이 남아 남음'
    (사용자 지정 형식, 2026-11-18). 보관 기간이 남아 남은 항목이
    없으면 삭제 수만, 수를 못 읽었으면 기본 문구를 쓴다."""
    if removed is None:
        return '휴지통을 비웠습니다.'
    if retained:
        return f'{removed}개 삭제, {retained}개는 보관 기간이 남아 남음'
    return f'{removed}개를 삭제했습니다.'


class ModuleTrash(PluginModuleBase):
    """휴지통 주기 작업 — 라이브러리와 동작(새로고침/스캔/비우기 조합)을 골라
    cron으로 등록해 두면 정해진 시각에 하나씩 순차로 실행한다.
    주기적 스캔과 같은 작업 목록 방식이다."""

    def __init__(self, P):
        super(ModuleTrash, self).__init__(P, name=name, first_menu='setting')
        self.db_default = {
            f'{self.name}_db_version': '1',
            f'{self.name}_jobs': '[]',
            f'{self.name}_db_delete_day': '30',
            f'{self.name}_db_auto_delete': 'True',
            # 진행 중 표시 정리 시간(분) — 진행 상태를 확인할 방법이 없을 때
            # 이 시간이 지나면 '중단됨'으로 정리한다. 0이면 끈다 (기본값 30)
            f'{self.name}_stuck_timeout_min': '30',
            # VFS 새로고침 제한 시간(분) — 폴더 새로고침이 끝나기를
            # 이 시간까지 기다린다. 넘기면 그 폴더는 실패로 끝나고
            # 스캔·비우기에서 빠진다. 0 이하면 기본값으로 동작한다 (기본값 360)
            f'{self.name}_vfs_timeout_min': '360',
            # VFS 비동기 사용 — 켜면(기본값) 새로고침 작업을 걸어 두고
            # 완료를 확인하면서 기다리고, 끄면 새로고침이 끝날 때까지
            # 기다리는 동기 방식으로 진행한다.
            f'{self.name}_vfs_use_async': 'True',
            # 스캔·비우기 대기 기한(분) — shyni job이 끝나기를 이 시간까지
            # 기다린다. 기한이 됐을 때 shyni에서 job이 아직 진행 중이면
            # 이 시간만큼 기한을 연장하며 계속 기다린다. 0 이하면 기본값으로
            # 동작한다 (기본값 360)
            f'{self.name}_scan_timeout_min': '360',
            # 작업 대기 시간(분) — 앞선 작업이 돌고 있을 때 이 시간까지
            # 기다렸다가 끝나면 이어서 실행한다. 0이면 기다리지 않는다 (기본값 360)
            f'{self.name}_max_wait_min': '360',
            # 휴지통 조회 페이지 라이브러리 선택 팝업 정렬
            # (jobs 페이지의 jobs_lib_pick_sort와 별개 키다)
            f'{self.name}_lib_pick_sort': 'default',
            # 휴지통 조회 페이지 전체 현황 정렬 (기본은 휴지통 수 내림차순)
            f'{self.name}_overview_sort': 'trash_desc',
            # 휴지통 조회 페이지 전체 현황 표시 선택 — 숨긴 라이브러리
            # 키(쉼표 구분, 기본은 전체 표시)
            f'{self.name}_overview_hidden_libs': '',
        }
        self.web_list_model = ModelTrashItem

    def migration(self):
        # trash_item에 작업 결과 컬럼 보장 — 실제 컬럼 존재로 판단(멱등).
        # mod_scan.py의 migration과 같은 패턴이다.
        try:
            from sqlalchemy import inspect, text
            with F.app.app_context():
                engine = F.db.engines[P.package_name] if hasattr(F.db, 'engines') else F.db.get_engine(F.app, bind=P.package_name)
                insp = inspect(engine)
                if 'trash_item' in insp.get_table_names():
                    cols = [c['name'] for c in insp.get_columns('trash_item')]
                    new_cols = [
                        ('action', 'VARCHAR'),
                        ('shyni_job_id', 'VARCHAR'), ('stat_total', 'INTEGER'),
                        ('stat_new', 'INTEGER'), ('stat_missing', 'INTEGER'),
                        ('stat_updated', 'INTEGER'), ('stat_restored', 'INTEGER'),
                        ('stat_registered', 'INTEGER'), ('stat_failed', 'INTEGER'),
                        ('vfs_duration', 'INTEGER'), ('scan_duration', 'INTEGER'),
                        ('append_files', 'TEXT'), ('append_count', 'INTEGER'),
                        ('stat_folders', 'TEXT'), ('emptied_count', 'INTEGER'),
                        ('emptied_files', 'TEXT'),
                        ('retained_count', 'INTEGER'),
                        ('retained_files', 'TEXT'),
                        ('target_files', 'TEXT'), ('restored_files', 'TEXT'),
                        ('vfs_results', 'TEXT'),
                        ('scan_results', 'TEXT'),
                        ('file_exists', 'TEXT'),
                        ('wait_seconds', 'INTEGER'),
                    ]
                    added = [c for c, _ in new_cols if c not in cols]
                    if added:
                        with engine.begin() as conn:
                            for col, typ in new_cols:
                                if col not in cols:
                                    conn.execute(text(f'ALTER TABLE trash_item ADD {col} {typ}'))
                        P.logger.info(f'[Trash] migration: trash_item 컬럼 추가 — {added}')
        except Exception as e:
            P.logger.error(f'[Trash] migration 오류: {e}')

    def plugin_load(self):
        self.migration()
        ModelTrashItem.set_terminated()
        P.ModelSetting.set('trash_current', '')
        P.ModelSetting.set('trash_cancel', '')
        self.start()
        # 휴지통 조회 워커 — 조회 페이지가 이 모듈로 이동하면서
        # 워커 기동도 함께 옮김 (스캔 메뉴에서 이동, 2026-11-18)
        def _start_trash_workers():
            self.start_celery(TaskTrashView.start)
            self.start_celery(TaskTrashSelected.start)
        thread = threading.Thread(target=_start_trash_workers, args=())
        thread.daemon = True
        thread.start()

    # ---------- 스케줄러 ----------
    def start(self):
        for idx, item in enumerate(get_trash_jobs()):
            if item.get('use'):
                self.sched_add(idx, item=item)

    def sched_add(self, idx, item=None):
        try:
            if item is None:
                item = get_trash_jobs()[idx]
            if F.scheduler.is_include(item['job_id']):
                return True
            job = Job(self.P.package_name, item['job_id'], item['cron'],
                      self.job_function, item.get('name') or f"작업 {idx + 1}",
                      args=(idx,))
            F.scheduler.add_job_instance(job)
            return True
        except Exception as e:
            logger.error(f'휴지통 스캔 스케줄 등록 실패: {e}')
            logger.error(traceback.format_exc())
        return False

    def sched_remove_all(self):
        for item in get_trash_jobs():
            try:
                if F.scheduler.is_include(item['job_id']):
                    F.scheduler.remove_job(item['job_id'])
            except Exception:
                pass
        try:
            for job in list(F.scheduler.get_jobs()):
                jid = getattr(job, 'id', '') or ''
                if jid.startswith(f'{self.P.package_name}_trash_'):
                    F.scheduler.remove_job(jid)
        except Exception:
            pass

    def job_function(self, idx, mode='scheduler'):
        # Celery에 넘기지 못하면 스레드·태스크 안에서 조용히 죽는다 —
        # '실행을 시작했습니다'라고 답한 뒤 아무것도 안 뜨는 경우다.
        # 그래서 여기서 실패를 잡아 이력 행과 배너 기록으로 남긴다.
        try:
            self.start_celery(TaskTrash.start, None, *(idx, mode))
        except Exception as e:
            logger.error(f'[Trash] Celery 시작 실패: {e}')
            try:
                jobs = get_trash_jobs()
                job = jobs[idx] if 0 <= idx < len(jobs) else None
                job_name = (job.get('name') if job else '') or ''
                job_action = job.get('action') if job else None
            except Exception:
                job_name = ''
                job_action = None
            fail_trash_start(job_name, mode, f'작업을 넘기지 못했습니다: {e}', '전체', job_action)

    def one_execute(self, idx):
        # '지금 실행'은 수동 실행이라 mode를 'manual'로 넘긴다 (R19).
        # 스케줄에 등록된 작업도 스케줄러 경유가 아니라 이 경로로 실행한다.
        jobs = get_trash_jobs()
        if idx < 0 or idx >= len(jobs):
            return 'fail'
        job_id = jobs[idx]['job_id']
        try:
            if F.scheduler.is_include(job_id) and F.scheduler.is_running(job_id):
                return 'is_running'
        except Exception:
            pass

        def func():
            time.sleep(1)
            try:
                self.job_function(idx, 'manual')
            except Exception as e:
                logger.error(f'[Trash] 지금 실행 실패: {e}')
                fail_trash_start(jobs[idx].get('name') or '', 'manual',
                                 f'실행을 시작하지 못했습니다: {e}', '전체',
                                 jobs[idx].get('action'))
        t = threading.Thread(target=func, args=())
        t.daemon = True
        t.start()
        return 'thread'

    # ---------- 화면 ----------
    def process_menu(self, sub, req):
        # 서브메뉴: setting(설정) · trash(휴지통) · jobs(작업 관리) · result(작업 결과) · manual(매뉴얼)
        arg = P.ModelSetting.to_dict()
        try:
            # 라이브러리 목록은 페이지가 뜬 뒤 ajax('sections')로 따로
            # 불러온다 — shyni 응답을 기다리느라 페이지가 늦게 뜨지 않게.
            if sub == 'jobs':
                arg['trash_jobs_json'] = P.ModelSetting.get('trash_jobs') or '[]'
                sort = P.ModelSetting.get(PICK_SORT_KEY) or 'default_asc'
                arg['lib_pick_sort'] = sort if sort in PICK_SORTS else 'default_asc'
            if sub == 'trash':
                # 휴지통 조회 페이지 (스캔 메뉴에서 이동, 2026-11-18)
                # 라이브러리 목록은 페이지가 뜬 뒤 ajax('sections')로 따로
                # 불러온다 — shyni가 느릴 때 동기 호출이 페이지 표시를
                # 15초 타임아웃까지 막는 것을 없애기 위해서다 (0.9.9).
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
                # 전체 현황 행의 상세 내역(파일·묶음 수, missing·trashed) —
                # 조회한 적 있는 라이브러리의 상세 캐시 요약만 들어 있다.
                try:
                    from .task_trash_view import cache_summaries
                    arg['trash_summaries_init'] = cache_summaries()
                except Exception:
                    arg['trash_summaries_init'] = {}
                # 휴지통 조회 페이지 선택 팝업 정렬 (기본/이름/휴지통 수 내림차순)
                sort = P.ModelSetting.get('trash_lib_pick_sort') or 'default'
                arg['trash_lib_pick_sort'] = sort if sort in TRASH_PICK_SORTS else 'default'
                # 전체 현황 정렬 (기본/이름/휴지통 수 내림차순 — 기본은 휴지통 수 내림차순)
                sort = P.ModelSetting.get('trash_overview_sort') or 'trash_desc'
                arg['trash_overview_sort'] = sort if sort in TRASH_OVERVIEW_SORTS else 'trash_desc'
            return render_template(f'{P.package_name}_{self.name}_{sub}.html', arg=arg)
        except Exception as e:
            logger.error(f'Exception:{str(e)}')
            logger.error(traceback.format_exc())
            return render_template('sample.html', title=f'{P.package_name}/{self.name}/{sub}')

    def process_command(self, command, arg1, arg2, arg3, req):
        ret = {'ret': 'success'}
        if command == 'save_jobs':
            try:
                jobs = json.loads(arg1 or '[]')
                if not isinstance(jobs, list):
                    raise ValueError('형식이 올바르지 않습니다.')
            except Exception as e:
                return jsonify({'ret': 'warning', 'msg': f'작업 목록을 읽지 못했습니다: {e}'})
            from apscheduler.triggers.cron import CronTrigger
            cleaned = []
            for i, job in enumerate(jobs):
                cron = (job.get('cron') or '').strip()
                try:
                    CronTrigger.from_crontab(cron)
                except Exception:
                    return jsonify({'ret': 'warning',
                                    'msg': f"{i + 1}번째 작업의 cron 표현식이 올바르지 않습니다: {cron}"})
                sections = [str(s) for s in (job.get('sections') or [])]
                if not sections:
                    return jsonify({'ret': 'warning',
                                    'msg': f"{i + 1}번째 작업에 라이브러리를 하나 이상 선택해 주세요."})
                action = job.get('action') or 'empty'
                if action not in ACTION_LABEL:
                    action = 'empty'
                cleaned.append({
                    'name': (job.get('name') or f'작업 {i + 1}').strip(),
                    'sections': sections,
                    'cron': cron,
                    'action': action,
                    'use': bool(job.get('use')),
                })
            # 방어: 진행 중인 작업의 정의가 바뀌었거나 삭제됐으면 저장 거절.
            # (작업 관리 화면에서 행을 잠그지만, 페이지를 연 뒤 실행이
            # 시작되는 틈이나 다른 경로의 저장을 막기 위한 서버 쪽 확인이다.
            # 진행 중인 작업의 정의가 제출 목록 어딘가에 그대로 있으면 된다 —
            # 다른 행을 고치거나 앞 행을 지우는 저장은 그대로 허용한다.)
            # 여러 작업이 함께 돌 수 있으니 진행 중인 작업마다 확인한다.
            for cur in get_trash_currents():
                stored = get_trash_jobs()
                ridx = cur.get('job_idx')
                running = (stored[ridx] if isinstance(ridx, int)
                           and 0 <= ridx < len(stored) else None)
                if running is None:
                    return jsonify({'ret': 'warning',
                                    'msg': '진행 중인 작업 정보를 찾지 못했습니다. 취소한 뒤에 저장해 주세요.'})

                def _same_job(a, b):
                    return ((a.get('name') or '').strip() == (b.get('name') or '').strip()
                            and (a.get('cron') or '').strip() == (b.get('cron') or '').strip()
                            and a.get('action') == b.get('action')
                            and bool(a.get('use')) == bool(b.get('use'))
                            and sorted(str(s) for s in (a.get('sections') or []))
                            == sorted(str(s) for s in (b.get('sections') or [])))
                if not any(_same_job(running, c) for c in cleaned):
                    return jsonify({'ret': 'warning',
                                    'msg': f"진행 중인 작업 '{running.get('name') or ''}'은(는) 취소한 뒤에 수정할 수 있습니다."})
                # 순서 변경: 진행 중 작업이 새 목록에서 다른 위치로
                # 옮겨졌으면 진행 상태의 작업 번호를 새 위치로 고쳐 쓴다.
                new_idx = next((k for k, c in enumerate(cleaned)
                                if _same_job(running, c)), None)
                if new_idx is not None and new_idx != ridx:
                    update_trash_current_job_idx(cur.get('claim_id'), new_idx)
            self.sched_remove_all()
            P.ModelSetting.set('trash_jobs', json.dumps(cleaned, ensure_ascii=False))
            self.start()
            logger.info(f"[Trash] 작업 저장: {len(cleaned)}개 — "
                        f"{[(j['name'], j['action'], j['cron'], j['use']) for j in cleaned]}")
            ret['msg'] = f'작업 {len(cleaned)}개를 저장했습니다. 사용으로 켜진 작업은 스케줄러에 반영됐습니다.'
        elif command == 'task_execute':
            try:
                idx = int(arg1)
            except Exception:
                return jsonify({'ret': 'warning', 'msg': '작업 번호가 올바르지 않습니다.'})
            jobs = get_trash_jobs()
            if idx < 0 or idx >= len(jobs):
                return jsonify({'ret': 'warning', 'msg': '작업 번호가 올바르지 않습니다.'})
            if not (jobs[idx].get('sections') or []):
                return jsonify({'ret': 'warning',
                                'msg': '선택한 라이브러리가 없습니다. 라이브러리 선택에서 골라 주세요.'})
            if _same_job_busy(jobs[idx].get('name') or ''):
                # 거절은 로그와 배너에 남긴다 — 안 그러면 '시작 중'
                # 카드만 떴다 사라지고 왜 시작 안 했는지 알 수 없다
                # (2026-11-25 주기적 스캔과 같은 문제).
                logger.info(f"[Trash] 지금 실행 거절: {jobs[idx].get('name') or ''} — 이미 실행 중입니다.")
                _record_trash_start_error(jobs[idx].get('name') or '',
                                          '이미 실행 중입니다.')
                return jsonify({'ret': 'warning', 'msg': '이미 실행 중입니다.'})
            # 규칙 1이 규칙 2보다 먼저다. 규칙 1: 같은 모듈 안에서
            # 같은 라이브러리가 실행 중이면 동작 종류와 무관하게
            # 거절한다. 새로고침만·비우기만 하는 동작도 예외가 아니다.
            if _same_library_busy(jobs[idx]['sections'][0]):
                logger.info(f"[Trash] 지금 실행 거절: {jobs[idx].get('name') or ''} — {LIBRARY_BUSY_MESSAGE}")
                _record_trash_start_error(jobs[idx].get('name') or '',
                                          LIBRARY_BUSY_MESSAGE)
                return jsonify({'ret': 'warning',
                                'msg': LIBRARY_BUSY_MESSAGE})
            # 규칙 2: 스캔이 들어간 동작은 첫 라이브러리 기준으로
            # 게이트를 본다 — 같은 라이브러리가 스캔 중이거나 설정한
            # 개수까지 돌고 있으면 시작하지 않고 거절한다.
            # 새로고침만·비우기만 하는 동작은 스캔 자리를 안 쓴다.
            if 'scan' in str(jobs[idx].get('action') or ''):
                gate_msg = ModuleStatus.library_gate_check(jobs[idx]['sections'][0])
                if gate_msg:
                    logger.info(f"[Trash] 지금 실행 거절: {jobs[idx].get('name') or ''} — {gate_msg}")
                    _record_trash_start_error(jobs[idx].get('name') or '',
                                              gate_msg)
                    return jsonify({'ret': 'warning', 'msg': gate_msg})
            result = self.one_execute(idx)
            logger.info(f'[Trash] 지금 실행 요청: idx={idx} 결과={result}')
            if result == 'is_running':
                ret['msg'] = '이미 실행 중입니다.'
            elif result == 'fail':
                return jsonify({'ret': 'warning', 'msg': '실행하지 못했습니다.'})
            else:
                ret['msg'] = '실행을 시작했습니다. 잠시 뒤 워커가 작업을 시작합니다.'
        elif command == 'cancel_current':
            claim_id = (arg1 or '').strip() or None
            curs = get_trash_currents()
            cur = None
            if claim_id is not None:
                for c in curs:
                    if str(c.get('claim_id')) == claim_id:
                        cur = c
                        break
            elif len(curs) == 1:
                cur = curs[0]
            if cur is None:
                return jsonify({'ret': 'warning', 'msg': '지금 진행 중인 휴지통 스캔이 없습니다.'})
            request_trash_cancel(cur.get('claim_id'))
            logger.info('[Trash] 취소 요청')
            # Step5-1 — shyni 스캔까지 함께 취소하므로 "웹에서 취소" 안내는 없앴다.
            # 비우기 중에는 버튼이 막혀 있지만, 들어온 요청은 비우기를
            # 끝까지 진행하고 남은 라이브러리를 멈추는 것으로 처리한다.
            stage = (cur.get('stage') or '')
            if stage == 'scan':
                ret['msg'] = '취소를 요청했습니다. shyni 스캔 취소가 확인되면 멈춥니다.'
            elif stage == 'empty':
                ret['msg'] = '비우기는 끝까지 진행되고, 남은 라이브러리는 멈춥니다.'
            elif stage == 'waiting':
                ret['msg'] = '취소를 요청했습니다. 대기가 중단되면 멈춥니다.'
            else:
                ret['msg'] = '취소를 요청했습니다. 새로고침이 정리되면 멈춥니다.'
        elif command == 'trash_counts':
            from . import shyni_admin, db_support
            if not shyni_admin.admin_available() and not db_support.db_ready():
                return jsonify({'ret': 'fail', 'msg': '관리자 API나 Relay 중 하나를 설정해 주세요.'})
            try:
                # 현황·건강도와 같은 개수 캐시를 쓴다 — 라이브러리 선택
                # 팝업을 열 때마다 무거운 전체 읽기를 돌리지 않기 위해서다
                from .task_status import trash_counts_for_stats
                ret['data'] = trash_counts_for_stats()
            except Exception as e:
                return jsonify({'ret': 'fail', 'msg': f'휴지통 수를 읽지 못했습니다: {e}'})
            # 전체 현황 행의 상세 내역도 함께 실어 보낸다 — 조회가
            # 끝난 뒤 현황으로 돌아올 때 새 요약을 바로 보여 주기 위해서다
            try:
                from .task_trash_view import cache_summaries
                ret['summaries'] = cache_summaries()
            except Exception:
                ret['summaries'] = {}
        elif command == 'trash_query':
            # 조회 계산은 워커(task_trash_view)가 수행한다 — 웹은 요청만 남기고
            # 화면은 trash_state로 진행률을 폴링한다.
            from . import shyni_admin, db_support
            if not shyni_admin.admin_available() and not db_support.db_ready():
                return jsonify({'ret': 'fail',
                                'msg': '휴지통 조회는 관리자 API나 Relay 중 하나를 설정해야 사용할 수 있습니다.'})
            try:
                lib_id = int(arg1)
            except Exception:
                return jsonify({'ret': 'warning', 'msg': '라이브러리를 선택해 주세요.'})
            from .task_trash_view import request_view
            ret['data'] = request_view(lib_id, force=(arg2 == '1'))
        elif command == 'trash_query_all':
            # 전체 조회 접수 — 계산은 워커가 라이브러리 단위로 수행한다
            from . import shyni_admin, db_support
            if not shyni_admin.admin_available() and not db_support.db_ready():
                return jsonify({'ret': 'fail',
                                'msg': '휴지통 조회는 관리자 API나 Relay 중 하나를 설정해야 사용할 수 있습니다.'})
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
            from . import shyni_admin, db_support
            if not shyni_admin.admin_available() and not db_support.db_ready():
                return jsonify({'ret': 'fail',
                                'msg': '휴지통 조회는 관리자 API나 Relay 중 하나를 설정해야 사용할 수 있습니다.'})
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
            from . import shyni_admin, db_support
            if not shyni_admin.admin_available() and not db_support.db_ready():
                return jsonify({'ret': 'fail', 'msg': '관리자 API나 Relay 중 하나를 설정해 주세요.'})
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
            # 작업 결과에도 남긴다 — 휴지통 페이지에서 비운 것도 이력이다.
            # 이력 기록이 실패해도 비우기 자체는 이미 끝났으니 경고만 남긴다.
            removed, retained = None, None
            try:
                removed, retained = _parse_empty_stats(
                    res.json() if res is not None else None)
            except Exception:
                pass
            msg = _empty_result_message(removed, retained)
            try:
                title = (arg2 or '').strip()
                if not title:
                    try:
                        sections, _ = ModuleStatus._sections()
                        for s in sections or []:
                            if int(s.get('key')) == lib_id:
                                title = s.get('title') or ''
                                break
                    except Exception:
                        title = ''
                with F.app.app_context():
                    item = ModelTrashItem('휴지통 (전체)', lib_id, title,
                                          'manual', 'empty')
                    if removed is not None:
                        item.emptied_count = removed
                    item.finish('done', msg)
            except Exception as e:
                logger.warning(f'[휴지통] 전체 비우기 이력 기록 실패: {e}')
            ret['msg'] = msg
        elif command == 'trash_empty_selected':
            # 선택한 작품 폴더의 휴지통 항목만 비운다 (관리자 API, part_ids).
            # 선택 실행(새로고침·스캔)과 달리 API로 조회한 결과에서도 된다 —
            # 캐시된 파일 목록에 part_id가 들어 있어 (API·Relay 출처 모두)
            # 폴더만으로 part_ids를 모을 수 있다. 보관 기간은 지킨다
            # (ignore_retention/force를 보내지 않는다).
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
            folders = payload.get('folders')
            title = (payload.get('title') or '').strip()
            from .task_trash_selected import MAX_FOLDERS
            if not isinstance(folders, list) or not folders:
                return jsonify({'ret': 'warning', 'msg': '비울 항목을 선택해 주세요.'})
            folder_set = {str(f) for f in folders if str(f).strip()}
            if not folder_set:
                return jsonify({'ret': 'warning', 'msg': '비울 항목을 선택해 주세요.'})
            if len(folder_set) > MAX_FOLDERS:
                return jsonify({'ret': 'warning',
                                'msg': f'한 번에 최대 {MAX_FOLDERS}개까지 비울 수 있습니다.'})
            from . import shyni_admin
            if not shyni_admin.admin_available():
                return jsonify({'ret': 'fail',
                                'msg': '선택 비우기는 관리자 API 설정이 필요합니다. 설정에서 관리자 아이디·비밀번호를 입력해 주세요.'})
            # 대상 폴더는 지금 조회된(캐시된) 작품의 공통 폴더여야 한다 —
            # 화면 밖의 임의 항목이 비워지지 않게 한다 (선택 실행과 같은 검증).
            from .task_trash_view import cached_data
            data = cached_data(lib_id)
            if not data:
                return jsonify({'ret': 'fail',
                                'msg': '조회된 결과가 없습니다. 조회 버튼을 눌러 주세요.'})
            groups = data.get('groups') or []
            allowed = {g.get('folder') for g in groups if g.get('folder')}
            unknown = [f for f in folder_set if f not in allowed]
            if unknown:
                return jsonify({'ret': 'warning',
                                'msg': '조회 결과에 없는 항목이 섞여 있습니다. 조회 버튼을 눌러 다시 불러온 뒤 실행해 주세요.'})
            # 폴더 → part_ids: 캐시된 파일 목록에서 모은다 (API·Relay 출처
            # 모두 파일 항목에 part_id('id')가 있다).
            files_map = data.get('files') or {}
            part_ids = set()
            for g in groups:
                if g.get('folder') not in folder_set:
                    continue
                for f in files_map.get(str(g.get('root_id'))) or []:
                    try:
                        pid = int(f.get('id'))
                    except Exception:
                        continue
                    if pid > 0:
                        part_ids.add(pid)
            if not part_ids:
                return jsonify({'ret': 'warning',
                                'msg': '선택한 항목에서 비울 파일을 찾지 못했습니다. 조회 버튼을 눌러 다시 불러와 주세요.'})
            res = shyni_admin.api_empty_trash(lib_id, sorted(part_ids))
            if res is None:
                return jsonify({'ret': 'fail', 'msg': 'shyni에 연결하지 못했습니다. 잠시 후 다시 시도해 주세요.'})
            if res.status_code == 409:
                # 선택 항목이 바뀐 경우 — shyni 메시지를 그대로 보여준다
                detail = None
                try:
                    detail = (res.json() or {}).get('detail')
                except Exception:
                    detail = None
                return jsonify({'ret': 'warning',
                                'msg': detail or '선택 항목이 변경되었습니다. 휴지통 목록을 새로고침하세요.'})
            if res.status_code != 200:
                return jsonify({'ret': 'fail',
                                'msg': f'휴지통 비우기에 실패했습니다 (응답 코드 {res.status_code}).'})
            removed, retained = None, None
            try:
                removed, retained = _parse_empty_stats(res.json())
            except Exception:
                pass
            msg = _empty_result_message(removed, retained)
            logger.info(f'[휴지통] 선택 비우기: 라이브러리 {lib_id} 폴더 {len(folder_set)}개 · '
                        f'part {len(part_ids)}개 · {msg}')
            try:
                from .task_trash_view import clear_cache
                clear_cache(lib_id)
            except Exception as e:
                logger.warning(f'[휴지통] 선택 비우기 후 상세 캐시 지우기 실패: {e}')
            try:
                from .task_status import refresh_trash_counts_async, request_stats
                refresh_trash_counts_async()
                request_stats()
            except Exception as e:
                logger.warning(f'[휴지통] 선택 비우기 후 개수·통계 갱신 요청 실패: {e}')
            # 작업 결과에도 남긴다 — 이력 기록이 실패해도 비우기 자체는
            # 이미 끝났으니 경고만 남긴다.
            try:
                if not title:
                    try:
                        sections, _ = ModuleStatus._sections()
                        for s in sections or []:
                            if int(s.get('key')) == lib_id:
                                title = s.get('title') or ''
                                break
                    except Exception:
                        title = ''
                with F.app.app_context():
                    item = ModelTrashItem('휴지통 (선택)', lib_id, title,
                                          'manual', 'empty')
                    if removed is not None:
                        item.emptied_count = removed
                    item.finish('done', msg)
            except Exception as e:
                logger.warning(f'[휴지통] 선택 비우기 이력 기록 실패: {e}')
            ret['msg'] = msg
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
            # API로 조회한 결과에서는 개별 선택 실행을 막는다
            # (사용자 지시, 2026-11-17 — 화면에서도 체크박스를 숨긴다)
            if (data.get('source') or 'relay') == 'api':
                return jsonify({'ret': 'warning',
                                'msg': '관리자 API로 조회한 결과에서는 개별 선택 실행을 할 수 없습니다.'})
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
        return jsonify(ret)

    # ---------- 화면 AJAX ----------
    def process_ajax(self, sub, req):
        if sub == 'sections':
            sections, err = ModuleStatus._sections()
            if sections is None:
                return jsonify({'ret': 'fail', 'msg': err})
            return jsonify({'ret': 'success', 'data': sections})
        if sub == 'current':
            from .model_trash import action_label
            cards = []
            for cur in get_trash_currents():
                if cur.get('action'):
                    cur['action_label'] = action_label(cur.get('action'))
                if cur.get('stage') == 'scan' and cur.get('section_key'):
                    cur['scan_progress'] = current_scan_progress(
                        cur['section_key'], 'trash')
                # 취소 요청 여부 — 새로고침해도 '취소중' 버튼이 유지되게
                # 서버에서 알려준다. 취소 요청은 작업이 멈출 때 지워진다.
                cur['cancel_requested'] = bool(
                    _trash_cancel_requested(cur.get('claim_id')))
                cards.append(cur)
            # 대시보드에서 시작한 스캔도 함께 보여 준다 — 어디서
            # 시작했든 진행 중인 스캔을 한곳에서 볼 수 있게 한다.
            try:
                cards.extend(ModuleStatus.dashboard_scan_cards())
            except Exception as e:
                logger.warning(f'[Trash] 대시보드 스캔 목록 조회 실패: {e}')
            return jsonify({'ret': 'success', 'data': cards,
                            'last_error': get_last_trash_start_error()})
        if sub == 'clear_start_error':
            clear_trash_start_error()
            return jsonify({'ret': 'success'})
        if sub == 'pick_sort':
            sort = (req.form.get('sort') or '').strip()
            if sort not in PICK_SORTS:
                return jsonify({'ret': 'fail', 'msg': '정렬 조건이 올바르지 않습니다.'})
            P.ModelSetting.set(PICK_SORT_KEY, sort)
            return jsonify({'ret': 'success'})
        if sub == 'lib_pick_sort':
            # 휴지통 조회 페이지 선택 팝업 정렬 — jobs 페이지의 pick_sort와 별개 키
            sort = (req.form.get('sort') or '').strip()
            if sort not in TRASH_PICK_SORTS:
                return jsonify({'ret': 'fail', 'msg': '정렬 조건이 올바르지 않습니다.'})
            P.ModelSetting.set(TRASH_PICK_SORT_KEY, sort)
            return jsonify({'ret': 'success'})
        if sub == 'overview_sort':
            # 전체 현황 정렬 — 선택 팝업의 lib_pick_sort와 별개 키
            sort = (req.form.get('sort') or '').strip()
            if sort not in TRASH_OVERVIEW_SORTS:
                return jsonify({'ret': 'fail', 'msg': '정렬 조건이 올바르지 않습니다.'})
            P.ModelSetting.set(TRASH_OVERVIEW_SORT_KEY, sort)
            return jsonify({'ret': 'success'})
        if sub == 'set_hidden_libs':
            # '표시'에서 고른 숨김 라이브러리를 휴지통 화면 전용으로
            # 기억한다. 메타의 set_hidden_libs와 같은 방식 — 체크할
            # 때마다 조용히 저장하므로 성공 응답에 msg를 넣지 않는다.
            keys = []
            for part in (req.form.get('keys') or '').split(','):
                part = part.strip()
                if not part:
                    continue
                try:
                    keys.append(str(int(part)))
                except Exception:
                    return jsonify({'ret': 'fail', 'msg': '요청 값이 올바르지 않습니다.'})
            P.ModelSetting.set(TRASH_OVERVIEW_HIDDEN_LIBS_KEY, ','.join(keys))
            return jsonify({'ret': 'success'})
        if sub == 'history':
            try:
                page = int(req.form.get('page') or 1)
            except Exception:
                page = 1
            section = (req.form.get('section') or '').strip() or None
            only_append = (req.form.get('only_append') or '') == '1'
            search = (req.form.get('search') or '').strip() or None
            data = ModelTrashItem.get_page(
                page, section_key=section, only_append=only_append, search=search)
            annotate_total_labels(data.get('list') or [])
            # 대상 목록이 있는 행(재설계 후 실행)은 stat_total이 대상
            # 휴지통 파일 수 실측값이라 도서여도 '전체'로 본다.
            for row in data.get('list') or []:
                if row.get('has_target_files'):
                    row['total_label'] = '전체'
            return jsonify({'ret': 'success', 'data': data})
        if sub == 'history_detail':
            try:
                item_id = int(req.form.get('id') or 0)
            except Exception:
                item_id = 0
            data = ModelTrashItem.get_detail(item_id)
            if data is None:
                return jsonify({'ret': 'fail', 'msg': '이력을 찾지 못했습니다.'})
            annotate_total_labels([data])
            if data.get('has_target_files'):
                data['total_label'] = '전체'
            return jsonify({'ret': 'success', 'data': data})
        return jsonify({'ret': 'fail', 'msg': '알 수 없는 요청입니다.'})

    def process_api(self, sub, req):
        return self.process_ajax(sub, req)
