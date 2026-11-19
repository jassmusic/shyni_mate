import json
import threading

from .mod_status import ModuleStatus
from .setup import *
from .task_meta_view import TaskMetaView

name = 'meta'
logger = P.logger

# 라이브러리 선택 목록 정렬 — 휴지통 선택 팝업과 같은 방식이다.
# 종류별 정렬(코드·설명·연도·표지 없음 순)은 건강도의 종류 구분과 같다.
META_PICK_SORTS = ('default', 'name', 'nocode_desc', 'code_desc',
                   'summary_desc', 'year_desc', 'thumb_desc')
META_PICK_SORT_KEY = 'meta_lib_pick_sort'
# 메타 전체 현황 정렬 (선택 팝업과 별개, 기본은 메타 없음 수 내림차순)
META_OVERVIEW_SORTS = ('default', 'name', 'nocode_desc', 'code_desc',
                       'summary_desc', 'year_desc', 'thumb_desc')
META_OVERVIEW_SORT_KEY = 'meta_overview_sort'
# 메타 전체 현황 표시 선택 — 숨긴 라이브러리 키(쉼표 구분, 대시보드·건강도와 별개)
META_OVERVIEW_HIDDEN_LIBS_KEY = 'meta_overview_hidden_libs'


class ModuleMeta(PluginModuleBase):
    """메타 상세보기 — 라이브러리별 미디어의 메타(코드·소개·연도·표지)
    유무를 작품 단위로 보여 준다. 휴지통 탭이 휴지통의 상세보기라면,
    메타 탭은 메타의 상세보기이다 (사용자 정의, 2026-11-18).

    목록은 Relay로만 읽는다 — 메타는 Relay 없이는 의미가 없어
    (사용자 지시) 관리자 API 폴백이 없고, Relay가 꺼져 있으면
    건강도·세션 탭과 같은 안내를 보여 준다."""

    def __init__(self, P):
        super(ModuleMeta, self).__init__(P, name=name, first_menu='list')
        self.db_default = {
            f'{self.name}_db_version': '1',
            f'{self.name}_lib_pick_sort': 'default',
            f'{self.name}_overview_sort': 'nocode_desc',
            f'{self.name}_overview_hidden_libs': '',
        }

    def plugin_load(self):
        # 메타 조회 워커 — 휴지통 조회 워커와 같은 기동 방식이다
        def _start_meta_workers():
            self.start_celery(TaskMetaView.start)
        thread = threading.Thread(target=_start_meta_workers, args=())
        thread.daemon = True
        thread.start()

    # ---------- 화면 ----------
    def process_menu(self, sub, req):
        # 서브메뉴: list(메타) · manual(매뉴얼) — 대시보드 '메타 상세' 버튼이
        # /shyni_mate/meta/list?key=<라이브러리 key>로 넘어온다.
        # manual은 템플릿만 렌더한다 (주기적·휴지통·스캔 매뉴얼과 같은 방식).
        arg = P.ModelSetting.to_dict()
        try:
            if sub == 'list':
                # 라이브러리 목록·현황은 페이지가 뜬 뒤 ajax로 따로 불러온다 —
                # shyni 응답을 기다리느라 페이지가 늦게 뜨지 않게.
                # '메타 상세'로 넘어오면 그 라이브러리를 미리 골라 둔다
                try:
                    arg['meta_preselect'] = (req.args.get('key') or '') if req is not None else ''
                except Exception:
                    arg['meta_preselect'] = ''
                # 캐시 현황은 첫 화면에 실어 보내지 않는다 — 페이지가 뜬 뒤
                # meta_state로 채운다. 첫 화면 렌더가 캐시에 닿지 않게
                # 해 메뉴 클릭 때 페이지가 늦게 뜨지 않게 한다.
                arg['meta_cache_times'] = {}
                sort = P.ModelSetting.get(META_PICK_SORT_KEY) or 'default'
                arg['meta_lib_pick_sort'] = sort if sort in META_PICK_SORTS else 'default'
                # 전체 현황 정렬 (기본/이름/메타 없음 수 내림차순 — 기본은 메타 없음 수 내림차순)
                sort = P.ModelSetting.get(META_OVERVIEW_SORT_KEY) or 'nocode_desc'
                arg['meta_overview_sort'] = sort if sort in META_OVERVIEW_SORTS else 'nocode_desc'
            return render_template(f'{P.package_name}_{self.name}_{sub}.html', arg=arg)
        except Exception as e:
            logger.error(f'Exception:{str(e)}')
            logger.error(traceback.format_exc())
            return render_template('sample.html', title=f'{P.package_name}/{self.name}/{sub}')

    def process_command(self, command, arg1, arg2, arg3, req):
        ret = {'ret': 'success'}
        if command == 'meta_overview':
            # 진입 현황 — 라이브러리별 전체·메타 없음 수 + 캐시 정보.
            # 개수는 메타 상세 캐시의 요약(인덱스)에서 가져온다 — 상세와
            # 같은 출처·같은 시점이라 현황·상세 숫자가 일치한다
            # (2026-11-20 출처 통일). 아직 상세 캐시가 없는 라이브러리는
            # 대시보드 통계 캐시 값으로 채운다.
            from . import db_support
            if not db_support.db_ready():
                return jsonify({'ret': 'success', 'relay_off': True})
            try:
                from .task_status import CACHE_KEY as STATS_CACHE_KEY
                raw = P.ModelSetting.get(STATS_CACHE_KEY)
                saved = json.loads(raw) if raw else {}
                libs = ((saved.get('data') or {}).get('libs') or {}) \
                    if isinstance(saved, dict) else {}
            except Exception:
                libs = {}
            from .task_meta_view import cache_times, read_index
            index = read_index()
            if not libs and not index:
                # 아직 계산된 통계도, 상세 캐시도 없으면 워커에 계산을
                # 요청하고, 화면이 잠시 뒤 다시 물어보게 한다.
                try:
                    from .task_status import request_stats
                    request_stats()
                except Exception:
                    pass
                return jsonify({'ret': 'success', 'relay_off': False,
                                'stats_missing': True})
            out = {}
            for k in sorted(set([str(x) for x in libs.keys()]
                                + [str(x) for x in index.keys()])):
                st = libs.get(k)
                if not isinstance(st, dict):
                    st = {}
                entry = index.get(k) or {}
                summary = entry.get('summary') \
                    if isinstance(entry.get('summary'), dict) else None
                if summary is not None:
                    no_meta = summary.get('no_meta')
                    if no_meta is None:
                        no_meta = summary.get('no_code') or 0
                    out[k] = {
                        'total': summary.get('total')
                        if summary.get('total') is not None
                        else (entry.get('total') or 0),
                        'no_meta': no_meta,
                        'missing_any': summary.get('missing_any'),
                        'no_code': summary.get('no_code') or 0,
                        'no_summary': summary.get('no_summary') or 0,
                        'no_year': summary.get('no_year') or 0,
                        'no_thumb': summary.get('no_thumb') or 0,
                        'is_folder': entry.get('is_folder')
                        if entry.get('is_folder') is not None
                        else bool(st.get('is_folder')),
                        'main_label': entry.get('main_label')
                        or st.get('main_label') or '미디어',
                    }
                else:
                    no_meta = st.get('no_meta')
                    if no_meta is None:
                        no_meta = st.get('no_code') or 0
                    out[k] = {
                        'total': st.get('main_count') or 0,
                        'no_meta': no_meta,
                        # 값이 하나라도 없는 미디어 수 — 상세 첫 줄과 같은 값이라
                        # 현황 첫 줄에도 이 수를 쓴다 (no_meta는 코드 없음 수라
                        # 상세와 달라 보였다).
                        'missing_any': st.get('missing_any'),
                        'no_code': st.get('no_code') or 0,
                        'no_summary': st.get('no_summary') or 0,
                        'no_year': st.get('no_year') or 0,
                        'no_thumb': st.get('no_thumb') or 0,
                        'is_folder': bool(st.get('is_folder')),
                        'main_label': st.get('main_label') or '미디어',
                    }
            ret['data'] = {'counts': out,
                           'cache_times': cache_times()}
        elif command == 'meta_query':
            # 조회 계산은 워커(task_meta_view)가 수행한다 — 웹은 요청만 남기고
            # 화면은 meta_state로 진행률을 폴링한다.
            from . import db_support
            if not db_support.db_ready():
                return jsonify({'ret': 'fail',
                                'msg': '메타 목록은 Relay를 켜야 볼 수 있습니다. 설정 > Relay에서 켜 주세요.'})
            try:
                lib_id = int(arg1)
            except Exception:
                return jsonify({'ret': 'warning', 'msg': '라이브러리를 선택해 주세요.'})
            from .task_meta_view import request_view
            ret['data'] = request_view(lib_id, force=(arg2 == '1'))
        elif command == 'meta_query_all':
            # 전체 조회 접수 — 계산은 워커가 라이브러리 단위로 수행한다
            from . import db_support
            if not db_support.db_ready():
                return jsonify({'ret': 'fail',
                                'msg': '메타 목록은 Relay를 켜야 볼 수 있습니다. 설정 > Relay에서 켜 주세요.'})
            try:
                lib_ids = json.loads(arg1 or '[]')
            except Exception:
                lib_ids = []
            from .task_meta_view import request_bulk
            res = request_bulk(lib_ids)
            if res.get('ret') == 'fail':
                return jsonify(res)
            ret['data'] = res
        elif command == 'meta_query_all_cancel':
            from .task_meta_view import cancel_bulk
            cancel_bulk()
        elif command == 'meta_query_cancel':
            from .task_meta_view import cancel_query
            cancel_query()
        elif command == 'meta_state':
            from .task_meta_view import cache_state, read_state, read_bulk
            cs = cache_state()
            ret['data'] = {'state': read_state(),
                           'cache_libs': cs['libs'],
                           'cache_times': cs['times'],
                           'bulk': read_bulk()}
        elif command == 'meta_list':
            # 워커가 계산해 둔 캐시를 범위·검색·정렬·페이지 단위로 잘라 돌려준다.
            from . import db_support
            if not db_support.db_ready():
                return jsonify({'ret': 'fail',
                                'msg': '메타 목록은 Relay를 켜야 볼 수 있습니다. 설정 > Relay에서 켜 주세요.'})
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
            from .task_meta_view import cached_data, page_from_cache
            data = cached_data(lib_id)
            if not data:
                return jsonify({'ret': 'fail',
                                'msg': '조회된 결과가 없습니다. 조회 버튼을 눌러 주세요.'})
            try:
                ret['data'] = page_from_cache(
                    data,
                    page=int(opts.get('page') or 1),
                    search=(opts.get('search') or '').strip(),
                    order=opts.get('order') or 'default',
                    scope=opts.get('scope') or 'missing')
            except Exception as e:
                logger.error(f'[메타] 조회 결과 표시 실패: {e}')
                return jsonify({'ret': 'fail', 'msg': f'메타 목록을 표시하지 못했습니다: {e}'})
        return jsonify(ret)

    # ---------- 화면 AJAX ----------
    def process_ajax(self, sub, req):
        if sub == 'sections':
            sections, err = ModuleStatus._sections()
            if sections is None:
                return jsonify({'ret': 'fail', 'msg': err})
            return jsonify({'ret': 'success', 'data': sections})
        if sub == 'lib_pick_sort':
            sort = (req.form.get('sort') or '').strip()
            if sort not in META_PICK_SORTS:
                return jsonify({'ret': 'fail', 'msg': '정렬 조건이 올바르지 않습니다.'})
            P.ModelSetting.set(META_PICK_SORT_KEY, sort)
            return jsonify({'ret': 'success'})
        if sub == 'overview_sort':
            # 전체 현황 정렬 — 선택 팝업의 lib_pick_sort와 별개 키
            sort = (req.form.get('sort') or '').strip()
            if sort not in META_OVERVIEW_SORTS:
                return jsonify({'ret': 'fail', 'msg': '정렬 조건이 올바르지 않습니다.'})
            P.ModelSetting.set(META_OVERVIEW_SORT_KEY, sort)
            return jsonify({'ret': 'success'})
        if sub == 'set_hidden_libs':
            # '표시'에서 고른 숨김 라이브러리를 메타 화면 전용으로 기억한다.
            # 체크할 때마다 조용히 저장하므로 성공 응답에 msg를 넣지 않는다.
            keys = []
            for part in (req.form.get('keys') or '').split(','):
                part = part.strip()
                if not part:
                    continue
                try:
                    keys.append(str(int(part)))
                except Exception:
                    return jsonify({'ret': 'fail', 'msg': '요청 값이 올바르지 않습니다.'})
            P.ModelSetting.set(META_OVERVIEW_HIDDEN_LIBS_KEY, ','.join(keys))
            return jsonify({'ret': 'success'})
        return jsonify({'ret': 'fail', 'msg': '알 수 없는 요청입니다.'})

    def process_api(self, sub, req):
        return self.process_ajax(sub, req)
