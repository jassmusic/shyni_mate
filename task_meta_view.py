# -*- coding: utf-8 -*-
"""메타 상세 목록 계산을 Celery 워커가 전담한다.

작품 수가 8만 개를 넘는 라이브러리도 있어, Relay DB에서 작품 전체와
부모 제목을 읽어 4가지 메타 유무를 매기는 계산이 수 초에서 수십 초까지
걸린다. 웹 프로세스에서 직접 하면 화면이 멈춘 느낌을 주므로, 휴지통
조회(task_trash_view)와 같은 방식이다: 웹은 ModelSetting에 '조회 요청'만
남기고 진행률을 폴링하고, 워커가 계산해 캐시에 저장하면 웹은 캐시를
검색·정렬·페이지 단위로 잘라 돌려준다.

메타는 Relay 없이는 의미가 없어 (사용자 지시) 관리자 API 폴백이 없다 —
Relay가 꺼져 있으면 계산하지 않고 화면에 안내만 보여 준다.

상태 공유 키:
- meta_view_state : {'lib', 'busy', 'progress', 'requested_at', 'started_at',
                     'finished_at', 'error', 'cancel', 'cancelled'}
  (cancel: 조회 중 취소 요청, cancelled: 취소로 끝난 계산)
- meta_view_cache : {'libs': {라이브러리키: {'at', 'data'}}} — 라이브러리별
  캐시를 전부 보관한다 (휴지통과 같은 방식).
- meta_bulk_state : 전체 조회 진행 상태.

자동 갱신 주기(base_meta_refresh_hours, 기본 12시간)가 지난 캐시는
오래된 것으로 보고 다시 계산한다. 0이면 열 때마다 새로 조회한다.
"""
import json
import threading
import time

from .setup import *
from . import db_support

logger = P.logger

STATE_KEY = 'meta_view_state'
BULK_KEY = 'meta_bulk_state'
CACHE_KEY = 'meta_view_cache'
WATCH_INTERVAL = 2
REQUEST_EXPIRE = 15 * 60
# 계산 중(busy)인 채로 이 시간이 지나면 중단된 것으로 본다 — 계산 도중
# ff가 재시작되면 busy가 남아 이후 조회가 전부 막히는 것을 막는다.
BUSY_EXPIRE = 10 * 60


def refresh_hours():
    """메타 목록 자동 갱신 주기(시간) — 기본 12시간, 0이면 매번 새로 조회."""
    try:
        return int(P.ModelSetting.get('base_meta_refresh_hours') or 12)
    except Exception:
        return 12


def read_state():
    try:
        raw = P.ModelSetting.get(STATE_KEY)
        if raw:
            state = json.loads(raw)
            if isinstance(state, dict):
                return state
    except Exception:
        pass
    return {}


def write_state(**kwargs):
    state = read_state()
    state.update(kwargs)
    try:
        P.ModelSetting.set(STATE_KEY, json.dumps(state))
    except Exception as e:
        logger.warning(f'[Meta] 조회 상태 저장 실패: {e}')
    return state


def read_cache():
    try:
        raw = P.ModelSetting.get(CACHE_KEY)
        if raw:
            cache = json.loads(raw)
            if isinstance(cache, dict):
                return cache
    except Exception:
        pass
    return None


def _cache_libs():
    """캐시를 {라이브러리키: {'at', 'data'}} 형태로 돌려준다."""
    cache = read_cache() or {}
    libs = cache.get('libs')
    if isinstance(libs, dict):
        return libs
    return {}


def write_cache(lib_id, data):
    """조회 결과 캐시를 저장한다 — 성공 여부를 돌려준다 (실패하면
    compute_view가 완료 표시 대신 오류 상태를 남긴다)."""
    libs = _cache_libs()
    libs[str(lib_id)] = {'at': time.time(), 'data': data}
    try:
        P.ModelSetting.set(CACHE_KEY, json.dumps({'libs': libs},
                                                 ensure_ascii=False))
        return True
    except Exception as e:
        logger.warning(f'[Meta] 메타 조회 캐시 저장 실패: {e}')
        return False


def cached_data(lib_id):
    entry = _cache_libs().get(str(lib_id))
    if isinstance(entry, dict):
        return entry.get('data')
    return None


def cache_libs():
    return list(_cache_libs().keys())


def cache_times():
    """라이브러리별 캐시 저장 시각 {키: epoch} — '캐시 데이터 기준' 표시용."""
    return {k: (v.get('at') or 0) for k, v in _cache_libs().items()}


def is_cache_fresh(lib_id):
    """캐시가 있고 자동 갱신 주기 안에 들면 True. 주기가 0이면 항상 False.
    이전 행 형식(폴더명 없음, v 없음) 캐시는 오래된 것으로 보고 다시
    계산한다 — 다음 조회 때 새 형식으로 채워진다."""
    entry = _cache_libs().get(str(lib_id))
    if not isinstance(entry, dict) or not isinstance(entry.get('data'), dict):
        return False
    if entry['data'].get('v') != db_support.META_VIEW_FORMAT:
        return False
    hours = refresh_hours()
    if hours <= 0:
        return False
    return time.time() - (entry.get('at') or 0) <= hours * 3600


def clear_cache(lib_id=None):
    if lib_id is None:
        try:
            P.ModelSetting.set(CACHE_KEY, '')
        except Exception as e:
            logger.warning(f'[Meta] 메타 조회 캐시 지우기 실패: {e}')
        return
    libs = _cache_libs()
    if str(lib_id) in libs:
        del libs[str(lib_id)]
        try:
            P.ModelSetting.set(CACHE_KEY, json.dumps({'libs': libs},
                                                     ensure_ascii=False))
        except Exception as e:
            logger.warning(f'[Meta] 메타 조회 캐시 지우기 실패: {e}')


def request_view(lib_id, force=False):
    """웹이 조회를 요청한다 — 실제 계산은 워커가 수행한다.

    이미 계산 중이면 그 상태를 그대로 돌려주고, 같은 라이브러리의 요청이
    접수만 된 채 15분이 지나면 워커 무응답으로 보고 다시 요청할 수 있게 한다.
    다른 라이브러리의 요청이 대기 중이면 덮어쓰지 않고 거부한다.
    단, 주기 안에 든 캐시가 있는데 force('최신으로 조회')가 아니면 계산을
    시작하지 않는다 — 화면은 캐시를 보여 준다. force이거나 캐시가
    오래됐을 때만 다시 계산한다.
    """
    if not force and is_cache_fresh(lib_id):
        return {'lib': lib_id, 'from_cache': True}
    state = read_state()
    if state.get('busy'):
        return state
    now = time.time()
    requested_at = state.get('requested_at') or 0
    finished_at = state.get('finished_at') or 0
    pending = requested_at > finished_at and now - requested_at < REQUEST_EXPIRE
    if pending:
        if str(state.get('lib')) == str(lib_id):
            return state
        # 다른 라이브러리의 조회가 대기 중이면 덮어쓰지 않고 거부한다 —
        # 덮어쓰면 그 라이브러리는 계산되지 않은 채 화면만 오래 기다리게 된다.
        # 화면은 다음 폴링 때 다시 요청하므로, 끝난 뒤 자동으로 이어진다
        return {'ret': 'fail',
                'msg': '다른 라이브러리의 조회가 대기 중입니다. 끝난 뒤에 다시 시도해 주세요.',
                'lib': state.get('lib'), 'requested_at': requested_at}
    # 새 요청을 받으면 이전 계산의 started_at을 지운다 — 공유 상태라
    # 낡은 값이 남아 있으면 완료 표시의 소요시간이 터무니없이 커진다
    # (휴지통 조회에서 102분으로 나온 전례, 2026-11-18).
    # 워커가 계산을 시작하면 compute_view가 새 started_at을 찍는다.
    return write_state(lib=lib_id, requested_at=now, progress=0, error=None,
                       cancel=False, cancelled=False, started_at=None)


def read_bulk():
    try:
        raw = P.ModelSetting.get(BULK_KEY)
        if raw:
            data = json.loads(raw)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def write_bulk(**kwargs):
    bulk = read_bulk()
    bulk.update(kwargs)
    try:
        P.ModelSetting.set(BULK_KEY, json.dumps(bulk))
    except Exception as e:
        logger.warning(f'[Meta] 전체 조회 상태 저장 실패: {e}')
    return bulk


def request_bulk(lib_ids):
    """전체 조회를 접수한다 — 실제 계산은 워커가 라이브러리 단위로 수행한다."""
    if read_bulk().get('active'):
        return {'ret': 'fail', 'msg': '이미 전체 조회가 진행 중입니다.'}
    libs = []
    for x in lib_ids or []:
        try:
            libs.append(int(x))
        except Exception:
            pass
    if not libs:
        return {'ret': 'fail', 'msg': '조회할 라이브러리가 없습니다.'}
    write_bulk(active=True, cancel=False, cancelled=False, libs=libs, done=0,
               errors=0, total=len(libs), current=None,
               started_at=time.time(), finished_at=None)
    return {'ret': 'success', 'total': len(libs)}


def cancel_bulk():
    """진행 중인 라이브러리까지만 마치고 멈추도록 취소 표시만 남긴다."""
    if read_bulk().get('active'):
        write_bulk(cancel=True)
    return {'ret': 'success'}


class _Cancelled(Exception):
    """조회 취소 — 진행률 콜백이 계산을 멈출 때 쓴다."""


def cancel_query():
    """한 라이브러리 조회 취소 — 계산 중이면 진행률 콜백이 멈추게 하고,
    접수만 된 상태면 워커가 시작할 때 계산하지 않고 중단 상태로 끝낸다.
    취소해도 기존 캐시는 건드리지 않는다 (화면은 기존 데이터를 쓴다)."""
    state = read_state()
    busy = state.get('busy')
    pending = (state.get('requested_at') or 0) > (state.get('finished_at') or 0)
    if busy or pending:
        write_state(cancel=True)
    return {'ret': 'success'}


def _row_dict(row):
    """캐시 행 배열([id, title, parent_title, year, flags, folder])을
    화면용 dict로. 이전 형식(폴더명 없는 5개짜리) 행도 받아들인다."""
    item_id, title, parent_title, year, flags = row[:5]
    folder = row[5] if len(row) > 5 else ''
    return {'id': item_id, 'title': title, 'parent_title': parent_title,
            'year': year, 'folder': folder or '',
            'has_code': bool(flags & db_support.META_FLAG_CODE),
            'has_summary': bool(flags & db_support.META_FLAG_SUMMARY),
            'has_year': bool(flags & db_support.META_FLAG_YEAR),
            'has_thumb': bool(flags & db_support.META_FLAG_THUMB),
            'missing': flags != db_support.META_FLAG_ALL}


def page_from_cache(data, page=1, per_page=30, search='', order='default',
                    scope='missing'):
    """계산이 끝난 캐시에서 범위·검색·정렬·페이지를 처리해 돌려준다 (웹에서 실행).

    scope: 'missing'(값이 하나라도 없는 대상만 — 기본값) · 'all'(전체 미디어) ·
    'nocode'(메타 없는 미디어만, 이전 범위). '메타 없는'은 대시보드 팝업과
    같은 기준이다 — 폴더 라이브러리는 표지 없음, 아니면 코드 없음이다.
    order: 'default'(기본, 등록 순) · 'name'(이름순) · 'missing'(메타 없는 순) ·
    'no_code'/'no_summary'/'no_year'/'no_thumb'(해당 값이 없는 것만, 이름순).
    """
    rows = list(data.get('items') or [])
    is_folder = bool(data.get('is_folder'))
    if scope == 'missing':
        # 값이 하나라도 없는 대상만 — 4가지 중 하나라도 빠졌으면 표시한다
        rows = [r for r in rows if r[4] != db_support.META_FLAG_ALL]
    elif scope == 'nocode':
        # 폴더 라이브러리는 표지 없음, 아니면 코드 없음이 '메타 없음'이다
        bit = db_support.META_FLAG_THUMB if is_folder else db_support.META_FLAG_CODE
        rows = [r for r in rows if not (r[4] & bit)]
    if search:
        needle = search.lower()
        rows = [r for r in rows
                if needle in ((r[2] or '') + ' ' + (r[1] or '') + ' '
                              + ((r[5] or '') if len(r) > 5 else '')).lower()]
    if order == 'name':
        rows.sort(key=lambda r: ((r[2] or ''), (r[1] or '')))
    elif order == 'missing':
        # 메타가 빠진 것부터 — 하나라도 빠졌으면 먼저, 그 안에서는 이름순
        rows.sort(key=lambda r: (0 if r[4] != db_support.META_FLAG_ALL else 1,
                                 (r[2] or ''), (r[1] or '')))
    elif order in ('no_code', 'no_summary', 'no_year', 'no_thumb'):
        # 해당 값이 없는 것만 보여 준다 (필터) — 그 안에서는 이름순.
        # 정렬만으로는 목록이 안 바뀌어 보여 필터가 안 된다고 느껴진다
        # (2026-11-18 사용자 지적). 드롭다운 이름이 '코드'·'표지'처럼
        # 필터처럼 읽히기도 해서, 아예 필터로 동작하게 바꾼다.
        bit = {'no_code': db_support.META_FLAG_CODE,
               'no_summary': db_support.META_FLAG_SUMMARY,
               'no_year': db_support.META_FLAG_YEAR,
               'no_thumb': db_support.META_FLAG_THUMB}[order]
        rows = [r for r in rows if not (r[4] & bit)]
        rows.sort(key=lambda r: ((r[2] or ''), (r[1] or '')))
    filtered = len(rows)
    pages = max(1, (filtered + per_page - 1) // per_page)
    page = max(1, min(page, pages))
    chunk = [_row_dict(r)
             for r in rows[(page - 1) * per_page: page * per_page]]
    return {'lib_type': data.get('lib_type'),
            'main_label': data.get('main_label'),
            'is_folder': is_folder,
            'summary': data.get('summary'),
            'total_items': filtered, 'page': page, 'pages': pages,
            'items': chunk}


def compute_view(lib_id):
    """메타 조회 계산 본체 — 워커에서만 실행한다. Relay로만 읽는다."""
    if not lib_id:
        write_state(finished_at=time.time())
        return
    if read_state().get('cancel'):
        # 접수만 된 채 취소된 경우 — 계산하지 않고 중단 상태로 끝낸다
        write_state(busy=False, progress=0, finished_at=time.time(),
                    cancel=False, cancelled=True, error=None)
        logger.info(f'[Meta] 메타 조회 취소됨(시작 전): 라이브러리 {lib_id}')
        return
    t0 = time.time()
    write_state(busy=True, progress=2, started_at=t0, error=None,
                cancelled=False)
    try:
        def cb(pct):
            if read_state().get('cancel'):
                raise _Cancelled()
            write_state(progress=max(2, min(98, int(pct))))
        if not db_support.db_ready():
            raise Exception('Relay가 꺼져 있어 메타 목록을 읽을 수 없습니다.')
        data = db_support.meta_compute(lib_id, progress_cb=cb)
        if not write_cache(lib_id, data):
            # 캐시를 저장하지 못했으면 완료 표시를 남기지 않고 오류로 끝낸다.
            # finished_at은 함께 찍는다 — 화면이 오류를 보려면 끝난 시각이
            # 필요하고, 안 찍으면 감시 루프가 같은 계산을 계속 다시 돌린다
            write_state(busy=False, progress=0, finished_at=time.time(),
                        error='조회 결과를 저장하지 못했습니다. 다시 조회해 주세요.')
            logger.warning(f'[Meta] 메타 조회 캐시 저장 실패로 오류 처리: 라이브러리 {lib_id}')
            return
        write_state(busy=False, progress=100, finished_at=time.time())
        logger.info(f"[Meta] 메타 조회 계산 완료 (워커): 라이브러리 {lib_id}, "
                    f"작품 {data['summary']['total']}개 · "
                    f"{round(time.time() - t0, 2)}초")
    except _Cancelled:
        # 취소됨 — 캐시는 건드리지 않고(기존 데이터 유지) 중단 상태로 끝낸다.
        write_state(busy=False, progress=0, finished_at=time.time(),
                    cancel=False, cancelled=True, error=None)
        logger.info(f'[Meta] 메타 조회 취소됨: 라이브러리 {lib_id}')
    except Exception as e:
        logger.warning(f'[Meta] 메타 조회 계산 실패(워커): {e}')
        write_state(busy=False, progress=0, finished_at=time.time(),
                    error=str(e))


class TaskMetaView:
    watch_thread = None

    @F.celery.task
    def start():
        if TaskMetaView.watch_thread is None:
            TaskMetaView.watch_thread = threading.Thread(
                target=TaskMetaView.watch_function, args=())
            TaskMetaView.watch_thread.daemon = True
            TaskMetaView.watch_thread.start()
            logger.info('SHYNI META VIEW 조회 워커 대기')

    @staticmethod
    def watch_function():
        while True:
            try:
                state = read_state()
                if state.get('busy'):
                    started_at = state.get('started_at') or 0
                    if started_at and time.time() - started_at > BUSY_EXPIRE:
                        logger.warning('[Meta] 중단된 메타 조회 계산 감지 — busy를 풀고 다시 계산합니다')
                        state = write_state(busy=False, progress=0)
                requested_at = state.get('requested_at') or 0
                finished_at = state.get('finished_at') or 0
                if requested_at > finished_at and not state.get('busy'):
                    compute_view(state.get('lib'))
                # 전체 조회 — 단일 조회가 비어 있을 때 라이브러리를 하나씩
                # 계산해 캐시를 채운다. 취소가 표시되면 진행 중이던 것까지만
                # 마치고 멈춘다 (계산 하나의 쿼리는 중간에 끊지 않는다).
                bulk = read_bulk()
                if bulk.get('active') and not read_state().get('busy'):
                    if bulk.get('cancel'):
                        write_bulk(active=False, cancelled=True,
                                   finished_at=time.time(), current=None)
                        logger.info('[Meta] 전체 조회 취소됨')
                    else:
                        libs = bulk.get('libs') or []
                        done = bulk.get('done') or 0
                        if done >= len(libs):
                            write_bulk(active=False, finished_at=time.time(),
                                       current=None)
                            logger.info(f'[Meta] 전체 조회 완료: {len(libs)}개 라이브러리')
                        else:
                            write_bulk(current=libs[done])
                            compute_view(libs[done])
                            errors = bulk.get('errors') or 0
                            if read_state().get('error'):
                                errors += 1
                            write_bulk(done=done + 1, errors=errors, current=None)
            except Exception as e:
                logger.error(f'[Meta] 메타 조회 워커 오류: {e}')
            time.sleep(WATCH_INTERVAL)
