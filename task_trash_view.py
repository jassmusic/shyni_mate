# -*- coding: utf-8 -*-
"""휴지통 조회 계산을 Celery 워커가 전담한다.

조회 한 번에 릴레이 DB에서 파일 전체와 항목 계층을 읽어 작품 단위로 묶는
계산이 수 초에서 수십 초까지 걸려, 웹 프로세스에서 직접 하면 화면이 멈춘
느낌을 준다. 현황 통계(task_status)와 같은 방식이다: 웹은 ModelSetting에
'조회 요청'만 남기고 진행률을 폴링하고, 워커가 계산해 캐시에 저장하면
웹은 캐시를 검색·정렬·페이지 단위로 잘라 돌려준다.

상태 공유 키:
- trash_view_state : {'lib', 'busy', 'progress', 'requested_at', 'started_at',
                      'finished_at', 'error', 'cancel', 'cancelled'}
  (cancel: 조회 중 취소 요청, cancelled: 취소로 끝난 계산)
- trash_view_cache : {'libs': {라이브러리키: {'at', 'data'}}} — 라이브러리별
  캐시를 전부 보관한다 (캐시 1개는 수 KB 수준이라 전부 둬도 부담이 없고,
  그래야 어느 라이브러리를 골라도 캐시로 바로 보여 줄 수 있다).
"""
import json
import threading
import time

from .setup import *
from . import db_support

logger = P.logger

STATE_KEY = 'trash_view_state'
BULK_KEY = 'trash_bulk_state'
CACHE_KEY = 'trash_view_cache'
WATCH_INTERVAL = 2
REQUEST_EXPIRE = 15 * 60
# 계산 중(busy)인 채로 이 시간이 지나면 중단된 것으로 본다 — 계산 도중
# ff가 재시작되면 busy가 남아 이후 조회가 전부 막히는 것을 막는다.
BUSY_EXPIRE = 10 * 60


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
        logger.warning(f'[Trash] 조회 상태 저장 실패: {e}')
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
    """캐시를 {라이브러리키: {'at', 'data'}} 형태로 정규화해 돌려준다.

    구 형식({'lib', 'at', 'data'})도 읽어 새 형식으로 취급한다.
    """
    cache = read_cache() or {}
    libs = cache.get('libs')
    if isinstance(libs, dict):
        return libs
    if cache.get('lib') is not None and isinstance(cache.get('data'), dict):
        return {str(cache['lib']): {'at': cache.get('at') or 0,
                                     'data': cache['data']}}
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
        logger.warning(f'[Trash] 휴지통 조회 캐시 저장 실패: {e}')
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


def files_for(lib_id, root_id):
    data = cached_data(lib_id)
    if not data:
        return None
    return (data.get('files') or {}).get(str(root_id))


def clear_cache(lib_id=None):
    if lib_id is None:
        try:
            P.ModelSetting.set(CACHE_KEY, '')
        except Exception as e:
            logger.warning(f'[Trash] 휴지통 조회 캐시 지우기 실패: {e}')
        return
    libs = _cache_libs()
    if str(lib_id) in libs:
        del libs[str(lib_id)]
        try:
            P.ModelSetting.set(CACHE_KEY, json.dumps({'libs': libs},
                                                     ensure_ascii=False))
        except Exception as e:
            logger.warning(f'[Trash] 휴지통 조회 캐시 지우기 실패: {e}')


def request_view(lib_id, force=False):
    """웹이 조회를 요청한다 — 실제 계산은 워커가 수행한다.

    이미 계산 중이면 그 상태를 그대로 돌려주고, 같은 라이브러리의 요청이
    접수만 된 채 15분이 지나면 워커 무응답으로 보고 다시 요청할 수 있게 한다.
    다른 라이브러리의 요청이 대기 중이면 덮어쓰지 않고 거부한다.
    단, 캐시가 있는데 force('최신으로 조회')가 아니면 계산을 시작하지
    않는다 — 캐시 확인보다 조회가 먼저 눌린 경우의 방어로, 화면은 캐시를
    보여 준다. force일 때만 끝난 계산이 있어도 다시 계산한다.
    """
    if not force and cached_data(lib_id):
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
    return write_state(lib=lib_id, requested_at=now, progress=0, error=None,
                       cancel=False, cancelled=False)


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
        logger.warning(f'[Trash] 전체 조회 상태 저장 실패: {e}')
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


def page_from_cache(data, page=1, per_page=30, search='', order='old'):
    """계산이 끝난 캐시에서 검색·정렬·페이지를 처리해 돌려준다 (웹에서 실행)."""
    rows = list(data.get('groups') or [])
    if search:
        needle = search.lower()
        rows = [g for g in rows
                if needle in (g['parent_title'] + ' ' + g['title']).lower()
                or needle in g['folder'].lower()]
    if order == 'new':
        rows.sort(key=lambda g: g.get('_last') or 0, reverse=True)
    elif order == 'files':
        rows.sort(key=lambda g: (-g['file_count'], g['title']))
    elif order == 'name':
        rows.sort(key=lambda g: (g['parent_title'], g['title']))
    else:
        rows.sort(key=lambda g: g.get('_first') or 0)
    filtered = len(rows)
    pages = max(1, (filtered + per_page - 1) // per_page)
    page = max(1, min(page, pages))
    chunk = [{k: v for k, v in g.items() if not k.startswith('_')}
             for g in rows[(page - 1) * per_page: page * per_page]]
    return {'lib_type': data.get('lib_type'),
            'main_label': data.get('main_label'),
            'retention_days': data.get('retention_days'),
            'summary': data.get('summary'),
            'total_groups': filtered, 'page': page, 'pages': pages,
            'groups': chunk}


def compute_view(lib_id):
    """휴지통 조회 계산 본체 — 워커에서만 실행한다."""
    if not lib_id:
        write_state(finished_at=time.time())
        return
    if read_state().get('cancel'):
        # 접수만 된 채 취소된 경우 — 계산하지 않고 중단 상태로 끝낸다
        write_state(busy=False, progress=0, finished_at=time.time(),
                    cancel=False, cancelled=True, error=None)
        logger.info(f'[Trash] 휴지통 조회 취소됨(시작 전): 라이브러리 {lib_id}')
        return
    t0 = time.time()
    write_state(busy=True, progress=2, started_at=t0, error=None,
                cancelled=False)
    try:
        def cb(pct):
            if read_state().get('cancel'):
                raise _Cancelled()
            write_state(progress=max(2, min(98, int(pct))))
        data = db_support.trash_compute(lib_id, progress_cb=cb)
        if not write_cache(lib_id, data):
            # 캐시를 저장하지 못했으면 완료 표시를 남기지 않고 오류로 끝낸다.
            # finished_at은 함께 찍는다 — 화면이 오류를 보려면 끝난 시각이
            # 필요하고, 안 찍으면 감시 루프가 같은 계산을 계속 다시 돌린다
            write_state(busy=False, progress=0, finished_at=time.time(),
                        error='조회 결과를 저장하지 못했습니다. 다시 조회해 주세요.')
            logger.warning(f'[Trash] 휴지통 조회 캐시 저장 실패로 오류 처리: 라이브러리 {lib_id}')
            return
        # 현황·건강도의 휴지통 개수 캐시에도 이 라이브러리 개수를 바로 반영한다
        try:
            from .task_status import set_trash_count
            set_trash_count(lib_id, (data.get('summary') or {}).get('files') or 0)
        except Exception as e:
            logger.warning(f'[Trash] 휴지통 개수 반영 실패: {e}')
        write_state(busy=False, progress=100, finished_at=time.time())
        logger.info(f"[Trash] 휴지통 조회 계산 완료 (워커): 라이브러리 {lib_id}, "
                    f"파일 {data['summary']['files']}개 · "
                    f"{round(time.time() - t0, 2)}초")
    except _Cancelled:
        # 취소됨 — 캐시는 건드리지 않고(기존 데이터 유지) 중단 상태로 끝낸다.
        # 진행 중이던 단계의 DB 읽기는 콜백까지 마치고 멈춘다.
        write_state(busy=False, progress=0, finished_at=time.time(),
                    cancel=False, cancelled=True, error=None)
        logger.info(f'[Trash] 휴지통 조회 취소됨: 라이브러리 {lib_id}')
    except Exception as e:
        logger.warning(f'[Trash] 휴지통 조회 계산 실패(워커): {e}')
        write_state(busy=False, progress=0, finished_at=time.time(),
                    error=str(e))


class TaskTrashView:
    watch_thread = None

    @F.celery.task
    def start():
        if TaskTrashView.watch_thread is None:
            TaskTrashView.watch_thread = threading.Thread(
                target=TaskTrashView.watch_function, args=())
            TaskTrashView.watch_thread.daemon = True
            TaskTrashView.watch_thread.start()
            logger.info('SHYNI TRASH VIEW 조회 워커 대기')

    @staticmethod
    def watch_function():
        while True:
            try:
                state = read_state()
                if state.get('busy'):
                    started_at = state.get('started_at') or 0
                    if started_at and time.time() - started_at > BUSY_EXPIRE:
                        logger.warning('[Trash] 중단된 휴지통 조회 계산 감지 — busy를 풀고 다시 계산합니다')
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
                        logger.info('[Trash] 전체 조회 취소됨')
                    else:
                        libs = bulk.get('libs') or []
                        done = bulk.get('done') or 0
                        if done >= len(libs):
                            write_bulk(active=False, finished_at=time.time(),
                                       current=None)
                            logger.info(f'[Trash] 전체 조회 완료: {len(libs)}개 라이브러리')
                        else:
                            write_bulk(current=libs[done])
                            compute_view(libs[done])
                            errors = bulk.get('errors') or 0
                            if read_state().get('error'):
                                errors += 1
                            write_bulk(done=done + 1, errors=errors, current=None)
            except Exception as e:
                logger.error(f'[Trash] 휴지통 조회 워커 오류: {e}')
            time.sleep(WATCH_INTERVAL)
