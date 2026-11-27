# -*- coding: utf-8 -*-
"""현황·건강도 DB 통계 계산을 Celery 워커가 전담한다.

기존에는 웹 프로세스 안의 스레드가 계산해서, 계산(수십 초~100초) 중에는
웹 전체가 무거워지고 ff 웹이 재시작되면 계산이 중간에 죽었다.
이제 웹은 ModelSetting에 '계산 요청'만 남기고 저장된 결과만 읽으며,
실제 계산은 이 워커의 감시 루프가 수행해 결과를 같은 자리에 저장한다.

상태 공유 키:
- status_db_stats_cache : 계산 결과 {'at': 시각, 'data': {...}} (기존과 동일)
- status_db_stats_state : {'busy', 'progress', 'requested_at', 'started_at',
                           'finished_at', 'error'}
- status_list_cache : 대시보드 목록 payload {'at': 시각, 'data': {...}}
- status_list_state : 목록 재구축 상태 (위와 같은 형태)

대시보드 목록 재구축도 이 워커가 전담한다 (2026-11-15 — 웹이 첫 접속 때
목록을 동기 재구축하느라 FF 웹(gevent 단일 루프)이 멈추던 문제의 수정).
웹은 저장된 복사본을 즉시 보여 주고 재구축 요청만 남기며, 이 워커의 감시
루프가 요청을 받아 목록을 새로 만들어 같은 자리에 저장한다.
"""
import json
import threading
import time

from .setup import *
from . import db_support

name = 'status'
logger = P.logger

STATE_KEY = 'status_db_stats_state'
CACHE_KEY = 'status_db_stats_cache'
# 마지막 DB 통계 계산 실패 기록 {'at': 시각, 'error': 메시지} — state의
# error는 새 요청이 들어오면 지워져서, 실패 상태를 화면에 알리는 용도로
# 따로 남긴다. 계산이 성공하면 비운다.
LAST_ERROR_KEY = 'status_db_stats_last_error'
LIST_STATE_KEY = 'status_list_state'
LIST_CACHE_KEY = 'status_list_cache'
WATCH_INTERVAL = 10
REQUEST_EXPIRE = 15 * 60
# 계산 중(busy)인 채로 이 시간(초)이 지나면 중단된 것으로 본다 — 정상 계산은
# 2분을 넘기지 않는다. 계산 도중 ff가 재시작되면 busy가 영원히 남아 이후
# 계산이 전부 막히던 문제(2026-11-11 실측)의 복구 기준이다.
BUSY_EXPIRE = 10 * 60
# 통계 캐시가 이보다 오래되면 워커가 주기적으로 다시 계산한다 — 대시보드를
# 열 때마다 재계산을 트리거하던 기존 방식은 서버를 무겁게 만들었다.
# (스캔·비우기 완료 시에는 사건 트리거로 이미 재계산이 요청된다.)
STATS_MAX_AGE = 6 * 3600


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
        logger.warning(f'[Status] 상태 저장 실패: {e}')
    return state


def read_last_error():
    """마지막 DB 통계 계산 실패 기록 — 없으면 None."""
    try:
        raw = P.ModelSetting.get(LAST_ERROR_KEY)
        if raw:
            d = json.loads(raw)
            if isinstance(d, dict) and d.get('error'):
                return d
    except Exception:
        pass
    return None


def write_last_error(err):
    try:
        P.ModelSetting.set(LAST_ERROR_KEY, json.dumps(
            {'at': time.time(), 'error': str(err)}))
    except Exception as e:
        logger.warning(f'[Status] 마지막 실패 기록 저장 실패: {e}')


def clear_last_error():
    try:
        P.ModelSetting.set(LAST_ERROR_KEY, '')
    except Exception:
        pass


def request_stats(force=False):
    """웹 프로세스가 계산을 요청한다 — 실제 계산은 워커가 수행한다.

    접수된 요청이 15분 이내면 중복 요청하지 않는다. 15분이 지나도 처리되지
    않은 요청은 워커 무응답으로 보고 다시 요청할 수 있게 하며, 그때까지 웹은
    마지막 저장 결과를 계속 보여 준다.
    계산 중에도 요청은 남긴다 — 그 요청이 없으면 비우기처럼 계산 도중에
    바뀐 내용이 다음 재계산까지 반영되지 않는다. compute_stats가 끝나면서
    계산 중에 들어온 요청을 finished_at 뒤에 다시 남겨 워커가 다시 계산한다.
    Relay가 꺼져 있으면 요청을 남기지 않는다 — 스캔 완료 등으로 남긴 요청이
    Relay OFF 상태에서도 계산을 트리거해 DB 통계 캐시를 다시 만들어버리던
    문제(2026-11-17 실측)의 수정. 계산 단계(compute_stats)에도 같은 확인이
    있어, 이미 남겨진 요청도 계산되지 않는다."""
    try:
        if not db_support.db_ready():
            return read_state()
    except Exception:
        return read_state()
    state = read_state()
    now = time.time()
    requested_at = state.get('requested_at') or 0
    finished_at = state.get('finished_at') or 0
    if state.get('busy'):
        # 계산 중 — 이미 계산 시작 뒤에 남긴 요청이 있으면 중복하지 않고,
        # 없으면 요청만 남긴다 (진행률·busy는 건드리지 않는다).
        started_at = state.get('started_at') or 0
        if requested_at > started_at and now - requested_at < REQUEST_EXPIRE:
            return state
        return write_state(requested_at=now)
    if requested_at > finished_at and now - requested_at < REQUEST_EXPIRE:
        return state
    return write_state(requested_at=now, progress=0, error=None)


def _requeue_if_requested_during(req_at):
    """계산 중에 들어온 새 요청이 있으면 finished_at 뒤에 다시 남긴다.

    계산이 끝나면서 finished_at을 찍으면, 계산 중에 남긴 요청이 그보다
    먼저 찍혀 워커가 '이미 처리된 요청'으로 보고 넘겨버린다. 요청 시각을
    finished_at보다 뒤로 다시 찍어 다음 감시 주기에 다시 계산하게 한다."""
    state = read_state()
    if (state.get('requested_at') or 0) > req_at:
        finished_at = state.get('finished_at') or 0
        write_state(requested_at=max(time.time(), finished_at + 0.001))


# ---------- 대시보드 목록 재구축 (웹은 요청만 남기고 워커가 수행) ----------

def read_list_state():
    try:
        raw = P.ModelSetting.get(LIST_STATE_KEY)
        if raw:
            state = json.loads(raw)
            if isinstance(state, dict):
                return state
    except Exception:
        pass
    return {}


def write_list_state(**kwargs):
    state = read_list_state()
    state.update(kwargs)
    try:
        P.ModelSetting.set(LIST_STATE_KEY, json.dumps(state))
    except Exception as e:
        logger.warning(f'[Status] 목록 상태 저장 실패: {e}')
    return state


def request_list_rebuild(force=False):
    """웹 프로세스가 목록 재구축을 요청한다 — 실제 재구축은 워커가 수행한다.

    request_stats와 같은 중복 방지 규칙이다. 단 force(수동 새로고침)는
    대기 중인 요청이 있어도 새로 남긴다 — 사용자가 눌렀는데 낡은
    복사본만 보이는 일이 없게 하기 위해서다."""
    state = read_list_state()
    now = time.time()
    requested_at = state.get('requested_at') or 0
    finished_at = state.get('finished_at') or 0
    if state.get('busy'):
        started_at = state.get('started_at') or 0
        if not force and requested_at > started_at and now - requested_at < REQUEST_EXPIRE:
            return state
        return write_list_state(requested_at=now)
    if not force and requested_at > finished_at and now - requested_at < REQUEST_EXPIRE:
        return state
    return write_list_state(requested_at=now, progress=0, error=None)


def _requeue_list_if_requested_during(req_at):
    """재구축 중에 들어온 새 요청이 있으면 finished_at 뒤에 다시 남긴다."""
    state = read_list_state()
    if (state.get('requested_at') or 0) > req_at:
        finished_at = state.get('finished_at') or 0
        write_list_state(requested_at=max(time.time(), finished_at + 0.001))


def compute_list():
    """대시보드 목록 재구축 본체 — 워커에서만 실행한다.

    목록 재구축(섹션 API·스캔 이력·건강·용량)은 수 초가 걸리고, 그중
    로그 파싱은 CPU 작업이라 웹 프로세스에서 하면 FF 웹 전체가 멈춘다.
    그래서 웹은 캐시만 읽고, 재구축은 이 워커가 수행해 결과를 저장한다.
    실패해도 finished_at을 찍어 요청을 소진한다 — 찍지 않으면 감시
    루프가 5초마다 실패하는 재구축을 반복하게 된다. 다음 대시보드
    접속 때 다시 요청되므로, 실패가 영원히 남지는 않는다."""
    t0 = time.time()
    req_at = read_list_state().get('requested_at') or 0
    write_list_state(busy=True, progress=0, started_at=t0, error=None)
    try:
        from .mod_status import ModuleStatus
        data = ModuleStatus.build_list_data()
        if data is None:
            write_list_state(busy=False, progress=0, finished_at=time.time(),
                             error='목록을 만들지 못했습니다 (shyni 연결·설정 확인 필요)')
            return
        try:
            P.ModelSetting.set(LIST_CACHE_KEY,
                               json.dumps({'at': time.time(), 'data': data}))
        except Exception as e:
            logger.warning(f'[Status] 대시보드 목록 캐시 저장 실패: {e}')
            write_list_state(busy=False, progress=0, finished_at=time.time(),
                             error=f'목록 저장에 실패했습니다: {e}')
            return
        write_list_state(busy=False, progress=100, finished_at=time.time())
        _requeue_list_if_requested_during(req_at)
        logger.info(f"[Status] 대시보드 목록 재구축 완료 (워커): {time.time() - t0:.2f}초")
    except Exception as e:
        logger.warning(f'[Status] 대시보드 목록 재구축 실패(워커): {e}')
        write_list_state(busy=False, progress=0, finished_at=time.time(), error=str(e))
        _requeue_list_if_requested_during(req_at)


def _stats_cache_age():
    """DB 통계 캐시의 나이(초) — 없으면 None."""
    try:
        raw = P.ModelSetting.get(CACHE_KEY)
        if raw:
            saved = json.loads(raw)
            if isinstance(saved, dict) and saved.get('data'):
                return time.time() - (saved.get('at') or 0)
    except Exception:
        pass
    return None


TRASH_CACHE_KEY = 'status_trash_counts_cache'
_trash_refresh_lock = threading.Lock()
_trash_refresh_last = [0.0]


def read_trash_cache():
    try:
        raw = P.ModelSetting.get(TRASH_CACHE_KEY)
        d = json.loads(raw) if raw else {}
        if isinstance(d, dict) and isinstance(d.get('data'), dict):
            return d
    except Exception:
        pass
    return {}


def write_trash_cache(counts, at=None):
    try:
        P.ModelSetting.set(TRASH_CACHE_KEY, json.dumps(
            {'at': time.time() if at is None else at,
             'data': {str(k): int(v) for k, v in counts.items()}}))
    except Exception as e:
        logger.warning(f'[Status] 휴지통 개수 캐시 저장 실패: {e}')


def _trash_counts_via_api():
    """관리자 API로 라이브러리별 휴지통 개수 읽기. 실패하면 None.
    라이브러리 목록은 compat API(섹션 목록)에서 얻는다 — Relay와 무관."""
    try:
        from .mod_status import ModuleStatus
        sections, _err = ModuleStatus._sections()
        if not sections:
            return None
        from . import shyni_admin
        return shyni_admin.api_trash_counts([s['key'] for s in sections])
    except Exception as e:
        logger.warning(f'[Status] 휴지통 개수 API 읽기 실패: {e}')
        return None


def trash_counts_for_stats():
    """통계 계산용 휴지통 개수 — 설정 주기 안이면 캐시를 쓰고, 지났으면
    새로 읽어 캐시에 넣는다. 주기가 0이면 매번 새로 읽는다.
    관리자 API 우선, 실패하면 Relay로 폴백한다 (Step4)."""
    try:
        hours = int(P.ModelSetting.get('base_trash_counts_refresh_hours') or 12)
    except Exception:
        hours = 12
    cache = read_trash_cache()
    if hours > 0 and cache and (time.time() - (cache.get('at') or 0) <= hours * 3600):
        return {int(k): v for k, v in cache['data'].items()}
    counts = _trash_counts_via_api()
    if counts is None:
        counts = db_support.trash_counts()
    write_trash_cache(counts)
    return counts


def set_trash_count(lib_id, count):
    """한 라이브러리의 휴지통 개수만 캐시에 반영한다 (휴지통 조회 계산이
    끝났을 때 — 그 라이브러리 개수는 이미 정확히 알아낸 상태다).
    전체 갱신 시각(at)은 유지해 다른 라이브러리의 주기를 앞당기지 않는다."""
    cache = read_trash_cache()
    data = dict(cache.get('data') or {})
    data[str(lib_id)] = int(count)
    write_trash_cache(data, at=cache.get('at') or 0)


def refresh_trash_counts_async():
    """휴지통 개수를 백그라운드에서 새로 읽어 캐시에 넣는다 (비우기 직후용).
    실행 중이거나 방금(60초 안) 읽었으면 건너뛴다."""
    def _run():
        if not _trash_refresh_lock.acquire(False):
            return
        try:
            counts = _trash_counts_via_api()
            if counts is None:
                counts = db_support.trash_counts()
            write_trash_cache(counts)
            _trash_refresh_last[0] = time.time()
            logger.info('[Status] 휴지통 개수 캐시를 새로 읽었습니다')
        except Exception as e:
            logger.warning(f'[Status] 휴지통 개수 새로 읽기 실패: {e}')
        finally:
            _trash_refresh_lock.release()
    if time.time() - _trash_refresh_last[0] < 60:
        return
    threading.Thread(target=_run, daemon=True).start()


def mark_trash_counts_stale():
    """개수 캐시를 '오래됨'으로만 표시한다 — 다음 통계 계산 때 한 번에 새로 읽는다.

    스캔 완료처럼 개수가 바뀌었을 수 있는 지점에서 무거운 전체 읽기를
    그때마다 돌리지 않기 위한 표시. 데이터는 그대로 두어 화면에는
    직전 개수가 계속 보인다."""
    try:
        cache = read_trash_cache()
        write_trash_cache(cache.get('data') or {}, at=0)
    except Exception as e:
        logger.warning(f'[Status] 휴지통 개수 캐시 오래됨 표시 실패: {e}')


def compute_stats():
    """DB 통계 계산 본체 — 워커에서만 실행한다 (기존 웹 스레드 계산을 옮긴 것)."""
    # Relay가 꺼져 있으면 계산하지 않는다 — 요청을 남기는 쪽(request_stats)
    # 에도 같은 확인이 있지만, 수정 전에 남겨진 요청이나 다른 경로의 요청이
    # 계산 단계까지 오는 경우를 막는다. 요청은 소진(finished_at) 처리해
    # 감시 루프가 5초마다 다시 부르며 기록을 채우지 않게 한다.
    try:
        relay_on = db_support.db_ready()
    except Exception:
        relay_on = False
    if not relay_on:
        logger.info('[Status] Relay가 꺼져 있어 DB 통계 계산을 건너뜁니다')
        write_state(busy=False, progress=0, finished_at=time.time())
        return
    t0 = time.time()
    req_at = read_state().get('requested_at') or 0
    write_state(busy=True, progress=5, started_at=t0, error=None)
    try:
        counts = trash_counts_for_stats()
        write_state(progress=15)

        def _lib_cb(frac):
            write_state(progress=int(15 + frac * 60))

        libs = db_support.library_stats(trash_map=counts, progress_cb=_lib_cb)
        write_state(progress=80)
        dash = db_support.dashboard_stats(libs)
        write_state(progress=90)
        data = {'libs': libs, 'dash': dash,
                'elapsed': round(time.time() - t0, 2)}
        try:
            P.ModelSetting.set(CACHE_KEY,
                               json.dumps({'at': time.time(), 'data': data}))
        except Exception as e:
            # 저장이 안 됐는데 완료로 표시하면 화면에 '불러오기 완료'가 뜨고
            # 옛 통계가 그대로 보인다 — error를 남기고 finished_at은 찍지 않아
            # 다음 감시 주기에 다시 계산하게 한다.
            logger.warning(f'[Status] DB 통계 캐시 저장 실패: {e}')
            write_state(busy=False, progress=0, error=f'통계 저장에 실패했습니다: {e}')
            return
        write_state(busy=False, progress=100, finished_at=time.time())
        clear_last_error()
        _requeue_if_requested_during(req_at)
        logger.info(f"[Status] DB 통계 계산 완료 (워커): {data['elapsed']}초")
    except Exception as e:
        logger.warning(f'[Status] DB 통계 계산 실패(워커): {e}')
        write_state(busy=False, progress=0, finished_at=time.time(), error=str(e))
        write_last_error(e)
        _requeue_if_requested_during(req_at)


class TaskStatus:
    watch_thread = None

    @F.celery.task
    def start():
        if TaskStatus.watch_thread is None:
            TaskStatus.watch_thread = threading.Thread(
                target=TaskStatus.watch_function, args=())
            TaskStatus.watch_thread.daemon = True
            TaskStatus.watch_thread.start()
            logger.info('SHYNI STATUS 통계 워커 대기')

    @staticmethod
    def watch_function():
        while True:
            try:
                state = read_state()
                # busy인데 시작 후 BUSY_EXPIRE가 지났으면, 계산하던 프로세스가
                # 재시작 등으로 사라진 것이다 — busy를 풀어 남은 요청이 다시
                # 계산되게 한다 (재부팅 후에도 작업이 이어져야 한다는 원칙).
                if state.get('busy'):
                    started_at = state.get('started_at') or 0
                    if started_at and time.time() - started_at > BUSY_EXPIRE:
                        logger.warning('[Status] 중단된 통계 계산 감지 — busy를 풀고 다시 계산합니다')
                        state = write_state(busy=False, progress=0)
                requested_at = state.get('requested_at') or 0
                finished_at = state.get('finished_at') or 0
                if requested_at > finished_at and not state.get('busy'):
                    compute_stats()
                else:
                    # 주기 재계산 — 대시보드를 열 때마다 트리거하던 기존
                    # 방식 대신, 워커가 캐시 나이를 보고 스스로 계산한다.
                    # Relay가 꺼져 있으면 계산해도 실패하므로 요청하지 않는다.
                    age = _stats_cache_age()
                    if age is not None and age >= STATS_MAX_AGE \
                            and not state.get('busy') \
                            and not (requested_at > finished_at):
                        try:
                            if db_support.db_ready():
                                request_stats()
                        except Exception:
                            pass
                # 대시보드 목록 재구축 — 웹이 남긴 요청을 받아 처리한다
                # (통계 계산과 같은 busy 만료·요청 소진 규칙).
                list_state = read_list_state()
                if list_state.get('busy'):
                    started_at = list_state.get('started_at') or 0
                    if started_at and time.time() - started_at > BUSY_EXPIRE:
                        logger.warning('[Status] 중단된 목록 재구축 감지 — busy를 풀고 다시 만듭니다')
                        list_state = write_list_state(busy=False, progress=0)
                list_requested = list_state.get('requested_at') or 0
                list_finished = list_state.get('finished_at') or 0
                if list_requested > list_finished and not list_state.get('busy'):
                    compute_list()
            except Exception as e:
                logger.error(f'[Status] 통계 워커 오류: {e}')
            time.sleep(WATCH_INTERVAL)
