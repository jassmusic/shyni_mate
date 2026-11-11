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
WATCH_INTERVAL = 5
REQUEST_EXPIRE = 15 * 60
# 계산 중(busy)인 채로 이 시간(초)이 지나면 중단된 것으로 본다 — 정상 계산은
# 2분을 넘기지 않는다. 계산 도중 ff가 재시작되면 busy가 영원히 남아 이후
# 계산이 전부 막히던 문제(2026-11-11 실측)의 복구 기준이다.
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
    except Exception:
        pass
    return state


def request_stats(force=False):
    """웹 프로세스가 계산을 요청한다 — 실제 계산은 워커가 수행한다.

    이미 계산 중이거나, 접수된 요청이 15분 이내면 중복 요청하지 않는다.
    15분이 지나도 처리되지 않은 요청은 워커 무응답으로 보고 다시 요청할 수
    있게 하며, 그때까지 웹은 마지막 저장 결과를 계속 보여 준다."""
    state = read_state()
    if state.get('busy'):
        return state
    now = time.time()
    requested_at = state.get('requested_at') or 0
    finished_at = state.get('finished_at') or 0
    if requested_at > finished_at and now - requested_at < REQUEST_EXPIRE:
        return state
    return write_state(requested_at=now, progress=0, error=None)


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
    except Exception:
        pass


def trash_counts_for_stats():
    """통계 계산용 휴지통 개수 — 설정 주기 안이면 캐시를 쓰고, 지났으면
    새로 읽어 캐시에 넣는다. 주기가 0이면 매번 새로 읽는다."""
    try:
        hours = int(P.ModelSetting.get('base_trash_counts_refresh_hours') or 12)
    except Exception:
        hours = 12
    cache = read_trash_cache()
    if hours > 0 and cache and (time.time() - (cache.get('at') or 0) <= hours * 3600):
        return {int(k): v for k, v in cache['data'].items()}
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
        cache = _get_json(TRASH_COUNTS_KEY)
        if cache:
            cache['at'] = 0
            P.ModelSetting.set(TRASH_COUNTS_KEY, json.dumps(cache))
    except Exception:
        pass


def compute_stats():
    """DB 통계 계산 본체 — 워커에서만 실행한다 (기존 웹 스레드 계산을 옮긴 것)."""
    t0 = time.time()
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
        except Exception:
            pass
        write_state(busy=False, progress=100, finished_at=time.time())
        logger.info(f"[Status] DB 통계 계산 완료 (워커): {data['elapsed']}초")
    except Exception as e:
        logger.warning(f'[Status] DB 통계 계산 실패(워커): {e}')
        write_state(busy=False, progress=0, finished_at=time.time(), error=str(e))


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
            except Exception as e:
                logger.error(f'[Status] 통계 워커 오류: {e}')
            time.sleep(WATCH_INTERVAL)
