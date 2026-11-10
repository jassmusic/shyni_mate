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


def compute_stats():
    """DB 통계 계산 본체 — 워커에서만 실행한다 (기존 웹 스레드 계산을 옮긴 것)."""
    t0 = time.time()
    write_state(busy=True, progress=5, started_at=t0, error=None)
    try:
        libs = db_support.library_stats()
        write_state(progress=55)
        dash = db_support.dashboard_stats(libs)
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
                requested_at = state.get('requested_at') or 0
                finished_at = state.get('finished_at') or 0
                if requested_at > finished_at and not state.get('busy'):
                    compute_stats()
            except Exception as e:
                logger.error(f'[Status] 통계 워커 오류: {e}')
            time.sleep(WATCH_INTERVAL)
