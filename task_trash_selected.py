# -*- coding: utf-8 -*-
"""휴지통 조회에서 작품을 선택해 실행하는 동작(새로고침·스캔·새로고침 후
스캔)을 Celery 워커가 전담한다.

선택 비우기는 관리자 세션이 필요해 이 범위에 넣지 않는다 (2026-11-11
사용자 결정 — 새로고침·스캔 3종만 먼저 만든다). 웹 프로세스에서 스레드로
돌리면 ff가 재시작될 때 실행이 끊기고 상태도 사라지므로, 휴지통 조회
(task_trash_view)와 같은 방식이다: 웹은 ModelSetting에 '실행 요청'만
남기고 진행을 폴링하고, 워커의 감시 루프가 요청을 집어 폴더별로 실행하며
진행·결과를 같은 자리에 기록한다. 재부팅으로 워커가 죽어도 busy가 오래
남으면 풀고 요청을 처음부터 다시 실행한다 (작업이 빠르게 재개돼야 한다는
원칙 — 새로고침·스캔은 다시 실행해도 안전한 동작이다).

상태 공유 키:
- trash_sel_state : {'busy', 'lib', 'mode', 'folders', 'total', 'done',
                     'current', 'results', 'requested_at', 'started_at',
                     'finished_at', 'error'}
  results는 폴더별 {'folder', 'ok', 'error', 'job_id'} 목록이다.

동작 의미:
- refresh      : 대상 폴더에 rclone RC vfs/refresh(재귀) — 스캔 항목
                 파일체크가 쓰는 extensions.vfs_refresh와 같은 헬퍼.
- scan         : 그 라이브러리에 폴더 경로를 지정한 compat 부분 스캔 —
                 ShyniBackend.request_scan과 같은 호출(POST
                 /library/sections/{id}/refresh, 본문에 path·mode=add·wait).
                 job을 끝까지 기다리지 않고 전송 성공을 성공으로 본다
                 (shyni가 라이브러리별로 스캔을 직렬화해 순서대로 처리한다).
- refresh_scan : 폴더마다 새로고침이 끝난 뒤 스캔을 보낸다. 새로고침이
                 실패한 폴더는 스캔을 보내지 않는다.
폴더 하나가 실패해도 나머지는 계속 실행한다.
"""
import json
import threading
import time

import requests

from .setup import *
from .extensions import vfs_refresh
from .shyni_backend import ShyniBackend

logger = P.logger

STATE_KEY = 'trash_sel_state'
WATCH_INTERVAL = 2
REQUEST_EXPIRE = 15 * 60
# 실행 중(busy)인 채로 이 시간이 지나면 중단된 것으로 본다 — 실행은 이
# 워커의 감시 루프가 동기적으로 수행하므로, 루프가 비어 있는데 busy가
# 오래 남아 있으면 실행하던 프로세스가 재시작 등으로 사라진 것이다.
BUSY_EXPIRE = 10 * 60
MAX_FOLDERS = 100
MODES = ('refresh', 'scan', 'refresh_scan')
MODE_LABEL = {'refresh': '새로고침', 'scan': '스캔',
              'refresh_scan': '새로고침 후 스캔'}


def read_sel_state():
    try:
        raw = P.ModelSetting.get(STATE_KEY)
        if raw:
            state = json.loads(raw)
            if isinstance(state, dict):
                return state
    except Exception:
        pass
    return {}


def write_sel_state(**kwargs):
    state = read_sel_state()
    state.update(kwargs)
    try:
        P.ModelSetting.set(STATE_KEY, json.dumps(state, ensure_ascii=False))
    except Exception as e:
        logger.warning(f'[Trash] 선택 실행 상태 저장 실패: {e}')
    return state


def validate_request(lib_id, mode, folders):
    """실행 요청 값 검증. 문제 없으면 (정규화된 폴더 목록, None),
    문제 있으면 (None, 오류 문구)를 돌려준다."""
    if mode not in MODES:
        return None, '동작이 올바르지 않습니다.'
    try:
        lib_id = int(lib_id)
    except Exception:
        return None, '라이브러리를 선택해 주세요.'
    if lib_id <= 0:
        return None, '라이브러리를 선택해 주세요.'
    if not isinstance(folders, list) or not folders:
        return None, '선택된 항목이 없습니다.'
    out = []
    seen = set()
    for f in folders:
        if not isinstance(f, str):
            return None, '폴더 경로가 올바르지 않습니다.'
        f = f.strip()
        if not f or not f.startswith('/'):
            return None, '폴더 경로가 올바르지 않습니다.'
        if f not in seen:
            seen.add(f)
            out.append(f)
    if len(out) > MAX_FOLDERS:
        return None, f'한 번에 최대 {MAX_FOLDERS}개까지 실행할 수 있습니다.'
    return out, None


def request_selected_action(lib_id, mode, folders):
    """웹이 실행을 요청한다 — 실제 실행은 워커가 수행한다.

    이미 실행 중이거나 접수된 요청이 처리되지 않은 채 15분이 지나지
    않았으면 새 요청을 받지 않고 None을 돌려준다."""
    folders, err = validate_request(lib_id, mode, folders)
    if err:
        raise ValueError(err)
    state = read_sel_state()
    if state.get('busy'):
        return None
    now = time.time()
    requested_at = state.get('requested_at') or 0
    finished_at = state.get('finished_at') or 0
    if requested_at > finished_at and now - requested_at < REQUEST_EXPIRE:
        return None
    return write_sel_state(lib=int(lib_id), mode=mode, folders=folders,
                           total=len(folders), done=0, current='',
                           results=[], requested_at=now, started_at=None,
                           finished_at=state.get('finished_at'),
                           error=None)


def _refresh_ok(ret):
    """vfs_refresh 결과 dict에서 성공 여부와 실패 사유를 뽑는다.

    파일체크(task_scan)의 판정과 같다: result 값 중 하나라도 'OK'면 성공.
    빈 응답은 RC 실행 자체가 실패한 것으로 본다."""
    if not isinstance(ret, dict) or not ret:
        return False, '새로고침 응답이 없습니다.'
    if ret.get('error'):
        return False, str(ret['error'])[:200]
    result = ret.get('result')
    if isinstance(result, dict):
        if any(str(v).lower() == 'ok' for v in result.values()):
            return True, ''
        vals = [str(v) for v in result.values() if v]
        return False, (vals[0][:200] if vals else '새로고침에 실패했습니다.')
    return True, ''


def _do_refresh(folder):
    ret = vfs_refresh(folder, recursive=True)
    ok, err = _refresh_ok(ret)
    if not ok:
        raise RuntimeError(err)
    return None


def _do_scan(lib_id, folder, seq):
    """폴더 경로를 지정한 compat 부분 스캔 전송. job id를 돌려준다."""
    url, token = ShyniBackend.get_config()
    if not url or not token:
        raise RuntimeError('기본 설정에서 shyni 주소와 토큰을 먼저 설정해 주세요.')
    shyni_path = ShyniBackend.map_path(folder)
    wait = max(P.ModelSetting.get_int('scan_max_wait_time') or 5, 1) * 60
    body = {'path': shyni_path, 'mode': 'add', 'wait': wait}
    res = requests.post(
        f'{url}/library/sections/{lib_id}/refresh', json=body,
        headers={'X-Plex-Token': token, 'Accept': 'application/json',
                 'Idempotency-Key': f'sm-trash-sel-{lib_id}-{seq}-'
                                    f'{int(time.time() * 1000)}'},
        timeout=ShyniBackend.timeout_sec('send'))
    res.raise_for_status()
    job_id = res.headers.get('X-Job-Id')
    if not job_id:
        try:
            body_res = res.json()
            if isinstance(body_res, dict):
                job_id = (body_res.get('job_id') or body_res.get('jobId')
                          or body_res.get('jobID'))
        except Exception:
            job_id = None
    return str(job_id) if job_id else None


def run_selected_action(state):
    """선택 실행 본체 — 워커에서만 실행한다."""
    lib_id = state.get('lib')
    mode = state.get('mode') or 'scan'
    folders = list(state.get('folders') or [])
    label = MODE_LABEL.get(mode, mode)
    write_sel_state(busy=True, started_at=time.time(), done=0, current='',
                    results=[], error=None)
    results = []
    try:
        for idx, folder in enumerate(folders):
            write_sel_state(current=folder, done=idx, results=results)
            entry = {'folder': folder, 'ok': True, 'error': '', 'job_id': None}
            step = None
            try:
                if mode in ('refresh', 'refresh_scan'):
                    step = '새로고침'
                    _do_refresh(folder)
                if mode in ('scan', 'refresh_scan'):
                    step = '스캔'
                    entry['job_id'] = _do_scan(lib_id, folder, idx)
            except Exception as e:
                entry['ok'] = False
                # refresh_scan은 어느 단계에서 실패했는지 구분해 남긴다.
                # 새로고침이 실패하면 스캔 단계로 넘어가지 않는다.
                if mode == 'refresh_scan' and step:
                    entry['error'] = f'{step} 실패: {e}'[:300]
                else:
                    entry['error'] = str(e)[:300]
                logger.warning(f'[Trash] 선택 {label} 실패: {folder} — {e}')
            results.append(entry)
            write_sel_state(done=idx + 1, results=results)
        ok_count = sum(1 for r in results if r['ok'])
        logger.info(f'[Trash] 선택 {label} 완료 (워커): 라이브러리 {lib_id}, '
                    f'{ok_count}/{len(results)}개 성공')
    except Exception as e:
        logger.error(f'[Trash] 선택 실행 실패(워커): {e}')
        write_sel_state(error=str(e)[:300])
    finally:
        # 스캔으로 복원된 파일이 있을 수 있어 이 라이브러리의 상세 캐시는 지운다
        try:
            from .task_trash_view import clear_cache
            clear_cache(lib_id)
        except Exception as e:
            logger.warning(f'[Trash] 선택 실행 후 상세 캐시 지우기 실패: {e}')
        # 개수 캐시도 오래됨으로 표시해 다음 통계 계산 때 새로 읽게 한다
        # (스캔으로 파일이 복원되면 휴지통 개수가 바뀐다)
        try:
            from .task_status import mark_trash_counts_stale
            mark_trash_counts_stale()
        except Exception as e:
            logger.warning(f'[Trash] 선택 실행 후 개수 캐시 오래됨 표시 실패: {e}')
        write_sel_state(busy=False, current='', finished_at=time.time())


class TaskTrashSelected:
    watch_thread = None

    @F.celery.task
    def start():
        if TaskTrashSelected.watch_thread is None:
            TaskTrashSelected.watch_thread = threading.Thread(
                target=TaskTrashSelected.watch_function, args=())
            TaskTrashSelected.watch_thread.daemon = True
            TaskTrashSelected.watch_thread.start()
            logger.info('SHYNI TRASH SELECTED 선택 실행 워커 대기')

    @staticmethod
    def watch_function():
        while True:
            try:
                state = read_sel_state()
                if state.get('busy'):
                    started_at = state.get('started_at') or 0
                    if started_at and time.time() - started_at > BUSY_EXPIRE:
                        logger.warning('[Trash] 중단된 선택 실행 감지 — busy를 풀고 다시 실행합니다')
                        state = write_sel_state(busy=False)
                requested_at = state.get('requested_at') or 0
                finished_at = state.get('finished_at') or 0
                if requested_at > finished_at and not state.get('busy'):
                    run_selected_action(state)
            except Exception as e:
                logger.error(f'[Trash] 선택 실행 워커 오류: {e}')
            time.sleep(WATCH_INTERVAL)
