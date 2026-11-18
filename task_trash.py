import json
import os
import random
import time

import requests

from .setup import *
from .shyni_backend import ShyniBackend
from .task_periodic import (CANCEL_RESULT_MESSAGE, WAIT_TIMEOUT_SEC,
                            _cancel_shyni_scan, _extract_job_id,
                            _record_scan_progress, _scan_poll_outcome,
                            _stat_int, _write_status,
                            apply_scan_result, scan_summary_text)

logger = P.logger

# 옛 휴지통 스케줄러(run_empty_trash)는 호출처가 없어 z38에서 제거했다 (A3).
# 기준 경로 설정(scan_trash_schedule_anchors)은 TaskTrash가 쓰므로 유지한다.

TRASH_JOBS_KEY = 'trash_jobs'
TRASH_CURRENT_KEY = 'trash_current'
TRASH_CANCEL_KEY = 'trash_cancel'
TRASH_MAX_WAIT_KEY = 'trash_max_wait_min'
# 대기 확인 간격(초) — 앞선 작업이 끝났는지 확인하는 주기.
WAIT_POLL_SEC = 10
# 기본 작업 대기 시간(분) — 설정이 없거나 읽지 못하면 이 값을 쓴다.
DEFAULT_MAX_WAIT_MIN = 360
# VFS 새로고침 제한 시간 설정 키(분) — 폴더 새로고침이 끝나기를
# 이 시간까지 기다린다. 설정이 없거나 0 이하면 기본값을 쓴다.
TRASH_VFS_TIMEOUT_KEY = 'trash_vfs_timeout_min'
DEFAULT_VFS_TIMEOUT_MIN = 360
# 가장 최근 시작 실패 — 작업 관리 배너가 읽는다 (주기적 스캔과 같은 구조).
TRASH_START_ERROR_KEY = 'trash_last_start_error'
# 시작 실패 기록 보관 기한(초) — 이보다 오래된 오류는 배너에 띄우지 않는다.
TRASH_START_ERROR_MAX_AGE_SEC = 3600

# 폴링 실패 기준 (W7과 같은 기준): 일시적 오류는 재시도하고,
# 연속 3회 실패 시에만 실패로 확정한다.
POLL_FAIL_MAX = 3
VFS_POLL_SEC = 10
SCAN_POLL_SEC = 20
EMPTY_POLL_SEC = 10


def _trash_file_paths(key):
    """휴지통 파일 경로 목록 — 관리자 API 우선, Relay 폴백 (Step4).
    둘 다 안 되면 None을 돌려준다."""
    try:
        from . import shyni_admin
        paths = shyni_admin.api_trash_file_paths(key)
        if paths is not None:
            return paths
    except Exception as e:
        logger.warning(f'[휴지통 스캔] 휴지통 목록 API 읽기 실패: {e}')
    try:
        from . import db_support
        if db_support.db_ready():
            return db_support.trash_file_paths(key)
    except Exception as e:
        logger.warning(f'[휴지통 스캔] 휴지통 목록 Relay 읽기 실패: {e}')
    return None

ACTION_LABEL = {
    'refresh': '새로고침',
    'scan': '스캔',
    'empty': '비우기',
    'refresh_scan': '새로고침 후 스캔',
    'refresh_scan_empty': '새로고침 후 스캔 후 비우기',
}
ACTION_STEPS = {
    'refresh': ['refresh'],
    'scan': ['scan'],
    'empty': ['empty'],
    'refresh_scan': ['refresh', 'scan'],
    'refresh_scan_empty': ['refresh', 'scan', 'empty'],
}
STAGE_LABEL = {'refresh': '새로고침 중', 'scan': '스캔 중', 'empty': '비우는 중'}

# 휴지통 파일 목록 저장 상한 (비운 파일과 같은 기준).
TRASH_FILES_MAX = 1000
# 폴더별 스캔 job stats 합산 대상 수치 키 — 도서 스캐너의
# entries/missing_files 키도 함께 합산한다.
_SCAN_STAT_KEYS = ('total', 'new', 'missing', 'updated', 'restored',
                   'registered', 'failed', 'entries', 'missing_files')


def _sum_scan_stats(stats_list):
    """폴더별 스캔 job stats 합산 — 순수 함수(하니스 검증용).
    수치 키는 합산하고(값이 있는 job이 하나도 없으면 키 없음),
    폴더별 results[]는 이어 붙인다."""
    merged = {}
    results = []
    for stats in stats_list or []:
        if not isinstance(stats, dict):
            continue
        for k in _SCAN_STAT_KEYS:
            v = _stat_int(stats.get(k))
            if v is not None:
                merged[k] = merged.get(k, 0) + v
        r = stats.get('results')
        if isinstance(r, list):
            results.extend(r)
    if results:
        merged['results'] = results
    return merged


def _trash_done_message(action, target_count, restored, emptied_count,
                        vfs_failed, scan_failed, empty_skipped,
                        folder_count, remaining):
    """완료 메시지 조립 — 순수 함수(하니스 검증용).
    숫자는 전부 파일 수. restored가 None이면 복원 여부를 확인하지
    못한 것이고, emptied_count가 None이면 비운 개수를 job stats에서
    읽지 못한 것이다 (그 경우 remaining으로 대체한다)."""
    bits = []
    if action == 'refresh':
        bits.append(f'휴지통 파일 {target_count}개 새로고침')
        if vfs_failed:
            bits.append(f'VFS {vfs_failed}개 폴더 실패')
        else:
            bits.append(f'VFS {folder_count}개 폴더 OK')
    elif action in ('scan', 'refresh_scan', 'refresh_scan_empty'):
        bits.append(f'휴지통 파일 {target_count}개 확인')
        if restored is None:
            bits.append('복원 확인 불가')
        elif restored:
            bits.append(f'복원 {restored}개')
        else:
            bits.append('복원 없음')
        if action == 'refresh_scan_empty':
            if empty_skipped:
                bits.append('비우기 생략 — 실패한 폴더 있음')
            else:
                emptied = (emptied_count if emptied_count is not None
                           else remaining)
                if emptied is not None:
                    bits.append(f'{emptied}개 비움')
        if vfs_failed:
            bits.append(f'VFS {vfs_failed}개 폴더 실패')
        if scan_failed:
            bits.append(f'스캔 실패 폴더 {scan_failed}개')
    elif action == 'empty':
        emptied = (emptied_count if emptied_count is not None
                   else target_count)
        if emptied is not None:
            bits.append(f'휴지통 파일 {emptied}개 비움')
    if not bits:
        return '작업 완료'
    return '작업 완료 (' + ' · '.join(bits) + ')'


def _trash_skip_decision(running_library_keys, key):
    """섹션 시작 시 건너뜀 판정 — 순수 함수(하니스 검증용).
    같은 라이브러리에 라이브러리 단위 스캔(대시보드·주기적 스캔)이
    돌아가고 있으면 건너뛴다. 스캔 큐(폴더 단위)는 세지 않는다 —
    라이브러리 동시 스캔 수 게이트와 같은 기준이다.
    (건너뜀 안내 문장 | None)을 돌려준다."""
    running_keys = {str(k) for k in (running_library_keys or [])
                    if k is not None}
    if str(key) in running_keys:
        return '동일 라이브러리의 스캔이 실행 중이므로 건너뜁니다.'
    return None


def get_trash_jobs():
    """ModelSetting에 저장된 휴지통 주기 작업 목록."""
    raw = P.ModelSetting.get(TRASH_JOBS_KEY)
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                for idx, item in enumerate(data):
                    item['job_id'] = f'{P.package_name}_trash_{idx}'
                return data
        except Exception:
            pass
    return []


def get_trash_current():
    raw = P.ModelSetting.get(TRASH_CURRENT_KEY)
    if raw:
        try:
            cur = json.loads(raw)
            if isinstance(cur, dict):
                return cur
        except Exception:
            pass
    return None


def _set_trash_current(cur):
    if cur is None:
        P.ModelSetting.set(TRASH_CURRENT_KEY, '')
    else:
        P.ModelSetting.set(TRASH_CURRENT_KEY, json.dumps(cur, ensure_ascii=False))


def update_trash_current_job_idx(new_idx):
    """진행 상태의 작업 번호를 바꾼다 — 작업 관리에서 순서를 바꿔
    저장했을 때 진행 중인 작업의 위치를 새 목록에 맞게 고친다."""
    cur = get_trash_current()
    if cur is None or cur.get('job_idx') == new_idx:
        return
    cur['job_idx'] = new_idx
    _set_trash_current(cur)


def _max_wait_min():
    """작업 대기 시간(분) — 설정값, 없거나 숫자가 아니면 기본 360분.
    음수는 기본값으로 취급한다. 0이면 기다리지 않는다."""
    try:
        val = P.ModelSetting.get_int(TRASH_MAX_WAIT_KEY)
    except Exception:
        val = None
    if val is None or val < 0:
        return DEFAULT_MAX_WAIT_MIN
    return val


def _vfs_timeout_sec():
    """VFS 새로고침 제한 시간(초) — 설정값(분)을 초로 바꾼다.
    없거나 숫자가 아니거나 0 이하면 기본 360분으로 동작한다."""
    try:
        val = P.ModelSetting.get_int(TRASH_VFS_TIMEOUT_KEY)
    except Exception:
        val = None
    if val is None or val <= 0:
        return DEFAULT_VFS_TIMEOUT_MIN * 60
    return val * 60


def _fmt_wait(sec):
    """대기 시간 표시 — 45초 · 12분 3초 · 6시간 5분. 순수 함수(하니스 검증용)."""
    sec = int(sec)
    if sec < 0:
        sec = 0
    if sec < 60:
        return f'{sec}초'
    if sec < 3600:
        m, s = divmod(sec, 60)
        return f'{m}분 {s}초' if s else f'{m}분'
    h, m = divmod(sec, 3600)
    m //= 60
    return f'{h}시간 {m}분' if m else f'{h}시간'


def _same_job_busy(name):
    """같은 이름의 작업이 이미 돌거나 기다리고 있으면 True.
    스케줄이 겹쳐 같은 작업이 두 번 도는 것을 막는다."""
    try:
        cur = get_trash_current()
        if cur is not None and (cur.get('job_name') or '') == (name or ''):
            return True
    except Exception:
        pass
    try:
        from .model_trash import ModelTrashItem
        row = (F.db.session.query(ModelTrashItem.id)
               .filter(ModelTrashItem.job_name == name,
                       ModelTrashItem.status == 'waiting').first())
        return row is not None
    except Exception as e:
        logger.warning(f'[휴지통 스캔] 대기 중 작업 조회 실패: {e}')
        return False


def _try_claim(idx, name, mode, job_started, job_row_id, section_label='전체', action=None):
    """차례를 차지한다 — 진행 상태를 이 작업 것으로 쓰고 다시 읽어
    확인한다. 차지했으면 True.
    두 작업이 거의 동시에 차지하려는 틈을 줄이기 위해 쓰고 잠깐 쉰
    뒤에 확인한다. 확인에 실패하면 상대가 차지한 것이니 기다린다.
    section_label — 진행 상태에 표시할 라이브러리명. 대상이 1개면
    그 이름, 여러 개면 '전체'."""
    claim_id = (f'wait-{job_row_id}' if job_row_id
                else f'run-{idx}-{int(time.time() * 1000)}')
    _set_trash_current({
        'job_idx': idx, 'job_name': name, 'section_key': '',
        'section_title': section_label, 'stage': 'scan', 'mode': mode,
        'action': action,
        'job_started_at': job_started, 'section_started_at': _now_text(),
        'claim_id': claim_id, 'job_row_id': job_row_id})
    time.sleep(random.uniform(0.3, 1.0))
    cur = get_trash_current() or {}
    return cur.get('claim_id') == claim_id


def _finish_job_row(row_id, status, message):
    """대기 행을 끝낸다 — 행을 id로 다시 읽어 끝낸다.
    (세션이 달라져도 찾을 수 있게 객체를 들고 있지 않는다.)"""
    try:
        from .model_trash import ModelTrashItem
        item = (F.db.session.query(ModelTrashItem)
                .filter(ModelTrashItem.id == row_id).first())
        if item is not None:
            item.finish(status, message)
    except Exception as e:
        logger.warning(f'[휴지통 스캔] 대기 행 정리 실패: {e}')


def _wait_for_turn(idx, name, mode, job_started, section_label='전체', action=None):
    """차례를 기다렸다가 차지한다.
    (대기 행 id 또는 None, 기다린 초 또는 None)을 돌려준다.
    바로 시작하면 (None, 0), 기다렸다 시작하면 (행 id, 기다린 초),
    기다리다 시간 초과하면 (None, None) — 이 때는 실패 행을 남긴다.
    작업 대기 시간이 0이면 기다리지 않고 바로 시작 실패로 끝낸다.
    section_label — 대기 행에 표시할 라이브러리명. 대상이 1개면
    그 이름, 여러 개면 '전체'."""
    from .model_trash import ModelTrashItem
    if get_trash_current() is None and _try_claim(idx, name, mode, job_started, None, section_label, action):
        return (None, 0)
    max_min = _max_wait_min()
    if max_min <= 0:
        fail_trash_start(name, mode, '이미 진행 중인 휴지통 스캔이 있어 시작하지 못했습니다.', section_label, action)
        return (None, None)
    running_name = (get_trash_current() or {}).get('job_name') or ''
    if running_name:
        wait_msg = f'앞선 작업({running_name}) 진행 중으로 끝나면 이어서 진행합니다.'
    else:
        wait_msg = '앞선 작업 진행 중으로 끝나면 이어서 진행합니다.'
    item = ModelTrashItem(name, '', section_label, mode, action)
    item.status = 'waiting'
    item.message = wait_msg
    try:
        item.save()
    except Exception as e:
        logger.error(f'[휴지통 스캔] 대기 행 저장 실패: {e}')
        fail_trash_start(name, mode, '대기 기록을 남기지 못했습니다.', section_label, action)
        return (None, None)
    row_id = item.id
    t0 = time.time()
    deadline = t0 + max_min * 60
    while time.time() < deadline:
        time.sleep(WAIT_POLL_SEC)
        if get_trash_current() is None and _try_claim(idx, name, mode, job_started, row_id, section_label, action):
            waited = int(time.time() - t0)
            try:
                row = (F.db.session.query(ModelTrashItem)
                       .filter(ModelTrashItem.id == row_id).first())
                if row is not None:
                    row.status = 'running'
                    row.wait_seconds = waited
                    row.message = f'대기 {_fmt_wait(waited)} 후 시작했습니다.'
                    row.save()
            except Exception as e:
                logger.warning(f'[휴지통 스캔] 대기 행 시작 기록 실패: {e}')
            return (row_id, waited)
    _finish_job_row(row_id, 'failed',
                    f'앞선 작업이 끝나기를 기다리다 시간 초과됐습니다. (작업 대기 {_fmt_wait(max_min * 60)})')
    logger.warning(f'[휴지통 스캔] {name}: 작업 대기 시간 초과로 시작하지 않습니다.')
    return (None, None)


def _record_trash_start_error(job_name, reason):
    """시작 실패를 기록한다 — 작업 관리 배너가 읽는다.
    기록 실패가 실행을 막지 않게 예외를 삼킨다."""
    try:
        P.ModelSetting.set(TRASH_START_ERROR_KEY, json.dumps(
            {'at': time.time(), 'job_name': job_name or '',
             'reason': str(reason or '')[:500]},
            ensure_ascii=False))
    except Exception as e:
        logger.warning(f'[휴지통 스캔] 시작 오류 기록 실패: {e}')


def _clear_trash_start_error():
    try:
        P.ModelSetting.set(TRASH_START_ERROR_KEY, '')
    except Exception:
        pass


def clear_trash_start_error():
    """시작 오류 기록을 지운다 (배너 닫기용)."""
    _clear_trash_start_error()


def get_last_trash_start_error(max_age_sec=TRASH_START_ERROR_MAX_AGE_SEC):
    """가장 최근 시작 오류 — 없거나 깨졌거나 오래됐으면 None."""
    raw = P.ModelSetting.get(TRASH_START_ERROR_KEY)
    if not raw:
        return None
    try:
        err = json.loads(raw)
    except Exception:
        return None
    if not isinstance(err, dict):
        return None
    at = err.get('at')
    if not isinstance(at, (int, float)) or isinstance(at, bool):
        return None
    if time.time() - at > max_age_sec:
        return None
    return err


def fail_trash_start(job_name, mode, reason, section_label='전체', action=None):
    """작업 단위 시작 실패 — 이력 행을 남기고 배너용 오류를 기록한다.
    이력 행 저장이 실패해도 배너 기록은 시도한다.
    section_label — 이력 행에 표시할 라이브러리명. 대상을 알면 그 이름,
    모르면 '전체'. action — 알면 이력 행에 함께 남긴다."""
    try:
        from .model_trash import ModelTrashItem
        item = ModelTrashItem(job_name, '', section_label, mode, action)
        item.finish('failed', reason)
    except Exception as e:
        logger.error(f'[휴지통 스캔] 시작 실패 이력 기록 실패: {e}')
    _record_trash_start_error(job_name, reason)


def _trash_cancel_requested():
    return (P.ModelSetting.get(TRASH_CANCEL_KEY) or '') == '1'


def request_trash_cancel():
    P.ModelSetting.set(TRASH_CANCEL_KEY, '1')


def _now_text():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _wait_vfs(addr, jobid, timeout=None):
    """rclone VFS 새로고침 job 완료를 기다린다.
    {'result': done|failed|cancelled, 'duration': 초}를 돌려준다.
    기한은 설정(휴지통 VFS 새로고침 제한 시간, 기본 360분)에서 읽는다.
    timeout을 직접 주면 그 값(초)을 쓴다."""
    if timeout is None:
        timeout = _vfs_timeout_sec()
    t0 = time.time()
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _trash_cancel_requested():
            return {'result': 'cancelled', 'duration': int(time.time() - t0)}
        try:
            from .mod_status import ModuleStatus
            job = ModuleStatus._rc(addr, 'job/status', {'jobid': jobid})
            if job.get('finished'):
                return {'result': 'failed' if job.get('error') else 'done',
                        'duration': int(time.time() - t0)}
        except requests.HTTPError as e:
            body = ''
            try:
                body = e.response.text if e.response is not None else ''
            except Exception:
                body = ''
            if 'not found' in body:
                return {'result': 'failed', 'duration': int(time.time() - t0)}
        except Exception:
            pass
        time.sleep(VFS_POLL_SEC)
    return {'result': 'failed', 'duration': int(time.time() - t0)}


def _wait_job(url, token, job_id, timeout=WAIT_TIMEOUT_SEC, poll_sec=SCAN_POLL_SEC,
              section_key=None):
    """shyni job 완료를 기다린다 (스캔·비우기 공통).
    {'result': done|failed|cancelled|not_found, 'job': job 응답 또는 None,
     'duration': 초, 'cancel_message': 취소 시 이력 문구(있을 때만)}를
    돌려준다. job 조회에서 404를 받으면 즉시 실패로 끝낸다 (W3). 폴링 중
    일시적 오류는 재시도하고 연속 3회 실패 시에만 실패로 확정한다.
    section_key가 있으면 60초마다 스캔 진행률을 현황에 기록한다 (스캔
    job만 — 비우기 job은 section_key를 넘기지 않는다). 스캔 job은 취소
    요청이 오면 shyni 스캔도 함께 취소하고 확인한다 (Step5-1). 비우기
    job은 취소할 수 없어 플래그가 있어도 끝까지 기다린다 — 플래그는
    남아 있어 비우기가 끝난 뒤 다음 라이브러리는 멈춘다."""
    t0 = time.time()
    deadline = time.time() + timeout
    fail_count = 0
    poll_count = 0
    while time.time() < deadline:
        if section_key is not None and _trash_cancel_requested():
            # 스캔 job — shyni 스캔도 함께 취소하고 확인한다 (Step5-1).
            cres = _cancel_shyni_scan(url, token, job_id, section_key)
            cjob = cres.get('job') or {}
            if cres['reason'] == 'already_done' or (
                    cres['reason'] == 'finished_first'
                    and cjob.get('status') == 'completed'):
                return {'result': 'done', 'job': cjob,
                        'duration': int(time.time() - t0)}
            if cres['reason'] == 'already_failed' or (
                    cres['reason'] == 'finished_first'
                    and cjob.get('status') == 'failed'):
                return {'result': 'failed', 'job': cjob,
                        'duration': int(time.time() - t0)}
            return {'result': 'cancelled', 'job': None,
                    'duration': int(time.time() - t0),
                    'cancel_message': CANCEL_RESULT_MESSAGE.get(
                        cres['reason'], '취소 요청으로 중단했습니다.')}
        try:
            res = requests.get(f'{url}/compat/jobs/{job_id}',
                               headers={'X-Plex-Token': token,
                                        'Accept': 'application/json'},
                               timeout=15)
        except Exception:
            fail_count += 1
            if fail_count >= POLL_FAIL_MAX:
                return {'result': 'failed', 'job': None,
                        'duration': int(time.time() - t0)}
            time.sleep(poll_sec)
            continue
        job = None
        if res.status_code == 200:
            try:
                job = res.json()
            except Exception:
                job = None
        outcome, _msg = _scan_poll_outcome(res.status_code, job)
        if outcome == 'not_found':
            return {'result': 'not_found', 'job': None,
                    'duration': int(time.time() - t0)}
        if outcome == 'done':
            return {'result': 'done', 'job': job,
                    'duration': int(time.time() - t0)}
        if outcome == 'failed':
            return {'result': 'failed', 'job': job,
                    'duration': int(time.time() - t0)}
        if outcome == 'cancelled':
            return {'result': 'cancelled', 'job': job,
                    'duration': int(time.time() - t0),
                    'cancel_message': '스캔이 취소됐습니다.'}
        if res.status_code != 200:
            fail_count += 1
            if fail_count >= POLL_FAIL_MAX:
                return {'result': 'failed', 'job': None,
                        'duration': int(time.time() - t0)}
        else:
            fail_count = 0
        # 진행률 기록 — 60초마다 (폴링 간격 × 횟수, SCAN_POLL_SEC 20초 기준 3회).
        poll_count += 1
        if section_key and poll_count % max(1, int(60 / max(poll_sec, 1))) == 0:
            _record_scan_progress(section_key)
        time.sleep(poll_sec)
    return {'result': 'failed', 'job': None, 'duration': int(time.time() - t0)}


def _wait_scan(url, token, job_id, timeout=WAIT_TIMEOUT_SEC, section_key=None):
    """shyni 스캔 job 완료를 기다린다 — _wait_job과 같은 결과를 돌려준다."""
    return _wait_job(url, token, job_id, timeout=timeout, poll_sec=SCAN_POLL_SEC,
                     section_key=section_key)


def _send_folder_scan(url, token, key, folder, seq):
    """폴더 경로를 지정한 compat 부분 스캔 전송. job id를 돌려준다.
    선택 실행(task_trash_selected._do_scan)과 같은 호출 — 휴지통
    작업은 여기서 전송만 하고 완료는 _wait_scan으로 기다린다."""
    shyni_path = ShyniBackend.map_path(folder)
    wait = max(P.ModelSetting.get_int('scan_max_wait_time') or 5, 1) * 60
    body = {'path': shyni_path, 'mode': 'add', 'wait': wait}
    res = requests.post(
        f'{url}/library/sections/{key}/refresh', json=body,
        headers={'X-Plex-Token': token, 'Accept': 'application/json',
                 'Idempotency-Key': f'sm-trash-job-{key}-{seq}-'
                                    f'{int(time.time() * 1000)}'},
        timeout=ShyniBackend.timeout_sec('send'))
    res.raise_for_status()
    job_id = _extract_job_id(res)
    return str(job_id) if job_id else None


def _refresh_folder_vfs(key, folder, name):
    """폴더 하나 VFS 새로고침.
    {'result': done|failed|cancelled, 'duration': 초, 'error': 사유}를
    돌려준다. 라이브러리 전체를 한 번에 새로고침하던 기존 방식과 같은
    RC 호출을 폴더 단위로 한다. jobid가 안 오면 동기 응답의 result를
    직접 판정한다 (선택 실행의 _refresh_ok와 같은 기준)."""
    from .mod_status import ModuleStatus
    addr, remote = ModuleStatus._rc_target(folder)
    if not addr:
        return {'result': 'failed', 'duration': 0,
                'error': 'VFS 새로고침 규칙과 맞지 않는 경로입니다.'}
    started = _now_text()
    _write_status(key, 'vfs', {
        'job': None, 'path': folder,
        'started_at': started, 'finished_at': None,
        'status': 'running', 'source': 'trash',
        'job_name': name}, label='휴지통 스캔')
    t0 = time.time()
    try:
        ret = ModuleStatus._rc(addr, 'vfs/refresh',
                               {'dir': remote, 'recursive': 'true',
                                '_async': 'true'})
    except Exception as e:
        _write_status(key, 'vfs', {
            'job': None, 'path': folder,
            'started_at': started, 'finished_at': _now_text(),
            'status': 'failed', 'source': 'trash',
            'job_name': name}, label='휴지통 스캔')
        return {'result': 'failed', 'duration': int(time.time() - t0),
                'error': str(e)[:200]}
    jobid = ret.get('jobid') if isinstance(ret, dict) else None
    if jobid:
        _write_status(key, 'vfs', {
            'job': jobid, 'path': folder,
            'started_at': started, 'finished_at': None,
            'status': 'running', 'source': 'trash',
            'job_name': name}, label='휴지통 스캔')
        w = _wait_vfs(addr, jobid)
        _write_status(key, 'vfs', {
            'job': jobid, 'path': folder,
            'started_at': started, 'finished_at': _now_text(),
            'status': {'done': 'done', 'failed': 'failed',
                       'cancelled': 'cancelled'}.get(w['result'], 'failed'),
            'source': 'trash', 'job_name': name}, label='휴지통 스캔')
        if w['result'] == 'done':
            return {'result': 'done', 'duration': w['duration'], 'error': ''}
        if w['result'] == 'cancelled':
            return {'result': 'cancelled', 'duration': w['duration'], 'error': ''}
        return {'result': 'failed', 'duration': w['duration'],
                'error': 'VFS 새로고침이 끝나지 않았습니다.'}
    # jobid 없이 동기 결과가 온 경우 — result 값을 직접 판정한다.
    result = ret.get('result') if isinstance(ret, dict) else None
    ok = False
    error = ''
    if isinstance(result, dict):
        if any(str(v).lower() == 'ok' for v in result.values()):
            ok = True
        else:
            vals = [str(v) for v in result.values() if v]
            error = vals[0][:200] if vals else '새로고침에 실패했습니다.'
    elif isinstance(ret, dict) and ret.get('error'):
        error = str(ret['error'])[:200]
    else:
        error = '새로고침 응답이 없습니다.'
    _write_status(key, 'vfs', {
        'job': None, 'path': folder,
        'started_at': started, 'finished_at': _now_text(),
        'status': 'done' if ok else 'failed', 'source': 'trash',
        'job_name': name}, label='휴지통 스캔')
    return {'result': 'done' if ok else 'failed',
            'duration': int(time.time() - t0), 'error': error}


def _compute_skip_reason(key):
    """이 섹션을 건너뛸 사유 — None이면 진행한다.
    라이브러리 동시 스캔 수 게이트와 같은 목록(대시보드·주기적·
    휴지통 스캔)으로 판정한다. 스캔 큐(폴더 단위)는 세지 않는다.
    자기 작업의 진행 상태는 빼고 센다. 목록을 읽지 못하면
    게이트와 마찬가지로 막지 않고 진행한다.
    섹션 시작 시마다 다시 읽는다."""
    try:
        running = ModuleStatus.running_library_scans(exclude_source='trash')
        running_keys = [k for k, _ in running]
    except Exception as e:
        running_keys = []
        logger.warning(f'[휴지통 스캔] 실행 중인 라이브러리 스캔 조회 실패: {e}')
    return _trash_skip_decision(running_keys, key)


class TaskTrash:
    """휴지통 주기 작업 실행 — 선택한 라이브러리를 하나씩 순차로 처리한다.

    실행 전에 Relay DB에서 휴지통 파일 목록을 읽어, 항목이 없으면
    건너뛰고 항목이 있을 때만 그 항목의 폴더를 대상으로 새로고침·스캔을
    수행한다. 비우기는 스캔이 끝난 뒤 섹션 비우기 1회로 수행한다
    (폴더별 비우기 API는 없다). 새로 없어진 파일을 찾는 전체 스캔은
    주기적 스캔이 담당한다. 진행 상태는 ModelSetting trash_current에
    기록해 화면이 공유하고, 취소는 trash_cancel 플래그로 요청받는다.
    완료된 스캔의 집계와 비운 개수는 이력 행에 남기고, 진행 상황은
    대시보드 섹션 상태(status_sec_N)에도 수동 작업과 같은 모양으로
    기록한다.
    """

    @F.celery.task
    def start(idx, mode):
        with F.app.app_context():
            try:
                TaskTrash._run(idx, mode)
            except Exception as e:
                logger.error(f'휴지통 주기 작업 실행 실패: {e}')
                logger.error(traceback.format_exc())
                try:
                    row_id = (get_trash_current() or {}).get('job_row_id')
                except Exception:
                    row_id = None
                _set_trash_current(None)
                if row_id:
                    # 기다렸다 시작한 작업이면 대기 행을 실패로 끝낸다 —
                    # fail_trash_start로 또 행을 만들면 두 개가 된다.
                    _finish_job_row(row_id, 'failed', f'실행 중 오류가 발생했습니다: {e}')
                    return
                try:
                    jobs = get_trash_jobs()
                    job = jobs[idx] if 0 <= idx < len(jobs) else None
                except Exception:
                    job = None
                if job is not None:
                    fail_trash_start(job.get('name') or f'작업 {idx + 1}',
                                     mode, f'실행 중 오류가 발생했습니다: {e}',
                                     '전체', job.get('action'))

    @staticmethod
    def _fail_running_status(key, job_name):
        """예외로 섹션이 끝났을 때 이 작업이 남긴 running 표시를 실패로 정리한다."""
        try:
            from .mod_status import ModuleStatus
            st = ModuleStatus._get_state(int(key))
            changed = False
            for slot in ('vfs', 'scan', 'trash'):
                entry = st.get(slot) or {}
                if (entry.get('status') == 'running'
                        and entry.get('source') == 'trash'
                        and entry.get('job_name') == job_name):
                    entry['status'] = 'failed'
                    entry['finished_at'] = _now_text()
                    changed = True
            if changed:
                ModuleStatus._set_state(int(key), st)
        except Exception as e:
            logger.warning(f'[휴지통 스캔] 현황 상태 정리 실패: {e}')

    @staticmethod
    def _run(idx, mode):
        from .model_trash import ModelTrashItem
        from .mod_status import ModuleStatus
        jobs = get_trash_jobs()
        if idx < 0 or idx >= len(jobs):
            logger.warning(f'[휴지통 스캔] 없는 작업 번호: {idx}')
            _record_trash_start_error('', f'잘못된 작업 번호라 시작하지 못했습니다: {idx}')
            return
        job = jobs[idx]
        name = job.get('name') or f'작업 {idx + 1}'
        # 같은 작업이 이미 돌거나 기다리고 있으면 중복으로 세지 않는다.
        if _same_job_busy(name):
            logger.info(f'[휴지통 스캔] {name}: 이미 실행 중이거나 대기 중이라 건너뜁니다.')
            return
        action = job.get('action') or 'empty'
        steps = ACTION_STEPS.get(action, ['empty'])
        if action not in ACTION_STEPS:
            action = 'empty'
        P.ModelSetting.set(TRASH_CANCEL_KEY, '')
        job_started = _now_text()
        logger.info(f'[휴지통 스캔] 시작: {name} ({ACTION_LABEL.get(action, action)}, {mode})')

        # 안전장치: 기준 경로가 하나라도 보이지 않으면 전체를 중단한다
        anchors = [a.strip() for a in
                   (P.ModelSetting.get('scan_trash_schedule_anchors') or '').splitlines()
                   if a.strip()]
        for anchor in anchors:
            if not os.path.exists(anchor):
                fail_trash_start(name, mode, f'기준 경로를 찾을 수 없습니다 — {anchor}', '전체', action)
                logger.error(f'[휴지통 스캔] 중단: 기준 경로 없음 — {anchor}')
                return

        sections, err = ModuleStatus._sections()
        if err:
            fail_trash_start(name, mode, err, '전체', action)
            return
        selected = {str(s) for s in (job.get('sections') or [])}
        targets = [s for s in sections if not selected or str(s['key']) in selected]
        if not targets:
            fail_trash_start(name, mode, '대상 라이브러리가 없습니다.', '전체', action)
            return
        # 작업 단위 행(대기 행) 표시 — 대상이 라이브러리 1개면 그 이름을 쓰고,
        # 여러 개면 '전체'로 둔다. 1개짜리 작업에 '전체'가 붙으면 모든
        # 라이브러리를 건드린 것처럼 보여 혼란스럽다.
        job_label = (targets[0].get('title') or str(targets[0]['key'])
                     if len(targets) == 1 else '전체')

        url, token = ShyniBackend.get_config()
        if not url or not token:
            fail_trash_start(name, mode, '기본 설정에서 shyni 주소와 토큰을 먼저 설정해 주세요.',
                             job_label, action)
            return
        _clear_trash_start_error()

        # 앞선 작업이 돌고 있으면 기다렸다가 차례가 되면 실행한다.
        # 기다리다 시간 초과하면 _wait_for_turn이 실패 행을 남긴다.
        job_row_id, _waited = _wait_for_turn(idx, name, mode, job_started, job_label, action)
        if _waited is None:
            return

        for sec in targets:
            key = str(sec['key'])
            title = sec.get('title') or key
            if _trash_cancel_requested():
                item = ModelTrashItem(name, key, title, mode, action)
                item.finish('cancelled', '취소 요청으로 실행하지 않았습니다.')
                continue
            # 건너뜀 가드는 스캔이 들어간 동작에만 적용한다 — 새로고침만·
            # 비우기만은 스캔과 충돌하지 않아 가드에서 뺀다. 가드는 섹션
            # 시작 시마다 다시 읽는다 (W5).
            guard_applies = 'scan' in steps
            skip_reason = _compute_skip_reason(key) if guard_applies else None
            if skip_reason is not None:
                item = ModelTrashItem(name, key, title, mode, action)
                item.finish('skipped', skip_reason)
                logger.info(f'[휴지통 스캔] {title}: {skip_reason}')
                continue
            busy = False
            if guard_applies:
                try:
                    st = ModuleStatus._get_state(int(key))
                    busy = any((st.get(k) or {}).get('status') == 'running'
                               for k in ('vfs', 'scan', 'meta'))
                except Exception:
                    busy = False
            if busy:
                item = ModelTrashItem(name, key, title, mode, action)
                item.finish('skipped', '현황에서 작업 중이라 건너뛰었습니다.')
                continue

            item = ModelTrashItem(name, key, title, mode, action)
            item.save()
            status, message = 'done', ''
            vfs_duration = None
            scan_duration = None
            scan_job_ids = []
            scan_stats_list = []
            emptied_count = None
            emptied_files = None
            # 휴지통 항목 대상 수행 — 실행 전에 목록을 읽어 둔다.
            # target_files가 None이면 목록을 모른다는 뜻이다.
            target_files = None
            target_groups = []
            restored_files = None
            restored_count = None
            remaining_count = None
            vfs_results = None
            scan_results = None
            file_exists = None
            vfs_failed_folders = 0
            scan_failed_folders = 0
            empty_skipped = False
            try:
                # 실행 전 휴지통 목록 확인 — 0개면 스킵, 모르면 새로고침·
                # 스캔 포함 동작은 실행하지 않는다. 비우기만은 목록 없이도
                # 섹션 비우기로 실행한다.
                # (db_support는 아래 group_append_paths에서도 쓰므로 여기서
                # 가져온다 — Phase1에서 이 줄을 빼먹어 NameError가 났음,
                # 2026-11-17 실측)
                from . import db_support
                list_error = None
                try:
                    target_files = _trash_file_paths(key)
                    if target_files is None:
                        list_error = 'read_failed'
                except Exception as e:
                    logger.warning(f'[휴지통 스캔] 휴지통 목록 읽기 실패: {e}')
                    list_error = 'read_failed'
                    target_files = None
                if target_files is None:
                    if len(steps) == 1 and steps[0] == 'empty':
                        pass
                    else:
                        skip_msg = '휴지통 항목을 확인하지 못해 건너뛰었습니다. (관리자 API와 Relay 둘 다 읽지 못했습니다.)'
                        item.finish('skipped', skip_msg)
                        logger.info(f'[휴지통 스캔] {title}: skipped {skip_msg}')
                        continue
                elif not target_files:
                    item.stat_total = 0
                    item.target_files = '[]'
                    item.finish('done', '휴지통 파일이 없어 건너뛰었습니다.')
                    logger.info(f'[휴지통 스캔] {title}: done 휴지통 파일이 없어 건너뜀')
                    continue
                else:
                    target_groups = db_support.group_append_paths(target_files)
                for step in steps:
                    if _trash_cancel_requested():
                        status, message = 'cancelled', '취소 요청으로 중단했습니다.'
                        break
                    if step == 'empty' and (vfs_failed_folders or scan_failed_folders):
                        # 실패한 폴더가 있으면 비우지 않는다 — 재확인
                        # 못한 항목을 비우면 안 된다.
                        empty_skipped = True
                        logger.info(f'[휴지통 스캔] 비우기 생략: 실패한 폴더가 있음 '
                                    f'(VFS {vfs_failed_folders}개 · 스캔 {scan_failed_folders}개)')
                        continue
                    _set_trash_current({
                        'job_idx': idx, 'job_name': name, 'section_key': key,
                        'section_title': title,
                        'stage': step, 'job_started_at': job_started,
                        'section_started_at': _now_text(),
                        'mode': mode, 'action': action, 'job_row_id': job_row_id})
                    try:
                        if step == 'refresh':
                            # 폴더별 VFS 새로고침 — 하나가 실패해도
                            # 나머지는 계속한다. 전부 실패하면 실패로 끝낸다.
                            vfs_results = {}
                            file_exists = {}
                            vfs_total = 0
                            for grp in target_groups:
                                if _trash_cancel_requested():
                                    status, message = 'cancelled', '취소 요청으로 중단했습니다.'
                                    break
                                folder = grp['folder']
                                w = _refresh_folder_vfs(key, folder, name)
                                vfs_total += w['duration']
                                if w['result'] == 'cancelled':
                                    status, message = 'cancelled', '취소 요청으로 중단했습니다.'
                                    break
                                ok = w['result'] == 'done'
                                vfs_results[folder] = {'ok': ok,
                                                       'error': w.get('error') or ''}
                                if ok:
                                    # 새로고침이 끝난 폴더는 목록을 한 번 읽어
                                    # 대상 파일이 실제로 있는지 확인한다.
                                    # 폴더당 1회만 읽으므로 파일이 많아도 빠르다.
                                    try:
                                        names = set(os.listdir(folder))
                                        for fn in grp['files']:
                                            file_exists[folder + '/' + fn] = fn in names
                                    except Exception:
                                        pass
                                if not ok:
                                    vfs_failed_folders += 1
                                    logger.warning(f'[휴지통 스캔] VFS 새로고침 실패: '
                                                   f'{folder} — {w.get("error")}')
                            vfs_duration = vfs_total
                            if status == 'cancelled':
                                break
                            if vfs_failed_folders >= len(target_groups):
                                status, message = 'failed', f'VFS 새로고침 실패 (폴더 {len(target_groups)}개)'
                                break
                        elif step == 'scan':
                            # 폴더별 부분 스캔 — VFS 새로고침이 실패한
                            # 폴더는 뺀다. 하나가 실패해도 나머지는 계속
                            # 하고, job이 끝날 때까지 기다린다 (전송 성공만
                            # 보고 끝내는 선택 실행과 다르다).
                            scan_folders = [
                                g for g in target_groups
                                if vfs_results is None
                                or (vfs_results.get(g['folder']) or {}).get('ok')]
                            # 라이브러리 동시 스캔 수 게이트 — 같은 라이브러리가
                            # 스캔 중이거나 상한까지 돌고 있으면 실패로 끝낸다
                            # (기다리지 않는다). 폴더마다 검사하지 않고 섹션
                            # 단위로 한 번만 검사한다. 스캔할 폴더가 없으면
                            # 검사할 필요가 없다.
                            if scan_folders:
                                gate_msg = ModuleStatus.library_gate_check(
                                    key, exclude_source='trash')
                                if gate_msg:
                                    status, message = 'failed', gate_msg
                                    break
                            scan_results = {}
                            scan_total = 0
                            first_scan_error = ''
                            for seq, grp in enumerate(scan_folders):
                                if _trash_cancel_requested():
                                    status, message = 'cancelled', '취소 요청으로 중단했습니다.'
                                    break
                                folder = grp['folder']
                                scan_started = _now_text()
                                _write_status(key, 'scan', {
                                    'job': None, 'path': folder,
                                    'started_at': scan_started, 'finished_at': None,
                                    'status': 'running', 'source': 'trash',
                                    'job_name': name}, label='휴지통 스캔')
                                try:
                                    job_id = _send_folder_scan(url, token, key, folder, seq)
                                    send_error = ''
                                except Exception as e:
                                    job_id = None
                                    send_error = str(e)[:200]
                                if job_id is None:
                                    scan_failed_folders += 1
                                    scan_results[folder] = {
                                        'ok': False,
                                        'error': (send_error
                                                  or '스캔 작업 번호를 받지 못했습니다.')}
                                    if not first_scan_error:
                                        first_scan_error = (send_error
                                                            or '스캔 작업 번호를 받지 못했습니다.')
                                    _write_status(key, 'scan', {
                                        'job': None, 'path': folder,
                                        'started_at': scan_started,
                                        'finished_at': _now_text(),
                                        'status': 'failed', 'source': 'trash',
                                        'job_name': name}, label='휴지통 스캔')
                                    logger.warning(f'[휴지통 스캔] 폴더 스캔 전송 실패: '
                                                   f'{folder} — {first_scan_error}')
                                    continue
                                scan_job_ids.append(job_id)
                                _write_status(key, 'scan', {
                                    'job': job_id, 'path': folder,
                                    'started_at': scan_started, 'finished_at': None,
                                    'status': 'running', 'source': 'trash',
                                    'job_name': name}, label='휴지통 스캔')
                                cur = get_trash_current() or {}
                                cur['scan_job'] = job_id
                                _set_trash_current(cur)
                                w = _wait_scan(url, token, job_id, section_key=key)
                                scan_total += w['duration']
                                job_stats = (w.get('job') or {}).get('stats') or {}
                                summary = scan_summary_text(job_stats)
                                scan_entry = {
                                    'job': job_id, 'path': folder,
                                    'started_at': scan_started, 'finished_at': _now_text(),
                                    'status': {'done': 'done', 'failed': 'failed',
                                               'cancelled': 'cancelled',
                                               'not_found': 'failed'}.get(w['result'], 'failed'),
                                    'source': 'trash', 'job_name': name}
                                if summary:
                                    scan_entry['summary'] = summary
                                _write_status(key, 'scan', scan_entry, label='휴지통 스캔')
                                if w['result'] == 'cancelled':
                                    status, message = 'cancelled', w.get(
                                        'cancel_message') or '취소 요청으로 중단했습니다.'
                                    break
                                if w['result'] != 'done':
                                    scan_failed_folders += 1
                                    job_err = ((w.get('job') or {}).get('error') or '')
                                    scan_results[folder] = {
                                        'ok': False,
                                        'error': (job_err[:200] if job_err
                                                  else '스캔이 완료되지 않았습니다.')}
                                    if not first_scan_error:
                                        first_scan_error = (job_err[:200] if job_err
                                                            else '스캔이 완료되지 않았습니다.')
                                    logger.warning(f'[휴지통 스캔] 폴더 스캔 실패: '
                                                   f'{folder} — {first_scan_error}')
                                    continue
                                scan_results[folder] = {'ok': True}
                                scan_stats_list.append(job_stats)
                            scan_duration = scan_total
                            if status == 'cancelled':
                                break
                            if scan_folders and scan_failed_folders >= len(scan_folders):
                                status, message = 'failed', f'스캔 실패 (폴더 {len(scan_folders)}개)'
                                break
                            # 복원 판정 — 같은 실행 안의 스캔 전/후
                            # 목록 비교. 이력은 판단에 쓰지 않는다.
                            try:
                                post_files = _trash_file_paths(key)
                                post_set = set(post_files or [])
                                restored_files = [f for f in target_files
                                                  if f not in post_set]
                                restored_count = len(restored_files)
                                remaining_count = len(target_files) - restored_count
                            except Exception as e:
                                logger.warning(f'[휴지통 스캔] 스캔 후 휴지통 목록 읽기 실패: {e}')
                                restored_files = None
                                restored_count = None
                                remaining_count = None
                        elif step == 'empty':
                            # 비우기 전에 휴지통 파일 목록을 먼저 읽어 둔다 —
                            # 비우고 나면 DB에서 지워져 다시 볼 수 없다.
                            # 스캔 단계가 있었다면 스캔이 끝난 뒤라, 스캔에서
                            # 복원된 파일은 빠지고 여전히 없는 것만 남는다.
                            try:
                                emptied_files = _trash_file_paths(key)
                                if emptied_files is None:
                                    logger.info('[휴지통 스캔] 휴지통 목록을 읽지 못해 비운 파일 목록을 저장하지 않습니다.')
                            except Exception as e:
                                logger.warning(f'[휴지통 스캔] 비우기 전 휴지통 목록 읽기 실패: {e}')
                                emptied_files = None
                            empty_started = _now_text()
                            _write_status(key, 'trash', {
                                'job': None, 'mode': action,
                                'started_at': empty_started, 'finished_at': None,
                                'status': 'running', 'source': 'trash',
                                'job_name': name}, label='휴지통 스캔')
                            res = requests.get(f'{url}/library/sections/{key}/emptyTrash',
                                               headers={'X-Plex-Token': token,
                                                        'Accept': 'application/json'},
                                               timeout=600)
                            if res.status_code != 200:
                                _write_status(key, 'trash', {
                                    'job': None, 'mode': action,
                                    'started_at': empty_started, 'finished_at': _now_text(),
                                    'status': 'failed', 'source': 'trash',
                                    'job_name': name}, label='휴지통 스캔')
                                status, message = 'failed', f'비우기 실패 (응답 코드 {res.status_code})'
                                break
                            # 비우기는 job으로 돌아오므로, 완료 후 job을 조회해
                            # stats.removed_parts에서 비운 개수를 읽는다.
                            empty_job_id = _extract_job_id(res)
                            if empty_job_id:
                                _write_status(key, 'trash', {
                                    'job': empty_job_id, 'mode': action,
                                    'started_at': empty_started, 'finished_at': None,
                                    'status': 'running', 'source': 'trash',
                                    'job_name': name}, label='휴지통 스캔')
                                w = _wait_job(url, token, empty_job_id,
                                              poll_sec=EMPTY_POLL_SEC)
                                if w['result'] == 'cancelled':
                                    _write_status(key, 'trash', {
                                        'job': empty_job_id, 'mode': action,
                                        'started_at': empty_started,
                                        'finished_at': _now_text(),
                                        'status': 'cancelled', 'source': 'trash',
                                        'job_name': name}, label='휴지통 스캔')
                                    status, message = 'cancelled', '취소 요청으로 중단했습니다.'
                                    break
                                if w['result'] == 'not_found':
                                    _write_status(key, 'trash', {
                                        'job': empty_job_id, 'mode': action,
                                        'started_at': empty_started,
                                        'finished_at': _now_text(),
                                        'status': 'failed', 'source': 'trash',
                                        'job_name': name}, label='휴지통 스캔')
                                    status, message = 'failed', '비우기 작업 기록을 찾을 수 없습니다.'
                                    break
                                if w['result'] != 'done':
                                    _write_status(key, 'trash', {
                                        'job': empty_job_id, 'mode': action,
                                        'started_at': empty_started,
                                        'finished_at': _now_text(),
                                        'status': 'failed', 'source': 'trash',
                                        'job_name': name}, label='휴지통 스캔')
                                    status, message = 'failed', '비우기가 완료되지 않았습니다.'
                                    break
                                estat = (w.get('job') or {}).get('stats') or {}
                                emptied_count = _stat_int(estat.get('removed_parts'))
                            trash_entry = {
                                'job': empty_job_id, 'mode': action,
                                'started_at': empty_started, 'finished_at': _now_text(),
                                'status': 'done', 'source': 'trash',
                                'job_name': name}
                            if emptied_count is not None:
                                trash_entry['summary'] = f'{emptied_count}개 비움'
                            _write_status(key, 'trash', trash_entry, label='휴지통 스캔')
                            # 비우기에 성공했으니 이 라이브러리 휴지통은 0개 —
                            # 개수 캐시에 바로 반영한다.
                            try:
                                from .task_status import set_trash_count
                                set_trash_count(key, 0)
                            except Exception as e:
                                logger.warning(f'[휴지통 스캔] 휴지통 개수 캐시 반영 실패: {e}')
                            # 휴지통 개수가 바뀌므로 현황·건강도 개수 캐시를 새로 읽어 둔다
                            try:
                                from .task_status import refresh_trash_counts_async
                                refresh_trash_counts_async()
                            except Exception as e:
                                logger.warning(f'[휴지통 스캔] 휴지통 개수 새로 읽기 실패: {e}')
                            # 비운 라이브러리의 상세 캐시 목록은 낡으므로 지운다
                            try:
                                from .task_trash_view import clear_cache
                                clear_cache(key)
                            except Exception as e:
                                logger.warning(f'[휴지통 스캔] 휴지통 상세 캐시 정리 실패: {e}')
                    except Exception as e:
                        status, message = 'failed', f'{STAGE_LABEL.get(step, step)} 단계 오류: {e}'
                        break
            except Exception as e:
                logger.error(f'[휴지통 스캔] {title} 처리 실패: {e}')
                logger.error(traceback.format_exc())
                status, message = 'failed', str(e)
                TaskTrash._fail_running_status(key, name)
            # 집계 — '전체'는 대상 휴지통 파일 수, 복원은 스캔 전/후
            # 목록 비교(못 읽었으면 job stats로 대체), 누락은 전체-복원.
            # 추가 파일 목록은 휴지통 스캔에서 의미가 없어 수집하지 않는다.
            summed_stats = _sum_scan_stats(scan_stats_list)
            target_count = len(target_files) if target_files is not None else None
            eff_restored = restored_count
            if eff_restored is None and scan_job_ids:
                eff_restored = _stat_int(summed_stats.get('restored'))
            eff_remaining = remaining_count
            if (eff_remaining is None and eff_restored is not None
                    and target_count is not None):
                eff_remaining = target_count - eff_restored
            if scan_job_ids or scan_stats_list:
                synth_stats = {}
                if target_count is not None:
                    synth_stats['total'] = target_count
                if eff_restored is not None:
                    synth_stats['restored'] = eff_restored
                if eff_remaining is not None:
                    synth_stats['missing'] = eff_remaining
                if summed_stats.get('results'):
                    synth_stats['results'] = summed_stats['results']
                job_id_display = scan_job_ids[0]
                if len(scan_job_ids) > 1:
                    job_id_display += f' 외 {len(scan_job_ids) - 1}개'
                apply_scan_result(item, job_id_display, synth_stats, vfs_duration,
                                  scan_duration, None, key,
                                  label='휴지통 스캔', collect_append=False)
            else:
                item.vfs_duration = vfs_duration
                item.scan_duration = scan_duration
                if target_count is not None:
                    item.stat_total = target_count
            item.emptied_count = emptied_count
            if emptied_files is not None:
                item.emptied_files = json.dumps(emptied_files[:TRASH_FILES_MAX],
                                                ensure_ascii=False)
            if target_files is not None:
                item.target_files = json.dumps(target_files[:TRASH_FILES_MAX],
                                               ensure_ascii=False)
            if restored_files is not None:
                item.restored_files = json.dumps(restored_files[:TRASH_FILES_MAX],
                                                 ensure_ascii=False)
            if vfs_results is not None:
                item.vfs_results = json.dumps(vfs_results, ensure_ascii=False)
            if file_exists is not None:
                item.file_exists = json.dumps(file_exists, ensure_ascii=False)
            if scan_results is not None:
                item.scan_results = json.dumps(scan_results, ensure_ascii=False)
            if status == 'done':
                done_msg = _trash_done_message(
                    action, target_count, eff_restored, emptied_count,
                    vfs_failed_folders, scan_failed_folders, empty_skipped,
                    len(target_groups), eff_remaining)
                message = done_msg
                item.finish('done', done_msg)
            else:
                item.finish(status, message)
            logger.info(f'[휴지통 스캔] {title}: {status} {message}')
            if status == 'done' and 'scan' in steps:
                # 스캔으로 휴지통 구성이 바뀌었을 수 있어 상세 캐시를 지우고
                # 개수 캐시는 '오래됨'으로 표시해 다음 통계 계산 때 읽게 한다.
                # 스캔으로 미디어 수가 바뀌었을 수 있어 통계 재계산도 요청한다.
                try:
                    from .task_trash_view import clear_cache
                    clear_cache(key)
                except Exception as e:
                    logger.warning(f'[휴지통 스캔] 휴지통 상세 캐시 정리 실패: {e}')
                try:
                    from .task_status import mark_trash_counts_stale
                    mark_trash_counts_stale()
                except Exception as e:
                    logger.warning(f'[휴지통 스캔] 휴지통 개수 캐시 표시 실패: {e}')
                try:
                    from .task_status import request_stats
                    request_stats()
                except Exception as e:
                    logger.warning(f'[휴지통 스캔] 통계 재계산 요청 실패: {e}')
        # 진행 상태는 작업이 끝날 때까지 유지한다 — 라이브러리마다
        # 지우면 지운 틈에 다른 작업이 비어 있다고 보고 시작해버린다.
        # (주기적 스캔은 처음부터 끝날 때만 지운다.)
        if job_row_id is not None:
            if _trash_cancel_requested():
                _finish_job_row(job_row_id, 'cancelled', '취소 요청으로 중단했습니다.')
            else:
                _finish_job_row(job_row_id, 'done', '완료됐습니다.')
        _set_trash_current(None)
        logger.info(f'[휴지통 스캔] 종료: {name}')
