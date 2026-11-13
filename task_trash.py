import json
import os
import random
import time

import requests

from .model_scan import ModelScanItem
from .setup import *
from .shyni_backend import ShyniBackend
from .task_periodic import (WAIT_TIMEOUT_SEC, _extract_job_id,
                            _scan_poll_outcome, _stat_int, _write_status,
                            apply_scan_result, book_relay_total,
                            scan_summary_text)

logger = P.logger

# 옛 휴지통 스케줄러(run_empty_trash)는 호출처가 없어 z38에서 제거했다 (A3).
# 기준 경로 설정(scan_trash_schedule_anchors)은 TaskTrash가 쓰므로 유지한다.

TRASH_JOBS_KEY = 'trash_jobs'
TRASH_CURRENT_KEY = 'trash_current'
TRASH_CANCEL_KEY = 'trash_cancel'
TRASH_MAX_WAIT_KEY = 'trash_max_wait_min'
# 대기 확인 간격(초) — 앞선 작업이 끝났는지 확인하는 주기.
WAIT_POLL_SEC = 10
# 기본 최대 대기 시간(분) — 설정이 없거나 읽지 못하면 이 값을 쓴다.
DEFAULT_MAX_WAIT_MIN = 360
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


def _trash_skip_decision(periodic_parse_ok, periodic_section_key,
                         running_query_ok, running_section_keys, key):
    """섹션 시작 시 건너뜀 판정 — 순수 함수(하니스 검증용).
    가드 조회에 실패하면 fail-closed로 건너뛴다 (W5).
    (건너뜀 사유 | None)을 돌려준다."""
    if not periodic_parse_ok:
        return '주기적 스캔 상태를 확인할 수 없음'
    if not running_query_ok:
        return '실행 중인 스캔 항목을 확인할 수 없음'
    if periodic_section_key is not None and str(periodic_section_key) == str(key):
        return '주기적 스캔 실행 중'
    running_keys = {str(k) for k in (running_section_keys or []) if k is not None}
    if str(key) in running_keys:
        return '스캔 항목 실행 중'
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


def _max_wait_min():
    """최대 대기 시간(분) — 설정값, 없거나 숫자가 아니면 기본 360분.
    음수는 기본값으로 취급한다. 0이면 기다리지 않는다."""
    try:
        val = P.ModelSetting.get_int(TRASH_MAX_WAIT_KEY)
    except Exception:
        val = None
    if val is None or val < 0:
        return DEFAULT_MAX_WAIT_MIN
    return val


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
        logger.warning(f'[휴지통 작업] 대기 중 작업 조회 실패: {e}')
        return False


def _try_claim(idx, name, mode, job_started, job_row_id):
    """차례를 차지한다 — 진행 상태를 이 작업 것으로 쓰고 다시 읽어
    확인한다. 차지했으면 True.
    두 작업이 거의 동시에 차지하려는 틈을 줄이기 위해 쓰고 잠깐 쉰
    뒤에 확인한다. 확인에 실패하면 상대가 차지한 것이니 기다린다."""
    claim_id = (f'wait-{job_row_id}' if job_row_id
                else f'run-{idx}-{int(time.time() * 1000)}')
    _set_trash_current({
        'job_idx': idx, 'job_name': name, 'section_key': '',
        'section_title': '전체', 'stage': 'scan', 'mode': mode,
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
        logger.warning(f'[휴지통 작업] 대기 행 정리 실패: {e}')


def _wait_for_turn(idx, name, mode, job_started):
    """차례를 기다렸다가 차지한다.
    (대기 행 id 또는 None, 기다린 초 또는 None)을 돌려준다.
    바로 시작하면 (None, 0), 기다렸다 시작하면 (행 id, 기다린 초),
    기다리다 시간 초과하면 (None, None) — 이 때는 실패 행을 남긴다.
    최대 대기 시간이 0이면 기다리지 않고 바로 시작 실패로 끝낸다."""
    from .model_trash import ModelTrashItem
    if get_trash_current() is None and _try_claim(idx, name, mode, job_started, None):
        return (None, 0)
    max_min = _max_wait_min()
    if max_min <= 0:
        fail_trash_start(name, mode, '이미 진행 중인 휴지통 작업이 있어 시작하지 못했습니다.')
        return (None, None)
    running_name = (get_trash_current() or {}).get('job_name') or ''
    if running_name:
        wait_msg = f'앞선 작업({running_name}) 진행 중으로 끝나면 이어서 진행합니다.'
    else:
        wait_msg = '앞선 작업 진행 중으로 끝나면 이어서 진행합니다.'
    item = ModelTrashItem(name, '', '전체', mode)
    item.status = 'waiting'
    item.message = wait_msg
    try:
        item.save()
    except Exception as e:
        logger.error(f'[휴지통 작업] 대기 행 저장 실패: {e}')
        fail_trash_start(name, mode, '대기 기록을 남기지 못했습니다.')
        return (None, None)
    row_id = item.id
    t0 = time.time()
    deadline = t0 + max_min * 60
    while time.time() < deadline:
        time.sleep(WAIT_POLL_SEC)
        if get_trash_current() is None and _try_claim(idx, name, mode, job_started, row_id):
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
                logger.warning(f'[휴지통 작업] 대기 행 시작 기록 실패: {e}')
            return (row_id, waited)
    _finish_job_row(row_id, 'failed',
                    f'앞선 작업이 끝나기를 기다리다 시간 초과됐습니다. (최대 대기 {_fmt_wait(max_min * 60)})')
    logger.warning(f'[휴지통 작업] {name}: 최대 대기 시간 초과로 시작하지 않습니다.')
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
        logger.warning(f'[휴지통 작업] 시작 오류 기록 실패: {e}')


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


def fail_trash_start(job_name, mode, reason):
    """작업 단위 시작 실패 — 이력 행을 남기고 배너용 오류를 기록한다.
    이력 행 저장이 실패해도 배너 기록은 시도한다."""
    try:
        from .model_trash import ModelTrashItem
        item = ModelTrashItem(job_name, '', '전체', mode)
        item.finish('failed', reason)
    except Exception as e:
        logger.error(f'[휴지통 작업] 시작 실패 이력 기록 실패: {e}')
    _record_trash_start_error(job_name, reason)


def _trash_cancel_requested():
    return (P.ModelSetting.get(TRASH_CANCEL_KEY) or '') == '1'


def request_trash_cancel():
    P.ModelSetting.set(TRASH_CANCEL_KEY, '1')


def _now_text():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _wait_vfs(addr, jobid, timeout=WAIT_TIMEOUT_SEC):
    """rclone VFS 새로고침 job 완료를 기다린다.
    {'result': done|failed|cancelled, 'duration': 초}를 돌려준다.
    기한은 다른 대기(6시간)와 맞췄다 (R18)."""
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


def _wait_job(url, token, job_id, timeout=WAIT_TIMEOUT_SEC, poll_sec=SCAN_POLL_SEC):
    """shyni job 완료를 기다린다 (스캔·비우기 공통).
    {'result': done|failed|cancelled|not_found, 'job': job 응답 또는 None,
     'duration': 초}를 돌려준다. job 조회에서 404를 받으면 즉시 실패로
    끝낸다 (W3). 폴링 중 일시적 오류는 재시도하고 연속 3회 실패 시에만
    실패로 확정한다."""
    t0 = time.time()
    deadline = time.time() + timeout
    fail_count = 0
    while time.time() < deadline:
        if _trash_cancel_requested():
            return {'result': 'cancelled', 'job': None,
                    'duration': int(time.time() - t0)}
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
        if res.status_code != 200:
            fail_count += 1
            if fail_count >= POLL_FAIL_MAX:
                return {'result': 'failed', 'job': None,
                        'duration': int(time.time() - t0)}
        else:
            fail_count = 0
        time.sleep(poll_sec)
    return {'result': 'failed', 'job': None, 'duration': int(time.time() - t0)}


def _wait_scan(url, token, job_id, timeout=WAIT_TIMEOUT_SEC):
    """shyni 스캔 job 완료를 기다린다 — _wait_job과 같은 결과를 돌려준다."""
    return _wait_job(url, token, job_id, timeout=timeout, poll_sec=SCAN_POLL_SEC)


def _compute_skip_reason(key):
    """이 섹션을 건너뛸 사유 — None이면 진행한다.
    가드 조회에 실패하면 fail-closed로 건너뛴다 (W5).
    섹션 시작 시마다 다시 읽는다."""
    periodic_parse_ok = True
    periodic_section_key = None
    raw_cur = P.ModelSetting.get('periodic_current')
    if raw_cur:
        try:
            cur = json.loads(raw_cur)
            if isinstance(cur, dict):
                periodic_section_key = cur.get('section_key')
            else:
                periodic_parse_ok = False
        except Exception:
            periodic_parse_ok = False
    running_query_ok = True
    running_keys = []
    try:
        running = F.db.session.query(ModelScanItem).filter(
            ModelScanItem.shyni_status == 'RUNNING').all()
        running_keys = [it.section_id for it in running if it.section_id]
    except Exception as e:
        running_query_ok = False
        logger.warning(f'[휴지통 작업] 실행 중인 스캔 항목 조회 실패: {e}')
    return _trash_skip_decision(periodic_parse_ok, periodic_section_key,
                                running_query_ok, running_keys, key)


class TaskTrash:
    """휴지통 주기 작업 실행 — 선택한 라이브러리를 하나씩 순차로 처리한다.

    동작(새로고침/스캔/비우기 조합)을 단계별로 실행하고, 단계 완료를 확인한
    뒤 다음 단계로 넘어간다. 진행 상태는 ModelSetting trash_current에 기록해
    화면이 공유하고, 취소는 trash_cancel 플래그로 요청받는다.
    완료된 스캔의 집계와 비운 개수는 이력 행에 남기고, 진행 상황은 대시보드
    섹션 상태(status_sec_N)에도 수동 작업과 같은 모양으로 기록한다.
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
                                     mode, f'실행 중 오류가 발생했습니다: {e}')

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
            logger.warning(f'[휴지통 작업] 현황 상태 정리 실패: {e}')

    @staticmethod
    def _run(idx, mode):
        from .model_trash import ModelTrashItem
        from .mod_status import ModuleStatus
        jobs = get_trash_jobs()
        if idx < 0 or idx >= len(jobs):
            logger.warning(f'[휴지통 작업] 없는 작업 번호: {idx}')
            _record_trash_start_error('', f'잘못된 작업 번호라 시작하지 못했습니다: {idx}')
            return
        job = jobs[idx]
        name = job.get('name') or f'작업 {idx + 1}'
        # 같은 작업이 이미 돌거나 기다리고 있으면 중복으로 세지 않는다.
        if _same_job_busy(name):
            logger.info(f'[휴지통 작업] {name}: 이미 실행 중이거나 대기 중이라 건너뜁니다.')
            return
        action = job.get('action') or 'empty'
        steps = ACTION_STEPS.get(action, ['empty'])
        P.ModelSetting.set(TRASH_CANCEL_KEY, '')
        job_started = _now_text()
        logger.info(f'[휴지통 작업] 시작: {name} ({ACTION_LABEL.get(action, action)}, {mode})')

        # 안전장치: 기준 경로가 하나라도 보이지 않으면 전체를 중단한다
        anchors = [a.strip() for a in
                   (P.ModelSetting.get('scan_trash_schedule_anchors') or '').splitlines()
                   if a.strip()]
        for anchor in anchors:
            if not os.path.exists(anchor):
                fail_trash_start(name, mode, f'기준 경로를 찾을 수 없습니다 — {anchor}')
                logger.error(f'[휴지통 작업] 중단: 기준 경로 없음 — {anchor}')
                return

        sections, err = ModuleStatus._sections()
        if err:
            fail_trash_start(name, mode, err)
            return
        selected = {str(s) for s in (job.get('sections') or [])}
        targets = [s for s in sections if not selected or str(s['key']) in selected]
        if not targets:
            fail_trash_start(name, mode, '대상 라이브러리가 없습니다.')
            return

        url, token = ShyniBackend.get_config()
        if not url or not token:
            fail_trash_start(name, mode, '기본 설정에서 shyni 주소와 토큰을 먼저 설정해 주세요.')
            return
        _clear_trash_start_error()

        # 앞선 작업이 돌고 있으면 기다렸다가 차례가 되면 실행한다.
        # 기다리다 시간 초과하면 _wait_for_turn이 실패 행을 남긴다.
        job_row_id, _waited = _wait_for_turn(idx, name, mode, job_started)
        if _waited is None:
            return

        for sec in targets:
            key = str(sec['key'])
            title = sec.get('title') or key
            if _trash_cancel_requested():
                item = ModelTrashItem(name, key, title, mode)
                item.finish('cancelled', '취소 요청으로 실행하지 않았습니다.')
                continue
            # 건너뜀 가드는 섹션 시작 시마다 다시 읽는다 (W5)
            skip_reason = _compute_skip_reason(key)
            if skip_reason is not None:
                item = ModelTrashItem(name, key, title, mode)
                item.finish('cancelled', f'건너뜀 ({skip_reason})')
                logger.info(f'[휴지통 작업] {title}: 건너뜀 ({skip_reason})')
                continue
            try:
                st = ModuleStatus._get_state(int(key))
                busy = any((st.get(k) or {}).get('status') == 'running'
                           for k in ('vfs', 'scan', 'meta'))
            except Exception:
                busy = False
            if busy:
                item = ModelTrashItem(name, key, title, mode)
                item.finish('cancelled', '건너뜀 (현황에서 작업 중)')
                continue

            item = ModelTrashItem(name, key, title, mode)
            item.save()
            status, message = 'done', ''
            vfs_duration = None
            scan_duration = None
            scan_job_id = None
            scan_stats = None
            relay_total = None
            pre_max_id = None
            emptied_count = None
            try:
                for step in steps:
                    if _trash_cancel_requested():
                        status, message = 'cancelled', '취소 요청으로 중단했습니다.'
                        break
                    _set_trash_current({
                        'job_idx': idx, 'job_name': name, 'section_key': key,
                        'section_title': title,
                        'stage': step, 'job_started_at': job_started,
                        'section_started_at': _now_text(),
                        'mode': mode, 'job_row_id': job_row_id})
                    try:
                        if step == 'refresh':
                            addr, remote = ModuleStatus._rc_target(sec['path'])
                            if not addr:
                                status, message = 'failed', 'VFS 새로고침 규칙과 맞지 않는 경로입니다.'
                                break
                            refresh_started = _now_text()
                            _write_status(key, 'vfs', {
                                'job': None, 'path': sec.get('path', ''),
                                'started_at': refresh_started, 'finished_at': None,
                                'status': 'running', 'source': 'trash',
                                'job_name': name}, label='휴지통 작업')
                            ret = ModuleStatus._rc(addr, 'vfs/refresh',
                                                   {'dir': remote, 'recursive': 'true',
                                                    '_async': 'true'})
                            vfs_jobid = ret.get('jobid')
                            if vfs_jobid:
                                _write_status(key, 'vfs', {
                                    'job': vfs_jobid, 'path': sec.get('path', ''),
                                    'started_at': refresh_started, 'finished_at': None,
                                    'status': 'running', 'source': 'trash',
                                    'job_name': name}, label='휴지통 작업')
                            w = _wait_vfs(addr, vfs_jobid)
                            vfs_duration = w['duration']
                            _write_status(key, 'vfs', {
                                'job': vfs_jobid, 'path': sec.get('path', ''),
                                'started_at': refresh_started, 'finished_at': _now_text(),
                                'status': {'done': 'done', 'failed': 'failed',
                                           'cancelled': 'cancelled'}.get(w['result'], 'failed'),
                                'source': 'trash', 'job_name': name}, label='휴지통 작업')
                            if w['result'] != 'done':
                                status = 'cancelled' if w['result'] == 'cancelled' else 'failed'
                                message = ('취소 요청으로 중단했습니다.' if w['result'] == 'cancelled'
                                           else 'VFS 새로고침이 끝나지 않았습니다.')
                                break
                        elif step == 'scan':
                            # 스캔 시작 전 MAX(media_parts.id) 기록 —
                            # 추가 파일 목록 2차 방법용 기준점이다.
                            try:
                                from . import db_support
                                if db_support.db_ready():
                                    pre_max_id = db_support.parts_max_id()
                            except Exception as e:
                                logger.warning(f'[휴지통 작업] 추가 파일 기준점 기록 실패: {e}')
                            scan_started = _now_text()
                            _write_status(key, 'scan', {
                                'job': None, 'path': sec.get('path', ''),
                                'started_at': scan_started, 'finished_at': None,
                                'status': 'running', 'source': 'trash',
                                'job_name': name}, label='휴지통 작업')
                            res = requests.get(f'{url}/library/sections/{key}/refresh',
                                               headers={'X-Plex-Token': token,
                                                        'Accept': 'application/json'},
                                               timeout=30)
                            res.raise_for_status()
                            scan_job_id = _extract_job_id(res)
                            if scan_job_id:
                                _write_status(key, 'scan', {
                                    'job': scan_job_id, 'path': sec.get('path', ''),
                                    'started_at': scan_started, 'finished_at': None,
                                    'status': 'running', 'source': 'trash',
                                    'job_name': name}, label='휴지통 작업')
                                cur = get_trash_current() or {}
                                cur['scan_job'] = scan_job_id
                                _set_trash_current(cur)
                                w = _wait_scan(url, token, scan_job_id)
                                scan_duration = w['duration']
                                scan_stats = (w.get('job') or {}).get('stats') or {}
                                relay_total = book_relay_total(scan_stats, key)
                                summary = scan_summary_text(scan_stats, relay_total)
                                scan_entry = {
                                    'job': scan_job_id, 'path': sec.get('path', ''),
                                    'started_at': scan_started, 'finished_at': _now_text(),
                                    'status': {'done': 'done', 'failed': 'failed',
                                               'cancelled': 'cancelled',
                                               'not_found': 'failed'}.get(w['result'], 'failed'),
                                    'source': 'trash', 'job_name': name}
                                if summary:
                                    scan_entry['summary'] = summary
                                _write_status(key, 'scan', scan_entry, label='휴지통 작업')
                                if w['result'] == 'cancelled':
                                    status, message = 'cancelled', '취소 요청으로 중단했습니다.'
                                    break
                                if w['result'] == 'not_found':
                                    status, message = 'failed', '스캔 작업 기록을 찾을 수 없습니다.'
                                    break
                                if w['result'] != 'done':
                                    job_err = ((w.get('job') or {}).get('error') or '')
                                    status = 'failed'
                                    message = (f"스캔 실패: {job_err}" if job_err
                                               else '스캔이 완료되지 않았습니다.')
                                    break
                            else:
                                _write_status(key, 'scan', {
                                    'job': None, 'path': sec.get('path', ''),
                                    'started_at': scan_started, 'finished_at': _now_text(),
                                    'status': 'done', 'source': 'trash',
                                    'job_name': name}, label='휴지통 작업')
                        elif step == 'empty':
                            empty_started = _now_text()
                            _write_status(key, 'trash', {
                                'job': None, 'mode': action,
                                'started_at': empty_started, 'finished_at': None,
                                'status': 'running', 'source': 'trash',
                                'job_name': name}, label='휴지통 작업')
                            res = requests.get(f'{url}/library/sections/{key}/emptyTrash',
                                               headers={'X-Plex-Token': token,
                                                        'Accept': 'application/json'},
                                               timeout=600)
                            if res.status_code != 200:
                                _write_status(key, 'trash', {
                                    'job': None, 'mode': action,
                                    'started_at': empty_started, 'finished_at': _now_text(),
                                    'status': 'failed', 'source': 'trash',
                                    'job_name': name}, label='휴지통 작업')
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
                                    'job_name': name}, label='휴지통 작업')
                                w = _wait_job(url, token, empty_job_id,
                                              poll_sec=EMPTY_POLL_SEC)
                                if w['result'] == 'cancelled':
                                    _write_status(key, 'trash', {
                                        'job': empty_job_id, 'mode': action,
                                        'started_at': empty_started,
                                        'finished_at': _now_text(),
                                        'status': 'cancelled', 'source': 'trash',
                                        'job_name': name}, label='휴지통 작업')
                                    status, message = 'cancelled', '취소 요청으로 중단했습니다.'
                                    break
                                if w['result'] == 'not_found':
                                    _write_status(key, 'trash', {
                                        'job': empty_job_id, 'mode': action,
                                        'started_at': empty_started,
                                        'finished_at': _now_text(),
                                        'status': 'failed', 'source': 'trash',
                                        'job_name': name}, label='휴지통 작업')
                                    status, message = 'failed', '비우기 작업 기록을 찾을 수 없습니다.'
                                    break
                                if w['result'] != 'done':
                                    _write_status(key, 'trash', {
                                        'job': empty_job_id, 'mode': action,
                                        'started_at': empty_started,
                                        'finished_at': _now_text(),
                                        'status': 'failed', 'source': 'trash',
                                        'job_name': name}, label='휴지통 작업')
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
                            _write_status(key, 'trash', trash_entry, label='휴지통 작업')
                            # 비우기에 성공했으니 이 라이브러리 휴지통은 0개 —
                            # 개수 캐시에 바로 반영한다.
                            try:
                                from .task_status import set_trash_count
                                set_trash_count(key, 0)
                            except Exception as e:
                                logger.warning(f'[휴지통 작업] 휴지통 개수 캐시 반영 실패: {e}')
                            # 휴지통 개수가 바뀌므로 현황·건강도 개수 캐시를 새로 읽어 둔다
                            try:
                                from .task_status import refresh_trash_counts_async
                                refresh_trash_counts_async()
                            except Exception as e:
                                logger.warning(f'[휴지통 작업] 휴지통 개수 새로 읽기 실패: {e}')
                            # 비운 라이브러리의 상세 캐시 목록은 낡으므로 지운다
                            try:
                                from .task_trash_view import clear_cache
                                clear_cache(key)
                            except Exception as e:
                                logger.warning(f'[휴지통 작업] 휴지통 상세 캐시 정리 실패: {e}')
                    except Exception as e:
                        status, message = 'failed', f'{STAGE_LABEL.get(step, step)} 단계 오류: {e}'
                        break
            except Exception as e:
                logger.error(f'[휴지통 작업] {title} 처리 실패: {e}')
                logger.error(traceback.format_exc())
                status, message = 'failed', str(e)
                TaskTrash._fail_running_status(key, name)
            # 스캔 단계가 있었다면 집계·추가 파일 목록을 이력에 남긴다
            if scan_job_id or scan_stats is not None:
                apply_scan_result(item, scan_job_id, scan_stats, vfs_duration,
                                  scan_duration, pre_max_id, key,
                                  label='휴지통 작업', relay_total=relay_total)
            else:
                item.vfs_duration = vfs_duration
                item.scan_duration = scan_duration
            item.emptied_count = emptied_count
            if status == 'done':
                summary = scan_summary_text(scan_stats, relay_total)
                done_msg = message
                if not done_msg:
                    bits = []
                    if summary:
                        bits.append(summary)
                    if emptied_count is not None:
                        bits.append(f'{emptied_count}개 비움')
                    done_msg = ('완료됐습니다. (' + ' · '.join(bits) + ')'
                                if bits else '완료됐습니다.')
                item.finish('done', done_msg)
            else:
                item.finish(status, message)
            logger.info(f'[휴지통 작업] {title}: {status} {message}')
            if status == 'done' and 'scan' in steps:
                # 스캔으로 휴지통 구성이 바뀌었을 수 있어 상세 캐시를 지우고
                # 개수 캐시는 '오래됨'으로 표시해 다음 통계 계산 때 읽게 한다.
                # 스캔으로 미디어 수가 바뀌었을 수 있어 통계 재계산도 요청한다.
                try:
                    from .task_trash_view import clear_cache
                    clear_cache(key)
                except Exception as e:
                    logger.warning(f'[휴지통 작업] 휴지통 상세 캐시 정리 실패: {e}')
                try:
                    from .task_status import mark_trash_counts_stale
                    mark_trash_counts_stale()
                except Exception as e:
                    logger.warning(f'[휴지통 작업] 휴지통 개수 캐시 표시 실패: {e}')
                try:
                    from .task_status import request_stats
                    request_stats()
                except Exception as e:
                    logger.warning(f'[휴지통 작업] 통계 재계산 요청 실패: {e}')
        # 진행 상태는 작업이 끝날 때까지 유지한다 — 라이브러리마다
        # 지우면 지운 틈에 다른 작업이 비어 있다고 보고 시작해버린다.
        # (주기적 스캔은 처음부터 끝날 때만 지운다.)
        if job_row_id is not None:
            if _trash_cancel_requested():
                _finish_job_row(job_row_id, 'cancelled', '취소 요청으로 중단했습니다.')
            else:
                _finish_job_row(job_row_id, 'done', '완료됐습니다.')
        _set_trash_current(None)
        logger.info(f'[휴지통 작업] 종료: {name}')
