import json
import os
import random
import threading
import time

import requests

from .extensions import (rc_job_status, rc_server_by_addr, sync_pid_kill,
                         vfs_refresh_async_start, vfs_refresh_sync_spawn)
from .setup import *
from .shyni_backend import ShyniBackend
from .task_periodic import (CANCEL_RESULT_MESSAGE, _cancel_shyni_scan,
                            _extract_job_id, _fetch_compat_job,
                            _record_scan_progress, _scan_poll_outcome,
                            _stat_int, _write_status,
                            apply_scan_result, scan_summary_text)

logger = P.logger

# 지금 이 스레드에서 돌고 있는 작업의 식별자 — start()가 _run을 같은
# 스레드에서 부르므로, _run이 차지한 뒤에 오류가 나면 start()의
# 예외 처리에서 이 값으로 자기 진행 상태만 지울 수 있다. 스레드마다
# 따로 들고 있어 동시에 도는 작업이 서로를 건드리지 않는다.
_ACTIVE = threading.local()

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
# VFS 비동기 사용 설정 키 — 켜면(기본값) 새로고침 작업을 걸어 두고
# 확인하면서 기다리고, 끄면 동기 방식으로 진행한다.
TRASH_VFS_ASYNC_KEY = 'trash_vfs_use_async'
# 스캔·비우기 대기 기한 설정 키(분) — shyni job이 끝나기를 이 시간까지
# 기다린다. 기한이 됐을 때 shyni에서 job이 아직 진행 중이면 이 시간만큼
# 기한을 연장하며 계속 기다린다. 설정이 없거나 0 이하면 기본값을 쓴다.
TRASH_SCAN_TIMEOUT_KEY = 'trash_scan_timeout_min'
DEFAULT_SCAN_TIMEOUT_MIN = 360
# 가장 최근 시작 실패 — 작업 관리 배너가 읽는다 (주기적 스캔과 같은 구조).
TRASH_START_ERROR_KEY = 'trash_last_start_error'
# 시작 실패 기록 보관 기한(초) — 이보다 오래된 오류는 배너에 띄우지 않는다.
TRASH_START_ERROR_MAX_AGE_SEC = 3600

# 폴링 실패 처리 (0.9.9h, W7 개정): 일시적 오류는 원인을 로그에 남기고
# 계속 기다린다 — 확인 실패만으로는 실패로 확정하지 않는다. 실패 확정은
# shyni/rclone이 직접 실패라고 답하거나, 작업 기록이 없을 때(404/not found),
# 또는 기한 안에 한 번도 확인이 안 됐을 때만 한다.
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


def _trash_file_entries(key):
    """휴지통 파일 목록 + 상태별 경과 — 관리자 API 우선, Relay 폴백.
    [{'p': 경로, 's': 'missing'|'trashed', 'd': 경과일}].
    둘 다 안 되면 None. 항목 형식을 못 읽으면 경로만 읽어
    상태·일수 없이 돌려준다 (옛 shyni 대비)."""
    try:
        from . import shyni_admin
        entries = shyni_admin.api_trash_file_entries(key)
        if entries is not None:
            return entries
    except Exception as e:
        logger.warning(f'[휴지통 스캔] 휴지통 목록 API 읽기 실패: {e}')
    try:
        from . import db_support
        if db_support.db_ready():
            return db_support.trash_file_entries(key)
    except Exception as e:
        logger.warning(f'[휴지통 스캔] 휴지통 목록 Relay 읽기 실패: {e}')
    try:
        paths = _trash_file_paths(key)
        if paths is not None:
            return [{'p': p, 's': None, 'd': None} for p in paths]
    except Exception:
        pass
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
                        folder_count, remaining, retained=None):
    """완료 메시지 조립 — 순수 함수(하니스 검증용).
    숫자는 전부 파일 수. restored가 None이면 복원 여부를 확인하지
    못한 것이고, emptied_count가 None이면 비운 개수를 job stats에서
    읽지 못한 것이다 (그 경우 remaining으로 대체한다).
    retained(보관 기간이 남아 남은 수)가 있으면 선택 비우기와 같은
    문구('N개는 보관 기간이 남아 남음')를 함께 붙인다."""
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
                if retained:
                    bits.append(f'{retained}개는 보관 기간이 남아 남음')
        if vfs_failed:
            bits.append(f'VFS {vfs_failed}개 폴더 실패')
        if scan_failed:
            bits.append(f'스캔 실패 폴더 {scan_failed}개')
    elif action == 'empty':
        emptied = (emptied_count if emptied_count is not None
                   else target_count)
        if emptied is not None:
            bits.append(f'휴지통 파일 {emptied}개 비움')
        if retained:
            bits.append(f'{retained}개는 보관 기간이 남아 남음')
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


def _parse_trash_currents(raw):
    """진행 상태 저장값 → 목록. 구 형식(객체 하나)이면 목록으로 감싸고,
    비어 있거나 깨졌으면 빈 목록을 돌려준다. 순수 함수(하니스 검증용)."""
    if not raw:
        return []
    try:
        cur = json.loads(raw)
    except Exception:
        return []
    if isinstance(cur, dict):
        return [cur]
    if isinstance(cur, list):
        return [c for c in cur if isinstance(c, dict)]
    return []


def get_trash_currents():
    """지금 돌고 있는 휴지통 스캔 진행 상태 목록 (claim_id 순)."""
    return _parse_trash_currents(P.ModelSetting.get(TRASH_CURRENT_KEY))


def _save_trash_currents(curs):
    P.ModelSetting.set(TRASH_CURRENT_KEY, json.dumps(curs, ensure_ascii=False))


def _upsert_trash_current(cur):
    """진행 상태 한 건을 넣거나 고친다 — claim_id가 같은 항목이 있으면
    그 자리에서 합치고(새 값이 이긴다), 없으면 뒤에 붙인다.
    합치는 방식이라 차지할 때 넣어 둔 claimed_at 같은 값은
    섹션이 바뀌어도 유지된다."""
    curs = get_trash_currents()
    claim_id = (cur or {}).get('claim_id')
    for i, c in enumerate(curs):
        if claim_id is not None and c.get('claim_id') == claim_id:
            merged = dict(c)
            merged.update(cur)
            curs[i] = merged
            _save_trash_currents(curs)
            return
    curs.append(cur)
    _save_trash_currents(curs)


def _remove_trash_current(claim_id):
    """claim_id가 같은 진행 상태를 지운다. 없으면 그대로 둔다."""
    if claim_id is None:
        return
    curs = [c for c in get_trash_currents() if c.get('claim_id') != claim_id]
    _save_trash_currents(curs)


def _clear_trash_currents():
    P.ModelSetting.set(TRASH_CURRENT_KEY, '')


def update_trash_current_job_idx(claim_id, new_idx):
    """진행 상태의 작업 번호를 바꾼다 — 작업 관리에서 순서를 바꿔
    저장했을 때 진행 중인 작업의 위치를 새 목록에 맞게 고친다.
    claim_id로 그 작업의 진행 상태만 고친다."""
    curs = get_trash_currents()
    for cur in curs:
        if cur.get('claim_id') == claim_id:
            if cur.get('job_idx') == new_idx:
                return
            cur['job_idx'] = new_idx
            _save_trash_currents(curs)
            return


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


def _scan_timeout_sec():
    """스캔·비우기 대기 기한(초) — 설정값(분)을 초로 바꾼다.
    없거나 숫자가 아니거나 0 이하면 기본 360분으로 동작한다."""
    try:
        val = P.ModelSetting.get_int(TRASH_SCAN_TIMEOUT_KEY)
    except Exception:
        val = None
    if val is None or val <= 0:
        return DEFAULT_SCAN_TIMEOUT_MIN * 60
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
        for cur in get_trash_currents():
            if (cur.get('job_name') or '') == (name or ''):
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


# 규칙 1 — 같은 모듈 안에서 같은 라이브러리가 진행 중일 때 내는 문구.
# 동작 종류(새로고침·스캔·새로고침 후 스캔·새로고침 후 스캔 후 비우기·
# 비우기)와 무관하게 이 문구로 거절한다.
LIBRARY_BUSY_MESSAGE = '현재 해당 라이브러리 작업이 진행중 입니다.'


def _same_library_busy(first_key, exclude_claim_id=None):
    """규칙 1 — 같은 모듈 안에서 첫 라이브러리가 이미 실행 중이면 True.
    동작 종류와 무관하게 막는다. 대기 중인 작업은 자리를 예약하지
    않으므로 세지 않는다 — 대기 중인 작업까지 세면 대기 중인 작업끼리
    서로를 막아 아무도 시작하지 못한다. 자기 claim_id는 뺀다 (대기 중
    재검사 때 자기 대기 표시 때문에 스스로 막히지 않게)."""
    if first_key is None:
        return False
    try:
        for cur in get_trash_currents():
            if (exclude_claim_id is not None
                    and cur.get('claim_id') == exclude_claim_id):
                continue
            if cur.get('stage') == 'waiting':
                continue
            if str(cur.get('section_key') or '') == str(first_key):
                return True
    except Exception:
        pass
    return False


def _rule1_survives(claim_id, first_key, claimed_at):
    """규칙 1 차지 재확인 — 차지한 자리가 살아남으면 True.
    게이트를 보고 차지하는 사이에 같은 라이브러리의 다른 작업이
    차지했을 수 있어, 차지한 뒤에 한 번 더 확인한다. 동시에 차지한
    작업끼리는 차지한 시각(claimed_at) 순서로 가른다 — 앞 순서가
    자리를 갖고, 뒤 순서가 물러난다. 차지한 시각이 없는 옛 항목은
    가장 먼저 차지한 것으로 센다. 대기 중인 항목은 세지 않는다.
    읽기에 실패하면 True로 열어 둔다."""
    try:
        my_at = int(claimed_at) if claimed_at is not None else 0
    except Exception:
        my_at = 0
    try:
        curs = get_trash_currents()
    except Exception:
        return True
    for cur in curs:
        if not isinstance(cur, dict):
            continue
        if cur.get('claim_id') == claim_id:
            continue
        if cur.get('stage') == 'waiting':
            continue
        if str(cur.get('section_key') or '') != str(first_key):
            continue
        try:
            other_at = int(cur.get('claimed_at') or 0)
        except Exception:
            other_at = 0
        # 나보다 먼저 차지한 작업이 있으면 내가 물러난다. 나보다
        # 늦게 차지한 작업은 그쪽이 나를 보고 스스로 물러난다.
        # 같은 시각이면 claim_id 문자열 순서로 가른다.
        if (other_at, str(cur.get('claim_id') or '')) < (my_at, str(claim_id)):
            return False
    return True


def _slot_free(first_key, action, exclude_claim_id=None):
    """이 작업이 시작할 자리가 있는지 — (자리 있음, 막는 사유).
    규칙 1이 규칙 2보다 먼저다. 규칙 1: 같은 모듈 안에서 같은
    라이브러리가 실행 중이면 동작 종류와 무관하게 막는다.
    규칙 2: 스캔이 들어간 동작만 동시 스캔 수 게이트를 본다.
    새로고침만·비우기만 하는 동작은 스캔 자리를 쓰지 않지만
    규칙 1은 본다. 첫 라이브러리를 기준으로 검사한다 — 나중
    라이브러리는 섹션 시작 때 가드가 다시 본다."""
    if _same_library_busy(first_key, exclude_claim_id=exclude_claim_id):
        return False, LIBRARY_BUSY_MESSAGE
    if 'scan' not in str(action or ''):
        return True, None
    try:
        from .mod_status import ModuleStatus
        msg = ModuleStatus.library_gate_check(first_key)
    except Exception as e:
        logger.warning(f'[휴지통 스캔] 게이트 검사 실패: {e}')
        return True, None
    return msg is None, msg


def _try_claim(idx, name, mode, job_started, job_row_id, claim_id,
               section_label='전체', action=None, first_key=None):
    """차례를 차지한다 — 진행 상태를 목록에 넣고 다시 읽어
    확인한다. 차지했으면 True.
    두 작업이 거의 동시에 차지하려는 틈을 줄이기 위해 쓰고 잠깐 쉰
    뒤에 확인한다. 확인에 실패하면 상대가 차지한 것이니 기다린다.
    section_label — 진행 상태에 표시할 라이브러리명. 대상이 1개면
    그 이름, 여러 개면 '전체'. first_key — 첫 라이브러리 번호.
    차지한 시점부터 자리를 세게 진행 상태에 미리 넣어 둔다."""
    if str(action or '').startswith('refresh'):
        stage = 'refresh'
    elif 'scan' in str(action or ''):
        stage = 'scan'
    else:
        stage = 'empty'
    claimed_at = int(time.time() * 1000)
    _upsert_trash_current({
        'job_idx': idx, 'job_name': name,
        'section_key': str(first_key) if first_key is not None else '',
        'section_title': section_label, 'stage': stage, 'mode': mode,
        'action': action,
        'job_started_at': job_started, 'section_started_at': _now_text(),
        'claim_id': claim_id, 'job_row_id': job_row_id,
        'claimed_at': claimed_at})
    time.sleep(random.uniform(0.3, 1.0))
    curs = get_trash_currents()
    if not any(c.get('claim_id') == claim_id for c in curs):
        return False
    # 규칙 1 재확인 — 게이트를 보고 차지하는 사이에 같은
    # 라이브러리의 다른 작업이 차지했을 수 있다. 스캔이 안 들어간
    # 동작도 같은 라이브러리와 겹치면 물러난다. 동시에 차지한
    # 작업끼리는 차지한 시각 순서로 가른다.
    try:
        survives1 = _rule1_survives(claim_id, first_key, claimed_at)
    except Exception as e:
        logger.warning(f'[휴지통 스캔] 규칙 1 재확인 실패: {e}')
        survives1 = True
    if not survives1:
        _remove_trash_current(claim_id)
        return False
    if 'scan' in str(action or ''):
        # 자리 재확인 — 게이트를 보고 차지하는 사이에 다른 작업이
        # 차지했을 수 있다. 동시에 차지한 작업끼리는 차지한 시각
        # 순서로 가른다 (앞 순서가 자리를 갖고, 뒤 순서가 물러난다).
        try:
            survives = ModuleStatus.claim_survives(claim_id, first_key, claimed_at)
        except Exception as e:
            logger.warning(f'[휴지통 스캔] 자리 재확인 실패: {e}')
            survives = True
        if not survives:
            _remove_trash_current(claim_id)
            return False
    return True


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


def _wait_for_turn(idx, name, mode, job_started, section_label='전체',
                   action=None, first_key=None):
    """차례를 기다렸다가 차지한다.
    (대기 행 id 또는 None, 기다린 초 또는 None, claim_id 또는 None)을
    돌려준다. 바로 시작하면 (None, 0, claim_id), 기다렸다 시작하면
    (행 id, 기다린 초, claim_id), 기다리다 시간 초과하면
    (None, None, None) — 이 때는 실패 행을 남긴다.
    작업 대기 시간이 0이면 기다리지 않고 바로 건너뜀으로 끝낸다.
    '지금 실행'(수동)은 기다리지 않고 자리가 없으면 바로 건너뜀으로
    끝낸다. 자리가 없어 시작하지 못한 것은 실패가 아니라 건너뜀이다.
    규칙 1(같은 라이브러리가 실행 중)에 걸린 경우도 기다리지 않고
    건너뜀으로 끝낸다 — 기다리는 것은 규칙 2(동시 스캔 수 제한)에
    걸린 경우만이다.
    section_label — 대기 행에 표시할 라이브러리명. 대상이 1개면
    그 이름, 여러 개면 '전체'. first_key — 첫 라이브러리 번호."""
    from .model_trash import ModelTrashItem
    claim_id = f'run-{idx}-{int(time.time() * 1000)}'
    free, gate_msg = _slot_free(first_key, action,
                                exclude_claim_id=claim_id)
    if free and _try_claim(idx, name, mode, job_started, None, claim_id,
                           section_label, action, first_key):
        return (None, 0, claim_id)
    if mode == 'manual':
        fail_trash_start(name, mode,
                         gate_msg or '이미 진행 중인 휴지통 스캔이 있어 시작하지 못했습니다.',
                         section_label, action, status='skipped')
        return (None, None, None)
    max_min = _max_wait_min()
    if max_min <= 0:
        fail_trash_start(name, mode,
                         gate_msg or '이미 진행 중인 휴지통 스캔이 있어 시작하지 못했습니다.',
                         section_label, action, status='skipped')
        return (None, None, None)
    # 규칙 1 — 같은 라이브러리가 실행 중이면 기다리지 않고 건너뜀으로
    # 끝낸다. 그 작업이 끝나면 라이브러리는 최신 상태라, 대기했다가
    # 이어서 도는 것은 중복에 가깝고, 기다리는 동안 다시 시작되면
    # 대기가 중단돼 헷갈리는 기록만 남는다. 기다리는 것은 규칙 2
    # (동시 스캔 수 제한)에 걸린 경우만이다. 판정은 이 시점에 다시
    # 확인한다 — 자리를 차지하려다 같은 라이브러리 작업에 진 경우도
    # 여기서 걸린다.
    if _same_library_busy(first_key, exclude_claim_id=claim_id):
        fail_trash_start(name, mode, LIBRARY_BUSY_MESSAGE,
                         section_label, action, status='skipped')
        logger.info(f'[휴지통 스캔] {name}: 같은 라이브러리 작업이 진행 중이라 건너뜁니다.')
        return (None, None, None)
    running_names = [(c.get('job_name') or '') for c in get_trash_currents()]
    running_names = [n for n in running_names if n]
    if running_names:
        shown = ', '.join(running_names[:2])
        extra = f' 외 {len(running_names) - 2}개' if len(running_names) > 2 else ''
        wait_msg = f'앞선 작업({shown}{extra}) 진행 중으로 끝나면 이어서 진행합니다.'
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
        return (None, None, None)
    row_id = item.id
    # 대기 중 표시 — 작업 관리·작업 결과 목록에 '대기중' 카드가
    # 나오게 진행 상태에도 넣어 둔다. 차지할 때 _try_claim이 같은
    # claim_id로 덮어써 실행 중 표시로 바뀐다. 대기 중 표시는
    # 자리를 예약하지 않는다 — 게이트 집계에서는 세지 않는다.
    _upsert_trash_current({
        'job_idx': idx, 'job_name': name,
        'section_key': str(first_key) if first_key is not None else '',
        'section_title': section_label, 'stage': 'waiting', 'mode': mode,
        'action': action,
        'job_started_at': job_started, 'section_started_at': _now_text(),
        'claim_id': claim_id, 'job_row_id': row_id})
    t0 = time.time()
    deadline = t0 + max_min * 60
    while time.time() < deadline:
        time.sleep(WAIT_POLL_SEC)
        # 대기 중 취소 — 카드의 취소 버튼이 먹히게 여기서도 확인한다.
        if _trash_cancel_requested(claim_id):
            _finish_job_row(row_id, 'cancelled', '취소 요청으로 대기를 중단했습니다.')
            _remove_trash_current(claim_id)
            _clear_trash_cancel(claim_id)
            logger.info(f'[휴지통 스캔] {name}: 대기 중 취소됐습니다.')
            return (None, None, None)
        free, _ = _slot_free(first_key, action, exclude_claim_id=claim_id)
        if free and _try_claim(idx, name, mode, job_started, row_id, claim_id,
                               section_label, action, first_key):
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
            return (row_id, waited, claim_id)
    _finish_job_row(row_id, 'failed',
                    f'앞선 작업이 끝나기를 기다리다 시간 초과됐습니다. (작업 대기 {_fmt_wait(max_min * 60)})')
    _remove_trash_current(claim_id)
    _clear_trash_cancel(claim_id)
    logger.warning(f'[휴지통 스캔] {name}: 작업 대기 시간 초과로 시작하지 않습니다.')
    return (None, None, None)


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


def fail_trash_start(job_name, mode, reason, section_label='전체', action=None,
                     status='failed'):
    """작업 단위 시작 실패 — 이력 행을 남기고 배너용 오류를 기록한다.
    이력 행 저장이 실패해도 배너 기록은 시도한다.
    section_label — 이력 행에 표시할 라이브러리명. 대상을 알면 그 이름,
    모르면 '전체'. action — 알면 이력 행에 함께 남긴다.
    status — 기본 'failed'. 자리가 없어 시작하지 못한 경우(게이트 차단)는
    'skipped'로 남기고 배너에는 띄우지 않는다 — 실패가 아니라 건너뛴
    것이기 때문이다."""
    try:
        from .model_trash import ModelTrashItem
        item = ModelTrashItem(job_name, '', section_label, mode, action)
        item.finish(status, reason)
    except Exception as e:
        logger.error(f'[휴지통 스캔] 시작 실패 이력 기록 실패: {e}')
    if status == 'failed':
        _record_trash_start_error(job_name, reason)


def _trash_cancel_ids():
    """취소 요청된 작업의 claim_id 목록. 구 형식 '1'(전부 취소)이면
    ['*']로, 비어 있거나 깨졌으면 빈 목록으로 돌려준다."""
    raw = P.ModelSetting.get(TRASH_CANCEL_KEY) or ''
    if raw == '1':
        return ['*']
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    if isinstance(data, list):
        return [str(x) for x in data if x is not None]
    return []


def _trash_cancel_requested(claim_id=None):
    """이 작업에 취소 요청이 들어왔으면 True. claim_id가 없으면
    구 형식 '1'(전부 취소)일 때만 True를 돌려준다."""
    ids = _trash_cancel_ids()
    if '*' in ids:
        return True
    if claim_id is None:
        return False
    return str(claim_id) in ids


def request_trash_cancel(claim_id):
    """이 작업의 취소를 요청한다 — 다른 작업의 취소 요청은 건드리지 않는다."""
    if claim_id is None:
        return
    ids = _trash_cancel_ids()
    if '*' in ids or str(claim_id) in ids:
        return
    ids.append(str(claim_id))
    P.ModelSetting.set(TRASH_CANCEL_KEY, json.dumps(ids, ensure_ascii=False))


def _clear_trash_cancel(claim_id):
    """끝난 작업의 취소 요청을 지운다 — 목록이 쌓이지 않게 한다."""
    if claim_id is None:
        return
    ids = [x for x in _trash_cancel_ids() if x != str(claim_id)]
    if '*' in ids:
        return
    P.ModelSetting.set(TRASH_CANCEL_KEY,
                       json.dumps(ids, ensure_ascii=False) if ids else '')


def _now_text():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _vfs_use_async():
    """VFS 비동기 사용 여부 — 설정이 없으면 기본값(켜짐)으로 본다."""
    try:
        v = P.ModelSetting.get(TRASH_VFS_ASYNC_KEY)
    except Exception:
        v = None
    if v is None:
        return True
    return str(v).lower() == 'true'


def _query_rc_job(server, jobid):
    """rclone job 1회 조회 (CLI) — 결과 dict / 'not_found' / None(조회 실패).
    조회 실패 원인은 로그에 남긴다 (0.9.9h)."""
    job = rc_job_status(server, jobid)
    if job is None:
        logger.warning(f'[휴지통] VFS 새로고침 확인 실패 (job {jobid})')
    return job


def _wait_vfs(server, jobid, timeout=None, claim_id=None):
    """rclone VFS 새로고침 job 완료를 기다린다 (CLI 조회).
    {'result': done|failed|cancelled|not_found, 'seen': 진행 중인 것을
     본 적이 있는지, 'duration': 초}를 돌려준다.
    기한은 설정(휴지통 VFS 새로고침 제한 시간, 기본 360분)에서 읽는다.
    timeout을 직접 주면 그 값(초)을 쓴다. 폴링 중 일시적 오류는 원인을
    로그에 남기고 계속 기다린다 (0.9.9h). 기한이 됐을 때 아직 진행 중이면
    기한을 연장하고, 기한 안에 한 번도 확인이 안 됐을 때만 실패로 확정한다.
    작업 기록이 사라지면(not_found) 즉시 돌려주되, 본 적이 있는지는
    'seen'으로 함께 알린다 — 호출자가 동기 새로고침으로 마무리할지
    정한다 (0.9.9l).
    claim_id — 취소 요청을 이 작업 것으로만 확인하기 위한 식별자다."""
    if timeout is None:
        timeout = _vfs_timeout_sec()
    t0 = time.time()
    deadline = time.time() + timeout
    last_ok = None
    while True:
        if time.time() >= deadline:
            # 기한 도달 — job을 한 번 더 조회한다. 끝났으면 그 결과를
            # 쓰고, 아직 돌고 있으면 기한을 연장한다. 확인이 안 되면
            # 기한 내내 확인이 안 됐을 때만 실패로 확정한다 (0.9.9h).
            job = _query_rc_job(server, jobid)
            if isinstance(job, dict):
                last_ok = time.time()
                if job.get('finished'):
                    return {'result': 'failed' if job.get('error') else 'done',
                            'seen': True, 'duration': int(time.time() - t0)}
                deadline = time.time() + timeout
                logger.info(
                    f"휴지통: VFS 새로고침이 아직 진행 중이라 기한을 "
                    f"{_fmt_wait(timeout)} 연장합니다 (job {jobid})")
                continue
            if job == 'not_found':
                return {'result': 'not_found', 'seen': last_ok is not None,
                        'duration': int(time.time() - t0)}
            if last_ok is None or time.time() - last_ok >= timeout:
                return {'result': 'failed', 'seen': last_ok is not None,
                        'duration': int(time.time() - t0)}
            deadline = time.time() + VFS_POLL_SEC * 6
        if _trash_cancel_requested(claim_id):
            return {'result': 'cancelled', 'seen': last_ok is not None,
                    'duration': int(time.time() - t0)}
        job = _query_rc_job(server, jobid)
        if job == 'not_found':
            return {'result': 'not_found', 'seen': last_ok is not None,
                    'duration': int(time.time() - t0)}
        if job is None:
            # 조회 실패 원인은 _query_rc_job이 로그에 남겼다.
            time.sleep(VFS_POLL_SEC)
            continue
        last_ok = time.time()
        if job.get('finished'):
            return {'result': 'failed' if job.get('error') else 'done',
                    'seen': True, 'duration': int(time.time() - t0)}
        time.sleep(VFS_POLL_SEC)


def _wait_vfs_sync(proc, timeout=None, claim_id=None):
    """동기 VFS 새로고침 프로세스가 끝날 때까지 기다린다.
    {'result': done|failed|cancelled, 'duration': 초}를 돌려준다.
    기한은 설정(휴지통 VFS 새로고침 제한 시간, 기본 360분)에서 읽는다.
    기한이 됐을 때 아직 진행 중이면 기한을 연장한다 (비동기와 같은 기준).
    취소 요청이 오면 프로세스를 멈추고 취소로 끝낸다."""
    if timeout is None:
        timeout = _vfs_timeout_sec()
    t0 = time.time()
    deadline = time.time() + timeout
    while True:
        if _trash_cancel_requested(claim_id):
            sync_pid_kill(proc.pid)
            return {'result': 'cancelled', 'duration': int(time.time() - t0)}
        rc = proc.poll()
        if rc is not None:
            return {'result': 'done' if rc == 0 else 'failed',
                    'duration': int(time.time() - t0)}
        if time.time() >= deadline:
            deadline = time.time() + timeout
            logger.info(
                f"휴지통: VFS 새로고침이 아직 진행 중이라 기한을 "
                f"{_fmt_wait(timeout)} 연장합니다 (동기)")
        time.sleep(VFS_POLL_SEC)


def _wait_job(url, token, job_id, timeout=None, poll_sec=SCAN_POLL_SEC,
              section_key=None, claim_id=None):
    """shyni job 완료를 기다린다 (스캔·비우기 공통).
    {'result': done|failed|cancelled|not_found, 'job': job 응답 또는 None,
     'duration': 초, 'cancel_message': 취소 시 이력 문구(있을 때만)}를
    돌려준다. timeout — None이면 설정 trash_scan_timeout_min에서 읽는다
    (기본 360분). 기한이 됐을 때 shyni에서 job이 아직 진행 중이면 기한을
    연장하며 계속 기다린다. job 조회에서 404를 받으면 즉시 실패로 끝낸다
    (W3). 폴링 중 일시적 오류는 원인을 로그에 남기고 계속 기다린다 —
    확인 실패만으로는 실패로 확정하지 않는다 (0.9.9h). 기한 안에 한 번도
    확인이 안 됐을 때만 실패로 확정한다.
    section_key가 있으면 60초마다 스캔 진행률을 현황에 기록한다 (스캔
    job만 — 비우기 job은 section_key를 넘기지 않는다). 스캔 job은 취소
    요청이 오면 shyni 스캔도 함께 취소하고 확인한다 (Step5-1). 비우기
    job은 취소할 수 없어 플래그가 있어도 끝까지 기다린다 — 플래그는
    남아 있어 비우기가 끝난 뒤 다음 라이브러리는 멈춘다.
    claim_id — 취소 요청을 이 작업 것으로만 확인하기 위한 식별자다."""
    t0 = time.time()
    if timeout is None:
        timeout = _scan_timeout_sec()
    deadline = time.time() + timeout
    last_ok = None
    poll_count = 0
    while True:
        if time.time() >= deadline:
            # 기한 도달 — job을 한 번 더 조회해 연장 여부를 정한다.
            # shyni에서 아직 진행 중이면 기한을 연장하고 계속
            # 기다린다. job이 끝났으면 그 결과를 그대로 쓴다.
            job = _fetch_compat_job(url, token, job_id)
            if isinstance(job, dict):
                last_ok = time.time()
                outcome, _msg = _scan_poll_outcome(200, job)
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
                deadline = time.time() + timeout
                label = ('휴지통 스캔' if section_key is not None
                         else '휴지통 비우기')
                logger.info(
                    f"{label}: shyni에서 아직 진행 중이라 기한을 "
                    f"{_fmt_wait(timeout)} 연장합니다 (job {job_id})")
                continue
            if job == 'not_found':
                return {'result': 'not_found', 'job': None,
                        'duration': int(time.time() - t0)}
            # 조회 실패 — 기한 안에 한 번이라도 확인이 됐으면 잠깐
            # 뒤에 다시 확인한다. 기한 내내 확인이 안 됐으면 그때
            # 실패로 확정한다 (0.9.9h).
            if last_ok is None or time.time() - last_ok >= timeout:
                return {'result': 'failed', 'job': None,
                        'duration': int(time.time() - t0)}
            deadline = time.time() + poll_sec * 3
        if section_key is not None and _trash_cancel_requested(claim_id):
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
        except Exception as e:
            logger.warning(f'[휴지통] 스캔 확인 실패 (job {job_id}): {type(e).__name__}: {e}')
            time.sleep(poll_sec)
            continue
        job = None
        if res.status_code == 200:
            try:
                job = res.json()
            except Exception as e:
                logger.warning(f'[휴지통] 스캔 확인 응답 파싱 실패 (job {job_id}): {type(e).__name__}: {e}')
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
            logger.warning(f'[휴지통] 스캔 확인 실패 (job {job_id}): HTTP {res.status_code}')
        elif isinstance(job, dict):
            last_ok = time.time()
        # 진행률 기록 — 60초마다 (폴링 간격 × 횟수, SCAN_POLL_SEC 20초 기준 3회).
        poll_count += 1
        if section_key and poll_count % max(1, int(60 / max(poll_sec, 1))) == 0:
            _record_scan_progress(section_key)
        time.sleep(poll_sec)


def _wait_scan(url, token, job_id, timeout=None, section_key=None,
               claim_id=None):
    """shyni 스캔 job 완료를 기다린다 — _wait_job과 같은 결과를 돌려준다.
    timeout — None이면 설정 trash_scan_timeout_min에서 읽는다(기본 360분)."""
    return _wait_job(url, token, job_id, timeout=timeout, poll_sec=SCAN_POLL_SEC,
                     section_key=section_key, claim_id=claim_id)


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


def _refresh_folder_vfs(key, folder, name, claim_id=None):
    """폴더 하나 VFS 새로고침.
    {'result': done|failed|cancelled, 'duration': 초, 'error': 사유}를
    돌려준다. 전송은 rclone CLI로 한다 (0.9.9l). 비동기(기본값)는
    작업을 걸어 두고 job/status로 확인하면서 기다리고, 동기는
    새로고침 프로세스가 끝날 때까지 기다린다. 비동기에서 작업
    기록이 사라진 경우 — 진행 중인 것을 본 적이 있으면 동기로
    한 번 더 걸어 끝내고, 본 적이 없으면 실패로 끝낸다.
    claim_id — 취소 요청을 이 작업 것으로만 확인하기 위한 식별자다."""
    from .mod_status import ModuleStatus
    addr, remote = ModuleStatus._rc_target(folder)
    server = rc_server_by_addr(addr) if addr else None
    if server is None:
        return {'result': 'failed', 'duration': 0,
                'error': 'VFS 새로고침 규칙과 맞지 않는 경로입니다.'}
    started = _now_text()
    _write_status(key, 'vfs', {
        'job': None, 'pid': None, 'mode': None, 'path': folder,
        'started_at': started, 'finished_at': None,
        'status': 'running', 'source': 'trash',
        'job_name': name}, label='휴지통 스캔')
    t0 = time.time()
    if not _vfs_use_async():
        proc = vfs_refresh_sync_spawn(server, remote)
        if proc is None:
            _write_status(key, 'vfs', {
                'job': None, 'pid': None, 'mode': 'sync', 'path': folder,
                'started_at': started, 'finished_at': _now_text(),
                'status': 'failed', 'source': 'trash',
                'job_name': name}, label='휴지통 스캔')
            return {'result': 'failed', 'duration': int(time.time() - t0),
                    'error': 'VFS 새로고침을 시작하지 못했습니다.'}
        _write_status(key, 'vfs', {
            'job': None, 'pid': proc.pid, 'mode': 'sync', 'path': folder,
            'started_at': started, 'finished_at': None,
            'status': 'running', 'source': 'trash',
            'job_name': name}, label='휴지통 스캔')
        w = _wait_vfs_sync(proc, claim_id=claim_id)
        _write_status(key, 'vfs', {
            'job': None, 'pid': proc.pid, 'mode': 'sync', 'path': folder,
            'started_at': started, 'finished_at': _now_text(),
            'status': {'done': 'done', 'failed': 'failed',
                       'cancelled': 'cancelled'}.get(w['result'], 'failed'),
            'source': 'trash', 'job_name': name}, label='휴지통 스캔')
        if w['result'] == 'done':
            return {'result': 'done', 'duration': w['duration'], 'error': '',
                    'pid': proc.pid}
        if w['result'] == 'cancelled':
            return {'result': 'cancelled', 'duration': w['duration'],
                    'error': '', 'pid': proc.pid}
        return {'result': 'failed', 'duration': w['duration'],
                'error': 'VFS 새로고침이 끝나지 않았습니다.',
                'pid': proc.pid}
    jobid = vfs_refresh_async_start(server, remote)
    if jobid is None:
        _write_status(key, 'vfs', {
            'job': None, 'pid': None, 'mode': 'async', 'path': folder,
            'started_at': started, 'finished_at': _now_text(),
            'status': 'failed', 'source': 'trash',
            'job_name': name}, label='휴지통 스캔')
        return {'result': 'failed', 'duration': int(time.time() - t0),
                'error': 'VFS 새로고침을 시작하지 못했습니다.'}
    _write_status(key, 'vfs', {
        'job': jobid, 'pid': None, 'mode': 'async', 'path': folder,
        'started_at': started, 'finished_at': None,
        'status': 'running', 'source': 'trash',
        'job_name': name}, label='휴지통 스캔')
    w = _wait_vfs(server, jobid, claim_id=claim_id)
    if w['result'] == 'not_found' and w.get('seen'):
        # 진행 중인 것을 본 적이 있다 — 끝난 뒤 기록이 만료된 경우라
        # 동기로 한 번 더 걸어 끝낸다 (0.9.9l).
        logger.warning(
            f'휴지통: VFS 새로고침 작업 기록이 사라져 동기 새로고침으로 '
            f'마무리합니다 (job {jobid})')
        proc = vfs_refresh_sync_spawn(server, remote)
        if proc is not None:
            sw = _wait_vfs_sync(proc, claim_id=claim_id)
            w = {'result': sw['result'], 'duration': int(time.time() - t0),
                 'pid': proc.pid}
        else:
            w = {'result': 'failed', 'duration': int(time.time() - t0)}
    _write_status(key, 'vfs', {
        'job': jobid, 'pid': None, 'mode': 'async', 'path': folder,
        'started_at': started, 'finished_at': _now_text(),
        'status': {'done': 'done', 'failed': 'failed',
                   'cancelled': 'cancelled'}.get(w['result'], 'failed'),
        'source': 'trash', 'job_name': name}, label='휴지통 스캔')
    if w['result'] == 'done':
        return {'result': 'done', 'duration': w['duration'], 'error': '',
                'jobid': jobid, 'pid': w.get('pid')}
    if w['result'] == 'cancelled':
        return {'result': 'cancelled', 'duration': w['duration'], 'error': '',
                'jobid': jobid, 'pid': w.get('pid')}
    if w['result'] == 'not_found':
        return {'result': 'failed', 'duration': w['duration'],
                'error': 'VFS 새로고침 작업 기록을 찾을 수 없습니다.',
                'jobid': jobid, 'pid': w.get('pid')}
    return {'result': 'failed', 'duration': w['duration'],
            'error': 'VFS 새로고침이 끝나지 않았습니다.',
            'jobid': jobid, 'pid': w.get('pid')}

def _compute_skip_reason(key, claim_id=None):
    """이 섹션을 건너뛸 사유 — None이면 진행한다.
    라이브러리 동시 스캔 수 게이트와 같은 목록(대시보드·주기적·
    휴지통 스캔)으로 판정한다. 스캔 큐(폴더 단위)는 세지 않는다.
    자기 작업의 진행 상태만 빼고 센다. 목록을 읽지 못하면
    게이트와 마찬가지로 막지 않고 진행한다.
    섹션 시작 시마다 다시 읽는다."""
    try:
        running = ModuleStatus.running_library_scans(exclude_claim_id=claim_id)
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
    목록으로 기록해 화면이 공유한다. 여러 작업이 설정한 '라이브러리
    동시 스캔 수'까지 함께 돌 수 있다. 취소는 trash_cancel에 claim_id
    목록으로 요청받는다.
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
                # _run이 차지한 뒤의 오류면 자기 진행 상태만 지운다 —
                # 다른 작업의 진행 상태는 건드리지 않는다. 차지하기
                # 전이면 _ACTIVE가 비어 있어 아무것도 지우지 않는다.
                logger.error(f'휴지통 주기 작업 실행 실패: {e}')
                logger.error(traceback.format_exc())
                claim_id = getattr(_ACTIVE, 'claim_id', None)
                row_id = getattr(_ACTIVE, 'job_row_id', None)
                _ACTIVE.claim_id = None
                _ACTIVE.job_row_id = None
                if claim_id is not None:
                    try:
                        _remove_trash_current(claim_id)
                        _clear_trash_cancel(claim_id)
                    except Exception:
                        pass
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
        _ACTIVE.claim_id = None
        _ACTIVE.job_row_id = None
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
        # 자리 검사는 첫 라이브러리 기준으로 한다 — 나중 라이브러리는
        # 섹션 시작 때 가드가 다시 본다.
        job_row_id, _waited, claim_id = _wait_for_turn(
            idx, name, mode, job_started, job_label, action,
            first_key=targets[0]['key'])
        if _waited is None:
            return
        _ACTIVE.claim_id = claim_id
        _ACTIVE.job_row_id = job_row_id

        for sec in targets:
            key = str(sec['key'])
            title = sec.get('title') or key
            if _trash_cancel_requested(claim_id):
                item = ModelTrashItem(name, key, title, mode, action)
                item.finish('cancelled', '취소 요청으로 실행하지 않았습니다.')
                continue
            # 건너뜀 가드는 스캔이 들어간 동작에만 적용한다 — 새로고침만·
            # 비우기만은 스캔과 충돌하지 않아 가드에서 뺀다. 가드는 섹션
            # 시작 시마다 다시 읽는다 (W5).
            guard_applies = 'scan' in steps
            skip_reason = _compute_skip_reason(key, claim_id) if guard_applies else None
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
            retained_count = None
            emptied_entries = None
            retained_entries = None
            pre_entries = None
            # 휴지통 항목 대상 수행 — 실행 전에 목록을 읽어 둔다.
            # target_files가 None이면 목록을 모른다는 뜻이다.
            # target_entries는 상태·경과를 함께 가진 목록(이력 표시용).
            target_files = None
            target_entries = None
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
                    target_entries = _trash_file_entries(key)
                    if target_entries is None:
                        list_error = 'read_failed'
                    else:
                        target_files = [e['p'] for e in target_entries]
                except Exception as e:
                    logger.warning(f'[휴지통 스캔] 휴지통 목록 읽기 실패: {e}')
                    list_error = 'read_failed'
                    target_files = None
                    target_entries = None
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
                    if _trash_cancel_requested(claim_id):
                        status, message = 'cancelled', '취소 요청으로 중단했습니다.'
                        break
                    if step == 'empty' and (vfs_failed_folders or scan_failed_folders):
                        # 실패한 폴더가 있으면 비우지 않는다 — 재확인
                        # 못한 항목을 비우면 안 된다.
                        empty_skipped = True
                        logger.info(f'[휴지통 스캔] 비우기 생략: 실패한 폴더가 있음 '
                                    f'(VFS {vfs_failed_folders}개 · 스캔 {scan_failed_folders}개)')
                        continue
                    _upsert_trash_current({
                        'job_idx': idx, 'job_name': name, 'section_key': key,
                        'section_title': title,
                        'stage': step, 'job_started_at': job_started,
                        'section_started_at': _now_text(),
                        'mode': mode, 'action': action, 'job_row_id': job_row_id,
                        'claim_id': claim_id})
                    try:
                        if step == 'refresh':
                            # 폴더별 VFS 새로고침 — 하나가 실패해도
                            # 나머지는 계속한다. 전부 실패하면 실패로 끝낸다.
                            vfs_results = {}
                            file_exists = {}
                            vfs_total = 0
                            for grp in target_groups:
                                if _trash_cancel_requested(claim_id):
                                    status, message = 'cancelled', '취소 요청으로 중단했습니다.'
                                    break
                                folder = grp['folder']
                                w = _refresh_folder_vfs(key, folder, name, claim_id)
                                vfs_total += w['duration']
                                if w['result'] == 'cancelled':
                                    status, message = 'cancelled', '취소 요청으로 중단했습니다.'
                                    break
                                ok = w['result'] == 'done'
                                vfs_results[folder] = {'ok': ok,
                                                       'error': w.get('error') or '',
                                                       'job': w.get('jobid'),
                                                       'pid': w.get('pid')}
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
                            # 스캔 중이거나 상한까지 돌고 있으면 건너뜀으로
                            # 끝낸다 (기다리지 않는다). 실패가 아니다 —
                            # 실행하지 못한 것이다. 폴더마다 검사하지 않고
                            # 섹션 단위로 한 번만 검사한다. 스캔할 폴더가
                            # 없으면 검사할 필요가 없다.
                            if scan_folders:
                                gate_msg = ModuleStatus.library_gate_check(
                                    key, exclude_claim_id=claim_id)
                                if gate_msg:
                                    status, message = 'skipped', gate_msg
                                    break
                            scan_results = {}
                            scan_total = 0
                            first_scan_error = ''
                            for seq, grp in enumerate(scan_folders):
                                if _trash_cancel_requested(claim_id):
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
                                cur = None
                                for c in get_trash_currents():
                                    if c.get('claim_id') == claim_id:
                                        cur = c
                                        break
                                if cur is not None:
                                    cur['scan_job'] = job_id
                                    _upsert_trash_current(cur)
                                w = _wait_scan(url, token, job_id, section_key=key,
                                               claim_id=claim_id)
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
                                pre_entries = _trash_file_entries(key)
                                if pre_entries is None:
                                    logger.info('[휴지통 스캔] 휴지통 목록을 읽지 못해 비운 파일 목록을 저장하지 않습니다.')
                            except Exception as e:
                                logger.warning(f'[휴지통 스캔] 비우기 전 휴지통 목록 읽기 실패: {e}')
                                pre_entries = None
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
                                retained_count = _stat_int(estat.get('retained_parts'))
                                # 실제로 비운 파일은 비우기 전/후 목록 비교로
                                # 가린다 — 비우기 전 목록을 그대로 '비운 파일'로
                                # 두면 보관 기간이 남아 남은 파일까지 비운 것으로
                                # 보인다 (2026-11-20 '방송중' 건: 비움 0개인데
                                # 목록 2개). 비운 뒤 목록이 곧 남은 파일이다.
                                try:
                                    post_entries = _trash_file_entries(key)
                                except Exception as e:
                                    logger.warning(f'[휴지통 스캔] 비우기 후 휴지통 목록 읽기 실패: {e}')
                                    post_entries = None
                                if pre_entries is not None and post_entries is not None:
                                    post_paths = {e['p'] for e in post_entries}
                                    emptied_entries = [e for e in pre_entries
                                                       if e['p'] not in post_paths]
                                    retained_entries = post_entries
                                    if emptied_count is None:
                                        emptied_count = len(emptied_entries)
                                    if retained_count is None:
                                        retained_count = len(retained_entries)
                                elif pre_entries is not None:
                                    # 비운 뒤 목록을 못 읽으면 비우기 전 목록을
                                    # 그대로 둔다 (옛 동작).
                                    emptied_entries = pre_entries
                                    retained_entries = None
                                else:
                                    emptied_entries = None
                                    retained_entries = None
                            trash_entry = {
                                'job': empty_job_id, 'mode': action,
                                'started_at': empty_started, 'finished_at': _now_text(),
                                'status': 'done', 'source': 'trash',
                                'job_name': name}
                            if emptied_count is not None:
                                summary = f'{emptied_count}개 비움'
                                if retained_count:
                                    summary += f' · 보관 기간 남음 {retained_count}개'
                                trash_entry['summary'] = summary
                            _write_status(key, 'trash', trash_entry, label='휴지통 스캔')
                            # 비우기에 성공했으니 휴지통은 보관 기간이 남아
                            # 남은 수만큼 — 개수 캐시에 바로 반영한다
                            # (남은 게 없으면 0개, 수를 모르면 옛 동작대로 0개).
                            try:
                                from .task_status import set_trash_count
                                set_trash_count(key, retained_count
                                                if retained_count is not None else 0)
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
            item.retained_count = retained_count
            if emptied_entries is not None:
                item.emptied_files = json.dumps(emptied_entries[:TRASH_FILES_MAX],
                                                ensure_ascii=False)
            if retained_entries is not None:
                item.retained_files = json.dumps(retained_entries[:TRASH_FILES_MAX],
                                                 ensure_ascii=False)
            if target_entries is not None:
                item.target_files = json.dumps(target_entries[:TRASH_FILES_MAX],
                                               ensure_ascii=False)
            elif target_files is not None:
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
                    len(target_groups), eff_remaining, retained_count)
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
            if _trash_cancel_requested(claim_id):
                _finish_job_row(job_row_id, 'cancelled', '취소 요청으로 중단했습니다.')
            else:
                _finish_job_row(job_row_id, 'done', '완료됐습니다.')
        _remove_trash_current(claim_id)
        _clear_trash_cancel(claim_id)
        _ACTIVE.claim_id = None
        _ACTIVE.job_row_id = None
        logger.info(f'[휴지통 스캔] 종료: {name}')
