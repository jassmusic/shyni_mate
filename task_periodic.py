import json
import os
import random
import threading
import time

import requests

from .model_periodic import ModelPeriodicItem
from .mod_status import ModuleStatus
from .setup import *
from .shyni_backend import ShyniBackend

name = 'periodic'
logger = P.logger

CURRENT_KEY = 'periodic_current'
CANCEL_KEY = 'periodic_cancel'
JOBS_KEY = 'periodic_jobs'
MAX_WAIT_KEY = 'periodic_max_wait_min'
# 대기 확인 간격(초) — 앞선 작업이 끝났는지 확인하는 주기.
WAIT_POLL_SEC = 10
# 기본 작업 대기 시간(분) — 설정이 없거나 읽지 못하면 이 값을 쓴다.
DEFAULT_MAX_WAIT_MIN = 360
# VFS 새로고침 제한 시간 설정 키(분) — 라이브러리 새로고침이 끝나기를
# 이 시간까지 기다린다. 설정이 없거나 0 이하면 기본값을 쓴다.
VFS_TIMEOUT_KEY = 'periodic_vfs_timeout_min'
DEFAULT_VFS_TIMEOUT_MIN = 360
# 스캔 대기 기한 설정 키(분) — 스캔이 끝나기를 이 시간까지 기다린다.
# 기한이 됐을 때 shyni에서 스캔이 아직 진행 중이면 이 시간만큼 기한을
# 연장하며 계속 기다린다. 설정이 없거나 0 이하면 기본값을 쓴다.
SCAN_TIMEOUT_KEY = 'periodic_scan_timeout_min'
DEFAULT_SCAN_TIMEOUT_MIN = 360
# 기동 식별자 — FF 메인 프로세스의 기동 시각(epoch). 웹·워커 두
# 프로세스가 플러그인을 따로 로드하는 틈에 방금 시작한 실행을 중단된
# 작업으로 오인해 복구 항목을 만들면, 새 실행이 게이트에서 자기 복구
# 항목에 걸려 실패하고 시스템이 멈춘다 (2026-11-25 실측). 이력·항목의
# 시작 시각이 기동보다 이전이면 끊긴 것(복구), 이후면 살아 있는
# 것(건드리지 않음)으로 판정한다. 나이 기준(10분)은 폐기 — 재시작을
# 거의 안 하면 10분 안에 끊긴 이력이 영원히 '실행 중'으로 남는다.
# 기동 시각과 비교할 때 시계 보정 오차(수 초)를 흡수하는 여유분.
RUN_START_MARGIN_SEC = 5
# 기동 식별자 비교 허용 오차(초) — 두 프로세스가 같은 메인 프로세스에서
# 독립 계산해도 값은 같지만, 저장·복원 과정의 반올림을 흡수한다.
RUN_ID_TOLERANCE_SEC = 1.0
# 기동 식별자를 못 읽을 때(/proc 없음) 폴백 — 모듈 로드 시각을 기동
# 시각 대신 쓰고, 이 시간 창 안에 있으면 같은 기동으로 본다.
RUN_ID_FALLBACK_WINDOW_SEC = 180
# 복구 실행 기록 — resume_running이 돌았던 기동의 식별자를 남긴다.
# 같은 기동에서 다시 로드되면 복구를 건너뛴다.
RESUME_RUN_KEY = 'periodic_resume_run_id'
# 모듈 로드 시각 — _ff_run_id()가 /proc를 못 읽을 때 폴백 기동 식별자.
_PLUGIN_LOAD_TIME = time.time()
# 가장 최근 시작 실패 — 작업 관리 배너가 읽는다. '지금 실행'을 눌렀는데
# 아무것도 시작되지 않은 경우(진행 중·목록 조회 실패 등)는 이력 행이
# 생기기 전에 끝나므로, 로그에만 남던 사유를 여기에 남긴다.
START_ERROR_KEY = 'periodic_last_start_error'
# 시작 실패 기록 보관 기한(초) — 이보다 오래된 오류는 배너에 띄우지 않는다.
START_ERROR_MAX_AGE_SEC = 3600

STAGE_LABEL = {'vfs': 'VFS 새로고침', 'scan': '스캔'}

# 폴링 실패 처리 (0.9.9h, W7 개정): 폴링 중 일시적 오류(연결 끊김 등)는
# 원인을 로그에 남기고 계속 기다린다 — 확인 실패만으로는 실패로 확정하지
# 않는다. 실패 확정은 shyni/rclone이 직접 실패라고 답하거나, 작업 기록이
# 없을 때(404/not found), 또는 기한 안에 한 번도 확인이 안 됐을 때만 한다.
# (스캔 대기 기한은 설정 periodic_scan_timeout_min에서 읽고,
# VFS 새로고침 기한은 설정 periodic_vfs_timeout_min에서 읽는다.)
VFS_POLL_SEC = 10
SCAN_POLL_SEC = 20
# 폴더별 결과(job stats의 results[]) 저장 상한 — 폴더가 많은 라이브러리에서도
# 이력 한 행이 지나치게 커지지 않게 한다.
STAT_FOLDERS_MAX = 500

# 동작 드롭다운 값 ↔ 저장 형식(kind + use_vfs) 매핑 (권장안 A — 화면 매핑만
# 바꾸고 저장 형식은 그대로 둔다. 마이그레이션 없음).
ACTION_LABEL = {'refresh': '새로고침', 'scan': '스캔',
                'refresh_scan': '새로고침 후 스캔'}
ACTION_TO_KIND = {
    'refresh': ('vfs', False),
    'scan': ('scan', False),
    'refresh_scan': ('scan', True),
}


def periodic_action_to_kind_use_vfs(action):
    """드롭다운 값 → (kind, use_vfs). 모르는 값이면 기본 '새로고침 후 스캔'."""
    return ACTION_TO_KIND.get(action or '', ('scan', True))


def periodic_kind_use_vfs_to_action(kind, use_vfs):
    """저장된 (kind, use_vfs) → 드롭다운 값."""
    if kind == 'vfs':
        return 'refresh'
    return 'refresh_scan' if use_vfs else 'scan'


def _stat_int(v):
    """job stats 값 → int 또는 None. 순수 함수(하니스 검증용)."""
    if isinstance(v, bool):
        return None
    return int(v) if isinstance(v, (int, float)) else None


def _stat_first(stats, *keys):
    """여러 stats 키 중 첫 번째로 값이 있는 것을 int로 돌려준다.

    도서 스캐너(book_scanner.scan_book_section)는 total/missing 대신
    entries/missing_files 키를 쓰므로, 음악 키가 없으면 도서 키로
    대체한다. 0도 유효값이라 `is not None`으로 분기한다.
    순수 함수(하니스 검증용)."""
    for k in keys:
        v = _stat_int(stats.get(k))
        if v is not None:
            return v
    return None


def _is_book_stats(stats):
    """도서 스캐너 stats인지 여부 — total이 없고 entries가 있다.
    도서 스캐너는 total/new/missing 대신 entries/missing_files
    키를 쓴다. 순수 함수(하니스 검증용)."""
    if not isinstance(stats, dict):
        return False
    return (_stat_int(stats.get('total')) is None
            and _stat_int(stats.get('entries')) is not None)


def book_relay_total(stats, section_key):
    """도서 스캔이면 Relay DB에서 실측 파일 수를 읽는다.

    도서 stats의 entries는 라이브러리 총수가 아니라 이번 스캔
    처리 수(델타)라, '전체'에는 Relay 실측 파일 수를 쓴다.
    도서가 아니거나 Relay가 꺼져 있거나 조회에 실패하면 None을
    돌려준다 (그 경우 entries를 '처리' 라벨로 표시한다)."""
    if not _is_book_stats(stats):
        return None
    try:
        from . import db_support
        if not db_support.db_ready():
            return None
        return db_support.book_file_count(int(section_key))
    except Exception as e:
        logger.warning(f'[스캔 결과] 도서 실측 파일 수 조회 실패: {e}')
        return None


def annotate_total_labels(rows):
    """이력 행 dict 목록에 'total_label'을 붙인다.

    도서 라이브러리인데 Relay가 꺼져 있으면 stat_total이 entries
    델타라 '처리', 그 외는 '전체'다. 종류는 섹션 목록 캐시에서
    읽는다 — 이력 조회 때 shyni를 새로 물으면 느려질 수 있어
    캐시(_sections_cache_read)만 보고, 캐시가 없으면 '전체'로 둔다.
    Relay 상태는 지금 시점 값이다 — 스캔 당시와 다르면 라벨이
    어긋날 수 있지만, 출처를 구분하는 컬럼은 두지 않기로 했다
    (마이그레이션 없음 원칙)."""
    if not rows:
        return rows
    try:
        cached, _ts = ModuleStatus._sections_cache_read()
        types = {str(s.get('key')): (s.get('type') or '')
                 for s in (cached or [])}
    except Exception:
        types = {}
    try:
        from . import db_support
        relay_on = db_support.db_ready()
    except Exception:
        relay_on = False
    for r in rows:
        is_book = types.get(str(r.get('section_key'))) == 'book'
        r['total_label'] = '처리' if (is_book and not relay_on) else '전체'
    return rows


def _extract_job_id(res):
    """스캔·비우기 전송 응답에서 shyni job id를 구한다 — 응답 헤더
    X-Job-Id가 없으면 본문의 job_id/jobId/jobID를 본다."""
    job_id = res.headers.get('X-Job-Id')
    if job_id:
        return job_id
    try:
        body = res.json()
        if isinstance(body, dict):
            return body.get('job_id') or body.get('jobId') or body.get('jobID')
    except Exception:
        pass
    return None


def _scan_poll_outcome(status_code, job):
    """job 조회 1회 결과 판정 — 순수 함수(하니스 검증용).
    ('done'|'failed'|'cancelled'|'not_found'|'wait', 메시지)를 돌려준다.
    404는 기록이 사라진 job이라 기다려 봤자 소용이 없어 즉시 실패다 (W3).
    'cancelled'는 shyni에서 취소된 job이다 (Step5-1 관리자 API 취소·
    shyni 웹 취소 둘 다 이 상태로 끝난다)."""
    if status_code == 404:
        return 'not_found', '스캔 작업 기록을 찾을 수 없습니다.'
    if status_code != 200:
        return 'wait', ''
    status = (job or {}).get('status')
    if status == 'completed':
        return 'done', '스캔이 완료됐습니다.'
    if status == 'failed':
        return 'failed', f"스캔 실패: {(job or {}).get('error') or ''}"
    if status == 'cancelled':
        return 'cancelled', '스캔이 취소됐습니다.'
    return 'wait', ''


# 스캔 취소 확인 기한(초) — DELETE 뒤 compat job이 'cancelled'가 될
# 때까지 기다리는 시간. 분석 중 파일은 끝까지 기다리므로 수 초~수십 초
# 걸릴 수 있다 (기준 문서 4절).
CANCEL_CONFIRM_TIMEOUT_SEC = 120
CANCEL_CONFIRM_POLL_SEC = 5

# 취소 결과별 이력 메시지 (Step5-1). shyni 스캔까지 취소했는지,
# 못 했으면 왜 못 했는지를 알려 준다.
CANCEL_RESULT_MESSAGE = {
    'confirmed': '취소했습니다. shyni 스캔도 함께 취소됐습니다.',
    'already_cancelled': '스캔이 이미 취소됐습니다.',
    'already_done': '스캔이 이미 완료됐습니다.',
    'already_failed': '스캔이 이미 실패로 끝났습니다.',
    'no_admin': '취소 요청으로 대기를 중단했습니다. 관리자 API가 설정돼 있지 않아 shyni 스캔은 취소하지 못했습니다. shyni 웹 화면에서 취소해 주세요.',
    'api_failed': '취소 요청으로 대기를 중단했습니다. shyni 스캔 취소 요청에 실패했으니, shyni 웹 화면에서 취소됐는지 확인해 주세요.',
    'finished_first': '취소 요청 뒤 스캔이 먼저 끝났습니다.',
    'job_gone': '취소 요청으로 대기를 중단했습니다. 스캔 작업 기록을 찾을 수 없습니다.',
    'timeout': '취소 요청으로 대기를 중단했습니다. shyni 스캔 취소가 기한 안에 확인되지 않았습니다. shyni 웹 화면에서 취소됐는지 확인해 주세요.',
}


def _fetch_compat_job(url, token, job_id):
    """compat job 1회 조회. job dict / 'not_found' / None(조회 실패).
    조회 실패 원인은 로그에 남긴다 (0.9.9h) — 다음 실패 때 원인을
    바로 알 수 있게 한다."""
    if not job_id:
        return None
    try:
        res = requests.get(f'{url}/compat/jobs/{job_id}',
                           headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                           timeout=15)
    except Exception as e:
        logger.warning(f'[shyni] compat job 조회 실패 (job {job_id}): {type(e).__name__}: {e}')
        return None
    if res.status_code == 404:
        return 'not_found'
    if res.status_code != 200:
        logger.warning(f'[shyni] compat job 조회 실패 (job {job_id}): HTTP {res.status_code}')
        return None
    try:
        return res.json()
    except Exception as e:
        logger.warning(f'[shyni] compat job 응답 파싱 실패 (job {job_id}): {type(e).__name__}: {e}')
        return None


def _cancel_shyni_scan(url, token, job_id, section_key):
    """shyni 스캔 취소 요청 + compat job 'cancelled' 확인 (Step5-1).

    DELETE /libraries/{id}/scan(관리자 API)으로 취소하고, compat job이
    'cancelled'가 될 때까지 폴링한다. job이 이미 끝났으면 그 결과를
    돌려주고, 관리자 API를 쓸 수 없으면 취소하지 못했음을 돌려준다.
    휴지통 스캔(task_trash)도 같은 헬퍼를 쓴다.
    {'cancelled': True|False, 'reason': 사유 코드, 'job': 마지막 job dict
    또는 None}을 돌려준다. reason 코드는 CANCEL_RESULT_MESSAGE의 키다.
    """
    from . import shyni_admin
    if not section_key or not shyni_admin.admin_available():
        return {'cancelled': False, 'reason': 'no_admin', 'job': None}
    # job이 이미 끝났는지 먼저 본다 — 끝난 스캔에 DELETE를 보내면
    # 플래그만 남고 다음 스캔 시작 때 지워져 소실된다 (기준 문서 4절).
    job = _fetch_compat_job(url, token, job_id)
    if isinstance(job, dict):
        st = job.get('status')
        if st == 'cancelled':
            return {'cancelled': True, 'reason': 'already_cancelled', 'job': job}
        if st == 'completed':
            return {'cancelled': False, 'reason': 'already_done', 'job': job}
        if st == 'failed':
            return {'cancelled': False, 'reason': 'already_failed', 'job': job}
    res = shyni_admin.api_cancel_scan(section_key)
    if res is None:
        return {'cancelled': False, 'reason': 'api_failed', 'job': None}
    if res.status_code != 404 and res.status_code >= 300:
        logger.warning(f'[취소] shyni 스캔 취소 요청 실패: 응답 코드 {res.status_code}')
        return {'cancelled': False, 'reason': 'api_failed', 'job': None}
    # 404는 돌고 있는 스캔이 없다는 뜻 — job도 곧 끝날 것이니 확인만 한다.
    deadline = time.time() + CANCEL_CONFIRM_TIMEOUT_SEC
    while time.time() < deadline:
        job = _fetch_compat_job(url, token, job_id)
        if isinstance(job, dict):
            st = job.get('status')
            if st == 'cancelled':
                return {'cancelled': True, 'reason': 'confirmed', 'job': job}
            if st in ('completed', 'failed'):
                return {'cancelled': False, 'reason': 'finished_first', 'job': job}
        elif job == 'not_found':
            return {'cancelled': False, 'reason': 'job_gone', 'job': None}
        time.sleep(CANCEL_CONFIRM_POLL_SEC)
    return {'cancelled': False, 'reason': 'timeout', 'job': None}


def _periodic_skip_decision(trash_parse_ok, trash_section_key, status_busy, key):
    """섹션 시작 시 건너뜀 판정 — 순수 함수(하니스 검증용).
    휴지통 스캔 상태를 확인할 수 없으면 fail-closed로 건너뛴다 (W5).
    (건너뜀 안내 문장 | None)을 돌려준다."""
    if not trash_parse_ok:
        return '휴지통 스캔 상태를 확인할 수 없어 건너뛰었습니다.'
    if trash_section_key is not None and str(trash_section_key) == str(key):
        return '휴지통 스캔이 실행 중이라 건너뛰었습니다.'
    if status_busy:
        return '현황에서 작업 중이라 건너뛰었습니다.'
    return None


def _write_status(key, slot, entry, label='주기적 스캔'):
    """대시보드 섹션 상태(status_sec_N)의 한 슬롯만 갱신한다.
    수동 작업과 같은 모양에 source·job_name·summary를 덧붙인다.
    실패하면 warning만 남기고 작업은 계속한다."""
    try:
        st = ModuleStatus._get_state(int(key))
        st[slot] = entry
        ModuleStatus._set_state(int(key), st)
    except Exception as e:
        logger.warning(f'[{label}] 현황 상태 기록 실패: {e}')


def _record_scan_progress(key):
    """스캔 진행률을 현황 'scan' 슬롯에 기록한다.
    진행 표시용으로만 쓰고, 완료 판정은 job 폴링이 계속 맡는다.
    기록에 실패하면 경고로 남긴다 (2026-11-19 실측: 어느 스캔에서도
    scan_progress가 쌓이지 않아, 조용히 넘기던 실패를 로그로 확인한다)."""
    try:
        from .shyni_admin import api_scan_status
        st = api_scan_status(key)
        if not st:
            logger.warning(f'[Periodic] 스캔 진행률 조회 실패 (섹션 {key}): 관리자 API 응답 없음')
            return
        cur = ModuleStatus._get_state(int(key))
        entry = dict(cur.get('scan') or {})
        if entry.get('status') != 'running':
            return
        entry['scan_progress'] = {
            'processed': st.get('processed'),
            'total': st.get('total'),
            'stage': st.get('stage'),
            'stage_processed': st.get('stage_processed'),
            'stage_total': st.get('stage_total'),
            'current_file': st.get('current_file'),
            'status': st.get('status'),
        }
        cur['scan'] = entry
        ModuleStatus._set_state(int(key), cur)
    except Exception as e:
        logger.warning(f'[Periodic] 스캔 진행률 기록 실패 (섹션 {key}): {type(e).__name__}: {e}')


def current_scan_progress(section_key, source):
    """진행 중 배너용 — 현황 'scan' 슬롯에 기록된 진행률을 돌려준다.
    실행 중인 표시가 아니면 None을 돌려준다 (끝난 스캔의 낡은 수치가
    새 작업의 배너에 안 보이게 한다). source는 'periodic'·'trash'."""
    try:
        st = ModuleStatus._get_state(int(section_key))
        entry = st.get('scan') or {}
        if entry.get('status') == 'running' and entry.get('source') == source:
            sp = entry.get('scan_progress')
            if isinstance(sp, dict):
                return sp
    except Exception:
        pass
    return None


def _now_text():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def apply_scan_result(item, job_id, stats, vfs_duration, scan_duration,
                      pre_max_id, section_key, label='주기적 스캔',
                      relay_total=None, collect_append=True):
    """완료된 스캔의 job stats와 추가 파일 목록을 이력 행에 채운다.
    저장은 호출처의 finish()가 한다. 주기적 스캔·휴지통 스캔이 함께 쓴다.
    도서 스캔이면 relay_total(Relay 실측 파일 수)이 있을 때 stat_total을
    그 값으로 덮어쓴다. relay_total을 안 주면 여기서 직접 읽어본다.
    collect_append=False면 추가 파일 목록을 수집하지 않는다 — 휴지통
    작업은 스캔이 휴지통 항목을 다시 확인하는 용도라 추가 파일이
    의미가 없다."""
    item.shyni_job_id = job_id
    item.vfs_duration = vfs_duration
    item.scan_duration = scan_duration
    if not isinstance(stats, dict):
        return
    # 도서 스캔 stats는 total/missing 키가 없고 entries/missing_files
    # 키를 쓴다. 신규에 대응하는 키는 stats에 없어서 None으로 둔다
    # (아래에서 추가 파일 비교 수로 채운다).
    item.stat_total = _stat_first(stats, 'total', 'entries')
    # 도서 '전체'는 entries(이번 스캔 처리 수)가 아니라 Relay 실측
    # 파일 수다. 호출처가 미리 읽은 값이 없으면 여기서 읽어본다.
    # 도서가 아니면 relay_total이 와도 무시한다 (호출처 실수 대비).
    if relay_total is None:
        relay_total = book_relay_total(stats, section_key)
    if relay_total is not None and _is_book_stats(stats):
        item.stat_total = relay_total
    item.stat_new = _stat_int(stats.get('new'))
    item.stat_missing = _stat_first(stats, 'missing', 'missing_files')
    item.stat_updated = _stat_int(stats.get('updated'))
    item.stat_restored = _stat_int(stats.get('restored'))
    item.stat_registered = _stat_int(stats.get('registered'))
    item.stat_failed = _stat_int(stats.get('failed'))
    results = stats.get('results')
    if isinstance(results, list) and results:
        try:
            item.stat_folders = json.dumps(results[:STAT_FOLDERS_MAX],
                                           ensure_ascii=False)
        except Exception as e:
            logger.warning(f'[{label}] 폴더별 결과 저장 실패: {e}')
    try:
        from . import db_support
        if collect_append and db_support.db_ready():
            analyze = stats.get('analyze') or {}
            ids = analyze.get('metadata_item_ids') or [] if isinstance(analyze, dict) else []
            files, count = db_support.collect_append_paths(
                int(section_key), ids, pre_max_id, item.stat_new)
            if files is not None:
                item.append_files = json.dumps(files, ensure_ascii=False)
                item.append_count = count
                # 도서 스캔은 shyni stats에 신규 키가 없어 stat_new이
                # 비어 있다. 추가 파일 비교로 측정한 수가 있으면 그걸로
                # 채운다 — shyni 집계가 아니라 플러그인 측정치다.
                if item.stat_new is None:
                    item.stat_new = count
    except Exception as e:
        logger.warning(f'[{label}] 추가 파일 목록 수집 실패: {e}')


def scan_summary_text(stats, relay_total=None):
    """대시보드 최근 작업 줄에 붙일 요약 — '신규 N · 전체 N'.
    도서 스캔은 stats에 total이 없고 entries(이번 스캔 처리 수)만
    있다. Relay 실측값(relay_total)을 주면 '전체 N'으로, 없으면
    '처리 N'으로 표시한다. 도서가 아니면 relay_total을 무시한다."""
    if not isinstance(stats, dict):
        return None
    new = _stat_int(stats.get('new'))
    total = _stat_first(stats, 'total', 'entries')
    if new is None and total is None:
        return None
    parts = []
    if new is not None:
        parts.append(f'신규 {new}')
    if total is not None:
        if _is_book_stats(stats):
            if relay_total is not None:
                parts.append(f'전체 {relay_total}')
            else:
                parts.append(f'처리 {total}')
        else:
            parts.append(f'전체 {total}')
    return ' · '.join(parts)


def get_jobs():
    """ModelSetting에 저장된 주기 작업 목록."""
    raw = P.ModelSetting.get(JOBS_KEY)
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                for idx, item in enumerate(data):
                    item['job_id'] = f'{P.package_name}_periodic_{idx}'
                return data
        except Exception:
            pass
    return []


def _parse_currents(raw):
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


def get_currents():
    """지금 돌고 있는 주기적 스캔 진행 상태 목록 (claim_id 순)."""
    return _parse_currents(P.ModelSetting.get(CURRENT_KEY))


def _save_currents(curs):
    P.ModelSetting.set(CURRENT_KEY, json.dumps(curs, ensure_ascii=False))


def _upsert_current(cur):
    """진행 상태 한 건을 넣거나 고친다 — claim_id가 같은 항목이 있으면
    그 자리에서 합치고(새 값이 이긴다), 없으면 뒤에 붙인다.
    합치는 방식이라 차지할 때 넣어 둔 claimed_at 같은 값은
    섹션이 바뀌어도 유지된다."""
    curs = get_currents()
    claim_id = (cur or {}).get('claim_id')
    for i, c in enumerate(curs):
        if claim_id is not None and c.get('claim_id') == claim_id:
            merged = dict(c)
            merged.update(cur)
            curs[i] = merged
            _save_currents(curs)
            return
    curs.append(cur)
    _save_currents(curs)


def _remove_current(claim_id):
    """claim_id가 같은 진행 상태를 지운다. 없으면 그대로 둔다."""
    if claim_id is None:
        return
    curs = [c for c in get_currents() if c.get('claim_id') != claim_id]
    _save_currents(curs)


def _clear_currents():
    P.ModelSetting.set(CURRENT_KEY, '')


def update_current_job_idx(claim_id, new_idx):
    """진행 상태의 작업 번호를 바꾼다 — 작업 관리에서 순서를 바꿔
    저장했을 때 진행 중인 작업의 위치를 새 목록에 맞게 고친다.
    claim_id로 그 작업의 진행 상태만 고친다."""
    curs = get_currents()
    for cur in curs:
        if cur.get('claim_id') == claim_id:
            if cur.get('job_idx') == new_idx:
                return
            cur['job_idx'] = new_idx
            _save_currents(curs)
            return


def _max_wait_min():
    """작업 대기 시간(분) — 설정값, 없거나 숫자가 아니면 기본 360분.
    음수는 기본값으로 취급한다. 0이면 기다리지 않는다."""
    try:
        val = P.ModelSetting.get_int(MAX_WAIT_KEY)
    except Exception:
        val = None
    if val is None or val < 0:
        return DEFAULT_MAX_WAIT_MIN
    return val


def _vfs_timeout_sec():
    """VFS 새로고침 제한 시간(초) — 설정값(분)을 초로 바꾼다.
    없거나 숫자가 아니거나 0 이하면 기본 360분으로 동작한다."""
    try:
        val = P.ModelSetting.get_int(VFS_TIMEOUT_KEY)
    except Exception:
        val = None
    if val is None or val <= 0:
        return DEFAULT_VFS_TIMEOUT_MIN * 60
    return val * 60


def _scan_timeout_sec():
    """스캔 대기 기한(초) — 설정값(분)을 초로 바꾼다.
    없거나 숫자가 아니거나 0 이하면 기본 360분으로 동작한다."""
    try:
        val = P.ModelSetting.get_int(SCAN_TIMEOUT_KEY)
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
    스케줄이 겹쳐 같은 작업이 두 번 도는 것을 막는다.
    아직 시작하지 않은 복구 항목('waiting')은 세지 않는다 — 복구
    작업이 뜨지 않으면 이 항목이 영원히 남아 뒤의 실행을 전부 막아
    버린다. 복구 작업이 뜨면 단계를 바꿔 다시 센다."""
    try:
        for cur in get_currents():
            if (cur.get('stage') == 'waiting'
                    and cur.get('resume_item_id') is not None):
                continue
            if (cur.get('job_name') or '') == (name or ''):
                return True
    except Exception:
        pass
    try:
        row = (F.db.session.query(ModelPeriodicItem.id)
               .filter(ModelPeriodicItem.job_name == name,
                       ModelPeriodicItem.status == 'waiting').first())
        return row is not None
    except Exception as e:
        logger.warning(f'[주기적 스캔] 대기 중 작업 조회 실패: {e}')
        return False


# 규칙 1 — 같은 모듈 안에서 같은 라이브러리가 진행 중일 때 내는 문구.
# 동작 종류(새로고침·스캔·새로고침 후 스캔)와 무관하게 이 문구로 거절한다.
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
        for cur in get_currents():
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
        curs = get_currents()
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
    새로고침만 하는 동작은 스캔 자리를 쓰지 않지만 규칙 1은 본다.
    첫 라이브러리를 기준으로 검사한다 — 나중 라이브러리는 섹션
    시작 때 게이트가 다시 본다."""
    if _same_library_busy(first_key, exclude_claim_id=exclude_claim_id):
        return False, LIBRARY_BUSY_MESSAGE
    if 'scan' not in str(action or ''):
        return True, None
    try:
        msg = ModuleStatus.library_gate_check(first_key)
    except Exception as e:
        logger.warning(f'[주기적 스캔] 게이트 검사 실패: {e}')
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
    stage = 'vfs' if action in ('refresh', 'refresh_scan') else 'scan'
    claimed_at = int(time.time() * 1000)
    _upsert_current({
        'job_idx': idx, 'job_name': name,
        'section_key': str(first_key) if first_key is not None else '',
        'section_title': section_label, 'stage': stage, 'mode': mode,
        'job_started_at': job_started, 'section_started_at': _now_text(),
        'claim_id': claim_id, 'job_row_id': job_row_id, 'action': action,
        'claimed_at': claimed_at})
    time.sleep(random.uniform(0.3, 1.0))
    curs = get_currents()
    if not any(c.get('claim_id') == claim_id for c in curs):
        return False
    # 규칙 1 재확인 — 게이트를 보고 차지하는 사이에 같은
    # 라이브러리의 다른 작업이 차지했을 수 있다. 스캔이 안 들어간
    # 동작도 같은 라이브러리와 겹치면 물러난다. 동시에 차지한
    # 작업끼리는 차지한 시각 순서로 가른다.
    try:
        survives1 = _rule1_survives(claim_id, first_key, claimed_at)
    except Exception as e:
        logger.warning(f'[주기적 스캔] 규칙 1 재확인 실패: {e}')
        survives1 = True
    if not survives1:
        _remove_current(claim_id)
        return False
    if 'scan' in str(action or ''):
        # 자리 재확인 — 게이트를 보고 차지하는 사이에 다른 작업이
        # 차지했을 수 있다. 동시에 차지한 작업끼리는 차지한 시각
        # 순서로 가른다 (앞 순서가 자리를 갖고, 뒤 순서가 물러난다).
        try:
            survives = ModuleStatus.claim_survives(claim_id, first_key, claimed_at)
        except Exception as e:
            logger.warning(f'[주기적 스캔] 자리 재확인 실패: {e}')
            survives = True
        if not survives:
            _remove_current(claim_id)
            return False
    return True


def _finish_job_row(row_id, status, message):
    """대기 행을 끝낸다 — 행을 id로 다시 읽어 끝낸다.
    (세션이 달라져도 찾을 수 있게 객체를 들고 있지 않는다.)"""
    try:
        item = (F.db.session.query(ModelPeriodicItem)
                .filter(ModelPeriodicItem.id == row_id).first())
        if item is not None:
            item.finish(status, message)
    except Exception as e:
        logger.warning(f'[주기적 스캔] 대기 행 정리 실패: {e}')


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
    section_label — 대기 행에 표시할 라이브러리명. 대상이 1개면
    그 이름, 여러 개면 '전체'. first_key — 첫 라이브러리 번호."""
    claim_id = f'run-{idx}-{int(time.time() * 1000)}'
    free, gate_msg = _slot_free(first_key, action,
                                exclude_claim_id=claim_id)
    if free and _try_claim(idx, name, mode, job_started, None, claim_id,
                           section_label, action, first_key):
        return (None, 0, claim_id)
    if mode == 'manual':
        fail_start(name, mode,
                   gate_msg or '이미 진행 중인 주기적 스캔이 있어 시작하지 못했습니다.',
                   section_label, action, status='skipped')
        return (None, None, None)
    max_min = _max_wait_min()
    if max_min <= 0:
        fail_start(name, mode,
                   gate_msg or '이미 진행 중인 주기적 스캔이 있어 시작하지 못했습니다.',
                   section_label, action, status='skipped')
        return (None, None, None)
    running_names = [(c.get('job_name') or '') for c in get_currents()]
    running_names = [n for n in running_names if n]
    if running_names:
        shown = ', '.join(running_names[:2])
        extra = f' 외 {len(running_names) - 2}개' if len(running_names) > 2 else ''
        wait_msg = f'앞선 작업({shown}{extra}) 진행 중으로 끝나면 이어서 진행합니다.'
    else:
        wait_msg = '앞선 작업 진행 중으로 끝나면 이어서 진행합니다.'
    item = ModelPeriodicItem(name, '', section_label, mode, action)
    item.status = 'waiting'
    item.message = wait_msg
    try:
        item.save()
    except Exception as e:
        logger.error(f'[주기적 스캔] 대기 행 저장 실패: {e}')
        fail_start(name, mode, '대기 기록을 남기지 못했습니다.', section_label, action)
        return (None, None, None)
    row_id = item.id
    # 대기 중 표시 — 작업 관리·작업 결과 목록에 '대기중' 카드가
    # 나오게 진행 상태에도 넣어 둔다. 차지할 때 _try_claim이 같은
    # claim_id로 덮어써 실행 중 표시로 바뀐다. 대기 중 표시는
    # 자리를 예약하지 않는다 — 게이트 집계에서는 세지 않는다.
    _upsert_current({
        'job_idx': idx, 'job_name': name,
        'section_key': str(first_key) if first_key is not None else '',
        'section_title': section_label, 'stage': 'waiting', 'mode': mode,
        'job_started_at': job_started, 'section_started_at': _now_text(),
        'claim_id': claim_id, 'job_row_id': row_id, 'action': action})
    t0 = time.time()
    deadline = t0 + max_min * 60
    while time.time() < deadline:
        time.sleep(WAIT_POLL_SEC)
        # 대기 중 취소 — 카드의 취소 버튼이 먹히게 여기서도 확인한다.
        if _cancel_requested(claim_id):
            _finish_job_row(row_id, 'cancelled', '취소 요청으로 대기를 중단했습니다.')
            _remove_current(claim_id)
            _clear_cancel(claim_id)
            logger.info(f'[주기적 스캔] {name}: 대기 중 취소됐습니다.')
            return (None, None, None)
        free, _ = _slot_free(first_key, action, exclude_claim_id=claim_id)
        if free and _try_claim(idx, name, mode, job_started, row_id, claim_id,
                               section_label, action, first_key):
            waited = int(time.time() - t0)
            try:
                row = (F.db.session.query(ModelPeriodicItem)
                       .filter(ModelPeriodicItem.id == row_id).first())
                if row is not None:
                    row.status = 'running'
                    row.wait_seconds = waited
                    row.message = f'대기 {_fmt_wait(waited)} 후 시작했습니다.'
                    row.save()
            except Exception as e:
                logger.warning(f'[주기적 스캔] 대기 행 시작 기록 실패: {e}')
            return (row_id, waited, claim_id)
    _finish_job_row(row_id, 'failed',
                    f'앞선 작업이 끝나기를 기다리다 시간 초과됐습니다. (작업 대기 {_fmt_wait(max_min * 60)})')
    _remove_current(claim_id)
    _clear_cancel(claim_id)
    logger.warning(f'[주기적 스캔] {name}: 작업 대기 시간 초과로 시작하지 않습니다.')
    return (None, None, None)


def _record_start_error(job_name, reason):
    """시작 실패를 기록한다 — 작업 관리 배너가 읽는다.
    기록 실패가 실행을 막지 않게 예외를 삼킨다."""
    try:
        P.ModelSetting.set(START_ERROR_KEY, json.dumps(
            {'at': time.time(), 'job_name': job_name or '',
             'reason': str(reason or '')[:500]},
            ensure_ascii=False))
    except Exception as e:
        logger.warning(f'[주기적 스캔] 시작 오류 기록 실패: {e}')


def _clear_start_error():
    try:
        P.ModelSetting.set(START_ERROR_KEY, '')
    except Exception:
        pass


def clear_start_error():
    """시작 오류 기록을 지운다 (배너 닫기용)."""
    _clear_start_error()


def get_last_start_error(max_age_sec=START_ERROR_MAX_AGE_SEC):
    """가장 최근 시작 오류 — 없거나 깨졌거나 오래됐으면 None."""
    raw = P.ModelSetting.get(START_ERROR_KEY)
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


def fail_start(job_name, mode, reason, section_label='전체', action=None,
               status='failed'):
    """작업 단위 시작 실패 — 이력 행을 남기고 배너용 오류를 기록한다.
    이력 행 저장이 실패해도 배너 기록은 시도한다.
    section_label — 이력 행에 표시할 라이브러리명. 대상을 알면 그 이름,
    모르면 '전체'. status — 기본 'failed'. 자리가 없어 시작하지 못한
    경우(게이트 차단)는 'skipped'로 남기고 배너에는 띄우지 않는다 —
    실패가 아니라 건너뛴 것이기 때문이다."""
    try:
        item = ModelPeriodicItem(job_name, '', section_label, mode, action)
        item.finish(status, reason)
    except Exception as e:
        logger.error(f'[주기적 스캔] 시작 실패 이력 기록 실패: {e}')
    if status == 'failed':
        _record_start_error(job_name, reason)


def _cancel_ids():
    """취소 요청된 작업의 claim_id 목록. 구 형식 '1'(전부 취소)이면
    ['*']로, 비어 있거나 깨졌으면 빈 목록으로 돌려준다."""
    raw = P.ModelSetting.get(CANCEL_KEY) or ''
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


def _cancel_requested(claim_id=None):
    """이 작업에 취소 요청이 들어왔으면 True. claim_id가 없으면
    구 형식 '1'(전부 취소)일 때만 True를 돌려준다."""
    ids = _cancel_ids()
    if '*' in ids:
        return True
    if claim_id is None:
        return False
    return str(claim_id) in ids


def _clear_resume_current(item_id):
    """복구용 진행 상태면 지운다 — 새 실행의 진행 상태를 건드리지 않는다."""
    try:
        for cur in get_currents():
            if str(cur.get('resume_item_id')) == str(item_id):
                _remove_current(cur.get('claim_id'))
                return
    except Exception:
        pass


def request_cancel(claim_id):
    """이 작업의 취소를 요청한다 — 다른 작업의 취소 요청은 건드리지 않는다."""
    if claim_id is None:
        return
    ids = _cancel_ids()
    if '*' in ids or str(claim_id) in ids:
        return
    ids.append(str(claim_id))
    P.ModelSetting.set(CANCEL_KEY, json.dumps(ids, ensure_ascii=False))


def _clear_cancel(claim_id):
    """끝난 작업의 취소 요청을 지운다 — 목록이 쌓이지 않게 한다."""
    if claim_id is None:
        return
    ids = [x for x in _cancel_ids() if x != str(claim_id)]
    if '*' in ids:
        return
    P.ModelSetting.set(CANCEL_KEY,
                       json.dumps(ids, ensure_ascii=False) if ids else '')


def _vfs_resume_decision(job):
    """VFS 복구 판정 — rclone job/status 조회 결과 → 갈래와 사유.
    순수 함수(하니스 검증용).
    - finished=false → 'poll': 아직 돌고 있으니 그 job을 이어서 기다린다.
    - finished=true, error 없음 → 'done': 끝났으니 완료로 기록하고 스캔으로 간다.
    - finished=true, error 있음 → 'restart': 새로고침이 실패했으니 처음부터 다시 건다.
    - 'not_found' → 'restart': 끝난 뒤 기록이 만료됐을 수 있고, 따뜻한
      캐시에서 다시 걸면 금방 끝나니(0.1초 실측) 처음부터 다시 건다.
    - 그 외(조회 실패) → 'restart': 다시 걸면 돌고 있는 작업을 빌려 타거나
      따뜻한 캐시를 걷으니 안전하다."""
    if job == 'not_found':
        return 'restart', 'VFS 새로고침 작업 기록이 없어 처음부터 다시 새로고침합니다.'
    if not isinstance(job, dict):
        return 'restart', 'VFS 새로고침 작업 확인에 실패해 처음부터 다시 새로고침합니다.'
    if not job.get('finished'):
        return 'poll', 'VFS 새로고침이 아직 진행 중이라 이어서 기다립니다.'
    if job.get('error'):
        return 'restart', f"VFS 새로고침이 실패했습니다: {job['error']}"
    return 'done', 'VFS 새로고침이 완료됐습니다.'


_RUN_ID_CACHE = {'done': False, 'run_id': None, 'exact': False}


def _proc_start_epoch(pid):
    """프로세스 기동 시각(epoch) — /proc/<pid>/stat 22번 필드와
    /proc/stat btime으로 계산한다. 실패하면 None."""
    try:
        with open(f'/proc/{pid}/stat', 'r') as f:
            stat = f.read()
        rparen = stat.rfind(')')
        if rparen < 0:
            return None
        parts = stat[rparen + 2:].split()
        start_ticks = int(parts[19])
        btime = None
        with open('/proc/stat', 'r') as f:
            for line in f:
                if line.startswith('btime '):
                    btime = int(line.split()[1])
                    break
        if btime is None:
            return None
        try:
            hz = os.sysconf('SC_CLK_TCK')
        except Exception:
            hz = 100
        return btime + start_ticks / hz
    except Exception:
        return None


def _ff_run_id():
    """FF 기동 식별자 — 메인 프로세스(플라스크팜) 기동 시각(epoch)과
    정확 여부. 웹은 자기 것, 워커는 부모(메인)의 것을 쓴다 — 둘 다
    같은 메인을 가리키니 같은 값이 나온다. 부모 명령줄에 'flaskfarm'이
    없으면 자기 것을 쓴다. /proc를 못 읽으면 모듈 로드 시각을 폴백으로
    쓰고 정확하지 않다고 표시한다. 결과를 캐시한다 (기동 중 불변)."""
    if _RUN_ID_CACHE['done']:
        return _RUN_ID_CACHE['run_id'], _RUN_ID_CACHE['exact']
    run_id = None
    exact = False
    try:
        ppid = os.getppid()
        use_parent = False
        try:
            with open(f'/proc/{ppid}/cmdline', 'rb') as f:
                if b'flaskfarm' in f.read():
                    use_parent = True
        except Exception:
            pass
        pid = ppid if use_parent else os.getpid()
        run_id = _proc_start_epoch(pid)
        exact = run_id is not None
    except Exception:
        run_id = None
    if run_id is None:
        run_id = _PLUGIN_LOAD_TIME
        exact = False
    _RUN_ID_CACHE['done'] = True
    _RUN_ID_CACHE['run_id'] = run_id
    _RUN_ID_CACHE['exact'] = exact
    return run_id, exact


def _format_run_id(run_id):
    """기동 식별자를 저장용 문자열로 — 소수 둘째 자리."""
    try:
        return f'{float(run_id):.2f}'
    except Exception:
        return ''


def _parse_run_id(raw):
    """저장된 기동 식별자 문자열을 실수로 — 깨졌으면 None."""
    try:
        if raw is None:
            return None
        return float(str(raw).strip())
    except Exception:
        return None


def _same_run(a, b, exact):
    """두 기동 식별자가 같은 기동인지 — 정확한 값이면 1초, 폴백
    (모듈 로드 시각)이면 3분 창 안에서 같은 기동으로 본다."""
    if a is None or b is None:
        return False
    try:
        window = (RUN_ID_TOLERANCE_SEC if exact
                  else RUN_ID_FALLBACK_WINDOW_SEC)
        return abs(float(a) - float(b)) < window
    except Exception:
        return False


def _is_interrupted_run(start_time, run_start,
                        margin_sec=RUN_START_MARGIN_SEC):
    """이력이 기동보다 전에 시작됐으면 True — 끊긴 것으로 보고 복구
    대상이다. 기동 이후에 시작됐으면 살아 있는 실행이다. 시작 시각이
    없으면 끊긴 것으로 본다. 순수 함수(하니스 검증용)."""
    if start_time is None:
        return True
    try:
        return start_time.timestamp() < run_start - margin_sec
    except Exception:
        return True


def _entry_is_live(cur, run_start, margin_sec=RUN_START_MARGIN_SEC):
    """진행 상태 항목이 기동 이후에 시작됐으면 True — 살아 있는
    실행의 표시다. 시작 시각이 없거나 못 읽으면 살아 있는 것으로 본다
    (죽은 표시를 남기는 것보다 살아 있는 표시를 지우는 쪽이 위험하다).
    순수 함수(하니스 검증용)."""
    try:
        raw = cur.get('job_started_at') if isinstance(cur, dict) else None
        if not raw:
            return True
        started = datetime.strptime(str(raw), '%Y-%m-%d %H:%M:%S')
        return started.timestamp() >= run_start - margin_sec
    except Exception:
        return True


def _live_entry_for_section(currents, section_key, run_start):
    """같은 라이브러리에 살아 있는 실행의 표시(복구 항목이 아닌 것)가
    있으면 True. 순수 함수(하니스 검증용)."""
    try:
        for cur in currents or []:
            if not isinstance(cur, dict):
                continue
            if cur.get('resume_item_id') is not None:
                continue
            if str(cur.get('section_key') or '') != str(section_key):
                continue
            if _entry_is_live(cur, run_start):
                return True
    except Exception:
        pass
    return False


def _merge_resume_currents(old, resume_entries, keep_resume_ids=None,
                           run_start=None):
    """복구 항목과 살아 있는 실행의 진행 상태를 합친다 — 순수 함수
    (하니스 검증용). 복구 항목은 같은 라이브러리의 옛 표시를 대신하고,
    복구 대상이 아닌 표시 중 살아 있는 것만 둔다. run_start가 주어지면
    기동보다 전에 시작된 표시는 죽은 것으로 보고 버린다 (지난 기동의
    죽은 카드가 계속 남는 것을 막는다). 낡은 복구 항목은 버린다 —
    복구 작업이 뜨면 시작할 때 다시 남기니, 남아 있는 것은 뜨지 않은
    작업의 것이다. keep_resume_ids — 이미 복구 작업이 떠서 진행 중인
    이력 id 집합. 이 항목들은 그대로 둔다."""
    keep_resume_ids = keep_resume_ids or set()
    resume_keys = {str(e.get('section_key')) for e in resume_entries}
    kept = []
    for cur in old:
        if not isinstance(cur, dict):
            continue
        rid = cur.get('resume_item_id')
        if rid is not None:
            try:
                if int(rid) in keep_resume_ids:
                    kept.append(cur)
            except Exception:
                pass
            continue
        if str(cur.get('section_key')) in resume_keys:
            continue
        if run_start is not None and not _entry_is_live(cur, run_start):
            continue
        kept.append(cur)
    return list(resume_entries) + kept


def _resume_already_done(run_id, exact):
    """이 기동에서 resume_running이 이미 돌았으면 True — 마커에
    남긴 기동 식별자와 지금 기동을 비교한다."""
    try:
        raw = P.ModelSetting.get(RESUME_RUN_KEY) or ''
        if not raw:
            return False
        return _same_run(_parse_run_id(raw), run_id, exact)
    except Exception:
        return False


def _mark_resume_done(run_id):
    """복구 실행 기록을 지금 기동 식별자로 남긴다."""
    try:
        P.ModelSetting.set(RESUME_RUN_KEY, _format_run_id(run_id))
    except Exception as e:
        logger.warning(f'[주기적 스캔 복구] 복구 기록 저장 실패: {e}')


class TaskPeriodic:
    """주기적 스캔 실행 — 선택한 섹션을 하나씩 순차로 처리한다.

    단계: (옵션) VFS 새로고침 → 완료 확인 → 스캔 전송 → 완료 확인.
    진행 상태는 ModelSetting periodic_current에 목록으로 기록해
    현황/주기 화면이 공유한다. 여러 작업이 설정한 '라이브러리 동시
    스캔 수'까지 함께 돌 수 있다. 취소는 periodic_cancel에 claim_id
    목록으로 요청받고, 단계 폴링 중 확인해 멈춘다.
    완료된 스캔의 집계는 이력 행에 남기고, 진행 상황은 대시보드 섹션 상태
    (status_sec_N)에도 수동 작업과 같은 모양으로 기록한다.
    """

    @F.celery.task
    def start(idx, mode):
        with F.app.app_context():
            try:
                TaskPeriodic._run(idx, mode)
            except Exception as e:
                # _run이 차지한 뒤의 오류는 _run 안에서 자기 진행
                # 상태·이력 행까지 정리한다. 여기까지 올라오는 건
                # 차지하기 전의 오류라 진행 상태를 건드리지 않는다
                # (다른 작업의 진행 상태를 지우면 안 된다).
                logger.error(f'주기적 스캔 실행 실패: {e}')
                logger.error(traceback.format_exc())
                try:
                    jobs = get_jobs()
                    job = jobs[idx] if 0 <= idx < len(jobs) else None
                except Exception:
                    job = None
                if job is not None:
                    fail_start(job.get('name') or f'작업 {idx + 1}',
                               mode, f'실행 중 오류가 발생했습니다: {e}',
                               action=periodic_kind_use_vfs_to_action(
                                   job.get('kind'), job.get('use_vfs')))

    @staticmethod
    def _run(idx, mode):
        jobs = get_jobs()
        if idx < 0 or idx >= len(jobs):
            logger.error(f'주기적 스캔: 잘못된 작업 번호 {idx}')
            _record_start_error('', f'잘못된 작업 번호라 시작하지 못했습니다: {idx}')
            return
        job = jobs[idx]
        job_name = job.get('name') or ''
        # 이력·진행 상태에 남길 동작 — 작업 정의의 kind + use_vfs를
        # 동작 코드로 되돌린다 (refresh | scan | refresh_scan).
        action = periodic_kind_use_vfs_to_action(job.get('kind'), job.get('use_vfs'))
        # 같은 작업이 이미 돌거나 기다리고 있으면 중복으로 세지 않는다.
        if _same_job_busy(job_name):
            logger.info(f'주기적 스캔: {job_name} — 이미 실행 중이거나 대기 중이라 건너뜁니다.')
            return
        sections, err = ModuleStatus._sections()
        if err:
            logger.error(f'주기적 스캔: 섹션 조회 실패 — {err}')
            fail_start(job_name, mode, err, action=action)
            return
        sec_map = {str(s['key']): s for s in sections}
        if not (job.get('sections') or []):
            fail_start(job_name, mode, '선택한 라이브러리가 없어 시작하지 못했습니다.', action=action)
            return
        if not any(str(k) in sec_map for k in (job.get('sections') or [])):
            fail_start(job_name, mode,
                       '선택한 라이브러리를 shyni에서 찾지 못했습니다. '
                       '라이브러리가 삭제됐거나 이름이 바뀌었는지 확인해 주세요.',
                       action=action)
            return
        _clear_start_error()
        any_scan_done = False
        job_started = _now_text()
        logger.info(f"주기적 스캔 시작: {job.get('name')} ({mode})")
        # 작업 단위 행(대기 행) 표시 — 대상이 라이브러리 1개면 그 이름을 쓰고,
        # 여러 개면 '전체'로 둔다. 1개짜리 작업에 '전체'가 붙으면 모든
        # 라이브러리를 건드린 것처럼 보여 혼란스럽다.
        _valid_keys = [k for k in (job.get('sections') or []) if str(k) in sec_map]
        job_label = (sec_map[str(_valid_keys[0])].get('title') or str(_valid_keys[0])
                     if len(_valid_keys) == 1 else '전체')
        # 앞선 작업이 돌고 있으면 기다렸다가 차례가 되면 실행한다.
        # 기다리다 시간 초과하면 _wait_for_turn이 실패 행을 남긴다.
        # 자리 검사는 첫 라이브러리 기준으로 한다 — 나중 라이브러리는
        # 섹션 시작 때 게이트가 다시 본다.
        job_row_id, _waited, claim_id = _wait_for_turn(
            idx, job_name, mode, job_started, job_label, action,
            first_key=_valid_keys[0])
        if _waited is None:
            return
        try:
            for key in job.get('sections') or []:
                if _cancel_requested(claim_id):
                    logger.info('주기적 스캔: 취소 요청으로 중단합니다.')
                    break
                sec = sec_map.get(str(key))
                if sec is None:
                    continue
                if TaskPeriodic._run_section(job, idx, sec, mode, job_started,
                                             job_row_id, claim_id):
                    any_scan_done = True
                if _cancel_requested(claim_id):
                    break
        except Exception as e:
            # 차지한 뒤의 오류는 여기서 자기 것만 정리한다 — 다른
            # 작업의 진행 상태·이력 행은 건드리지 않는다.
            logger.error(f'주기적 스캔 실행 실패: {e}')
            logger.error(traceback.format_exc())
            _remove_current(claim_id)
            _clear_cancel(claim_id)
            if job_row_id is not None:
                _finish_job_row(job_row_id, 'failed', f'실행 중 오류가 발생했습니다: {e}')
            else:
                fail_start(job_name, mode, f'실행 중 오류가 발생했습니다: {e}',
                           action=action)
            return
        if job_row_id is not None:
            if _cancel_requested(claim_id):
                _finish_job_row(job_row_id, 'cancelled', '취소 요청으로 중단했습니다.')
            else:
                _finish_job_row(job_row_id, 'done', '완료됐습니다.')
        _remove_current(claim_id)
        _clear_cancel(claim_id)
        if any_scan_done:
            # 스캔으로 통계가 바뀌었을 수 있어 현황 통계 재계산을 요청한다
            try:
                from .task_status import request_stats
                request_stats()
            except Exception as e:
                logger.warning(f'[주기적 스캔] 통계 재계산 요청 실패: {e}')
        logger.info(f"주기적 스캔 종료: {job.get('name')}")

    @staticmethod
    def _section_skip_reason(key):
        """섹션 시작 시 건너뜀 사유 — None이면 진행한다.
        휴지통 스캔 중인 섹션은 건너뛰고, 가드 조회에 실패하면
        fail-closed로 건너뛴다 (W5). 섹션 시작 시마다 다시 읽는다."""
        trash_parse_ok = True
        trash_section_keys = set()
        raw_cur = P.ModelSetting.get('trash_current')
        if raw_cur:
            try:
                cur = json.loads(raw_cur)
                if isinstance(cur, dict):
                    if cur.get('section_key') and cur.get('stage') != 'waiting':
                        trash_section_keys.add(str(cur.get('section_key')))
                elif isinstance(cur, list):
                    for c in cur:
                        if (isinstance(c, dict) and c.get('section_key')
                                and c.get('stage') != 'waiting'):
                            trash_section_keys.add(str(c.get('section_key')))
                else:
                    trash_parse_ok = False
            except Exception:
                trash_parse_ok = False
        try:
            st = ModuleStatus._get_state(int(key))
            status_busy = any((st.get(k) or {}).get('status') == 'running'
                              for k in ('vfs', 'scan', 'meta', 'trash'))
        except Exception:
            status_busy = False
        return _periodic_skip_decision(trash_parse_ok,
                                       str(key) if str(key) in trash_section_keys else None,
                                       status_busy, key)

    @staticmethod
    def _fail_running_status(key, job_name):
        """예외로 섹션이 끝났을 때 이 작업이 남긴 running 표시를 실패로 정리한다."""
        try:
            st = ModuleStatus._get_state(int(key))
            changed = False
            for slot in ('vfs', 'scan'):
                entry = st.get(slot) or {}
                if (entry.get('status') == 'running'
                        and entry.get('source') == 'periodic'
                        and entry.get('job_name') == job_name):
                    entry['status'] = 'failed'
                    entry['finished_at'] = _now_text()
                    changed = True
            if changed:
                ModuleStatus._set_state(int(key), st)
        except Exception as e:
            logger.warning(f'[주기적 스캔] 현황 상태 정리 실패: {e}')

    @staticmethod
    def _run_section(job, idx, sec, mode, job_started, job_row_id=None,
                     claim_id=None):
        key = sec['key']
        job_name = job.get('name') or ''
        title = sec.get('title') or ''
        action = periodic_kind_use_vfs_to_action(job.get('kind'), job.get('use_vfs'))
        skip_reason = TaskPeriodic._section_skip_reason(key)
        if skip_reason is not None:
            item = ModelPeriodicItem(job_name, key, title, mode, action)
            item.finish('skipped', skip_reason)
            logger.info(f'주기적 스캔: {title} {skip_reason}')
            return False
        item = ModelPeriodicItem(job_name, key, title, mode, action)
        item.save()
        vfs_only = job.get('kind') == 'vfs'
        cur = {
            'job_idx': idx,
            'job_name': job_name,
            'section_key': str(key),
            'section_title': title,
            'stage': 'vfs' if (vfs_only or job.get('use_vfs')) else 'scan',
            'mode': mode,
            'job_started_at': job_started,
            'section_started_at': _now_text(),
            'job_row_id': job_row_id,
            'claim_id': claim_id,
            'action': action,
        }
        _upsert_current(cur)
        logger.info(f"주기적 스캔: {title} 시작 (종류: {'VFS 새로고침만' if vfs_only else '스캔'})")
        vfs_duration = None
        scan_duration = None
        try:
            if vfs_only or job.get('use_vfs'):
                vfs_started = _now_text()
                _write_status(key, 'vfs', {
                    'job': None, 'path': sec.get('path', ''),
                    'started_at': vfs_started, 'finished_at': None,
                    'status': 'running', 'source': 'periodic',
                    'job_name': job_name})

                def _on_vfs_job(vfs_jobid):
                    _write_status(key, 'vfs', {
                        'job': vfs_jobid, 'path': sec.get('path', ''),
                        'started_at': vfs_started, 'finished_at': None,
                        'status': 'running', 'source': 'periodic',
                        'job_name': job_name})
                    # rclone 작업 번호를 이력 행에도 남긴다 — 이력 상세에서
                    # 새로고침 job id를 볼 수 있다.
                    item.vfs_job_id = str(vfs_jobid)
                    item.save()

                vres = TaskPeriodic._do_vfs(sec, cur, on_job_id=_on_vfs_job)
                vfs_duration = vres['duration']
                vfs_status = {'done': 'done', 'failed': 'failed',
                              'cancelled': 'cancelled'}.get(vres['status'], 'failed')
                _write_status(key, 'vfs', {
                    'job': vres.get('jobid'), 'path': sec.get('path', ''),
                    'started_at': vfs_started, 'finished_at': _now_text(),
                    'status': vfs_status, 'source': 'periodic',
                    'job_name': job_name})
                if vres['status'] != 'done':
                    item.vfs_duration = vfs_duration
                    if vres['status'] == 'cancelled':
                        item.finish('cancelled', vres['message'])
                    else:
                        item.finish('failed', vres['message'])
                    return False
                if vfs_only:
                    item.vfs_duration = vfs_duration
                    item.finish('done', 'VFS 새로고침이 완료됐습니다.')
                    return False
                # VFS 소요를 미리 저장해 둔다 — 스캔 중 재시작돼도
                # 복구한 이력의 상세에 VFS 소요가 남는다.
                item.vfs_duration = vfs_duration
                item.save()
                cur['stage'] = 'scan'
                _upsert_current(cur)
            # 스캔 시작 전 MAX(media_parts.id)를 기록해 둔다 —
            # 추가 파일 목록 2차 방법(전후 비교)용 기준점이다.
            pre_max_id = None
            try:
                from . import db_support
                if db_support.db_ready():
                    pre_max_id = db_support.parts_max_id()
            except Exception as e:
                logger.warning(f'[주기적 스캔] 추가 파일 기준점 기록 실패: {e}')
            if pre_max_id is not None:
                # 추가 파일 2차 방법(전후 비교)용 기준점 — 재시작 후
                # 복구해도 쓸 수 있게 진행 상태에 함께 남긴다.
                cur['pre_max_id'] = pre_max_id
                _upsert_current(cur)
            # 라이브러리 동시 스캔 수 게이트 — 같은 라이브러리가 스캔
            # 중이거나 상한까지 돌고 있으면 이 섹션은 건너뜀으로 끝낸다
            # (기다리지 않는다). 실패가 아니다 — 실행하지 못한 것이다.
            # 자기 작업의 진행 상태만 빼고 센다.
            gate_msg = ModuleStatus.library_gate_check(
                key, exclude_claim_id=claim_id)
            if gate_msg:
                item.finish('skipped', gate_msg)
                return False
            scan_started = _now_text()
            _write_status(key, 'scan', {
                'job': None, 'path': sec.get('path', ''),
                'started_at': scan_started, 'finished_at': None,
                'status': 'running', 'source': 'periodic',
                'job_name': job_name})

            def _on_scan_job(scan_jobid):
                cur['scan_job'] = scan_jobid
                _upsert_current(cur)
                # shyni 작업 번호를 이력 행에도 남긴다 — 재시작 후
                # 복구가 이 번호로 스캔 완료를 이어서 확인할 수 있다.
                item.shyni_job_id = scan_jobid
                item.save()
                _write_status(key, 'scan', {
                    'job': scan_jobid, 'path': sec.get('path', ''),
                    'started_at': scan_started, 'finished_at': None,
                    'status': 'running', 'source': 'periodic',
                    'job_name': job_name})

            sres = TaskPeriodic._do_scan(sec, claim_id, on_job_id=_on_scan_job)
            scan_duration = sres['duration']
            job_id = sres.get('job_id')
            stats = sres.get('stats')
            relay_total = book_relay_total(stats, key)
            summary = scan_summary_text(stats, relay_total)
            scan_entry = {
                'job': job_id, 'path': sec.get('path', ''),
                'started_at': scan_started, 'finished_at': _now_text(),
                'status': {'done': 'done', 'failed': 'failed',
                           'cancelled': 'cancelled'}.get(sres['status'], 'failed'),
                'source': 'periodic', 'job_name': job_name}
            if summary:
                scan_entry['summary'] = summary
            _write_status(key, 'scan', scan_entry)
            if sres['status'] == 'cancelled':
                apply_scan_result(item, job_id, stats, vfs_duration,
                                  scan_duration, pre_max_id, key,
                                  relay_total=relay_total)
                item.finish('cancelled', sres['message'])
                return False
            if sres['status'] == 'done':
                apply_scan_result(item, job_id, stats, vfs_duration,
                                  scan_duration, pre_max_id, key,
                                  relay_total=relay_total)
                msg = sres['message']
                if summary:
                    msg = f'{msg} ({summary})'
                item.finish('done', msg)
                # 스캔으로 휴지통 구성이 바뀌었을 수 있어 상세 캐시를 지우고
                # 개수 캐시는 '오래됨'으로 표시한다 (현황 수동 스캔과 같은 기준)
                try:
                    from .task_trash_view import clear_cache
                    clear_cache(key)
                except Exception as e:
                    logger.warning(f'[주기적 스캔] 휴지통 상세 캐시 정리 실패: {e}')
                try:
                    from .task_status import mark_trash_counts_stale
                    mark_trash_counts_stale()
                except Exception as e:
                    logger.warning(f'[주기적 스캔] 휴지통 개수 캐시 표시 실패: {e}')
                return True
            apply_scan_result(item, job_id, stats, vfs_duration,
                              scan_duration, pre_max_id, key,
                              relay_total=relay_total)
            item.finish('failed', sres['message'])
            return False
        except Exception as e:
            logger.error(f'주기적 스캔 섹션 처리 실패: {e}')
            logger.error(traceback.format_exc())
            item.vfs_duration = vfs_duration
            item.scan_duration = scan_duration
            item.finish('failed', str(e))
            TaskPeriodic._fail_running_status(key, job_name)
            return False

    @staticmethod
    def _do_vfs(sec, cur, on_job_id=None):
        """VFS 새로고침을 걸고 완료까지 기다린다.
        {'status': done|failed|cancelled, 'message', 'duration': 초,
         'jobid': rclone job id}를 돌려준다. 기한은 설정(VFS 새로고침
        제한 시간, 기본 360분)에서 읽고, 폴링 중 일시적 오류는 원인을
        로그에 남기고 계속 기다린다 (0.9.9h)."""
        t0 = time.time()
        addr, remote = ModuleStatus._rc_target(sec.get('path', ''))
        if not addr:
            raise Exception('경로가 VFS 새로고침 규칙과 맞지 않습니다. 스캔 설정을 확인해 주세요.')
        ret = ModuleStatus._rc(addr, 'vfs/refresh',
                               {'dir': remote, 'recursive': 'true', '_async': 'true'})
        jobid = ret.get('jobid')
        cur['vfs_job'] = jobid
        _upsert_current(cur)
        if on_job_id is not None and jobid:
            try:
                on_job_id(jobid)
            except Exception:
                pass
        deadline = time.time() + _vfs_timeout_sec()
        return TaskPeriodic._poll_vfs_job(addr, jobid, t0, deadline,
                                          claim_id=cur.get('claim_id'),
                                          title=sec.get('title') or '')

    @staticmethod
    def _query_vfs_job(addr, jobid):
        """rclone VFS 새로고침 job 한 번 조회 — 결과 dict를 돌려주고,
        기록이 없으면 'not_found', 그 외 조회 실패면 None을 돌려준다.
        조회 실패 원인은 로그에 남긴다 (0.9.9h)."""
        try:
            return ModuleStatus._rc(addr, 'job/status', {'jobid': jobid})
        except requests.HTTPError as e:
            body = ''
            try:
                body = e.response.text if e.response is not None else ''
            except Exception:
                body = ''
            if 'not found' in body:
                return 'not_found'
            code = e.response.status_code if e.response is not None else '?'
            logger.warning(f'[주기적 스캔] VFS 새로고침 확인 실패 (job {jobid}): HTTP {code}')
            return None
        except Exception as e:
            logger.warning(f'[주기적 스캔] VFS 새로고침 확인 실패 (job {jobid}): {type(e).__name__}: {e}')
            return None

    @staticmethod
    def _poll_vfs_job(addr, jobid, t0, deadline, claim_id=None, title=''):
        """실행 중인 rclone VFS 새로고침 job이 끝날 때까지 기다린다.
        _do_vfs(새로 건 경우)와 VFS 단계 복구(이미 돌고 있는 경우)가 함께 쓴다.
        {'status': done|failed|cancelled, 'message', 'duration': 초,
         'jobid': rclone job id}를 돌려준다. 폴링 중 일시적 오류는 원인을
        로그에 남기고 계속 기다린다 — 확인 실패만으로는 실패로 확정하지
        않는다 (0.9.9h). 기한이 됐을 때 아직 진행 중이면 기한을 연장하고,
        기한 안에 한 번도 확인이 안 됐을 때만 실패로 확정한다."""
        timeout_sec = _vfs_timeout_sec()
        last_ok = None
        while True:
            if time.time() >= deadline:
                # 기한 도달 — job을 한 번 더 조회한다. 끝났으면 그 결과를
                # 쓰고, 아직 돌고 있으면 기한을 연장한다. 확인이 안 되면
                # 기한 내내 확인이 안 됐을 때만 실패로 확정한다 (0.9.9h).
                job = TaskPeriodic._query_vfs_job(addr, jobid)
                if isinstance(job, dict):
                    last_ok = time.time()
                    if job.get('finished'):
                        if job.get('error'):
                            return {'status': 'failed',
                                    'message': f"VFS 새로고침 실패: {job['error']}",
                                    'duration': int(time.time() - t0), 'jobid': jobid}
                        logger.info(f"주기적 스캔: {title} VFS 새로고침 완료 (job {jobid})")
                        return {'status': 'done', 'message': 'VFS 새로고침이 완료됐습니다.',
                                'duration': int(time.time() - t0), 'jobid': jobid}
                    deadline = time.time() + timeout_sec
                    logger.info(
                        f"주기적 스캔: {title} VFS 새로고침이 아직 진행 중이라 기한을 "
                        f"{_fmt_wait(timeout_sec)} 연장합니다 (job {jobid})")
                    continue
                if job == 'not_found':
                    return {'status': 'failed',
                            'message': 'VFS 새로고침 작업 기록을 찾을 수 없습니다.',
                            'duration': int(time.time() - t0), 'jobid': jobid}
                if last_ok is None or time.time() - last_ok >= timeout_sec:
                    return {'status': 'failed',
                            'message': f'VFS 새로고침 확인이 기한({_fmt_wait(timeout_sec)}) 안에 되지 않았습니다.',
                            'duration': int(time.time() - t0), 'jobid': jobid}
                deadline = time.time() + VFS_POLL_SEC * 6
            if _cancel_requested(claim_id):
                try:
                    ModuleStatus._rc(addr, 'job/stop', {'jobid': jobid})
                except Exception as e:
                    logger.warning(f'[주기적 스캔] VFS 새로고침 중단 요청 실패: {e}')
                return {'status': 'cancelled', 'message': '취소 요청으로 중단했습니다.',
                        'duration': int(time.time() - t0), 'jobid': jobid}
            job = TaskPeriodic._query_vfs_job(addr, jobid)
            if job == 'not_found':
                return {'status': 'failed',
                        'message': 'VFS 새로고침 작업 기록을 찾을 수 없습니다.',
                        'duration': int(time.time() - t0), 'jobid': jobid}
            if job is None:
                # 조회 실패 원인은 _query_vfs_job이 로그에 남겼다.
                time.sleep(VFS_POLL_SEC)
                continue
            last_ok = time.time()
            if job.get('finished'):
                if job.get('error'):
                    return {'status': 'failed',
                            'message': f"VFS 새로고침 실패: {job['error']}",
                            'duration': int(time.time() - t0), 'jobid': jobid}
                logger.info(f"주기적 스캔: {title} VFS 새로고침 완료 (job {jobid})")
                return {'status': 'done', 'message': 'VFS 새로고침이 완료됐습니다.',
                        'duration': int(time.time() - t0), 'jobid': jobid}
            time.sleep(VFS_POLL_SEC)

    @staticmethod
    def _do_scan(sec, claim_id=None, on_job_id=None):
        """스캔을 전송하고 완료까지 기다린다.
        {'status': done|failed|cancelled, 'message', 'job_id', 'stats',
         'duration': 초}를 돌려준다. 기한(설정 periodic_scan_timeout_min,
        기본 360분)이 있고, 폴링 중 일시적 오류는 원인을 로그에 남기고
        계속 기다린다 — 확인 실패만으로는 실패로 확정하지 않는다 (0.9.9h).
        job 조회에서 404를 받으면 즉시 실패로 끝낸다 (W3).
        claim_id — 취소 요청을 이 작업 것으로만 확인하기 위한 식별자다."""
        t0 = time.time()
        url, token = ShyniBackend.get_config()
        if not url or not token:
            raise Exception('기본 설정에서 shyni 주소와 토큰을 먼저 설정해 주세요.')
        res = requests.get(f"{url}/library/sections/{sec['key']}/refresh",
                           headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                           timeout=30)
        res.raise_for_status()
        job_id = _extract_job_id(res)
        if not job_id:
            return {'status': 'done',
                    'message': '스캔을 전송했습니다. (작업 ID가 없어 완료 확인은 생략)',
                    'job_id': None, 'stats': None,
                    'duration': int(time.time() - t0)}
        if on_job_id is not None:
            try:
                on_job_id(job_id)
            except Exception:
                pass
        logger.info(f"주기적 스캔: {sec.get('title')} 스캔 전송 (job {job_id})")
        return TaskPeriodic._poll_scan_job(url, token, job_id, t0,
                                           section_key=sec['key'],
                                           claim_id=claim_id)

    @staticmethod
    def _poll_scan_job(url, token, job_id, t0, section_key=None, claim_id=None):
        """compat job 완료까지 폴링한다. _do_scan과 재시작 복구가 함께 쓴다.
        {'status': done|failed|cancelled, 'message', 'job_id', 'stats',
         'duration': 초}를 돌려준다. 기한(설정 periodic_scan_timeout_min,
        기본 360분)이 됐을 때 shyni에서 스캔이 아직 진행 중이면 기한을
        연장하며 계속 기다린다. 폴링 중 일시적 오류는 원인을 로그에 남기고
        계속 기다린다 — 확인 실패만으로는 실패로 확정하지 않는다 (0.9.9h).
        실패 확정은 shyni가 직접 failed라고 답하거나, 작업 기록이 없을 때
        (404), 또는 기한 안에 한 번도 확인이 안 됐을 때만 한다.
        job 조회에서 404를 받으면 즉시 실패로 끝낸다 (W3).
        section_key가 있으면 60초마다 스캔 진행률을 현황에 기록한다
        (진행 표시용 — 완료 판정은 job 폴링이 계속 맡는다).
        claim_id — 취소 요청을 이 작업 것으로만 확인하기 위한 식별자다."""
        timeout_sec = _scan_timeout_sec()
        deadline = time.time() + timeout_sec
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
                    outcome, msg = _scan_poll_outcome(200, job)
                    if outcome == 'done':
                        logger.info(f"주기적 스캔 완료 (job {job_id})")
                        return {'status': 'done', 'message': msg,
                                'job_id': job_id,
                                'stats': job.get('stats') or {},
                                'duration': int(time.time() - t0)}
                    if outcome == 'failed':
                        return {'status': 'failed', 'message': msg,
                                'job_id': job_id,
                                'stats': job.get('stats') or {},
                                'duration': int(time.time() - t0)}
                    if outcome == 'cancelled':
                        return {'status': 'cancelled', 'message': msg,
                                'job_id': job_id,
                                'stats': job.get('stats') or {},
                                'duration': int(time.time() - t0)}
                    deadline = time.time() + timeout_sec
                    logger.info(
                        f"주기적 스캔: shyni에서 아직 진행 중이라 기한을 "
                        f"{_fmt_wait(timeout_sec)} 연장합니다 (job {job_id})")
                    continue
                if job == 'not_found':
                    return {'status': 'failed',
                            'message': '스캔 작업 기록을 찾을 수 없습니다.',
                            'job_id': job_id, 'stats': None,
                            'duration': int(time.time() - t0)}
                # 조회 실패 — 기한 안에 한 번이라도 확인이 됐으면 잠깐
                # 뒤에 다시 확인한다. 기한 내내 확인이 안 됐으면 그때
                # 실패로 확정한다 (0.9.9h).
                if last_ok is None or time.time() - last_ok >= timeout_sec:
                    return {'status': 'failed',
                            'message': f'스캔 확인이 기한({_fmt_wait(timeout_sec)}) 안에 되지 않았습니다. '
                                       'shyni 웹에서 스캔 상태를 확인해 주세요.',
                            'job_id': job_id, 'stats': None,
                            'duration': int(time.time() - t0)}
                deadline = time.time() + SCAN_POLL_SEC * 3
            if _cancel_requested(claim_id):
                # Step5-1 — shyni 스캔도 함께 취소하고 확인한다.
                # job이 이미 끝났으면 그 결과를 그대로 쓴다.
                cres = _cancel_shyni_scan(url, token, job_id, section_key)
                cjob = cres.get('job') or {}
                if cres['reason'] == 'already_done' or (
                        cres['reason'] == 'finished_first'
                        and cjob.get('status') == 'completed'):
                    return {'status': 'done', 'message': '스캔이 완료됐습니다.',
                            'job_id': job_id, 'stats': cjob.get('stats') or {},
                            'duration': int(time.time() - t0)}
                if cres['reason'] == 'already_failed' or (
                        cres['reason'] == 'finished_first'
                        and cjob.get('status') == 'failed'):
                    return {'status': 'failed',
                            'message': f"스캔 실패: {cjob.get('error') or ''}",
                            'job_id': job_id, 'stats': cjob.get('stats') or {},
                            'duration': int(time.time() - t0)}
                return {'status': 'cancelled',
                        'message': CANCEL_RESULT_MESSAGE.get(
                            cres['reason'], '취소 요청으로 중단했습니다.'),
                        'job_id': job_id, 'stats': None,
                        'duration': int(time.time() - t0)}
            try:
                res = requests.get(f'{url}/compat/jobs/{job_id}',
                                   headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                                   timeout=15)
            except Exception as e:
                logger.warning(f'[주기적 스캔] 스캔 확인 실패 (job {job_id}): {type(e).__name__}: {e}')
                time.sleep(SCAN_POLL_SEC)
                continue
            job = None
            if res.status_code == 200:
                try:
                    job = res.json()
                except Exception as e:
                    logger.warning(f'[주기적 스캔] 스캔 확인 응답 파싱 실패 (job {job_id}): {type(e).__name__}: {e}')
                    job = None
            outcome, msg = _scan_poll_outcome(res.status_code, job)
            if outcome == 'not_found':
                return {'status': 'failed', 'message': msg,
                        'job_id': job_id, 'stats': None,
                        'duration': int(time.time() - t0)}
            if outcome == 'done':
                logger.info(f"주기적 스캔 완료 (job {job_id})")
                return {'status': 'done', 'message': msg,
                        'job_id': job_id,
                        'stats': (job or {}).get('stats') or {},
                        'duration': int(time.time() - t0)}
            if outcome == 'failed':
                return {'status': 'failed', 'message': msg,
                        'job_id': job_id,
                        'stats': (job or {}).get('stats') or {},
                        'duration': int(time.time() - t0)}
            if outcome == 'cancelled':
                return {'status': 'cancelled', 'message': msg,
                        'job_id': job_id,
                        'stats': (job or {}).get('stats') or {},
                        'duration': int(time.time() - t0)}
            if res.status_code != 200:
                logger.warning(f'[주기적 스캔] 스캔 확인 실패 (job {job_id}): HTTP {res.status_code}')
            elif isinstance(job, dict):
                last_ok = time.time()
            # 진행률 기록 — 60초마다 (폴링 20초 × 3회).
            poll_count += 1
            if section_key and poll_count % 3 == 0:
                _record_scan_progress(section_key)
            time.sleep(SCAN_POLL_SEC)

    @F.celery.task
    def resume(item_id):
        """재시작 복구 — 스캔 중이던 이력은 완료 확인을 이어서 하고,
        VFS 단계에서 멈춘 이력은 새로고침부터 이어서 한다 (0.9.9g).
        복구가 끝난 뒤에는 같은 작업의 남은 라이브러리부터 이어서
        스캔한다 (0.9.9h) — 복구한 이력이 취소됐을 때는 이어서 하지
        않는다."""
        # 이어서 할 작업 정보 — _resume이 진행 상태를 지우기 전에 미리 읽는다.
        cont = None
        try:
            with F.app.app_context():
                cont = TaskPeriodic._resume_continuation_info(item_id)
        except Exception as e:
            logger.warning(f'[주기적 스캔 복구] 이어서 할 작업 정보 읽기 실패: {e}')
        with F.app.app_context():
            try:
                TaskPeriodic._resume(item_id)
            except Exception as e:
                logger.error(f'주기적 스캔 복구 실패: {e}')
                logger.error(traceback.format_exc())
                try:
                    item = (F.db.session.query(ModelPeriodicItem)
                            .filter(ModelPeriodicItem.id == int(item_id)).first())
                    if item is not None and item.status == 'running':
                        item.finish('failed', f'복구 중 오류가 발생했습니다: {e}')
                except Exception:
                    pass
                _clear_resume_current(item_id)
            # 복구가 끝난 뒤 같은 작업의 남은 라이브러리 이어서
            if cont is not None:
                try:
                    TaskPeriodic._continue_after_resume(cont)
                except Exception as e:
                    logger.error(f'[주기적 스캔 복구] 남은 라이브러리 이어서 하기 실패: {e}')
                    logger.error(traceback.format_exc())
            # 복구 작업이 끝났으면 이 작업의 취소 요청은 지운다 —
            # 남겨 두면 취소 목록에 쌓인다.
            _clear_cancel(f'resume-{item_id}')

    @staticmethod
    def _resume_continuation_info(item_id):
        """복구 뒤 이어서 할 작업 정보 — 이력 행과 복구 진행 상태에서 읽는다.
        이어서 할 수 없으면 None을 돌려준다. _resume이 진행 상태를
        지우기 전에 불러야 한다."""
        item = (F.db.session.query(ModelPeriodicItem)
                .filter(ModelPeriodicItem.id == int(item_id)).first())
        if item is None or item.status != 'running':
            return None
        job_idx = None
        job_started = None
        for cur in get_currents():
            if str(cur.get('resume_item_id')) == str(item_id):
                job_idx = cur.get('job_idx')
                job_started = cur.get('job_started_at')
                break
        if job_started is None and item.start_time:
            job_started = item.start_time.strftime('%Y-%m-%d %H:%M:%S')
        return {
            'item_id': item_id,
            'job_idx': job_idx,
            'job_name': item.job_name or '',
            'section_key': str(item.section_key),
            'mode': item.mode or '',
            'job_started': job_started or _now_text(),
            'claim_id': f'resume-{item_id}',
        }

    @staticmethod
    def _continue_after_resume(cont):
        """복구가 끝난 뒤 같은 작업의 남은 라이브러리부터 이어서 스캔한다
        (0.9.9h). 작업의 섹션 목록에서 복구한 라이브러리 다음부터
        _run_section으로 실행한다. 복구한 이력이 취소됐으면 이어서 하지
        않고, 이어서 하는 도중 취소 요청이 오면 멈춘다."""
        # 복구한 이력의 최종 상태 — 취소됐으면 이어서 하지 않는다.
        item = (F.db.session.query(ModelPeriodicItem)
                .filter(ModelPeriodicItem.id == int(cont['item_id'])).first())
        if item is not None and item.status == 'cancelled':
            logger.info(f"[주기적 스캔 복구] {cont['job_name']}: 복구한 작업이 "
                        '취소돼 남은 라이브러리를 이어서 하지 않습니다.')
            return
        jobs = get_jobs()
        job = None
        job_idx = cont.get('job_idx')
        # 번호로 찾은 작업이 이름까지 맞을 때만 쓴다 — 작업 목록이
        # 바뀌었을 수 있어 번호만 믿지 않는다.
        if isinstance(job_idx, int) and 0 <= job_idx < len(jobs):
            if (jobs[job_idx].get('name') or '') == (cont['job_name'] or ''):
                job = jobs[job_idx]
        if job is None:
            job_idx = None
            for i, j in enumerate(jobs):
                if (j.get('name') or '') == (cont['job_name'] or ''):
                    job = j
                    job_idx = i
                    break
        if job is None:
            logger.info(f"[주기적 스캔 복구] 작업({cont['job_name']})을 찾지 못해 "
                        '남은 라이브러리를 이어서 하지 않습니다.')
            return
        sections = job.get('sections') or []
        keys = [str(k) for k in sections]
        if str(cont['section_key']) not in keys:
            logger.info(f"[주기적 스캔 복구] {cont['job_name']}: 복구한 라이브러리가 "
                        '작업 목록에 없어 남은 라이브러리를 이어서 하지 않습니다.')
            return
        remaining = sections[keys.index(str(cont['section_key'])) + 1:]
        if not remaining:
            return
        sections_info, err = ModuleStatus._sections()
        if err:
            logger.warning(f"[주기적 스캔 복구] {cont['job_name']}: "
                           f'섹션 조회 실패 — {err}')
            return
        sec_map = {str(s['key']): s for s in (sections_info or [])}
        claim_id = cont['claim_id']
        any_scan_done = False
        logger.info(f"[주기적 스캔 복구] {cont['job_name']}: 남은 라이브러리 "
                    f"{len(remaining)}개를 이어서 스캔합니다.")
        for key in remaining:
            if _cancel_requested(claim_id):
                logger.info(f"[주기적 스캔 복구] {cont['job_name']}: 취소 요청으로 "
                            '남은 라이브러리를 멈춥니다.')
                break
            sec = sec_map.get(str(key))
            if sec is None:
                continue
            if TaskPeriodic._run_section(job, job_idx, sec, cont['mode'],
                                         cont['job_started'], None, claim_id):
                any_scan_done = True
            if _cancel_requested(claim_id):
                break
        if any_scan_done:
            try:
                from .task_status import request_stats
                request_stats()
            except Exception as e:
                logger.warning(f'[주기적 스캔 복구] 통계 재계산 요청 실패: {e}')
        logger.info(f"[주기적 스캔 복구] {cont['job_name']}: 남은 라이브러리 이어서 완료")

    @staticmethod
    def _resume(item_id):
        item = (F.db.session.query(ModelPeriodicItem)
                .filter(ModelPeriodicItem.id == int(item_id)).first())
        if item is None or item.status != 'running':
            logger.info(f'주기적 스캔 복구: 이력 {item_id}이 이미 정리돼 있어 건너뜁니다.')
            _clear_resume_current(item_id)
            return
        # 복구 작업이 워커에서 실행되기 시작한 순간 — 이 로그가 없으면
        # 복구 작업이 전송만 되고 실행은 안 된 것이다.
        logger.info(f'[주기적 스캔 복구] {item.section_title or ""} '
                    f'복구 작업 시작 (이력 {item_id})')
        # 복구 작업이 실제로 떴다는 표시 — 진행 상태 단계를 'waiting'에서
        # 실제 단계로 바꾼다. resume_running이 'waiting'으로 남겨 두는
        # 것은 복구 작업이 뜨지 않았을 때 항목이 게이트를 막지 않게
        # 하기 위해서다. 여기서 단계를 바꿔야 이 복구가 게이트에 잡힌다.
        flipped = False
        try:
            _kind, use_vfs = periodic_action_to_kind_use_vfs(item.action)
            real_stage = ('scan' if item.shyni_job_id
                          else ('vfs' if use_vfs else 'scan'))
            for cur in get_currents():
                if str(cur.get('resume_item_id')) == str(item_id):
                    cur['stage'] = real_stage
                    _upsert_current(cur)
                    flipped = True
                    break
        except Exception as e:
            # 전환에 실패하면 여기서 멈춘다 — 항목이 'waiting'인 채로
            # 스캔이 돌면 게이트가 이 복구를 못 잡아 같은 라이브러리
            # 스캔이 겹친다. 다음 재시작 때 다시 복구한다.
            logger.error(f'[주기적 스캔 복구] 진행 상태 단계 전환 실패로 '
                         f'복구를 중단합니다: {e}')
            try:
                item.finish('failed', '진행 상태 표시를 바꾸지 못해 복구를 중단했습니다.')
            except Exception:
                pass
            _clear_resume_current(item_id)
            return
        if not flipped:
            # 항목이 없으면 게이트가 이 복구를 못 잡으니 여기서 멈춘다.
            logger.error(f'[주기적 스캔 복구] 이력 {item_id}의 진행 상태 '
                         f'항목이 없어 복구를 중단합니다.')
            try:
                item.finish('failed', '진행 상태 표시가 없어 복구를 중단했습니다.')
            except Exception:
                pass
            _clear_resume_current(item_id)
            return
        job_id = item.shyni_job_id
        key = item.section_key
        if not job_id:
            # VFS 단계에서 멈춘 이력 — 0.9.9g부터 VFS 새로고침도 복구한다.
            TaskPeriodic._resume_vfs(item, key)
            return
        pre_max_id = None
        for cur in get_currents():
            if str(cur.get('resume_item_id')) == str(item_id):
                pre_max_id = cur.get('pre_max_id')
                break
        url, token = ShyniBackend.get_config()
        if not url or not token:
            item.finish('failed', 'shyni 주소와 토큰을 먼저 설정해 주세요.')
            _clear_resume_current(item_id)
            return
        logger.info(f'주기적 스캔 복구: {item.section_title} '
                    f'스캔(job {job_id}) 완료 확인을 이어서 합니다.')
        # 복구 시작 때 현황 슬롯도 running으로 되돌린다 — 안 그러면
        # _record_scan_progress가 failed 슬롯을 보고 건너뛰어 진행률이
        # 완료 전까지 안 보인다 (2026-11-20 V.A job 20723 실측).
        try:
            st = ModuleStatus._get_state(int(key))
            prev = st.get('scan') or {}
            started = prev.get('started_at')
            if not started and item.start_time:
                started = item.start_time.strftime('%Y-%m-%d %H:%M:%S')
            _write_status(key, 'scan', {
                'job': job_id, 'path': prev.get('path', ''),
                'started_at': started, 'finished_at': None,
                'status': 'running', 'source': 'periodic',
                'job_name': item.job_name or ''})
        except Exception as e:
            logger.warning(f'[주기적 스캔 복구] 현황 상태 기록 실패: {e}')
        res = TaskPeriodic._poll_scan_job(url, token, job_id, time.time(),
                                          section_key=key,
                                          claim_id=f'resume-{item_id}')
        stats = res.get('stats')
        # 스캔 소요는 섹션 시작부터 끝까지에서 VFS 소요를 뺀 값으로 본다
        # (재시작으로 멈췄던 시간도 벽시계 기준으로는 스캔 단계에 든 시간이다).
        scan_duration = res.get('duration')
        try:
            if item.start_time:
                total = int((datetime.now() - item.start_time).total_seconds())
                scan_duration = max(0, total - (item.vfs_duration or 0))
        except Exception:
            pass
        relay_total = book_relay_total(stats, key)
        summary = scan_summary_text(stats, relay_total)
        # 대시보드 스캔 슬롯을 마무리한다 — 시작 시각은 원래 값을 유지한다.
        try:
            st = ModuleStatus._get_state(int(key))
            prev = st.get('scan') or {}
            started = prev.get('started_at')
            if not started and item.start_time:
                started = item.start_time.strftime('%Y-%m-%d %H:%M:%S')
            scan_entry = {
                'job': job_id, 'path': prev.get('path', ''),
                'started_at': started, 'finished_at': _now_text(),
                'status': {'done': 'done', 'failed': 'failed',
                           'cancelled': 'cancelled'}.get(res['status'], 'failed'),
                'source': 'periodic', 'job_name': item.job_name or ''}
            if summary:
                scan_entry['summary'] = summary
            _write_status(key, 'scan', scan_entry)
        except Exception as e:
            logger.warning(f'[주기적 스캔 복구] 현황 상태 기록 실패: {e}')
        if res['status'] == 'cancelled':
            apply_scan_result(item, job_id, stats, item.vfs_duration,
                              scan_duration, pre_max_id, key,
                              label='주기적 스캔 복구', relay_total=relay_total)
            item.finish('cancelled', res['message'])
            _clear_resume_current(item_id)
            return
        if res['status'] == 'done':
            apply_scan_result(item, job_id, stats, item.vfs_duration,
                              scan_duration, pre_max_id, key,
                              label='주기적 스캔 복구', relay_total=relay_total)
            msg = res['message']
            if summary:
                msg = f'{msg} ({summary})'
            item.finish('done', msg)
            try:
                from .task_trash_view import clear_cache
                clear_cache(key)
            except Exception as e:
                logger.warning(f'[주기적 스캔 복구] 휴지통 상세 캐시 정리 실패: {e}')
            try:
                from .task_status import mark_trash_counts_stale
                mark_trash_counts_stale()
            except Exception as e:
                logger.warning(f'[주기적 스캔 복구] 휴지통 개수 캐시 표시 실패: {e}')
            try:
                from .task_status import request_stats
                request_stats()
            except Exception as e:
                logger.warning(f'[주기적 스캔 복구] 통계 재계산 요청 실패: {e}')
            _clear_resume_current(item_id)
            logger.info(f'주기적 스캔 복구 완료: {item.section_title}')
            return
        apply_scan_result(item, job_id, stats, item.vfs_duration,
                          scan_duration, pre_max_id, key,
                          label='주기적 스캔 복구', relay_total=relay_total)
        item.finish('failed', res['message'])
        _clear_resume_current(item_id)

    @staticmethod
    def _resume_vfs(item, key):
        """VFS 단계 복구 — 새로고침 job id로 rclone을 조회해 갈래를 정한다.
        1) job id 없음 → 처음부터 다시 새로고침
        2) job id 있음 → 조회
           2-1) 기록 없음 → 처음부터 다시 새로고침
                (따뜻한 캐시에서 다시 걸면 금방 끝나니까 상관없다)
           2-2) finished=false → 그 job을 이어서 기다린다
           2-3) finished=true → 확인한 시점에 완료된 걸로 기록하고 스캔으로 간다
           (finished=true + error → 새로고침 실패라 처음부터 다시 새로고침)
        새로고침이 끝나면 스캔을 전송하고 완료까지 기다린다 (새로고침만
        동작이면 스캔 없이 끝낸다). 스캔을 보내지 않는 동작이면
        새로고침 단계를 건너뛰고 바로 스캔으로 간다."""
        item_id = item.id
        claim_id = f'resume-{item_id}'
        job_name = item.job_name or ''
        kind, use_vfs = periodic_action_to_kind_use_vfs(item.action)
        vfs_only = kind == 'vfs'
        # 진행 상태가 복구용으로 남아 있는지 확인하고, 없으면 만든다.
        cur = None
        for c in get_currents():
            if str(c.get('resume_item_id')) == str(item_id):
                cur = c
                break
        if cur is None:
            cur = {
                'job_name': job_name,
                'section_key': str(key),
                'section_title': item.section_title or '',
                'stage': 'vfs' if use_vfs else 'scan',
                'mode': item.mode or '',
                'job_started_at': (item.start_time.strftime('%Y-%m-%d %H:%M:%S')
                                   if item.start_time else ''),
                'section_started_at': (item.start_time.strftime('%Y-%m-%d %H:%M:%S')
                                       if item.start_time else ''),
                'resume_item_id': item_id,
                'run_id': _format_run_id(_ff_run_id()[0]),
                'claim_id': claim_id,
                'action': item.action,
            }
            _upsert_current(cur)
        # 라이브러리가 아직 있는지 확인한다 — 없으면 복구할 수 없다.
        sections, err = ModuleStatus._sections()
        if err:
            item.finish('failed', f'라이브러리 목록 조회 실패: {err}')
            _clear_resume_current(item_id)
            return
        sec_map = {str(s['key']): s for s in (sections or [])}
        sec = sec_map.get(str(key))
        if sec is None:
            item.finish('failed', '선택한 라이브러리를 shyni에서 찾지 못했습니다. '
                                  '라이브러리가 삭제됐거나 이름이 바뀌었는지 확인해 주세요.')
            _clear_resume_current(item_id)
            return
        title = sec.get('title') or item.section_title or ''
        vfs_duration = None
        scan_duration = None
        try:
            if use_vfs:
                # 현황 슬롯에서 VFS job id와 시작 시각을 읽는다 — 재시작
                # 후에도 슬롯에 남아 있다 (대시보드 정리도 job id는 지우지 않는다).
                try:
                    st = ModuleStatus._get_state(int(key))
                    vfs_slot = st.get('vfs') or {}
                except Exception:
                    vfs_slot = {}
                jobid = vfs_slot.get('job')
                vfs_started = vfs_slot.get('started_at')
                addr, _remote = ModuleStatus._rc_target(sec.get('path', ''))
                branch = 'restart'
                branch_msg = 'VFS 새로고침 작업 번호가 없어 처음부터 다시 새로고침합니다.'
                if jobid is not None and addr:
                    branch, branch_msg = _vfs_resume_decision(
                        TaskPeriodic._query_vfs_job(addr, jobid))
                logger.info(f'[주기적 스캔 복구] {title} VFS 판정: {branch} — {branch_msg}')
                if branch == 'done':
                    # 확인한 시점에 완료된 걸로 기록하고 스캔으로 간다.
                    vfs_duration = ModuleStatus._slot_seconds(vfs_started, _now_text())
                    _write_status(key, 'vfs', {
                        'job': jobid, 'path': sec.get('path', ''),
                        'started_at': vfs_started, 'finished_at': _now_text(),
                        'status': 'done', 'source': 'periodic',
                        'job_name': job_name})
                    item.vfs_job_id = str(jobid)
                    item.vfs_duration = vfs_duration
                    item.save()
                elif branch == 'poll':
                    # 기한은 원래 시작한 시각 기준으로 잰다 — 복구할 때마다
                    # 새로 주면 제한이 의미가 없어진다.
                    try:
                        started_ts = datetime.strptime(
                            vfs_started or '', '%Y-%m-%d %H:%M:%S').timestamp()
                    except Exception:
                        started_ts = None
                    if started_ts:
                        deadline = started_ts + _vfs_timeout_sec()
                    else:
                        deadline = time.time() + _vfs_timeout_sec()
                    cur['vfs_job'] = jobid
                    _upsert_current(cur)
                    # 이력 행에도 rclone 작업 번호를 남긴다 — 복구 후
                    # 이력 상세에서 새로고침 job id를 볼 수 있다.
                    item.vfs_job_id = str(jobid)
                    item.save()
                    t0 = time.time()
                    vres = TaskPeriodic._poll_vfs_job(
                        addr, jobid, t0, deadline, claim_id=claim_id, title=title)
                    # 소요는 원래 시작부터 끝까지로 본다.
                    if started_ts:
                        vfs_duration = max(0, int(time.time() - started_ts))
                    else:
                        vfs_duration = vres['duration']
                    vfs_status = {'done': 'done', 'failed': 'failed',
                                  'cancelled': 'cancelled'}.get(vres['status'], 'failed')
                    _write_status(key, 'vfs', {
                        'job': jobid, 'path': sec.get('path', ''),
                        'started_at': vfs_started, 'finished_at': _now_text(),
                        'status': vfs_status, 'source': 'periodic',
                        'job_name': job_name})
                    if vres['status'] != 'done':
                        item.vfs_duration = vfs_duration
                        if vres['status'] == 'cancelled':
                            item.finish('cancelled', vres['message'])
                        else:
                            item.finish('failed', vres['message'])
                        _clear_resume_current(item_id)
                        return
                else:  # restart — 처음부터 다시 새로고침
                    vfs_started = _now_text()
                    _write_status(key, 'vfs', {
                        'job': None, 'path': sec.get('path', ''),
                        'started_at': vfs_started, 'finished_at': None,
                        'status': 'running', 'source': 'periodic',
                        'job_name': job_name})

                    def _on_vfs_job(vfs_jobid):
                        _write_status(key, 'vfs', {
                            'job': vfs_jobid, 'path': sec.get('path', ''),
                            'started_at': vfs_started, 'finished_at': None,
                            'status': 'running', 'source': 'periodic',
                            'job_name': job_name})
                        # rclone 작업 번호를 이력 행에도 남긴다 — 이력 상세에서
                        # 새로고침 job id를 볼 수 있다.
                        item.vfs_job_id = str(vfs_jobid)
                        item.save()

                    vres = TaskPeriodic._do_vfs(sec, cur, on_job_id=_on_vfs_job)
                    vfs_duration = vres['duration']
                    vfs_status = {'done': 'done', 'failed': 'failed',
                                  'cancelled': 'cancelled'}.get(vres['status'], 'failed')
                    _write_status(key, 'vfs', {
                        'job': vres.get('jobid'), 'path': sec.get('path', ''),
                        'started_at': vfs_started, 'finished_at': _now_text(),
                        'status': vfs_status, 'source': 'periodic',
                        'job_name': job_name})
                    if vres['status'] != 'done':
                        item.vfs_duration = vfs_duration
                        if vres['status'] == 'cancelled':
                            item.finish('cancelled', vres['message'])
                        else:
                            item.finish('failed', vres['message'])
                        _clear_resume_current(item_id)
                        return
                if vfs_only:
                    item.vfs_duration = vfs_duration
                    item.finish('done', 'VFS 새로고침이 완료됐습니다.')
                    _clear_resume_current(item_id)
                    return
                item.vfs_duration = vfs_duration
                item.save()
                cur['stage'] = 'scan'
                _upsert_current(cur)
            # 스캔 시작 전 MAX(media_parts.id)를 기록해 둔다 —
            # 추가 파일 목록 2차 방법(전후 비교)용 기준점이다.
            pre_max_id = None
            try:
                from . import db_support
                if db_support.db_ready():
                    pre_max_id = db_support.parts_max_id()
            except Exception as e:
                logger.warning(f'[주기적 스캔 복구] 추가 파일 기준점 기록 실패: {e}')
            if pre_max_id is not None:
                cur['pre_max_id'] = pre_max_id
                _upsert_current(cur)
            # 라이브러리 동시 스캔 수 게이트 — 같은 라이브러리가 스캔
            # 중이거나 상한까지 돌고 있으면 이 섹션은 건너뜀으로 끝낸다
            # (기다리지 않는다). 실패가 아니다 — 실행하지 못한 것이다.
            # 자기 작업의 진행 상태만 빼고 센다.
            gate_msg = ModuleStatus.library_gate_check(
                key, exclude_claim_id=claim_id)
            if gate_msg:
                item.finish('skipped', gate_msg)
                _clear_resume_current(item_id)
                return
            scan_started = _now_text()
            _write_status(key, 'scan', {
                'job': None, 'path': sec.get('path', ''),
                'started_at': scan_started, 'finished_at': None,
                'status': 'running', 'source': 'periodic',
                'job_name': job_name})

            def _on_scan_job(scan_jobid):
                cur['scan_job'] = scan_jobid
                _upsert_current(cur)
                # shyni 작업 번호를 이력 행에도 남긴다 — 재시작 후
                # 복구가 이 번호로 스캔 완료를 이어서 확인할 수 있다.
                item.shyni_job_id = scan_jobid
                item.save()
                _write_status(key, 'scan', {
                    'job': scan_jobid, 'path': sec.get('path', ''),
                    'started_at': scan_started, 'finished_at': None,
                    'status': 'running', 'source': 'periodic',
                    'job_name': job_name})

            sres = TaskPeriodic._do_scan(sec, claim_id, on_job_id=_on_scan_job)
            scan_duration = sres['duration']
            job_id = sres.get('job_id')
            stats = sres.get('stats')
            relay_total = book_relay_total(stats, key)
            summary = scan_summary_text(stats, relay_total)
            scan_entry = {
                'job': job_id, 'path': sec.get('path', ''),
                'started_at': scan_started, 'finished_at': _now_text(),
                'status': {'done': 'done', 'failed': 'failed',
                           'cancelled': 'cancelled'}.get(sres['status'], 'failed'),
                'source': 'periodic', 'job_name': job_name}
            if summary:
                scan_entry['summary'] = summary
            _write_status(key, 'scan', scan_entry)
            if sres['status'] == 'cancelled':
                apply_scan_result(item, job_id, stats, vfs_duration,
                                  scan_duration, pre_max_id, key,
                                  relay_total=relay_total)
                item.finish('cancelled', sres['message'])
                _clear_resume_current(item_id)
                return
            if sres['status'] == 'done':
                apply_scan_result(item, job_id, stats, vfs_duration,
                                  scan_duration, pre_max_id, key,
                                  relay_total=relay_total)
                msg = sres['message']
                if summary:
                    msg = f'{msg} ({summary})'
                item.finish('done', msg)
                try:
                    from .task_trash_view import clear_cache
                    clear_cache(key)
                except Exception as e:
                    logger.warning(f'[주기적 스캔 복구] 휴지통 상세 캐시 정리 실패: {e}')
                try:
                    from .task_status import mark_trash_counts_stale
                    mark_trash_counts_stale()
                except Exception as e:
                    logger.warning(f'[주기적 스캔 복구] 휴지통 개수 캐시 표시 실패: {e}')
                try:
                    from .task_status import request_stats
                    request_stats()
                except Exception as e:
                    logger.warning(f'[주기적 스캔 복구] 통계 재계산 요청 실패: {e}')
                _clear_resume_current(item_id)
                logger.info(f'주기적 스캔 복구 완료: {item.section_title}')
                return
            apply_scan_result(item, job_id, stats, vfs_duration,
                              scan_duration, pre_max_id, key,
                              relay_total=relay_total)
            item.finish('failed', sres['message'])
            _clear_resume_current(item_id)
        except Exception as e:
            logger.error(f'주기적 스캔 VFS 복구 실패: {e}')
            logger.error(traceback.format_exc())
            try:
                item.vfs_duration = vfs_duration
                item.scan_duration = scan_duration
            except Exception:
                pass
            item.finish('failed', str(e))
            TaskPeriodic._fail_running_status(key, job_name)
            _clear_resume_current(item_id)

    @staticmethod
    def resume_running(start_celery):
        """플러그인 로드 시 실행 중이던 이력을 찾아 이어서 한다.

        스캔 전송까지 끝난 이력(작업 번호가 이력 행에 있음)은 스캔 완료
        확인을 이어서 하고, VFS 단계에서 멈춘 이력(작업 번호 없음)은
        0.9.9g부터 VFS 새로고침부터 복구한다 — 새로고침 job id로
        rclone을 조회해 이어서 기다리거나 처음부터 다시 건다.
        여러 작업이 함께 돌고 있었다면 그만큼 복구한다.
        running 이력을 확인하고 정리했으면 True, running 이력이
        하나도 없으면 False — False일 때만 플러그인 로드가 나머지
        이력을 중단됨으로 정리하고 진행 상태를 비운다.

        기동 이후에 시작한 이력은 건드리지 않는다 — 웹·워커 두
        프로세스가 플러그인을 따로 로드하는 틈에 방금 시작한 실행을
        중단된 작업으로 오인하면, 새 실행이 게이트에서 자기 복구
        항목에 걸려 실패하고 시스템이 멈춘다 (2026-11-25 실측).
        복구 항목은 'waiting'으로 남겨 두고, 복구 작업이 실제로
        뜰 때 단계를 바꾼다 — 복구 작업이 뜨지 않았을 때 항목이
        게이트를 막는 일을 막기 위해서다."""
        run_id, run_exact = _ff_run_id()
        if _resume_already_done(run_id, run_exact):
            logger.info('[주기적 스캔 복구] 이 기동에서 이미 확인했으니 건너뜁니다.')
            return True
        with F.app.app_context():
            try:
                rows = (F.db.session.query(ModelPeriodicItem)
                        .filter(ModelPeriodicItem.status == 'running')
                        .order_by(ModelPeriodicItem.id.desc()).all())
            except Exception as e:
                logger.error(f'[주기적 스캔 복구] 이력 조회 실패: {e}')
                return False
            if not rows:
                return False
            # 이 기동에서 이미 복구 작업을 보낸 이력 — 진행 상태에
            # 복구 항목이 있고 기동 식별자가 지금 기동과 같다. 다시
            # 보내면 같은 이력을 두 번 이어서 하게 된다. 지난 기동의
            # 항목은 작업이 죽은 것이니 다시 보낸다.
            try:
                old_curs = get_currents()
            except Exception:
                old_curs = []
            launched_ids = set()
            for cur in old_curs:
                if not isinstance(cur, dict):
                    continue
                rid = cur.get('resume_item_id')
                if rid is None:
                    continue
                if _same_run(_parse_run_id(cur.get('run_id')),
                             run_id, run_exact):
                    try:
                        launched_ids.add(int(rid))
                    except Exception:
                        pass
            targets = []
            keep_ids = []
            for row in rows:
                if row.id in launched_ids:
                    logger.info(f"[주기적 스캔 복구] {row.section_title or ''} "
                                f"이력 {row.id}은(는) 이 기동에서 이미 복구 "
                                f"작업을 보냈으니 건너뜁니다.")
                    keep_ids.append(row.id)
                    continue
                if not _is_interrupted_run(row.start_time, run_id):
                    # 기동 이후에 시작한 실행 — 살아 있는 것으로 보고
                    # 건드리지 않는다. 중단됨으로 정리하지도 않는다
                    # (살아 있는 실행의 이력을 망가뜨리면 안 된다).
                    logger.info(f"[주기적 스캔 복구] {row.section_title or ''} "
                                f"이력 {row.id}은(는) 기동 이후에 시작돼 "
                                f"살아 있는 실행으로 보고 건너뜁니다.")
                    keep_ids.append(row.id)
                    continue
                # 같은 라이브러리에 살아 있는 실행의 표시가 있으면
                # 복구하지 않고 건너뜀으로 마감한다 — 복구 항목이
                # 살아 있는 표시를 대신하면 카드가 사라지고, 게이트가
                # 어차피 새 스캔을 막는다.
                if _live_entry_for_section(old_curs, row.section_key,
                                           run_id):
                    logger.info(f"[주기적 스캔 복구] {row.section_title or ''} "
                                f"이력 {row.id}은(는) 같은 라이브러리 스캔이 "
                                f"진행 중이라 건너뜁니다.")
                    try:
                        row.finish('skipped',
                                   '같은 라이브러리 스캔이 진행 중이라 건너뜁니다.')
                    except Exception as e:
                        logger.warning(f'[주기적 스캔 복구] 이력 {row.id} '
                                       f'건너뜀 마감 실패: {e}')
                    continue
                if row.shyni_job_id:
                    targets.append({
                        'item_id': row.id,
                        'job_id': row.shyni_job_id,
                        'job_name': row.job_name or '',
                        'section_key': str(row.section_key),
                        'section_title': row.section_title or '',
                        'mode': row.mode or '',
                        'action': row.action,
                        'section_started': (row.start_time.strftime('%Y-%m-%d %H:%M:%S')
                                            if row.start_time else ''),
                        'stage': 'scan',
                    })
                else:
                    # VFS 단계에서 멈춘 이력 — 스캔을 아직 보내지 않았다.
                    targets.append({
                        'item_id': row.id,
                        'job_id': None,
                        'job_name': row.job_name or '',
                        'section_key': str(row.section_key),
                        'section_title': row.section_title or '',
                        'mode': row.mode or '',
                        'action': row.action,
                        'section_started': (row.start_time.strftime('%Y-%m-%d %H:%M:%S')
                                            if row.start_time else ''),
                        'stage': 'vfs',
                    })
        # 복구 대상이 아닌 running 이력은 중단됨으로 정리한다 —
        # 기동 이후 시작한 이력과 이미 복구 작업을 보낸 이력은 살아 있는
        # 실행이니 함께 뺀다.
        _mark_resume_done(run_id)
        try:
            ModelPeriodicItem.set_terminated(
                exclude_ids=[t['item_id'] for t in targets] + keep_ids)
        except Exception as e:
            logger.error(f'[주기적 스캔 복구] 나머지 이력 정리 실패: {e}')
        # 진행 중 표시 — 복구 항목을 더하고, 복구 대상이 아닌 살아 있는
        # 실행의 표시는 그대로 둔다. 통째로 갈아끼우면 살아 있는 실행의
        # 표시가 사라진다. 기동보다 전에 시작된 표시는 죽은 것으로 보고
        # 버린다. 복구 항목은 'waiting'으로 남겨 두고 복구 작업이 뜰 때
        # 단계를 바꾼다.
        try:
            old_by_key = {str(c.get('section_key')): c for c in old_curs
                          if isinstance(c, dict) and c.get('section_key')}
            entries = []
            for t in targets:
                prev = old_by_key.get(t['section_key']) or {}
                entries.append({
                    'job_idx': prev.get('job_idx'),
                    'job_name': t['job_name'],
                    'section_key': t['section_key'],
                    'section_title': t['section_title'],
                    'stage': 'waiting',
                    'mode': t['mode'],
                    'job_started_at': prev.get('job_started_at') or t['section_started'],
                    'section_started_at': t['section_started'],
                    'scan_job': t['job_id'],
                    'resume_item_id': t['item_id'],
                    'run_id': _format_run_id(run_id),
                    'claim_id': f"resume-{t['item_id']}",
                    'claimed_at': prev.get('claimed_at'),
                    'pre_max_id': prev.get('pre_max_id'),
                    'action': t['action'],
                })
            _save_currents(_merge_resume_currents(old_curs, entries,
                                                  launched_ids, run_id))
        except Exception as e:
            logger.error(f'[주기적 스캔 복구] 진행 상태 기록 실패: {e}')
        for t in targets:
            if t.get('job_id'):
                logger.info(f"[주기적 스캔 복구] {t['section_title']} "
                            f"스캔(job {t['job_id']}) 완료 확인을 이어서 합니다.")
            else:
                logger.info(f"[주기적 스캔 복구] {t['section_title']} "
                            f"VFS 단계부터 이어서 합니다.")
            item_id = t['item_id']
            title = t['section_title']

            def _launch(item_id=item_id, title=title):
                try:
                    # start_celery는 전송 후 작업이 끝날 때까지 기다리므로
                    # (result.get()), 전송 기록은 호출 전에 남긴다.
                    # 호출 뒤에 남기면 작업이 끝난 뒤에야 찍힌다.
                    logger.info(f'[주기적 스캔 복구] 복구 작업 전송: {title} '
                                f'(이력 {item_id})')
                    start_celery(TaskPeriodic.resume, None, *(item_id,))
                except Exception as e:
                    logger.error(f'[주기적 스캔 복구] 복구 작업 시작 실패: {e}')
                    try:
                        with F.app.app_context():
                            item = (F.db.session.query(ModelPeriodicItem)
                                    .filter(ModelPeriodicItem.id == item_id).first())
                            if item is not None and item.status == 'running':
                                item.finish('failed', '복구 작업을 시작하지 못했습니다.')
                        _clear_resume_current(item_id)
                    except Exception:
                        pass
            # daemon으로 둔다 — FF 재시작은 sys.exit으로 끝나는데,
            # non-daemon 스레드가 start_celery의 result.get()에서
            # 기다리고 있으면 재시작이 멈춘다 (워커가 먼저 꺼져서
            # 결과는 영원히 안 옴). 전송은 로그로 확인한다.
            threading.Thread(target=_launch, daemon=True).start()
        return True
