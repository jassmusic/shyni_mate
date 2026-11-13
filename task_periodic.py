import json
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
# 기본 최대 대기 시간(분) — 설정이 없거나 읽지 못하면 이 값을 쓴다.
DEFAULT_MAX_WAIT_MIN = 360
# 가장 최근 시작 실패 — 작업 관리 배너가 읽는다. '지금 실행'을 눌렀는데
# 아무것도 시작되지 않은 경우(진행 중·목록 조회 실패 등)는 이력 행이
# 생기기 전에 끝나므로, 로그에만 남던 사유를 여기에 남긴다.
START_ERROR_KEY = 'periodic_last_start_error'
# 시작 실패 기록 보관 기한(초) — 이보다 오래된 오류는 배너에 띄우지 않는다.
START_ERROR_MAX_AGE_SEC = 3600

STAGE_LABEL = {'vfs': 'VFS 새로고침', 'scan': '스캔'}

# 대기 기한·폴링 실패 기준 (W7): 대시보드·휴지통과 같은 6시간 기한을 두고,
# 폴링 중 일시적 오류(연결 끊김 등)는 재시도하며, 연속 3회 실패 시에만
# 섹션을 실패로 확정한다.
WAIT_TIMEOUT_SEC = 6 * 3600
POLL_FAIL_MAX = 3
VFS_POLL_SEC = 5
SCAN_POLL_SEC = 10
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
    ('done'|'failed'|'not_found'|'wait', 메시지)를 돌려준다.
    404는 기록이 사라진 job이라 기다려 봤자 소용이 없어 즉시 실패다 (W3)."""
    if status_code == 404:
        return 'not_found', '스캔 작업 기록을 찾을 수 없습니다.'
    if status_code != 200:
        return 'wait', ''
    status = (job or {}).get('status')
    if status == 'completed':
        return 'done', '스캔이 완료됐습니다.'
    if status == 'failed':
        return 'failed', f"스캔 실패: {(job or {}).get('error') or ''}"
    return 'wait', ''


def _periodic_skip_decision(trash_parse_ok, trash_section_key, status_busy, key):
    """섹션 시작 시 건너뜀 판정 — 순수 함수(하니스 검증용).
    휴지통 작업 상태를 확인할 수 없으면 fail-closed로 건너뛴다 (W5).
    (건너뜀 사유 | None)을 돌려준다."""
    if not trash_parse_ok:
        return '휴지통 작업 상태를 확인할 수 없음'
    if trash_section_key is not None and str(trash_section_key) == str(key):
        return '휴지통 작업 실행 중'
    if status_busy:
        return '현황에서 작업 중'
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


def _now_text():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def apply_scan_result(item, job_id, stats, vfs_duration, scan_duration,
                      pre_max_id, section_key, label='주기적 스캔',
                      relay_total=None):
    """완료된 스캔의 job stats와 추가 파일 목록을 이력 행에 채운다.
    저장은 호출처의 finish()가 한다. 주기적 스캔·휴지통 작업이 함께 쓴다.
    도서 스캔이면 relay_total(Relay 실측 파일 수)이 있을 때 stat_total을
    그 값으로 덮어쓴다. relay_total을 안 주면 여기서 직접 읽어본다."""
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
        if db_support.db_ready():
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


def get_current():
    raw = P.ModelSetting.get(CURRENT_KEY)
    if raw:
        try:
            cur = json.loads(raw)
            if isinstance(cur, dict):
                return cur
        except Exception:
            pass
    return None


def _set_current(cur):
    if cur is None:
        P.ModelSetting.set(CURRENT_KEY, '')
    else:
        P.ModelSetting.set(CURRENT_KEY, json.dumps(cur, ensure_ascii=False))


def _max_wait_min():
    """최대 대기 시간(분) — 설정값, 없거나 숫자가 아니면 기본 360분.
    음수는 기본값으로 취급한다. 0이면 기다리지 않는다."""
    try:
        val = P.ModelSetting.get_int(MAX_WAIT_KEY)
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
        cur = get_current()
        if cur is not None and (cur.get('job_name') or '') == (name or ''):
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


def _try_claim(idx, name, mode, job_started, job_row_id):
    """차례를 차지한다 — 진행 상태를 이 작업 것으로 쓰고 다시 읽어
    확인한다. 차지했으면 True.
    두 작업이 거의 동시에 차지하려는 틈을 줄이기 위해 쓰고 잠깐 쉰
    뒤에 확인한다. 확인에 실패하면 상대가 차지한 것이니 기다린다."""
    claim_id = (f'wait-{job_row_id}' if job_row_id
                else f'run-{idx}-{int(time.time() * 1000)}')
    _set_current({
        'job_idx': idx, 'job_name': name, 'section_key': '',
        'section_title': '전체', 'stage': 'scan', 'mode': mode,
        'job_started_at': job_started, 'section_started_at': _now_text(),
        'claim_id': claim_id, 'job_row_id': job_row_id})
    time.sleep(random.uniform(0.3, 1.0))
    cur = get_current() or {}
    return cur.get('claim_id') == claim_id


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


def _wait_for_turn(idx, name, mode, job_started):
    """차례를 기다렸다가 차지한다.
    (대기 행 id 또는 None, 기다린 초 또는 None)을 돌려준다.
    바로 시작하면 (None, 0), 기다렸다 시작하면 (행 id, 기다린 초),
    기다리다 시간 초과하면 (None, None) — 이 때는 실패 행을 남긴다.
    최대 대기 시간이 0이면 기다리지 않고 바로 시작 실패로 끝낸다."""
    if get_current() is None and _try_claim(idx, name, mode, job_started, None):
        return (None, 0)
    max_min = _max_wait_min()
    if max_min <= 0:
        fail_start(name, mode, '이미 진행 중인 주기적 스캔이 있어 시작하지 못했습니다.')
        return (None, None)
    running_name = (get_current() or {}).get('job_name') or ''
    if running_name:
        wait_msg = f'앞선 작업({running_name}) 진행 중으로 끝나면 이어서 진행합니다.'
    else:
        wait_msg = '앞선 작업 진행 중으로 끝나면 이어서 진행합니다.'
    item = ModelPeriodicItem(name, '', '전체', mode)
    item.status = 'waiting'
    item.message = wait_msg
    try:
        item.save()
    except Exception as e:
        logger.error(f'[주기적 스캔] 대기 행 저장 실패: {e}')
        fail_start(name, mode, '대기 기록을 남기지 못했습니다.')
        return (None, None)
    row_id = item.id
    t0 = time.time()
    deadline = t0 + max_min * 60
    while time.time() < deadline:
        time.sleep(WAIT_POLL_SEC)
        if get_current() is None and _try_claim(idx, name, mode, job_started, row_id):
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
            return (row_id, waited)
    _finish_job_row(row_id, 'failed',
                    f'앞선 작업이 끝나기를 기다리다 시간 초과됐습니다. (최대 대기 {_fmt_wait(max_min * 60)})')
    logger.warning(f'[주기적 스캔] {name}: 최대 대기 시간 초과로 시작하지 않습니다.')
    return (None, None)


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


def fail_start(job_name, mode, reason):
    """작업 단위 시작 실패 — 이력 행을 남기고 배너용 오류를 기록한다.
    이력 행 저장이 실패해도 배너 기록은 시도한다."""
    try:
        item = ModelPeriodicItem(job_name, '', '전체', mode)
        item.finish('failed', reason)
    except Exception as e:
        logger.error(f'[주기적 스캔] 시작 실패 이력 기록 실패: {e}')
    _record_start_error(job_name, reason)


def _cancel_requested():
    return (P.ModelSetting.get(CANCEL_KEY) or '') == '1'


def _clear_resume_current(item_id):
    """복구용 진행 상태면 지운다 — 새 실행의 진행 상태를 건드리지 않는다."""
    try:
        cur = get_current()
        if isinstance(cur, dict) and str(cur.get('resume_item_id')) == str(item_id):
            _set_current(None)
    except Exception:
        pass


def request_cancel():
    P.ModelSetting.set(CANCEL_KEY, '1')


class TaskPeriodic:
    """주기적 스캔 실행 — 선택한 섹션을 하나씩 순차로 처리한다.

    단계: (옵션) VFS 새로고침 → 완료 확인 → 스캔 전송 → 완료 확인.
    진행 상태는 ModelSetting periodic_current에 기록해 현황/주기 화면이 공유한다.
    취소는 periodic_cancel 플래그로 요청받고, 단계 폴링 중 확인해 멈춘다.
    완료된 스캔의 집계는 이력 행에 남기고, 진행 상황은 대시보드 섹션 상태
    (status_sec_N)에도 수동 작업과 같은 모양으로 기록한다.
    """

    @F.celery.task
    def start(idx, mode):
        with F.app.app_context():
            try:
                TaskPeriodic._run(idx, mode)
            except Exception as e:
                logger.error(f'주기적 스캔 실행 실패: {e}')
                logger.error(traceback.format_exc())
                try:
                    row_id = (get_current() or {}).get('job_row_id')
                except Exception:
                    row_id = None
                _set_current(None)
                if row_id:
                    # 기다렸다 시작한 작업이면 대기 행을 실패로 끝낸다 —
                    # fail_start로 또 행을 만들면 두 개가 된다.
                    _finish_job_row(row_id, 'failed', f'실행 중 오류가 발생했습니다: {e}')
                    return
                try:
                    jobs = get_jobs()
                    job = jobs[idx] if 0 <= idx < len(jobs) else None
                except Exception:
                    job = None
                if job is not None:
                    fail_start(job.get('name') or f'작업 {idx + 1}',
                               mode, f'실행 중 오류가 발생했습니다: {e}')

    @staticmethod
    def _run(idx, mode):
        jobs = get_jobs()
        if idx < 0 or idx >= len(jobs):
            logger.error(f'주기적 스캔: 잘못된 작업 번호 {idx}')
            _record_start_error('', f'잘못된 작업 번호라 시작하지 못했습니다: {idx}')
            return
        job = jobs[idx]
        job_name = job.get('name') or ''
        # 같은 작업이 이미 돌거나 기다리고 있으면 중복으로 세지 않는다.
        if _same_job_busy(job_name):
            logger.info(f'주기적 스캔: {job_name} — 이미 실행 중이거나 대기 중이라 건너뜁니다.')
            return
        P.ModelSetting.set(CANCEL_KEY, '')
        sections, err = ModuleStatus._sections()
        if err:
            logger.error(f'주기적 스캔: 섹션 조회 실패 — {err}')
            fail_start(job_name, mode, err)
            return
        sec_map = {str(s['key']): s for s in sections}
        if not (job.get('sections') or []):
            fail_start(job_name, mode, '선택한 라이브러리가 없어 시작하지 못했습니다.')
            return
        if not any(str(k) in sec_map for k in (job.get('sections') or [])):
            fail_start(job_name, mode,
                       '선택한 라이브러리를 shyni에서 찾지 못했습니다. '
                       '라이브러리가 삭제됐거나 이름이 바뀌었는지 확인해 주세요.')
            return
        _clear_start_error()
        any_scan_done = False
        job_started = _now_text()
        logger.info(f"주기적 스캔 시작: {job.get('name')} ({mode})")
        # 앞선 작업이 돌고 있으면 기다렸다가 차례가 되면 실행한다.
        # 기다리다 시간 초과하면 _wait_for_turn이 실패 행을 남긴다.
        job_row_id, _waited = _wait_for_turn(idx, job_name, mode, job_started)
        if _waited is None:
            return
        for key in job.get('sections') or []:
            if _cancel_requested():
                logger.info('주기적 스캔: 취소 요청으로 중단합니다.')
                break
            sec = sec_map.get(str(key))
            if sec is None:
                continue
            if TaskPeriodic._run_section(job, idx, sec, mode, job_started, job_row_id):
                any_scan_done = True
            if _cancel_requested():
                break
        if job_row_id is not None:
            if _cancel_requested():
                _finish_job_row(job_row_id, 'cancelled', '취소 요청으로 중단했습니다.')
            else:
                _finish_job_row(job_row_id, 'done', '완료됐습니다.')
        _set_current(None)
        P.ModelSetting.set(CANCEL_KEY, '')
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
        휴지통 작업 중인 섹션은 건너뛰고, 가드 조회에 실패하면
        fail-closed로 건너뛴다 (W5). 섹션 시작 시마다 다시 읽는다."""
        trash_parse_ok = True
        trash_section_key = None
        raw_cur = P.ModelSetting.get('trash_current')
        if raw_cur:
            try:
                cur = json.loads(raw_cur)
                if isinstance(cur, dict):
                    trash_section_key = cur.get('section_key')
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
        return _periodic_skip_decision(trash_parse_ok, trash_section_key,
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
    def _run_section(job, idx, sec, mode, job_started, job_row_id=None):
        key = sec['key']
        job_name = job.get('name') or ''
        title = sec.get('title') or ''
        skip_reason = TaskPeriodic._section_skip_reason(key)
        if skip_reason is not None:
            item = ModelPeriodicItem(job_name, key, title, mode)
            item.finish('cancelled', f'건너뜀 ({skip_reason})')
            logger.info(f'주기적 스캔: {title} 건너뜀 ({skip_reason})')
            return False
        item = ModelPeriodicItem(job_name, key, title, mode)
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
        }
        _set_current(cur)
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
                _set_current(cur)
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
                _set_current(cur)
            scan_started = _now_text()
            _write_status(key, 'scan', {
                'job': None, 'path': sec.get('path', ''),
                'started_at': scan_started, 'finished_at': None,
                'status': 'running', 'source': 'periodic',
                'job_name': job_name})

            def _on_scan_job(scan_jobid):
                cur['scan_job'] = scan_jobid
                _set_current(cur)
                # shyni 작업 번호를 이력 행에도 남긴다 — 재시작 후
                # 복구가 이 번호로 스캔 완료를 이어서 확인할 수 있다.
                item.shyni_job_id = scan_jobid
                item.save()
                _write_status(key, 'scan', {
                    'job': scan_jobid, 'path': sec.get('path', ''),
                    'started_at': scan_started, 'finished_at': None,
                    'status': 'running', 'source': 'periodic',
                    'job_name': job_name})

            sres = TaskPeriodic._do_scan(sec, on_job_id=_on_scan_job)
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
         'jobid': rclone job id}를 돌려준다. 6시간 기한이 있고, 폴링 중
        일시적 오류는 재시도하며 연속 3회 실패 시에만 실패로 확정한다 (W7)."""
        t0 = time.time()
        addr, remote = ModuleStatus._rc_target(sec.get('path', ''))
        if not addr:
            raise Exception('경로가 VFS 새로고침 규칙과 맞지 않습니다. 스캔 설정을 확인해 주세요.')
        ret = ModuleStatus._rc(addr, 'vfs/refresh',
                               {'dir': remote, 'recursive': 'true', '_async': 'true'})
        jobid = ret.get('jobid')
        cur['vfs_job'] = jobid
        _set_current(cur)
        if on_job_id is not None and jobid:
            try:
                on_job_id(jobid)
            except Exception:
                pass
        deadline = time.time() + WAIT_TIMEOUT_SEC
        fail_count = 0
        while time.time() < deadline:
            if _cancel_requested():
                try:
                    ModuleStatus._rc(addr, 'job/stop', {'jobid': jobid})
                except Exception as e:
                    logger.warning(f'[주기적 스캔] VFS 새로고침 중단 요청 실패: {e}')
                return {'status': 'cancelled', 'message': '취소 요청으로 중단했습니다.',
                        'duration': int(time.time() - t0), 'jobid': jobid}
            try:
                job = ModuleStatus._rc(addr, 'job/status', {'jobid': jobid})
            except requests.HTTPError as e:
                body = ''
                try:
                    body = e.response.text if e.response is not None else ''
                except Exception:
                    body = ''
                if 'not found' in body:
                    return {'status': 'failed',
                            'message': 'VFS 새로고침 작업 기록을 찾을 수 없습니다.',
                            'duration': int(time.time() - t0), 'jobid': jobid}
                fail_count += 1
                if fail_count >= POLL_FAIL_MAX:
                    return {'status': 'failed',
                            'message': f'VFS 새로고침 확인이 연속 {POLL_FAIL_MAX}회 실패했습니다.',
                            'duration': int(time.time() - t0), 'jobid': jobid}
                time.sleep(VFS_POLL_SEC)
                continue
            except Exception:
                fail_count += 1
                if fail_count >= POLL_FAIL_MAX:
                    return {'status': 'failed',
                            'message': f'VFS 새로고침 확인이 연속 {POLL_FAIL_MAX}회 실패했습니다.',
                            'duration': int(time.time() - t0), 'jobid': jobid}
                time.sleep(VFS_POLL_SEC)
                continue
            fail_count = 0
            if job.get('finished'):
                if job.get('error'):
                    return {'status': 'failed',
                            'message': f"VFS 새로고침 실패: {job['error']}",
                            'duration': int(time.time() - t0), 'jobid': jobid}
                logger.info(f"주기적 스캔: {sec.get('title')} VFS 새로고침 완료 (job {jobid})")
                return {'status': 'done', 'message': 'VFS 새로고침이 완료됐습니다.',
                        'duration': int(time.time() - t0), 'jobid': jobid}
            time.sleep(VFS_POLL_SEC)
        return {'status': 'failed',
                'message': 'VFS 새로고침이 기한 안에 끝나지 않았습니다.',
                'duration': int(time.time() - t0), 'jobid': jobid}

    @staticmethod
    def _do_scan(sec, on_job_id=None):
        """스캔을 전송하고 완료까지 기다린다.
        {'status': done|failed|cancelled, 'message', 'job_id', 'stats',
         'duration': 초}를 돌려준다. 6시간 기한이 있고, 폴링 중 일시적
        오류는 재시도하며 연속 3회 실패 시에만 실패로 확정한다 (W7).
        job 조회에서 404를 받으면 즉시 실패로 끝낸다 (W3)."""
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
        return TaskPeriodic._poll_scan_job(url, token, job_id, t0)

    @staticmethod
    def _poll_scan_job(url, token, job_id, t0):
        """compat job 완료까지 폴링한다. _do_scan과 재시작 복구가 함께 쓴다.
        {'status': done|failed|cancelled, 'message', 'job_id', 'stats',
         'duration': 초}를 돌려준다. 6시간 기한이 있고, 폴링 중 일시적
        오류는 재시도하며 연속 3회 실패 시에만 실패로 확정한다 (W7).
        job 조회에서 404를 받으면 즉시 실패로 끝낸다 (W3)."""
        deadline = time.time() + WAIT_TIMEOUT_SEC
        fail_count = 0
        while time.time() < deadline:
            if _cancel_requested():
                return {'status': 'cancelled',
                        'message': '취소 요청으로 대기를 중단했습니다. shyni에서 실행 중인 스캔은 계속될 수 있습니다.',
                        'job_id': job_id, 'stats': None,
                        'duration': int(time.time() - t0)}
            try:
                res = requests.get(f'{url}/compat/jobs/{job_id}',
                                   headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                                   timeout=15)
            except Exception:
                fail_count += 1
                if fail_count >= POLL_FAIL_MAX:
                    return {'status': 'failed',
                            'message': f'스캔 확인이 연속 {POLL_FAIL_MAX}회 실패했습니다.',
                            'job_id': job_id, 'stats': None,
                            'duration': int(time.time() - t0)}
                time.sleep(SCAN_POLL_SEC)
                continue
            job = None
            if res.status_code == 200:
                try:
                    job = res.json()
                except Exception:
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
            if res.status_code != 200:
                fail_count += 1
                if fail_count >= POLL_FAIL_MAX:
                    return {'status': 'failed',
                            'message': f'스캔 확인이 연속 {POLL_FAIL_MAX}회 실패했습니다.',
                            'job_id': job_id, 'stats': None,
                            'duration': int(time.time() - t0)}
            else:
                fail_count = 0
            time.sleep(SCAN_POLL_SEC)
        return {'status': 'failed',
                'message': '스캔이 기한 안에 끝나지 않았습니다.',
                'job_id': job_id, 'stats': None,
                'duration': int(time.time() - t0)}

    @F.celery.task
    def resume(item_id):
        """재시작 복구 — 스캔 중이던 이력의 완료 확인을 이어서 한다."""
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

    @staticmethod
    def _resume(item_id):
        item = (F.db.session.query(ModelPeriodicItem)
                .filter(ModelPeriodicItem.id == int(item_id)).first())
        if item is None or item.status != 'running':
            logger.info(f'주기적 스캔 복구: 이력 {item_id}이 이미 정리돼 있어 건너뜁니다.')
            _clear_resume_current(item_id)
            return
        job_id = item.shyni_job_id
        key = item.section_key
        if not job_id:
            item.finish('failed', '스캔 작업 번호가 없어 복구할 수 없습니다.')
            _clear_resume_current(item_id)
            return
        cur = get_current() or {}
        pre_max_id = cur.get('pre_max_id')
        url, token = ShyniBackend.get_config()
        if not url or not token:
            item.finish('failed', 'shyni 주소와 토큰을 먼저 설정해 주세요.')
            _clear_resume_current(item_id)
            return
        logger.info(f'주기적 스캔 복구: {item.section_title} '
                    f'스캔(job {job_id}) 완료 확인을 이어서 합니다.')
        res = TaskPeriodic._poll_scan_job(url, token, job_id, time.time())
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
    def resume_running(start_celery):
        """플러그인 로드 시 스캔 중이던 이력을 찾아 완료 확인을 이어서 한다.

        스캔 전송까지 끝난 이력(작업 번호가 이력 행에 있음)만 복구한다.
        VFS 단계에서 멈춘 이력은 복구하지 않는다 — 다음 실행 때 VFS부터
        다시 하면 된다. 복구를 시작했으면 True, 복구할 게 없으면 False."""
        with F.app.app_context():
            try:
                rows = (F.db.session.query(ModelPeriodicItem)
                        .filter(ModelPeriodicItem.status == 'running')
                        .order_by(ModelPeriodicItem.id.desc()).all())
            except Exception as e:
                logger.error(f'[주기적 스캔 복구] 이력 조회 실패: {e}')
                return False
            target = None
            for row in rows:
                if row.shyni_job_id:
                    target = row
                    break
            if target is None:
                return False
            # 세션이 닫히기 전에 필요한 값을 평범한 값으로 뽑아 둔다
            item_id = target.id
            job_id = target.shyni_job_id
            job_name = target.job_name or ''
            section_key = str(target.section_key)
            section_title = target.section_title or ''
            mode = target.mode or ''
            section_started = (target.start_time.strftime('%Y-%m-%d %H:%M:%S')
                               if target.start_time else '')
        # 복구 대상이 아닌 running 이력은 중단됨으로 정리한다
        try:
            ModelPeriodicItem.set_terminated(exclude_id=item_id)
        except Exception as e:
            logger.error(f'[주기적 스캔 복구] 나머지 이력 정리 실패: {e}')
        # 진행 중 표시를 복구 상태로 유지한다 — 새 실행을 막고
        # 대시보드·작업 관리 화면이 진행 중임을 알 수 있게 한다.
        try:
            cur = get_current() or {}
            cur.update({
                'job_name': job_name,
                'section_key': section_key,
                'section_title': section_title,
                'stage': 'scan',
                'mode': mode,
                'section_started_at': section_started,
                'scan_job': job_id,
                'resume_item_id': item_id,
            })
            _set_current(cur)
        except Exception as e:
            logger.error(f'[주기적 스캔 복구] 진행 상태 기록 실패: {e}')
        logger.info(f'[주기적 스캔 복구] {section_title} '
                    f'스캔(job {job_id}) 완료 확인을 이어서 합니다.')
        def _launch():
            try:
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
        threading.Thread(target=_launch, daemon=True).start()
        return True
