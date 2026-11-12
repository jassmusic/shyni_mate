import json
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
                      pre_max_id, section_key, label='주기적 스캔'):
    """완료된 스캔의 job stats와 추가 파일 목록을 이력 행에 채운다.
    저장은 호출처의 finish()가 한다. 주기적 스캔·휴지통 작업이 함께 쓴다."""
    item.shyni_job_id = job_id
    item.vfs_duration = vfs_duration
    item.scan_duration = scan_duration
    if not isinstance(stats, dict):
        return
    item.stat_total = _stat_int(stats.get('total'))
    item.stat_new = _stat_int(stats.get('new'))
    item.stat_missing = _stat_int(stats.get('missing'))
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
    except Exception as e:
        logger.warning(f'[{label}] 추가 파일 목록 수집 실패: {e}')


def scan_summary_text(stats):
    """대시보드 최근 작업 줄에 붙일 요약 — '신규 N · 전체 N'."""
    if not isinstance(stats, dict):
        return None
    new = _stat_int(stats.get('new'))
    total = _stat_int(stats.get('total'))
    if new is None and total is None:
        return None
    parts = []
    if new is not None:
        parts.append(f'신규 {new}')
    if total is not None:
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


def _cancel_requested():
    return (P.ModelSetting.get(CANCEL_KEY) or '') == '1'


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
                _set_current(None)

    @staticmethod
    def _run(idx, mode):
        jobs = get_jobs()
        if idx < 0 or idx >= len(jobs):
            logger.error(f'주기적 스캔: 잘못된 작업 번호 {idx}')
            return
        job = jobs[idx]
        if get_current() is not None:
            logger.warning('주기적 스캔: 이미 진행 중인 작업이 있어 시작하지 않습니다.')
            return
        P.ModelSetting.set(CANCEL_KEY, '')
        sections, err = ModuleStatus._sections()
        if err:
            logger.error(f'주기적 스캔: 섹션 조회 실패 — {err}')
            return
        sec_map = {str(s['key']): s for s in sections}
        any_scan_done = False
        job_started = _now_text()
        logger.info(f"주기적 스캔 시작: {job.get('name')} ({mode})")
        for key in job.get('sections') or []:
            if _cancel_requested():
                logger.info('주기적 스캔: 취소 요청으로 중단합니다.')
                break
            sec = sec_map.get(str(key))
            if sec is None:
                continue
            if TaskPeriodic._run_section(job, idx, sec, mode, job_started):
                any_scan_done = True
            if _cancel_requested():
                break
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
    def _run_section(job, idx, sec, mode, job_started):
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
            scan_started = _now_text()
            _write_status(key, 'scan', {
                'job': None, 'path': sec.get('path', ''),
                'started_at': scan_started, 'finished_at': None,
                'status': 'running', 'source': 'periodic',
                'job_name': job_name})

            def _on_scan_job(scan_jobid):
                cur['scan_job'] = scan_jobid
                _set_current(cur)
                _write_status(key, 'scan', {
                    'job': scan_jobid, 'path': sec.get('path', ''),
                    'started_at': scan_started, 'finished_at': None,
                    'status': 'running', 'source': 'periodic',
                    'job_name': job_name})

            sres = TaskPeriodic._do_scan(sec, on_job_id=_on_scan_job)
            scan_duration = sres['duration']
            job_id = sres.get('job_id')
            stats = sres.get('stats')
            summary = scan_summary_text(stats)
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
                                  scan_duration, pre_max_id, key)
                item.finish('cancelled', sres['message'])
                return False
            if sres['status'] == 'done':
                apply_scan_result(item, job_id, stats, vfs_duration,
                                  scan_duration, pre_max_id, key)
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
                              scan_duration, pre_max_id, key)
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
                logger.info(f"주기적 스캔: {sec.get('title')} 스캔 완료 (job {job_id})")
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
