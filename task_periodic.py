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
        job_started = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
            except Exception:
                pass
        logger.info(f"주기적 스캔 종료: {job.get('name')}")

    @staticmethod
    def _run_section(job, idx, sec, mode, job_started):
        key = sec['key']
        item = ModelPeriodicItem(job.get('name') or '', key, sec.get('title') or '', mode)
        item.save()
        vfs_only = job.get('kind') == 'vfs'
        cur = {
            'job_idx': idx,
            'job_name': job.get('name') or '',
            'section_key': str(key),
            'section_title': sec.get('title') or '',
            'stage': 'vfs' if (vfs_only or job.get('use_vfs')) else 'scan',
            'mode': mode,
            'job_started_at': job_started,
            'section_started_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        _set_current(cur)
        logger.info(f"주기적 스캔: {sec.get('title')} 시작 (종류: {'VFS 새로고침만' if vfs_only else '스캔'})")
        try:
            if vfs_only:
                if not TaskPeriodic._do_vfs(sec, cur):
                    item.finish('cancelled', '취소 요청으로 중단했습니다.')
                    return
                item.finish('done', 'VFS 새로고침이 완료됐습니다.')
                return
            if job.get('use_vfs'):
                if not TaskPeriodic._do_vfs(sec, cur):
                    item.finish('cancelled', '취소 요청으로 VFS 새로고침 단계에서 중단했습니다.')
                    return
                cur['stage'] = 'scan'
                _set_current(cur)
            done, msg = TaskPeriodic._do_scan(sec)
            if done is None:
                item.finish('cancelled', msg)
            elif done:
                item.finish('done', msg)
                # 스캔으로 휴지통 구성이 바뀌었을 수 있어 상세 캐시를 지우고
                # 개수 캐시는 '오래됨'으로 표시한다 (현황 수동 스캔과 같은 기준)
                try:
                    from .task_trash_view import clear_cache
                    clear_cache(key)
                except Exception:
                    pass
                try:
                    from .task_status import mark_trash_counts_stale
                    mark_trash_counts_stale()
                except Exception:
                    pass
                return True
            else:
                item.finish('failed', msg)
        except Exception as e:
            logger.error(f'주기적 스캔 섹션 처리 실패: {e}')
            logger.error(traceback.format_exc())
            item.finish('failed', str(e))

    @staticmethod
    def _do_vfs(sec, cur):
        """VFS 새로고침을 걸고 완료까지 기다린다. 취소되면 False."""
        addr, remote = ModuleStatus._rc_target(sec.get('path', ''))
        if not addr:
            raise Exception('경로가 VFS 새로고침 규칙과 맞지 않습니다. 스캔 설정을 확인해 주세요.')
        ret = ModuleStatus._rc(addr, 'vfs/refresh',
                               {'dir': remote, 'recursive': 'true', '_async': 'true'})
        jobid = ret.get('jobid')
        cur['vfs_job'] = jobid
        _set_current(cur)
        while True:
            if _cancel_requested():
                try:
                    ModuleStatus._rc(addr, 'job/stop', {'jobid': jobid})
                except Exception:
                    pass
                return False
            job = ModuleStatus._rc(addr, 'job/status', {'jobid': jobid})
            if job.get('finished'):
                if job.get('error'):
                    raise Exception(f"VFS 새로고침 실패: {job['error']}")
                logger.info(f"주기적 스캔: {sec.get('title')} VFS 새로고침 완료 (job {jobid})")
                return True
            time.sleep(5)

    @staticmethod
    def _do_scan(sec):
        """스캔을 전송하고 완료까지 기다린다. (완료여부, 메시지) — 취소 시 (None, msg)."""
        url, token = ShyniBackend.get_config()
        if not url or not token:
            raise Exception('기본 설정에서 shyni 주소와 토큰을 먼저 설정해 주세요.')
        res = requests.get(f"{url}/library/sections/{sec['key']}/refresh",
                           headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                           timeout=30)
        res.raise_for_status()
        job_id = res.headers.get('X-Job-Id')
        if not job_id:
            try:
                body = res.json()
                if isinstance(body, dict):
                    job_id = body.get('job_id') or body.get('jobId') or body.get('jobID')
            except Exception:
                job_id = None
        if not job_id:
            return True, '스캔을 전송했습니다. (작업 ID가 없어 완료 확인은 생략)'
        logger.info(f"주기적 스캔: {sec.get('title')} 스캔 전송 (job {job_id})")
        while True:
            if _cancel_requested():
                return None, '취소 요청으로 대기를 중단했습니다. shyni에서 실행 중인 스캔은 계속될 수 있습니다.'
            res = requests.get(f'{url}/compat/jobs/{job_id}',
                               headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                               timeout=15)
            if res.status_code == 200:
                job = res.json()
                if job.get('status') == 'completed':
                    logger.info(f"주기적 스캔: {sec.get('title')} 스캔 완료 (job {job_id})")
                    return True, '스캔이 완료됐습니다.'
                if job.get('status') == 'failed':
                    return False, f"스캔 실패: {job.get('error') or ''}"
            time.sleep(10)
