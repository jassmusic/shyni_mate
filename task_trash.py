import json
import os

import requests

from .model_scan import ModelScanItem
from .setup import *
from .shyni_backend import ShyniBackend

logger = P.logger

RUNNING_KEY = 'scan_trash_running'
RESULT_KEY = 'scan_trash_last_result'


def run_empty_trash(mode):
    """선택한 라이브러리의 휴지통을 비운다 (plex_mate 휴지통 스캔과 같은 방식).

    안전장치:
    - 기준 경로(anchors)가 하나라도 실제로 보이지 않으면 전체를 중단한다.
      (마운트가 깨진 상태에서 비우면 멀쩡한 항목까지 지워질 수 있어서다)
    - 주기적 스캔이 도는 라이브러리와, 스캔 항목이 실행 중인 라이브러리는 건너뛴다.
    결과는 ModelSetting scan_trash_last_result에 남긴다.
    """
    with F.app.app_context():
        if (P.ModelSetting.get(RUNNING_KEY) or '') == '1':
            logger.warning('휴지통 비우기: 이미 실행 중이라 시작하지 않습니다.')
            return
        P.ModelSetting.set(RUNNING_KEY, '1')
        lines = []
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            lines.append(f'{now} ({mode}) 휴지통 비우기 시작')

            anchors = [a.strip() for a in (P.ModelSetting.get('scan_trash_schedule_anchors') or '').splitlines() if a.strip()]
            for anchor in anchors:
                if not os.path.exists(anchor):
                    lines.append(f'중단: 기준 경로를 찾을 수 없습니다 — {anchor}')
                    lines.append('마운트 상태를 확인한 뒤 다시 실행해 주세요.')
                    return

            from .mod_status import ModuleStatus
            sections, err = ModuleStatus._sections()
            if err:
                lines.append(f'중단: {err}')
                return
            selected = []
            raw = P.ModelSetting.get('scan_trash_sections')
            if raw:
                try:
                    selected = [str(s) for s in json.loads(raw)]
                except Exception:
                    selected = []

            # 건너뛸 섹션: 주기적 스캔 실행 중 + 스캔 항목 실행 중
            skip = {}
            raw_cur = P.ModelSetting.get('periodic_current')
            if raw_cur:
                try:
                    cur = json.loads(raw_cur)
                    if isinstance(cur, dict) and cur.get('section_key'):
                        skip[str(cur['section_key'])] = '주기적 스캔 실행 중'
                except Exception:
                    pass
            try:
                running = F.db.session.query(ModelScanItem).filter(
                    ModelScanItem.shyni_status == 'RUNNING').all()
                for item in running:
                    if item.section_id:
                        skip[str(item.section_id)] = '스캔 항목 실행 중'
            except Exception:
                pass

            url, token = ShyniBackend.get_config()
            if not url or not token:
                lines.append('중단: 기본 설정에서 shyni 주소와 토큰을 먼저 설정해 주세요.')
                return
            for sec in sections:
                key = str(sec['key'])
                if selected and key not in selected:
                    continue
                title = sec.get('title') or key
                if key in skip:
                    lines.append(f'{title}: 건너뜀 ({skip[key]})')
                    continue
                try:
                    res = requests.get(f'{url}/library/sections/{key}/emptyTrash',
                                       headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                                       timeout=600)
                    if res.status_code == 200:
                        lines.append(f'{title}: 휴지통을 비웠습니다.')
                        try:
                            from .task_trash_view import clear_cache
                            clear_cache(key)
                        except Exception:
                            pass
                    else:
                        lines.append(f'{title}: 실패 (응답 코드 {res.status_code})')
                except Exception as e:
                    lines.append(f'{title}: 실패 ({e})')
            try:
                from .task_status import refresh_trash_counts_async
                refresh_trash_counts_async()
            except Exception:
                pass
            lines.append('완료')
        except Exception as e:
            logger.error(f'휴지통 비우기 실패: {e}')
            logger.error(traceback.format_exc())
            lines.append(f'실패: {e}')
        finally:
            for line in lines:
                logger.info(f'[휴지통] {line}')
            P.ModelSetting.set(RESULT_KEY, '\n'.join(lines))
            P.ModelSetting.set(RUNNING_KEY, '')


# ---- 휴지통 주기 작업 (주기적 스캔과 같은 작업 목록 방식) ----
TRASH_JOBS_KEY = 'trash_jobs'
TRASH_CURRENT_KEY = 'trash_current'
TRASH_CANCEL_KEY = 'trash_cancel'

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


def _trash_cancel_requested():
    return (P.ModelSetting.get(TRASH_CANCEL_KEY) or '') == '1'


def request_trash_cancel():
    P.ModelSetting.set(TRASH_CANCEL_KEY, '1')


def _wait_vfs(addr, jobid, timeout=3600):
    """rclone VFS 새로고침 job 완료를 기다린다. done | failed | cancelled"""
    import time as _time
    deadline = _time.time() + timeout
    while _time.time() < deadline:
        if _trash_cancel_requested():
            return 'cancelled'
        try:
            from .mod_status import ModuleStatus
            job = ModuleStatus._rc(addr, 'job/status', {'jobid': jobid})
            if job.get('finished'):
                return 'failed' if job.get('error') else 'done'
        except Exception:
            pass
        _time.sleep(10)
    return 'failed'


def _wait_scan(url, token, job_id, timeout=6 * 3600):
    """shyni 스캔 job 완료를 기다린다. done | failed | cancelled"""
    import time as _time
    deadline = _time.time() + timeout
    while _time.time() < deadline:
        if _trash_cancel_requested():
            return 'cancelled'
        try:
            res = requests.get(f'{url}/compat/jobs/{job_id}',
                               headers={'X-Plex-Token': token,
                                        'Accept': 'application/json'},
                               timeout=15)
            if res.status_code == 200:
                status = (res.json() or {}).get('status')
                if status == 'completed':
                    return 'done'
                if status == 'failed':
                    return 'failed'
        except Exception:
            pass
        _time.sleep(20)
    return 'failed'


class TaskTrash:
    """휴지통 주기 작업 실행 — 선택한 라이브러리를 하나씩 순차로 처리한다.

    동작(새로고침/스캔/비우기 조합)을 단계별로 실행하고, 단계 완료를 확인한
    뒤 다음 단계로 넘어간다. 진행 상태는 ModelSetting trash_current에 기록해
    화면이 공유하고, 취소는 trash_cancel 플래그로 요청받는다.
    """

    @F.celery.task
    def start(idx, mode):
        with F.app.app_context():
            try:
                TaskTrash._run(idx, mode)
            except Exception as e:
                logger.error(f'휴지통 주기 작업 실행 실패: {e}')
                logger.error(traceback.format_exc())
                _set_trash_current(None)

    @staticmethod
    def _run(idx, mode):
        from .model_trash import ModelTrashItem
        from .mod_status import ModuleStatus
        jobs = get_trash_jobs()
        if idx < 0 or idx >= len(jobs):
            logger.warning(f'[휴지통 작업] 없는 작업 번호: {idx}')
            return
        if get_trash_current() is not None:
            logger.warning('[휴지통 작업] 이미 진행 중인 작업이 있어 시작하지 않습니다.')
            return
        job = jobs[idx]
        name = job.get('name') or f'작업 {idx + 1}'
        action = job.get('action') or 'empty'
        steps = ACTION_STEPS.get(action, ['empty'])
        P.ModelSetting.set(TRASH_CANCEL_KEY, '')
        job_started = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f'[휴지통 작업] 시작: {name} ({ACTION_LABEL.get(action, action)}, {mode})')

        # 안전장치: 기준 경로가 하나라도 보이지 않으면 전체를 중단한다
        anchors = [a.strip() for a in
                   (P.ModelSetting.get('scan_trash_schedule_anchors') or '').splitlines()
                   if a.strip()]
        for anchor in anchors:
            if not os.path.exists(anchor):
                item = ModelTrashItem(name, '', '전체', mode)
                item.finish('failed', f'기준 경로를 찾을 수 없습니다 — {anchor}')
                logger.error(f'[휴지통 작업] 중단: 기준 경로 없음 — {anchor}')
                return

        sections, err = ModuleStatus._sections()
        if err:
            item = ModelTrashItem(name, '', '전체', mode)
            item.finish('failed', err)
            return
        selected = {str(s) for s in (job.get('sections') or [])}
        targets = [s for s in sections if not selected or str(s['key']) in selected]
        if not targets:
            item = ModelTrashItem(name, '', '전체', mode)
            item.finish('failed', '대상 라이브러리가 없습니다.')
            return

        # 건너뛸 섹션: 주기적 스캔 실행 중 + 스캔 항목 실행 중 + 현황 작업 중
        skip = {}
        raw_cur = P.ModelSetting.get('periodic_current')
        if raw_cur:
            try:
                cur = json.loads(raw_cur)
                if isinstance(cur, dict) and cur.get('section_key'):
                    skip[str(cur['section_key'])] = '주기적 스캔 실행 중'
            except Exception:
                pass
        try:
            running = F.db.session.query(ModelScanItem).filter(
                ModelScanItem.shyni_status == 'RUNNING').all()
            for it in running:
                if it.section_id:
                    skip[str(it.section_id)] = '스캔 항목 실행 중'
        except Exception:
            pass

        url, token = ShyniBackend.get_config()
        if not url or not token:
            item = ModelTrashItem(name, '', '전체', mode)
            item.finish('failed', '기본 설정에서 shyni 주소와 토큰을 먼저 설정해 주세요.')
            return

        for sec in targets:
            key = str(sec['key'])
            title = sec.get('title') or key
            if _trash_cancel_requested():
                item = ModelTrashItem(name, key, title, mode)
                item.finish('cancelled', '취소 요청으로 실행하지 않았습니다.')
                continue
            if key in skip:
                item = ModelTrashItem(name, key, title, mode)
                item.finish('cancelled', f'건너뜀 ({skip[key]})')
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
            for step in steps:
                if _trash_cancel_requested():
                    status, message = 'cancelled', '취소 요청으로 중단했습니다.'
                    break
                _set_trash_current({
                    'job_name': name, 'section_key': key, 'section_title': title,
                    'stage': step, 'job_started_at': job_started,
                    'section_started_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'mode': mode})
                try:
                    if step == 'refresh':
                        addr, remote = ModuleStatus._rc_target(sec['path'])
                        if not addr:
                            status, message = 'failed', 'VFS 새로고침 규칙과 맞지 않는 경로입니다.'
                            break
                        ret = ModuleStatus._rc(addr, 'vfs/refresh',
                                               {'dir': remote, 'recursive': 'true',
                                                '_async': 'true'})
                        r = _wait_vfs(addr, ret.get('jobid'))
                        if r != 'done':
                            status = 'cancelled' if r == 'cancelled' else 'failed'
                            message = ('취소 요청으로 중단했습니다.' if r == 'cancelled'
                                       else 'VFS 새로고침이 끝나지 않았습니다.')
                            break
                    elif step == 'scan':
                        res = requests.get(f'{url}/library/sections/{key}/refresh',
                                           headers={'X-Plex-Token': token,
                                                    'Accept': 'application/json'},
                                           timeout=30)
                        res.raise_for_status()
                        job_id = res.headers.get('X-Job-Id')
                        if not job_id:
                            try:
                                body = res.json()
                                if isinstance(body, dict):
                                    job_id = (body.get('job_id') or body.get('jobId')
                                              or body.get('jobID'))
                            except Exception:
                                job_id = None
                        if job_id:
                            r = _wait_scan(url, token, job_id)
                            if r != 'done':
                                status = 'cancelled' if r == 'cancelled' else 'failed'
                                message = ('취소 요청으로 중단했습니다.' if r == 'cancelled'
                                           else '스캔이 완료되지 않았습니다.')
                                break
                    elif step == 'empty':
                        res = requests.get(f'{url}/library/sections/{key}/emptyTrash',
                                           headers={'X-Plex-Token': token,
                                                    'Accept': 'application/json'},
                                           timeout=600)
                        if res.status_code != 200:
                            status, message = 'failed', f'비우기 실패 (응답 코드 {res.status_code})'
                            break
                        # 휴지통 개수가 바뀌므로 현황·건강도 개수 캐시를 새로 읽어 둔다
                        try:
                            from .task_status import refresh_trash_counts_async
                            refresh_trash_counts_async()
                        except Exception:
                            pass
                        # 비운 라이브러리의 상세 캐시 목록은 낡으므로 지운다
                        try:
                            from .task_trash_view import clear_cache
                            clear_cache(key)
                        except Exception:
                            pass
                except Exception as e:
                    status, message = 'failed', f'{STAGE_LABEL.get(step, step)} 단계 오류: {e}'
                    break
            item.finish(status, message)
            logger.info(f'[휴지통 작업] {title}: {status} {message}')
            if status == 'done' and 'scan' in steps:
                # 스캔으로 휴지통 구성이 바뀌었을 수 있어 상세 캐시를 지우고
                # 개수 캐시는 '오래됨'으로 표시해 다음 통계 계산 때 읽게 한다
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
            _set_trash_current(None)
        logger.info(f'[휴지통 작업] 종료: {name}')
