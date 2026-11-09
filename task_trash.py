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
                    else:
                        lines.append(f'{title}: 실패 (응답 코드 {res.status_code})')
                except Exception as e:
                    lines.append(f'{title}: 실패 ({e})')
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
