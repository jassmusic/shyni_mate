"""플러그인 버전 읽기 — 프레임워크 의존 없음.

info.json은 패키지 폴더 안에 있고, 버전은 거기서만 읽는다.
RUNNING_VERSION은 이 모듈이 처음 불러와질 때(프로세스 시작 시) 한 번
읽은 값이라, 재시작 전까지는 바뀌지 않는다. 디스크 버전과 다르면
새 파일이 배포됐는데 아직 재시작을 안 한 상태다.
"""

import json
import os


def read_disk_version():
    """디스크의 info.json에서 버전을 읽는다. 못 읽으면 None."""
    try:
        path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'info.json')
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        v = (data or {}).get('version')
        return str(v).strip() if v else None
    except Exception:
        return None


RUNNING_VERSION = read_disk_version()
