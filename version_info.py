"""플러그인 버전 읽기 — 프레임워크 의존 없음.

info.json은 패키지 폴더 안에 있고, 버전은 거기서만 읽는다.
RUNNING_VERSION은 이 모듈이 처음 불러와질 때(프로세스 시작 시) 한 번
읽은 값이라, 재시작 전까지는 바뀌지 않는다. 디스크 버전과 다르면
새 파일이 배포됐는데 아직 재시작을 안 한 상태다.

GitHub 최신 버전은 raw.githubusercontent.com에서 info.json을 읽어 온다.
설정 페이지가 열릴 때마다 캐시를 확인하고, 1시간이 지났으면 백그라운드
스레드로 새로 읽어 온다. 페이지 렌더링은 절대 기다리지 않는다.
"""

import json
import os
import threading
import time

try:
    from urllib.request import urlopen
except Exception:  # pragma: no cover
    urlopen = None


GITHUB_INFO_URL = (
    'https://raw.githubusercontent.com/jassmusic/shyni_mate/main/info.json')
GITHUB_CHECK_TTL_SEC = 3600  # 1시간
GITHUB_CHECK_TIMEOUT_SEC = 5
_GITHUB_RETRY_AFTER_FAIL_SEC = 600  # 실패하면 10분 뒤에 다시 시도


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


def _version_tuple(version):
    """'0.9.6' -> (0, 9, 6). 숫자가 아닌 부분은 0으로 본다."""
    parts = []
    for piece in str(version or '').split('.'):
        digits = ''
        for ch in piece:
            if ch.isdigit():
                digits += ch
            else:
                break
        try:
            parts.append(int(digits) if digits else 0)
        except Exception:
            parts.append(0)
    return tuple(parts) or (0,)


def is_newer(candidate, base):
    """candidate 버전이 base보다 높으면 True. 둘 중 하나라도 없으면 False."""
    if not candidate or not base:
        return False
    a = _version_tuple(candidate)
    b = _version_tuple(base)
    width = max(len(a), len(b))
    a = a + (0,) * (width - len(a))
    b = b + (0,) * (width - len(b))
    return a > b


def fetch_github_version():
    """GitHub main의 info.json에서 최신 버전을 읽는다. 실패하면 None."""
    if urlopen is None:
        return None
    try:
        with urlopen(GITHUB_INFO_URL, timeout=GITHUB_CHECK_TIMEOUT_SEC) as res:
            if res.status != 200:
                return None
            data = json.load(res)
        v = (data or {}).get('version')
        return str(v).strip() if v else None
    except Exception:
        return None


_github_lock = threading.Lock()
_github_cache = {'version': None, 'checked_at': 0.0, 'fetching': False}


def _refresh_github_version():
    """GitHub 버전을 동기적으로 읽어 캐시를 채운다. 스레드 안에서만 쓴다."""
    version = fetch_github_version()
    with _github_lock:
        _github_cache['fetching'] = False
        if version:
            _github_cache['version'] = version
            _github_cache['checked_at'] = time.time()
        else:
            # 실패하면 다음 시도를 10분 뒤로 미룬다.
            _github_cache['checked_at'] = (
                time.time() - GITHUB_CHECK_TTL_SEC
                + _GITHUB_RETRY_AFTER_FAIL_SEC)


def get_github_version():
    """캐시된 GitHub 최신 버전을 돌려준다. 캐시가 오래됐으면 백그라운드로
    새로 읽어 온다. 이 함수는 네트워크를 기다리지 않는다."""
    now = time.time()
    with _github_lock:
        stale = (now - _github_cache['checked_at']) >= GITHUB_CHECK_TTL_SEC
        fetching = _github_cache['fetching']
        version = _github_cache['version']
        if stale and not fetching:
            _github_cache['fetching'] = True
            need_fetch = True
        else:
            need_fetch = False
    if need_fetch:
        threading.Thread(target=_refresh_github_version, daemon=True).start()
    return version


def refresh_github_version_async():
    """플러그인 로드 시 캐시를 미리 채워 둔다. 실패해도 조용히 넘어간다."""
    with _github_lock:
        if _github_cache['fetching']:
            return
        _github_cache['fetching'] = True
    threading.Thread(target=_refresh_github_version, daemon=True).start()
