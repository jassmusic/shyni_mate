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
import logging
import os
import threading
import time

_log = logging.getLogger(__name__)

try:
    from urllib.request import Request, urlopen
except Exception:  # pragma: no cover
    Request = None
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
    """'0.9.8d' -> ((0, ''), (9, ''), (8, 'd')).
    마디마다 숫자 뒤에 붙은 문자 접미사도 함께 비교한다.
    접미사를 버리면 0.9.8c와 0.9.8d가 같은 버전으로 보여
    새 버전 알림이 뜨지 않는다."""
    parts = []
    for piece in str(version or '').split('.'):
        digits = ''
        suffix = ''
        for i, ch in enumerate(piece):
            if ch.isdigit():
                digits += ch
            else:
                suffix = piece[i:]
                break
        try:
            num = int(digits) if digits else 0
        except Exception:
            num = 0
        parts.append((num, suffix))
    return tuple(parts) or ((0, ''),)


def is_newer(candidate, base):
    """candidate 버전이 base보다 높으면 True. 둘 중 하나라도 없으면 False."""
    if not candidate or not base:
        return False
    a = _version_tuple(candidate)
    b = _version_tuple(base)
    width = max(len(a), len(b))
    a = a + ((0, ''),) * (width - len(a))
    b = b + ((0, ''),) * (width - len(b))
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


# ---------- shyni GHCR 최신 버전 ----------

GHCR_REGISTRY = 'ghcr.io'
GHCR_REPO = 'kihyyo/shyni'
GHCR_TAG = 'beta'
GHCR_TOKEN_URL = (
    'https://ghcr.io/token?service=ghcr.io'
    '&scope=repository:kihyyo/shyni:pull')
GHCR_MANIFEST_URL = 'https://ghcr.io/v2/kihyyo/shyni/manifests/beta'
GHCR_BLOB_URL = 'https://ghcr.io/v2/kihyyo/shyni/blobs/'
GHCR_CHECK_TTL_SEC = 3600  # 1시간
GHCR_CHECK_TIMEOUT_SEC = 10
_GHCR_RETRY_AFTER_FAIL_SEC = 600  # 실패하면 10분 뒤에 다시 시도
_GHCR_ACCEPT = (
    'application/vnd.oci.image.index.v1+json, '
    'application/vnd.oci.image.manifest.v1+json, '
    'application/vnd.docker.distribution.manifest.v2+json')


def _ghcr_get(url, headers, timeout=GHCR_CHECK_TIMEOUT_SEC):
    """GHCR에 GET 요청을 보내고 JSON을 돌려준다. 실패하면 예외."""
    req = Request(url, headers=headers)
    with urlopen(req, timeout=timeout) as res:
        if res.status != 200:
            raise RuntimeError(f'GHCR HTTP {res.status}')
        return json.load(res)


def fetch_ghcr_latest(token):
    """GHCR에서 shyni beta 이미지의 최신 버전·git SHA를 읽는다.

    토큰이 없거나 실패하면 None. 성공하면
    {'version': '0.3.0-beta.14', 'git_sha': 'f7f93e1f...'}를 돌려준다.
    beta 태그는 멀티 아키텍처 인덱스라 첫 번째 매니페스트를 쓴다
    — git SHA는 아키텍처와 무관하게 같은 빌드 값이다.
    """
    if not token or Request is None or urlopen is None:
        return None
    try:
        import base64
        auth = base64.b64encode(
            ('shyni-pull:' + token).encode()).decode()
        # 1. 레지스트리 토큰 발급
        reg = _ghcr_get(
            GHCR_TOKEN_URL, {'Authorization': 'Basic ' + auth})
        reg_token = (reg or {}).get('token')
        if not reg_token:
            return None
        hdr = {'Authorization': 'Bearer ' + reg_token}
        # 2. beta 매니페스트 (인덱스면 첫 항목의 매니페스트로 다시 읽는다)
        manifest = _ghcr_get(
            GHCR_MANIFEST_URL, dict(hdr, Accept=_GHCR_ACCEPT))
        media_type = (manifest or {}).get('mediaType', '')
        if 'index' in media_type or 'manifest.list' in media_type:
            manifests = (manifest or {}).get('manifests') or []
            if not manifests:
                return None
            digest = manifests[0].get('digest')
            if not digest:
                return None
            manifest = _ghcr_get(
                GHCR_MANIFEST_URL.rsplit('/', 1)[0] + '/' + digest,
                dict(hdr, Accept=_GHCR_ACCEPT))
        cfg_digest = ((manifest or {}).get('config') or {}).get('digest')
        if not cfg_digest:
            return None
        # 3. 설정 블롭에서 이미지 라벨을 읽는다
        config = _ghcr_get(GHCR_BLOB_URL + cfg_digest, hdr)
        labels = ((config or {}).get('config') or {}).get('Labels') or {}
        version = labels.get('org.opencontainers.image.version') or ''
        git_sha = labels.get('org.opencontainers.image.revision') or ''
        created = (config or {}).get('created') or ''
        build_date = str(created)[:10] if created else ''
        if not version and not git_sha:
            return None
        return {'version': str(version).strip(),
                'git_sha': str(git_sha).strip(),
                'build_date': build_date}
    except Exception:
        return None


_ghcr_lock = threading.Lock()
_ghcr_cache = {'latest': None, 'checked_at': 0.0, 'fetching': False,
               'token': None}


def _refresh_ghcr_latest(token):
    """GHCR 최신 버전을 동기적으로 읽어 캐시를 채운다. 스레드 안에서만 쓴다."""
    try:
        latest = fetch_ghcr_latest(token)
    except Exception as e:
        latest = None
        _log.debug(f'[GHCR] fetch exception: {e}')
    with _ghcr_lock:
        _ghcr_cache['fetching'] = False
        if latest:
            _ghcr_cache['latest'] = latest
            _ghcr_cache['checked_at'] = time.time()
            _ghcr_cache['token'] = token
            _log.debug(f"[GHCR] fetched: {latest.get('version')} "
                       f"{(latest.get('git_sha') or '')[:7]}")
        else:
            # 실패하면 다음 시도를 10분 뒤로 미룬다.
            _ghcr_cache['checked_at'] = (
                time.time() - GHCR_CHECK_TTL_SEC
                + _GHCR_RETRY_AFTER_FAIL_SEC)
            _log.debug('[GHCR] fetch failed (None)')


def get_ghcr_latest(token):
    """캐시된 GHCR 최신 버전을 돌려준다. 토큰이 없으면 None.
    캐시가 오래됐거나 토큰이 바뀌었으면 백그라운드로 새로 읽어 온다.
    이 함수는 네트워크를 기다리지 않는다."""
    if not token:
        return None
    now = time.time()
    with _ghcr_lock:
        stale = ((now - _ghcr_cache['checked_at']) >= GHCR_CHECK_TTL_SEC
                 or _ghcr_cache['token'] != token)
        fetching = _ghcr_cache['fetching']
        latest = _ghcr_cache['latest']
        if stale and not fetching:
            _ghcr_cache['fetching'] = True
            need_fetch = True
        else:
            need_fetch = False
    if need_fetch:
        threading.Thread(target=_refresh_ghcr_latest, args=(token,),
                         daemon=True).start()
    return latest
