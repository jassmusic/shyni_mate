# -*- coding: utf-8 -*-
"""비밀 설정값 암호화 저장 — Fernet.

대상 키(SECRET_KEYS)의 값은 mate DB에 평문이 아니라 암호문으로 저장한다.
암호문은 'enc:v1:' 접두사로 구분한다. 접두사가 없는 값은 기존 평문으로
그대로 읽어 하위 호환을 유지하고, 플러그인 로드 시 마이그레이션이
암호문으로 되쓴다 (mod_base의 plugin_load·setting_save_after 참고).

암호화 키는 플러그인 DB 파일과 같은 디렉터리의 shyni_mate.key 파일에
두고 권한을 0600으로 만든다. 키를 DB 안이나 코드 안에 넣지 않는다 —
DB 파일만 새어 나가도 값을 읽을 수 없게 하기 위해서다. 반대로 키 파일을
잃으면 저장된 값을 복호화할 수 없으니, 키 파일도 DB와 함께 백업해야 한다.

복호화에 실패하면 예외를 삼키지 않고 오류 로그를 남긴 뒤 빈 값을 돌려준다
— 잘못된 값으로 조용히 연결을 시도해 원인을 못 찾는 일이 없게 하기
위해서다.
"""
import os
import threading

PREFIX = 'enc:v1:'
KEY_FILENAME = 'shyni_mate.key'
DB_FILENAME = 'shyni_mate.db'

# 암호화 대상 — 이 키들만 암호화한다. base_shyni_admin_id는 식별자라
# 제외하고, base_shyni_url 같은 일반 설정도 대상이 아니다.
SECRET_KEYS = frozenset({
    'base_shyni_token',
    'base_shyni_admin_pw',
    'base_shyni_session_token',
    'base_ghcr_token',
    'base_db_password',
})

_lock = threading.Lock()
_fernet = None
_fernet_failed = False


def _logger():
    # 순환 import를 피하려고 쓸 때 가져온다 (setup은 이 모듈을 모른다).
    try:
        from .setup import P
        return P.logger
    except Exception:
        import logging
        return logging.getLogger(__name__)


def db_dir():
    """플러그인 DB 디렉터리 — 프레임워크가 DB를 만드는 위치와 같은 규칙.

    프레임워크(create_plugin)는 path_data/db/<패키지>.db에 플러그인 DB를
    만든다. 키 파일도 같은 디렉터리에 둬서 백업 단위를 하나로 유지한다.
    """
    from .setup import F
    return os.path.join(F.config['path_data'], 'db')


def db_path():
    return os.path.join(db_dir(), DB_FILENAME)


def key_path():
    return os.path.join(db_dir(), KEY_FILENAME)


def is_secret_key(key):
    return key in SECRET_KEYS


def is_encrypted(value):
    return isinstance(value, str) and value.startswith(PREFIX)


def _load_or_create_key(path):
    """키 파일에서 Fernet 키를 읽고, 없으면 새로 만들어 0600으로 저장한다."""
    from cryptography.fernet import Fernet
    try:
        with open(path, 'rb') as f:
            return f.read().strip()
    except FileNotFoundError:
        pass
    key = Fernet.generate_key()
    try:
        # O_EXCL로 원자적으로 만든다 — 동시에 두 프로세스가 만들어도
        # 하나만 성공하고, 진 쪽은 성공한 쪽의 키를 다시 읽는다.
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        with open(path, 'rb') as f:
            return f.read().strip()
    try:
        os.write(fd, key)
    finally:
        os.close(fd)
    _logger().info('[Secret] 암호화 키 파일을 새로 만들었습니다.')
    return key


def _get_fernet():
    """Fernet 인스턴스 (프로세스당 한 번만 만든다). 실패하면 None."""
    global _fernet, _fernet_failed
    if _fernet is not None or _fernet_failed:
        return _fernet
    with _lock:
        if _fernet is not None or _fernet_failed:
            return _fernet
        try:
            from cryptography.fernet import Fernet
            path = key_path()
            key = _load_or_create_key(path)
            try:
                mode = os.stat(path).st_mode & 0o777
                if mode & 0o077:
                    _logger().warning(
                        f'[Secret] 키 파일 권한이 넓습니다 ({oct(mode)}). '
                        '0600으로 좁히는 것을 권장합니다.')
            except Exception:
                pass
            _fernet = Fernet(key)
        except Exception as e:
            _fernet_failed = True
            _logger().error(
                f'[Secret] 암호화를 준비하지 못했습니다 — 비밀 설정값이 '
                f'평문으로 동작합니다: {e}')
        return _fernet


def encrypt_value(value):
    """평문 → 'enc:v1:' + Fernet 토큰. 빈 값이거나 이미 암호문이면 그대로."""
    if not value or is_encrypted(value):
        return value
    fernet = _get_fernet()
    if fernet is None:
        return value
    token = fernet.encrypt(str(value).encode('utf-8')).decode('ascii')
    return PREFIX + token


def decrypt_value(value):
    """저장값 → 평문. 접두사가 없으면 기존 평문으로 보고 그대로 돌려준다.

    복호화에 실패하면 오류 로그를 남기고 빈 값을 돌려준다.
    """
    if not value or not is_encrypted(value):
        return value
    fernet = _get_fernet()
    if fernet is None:
        _logger().error('[Secret] 암호화된 설정값이 있는데 암호화를 '
                        '준비하지 못해 읽을 수 없습니다.')
        return ''
    try:
        return fernet.decrypt(
            value[len(PREFIX):].encode('ascii')).decode('utf-8')
    except Exception as e:
        _logger().error(f'[Secret] 설정값 복호화에 실패했습니다: {e}')
        return ''


def encrypt_for_key(key, value):
    """키가 암호화 대상이면 암호화하고, 아니면 값을 그대로 돌려준다."""
    if is_secret_key(key):
        return encrypt_value(value)
    return value


def decrypt_for_key(key, value):
    """키가 암호화 대상이면 복호화하고, 아니면 값을 그대로 돌려준다."""
    if is_secret_key(key):
        return decrypt_value(value)
    return value
