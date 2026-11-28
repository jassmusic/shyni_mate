"""shyni 관리자 세션 API — Step3.

저장된 관리자 id/pw로 POST /auth/login을 해 세션 토큰을 받아 관리한다.
토큰은 만료가 없어서 한 번 받으면 설정에 저장해 두고 재사용하고,
401이 나오면 지우고 다시 로그인한다 (세션이 서버에서 지워진 경우).

로그인 형식 (shyni 소스 /app/app/api/auth.py 실측, 2026-11-17):
- 요청: POST {url}/auth/login, JSON {"username", "password",
  "device_name", "app_name"} — username은 users.display_name과 대조한다.
- 성공: {"token", "user_id", "username", "is_admin"} + 세션 쿠키.
  TOTP 켜진 계정은 {"require_totp": true, "challenge_token"}을 준다.
- 관리자 API는 Authorization: Bearer <token> 헤더로 부른다.
"""
import re
import time

import requests

from .setup import *

logger = P.logger


def _decrypt(key, value):
    """암호화 대상 키면 저장값을 복호화한다 (secret_store)."""
    try:
        from . import secret_store
        return secret_store.decrypt_for_key(key, value)
    except Exception:
        return value


def _encrypt(key, value):
    """암호화 대상 키면 저장 직전에 암호화한다 (secret_store)."""
    try:
        from . import secret_store
        return secret_store.encrypt_for_key(key, value)
    except Exception:
        return value


def _setting_get(key):
    """app_context 중첩 없이 설정값을 읽는다.

    P.ModelSetting.get은 항상 새 app_context를 만들어서, 이미 컨텍스트
    안에서 부르면 중첩되고 DB 연결이 풀에 반환되지 않는다 (QueuePool
    고갈 원인). 컨텍스트가 이미 있으면 현재 세션으로 직접 쿼리하고,
    없을 때만 프레임워크 방식을 쓴다. shyni_backend의 같은 이름 함수와
    동일 — shyni_admin을 독립 모듈로 두기 위해 여기에 둔다.
    """
    try:
        import flask
        if flask.has_app_context():
            ret = F.db.session.query(P.ModelSetting).filter_by(key=key).first()
            value = ret.value.strip() if ret is not None else None
            return _decrypt(key, value)
    except Exception:
        pass
    return _decrypt(key, P.ModelSetting.get(key))

SESSION_TOKEN_KEY = 'base_shyni_session_token'
ADMIN_ID_KEY = 'base_shyni_admin_id'
ADMIN_PW_KEY = 'base_shyni_admin_pw'
DEVICE_NAME = 'shyni_mate'
APP_NAME = 'shyni_mate'


def _setting_set(key, value):
    """app_context 중첩 없이 설정값을 쓴다 (_setting_get의 쓰기 버전).

    P.ModelSetting.set은 항상 새 app_context를 만들어서, 이미 컨텍스트
    안에서 부르면 중첩되고 DB 연결이 풀에 반환되지 않는다 (QueuePool
    고갈 원인 — get과 같은 원리). 컨텍스트가 이미 있으면 현재 세션으로
    직접 쓰고, 없을 때만 프레임워크 방식을 쓴다.

    암호화 대상 키(세션 토큰 등)는 저장 직전에 암호화한다 — 세션 토큰은
    이 경로로만 저장되므로, 여기서 암호화하면 자동 저장까지 일관된다.
    """
    value = _encrypt(key, value)
    try:
        import flask
        if flask.has_app_context():
            row = F.db.session.query(P.ModelSetting).filter_by(key=key).first()
            if row is None:
                row = P.ModelSetting(key=key, value=value)
                F.db.session.add(row)
            else:
                row.value = value
            F.db.session.commit()
            return
    except Exception:
        try:
            F.db.session.rollback()
        except Exception:
            pass
    P.ModelSetting.set(key, value)


def get_admin_credentials():
    """저장된 관리자 id/pw. 둘 중 하나라도 없으면 (None, None)."""
    admin_id = (_setting_get(ADMIN_ID_KEY) or '').strip()
    admin_pw = (_setting_get(ADMIN_PW_KEY) or '').strip()
    if not admin_id or not admin_pw:
        return None, None
    return admin_id, admin_pw


def get_session_token():
    """저장된 세션 토큰. 없으면 None."""
    return (_setting_get(SESSION_TOKEN_KEY) or '').strip() or None


def clear_session_token():
    _setting_set(SESSION_TOKEN_KEY, '')


def login():
    """저장된 id/pw로 로그인해 세션 토큰을 받아 저장한다. 실패하면 None."""
    url = (_setting_get('base_shyni_url') or '').strip().rstrip('/')
    admin_id, admin_pw = get_admin_credentials()
    if not url or not admin_id or not admin_pw:
        return None
    try:
        res = requests.post(
            f'{url}/auth/login',
            json={'username': admin_id, 'password': admin_pw,
                  'device_name': DEVICE_NAME, 'app_name': APP_NAME},
            timeout=10,
        )
    except Exception as e:
        logger.warning(f'[Admin] shyni 로그인 요청 실패: {e}')
        return None
    if res.status_code == 401:
        logger.warning('[Admin] shyni 로그인 실패: 아이디·비밀번호를 확인해 주세요.')
        return None
    if res.status_code != 200:
        logger.warning(f'[Admin] shyni 로그인 실패: 응답 코드 {res.status_code}')
        return None
    try:
        data = res.json()
    except Exception:
        logger.warning('[Admin] shyni 로그인 응답을 읽지 못했습니다.')
        return None
    if data.get('require_totp'):
        logger.warning('[Admin] shyni 계정에 2단계 인증이 켜져 있어 자동 로그인이 안 됩니다.')
        return None
    token = (data.get('token') or '').strip()
    if not token:
        logger.warning('[Admin] shyni 로그인 응답에 토큰이 없습니다.')
        return None
    _setting_set(SESSION_TOKEN_KEY, token)
    logger.info(f"[Admin] shyni 세션 토큰 발급 완료 "
                f"(user={data.get('username')}, admin={data.get('is_admin')})")
    return token


def test_credentials(admin_id, admin_pw):
    """입력받은 id/pw로 로그인이 되는지 확인만 한다 (토큰을 저장하지 않음).
    테스트용 세션은 확인 뒤 바로 로그아웃해 지운다 (쌓이지 않게).
    (성공 여부, 메시지, 관리자 여부)를 반환한다."""
    url = (_setting_get('base_shyni_url') or '').strip().rstrip('/')
    if not url:
        return False, 'shyni 주소를 먼저 입력해 주세요.', False
    if not admin_id or not admin_pw:
        return False, '관리자 아이디와 비밀번호를 입력해 주세요.', False
    try:
        res = requests.post(
            f'{url}/auth/login',
            json={'username': admin_id, 'password': admin_pw,
                  'device_name': DEVICE_NAME, 'app_name': APP_NAME},
            timeout=10,
        )
    except Exception as e:
        return False, f'연결에 실패했습니다: {e}', False
    if res.status_code == 401:
        return False, '아이디·비밀번호가 맞지 않습니다.', False
    if res.status_code != 200:
        return False, f'응답 코드가 {res.status_code}입니다.', False
    try:
        data = res.json()
    except Exception:
        return False, '로그인 응답을 읽지 못했습니다.', False
    if data.get('require_totp'):
        return False, '이 계정은 2단계 인증이 켜져 있어 자동 로그인이 안 됩니다.', False
    token = (data.get('token') or '').strip()
    if not token:
        return False, '로그인 응답에 토큰이 없습니다.', False
    # 토큰으로 내 정보 조회 — 관리자 API가 실제로 되는지 확인한다.
    me = None
    try:
        me = requests.get(
            f'{url}/auth/me',
            headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json'},
            timeout=10,
        )
    except Exception as e:
        me = None
        me_error = str(e)
    finally:
        # 테스트용 세션은 바로 지운다 (쌓이지 않게).
        try:
            requests.post(f'{url}/auth/logout',
                          headers={'Authorization': f'Bearer {token}'}, timeout=10)
        except Exception:
            pass
    if me is None:
        return False, f'로그인은 됐는데 정보 조회에 실패했습니다: {me_error}', False
    if me.status_code != 200:
        return False, f'로그인은 됐는데 정보 조회가 안 됩니다 (응답 코드 {me.status_code}).', False
    try:
        me_data = me.json()
    except Exception:
        return False, '정보 조회 응답을 읽지 못했습니다.', False
    is_admin = bool(me_data.get('is_admin'))
    username = me_data.get('username') or admin_id
    if not is_admin:
        return False, f'{username} 계정으로 로그인됐지만 관리자 계정이 아닙니다. 관리자 계정을 입력해 주세요.', False
    return True, f'{username} 계정으로 로그인에 성공했습니다. 관리자 API를 쓸 수 있습니다.', True


def ensure_session_token():
    """저장된 토큰이 있으면 쓰고, 없으면 로그인한다."""
    token = get_session_token()
    if token:
        return token
    return login()


def admin_request(method, path, _retried=False, **kwargs):
    """관리자 API 요청. 401이면 토큰을 지우고 다시 로그인해 한 번 재시도한다.
    성공 여부와 관계없이 requests Response를 반환하고, 요청 자체가 실패하면 None."""
    url = (_setting_get('base_shyni_url') or '').strip().rstrip('/')
    if not url:
        return None
    token = ensure_session_token()
    if not token:
        return None
    timeout = kwargs.pop('timeout', 15)
    headers = dict(kwargs.pop('headers', {}) or {})
    headers['Authorization'] = f'Bearer {token}'
    headers.setdefault('Accept', 'application/json')
    try:
        res = requests.request(method, f'{url}{path}', headers=headers,
                               timeout=timeout, **kwargs)
    except Exception as e:
        logger.warning(f'[Admin] 관리자 API 요청 실패 ({method} {path}): {e}')
        return None
    if res.status_code == 401 and not _retried:
        logger.info('[Admin] 세션 토큰이 거부되어 다시 로그인합니다.')
        clear_session_token()
        token = login()
        if not token:
            return None
        headers['Authorization'] = f'Bearer {token}'
        try:
            res = requests.request(method, f'{url}{path}', headers=headers,
                                   timeout=timeout, **kwargs)
        except Exception as e:
            logger.warning(f'[Admin] 관리자 API 재시도 실패 ({method} {path}): {e}')
            return None
    return res


def admin_get(path, **kwargs):
    """관리자 API GET. 성공(200)이면 JSON dict, 아니면 None."""
    res = admin_request('GET', path, **kwargs)
    if res is None or res.status_code != 200:
        return None
    try:
        return res.json()
    except Exception:
        return None


def admin_available():
    """관리자 API를 쓸 수 있는지 — 관리자 id/pw가 저장돼 있는지.
    토큰 발급까지는 확인하지 않는다 (필요할 때 로그인한다)."""
    admin_id, admin_pw = get_admin_credentials()
    url = (_setting_get('base_shyni_url') or '').strip()
    return bool(url and admin_id and admin_pw)


# ── 휴지통 API (Step4) ─────────────────────────────────────────────────────
# GET /libraries/{library_id}/trash 응답 (shyni 소스 /app/app/api/library_scan.py
# 실측 — 라우터 prefix가 "/libraries"라 전체 경로는 /libraries/{id}/trash다.
# prefix를 빼고 /{id}/trash로 부르면 404가 난다, 2026-11-17 실측):
# {"items": [...], "count": N} — items 각 항목은 part_id·folder_id·
# metadata_item_id·file_path·status·missing_since·deleted_at·
# deleted_reason·title·metadata_type·folder_path. status <> 'available'라
# missing+trashed 둘 다 포함 (Relay의 deleted_at IS NOT NULL과 같은 범위).
# 기본 limit 500, offset 페이지네이션. count는 페이지 내 건수라 전체 개수는
# 주지 않아서 전수 조회로 센다 (limit=1일 때 count=1 확인, 2026-11-17 실측).

def api_trash_list(library_id, limit=500):
    """한 라이브러리의 휴지통 항목 전수 조회 (페이지네이션).
    성공하면 항목 dict 목록, API 실패하면 None."""
    items = []
    offset = 0
    while True:
        data = admin_get(f'/libraries/{int(library_id)}/trash',
                         params={'limit': limit, 'offset': offset})
        if data is None:
            return None
        page = data.get('items') or []
        items.extend(page)
        if len(page) < limit:
            break
        offset += limit
        if offset > 100000:  # 안전장치 — 비정상적으로 많으면 중단
            logger.warning(f'[Admin] 휴지통 목록이 너무 많습니다 (library {library_id})')
            break
    return items


def api_trash_file_paths(library_id):
    """휴지통 파일 경로 목록 (API). 실패하면 None."""
    items = api_trash_list(library_id)
    if items is None:
        return None
    return [it.get('file_path') or '' for it in items if it.get('file_path')]


def api_trash_file_entries(library_id):
    """휴지통 파일 목록 + 상태별 경과 (API). 실패하면 None.
    [{'p': 경로, 's': 'missing'|'trashed', 'd': 경과일}]"""
    from datetime import datetime, timezone
    from . import db_support
    items = api_trash_list(library_id)
    if items is None:
        return None
    now = datetime.now(timezone.utc)
    out = []
    for it in items:
        path = it.get('file_path') or ''
        if not path:
            continue
        st = (it.get('status') or '').strip() or None
        out.append({'p': path, 's': st,
                    'd': db_support.trash_age_days(
                        st, it.get('missing_since'), it.get('deleted_at'),
                        now)})
    return out


def api_trash_counts(library_ids):
    """라이브러리별 휴지통 파일 수 {library_id: count} (API).
    한 라이브러리라도 실패하면 None (전체 실패로 보고 Relay 폴백)."""
    counts = {}
    for lib_id in library_ids:
        items = api_trash_list(lib_id)
        if items is None:
            return None
        counts[int(lib_id)] = len(items)
    return counts


def api_empty_trash(library_id, part_ids=None, timeout=600):
    """POST /libraries/{id}/empty_trash — 휴지통 비우기 (관리자 세션).

    part_ids를 주면 고른 항목만 비운다 (EmptyTrashRequest.part_ids,
    shyni 소스 /app/app/api/library_schemas.py 실측, 2026-11-18).
    part_ids가 None이면 라이브러리 전체를 비운다. 빈 목록을 주면
    전체 비우기로 오해할 수 있어 None을 돌려준다 (호출 전 검증용).
    보관 기간은 기본 동작(준수)으로 둔다 — ignore_retention/force는
    보내지 않는다. 기간이 안 지난 항목은 trashed로 승격돼 남고,
    응답의 retained_parts에 그 수가 들어간다.
    requests Response를 돌려준다 (요청 자체 실패면 None) — 호출하는
    쪽이 상태 코드를 보고 판단한다. 선택 항목이 바뀌었으면 shyni가
    409("선택 항목이 변경되었습니다. 휴지통 목록을 새로고침하세요.")를
    준다. 성공 응답은 평평한 dict (removed_parts·retained_parts가
    최상위 — /app/app/services/trash.py 실측).
    """
    try:
        lib_id = int(library_id)
    except Exception:
        return None
    payload = {}
    if part_ids is not None:
        try:
            ids = sorted({int(p) for p in part_ids if int(p) > 0})
        except Exception:
            return None
        if not ids:
            return None
        payload['part_ids'] = ids
    return admin_request('POST', f'/libraries/{lib_id}/empty_trash',
                         json=payload, timeout=timeout)


def api_library_detail(library_id):
    """GET /libraries/{id} — 라이브러리 상세 (type·trash_retention_days 포함).
    실패하면 None. (shyni 소스 /app/app/api/library_management.py 실측:
    libraries 행 전체를 돌려준다.)"""
    return admin_get(f'/libraries/{int(library_id)}')


def api_library_scan_timeouts():
    """GET /libraries — 라이브러리별 파일 목록 응답 대기시간(scan_timeout_sec).
    성공하면 [{'name': 이름, 'sec': 초}] 목록, 실패하면 None.
    (shyni 소스 /app/app/api/library_management.py 실측: 목록 응답에
    scan_timeout_sec가 라이브러리마다 들어 있다. 응답의 id는 View ID
    기준이라 물리 섹션 ID와 달라, 라이브러리 구분은 이름으로 한다.)"""
    data = admin_get('/libraries')
    if not isinstance(data, list):
        return None
    items = []
    for lib in data:
        if not isinstance(lib, dict):
            continue
        sec = lib.get('scan_timeout_sec')
        if sec is None:
            continue
        items.append({'name': lib.get('name') or '', 'sec': sec})
    return items or None


# ── 스캔 실패 문구 정리 ─────────────────────────────────────────────
_SCAN_TIMEOUT_RE = re.compile(
    r'파일 목록 조회가 ([0-9]+(?:\.[0-9]+)?)초 동안 응답하지 않습니다'
    r'(?: \(수집 ([0-9,]+)개\))?(?:: (.+))?$')
_timeout_cache = {'at': 0.0, 'items': None}


def _scan_timeouts_cached():
    """라이브러리별 대기시간 목록을 5분간 재사용한다 (실패 문구 정리용)."""
    now = time.monotonic()
    if (_timeout_cache['items'] is not None
            and now - _timeout_cache['at'] < 300):
        return _timeout_cache['items']
    items = api_library_scan_timeouts()
    if items is not None:
        _timeout_cache['items'] = items
        _timeout_cache['at'] = now
    return items


def friendly_scan_error(error, library_name=None):
    """shyni 스캔 실패 원문을 표시용 문구로 정리한다.

    수집 중 응답 대기시간 초과(파일 목록 조회가 N초 동안 응답하지
    않습니다)는 '파일 목록 조회가 설정된 응답 대기시간 N초를
    초과했습니다'로 바꾸고, 수집 개수·경로는 그대로 붙인다. N은
    library_name으로 라이브러리 설정값을 확인해 쓰고, 확인이 안 되면
    원문에 찍힌 실제 적용값을 쓴다. 해당하지 않는 오류는 원문 그대로
    돌려준다. library_name이 없으면 관리자 API를 부르지 않는다."""
    text = (error or '').strip()
    m = _SCAN_TIMEOUT_RE.search(text)
    if not m:
        return text
    sec = None
    if library_name:
        items = _scan_timeouts_cached() or []
        for it in items:
            if it.get('name') == library_name:
                sec = it.get('sec')
                break
        if sec is None:
            for it in items:
                name = it.get('name') or ''
                if name and (library_name in name or name in library_name):
                    sec = it.get('sec')
                    break
    if sec is None:
        raw = float(m.group(1))
        sec = int(raw) if raw == int(raw) else raw
    out = f'파일 목록 조회가 설정된 응답 대기시간 {sec}초를 초과했습니다'
    if m.group(2):
        out += f' (수집 {m.group(2)}개)'
    if m.group(3):
        out += f': {m.group(3)}'
    return out


def is_collect_timeout_error(error):
    """스캔 실패 원문이 수집 중 응답 대기시간 초과인지 판정한다."""
    return bool(_SCAN_TIMEOUT_RE.search((error or '').strip()))


def _epoch_from_value(value):
    """ISO 문자열·datetime·숫자를 epoch 초로 바꾼다. 실패하면 None."""
    from datetime import datetime, timezone
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace('Z', '+00:00'))
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def stuck_collect_state(status_info, started_epoch, finished_epoch):
    """남은 스캔 상태가 실패한 job의 수집 잔재인지 판정한다 (순수 함수).

    수집 타임아웃으로 job이 실패하면 shyni job은 failed로 끝나는데
    라이브러리 스캔 상태는 '수집 중(collecting/collect)'에 멈춰 남는다
    (2026-11-28 실측 — 영화·국내 모음). 그 잔재는 마지막 갱신 시각(ts)이
    실패한 job이 살아 있던 구간 안에 있다. 새 스캔이 시작되면 상태가
    새 ts로 다시 쓰이므로, ts가 job 종료 뒤면 남의 스캔이라 건드리지
    않는다."""
    if not isinstance(status_info, dict):
        return False
    if status_info.get('status') != 'collecting':
        return False
    if status_info.get('stage') != 'collect':
        return False
    ts = status_info.get('ts')
    if not isinstance(ts, (int, float)):
        return False
    if started_epoch is not None and ts < started_epoch - 5:
        return False
    if finished_epoch is not None and ts > finished_epoch + 5:
        return False
    return True


def clear_stuck_collect_state(section_key, job=None):
    """수집 타임아웃 실패 뒤 남은 '수집 중' 상태를 취소 호출로 정리한다.

    상태가 실패한 job의 잔재일 때만 api_cancel_scan을 한 번 건다 —
    사용자가 shyni 웹에서 직접 취소하는 것과 같은 호출이다. 취소
    표시는 다음 스캔 시작 때 shyni가 스스로 지우므로(scanner 시작
    경로가 _scan_cancel을 discard) 다음 스캔에 영향이 없다.
    정리했으면 True, 대상이 아니거나 실패하면 False."""
    try:
        lib_key = int(section_key)
    except Exception:
        return False
    job = job or {}
    started = _epoch_from_value(job.get('started_at'))
    finished = _epoch_from_value(job.get('finished_at'))
    status_info = api_scan_status(lib_key)
    if not stuck_collect_state(status_info, started, finished):
        return False
    res = api_cancel_scan(lib_key)
    return bool(res is not None and 200 <= res.status_code < 300)


def api_item_info(library_id, item_id):
    """GET /libraries/{id}/media_info/item/{item_id} — 항목 정보.
    실패하면 None. 반환: {"item": {항목 행 + parent_title·parent_type·
    grandparent_title·grandparent_type}, "parts": [...], ...}.
    (shyni 소스 /app/app/api/library_items.py 실측.)"""
    return admin_get(f'/libraries/{int(library_id)}/media_info/item/{int(item_id)}')


def _api_item_row(library_id, item_id):
    """media_info의 item 행 하나. admin=true(물리 라이브러리 검증) 우선,
    안 되면 admin 없이(View 검증) 재시도한다. 둘 다 안 되면 None.

    /libraries/{id}는 엔드포인트마다 View ID와 물리 라이브러리 ID를
    다르게 쓰므로(2026-11-17 실측: /libraries/4는 '음악 | OST'인데
    물리 라이브러리 4는 '음악 | V.A'), 두 방식을 다 시도한다.
    """
    try:
        lib_id = int(library_id)
        meta_id = int(item_id)
    except Exception:
        return None
    for admin in (True, False):
        path = f'/libraries/{lib_id}/media_info/item/{meta_id}'
        if admin:
            path += '?admin=true'
        res = admin_get(path)
        item = (res or {}).get('item') or {} if isinstance(res, dict) else {}
        if item.get('id'):
            return item
    return None


def api_metadata_info(library_id, metadata_id):
    """관리자 API로 메타데이터 한 건을 읽어 db_support.metadata_info()와
    같은 dict로 반환한다. 없으면 None.

    - 앨범(9): 앨범 정보 그대로
    - 가수(8): 가수 정보 + 앨범·트랙 수 (API로 셀 수 없어 None)
    - 트랙(10): 부모 앨범 정보로 교체 (track_title에 원래 트랙 제목)
    """
    if not library_id or not metadata_id:
        return None
    item = _api_item_row(library_id, metadata_id)
    if not item:
        return None
    mtype = item.get('metadata_type')
    parent_id = item.get('parent_id')
    if mtype == 10 and parent_id:
        track_title = item.get('title')
        pitem = _api_item_row(library_id, parent_id)
        if pitem:
            return {'kind': 'album', 'title': pitem.get('title'),
                    'year': pitem.get('year'), 'summary': pitem.get('summary'),
                    'thumb_url': pitem.get('thumb_url'),
                    'track_title': track_title}
    if mtype == 8:
        return {'kind': 'artist', 'title': item.get('title'),
                'year': item.get('year'), 'summary': item.get('summary'),
                'thumb_url': item.get('thumb_url'),
                'album_count': None, 'track_count': None}
    return {'kind': 'album', 'title': item.get('title'),
            'year': item.get('year'), 'summary': item.get('summary'),
            'thumb_url': item.get('thumb_url')}


def api_scan_status(library_id):
    """GET /libraries/{id}/scan/status — 스캔 진행 상태 (관리자 세션).
    실패하면 None. 반환: {status, stage, processed, total, current_file,
    stage_processed, stage_total, new, failed, ...} (2026-11-17 실측).
    shyni 서버 메모리 상태라 라이브러리 단위다 — job 단위가 아니므로
    완료 판정에는 쓰지 않고 진행 표시용으로만 쓴다."""
    try:
        lib_id = int(library_id)
    except Exception:
        return None
    return admin_get(f'/libraries/{lib_id}/scan/status')


def api_cancel_scan(library_id):
    """DELETE /libraries/{id}/scan — 스캔 취소 (관리자 세션, Step5-1).

    shyni services/scanner.py cancel_scan 실측 (2026-11-18): _scan_cancel
    플래그 설정 + 상태 'cancelled' 병합 + 등록된 asyncio 작업 직접 취소.
    compat 스캔도 같은 scanner로 돌아 취소가 적용되고, 취소된 compat
    job은 status 'cancelled'로 끝난다 (failed 아님).
    requests Response를 돌려준다 (요청 자체 실패면 None) — 호출하는
    쪽이 상태 코드를 보고 판단한다. 2xx면 취소 요청이 받아들여진 것이고,
    404면 돌고 있는 스캔이 없다는 뜻이라 취소할 게 없다. 취소가 실제로
    반영됐는지는 compat job 'cancelled'나 scan/status 'cancelled'로
    따로 확인해야 한다 (즉시 반영 아님).
    """
    try:
        lib_id = int(library_id)
    except Exception:
        return None
    return admin_request('DELETE', f'/libraries/{lib_id}/scan', timeout=15)


def api_current_session_id():
    """관리자 API로 현재 세션 id를 확인한다. 실패하면 None.
    GET /auth/sessions는 본인 세션만 돌려주며, current=true인 항목이
    이 플러그인이 쓰는 세션이다 (2026-11-17 실측)."""
    res = admin_get('/auth/sessions')
    if not res or not isinstance(res, dict):
        return None
    for s in res.get('sessions') or []:
        if s.get('current'):
            return s.get('id')
    return None
