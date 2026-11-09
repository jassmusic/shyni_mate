# -*- coding: utf-8 -*-
"""shyni PostgreSQL 직접 조회를 위한 드라이버(psycopg2) 준비.

플러그인이 로드될 때 드라이버가 없으면 pip로 설치를 시도한다.
설치에 실패해도 플러그인의 다른 동작에는 영향이 없고,
DB 조회 기능만 사용할 수 없는 상태가 된다.
"""
import hashlib
import subprocess
import sys
import threading

from .setup import *

logger = P.logger

_state = {'ready': False, 'tried': False}
_lock = threading.Lock()


def psycopg2_available():
    try:
        import psycopg2  # noqa: F401
        return True
    except Exception:
        return False


def ensure_driver():
    """psycopg2가 없으면 psycopg2-binary 설치를 시도한다. 준비 여부(bool)를 돌려준다."""
    with _lock:
        if _state['tried']:
            return _state['ready']
        _state['tried'] = True
        if psycopg2_available():
            _state['ready'] = True
            logger.info('DB 드라이버(psycopg2)가 이미 설치되어 있습니다.')
            return True
        logger.info('DB 드라이버(psycopg2)가 없어 설치를 시도합니다.')
        try:
            res = subprocess.run(
                [sys.executable, '-m', 'pip', 'install', 'psycopg2-binary'],
                capture_output=True, text=True, timeout=900)
            if res.returncode != 0:
                tail = (res.stderr or res.stdout or '').strip().splitlines()
                logger.warning(f"DB 드라이버 설치에 실패했습니다: {tail[-1] if tail else '원인을 알 수 없습니다'}")
                return False
        except Exception as e:
            logger.warning(f'DB 드라이버 설치 중 오류가 발생했습니다: {e}')
            return False
        _state['ready'] = psycopg2_available()
        if _state['ready']:
            logger.info('DB 드라이버(psycopg2) 설치가 완료됐습니다.')
        else:
            logger.warning('DB 드라이버를 설치했지만 불러오지 못했습니다.')
        return _state['ready']


# ---- PostgreSQL Relay 접속 (고급 설정) ----
# 활용 조건: '사용'이 켜져 있고, 연결 테스트에 성공한 접속 정보가
# 지금 저장된 설정과 그대로 일치할 때만 정보를 활용한다.
# 설정을 하나라도 바꾸면 지문이 달라져 다시 테스트하기 전까지 활용하지 않는다.

def db_params():
    """저장된 설정에서 접속 정보를 읽는다. 필수 값이 비어 있으면 None."""
    get = P.ModelSetting.get
    host = (get('base_db_host') or '').strip()
    user = (get('base_db_user') or '').strip()
    if not host or not user:
        return None
    try:
        port = int(get('base_db_port') or 15432)
    except Exception:
        port = 15432
    return {'host': host, 'port': port,
            'dbname': (get('base_db_name') or 'shyni').strip() or 'shyni',
            'user': user, 'password': get('base_db_password') or ''}


def params_fingerprint(params):
    raw = '|'.join([params['host'], str(params['port']), params['dbname'],
                    params['user'], params['password']])
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()


def db_ready():
    """DB 정보를 활용해도 되는지 여부."""
    if not P.ModelSetting.get_bool('base_db_use'):
        return False
    params = db_params()
    if params is None:
        return False
    ok = P.ModelSetting.get('base_db_ok_hash') or ''
    return bool(ok) and ok == params_fingerprint(params)


def connect(params=None, timeout=5):
    import psycopg2
    params = params or db_params()
    if params is None:
        raise ValueError('DB 접속 정보가 부족합니다.')
    return psycopg2.connect(connect_timeout=timeout, **params)


def library_stats():
    """라이브러리별 앨범·트랙·아티스트 수와 메타 누락 집계.

    반환: {library_id(int): {...}}
    metadata_type 8=아티스트, 9=앨범, 10=트랙. 메타 없음은 앨범 중
    metadata_code와 yaml_code가 모두 없는 것이다.
    """
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT library_id,
                   count(*) FILTER (WHERE metadata_type = 9),
                   count(*) FILTER (WHERE metadata_type = 10),
                   count(*) FILTER (WHERE metadata_type = 8),
                   count(*) FILTER (WHERE metadata_type = 9
                                      AND metadata_code IS NULL AND yaml_code IS NULL),
                   count(*) FILTER (WHERE metadata_type = 9 AND (summary IS NULL OR summary = '')),
                   count(*) FILTER (WHERE metadata_type = 9 AND year IS NULL),
                   count(*) FILTER (WHERE metadata_type = 9 AND (thumb_url IS NULL OR thumb_url = ''))
            FROM metadata_items
            GROUP BY library_id
        """)
        stats = {}
        for lib_id, albums, tracks, artists, no_code, no_summary, no_year, no_thumb in cur.fetchall():
            albums = int(albums or 0)
            no_code = int(no_code or 0)
            stats[int(lib_id)] = {
                'albums': albums, 'tracks': int(tracks or 0),
                'artists': int(artists or 0), 'no_code': no_code,
                'no_summary': int(no_summary or 0), 'no_year': int(no_year or 0),
                'no_thumb': int(no_thumb or 0),
                'ok_pct': round((albums - no_code) * 100.0 / albums, 1) if albums else 0}
        return stats
    finally:
        conn.close()


def dashboard_stats():
    """현황 건강도용 종합 집계 (전체 앨범/트랙/아티스트, 정상·점검 필요, 누락 항목)."""
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
              count(*) FILTER (WHERE metadata_type = 9),
              count(*) FILTER (WHERE metadata_type = 10),
              count(*) FILTER (WHERE metadata_type = 8),
              count(*) FILTER (WHERE metadata_type = 9
                                 AND metadata_code IS NULL AND yaml_code IS NULL),
              count(*) FILTER (WHERE metadata_type = 9 AND (summary IS NULL OR summary = '')),
              count(*) FILTER (WHERE metadata_type = 9 AND year IS NULL),
              count(*) FILTER (WHERE metadata_type = 9 AND (thumb_url IS NULL OR thumb_url = ''))
            FROM metadata_items
        """)
        albums, tracks, artists, no_code, no_summary, no_year, no_thumb = cur.fetchone()
        albums = int(albums or 0)
        no_code = int(no_code or 0)
        ok = albums - no_code
        return {'albums': albums, 'tracks': int(tracks or 0), 'artists': int(artists or 0),
                'ok': ok, 'no_code': no_code,
                'ok_pct': round(ok * 100.0 / albums, 1) if albums else 0,
                'check_pct': round(no_code * 100.0 / albums, 1) if albums else 0,
                'no_summary': int(no_summary or 0), 'no_year': int(no_year or 0),
                'no_thumb': int(no_thumb or 0)}
    finally:
        conn.close()


def album_ids(library_id, only_no_code=False):
    """라이브러리의 앨범 id 목록 (메타 새로고침용). 메타 없는 앨범부터 먼저 온다."""
    conn = connect()
    try:
        cur = conn.cursor()
        sql = 'SELECT id FROM metadata_items WHERE library_id = %s AND metadata_type = 9 '
        if only_no_code:
            sql += 'AND metadata_code IS NULL AND yaml_code IS NULL '
        sql += 'ORDER BY (metadata_code IS NULL AND yaml_code IS NULL) DESC, id'
        cur.execute(sql, (library_id,))
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def metadata_info(metadata_id):
    """메타데이터 한 건을 유형에 맞게 읽는다. 없으면 None.

    - 앨범(9): 앨범 정보 그대로
    - 가수(8): 가수 정보 + 앨범·트랙 수
    - 트랙(10): 소속 앨범 정보로 바꿔서 반환 (track_title에 원래 트랙 제목)
    """
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT metadata_type, parent_id, title, year, summary, thumb_url '
                    'FROM metadata_items WHERE id = %s', (metadata_id,))
        row = cur.fetchone()
        if not row:
            return None
        mtype, parent_id, title, year, summary, thumb_url = row
        if mtype == 10 and parent_id:
            track_title = title
            cur.execute('SELECT title, year, summary, thumb_url '
                        'FROM metadata_items WHERE id = %s', (parent_id,))
            album = cur.fetchone()
            if album:
                return {'kind': 'album', 'title': album[0], 'year': album[1],
                        'summary': album[2], 'thumb_url': album[3],
                        'track_title': track_title}
        if mtype == 8:
            cur.execute('SELECT count(*) FROM metadata_items '
                        'WHERE metadata_type = 9 AND parent_id = %s', (metadata_id,))
            albums = cur.fetchone()[0]
            cur.execute('SELECT count(*) FROM metadata_items WHERE metadata_type = 10 '
                        'AND parent_id IN (SELECT id FROM metadata_items '
                        'WHERE metadata_type = 9 AND parent_id = %s)', (metadata_id,))
            tracks = cur.fetchone()[0]
            return {'kind': 'artist', 'title': title, 'year': year,
                    'summary': summary, 'thumb_url': thumb_url,
                    'album_count': int(albums or 0), 'track_count': int(tracks or 0)}
        return {'kind': 'album', 'title': title, 'year': year,
                'summary': summary, 'thumb_url': thumb_url}
    finally:
        conn.close()
