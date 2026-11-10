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


# 라이브러리 종류별 항목 구성 (종류 문자열과 MT_ 상수는 shyni 소스 기준)
# main=작품 단위, sub=하위 단위, group=묶음 단위. movie의 sub는 media_parts 파일 수로 센다.
# 주의: 쇼 라이브러리의 종류 문자열은 'show'가 아니라 'tv'다.
TYPE_INFO = {
    'music': {'main': 9, 'main_label': '앨범', 'sub': 10, 'sub_label': '트랙',
              'group': 8, 'group_label': '아티스트'},
    'movie': {'main': 1, 'main_label': '영화', 'sub': None, 'sub_label': '파일',
              'group': None, 'group_label': ''},
    'tv': {'main': 2, 'main_label': '쇼', 'sub': 4, 'sub_label': '에피소드',
           'group': 3, 'group_label': '시즌'},
    'book': {'main': 100, 'main_label': '시리즈', 'sub': 102, 'sub_label': '책',
             'group': None, 'group_label': ''},
}
TYPE_INFO['show'] = TYPE_INFO['tv']


def library_stats():
    """라이브러리별 통계를 종류(music/movie/show/book)에 맞춰 집계한다.

    반환: {library_id(int): {...}}
    작품(main)·하위(sub)·묶음(group) 수와, 작품 기준 메타 누락을 돌려준다.
    메타 없음은 작품 중 metadata_code와 yaml_code가 모두 없는 것이다.
    음악 라이브러리는 albums/tracks/artists 별칭도 함께 채운다.
    """
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, type, scanner_variant, ff_module_name FROM libraries")
        lib_rows = cur.fetchall()
        lib_types = {int(r[0]): (r[1] or 'music') for r in lib_rows}
        # 적용 에이전트는 shyni 자체 규칙과 같다: ff_module_name이 있으면 그 값,
        # 없으면 scanner_variant (library_views.py의 COALESCE와 동일).
        lib_variants = {int(r[0]): ((r[3] or '').strip() or (r[2] or ''))
                        for r in lib_rows}
        cur.execute("""
            SELECT library_id, metadata_type, count(*),
                   count(*) FILTER (WHERE metadata_code IS NULL AND yaml_code IS NULL),
                   count(*) FILTER (WHERE summary IS NULL OR summary = ''),
                   count(*) FILTER (WHERE year IS NULL),
                   count(*) FILTER (WHERE thumb_url IS NULL OR thumb_url = ''),
                   count(*) FILTER (WHERE (metadata_code IS NULL AND yaml_code IS NULL)
                                      OR (summary IS NULL OR summary = '')
                                      OR year IS NULL
                                      OR (thumb_url IS NULL OR thumb_url = ''))
            FROM metadata_items
            GROUP BY library_id, metadata_type
        """)
        per = {}
        for lib_id, mtype, cnt, nc, ns, ny, nt, ma in cur.fetchall():
            per.setdefault(int(lib_id), {})[int(mtype)] = {
                'count': int(cnt or 0), 'no_code': int(nc or 0),
                'no_summary': int(ns or 0), 'no_year': int(ny or 0),
                'no_thumb': int(nt or 0), 'missing_any': int(ma or 0)}
        cur.execute("""
            SELECT mi.library_id, count(*)
            FROM media_parts mp
            JOIN metadata_items mi ON mi.id = mp.metadata_item_id
            WHERE mi.metadata_type = 1
            GROUP BY mi.library_id
        """)
        movie_files = {int(r[0]): int(r[1] or 0) for r in cur.fetchall()}
        # 하위 항목(에피소드·책)의 파일 수 — 쇼·도서 카드의 '파일' 행용
        cur.execute("""
            SELECT mi.library_id, mi.metadata_type, count(*)
            FROM media_parts mp
            JOIN metadata_items mi ON mi.id = mp.metadata_item_id
            WHERE mi.metadata_type IN (4, 102)
            GROUP BY mi.library_id, mi.metadata_type
        """)
        sub_files = {(int(r[0]), int(r[1])): int(r[2] or 0) for r in cur.fetchall()}
        stats = {}
        for lib_id, lib_type in lib_types.items():
            rows = per.get(lib_id, {})
            info = TYPE_INFO.get(lib_type)
            if info is None:
                total = sum(v['count'] for v in rows.values())
                stats[lib_id] = {
                    'lib_type': lib_type, 'main_label': '항목', 'main_count': total,
                    'sub_label': '', 'sub_count': 0, 'group_label': '', 'group_count': 0,
                    'no_code': 0, 'no_summary': 0, 'no_year': 0, 'no_thumb': 0,
                    'missing_any': 0, 'ok_pct': 100.0,
                    'is_folder': False, 'judge_missing': 0, 'no_meta': 0,
                    'file_count': None,
                    'albums': 0, 'tracks': 0, 'artists': 0}
                continue
            main_row = rows.get(info['main'], {})
            main = main_row.get('count', 0)
            if info['sub'] is not None:
                sub = rows.get(info['sub'], {}).get('count', 0)
            else:
                sub = movie_files.get(lib_id, 0)
            # 파일 수: 영화는 sub 자체가 파일 수라 따로 두지 않고(None),
            # 쇼·도서는 하위 항목의 media_parts 수로 채운다.
            file_count = None
            if lib_type in ('tv', 'show', 'book') and info['sub'] is not None:
                file_count = sub_files.get((lib_id, info['sub']), 0)
            group = rows.get(info['group'], {}).get('count', 0) if info['group'] else 0
            no_code = main_row.get('no_code', 0)
            missing_any = main_row.get('missing_any', 0)
            no_thumb = main_row.get('no_thumb', 0)
            # 폴더 에이전트(적용 에이전트 이름에 folder가 들어간) 라이브러리는
            # 코드가 없는 것이 특성이라 건강도를 표지 여부로만 판단한다
            # (judge_missing가 판정 기준).
            is_folder = 'folder' in (lib_variants.get(lib_id) or '').lower()
            judge_missing = no_thumb if is_folder else missing_any
            stats[lib_id] = {
                'lib_type': lib_type,
                'main_label': info['main_label'], 'main_count': main,
                'sub_label': info['sub_label'], 'sub_count': sub,
                'group_label': info['group_label'], 'group_count': group,
                'no_code': no_code,
                'no_summary': main_row.get('no_summary', 0),
                'no_year': main_row.get('no_year', 0),
                'no_thumb': no_thumb,
                'missing_any': missing_any,
                'is_folder': is_folder,
                'file_count': file_count,
                'judge_missing': judge_missing,
                # 현황 '메타 없음' 표시용 — 폴더 에이전트는 표지 기준, 아니면 코드 기준
                'no_meta': judge_missing if is_folder else no_code,
                'ok_pct': round((main - judge_missing) * 100.0 / main, 1) if main else 100.0,
                'albums': main if lib_type == 'music' else 0,
                'tracks': sub if lib_type == 'music' else 0,
                'artists': group if lib_type == 'music' else 0}
        return stats
    finally:
        conn.close()


def dashboard_stats(libs=None):
    """건강도용 종합 집계 — 라이브러리 종류별 작품 기준 합계.

    libs(library_stats 결과)를 주면 다시 조회하지 않고 합산한다.
    전부 음악 라이브러리면 라벨이 앨범/트랙, 아니면 작품/항목이 된다.
    """
    if libs is None:
        libs = library_stats()
    main = sum(v['main_count'] for v in libs.values())
    no_code = sum(v['no_code'] for v in libs.values())
    missing_any = sum(v.get('missing_any', v['no_code']) for v in libs.values())
    judge_missing = sum(v.get('judge_missing', v.get('missing_any', v['no_code']))
                        for v in libs.values())
    ok = main - judge_missing
    all_music = bool(libs) and all(v['lib_type'] == 'music' for v in libs.values())
    return {'works': main,
            'subs': sum(v['sub_count'] for v in libs.values()),
            'groups': sum(v['group_count'] for v in libs.values()),
            'albums': sum(v['albums'] for v in libs.values()),
            'tracks': sum(v['tracks'] for v in libs.values()),
            'artists': sum(v['artists'] for v in libs.values()),
            'ok': ok, 'no_code': no_code, 'missing_any': missing_any,
            'judge_missing': judge_missing,
            'ok_pct': round(ok * 100.0 / main, 1) if main else 100.0,
            'check_pct': round(judge_missing * 100.0 / main, 1) if main else 0,
            'no_summary': sum(v['no_summary'] for v in libs.values()),
            'no_year': sum(v['no_year'] for v in libs.values()),
            'no_thumb': sum(v['no_thumb'] for v in libs.values()),
            'all_music': all_music,
            'main_label': '앨범' if all_music else '미디어',
            'sub_label': '트랙' if all_music else '항목',
            'lib_count': len(libs),
            'lib_types': {t: sum(1 for v in libs.values() if v['lib_type'] == t)
                          for t in {v['lib_type'] for v in libs.values()}}}


def library_main_label(library_id):
    """라이브러리의 종류와 작품 단위 이름 (예: ('music', '앨범')).

    종류를 알 수 없으면 ('', '미디어')를 돌려준다 — 문구에 종류별 단위가
    필요한 곳(메타 새로고침 안내 등)에서 쓴다.
    """
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT type FROM libraries WHERE id = %s', (library_id,))
        row = cur.fetchone()
        if not row:
            return '', '미디어'
        info = TYPE_INFO.get(row[0] or '')
        if info is None:
            return (row[0] or ''), '미디어'
        return (row[0] or ''), info['main_label']
    finally:
        conn.close()


def library_is_folder(library_id):
    """라이브러리의 적용 에이전트가 폴더 에이전트인지 여부.

    적용 에이전트는 ff_module_name이 있으면 그 값, 없으면 scanner_variant다
    (library_stats와 같은 규칙)."""
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT scanner_variant, ff_module_name FROM libraries WHERE id = %s',
                    (library_id,))
        row = cur.fetchone()
        if not row:
            return False
        agent = (row[1] or '').strip() or (row[0] or '')
        return 'folder' in agent.lower()
    finally:
        conn.close()


def album_ids(library_id, only_no_code=False, only_no_thumb=False):
    """라이브러리의 앨범 id 목록 (메타 새로고침용). 메타 없는 앨범부터 먼저 온다."""
    conn = connect()
    try:
        cur = conn.cursor()
        sql = 'SELECT id FROM metadata_items WHERE library_id = %s AND metadata_type = 9 '
        if only_no_code:
            sql += 'AND metadata_code IS NULL AND yaml_code IS NULL '
        if only_no_thumb:
            sql += "AND (thumb_url IS NULL OR thumb_url = '') "
        sql += 'ORDER BY (metadata_code IS NULL AND yaml_code IS NULL) DESC, id'
        cur.execute(sql, (library_id,))
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def count_filled(ids, kind):
    """메타 새로고침 대상 중 지금은 채워져 있는 항목 수.
    kind='thumb'면 표지가, 'code'면 코드가 생긴 항목을 센다."""
    if not ids:
        return 0
    cond = ("(thumb_url IS NOT NULL AND thumb_url <> '')" if kind == 'thumb'
            else '(metadata_code IS NOT NULL OR yaml_code IS NOT NULL)')
    conn = connect()
    try:
        cur = conn.cursor()
        total = 0
        for i in range(0, len(ids), 500):
            part = list(ids[i:i + 500])
            cur.execute('SELECT count(*) FROM metadata_items '
                        f"WHERE id IN ({','.join(['%s'] * len(part))}) AND {cond}", part)
            total += cur.fetchone()[0]
        return total
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
