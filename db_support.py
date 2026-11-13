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


def library_stats(trash_map=None, progress_cb=None):
    """라이브러리별 통계를 종류(music/movie/show/book)에 맞춰 집계한다.

    반환: {library_id(int): {...}}
    작품(main)·하위(sub)·묶음(group) 수와, 작품 기준 메타 누락을 돌려준다.
    메타 없음은 작품 중 metadata_code와 yaml_code가 모두 없는 것이다.
    음악 라이브러리는 albums/tracks/artists 별칭도 함께 채운다.

    trash_map({library_id: 개수})을 주면 휴지통 개수는 그 값을 쓰고
    media_parts 전체를 읽는 휴지통 쿼리는 실행하지 않는다 (현황 통계가
    캐시된 개수를 재사용하는 경로). None이면 지금처럼 직접 읽는다.
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
        if progress_cb:
            progress_cb(0.4)
        cur.execute("""
            SELECT mi.library_id, count(*)
            FROM media_parts mp
            JOIN metadata_items mi ON mi.id = mp.metadata_item_id
            WHERE mi.metadata_type = 1
            GROUP BY mi.library_id
        """)
        movie_files = {int(r[0]): int(r[1] or 0) for r in cur.fetchall()}
        if progress_cb:
            progress_cb(0.55)
        # 하위 항목(에피소드·책)의 파일 수 — 쇼·도서 카드의 '파일' 행용
        cur.execute("""
            SELECT mi.library_id, mi.metadata_type, count(*)
            FROM media_parts mp
            JOIN metadata_items mi ON mi.id = mp.metadata_item_id
            WHERE mi.metadata_type IN (4, 102)
            GROUP BY mi.library_id, mi.metadata_type
        """)
        sub_files = {(int(r[0]), int(r[1])): int(r[2] or 0) for r in cur.fetchall()}
        if progress_cb:
            progress_cb(0.65)
        # 휴지통 파일 수 — 삭제 표시(deleted_at)된 media_parts를 라이브러리별로 센다.
        # trash_map이 주어지면 이 쿼리(전체 스캔)를 건너뛰고 그 값을 쓴다.
        if trash_map is None:
            cur.execute("""
                SELECT mi.library_id, count(*)
                FROM media_parts mp
                JOIN metadata_items mi ON mi.id = mp.metadata_item_id
                WHERE mp.deleted_at IS NOT NULL
                GROUP BY mi.library_id
            """)
            trash_counts = {int(r[0]): int(r[1] or 0) for r in cur.fetchall()}
        else:
            trash_counts = {int(k): int(v or 0) for k, v in trash_map.items()}
        if progress_cb:
            progress_cb(0.8)
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
                    'trash': trash_counts.get(lib_id, 0),
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
            # 메타가 없는 것이 특성이라 건강도를 표지 여부로만 판단한다
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
                'trash': trash_counts.get(lib_id, 0),
                'judge_missing': judge_missing,
                # 현황 '메타 없음' 표시용 — 폴더 에이전트는 표지 기준, 아니면 메타 기준
                'no_meta': judge_missing if is_folder else no_code,
                'ok_pct': round((main - judge_missing) * 100.0 / main, 1) if main else 100.0,
                'albums': main if lib_type == 'music' else 0,
                'tracks': sub if lib_type == 'music' else 0,
                'artists': group if lib_type == 'music' else 0}
        return stats
    finally:
        conn.close()


def book_file_count(section_key):
    """도서 라이브러리 1개의 파일 수(media_parts)를 센다.

    도서 스캔 결과의 '전체' 실측값용 — library_stats() 전수 집계
    대신 해당 라이브러리만 세는 단건 쿼리다. library_stats()의
    도서 '파일' 행과 같은 기준(타입 102 항목의 media_parts, 휴지통
    포함)이라 대시보드 카드 4줄 숫자와 일치한다.
    조회에 실패하면 None을 돌려준다 (그 경우 entries 델타를 쓴다)."""
    try:
        conn = connect(timeout=10)
    except Exception as e:
        logger.warning(f'도서 파일 수 조회 실패 (연결): {e}')
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT count(*)
            FROM media_parts mp
            JOIN metadata_items mi ON mi.id = mp.metadata_item_id
            WHERE mi.library_id = %s AND mi.metadata_type = 102
        """, (int(section_key),))
        row = cur.fetchone()
        return int(row[0] or 0) if row else 0
    except Exception as e:
        logger.warning(f'도서 파일 수 조회 실패: {e}')
        return None
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


# ---- 휴지통 조회 ----
# 휴지통은 media_parts의 deleted_at 표시다. 파일(part)이 속한 항목에서
# 작품(main) 단위까지 parent_id를 따라 올라가 묶는다 — 음악은 앨범,
# 영화는 영화 자체, 쇼는 쇼, 도서는 시리즈가 묶음 단위가 된다.

def trash_retention_days(library_id):
    """라이브러리 보관 기간(일) — 화면 표시 전용 정보.

    보관 기간의 설정·관리는 shyni 서버가 한다. 플러그인은 판정에 쓰지 않고
    항목별 표시용으로만 읽는다: 라이브러리별 설정이 있으면 그 값, 없으면
    서버 전역 설정(server_settings config), 그래도 없으면 7일.
    """
    import json
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT trash_retention_days FROM libraries WHERE id=%s",
                    (int(library_id),))
        row = cur.fetchone()
        days = int(row[0]) if row and row[0] else 0
        if days > 0:
            return days
        try:
            cur.execute("SELECT value FROM server_settings WHERE key='config'")
            row = cur.fetchone()
            if row and row[0]:
                cfg = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                gdays = int(cfg.get('trash_retention_days') or 0)
                if gdays > 0:
                    return gdays
        except Exception:
            pass
        return 7
    finally:
        conn.close()


def trash_counts():
    """라이브러리별 휴지통 파일 수 {library_id: count} — 선택 목록 표시용."""
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT library_id, count(*) FROM media_parts
            WHERE deleted_at IS NOT NULL GROUP BY library_id
        """)
        return {int(r[0]): int(r[1] or 0) for r in cur.fetchall()}
    finally:
        conn.close()


def _trash_load(library_id, progress_cb=None):
    """휴지통 원자료: (라이브러리 종류, 파일 목록, 항목 사전).

    항목 사전은 파일이 속한 항목부터 최상위(아티스트 등)까지 모두 담는다.
    파일이 수천 개여도 쿼리는 항목 묶음 단위로 나눠 실행한다.
    progress_cb가 있으면 읽는 단계마다 진행률(0~45)을 알린다.
    """
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT type FROM libraries WHERE id=%s", (int(library_id),))
        row = cur.fetchone()
        lib_type = (row[0] or 'music') if row else 'music'
        cur.execute("""
            SELECT id, metadata_item_id, file_path, deleted_at, deleted_reason
            FROM media_parts
            WHERE library_id=%s AND deleted_at IS NOT NULL
        """, (int(library_id),))
        parts = [{'id': r[0], 'item_id': r[1], 'path': r[2] or '',
                  'deleted_at': r[3], 'reason': r[4] or ''}
                 for r in cur.fetchall()]
        if progress_cb:
            progress_cb(15)
        items = {}
        frontier = {p['item_id'] for p in parts if p['item_id']}
        level = 0
        while frontier:
            level += 1
            ids = list(frontier)
            frontier = set()
            cur.execute("""
                SELECT id, title, year, metadata_type, parent_id
                FROM metadata_items WHERE id = ANY(%s)
            """, (ids,))
            for r in cur.fetchall():
                items[int(r[0])] = {'id': int(r[0]), 'title': r[1] or '',
                                    'year': r[2], 'type': int(r[3]),
                                    'parent_id': int(r[4]) if r[4] else None}
                if r[4] and int(r[4]) not in items:
                    frontier.add(int(r[4]))
            if progress_cb:
                progress_cb(min(45, 15 + level * 6))
        return lib_type, parts, items
    finally:
        conn.close()


def _trash_root(item_id, items, main_type):
    """항목에서 작품(main) 단위까지 올라간다. 못 찾으면 자기 자신을 돌려준다."""
    cur = items.get(item_id)
    if cur is None:
        return None
    seen = set()
    while (cur['type'] != main_type and cur['parent_id']
           and cur['parent_id'] in items and cur['id'] not in seen):
        seen.add(cur['id'])
        cur = items[cur['parent_id']]
    return cur


def _trash_common_folder(paths):
    """그룹 파일 경로들에서 공통 폴더와 하위 폴더 이름들을 구한다.

    파일이 CD1/CD2 같은 하위 폴더에 나뉘어 있으면 공통 폴더는 작품
    폴더가 된다. 파일이 전부 한 디스크 폴더 안에만 있으면 그 폴더를
    작품 폴더로 올리고 디스크 폴더 이름은 하위 폴더로 돌려준다.
    """
    import os
    import re
    dirs = []
    for p in paths:
        if p and '/' in p:
            dirs.append(p.rsplit('/', 1)[0])
    if not dirs:
        return '', []
    try:
        folder = os.path.commonpath(dirs)
    except ValueError:
        folder = dirs[0]
    subs = set()
    prefix = folder + '/'
    for d in dirs:
        if d == folder:
            continue
        rel = d[len(prefix):] if d.startswith(prefix) else ''
        if rel:
            subs.add(rel.split('/')[0])
    base = os.path.basename(folder)
    if base and re.match(r'^(cd|disc|disk)\s*[-_.]?\s*\d{1,2}$', base, re.I):
        parent = os.path.dirname(folder)
        if parent and parent != folder:
            subs.add(base)
            folder = parent
    return folder, sorted(subs)


def _fmt_dt(dt):
    if dt is None:
        return ''
    try:
        return dt.strftime('%Y-%m-%d %H:%M')
    except Exception:
        return str(dt)


def trash_groups(library_id, page=1, per_page=30, search='', order='old'):
    """휴지통을 작품 단위로 묶어 페이지 단위로 돌려준다.

    order: old(오래된 순) | new(최근 순) | files(파일 많은 순) | name(이름순)
    보관 기간은 shyni 서버가 관리하므로 여기서는 판정하지 않고,
    그룹별 경과일(first_days_ago)과 보관 기간(retention_days) 표시만 한다.
    """
    from datetime import datetime, timezone
    lib_type, parts, items = _trash_load(library_id)
    info = TYPE_INFO.get(lib_type) or TYPE_INFO['music']
    main_type = info['main']
    days = trash_retention_days(library_id)
    now = datetime.now(timezone.utc)
    groups = {}
    for p in parts:
        root = _trash_root(p['item_id'], items, main_type)
        rid = root['id'] if root else p['item_id']
        g = groups.get(rid)
        if g is None:
            parent = items.get(root['parent_id']) if root and root['parent_id'] else None
            g = {'root_id': rid,
                 'title': (root['title'] if root else ''),
                 'year': (root['year'] if root else None),
                 'parent_title': (parent['title'] if parent else ''),
                 'file_count': 0,
                 'first_deleted': None, 'last_deleted': None,
                 'reasons': set(), 'folder': '', 'subs': [], 'paths': []}
            groups[rid] = g
        g['file_count'] += 1
        da = p['deleted_at']
        if da is not None:
            if g['first_deleted'] is None or da < g['first_deleted']:
                g['first_deleted'] = da
            if g['last_deleted'] is None or da > g['last_deleted']:
                g['last_deleted'] = da
        if p['reason']:
            g['reasons'].add(p['reason'])
        if p['path']:
            g['paths'].append(p['path'])
    for g in groups.values():
        g['folder'], g['subs'] = _trash_common_folder(g['paths'])
    total_files = len(parts)
    total_groups = len(groups)
    rows = list(groups.values())
    if search:
        needle = search.lower()
        rows = [g for g in rows
                if needle in (g['parent_title'] + ' ' + g['title']).lower()
                or needle in g['folder'].lower()]
    if order == 'new':
        rows.sort(key=lambda g: g['last_deleted'] or g['first_deleted'],
                  reverse=True)
    elif order == 'files':
        rows.sort(key=lambda g: (-g['file_count'], g['title']))
    elif order == 'name':
        rows.sort(key=lambda g: (g['parent_title'], g['title']))
    else:
        rows.sort(key=lambda g: g['first_deleted'] or g['last_deleted'])
    filtered = len(rows)
    pages = max(1, (filtered + per_page - 1) // per_page)
    page = max(1, min(page, pages))
    chunk = rows[(page - 1) * per_page: page * per_page]
    out = []
    for g in chunk:
        last = g['last_deleted']
        first = g['first_deleted']
        out.append({
            'root_id': g['root_id'], 'title': g['title'], 'year': g['year'],
            'parent_title': g['parent_title'],
            'file_count': g['file_count'],
            'first_deleted': _fmt_dt(first),
            'last_deleted': _fmt_dt(last),
            'days_ago': (now - last).days if last else None,
            'first_days_ago': (now - first).days if first else None,
            'reasons': sorted(g['reasons']),
            'folder': g['folder'],
            'subs': g['subs'],
        })
    return {'lib_type': lib_type, 'main_label': info['main_label'],
            'retention_days': days,
            'summary': {'files': total_files, 'groups': total_groups},
            'total_groups': filtered, 'page': page, 'pages': pages,
            'groups': out}


def trash_compute(library_id, progress_cb=None):
    """휴지통 조회용 전체 계산 — 워커에서 실행해 결과를 캐시에 저장한다.

    trash_groups와 같은 묶음을 만들지만 페이지로 자르지 않고 전부 돌려주며,
    그룹별 파일 목록까지 한 번에 만든다. 웹은 이 결과를 캐시에서 읽어
    검색·정렬·페이지만 처리하므로 조회가 가볍다.
    그룹 행에는 정렬용 epoch(_first·_last)를 함께 넣어 둔다.
    """
    import os
    from datetime import datetime, timezone
    lib_type, parts, items = _trash_load(library_id, progress_cb=progress_cb)
    info = TYPE_INFO.get(lib_type) or TYPE_INFO['music']
    main_type = info['main']
    days = trash_retention_days(library_id)
    now = datetime.now(timezone.utc)
    groups = {}
    files = {}
    total = len(parts)
    for idx, p in enumerate(parts):
        if progress_cb and idx % 200 == 0:
            progress_cb(45 + int(50 * idx / max(1, total)))
        root = _trash_root(p['item_id'], items, main_type)
        rid = root['id'] if root else p['item_id']
        g = groups.get(rid)
        if g is None:
            parent = items.get(root['parent_id']) if root and root['parent_id'] else None
            g = {'root_id': rid,
                 'title': (root['title'] if root else ''),
                 'year': (root['year'] if root else None),
                 'parent_title': (parent['title'] if parent else ''),
                 'file_count': 0,
                 'first_dt': None, 'last_dt': None,
                 'reasons': set(), 'folder': '', 'subs': [], 'paths': []}
            groups[rid] = g
            files[rid] = []
        g['file_count'] += 1
        da = p['deleted_at']
        if da is not None:
            if g['first_dt'] is None or da < g['first_dt']:
                g['first_dt'] = da
            if g['last_dt'] is None or da > g['last_dt']:
                g['last_dt'] = da
        if p['reason']:
            g['reasons'].add(p['reason'])
        if p['path']:
            g['paths'].append(p['path'])
        item = items.get(p['item_id']) or {}
        files[rid].append({'id': p['id'], 'name': os.path.basename(p['path']),
                           'path': p['path'],
                           'item_title': item.get('title') or '',
                           'deleted_at': _fmt_dt(da),
                           'reason': p['reason']})
    out_groups = []
    out_files = {}
    for rid, g in groups.items():
        last = g['last_dt']
        g['folder'], g['subs'] = _trash_common_folder(g['paths'])
        out_groups.append({
            'root_id': rid, 'title': g['title'], 'year': g['year'],
            'parent_title': g['parent_title'],
            'file_count': g['file_count'],
            'first_deleted': _fmt_dt(g['first_dt']),
            'last_deleted': _fmt_dt(last),
            'days_ago': (now - last).days if last else None,
            'first_days_ago': ((now - g['first_dt']).days
                               if g['first_dt'] else None),
            'reasons': sorted(g['reasons']),
            'folder': g['folder'],
            'subs': g['subs'],
            '_first': g['first_dt'].timestamp() if g['first_dt'] else None,
            '_last': last.timestamp() if last else None,
        })
        fl = files[rid]
        fl.sort(key=lambda x: x['path'])
        out_files[str(rid)] = fl
    if progress_cb:
        progress_cb(98)
    return {'lib_type': lib_type, 'main_label': info['main_label'],
            'retention_days': days,
            'summary': {'files': total, 'groups': len(groups)},
            'groups': out_groups, 'files': out_files}


def trash_group_files(library_id, root_id, limit=500):
    """한 작품(묶음)에 속한 휴지통 파일 목록을 돌려준다."""
    import os
    lib_type, parts, items = _trash_load(library_id)
    info = TYPE_INFO.get(lib_type) or TYPE_INFO['music']
    out = []
    for p in parts:
        root = _trash_root(p['item_id'], items, info['main'])
        rid = root['id'] if root else p['item_id']
        if rid != int(root_id):
            continue
        item = items.get(p['item_id']) or {}
        out.append({'id': p['id'], 'name': os.path.basename(p['path']),
                    'path': p['path'], 'item_title': item.get('title') or '',
                    'deleted_at': _fmt_dt(p['deleted_at']),
                    'reason': p['reason']})
    out.sort(key=lambda x: x['path'])
    return {'files': out[:limit], 'total': len(out)}


# ---- 스캔 결과: 추가 파일 목록 ----
# 주기적 스캔·휴지통 작업이 스캔 완료 후에 이력에 남길 추가 파일 목록을
# Relay 경유로 조회한다. Relay가 꺼져 있으면 조회하지 않고 개수까지만 둔다.
# 1차: 완료된 job의 stats.analyze.metadata_item_ids → media_parts 조인으로
# 경로를 구한다 (이 job이 분석한 항목만 정확히 가져온다).
# 2차: 1차가 비어 있고 신규가 있으면, 스캔 시작 전에 기록해 둔
# MAX(media_parts.id)보다 큰 id의 행을 읽는다 (plex mate와 같은 방식).

APPEND_MAX_FILES = 1000


def parts_max_id():
    """media_parts.id의 현재 최댓값 — 스캔 시작 전 2차 방법용 기준점."""
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT MAX(id) FROM media_parts')
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    finally:
        conn.close()


def paths_by_metadata_item_ids(item_ids, library_id=None):
    """분석된 항목 id 목록에 속한 파일 경로를 media_parts에서 읽는다."""
    ids = [int(i) for i in (item_ids or []) if i is not None]
    if not ids:
        return []
    conn = connect()
    try:
        cur = conn.cursor()
        out = []
        for i in range(0, len(ids), 500):
            part = ids[i:i + 500]
            sql = ('SELECT file_path FROM media_parts '
                   f"WHERE metadata_item_id IN ({','.join(['%s'] * len(part))}) "
                   'AND deleted_at IS NULL ')
            args = list(part)
            if library_id is not None:
                sql += 'AND library_id = %s '
                args.append(int(library_id))
            sql += 'ORDER BY file_path'
            cur.execute(sql, args)
            out.extend(r[0] for r in cur.fetchall() if r and r[0])
        return out
    finally:
        conn.close()


def count_parts_newer_than(max_id, library_id):
    """기준 id보다 나중에 생긴 파일 수 (2차 방법용 전체 수)."""
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT count(*) FROM media_parts '
                    'WHERE library_id = %s AND id > %s AND deleted_at IS NULL',
                    (int(library_id), int(max_id)))
        return int(cur.fetchone()[0] or 0)
    finally:
        conn.close()


def paths_newer_than(max_id, library_id, limit=APPEND_MAX_FILES):
    """기준 id보다 나중에 생긴 파일 경로 (2차 방법용, 저장 상한까지만)."""
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute('SELECT file_path FROM media_parts '
                    'WHERE library_id = %s AND id > %s AND deleted_at IS NULL '
                    'ORDER BY id LIMIT %s',
                    (int(library_id), int(max_id), int(limit)))
        return [r[0] for r in cur.fetchall() if r and r[0]]
    finally:
        conn.close()


def limit_append_files(paths, limit=APPEND_MAX_FILES):
    """추가 파일 목록 저장 상한 — 순수 함수(하니스 검증용)."""
    return list(paths or [])[:max(0, int(limit))]


def collect_append_paths(library_id, metadata_item_ids, max_id_before, stat_new,
                         limit=APPEND_MAX_FILES):
    """추가 파일 목록을 수집한다. (파일 경로 목록, 전체 수)를 돌려준다.

    파일 목록이 없으면 (None, None) — Relay가 꺼져 있거나 수집할 방법이
    없을 때다. 신규가 없으면 ([], 0)이다. stat_new이 None이면 신규 수를
    모른다는 뜻(도서 스캔은 shyni stats에 신규 키가 없음)이라 1·2차
    방법으로 직접 세어 본다.
    """
    if not db_ready():
        return None, None
    # stat_new이 None이면 '모름' — 도서 스캔은 shyni stats에 신규 키가
    # 없어서 1·2차 방법으로 직접 세어 본다. 명시적 0 이하일 때만
    # 조기 반환한다.
    if stat_new is not None:
        try:
            stat_new = int(stat_new)
        except Exception:
            stat_new = 0
        if stat_new <= 0:
            return [], 0
    # 1차: 스캔 시작 전 기준 id보다 나중에 생긴 파일 — 실제로 추가된 파일이다
    if max_id_before is not None:
        count = count_parts_newer_than(max_id_before, library_id)
        if count <= 0:
            return [], 0
        return (limit_append_files(paths_newer_than(max_id_before, library_id, limit),
                                   limit),
                count)
    # 2차: 기준 id가 없을 때만 이 job이 분석한 항목 id로 경로를 구한다.
    # 분석은 기존 항목도 다시 할 수 있어(앞선 스캔이 끝나지 못하고 분석을
    # 못 받은 경우 등) 추가와 다를 수 있으니, 기준이 있을 때는 쓰지 않는다.
    ids = [i for i in (metadata_item_ids or []) if i is not None]
    if ids:
        paths = paths_by_metadata_item_ids(ids, library_id=library_id)
        if paths:
            return limit_append_files(paths, limit), len(paths)
    return None, None


def group_append_paths(paths):
    """추가 파일 경로를 앨범(폴더) 단위로 묶는다 — 순수 함수(하니스 검증용).

    반환: [{'folder': 폴더 전체 경로, 'name': 폴더 이름,
            'count': 파일 수, 'files': [파일 이름...]}] (폴더 이름순)
    """
    import os
    groups = {}
    for p in paths or []:
        if not p:
            continue
        folder = p.rsplit('/', 1)[0] if '/' in p else ''
        groups.setdefault(folder, []).append(os.path.basename(p) or p)
    out = []
    for folder in sorted(groups):
        files = sorted(groups[folder])
        out.append({'folder': folder,
                    'name': os.path.basename(folder) or folder,
                    'count': len(files), 'files': files})
    return out
