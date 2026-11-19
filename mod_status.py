import json
import os
import shutil
import subprocess
import threading
import time
from datetime import timedelta, timezone

import requests

from .setup import *
from .shyni_backend import ShyniBackend
from .task_status import TaskStatus

name = 'status'
logger = P.logger

# 라이브러리 전체 메타 새로고침 실행 스레드 (key -> Thread). 같은 프로세스에서만 유효하다.
_meta_refresh_threads = {}
_vfs_scan_threads = {}
_trash_chain_threads = {}
# 카드 스캔 시작 직렬화 — 동시에 누른 카드 스캔이 게이트 검사를 함께
# 통과해 자리를 초과하는 틈을 줄인다. 같은 프로세스 안에서만 유효하고,
# Celery 워커(주기·휴지통)와는 공유되지 않아 프로세스 간 틈은 남는다.
_library_scan_gate_lock = threading.Lock()


class ModuleStatus(PluginModuleBase):
    """현황 — 라이브러리 목록과 라이브러리별 VFS 새로고침/스캔 실행·상태.

    섹션 목록·경로는 shyni 토큰 API(GET /library/sections)에서 가져온다.
    마지막 스캔·파일수·신규·missing은 Relay가 켜져 있으면 shyni DB의
    compat_jobs에서 읽고, 꺼져 있으면 주기 스캔 이력으로 채운다.
    이력이 없는 라이브러리는 '스캔 후 내용이 반영됩니다'로 표시한다.
    작업 상태는 이 플러그인이 시작한 작업만 섹션별로 기록해 표시한다.
    스캔 취소는 토큰으로 불가능하다(DELETE /libraries/{id}/scan은 관리자 전용 401).
    VFS 새로고침 취소는 rclone RC job/stop으로 가능하다.
    """

    def __init__(self, P):
        super(ModuleStatus, self).__init__(P, name=name, first_menu='list')
        self.db_default = {
            f"{self.name}_db_version": "1",
            f"{self.name}_auto_refresh_min": "30",
            f"{self.name}_hidden_libs": "",
            f"{self.name}_health_hidden_libs": "",
            f"{self.name}_lib_sort": "default",
            f"{self.name}_health_sort": "default",
        }

    def plugin_load(self):
        # '표시' 숨김 목록을 대시보드·건강도 개별 저장으로 나누는 1회 이전:
        # 지금까지 함께 쓰던 목록을 건강도 쪽에도 복사해 두 화면의 현재 선택을 그대로 잇는다.
        try:
            if P.ModelSetting.get('status_hidden_split') != '1':
                P.ModelSetting.set('status_health_hidden_libs',
                                   P.ModelSetting.get('status_hidden_libs') or '')
                P.ModelSetting.set('status_hidden_split', '1')
        except Exception as e:
            logger.warning(f'[Status] 숨김 목록 이전 실패: {e}')
        # DB 통계 계산은 Celery 워커(task_status)가 전담한다 — 웹은 요청만 남긴다
        def func():
            self.start_celery(TaskStatus.start)
        thread = threading.Thread(target=func, args=())
        thread.daemon = True
        thread.start()

    # ---------- shyni 서버 정보 (헬스체크·용량) ----------
    @staticmethod
    def _health():
        """GET /health/ready — 인증 없이 버전과 구성 요소 상태를 알려준다."""
        url, _ = ShyniBackend.get_config()
        if not url:
            return None
        try:
            res = requests.get(f'{url}/health/ready', timeout=5)
            if res.status_code == 200:
                return res.json()
        except Exception:
            pass
        return None

    _size_lock = threading.Lock()
    # 용량 측정 중 표시 — 측정 스레드가 돌고 있는 동안 참이다.
    _size_measuring = False

    @classmethod
    def _start_measure_data_size(cls, path):
        """데이터 폴더 용량 측정을 시작한다. 이미 측정 중이면 False를 돌려준다."""
        if not cls._size_lock.acquire(False):
            return False
        cls._size_measuring = True
        try:
            threading.Thread(target=cls._measure_data_size,
                             args=(path,), daemon=True).start()
        except Exception:
            cls._size_measuring = False
            cls._size_lock.release()
            raise
        return True

    @classmethod
    def _storage(cls):
        """데이터 폴더가 있는 디스크의 사용량 + 데이터 폴더 용량(캐시).

        폴더 용량 측정은 du 전체 순회라 디스크를 오래 붙잡으므로,
        자동 측정은 하지 않는다 — 사용자가 새로고침 아이콘을 눌렀을
        때만 잰다. (이전에는 캐시가 없을 때 첫 대시보드 접속에서
        자동으로 측정했는데, 첫 접속을 무겁게 만드는 원인 중 하나였다.)
        """
        path = (P.ModelSetting.get('base_shyni_data_path') or '').strip()
        info = {'path': path, 'measuring': cls._size_measuring}
        if not path or not os.path.isdir(path):
            return info
        try:
            du = shutil.disk_usage(path)
            info.update({'disk_total': du.total, 'disk_used': du.used,
                         'disk_free': du.free})
        except Exception:
            pass
        try:
            info['data_size'] = int(P.ModelSetting.get('status_data_size') or 0) or None
            info['data_size_at'] = int(P.ModelSetting.get('status_data_size_at') or 0)
        except Exception:
            info['data_size'] = None
            info['data_size_at'] = 0
        raw_bd = P.ModelSetting.get('status_data_breakdown')
        if raw_bd:
            try:
                bd = json.loads(raw_bd)
                if isinstance(bd, list):
                    info['breakdown'] = bd
            except Exception:
                pass
        return info

    # 데이터 폴더 하위 폴더를 유형별로 묶는 규칙 (라벨, 폴더 이름들)
    _SIZE_CATEGORIES = (
        ('데이터베이스', ('postgres',)),
        ('검색 인덱스', ('meilisearch', 'meilisearch-dumps', 'meilisearch-snapshots')),
        ('캐시', ('cache', 'rclone-cache')),
    )

    @classmethod
    def _measure_data_size(cls, path):
        try:
            res = subprocess.run(['du', '-sb', path],
                                 capture_output=True, text=True, timeout=900)
            size = int(res.stdout.split()[0])
            P.ModelSetting.set('status_data_size', str(size))
            P.ModelSetting.set('status_data_size_at', str(int(time.time())))
            # 유형별(하위 폴더) 용량도 함께 잰다
            try:
                sub_sizes = {}
                for name in os.listdir(path):
                    sub = os.path.join(path, name)
                    if os.path.isdir(sub):
                        r = subprocess.run(['du', '-sb', sub],
                                           capture_output=True, text=True, timeout=300)
                        sub_sizes[name] = int(r.stdout.split()[0])
                breakdown = []
                used = 0
                for label, names in cls._SIZE_CATEGORIES:
                    s = sum(sub_sizes.get(n, 0) for n in names)
                    if s > 0:
                        breakdown.append({'label': label, 'bytes': s})
                        used += s
                rest = size - used
                if rest > 0:
                    breakdown.append({'label': '기타', 'bytes': rest})
                P.ModelSetting.set('status_data_breakdown',
                                   json.dumps(breakdown, ensure_ascii=False))
            except Exception as e:
                logger.error(f'유형별 용량 측정 실패: {e}')
        except Exception as e:
            logger.error(f'데이터 폴더 용량 측정 실패: {e}')
        finally:
            cls._size_measuring = False
            cls._size_lock.release()

    @staticmethod
    def _refresh_min():
        try:
            return max(0, min(120, int(P.ModelSetting.get('status_auto_refresh_min') or 30)))
        except Exception:
            return 1

    # ---------- 섹션별 작업 상태 저장 ----------
    @staticmethod
    def _state_key(key):
        return f'status_sec_{key}'

    @classmethod
    def _get_state(cls, key):
        raw = P.ModelSetting.get(cls._state_key(key))
        if raw:
            try:
                state = json.loads(raw)
                if isinstance(state, dict):
                    return state
            except Exception:
                pass
        return {'vfs': None, 'scan': None}

    @classmethod
    def _set_state(cls, key, state):
        P.ModelSetting.set(cls._state_key(key), json.dumps(state, ensure_ascii=False))

    # ---------- shyni 섹션 ----------
    # 라이브러리 목록 캐시 — 목록은 자주 바뀌지 않으니 5분간 재사용해
    # 페이지가 열릴 때마다 shyni API를 기다리지 않게 한다. 캐시는
    # ModelSetting에 두어 웹·Celery 프로세스가 함께 쓴다.
    SECTIONS_CACHE_KEY = 'status_sections_cache'
    SECTIONS_CACHE_TTL = 300

    @staticmethod
    def _sections_cache_read():
        try:
            raw = P.ModelSetting.get(ModuleStatus.SECTIONS_CACHE_KEY)
            if not raw:
                return None, 0
            data = json.loads(raw)
            cached = data.get('data')
            ts = data.get('ts') or 0
            if isinstance(cached, list):
                return cached, ts
        except Exception:
            pass
        return None, 0

    @staticmethod
    def _sections():
        cached, ts = ModuleStatus._sections_cache_read()
        if cached is not None and time.time() - ts < ModuleStatus.SECTIONS_CACHE_TTL:
            return cached, None
        url, token = ShyniBackend.get_config()
        if not url or not token:
            return None, '기본 설정에서 shyni 주소와 토큰을 먼저 설정해 주세요.'
        try:
            res = requests.get(f'{url}/library/sections',
                               headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                               timeout=15)
            res.raise_for_status()
            dirs = res.json().get('MediaContainer', {}).get('Directory', [])
        except Exception as e:
            # 새로 읽지 못해도 낡은 목록이 있으면 그걸 쓴다 — 목록이
            # 잠깐 안 뜬다고 페이지를 막을 이유가 없다.
            if cached is not None:
                return cached, None
            return None, f'shyni 라이브러리 목록을 가져오지 못했습니다: {e}'
        out = []
        for d in dirs:
            locs = d.get('locations') or []
            out.append({
                'key': d.get('key'),
                'title': d.get('title'),
                'type': d.get('type'),
                'path': locs[0].get('path') if locs else '',
                # 연결된 폴더 전 목록 — 대시보드에서 폴더를 클릭하면 펼친다.
                # 'path'(첫 번째)는 다른 곳에서 쓰므로 그대로 둔다.
                'paths': [loc.get('path') for loc in locs if loc.get('path')],
            })
        try:
            P.ModelSetting.set(ModuleStatus.SECTIONS_CACHE_KEY,
                               json.dumps({'ts': time.time(), 'data': out}, ensure_ascii=False))
        except Exception:
            pass
        return out, None

    # ---------- shyni DB에서 스캔 이력 읽기 ----------
    # 로그 파싱은 0.9.9에서 뺐다 — beta.13은 [scan complete] 기록을 아예
    # 남기지 않아 로그를 읽어도 결과가 없고, 수십 MB를 읽는 동안 웹이
    # 멈추기만 한다 (2026-11-18 사용자 결정). Relay가 켜져 있으면 DB
    # compat_jobs가 정본이고, 꺼져 있으면 빈 기록을 돌려주고 주기 스캔
    # 이력 폴백(_fill_last_scan_from_periodic)이 카드를 채운다.
    @staticmethod
    def _to_kst(ts_raw):
        try:
            dt = datetime.strptime(ts_raw[:19], '%Y-%m-%dT%H:%M:%S')
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone(timedelta(hours=9))).strftime('%m-%d %H:%M')
        except Exception:
            return ts_raw[:16].replace('T', ' ')

    @classmethod
    def _db_scan_history(cls):
        """shyni DB의 compat_jobs에서 라이브러리별 최신 전체 스캔 기록을 읽는다.

        전체 스캔은 payload.path가 없는 scan job이다 (부분 스캔은 제외).
        반환: {섹션 key(str): rec} — rec 형태는 로그 기록과 같다.
        DB를 활용하지 않는 설정이면 빈 dict를 돌려준다.
        """
        try:
            from . import db_support
            if not db_support.db_ready():
                return {}
            conn = db_support.connect(timeout=10)
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT DISTINCT ON (payload->>'section') payload->>'section', status, finished_at, stats "
                    "FROM compat_jobs "
                    "WHERE kind='scan' AND status IN ('completed','failed') "
                    "AND payload->>'path' IS NULL AND finished_at IS NOT NULL "
                    "ORDER BY payload->>'section', finished_at DESC")
                rows = cur.fetchall()
            finally:
                conn.close()
        except Exception as e:
            logger.error(f'DB 스캔 이력 읽기 실패: {e}')
            return {}
        out = {}
        for sec, status, finished_at, stats in rows:
            if sec is None or finished_at is None:
                continue
            try:
                ts = finished_at.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            except Exception:
                continue
            stats = stats if isinstance(stats, dict) else {}
            rec = {'result': 'done' if status == 'completed' else 'failed', 'time': ts}
            for k in ('total', 'new', 'missing', 'registered'):
                v = stats.get(k)
                rec[k] = int(v) if isinstance(v, (int, float)) else None
            # 도서 스캔 stats는 total/missing 키가 없고
            # entries/missing_files 키를 쓴다. 신규 키는 없어서
            # 도서 신규는 '-'로 남는다.
            if rec['total'] is None:
                v = stats.get('entries')
                rec['total'] = int(v) if isinstance(v, (int, float)) else None
            if rec['missing'] is None:
                v = stats.get('missing_files')
                rec['missing'] = int(v) if isinstance(v, (int, float)) else None
            rec['time_text'] = cls._to_kst(ts)
            out[str(sec)] = rec
        return out

    @classmethod
    def _scan_history(cls):
        """라이브러리별 최신 스캔 기록을 만든다. (기록, 파일수)

        Relay가 켜져 있으면 shyni DB compat_jobs가 정본이다.
        Relay가 꺼져 있으면 빈 기록을 돌려준다 — 로그 파싱은 0.9.9에서
        뺐다. beta.13은 [scan complete] 기록을 아예 남기지 않아 로그를
        읽어도 결과가 없고, 수십 MB를 읽는 동안 웹이 멈추기만 한다
        (2026-11-18 사용자 결정). 빈 카드는 주기 스캔 이력 폴백
        (_fill_last_scan_from_periodic)이 채운다.
        두 번째 값은 예전 로그 파일 수 자리 — 항상 0이다.
        """
        try:
            from . import db_support
            if db_support.db_ready():
                return cls._db_scan_history(), 0
        except Exception:
            pass
        return {}, 0

    @classmethod
    def _fill_book_new(cls, rows):
        """도서 카드의 신규(last_scan['new'])를 실측값으로 채운다.

        shyni 도서 스캔 stats에는 신규에 대응하는 키가 없어 last_scan의
        신규가 항상 비어 있다 ('-'). 대신 이 플러그인이 직접 잰 값을
        쓴다 — 주기 스캔 이력의 stat_new과 카드 스캔 측정값 중 더 최신인
        쪽을 쓴다 (2026-11-13 사용자 결정: 카드 스캔도 주기 스캔과 같은
        방식으로 신규를 잰다).
        - 값이 0이면 0으로 표시한다 (실측된 0건 — NULL과 다르다).
        - 두 출처 모두 값이 없으면 비워 둔다 ('-' 유지 — 모른다는
          뜻이라 0으로 속이면 안 된다).
        - last_scan의 다른 값(파일수·missing·시각)은 건드리지 않는다.
        - 신규 값이 last_scan보다 오래된 스캔에서 올 수 있다 —
          시각과 값의 출처가 달라질 수 있음을 받아들인 동작이다.
        - 도서 외 종류는 shyni stats에 new 키가 있어 건드리지 않는다.
        채운 값에는 _new_from_periodic 또는 _new_from_card 표시를 남긴다 —
        캐시 응답 때 다시 채울 때 우리 채움만 갈아 끼우거나 정리하고,
        수동 이력 같은 다른 출처의 값은 건드리지 않기 위해서다. 목록을
        새로 만들 때와 캐시로 답할 때 양쪽에서 부른다.
        """
        targets = {}
        for sec in rows:
            if (sec.get('type') or '') != 'book':
                continue
            ls = sec.get('last_scan')
            if not ls:
                continue
            targets[str(sec['key'])] = ls
        if not targets:
            return
        try:
            from .model_periodic import ModelPeriodicItem
            periodic = ModelPeriodicItem.latest_stat_new_time_map(list(targets))
        except Exception as e:
            logger.warning(f'[Status] 도서 신규 조회 실패: {e}')
            return
        try:
            card = cls._card_scan_new_read()
        except Exception:
            card = {}
        for key, ls in targets.items():
            ours = bool(ls.get('_new_from_periodic') or ls.get('_new_from_card'))
            # 두 출처 중 더 최신인 쪽을 쓴다 — 동점이면 카드 측정값을 쓴다.
            p = periodic.get(key)
            c = card.get(key)
            winner = None
            if c is not None and (p is None or c.get('at', 0) >= p.get('at', 0)):
                winner = ('card', c['new'])
            elif p is not None:
                winner = ('periodic', p['new'])
            if winner is not None:
                # 비어 있거나 우리 채움이면 갈아 끼우고, 수동 이력 같은
                # 다른 출처의 값은 건드리지 않는다.
                if ls.get('new') is None or ours:
                    src, val = winner
                    ls['new'] = val
                    if src == 'card':
                        ls['_new_from_card'] = True
                        ls.pop('_new_from_periodic', None)
                    else:
                        ls['_new_from_periodic'] = True
                        ls.pop('_new_from_card', None)
            elif ours:
                # 이전에 채웠던 값이 있었는데 두 출처 모두 사라졌으면
                # 다시 비운다 — 모른다는 뜻이다.
                ls['new'] = None
                ls.pop('_new_from_periodic', None)
                ls.pop('_new_from_card', None)

    @classmethod
    def _fill_last_scan_from_periodic(cls, rows):
        """스캔 이력이 없는 카드의 last_scan을 주기 스캔 이력으로 채운다.

        Relay가 꺼져 있으면 _scan_history가 빈 기록을 돌려준다 —
        로그 파싱은 0.9.9에서 뺐다 (beta.13은 [scan complete] 기록을
        아예 남기지 않아, 2026-11-17 실측: 로그 46MB 전수 0건).
        그래서 이력이 없는 카드만 플러그인 자체 기록(주기 스캔 이력의
        가장 최근 완료 행)으로 채운다. 주기 스캔 작업 결과 페이지에
        나오는 수치와 같은 값이다. 대시보드에서 누른 스캔·새로고침도
        주기 스캔 이력에 남으니 카드에 반영된다.
        - DB·수동 이력이 있는 카드는 건드리지 않는다.
        - 채운 기록에는 '_from': 'periodic' 표시를 남긴다 — 캐시 응답 때
          우리 채움만 갈아 끼우거나 정리하고, 다른 출처의 값은 건드리지
          않기 위해서다 (_fill_book_new와 같은 방식).
        - 주기 이력이 사라지면(기록 삭제 등) 우리 채움도 다시 비운다.
        - 도서는 stat_total이 entries(이번 스캔 처리 수)일 수 있고
          stat_new이 비어 있을 수 있다 — 템플릿이 도서에는 '처리' 라벨을
          쓰고 new가 없으면 '-'로 표시한다.
        목록을 새로 만들 때와 캐시로 답할 때 양쪽에서 부른다.
        """
        targets = {}
        for sec in rows:
            ls = sec.get('last_scan')
            if ls is None or (isinstance(ls, dict)
                              and ls.get('_from') == 'periodic'):
                targets[str(sec['key'])] = sec
        if not targets:
            return
        try:
            from .model_periodic import ModelPeriodicItem
            stats = ModelPeriodicItem.latest_done_stats_map(list(targets))
        except Exception as e:
            logger.warning(f'[Status] 주기 스캔 이력 조회 실패: {e}')
            return
        for key, sec in targets.items():
            rec = stats.get(key)
            if rec is None:
                # 우리 채움이 있었는데 이력이 사라졌으면 다시 비운다.
                if sec.get('last_scan') is not None:
                    sec['last_scan'] = None
                continue
            finish = rec.get('finish')
            try:
                time_text = finish.strftime('%m-%d %H:%M') \
                    if finish is not None else ''
            except Exception:
                time_text = ''
            try:
                time_iso = finish.isoformat() if finish is not None else ''
            except Exception:
                time_iso = ''
            sec['last_scan'] = {
                'result': 'done',
                'time': time_iso,
                'time_text': time_text,
                'total': rec.get('total'),
                'new': rec.get('new'),
                'missing': rec.get('missing'),
                '_from': 'periodic',
            }

    # 카드 스캔 신규 측정값 저장소 (ModelSetting JSON, 주기 이력과 별개).
    # {섹션 key(str): {'new': int, 'at': float(epoch), 'job': str|None}}
    _CARD_NEW_KEY = 'status_card_scan_new'
    _card_new_lock = threading.Lock()

    # 대시보드에서 누른 동작의 이력 작업명 — 주기적·휴지통 이력에
    # '대시보드 (수동)'으로 남는다 (모드 manual이라 '(수동)'은 화면에서 붙는다).
    _DASHBOARD_JOB_NAME = '대시보드'

    @classmethod
    def _dashboard_row_create(cls, key, title, action, trash=False, shyni_job_id=None):
        """대시보드 동작의 이력 행을 만든다 — 주기적 스캔 이력(periodic_item)
        또는 휴지통 스캔 이력(trash_item). 성공하면 행 id, 실패하면 None.
        이력 실패가 동작 자체를 막지 않는다. 행 id는 현황 슬롯
        (vfs/scan의 history_row_id)에 남겨 완료 때 끝낸다."""
        try:
            if trash:
                from .model_trash import ModelTrashItem as Model
            else:
                from .model_periodic import ModelPeriodicItem as Model
            with F.app.app_context():
                item = Model(cls._DASHBOARD_JOB_NAME, key, title, 'manual', action)
                if shyni_job_id:
                    item.shyni_job_id = str(shyni_job_id)
                item.save()
                return item.id
        except Exception as e:
            logger.warning(f'[현황] 대시보드 이력 행 만들기 실패: {e}')
            return None

    @classmethod
    def _dashboard_row_finish(cls, row_id, status, message=None, trash=False,
                              job=None, job_id=None, vfs_duration=None,
                              scan_duration=None, pre_max_id=None):
        """대시보드 이력 행을 끝낸다. 이미 끝난 행이면 건드리지 않는다 —
        완료 감지와 실행 스레드가 둘 다 끝내려 해도 한 번만 끝난다.
        job이 있으면 스캔 통계를 채운다 (주기적 스캔과 같은 방식)."""
        if not row_id:
            return
        try:
            if trash:
                from .model_trash import ModelTrashItem as Model
            else:
                from .model_periodic import ModelPeriodicItem as Model
            with F.app.app_context():
                item = (F.db.session.query(Model)
                        .filter(Model.id == row_id).first())
                if item is None or item.status != 'running':
                    return
                if job is not None and not trash:
                    from .task_periodic import apply_scan_result
                    stats = job.get('stats') if isinstance(job, dict) else None
                    apply_scan_result(item, job_id, stats, vfs_duration,
                                      scan_duration, pre_max_id,
                                      item.section_key, label='대시보드')
                else:
                    if vfs_duration is not None:
                        item.vfs_duration = vfs_duration
                    if scan_duration is not None:
                        item.scan_duration = scan_duration
                item.finish(status, message)
        except Exception as e:
            logger.warning(f'[현황] 대시보드 이력 행 끝내기 실패: {e}')

    @classmethod
    def _dashboard_row_set_vfs_duration(cls, row_id, seconds):
        """대시보드 이력 행에 VFS 소요만 저장한다 (스캔이 이어질 때)."""
        if not row_id or seconds is None:
            return
        try:
            from .model_periodic import ModelPeriodicItem
            with F.app.app_context():
                item = (F.db.session.query(ModelPeriodicItem)
                        .filter(ModelPeriodicItem.id == row_id).first())
                if item is None or item.status != 'running':
                    return
                item.vfs_duration = seconds
                item.save()
        except Exception as e:
            logger.warning(f'[현황] 대시보드 이력 VFS 소요 저장 실패: {e}')

    @classmethod
    def _dashboard_row_set_scan_job(cls, row_id, job_id):
        """대시보드 이력 행에 shyni 작업 번호를 남긴다 — 재시작 후 복구가
        이 번호로 스캔 완료를 이어서 확인할 수 있다 (주기적 스캔과 같은 방식)."""
        if not row_id or not job_id:
            return
        try:
            from .model_periodic import ModelPeriodicItem
            with F.app.app_context():
                item = (F.db.session.query(ModelPeriodicItem)
                        .filter(ModelPeriodicItem.id == row_id).first())
                if item is None or item.status != 'running':
                    return
                item.shyni_job_id = str(job_id)
                item.save()
        except Exception as e:
            logger.warning(f'[현황] 대시보드 이력 작업 번호 저장 실패: {e}')

    @staticmethod
    def _slot_seconds(started_at, finished_at=None):
        """현황 슬롯 시각 두 개 사이의 초 — 읽을 수 없으면 None.
        finished_at이 없으면 지금을 끝으로 본다."""
        try:
            t0 = datetime.strptime(started_at or '', '%Y-%m-%d %H:%M:%S').timestamp()
            if finished_at:
                t1 = datetime.strptime(finished_at, '%Y-%m-%d %H:%M:%S').timestamp()
            else:
                t1 = time.time()
            return max(0, int(t1 - t0))
        except Exception:
            return None

    @classmethod
    def _card_scan_new_read(cls):
        """카드 스캔 신규 측정값 저장소를 읽는다.
        깨진 값은 무시한다 — 목록 응답을 망가뜨리지 않기 위해서다."""
        try:
            raw = P.ModelSetting.get(cls._CARD_NEW_KEY)
            data = json.loads(raw) if raw else {}
        except Exception:
            return {}
        if not isinstance(data, dict):
            return {}
        out = {}
        for k, v in data.items():
            if not isinstance(v, dict):
                continue
            try:
                out[str(k)] = {'new': int(v['new']),
                               'at': float(v.get('at') or 0),
                               'job': v.get('job')}
            except Exception:
                continue
        return out

    @classmethod
    def _card_scan_new_save(cls, key, value, at, job_id):
        """카드 스캔 신규 측정값을 저장한다 — 다른 라이브러리 값은 그대로 둔다."""
        with cls._card_new_lock:
            try:
                data = cls._card_scan_new_read()
                data[str(key)] = {'new': int(value), 'at': float(at),
                                  'job': (str(job_id) if job_id is not None else None)}
                P.ModelSetting.set(cls._CARD_NEW_KEY,
                                   json.dumps(data, ensure_ascii=False))
            except Exception as e:
                logger.warning(f'[현황] 카드 스캔 신규 저장 실패: {e}')

    @classmethod
    def _card_scan_pre_max_id(cls):
        """카드 스캔 전송 시점의 media_parts 최대 id — 신규 측정 기준점.
        주기 스캔이 스캔 전에 기록하는 것과 같은 값이다. Relay가 꺼져
        있거나 조회가 실패하면 None을 돌려준다 (측정 안 함)."""
        try:
            from . import db_support
            if not db_support.db_ready():
                return None
            v = db_support.parts_max_id()
            return int(v) if v is not None else None
        except Exception as e:
            logger.warning(f'[현황] 카드 스캔 기준점 기록 실패: {e}')
            return None

    @classmethod
    def _maybe_measure_card_scan_new(cls, key, scan, job):
        """카드 스캔이 완료됐으면 신규 측정 스레드를 띄운다.
        pre_max_id가 있는 카드 스캔 기록만 대상이다 (주기 스캔 기록에는
        pre_max_id가 없고 source='periodic'이라 걸러진다). job stats에
        new 키가 있으면 수확 경로(_db_scan_history)가 이미 표시하므로
        측정하지 않는다 — 도서처럼 new 키가 없는 경우만 잰다. 측정은
        백그라운드 스레드에서 해서 목록 응답을 막지 않는다."""
        pre = scan.get('pre_max_id')
        if pre is None:
            return False
        if scan.get('source') == 'periodic':
            return False
        stats = (job or {}).get('stats') or {}
        if not isinstance(stats, dict):
            stats = {}
        if stats.get('new') is not None:
            return False
        try:
            ids = (stats.get('analyze') or {}).get('metadata_item_ids') or []
        except Exception:
            ids = []
        th = threading.Thread(target=cls._measure_card_scan_new,
                              args=(key, scan.get('job'), pre, ids, time.time()),
                              daemon=True)
        th.start()
        return True

    @classmethod
    def _measure_card_scan_new(cls, key, job_id, pre_max_id, analyze_ids, at):
        """카드 스캔 신규 측정 (백그라운드 스레드) — 주기 스캔과 동일 방식.
        collect_append_paths를 재사용한다. 결과는 별도 저장소에 남기고
        주기 이력은 건드리지 않는다."""
        try:
            from . import db_support
            if not db_support.db_ready():
                return
            _files, count = db_support.collect_append_paths(
                int(key), analyze_ids or [], pre_max_id, None)
            if count is None:
                return
            cls._card_scan_new_save(key, count, at, job_id)
            logger.info(f'[현황] 카드 스캔 신규 측정: 섹션 {key} 신규 {count}건 (job {job_id})')
        except Exception as e:
            logger.warning(f'[현황] 카드 스캔 신규 측정 실패: 섹션 {key} — {e}')

    # ---------- rclone RC ----------
    @staticmethod
    def _rc_target(path):
        """라이브러리 경로 → (RC 주소, remote dir). scan_vfs_change_rule 첫 일치 규칙."""
        for rule in P.ModelSetting.get_list('scan_vfs_change_rule'):
            tmps = rule.split('|')
            if len(tmps) < 3:
                continue
            src, dst, addr = tmps[0].strip(), tmps[1].strip(), tmps[2].strip()
            addr = addr.split('#')[0].strip()
            if src and path.startswith(src):
                remote = (dst + path[len(src):]).replace('\\', '/').lstrip('/')
                return addr, remote
        return None, None

    @staticmethod
    def _rc(addr, endpoint, payload):
        res = requests.post(f'http://{addr}/{endpoint}', json=payload, timeout=20)
        res.raise_for_status()
        return res.json()

    # ---------- 실행 중 작업 상태 갱신 ----------
    @staticmethod
    def _stuck_timeout_min(record):
        """진행 중 표시의 시간 제한(분) — 기록의 source별로 설정을 읽는다.

        source가 'periodic'이면 periodic_stuck_timeout_min, 'trash'이면
        trash_stuck_timeout_min, 없거나 그 외면 scan_stuck_timeout_min을
        읽는다. 설정이 비어 get_int이 None을 돌려주면 기본값 30분을 쓴다.
        0이면 시간 제한을 끈다.
        """
        source = (record or {}).get('source')
        key = {'periodic': 'periodic_stuck_timeout_min',
               'trash': 'trash_stuck_timeout_min'}.get(source, 'scan_stuck_timeout_min')
        try:
            val = P.ModelSetting.get_int(key)
        except Exception:
            val = None
        if val is None:
            return 30
        return val

    @staticmethod
    def _vfs_timeout_min(source=None):
        """VFS 새로고침 제한 시간(분) — 휴지통 쪽이면 휴지통 설정,
        그 외(주기적·대시보드)는 주기적 설정을 쓴다. 없거나 0 이하면
        기본 360분으로 동작한다."""
        key = ('trash_vfs_timeout_min' if source == 'trash'
               else 'periodic_vfs_timeout_min')
        try:
            val = P.ModelSetting.get_int(key)
        except Exception:
            val = None
        if val is None or val <= 0:
            return 360
        return val

    @staticmethod
    def _started_over_min(record, minutes):
        """started_at이 minutes분보다 오래됐으면 True. 0 이하면 끔,
        시각을 읽을 수 없으면 False를 돌려준다."""
        if not minutes or minutes <= 0:
            return False
        try:
            started_ts = datetime.strptime(record.get('started_at') or '',
                                           '%Y-%m-%d %H:%M:%S').timestamp()
        except Exception:
            return False
        return bool(started_ts) and time.time() - started_ts > minutes * 60

    @classmethod
    def _refresh_state(cls, key, state):
        # classmethod — 워커의 목록 재구축(build_list_data)에서도 쓴다.
        # 인스턴스 상태를 쓰지 않아 클래스 메서드로 바꿔도 동작이 같다.
        changed = False
        vfs = state.get('vfs')
        if vfs and vfs.get('status') == 'running' and vfs.get('job') is not None:
            addr, _ = cls._rc_target(vfs.get('path', ''))
            if addr:
                try:
                    job = cls._rc(addr, 'job/status', {'jobid': vfs['job']})
                    if job.get('finished'):
                        vfs['status'] = 'failed' if job.get('error') else 'done'
                        vfs['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        if job.get('error'):
                            vfs['error'] = str(job['error'])[:200]
                            logger.error(f"[현황] VFS 새로고침 실패: 섹션 {key} job {vfs['job']} — {vfs['error']}")
                        else:
                            logger.info(f"[현황] VFS 새로고침 완료: 섹션 {key} job {vfs['job']}")
                        changed = True
                        # 대시보드 이력 행 — 새로고침만이면 여기서 끝내고,
                        # 새로고침 후 스캔이면 VFS 소요만 남긴다 (스캔이 이어진다).
                        # 스캔이 이어지는 행은 스캔 완료 때 끝낸다.
                        row_id = vfs.get('history_row_id')
                        if row_id:
                            vfs_secs = cls._slot_seconds(vfs.get('started_at'),
                                                         vfs.get('finished_at'))
                            if vfs.get('then_scan'):
                                if vfs['status'] == 'done':
                                    cls._dashboard_row_set_vfs_duration(row_id, vfs_secs)
                                else:
                                    cls._dashboard_row_finish(
                                        row_id, 'failed',
                                        vfs.get('error') or 'VFS 새로고침에 실패했습니다.',
                                        vfs_duration=vfs_secs)
                            else:
                                cls._dashboard_row_finish(
                                    row_id,
                                    'done' if vfs['status'] == 'done' else 'failed',
                                    'VFS 새로고침이 완료됐습니다.' if vfs['status'] == 'done'
                                    else (vfs.get('error') or 'VFS 새로고침에 실패했습니다.'),
                                    vfs_duration=vfs_secs)
                except requests.HTTPError as e:
                    # rclone 재시작 등으로 job 기록이 사라진 경우 — 실행 중으로 굳지 않게 중단으로 정리한다
                    body = ''
                    try:
                        body = e.response.text if e.response is not None else ''
                    except Exception:
                        body = ''
                    if 'not found' in body:
                        vfs['status'] = 'interrupted'
                        vfs['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        logger.warning(f"[현황] VFS 새로고침 job {vfs['job']} 기록이 없어 중단으로 정리: 섹션 {key}")
                        changed = True
                        cls._dashboard_row_finish(
                            vfs.get('history_row_id'), 'failed',
                            'VFS 새로고침 작업 기록을 찾을 수 없습니다.')
                except Exception:
                    pass
            if vfs.get('status') == 'running' and cls._started_over_min(
                    vfs, cls._vfs_timeout_min(vfs.get('source'))):
                # VFS 새로고침 제한 시간 초과 — 대시보드 새로고침만은
                # 여기가 아니면 기한이 없어 계속 진행 중으로 남는다.
                # 새로고침 후 스캔은 실행 스레드도 같은 기한을 보지만,
                # 이력 행 끝내기는 한 번만 되게 돼 있어 겹쳐도 괜찮다.
                vfs['status'] = 'failed'
                vfs['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                vfs['error'] = 'VFS 새로고침이 기한 안에 끝나지 않았습니다.'
                logger.warning(f"[현황] VFS 새로고침 기한 초과로 정리: 섹션 {key} job {vfs['job']}")
                changed = True
                row_id = vfs.get('history_row_id')
                if row_id:
                    cls._dashboard_row_finish(
                        row_id, 'failed', vfs['error'],
                        vfs_duration=cls._slot_seconds(vfs.get('started_at'),
                                                       vfs.get('finished_at')))
        meta = state.get('meta')
        if meta and meta.get('status') == 'running' \
                and str(key) not in _meta_refresh_threads:
            # 실행 스레드가 없는데 running으로 남아 있으면 재시작 등으로 끊긴 것이다
            meta['status'] = 'interrupted'
            meta['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logger.warning(f'[현황] 메타 새로고침 중단 상태로 표시: 섹션 {key}')
            changed = True
        trash = state.get('trash')
        if trash and trash.get('status') == 'running' \
                and str(key) not in _trash_chain_threads:
            # 실행 스레드가 없는데 running으로 남아 있으면 재시작 등으로
            # 끊긴 것이다. 단, 워커(주기·휴지통)가 남긴 표시는 워커가 지금
            # 이 라이브러리를 실행 중인지 먼저 본다 — 워커는 진행 상태를
            # *_current에 남기므로, 살아 있는 워커의 표시는 건드리지 않고
            # 워커가 없을 때만 설정 시간 뒤에 정리한다. 워커의 비우기
            # 표시는 mode가 동작 이름(예: refresh_scan_empty)이라, 이
            # 구분이 없으면 실행 중인 비우기가 1분 만에 중단으로 찍힌다.
            # 연결 실행(refresh_scan_empty)은 스레드가 살아 있는 동안만
            # running이므로, 시작 1분이 지나도 스레드가 없으면 바로
            # 정리한다 (1분 유예는 스레드 등록 직전의 찰나를 피하기 위함).
            # 단독 비우기는 동기 실행이라 스레드가 없으므로 시작 후 설정
            # 시간(기본값 30분)이 지난 경우에만 중단으로 정리한다.
            source = trash.get('source')
            if source in ('periodic', 'trash'):
                cur_key = ('trash_current' if source == 'trash'
                           else 'periodic_current')
                alive = False
                try:
                    raw_cur = P.ModelSetting.get(cur_key)
                    curs = json.loads(raw_cur) if raw_cur else None
                    if isinstance(curs, dict):
                        curs = [curs]
                    alive = any(isinstance(c, dict)
                                and c.get('stage') != 'waiting'
                                and str(c.get('section_key')) == str(key)
                                for c in (curs or []))
                except Exception:
                    alive = False
                stale = (not alive) and cls._started_over_min(
                    trash, cls._stuck_timeout_min(trash))
            elif trash.get('mode') == 'refresh_scan_empty':
                stale = cls._started_over_min(trash, 1)
            else:
                stale = cls._started_over_min(trash, cls._stuck_timeout_min(trash))
            if stale:
                trash['status'] = 'interrupted'
                trash['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                logger.warning(f'[현황] 휴지통 비우기 중단 상태로 표시: 섹션 {key}')
                changed = True
        scan = state.get('scan')
        if scan and scan.get('status') in ('running', 'requested') and scan.get('job') is not None:
            url, token = ShyniBackend.get_config()
            if url and token:
                try:
                    res = requests.get(f"{url}/compat/jobs/{scan['job']}",
                                       headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                                       timeout=15)
                    if res.status_code == 200:
                        job = res.json()
                        # 이 스캔에 딸린 VFS 소요 — 같은 이력 행의 vfs
                        # 슬롯일 때만 쓴다 (옛 새로고침 기록을 섞지 않는다).
                        vfs_slot = state.get('vfs') or {}
                        vfs_secs = None
                        if vfs_slot.get('history_row_id') and \
                                vfs_slot.get('history_row_id') == scan.get('history_row_id'):
                            vfs_secs = cls._slot_seconds(vfs_slot.get('started_at'),
                                                         vfs_slot.get('finished_at'))
                        if job.get('status') == 'completed':
                            scan['status'] = 'done'
                            scan['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            logger.info(f"[현황] 스캔 완료: 섹션 {key} job {scan['job']}")
                            changed = True
                            # 스캔으로 휴지통 구성이 바뀌었을 수 있어 상세 캐시를
                            # 지우고, 개수 캐시는 '오래됨'으로 표시해 다음 통계
                            # 계산 때 한 번에 읽게 한다
                            try:
                                from .task_trash_view import clear_cache
                                clear_cache(key)
                            except Exception as e:
                                logger.warning(f'[현황] 휴지통 상세 캐시 지우기 실패: {e}')
                            try:
                                from .task_status import mark_trash_counts_stale
                                mark_trash_counts_stale()
                            except Exception as e:
                                logger.warning(f'[현황] 휴지통 개수 캐시 오래됨 표시 실패: {e}')
                            # 카드 스캔 신규 측정 — 주기 스캔과 같은 방식으로 잰다
                            # (pre_max_id가 있는 카드 스캔만, stats에 new가 없는
                            # 경우만; 측정은 백그라운드 스레드에서 한다).
                            try:
                                cls._maybe_measure_card_scan_new(key, scan, job)
                            except Exception as e:
                                logger.warning(f'[현황] 카드 스캔 신규 측정 시작 실패: {e}')
                            # 대시보드 이력 행 — 스캔 통계를 채워 끝낸다.
                            # 카드 숫자는 이 이력에서 채워진다.
                            try:
                                from .task_periodic import scan_summary_text
                                summary = scan_summary_text(
                                    job.get('stats') if isinstance(job, dict) else None)
                            except Exception:
                                summary = None
                            done_msg = '스캔이 완료됐습니다.'
                            if summary:
                                done_msg = f'{done_msg} ({summary})'
                            cls._dashboard_row_finish(
                                scan.get('history_row_id'), 'done', done_msg,
                                job=job, job_id=scan.get('job'),
                                vfs_duration=vfs_secs,
                                scan_duration=cls._slot_seconds(
                                    scan.get('started_at'), scan.get('finished_at')),
                                pre_max_id=scan.get('pre_max_id'))
                        elif job.get('status') == 'failed':
                            scan['status'] = 'failed'
                            scan['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            scan['error'] = str(job.get('error') or '')[:200]
                            logger.error(f"[현황] 스캔 실패: 섹션 {key} job {scan['job']} — {scan['error']}")
                            changed = True
                            # 실패한 스캔도 부분 통계를 이력에 남긴다 (주기적 스캔과 동일).
                            cls._dashboard_row_finish(
                                scan.get('history_row_id'), 'failed',
                                f"스캔 실패: {scan.get('error') or ''}",
                                job=job, job_id=scan.get('job'),
                                vfs_duration=vfs_secs,
                                scan_duration=cls._slot_seconds(
                                    scan.get('started_at'), scan.get('finished_at')),
                                pre_max_id=scan.get('pre_max_id'))
                        elif job.get('status') == 'cancelled':
                            # Step5-1 — 관리자 API·shyni 웹에서 취소된 스캔은
                            # job이 'cancelled'로 끝난다. 슬롯도 취소됨으로
                            # 정리하고 이력 행에 '취소됨'으로 남긴다.
                            scan['status'] = 'cancelled'
                            scan['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            logger.info(f"[현황] 스캔 취소됨: 섹션 {key} job {scan['job']}")
                            changed = True
                            cls._dashboard_row_finish(
                                scan.get('history_row_id'), 'cancelled',
                                '스캔이 취소됐습니다.',
                                job=job, job_id=scan.get('job'),
                                vfs_duration=vfs_secs,
                                scan_duration=cls._slot_seconds(
                                    scan.get('started_at'), scan.get('finished_at')),
                                pre_max_id=scan.get('pre_max_id'))
                    elif res.status_code == 404:
                        # job 기록이 사라지면(보관 기간 경과·shyni 재시작 등)
                        # 완료를 확인할 길이 없어 진행 중으로 굳는다 — 중단으로 정리한다
                        scan['status'] = 'interrupted'
                        scan['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        scan['error'] = 'job 기록을 찾을 수 없습니다'
                        logger.warning(f"[현황] 스캔 job {scan['job']} 기록이 없어 중단으로 정리: 섹션 {key}")
                        changed = True
                        cls._dashboard_row_finish(scan.get('history_row_id'), 'failed',
                                                  '스캔 작업 기록을 찾을 수 없습니다.')
                except Exception:
                    pass
        if scan and scan.get('status') == 'requested' and not scan.get('job'):
            # job id가 없어 완료를 확인할 수 없는 표시 — 시작 후 설정 시간
            # (기본값 30분)이 지나면 중단으로 정리한다. job id가 있는 기록은
            # 폴링으로만 판정하고 시간 제한을 적용하지 않는다 (수 시간짜리
            # 정상 스캔이 중단으로 찍히면 안 되므로).
            timeout = cls._stuck_timeout_min(scan)
            if cls._started_over_min(scan, timeout):
                scan['status'] = 'interrupted'
                scan['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                scan['error'] = '스캔 상태를 확인할 수 없어 중단으로 정리했습니다'
                logger.warning(f'[현황] 스캔 전송됨 표시가 오래돼 중단으로 정리: 섹션 {key}')
                changed = True
                cls._dashboard_row_finish(scan.get('history_row_id'), 'failed',
                                          '스캔 상태를 확인할 수 없어 중단으로 정리했습니다')
        if changed:
            cls._set_state(key, state)
        return state

    # ---------- 라이브러리 동시 스캔 수 게이트 (규칙 2) ----------
    # 규칙 1(같은 모듈 안에서 같은 라이브러리 중복 금지)이 규칙 2보다
    # 먼저 적용된다 — 규칙 1은 task_periodic/task_trash에서 본다.
    # 규칙 2: 스캔이 들어간 작업은 경로와 상관없이 1건으로 세서, 설정
    # '라이브러리 동시 스캔 수'(기본값 2)까지만 동시에 돈다.
    # 대시보드 스캔·새로고침 후 스캔은 누른 시점부터 1건이고,
    # 주기적·휴지통은 스캔이 들어간 작업이 실행 중이면 1건이다.
    # 상한에 걸리거나 같은 라이브러리가 스캔 중이면 시작하지 않고
    # 취소한다 — 기다리지 않는다. 대기 중인 작업은 세지 않는다.
    # 스캔 큐(폴더 단위)는 기존 '동시 스캔 수'가 따로 제한하므로
    # 여기서 세지 않는다. 실행 중 집계는 현황 scan 슬롯을 compat
    # job으로 직접 확인해 센다 — shyni 스캔 상태 API는 큐 스캔까지
    # 스캔 중으로 보여 자리를 차지하므로 쓰지 않는다.
    LIBRARY_SCAN_LIMIT_KEY = 'scan_max_library_scan_count'
    LIBRARY_SCAN_LIMIT_DEFAULT = 2
    LIBRARY_SCAN_SAME_MESSAGE = '현재 해당 라이브러리 스캔 작업이 진행중 입니다.'

    @classmethod
    def library_scan_limit(cls):
        """라이브러리 동시 스캔 수 설정 — 없거나 잘못됐으면 기본값 2."""
        try:
            v = P.ModelSetting.get_int(cls.LIBRARY_SCAN_LIMIT_KEY)
        except Exception:
            v = None
        if not v:
            return cls.LIBRARY_SCAN_LIMIT_DEFAULT
        return max(1, min(20, v))

    @classmethod
    def dashboard_running_scans(cls):
        """대시보드에서 시작한 스캔 — [(key, title)]. 현황 scan·vfs
        슬롯을 compat job으로 확인해 세며, 주기적·휴지통 진행
        상태는 세지 않는다. (차지 재확인용.)"""
        try:
            sections, err = cls._sections()
        except Exception:
            return []
        if err or not sections:
            return []
        running = {}
        for sec in sections:
            key = sec.get('key')
            if key is None:
                continue
            try:
                state = cls._refresh_state(int(key), cls._get_state(int(key)))
            except Exception:
                continue
            scan = (state or {}).get('scan') or {}
            if scan.get('status') in ('running', 'requested'):
                running[str(key)] = sec.get('title') or f'라이브러리 {key}'
                continue
            vfs = (state or {}).get('vfs') or {}
            if vfs.get('status') == 'running' and vfs.get('then_scan'):
                running[str(key)] = sec.get('title') or f'라이브러리 {key}'
        order = {str(sec.get('key')): i for i, sec in enumerate(sections)}
        return sorted(running.items(), key=lambda kv: order.get(kv[0], 999))

    @classmethod
    def claim_survives(cls, claim_id, first_key, claimed_at):
        """차지한 스캔 자리가 살아남는지 — True면 그대로 진행, False면
        자리를 내놓고 기다린다. 게이트를 보고 차지하는 사이에 다른
        작업이 차지했을 수 있어, 차지한 뒤에 한 번 더 확인한다.
        동시에 차지한 작업끼리는 차지한 시각(claimed_at) 순서로
        가른다 — 앞 순서가 자리를 갖고, 뒤 순서가 물러난다. (전부
        물러나면 아무도 시작하지 못하니 순서로 가른다.) 차지한
        시각이 없는 옛 항목은 가장 먼저 차지한 것으로 센다.
        대시보드 스캔은 이미 돌고 있는 것이라 전부 센다. 대기 중인
        항목은 자리를 예약하지 않으므로 세지 않는다.
        읽기에 실패하면 True로 열어 둔다 (게이트가 스캔을 막지 않게)."""
        try:
            limit = cls.library_scan_limit()
        except Exception:
            return True
        try:
            dashboard = cls.dashboard_running_scans()
        except Exception:
            dashboard = []
        running = {str(k) for k, _t in dashboard}
        if str(first_key) in running:
            return False
        try:
            my_at = int(claimed_at) if claimed_at is not None else 0
        except Exception:
            my_at = 0
        for setting_key in ('periodic_current', 'trash_current'):
            try:
                raw = P.ModelSetting.get(setting_key)
                curs = json.loads(raw) if raw else None
            except Exception:
                continue
            if isinstance(curs, dict):
                curs = [curs]
            if not isinstance(curs, list):
                continue
            for cur in curs:
                if not isinstance(cur, dict):
                    continue
                if cur.get('claim_id') == claim_id:
                    continue
                if cur.get('stage') == 'waiting':
                    continue
                if 'scan' not in str(cur.get('action') or ''):
                    continue
                try:
                    other_at = int(cur.get('claimed_at') or 0)
                except Exception:
                    other_at = 0
                # 나보다 늦게 차지한 작업은 세지 않는다 — 그쪽이
                # 나를 보고 스스로 물러난다. 같은 시각이면
                # claim_id 문자열 순서로 가른다.
                if (other_at, str(cur.get('claim_id') or '')) >= (my_at, str(claim_id)):
                    continue
                skey = cur.get('section_key')
                if not skey:
                    continue
                if str(skey) == str(first_key):
                    return False
                running.add(str(skey))
        return len(running) < limit

    @classmethod
    def running_library_scans(cls, exclude_source=None, exclude_claim_id=None):
        """지금 스캔 중인 라이브러리 — [(key, title)].
        현황 scan 슬롯을 compat job으로 확인해 세므로, 대시보드를
        열지 않아 표시만 낡게 남은 스캔은 세지 않는다. 새로고침 후
        스캔은 새로고침 단계부터 센다. 주기적·휴지통은 진행 상태에
        스캔이 들어간 작업이 있으면 그 라이브러리를 센다. 대기 중인
        작업은 자리를 예약하지 않으므로 세지 않는다.
        exclude_source='periodic'/'trash'면 그쪽 진행 상태는 빼고
        센다 (호환용). exclude_claim_id면 그 작업만 빼고 센다 —
        여러 작업이 함께 돌 때 자기 자리만 빼는 정확한 방식이다.
        목록을 읽지 못하면 빈 목록을 돌려준다 (게이트가 스캔을
        막지 않게)."""
        try:
            sections, err = cls._sections()
        except Exception:
            return []
        if err or not sections:
            return []
        titles = {}
        running = {}
        for sec in sections:
            key = sec.get('key')
            if key is None:
                continue
            titles[str(key)] = sec.get('title') or f'라이브러리 {key}'
            try:
                state = cls._refresh_state(int(key), cls._get_state(int(key)))
            except Exception:
                continue
            scan = (state or {}).get('scan') or {}
            if scan.get('status') in ('running', 'requested'):
                running[str(key)] = titles[str(key)]
                continue
            vfs = (state or {}).get('vfs') or {}
            if vfs.get('status') == 'running' and vfs.get('then_scan'):
                running[str(key)] = titles[str(key)]
        for source, setting_key in (('periodic', 'periodic_current'),
                                    ('trash', 'trash_current')):
            if source == exclude_source:
                continue
            try:
                raw = P.ModelSetting.get(setting_key)
                curs = json.loads(raw) if raw else None
            except Exception:
                curs = None
            if isinstance(curs, dict):
                curs = [curs]
            if not isinstance(curs, list):
                continue
            for cur in curs:
                if not isinstance(cur, dict):
                    continue
                if (exclude_claim_id is not None
                        and cur.get('claim_id') == exclude_claim_id):
                    continue
                if cur.get('stage') == 'waiting':
                    continue
                skey = cur.get('section_key')
                if not skey:
                    continue
                if 'scan' not in str(cur.get('action') or ''):
                    continue
                skey = str(skey)
                if skey not in running:
                    running[skey] = (titles.get(skey)
                                     or cur.get('section_title')
                                     or f'라이브러리 {skey}')
        order = {str(sec.get('key')): i for i, sec in enumerate(sections)}
        return sorted(running.items(), key=lambda kv: order.get(kv[0], 999))

    @classmethod
    def library_gate_check(cls, key, exclude_source=None, exclude_claim_id=None):
        """스캔 시작 전 게이트 검사 — 막아야 하면 안내 문구, 아니면 None.
        같은 라이브러리가 이미 스캔 중이면 그 문구를 먼저 낸다."""
        running = cls.running_library_scans(
            exclude_source=exclude_source, exclude_claim_id=exclude_claim_id)
        if str(key) in {str(k) for k, _ in running}:
            return cls.LIBRARY_SCAN_SAME_MESSAGE
        limit = cls.library_scan_limit()
        if len(running) >= limit:
            names = ', '.join(t for _, t in running)
            return (f'다른 라이브러리 스캔 {len(running)}개가 실행 중입니다 '
                    f'({names}). (라이브러리 동시 스캔 수: {limit}개)')
        return None

    @classmethod
    def dashboard_scan_cards(cls):
        """대시보드에서 시작한 진행 중 스캔 카드 — 주기적·휴지통
        작업 관리·결과 페이지의 진행 중 카드 목록에 이어 붙인다.
        현황 scan·vfs 슬롯을 compat job으로 검증한 뒤, 주기적·
        휴지통 진행 상태가 소유한 섹션과 source가 'periodic'/
        'trash'인 슬롯은 뺀다. 카드 모양은 주기적·휴지통 카드와
        같고, 출처는 job_name='대시보드'로 표시한다."""
        try:
            sections, err = cls._sections()
        except Exception:
            return []
        if err or not sections:
            return []
        owned = set()
        for setting_key in ('periodic_current', 'trash_current'):
            try:
                raw = P.ModelSetting.get(setting_key)
                curs = json.loads(raw) if raw else None
            except Exception:
                curs = None
            if isinstance(curs, dict):
                curs = [curs]
            if isinstance(curs, list):
                for c in curs:
                    if (isinstance(c, dict) and c.get('section_key') is not None
                            and c.get('stage') != 'waiting'):
                        owned.add(str(c.get('section_key')))
        cards = []
        for sec in sections:
            key = sec.get('key')
            if key is None or str(key) in owned:
                continue
            try:
                state = cls._refresh_state(int(key), cls._get_state(int(key)))
            except Exception:
                continue
            scan = (state or {}).get('scan') or {}
            vfs = (state or {}).get('vfs') or {}
            if scan.get('source') in ('periodic', 'trash'):
                continue
            if vfs.get('source') in ('periodic', 'trash'):
                continue
            stage = None
            action = None
            started_at = None
            if scan.get('status') in ('running', 'requested'):
                stage = 'scan'
                action = 'scan'
                started_at = scan.get('started_at')
            elif vfs.get('status') == 'running':
                if vfs.get('then_scan'):
                    stage = 'vfs'
                    action = 'refresh_scan'
                else:
                    stage = 'refresh'
                    action = 'refresh'
                started_at = vfs.get('started_at')
            if stage is None:
                continue
            cards.append({
                'source': 'dashboard', 'job_name': '대시보드',
                'section_key': str(key),
                'section_title': sec.get('title') or f'라이브러리 {key}',
                'stage': stage, 'mode': 'manual', 'action': action,
                'job_started_at': started_at,
                'section_started_at': started_at,
            })
        return cards

    # ---------- 대시보드 목록 재구축 (Celery 워커 전용) ----------
    @classmethod
    def build_list_data(cls):
        """대시보드 목록 payload를 처음부터 만든다 — Celery 워커가 부른다.

        웹은 이 함수를 직접 부르지 않는다. 재구축(섹션 API·스캔 이력·
        건강·용량)은 수 초가 걸려 웹 프로세스에서 하면 FF 웹(gevent
        단일 루프) 전체가 멈춘다. 웹은 저장된 복사본만 읽고, 재구축
        요청은 워커에 남긴다. 실패하면 None을 돌려준다 (워커가 error
        상태로 기록한다).
        """
        sections, err = cls._sections()
        if err:
            logger.warning(f'[Status] 대시보드 목록 재구축 실패: {err}')
            return None
        history, log_file_count = cls._scan_history()
        rows = []
        for sec in sections:
            state = cls._refresh_state(sec['key'], cls._get_state(sec['key']))
            sec['state'] = state
            sec['last_scan'] = history.get(str(sec['key']))
            rows.append(sec)
        # 스캔 이력이 없는 카드는 주기 스캔 이력으로 채운다
        # (Relay OFF면 스캔 이력이 비어 있다).
        cls._fill_last_scan_from_periodic(rows)
        # 도서 카드 신규는 주기 스캔 이력 실측값으로 채운다
        # (shyni 도서 stats에는 신규 키가 없어 last_scan이 비어 있다).
        cls._fill_book_new(rows)
        periodic_current = []
        raw_cur = P.ModelSetting.get('periodic_current')
        if raw_cur:
            try:
                cur = json.loads(raw_cur)
                if isinstance(cur, dict):
                    cur = [cur]
                if isinstance(cur, list):
                    periodic_current = [c for c in cur if isinstance(c, dict)]
            except Exception:
                pass
        trash_current = []
        raw_trash = P.ModelSetting.get('trash_current')
        if raw_trash:
            try:
                cur = json.loads(raw_trash)
                if isinstance(cur, dict):
                    cur = [cur]
                if isinstance(cur, list):
                    trash_current = [c for c in cur if isinstance(c, dict)]
            except Exception:
                pass
        type_counts = {}
        for sec in sections:
            t = sec.get('type') or 'unknown'
            type_counts[t] = type_counts.get(t, 0) + 1
        return {'data': rows, 'log_files': log_file_count,
                'periodic_current': periodic_current,
                'trash_current': trash_current,
                'type_counts': type_counts,
                'server': cls._health(), 'storage': cls._storage(),
                'cached_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

    # ---------- 화면 AJAX (로그인 세션용 /ajax/<module>/<cmd>) ----------
    def process_ajax(self, sub, req):
        if sub == 'db_stats':
            # 화면 폴링용: DB 통계가 준비됐는지와 진척률만 가볍게 돌려준다
            try:
                bundle, loading, db_status = self._db_stats()
                if type(self)._db_stats_busy:
                    # 강제 새로고침으로 재계산이 돌고 있으면 끝날 때까지 로딩중으로 본다
                    loading = True
            except Exception:
                bundle, loading, db_status = None, False, {}
            return jsonify({'ret': 'success',
                            'ready': bundle is not None and not loading,
                            'db_off': bundle is None and not loading,
                            'db_relay_off': bool(db_status.get('relay_off')),
                            'db_stats_failed': bool(db_status.get('failed')),
                            'db_stats_at': db_status.get('at'),
                            'progress': self._overall_progress(loading),
                            'libs': (bundle or {}).get('libs') or {}})
        if sub == 'states':
            # 화면 폴링용: 섹션별 작업 상태만 가볍게 돌려준다 (상태 줄 자동 갱신용)
            keys = [k.strip() for k in (req.form.get('keys') or '').split(',') if k.strip()]
            out = {}
            for k in keys:
                try:
                    out[k] = self._refresh_state(k, self._get_state(k))
                except Exception:
                    out[k] = self._get_state(k)
            return jsonify({'ret': 'success', 'states': out})
        if sub == 'sessions':
            # 세션 탭용: Relay가 켜져 있으면 전 사용자 세션 목록을,
            # 꺼져 있으면 안내 문구를 보여 준다.
            from . import db_support
            if not db_support.db_ready():
                return jsonify({'ret': 'success', 'relay_off': True, 'sessions': [],
                                'refresh_min': self._refresh_min()})
            sessions = db_support.list_sessions()
            if sessions is None:
                return jsonify({'ret': 'fail', 'msg': '세션 목록을 불러오지 못했습니다.'})
            # 현재 세션 표시 — 관리자 API로 현재 세션 id 확인
            current_id = None
            try:
                from .shyni_admin import api_current_session_id
                current_id = api_current_session_id()
            except Exception:
                current_id = None
            return jsonify({'ret': 'success', 'relay_off': False,
                            'sessions': sessions, 'current_id': current_id,
                            'refresh_min': self._refresh_min()})
        if sub == 'list':
            cfg_url, cfg_token = ShyniBackend.get_config()
            if not cfg_url or not cfg_token:
                # 설정이 비어 있으면 오류가 아니라 안내 화면으로 보여 준다
                return jsonify({'ret': 'success', 'data': [], 'need_config': True,
                                'log_files': 0,
                                'refresh_min': self._refresh_min(),
                                'periodic_current': [], 'trash_current': [],
                                'type_counts': {},
                                'db_loading': False, 'list_loading': False,
                                'progress': 100,
                                'server': None, 'storage': None})
            # 웹은 저장된 복사본을 즉시 보여 주고, 재구축은 워커에 맡긴다.
            # 재구축(섹션 API·스캔 이력·건강·용량)은 수 초가 걸리고 그중
            # 로그 파싱은 CPU 작업이라 웹에서 하면 FF 웹(gevent 단일 루프)
            # 전체가 멈춘다 — 첫 접속 때 웹이 뻗던 원인이다 (2026-11-15 실측).
            # 복사본이 낡았어도 먼저 보여 주고(기준 시각 표시), 워커가
            # 뒤에서 새로 만들면 화면이 폴링으로 받아 간다.
            force = (req.form.get('force') or '') == '1'
            interval = self._refresh_min()
            ttl = interval * 60 if interval and interval >= 1 else 0
            now_ts = time.time()
            cache = type(self)._list_cache
            try:
                saved_raw = P.ModelSetting.get('status_list_cache')
                if saved_raw:
                    saved = json.loads(saved_raw)
                    saved_at = (saved.get('at') or 0) if isinstance(saved, dict) else 0
                    if isinstance(saved, dict) and saved.get('data') \
                            and saved_at >= (cache['at'] or 0):
                        cache = {'at': saved_at, 'data': saved['data']}
                        type(self)._list_cache = cache
            except Exception:
                pass
            base = cache['data']
            from_cache = base is not None
            stale = base is None or force \
                or (now_ts - (cache['at'] or 0) >= ttl)
            list_loading = False
            if stale:
                try:
                    from .task_status import request_list_rebuild, read_list_state
                    request_list_rebuild(force=force)
                    st = read_list_state()
                    list_loading = bool(st.get('busy')) \
                        or ((st.get('requested_at') or 0) > (st.get('finished_at') or 0))
                except Exception:
                    list_loading = base is None
            if base is None:
                # 저장된 복사본이 아예 없으면(최초 설치) 빈 목록 + 로딩
                # 표시로 답하고, 화면이 폴링으로 워커의 복사본을 받아 간다.
                # log_files는 None으로 — 0을 보내면 화면이 '로그를 찾지
                # 못했습니다' 경고를 띄우는데, 아직 확인 전이라 틀린 말이다.
                return jsonify({'ret': 'success', 'data': [],
                                'log_files': None,
                                'refresh_min': interval,
                                'periodic_current': [], 'trash_current': [],
                                'type_counts': {},
                                'db_stats_on': False,
                                'db_loading': False,
                                'db_stats_at': None,
                                'db_relay_off': False,
                                'db_stats_failed': False,
                                'list_loading': True,
                                'progress': 0,
                                'health': None,
                                'from_cache': False,
                                'cached_at': None,
                                'server': None, 'storage': None})
            rows = base['data']
            # 캐시로 답하는 경우에도 상태만큼은 그 자리에서 다시 확인한다 —
            # 캐시를 만든 뒤에 시작한 작업도 바로 보이고, 끝난 작업이
            # '진행 중'으로 남지도 않는다. (이전에는 캐시에 이미 진행 중으로
            # 표시된 경우에만 다시 확인해, 새로 시작한 작업이 다음 주기까지
            # 화면에 안 나왔다 — 2026-11-12 국내 모음 새로고침 후 스캔에서 확인됨)
            for sec in rows:
                try:
                    sec['state'] = self._refresh_state(sec['key'], self._get_state(sec['key']))
                except Exception:
                    pass
            # 스캔 이력이 없는 카드는 주기 스캔 이력으로 채운다 —
            # 캐시를 만든 뒤에 끝난 주기 스캔도 다음 주기까지 기다리지
            # 않고 바로 보인다. 도서 신규 채움보다 먼저 해야
            # 채운 기록의 신규도 바로 채워진다.
            try:
                self._fill_last_scan_from_periodic(rows)
            except Exception:
                pass
            # 신규도 마찬가지 — 캐시를 만든 뒤에 끝난 주기 스캔의 신규가
            # 다음 주기까지 '-'로 남지 않게 그 자리에서 다시 채운다.
            try:
                self._fill_book_new(rows)
            except Exception:
                pass
            periodic_current = base['periodic_current']
            trash_current = base.get('trash_current')
            db_stats_on = False
            health = None
            db_loading = False
            db_status = {}
            try:
                bundle, db_loading, db_status = self._db_stats(force=force)
                if bundle:
                    db_stats_on = True
                    stats = bundle['libs']
                    for sec in rows:
                        st = stats.get(int(sec['key']))
                        if st:
                            sec['db_stats'] = st
                    health = self._health_payload(bundle, rows, periodic_current)
            except Exception as e:
                logger.warning(f'[Status] DB 통계 조회 실패(무시): {e}')
            if not db_stats_on:
                # Relay가 꺼져 있어 DB 통계가 없어도 휴지통 개수는
                # 휴지통 조회·비우기 때 저장된 개수 캐시에서 보여 준다 —
                # 휴지통 개수는 관리자 API로도 읽을 수 있어 Relay와 무관하다.
                # 최소 정보(trash)만 넣어, 미디어·메타 없음은 빈 칸 그대로 둔다.
                try:
                    from .task_status import read_trash_cache
                    cached = read_trash_cache().get('data') or {}
                    for sec in rows:
                        v = cached.get(str(sec['key']))
                        if v is not None:
                            sec['db_stats'] = {'trash': int(v)}
                except Exception:
                    pass
            return jsonify({'ret': 'success', 'data': rows,
                            'log_files': base['log_files'],
                            'refresh_min': interval,
                            'periodic_current': periodic_current,
                            'trash_current': trash_current,
                            'type_counts': base['type_counts'],
                            'db_stats_on': db_stats_on,
                            'db_loading': db_loading,
                            'db_stats_at': db_status.get('at'),
                            'db_relay_off': bool(db_status.get('relay_off')),
                            'db_stats_failed': bool(db_status.get('failed')),
                            'list_loading': list_loading,
                            # 목록 재구축 중에는 진행률을 40%로 묶어 둔다 —
                            # DB 통계가 준비돼 100%가 찍히면 목록이 아직
                            # 오는 중인데 '완료'로 보이는 문제가 생긴다.
                            'progress': min(self._overall_progress(db_loading), 40)
                            if list_loading else self._overall_progress(db_loading),
                            'health': health,
                            'from_cache': from_cache,
                            'cached_at': base.get('cached_at'),
                            'server': base['server'],
                            # 용량은 캐시가 아니라 그 자리에서 다시 읽는다 —
                            # 캐시(60분 TTL)에는 측정 중 표시가 안 남고 측정한
                            # 새 값도 안 들어 있어, 동그라미가 금방 멈추고
                            # 측정이 끝나도 값이 안 바뀌어 보였던 원인이다.
                            # _storage()는 디스크 사용량 + 설정 읽기뿐이라 가볍다.
                            'storage': self._storage()})

        if sub == 'refresh_data_size':
            # 데이터 폴더 용량 수동 측정 — du 전체 순회는 디스크를 오래
            # 붙잡으므로 자동으로는 최초 1회만 재고, 이후에는 사용자가
            # 새로고침 아이콘을 눌렀을 때만 잰다.
            path = (P.ModelSetting.get('base_shyni_data_path') or '').strip()
            if not path or not os.path.isdir(path):
                return jsonify({'ret': 'fail', 'msg': '데이터 폴더를 찾지 못했습니다. 기본 설정에서 shyni 데이터 폴더를 확인해 주세요.'})
            if not type(self)._start_measure_data_size(path):
                return jsonify({'ret': 'success', 'measuring': True,
                                'msg': '용량을 측정하고 있습니다. 잠시만 기다려 주세요.'})
            return jsonify({'ret': 'success', 'measuring': True,
                            'msg': '데이터 용량을 다시 측정하고 있습니다. 끝나면 자동으로 반영됩니다.'})

        if sub == 'storage':
            # 용량 카드만 가볍게 다시 읽는다 (수동 측정 완료 확인용 폴링)
            return jsonify({'ret': 'success', 'storage': self._storage()})

        if sub == 'set_lib_sort':
            # 대시보드·건강도에서 고른 정렬을 화면별로 기억한다.
            # 바꿀 때마다 조용히 저장하므로 성공 응답에 msg를 넣지 않는다.
            which = req.form.get('which') or ''
            value = (req.form.get('value') or '').strip()
            allowed = {
                'list': ('status_lib_sort', ('default', 'name', 'nometa', 'media', 'trash')),
                'health': ('status_health_sort', ('default', 'name', 'check', 'okpct', 'media', 'trash', 'code', 'summary', 'year', 'thumb')),
            }
            if which not in allowed or value not in allowed[which][1]:
                return jsonify({'ret': 'fail', 'msg': '요청 값이 올바르지 않습니다.'})
            P.ModelSetting.set(allowed[which][0], value)
            return jsonify({'ret': 'success'})

        if sub == 'set_hidden_libs':
            # '표시'에서 고른 숨김 라이브러리를 화면별로 따로 기억한다.
            # 체크할 때마다 조용히 저장하므로 성공 응답에 msg를 넣지 않는다.
            setting_key = ('status_health_hidden_libs'
                           if (req.form.get('which') or '') == 'health'
                           else 'status_hidden_libs')
            keys = []
            for part in (req.form.get('keys') or '').split(','):
                part = part.strip()
                if not part:
                    continue
                try:
                    keys.append(str(int(part)))
                except Exception:
                    return jsonify({'ret': 'fail', 'msg': '요청 값이 올바르지 않습니다.'})
            P.ModelSetting.set(setting_key, ','.join(keys))
            return jsonify({'ret': 'success'})

        if sub in ('vfs_refresh', 'scan', 'cancel', 'meta_refresh', 'vfs_scan', 'trash_empty', 'trash_action'):
            try:
                key = int(req.form.get('key'))
            except Exception:
                return jsonify({'ret': 'fail', 'msg': '요청 값이 올바르지 않습니다. 화면을 새로고침한 뒤 다시 시도해 주세요.'})
            sections, err = self._sections()
            if err:
                return jsonify({'ret': 'fail', 'msg': err})
            sec = next((s for s in sections if s['key'] == key), None)
            if sec is None:
                return jsonify({'ret': 'fail', 'msg': '해당 라이브러리를 찾지 못했습니다. shyni 연결 상태를 확인해 주세요.'})
            state = self._refresh_state(key, self._get_state(key))
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 휴지통 버튼의 동작 선택 — 고른 동작을 기존 시작 처리로 그대로 넘긴다.
            # '새로고침 후 스캔 후 비우기'만 스캔 완료 뒤 비우기를 잇는 체인이 붙는다.
            then_trash = False
            if sub == 'trash_action':
                mode = (req.form.get('mode') or 'empty').strip()
                if mode == 'refresh':
                    sub = 'vfs_refresh'
                elif mode == 'scan':
                    sub = 'scan'
                elif mode == 'refresh_scan':
                    sub = 'vfs_scan'
                elif mode == 'refresh_scan_empty':
                    sub = 'vfs_scan'
                    then_trash = True
                else:
                    sub = 'trash_empty'

            if sub == 'cancel':
                # Step5-1 — 취소는 shyni 스캔까지 함께 취소한다.
                # 응답에 stage(어떤 작업인지)와 cancelled(바로 확인됐는지)를
                # 함께 넣어, 화면이 버튼을 '취소중'으로 바꿨다가 확인되면
                # '작업이 취소되었습니다'로 표시할 수 있게 한다.
                # 주기적 스캔이 이 라이브러리를 실행 중이면 그쪽 취소를 요청한다
                # (task_periodic이 이 모듈을 참조하므로 순환을 피하려 안에서 가져온다)
                from .task_periodic import get_currents, request_cancel
                cur = None
                for c in get_currents():
                    if str(c.get('section_key')) == str(key):
                        cur = c
                        break
                if cur is not None:
                    request_cancel(cur.get('claim_id'))
                    logger.info(f"[현황] 주기적 스캔 취소 요청: {sec['title']}")
                    if (cur.get('stage') or '') == 'scan':
                        msg = '주기적 스캔 취소를 요청했습니다. shyni 스캔 취소가 확인되면 멈추고 하이라이트가 해제됩니다.'
                    elif (cur.get('stage') or '') == 'waiting':
                        msg = '주기적 스캔 취소를 요청했습니다. 대기가 중단되면 멈추고 하이라이트가 해제됩니다.'
                    else:
                        msg = '주기적 스캔 취소를 요청했습니다. 새로고침이 정리되면 멈추고 하이라이트가 해제됩니다.'
                    return jsonify({'ret': 'success', 'stage': 'periodic',
                                    'cancelled': False, 'msg': msg})
                # 휴지통 스캔이 이 라이브러리를 실행 중이면 그쪽 취소를 요청한다
                from .task_trash import get_trash_currents, request_trash_cancel
                cur = None
                for c in get_trash_currents():
                    if str(c.get('section_key')) == str(key):
                        cur = c
                        break
                if cur is not None:
                    request_trash_cancel(cur.get('claim_id'))
                    logger.info(f"[현황] 휴지통 스캔 취소 요청: {sec['title']}")
                    if (cur.get('stage') or '') == 'empty':
                        # 비우기는 끝까지 가고 남은 라이브러리가 멈춘다
                        msg = '비우기는 끝까지 진행되고, 남은 라이브러리는 멈춥니다.'
                    elif (cur.get('stage') or '') == 'scan':
                        msg = '휴지통 스캔 취소를 요청했습니다. shyni 스캔 취소가 확인되면 멈추고 하이라이트가 해제됩니다.'
                    elif (cur.get('stage') or '') == 'waiting':
                        msg = '휴지통 스캔 취소를 요청했습니다. 대기가 중단되면 멈추고 하이라이트가 해제됩니다.'
                    else:
                        msg = '휴지통 스캔 취소를 요청했습니다. 새로고침이 정리되면 멈추고 하이라이트가 해제됩니다.'
                    return jsonify({'ret': 'success', 'stage': 'trash',
                                    'cancelled': False, 'msg': msg})
                meta = state.get('meta')
                if meta and meta.get('status') == 'running':
                    meta['cancel'] = True
                    self._set_state(key, state)
                    logger.info(f"[현황] 메타 새로고침 취소 요청: {sec['title']}")
                    return jsonify({'ret': 'success', 'stage': 'meta', 'cancelled': False,
                                    'msg': f"{sec['title']} 메타 새로고침에서 진행 예정인 작업을 취소합니다. 현재 처리 중인 항목까지만 마치고 멈춥니다."})
                vfs = state.get('vfs')
                if vfs and vfs.get('status') == 'running' and vfs.get('job') is not None:
                    addr, _ = self._rc_target(vfs.get('path', ''))
                    if not addr:
                        return jsonify({'ret': 'fail', 'msg': 'VFS 새로고침 규칙에서 RC 주소를 찾지 못했습니다. 스캔 설정을 확인해 주세요.'})
                    try:
                        self._rc(addr, 'job/stop', {'jobid': vfs['job']})
                    except Exception as e:
                        return jsonify({'ret': 'fail', 'msg': f'VFS 새로고침 취소에 실패했습니다: {e}'})
                    vfs['status'] = 'cancelled'
                    vfs['finished_at'] = now
                    self._set_state(key, state)
                    self._dashboard_row_finish(vfs.get('history_row_id'), 'cancelled',
                                               '취소 요청으로 중단했습니다.')
                    logger.info(f"[현황] VFS 새로고침 취소: {sec['title']} job {vfs['job']}")
                    return jsonify({'ret': 'success', 'stage': 'vfs', 'cancelled': True,
                                    'msg': f"{sec['title']} VFS 새로고침을 취소했습니다."})
                scan = state.get('scan')
                if scan and scan.get('status') in ('running', 'requested'):
                    # Step5-1 — 관리자 API로 shyni 스캔을 취소한다.
                    # 확인은 목록 조회(_refresh_state)가 compat job
                    # 'cancelled'로 한다 — 화면이 폴링해 확인되면
                    # '작업이 취소되었습니다'를 표시한다.
                    from . import shyni_admin
                    if not shyni_admin.admin_available():
                        return jsonify({'ret': 'fail', 'msg': '스캔 취소를 하려면 관리자 API 설정이 필요합니다. 기본 설정에서 shyni 관리자 아이디·비밀번호를 입력해 주세요.'})
                    res = shyni_admin.api_cancel_scan(key)
                    if res is None or (res.status_code != 404 and res.status_code >= 300):
                        return jsonify({'ret': 'fail', 'msg': 'shyni 스캔 취소 요청에 실패했습니다. shyni 웹 화면에서 취소해 주세요.'})
                    job_id = scan.get('job')
                    if not job_id:
                        # 작업 번호가 없어 완료를 확인할 수 없는 표시 —
                        # 취소 요청이 받아들여졌으니 바로 취소됨으로 정리한다.
                        scan['status'] = 'cancelled'
                        scan['finished_at'] = now
                        self._set_state(key, state)
                        self._dashboard_row_finish(scan.get('history_row_id'), 'cancelled',
                                                   '스캔이 취소됐습니다.')
                        logger.info(f"[현황] 스캔 취소 (작업 번호 없음): {sec['title']}")
                        return jsonify({'ret': 'success', 'stage': 'scan', 'cancelled': True,
                                        'msg': f"{sec['title']} 스캔을 취소했습니다."})
                    logger.info(f"[현황] 스캔 취소 요청: {sec['title']} job {job_id}")
                    return jsonify({'ret': 'success', 'stage': 'scan', 'cancelled': False,
                                    'msg': f"{sec['title']} 스캔 취소를 요청했습니다. shyni에서 취소가 확인되면 멈춥니다."})
                trash = state.get('trash')
                if trash and trash.get('status') == 'running':
                    return jsonify({'ret': 'fail', 'msg': '휴지통 비우기 중에는 취소할 수 없습니다.'})
                return jsonify({'ret': 'fail', 'msg': '지금 취소할 수 있는 진행 중 작업이 없습니다.'})

            # 시작 계열 — 같은 섹션에 실행 중 작업이 있으면 중복 시작 금지
            for kind in ('vfs', 'scan', 'meta', 'trash'):
                job = state.get(kind)
                if job and job.get('status') == 'running':
                    return jsonify({'ret': 'fail', 'msg': f"이미 진행 중인 작업이 있습니다 ({kind}). 끝난 뒤에 다시 시도해 주세요."})

            if sub == 'vfs_refresh':
                addr, remote = self._rc_target(sec['path'])
                if not addr:
                    return jsonify({'ret': 'fail', 'msg': '이 라이브러리 경로가 VFS 새로고침 규칙과 맞지 않습니다. 스캔 설정의 규칙을 확인해 주세요.'})
                try:
                    ret = self._rc(addr, 'vfs/refresh',
                                   {'dir': remote, 'recursive': 'true', '_async': 'true'})
                except Exception as e:
                    return jsonify({'ret': 'fail', 'msg': f'VFS 새로고침을 시작하지 못했습니다: {e}'})
                row_id = self._dashboard_row_create(key, sec['title'], 'refresh')
                state['vfs'] = {'job': ret.get('jobid'), 'path': sec['path'],
                                'started_at': now, 'finished_at': None, 'status': 'running',
                                'history_row_id': row_id}
                self._set_state(key, state)
                logger.info(f"[현황] VFS 새로고침 시작: {sec['title']} job {ret.get('jobid')} ({remote})")
                return jsonify({'ret': 'success', 'msg': f"{sec['title']} VFS 새로고침을 시작했습니다."})

            if sub == 'vfs_scan':
                # 누른 시점에 같은 라이브러리가 스캔 중이거나 자리가
                # 없으면 새로고침부터 시작하지 않는다. 새로고침이 끝날
                # 때 자리가 찼으면 스캔은 취소된다.
                # 자리 검사부터 표시 기록까지 락으로 묶어, 동시에 누른
                # 카드가 자리를 겹쳐 세는 것을 막는다.
                with _library_scan_gate_lock:
                    gate_msg = self.library_gate_check(key)
                    if gate_msg:
                        return jsonify({'ret': 'fail', 'msg': gate_msg})
                    addr, remote = self._rc_target(sec['path'])
                    if not addr:
                        return jsonify({'ret': 'fail', 'msg': '이 라이브러리 경로가 VFS 새로고침 규칙과 맞지 않습니다. 스캔 설정의 규칙을 확인해 주세요.'})
                    try:
                        ret = self._rc(addr, 'vfs/refresh',
                                       {'dir': remote, 'recursive': 'true', '_async': 'true'})
                    except Exception as e:
                        return jsonify({'ret': 'fail', 'msg': f'VFS 새로고침을 시작하지 못했습니다: {e}'})
                    row_id = self._dashboard_row_create(key, sec['title'], 'refresh_scan')
                    state['vfs'] = {'job': ret.get('jobid'), 'path': sec['path'],
                                    'started_at': now, 'finished_at': None, 'status': 'running',
                                    'then_scan': True, 'history_row_id': row_id}
                    self._set_state(key, state)
                th = threading.Thread(target=self._run_vfs_scan,
                                      args=(key, sec['title'], ret.get('jobid')), daemon=True)
                _vfs_scan_threads[str(key)] = th
                th.start()
                logger.info(f"[현황] 새로고침 후 스캔 시작: {sec['title']} job {ret.get('jobid')} ({remote})")
                if then_trash:
                    st2 = self._get_state(key)
                    st2['trash'] = {'job': None, 'mode': 'refresh_scan_empty',
                                    'started_at': now, 'finished_at': None,
                                    'status': 'running'}
                    self._set_state(key, st2)
                    cth = threading.Thread(target=self._run_scan_trash,
                                           args=(key, sec['title']), daemon=True)
                    _trash_chain_threads[str(key)] = cth
                    cth.start()
                    logger.info(f"[현황] 스캔 후 휴지통 비우기 연결: {sec['title']}")
                    return jsonify({'ret': 'success', 'msg': f"{sec['title']} 새로고침을 시작했습니다. 스캔이 끝나면 휴지통을 비웁니다."})
                return jsonify({'ret': 'success', 'msg': f"{sec['title']} 새로고침을 시작했습니다. 끝나면 스캔이 자동으로 시작됩니다."})

            if sub == 'meta_refresh':
                from . import db_support
                if not db_support.db_ready():
                    return jsonify({'ret': 'fail',
                                    'msg': '메타 새로고침은 Relay에서 연결 테스트를 통과해야 사용할 수 있습니다.'})
                # 선택 항목 새로고침 (메타 상세 페이지) — ids(쉼표로 이은
                # 작품 번호)가 있으면 범위 계산 없이 그 항목만 새로고침한다.
                # 선택 항목은 빠진 값이 제각각이라 채워짐 확인은 건너뛴다.
                ids_param = (req.form.get('ids') or '').strip()
                if ids_param:
                    ids = [int(p) for p in ids_param.split(',') if p.strip().isdigit()]
                    if not ids:
                        return jsonify({'ret': 'fail', 'msg': '선택한 항목이 없습니다.'})
                    _, ml = db_support.library_main_label(key)
                    state['meta'] = {'status': 'running', 'total': len(ids), 'done': 0, 'fail': 0,
                                     'cancel': False, 'current': None,
                                     'started_at': now, 'finished_at': None}
                    self._set_state(key, state)
                    run_id = int(time.time())
                    th = threading.Thread(target=self._run_meta_refresh,
                                          args=(key, sec['title'], ids, run_id, None), daemon=True)
                    _meta_refresh_threads[str(key)] = th
                    th.start()
                    logger.info(f"[현황] 메타 새로고침 시작: {sec['title']} 선택한 {ml} {len(ids)}개")
                    return jsonify({'ret': 'success',
                                    'msg': f"{sec['title']} 선택한 {len(ids)}개의 메타 새로고침을 시작했습니다."})
                scope = (req.form.get('scope') or 'all').strip()
                only_no_code = scope == 'nocode'
                is_folder = bool(only_no_code and db_support.library_is_folder(key))
                _, ml = db_support.library_main_label(key)
                try:
                    # 폴더 에이전트 라이브러리는 메타가 없는 것이 특성이라
                    # '메타 없는 앨범'을 표지 없는 앨범으로 본다.
                    if is_folder:
                        ids = db_support.album_ids(key, only_no_thumb=True)
                    else:
                        ids = db_support.album_ids(key, only_no_code=only_no_code)
                except Exception as e:
                    return jsonify({'ret': 'fail', 'msg': f'{ml} 목록을 읽지 못했습니다: {e}'})
                if not ids:
                    return jsonify({'ret': 'fail',
                                    'msg': f'메타 없는 {ml}이 없습니다.' if only_no_code else f'이 라이브러리에는 {ml}이 없습니다.'})
                state['meta'] = {'status': 'running', 'total': len(ids), 'done': 0, 'fail': 0,
                                 'cancel': False, 'current': None,
                                 'started_at': now, 'finished_at': None}
                self._set_state(key, state)
                run_id = int(time.time())
                filled_kind = 'thumb' if is_folder else ('code' if only_no_code else None)
                th = threading.Thread(target=self._run_meta_refresh,
                                      args=(key, sec['title'], ids, run_id, filled_kind), daemon=True)
                _meta_refresh_threads[str(key)] = th
                th.start()
                scope_label = ('표지 없는 ' if is_folder else '메타 없는 ') + ml if only_no_code else ml
                logger.info(f"[현황] 메타 새로고침 시작: {sec['title']} {scope_label} {len(ids)}개 (범위 {scope})")
                return jsonify({'ret': 'success',
                                'msg': f"{sec['title']} 메타 새로고침을 시작했습니다. {scope_label} {len(ids)}개를 순서대로 처리합니다."})

            if sub == 'scan':
                # 게이트 검사부터 전송·표시까지 직렬화한다 — 동시에 누른
                # 카드 스캔이 검사를 함께 통과하지 않게 한다.
                with _library_scan_gate_lock:
                    gate_msg = self.library_gate_check(key)
                    if gate_msg:
                        return jsonify({'ret': 'fail', 'msg': gate_msg})
                    url, token = ShyniBackend.get_config()
                    # 신규 측정 기준점 — 스캔 전송 전에 기록한다 (주기 스캔과 동일 방식).
                    pre_max_id = self._card_scan_pre_max_id()
                    try:
                        res = requests.get(f"{url}/library/sections/{key}/refresh",
                                           headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                                           timeout=30)
                        res.raise_for_status()
                    except Exception as e:
                        return jsonify({'ret': 'fail', 'msg': f'스캔 전송에 실패했습니다: {e}'})
                    job_id = res.headers.get('X-Job-Id')
                    if not job_id:
                        try:
                            body = res.json()
                            if isinstance(body, dict):
                                job_id = body.get('job_id') or body.get('jobId') or body.get('jobID')
                        except Exception:
                            job_id = None
                    row_id = self._dashboard_row_create(key, sec['title'], 'scan',
                                                        shyni_job_id=job_id)
                    state['scan'] = {'job': job_id, 'path': sec['path'],
                                     'started_at': now, 'finished_at': None,
                                     'status': 'running' if job_id else 'requested',
                                     'history_row_id': row_id}
                    if pre_max_id is not None:
                        state['scan']['pre_max_id'] = pre_max_id
                    self._set_state(key, state)
                    logger.info(f"[현황] 스캔 전송: {sec['title']} job {job_id}")
                    return jsonify({'ret': 'success', 'msg': f"{sec['title']} 스캔을 전송했습니다."})

            if sub == 'trash_empty':
                url, token = ShyniBackend.get_config()
                if not url or not token:
                    return jsonify({'ret': 'fail', 'msg': 'Shyni 연결 설정이 비어 있습니다. 기본 설정을 확인해 주세요.'})
                row_id = self._dashboard_row_create(key, sec['title'], 'empty', trash=True)
                state['trash'] = {'job': None, 'mode': 'empty',
                                  'started_at': now, 'finished_at': None,
                                  'status': 'running'}
                self._set_state(key, state)
                ok, err = self._empty_trash_now(key)
                self._dashboard_row_finish(row_id, 'done' if ok else 'failed',
                                           '작업 완료' if ok else err, trash=True)
                state = self._get_state(key)
                tr = state.get('trash') or {}
                tr['status'] = 'done' if ok else 'failed'
                if err:
                    tr['error'] = err
                tr['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                state['trash'] = tr
                self._set_state(key, state)
                if not ok:
                    return jsonify({'ret': 'fail', 'msg': f'휴지통 비우기에 실패했습니다: {err}'})
                logger.info(f"[현황] 휴지통 비우기: {sec['title']}")
                return jsonify({'ret': 'success', 'msg': f"{sec['title']} 휴지통을 비웠습니다."})

        return jsonify({'ret': 'fail', 'msg': '알 수 없는 요청입니다.'})

    _db_stats_cache = {'at': 0, 'data': None}
    _db_stats_busy = False
    _db_stats_progress = 0
    _list_cache = {'at': 0, 'data': None}

    def _db_stats(self, force=False):
        """DB 통계를 (데이터, 로딩중, 상태) 로 돌려준다. 계산은 Celery 워커(task_status)가
        전담하고, 웹은 저장된 결과만 읽어 즉시 돌려준다 — 계산 중에도 화면이
        무거워지지 않는다. 결과가 없거나 오래됐거나 force(강제 새로고침)면
        워커에 계산 요청만 남긴다. 워커가 저장한 최신 결과가 메모리 복사본보다
        새로우면 받아들여, 옛 결과가 화면에 고착되지 않게 한다.
        상태 dict: relay_off(Relay 꺼짐/미테스트), at(저장 시각 unix),
        failed(마지막 계산 실패 — Relay가 켜져 있는데 연결이 안 될 때).
        Relay가 꺼져 있어도 이전에 저장된 결과는 보여 준다 — 화면에
        '마지막 Relay 연결시 ... 이전 데이터' 문구와 함께 표시한다 (1-1a)."""
        from .db_support import db_ready
        from .task_status import read_state, request_stats, read_last_error
        cls = type(self)
        relay_off = not db_ready()
        try:
            saved_raw = P.ModelSetting.get('status_db_stats_cache')
            if saved_raw:
                saved = json.loads(saved_raw)
                data = saved.get('data') if isinstance(saved, dict) else None
                saved_at = (saved.get('at') or 0) if isinstance(saved, dict) else 0
                if data and data.get('libs') and saved_at >= cls._db_stats_cache['at']:
                    data = dict(data)
                    data['libs'] = {int(k): v for k, v in data['libs'].items()}
                    cls._db_stats_cache = {'at': saved_at, 'data': data}
        except Exception:
            pass
        cached = cls._db_stats_cache['data']
        cached_at = cls._db_stats_cache['at'] or None
        if relay_off:
            # Relay가 꺼져 있으면 재계산을 요청하지 않는다 — 워커도
            # 계산하지 않으니(주기 검사에서 db_ready 확인) 요청만 쌓인다.
            # 이전 결과는 그대로 보여 주고, 화면이 낡은 데이터임을 알린다.
            cls._db_stats_busy = False
            cls._db_stats_progress = 0
            return cached, False, {'relay_off': True, 'at': cached_at,
                                   'failed': False}
        failed = read_last_error() is not None
        state = read_state()
        pending = (state.get('requested_at') or 0) > (state.get('finished_at') or 0)
        cls._db_stats_busy = bool(state.get('busy')) or pending
        cls._db_stats_progress = state.get('progress') or 0
        if cached is not None:
            if force:
                request_stats(force=True)
                state = read_state()
                pending = (state.get('requested_at') or 0) > (state.get('finished_at') or 0)
                cls._db_stats_busy = bool(state.get('busy')) or pending
                cls._db_stats_progress = state.get('progress') or 0
                return cached, cls._db_stats_busy, {'relay_off': False,
                                                    'at': cached_at,
                                                    'failed': failed}
            # 낡은 캐시라도 대시보드를 열 때마다 재계산을 트리거하지 않는다.
            # 재계산은 17초짜리 무거운 DB 집계(2026-11-15 실측)라, 열 때마다
            # 돌리면 서버가 무거워진다. 재계산은 스캔·비우기 완료 같은
            # 사건과 워커의 주기 검사(6시간)가 맡는다 — 대시보드는 저장된
            # 결과를 보여 주는 데 집중한다.
            return cached, False, {'relay_off': False, 'at': cached_at,
                                   'failed': failed}
        request_stats()
        return None, True, {'relay_off': False, 'at': None, 'failed': failed}

    @classmethod
    def _overall_progress(cls, db_loading):
        """화면 전체 로딩 진척률: 기본 정보 40% + DB 통계 60%."""
        if not db_loading:
            return 100
        return int(40 + 0.6 * (cls._db_stats_progress or 0))

    @staticmethod
    def _health_payload(bundle, rows, periodic_current):
        """건강도 섹션용 payload: 종합 집계 + 라이브러리별 상세 + 플러그인 스캔 상태."""
        libs = bundle['libs']
        lib_cards = []
        lib_no_code = []
        for sec in rows:
            st = libs.get(int(sec['key']))
            if not st:
                continue
            card = dict(st)
            card['key'] = sec['key']
            card['title'] = sec.get('title')
            missing_any = st.get('missing_any', st['no_code'])
            judge_missing = st.get('judge_missing', missing_any)
            card['missing_any'] = missing_any
            card['judge_missing'] = judge_missing
            card['is_folder'] = bool(st.get('is_folder'))
            card['ok'] = st['main_count'] - judge_missing
            card['ok_pct'] = round(card['ok'] * 100.0 / st['main_count'], 1) if st['main_count'] else 100.0
            card['check_pct'] = round(judge_missing * 100.0 / st['main_count'], 1) if st['main_count'] else 0
            card['code_pct'] = round(st['no_code'] * 100.0 / st['main_count'], 1) if st['main_count'] else 0
            card['avg_sub'] = round(st['sub_count'] * 1.0 / st['main_count'], 1) if st['main_count'] else 0
            card['avg_tracks'] = card['avg_sub']
            card['avg_file'] = round(st['file_count'] * 1.0 / st['sub_count'], 1) if st.get('file_count') is not None and st['sub_count'] else 0
            lib_cards.append(card)
            if missing_any:
                lib_no_code.append({'key': sec['key'], 'title': sec.get('title'),
                                    'works': st['main_count'], 'main_label': st['main_label'],
                                    'albums': st['main_count'], 'no_code': st['no_code'],
                                    'missing_any': missing_any})
        lib_no_code.sort(key=lambda x: -x['no_code'])
        scan = {'periodic_running': bool([c for c in (periodic_current or [])
                                          if isinstance(c, dict)
                                          and c.get('stage') != 'waiting']),
                'running': 0, 'analyzing': 0, 'done': 0, 'failed': 0,
                'queued': 0, 'assembling': 0, 'ready': 0, 'held': 0, 'total': 0}
        try:
            with F.app.app_context():
                q = F.db.session.query(P.ModelScanItem)
                scan['done'] = q.filter_by(status='FINISH_SHYNI').count()
                scan['failed'] = q.filter_by(status='FINISH_SHYNI_FAILED').count()
                scan['total'] = q.count()
            # 진행 단계별 집계는 스캔 목록 표시 상태와 같은 기준(summary_counts)을 쓴다
            counts = P.ModelScanItem.summary_counts()
            scan['running'] = len(counts.get('scanning_ids') or [])
            scan['analyzing'] = len(counts.get('analyzing_ids') or [])
            scan['assembling'] = counts.get('assembling') or 0
            scan['queued'] = counts.get('queued') or 0
            scan['ready'] = counts.get('ready') or 0
            scan['held'] = counts.get('held') or 0
        except Exception:
            pass
        return {'totals': bundle['dash'], 'lib_no_code': lib_no_code,
                'libs': lib_cards, 'scan': scan,
                'elapsed': bundle.get('elapsed')}


    def _run_vfs_scan(self, key, title, jobid):
        """'새로고침 후 스캔' — VFS 새로고침이 끝나면 이어서 스캔을 전송한다 (백그라운드 스레드).

        새로고침이 취소·실패·중단되면 스캔은 보내지 않는다. 현황 목록 조회가
        먼저 완료를 표시한 경우에도 스캔 단계로 넘어간다."""
        try:
            # 대시보드 이력 행 id — 시작 때 vfs 슬롯에 남겨 둔다.
            # 스레드가 끝날 때(중단 포함) 이 행을 끝낸다.
            row_id = (self._get_state(key).get('vfs') or {}).get('history_row_id')
            proceed = False
            # 대시보드 새로고침 후 스캔은 주기적 스캔 이력으로 가므로
            # 주기적 스캔의 VFS 새로고침 제한 시간을 따른다.
            deadline = time.time() + self._vfs_timeout_min() * 60
            while time.time() < deadline:
                st = self._get_state(key)
                vfs = (st or {}).get('vfs') or {}
                if vfs.get('job') != jobid:
                    self._dashboard_row_finish(row_id, 'failed',
                                               '새로고침이 다른 작업으로 바뀌어 중단됐습니다.')
                    return
                if vfs.get('status') == 'done':
                    proceed = True
                    break
                if vfs.get('status') != 'running':
                    # cancelled / failed / interrupted — 스캔으로 이어지지 않는다
                    if vfs.get('status') == 'cancelled':
                        self._dashboard_row_finish(row_id, 'cancelled',
                                                   '취소 요청으로 중단했습니다.')
                    else:
                        self._dashboard_row_finish(
                            row_id, 'failed',
                            vfs.get('error') or 'VFS 새로고침이 중단됐습니다.')
                    logger.info(f"[현황] 새로고침 후 스캔 중단: {title} (새로고침 {vfs.get('status')})")
                    return
                addr, _ = self._rc_target(vfs.get('path', ''))
                if not addr:
                    self._dashboard_row_finish(row_id, 'failed',
                                               'VFS 새로고침 규칙에서 RC 주소를 찾지 못했습니다.')
                    return
                try:
                    job = self._rc(addr, 'job/status', {'jobid': jobid})
                except requests.HTTPError as e:
                    body = ''
                    try:
                        body = e.response.text if e.response is not None else ''
                    except Exception:
                        body = ''
                    if 'not found' in body:
                        st = self._get_state(key)
                        if st.get('vfs') and st['vfs'].get('job') == jobid \
                                and st['vfs'].get('status') == 'running':
                            st['vfs']['status'] = 'interrupted'
                            st['vfs']['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            self._set_state(key, st)
                        self._dashboard_row_finish(row_id, 'failed',
                                                   'VFS 새로고침 작업 기록을 찾을 수 없습니다.')
                        logger.warning(f'[현황] 새로고침 후 스캔 중단(job 기록 없음): {title} job {jobid}')
                        return
                    time.sleep(10)
                    continue
                except Exception:
                    time.sleep(10)
                    continue
                if not job.get('finished'):
                    time.sleep(10)
                    continue
                st = self._get_state(key)
                vfs = (st or {}).get('vfs') or {}
                if vfs.get('job') != jobid or vfs.get('status') != 'running':
                    # 그 사이 취소됐거나 목록 조회가 먼저 완료로 표시한 경우
                    if vfs.get('job') == jobid and vfs.get('status') == 'done':
                        proceed = True
                    break
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if job.get('error'):
                    vfs['status'] = 'failed'
                    vfs['finished_at'] = now
                    vfs['error'] = str(job['error'])[:200]
                    self._set_state(key, st)
                    self._dashboard_row_finish(row_id, 'failed', vfs['error'])
                    logger.error(f'[현황] 새로고침 후 스캔 중단(새로고침 실패): {title} — {vfs["error"]}')
                    return
                vfs['status'] = 'done'
                vfs['finished_at'] = now
                self._set_state(key, st)
                logger.info(f'[현황] VFS 새로고침 완료: 섹션 {key} job {jobid}')
                proceed = True
                break
            if not proceed:
                self._dashboard_row_finish(row_id, 'failed',
                                           'VFS 새로고침이 기한 안에 끝나지 않았습니다.')
                logger.warning(f'[현황] 새로고침 후 스캔 시간 초과로 중단: {title} job {jobid}')
                return
            # 이어서 스캔 전송 — 같은 라이브러리가 스캔 중이거나
            # 자리가 없으면 취소한다 (기다리지 않는다).
            gate_msg = self.library_gate_check(key)
            if gate_msg:
                st = self._get_state(key)
                st['scan'] = {'job': None, 'path': ((st or {}).get('vfs') or {}).get('path'),
                              'started_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                              'finished_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                              'status': 'cancelled',
                              'error': gate_msg}
                self._set_state(key, st)
                self._dashboard_row_finish(row_id, 'failed', gate_msg)
                logger.warning(f'[현황] 새로고침 후 스캔 취소: {title} — {gate_msg}')
                return
            st = self._get_state(key)
            vfs_path = ((st or {}).get('vfs') or {}).get('path')
            url, token = ShyniBackend.get_config()
            # 신규 측정 기준점 — 스캔 전송 전에 기록한다 (주기 스캔과 동일 방식).
            pre_max_id = self._card_scan_pre_max_id()
            try:
                res = requests.get(f"{url}/library/sections/{key}/refresh",
                                   headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                                   timeout=30)
                res.raise_for_status()
            except Exception as e:
                self._dashboard_row_finish(row_id, 'failed', f'스캔 전송에 실패했습니다: {e}')
                logger.error(f'[현황] 새로고침 후 스캔 전송 실패: {title} — {e}')
                return
            job_id = res.headers.get('X-Job-Id')
            if not job_id:
                try:
                    body = res.json()
                    if isinstance(body, dict):
                        job_id = body.get('job_id') or body.get('jobId') or body.get('jobID')
                except Exception:
                    job_id = None
            st = self._get_state(key)
            st['scan'] = {'job': job_id, 'path': vfs_path,
                          'started_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                          'finished_at': None,
                          'status': 'running' if job_id else 'requested',
                          'history_row_id': row_id}
            if pre_max_id is not None:
                st['scan']['pre_max_id'] = pre_max_id
            self._set_state(key, st)
            self._dashboard_row_set_scan_job(row_id, job_id)
            logger.info(f'[현황] 새로고침 후 스캔 전송: {title} job {job_id}')
        finally:
            _vfs_scan_threads.pop(str(key), None)

    def _empty_trash_now(self, key):
        """휴지통 비우기를 바로 실행하고 DB 통계 재계산을 요청한다.
        (성공 여부, 오류 문구)를 돌려준다."""
        url, token = ShyniBackend.get_config()
        if not url or not token:
            return False, 'Shyni 연결 설정이 비어 있습니다.'
        try:
            res = requests.get(f"{url}/library/sections/{key}/emptyTrash",
                               headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                               timeout=120)
            res.raise_for_status()
        except Exception as e:
            return False, str(e)[:200]
        # 휴지통 개수가 바뀌므로 개수 캐시도 바로 새로 읽어 둔다 (백그라운드)
        try:
            from .task_status import refresh_trash_counts_async
            refresh_trash_counts_async()
        except Exception as e:
            logger.warning(f'[현황] 휴지통 개수 새로 읽기 요청 실패: {e}')
        # 비운 라이브러리의 상세 캐시 목록은 낡으므로 지운다
        try:
            from .task_trash_view import clear_cache
            clear_cache(key)
        except Exception as e:
            logger.warning(f'[현황] 휴지통 상세 캐시 지우기 실패: {e}')
        # 휴지통 수가 바뀌므로 DB 통계 재계산을 요청한다.
        # 상태를 직접 쓰면 계산 중일 때 요청이 소실되고 busy까지 지워지므로
        # request_stats를 쓴다 (계산 중에도 요청이 유지된다).
        try:
            from .task_status import request_stats
            request_stats()
        except Exception as e:
            logger.warning(f'[현황] DB 통계 재계산 요청 실패: {e}')
        return True, ''

    def _run_scan_trash(self, key, title):
        """'새로고침 후 스캔 후 비우기' — 스캔이 완료되면 이어서 휴지통을 비운다
        (백그라운드 스레드). 스캔이 실패·중단되면 비우지 않는다."""
        try:
            deadline = time.time() + 12 * 3600
            while time.time() < deadline:
                st = self._refresh_state(key, self._get_state(key))
                scan = (st or {}).get('scan') or {}
                vfs = (st or {}).get('vfs') or {}
                s_status = scan.get('status')
                if s_status == 'done':
                    trow_id = self._dashboard_row_create(key, title, 'empty', trash=True)
                    ok, err = self._empty_trash_now(key)
                    self._dashboard_row_finish(trow_id, 'done' if ok else 'failed',
                                               '작업 완료' if ok else err, trash=True)
                    st = self._get_state(key)
                    tr = st.get('trash') or {}
                    tr['status'] = 'done' if ok else 'failed'
                    if err:
                        tr['error'] = err
                    tr['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st['trash'] = tr
                    self._set_state(key, st)
                    logger.info(f"[현황] 스캔 후 휴지통 비우기 {'완료' if ok else '실패'}: {title} {err}")
                    return
                if s_status in ('failed', 'cancelled', 'interrupted'):
                    self._finish_trash_chain(key, '스캔이 완료되지 않아 휴지통을 비우지 않았습니다.')
                    return
                if s_status == 'requested' and not scan.get('job'):
                    # job id가 없어 완료를 확인할 수 없는 상태가 오래 가면 중단한다
                    try:
                        started_ts = datetime.strptime(scan.get('started_at') or '',
                                                       '%Y-%m-%d %H:%M:%S').timestamp()
                    except Exception:
                        started_ts = 0
                    if started_ts and time.time() - started_ts > 1800:
                        self._finish_trash_chain(key, '스캔 상태를 확인할 수 없어 휴지통을 비우지 않았습니다.')
                        return
                if not scan and vfs.get('status') in ('failed', 'cancelled', 'interrupted'):
                    self._finish_trash_chain(key, '새로고침이 완료되지 않아 휴지통을 비우지 않았습니다.')
                    return
                time.sleep(20)
            self._finish_trash_chain(key, '시간이 너무 오래 걸려 휴지통을 비우지 않았습니다.')
        finally:
            _trash_chain_threads.pop(str(key), None)

    def _finish_trash_chain(self, key, error):
        st = self._get_state(key)
        tr = st.get('trash') or {}
        tr['status'] = 'interrupted'
        tr['error'] = error
        tr['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        st['trash'] = tr
        self._set_state(key, st)
        logger.warning(f'[현황] 휴지통 비우기 연결 중단: 섹션 {key} — {error}')

    @staticmethod
    def _meta_refresh_titles(key):
        """진행 표시용 작품 번호 → 제목 (메타 캐시에서 읽는다, 없으면 빈 값)."""
        try:
            from . import task_meta_view
            cached = task_meta_view.cached_data(key) or {}
            titles = {}
            for row in cached.get('data') or []:
                if not isinstance(row, (list, tuple)) or len(row) < 2:
                    continue
                try:
                    mid = int(row[0])
                except (TypeError, ValueError):
                    continue
                title = ((row[2] + ' — ') if len(row) > 2 and row[2] else '') + (row[1] or '')
                if len(row) > 3 and row[3]:
                    title += f" ({row[3]})"
                titles[mid] = title
            return titles
        except Exception:
            return {}

    def _run_meta_refresh(self, key, title, ids, run_id, filled_kind=None):
        """라이브러리 전체 앨범에 메타 새로고침을 하나씩 건다 (백그라운드 스레드)."""
        url, token = ShyniBackend.get_config()
        done = 0
        fail = 0
        cancelled = False
        finished_at = None
        titles = self._meta_refresh_titles(key)
        try:
            for mid in ids:
                st = self._get_state(key)
                meta = (st or {}).get('meta') or {}
                if meta.get('cancel'):
                    cancelled = True
                    break
                # 진행 표시용 현재 항목 — 호출 전에 저장한다 (휴지통의
                # '현재:' 줄과 같은 표시). 캐시에 없는 항목은 표시를 비운다.
                st = self._get_state(key)
                if st.get('meta'):
                    st['meta']['current'] = titles.get(mid)
                    self._set_state(key, st)
                try:
                    res = requests.put(f'{url}/library/metadata/{mid}/refresh',
                                       headers={'X-Plex-Token': token, 'Accept': 'application/json',
                                                'Idempotency-Key': f'sm-lib-meta-{key}-{run_id}-{mid}'},
                                       timeout=20)
                    if res.status_code < 300:
                        done += 1
                    else:
                        fail += 1
                except Exception:
                    fail += 1
                st = self._get_state(key)
                if st.get('meta'):
                    st['meta']['done'] = done
                    st['meta']['fail'] = fail
                    self._set_state(key, st)
                time.sleep(0.15)
        finally:
            # 스레드 사전부터 비운다 — set_state가 예외를 일으켜도 항목이
            # 남아 '실행 중'으로 오인되지 않게 한다.
            _meta_refresh_threads.pop(str(key), None)
            try:
                st = self._get_state(key)
                if st.get('meta'):
                    finished_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    st['meta']['done'] = done
                    st['meta']['fail'] = fail
                    st['meta']['current'] = None
                    st['meta']['status'] = 'cancelled' if cancelled else 'done'
                    st['meta']['finished_at'] = finished_at
                    self._set_state(key, st)
            except Exception as e:
                logger.warning(f'[현황] 메타 새로고침 상태 저장 실패: {e}')
            logger.info(f'[현황] 메타 새로고침 {"취소됨" if cancelled else "완료"}: '
                        f'{title} 처리 {done} · 실패 {fail} / 전체 {len(ids)}')
        if filled_kind and not cancelled and finished_at:
            self._check_meta_filled(key, ids, filled_kind, finished_at)

    def _check_meta_filled(self, key, ids, kind, finished_at):
        """전송이 끝난 뒤 shyni 반영을 두 차례 확인해, 실제로 채워진 수를
        완료 상태에 남긴다 (전송 완료와 반영 완료가 다르기 때문)."""
        from . import db_support
        label = '표지' if kind == 'thumb' else '메타'
        n = None
        for delay in (90, 150):
            time.sleep(delay)
            try:
                if not db_support.db_ready():
                    return
                n = db_support.count_filled(ids, kind)
            except Exception as e:
                logger.warning(f'[현황] 메타 반영 확인 실패: {e}')
                return
            st = self._get_state(key)
            meta = (st or {}).get('meta') or {}
            # 그 사이 새 작업이 시작됐으면 그 상태는 건드리지 않는다
            if meta.get('status') != 'done' or meta.get('finished_at') != finished_at:
                return
            meta['filled'] = n
            meta['filled_label'] = label
            st['meta'] = meta
            self._set_state(key, st)
        logger.info(f'[현황] 메타 새로고침 반영 확인: 섹션 {key} {label} {n}개 채워짐')

    # 외부 API(/api/<module>/<cmd>)로 들어와도 같은 처리를 한다
    def process_api(self, sub, req):
        return self.process_ajax(sub, req)
