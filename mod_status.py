import glob
import json
import os
import re
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


class ModuleStatus(PluginModuleBase):
    """현황 — 라이브러리 목록과 라이브러리별 VFS 새로고침/스캔 실행·상태.

    섹션 목록·경로는 shyni 토큰 API(GET /library/sections)에서 가져온다.
    마지막 스캔·파일수·신규·missing은 기본 설정에서 지정한 shyni 로그 폴더의
    docker 로그(*-json.log)에서 [scan complete]/[scan failed] 기록을 읽어 표시한다.
    로그에 기록이 없는 라이브러리는 '스캔 후 내용이 반영됩니다'로 표시한다.
    앨범수는 로그에 기록되지 않아 표시할 수 없다.
    작업 상태는 이 플러그인이 시작한 작업만 섹션별로 기록해 표시한다.
    스캔 취소는 토큰으로 불가능하다(DELETE /libraries/{id}/scan은 관리자 전용 401).
    VFS 새로고침 취소는 rclone RC job/stop으로 가능하다.
    """

    _log_cache = {}
    _RE_EVENT = re.compile(r'\[scan (complete|failed)\] library=(\d+)')
    _RE_PATH = re.compile(r'\[scan (?:complete|failed)\] library=\d+ path=(\S+)')
    _RE_NUM = {k: re.compile(r"'%s': (\d+)" % k)
               for k in ('total', 'new', 'missing', 'registered')}

    def __init__(self, P):
        super(ModuleStatus, self).__init__(P, name=name, first_menu='list')
        self.db_default = {
            f"{self.name}_db_version": "1",
            f"{self.name}_auto_refresh_min": "30",
        }

    def plugin_load(self):
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

    @classmethod
    def _storage(cls):
        """데이터 폴더가 있는 디스크의 사용량 + 데이터 폴더 용량(캐시).

        폴더 용량 측정은 비싸서 30분 캐시하고, 오래되면 백그라운드로 다시 잰다.
        """
        path = (P.ModelSetting.get('base_shyni_data_path') or '').strip()
        info = {'path': path}
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
        if (time.time() - (info['data_size_at'] or 0) > 1800
                or not raw_bd) \
                and cls._size_lock.acquire(False):
            threading.Thread(target=cls._measure_data_size,
                             args=(path,), daemon=True).start()
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
    @staticmethod
    def _sections():
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
            return None, f'shyni 라이브러리 목록을 가져오지 못했습니다: {e}'
        out = []
        for d in dirs:
            locs = d.get('locations') or []
            out.append({
                'key': d.get('key'),
                'title': d.get('title'),
                'type': d.get('type'),
                'path': locs[0].get('path') if locs else '',
            })
        return out, None

    # ---------- shyni 로그에서 스캔 이력 읽기 ----------
    @classmethod
    def _log_files(cls):
        base = (P.ModelSetting.get('base_shyni_log_path') or '').strip()
        if not base:
            return []
        if os.path.isfile(base):
            return [base]
        if not os.path.isdir(base):
            return []
        files, seen = [], set()
        for pat in (os.path.join(base, '*-json.log*'),
                    os.path.join(base, '*', '*-json.log*')):
            for f in glob.glob(pat):
                if f not in seen and os.path.isfile(f) and not f.endswith('.gz'):
                    seen.add(f)
                    files.append(f)
        files.sort(key=lambda f: os.path.getmtime(f))
        return files

    @classmethod
    def _parse_log_file(cls, path, offset=0, records=None):
        """파일에서 라이브러리별 전체 스캔(path=None) 최신 기록을 뽑는다.

        offset부터 새로 붙은 부분만 읽는 증분 파싱을 지원한다.
        쓰기 중인 마지막 줄(개행 없음)은 다음 호출로 미룬다.
        반환: (records, 다음 offset)
        """
        records = dict(records or {})
        try:
            with open(path, 'rb') as fp:
                fp.seek(offset)
                data = fp.read()
            cut = data.rfind(b'\n')
            if cut < 0:
                return records, offset
            data = data[:cut + 1]
            new_offset = offset + len(data)
            for line in data.decode('utf-8', errors='replace').splitlines():
                if '[scan complete]' not in line and '[scan failed]' not in line:
                    continue
                try:
                    env = json.loads(line)
                    text = env.get('log') or ''
                    ts_raw = env.get('time') or ''
                except Exception:
                    continue
                m = cls._RE_EVENT.search(text)
                if not m:
                    continue
                pm = cls._RE_PATH.search(text)
                if pm and pm.group(1) != 'None':
                    continue  # 부분 스캔은 라이브러리 전체 스캔 기록으로 쓰지 않음
                kind, lib = m.group(1), m.group(2)
                rec = {'result': 'done' if kind == 'complete' else 'failed',
                       'time': ts_raw}
                if kind == 'complete':
                    for k, rx in cls._RE_NUM.items():
                        mm = rx.search(text)
                        rec[k] = int(mm.group(1)) if mm else None
                prev = records.get(lib)
                if prev is None or ts_raw >= prev['time']:
                    records[lib] = rec
            return records, new_offset
        except Exception as e:
            logger.error(f'shyni 로그 읽기 실패 ({path}): {e}')
            return records, offset

    @staticmethod
    def _to_kst(ts_raw):
        try:
            dt = datetime.strptime(ts_raw[:19], '%Y-%m-%dT%H:%M:%S')
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone(timedelta(hours=9))).strftime('%m-%d %H:%M')
        except Exception:
            return ts_raw[:16].replace('T', ' ')

    @classmethod
    def _scan_history(cls):
        """로그 파일들을 병합해 라이브러리별 최신 스캔 기록을 만든다. (기록, 파일수)"""
        files = cls._log_files()
        merged = {}
        for f in files:
            try:
                st = os.stat(f)
            except Exception:
                continue
            cached = cls._log_cache.get(f)
            if cached and cached['ino'] == st.st_ino \
                    and cached['size'] == st.st_size and cached['mtime'] == st.st_mtime:
                recs = cached['records']
            elif cached and cached['ino'] == st.st_ino and st.st_size > cached['offset']:
                recs, off = cls._parse_log_file(f, cached['offset'], cached['records'])
                cls._log_cache[f] = {'ino': st.st_ino, 'size': st.st_size,
                                     'mtime': st.st_mtime, 'offset': off, 'records': recs}
            else:
                recs, off = cls._parse_log_file(f)
                cls._log_cache[f] = {'ino': st.st_ino, 'size': st.st_size,
                                     'mtime': st.st_mtime, 'offset': off, 'records': recs}
            for lib, rec in recs.items():
                prev = merged.get(lib)
                if prev is None or rec['time'] >= prev['time']:
                    merged[lib] = rec
        for rec in merged.values():
            rec['time_text'] = cls._to_kst(rec['time'])
        return merged, len(files)

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
    def _refresh_state(self, key, state):
        changed = False
        vfs = state.get('vfs')
        if vfs and vfs.get('status') == 'running' and vfs.get('job') is not None:
            addr, _ = self._rc_target(vfs.get('path', ''))
            if addr:
                try:
                    job = self._rc(addr, 'job/status', {'jobid': vfs['job']})
                    if job.get('finished'):
                        vfs['status'] = 'failed' if job.get('error') else 'done'
                        vfs['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        if job.get('error'):
                            vfs['error'] = str(job['error'])[:200]
                            logger.error(f"[현황] VFS 새로고침 실패: 섹션 {key} job {vfs['job']} — {vfs['error']}")
                        else:
                            logger.info(f"[현황] VFS 새로고침 완료: 섹션 {key} job {vfs['job']}")
                        changed = True
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
                except Exception:
                    pass
        meta = state.get('meta')
        if meta and meta.get('status') == 'running' \
                and str(key) not in _meta_refresh_threads:
            # 실행 스레드가 없는데 running으로 남아 있으면 재시작 등으로 끊긴 것이다
            meta['status'] = 'interrupted'
            meta['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logger.warning(f'[현황] 메타 새로고침 중단 상태로 표시: 섹션 {key}')
            changed = True
        scan = state.get('scan')
        if scan and scan.get('status') == 'running' and scan.get('job') is not None:
            url, token = ShyniBackend.get_config()
            if url and token:
                try:
                    res = requests.get(f"{url}/compat/jobs/{scan['job']}",
                                       headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                                       timeout=15)
                    if res.status_code == 200:
                        job = res.json()
                        if job.get('status') == 'completed':
                            scan['status'] = 'done'
                            scan['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            logger.info(f"[현황] 스캔 완료: 섹션 {key} job {scan['job']}")
                            changed = True
                        elif job.get('status') == 'failed':
                            scan['status'] = 'failed'
                            scan['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            scan['error'] = str(job.get('error') or '')[:200]
                            logger.error(f"[현황] 스캔 실패: 섹션 {key} job {scan['job']} — {scan['error']}")
                            changed = True
                except Exception:
                    pass
        if changed:
            self._set_state(key, state)
        return state

    # ---------- 화면 AJAX (로그인 세션용 /ajax/<module>/<cmd>) ----------
    def process_ajax(self, sub, req):
        if sub == 'db_stats':
            # 화면 폴링용: DB 통계가 준비됐는지와 진척률만 가볍게 돌려준다
            try:
                bundle, loading = self._db_stats()
                if type(self)._db_stats_busy:
                    # 강제 새로고침으로 재계산이 돌고 있으면 끝날 때까지 로딩중으로 본다
                    loading = True
            except Exception:
                bundle, loading = None, False
            return jsonify({'ret': 'success',
                            'ready': bundle is not None and not loading,
                            'db_off': bundle is None and not loading,
                            'progress': self._overall_progress(loading),
                            'libs': (bundle or {}).get('libs') or {}})
        if sub == 'list':
            cfg_url, cfg_token = ShyniBackend.get_config()
            if not cfg_url or not cfg_token:
                # 설정이 비어 있으면 오류가 아니라 안내 화면으로 보여 준다
                return jsonify({'ret': 'success', 'data': [], 'need_config': True,
                                'log_files': 0,
                                'refresh_min': self._refresh_min(),
                                'periodic_current': None, 'type_counts': {},
                                'db_loading': False, 'progress': 100,
                                'server': None, 'storage': None})
            # 한 번 불러온 정보는 새로고침 주기 동안 그대로 보여 주고, 주기가 지나거나
            # 강제 새로고침(force=1)일 때만 다시 가져온다.
            force = (req.form.get('force') or '') == '1'
            interval = self._refresh_min()
            ttl = interval * 60 if interval and interval >= 1 else 0
            now_ts = time.time()
            base = None
            from_cache = False
            cache = type(self)._list_cache
            if cache['data'] is None:
                # 재시작 직후에는 디스크에 저장해 둔 지난 복사본으로라도 먼저 보여 준다
                try:
                    saved_raw = P.ModelSetting.get('status_list_cache')
                    if saved_raw:
                        saved = json.loads(saved_raw)
                        if isinstance(saved, dict) and saved.get('data'):
                            cache = {'at': saved.get('at') or 0, 'data': saved['data']}
                            type(self)._list_cache = cache
                except Exception:
                    pass
            if not force and ttl > 0 and cache['data'] is not None \
                    and now_ts - cache['at'] < ttl:
                base = cache['data']
                from_cache = True
            if base is None:
                sections, err = self._sections()
                if err:
                    return jsonify({'ret': 'fail', 'msg': err})
                history, log_file_count = self._scan_history()
                rows = []
                for sec in sections:
                    state = self._refresh_state(sec['key'], self._get_state(sec['key']))
                    sec['state'] = state
                    sec['last_scan'] = history.get(str(sec['key']))
                    rows.append(sec)
                periodic_current = None
                raw_cur = P.ModelSetting.get('periodic_current')
                if raw_cur:
                    try:
                        cur = json.loads(raw_cur)
                        if isinstance(cur, dict):
                            periodic_current = cur
                    except Exception:
                        pass
                type_counts = {}
                for sec in sections:
                    t = sec.get('type') or 'unknown'
                    type_counts[t] = type_counts.get(t, 0) + 1
                base = {'data': rows, 'log_files': log_file_count,
                        'periodic_current': periodic_current,
                        'type_counts': type_counts,
                        'server': self._health(), 'storage': self._storage(),
                        'cached_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                type(self)._list_cache = {'at': now_ts, 'data': base}
                try:
                    P.ModelSetting.set('status_list_cache',
                                       json.dumps({'at': now_ts, 'data': base}))
                except Exception:
                    pass
            rows = base['data']
            # 진행 중으로 표시된 작업이 있으면, 캐시로 답하는 경우에도 상태만큼은
            # 그 자리에서 다시 확인한다 (끝난 작업이 다음 주기까지 '진행 중'으로 남지 않게)
            for sec in rows:
                st = sec.get('state') or {}
                vfs_st = (st.get('vfs') or {}).get('status')
                scan_st = (st.get('scan') or {}).get('status')
                if vfs_st == 'running' or scan_st in ('running', 'requested'):
                    try:
                        sec['state'] = self._refresh_state(sec['key'], self._get_state(sec['key']))
                    except Exception:
                        pass
            periodic_current = base['periodic_current']
            db_stats_on = False
            health = None
            db_loading = False
            try:
                bundle, db_loading = self._db_stats(force=force)
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
            return jsonify({'ret': 'success', 'data': rows,
                            'log_files': base['log_files'],
                            'refresh_min': interval,
                            'periodic_current': periodic_current,
                            'type_counts': base['type_counts'],
                            'db_stats_on': db_stats_on,
                            'db_loading': db_loading,
                            'progress': self._overall_progress(db_loading),
                            'health': health,
                            'from_cache': from_cache,
                            'cached_at': base.get('cached_at'),
                            'server': base['server'],
                            'storage': base['storage']})

        if sub == 'set_refresh':
            try:
                minutes = int(req.form.get('min'))
            except Exception:
                return jsonify({'ret': 'fail', 'msg': '분 값을 숫자로 입력해 주세요.'})
            minutes = max(0, min(120, minutes))
            P.ModelSetting.set('status_auto_refresh_min', str(minutes))
            if minutes == 0:
                return jsonify({'ret': 'success', 'msg': '자동 업데이트를 껐습니다.'})
            return jsonify({'ret': 'success', 'msg': f'자동 업데이트 주기를 {minutes}분으로 저장했습니다.'})

        if sub in ('vfs_refresh', 'scan', 'cancel', 'meta_refresh', 'vfs_scan'):
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

            if sub == 'cancel':
                # 주기적 스캔이 이 라이브러리를 실행 중이면 그쪽 취소를 요청한다
                raw_cur = P.ModelSetting.get('periodic_current')
                if raw_cur:
                    try:
                        cur = json.loads(raw_cur)
                    except Exception:
                        cur = None
                    if isinstance(cur, dict) and str(cur.get('section_key')) == str(key):
                        P.ModelSetting.set('periodic_cancel', '1')
                        logger.info(f"[현황] 주기적 스캔 취소 요청: {sec['title']}")
                        return jsonify({'ret': 'success',
                                        'msg': '주기적 스캔 취소를 요청했습니다. 진행 중인 단계가 정리되면 멈추고 하이라이트가 해제됩니다.'})
                meta = state.get('meta')
                if meta and meta.get('status') == 'running':
                    meta['cancel'] = True
                    self._set_state(key, state)
                    logger.info(f"[현황] 메타 새로고침 취소 요청: {sec['title']}")
                    return jsonify({'ret': 'success',
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
                    logger.info(f"[현황] VFS 새로고침 취소: {sec['title']} job {vfs['job']}")
                    return jsonify({'ret': 'success', 'msg': f"{sec['title']} VFS 새로고침을 취소했습니다."})
                scan = state.get('scan')
                if scan and scan.get('status') == 'running':
                    return jsonify({'ret': 'fail', 'msg': '스캔 취소는 지금 사용 중인 토큰으로는 할 수 없습니다. shyni 웹 화면에서 취소해 주세요.'})
                return jsonify({'ret': 'fail', 'msg': '지금 취소할 수 있는 진행 중 작업이 없습니다.'})

            # 시작 계열 — 같은 섹션에 실행 중 작업이 있으면 중복 시작 금지
            for kind in ('vfs', 'scan', 'meta'):
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
                state['vfs'] = {'job': ret.get('jobid'), 'path': sec['path'],
                                'started_at': now, 'finished_at': None, 'status': 'running'}
                self._set_state(key, state)
                logger.info(f"[현황] VFS 새로고침 시작: {sec['title']} job {ret.get('jobid')} ({remote})")
                return jsonify({'ret': 'success', 'msg': f"{sec['title']} VFS 새로고침을 시작했습니다."})

            if sub == 'vfs_scan':
                addr, remote = self._rc_target(sec['path'])
                if not addr:
                    return jsonify({'ret': 'fail', 'msg': '이 라이브러리 경로가 VFS 새로고침 규칙과 맞지 않습니다. 스캔 설정의 규칙을 확인해 주세요.'})
                try:
                    ret = self._rc(addr, 'vfs/refresh',
                                   {'dir': remote, 'recursive': 'true', '_async': 'true'})
                except Exception as e:
                    return jsonify({'ret': 'fail', 'msg': f'VFS 새로고침을 시작하지 못했습니다: {e}'})
                state['vfs'] = {'job': ret.get('jobid'), 'path': sec['path'],
                                'started_at': now, 'finished_at': None, 'status': 'running',
                                'then_scan': True}
                self._set_state(key, state)
                th = threading.Thread(target=self._run_vfs_scan,
                                      args=(key, sec['title'], ret.get('jobid')), daemon=True)
                _vfs_scan_threads[str(key)] = th
                th.start()
                logger.info(f"[현황] 새로고침 후 스캔 시작: {sec['title']} job {ret.get('jobid')} ({remote})")
                return jsonify({'ret': 'success', 'msg': f"{sec['title']} 새로고침을 시작했습니다. 끝나면 스캔이 자동으로 시작됩니다."})

            if sub == 'meta_refresh':
                from . import db_support
                if not db_support.db_ready():
                    return jsonify({'ret': 'fail',
                                    'msg': '메타 새로고침은 고급 설정에서 PostgreSQL Relay 연결 테스트를 통과해야 사용할 수 있습니다.'})
                scope = (req.form.get('scope') or 'all').strip()
                only_no_code = scope == 'nocode'
                try:
                    ids = db_support.album_ids(key, only_no_code=only_no_code)
                except Exception as e:
                    return jsonify({'ret': 'fail', 'msg': f'앨범 목록을 읽지 못했습니다: {e}'})
                if not ids:
                    return jsonify({'ret': 'fail',
                                    'msg': '메타 없는 앨범이 없습니다.' if only_no_code else '이 라이브러리에는 앨범이 없습니다.'})
                state['meta'] = {'status': 'running', 'total': len(ids), 'done': 0, 'fail': 0,
                                 'cancel': False,
                                 'started_at': now, 'finished_at': None}
                self._set_state(key, state)
                run_id = int(time.time())
                th = threading.Thread(target=self._run_meta_refresh,
                                      args=(key, sec['title'], ids, run_id), daemon=True)
                _meta_refresh_threads[str(key)] = th
                th.start()
                scope_label = '메타 없는 앨범' if only_no_code else '앨범'
                logger.info(f"[현황] 메타 새로고침 시작: {sec['title']} {scope_label} {len(ids)}개 (범위 {scope})")
                return jsonify({'ret': 'success',
                                'msg': f"{sec['title']} 메타 새로고침을 시작했습니다. {scope_label} {len(ids)}개를 순서대로 처리합니다."})

            if sub == 'scan':
                url, token = ShyniBackend.get_config()
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
                state['scan'] = {'job': job_id, 'path': sec['path'],
                                 'started_at': now, 'finished_at': None,
                                 'status': 'running' if job_id else 'requested'}
                self._set_state(key, state)
                logger.info(f"[현황] 스캔 전송: {sec['title']} job {job_id}")
                return jsonify({'ret': 'success', 'msg': f"{sec['title']} 스캔을 전송했습니다."})

        return jsonify({'ret': 'fail', 'msg': '알 수 없는 요청입니다.'})

    _db_stats_cache = {'at': 0, 'data': None}
    _db_stats_busy = False
    _db_stats_progress = 0
    _list_cache = {'at': 0, 'data': None}

    def _db_stats(self, force=False):
        """DB 통계를 (데이터, 로딩중) 으로 돌려준다. 계산은 Celery 워커(task_status)가
        전담하고, 웹은 저장된 결과만 읽어 즉시 돌려준다 — 계산 중에도 화면이
        무거워지지 않는다. 결과가 없거나 오래됐거나 force(강제 새로고침)면
        워커에 계산 요청만 남긴다. 워커가 저장한 최신 결과가 메모리 복사본보다
        새로우면 받아들여, 옛 결과가 화면에 고착되지 않게 한다."""
        from .db_support import db_ready
        if not db_ready():
            return None, False
        from .task_status import read_state, request_stats
        cls = type(self)
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
        state = read_state()
        pending = (state.get('requested_at') or 0) > (state.get('finished_at') or 0)
        cls._db_stats_busy = bool(state.get('busy')) or pending
        cls._db_stats_progress = state.get('progress') or 0
        cached = cls._db_stats_cache['data']
        if cached is not None:
            if force:
                request_stats(force=True)
                state = read_state()
                pending = (state.get('requested_at') or 0) > (state.get('finished_at') or 0)
                cls._db_stats_busy = bool(state.get('busy')) or pending
                cls._db_stats_progress = state.get('progress') or 0
                return cached, cls._db_stats_busy
            try:
                stale_after = max(60, int(self._refresh_min()) * 60)
            except Exception:
                stale_after = 60
            if time.time() - cls._db_stats_cache['at'] >= stale_after:
                request_stats()
            return cached, False
        request_stats()
        return None, True

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
            card['missing_any'] = missing_any
            card['ok'] = st['main_count'] - missing_any
            card['check_pct'] = round(missing_any * 100.0 / st['main_count'], 1) if st['main_count'] else 0
            card['code_pct'] = round(st['no_code'] * 100.0 / st['main_count'], 1) if st['main_count'] else 0
            card['avg_sub'] = round(st['sub_count'] * 1.0 / st['main_count'], 1) if st['main_count'] else 0
            card['avg_tracks'] = card['avg_sub']
            lib_cards.append(card)
            if missing_any:
                lib_no_code.append({'key': sec['key'], 'title': sec.get('title'),
                                    'works': st['main_count'], 'main_label': st['main_label'],
                                    'albums': st['main_count'], 'no_code': st['no_code'],
                                    'missing_any': missing_any})
        lib_no_code.sort(key=lambda x: -x['no_code'])
        scan = {'periodic_running': bool(periodic_current),
                'running': 0, 'done': 0, 'failed': 0}
        try:
            with F.app.app_context():
                q = F.db.session.query(P.ModelScanItem)
                scan['running'] = q.filter_by(shyni_status='RUNNING').count()
                scan['done'] = q.filter_by(status='FINISH_SHYNI').count()
                scan['failed'] = q.filter_by(status='FINISH_SHYNI_FAILED').count()
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
            proceed = False
            deadline = time.time() + 6 * 3600
            while time.time() < deadline:
                st = self._get_state(key)
                vfs = (st or {}).get('vfs') or {}
                if vfs.get('job') != jobid:
                    return
                if vfs.get('status') == 'done':
                    proceed = True
                    break
                if vfs.get('status') != 'running':
                    # cancelled / failed / interrupted — 스캔으로 이어지지 않는다
                    logger.info(f"[현황] 새로고침 후 스캔 중단: {title} (새로고침 {vfs.get('status')})")
                    return
                addr, _ = self._rc_target(vfs.get('path', ''))
                if not addr:
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
                    logger.error(f'[현황] 새로고침 후 스캔 중단(새로고침 실패): {title} — {vfs["error"]}')
                    return
                vfs['status'] = 'done'
                vfs['finished_at'] = now
                self._set_state(key, st)
                logger.info(f'[현황] VFS 새로고침 완료: 섹션 {key} job {jobid}')
                proceed = True
                break
            if not proceed:
                logger.warning(f'[현황] 새로고침 후 스캔 시간 초과로 중단: {title} job {jobid}')
                return
            # 이어서 스캔 전송 — 이미 스캔이 걸려 있으면 건너뛴다
            st = self._get_state(key)
            scan = (st or {}).get('scan') or {}
            if scan.get('status') in ('running', 'requested'):
                logger.info(f'[현황] 새로고침 후 스캔 건너뜀(이미 스캔 중): {title}')
                return
            vfs_path = ((st or {}).get('vfs') or {}).get('path')
            url, token = ShyniBackend.get_config()
            try:
                res = requests.get(f"{url}/library/sections/{key}/refresh",
                                   headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                                   timeout=30)
                res.raise_for_status()
            except Exception as e:
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
                          'status': 'running' if job_id else 'requested'}
            self._set_state(key, st)
            logger.info(f'[현황] 새로고침 후 스캔 전송: {title} job {job_id}')
        finally:
            _vfs_scan_threads.pop(str(key), None)

    def _run_meta_refresh(self, key, title, ids, run_id):
        """라이브러리 전체 앨범에 메타 새로고침을 하나씩 건다 (백그라운드 스레드)."""
        url, token = ShyniBackend.get_config()
        done = 0
        fail = 0
        cancelled = False
        try:
            for mid in ids:
                st = self._get_state(key)
                meta = (st or {}).get('meta') or {}
                if meta.get('cancel'):
                    cancelled = True
                    break
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
                if (done + fail) % 20 == 0:
                    st = self._get_state(key)
                    if st.get('meta'):
                        st['meta']['done'] = done
                        st['meta']['fail'] = fail
                        self._set_state(key, st)
                time.sleep(0.15)
        finally:
            st = self._get_state(key)
            if st.get('meta'):
                st['meta']['done'] = done
                st['meta']['fail'] = fail
                st['meta']['status'] = 'cancelled' if cancelled else 'done'
                st['meta']['finished_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self._set_state(key, st)
            _meta_refresh_threads.pop(str(key), None)
            logger.info(f'[현황] 메타 새로고침 {"취소됨" if cancelled else "완료"}: '
                        f'{title} 처리 {done} · 실패 {fail} / 전체 {len(ids)}')

    # 외부 API(/api/<module>/<cmd>)로 들어와도 같은 처리를 한다
    def process_api(self, sub, req):
        return self.process_ajax(sub, req)
