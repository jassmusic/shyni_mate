import json
import os
import threading

import requests
import xmltodict
from flask_login import login_required

from .setup import *
from .version_info import (RUNNING_VERSION, get_github_version,
                           is_newer, refresh_github_version_async)

logger = P.logger


def _backup_db_once_for_encryption():
    """비밀 설정값 암호화에 앞서 mate DB를 한 번만 백업한다.

    백업 이름은 shyni_mate.db.bak-pre-0.9.9m이고, 이미 있으면 덮지
    않는다. sqlite3 backup API로 복사해 WAL 상태에서도 일관된
    복사본이 되게 한다. 실패해도 예외를 밖으로 내지 않는다 — 호출자가
    로그를 보고 판단한다. 백업을 만들었거나 이미 있으면 True.
    """
    try:
        from . import secret_store
        src = secret_store.db_path()
        dst = src + '.bak-pre-0.9.9m'
        if os.path.exists(dst):
            return True
        if not os.path.exists(src):
            return False
        import sqlite3
        src_conn = sqlite3.connect(f'file:{src}?mode=ro', uri=True)
        try:
            dst_conn = sqlite3.connect(dst)
            try:
                with dst_conn:
                    src_conn.backup(dst_conn)
            finally:
                dst_conn.close()
        finally:
            src_conn.close()
        logger.info('[Base] 비밀 설정값 암호화에 앞서 mate DB를 백업했습니다.')
        return True
    except Exception as e:
        logger.error(f'[Base] mate DB 백업에 실패했습니다: {e}')
        return False


class ModuleBase(PluginModuleBase):

    def __init__(self, P):
        super(ModuleBase, self).__init__(P, name='base', first_menu='setting')
        self.db_default = {
            f'{self.name}_db_version' : '1',
            f'{self.name}_shyni_url' : '',
            f'{self.name}_shyni_token' : '',
            # 관리자 계정 — 관리자 API용 세션 토큰 발급에 쓴다 (Step2·3).
            # 비밀번호·세션 토큰은 암호화해서 저장한다 (secret_store).
            f'{self.name}_shyni_admin_id' : '',
            f'{self.name}_shyni_admin_pw' : '',
            f'{self.name}_shyni_session_token' : '',
            # GHCR 토큰 — 대시보드에 shyni 최신 버전을 표시할 때 쓴다.
            # 비어 있으면 최신 버전 표시를 하지 않는다.
            f'{self.name}_ghcr_token' : '',
            f'{self.name}_shyni_log_path' : '',
            f'{self.name}_shyni_data_path' : '',
            # 고급 설정 — PostgreSQL Relay (배포 기본값은 표준값을 넣고 비밀번호만 비워 둔다)
            f'{self.name}_db_use' : 'False',
            f'{self.name}_db_host' : 'pgrelay_out',
            f'{self.name}_db_port' : '15432',
            f'{self.name}_db_name' : 'shyni',
            f'{self.name}_db_user' : 'shyni_ro',
            f'{self.name}_db_password' : '',
            f'{self.name}_db_ok_hash' : '',
            # 시간 제한(base_timeout_*) 기본값은 스캔 모듈(mod_scan.py)의
            # db_default에 한 곳으로 모았다 — 두 곳에 두면 한쪽만 바뀌는
            # 함정이 된다.
            # 현황·건강도 — 휴지통 개수 자동 갱신 주기 (시간). 0이면 통계 때마다 읽는다
            f'{self.name}_trash_counts_refresh_hours' : '12',
            # 라이브러리 동시 스캔 수 (기본 설정 기타 탭) — db_default에 없으면
            # 새로 설치한 서버에서도 설정 저장이 실패한다 (2026-11-18 쉴드 실측).
            # 키 이름은 mod_status.py의 LIBRARY_SCAN_LIMIT_KEY와 같은 값이다.
            'scan_max_library_scan_count' : '2',
        }
        # 매뉴얼 페이지: /shyni_mate/manual_toc — 소개 매뉴얼을 FF 안에서
        # 템플릿으로 보여준다. 정적 라우트라 기본 라우트 /<sub>보다 우선 매칭된다.
        try:
            P.blueprint.add_url_rule('/manual_toc', 'manual_toc', login_required(self._manual_toc))
        except Exception as e:
            logger.warning(f'[Base] 매뉴얼 라우트 등록 실패: {e}')


    def _manual_toc(self):
        # 소개 매뉴얼(설치·기능·라이선스) — FF 크롬 안에서 목차형으로 표시한다.
        arg = P.ModelSetting.to_dict()
        return render_template(f'{P.package_name}_base_manual.html', arg=arg)


    def plugin_load(self):
        # 비밀 설정값 암호화 마이그레이션 — 평문으로 저장된 대상 키를
        # 암호문으로 되쓴다 (멱등, secret_store 참고).
        self._migrate_secret_settings()
        # mate DB를 WAL 모드로 — 쓰기 몰림 때 웹 읽기가 'database is locked'로
        # 실패하고 페이지가 버벅거리는 것을 막는다 (저널 모드는 DB 파일에 영구 저장).
        try:
            from .extensions import ensure_wal
            ensure_wal()
        except Exception:
            pass
        # DB 드라이버 준비는 플러그인 로드를 막지 않도록 별도 스레드로 한다
        def _prep():
            try:
                from .db_support import ensure_driver
                ensure_driver()
            except Exception as e:
                logger.error(f'Exception:{str(e)}')
                logger.error(traceback.format_exc())
        threading.Thread(target=_prep, daemon=True).start()
        # GitHub 최신 버전 캐시를 미리 채워 둔다 (실패해도 무시한다)
        try:
            refresh_github_version_async()
        except Exception:
            pass


    def _migrate_secret_settings(self):
        """평문으로 저장된 비밀 설정값을 암호문으로 되쓴다 (멱등).

        대상 키 중 평문이 있으면 먼저 mate DB를 백업하고(1회만) 나서
        암호화한다. 이미 암호문이거나 비어 있는 키는 건드리지 않아,
        몇 번을 다시 실행해도 결과가 같다. 백업에 실패하면 암호화하지
        않는다 — 되돌릴 복사본 없이 값을 바꾸지 않기 위해서다.
        """
        try:
            from . import secret_store
            plain_keys = []
            for key in sorted(secret_store.SECRET_KEYS):
                value = P.ModelSetting.get(key)
                if value and not secret_store.is_encrypted(value):
                    plain_keys.append(key)
            if not plain_keys:
                return
            if not _backup_db_once_for_encryption():
                logger.error('[Base] mate DB 백업을 만들지 못해 비밀 설정값 '
                             '암호화를 건너뜁니다.')
                return
            done = 0
            for key in plain_keys:
                value = P.ModelSetting.get(key)
                encrypted = secret_store.encrypt_value(value)
                if secret_store.is_encrypted(encrypted):
                    P.ModelSetting.set(key, encrypted)
                    done += 1
                else:
                    logger.error(f'[Base] 비밀 설정값 암호화에 실패했습니다: {key}')
            if done:
                logger.info(f'[Base] 비밀 설정값 {done}개를 암호화해서 저장했습니다.')
        except Exception as e:
            logger.error(f'Exception:{str(e)}')
            logger.error(traceback.format_exc())


    def setting_save_after(self, change_list):
        """설정 저장 직후 — 평문으로 저장된 비밀 설정값을 암호문으로 되쓴다.

        프레임워크의 설정 저장은 폼 값을 그대로 DB에 넣으므로, 저장 후크에서
        대상 키만 골라 암호화한다. 화면에는 복호화된 값을 보여 주기 때문에,
        사용자가 다른 항목만 바꿔 저장해도 대상 키가 바뀐 것으로 잡혀
        change_list에 들어올 수 있다 — 그 경우에도 여기서 암호화된다.
        """
        try:
            from . import secret_store
            for key in (change_list or []):
                if not secret_store.is_secret_key(key):
                    continue
                value = P.ModelSetting.get(key)
                if value and not secret_store.is_encrypted(value):
                    encrypted = secret_store.encrypt_value(value)
                    if secret_store.is_encrypted(encrypted):
                        P.ModelSetting.set(key, encrypted)
        except Exception as e:
            logger.error(f'Exception:{str(e)}')
            logger.error(traceback.format_exc())


    def process_menu(self, sub, req):
        arg = P.ModelSetting.to_dict()
        # 비밀 설정값은 DB에 암호문으로 저장돼 있다 — 설정 화면에는
        # 복호화된 실제 값을 보여 준다 (사용자가 값을 확인할 수 있어야
        # 한다는 확정 요건). 저장은 setting_save_after가 다시 암호화한다.
        try:
            from . import secret_store
            for key in secret_store.SECRET_KEYS:
                if arg.get(key):
                    arg[key] = secret_store.decrypt_value(arg[key])
        except Exception:
            pass
        if sub == 'setting':
            # 현재 버전(실행 중)은 기동 시 한 번 읽은 값이다.
            # 새 버전은 GitHub에서만 확인한다 (디스크는 매번 읽지 않는다).
            arg['plugin_version'] = RUNNING_VERSION or '-'
            github = get_github_version()
            arg['plugin_github_version'] = github or ''
            arg['plugin_github_newer'] = bool(
                github and is_newer(github, RUNNING_VERSION))
        if sub == 'advanced':
            try:
                from .db_support import db_ready
                arg['db_ok'] = db_ready()
            except Exception:
                arg['db_ok'] = False
        if sub == 'webhook':
            # 웹훅 설정은 최상위 메뉴에서 설정 메뉴 안으로 이동했다.
            # 수신 주소는 mod_webhook의 receive_url()을 그대로 재사용한다.
            try:
                from .mod_webhook import receive_url
                arg['api_webhook'] = receive_url()
            except Exception:
                arg['api_webhook'] = ''
        try:
            return render_template(f'{P.package_name}_{self.name}_{sub}.html', arg=arg)
        except Exception as e:
            logger.error(f'Exception:{str(e)}')
            logger.error(traceback.format_exc())
            return render_template('sample.html', title=f"{P.package_name}/{self.name}/{sub}")


    def process_command(self, command, arg1, arg2, arg3, req):
        ret = {'ret':'success'}
        if command == 'shyni_test':
            url = (arg1 or '').rstrip('/')
            token = arg2 or ''
            if not url or not token:
                return jsonify({'ret':'warning', 'msg':'shyni 주소와 토큰을 입력해 주세요.'})
            try:
                res = requests.get(f'{url}/library/sections',
                                   headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                                   timeout=10)
                if res.status_code != 200:
                    logger.warning(f'[Base] shyni 연결 테스트 실패: {url} 응답 코드 {res.status_code}')
                    return jsonify({'ret':'warning', 'msg':f'응답 코드가 {res.status_code}입니다. 주소와 토큰을 한 번 더 확인해 주세요.'})
                try:
                    data = res.json()
                    dirs = data.get('MediaContainer', {}).get('Directory', [])
                except Exception:
                    data = xmltodict.parse(res.text)
                    dirs = data.get('MediaContainer', {}).get('Directory', [])
                    if isinstance(dirs, dict):
                        dirs = [dirs]
                logger.info(f'[Base] shyni 연결 테스트 성공: {url} 라이브러리 {len(dirs)}개')
                lines = [f"연결에 성공했습니다. 라이브러리 {len(dirs)}개가 보입니다."]
                for d in dirs:
                    lines.append(f"- [{d.get('key')}] {d.get('title')} ({d.get('type')})")
                ret['title'] = 'shyni 연결 테스트'
                ret['modal'] = '<br>'.join(lines)
            except Exception as e:
                logger.error(f'[Base] shyni 연결 테스트 실패: {url} {e}')
                ret = {'ret':'warning', 'msg':f'연결에 실패했습니다: {e}'}
        if command == 'shyni_admin_test':
            from . import shyni_admin
            ok, msg, _is_admin = shyni_admin.test_credentials(
                (arg1 or '').strip(), arg2 or '')
            if ok:
                ret['title'] = 'shyni 관리자 로그인 테스트'
                ret['modal'] = (msg + '<br>설정을 저장하면 이 계정으로 '
                                '세션 토큰을 받아 관리자 API를 씁니다.')
            else:
                ret = {'ret': 'warning', 'msg': msg}
        if command == 'db_test':
            return jsonify(self._db_test(arg1, arg2, arg3))
        if command == 'scan_timeout_info':
            return jsonify(self._scan_timeout_info())
        return jsonify(ret)


    def _db_test(self, host, port, extra_json):
        """PostgreSQL Relay 연결 테스트. 성공하면 그 접속 정보의 지문을 남겨,
        설정이 그대로인 동안에만 DB 정보를 활용할 수 있게 한다."""
        from . import db_support
        try:
            extra = json.loads(extra_json or '{}')
        except Exception:
            extra = {}
        params = {
            'host': (host or '').strip(),
            'dbname': (extra.get('name') or 'shyni').strip() or 'shyni',
            'user': (extra.get('user') or '').strip(),
            'password': extra.get('password') or '',
        }
        try:
            params['port'] = int(port or 15432)
        except Exception:
            return {'ret': 'warning', 'msg': '포트를 숫자로 입력해 주세요.'}
        if not params['host'] or not params['user']:
            return {'ret': 'warning', 'msg': '중계 주소와 계정을 입력해 주세요.'}
        if not db_support.ensure_driver():
            P.ModelSetting.set('base_db_ok_hash', '')
            return {'ret': 'warning', 'msg': 'DB 드라이버(psycopg2)를 준비하지 못했습니다. 로그를 확인해 주세요.'}
        try:
            conn = db_support.connect(params, timeout=8)
            try:
                cur = conn.cursor()
                cur.execute('SELECT 1')
                cur.execute('SELECT count(*) FROM metadata_items WHERE metadata_type IN (1, 2, 9, 100)')
                albums = cur.fetchone()[0]
            finally:
                conn.close()
        except Exception as e:
            P.ModelSetting.set('base_db_ok_hash', '')
            logger.warning(f"[Base] DB 연결 테스트 실패: {params['host']}:{params['port']} {e}")
            return {'ret': 'warning', 'msg': f'연결에 실패했습니다: {e}'}
        P.ModelSetting.set('base_db_ok_hash', db_support.params_fingerprint(params))
        logger.info(f"[Base] DB 연결 테스트 성공: {params['host']}:{params['port']} "
                    f"db={params['dbname']} user={params['user']} 미디어 총 {albums}개")
        return {'ret': 'success', 'title': 'PostgreSQL Relay 연결 테스트',
                'modal': (f"연결에 성공했습니다. 전체 미디어 {albums:,}개가 조회됩니다. "
                          '에피소드·트랙까지 읽어오지 않고, 미디어 하나하나만 읽어 온 수입니다.<br>'
                          '대시보드에서 라이브러리별 미디어 수·항목 수·메타 없음 수를 확인할 수 있습니다.<br>'
                          '설정을 저장해 두시면, 설정을 바꾸기 전까지 이 연결 정보를 활용합니다.')}


    def _scan_timeout_info(self):
        """스캔 전역 설정 탭의 '파일 목록 응답 대기시간' 표시용 조회.
        shyni 라이브러리별 값을 관리자 API로 읽어 돌려준다 (읽기 전용).
        관리자 계정이 없거나 조회에 실패하면 fail."""
        from . import shyni_admin
        if not shyni_admin.admin_available():
            return {'ret': 'fail'}
        items = shyni_admin.api_library_scan_timeouts()
        if not items:
            return {'ret': 'fail'}
        return {'ret': 'success', 'items': items}
