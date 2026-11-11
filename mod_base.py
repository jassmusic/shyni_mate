import json
import threading

import requests
import xmltodict

from .setup import *

logger = P.logger


class ModuleBase(PluginModuleBase):

    def __init__(self, P):
        super(ModuleBase, self).__init__(P, name='base', first_menu='setting')
        self.db_default = {
            f'{self.name}_db_version' : '1',
            f'{self.name}_shyni_url' : '',
            f'{self.name}_shyni_token' : '',
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
            # 고급 설정 — 시간 제한 (초). 응답이 늦으면 그 주기는 건너뛰고 다음 주기에 다시 시도한다
            f'{self.name}_timeout_query' : '15',
            f'{self.name}_timeout_send' : '30',
            f'{self.name}_timeout_poll' : '15',
            f'{self.name}_timeout_callback' : '10',
            f'{self.name}_timeout_vfs_rc' : '600',
            # 현황·건강도 — 휴지통 개수 자동 갱신 주기 (시간). 0이면 통계 때마다 읽는다
            f'{self.name}_trash_counts_refresh_hours' : '12',
        }


    def plugin_load(self):
        # DB 드라이버 준비는 플러그인 로드를 막지 않도록 별도 스레드로 한다
        def _prep():
            try:
                from .db_support import ensure_driver
                ensure_driver()
            except Exception as e:
                logger.error(f'Exception:{str(e)}')
                logger.error(traceback.format_exc())
        threading.Thread(target=_prep, daemon=True).start()


    def process_menu(self, sub, req):
        arg = P.ModelSetting.to_dict()
        if sub == 'advanced':
            try:
                from .db_support import db_ready
                arg['db_ok'] = db_ready()
            except Exception:
                arg['db_ok'] = False
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
        if command == 'db_test':
            return jsonify(self._db_test(arg1, arg2, arg3))
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
