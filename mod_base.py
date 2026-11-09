import requests
import xmltodict

from .setup import *

logger = P.logger


class ModuleBase(PluginModuleBase):

    def __init__(self, P):
        super(ModuleBase, self).__init__(P, name='base', first_menu='setting')
        self.db_default = {
            f'{self.name}_db_version' : '1',
            f'{self.name}_shyni_url' : 'http://localhost:7182',
            f'{self.name}_shyni_token' : '',
        }


    def process_menu(self, sub, req):
        arg = P.ModelSetting.to_dict()
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
                return jsonify({'ret':'warning', 'msg':'shyni 주소와 토큰을 입력하세요.'})
            try:
                res = requests.get(f'{url}/library/sections',
                                   headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                                   timeout=10)
                if res.status_code != 200:
                    return jsonify({'ret':'warning', 'msg':f'응답 코드 {res.status_code} — 주소와 토큰을 확인하세요.'})
                try:
                    data = res.json()
                    dirs = data.get('MediaContainer', {}).get('Directory', [])
                except Exception:
                    data = xmltodict.parse(res.text)
                    dirs = data.get('MediaContainer', {}).get('Directory', [])
                    if isinstance(dirs, dict):
                        dirs = [dirs]
                lines = [f"연결 성공 — 라이브러리 {len(dirs)}개"]
                for d in dirs:
                    lines.append(f"- [{d.get('key')}] {d.get('title')} ({d.get('type')})")
                ret['title'] = 'shyni 연결 테스트'
                ret['modal'] = '<br>'.join(lines)
            except Exception as e:
                ret = {'ret':'warning', 'msg':f'연결 실패: {e}'}
        return jsonify(ret)
