# -*- coding: utf-8 -*-
import json
import threading
import urllib.parse

from flask import abort, request

from .setup import *
from .task_webhook import TaskWebhook, handle_event

name = 'webhook'
logger = P.logger

ROUTE_ENDPOINT = 'shyni_mate_webhook_shyni'


def _receive():
    """shyni 웹훅 수신 — 큐·버퍼에 쌓기만 하고 바로 응답한다.

    shyni 쪽 발신 제한 시간이 5초라, 여기서 Discord 전송 같은 오래 걸리는
    일을 하면 안 된다. 실제 전송은 워커(task_webhook)가 맡는다.
    """
    if F.SystemModelSetting.get_bool('use_apikey'):
        if request.args.get('apikey') != F.SystemModelSetting.get('apikey'):
            abort(403)
    try:
        raw = request.form.get('payload')
        if not raw:
            return jsonify({'status': 'invalid'})
        data = json.loads(raw)
        thumb = None
        thumb_file = request.files.get('thumb')
        if thumb_file is not None:
            thumb = thumb_file.read()
        handle_event(data, thumb)
        return jsonify({'status': 'ok'})
    except Exception as e:
        logger.error(f'[Webhook] 수신 처리 오류: {e}')
        return jsonify({'status': 'invalid'})


def receive_url():
    """shyni 웹훅 설정에 붙여넣을 이 플러그인의 수신 주소를 만든다."""
    path = f'{P.package_name}/webhook/shyni'
    query = {}
    if F.SystemModelSetting.get_bool('use_apikey'):
        query['apikey'] = F.SystemModelSetting.get('apikey')
    full = f'{path}?{urllib.parse.urlencode(query)}' if query else path
    return urllib.parse.urljoin(F.SystemModelSetting.get('ddns') or '', full)


class ModuleWebhook(PluginModuleBase):
    """shyni가 보내는 웹훅을 받아 Discord로 다시 보내는 중계 모듈.

    수신 방식과 기본 설정 틀은 Plex 웹훅 알림에서 쓰던 방식을 가져왔고,
    메시지 내용과 신규 항목 묶음은 shyni에 맞게 새로 만들었다.
    """

    def __init__(self, P):
        super(ModuleWebhook, self).__init__(P, name=name, first_menu='setting')
        self.db_default = {
            f'{self.name}_db_version': '1',
            f'{self.name}_use': 'False',
            f'{self.name}_discord_url': '',
            f'{self.name}_play_events': 'play, pause, resume, stop, scrobble',
            f'{self.name}_library_use': 'True',
            f'{self.name}_library_interval_min': '5',
            f'{self.name}_attach_thumb': 'True',
        }
        self._register_route()

    def _register_route(self):
        # 플러그인이 다시 로드돼도 같은 주소를 두 번 열지 않는다
        try:
            if ROUTE_ENDPOINT in F.app.view_functions:
                return
            path = f'/{P.package_name}/webhook/shyni'
            F.app.add_url_rule(path, endpoint=ROUTE_ENDPOINT,
                               view_func=_receive, methods=['POST'])
            logger.info(f'[Webhook] 수신 주소 등록: {path}')
        except Exception as e:
            logger.error(f'[Webhook] 수신 주소 등록 실패: {e}')

    def plugin_load(self):
        def func():
            self.start_celery(TaskWebhook.start)
        thread = threading.Thread(target=func, args=())
        thread.daemon = True
        thread.start()

    def process_menu(self, sub, req):
        arg = P.ModelSetting.to_dict()
        arg['api_webhook'] = receive_url()
        try:
            return render_template(f'{P.package_name}_{self.name}_{sub}.html',
                                   arg=arg)
        except Exception as e:
            logger.error(f'Exception:{str(e)}')
            logger.error(traceback.format_exc())
            return render_template('sample.html',
                                   title=f'{P.package_name}/{self.name}/{sub}')
