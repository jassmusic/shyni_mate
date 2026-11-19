# -*- coding: utf-8 -*-
setting = {
    'filepath' : __file__,
    'use_db': True,
    'use_default_setting': True,
    'home_module': None,
    'menu': {
        'uri': __package__,
        'name': 'SHYNI MATE',
        'list': [
            {
                'uri': 'base',
                'name': '설정',
                'list': [
                    {'uri': 'setting', 'name': '설정'},
                    {'uri': 'advanced', 'name': 'Relay'},
                    {'uri': 'webhook', 'name': '웹훅'},
                ]
            },
            {
                'uri': 'status',
                'name': '대시보드',
                'list': [
                    {'uri': 'list', 'name': '대시보드'},
                    {'uri': 'health', 'name': '건강도'},
                    {'uri': 'sessions', 'name': '세션'},
                ]
            },
            {
                'uri': 'scan',
                'name': '스캔',
                'list': [
                    {'uri': 'setting', 'name': '설정'},
                    {'uri': 'list', 'name': '스캔 목록'},
                    {'uri': 'browser', 'name': '탐색'},
                    {'uri': 'manual', 'name': '매뉴얼'},
                ]
            },
            {
                'uri': 'periodic',
                'name': '주기적 스캔',
                'list': [
                    {'uri': 'setting', 'name': '설정'},
                    {'uri': 'jobs', 'name': '작업 관리'},
                    {'uri': 'result', 'name': '작업 결과'},
                    {'uri': 'manual', 'name': '매뉴얼'},
                ]
            },
            {
                'uri': 'trash',
                'name': '휴지통 스캔',
                'list': [
                    {'uri': 'setting', 'name': '설정'},
                    {'uri': 'trash', 'name': '휴지통'},
                    {'uri': 'jobs', 'name': '작업 관리'},
                    {'uri': 'result', 'name': '작업 결과'},
                    {'uri': 'manual', 'name': '매뉴얼'},
                ]
            },
            {
                'uri': 'meta',
                'name': '메타',
                'list': [
                    {'uri': 'list', 'name': '메타'},
                    {'uri': 'manual', 'name': '매뉴얼'},
                ]
            },
            {
                'uri': 'manual_toc',
                'name': '매뉴얼',
            },
            {
                'uri': 'log',
                'name': '로그',
            },
        ]
    },
    'setting_menu': None,
    'default_route': 'normal',
}


from plugin import *

P = create_plugin_instance(setting)

try:
    from .mod_base import ModuleBase
    from .mod_scan import ModelScanItem, ModuleScan
    from .mod_status import ModuleStatus
    from .mod_periodic import ModulePeriodic
    from .model_periodic import ModelPeriodicItem
    from .mod_trash import ModuleTrash
    from .model_trash import ModelTrashItem
    from .mod_meta import ModuleMeta
    from .mod_webhook import ModuleWebhook

    P.set_module_list([ModuleBase, ModuleScan, ModuleStatus, ModulePeriodic, ModuleTrash, ModuleMeta, ModuleWebhook])

    # 외부 호출
    P.ModelScanItem = ModelScanItem
except Exception as e:
    P.logger.error(f'Exception:{str(e)}')
    P.logger.error(traceback.format_exc())

logger = P.logger

P.cache = F.get_cache('shyni_mate')
