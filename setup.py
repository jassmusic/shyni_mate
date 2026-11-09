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
                ]
            },
            {
                'uri': 'scan',
                'name': '스캔',
                'list': [
                    {'uri': 'setting', 'name': '설정'},
                    {'uri': 'list', 'name': '스캔 목록'},
                    {'uri': 'browser', 'name': '탐색'},
                    {'uri': 'manual/files/스캔.md', 'name': '매뉴얼'},
                ]
            },
            {
                'uri': 'manual',
                'name': '매뉴얼',
                'list': [
                    {'uri': 'README.md', 'name': 'README'},
                ]
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

    P.set_module_list([ModuleBase, ModuleScan])

    # 외부 호출
    P.ModelScanItem = ModelScanItem
except Exception as e:
    P.logger.error(f'Exception:{str(e)}')
    P.logger.error(traceback.format_exc())

logger = P.logger

P.cache = F.get_cache('shyni_mate')
