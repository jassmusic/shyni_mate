import os
import json
import time
import base64
import shutil
import urllib
import pathlib
import sqlite3
import datetime
import functools
import traceback

from typing import Any, Optional, Union, Callable
from threading import Thread

import flask
import requests
from sqlalchemy.orm import object_session

from support import SupportSubprocess
from support import AlchemyEncoder
from plugin.create_plugin import PluginBase
from plugin.logic_module_base import PluginModuleBase, PluginPageBase

from .setup import F, P


# rclone rc async job 대기 기한 (초). 대시보드·휴지통의 스캔 대기 기한(6시간)과
# 맞춘다. 이 시간을 넘기면 실패로 빠져나와 호출자가 빈 결과를 받는다.
RC_ASYNC_JOB_TIMEOUT_SEC = 6 * 3600




def get_rc_servers() -> list[dict]:
    '''
    로컬경로|리모트경로|ADDR#VFS|USER|PASS
    '''
    servers = []
    for user_rule in P.ModelSetting.get_list('scan_vfs_change_rule', comment=None):
        tmps = user_rule.split('|')
        if len(tmps) < 3:
            P.logger.warning(f'vfs/refresh 규칙을 확인하세요: {user_rule}')
            continue
        addr, _, vfs = tmps[2].partition('#')
        if vfs and ':' not in vfs:
            vfs = vfs if vfs.endswith(':') else f'{vfs}:'
        auth, _, addr = addr.rpartition('@')
        username, _, password = auth.partition(':')
        servers.append({
            'local': pathlib.Path(tmps[0]).as_posix(),
            'remote': pathlib.Path(tmps[1]).as_posix() if tmps[1] else '',
            'address': addr,
            'user': tmps[3] if len(tmps) > 4 else username,
            'pass': tmps[4] if len(tmps) > 4 else password,
            'vfs': vfs,
        })
    return servers




def rc_command(function: Callable) -> Callable:
    @functools.wraps(function)
    def wrapper(*args: tuple, **kwds: dict) -> dict:
        data = function(*args, **kwds)
        command = '/'.join(function.__name__.split('__'))
        server = data.get('server')
        rclone = F.PluginManager.get_plugin_instance('rclone')
        cmd = [
            rclone.ModelSetting.get('rclone_path'),
            '--timeout=0',
            'rc',
            command,
            f'--url={server["address"]}'
        ]
        if server['user']:
            cmd.extend([f"--user={server['user']}", f"--pass={server['pass']}"])
        if data.get('args'):
            cmd.extend(data.get('args'))
        if data.get('async'):
            cmd.append('_async=true')
        #P.logger.debug(f'RC command: {cmd}')
        # rclone rc 자체는 --timeout=0(무제한)이라, RC 서버가 응답하지 않으면
        # 동기 호출이 영원히 멈출 수 있다 — 실행 쪽에 상한을 둔다.
        # 상한은 고급 설정 base_timeout_vfs_rc (기본 600초).
        # 시간 초과 시 결과는 빈 dict로 처리돼 다음 주기에 다시 시도한다.
        rc_timeout = 600
        try:
            _v = P.ModelSetting.get_int('base_timeout_vfs_rc')
            if _v and _v > 0:
                rc_timeout = _v
        except Exception:
            pass
        result = SupportSubprocess.execute_command_return(cmd, timeout=rc_timeout)
        try:
            '''
            {'error': '', ...}
            {'result': {'/path/to': 'Invalid...'}}
            {'result': {'/path/to': 'OK'}}
            {'forgotten': ['/path/to']}
            {'jobid': 12345}
            '''
            rc_result = json.loads(result['log'])
            if data.get('async'):
                job_id = rc_result["jobid"]
                job_status = job__status(server, int(job_id))
                P.logger.debug(f"RC job status: {job_status}")
                counter = 1
                deadline = time.time() + RC_ASYNC_JOB_TIMEOUT_SEC
                while not job_status['finished']:
                    # --rc-job-expire-duration=60s --rc-job-expire-interval=10s
                    # 작업 상태 확인: 10~60초
                    time.sleep(min(counter**2, 30))
                    job_status = job__status(server, int(job_id))
                    counter = counter + 1
                    P.logger.debug(f'Waiting for the RC job: {job_id}')
                    if time.time() > deadline:
                        raise Exception(f'RC async job 대기 시간 초과 '
                                        f'({RC_ASYNC_JOB_TIMEOUT_SEC // 3600}시간): '
                                        f'jobid={job_id}')
                P.logger.debug(f"RC job status: {job_status}")
                if job_status.get('error'):
                    P.logger.error(job_status)
                    raise Exception(job_status.get('error'))
                return job_status.get('output')
            else:
                if rc_result.get('error'):
                    P.logger.error(rc_result)
                    raise Exception(rc_result.get('error'))
                #P.logger.debug(f'RC result: {rc_result}')
                return rc_result
        except Exception:
            P.logger.error(traceback.format_exc())
            P.logger.error(f'result={result}')
            P.logger.error(f'cmd={cmd}')
            if (rclone_path := shutil.which(rclone.ModelSetting.get("rclone_path"))) is None:
                P.logger.error(f'rclone을 찾을 수 없습니다: {rclone_path=}')
            else:
                P.logger.error(f'{rclone_path=}')
            return {}
    return wrapper


@rc_command
def job__status(server: dict, job_id: int) -> dict:
    return {
        'server': server,
        'args': [f"jobid={job_id}"],
    }


@rc_command
def vfs__refresh(server: dict, remote_path: str = None, recursive: bool = False, async_: bool = False) -> dict:
    data = {
        'server': server,
        'async': async_,
        'args': ['--fast-list'],
    }
    if remote_path:
        data['args'].append(f'dir={remote_path}')
    if recursive:
        data['args'].append('recursive=true')
    if server['vfs']:
        data['args'].append(f'fs={server["vfs"]}')
    return data


@rc_command
def vfs__forget(server: dict, remote_path: str, is_file: bool = False) -> dict:
    data = {
        'server': server,
        'args': [f'file={remote_path}'] if is_file else [f'dir={remote_path}'],
    }
    if server['vfs']:
        data['args'].append(f'fs={server["vfs"]}')
    return data


def with_servers(function: Callable) -> Callable:
    @functools.wraps(function)
    def wrapper(target: str, *args: tuple, **kwds: dict) -> dict:
        if not kwds.get('server'):
            target_path = pathlib.Path(pathlib.Path(target).as_posix())
            for server in get_rc_servers():
                local_path = pathlib.Path(pathlib.Path(server['local']).as_posix())
                if target_path.is_relative_to(local_path):
                    kwds['server'] = server
                    return function(target, *args, **kwds)
            raise Exception(f'대상에 적합한 RC 서버를 찾을 수 없습니다: {target}')
        return function(target, *args, **kwds)
    return wrapper


@with_servers
def vfs_forget(target: str, server: dict = None) -> None:
    is_file = pathlib.Path(target).is_file()
    remote_path = update_path(target, {server['local']: server['remote']})
    P.logger.info(vfs__forget(server, remote_path, is_file))


@with_servers
def vfs_refresh(target: str, recursive: bool = False, async_: bool = False, server: dict = None) -> dict:
    remote_path = pathlib.Path(update_path(target, {server['local']: server['remote']}))
    for parent in remote_path.parents:
        if parent == parent.parent:
            result = vfs__refresh(server) or {}
        else:
            result = vfs__refresh(server, parent.as_posix()) or {}
        P.logger.info(f'RC result: {result}')
        if ((result.get('result') or {}).get(parent.as_posix()) or '').lower() == 'ok':
            break
        if (result.get('result') or {}).get('error'):
            return result
    else:
        P.logger.warning(f'It has hit the root path: "{remote_path.as_posix()}"')
    if not pathlib.Path(target).is_file():
        result = vfs__refresh(server, remote_path.as_posix(), recursive, async_)
        P.logger.info(f'RC result: {result}')
    return result


def update_path(target: str, mappings: dict) -> str:
    target = pathlib.Path(target).as_posix()
    for k, v in mappings.items():
        target = target.replace(k, v)
    if not target:
        target = '/'
    return target










































def get_readable_time(_time: float) -> str:
    return datetime.datetime.fromtimestamp(_time, datetime.timezone.utc).strftime('%b %d %H:%M')


def get_dir(target_path: str) -> list[dict[str, str]]:
    target_path = pathlib.Path(target_path)
    with os.scandir(target_path) as scandirs:
        target_list = []
        for entry in scandirs:
            try:
                target_list.append(pack_dir(entry))
            except Exception as e:
                P.logger.warning(e)
        target_list = sorted(target_list, key=lambda entry: (entry.get('is_file'), entry.get('name')))
        parent_pack = pack_dir(target_path.parent)
        parent_pack['name'] = '..'
        target_list.insert(0, parent_pack)
        return target_list


def pack_dir(entry: os.DirEntry | pathlib.Path) -> dict:
    stats: os.stat_result = entry.stat(follow_symlinks=True)
    return {
        'name': entry.name,
        'path': pathlib.Path(entry.path).as_posix() if isinstance(entry, os.DirEntry) else str(pathlib.Path(entry).as_posix()),
        'is_file': entry.is_file(),
        'size': format_file_size(stats.st_size),
        'ctime': get_readable_time(stats.st_ctime),
        'mtime': get_readable_time(stats.st_mtime),
    }


def format_file_size(size: int, decimals: int = 1, binary_system: bool = True) -> str:
    units = ['B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']
    largest_unit = 'Y'
    if binary_system:
        step = 1024
    else:
        step = 1000
    for unit in units:
        if size < step:
            return f'{size:.{decimals}f}{unit}'
        size /= step
    return f'{size:.{decimals}f}{largest_unit}'


def default_route_socketio_page(page):
    module = page.parent
    page.socketio_list = page.socketio_list or []

    @F.socketio.on('connect', namespace=f'/{P.package_name}/{module.name}/{page.name}')
    @F.login_required
    def connect():
        P.logger.debug(f'socket_connect : {P.package_name}/{module.name}/{page.name}')
        page.socketio_list.append(flask.request.sid)
        socketio_callback('start', '')
        page.socketio_connect()

    @F.socketio.on('disconnect', namespace=f'/{P.package_name}/{module.name}/{page.name}')
    def disconnect():
        P.logger.debug(f'socket_disconnect : {P.package_name}/{module.name}/{page.name}')
        page.socketio_list.remove(flask.request.sid)
        page.socketio_disconnect()

    def socketio_callback(cmd, data, encoding=True):
        if page.socketio_list:
            if encoding:
                data = json.dumps(data, cls=AlchemyEncoder)
                data = json.loads(data)
            F.socketio.emit(cmd, data, namespace=f'/{P.package_name}/{module.name}/{page.name}')

    page.socketio_callback = socketio_callback


def celery_is_available() -> bool:
    try:
        return True if F.celery.control.inspect().stats() else False
    except Exception:
        return False


def start_task(task: dict) -> tuple[bool, str]:
    '''
    task: dict
        command: str  (refresh_scan | refresh | scan | forget)
        path: str
        recursive: bool
        async: bool
    '''
    try:
        match task['command']:
            case 'refresh_scan':
                vfs_refresh(task['path'], task['recursive'], task['async'])
                from .model_scan import ModelScanItem
                ModelScanItem(task['path'], mode='ADD').save()
            case 'refresh':
                vfs_refresh(task['path'], task['recursive'], task['async'])
            case 'scan':
                from .model_scan import ModelScanItem
                ModelScanItem(task['path'], mode='ADD').save()
            case 'forget':
                vfs_forget(task['path'])
        msg = f'작업이 끝났습니다: command={task["command"]} path={task["path"]}'
        P.logger.info(msg)
        return True, msg
    except Exception as e:
        P.logger.error(traceback.format_exc())
        return False, f'작업 도중에 오류가 발생했습니다: {repr(e)}'







class ThreadHasReturn(Thread):

    def __init__(self, group=None, target: Callable = None, name: str = None, args: tuple | list = (),
                 kwargs: dict = None, daemon: bool = None, callback: Callable = None) -> None:
        Thread.__init__(self, group, target, name, args, kwargs, daemon=daemon)
        self._return = None
        self.callback = callback

    def run(self) -> None:
        if self._target:
            self._return = self._target(*self._args, **self._kwargs)
        if self.callback:
            self.callback(self.get_return())

    def join(self, *args) -> dict:
        Thread.join(self, *args)
        return self.get_return()

    def get_return(self) -> dict:
        return self._return


class Base:

    _DB_VERSIONS = ['1']

    def __init__(self, *args, **kwds) -> None:
        super().__init__(*args, **kwds)

    def set_recent_menu(self, req: flask.Request) -> None:
        current_menu = '|'.join(req.path[1:].split('/')[1:])
        if not current_menu == P.ModelSetting.get('recent_menu_plugin'):
            P.ModelSetting.set('recent_menu_plugin', current_menu)

    def get_template_args(self) -> dict:
        args = {
            'package_name': P.package_name,
            'module_name': self.name if isinstance(self, PluginModuleBase) else self.parent.name,
            'page_name': self.name if isinstance(self, PluginPageBase) else None,
        }
        for conf in self.db_default.keys():
            args[conf] = P.ModelSetting.get(conf)
        return args

    def prerender(self, sub: str, req: flask.Request) -> None:
        self.set_recent_menu(req)

    def run_async(self, func: Callable, args: tuple = (), kwargs: dict = {}, **opts) -> None:
        if celery_is_available():
            P.logger.debug(f'Run by celery: {func.__name__}()')
            result = func.apply_async(args=args, kwargs=kwargs, **opts)
            Thread(target=result.get, kwargs={'on_message': self.socketio_emit_by_celery, 'propagate': False}, daemon=True).start()
        else:
            P.logger.debug(f'Run by Thread: {func.__name__}()')
            th = ThreadHasReturn(target=func, args=args, kwargs=kwargs, daemon=True, callback=self.socketio_emit)
            th.start()

    def process_command(self, command: str, arg1: str, arg2: str, arg3: str, request: flask.Request) -> flask.Response:
        '''override'''
        try:
            data = getattr(self, f'command_{command}', self.command_default)(self.parse_command(request))
        except Exception as e:
            P.logger.error(traceback.format_exc())
            data = self.returns('warning', str(e))
        finally:
            return flask.jsonify(data)

    def parse_command(self, request: flask.Request) -> dict[str, Any]:
        query = urllib.parse.parse_qs(request.form.get('arg1') or '', keep_blank_values=True)
        return {
            'command': request.form.get('command'),
            'query': query,
        }

    def command_default(self, request: flask.Request) -> dict:
        data = self.returns('danger', title='SHYNI MATE')
        data['msg'] = '아직 구현되지 않았습니다.'
        return data

    def returns(self, success: str, msg: str = None, title: str = None, modal: str = None, json: dict = None, reload: bool = False, data: dict = None) -> dict:
        return {'ret': success, 'msg': msg, 'title': title, 'modal': modal, 'json': json, 'reload': reload, 'data': data}

    def _migration(self, version_key: str, table: str, migrate_func: Callable) -> None:
        version = P.ModelSetting.get(version_key) or self._DB_VERSIONS[0]
        P.logger.debug(f'{table} 현재 DB 버전: {version}')
        with F.app.app_context():
            db_file = F.app.config['SQLALCHEMY_BINDS'][P.package_name].replace('sqlite:///', '').split('?')[0]
            try:
                with sqlite3.connect(db_file) as conn:
                    conn.row_factory = sqlite3.Row
                    cs = conn.cursor()
                    # DB 볼륨 정리
                    cs.execute('VACUUM;')
                    for ver in self._DB_VERSIONS[(self._DB_VERSIONS.index(version)):]:
                        migrate_func(ver, table, cs)
                        version = ver
                    F.db.session.flush()
                P.logger.debug(f'{table} 최종 DB 버전: {version}')
                P.ModelSetting.set(version_key, version)
            except Exception:
                P.logger.error(traceback.format_exc())
                P.logger.error('마이그레이션 실패')

    def socketio_emit(self, result: tuple[bool, str]) -> None:
        raise Exception('이 메소드를 구현하세요.')

    def socketio_emit_by_celery(self, result: dict) -> None:
        raise Exception('이 메소드를 구현하세요.')




class ExtPageBase(Base, PluginPageBase):

    def __init__(self, plugin: PluginBase, parent: PluginModuleBase, name: str = None, scheduler_desc: str = None) -> None:
        super().__init__(plugin, parent, name, scheduler_desc)
        default_route_socketio_page(self)
        self.db_default = {}

    def process_menu(self, req: flask.Request) -> flask.Response:
        '''override'''
        self.prerender(self.name, req)
        try:
            return flask.render_template(f'{P.package_name}_{self.parent.name}_{self.name}.html', args=self.get_template_args())
        except Exception:
            P.logger.error(traceback.format_exc())
            return flask.render_template('sample.html', title=f"process_menu() - {P.package_name}/{self.parent.name}/{self.name}")

    def socketio_connect(self) -> None:
        pass

    def socketio_disconnect(self) -> None:
        pass

    def socketio_emit(self, result: tuple[bool, str]) -> None:
        '''override'''
        # {P.package_name}/{module.name}/{page.name}
        F.socketio.emit('result', {'status': 'success' if result[0] else 'warning', 'result': result[1]}, namespace=f'/{P.package_name}/{self.parent.name}/{self.name}')

    def socketio_emit_by_celery(self, result: dict) -> None:
        '''override'''
        # celery status: SUCCESS, STARTED, REVOKED, RETRY, RECEIVED, PENDING, FAILURE
        #{'status': 'SUCCESS', 'result': (True, '작업을 완료했습니다.'), 'traceback': None, 'children': [(('cf6280f8-917e-4a21-b89e-cf890a4c991c', None), None)], 'date_done': '2024-08-13T08:42:04.600936', 'task_id': '492119df-94e7-4aae-9e4a-ab6b3e080b1c'}
        self.socketio_emit(result['result'])


class BrowserPage(ExtPageBase):

    def __init__(self, plugin: PluginBase, parent: PluginModuleBase) -> None:
        super().__init__(plugin, parent, name='browser')
        default_route_socketio_page(self)
        self.db_default = {
            'scan_browser_working_directory': '/',
        }

    def command_default(self, commands: dict[str, Any]) -> tuple[bool, str]:
        '''override'''
        if commands['command'] in ['refresh_scan', 'refresh', 'scan', 'forget']:
            task = {
                'command': commands['command'],
                'path': commands['query'].get('path')[0],
            }
            if 'refresh' in commands['command']:
                task['recursive'] = commands['query'].get('recursive')[0].lower() == 'true'
                task['async'] = commands['query'].get('async')[0].lower() == 'true'
            self.run_async(start_task, (task,))
            return self.returns('success', '실행했습니다.')
        else:
            data = self.returns('danger', title='Browser')
            data['msg'] = '아직 구현되지 않았습니다.'
            return data

    def command_list(self, commands: dict[str, Any]) -> dict:
        path = commands['query'].get('path', ['/'])[0]
        try:
            dir_list = json.dumps(get_dir(path))
        except Exception as e:
            P.logger.error(traceback.format_exc())
            return self.returns('warning', str(e))
        P.ModelSetting.set('scan_browser_working_directory', path)
        if dir_list:
            return self.returns('success', data=dir_list)
        else:
            return self.returns('warning', '폴더 목록을 생성할 수 없습니다.')




def save_with_retry(item, attempts=6):
    """ModelBase.save()와 같은 의미로 저장하되, SQLite 'database is locked'면
    롤백 후 간격을 두고 재시도한다.

    프레임워크 기본 save()는 예외를 로그만 남기고 삼켜서, 등록이 몰리는 순간
    잠금에 걸린 항목이 조용히 사라진 일이 있었다 (2026-11-10 FP 등록 5건 유실).
    성공하면 item, 끝내 실패하면 기존처럼 오류 로그를 남기고 None을 돌려준다.

    다른 앱 컨텍스트의 세션에 붙은 항목은 add()가 충돌하므로 merge()로
    상태를 현재 세션에 합친다 — Flask-SQLAlchemy 3.x는 세션을 앱 컨텍스트
    단위로 나눈다. z34(2026-11-12) 이후 poll_running이 finalize를 그 자리에서
    부르면서 이 경우가 생겼다 ("already attached to session" 오류).
    새로 만드는 항목(세션 없음)은 기존대로 add()를 써야 id가 채워진다 —
    merge()는 원본에 id를 채우지 않아 뒤의 저장이 중복 행을 만든다."""
    # 실패한 commit 뒤에는 인스턴스 속성이 만료돼 있어 item.id를 그냥 읽으면
    # DetachedInstanceError가 난다 — dict에서 직접 읽어 그 함정을 피한다.
    item_id = item.__dict__.get('id')
    delays = (1, 2, 3, 5, 8)
    last_exc = None
    last_tb = None
    for attempt in range(attempts):
        try:
            with F.app.app_context():
                try:
                    attached = object_session(item)
                    if attached is not None and attached is not F.db.session:
                        F.db.session.merge(item)
                    else:
                        F.db.session.add(item)
                    F.db.session.commit()
                    return item
                except Exception:
                    try:
                        F.db.session.rollback()
                    except Exception:
                        pass
                    raise
        except Exception as e:
            last_exc = e
            last_tb = traceback.format_exc()
            if 'database is locked' not in str(e) or attempt >= attempts - 1:
                break
            delay = delays[min(attempt, len(delays) - 1)]
            P.logger.info(f'[DB] 잠금으로 저장 재시도 ({attempt + 1}/{attempts - 1}): '
                        f'{type(item).__name__} id={item_id} — {delay}초 후')
            time.sleep(delay)
    P.logger.error(f'Exception:{last_exc}')
    if last_tb:
        P.logger.error(last_tb)
    return None
