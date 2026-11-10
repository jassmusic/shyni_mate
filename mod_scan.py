import json
import threading

import requests

from .model_scan import ModelScanItem
from .setup import *
from .task_scan import Task
from .task_trash import run_empty_trash
from .extensions import BrowserPage, vfs_refresh
from .shyni_backend import ShyniBackend

name = 'scan'
class ModuleScan(PluginModuleBase):
    scan_queue = None
    scan_thread = None
    file_exist_thread = None
    current_scan_count = 0

    def __init__(self, P):
        super(ModuleScan, self).__init__(P, name=name, first_menu='setting')
        self.db_default = {
            f"sacn_item_last_list_option": "",
            f"{self.name}_db_version": "1",
            f"{self.name}_max_scan_count": "1",
            #f"{self.name}_incompleted_rescan": "False",
            f"{self.name}_max_wait_time": "5",
            f"{self.name}_probe_timeout_sec": "60",
            f"{self.name}_manual_target": "",
            f"{self.name}_filecheck_thread_interval": "60",
            f"{self.name}_db_delete_day": "30",
            f"{self.name}_db_auto_delete": "True",
            f"{self.name}_use_vfs_refresh": "False",
            f"{self.name}_vfs_change_rule": "/mnt/gds2/GDRIVE|/GDRIVE|172.17.0.1:5574",
            f"{self.name}_shyni_use": "True",
            f"{self.name}_shyni_path_rule": "",
            f"{self.name}_trash_schedule_enable": "False",
            f"{self.name}_trash_cron": "0 5 * * *",
            f"{self.name}_trash_schedule_anchors": "",
            f"{self.name}_trash_sections": "[]",
            f"{self.name}_trash_last_result": "",
            f"{self.name}_trash_running": "",
        }
        self.web_list_model = ModelScanItem
        self.set_page_list([BrowserPage])

    def migration(self):
        # scan_item에 샤이니 대상 상태 컬럼 보장 — 실제 컬럼 존재로 판단(멱등).
        # binds URL 문자열 파싱 금지(웹훅 migration에서 상대경로 오해석 전례).
        try:
            from sqlalchemy import inspect, text
            with F.app.app_context():
                engine = F.db.engines[P.package_name] if hasattr(F.db, 'engines') else F.db.get_engine(F.app, bind=P.package_name)
                insp = inspect(engine)
                if 'scan_item' in insp.get_table_names():
                    cols = [c['name'] for c in insp.get_columns('scan_item')]
                    with engine.begin() as conn:
                        if 'shyni_status' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD shyni_status VARCHAR'))
                        if 'shyni_job_id' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD shyni_job_id VARCHAR'))
                        if 'vfs_done' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD vfs_done BOOLEAN'))
                        if 'vfs_done_time' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD vfs_done_time DATETIME'))
                        if 'reset_done' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD reset_done BOOLEAN'))
                        if 'reset_time' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD reset_time DATETIME'))
                        if 'in_library' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD in_library BOOLEAN'))
                        if 'shyni_phase' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD shyni_phase VARCHAR'))
                        if 'probe_received' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD probe_received INTEGER'))
                        if 'probe_expected' not in cols:
                            conn.execute(text('ALTER TABLE scan_item ADD probe_expected INTEGER'))
                    if 'shyni_status' not in cols:
                        P.logger.info('[Scan] migration: scan_item.shyni_status/shyni_job_id 컬럼 추가')
        except Exception as e:
            P.logger.error(f'[Scan] migration 오류: {e}')


    def process_menu(self, sub, req):
        if sub == 'trash':
            arg = P.ModelSetting.to_dict()
            try:
                from .mod_status import ModuleStatus
                sections, _ = ModuleStatus._sections()
                arg['library_list'] = sections or []
            except Exception:
                arg['library_list'] = []
            return render_template(f'{P.package_name}_{self.name}_trash.html', arg=arg)
        return super().process_menu(sub, req)


    # ---------- 휴지통 스캔 ----------
    def trash_job_function(self):
        t = threading.Thread(target=run_empty_trash, args=('scheduler',))
        t.daemon = True
        t.start()

    def trash_sched_sync(self):
        job_id = f'{P.package_name}_trash'
        try:
            if F.scheduler.is_include(job_id):
                F.scheduler.remove_job(job_id)
            if P.ModelSetting.get_bool('scan_trash_schedule_enable'):
                cron = (P.ModelSetting.get('scan_trash_cron') or '').strip()
                from apscheduler.triggers.cron import CronTrigger
                CronTrigger.from_crontab(cron)
                job = Job(self.P.package_name, job_id, cron,
                          self.trash_job_function, '휴지통 비우기', args=())
                F.scheduler.add_job_instance(job)
                logger.info(f'휴지통 비우기 스케줄 등록: {cron}')
        except Exception as e:
            logger.error(f'휴지통 스케줄 등록 실패: {e}')

    def process_command(self, command, arg1, arg2, arg3, req):
        ret = {'ret':'success'}
        if command == 'trash_execute':
            if (P.ModelSetting.get('scan_trash_running') or '') == '1':
                return jsonify({'ret': 'warning', 'msg': '휴지통 비우기가 이미 실행 중입니다.'})
            logger.info('[휴지통] 수동 실행을 시작합니다.')
            t = threading.Thread(target=run_empty_trash, args=('manual',))
            t.daemon = True
            t.start()
            ret['msg'] = '휴지통 비우기를 시작했습니다. 잠시 뒤 새로고침하면 아래에 결과가 표시됩니다.'
        elif command == 'trash_save_sections':
            try:
                sections = json.loads(arg1 or '[]')
                if not isinstance(sections, list):
                    raise ValueError('형식이 올바르지 않습니다.')
            except Exception as e:
                return jsonify({'ret': 'warning', 'msg': f'라이브러리 선택을 읽지 못했습니다: {e}'})
            P.ModelSetting.set('scan_trash_sections', json.dumps([str(s) for s in sections]))
            logger.info(f'[휴지통] 대상 라이브러리 저장: {[str(s) for s in sections]}')
            ret['msg'] = '대상 라이브러리를 저장했습니다.'
        elif command == 'manual':
            P.ModelSetting.set(f"{self.name}_manual_target", arg2)
            ModelScanItem(arg2, mode=arg1).save()
            logger.info(f'[Scan] 수동 추가: mode={arg1} target={arg2}')
            ret['msg'] = "추가하였습니다."
        elif command == 'reset_items':
            ids = [x.strip() for x in (arg1 or '').split(',') if x.strip()]
            done, skipped = 0, 0
            for id_str in ids:
                try:
                    item = ModelScanItem.get_by_id(int(id_str))
                except Exception:
                    item = None
                if item is None:
                    continue
                if item.shyni_status == 'RUNNING':
                    skipped += 1
                    continue
                # 생성 직후 상태로 되돌린다 — filecheck가 처음부터 다시 처리한다
                item.shyni_status = None
                item.in_library = None
                item.shyni_job_id = None
                item.section_id = None
                item.section_type = None
                item.meta_info = None
                item.vfs_done = None
                item.vfs_done_time = None
                item.filecheck_time = None
                item.filecheck_count = 0
                item.process_start_time = None
                item.process_finish_time = None
                item.process_duration = None
                item.completed_time = None
                item.flag_filecheck = False
                item.flag_last_filecheck = False
                item.flag_db_include = False
                item.flag_cancel = False
                item.flag_finish = False
                item.scan_folder = None
                item.reset_done = True
                item.reset_time = datetime.now()
                item.set_status('READY', save=True)
                done += 1
            logger.info(f'[Scan] 항목 리셋: {done}개 다시 시작 (요청 ids={ids}, 실행 중 건너뜀 {skipped}개)')
            ret['msg'] = f'{done}개 항목을 초기화했습니다. 처음부터 다시 시작합니다.'
            if skipped:
                ret['msg'] += f' (실행 중이라 건너뜀 {skipped}개)'
        elif command == 'retry_scan':
            item = ModelScanItem.get_by_id(arg1)
            if item.mode == 'REFRESH':
                # 이전 404/실패 결과가 재요청을 가로막지 않도록 초기화한다.
                # 이미 실행 중인 샤이니 job은 중복 발사하지 않는다.
                item.section_id = None
                item.section_type = None
                if item.shyni_status != 'RUNNING':
                    item.shyni_status = None
                    item.shyni_job_id = None
            item.set_status('READY', save=True)
            logger.info(f'[Scan] 재시도(READY로 변경): item={arg1} mode={item.mode}')
            ret['msg'] = 'READY 상태로 변경했습니다.'
        elif command == 'meta_load_items':
            ids = [int(x) for x in (arg1 or '').split(',') if x.strip().isdigit()]
            if not ids:
                ret = {'ret': 'warning', 'msg': '선택된 항목이 없습니다.'}
            else:
                t = threading.Thread(target=ShyniBackend.load_meta_many, args=(ids,))
                t.daemon = True
                t.start()
                logger.info(f'[Scan] 선택 항목 메타 불러오기 시작: {len(ids)}개 ids={ids}')
                ret['msg'] = f'{len(ids)}개 항목의 메타 정보를 불러오는 중입니다. 잠시 뒤 목록에 표시됩니다.'
        elif command == 'meta_refresh_items':
            ids = [int(x) for x in (arg1 or '').split(',') if x.strip().isdigit()]
            if not ids:
                ret = {'ret': 'warning', 'msg': '선택된 항목이 없습니다.'}
            else:
                t = threading.Thread(target=ShyniBackend.meta_refresh_many, args=(ids,))
                t.daemon = True
                t.start()
                logger.info(f'[Scan] 선택 항목 메타 새로고침 시작: {len(ids)}개 ids={ids}')
                ret['msg'] = f'{len(ids)}개 항목의 메타 새로고침을 시작했습니다. 반영되면 앨범 정보가 갱신됩니다.'
        elif command == 'meta_load':
            item = ModelScanItem.get_by_id(arg1)
            if item is None:
                ret = {'ret': 'warning', 'msg': '항목을 찾지 못했습니다.'}
            else:
                t = threading.Thread(target=ShyniBackend.load_meta_now, args=(item.id,))
                t.daemon = True
                t.start()
                logger.info(f'[Scan] 라이브러리 메타 불러오기(수동) 시작: item={item.id} target={item.target}')
                ret['msg'] = '라이브러리 메타 정보를 불러오는 중입니다. 잠시 뒤 목록에 표시됩니다.'
        elif command == 'meta_refresh':
            item = ModelScanItem.get_by_id(arg1)
            if item is None:
                ret = {'ret': 'warning', 'msg': '항목을 찾지 못했습니다.'}
            else:
                t = threading.Thread(target=ShyniBackend.meta_refresh_now, args=(item.id,))
                t.daemon = True
                t.start()
                logger.info(f'[Scan] 메타 새로고침(수동) 시작: item={item.id} target={item.target}')
                ret['msg'] = '메타 새로고침을 요청했습니다. 반영되면 앨범 정보(이미지 포함)가 갱신됩니다.'
        return jsonify(ret)


    def process_api(self, sub, req):
        ret = {'ret':'success'}
        if sub == 'do_scan':
            target = req.form['target']
            target_section_id = req.form.get('target_section_id') or 0
            mode = req.form.get('mode') or 'ADD'
            callback_id = req.form.get('callback_id')
            callback_url = req.form.get('callback_url')
            ModelScanItem(
                target,
                mode = mode,
                target_section_id = target_section_id,
                callback_id = callback_id,
                callback_url = callback_url,
            ).save()
            logger.info(f'[Scan] 수신 등록: mode={mode} target={target} callback={callback_id}')
            ret['msg'] = f'{mode=} {target=}'
        elif sub == 'manual_refresh':
            meta_id = req.form.get('metadata_item_id')
            if not meta_id:
                return jsonify({'ret': 'fail', 'msg': '해당 항목을 찾지 못했습니다.'})
            url, token = ShyniBackend.get_config()
            if not url or not token:
                return jsonify({'ret': 'fail', 'msg': '기본 설정에서 shyni 주소와 토큰을 먼저 설정해 주세요.'})
            try:
                res = requests.put(f'{url}/library/metadata/{meta_id}/refresh',
                                   headers={'X-Plex-Token': token, 'Accept': 'application/json'},
                                   timeout=30)
                res.raise_for_status()
                logger.info(f'[Scan] 메타데이터 새로고침 요청: metadata_id={meta_id}')
                ret = {'ret':'success', 'msg': f'{meta_id} 새로고침을 요청했습니다.'}
            except Exception as e:
                logger.error(f'[Scan] 메타데이터 새로고침 실패: metadata_id={meta_id} {e}')
                ret = {'ret': 'fail', 'msg': f'새로고침 요청에 실패했습니다: {e}'}
        elif sub == 'vfs_refresh':
            target = req.form.get('target')
            recursive = (req.form.get('recursive') == 'true') or False
            async_ = (req.form.get('async') == 'true') or False
            result = vfs_refresh(target, recursive, async_)
            ret.update(result)
            ret.update({
                'recursive': recursive,
                'async': async_,
                'target': target,
            })
        else:
            return {'ret':'fail', 'msg':'Bad request'}, 400
        return jsonify(ret)


    def plugin_load(self):
        def func():
            self.start_celery(Task.start)
        thread = threading.Thread(target=func, args=())
        thread.daemon = True
        thread.start()
        P.ModelSetting.set('scan_trash_running', '')
        self.trash_sched_sync()


    def setting_save_after(self, changes: list) -> None:
        '''override'''
        super().setting_save_after(changes)
        for page in self.page_list:
            page.setting_save_after(changes)
        try:
            changed_keys = {c.get('key') if isinstance(c, dict) else str(c) for c in (changes or [])}
        except Exception:
            changed_keys = set()
        if any('trash' in (k or '') for k in changed_keys) or not changed_keys:
            self.trash_sched_sync()
