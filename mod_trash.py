import json

from .model_trash import ModelTrashItem
from .mod_status import ModuleStatus
from .setup import *
from .task_periodic import annotate_total_labels
from .task_trash import (ACTION_LABEL, TaskTrash, clear_trash_start_error,
                         fail_trash_start, get_last_trash_start_error,
                         get_trash_current, get_trash_jobs,
                         request_trash_cancel)

name = 'trash'
logger = P.logger

# 라이브러리 선택 목록 정렬 — 주기적 스캔·휴지통 작업이 같은 설정을 쓴다.
PICK_SORTS = ('default_asc', 'default_desc', 'name_asc', 'name_desc')
PICK_SORT_KEY = 'jobs_lib_pick_sort'


class ModuleTrash(PluginModuleBase):
    """휴지통 주기 작업 — 라이브러리와 동작(새로고침/스캔/비우기 조합)을 골라
    cron으로 등록해 두면 정해진 시각에 하나씩 순차로 실행한다.
    주기적 스캔과 같은 작업 목록 방식이다."""

    def __init__(self, P):
        super(ModuleTrash, self).__init__(P, name=name, first_menu='jobs')
        self.db_default = {
            f'{self.name}_db_version': '1',
            f'{self.name}_jobs': '[]',
            f'{self.name}_db_delete_day': '30',
            f'{self.name}_db_auto_delete': 'True',
            # 진행 중 표시 정리 시간(분) — 진행 상태를 확인할 방법이 없을 때
            # 이 시간이 지나면 '중단됨'으로 정리한다. 0이면 끈다 (기본값 30)
            f'{self.name}_stuck_timeout_min': '30',
        }
        self.web_list_model = ModelTrashItem

    def migration(self):
        # trash_item에 작업 결과 컬럼 보장 — 실제 컬럼 존재로 판단(멱등).
        # mod_scan.py의 migration과 같은 패턴이다.
        try:
            from sqlalchemy import inspect, text
            with F.app.app_context():
                engine = F.db.engines[P.package_name] if hasattr(F.db, 'engines') else F.db.get_engine(F.app, bind=P.package_name)
                insp = inspect(engine)
                if 'trash_item' in insp.get_table_names():
                    cols = [c['name'] for c in insp.get_columns('trash_item')]
                    new_cols = [
                        ('shyni_job_id', 'VARCHAR'), ('stat_total', 'INTEGER'),
                        ('stat_new', 'INTEGER'), ('stat_missing', 'INTEGER'),
                        ('stat_updated', 'INTEGER'), ('stat_restored', 'INTEGER'),
                        ('stat_registered', 'INTEGER'), ('stat_failed', 'INTEGER'),
                        ('vfs_duration', 'INTEGER'), ('scan_duration', 'INTEGER'),
                        ('append_files', 'TEXT'), ('append_count', 'INTEGER'),
                        ('stat_folders', 'TEXT'), ('emptied_count', 'INTEGER'),
                    ]
                    added = [c for c, _ in new_cols if c not in cols]
                    if added:
                        with engine.begin() as conn:
                            for col, typ in new_cols:
                                if col not in cols:
                                    conn.execute(text(f'ALTER TABLE trash_item ADD {col} {typ}'))
                        P.logger.info(f'[Trash] migration: trash_item 컬럼 추가 — {added}')
        except Exception as e:
            P.logger.error(f'[Trash] migration 오류: {e}')

    def plugin_load(self):
        self.migration()
        ModelTrashItem.set_terminated()
        P.ModelSetting.set('trash_current', '')
        P.ModelSetting.set('trash_cancel', '')
        self.start()

    # ---------- 스케줄러 ----------
    def start(self):
        for idx, item in enumerate(get_trash_jobs()):
            if item.get('use'):
                self.sched_add(idx, item=item)

    def sched_add(self, idx, item=None):
        try:
            if item is None:
                item = get_trash_jobs()[idx]
            if F.scheduler.is_include(item['job_id']):
                return True
            job = Job(self.P.package_name, item['job_id'], item['cron'],
                      self.job_function, item.get('name') or f"작업 {idx + 1}",
                      args=(idx,))
            F.scheduler.add_job_instance(job)
            return True
        except Exception as e:
            logger.error(f'휴지통 작업 스케줄 등록 실패: {e}')
            logger.error(traceback.format_exc())
        return False

    def sched_remove_all(self):
        for item in get_trash_jobs():
            try:
                if F.scheduler.is_include(item['job_id']):
                    F.scheduler.remove_job(item['job_id'])
            except Exception:
                pass
        try:
            for job in list(F.scheduler.get_jobs()):
                jid = getattr(job, 'id', '') or ''
                if jid.startswith(f'{self.P.package_name}_trash_'):
                    F.scheduler.remove_job(jid)
        except Exception:
            pass

    def job_function(self, idx, mode='scheduler'):
        # Celery에 넘기지 못하면 스레드·태스크 안에서 조용히 죽는다 —
        # '실행을 시작했습니다'라고 답한 뒤 아무것도 안 뜨는 경우다.
        # 그래서 여기서 실패를 잡아 이력 행과 배너 기록으로 남긴다.
        try:
            self.start_celery(TaskTrash.start, None, *(idx, mode))
        except Exception as e:
            logger.error(f'[Trash] Celery 시작 실패: {e}')
            try:
                jobs = get_trash_jobs()
                job_name = (jobs[idx].get('name')
                            if 0 <= idx < len(jobs) else '') or ''
            except Exception:
                job_name = ''
            fail_trash_start(job_name, mode, f'작업을 넘기지 못했습니다: {e}')

    def one_execute(self, idx):
        # '지금 실행'은 수동 실행이라 mode를 'manual'로 넘긴다 (R19).
        # 스케줄에 등록된 작업도 스케줄러 경유가 아니라 이 경로로 실행한다.
        jobs = get_trash_jobs()
        if idx < 0 or idx >= len(jobs):
            return 'fail'
        job_id = jobs[idx]['job_id']
        try:
            if F.scheduler.is_include(job_id) and F.scheduler.is_running(job_id):
                return 'is_running'
        except Exception:
            pass

        def func():
            time.sleep(1)
            try:
                self.job_function(idx, 'manual')
            except Exception as e:
                logger.error(f'[Trash] 지금 실행 실패: {e}')
                fail_trash_start(jobs[idx].get('name') or '', 'manual',
                                 f'실행을 시작하지 못했습니다: {e}')
        t = threading.Thread(target=func, args=())
        t.daemon = True
        t.start()
        return 'thread'

    # ---------- 화면 ----------
    def process_menu(self, sub, req):
        # 서브메뉴: jobs(작업 관리) · result(작업 결과) · setting(설정) · manual(매뉴얼)
        arg = P.ModelSetting.to_dict()
        try:
            # 라이브러리 목록은 페이지가 뜬 뒤 ajax('sections')로 따로
            # 불러온다 — shyni 응답을 기다리느라 페이지가 늦게 뜨지 않게.
            if sub == 'jobs':
                arg['trash_jobs_json'] = P.ModelSetting.get('trash_jobs') or '[]'
                sort = P.ModelSetting.get(PICK_SORT_KEY) or 'default_asc'
                arg['lib_pick_sort'] = sort if sort in PICK_SORTS else 'default_asc'
            return render_template(f'{P.package_name}_{self.name}_{sub}.html', arg=arg)
        except Exception as e:
            logger.error(f'Exception:{str(e)}')
            logger.error(traceback.format_exc())
            return render_template('sample.html', title=f'{P.package_name}/{self.name}/{sub}')

    def process_command(self, command, arg1, arg2, arg3, req):
        ret = {'ret': 'success'}
        if command == 'save_jobs':
            try:
                jobs = json.loads(arg1 or '[]')
                if not isinstance(jobs, list):
                    raise ValueError('형식이 올바르지 않습니다.')
            except Exception as e:
                return jsonify({'ret': 'warning', 'msg': f'작업 목록을 읽지 못했습니다: {e}'})
            from apscheduler.triggers.cron import CronTrigger
            cleaned = []
            for i, job in enumerate(jobs):
                cron = (job.get('cron') or '').strip()
                try:
                    CronTrigger.from_crontab(cron)
                except Exception:
                    return jsonify({'ret': 'warning',
                                    'msg': f"{i + 1}번째 작업의 cron 표현식이 올바르지 않습니다: {cron}"})
                sections = [str(s) for s in (job.get('sections') or [])]
                if not sections:
                    return jsonify({'ret': 'warning',
                                    'msg': f"{i + 1}번째 작업에 라이브러리를 하나 이상 선택해 주세요."})
                action = job.get('action') or 'empty'
                if action not in ACTION_LABEL:
                    action = 'empty'
                cleaned.append({
                    'name': (job.get('name') or f'작업 {i + 1}').strip(),
                    'sections': sections,
                    'cron': cron,
                    'action': action,
                    'use': bool(job.get('use')),
                })
            # 방어: 진행 중인 작업의 정의가 바뀌었거나 삭제됐으면 저장 거절.
            # (작업 관리 화면에서 행을 잠그지만, 페이지를 연 뒤 실행이
            # 시작되는 틈이나 다른 경로의 저장을 막기 위한 서버 쪽 확인이다.
            # 진행 중인 작업의 정의가 제출 목록 어딘가에 그대로 있으면 된다 —
            # 다른 행을 고치거나 앞 행을 지우는 저장은 그대로 허용한다.)
            cur = get_trash_current()
            if cur is not None:
                stored = get_trash_jobs()
                ridx = cur.get('job_idx')
                running = (stored[ridx] if isinstance(ridx, int)
                           and 0 <= ridx < len(stored) else None)
                if running is None:
                    return jsonify({'ret': 'warning',
                                    'msg': '진행 중인 작업 정보를 찾지 못했습니다. 취소한 뒤에 저장해 주세요.'})

                def _same_job(a, b):
                    return ((a.get('name') or '').strip() == (b.get('name') or '').strip()
                            and (a.get('cron') or '').strip() == (b.get('cron') or '').strip()
                            and a.get('action') == b.get('action')
                            and bool(a.get('use')) == bool(b.get('use'))
                            and sorted(str(s) for s in (a.get('sections') or []))
                            == sorted(str(s) for s in (b.get('sections') or [])))
                if not any(_same_job(running, c) for c in cleaned):
                    return jsonify({'ret': 'warning',
                                    'msg': f"진행 중인 작업 '{running.get('name') or ''}'은(는) 취소한 뒤에 수정할 수 있습니다."})
            self.sched_remove_all()
            P.ModelSetting.set('trash_jobs', json.dumps(cleaned, ensure_ascii=False))
            self.start()
            logger.info(f"[Trash] 작업 저장: {len(cleaned)}개 — "
                        f"{[(j['name'], j['action'], j['cron'], j['use']) for j in cleaned]}")
            ret['msg'] = f'작업 {len(cleaned)}개를 저장했습니다. 사용으로 켜진 작업은 스케줄러에 반영됐습니다.'
        elif command == 'task_execute':
            try:
                idx = int(arg1)
            except Exception:
                return jsonify({'ret': 'warning', 'msg': '작업 번호가 올바르지 않습니다.'})
            if get_trash_current() is not None:
                return jsonify({'ret': 'warning', 'msg': '이미 진행 중인 휴지통 작업이 있습니다.'})
            jobs = get_trash_jobs()
            if idx < 0 or idx >= len(jobs):
                return jsonify({'ret': 'warning', 'msg': '작업 번호가 올바르지 않습니다.'})
            if not (jobs[idx].get('sections') or []):
                return jsonify({'ret': 'warning',
                                'msg': '선택한 라이브러리가 없습니다. 라이브러리 선택에서 골라 주세요.'})
            result = self.one_execute(idx)
            logger.info(f'[Trash] 지금 실행 요청: idx={idx} 결과={result}')
            if result == 'is_running':
                ret['msg'] = '이미 실행 중입니다.'
            elif result == 'fail':
                return jsonify({'ret': 'warning', 'msg': '실행하지 못했습니다.'})
            else:
                ret['msg'] = '실행을 시작했습니다. 잠시 뒤 워커가 작업을 시작합니다.'
        elif command == 'cancel_current':
            if get_trash_current() is None:
                return jsonify({'ret': 'warning', 'msg': '지금 진행 중인 휴지통 작업이 없습니다.'})
            request_trash_cancel()
            logger.info('[Trash] 취소 요청')
            ret['msg'] = '취소를 요청했습니다. 진행 중인 단계가 정리되면 멈춥니다.'
        return jsonify(ret)

    # ---------- 화면 AJAX ----------
    def process_ajax(self, sub, req):
        if sub == 'sections':
            sections, err = ModuleStatus._sections()
            if sections is None:
                return jsonify({'ret': 'fail', 'msg': err})
            return jsonify({'ret': 'success', 'data': sections})
        if sub == 'current':
            return jsonify({'ret': 'success', 'data': get_trash_current(),
                            'last_error': get_last_trash_start_error()})
        if sub == 'clear_start_error':
            clear_trash_start_error()
            return jsonify({'ret': 'success'})
        if sub == 'pick_sort':
            sort = (req.form.get('sort') or '').strip()
            if sort not in PICK_SORTS:
                return jsonify({'ret': 'fail', 'msg': '정렬 조건이 올바르지 않습니다.'})
            P.ModelSetting.set(PICK_SORT_KEY, sort)
            return jsonify({'ret': 'success'})
        if sub == 'history':
            try:
                page = int(req.form.get('page') or 1)
            except Exception:
                page = 1
            section = (req.form.get('section') or '').strip() or None
            only_append = (req.form.get('only_append') or '') == '1'
            search = (req.form.get('search') or '').strip() or None
            data = ModelTrashItem.get_page(
                page, section_key=section, only_append=only_append, search=search)
            annotate_total_labels(data.get('list') or [])
            return jsonify({'ret': 'success', 'data': data})
        if sub == 'history_detail':
            try:
                item_id = int(req.form.get('id') or 0)
            except Exception:
                item_id = 0
            data = ModelTrashItem.get_detail(item_id)
            if data is None:
                return jsonify({'ret': 'fail', 'msg': '이력을 찾지 못했습니다.'})
            annotate_total_labels([data])
            return jsonify({'ret': 'success', 'data': data})
        return jsonify({'ret': 'fail', 'msg': '알 수 없는 요청입니다.'})

    def process_api(self, sub, req):
        return self.process_ajax(sub, req)
