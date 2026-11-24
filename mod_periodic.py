import json

from .model_periodic import ModelPeriodicItem
from .mod_status import ModuleStatus
from .setup import *
from .task_periodic import (LIBRARY_BUSY_MESSAGE, TaskPeriodic,
                            _cancel_requested, _clear_resume_current,
                            _record_start_error, _same_job_busy,
                            _same_library_busy,
                            annotate_total_labels, clear_start_error,
                            current_scan_progress, fail_start, get_currents,
                            get_jobs, get_last_start_error,
                            periodic_action_to_kind_use_vfs,
                            periodic_kind_use_vfs_to_action, request_cancel,
                            update_current_job_idx)

name = 'periodic'
logger = P.logger

# 라이브러리 선택 목록 정렬 — 주기적 스캔·휴지통 스캔이 같은 설정을 쓴다.
PICK_SORTS = ('default_asc', 'default_desc', 'name_asc', 'name_desc')
PICK_SORT_KEY = 'jobs_lib_pick_sort'


class ModulePeriodic(PluginModuleBase):
    """주기적 스캔 — 라이브러리를 골라 cron으로 등록하면 정해진 시각에
    (옵션) VFS 새로고침 → 스캔을 하나씩 순차로 실행한다."""

    def __init__(self, P):
        super(ModulePeriodic, self).__init__(P, name=name, first_menu='setting')
        self.db_default = {
            f'{self.name}_db_version': '1',
            f'{self.name}_jobs': '[]',
            f'{self.name}_db_delete_day': '30',
            f'{self.name}_db_auto_delete': 'True',
            # 진행 중 표시 정리 시간(분) — 진행 상태를 확인할 방법이 없을 때
            # 이 시간이 지나면 '중단됨'으로 정리한다. 0이면 끈다 (기본값 30)
            f'{self.name}_stuck_timeout_min': '30',
            # VFS 새로고침 제한 시간(분) — 라이브러리 새로고침이 끝나기를
            # 이 시간까지 기다린다. 넘기면 그 라이브러리는 실패로 끝나고
            # 스캔은 보내지 않는다. 0 이하면 기본값으로 동작한다 (기본값 360)
            f'{self.name}_vfs_timeout_min': '360',
            # 스캔 대기 기한(분) — 스캔이 끝나기를 이 시간까지 기다린다.
            # 기한이 됐을 때 shyni에서 스캔이 아직 진행 중이면 이 시간만큼
            # 기한을 연장하며 계속 기다린다. 0 이하면 기본값으로 동작한다 (기본값 360)
            f'{self.name}_scan_timeout_min': '360',
            # 작업 대기 시간(분) — 앞선 작업이 돌고 있을 때 이 시간까지
            # 기다렸다가 끝나면 이어서 실행한다. 0이면 기다리지 않는다 (기본값 360)
            f'{self.name}_max_wait_min': '360',
        }
        self.web_list_model = ModelPeriodicItem

    def migration(self):
        # periodic_item에 스캔 결과 컬럼 보장 — 실제 컬럼 존재로 판단(멱등).
        # mod_scan.py의 migration과 같은 패턴이다.
        try:
            from sqlalchemy import inspect, text
            with F.app.app_context():
                engine = F.db.engines[P.package_name] if hasattr(F.db, 'engines') else F.db.get_engine(F.app, bind=P.package_name)
                insp = inspect(engine)
                if 'periodic_item' in insp.get_table_names():
                    cols = [c['name'] for c in insp.get_columns('periodic_item')]
                    new_cols = [
                        ('action', 'VARCHAR'),
                        ('shyni_job_id', 'VARCHAR'), ('stat_total', 'INTEGER'),
                        ('stat_new', 'INTEGER'), ('stat_missing', 'INTEGER'),
                        ('stat_updated', 'INTEGER'), ('stat_restored', 'INTEGER'),
                        ('stat_registered', 'INTEGER'), ('stat_failed', 'INTEGER'),
                        ('vfs_duration', 'INTEGER'), ('vfs_job_id', 'VARCHAR'),
                        ('scan_duration', 'INTEGER'),
                        ('append_files', 'TEXT'), ('append_count', 'INTEGER'),
                        ('stat_folders', 'TEXT'), ('wait_seconds', 'INTEGER'),
                    ]
                    added = [c for c, _ in new_cols if c not in cols]
                    if added:
                        with engine.begin() as conn:
                            for col, typ in new_cols:
                                if col not in cols:
                                    conn.execute(text(f'ALTER TABLE periodic_item ADD {col} {typ}'))
                        P.logger.info(f'[Periodic] migration: periodic_item 컬럼 추가 — {added}')
        except Exception as e:
            P.logger.error(f'[Periodic] migration 오류: {e}')

    def plugin_load(self):
        self.migration()
        # 재시작 복구: 스캔 중이던 이력이 있으면 완료 확인을 이어서 한다.
        # running 이력이 하나도 없을 때만(복구가 False를 돌려줄 때)
        # 나머지 이력을 중단됨으로 정리하고 진행 상태를 비운다 —
        # 방금 시작한 살아 있는 실행이 있으면 복구가 True를 돌려줘서
        # 여기서 정리하지 않는다.
        resumed = False
        try:
            resumed = TaskPeriodic.resume_running(self.start_celery)
        except Exception as e:
            logger.error(f'[Periodic] 재시작 복구 확인 실패: {e}')
        if not resumed:
            ModelPeriodicItem.set_terminated()
            P.ModelSetting.set('periodic_current', '')
        P.ModelSetting.set('periodic_cancel', '')
        self.start()

    # ---------- 스케줄러 ----------
    def start(self):
        for idx, item in enumerate(get_jobs()):
            if item.get('use'):
                self.sched_add(idx, item=item)

    def sched_add(self, idx, item=None):
        try:
            if item is None:
                item = get_jobs()[idx]
            if F.scheduler.is_include(item['job_id']):
                return True
            job = Job(self.P.package_name, item['job_id'], item['cron'],
                      self.job_function, item.get('name') or f"작업 {idx + 1}", args=(idx,))
            F.scheduler.add_job_instance(job)
            return True
        except Exception as e:
            logger.error(f'스케줄 등록 실패: {e}')
            logger.error(traceback.format_exc())
        return False

    def sched_remove_all(self):
        for item in get_jobs():
            try:
                if F.scheduler.is_include(item['job_id']):
                    F.scheduler.remove_job(item['job_id'])
            except Exception:
                pass
        # 인덱스가 바뀐 예전 작업까지 확실히 제거
        try:
            for job in list(F.scheduler.get_jobs()):
                jid = getattr(job, 'id', '') or ''
                if jid.startswith(f'{self.P.package_name}_periodic_'):
                    F.scheduler.remove_job(jid)
        except Exception:
            pass

    def job_function(self, idx, mode='scheduler'):
        # Celery에 넘기지 못하면 스레드·태스크 안에서 조용히 죽는다 —
        # '실행을 시작했습니다'라고 답한 뒤 아무것도 안 뜨는 경우다.
        # 그래서 여기서 실패를 잡아 이력 행과 배너 기록으로 남긴다.
        try:
            self.start_celery(TaskPeriodic.start, None, *(idx, mode))
        except Exception as e:
            logger.error(f'[Periodic] Celery 시작 실패: {e}')
            try:
                jobs = get_jobs()
                job = jobs[idx] if 0 <= idx < len(jobs) else None
                job_name = (job.get('name') if job else '') or ''
                action = (periodic_kind_use_vfs_to_action(
                    job.get('kind'), job.get('use_vfs')) if job else None)
            except Exception:
                job_name = ''
                action = None
            fail_start(job_name, mode, f'작업을 넘기지 못했습니다: {e}',
                       action=action)

    def one_execute(self, idx):
        # '지금 실행'은 수동 실행이라 mode를 'manual'로 넘긴다 (R19).
        # 스케줄에 등록된 작업도 스케줄러 경유가 아니라 이 경로로 실행한다.
        jobs = get_jobs()
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
                logger.error(f'[Periodic] 지금 실행 실패: {e}')
                fail_start(jobs[idx].get('name') or '', 'manual',
                           f'실행을 시작하지 못했습니다: {e}',
                           action=periodic_kind_use_vfs_to_action(
                               jobs[idx].get('kind'),
                               jobs[idx].get('use_vfs')))
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
                arg['periodic_jobs_json'] = P.ModelSetting.get('periodic_jobs') or '[]'
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
                # 동작은 드롭다운 값(action)으로 받아 kind + use_vfs 저장
                # 형식으로 바꾼다 (화면 매핑만 바꾸고 저장은 그대로 — 마이그레이션 없음).
                # 구 형식(kind + use_vfs)이 오면 그대로 받는다.
                action = job.get('action')
                if action in ('refresh', 'scan', 'refresh_scan'):
                    kind, use_vfs = periodic_action_to_kind_use_vfs(action)
                else:
                    kind = 'vfs' if job.get('kind') == 'vfs' else 'scan'
                    use_vfs = bool(job.get('use_vfs'))
                cleaned.append({
                    'name': (job.get('name') or f'작업 {i + 1}').strip(),
                    'sections': sections,
                    'cron': cron,
                    'kind': kind,
                    'use_vfs': use_vfs,
                    'use': bool(job.get('use')),
                })
            # 방어: 진행 중인 작업의 정의가 바뀌었거나 삭제됐으면 저장 거절.
            # (작업 관리 화면에서 행을 잠그지만, 페이지를 연 뒤 실행이
            # 시작되는 틈이나 다른 경로의 저장을 막기 위한 서버 쪽 확인이다.
            # 진행 중인 작업의 정의가 제출 목록 어딘가에 그대로 있으면 된다 —
            # 다른 행을 고치거나 앞 행을 지우는 저장은 그대로 허용한다.)
            # 여러 작업이 함께 돌 수 있으니 진행 중인 작업마다 확인한다.
            for cur in get_currents():
                # 아직 시작하지 않은 복구 항목은 작업 정의를 잠그지 않는다 —
                # 복구 작업이 뜨지 않으면 이 항목이 남아 작업 수정을 영원히 막는다.
                if (cur.get('stage') == 'waiting'
                        and cur.get('resume_item_id') is not None):
                    continue
                stored = get_jobs()
                ridx = cur.get('job_idx')
                running = (stored[ridx] if isinstance(ridx, int)
                           and 0 <= ridx < len(stored) else None)
                if running is None:
                    return jsonify({'ret': 'warning',
                                    'msg': '진행 중인 작업 정보를 찾지 못했습니다. 취소한 뒤에 저장해 주세요.'})

                def _same_job(a, b):
                    return ((a.get('name') or '').strip() == (b.get('name') or '').strip()
                            and (a.get('cron') or '').strip() == (b.get('cron') or '').strip()
                            and a.get('kind') == b.get('kind')
                            and bool(a.get('use_vfs')) == bool(b.get('use_vfs'))
                            and bool(a.get('use')) == bool(b.get('use'))
                            and sorted(str(s) for s in (a.get('sections') or []))
                            == sorted(str(s) for s in (b.get('sections') or [])))
                if not any(_same_job(running, c) for c in cleaned):
                    return jsonify({'ret': 'warning',
                                    'msg': f"진행 중인 작업 '{running.get('name') or ''}'은(는) 취소한 뒤에 수정할 수 있습니다."})
                # 순서 변경: 진행 중 작업이 새 목록에서 다른 위치로
                # 옮겨졌으면 진행 상태의 작업 번호를 새 위치로 고쳐 쓴다.
                new_idx = next((k for k, c in enumerate(cleaned)
                                if _same_job(running, c)), None)
                if new_idx is not None and new_idx != ridx:
                    update_current_job_idx(cur.get('claim_id'), new_idx)
            self.sched_remove_all()
            P.ModelSetting.set('periodic_jobs', json.dumps(cleaned, ensure_ascii=False))
            self.start()
            logger.info(f"[Periodic] 작업 저장: {len(cleaned)}개 — "
                        f"{[(j['name'], j['kind'], j['cron'], j['use']) for j in cleaned]}")
            ret['msg'] = f'작업 {len(cleaned)}개를 저장했습니다. 사용으로 켜진 작업은 스케줄러에 반영됐습니다.'
        elif command == 'task_execute':
            try:
                idx = int(arg1)
            except Exception:
                return jsonify({'ret': 'warning', 'msg': '작업 번호가 올바르지 않습니다.'})
            jobs = get_jobs()
            if idx < 0 or idx >= len(jobs):
                return jsonify({'ret': 'warning', 'msg': '작업 번호가 올바르지 않습니다.'})
            if not (jobs[idx].get('sections') or []):
                return jsonify({'ret': 'warning',
                                'msg': '선택한 라이브러리가 없습니다. 라이브러리 선택에서 골라 주세요.'})
            if _same_job_busy(jobs[idx].get('name') or ''):
                # 거절은 로그와 배너에 남긴다 — 안 그러면 '시작 중'
                # 카드만 떴다 사라지고 왜 시작 안 했는지 알 수 없다
                # (2026-11-25 다른 서버 미드 스캔 수동 실행 실측).
                logger.info(f"[Periodic] 지금 실행 거절: {jobs[idx].get('name') or ''} — 이미 실행 중입니다.")
                _record_start_error(jobs[idx].get('name') or '',
                                    '이미 실행 중입니다.')
                return jsonify({'ret': 'warning', 'msg': '이미 실행 중입니다.'})
            # 규칙 1이 규칙 2보다 먼저다. 규칙 1: 같은 모듈 안에서
            # 같은 라이브러리가 실행 중이면 동작 종류와 무관하게
            # 거절한다. 새로고침만 하는 동작도 예외가 아니다.
            if _same_library_busy(jobs[idx]['sections'][0]):
                logger.info(f"[Periodic] 지금 실행 거절: {jobs[idx].get('name') or ''} — {LIBRARY_BUSY_MESSAGE}")
                _record_start_error(jobs[idx].get('name') or '',
                                    LIBRARY_BUSY_MESSAGE)
                return jsonify({'ret': 'warning',
                                'msg': LIBRARY_BUSY_MESSAGE})
            # 규칙 2: 스캔이 들어간 동작은 첫 라이브러리 기준으로
            # 게이트를 본다 — 같은 라이브러리가 스캔 중이거나 설정한
            # 개수까지 돌고 있으면 시작하지 않고 거절한다.
            # 새로고침만 하는 동작은 스캔 자리를 안 쓴다.
            action = periodic_kind_use_vfs_to_action(
                jobs[idx].get('kind'), jobs[idx].get('use_vfs'))
            if 'scan' in str(action or ''):
                gate_msg = ModuleStatus.library_gate_check(jobs[idx]['sections'][0])
                if gate_msg:
                    logger.info(f"[Periodic] 지금 실행 거절: {jobs[idx].get('name') or ''} — {gate_msg}")
                    _record_start_error(jobs[idx].get('name') or '', gate_msg)
                    return jsonify({'ret': 'warning', 'msg': gate_msg})
            result = self.one_execute(idx)
            logger.info(f'[Periodic] 지금 실행 요청: idx={idx} 결과={result}')
            if result == 'is_running':
                ret['msg'] = '이미 실행 중입니다.'
            elif result == 'fail':
                return jsonify({'ret': 'warning', 'msg': '실행하지 못했습니다.'})
            else:
                ret['msg'] = '실행을 시작했습니다. 잠시 뒤 워커가 작업을 시작합니다.'
        elif command == 'cancel_current':
            claim_id = (arg1 or '').strip() or None
            curs = get_currents()
            cur = None
            if claim_id is not None:
                for c in curs:
                    if str(c.get('claim_id')) == claim_id:
                        cur = c
                        break
            elif len(curs) == 1:
                cur = curs[0]
            if cur is None:
                return jsonify({'ret': 'warning',
                                'msg': '지금 진행 중인 주기적 스캔이 없습니다.'})
            # 아직 시작하지 않은 복구 항목은 실행 중인 작업이 없어
            # 취소 요청만으로는 안 사라진다 — 이력을 취소됨으로 끝내고
            # 항목을 바로 지운다. 취소 요청도 함께 남겨, 늦게 뜨는
            # 복구 작업이 있으면 시작할 때 멈추게 한다.
            if (cur.get('resume_item_id') is not None
                    and (cur.get('stage') or '') == 'waiting'):
                request_cancel(cur.get('claim_id'))
                try:
                    item = (F.db.session.query(ModelPeriodicItem)
                            .filter(ModelPeriodicItem.id
                                    == int(cur.get('resume_item_id'))).first())
                    if item is not None and item.status in ('running', 'waiting'):
                        item.finish('cancelled', '취소했습니다. (실행되지 않은 복구 작업)')
                except Exception as e:
                    logger.warning(f'[Periodic] 복구 이력 취소 기록 실패: {e}')
                _clear_resume_current(cur.get('resume_item_id'))
                logger.info('[Periodic] 실행되지 않은 복구 작업 취소·정리')
                ret['msg'] = '실행되지 않은 복구 작업을 정리했습니다.'
                return jsonify(ret)
            request_cancel(cur.get('claim_id'))
            logger.info('[Periodic] 취소 요청')
            # Step5-1 — shyni 스캔까지 함께 취소하므로 "웹에서 취소" 안내는 없앴다.
            if (cur.get('stage') or '') == 'scan':
                ret['msg'] = '취소를 요청했습니다. shyni 스캔 취소가 확인되면 멈춥니다.'
            elif (cur.get('stage') or '') == 'waiting':
                ret['msg'] = '취소를 요청했습니다. 대기가 중단되면 멈춥니다.'
            else:
                ret['msg'] = '취소를 요청했습니다. 새로고침이 정리되면 멈춥니다.'
        return jsonify(ret)

    # ---------- 화면 AJAX ----------
    def process_ajax(self, sub, req):
        if sub == 'sections':
            sections, err = ModuleStatus._sections()
            if sections is None:
                return jsonify({'ret': 'fail', 'msg': err})
            return jsonify({'ret': 'success', 'data': sections})
        if sub == 'current':
            from .model_periodic import action_label
            cards = []
            for cur in get_currents():
                if cur.get('action'):
                    cur['action_label'] = action_label(cur.get('action'))
                if cur.get('stage') == 'scan' and cur.get('section_key'):
                    cur['scan_progress'] = current_scan_progress(
                        cur['section_key'], 'periodic')
                # 취소 요청 여부 — 새로고침해도 '취소중' 버튼이 유지되게
                # 서버에서 알려준다. 취소 요청은 작업이 멈출 때 지워진다.
                cur['cancel_requested'] = bool(
                    _cancel_requested(cur.get('claim_id')))
                cards.append(cur)
            # 대시보드에서 시작한 스캔도 함께 보여 준다 — 어디서
            # 시작했든 진행 중인 스캔을 한곳에서 볼 수 있게 한다.
            try:
                cards.extend(ModuleStatus.dashboard_scan_cards())
            except Exception as e:
                logger.warning(f'[Periodic] 대시보드 스캔 목록 조회 실패: {e}')
            return jsonify({'ret': 'success', 'data': cards,
                            'last_error': get_last_start_error()})
        if sub == 'clear_start_error':
            clear_start_error()
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
            data = ModelPeriodicItem.get_page(
                page, section_key=section, only_append=only_append, search=search)
            annotate_total_labels(data.get('list') or [])
            return jsonify({'ret': 'success', 'data': data})
        if sub == 'history_detail':
            try:
                item_id = int(req.form.get('id') or 0)
            except Exception:
                item_id = 0
            data = ModelPeriodicItem.get_detail(item_id)
            if data is None:
                return jsonify({'ret': 'fail', 'msg': '이력을 찾지 못했습니다.'})
            annotate_total_labels([data])
            return jsonify({'ret': 'success', 'data': data})
        return jsonify({'ret': 'fail', 'msg': '알 수 없는 요청입니다.'})

    def process_api(self, sub, req):
        return self.process_ajax(sub, req)
