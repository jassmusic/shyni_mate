import time as _time
from datetime import timedelta

from sqlalchemy import and_, desc, false, func, not_, or_

from .setup import *

logger = P.logger

# 설정값 단기 캐시 — analyze 대기 시간은 목록 조회·진행현황 집계·게이트 판정이
# 한 요청 안에서도 여러 번 읽는데, 그때마다 DB를 조회하면 연결 풀 대기로
# 화면 로딩이 수십 초씩 멈추는 일이 있었다 (2026-11-11 실측: QueuePool
# 타임아웃 30초). 프로세스 안에서 30초만 기억해 재사용한다.
_setting_cache = {}


class ModelScanItem(ModelBase):
    P = P
    __tablename__ = f'scan_item'
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = P.package_name

    id = db.Column(db.Integer, primary_key=True)
    created_time    = db.Column(db.DateTime)
    filecheck_time  = db.Column(db.DateTime)
    completed_time  = db.Column(db.DateTime)

    callback = db.Column(db.String)
    callback_id = db.Column(db.String)
    callback_url = db.Column(db.String)
    mode = db.Column(db.String) # add remove
    target = db.Column(db.String) # 입력받은 것 그대로
    target_type = db.Column(db.String) # file, folder
    target_section_id = db.Column(db.Integer) # 입력 받을 수도 있음. 같은 폴더가 여러 라이브러리에 있을 수 있음. 모든것을 다 스캔하기는 귀찮지만 이 섹션id를 요청하면 여기 아래 폴더만 찾음. 없다면 db 순서대로 하나 찾으면 끝
    flag_filecheck = db.Column(db.Boolean)
    flag_last_filecheck = db.Column(db.Boolean)
    flag_db_include = db.Column(db.Boolean)
    flag_cancel = db.Column(db.Boolean)
    flag_finish = db.Column(db.Boolean)

    scan_folder = db.Column(db.String)
    process_pid = db.Column(db.String)
    process_start_time = db.Column(db.DateTime)
    process_finish_time = db.Column(db.DateTime)
    process_duration = db.Column(db.Integer)
    status = db.Column(db.String)
    queue_list = []

    filecheck_count = db.Column(db.Integer)
    send_attempt = db.Column(db.Integer)  # shyni 전송 시도 번호 — 멱등 키 재료 전용 (파일체크 횟수와 별개로 세며, 리셋·재시도에서도 되돌리지 않는다)
    mediapart_id = db.Column(db.String)
    # 샤이니 대상 실행 상태(F-4: 대상별 독립) — RUNNING/FINISH/FAILED/ERROR/SKIP/NOT_FIND_LIBRARY
    shyni_status = db.Column(db.String)
    shyni_job_id = db.Column(db.String)
    shyni_started_at = db.Column(db.DateTime)  # shyni job이 실제 시작된 시각 (compat 폴링의 started_at) — analyze 정체 판정 기준
    shyni_phase = db.Column(db.String)  # shyni job 단계 (probe_assembling = ffprobe 대기 중) — 진행중 세부 표시용
    probe_received = db.Column(db.Integer)  # 조립 중 받은 파일 수 (job stats manifest_files)
    probe_expected = db.Column(db.Integer)  # 조립 전체 파일 수 (job stats expected_files)
    vfs_done = db.Column(db.Boolean)  # VFS 새로고침 완료 여부 (체크완료 표시용)
    vfs_done_time = db.Column(db.DateTime)  # VFS 새로고침 완료 시각
    held = db.Column(db.Boolean)  # Shyni 연동이 꺼진 동안 쌓여 보류된 항목인지 표시 (켜도 자동 처리 안 함)
    reset_done = db.Column(db.Boolean)  # 선택 항목 리셋으로 초기화된 항목인지 표시
    reset_time = db.Column(db.DateTime)  # 리셋된 시각
    in_library = db.Column(db.Boolean)  # shyni 라이브러리에 이미 반영된 항목인지 표시
    fail_reason = db.Column(db.String)  # 실패 사유 (전송 단계 오류 등) — 목록 표시용
    send_error_count = db.Column(db.Integer)  # 전송 단계 오류가 연속된 횟수 — 상한(5회)을 넘기면 실패로 확정한다
    shyni_late_result = db.Column(db.String)  # 분석시간 초과로 실패 표시한 뒤 확인한 shyni job의 뒤늦은 결과 (FINISH/FAILED/GONE)
    shyni_late_finished_at = db.Column(db.DateTime)  # 뒤늦게 끝난 shyni job의 종료 시각
    shyni_late_checked_at = db.Column(db.DateTime)  # 뒤늦은 결과를 마지막으로 확인한 시각 (재확인 간격 조절용)
    #metadata_title = db.Column(db.String)
    #metadata_type = db.Column(db.String)
    #metadata_item_id = db.Column(db.String)
    #show_metadata_item_id = db.Column(db.String)
    meta_info = db.Column(db.JSON)
    section_id = db.Column(db.String)
    section_type = db.Column(db.String)

    # 상태 (status)
    # READY : 대기·처리 중 (파일 체크, VFS 새로고침, 스캔 전송·진행 중 모두 포함.
    #         세부 진행은 shyni_status(None/RUNNING/FINISH 등)가 따로 표시한다)
    # FINISH_SHYNI : shyni 스캔까지 마치고 완료
    # FINISH_SHYNI_FAILED : 처리 실패 (경로·섹션을 찾지 못했거나 shyni 작업이 실패)

    def save(self):
        # SQLite 잠금이면 재시도한다 (등록 폭주 시 항목이 조용히 사라지던 문제)
        from .extensions import save_with_retry
        return save_with_retry(self)

    def __init__(self, target, mode="ADD", target_section_id='0', callback_id=None, callback_url=None):
        self.mode = mode
        self.target = target
        self.target_section_id = target_section_id

        self.callback_id = callback_id
        if self.callback_id != None:
            self.callback = callback_id.rsplit('_', 1)[0]
        self.callback_url = callback_url

        self.created_time = datetime.now()
        self.status = "READY"
        self.flag_filecheck = False
        self.flag_last_filecheck = False
        self.flag_db_include = False
        self.flag_cancel = False
        self.flag_finish = False
        self.filecheck_count = 0
        self.send_attempt = 0
        self.held = False
        self.reset_done = False
        self.in_library = None



    @classmethod
    def get_incompleted(cls):
        with F.app.app_context():
            return db.session.query(cls).filter_by(
                flag_finish=False
            ).all()


    ### only for queue
    def init_for_queue(self):
        self.queue_list.append(self)

    @classmethod
    def get_by_id_in_queue(cls, id):
        for _ in cls.queue_list:
            if _.id == int(id):
                return _

    @classmethod
    def remove_in_queue(cls, db_item):
        ret = []
        for _ in cls.queue_list:
            if _.id == db_item.id:
                continue
            ret.append(_)
        cls.queue_list = ret



    def set_status(self, status, save=False):
        self.status = status
        # 리셋 표시는 재처리가 끝나도 지우지 않는다 (리셋한 항목임을 목록에서 계속 보여 준다)
        if status == 'ENQUEUE_ADD_FIND':
            self.filecheck_time = datetime.now()
            self.flag_filecheck = True
        elif status == 'SCANNING':
            self.process_start_time = datetime.now()
        elif status == 'FINISH_ADD':
            self.process_finish_time = datetime.now()
            self.process_duration = str(self.process_finish_time - self.process_start_time)
        if self.status.startswith('FINISH_'):
            self.completed_time = datetime.now()
            if self.callback_id != None and self.callback_url != None:
                try:
                    # 콜백 상대가 응답하지 않아도 처리가 멈추지 않게 시간 제한을 둔다
                    # (제한이 없던 시절 파일체크 주기 전체가 멈춘 전례 — 2026-11-11)
                    # 시간은 고급 설정 base_timeout_callback (기본 10초)
                    cb_timeout = 10
                    try:
                        _v = P.ModelSetting.get_int('base_timeout_callback')
                        if _v and _v > 0:
                            cb_timeout = _v
                    except Exception:
                        pass
                    ret = requests.post(self.callback_url, data=self.as_dict(), timeout=cb_timeout).text
                    logger.debug(f'scan callback : {ret}')
                except Exception as e:
                    logger.error(f'Exception:{str(e)}')
                    #logger.error(traceback.format_exc())
            self.remove_in_queue(self)
        if save:
            self.save()


    @classmethod
    def get_list_by_status(cls, status):
        with F.app.app_context():
            query = db.session.query(cls).filter(
                cls.status == status,
            )
            query = query.order_by(cls.id)
            return query.all()


    @classmethod
    def set_status_incompleted_to_ready(cls):
        with F.app.app_context():
            ret = db.session.query(cls).filter(
                (not_(cls.status.like('FINISH_%'))) & (cls.mode == 'ADD')
            ).update({'status':'READY'}, synchronize_session="fetch")
            db.session.commit()
            return ret

    @classmethod
    def analyze_overdue_condition(cls, wait_min=None, now=None):
        """analyze 대기 시간(scan_analyze_wait_min)을 넘겨 실행 중인 항목을 가르는 조건.

        동시 스캔 수 게이트 제외(task_scan.running_shyni_count)와 스캔 목록의
        '분석중' 표시 분류가 이 조건 하나를 공유한다 (기준이 갈라지지 않게).
        기준 시각은 shyni job의 실제 시작(shyni_started_at), 기록이 없으면
        mate가 전송한 시각(process_start_time)이다.
        REFRESH 등록 대기(refresh-scan:)는 실제 등록 스캔이 도는 구간을 포함해
        제외하지 않는다. 설정이 0이면 어떤 항목도 해당하지 않는다."""
        if wait_min is None:
            wait_min = cls.analyze_wait_min_cached()
        if now is None:
            now = datetime.now()
        if wait_min <= 0:
            return false()
        cutoff = now - timedelta(minutes=wait_min)
        started = or_(
            and_(cls.shyni_started_at.isnot(None), cls.shyni_started_at <= cutoff),
            and_(cls.shyni_started_at.is_(None),
                 cls.process_start_time.isnot(None),
                 cls.process_start_time <= cutoff))
        not_refresh_wait = or_(cls.shyni_job_id.is_(None),
                                not_(cls.shyni_job_id.like('refresh-scan:%')))
        return and_(started, not_refresh_wait)

    @classmethod
    def analyze_wait_min_cached(cls):
        """analyze 대기 시간 설정을 30초 캐시로 읽는다. 읽기에 실패하면
        캐시하지 않고 0(기능 끔에 준하는 값)을 돌려줘 화면 조회를 멈추지 않는다."""
        hit = _setting_cache.get('scan_analyze_wait_min')
        now_ts = _time.time()
        if hit is not None and now_ts - hit[1] < 30:
            return hit[0]
        try:
            val = P.ModelSetting.get_int('scan_analyze_wait_min')
        except Exception:
            val = None
        if val is None:
            return 0
        _setting_cache['scan_analyze_wait_min'] = (val, now_ts)
        return val

    @classmethod
    def is_analyze_overdue(cls, item, wait_min=None, now=None):
        """이미 불러온 항목 객체 하나에 대한 '분석중' 판정.

        make_query의 ANALYZING 분류(상태·실행 상태·단계·analyze_overdue_condition)와
        같은 기준을 쿼리 없이 적용한다 — 목록 한 페이지(30개)처럼 소수만 판정할 때
        분류 쿼리를 통째로 다시 돌리지 않기 위함이다."""
        if wait_min is None:
            wait_min = cls.analyze_wait_min_cached()
        if now is None:
            now = datetime.now()
        if not wait_min or wait_min <= 0:
            return False
        if item.status != 'READY':
            return False
        if item.shyni_status not in ('RUNNING', 'REQUESTED'):
            return False
        if item.shyni_phase in ('probe_assembling', 'queued'):
            return False
        if str(item.shyni_job_id or '').startswith('refresh-scan:'):
            return False
        started = (item.shyni_started_at if item.shyni_started_at is not None
                   else item.process_start_time)
        if started is None:
            return False
        return started <= now - timedelta(minutes=wait_min)

    @classmethod
    def make_query(cls, req, order='desc', search='', option1='all', option2='all', option3='all'):
        with F.app.app_context():
            # 표시 상태 이름으로 검색하면(예: '분석중') 경로가 아니라 그 상태로 걸러 본다.
            # 상태 필터(option2)를 직접 고른 경우가 우선한다.
            if option2 == 'all' and (search or '').strip() == '분석중':
                option2 = 'ANALYZING'
                query = F.db.session.query(cls)
            else:
                query = cls.make_query_search(F.db.session.query(cls), search, cls.target)

            if option1 != 'all':
                query = query.filter(cls.callback == option1)

            if option2 != 'all':
                if option2 == 'SCANNING':
                    # 진행중 — 대기(ffprobe/스캔 차례)가 아니라 실제로 스캔이 도는 중.
                    # analyze 대기 시간을 넘긴 항목은 아래 '분석중'으로 따로 가른다.
                    query = query.filter(cls.status == 'READY',
                                         cls.shyni_status.in_(('RUNNING', 'REQUESTED')),
                                         or_(cls.shyni_phase.is_(None),
                                             not_(cls.shyni_phase.in_(('probe_assembling', 'queued')))),
                                         not_(cls.analyze_overdue_condition()))
                elif option2 == 'ANALYZING':
                    # 분석중 — 스캔은 시작됐지만 analyze 단계에서 analyze 대기 시간 이상
                    # 머무는 중. 동시 스캔 수 게이트에서 제외되는 항목과 같은 기준
                    # (analyze_overdue_condition)을 쓴다. ffprobe 대기·스캔 대기와는
                    # 겹치지 않게 그 분류를 먼저 적용한 나머지를 가른다.
                    query = query.filter(cls.status == 'READY',
                                         cls.shyni_status.in_(('RUNNING', 'REQUESTED')),
                                         or_(cls.shyni_phase.is_(None),
                                             not_(cls.shyni_phase.in_(('probe_assembling', 'queued')))),
                                         cls.analyze_overdue_condition())
                elif option2 == 'ASSEMBLING':
                    # ffprobe 대기 중 — 전송은 됐고 shyni가 나머지 데이터를 기다리는 중
                    query = query.filter(cls.status == 'READY',
                                         cls.shyni_status.in_(('RUNNING', 'REQUESTED')),
                                         cls.shyni_phase == 'probe_assembling',
                                         or_(cls.probe_expected.is_(None),
                                             cls.probe_expected == 0,
                                             cls.probe_received.is_(None),
                                             cls.probe_received < cls.probe_expected))
                elif option2 == 'QUEUED':
                    # 스캔 대기 중 — 데이터는 다 모였고(또는 일반 job이) 스캐너 차례를 기다리는 중
                    query = query.filter(cls.status == 'READY',
                                         cls.shyni_status.in_(('RUNNING', 'REQUESTED')),
                                         or_(cls.shyni_phase == 'queued',
                                             and_(cls.shyni_phase == 'probe_assembling',
                                                  cls.probe_expected.isnot(None),
                                                  cls.probe_expected > 0,
                                                  cls.probe_received.isnot(None),
                                                  cls.probe_received >= cls.probe_expected)))
                elif option2 == 'ALREADY_IN_DB':
                    # 완료됐지만 신규 등록이 없어 이미 반영돼 있던 항목 (plex의 FINISH_ADD_ALREADY_IN_DB)
                    query = query.filter(cls.status == 'FINISH_SHYNI',
                                         cls.in_library.is_(True))
                elif option2 == 'NOT_FIND_LIBRARY':
                    query = query.filter(cls.status == 'FINISH_SHYNI_FAILED',
                                         cls.shyni_status == 'NOT_FIND_LIBRARY')
                elif option2 == 'NOT_FIND_IN_LIBRARY':
                    query = query.filter(cls.status == 'FINISH_SHYNI_FAILED',
                                         cls.shyni_status == 'NOT_FIND_IN_LIBRARY')
                elif option2 == 'FAILED':
                    # 라이브러리/항목 없음 외의 job 실패·오류
                    query = query.filter(cls.status == 'FINISH_SHYNI_FAILED',
                                         cls.shyni_status.in_(('FAILED', 'ERROR')))
                elif option2 == 'ANALYZE_TIMEOUT':
                    # analyze 후 대기 시간을 넘겨 실패로 확정된 항목 (시간 초과)
                    query = query.filter(cls.status == 'FINISH_SHYNI_FAILED',
                                         cls.shyni_status == 'ANALYZE_TIMEOUT')
                elif option2 == 'HELD':
                    # 보류 — Shyni 연동이 꺼진 동안 쌓여, 켜도 자동 처리되지 않는 항목
                    query = query.filter(cls.status == 'READY', cls.held.is_(True))
                elif option2 == 'READY':
                    # 진행중은 READY가 아니다 — 아직 전송되지 않은 항목만 (보류 제외)
                    query = query.filter(cls.status == 'READY',
                                         or_(cls.held.is_(None), cls.held.is_(False)),
                                         or_(cls.shyni_status.is_(None),
                                             not_(cls.shyni_status.in_(('RUNNING', 'REQUESTED')))))
                else:
                    query = query.filter(cls.status == option2)

            if option3 != 'all':
                query = query.filter(cls.mode == option3)

            if order == 'desc':
                query = query.order_by(desc(cls.id))
            else:
                query = query.order_by(cls.id)

            return query


    @classmethod
    def release_all_held(cls):
        """보류 중인 READY 항목을 전부 해제하고 개수를 돌려준다."""
        with F.app.app_context():
            rows = db.session.query(cls).filter(
                cls.status == 'READY', cls.held.is_(True)).all()
            for item in rows:
                item.held = False
                item.save()
            return len(rows)

    @classmethod
    def summary_counts(cls):
        """스캔 진행현황 한 줄용 집계 — 목록 필터(표시 상태)와 같은 기준으로 센다."""
        with F.app.app_context():
            def _query(option2):
                return cls.make_query(None, order='asc', search='', option2=option2)
            scanning_ids = [r[0] for r in
                            _query('SCANNING').with_entities(cls.id).all()]
            analyzing_ids = [r[0] for r in
                             _query('ANALYZING').with_entities(cls.id).all()]
            queued_ids = [r[0] for r in
                          _query('QUEUED').with_entities(cls.id).all()]
            # 최근 완료 10개 — 스캔 목록 '최근 완료' 토글이 번호를 펼쳐 보여 준다
            recent_done_ids = [r[0] for r in db.session.query(cls.id).filter(
                cls.status == 'FINISH_SHYNI').order_by(
                    cls.completed_time.desc(), cls.id.desc()).limit(10).all()]
            return {'recent_done_ids': recent_done_ids,
                    'scanning_ids': scanning_ids,
                    'analyzing_ids': analyzing_ids,
                    'assembling': _query('ASSEMBLING').count(),
                    'queued': len(queued_ids),
                    'queued_ids': queued_ids,
                    'ready': _query('READY').count(),
                    'held': _query('HELD').count()}

    @classmethod
    def web_list(cls, req) -> dict:
        '''override'''
        try:
            ret = {}
            page = 1
            page_size = 30
            search = ''
            if 'page' in req.form:
                page = int(req.form['page'])
            if 'keyword' in req.form:
                search = req.form['keyword'].strip()
            option1 = req.form.get('option1', 'all')
            option2 = req.form.get('option2', 'all')
            option3 = req.form.get('option3') or 'all'
            order = req.form['order'] if 'order' in req.form else 'desc'

            query = cls.make_query(req, order=order, search=search, option1=option1, option2=option2, option3=option3)
            count = query.count()
            query = query.limit(page_size).offset((page-1)*page_size)
            #F.logger.debug('cls count:%s', count)
            lists = query.all()
            # 행 표시용 '분석중' 플래그 — 목록 필터·진행현황 집계와 같은 분류를
            # 이미 불러온 이 페이지 항목들에만 객체 판정으로 적용해 함께 내려준다.
            # 예전에는 분류 쿼리를 한 번 더 돌렸는데, 그 쿼리의 설정값 조회가
            # DB 연결 풀에서 30초씩 대기해 목록 로딩 전체를 멈추는 일이 있었다
            # (2026-11-11 실측). 판정 기준은 is_analyze_overdue 한 곳에서 공유한다.
            analyzing_ids = set()
            if lists:
                wait_min = cls.analyze_wait_min_cached()
                now = datetime.now()
                analyzing_ids = {item.id for item in lists
                                 if cls.is_analyze_overdue(item, wait_min, now)}
            ret['list'] = []
            for item in lists:
                row = item.as_dict()
                row['analyzing'] = item.id in analyzing_ids
                ret['list'].append(row)
            ret['paging'] = cls.get_paging_info(count, page, page_size)
            try:
                if cls.P.ModelSetting is not None and cls.__tablename__ is not None:
                    cls.P.ModelSetting.set(f'{cls.__tablename__}_last_list_option', f'{order}|{page}|{search}|{option1}|{option2}|{option3}')
            except Exception as e:
                F.logger.error(f"Exception:{str(e)}")
                F.logger.error(traceback.format_exc())
                F.logger.error(f'{cls.__tablename__}_last_list_option ERROR!' )
            return ret
        except Exception as e:
            cls.P.logger.error(f"Exception:{str(e)}")
            cls.P.logger.error(traceback.format_exc())

    """
    @classmethod
    def get_items(cls, mode):
        if mode == 'wait':
            query = db.session.query(cls).filter(or_(cls.status == 'ready', cls.status.like('wait_%')))

        elif mode == 'run':
            query = db.session.query(cls).filter(cls.status.like('run_%'))
        elif mode == 'all':
            query = db.session.query(cls)

        query = query.order_by(cls.id)
        #logger.debug(query)
        items = query.all()
        return items


    @classmethod
    def is_already_scan_folder_exist(cls, scan_folder):
        query = db.session.query(cls).filter(
            cls.scan_folder == scan_folder,
            or_(cls.status == 'run_add_find', cls.status == 'run_remove_removed')
        )
        query = query.order_by(cls.id)
        items = query.all()
        return items

    @classmethod
    def not_finished_to_ready(cls):
        items = db.session.query(cls).filter(not_(cls.status.like('finish_%'))).with_for_update().all()
        for item in items:
            item.status = 'ready'
        db.session.commit()


    # JSON


    """
