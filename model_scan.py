from sqlalchemy import and_, desc, func, not_, or_

from .setup import *

logger = P.logger


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
    mediapart_id = db.Column(db.String)
    # 샤이니 대상 실행 상태(F-4: 대상별 독립) — RUNNING/FINISH/FAILED/ERROR/SKIP/NOT_FIND_LIBRARY
    shyni_status = db.Column(db.String)
    shyni_job_id = db.Column(db.String)
    vfs_done = db.Column(db.Boolean)  # VFS 새로고침 완료 여부 (체크완료 표시용)
    vfs_done_time = db.Column(db.DateTime)  # VFS 새로고침 완료 시각
    reset_done = db.Column(db.Boolean)  # 선택 항목 리셋으로 초기화된 항목인지 표시
    reset_time = db.Column(db.DateTime)  # 리셋된 시각
    in_library = db.Column(db.Boolean)  # shyni 라이브러리에 이미 반영된 항목인지 표시
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
                    ret = requests.post(self.callback_url, data=self.as_dict()).text
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
    def make_query(cls, req, order='desc', search='', option1='all', option2='all', option3='all'):
        with F.app.app_context():
            query = cls.make_query_search(F.db.session.query(cls), search, cls.target)

            if option1 != 'all':
                query = query.filter(cls.callback == option1)

            if option2 != 'all':
                query = query.filter(cls.status == option2)

            if option3 != 'all':
                query = query.filter(cls.mode == option3)

            if order == 'desc':
                query = query.order_by(desc(cls.id))
            else:
                query = query.order_by(cls.id)

            return query


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
            ret['list'] = [item.as_dict() for item in lists]
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
