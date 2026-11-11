from sqlalchemy import desc

from .setup import *

logger = P.logger


class ModelTrashItem(ModelBase):
    """휴지통 주기 작업 실행 이력."""
    P = P
    __tablename__ = 'trash_item'
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = P.package_name

    id = db.Column(db.Integer, primary_key=True)
    created_time = db.Column(db.DateTime)
    mode = db.Column(db.String)            # scheduler | manual
    status = db.Column(db.String)          # running | done | failed | cancelled | terminated
    job_name = db.Column(db.String)
    section_key = db.Column(db.String)
    section_title = db.Column(db.String)
    start_time = db.Column(db.DateTime)
    finish_time = db.Column(db.DateTime)
    duration = db.Column(db.Integer)       # 초
    message = db.Column(db.String)

    def save(self):
        # SQLite 잠금이면 재시도한다 (스캔 항목과 같은 DB를 쓴다)
        from .extensions import save_with_retry
        return save_with_retry(self)

    def __init__(self, job_name, section_key, section_title, mode):
        self.created_time = datetime.now()
        self.start_time = datetime.now()
        self.mode = mode
        self.status = 'running'
        self.job_name = job_name
        self.section_key = str(section_key)
        self.section_title = section_title

    def finish(self, status, message=None):
        self.status = status
        self.finish_time = datetime.now()
        if self.start_time and self.finish_time:
            self.duration = int((self.finish_time - self.start_time).total_seconds())
        if message:
            self.message = str(message)[:500]
        self.save()

    @classmethod
    def set_terminated(cls):
        """플러그인 재시작 시 running으로 남은 이력을 정리한다."""
        with F.app.app_context():
            try:
                rows = F.db.session.query(cls).filter(cls.status == 'running').all()
                for row in rows:
                    row.status = 'terminated'
                    row.finish_time = datetime.now()
                    if row.start_time:
                        row.duration = int((row.finish_time - row.start_time).total_seconds())
                    row.save()
            except Exception as e:
                logger.error(f'set_terminated 실패: {e}')

    @classmethod
    def get_recent(cls, limit=10):
        with F.app.app_context():
            rows = F.db.session.query(cls).order_by(desc(cls.id)).limit(limit).all()
            out = []
            for r in rows:
                out.append({
                    'id': r.id,
                    'mode': r.mode,
                    'status': r.status,
                    'job_name': r.job_name,
                    'section_title': r.section_title,
                    'start_time': r.start_time.strftime('%m-%d %H:%M:%S') if r.start_time else '',
                    'finish_time': r.finish_time.strftime('%m-%d %H:%M:%S') if r.finish_time else '',
                    'duration': r.duration,
                    'message': r.message or '',
                })
            return out

    @classmethod
    def get_page(cls, page=1, per=30):
        with F.app.app_context():
            total = F.db.session.query(cls).count()
            pages = max(1, (total + per - 1) // per)
            try:
                page = int(page or 1)
            except Exception:
                page = 1
            page = min(max(1, page), pages)
            rows = (F.db.session.query(cls).order_by(desc(cls.id))
                    .offset((page - 1) * per).limit(per).all())
            out = []
            for r in rows:
                out.append({
                    'id': r.id,
                    'mode': r.mode,
                    'status': r.status,
                    'job_name': r.job_name,
                    'section_title': r.section_title,
                    'start_time': r.start_time.strftime('%m-%d %H:%M:%S') if r.start_time else '',
                    'finish_time': r.finish_time.strftime('%m-%d %H:%M:%S') if r.finish_time else '',
                    'duration': r.duration,
                    'message': r.message or '',
                })
            return {'list': out, 'count': total, 'page': page, 'pages': pages}
