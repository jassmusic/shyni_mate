import json

from sqlalchemy import desc, or_

from .setup import *

logger = P.logger


class ModelPeriodicItem(ModelBase):
    """주기적 스캔 실행 이력."""
    P = P
    __tablename__ = 'periodic_item'
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
    # 스캔 결과 (z38 — 완료 시 job stats와 추가 파일 목록을 저장한다).
    # 업데이트 전에 기록된 행에는 이 값들이 없다(NULL). 읽는 쪽은 NULL을
    # '없음'으로 취급하고 화면에는 '-'로 표시한다. 소급해서 채우지 않는다.
    shyni_job_id = db.Column(db.String)
    stat_total = db.Column(db.Integer)
    stat_new = db.Column(db.Integer)
    stat_missing = db.Column(db.Integer)
    stat_updated = db.Column(db.Integer)
    stat_restored = db.Column(db.Integer)
    stat_registered = db.Column(db.Integer)
    stat_failed = db.Column(db.Integer)
    vfs_duration = db.Column(db.Integer)   # 초 (VFS 새로고침 단계 소요)
    scan_duration = db.Column(db.Integer)  # 초 (스캔 단계 소요)
    append_files = db.Column(db.Text)      # JSON 배열(추가 파일 경로, 최대 1,000개)
    append_count = db.Column(db.Integer)   # 추가 파일 전체 수(저장 상한을 넘으면 파일 수보다 크다)
    stat_folders = db.Column(db.Text)      # JSON 배열(job stats의 폴더별 results[])

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
    def _row_dict(cls, r):
        # 결과 컬럼은 예전 행에서 NULL일 수 있다 — None 그대로 내보내고
        # 화면에서 '-'로 표시한다.
        return {
            'id': r.id,
            'mode': r.mode,
            'status': r.status,
            'job_name': r.job_name,
            'section_key': r.section_key,
            'section_title': r.section_title,
            'start_time': r.start_time.strftime('%m-%d %H:%M:%S') if r.start_time else '',
            'finish_time': r.finish_time.strftime('%m-%d %H:%M:%S') if r.finish_time else '',
            'duration': r.duration,
            'message': r.message or '',
            'shyni_job_id': r.shyni_job_id,
            'stat_total': r.stat_total,
            'stat_new': r.stat_new,
            'stat_missing': r.stat_missing,
            'stat_updated': r.stat_updated,
            'stat_restored': r.stat_restored,
            'stat_registered': r.stat_registered,
            'stat_failed': r.stat_failed,
            'vfs_duration': r.vfs_duration,
            'scan_duration': r.scan_duration,
            'append_count': r.append_count,
            'has_append': bool(r.append_files),
        }

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
    def get_page(cls, page=1, per=30, section_key=None, only_append=False, search=None):
        with F.app.app_context():
            query = F.db.session.query(cls)
            if section_key:
                query = query.filter(cls.section_key == str(section_key))
            if only_append:
                query = query.filter(cls.append_count > 0)
            if search:
                needle = str(search).strip()
                if needle:
                    query = query.filter(or_(
                        cls.job_name.contains(needle),
                        cls.section_title.contains(needle),
                        cls.message.contains(needle),
                        cls.append_files.contains(needle)))
            total = query.count()
            pages = max(1, (total + per - 1) // per)
            try:
                page = int(page or 1)
            except Exception:
                page = 1
            page = min(max(1, page), pages)
            rows = (query.order_by(desc(cls.id))
                    .offset((page - 1) * per).limit(per).all())
            out = [cls._row_dict(r) for r in rows]
            return {'list': out, 'count': total, 'page': page, 'pages': pages}

    @classmethod
    def get_detail(cls, item_id):
        """펼침 상세용 — 목록에는 싣지 않는 파일 목록·폴더별 결과를 함께 돌려준다."""
        with F.app.app_context():
            try:
                r = F.db.session.query(cls).filter(cls.id == int(item_id)).first()
            except Exception:
                return None
            if r is None:
                return None
            out = cls._row_dict(r)
            try:
                folders = json.loads(r.stat_folders) if r.stat_folders else []
                if not isinstance(folders, list):
                    folders = []
            except Exception:
                folders = []
            try:
                files = json.loads(r.append_files) if r.append_files else None
                if files is not None and not isinstance(files, list):
                    files = None
            except Exception:
                files = None
            groups = []
            if files:
                try:
                    from . import db_support
                    groups = db_support.group_append_paths(files)
                except Exception:
                    groups = []
            out['stat_folders'] = folders
            out['append_groups'] = groups
            out['append_total'] = len(files) if files is not None else None
            out['append_truncated'] = bool(
                r.append_count is not None and files is not None
                and len(files) < r.append_count)
            return out
