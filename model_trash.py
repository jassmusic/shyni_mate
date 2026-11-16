import json

from sqlalchemy import desc, or_

from .setup import *

logger = P.logger


def action_label(action):
    """동작 코드 → 화면 표시 라벨. task_trash.ACTION_LABEL이 원본이다.
    (모듈 수준 import는 순환 참조가 되니 안에서 늦게 가져온다.)
    모르는 코드·없음은 None — 화면에서 줄을 생략한다."""
    if not action:
        return None
    try:
        from .task_trash import ACTION_LABEL
        return ACTION_LABEL.get(action)
    except Exception:
        return None


class ModelTrashItem(ModelBase):
    """휴지통 주기 작업 실행 이력."""
    P = P
    __tablename__ = 'trash_item'
    __table_args__ = {'mysql_collate': 'utf8_general_ci'}
    __bind_key__ = P.package_name

    id = db.Column(db.Integer, primary_key=True)
    created_time = db.Column(db.DateTime)
    mode = db.Column(db.String)            # scheduler | manual
    action = db.Column(db.String)          # refresh | scan | empty | refresh_scan | refresh_scan_empty
    status = db.Column(db.String)          # waiting | running | done | failed | cancelled | terminated
    job_name = db.Column(db.String)
    section_key = db.Column(db.String)
    section_title = db.Column(db.String)
    start_time = db.Column(db.DateTime)
    finish_time = db.Column(db.DateTime)
    duration = db.Column(db.Integer)       # 초
    wait_seconds = db.Column(db.Integer)   # 초 (앞선 작업을 기다린 시간, 기다리지 않았으면 None)
    message = db.Column(db.String)
    # 작업 결과 (z38 — 주기적 스캔 이력과 같은 결과 컬럼 + 비운 개수).
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
    vfs_duration = db.Column(db.Integer)   # 초 (새로고침 단계 소요)
    scan_duration = db.Column(db.Integer)  # 초 (스캔 단계 소요)
    append_files = db.Column(db.Text)      # JSON 배열(추가 파일 경로, 최대 1,000개)
    append_count = db.Column(db.Integer)   # 추가 파일 전체 수(저장 상한을 넘으면 파일 수보다 크다)
    stat_folders = db.Column(db.Text)      # JSON 배열(job stats의 폴더별 results[])
    emptied_count = db.Column(db.Integer)  # 비우기 단계에서 비운 파일 수
    emptied_files = db.Column(db.Text)     # JSON 배열(비우기 직전 휴지통 파일 경로, 최대 1,000개)
    target_files = db.Column(db.Text)      # JSON 배열(실행 전 휴지통 파일 경로, 최대 1,000개)
    restored_files = db.Column(db.Text)    # JSON 배열(스캔으로 복원된 휴지통 파일 경로, 최대 1,000개)
    vfs_results = db.Column(db.Text)       # JSON 객체(폴더 경로 → {'ok': bool, 'error': str})

    def save(self):
        # SQLite 잠금이면 재시도한다 (스캔 항목과 같은 DB를 쓴다)
        from .extensions import save_with_retry
        return save_with_retry(self)

    def __init__(self, job_name, section_key, section_title, mode, action=None):
        self.created_time = datetime.now()
        self.start_time = datetime.now()
        self.mode = mode
        self.action = action
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
        """플러그인 재시작 시 running·waiting으로 남은 이력을 정리한다.
        대기 중이던 행은 대기가 중단됐다는 문구를 남긴다."""
        with F.app.app_context():
            try:
                rows = F.db.session.query(cls).filter(
                    cls.status.in_(['running', 'waiting'])).all()
                for row in rows:
                    was_waiting = row.status == 'waiting'
                    row.status = 'terminated'
                    row.finish_time = datetime.now()
                    if row.start_time:
                        row.duration = int((row.finish_time - row.start_time).total_seconds())
                    if was_waiting:
                        row.message = '다시 시작되면서 대기가 중단됐습니다.'
                    row.save()
            except Exception as e:
                logger.error(f'set_terminated 실패: {e}')

    @classmethod
    def _row_dict(cls, r):
        # 결과 컬럼은 예전 행에서 NULL일 수 있다 — None 그대로 내보내고
        # 화면에서 '-'로 표시한다.
        # 대기 중인 행은 읽는 시점의 대기 경과 초를 함께 내보낸다 —
        # 화면이 초 단위로 올려서 보여주는 기준점이다.
        waiting_seconds = None
        if r.status == 'waiting' and r.start_time:
            try:
                waiting_seconds = max(
                    0, int((datetime.now() - r.start_time).total_seconds()))
            except Exception:
                waiting_seconds = None
        return {
            'id': r.id,
            'mode': r.mode,
            'action': r.action,
            'action_label': action_label(r.action),
            'status': r.status,
            'job_name': r.job_name,
            'section_key': r.section_key,
            'section_title': r.section_title,
            'start_time': r.start_time.strftime('%m-%d %H:%M:%S') if r.start_time else '',
            'finish_time': r.finish_time.strftime('%m-%d %H:%M:%S') if r.finish_time else '',
            'duration': r.duration,
            'wait_seconds': r.wait_seconds,
            'waiting_seconds': waiting_seconds,
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
            'emptied_count': r.emptied_count,
            'has_target_files': bool(r.target_files),
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
                    'action': r.action,
                    'action_label': action_label(r.action),
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
                    # 동작 라벨로도 찾는다 — '비우기'를 치면 비우기·
                    # 새로고침 후 스캔 후 비우기 행이 함께 나온다.
                    codes = []
                    try:
                        from .task_trash import ACTION_LABEL
                        codes = [c for c, label in ACTION_LABEL.items()
                                 if needle in label]
                    except Exception:
                        codes = []
                    conds = [
                        cls.job_name.contains(needle),
                        cls.section_title.contains(needle),
                        cls.message.contains(needle),
                        cls.append_files.contains(needle)]
                    if codes:
                        conds.append(cls.action.in_(codes))
                    query = query.filter(or_(*conds))
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
            # 비운 파일 목록 — 추가 파일과 같은 묶음 방식이다.
            try:
                e_files = json.loads(r.emptied_files) if r.emptied_files else None
                if e_files is not None and not isinstance(e_files, list):
                    e_files = None
            except Exception:
                e_files = None
            e_groups = []
            if e_files:
                try:
                    from . import db_support
                    e_groups = db_support.group_append_paths(e_files)
                except Exception:
                    e_groups = []
            # 대상 파일 목록 — 실행 전 휴지통 파일 경로. 폴더 묶음에
            # 폴더별 VFS 새로고침 결과를 붙인다.
            try:
                t_files = json.loads(r.target_files) if r.target_files else None
                if t_files is not None and not isinstance(t_files, list):
                    t_files = None
            except Exception:
                t_files = None
            try:
                vfs_map = json.loads(r.vfs_results) if r.vfs_results else {}
                if not isinstance(vfs_map, dict):
                    vfs_map = {}
            except Exception:
                vfs_map = {}
            t_groups = []
            if t_files:
                try:
                    from . import db_support
                    t_groups = db_support.group_append_paths(t_files)
                    for grp in t_groups:
                        v = vfs_map.get(grp['folder'])
                        if isinstance(v, dict):
                            grp['vfs_ok'] = bool(v.get('ok'))
                            grp['vfs_error'] = v.get('error') or ''
                except Exception:
                    t_groups = []
            # 복원된 파일 목록 — 스캔 전/후 목록 비교로 가려낸 것.
            try:
                r_files = json.loads(r.restored_files) if r.restored_files else None
                if r_files is not None and not isinstance(r_files, list):
                    r_files = None
            except Exception:
                r_files = None
            r_groups = []
            if r_files:
                try:
                    from . import db_support
                    r_groups = db_support.group_append_paths(r_files)
                except Exception:
                    r_groups = []
            out['stat_folders'] = folders
            out['append_groups'] = groups
            out['append_total'] = len(files) if files is not None else None
            out['append_truncated'] = bool(
                r.append_count is not None and files is not None
                and len(files) < r.append_count)
            out['emptied_groups'] = e_groups
            out['emptied_total'] = len(e_files) if e_files is not None else None
            out['emptied_truncated'] = bool(
                r.emptied_count is not None and e_files is not None
                and len(e_files) < r.emptied_count)
            out['target_groups'] = t_groups
            out['target_total'] = len(t_files) if t_files is not None else None
            out['target_truncated'] = bool(
                r.stat_total is not None and t_files is not None
                and len(t_files) < r.stat_total)
            out['restored_groups'] = r_groups
            out['restored_total'] = len(r_files) if r_files is not None else None
            out['restored_truncated'] = bool(
                r.stat_restored is not None and r_files is not None
                and len(r_files) < r.stat_restored)
            # 실행 중 목록 문구용 — 목록 저장 게이트(db_ready)와 같은
            # 판정을 함께 내려준다. 읽기 실패 시에는 '끝나면 표시됩니다'
            # 쪽을 기본으로 한다(실행 중 상세는 펼칠 때마다 다시 읽는다).
            try:
                from . import db_support
                out['db_ready'] = bool(db_support.db_ready())
            except Exception:
                out['db_ready'] = True
            return out
