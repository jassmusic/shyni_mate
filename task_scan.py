from sqlalchemy import or_

from .extensions import vfs_refresh
from .model_scan import ModelScanItem
from .setup import *
from .shyni_backend import ShyniBackend

name = 'scan'
logger = P.logger


class Task:
    filecheck_thread = None

    @F.celery.task
    def start():
        ModelScanItem.set_status_incompleted_to_ready()
        if Task.filecheck_thread is None:
            Task.filecheck_thread = threading.Thread(target=Task.filecheck_thread_function, args=())
            Task.filecheck_thread.daemon = True
            Task.filecheck_thread.start()
            P.logger.info("SHYNI SCAN 대기")


    @staticmethod
    def running_shyni_count():
        # 동시 스캔 수 게이트가 세는 실행 중 개수.
        # ffprobe 대기 중(probe_assembling)인 job은 shyni가 남은 데이터를 기다리며
        # 실제로는 스캔하지 않는 상태라 세지 않는다 (예외 — 그래야 그 사이에
        # 다음 스캔이 진행된다).
        with F.app.app_context():
            return F.db.session.query(ModelScanItem).filter(
                ModelScanItem.shyni_status == 'RUNNING',
                or_(ModelScanItem.shyni_phase.is_(None),
                    ModelScanItem.shyni_phase != 'probe_assembling')).count()


    @staticmethod
    def assembling_shyni_count():
        with F.app.app_context():
            return F.db.session.query(ModelScanItem).filter(
                ModelScanItem.shyni_status == 'RUNNING',
                ModelScanItem.shyni_phase == 'probe_assembling').count()


    @staticmethod
    def running_probe_groups():
        """실행 중인 항목들의 probe 그룹별 남은 ffprobe 데이터 수 {그룹 id: 남은 수}.

        이미 전송된 데이터 수를 세어, 아직 도착하지 않은 이어지는 ffprobe 데이터이 있는
        그룹만 양수로 남긴다 — 게이트 예외 판정에 쓴다."""
        pending = {}
        sent = {}
        with F.app.app_context():
            items = F.db.session.query(ModelScanItem).filter(
                ModelScanItem.shyni_status == 'RUNNING').all()
            for it in items:
                group = ShyniBackend.probe_group(it)
                if not group:
                    continue
                sent[group['id']] = sent.get(group['id'], 0) + 1
                pending[group['id']] = max(int(group.get('parts') or 1) - sent[group['id']], 0)
        return pending


    @staticmethod
    def filecheck_thread_function():
        while True:
            try:
                items = ModelScanItem.get_list_by_status('READY')
                max_scan_count = max(P.ModelSetting.get_int('scan_max_scan_count'), 1)
                running = Task.running_shyni_count()
                assembling = Task.assembling_shyni_count()
                pending_groups = Task.running_probe_groups()
                for item in items:
                    try:
                        item.filecheck_count += 1
                        group = None
                        continuation = False
                        if item.shyni_status:
                            # 이미 전송된 항목 — poll_running이 job 상태를 갱신하면 확정한다
                            ShyniBackend.finalize(item)
                            continue
                        if item.mode not in ('ADD', 'REFRESH', 'REMOVE_FILE', 'REMOVE_FOLDER'):
                            continue
                        # Shyni 연동 사용이 꺼져 있으면 shyni로만 보내지 않는다 (단, 리셋한 항목은 전송한다).
                        # VFS 새로고침은 그 설정(scan_use_vfs_refresh)을 따로 따른다.
                        hold_send = not P.ModelSetting.get_bool('scan_shyni_use') and not getattr(item, 'reset_done', None)
                        if not hold_send and running >= max_scan_count:
                            # 동시 스캔 수 제한 — 항목별 직렬 처리.
                            # VFS 새로고침 → 스캔 전송 → 결과 확인이 끝나야 다음 항목을 시작한다.
                            # 단, 실행 중인 스캔과 같은 probe 그룹의 이어지는 ffprobe 데이터은 예외로 통과시킨다.
                            # 데이터는 shyni에서 하나의 job으로 합쳐져 동시 스캔이 늘어나는 게 아니고,
                            # 막으면 probe 조립이 마감(30분)까지 멈추는 상호 대기가 된다.
                            if item.mode == 'ADD' and any(v > 0 for v in pending_groups.values()):
                                group = ShyniBackend.probe_group(item)
                            continuation = bool(group) and pending_groups.get(group['id'], 0) > 0
                            if not continuation:
                                continue
                        if item.filecheck_time is None:
                            item.filecheck_time = datetime.now()
                        if item.mode == 'ADD' and P.ModelSetting.get_bool('scan_use_vfs_refresh') and not item.vfs_done:
                            try:
                                vfs_ret = vfs_refresh(item.target, recursive=True)
                                values = (vfs_ret.get('result') or {}).values() if isinstance(vfs_ret, dict) else []
                                if any(str(v).lower() == 'ok' for v in values):
                                    item.vfs_done = True
                                    item.vfs_done_time = datetime.now()
                                    logger.info(f'[Scan] VFS 새로고침 완료: item={item.id} {item.target}')
                            except Exception as e:
                                logger.error(f'vfs_refresh 실패: {item.target} {e}')
                        if hold_send:
                            continue
                        if continuation:
                            logger.info(f'[Scan] probe 그룹 이어지는 ffprobe 데이터 — 게이트 예외 전송: item={item.id} '
                                        f'index={group.get("index")}/{group.get("parts")}')
                        if item.mode == 'REFRESH':
                            ShyniBackend.request_refresh(item)
                        else:
                            ShyniBackend.request_scan(item)
                        if item.shyni_status == 'RUNNING':
                            if group is None and item.mode == 'ADD':
                                group = ShyniBackend.probe_group(item)
                            if group:
                                if continuation:
                                    pending_groups[group['id']] -= 1
                                else:
                                    pending_groups[group['id']] = max(int(group.get('parts') or 1) - 1, 0)
                            # 이어지는 ffprobe 데이터은 기존 job에 합쳐지므로 동시 스캔 수에 세지 않는다
                            if not continuation:
                                running += 1
                                if assembling:
                                    logger.info(f'[Scan] ffprobe 대기 중 예외로 다음 스캔 전송: item={item.id} '
                                                f'(조립 대기 {assembling}건은 동시 스캔 수에서 제외)')
                        ShyniBackend.finalize(item)
                    except Exception as e:
                        logger.error(f"Exception:{str(e)}")
                        logger.error(traceback.format_exc())
                    finally:
                        item.save()
                # 샤이니 job 상태 확정 — 주기당 1회 폴링
                ShyniBackend.poll_running()
            except Exception as e:
                logger.error(f"Exception:{str(e)}")
                logger.error(traceback.format_exc())
            for i in range(P.ModelSetting.get_int(f"{name}_filecheck_thread_interval")):
                time.sleep(1)
