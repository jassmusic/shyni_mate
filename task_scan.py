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
        with F.app.app_context():
            return F.db.session.query(ModelScanItem).filter(
                ModelScanItem.shyni_status == 'RUNNING').count()


    @staticmethod
    def filecheck_thread_function():
        while True:
            try:
                items = ModelScanItem.get_list_by_status('READY')
                max_scan_count = max(P.ModelSetting.get_int('scan_max_scan_count'), 1)
                running = Task.running_shyni_count()
                for item in items:
                    try:
                        item.filecheck_count += 1
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
                        if item.mode == 'REFRESH':
                            ShyniBackend.request_refresh(item)
                        else:
                            ShyniBackend.request_scan(item)
                        if item.shyni_status == 'RUNNING':
                            running += 1
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
