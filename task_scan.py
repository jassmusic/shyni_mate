import json
import time

from sqlalchemy import or_

from .extensions import vfs_refresh
from .model_scan import ModelScanItem
from .setup import *
from .shyni_backend import ShyniBackend

name = 'scan'
logger = P.logger


class Task:
    filecheck_thread = None

    # 디스크 양보 임계값 — /proc/pressure/io의 full avg10(%)이 이 값 이상이면
    # 신규 전송을 한 주기 미룬다. FF 웹이 10초 넘게 늦어졌던 사고 때 실측이
    # 60%였고, 평소 스캔 부하에서는 그 아래에 머무는 값으로 잡았다.
    IO_YIELD_THRESHOLD = 40.0

    @staticmethod
    def _io_pressure_full_avg10():
        """디스크 대기 pressure의 full avg10(%) — 읽지 못하면 None.

        컨테이너 안에서 읽어도 호스트 전체 값이다 (pressure는 네임스페이스로
        나뉘지 않는다). full은 모든 스레드가 멈춰 있던 비율이라, 디스크가
        포화해 다른 작업까지 줄 서는 정도를 그대로 보여 준다."""
        try:
            with open('/proc/pressure/io') as f:
                for line in f:
                    if line.startswith('full'):
                        for part in line.split():
                            if part.startswith('avg10='):
                                return float(part.split('=', 1)[1])
        except Exception:
            pass
        return None

    @F.celery.task
    def start():
        ModelScanItem.set_status_incompleted_to_ready()
        if Task.filecheck_thread is None:
            Task.filecheck_thread = threading.Thread(target=Task.filecheck_thread_function, args=())
            Task.filecheck_thread.daemon = True
            Task.filecheck_thread.start()
            P.logger.info("SHYNI SCAN 대기")


    # 직전 호출에서 정체로 제외한 항목 id — 제외 로그를 상태가 바뀔 때만 남기기 위함
    _stall_excluded_ids = None

    @staticmethod
    def running_shyni_count():
        # 동시 스캔 수 게이트가 세는 실행 중 개수.
        # ffprobe 대기 중(probe_assembling)인 job은 shyni가 남은 데이터를 기다리며
        # 실제로는 스캔하지 않는 상태라 세지 않는다 (예외 — 그래야 그 사이에
        # 다음 스캔이 진행된다).
        # analyze 대기 시간(scan_analyze_wait_min)을 넘겨 실행 중인 항목도 세지 않는다 —
        # analyze 단계에서 멈춘 job이 슬롯을 계속 차지하면 큐 전체가 막힌다.
        # 항목 자체의 상태는 바꾸지 않고 게이트 계산에서만 빼며, shyni가 스캔을
        # 끝내면 폴링이 확정해 정상적으로 완료 처리된다.
        # 설정값이 0이면 이 제외를 하지 않는다 (기능 끔).
        # REFRESH 등록 대기(refresh-scan:)는 실제 등록 스캔이 도는 구간을 포함하므로
        # 종전대로 계속 센다.
        wait_min = P.ModelSetting.get_int('scan_analyze_wait_min')
        now = datetime.now()
        with F.app.app_context():
            base = F.db.session.query(ModelScanItem).filter(
                ModelScanItem.shyni_status == 'RUNNING',
                or_(ModelScanItem.shyni_phase.is_(None),
                    ModelScanItem.shyni_phase != 'probe_assembling'))
            if wait_min <= 0:
                return base.count()
            # 제외 판정은 스캔 목록의 '분석중' 분류와 같은 조건을 공유한다
            # (ModelScanItem.analyze_overdue_condition — 기준 시각·REFRESH 예외 포함).
            excluded = [r[0] for r in base.filter(
                ModelScanItem.analyze_overdue_condition(wait_min, now)
            ).with_entities(ModelScanItem.id).all()]
            if excluded != Task._stall_excluded_ids:
                Task._stall_excluded_ids = excluded
                if excluded:
                    logger.info(f'[Scan] analyze 대기 시간({wait_min}분) 초과로 동시 스캔 수에서 제외: items={excluded}')
            return base.count() - len(excluded)


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
    def save_summary_cache():
        """스캔 진행현황 집계를 워커(Celery) 쪽에서 미리 계산해 설정에 캐시한다.

        웹 화면의 진행현황 조회는 이 캐시만 읽는다 — 예전에는 새로고침할 때마다
        웹 프로세스가 분류 쿼리 6개를 직접 돌렸고, 파일체크 쓰기와 겹치면
        DB 연결 대기·잠금으로 FF 화면 전체가 멈추는 일이 있었다 (2026-11-11).
        캐시 시각(ts)도 함께 남겨 화면이 캐시 기준 경과를 표시할 수 있게 한다."""
        try:
            summary = ModelScanItem.summary_counts()
            summary['ts'] = time.time()
            P.ModelSetting.set('scan_summary_cache', json.dumps(summary))
        except Exception as e:
            logger.error(f'[Scan] 진행현황 캐시 저장 실패: {e}')

    @staticmethod
    def filecheck_thread_function():
        while True:
            cycle_started = time.time()
            try:
                Task.save_summary_cache()
                items = ModelScanItem.get_list_by_status('READY')
                max_scan_count = max(P.ModelSetting.get_int('scan_max_scan_count'), 1)
                running = Task.running_shyni_count()
                assembling = Task.assembling_shyni_count()
                pending_groups = Task.running_probe_groups()
                # 디스크 양보 판단은 주기당 한 번만 한다 — 켜져 있을 때만 읽는다
                io_pressure = (Task._io_pressure_full_avg10()
                               if P.ModelSetting.get_bool('scan_io_yield_use') else None)
                io_yield = io_pressure is not None and io_pressure >= Task.IO_YIELD_THRESHOLD
                if io_yield:
                    logger.info(f'[Scan] 디스크 대기가 높아 신규 전송을 한 주기 미룹니다 '
                                f'(io pressure full avg10={io_pressure:.0f}%)')
                for item in items:
                    dirty = False
                    try:
                        group = None
                        continuation = False
                        if item.shyni_status:
                            # 이미 전송된 항목 — poll_running이 job 상태를 갱신하면 확정한다
                            ShyniBackend.finalize(item)
                            continue
                        if item.mode not in ('ADD', 'REFRESH', 'REMOVE_FILE', 'REMOVE_FOLDER'):
                            continue
                        # 보류 항목은 연동을 켜도 자동 처리하지 않는다 — 보류 해제가 있어야
                        # 다시 흐른다 (단, 리셋한 항목은 현행대로 바로 전송한다).
                        if getattr(item, 'held', None) and not getattr(item, 'reset_done', None):
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
                        # 디스크 양보 — 대기가 높으면 신규 전송만 한 주기 미룬다.
                        # 게이트 대기와 마찬가지로 파일체크 횟수는 올리지 않고,
                        # 이미 전송된 항목의 확정과 probe 이어지는 데이터는 막지 않는다.
                        if io_yield and not hold_send and not continuation:
                            continue
                        # 여기서부터 항목이 실제로 바뀌므로 이번 주기 끝에 저장한다.
                        # 게이트·보류로 건너뛴 항목까지 매주기 전부 저장하면 쓰기
                        # 잠금이 상시 걸려 웹 화면 조회가 함께 멈춘다 (2026-11-11).
                        dirty = True
                        # 여기까지 통과한 항목만 이번 주기에 실제로 파일을 확인한다.
                        # 파일체크는 항목당 한 번만 한다 (사용자 확정 규칙): 보류·게이트
                        # 대기로 건너뛴 주기는 세지 않고, 이미 한 번 체크한 항목은 어떤
                        # 이유로 다시 진입해도 횟수를 올리지 않는다 — filecheck_count는
                        # 1에서 멈춘다. 전송된 항목의 job 확정 확인(probe 조립 대기의
                        # 데이터 수신 확인 포함)은 poll_running이 매 주기 그대로 수행한다.
                        if not item.filecheck_count:
                            item.filecheck_count = 1
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
                            # 연동이 꺼진 동안 쌓인 항목은 보류로 확정한다 — 나중에 켜도
                            # 자동 처리되지 않고, 보류 해제를 해야 처리가 시작된다.
                            if not getattr(item, 'held', None):
                                item.held = True
                                logger.info(f'[Scan] 보류 확정 (Shyni 연동 꺼짐): item={item.id} {item.target}')
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
                        if dirty:
                            item.save()
                # 전송까지 끝난 시점의 숫자를 먼저 캐시에 남긴다 — 폴링이 긴
                # 주기에도 진행현황 캐시가 이 시점보다 오래되지 않게 하기 위함
                Task.save_summary_cache()
                # 샤이니 job 상태 확정 — 주기당 1회 폴링
                ShyniBackend.poll_running()
                Task.save_summary_cache()
                # 분석시간 초과로 실패 표시한 항목의 뒤늦은 종료 확인 (주기당 소수만)
                ShyniBackend.poll_late_completions()
                # 완료됐는데 앨범 정보가 없는 기존 항목의 메타 정보 갱신 (주기당 소수만)
                ShyniBackend.backfill_meta_notes()
                Task.save_summary_cache()
                cycle_sec = time.time() - cycle_started
                if cycle_sec > 90:
                    # 주기가 비정상적으로 길면 다음 정체 때 위치를 가늠할 수 있게 남긴다
                    logger.warning(f'[Scan] 파일체크 주기 장기 실행: {cycle_sec:.0f}초')
            except Exception as e:
                logger.error(f"Exception:{str(e)}")
                logger.error(traceback.format_exc())
            for i in range(P.ModelSetting.get_int(f"{name}_filecheck_thread_interval")):
                time.sleep(1)
