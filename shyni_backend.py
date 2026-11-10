import os
import time

import requests

from .setup import *

logger = P.logger


class ShyniBackend:

    @staticmethod
    def enabled():
        # shyni_mate는 shyni 전용 — 항상 활성
        return True

    @staticmethod
    def get_config():
        # 연결 정보는 기본 설정(base_shyni_*)에서 읽는다.
        url = (P.ModelSetting.get('base_shyni_url') or '').strip().rstrip('/')
        token = (P.ModelSetting.get('base_shyni_token') or '').strip()
        return url, token

    @staticmethod
    def map_path(local_path):
        """shyni_mate가 보는 경로 → 샤이니 서버가 보는 경로 (scan_shyni_path_rule: 'src|dst' 줄단위)."""
        if not local_path:
            return None
        rules = P.ModelSetting.get_list('scan_shyni_path_rule')
        for rule in rules:
            tmps = rule.split('|')
            if len(tmps) != 2:
                continue
            src, dst = tmps[0].strip(), tmps[1].strip()
            if src and local_path.startswith(src):
                return (dst + local_path[len(src):]).replace('\\', '/')
        return local_path.replace('\\', '/')

    @staticmethod
    def _gds_metadata(db_item):
        """수신 원문은 GDS_TOOL JSON에 그대로 보존된다. 마운트 파일은 읽지 않는다."""
        try:
            if (db_item.callback or '') != 'gds_tool' or not db_item.callback_id:
                return {}
            gds = F.PluginManager.get_plugin_instance('gds_tool')
            model = gds.get_module('fp').web_list_model
            fp_item = model.get_by_id(db_item.callback_id.rsplit('_', 1)[-1])
            if fp_item is None or not fp_item.data:
                return {}
            md = (fp_item.data.get('msg') or {}).get('data') or {}
            return md if isinstance(md, dict) else {}
        except Exception as e:
            logger.warning(f'[Shyni] GDS 수신 자료 조회 실패(무시): {type(e).__name__}')
            return {}

    @staticmethod
    def probe_group(db_item):
        """FP 수신 원문에 담긴 probe 그룹 정보({id, index, parts, files})를 돌려준다.

        파일이 많은 앨범은 probe가 여러 항목으로 쪼개져 들어오며, 데이터마다 같은
        그룹 id가 붙는다. 게이트 예외(이어지는 ffprobe 데이터 판정)에 쓰인다. 없으면 None."""
        try:
            probe = ShyniBackend._gds_metadata(db_item).get('scan_probe')
            if isinstance(probe, dict):
                group = probe.get('group')
                if isinstance(group, dict) and group.get('id'):
                    return group
        except Exception:
            pass
        return None

    @staticmethod
    def gather_probe_payload(db_item, shyni_path):
        """폴더별 압축 자료는 해제/경로 재작성 없이 샤이니가 검증하도록 전달한다.

        토큰은 폴더 안 파일명만 식별한다. 실제 범위는 변환된 스캔 path이며,
        여기서 파일 크기 확인이나 ffprobe를 실행하지 않는다.
        """
        md = ShyniBackend._gds_metadata(db_item)
        probe = md.get('scan_probe')
        if (isinstance(probe, dict) and probe.get('v') == 1
                and probe.get('codec') == 'zlib-json'
                and isinstance(probe.get('data'), str)
                and 0 < len(probe['data']) <= 65536):
            return {'scan_probe': probe}
        files = ShyniBackend.gather_ffprobe(db_item, shyni_path, metadata=md)
        return {'files': files} if files else {}

    @staticmethod
    def gather_ffprobe(db_item, shyni_path, metadata=None):
        """기존 파일 한 개짜리 ffprobe_data/ffprobe 메시지와의 호환 경로."""
        try:
            md = metadata if metadata is not None else ShyniBackend._gds_metadata(db_item)
            ffprobe = md.get('ffprobe_data') or md.get('ffprobe')
            if not ffprobe:
                return None
            size = md.get('size') or md.get('file_size') or 0
            if not size:
                try:
                    size = os.path.getsize(db_item.target)
                except Exception:
                    size = 0
            return [{'path': shyni_path, 'size': size, 'ffprobe': ffprobe}]
        except Exception as e:
            logger.error(f'[Shyni] ffprobe 자료 조회 실패(무시): {e}')
            return None

    @staticmethod
    def _section_by_path(url, headers, shyni_path):
        res = requests.get(f'{url}/compat/section_by_path', params={'path': shyni_path},
                           headers=headers, timeout=15)
        if res.status_code == 404:
            return None
        res.raise_for_status()
        return res.json()

    @staticmethod
    def _section_title(url, headers, section_id):
        try:
            res = requests.get(f'{url}/library/sections',
                               headers={**headers, 'Accept': 'application/json'}, timeout=15)
            res.raise_for_status()
            dirs = (res.json().get('MediaContainer') or {}).get('Directory') or []
            for d in dirs:
                if str(d.get('key')) == str(section_id):
                    return d.get('title')
        except Exception:
            pass
        return None

    @staticmethod
    def _record_section(db_item, url, headers, section):
        """전송 시점에 확정된 샤이니 라이브러리 번호·이름을 항목에 기록한다.
        목록의 스캔종료 표시는 이 기록을 기준으로 보여 준다."""
        try:
            section_id = section.get('section_id')
            if section_id is None:
                return
            db_item.section_id = str(section_id)
            if section.get('section_type'):
                db_item.section_type = section['section_type']
            if not db_item.meta_info:
                title = ShyniBackend._section_title(url, headers, section_id)
                if title:
                    db_item.meta_info = [{'name': title, 'summary': ''}]
        except Exception as e:
            logger.warning(f'[Shyni] 섹션 기록 실패(무시): {e}')

    @staticmethod
    def request_scan(db_item):
        """ADD/REMOVE 공통 — 대상 경로가 속한 샤이니 Section을 찾아 부분 스캔 요청.

        FF 호스트의 파일 존재 여부와 무관하게 즉시 보낸다 — 파일이 아직 마운트에 안
        보이는 경우의 refresh·대기는 샤이니 서버가 자체 수행한다(mode=add + wait).
        db_item의 shyni_status/shyni_job_id만 갱신하고 저장은 호출자(finally save)가 한다."""
        try:
            if not ShyniBackend.enabled():
                return
            if db_item.shyni_status:  # READY 재진입 등으로 중복 요청 방지
                return
            url, token = ShyniBackend.get_config()
            if not url or not token:
                db_item.shyni_status = 'SKIP'
                logger.warning(f'[Shyni] 설정 미완료로 건너뜀: item={db_item.id} mode={db_item.mode}')
                return
            headers = {'X-Plex-Token': token}

            if db_item.mode == 'REMOVE_FOLDER':
                scan_mode, target = 'remove', db_item.target
            elif db_item.mode == 'REMOVE_FILE':
                scan_mode = 'remove'
                target = db_item.target.rsplit('/', 1)[0] if db_item.target.startswith('/') \
                    else db_item.target.rsplit('\\', 1)[0]
            else:
                scan_mode, target = 'add', db_item.target
            shyni_path = ShyniBackend.map_path(target)

            section = ShyniBackend._section_by_path(url, headers, shyni_path)
            if section is None:
                db_item.shyni_status = 'NOT_FIND_LIBRARY'
                logger.warning(f'[Shyni] 라이브러리를 찾지 못함: item={db_item.id} mode={db_item.mode} path={shyni_path}')
                return
            section_id = section['section_id']
            ShyniBackend._record_section(db_item, url, headers, section)

            body = {
                'path': shyni_path,
                'mode': scan_mode,
                'wait': max(P.ModelSetting.get_int('scan_max_wait_time'), 1) * 60,
            }
            if scan_mode == 'add':
                body.update(ShyniBackend.gather_probe_payload(db_item, shyni_path))
                # 반영 여부 확인은 리셋된 항목만 한다 — 이전 결과를 모르므로, 보내기 전에
                # shyni에 직접 물어봐서 이미 라이브러리에 있는 항목인지 확인해 둔다.
                # 일반 항목은 확인 없이 바로 보낸다 (확인이 느려 전체 처리가 늦어지는 것을 막기 위함)
                if getattr(db_item, 'reset_done', None):
                    try:
                        # 이 조회는 실제로 수십 초 걸릴 수 있어 제한 시간을 설정으로 받는다
                        probe_timeout = max(P.ModelSetting.get_int('scan_probe_timeout_sec'), 10)
                        probe = requests.get(
                            f'{url}/compat/metadata_by_path',
                            params={'path': ShyniBackend._refresh_path(db_item.target)},
                            headers=headers, timeout=probe_timeout)
                        db_item.in_library = (probe.status_code == 200)
                        logger.info(f'[Shyni] 반영 여부 확인: item={db_item.id} '
                                    f'status={probe.status_code} in_library={db_item.in_library}')
                    except Exception as e:
                        logger.warning(f'[Shyni] 반영 여부 확인 실패(무시): item={db_item.id} {e}')
            res = requests.post(
                f'{url}/library/sections/{section_id}/refresh', json=body,
                headers={**headers, 'Accept': 'application/json',
                         'Idempotency-Key': f'sm-scan-{db_item.id}-{db_item.mode}-{getattr(db_item, "filecheck_count", 0)}'},
                timeout=30,
            )
            res.raise_for_status()
            db_item.process_start_time = datetime.now()
            job_id = res.headers.get('X-Job-Id') or (res.json() or {}).get('job_id')
            db_item.shyni_job_id = str(job_id) if job_id else None
            db_item.shyni_status = 'RUNNING' if job_id else 'REQUESTED'
            logger.info(f'[Shyni] scan 요청: item={db_item.id} mode={db_item.mode} '
                        f'section={section_id} job={job_id} '
                        f'ffprobe={"files" in body or "scan_probe" in body}')
        except Exception as e:
            logger.error(f'[Shyni] scan 요청 실패: item={db_item.id} {e}')
            db_item.shyni_status = 'ERROR'

    # 대상별 실행이 끝났다고 볼 수 있는 상태(F-4 부분 성공 판정용)
    TERMINAL_OK = ('FINISH', 'REQUESTED')
    TERMINAL_FAIL = ('FAILED', 'ERROR', 'NOT_FIND_LIBRARY', 'NOT_FIND_IN_LIBRARY', 'SKIP')

    @staticmethod
    def _apply_album_info(db_item, metadata_id):
        """DB 활용이 가능할 때 앨범 정보를 읽어 meta_info에 넣어 둔다 (목록 표시용)."""
        try:
            from . import db_support
            if not metadata_id or not db_support.db_ready():
                return
            info = db_support.metadata_info(metadata_id)
            if not info or not info.get('title'):
                return
            meta = [dict(m) for m in (db_item.meta_info or [{'name': '', 'summary': ''}])]
            meta[0]['info_kind'] = info.get('kind') or 'album'
            meta[0]['album_title'] = info['title']
            meta[0]['album_year'] = info.get('year')
            if info.get('summary'):
                meta[0]['summary'] = info['summary']
            if info.get('thumb_url'):
                meta[0]['thumb_url'] = info['thumb_url']
            if info.get('kind') == 'artist':
                meta[0]['album_count'] = info.get('album_count')
                meta[0]['track_count'] = info.get('track_count')
            db_item.meta_info = meta
            logger.info(f"[Shyni] 앨범 정보 반영: item={db_item.id} title={info['title']}")
        except Exception as e:
            logger.warning(f'[Shyni] 앨범 정보 반영 실패(무시): item={db_item.id} {e}')

    @staticmethod
    def _enrich_by_path(db_item):
        """ADD 완료 후 경로로 메타데이터를 찾아 앨범 정보를 채운다."""
        try:
            url, token = ShyniBackend.get_config()
            if not url or not token:
                return
            headers = {'X-Plex-Token': token, 'Accept': 'application/json'}
            timeout = max(P.ModelSetting.get_int('scan_probe_timeout_sec'), 10)
            res = requests.get(f'{url}/compat/metadata_by_path',
                               params={'path': ShyniBackend._refresh_path(db_item.target)},
                               headers=headers, timeout=timeout)
            if res.status_code != 200:
                return
            ShyniBackend._apply_album_info(db_item, (res.json() or {}).get('metadata_id'))
        except Exception as e:
            logger.warning(f'[Shyni] 앨범 정보 조회 실패(무시): item={db_item.id} {e}')

    @staticmethod
    def finalize(db_item):
        """샤이니 결과가 확정되면 항목을 FINISH 처리한다.
        set_status가 FINISH_*에서 GDS callback을 1회 발사하는 기존 경로를 그대로 탄다."""
        st = db_item.shyni_status
        if st in ShyniBackend.TERMINAL_OK:
            logger.info(f'[Shyni] 처리 완료: item={db_item.id} mode={db_item.mode} '
                        f'job={db_item.shyni_job_id} 소요={db_item.process_duration or "-"}')
            if db_item.mode == 'ADD':
                ShyniBackend._enrich_by_path(db_item)
            db_item.set_status('FINISH_SHYNI', save=True)
        elif st in ShyniBackend.TERMINAL_FAIL:
            logger.warning(f'[Shyni] 처리 실패: item={db_item.id} mode={db_item.mode} 결과={st}')
            db_item.set_status('FINISH_SHYNI_FAILED', save=True)
        # RUNNING/None → READY 유지, 다음 filecheck 주기에 재판정

    @staticmethod
    def _refresh_path(target):
        # 경로 변환 후 파일명만 벗긴다. 점이 들어간 폴더를 파일로 오인하지 않는다.
        target = ShyniBackend.map_path(target).rstrip('/')
        file_exts = {'.yaml', '.yml', '.nfo', '.mp4', '.mkv', '.avi', '.ts', '.m2ts',
                     '.mov', '.m4v', '.mp3', '.flac', '.m4a', '.aac', '.ogg', '.wav',
                     '.opus', '.wma', '.zip', '.cbz', '.epub', '.pdf', '.txt'}
        if os.path.splitext(target)[1].lower() in file_exts:
            return target.rsplit('/', 1)[0]
        return target

    @staticmethod
    def load_meta_many(item_ids):
        """선택 항목들의 메타 정보를 순서대로 하나씩 불러온다."""
        logger.info(f'[Shyni] 메타 불러오기(선택) 시작: {len(item_ids)}개')
        for iid in item_ids:
            ShyniBackend.load_meta_now(iid)
        logger.info(f'[Shyni] 메타 불러오기(선택) 종료: {len(item_ids)}개')

    @staticmethod
    def meta_refresh_many(item_ids):
        """선택 항목들의 메타 새로고침을 순서대로 하나씩 수행한다."""
        logger.info(f'[Shyni] 메타 새로고침(선택) 시작: {len(item_ids)}개')
        for iid in item_ids:
            ShyniBackend.meta_refresh_now(iid)
        logger.info(f'[Shyni] 메타 새로고침(선택) 종료: {len(item_ids)}개')

    @staticmethod
    def load_meta_now(item_id):
        """목록의 '라이브러리 메타 불러오기' 버튼용: 새로고침 없이 현재 라이브러리 정보를 읽어 채운다."""
        try:
            from .model_scan import ModelScanItem
            db_item = ModelScanItem.get_by_id(item_id)
            if db_item is None:
                return
            ShyniBackend._enrich_by_path(db_item)
            db_item.save()
            meta = (db_item.meta_info or [{}])[0]
            logger.info(f"[Shyni] 메타 불러오기(수동): item={item_id} "
                        f"title={meta.get('album_title')} thumb={'있음' if meta.get('thumb_url') else '없음'}")
        except Exception as e:
            logger.warning(f'[Shyni] 메타 불러오기(수동) 실패: item={item_id} {e}')

    @staticmethod
    def meta_refresh_now(item_id):
        """목록의 '메타 새로고침' 버튼용: 새로고침을 걸고, DB에 반영되면 앨범 정보를 다시 채운다."""
        try:
            from .model_scan import ModelScanItem
            from . import db_support
            db_item = ModelScanItem.get_by_id(item_id)
            if db_item is None:
                return
            url, token = ShyniBackend.get_config()
            if not url or not token:
                logger.warning(f'[Shyni] 메타 새로고침(수동): 연결 설정이 없어 중단: item={item_id}')
                return
            headers = {'X-Plex-Token': token, 'Accept': 'application/json'}
            timeout = max(P.ModelSetting.get_int('scan_probe_timeout_sec'), 10)
            res = requests.get(f'{url}/compat/metadata_by_path',
                               params={'path': ShyniBackend._refresh_path(db_item.target)},
                               headers=headers, timeout=timeout)
            if res.status_code == 404:
                logger.info(f'[Shyni] 메타 새로고침(수동): 라이브러리에서 찾지 못함: item={item_id}')
                return
            res.raise_for_status()
            meta_id = res.json()['metadata_id']
            old = db_support.metadata_info(meta_id) if db_support.db_ready() else None
            res = requests.put(f'{url}/library/metadata/{meta_id}/refresh',
                               headers={**headers,
                                        'Idempotency-Key': f'sm-manual-refresh-{item_id}-{int(time.time())}'},
                               timeout=30)
            res.raise_for_status()
            job_id = res.headers.get('X-Job-Id') or (res.json() or {}).get('job_id')
            logger.info(f'[Shyni] 메타 새로고침(수동) 요청: item={item_id} meta={meta_id} job={job_id}')
            # 샤이니가 새로고침을 DB에 반영할 때까지 기다렸다가 최신 정보로 채운다
            changed = False
            if old is not None:
                keys = ('title', 'year', 'summary', 'thumb_url')
                before = tuple(old.get(k) for k in keys)
                deadline = time.time() + 180
                while time.time() < deadline:
                    time.sleep(10)
                    try:
                        now = db_support.metadata_info(meta_id)
                    except Exception:
                        now = None
                    if now and tuple(now.get(k) for k in keys) != before:
                        changed = True
                        break
            else:
                time.sleep(20)
            ShyniBackend._apply_album_info(db_item, meta_id)
            db_item.save()
            logger.info(f'[Shyni] 메타 새로고침(수동) 정보 반영: item={item_id} 변경 확인={changed}')
        except Exception as e:
            logger.warning(f'[Shyni] 메타 새로고침(수동) 실패: item={item_id} {e}')

    @staticmethod
    def _refresh_metadata(url, headers, db_item, shyni_path):
        """찾은 항목만 refresh. 404는 호출자가 등록 대기/부분 스캔으로 처리한다."""
        res = requests.get(f'{url}/compat/metadata_by_path', params={'path': shyni_path},
                           headers=headers,
                           timeout=max(P.ModelSetting.get_int('scan_probe_timeout_sec'), 10))
        if res.status_code == 404:
            return False
        res.raise_for_status()
        # 리셋된 항목이나 아직 확인되지 않은 새 항목이 여기서 찾아지면 이미 라이브러리에 있던 항목이다
        if getattr(db_item, 'reset_done', None) or getattr(db_item, 'in_library', None) is None:
            db_item.in_library = True
        meta_id = res.json()['metadata_id']
        ShyniBackend._apply_album_info(db_item, meta_id)
        attempt = db_item.shyni_job_id or getattr(db_item, 'filecheck_count', 0)
        res = requests.put(f'{url}/library/metadata/{meta_id}/refresh',
                           headers={**headers, 'Accept': 'application/json',
                                    'Idempotency-Key': f'sm-refresh-{db_item.id}-{attempt}'}, timeout=30)
        res.raise_for_status()
        db_item.process_start_time = datetime.now()
        job_id = res.headers.get('X-Job-Id') or (res.json() or {}).get('job_id')
        db_item.shyni_job_id = str(job_id) if job_id else None
        db_item.shyni_status = 'RUNNING' if job_id else 'FINISH'
        logger.info(f'[Shyni] refresh 요청: item={db_item.id} meta={meta_id} job={job_id}')
        return True

    @staticmethod
    def _scan_before_refresh(url, headers, db_item, shyni_path):
        """YAML이 영상보다 먼저 도착한 경우: 폴더 등록 → metadata refresh 순서 보장.

        기존 job 필드에 단계와 만료 시각을 보존해 재시작 후에도 이어서 처리한다.
        scan 성공만으로 FINISH로 표시하지 않고 반드시 metadata refresh를 수행한다.
        """
        section = ShyniBackend._section_by_path(url, headers, shyni_path)
        if section is None:
            db_item.shyni_status = 'NOT_FIND_LIBRARY'
            logger.warning(f'[Shyni] refresh 등록: 라이브러리를 찾지 못함: item={db_item.id} path={shyni_path}')
            return
        section_id = section['section_id']
        ShyniBackend._record_section(db_item, url, headers, section)
        wait = max(P.ModelSetting.get_int('scan_max_wait_time'), 1) * 60
        attempt = getattr(db_item, 'filecheck_count', 0)
        body = {'path': shyni_path, 'mode': 'add', 'wait': wait}
        body.update(ShyniBackend.gather_probe_payload(db_item, shyni_path))
        res = requests.post(f'{url}/library/sections/{section_id}/refresh',
                            params={'force': 1},
                            json=body,
                            headers={**headers, 'Accept': 'application/json',
                                     'Idempotency-Key': f'sm-refresh-scan-{db_item.id}-{attempt}'},
                            timeout=30)
        res.raise_for_status()
        db_item.process_start_time = datetime.now()
        job_id = res.headers.get('X-Job-Id') or (res.json() or {}).get('job_id')
        db_item.shyni_job_id = f'refresh-scan:{job_id or 0}:{int(time.time()) + wait}'
        db_item.shyni_status = 'RUNNING'
        logger.info(f'[Shyni] refresh 대상 등록 대기: item={db_item.id} '
                    f'section={section_id} scan_job={job_id} wait={wait}s')

    @staticmethod
    def request_refresh(db_item):
        """REFRESH 모드 — 경로의 샤이니 metadata를 찾아 재동기화 요청."""
        try:
            if not ShyniBackend.enabled():
                return
            if db_item.shyni_status:
                return
            url, token = ShyniBackend.get_config()
            if not url or not token:
                db_item.shyni_status = 'SKIP'
                logger.warning(f'[Shyni] 설정 미완료로 건너뜀: item={db_item.id} mode={db_item.mode}')
                return
            headers = {'X-Plex-Token': token}
            shyni_path = ShyniBackend._refresh_path(db_item.target)
            if not ShyniBackend._refresh_metadata(url, headers, db_item, shyni_path):
                ShyniBackend._scan_before_refresh(url, headers, db_item, shyni_path)
        except Exception as e:
            logger.error(f'[Shyni] refresh 요청 실패: item={db_item.id} {e}')
            db_item.shyni_status = 'ERROR'

    @staticmethod
    def _poll_item(url, headers, item):
        job_id = str(item.shyni_job_id or '')
        waiting_refresh = job_id.startswith('refresh-scan:')
        deadline = None
        if waiting_refresh:
            _, job_id, deadline = job_id.split(':', 2)
            deadline = int(deadline)
        if job_id and job_id != '0':
            res = requests.get(f'{url}/compat/jobs/{job_id}', headers=headers, timeout=15)
            if res.status_code == 404:
                item.shyni_status = 'ERROR'
                return
            res.raise_for_status()
            job = res.json()
            # 진행 단계 기록 — 단, stats.phase는 조립 당시 값으로 고정돼 스캔이 시작된
            # 뒤에도 probe_assembling으로 남는다 (실측). 실제 대기와 스캔 중의 구분은
            # job 상태(accepted=미시작, running=스캔 중)로만 가능하므로 함께 본다.
            stats = job.get('stats') or {}
            if job['status'] == 'accepted' and stats.get('phase') == 'probe_assembling':
                item.shyni_phase = 'probe_assembling'
                item.probe_received = stats.get('manifest_files')
                item.probe_expected = stats.get('expected_files')
            elif job['status'] == 'accepted':
                item.shyni_phase = 'queued'
            else:
                item.shyni_phase = None
                item.probe_received = None
                item.probe_expected = None
            if job['status'] == 'failed':
                item.shyni_status = 'FAILED'
                item.shyni_phase = None
                if item.process_finish_time is None:
                    item.process_finish_time = datetime.now()
                logger.warning(f"[Shyni] job 실패: item={item.id} error={job.get('error')}")
                return
            if job['status'] != 'completed':
                return
        elif not waiting_refresh:
            item.shyni_status = 'ERROR'
            return
        if not waiting_refresh:
            item.shyni_status = 'FINISH'
            # 완료 판정: 스캔은 됐지만 신규 등록이 없었으면 '이미 반영됨'으로 구분한다
            # (plex_mate의 FINISH_ADD_ALREADY_IN_DB에 해당 — 상태값은 FINISH_SHYNI 그대로).
            stats = job.get('stats') or {}
            if stats.get('total') is not None:
                item.in_library = (stats.get('new') == 0 and stats.get('total') > 0)
            item.shyni_phase = None
            item.probe_received = None
            item.probe_expected = None
            if item.process_finish_time is None:
                item.process_finish_time = datetime.now()
                if item.process_start_time is not None:
                    item.process_duration = str(
                        item.process_finish_time - item.process_start_time).split('.')[0]
            return
        # 다른 ADD 스캔과 겹쳐 already_running으로 끝난 경우에도 다음 주기에 재조회한다.
        # 대기 중 scan을 반복 발사하거나 섹션 전체를 refresh하지 않는다.
        if ShyniBackend._refresh_metadata(
                url, headers, item, ShyniBackend._refresh_path(item.target)):
            return
        if time.time() >= deadline:
            item.shyni_status = 'NOT_FIND_IN_LIBRARY'
            logger.warning(f'[Shyni] refresh 대상 등록 대기 만료: item={item.id}')

    @staticmethod
    def poll_running():
        """filecheck 주기마다 호출 — RUNNING 항목의 job 상태를 확인해 확정한다."""
        try:
            if not ShyniBackend.enabled():
                return
            from .model_scan import ModelScanItem
            with F.app.app_context():
                items = F.db.session.query(ModelScanItem).filter(
                    ModelScanItem.shyni_status == 'RUNNING').all()
                if not items:
                    return
                url, token = ShyniBackend.get_config()
                if not url or not token:
                    return
                headers = {'X-Plex-Token': token}
                for item in items:
                    try:
                        ShyniBackend._poll_item(url, headers, item)
                    except Exception as e:
                        logger.error(f'[Shyni] job 폴링 실패: item={item.id} {e}')
                F.db.session.commit()
        except Exception as e:
            logger.error(f'[Shyni] poll_running 오류: {e}')
