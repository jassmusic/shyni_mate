import os
import time
from datetime import timedelta

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
    def timeout_sec(kind):
        """고급 설정의 시간 제한(초) — query(조회) 15 · send(전송) 30 · poll(작업 확인) 15.

        0 이하이거나 읽기에 실패하면 종류별 기본값을 쓴다. 메타 조회는 이 값이
        아니라 스캔 설정의 scan_probe_timeout_sec(반영 여부 확인 제한 시간)를 쓴다."""
        defaults = {'query': 15, 'send': 30, 'poll': 15}
        try:
            val = P.ModelSetting.get_int(f'base_timeout_{kind}')
        except Exception:
            val = 0
        return val if val and val > 0 else defaults.get(kind, 15)

    @staticmethod
    def _section_by_path(url, headers, shyni_path):
        res = requests.get(f'{url}/compat/section_by_path', params={'path': shyni_path},
                           headers=headers, timeout=ShyniBackend.timeout_sec('query'))
        if res.status_code == 404:
            return None
        res.raise_for_status()
        return res.json()

    @staticmethod
    def _section_title(url, headers, section_id):
        try:
            res = requests.get(f'{url}/library/sections',
                               headers={**headers, 'Accept': 'application/json'}, timeout=ShyniBackend.timeout_sec('query'))
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
    def _next_attempt(db_item):
        """shyni 전송 시도 번호를 올려서 돌려준다 — 멱등 키 재료 전용.

        filecheck_count는 파일체크 횟수(항목당 1 고정)라, 키 재료로 쓰면 재시도·
        리셋 후 재전송의 키가 최초 전송과 겹쳐 shyni가 중복 요청으로 걸러낼 수
        있다. 전송할 때만 오르는 별도 번호를 써서 다시 보내는 전송은 항상 새 키를
        갖게 한다. 리셋·재시도에서도 이 번호는 되돌리지 않는다.
        저장은 호출자(filecheck 주기의 finally save, 폴링의 session commit)가 한다."""
        attempt = (getattr(db_item, 'send_attempt', None) or 0) + 1
        db_item.send_attempt = attempt
        return attempt

    @staticmethod
    def _send_error_reason(step, e):
        """전송 단계 예외를 목록 표시용 짧은 사유로 바꾼다 (어느 단계에서 무슨 일인지)."""
        if isinstance(e, requests.Timeout):
            kind = '시간 초과'
        elif isinstance(e, requests.ConnectionError):
            kind = '연결 실패'
        elif isinstance(e, requests.HTTPError):
            code = None
            try:
                code = e.response.status_code if e.response is not None else None
            except Exception:
                code = None
            kind = f'오류 응답 ({code})' if code else '오류 응답'
        else:
            kind = '오류'
        return f'{step} {kind}'

    @staticmethod
    def _note_send_error(db_item, step, e, tag):
        """전송 단계 오류를 기록한다 — 시간 초과·연결 실패 같은 일시적 오류는 실패로
        확정하지 않고 항목을 준비 상태로 남겨 다음 주기에 다시 보내고, 연속 5회
        실패하면 그때 실패로 확정하고 사유를 남긴다 (2026-11-11 사용자 승인안).
        라이브러리를 못 찾는 경우처럼 다시 보내도 같은 결과인 판정은 이 경로를
        타지 않고 호출자가 바로 확정한다."""
        reason = ShyniBackend._send_error_reason(step, e)
        count = (getattr(db_item, 'send_error_count', None) or 0) + 1
        db_item.send_error_count = count
        if count >= 5:
            db_item.shyni_status = 'ERROR'
            db_item.fail_reason = reason
            logger.error(f'[Shyni] {tag} 실패 {count}회 연속 — 실패 확정: item={db_item.id} {reason} ({e})')
        else:
            logger.warning(f'[Shyni] {tag} 실패 — 다음 주기에 다시 시도 ({count}/5): item={db_item.id} {reason} ({e})')

    @staticmethod
    def request_scan(db_item):
        """ADD/REMOVE 공통 — 대상 경로가 속한 샤이니 Section을 찾아 부분 스캔 요청.

        FF 호스트의 파일 존재 여부와 무관하게 즉시 보낸다 — 파일이 아직 마운트에 안
        보이는 경우의 refresh·대기는 샤이니 서버가 자체 수행한다(mode=add + wait).
        db_item의 shyni_status/shyni_job_id만 갱신하고 저장은 호출자(finally save)가 한다."""
        step = '라이브러리 조회'
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

            step = '스캔 전송'
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
                            params=ShyniBackend._by_path_params(
                                ShyniBackend._refresh_path(db_item.target), section_id),
                            headers=headers, timeout=probe_timeout)
                        db_item.in_library = (probe.status_code == 200)
                        logger.info(f'[Shyni] 반영 여부 확인: item={db_item.id} '
                                    f'status={probe.status_code} in_library={db_item.in_library}')
                    except Exception as e:
                        logger.warning(f'[Shyni] 반영 여부 확인 실패(무시): item={db_item.id} {e}')
            res = requests.post(
                f'{url}/library/sections/{section_id}/refresh', json=body,
                headers={**headers, 'Accept': 'application/json',
                         'Idempotency-Key': f'sm-scan-{db_item.id}-{db_item.mode}-{ShyniBackend._next_attempt(db_item)}'},
                timeout=ShyniBackend.timeout_sec('send'),
            )
            res.raise_for_status()
            db_item.process_start_time = datetime.now()
            job_id = res.headers.get('X-Job-Id') or (res.json() or {}).get('job_id')
            db_item.shyni_job_id = str(job_id) if job_id else None
            db_item.shyni_status = 'RUNNING' if job_id else 'REQUESTED'
            db_item.send_error_count = 0
            db_item.fail_reason = None
            logger.info(f'[Shyni] scan 요청: item={db_item.id} mode={db_item.mode} '
                        f'section={section_id} job={job_id} '
                        f'ffprobe={"files" in body or "scan_probe" in body}')
        except Exception as e:
            ShyniBackend._note_send_error(db_item, step, e, 'scan 요청')

    # 대상별 실행이 끝났다고 볼 수 있는 상태(F-4 부분 성공 판정용)
    TERMINAL_OK = ('FINISH', 'REQUESTED')
    TERMINAL_FAIL = ('FAILED', 'ERROR', 'NOT_FIND_LIBRARY', 'NOT_FIND_IN_LIBRARY', 'SKIP',
                     'ANALYZE_TIMEOUT')

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
    def _mark_meta_checked(db_item, reason):
        """메타 정보 확인 결과를 meta_info[0]에 기록한다 — 목록의 '메타 정보 없음'
        표시와 기존 항목 갱신(백필)이 이미 확인한 항목을 다시 보지 않기 위한 표시.
        reason='subfolder'면 하위 폴더(CD1 등) 대상이라 못 불러온 것으로 구분한다."""
        meta = [dict(m) if isinstance(m, dict) else m for m in (db_item.meta_info or [])]
        if not meta or not isinstance(meta[0], dict):
            meta = [{'name': '', 'summary': ''}]
        meta[0]['meta_checked'] = True
        if reason:
            meta[0]['no_meta_reason'] = reason
        else:
            meta[0].pop('no_meta_reason', None)
        db_item.meta_info = meta

    @staticmethod
    def _section_of(db_item):
        """항목이 속한 섹션(라이브러리) 번호. metadata_by_path에 section을
        함께 넘기면 shyni가 (library_id, file_path) 인덱스로 바로 찾아,
        넘기지 않았을 때의 media_parts 전체 스캔(수십 초)이 사라진다.
        섹션을 모르는 항목은 None을 돌려줘 기존 방식대로 조회한다."""
        for attr in ('section_id', 'target_section_id'):
            v = getattr(db_item, attr, None)
            if v:
                try:
                    return int(v)
                except Exception:
                    pass
        return None

    @staticmethod
    def _by_path_params(path, section):
        params = {'path': path}
        if section:
            params['section'] = section
        return params

    @staticmethod
    def _parent_album_resolves(url, headers, path, timeout, section=None):
        """대상 폴더의 상위 폴더(최대 2단계)에서 앨범(metadata_type=9)이 조회되면 True.
        대상이 CD1 같은 앨범 아래 하위 폴더라, 포함 폴더 경로로는 앨범을 못 찾는
        경우를 구분한다. 상위에서 아티스트 폴더 등이 조회되는 것만으로는 하위 폴더로
        보지 않는다 (앨범 폴더 자체를 못 찾는 경우와 구분)."""
        cur = (path or '').rstrip('/')
        for _ in range(2):
            if '/' not in cur:
                return False
            cur = cur.rsplit('/', 1)[0]
            if not cur:
                return False
            try:
                res = requests.get(f'{url}/compat/metadata_by_path',
                                   params=ShyniBackend._by_path_params(cur, section),
                                   headers=headers,
                                   timeout=min(timeout, 20))
            except Exception:
                return False
            if res.status_code != 200:
                continue
            try:
                return (res.json() or {}).get('metadata_type') == 9
            except Exception:
                return False
        return False

    @staticmethod
    def _enrich_by_path(db_item):
        """ADD 완료 후 경로로 메타데이터를 찾아 앨범 정보를 채운다.
        포함 폴더로 조회가 안 되면 상위 폴더를 확인해 하위 폴더(CD1 등) 대상인지
        구분해 기록한다 — 목록에 '메타 정보 없음'의 이유가 표시된다."""
        try:
            url, token = ShyniBackend.get_config()
            if not url or not token:
                return
            headers = {'X-Plex-Token': token, 'Accept': 'application/json'}
            timeout = max(P.ModelSetting.get_int('scan_probe_timeout_sec'), 10)
            path = ShyniBackend._refresh_path(db_item.target)
            section = ShyniBackend._section_of(db_item)
            res = requests.get(f'{url}/compat/metadata_by_path',
                               params=ShyniBackend._by_path_params(path, section),
                               headers=headers, timeout=timeout)
            if res.status_code == 200:
                ShyniBackend._apply_album_info(db_item, (res.json() or {}).get('metadata_id'))
                ShyniBackend._mark_meta_checked(db_item, None)
                return
            reason = 'subfolder' if ShyniBackend._parent_album_resolves(
                url, headers, path, timeout, section) else None
            ShyniBackend._mark_meta_checked(db_item, reason)
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
                               params=ShyniBackend._by_path_params(
                                   ShyniBackend._refresh_path(db_item.target),
                                   ShyniBackend._section_of(db_item)),
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
                               timeout=ShyniBackend.timeout_sec('send'))
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
        res = requests.get(f'{url}/compat/metadata_by_path',
                           params=ShyniBackend._by_path_params(
                               shyni_path, ShyniBackend._section_of(db_item)),
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
        # 등록 대기 폴링이 다시 부르는 경우는 job_id(등록 회차 식별값)를 그대로 써서
        # 같은 새로고침 재전송이 중복으로 걸리지 않게 하고, 새 전송일 때만 시도 번호를 올린다.
        attempt = db_item.shyni_job_id or ShyniBackend._next_attempt(db_item)
        res = requests.put(f'{url}/library/metadata/{meta_id}/refresh',
                           headers={**headers, 'Accept': 'application/json',
                                    'Idempotency-Key': f'sm-refresh-{db_item.id}-{attempt}'}, timeout=ShyniBackend.timeout_sec('send'))
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
        attempt = ShyniBackend._next_attempt(db_item)
        body = {'path': shyni_path, 'mode': 'add', 'wait': wait}
        body.update(ShyniBackend.gather_probe_payload(db_item, shyni_path))
        res = requests.post(f'{url}/library/sections/{section_id}/refresh',
                            params={'force': 1},
                            json=body,
                            headers={**headers, 'Accept': 'application/json',
                                     'Idempotency-Key': f'sm-refresh-scan-{db_item.id}-{attempt}'},
                            timeout=ShyniBackend.timeout_sec('send'))
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
        step = '항목 조회'
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
                step = '등록 전송'
                ShyniBackend._scan_before_refresh(url, headers, db_item, shyni_path)
            db_item.send_error_count = 0
            db_item.fail_reason = None
        except Exception as e:
            ShyniBackend._note_send_error(db_item, step, e, 'refresh 요청')

    @staticmethod
    def _poll_item(url, headers, item):
        job_id = str(item.shyni_job_id or '')
        waiting_refresh = job_id.startswith('refresh-scan:')
        deadline = None
        if waiting_refresh:
            _, job_id, deadline = job_id.split(':', 2)
            deadline = int(deadline)
        if job_id and job_id != '0':
            res = requests.get(f'{url}/compat/jobs/{job_id}', headers=headers, timeout=ShyniBackend.timeout_sec('poll'))
            if res.status_code == 404:
                item.shyni_status = 'ERROR'
                item.fail_reason = 'job 조회 실패 (job 기록 없음)'
                return
            res.raise_for_status()
            job = res.json()
            # job 시작 시각 기록 — analyze 정체 판정(게이트 제외)의 기준이 된다.
            # compat 응답의 started_at은 UTC 오프셋을 포함한 ISO 문자열이라
            # mate가 쓰는 로컬 naive 시각으로 바꿔 저장한다.
            raw_started = job.get('started_at')
            if raw_started:
                try:
                    started = datetime.fromisoformat(str(raw_started).replace('Z', '+00:00'))
                    if started.tzinfo is not None:
                        started = started.astimezone().replace(tzinfo=None)
                    if item.shyni_started_at != started:
                        item.shyni_started_at = started
                except Exception:
                    pass
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
    def _fail_if_analyze_timeout(item, fail_min=None, now=None):
        """analyze 후 대기 시간(scan_analyze_fail_min)을 넘긴 실행 중 항목을 실패로 확정한다.

        판정 기준은 '분석중' 분류와 같은 계열이다: 실제로 스캔이 시작된 항목
        (shyni_phase가 probe_assembling/queued가 아닌 RUNNING)만 대상으로,
        job 시작(shyni_started_at, 없으면 전송 시각 process_start_time) 후
        설정 분 이상 지났으면 shyni_status를 'ANALYZE_TIMEOUT'으로 바꾼다.
        - probe 조립 대기(probe_assembling)는 shyni가 30분에 마감해 별도 실패로
          끝나므로 대상이 아니다.
        - REFRESH 등록 대기(refresh-scan:)는 analyze 후의 대기가 아니라 제외한다
          (게이트·분석중 판정과 같은 논리).
        표시만 바꾸고 확정을 다음 주기에 미루면, 그 사이 목록에서는 실패로
        보이는데 필터·개수에서는 준비로 잡히는 어긋남이 생긴다 (2026-11-12
        실측). 호출부(poll_running)가 반환값을 보고 그 자리에서 finalize로
        확정까지 끝낸다. shyni job이 나중에 끝나도 폴링 대상에서 빠진 뒤라
        실패로 남는다 (복구는 리셋).
        설정이 0이면 이 기능을 끈다. 실패로 바꾸면 True를 돌려준다."""
        if fail_min is None:
            fail_min = P.ModelSetting.get_int('scan_analyze_fail_min')
        if fail_min <= 0:
            return False
        if item.shyni_status != 'RUNNING':
            return False
        if str(item.shyni_job_id or '').startswith('refresh-scan:'):
            return False
        if item.shyni_phase in ('probe_assembling', 'queued'):
            return False
        started = item.shyni_started_at or item.process_start_time
        if started is None:
            return False
        if now is None:
            now = datetime.now()
        if started > now - timedelta(minutes=fail_min):
            return False
        item.shyni_status = 'ANALYZE_TIMEOUT'
        item.shyni_phase = None
        item.probe_received = None
        item.probe_expected = None
        if item.process_finish_time is None:
            item.process_finish_time = now
            if item.process_start_time is not None:
                item.process_duration = str(
                    item.process_finish_time - item.process_start_time).split('.')[0]
        logger.warning(f'[Shyni] analyze 후 대기 시간({fail_min}분) 초과로 실패 처리: '
                       f'item={item.id} job={item.shyni_job_id} '
                       f'경과={str(now - started).split(".")[0]}')
        return True

    # 직전 폴링 주기에서 연결 장애(회로 차단)가 있었는지 — 복구 후 첫 주기의
    # 실패 확정 유예 판정에 쓴다. 프로세스가 재시작되면 초기화되는데, 그 경우에도
    # 폴링이 job 시작 시각을 먼저 다시 읽어 오므로 판정 기준은 자연히 맞춰진다.
    _poll_outage = False

    @staticmethod
    def poll_running():
        """filecheck 주기마다 호출 — RUNNING 항목의 job 상태를 확인해 확정한다.

        shyni가 재시작 중이면 항목마다 연결 대기가 쌓여 주기 전체가 수십 분으로
        늘어질 수 있어, 연결 오류가 연속 3회 나면 이번 주기 폴링을 즉시 중단하고
        다음 주기에 재개한다(회로 차단). 장애 뒤 복구된 첫 주기에는 analyze 시간
        초과 실패 확정을 한 주기 미룬다 — 재시작으로 재개된 job의 시작 시각이
        폴링으로 다시 맞춰진 뒤부터 판정하기 위해서다 (2026-11-11)."""
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
                grace = ShyniBackend._poll_outage
                if grace:
                    logger.info('[Shyni] 연결 복구 후 첫 폴링 — 이번 주기는 analyze 시간 초과 실패 확정을 미룹니다')
                conn_errors = 0
                tripped = False
                for item in items:
                    try:
                        ShyniBackend._poll_item(url, headers, item)
                        conn_errors = 0
                    except (requests.ConnectionError, requests.Timeout) as e:
                        conn_errors += 1
                        logger.error(f'[Shyni] job 폴링 실패: item={item.id} {e}')
                        if conn_errors >= 3:
                            tripped = True
                            logger.warning('[Shyni] 연결 오류 연속 3회 — 이번 주기 폴링을 중단합니다 '
                                           '(shyni 재시작 중으로 추정, 다음 주기에 재개)')
                            break
                        continue
                    except Exception as e:
                        logger.error(f'[Shyni] job 폴링 실패: item={item.id} {e}')
                        continue
                    # 종료가 확인된 항목은 그 자리에서 확정까지 끝낸다 — 확정을
                    # 다음 filecheck 주기로 미루면, 그 사이 목록 표시(종료)와
                    # 필터·개수(준비)가 어긋나는 구간이 생긴다 (2026-11-12 실측:
                    # 완료 1545·1546, 시간 초과 1507).
                    if item.shyni_status in (ShyniBackend.TERMINAL_OK
                                             + ShyniBackend.TERMINAL_FAIL):
                        ShyniBackend.finalize(item)
                        continue
                    # 폴링으로 job이 끝나지 않은 채 남아 있으면 시간 초과 판정
                    if not grace and ShyniBackend._fail_if_analyze_timeout(item):
                        ShyniBackend.finalize(item)
                ShyniBackend._poll_outage = tripped
                F.db.session.commit()
        except Exception as e:
            logger.error(f'[Shyni] poll_running 오류: {e}')

    @staticmethod
    def poll_late_completions(limit=3):
        """분석시간 초과로 실패 표시한 항목의 shyni job을 뒤늦게 다시 확인한다.

        실패로 확정해도 shyni job은 계속 돌 수 있어, 나중에 끝나면 그 시각을 항목에
        남겨 목록 이력에 스캔완료가 이어서 표시되게 한다 (상태는 실패로 유지하고
        복구는 리셋으로 한다). 실패 후 24시간 동안, 항목당 10분 간격으로,
        주기마다 최대 limit개만 확인해 부하를 만들지 않는다."""
        try:
            if not ShyniBackend.enabled():
                return
            from sqlalchemy import or_
            from .model_scan import ModelScanItem
            with F.app.app_context():
                now = datetime.now()
                items = (F.db.session.query(ModelScanItem)
                         .filter(ModelScanItem.status == 'FINISH_SHYNI_FAILED',
                                 ModelScanItem.shyni_status == 'ANALYZE_TIMEOUT',
                                 ModelScanItem.shyni_job_id.isnot(None),
                                 ModelScanItem.shyni_late_result.is_(None),
                                 ModelScanItem.completed_time >= now - timedelta(hours=24),
                                 or_(ModelScanItem.shyni_late_checked_at.is_(None),
                                     ModelScanItem.shyni_late_checked_at <= now - timedelta(minutes=10)))
                         .order_by(ModelScanItem.completed_time.asc())
                         .limit(limit).all())
                if not items:
                    return
                url, token = ShyniBackend.get_config()
                if not url or not token:
                    return
                headers = {'X-Plex-Token': token}
                for item in items:
                    item.shyni_late_checked_at = now
                    job_id = str(item.shyni_job_id)
                    if ':' in job_id:
                        # refresh-wait:/refresh-scan: 같은 내부 표식은 실제 job이 아니다
                        item.shyni_late_result = 'GONE'
                        continue
                    try:
                        res = requests.get(f'{url}/compat/jobs/{job_id}',
                                           headers=headers, timeout=ShyniBackend.timeout_sec('poll'))
                        if res.status_code == 404:
                            item.shyni_late_result = 'GONE'
                            continue
                        res.raise_for_status()
                        job = res.json() or {}
                    except Exception as e:
                        logger.warning(f'[Shyni] 뒤늦은 완료 확인 실패(다음에 재확인): item={item.id} {e}')
                        continue
                    job_status = job.get('status')
                    if job_status not in ('completed', 'failed'):
                        continue
                    finished = None
                    raw_fin = job.get('finished_at')
                    if raw_fin:
                        try:
                            finished = datetime.fromisoformat(str(raw_fin).replace('Z', '+00:00'))
                            if finished.tzinfo is not None:
                                finished = finished.astimezone().replace(tzinfo=None)
                        except Exception:
                            finished = None
                    item.shyni_late_finished_at = finished or now
                    item.shyni_late_result = 'FINISH' if job_status == 'completed' else 'FAILED'
                    logger.info(f'[Shyni] 분석시간 초과 뒤 shyni job 뒤늦은 종료 확인: '
                                f'item={item.id} job={job_id} 결과={item.shyni_late_result}')
                F.db.session.commit()
        except Exception as e:
            logger.error(f'[Shyni] poll_late_completions 오류: {e}')

    @staticmethod
    def backfill_meta_notes(limit=3):
        """완료됐는데 앨범 정보가 없는 기존 항목을 새 기준으로 다시 확인한다.

        이 기능이 배포되기 전에 끝난 항목들도 '메타 정보 없음'의 이유(하위 폴더
        여부)가 붙도록 파일체크 주기마다 조금씩 나눠 확인한다. 확인한 항목은
        meta_info의 meta_checked로 표시해 다시 보지 않는다."""
        try:
            if not ShyniBackend.enabled():
                return
            from .model_scan import ModelScanItem
            with F.app.app_context():
                pairs = (F.db.session.query(ModelScanItem.id, ModelScanItem.meta_info)
                         .filter(ModelScanItem.status == 'FINISH_SHYNI',
                                 ModelScanItem.mode == 'ADD')
                         .order_by(ModelScanItem.id.asc()).all())
                target_ids = []
                for iid, minfo in pairs:
                    first = {}
                    if isinstance(minfo, list) and minfo and isinstance(minfo[0], dict):
                        first = minfo[0]
                    if first.get('album_title') or first.get('meta_checked'):
                        continue
                    target_ids.append(iid)
                    if len(target_ids) >= limit:
                        break
                if not target_ids:
                    return
                done = 0
                for iid in target_ids:
                    item = F.db.session.query(ModelScanItem).get(iid)
                    if item is None:
                        continue
                    ShyniBackend._enrich_by_path(item)
                    # 조회 자체가 실패해 표시가 안 남은 항목도 이번 확인으로 종결한다
                    # (이유가 확인 안 된 일반 표시로 남는다)
                    meta = item.meta_info or []
                    first = meta[0] if meta and isinstance(meta[0], dict) else {}
                    if not first.get('meta_checked'):
                        ShyniBackend._mark_meta_checked(item, None)
                    done += 1
                if done:
                    F.db.session.commit()
                    logger.info(f'[Shyni] 기존 항목 메타 정보 갱신: {done}개 확인')
        except Exception as e:
            logger.error(f'[Shyni] backfill_meta_notes 오류: {e}')
