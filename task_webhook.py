# -*- coding: utf-8 -*-
"""shyni 웹훅 중계 — 받은 이벤트를 Discord로 보내는 워커 쪽 처리.

웹 프로세스의 수신 라우트(mod_webhook)는 ModelSetting 큐·버퍼에 쌓기만 하고
바로 응답한다. 실제 Discord 전송은 이 모듈의 Celery 감시 루프가 맡는다
(task_trash_view와 같은 방식 — 웹에서 오래 걸리는 일을 하지 않고,
플러그인이 재시작돼도 큐와 버퍼가 그대로 이어진다).

상태 공유 키:
- webhook_play_queue     : 재생 이벤트 대기열 [{payload, thumb_b64, at}]
- webhook_library_buffer : 신규 항목 버퍼
  {'libs': {라이브러리명: {'groups': {...}, 'thumb_b64'}},
   'last_at': 마지막 수신 시각, 'dropped': 상한으로 버린 건수}
"""
import base64
import json
import threading
import time
from datetime import datetime, timezone

import requests

from .setup import *

logger = P.logger

PLAY_QUEUE_KEY = 'webhook_play_queue'
LIB_BUFFER_KEY = 'webhook_library_buffer'
WATCH_INTERVAL = 2
MAX_PLAY_QUEUE = 100
MAX_LIB_TRACKS = 2000
MAX_THUMB_BYTES = 1024 * 1024

PLAY_EVENTS = ('media.play', 'media.pause', 'media.resume',
               'media.stop', 'media.scrobble')
EVENT_SHORT = {'play': 'media.play', 'pause': 'media.pause',
               'resume': 'media.resume', 'stop': 'media.stop',
               'scrobble': 'media.scrobble'}
# 이모지·문구·색 — 색은 재생 계열(하늘색)·일시정지(노란색)·완료(초록색)·
# 중지(빨간색)로 구분한다 (2026-11-12 사용자 확정). 이모지는 Discord에서
# 그림으로 표시되도록 이모지 표시 선택자(️)를 붙인다
EVENT_STYLE = {
    'media.play': ('🟦 ▶️', '재생했습니다.', 3447003),
    'media.pause': ('🟨 ⏸️', '일시정지했습니다.', 15844367),
    'media.resume': ('🟦 ⏯️', '다시 재생했습니다.', 3447003),
    'media.scrobble': ('🟩 ✅', '재생완료했습니다.', 3066993),
    'media.stop': ('🟥 ⏹️', '중지했습니다.', 15158332),
}
LIBRARY_NEW_COLOR = 10181046

_lock = threading.Lock()


# ---------- 설정 ----------
def _use():
    return P.ModelSetting.get_bool('webhook_use')


def _url():
    return (P.ModelSetting.get('webhook_discord_url') or '').strip()


def _attach_thumb():
    return P.ModelSetting.get_bool('webhook_attach_thumb')


def _play_allowed(event):
    raw = (P.ModelSetting.get('webhook_play_events') or '').strip()
    if not raw:
        return True
    allowed = set()
    for part in raw.split(','):
        part = part.strip().lower()
        if part:
            allowed.add(EVENT_SHORT.get(part, part))
    return event in allowed


# ---------- 상태 저장 ----------
def _read_json(key, default):
    try:
        raw = P.ModelSetting.get(key)
        if raw:
            data = json.loads(raw)
            if isinstance(data, type(default)):
                return data
    except Exception:
        pass
    return default


def _write_json(key, data):
    try:
        P.ModelSetting.set(key, json.dumps(data, ensure_ascii=False))
    except Exception:
        pass


def _thumb_b64(thumb_bytes):
    if not thumb_bytes or not _attach_thumb():
        return None
    if len(thumb_bytes) > MAX_THUMB_BYTES:
        return None
    return base64.b64encode(thumb_bytes).decode('ascii')


def _b64_bytes(b64):
    if not b64:
        return None
    try:
        return base64.b64decode(b64)
    except Exception:
        return None


# ---------- 수신 쪽 (웹 프로세스에서 호출) ----------
def handle_event(payload, thumb_bytes):
    """수신 라우트가 호출한다. 쌓기만 하고, 전송은 워커가 한다."""
    if not isinstance(payload, dict) or not _use():
        return
    event = payload.get('event') or ''
    if event in PLAY_EVENTS:
        enqueue_play(payload, thumb_bytes)
    elif event == 'library.new':
        if P.ModelSetting.get_bool('webhook_library_use'):
            buffer_library_new(payload, thumb_bytes)


def enqueue_play(payload, thumb_bytes):
    with _lock:
        queue = _read_json(PLAY_QUEUE_KEY, [])
        if len(queue) >= MAX_PLAY_QUEUE:
            logger.warning('[Webhook] 재생 알림 대기열이 가득 차 한 건을 건너뜁니다')
            return False
        queue.append({'payload': payload,
                      'thumb_b64': _thumb_b64(thumb_bytes),
                      'at': time.time()})
        _write_json(PLAY_QUEUE_KEY, queue)
    return True


def buffer_library_new(payload, thumb_bytes):
    """신규 항목을 (라이브러리, 아티스트, 앨범) 단위로 버퍼에 쌓는다."""
    meta = payload.get('Metadata') or {}
    lib_name = (meta.get('librarySectionTitle')
                or str(meta.get('librarySectionID') or '') or '라이브러리')
    grand = meta.get('grandparentTitle') or ''
    parent = meta.get('parentTitle') or ''
    group_main = parent or (meta.get('title') or '')
    if not group_main:
        return
    with _lock:
        buf = _read_json(LIB_BUFFER_KEY, {'libs': {}, 'last_at': 0, 'dropped': 0})
        total = sum(g.get('tracks', 0)
                    for lib in buf['libs'].values()
                    for g in lib.get('groups', {}).values())
        buf['last_at'] = time.time()
        if total >= MAX_LIB_TRACKS:
            # 상한을 넘으면 건수만 세어 둔다 (버퍼가 무한히 커지지 않게)
            buf['dropped'] = buf.get('dropped', 0) + 1
            _write_json(LIB_BUFFER_KEY, buf)
            return
        lib = buf['libs'].setdefault(lib_name, {'groups': {}, 'thumb_b64': None})
        key = grand + '\n' + group_main
        group = lib['groups'].setdefault(key, {
            'grand': grand, 'main': group_main,
            'year': meta.get('year'), 'type': meta.get('type') or '',
            'tracks': 0})
        group['tracks'] += 1
        if not group.get('year') and meta.get('year'):
            group['year'] = meta.get('year')
        if lib.get('thumb_b64') is None:
            # 대표 표지는 라이브러리당 첫 한 장만 보관한다
            lib['thumb_b64'] = _thumb_b64(thumb_bytes)
        _write_json(LIB_BUFFER_KEY, buf)


# ---------- Discord 전송 ----------
def send_discord(embed, thumb_bytes=None):
    url = _url()
    if not url:
        return False
    body = {'username': 'Shyni Webhook', 'embeds': [embed]}
    try:
        if thumb_bytes:
            files = {'file': ('thumb.jpg', thumb_bytes, 'image/jpeg')}
            res = requests.post(
                url,
                data={'payload_json': json.dumps(body, ensure_ascii=False)},
                files=files, timeout=10)
        else:
            res = requests.post(url, json=body, timeout=10)
    except Exception as e:
        logger.warning(f'[Webhook] Discord 전송 실패: {e}')
        return False
    if res.status_code in (200, 204):
        return True
    extra = ''
    if res.status_code == 429:
        extra = f" (Retry-After: {res.headers.get('Retry-After')})"
    logger.warning(f'[Webhook] Discord 전송 실패: HTTP {res.status_code}{extra}')
    return False


def _play_method(transcode):
    video = str(transcode.get('videoDecision') or '').lower()
    audio = str(transcode.get('audioDecision') or '').lower()
    if video == 'directplay' and audio == 'directplay':
        return '직접 재생 (Direct Play)'
    if 'copy' in (video, audio):
        return '🔄 직접 스트리밍 (Direct Stream)'
    if not video and not audio:
        return '직접 재생 (Direct Play)'
    return '🔥 트랜스코딩 (Transcoding)'


def _episode_code(meta):
    try:
        return (f"S{int(meta.get('parentIndex')):02d}"
                f"E{int(meta.get('index')):02d}")
    except (TypeError, ValueError):
        return ''


def build_play_embed(payload):
    meta = payload.get('Metadata') or {}
    event = payload.get('event') or ''
    emoji, phrase, color = EVENT_STYLE.get(
        event, ('🔔', '알림이 도착했습니다.', 16711680))
    user = (payload.get('Account') or {}).get('title') or 'Shyni'
    title = meta.get('title') or ''
    grand = meta.get('grandparentTitle') or ''
    mtype = meta.get('type') or ''
    target = f'{grand} - {title}' if grand else title
    text = f'{emoji} **{user}**님이 `{target}`'
    if mtype == 'episode':
        code = _episode_code(meta)
        if code:
            text += f' - {code}'
    text += f' {phrase}'
    embed = {'title': text[:256], 'color': color,
             'timestamp': datetime.now(timezone.utc).isoformat()}
    # 필드는 모바일에서 읽기 좋게 한 줄에 하나씩 쌓는다 (Plex Mate 형식)
    fields = []
    section = meta.get('librarySectionTitle')
    if section:
        fields.append({'name': '라이브러리명',
                       'value': str(section)[:1024], 'inline': True})
    if target:
        fields.append({'name': '제목',
                       'value': target[:1024], 'inline': False})
    if mtype == 'track' and meta.get('parentTitle'):
        fields.append({'name': '앨범',
                       'value': str(meta['parentTitle'])[:1024],
                       'inline': False})
    transcode = payload.get('TranscodeSession')
    if isinstance(transcode, dict) and transcode:
        method = _play_method(transcode)
    else:
        # shyni 웹훅에는 재생 방식이 실려 오지 않는다. Plex Mate와 같이
        # 정보가 없으면 직접 재생으로 표시한다.
        method = '직접 재생 (Direct Play)'
    fields.append({'name': '재생 방식', 'value': method, 'inline': False})
    player = (payload.get('Player') or {}).get('title')
    if player:
        fields.append({'name': '재생 기기',
                       'value': str(player)[:1024], 'inline': False})
    embed['fields'] = fields
    return embed


def drain_play_queue():
    """재생 이벤트 대기열을 비우며 Discord로 보낸다 (워커에서 실행)."""
    with _lock:
        queue = _read_json(PLAY_QUEUE_KEY, [])
        if queue:
            _write_json(PLAY_QUEUE_KEY, [])
    if not queue or not _use() or not _url():
        return
    for item in queue:
        payload = item.get('payload') or {}
        event = payload.get('event') or ''
        if event not in PLAY_EVENTS or not _play_allowed(event):
            continue
        embed = build_play_embed(payload)
        thumb = _b64_bytes(item.get('thumb_b64')) if _attach_thumb() else None
        if thumb:
            embed['image'] = {'url': 'attachment://thumb.jpg'}
        send_discord(embed, thumb)


# ---------- 신규 항목 요약 ----------
def build_library_embed(lib_name, lib):
    groups = list((lib.get('groups') or {}).values())
    total = sum(g.get('tracks', 0) for g in groups)
    types = {g.get('type') for g in groups}
    if types == {'track'}:
        head = f'앨범 {len(groups)}개 · 트랙 {total}개'
    elif types == {'episode'}:
        head = f'시즌 {len(groups)}개 · 에피소드 {total}개'
    elif types == {'movie'}:
        head = f'영화 {total}개'
    else:
        head = f'항목 {total}개'
    lines = [head]
    ordered = sorted(groups,
                     key=lambda g: (g.get('grand') or '', g.get('main') or ''))
    for group in ordered[:10]:
        label = (f"{group['grand']} — {group['main']}" if group.get('grand')
                 else group['main'])
        if group.get('year'):
            label += f" ({group['year']})"
        if group.get('tracks', 0) > 1 and types != {'movie'}:
            unit = '트랙' if types == {'track'} else '개'
            label += f" · {group['tracks']}{unit}"
        lines.append(label)
    if len(groups) > 10:
        lines.append(f'외 {len(groups) - 10}개')
    desc = '\n'.join(lines)
    if len(desc) > 2000:
        desc = desc[:1997] + '...'
    return {'title': f'🟪 ✨ 새 미디어가 추가되었습니다 — {lib_name}'[:256],
            'description': desc, 'color': LIBRARY_NEW_COLOR,
            'timestamp': datetime.now(timezone.utc).isoformat()}


def flush_library_buffer(force=False):
    """마지막 수신 후 설정 간격이 지나면 라이브러리별로 요약을 보낸다.

    전송에 실패한 라이브러리는 버퍼에 그대로 남겨 다음 주기에 다시 보낸다.
    """
    if not _use() or not _url():
        return False
    if not P.ModelSetting.get_bool('webhook_library_use'):
        return False
    buf = _read_json(LIB_BUFFER_KEY, {'libs': {}, 'last_at': 0, 'dropped': 0})
    libs = buf.get('libs') or {}
    if not libs:
        return False
    interval = P.ModelSetting.get_int('webhook_library_interval_min') or 5
    if not force and time.time() - (buf.get('last_at') or 0) < max(1, interval) * 60:
        return False
    sent_any = False
    for lib_name in list(libs.keys()):
        lib = libs[lib_name]
        embed = build_library_embed(lib_name, lib)
        thumb = _b64_bytes(lib.get('thumb_b64')) if _attach_thumb() else None
        if thumb:
            embed['image'] = {'url': 'attachment://thumb.jpg'}
        if not send_discord(embed, thumb):
            continue
        sent_any = True
        # 보낸 만큼만 버퍼에서 뺀다 — 보내는 동안 새로 쌓인 항목은 남는다
        with _lock:
            cur = _read_json(LIB_BUFFER_KEY,
                             {'libs': {}, 'last_at': 0, 'dropped': 0})
            cur_lib = (cur.get('libs') or {}).get(lib_name)
            if cur_lib:
                for key, group in (lib.get('groups') or {}).items():
                    cur_group = cur_lib['groups'].get(key)
                    if cur_group:
                        cur_group['tracks'] -= group.get('tracks', 0)
                        if cur_group['tracks'] <= 0:
                            del cur_lib['groups'][key]
                if not cur_lib['groups']:
                    del cur['libs'][lib_name]
                else:
                    # 대표 표지는 이미 보냈으니 비운다 (새 항목이 오면 다시 채워진다)
                    cur_lib['thumb_b64'] = None
            if not cur.get('libs') and cur.get('dropped'):
                logger.info(f"[Webhook] 버퍼 상한으로 요약에서 제외된 항목 "
                            f"{cur['dropped']}건")
                cur['dropped'] = 0
            _write_json(LIB_BUFFER_KEY, cur)
    if sent_any:
        logger.info('[Webhook] 신규 항목 요약을 Discord로 보냈습니다')
    return sent_any


class TaskWebhook:
    watch_thread = None

    @F.celery.task
    def start():
        if TaskWebhook.watch_thread is None:
            TaskWebhook.watch_thread = threading.Thread(
                target=TaskWebhook.watch_function, args=())
            TaskWebhook.watch_thread.daemon = True
            TaskWebhook.watch_thread.start()
            logger.info('SHYNI WEBHOOK 중계 워커 대기')

    @staticmethod
    def watch_function():
        while True:
            try:
                drain_play_queue()
            except Exception as e:
                logger.error(f'[Webhook] 재생 알림 처리 오류: {e}')
            try:
                flush_library_buffer()
            except Exception as e:
                logger.error(f'[Webhook] 신규 항목 요약 처리 오류: {e}')
            time.sleep(WATCH_INTERVAL)
