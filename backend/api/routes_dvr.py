#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import aiohttp
from sqlalchemy import select

from quart import request, jsonify, current_app

from backend.api import blueprint
from backend.auth import streamer_or_admin_required
from backend.api.tasks import TaskQueueBroker, reconcile_dvr_recordings, apply_dvr_rules
from backend.dvr_profiles import get_profile_key_or_default, read_recording_profiles_from_settings
from backend.dvr import (
    list_recordings,
    list_rules,
    create_recording,
    cancel_recording,
    delete_recording,
    create_rule,
    delete_rule,
    update_rule,
)
from backend.models import Session, Recording, RecordingRule


def _is_admin(user) -> bool:
    roles = [role.name for role in (getattr(user, "roles", None) or [])]
    return "admin" in roles


def _dvr_access_mode(user) -> str:
    return str(getattr(user, "dvr_access_mode", "none") or "none").strip().lower()


def _can_use_dvr(user) -> bool:
    if _is_admin(user):
        return True
    return _dvr_access_mode(user) in {"read_write_own", "read_all_write_own"}


def _can_read_all_dvr(user) -> bool:
    if _is_admin(user):
        return True
    return _dvr_access_mode(user) == "read_all_write_own"


async def _get_recording(recording_id: int):
    async with Session() as session:
        result = await session.execute(select(Recording).where(Recording.id == recording_id))
        return result.scalar_one_or_none()


async def _get_rule(rule_id: int):
    async with Session() as session:
        result = await session.execute(select(RecordingRule).where(RecordingRule.id == rule_id))
        return result.scalar_one_or_none()


@blueprint.route('/tic-api/recordings', methods=['GET'])
@streamer_or_admin_required
async def api_list_recordings():
    user = getattr(request, "_current_user", None)
    if user and not _can_use_dvr(user):
        return jsonify({"success": False, "message": "DVR access is disabled for this user"}), 403
    records = await list_recordings()
    if user and not _can_read_all_dvr(user):
        records = [item for item in records if item.get("owner_user_id") == user.id]
    return jsonify({"success": True, "data": records})


@blueprint.route('/tic-api/recording-profiles', methods=['GET'])
@streamer_or_admin_required
async def api_list_recording_profiles():
    settings = current_app.config["APP_CONFIG"].read_settings()
    profiles = read_recording_profiles_from_settings(settings)
    return jsonify({"success": True, "data": profiles})


async def _get_tvh_proxy_base():
    config = current_app.config['APP_CONFIG']
    tvh_settings = await config.tvh_connection_settings()
    username = tvh_settings.get("tvh_username")
    password = tvh_settings.get("tvh_password")
    if not username or not password:
        return None, None, None
    host = tvh_settings["tvh_host"]
    port = tvh_settings["tvh_port"]
    path = tvh_settings["tvh_path"].rstrip("/")
    base_url = f"http://{host}:{port}{path}"
    return base_url, username, password


async def _fetch_tvh_dvr_entry(base_url, username, password, tvh_uuid):
    url = f"{base_url}/api/dvr/entry/grid"
    payload = {
        "limit": 2000,
        "sort": "start",
        "dir": "ASC",
    }
    timeout = aiohttp.ClientTimeout(total=10)
    session = aiohttp.ClientSession(timeout=timeout, auth=aiohttp.BasicAuth(username, password))
    try:
        async with session.post(url, data=payload, allow_redirects=False) as resp:
            if resp.status != 200:
                return None
            data = await resp.json(content_type=None)
            entries = data.get("entries", [])
            for entry in entries:
                if entry.get("uuid") == tvh_uuid:
                    return entry
    except aiohttp.ClientError:
        return None
    finally:
        await session.close()
    return None


@blueprint.route('/tic-api/recordings/poll', methods=['GET'])
@streamer_or_admin_required
async def api_poll_recordings():
    user = getattr(request, "_current_user", None)
    if user and not _can_use_dvr(user):
        return jsonify({"success": False, "message": "DVR access is disabled for this user"}), 403
    wait = request.args.get('wait', '0')
    timeout = request.args.get('timeout', '0')
    try:
        wait = int(wait)
    except ValueError:
        wait = 0
    try:
        timeout = int(timeout)
    except ValueError:
        timeout = 0

    data = await list_recordings()
    if user and not _can_read_all_dvr(user):
        data = [item for item in data if item.get("owner_user_id") == user.id]
    if wait and timeout:
        start = asyncio.get_event_loop().time()
        while True:
            await asyncio.sleep(1)
            updated = await list_recordings()
            if user and not _can_read_all_dvr(user):
                updated = [item for item in updated if item.get("owner_user_id") == user.id]
            if updated != data:
                data = updated
                break
            if (asyncio.get_event_loop().time() - start) >= timeout:
                break
    return jsonify({"success": True, "data": data}), 200


@blueprint.route('/tic-api/recordings/<int:recording_id>/stream', methods=['GET', 'HEAD'])
@streamer_or_admin_required
async def api_stream_recording(recording_id):
    user = getattr(request, "_current_user", None)
    if user and not _can_use_dvr(user):
        return jsonify({"success": False, "message": "DVR access is disabled for this user"}), 403

    recording = await _get_recording(recording_id)
    if user and not _can_read_all_dvr(user) and recording and recording.owner_user_id != user.id:
        return jsonify({"success": False, "message": "Forbidden"}), 403
    if not recording or not recording.tvh_uuid:
        return jsonify({"success": False, "message": "Recording not found"}), 404

    base_url, username, password = await _get_tvh_proxy_base()
    if not base_url:
        return jsonify({"success": False, "message": "TVHeadend credentials not configured"}), 502

    target = f"{base_url}/dvrfile/{recording.tvh_uuid}"
    headers = dict(request.headers)
    headers.pop("Host", None)
    headers.pop("Authorization", None)
    timeout = aiohttp.ClientTimeout(total=0)
    session = aiohttp.ClientSession(timeout=timeout, auto_decompress=False)
    try:
        resp = await session.request(
            request.method,
            target,
            headers=headers,
            auth=aiohttp.BasicAuth(username, password),
            allow_redirects=True,
        )
    except aiohttp.ClientError as exc:
        await session.close()
        current_app.logger.warning("DVR proxy error: %s", exc)
        return jsonify({"success": False, "message": "TVH DVR proxy failed"}), 502

    response_headers = {k: v for k, v in resp.headers.items() if k.lower() != "transfer-encoding"}
    response_headers.pop("Content-Length", None)

    async def stream_body():
        try:
            async for chunk in resp.content.iter_chunked(8192):
                yield chunk
        except aiohttp.ClientConnectionError:
            return
        finally:
            resp.release()
            await session.close()

    return current_app.response_class(stream_body(), status=resp.status, headers=response_headers)


@blueprint.route('/tic-api/recordings/<int:recording_id>/hls.m3u8', methods=['GET', 'HEAD'])
@streamer_or_admin_required
async def api_stream_recording_hls(recording_id):
    user = getattr(request, "_current_user", None)
    if user and not _can_use_dvr(user):
        return jsonify({"success": False, "message": "DVR access is disabled for this user"}), 403

    recording = await _get_recording(recording_id)
    if user and not _can_read_all_dvr(user) and recording and recording.owner_user_id != user.id:
        return jsonify({"success": False, "message": "Forbidden"}), 403
    if not recording or not recording.tvh_uuid:
        return jsonify({"success": False, "message": "Recording not found"}), 404

    duration = 0
    if recording.start_ts and recording.stop_ts:
        duration = max(int(recording.stop_ts - recording.start_ts), 1)
    if not duration:
        duration = 1

    base_url, username, password = await _get_tvh_proxy_base()
    if not base_url:
        return jsonify({"success": False, "message": "TVHeadend credentials not configured"}), 502

    tvh_entry = await _fetch_tvh_dvr_entry(base_url, username, password, recording.tvh_uuid)
    if tvh_entry:
        entry_duration = tvh_entry.get("duration")
        if entry_duration:
            duration = max(int(entry_duration), duration)
        else:
            start_real = tvh_entry.get("start_real")
            stop_real = tvh_entry.get("stop_real")
            if start_real and stop_real and stop_real > start_real:
                duration = max(int(stop_real - start_real), duration)

    target = f"{base_url}/dvrfile/{recording.tvh_uuid}"
    headers = dict(request.headers)
    headers.pop("Host", None)
    headers.pop("Authorization", None)

    filesize = None
    timeout = aiohttp.ClientTimeout(total=10)
    session = aiohttp.ClientSession(timeout=timeout)
    try:
        head = await session.request(
            "HEAD",
            target,
            headers=headers,
            auth=aiohttp.BasicAuth(username, password),
            allow_redirects=True,
        )
        if head.status == 200:
            size_header = head.headers.get("Content-Length")
            if size_header and size_header.isdigit():
                filesize = int(size_header)
        await head.release()

        if filesize is None:
            rng = await session.request(
                "GET",
                target,
                headers={**headers, "Range": "bytes=0-0"},
                auth=aiohttp.BasicAuth(username, password),
                allow_redirects=True,
            )
            if rng.status in (200, 206):
                content_range = rng.headers.get("Content-Range")
                if content_range and "/" in content_range:
                    try:
                        filesize = int(content_range.split("/")[-1])
                    except ValueError:
                        filesize = None
            await rng.release()
    finally:
        await session.close()

    segment_url = f"/tic-api/recordings/{recording_id}/stream"
    if not filesize:
        playlist = "\n".join(
            [
                "#EXTM3U",
                "#EXT-X-VERSION:3",
                "#EXT-X-PLAYLIST-TYPE:VOD",
                f"#EXT-X-TARGETDURATION:{duration}",
                "#EXT-X-MEDIA-SEQUENCE:0",
                f"#EXTINF:{duration},",
                segment_url,
                "#EXT-X-ENDLIST",
                "",
            ]
        )
    else:
        segment_seconds = 10
        if duration < segment_seconds:
            segment_seconds = max(duration, 1)
        bytes_per_second = max(int(filesize / max(duration, 1)), 1)
        segment_size = max(int(bytes_per_second * segment_seconds), 1)

        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            "#EXT-X-PLAYLIST-TYPE:VOD",
            f"#EXT-X-TARGETDURATION:{segment_seconds}",
            "#EXT-X-MEDIA-SEQUENCE:0",
        ]

        offset = 0
        index = 0
        while offset < filesize:
            remaining = filesize - offset
            length = min(segment_size, remaining)
            remaining_seconds = duration - (segment_seconds * index)
            extinf = segment_seconds if remaining_seconds > segment_seconds else max(remaining_seconds, 1)
            lines.append(f"#EXTINF:{extinf},")
            lines.append(f"#EXT-X-BYTERANGE:{length}@{offset}")
            lines.append(segment_url)
            offset += length
            index += 1

        lines.append("#EXT-X-ENDLIST")
        lines.append("")
        playlist = "\n".join(lines)

    headers = {"Content-Type": "application/vnd.apple.mpegurl"}
    return current_app.response_class(playlist, status=200, headers=headers)


@blueprint.route('/tic-api/recordings', methods=['POST'])
@streamer_or_admin_required
async def api_create_recording():
    user = getattr(request, "_current_user", None)
    if user and not _can_use_dvr(user):
        return jsonify({"success": False, "message": "DVR access is disabled for this user"}), 403
    payload = await request.get_json()
    channel_id = payload.get("channel_id")
    title = payload.get("title")
    start_ts = payload.get("start_ts")
    stop_ts = payload.get("stop_ts")
    description = payload.get("description")
    epg_programme_id = payload.get("epg_programme_id")
    settings = current_app.config["APP_CONFIG"].read_settings()
    profiles = read_recording_profiles_from_settings(settings)
    profile_key = get_profile_key_or_default(payload.get("recording_profile_key"), profiles)

    if not channel_id or not start_ts or not stop_ts:
        return jsonify({"success": False, "message": "Missing channel_id/start_ts/stop_ts"}), 400

    recording_id = await create_recording(
        channel_id=channel_id,
        title=title,
        start_ts=int(start_ts),
        stop_ts=int(stop_ts),
        description=description,
        epg_programme_id=epg_programme_id,
        owner_user_id=getattr(user, "id", None),
        recording_profile_key=profile_key,
    )

    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task({
        'name': f'Reconcile DVR recordings',
        'function': reconcile_dvr_recordings,
        'args': [current_app],
    }, priority=20)

    return jsonify({"success": True, "recording_id": recording_id})


@blueprint.route('/tic-api/recordings/<int:recording_id>/cancel', methods=['POST'])
@streamer_or_admin_required
async def api_cancel_recording(recording_id):
    user = getattr(request, "_current_user", None)
    if user and not _can_use_dvr(user):
        return jsonify({"success": False, "message": "DVR access is disabled for this user"}), 403
    recording = await _get_recording(recording_id)
    if not recording:
        return jsonify({"success": False, "message": "Recording not found"}), 404
    if user and not _is_admin(user) and recording.owner_user_id != user.id:
        return jsonify({"success": False, "message": "Forbidden"}), 403
    ok = await cancel_recording(recording_id)
    if not ok:
        return jsonify({"success": False, "message": "Recording not found"}), 404
    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task({
        'name': f'Reconcile DVR recordings',
        'function': reconcile_dvr_recordings,
        'args': [current_app],
    }, priority=20)
    return jsonify({"success": True})


@blueprint.route('/tic-api/recordings/<int:recording_id>', methods=['DELETE'])
@streamer_or_admin_required
async def api_delete_recording(recording_id):
    user = getattr(request, "_current_user", None)
    if user and not _can_use_dvr(user):
        return jsonify({"success": False, "message": "DVR access is disabled for this user"}), 403
    recording = await _get_recording(recording_id)
    if not recording:
        return jsonify({"success": False, "message": "Recording not found"}), 404
    if user and not _is_admin(user) and recording.owner_user_id != user.id:
        return jsonify({"success": False, "message": "Forbidden"}), 403
    ok = await delete_recording(recording_id)
    if not ok:
        return jsonify({"success": False, "message": "Recording not found"}), 404
    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task({
        'name': f'Reconcile DVR recordings',
        'function': reconcile_dvr_recordings,
        'args': [current_app],
    }, priority=20)
    return jsonify({"success": True})


@blueprint.route('/tic-api/recording-rules', methods=['GET'])
@streamer_or_admin_required
async def api_list_recording_rules():
    user = getattr(request, "_current_user", None)
    if user and not _can_use_dvr(user):
        return jsonify({"success": False, "message": "DVR access is disabled for this user"}), 403
    rules = await list_rules()
    if user and not _can_read_all_dvr(user):
        rules = [item for item in rules if item.get("owner_user_id") == user.id]
    return jsonify({"success": True, "data": rules})


@blueprint.route('/tic-api/recording-rules', methods=['POST'])
@streamer_or_admin_required
async def api_create_recording_rule():
    user = getattr(request, "_current_user", None)
    if user and not _can_use_dvr(user):
        return jsonify({"success": False, "message": "DVR access is disabled for this user"}), 403
    payload = await request.get_json()
    channel_id = payload.get("channel_id")
    title_match = payload.get("title_match")
    lookahead_days = payload.get("lookahead_days", 7)
    settings = current_app.config["APP_CONFIG"].read_settings()
    profiles = read_recording_profiles_from_settings(settings)
    profile_key = get_profile_key_or_default(payload.get("recording_profile_key"), profiles)
    if not channel_id or not title_match:
        return jsonify({"success": False, "message": "Missing channel_id/title_match"}), 400
    rule_id = await create_rule(
        channel_id,
        title_match,
        lookahead_days=int(lookahead_days),
        owner_user_id=getattr(user, "id", None),
        recording_profile_key=profile_key,
    )
    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task({
        'name': f'Applying DVR recording rules',
        'function': apply_dvr_rules,
        'args': [current_app],
    }, priority=19)
    await task_broker.add_task({
        'name': f'Reconcile DVR recordings',
        'function': reconcile_dvr_recordings,
        'args': [current_app],
    }, priority=20)
    return jsonify({"success": True, "rule_id": rule_id})


@blueprint.route('/tic-api/recording-rules/<int:rule_id>', methods=['DELETE'])
@streamer_or_admin_required
async def api_delete_recording_rule(rule_id):
    user = getattr(request, "_current_user", None)
    if user and not _can_use_dvr(user):
        return jsonify({"success": False, "message": "DVR access is disabled for this user"}), 403
    rule = await _get_rule(rule_id)
    if not rule:
        return jsonify({"success": False, "message": "Rule not found"}), 404
    if user and not _is_admin(user) and rule.owner_user_id != user.id:
        return jsonify({"success": False, "message": "Forbidden"}), 403
    ok = await delete_rule(rule_id)
    if not ok:
        return jsonify({"success": False, "message": "Rule not found"}), 404
    return jsonify({"success": True})


@blueprint.route('/tic-api/recording-rules/<int:rule_id>', methods=['PUT'])
@streamer_or_admin_required
async def api_update_recording_rule(rule_id):
    user = getattr(request, "_current_user", None)
    if user and not _can_use_dvr(user):
        return jsonify({"success": False, "message": "DVR access is disabled for this user"}), 403
    rule = await _get_rule(rule_id)
    if not rule:
        return jsonify({"success": False, "message": "Rule not found"}), 404
    if user and not _is_admin(user) and rule.owner_user_id != user.id:
        return jsonify({"success": False, "message": "Forbidden"}), 403
    payload = await request.get_json()
    channel_id = payload.get("channel_id")
    title_match = payload.get("title_match")
    lookahead_days = payload.get("lookahead_days")
    enabled = payload.get("enabled")
    profile_key = None
    if "recording_profile_key" in payload:
        settings = current_app.config["APP_CONFIG"].read_settings()
        profiles = read_recording_profiles_from_settings(settings)
        profile_key = get_profile_key_or_default(payload.get("recording_profile_key"), profiles)
    ok = await update_rule(
        rule_id,
        channel_id=channel_id,
        title_match=title_match,
        lookahead_days=lookahead_days,
        enabled=enabled,
        recording_profile_key=profile_key,
    )
    if not ok:
        return jsonify({"success": False, "message": "Rule not found"}), 404
    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task({
        'name': f'Applying DVR recording rules',
        'function': apply_dvr_rules,
        'args': [current_app],
    }, priority=19)
    await task_broker.add_task({
        'name': f'Reconcile DVR recordings',
        'function': reconcile_dvr_recordings,
        'args': [current_app],
    }, priority=20)
    return jsonify({"success": True})
