#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import io
from backend.api import blueprint
from quart import request, jsonify, current_app, send_file
from urllib.parse import urlparse

from backend.auth import admin_auth_required, streamer_or_admin_required
from backend.channels import read_config_all_channels, add_new_channel, read_config_one_channel, update_channel, \
    delete_channel, add_bulk_channels, queue_background_channel_update_tasks, read_channel_logo, add_channels_from_groups
from backend.streaming import build_local_hls_proxy_url, normalize_local_proxy_url, append_stream_key
from backend.utils import normalize_id
from backend.tvheadend.tvh_requests import get_tvh
from backend.models import Session, ChannelSource
from sqlalchemy import select


async def _fetch_tvh_mux_map(config):
    try:
        async with await get_tvh(config) as tvh:
            muxes = await tvh.list_all_muxes()
        return {mux.get("uuid"): mux for mux in muxes if mux.get("uuid")}
    except Exception as exc:
        current_app.logger.warning("Failed to fetch TVH mux list: %s", exc)
        return None


def _is_truthy(value):
    if isinstance(value, str):
        return value.lower() in ("1", "true", "yes", "on")
    return bool(value)


def _build_channel_status(channel, mux_map):
    if not channel.get("enabled"):
        return {
            "state": "disabled",
            "issues": [],
            "disabled_source_count": 0,
            "missing_mux_count": 0,
            "failed_mux_count": 0,
        }

    sources = channel.get("sources") or []
    if not sources:
        return {
            "state": "warning",
            "issues": ["no_sources"],
            "disabled_source_count": 0,
            "missing_mux_count": 0,
            "failed_mux_count": 0,
        }

    disabled_sources = 0
    missing_muxes = 0
    failed_muxes = 0
    has_enabled_source = False

    for source in sources:
        if source.get("source_type") == "manual":
            if source.get("stream_url"):
                has_enabled_source = True
            continue

        playlist_enabled = source.get("playlist_enabled", True)
        if not playlist_enabled:
            disabled_sources += 1
            continue

        has_enabled_source = True
        tvh_uuid = source.get("tvh_uuid")
        if mux_map is None:
            continue
        if not tvh_uuid or tvh_uuid not in mux_map:
            missing_muxes += 1
            continue
        mux_entry = mux_map.get(tvh_uuid) or {}
        if "enabled" in mux_entry and not _is_truthy(mux_entry.get("enabled")):
            failed_muxes += 1
        scan_result = mux_entry.get("scan_result")
        if scan_result == 2:
            failed_muxes += 1

    issues = []
    if not has_enabled_source:
        issues.append("all_sources_disabled")
    if disabled_sources:
        issues.append("has_disabled_sources")
    if missing_muxes:
        issues.append("missing_tvh_mux")
    if failed_muxes:
        issues.append("tvh_mux_failed")

    return {
        "state": "warning" if issues else "ok",
        "issues": issues,
        "disabled_source_count": disabled_sources,
        "missing_mux_count": missing_muxes,
        "failed_mux_count": failed_muxes,
    }


@blueprint.route('/tic-api/channels/get', methods=['GET'])
@admin_auth_required
async def api_get_channels():
    include_status = request.args.get('include_status') == 'true'
    channels_config = await read_config_all_channels(include_status=include_status)
    if include_status:
        config = current_app.config['APP_CONFIG']
        mux_map = await _fetch_tvh_mux_map(config)
        for channel in channels_config:
            channel["status"] = _build_channel_status(channel, mux_map)
    return jsonify(
        {
            "success": True,
            "data":    channels_config
        }
    )


@blueprint.route('/tic-api/channels/basic', methods=['GET'])
@streamer_or_admin_required
async def api_get_channels_basic():
    channels_config = await read_config_all_channels()
    basic = []
    for channel in channels_config:
        basic.append({
            "id": channel.get("id"),
            "name": channel.get("name"),
            "number": channel.get("number"),
            "logo_url": channel.get("logo_url"),
            "guide": channel.get("guide") or {},
        })
    return jsonify({"success": True, "data": basic})


def _infer_stream_type(url):
    parsed = urlparse(url)
    if parsed.path.lower().endswith(".m3u8"):
        return "hls"
    if parsed.path.lower().endswith(".ts"):
        return "mpegts"
    return "auto"


@blueprint.route('/tic-api/channels/<int:channel_id>/preview', methods=['GET'])
@streamer_or_admin_required
async def api_get_channel_preview(channel_id):
    user = getattr(request, "_current_user", None)
    if not user or not user.streaming_key:
        return jsonify({"success": False, "message": "Streaming key missing"}), 400

    async with Session() as session:
        result = await session.execute(
            select(ChannelSource)
            .where(ChannelSource.channel_id == channel_id)
            .order_by(ChannelSource.id.asc())
        )
        source = result.scalars().first()

    if not source or not source.playlist_stream_url:
        return jsonify({"success": False, "message": "Channel has no source URL"}), 404

    config = current_app.config['APP_CONFIG']
    settings = config.read_settings()
    use_tvh_source = settings['settings'].get('route_playlists_through_tvh', False)
    instance_id = config.ensure_instance_id()
    request_base_url = request.host_url.rstrip('/')
    preview_base_url = request_base_url
    if use_tvh_source and source.tvh_uuid:
        preview_url = f"{preview_base_url}/tic-api/tvh_stream/stream/channel/{source.tvh_uuid}?profile=pass&weight=300"
        preview_url = append_stream_key(preview_url, stream_key=user.streaming_key)
        stream_type = "mpegts"
    else:
        is_manual = not source.playlist_id
        use_hls_proxy = bool(getattr(source, "use_hls_proxy", False)) if is_manual else False
        if is_manual and use_hls_proxy:
            preview_url = build_local_hls_proxy_url(
                preview_base_url,
                instance_id,
                source.playlist_stream_url,
                stream_key=user.streaming_key,
            )
        else:
            preview_url = normalize_local_proxy_url(
                source.playlist_stream_url,
                base_url=preview_base_url,
                instance_id=instance_id,
                stream_key=user.streaming_key,
            )
        stream_type = _infer_stream_type(preview_url)
    return jsonify({"success": True, "preview_url": preview_url, "stream_type": stream_type})


@blueprint.route('/tic-api/channels/new', methods=['POST'])
@admin_auth_required
async def api_add_new_channel():
    json_data = await request.get_json()
    config = current_app.config['APP_CONFIG']
    await add_new_channel(config, json_data)
    await queue_background_channel_update_tasks(config)
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/channels/settings/<channel_id>', methods=['GET'])
@admin_auth_required
async def api_get_channel_config(channel_id):
    try:
        channel_id = int(channel_id)
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "Invalid channel id"}), 400
    channel_config = read_config_one_channel(channel_id)
    return jsonify(
        {
            "success": True,
            "data":    channel_config
        }
    )


@blueprint.route('/tic-api/channels/settings/<channel_id>/save', methods=['POST'])
@admin_auth_required
async def api_set_config_channels(channel_id):
    json_data = await request.get_json()
    config = current_app.config['APP_CONFIG']
    try:
        channel_id = int(channel_id)
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "Invalid channel id"}), 400
    await update_channel(config, channel_id, json_data)
    await queue_background_channel_update_tasks(config)
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/channels/settings/multiple/save', methods=['POST'])
@admin_auth_required
async def api_set_config_multiple_channels():
    json_data = await request.get_json()
    config = current_app.config['APP_CONFIG']
    for channel_id in json_data.get('channels', {}):
        channel = json_data['channels'][channel_id]
        normalized = normalize_id(channel_id, "channel")
        await update_channel(config, normalized, channel)
    await queue_background_channel_update_tasks(config)
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/channels/settings/multiple/add', methods=['POST'])
@admin_auth_required
async def api_add_multiple_channels():
    json_data = await request.get_json()
    config = current_app.config['APP_CONFIG']
    await add_bulk_channels(config, json_data.get('channels', []))
    await queue_background_channel_update_tasks(config)
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/channels/settings/multiple/delete', methods=['POST'])
@admin_auth_required
async def api_delete_multiple_channels():
    json_data = await request.get_json()
    config = current_app.config['APP_CONFIG']
    current_app.logger.warning(json_data)

    missing = []
    for channel_id in json_data.get('channels', {}):
        normalized = normalize_id(channel_id, "channel")
        deleted = await delete_channel(config, normalized)
        if not deleted:
            missing.append(normalized)

    # Queue background tasks to update TVHeadend
    await queue_background_channel_update_tasks(config)

    return jsonify({
        "success": True,
        "missing": missing
    })


@blueprint.route('/tic-api/channels/settings/<channel_id>/delete', methods=['DELETE'])
@admin_auth_required
async def api_delete_config_channels(channel_id):
    config = current_app.config['APP_CONFIG']
    try:
        channel_id = normalize_id(channel_id, "channel")
    except ValueError:
        return jsonify({"success": False, "message": "Invalid channel id"}), 400
    await delete_channel(config, channel_id)
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/channels/<channel_id>/logo/<file_placeholder>', methods=['GET'])
async def api_get_channel_logo(channel_id, file_placeholder):
    image_base64_string, mime_type = await read_channel_logo(channel_id)
    # Convert to a BytesIO object for sending file
    image_io = io.BytesIO(image_base64_string)
    image_io.seek(0)
    # Return file blob
    return await send_file(image_io, mimetype=mime_type)


@blueprint.route('/tic-api/channels/settings/groups/add', methods=['POST'])
@admin_auth_required
async def api_add_channels_from_groups():
    json_data = await request.get_json()
    groups = json_data.get('groups', [])

    if not groups:
        return jsonify({
            "success": False,
            "message": "No groups provided"
        }), 400

    config = current_app.config['APP_CONFIG']

    # This function needs to be implemented in the channels module
    # It should add all channels from the specified groups
    added_count = await add_channels_from_groups(config, groups)

    await queue_background_channel_update_tasks(config)

    return jsonify({
        "success": True,
        "data": {
            "added_count": added_count
        }
    })
