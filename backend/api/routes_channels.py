#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import io
import base64

from backend.api import blueprint
from quart import request, jsonify, current_app, send_file
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

from backend.auth import admin_auth_required, streamer_or_admin_required
from backend.channels import read_config_all_channels, add_new_channel, read_config_one_channel, update_channel, \
    delete_channel, add_bulk_channels, queue_background_channel_update_tasks, read_channel_logo, add_channels_from_groups
from backend.utils import normalize_id
from backend.models import Session, ChannelSource
from sqlalchemy import select


@blueprint.route('/tic-api/channels/get', methods=['GET'])
@admin_auth_required
async def api_get_channels():
    channels_config = await read_config_all_channels()
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

def _build_preview_url(app_url, source_url, stream_key, use_hls_proxy=True):
    if not source_url:
        return None, None
    if not use_hls_proxy:
        parsed_direct = urlparse(source_url)
        if parsed_direct.path.lower().endswith('.m3u8'):
            return source_url, "hls"
        if parsed_direct.path.lower().endswith('.ts'):
            return source_url, "mpegts"
        return source_url, "auto"
    if source_url.startswith(app_url) and '/tic-hls-proxy/' in source_url:
        parsed = urlparse(source_url)
        query = parse_qs(parsed.query)
        if 'stream_key' not in query and 'password' not in query:
            query['stream_key'] = [stream_key]
            parsed = parsed._replace(query=urlencode(query, doseq=True))
        url = urlunparse(parsed)
        if url.endswith('.m3u8'):
            return url, "hls"
        if url.endswith('.ts'):
            return url, "mpegts"
        return url, "auto"

    parsed_source = urlparse(source_url)
    is_hls = parsed_source.path.lower().endswith('.m3u8')
    encoded_url = base64.urlsafe_b64encode(source_url.encode('utf-8')).decode('utf-8')
    if is_hls:
        return f'{app_url}/tic-hls-proxy/{encoded_url}.m3u8?stream_key={stream_key}', "hls"
    return f'{app_url}/tic-hls-proxy/stream/{encoded_url}?stream_key={stream_key}', "mpegts"


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
    request_base_url = request.host_url.rstrip('/')
    app_url = settings['settings'].get('app_url') or ''
    preview_base_url = request_base_url or app_url
    if use_tvh_source and source.tvh_uuid:
        preview_url = f"{request_base_url}/tic-tvh/stream/channel/{source.tvh_uuid}?profile=pass&weight=300"
        stream_type = "mpegts"
    else:
        if not preview_base_url:
            preview_base_url = app_url
        is_manual = not source.playlist_id
        use_hls_proxy = True
        if is_manual:
            use_hls_proxy = bool(getattr(source, "use_hls_proxy", False))
        preview_url, stream_type = _build_preview_url(
            preview_base_url,
            source.playlist_stream_url,
            user.streaming_key,
            use_hls_proxy=use_hls_proxy
        )
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
    
    for channel_id in json_data.get('channels', {}):
        normalized = normalize_id(channel_id, "channel")
        await delete_channel(config, normalized)
    
    # Queue background tasks to update TVHeadend
    await queue_background_channel_update_tasks(config)
    
    return jsonify({
        "success": True
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
