#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import os
import aiofiles

from quart import request, jsonify, redirect, send_from_directory, current_app

from backend.api import blueprint

from backend.api.tasks import TaskQueueBroker
from backend.auth import admin_auth_required, get_user_from_token, stream_key_required, audit_stream_event
from backend.config import is_tvh_process_running_locally
from backend.tvheadend.tvh_requests import configure_tvh


@blueprint.route('/')
def index():
    return redirect('/tic-web/')


@blueprint.route('/tic-web/')
@admin_auth_required
async def serve_index():
    response = await send_from_directory(current_app.config['ASSETS_ROOT'], 'index.html')
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@blueprint.route('/tic-web/<path:path>')
@admin_auth_required
async def serve_static(path):
    response = await send_from_directory(current_app.config['ASSETS_ROOT'], path)
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@blueprint.route('/tic-web/epg.xml')
@stream_key_required
async def serve_epg_static():
    await audit_stream_event(request._stream_user, "epg_xml", request.path)
    config = current_app.config['APP_CONFIG']
    return await send_from_directory(os.path.join(config.config_path), 'epg.xml')


async def _build_playlist_with_epg():
    config = current_app.config['APP_CONFIG']
    stream_key = request.args.get('stream_key') or request.args.get('password')
    username = request.args.get('username')
    epg_url = f'{request.url_root.rstrip("/")}/xmltv.php'
    if stream_key:
        if username:
            epg_url = f'{epg_url}?username={username}&password={stream_key}'
        else:
            epg_url = f'{epg_url}?stream_key={stream_key}'
    m3u_path = os.path.join(config.config_path, 'playlist.m3u8')
    async with aiofiles.open(m3u_path, mode='r', encoding='utf8', errors='ignore') as f:
        content = await f.read()
    lines = content.splitlines()
    if lines:
        lines[0] = f'#EXTM3U url-tvg="{epg_url}"'
    return Response("\n".join(lines), mimetype='application/vnd.apple.mpegurl')


@blueprint.route('/tic-web/playlist.m3u8')
@stream_key_required
async def serve_playlist_static():
    await audit_stream_event(request._stream_user, "playlist_m3u8", request.path)
    return await _build_playlist_with_epg()


@blueprint.route('/get.php', methods=['GET'])
@stream_key_required
async def xtreamcodes_get():
    await audit_stream_event(request._stream_user, "xtream_get", request.path)
    return await _build_playlist_with_epg()


@blueprint.route('/xmltv.php', methods=['GET'])
@stream_key_required
async def xtreamcodes_xmltv():
    await audit_stream_event(request._stream_user, "xtream_xmltv", request.path)
    config = current_app.config['APP_CONFIG']
    return await send_from_directory(os.path.join(config.config_path), 'epg.xml')


@blueprint.route('/tic-api/ping')
async def ping():
    # Frontend AIO mixin expects uppercase 'PONG' substring in plain response
    return 'PONG', 200, {'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store'}

# Convenience alias: some clients are probing /tic-tvh/ping (tvheadend http_root); return same pong
@blueprint.route('/tic-tvh/ping')
async def ping_tvh_alias():
    return await ping()


# Fallback redirector for TVHeadend UI paths when nginx reverse proxy is not installed.
# Without nginx, requests to /tic-tvh/... hit the Quart app and 404. We redirect the
# browser to the real TVH port (9981) preserving the sub-path. This does not proxy
# WebSockets (so log streaming etc may be limited) but restores basic UI access.
@blueprint.route('/tic-tvh/')
@admin_auth_required
async def tvh_root_redirect():
    host_only = request.host.split(':')[0]
    target = f'http://{host_only}:9981/tic-tvh/'
    return redirect(target, 302)


@blueprint.route('/tic-tvh/<path:subpath>')
@admin_auth_required
async def tvh_any_redirect(subpath: str):
    # Special case: keep existing ping handler (already defined above)
    if subpath == 'ping':
        return await ping()
    host_only = request.host.split(':')[0]
    target = f'http://{host_only}:9981/tic-tvh/{subpath}'
    return redirect(target, 302)


@blueprint.route('/tic-api/check-auth')
async def api_check_auth():
    config = current_app.config['APP_CONFIG']
    user = await get_user_from_token()
    if user:
        return jsonify(
            {
                "success":     True,
                "runtime_key": config.runtime_key,
                "user": {
                    "id": user.id,
                    "username": user.username,
                    "roles": [role.name for role in user.roles] if user.roles else [],
                    "streaming_key": user.streaming_key,
                },
            }
        ), 200
    return jsonify(
        {
            "success": False,
        }
    ), 401


@blueprint.route('/tic-api/require-auth')
@admin_auth_required
async def api_require_auth():
    return jsonify(
        {
            "success": True,
        }
    ), 200


@blueprint.route('/tic-api/get-background-tasks', methods=['GET'])
@admin_auth_required
async def api_get_background_tasks():
    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.get_pending_tasks()
    return jsonify(
        {
            "success": True,
            "data":    {
                "task_queue_status": await task_broker.get_status(),
                "current_task":      await task_broker.get_currently_running_task(),
                "pending_tasks":     await task_broker.get_pending_tasks(),
            },
        }
    ), 200


@blueprint.route('/tic-api/toggle-pause-background-tasks', methods=['GET'])
@admin_auth_required
async def api_toggle_background_tasks_status():
    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.toggle_status()
    return jsonify(
        {
            "success": True
        }
    ), 200


@blueprint.route('/tic-api/tvh-running', methods=['GET'])
@admin_auth_required
async def api_check_if_tvh_running_status():
    running = await is_tvh_process_running_locally()
    return jsonify(
        {
            "success": True,
            "data":    {
                "running": running
            }
        }
    ), 200


@blueprint.route('/tic-api/save-settings', methods=['POST'])
@admin_auth_required
async def api_save_config():
    json_data = await request.get_json()
    config = current_app.config['APP_CONFIG']

    # Mark first run as complete
    json_data['settings']['first_run'] = False

    # Save the config
    config.update_settings(json_data)
    config.save_settings()

    # Store settings for TVH service
    if json_data.get('settings', {}).get('tvheadend'):
        try:
            await configure_tvh(config)
            pass
        except Exception as e:
            current_app.logger.exception(f"Error while configuring TVH: %s", e)
            return jsonify(
                {
                    "success": False
                }
            ), 400
    return jsonify(
        {
            "success": True
        }
    ), 200


@blueprint.route('/tic-api/get-settings')
@admin_auth_required
async def api_get_config_tvheadend():
    config = current_app.config['APP_CONFIG']
    settings = config.read_settings()
    return_data = settings.get('settings', {})
    return jsonify(
        {
            "success": True,
            "data":    return_data
        }
    ), 200


@blueprint.route('/tic-api/export-config')
@admin_auth_required
async def api_export_config():
    config = current_app.config['APP_CONFIG']
    # Fetch all playlists
    from backend.playlists import read_config_all_playlists
    all_playlist_configs = await read_config_all_playlists(config, output_for_export=True)
    # Fetch all epgs
    from backend.epgs import read_config_all_epgs
    all_epg_configs = await read_config_all_epgs(output_for_export=True)
    # Fetch all channels
    from backend.channels import read_config_all_channels
    channels_config = await read_config_all_channels(output_for_export=True)
    return_data = {
        'playlists': all_playlist_configs,
        'epgs':      all_epg_configs,
        'channels':  channels_config,
    }
    return jsonify(
        {
            "success": True,
            "data":    return_data
        }
    ), 200
