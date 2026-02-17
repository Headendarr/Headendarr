#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import os
import aiohttp

from quart import request, jsonify, send_from_directory, current_app, Response, websocket, redirect

from backend.api import blueprint

from backend.api.tasks import TaskQueueBroker
from backend.api.tasks import sync_all_users_to_tvh
from backend.auth import (
    admin_auth_required,
    get_user_from_token,
    get_authenticated_session_expires_at,
    stream_key_required,
    audit_stream_event,
)
from backend.config import is_tvh_process_running_locally
from backend.datetime_utils import to_utc_iso
from backend.dvr_profiles import normalize_recording_profiles, normalize_retention_policy
from backend.streaming import build_local_hls_proxy_url, normalize_local_proxy_url, append_stream_key
from backend.tvheadend.tvh_requests import configure_tvh, ensure_tvh_sync_user
from backend.channels import build_channel_logo_proxy_url


@blueprint.route('/')
def index():
    return redirect('/tic-web/')


@blueprint.route('/tic-web/', strict_slashes=False)
@blueprint.route('/tic-web/<path:path>')
async def serve_frontend(path=None):
    assets_root = current_app.config.get('ASSETS_ROOT')
    if not assets_root or not os.path.exists(assets_root):
        current_app.logger.error(f"ASSETS_ROOT does not exist: {assets_root}")
        return "Frontend assets not found", 404

    # If no path provided, or if it's a directory request, serve index.html
    if not path:
        return await _serve_file(assets_root, 'index.html')

    # Check if the requested path is a real file
    full_path = os.path.join(assets_root, path)
    if os.path.isfile(full_path):
        return await _serve_file(assets_root, path)

    # History API fallback: serve index.html for virtual routes
    return await _serve_file(assets_root, 'index.html')


async def _serve_file(directory, filename):
    try:
        response = await send_from_directory(directory, filename)
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
    except Exception:
        return "File not found", 404


@blueprint.route('/tic-web/epg.xml')
@stream_key_required
async def serve_epg_static():
    await audit_stream_event(request._stream_user, "epg_xml", request.path)
    config = current_app.config['APP_CONFIG']
    settings = config.read_settings()
    base_url = request.url_root.rstrip("/")
    file_path = os.path.join(config.config_path, 'epg.xml')
    with open(file_path, "r", encoding="utf-8") as epg_file:
        payload = epg_file.read()
    from backend.epgs import XMLTV_HOST_PLACEHOLDER
    if base_url and XMLTV_HOST_PLACEHOLDER in payload:
        payload = payload.replace(XMLTV_HOST_PLACEHOLDER, base_url)
    return Response(payload, mimetype='application/xml')


async def _build_playlist_with_epg():
    config = current_app.config['APP_CONFIG']
    settings = config.read_settings()
    use_tvh_source = settings['settings'].get('route_playlists_through_tvh', False)
    instance_id = config.ensure_instance_id()
    stream_key = request.args.get('stream_key') or request.args.get('password')
    username = None
    if stream_key and getattr(request, "_stream_user", None):
        username = request._stream_user.username
    else:
        username = request.args.get('username')
    base_url = request.url_root.rstrip("/") or settings['settings'].get('app_url') or ""
    epg_url = f'{base_url}/xmltv.php'
    if stream_key:
        if username:
            epg_url = f'{epg_url}?username={username}&password={stream_key}'
        else:
            epg_url = f'{epg_url}?stream_key={stream_key}'

    from backend.channels import read_config_all_channels
    channels = await read_config_all_channels()
    playlist = [f'#EXTM3U url-tvg="{epg_url}"']

    for channel in channels:
        if not channel.get("enabled"):
            continue
        channel_uuid = channel.get("tvh_uuid")
        channel_name = channel.get("name")
        channel_logo_url = build_channel_logo_proxy_url(
            channel.get("id"),
            base_url,
            channel.get("logo_url") or "",
        )
        channel_number = channel.get("number")
        line = f'#EXTINF:-1 tvg-name="{channel_name}" tvg-logo="{channel_logo_url}" tvg-id="{channel_uuid}" tvg-chno="{channel_number}"'
        if channel.get("tags"):
            group_title = channel["tags"][0]
            line += f' group-title="{group_title}"'
        line += f",{channel_name}"
        playlist.append(line)

        channel_url = None
        if use_tvh_source and channel_uuid:
            channel_url = f"{base_url}/tic-api/tvh_stream/stream/channel/{channel_uuid}?profile=pass&weight=300"
            if stream_key:
                channel_url = append_stream_key(channel_url, stream_key=stream_key)
        else:
            source = channel['sources'][0] if channel.get('sources') else None
            source_url = source.get('stream_url') if source else None
            if source_url:
                is_manual = source.get('source_type') == 'manual'
                use_hls_proxy = bool(source.get('use_hls_proxy', False))
                if is_manual and use_hls_proxy:
                    channel_url = build_local_hls_proxy_url(
                        base_url,
                        instance_id,
                        source_url,
                        stream_key=stream_key,
                        username=username,
                    )
                else:
                    channel_url = normalize_local_proxy_url(
                        source_url,
                        base_url=base_url,
                        instance_id=instance_id,
                        stream_key=stream_key,
                        username=username,
                    )
        if channel_url:
            playlist.append(channel_url)

    return Response("\n".join(playlist), mimetype='application/vnd.apple.mpegurl')


@blueprint.route('/tic-web/playlist.m3u8')
@stream_key_required
async def serve_playlist_static():
    await audit_stream_event(request._stream_user, "playlist_m3u8", request.path)
    return await _build_playlist_with_epg()


@blueprint.route('/tic-api/ping')
async def ping():
    # Frontend AIO mixin expects uppercase 'PONG' substring in plain response
    return 'PONG', 200, {'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store'}


@blueprint.route('/tic-tvh/ping')
async def ping_tvh_alias():
    """
    This is just a convenience alias. Some clients are probing /tic-tvh/ping (tvheadend http_root); return same pong
    """
    return await ping()


def _strip_hop_by_hop_headers(headers: dict) -> dict:
    skip = {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
    return {k: v for k, v in headers.items() if k.lower() not in skip}


async def _get_tvh_proxy_base():
    config = current_app.config['APP_CONFIG']
    await ensure_tvh_sync_user(config)
    tvh_settings = await config.tvh_connection_settings()
    if tvh_settings.get("tvh_local"):
        username = tvh_settings.get("tvh_username")
        password = tvh_settings.get("tvh_password")
    else:
        sync_user = await asyncio.to_thread(config.get_tvh_sync_user)
        username = sync_user.get("username") or tvh_settings.get("tvh_username")
        password = sync_user.get("password") or tvh_settings.get("tvh_password")
    if not username or not password:
        return None, None, None
    host = tvh_settings["tvh_host"]
    port = tvh_settings["tvh_port"]
    path = tvh_settings["tvh_path"].rstrip("/")
    base_url = f"http://{host}:{port}{path}"
    return base_url, username, password


async def _proxy_tvh_http(subpath: str):
    base_url, username, password = await _get_tvh_proxy_base()
    if not base_url:
        return jsonify({"success": False, "message": "TVH sync user not configured"}), 503
    path = subpath.lstrip("/")
    target = f"{base_url}/{path}" if path else f"{base_url}/"
    if request.query_string:
        target = f"{target}?{request.query_string.decode()}"
    headers = _strip_hop_by_hop_headers(dict(request.headers))
    headers.pop("Host", None)
    headers.pop("Authorization", None)
    data = await request.get_data() if request.method not in ("GET", "HEAD") else None
    timeout = aiohttp.ClientTimeout(total=60)
    allow_redirects = request.method in ("GET", "HEAD")
    session = aiohttp.ClientSession(timeout=timeout, auto_decompress=False)
    try:
        resp = await session.request(
            request.method,
            target,
            data=data,
            headers=headers,
            auth=aiohttp.BasicAuth(username, password),
            allow_redirects=allow_redirects,
        )
    except aiohttp.ClientError as exc:
        await session.close()
        current_app.logger.warning("TVH proxy error: %s", exc)
        return jsonify({"success": False, "message": "TVH proxy connection failed"}), 502

    response_headers = _strip_hop_by_hop_headers(dict(resp.headers))
    response_headers.pop("Content-Length", None)

    first_chunk = await resp.content.read(8192)
    is_gzip = len(first_chunk) >= 2 and first_chunk[0] == 0x1F and first_chunk[1] == 0x8B
    if response_headers.get("Content-Encoding", "").lower() == "gzip" and not is_gzip:
        response_headers.pop("Content-Encoding", None)
    if response_headers.get("Content-Encoding") is None and is_gzip:
        response_headers["Content-Encoding"] = "gzip"

    async def stream_body():
        try:
            if first_chunk:
                yield first_chunk
            async for chunk in resp.content.iter_chunked(8192):
                yield chunk
        except aiohttp.ClientConnectionError:
            return
        finally:
            resp.release()
            await session.close()

    return Response(stream_body(), status=resp.status, headers=response_headers)


@blueprint.route('/tic-tvh/', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS'])
@admin_auth_required
async def tvh_root_proxy():
    return await _proxy_tvh_http("")


@blueprint.route('/tic-tvh/<path:subpath>', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS'])
@admin_auth_required
async def tvh_any_proxy(subpath: str):
    if subpath == 'ping':
        return await ping()
    return await _proxy_tvh_http(subpath)


@blueprint.route('/tic-api/tvh_stream/<path:subpath>', methods=['GET', 'HEAD', 'OPTIONS'])
@stream_key_required
async def tvh_stream_proxy(subpath: str):
    # Internal stream-only proxy for external clients (stream key auth).
    if not (subpath.startswith("stream/") or subpath.startswith("dvrfile/")):
        return jsonify({"success": False, "message": "Not found"}), 404
    return await _proxy_tvh_http(subpath)


@blueprint.websocket('/tic-tvh/<path:subpath>')
@admin_auth_required
async def tvh_ws_proxy(subpath: str):
    base_url, username, password = await _get_tvh_proxy_base()
    if not base_url:
        await websocket.close(1011)
        return
    path = subpath.lstrip("/")
    ws_url = base_url.replace("http://", "ws://").replace("https://", "wss://")
    ws_url = f"{ws_url}/{path}" if path else f"{ws_url}/"
    if websocket.query_string:
        ws_url = f"{ws_url}?{websocket.query_string.decode()}"
    headers = _strip_hop_by_hop_headers(dict(websocket.headers))
    headers.pop("Host", None)
    headers.pop("Authorization", None)
    protocols = []
    raw_protocols = websocket.headers.get("Sec-WebSocket-Protocol")
    if raw_protocols:
        protocols = [p.strip() for p in raw_protocols.split(",") if p.strip()]
        try:
            await websocket.accept(subprotocol=protocols[0])
        except Exception:
            pass
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(
            ws_url,
            headers=headers,
            auth=aiohttp.BasicAuth(username, password),
            protocols=protocols or None,
        ) as ws:
            async def to_upstream():
                try:
                    while True:
                        data = await websocket.receive()
                        await ws.send_str(data) if isinstance(data, str) else await ws.send_bytes(data)
                except Exception:
                    pass

            async def to_client():
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await websocket.send(msg.data)
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        await websocket.send(msg.data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                        break

            await asyncio.gather(to_upstream(), to_client())


@blueprint.route('/tic-api/check-auth')
async def api_check_auth():
    config = current_app.config['APP_CONFIG']
    user = await get_user_from_token()
    if user:
        session_expires_at = get_authenticated_session_expires_at()
        return jsonify(
            {
                "success":     True,
                "runtime_key": config.runtime_key,
                "session_expires_at": to_utc_iso(session_expires_at),
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
    async def snapshot(task_broker):
        return {
            "task_queue_status": await task_broker.get_status(),
            "current_task": await task_broker.get_currently_running_task(),
            "pending_tasks": await task_broker.get_pending_tasks(),
        }

    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.get_pending_tasks()
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

    data = await snapshot(task_broker)
    if wait and timeout:
        start = asyncio.get_event_loop().time()
        while True:
            await asyncio.sleep(1)
            updated = await snapshot(task_broker)
            if updated != data:
                data = updated
                break
            if (asyncio.get_event_loop().time() - start) >= timeout:
                break
    return jsonify(
        {
            "success": True,
            "data":    data,
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

    settings_payload = json_data.get("settings") if isinstance(json_data, dict) else None
    if isinstance(settings_payload, dict):
        dvr_payload = settings_payload.get("dvr")
        if isinstance(dvr_payload, dict):
            settings_payload["dvr"] = {
                "pre_padding_mins": int(dvr_payload.get("pre_padding_mins", 2) or 2),
                "post_padding_mins": int(dvr_payload.get("post_padding_mins", 5) or 5),
                "retention_policy": normalize_retention_policy(dvr_payload.get("retention_policy")),
                "recording_profiles": normalize_recording_profiles(dvr_payload.get("recording_profiles")),
            }

    # Mark first run as complete
    json_data['settings']['first_run'] = False

    # Save the config
    config.update_settings(json_data)
    config.save_settings()

    # Store settings for TVH service (async via task queue)
    tvh_update_requested = any(
        key in (json_data.get('settings') or {})
        for key in ('tvheadend', 'dvr', 'route_playlists_through_tvh')
    )
    tvh_cfg = config.read_settings().get('settings', {}).get('tvheadend', {})
    tvh_is_configured = bool(tvh_cfg.get('host')) or bool(tvh_cfg.get('port')) or bool(tvh_cfg.get('path'))
    tvh_is_local = await is_tvh_process_running_locally()
    if tvh_update_requested and (tvh_is_configured or tvh_is_local):
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.add_task({
            'name':     'Configure TVH settings',
            'function': configure_tvh,
            'args':     [config],
        }, priority=15)
        if 'dvr' in (json_data.get('settings') or {}):
            await task_broker.add_task({
                'name':     'Sync all users to TVH',
                'function': sync_all_users_to_tvh,
                'args':     [config],
            }, priority=16)
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
