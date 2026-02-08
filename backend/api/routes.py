#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import os
import aiofiles
import aiohttp

from quart import request, jsonify, send_from_directory, current_app, Response, websocket

from backend.api import blueprint

from backend.api.tasks import TaskQueueBroker
from backend.auth import admin_auth_required, get_user_from_token, stream_key_required, audit_stream_event
from backend.config import is_tvh_process_running_locally
from backend.tvheadend.tvh_requests import configure_tvh, ensure_tvh_sync_user


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
    sync_user = await asyncio.to_thread(config.get_tvh_sync_user)
    if not sync_user.get("username") or not sync_user.get("password"):
        return None, None, None
    tvh_settings = await config.tvh_connection_settings()
    host = tvh_settings["tvh_host"]
    port = tvh_settings["tvh_port"]
    path = tvh_settings["tvh_path"].rstrip("/")
    base_url = f"http://{host}:{port}{path}"
    return base_url, sync_user["username"], sync_user["password"]


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
    async def snapshot(task_broker):
        return {
            "task_queue_status": await task_broker.get_status(),
            "current_task":      await task_broker.get_currently_running_task(),
            "pending_tasks":     await task_broker.get_pending_tasks(),
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
