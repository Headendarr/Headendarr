#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import time
from typing import Any, Dict, List, Optional, Tuple

from quart import request, jsonify, Response, redirect, current_app

from backend.api import blueprint
from backend.api.connections_common import resolve_channel_stream_url
from backend.api.routes_connections_epg import build_xmltv_response
from backend.auth import audit_stream_event
from backend.channels import read_config_all_channels, build_channel_logo_proxy_url
from backend.playlists import build_m3u_playlist_content, read_config_all_playlists
from backend.users import get_user_by_username


class _TTLCache:
    def __init__(self):
        self._store: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str):
        entry = self._store.get(key)
        if not entry:
            return None
        expires_at, value = entry
        if expires_at and expires_at < time.time():
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any, ttl_seconds: int = 30):
        expires_at = time.time() + ttl_seconds if ttl_seconds else None
        self._store[key] = (expires_at, value)


_xc_cache = _TTLCache()
_XC_ALLOWED_PROFILES = {"default", "mpegts", "h264-aac-mpegts"}


async def _xc_auth_user():
    username = request.args.get("username") or ""
    password = request.args.get("password") or ""
    if not username or not password:
        return None, ("Missing username or password", 400)
    user = await get_user_by_username(username)
    if not user or not user.is_active:
        return None, ("Unauthorized", 401)
    if user.streaming_key != password:
        return None, ("Unauthorized", 401)
    return user, None


def _build_base_url() -> str:
    return request.host_url.rstrip("/")


def _xc_channel_profile(channel: Dict[str, Any]) -> str:
    profile = str(channel.get("cso_profile") or "").strip().lower()
    if not profile:
        policy = channel.get("cso_policy")
        if isinstance(policy, dict):
            profile = str(policy.get("profile") or "").strip().lower()
    if profile in _XC_ALLOWED_PROFILES:
        return profile
    return "default"


async def _get_enabled_channels() -> List[Dict[str, Any]]:
    channels = await read_config_all_channels()
    base_url = _build_base_url()
    enabled = []
    for channel in channels:
        if not channel.get("enabled"):
            continue
        channel["logo_url"] = build_channel_logo_proxy_url(
            channel.get("id"),
            base_url,
            channel.get("logo_url") or "",
        )
        enabled.append(channel)
    return enabled


async def _get_channel_map() -> Dict[str, Dict[str, Any]]:
    cached = _xc_cache.get("xc_channel_map")
    if cached:
        return cached
    channel_map = {str(ch["id"]): ch for ch in await _get_enabled_channels()}
    _xc_cache.set("xc_channel_map", channel_map, ttl_seconds=30)
    return channel_map


def _build_category_map(channels: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    names = []
    for channel in channels:
        tags = channel.get("tags") or []
        names.extend(tags or ["Uncategorized"])
    unique_names = sorted({n for n in names if n})
    if "Uncategorized" not in unique_names:
        unique_names.insert(0, "Uncategorized")
    categories = []
    name_to_id: Dict[str, str] = {}
    for idx, name in enumerate(unique_names, start=1):
        category_id = str(idx)
        categories.append(
            {
                "category_id": category_id,
                "category_name": name,
                "parent_id": 0,
            }
        )
        name_to_id[name] = category_id
    return categories, name_to_id


async def _get_max_connections() -> str:
    cached = _xc_cache.get("xc_max_connections")
    if cached:
        return cached
    playlists = await read_config_all_playlists(current_app.config["APP_CONFIG"])
    max_connections = 1
    for playlist in playlists:
        try:
            max_connections = max(max_connections, int(playlist.get("connections", 1)))
        except (TypeError, ValueError):
            continue
    value = str(max_connections)
    _xc_cache.set("xc_max_connections", value, ttl_seconds=60)
    return value


def _build_xc_server_info(user, include_categories=False):
    raw_host = request.host
    if ":" in raw_host:
        hostname, port = raw_host.split(":", 1)
    else:
        hostname = raw_host
        port = "443" if request.scheme == "https" else "80"
    info = {
        "user_info": {
            "username": user.username,
            "password": user.streaming_key,
            "message": "Headendarr XC API",
            "auth": 1,
            "status": "Active",
            "exp_date": str(int(time.time()) + (90 * 24 * 60 * 60)),
            "max_connections": _xc_cache.get("xc_max_connections") or "1",
            "allowed_output_formats": ["ts"],
        },
        "server_info": {
            "url": hostname,
            "server_protocol": request.scheme,
            "port": port,
            "timezone": time.tzname[0],
            "timestamp_now": int(time.time()),
            "time_now": time.strftime("%Y-%m-%d %H:%M:%S"),
            "process": True,
        },
    }
    if include_categories:
        info["categories"] = {"live": _xc_cache.get("xc_categories") or []}
    return info


@blueprint.route("/get.php", methods=["GET"])
async def xc_get():
    user, error = await _xc_auth_user()
    if error:
        return jsonify({"error": error[0]}), error[1]
    await audit_stream_event(user, "xc_get", request.path)

    cache_key = f"xc_m3u:{user.id}:ts_only"
    cached = _xc_cache.get(cache_key)
    if cached:
        return Response(cached, mimetype="text/plain")

    channels = await _get_enabled_channels()
    categories, _ = _build_category_map(channels)
    _xc_cache.set("xc_categories", categories, ttl_seconds=60)

    base_url = request.url_root.rstrip("/")
    epg_url = f"{base_url}/tic-api/epg/xmltv.xml?username={user.username}&password={user.streaming_key}"

    async def _resolve_stream_url(channel):
        stream_url, _, _ = await resolve_channel_stream_url(
            config=current_app.config["APP_CONFIG"],
            channel_details=channel,
            base_url=base_url,
            stream_key=user.streaming_key,
            username=user.username,
            requested_profile=_xc_channel_profile(channel),
            route_scope="combined",
        )
        return stream_url

    content = await build_m3u_playlist_content(
        channels=channels,
        epg_url=epg_url,
        stream_url_resolver=_resolve_stream_url,
        include_xtvg=True,
    )
    _xc_cache.set(cache_key, content, ttl_seconds=30)
    return Response(content, mimetype="text/plain")


@blueprint.route("/xmltv.php", methods=["GET"])
async def xc_xmltv():
    user, error = await _xc_auth_user()
    if error:
        return jsonify({"error": error[0]}), error[1]
    await audit_stream_event(user, "xc_xmltv", request.path)
    return await build_xmltv_response()


@blueprint.route("/player_api.php", methods=["GET"])
async def xc_player_api():
    user, error = await _xc_auth_user()
    if error:
        return jsonify({"error": error[0]}), error[1]
    await audit_stream_event(user, "xc_player_api", request.path)

    action = request.args.get("action")
    channels = await _get_enabled_channels()
    categories = _xc_cache.get("xc_categories")
    name_to_id = _xc_cache.get("xc_category_map")
    if not categories or not name_to_id:
        categories, name_to_id = _build_category_map(channels)
        _xc_cache.set("xc_categories", categories, ttl_seconds=60)
        _xc_cache.set("xc_category_map", name_to_id, ttl_seconds=60)
    _xc_cache.set("xc_max_connections", await _get_max_connections(), ttl_seconds=60)

    if action == "get_live_categories":
        return jsonify(categories)
    if action == "get_live_streams":
        cached_streams = _xc_cache.get("xc_live_streams")
        if cached_streams:
            return jsonify(cached_streams)
        stream_list = []
        for channel in channels:
            group_title = (channel.get("tags") or ["Uncategorized"])[0]
            category_id = name_to_id.get(group_title, "1")
            stream_list.append(
                {
                    "num": channel.get("number") or 0,
                    "name": channel.get("name") or "",
                    "stream_id": str(channel["id"]),
                    "stream_type": "live",
                    "stream_icon": channel.get("logo_url") or "",
                    "category_id": category_id,
                    "tv_archive": 0,
                    "tv_archive_duration": 0,
                }
            )
        _xc_cache.set("xc_live_streams", stream_list, ttl_seconds=30)
        return jsonify(stream_list)
    if action in ("get_short_epg", "get_simple_data_table"):
        return jsonify({"epg_listings": []})
    if action in (
        "get_vod_categories",
        "get_vod_streams",
        "get_vod_info",
        "get_series_categories",
        "get_series",
        "get_series_info",
    ):
        return jsonify([])

    info = _build_xc_server_info(user)
    return jsonify(info)


@blueprint.route("/panel_api.php", methods=["GET"])
async def xc_panel_api():
    user, error = await _xc_auth_user()
    if error:
        return jsonify({"error": error[0]}), error[1]
    await audit_stream_event(user, "xc_panel_api", request.path)

    _xc_cache.set("xc_max_connections", await _get_max_connections(), ttl_seconds=60)
    info = _build_xc_server_info(user, include_categories=True)
    return jsonify(info)


@blueprint.route("/live/<username>/<password>/<stream_id>", methods=["GET"])
@blueprint.route("/live/<username>/<password>/<stream_id>.<ext>", methods=["GET"])
@blueprint.route("/<username>/<password>/<stream_id>", methods=["GET"])
@blueprint.route("/<username>/<password>/<stream_id>.<ext>", methods=["GET"])
async def xc_stream(username: str, password: str, stream_id: str, ext: str = None):
    user = await get_user_by_username(username)
    if not user or not user.is_active or user.streaming_key != password:
        return jsonify({"error": "Unauthorized"}), 401
    await audit_stream_event(user, "xc_stream", request.path)

    channel_map = await _get_channel_map()
    channel = channel_map.get(str(stream_id))
    if not channel:
        return jsonify({"error": "Not found"}), 404

    target, _, _ = await resolve_channel_stream_url(
        config=current_app.config["APP_CONFIG"],
        channel_details=channel,
        base_url=_build_base_url(),
        stream_key=user.streaming_key,
        username=user.username,
        requested_profile=_xc_channel_profile(channel),
        route_scope="combined",
    )
    if not target:
        return jsonify({"error": "Stream unavailable"}), 404
    return redirect(target, code=302)
