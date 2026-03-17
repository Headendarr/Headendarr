#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import time
from typing import Any, Dict, List, Optional, Tuple

from quart import request, jsonify, Response, redirect, current_app

from backend.api import blueprint
from backend.api.connections_common import resolve_channel_stream_url
from backend.auth import get_request_client_ip
from backend.api.routes_connections_epg import build_xmltv_response
from backend.auth import audit_stream_event, mark_stream_key_usage
from backend.cso import (
    CS_VOD_USE_PROXY_SESSION,
    should_use_vod_proxy_session,
    subscribe_vod_hls,
    subscribe_vod_proxy_stream,
    subscribe_vod_stream,
)
from backend.channels import read_config_all_channels, build_channel_logo_output_url
from backend.playlists import build_m3u_playlist_content, read_config_all_playlists
from backend.stream_activity import stop_stream_activity, touch_stream_activity, upsert_stream_activity
from backend.url_resolver import get_request_base_url, get_request_host_info
from backend.users import get_user_by_username
from backend.vod import (
    VOD_KIND_MOVIE,
    VOD_KIND_SERIES,
    build_vod_activity_metadata,
    build_curated_category_payloads,
    build_curated_item_payloads,
    fetch_series_info_payload,
    fetch_vod_info_payload,
    resolve_episode_playback_candidates,
    resolve_movie_playback_candidates,
    resolve_vod_profile_id,
    select_vod_playback_target,
    user_can_access_vod_kind,
)


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
    await mark_stream_key_usage(user)
    return user, None


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
    config = current_app.config["APP_CONFIG"]
    channels = await read_config_all_channels()
    base_url = get_request_base_url(request)
    enabled = []
    for channel in channels:
        if not channel.get("enabled"):
            continue
        channel["logo_url"] = build_channel_logo_output_url(
            config,
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
    hostname, port, scheme = get_request_host_info(request)
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
            "server_protocol": scheme,
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


def _xc_vod_allowed(user, kind: str) -> bool:
    return user_can_access_vod_kind(user, kind)


def _xc_vod_profile(candidate, ext: str | None = None) -> str:
    requested_ext = str(ext or "").strip().lower().lstrip(".")
    if requested_ext == "m3u8":
        return "hls"
    if requested_ext in {"ts", "mpegts"}:
        return "mpegts"
    if requested_ext in {"mkv", "matroska"}:
        return "matroska"
    if requested_ext == "mp4":
        return "mp4"
    if requested_ext == "webm":
        return "webm"
    return resolve_vod_profile_id(candidate)


def _get_connection_id():
    value = (request.args.get("connection_id") or request.args.get("cid") or "").strip()
    return value or f"xc-{int(time.time() * 1000)}"


def _response_from_plan(plan, fallback_message, fallback_status=503):
    if plan.generator is None:
        return Response(fallback_message, status=int(plan.status_code or fallback_status))
    response = Response(
        plan.generator,
        content_type=plan.content_type or "application/octet-stream",
        status=plan.status_code or 200,
    )
    for key, value in (getattr(plan, "headers", None) or {}).items():
        response.headers[key] = value
    response.timeout = None
    return response


async def _iter_plan_generator(plan, connection_id, touch_identity):
    last_touch_ts = time.time()
    try:
        while True:
            chunk = await plan.generator.__anext__()
            now = time.time()
            if (now - last_touch_ts) >= 5.0:
                await touch_stream_activity(connection_id, identity=touch_identity)
                last_touch_ts = now
            yield chunk
    except StopAsyncIteration:
        return


def _wrap_stream_plan(source_plan, connection_id, identity, stop_kwargs):
    async def _generator():
        try:
            async for chunk in _iter_plan_generator(source_plan, connection_id, identity):
                yield chunk
        finally:
            try:
                close = getattr(source_plan.generator, "aclose", None)
                if close is not None:
                    await close()
            except Exception:
                pass
            await stop_stream_activity(
                identity,
                connection_id=connection_id,
                endpoint_override=identity,
                perform_audit=False,
                **stop_kwargs,
            )

    return type(
        "Plan",
        (),
        {
            "generator": _generator(),
            "content_type": source_plan.content_type,
            "status_code": source_plan.status_code,
            "headers": getattr(source_plan, "headers", None),
        },
    )()


async def _render_hls_playlist(output_session, connection_id: str):
    deadline = time.time() + 15.0
    while time.time() < deadline:
        await output_session.touch_client(connection_id)
        playlist_text = await output_session.read_playlist_text()
        if playlist_text:
            return playlist_text
        await asyncio.sleep(0.2)
    return ""


def _rewrite_hls_playlist(playlist_text: str, segment_base_path: str) -> str:
    lines = []
    for raw_line in str(playlist_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            lines.append(raw_line)
            continue
        segment_name = line.split("?", 1)[0]
        lines.append(f"{segment_base_path.rstrip('/')}/{segment_name}")
    return "\n".join(lines) + "\n"


def _combined_cso_enabled() -> bool:
    settings = current_app.config["APP_CONFIG"].read_settings()
    return bool((settings.get("settings") or {}).get("route_playlists_through_cso", True))


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

    base_url = get_request_base_url(request)
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
    if action == "get_vod_categories":
        if not _xc_vod_allowed(user, VOD_KIND_MOVIE):
            return jsonify([])
        return jsonify(await build_curated_category_payloads(VOD_KIND_MOVIE))
    if action == "get_vod_streams":
        if not _xc_vod_allowed(user, VOD_KIND_MOVIE):
            return jsonify([])
        return jsonify(await build_curated_item_payloads(VOD_KIND_MOVIE, category_id=request.args.get("category_id")))
    if action == "get_vod_info":
        if not _xc_vod_allowed(user, VOD_KIND_MOVIE):
            return jsonify({})
        vod_id = request.args.get("vod_id")
        if not str(vod_id or "").isdigit():
            return jsonify({})
        payload = await fetch_vod_info_payload(int(vod_id))
        return jsonify(payload or {})
    if action == "get_series_categories":
        if not _xc_vod_allowed(user, VOD_KIND_SERIES):
            return jsonify([])
        return jsonify(await build_curated_category_payloads(VOD_KIND_SERIES))
    if action == "get_series":
        if not _xc_vod_allowed(user, VOD_KIND_SERIES):
            return jsonify([])
        return jsonify(await build_curated_item_payloads(VOD_KIND_SERIES, category_id=request.args.get("category_id")))
    if action == "get_series_info":
        if not _xc_vod_allowed(user, VOD_KIND_SERIES):
            return jsonify({})
        series_id = request.args.get("series_id")
        if not str(series_id or "").isdigit():
            return jsonify({})
        payload = await fetch_series_info_payload(int(series_id))
        return jsonify(payload or {})

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
    await mark_stream_key_usage(user)
    await audit_stream_event(user, "xc_stream", request.path)

    channel_map = await _get_channel_map()
    channel = channel_map.get(str(stream_id))
    if not channel:
        return jsonify({"error": "Not found"}), 404

    target, _, _ = await resolve_channel_stream_url(
        config=current_app.config["APP_CONFIG"],
        channel_details=channel,
        base_url=get_request_base_url(request),
        stream_key=user.streaming_key,
        username=user.username,
        requested_profile=_xc_channel_profile(channel),
        route_scope="combined",
    )
    if not target:
        return jsonify({"error": "Stream unavailable"}), 404
    return redirect(target, code=302)


@blueprint.route("/movie/<username>/<password>/<item_id>", methods=["GET"])
@blueprint.route("/movie/<username>/<password>/<item_id>.<ext>", methods=["GET"])
async def xc_movie_stream(username: str, password: str, item_id: str, ext: str = None):
    user = await get_user_by_username(username)
    if not user or not user.is_active or user.streaming_key != password:
        return jsonify({"error": "Unauthorized"}), 401
    stream_key = user.streaming_key
    if not _xc_vod_allowed(user, VOD_KIND_MOVIE):
        return jsonify({"error": "Forbidden"}), 403
    candidates = await resolve_movie_playback_candidates(int(item_id))
    if not candidates:
        return jsonify({"error": "Not found"}), 404
    profile = _xc_vod_profile(candidates[0], ext=ext)
    use_proxy_session = CS_VOD_USE_PROXY_SESSION and should_use_vod_proxy_session(candidates[0], profile)
    candidate, upstream_url, selection_error = await select_vod_playback_target(
        candidates,
        prefer_local_cache=use_proxy_session,
    )
    connection_id = _get_connection_id()
    if selection_error == "capacity_blocked" and not upstream_url:
        return jsonify({"error": "Source capacity limit reached"}), 503
    if not candidate:
        return jsonify({"error": "Not found"}), 404
    if not upstream_url and selection_error not in {None, "capacity_blocked"}:
        return jsonify({"error": "Stream unavailable"}), 404
    if upstream_url and not _combined_cso_enabled():
        return redirect(upstream_url, code=302)

    config = current_app.config["APP_CONFIG"]
    request_base_url = get_request_base_url(request)
    profile = _xc_vod_profile(candidate, ext=ext)
    if profile == "hls":
        return redirect(
            f"{request_base_url}/movie/{username}/{password}/{int(item_id)}/hls/{connection_id}/index.m3u8",
            code=302,
        )

    identity = f"/xc/movie/{int(item_id)}"
    request_client_ip = get_request_client_ip()
    request_user_agent = request.headers.get("User-Agent")
    activity_metadata = build_vod_activity_metadata(candidate)

    await upsert_stream_activity(
        identity,
        connection_id=connection_id,
        endpoint_override=identity,
        user=user,
        ip_address=request_client_ip,
        user_agent=request_user_agent,
        perform_audit=False,
        channel_name=activity_metadata.get("channel_name"),
        channel_logo_url=activity_metadata.get("channel_logo_url"),
        stream_name=activity_metadata.get("stream_name"),
        source_url=upstream_url or identity,
        display_url=activity_metadata.get("display_url"),
        vod_item_id=candidate.group_item.id,
        vod_category_id=candidate.group_item.category_id,
        enrich_metadata=False,
    )

    if use_proxy_session:
        plan = await subscribe_vod_proxy_stream(
            candidate,
            upstream_url,
            connection_id,
            request_headers=dict(request.headers),
        )
    else:
        if not upstream_url:
            return jsonify({"error": "Stream unavailable"}), 404
        plan = await subscribe_vod_stream(
            config,
            candidate,
            upstream_url,
            stream_key,
            profile,
            connection_id,
            request_base_url=request_base_url,
        )

    if plan.generator is not None:
        plan = _wrap_stream_plan(
            plan,
            connection_id,
            identity,
            {
                "user": user,
                "ip_address": request_client_ip,
                "user_agent": request_user_agent,
            },
        )
    return _response_from_plan(plan, "Unable to start playback", 503)


@blueprint.route("/series/<username>/<password>/<episode_id>", methods=["GET"])
@blueprint.route("/series/<username>/<password>/<episode_id>.<ext>", methods=["GET"])
async def xc_series_episode_stream(username: str, password: str, episode_id: str, ext: str = None):
    user = await get_user_by_username(username)
    if not user or not user.is_active or user.streaming_key != password:
        return jsonify({"error": "Unauthorized"}), 401
    stream_key = user.streaming_key
    if not _xc_vod_allowed(user, VOD_KIND_SERIES):
        return jsonify({"error": "Forbidden"}), 403
    candidates, episode_map = await resolve_episode_playback_candidates(int(episode_id))
    if not candidates or episode_map is None:
        return jsonify({"error": "Not found"}), 404
    profile = _xc_vod_profile(candidates[0], ext=ext)
    use_proxy_session = CS_VOD_USE_PROXY_SESSION and should_use_vod_proxy_session(candidates[0], profile)
    candidate, upstream_url, selection_error = await select_vod_playback_target(
        candidates,
        episode=episode_map,
        prefer_local_cache=use_proxy_session,
    )
    connection_id = _get_connection_id()
    if selection_error == "capacity_blocked" and not upstream_url:
        return jsonify({"error": "Source capacity limit reached"}), 503
    if not candidate:
        return jsonify({"error": "Not found"}), 404
    if not upstream_url and selection_error not in {None, "capacity_blocked"}:
        return jsonify({"error": "Stream unavailable"}), 404
    if upstream_url and not _combined_cso_enabled():
        return redirect(upstream_url, code=302)

    config = current_app.config["APP_CONFIG"]
    request_base_url = get_request_base_url(request)
    profile = _xc_vod_profile(candidate, ext=ext)
    if profile == "hls":
        return redirect(
            f"{request_base_url}/series/{username}/{password}/{int(episode_id)}/hls/{connection_id}/index.m3u8",
            code=302,
        )

    identity = f"/xc/series/{int(episode_id)}"
    request_client_ip = get_request_client_ip()
    request_user_agent = request.headers.get("User-Agent")
    activity_metadata = build_vod_activity_metadata(candidate, episode=episode_map)

    await upsert_stream_activity(
        identity,
        connection_id=connection_id,
        endpoint_override=identity,
        user=user,
        ip_address=request_client_ip,
        user_agent=request_user_agent,
        perform_audit=False,
        channel_name=activity_metadata.get("channel_name"),
        channel_logo_url=activity_metadata.get("channel_logo_url"),
        stream_name=activity_metadata.get("stream_name"),
        source_url=upstream_url or identity,
        display_url=activity_metadata.get("display_url"),
        vod_item_id=candidate.group_item.id,
        vod_category_id=candidate.group_item.category_id,
        vod_episode_id=episode_map.id,
        enrich_metadata=False,
    )

    if use_proxy_session:
        plan = await subscribe_vod_proxy_stream(
            candidate,
            upstream_url,
            connection_id,
            request_headers=dict(request.headers),
            episode=episode_map,
        )
    else:
        if not upstream_url:
            return jsonify({"error": "Stream unavailable"}), 404
        plan = await subscribe_vod_stream(
            config,
            candidate,
            upstream_url,
            stream_key,
            profile,
            connection_id,
            episode=episode_map,
            request_base_url=request_base_url,
        )

    if plan.generator is not None:
        plan = _wrap_stream_plan(
            plan,
            connection_id,
            identity,
            {
                "user": user,
                "ip_address": request_client_ip,
                "user_agent": request_user_agent,
            },
        )
    return _response_from_plan(plan, "Unable to start playback", 503)


@blueprint.route("/movie/<username>/<password>/<int:item_id>/hls/<connection_id>/index.m3u8", methods=["GET"])
async def xc_movie_hls_playlist(username: str, password: str, item_id: int, connection_id: str):
    user = await get_user_by_username(username)
    if not user or not user.is_active or user.streaming_key != password:
        return jsonify({"error": "Unauthorized"}), 401
    stream_key = user.streaming_key
    candidates = await resolve_movie_playback_candidates(int(item_id))
    if not candidates:
        return jsonify({"error": "Not found"}), 404
    candidate, upstream_url, selection_error = await select_vod_playback_target(candidates)
    if not candidate:
        return jsonify({"error": "Not found"}), 404
    if not upstream_url:
        return Response("Source capacity limit reached" if selection_error == "capacity_blocked" else "Stream unavailable", status=503 if selection_error == "capacity_blocked" else 404)

    output_session, error_message, status = await subscribe_vod_hls(
        current_app.config["APP_CONFIG"],
        candidate,
        upstream_url,
        stream_key,
        "hls",
        connection_id,
        request_base_url=get_request_base_url(request),
    )
    if not output_session:
        return Response(error_message or "Unable to start CSO HLS stream", status=status or 503)
    await output_session.touch_client(connection_id)
    playlist_text = await _render_hls_playlist(output_session, connection_id)
    playlist_text = _rewrite_hls_playlist(
        playlist_text,
        f"/movie/{username}/{password}/{int(item_id)}/hls/{connection_id}",
    )
    return Response(playlist_text or "", content_type="application/vnd.apple.mpegurl")


@blueprint.route("/movie/<username>/<password>/<int:item_id>/hls/<connection_id>/<segment_name>", methods=["GET"])
async def xc_movie_hls_segment(username: str, password: str, item_id: int, connection_id: str, segment_name: str):
    user = await get_user_by_username(username)
    if not user or not user.is_active or user.streaming_key != password:
        return jsonify({"error": "Unauthorized"}), 401
    stream_key = user.streaming_key
    candidates = await resolve_movie_playback_candidates(int(item_id))
    if not candidates:
        return jsonify({"error": "Not found"}), 404
    candidate, upstream_url, selection_error = await select_vod_playback_target(candidates)
    if not candidate:
        return jsonify({"error": "Not found"}), 404
    if not upstream_url:
        return Response("Source capacity limit reached" if selection_error == "capacity_blocked" else "Stream unavailable", status=503 if selection_error == "capacity_blocked" else 404)

    output_session, error_message, status = await subscribe_vod_hls(
        current_app.config["APP_CONFIG"],
        candidate,
        upstream_url,
        stream_key,
        "hls",
        connection_id,
        request_base_url=get_request_base_url(request),
    )
    if not output_session:
        return Response(error_message or "Unable to start CSO HLS stream", status=status or 503)
    await output_session.touch_client(connection_id)
    payload = await output_session.read_segment_bytes(segment_name)
    return Response(payload or b"", content_type="video/mp2t")


@blueprint.route("/series/<username>/<password>/<int:episode_id>/hls/<connection_id>/index.m3u8", methods=["GET"])
async def xc_series_hls_playlist(username: str, password: str, episode_id: int, connection_id: str):
    user = await get_user_by_username(username)
    if not user or not user.is_active or user.streaming_key != password:
        return jsonify({"error": "Unauthorized"}), 401
    stream_key = user.streaming_key
    candidates, episode_map = await resolve_episode_playback_candidates(int(episode_id))
    if not candidates or episode_map is None:
        return jsonify({"error": "Not found"}), 404
    candidate, upstream_url, selection_error = await select_vod_playback_target(candidates, episode=episode_map)
    if not candidate:
        return jsonify({"error": "Not found"}), 404
    if not upstream_url:
        return Response("Source capacity limit reached" if selection_error == "capacity_blocked" else "Stream unavailable", status=503 if selection_error == "capacity_blocked" else 404)

    output_session, error_message, status = await subscribe_vod_hls(
        current_app.config["APP_CONFIG"],
        candidate,
        upstream_url,
        stream_key,
        "hls",
        connection_id,
        episode=episode_map,
        request_base_url=get_request_base_url(request),
    )
    if not output_session:
        return Response(error_message or "Unable to start CSO HLS stream", status=status or 503)
    await output_session.touch_client(connection_id)
    playlist_text = await _render_hls_playlist(output_session, connection_id)
    playlist_text = _rewrite_hls_playlist(
        playlist_text,
        f"/series/{username}/{password}/{int(episode_id)}/hls/{connection_id}",
    )
    return Response(playlist_text or "", content_type="application/vnd.apple.mpegurl")


@blueprint.route("/series/<username>/<password>/<int:episode_id>/hls/<connection_id>/<segment_name>", methods=["GET"])
async def xc_series_hls_segment(username: str, password: str, episode_id: int, connection_id: str, segment_name: str):
    user = await get_user_by_username(username)
    if not user or not user.is_active or user.streaming_key != password:
        return jsonify({"error": "Unauthorized"}), 401
    stream_key = user.streaming_key
    candidates, episode_map = await resolve_episode_playback_candidates(int(episode_id))
    if not candidates or episode_map is None:
        return jsonify({"error": "Not found"}), 404
    candidate, upstream_url, selection_error = await select_vod_playback_target(candidates, episode=episode_map)
    if not candidate:
        return jsonify({"error": "Not found"}), 404
    if not upstream_url:
        return Response("Source capacity limit reached" if selection_error == "capacity_blocked" else "Stream unavailable", status=503 if selection_error == "capacity_blocked" else 404)

    output_session, error_message, status = await subscribe_vod_hls(
        current_app.config["APP_CONFIG"],
        candidate,
        upstream_url,
        stream_key,
        "hls",
        connection_id,
        episode=episode_map,
        request_base_url=get_request_base_url(request),
    )
    if not output_session:
        return Response(error_message or "Unable to start CSO HLS stream", status=status or 503)
    await output_session.touch_client(connection_id)
    payload = await output_session.read_segment_bytes(segment_name)
    return Response(payload or b"", content_type="video/mp2t")
