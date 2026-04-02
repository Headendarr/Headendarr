#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import base64
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, cast
from quart import Response, current_app, jsonify, redirect, request
from sqlalchemy import or_, select
from sqlalchemy.orm import joinedload

from backend.api import blueprint
from backend.api.connections_common import resolve_channel_stream_url
from backend.api.routes_connections_epg import build_xmltv_response
from backend.auth import (
    audit_stream_event,
    forbidden_response,
    get_request_client_ip,
    mark_stream_key_usage,
    precheck_stream_auth_limit,
    record_failed_stream_auth,
)
from backend.channels import read_config_all_channels
from backend.epgs import build_channel_logo_output_url, load_preferred_epg_channel_row
from backend.models import EpgChannelProgrammes, PlaylistStreams, Session, XcAccount
from backend.cso import (
    CS_VOD_USE_PROXY_SESSION,
    should_use_vod_proxy_session,
    subscribe_vod_hls,
    subscribe_vod_proxy_stream,
    subscribe_vod_stream,
)
from backend.playlists import (
    XC_ACCOUNT_TYPE,
    build_m3u_playlist_content,
    read_config_all_playlists,
)
from backend.xc.cache import xc_cache
from backend.stream_activity import stop_stream_activity, touch_stream_activity, upsert_stream_activity
from backend.stream_profiles import content_type_for_media_path, is_hls_stream_profile
from backend.url_resolver import get_request_base_url, get_request_host_info
from backend.users import get_user_by_username, user_timeshift_enabled
from backend.utils import convert_to_int
from backend.vod import (
    VOD_KIND_MOVIE,
    VOD_KIND_SERIES,
    build_curated_category_payloads,
    build_curated_item_payloads,
    build_vod_activity_metadata,
    fetch_series_info_payload,
    fetch_vod_info_payload,
    find_cached_vod_playback_candidate,
    resolve_episode_playback_candidates,
    resolve_movie_playback_candidates,
    resolve_vod_profile_id,
    select_vod_playback_target,
    user_can_access_vod_kind,
)
from backend.xc_hosts import first_xc_host
from backend.xc.timeshift import (
    XC_TIMESHIFT_DATETIME_FORMATS,
    build_xc_timeshift_proxy_url,
    build_xc_timeshift_request_headers,
    detect_xc_timeshift_datetime_format,
    fetch_and_rewrite_xc_timeshift_manifest,
    parse_timeshift_timestring_with_format,
    parse_xc_stream_reference,
    persist_xc_timeshift_datetime_format,
    stream_xc_timeshift_response,
    xc_timeshift_output_extension,
)

_XC_ALLOWED_PROFILES = {"default", "mpegts", "h264-aac-mpegts"}


def _xc_rate_limited_response(message: str, retry_after: int) -> Response:
    response = jsonify({"error": message})
    response.status_code = 429
    response.headers["Retry-After"] = str(max(1, int(retry_after or 1)))
    return response


async def _xc_auth_user(username: str | None = None, password: str | None = None) -> tuple[Any, Any]:
    username = str(username or request.args.get("username") or "").strip()
    password = str(password or request.args.get("password") or "").strip()
    if not username or not password:
        return None, (jsonify({"error": "Missing username or password"}), 400)

    failure_key = f"xc:{username}:{password}"
    limiter_result = await precheck_stream_auth_limit(failure_key=failure_key, attempted_username=username)
    if not limiter_result.allowed:
        return None, _xc_rate_limited_response(
            "Too many invalid stream key attempts. Please try again later.",
            limiter_result.retry_after,
        )

    user = await get_user_by_username(username)
    user_is_active = cast(bool, user.is_active) if user is not None else False
    if user is None or not user_is_active:
        failure_result = await record_failed_stream_auth(failure_key=failure_key, attempted_username=username)
        if not failure_result.allowed:
            return None, _xc_rate_limited_response(
                "Too many invalid stream key attempts. Please try again later.",
                failure_result.retry_after,
            )
        return None, (jsonify({"error": "Unauthorized"}), 401)
    if str(user.streaming_key) != password:
        failure_result = await record_failed_stream_auth(failure_key=failure_key, attempted_username=username)
        if not failure_result.allowed:
            return None, _xc_rate_limited_response(
                "Too many invalid stream key attempts. Please try again later.",
                failure_result.retry_after,
            )
        return None, (jsonify({"error": "Unauthorized"}), 401)
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
    cached = xc_cache.get("xc_channel_map")
    if cached:
        return cached
    channel_map = {str(ch["id"]): ch for ch in await _get_enabled_channels()}
    xc_cache.set("xc_channel_map", channel_map, ttl_seconds=30)
    return channel_map


def _xc_timeshift_enabled(user) -> bool:
    return user_timeshift_enabled(user)


def _xc_encoded_text(value: str | None) -> str:
    return base64.b64encode(str(value or "").encode("utf-8")).decode("ascii")


def _format_xc_epg_datetime(timestamp_value: str | int | None) -> str:
    timestamp_int = convert_to_int(timestamp_value, None)
    if timestamp_int is None:
        return ""
    return datetime.fromtimestamp(timestamp_int, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


async def _get_enabled_xc_account_maps(playlist_ids: set[int]) -> tuple[dict[int, XcAccount], dict[int, XcAccount]]:
    if not playlist_ids:
        return {}, {}

    async with Session() as session:
        accounts_result = await session.execute(
            select(XcAccount)
            .where(XcAccount.playlist_id.in_(playlist_ids), XcAccount.enabled.is_(True))
            .order_by(XcAccount.playlist_id.asc(), XcAccount.id.asc())
        )
        accounts = accounts_result.scalars().all()

    account_by_id: dict[int, XcAccount] = {}
    primary_by_playlist: dict[int, XcAccount] = {}
    for account in accounts:
        account_by_id[int(account.id)] = account
        primary_by_playlist.setdefault(int(account.playlist_id), account)
    return account_by_id, primary_by_playlist


async def _resolve_xc_archive_sources(channels: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    playlist_ids: set[int] = set()
    name_pairs: set[tuple[int, str]] = set()
    url_pairs: set[tuple[int, str]] = set()

    for channel in channels:
        for source in channel.get("sources") or []:
            playlist_id = convert_to_int(source.get("playlist_id"), None)
            if playlist_id is None:
                continue
            playlist_ids.add(playlist_id)
            stream_name = str(source.get("stream_name") or "").strip()
            stream_url = str(source.get("stream_url") or "").strip()
            if stream_name:
                name_pairs.add((playlist_id, stream_name))
            if stream_url:
                url_pairs.add((playlist_id, stream_url))

    if not playlist_ids or (not name_pairs and not url_pairs):
        return {}

    match_clauses = []
    if name_pairs:
        match_clauses.extend(
            [
                (PlaylistStreams.playlist_id == playlist_id) & (PlaylistStreams.name == stream_name)
                for playlist_id, stream_name in name_pairs
            ]
        )
    if url_pairs:
        match_clauses.extend(
            [
                (PlaylistStreams.playlist_id == playlist_id) & (PlaylistStreams.url == stream_url)
                for playlist_id, stream_url in url_pairs
            ]
        )

    async with Session() as session:
        stream_result = await session.execute(
            select(PlaylistStreams)
            .options(joinedload(PlaylistStreams.playlist))
            .where(
                PlaylistStreams.playlist_id.in_(playlist_ids),
                PlaylistStreams.source_type == XC_ACCOUNT_TYPE,
                PlaylistStreams.xc_stream_id.is_not(None),
                or_(*match_clauses),
            )
        )
        stream_rows = stream_result.scalars().all()

    stream_map: dict[tuple[str, int, str], PlaylistStreams] = {}
    for row in stream_rows:
        playlist_id = convert_to_int(row.playlist_id, None)
        if playlist_id is None:
            continue
        row_name = str(row.name or "").strip()
        row_url = str(row.url or "").strip()
        if row_name:
            stream_map[("name", playlist_id, row_name)] = row
        if row_url:
            stream_map[("url", playlist_id, row_url)] = row

    account_by_id, primary_by_playlist = await _get_enabled_xc_account_maps(playlist_ids)

    archive_sources: Dict[str, Dict[str, Any]] = {}
    for channel in channels:
        channel_id = str(channel.get("id") or "")
        if not channel_id:
            continue
        for source in channel.get("sources") or []:
            playlist_id = convert_to_int(source.get("playlist_id"), None)
            if playlist_id is None:
                continue

            stream_row = None
            stream_url = str(source.get("stream_url") or "").strip()
            stream_name = str(source.get("stream_name") or "").strip()
            if stream_url:
                stream_row = stream_map.get(("url", playlist_id, stream_url))
            if stream_row is None and stream_name:
                stream_row = stream_map.get(("name", playlist_id, stream_name))
            if stream_row is None:
                continue

            if not bool(stream_row.xc_tv_archive) or convert_to_int(stream_row.xc_stream_id, None) is None:
                continue
            playlist = stream_row.playlist
            if playlist is None or not bool(getattr(playlist, "enabled", False)):
                continue

            source_account_id = convert_to_int(source.get("xc_account_id"), None)
            account = account_by_id.get(source_account_id) if source_account_id is not None else None
            if account is None:
                account = primary_by_playlist.get(playlist_id)
            if account is None and playlist.xc_username and playlist.xc_password:
                account = type("LegacyAccount", (), {})()
                account.id = None
                account.username = playlist.xc_username
                account.password = playlist.xc_password
            if account is None:
                continue

            archive_sources[channel_id] = {
                "playlist_id": playlist_id,
                "playlist": playlist,
                "playlist_stream_id": int(stream_row.id),
                "playlist_stream_name": stream_row.name or "",
                "upstream_stream_id": int(stream_row.xc_stream_id),
                "epg_channel_id": str(stream_row.xc_epg_channel_id or stream_row.tvg_id or "").strip(),
                "tv_archive_duration": convert_to_int(stream_row.xc_tv_archive_duration, 0),
                "account": account,
                "xc_account_id": getattr(account, "id", None),
                "host_url": first_xc_host(playlist.url),
            }
            break

    return archive_sources


async def _resolve_xc_archive_source(channel: Dict[str, Any]) -> Dict[str, Any] | None:
    if not channel:
        return None
    archive_sources = await _resolve_xc_archive_sources([channel])
    return archive_sources.get(str(channel.get("id")))


async def _get_channel_epg_rows(
    channel: Dict[str, Any], include_archive: bool, archive_duration_days: int, limit: int | None
):
    guide = channel.get("guide") or {}
    epg_id = convert_to_int(guide.get("epg_id"), None)
    guide_channel_id = str(guide.get("channel_id") or "").strip()
    if epg_id is None or not guide_channel_id:
        return []

    now_ts = int(time.time())
    min_start_ts = None
    if include_archive and archive_duration_days > 0:
        min_start_ts = now_ts - (archive_duration_days * 86400)

    async with Session() as session:
        epg_channel_row = await load_preferred_epg_channel_row(
            session,
            epg_id=int(epg_id),
            channel_id=guide_channel_id,
        )
        if not epg_channel_row:
            return []

        programme_query = select(EpgChannelProgrammes).where(
            EpgChannelProgrammes.epg_channel_id == int(epg_channel_row["epg_channel_row_id"])
        )
        if include_archive and min_start_ts is not None:
            programme_query = programme_query.where(EpgChannelProgrammes.stop_timestamp >= str(min_start_ts))
        else:
            programme_query = programme_query.where(EpgChannelProgrammes.stop_timestamp >= str(now_ts))
        programme_query = programme_query.order_by(EpgChannelProgrammes.start_timestamp.asc())
        if limit is not None and limit > 0:
            programme_query = programme_query.limit(limit)

        programme_result = await session.execute(programme_query)
        return programme_result.scalars().all()


def _build_xc_epg_payload(
    channel: Dict[str, Any],
    programme_rows: List[EpgChannelProgrammes],
    include_archive: bool,
    archive_duration_days: int,
) -> Dict[str, Any]:
    now_ts = int(time.time())
    guide = channel.get("guide") or {}
    guide_channel_id = str(guide.get("channel_id") or channel.get("id") or "").strip()
    stream_id = str(channel.get("id") or "")

    listings = []
    for programme in programme_rows:
        start_ts = convert_to_int(programme.start_timestamp, None)
        stop_ts = convert_to_int(programme.stop_timestamp, None)
        if start_ts is None or stop_ts is None:
            continue

        has_archive = 0
        if include_archive and archive_duration_days > 0 and stop_ts < now_ts:
            if stop_ts >= now_ts - (archive_duration_days * 86400):
                has_archive = 1

        listings.append(
            {
                "id": str(programme.id),
                "epg_id": str(programme.id),
                "title": _xc_encoded_text(programme.title),
                "lang": "en",
                "start": _format_xc_epg_datetime(start_ts),
                "end": _format_xc_epg_datetime(stop_ts),
                "description": _xc_encoded_text(programme.desc),
                "channel_id": guide_channel_id,
                "start_timestamp": str(start_ts),
                "stop_timestamp": str(stop_ts),
                "stream_id": stream_id,
                "now_playing": 1 if start_ts <= now_ts < stop_ts else 0,
                "has_archive": has_archive,
            }
        )

    return {"epg_listings": listings}


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
    cached = xc_cache.get("xc_max_connections")
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
    xc_cache.set("xc_max_connections", value, ttl_seconds=60)
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
            "max_connections": xc_cache.get("xc_max_connections") or "1",
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
        info["categories"] = {"live": xc_cache.get("xc_categories") or []}
    return info


def _xc_vod_allowed(user, kind: str) -> bool:
    return user_can_access_vod_kind(user, kind)


def _xc_requested_vod_profile(ext: str | None = None) -> str | None:
    """Return the XC VOD output override from the requested path extension.

    Note that XC clients select output format through the path suffix, not a
    TIC-specific `?profile=...` query override. If no extension is present, the route
    falls back to candidate-derived profile resolution with
    `resolve_vod_profile_id(...)`.
    """
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
    return None


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
    deadline = time.time() + 40.0
    while time.time() < deadline:
        await output_session.touch_client(connection_id)
        playlist_text = await output_session.read_playlist_text()
        if playlist_text and "#EXTM3U" in str(playlist_text):
            return playlist_text
        await asyncio.sleep(0.2)
    return ""


def _rewrite_hls_playlist(playlist_text: str, segment_base_path: str, query_string: str = "") -> str:
    if "#EXTM3U" not in str(playlist_text or ""):
        return ""
    lines = []
    suffix = f"?{query_string}" if query_string else ""
    for raw_line in str(playlist_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            if line.startswith("#EXT-X-MAP:") and 'URI="' in raw_line:
                prefix, remainder = raw_line.split('URI="', 1)
                uri_value, quote_suffix, tail = remainder.partition('"')
                map_name = uri_value.split("?", 1)[0].strip().split("/")[-1]
                raw_line = f'{prefix}URI="{segment_base_path.rstrip("/")}/{map_name}{suffix}"{tail}'
            lines.append(raw_line)
            continue
        segment_name = line.split("?", 1)[0]
        lines.append(f"{segment_base_path.rstrip('/')}/{segment_name}{suffix}")
    return "\n".join(lines) + "\n"


def _combined_cso_enabled() -> bool:
    settings = current_app.config["APP_CONFIG"].read_settings()
    return bool((settings.get("settings") or {}).get("route_playlists_through_cso", True))


@blueprint.route("/get.php", methods=["GET"])
async def xc_get():
    user, error = await _xc_auth_user()
    if error:
        return error
    await audit_stream_event(user, "xc_get", request.path)

    cache_key = f"xc_m3u:{user.id}:ts_only"
    cached = xc_cache.get(cache_key)
    if cached:
        return Response(cached, mimetype="text/plain")

    channels = await _get_enabled_channels()
    categories, _ = _build_category_map(channels)
    xc_cache.set("xc_categories", categories, ttl_seconds=60)

    base_url = get_request_base_url(request)
    epg_url = f"{base_url}/xmltv.php?username={user.username}&password={user.streaming_key}"

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
    xc_cache.set(cache_key, content, ttl_seconds=30)
    return Response(content, mimetype="text/plain")


@blueprint.route("/xmltv.php", methods=["GET"])
async def xc_xmltv():
    user, error = await _xc_auth_user()
    if error:
        return error
    await audit_stream_event(user, "xc_xmltv", request.path)
    return await build_xmltv_response(sanitise_unicode=True)


@blueprint.route("/player_api.php", methods=["GET"])
async def xc_player_api():
    user, error = await _xc_auth_user()
    if error:
        return error
    await audit_stream_event(user, "xc_player_api", request.path)

    action = request.args.get("action")
    channels = await _get_enabled_channels()
    categories = xc_cache.get("xc_categories")
    name_to_id = xc_cache.get("xc_category_map")
    if not categories or not name_to_id:
        categories, name_to_id = _build_category_map(channels)
        xc_cache.set("xc_categories", categories, ttl_seconds=60)
        xc_cache.set("xc_category_map", name_to_id, ttl_seconds=60)
    xc_cache.set("xc_max_connections", await _get_max_connections(), ttl_seconds=60)

    if action == "get_live_categories":
        return jsonify(categories)
    if action == "get_live_streams":
        cache_key = f"xc_live_streams:{int(_xc_timeshift_enabled(user))}"
        cached_streams = xc_cache.get(cache_key)
        if cached_streams:
            return jsonify(cached_streams)
        archive_sources = await _resolve_xc_archive_sources(channels) if _xc_timeshift_enabled(user) else {}
        stream_list = []
        for channel in channels:
            group_title = (channel.get("tags") or ["Uncategorized"])[0]
            category_id = name_to_id.get(group_title, "1")
            archive_source = archive_sources.get(str(channel.get("id")))
            stream_list.append(
                {
                    "num": channel.get("number") or 0,
                    "name": channel.get("name") or "",
                    "stream_id": str(channel["id"]),
                    "stream_type": "live",
                    "stream_icon": channel.get("logo_url") or "",
                    "category_id": category_id,
                    "epg_channel_id": str((channel.get("guide") or {}).get("channel_id") or "") or None,
                    "tv_archive": 1 if archive_source else 0,
                    "tv_archive_duration": int(archive_source.get("tv_archive_duration") or 0) if archive_source else 0,
                }
            )
        xc_cache.set(cache_key, stream_list, ttl_seconds=30)
        return jsonify(stream_list)
    if action in ("get_short_epg", "get_simple_data_table"):
        stream_id = str(request.args.get("stream_id") or request.args.get("channel_id") or "").strip()
        if not stream_id:
            return jsonify({"epg_listings": []})

        channel_map = await _get_channel_map()
        channel = channel_map.get(stream_id)
        if not channel:
            return jsonify({"epg_listings": []})

        include_archive = False
        archive_duration_days = 0
        if action == "get_simple_data_table" and _xc_timeshift_enabled(user):
            archive_source = await _resolve_xc_archive_source(channel)
            if archive_source:
                include_archive = True
                archive_duration_days = int(archive_source.get("tv_archive_duration") or 0)

        limit = convert_to_int(request.args.get("limit"), None)
        if action == "get_short_epg" and (limit is None or limit <= 0):
            limit = 4

        programme_rows = await _get_channel_epg_rows(
            channel,
            include_archive=include_archive,
            archive_duration_days=archive_duration_days,
            limit=limit,
        )
        return jsonify(_build_xc_epg_payload(channel, programme_rows, include_archive, archive_duration_days))
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
        return error
    await audit_stream_event(user, "xc_panel_api", request.path)

    xc_cache.set("xc_max_connections", await _get_max_connections(), ttl_seconds=60)
    info = _build_xc_server_info(user, include_categories=True)
    return jsonify(info)


@blueprint.route("/live/<username>/<password>/<stream_id>", methods=["GET"])
@blueprint.route("/live/<username>/<password>/<stream_id>.<ext>", methods=["GET"])
@blueprint.route("/<username>/<password>/<stream_id>", methods=["GET"])
@blueprint.route("/<username>/<password>/<stream_id>.<ext>", methods=["GET"])
async def xc_stream(username: str, password: str, stream_id: str, ext: str | None = None):
    user, error = await _xc_auth_user(username, password)
    if error:
        return error
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


async def _xc_timeshift_response(
    username: str,
    password: str,
    duration: str,
    timestamp: str,
    stream_id: str,
    ext: str | None = None,
):
    user, error = await _xc_auth_user(username, password)
    if error:
        return error
    await audit_stream_event(user, "xc_timeshift", request.path)
    if not _xc_timeshift_enabled(user):
        return forbidden_response("Timeshift access is disabled for this user")

    channel_map = await _get_channel_map()
    channel = channel_map.get(str(stream_id))
    if not channel:
        return jsonify({"error": "Not found"}), 404

    archive_source = await _resolve_xc_archive_source(channel)
    if not archive_source:
        return jsonify({"error": "Timeshift not supported for this channel"}), 404

    upstream_duration = max(1, convert_to_int(duration, 0))
    upstream_stream_id = archive_source["upstream_stream_id"]
    playlist = archive_source["playlist"]
    account = archive_source["account"]
    host_url = str(archive_source.get("host_url") or "").rstrip("/")
    if not host_url:
        return jsonify({"error": "Stream unavailable"}), 404

    parsed_timestamp = parse_timeshift_timestring_with_format(timestamp)
    if parsed_timestamp is None:
        return jsonify({"error": "Invalid timeshift timestamp"}), 400

    output_extension = xc_timeshift_output_extension(ext or getattr(playlist, "xc_live_stream_format", None), stream_id)
    if output_extension is None:
        return jsonify({"error": "Unsupported output format"}), 401
    request_headers = build_xc_timeshift_request_headers(playlist)

    preferred_datetime_format = await detect_xc_timeshift_datetime_format(archive_source, request_headers)
    connection_id = _get_connection_id()
    identity = f"/xc/timeshift/{int(channel['id'])}"
    request_client_ip = get_request_client_ip()
    request_user_agent = request.headers.get("User-Agent")
    request_base_url = get_request_base_url(request)
    instance_id = current_app.config["APP_CONFIG"].ensure_instance_id()
    candidate_datetime_formats: list[str] = []
    for candidate_format in (preferred_datetime_format, *XC_TIMESHIFT_DATETIME_FORMATS):
        if candidate_format and candidate_format not in candidate_datetime_formats:
            candidate_datetime_formats.append(candidate_format)

    last_response = None
    for candidate_format in candidate_datetime_formats:
        candidate_timestamp = parsed_timestamp.strftime(candidate_format)
        path_url = (
            f"{host_url}/timeshift/{account.username}/{account.password}/{upstream_duration}/{candidate_timestamp}/"
            f"{upstream_stream_id}.{output_extension}"
        )

        if output_extension == "m3u8":
            response = await fetch_and_rewrite_xc_timeshift_manifest(
                path_url,
                playlist,
                request_base_url,
                instance_id,
                user.streaming_key,
                user.username,
                request_headers,
                connection_id,
            )
        else:
            proxy_url = build_xc_timeshift_proxy_url(
                playlist,
                path_url,
                request_base_url,
                instance_id,
                user.streaming_key,
                user.username,
                request_headers,
                force_internal_hls_proxy=False,
                prefer_stream_endpoint=False,
            )
            current_app.logger.warning("XC upstream request: %s", proxy_url)
            response = await stream_xc_timeshift_response(
                proxy_url if proxy_url != path_url else path_url,
                request_headers,
                identity,
                connection_id,
                user,
                request_client_ip,
                request_user_agent,
            )

        if response.status_code == 404:
            last_response = response
            continue
        if response.status_code >= 400:
            return response

        await upsert_stream_activity(
            identity,
            connection_id=connection_id,
            endpoint_override=identity,
            user=user,
            ip_address=request_client_ip,
            user_agent=request_user_agent,
            perform_audit=False,
            channel_id=channel.get("id"),
            channel_name=channel.get("name"),
            channel_logo_url=channel.get("logo_url"),
            stream_name=f"{channel.get('name') or ''} (timeshift)".strip(),
            source_url=path_url,
            display_url=request.path,
            source_id=archive_source.get("playlist_stream_id"),
            playlist_id=archive_source.get("playlist_id"),
            xc_account_id=archive_source.get("xc_account_id"),
            enrich_metadata=False,
        )
        if candidate_format != preferred_datetime_format:
            await persist_xc_timeshift_datetime_format(archive_source.get("playlist_id", ""), candidate_format)
        return response

    if last_response is not None:
        return last_response
    return jsonify({"error": "Timeshift content not found"}), 404


@blueprint.route("/timeshift/<username>/<password>/<duration>/<timestamp>/<stream_id>", methods=["GET"])
@blueprint.route("/timeshift/<username>/<password>/<duration>/<timestamp>/<stream_id>.<ext>", methods=["GET"])
async def xc_timeshift_stream(
    username: str, password: str, duration: str, timestamp: str, stream_id: str, ext: str | None = None
):
    current_app.logger.warning("XC timeshift path request: %s", request.full_path)
    return await _xc_timeshift_response(username, password, duration, timestamp, stream_id, ext=ext)


@blueprint.route("/streaming/timeshift.php", methods=["GET"])
async def xc_timeshift_stream_query():
    current_app.logger.warning("XC timeshift query request: %s", request.full_path)
    username = str(request.args.get("username") or "").strip()
    password = str(request.args.get("password") or "").strip()
    duration = str(request.args.get("duration") or "").strip()
    timestamp = str(request.args.get("start") or request.args.get("timestamp") or "").strip()
    stream_id, ext = parse_xc_stream_reference(
        request.args.get("stream") or request.args.get("stream_id"),
        request.args.get("extension") or request.args.get("ext"),
    )
    return await _xc_timeshift_response(username, password, duration, timestamp, stream_id, ext=ext)


@blueprint.route("/movie/<username>/<password>/<item_id>", methods=["GET"])
@blueprint.route("/movie/<username>/<password>/<item_id>.<ext>", methods=["GET"])
async def xc_movie_stream(username: str, password: str, item_id: str, ext: str | None = None):
    user, error = await _xc_auth_user(username, password)
    if error:
        return error
    stream_key = user.streaming_key
    if not _xc_vod_allowed(user, VOD_KIND_MOVIE):
        return jsonify({"error": "Forbidden"}), 403
    candidates = await resolve_movie_playback_candidates(int(item_id))
    if not candidates:
        return jsonify({"error": "Not found"}), 404
    # Start by checking for any cached copies of a candidate. If any are found, we will use that one
    cached_candidate = await find_cached_vod_playback_candidate(candidates)
    if cached_candidate is not None:
        candidate = cached_candidate
        upstream_url = ""
        selection_error = None
    else:
        # Find the next available candidate within the list of candidates by order of priority
        candidate, upstream_url, selection_error = await select_vod_playback_target(candidates)

    # Get/Create the connection ID
    connection_id = _get_connection_id()

    # Check for any errors and return if we cannot playback a candidate
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
    profile = _xc_requested_vod_profile(ext=ext)
    if not profile:
        profile = resolve_vod_profile_id(candidate)
    use_proxy_session = should_use_vod_proxy_session(candidate, profile)
    if profile == "hls":
        return redirect(
            f"{request_base_url}/movie/{username}/{password}/{int(item_id)}/hls/{connection_id}/index.m3u8",
            code=302,
        )
    if is_hls_stream_profile(profile):
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
async def xc_series_episode_stream(username: str, password: str, episode_id: str, ext: str | None = None):
    user, error = await _xc_auth_user(username, password)
    if error:
        return error
    stream_key = user.streaming_key
    if not _xc_vod_allowed(user, VOD_KIND_SERIES):
        return jsonify({"error": "Forbidden"}), 403
    candidates, episode_map = await resolve_episode_playback_candidates(int(episode_id))
    if not candidates or episode_map is None:
        return jsonify({"error": "Not found"}), 404

    # Start by checking for any cached copies of a candidate. If any are found, we will use that one
    cached_candidate = await find_cached_vod_playback_candidate(candidates, episode=episode_map)
    if cached_candidate is not None:
        candidate = cached_candidate
        upstream_url = ""
        selection_error = None
    else:
        # Find the next available candidate within the list of candidates by order of priority
        candidate, upstream_url, selection_error = await select_vod_playback_target(candidates, episode=episode_map)

    # Get/Create the connection ID
    connection_id = _get_connection_id()

    # Check for any errors and return if we cannot playback a candidate
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
    profile = _xc_requested_vod_profile(ext=ext)
    if not profile:
        profile = resolve_vod_profile_id(candidate)
    use_proxy_session = should_use_vod_proxy_session(candidate, profile)
    if profile == "hls":
        return redirect(
            f"{request_base_url}/series/{username}/{password}/{int(episode_id)}/hls/{connection_id}/index.m3u8",
            code=302,
        )
    if is_hls_stream_profile(profile):
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
    user, error = await _xc_auth_user(username, password)
    if error:
        return error
    stream_key = user.streaming_key
    candidates = await resolve_movie_playback_candidates(int(item_id))
    if not candidates:
        return jsonify({"error": "Not found"}), 404
    candidate, upstream_url, selection_error = await select_vod_playback_target(candidates)
    if not candidate:
        return jsonify({"error": "Not found"}), 404
    if not upstream_url:
        return Response(
            "Source capacity limit reached" if selection_error == "capacity_blocked" else "Stream unavailable",
            status=503 if selection_error == "capacity_blocked" else 404,
        )

    profile = "hls"
    config = current_app.config["APP_CONFIG"]
    output_session, error_message, status = await subscribe_vod_hls(
        config,
        candidate,
        upstream_url,
        stream_key,
        profile,
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
        query_string="",
    )
    if not playlist_text:
        return Response("HLS playlist not ready", status=503)
    return Response(playlist_text, content_type="application/vnd.apple.mpegurl")


@blueprint.route("/movie/<username>/<password>/<int:item_id>/hls/<connection_id>/<segment_name>", methods=["GET"])
async def xc_movie_hls_segment(username: str, password: str, item_id: int, connection_id: str, segment_name: str):
    user, error = await _xc_auth_user(username, password)
    if error:
        return error
    stream_key = user.streaming_key
    candidates = await resolve_movie_playback_candidates(int(item_id))
    if not candidates:
        return jsonify({"error": "Not found"}), 404
    candidate, upstream_url, selection_error = await select_vod_playback_target(candidates)
    if not candidate:
        return jsonify({"error": "Not found"}), 404
    if not upstream_url:
        return Response(
            "Source capacity limit reached" if selection_error == "capacity_blocked" else "Stream unavailable",
            status=503 if selection_error == "capacity_blocked" else 404,
        )

    profile = "hls"
    config = current_app.config["APP_CONFIG"]
    output_session, error_message, status = await subscribe_vod_hls(
        config,
        candidate,
        upstream_url,
        stream_key,
        profile,
        connection_id,
        request_base_url=get_request_base_url(request),
    )
    if not output_session:
        return Response(error_message or "Unable to start CSO HLS stream", status=status or 503)
    await output_session.touch_client(connection_id)
    payload = await output_session.read_segment_bytes(segment_name)
    return Response(payload or b"", content_type=content_type_for_media_path(segment_name))


@blueprint.route("/series/<username>/<password>/<int:episode_id>/hls/<connection_id>/index.m3u8", methods=["GET"])
async def xc_series_hls_playlist(username: str, password: str, episode_id: int, connection_id: str):
    user, error = await _xc_auth_user(username, password)
    if error:
        return error
    stream_key = user.streaming_key
    candidates, episode_map = await resolve_episode_playback_candidates(int(episode_id))
    if not candidates or episode_map is None:
        return jsonify({"error": "Not found"}), 404
    candidate, upstream_url, selection_error = await select_vod_playback_target(candidates, episode=episode_map)
    if not candidate:
        return jsonify({"error": "Not found"}), 404
    if not upstream_url:
        return Response(
            "Source capacity limit reached" if selection_error == "capacity_blocked" else "Stream unavailable",
            status=503 if selection_error == "capacity_blocked" else 404,
        )

    profile = "hls"
    config = current_app.config["APP_CONFIG"]
    output_session, error_message, status = await subscribe_vod_hls(
        config,
        candidate,
        upstream_url,
        stream_key,
        profile,
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
        query_string="",
    )
    if not playlist_text:
        return Response("HLS playlist not ready", status=503)
    return Response(playlist_text, content_type="application/vnd.apple.mpegurl")


@blueprint.route("/series/<username>/<password>/<int:episode_id>/hls/<connection_id>/<segment_name>", methods=["GET"])
async def xc_series_hls_segment(username: str, password: str, episode_id: int, connection_id: str, segment_name: str):
    user, error = await _xc_auth_user(username, password)
    if error:
        return error
    stream_key = user.streaming_key
    candidates, episode_map = await resolve_episode_playback_candidates(int(episode_id))
    if not candidates or episode_map is None:
        return jsonify({"error": "Not found"}), 404
    candidate, upstream_url, selection_error = await select_vod_playback_target(candidates, episode=episode_map)
    if not candidate:
        return jsonify({"error": "Not found"}), 404
    if not upstream_url:
        return Response(
            "Source capacity limit reached" if selection_error == "capacity_blocked" else "Stream unavailable",
            status=503 if selection_error == "capacity_blocked" else 404,
        )

    profile = "hls"
    config = current_app.config["APP_CONFIG"]
    output_session, error_message, status = await subscribe_vod_hls(
        config,
        candidate,
        upstream_url,
        stream_key,
        profile,
        connection_id,
        episode=episode_map,
        request_base_url=get_request_base_url(request),
    )
    if not output_session:
        return Response(error_message or "Unable to start CSO HLS stream", status=status or 503)
    await output_session.touch_client(connection_id)
    payload = await output_session.read_segment_bytes(segment_name)
    return Response(payload or b"", content_type=content_type_for_media_path(segment_name))
