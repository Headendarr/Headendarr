#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import asyncio
import logging
import hashlib
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from flask import request
from quart import Response, current_app, redirect, stream_with_context
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from backend.api import blueprint
from backend.auth import (
    get_request_client_ip,
    get_request_stream_key,
    get_request_stream_user,
    skip_stream_connect_audit,
    stream_key_required,
)
from backend.cso import (
    cso_capacity_registry,
    cso_session_manager,
    emit_channel_stream_event,
    is_internal_cso_activity,
    order_cso_channel_sources,
    policy_content_type,
    resolve_channel_for_stream,
    subscribe_slate_stream,
    subscribe_slate_hls,
    subscribe_channel_hls,
    subscribe_channel_stream,
    subscribe_source_hls,
    subscribe_source_stream,
    subscribe_vod_channel_hls,
    subscribe_vod_hls,
    subscribe_vod_ingest_stream,
    subscribe_vod_proxy_stream,
    subscribe_vod_stream,
    source_capacity_key,
    source_capacity_limit,
    should_use_vod_proxy_session,
)
from backend.models import ChannelSource, Session, XcVodItem
from backend.hls_multiplexer import parse_size
from backend.stream_activity import (
    get_stream_activity_snapshot,
    stop_stream_activity,
    touch_stream_activity,
    upsert_stream_activity,
)
from backend.stream_profiles import (
    content_type_for_media_path,
    generate_cso_policy_from_profile,
    resolve_cso_profile_name,
)
from backend.stream_profiles import parse_stream_profile_request
from backend.streaming import parse_local_proxy_url
from backend.url_resolver import get_request_base_url
from backend.utils import convert_to_int, int_or_none
from backend.vod import (
    VOD_KIND_MOVIE,
    VOD_KIND_SERIES,
    VodCuratedPlaybackCandidate,
    VodSourcePlaybackCandidate,
    build_vod_activity_metadata,
    find_cached_vod_playback_candidate,
    require_vod_content_type,
    resolve_episode_playback_candidates,
    resolve_movie_playback_candidates,
    resolve_upstream_vod_profile_id,
    resolve_vod_profile_id,
    resolve_xc_item_upstream_url,
    select_vod_playback_target,
)
from backend.vod_channels import is_vod_channel_type, subscribe_vod_channel_stream
from backend.channel_stream_health import (
    cancel_background_health_checks_for_capacity_key,
    has_background_health_check_for_capacity_key,
    preempt_background_health_checks_for_channel,
)

CONNECTION_LIMIT_REACHED_MESSAGE = "Channel unavailable due to connection limits"
logger = logging.getLogger("cso.api")


def _get_connection_id() -> str:
    value = (request.args.get("connection_id") or request.args.get("cid") or "").strip()
    if value:
        return value
    return uuid.uuid4().hex


def _response_from_cso_plan(plan: Any, fallback_message: str, fallback_status: int = 503) -> Response:
    if plan.generator is None:
        return Response(fallback_message, status=int(plan.status_code or fallback_status))
    response = Response(
        plan.generator, content_type=plan.content_type or "application/octet-stream", status=plan.status_code
    )
    response.timeout = None
    return response


async def _iter_cso_plan_generator(plan: Any, connection_id: str, touch_identity: str) -> AsyncIterator[bytes]:
    last_touch_ts = time.time()
    cutoff_deadline = None
    if plan.cutoff_seconds is not None:
        cutoff_deadline = time.time() + max(0, int(plan.cutoff_seconds))
    try:
        while True:
            if cutoff_deadline is None:
                chunk = await plan.generator.__anext__()
            else:
                remaining_seconds = cutoff_deadline - time.time()
                if remaining_seconds <= 0:
                    break
                chunk = await asyncio.wait_for(plan.generator.__anext__(), timeout=remaining_seconds)
            now = time.time()
            if (now - last_touch_ts) >= 5.0:
                await touch_stream_activity(connection_id, identity=touch_identity)
                last_touch_ts = now
            yield chunk
    except StopAsyncIteration:
        return
    except asyncio.TimeoutError:
        logger.info(
            "CSO stream plan cutoff reached connection_id=%s identity=%s cutoff_seconds=%s",
            connection_id,
            touch_identity,
            plan.cutoff_seconds,
        )
        return


def _client_fingerprint(stream_key, ip_address, user_agent):
    payload = f"{str(stream_key or '').strip()}|{str(ip_address or '').strip()}|{str(user_agent or '').strip()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _session_capacity_key(session):
    if not isinstance(session, dict):
        return None
    xc_account_id = session.get("xc_account_id")
    playlist_id = session.get("playlist_id")
    source_id = session.get("source_id")
    if xc_account_id:
        return f"xc:{int(xc_account_id)}"
    if playlist_id:
        return f"playlist:{int(playlist_id)}"
    if source_id:
        return f"source:{int(source_id)}"
    return None


def _same_client_session_for_capacity(
    activity_sessions, *, capacity_key_name, client_fingerprint, exclude_connection_id=None
):
    for session in activity_sessions or []:
        if not isinstance(session, dict):
            continue
        if exclude_connection_id and str(session.get("connection_id") or "") == str(exclude_connection_id):
            continue
        session_capacity_key = _session_capacity_key(session)
        if session_capacity_key != capacity_key_name:
            continue
        session_fingerprint = _client_fingerprint(
            session.get("stream_key"),
            session.get("ip_address"),
            session.get("user_agent"),
        )
        if session_fingerprint == client_fingerprint:
            return True
    return False


async def _wait_for_source_handover_window(
    *,
    source,
    capacity_key_name,
    capacity_limit,
    client_fingerprint,
    exclude_connection_id,
    timeout_seconds=12.0,
    poll_interval_seconds=0.5,
):
    deadline = time.time() + float(timeout_seconds)
    while time.time() < deadline:
        usage = await cso_capacity_registry.get_usage(capacity_key_name)
        activity_sessions = await get_stream_activity_snapshot()
        external_count = _active_external_count_for_source(activity_sessions, source, capacity_key_name)
        active_connections = int(usage.get("allocations") or 0) + int(external_count)
        if active_connections < int(capacity_limit):
            return True
        if not _same_client_session_for_capacity(
            activity_sessions,
            capacity_key_name=capacity_key_name,
            client_fingerprint=client_fingerprint,
            exclude_connection_id=exclude_connection_id,
        ):
            return False
        await asyncio.sleep(float(poll_interval_seconds))
    return False


async def _select_primary_source_for_capacity(channel):
    if not channel:
        return None

    channel_id = int(channel if isinstance(channel, int) else (getattr(channel, "id", 0) or 0))
    if channel_id <= 0:
        return None

    sources = []
    if not isinstance(channel, int):
        # Avoid lazy-loading on detached channel instances.
        sources = list((getattr(channel, "__dict__", {}) or {}).get("sources") or [])

    if not sources:
        async with Session() as session:
            result = await session.execute(
                select(ChannelSource)
                .options(
                    joinedload(ChannelSource.playlist),
                    joinedload(ChannelSource.xc_account),
                )
                .where(ChannelSource.channel_id == channel_id)
            )
            sources = list(result.scalars().all())

    candidates = await order_cso_channel_sources(sources, channel_id=channel_id)
    for source in candidates:
        playlist = getattr(source, "playlist", None)
        if playlist is not None and not bool(getattr(playlist, "enabled", False)):
            continue
        stream_url = str(getattr(source, "playlist_stream_url", "") or "").strip()
        if not stream_url:
            continue
        return source
    return None


def _resolve_cso_profile_for_source_request(config, requested_profile=None):
    return resolve_cso_profile_name(
        config,
        requested_profile=requested_profile,
        channel=None,
    )


def _append_or_replace_query_param(url, key, value):
    parsed = urlparse(str(url or ""))
    query_items = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != key]
    query_items.append((str(key), str(value)))
    return urlunparse(parsed._replace(query=urlencode(query_items)))


def _build_hls_query_string(
    stream_key: str = None,
    username: str = None,
    profile: str = None,
    start_seconds: int = 0,
    container_extension: str = None,
):
    query_items = []
    if stream_key:
        query_items.append(("stream_key", str(stream_key)))
    if username:
        query_items.append(("username", str(username)))
    if profile:
        query_items.append(("profile", str(profile)))
    if container_extension:
        query_items.append(("container_extension", str(container_extension)))
    if start_seconds is not None and int_or_none(start_seconds) > 0:
        query_items.append(("start", str(start_seconds)))
    if not query_items:
        return ""
    return urlencode(query_items)


def _requested_hls_start_seconds() -> int:
    value = int_or_none(request.args.get("start"))
    if value is None:
        value = int_or_none(request.args.get("start_seconds"))
    return max(0, int(value or 0))


def _is_restartable_vod_output_profile(profile_id: str) -> bool:
    if not str(profile_id or "").strip():
        return False
    parsed_profile = parse_stream_profile_request(profile_id)
    clean_profile = str(parsed_profile["profile_id"] or "").strip().lower()
    if not clean_profile:
        return False
    return clean_profile not in {"hls", "mpegts", "matroska", "mp4", "webm"}


def _render_hls_playlist(playlist_text: str, segment_base_path: str, query_string: str = "") -> str:
    lines = []
    suffix = f"?{query_string}" if query_string else ""
    for raw_line in str(playlist_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#EXT-X-MAP:"):
            if 'URI="' in raw_line:
                prefix, remainder = raw_line.split('URI="', 1)
                map_name, suffix_part = remainder.split('"', 1)
                map_target = f"{segment_base_path.rstrip('/')}/{map_name.split('?', 1)[0]}{suffix}"
                lines.append(f'{prefix}URI="{map_target}"{suffix_part}')
                continue
        if line.startswith("#"):
            lines.append(raw_line)
            continue
        segment_name = line.split("?", 1)[0]
        lines.append(f"{segment_base_path.rstrip('/')}/{segment_name}{suffix}")
    return "\n".join(lines) + "\n"


def _resolve_requested_vod_profile(config, default_profile: str) -> str:
    requested = parse_stream_profile_request(request.args.get("profile"))
    if not requested["profile_id"]:
        return str(default_profile or "").strip().lower()
    resolved_profile = resolve_cso_profile_name(config, requested_profile=requested["profile_id"], channel=None)
    if resolved_profile != requested["profile_id"]:
        return str(default_profile or "").strip().lower()
    return requested["raw"] or resolved_profile


def _upstream_vod_activity_metadata(candidate: VodSourcePlaybackCandidate, item_id: int) -> dict[str, str]:
    source_item = getattr(candidate, "source_item", None)
    title = str(getattr(source_item, "title", "") or "").strip() or f"Upstream VOD {int(item_id)}"
    poster_url = str(getattr(source_item, "poster_url", "") or "").strip()
    label = "Movie" if str(candidate.content_type or "").strip().lower() == VOD_KIND_MOVIE else "Series episode"
    return {
        "channel_name": title,
        "channel_logo_url": poster_url,
        "stream_name": title,
        "display_url": f"Upstream VOD {label}: {title}",
    }


async def _resolve_curated_vod_request(config, stream_type: str, item_id: int) -> dict[str, object]:
    resolved_type = require_vod_content_type(stream_type)
    episode = None
    if resolved_type == VOD_KIND_MOVIE:
        candidates = await resolve_movie_playback_candidates(int(item_id))
    else:
        candidates, episode = await resolve_episode_playback_candidates(int(item_id))
    if not candidates:
        return {"error": "Not found", "status": 404}

    default_profile = resolve_vod_profile_id(candidates[0])
    effective_profile = _resolve_requested_vod_profile(config, default_profile)
    cached_candidate = await find_cached_vod_playback_candidate(candidates, episode=episode)
    if cached_candidate is not None:
        candidate = cached_candidate
        upstream_url = ""
        selection_error = None
    else:
        candidate, upstream_url, selection_error = await select_vod_playback_target(candidates, episode=episode)
    if candidate is None:
        return {"error": "Not found", "status": 404}
    use_proxy_session = should_use_vod_proxy_session(candidate, effective_profile)
    if not upstream_url and selection_error == "capacity_blocked":
        return {"error": "Source capacity limit reached", "status": 503}
    if not upstream_url and cached_candidate is None:
        return {"error": "Stream unavailable", "status": 404}
    return {
        "candidate": candidate,
        "episode": episode,
        "upstream_url": upstream_url,
        "effective_profile": effective_profile,
        "use_proxy_session": bool(use_proxy_session),
        "activity_metadata": build_vod_activity_metadata(candidate, episode=episode),
    }


async def _resolve_upstream_vod_request(
    config,
    source_id: int,
    stream_type: str,
    item_id: int,
    upstream_episode_id: str | None = None,
) -> dict[str, object]:
    resolved_type = require_vod_content_type(stream_type)
    container_extension = str(request.args.get("container_extension") or "").strip()
    async with Session() as session:
        source_item = await session.get(XcVodItem, int(item_id))
    if source_item is None or int(getattr(source_item, "playlist_id", 0) or 0) != int(source_id):
        return {"error": "Upstream VOD item was not found for the selected source", "status": 404}

    resolved_episode_id = str(upstream_episode_id or "").strip() if resolved_type == VOD_KIND_SERIES else None
    source_item, upstream_url, account, error_message = await resolve_xc_item_upstream_url(
        int(item_id),
        resolved_type,
        upstream_episode_id=resolved_episode_id,
        container_extension=container_extension,
    )
    if error_message or source_item is None or not upstream_url:
        return {"error": error_message or "Stream unavailable", "status": 404}

    candidate = VodSourcePlaybackCandidate(
        source_item=source_item,
        content_type=resolved_type,
        xc_account=account,
        host_url="",
        container_extension=container_extension or getattr(source_item, "container_extension", ""),
        upstream_episode_id=resolved_episode_id,
        internal_id=None,
        cache_internal_id=(
            convert_to_int(resolved_episode_id, default=0) or int(getattr(source_item, "id", 0) or 0) or None
        )
        if resolved_type == VOD_KIND_SERIES
        else int(getattr(source_item, "id", 0) or 0) or None,
    )
    requested_profile = parse_stream_profile_request(request.args.get("profile"))
    if not requested_profile["profile_id"]:
        effective_profile = ""
        use_proxy_session = True
    else:
        default_profile = resolve_upstream_vod_profile_id(source_item, container_extension=container_extension)
        effective_profile = _resolve_requested_vod_profile(config, default_profile)
        use_proxy_session = should_use_vod_proxy_session(candidate, effective_profile)
    return {
        "candidate": candidate,
        "episode": None,
        "upstream_url": upstream_url,
        "effective_profile": effective_profile,
        "use_proxy_session": bool(use_proxy_session),
        "activity_metadata": _upstream_vod_activity_metadata(candidate, item_id),
    }


async def _hls_output_has_client(output_session_key: str, connection_id: str) -> bool:
    session = await cso_session_manager.get_output_session(output_session_key)
    if not session:
        return False
    prune_hook = getattr(session, "prune_idle_clients", None)
    if callable(prune_hook):
        try:
            await prune_hook(time.time())
        except Exception as exc:
            logger.warning("CSO HLS idle-prune failed output_key=%s error=%s", output_session_key, exc)
    has_client_hook = getattr(session, "has_client", None)
    if callable(has_client_hook):
        try:
            return bool(await has_client_hook(connection_id))
        except Exception:
            return False
    return False


def _same_origin(target_url: str, request_base_url: str) -> bool:
    parsed_target = urlparse(str(target_url or ""))
    parsed_base = urlparse(str(request_base_url or ""))
    if not parsed_target.scheme and not parsed_target.netloc:
        return True
    return (
        str(parsed_target.scheme or "").lower() == str(parsed_base.scheme or "").lower()
        and str(parsed_target.netloc or "").lower() == str(parsed_base.netloc or "").lower()
    )


def _monitorable_target_url(target_url: str, request_base_url: str, instance_id: str) -> bool:
    if not _same_origin(target_url, request_base_url):
        return False
    path = str(urlparse(str(target_url or "")).path or "")
    return path.startswith(f"/tic-hls-proxy/{instance_id}/") or path.startswith("/tic-api/tvh_stream/stream/channel/")


def _active_external_count_for_source(activity_sessions, source, capacity_key_name: str) -> int:
    count = 0
    expected_playlist_key = f"playlist:{int(source.playlist_id)}" if getattr(source, "playlist_id", None) else None
    expected_source_key = f"source:{int(source.id)}"
    expected_xc_key = f"xc:{int(source.xc_account_id)}" if getattr(source, "xc_account_id", None) else None

    for session in activity_sessions or []:
        if not isinstance(session, dict):
            continue
        endpoint = str(session.get("endpoint") or "")
        display_url = str(session.get("display_url") or "").lower()
        if is_internal_cso_activity(endpoint, display_url):
            continue

        session_key = None
        xc_account_id = session.get("xc_account_id")
        playlist_id = session.get("playlist_id")
        source_id = session.get("source_id")
        if xc_account_id:
            session_key = f"xc:{int(xc_account_id)}"
        elif playlist_id:
            session_key = f"playlist:{int(playlist_id)}"
        elif source_id:
            session_key = f"source:{int(source_id)}"

        if not session_key:
            continue
        if session_key != capacity_key_name:
            continue
        if expected_xc_key and session_key == expected_xc_key:
            count += 1
            continue
        if expected_playlist_key and session_key == expected_playlist_key:
            count += 1
            continue
        if session_key == expected_source_key:
            count += 1
    return count


def _build_target_url(
    *,
    source,
    request_base_url,
    instance_id,
    stream_key=None,
    username=None,
):
    source_url = str(getattr(source, "playlist_stream_url", "") or "").strip()
    if not source_url:
        return ""

    return parse_local_proxy_url(
        source_url,
        base_url=request_base_url,
        instance_id=instance_id,
        stream_key=stream_key,
        username=username,
    )


@blueprint.route("/tic-api/cso/channel_stream/<int:stream_id>", methods=["GET"])
@stream_key_required
@skip_stream_connect_audit
async def stream_from_source_gate(stream_id):
    config = current_app.config["APP_CONFIG"]
    instance_id = config.ensure_instance_id()
    request_base_url = get_request_base_url(request)
    stream_user = get_request_stream_user()
    stream_key = get_request_stream_key()
    username = stream_user.username if stream_user else request.args.get("username")
    requested_profile = str(request.args.get("profile") or "").strip().lower()
    client_ip = get_request_client_ip()
    user_agent = request.headers.get("User-Agent")
    client_fingerprint = _client_fingerprint(stream_key, client_ip, user_agent)

    async with Session() as session:
        result = await session.execute(
            select(ChannelSource)
            .options(
                joinedload(ChannelSource.playlist),
                joinedload(ChannelSource.xc_account),
                joinedload(ChannelSource.channel),
            )
            .where(ChannelSource.id == int(stream_id))
        )
        source = result.scalars().first()

    if not source:
        return Response("Stream not found", status=404)
    channel = getattr(source, "channel", None)
    if not channel or not bool(getattr(channel, "enabled", False)):
        return Response("Channel is disabled", status=404)
    playlist = getattr(source, "playlist", None)
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return Response("Stream playlist is disabled", status=404)
    effective_profile = _resolve_cso_profile_for_source_request(
        config,
        requested_profile=requested_profile or "default",
    )
    effective_policy = generate_cso_policy_from_profile(config, effective_profile)
    use_hls_output = str(effective_policy.get("container") or "").strip().lower() == "hls"

    connection_id = _get_connection_id()
    if connection_id == "tvh":
        connection_id = f"tvh-{uuid.uuid4().hex}"
    capacity_key_name = source_capacity_key(source)
    capacity_limit = int(source_capacity_limit(source) or 0)
    usage = await cso_capacity_registry.get_usage(capacity_key_name)
    if (
        await has_background_health_check_for_capacity_key(capacity_key_name)
        and int(usage.get("total") or 0) >= capacity_limit
    ):
        await cancel_background_health_checks_for_capacity_key(
            capacity_key_name,
            reason="source_stream_playback_priority",
        )

    if use_hls_output:
        hls_query = _build_hls_query_string(
            stream_key=stream_key,
            username=username,
            profile=effective_profile,
        )
        target_path = f"/tic-api/cso/channel_stream/{int(source.id)}/hls/{connection_id}/index.m3u8"
        target_url = f"{request_base_url.rstrip('/')}{target_path}"
        if hls_query:
            target_url = f"{target_url}?{hls_query}"
        return redirect(target_url, code=302)

    prebuffer_bytes = parse_size(request.args.get("prebuffer"), default=0)

    plan = await subscribe_source_stream(
        config=config,
        source=source,
        stream_key=stream_key,
        profile=effective_profile,
        connection_id=connection_id,
        prebuffer_bytes=prebuffer_bytes,
        request_base_url=request_base_url,
    )
    if not plan.generator:
        return Response(plan.error_message or "Unable to start CSO stream", status=plan.status_code or 500)

    source_identity = f"/tic-api/cso/channel_stream/{int(source.id)}"
    source_name = str(getattr(source, "playlist_stream_name", "") or getattr(channel, "name", "") or "").strip()
    details_override = source_identity
    if source_name:
        details_override = f"{source_name}\n{source_identity}"
    await upsert_stream_activity(
        source_identity,
        connection_id=connection_id,
        endpoint_override=request.path,
        start_event_type="stream_start",
        user=stream_user,
        details_override=details_override,
        channel_id=getattr(channel, "id", None),
        channel_name=getattr(channel, "name", None),
        channel_logo_url=getattr(channel, "logo_url", None),
        stream_name=source_name,
        display_url=source_identity,
        source_id=getattr(source, "id", None),
        playlist_id=getattr(source, "playlist_id", None),
        xc_account_id=getattr(source, "xc_account_id", None),
    )

    @stream_with_context
    async def generate_source_stream():
        touch_identity = source_identity
        try:
            async for chunk in _iter_cso_plan_generator(plan, connection_id, touch_identity):
                yield chunk
        finally:
            try:
                close = getattr(plan.generator, "aclose", None)
                if close is not None:
                    await close()
            except Exception:
                pass
            await stop_stream_activity(
                "",
                connection_id=connection_id,
                event_type="stream_stop",
                endpoint_override=request.path,
                user=stream_user,
            )

    response = Response(generate_source_stream(), content_type=plan.content_type or "application/octet-stream")
    response.timeout = None
    return response


@blueprint.route("/tic-api/cso/channel/<channel_id>", methods=["GET"])
@stream_key_required
@skip_stream_connect_audit
async def stream_channel(channel_id):
    """
    TIC Channel Stream Organiser (CSO) playback endpoint.

    Route:
    - `GET /tic-api/cso/channel/<channel_id>`

    Authentication:
    - Requires `stream_key_required`.

    Behavior:
    - Resolves the channel and starts/joins CSO sessions for that channel.
    - Uses `profile` query param to select output behavior (remux/transcode profile).
    - Supports shared upstream ingest per channel, with per-profile output pipelines.
    - Returns a continuous stream response with container/content type based on the
      resolved CSO profile.

    Query params:
    - `stream_key` (required): stream authentication token.
    - `profile` (optional): requested stream profile.
      Resolution order is: request profile -> channel profile -> `default`.
      Special case: `tvh` maps to the TVHeadend-oriented MPEG-TS CSO behavior.
    - `prebuffer` (optional): per-client prebuffer size for newly attached output
      clients (for example `50k`, `1M`).

    Failure behavior:
    - If CSO cannot start due to capacity or source playback failure, returns 503.
    - If `CSO_UNAVAILABLE_SHOW_SLATE` is enabled, 503 failures are replaced with a
      temporary MPEG-TS unavailable slate stream (HTTP 200).

    Notes:
    - This endpoint is CSO-specific and separate from direct HLS proxy passthrough
      routes that proxy encoded upstream URLs.
    """
    try:
        channel_id_int = int(channel_id)
    except (TypeError, ValueError):
        return Response("Invalid channel id", status=400)

    config = current_app.config["APP_CONFIG"]
    requested_profile = (request.args.get("profile") or "").strip().lower()
    prebuffer_bytes = parse_size(request.args.get("prebuffer"), default=0)
    channel = await resolve_channel_for_stream(channel_id_int)
    await preempt_background_health_checks_for_channel(channel_id_int)
    effective_profile = resolve_cso_profile_name(
        config,
        requested_profile,
        channel=channel,
    )
    effective_policy = generate_cso_policy_from_profile(config, effective_profile)
    use_hls_output = str(effective_policy.get("container") or "").strip().lower() == "hls"

    connection_id = _get_connection_id()
    if connection_id == "tvh":
        # Treat "tvh" as a logical label only. Internally, each request gets a
        # unique client id to avoid teardown collisions across reconnects.
        connection_id = f"tvh-{uuid.uuid4().hex}"
        logger.info(
            "Remapped reserved connection_id requested=tvh effective=%s channel=%s",
            connection_id,
            channel_id_int,
        )
    stream_user = get_request_stream_user()
    stream_key = get_request_stream_key()
    if use_hls_output:
        hls_query = _build_hls_query_string(
            stream_key=stream_key,
            username=stream_user.username if stream_user else None,
            profile=effective_profile,
        )
        target_path = f"/tic-api/cso/channel/{channel_id_int}/hls/{connection_id}/index.m3u8"
        target_url = f"{get_request_base_url(request).rstrip('/')}{target_path}"
        if hls_query:
            target_url = f"{target_url}?{hls_query}"
        return redirect(target_url, code=302)
    if channel is not None and is_vod_channel_type(getattr(channel, "channel_type", None)):
        generator, content_type, error_message, status_code = await subscribe_vod_channel_stream(
            config,
            channel_id_int,
            stream_key=stream_key,
            profile=effective_profile,
            connection_id=connection_id,
            request_headers=dict(request.headers),
        )
        if generator is None:
            return Response(error_message or "Unable to start VOD channel stream", status=status_code or 500)

        @stream_with_context
        async def generate_vod_channel_stream():
            try:
                async for chunk in generator:
                    yield chunk
            finally:
                await stop_stream_activity(
                    "",
                    connection_id=connection_id,
                    event_type="stream_stop",
                    endpoint_override=request.path,
                    user=stream_user,
                )

        await upsert_stream_activity(
            f"/tic-api/cso/channel/{channel_id_int}",
            connection_id=connection_id,
            endpoint_override=request.path,
            start_event_type="stream_start",
            user=stream_user,
            details_override=f"{getattr(channel, 'name', '')}\n/tic-api/cso/channel/{channel_id_int}",
            channel_id=channel_id_int,
            channel_name=getattr(channel, "name", None),
            channel_logo_url=getattr(channel, "logo_url", None),
            stream_name=getattr(channel, "name", None),
            display_url=f"/tic-api/cso/channel/{channel_id_int}",
        )
        response = Response(generate_vod_channel_stream(), content_type=content_type or "video/mp2t")
        response.timeout = None
        return response
    plan = await subscribe_channel_stream(
        config=config,
        channel=channel,
        stream_key=stream_key,
        profile=effective_profile,
        connection_id=connection_id,
        prebuffer_bytes=prebuffer_bytes,
        request_base_url=get_request_base_url(request),
    )
    if not plan.generator:
        return Response(plan.error_message or "Unable to start CSO stream", status=plan.status_code or 500)

    activity_identity = f"/tic-api/cso/channel/{channel_id_int}"
    channel_name = getattr(channel, "name", None) if channel else None
    channel_logo_url = getattr(channel, "logo_url", None) if channel else None
    details_override = activity_identity
    if channel_name:
        details_override = f"{channel_name}\n{activity_identity}"

    await upsert_stream_activity(
        activity_identity,
        connection_id=connection_id,
        endpoint_override=request.path,
        start_event_type="stream_start",
        user=stream_user,
        details_override=details_override,
        channel_id=channel_id_int,
        channel_name=channel_name,
        channel_logo_url=channel_logo_url,
        stream_name=channel_name,
        display_url=activity_identity,
    )

    @stream_with_context
    async def generate_stream():
        touch_identity = activity_identity
        try:
            async for chunk in _iter_cso_plan_generator(plan, connection_id, touch_identity):
                yield chunk
        finally:
            try:
                close = getattr(plan.generator, "aclose", None)
                if close is not None:
                    await close()
            except Exception:
                pass
            await stop_stream_activity(
                "",
                connection_id=connection_id,
                event_type="stream_stop",
                endpoint_override=request.path,
                user=stream_user,
            )

    response = Response(generate_stream(), content_type=plan.content_type or "application/octet-stream")
    response.timeout = None
    return response


async def _wait_for_hls_playlist(output_session, connection_id=None, timeout_seconds=15.0):
    deadline = time.time() + float(timeout_seconds)
    while time.time() < deadline:
        if connection_id is not None:
            await output_session.touch_client(connection_id)
        playlist_text = await output_session.read_playlist_text()
        if playlist_text:
            return playlist_text
        await asyncio.sleep(0.25)
    return None


async def _hls_start_failure_response(config, channel, effective_profile, error_message, status):
    message = (error_message or "").strip()
    if int(status or 500) == 503 and "connection limit" in message.lower():
        effective_policy = generate_cso_policy_from_profile(config, effective_profile)
        return _response_from_cso_plan(
            subscribe_slate_stream(
                config,
                effective_policy,
                "capacity_blocked",
                detail_hint="",
                profile_name=effective_profile,
                channel=channel,
                status_code=503,
            ),
            CONNECTION_LIMIT_REACHED_MESSAGE,
            503,
        )
    return Response(message or "Unable to start CSO HLS stream", status=status or 500)


async def _build_hls_slate_response(
    config,
    channel,
    effective_profile,
    connection_id,
    request_path,
    reason,
    on_disconnect=None,
    detail_hint="",
):
    effective_policy = generate_cso_policy_from_profile(config, effective_profile)
    output_session, error_message, status = await subscribe_slate_hls(
        config,
        effective_policy,
        reason,
        connection_id=connection_id,
        on_disconnect=on_disconnect,
        detail_hint=detail_hint,
        profile_name=effective_profile,
        channel=channel,
        status_code=503,
    )
    if not output_session:
        return Response(error_message or "Unable to start CSO HLS stream", status=status or 503)
    await output_session.touch_client(connection_id)
    playlist_text = await _wait_for_hls_playlist(output_session, connection_id=connection_id)
    if not playlist_text:
        return Response("HLS playlist not ready", status=503)
    stream_user = get_request_stream_user()
    query_string = _build_hls_query_string(
        stream_key=get_request_stream_key(),
        username=stream_user.username if stream_user else None,
        profile=effective_profile,
    )
    segment_base_path = request_path.rsplit("/", 1)[0]
    rendered_playlist = _render_hls_playlist(playlist_text, segment_base_path, query_string=query_string)
    return Response(rendered_playlist, content_type="application/vnd.apple.mpegurl")


@blueprint.route("/tic-api/cso/channel/<int:channel_id>/hls/<connection_id>/index.m3u8", methods=["GET"])
@stream_key_required
@skip_stream_connect_audit
async def stream_channel_hls_playlist(channel_id: int, connection_id: str):
    config = current_app.config["APP_CONFIG"]
    requested_profile = str(request.args.get("profile") or "hls").strip().lower()
    request_base_url = get_request_base_url(request)
    stream_user = get_request_stream_user()
    stream_key = get_request_stream_key()

    channel = await resolve_channel_for_stream(int(channel_id))
    if not channel or not bool(getattr(channel, "enabled", False)):
        return Response("Channel is disabled", status=404)
    await preempt_background_health_checks_for_channel(int(channel_id))
    effective_profile = resolve_cso_profile_name(config, requested_profile, channel=channel)
    effective_policy = generate_cso_policy_from_profile(config, effective_profile)
    if str(effective_policy.get("container") or "").strip().lower() != "hls":
        return Response("Requested profile is not HLS output", status=400)
    is_vod_channel = bool(channel is not None and is_vod_channel_type(getattr(channel, "channel_type", None)))
    output_session_key = (
        f"cso-vod-channel-hls-output-{int(channel_id)}-{effective_profile}"
        if is_vod_channel
        else f"cso-hls-output-{int(channel_id)}-{effective_profile}"
    )
    existing_client = await _hls_output_has_client(output_session_key, connection_id)

    has_active_ingest = await cso_session_manager.has_active_ingest_for_channel(int(channel_id))
    has_existing_ingest = has_active_ingest or await cso_session_manager.has_ingest_session_for_channel(int(channel_id))
    primary_source = None if is_vod_channel else await _select_primary_source_for_capacity(channel)
    if primary_source is not None and not existing_client and not has_existing_ingest:
        capacity_key_name = source_capacity_key(primary_source)
        capacity_limit = int(source_capacity_limit(primary_source) or 0)
        if capacity_limit > 0:
            usage = await cso_capacity_registry.get_usage(capacity_key_name)
            activity_sessions = await get_stream_activity_snapshot()
            external_count = _active_external_count_for_source(activity_sessions, primary_source, capacity_key_name)
            active_connections = int(usage.get("allocations") or 0) + int(external_count)
            if active_connections >= capacity_limit:
                waited = await _wait_for_source_handover_window(
                    source=primary_source,
                    capacity_key_name=capacity_key_name,
                    capacity_limit=capacity_limit,
                    client_fingerprint=_client_fingerprint(
                        stream_key,
                        get_request_client_ip(),
                        request.headers.get("User-Agent"),
                    ),
                    exclude_connection_id=connection_id,
                )
                if not waited:
                    return await _build_hls_slate_response(
                        config,
                        channel,
                        effective_profile,
                        connection_id,
                        request.path,
                        "capacity_blocked",
                    )

    async def _on_disconnect(client_id):
        await stop_stream_activity(
            "",
            connection_id=client_id,
            event_type="stream_stop",
            endpoint_override=f"/tic-api/cso/channel/{int(channel_id)}/hls/{client_id}",
            user=None,
        )
        await emit_channel_stream_event(
            channel_id=int(channel_id),
            source_id=None,
            playlist_id=None,
            session_id=output_session_key,
            event_type="session_end",
            severity="info",
            details={"profile": effective_profile, "connection_id": str(client_id)},
        )

    if is_vod_channel:
        output_session, error_message, status = await subscribe_vod_channel_hls(
            config=config,
            channel_id=int(channel_id),
            stream_key=stream_key,
            profile=effective_profile,
            connection_id=connection_id,
            request_headers=dict(request.headers),
            on_disconnect=_on_disconnect,
        )
    else:
        output_session, error_message, status = await subscribe_channel_hls(
            config=config,
            channel=channel,
            stream_key=stream_key,
            profile=effective_profile,
            connection_id=connection_id,
            request_base_url=request_base_url,
            on_disconnect=_on_disconnect,
        )
    if not output_session:
        if int(status or 500) == 503 and "connection limit" in str(error_message or "").lower():
            return await _build_hls_slate_response(
                config,
                channel,
                effective_profile,
                connection_id,
                request.path,
                "capacity_blocked",
                on_disconnect=_on_disconnect,
            )
        return await _hls_start_failure_response(config, channel, effective_profile, error_message, status)

    await output_session.touch_client(connection_id)
    playlist_text = await _wait_for_hls_playlist(output_session, connection_id=connection_id)
    if not playlist_text:
        return Response("HLS playlist not ready", status=503)

    query_string = _build_hls_query_string(
        stream_key=stream_key,
        username=stream_user.username if stream_user else None,
        profile=effective_profile,
    )
    segment_base_path = request.path.rsplit("/", 1)[0]
    rendered_playlist = _render_hls_playlist(playlist_text, segment_base_path, query_string=query_string)
    await upsert_stream_activity(
        f"/tic-api/cso/channel/{int(channel_id)}",
        connection_id=str(connection_id),
        endpoint_override=request.path,
        start_event_type="stream_start",
        user=stream_user,
        details_override=f"{getattr(channel, 'name', '')}\n/tic-api/cso/channel/{int(channel_id)}".strip(),
        channel_id=int(channel_id),
        channel_name=getattr(channel, "name", None) if channel else None,
        channel_logo_url=getattr(channel, "logo_url", None) if channel else None,
        stream_name=getattr(channel, "name", None) if channel else None,
        display_url=f"/tic-api/cso/channel/{int(channel_id)}",
    )
    return Response(rendered_playlist, content_type="application/vnd.apple.mpegurl")


@blueprint.route("/tic-api/cso/channel/<int:channel_id>/hls/<connection_id>/<segment_name>", methods=["GET"])
@stream_key_required
@skip_stream_connect_audit
async def stream_channel_hls_segment(channel_id: int, connection_id: str, segment_name: str):
    config = current_app.config["APP_CONFIG"]
    requested_profile = str(request.args.get("profile") or "hls").strip().lower()
    request_base_url = get_request_base_url(request)
    stream_key = get_request_stream_key()

    channel = await resolve_channel_for_stream(int(channel_id))
    if not channel or not bool(getattr(channel, "enabled", False)):
        return Response("Channel is disabled", status=404)
    effective_profile = resolve_cso_profile_name(config, requested_profile, channel=channel)
    effective_policy = generate_cso_policy_from_profile(config, effective_profile)
    if str(effective_policy.get("container") or "").strip().lower() != "hls":
        return Response("Requested profile is not HLS output", status=400)
    is_vod_channel = bool(channel is not None and is_vod_channel_type(getattr(channel, "channel_type", None)))
    output_session_key = (
        f"cso-vod-channel-hls-output-{int(channel_id)}-{effective_profile}"
        if is_vod_channel
        else f"cso-hls-output-{int(channel_id)}-{effective_profile}"
    )
    existing_client = await _hls_output_has_client(output_session_key, connection_id)
    has_active_ingest = await cso_session_manager.has_active_ingest_for_channel(int(channel_id))
    has_existing_ingest = has_active_ingest or await cso_session_manager.has_ingest_session_for_channel(int(channel_id))
    primary_source = None if is_vod_channel else await _select_primary_source_for_capacity(channel)
    if primary_source is not None and not existing_client and not has_existing_ingest:
        capacity_key_name = source_capacity_key(primary_source)
        capacity_limit = int(source_capacity_limit(primary_source) or 0)
        if capacity_limit > 0:
            usage = await cso_capacity_registry.get_usage(capacity_key_name)
            activity_sessions = await get_stream_activity_snapshot()
            external_count = _active_external_count_for_source(activity_sessions, primary_source, capacity_key_name)
            active_connections = int(usage.get("allocations") or 0) + int(external_count)
            if active_connections >= capacity_limit:
                output_session, _, _ = await subscribe_slate_hls(
                    config,
                    effective_policy,
                    "capacity_blocked",
                    connection_id=connection_id,
                    profile_name=effective_profile,
                    channel=channel,
                    status_code=503,
                )
                if output_session is None:
                    return Response(CONNECTION_LIMIT_REACHED_MESSAGE, status=503)
                await output_session.touch_client(connection_id)
                payload = await output_session.read_segment_bytes(segment_name)
                return (
                    Response(payload, content_type=content_type_for_media_path(segment_name))
                    if payload is not None
                    else Response("HLS segment not found", status=404)
                )

    async def _on_disconnect(client_id):
        await stop_stream_activity(
            "",
            connection_id=client_id,
            event_type="stream_stop",
            endpoint_override=f"/tic-api/cso/channel/{int(channel_id)}/hls/{client_id}",
            user=None,
        )
        await emit_channel_stream_event(
            channel_id=int(channel_id),
            source_id=None,
            playlist_id=None,
            session_id=output_session_key,
            event_type="session_end",
            severity="info",
            details={"profile": effective_profile, "connection_id": str(client_id)},
        )

    if is_vod_channel:
        output_session, error_message, status = await subscribe_vod_channel_hls(
            config=config,
            channel_id=int(channel_id),
            stream_key=stream_key,
            profile=effective_profile,
            connection_id=connection_id,
            request_headers=dict(request.headers),
            on_disconnect=_on_disconnect,
        )
    else:
        output_session, error_message, status = await subscribe_channel_hls(
            config=config,
            channel=channel,
            stream_key=stream_key,
            profile=effective_profile,
            connection_id=connection_id,
            request_base_url=request_base_url,
            on_disconnect=_on_disconnect,
        )
    if not output_session:
        if int(status or 500) == 503 and "connection limit" in str(error_message or "").lower():
            output_session, _, _ = await subscribe_slate_hls(
                config,
                effective_policy,
                "capacity_blocked",
                connection_id=connection_id,
                on_disconnect=_on_disconnect,
                profile_name=effective_profile,
                channel=channel,
                status_code=503,
            )
            if output_session is None:
                return Response(CONNECTION_LIMIT_REACHED_MESSAGE, status=503)
            await output_session.touch_client(connection_id)
            payload = await output_session.read_segment_bytes(segment_name)
            return (
                Response(payload, content_type=content_type_for_media_path(segment_name))
                if payload is not None
                else Response("HLS segment not found", status=404)
            )
        return await _hls_start_failure_response(config, channel, effective_profile, error_message, status)
    await output_session.touch_client(connection_id)
    await touch_stream_activity(str(connection_id), identity=f"/tic-api/cso/channel/{int(channel_id)}")

    payload = await output_session.read_segment_bytes(segment_name)
    if payload is None:
        return Response("HLS segment not found", status=404)
    return Response(payload, content_type=content_type_for_media_path(segment_name))


@blueprint.route("/tic-api/cso/channel_stream/<int:stream_id>/hls/<connection_id>/index.m3u8", methods=["GET"])
@stream_key_required
@skip_stream_connect_audit
async def stream_source_hls_playlist(stream_id, connection_id):
    config = current_app.config["APP_CONFIG"]
    requested_profile = str(request.args.get("profile") or "hls").strip().lower()
    request_base_url = get_request_base_url(request)
    stream_user = get_request_stream_user()
    stream_key = get_request_stream_key()

    async with Session() as session:
        result = await session.execute(
            select(ChannelSource)
            .options(
                joinedload(ChannelSource.playlist),
                joinedload(ChannelSource.xc_account),
                joinedload(ChannelSource.channel),
            )
            .where(ChannelSource.id == int(stream_id))
        )
        source = result.scalars().first()

    if not source:
        return Response("Stream not found", status=404)
    channel = getattr(source, "channel", None)
    if not channel or not bool(getattr(channel, "enabled", False)):
        return Response("Channel is disabled", status=404)
    playlist = getattr(source, "playlist", None)
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return Response("Stream playlist is disabled", status=404)
    await preempt_background_health_checks_for_channel(int(getattr(source, "channel_id", 0) or 0))
    effective_profile = _resolve_cso_profile_for_source_request(
        config,
        requested_profile=requested_profile or "hls",
    )
    effective_policy = generate_cso_policy_from_profile(config, effective_profile)
    if str(effective_policy.get("container") or "").strip().lower() != "hls":
        return Response("Requested profile is not HLS output", status=400)
    output_session_key = f"cso-source-hls-output-{int(source.id)}-{effective_profile}"
    existing_client = await _hls_output_has_client(output_session_key, connection_id)
    has_active_ingest = await cso_session_manager.has_active_ingest_for_source(int(source.id))
    has_existing_ingest = has_active_ingest or await cso_session_manager.has_ingest_session_for_source(int(source.id))
    capacity_key_name = source_capacity_key(source)
    capacity_limit = int(source_capacity_limit(source) or 0)
    if capacity_limit > 0 and not existing_client and not has_existing_ingest:
        usage = await cso_capacity_registry.get_usage(capacity_key_name)
        activity_sessions = await get_stream_activity_snapshot()
        external_count = _active_external_count_for_source(activity_sessions, source, capacity_key_name)
        active_connections = int(usage.get("allocations") or 0) + int(external_count)
        if active_connections >= capacity_limit:
            waited = await _wait_for_source_handover_window(
                source=source,
                capacity_key_name=capacity_key_name,
                capacity_limit=capacity_limit,
                client_fingerprint=_client_fingerprint(
                    stream_key,
                    get_request_client_ip(),
                    request.headers.get("User-Agent"),
                ),
                exclude_connection_id=connection_id,
            )
            if not waited:
                return await _build_hls_slate_response(
                    config,
                    channel,
                    effective_profile,
                    connection_id,
                    request.path,
                    "capacity_blocked",
                )

    async def _on_disconnect(client_id):
        await stop_stream_activity(
            "",
            connection_id=client_id,
            event_type="stream_stop",
            endpoint_override=f"/tic-api/cso/channel_stream/{int(stream_id)}/hls/{client_id}",
            user=None,
        )
        await emit_channel_stream_event(
            channel_id=getattr(channel, "id", None),
            source_id=getattr(source, "id", None),
            playlist_id=getattr(source, "playlist_id", None),
            session_id=output_session_key,
            event_type="session_end",
            severity="info",
            details={"profile": effective_profile, "connection_id": str(client_id)},
        )

    output_session, error_message, status = await subscribe_source_hls(
        config=config,
        source=source,
        stream_key=stream_key,
        profile=effective_profile,
        connection_id=connection_id,
        request_base_url=request_base_url,
        on_disconnect=_on_disconnect,
    )
    if not output_session:
        if int(status or 500) == 503 and "connection limit" in str(error_message or "").lower():
            return await _build_hls_slate_response(
                config,
                channel,
                effective_profile,
                connection_id,
                request.path,
                "capacity_blocked",
                on_disconnect=_on_disconnect,
            )
        return await _hls_start_failure_response(config, channel, effective_profile, error_message, status)

    await output_session.touch_client(connection_id)
    playlist_text = await _wait_for_hls_playlist(output_session, connection_id=connection_id)
    if not playlist_text:
        return Response("HLS playlist not ready", status=503)

    query_string = _build_hls_query_string(
        stream_key=stream_key,
        username=stream_user.username if stream_user else None,
        profile=effective_profile,
    )
    segment_base_path = request.path.rsplit("/", 1)[0]
    rendered_playlist = _render_hls_playlist(playlist_text, segment_base_path, query_string=query_string)
    source_identity = f"/tic-api/cso/channel_stream/{int(stream_id)}"
    source_name = str(getattr(source, "playlist_stream_name", "") or getattr(channel, "name", "") or "").strip()
    details_override = source_identity if not source_name else f"{source_name}\n{source_identity}"
    await upsert_stream_activity(
        source_identity,
        connection_id=str(connection_id),
        endpoint_override=request.path,
        start_event_type="stream_start",
        user=stream_user,
        details_override=details_override,
        channel_id=getattr(channel, "id", None),
        channel_name=getattr(channel, "name", None),
        channel_logo_url=getattr(channel, "logo_url", None),
        stream_name=source_name,
        display_url=source_identity,
        source_id=getattr(source, "id", None),
        playlist_id=getattr(source, "playlist_id", None),
        xc_account_id=getattr(source, "xc_account_id", None),
    )
    return Response(rendered_playlist, content_type="application/vnd.apple.mpegurl")


@blueprint.route("/tic-api/cso/channel_stream/<int:stream_id>/hls/<connection_id>/<segment_name>", methods=["GET"])
@stream_key_required
@skip_stream_connect_audit
async def stream_source_hls_segment(stream_id, connection_id, segment_name):
    config = current_app.config["APP_CONFIG"]
    requested_profile = str(request.args.get("profile") or "hls").strip().lower()
    request_base_url = get_request_base_url(request)
    stream_key = get_request_stream_key()

    async with Session() as session:
        result = await session.execute(
            select(ChannelSource)
            .options(
                joinedload(ChannelSource.playlist),
                joinedload(ChannelSource.xc_account),
                joinedload(ChannelSource.channel),
            )
            .where(ChannelSource.id == int(stream_id))
        )
        source = result.scalars().first()

    if not source:
        return Response("Stream not found", status=404)
    channel = getattr(source, "channel", None)
    if not channel or not bool(getattr(channel, "enabled", False)):
        return Response("Channel is disabled", status=404)
    playlist = getattr(source, "playlist", None)
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return Response("Stream playlist is disabled", status=404)
    effective_profile = _resolve_cso_profile_for_source_request(
        config,
        requested_profile=requested_profile or "hls",
    )
    effective_policy = generate_cso_policy_from_profile(config, effective_profile)
    if str(effective_policy.get("container") or "").strip().lower() != "hls":
        return Response("Requested profile is not HLS output", status=400)
    output_session_key = f"cso-source-hls-output-{int(source.id)}-{effective_profile}"
    existing_client = await _hls_output_has_client(output_session_key, connection_id)
    has_active_ingest = await cso_session_manager.has_active_ingest_for_source(int(source.id))
    has_existing_ingest = has_active_ingest or await cso_session_manager.has_ingest_session_for_source(int(source.id))
    capacity_key_name = source_capacity_key(source)
    capacity_limit = int(source_capacity_limit(source) or 0)
    if capacity_limit > 0 and not existing_client and not has_existing_ingest:
        usage = await cso_capacity_registry.get_usage(capacity_key_name)
        activity_sessions = await get_stream_activity_snapshot()
        external_count = _active_external_count_for_source(activity_sessions, source, capacity_key_name)
        active_connections = int(usage.get("allocations") or 0) + int(external_count)
        if active_connections >= capacity_limit:
            output_session, _, _ = await subscribe_slate_hls(
                config,
                effective_policy,
                "capacity_blocked",
                connection_id=connection_id,
                profile_name=effective_profile,
                channel=channel,
                source=source,
                status_code=503,
            )
            if output_session is None:
                return Response(CONNECTION_LIMIT_REACHED_MESSAGE, status=503)
            await output_session.touch_client(connection_id)
            payload = await output_session.read_segment_bytes(segment_name)
            return (
                Response(payload, content_type=content_type_for_media_path(segment_name))
                if payload is not None
                else Response("HLS segment not found", status=404)
            )

    async def _on_disconnect(client_id):
        await stop_stream_activity(
            "",
            connection_id=client_id,
            event_type="stream_stop",
            endpoint_override=f"/tic-api/cso/channel_stream/{int(stream_id)}/hls/{client_id}",
            user=None,
        )
        await emit_channel_stream_event(
            channel_id=getattr(channel, "id", None),
            source_id=getattr(source, "id", None),
            playlist_id=getattr(source, "playlist_id", None),
            session_id=output_session_key,
            event_type="session_end",
            severity="info",
            details={"profile": effective_profile, "connection_id": str(client_id)},
        )

    output_session, error_message, status = await subscribe_source_hls(
        config=config,
        source=source,
        stream_key=stream_key,
        profile=effective_profile,
        connection_id=connection_id,
        request_base_url=request_base_url,
        on_disconnect=_on_disconnect,
    )
    if not output_session:
        if int(status or 500) == 503 and "connection limit" in str(error_message or "").lower():
            output_session, _, _ = await subscribe_slate_hls(
                config,
                effective_policy,
                "capacity_blocked",
                connection_id=connection_id,
                on_disconnect=_on_disconnect,
                profile_name=effective_profile,
                channel=channel,
                source=source,
                status_code=503,
            )
            if output_session is None:
                return Response(CONNECTION_LIMIT_REACHED_MESSAGE, status=503)
            await output_session.touch_client(connection_id)
            payload = await output_session.read_segment_bytes(segment_name)
            return (
                Response(payload, content_type=content_type_for_media_path(segment_name))
                if payload is not None
                else Response("HLS segment not found", status=404)
            )
        return await _hls_start_failure_response(config, channel, effective_profile, error_message, status)
    await output_session.touch_client(connection_id)
    await touch_stream_activity(str(connection_id), identity=f"/tic-api/cso/channel_stream/{int(stream_id)}")

    payload = await output_session.read_segment_bytes(segment_name)
    if payload is None:
        return Response("HLS segment not found", status=404)
    return Response(payload, content_type=content_type_for_media_path(segment_name))


async def _stream_cso_vod_route(resolver, identity: str):
    config = current_app.config["APP_CONFIG"]
    resolved = await resolver(config)
    if resolved.get("error"):
        return Response(str(resolved["error"]), status=int(resolved.get("status") or 404))

    candidate = resolved["candidate"]
    episode = resolved.get("episode")
    upstream_url = str(resolved.get("upstream_url") or "")
    effective_profile = str(resolved.get("effective_profile") or "")
    use_proxy_session = bool(resolved.get("use_proxy_session"))
    activity_metadata = dict(resolved.get("activity_metadata") or {})
    start_seconds = _requested_hls_start_seconds()
    use_restartable_output = _is_restartable_vod_output_profile(effective_profile)
    stream_key = get_request_stream_key()
    stream_user = get_request_stream_user()
    stream_username = stream_user.username if stream_user is not None else None
    source_item = getattr(candidate, "source_item", None)
    source_id = getattr(source_item, "id", None)
    requested_container_extension = str(request.args.get("container_extension") or "").strip().lstrip(".").lower() or None
    source_container = requested_container_extension or (
        str(getattr(source_item, "container_extension", "") or "").strip().lower() or None
    )

    vod_item_id = None
    vod_category_id = None
    if isinstance(candidate, VodCuratedPlaybackCandidate):
        vod_item_id = candidate.group_item.id
        vod_category_id = candidate.group_item.category_id
    vod_episode_id = episode.id if episode is not None else None

    effective_policy = generate_cso_policy_from_profile(config, effective_profile)
    if str(effective_policy.get("container") or "").strip().lower() == "hls":
        connection_id = _get_connection_id()
        query_string = _build_hls_query_string(
            stream_key=stream_key,
            username=stream_username,
            profile=effective_profile,
            start_seconds=start_seconds,
            container_extension=request.args.get("container_extension"),
        )
        target_url = f"{get_request_base_url(request).rstrip('/')}{request.path}/hls/{connection_id}/index.m3u8"
        if query_string:
            target_url = f"{target_url}?{query_string}"
        return redirect(target_url, code=302)

    connection_id = _get_connection_id()
    request_client_ip = get_request_client_ip()
    request_user_agent = request.headers.get("User-Agent")
    selected_mode = "session"
    if use_proxy_session:
        selected_mode = "proxy"
    elif use_restartable_output:
        selected_mode = "restartable_output"
    logger.info(
        "CSO VOD route request path=%s connection_id=%s mode=%s profile=%s source_id=%s item_id=%s episode_id=%s start_seconds=%s container_extension=%s upstream_url=%s",
        request.path,
        connection_id,
        selected_mode,
        effective_profile,
        source_id,
        vod_item_id,
        vod_episode_id,
        start_seconds,
        source_container,
        upstream_url or "",
    )
    await upsert_stream_activity(
        identity,
        connection_id=connection_id,
        endpoint_override=request.path,
        user=stream_user,
        ip_address=request_client_ip,
        user_agent=request_user_agent,
        perform_audit=False,
        channel_name=activity_metadata.get("channel_name"),
        channel_logo_url=activity_metadata.get("channel_logo_url"),
        stream_name=activity_metadata.get("stream_name"),
        source_url=upstream_url or identity,
        display_url=activity_metadata.get("display_url") or identity,
        vod_item_id=vod_item_id,
        vod_category_id=vod_category_id,
        vod_episode_id=vod_episode_id,
        enrich_metadata=False,
    )
    if use_proxy_session:
        plan = await subscribe_vod_proxy_stream(
            candidate,
            upstream_url,
            connection_id,
            request_headers=dict(request.headers),
            episode=episode,
        )
    elif use_restartable_output:
        plan = await subscribe_vod_ingest_stream(
            config,
            candidate,
            upstream_url,
            effective_profile,
            connection_id,
            episode=episode,
            start_seconds=start_seconds,
            request_headers=dict(request.headers),
        )
    else:
        plan = await subscribe_vod_stream(
            config,
            candidate,
            upstream_url,
            stream_key,
            effective_profile,
            connection_id,
            episode=episode,
            request_base_url=get_request_base_url(request),
        )

    if plan.generator is None:
        logger.warning(
            "CSO VOD route start failed path=%s connection_id=%s mode=%s profile=%s source_id=%s status=%s",
            request.path,
            connection_id,
            selected_mode,
            effective_profile,
            source_id,
            int(plan.status_code or 503),
        )
        return Response("Unable to start playback", status=int(plan.status_code or 503))

    logger.info(
        "CSO VOD route stream started path=%s connection_id=%s mode=%s profile=%s source_id=%s content_type=%s status=%s",
        request.path,
        connection_id,
        selected_mode,
        effective_profile,
        source_id,
        plan.content_type or "application/octet-stream",
        int(plan.status_code or 200),
    )

    @stream_with_context
    async def generate_vod_stream():
        try:
            async for chunk in _iter_cso_plan_generator(plan, connection_id, identity):
                yield chunk
        finally:
            try:
                await plan.generator.aclose()
            except (AttributeError, Exception):
                pass
            await stop_stream_activity(
                "",
                connection_id=connection_id,
                event_type="stream_stop",
                endpoint_override=request.path,
                user=stream_user,
            )
            logger.info(
                "CSO VOD route stream closed path=%s connection_id=%s mode=%s profile=%s source_id=%s",
                request.path,
                connection_id,
                selected_mode,
                effective_profile,
                source_id,
            )

    response = Response(
        generate_vod_stream(),
        status=int(plan.status_code or 200),
        headers=dict(plan.headers or {}),
        content_type=plan.content_type or "application/octet-stream",
    )
    response.timeout = None
    return response


async def _stream_cso_vod_hls_playlist(resolver, segment_base_path: str, identity: str):
    try:
        config = current_app.config["APP_CONFIG"]
        resolved = await resolver(config)
        if resolved.get("error"):
            return Response(str(resolved["error"]), status=int(resolved.get("status") or 404))

        effective_profile = str(resolved.get("effective_profile") or "")
        effective_policy = generate_cso_policy_from_profile(config, effective_profile)
        if str(effective_policy.get("container") or "").strip().lower() != "hls":
            return Response("Requested profile is not HLS output", status=400)

        candidate = resolved["candidate"]
        episode = resolved.get("episode")
        upstream_url = str(resolved.get("upstream_url") or "")
        connection_id = request.view_args.get("connection_id")
        start_seconds = _requested_hls_start_seconds()
        stream_key = get_request_stream_key()
        stream_user = get_request_stream_user()
        stream_username = stream_user.username if stream_user is not None else None

        vod_item_id = None
        vod_category_id = None
        if isinstance(candidate, VodCuratedPlaybackCandidate):
            vod_item_id = candidate.group_item.id
            vod_category_id = candidate.group_item.category_id
        vod_episode_id = episode.id if episode is not None else None

        output_session, error_message, status = await subscribe_vod_hls(
            config,
            candidate,
            upstream_url,
            stream_key,
            effective_profile,
            str(connection_id),
            episode=episode,
            request_base_url=get_request_base_url(request),
            start_seconds=start_seconds,
        )
        if not output_session:
            return Response(error_message or "Unable to start CSO HLS stream", status=status or 503)

        await output_session.touch_client(str(connection_id))
        playlist_text = await _wait_for_hls_playlist(output_session, connection_id=str(connection_id))
        if not playlist_text:
            return Response("HLS playlist not ready", status=503)

        query_string = _build_hls_query_string(
            stream_key=stream_key,
            username=stream_username,
            profile=effective_profile,
            start_seconds=start_seconds,
            container_extension=request.args.get("container_extension"),
        )
        rendered_playlist = _render_hls_playlist(playlist_text, segment_base_path, query_string=query_string)
        activity_metadata = dict(resolved.get("activity_metadata") or {})
        try:
            await upsert_stream_activity(
                identity,
                connection_id=str(connection_id),
                endpoint_override=request.path,
                start_event_type="stream_start",
                user=stream_user,
                details_override=activity_metadata.get("display_url") or identity,
                channel_name=activity_metadata.get("channel_name"),
                channel_logo_url=activity_metadata.get("channel_logo_url"),
                stream_name=activity_metadata.get("stream_name"),
                display_url=activity_metadata.get("display_url") or identity,
                vod_item_id=vod_item_id,
                vod_category_id=vod_category_id,
                vod_episode_id=vod_episode_id,
                enrich_metadata=False,
            )
        except Exception:
            logger.exception(
                "Failed to upsert VOD HLS playlist activity identity=%s connection_id=%s path=%s",
                identity,
                connection_id,
                request.path,
            )
        return Response(rendered_playlist, content_type="application/vnd.apple.mpegurl")
    except Exception:
        logger.exception(
            "Unhandled VOD HLS playlist failure identity=%s path=%s query=%s",
            identity,
            request.path,
            request.query_string.decode("utf-8", errors="ignore"),
        )
        raise


async def _stream_cso_vod_hls_segment(resolver):
    config = current_app.config["APP_CONFIG"]
    resolved = await resolver(config)
    if resolved.get("error"):
        return Response(str(resolved["error"]), status=int(resolved.get("status") or 404))

    effective_profile = str(resolved.get("effective_profile") or "")
    effective_policy = generate_cso_policy_from_profile(config, effective_profile)
    if str(effective_policy.get("container") or "").strip().lower() != "hls":
        return Response("Requested profile is not HLS output", status=400)

    candidate = resolved["candidate"]
    episode = resolved.get("episode")
    upstream_url = str(resolved.get("upstream_url") or "")
    connection_id = request.view_args.get("connection_id")
    segment_name = request.view_args.get("segment_name")
    start_seconds = _requested_hls_start_seconds()
    stream_key = get_request_stream_key()
    output_session, error_message, status = await subscribe_vod_hls(
        config,
        candidate,
        upstream_url,
        stream_key,
        effective_profile,
        str(connection_id),
        episode=episode,
        request_base_url=get_request_base_url(request),
        start_seconds=start_seconds,
    )
    if not output_session:
        return Response(error_message or "Unable to start CSO HLS stream", status=status or 503)

    await output_session.touch_client(str(connection_id))
    payload = await output_session.read_segment_bytes(str(segment_name))
    if payload is None:
        return Response("HLS segment not found", status=404)
    return Response(payload, content_type=content_type_for_media_path(str(segment_name)))


@blueprint.route("/tic-api/cso/vod/<stream_type>/<int:item_id>", methods=["GET"])
@stream_key_required
@skip_stream_connect_audit
async def stream_curated_vod(stream_type: str, item_id: int):
    return await _stream_cso_vod_route(
        lambda config: _resolve_curated_vod_request(config, stream_type, int(item_id)),
        f"/tic-api/cso/vod/{stream_type}/{int(item_id)}",
    )


@blueprint.route("/tic-api/cso/vod/<stream_type>/<int:item_id>/hls/<connection_id>/index.m3u8", methods=["GET"])
@stream_key_required
@skip_stream_connect_audit
async def stream_curated_vod_hls_playlist(stream_type: str, item_id: int, connection_id: str):
    return await _stream_cso_vod_hls_playlist(
        lambda config: _resolve_curated_vod_request(config, stream_type, int(item_id)),
        f"/tic-api/cso/vod/{stream_type}/{int(item_id)}/hls/{connection_id}",
        f"/tic-api/cso/vod/{stream_type}/{int(item_id)}",
    )


@blueprint.route("/tic-api/cso/vod/<stream_type>/<int:item_id>/hls/<connection_id>/<segment_name>", methods=["GET"])
@stream_key_required
@skip_stream_connect_audit
async def stream_curated_vod_hls_segment(stream_type: str, item_id: int, connection_id: str, segment_name: str):
    return await _stream_cso_vod_hls_segment(
        lambda config: _resolve_curated_vod_request(config, stream_type, int(item_id))
    )


@blueprint.route("/tic-api/cso/vod/upstream/<int:source_id>/movie/<int:item_id>", methods=["GET"])
@stream_key_required
@skip_stream_connect_audit
async def stream_upstream_vod_movie(source_id: int, item_id: int):
    return await _stream_cso_vod_route(
        lambda config: _resolve_upstream_vod_request(config, int(source_id), VOD_KIND_MOVIE, int(item_id)),
        f"/tic-api/cso/vod/upstream/{int(source_id)}/movie/{int(item_id)}",
    )


@blueprint.route(
    "/tic-api/cso/vod/upstream/<int:source_id>/movie/<int:item_id>/hls/<connection_id>/index.m3u8",
    methods=["GET"],
)
@stream_key_required
@skip_stream_connect_audit
async def stream_upstream_vod_movie_hls_playlist(source_id: int, item_id: int, connection_id: str):
    return await _stream_cso_vod_hls_playlist(
        lambda config: _resolve_upstream_vod_request(config, int(source_id), VOD_KIND_MOVIE, int(item_id)),
        f"/tic-api/cso/vod/upstream/{int(source_id)}/movie/{int(item_id)}/hls/{connection_id}",
        f"/tic-api/cso/vod/upstream/{int(source_id)}/movie/{int(item_id)}",
    )


@blueprint.route(
    "/tic-api/cso/vod/upstream/<int:source_id>/movie/<int:item_id>/hls/<connection_id>/<segment_name>",
    methods=["GET"],
)
@stream_key_required
@skip_stream_connect_audit
async def stream_upstream_vod_movie_hls_segment(
    source_id: int,
    item_id: int,
    connection_id: str,
    segment_name: str,
):
    return await _stream_cso_vod_hls_segment(
        lambda config: _resolve_upstream_vod_request(config, int(source_id), VOD_KIND_MOVIE, int(item_id))
    )


@blueprint.route(
    "/tic-api/cso/vod/upstream/<int:source_id>/series/<int:item_id>/<upstream_episode_id>", methods=["GET"]
)
@stream_key_required
@skip_stream_connect_audit
async def stream_upstream_vod_series(source_id: int, item_id: int, upstream_episode_id: str):
    return await _stream_cso_vod_route(
        lambda config: _resolve_upstream_vod_request(
            config,
            int(source_id),
            VOD_KIND_SERIES,
            int(item_id),
            upstream_episode_id=upstream_episode_id,
        ),
        f"/tic-api/cso/vod/upstream/{int(source_id)}/series/{int(item_id)}/{upstream_episode_id}",
    )


@blueprint.route(
    "/tic-api/cso/vod/upstream/<int:source_id>/series/<int:item_id>/<upstream_episode_id>/hls/<connection_id>/index.m3u8",
    methods=["GET"],
)
@stream_key_required
@skip_stream_connect_audit
async def stream_upstream_vod_series_hls_playlist(
    source_id: int,
    item_id: int,
    upstream_episode_id: str,
    connection_id: str,
):
    return await _stream_cso_vod_hls_playlist(
        lambda config: _resolve_upstream_vod_request(
            config,
            int(source_id),
            VOD_KIND_SERIES,
            int(item_id),
            upstream_episode_id=upstream_episode_id,
        ),
        f"/tic-api/cso/vod/upstream/{int(source_id)}/series/{int(item_id)}/{upstream_episode_id}/hls/{connection_id}",
        f"/tic-api/cso/vod/upstream/{int(source_id)}/series/{int(item_id)}/{upstream_episode_id}",
    )


@blueprint.route(
    "/tic-api/cso/vod/upstream/<int:source_id>/series/<int:item_id>/<upstream_episode_id>/hls/<connection_id>/<segment_name>",
    methods=["GET"],
)
@stream_key_required
@skip_stream_connect_audit
async def stream_upstream_vod_series_hls_segment(
    source_id: int,
    item_id: int,
    upstream_episode_id: str,
    connection_id: str,
    segment_name: str,
):
    return await _stream_cso_vod_hls_segment(
        lambda config: _resolve_upstream_vod_request(
            config,
            int(source_id),
            VOD_KIND_SERIES,
            int(item_id),
            upstream_episode_id=upstream_episode_id,
        )
    )
