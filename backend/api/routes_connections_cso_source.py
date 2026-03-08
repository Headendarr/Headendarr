#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import asyncio
import logging
import hashlib
import time
import uuid
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from flask import request
from quart import Response, current_app, redirect, stream_with_context
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from backend.api import blueprint
from backend.auth import get_request_client_ip, skip_stream_connect_audit, stream_key_required
from backend.cso import (
    cso_capacity_registry,
    cso_session_manager,
    emit_channel_stream_event,
    is_internal_cso_activity,
    preempt_backpressured_clients_for_capacity_key,
    policy_content_type,
    resolve_channel_for_stream,
    subscribe_channel_hls,
    subscribe_channel_stream,
    subscribe_source_hls,
    subscribe_source_stream,
    source_capacity_key,
    source_capacity_limit,
    source_should_use_cso_buffer,
    CsoOutputReaderEnded,
)
from backend.models import ChannelSource, Session
from backend.hls_multiplexer import parse_size
from backend.stream_activity import (
    get_stream_activity_snapshot,
    stop_stream_activity,
    touch_stream_activity,
    upsert_stream_activity,
)
from backend.stream_profiles import generate_cso_policy_from_profile, resolve_cso_profile_name
from backend.streaming import normalize_local_proxy_url
from backend.url_resolver import get_request_base_url
from backend.channel_stream_health import (
    cancel_background_health_checks_for_capacity_key,
    has_background_health_check_for_capacity_key,
    preempt_background_health_checks_for_channel,
)

CONNECTION_LIMIT_REACHED_MESSAGE = "Channel unavailable due to connection limits"
logger = logging.getLogger("cso.api")


def _get_connection_id(default_new=False):
    value = (request.args.get("connection_id") or request.args.get("cid") or "").strip()
    if value:
        return value
    if default_new:
        return uuid.uuid4().hex
    return None


def _should_allow_unavailable_slate(effective_profile: str, channel) -> bool:
    profile_name = str(effective_profile or "").strip().lower()
    channel_forced_cso = bool(getattr(channel, "cso_enabled", False)) if channel is not None else False
    # For TVH profile traffic, return hard failures unless the channel is explicitly forced through CSO.
    if profile_name == "tvh" and not channel_forced_cso:
        return False
    return True


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

    candidates = sorted(
        sources,
        key=lambda item: int(getattr(item, "priority", 0) or 0),
        reverse=True,
    )
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


def _build_hls_query_string(*, stream_key=None, username=None, profile=None):
    query_items = []
    if stream_key:
        query_items.append(("stream_key", str(stream_key)))
    if username:
        query_items.append(("username", str(username)))
    if profile:
        query_items.append(("profile", str(profile)))
    if not query_items:
        return ""
    return urlencode(query_items)


def _render_hls_playlist(playlist_text: str, segment_base_path: str, query_string: str = "") -> str:
    lines = []
    suffix = f"?{query_string}" if query_string else ""
    for raw_line in str(playlist_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            lines.append(raw_line)
            continue
        segment_name = line.split("?", 1)[0]
        lines.append(f"{segment_base_path.rstrip('/')}/{segment_name}{suffix}")
    return "\n".join(lines) + "\n"


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


async def _connection_limit_blocked_response(config, channel, effective_profile="default"):
    from backend.api.routes_hls_proxy import CSO_UNAVAILABLE_SHOW_SLATE, _cso_unavailable_slate_stream

    allow_unavailable_slate = _should_allow_unavailable_slate(effective_profile, channel)
    if not CSO_UNAVAILABLE_SHOW_SLATE or not allow_unavailable_slate:
        return Response(CONNECTION_LIMIT_REACHED_MESSAGE, status=503)

    resolved_profile = _resolve_cso_profile_for_source_request(config, requested_profile=effective_profile)
    policy = generate_cso_policy_from_profile(config, resolved_profile)

    @stream_with_context
    async def generate_unavailable_slate():
        async for chunk in _cso_unavailable_slate_stream(
            "capacity_blocked",
            policy=policy,
            detail_hint="",
        ):
            yield chunk

    response = Response(
        generate_unavailable_slate(),
        content_type=policy_content_type(policy) or "application/octet-stream",
        status=200,
    )
    response.timeout = None
    return response


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

    return normalize_local_proxy_url(
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
    stream_key = request._stream_key
    username = request._stream_user.username if request._stream_user else request.args.get("username")
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
    force_cso_buffer = effective_profile != "default"

    connection_id = _get_connection_id(default_new=True)
    if connection_id == "tvh":
        connection_id = f"tvh-{uuid.uuid4().hex}"

    capacity_key_name = source_capacity_key(source)
    capacity_limit = int(source_capacity_limit(source) or 0)
    if capacity_limit <= 0:
        return await _connection_limit_blocked_response(
            config,
            channel,
            effective_profile=effective_profile,
        )

    usage = await cso_capacity_registry.get_usage(capacity_key_name)
    if (
        await has_background_health_check_for_capacity_key(capacity_key_name)
        and int(usage.get("total") or 0) >= capacity_limit
    ):
        await cancel_background_health_checks_for_capacity_key(
            capacity_key_name,
            reason="source_stream_playback_priority",
        )

    if source_should_use_cso_buffer(source, force_tvh_remux=force_cso_buffer):
        activity_sessions = await get_stream_activity_snapshot()
        external_count = _active_external_count_for_source(activity_sessions, source, capacity_key_name)
        active_connections = int(usage.get("allocations") or 0) + int(external_count)
        if active_connections >= capacity_limit:
            preempted = await preempt_backpressured_clients_for_capacity_key(capacity_key_name)
            if preempted > 0:
                logger.info(
                    "CSO source handover preempted backpressured client(s) source_id=%s key=%s count=%s",
                    getattr(source, "id", None),
                    capacity_key_name,
                    preempted,
                )
                usage = await cso_capacity_registry.get_usage(capacity_key_name)
                activity_sessions = await get_stream_activity_snapshot()
                external_count = _active_external_count_for_source(activity_sessions, source, capacity_key_name)
                active_connections = int(usage.get("allocations") or 0) + int(external_count)

        if active_connections >= capacity_limit:
            waited = await _wait_for_source_handover_window(
                source=source,
                capacity_key_name=capacity_key_name,
                capacity_limit=capacity_limit,
                client_fingerprint=client_fingerprint,
                exclude_connection_id=connection_id,
            )
            if waited:
                logger.info(
                    "CSO source handover grace cleared capacity source_id=%s key=%s",
                    getattr(source, "id", None),
                    capacity_key_name,
                )
            else:
                logger.info(
                    "CSO source handover grace exhausted source_id=%s key=%s",
                    getattr(source, "id", None),
                    capacity_key_name,
                )
                usage = await cso_capacity_registry.get_usage(capacity_key_name)
                activity_sessions = await get_stream_activity_snapshot()
                external_count = _active_external_count_for_source(activity_sessions, source, capacity_key_name)
                active_connections = int(usage.get("allocations") or 0) + int(external_count)
                if active_connections >= capacity_limit:
                    return await _connection_limit_blocked_response(
                        config,
                        channel,
                        effective_profile=effective_profile,
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

        generator, content_type, error_message, status = await subscribe_source_stream(
            config=config,
            source=source,
            stream_key=stream_key,
            profile=effective_profile,
            connection_id=connection_id,
            prebuffer_bytes=prebuffer_bytes,
            request_base_url=request_base_url,
        )
        if not generator:
            from backend.api.routes_hls_proxy import (
                CSO_UNAVAILABLE_SHOW_SLATE,
                _cso_unavailable_slate_stream,
                _summarize_playback_issue,
            )

            message = (error_message or "").strip()
            allow_unavailable_slate = _should_allow_unavailable_slate(effective_profile, channel)
            if (status or 500) == 503 and CSO_UNAVAILABLE_SHOW_SLATE and allow_unavailable_slate:
                lower_message = message.lower()
                reason = "capacity_blocked" if "connection limit" in lower_message else "playback_unavailable"
                detail_hint = _summarize_playback_issue(message) if reason == "playback_unavailable" else ""

                @stream_with_context
                async def generate_unavailable_slate():
                    async for chunk in _cso_unavailable_slate_stream(
                        reason,
                        policy=effective_policy,
                        detail_hint=detail_hint,
                    ):
                        yield chunk

                return Response(
                    generate_unavailable_slate(),
                    content_type=policy_content_type(effective_policy) or "application/octet-stream",
                    status=200,
                )
            return Response(message or "Unable to start CSO stream", status=status or 500)

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
            user=getattr(request, "_stream_user", None),
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
        output_session_key = f"cso-source-output-{int(source.id)}-{effective_profile}"

        @stream_with_context
        async def generate_source_stream():
            from backend.api.routes_hls_proxy import (
                CSO_UNAVAILABLE_SHOW_SLATE,
                _cso_unavailable_slate_stream,
                _latest_cso_playback_issue_hint,
            )

            touch_identity = source_identity
            last_touch_ts = time.time()
            should_emit_failure_slate = False
            try:
                try:
                    async for chunk in generator:
                        now = time.time()
                        if (now - last_touch_ts) >= 5.0:
                            await touch_stream_activity(connection_id, identity=touch_identity)
                            last_touch_ts = now
                        yield chunk
                except CsoOutputReaderEnded:
                    should_emit_failure_slate = True

                if not should_emit_failure_slate:
                    session = await cso_session_manager.get_output_session(output_session_key)
                    should_emit_failure_slate = bool(
                        session and getattr(session, "last_error", "") == "output_reader_ended"
                    )

                allow_unavailable_slate = _should_allow_unavailable_slate(effective_profile, channel)
                if should_emit_failure_slate and CSO_UNAVAILABLE_SHOW_SLATE and allow_unavailable_slate:
                    reason = "playback_unavailable"
                    detail_hint = await _latest_cso_playback_issue_hint(
                        int(getattr(channel, "id", 0) or 0),
                        session_id=output_session_key,
                    )
                    async for chunk in _cso_unavailable_slate_stream(
                        reason,
                        policy=effective_policy,
                        detail_hint=detail_hint,
                    ):
                        yield chunk
            finally:
                try:
                    close = getattr(generator, "aclose", None)
                    if close is not None:
                        await close()
                except Exception:
                    pass
                await stop_stream_activity(
                    "",
                    connection_id=connection_id,
                    event_type="stream_stop",
                    endpoint_override=request.path,
                    user=getattr(request, "_stream_user", None),
                )

        response = Response(generate_source_stream(), content_type=content_type or "application/octet-stream")
        response.timeout = None
        return response

    target_url = _build_target_url(
        source=source,
        request_base_url=request_base_url,
        instance_id=instance_id,
        stream_key=stream_key,
        username=username,
    )
    if not target_url:
        return Response("No available stream source for this channel", status=503)

    target_url = _append_or_replace_query_param(target_url, "connection_id", connection_id)
    if not _monitorable_target_url(target_url, request_base_url, instance_id):
        return redirect(target_url, code=302)

    usage = await cso_capacity_registry.get_usage(capacity_key_name)
    activity_sessions = await get_stream_activity_snapshot()
    external_count = _active_external_count_for_source(activity_sessions, source, capacity_key_name)
    active_connections = int(usage.get("allocations") or 0) + int(external_count)
    if active_connections >= capacity_limit:
        waited = await _wait_for_source_handover_window(
            source=source,
            capacity_key_name=capacity_key_name,
            capacity_limit=capacity_limit,
            client_fingerprint=client_fingerprint,
            exclude_connection_id=connection_id,
        )
        if waited:
            return redirect(target_url, code=302)
        if await has_background_health_check_for_capacity_key(capacity_key_name):
            await cancel_background_health_checks_for_capacity_key(
                capacity_key_name,
                reason="source_redirect_playback_priority",
            )
            usage = await cso_capacity_registry.get_usage(capacity_key_name)
            activity_sessions = await get_stream_activity_snapshot()
            external_count = _active_external_count_for_source(activity_sessions, source, capacity_key_name)
            active_connections = int(usage.get("allocations") or 0) + int(external_count)
            if active_connections < capacity_limit:
                return redirect(target_url, code=302)
        return await _connection_limit_blocked_response(
            config,
            channel,
            effective_profile=effective_profile,
        )

    return redirect(target_url, code=302)


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

    connection_id = _get_connection_id(default_new=True)
    if connection_id == "tvh":
        # Treat "tvh" as a logical label only. Internally, each request gets a
        # unique client id to avoid teardown collisions across reconnects.
        connection_id = f"tvh-{uuid.uuid4().hex}"
        logger.info(
            "Remapped reserved connection_id requested=tvh effective=%s channel=%s",
            connection_id,
            channel_id_int,
        )
    stream_key = getattr(request, "_stream_key", None)
    client_ip = get_request_client_ip()
    user_agent = request.headers.get("User-Agent")
    client_fingerprint = _client_fingerprint(stream_key, client_ip, user_agent)

    primary_source = await _select_primary_source_for_capacity(channel)
    if primary_source is not None:
        capacity_key_name = source_capacity_key(primary_source)
        capacity_limit = int(source_capacity_limit(primary_source) or 0)
        if capacity_limit > 0:
            usage = await cso_capacity_registry.get_usage(capacity_key_name)
            activity_sessions = await get_stream_activity_snapshot()
            external_count = _active_external_count_for_source(activity_sessions, primary_source, capacity_key_name)
            active_connections = int(usage.get("allocations") or 0) + int(external_count)
            if active_connections >= capacity_limit:
                preempted = await preempt_backpressured_clients_for_capacity_key(capacity_key_name)
                if preempted > 0:
                    logger.info(
                        "CSO channel handover preempted backpressured client(s) channel=%s source_id=%s key=%s count=%s",
                        channel_id_int,
                        getattr(primary_source, "id", None),
                        capacity_key_name,
                        preempted,
                    )
                    usage = await cso_capacity_registry.get_usage(capacity_key_name)
                    activity_sessions = await get_stream_activity_snapshot()
                    external_count = _active_external_count_for_source(
                        activity_sessions, primary_source, capacity_key_name
                    )
                    active_connections = int(usage.get("allocations") or 0) + int(external_count)

            if active_connections >= capacity_limit:
                waited = await _wait_for_source_handover_window(
                    source=primary_source,
                    capacity_key_name=capacity_key_name,
                    capacity_limit=capacity_limit,
                    client_fingerprint=client_fingerprint,
                    exclude_connection_id=connection_id,
                )
                if waited:
                    logger.info(
                        "CSO channel handover grace cleared capacity channel=%s source_id=%s key=%s",
                        channel_id_int,
                        getattr(primary_source, "id", None),
                        capacity_key_name,
                    )
                else:
                    logger.info(
                        "CSO channel handover grace exhausted channel=%s source_id=%s key=%s",
                        channel_id_int,
                        getattr(primary_source, "id", None),
                        capacity_key_name,
                    )
                    usage = await cso_capacity_registry.get_usage(capacity_key_name)
                    activity_sessions = await get_stream_activity_snapshot()
                    external_count = _active_external_count_for_source(
                        activity_sessions, primary_source, capacity_key_name
                    )
                    active_connections = int(usage.get("allocations") or 0) + int(external_count)
                    if active_connections >= capacity_limit:
                        return await _connection_limit_blocked_response(
                            config,
                            channel,
                            effective_profile=effective_profile,
                        )
    if use_hls_output:
        hls_query = _build_hls_query_string(
            stream_key=stream_key,
            username=getattr(getattr(request, "_stream_user", None), "username", None),
            profile=effective_profile,
        )
        target_path = f"/tic-api/cso/channel/{channel_id_int}/hls/{connection_id}/index.m3u8"
        target_url = f"{get_request_base_url(request).rstrip('/')}{target_path}"
        if hls_query:
            target_url = f"{target_url}?{hls_query}"
        return redirect(target_url, code=302)
    generator, content_type, error_message, status = await subscribe_channel_stream(
        config=config,
        channel=channel,
        stream_key=stream_key,
        profile=effective_profile,
        connection_id=connection_id,
        prebuffer_bytes=prebuffer_bytes,
        request_base_url=get_request_base_url(request),
    )
    if not generator:
        from backend.api.routes_hls_proxy import (
            CSO_UNAVAILABLE_SHOW_SLATE,
            _cso_unavailable_slate_stream,
            _summarize_playback_issue,
        )

        message = (error_message or "").strip()
        allow_unavailable_slate = _should_allow_unavailable_slate(effective_profile, channel)
        if (status or 500) == 503 and CSO_UNAVAILABLE_SHOW_SLATE and allow_unavailable_slate:
            lower_message = message.lower()
            reason = "capacity_blocked" if "connection limit" in lower_message else "playback_unavailable"
            detail_hint = _summarize_playback_issue(message) if reason == "playback_unavailable" else ""

            @stream_with_context
            async def generate_unavailable_slate():
                async for chunk in _cso_unavailable_slate_stream(
                    reason,
                    policy=effective_policy,
                    detail_hint=detail_hint,
                ):
                    yield chunk

            return Response(
                generate_unavailable_slate(),
                content_type=policy_content_type(effective_policy) or "application/octet-stream",
                status=200,
            )

        return Response(message or "Unable to start CSO stream", status=status or 500)

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
        user=getattr(request, "_stream_user", None),
        details_override=details_override,
        channel_id=channel_id_int,
        channel_name=channel_name,
        channel_logo_url=channel_logo_url,
        stream_name=channel_name,
        display_url=activity_identity,
    )
    output_session_key = f"cso-output-{channel_id_int}-{effective_profile}"

    @stream_with_context
    async def generate_stream():
        from backend.api.routes_hls_proxy import (
            CSO_UNAVAILABLE_SHOW_SLATE,
            _cso_unavailable_slate_stream,
            _latest_cso_playback_issue_hint,
        )

        touch_identity = activity_identity
        last_touch_ts = time.time()
        should_emit_failure_slate = False
        try:
            try:
                async for chunk in generator:
                    now = time.time()
                    if (now - last_touch_ts) >= 5.0:
                        await touch_stream_activity(connection_id, identity=touch_identity)
                        last_touch_ts = now
                    yield chunk
            except CsoOutputReaderEnded:
                should_emit_failure_slate = True

            if not should_emit_failure_slate:
                session = await cso_session_manager.get_output_session(output_session_key)
                should_emit_failure_slate = bool(
                    session and getattr(session, "last_error", "") == "output_reader_ended"
                )

            allow_unavailable_slate = _should_allow_unavailable_slate(effective_profile, channel)
            if should_emit_failure_slate and CSO_UNAVAILABLE_SHOW_SLATE and allow_unavailable_slate:
                reason = "playback_unavailable"
                detail_hint = await _latest_cso_playback_issue_hint(
                    channel_id_int,
                    session_id=output_session_key,
                )
                async for chunk in _cso_unavailable_slate_stream(
                    reason,
                    policy=effective_policy,
                    detail_hint=detail_hint,
                ):
                    yield chunk
        finally:
            try:
                close = getattr(generator, "aclose", None)
                if close is not None:
                    await close()
            except Exception:
                pass
            await stop_stream_activity(
                "",
                connection_id=connection_id,
                event_type="stream_stop",
                endpoint_override=request.path,
                user=getattr(request, "_stream_user", None),
            )

    response = Response(generate_stream(), content_type=content_type or "application/octet-stream")
    response.timeout = None
    return response


async def _wait_for_hls_playlist(output_session, timeout_seconds=8.0):
    deadline = time.time() + float(timeout_seconds)
    while time.time() < deadline:
        playlist_text = await output_session.read_playlist_text()
        if playlist_text:
            return playlist_text
        await asyncio.sleep(0.25)
    return None


async def _hls_start_failure_response(config, channel, effective_profile, error_message, status):
    message = (error_message or "").strip()
    if int(status or 500) == 503 and "connection limit" in message.lower():
        return await _connection_limit_blocked_response(
            config,
            channel,
            effective_profile=effective_profile,
        )
    return Response(message or "Unable to start CSO HLS stream", status=status or 500)


@blueprint.route("/tic-api/cso/channel/<int:channel_id>/hls/<connection_id>/index.m3u8", methods=["GET"])
@stream_key_required
@skip_stream_connect_audit
async def stream_channel_hls_playlist(channel_id, connection_id):
    config = current_app.config["APP_CONFIG"]
    requested_profile = str(request.args.get("profile") or "hls").strip().lower()
    request_base_url = get_request_base_url(request)
    stream_key = request._stream_key

    channel = await resolve_channel_for_stream(int(channel_id))
    if not channel or not bool(getattr(channel, "enabled", False)):
        return Response("Channel is disabled", status=404)
    await preempt_background_health_checks_for_channel(int(channel_id))
    effective_profile = resolve_cso_profile_name(config, requested_profile, channel=channel)
    effective_policy = generate_cso_policy_from_profile(config, effective_profile)
    if str(effective_policy.get("container") or "").strip().lower() != "hls":
        return Response("Requested profile is not HLS output", status=400)
    output_session_key = f"cso-hls-output-{int(channel_id)}-{effective_profile}"
    existing_client = await _hls_output_has_client(output_session_key, connection_id)

    primary_source = await _select_primary_source_for_capacity(channel)
    if primary_source is not None and not existing_client:
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
                    return await _connection_limit_blocked_response(
                        config,
                        channel,
                        effective_profile=effective_profile,
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
        return await _hls_start_failure_response(
            config,
            channel,
            effective_profile,
            error_message,
            status,
        )

    await output_session.touch_client(connection_id)
    playlist_text = await _wait_for_hls_playlist(output_session)
    if not playlist_text:
        return Response("HLS playlist not ready", status=503)

    query_string = _build_hls_query_string(
        stream_key=stream_key,
        username=getattr(getattr(request, "_stream_user", None), "username", None),
        profile=effective_profile,
    )
    segment_base_path = request.path.rsplit("/", 1)[0]
    rendered_playlist = _render_hls_playlist(playlist_text, segment_base_path, query_string=query_string)
    await upsert_stream_activity(
        f"/tic-api/cso/channel/{int(channel_id)}",
        connection_id=str(connection_id),
        endpoint_override=request.path,
        start_event_type="stream_start",
        user=getattr(request, "_stream_user", None),
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
async def stream_channel_hls_segment(channel_id, connection_id, segment_name):
    config = current_app.config["APP_CONFIG"]
    requested_profile = str(request.args.get("profile") or "hls").strip().lower()
    request_base_url = get_request_base_url(request)
    stream_key = request._stream_key

    channel = await resolve_channel_for_stream(int(channel_id))
    if not channel or not bool(getattr(channel, "enabled", False)):
        return Response("Channel is disabled", status=404)
    effective_profile = resolve_cso_profile_name(config, requested_profile, channel=channel)
    effective_policy = generate_cso_policy_from_profile(config, effective_profile)
    if str(effective_policy.get("container") or "").strip().lower() != "hls":
        return Response("Requested profile is not HLS output", status=400)
    output_session_key = f"cso-hls-output-{int(channel_id)}-{effective_profile}"
    existing_client = await _hls_output_has_client(output_session_key, connection_id)
    primary_source = await _select_primary_source_for_capacity(channel)
    if primary_source is not None and not existing_client:
        capacity_key_name = source_capacity_key(primary_source)
        capacity_limit = int(source_capacity_limit(primary_source) or 0)
        if capacity_limit > 0:
            usage = await cso_capacity_registry.get_usage(capacity_key_name)
            activity_sessions = await get_stream_activity_snapshot()
            external_count = _active_external_count_for_source(activity_sessions, primary_source, capacity_key_name)
            active_connections = int(usage.get("allocations") or 0) + int(external_count)
            if active_connections >= capacity_limit:
                return await _connection_limit_blocked_response(
                    config,
                    channel,
                    effective_profile=effective_profile,
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
        return await _hls_start_failure_response(
            config,
            channel,
            effective_profile,
            error_message,
            status,
        )
    await output_session.touch_client(connection_id)
    await touch_stream_activity(str(connection_id), identity=f"/tic-api/cso/channel/{int(channel_id)}")

    payload = await output_session.read_segment_bytes(segment_name)
    if payload is None:
        return Response("HLS segment not found", status=404)
    return Response(payload, content_type="video/mp2t")


@blueprint.route("/tic-api/cso/channel_stream/<int:stream_id>/hls/<connection_id>/index.m3u8", methods=["GET"])
@stream_key_required
@skip_stream_connect_audit
async def stream_source_hls_playlist(stream_id, connection_id):
    config = current_app.config["APP_CONFIG"]
    requested_profile = str(request.args.get("profile") or "hls").strip().lower()
    request_base_url = get_request_base_url(request)
    stream_key = request._stream_key

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
    capacity_key_name = source_capacity_key(source)
    capacity_limit = int(source_capacity_limit(source) or 0)
    if capacity_limit <= 0 and not existing_client:
        return await _connection_limit_blocked_response(
            config,
            channel,
            effective_profile=effective_profile,
        )
    if not existing_client:
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
                return await _connection_limit_blocked_response(
                    config,
                    channel,
                    effective_profile=effective_profile,
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
        return await _hls_start_failure_response(
            config,
            channel,
            effective_profile,
            error_message,
            status,
        )

    await output_session.touch_client(connection_id)
    playlist_text = await _wait_for_hls_playlist(output_session)
    if not playlist_text:
        return Response("HLS playlist not ready", status=503)

    query_string = _build_hls_query_string(
        stream_key=stream_key,
        username=getattr(getattr(request, "_stream_user", None), "username", None),
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
        user=getattr(request, "_stream_user", None),
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
    stream_key = request._stream_key

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
    capacity_key_name = source_capacity_key(source)
    capacity_limit = int(source_capacity_limit(source) or 0)
    if capacity_limit <= 0 and not existing_client:
        return await _connection_limit_blocked_response(
            config,
            channel,
            effective_profile=effective_profile,
        )
    if not existing_client:
        usage = await cso_capacity_registry.get_usage(capacity_key_name)
        activity_sessions = await get_stream_activity_snapshot()
        external_count = _active_external_count_for_source(activity_sessions, source, capacity_key_name)
        active_connections = int(usage.get("allocations") or 0) + int(external_count)
        if active_connections >= capacity_limit:
            return await _connection_limit_blocked_response(
                config,
                channel,
                effective_profile=effective_profile,
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
        return await _hls_start_failure_response(
            config,
            channel,
            effective_profile,
            error_message,
            status,
        )
    await output_session.touch_client(connection_id)
    await touch_stream_activity(str(connection_id), identity=f"/tic-api/cso/channel_stream/{int(stream_id)}")

    payload = await output_session.read_segment_bytes(segment_name)
    if payload is None:
        return Response("HLS segment not found", status=404)
    return Response(payload, content_type="video/mp2t")
