import logging
import os
import time
from typing import Any

from quart import request

from backend.stream_profiles import generate_cso_policy_from_profile
from backend.vod import VodCuratedPlaybackCandidate, VodSourcePlaybackCandidate

from .common import build_cso_stream_plan, cso_session_manager, current_quart_app_object
from .constants import CSO_CONSUMER_PROGRESS_LOG_INTERVAL_SECONDS, CSO_UNAVAILABLE_SHOW_SLATE
from .events import emit_channel_stream_event, source_event_context, summarize_cso_playback_issue
from .live_ingest import CsoIngestSession, resolve_cso_ingest_user_agent
from .output import CsoHlsOutputSession, CsoOutputSession
from .policy import (
    generate_vod_channel_ingest_policy,
    policy_content_type,
    resolve_vod_channel_output_policy,
)
from .slate import CsoSlateSession
from .sources import cso_source_from_vod_source
from .subscriptions_shared import resolve_username_for_stream_key
from .vod_cache import vod_cache_manager
from .vod_ingest import VodChannelIngestSession


logger = logging.getLogger("cso")


async def subscribe_vod_stream(
    config,
    candidate,
    upstream_url,
    stream_key,
    profile,
    connection_id,
    episode=None,
    prebuffer_bytes=0,
    request_base_url="",
):
    """Subscribe a playback client to a VOD item/episode CSO output session."""
    if not candidate:
        return build_cso_stream_plan(None, None, "VOD item not found", 404)

    item = candidate.group_item if isinstance(candidate, VodCuratedPlaybackCandidate) else None
    source = await cso_source_from_vod_source(candidate, upstream_url)

    if not source:
        return build_cso_stream_plan(None, None, "Source not found", 404)

    playlist = source.playlist
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return build_cso_stream_plan(None, None, "Source playlist is disabled", 404)

    if not source.url:
        return build_cso_stream_plan(None, None, "No available stream source", 503)

    source_id = source.id
    sources = [source]

    policy = generate_cso_policy_from_profile(config, profile)
    ingest_key = f"cso-vod-ingest-{source_id}"
    output_session_key = f"cso-vod-output-{source_id}-{profile}"
    capacity_owner_key = f"cso-vod-{source_id}"
    username = await resolve_username_for_stream_key(config, stream_key)
    ingest_user_agent = resolve_cso_ingest_user_agent(config, source)

    # Init slate pipeline
    slate_session = None
    if CSO_UNAVAILABLE_SHOW_SLATE:
        slate_session = CsoSlateSession(
            f"{output_session_key}-slate",
            config_path=config.config_path,
        )

    # Init ingest pipeline
    def _ingest_factory():
        return CsoIngestSession(
            ingest_key,
            None,  # channel_id is None for VOD
            sources,
            request_base_url=(request_base_url or "").rstrip("/"),
            instance_id=config.ensure_instance_id(),
            capacity_owner_key=capacity_owner_key,
            stream_key=stream_key,
            username=username,
            ingest_user_agent=ingest_user_agent,
            slate_session=slate_session,
        )

    ingest_session = await cso_session_manager.get_or_create_ingest(ingest_key, _ingest_factory)
    try:
        ingest_session.app = current_quart_app_object()
    except Exception:
        pass
    ingest_session.slate_session = slate_session
    await ingest_session.start()

    vod_category_id = None
    vod_item_id = None
    vod_episode_id = None
    if item is not None:
        vod_category_id = item.category_id
        vod_item_id = item.id
    if episode is not None:
        vod_episode_id = episode.id

    if not ingest_session.running:
        reason = ingest_session.last_error or "no_available_source"
        await emit_channel_stream_event(
            vod_category_id=vod_category_id,
            vod_item_id=vod_item_id,
            vod_episode_id=vod_episode_id,
            session_id=ingest_key,
            event_type="capacity_blocked" if reason == "capacity_blocked" else "playback_unavailable",
            severity="warning",
            details={"reason": reason, "profile": profile},
        )
        detail_hint = summarize_cso_playback_issue(reason) if reason == "playback_unavailable" else ""
        plan = ingest_session.build_unavailable_stream_plan(
            policy,
            reason,
            detail_hint=detail_hint,
            profile_name=profile,
            status_code=503,
        )
        if plan.generator is not None:
            return plan
        return build_cso_stream_plan(None, None, "VOD unavailable because playback could not be started", 503)

    # Init output pipeline
    def _output_factory():
        return CsoOutputSession(
            output_session_key,
            None,  # channel_id is None for VOD
            policy,
            ingest_session,
            slate_session,
        )

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
    output_session.slate_session = slate_session
    output_session.event_source = source
    await output_session.start()

    if not output_session.running:
        reason = output_session.last_error or "output_not_running"
        await emit_channel_stream_event(
            vod_category_id=vod_category_id,
            vod_item_id=vod_item_id,
            vod_episode_id=vod_episode_id,
            session_id=output_session_key,
            event_type="playback_unavailable",
            severity="warning",
            details={"reason": reason, "profile": profile},
        )
        detail_hint = summarize_cso_playback_issue(reason)
        plan = ingest_session.build_unavailable_stream_plan(
            policy,
            "playback_unavailable",
            detail_hint=detail_hint,
            profile_name=profile,
            status_code=503,
        )
        if plan.generator is not None:
            return plan
        return build_cso_stream_plan(None, None, "VOD unavailable because output pipeline could not be started", 503)

    queue = await output_session.add_client(connection_id, prebuffer_bytes=prebuffer_bytes)
    content_type = policy_content_type(policy)

    async def _generator():
        emitted_chunks = 0
        emitted_bytes = 0
        last_chunk_ts = 0.0
        last_progress_log_ts = 0.0
        try:
            while True:
                chunk = await queue.get()
                if chunk is None:
                    break
                now_value = time.time()
                emitted_chunks += 1
                emitted_bytes += len(chunk)
                yield_gap_seconds = 0.0
                if last_chunk_ts > 0:
                    yield_gap_seconds = max(0.0, now_value - last_chunk_ts)
                last_chunk_ts = now_value
                yield chunk
                await output_session.touch_client(connection_id)
                if (now_value - last_progress_log_ts) >= float(CSO_CONSUMER_PROGRESS_LOG_INTERVAL_SECONDS):
                    queue_stats = await queue.stats()
                    progress_item_id = vod_item_id
                    if progress_item_id is None and candidate.source_item is not None:
                        progress_item_id = candidate.source_item.id
                    logger.info(
                        "CSO VOD output consumer progress item=%s output_key=%s connection_id=%s yielded_chunks=%s yielded_bytes=%s yield_gap_ms=%s queue_items=%s queue_bytes=%s queue_max_bytes=%s queue_oldest_age_ms=%s",
                        progress_item_id,
                        output_session_key,
                        connection_id,
                        emitted_chunks,
                        emitted_bytes,
                        int(yield_gap_seconds * 1000),
                        int(queue_stats.get("queued_items") or 0),
                        int(queue_stats.get("queued_bytes") or 0),
                        int(queue_stats.get("max_bytes") or 0),
                        int(float(queue_stats.get("oldest_age_seconds") or 0.0) * 1000),
                    )
                    last_progress_log_ts = now_value
        finally:
            await output_session.remove_client(connection_id)
            await emit_channel_stream_event(
                vod_category_id=vod_category_id,
                vod_item_id=vod_item_id,
                vod_episode_id=vod_episode_id,
                source=ingest_session.current_source,
                session_id=output_session_key,
                event_type="session_end",
                severity="info",
                details={
                    "profile": profile,
                    "connection_id": connection_id,
                    **source_event_context(
                        ingest_session.current_source,
                        source_url=ingest_session.current_source_url,
                    ),
                },
            )

    return build_cso_stream_plan(_generator(), content_type, None, 200)


async def subscribe_vod_hls(
    config,
    candidate,
    upstream_url,
    stream_key,
    profile,
    connection_id,
    episode=None,
    request_base_url="",
    on_disconnect=None,
    start_seconds=0,
):
    """Subscribe a playback client to a VOD item/episode CSO HLS output session."""
    if not candidate:
        return None, "VOD item not found", 404

    item = candidate.group_item if isinstance(candidate, VodCuratedPlaybackCandidate) else None
    source = await cso_source_from_vod_source(candidate, upstream_url)

    if not source:
        return None, "Source not found", 404

    playlist = source.playlist
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return None, "Source playlist is disabled", 404

    source_id = source.id
    policy = generate_cso_policy_from_profile(config, profile)
    start_value = max(0, int(start_seconds or 0))
    output_session_key = f"cso-vod-hls-output-{source_id}-{profile}-start{start_value}"
    ingest_user_agent = resolve_cso_ingest_user_agent(config, source)
    request_headers = {}
    for name, value in dict(getattr(request, "headers", {}) or {}).items():
        if str(name).lower() == "range":
            continue
        request_headers[name] = value
    cache_entry = await vod_cache_manager.get_or_create(source, source.url)
    local_cache_ready = bool(cache_entry.complete and cache_entry.final_path.exists())
    input_target = str(cache_entry.final_path) if local_cache_ready else str(source.url or "").strip()
    input_is_url = not local_cache_ready
    if not input_target:
        return None, "No available stream source", 503

    vod_category_id = None
    vod_item_id = None
    vod_episode_id = None
    if item is not None:
        vod_category_id = item.category_id
        vod_item_id = item.id
    if episode is not None:
        vod_episode_id = episode.id

    def _output_factory():
        return CsoHlsOutputSession(
            output_session_key,
            None,  # channel_id is None for VOD
            policy,
            None,
            cache_root_dir=os.path.join(config.config_path, "cache", "cso_hls"),
            event_source=source,
            input_target=input_target,
            input_is_url=input_is_url,
            input_user_agent=ingest_user_agent,
            input_request_headers=request_headers,
            start_seconds=start_value,
        )

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
    await output_session.start()
    if not output_session.running:
        reason = output_session.last_error or "output_not_running"
        await emit_channel_stream_event(
            vod_category_id=vod_category_id,
            vod_item_id=vod_item_id,
            vod_episode_id=vod_episode_id,
            session_id=output_session_key,
            event_type="playback_unavailable",
            severity="warning",
            details={"reason": reason, "profile": profile},
        )
        return None, "VOD unavailable because output pipeline could not be started", 503

    is_new_client = await output_session.add_client(connection_id, on_disconnect=on_disconnect)
    if is_new_client:
        await emit_channel_stream_event(
            vod_category_id=vod_category_id,
            vod_item_id=vod_item_id,
            vod_episode_id=vod_episode_id,
            source=source,
            session_id=output_session_key,
            event_type="session_start",
            severity="info",
            details={
                "profile": profile,
                "connection_id": connection_id,
                **source_event_context(
                    source,
                    source_url=upstream_url or input_target,
                ),
            },
        )
    return output_session, None, 200


async def subscribe_vod_channel_output_stream(
    config: Any,
    channel_id: int,
    stream_key: str,
    profile: str,
    connection_id: str,
    request_headers: dict[str, str] | None = None,
) -> Any:
    requested_policy = generate_cso_policy_from_profile(config, profile)
    ingest_policy = generate_vod_channel_ingest_policy(config, requested_policy)
    policy = resolve_vod_channel_output_policy(requested_policy, ingest_policy=ingest_policy)
    ingest_key = f"cso-vod-channel-ingest-{int(channel_id)}"
    output_session_key = f"cso-vod-channel-output-{int(channel_id)}-{profile}"

    def _ingest_factory():
        return VodChannelIngestSession(
            ingest_key,
            config,
            int(channel_id),
            stream_key=stream_key,
            request_headers=request_headers,
            output_policy=policy,
        )

    ingest_session = await cso_session_manager.get_or_create_ingest(ingest_key, _ingest_factory)
    await ingest_session.start()
    if not ingest_session.running:
        reason = ingest_session.last_error or "no_scheduled_programme"
        if reason == "no_scheduled_programme":
            return build_cso_stream_plan(None, None, "No scheduled VOD programme is currently available", 404)
        return build_cso_stream_plan(None, None, "Unable to start VOD channel ingest", 503)

    def _output_factory():
        return CsoOutputSession(
            output_session_key,
            int(channel_id),
            policy,
            ingest_session,
        )

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
    await output_session.start()
    if not output_session.running:
        return build_cso_stream_plan(None, None, "Unable to start VOD channel output", 503)

    queue = await output_session.add_client(connection_id, prebuffer_bytes=0)
    content_type = policy_content_type(policy)

    async def _generator():
        try:
            while True:
                chunk = await queue.get()
                if chunk is None:
                    break
                yield chunk
                await output_session.touch_client(connection_id)
        finally:
            await output_session.remove_client(connection_id)

    return build_cso_stream_plan(_generator(), content_type, None, 200)


async def subscribe_vod_channel_hls(
    config: Any,
    channel_id: int,
    stream_key: str,
    profile: str,
    connection_id: str,
    request_headers: dict[str, str] | None = None,
    on_disconnect: Any = None,
) -> tuple[Any | None, str | None, int]:
    requested_policy = generate_cso_policy_from_profile(config, profile)
    ingest_policy = generate_vod_channel_ingest_policy(config, requested_policy)
    policy = resolve_vod_channel_output_policy(requested_policy, ingest_policy=ingest_policy)
    ingest_key = f"cso-vod-channel-ingest-{int(channel_id)}"
    output_session_key = f"cso-vod-channel-hls-output-{int(channel_id)}-{profile}"

    def _ingest_factory():
        return VodChannelIngestSession(
            ingest_key,
            config,
            int(channel_id),
            stream_key=stream_key,
            request_headers=request_headers,
            output_policy=policy,
        )

    ingest_session = await cso_session_manager.get_or_create_ingest(ingest_key, _ingest_factory)
    await ingest_session.start()
    if not ingest_session.running:
        reason = ingest_session.last_error or "no_scheduled_programme"
        if reason == "no_scheduled_programme":
            return None, "No scheduled VOD programme is currently available", 404
        return None, "Unable to start VOD channel ingest", 503

    def _output_factory():
        return CsoHlsOutputSession(
            output_session_key,
            int(channel_id),
            policy,
            ingest_session,
            cache_root_dir=os.path.join(config.config_path, "cache", "cso_hls"),
        )

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
    await output_session.start()
    if not output_session.running:
        return None, "Unable to start VOD channel HLS output", 503

    is_new_client = await output_session.add_client(connection_id, on_disconnect=on_disconnect)
    if is_new_client:
        await emit_channel_stream_event(
            channel_id=int(channel_id),
            source=getattr(ingest_session, "current_source", None),
            session_id=output_session_key,
            event_type="session_start",
            severity="info",
            details={
                "profile": profile,
                "connection_id": connection_id,
                **source_event_context(
                    ingest_session.current_source,
                    source_url=getattr(ingest_session, "current_source_url", None),
                ),
            },
        )
    return output_session, None, 200
