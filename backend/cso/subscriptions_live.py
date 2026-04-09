import logging
import os
import time

from backend.stream_profiles import generate_cso_policy_from_profile

from .common import build_cso_stream_plan, cso_session_manager, current_quart_app_object
from .constants import CSO_CONSUMER_PROGRESS_LOG_INTERVAL_SECONDS, CSO_UNAVAILABLE_SHOW_SLATE
from .events import (
    emit_channel_stream_event,
    latest_cso_playback_issue_hint,
    source_event_context,
    summarize_cso_playback_issue,
)
from .live_ingest import CsoIngestSession, resolve_cso_ingest_user_agent
from .output import CsoHlsOutputSession, CsoOutputSession
from .policy import policy_content_type
from .slate import CsoSlateSession, should_allow_unavailable_slate
from .sources import cso_source_from_channel_source, order_cso_channel_sources
from .subscriptions_shared import resolve_username_for_stream_key


logger = logging.getLogger("cso")


async def subscribe_channel_hls(
    config,
    channel,
    stream_key,
    profile,
    connection_id,
    request_base_url="",
    on_disconnect=None,
):
    """Attach client to a channel CSO HLS output session."""
    if not channel:
        return None, "Channel not found", 404
    if not channel.enabled:
        return None, "Channel is disabled", 404

    sources = await order_cso_channel_sources(list(channel.sources or []), channel_id=getattr(channel, "id", None))
    sources = [cso_source_from_channel_source(s) for s in sources]

    if not sources:
        return None, "No available stream source for this channel", 503

    policy = generate_cso_policy_from_profile(config, profile)
    ingest_key = f"cso-ingest-{channel.id}"
    output_session_key = f"cso-hls-output-{channel.id}-{profile}"
    capacity_owner_key = f"cso-channel-{channel.id}"
    username = await resolve_username_for_stream_key(config, stream_key)
    ingest_user_agent = resolve_cso_ingest_user_agent(config, sources[0] if sources else None)

    def _ingest_factory():
        return CsoIngestSession(
            ingest_key,
            channel.id,
            sources,
            request_base_url=(request_base_url or "").rstrip("/"),
            instance_id=config.ensure_instance_id(),
            capacity_owner_key=capacity_owner_key,
            stream_key=stream_key,
            username=username,
            ingest_user_agent=ingest_user_agent,
        )

    ingest_session = await cso_session_manager.get_or_create_ingest(ingest_key, _ingest_factory)
    try:
        ingest_session.app = current_quart_app_object()
    except Exception:
        pass
    await ingest_session.start()
    if not ingest_session.running:
        reason = ingest_session.last_error or "no_available_source"
        await emit_channel_stream_event(
            channel_id=channel.id,
            source=sources[0] if sources else None,
            session_id=ingest_key,
            event_type="capacity_blocked" if reason == "capacity_blocked" else "playback_unavailable",
            severity="warning",
            details={"reason": reason, "profile": profile},
        )
        return (
            None,
            (
                "Channel unavailable due to connection limits"
                if reason == "capacity_blocked"
                else "Channel unavailable because playback could not be started"
            ),
            503,
        )

    def _output_factory():
        return CsoHlsOutputSession(
            output_session_key,
            channel.id,
            policy,
            ingest_session,
            cache_root_dir=os.path.join(config.config_path, "cache", "cso_hls"),
        )

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
    await output_session.start()
    if not output_session.running:
        reason = output_session.last_error or "output_not_running"
        logger.warning(
            "CSO HLS output failed to start channel=%s output_key=%s reason=%s",
            channel.id,
            output_session_key,
            reason,
        )
        await emit_channel_stream_event(
            channel_id=channel.id,
            source=getattr(ingest_session, "current_source", None),
            session_id=output_session_key,
            event_type="playback_unavailable",
            severity="warning",
            details={"reason": reason, "profile": profile},
        )
        return None, "Channel unavailable because output pipeline could not be started", 503

    is_new_client = await output_session.add_client(connection_id, on_disconnect=on_disconnect)
    if is_new_client:
        await emit_channel_stream_event(
            channel_id=channel.id,
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


async def subscribe_source_hls(
    config,
    source,
    stream_key,
    profile,
    connection_id,
    request_base_url="",
    on_disconnect=None,
):
    """Attach client to a source CSO HLS output session."""
    if not source:
        return None, "Stream not found", 404

    source = cso_source_from_channel_source(source)

    playlist = source.playlist
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return None, "Stream playlist is disabled", 404
    if not source.url:
        return None, "No available stream source for this channel", 503

    source_id = source.id
    channel_id = source.channel_id
    sources = [source]
    policy = generate_cso_policy_from_profile(config, profile)
    ingest_key = f"cso-source-ingest-{source_id}"
    output_session_key = f"cso-source-hls-output-{source_id}-{profile}"
    capacity_owner_key = f"cso-source-{source_id}"
    username = await resolve_username_for_stream_key(config, stream_key)
    ingest_user_agent = resolve_cso_ingest_user_agent(config, source)

    def _ingest_factory():
        return CsoIngestSession(
            ingest_key,
            channel_id,
            sources,
            request_base_url=(request_base_url or "").rstrip("/"),
            instance_id=config.ensure_instance_id(),
            capacity_owner_key=capacity_owner_key,
            stream_key=stream_key,
            username=username,
            allow_failover=False,
            ingest_user_agent=ingest_user_agent,
        )

    ingest_session = await cso_session_manager.get_or_create_ingest(ingest_key, _ingest_factory)
    await ingest_session.start()
    if not ingest_session.running:
        reason = ingest_session.last_error or "no_available_source"
        await emit_channel_stream_event(
            channel_id=channel_id,
            source=source,
            session_id=ingest_key,
            event_type="capacity_blocked" if reason == "capacity_blocked" else "playback_unavailable",
            severity="warning",
            details={"reason": reason, "profile": profile},
        )
        return (
            None,
            (
                "Channel unavailable due to connection limits"
                if reason == "capacity_blocked"
                else "Channel unavailable because playback could not be started"
            ),
            503,
        )

    def _output_factory():
        return CsoHlsOutputSession(
            output_session_key,
            channel_id,
            policy,
            ingest_session,
            cache_root_dir=os.path.join(config.config_path, "cache", "cso_hls"),
        )

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
    await output_session.start()
    if not output_session.running:
        reason = output_session.last_error or "output_not_running"
        logger.warning(
            "CSO source HLS output failed to start channel=%s source_id=%s output_key=%s reason=%s",
            channel_id,
            source_id,
            output_session_key,
            reason,
        )
        await emit_channel_stream_event(
            channel_id=channel_id,
            source=source,
            session_id=output_session_key,
            event_type="playback_unavailable",
            severity="warning",
            details={"reason": reason, "profile": profile},
        )
        return None, "Channel unavailable because output pipeline could not be started", 503

    is_new_client = await output_session.add_client(connection_id, on_disconnect=on_disconnect)
    if is_new_client:
        await emit_channel_stream_event(
            channel_id=channel_id,
            source=source,
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


async def subscribe_channel_stream(
    config,
    channel,
    stream_key,
    profile,
    connection_id,
    prebuffer_bytes=0,
    request_base_url="",
):
    """Subscribe a playback client to a channel/profile CSO output session.

    Returns:
    - `(generator, content_type, error_message, status_code)`
      where `generator` is an async byte iterator when successful, otherwise
      `None` with error details.
    """
    if not channel:
        return build_cso_stream_plan(None, None, "Channel not found", 404)
    if not channel.enabled:
        return build_cso_stream_plan(None, None, "Channel is disabled", 404)

    sources = await order_cso_channel_sources(list(channel.sources or []), channel_id=getattr(channel, "id", None))
    sources = [cso_source_from_channel_source(s) for s in sources]

    if not sources:
        await emit_channel_stream_event(
            channel_id=channel.id,
            session_id=connection_id,
            event_type="capacity_blocked",
            severity="warning",
            details={"reason": "no_eligible_source"},
        )
        return build_cso_stream_plan(None, None, "No available stream source for this channel", 503)

    policy = generate_cso_policy_from_profile(config, profile)
    ingest_key = f"cso-ingest-{channel.id}"
    output_session_key = f"cso-output-{channel.id}-{profile}"
    capacity_owner_key = f"cso-channel-{channel.id}"
    username = await resolve_username_for_stream_key(config, stream_key)
    ingest_user_agent = resolve_cso_ingest_user_agent(config, sources[0] if sources else None)
    allow_unavailable_slate = should_allow_unavailable_slate(
        profile, channel=channel, source=sources[0] if sources else None
    )

    # Init slate pipeline
    slate_session = None
    if CSO_UNAVAILABLE_SHOW_SLATE and allow_unavailable_slate:
        slate_session = CsoSlateSession(
            f"{output_session_key}-slate",
            config_path=config.config_path,
        )

    # Init ingest pipeline
    def _ingest_factory():
        return CsoIngestSession(
            ingest_key,
            channel.id,
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
    if not ingest_session.running:
        reason = ingest_session.last_error or "no_available_source"
        await emit_channel_stream_event(
            channel_id=channel.id,
            session_id=ingest_key,
            event_type="capacity_blocked" if reason == "capacity_blocked" else "playback_unavailable",
            severity="warning",
            details={"reason": reason, "profile": profile},
        )
        message = (
            "Channel unavailable due to connection limits"
            if reason == "capacity_blocked"
            else "Channel unavailable because playback could not be started"
        )
        detail_hint = summarize_cso_playback_issue(message) if reason == "playback_unavailable" else ""
        plan = ingest_session.build_unavailable_stream_plan(
            policy,
            reason,
            detail_hint=detail_hint,
            profile_name=profile,
            channel=channel,
            status_code=503,
        )
        if plan.generator is not None:
            return plan
        return build_cso_stream_plan(None, None, message, 503)

    # Init output pipeline
    def _output_factory():
        return CsoOutputSession(output_session_key, channel.id, policy, ingest_session, slate_session)

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
    output_session.slate_session = slate_session
    output_session.event_source = None
    await output_session.start()
    if not output_session.running:
        reason = output_session.last_error or "output_not_running"
        await emit_channel_stream_event(
            channel_id=channel.id,
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
            channel=channel,
            status_code=503,
        )
        if plan.generator is not None:
            return plan
        return build_cso_stream_plan(
            None, None, "Channel unavailable because output pipeline could not be started", 503
        )

    queue = await output_session.add_client(connection_id, prebuffer_bytes=prebuffer_bytes)
    content_type = policy_content_type(policy)
    await emit_channel_stream_event(
        channel_id=channel.id,
        source=ingest_session.current_source,
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

    async def _generator():
        last_chunk_ts = 0.0
        last_progress_log_ts = 0.0
        emitted_bytes = 0
        emitted_chunks = 0
        try:
            while True:
                chunk = await queue.get()
                if chunk is None:
                    if output_session.last_error == "output_reader_ended":
                        detail_hint = await latest_cso_playback_issue_hint(channel.id, session_id=output_session_key)
                        unavailable_plan = ingest_session.build_unavailable_stream_plan(
                            policy,
                            "playback_unavailable",
                            detail_hint=detail_hint,
                            profile_name=profile,
                            channel=channel,
                            status_code=503,
                        )
                        if unavailable_plan.generator is not None:
                            async for unavailable_chunk in unavailable_plan.generator:
                                yield unavailable_chunk
                            break
                        break
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
                    logger.info(
                        "CSO output consumer progress channel=%s output_key=%s connection_id=%s yielded_chunks=%s yielded_bytes=%s yield_gap_ms=%s queue_items=%s queue_bytes=%s queue_max_bytes=%s queue_oldest_age_ms=%s",
                        channel.id,
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
                channel_id=channel.id,
                source=getattr(ingest_session, "current_source", None),
                session_id=output_session_key,
                event_type="session_end",
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

    return build_cso_stream_plan(_generator(), content_type, None, 200)


async def subscribe_source_stream(
    config,
    source,
    stream_key,
    profile,
    connection_id,
    prebuffer_bytes=0,
    request_base_url="",
):
    """Subscribe a playback client to a single-source CSO output session."""
    if not source:
        return build_cso_stream_plan(None, None, "Source not found", 404)

    source = cso_source_from_channel_source(source)

    playlist = source.playlist
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return build_cso_stream_plan(None, None, "Source playlist is disabled", 404)

    if not source.url:
        return build_cso_stream_plan(None, None, "No available stream source for this channel", 503)

    source_id = source.id
    channel_id = source.channel_id
    sources = [source]

    policy = generate_cso_policy_from_profile(config, profile)
    ingest_key = f"cso-source-ingest-{source_id}"
    output_session_key = f"cso-source-output-{source_id}-{profile}"
    capacity_owner_key = f"cso-source-{source_id}"
    username = await resolve_username_for_stream_key(config, stream_key)
    ingest_user_agent = resolve_cso_ingest_user_agent(config, source)
    allow_unavailable_slate = should_allow_unavailable_slate(
        profile, channel=getattr(source, "channel", None), source=source
    )

    # Init slate pipeline
    slate_session = None
    if CSO_UNAVAILABLE_SHOW_SLATE and allow_unavailable_slate:
        slate_session = CsoSlateSession(
            f"{output_session_key}-slate",
            config_path=config.config_path,
        )

    # Init ingest pipeline
    def _ingest_factory():
        return CsoIngestSession(
            ingest_key,
            channel_id,
            sources,
            request_base_url=(request_base_url or "").rstrip("/"),
            instance_id=config.ensure_instance_id(),
            capacity_owner_key=capacity_owner_key,
            stream_key=stream_key,
            username=username,
            allow_failover=False,
            ingest_user_agent=ingest_user_agent,
            slate_session=slate_session,
        )

    ingest_session = await cso_session_manager.get_or_create_ingest(ingest_key, _ingest_factory)
    ingest_session.slate_session = slate_session
    await ingest_session.start()
    if not ingest_session.running:
        reason = ingest_session.last_error or "no_available_source"
        await emit_channel_stream_event(
            channel_id=channel_id,
            source=source,
            session_id=ingest_key,
            event_type="capacity_blocked" if reason == "capacity_blocked" else "playback_unavailable",
            severity="warning",
            details={"reason": reason, "profile": profile},
        )
        message = (
            "Channel unavailable due to connection limits"
            if reason == "capacity_blocked"
            else "Channel unavailable because playback could not be started"
        )
        detail_hint = summarize_cso_playback_issue(message) if reason == "playback_unavailable" else ""
        plan = ingest_session.build_unavailable_stream_plan(
            policy,
            reason,
            detail_hint=detail_hint,
            profile_name=profile,
            channel=getattr(source, "channel", None),
            source=source,
            status_code=503,
        )
        if plan.generator is not None:
            return plan
        return build_cso_stream_plan(None, None, message, 503)

    # Init output pipeline
    def _output_factory():
        return CsoOutputSession(
            output_session_key,
            channel_id,
            policy,
            ingest_session,
            slate_session,
            event_source=source,
        )

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
    output_session.slate_session = slate_session
    output_session.event_source = source
    await output_session.start()
    if not output_session.running:
        reason = output_session.last_error or "output_not_running"
        await emit_channel_stream_event(
            channel_id=channel_id,
            source=source,
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
            channel=getattr(source, "channel", None),
            source=source,
            status_code=503,
        )
        if plan.generator is not None:
            return plan
        return build_cso_stream_plan(
            None, None, "Channel unavailable because output pipeline could not be started", 503
        )

    queue = await output_session.add_client(connection_id, prebuffer_bytes=prebuffer_bytes)
    content_type = policy_content_type(policy)
    await emit_channel_stream_event(
        channel_id=channel_id,
        source=source,
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

    async def _generator():
        last_chunk_ts = 0.0
        last_progress_log_ts = 0.0
        emitted_bytes = 0
        emitted_chunks = 0
        try:
            while True:
                chunk = await queue.get()
                if chunk is None:
                    if output_session.last_error == "output_reader_ended":
                        detail_hint = await latest_cso_playback_issue_hint(channel_id, session_id=output_session_key)
                        unavailable_plan = ingest_session.build_unavailable_stream_plan(
                            policy,
                            "playback_unavailable",
                            detail_hint=detail_hint,
                            profile_name=profile,
                            channel=getattr(source, "channel", None),
                            source=source,
                            status_code=503,
                        )
                        if unavailable_plan.generator is not None:
                            async for unavailable_chunk in unavailable_plan.generator:
                                yield unavailable_chunk
                            break
                        break
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
                    logger.info(
                        "CSO output consumer progress channel=%s output_key=%s connection_id=%s yielded_chunks=%s yielded_bytes=%s yield_gap_ms=%s queue_items=%s queue_bytes=%s queue_max_bytes=%s queue_oldest_age_ms=%s",
                        channel_id,
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
                channel_id=channel_id,
                source=source,
                session_id=output_session_key,
                event_type="session_end",
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

    return build_cso_stream_plan(_generator(), content_type, None, 200)
