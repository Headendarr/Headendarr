import logging
from backend.vod import VodCuratedPlaybackCandidate, VodSourcePlaybackCandidate

from .common import build_cso_stream_plan
from .events import emit_channel_stream_event, source_event_context
from .policy import policy_content_type
from .sources import cso_source_from_vod_source
from .subscriptions_shared import should_use_vod_proxy_session
from .vod_proxy import vod_proxy_session_manager
from .vod_cache import vod_cache_manager

logger = logging.getLogger("cso")


async def subscribe_vod_proxy_stream(
    candidate,
    upstream_url,
    connection_id,
    request_headers=None,
    episode=None,
    source_override=None,
):
    if not candidate:
        return build_cso_stream_plan(None, None, "VOD item not found", 404)

    if isinstance(candidate, VodCuratedPlaybackCandidate):
        item = candidate.group_item
        vod_category_id = item.category_id
        vod_item_id = item.id
    else:
        item = None
        vod_category_id = None
        vod_item_id = None
    source = source_override or await cso_source_from_vod_source(candidate, upstream_url)
    if not source:
        return build_cso_stream_plan(None, None, "Source not found", 404)

    playlist = source.playlist
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return build_cso_stream_plan(None, None, "Source playlist is disabled", 404)
    local_cache_ready = False
    if not source.url:
        entry = await vod_cache_manager.get_or_create(source, upstream_url or "")
        local_cache_ready = bool(entry.complete and entry.final_path.exists())
    if not source.url and not local_cache_ready:
        return build_cso_stream_plan(None, None, "No available stream source", 503)

    session_key = f"vod-proxy-{source.id}-{connection_id}"
    session = await vod_proxy_session_manager.create(
        session_key,
        source,
        source.url,
        request_headers=request_headers,
    )
    started = await session.start()
    if not started:
        reason = session.last_error or "proxy_start_failed"
        await emit_channel_stream_event(
            vod_category_id=vod_category_id,
            vod_item_id=vod_item_id,
            vod_episode_id=episode.id if episode else None,
            source=source,
            session_id=session_key,
            event_type="capacity_blocked" if reason == "capacity_blocked" else "playback_unavailable",
            severity="warning",
            details={
                "reason": reason,
                **source_event_context(source, source_url=source.url),
            },
        )
        await session.stop(force=True)
        return build_cso_stream_plan(
            None,
            None,
            "Source capacity limit reached" if reason == "capacity_blocked" else "Unable to start proxy stream",
            503 if reason == "capacity_blocked" else 502,
        )

    await emit_channel_stream_event(
        vod_category_id=vod_category_id,
        vod_item_id=vod_item_id,
        vod_episode_id=episode.id if episode else None,
        source=source,
        session_id=session_key,
        event_type="session_start",
        severity="info",
        details={
            "mode": "proxy",
            "connection_id": connection_id,
            **source_event_context(source, source_url=source.url),
        },
    )

    async def _generator():
        try:
            async for chunk in session.iter_bytes():
                yield chunk
        finally:
            await emit_channel_stream_event(
                vod_category_id=vod_category_id,
                vod_item_id=vod_item_id,
                vod_episode_id=getattr(episode, "id", None),
                source=source,
                session_id=session_key,
                event_type="session_end",
                severity="info",
                details={
                    "mode": "proxy",
                    "connection_id": connection_id,
                    **source_event_context(source, source_url=source.url),
                },
            )

    return build_cso_stream_plan(
        _generator(),
        session.content_type or policy_content_type({"container": source.container_extension or "matroska"}),
        None,
        int(session.status_code or 200),
        headers=session.response_headers,
    )
