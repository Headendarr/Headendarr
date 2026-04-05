import os
import time

from backend.utils import clean_key

from .common import build_cso_stream_plan, cso_session_manager
from .constants import CSO_UNAVAILABLE_SHOW_SLATE
from .output import CsoHlsOutputSession, CsoOutputSession
from .policy import policy_content_type
from .slate import CsoSlateSession, cso_unavailable_duration_seconds, should_allow_unavailable_slate
from .types import CsoSource


def subscribe_slate_stream(
    config, policy, reason, detail_hint="", profile_name="", channel=None, source: CsoSource = None, status_code=503
):
    allow_unavailable_slate = should_allow_unavailable_slate(profile_name, channel=channel, source=source)
    if not CSO_UNAVAILABLE_SHOW_SLATE or not allow_unavailable_slate:
        message = (
            "Channel unavailable due to connection limits"
            if reason == "capacity_blocked"
            else "Unable to start CSO stream"
        )
        return build_cso_stream_plan(None, None, message, status_code)

    reason_key = clean_key(reason, fallback="playback_unavailable")
    resolved_duration = cso_unavailable_duration_seconds(reason_key)
    unique_suffix = int(time.time() * 1000)
    slate_session = CsoSlateSession(
        key=f"cso-terminal-slate-{reason_key}-{unique_suffix}",
        config_path=config.config_path,
        reason=reason_key,
        detail_hint=detail_hint,
        duration_seconds=resolved_duration,
    )

    async def _generator():
        output_session = CsoOutputSession(
            key=f"cso-terminal-output-{reason_key}-{unique_suffix}",
            channel_id=getattr(channel, "id", None) if channel is not None else None,
            policy=policy,
            ingest_session=None,
            slate_session=slate_session,
            event_source=source,
            use_slate_as_input=True,
        )
        subscriber_id = f"{output_session.key}-subscriber"
        await output_session.start()
        queue = await output_session.add_client(subscriber_id, prebuffer_bytes=0)
        try:
            while True:
                chunk = await queue.get()
                if chunk is None:
                    break
                yield chunk
        finally:
            try:
                await output_session.remove_client(subscriber_id)
            except Exception:
                pass
            try:
                await output_session.stop(force=True)
            except Exception:
                pass

    return build_cso_stream_plan(
        _generator(),
        policy_content_type(policy) or "application/octet-stream",
        None,
        200,
        cutoff_seconds=cso_unavailable_duration_seconds(reason),
        final_status_code=int(status_code or 503),
    )


async def subscribe_slate_hls(
    config,
    policy,
    reason,
    connection_id,
    on_disconnect=None,
    detail_hint="",
    profile_name="",
    channel=None,
    source=None,
    status_code=503,
):
    allow_unavailable_slate = should_allow_unavailable_slate(profile_name, channel=channel, source=source)
    if not CSO_UNAVAILABLE_SHOW_SLATE or not allow_unavailable_slate:
        message = (
            "Channel unavailable due to connection limits"
            if reason == "capacity_blocked"
            else "Unable to start CSO stream"
        )
        return None, message, status_code

    reason_key = clean_key(reason, fallback="playback_unavailable")
    unique_scope = getattr(channel, "id", None)
    if unique_scope is None and source is not None:
        unique_scope = getattr(source, "id", None)
    unique_scope = clean_key(str(unique_scope or "global"), fallback="global")
    slate_key = f"cso-terminal-hls-slate-{reason_key}-{unique_scope}-{connection_id}"
    output_key = f"cso-terminal-hls-output-{reason_key}-{unique_scope}-{connection_id}"
    resolved_duration = cso_unavailable_duration_seconds(reason_key)

    def _slate_factory():
        return CsoSlateSession(
            key=slate_key,
            config_path=config.config_path,
            reason=reason_key,
            detail_hint=detail_hint,
            duration_seconds=resolved_duration,
        )

    slate_session = await cso_session_manager.get_or_create_slate(slate_key, _slate_factory)

    def _output_factory():
        return CsoHlsOutputSession(
            output_key,
            getattr(channel, "id", None) if channel is not None else None,
            policy,
            ingest_session=None,
            cache_root_dir=os.path.join(config.config_path, "cache", "cso_hls"),
            slate_session=slate_session,
            use_slate_as_input=True,
            event_source=source,
        )

    output_session = await cso_session_manager.get_or_create_output(output_key, _output_factory)
    await output_session.start()
    if not output_session.running:
        return None, "Unable to start CSO HLS slate output", status_code

    await output_session.add_client(connection_id, on_disconnect=on_disconnect)
    return output_session, None, 200
