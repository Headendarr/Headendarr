import json
import logging
from datetime import timedelta

from sqlalchemy import delete, select

from backend.models import CsoEventLog, Session, VodCategoryEpisode, VodCategoryItem
from backend.utils import clean_text, convert_to_int, utc_now_naive

from .types import CsoSource


logger = logging.getLogger("cso")


def source_event_context(source: CsoSource, source_url=None):
    if not source:
        return {}
    playlist = source.playlist
    stream_name = ""
    if source.source_type == "channel":
        stream_name = clean_text(getattr(source, "playlist_stream_name", ""))

    playlist_name = clean_text(getattr(playlist, "name", ""))
    payload = {
        "source_id": source.id,
        "playlist_id": source.playlist_id,
        "playlist_name": playlist_name or None,
        "stream_name": stream_name or None,
        "source_priority": source.priority,
    }
    if source_url or source.url:
        payload["source_url"] = source_url or source.url
    return payload


async def emit_channel_stream_event(
    channel_id=None,
    source_id=None,
    playlist_id=None,
    recording_id=None,
    vod_category_id=None,
    vod_item_id=None,
    vod_episode_id=None,
    tvh_subscription_id=None,
    session_id=None,
    event_type=None,
    severity="info",
    details=None,
    source: CsoSource | None = None,
):
    if not event_type:
        raise ValueError("event_type is required")

    async def _resolve_vod_event_targets(event_source: CsoSource):
        event_category_id = None
        event_item_id = None
        event_episode_id = None

        if event_source is None:
            return event_category_id, event_item_id, event_episode_id

        if event_source.source_type == "vod_movie":
            event_item_id = convert_to_int(event_source.internal_id, 0)
            if event_item_id > 0:
                async with Session() as session:
                    item = await session.get(VodCategoryItem, event_item_id)
                if item is not None:
                    event_item_id = item.id
                    event_category_id = item.category_id
                else:
                    event_item_id = None
            return event_category_id, event_item_id, None

        if event_source.source_type == "vod_episode":
            event_episode_id = convert_to_int(event_source.internal_id, 0)
            if event_episode_id > 0:
                async with Session() as session:
                    episode = await session.get(VodCategoryEpisode, event_episode_id)
                    item = (
                        await session.get(VodCategoryItem, int(episode.category_item_id))
                        if episode is not None and int(episode.category_item_id) > 0
                        else None
                    )
                if item is not None and episode is not None:
                    event_item_id = item.id
                    event_category_id = item.category_id
                    event_episode_id = episode.id
                else:
                    event_item_id = None
                    event_episode_id = None
            return event_category_id, event_item_id, event_episode_id

        return event_category_id, event_item_id, event_episode_id

    # If a CsoSource adapter is provided, derive the correct database ID based on its type.
    # This prevents using a VOD ID in the Live TV source_id column (which has a FK constraint).
    if source is not None:
        # Clear any passed source_id to prevent FK conflicts if this is VOD
        source_id = None

        if source.source_type == "channel":
            source_id = source.id
        elif source.source_type == "vod_movie":
            resolved_category_id, resolved_item_id, _resolved_episode_id = await _resolve_vod_event_targets(source)
            if vod_category_id is None:
                vod_category_id = resolved_category_id
            if vod_item_id is None:
                vod_item_id = resolved_item_id
        elif source.source_type == "vod_episode":
            resolved_category_id, resolved_item_id, resolved_episode_id = await _resolve_vod_event_targets(source)
            if vod_category_id is None:
                vod_category_id = resolved_category_id
            if vod_item_id is None:
                vod_item_id = resolved_item_id
            if vod_episode_id is None:
                vod_episode_id = resolved_episode_id

        if playlist_id is None:
            playlist_id = source.playlist_id

    if channel_id is not None and int(channel_id) <= 0:
        channel_id = None
    if source_id is not None and int(source_id) <= 0:
        source_id = None
    if playlist_id is not None and int(playlist_id) <= 0:
        playlist_id = None
    if recording_id is not None and int(recording_id) <= 0:
        recording_id = None
    if vod_category_id is not None and int(vod_category_id) <= 0:
        vod_category_id = None
    if vod_item_id is not None and int(vod_item_id) <= 0:
        vod_item_id = None
    if vod_episode_id is not None and int(vod_episode_id) <= 0:
        vod_episode_id = None

    details_json = None
    if details is not None:
        try:
            details_json = json.dumps(details, sort_keys=True)
        except Exception:
            details_json = json.dumps({"detail": str(details)})
    async with Session() as session:
        async with session.begin():
            session.add(
                CsoEventLog(
                    channel_id=channel_id,
                    source_id=source_id,
                    playlist_id=playlist_id,
                    recording_id=recording_id,
                    vod_category_id=vod_category_id,
                    vod_item_id=vod_item_id,
                    vod_episode_id=vod_episode_id,
                    tvh_subscription_id=tvh_subscription_id,
                    session_id=session_id,
                    event_type=event_type,
                    severity=severity or "info",
                    details_json=details_json,
                )
            )


async def cleanup_channel_stream_events(app_config, retention_days=None):
    settings = app_config.read_settings()
    configured_days = settings.get("settings", {}).get("audit_log_retention_days", 7)
    try:
        days = int(retention_days if retention_days is not None else configured_days)
    except (TypeError, ValueError):
        days = 7
    if days < 1:
        days = 1
    cutoff_dt = utc_now_naive() - timedelta(days=days)
    async with Session() as session:
        result = await session.execute(delete(CsoEventLog).where(CsoEventLog.created_at < cutoff_dt))
        await session.commit()
        return int(result.rowcount or 0)


def summarize_cso_playback_issue(raw_message: str) -> str:
    message = str(raw_message or "").strip()
    if not message:
        return ""
    lower = message.lower()
    if "connection limit" in lower:
        return "Source connection limit reached for this channel."
    if "matroska" in lower and ("aac extradata" in lower or "samplerate" in lower):
        return "Requested Matroska remux is not compatible with source audio. Try profile aac-matroska or default."
    if "could not write header" in lower and "matroska" in lower:
        return "Requested Matroska profile failed to initialize. Try default or aac-matroska."
    if "no available stream source" in lower or "no_available_source" in lower:
        return "No eligible upstream stream is currently available."
    if "output pipeline could not be started" in lower:
        return "Requested profile could not be started for this source. Try default profile."
    if "ingest_start_failed" in lower:
        return "Upstream ingest could not be started for this source."
    compact = " ".join(message.split())
    if len(compact) > 140:
        compact = compact[:140].rstrip() + "..."
    return compact


async def latest_cso_playback_issue_hint(channel_id: int, session_id: str = "") -> str:
    try:
        async with Session() as session:
            stmt = (
                select(CsoEventLog)
                .where(
                    CsoEventLog.channel_id == int(channel_id),
                    CsoEventLog.event_type.in_(["playback_unavailable", "capacity_blocked", "switch_attempt"]),
                )
                .order_by(CsoEventLog.created_at.desc(), CsoEventLog.id.desc())
                .limit(10)
            )
            if session_id:
                stmt = stmt.where(CsoEventLog.session_id == session_id)
            result = await session.execute(stmt)
            rows = result.scalars().all()
    except Exception:
        return ""

    for row in rows:
        try:
            details = json.loads(row.details_json or "{}")
        except Exception:
            details = {}
        ffmpeg_error = str(details.get("ffmpeg_error") or "").strip()
        reason = str(details.get("reason") or details.get("after_failure_reason") or "").strip()
        if ffmpeg_error:
            return summarize_cso_playback_issue(ffmpeg_error)
        if reason:
            return summarize_cso_playback_issue(reason)
    return ""
