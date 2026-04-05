import logging

from backend.users import get_user_by_stream_key
from backend.utils import clean_key, clean_text
from backend.vod import VodCuratedPlaybackCandidate, VodSourcePlaybackCandidate

from .constants import CS_VOD_USE_PROXY_SESSION


logger = logging.getLogger("cso")


async def resolve_username_for_stream_key(config, stream_key):
    key = clean_text(stream_key)
    if not key:
        return None
    try:
        tvh_stream_user = await config.get_tvh_stream_user()
        if tvh_stream_user and clean_text(tvh_stream_user.get("stream_key")) == key:
            return tvh_stream_user.get("username")
    except Exception:
        pass
    try:
        user = await get_user_by_stream_key(key)
        if user:
            return user.username
    except Exception:
        pass
    return None


def vod_passthrough_profile_for_source(candidate: VodCuratedPlaybackCandidate | VodSourcePlaybackCandidate) -> str:
    """Return the direct-stream profile id that matches the source container."""
    source_container = ""
    if isinstance(candidate, VodCuratedPlaybackCandidate) and candidate.episode_source is not None:
        source_container = clean_key(candidate.episode_source.container_extension)
    if not source_container:
        source_container = clean_key(candidate.source_item.container_extension)
    if isinstance(candidate, VodCuratedPlaybackCandidate) and not source_container:
        source_container = clean_key(candidate.group_item.container_extension)

    container_profile_map = {
        "ts": "mpegts",
        "mpegts": "mpegts",
        "mkv": "matroska",
        "matroska": "matroska",
        "mp4": "mp4",
        "webm": "webm",
    }
    return container_profile_map.get(source_container, "")


def should_use_vod_proxy_session(
    candidate: VodCuratedPlaybackCandidate | VodSourcePlaybackCandidate, requested_profile: str
) -> bool:
    """Return True when the requested output can be served from a direct proxy session."""
    if not CS_VOD_USE_PROXY_SESSION:
        return False
    profile_name = clean_key(requested_profile)
    if not candidate or profile_name in {"", "hls"}:
        return False
    return profile_name == vod_passthrough_profile_for_source(candidate)
