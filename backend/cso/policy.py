from typing import Any

from backend.stream_profiles import content_type_for_media_path, generate_cso_policy_from_profile
from backend.utils import clean_key

from .constants import CONTAINER_TO_FFMPEG_FORMAT
from .types import CsoSource


def policy_content_type(policy: dict[str, Any] | None) -> str:
    container = (policy or {}).get("container", "mpegts")
    return content_type_for_media_path(str(container or ""))


def policy_ffmpeg_format(policy: dict[str, Any] | None) -> str:
    container = (policy or {}).get("container", "mpegts")
    return CONTAINER_TO_FFMPEG_FORMAT.get(clean_key(container), "mpegts")


def resolve_cso_output_policy(policy: dict[str, Any] | None, use_slate_as_input: bool = False) -> dict[str, Any]:
    resolved = dict(policy or {})
    if use_slate_as_input:
        resolved["output_mode"] = "force_remux"
        resolved["container"] = "mpegts"
        resolved["video_codec"] = "copy"
        resolved["audio_codec"] = "copy"
        resolved["subtitle_mode"] = "copy"
    return resolved


def generate_vod_channel_ingest_policy(config: Any, output_policy: dict[str, Any] | None = None) -> dict[str, Any]:
    resolved = dict(output_policy or {})
    if not resolved:
        resolved = dict(generate_cso_policy_from_profile(config, "h264-aac-mpegts") or {})
    resolved["output_mode"] = "force_transcode"
    resolved["container"] = "mpegts"
    video_codec = clean_key(resolved.get("video_codec")) or "h264"
    audio_codec = clean_key(resolved.get("audio_codec")) or "aac"
    resolved["video_codec"] = "h264" if video_codec == "copy" else video_codec
    resolved["audio_codec"] = "aac" if audio_codec == "copy" else audio_codec
    resolved["subtitle_mode"] = "drop"
    resolved["transcode"] = True
    return resolved


def resolve_vod_channel_output_policy(policy: dict[str, Any] | None, ingest_policy: dict[str, Any]) -> dict[str, Any]:
    resolved = dict(policy or {})
    resolved["subtitle_mode"] = "drop"
    container_key = clean_key(resolved.get("container")) or "mpegts"
    if container_key not in {"mpegts", "matroska", "mp4", "hls"}:
        return resolved
    ingest_video_codec = clean_key(ingest_policy.get("video_codec")) or "h264"
    ingest_audio_codec = clean_key(ingest_policy.get("audio_codec")) or "aac"
    resolved_video_codec = clean_key(resolved.get("video_codec"))
    if resolved_video_codec not in {"", "copy", ingest_video_codec}:
        return resolved
    resolved_audio_codec = clean_key(resolved.get("audio_codec"))
    if resolved_audio_codec not in {"", "copy", ingest_audio_codec}:
        return resolved
    resolved["output_mode"] = "force_remux"
    resolved["video_codec"] = "copy"
    resolved["audio_codec"] = "copy"
    return resolved


def resolve_vod_pipe_container(source: CsoSource | None, source_probe: dict[str, Any] | None = None) -> str:
    if source is None or source.source_type not in {"vod_movie", "vod_episode"}:
        return "mpegts"

    container_key = clean_key((source_probe or {}).get("container")) or clean_key(
        getattr(source, "container_extension", "")
    )
    if container_key in {"matroska", "mkv"}:
        return "matroska"
    if container_key in {"mp4"}:
        return "mp4"
    if container_key in {"webm"}:
        return "webm"
    if container_key in {"mpegts", "ts"}:
        return "mpegts"
    # Any unspecified and unfriendly source containers should be remuxed to Matroska for the CSO pipe.
    return "matroska"


def pipe_container_from_content_type(content_type: str) -> str:
    lowered = clean_key(content_type)
    if lowered == "video/mp4":
        return "mp4"
    if lowered in {"video/webm", "audio/webm"}:
        return "webm"
    if lowered in {"video/mp2t", "video/ts"}:
        return "mpegts"
    if lowered in {"video/x-matroska", "audio/x-matroska"}:
        return "matroska"
    return ""

def effective_vod_hls_runtime_policy(policy, source: CsoSource | None):
    resolved = dict(policy or {})
    if source is None or source.source_type not in {"vod_movie", "vod_episode"}:
        return resolved
    resolved.setdefault("hls_playlist_mode", "event")
    resolved.setdefault("hls_list_size", 0)
    return resolved
