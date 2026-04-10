import logging
from typing import Any

from backend.stream_profiles import content_type_for_media_path, generate_cso_policy_from_profile
from backend.utils import clean_key, clean_text

from .constants import CONTAINER_TO_FFMPEG_FORMAT
from .types import CsoSource


logger = logging.getLogger("cso")


VOD_CHANNEL_TS_SAFE_VIDEO_CODECS = {"h264", "h265", "hevc", "mpeg2video"}
LIVE_PIPE_TS_SAFE_AUDIO_CODECS = {"", "aac", "ac3", "eac3", "mp2", "mp3"}


def policy_content_type(policy: dict[str, Any] | None) -> str:
    container = (policy or {}).get("container", "mpegts")
    return content_type_for_media_path(str(container or ""))


def policy_ffmpeg_format(policy: dict[str, Any] | None) -> str:
    container = (policy or {}).get("container", "mpegts")
    return CONTAINER_TO_FFMPEG_FORMAT.get(clean_key(container), "mpegts")


def policy_log_label(policy: dict[str, Any] | None) -> str:
    if not policy:
        return "none"
    data = policy or {}
    parts = [
        f"output_mode={data.get('output_mode', 'force_remux')}",
        f"container={data.get('container', 'mpegts')}",
        f"video_codec={data.get('video_codec', '') or 'copy'}",
        f"audio_codec={data.get('audio_codec', '') or 'copy'}",
        f"subtitle_mode={data.get('subtitle_mode', 'copy')}",
        f"hwaccel={bool(data.get('hwaccel', False))}",
        f"hardware_decode={bool(data.get('hardware_decode', True))}",
        f"deinterlace={bool(data.get('deinterlace', False))}",
    ]
    return ", ".join(parts)


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
    video_codec = clean_key(resolved.get("video_codec")) or "h264"
    audio_codec = clean_key(resolved.get("audio_codec")) or "aac"
    resolved_video_codec = "h264" if video_codec == "copy" else video_codec
    if resolved_video_codec not in VOD_CHANNEL_TS_SAFE_VIDEO_CODECS:
        resolved_video_codec = "h264"
    resolved["video_codec"] = resolved_video_codec
    resolved["audio_codec"] = "aac" if audio_codec == "copy" else audio_codec
    resolved["container"] = "mpegts"
    # 24/7 VOD channel ingest is a shared intermediate feed. Keep source-cleanup behaviour such
    # as deinterlacing when requested by the first viewer that establishes the ingest, but do not
    # bake request-specific sizing and bitrate-shaping into the shared ingest.
    resolved["target_width"] = 0
    resolved["target_video_bitrate"] = ""
    resolved["target_video_maxrate"] = ""
    resolved["target_video_bufsize"] = ""
    resolved["subtitle_mode"] = "drop"
    resolved["transcode"] = True
    return resolved


def resolve_vod_channel_output_policy(policy: dict[str, Any] | None, ingest_policy: dict[str, Any]) -> dict[str, Any]:
    resolved = dict(policy or {})
    resolved["subtitle_mode"] = "drop"
    container_key = clean_key(resolved.get("container")) or "mpegts"
    if container_key not in {"mpegts", "matroska", "mp4", "hls"}:
        return resolved
    ingest_container = clean_key(ingest_policy.get("container")) or "mpegts"
    ingest_video_codec = clean_key(ingest_policy.get("video_codec")) or "h264"
    ingest_audio_codec = clean_key(ingest_policy.get("audio_codec")) or "aac"
    if bool(ingest_policy.get("deinterlace")):
        resolved["deinterlace"] = False
    target_width = max(0, int(resolved.get("target_width") or 0))
    target_video_bitrate = str(resolved.get("target_video_bitrate") or "").strip()
    target_video_maxrate = str(resolved.get("target_video_maxrate") or "").strip()
    target_video_bufsize = str(resolved.get("target_video_bufsize") or "").strip()
    audio_bitrate = str(resolved.get("audio_bitrate") or "").strip()
    if target_width > 0 or target_video_bitrate or target_video_maxrate or target_video_bufsize or audio_bitrate:
        return resolved
    if ingest_container == "mpegts" and container_key == "matroska" and ingest_audio_codec == "aac":
        resolved["output_mode"] = "force_transcode"
        resolved["video_codec"] = ""
        resolved["audio_codec"] = "aac"
        return resolved
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
    video_codec = clean_key((source_probe or {}).get("video_codec"))
    if video_codec and video_codec in VOD_CHANNEL_TS_SAFE_VIDEO_CODECS:
        return "mpegts"
    if video_codec:
        return "nut"
    if source is None:
        return "mpegts"
    if source.source_type not in {"vod_movie", "vod_episode"}:
        return "mpegts"
    return "nut"


def resolve_live_pipe_container(source_probe: dict[str, Any] | None = None) -> str:
    probe = dict(source_probe or {})
    video_codec = clean_key(probe.get("video_codec"))
    if video_codec and video_codec in VOD_CHANNEL_TS_SAFE_VIDEO_CODECS:
        return "nut"
    audio_codec = clean_key(probe.get("audio_codec"))
    if audio_codec and audio_codec not in LIVE_PIPE_TS_SAFE_AUDIO_CODECS:
        return "nut"
    return "mpegts"


def should_prefer_direct_vod_url_input(
    source: CsoSource | None, start_seconds: int = 0, source_probe: dict[str, Any] | None = None
) -> bool:
    if source is None or not source.url:
        return False
    if int(start_seconds or 0) > 0:
        return True
    container_key = clean_key((source_probe or {}).get("container")) or clean_key(
        getattr(source, "container_extension", "")
    )
    return container_key not in {"mp4", "mkv", "matroska", "webm", "mpegts", "ts"}


def pipe_container_from_content_type(content_type: str) -> str:
    raw_content_type = clean_text(content_type)
    lowered = clean_key(raw_content_type)
    if lowered == "video/mp4":
        return "mp4"
    if lowered in {"video/x-msvideo", "video/avi", "video/msvideo"}:
        return "avi"
    if lowered in {"video/x-flv", "video/flv"}:
        return "flv"
    if lowered in {"video/webm", "audio/webm"}:
        return "webm"
    if lowered in {"video/mp2t", "video/ts"}:
        return "mpegts"
    if lowered in {"video/x-matroska", "audio/x-matroska"}:
        return "matroska"
    if raw_content_type:
        logger.warning(
            "Unable to map VOD proxy content type to pipe container; falling back to source container resolution content_type=%s",
            raw_content_type,
        )
    return ""


def effective_vod_hls_runtime_policy(policy, source: CsoSource | None):
    resolved = dict(policy or {})
    if source is None or source.source_type not in {"vod_movie", "vod_episode"}:
        return resolved
    resolved.setdefault("hls_playlist_mode", "event")
    resolved.setdefault("hls_list_size", 0)
    return resolved
