import logging
from typing import Any
from urllib.parse import urlparse

from backend.stream_profiles import content_type_for_media_path, generate_cso_policy_from_profile
from backend.utils import clean_key, clean_text

from .constants import CONTAINER_TO_FFMPEG_FORMAT
from .types import CsoSource


logger = logging.getLogger("cso")


VOD_CHANNEL_TS_SAFE_VIDEO_CODECS = {"h264", "h265", "hevc", "mpeg2video"}
LIVE_PIPE_TS_SAFE_AUDIO_CODECS = {"", "aac", "ac3", "eac3", "mp2", "mp3"}
VOD_CHANNEL_FMP4_COPY_VIDEO_CODECS = {"h264", "h265", "hevc"}
VOD_CHANNEL_FMP4_COPY_AUDIO_CODECS = {"aac", "ac3"}


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


def output_profile_requires_audio(policy: dict[str, Any] | None) -> bool:
    return clean_key((policy or {}).get("audio_codec")) not in {"none", "drop", "disabled"}


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
    resolved["container"] = "hls"
    resolved["hls_segment_type"] = "fmp4"
    resolved["hls_playlist_mode"] = "event"
    # 24/7 VOD channel ingest is a shared intermediate feed. Keep source-cleanup behaviour such
    # as deinterlacing when requested by the first viewer that establishes the ingest, but do not
    # bake request-specific sizing and bitrate-shaping into the shared ingest.
    resolved["target_width"] = 0
    resolved["target_height"] = 0
    resolved["output_fps"] = 0.0
    resolved["output_pixel_format"] = ""
    resolved["target_video_bitrate"] = ""
    resolved["target_video_maxrate"] = ""
    resolved["target_video_bufsize"] = ""
    resolved["audio_sample_rate"] = 48000
    resolved["audio_channels"] = 2
    resolved["subtitle_mode"] = "drop"
    resolved["transcode"] = True
    return resolved


def _normalise_vod_channel_video_codec(codec: object) -> str:
    codec_key = clean_key(codec)
    return "h265" if codec_key == "hevc" else codec_key


def generate_vod_channel_segment_cache_policy(
    config: Any,
    source_probe: dict[str, Any] | None = None,
    canonical_probe: dict[str, Any] | None = None,
    optimistic_unknown: bool = False,
) -> dict[str, Any]:
    """Build the shared 24/7-channel intermediate policy.

    The timeshift cache is profile-independent. Preserve compatible H.264/H.265
    video and AAC/AC-3 audio, and transcode only streams that cannot be safely
    joined to the canonical channel handoff established by the first airing.
    """

    source = dict(source_probe or {})
    canonical = dict(canonical_probe or {})
    resolved = dict(generate_cso_policy_from_profile(config, "h264-aac-mpegts") or {})

    source_video_codec = clean_key(source.get("video_codec"))
    source_video_policy_codec = _normalise_vod_channel_video_codec(source_video_codec)
    canonical_video_codec = clean_key(canonical.get("video_codec"))
    canonical_video_policy_codec = _normalise_vod_channel_video_codec(canonical_video_codec)

    copy_video = source_video_codec in VOD_CHANNEL_FMP4_COPY_VIDEO_CODECS or (
        optimistic_unknown and not source_video_codec
    )
    if canonical_video_policy_codec and source_video_codec:
        copy_video = copy_video and source_video_policy_codec == canonical_video_policy_codec
        for field_name in ("width", "height", "pixel_format"):
            expected = canonical.get(field_name)
            observed = source.get(field_name)
            if expected not in {None, "", 0} and observed not in {None, "", 0} and observed != expected:
                copy_video = False
        expected_fps = float(canonical.get("fps") or 0.0)
        observed_fps = float(source.get("fps") or 0.0)
        if expected_fps > 0.0 and observed_fps > 0.0 and abs(expected_fps - observed_fps) > 0.05:
            copy_video = False

    source_audio_codec = clean_key(source.get("audio_codec"))
    canonical_audio_codec = clean_key(canonical.get("audio_codec"))
    copy_audio = source_audio_codec in VOD_CHANNEL_FMP4_COPY_AUDIO_CODECS or (
        optimistic_unknown and not source_audio_codec
    )
    if canonical_audio_codec and source_audio_codec:
        copy_audio = copy_audio and source_audio_codec == canonical_audio_codec
        for field_name in ("audio_sample_rate", "audio_channels"):
            expected = canonical.get(field_name)
            observed = source.get(field_name)
            if expected not in {None, "", 0} and observed not in {None, "", 0} and observed != expected:
                copy_audio = False

    resolved["output_mode"] = "force_remux" if copy_video and copy_audio else "force_transcode"
    resolved["video_codec"] = "" if copy_video else (canonical_video_policy_codec or "h264")
    resolved["audio_codec"] = "" if copy_audio else (canonical_audio_codec or "aac")
    resolved["container"] = "hls"
    resolved["hls_segment_type"] = "fmp4"
    resolved["hls_playlist_mode"] = "event"
    resolved["hls_list_size"] = 0
    resolved["target_width"] = int(canonical.get("width") or 0)
    resolved["target_height"] = int(canonical.get("height") or 0)
    resolved["output_fps"] = float(canonical.get("fps") or 0.0)
    resolved["output_pixel_format"] = clean_key(canonical.get("pixel_format")) or ""
    resolved["target_video_bitrate"] = ""
    resolved["target_video_maxrate"] = ""
    resolved["target_video_bufsize"] = ""
    resolved["audio_sample_rate"] = int(canonical.get("audio_sample_rate") or source.get("audio_sample_rate") or 48000)
    resolved["audio_channels"] = int(canonical.get("audio_channels") or source.get("audio_channels") or 2)
    resolved["subtitle_mode"] = "drop"
    resolved["deinterlace"] = False
    resolved["hardware_decode"] = not copy_video
    resolved["transcode"] = resolved["output_mode"] == "force_transcode"
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
        return "mpegts"
    if source is None:
        return "mpegts"
    if source.source_type not in {"vod_movie", "vod_episode"}:
        return "mpegts"
    return "mpegts"


def resolve_live_pipe_container(source_probe: dict[str, Any] | None = None) -> str:
    probe = dict(source_probe or {})
    video_codec = clean_key(probe.get("video_codec"))
    if video_codec and video_codec not in VOD_CHANNEL_TS_SAFE_VIDEO_CODECS:
        return "mpegts"
    audio_codec = clean_key(probe.get("audio_codec"))
    if audio_codec and audio_codec not in LIVE_PIPE_TS_SAFE_AUDIO_CODECS:
        return "mpegts"
    return "mpegts"


def source_uses_segmented_handoff(source: CsoSource | None, source_probe: dict[str, Any] | None = None) -> bool:
    try:
        from quart import current_app

        if current_app:
            app_config = current_app.config.get("APP_CONFIG")
            if app_config and getattr(app_config, "settings", None):
                settings = app_config.settings.get("settings") or {}
                if settings.get("cso_force_segmented_handoff", True):
                    return True
    except Exception:
        pass

    probe = dict(source_probe or {})
    if source is not None:
        source_path = (urlparse(clean_text(source.url)).path or "").lower()
        source_type = clean_key(source.source_type)
        if source_path.endswith((".m3u8", ".m3u")) or source_type in {"vod_movie", "vod_episode"}:
            return True
    container = clean_key(probe.get("container")) or clean_key(getattr(source, "container_extension", ""))
    if container and container not in {"mpegts", "ts"}:
        return True
    video_codec = clean_key(probe.get("video_codec"))
    if video_codec and video_codec not in VOD_CHANNEL_TS_SAFE_VIDEO_CODECS:
        return True
    audio_codec = clean_key(probe.get("audio_codec"))
    if audio_codec and audio_codec not in LIVE_PIPE_TS_SAFE_AUDIO_CODECS:
        return True
    return False


def segmented_hls_segment_type(source: CsoSource | None, source_probe: dict[str, Any] | None = None) -> str:
    probe = dict(source_probe or {})
    container = clean_key(probe.get("container")) or clean_key(getattr(source, "container_extension", ""))
    if container in {"mpegts", "ts"}:
        return "mpegts"
    return "fmp4"


def segmented_handoff_subtitle_policy(segment_type: str) -> tuple[str, str]:
    """Return the explicit subtitle policy for the single-playlist HLS handoff.

    The current handoff writes one media playlist and one segment family. HLS
    WebVTT and other alternate subtitles require their own rendition playlist,
    while MP4 subtitle streams are not valid in MPEG-TS segments. Preserve
    subtitle discovery in the selected input master, but deliberately drop
    subtitles at this intermediate until the handoff can own separate subtitle
    playlists.
    """
    resolved_segment_type = clean_key(segment_type) or "mpegts"
    return "drop", f"single_playlist_hls_{resolved_segment_type}_cannot_preserve_alternate_subtitles"


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
