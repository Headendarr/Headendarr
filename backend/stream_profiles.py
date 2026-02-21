#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import logging
import os
import json

logger = logging.getLogger("stream_profiles")

# Profiles that TVHeadend can consume directly via ?profile=<id>
TVH_COMPATIBLE_PROFILE_IDS_ORDER = [
    "pass",
    "matroska",
    "webtv-h264-aac-mpegts",
    "webtv-h264-aac-matroska",
    "webtv-h264-aac-mp4",
    "webtv-vp8-vorbis-webm",
]
TVH_COMPATIBLE_PROFILE_IDS = set(TVH_COMPATIBLE_PROFILE_IDS_ORDER)
TVH_PROFILE_TO_STREAM_PROFILE = {
    "matroska": "matroska",
    "webtv-h264-aac-mpegts": "h264-aac-mpegts",
    "webtv-h264-aac-matroska": "h264-aac-matroska",
    "webtv-h264-aac-mp4": "h264-aac-mp4",
    "webtv-vp8-vorbis-webm": "vp8-vorbis-webm",
}

# CSO profiles that users can request via ?profile=<id>
SUPPORTED_STREAM_PROFILES = {
    "mpegts": {
        "label": "mpegts",
        "description": "Force remux to MPEG-TS.",
        "output_mode": "force_remux",
        "container": "mpegts",
        "video_codec": "",
        "audio_codec": "",
        "subtitle_mode": "copy",
        "transcode": False,
        "tvh_profile_name": "pass",
    },
    "matroska": {
        "label": "matroska",
        "description": "Force remux to Matroska.",
        "output_mode": "force_remux",
        "container": "matroska",
        "video_codec": "",
        "audio_codec": "",
        "subtitle_mode": "copy",
        "transcode": False,
        "tvh_profile_name": "matroska",
    },
    "aac-mpegts": {
        "label": "aac-mpegts",
        "description": "Transcode audio to AAC in MPEG-TS (copy video).",
        "output_mode": "force_transcode",
        "container": "mpegts",
        "video_codec": "",
        "audio_codec": "aac",
        "subtitle_mode": "copy",
        "transcode": True,
        "tvh_profile_name": "pass",
    },
    "aac-matroska": {
        "label": "aac-matroska",
        "description": "Transcode audio to AAC in Matroska (copy video).",
        "output_mode": "force_transcode",
        "container": "matroska",
        "video_codec": "",
        "audio_codec": "aac",
        "subtitle_mode": "copy",
        "transcode": True,
        "tvh_profile_name": "pass",
    },
    "aac-mp4": {
        "label": "aac-mp4",
        "description": "Transcode audio to AAC in MP4 (copy video).",
        "output_mode": "force_transcode",
        "container": "mp4",
        "video_codec": "",
        "audio_codec": "aac",
        "subtitle_mode": "drop",
        "transcode": True,
        "tvh_profile_name": "pass",
    },
    "h264-aac-mpegts": {
        "label": "h264-aac-mpegts",
        "description": "Transcode to H.264/AAC in MPEG-TS.",
        "output_mode": "force_transcode",
        "container": "mpegts",
        "video_codec": "h264",
        "audio_codec": "aac",
        "subtitle_mode": "copy",
        "transcode": True,
        "tvh_profile_name": "webtv-h264-aac-mpegts",
    },
    "h264-aac-matroska": {
        "label": "h264-aac-matroska",
        "description": "Transcode to H.264/AAC in Matroska.",
        "output_mode": "force_transcode",
        "container": "matroska",
        "video_codec": "h264",
        "audio_codec": "aac",
        "subtitle_mode": "copy",
        "transcode": True,
        "tvh_profile_name": "webtv-h264-aac-matroska",
    },
    "h264-aac-mp4": {
        "label": "h264-aac-mp4",
        "description": "Transcode to H.264/AAC in MP4.",
        "output_mode": "force_transcode",
        "container": "mp4",
        "video_codec": "h264",
        "audio_codec": "aac",
        "subtitle_mode": "drop",
        "transcode": True,
        "tvh_profile_name": "webtv-h264-aac-mp4",
    },
    "vp8-vorbis-webm": {
        "label": "vp8-vorbis-webm",
        "description": "Transcode to VP8/Vorbis in WebM.",
        "output_mode": "force_transcode",
        "container": "webm",
        "video_codec": "vp8",
        "audio_codec": "vorbis",
        "subtitle_mode": "drop",
        "transcode": True,
        "tvh_profile_name": "webtv-vp8-vorbis-webm",
    },
    "h265-aac-mp4": {
        "label": "h265-aac-mp4",
        "description": "Transcode to H.265/AAC in MP4.",
        "output_mode": "force_transcode",
        "container": "mp4",
        "video_codec": "h265",
        "audio_codec": "aac",
        "subtitle_mode": "drop",
        "transcode": True,
        "tvh_profile_name": "pass",
    },
    "h265-aac-matroska": {
        "label": "h265-aac-matroska",
        "description": "Transcode to H.265/AAC in Matroska.",
        "output_mode": "force_transcode",
        "container": "matroska",
        "video_codec": "h265",
        "audio_codec": "aac",
        "subtitle_mode": "copy",
        "transcode": True,
        "tvh_profile_name": "pass",
    },
    "h265-ac3-mp4": {
        "label": "h265-ac3-mp4",
        "description": "Transcode to H.265/AC3 in MP4.",
        "output_mode": "force_transcode",
        "container": "mp4",
        "video_codec": "h265",
        "audio_codec": "ac3",
        "subtitle_mode": "drop",
        "transcode": True,
        "tvh_profile_name": "pass",
    },
    "h265-ac3-matroska": {
        "label": "h265-ac3-matroska",
        "description": "Transcode to H.265/AC3 in Matroska.",
        "output_mode": "force_transcode",
        "container": "matroska",
        "video_codec": "h265",
        "audio_codec": "ac3",
        "subtitle_mode": "copy",
        "transcode": True,
        "tvh_profile_name": "pass",
    },
}


DEFAULT_PROFILE = "default"
TVH_PROFILE = "tvh"
DEFAULT_PROFILE_TEMPLATE = {
    "label": "",
    "description": "",
    "output_mode": "force_remux",
    "container": "mpegts",
    "video_codec": "",
    "audio_codec": "",
    "subtitle_mode": "copy",
    "transcode": False,
    "tvh_profile_name": "pass",
}


def _normalized_profile_definition(profile_data):
    normalized = dict(DEFAULT_PROFILE_TEMPLATE)
    if isinstance(profile_data, dict):
        normalized.update(profile_data)
    return normalized


SUPPORTED_STREAM_PROFILES = {
    profile_name: _normalized_profile_definition(profile_data)
    for profile_name, profile_data in SUPPORTED_STREAM_PROFILES.items()
}


def _parse_env_supported_profiles():
    raw = os.environ.get("SUPPORTED_STREAM_PROFILES") or os.environ.get("SUPPORTED_TVH_STREAM_PROFILES") or ""
    raw = str(raw or "").strip()
    if not raw:
        return set(SUPPORTED_STREAM_PROFILES.keys())
    values = {item.strip().lower() for item in raw.split(",") if item and item.strip()}
    if not values:
        return set(SUPPORTED_STREAM_PROFILES.keys())
    return values


def _profile_settings_map(settings: dict):
    raw = (settings or {}).get("settings", {}).get("stream_profiles") or {}
    if not isinstance(raw, dict):
        return {}
    normalized = {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            continue
        normalized[str(key).strip().lower()] = {
            "enabled": bool(value.get("enabled", True)),
            "hwaccel": bool(value.get("hwaccel", False)),
            "deinterlace": bool(value.get("deinterlace", False)),
        }
    return normalized


def _channel_profile_override(channel):
    if channel is None:
        return ""
    if isinstance(channel, dict):
        return profile_from_cso_policy(channel.get("cso_policy"))
    return profile_from_cso_policy(getattr(channel, "cso_policy", None))


def _is_supported_profile_enabled(settings, requested):
    if requested not in SUPPORTED_STREAM_PROFILES:
        return False
    env_allowed = _parse_env_supported_profiles()
    if requested not in env_allowed:
        logger.warning("Stream profile %s disabled by SUPPORTED_STREAM_PROFILES; falling back to default.", requested)
        return False
    profile_settings = _profile_settings_map(settings)
    if not profile_settings.get(requested, {"enabled": True}).get("enabled", True):
        logger.warning("Stream profile %s disabled in app settings; falling back to default.", requested)
        return False
    return True


def profile_from_cso_policy(value):
    policy = value
    if isinstance(policy, str):
        text = policy.strip()
        if not text:
            return DEFAULT_PROFILE
        try:
            policy = json.loads(text)
        except Exception:
            return DEFAULT_PROFILE
    if not isinstance(policy, dict):
        return DEFAULT_PROFILE
    profile = str(policy.get("profile") or "").strip().lower()
    return profile or DEFAULT_PROFILE


def resolve_cso_profile_name(config, requested_profile=None, channel=None):
    """Resolve a CSO stream profile from request input and optional channel config.

    Resolution order:
    1. `requested_profile` when provided and enabled.
    2. Channel CSO profile when configured and enabled.
    3. Fallback to `default`.
    """
    settings = config.read_settings()
    requested = str(requested_profile or "").strip().lower()

    if requested in {DEFAULT_PROFILE, TVH_PROFILE}:
        return requested
    if requested and _is_supported_profile_enabled(settings, requested):
        return requested
    if requested and requested not in SUPPORTED_STREAM_PROFILES:
        logger.warning("Unsupported stream profile %s; falling back to channel/default.", requested)

    channel_profile = str(_channel_profile_override(channel) or "").strip().lower()
    if channel_profile in {DEFAULT_PROFILE, TVH_PROFILE}:
        return channel_profile
    if channel_profile and _is_supported_profile_enabled(settings, channel_profile):
        return channel_profile
    if channel_profile and channel_profile not in SUPPORTED_STREAM_PROFILES:
        logger.warning("Unsupported channel CSO profile %s; falling back to default.", channel_profile)
    return DEFAULT_PROFILE


def resolve_tvh_profile_name(config, cso_profile=None):
    """Resolve a TVH profile ID from a resolved CSO profile.

    This function does not perform request/channel fallback logic. It only maps
    a provided CSO profile to a TVH-compatible profile ID.
    """
    settings = config.read_settings()
    profile = str(cso_profile or "").strip().lower()

    if not profile or profile in {DEFAULT_PROFILE, TVH_PROFILE}:
        return TVH_COMPATIBLE_PROFILE_IDS_ORDER[0]

    if profile in TVH_COMPATIBLE_PROFILE_IDS:
        if profile == "pass":
            return profile
        mapped_profile = TVH_PROFILE_TO_STREAM_PROFILE.get(profile)
        if mapped_profile and _is_supported_profile_enabled(settings, mapped_profile):
            return profile
        logger.warning(
            "Requested TVH profile %s is disabled via mapped stream profile %s; falling back to %s.",
            profile,
            mapped_profile or "<unknown>",
            TVH_COMPATIBLE_PROFILE_IDS_ORDER[0],
        )
        return TVH_COMPATIBLE_PROFILE_IDS_ORDER[0]

    if profile not in SUPPORTED_STREAM_PROFILES:
        logger.warning(
            "Unsupported CSO profile %s for TVH mapping; falling back to %s.",
            profile,
            TVH_COMPATIBLE_PROFILE_IDS_ORDER[0],
        )
        return TVH_COMPATIBLE_PROFILE_IDS_ORDER[0]

    if not _is_supported_profile_enabled(settings, profile):
        return TVH_COMPATIBLE_PROFILE_IDS_ORDER[0]

    mapped = (
        str(
            (_normalized_profile_definition(SUPPORTED_STREAM_PROFILES.get(profile) or {}).get("tvh_profile_name") or "")
        )
        .strip()
        .lower()
    )
    if mapped in TVH_COMPATIBLE_PROFILE_IDS:
        return mapped

    logger.error(
        "Invalid TVH profile mapping for stream profile %s (mapped=%s). Falling back to %s.",
        profile,
        mapped or "<empty>",
        TVH_COMPATIBLE_PROFILE_IDS_ORDER[0],
    )
    return TVH_COMPATIBLE_PROFILE_IDS_ORDER[0]


def generate_cso_policy_from_profile(config, profile):
    """Generate runtime CSO policy for an already-resolved and enabled profile.

    The provided `profile` must already be validated as enabled by upstream
    resolution logic. This function only expands profile defaults and app-level
    per-profile runtime toggles (hardware acceleration and deinterlace).
    """
    settings = config.read_settings()
    profile_settings = _profile_settings_map(settings)
    profile_data = _normalized_profile_definition(SUPPORTED_STREAM_PROFILES.get(profile) or {})
    current_profile_settings = profile_settings.get(profile) or {}
    return {
        "output_mode": profile_data.get("output_mode", "force_remux"),
        "container": profile_data.get("container", "mpegts"),
        "video_codec": profile_data.get("video_codec", ""),
        "audio_codec": profile_data.get("audio_codec", ""),
        "subtitle_mode": profile_data.get("subtitle_mode", "copy"),
        "transcode": bool(profile_data.get("transcode", False)),
        "deinterlace": bool(current_profile_settings.get("deinterlace", False)),
        "hwaccel": bool(current_profile_settings.get("hwaccel", False)),
    }


def get_profile_options_payload(config):
    settings = config.read_settings()
    profile_settings = _profile_settings_map(settings)

    entries = []
    for profile_key, profile in SUPPORTED_STREAM_PROFILES.items():
        configured = profile_settings.get(profile_key, {})
        profile = _normalized_profile_definition(profile)
        entries.append(
            {
                "profile": profile_key,
                "enabled": bool(configured.get("enabled", True)),
                "hwaccel": bool(configured.get("hwaccel", False)),
                "deinterlace": bool(configured.get("deinterlace", False)),
                "transcode": bool(profile.get("transcode", False)),
            }
        )
    return {
        "profiles": entries,
    }


def get_stream_profile_definitions():
    definitions = []
    for profile_key, profile in SUPPORTED_STREAM_PROFILES.items():
        profile = _normalized_profile_definition(profile)
        video_codec = str(profile.get("video_codec") or "").strip().lower()
        transcode = bool(profile.get("transcode", False))
        supports_video_filters = bool(transcode and video_codec)
        definitions.append(
            {
                "key": profile_key,
                "label": str(profile.get("label") or profile_key),
                "description": str(profile.get("description") or ""),
                "transcode": transcode,
                "supports_hwaccel": supports_video_filters,
                "supports_deinterlace": supports_video_filters,
            }
        )
    return definitions
