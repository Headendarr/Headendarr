#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import json
from datetime import datetime
from fractions import Fraction

from sqlalchemy import select

from backend.http_headers import sanitise_headers
from backend.models import ChannelSource, VodCategoryEpisode, XcVodItem, Session


def _clean_text(value):
    return str(value or "").strip()


def _clean_key(value):
    return _clean_text(value).lower()


def _header_value(headers, name):
    target = _clean_key(name)
    if not target:
        return ""
    for key, value in (headers or {}).items():
        if _clean_key(key) == target:
            return _clean_text(value)
    return ""


def _format_ffmpeg_headers_arg(headers):
    lines = []
    for key, value in (headers or {}).items():
        key_name = _clean_key(key)
        if key_name in {"user-agent", "referer"}:
            continue
        text = _clean_text(value)
        if not text:
            continue
        lines.append(f"{key}: {text}")
    if not lines:
        return None
    return "\r\n".join(lines) + "\r\n"


def _parse_fractional_rate(value):
    text = _clean_text(value)
    if not text or text in {"0/0", "0", "n/a"}:
        return 0.0
    try:
        if "/" in text:
            return float(Fraction(text))
        return float(text)
    except Exception:
        return 0.0


def extract_media_shape_from_ffprobe_payload(payload):
    data = payload or {}
    streams = data.get("streams") or []
    format_info = data.get("format") or {}

    video_stream = None
    audio_stream = None
    for stream in streams:
        codec_type = _clean_key(stream.get("codec_type"))
        if codec_type == "video" and video_stream is None:
            video_stream = stream
        elif codec_type == "audio" and audio_stream is None:
            audio_stream = stream

    avg_frame_rate = ""
    fps = 0.0
    if video_stream is not None:
        avg_frame_rate = _clean_text(video_stream.get("avg_frame_rate") or video_stream.get("r_frame_rate"))
        fps = _parse_fractional_rate(avg_frame_rate)

    return {
        "container": _clean_key((format_info.get("format_name") or "").split(",", 1)[0]),
        "video_codec": _clean_key(video_stream.get("codec_name")) if video_stream else "",
        "video_profile": _clean_text(video_stream.get("profile")) if video_stream else "",
        "audio_codec": _clean_key(audio_stream.get("codec_name")) if audio_stream else "",
        "width": int(video_stream.get("width") or 0) if video_stream else 0,
        "height": int(video_stream.get("height") or 0) if video_stream else 0,
        "pixel_format": _clean_key(video_stream.get("pix_fmt")) if video_stream else "",
        "field_order": _clean_key(video_stream.get("field_order")) if video_stream else "",
        "avg_frame_rate": avg_frame_rate,
        "fps": fps,
        "audio_sample_rate": int(audio_stream.get("sample_rate") or 0) if audio_stream else 0,
        "audio_channels": int(audio_stream.get("channels") or 0) if audio_stream else 0,
        "audio_channel_layout": _clean_key(audio_stream.get("channel_layout")) if audio_stream else "",
        "has_audio": bool(audio_stream is not None),
    }


async def probe_stream_media_shape(source_url, user_agent=None, request_headers=None, timeout_seconds=8.0):
    header_values = sanitise_headers(request_headers)
    command = [
        "ffprobe",
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        "-show_error",
    ]
    user_agent_value = _clean_text(user_agent) or _header_value(header_values, "User-Agent")
    if user_agent_value:
        command += ["-user_agent", user_agent_value]
    header_arg = _format_ffmpeg_headers_arg(header_values)
    if header_arg:
        command += ["-headers", header_arg]
    command.append(source_url)
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_data, _stderr_data = await asyncio.wait_for(process.communicate(), timeout=float(timeout_seconds or 8.0))
        if process.returncode not in (0, None):
            return {}
        payload = json.loads((stdout_data or b"{}").decode("utf-8", errors="replace") or "{}")
    except Exception:
        return {}
    return extract_media_shape_from_ffprobe_payload(payload)


def serialise_media_shape(media_shape):
    cleaned = {}
    for key, value in (media_shape or {}).items():
        if isinstance(value, float):
            cleaned[key] = round(value, 3)
        else:
            cleaned[key] = value
    return json.dumps(cleaned, sort_keys=True)


def load_source_media_shape(source):
    raw_value = getattr(source, "stream_probe_details", None)
    text = _clean_text(raw_value)
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def choose_channel_media_shape(sources):
    for source in sources or []:
        media_shape = load_source_media_shape(source)
        if not media_shape:
            continue
        if int(media_shape.get("width") or 0) > 0 and int(media_shape.get("height") or 0) > 0:
            return media_shape
    return {}


async def persist_source_media_shape(source_id, media_shape, observed_at=None, source_type="channel"):
    try:
        parsed_source_id = int(source_id)
    except Exception:
        return False
    if parsed_source_id <= 0 or not isinstance(media_shape, dict) or not media_shape:
        return False

    observed_value = observed_at if isinstance(observed_at, datetime) else datetime.utcnow()
    payload = serialise_media_shape(media_shape)

    async with Session() as session:
        async with session.begin():
            if source_type == "vod_movie":
                source = await session.get(XcVodItem, parsed_source_id)
            elif source_type == "vod_episode":
                source = await session.get(VodCategoryEpisode, parsed_source_id)
            else:
                source = await session.get(ChannelSource, parsed_source_id)

            if source is None:
                return False
            source.stream_probe_at = observed_value
            source.stream_probe_details = payload
    return True


async def get_source_media_shape(source_id, source_type="channel"):
    try:
        parsed_source_id = int(source_id)
    except Exception:
        return {}
    if parsed_source_id <= 0:
        return {}
    async with Session() as session:
        if source_type == "vod_movie":
            result = await session.execute(select(XcVodItem).where(XcVodItem.id == parsed_source_id))
        elif source_type == "vod_episode":
            result = await session.execute(select(VodCategoryEpisode).where(VodCategoryEpisode.id == parsed_source_id))
        else:
            result = await session.execute(select(ChannelSource).where(ChannelSource.id == parsed_source_id))

        source = result.scalars().first()
    if source is None:
        return {}
    return load_source_media_shape(source)
