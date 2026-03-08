#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import base64
import json
import logging
import os
import re
import shutil
import time
from collections import deque
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import aiohttp
from sqlalchemy import delete
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from backend.models import (
    Session,
    Channel,
    ChannelSource,
    CsoEventLog,
)
from backend.streaming import (
    LOCAL_PROXY_HOST_PLACEHOLDER,
    append_stream_key,
    is_local_hls_proxy_url,
    normalize_local_proxy_url,
)
from backend.stream_profiles import generate_cso_policy_from_profile
from backend.users import get_user_by_stream_key
from backend.config import enable_cso_command_debug_logging
from backend.datetime_utils import utc_now_naive
from backend.http_headers import parse_headers_json, sanitise_headers
from backend.xc_hosts import parse_xc_hosts

logger = logging.getLogger("cso")
CSO_SOURCE_HOLD_DOWN_SECONDS = 20
CSO_INGEST_RECOVERY_RETRY_WINDOW_SECONDS = 12
CSO_INGEST_RECOVERY_RETRY_INTERVAL_SECONDS = 1
CSO_STALL_SECONDS_DEFAULT = 20
CSO_UNDERSPEED_RATIO_DEFAULT = 0.9
CSO_UNDERSPEED_WINDOW_SECONDS_DEFAULT = 12
CSO_STARTUP_GRACE_SECONDS_DEFAULT = 8
CSO_HTTP_ERROR_WINDOW_SECONDS_DEFAULT = 10
CSO_HTTP_ERROR_THRESHOLD_DEFAULT = 4
CSO_SPEED_STALE_SECONDS_DEFAULT = 6
CSO_INGEST_RECONNECT_DELAY_MAX_SECONDS = 2
CSO_INGEST_RW_TIMEOUT_US = 15_000_000
CSO_INGEST_TIMEOUT_US = 10_000_000
CSO_OUTPUT_CLIENT_STALE_SECONDS = 8.0
CSO_HLS_SEGMENT_SECONDS = 3
CSO_HLS_LIST_SIZE = 8
CSO_HLS_CLIENT_IDLE_SECONDS = min(10, max(1, int(CSO_HLS_SEGMENT_SECONDS) * 3))
CSO_OUTPUT_CLIENT_QUEUE_MAX_BYTES = 90_000_000
CSO_INGEST_SUBSCRIBER_QUEUE_MAX_BYTES = 90_000_000
CSO_CONSUMER_PROGRESS_LOG_INTERVAL_SECONDS = 10
_FFMPEG_SPEED_RE = re.compile(r"speed=\s*([0-9.]+)x")
_HTTP_STATUS_CODE_RE = re.compile(r"\b([45]\d{2})\b")
_HLS_BANDWIDTH_RE = re.compile(r"BANDWIDTH=(\d+)")
_HLS_RESOLUTION_RE = re.compile(r"RESOLUTION=(\d+)x(\d+)")
_HLS_STARTUP_RAMP_INTERVAL_SECONDS = 4
_SAFE_HLS_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_.-]+$")

CONTAINER_TO_FORMAT = {
    "mpegts": "mpegts",
    "ts": "mpegts",
    "matroska": "matroska",
    "mkv": "matroska",
    "mp4": "mp4",
    "webm": "webm",
    "hls": "hls",
}

CONTAINER_TO_CONTENT_TYPE = {
    "mpegts": "video/mp2t",
    "ts": "video/mp2t",
    "matroska": "video/x-matroska",
    "mkv": "video/x-matroska",
    "mp4": "video/mp4",
    "webm": "video/webm",
    "hls": "application/vnd.apple.mpegurl",
}


class CsoOutputReaderEnded(Exception):
    """Raised when CSO output ended unexpectedly while clients were still attached."""


async def _wait_process_exit_with_timeout(process, timeout_seconds=2.0):
    if not process:
        return None
    return await asyncio.wait_for(process.wait(), timeout=float(timeout_seconds))


class ByteBudgetQueue:
    """Leaky async queue bounded by payload bytes instead of item count."""

    def __init__(self, max_bytes):
        self.max_bytes = max(1, int(max_bytes or 1))
        self._items = deque()
        self._bytes = 0
        self._cond = asyncio.Condition()

    @staticmethod
    def _payload_size(payload):
        if payload is None:
            return 0
        try:
            return len(payload)
        except Exception:
            return 0

    async def put_drop_oldest(self, payload):
        now_value = time.time()
        size = self._payload_size(payload)
        dropped_items = 0
        dropped_bytes = 0
        payload_too_large = False
        async with self._cond:
            while payload is not None and self._items and (self._bytes + size) > self.max_bytes:
                old_payload, old_size, _ = self._items.popleft()
                if old_payload is not None:
                    self._bytes = max(0, self._bytes - old_size)
                    dropped_items += 1
                    dropped_bytes += int(old_size or 0)
            if payload is not None and size > self.max_bytes:
                while self._items:
                    old_payload, old_size, _ = self._items.popleft()
                    if old_payload is not None:
                        dropped_items += 1
                        dropped_bytes += int(old_size or 0)
                self._bytes = 0
                payload_too_large = True
            self._items.append((payload, size, now_value))
            if payload is not None:
                self._bytes += size
            self._cond.notify(1)
            queued_bytes = int(self._bytes)
            queued_items = len(self._items)
        return {
            "dropped_items": dropped_items,
            "dropped_bytes": dropped_bytes,
            "payload_too_large": payload_too_large,
            "queued_bytes": queued_bytes,
            "queued_items": queued_items,
            "max_bytes": int(self.max_bytes),
        }

    async def put_eof(self):
        await self.put_drop_oldest(None)

    async def get(self):
        async with self._cond:
            while not self._items:
                await self._cond.wait()
            payload, size, _ = self._items.popleft()
            if payload is not None:
                self._bytes = max(0, self._bytes - size)
            return payload

    async def stats(self):
        now_value = time.time()
        async with self._cond:
            oldest_age = 0.0
            if self._items:
                oldest_age = max(0.0, now_value - float(self._items[0][2] or now_value))
            return {
                "queued_items": len(self._items),
                "queued_bytes": int(self._bytes),
                "max_bytes": int(self.max_bytes),
                "oldest_age_seconds": oldest_age,
            }


def detect_vaapi_device_path() -> str | None:
    for candidate in ("/dev/dri/renderD128", "/dev/dri/renderD129"):
        if Path(candidate).exists():
            return candidate
    for candidate in sorted(Path("/dev/dri").glob("renderD*")) if Path("/dev/dri").exists() else []:
        if candidate.exists():
            return str(candidate)
    return None


def cso_runtime_capabilities():
    return {
        "vaapi_available": bool(detect_vaapi_device_path()),
    }


def policy_content_type(policy):
    container = (policy or {}).get("container", "mpegts")
    return CONTAINER_TO_CONTENT_TYPE.get(container, "application/octet-stream")


def policy_ffmpeg_format(policy):
    container = (policy or {}).get("container", "mpegts")
    return CONTAINER_TO_FORMAT.get(container, "mpegts")


def _priority_value(value):
    try:
        return int(value)
    except Exception:
        return 0


def source_capacity_key(source):
    xc_account_id = getattr(source, "xc_account_id", None)
    playlist_id = getattr(source, "playlist_id", None)
    if xc_account_id:
        return f"xc:{int(xc_account_id)}"
    if playlist_id:
        return f"playlist:{int(playlist_id)}"
    return f"source:{int(getattr(source, 'id', 0) or 0)}"


def source_capacity_limit(source):
    xc_account = getattr(source, "xc_account", None)
    if xc_account:
        try:
            return max(0, int(getattr(xc_account, "connection_limit", 0) or 0))
        except Exception:
            return 0
    playlist = getattr(source, "playlist", None)
    if playlist:
        try:
            return max(0, int(getattr(playlist, "connections", 0) or 0))
        except Exception:
            return 0
    return 1_000_000


def source_should_use_cso_buffer(source, force_tvh_remux=False):
    if force_tvh_remux:
        return True
    playlist = getattr(source, "playlist", None)
    return bool(
        getattr(source, "use_hls_proxy", False) and playlist and getattr(playlist, "hls_proxy_use_ffmpeg", False)
    )


def _source_event_context(source, source_url=None):
    if not source:
        return {}
    playlist = getattr(source, "playlist", None)
    stream_name = str(getattr(source, "playlist_stream_name", "") or "").strip()
    playlist_name = str(getattr(playlist, "name", "") or "").strip()
    payload = {
        "source_id": getattr(source, "id", None),
        "playlist_id": getattr(source, "playlist_id", None),
        "playlist_name": playlist_name or None,
        "stream_name": stream_name or None,
        "source_priority": _priority_value(getattr(source, "priority", 0)),
    }
    if source_url:
        payload["source_url"] = source_url
    return payload


@dataclass
class CsoStartResult:
    success: bool
    reason: str | None = None


class CsoCapacityRegistry:
    def __init__(self):
        self._allocations = {}
        self._external_counts = {}
        self._lock = asyncio.Lock()

    async def try_reserve(self, key, owner_key, limit, slot_id=None):
        async with self._lock:
            # key -> {owner_key: {slot_id: 1}}
            current = self._allocations.setdefault(key, {})
            owner_slots = current.setdefault(owner_key, {})
            if slot_id in owner_slots:
                # Owner already holds this specific slot; do not ref-count leak.
                return True

            # Count total slots across ALL owners for this key
            total_active = sum(len(slots) for slots in current.values())
            external = int(self._external_counts.get(key) or 0)
            if (total_active + external) >= max(0, int(limit or 0)):
                return False

            owner_slots[slot_id] = 1
            return True

    async def release(self, key, owner_key, slot_id=None):
        async with self._lock:
            current = self._allocations.get(key)
            if not current:
                return
            owner_slots = current.get(owner_key)
            if not owner_slots:
                return
            owner_slots.pop(slot_id, None)
            if not owner_slots:
                current.pop(owner_key, None)
            if not current:
                self._allocations.pop(key, None)

    async def release_all(self, owner_key):
        """Release all slots held by a specific owner across all keys."""
        async with self._lock:
            for key in list(self._allocations.keys()):
                current = self._allocations[key]
                current.pop(owner_key, None)
                if not current:
                    self._allocations.pop(key, None)

    async def set_external_counts(self, counts):
        async with self._lock:
            normalized = {}
            for key, count in (counts or {}).items():
                try:
                    value = int(count or 0)
                except Exception:
                    value = 0
                if value > 0:
                    normalized[str(key)] = value
            self._external_counts = normalized

    async def get_usage(self, key):
        key_name = str(key or "")
        if not key_name:
            return {"allocations": 0, "external": 0, "total": 0}
        async with self._lock:
            current = self._allocations.get(key_name, {})
            allocations = sum(len(slots) for slots in current.values())
            external = int(self._external_counts.get(key_name) or 0)
            return {
                "allocations": int(allocations),
                "external": int(external),
                "total": int(allocations + external),
            }


cso_capacity_registry = CsoCapacityRegistry()


def _unwrap_local_tic_hls_proxy_url(url, instance_id=None):
    if not url:
        return None
    parsed = urlparse(url)
    path = parsed.path or ""
    if "/tic-hls-proxy/" not in path:
        return None
    parts = [part for part in path.split("/") if part]
    try:
        proxy_idx = parts.index("tic-hls-proxy")
    except ValueError:
        return None

    if not instance_id:
        return None
    if len(parts) <= proxy_idx + 1:
        return None
    if parts[proxy_idx + 1] != str(instance_id):
        return None

    token = None
    if len(parts) > proxy_idx + 2 and parts[proxy_idx + 2] == "stream":
        if len(parts) > proxy_idx + 3:
            token = parts[proxy_idx + 3]
    elif len(parts) > proxy_idx + 2 and parts[proxy_idx + 2] == "proxy.m3u8":
        source = (parse_qs(parsed.query).get("url") or [None])[0]
        if source and source.startswith(("http://", "https://")):
            return source
        return None
    elif len(parts) > proxy_idx + 2:
        token = parts[proxy_idx + 2]

    if not token:
        return None
    token = token.split(".", 1)[0]
    if not token:
        return None
    try:
        padded = token + "=" * (-len(token) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8")
        if decoded.startswith(("http://", "https://")):
            return decoded
    except Exception:
        return None
    return None


def _resolve_source_url(source_url, base_url, instance_id, stream_key=None, username=None):
    normalized = (source_url or "").strip()
    if not normalized:
        return normalized
    if LOCAL_PROXY_HOST_PLACEHOLDER in normalized:
        normalized = normalized.replace(LOCAL_PROXY_HOST_PLACEHOLDER, base_url)
    # Only unwrap this same instance's local TIC HLS proxy URL.
    for _ in range(4):
        next_url = _unwrap_local_tic_hls_proxy_url(normalized, instance_id=instance_id)
        if not next_url:
            break
        if next_url == normalized:
            break
        normalized = next_url

    if is_local_hls_proxy_url(normalized, instance_id=instance_id):
        # CSO ingest must never route through this same instance's HLS proxy.
        # If we still have a local proxy URL at this point, treat it as unresolved.
        logger.warning(
            "CSO source URL still points to local HLS proxy after unwrapping; skipping source url=%s", normalized
        )
        return ""
    elif stream_key and "/tic-hls-proxy/" in normalized and "stream_key=" not in normalized:
        normalized = append_stream_key(normalized, stream_key=stream_key, username=username)
    return normalized


def resolve_source_url_for_stream(source_url, base_url, instance_id, stream_key=None, username=None):
    return _resolve_source_url(
        source_url,
        base_url,
        instance_id,
        stream_key=stream_key,
        username=username,
    )


def _resolve_source_url_candidates(source, base_url, instance_id, stream_key=None, username=None):
    stream_url = (getattr(source, "playlist_stream_url", None) or "").strip()
    primary_url = _resolve_source_url(
        stream_url,
        base_url=base_url,
        instance_id=instance_id,
        stream_key=stream_key,
        username=username,
    )
    if not primary_url:
        return []

    xc_account_id = getattr(source, "xc_account_id", None)
    if not xc_account_id:
        return [primary_url]

    playlist = getattr(source, "playlist", None)
    raw_hosts = getattr(playlist, "url", None) if playlist is not None else None
    host_list = parse_xc_hosts(raw_hosts)
    if len(host_list) <= 1:
        return [primary_url]

    parsed_primary = urlparse(primary_url)
    if not parsed_primary.scheme or not parsed_primary.netloc:
        return [primary_url]

    candidates = [primary_url]
    for host in host_list:
        parsed_host = urlparse(host if "://" in host else f"{parsed_primary.scheme}://{host}")
        if not parsed_host.scheme or not parsed_host.netloc:
            continue
        candidate = parsed_primary._replace(scheme=parsed_host.scheme, netloc=parsed_host.netloc).geturl()
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates


async def emit_channel_stream_event(
    *,
    channel_id=None,
    source_id=None,
    playlist_id=None,
    recording_id=None,
    tvh_subscription_id=None,
    session_id=None,
    event_type,
    severity="info",
    details=None,
):
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


def _header_value(headers, name):
    target = str(name or "").strip().lower()
    if not target:
        return None
    for key, value in (headers or {}).items():
        if str(key or "").strip().lower() == target:
            return str(value or "").strip() or None
    return None


def _format_ffmpeg_headers_arg(headers):
    lines = []
    for key, value in (headers or {}).items():
        lower = str(key or "").strip().lower()
        if lower in {"user-agent", "referer"}:
            continue
        text = str(value or "").strip()
        if not text:
            continue
        lines.append(f"{key}: {text}")
    if not lines:
        return None
    # FFmpeg expects CRLF-separated request headers.
    return "\r\n".join(lines) + "\r\n"


def _build_ingest_ffmpeg_command(source_url, program_index=0, user_agent=None, request_headers=None):
    map_program = max(0, int(program_index or 0))
    is_hls_input = (urlparse(str(source_url or "")).path or "").lower().endswith(".m3u8")
    header_values = sanitise_headers(request_headers)
    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "info" if enable_cso_command_debug_logging else "warning",
    ]
    if enable_cso_command_debug_logging:
        command += ["-stats"]
    else:
        command += ["-nostats"]
    command += [
        # Progress reporting for health-checks; reconnect flags for resilience.
        "-progress",
        "pipe:2",
        "-reconnect",
        "1",
        "-reconnect_on_network_error",
        "1",
        "-reconnect_delay_max",
        str(max(1, int(CSO_INGEST_RECONNECT_DELAY_MAX_SECONDS))),
    ]
    user_agent_value = str(user_agent or "").strip() or _header_value(header_values, "User-Agent")
    if user_agent_value:
        command += [
            "-user_agent",
            user_agent_value,
        ]
    referer_value = _header_value(header_values, "Referer")
    if referer_value:
        command += ["-referer", referer_value]
    extra_headers = _format_ffmpeg_headers_arg(header_values)
    if extra_headers:
        command += ["-headers", extra_headers]
    # For HLS, we let the smart HLS demuxer handle playlist retries to avoid manifest
    # spam/IP bans. For direct streams (non-HLS), we use socket-level reconnection.
    if not is_hls_input:
        command += [
            "-reconnect_at_eof",
            "1",
            "-reconnect_streamed",
            "1",
            "-reconnect_on_http_error",
            "4xx,5xx",
        ]
    else:
        # HLS demuxing already handles playlist refresh and segment polling.
        # Avoid EOF/http reconnect loops that can spam upstream proxy requests.
        command += [
            "-reconnect_streamed",
            "0",
        ]
    command += [
        # Tolerate malformed/corrupt packets and continue ingest.
        "-fflags",
        "+discardcorrupt+genpts",
        "-err_detect",
        "ignore_err",
        # Bound network stalls so reconnect logic can recover.
        "-rw_timeout",
        str(max(1_000_000, int(CSO_INGEST_RW_TIMEOUT_US))),
        "-timeout",
        str(max(1_000_000, int(CSO_INGEST_TIMEOUT_US))),
        "-i",
        source_url,
        "-map",
        f"0:p:{map_program}:v:0?",
        "-map",
        f"0:p:{map_program}:a?",
        "-map",
        f"0:p:{map_program}:s?",
        "-c",
        "copy",
        "-dn",
        "-muxdelay",
        "0",
        "-muxpreload",
        "0",
        "-f",
        "mpegts",
        "pipe:1",
    ]
    return command


def _resolve_cso_ingest_user_agent(config, source):
    playlist = getattr(source, "playlist", None) if source is not None else None
    playlist_user_agent = str(getattr(playlist, "user_agent", "") or "").strip()
    if playlist_user_agent:
        return playlist_user_agent

    settings = {}
    try:
        settings = config.read_settings() if config else {}
    except Exception:
        settings = {}
    defaults = settings.get("settings", {}).get("user_agents", [])
    if isinstance(defaults, list):
        for item in defaults:
            if not isinstance(item, dict):
                continue
            candidate = str(item.get("value") or item.get("name") or "").strip()
            if candidate:
                return candidate
    return "VLC/3.0.23 LibVLC/3.0.23"


def _resolve_cso_ingest_headers(source):
    playlist = getattr(source, "playlist", None) if source is not None else None
    try:
        configured = parse_headers_json(getattr(playlist, "hls_proxy_headers", None))
    except ValueError:
        configured = {}
    return configured


def _redact_ingest_command_for_log(command):
    redacted = list(command or [])
    for idx, token in enumerate(redacted):
        if token == "-headers" and idx + 1 < len(redacted):
            redacted[idx + 1] = "<redacted>"
    return redacted


def _policy_log_label(policy):
    data = policy or {}
    return (
        f"output_mode={data.get('output_mode', 'force_remux')}, "
        f"container={data.get('container', 'mpegts')}, "
        f"video_codec={data.get('video_codec', '') or 'copy'}, "
        f"audio_codec={data.get('audio_codec', '') or 'copy'}, "
        f"subtitle_mode={data.get('subtitle_mode', 'copy')}, "
        f"hwaccel={bool(data.get('hwaccel', False))}, "
        f"deinterlace={bool(data.get('deinterlace', False))}"
    )


class CsoOutputFfmpegCommandBuilder:
    """Curated FFmpeg command builder for CSO output pipelines."""

    def __init__(self, policy):
        self.policy = policy or {}

    def _base(self):
        command = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "info" if enable_cso_command_debug_logging else "warning",
        ]
        if enable_cso_command_debug_logging:
            command += ["-stats"]
        command += [
            "-f",
            "mpegts",
            "-i",
            "pipe:0",
            "-map",
            "0:v:0?",
            "-map",
            "0:a?",
        ]
        command += [
            # Keep output path resilient to malformed packets and timestamp drift.
            "-fflags",
            "+discardcorrupt+genpts",
            "-err_detect",
            "ignore_err",
            "-max_muxing_queue_size",
            "4096",
        ]
        return command

    @staticmethod
    def video_encoder_for_codec(video_codec: str) -> str:
        codec = str(video_codec or "")
        return {
            "h264": "libx264",
            "h265": "libx265",
            "vp8": "libvpx",
        }.get(codec, "libx264")

    @staticmethod
    def vaapi_encoder_for_codec(video_codec: str) -> str:
        codec = str(video_codec or "")
        return "hevc_vaapi" if codec == "h265" else "h264_vaapi"

    @staticmethod
    def audio_encoder_for_codec(audio_codec: str) -> str:
        codec = str(audio_codec or "")
        return {
            "aac": "aac",
            "ac3": "ac3",
            "vorbis": "libvorbis",
        }.get(codec, "aac")

    def _apply_stream_selection(self, command):
        subtitle_mode = self.policy.get("subtitle_mode") or "copy"
        if subtitle_mode != "drop":
            command += ["-map", "0:s?"]
        return subtitle_mode

    def _apply_transcode_options(self, command, subtitle_mode):
        video_codec = self.policy.get("video_codec") or ""
        audio_codec = self.policy.get("audio_codec") or ""
        use_hwaccel = bool(self.policy.get("hwaccel", False)) and bool(video_codec)
        deinterlace = bool(self.policy.get("deinterlace", False)) and bool(video_codec)
        vaapi_device = detect_vaapi_device_path() if use_hwaccel else None

        if video_codec:
            if vaapi_device:
                encoder = self.vaapi_encoder_for_codec(video_codec)
                filters = []
                if deinterlace:
                    filters.append("bwdif=mode=send_frame:parity=auto:deint=all")
                filters += ["format=nv12", "hwupload"]
                command += ["-vaapi_device", vaapi_device, "-vf", ",".join(filters), "-c:v", encoder]
            else:
                if deinterlace:
                    command += ["-vf", "bwdif=mode=send_frame:parity=auto:deint=all"]
                sw_video_encoder = self.video_encoder_for_codec(video_codec)
                command += ["-c:v", sw_video_encoder]
        else:
            command += ["-c:v", "copy"]

        if audio_codec:
            sw_audio_encoder = self.audio_encoder_for_codec(audio_codec)
            command += ["-c:a", sw_audio_encoder]
            # Rebuild audio timestamps during transcode to reduce DTS regressions downstream.
            command += ["-af", "aresample=async=1:first_pts=0"]
            if audio_codec == "aac":
                command += ["-b:a", "128k", "-ar", "48000", "-ac", "2"]
        else:
            command += ["-c:a", "copy"]
        command += ["-c:s", "copy" if subtitle_mode != "drop" else "none"]
        if subtitle_mode == "drop":
            command.append("-sn")

    def build_output_command(self):
        command = self._base()
        subtitle_mode = self._apply_stream_selection(command)
        mode = self.policy.get("output_mode") or "force_remux"
        ffmpeg_format = policy_ffmpeg_format(self.policy)

        if mode == "force_transcode":
            self._apply_transcode_options(command, subtitle_mode)
        else:
            command += ["-c", "copy"]
            if ffmpeg_format == "mp4":
                # TS/HLS (MPEG-TS) often carries AAC with ADTS headers per frame.
                # MP4 stores codec config once in container header (extradata), so
                # remuxing copy into MP4 needs this bitstream rewrite.
                command += ["-bsf:a", "aac_adtstoasc"]
            if subtitle_mode == "drop":
                command.append("-sn")

        # Hard CSO rule: never include data streams in output.
        command.append("-dn")
        if ffmpeg_format == "mpegts":
            command += [
                "-mpegts_flags",
                "+resend_headers",
                "-muxdelay",
                "0",
                "-muxpreload",
                "0",
            ]
        elif ffmpeg_format == "mp4":
            # Fragmented MP4 is required for live streaming to a pipe.
            command += ["-movflags", "+frag_keyframe+empty_moov+default_base_moof"]
        command += ["-f", ffmpeg_format, "pipe:1"]
        return command

    def build_hls_output_command(self, output_dir: Path):
        command = self._base()
        subtitle_mode = self._apply_stream_selection(command)
        mode = self.policy.get("output_mode") or "force_remux"

        if mode == "force_transcode":
            self._apply_transcode_options(command, subtitle_mode)
        else:
            command += ["-c", "copy"]
            if subtitle_mode == "drop":
                command.append("-sn")

        command.append("-dn")
        segment_pattern = str(output_dir / "seg_%06d.ts")
        playlist_path = str(output_dir / "index.m3u8")
        command += [
            "-f",
            "hls",
            "-hls_time",
            str(max(1, int(CSO_HLS_SEGMENT_SECONDS))),
            "-hls_segment_type",
            "mpegts",
            "-hls_list_size",
            str(max(3, int(CSO_HLS_LIST_SIZE))),
            "-hls_flags",
            "delete_segments+append_list+independent_segments+omit_endlist+temp_file",
            "-hls_delete_threshold",
            "2",
            "-hls_segment_filename",
            segment_pattern,
            playlist_path,
        ]
        return command


class CsoIngestSession:
    def __init__(
        self,
        key,
        channel_id,
        sources,
        *,
        request_base_url,
        instance_id,
        capacity_owner_key,
        stream_key=None,
        username=None,
        allow_failover=True,
        ingest_user_agent=None,
    ):
        self.key = key
        self.channel_id = channel_id
        self.sources = list(sources or [])
        self.request_base_url = request_base_url
        self.instance_id = instance_id
        self.capacity_owner_key = capacity_owner_key
        self.stream_key = stream_key
        self.username = username
        self.allow_failover = bool(allow_failover)
        self.ingest_user_agent = str(ingest_user_agent or "").strip()
        self.process = None
        self.read_task = None
        self.stderr_task = None
        self.running = False
        self.lock = asyncio.Lock()
        self.last_activity = time.time()
        self.subscribers = {}
        self.history = deque()
        self.history_bytes = 0
        self.max_history_bytes = 4 * 1024 * 1024
        self.current_source = None
        self.current_source_url = ""
        self.current_capacity_key = None
        self.failed_source_until = {}
        self.last_error = None
        self.health_task = None
        self.last_chunk_ts = 0.0
        self.last_source_start_ts = 0.0
        self.low_speed_since = None
        self.last_ffmpeg_speed = None
        self.last_ffmpeg_speed_ts = 0.0
        self.health_failover_reason = None
        self.health_failover_details = None
        self.last_reader_end_reason = None
        self.last_reader_end_saw_data = False
        self.last_reader_end_return_code = None
        self.last_reader_end_ts = 0.0
        self._recent_ffmpeg_stderr = deque(maxlen=50)
        self.http_error_timestamps = deque(maxlen=200)
        self.hls_variants = []
        self.current_variant_position = None
        self.current_program_index = 0
        self.source_program_index = {}
        self.startup_jump_done = False
        self.process_token = 0
        self.failover_failed_sources = set()

    async def _refresh_sources_from_db(self):
        """Refresh the internal sources list from the database to capture state changes."""
        channel = await resolve_channel_for_stream(self.channel_id)
        if channel:
            async with self.lock:
                self.sources = list(channel.sources or [])
                logger.debug(
                    "CSO ingest refreshed sources channel=%s count=%s",
                    self.channel_id,
                    len(self.sources),
                )

    async def start(self):
        async with self.lock:
            if self.running:
                return
            self.failover_failed_sources.clear()
            logger.info(
                "CSO ingest start requested channel=%s sources=%s",
                self.channel_id,
                len(self.sources or []),
            )
            start_result = await self._start_best_source_unlocked(reason="initial_start")
            if not start_result.success:
                self.running = False
                self.last_error = start_result.reason or "no_available_source"
                return

    async def _spawn_ingest_process(self, source_url, program_index, source=None):
        playlist = getattr(source, "playlist", None) if source is not None else None
        source_user_agent = str(getattr(playlist, "user_agent", "") or "").strip() or self.ingest_user_agent
        source_headers = _resolve_cso_ingest_headers(source)
        source_user_agent = _header_value(source_headers, "User-Agent") or source_user_agent
        command = _build_ingest_ffmpeg_command(
            source_url,
            program_index=program_index,
            user_agent=source_user_agent,
            request_headers=source_headers,
        )
        logger.info(
            "Starting CSO ingest channel=%s source=%s command=%s",
            self.channel_id,
            getattr(source, "id", None) if source is not None else getattr(self.current_source, "id", None),
            _redact_ingest_command_for_log(command),
        )
        return await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    def _activate_process_unlocked(self, process):
        self.process = process
        self.running = True
        self.process_token += 1
        token = self.process_token
        self.last_source_start_ts = time.time()
        self.last_chunk_ts = self.last_source_start_ts
        self.low_speed_since = None
        self.last_ffmpeg_speed = None
        self.last_ffmpeg_speed_ts = self.last_source_start_ts
        self.http_error_timestamps.clear()
        self.health_failover_reason = None
        self.health_failover_details = None
        self.last_reader_end_reason = None
        self.last_reader_end_saw_data = False
        self.last_reader_end_return_code = None
        self.last_reader_end_ts = 0.0
        logger.info(
            "CSO ingest upstream connected channel=%s source_id=%s source_url=%s subscribers=%s",
            self.channel_id,
            getattr(self.current_source, "id", None),
            self.current_source_url,
            len(self.subscribers),
        )
        self.read_task = asyncio.create_task(self._read_loop(token, process))
        self.stderr_task = asyncio.create_task(self._stderr_loop(token, process))
        self.health_task = asyncio.create_task(self._health_loop(token))

    def _eligible_source_ids_unlocked(self):
        eligible_ids = set()
        for source in self.sources:
            source_id = getattr(source, "id", None)
            if source_id is None:
                continue
            playlist = getattr(source, "playlist", None)
            if playlist is not None and not bool(getattr(playlist, "enabled", False)):
                continue
            xc_account = getattr(source, "xc_account", None)
            if xc_account is not None and not bool(getattr(xc_account, "enabled", False)):
                continue
            stream_url = (getattr(source, "playlist_stream_url", None) or "").strip()
            if not stream_url:
                continue
            eligible_ids.add(source_id)
        return eligible_ids

    async def _start_best_source_unlocked(
        self,
        reason,
        preferred_source_id=None,
        excluded_source_ids=None,
        ignore_hold_down=False,
    ):
        now = time.time()
        excluded_ids = set(excluded_source_ids or [])
        candidates = sorted(
            self.sources,
            key=lambda item: _priority_value(getattr(item, "priority", 0)),
            reverse=True,
        )
        if preferred_source_id is not None:
            preferred = [source for source in candidates if getattr(source, "id", None) == preferred_source_id]
            others = [source for source in candidates if getattr(source, "id", None) != preferred_source_id]
            candidates = preferred + others
        saw_capacity_block = False
        for source in candidates:
            source_id = getattr(source, "id", None)
            if source_id in excluded_ids:
                continue
            hold_until = self.failed_source_until.get(source_id, 0)
            if not ignore_hold_down and hold_until > now:
                continue
            playlist = getattr(source, "playlist", None)
            if playlist is not None and not bool(getattr(playlist, "enabled", False)):
                continue
            xc_account = getattr(source, "xc_account", None)
            if xc_account is not None and not bool(getattr(xc_account, "enabled", False)):
                continue
            stream_url = (getattr(source, "playlist_stream_url", None) or "").strip()
            if not stream_url:
                continue

            capacity_key = source_capacity_key(source)
            capacity_limit = source_capacity_limit(source)
            reserved = await cso_capacity_registry.try_reserve(
                capacity_key,
                self.capacity_owner_key,
                capacity_limit,
                slot_id=source_id,
            )
            if not reserved:
                saw_capacity_block = True
                continue

            source_urls = _resolve_source_url_candidates(
                source,
                base_url=self.request_base_url,
                instance_id=self.instance_id,
                stream_key=self.stream_key,
                username=self.username,
            )
            if not source_urls:
                await cso_capacity_registry.release(capacity_key, self.capacity_owner_key, slot_id=source_id)
                continue

            process = None
            resolved_url = ""
            variants = []
            variant_position = None
            remembered_program_index = self.source_program_index.get(source_id)
            last_error = None
            for candidate_url in source_urls:
                variants = await _discover_hls_variants(candidate_url)
                variant_position = None
                if variants:
                    if remembered_program_index is not None:
                        for idx, item in enumerate(variants):
                            if int(item.get("program_index") or 0) == int(remembered_program_index):
                                variant_position = idx
                                break
                    if variant_position is None:
                        variant_position = len(variants) - 1
                    program_index = int(variants[variant_position].get("program_index") or 0)
                else:
                    program_index = int(remembered_program_index or 0)
                    if remembered_program_index is not None:
                        logger.info(
                            "CSO ingest variant discovery empty; reusing remembered program index "
                            "channel=%s source_id=%s program_index=%s",
                            self.channel_id,
                            source_id,
                            program_index,
                        )
                try:
                    process = await self._spawn_ingest_process(candidate_url, program_index, source=source)
                    resolved_url = candidate_url
                    break
                except Exception as exc:
                    last_error = exc
                    await emit_channel_stream_event(
                        channel_id=self.channel_id,
                        source_id=getattr(source, "id", None),
                        playlist_id=getattr(source, "playlist_id", None),
                        session_id=self.key,
                        event_type="playback_unavailable",
                        severity="warning",
                        details={
                            "reason": "ingest_start_failed",
                            "pipeline": "ingest",
                            "error": str(exc),
                            **_source_event_context(source, source_url=candidate_url),
                        },
                    )
                    continue

            if not process:
                if last_error:
                    logger.warning(
                        "CSO ingest failed for all URLs on source channel=%s source_id=%s error=%s",
                        self.channel_id,
                        source_id,
                        last_error,
                    )
                self.current_source = None
                self.current_source_url = ""
                self.current_capacity_key = None
                self.running = False
                self.process = None
                await cso_capacity_registry.release(capacity_key, self.capacity_owner_key, slot_id=source_id)
                continue
            old_capacity_key = self.current_capacity_key
            old_source_id = getattr(self.current_source, "id", None)
            self.current_source = source
            self.current_source_url = resolved_url
            self.current_capacity_key = capacity_key
            self.hls_variants = variants
            self.current_variant_position = variant_position
            self.current_program_index = program_index
            if source_id is not None:
                self.source_program_index[source_id] = int(program_index)
            self.startup_jump_done = True
            self._activate_process_unlocked(process)
            if old_capacity_key:
                await cso_capacity_registry.release(old_capacity_key, self.capacity_owner_key, slot_id=old_source_id)

            await emit_channel_stream_event(
                channel_id=self.channel_id,
                source_id=getattr(source, "id", None),
                playlist_id=getattr(source, "playlist_id", None),
                session_id=self.key,
                event_type="switch_success",
                severity="info",
                details={
                    "reason": reason,
                    "pipeline": "ingest",
                    "program_index": self.current_program_index,
                    "variant_count": len(self.hls_variants),
                    **_source_event_context(source, source_url=self.current_source_url),
                },
            )
            return CsoStartResult(success=True)

        return CsoStartResult(success=False, reason="capacity_blocked" if saw_capacity_block else "no_available_source")

    async def _stderr_loop(self, token, process):
        if not process:
            return
        text_buffer = ""
        while True:
            try:
                chunk = await process.stderr.read(4096)
            except Exception:
                break
            if not chunk:
                break
            if token != self.process_token:
                break
            text_buffer += chunk.decode("utf-8", errors="replace")
            lines = re.split(r"[\r\n]+", text_buffer)
            text_buffer = lines.pop() if lines else ""
            for rendered in lines:
                rendered = rendered.strip()
                if not rendered:
                    continue
                self._recent_ffmpeg_stderr.append(rendered)
                progress_handled = False
                if "=" in rendered:
                    key, value = rendered.split("=", 1)
                    key = key.strip().lower()
                    value = value.strip()
                    if key == "speed":
                        progress_handled = True
                        value = value.rstrip("xX")
                        try:
                            self.last_ffmpeg_speed = float(value)
                            self.last_ffmpeg_speed_ts = time.time()
                        except Exception:
                            self.last_ffmpeg_speed = None

                if not progress_handled:
                    speed_match = _FFMPEG_SPEED_RE.search(rendered)
                    if speed_match:
                        try:
                            self.last_ffmpeg_speed = float(speed_match.group(1))
                            self.last_ffmpeg_speed_ts = time.time()
                        except Exception:
                            self.last_ffmpeg_speed = None

                lower = rendered.lower()
                if (
                    "http error" in lower
                    or "server returned" in lower
                    or "forbidden" in lower
                    or "unauthorized" in lower
                ):
                    status_codes = _HTTP_STATUS_CODE_RE.findall(rendered)
                    if status_codes:
                        if any(code.startswith("4") or code.startswith("5") for code in status_codes):
                            self.http_error_timestamps.append(time.time())
                    else:
                        self.http_error_timestamps.append(time.time())
        rendered = text_buffer.strip()
        if rendered and token == self.process_token:
            self._recent_ffmpeg_stderr.append(rendered)
            lower = rendered.lower()
            if "http error" in lower or "server returned" in lower or "forbidden" in lower or "unauthorized" in lower:
                status_codes = _HTTP_STATUS_CODE_RE.findall(rendered)
                if status_codes:
                    if any(code.startswith("4") or code.startswith("5") for code in status_codes):
                        self.http_error_timestamps.append(time.time())
                else:
                    self.http_error_timestamps.append(time.time())

    def _ffmpeg_error_summary(self):
        lines = [line for line in self._recent_ffmpeg_stderr if line]
        if not lines:
            return ""
        error_lines = [
            line
            for line in lines
            if any(token in line.lower() for token in ("error", "invalid", "failed", "could not", "unsupported"))
        ]
        selected = error_lines[-3:] if error_lines else lines[-3:]
        return " | ".join(selected)

    async def _health_loop(self, token):
        if not self.allow_failover:
            return
        while self.running and token == self.process_token:
            await asyncio.sleep(1.0)
            now = time.time()
            if (now - self.last_source_start_ts) < CSO_STARTUP_GRACE_SECONDS_DEFAULT:
                continue

            if self.http_error_timestamps:
                window_seconds = max(1, int(CSO_HTTP_ERROR_WINDOW_SECONDS_DEFAULT))
                threshold = max(1, int(CSO_HTTP_ERROR_THRESHOLD_DEFAULT))
                while self.http_error_timestamps and (now - self.http_error_timestamps[0]) > window_seconds:
                    self.http_error_timestamps.popleft()
                if len(self.http_error_timestamps) >= threshold:
                    await self._request_health_failover(
                        "http_error_burst",
                        {
                            "http_error_count": len(self.http_error_timestamps),
                            "threshold_count": threshold,
                            "window_seconds": window_seconds,
                        },
                    )
                    return

            # Treat stall as actionable only when we have sustained no-data and
            # ingest is not keeping up at realtime speed.
            if self.last_chunk_ts and (now - self.last_chunk_ts) >= CSO_STALL_SECONDS_DEFAULT:
                speed = self.last_ffmpeg_speed
                speed_age = now - float(self.last_ffmpeg_speed_ts or 0.0)
                speed_stale_seconds = max(1, int(CSO_SPEED_STALE_SECONDS_DEFAULT))
                speed_is_stale = speed_age >= speed_stale_seconds
                if speed is not None and not speed_is_stale and speed >= 1.0:
                    continue
                await self._request_health_failover(
                    "stall_timeout",
                    {
                        "stall_seconds": round(now - self.last_chunk_ts, 2),
                        "threshold_seconds": CSO_STALL_SECONDS_DEFAULT,
                        "speed": speed,
                        "speed_stale": speed_is_stale,
                        "speed_age_seconds": round(speed_age, 2),
                        "speed_stale_threshold_seconds": speed_stale_seconds,
                    },
                )
                return

            speed = self.last_ffmpeg_speed
            speed_age = now - float(self.last_ffmpeg_speed_ts or 0.0)
            if speed is None or speed_age >= max(1, int(CSO_SPEED_STALE_SECONDS_DEFAULT)):
                self.low_speed_since = None
                continue

            if speed < CSO_UNDERSPEED_RATIO_DEFAULT:
                if self.low_speed_since is None:
                    self.low_speed_since = now
                elif (now - self.low_speed_since) >= CSO_UNDERSPEED_WINDOW_SECONDS_DEFAULT:
                    await self._request_health_failover(
                        "under_speed",
                        {
                            "speed": speed,
                            "threshold_ratio": CSO_UNDERSPEED_RATIO_DEFAULT,
                            "window_seconds": CSO_UNDERSPEED_WINDOW_SECONDS_DEFAULT,
                        },
                    )
                    return
            else:
                self.low_speed_since = None

    async def _request_health_failover(self, reason, details):
        async with self.lock:
            if not self.running or not self.process:
                return
            if self.health_failover_reason:
                return
            self.health_failover_reason = reason
            self.health_failover_details = details or {}
            process = self.process
            source = self.current_source
            source_url = self.current_source_url

        logger.warning(
            "CSO ingest health-triggered failover channel=%s source_id=%s reason=%s details=%s",
            self.channel_id,
            getattr(self.current_source, "id", None),
            reason,
            details,
        )
        await emit_channel_stream_event(
            channel_id=self.channel_id,
            source_id=getattr(source, "id", None),
            playlist_id=getattr(source, "playlist_id", None),
            session_id=self.key,
            event_type="health_actioned",
            severity="warning",
            details={
                "reason": reason,
                "pipeline": "ingest",
                "action": "trigger_failover",
                **(details or {}),
                **_source_event_context(source, source_url=source_url),
            },
        )
        try:
            process.terminate()
        except Exception:
            pass

    async def _read_loop(self, token, process):
        saw_data = False
        return_code = None
        try:
            while self.running and token == self.process_token and process and process.stdout:
                chunk = await process.stdout.read(16384)
                if not chunk:
                    break
                saw_data = True
                self.last_chunk_ts = time.time()
                await self._broadcast(chunk)
        finally:
            if process:
                try:
                    return_code = process.returncode
                    if return_code is None:
                        return_code = await process.wait()
                except Exception:
                    return_code = None

            if token != self.process_token:
                return

            self.last_reader_end_reason = "ingest_reader_ended"
            self.last_reader_end_saw_data = bool(saw_data)
            self.last_reader_end_return_code = return_code
            self.last_reader_end_ts = time.time()

            async with self.lock:
                has_subscribers = bool(self.subscribers)
            if not has_subscribers:
                logger.info(
                    "CSO ingest channel=%s reader ended with no subscribers (saw_data=%s return_code=%s)",
                    self.channel_id,
                    saw_data,
                    return_code,
                )
                await self.stop(force=True)
                return

            failover_reason = self.health_failover_reason or "ingest_reader_ended"
            failover_details = self.health_failover_details or {}
            if return_code not in (None, 0):
                logger.warning(
                    "CSO ingest non-zero exit channel=%s return_code=%s reason=%s stderr=%s",
                    self.channel_id,
                    return_code,
                    failover_reason,
                    self._ffmpeg_error_summary() or "n/a",
                )
            switched = await self._switch_source_after_failure(
                reason=failover_reason,
                return_code=return_code,
                saw_data=saw_data,
                details=failover_details,
            )
            if switched:
                return
            logger.info(
                "CSO ingest channel=%s reader ended (saw_data=%s return_code=%s)",
                self.channel_id,
                saw_data,
                return_code,
            )
            await self.stop(force=True)

    async def _switch_source_after_failure(self, reason, return_code, saw_data, details=None):
        graceful_reader_end = bool(reason == "ingest_reader_ended" and saw_data and return_code == 0)
        async with self.lock:
            if not self.subscribers:
                old_capacity_key = self.current_capacity_key
                self.current_source = None
                self.current_source_url = ""
                self.current_capacity_key = None
                self.process = None
                self.running = False
                if old_capacity_key:
                    await cso_capacity_registry.release(old_capacity_key, self.capacity_owner_key)
                return False

            failed_source = self.current_source
            failed_source_id = getattr(failed_source, "id", None)
            if failed_source_id is not None:
                self.failover_failed_sources.add(failed_source_id)
            ffmpeg_error = self._ffmpeg_error_summary()
            ffmpeg_error_lower = (ffmpeg_error or "").lower()
            is_connectivity_startup_failure = (
                reason == "ingest_reader_ended"
                and return_code not in (None, 0)
                and any(
                    token in ffmpeg_error_lower
                    for token in (
                        "connection refused",
                        "timed out",
                        "network is unreachable",
                        "name or service not known",
                        "could not resolve",
                        "forbidden",
                        "unauthorized",
                        "http error",
                        "server returned",
                        "invalid data",
                    )
                )
            )

            # Apply source hold-down only for health-triggered failover. For generic ingest
            # exits we allow immediate same-source restart to avoid tearing down clients,
            # except startup/connectivity failures where immediate same-source retry
            # causes endless loops on an unavailable upstream.
            multi_source_channel = len(self.sources or []) > 1
            hold_down_applicable = reason in {"under_speed", "stall_timeout"} or is_connectivity_startup_failure
            hold_down_applied = bool(failed_source_id and multi_source_channel and hold_down_applicable)

            if hold_down_applied:
                self.failed_source_until[failed_source_id] = time.time() + CSO_SOURCE_HOLD_DOWN_SECONDS

            old_capacity_key = self.current_capacity_key
            self.current_source = None
            self.current_source_url = ""
            self.current_capacity_key = None
            self.hls_variants = []
            self.current_variant_position = None
            self.current_program_index = 0
            self.startup_jump_done = False
            self.process = None
            self.running = False
            if old_capacity_key:
                await cso_capacity_registry.release(
                    old_capacity_key,
                    self.capacity_owner_key,
                    slot_id=failed_source_id,
                )

        if not self.allow_failover:
            if graceful_reader_end:
                logger.info(
                    "CSO ingest graceful reader end channel=%s source_id=%s saw_data=%s return_code=%s",
                    self.channel_id,
                    failed_source_id,
                    saw_data,
                    return_code,
                )
                return False
            event_type = "capacity_blocked" if reason == "capacity_blocked" else "playback_unavailable"
            await emit_channel_stream_event(
                channel_id=self.channel_id,
                source_id=failed_source_id,
                playlist_id=getattr(failed_source, "playlist_id", None),
                session_id=self.key,
                event_type=event_type,
                severity="warning",
                details={
                    "reason": reason,
                    "return_code": return_code,
                    "saw_data": saw_data,
                    "pipeline": "ingest",
                    "ffmpeg_error": ffmpeg_error or None,
                    **(details or {}),
                    **_source_event_context(failed_source),
                },
            )
            return False

        await emit_channel_stream_event(
            channel_id=self.channel_id,
            source_id=failed_source_id,
            playlist_id=getattr(failed_source, "playlist_id", None),
            session_id=self.key,
            event_type="switch_attempt",
            severity="warning",
            details={
                "reason": reason,
                "return_code": return_code,
                "saw_data": saw_data,
                "pipeline": "ingest",
                "ffmpeg_error": ffmpeg_error or None,
                **(details or {}),
                **_source_event_context(failed_source),
            },
        )
        logger.info(
            "CSO ingest failover decision channel=%s reason=%s failed_source_id=%s hold_down_applied=%s",
            self.channel_id,
            reason,
            failed_source_id,
            hold_down_applied,
        )

        await self._refresh_sources_from_db()

        # If a single-source channel (or if only one source is currently enabled)
        # exits gracefully with code 0, allow an immediate restart of that same source
        # to bridge the upstream disconnection without cycling through others or holding down.
        if graceful_reader_end:
            async with self.lock:
                eligible_ids = self._eligible_source_ids_unlocked()
            if len(eligible_ids) == 1 and failed_source_id in eligible_ids:
                logger.info(
                    "CSO ingest immediate restart of only eligible source after graceful end channel=%s source_id=%s",
                    self.channel_id,
                    failed_source_id,
                )
                async with self.lock:
                    self.failover_failed_sources.clear()
                    start_result = await self._start_best_source_unlocked(reason="failover", ignore_hold_down=True)
                    if start_result.success:
                        self.running = True
                        return True

        deadline = time.time() + CSO_INGEST_RECOVERY_RETRY_WINDOW_SECONDS
        last_result = CsoStartResult(success=False, reason="no_available_source")
        while True:
            async with self.lock:
                has_subscribers = bool(self.subscribers)
                eligible_ids = self._eligible_source_ids_unlocked()
                cycle_failed_ids = set(self.failover_failed_sources).intersection(eligible_ids)
                untried_ids = eligible_ids.difference(cycle_failed_ids)
                recycle_failed_sources = bool(eligible_ids) and not bool(untried_ids)
                excluded_ids = cycle_failed_ids if untried_ids else set()
                if recycle_failed_sources:
                    # All currently eligible sources have failed at least once in this
                    # cycle, so recycle the list and allow immediate retries.
                    self.failover_failed_sources.clear()
                start_result = await self._start_best_source_unlocked(
                    reason="failover",
                    excluded_source_ids=excluded_ids,
                    ignore_hold_down=recycle_failed_sources,
                )
                if start_result.success:
                    logger.info(
                        "CSO ingest failover started replacement channel=%s recycled_cycle=%s",
                        self.channel_id,
                        recycle_failed_sources,
                    )
                    self.running = True
                    return True
            last_result = start_result

            if not has_subscribers:
                return False
            if time.time() >= deadline:
                break
            await asyncio.sleep(CSO_INGEST_RECOVERY_RETRY_INTERVAL_SECONDS)

        event_type = "capacity_blocked" if last_result.reason == "capacity_blocked" else "playback_unavailable"
        await emit_channel_stream_event(
            channel_id=self.channel_id,
            source_id=failed_source_id,
            playlist_id=getattr(failed_source, "playlist_id", None),
            session_id=self.key,
            event_type=event_type,
            severity="warning",
            details={
                "reason": last_result.reason,
                "after_failure_reason": reason,
                "pipeline": "ingest",
                "ffmpeg_error": ffmpeg_error or None,
                **(details or {}),
                **_source_event_context(failed_source),
            },
        )
        return False

    async def _broadcast(self, chunk):
        if not chunk:
            return
        self.last_activity = time.time()
        subscriber_queues = []
        async with self.lock:
            self.history.append(chunk)
            self.history_bytes += len(chunk)
            while self.history_bytes > self.max_history_bytes and self.history:
                old = self.history.popleft()
                self.history_bytes -= len(old)
            subscriber_queues = list(self.subscribers.values())
        for q in subscriber_queues:
            await q.put_drop_oldest(chunk)

    async def add_subscriber(self, subscriber_id, prebuffer_bytes=0):
        async with self.lock:
            q = ByteBudgetQueue(max_bytes=CSO_INGEST_SUBSCRIBER_QUEUE_MAX_BYTES)
            if prebuffer_bytes > 0 and self.history:
                total = 0
                items = []
                for chunk in reversed(self.history):
                    items.append(chunk)
                    total += len(chunk)
                    if total >= prebuffer_bytes:
                        break
                for chunk in reversed(items):
                    await q.put_drop_oldest(chunk)
            self.subscribers[subscriber_id] = q
            subscriber_count = len(self.subscribers)
            source_id = getattr(self.current_source, "id", None)
            source_url = self.current_source_url
        logger.info(
            "CSO ingest subscriber added channel=%s ingest_key=%s subscriber=%s subscribers=%s source_id=%s source_url=%s",
            self.channel_id,
            self.key,
            subscriber_id,
            subscriber_count,
            source_id,
            source_url,
        )
        return q

    async def remove_subscriber(self, subscriber_id):
        async with self.lock:
            self.subscribers.pop(subscriber_id, None)
            remaining = len(self.subscribers)
            source_id = getattr(self.current_source, "id", None)
            source_url = self.current_source_url
        logger.info(
            "CSO ingest subscriber removed channel=%s ingest_key=%s subscriber=%s subscribers=%s source_id=%s source_url=%s",
            self.channel_id,
            self.key,
            subscriber_id,
            remaining,
            source_id,
            source_url,
        )
        if remaining == 0:
            await self.stop(force=True)
        return remaining

    async def stop(self, force=False):
        async with self.lock:
            if not self.running and not self.process and not self.subscribers:
                return
            if not force and self.subscribers:
                return
            self.running = False
            process = self.process
            self.process = None
            capacity_key = self.current_capacity_key
            self.current_capacity_key = None
            source_id = getattr(self.current_source, "id", None)
            source_url = self.current_source_url
            self.current_source = None
            self.current_source_url = ""
            self.hls_variants = []
            self.current_variant_position = None
            self.current_program_index = 0
            self.startup_jump_done = False
            self.failover_failed_sources.clear()
            subscriber_count = len(self.subscribers)
        # Release capacity immediately so other channels are not blocked while
        # this ingest session drains/tears down.
        if capacity_key:
            await cso_capacity_registry.release(
                capacity_key,
                self.capacity_owner_key,
                slot_id=source_id,
            )
        await cso_capacity_registry.release_all(self.capacity_owner_key)

        logger.info(
            "Stopping CSO ingest channel=%s ingest_key=%s source_id=%s source_url=%s subscribers=%s force=%s",
            self.channel_id,
            self.key,
            source_id,
            source_url,
            subscriber_count,
            force,
        )
        return_code = None
        if process:
            try:
                process.terminate()
                return_code = await _wait_process_exit_with_timeout(process, timeout_seconds=2.0)
            except Exception:
                try:
                    process.kill()
                    return_code = await _wait_process_exit_with_timeout(process, timeout_seconds=2.0)
                except Exception:
                    logger.warning(
                        "CSO ingest process did not exit after kill channel=%s ingest_key=%s",
                        self.channel_id,
                        self.key,
                    )
                    pass
        health_task = self.health_task
        self.health_task = None
        if health_task and not health_task.done():
            health_task.cancel()
            try:
                await health_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        logger.info(
            "CSO ingest upstream disconnected channel=%s ingest_key=%s source_id=%s return_code=%s",
            self.channel_id,
            self.key,
            source_id,
            return_code,
        )
        async with self.lock:
            for q in self.subscribers.values():
                await q.put_eof()


class CsoOutputSession:
    def __init__(self, key, channel_id, policy, ingest_session):
        self.key = key
        self.channel_id = channel_id
        self.policy = policy
        self.ingest_session = ingest_session
        self.process = None
        self.read_task = None
        self.write_task = None
        self.stderr_task = None
        self.running = False
        self.lock = asyncio.Lock()
        self.last_activity = time.time()
        self.clients = {}
        self.history = deque()
        self.history_bytes = 0
        self.max_history_bytes = 16 * 1024 * 1024
        self.last_error = None
        self.ingest_queue = None
        self._recent_ffmpeg_stderr = deque(maxlen=30)
        self.client_drop_state = {}
        self.client_last_touch = {}

    def _ffmpeg_error_summary(self):
        lines = [line for line in self._recent_ffmpeg_stderr if line]
        if not lines:
            return ""
        error_lines = [
            line
            for line in lines
            if any(token in line.lower() for token in ("error", "invalid", "failed", "could not", "unsupported"))
        ]
        selected = error_lines[-3:] if error_lines else lines[-3:]
        return " | ".join(selected)

    async def start(self):
        async with self.lock:
            if self.running:
                return
            await self.ingest_session.start()
            if not self.ingest_session.running:
                self.last_error = self.ingest_session.last_error or "ingest_not_running"
                return
            self.ingest_queue = await self.ingest_session.add_subscriber(self.key, prebuffer_bytes=256 * 1024)
            command = CsoOutputFfmpegCommandBuilder(self.policy).build_output_command()
            logger.info(
                "Starting CSO output channel=%s output_key=%s policy=(%s) command=%s",
                self.channel_id,
                self.key,
                _policy_log_label(self.policy),
                command,
            )
            self.process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            self.running = True
            logger.info(
                "CSO output started channel=%s output_key=%s policy=(%s) clients=%s",
                self.channel_id,
                self.key,
                _policy_log_label(self.policy),
                len(self.clients),
            )
            self.read_task = asyncio.create_task(self._read_loop())
            self.write_task = asyncio.create_task(self._write_loop())
            self.stderr_task = asyncio.create_task(self._stderr_loop())

    async def _stderr_loop(self):
        if not self.process:
            return
        text_buffer = ""
        while True:
            try:
                chunk = await self.process.stderr.read(4096)
            except Exception:
                break
            if not chunk:
                break
            text_buffer += chunk.decode("utf-8", errors="replace")
            lines = re.split(r"[\r\n]+", text_buffer)
            text_buffer = lines.pop() if lines else ""
            for rendered in lines:
                rendered = rendered.strip()
                if not rendered:
                    continue
                self._recent_ffmpeg_stderr.append(rendered)
                if enable_cso_command_debug_logging:
                    logger.info("CSO output ffmpeg[%s][%s]: %s", self.channel_id, self.key, rendered)
        rendered = text_buffer.strip()
        if rendered:
            self._recent_ffmpeg_stderr.append(rendered)
            if enable_cso_command_debug_logging:
                logger.info("CSO output ffmpeg[%s][%s]: %s", self.channel_id, self.key, rendered)

    async def _write_loop(self):
        try:
            while self.running and self.process and self.process.stdin and self.ingest_queue:
                chunk = await self.ingest_queue.get()
                if chunk is None:
                    break
                try:
                    self.process.stdin.write(chunk)
                    await self.process.stdin.drain()
                except Exception:
                    break
        finally:
            try:
                if self.process and self.process.stdin:
                    self.process.stdin.close()
            except Exception:
                pass

    async def _read_loop(self):
        return_code = None
        try:
            while self.running and self.process and self.process.stdout:
                chunk = await self.process.stdout.read(16384)
                if not chunk:
                    break
                await self._broadcast(chunk)
        finally:
            try:
                if self.process:
                    return_code = self.process.returncode
                    if return_code is None:
                        return_code = await self.process.wait()
            except Exception:
                return_code = None

            async with self.lock:
                client_count = len(self.clients)
                still_running = bool(self.running)

            if still_running and client_count > 0:
                intentional_failover = bool(getattr(self.ingest_session, "health_failover_reason", None))
                ingest_graceful_reader_end = bool(
                    getattr(self.ingest_session, "last_reader_end_reason", None) == "ingest_reader_ended"
                    and bool(getattr(self.ingest_session, "last_reader_end_saw_data", False))
                    and getattr(self.ingest_session, "last_reader_end_return_code", None) == 0
                    and (time.time() - float(getattr(self.ingest_session, "last_reader_end_ts", 0.0) or 0.0)) <= 30.0
                )
                if (intentional_failover and return_code in (None, 0)) or ingest_graceful_reader_end:
                    logger.info(
                        "CSO output reader ended gracefully channel=%s output_key=%s return_code=%s intentional_failover=%s ingest_graceful_reader_end=%s",
                        self.channel_id,
                        self.key,
                        return_code,
                        intentional_failover,
                        ingest_graceful_reader_end,
                    )
                else:
                    self.last_error = "output_reader_ended"
                    ffmpeg_error = self._ffmpeg_error_summary()
                    severity = "error" if return_code not in (None, 0) else "warning"
                    await emit_channel_stream_event(
                        channel_id=self.channel_id,
                        source_id=getattr(self.ingest_session.current_source, "id", None),
                        playlist_id=getattr(self.ingest_session.current_source, "playlist_id", None),
                        session_id=self.key,
                        event_type="playback_unavailable",
                        severity=severity,
                        details={
                            "reason": "output_reader_ended",
                            "return_code": return_code,
                            "ffmpeg_error": ffmpeg_error or None,
                            "policy": self.policy,
                            **_source_event_context(
                                self.ingest_session.current_source,
                                source_url=getattr(self.ingest_session, "current_source_url", None),
                            ),
                        },
                    )

            await self.stop(force=True)

    async def _broadcast(self, chunk):
        if not chunk:
            return
        self.last_activity = time.time()
        now = time.time()
        stale_clients = []
        active_clients = []
        drop_results = {}
        async with self.lock:
            self.history.append(chunk)
            self.history_bytes += len(chunk)
            while self.history_bytes > self.max_history_bytes and self.history:
                old = self.history.popleft()
                self.history_bytes -= len(old)
            for connection_id, q in list(self.clients.items()):
                last_touch = float(self.client_last_touch.get(connection_id, now) or now)
                if (now - last_touch) >= float(CSO_OUTPUT_CLIENT_STALE_SECONDS):
                    stale_clients.append(connection_id)
                    continue
                active_clients.append((connection_id, q))
        for connection_id, q in active_clients:
            drop_results[connection_id] = await q.put_drop_oldest(chunk)
        async with self.lock:
            for connection_id, queue_result in drop_results.items():
                if connection_id not in self.clients:
                    continue
                if int(queue_result.get("dropped_items") or 0) > 0:
                    state = self.client_drop_state.get(connection_id)
                    if not state:
                        state = {
                            "first_ts": now,
                            "last_ts": now,
                            "count": int(queue_result.get("dropped_items") or 0),
                        }
                        self.client_drop_state[connection_id] = state
                    else:
                        state["last_ts"] = now
                        state["count"] = int(state.get("count") or 0) + int(queue_result.get("dropped_items") or 0)
                else:
                    self.client_drop_state.pop(connection_id, None)
        for connection_id in stale_clients:
            logger.warning(
                "CSO output dropping stale client channel=%s output_key=%s connection_id=%s reason=no_consumer_progress stale_seconds=%s",
                self.channel_id,
                self.key,
                connection_id,
                int(CSO_OUTPUT_CLIENT_STALE_SECONDS),
            )
            await self.remove_client(connection_id)

    async def add_client(self, connection_id, prebuffer_bytes=0):
        async with self.lock:
            q = ByteBudgetQueue(max_bytes=CSO_OUTPUT_CLIENT_QUEUE_MAX_BYTES)
            if prebuffer_bytes > 0 and self.history:
                total = 0
                items = []
                for chunk in reversed(self.history):
                    items.append(chunk)
                    total += len(chunk)
                    if total >= prebuffer_bytes:
                        break
                for chunk in reversed(items):
                    await q.put_drop_oldest(chunk)
            self.clients[connection_id] = q
            self.client_drop_state.pop(connection_id, None)
            self.client_last_touch[connection_id] = time.time()
            client_count = len(self.clients)
        logger.info(
            "CSO output client connected channel=%s output_key=%s connection_id=%s clients=%s policy=(%s)",
            self.channel_id,
            self.key,
            connection_id,
            client_count,
            _policy_log_label(self.policy),
        )
        return q

    async def touch_client(self, connection_id):
        async with self.lock:
            if connection_id in self.clients:
                self.client_last_touch[connection_id] = time.time()
                self.last_activity = time.time()

    async def prune_idle_clients(self, now_ts=None):
        now_value = float(now_ts if now_ts is not None else time.time())
        stale_ids = []
        async with self.lock:
            for connection_id in list(self.clients.keys()):
                last_touch = float(self.client_last_touch.get(connection_id, 0.0) or 0.0)
                if (now_value - last_touch) >= float(CSO_OUTPUT_CLIENT_STALE_SECONDS):
                    stale_ids.append(connection_id)
        for connection_id in stale_ids:
            logger.warning(
                "CSO output dropping stale client channel=%s output_key=%s connection_id=%s reason=idle_prune stale_seconds=%s",
                self.channel_id,
                self.key,
                connection_id,
                int(CSO_OUTPUT_CLIENT_STALE_SECONDS),
            )
            await self.remove_client(connection_id)

    async def remove_client(self, connection_id):
        removed_queue = None
        async with self.lock:
            removed_queue = self.clients.pop(connection_id, None)
            self.client_drop_state.pop(connection_id, None)
            self.client_last_touch.pop(connection_id, None)
            remaining = len(self.clients)
        if removed_queue is not None:
            await removed_queue.put_eof()
        logger.info(
            "CSO output client disconnected channel=%s output_key=%s connection_id=%s clients=%s policy=(%s)",
            self.channel_id,
            self.key,
            connection_id,
            remaining,
            _policy_log_label(self.policy),
        )
        if remaining == 0:
            await self.stop(force=True)
        return remaining

    async def drop_backpressured_clients(self, min_elapsed_seconds=0.5, min_drop_count=3):
        now = time.time()
        candidates = []
        async with self.lock:
            for connection_id, state in list(self.client_drop_state.items()):
                if str(connection_id) not in self.clients:
                    continue
                elapsed = float(now - float(state.get("first_ts") or now))
                count = int(state.get("count") or 0)
                if elapsed >= float(min_elapsed_seconds) and count >= int(min_drop_count):
                    candidates.append(connection_id)
        removed = 0
        for connection_id in candidates:
            logger.warning(
                "CSO output preemptively dropping backpressured client channel=%s output_key=%s connection_id=%s reason=capacity_handover elapsed_threshold=%.2fs count_threshold=%s",
                self.channel_id,
                self.key,
                connection_id,
                float(min_elapsed_seconds),
                int(min_drop_count),
            )
            await self.remove_client(connection_id)
            removed += 1
        return removed

    async def stop(self, force=False):
        async with self.lock:
            if not self.running and not self.process and not self.clients:
                return
            if not force and self.clients:
                return
            self.running = False
            process = self.process
            self.process = None
            ingest_queue = self.ingest_queue
            self.ingest_queue = None
            client_count = len(self.clients)
        logger.info(
            "Stopping CSO output channel=%s output_key=%s clients=%s force=%s policy=(%s)",
            self.channel_id,
            self.key,
            client_count,
            force,
            _policy_log_label(self.policy),
        )
        return_code = None
        try:
            if ingest_queue is not None:
                await self.ingest_session.remove_subscriber(self.key)
        except Exception:
            pass
        if process:
            try:
                process.terminate()
                return_code = await _wait_process_exit_with_timeout(process, timeout_seconds=2.0)
            except Exception:
                try:
                    process.kill()
                    return_code = await _wait_process_exit_with_timeout(process, timeout_seconds=2.0)
                except Exception:
                    logger.warning(
                        "CSO output process did not exit after kill channel=%s output_key=%s",
                        self.channel_id,
                        self.key,
                    )
                    pass
        logger.info(
            "CSO output stopped channel=%s output_key=%s return_code=%s policy=(%s)",
            self.channel_id,
            self.key,
            return_code,
            _policy_log_label(self.policy),
        )
        async with self.lock:
            for q in self.clients.values():
                await q.put_eof()
            self.client_drop_state.clear()
            self.client_last_touch.clear()


class CsoHlsOutputSession:
    def __init__(self, key, channel_id, policy, ingest_session, cache_root_dir):
        self.key = key
        self.channel_id = channel_id
        self.policy = policy
        self.ingest_session = ingest_session
        self.cache_root_dir = Path(cache_root_dir)
        self.output_dir = self.cache_root_dir / self.key
        self.playlist_path = self.output_dir / "index.m3u8"
        self.process = None
        self.write_task = None
        self.stderr_task = None
        self.wait_task = None
        self.running = False
        self.lock = asyncio.Lock()
        self.last_activity = time.time()
        self.last_error = None
        self.ingest_queue = None
        self._recent_ffmpeg_stderr = deque(maxlen=30)
        self.clients = {}

    def _ffmpeg_error_summary(self):
        lines = [line for line in self._recent_ffmpeg_stderr if line]
        if not lines:
            return ""
        error_lines = [
            line
            for line in lines
            if any(token in line.lower() for token in ("error", "invalid", "failed", "could not", "unsupported"))
        ]
        selected = error_lines[-3:] if error_lines else lines[-3:]
        return " | ".join(selected)

    async def _prepare_output_dir(self):
        if self.output_dir.exists():
            await asyncio.to_thread(shutil.rmtree, self.output_dir, True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def start(self):
        async with self.lock:
            if self.running:
                return
            await self.ingest_session.start()
            if not self.ingest_session.running:
                self.last_error = self.ingest_session.last_error or "ingest_not_running"
                return

            await self._prepare_output_dir()
            self.ingest_queue = await self.ingest_session.add_subscriber(self.key, prebuffer_bytes=256 * 1024)
            command = CsoOutputFfmpegCommandBuilder(self.policy).build_hls_output_command(self.output_dir)
            logger.info(
                "Starting CSO HLS output channel=%s output_key=%s policy=(%s) command=%s",
                self.channel_id,
                self.key,
                _policy_log_label(self.policy),
                command,
            )
            self.process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            self.running = True
            self.last_error = None
            self.last_activity = time.time()
            self.write_task = asyncio.create_task(self._write_loop())
            self.stderr_task = asyncio.create_task(self._stderr_loop())
            self.wait_task = asyncio.create_task(self._wait_loop())

    async def _write_loop(self):
        try:
            while self.running and self.process and self.process.stdin and self.ingest_queue:
                chunk = await self.ingest_queue.get()
                if chunk is None:
                    break
                try:
                    self.process.stdin.write(chunk)
                    await self.process.stdin.drain()
                except Exception:
                    break
        finally:
            try:
                if self.process and self.process.stdin:
                    self.process.stdin.close()
            except Exception:
                pass

    async def _stderr_loop(self):
        if not self.process:
            return
        text_buffer = ""
        while True:
            try:
                chunk = await self.process.stderr.read(4096)
            except Exception:
                break
            if not chunk:
                break
            text_buffer += chunk.decode("utf-8", errors="replace")
            lines = re.split(r"[\r\n]+", text_buffer)
            text_buffer = lines.pop() if lines else ""
            for rendered in lines:
                rendered = rendered.strip()
                if not rendered:
                    continue
                self._recent_ffmpeg_stderr.append(rendered)
                if enable_cso_command_debug_logging:
                    logger.info("CSO HLS output ffmpeg[%s][%s]: %s", self.channel_id, self.key, rendered)
        rendered = text_buffer.strip()
        if rendered:
            self._recent_ffmpeg_stderr.append(rendered)
            if enable_cso_command_debug_logging:
                logger.info("CSO HLS output ffmpeg[%s][%s]: %s", self.channel_id, self.key, rendered)

    async def _wait_loop(self):
        return_code = None
        try:
            if self.process:
                return_code = await self.process.wait()
        except Exception:
            return_code = None
        finally:
            async with self.lock:
                client_count = len(self.clients)
                still_running = bool(self.running)
            if still_running and client_count > 0:
                self.last_error = "output_reader_ended"
                logger.warning(
                    "CSO HLS output ended unexpectedly channel=%s output_key=%s return_code=%s stderr=%s",
                    self.channel_id,
                    self.key,
                    return_code,
                    self._ffmpeg_error_summary() or "n/a",
                )
            await self.stop(force=True)

    async def add_client(self, connection_id, on_disconnect=None):
        async with self.lock:
            key = str(connection_id)
            existed = key in self.clients
            previous = self.clients.get(key) or {}
            self.clients[key] = {
                "last_touch": time.time(),
                "on_disconnect": on_disconnect if on_disconnect is not None else previous.get("on_disconnect"),
            }
            client_count = len(self.clients)
            self.last_activity = time.time()
        if not existed:
            logger.info(
                "CSO HLS output client connected channel=%s output_key=%s connection_id=%s clients=%s policy=(%s)",
                self.channel_id,
                self.key,
                connection_id,
                client_count,
                _policy_log_label(self.policy),
            )
        return not existed

    async def has_client(self, connection_id):
        async with self.lock:
            return str(connection_id) in self.clients

    async def _invoke_disconnect_hook(self, connection_id, disconnect_hook):
        if not callable(disconnect_hook):
            return
        try:
            await disconnect_hook(str(connection_id))
        except Exception as exc:
            logger.warning(
                "CSO HLS output disconnect hook failed channel=%s output_key=%s connection_id=%s error=%s",
                self.channel_id,
                self.key,
                connection_id,
                exc,
            )

    async def touch_client(self, connection_id):
        async with self.lock:
            key = str(connection_id)
            entry = self.clients.get(key)
            if not isinstance(entry, dict):
                entry = {"last_touch": time.time(), "on_disconnect": None}
                self.clients[key] = entry
            entry["last_touch"] = time.time()
            self.last_activity = time.time()

    async def remove_client(self, connection_id):
        disconnect_hook = None
        async with self.lock:
            removed = self.clients.pop(str(connection_id), None)
            if isinstance(removed, dict):
                disconnect_hook = removed.get("on_disconnect")
            remaining = len(self.clients)
        await self._invoke_disconnect_hook(connection_id, disconnect_hook)
        logger.info(
            "CSO HLS output client disconnected channel=%s output_key=%s connection_id=%s clients=%s policy=(%s)",
            self.channel_id,
            self.key,
            connection_id,
            remaining,
            _policy_log_label(self.policy),
        )
        if remaining == 0:
            await self.stop(force=True)
        return remaining

    async def prune_idle_clients(self, now_ts=None):
        now_value = float(now_ts if now_ts is not None else time.time())
        stale_ids = []
        async with self.lock:
            for connection_id, entry in list(self.clients.items()):
                last_touch = 0.0
                if isinstance(entry, dict):
                    last_touch = float(entry.get("last_touch") or 0.0)
                if (now_value - last_touch) >= float(CSO_HLS_CLIENT_IDLE_SECONDS):
                    stale_ids.append(connection_id)
        for connection_id in stale_ids:
            logger.info(
                "CSO HLS output dropping idle client channel=%s output_key=%s connection_id=%s idle_seconds=%s",
                self.channel_id,
                self.key,
                connection_id,
                int(CSO_HLS_CLIENT_IDLE_SECONDS),
            )
            await self.remove_client(connection_id)

    async def read_playlist_text(self):
        if not self.playlist_path.exists():
            return None
        return await asyncio.to_thread(self.playlist_path.read_text, "utf-8")

    async def read_segment_bytes(self, segment_name):
        name = str(segment_name or "").strip()
        if not name or not _SAFE_HLS_SEGMENT_RE.match(name):
            return None
        segment_path = (self.output_dir / name).resolve()
        if not str(segment_path).startswith(str(self.output_dir.resolve())):
            return None
        if not segment_path.exists() or not segment_path.is_file():
            return None
        return await asyncio.to_thread(segment_path.read_bytes)

    async def stop(self, force=False):
        async with self.lock:
            if not self.running and not self.process and not self.clients:
                return
            if not force and self.clients:
                return
            self.running = False
            process = self.process
            self.process = None
            ingest_queue = self.ingest_queue
            self.ingest_queue = None
            client_count = len(self.clients)
            disconnected_clients = list(self.clients.items())
            self.clients = {}
        logger.info(
            "Stopping CSO HLS output channel=%s output_key=%s clients=%s force=%s policy=(%s)",
            self.channel_id,
            self.key,
            client_count,
            force,
            _policy_log_label(self.policy),
        )
        for disconnected_id, disconnected_entry in disconnected_clients:
            disconnect_hook = None
            if isinstance(disconnected_entry, dict):
                disconnect_hook = disconnected_entry.get("on_disconnect")
            await self._invoke_disconnect_hook(disconnected_id, disconnect_hook)
            logger.info(
                "CSO HLS output client disconnected channel=%s output_key=%s connection_id=%s clients=%s policy=(%s)",
                self.channel_id,
                self.key,
                disconnected_id,
                0,
                _policy_log_label(self.policy),
            )
        try:
            if ingest_queue is not None:
                await self.ingest_session.remove_subscriber(self.key)
        except Exception:
            pass
        if process:
            try:
                process.terminate()
                await _wait_process_exit_with_timeout(process, timeout_seconds=2.0)
            except Exception:
                try:
                    process.kill()
                    await _wait_process_exit_with_timeout(process, timeout_seconds=2.0)
                except Exception:
                    logger.warning(
                        "CSO HLS output process did not exit after kill channel=%s output_key=%s",
                        self.channel_id,
                        self.key,
                    )
                    pass
        if self.output_dir.exists():
            await asyncio.to_thread(shutil.rmtree, self.output_dir, True)


class _SessionMap:
    def __init__(self):
        self.sessions = {}
        self.lock = asyncio.Lock()

    async def get_or_create(self, key, factory):
        async with self.lock:
            session = self.sessions.get(key)
            if session and (session.running or session.process):
                return session
            session = factory()
            self.sessions[key] = session
            return session

    async def cleanup_idle_streams(self, idle_timeout=300):
        now = time.time()
        async with self.lock:
            items = list(self.sessions.items())
        for key, session in items:
            prune_hook = getattr(session, "prune_idle_clients", None)
            if callable(prune_hook):
                try:
                    await prune_hook(now)
                except Exception as exc:
                    logger.warning("CSO session idle-prune failed key=%s error=%s", key, exc)
            if session.running and (now - session.last_activity) < idle_timeout:
                continue
            async with session.lock:
                has_subscribers = bool(getattr(session, "subscribers", None) or getattr(session, "clients", None))
                running = bool(session.running)
            if running and has_subscribers:
                continue
            try:
                await session.stop(force=True)
            except asyncio.CancelledError:
                # Session is being torn down; continue cleanup for other sessions.
                pass
            except Exception as exc:
                logger.warning("CSO session cleanup failed key=%s error=%s", key, exc)
            async with self.lock:
                if self.sessions.get(key) is session:
                    self.sessions.pop(key, None)


class CsoRuntimeManager:
    def __init__(self):
        self.ingest = _SessionMap()
        self.output = _SessionMap()

    async def get_or_create_ingest(self, key, factory):
        return await self.ingest.get_or_create(key, factory)

    async def get_or_create_output(self, key, factory):
        return await self.output.get_or_create(key, factory)

    async def cleanup_idle_streams(self, idle_timeout=300):
        await self.output.cleanup_idle_streams(idle_timeout=idle_timeout)
        await self.ingest.cleanup_idle_streams(idle_timeout=idle_timeout)

    async def get_output_session(self, key):
        async with self.output.lock:
            return self.output.sessions.get(key)

    async def has_active_ingest_for_channel(self, channel_id):
        prefix = f"cso-ingest-{int(channel_id)}"
        async with self.ingest.lock:
            session = self.ingest.sessions.get(prefix)
            if not session:
                return False
            return bool(session.running and session.process)


cso_session_manager = CsoRuntimeManager()


def _safe_int(value):
    try:
        return int(value)
    except Exception:
        return None


def _increment_external_count(external_counts, key):
    if not key:
        return
    external_counts[key] = int(external_counts.get(key) or 0) + 1


def is_internal_cso_activity(endpoint: str, display_url: str = "") -> bool:
    endpoint_value = str(endpoint or "")
    display_url_value = str(display_url or "").lower()
    if "/tic-api/cso/channel/" in endpoint_value or "/tic-api/cso/channel_stream/" in endpoint_value:
        return True
    if endpoint_value.startswith("/tic-tvh/") and "tic-cso-" in display_url_value:
        return True
    return False


async def reconcile_cso_capacity_with_tvh_channels(channel_ids, activity_sessions=None):
    external_counts = {}
    fallback_channel_ids = set()

    for value in channel_ids or []:
        parsed = _safe_int(value)
        if parsed:
            fallback_channel_ids.add(parsed)

    for session in activity_sessions or []:
        if not isinstance(session, dict):
            continue
        endpoint = str(session.get("endpoint") or "")
        display_url = str(session.get("display_url") or "").lower()
        # CSO endpoint usage is already tracked via in-process allocations.
        # TVH subscriptions against CSO mux should not count as additional external usage.
        if is_internal_cso_activity(endpoint, display_url):
            continue

        xc_account_id = _safe_int(session.get("xc_account_id"))
        playlist_id = _safe_int(session.get("playlist_id"))
        source_id = _safe_int(session.get("source_id"))
        if xc_account_id:
            _increment_external_count(external_counts, f"xc:{xc_account_id}")
            continue
        if playlist_id:
            _increment_external_count(external_counts, f"playlist:{playlist_id}")
            continue
        if source_id:
            _increment_external_count(external_counts, f"source:{source_id}")
            continue

        channel_id = _safe_int(session.get("channel_id"))
        if channel_id:
            fallback_channel_ids.add(channel_id)

    unresolved = []
    for channel_id in sorted(fallback_channel_ids):
        if not await cso_session_manager.has_active_ingest_for_channel(channel_id):
            unresolved.append(channel_id)
    if not unresolved:
        await cso_capacity_registry.set_external_counts(external_counts)
        return

    async with Session() as session:
        result = await session.execute(
            select(Channel)
            .options(
                joinedload(Channel.sources).joinedload(ChannelSource.playlist),
                joinedload(Channel.sources).joinedload(ChannelSource.xc_account),
            )
            .where(Channel.id.in_(unresolved))
        )
        channels = result.scalars().unique().all()
        for channel in channels:
            candidates = sorted(
                list(channel.sources or []),
                key=lambda item: _priority_value(getattr(item, "priority", 0)),
                reverse=True,
            )
            for source in candidates:
                playlist = getattr(source, "playlist", None)
                if playlist is not None and not bool(getattr(playlist, "enabled", False)):
                    continue
                stream_url = (getattr(source, "playlist_stream_url", None) or "").strip()
                if not stream_url:
                    continue
                key = source_capacity_key(source)
                _increment_external_count(external_counts, key)
                break
    await cso_capacity_registry.set_external_counts(external_counts)


async def _discover_hls_variants(url):
    parsed = urlparse(str(url or ""))
    if not parsed.path.lower().endswith(".m3u8"):
        return []
    timeout = aiohttp.ClientTimeout(total=6)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as client:
            async with client.get(url, allow_redirects=True) as response:
                if response.status >= 400:
                    return []
                payload = await response.text()
    except Exception:
        return []
    return _parse_hls_master_variants(payload)


def _parse_hls_master_variants(payload):
    lines = [line.strip() for line in str(payload or "").splitlines() if line.strip()]
    variants = []
    pending_bandwidth = None
    pending_width = 0
    pending_height = 0
    for line in lines:
        if line.startswith("#EXT-X-STREAM-INF:"):
            bandwidth_match = _HLS_BANDWIDTH_RE.search(line)
            pending_bandwidth = int(bandwidth_match.group(1)) if bandwidth_match else 0
            resolution_match = _HLS_RESOLUTION_RE.search(line)
            pending_width = int(resolution_match.group(1)) if resolution_match else 0
            pending_height = int(resolution_match.group(2)) if resolution_match else 0
            continue
        if line.startswith("#"):
            continue
        if pending_bandwidth is None:
            # Not a master playlist variant entry.
            continue
        variants.append(
            {
                "bandwidth": pending_bandwidth,
                "width": pending_width,
                "height": pending_height,
                "program_index": len(variants),
            }
        )
        pending_bandwidth = None
        pending_width = 0
        pending_height = 0
    variants.sort(key=lambda item: int(item.get("bandwidth") or 0))
    return variants


async def resolve_channel_for_stream(channel_id):
    """Return the channel model for stream playback if it exists.

    This is the single channel lookup for CSO playback requests. Callers should
    reuse the returned model for profile resolution, activity metadata, and
    session subscription setup to avoid duplicate database queries.
    """
    async with Session() as session:
        result = await session.execute(
            select(Channel)
            .options(
                joinedload(Channel.sources).joinedload(ChannelSource.playlist),
                joinedload(Channel.sources).joinedload(ChannelSource.xc_account),
            )
            .where(Channel.id == channel_id)
        )
        return result.scalars().unique().one_or_none()


async def _resolve_username_for_stream_key(config, stream_key):
    key = str(stream_key or "").strip()
    if not key:
        return None
    try:
        tvh_stream_user = await config.get_tvh_stream_user()
        if tvh_stream_user and str(tvh_stream_user.get("stream_key") or "") == key:
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

    sources = list(channel.sources or [])
    if not sources:
        return None, "No available stream source for this channel", 503

    policy = generate_cso_policy_from_profile(config, profile)
    ingest_key = f"cso-ingest-{channel.id}"
    output_session_key = f"cso-hls-output-{channel.id}-{profile}"
    capacity_owner_key = f"cso-channel-{channel.id}"
    username = await _resolve_username_for_stream_key(config, stream_key)
    ingest_user_agent = _resolve_cso_ingest_user_agent(config, sources[0] if sources else None)

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
    await ingest_session.start()
    if not ingest_session.running:
        reason = ingest_session.last_error or "no_available_source"
        await emit_channel_stream_event(
            channel_id=channel.id,
            source_id=getattr(sources[0], "id", None) if sources else None,
            playlist_id=getattr(sources[0], "playlist_id", None) if sources else None,
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
            source_id=getattr(ingest_session.current_source, "id", None),
            playlist_id=getattr(ingest_session.current_source, "playlist_id", None),
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
            source_id=getattr(ingest_session.current_source, "id", None),
            playlist_id=getattr(ingest_session.current_source, "playlist_id", None),
            session_id=output_session_key,
            event_type="session_start",
            severity="info",
            details={
                "profile": profile,
                "connection_id": connection_id,
                **_source_event_context(
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

    playlist = getattr(source, "playlist", None)
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return None, "Stream playlist is disabled", 404
    stream_url = (getattr(source, "playlist_stream_url", None) or "").strip()
    if not stream_url:
        return None, "No available stream source for this channel", 503

    source_id = int(getattr(source, "id", 0) or 0)
    channel_id = int(getattr(source, "channel_id", 0) or 0) or source_id
    sources = [source]
    policy = generate_cso_policy_from_profile(config, profile)
    ingest_key = f"cso-source-ingest-{source_id}"
    output_session_key = f"cso-source-hls-output-{source_id}-{profile}"
    capacity_owner_key = f"cso-source-{source_id}"
    username = await _resolve_username_for_stream_key(config, stream_key)
    ingest_user_agent = _resolve_cso_ingest_user_agent(config, source)

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
            source_id=source_id,
            playlist_id=getattr(source, "playlist_id", None),
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
            source_id=source_id,
            playlist_id=getattr(source, "playlist_id", None),
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
            source_id=source_id,
            playlist_id=getattr(source, "playlist_id", None),
            session_id=output_session_key,
            event_type="session_start",
            severity="info",
            details={
                "profile": profile,
                "connection_id": connection_id,
                **_source_event_context(
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
        return None, None, "Channel not found", 404
    if not channel.enabled:
        return None, None, "Channel is disabled", 404

    sources = list(channel.sources or [])
    if not sources:
        await emit_channel_stream_event(
            channel_id=channel.id,
            session_id=connection_id,
            event_type="capacity_blocked",
            severity="warning",
            details={"reason": "no_eligible_source"},
        )
        return None, None, "No available stream source for this channel", 503

    policy = generate_cso_policy_from_profile(config, profile)
    ingest_key = f"cso-ingest-{channel.id}"
    output_session_key = f"cso-output-{channel.id}-{profile}"
    capacity_owner_key = f"cso-channel-{channel.id}"
    username = await _resolve_username_for_stream_key(config, stream_key)
    ingest_user_agent = _resolve_cso_ingest_user_agent(config, sources[0] if sources else None)

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
        return None, None, message, 503

    def _output_factory():
        return CsoOutputSession(
            output_session_key,
            channel.id,
            policy,
            ingest_session,
        )

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
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
        return None, None, "Channel unavailable because output pipeline could not be started", 503

    queue = await output_session.add_client(connection_id, prebuffer_bytes=prebuffer_bytes)
    content_type = policy_content_type(policy)
    await emit_channel_stream_event(
        channel_id=channel.id,
        source_id=getattr(ingest_session.current_source, "id", None),
        playlist_id=getattr(ingest_session.current_source, "playlist_id", None),
        session_id=output_session_key,
        event_type="session_start",
        severity="info",
        details={
            "profile": profile,
            "connection_id": connection_id,
            **_source_event_context(
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
                        raise CsoOutputReaderEnded("output_reader_ended")
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
                source_id=getattr(ingest_session.current_source, "id", None),
                playlist_id=getattr(ingest_session.current_source, "playlist_id", None),
                session_id=output_session_key,
                event_type="session_end",
                severity="info",
                details={
                    "profile": profile,
                    "connection_id": connection_id,
                    **_source_event_context(
                        ingest_session.current_source,
                        source_url=getattr(ingest_session, "current_source_url", None),
                    ),
                },
            )

    return _generator(), content_type, None, 200


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
        return None, None, "Source not found", 404

    playlist = getattr(source, "playlist", None)
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return None, None, "Source playlist is disabled", 404

    stream_url = (getattr(source, "playlist_stream_url", None) or "").strip()
    if not stream_url:
        return None, None, "No available stream source for this channel", 503

    source_id = int(getattr(source, "id", 0) or 0)
    channel_id = int(getattr(source, "channel_id", 0) or 0) or source_id
    sources = [source]

    policy = generate_cso_policy_from_profile(config, profile)
    ingest_key = f"cso-source-ingest-{source_id}"
    output_session_key = f"cso-source-output-{source_id}-{profile}"
    capacity_owner_key = f"cso-source-{source_id}"
    username = await _resolve_username_for_stream_key(config, stream_key)
    ingest_user_agent = _resolve_cso_ingest_user_agent(config, source)

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
            source_id=source_id,
            playlist_id=getattr(source, "playlist_id", None),
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
        return None, None, message, 503

    def _output_factory():
        return CsoOutputSession(
            output_session_key,
            channel_id,
            policy,
            ingest_session,
        )

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
    await output_session.start()
    if not output_session.running:
        reason = output_session.last_error or "output_not_running"
        await emit_channel_stream_event(
            channel_id=channel_id,
            source_id=source_id,
            playlist_id=getattr(source, "playlist_id", None),
            session_id=output_session_key,
            event_type="playback_unavailable",
            severity="warning",
            details={"reason": reason, "profile": profile},
        )
        return None, None, "Channel unavailable because output pipeline could not be started", 503

    queue = await output_session.add_client(connection_id, prebuffer_bytes=prebuffer_bytes)
    content_type = policy_content_type(policy)
    await emit_channel_stream_event(
        channel_id=channel_id,
        source_id=source_id,
        playlist_id=getattr(source, "playlist_id", None),
        session_id=output_session_key,
        event_type="session_start",
        severity="info",
        details={
            "profile": profile,
            "connection_id": connection_id,
            **_source_event_context(
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
                        raise CsoOutputReaderEnded("output_reader_ended")
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
                source_id=source_id,
                playlist_id=getattr(source, "playlist_id", None),
                session_id=output_session_key,
                event_type="session_end",
                severity="info",
                details={
                    "profile": profile,
                    "connection_id": connection_id,
                    **_source_event_context(
                        ingest_session.current_source,
                        source_url=getattr(ingest_session, "current_source_url", None),
                    ),
                },
            )

    return _generator(), content_type, None, 200


async def disconnect_output_client(output_session_key, connection_id):
    """Force-disconnect a specific client from an output session if it still exists."""
    logging.getLogger("cso").info(
        "Request to disconnect CSO client key=%s connection_id=%s",
        output_session_key,
        connection_id,
    )
    if not output_session_key or not connection_id:
        return
    session = await cso_session_manager.get_output_session(output_session_key)
    if not session:
        return
    try:
        await session.remove_client(connection_id)
    except Exception as exc:
        logging.getLogger("cso").error(
            "Error disconnecting CSO client key=%s connection_id=%s error=%s",
            output_session_key,
            connection_id,
            exc,
            exc_info=True,
        )


async def preempt_backpressured_clients_for_capacity_key(capacity_key_name, min_elapsed_seconds=0.5, min_drop_count=3):
    if not capacity_key_name:
        return 0

    removed = 0
    async with cso_session_manager.output.lock:
        sessions = list(cso_session_manager.output.sessions.values())

    for session in sessions:
        try:
            source = getattr(session.ingest_session, "current_source", None)
            if source is None:
                continue
            if source_capacity_key(source) != str(capacity_key_name):
                continue
            removed += int(
                await session.drop_backpressured_clients(
                    min_elapsed_seconds=min_elapsed_seconds,
                    min_drop_count=min_drop_count,
                )
                or 0
            )
        except Exception as exc:
            logger.error(
                "Error preempting backpressured clients key=%s output_key=%s error=%s",
                capacity_key_name,
                getattr(session, "key", None),
                exc,
                exc_info=True,
            )
    return removed


def build_cso_stream_query(profile=None, connection_id=None, stream_key=None, username=None):
    query = []
    if profile:
        query.append(("profile", profile))
    if connection_id:
        query.append(("connection_id", connection_id))
    if stream_key:
        query.append(("stream_key", stream_key))
    if username:
        query.append(("username", username))
    if not query:
        return ""
    return urlencode(query)
