#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import base64
import hashlib
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
import aiofiles
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
)
from backend.stream_profiles import generate_cso_policy_from_profile, resolve_cso_profile_name
from backend.users import get_user_by_stream_key
from backend.config import (
    enable_cso_ingest_command_debug_logging,
    enable_cso_output_command_debug_logging,
    enable_cso_slate_command_debug_logging,
)
from backend.datetime_utils import utc_now_naive
from backend.http_headers import parse_headers_json, sanitise_headers
from backend.source_media import load_source_media_shape, persist_source_media_shape
from backend.utils import clean_key, clean_text, convert_to_int
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
CSO_OUTPUT_CLIENT_STALE_SECONDS_TVH = 20.0
CSO_HLS_SEGMENT_SECONDS = 3
CSO_HLS_LIST_SIZE = 5
CSO_HLS_CLIENT_IDLE_SECONDS = max(10, int(CSO_HLS_SEGMENT_SECONDS) * 3)
CSO_OUTPUT_CLIENT_QUEUE_MAX_BYTES = 90_000_000
CSO_INGEST_SUBSCRIBER_QUEUE_MAX_BYTES = 90_000_000
CSO_CONSUMER_PROGRESS_LOG_INTERVAL_SECONDS = 10
CSO_OUTPUT_SLATE_POLL_INTERVAL_SECONDS = 0.25
CSO_OUTPUT_PIPE_POLL_INTERVAL_SECONDS = 0.25
MPEGTS_PACKET_SIZE_BYTES = 188
MPEGTS_CHUNK_BYTES = MPEGTS_PACKET_SIZE_BYTES * 87
CSO_UNAVAILABLE_REASON_DURATIONS_SECONDS = {
    "default": 10,
    "capacity_blocked": 10,
    "playback_unavailable": 3,
    "startup_pending": 30,
}
CSO_UNAVAILABLE_SLATE_CACHE_TTL_SECONDS = 30 * 60
CSO_UNAVAILABLE_SLATE_CACHE_VERSION = "v3"
CSO_UNAVAILABLE_SLATE_MESSAGES = {
    "capacity_blocked": {
        "title": "Channel Temporarily Unavailable",
        "subtitle": "Source connection limit reached. Please try again shortly.",
    },
    "playback_unavailable": {
        "title": "Playback Issue Detected",
        "subtitle": "Unable to start playback right now. Please try again shortly.",
    },
}
CSO_UNAVAILABLE_SHOW_SLATE = True
_FFMPEG_SPEED_RE = re.compile(r"speed=\s*([0-9.]+)x")
_HTTP_STATUS_CODE_RE = re.compile(r"\b([45]\d{2})\b")
_HLS_BANDWIDTH_RE = re.compile(r"BANDWIDTH=(\d+)")
_HLS_RESOLUTION_RE = re.compile(r"RESOLUTION=(\d+)x(\d+)")
_HLS_STARTUP_RAMP_INTERVAL_SECONDS = 4
_SAFE_HLS_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_FFMPEG_INPUT_RE = re.compile(r"^Input #\d+,\s*([^,]+)")
_FFMPEG_VIDEO_STREAM_RE = re.compile(
    r"Stream #\d+:\d+(?:\[[^\]]+\])?: Video:\s*([a-zA-Z0-9_]+)(?:\s*\(([^)]*)\))?,\s*([^,]+),\s*(\d+)x(\d+)"
)
_FFMPEG_AUDIO_STREAM_RE = re.compile(
    r"Stream #\d+:\d+(?:\[[^\]]+\])?: Audio:\s*([a-zA-Z0-9_]+)(?:\s*\(([^)]*)\))?,\s*(\d+)\s*Hz,\s*([^,]+)"
)
_FFMPEG_FPS_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*fps")

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


async def _wait_process_exit_with_timeout(process, timeout_seconds=2.0):
    if not process:
        return None
    return await asyncio.wait_for(process.wait(), timeout=float(timeout_seconds))


def _process_is_running(pid):
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except Exception:
        return True
    return True


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

    async def clear(self):
        async with self._cond:
            self._items.clear()
            self._bytes = 0


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


def _resolve_cso_output_policy(policy, use_slate_as_input=False):
    resolved = dict(policy or {})
    if use_slate_as_input:
        resolved["output_mode"] = "force_remux"
        resolved["container"] = "mpegts"
        resolved["video_codec"] = "copy"
        resolved["audio_codec"] = "copy"
        resolved["subtitle_mode"] = "copy"
    return resolved


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
    stream_name = clean_text(getattr(source, "playlist_stream_name", ""))
    playlist_name = clean_text(getattr(playlist, "name", ""))
    payload = {
        "source_id": getattr(source, "id", None),
        "playlist_id": getattr(source, "playlist_id", None),
        "playlist_name": playlist_name or None,
        "stream_name": stream_name or None,
        "source_priority": convert_to_int(getattr(source, "priority", 0), 0),
    }
    if source_url:
        payload["source_url"] = source_url
    return payload


@dataclass
class CsoStartResult:
    success: bool
    reason: str | None = None


@dataclass
class CsoStreamPlan:
    generator: object | None
    content_type: str | None
    error_message: str | None
    status_code: int
    cutoff_seconds: int | None = None
    final_status_code: int | None = None


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
            external_counts = {}
            for key, count in (counts or {}).items():
                try:
                    value = int(count or 0)
                except Exception:
                    value = 0
                if value > 0:
                    external_counts[str(key)] = value
            self._external_counts = external_counts

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
    resolved_url = clean_text(source_url)
    if not resolved_url:
        return resolved_url
    if LOCAL_PROXY_HOST_PLACEHOLDER in resolved_url:
        resolved_url = resolved_url.replace(LOCAL_PROXY_HOST_PLACEHOLDER, base_url)
    # Only unwrap this same instance's local TIC HLS proxy URL.
    for _ in range(4):
        next_url = _unwrap_local_tic_hls_proxy_url(resolved_url, instance_id=instance_id)
        if not next_url:
            break
        if next_url == resolved_url:
            break
        resolved_url = next_url

    if is_local_hls_proxy_url(resolved_url, instance_id=instance_id):
        # CSO ingest must never route through this same instance's HLS proxy.
        # If we still have a local proxy URL at this point, treat it as unresolved.
        logger.warning(
            "CSO source URL still points to local HLS proxy after unwrapping; skipping source url=%s", resolved_url
        )
        return ""
    elif stream_key and "/tic-hls-proxy/" in resolved_url and "stream_key=" not in resolved_url:
        resolved_url = append_stream_key(resolved_url, stream_key=stream_key, username=username)
    return resolved_url


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
    target = clean_key(name)
    if not target:
        return None
    for key, value in (headers or {}).items():
        if clean_key(key) == target:
            return clean_text(value) or None
    return None


def _format_ffmpeg_headers_arg(headers):
    lines = []
    for key, value in (headers or {}).items():
        key_name = clean_key(key)
        if key_name in {"user-agent", "referer"}:
            continue
        text = clean_text(value)
        if not text:
            continue
        lines.append(f"{key}: {text}")
    if not lines:
        return None
    # FFmpeg expects CRLF-separated request headers.
    return "\r\n".join(lines) + "\r\n"


def _build_ingest_ffmpeg_command(source_url, program_index=0, user_agent=None, request_headers=None):
    map_program = max(0, int(program_index or 0))
    is_hls_input = (urlparse(source_url or "").path or "").lower().endswith(".m3u8")
    header_values = sanitise_headers(request_headers)
    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "info",
    ]
    if enable_cso_ingest_command_debug_logging:
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
    user_agent_value = clean_text(user_agent) or _header_value(header_values, "User-Agent")
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
        "-mpegts_flags",
        "+resend_headers",
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
    playlist_user_agent = clean_text(getattr(playlist, "user_agent", ""))
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
            candidate = clean_text(item.get("value") or item.get("name"))
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


def _effective_hls_runtime_policy(policy):
    return dict(policy or {})


def should_allow_unavailable_slate(profile_name, channel=None):
    channel_forced_cso = bool(getattr(channel, "cso_enabled", False)) if channel is not None else False
    # For TVH profile traffic, return hard failures unless the channel is explicitly forced through CSO.
    if profile_name == "tvh" and not channel_forced_cso:
        return False
    return True


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


class CsoOutputFfmpegCommandBuilder:
    """Curated FFmpeg command builder for CSO output pipelines."""

    def __init__(self, policy):
        self.policy = policy or {}

    def _base(self):
        command = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "info" if enable_cso_output_command_debug_logging else "warning",
        ]
        if enable_cso_output_command_debug_logging:
            command += ["-stats"]
        # TODO: Review analyzeduration and probesize for all the ffmpeg commands in this file
        command += [
            "-fflags",
            "+nobuffer",
            "-flags",
            "low_delay",
            "-probesize",
            "524288",
            "-analyzeduration",
            "1000000",
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
        codec = video_codec or ""
        return {
            "h264": "libx264",
            "h265": "libx265",
            "vp8": "libvpx",
        }.get(codec, "libx264")

    @staticmethod
    def vaapi_encoder_for_codec(video_codec: str) -> str:
        codec = video_codec or ""
        return "hevc_vaapi" if codec == "h265" else "h264_vaapi"

    @staticmethod
    def audio_encoder_for_codec(audio_codec: str) -> str:
        codec = audio_codec or ""
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
                if sw_video_encoder == "libx264":
                    command += ["-preset", "veryfast", "-tune", "zerolatency"]
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
        command = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "info" if enable_cso_output_command_debug_logging else "warning",
        ]
        if enable_cso_output_command_debug_logging:
            command += ["-stats"]
        command += [
            "-probesize",
            "2097152",
            "-analyzeduration",
            "5000000",
            "-f",
            "mpegts",
            "-i",
            "pipe:0",
            "-map",
            "0:v:0?",
            "-map",
            "0:a?",
            "-fflags",
            "+discardcorrupt+genpts",
            "-err_detect",
            "ignore_err",
            "-max_muxing_queue_size",
            "4096",
        ]
        subtitle_mode = self._apply_stream_selection(command)
        hls_policy = _effective_hls_runtime_policy(self.policy)
        mode = hls_policy.get("output_mode") or "force_remux"
        original_policy = self.policy
        try:
            self.policy = hls_policy
            if mode == "force_transcode":
                self._apply_transcode_options(command, subtitle_mode)
            else:
                command += ["-c", "copy"]
                if subtitle_mode == "drop":
                    command.append("-sn")
                # HLS MPEG-TS segments benefit from repeated codec headers for
                # clients joining mid-playlist.
                command += ["-mpegts_flags", "+resend_headers"]
        finally:
            self.policy = original_policy

        command.append("-dn")
        segment_pattern = str(output_dir / "seg_%06d.ts")
        playlist_path = str(output_dir / "index.m3u8")
        hls_flags = ["delete_segments", "append_list", "omit_endlist", "temp_file", "independent_segments"]
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
            "+".join(hls_flags),
            "-hls_delete_threshold",
            "2",
            "-hls_segment_filename",
            segment_pattern,
            playlist_path,
        ]
        return command


def _cso_unavailable_slate_message(reason_key, detail_hint=""):
    if reason_key == "startup_pending":
        return "", ""
    payload = CSO_UNAVAILABLE_SLATE_MESSAGES.get(reason_key) or CSO_UNAVAILABLE_SLATE_MESSAGES["playback_unavailable"]
    title = payload["title"]
    subtitle = payload["subtitle"]
    detail = clean_text(detail_hint)
    if detail:
        subtitle = f"{subtitle} {detail}".strip()
    return title, subtitle


def _cso_unavailable_duration_seconds(reason_key):
    fallback = CSO_UNAVAILABLE_REASON_DURATIONS_SECONDS.get("default", 10)
    try:
        return int(CSO_UNAVAILABLE_REASON_DURATIONS_SECONDS.get(reason_key, fallback))
    except Exception:
        return int(fallback)


def _resolve_cso_unavailable_logo_path():
    project_root = Path(__file__).resolve().parents[1]
    candidates = [
        project_root / "frontend/src/assets/icon.png",
        project_root / "logo.png",
        project_root / "frontend/public/icons/Headendarr-Logo.png",
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return str(candidate)
    return None


def _escape_ffmpeg_drawtext_text(value):
    text = clean_text(value)
    text = text.replace("\\", "\\\\")
    text = text.replace(":", "\\:")
    text = text.replace("'", "\\'")
    text = text.replace(",", "\\,")
    text = text.replace("[", "\\[")
    text = text.replace("]", "\\]")
    return text


def _wrap_slate_words(text, max_chars=44, max_lines=2):
    words = [part for part in str(text or "").strip().split() if part]
    if not words:
        return []
    lines = []
    current = []
    for word in words:
        candidate = " ".join(current + [word]).strip()
        if len(candidate) <= max_chars or not current:
            current.append(word)
            continue
        lines.append(" ".join(current))
        current = [word]
        if len(lines) >= max_lines - 1:
            break
    if current and len(lines) < max_lines:
        lines.append(" ".join(current))
    return lines[:max_lines]


def _build_cso_slate_media_hint(media_hint):
    hint = dict(media_hint or {})
    width = max(16, int(hint.get("width") or 0))
    height = max(16, int(hint.get("height") or 0))
    fps_value = float(hint.get("fps") or 0.0)
    fps = int(round(fps_value)) if fps_value > 0 else 0
    pixel_format = clean_key(hint.get("pixel_format")) or "yuv420p"
    if width <= 16 or height <= 16:
        width = 1280
        height = 720
    if fps <= 0:
        avg_frame_rate = clean_text(hint.get("avg_frame_rate"))
        if avg_frame_rate and "/" in avg_frame_rate:
            try:
                numerator, denominator = avg_frame_rate.split("/", 1)
                denominator_value = max(1, int(float(denominator)))
                fps = int(round(float(numerator) / float(denominator_value)))
            except Exception:
                fps = 0
    if fps <= 0:
        fps = 25
    if fps > 60:
        fps = 60
    return {
        "width": width,
        "height": height,
        "fps": fps,
        "pixel_format": pixel_format,
    }


def build_cso_slate_command(
    reason_key,
    duration_seconds=10,
    output_target="pipe:1",
    detail_hint="",
    realtime=False,
    media_hint=None,
):
    if not reason_key:
        reason_key = "playback_unavailable"
    duration_value = None if duration_seconds is None else max(1, int(duration_seconds))
    slate_media_hint = _build_cso_slate_media_hint(media_hint)
    startup_width = int(slate_media_hint.get("width") or 1280)
    startup_height = int(slate_media_hint.get("height") or 720)
    startup_fps = int(slate_media_hint.get("fps") or 25)
    startup_pix_fmt = clean_key(slate_media_hint.get("pixel_format")) or "yuv420p"
    render_fps = 60
    layout_scale = min(float(startup_width) / 1280.0, float(startup_height) / 720.0)
    title_font_size = max(28, int(round(52 * layout_scale)))
    subtitle_font_size = max(14, int(round(20 * layout_scale)))
    panel_x = max(24, int(round(70 * float(startup_width) / 1280.0)))
    panel_w = max(320, int(round(1140 * float(startup_width) / 1280.0)))
    panel_h = max(160, int(round(340 * float(startup_height) / 720.0)))
    panel_y = max(
        12, int(round((startup_height - panel_h) / 2.0 - (160 * float(startup_height) / 720.0) + panel_h / 2.0))
    )
    logo_width = max(52, int(round(92 * layout_scale)))
    logo_margin_x = max(24, int(round(42 * float(startup_width) / 1280.0)))
    logo_margin_y = max(24, int(round(34 * float(startup_height) / 720.0)))
    title_y = int(round((startup_height / 2.0) - (84 * float(startup_height) / 720.0)))
    subtitle_y_1 = int(round((startup_height / 2.0) + (2 * float(startup_height) / 720.0)))
    subtitle_y_2 = int(round((startup_height / 2.0) + (30 * float(startup_height) / 720.0)))
    subtitle_y_3 = int(round((startup_height / 2.0) + (58 * float(startup_height) / 720.0)))
    subtitle_y_4 = int(round((startup_height / 2.0) + (86 * float(startup_height) / 720.0)))
    blob1_size = max(220, int(round(680 * layout_scale)))
    blob2_size = max(240, int(round(760 * layout_scale)))
    blob3_size = max(210, int(round(620 * layout_scale)))
    blob1_side_size = max(80, int(round(240 * layout_scale)))
    blob2_side_size = max(72, int(round(210 * layout_scale)))
    startup_video = f"color=c=black:s={startup_width}x{startup_height}:r={startup_fps}"
    startup_audio = "anullsrc=channel_layout=stereo:sample_rate=48000"
    if duration_value is not None:
        startup_video = f"{startup_video}:d={duration_value}"
        startup_audio = f"{startup_audio}:d={duration_value}"
    if reason_key == "startup_pending":
        command = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "warning",
        ]
        if realtime:
            command += [
                "-re",
            ]
        command += [
            "-f",
            "lavfi",
            "-i",
            startup_video,
            "-f",
            "lavfi",
            "-i",
            startup_audio,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-tune",
            "stillimage",
            "-pix_fmt",
            startup_pix_fmt,
            "-bf",
            "0",
            "-g",
            str(startup_fps),
            "-keyint_min",
            str(startup_fps),
            "-sc_threshold",
            "0",
            "-x264-params",
            "repeat-headers=1:scenecut=0",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-ar",
            "48000",
            "-ac",
            "2",
            "-shortest",
            "-mpegts_flags",
            "+resend_headers",
            "-f",
            "mpegts",
            output_target,
        ]
        return command

    title, subtitle = _cso_unavailable_slate_message(reason_key, detail_hint=detail_hint)
    title = _escape_ffmpeg_drawtext_text(title)
    subtitle_lines = [
        _escape_ffmpeg_drawtext_text(line) for line in _wrap_slate_words(subtitle, max_chars=84, max_lines=4)
    ]
    drawtext_title = (
        "drawtext=" f"text='{title}':" "fontcolor=white:" f"fontsize={title_font_size}:" f"x=(w-text_w)/2:y={title_y}"
    )
    drawtext_subtitle_1 = (
        "drawtext="
        f"text='{subtitle_lines[0] if len(subtitle_lines) > 0 else ''}':"
        f"fontcolor=white:fontsize={subtitle_font_size}:"
        f"x=(w-text_w)/2:y={subtitle_y_1}"
    )
    drawtext_subtitle_2 = (
        "drawtext="
        f"text='{subtitle_lines[1] if len(subtitle_lines) > 1 else ''}':"
        f"fontcolor=white:fontsize={subtitle_font_size}:"
        f"x=(w-text_w)/2:y={subtitle_y_2}"
    )
    drawtext_subtitle_3 = (
        "drawtext="
        f"text='{subtitle_lines[2] if len(subtitle_lines) > 2 else ''}':"
        f"fontcolor=white:fontsize={subtitle_font_size}:"
        f"x=(w-text_w)/2:y={subtitle_y_3}"
    )
    drawtext_subtitle_4 = (
        "drawtext="
        f"text='{subtitle_lines[3] if len(subtitle_lines) > 3 else ''}':"
        f"fontcolor=white:fontsize={subtitle_font_size}:"
        f"x=(w-text_w)/2:y={subtitle_y_4}"
    )
    draw_panel = f"drawbox=x={panel_x}:y={panel_y}:w={panel_w}:h={panel_h}:color=0x0B0F14@0.64:t=fill"
    draw_border = f"drawbox=x={panel_x}:y={panel_y}:w={panel_w}:h={panel_h}:color=0xE2E8F0@0.16:t=2"
    logo_path = _resolve_cso_unavailable_logo_path()
    filter_steps = [
        "[1:v]format=rgba,colorchannelmixer=aa=0.30,gblur=sigma=90[blob1]",
        "[2:v]format=rgba,colorchannelmixer=aa=0.26,gblur=sigma=105[blob2]",
        "[3:v]format=rgba,colorchannelmixer=aa=0.24,gblur=sigma=98[blob3]",
        "[0:v][blob1]overlay=x='(W-w)/2-W*0.14+sin(2*PI*t/12)*42':y='H*0.16+cos(2*PI*t/12)*24':shortest=1[bg1]",
        "[bg1][blob2]overlay=x='(W-w)/2+W*0.12+cos(2*PI*t/11+0.8)*46':y='H*0.18+sin(2*PI*t/11+0.8)*28':shortest=1[bg2]",
        "[bg2][blob3]overlay=x='(W-w)/2+W*0.02+sin(2*PI*t/13+1.6)*50':y='H*0.54+cos(2*PI*t/13+1.6)*22':shortest=1[bg3]",
        f"[blob1]scale=w={blob1_side_size}:h={blob1_side_size}[blob1_side]",
        f"[blob2]scale=w={blob2_side_size}:h={blob2_side_size}[blob2_side]",
        "[bg3][blob1_side]overlay=x='W*0.06+sin(2*PI*t/9+0.35)*18':y='H*0.28+cos(2*PI*t/9+0.95)*14':shortest=1[bg4]",
        "[bg4][blob2_side]overlay=x='W-w-W*0.07+cos(2*PI*t/9+1.15)*20':y='H*0.72+sin(2*PI*t/9+0.55)*12':shortest=1[bg5]",
        "[bg5]gblur=sigma=42:steps=3,fps=60[bg_blur]",
    ]
    input_args = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-f",
        "lavfi",
        "-i",
        f"color=c=0x0B0F14:s={startup_width}x{startup_height}:r={render_fps}"
        + (f":d={duration_value}" if duration_value is not None else ""),
        "-f",
        "lavfi",
        "-i",
        f"color=c=0x21A3CF:s={blob1_size}x{blob1_size}:r={render_fps}"
        + (f":d={duration_value}" if duration_value is not None else ""),
        "-f",
        "lavfi",
        "-i",
        f"color=c=0x79D2C0:s={blob2_size}x{blob2_size}:r={render_fps}"
        + (f":d={duration_value}" if duration_value is not None else ""),
        "-f",
        "lavfi",
        "-i",
        f"color=c=0x6AA8FF:s={blob3_size}x{blob3_size}:r={render_fps}"
        + (f":d={duration_value}" if duration_value is not None else ""),
    ]
    if logo_path:
        input_args += ["-loop", "1", "-i", logo_path]
        filter_steps.append(f"[4:v]scale=w={logo_width}:h=-1:flags=lanczos,format=rgba,colorchannelmixer=aa=0.98[logo]")
        filter_steps.append(f"[bg_blur][logo]overlay=x={logo_margin_x}:y={logo_margin_y}:shortest=1[bg_logo]")
        background_label = "bg_logo"
    else:
        background_label = "bg_blur"
    filter_steps.append(f"[{background_label}]{draw_panel}[panel]")
    filter_steps.append(f"[panel]{draw_border}[panel2]")
    panel_label = "panel2"
    filter_steps += [
        f"[{panel_label}]{drawtext_title}[title1]",
        "[title1]" + drawtext_subtitle_1 + "[title2]",
        "[title2]" + drawtext_subtitle_2 + "[title3]",
        "[title3]" + drawtext_subtitle_3 + "[title4]",
        "[title4]" + drawtext_subtitle_4 + ",eq=brightness=-0.03:contrast=1.06:saturation=1.18[vout]",
    ]
    return input_args + [
        "-f",
        "lavfi",
        "-i",
        "anullsrc=channel_layout=stereo:sample_rate=48000"
        + (f":d={duration_value}" if duration_value is not None else ""),
        "-filter_complex",
        ";".join(filter_steps),
        "-map",
        "[vout]",
        "-map",
        f"{5 if logo_path else 4}:a",
        "-c:v",
        "libx264",
        "-preset",
        "superfast",
        "-tune",
        "zerolatency",
        "-pix_fmt",
        "yuv420p",
        "-r",
        str(render_fps),
        "-bf",
        "0",
        "-g",
        str(max(render_fps * 2, render_fps)),
        "-keyint_min",
        str(max(render_fps * 2, render_fps)),
        "-sc_threshold",
        "0",
        "-x264-params",
        "repeat-headers=1:scenecut=0",
        "-c:a",
        "aac",
        "-b:a",
        "96k",
        "-ar",
        "48000",
        "-ac",
        "2",
        "-shortest",
        "-mpegts_flags",
        "+resend_headers",
        "-f",
        "mpegts",
        output_target,
    ]


def _cso_slate_cache_hash(reason_key, duration_seconds):
    digest = hashlib.sha256()
    digest.update(CSO_UNAVAILABLE_SLATE_CACHE_VERSION.encode("utf-8"))
    digest.update(reason_key.encode("utf-8"))
    digest.update(str(int(duration_seconds)).encode("utf-8"))
    digest.update(" ".join(build_cso_slate_command(reason_key, duration_seconds=duration_seconds)).encode("utf-8"))
    return digest.hexdigest()[:12]


async def _cleanup_cso_slate_cache(cache_dir, max_age_seconds):
    if not cache_dir.exists():
        return
    now_value = time.time()
    for path in cache_dir.glob("*.ts"):
        try:
            if (now_value - path.stat().st_mtime) > max_age_seconds:
                await asyncio.to_thread(path.unlink, missing_ok=True)
        except Exception:
            continue


async def ensure_cso_slate_asset(config_path, reason_key, duration_seconds):
    if not config_path:
        return None
    cache_dir = Path(config_path) / "cache" / "cso_slates"
    cache_dir.mkdir(parents=True, exist_ok=True)
    await _cleanup_cso_slate_cache(cache_dir, max_age_seconds=CSO_UNAVAILABLE_SLATE_CACHE_TTL_SECONDS)
    cache_hash = _cso_slate_cache_hash(reason_key, int(duration_seconds))
    out_path = cache_dir / f"{reason_key}_{int(duration_seconds)}s_{cache_hash}.ts"
    if out_path.exists() and out_path.stat().st_size > 0:
        return str(out_path)
    command = build_cso_slate_command(reason_key, duration_seconds=duration_seconds, output_target=str(out_path))
    logger.info("Rendering CSO slate asset reason=%s path=%s", reason_key, out_path)
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    stderr = b""
    try:
        _, stderr = await process.communicate()
    except Exception:
        try:
            process.kill()
        except Exception:
            pass
    if process.returncode not in (0, None) or not out_path.exists() or out_path.stat().st_size <= 0:
        logger.warning(
            "Failed rendering CSO slate reason=%s rc=%s stderr=%s",
            reason_key,
            process.returncode,
            stderr.decode("utf-8", errors="replace").strip() or "n/a",
        )
        try:
            await asyncio.to_thread(out_path.unlink, missing_ok=True)
        except Exception:
            pass
        return None
    return str(out_path)


async def iter_cso_slate_source(config_path, reason, detail_hint=""):
    reason_key = clean_key(reason, fallback="playback_unavailable")
    resolved_duration = _cso_unavailable_duration_seconds(reason_key)
    detail_text = clean_text(detail_hint)
    session = CsoSlateSession(
        key=f"cso-unavailable-{reason_key}-{int(time.time() * 1000)}",
        config_path=config_path,
        reason=reason_key,
        detail_hint=detail_text,
        duration_seconds=resolved_duration,
    )
    subscriber_id = f"{session.key}-subscriber"
    await session.start()
    queue = await session.add_subscriber(subscriber_id, prebuffer_bytes=0)
    try:
        while True:
            chunk = await queue.get()
            if chunk is None:
                break
            yield chunk
    finally:
        try:
            await session.remove_subscriber(subscriber_id)
        except Exception:
            pass


async def cso_unavailable_slate_stream(
    reason,
    policy=None,
    detail_hint="",
    config_path="",
):
    reason_key = clean_key(reason, fallback="playback_unavailable")
    if not policy:
        async for chunk in iter_cso_slate_source(config_path, reason_key, detail_hint=detail_hint):
            yield chunk
        return

    effective_policy = dict(policy or {})
    container = str(effective_policy.get("container") or "mpegts")
    if container in {"matroska", "mp4"}:
        effective_policy["output_mode"] = "force_transcode"
        if not effective_policy.get("audio_codec"):
            effective_policy["audio_codec"] = "aac"
        if "video_codec" not in effective_policy:
            effective_policy["video_codec"] = ""
    command = CsoOutputFfmpegCommandBuilder(effective_policy).build_output_command()
    logger.info(
        "Starting CSO unavailable slate transform reason=%s duration=%ss policy=%s command=%s",
        reason_key,
        _cso_unavailable_duration_seconds(reason_key),
        effective_policy,
        command,
    )
    process = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def _writer():
        try:
            async for chunk in iter_cso_slate_source(config_path, reason_key, detail_hint=detail_hint):
                if not process.stdin:
                    break
                process.stdin.write(chunk)
                await process.stdin.drain()
        except Exception:
            pass
        finally:
            try:
                if process.stdin:
                    process.stdin.close()
            except Exception:
                pass

    async def _stderr_reader():
        while True:
            try:
                line = await process.stderr.readline()
            except Exception:
                break
            if not line:
                break

    writer_task = asyncio.create_task(_writer())
    stderr_task = asyncio.create_task(_stderr_reader())
    emitted_bytes = 0
    try:
        while process.stdout:
            chunk = await process.stdout.read(MPEGTS_CHUNK_BYTES)
            if not chunk:
                break
            emitted_bytes += len(chunk)
            yield chunk
    finally:
        try:
            await asyncio.wait_for(writer_task, timeout=2.0)
        except Exception:
            writer_task.cancel()
        try:
            process.terminate()
            await asyncio.wait_for(process.wait(), timeout=1.5)
        except Exception:
            try:
                process.kill()
            except Exception:
                pass
        try:
            await asyncio.wait_for(stderr_task, timeout=0.5)
        except Exception:
            pass
        logger.info("CSO unavailable slate transform ended reason=%s bytes=%s", reason, emitted_bytes)


def build_cso_stream_plan(
    generator, content_type, error_message, status_code, cutoff_seconds=None, final_status_code=None
):
    return CsoStreamPlan(
        generator=generator,
        content_type=content_type,
        error_message=error_message,
        status_code=int(status_code or 500),
        cutoff_seconds=cutoff_seconds,
        final_status_code=final_status_code,
    )


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
        slate_session=None,
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
        self.ingest_user_agent = clean_text(ingest_user_agent)
        self.slate_session = slate_session
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
        self.failover_in_progress = False
        self.failover_exhausted = False
        self.session_start_ts = 0.0
        self.failover_start_ts = 0.0
        self.current_attempt_start_ts = 0.0
        self.current_attempt_first_chunk_logged = False
        self.current_source_probe = {}
        self._current_source_probe_persisted = False
        self._current_source_probe_input_section_closed = False
        self.first_healthy_stream_seen = False

    def build_unavailable_stream_plan(
        self, config, policy, reason, detail_hint="", profile_name="", channel=None, source=None, status_code=503
    ):
        allow_unavailable_slate = should_allow_unavailable_slate(profile_name, channel)
        if not CSO_UNAVAILABLE_SHOW_SLATE or not allow_unavailable_slate or self.slate_session is None:
            message = (
                "Channel unavailable due to connection limits"
                if reason == "capacity_blocked"
                else "Unable to start CSO stream"
            )
            return build_cso_stream_plan(None, None, message, status_code)

        reason_key = clean_key(reason, fallback="playback_unavailable")
        resolved_duration = _cso_unavailable_duration_seconds(reason_key)
        unique_suffix = int(time.time() * 1000)

        async def _generator():
            self.slate_session.reason = reason_key
            self.slate_session.detail_hint = clean_text(detail_hint)
            self.slate_session.duration_seconds = resolved_duration
            output_session = CsoOutputSession(
                key=f"cso-terminal-output-{reason_key}-{unique_suffix}",
                channel_id=getattr(channel, "id", None) if channel is not None else self.channel_id,
                policy=policy,
                ingest_session=None,
                slate_session=self.slate_session,
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
            cutoff_seconds=_cso_unavailable_duration_seconds(reason),
            final_status_code=int(status_code or 503),
        )

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
            self.history.clear()
            self.history_bytes = 0
            logger.info(
                "CSO ingest start requested channel=%s sources=%s",
                self.channel_id,
                len(self.sources or []),
            )
            self.session_start_ts = time.time()
            self.failover_start_ts = 0.0
            self.failover_in_progress = False
            self.failover_exhausted = False
            start_result = await self._start_best_source_unlocked(reason="initial_start")
            if not start_result.success:
                self.running = False
                self.last_error = start_result.reason or "no_available_source"
                self.failover_exhausted = True
                return

    async def _spawn_ingest_process(self, source_url, program_index, source=None):
        playlist = getattr(source, "playlist", None) if source is not None else None
        source_user_agent = clean_text(getattr(playlist, "user_agent", "")) or self.ingest_user_agent
        source_headers = _resolve_cso_ingest_headers(source)
        source_user_agent = _header_value(source_headers, "User-Agent") or source_user_agent
        source_probe = load_source_media_shape(source) if source is not None else {}
        command = _build_ingest_ffmpeg_command(
            source_url,
            program_index=program_index,
            user_agent=source_user_agent,
            request_headers=source_headers,
        )
        self.current_source_probe = dict(source_probe or {})
        self._current_source_probe_persisted = False
        self._current_source_probe_input_section_closed = False
        logger.info(
            "Starting CSO ingest channel=%s source=%s policy=(%s) source_probe=%s command=%s",
            self.channel_id,
            getattr(source, "id", None) if source is not None else getattr(self.current_source, "id", None),
            "copy-only-ingest",
            self.current_source_probe or {},
            _redact_ingest_command_for_log(command),
        )
        return await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    def _ingest_probe_is_complete(self, probe):
        data = dict(probe or {})
        return bool(
            clean_key(data.get("video_codec"))
            and int(data.get("width") or 0) > 0
            and int(data.get("height") or 0) > 0
            and float(data.get("fps") or 0.0) > 0.0
        )

    async def _persist_current_source_probe_if_ready(self):
        if self._current_source_probe_persisted:
            return
        if not self._ingest_probe_is_complete(self.current_source_probe):
            return
        source_id = getattr(self.current_source, "id", None)
        if not source_id:
            return
        try:
            persisted = await persist_source_media_shape(
                source_id, self.current_source_probe, observed_at=utc_now_naive()
            )
        except Exception:
            persisted = False
        if persisted:
            self._current_source_probe_persisted = True
            if enable_cso_ingest_command_debug_logging:
                logger.info(
                    "CSO ingest learned live media shape channel=%s source_id=%s probe=%s",
                    self.channel_id,
                    source_id,
                    dict(self.current_source_probe or {}),
                )
        elif enable_cso_ingest_command_debug_logging:
            logger.info(
                "CSO ingest learned live media shape but persist failed channel=%s source_id=%s probe=%s",
                self.channel_id,
                source_id,
                dict(self.current_source_probe or {}),
            )

    async def _update_current_source_probe_from_stderr(self, rendered):
        text = clean_text(rendered)
        if not text or self._current_source_probe_input_section_closed:
            return
        if text.startswith("Output #"):
            self._current_source_probe_input_section_closed = True
            if enable_cso_ingest_command_debug_logging and self.current_source_probe:
                logger.info(
                    "CSO ingest ffmpeg input inspection completed channel=%s source_id=%s probe=%s",
                    self.channel_id,
                    getattr(self.current_source, "id", None),
                    dict(self.current_source_probe or {}),
                )
            await self._persist_current_source_probe_if_ready()
            return

        updated = False
        input_match = _FFMPEG_INPUT_RE.search(text)
        if input_match and not clean_key(self.current_source_probe.get("container")):
            self.current_source_probe["container"] = clean_key((input_match.group(1) or "").split(",", 1)[0])
            updated = True

        video_match = _FFMPEG_VIDEO_STREAM_RE.search(text)
        if video_match:
            self.current_source_probe["video_codec"] = clean_key(video_match.group(1))
            self.current_source_probe["video_profile"] = clean_text(video_match.group(2))
            self.current_source_probe["pixel_format"] = clean_key(video_match.group(3).split("(", 1)[0])
            self.current_source_probe["width"] = int(video_match.group(4) or 0)
            self.current_source_probe["height"] = int(video_match.group(5) or 0)
            fps_match = _FFMPEG_FPS_RE.search(text)
            if fps_match:
                try:
                    fps_value = float(fps_match.group(1))
                except Exception:
                    fps_value = 0.0
                if fps_value > 0:
                    self.current_source_probe["fps"] = fps_value
                    if not clean_text(self.current_source_probe.get("avg_frame_rate")):
                        rounded_fps = int(round(fps_value))
                        self.current_source_probe["avg_frame_rate"] = (
                            f"{rounded_fps}/1" if rounded_fps > 0 else clean_text(fps_match.group(1))
                        )
            updated = True

        audio_match = _FFMPEG_AUDIO_STREAM_RE.search(text)
        if audio_match:
            self.current_source_probe["audio_codec"] = clean_key(audio_match.group(1))
            try:
                self.current_source_probe["audio_sample_rate"] = int(audio_match.group(3) or 0)
            except Exception:
                self.current_source_probe["audio_sample_rate"] = 0
            channel_layout = clean_key(audio_match.group(4))
            self.current_source_probe["audio_channel_layout"] = channel_layout
            if channel_layout == "mono":
                self.current_source_probe["audio_channels"] = 1
            elif channel_layout in {"stereo", "2 channels"}:
                self.current_source_probe["audio_channels"] = 2
            self.current_source_probe["has_audio"] = True
            updated = True

        if updated:
            if enable_cso_ingest_command_debug_logging:
                logger.info(
                    "CSO ingest ffmpeg metadata update channel=%s source_id=%s line=%s probe=%s",
                    self.channel_id,
                    getattr(self.current_source, "id", None),
                    text,
                    dict(self.current_source_probe or {}),
                )
            await self._persist_current_source_probe_if_ready()

    def is_hunting_for_stream(self):
        if self.failover_in_progress:
            return True
        if not self.first_healthy_stream_seen:
            return True
        if not self.running:
            return True
        if self.current_source is None:
            return True
        return False

    def _activate_process_unlocked(self, process):
        self.process = process
        self.running = True
        self.history.clear()
        self.history_bytes = 0
        self.process_token += 1
        token = self.process_token
        self.last_source_start_ts = time.time()
        self.current_attempt_start_ts = self.last_source_start_ts
        self.current_attempt_first_chunk_logged = False
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
            "CSO ingest upstream connected channel=%s source_id=%s source_url=%s subscribers=%s elapsed_ms=%s failover_elapsed_ms=%s",
            self.channel_id,
            getattr(self.current_source, "id", None),
            self.current_source_url,
            len(self.subscribers),
            int(max(0.0, self.last_source_start_ts - float(self.session_start_ts or self.last_source_start_ts)) * 1000),
            int(
                max(0.0, self.last_source_start_ts - float(self.failover_start_ts or self.last_source_start_ts)) * 1000
            ),
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
            key=lambda item: convert_to_int(getattr(item, "priority", 0), 0),
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
                await self._update_current_source_probe_from_stderr(rendered)
                progress_handled = False
                if "=" in rendered:
                    key, value = rendered.split("=", 1)
                    key = clean_key(key)
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
            await self._update_current_source_probe_from_stderr(rendered)
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
                chunk = await process.stdout.read(MPEGTS_CHUNK_BYTES)
                if not chunk:
                    break
                if not saw_data and not self.current_attempt_first_chunk_logged:
                    now_value = time.time()
                    logger.info(
                        "CSO ingest first chunk channel=%s source_id=%s bytes=%s elapsed_ms=%s connect_elapsed_ms=%s failover_elapsed_ms=%s failover_in_progress=%s",
                        self.channel_id,
                        getattr(self.current_source, "id", None),
                        len(chunk),
                        int(max(0.0, now_value - float(self.session_start_ts or now_value)) * 1000),
                        int(max(0.0, now_value - float(self.current_attempt_start_ts or now_value)) * 1000),
                        int(max(0.0, now_value - float(self.failover_start_ts or now_value)) * 1000),
                        bool(self.failover_in_progress),
                    )
                    self.current_attempt_first_chunk_logged = True
                    self.first_healthy_stream_seen = True
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
        self.failover_exhausted = False
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
            self.running = bool(self.allow_failover)
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
            "CSO ingest failover decision channel=%s reason=%s failed_source_id=%s hold_down_applied=%s elapsed_ms=%s",
            self.channel_id,
            reason,
            failed_source_id,
            hold_down_applied,
            int(max(0.0, time.time() - float(self.session_start_ts or time.time())) * 1000),
        )
        self.failover_start_ts = time.time()
        self.failover_in_progress = True

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
                        "CSO ingest failover started replacement channel=%s recycled_cycle=%s elapsed_ms=%s failover_elapsed_ms=%s",
                        self.channel_id,
                        recycle_failed_sources,
                        int(max(0.0, time.time() - float(self.session_start_ts or time.time())) * 1000),
                        int(max(0.0, time.time() - float(self.failover_start_ts or time.time())) * 1000),
                    )
                    self.running = True
                    self.failover_in_progress = False
                    self.failover_exhausted = False
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
        self.failover_in_progress = False
        self.failover_exhausted = True
        self.running = False
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
            self.failover_in_progress = False
            self.failover_exhausted = False
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
            self.history.clear()
            self.history_bytes = 0
            for q in self.subscribers.values():
                await q.put_eof()


class CsoSlateSession:
    def __init__(
        self, key, config_path, reason="startup_pending", detail_hint="", media_hint=None, duration_seconds=None
    ):
        self.key = key
        self.config_path = clean_text(config_path)
        self.reason = clean_key(reason, fallback="startup_pending")
        self.detail_hint = clean_text(detail_hint)
        self.media_hint = dict(media_hint or {})
        self.duration_seconds = duration_seconds
        self.running = False
        self.process = None
        self.lock = asyncio.Lock()
        self.last_activity = time.time()
        self.subscribers = {}
        self.history = deque()
        self.history_bytes = 0
        self.max_history_bytes = 4 * 1024 * 1024
        self.read_task = None
        self.stderr_task = None
        self.start_ts = 0.0
        self.first_chunk_logged = False

    async def _spawn_process(self):
        command = build_cso_slate_command(
            self.reason,
            duration_seconds=self.duration_seconds,
            output_target="pipe:1",
            detail_hint=self.detail_hint,
            realtime=True,
            media_hint=self.media_hint,
        )
        logger.info("Starting CSO slate session key=%s reason=%s command=%s", self.key, self.reason, command)
        return await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    def update_media_hint(self, media_hint):
        if not media_hint:
            return
        self.media_hint = dict(media_hint or {})

    async def _broadcast(self, chunk):
        if not chunk:
            return
        self.last_activity = time.time()
        if not self.first_chunk_logged:
            logger.info(
                "CSO slate first chunk key=%s reason=%s bytes=%s elapsed_ms=%s",
                self.key,
                self.reason,
                len(chunk),
                int(max(0.0, self.last_activity - float(self.start_ts or self.last_activity)) * 1000),
            )
            self.first_chunk_logged = True
        subscriber_queues = []
        async with self.lock:
            self.history.append(chunk)
            self.history_bytes += len(chunk)
            while self.history_bytes > self.max_history_bytes and self.history:
                old = self.history.popleft()
                self.history_bytes -= len(old)
            subscriber_queues = list(self.subscribers.values())
        for queue in subscriber_queues:
            await queue.put_drop_oldest(chunk)

    async def _read_loop(self, process):
        try:
            while self.running and process and process.stdout:
                chunk = await process.stdout.read(MPEGTS_CHUNK_BYTES)
                if not chunk:
                    break
                await self._broadcast(chunk)
        finally:
            return_code = None
            try:
                return_code = process.returncode
                if return_code is None:
                    return_code = await process.wait()
            except Exception:
                return_code = None
            logger.info("CSO slate session ended key=%s reason=%s return_code=%s", self.key, self.reason, return_code)
            async with self.lock:
                self.running = False
                if self.process is process:
                    self.process = None

    async def _stderr_loop(self, process):
        text_buffer = ""
        while self.running and process and process.stderr:
            try:
                chunk = await process.stderr.read(4096)
            except Exception:
                break
            if not chunk:
                break
            text_buffer += chunk.decode("utf-8", errors="replace")
            lines = re.split(r"[\r\n]+", text_buffer)
            text_buffer = lines.pop() if lines else ""
            for rendered in lines:
                rendered = rendered.strip()
                if rendered and enable_cso_slate_command_debug_logging:
                    logger.info("CSO slate ffmpeg[%s][%s]: %s", self.reason, self.key, rendered)
        rendered = text_buffer.strip()
        if rendered and enable_cso_slate_command_debug_logging:
            logger.info("CSO slate ffmpeg[%s][%s]: %s", self.reason, self.key, rendered)

    async def start(self):
        async with self.lock:
            if self.running:
                return
            self.history.clear()
            self.history_bytes = 0
            self.start_ts = time.time()
            self.first_chunk_logged = False
            self.process = await self._spawn_process()
            self.running = True
            self.read_task = asyncio.create_task(self._read_loop(self.process))
            self.stderr_task = asyncio.create_task(self._stderr_loop(self.process))

    async def add_subscriber(self, subscriber_id, prebuffer_bytes=0):
        async with self.lock:
            queue = ByteBudgetQueue(max_bytes=CSO_INGEST_SUBSCRIBER_QUEUE_MAX_BYTES)
            if prebuffer_bytes > 0 and self.history:
                total = 0
                items = []
                for chunk in reversed(self.history):
                    items.append(chunk)
                    total += len(chunk)
                    if total >= prebuffer_bytes:
                        break
                for chunk in reversed(items):
                    await queue.put_drop_oldest(chunk)
            self.subscribers[subscriber_id] = queue
            self.last_activity = time.time()
        return queue

    async def remove_subscriber(self, subscriber_id):
        async with self.lock:
            queue = self.subscribers.pop(subscriber_id, None)
            remaining = len(self.subscribers)
        if queue is not None:
            await queue.put_eof()
        if remaining == 0:
            await self.stop(force=True)
        return remaining

    async def stop(self, force=False):
        async with self.lock:
            if not self.running and not self.subscribers:
                return
            if not force and self.subscribers:
                return
            self.running = False
            process = self.process
            self.process = None
            read_task = self.read_task
            self.read_task = None
            stderr_task = self.stderr_task
            self.stderr_task = None
        if process:
            try:
                process.terminate()
                await _wait_process_exit_with_timeout(process, timeout_seconds=1.5)
            except Exception:
                try:
                    process.kill()
                    await _wait_process_exit_with_timeout(process, timeout_seconds=1.5)
                except Exception:
                    pass
        for task in (read_task, stderr_task):
            if not task or task.done():
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        async with self.lock:
            subscribers = list(self.subscribers.values())
            self.subscribers = {}
            self.history.clear()
            self.history_bytes = 0
        for queue in subscribers:
            await queue.put_eof()


class CsoOutputSession:
    def __init__(
        self,
        key,
        channel_id,
        policy,
        ingest_session=None,
        slate_session=None,
        event_source=None,
        use_slate_as_input=False,
    ):
        self.key = key
        self.channel_id = channel_id
        self.policy = policy
        self.use_slate_as_input = bool(use_slate_as_input)
        self.output_policy = _resolve_cso_output_policy(policy, self.use_slate_as_input)
        self.ingest_session = ingest_session
        self.slate_session = slate_session
        self.event_source = event_source
        self.process = None
        self.read_task = None
        self.write_task = None
        self.ingest_recovery_task = None
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
        self.slate_queue = None
        self._recent_ffmpeg_stderr = deque(maxlen=30)
        self.client_drop_state = {}
        self.client_last_touch = {}
        self._input_mode = "slate" if self.use_slate_as_input else "ingest"
        self.start_ts = 0.0
        self.first_output_chunk_logged = False
        self.first_ingest_chunk_logged = False
        self._last_ingest_recovery_attempt_ts = 0.0
        self._pending_input_chunks = deque()

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

    @staticmethod
    def _is_expected_handover_log(line):
        text = str(line or "").lower()
        if not text:
            return False
        return any(
            marker in text
            for marker in (
                "packet corrupt",
                "corrupt input packet",
                "timestamp discontinuity",
                "reconfiguring filter graph because video parameters changed",
            )
        )

    @staticmethod
    def _should_log_ffmpeg_stderr_line(line):
        text = str(line or "").strip()
        if not text:
            return False
        lower_text = text.lower()
        if text.startswith("frame="):
            return False
        if CsoOutputSession._is_expected_handover_log(lower_text):
            return False
        if any(token in lower_text for token in ("error", "invalid", "failed", "could not", "unsupported")):
            return True
        return False

    async def start(self):
        async with self.lock:
            if self.running:
                return
            if self.ingest_session is None and self.slate_session is None:
                self.last_error = "no_input_session"
                return
            self.start_ts = time.time()
            self.first_output_chunk_logged = False
            self.first_ingest_chunk_logged = False
            self._input_mode = "slate" if self.use_slate_as_input else "ingest"
            if self.ingest_session is not None:
                await self.ingest_session.start()
                self.ingest_queue = await self.ingest_session.add_subscriber(self.key, prebuffer_bytes=256 * 1024)
            if self.use_slate_as_input and self.slate_session is not None:
                await self.slate_session.start()
                self.slate_queue = await self.slate_session.add_subscriber(self.key, prebuffer_bytes=0)
                prime_deadline = time.time() + 3.0
                primed_bytes = 0
                while time.time() < prime_deadline and primed_bytes < 128 * 1024:
                    timeout_seconds = max(0.1, prime_deadline - time.time())
                    try:
                        primed_chunk = await asyncio.wait_for(self.slate_queue.get(), timeout=timeout_seconds)
                    except asyncio.TimeoutError:
                        break
                    if primed_chunk is None:
                        break
                    self._pending_input_chunks.append(("slate", primed_chunk))
                    primed_bytes += len(primed_chunk)
                logger.info(
                    "CSO output primed slate input channel=%s output_key=%s primed_bytes=%s pending_chunks=%s elapsed_ms=%s",
                    self.channel_id,
                    self.key,
                    primed_bytes,
                    len(self._pending_input_chunks),
                    int(max(0.0, time.time() - float(self.start_ts or time.time())) * 1000),
                )
            self.running = True
            try:
                command = CsoOutputFfmpegCommandBuilder(self.output_policy).build_output_command()
                logger.info(
                    "Starting CSO output channel=%s output_key=%s policy=(%s) command=%s",
                    self.channel_id,
                    self.key,
                    _policy_log_label(self.output_policy),
                    command,
                )
                self.process = await asyncio.create_subprocess_exec(
                    *command,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
            except Exception as exc:
                self.running = False
                self.last_error = f"output_start_failed:{exc}"
                raise
            logger.info(
                "CSO output started channel=%s output_key=%s policy=(%s) clients=%s",
                self.channel_id,
                self.key,
                _policy_log_label(self.output_policy),
                len(self.clients),
            )
            self.read_task = asyncio.create_task(self._read_loop())
            self.write_task = asyncio.create_task(self._write_loop())
            self.ingest_recovery_task = asyncio.create_task(self._ingest_recovery_loop())
            self.stderr_task = asyncio.create_task(self._stderr_loop())

    async def _ingest_recovery_loop(self):
        if self.ingest_session is None:
            return
        retry_interval_seconds = max(1.0, float(CSO_INGEST_RECOVERY_RETRY_INTERVAL_SECONDS))
        while self.running:
            await asyncio.sleep(retry_interval_seconds)
            if not self.running:
                return
            if self.ingest_session.running or bool(getattr(self.ingest_session, "failover_in_progress", False)):
                continue
            now_value = time.time()
            if (now_value - float(self._last_ingest_recovery_attempt_ts or 0.0)) < retry_interval_seconds:
                continue
            self._last_ingest_recovery_attempt_ts = now_value
            try:
                logger.info(
                    "CSO output attempting ingest recovery channel=%s output_key=%s elapsed_ms=%s",
                    self.channel_id,
                    self.key,
                    int(max(0.0, now_value - float(self.start_ts or now_value)) * 1000),
                )
                await self.ingest_session.start()
            except Exception as exc:
                logger.warning(
                    "CSO output ingest recovery attempt failed channel=%s output_key=%s error=%s",
                    self.channel_id,
                    self.key,
                    exc,
                )

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
                if enable_cso_output_command_debug_logging and self._should_log_ffmpeg_stderr_line(rendered):
                    logger.info("CSO output ffmpeg[%s][%s]: %s", self.channel_id, self.key, rendered)
        rendered = text_buffer.strip()
        if rendered:
            self._recent_ffmpeg_stderr.append(rendered)
            if enable_cso_output_command_debug_logging and self._should_log_ffmpeg_stderr_line(rendered):
                logger.info("CSO output ffmpeg[%s][%s]: %s", self.channel_id, self.key, rendered)

    async def _write_loop(self):
        try:
            while self.running and self.process and self.process.stdin and self._pending_input_chunks:
                chunk_mode, chunk = self._pending_input_chunks.popleft()
                try:
                    self.process.stdin.write(chunk)
                    await self.process.stdin.drain()
                    await self.touch_all_clients()
                except Exception:
                    return
                if chunk_mode == "ingest" and not self.first_ingest_chunk_logged:
                    now_value = time.time()
                    logger.info(
                        "CSO output first ingest chunk channel=%s output_key=%s bytes=%s elapsed_ms=%s failover_elapsed_ms=%s",
                        self.channel_id,
                        self.key,
                        len(chunk),
                        int(max(0.0, now_value - float(self.start_ts or now_value)) * 1000),
                        int(
                            max(
                                0.0,
                                now_value - float(getattr(self.ingest_session, "failover_start_ts", 0.0) or now_value),
                            )
                            * 1000
                        ),
                    )
                    self.first_ingest_chunk_logged = True
                if self._input_mode != chunk_mode:
                    elapsed_ms = int(max(0.0, time.time() - float(self.start_ts or time.time())) * 1000)
                    failover_elapsed_ms = int(
                        max(
                            0.0,
                            time.time() - float(getattr(self.ingest_session, "failover_start_ts", 0.0) or time.time()),
                        )
                        * 1000
                    )
                    logger.info(
                        "CSO output input switched channel=%s output_key=%s mode=%s elapsed_ms=%s failover_elapsed_ms=%s",
                        self.channel_id,
                        self.key,
                        chunk_mode,
                        elapsed_ms,
                        failover_elapsed_ms,
                    )
                    self._input_mode = chunk_mode
            while self.running and self.process and self.process.stdin:
                chunk = None
                chunk_mode = None
                if self.ingest_queue is not None:
                    ingest_timed_out = False
                    try:
                        chunk = await asyncio.wait_for(
                            self.ingest_queue.get(),
                            timeout=float(CSO_OUTPUT_SLATE_POLL_INTERVAL_SECONDS),
                        )
                    except asyncio.TimeoutError:
                        ingest_timed_out = True
                        chunk = None
                    if (
                        not ingest_timed_out
                        and chunk is None
                        and self.ingest_session is not None
                        and not self.ingest_session.running
                    ):
                        self.ingest_queue = None
                if chunk is not None:
                    chunk_mode = "ingest"
                    if not self.first_ingest_chunk_logged:
                        now_value = time.time()
                        logger.info(
                            "CSO output first ingest chunk channel=%s output_key=%s bytes=%s elapsed_ms=%s failover_elapsed_ms=%s",
                            self.channel_id,
                            self.key,
                            len(chunk),
                            int(max(0.0, now_value - float(self.start_ts or now_value)) * 1000),
                            int(
                                max(
                                    0.0,
                                    now_value
                                    - float(getattr(self.ingest_session, "failover_start_ts", 0.0) or now_value),
                                )
                                * 1000
                            ),
                        )
                        self.first_ingest_chunk_logged = True
                if chunk is None and self.slate_queue is not None:
                    slate_timed_out = False
                    try:
                        chunk = await asyncio.wait_for(
                            self.slate_queue.get(),
                            timeout=float(CSO_OUTPUT_SLATE_POLL_INTERVAL_SECONDS),
                        )
                    except asyncio.TimeoutError:
                        slate_timed_out = True
                        chunk = None
                    if not slate_timed_out and chunk is None:
                        self.slate_queue = None
                    else:
                        chunk_mode = "slate"
                if chunk is None:
                    if self.ingest_queue is None and self.slate_queue is None:
                        break
                    continue
                if chunk_mode and self._input_mode != chunk_mode:
                    elapsed_ms = int(max(0.0, time.time() - float(self.start_ts or time.time())) * 1000)
                    failover_elapsed_ms = int(
                        max(
                            0.0,
                            time.time() - float(getattr(self.ingest_session, "failover_start_ts", 0.0) or time.time()),
                        )
                        * 1000
                    )
                    logger.info(
                        "CSO output input switched channel=%s output_key=%s mode=%s elapsed_ms=%s failover_elapsed_ms=%s",
                        self.channel_id,
                        self.key,
                        chunk_mode,
                        elapsed_ms,
                        failover_elapsed_ms,
                    )
                    self._input_mode = chunk_mode
                try:
                    self.process.stdin.write(chunk)
                    await self.process.stdin.drain()
                    await self.touch_all_clients()
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
                if not self.first_output_chunk_logged:
                    now_value = time.time()
                    logger.info(
                        "CSO output first client-visible chunk channel=%s output_key=%s bytes=%s elapsed_ms=%s input_mode=%s",
                        self.channel_id,
                        self.key,
                        len(chunk),
                        int(max(0.0, now_value - float(self.start_ts or now_value)) * 1000),
                        self._input_mode,
                    )
                    self.first_output_chunk_logged = True
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
                    self.ingest_session is not None
                    and getattr(self.ingest_session, "last_reader_end_reason", None) == "ingest_reader_ended"
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
                        source_id=(
                            getattr(getattr(self.ingest_session, "current_source", None), "id", None)
                            or getattr(self.event_source, "id", None)
                        ),
                        playlist_id=(
                            getattr(getattr(self.ingest_session, "current_source", None), "playlist_id", None)
                            or getattr(self.event_source, "playlist_id", None)
                        ),
                        session_id=self.key,
                        event_type="playback_unavailable",
                        severity=severity,
                        details={
                            "reason": "output_reader_ended",
                            "return_code": return_code,
                            "ffmpeg_error": ffmpeg_error or None,
                            "policy": self.policy,
                            **_source_event_context(
                                getattr(self.ingest_session, "current_source", None) or self.event_source,
                                source_url=(
                                    getattr(self.ingest_session, "current_source_url", None)
                                    or getattr(self.event_source, "playlist_stream_url", None)
                                ),
                            ),
                        },
                    )

            await self.stop(force=True)

    @staticmethod
    def _stale_seconds_for_connection(connection_id):
        connection_text = str(connection_id or "")
        if connection_text.startswith("tvh-"):
            return float(CSO_OUTPUT_CLIENT_STALE_SECONDS_TVH)
        return float(CSO_OUTPUT_CLIENT_STALE_SECONDS)

    def _suspend_client_stale_checks(self):
        if self.ingest_session is None:
            return False
        try:
            return bool(self.ingest_session.is_hunting_for_stream())
        except Exception:
            return False

    async def _broadcast(self, chunk):
        if not chunk:
            return
        self.last_activity = time.time()
        now = time.time()
        stale_clients = []
        active_clients = []
        drop_results = {}
        suspend_stale_checks = self._suspend_client_stale_checks()
        async with self.lock:
            self.history.append(chunk)
            self.history_bytes += len(chunk)
            while self.history_bytes > self.max_history_bytes and self.history:
                old = self.history.popleft()
                self.history_bytes -= len(old)
            for connection_id, q in list(self.clients.items()):
                last_touch = float(self.client_last_touch.get(connection_id, now) or now)
                last_touch = max(last_touch, float(self.last_activity or 0.0))
                stale_seconds = self._stale_seconds_for_connection(connection_id)
                if not suspend_stale_checks and (now - last_touch) >= stale_seconds:
                    stale_clients.append((connection_id, stale_seconds))
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
        for connection_id, stale_seconds in stale_clients:
            logger.warning(
                "CSO output dropping stale client channel=%s output_key=%s connection_id=%s reason=no_consumer_progress stale_seconds=%s",
                self.channel_id,
                self.key,
                connection_id,
                int(stale_seconds),
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
            _policy_log_label(self.output_policy),
        )
        return q

    async def touch_client(self, connection_id):
        async with self.lock:
            if connection_id in self.clients:
                self.client_last_touch[connection_id] = time.time()
                self.last_activity = time.time()

    async def touch_all_clients(self):
        now_value = time.time()
        async with self.lock:
            if not self.clients:
                return
            for connection_id in self.clients.keys():
                self.client_last_touch[connection_id] = now_value
            self.last_activity = now_value

    async def prune_idle_clients(self, now_ts=None):
        now_value = float(now_ts if now_ts is not None else time.time())
        if self._suspend_client_stale_checks():
            return
        stale_ids = []
        async with self.lock:
            for connection_id in list(self.clients.keys()):
                last_touch = float(self.client_last_touch.get(connection_id, 0.0) or 0.0)
                last_touch = max(last_touch, float(self.last_activity or 0.0))
                stale_seconds = self._stale_seconds_for_connection(connection_id)
                if (now_value - last_touch) >= stale_seconds:
                    stale_ids.append((connection_id, stale_seconds))
        for connection_id, stale_seconds in stale_ids:
            logger.warning(
                "CSO output dropping stale client channel=%s output_key=%s connection_id=%s reason=idle_prune stale_seconds=%s",
                self.channel_id,
                self.key,
                connection_id,
                int(stale_seconds),
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
            _policy_log_label(self.output_policy),
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
            slate_queue = self.slate_queue
            self.slate_queue = None
            read_task = self.read_task
            self.read_task = None
            write_task = self.write_task
            self.write_task = None
            ingest_recovery_task = self.ingest_recovery_task
            self.ingest_recovery_task = None
            stderr_task = self.stderr_task
            self.stderr_task = None
            client_count = len(self.clients)
        logger.info(
            "Stopping CSO output channel=%s output_key=%s clients=%s force=%s policy=(%s)",
            self.channel_id,
            self.key,
            client_count,
            force,
            _policy_log_label(self.output_policy),
        )
        return_code = None
        try:
            if ingest_queue is not None and self.ingest_session is not None:
                await self.ingest_session.remove_subscriber(self.key)
        except Exception:
            pass
        try:
            if slate_queue is not None and self.slate_session is not None:
                await slate_queue.clear()
                await self.slate_session.remove_subscriber(self.key)
        except Exception:
            pass
        if process:
            try:
                if process.stdin:
                    process.stdin.close()
            except Exception:
                pass
            try:
                return_code = await _wait_process_exit_with_timeout(process, timeout_seconds=0.75)
            except Exception:
                try:
                    process.terminate()
                    return_code = await _wait_process_exit_with_timeout(process, timeout_seconds=2.0)
                except Exception:
                    try:
                        process.kill()
                        return_code = await _wait_process_exit_with_timeout(process, timeout_seconds=6.0)
                    except Exception:
                        if process.returncode is not None or not _process_is_running(process.pid):
                            return_code = process.returncode if process.returncode is not None else -9
                        else:
                            logger.warning(
                                "CSO output process did not exit after kill channel=%s output_key=%s",
                                self.channel_id,
                                self.key,
                            )
        for task in (read_task, write_task, ingest_recovery_task, stderr_task):
            if not task or task.done():
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        logger.info(
            "CSO output stopped channel=%s output_key=%s return_code=%s policy=(%s)",
            self.channel_id,
            self.key,
            return_code,
            _policy_log_label(self.output_policy),
        )
        async with self.lock:
            for q in self.clients.values():
                await q.put_eof()
            self.client_drop_state.clear()
            self.client_last_touch.clear()


class CsoHlsOutputSession:
    def __init__(
        self,
        key,
        channel_id,
        policy,
        ingest_session,
        cache_root_dir,
        slate_session=None,
        use_slate_as_input=False,
        event_source=None,
    ):
        self.key = key
        self.channel_id = channel_id
        self.policy = policy
        self.ingest_session = ingest_session
        self.slate_session = slate_session
        self.use_slate_as_input = bool(use_slate_as_input and slate_session is not None)
        self.event_source = event_source
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
        self.slate_queue = None
        self._recent_ffmpeg_stderr = deque(maxlen=30)
        self.clients = {}
        self._pending_input_chunks = deque()
        self.process_token = 0
        self._last_good_playlist_text = None
        self._last_good_playlist_ts = 0.0
        self._retain_completed_output_until = 0.0
        self.runtime_policy = _effective_hls_runtime_policy(policy)

    @staticmethod
    def _probe_has_video(probe):
        data = dict(probe or {})
        return bool(
            clean_key(data.get("video_codec"))
            and int(data.get("width") or 0) > 0
            and int(data.get("height") or 0) > 0
            and float(data.get("fps") or 0.0) > 0.0
        )

    @staticmethod
    def _playlist_segment_names(playlist_text):
        names = []
        for raw_line in str(playlist_text or "").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            names.append(line.split("?", 1)[0])
        return names

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
        self._last_good_playlist_text = None
        self._last_good_playlist_ts = 0.0

    async def start(self):
        async with self.lock:
            if self.running:
                return
            if self._retain_completed_output_until > time.time() and self._last_good_playlist_text:
                self.running = True
                return
            if self.use_slate_as_input:
                await self.slate_session.start()
                if not self.slate_session.running:
                    self.last_error = "slate_not_running"
                    return
            else:
                await self.ingest_session.start()
                if not self.ingest_session.running:
                    self.last_error = self.ingest_session.last_error or "ingest_not_running"
                    return

            await self._prepare_output_dir()
            self.ingest_queue = None
            self.slate_queue = None
            if self.use_slate_as_input:
                self.slate_queue = await self.slate_session.add_subscriber(self.key, prebuffer_bytes=256 * 1024)
            else:
                self.ingest_queue = await self.ingest_session.add_subscriber(self.key, prebuffer_bytes=256 * 1024)
            self._pending_input_chunks.clear()
            primed_bytes = 0
            if self.use_slate_as_input:
                prime_deadline = time.time() + 2.0
                target_prime_bytes = 128 * 1024
            else:
                prime_deadline = time.time() + 2.5
                target_prime_bytes = 256 * 1024
            input_queue = self.slate_queue if self.use_slate_as_input else self.ingest_queue
            while input_queue and time.time() < prime_deadline:
                probe_has_video = (
                    True
                    if self.use_slate_as_input
                    else self._probe_has_video(getattr(self.ingest_session, "current_source_probe", {}))
                )
                if probe_has_video and primed_bytes >= target_prime_bytes:
                    break
                timeout_seconds = max(0.05, prime_deadline - time.time())
                try:
                    chunk = await asyncio.wait_for(input_queue.get(), timeout=timeout_seconds)
                except asyncio.TimeoutError:
                    continue
                if chunk is None:
                    break
                primed_bytes += len(chunk)
                self._pending_input_chunks.append(chunk)
            if primed_bytes > 0:
                logger.info(
                    "CSO HLS output primed %s input channel=%s output_key=%s primed_bytes=%s pending_chunks=%s probe_has_video=%s elapsed_ms=%s",
                    "slate" if self.use_slate_as_input else "ingest",
                    self.channel_id,
                    self.key,
                    primed_bytes,
                    len(self._pending_input_chunks),
                    (
                        True
                        if self.use_slate_as_input
                        else self._probe_has_video(getattr(self.ingest_session, "current_source_probe", {}))
                    ),
                    int((time.time() - self.last_activity) * 1000),
                )
            command = CsoOutputFfmpegCommandBuilder(self.policy).build_hls_output_command(self.output_dir)
            logger.info(
                "Starting CSO HLS output channel=%s output_key=%s policy=(%s) command=%s",
                self.channel_id,
                self.key,
                _policy_log_label(self.runtime_policy),
                command,
            )
            self.process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            self.process_token += 1
            token = self.process_token
            self.running = True
            self.last_error = None
            self.last_activity = time.time()
            self.write_task = asyncio.create_task(self._write_loop(token, self.process))
            self.stderr_task = asyncio.create_task(self._stderr_loop(token, self.process))
            self.wait_task = asyncio.create_task(self._wait_loop(token, self.process))

    async def _write_loop(self, token, process):
        exit_reason = "loop_exit"
        try:
            while (
                self.running
                and token == self.process_token
                and process
                and process.stdin
                and self._pending_input_chunks
            ):
                chunk = self._pending_input_chunks.popleft()
                try:
                    process.stdin.write(chunk)
                    await process.stdin.drain()
                except Exception as exc:
                    exit_reason = f"pending_write_error:{exc}"
                    return
            active_queue = self.slate_queue if self.use_slate_as_input else self.ingest_queue
            while self.running and token == self.process_token and process and process.stdin and active_queue:
                chunk = await active_queue.get()
                if chunk is None:
                    exit_reason = "queue_eof"
                    break
                try:
                    process.stdin.write(chunk)
                    await process.stdin.drain()
                    self.last_activity = time.time()
                except Exception as exc:
                    exit_reason = f"live_write_error:{exc}"
                    break
            if not active_queue:
                exit_reason = "no_active_queue"
        finally:
            if enable_cso_output_command_debug_logging:
                logger.info(
                    "CSO HLS output writer exiting channel=%s output_key=%s reason=%s running=%s token_match=%s has_process=%s has_stdin=%s",
                    self.channel_id,
                    self.key,
                    exit_reason,
                    self.running,
                    token == self.process_token,
                    bool(process),
                    bool(process and process.stdin),
                )
            try:
                if token == self.process_token and process and process.stdin:
                    process.stdin.close()
            except Exception:
                pass

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
                if enable_cso_output_command_debug_logging:
                    logger.info("CSO HLS output ffmpeg[%s][%s]: %s", self.channel_id, self.key, rendered)
        rendered = text_buffer.strip()
        if rendered and token == self.process_token:
            self._recent_ffmpeg_stderr.append(rendered)
            if enable_cso_output_command_debug_logging:
                logger.info("CSO HLS output ffmpeg[%s][%s]: %s", self.channel_id, self.key, rendered)

    async def _wait_loop(self, token, process):
        return_code = None
        try:
            if process:
                return_code = await process.wait()
        except Exception:
            return_code = None
        finally:
            if token != self.process_token:
                return
            if not self._last_good_playlist_text:
                try:
                    await self.read_playlist_text()
                except Exception:
                    pass
            async with self.lock:
                client_count = len(self.clients)
                still_running = bool(self.running)
                has_completed_playlist = bool(self._last_good_playlist_text)
                if process is self.process:
                    self.process = None
            if (
                still_running
                and client_count > 0
                and has_completed_playlist
                and int(return_code or 0) == 0
                and self.use_slate_as_input
            ):
                if "#EXT-X-ENDLIST" not in str(self._last_good_playlist_text):
                    self._last_good_playlist_text = f"{str(self._last_good_playlist_text).rstrip()}\n#EXT-X-ENDLIST\n"
                self._retain_completed_output_until = time.time() + max(15.0, float(CSO_HLS_CLIENT_IDLE_SECONDS))
                logger.info(
                    "CSO HLS output completed and retained channel=%s output_key=%s return_code=%s clients=%s retain_seconds=%s",
                    self.channel_id,
                    self.key,
                    return_code,
                    client_count,
                    int(max(15.0, float(CSO_HLS_CLIENT_IDLE_SECONDS))),
                )
                return
            if still_running and client_count > 0 and int(return_code or 0) == 0 and not self.use_slate_as_input:
                async with self.lock:
                    self.running = False
                    self.last_error = "output_completed_restart_pending"
                logger.warning(
                    "CSO HLS live output completed unexpectedly and will restart on next request channel=%s output_key=%s clients=%s",
                    self.channel_id,
                    self.key,
                    client_count,
                )
                asyncio.create_task(self.start())
                return
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
                _policy_log_label(self.runtime_policy),
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
            _policy_log_label(self.runtime_policy),
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
            return self._last_good_playlist_text
        try:
            playlist_text = await asyncio.to_thread(self.playlist_path.read_text, "utf-8")
        except Exception:
            return self._last_good_playlist_text
        segment_names = self._playlist_segment_names(playlist_text)
        if not segment_names:
            return self._last_good_playlist_text
        for segment_name in segment_names:
            segment_path = (self.output_dir / segment_name).resolve()
            if not str(segment_path).startswith(str(self.output_dir.resolve())):
                return self._last_good_playlist_text
            if not segment_path.exists() or not segment_path.is_file():
                return self._last_good_playlist_text
            try:
                if int(segment_path.stat().st_size or 0) <= 0:
                    return self._last_good_playlist_text
            except Exception:
                return self._last_good_playlist_text
        self._last_good_playlist_text = playlist_text
        self._last_good_playlist_ts = time.time()
        return playlist_text

    async def read_segment_bytes(self, segment_name):
        name = clean_text(segment_name)
        if not name or not _SAFE_HLS_SEGMENT_RE.match(name):
            return None
        segment_path = (self.output_dir / name).resolve()
        if not str(segment_path).startswith(str(self.output_dir.resolve())):
            return None
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if segment_path.exists() and segment_path.is_file():
                try:
                    if int(segment_path.stat().st_size or 0) > 0:
                        break
                except Exception:
                    pass
            await asyncio.sleep(0.05)
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
            self.process_token += 1
            stop_token = self.process_token
            ingest_queue = self.ingest_queue
            self.ingest_queue = None
            slate_queue = self.slate_queue
            self.slate_queue = None
            self._retain_completed_output_until = 0.0
            client_count = len(self.clients)
            disconnected_clients = list(self.clients.items())
            self.clients = {}
        logger.info(
            "Stopping CSO HLS output channel=%s output_key=%s clients=%s force=%s policy=(%s)",
            self.channel_id,
            self.key,
            client_count,
            force,
            _policy_log_label(self.runtime_policy),
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
                _policy_log_label(self.runtime_policy),
            )
        try:
            if ingest_queue is not None and self.ingest_session is not None:
                await self.ingest_session.remove_subscriber(self.key)
        except Exception:
            pass
        try:
            if slate_queue is not None and self.slate_session is not None:
                await self.slate_session.remove_subscriber(self.key)
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
        async with self.lock:
            should_cleanup_output_dir = (
                self.process_token == stop_token and not self.running and not self.process and not self.clients
            )
        if should_cleanup_output_dir and self.output_dir.exists():
            await asyncio.to_thread(shutil.rmtree, self.output_dir, True)


class _SessionMap:
    def __init__(self):
        self.sessions = {}
        self.lock = asyncio.Lock()

    async def get_or_create(self, key, factory):
        async with self.lock:
            session = self.sessions.get(key)
            if session is not None:
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
        self.slate = _SessionMap()
        self.output = _SessionMap()

    async def get_or_create_ingest(self, key, factory):
        return await self.ingest.get_or_create(key, factory)

    async def get_or_create_slate(self, key, factory):
        return await self.slate.get_or_create(key, factory)

    async def get_or_create_output(self, key, factory):
        return await self.output.get_or_create(key, factory)

    async def cleanup_idle_streams(self, idle_timeout=300):
        await self.output.cleanup_idle_streams(idle_timeout=idle_timeout)
        await self.slate.cleanup_idle_streams(idle_timeout=idle_timeout)
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

    async def has_active_ingest_for_source(self, source_id):
        prefix = f"cso-source-ingest-{int(source_id)}"
        async with self.ingest.lock:
            session = self.ingest.sessions.get(prefix)
            if not session:
                return False
            return bool(session.running and session.process)

    async def has_ingest_session_for_channel(self, channel_id):
        prefix = f"cso-ingest-{int(channel_id)}"
        async with self.ingest.lock:
            return self.ingest.sessions.get(prefix) is not None

    async def has_ingest_session_for_source(self, source_id):
        prefix = f"cso-source-ingest-{int(source_id)}"
        async with self.ingest.lock:
            return self.ingest.sessions.get(prefix) is not None


cso_session_manager = CsoRuntimeManager()


def _increment_external_count(external_counts, key):
    if not key:
        return
    external_counts[key] = int(external_counts.get(key) or 0) + 1


def is_internal_cso_activity(endpoint: str, display_url: str = "") -> bool:
    endpoint_value = endpoint or ""
    display_url_value = clean_key(display_url)
    if "/tic-api/cso/channel/" in endpoint_value or "/tic-api/cso/channel_stream/" in endpoint_value:
        return True
    if endpoint_value.startswith("/tic-tvh/") and "tic-cso-" in display_url_value:
        return True
    return False


async def reconcile_cso_capacity_with_tvh_channels(channel_ids, activity_sessions=None):
    external_counts = {}
    fallback_channel_ids = set()

    for value in channel_ids or []:
        parsed = convert_to_int(value, None)
        if parsed:
            fallback_channel_ids.add(parsed)

    for session in activity_sessions or []:
        if not isinstance(session, dict):
            continue
        endpoint = clean_text(session.get("endpoint"))
        display_url = clean_key(session.get("display_url"))
        # CSO endpoint usage is already tracked via in-process allocations.
        # TVH subscriptions against CSO mux should not count as additional external usage.
        if is_internal_cso_activity(endpoint, display_url):
            continue

        xc_account_id = convert_to_int(session.get("xc_account_id"), None)
        playlist_id = convert_to_int(session.get("playlist_id"), None)
        source_id = convert_to_int(session.get("source_id"), None)
        if xc_account_id:
            _increment_external_count(external_counts, f"xc:{xc_account_id}")
            continue
        if playlist_id:
            _increment_external_count(external_counts, f"playlist:{playlist_id}")
            continue
        if source_id:
            _increment_external_count(external_counts, f"source:{source_id}")
            continue

        channel_id = convert_to_int(session.get("channel_id"), None)
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
                key=lambda item: convert_to_int(getattr(item, "priority", 0), 0),
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
    parsed = urlparse(url or "")
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
    lines = [line.strip() for line in (payload or "").splitlines() if line.strip()]
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


def subscribe_slate_stream(
    config, policy, reason, detail_hint="", profile_name="", channel=None, source=None, status_code=503
):
    allow_unavailable_slate = should_allow_unavailable_slate(profile_name, channel)
    if not CSO_UNAVAILABLE_SHOW_SLATE or not allow_unavailable_slate:
        message = (
            "Channel unavailable due to connection limits"
            if reason == "capacity_blocked"
            else "Unable to start CSO stream"
        )
        return build_cso_stream_plan(None, None, message, status_code)

    reason_key = clean_key(reason, fallback="playback_unavailable")
    resolved_duration = _cso_unavailable_duration_seconds(reason_key)
    unique_suffix = int(time.time() * 1000)
    slate_session = CsoSlateSession(
        key=f"cso-terminal-slate-{reason_key}-{unique_suffix}",
        config_path=getattr(config, "config_path", ""),
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
        cutoff_seconds=_cso_unavailable_duration_seconds(reason),
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
    allow_unavailable_slate = should_allow_unavailable_slate(profile_name, channel)
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
    resolved_duration = _cso_unavailable_duration_seconds(reason_key)

    def _slate_factory():
        return CsoSlateSession(
            key=slate_key,
            config_path=getattr(config, "config_path", ""),
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

    source_id = source.id
    channel_id = source.channel_id
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
        return build_cso_stream_plan(None, None, "No available stream source for this channel", 503)

    policy = generate_cso_policy_from_profile(config, profile)
    ingest_key = f"cso-ingest-{channel.id}"
    output_session_key = f"cso-output-{channel.id}-{profile}"
    capacity_owner_key = f"cso-channel-{channel.id}"
    username = await _resolve_username_for_stream_key(config, stream_key)
    ingest_user_agent = _resolve_cso_ingest_user_agent(config, sources[0] if sources else None)
    allow_unavailable_slate = should_allow_unavailable_slate(profile, channel)

    # Init slate pipeline
    slate_session = None
    if CSO_UNAVAILABLE_SHOW_SLATE and allow_unavailable_slate:
        slate_session = CsoSlateSession(
            f"{output_session_key}-slate",
            config_path=getattr(config, "config_path", ""),
        )

    # Init ingest pipeline
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
            slate_session=slate_session,
        )

    ingest_session = await cso_session_manager.get_or_create_ingest(ingest_key, _ingest_factory)
    ingest_session.slate_session = slate_session
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
        detail_hint = summarize_cso_playback_issue(message) if reason == "playback_unavailable" else ""
        plan = ingest_session.build_unavailable_stream_plan(
            config,
            policy,
            reason,
            detail_hint=detail_hint,
            profile_name=profile,
            channel=channel,
            status_code=503,
        )
        if plan.generator is not None:
            return plan
        return build_cso_stream_plan(None, None, message, 503)

    # Init output pipeline
    def _output_factory():
        return CsoOutputSession(
            output_session_key,
            channel.id,
            policy,
            ingest_session,
            slate_session,
        )

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
    output_session.slate_session = slate_session
    output_session.event_source = None
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
        detail_hint = summarize_cso_playback_issue(reason)
        plan = ingest_session.build_unavailable_stream_plan(
            config,
            policy,
            "playback_unavailable",
            detail_hint=detail_hint,
            profile_name=profile,
            channel=channel,
            status_code=503,
        )
        if plan.generator is not None:
            return plan
        return build_cso_stream_plan(
            None, None, "Channel unavailable because output pipeline could not be started", 503
        )

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
                        detail_hint = await latest_cso_playback_issue_hint(
                            channel.id,
                            session_id=output_session_key,
                        )
                        unavailable_plan = ingest_session.build_unavailable_stream_plan(
                            config,
                            policy,
                            "playback_unavailable",
                            detail_hint=detail_hint,
                            profile_name=profile,
                            channel=channel,
                            status_code=503,
                        )
                        if unavailable_plan.generator is not None:
                            async for unavailable_chunk in unavailable_plan.generator:
                                yield unavailable_chunk
                            break
                        break
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

    return build_cso_stream_plan(_generator(), content_type, None, 200)


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
        return build_cso_stream_plan(None, None, "Source not found", 404)

    playlist = getattr(source, "playlist", None)
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return build_cso_stream_plan(None, None, "Source playlist is disabled", 404)

    stream_url = (getattr(source, "playlist_stream_url", None) or "").strip()
    if not stream_url:
        return build_cso_stream_plan(None, None, "No available stream source for this channel", 503)

    source_id = source.id
    channel_id = source.channel_id
    sources = [source]

    policy = generate_cso_policy_from_profile(config, profile)
    ingest_key = f"cso-source-ingest-{source_id}"
    output_session_key = f"cso-source-output-{source_id}-{profile}"
    capacity_owner_key = f"cso-source-{source_id}"
    username = await _resolve_username_for_stream_key(config, stream_key)
    ingest_user_agent = _resolve_cso_ingest_user_agent(config, source)
    allow_unavailable_slate = should_allow_unavailable_slate(profile, getattr(source, "channel", None))

    # Init slate pipeline
    slate_session = None
    if CSO_UNAVAILABLE_SHOW_SLATE and allow_unavailable_slate:
        slate_session = CsoSlateSession(
            f"{output_session_key}-slate",
            config_path=getattr(config, "config_path", ""),
        )

    # Init ingest pipeline
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
            slate_session=slate_session,
        )

    ingest_session = await cso_session_manager.get_or_create_ingest(ingest_key, _ingest_factory)
    ingest_session.slate_session = slate_session
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
        detail_hint = summarize_cso_playback_issue(message) if reason == "playback_unavailable" else ""
        plan = ingest_session.build_unavailable_stream_plan(
            config,
            policy,
            reason,
            detail_hint=detail_hint,
            profile_name=profile,
            channel=getattr(source, "channel", None),
            source=source,
            status_code=503,
        )
        if plan.generator is not None:
            return plan
        return build_cso_stream_plan(None, None, message, 503)

    # Init output pipeline
    def _output_factory():
        return CsoOutputSession(
            output_session_key,
            channel_id,
            policy,
            ingest_session,
            slate_session,
            event_source=source,
        )

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
    output_session.slate_session = slate_session
    output_session.event_source = source
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
        detail_hint = summarize_cso_playback_issue(reason)
        plan = ingest_session.build_unavailable_stream_plan(
            config,
            policy,
            "playback_unavailable",
            detail_hint=detail_hint,
            profile_name=profile,
            channel=getattr(source, "channel", None),
            source=source,
            status_code=503,
        )
        if plan.generator is not None:
            return plan
        return build_cso_stream_plan(
            None, None, "Channel unavailable because output pipeline could not be started", 503
        )

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
                        detail_hint = await latest_cso_playback_issue_hint(
                            channel_id,
                            session_id=output_session_key,
                        )
                        unavailable_plan = ingest_session.build_unavailable_stream_plan(
                            config,
                            policy,
                            "playback_unavailable",
                            detail_hint=detail_hint,
                            profile_name=profile,
                            channel=getattr(source, "channel", None),
                            source=source,
                            status_code=503,
                        )
                        if unavailable_plan.generator is not None:
                            async for unavailable_chunk in unavailable_plan.generator:
                                yield unavailable_chunk
                            break
                        break
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

    return build_cso_stream_plan(_generator(), content_type, None, 200)


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
