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
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

import aiohttp
import aiofiles
import requests
import urllib3
from sqlalchemy import delete
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from backend.models import (
    Session,
    Channel,
    ChannelSource,
    CsoEventLog,
    Playlist,
    XcAccount,
    VodCategoryEpisode,
    VodCategoryItem,
    XcVodItem,
)
from backend.streaming import (
    LOCAL_PROXY_HOST_PLACEHOLDER,
    append_stream_key,
    is_local_hls_proxy_url,
)
from backend.vod import VodCuratedPlaybackCandidate, VodSourcePlaybackCandidate
from backend.stream_profiles import generate_cso_policy_from_profile, resolve_cso_profile_name
from backend.users import get_user_by_stream_key
from backend.config import (
    enable_cso_ingest_command_debug_logging,
    enable_cso_output_command_debug_logging,
    enable_cso_slate_command_debug_logging,
)
from backend.http_headers import parse_headers_json, sanitise_headers
from backend.source_media import load_source_media_shape, persist_source_media_shape
from backend.utils import clean_key, clean_text, convert_to_int, utc_now_naive
from backend.xc_hosts import parse_xc_hosts

logger = logging.getLogger("cso")
CS_VOD_USE_PROXY_SESSION = True
CSO_SOURCE_HOLD_DOWN_SECONDS = 20
CSO_SOURCE_FAILURE_CACHE_TTL_SECONDS = 5 * 60
CSO_SOURCE_FAILURE_PRIORITY_PENALTY = 1000
CSO_UNHEALTHY_SOURCE_PRIORITY_PENALTY = 5
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
CSO_OUTPUT_CLIENT_STALE_SECONDS = 15.0
CSO_OUTPUT_CLIENT_STALE_SECONDS_TVH = 20.0
CSO_HLS_SEGMENT_SECONDS = 3
CSO_HLS_LIST_SIZE = 5
CSO_HLS_CLIENT_IDLE_SECONDS = max(10, int(CSO_HLS_SEGMENT_SECONDS) * 3)
CSO_OUTPUT_CLIENT_QUEUE_MAX_BYTES = 90_000_000
CSO_INGEST_SUBSCRIBER_QUEUE_MAX_BYTES = 90_000_000
CSO_INGEST_HISTORY_MAX_BYTES = 16 * 1024 * 1024
CSO_INGEST_SUBSCRIBER_PREBUFFER_BYTES = 512 * 1024
CSO_INGEST_PROBE_SIZE_BYTES = 2 * 1024 * 1024
CSO_INGEST_ANALYSE_DURATION_US = 3_000_000
CSO_INGEST_FPS_PROBE_SIZE = 64
VOD_CHANNEL_NEXT_SEGMENT_PRESTART_SECONDS = 5
VOD_CHANNEL_NEXT_SEGMENT_BUFFER_BYTES = 24 * 1024 * 1024
CSO_OUTPUT_PROBE_SIZE_BYTES = 1 * 1024 * 1024
CSO_OUTPUT_ANALYSE_DURATION_US = 2_000_000
CSO_OUTPUT_FPS_PROBE_SIZE = 32
CSO_CONSUMER_PROGRESS_LOG_INTERVAL_SECONDS = 10
CSO_OUTPUT_SLATE_POLL_INTERVAL_SECONDS = 0.25
CSO_OUTPUT_PIPE_POLL_INTERVAL_SECONDS = 0.25
MPEGTS_PACKET_SIZE_BYTES = 188
MPEGTS_CHUNK_BYTES = MPEGTS_PACKET_SIZE_BYTES * 87
VOD_CACHE_ROOT = Path("/timeshift/vod")
VOD_CACHE_TTL_SECONDS = 10 * 60
VOD_CACHE_CHUNK_BYTES = 64 * 1024
VOD_CACHE_METADATA_TIMEOUT_SECONDS = 10
VOD_HEAD_PROBE_STATE_TTL_SECONDS = 7 * 24 * 60 * 60
CSO_UNAVAILABLE_REASON_DURATIONS_SECONDS = {
    "default": 10,
    "capacity_blocked": 10,
    "playback_unavailable": 3,
    "startup_pending": 30,
}
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
_cso_channel_failed_sources: dict[int, dict[int, float]] = {}
_cso_channel_failed_sources_lock = asyncio.Lock()

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


def _vod_head_probe_state_path() -> Path:
    home_dir = os.environ.get("HOME_DIR") or os.path.expanduser("~")
    return Path(home_dir) / ".tvh_iptv_config" / "cache" / "vod_head_probe_state.json"


def _vod_head_probe_cache_key(source: "CsoSource", upstream_url: str) -> str:
    source_id = int(source.id or 0)
    parsed = urlparse(upstream_url or "")
    source_host = clean_text(parsed.netloc)
    return f"{source.source_type}:{source.playlist_id}:{source_id}:{source_host}"


class VodHeadProbeStateEntry(TypedDict):
    expires_at: int
    failure_reason: str
    head_supported: bool
    last_failure_at: int


class VodHeadProbeStateStore:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._state: dict[str, VodHeadProbeStateEntry] | None = None

    async def _load_state(self) -> dict[str, VodHeadProbeStateEntry]:
        if self._state is not None:
            return self._state
        path = _vod_head_probe_state_path()
        payload: Any = {}
        if path.exists():
            try:
                payload = json.loads(await asyncio.to_thread(path.read_text, encoding="utf-8")) or {}
            except Exception:
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        cleaned: dict[str, VodHeadProbeStateEntry] = {}
        now_ts = int(time.time())
        for key, value in payload.items():
            if not isinstance(key, str) or not isinstance(value, dict):
                continue
            expires_at = convert_to_int(value.get("expires_at"), 0)
            if expires_at > 0 and expires_at < now_ts:
                continue
            cleaned[key] = {
                "expires_at": expires_at,
                "failure_reason": clean_text(value.get("failure_reason")) or "head_failed",
                "head_supported": bool(value.get("head_supported")),
                "last_failure_at": convert_to_int(value.get("last_failure_at"), 0),
            }
        self._state = cleaned
        return self._state

    async def _write_state(self, state: dict[str, VodHeadProbeStateEntry]):
        path = _vod_head_probe_state_path()
        await asyncio.to_thread(path.parent.mkdir, 0o755, True, True)
        payload = json.dumps(state, indent=2, sort_keys=True)
        await asyncio.to_thread(path.write_text, payload, encoding="utf-8")

    async def should_skip_head(self, source: "CsoSource", upstream_url: str) -> bool:
        async with self._lock:
            state = await self._load_state()
            entry = state.get(_vod_head_probe_cache_key(source, upstream_url))
            if entry is None:
                return False
            return entry["head_supported"] is False

    async def mark_head_failed(self, source: "CsoSource", upstream_url: str, reason: str):
        async with self._lock:
            state = await self._load_state()
            now_ts = int(time.time())
            state[_vod_head_probe_cache_key(source, upstream_url)] = {
                "head_supported": False,
                "last_failure_at": now_ts,
                "failure_reason": clean_text(reason) or "head_failed",
                "expires_at": now_ts + VOD_HEAD_PROBE_STATE_TTL_SECONDS,
            }
            await self._write_state(state)

    async def mark_head_supported(self, source: "CsoSource", upstream_url: str):
        async with self._lock:
            state = await self._load_state()
            key = _vod_head_probe_cache_key(source, upstream_url)
            if key in state:
                state.pop(key, None)
                await self._write_state(state)


vod_head_probe_state_store = VodHeadProbeStateStore()


@dataclass
class CsoSource:
    """
    Unified adapter for all CSO ingest sources (Live TV, VOD, etc).
    Decouples the core streaming engine from database models.
    """

    id: int
    source_type: str  # "channel", "vod_movie", or "vod_episode"
    url: str
    playlist_id: int
    playlist: object | None = None
    xc_account_id: int | None = None
    xc_account: object | None = None
    priority: int = 0
    channel_id: int | None = None
    internal_id: int | None = None
    cache_internal_id: int | None = None
    use_hls_proxy: bool = False
    probe_details: dict | None = None
    probe_at: object | None = None
    container_extension: str | None = None

    @property
    def playlist_stream_url(self):
        return self.url


async def cso_source_from_vod_source(
    candidate: VodCuratedPlaybackCandidate | VodSourcePlaybackCandidate, upstream_url: str
) -> CsoSource:
    """Wrap a VOD item/episode candidate in a CsoSource adapter."""
    source_item = candidate.source_item
    if isinstance(candidate, VodCuratedPlaybackCandidate):
        episode_source = candidate.episode_source
        group_item = candidate.group_item
        episode_item = candidate.episode
    else:
        episode_source = None
        group_item = None
        episode_item = None

    source_playlist_id = convert_to_int(source_item.playlist_id, 0)
    async with Session() as session:
        playlist = await session.get(Playlist, source_playlist_id)

    xc_account = candidate.xc_account
    if xc_account is None:
        async with Session() as session:
            result = await session.execute(
                select(XcAccount)
                .where(XcAccount.playlist_id == source_playlist_id, XcAccount.enabled.is_(True))
                .order_by(XcAccount.id.asc())
            )
            xc_account = result.scalars().first()

    source_type = "vod_movie" if candidate.content_type == "movie" else "vod_episode"

    probe_details = None
    probe_at = None
    source_id = convert_to_int(source_item.id, 0)
    if candidate.content_type == "movie":
        probe_details = json.loads(source_item.stream_probe_details) if source_item.stream_probe_details else None
        probe_at = source_item.stream_probe_at

    internal_id = None
    cache_internal_id = None
    container_extension = source_item.container_extension
    if isinstance(candidate, VodCuratedPlaybackCandidate):
        if episode_item is not None:
            probe_details = json.loads(episode_item.stream_probe_details) if episode_item.stream_probe_details else None
            probe_at = episode_item.stream_probe_at
        if episode_source:
            source_id = convert_to_int(episode_source.id, 0)
            container_extension = episode_source.container_extension or container_extension
        group_item_is_curated = bool(group_item is not None and not convert_to_int(group_item.playlist_id, 0))
        episode_item_is_curated = bool(episode_item is not None and not convert_to_int(episode_item.playlist_id, 0))
        if group_item is not None:
            container_extension = container_extension or group_item.container_extension
        if candidate.content_type == "movie":
            if group_item_is_curated:
                internal_id = convert_to_int(group_item.id, 0) or None
            cache_internal_id = internal_id or source_id or None
        else:
            if episode_item_is_curated:
                internal_id = convert_to_int(episode_item.id, 0) or None
            cache_internal_id = internal_id or source_id or None
    else:
        if candidate.content_type == "series":
            source_id = convert_to_int(candidate.cache_internal_id, 0) or source_id
        internal_id = candidate.internal_id
        cache_internal_id = candidate.cache_internal_id or source_id or None
        container_extension = container_extension or candidate.container_extension

    xc_account_id = None
    if xc_account is not None:
        xc_account_id = xc_account.id

    return CsoSource(
        id=source_id,
        source_type=source_type,
        url=str(upstream_url),
        playlist_id=source_playlist_id,
        playlist=playlist,
        xc_account_id=xc_account_id,
        xc_account=xc_account,
        use_hls_proxy=False,
        priority=0,
        channel_id=None,
        internal_id=internal_id,
        cache_internal_id=cache_internal_id,
        probe_details=probe_details,
        probe_at=probe_at,
        container_extension=clean_key(container_extension) or None,
    )


def cso_source_from_channel_source(source: ChannelSource) -> CsoSource:
    """Wrap a ChannelSource database model in a CsoSource adapter."""
    probe_details = None
    try:
        if hasattr(source, "stream_probe_details") and source.stream_probe_details:
            probe_details = json.loads(source.stream_probe_details)
    except Exception:
        pass

    return CsoSource(
        id=int(source.id),
        source_type="channel",
        url=str(getattr(source, "playlist_stream_url", "") or ""),
        playlist_id=int(getattr(source, "playlist_id", 0) or 0),
        playlist=getattr(source, "playlist", None),
        xc_account_id=getattr(source, "xc_account_id", None),
        xc_account=getattr(source, "xc_account", None),
        priority=convert_to_int(getattr(source, "priority", 0), 0),
        channel_id=getattr(source, "channel_id", None),
        use_hls_proxy=bool(getattr(source, "use_hls_proxy", False)),
        probe_details=probe_details,
        probe_at=getattr(source, "stream_probe_at", None),
        container_extension=None,
    )


async def _get_cso_channel_failed_source_ids(channel_id: int | str | None) -> set[int]:
    try:
        channel_id = int(channel_id)
    except Exception:
        return set()
    if channel_id <= 0:
        return set()

    now = time.time()
    async with _cso_channel_failed_sources_lock:
        failed_map = _cso_channel_failed_sources.get(channel_id) or {}
        active_ids = set()
        expired_ids = []
        for source_id, hold_until in failed_map.items():
            if float(hold_until or 0.0) > now:
                active_ids.add(int(source_id))
            else:
                expired_ids.append(int(source_id))
        for source_id in expired_ids:
            failed_map.pop(source_id, None)
        if failed_map:
            _cso_channel_failed_sources[channel_id] = failed_map
        else:
            _cso_channel_failed_sources.pop(channel_id, None)
        return active_ids


async def mark_cso_channel_source_temporarily_failed(
    channel_id: int | str | None, source_id: int | str | None, ttl_seconds: float = CSO_SOURCE_FAILURE_CACHE_TTL_SECONDS
) -> bool:
    try:
        channel_id = int(channel_id)
        source_id = int(source_id)
    except Exception:
        return False
    if channel_id <= 0 or source_id <= 0:
        return False

    hold_until = time.time() + max(1.0, float(ttl_seconds or CSO_SOURCE_FAILURE_CACHE_TTL_SECONDS))
    async with _cso_channel_failed_sources_lock:
        failed_map = _cso_channel_failed_sources.setdefault(channel_id, {})
        failed_map[source_id] = hold_until
    logger.info(
        "CSO source failure cache updated channel=%s source_id=%s ttl_seconds=%s",
        channel_id,
        source_id,
        int(max(1.0, float(ttl_seconds or CSO_SOURCE_FAILURE_CACHE_TTL_SECONDS))),
    )
    return True


def _cso_source_effective_priority(source: CsoSource, failed_source_ids: Iterable[int] | None = None) -> int:
    base_priority = convert_to_int(getattr(source, "priority", None), 0)
    source_id = source.id
    health_status = str(getattr(source, "last_health_check_status", "") or "").strip().lower()
    unhealthy_penalty = CSO_UNHEALTHY_SOURCE_PRIORITY_PENALTY if health_status == "unhealthy" else 0
    temporary_penalty = CSO_SOURCE_FAILURE_PRIORITY_PENALTY if source_id in set(failed_source_ids or set()) else 0
    return base_priority - unhealthy_penalty - temporary_penalty


async def order_cso_channel_sources(
    sources: Iterable[CsoSource], channel_id: int | str | None = None
) -> list[CsoSource]:
    candidates = list(sources or [])
    resolved_channel_id = channel_id
    if resolved_channel_id is None:
        for source in candidates:
            resolved_channel_id = getattr(source, "channel_id", None)
            if resolved_channel_id is not None:
                break
    failed_source_ids = await _get_cso_channel_failed_source_ids(resolved_channel_id)
    return sorted(
        candidates,
        key=lambda item: (
            _cso_source_effective_priority(item, failed_source_ids=failed_source_ids),
            convert_to_int(getattr(item, "priority", None), 0),
            -int(item.id or 0),
        ),
        reverse=True,
    )


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


def cso_runtime_capabilities() -> dict[str, bool]:
    return {
        "vaapi_available": bool(detect_vaapi_device_path()),
    }


def policy_content_type(policy: dict[str, Any] | None) -> str:
    container = (policy or {}).get("container", "mpegts")
    return CONTAINER_TO_CONTENT_TYPE.get(container, "application/octet-stream")


def policy_ffmpeg_format(policy: dict[str, Any] | None) -> str:
    container = (policy or {}).get("container", "mpegts")
    return CONTAINER_TO_FORMAT.get(container, "mpegts")


def _resolve_cso_output_policy(
    policy: dict[str, Any] | None, use_slate_as_input: bool = False
) -> dict[str, Any]:
    resolved = dict(policy or {})
    if use_slate_as_input:
        resolved["output_mode"] = "force_remux"
        resolved["container"] = "mpegts"
        resolved["video_codec"] = "copy"
        resolved["audio_codec"] = "copy"
        resolved["subtitle_mode"] = "copy"
    return resolved


def _generate_vod_channel_ingest_policy(config: Any, output_policy: dict[str, Any] | None = None) -> dict[str, Any]:
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


def _resolve_vod_channel_output_policy(
    policy: dict[str, Any] | None, ingest_policy: dict[str, Any]
) -> dict[str, Any]:
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


def _resolve_vod_pipe_container(source: CsoSource | None, source_probe: dict[str, Any] | None = None) -> str:
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

    # Unsafe or poorly pipe-friendly source containers should be remuxed to Matroska for the CSO pipe.
    return "matroska"


def source_capacity_key(source: CsoSource):
    if source.xc_account_id:
        return f"xc:{int(source.xc_account_id)}"
    if source.playlist_id:
        return f"playlist:{int(source.playlist_id)}"
    return f"source:{int(source.id or 0)}"


def source_capacity_limit(source: CsoSource):
    xc_account = source.xc_account
    if xc_account:
        try:
            return max(0, int(getattr(xc_account, "connection_limit", 0) or 0))
        except Exception:
            return 0
    playlist = source.playlist
    if playlist:
        try:
            return max(0, int(getattr(playlist, "connections", 0) or 0))
        except Exception:
            return 0
    return 1_000_000


def source_should_use_cso_buffer(source: CsoSource, force_tvh_remux=False):
    if force_tvh_remux:
        return True
    playlist = source.playlist if source is not None else None
    return bool(
        getattr(source, "use_hls_proxy", False) and playlist and getattr(playlist, "hls_proxy_use_ffmpeg", False)
    )


def _source_event_context(source: CsoSource, source_url=None):
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
    headers: dict | None = None
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


def _vod_passthrough_profile_for_source(candidate) -> str:
    source_container = clean_key(
        getattr(getattr(candidate, "episode_source", None), "container_extension", "")
        or getattr(getattr(candidate, "source_item", None), "container_extension", "")
        or getattr(getattr(candidate, "group_item", None), "container_extension", "")
    )
    return {
        "ts": "mpegts",
        "mpegts": "mpegts",
        "mkv": "matroska",
        "matroska": "matroska",
        "mp4": "mp4",
        "webm": "webm",
    }.get(source_container, "")


def should_use_vod_proxy_session(candidate, requested_profile: str) -> bool:
    if not CS_VOD_USE_PROXY_SESSION:
        return False
    profile_name = clean_key(requested_profile)
    if not candidate or profile_name in {"", "hls"}:
        return False
    return profile_name == _vod_passthrough_profile_for_source(candidate)


def _filter_vod_proxy_request_headers(request_headers, source: CsoSource):
    headers = {}
    hop_by_hop = {
        "host",
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        "content-length",
    }
    allowed_passthrough = {"range", "if-range"}
    for key, value in (_resolve_cso_ingest_headers(source) or {}).items():
        key_name = str(key or "").strip()
        if not key_name or clean_key(key_name) in hop_by_hop:
            continue
        text_value = clean_text(value)
        if text_value:
            headers[key_name] = text_value
    for key, value in (request_headers or {}).items():
        key_name = str(key or "").strip()
        lowered = clean_key(key_name)
        if not key_name or lowered in hop_by_hop or lowered not in allowed_passthrough:
            continue
        headers[key_name] = str(value or "")
    user_agent = _resolve_cso_ingest_user_agent(None, source)
    if user_agent and "User-Agent" not in headers:
        headers["User-Agent"] = user_agent
    return headers


def _sanitise_proxy_accept_ranges(value):
    text = clean_text(value)
    if not text:
        return None
    return "bytes" if "bytes" in text.lower() else None


def _proxy_response_headers(status_code, upstream_headers, request_headers=None):
    response_status = int(status_code or 200)
    client_requested_range = bool(_header_value(request_headers, "Range"))
    headers = {}

    content_type = clean_text(_header_value(upstream_headers, "Content-Type"))
    if content_type:
        headers["Content-Type"] = content_type

    cache_control = clean_text(_header_value(upstream_headers, "Cache-Control"))
    if cache_control:
        headers["Cache-Control"] = cache_control

    content_disposition = clean_text(_header_value(upstream_headers, "Content-Disposition"))
    if content_disposition:
        headers["Content-Disposition"] = content_disposition

    etag = clean_text(_header_value(upstream_headers, "ETag"))
    if etag:
        headers["ETag"] = etag

    last_modified = clean_text(_header_value(upstream_headers, "Last-Modified"))
    if last_modified:
        headers["Last-Modified"] = last_modified

    accept_ranges = _sanitise_proxy_accept_ranges(_header_value(upstream_headers, "Accept-Ranges"))
    content_range = clean_text(_header_value(upstream_headers, "Content-Range"))
    content_length = clean_text(_header_value(upstream_headers, "Content-Length"))

    if accept_ranges:
        headers["Accept-Ranges"] = accept_ranges
    elif content_range or client_requested_range or response_status in {206, 416}:
        headers["Accept-Ranges"] = "bytes"

    if response_status in {206, 416} and content_range:
        headers["Content-Range"] = content_range
        if content_length:
            headers["Content-Length"] = content_length
    elif response_status not in {204, 304} and content_length:
        headers["Content-Length"] = content_length

    return headers


def _parse_range_request(range_header: str | None, total_size: int | None = None):
    text = clean_text(range_header)
    if not text or not text.lower().startswith("bytes="):
        return None
    spec = text[6:].strip()
    if "," in spec or "-" not in spec:
        return None
    start_text, end_text = spec.split("-", 1)
    start = None
    end = None
    if start_text.strip():
        if not start_text.strip().isdigit():
            return None
        start = int(start_text.strip())
    if end_text.strip():
        if not end_text.strip().isdigit():
            return None
        end = int(end_text.strip())
    if total_size is not None:
        if start is None:
            suffix = int(end or 0)
            if suffix <= 0:
                return None
            if suffix >= total_size:
                start = 0
            else:
                start = max(0, total_size - suffix)
            end = max(0, total_size - 1)
        else:
            if start >= total_size:
                return {"unsatisfied": True, "start": start, "end": None}
            if end is None or end >= total_size:
                end = total_size - 1
        if end is not None and start is not None and end < start:
            return None
    return {"start": start, "end": end, "raw": text}


def _is_from_start_request(request_headers=None):
    parsed = _parse_range_request(_header_value(request_headers, "Range"))
    if parsed is None:
        return True
    start = parsed.get("start")
    return start in {None, 0}


def _vod_cache_asset_parts(source: CsoSource):
    source_type = clean_key(source.source_type)
    internal_id = int(source.cache_internal_id or source.internal_id or 0)
    if source_type == "vod_episode":
        return "episode", internal_id
    return "movie", internal_id


def _vod_cache_asset_key(source: CsoSource):
    asset_kind, internal_id = _vod_cache_asset_parts(source)
    return f"{asset_kind}:{internal_id}"


def _vod_cache_paths(source: CsoSource):
    asset_kind, internal_id = _vod_cache_asset_parts(source)
    final_path = VOD_CACHE_ROOT / asset_kind / str(internal_id)
    return final_path, final_path.with_name(f"{final_path.name}.part")


def _vod_content_type_for_source(source: CsoSource):
    extension = clean_key(getattr(source, "container_extension", ""))
    if extension:
        return CONTAINER_TO_CONTENT_TYPE.get(extension)
    return None


def _build_vod_local_response_headers(total_size: int, metadata_headers=None, start=0, end=None, include_length=True):
    headers = {}
    meta = dict(metadata_headers or {})
    for key in ("Content-Type", "Cache-Control", "Content-Disposition", "ETag", "Last-Modified"):
        value = clean_text(meta.get(key))
        if value:
            headers[key] = value
    headers["Accept-Ranges"] = "bytes"
    if end is None:
        end = max(0, int(total_size or 0) - 1)
    if include_length:
        headers["Content-Length"] = str(max(0, int(end) - int(start) + 1))
    if start > 0 or end < max(0, int(total_size or 0) - 1):
        headers["Content-Range"] = f"bytes {int(start)}-{int(end)}/{int(total_size)}"
    return headers


@dataclass
class VodCacheEntry:
    key: str
    source: CsoSource
    upstream_url: str
    final_path: Path
    part_path: Path
    expected_size: int | None = None
    bytes_written: int = 0
    complete: bool = False
    failed_reason: str | None = None
    metadata_headers: dict | None = None
    content_type: str | None = None
    last_access_ts: float = 0.0
    active_sessions: int = 0
    active_readers: int = 0
    downloader_owner_key: str | None = None
    download_task: asyncio.Task | None = None
    probe_lock: asyncio.Lock | None = None
    state_lock: asyncio.Lock | None = None
    ready_event: asyncio.Event | None = None
    progress_event: asyncio.Event | None = None

    def __post_init__(self):
        if self.probe_lock is None:
            self.probe_lock = asyncio.Lock()
        if self.state_lock is None:
            self.state_lock = asyncio.Lock()
        if self.ready_event is None:
            self.ready_event = asyncio.Event()
        if self.progress_event is None:
            self.progress_event = asyncio.Event()
        if not self.last_access_ts:
            self.last_access_ts = time.time()

    def touch(self):
        self.last_access_ts = time.time()

    @property
    def downloader_running(self):
        return self.download_task is not None and not self.download_task.done()


class VodCacheManager:
    def __init__(self):
        self.entries = {}
        self.lock = asyncio.Lock()

    async def get(self, source: CsoSource):
        key = _vod_cache_asset_key(source)
        async with self.lock:
            entry = self.entries.get(key)
            if entry is not None:
                entry.touch()
            return entry

    async def get_or_create(self, source: CsoSource, upstream_url: str):
        key = _vod_cache_asset_key(source)
        async with self.lock:
            entry = self.entries.get(key)
            if entry is None:
                final_path, part_path = _vod_cache_paths(source)
                entry = VodCacheEntry(
                    key=key,
                    source=source,
                    upstream_url=clean_text(upstream_url),
                    final_path=final_path,
                    part_path=part_path,
                )
                self.entries[key] = entry
            else:
                entry.upstream_url = clean_text(upstream_url) or entry.upstream_url
                entry.source = source
                if entry.complete and entry.final_path.exists() and not entry.expected_size:
                    try:
                        entry.expected_size = int(entry.final_path.stat().st_size or 0)
                    except Exception:
                        entry.expected_size = None
            if entry.complete and not entry.content_type:
                entry.content_type = _vod_content_type_for_source(source)
            entry.touch()
            return entry

    async def import_existing_files(self):
        now_ts = time.time()
        imported = 0
        removed_parts = 0
        async with self.lock:
            for asset_kind in ("movie", "episode"):
                asset_dir = VOD_CACHE_ROOT / asset_kind
                if not asset_dir.exists() or not asset_dir.is_dir():
                    continue
                for path in sorted(asset_dir.iterdir()):
                    if not path.is_file():
                        continue
                    if path.suffix == ".part":
                        try:
                            path.unlink(missing_ok=True)
                            removed_parts += 1
                        except Exception:
                            logger.warning("Failed to remove orphaned VOD cache part file path=%s", path)
                        continue
                    file_name = clean_text(path.name)
                    if not file_name.isdigit():
                        continue
                    internal_id = int(file_name)
                    key = f"{asset_kind}:{internal_id}"
                    expected_size = 0
                    try:
                        expected_size = int(path.stat().st_size or 0)
                    except Exception:
                        expected_size = 0
                    if expected_size <= 0:
                        continue
                    source_type = "vod_movie" if asset_kind == "movie" else "vod_episode"
                    source = CsoSource(
                        id=internal_id,
                        source_type=source_type,
                        url="",
                        playlist_id=0,
                        internal_id=internal_id,
                    )
                    entry = self.entries.get(key)
                    if entry is None:
                        entry = VodCacheEntry(
                            key=key,
                            source=source,
                            upstream_url="",
                            final_path=path,
                            part_path=path.with_name(f"{path.name}.part"),
                        )
                        self.entries[key] = entry
                    else:
                        entry.source = source
                        entry.final_path = path
                        entry.part_path = path.with_name(f"{path.name}.part")
                    entry.expected_size = expected_size
                    entry.bytes_written = expected_size
                    entry.complete = True
                    entry.failed_reason = None
                    entry.metadata_headers = entry.metadata_headers or {}
                    entry.content_type = entry.content_type or _vod_content_type_for_source(source)
                    entry.last_access_ts = now_ts
                    imported += 1
        if imported or removed_parts:
            logger.info(
                "Imported existing VOD cache files imported=%s removed_orphan_parts=%s root=%s",
                imported,
                removed_parts,
                VOD_CACHE_ROOT,
            )
        return {"imported": imported, "removed_orphan_parts": removed_parts}

    async def attach_session(self, entry: VodCacheEntry):
        async with entry.state_lock:
            entry.active_sessions = int(entry.active_sessions or 0) + 1
            entry.touch()

    async def detach_session(self, entry: VodCacheEntry):
        task_to_cancel = None
        async with entry.state_lock:
            entry.active_sessions = max(0, int(entry.active_sessions or 0) - 1)
            entry.touch()
            if (
                int(entry.active_sessions or 0) <= 0
                and int(entry.active_readers or 0) <= 0
                and not entry.complete
                and entry.download_task is not None
                and not entry.download_task.done()
            ):
                task_to_cancel = entry.download_task
        if task_to_cancel is None:
            return
        task_to_cancel.cancel()
        try:
            await task_to_cancel
        except BaseException:
            pass
        async with entry.state_lock:
            entry.bytes_written = 0
            entry.failed_reason = "cancelled_no_clients"
            entry.ready_event.set()
            entry.progress_event.set()
        if entry.part_path.exists():
            await asyncio.to_thread(entry.part_path.unlink, True)

    async def cleanup(self, idle_seconds=VOD_CACHE_TTL_SECONDS):
        now_ts = time.time()
        async with self.lock:
            entries = list(self.entries.values())
        removed = 0
        for entry in entries:
            if entry.downloader_running or entry.active_readers > 0 or entry.active_sessions > 0:
                continue
            if (now_ts - float(entry.last_access_ts or 0)) < max(30, int(idle_seconds or 0)):
                continue
            await self._remove_entry(entry)
            removed += 1
        return removed

    async def _remove_entry(self, entry: VodCacheEntry):
        async with entry.state_lock:
            task = entry.download_task
            entry.download_task = None
        if task and not task.done():
            task.cancel()
            try:
                await task
            except BaseException:
                pass
        if entry.final_path.exists():
            await asyncio.to_thread(entry.final_path.unlink, True)
        if entry.part_path.exists():
            await asyncio.to_thread(entry.part_path.unlink, True)
        async with self.lock:
            current = self.entries.get(entry.key)
            if current is entry:
                self.entries.pop(entry.key, None)


vod_cache_manager = VodCacheManager()


async def _probe_vod_cache_metadata(source: CsoSource, upstream_url: str, request_headers=None):
    headers = _filter_vod_proxy_request_headers(request_headers, source)
    headers.pop("Range", None)
    timeout = aiohttp.ClientTimeout(total=VOD_CACHE_METADATA_TIMEOUT_SECONDS, connect=10, sock_connect=10, sock_read=10)
    async with aiohttp.ClientSession(timeout=timeout, auto_decompress=False) as session:
        skip_head = await vod_head_probe_state_store.should_skip_head(source, upstream_url)
        if skip_head:
            logger.debug(
                "Skipping VOD cache metadata HEAD probe source_id=%s upstream_url=%s due to cached unsupported state",
                source.id,
                upstream_url,
            )
        else:
            try:
                response = await session.request("HEAD", upstream_url, headers=headers, allow_redirects=True)
                try:
                    if int(response.status or 0) == 200:
                        size_header = clean_text(response.headers.get("Content-Length"))
                        if size_header.isdigit():
                            await vod_head_probe_state_store.mark_head_supported(source, upstream_url)
                            return {
                                "size": int(size_header),
                                "headers": dict(response.headers),
                                "status": int(response.status or 200),
                            }
                finally:
                    await response.release()
            except Exception as exc:
                logger.info(
                    "VOD cache metadata HEAD probe failed source_id=%s upstream_url=%s error=%s",
                    source.id,
                    upstream_url,
                    exc,
                )
                await vod_head_probe_state_store.mark_head_failed(source, upstream_url, str(exc))
        response = await session.get(
            upstream_url,
            headers={**headers, "Range": "bytes=0-0"},
            allow_redirects=True,
        )
        try:
            content_range = clean_text(response.headers.get("Content-Range"))
            total_size = None
            if "/" in content_range:
                tail = content_range.rsplit("/", 1)[-1].strip()
                if tail.isdigit():
                    total_size = int(tail)
            if total_size:
                return {
                    "size": int(total_size),
                    "headers": dict(response.headers),
                    "status": int(response.status or 206),
                }
        finally:
            await response.release()
    return {"size": None, "headers": {}, "status": 0}


async def _vod_cache_has_space(required_bytes: int):
    if required_bytes <= 0:
        return False
    usage = await asyncio.to_thread(shutil.disk_usage, str(VOD_CACHE_ROOT.parent))
    return int(usage.free or 0) >= int(required_bytes)


async def _ensure_vod_cache_ready(
    entry: VodCacheEntry,
    request_headers=None,
    require_size=False,
):
    async with entry.probe_lock:
        entry.touch()
        if entry.complete and entry.final_path.exists() and entry.expected_size:
            return {
                "cacheable": True,
                "size_known": True,
                "expected_size": int(entry.expected_size),
                "complete": True,
            }
        if entry.expected_size and entry.metadata_headers is not None:
            return {
                "cacheable": True,
                "size_known": True,
                "expected_size": int(entry.expected_size),
                "complete": False,
            }
        probe = await _probe_vod_cache_metadata(entry.source, entry.upstream_url, request_headers=request_headers)
        expected_size = int(probe.get("size") or 0)
        if expected_size <= 0:
            entry.failed_reason = "size_unknown"
            if require_size:
                return {
                    "cacheable": False,
                    "size_known": False,
                    "expected_size": None,
                    "reason": "size_unknown",
                }
            return {
                "cacheable": False,
                "size_known": False,
                "expected_size": None,
                "reason": "size_unknown",
            }
        has_space = await _vod_cache_has_space(expected_size * 2)
        if not has_space:
            entry.failed_reason = "insufficient_space"
            return {
                "cacheable": False,
                "size_known": True,
                "expected_size": expected_size,
                "reason": "insufficient_space",
            }
        entry.expected_size = expected_size
        entry.metadata_headers = _proxy_response_headers(int(probe.get("status") or 200), probe.get("headers") or {})
        entry.content_type = clean_text(_header_value(probe.get("headers") or {}, "Content-Type")) or None
        return {
            "cacheable": True,
            "size_known": True,
            "expected_size": expected_size,
            "complete": False,
        }


async def _start_vod_cache_download(entry: VodCacheEntry, owner_key: str, request_headers=None):
    async with entry.state_lock:
        if entry.complete:
            return True
        if entry.downloader_running:
            return True
        if not entry.expected_size:
            return False
        reserved = await cso_capacity_registry.try_reserve(
            source_capacity_key(entry.source),
            owner_key,
            source_capacity_limit(entry.source),
            slot_id=owner_key,
        )
        if not reserved:
            entry.failed_reason = "capacity_blocked"
            return False
        entry.downloader_owner_key = owner_key
        entry.failed_reason = None
        entry.ready_event.clear()
        entry.progress_event.clear()
        entry.download_task = asyncio.create_task(
            _run_vod_cache_download(entry, owner_key, request_headers=request_headers),
            name=f"vod-cache-{entry.key}",
        )
        return True


async def _run_vod_cache_download(entry: VodCacheEntry, owner_key: str, request_headers=None):
    headers = _filter_vod_proxy_request_headers(request_headers, entry.source)
    headers["Range"] = "bytes=0-"
    await asyncio.to_thread(entry.part_path.parent.mkdir, 0o755, True, True)
    http_session = None
    response = None
    iterator = None
    try:
        if entry.part_path.exists():
            await asyncio.to_thread(entry.part_path.unlink, True)
        http_session = requests.Session()
        response = await asyncio.to_thread(
            lambda: http_session.get(
                entry.upstream_url,
                headers=headers,
                allow_redirects=True,
                stream=True,
                timeout=(15, 30),
            )
        )
        status_code = int(response.status_code or 502)
        if status_code >= 400:
            entry.failed_reason = f"download_status_{status_code}"
            entry.ready_event.set()
            return
        entry.metadata_headers = _proxy_response_headers(status_code, response.headers)
        entry.content_type = clean_text(response.headers.get("Content-Type")) or entry.content_type
        size_header = clean_text(response.headers.get("Content-Length"))
        if not entry.expected_size and size_header.isdigit():
            entry.expected_size = int(size_header)
        entry.ready_event.set()
        bytes_written = 0
        iterator = response.iter_content(chunk_size=VOD_CACHE_CHUNK_BYTES)
        async with aiofiles.open(entry.part_path, "wb") as handle:
            while True:
                chunk = await asyncio.to_thread(next, iterator, None)
                if not chunk:
                    break
                await handle.write(chunk)
                bytes_written += len(chunk)
                entry.bytes_written = bytes_written
                entry.touch()
                entry.progress_event.set()
                entry.progress_event = asyncio.Event()
            await handle.flush()
        if entry.expected_size and bytes_written >= entry.expected_size:
            await asyncio.to_thread(os.replace, entry.part_path, entry.final_path)
            entry.complete = True
            entry.bytes_written = bytes_written
            entry.failed_reason = None
            logger.info("VOD cache completed asset=%s bytes=%s path=%s", entry.key, bytes_written, entry.final_path)
        else:
            entry.failed_reason = "download_incomplete"
        entry.touch()
    except asyncio.CancelledError:
        entry.failed_reason = "cancelled"
        raise
    except Exception as exc:
        entry.failed_reason = f"download_failed:{exc}"
        logger.warning("VOD cache download failed asset=%s error=%s", entry.key, exc)
    finally:
        entry.ready_event.set()
        entry.progress_event.set()
        try:
            if response is not None:
                await asyncio.to_thread(response.close)
        except Exception:
            pass
        try:
            if http_session is not None:
                await asyncio.to_thread(http_session.close)
        except Exception:
            pass
        await cso_capacity_registry.release(source_capacity_key(entry.source), owner_key, slot_id=owner_key)
        async with entry.state_lock:
            entry.downloader_owner_key = None
            entry.download_task = None


class VodProxySession:
    def __init__(self, key, source: CsoSource, upstream_url: str, request_headers=None):
        self.key = str(key)
        self.source = source
        self.upstream_url = clean_text(upstream_url)
        self.request_headers = dict(request_headers or {})
        self.timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_connect=15, sock_read=None)
        self.http_session = None
        self.response = None
        self.blocking_session = None
        self.blocking_response = None
        self.blocking_iterator = None
        self.running = False
        self.capacity_key = source_capacity_key(source)
        self.capacity_limit = source_capacity_limit(source)
        self.owner_key = self.key
        self.status_code = 0
        self.content_type = None
        self.response_headers = {}
        self.last_error = None
        self.lock = asyncio.Lock()
        self.first_chunk_logged = False
        self.cache_entry = None
        self.local_only = False
        self.local_start = 0
        self.local_end = None
        self.local_size = None
        self.direct_owner_key = f"{self.key}:direct"
        self.cache_owner_key = f"{self.key}:cache"
        self.cache_session_attached = False
        self.direct_next_offset = 0
        self.requested_end = None
        self.direct_retry_attempts = 0
        self.max_direct_retry_attempts = 1

    async def start(self):
        startup_failed = False
        async with self.lock:
            if self.running:
                return True
            try:
                range_header = _header_value(self.request_headers, "Range")
                self.cache_entry = await vod_cache_manager.get_or_create(self.source, self.upstream_url)
                await vod_cache_manager.attach_session(self.cache_entry)
                self.cache_session_attached = True
                self.cache_entry.touch()
                cache_meta = await _ensure_vod_cache_ready(self.cache_entry, request_headers=self.request_headers)
                from_start = _is_from_start_request(self.request_headers)
                parsed_range = _parse_range_request(range_header, total_size=self.cache_entry.expected_size)
                self.direct_next_offset = int(parsed_range.get("start") or 0) if parsed_range else 0
                self.requested_end = (
                    int(parsed_range.get("end")) if parsed_range and parsed_range.get("end") is not None else None
                )

                if self.cache_entry.complete and self.cache_entry.expected_size:
                    self.local_only = True
                    self.local_size = int(self.cache_entry.expected_size)
                    if parsed_range and parsed_range.get("unsatisfied"):
                        self.status_code = 416
                        self.content_type = self.cache_entry.content_type
                        self.response_headers = _build_vod_local_response_headers(
                            self.local_size,
                            metadata_headers=self.cache_entry.metadata_headers,
                            start=0,
                            end=max(0, self.local_size - 1),
                            include_length=False,
                        )
                        self.response_headers["Content-Range"] = f"bytes */{self.local_size}"
                    else:
                        start = int(parsed_range.get("start") or 0) if parsed_range else 0
                        end = (
                            int(parsed_range.get("end"))
                            if parsed_range and parsed_range.get("end") is not None
                            else max(0, self.local_size - 1)
                        )
                        self.local_start = start
                        self.local_end = end
                        self.status_code = 206 if parsed_range else 200
                        self.content_type = self.cache_entry.content_type
                        self.response_headers = _build_vod_local_response_headers(
                            self.local_size,
                            metadata_headers=self.cache_entry.metadata_headers,
                            start=start,
                            end=end,
                        )
                    self.running = True
                    logger.info(
                        "VOD proxy session serving local cache key=%s source_id=%s range=%s path=%s",
                        self.key,
                        getattr(self.source, "id", None),
                        range_header or None,
                        self.cache_entry.final_path,
                    )
                    return True

                if from_start and cache_meta.get("cacheable") and self.cache_entry.expected_size:
                    started_cache = await _start_vod_cache_download(
                        self.cache_entry,
                        self.cache_owner_key,
                        request_headers=self.request_headers,
                    )
                    if started_cache:
                        await asyncio.wait_for(self.cache_entry.ready_event.wait(), timeout=15)
                        if self.cache_entry.failed_reason and not self.cache_entry.complete:
                            logger.warning(
                                "VOD cache start failed; falling back to direct proxy key=%s source_id=%s reason=%s",
                                self.key,
                                getattr(self.source, "id", None),
                                self.cache_entry.failed_reason,
                            )
                        else:
                            self.local_only = True
                            self.local_start = 0
                            self.local_end = max(0, int(self.cache_entry.expected_size or 0) - 1)
                            self.local_size = int(self.cache_entry.expected_size or 0)
                            self.status_code = 200
                            self.content_type = self.cache_entry.content_type
                            self.response_headers = _build_vod_local_response_headers(
                                self.local_size,
                                metadata_headers=self.cache_entry.metadata_headers,
                                start=0,
                                end=self.local_end,
                            )
                            self.running = True
                            logger.info(
                                "VOD proxy session started local-tail key=%s source_id=%s status=%s content_type=%s upstream_url=%s",
                                self.key,
                                getattr(self.source, "id", None),
                                self.status_code,
                                self.content_type,
                                self.upstream_url,
                            )
                            return True
                    else:
                        logger.warning(
                            "VOD cache downloader unavailable for start-of-file playback key=%s source_id=%s reason=%s",
                            self.key,
                            getattr(self.source, "id", None),
                            self.cache_entry.failed_reason,
                        )

                reserved = await cso_capacity_registry.try_reserve(
                    self.capacity_key,
                    self.owner_key,
                    self.capacity_limit,
                    slot_id=self.direct_owner_key,
                )
                if not reserved:
                    self.last_error = "capacity_blocked"
                    return False

                proxy_headers = _filter_vod_proxy_request_headers(self.request_headers, self.source)
                if not _header_value(proxy_headers, "Range"):
                    proxy_headers["Range"] = "bytes=0-"
                self.blocking_session = requests.Session()
                self.blocking_response = await asyncio.to_thread(
                    lambda: self.blocking_session.get(
                        self.upstream_url,
                        headers=proxy_headers,
                        allow_redirects=True,
                        stream=True,
                        timeout=(15, 30),
                    )
                )
                self.blocking_iterator = self.blocking_response.iter_content(chunk_size=64 * 1024)
                self.status_code = int(self.blocking_response.status_code or 502)
                self.content_type = clean_text(self.blocking_response.headers.get("Content-Type")) or None
                self.response_headers = _proxy_response_headers(
                    self.status_code,
                    self.blocking_response.headers,
                    request_headers=self.request_headers,
                )
                self.running = True
                logger.info(
                    "VOD proxy session started key=%s source_id=%s status=%s client_range=%s content_type=%s content_range=%s accept_ranges=%s upstream_url=%s",
                    self.key,
                    getattr(self.source, "id", None),
                    self.status_code,
                    range_header or None,
                    self.content_type,
                    clean_text(self.blocking_response.headers.get("Content-Range")) or None,
                    clean_text(self.blocking_response.headers.get("Accept-Ranges")) or None,
                    self.upstream_url,
                )
                if (
                    not from_start
                    and cache_meta.get("cacheable")
                    and self.cache_entry
                    and not self.cache_entry.downloader_running
                ):
                    await _start_vod_cache_download(
                        self.cache_entry,
                        self.cache_owner_key,
                        request_headers=self.request_headers,
                    )
                return True
            except Exception as exc:
                self.last_error = f"proxy_start_failed:{exc}"
                logger.warning(
                    "VOD proxy session failed to start key=%s source_id=%s error=%s",
                    self.key,
                    getattr(self.source, "id", None),
                    exc,
                )
                startup_failed = True
        if startup_failed:
            await self.stop(force=True)
        return False

    async def _close_direct_upstream(self):
        blocking_response = self.blocking_response
        self.blocking_response = None
        blocking_session = self.blocking_session
        self.blocking_session = None
        self.blocking_iterator = None
        try:
            if blocking_response is not None:
                await asyncio.to_thread(blocking_response.close)
        except Exception:
            pass
        try:
            if blocking_session is not None:
                await asyncio.to_thread(blocking_session.close)
        except Exception:
            pass

    async def _switch_to_local_from_offset(self, offset: int):
        entry = self.cache_entry
        if entry is None:
            return False
        current_written = int(entry.expected_size or 0) if entry.complete else int(entry.bytes_written or 0)
        if not entry.complete and current_written <= int(offset):
            return False
        self.local_only = True
        self.local_start = int(offset)
        self.local_end = self.requested_end
        if entry.complete and entry.expected_size:
            self.local_size = int(entry.expected_size)
        await self._close_direct_upstream()
        logger.info(
            "VOD proxy session switched to local cache key=%s source_id=%s offset=%s complete=%s",
            self.key,
            getattr(self.source, "id", None),
            int(offset),
            bool(entry.complete),
        )
        return True

    async def _retry_direct_upstream_from_offset(self, offset: int):
        if self.direct_retry_attempts >= self.max_direct_retry_attempts:
            return False
        proxy_headers = _filter_vod_proxy_request_headers(self.request_headers, self.source)
        proxy_headers["Range"] = f"bytes={max(0, int(offset))}-"
        await self._close_direct_upstream()
        session = requests.Session()
        try:
            response = await asyncio.to_thread(
                lambda: session.get(
                    self.upstream_url,
                    headers=proxy_headers,
                    allow_redirects=True,
                    stream=True,
                    timeout=(15, 30),
                )
            )
        except Exception:
            try:
                await asyncio.to_thread(session.close)
            except Exception:
                pass
            raise
        status_code = int(response.status_code or 502)
        if status_code >= 400:
            try:
                await asyncio.to_thread(response.close)
            except Exception:
                pass
            try:
                await asyncio.to_thread(session.close)
            except Exception:
                pass
            self.last_error = f"proxy_retry_status_{status_code}"
            return False
        self.blocking_session = session
        self.blocking_response = response
        self.blocking_iterator = response.iter_content(chunk_size=64 * 1024)
        self.direct_retry_attempts += 1
        logger.warning(
            "VOD proxy upstream retry key=%s source_id=%s offset=%s status=%s attempt=%s",
            self.key,
            getattr(self.source, "id", None),
            int(offset),
            status_code,
            self.direct_retry_attempts,
        )
        return True

    async def iter_bytes(self):
        try:
            if self.local_only and self.cache_entry is not None:
                async for chunk in self._iter_local_bytes():
                    yield chunk
                return
            if self.blocking_response is None:
                return
            while True:
                if not self.running:
                    break
                try:
                    chunk = await asyncio.to_thread(next, self.blocking_iterator, None)
                except (
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ConnectionError,
                    urllib3.exceptions.ProtocolError,
                    ConnectionResetError,
                ) as exc:
                    if not self.running:
                        break
                    logger.warning(
                        "VOD proxy upstream read interrupted key=%s source_id=%s offset=%s error=%s",
                        self.key,
                        getattr(self.source, "id", None),
                        int(self.direct_next_offset),
                        exc,
                    )
                    if await self._switch_to_local_from_offset(self.direct_next_offset):
                        async for local_chunk in self._iter_local_bytes():
                            yield local_chunk
                        return
                    retried = await self._retry_direct_upstream_from_offset(self.direct_next_offset)
                    if retried:
                        continue
                    self.last_error = f"proxy_read_failed:{exc}"
                    break
                if chunk:
                    if not self.first_chunk_logged:
                        logger.info(
                            "VOD proxy first chunk key=%s source_id=%s bytes=%s status=%s",
                            self.key,
                            getattr(self.source, "id", None),
                            len(chunk),
                            self.status_code,
                        )
                        self.first_chunk_logged = True
                    self.direct_next_offset += len(chunk)
                    yield chunk
                else:
                    break
        finally:
            await self.stop(force=True)

    async def _iter_local_bytes(self):
        entry = self.cache_entry
        if entry is None:
            return
        if self.status_code == 416:
            return
        entry.active_readers += 1
        entry.touch()
        target_path = entry.final_path if entry.complete and entry.final_path.exists() else entry.part_path
        offset = int(self.local_start or 0)
        final_end = int(self.local_end) if self.local_end is not None else None
        try:
            while self.running:
                current_written = int(entry.expected_size or 0) if entry.complete else int(entry.bytes_written or 0)
                if final_end is not None and offset > final_end:
                    break
                if offset >= current_written and not entry.complete:
                    if entry.failed_reason and not entry.downloader_running:
                        break
                    await entry.progress_event.wait()
                    continue
                available_end = current_written - 1
                if final_end is not None:
                    available_end = min(available_end, final_end)
                if available_end < offset:
                    if entry.complete:
                        break
                    if entry.failed_reason and not entry.downloader_running:
                        break
                    await entry.progress_event.wait()
                    continue
                if not target_path.exists():
                    target_path = entry.final_path if entry.complete and entry.final_path.exists() else entry.part_path
                async with aiofiles.open(target_path, "rb") as handle:
                    await handle.seek(offset)
                    while self.running:
                        current_written = (
                            int(entry.expected_size or 0) if entry.complete else int(entry.bytes_written or 0)
                        )
                        max_end = current_written - 1
                        if final_end is not None:
                            max_end = min(max_end, final_end)
                        remaining = max_end - offset + 1
                        if remaining <= 0:
                            break
                        chunk = await handle.read(min(VOD_CACHE_CHUNK_BYTES, remaining))
                        if not chunk:
                            break
                        if not self.first_chunk_logged:
                            logger.info(
                                "VOD proxy first local chunk key=%s source_id=%s bytes=%s status=%s",
                                self.key,
                                getattr(self.source, "id", None),
                                len(chunk),
                                self.status_code,
                            )
                            self.first_chunk_logged = True
                        offset += len(chunk)
                        entry.touch()
                        yield chunk
                        if final_end is not None and offset > final_end:
                            return
                        if not self.running:
                            return
                if entry.complete and offset >= int(entry.expected_size or 0):
                    break
        finally:
            entry.active_readers = max(0, int(entry.active_readers or 0) - 1)
            entry.touch()

    async def stop(self, force=False):
        async with self.lock:
            if (
                not self.running
                and self.response is None
                and self.http_session is None
                and self.blocking_response is None
                and self.blocking_session is None
            ):
                return
            self.running = False
            response = self.response
            self.response = None
            http_session = self.http_session
            self.http_session = None
            blocking_response = self.blocking_response
            self.blocking_response = None
            blocking_session = self.blocking_session
            self.blocking_session = None
            self.blocking_iterator = None
        try:
            if response is not None:
                response.close()
        except Exception:
            pass
        try:
            if http_session is not None:
                await http_session.close()
        except Exception:
            pass
        try:
            if blocking_response is not None:
                await asyncio.to_thread(blocking_response.close)
        except Exception:
            pass
        try:
            if blocking_session is not None:
                await asyncio.to_thread(blocking_session.close)
        except Exception:
            pass
        logger.info(
            "VOD proxy session stopped key=%s source_id=%s status=%s",
            self.key,
            getattr(self.source, "id", None),
            self.status_code,
        )
        await cso_capacity_registry.release(self.capacity_key, self.owner_key, slot_id=self.direct_owner_key)
        if self.cache_entry is not None and self.cache_session_attached:
            await vod_cache_manager.detach_session(self.cache_entry)
            self.cache_session_attached = False
        await vod_proxy_session_manager.remove(self.key)


class VodProxySessionManager:
    def __init__(self):
        self.sessions = {}
        self.lock = asyncio.Lock()

    async def create(self, key, source: CsoSource, upstream_url: str, request_headers=None):
        session = VodProxySession(key, source, upstream_url, request_headers=request_headers)
        async with self.lock:
            self.sessions[str(key)] = session
        return session

    async def remove(self, key):
        async with self.lock:
            self.sessions.pop(str(key), None)


vod_proxy_session_manager = VodProxySessionManager()


async def cleanup_vod_proxy_cache():
    return await vod_cache_manager.cleanup()


async def warm_vod_cache(candidate, upstream_url, episode=None, owner_key=None, request_headers=None):
    if not candidate:
        return False
    source = await cso_source_from_vod_source(candidate, upstream_url)
    if not source or not source.url:
        return False
    entry = await vod_cache_manager.get_or_create(source, source.url)
    cache_meta = await _ensure_vod_cache_ready(entry, request_headers=request_headers)
    if not cache_meta.get("cacheable"):
        return False
    owner = clean_text(owner_key) or f"vod-cache-warm-{source.id}"
    return await _start_vod_cache_download(entry, owner, request_headers=request_headers)


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


def _resolve_source_url_candidates(source: CsoSource, base_url, instance_id, stream_key=None, username=None):
    primary_url = _resolve_source_url(
        source.url,
        base_url=base_url,
        instance_id=instance_id,
        stream_key=stream_key,
        username=username,
    )
    if not primary_url:
        return []

    if not source.xc_account_id:
        return [primary_url]

    playlist = source.playlist
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
    vod_category_id=None,
    vod_item_id=None,
    vod_episode_id=None,
    tvh_subscription_id=None,
    session_id=None,
    event_type,
    severity="info",
    details=None,
    source: CsoSource | None = None,
):
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
                    event_category_id = convert_to_int(getattr(item, "category_id", None), 0) or None
            return event_category_id, event_item_id or None, None

        if event_source.source_type == "vod_episode":
            event_episode_id = convert_to_int(event_source.internal_id, 0)
            if event_episode_id > 0:
                async with Session() as session:
                    episode = await session.get(VodCategoryEpisode, event_episode_id)
                    item = (
                        await session.get(VodCategoryItem, int(episode.category_item_id))
                        if episode is not None and int(getattr(episode, "category_item_id", 0) or 0) > 0
                        else None
                    )
                if item is not None:
                    event_item_id = convert_to_int(getattr(item, "id", None), 0) or None
                    event_category_id = convert_to_int(getattr(item, "category_id", None), 0) or None
            return event_category_id, event_item_id, event_episode_id or None

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


class CsoFfmpegCommandBuilder:
    """Structured FFmpeg command builder for ingest, output, HLS output, and slate sessions."""

    def __init__(self, policy=None, pipe_input_format="mpegts", pipe_output_format="mpegts"):
        self.policy = dict(policy or {})
        self.pipe_input_format = CONTAINER_TO_FORMAT.get(clean_key(pipe_input_format), "mpegts")
        self.pipe_output_format = CONTAINER_TO_FORMAT.get(clean_key(pipe_output_format), "mpegts")

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

    @staticmethod
    def _build_slate_media_hint(media_hint):
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

    @staticmethod
    def _ffmpeg_logging_command(debug_enabled, quiet_level="warning"):
        command = ["ffmpeg", "-hide_banner", "-loglevel", "info" if debug_enabled else quiet_level]
        if debug_enabled:
            command += ["-stats"]
        else:
            command += ["-nostats"]
        return command

    @staticmethod
    def _probe_flags(probe_size_bytes, analyse_duration_us, fps_probe_size):
        return [
            "-probesize",
            str(max(32_768, int(probe_size_bytes))),
            "-analyzeduration",
            str(max(250_000, int(analyse_duration_us))),
            "-fpsprobesize",
            str(max(0, int(fps_probe_size))),
        ]

    @staticmethod
    def _input_resilience_flags():
        return [
            "-fflags",
            "+discardcorrupt+genpts",
            "-err_detect",
            "ignore_err",
        ]

    @staticmethod
    def _drop_data_streams():
        return ["-dn"]

    @staticmethod
    def _mpegts_output_flags(zero_latency=True):
        command = [
            "-mpegts_flags",
            "+resend_headers",
        ]
        if zero_latency:
            command += [
                "-muxdelay",
                "0",
                "-muxpreload",
                "0",
            ]
        return command

    @staticmethod
    def _matroska_output_flags():
        # Configure Matroska for progressive/live output so players see clusters quickly
        # on non-seekable HTTP streams instead of waiting on larger default buffering.
        return [
            "-flush_packets",
            "1",
            "-cluster_time_limit",
            "1000",
            "-cluster_size_limit",
            "1048576",
            "-live",
            "1",
        ]

    @staticmethod
    def _pipe_output_target(ffmpeg_format, target="pipe:1"):
        command = []
        if ffmpeg_format == "mp4":
            command += ["-movflags", "+frag_keyframe+empty_moov+default_base_moof"]
        command += ["-f", ffmpeg_format, target]
        return command

    @staticmethod
    def _lavfi_input(spec):
        return ["-f", "lavfi", "-i", spec]

    def _slate_av_encode_flags(self, fps_value, pix_fmt, audio_bitrate, still_image=False):
        command = [
            "-c:v",
            "libx264",
            "-preset",
            "veryfast" if still_image else "superfast",
            "-tune",
            "stillimage" if still_image else "zerolatency",
            "-pix_fmt",
            pix_fmt,
            "-bf",
            "0",
            "-g",
            str(fps_value if still_image else max(fps_value * 2, fps_value)),
            "-keyint_min",
            str(fps_value if still_image else max(fps_value * 2, fps_value)),
            "-sc_threshold",
            "0",
            "-x264-params",
            "repeat-headers=1:scenecut=0",
            "-c:a",
            "aac",
            "-b:a",
            audio_bitrate,
            "-ar",
            "48000",
            "-ac",
            "2",
            "-shortest",
        ]
        return command

    def _apply_stream_selection(self, command, policy=None):
        effective_policy = policy or self.policy
        subtitle_mode = effective_policy.get("subtitle_mode") or "copy"
        if subtitle_mode != "drop":
            command += ["-map", "0:s?"]
        return subtitle_mode

    def _apply_transcode_options(self, command, subtitle_mode, policy=None):
        effective_policy = policy or self.policy
        video_codec = effective_policy.get("video_codec") or ""
        audio_codec = effective_policy.get("audio_codec") or ""
        hwaccel_requested = bool(effective_policy.get("hwaccel", False)) and bool(video_codec)
        use_hwaccel = hwaccel_requested
        deinterlace = bool(effective_policy.get("deinterlace", False)) and bool(video_codec)
        vaapi_device = detect_vaapi_device_path() if use_hwaccel else None
        if hwaccel_requested and not vaapi_device:
            logger.info(
                "CSO hwaccel requested but no VAAPI device is available; falling back to software encode video_codec=%s container=%s",
                video_codec,
                effective_policy.get("container") or "",
            )

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
                    command += [
                        "-preset",
                        "veryfast",
                        "-tune",
                        "zerolatency",
                        "-pix_fmt",
                        "yuv420p",
                        "-profile:v",
                        "high",
                        "-g",
                        "48",
                        "-keyint_min",
                        "48",
                        "-sc_threshold",
                        "0",
                        "-x264-params",
                        "repeat-headers=1:aud=1",
                    ]
        else:
            command += ["-c:v", "copy"]

        if audio_codec:
            sw_audio_encoder = self.audio_encoder_for_codec(audio_codec)
            command += ["-c:a", sw_audio_encoder]
            command += ["-af", "aresample=async=1:first_pts=0"]
            if audio_codec == "aac":
                command += ["-b:a", "128k", "-ar", "48000", "-ac", "2"]
        else:
            command += ["-c:a", "copy"]
        command += ["-c:s", "copy" if subtitle_mode != "drop" else "none"]
        if subtitle_mode == "drop":
            command.append("-sn")

    def _build_pipe_input(self, probe_size_bytes, analyse_duration_us, fps_probe_size, low_latency, pipe_format=None):
        pipe_format = CONTAINER_TO_FORMAT.get(clean_key(pipe_format), self.pipe_input_format)
        command = []
        if low_latency:
            command += [
                "-fflags",
                "+nobuffer",
                "-flags",
                "low_delay",
            ]
        command += self._probe_flags(probe_size_bytes, analyse_duration_us, fps_probe_size)
        command += [
            "-f",
            pipe_format,
            "-i",
            "pipe:0",
            "-map",
            "0:v:0?",
            "-map",
            "0:a?",
            "-max_muxing_queue_size",
            "4096",
        ]
        command += self._input_resilience_flags()
        return command

    def build_ingest_command(self, source_url, program_index=0, user_agent=None, request_headers=None):
        map_program = max(0, int(program_index or 0))
        is_hls_input = (urlparse(source_url or "").path or "").lower().endswith(".m3u8")
        header_values = sanitise_headers(request_headers)
        command = self._ffmpeg_logging_command(enable_cso_ingest_command_debug_logging, quiet_level="info")
        command += [
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
            command += ["-user_agent", user_agent_value]
        referer_value = _header_value(header_values, "Referer")
        if referer_value:
            command += ["-referer", referer_value]
        extra_headers = _format_ffmpeg_headers_arg(header_values)
        if extra_headers:
            command += ["-headers", extra_headers]
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
            command += ["-reconnect_streamed", "0"]
        command += self._input_resilience_flags()
        command += self._probe_flags(
            CSO_INGEST_PROBE_SIZE_BYTES,
            CSO_INGEST_ANALYSE_DURATION_US,
            CSO_INGEST_FPS_PROBE_SIZE,
        )
        command += [
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
        ]
        command += self._drop_data_streams()
        if self.pipe_output_format == "mpegts":
            command += self._mpegts_output_flags(zero_latency=True)
        elif self.pipe_output_format == "matroska":
            command += self._matroska_output_flags()
        command += self._pipe_output_target(self.pipe_output_format)
        return command

    def build_output_command(self, start_seconds=0, max_duration_seconds=None):
        command = self._ffmpeg_logging_command(enable_cso_output_command_debug_logging)
        command += self._build_pipe_input(
            CSO_OUTPUT_PROBE_SIZE_BYTES,
            CSO_OUTPUT_ANALYSE_DURATION_US,
            CSO_OUTPUT_FPS_PROBE_SIZE,
            low_latency=True,
            pipe_format=self.pipe_input_format,
        )
        start_value = max(0, int(start_seconds or 0))
        duration_value = max(1, int(max_duration_seconds or 0)) if max_duration_seconds is not None else None
        if start_value > 0:
            command += ["-ss", str(start_value)]
        if duration_value is not None:
            command += ["-t", str(duration_value)]
        subtitle_mode = self._apply_stream_selection(command)
        mode = self.policy.get("output_mode") or "force_remux"
        ffmpeg_format = policy_ffmpeg_format(self.policy)

        if mode == "force_transcode":
            self._apply_transcode_options(command, subtitle_mode)
        else:
            command += ["-c", "copy"]
            if ffmpeg_format in ["mp4"]:
                # TODO: We shouldonly do this if we have an AAC audio source
                command += ["-bsf:a", "aac_adtstoasc"]
            if subtitle_mode == "drop":
                command.append("-sn")

        command += self._drop_data_streams()
        if ffmpeg_format == "mpegts":
            command += self._mpegts_output_flags(zero_latency=True)
        elif ffmpeg_format == "matroska":
            command += self._matroska_output_flags()
        command += self._pipe_output_target(ffmpeg_format)
        return command

    def build_local_output_command(self, input_path: Path, start_seconds=0, max_duration_seconds=None, realtime=False):
        command = self._ffmpeg_logging_command(enable_cso_output_command_debug_logging)
        start_value = max(0, int(start_seconds or 0))
        duration_value = max(1, int(max_duration_seconds or 0)) if max_duration_seconds is not None else None
        if realtime:
            command += ["-re"]
        if start_value > 0:
            command += ["-ss", str(start_value)]
        command += self._probe_flags(
            CSO_OUTPUT_PROBE_SIZE_BYTES,
            CSO_OUTPUT_ANALYSE_DURATION_US,
            CSO_OUTPUT_FPS_PROBE_SIZE,
        )
        command += self._input_resilience_flags()
        command += [
            "-i",
            str(input_path),
            "-map",
            "0:v:0?",
            "-map",
            "0:a?",
            "-max_muxing_queue_size",
            "4096",
        ]
        if duration_value is not None:
            command += ["-t", str(duration_value)]
        subtitle_mode = self._apply_stream_selection(command)
        mode = self.policy.get("output_mode") or "force_remux"
        ffmpeg_format = policy_ffmpeg_format(self.policy)

        if mode == "force_transcode":
            self._apply_transcode_options(command, subtitle_mode)
        else:
            command += ["-c", "copy"]
            if ffmpeg_format in ["mp4"]:
                command += ["-bsf:a", "aac_adtstoasc"]
            if subtitle_mode == "drop":
                command.append("-sn")

        command += self._drop_data_streams()
        if ffmpeg_format == "mpegts":
            command += self._mpegts_output_flags(zero_latency=True)
        elif ffmpeg_format == "matroska":
            command += self._matroska_output_flags()
        command += self._pipe_output_target(ffmpeg_format)
        return command

    def build_vod_segment_ingest_command(
        self,
        input_target: str,
        start_seconds=0,
        max_duration_seconds=None,
        realtime=False,
        input_is_url=False,
        user_agent=None,
        request_headers=None,
    ):
        command = self._ffmpeg_logging_command(enable_cso_ingest_command_debug_logging, quiet_level="info")
        start_value = max(0, int(start_seconds or 0))
        duration_value = max(1, int(max_duration_seconds or 0)) if max_duration_seconds is not None else None
        input_seek_value = start_value
        trim_seek_value = 0
        if input_is_url and start_value > 0:
            trim_seek_value = min(2, start_value)
            input_seek_value = max(0, start_value - trim_seek_value)
        if realtime:
            command += ["-re"]
        if input_is_url:
            header_values = sanitise_headers(request_headers)
            user_agent_value = clean_text(user_agent) or _header_value(header_values, "User-Agent")
            command += [
                "-progress",
                "pipe:2",
                "-reconnect",
                "1",
                "-reconnect_on_network_error",
                "1",
                "-reconnect_delay_max",
                str(max(1, int(CSO_INGEST_RECONNECT_DELAY_MAX_SECONDS))),
            ]
            if user_agent_value:
                command += ["-user_agent", user_agent_value]
            referer_value = _header_value(header_values, "Referer")
            if referer_value:
                command += ["-referer", referer_value]
            extra_headers = _format_ffmpeg_headers_arg(header_values)
            if extra_headers:
                command += ["-headers", extra_headers]
            command += [
                "-reconnect_at_eof",
                "1",
                "-reconnect_streamed",
                "1",
                "-reconnect_on_http_error",
                "4xx,5xx",
                "-rw_timeout",
                str(max(1_000_000, int(CSO_INGEST_RW_TIMEOUT_US))),
                "-timeout",
                str(max(1_000_000, int(CSO_INGEST_TIMEOUT_US))),
            ]
        if input_seek_value > 0:
            command += ["-ss", str(input_seek_value)]
        command += self._probe_flags(
            CSO_INGEST_PROBE_SIZE_BYTES,
            CSO_INGEST_ANALYSE_DURATION_US,
            CSO_INGEST_FPS_PROBE_SIZE,
        )
        command += self._input_resilience_flags()
        command += [
            "-i",
            str(input_target),
            "-map",
            "0:v:0?",
            "-map",
            "0:a?",
        ]
        if trim_seek_value > 0:
            command += ["-ss", str(trim_seek_value)]
        command += ["-c", "copy"]
        if duration_value is not None:
            command += ["-t", str(duration_value)]
        command += self._drop_data_streams()
        if self.pipe_output_format == "mpegts":
            command += self._mpegts_output_flags(zero_latency=True)
        elif self.pipe_output_format == "matroska":
            command += self._matroska_output_flags()
        command += self._pipe_output_target(self.pipe_output_format)
        return command

    def build_vod_channel_ingest_command(
        self,
        input_target: str,
        start_seconds: int = 0,
        max_duration_seconds: int | None = None,
        realtime: bool = False,
        input_is_url: bool = False,
        user_agent: str | None = None,
        request_headers: dict[str, str] | None = None,
        policy: dict[str, Any] | None = None,
    ) -> list[str]:
        effective_policy = dict(policy or self.policy or {})
        command = self._ffmpeg_logging_command(enable_cso_ingest_command_debug_logging, quiet_level="info")
        start_value = max(0, int(start_seconds or 0))
        duration_value = max(1, int(max_duration_seconds or 0)) if max_duration_seconds is not None else None
        input_seek_value = start_value
        trim_seek_value = 0
        if start_value > 0:
            trim_seek_value = min(2, start_value)
            input_seek_value = max(0, start_value - trim_seek_value)
        if realtime:
            command += ["-re"]
        if input_is_url:
            header_values = sanitise_headers(request_headers)
            user_agent_value = clean_text(user_agent) or _header_value(header_values, "User-Agent")
            command += [
                "-progress",
                "pipe:2",
                "-reconnect",
                "1",
                "-reconnect_on_network_error",
                "1",
                "-reconnect_delay_max",
                str(max(1, int(CSO_INGEST_RECONNECT_DELAY_MAX_SECONDS))),
            ]
            if user_agent_value:
                command += ["-user_agent", user_agent_value]
            referer_value = _header_value(header_values, "Referer")
            if referer_value:
                command += ["-referer", referer_value]
            extra_headers = _format_ffmpeg_headers_arg(header_values)
            if extra_headers:
                command += ["-headers", extra_headers]
            command += [
                "-reconnect_at_eof",
                "1",
                "-reconnect_streamed",
                "1",
                "-reconnect_on_http_error",
                "4xx,5xx",
                "-rw_timeout",
                str(max(1_000_000, int(CSO_INGEST_RW_TIMEOUT_US))),
                "-timeout",
                str(max(1_000_000, int(CSO_INGEST_TIMEOUT_US))),
            ]
        if input_seek_value > 0:
            command += ["-ss", str(input_seek_value)]
        command += self._probe_flags(
            CSO_INGEST_PROBE_SIZE_BYTES,
            CSO_INGEST_ANALYSE_DURATION_US,
            CSO_INGEST_FPS_PROBE_SIZE,
        )
        command += self._input_resilience_flags()
        command += [
            "-i",
            str(input_target),
            "-map",
            "0:v:0?",
            "-map",
            "0:a:0?",
            "-map_metadata",
            "-1",
            "-map_chapters",
            "-1",
            "-max_muxing_queue_size",
            "4096",
        ]
        if trim_seek_value > 0:
            command += ["-ss", str(trim_seek_value)]
        subtitle_mode = effective_policy.get("subtitle_mode") or "drop"
        self._apply_transcode_options(command, subtitle_mode, policy=effective_policy)
        if duration_value is not None:
            command += ["-t", str(duration_value)]
        command += self._drop_data_streams()
        if self.pipe_output_format == "mpegts":
            command += self._mpegts_output_flags(zero_latency=True)
        elif self.pipe_output_format == "matroska":
            command += self._matroska_output_flags()
        command += self._pipe_output_target(self.pipe_output_format)
        return command

    def build_hls_output_command(self, output_dir: Path):
        command = self._ffmpeg_logging_command(enable_cso_output_command_debug_logging)
        command += self._build_pipe_input(
            2 * 1024 * 1024,
            5_000_000,
            CSO_OUTPUT_FPS_PROBE_SIZE,
            low_latency=False,
            pipe_format=self.pipe_input_format,
        )
        subtitle_mode = self._apply_stream_selection(command)
        hls_policy = _effective_hls_runtime_policy(self.policy)
        mode = hls_policy.get("output_mode") or "force_remux"

        if mode == "force_transcode":
            self._apply_transcode_options(command, subtitle_mode, policy=hls_policy)
        else:
            command += ["-c", "copy"]
            if subtitle_mode == "drop":
                command.append("-sn")
            command += self._mpegts_output_flags(zero_latency=False)

        command += self._drop_data_streams()
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

    def build_slate_command(
        self,
        slate_type,
        primary_text="",
        secondary_text="",
        duration_seconds=10,
        output_target="pipe:1",
        realtime=False,
        media_hint=None,
    ):
        reason_key = clean_key(slate_type, fallback="playback_unavailable")
        duration_value = None if duration_seconds is None else max(1, int(duration_seconds))
        slate_media_hint = self._build_slate_media_hint(media_hint)
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
            command = ["ffmpeg", "-hide_banner", "-loglevel", "warning"]
            if realtime:
                command += ["-re"]
            command += self._lavfi_input(startup_video)
            command += self._lavfi_input(startup_audio)
            command += self._slate_av_encode_flags(startup_fps, startup_pix_fmt, "128k", still_image=True)
            command += self._mpegts_output_flags(zero_latency=False)
            command += self._pipe_output_target("mpegts", target=output_target)
            return command

        title = _escape_ffmpeg_drawtext_text(clean_text(primary_text))
        subtitle_lines = [
            _escape_ffmpeg_drawtext_text(line)
            for line in _wrap_slate_words(clean_text(secondary_text), max_chars=84, max_lines=4)
        ]
        drawtext_title = (
            f"drawtext=text='{title}':fontcolor=white:fontsize={title_font_size}:x=(w-text_w)/2:y={title_y}"
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
        ]
        input_args += self._lavfi_input(
            f"color=c=0x0B0F14:s={startup_width}x{startup_height}:r={render_fps}"
            + (f":d={duration_value}" if duration_value is not None else "")
        )
        input_args += self._lavfi_input(
            f"color=c=0x21A3CF:s={blob1_size}x{blob1_size}:r={render_fps}"
            + (f":d={duration_value}" if duration_value is not None else "")
        )
        input_args += self._lavfi_input(
            f"color=c=0x79D2C0:s={blob2_size}x{blob2_size}:r={render_fps}"
            + (f":d={duration_value}" if duration_value is not None else "")
        )
        input_args += self._lavfi_input(
            f"color=c=0x6AA8FF:s={blob3_size}x{blob3_size}:r={render_fps}"
            + (f":d={duration_value}" if duration_value is not None else "")
        )
        if logo_path:
            input_args += ["-loop", "1", "-i", logo_path]
            filter_steps.append(
                f"[4:v]scale=w={logo_width}:h=-1:flags=lanczos,format=rgba,colorchannelmixer=aa=0.98[logo]"
            )
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
        command = list(input_args)
        command += self._lavfi_input(
            "anullsrc=channel_layout=stereo:sample_rate=48000"
            + (f":d={duration_value}" if duration_value is not None else "")
        )
        command += [
            "-filter_complex",
            ";".join(filter_steps),
            "-map",
            "[vout]",
            "-map",
            f"{5 if logo_path else 4}:a",
        ]
        command += self._slate_av_encode_flags(render_fps, "yuv420p", "96k", still_image=False)
        command += self._mpegts_output_flags(zero_latency=False)
        command += self._pipe_output_target("mpegts", target=output_target)
        return command


def _resolve_cso_ingest_user_agent(config, source: CsoSource):
    playlist = source.playlist if source is not None else None
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


def _resolve_cso_ingest_headers(source: CsoSource):
    playlist = source.playlist if source is not None else None
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


def should_allow_unavailable_slate(profile_name, channel=None, source: CsoSource = None):
    # VOD and Recordings don't have the same failover concerns as Live TV.
    # We should always allow slates here to provide better feedback to the user.
    if source and source.source_type in {"vod_movie", "vod_episode"}:
        return True

    # For Live TV, check if the channel is forced.
    channel_forced_cso = bool(getattr(channel, "cso_enabled", False)) if channel is not None else False

    # For TVH profile traffic, return hard failures unless the channel is explicitly forced through CSO.
    # This prevents TVH from getting "stuck" on a failing service if it has others to try.
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
        self.max_history_bytes = int(CSO_INGEST_HISTORY_MAX_BYTES)
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
        self, policy, reason, detail_hint="", profile_name="", channel=None, source: CsoSource = None, status_code=503
    ):
        allow_unavailable_slate = should_allow_unavailable_slate(profile_name, channel=channel, source=source)
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
                channel_id=getattr(channel, "id", None) or (source.channel_id if source else self.channel_id),
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
                sources = await order_cso_channel_sources(list(channel.sources or []), channel_id=int(channel.id))
                self.sources = [cso_source_from_channel_source(s) for s in sources]
                logger.debug(
                    "CSO ingest refreshed sources channel=%s count=%s",
                    self.channel_id,
                    len(self.sources),
                )

    async def _handle_source_failure(self, source: CsoSource, reason, details=None):
        if source.source_type != "channel":
            return

        source_id = int(source.id or 0)
        if source_id <= 0 or int(self.channel_id or 0) <= 0:
            return

        await mark_cso_channel_source_temporarily_failed(self.channel_id, source_id)

        app = getattr(self, "app", None)
        if app is None:
            return

        try:
            from backend.channel_stream_health import schedule_background_health_check_for_source

            await schedule_background_health_check_for_source(
                app,
                source_id,
                reason=reason,
                details=details or {},
            )
        except Exception as exc:
            logger.warning(
                "CSO failed to queue background health check channel=%s source_id=%s reason=%s error=%s",
                self.channel_id,
                source_id,
                reason,
                exc,
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

    async def _spawn_ingest_process(self, source_url, program_index, source: CsoSource = None):
        playlist = getattr(source, "playlist", None) if source is not None else None
        source_user_agent = clean_text(getattr(playlist, "user_agent", "")) or self.ingest_user_agent
        source_headers = _resolve_cso_ingest_headers(source)
        source_user_agent = _header_value(source_headers, "User-Agent") or source_user_agent

        # Load existing probe details from the adapter
        source_probe = {}
        if source is not None and source.probe_details:
            source_probe = source.probe_details
        elif source is not None:
            source_probe = load_source_media_shape(source)

        pipe_format = _resolve_vod_pipe_container(source, source_probe=source_probe)
        command = CsoFfmpegCommandBuilder(pipe_output_format=pipe_format).build_ingest_command(
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
            source.id if source is not None else getattr(self.current_source, "id", None),
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

        source = self.current_source
        source_id = getattr(source, "id", None)
        source_type = getattr(source, "source_type", "channel")

        if not source_id:
            return

        persisted = False
        try:
            persisted = await persist_source_media_shape(
                source_id, self.current_source_probe, observed_at=utc_now_naive(), source_type=source_type
            )
        except Exception:
            pass

        if persisted:
            self._current_source_probe_persisted = True
            if enable_cso_ingest_command_debug_logging:
                logger.info(
                    "CSO ingest learned live media shape type=%s id=%s probe=%s",
                    source_type,
                    source_id,
                    dict(self.current_source_probe or {}),
                )
        elif enable_cso_ingest_command_debug_logging:
            logger.info(
                "CSO ingest learned live media shape but persist failed type=%s id=%s probe=%s",
                source_type,
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
            if source.id is None:
                continue
            playlist = source.playlist
            if playlist is not None and not bool(getattr(playlist, "enabled", False)):
                continue
            xc_account = source.xc_account
            if xc_account is not None and not bool(getattr(xc_account, "enabled", False)):
                continue
            if not source.url:
                continue
            eligible_ids.add(source.id)
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
        candidates = await order_cso_channel_sources(self.sources, channel_id=self.channel_id)
        if preferred_source_id is not None:
            preferred = [source for source in candidates if source.id == preferred_source_id]
            others = [source for source in candidates if source.id != preferred_source_id]
            candidates = preferred + others
        saw_capacity_block = False
        for source in candidates:
            if source.id in excluded_ids:
                continue
            hold_until = self.failed_source_until.get(source.id, 0)
            if not ignore_hold_down and hold_until > now:
                continue
            playlist = source.playlist
            if playlist is not None and not bool(getattr(playlist, "enabled", False)):
                continue
            xc_account = source.xc_account
            if xc_account is not None and not bool(getattr(xc_account, "enabled", False)):
                continue
            if not source.url:
                continue

            capacity_key = source_capacity_key(source)
            capacity_limit = source_capacity_limit(source)
            reserved = await cso_capacity_registry.try_reserve(
                capacity_key,
                self.capacity_owner_key,
                capacity_limit,
                slot_id=source.id,
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
                await cso_capacity_registry.release(capacity_key, self.capacity_owner_key, slot_id=source.id)
                continue

            process = None
            resolved_url = ""
            variants = []
            variant_position = None
            remembered_program_index = self.source_program_index.get(source.id)
            last_error = None
            for candidate_url in source_urls:
                variants = await _discover_hls_variants(candidate_url)
                variant_position = None
                ingest_url = candidate_url
                url_path = urlparse(candidate_url).path.lower()
                if (url_path.endswith(".m3u8") or url_path.endswith(".m3u")) and variants:
                    if remembered_program_index is not None:
                        for idx, item in enumerate(variants):
                            if int(item.get("program_index") or 0) == int(remembered_program_index):
                                variant_position = idx
                                break
                    if variant_position is None:
                        variant_position = len(variants) - 1
                    selected_variant = variants[variant_position]
                    program_index = int(selected_variant.get("program_index") or 0)
                    ingest_url = (selected_variant.get("variant_url") or "").strip() or candidate_url
                    logger.info(
                        "CSO HLS ingest selected variant channel=%s source_id=%s "
                        "program_index=%s variant_position=%s variant_count=%s ingest_url=%s",
                        self.channel_id,
                        source.id,
                        program_index,
                        variant_position,
                        len(variants),
                        ingest_url,
                    )
                else:
                    program_index = int(remembered_program_index or 0)
                    if remembered_program_index is not None:
                        logger.info(
                            "CSO ingest variant discovery empty; reusing remembered program index "
                            "channel=%s source_id=%s program_index=%s",
                            self.channel_id,
                            source.id,
                            program_index,
                        )
                try:
                    process = await self._spawn_ingest_process(ingest_url, program_index, source=source)
                    resolved_url = ingest_url
                    break
                except Exception as exc:
                    last_error = exc
                    await self._handle_source_failure(source, "ingest_start_failed", {"error": str(exc)})
                    await emit_channel_stream_event(
                        channel_id=self.channel_id,
                        source=source,
                        session_id=self.key,
                        event_type="playback_unavailable",
                        severity="warning",
                        details={
                            "reason": "ingest_start_failed",
                            "pipeline": "ingest",
                            "error": str(exc),
                            **_source_event_context(source, source_url=ingest_url),
                        },
                    )
                    continue

            if not process:
                if last_error:
                    logger.warning(
                        "CSO ingest failed for all URLs on source channel=%s source_id=%s error=%s",
                        self.channel_id,
                        source.id,
                        last_error,
                    )
                self.current_source = None
                self.current_source_url = ""
                self.current_capacity_key = None
                self.running = False
                self.process = None
                await cso_capacity_registry.release(capacity_key, self.capacity_owner_key, slot_id=source.id)
                continue
            old_capacity_key = self.current_capacity_key
            old_source_id = getattr(self.current_source, "id", None)
            self.current_source = source
            self.current_source_url = resolved_url
            self.current_capacity_key = capacity_key
            self.hls_variants = variants
            self.current_variant_position = variant_position
            self.current_program_index = program_index
            if source.id is not None:
                self.source_program_index[source.id] = int(program_index)
            self.startup_jump_done = True
            self._activate_process_unlocked(process)
            if old_capacity_key:
                await cso_capacity_registry.release(old_capacity_key, self.capacity_owner_key, slot_id=old_source_id)

            await emit_channel_stream_event(
                channel_id=self.channel_id,
                source=source,
                session_id=self.key,
                event_type="switch_success",
                severity="info",
                details={
                    "reason": reason,
                    "pipeline": "ingest",
                    "program_index": self.current_program_index,
                    "variant_count": len(self.hls_variants),
                    **_source_event_context(
                        self.current_source,
                        source_url=self.current_source_url,
                    ),
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
            source=source,
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
                source=failed_source,
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
            source=failed_source,
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
        if failed_source is not None and reason != "capacity_blocked":
            await self._handle_source_failure(
                failed_source,
                reason,
                {
                    "return_code": return_code,
                    "saw_data": saw_data,
                    **(details or {}),
                },
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
            source=failed_source,
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


class VodChannelIngestSession:
    def __init__(
        self,
        key: object,
        config: Any,
        channel_id: int,
        stream_key: str | None = None,
        request_headers: dict[str, str] | None = None,
        output_policy: dict[str, Any] | None = None,
    ):
        self.key = str(key)
        self.config = config
        self.channel_id = int(channel_id)
        self.output_policy = dict(output_policy or {})
        self.ingest_policy = _generate_vod_channel_ingest_policy(config, self.output_policy)
        self.stream_key = clean_text(stream_key)
        self.request_headers = dict(request_headers or {})
        self.process = None
        self.segment_task = None
        self.stderr_task = None
        self.running = False
        self.lock = asyncio.Lock()
        self.last_activity = time.time()
        self.subscribers = {}
        self.history = deque()
        self.history_bytes = 0
        self.max_history_bytes = int(CSO_INGEST_HISTORY_MAX_BYTES)
        self.current_source = None
        self.current_source_url = ""
        self.current_source_probe = {}
        self.last_error = None
        self.failover_in_progress = False
        self.failover_exhausted = False
        self.failover_start_ts = 0.0
        self.health_failover_reason = None
        self.health_failover_details = None
        self.last_reader_end_reason = None
        self.last_reader_end_saw_data = False
        self.last_reader_end_return_code = None
        self.last_reader_end_ts = 0.0
        self.first_healthy_stream_seen = False
        self.current_segment_healthy = False
        self.session_start_ts = 0.0
        self._recent_ffmpeg_stderr = deque(maxlen=50)
        self._startup_event = None
        self._startup_succeeded = False
        self._warm_task = None

    def is_hunting_for_stream(self):
        if not self.running:
            return True
        if self.current_source is None:
            return True
        if not self.current_segment_healthy:
            return True
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
        for queue in subscriber_queues:
            await queue.put_drop_oldest(chunk)

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
            subscriber_count = len(self.subscribers)
        logger.info(
            "VOD channel ingest subscriber added channel=%s ingest_key=%s subscriber=%s subscribers=%s",
            self.channel_id,
            self.key,
            subscriber_id,
            subscriber_count,
        )
        return queue

    async def remove_subscriber(self, subscriber_id):
        async with self.lock:
            self.subscribers.pop(subscriber_id, None)
            remaining = len(self.subscribers)
        logger.info(
            "VOD channel ingest subscriber removed channel=%s ingest_key=%s subscriber=%s subscribers=%s",
            self.channel_id,
            self.key,
            subscriber_id,
            remaining,
        )
        if remaining == 0:
            await self.stop(force=True)
        return remaining

    async def start(self):
        async with self.lock:
            if self.running and self.segment_task is not None and not self.segment_task.done():
                return
            self.running = True
            self.last_error = None
            self.failover_exhausted = False
            self.current_source = None
            self.current_source_url = ""
            self.current_source_probe = {}
            self.history.clear()
            self.history_bytes = 0
            self.first_healthy_stream_seen = False
            self.current_segment_healthy = False
            self.session_start_ts = time.time()
            self._recent_ffmpeg_stderr.clear()
            self._startup_event = asyncio.Event()
            self._startup_succeeded = False
            self.segment_task = asyncio.create_task(
                self._run_loop(),
                name=f"vod-channel-ingest-{self.channel_id}",
            )
            startup_event = self._startup_event
        await startup_event.wait()

    async def _read_stderr(self, process, entry):
        if process.stderr is None:
            return
        while self.running:
            try:
                line = await process.stderr.readline()
            except Exception:
                break
            if not line:
                break
            rendered = line.decode(errors="ignore").strip()
            if not rendered:
                continue
            self._recent_ffmpeg_stderr.append(rendered)
            if CsoOutputSession._should_log_ffmpeg_stderr_line(rendered):
                logger.info(
                    "VOD channel ingest ffmpeg[%s][%s][%s]: %s",
                    self.channel_id,
                    self.key,
                    int(entry.get("start_ts") or 0),
                    rendered,
                )

    @staticmethod
    def _entry_identity(entry):
        payload = entry or {}
        return (
            int(payload.get("start_ts") or 0),
            int(payload.get("stop_ts") or 0),
            clean_text(payload.get("upstream_episode_id") or payload.get("source_item_id")),
        )

    def _activate_runtime(self, runtime):
        if not runtime:
            return
        self.process = runtime.get("process")
        self.stderr_task = runtime.get("stderr_task")
        self._warm_task = runtime.get("warm_task")
        self.current_source = runtime.get("source")
        self.current_source_url = clean_text(runtime.get("input_target"))
        self.current_source_probe = {
            "container": "mpegts",
            "video_codec": clean_key(self.ingest_policy.get("video_codec")) or "h264",
            "audio_codec": clean_key(self.ingest_policy.get("audio_codec")) or "aac",
        }

    async def _close_runtime(self, runtime):
        if not runtime:
            return
        for task_name in ("prefetch_reader_task", "warm_task", "stderr_task"):
            task = runtime.get(task_name)
            if task is None or task.done():
                continue
            task.cancel()
            try:
                await task
            except BaseException:
                pass
        queue = runtime.get("prefetch_queue")
        if queue is not None:
            try:
                await queue.put_eof()
            except Exception:
                pass
        process = runtime.get("process")
        if process is not None and process.returncode is None:
            try:
                process.terminate()
                await _wait_process_exit_with_timeout(process, timeout_seconds=1.0)
            except Exception:
                try:
                    process.kill()
                    await _wait_process_exit_with_timeout(process, timeout_seconds=1.0)
                except Exception:
                    pass

    async def _buffer_prefetched_runtime(self, runtime):
        process = runtime.get("process")
        queue = runtime.get("prefetch_queue")
        if process is None or queue is None or process.stdout is None:
            return
        try:
            while self.running:
                chunk = await process.stdout.read(MPEGTS_CHUNK_BYTES)
                if not chunk:
                    break
                await queue.put_drop_oldest(chunk)
        finally:
            await queue.put_eof()

    async def _warm_next_item_cache(self, next_entry, remaining_seconds):
        if not next_entry:
            return
        next_start_ts = int(next_entry.get("start_ts") or 0)
        from backend.vod_channels import NEXT_ITEM_CACHE_WARM_SECONDS, resolve_vod_channel_playback_target

        wait_seconds = max(0, int(remaining_seconds or 0) - int(NEXT_ITEM_CACHE_WARM_SECONDS))
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
        owner_key = f"vod-channel-next-{self.channel_id}-{next_start_ts}"

        while self.running:
            now_ts = int(time.time())
            if next_start_ts > 0 and now_ts >= next_start_ts:
                return

            next_playback = await resolve_vod_channel_playback_target(
                self.config,
                self.channel_id,
                now_ts=next_start_ts or now_ts,
            )
            if next_playback:
                next_candidate = next_playback.get("candidate")
                next_upstream_url = clean_text(next_playback.get("upstream_url"))
                if next_candidate is not None and next_upstream_url:
                    warmed = await warm_vod_cache(
                        next_candidate,
                        next_upstream_url,
                        episode=next_playback.get("episode"),
                        owner_key=owner_key,
                    )
                    if warmed:
                        logger.info(
                            "VOD channel next-item cache warmed channel=%s start_ts=%s source_item_id=%s",
                            self.channel_id,
                            next_start_ts,
                            int((next_playback.get("entry") or {}).get("source_item_id") or 0),
                        )
                        return

            if next_start_ts > 0:
                remaining_to_start = next_start_ts - now_ts
                if remaining_to_start <= 1:
                    return
                await asyncio.sleep(min(5, max(1, remaining_to_start - 1)))
            else:
                await asyncio.sleep(5)

    async def _start_current_cache_download(self, candidate, upstream_url, entry):
        owner_key = f"vod-channel-current-{self.channel_id}-{int(entry.get('start_ts') or 0)}"
        warmed = await warm_vod_cache(candidate, upstream_url, owner_key=owner_key)
        source = await cso_source_from_vod_source(candidate, upstream_url)
        cache_entry = await vod_cache_manager.get_or_create(source, source.url)
        if warmed:
            try:
                await asyncio.wait_for(cache_entry.ready_event.wait(), timeout=10.0)
            except asyncio.TimeoutError:
                pass
            if not cache_entry.complete and not cache_entry.part_path.exists():
                for _ in range(10):
                    if cache_entry.part_path.exists():
                        break
                    await asyncio.sleep(0.2)
        return source, cache_entry

    async def _build_segment_runtime(self, playback, segment_index, activate_session_state=True):
        candidate = playback.get("candidate")
        upstream_url = clean_text(playback.get("upstream_url"))
        source_item = playback.get("source_item")
        entry = playback.get("entry") or {}
        next_entry = playback.get("next_entry")
        offset_seconds = max(0, int(playback.get("offset_seconds") or 0))
        remaining_seconds = max(1, int(entry.get("stop_ts") or 0) - int(time.time()))
        if candidate is None or not upstream_url or source_item is None:
            return None

        source, cache_entry = await self._start_current_cache_download(candidate, upstream_url, entry)
        input_target = source.url
        input_is_url = True
        input_label = "upstream"
        if cache_entry.complete and cache_entry.final_path.exists():
            input_target = str(cache_entry.final_path)
            input_is_url = False
            input_label = "cache"
        elif offset_seconds <= 0 and cache_entry.part_path.exists():
            input_target = str(cache_entry.part_path)
            input_is_url = False
            input_label = "cache_part"

        command = CsoFfmpegCommandBuilder(self.ingest_policy, pipe_output_format="mpegts").build_vod_channel_ingest_command(
            input_target,
            start_seconds=offset_seconds,
            max_duration_seconds=remaining_seconds,
            realtime=True,
            input_is_url=input_is_url,
            user_agent=_resolve_cso_ingest_user_agent(None, source),
            request_headers=_resolve_cso_ingest_headers(source),
            policy=self.ingest_policy,
        )
        logger.info(
            "Starting VOD channel ingest segment channel=%s start_ts=%s stop_ts=%s source_id=%s input=%s "
            "offset_seconds=%s duration_seconds=%s policy=(%s) command=%s",
            self.channel_id,
            int(entry.get("start_ts") or 0),
            int(entry.get("stop_ts") or 0),
            source.id,
            input_label,
            offset_seconds,
            remaining_seconds,
            _policy_log_label(self.ingest_policy),
            command,
        )
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        warm_task = asyncio.create_task(
            self._warm_next_item_cache(next_entry, remaining_seconds),
            name=f"vod-channel-next-{self.channel_id}-{segment_index}",
        )
        stderr_task = asyncio.create_task(
            self._read_stderr(process, entry),
            name=f"vod-channel-stderr-{self.channel_id}-{segment_index}",
        )
        runtime = {
            "process": process,
            "stderr_task": stderr_task,
            "warm_task": warm_task,
            "entry": entry,
            "playback": playback,
            "source": source,
            "input_target": input_target,
        }
        if activate_session_state:
            self._activate_runtime(runtime)
        return runtime

    async def _prepare_next_segment_runtime(self, playback, segment_index):
        next_entry = (playback or {}).get("next_entry")
        if not next_entry:
            return None

        next_identity = self._entry_identity(next_entry)
        next_start_ts = int(next_entry.get("start_ts") or 0)
        if next_start_ts <= 0:
            return None

        prestart_ts = max(0, next_start_ts - int(VOD_CHANNEL_NEXT_SEGMENT_PRESTART_SECONDS))
        while self.running:
            remaining_seconds = prestart_ts - int(time.time())
            if remaining_seconds <= 0:
                break
            await asyncio.sleep(min(1.0, float(remaining_seconds)))

        if not self.running:
            return None

        from backend.vod_channels import resolve_vod_channel_playback_target

        prepared_playback = await resolve_vod_channel_playback_target(
            self.config,
            self.channel_id,
            now_ts=next_start_ts,
        )
        if not prepared_playback:
            return None
        if self._entry_identity((prepared_playback.get("entry") or {})) != next_identity:
            return None

        runtime = await self._build_segment_runtime(
            prepared_playback,
            segment_index,
            activate_session_state=False,
        )
        if runtime is None:
            return None

        prefetch_queue = ByteBudgetQueue(max_bytes=VOD_CHANNEL_NEXT_SEGMENT_BUFFER_BYTES)
        runtime["prefetch_queue"] = prefetch_queue
        runtime["prefetch_reader_task"] = asyncio.create_task(
            self._buffer_prefetched_runtime(runtime),
            name=f"vod-channel-prefetch-{self.channel_id}-{segment_index}",
        )
        logger.info(
            "Prepared next VOD channel segment channel=%s start_ts=%s source_item_id=%s prestart_seconds=%s",
            self.channel_id,
            next_start_ts,
            int(next_entry.get("source_item_id") or 0),
            int(VOD_CHANNEL_NEXT_SEGMENT_PRESTART_SECONDS),
        )
        return runtime

    async def _take_prepared_runtime(self, prepared_task):
        if prepared_task is None:
            return None
        if not prepared_task.done():
            prepared_task.cancel()
            try:
                await prepared_task
            except BaseException:
                pass
            return None
        try:
            return prepared_task.result()
        except BaseException:
            return None

    async def _wait_for_next_playback(self, current_entry):
        from backend.vod_channels import resolve_vod_channel_playback_target

        current_key = (
            int(current_entry.get("start_ts") or 0),
            int(current_entry.get("stop_ts") or 0),
            clean_text(current_entry.get("upstream_episode_id") or current_entry.get("source_item_id")),
        )
        boundary_ts = int(current_entry.get("stop_ts") or 0)
        for attempt in range(8):
            now_ts = int(time.time())
            if boundary_ts > 0 and now_ts < boundary_ts:
                await asyncio.sleep(min(1.0, float(boundary_ts - now_ts)))
            playback = await resolve_vod_channel_playback_target(self.config, self.channel_id)
            if not playback:
                return None
            next_entry = playback.get("entry") or {}
            next_key = (
                int(next_entry.get("start_ts") or 0),
                int(next_entry.get("stop_ts") or 0),
                clean_text(next_entry.get("upstream_episode_id") or next_entry.get("source_item_id")),
            )
            if next_key != current_key:
                logger.info(
                    "VOD channel continuing next programme channel=%s previous_stop_ts=%s next_start_ts=%s source_item_id=%s",
                    self.channel_id,
                    boundary_ts,
                    int(next_entry.get("start_ts") or 0),
                    int(next_entry.get("source_item_id") or 0),
                )
                return playback
            if boundary_ts > 0 and int(time.time()) + 2 < boundary_ts:
                logger.warning(
                    "VOD channel segment ended early; resuming current programme channel=%s start_ts=%s stop_ts=%s "
                    "attempt=%s",
                    self.channel_id,
                    int(current_entry.get("start_ts") or 0),
                    boundary_ts,
                    attempt + 1,
                )
                return playback
            if attempt < 7:
                await asyncio.sleep(1)
        logger.warning(
            "VOD channel playback did not advance at boundary channel=%s start_ts=%s stop_ts=%s",
            self.channel_id,
            int(current_entry.get("start_ts") or 0),
            int(current_entry.get("stop_ts") or 0),
        )
        return None

    async def _close_active_segment(self):
        runtime = {
            "process": self.process,
            "warm_task": self._warm_task,
            "stderr_task": self.stderr_task,
        }
        self.process = None
        self._warm_task = None
        self.stderr_task = None
        await self._close_runtime(runtime)

    async def _run_loop(self):
        from backend.vod_channels import resolve_vod_channel_playback_target

        startup_event = self._startup_event
        playback = await resolve_vod_channel_playback_target(self.config, self.channel_id)
        if not playback:
            self.last_error = "no_scheduled_programme"
            self.running = False
            if startup_event is not None:
                startup_event.set()
            await self._finish_session()
            return

        segment_index = 0
        try:
            prepared_runtime = None
            while self.running and playback:
                current_playback = playback
                runtime = prepared_runtime
                if runtime is None:
                    runtime = await self._build_segment_runtime(current_playback, segment_index)
                else:
                    self._activate_runtime(runtime)
                prepared_runtime = None
                if runtime is None:
                    self.last_error = "vod_channel_segment_unavailable"
                    self.running = False
                    if startup_event is not None:
                        startup_event.set()
                    break
                if startup_event is not None and not startup_event.is_set():
                    self._startup_succeeded = True
                    startup_event.set()

                process = runtime["process"]
                self.stderr_task = runtime["stderr_task"]
                self._warm_task = runtime["warm_task"]
                current_entry = runtime["entry"]
                prepared_task = asyncio.create_task(
                    self._prepare_next_segment_runtime(current_playback, segment_index + 1),
                    name=f"vod-channel-prepare-{self.channel_id}-{segment_index + 1}",
                )
                saw_data = False
                self.current_segment_healthy = False
                try:
                    prefetch_queue = runtime.get("prefetch_queue")
                    while self.running:
                        if prefetch_queue is not None:
                            chunk = await prefetch_queue.get()
                        elif process.stdout is not None:
                            chunk = await process.stdout.read(MPEGTS_CHUNK_BYTES)
                        else:
                            break
                        if not chunk:
                            break
                        if not self.first_healthy_stream_seen:
                            logger.info(
                                "VOD channel ingest first chunk channel=%s ingest_key=%s bytes=%s elapsed_ms=%s",
                                self.channel_id,
                                self.key,
                                len(chunk),
                                int(max(0.0, time.time() - float(self.session_start_ts or time.time())) * 1000),
                            )
                            self.first_healthy_stream_seen = True
                        self.current_segment_healthy = True
                        saw_data = True
                        await self._broadcast(chunk)
                finally:
                    return_code = None
                    try:
                        return_code = process.returncode
                        if return_code is None:
                            return_code = await process.wait()
                    except Exception:
                        return_code = None
                    self.last_reader_end_reason = "ingest_reader_ended"
                    self.last_reader_end_saw_data = saw_data
                    self.last_reader_end_return_code = return_code
                    self.last_reader_end_ts = time.time()
                    if self.stderr_task is not None and not self.stderr_task.done():
                        self.stderr_task.cancel()
                        try:
                            await self.stderr_task
                        except BaseException:
                            pass
                        self.stderr_task = None

                if not self.running:
                    await self._close_runtime(await self._take_prepared_runtime(prepared_task))
                    break
                next_playback = await self._wait_for_next_playback(current_entry)
                prepared_runtime = await self._take_prepared_runtime(prepared_task)
                if prepared_runtime is not None and (
                    next_playback is None
                    or self._entry_identity(prepared_runtime.get("entry"))
                    != self._entry_identity(next_playback.get("entry"))
                ):
                    await self._close_runtime(prepared_runtime)
                    prepared_runtime = None
                playback = next_playback
                segment_index += 1
            if startup_event is not None and not startup_event.is_set():
                startup_event.set()
        finally:
            self.running = False
            await self._finish_session()

    async def _finish_session(self):
        await self._close_active_segment()
        async with self.lock:
            subscribers = list(self.subscribers.values())
            self.subscribers = {}
            self.current_source = None
            self.current_source_url = ""
            self.current_source_probe = {}
            self.history.clear()
            self.history_bytes = 0
            self.failover_exhausted = bool(self.last_error)
        for queue in subscribers:
            await queue.put_eof()

    async def stop(self, force=False):
        async with self.lock:
            if not self.running and self.segment_task is None and not self.subscribers:
                return
            if not force and self.subscribers:
                return
            self.running = False
            segment_task = self.segment_task
            self.segment_task = None
        await self._close_active_segment()
        if segment_task is not None and not segment_task.done():
            segment_task.cancel()
            try:
                await segment_task
            except BaseException:
                pass
        await self._finish_session()


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
        title, subtitle = _cso_unavailable_slate_message(self.reason, detail_hint=self.detail_hint)
        command = CsoFfmpegCommandBuilder().build_slate_command(
            self.reason,
            primary_text=title,
            secondary_text=subtitle,
            duration_seconds=self.duration_seconds,
            output_target="pipe:1",
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

    async def _ensure_ingest_queue(self, prebuffer_bytes=0):
        if self.use_slate_as_input or self.ingest_session is None:
            return False
        if self.ingest_queue is not None:
            return True
        if not self.running or not self.ingest_session.running:
            return False
        try:
            self.ingest_queue = await self.ingest_session.add_subscriber(
                self.key,
                prebuffer_bytes=int(prebuffer_bytes or 0),
            )
        except Exception as exc:
            logger.warning(
                "CSO output failed to reattach ingest subscriber channel=%s output_key=%s error=%s",
                self.channel_id,
                self.key,
                exc,
            )
            return False
        logger.info(
            "CSO output reattached ingest subscriber channel=%s output_key=%s prebuffer_bytes=%s",
            self.channel_id,
            self.key,
            int(prebuffer_bytes or 0),
        )
        return True

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
                self.ingest_queue = await self.ingest_session.add_subscriber(
                    self.key,
                    prebuffer_bytes=int(CSO_INGEST_SUBSCRIBER_PREBUFFER_BYTES),
                )
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
                pipe_input_format = _resolve_vod_pipe_container(
                    getattr(self.ingest_session, "current_source", None),
                    source_probe=getattr(self.ingest_session, "current_source_probe", None),
                )
                command = CsoFfmpegCommandBuilder(
                    self.output_policy,
                    pipe_input_format=pipe_input_format,
                ).build_output_command()
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
                if self.ingest_session.running:
                    await self._ensure_ingest_queue(prebuffer_bytes=int(CSO_INGEST_SUBSCRIBER_PREBUFFER_BYTES))
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
                        recovered = await self._ensure_ingest_queue(
                            prebuffer_bytes=int(CSO_INGEST_SUBSCRIBER_PREBUFFER_BYTES)
                        )
                        if recovered:
                            continue
                        if self.ingest_session is not None and self.running:
                            await asyncio.sleep(float(CSO_OUTPUT_SLATE_POLL_INTERVAL_SECONDS))
                            continue
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
                        source=(getattr(self.ingest_session, "current_source", None) or self.event_source),
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
            pipe_input_format = _resolve_vod_pipe_container(
                getattr(self.ingest_session, "current_source", None),
                source_probe=getattr(self.ingest_session, "current_source_probe", None),
            )
            command = CsoFfmpegCommandBuilder(
                self.policy,
                pipe_input_format=pipe_input_format,
            ).build_hls_output_command(self.output_dir)
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
    endpoint_value = clean_text(endpoint)
    display_url_value = clean_key(display_url)
    if "/tic-api/cso/channel/" in endpoint_value or "/tic-api/cso/channel_stream/" in endpoint_value:
        return True
    if endpoint_value.startswith("/xc/movie/") or endpoint_value.startswith("/xc/series/"):
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
            candidates = await order_cso_channel_sources(
                list(channel.sources or []), channel_id=getattr(channel, "id", None)
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
                resolved_url = str(response.url or url)
    except Exception:
        return []
    return _parse_hls_playlist_variants(resolved_url, payload)


def _parse_hls_playlist_variants(base_url, payload):
    lines = [line.strip() for line in (payload or "").splitlines() if line.strip()]
    variants = []
    pending_bandwidth = None
    pending_width = 0
    pending_height = 0
    is_media_playlist = False
    for line in lines:
        if line.startswith("#EXT-X-STREAM-INF:"):
            bandwidth_match = _HLS_BANDWIDTH_RE.search(line)
            pending_bandwidth = int(bandwidth_match.group(1)) if bandwidth_match else 0
            resolution_match = _HLS_RESOLUTION_RE.search(line)
            pending_width = int(resolution_match.group(1)) if resolution_match else 0
            pending_height = int(resolution_match.group(2)) if resolution_match else 0
            continue
        if line.startswith(("#EXTINF", "#EXT-X-TARGETDURATION", "#EXT-X-MEDIA-SEQUENCE")):
            is_media_playlist = True
        if line.startswith("#"):
            continue
        if pending_bandwidth is None:
            # Not a master playlist variant entry.
            continue
        variant_url = urljoin(base_url, line)
        variants.append(
            {
                "bandwidth": pending_bandwidth,
                "width": pending_width,
                "height": pending_height,
                "program_index": len(variants),
                "variant_url": variant_url,
                "ffmpeg_program_index": 0,
                "playlist_type": "master",
            }
        )
        pending_bandwidth = None
        pending_width = 0
        pending_height = 0
    if variants:
        variants.sort(key=lambda item: int(item.get("bandwidth") or 0))
        return variants
    if is_media_playlist:
        return [
            {
                "bandwidth": 0,
                "width": 0,
                "height": 0,
                "program_index": 0,
                "variant_url": base_url,
                "ffmpeg_program_index": 0,
                "playlist_type": "media",
            }
        ]
    return []


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
    resolved_duration = _cso_unavailable_duration_seconds(reason_key)
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
    resolved_duration = _cso_unavailable_duration_seconds(reason_key)

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

    sources = await order_cso_channel_sources(list(channel.sources or []), channel_id=getattr(channel, "id", None))
    sources = [cso_source_from_channel_source(s) for s in sources]

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
    try:
        from quart import current_app

        ingest_session.app = current_app._get_current_object()
    except Exception:
        pass
    await ingest_session.start()
    if not ingest_session.running:
        reason = ingest_session.last_error or "no_available_source"
        await emit_channel_stream_event(
            channel_id=channel.id,
            source=sources[0] if sources else None,
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
            source=getattr(ingest_session, "current_source", None),
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
            source=getattr(ingest_session, "current_source", None),
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

    source = cso_source_from_channel_source(source)

    playlist = source.playlist
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return None, "Stream playlist is disabled", 404
    if not source.url:
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
            source=source,
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
            source=source,
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
            source=source,
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

    sources = await order_cso_channel_sources(list(channel.sources or []), channel_id=getattr(channel, "id", None))
    sources = [cso_source_from_channel_source(s) for s in sources]

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
    allow_unavailable_slate = should_allow_unavailable_slate(
        profile, channel=channel, source=sources[0] if sources else None
    )

    # Init slate pipeline
    slate_session = None
    if CSO_UNAVAILABLE_SHOW_SLATE and allow_unavailable_slate:
        slate_session = CsoSlateSession(
            f"{output_session_key}-slate",
            config_path=config.config_path,
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
    try:
        from quart import current_app

        ingest_session.app = current_app._get_current_object()
    except Exception:
        pass
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
        source=ingest_session.current_source,
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
                source=getattr(ingest_session, "current_source", None),
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


async def subscribe_vod_stream(
    config,
    candidate,
    upstream_url,
    stream_key,
    profile,
    connection_id,
    episode=None,
    prebuffer_bytes=0,
    request_base_url="",
):
    """Subscribe a playback client to a VOD item/episode CSO output session."""
    if not candidate or not candidate.group_item:
        return build_cso_stream_plan(None, None, "VOD item not found", 404)

    item = candidate.group_item
    source = await cso_source_from_vod_source(candidate, upstream_url)

    if not source:
        return build_cso_stream_plan(None, None, "Source not found", 404)

    playlist = source.playlist
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return build_cso_stream_plan(None, None, "Source playlist is disabled", 404)

    if not source.url:
        return build_cso_stream_plan(None, None, "No available stream source", 503)

    source_id = source.id
    sources = [source]

    policy = generate_cso_policy_from_profile(config, profile)
    ingest_key = f"cso-vod-ingest-{source_id}"
    output_session_key = f"cso-vod-output-{source_id}-{profile}"
    capacity_owner_key = f"cso-vod-{source_id}"
    username = await _resolve_username_for_stream_key(config, stream_key)
    ingest_user_agent = _resolve_cso_ingest_user_agent(config, source)

    # Init slate pipeline
    slate_session = None
    if CSO_UNAVAILABLE_SHOW_SLATE:
        slate_session = CsoSlateSession(
            f"{output_session_key}-slate",
            config_path=config.config_path,
        )

    # Init ingest pipeline
    def _ingest_factory():
        return CsoIngestSession(
            ingest_key,
            None,  # channel_id is None for VOD
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
    try:
        from quart import current_app

        ingest_session.app = current_app._get_current_object()
    except Exception:
        pass
    ingest_session.slate_session = slate_session
    await ingest_session.start()

    if not ingest_session.running:
        reason = ingest_session.last_error or "no_available_source"
        await emit_channel_stream_event(
            vod_category_id=item.category_id,
            vod_item_id=item.id,
            vod_episode_id=episode.id if episode else None,
            session_id=ingest_key,
            event_type="capacity_blocked" if reason == "capacity_blocked" else "playback_unavailable",
            severity="warning",
            details={"reason": reason, "profile": profile},
        )
        detail_hint = summarize_cso_playback_issue(reason) if reason == "playback_unavailable" else ""
        plan = ingest_session.build_unavailable_stream_plan(
            policy,
            reason,
            detail_hint=detail_hint,
            profile_name=profile,
            status_code=503,
        )
        if plan.generator is not None:
            return plan
        return build_cso_stream_plan(None, None, "VOD unavailable because playback could not be started", 503)

    # Init output pipeline
    def _output_factory():
        return CsoOutputSession(
            output_session_key,
            None,  # channel_id is None for VOD
            policy,
            ingest_session,
            slate_session,
        )

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
    output_session.slate_session = slate_session
    output_session.event_source = source
    await output_session.start()

    if not output_session.running:
        reason = output_session.last_error or "output_not_running"
        await emit_channel_stream_event(
            vod_category_id=item.category_id,
            vod_item_id=item.id,
            vod_episode_id=episode.id if episode else None,
            session_id=output_session_key,
            event_type="playback_unavailable",
            severity="warning",
            details={"reason": reason, "profile": profile},
        )
        detail_hint = summarize_cso_playback_issue(reason)
        plan = ingest_session.build_unavailable_stream_plan(
            policy,
            "playback_unavailable",
            detail_hint=detail_hint,
            profile_name=profile,
            status_code=503,
        )
        if plan.generator is not None:
            return plan
        return build_cso_stream_plan(None, None, "VOD unavailable because output pipeline could not be started", 503)

    queue = await output_session.add_client(connection_id, prebuffer_bytes=prebuffer_bytes)
    content_type = policy_content_type(policy)

    async def _generator():
        emitted_chunks = 0
        emitted_bytes = 0
        last_chunk_ts = 0.0
        last_progress_log_ts = 0.0
        try:
            while True:
                chunk = await queue.get()
                if chunk is None:
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
                        "CSO VOD output consumer progress item=%s output_key=%s connection_id=%s yielded_chunks=%s yielded_bytes=%s yield_gap_ms=%s queue_items=%s queue_bytes=%s queue_max_bytes=%s queue_oldest_age_ms=%s",
                        item.id,
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
                vod_category_id=item.category_id,
                vod_item_id=item.id,
                vod_episode_id=episode.id if episode else None,
                source=getattr(ingest_session, "current_source", None),
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


async def subscribe_vod_hls(
    config,
    candidate,
    upstream_url,
    stream_key,
    profile,
    connection_id,
    episode=None,
    request_base_url="",
    on_disconnect=None,
):
    """Subscribe a playback client to a VOD item/episode CSO HLS output session."""
    if not candidate or not candidate.group_item:
        return None, "VOD item not found", 404

    item = candidate.group_item
    source = await cso_source_from_vod_source(candidate, upstream_url)

    if not source:
        return None, "Source not found", 404

    playlist = source.playlist
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return None, "Source playlist is disabled", 404
    if not source.url:
        return None, "No available stream source", 503

    source_id = source.id
    sources = [source]
    policy = generate_cso_policy_from_profile(config, profile)
    ingest_key = f"cso-vod-ingest-{source_id}"
    output_session_key = f"cso-vod-hls-output-{source_id}-{profile}"
    capacity_owner_key = f"cso-vod-{source_id}"
    username = await _resolve_username_for_stream_key(config, stream_key)
    ingest_user_agent = _resolve_cso_ingest_user_agent(config, source)

    def _ingest_factory():
        return CsoIngestSession(
            ingest_key,
            None,  # channel_id is None for VOD
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
            vod_category_id=item.category_id,
            vod_item_id=item.id,
            vod_episode_id=episode.id if episode else None,
            session_id=ingest_key,
            event_type="capacity_blocked" if reason == "capacity_blocked" else "playback_unavailable",
            severity="warning",
            details={"reason": reason, "profile": profile},
        )
        return None, "VOD unavailable because playback could not be started", 503

    def _output_factory():
        return CsoHlsOutputSession(
            output_session_key,
            None,  # channel_id is None for VOD
            policy,
            ingest_session,
            cache_root_dir=os.path.join(config.config_path, "cache", "cso_hls"),
        )

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
    await output_session.start()
    if not output_session.running:
        reason = output_session.last_error or "output_not_running"
        await emit_channel_stream_event(
            vod_category_id=item.category_id,
            vod_item_id=item.id,
            vod_episode_id=episode.id if episode else None,
            session_id=output_session_key,
            event_type="playback_unavailable",
            severity="warning",
            details={"reason": reason, "profile": profile},
        )
        return None, "VOD unavailable because output pipeline could not be started", 503

    is_new_client = await output_session.add_client(connection_id, on_disconnect=on_disconnect)
    if is_new_client:
        await emit_channel_stream_event(
            vod_category_id=item.category_id,
            vod_item_id=item.id,
            vod_episode_id=episode.id if episode else None,
            source=getattr(ingest_session, "current_source", None),
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


async def subscribe_vod_proxy_stream(
    candidate,
    upstream_url,
    connection_id,
    request_headers=None,
    episode=None,
    source_override=None,
):
    if not candidate:
        return CsoStreamPlan(None, None, "VOD item not found", 404)

    if isinstance(candidate, VodCuratedPlaybackCandidate):
        item = candidate.group_item
        vod_category_id = item.category_id
        vod_item_id = item.id
    else:
        item = None
        vod_category_id = None
        vod_item_id = None
    source = source_override or await cso_source_from_vod_source(candidate, upstream_url)
    if not source:
        return CsoStreamPlan(None, None, "Source not found", 404)

    playlist = source.playlist
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return CsoStreamPlan(None, None, "Source playlist is disabled", 404)
    local_cache_ready = False
    if not source.url:
        entry = await vod_cache_manager.get_or_create(source, upstream_url or "")
        local_cache_ready = bool(entry.complete and entry.final_path.exists())
    if not source.url and not local_cache_ready:
        return CsoStreamPlan(None, None, "No available stream source", 503)

    session_key = f"vod-proxy-{source.id}-{connection_id}"
    session = await vod_proxy_session_manager.create(
        session_key,
        source,
        source.url,
        request_headers=request_headers,
    )
    started = await session.start()
    if not started:
        reason = session.last_error or "proxy_start_failed"
        await emit_channel_stream_event(
            vod_category_id=vod_category_id,
            vod_item_id=vod_item_id,
            vod_episode_id=episode.id if episode else None,
            source=source,
            session_id=session_key,
            event_type="capacity_blocked" if reason == "capacity_blocked" else "playback_unavailable",
            severity="warning",
            details={
                "reason": reason,
                **_source_event_context(source, source_url=source.url),
            },
        )
        await session.stop(force=True)
        return CsoStreamPlan(
            None,
            None,
            "Source capacity limit reached" if reason == "capacity_blocked" else "Unable to start proxy stream",
            503 if reason == "capacity_blocked" else 502,
        )

    await emit_channel_stream_event(
        vod_category_id=vod_category_id,
        vod_item_id=vod_item_id,
        vod_episode_id=episode.id if episode else None,
        source=source,
        session_id=session_key,
        event_type="session_start",
        severity="info",
        details={
            "mode": "proxy",
            "connection_id": connection_id,
            **_source_event_context(source, source_url=source.url),
        },
    )

    async def _generator():
        try:
            async for chunk in session.iter_bytes():
                yield chunk
        finally:
            await emit_channel_stream_event(
                vod_category_id=vod_category_id,
                vod_item_id=vod_item_id,
                vod_episode_id=episode.id if episode else None,
                source=source,
                session_id=session_key,
                event_type="session_end",
                severity="info",
                details={
                    "mode": "proxy",
                    "connection_id": connection_id,
                    **_source_event_context(source, source_url=source.url),
                },
            )

    return CsoStreamPlan(
        _generator(),
        session.content_type or policy_content_type({"container": source.container_extension or "matroska"}),
        None,
        int(session.status_code or 200),
        headers=session.response_headers,
    )


async def subscribe_vod_proxy_output_stream(
    config,
    candidate,
    upstream_url,
    stream_key,
    profile,
    connection_id,
    start_seconds=0,
    max_duration_seconds=None,
    request_headers=None,
    episode=None,
):
    if not candidate:
        return build_cso_stream_plan(None, None, "VOD item not found", 404)

    if isinstance(candidate, VodCuratedPlaybackCandidate):
        item = candidate.group_item
        item_id = item.id
    else:
        item = None
        item_id = None
    source = await cso_source_from_vod_source(candidate, upstream_url)
    if not source:
        return build_cso_stream_plan(None, None, "Source not found", 404)

    playlist = source.playlist
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return build_cso_stream_plan(None, None, "Source playlist is disabled", 404)

    if not source.url:
        return build_cso_stream_plan(None, None, "No available stream source", 503)

    policy = generate_cso_policy_from_profile(config, profile)
    proxy_session_key = f"vod-proxy-output-{source.id}-{connection_id}"
    cache_entry = await vod_cache_manager.get_or_create(source, source.url)
    local_cache_ready = bool(cache_entry.complete and cache_entry.final_path.exists())
    using_local_cache = local_cache_ready
    proxy_session = None

    if using_local_cache:
        command = CsoFfmpegCommandBuilder(
            policy,
            pipe_input_format=_resolve_vod_pipe_container(source, source_probe=getattr(source, "probe_details", None)),
        ).build_local_output_command(
            cache_entry.final_path,
            start_seconds=start_seconds,
            max_duration_seconds=max_duration_seconds,
            realtime=True,
        )
        logger.info(
            "Starting VOD local output stream item=%s source_id=%s profile=%s start_seconds=%s duration_seconds=%s path=%s command=%s",
            item_id,
            source.id,
            profile,
            int(start_seconds or 0),
            int(max_duration_seconds or 0) if max_duration_seconds is not None else None,
            cache_entry.final_path,
            command,
        )
    else:
        proxy_request_headers = dict(request_headers or {})
        proxy_request_headers.pop("Range", None)
        proxy_session = await vod_proxy_session_manager.create(
            proxy_session_key,
            source,
            source.url,
            request_headers=proxy_request_headers,
        )
        started = await proxy_session.start()
        if not started:
            reason = proxy_session.last_error or "proxy_start_failed"
            await emit_channel_stream_event(
                source=source,
                session_id=proxy_session_key,
                event_type="capacity_blocked" if reason == "capacity_blocked" else "playback_unavailable",
                severity="warning",
                details={
                    "reason": reason,
                    "profile": profile,
                    "mode": "proxy_output",
                    **_source_event_context(source, source_url=source.url),
                },
            )
            await proxy_session.stop(force=True)
            return build_cso_stream_plan(
                None,
                None,
                "Source capacity limit reached" if reason == "capacity_blocked" else "Unable to start proxy stream",
                503 if reason == "capacity_blocked" else 502,
            )

        pipe_input_format = _resolve_vod_pipe_container(source, source_probe=getattr(source, "probe_details", None))
        command = CsoFfmpegCommandBuilder(
            policy,
            pipe_input_format=pipe_input_format,
        ).build_output_command(
            start_seconds=start_seconds,
            max_duration_seconds=max_duration_seconds,
        )
        logger.info(
            "Starting VOD proxy output stream item=%s source_id=%s profile=%s start_seconds=%s duration_seconds=%s command=%s",
            item_id,
            source.id,
            profile,
            int(start_seconds or 0),
            int(max_duration_seconds or 0) if max_duration_seconds is not None else None,
            command,
        )
    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except Exception as exc:
        if proxy_session is not None:
            await proxy_session.stop(force=True)
        await emit_channel_stream_event(
            source=source,
            session_id=proxy_session_key,
            event_type="playback_unavailable",
            severity="warning",
            details={
                "reason": f"output_start_failed:{exc}",
                "profile": profile,
                "mode": "proxy_output",
                **_source_event_context(source, source_url=source.url),
            },
        )
        return build_cso_stream_plan(None, None, "Unable to start CSO output stream", 503)

    await emit_channel_stream_event(
        source=source,
        session_id=proxy_session_key,
        event_type="session_start",
        severity="info",
        details={
            "profile": profile,
            "connection_id": connection_id,
            "mode": "proxy_output",
            **_source_event_context(source, source_url=source.url),
        },
    )

    async def _write_proxy_to_ffmpeg():
        if proxy_session is None:
            return
        try:
            async for chunk in proxy_session.iter_bytes():
                if not isinstance(chunk, (bytes, bytearray, memoryview)):
                    continue
                if process.stdin is None or process.returncode is not None:
                    break
                process.stdin.write(chunk)
                await process.stdin.drain()
        except Exception as exc:
            logger.warning(
                "VOD proxy output writer interrupted item=%s source_id=%s error=%s",
                getattr(item, "id", None),
                getattr(source, "id", None),
                exc,
            )
        finally:
            if process.stdin is not None:
                try:
                    process.stdin.close()
                except Exception:
                    pass

    async def _log_stderr():
        if process.stderr is None:
            return
        while True:
            line = await process.stderr.readline()
            if not line:
                break
            text = line.decode(errors="ignore").rstrip()
            if CsoOutputSession._should_log_ffmpeg_stderr_line(text):
                logger.info("[vod-proxy-output-ffmpeg] %s", text)

    writer_task = asyncio.create_task(_write_proxy_to_ffmpeg(), name=f"vod-proxy-output-writer-{connection_id}")
    stderr_task = asyncio.create_task(_log_stderr(), name=f"vod-proxy-output-stderr-{connection_id}")

    async def _generator():
        try:
            while True:
                if process.stdout is None:
                    break
                chunk = await process.stdout.read(64 * 1024)
                if not chunk:
                    break
                yield chunk
        finally:
            if not writer_task.done():
                writer_task.cancel()
            if not stderr_task.done():
                stderr_task.cancel()
            if proxy_session is not None:
                await proxy_session.stop(force=True)
            if process.returncode is None:
                process.kill()
                await process.wait()
            await emit_channel_stream_event(
                source=source,
                session_id=proxy_session_key,
                event_type="session_end",
                severity="info",
                details={
                    "profile": profile,
                    "connection_id": connection_id,
                    "mode": "proxy_output",
                    **_source_event_context(source, source_url=source.url),
                },
            )

    return build_cso_stream_plan(_generator(), policy_content_type(policy), None, 200)


async def subscribe_vod_channel_output_stream(
    config: Any,
    channel_id: int,
    stream_key: str,
    profile: str,
    connection_id: str,
    request_headers: dict[str, str] | None = None,
) -> Any:
    requested_policy = generate_cso_policy_from_profile(config, profile)
    ingest_policy = _generate_vod_channel_ingest_policy(config, requested_policy)
    policy = _resolve_vod_channel_output_policy(requested_policy, ingest_policy=ingest_policy)
    ingest_key = f"cso-vod-channel-ingest-{int(channel_id)}"
    output_session_key = f"cso-vod-channel-output-{int(channel_id)}-{profile}"

    def _ingest_factory():
        return VodChannelIngestSession(
            ingest_key,
            config,
            int(channel_id),
            stream_key=stream_key,
            request_headers=request_headers,
            output_policy=policy,
        )

    ingest_session = await cso_session_manager.get_or_create_ingest(ingest_key, _ingest_factory)
    await ingest_session.start()
    if not ingest_session.running:
        reason = ingest_session.last_error or "no_scheduled_programme"
        if reason == "no_scheduled_programme":
            return build_cso_stream_plan(None, None, "No scheduled VOD programme is currently available", 404)
        return build_cso_stream_plan(None, None, "Unable to start VOD channel ingest", 503)

    def _output_factory():
        return CsoOutputSession(
            output_session_key,
            int(channel_id),
            policy,
            ingest_session,
        )

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
    await output_session.start()
    if not output_session.running:
        return build_cso_stream_plan(None, None, "Unable to start VOD channel output", 503)

    queue = await output_session.add_client(connection_id, prebuffer_bytes=0)
    content_type = policy_content_type(policy)

    async def _generator():
        try:
            while True:
                chunk = await queue.get()
                if chunk is None:
                    break
                yield chunk
                await output_session.touch_client(connection_id)
        finally:
            await output_session.remove_client(connection_id)

    return build_cso_stream_plan(_generator(), content_type, None, 200)


async def subscribe_vod_channel_hls(
    config: Any,
    channel_id: int,
    stream_key: str,
    profile: str,
    connection_id: str,
    request_headers: dict[str, str] | None = None,
    on_disconnect: Any = None,
) -> tuple[Any | None, str | None, int]:
    requested_policy = generate_cso_policy_from_profile(config, profile)
    ingest_policy = _generate_vod_channel_ingest_policy(config, requested_policy)
    policy = _resolve_vod_channel_output_policy(requested_policy, ingest_policy=ingest_policy)
    ingest_key = f"cso-vod-channel-ingest-{int(channel_id)}"
    output_session_key = f"cso-vod-channel-hls-output-{int(channel_id)}-{profile}"

    def _ingest_factory():
        return VodChannelIngestSession(
            ingest_key,
            config,
            int(channel_id),
            stream_key=stream_key,
            request_headers=request_headers,
            output_policy=policy,
        )

    ingest_session = await cso_session_manager.get_or_create_ingest(ingest_key, _ingest_factory)
    await ingest_session.start()
    if not ingest_session.running:
        reason = ingest_session.last_error or "no_scheduled_programme"
        if reason == "no_scheduled_programme":
            return None, "No scheduled VOD programme is currently available", 404
        return None, "Unable to start VOD channel ingest", 503

    def _output_factory():
        return CsoHlsOutputSession(
            output_session_key,
            int(channel_id),
            policy,
            ingest_session,
            cache_root_dir=os.path.join(config.config_path, "cache", "cso_hls"),
        )

    output_session = await cso_session_manager.get_or_create_output(output_session_key, _output_factory)
    await output_session.start()
    if not output_session.running:
        return None, "Unable to start VOD channel HLS output", 503

    is_new_client = await output_session.add_client(connection_id, on_disconnect=on_disconnect)
    if is_new_client:
        await emit_channel_stream_event(
            channel_id=int(channel_id),
            source=getattr(ingest_session, "current_source", None),
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

    source = cso_source_from_channel_source(source)

    playlist = source.playlist
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return build_cso_stream_plan(None, None, "Source playlist is disabled", 404)

    if not source.url:
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
    allow_unavailable_slate = should_allow_unavailable_slate(
        profile, channel=getattr(source, "channel", None), source=source
    )

    # Init slate pipeline
    slate_session = None
    if CSO_UNAVAILABLE_SHOW_SLATE and allow_unavailable_slate:
        slate_session = CsoSlateSession(
            f"{output_session_key}-slate",
            config_path=config.config_path,
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
            source=source,
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
            source=source,
            session_id=output_session_key,
            event_type="playback_unavailable",
            severity="warning",
            details={"reason": reason, "profile": profile},
        )
        detail_hint = summarize_cso_playback_issue(reason)
        plan = ingest_session.build_unavailable_stream_plan(
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
        source=source,
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
                source=source,
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
