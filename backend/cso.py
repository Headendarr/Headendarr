#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import base64
import json
import logging
import re
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
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

logger = logging.getLogger("cso")
CSO_SOURCE_HOLD_DOWN_SECONDS = 20
CSO_STALL_SECONDS_DEFAULT = 8
CSO_UNDERSPEED_RATIO_DEFAULT = 0.9
CSO_UNDERSPEED_WINDOW_SECONDS_DEFAULT = 12
CSO_STARTUP_GRACE_SECONDS_DEFAULT = 8
_FFMPEG_SPEED_RE = re.compile(r"speed=\s*([0-9.]+)x")
_HLS_BANDWIDTH_RE = re.compile(r"BANDWIDTH=(\d+)")
_HLS_RESOLUTION_RE = re.compile(r"RESOLUTION=(\d+)x(\d+)")
_HLS_STARTUP_RAMP_INTERVAL_SECONDS = 4


DEFAULT_CSO_POLICY = {
    "output_mode": "auto",
    "container": "mpegts",
    "video_codec": "",
    "audio_codec": "",
    "subtitle_mode": "copy",
    "data_mode": "copy",
    "hwaccel": "none",
    "deinterlace": False,
    "stall_seconds": CSO_STALL_SECONDS_DEFAULT,
    "under_speed_ratio": CSO_UNDERSPEED_RATIO_DEFAULT,
    "under_speed_window_seconds": CSO_UNDERSPEED_WINDOW_SECONDS_DEFAULT,
    "startup_grace_seconds": CSO_STARTUP_GRACE_SECONDS_DEFAULT,
}

CSO_ALLOWED_VIDEO_CODECS = {"libx264", "libx265"}
CSO_ALLOWED_AUDIO_CODECS = {"aac", "ac3"}
CSO_ALLOWED_HWACCEL = {"none", "vaapi"}

CONTAINER_TO_FORMAT = {
    "mpegts": "mpegts",
    "ts": "mpegts",
    "matroska": "matroska",
    "mkv": "matroska",
}

CONTAINER_TO_CONTENT_TYPE = {
    "mpegts": "video/mp2t",
    "ts": "video/mp2t",
    "matroska": "video/x-matroska",
    "mkv": "video/x-matroska",
}


def default_cso_policy():
    return dict(DEFAULT_CSO_POLICY)


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


def normalize_cso_policy(value):
    policy = default_cso_policy()
    raw = {}
    if isinstance(value, str):
        value = value.strip()
        if value:
            try:
                raw = json.loads(value)
            except Exception:
                raw = {}
    elif isinstance(value, dict):
        raw = value

    mode = str(raw.get("output_mode") or policy["output_mode"]).strip().lower()
    if mode not in {"auto", "force_remux", "force_transcode"}:
        mode = policy["output_mode"]
    policy["output_mode"] = mode

    container = str(raw.get("container") or raw.get("output_container") or policy["container"]).strip().lower()
    if container not in CONTAINER_TO_FORMAT:
        container = policy["container"]
    policy["container"] = container

    video_codec = str(raw.get("video_codec") or "").strip().lower()
    if video_codec and video_codec not in CSO_ALLOWED_VIDEO_CODECS:
        video_codec = ""
    policy["video_codec"] = video_codec

    audio_codec = str(raw.get("audio_codec") or "").strip().lower()
    if audio_codec and audio_codec not in CSO_ALLOWED_AUDIO_CODECS:
        audio_codec = ""
    policy["audio_codec"] = audio_codec

    subtitle_mode = str(raw.get("subtitle_mode") or policy["subtitle_mode"]).strip().lower()
    if subtitle_mode not in {"copy", "drop"}:
        subtitle_mode = policy["subtitle_mode"]
    policy["subtitle_mode"] = subtitle_mode

    data_mode = str(raw.get("data_mode") or policy["data_mode"]).strip().lower()
    if data_mode not in {"copy", "drop"}:
        data_mode = policy["data_mode"]
    policy["data_mode"] = data_mode

    hwaccel = str(raw.get("hwaccel") or raw.get("hardware_acceleration") or policy["hwaccel"]).strip().lower()
    if hwaccel not in CSO_ALLOWED_HWACCEL:
        hwaccel = "none"
    policy["hwaccel"] = hwaccel

    deinterlace_raw = raw.get("deinterlace", policy["deinterlace"])
    policy["deinterlace"] = bool(deinterlace_raw)
    try:
        policy["stall_seconds"] = max(
            3, int(raw.get("stall_seconds", policy["stall_seconds"]) or policy["stall_seconds"]))
    except Exception:
        policy["stall_seconds"] = CSO_STALL_SECONDS_DEFAULT
    try:
        policy["under_speed_ratio"] = float(
            raw.get("under_speed_ratio", policy["under_speed_ratio"]) or policy["under_speed_ratio"])
    except Exception:
        policy["under_speed_ratio"] = CSO_UNDERSPEED_RATIO_DEFAULT
    if policy["under_speed_ratio"] <= 0:
        policy["under_speed_ratio"] = CSO_UNDERSPEED_RATIO_DEFAULT
    try:
        policy["under_speed_window_seconds"] = max(
            4,
            int(raw.get("under_speed_window_seconds",
                policy["under_speed_window_seconds"]) or policy["under_speed_window_seconds"]),
        )
    except Exception:
        policy["under_speed_window_seconds"] = CSO_UNDERSPEED_WINDOW_SECONDS_DEFAULT
    try:
        policy["startup_grace_seconds"] = max(
            0,
            int(raw.get("startup_grace_seconds", policy["startup_grace_seconds"]) or policy["startup_grace_seconds"]),
        )
    except Exception:
        policy["startup_grace_seconds"] = CSO_STARTUP_GRACE_SECONDS_DEFAULT

    # Curated policy enforcement for maintainable FFmpeg command generation.
    if mode == "auto":
        policy["container"] = "mpegts"

    if mode != "force_transcode":
        policy["video_codec"] = ""
        policy["audio_codec"] = ""
        policy["hwaccel"] = "none"
        policy["deinterlace"] = False
    else:
        if not policy["video_codec"]:
            policy["video_codec"] = "libx264"
        if not policy["audio_codec"]:
            policy["audio_codec"] = "aac"
        # HEVC in MPEG-TS is intentionally disallowed in curated profile.
        if policy["container"] in {"mpegts", "ts"} and policy["video_codec"] == "libx265":
            policy["video_codec"] = "libx264"
        # VAAPI only available when /dev/dri is present.
        if policy["hwaccel"] == "vaapi" and not detect_vaapi_device_path():
            policy["hwaccel"] = "none"

    return policy


def resolve_cso_policy(channel, output_profile="default"):
    base = normalize_cso_policy(getattr(channel, "cso_policy", None))
    if (output_profile or "").lower() == "tvh":
        base["output_mode"] = "auto"
        base["container"] = "mpegts"
        base["video_codec"] = ""
        base["audio_codec"] = ""
    return base


def serialize_cso_policy(policy):
    return json.dumps(normalize_cso_policy(policy), sort_keys=True)


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


def _capacity_key_for_source(source):
    xc_account_id = getattr(source, "xc_account_id", None)
    playlist_id = getattr(source, "playlist_id", None)
    if xc_account_id:
        return f"xc:{int(xc_account_id)}"
    if playlist_id:
        return f"playlist:{int(playlist_id)}"
    return f"source:{int(getattr(source, 'id', 0) or 0)}"


def _capacity_limit_for_source(source):
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


@dataclass
class CsoStartResult:
    success: bool
    reason: str | None = None


class CsoCapacityRegistry:
    def __init__(self):
        self._allocations = {}
        self._external_counts = {}
        self._lock = asyncio.Lock()

    async def try_reserve(self, key, owner_key, limit):
        async with self._lock:
            # key -> {owner_key: ref_count}
            current = self._allocations.setdefault(key, {})
            if owner_key in current:
                current[owner_key] = int(current.get(owner_key) or 0) + 1
                return True
            external = int(self._external_counts.get(key) or 0)
            if (len(current) + external) >= max(0, int(limit or 0)):
                return False
            current[owner_key] = 1
            return True

    async def release(self, key, owner_key):
        async with self._lock:
            current = self._allocations.get(key)
            if not current:
                return
            existing = int(current.get(owner_key) or 0)
            if existing <= 1:
                current.pop(owner_key, None)
            else:
                current[owner_key] = existing - 1
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
    unwrapped = _unwrap_local_tic_hls_proxy_url(normalized, instance_id=instance_id)
    if unwrapped:
        return unwrapped
    if is_local_hls_proxy_url(normalized, instance_id=instance_id):
        normalized = normalize_local_proxy_url(
            normalized,
            base_url=base_url,
            instance_id=instance_id,
            stream_key=stream_key,
            username=username,
        )
    elif stream_key and "/tic-hls-proxy/" in normalized and "stream_key=" not in normalized:
        normalized = append_stream_key(normalized, stream_key=stream_key, username=username)
    return normalized


def _build_ffmpeg_command(source_url, policy, include_subtitles=True, include_data=True):
    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-reconnect",
        "1",
        "-reconnect_at_eof",
        "1",
        "-reconnect_streamed",
        "1",
        "-reconnect_delay_max",
        "2",
        "-i",
        source_url,
        "-map",
        "0:p:0:v:0?",
        "-map",
        "0:p:0:a?",
    ]
    if include_subtitles:
        command += ["-map", "0:p:0:s?"]

    mode = (policy or {}).get("output_mode", "auto")
    ffmpeg_format = policy_ffmpeg_format(policy)
    video_codec = (policy or {}).get("video_codec", "").strip()
    audio_codec = (policy or {}).get("audio_codec", "").strip()

    if mode in {"auto", "force_remux"}:
        command += ["-c", "copy"]
        if not include_subtitles:
            command.append("-sn")
    else:
        command += ["-c:v", video_codec or "libx264"]
        command += ["-c:a", audio_codec or "aac"]
        command += ["-c:s", "copy" if include_subtitles else "none"]
        if not include_subtitles:
            command.append("-sn")

    # Hard CSO rule: never include data streams in output.
    command.append("-dn")

    if ffmpeg_format == "mpegts":
        command += ["-muxdelay", "0", "-muxpreload", "0"]

    command += ["-f", ffmpeg_format, "pipe:1"]
    return command


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
    cutoff_dt = datetime.utcnow() - timedelta(days=days)
    async with Session() as session:
        result = await session.execute(
            delete(CsoEventLog).where(CsoEventLog.created_at < cutoff_dt)
        )
        await session.commit()
        return int(result.rowcount or 0)


def _build_ingest_ffmpeg_command(source_url, program_index=0):
    map_program = max(0, int(program_index or 0))
    return [
        "ffmpeg",
        "-hide_banner",
        "-nostats",
        "-loglevel",
        "warning",
        "-progress",
        "pipe:2",
        "-reconnect",
        "1",
        "-reconnect_at_eof",
        "1",
        "-reconnect_streamed",
        "1",
        "-reconnect_delay_max",
        "2",
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


def _build_output_ffmpeg_command(policy):
    builder = CsoFfmpegCommandBuilder(policy)
    return builder.build_output_command()


def _policy_log_label(policy):
    data = policy or {}
    return (
        f"output_mode={data.get('output_mode', 'auto')}, "
        f"container={data.get('container', 'mpegts')}, "
        f"video_codec={data.get('video_codec', '') or 'copy'}, "
        f"audio_codec={data.get('audio_codec', '') or 'copy'}, "
        f"subtitle_mode={data.get('subtitle_mode', 'copy')}, "
        f"hwaccel={data.get('hwaccel', 'none')}, "
        f"deinterlace={bool(data.get('deinterlace', False))}"
    )


class CsoFfmpegCommandBuilder:
    """Curated FFmpeg command builder for CSO output pipelines."""

    def __init__(self, policy):
        self.policy = normalize_cso_policy(policy)

    def _base(self):
        return [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "warning",
            "-f",
            "mpegts",
            "-i",
            "pipe:0",
            "-map",
            "0:v:0?",
            "-map",
            "0:a?",
        ]

    def _apply_stream_selection(self, command):
        subtitle_mode = str(self.policy.get("subtitle_mode") or "copy").strip().lower()
        if subtitle_mode != "drop":
            command += ["-map", "0:s?"]
        return subtitle_mode

    def _apply_transcode_options(self, command, subtitle_mode):
        video_codec = self.policy.get("video_codec", "libx264")
        audio_codec = self.policy.get("audio_codec", "aac")
        hwaccel = self.policy.get("hwaccel", "none")
        deinterlace = bool(self.policy.get("deinterlace", False))
        vaapi_device = detect_vaapi_device_path() if hwaccel == "vaapi" else None

        if vaapi_device:
            encoder = "hevc_vaapi" if video_codec == "libx265" else "h264_vaapi"
            filters = []
            if deinterlace:
                filters.append("bwdif=mode=send_frame:parity=auto:deint=all")
            filters += ["format=nv12", "hwupload"]
            command += ["-vaapi_device", vaapi_device, "-vf", ",".join(filters), "-c:v", encoder]
        else:
            if deinterlace:
                command += ["-vf", "bwdif=mode=send_frame:parity=auto:deint=all"]
            command += ["-c:v", video_codec or "libx264"]

        command += ["-c:a", audio_codec or "aac"]
        command += ["-c:s", "copy" if subtitle_mode != "drop" else "none"]
        if subtitle_mode == "drop":
            command.append("-sn")

    def build_output_command(self):
        command = self._base()
        subtitle_mode = self._apply_stream_selection(command)
        mode = self.policy.get("output_mode", "auto")
        ffmpeg_format = policy_ffmpeg_format(self.policy)

        if mode in {"auto", "force_remux"}:
            command += ["-c", "copy"]
            if subtitle_mode == "drop":
                command.append("-sn")
        else:
            self._apply_transcode_options(command, subtitle_mode)

        # Hard CSO rule: never include data streams in output.
        command.append("-dn")
        if ffmpeg_format == "mpegts":
            command += ["-muxdelay", "0", "-muxpreload", "0"]
        command += ["-f", ffmpeg_format, "pipe:1"]
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
        cso_policy=None,
    ):
        self.key = key
        self.channel_id = channel_id
        self.sources = list(sources or [])
        self.request_base_url = request_base_url
        self.instance_id = instance_id
        self.capacity_owner_key = capacity_owner_key
        self.stream_key = stream_key
        self.username = username
        self.cso_policy = cso_policy
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
        self.health_failover_reason = None
        self.health_failover_details = None
        self.health_policy = default_cso_policy()
        self.hls_variants = []
        self.current_variant_position = None
        self.current_program_index = 0
        self.rendition_switch_target_position = None
        self.ramp_last_switch_ts = 0.0
        self.startup_jump_done = False

    async def start(self):
        async with self.lock:
            if self.running:
                return
            start_result = await self._start_best_source_unlocked(reason="initial_start")
            if not start_result.success:
                self.running = False
                self.last_error = start_result.reason or "no_available_source"
                return

    async def _start_process_unlocked(self):
        command = _build_ingest_ffmpeg_command(self.current_source_url, program_index=self.current_program_index)
        logger.info(
            "Starting CSO ingest channel=%s source=%s command=%s",
            self.channel_id,
            getattr(self.current_source, "id", None),
            command,
        )
        self.process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self.running = True
        self.last_source_start_ts = time.time()
        self.last_chunk_ts = self.last_source_start_ts
        self.low_speed_since = None
        self.last_ffmpeg_speed = None
        self.health_failover_reason = None
        self.health_failover_details = None
        self.health_policy = normalize_cso_policy(self.cso_policy)
        logger.info(
            "CSO ingest upstream connected channel=%s source_id=%s source_url=%s subscribers=%s",
            self.channel_id,
            getattr(self.current_source, "id", None),
            self.current_source_url,
            len(self.subscribers),
        )
        self.read_task = asyncio.create_task(self._read_loop())
        self.stderr_task = asyncio.create_task(self._stderr_loop())
        self.health_task = asyncio.create_task(self._health_loop())

    async def _start_best_source_unlocked(self, reason):
        now = time.time()
        candidates = sorted(
            self.sources,
            key=lambda item: _priority_value(getattr(item, "priority", 0)),
            reverse=True,
        )
        saw_capacity_block = False
        for source in candidates:
            source_id = getattr(source, "id", None)
            hold_until = self.failed_source_until.get(source_id, 0)
            if hold_until > now:
                continue
            playlist = getattr(source, "playlist", None)
            if playlist is not None and not bool(getattr(playlist, "enabled", False)):
                continue
            stream_url = (getattr(source, "playlist_stream_url", None) or "").strip()
            if not stream_url:
                continue

            capacity_key = _capacity_key_for_source(source)
            capacity_limit = _capacity_limit_for_source(source)
            reserved = await cso_capacity_registry.try_reserve(
                capacity_key,
                self.capacity_owner_key,
                capacity_limit,
            )
            if not reserved:
                saw_capacity_block = True
                continue

            resolved_url = _resolve_source_url(
                stream_url,
                base_url=self.request_base_url,
                instance_id=self.instance_id,
                stream_key=self.stream_key,
                username=self.username,
            )
            if not resolved_url:
                await cso_capacity_registry.release(capacity_key, self.capacity_owner_key)
                continue

            variants = await _discover_hls_variants(resolved_url)
            self.hls_variants = variants
            if variants:
                # Lock to the highest available rendition for stable timeline continuity.
                self.current_variant_position = len(variants) - 1
                self.current_program_index = int(variants[self.current_variant_position].get("program_index") or 0)
                self.startup_jump_done = True
            else:
                self.current_variant_position = None
                self.current_program_index = 0
                self.startup_jump_done = True

            old_capacity_key = self.current_capacity_key
            self.current_source = source
            self.current_source_url = resolved_url
            self.current_capacity_key = capacity_key
            await self._start_process_unlocked()
            if old_capacity_key and old_capacity_key != capacity_key:
                await cso_capacity_registry.release(old_capacity_key, self.capacity_owner_key)

            await emit_channel_stream_event(
                channel_id=self.channel_id,
                source_id=getattr(source, "id", None),
                playlist_id=getattr(source, "playlist_id", None),
                session_id=self.key,
                event_type="switch_success",
                severity="info",
                details={
                    "reason": reason,
                    "source_id": getattr(source, "id", None),
                    "pipeline": "ingest",
                    "source_url": self.current_source_url,
                    "program_index": self.current_program_index,
                    "variant_count": len(self.hls_variants),
                },
            )
            return CsoStartResult(success=True)

        return CsoStartResult(success=False, reason="capacity_blocked" if saw_capacity_block else "no_available_source")

    async def _stderr_loop(self):
        if not self.process:
            return
        while True:
            try:
                line = await self.process.stderr.readline()
            except Exception:
                break
            if not line:
                break
            rendered = line.decode("utf-8", errors="replace").strip()
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
                    except Exception:
                        self.last_ffmpeg_speed = None

            if not progress_handled:
                speed_match = _FFMPEG_SPEED_RE.search(rendered)
                if speed_match:
                    try:
                        self.last_ffmpeg_speed = float(speed_match.group(1))
                    except Exception:
                        self.last_ffmpeg_speed = None
            # Per-line FFmpeg stderr forwarding is intentionally suppressed to avoid log noise.

    async def _health_loop(self):
        while self.running:
            await asyncio.sleep(1.0)
            now = time.time()
            startup_grace_seconds = int(self.health_policy.get(
                "startup_grace_seconds", CSO_STARTUP_GRACE_SECONDS_DEFAULT))
            if (now - self.last_source_start_ts) < startup_grace_seconds:
                continue

            stall_seconds = int(self.health_policy.get("stall_seconds", CSO_STALL_SECONDS_DEFAULT))
            if self.last_chunk_ts and (now - self.last_chunk_ts) >= stall_seconds:
                await self._request_health_failover(
                    "stall_timeout",
                    {"stall_seconds": round(now - self.last_chunk_ts, 2), "threshold_seconds": stall_seconds},
                )
                return

            speed = self.last_ffmpeg_speed
            if speed is None:
                self.low_speed_since = None
                continue

            under_speed_ratio = float(self.health_policy.get("under_speed_ratio", CSO_UNDERSPEED_RATIO_DEFAULT))
            under_speed_window_seconds = int(
                self.health_policy.get("under_speed_window_seconds", CSO_UNDERSPEED_WINDOW_SECONDS_DEFAULT)
            )
            if speed < under_speed_ratio:
                if self.low_speed_since is None:
                    self.low_speed_since = now
                elif (now - self.low_speed_since) >= under_speed_window_seconds:
                    await self._request_health_failover(
                        "under_speed",
                        {
                            "speed": speed,
                            "threshold_ratio": under_speed_ratio,
                            "window_seconds": under_speed_window_seconds,
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
        logger.warning(
            "CSO ingest health-triggered failover channel=%s source_id=%s reason=%s details=%s",
            self.channel_id,
            getattr(self.current_source, "id", None),
            reason,
            details,
        )
        try:
            process.terminate()
        except Exception:
            pass

    async def _read_loop(self):
        saw_data = False
        return_code = None
        try:
            while self.running and self.process and self.process.stdout:
                chunk = await self.process.stdout.read(16384)
                if not chunk:
                    break
                saw_data = True
                self.last_chunk_ts = time.time()
                await self._broadcast(chunk)
        finally:
            if self.process:
                try:
                    return_code = self.process.returncode
                    if return_code is None:
                        return_code = await self.process.wait()
                except Exception:
                    return_code = None

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
            if failed_source_id:
                self.failed_source_until[failed_source_id] = time.time() + CSO_SOURCE_HOLD_DOWN_SECONDS
            old_capacity_key = self.current_capacity_key
            self.current_source = None
            self.current_source_url = ""
            self.current_capacity_key = None
            self.hls_variants = []
            self.current_variant_position = None
            self.current_program_index = 0
            self.rendition_switch_target_position = None
            self.startup_jump_done = False
            self.process = None
            self.running = False
            if old_capacity_key:
                await cso_capacity_registry.release(old_capacity_key, self.capacity_owner_key)

            await emit_channel_stream_event(
                channel_id=self.channel_id,
                source_id=failed_source_id,
                playlist_id=getattr(failed_source, "playlist_id", None),
                session_id=self.key,
                event_type="switch_attempt",
                severity="warning",
                details={
                    "reason": reason,
                    "failed_source_id": failed_source_id,
                    "return_code": return_code,
                    "saw_data": saw_data,
                    "pipeline": "ingest",
                    **(details or {}),
                },
            )

            start_result = await self._start_best_source_unlocked(reason="failover")
            if start_result.success:
                self.running = True
                return True

            event_type = "capacity_blocked" if start_result.reason == "capacity_blocked" else "playback_unavailable"
            await emit_channel_stream_event(
                channel_id=self.channel_id,
                source_id=failed_source_id,
                playlist_id=getattr(failed_source, "playlist_id", None),
                session_id=self.key,
                event_type=event_type,
                severity="warning",
                details={
                    "reason": start_result.reason,
                    "after_failure_reason": reason,
                    "pipeline": "ingest",
                    **(details or {}),
                },
            )
            return False

    async def _broadcast(self, chunk):
        if not chunk:
            return
        self.last_activity = time.time()
        async with self.lock:
            self.history.append(chunk)
            self.history_bytes += len(chunk)
            while self.history_bytes > self.max_history_bytes and self.history:
                old = self.history.popleft()
                self.history_bytes -= len(old)
            for q in list(self.subscribers.values()):
                try:
                    q.put_nowait(chunk)
                except asyncio.QueueFull:
                    try:
                        q.get_nowait()
                        q.put_nowait(chunk)
                    except Exception:
                        pass

    async def add_subscriber(self, subscriber_id, prebuffer_bytes=0):
        async with self.lock:
            q = asyncio.Queue(maxsize=8000)
            if prebuffer_bytes > 0 and self.history:
                total = 0
                items = []
                for chunk in reversed(self.history):
                    items.append(chunk)
                    total += len(chunk)
                    if total >= prebuffer_bytes:
                        break
                for chunk in reversed(items):
                    try:
                        q.put_nowait(chunk)
                    except asyncio.QueueFull:
                        break
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
            await self.stop(force=False)
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
            self.rendition_switch_target_position = None
            self.startup_jump_done = False
            subscriber_count = len(self.subscribers)
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
                return_code = await asyncio.wait_for(process.wait(), timeout=2.0)
            except Exception:
                try:
                    process.kill()
                    return_code = await process.wait()
                except Exception:
                    pass
        health_task = self.health_task
        self.health_task = None
        if health_task and not health_task.done():
            health_task.cancel()
            try:
                await health_task
            except Exception:
                pass
        if capacity_key:
            await cso_capacity_registry.release(capacity_key, self.capacity_owner_key)
        logger.info(
            "CSO ingest upstream disconnected channel=%s ingest_key=%s source_id=%s return_code=%s",
            self.channel_id,
            self.key,
            source_id,
            return_code,
        )
        async with self.lock:
            for q in self.subscribers.values():
                try:
                    q.put_nowait(None)
                except Exception:
                    pass


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

    async def start(self):
        async with self.lock:
            if self.running:
                return
            await self.ingest_session.start()
            if not self.ingest_session.running:
                self.last_error = self.ingest_session.last_error or "ingest_not_running"
                return
            self.ingest_queue = await self.ingest_session.add_subscriber(self.key, prebuffer_bytes=256 * 1024)
            command = _build_output_ffmpeg_command(self.policy)
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
        while True:
            try:
                line = await self.process.stderr.readline()
            except Exception:
                break
            if not line:
                break
            rendered = line.decode("utf-8", errors="replace").strip()
            # Per-line FFmpeg stderr forwarding is intentionally suppressed to avoid log noise.

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
        try:
            while self.running and self.process and self.process.stdout:
                chunk = await self.process.stdout.read(16384)
                if not chunk:
                    break
                await self._broadcast(chunk)
        finally:
            await self.stop(force=True)

    async def _broadcast(self, chunk):
        if not chunk:
            return
        self.last_activity = time.time()
        async with self.lock:
            self.history.append(chunk)
            self.history_bytes += len(chunk)
            while self.history_bytes > self.max_history_bytes and self.history:
                old = self.history.popleft()
                self.history_bytes -= len(old)
            for q in list(self.clients.values()):
                try:
                    q.put_nowait(chunk)
                except asyncio.QueueFull:
                    try:
                        q.get_nowait()
                        q.put_nowait(chunk)
                    except Exception:
                        pass

    async def add_client(self, connection_id, prebuffer_bytes=0):
        async with self.lock:
            q = asyncio.Queue(maxsize=100000)
            if prebuffer_bytes > 0 and self.history:
                total = 0
                items = []
                for chunk in reversed(self.history):
                    items.append(chunk)
                    total += len(chunk)
                    if total >= prebuffer_bytes:
                        break
                for chunk in reversed(items):
                    try:
                        q.put_nowait(chunk)
                    except asyncio.QueueFull:
                        break
            self.clients[connection_id] = q
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

    async def remove_client(self, connection_id):
        async with self.lock:
            self.clients.pop(connection_id, None)
            remaining = len(self.clients)
        logger.info(
            "CSO output client disconnected channel=%s output_key=%s connection_id=%s clients=%s policy=(%s)",
            self.channel_id,
            self.key,
            connection_id,
            remaining,
            _policy_log_label(self.policy),
        )
        if remaining == 0:
            await self.stop(force=False)
        return remaining

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
                return_code = await asyncio.wait_for(process.wait(), timeout=2.0)
            except Exception:
                try:
                    process.kill()
                    return_code = await process.wait()
                except Exception:
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
                try:
                    q.put_nowait(None)
                except Exception:
                    pass


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
            if session.running and (now - session.last_activity) < idle_timeout:
                continue
            async with session.lock:
                has_subscribers = bool(getattr(session, "subscribers", None) or getattr(session, "clients", None))
                running = bool(session.running)
            if running and has_subscribers:
                continue
            await session.stop(force=True)
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


async def reconcile_cso_capacity_with_tvh_channels(channel_ids, activity_sessions=None):
    external_counts = {}
    fallback_channel_ids = set()

    for value in (channel_ids or []):
        parsed = _safe_int(value)
        if parsed:
            fallback_channel_ids.add(parsed)

    for session in (activity_sessions or []):
        if not isinstance(session, dict):
            continue
        endpoint = str(session.get("endpoint") or "")
        display_url = str(session.get("display_url") or "").lower()
        # CSO endpoint usage is already tracked via in-process allocations.
        if "/tic-hls-proxy/channel/" in endpoint:
            continue
        # TVH subscriptions against CSO mux should not count as additional external usage.
        if endpoint.startswith("/tic-tvh/") and "tic-cso-" in display_url:
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
                key = _capacity_key_for_source(source)
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


async def subscribe_channel_stream(
    *,
    config,
    channel_id,
    stream_key,
    username,
    output_profile,
    connection_id,
    prebuffer_bytes=0,
    request_base_url="",
):
    channel = await resolve_channel_for_stream(channel_id)
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

    policy = resolve_cso_policy(channel, output_profile=output_profile)
    policy_key = serialize_cso_policy(policy)
    ingest_key = f"cso-ingest-{channel.id}"
    output_session_key = f"cso-output-{channel.id}-{output_profile or 'default'}-{uuid.uuid5(uuid.NAMESPACE_URL, policy_key)}"
    capacity_owner_key = f"cso-channel-{channel.id}"

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
            cso_policy=getattr(channel, "cso_policy", None),
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
            details={"reason": reason, "output_profile": output_profile or "default"},
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
            details={"reason": reason, "output_profile": output_profile or "default"},
        )
        return None, None, "Channel unavailable because output pipeline could not be started", 503

    queue = await output_session.add_client(connection_id, prebuffer_bytes=prebuffer_bytes)
    content_type = policy_content_type(policy)

    async def _generator():
        try:
            while True:
                chunk = await queue.get()
                if chunk is None:
                    break
                yield chunk
        finally:
            await output_session.remove_client(connection_id)
            await emit_channel_stream_event(
                channel_id=channel.id,
                source_id=getattr(ingest_session.current_source, "id", None),
                playlist_id=getattr(ingest_session.current_source, "playlist_id", None),
                session_id=output_session_key,
                event_type="session_end",
                severity="info",
                details={"output_profile": output_profile or "default"},
            )

    return _generator(), content_type, None, 200


def build_cso_stream_query(output_profile=None, connection_id=None, stream_key=None, username=None):
    query = []
    if output_profile:
        query.append(("output_profile", output_profile))
    if connection_id:
        query.append(("connection_id", connection_id))
    if stream_key:
        query.append(("stream_key", stream_key))
    if username:
        query.append(("username", username))
    if not query:
        return ""
    return urlencode(query)
