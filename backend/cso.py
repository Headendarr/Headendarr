#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import base64
import json
import logging
import re
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

logger = logging.getLogger("cso")
CSO_SOURCE_HOLD_DOWN_SECONDS = 20
CSO_INGEST_RECOVERY_RETRY_WINDOW_SECONDS = 12
CSO_INGEST_RECOVERY_RETRY_INTERVAL_SECONDS = 1
CSO_STALL_SECONDS_DEFAULT = 20
CSO_UNDERSPEED_RATIO_DEFAULT = 0.9
CSO_UNDERSPEED_WINDOW_SECONDS_DEFAULT = 12
CSO_STARTUP_GRACE_SECONDS_DEFAULT = 8
_FFMPEG_SPEED_RE = re.compile(r"speed=\s*([0-9.]+)x")
_HLS_BANDWIDTH_RE = re.compile(r"BANDWIDTH=(\d+)")
_HLS_RESOLUTION_RE = re.compile(r"RESOLUTION=(\d+)x(\d+)")
_HLS_STARTUP_RAMP_INTERVAL_SECONDS = 4

CONTAINER_TO_FORMAT = {
    "mpegts": "mpegts",
    "ts": "mpegts",
    "matroska": "matroska",
    "mkv": "matroska",
    "mp4": "mp4",
    "webm": "webm",
}

CONTAINER_TO_CONTENT_TYPE = {
    "mpegts": "video/mp2t",
    "ts": "video/mp2t",
    "matroska": "video/x-matroska",
    "mkv": "video/x-matroska",
    "mp4": "video/mp4",
    "webm": "video/webm",
}


class CsoOutputReaderEnded(Exception):
    """Raised when CSO output ended unexpectedly while clients were still attached."""


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

    async def try_reserve(self, key, owner_key, limit):
        async with self._lock:
            # key -> {owner_key: ref_count}
            current = self._allocations.setdefault(key, {})
            if owner_key in current:
                # Owner already holds a slot for this key; do not ref-count leak on retries/restarts.
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


def _build_ingest_ffmpeg_command(source_url, program_index=0):
    map_program = max(0, int(program_index or 0))
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
    return command


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
            command += ["-muxdelay", "0", "-muxpreload", "0"]
        elif ffmpeg_format == "mp4":
            # Fragmented MP4 is required for live streaming to a pipe.
            command += ["-movflags", "+frag_keyframe+empty_moov+default_base_moof"]
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
    ):
        self.key = key
        self.channel_id = channel_id
        self.sources = list(sources or [])
        self.request_base_url = request_base_url
        self.instance_id = instance_id
        self.capacity_owner_key = capacity_owner_key
        self.stream_key = stream_key
        self.username = username
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
        self._recent_ffmpeg_stderr = deque(maxlen=50)
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
            try:
                await self._start_process_unlocked()
            except Exception as exc:
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
                        **_source_event_context(source, source_url=resolved_url),
                    },
                )
                self.current_source = None
                self.current_source_url = ""
                self.current_capacity_key = None
                self.running = False
                self.process = None
                await cso_capacity_registry.release(capacity_key, self.capacity_owner_key)
                continue
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
                    "pipeline": "ingest",
                    "program_index": self.current_program_index,
                    "variant_count": len(self.hls_variants),
                    **_source_event_context(source, source_url=self.current_source_url),
                },
            )
            return CsoStartResult(success=True)

        return CsoStartResult(success=False, reason="capacity_blocked" if saw_capacity_block else "no_available_source")

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
        rendered = text_buffer.strip()
        if rendered:
            self._recent_ffmpeg_stderr.append(rendered)

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

    async def _health_loop(self):
        while self.running:
            await asyncio.sleep(1.0)
            now = time.time()
            if (now - self.last_source_start_ts) < CSO_STARTUP_GRACE_SECONDS_DEFAULT:
                continue

            # Treat stall as actionable only when we have sustained no-data and
            # ingest is not keeping up at realtime speed.
            if self.last_chunk_ts and (now - self.last_chunk_ts) >= CSO_STALL_SECONDS_DEFAULT:
                speed = self.last_ffmpeg_speed
                if speed is not None and speed >= 1.0:
                    continue
                await self._request_health_failover(
                    "stall_timeout",
                    {
                        "stall_seconds": round(now - self.last_chunk_ts, 2),
                        "threshold_seconds": CSO_STALL_SECONDS_DEFAULT,
                    },
                )
                return

            speed = self.last_ffmpeg_speed
            if speed is None:
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
            ffmpeg_error = self._ffmpeg_error_summary()

            # Apply source hold-down only for health-triggered failover. For generic ingest
            # exits we allow immediate same-source restart to avoid tearing down clients.
            multi_source_channel = len(self.sources or []) > 1
            if failed_source_id and multi_source_channel and reason in {"under_speed", "stall_timeout"}:
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
                "return_code": return_code,
                "saw_data": saw_data,
                "pipeline": "ingest",
                "ffmpeg_error": ffmpeg_error or None,
                **(details or {}),
                **_source_event_context(failed_source),
            },
        )

        deadline = time.time() + CSO_INGEST_RECOVERY_RETRY_WINDOW_SECONDS
        last_result = CsoStartResult(success=False, reason="no_available_source")
        while True:
            async with self.lock:
                has_subscribers = bool(self.subscribers)
                start_result = await self._start_best_source_unlocked(reason="failover")
                if start_result.success:
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
        # Release capacity immediately so other channels are not blocked while
        # this ingest session drains/tears down.
        if capacity_key:
            await cso_capacity_registry.release(capacity_key, self.capacity_owner_key)
            capacity_key = None
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
        self._recent_ffmpeg_stderr = deque(maxlen=30)

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
                if intentional_failover and return_code in (None, 0):
                    logger.info(
                        "CSO output reader ended during intentional ingest failover channel=%s output_key=%s return_code=%s",
                        self.channel_id,
                        self.key,
                        return_code,
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
        try:
            while True:
                chunk = await queue.get()
                if chunk is None:
                    if output_session.last_error == "output_reader_ended":
                        raise CsoOutputReaderEnded("output_reader_ended")
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
