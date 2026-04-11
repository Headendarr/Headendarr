import asyncio
import json
import logging
import shutil
import time
from collections import deque
from pathlib import Path
from typing import Any

from backend.http_headers import sanitise_headers
from backend.utils import clean_key, clean_text

from .common import prepare_cso_cache_dir, remove_cso_cache_dir
from .constants import (
    CSO_HLS_SEGMENT_SECONDS,
    CSO_SEGMENT_CACHE_MIN_FREE_BYTES,
    CSO_SEGMENT_CACHE_ROOT,
)
from .ffmpeg import (
    CsoFfmpegCommandBuilder,
    log_hwaccel_failure,
    redact_ingest_command_for_log,
    start_ffmpeg_with_hw_decode_fallback,
)


logger = logging.getLogger("cso")


def _parse_rate(value: Any) -> float:
    text = clean_text(value)
    if not text or text in {"0", "0/0"}:
        return 0.0
    if "/" in text:
        left, _, right = text.partition("/")
        try:
            numerator = float(left or 0.0)
            denominator = float(right or 0.0)
        except Exception:
            return 0.0
        if denominator <= 0:
            return 0.0
        return numerator / denominator
    try:
        return float(text)
    except Exception:
        return 0.0


class SegmentedHandoffSession:
    def __init__(
        self,
        key: str,
        policy: dict[str, Any],
        input_target: str,
        input_is_url: bool = False,
        user_agent: str | None = None,
        request_headers: dict[str, str] | None = None,
        cache_root_dir: Path | str | None = None,
        start_seconds: int = 0,
        max_duration_seconds: int | None = None,
        realtime: bool = False,
    ):
        self.key = str(key)
        self.policy = dict(policy or {})
        self.input_target = clean_text(input_target)
        self.input_is_url = bool(input_is_url)
        self.user_agent = clean_text(user_agent)
        self.request_headers = sanitise_headers(request_headers)
        self.cache_root_dir = Path(cache_root_dir or CSO_SEGMENT_CACHE_ROOT)
        self.output_dir = self.cache_root_dir / self.key
        self.playlist_path = self.output_dir / "index.m3u8"
        self.start_seconds = max(0, int(start_seconds or 0))
        self.max_duration_seconds = (
            None
            if max_duration_seconds is None
            else max(1, int(max_duration_seconds or 0))
        )
        self.realtime = bool(realtime)
        self.process = None
        self.stderr_task = None
        self.wait_task = None
        self.running = False
        self.lock = asyncio.Lock()
        self.last_activity = time.time()
        self.last_error = None
        self.process_token = 0
        self._recent_ffmpeg_stderr = deque(maxlen=50)
        self.output_probe = {}

    def input_path(self) -> str:
        return str(self.playlist_path)

    def init_segment_path(self) -> Path:
        return self.output_dir / "init.mp4"

    def has_video_probe(self) -> bool:
        return bool(
            clean_key(self.output_probe.get("video_codec"))
            and int(self.output_probe.get("width") or 0) > 0
            and int(self.output_probe.get("height") or 0) > 0
        )

    async def _prepare_output_dir(self):
        self.cache_root_dir.mkdir(parents=True, exist_ok=True)
        await prepare_cso_cache_dir(self.output_dir, logger, f"segmented-handoff:{self.key}")

    async def _ensure_capacity(self):
        usage = await asyncio.to_thread(shutil.disk_usage, self.cache_root_dir)
        minimum_free_bytes = int(CSO_SEGMENT_CACHE_MIN_FREE_BYTES)
        if int(usage.free) >= minimum_free_bytes:
            return
        self.last_error = "segment_cache_insufficient_space"
        logger.error(
            "Segmented handoff unavailable because cache root is below the minimum free-space threshold "
            "cache_root=%s free_bytes=%s minimum_free_bytes=%s",
            self.cache_root_dir,
            int(usage.free),
            minimum_free_bytes,
        )
        raise RuntimeError(self.last_error)

    async def _cleanup_failed_start_attempt(self, process, stderr_task, wait_task):
        for task in (stderr_task, wait_task):
            if task is not None and not task.done():
                task.cancel()
        if process is not None and process.returncode is None:
            try:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=2.0)
            except Exception:
                try:
                    process.kill()
                    await asyncio.wait_for(process.wait(), timeout=2.0)
                except Exception:
                    pass

    async def _wait_for_startup_ready(
        self, process, timeout_seconds: float = 10.0
    ) -> tuple[bool, str]:
        startup_idle_timeout = max(1.0, float(timeout_seconds))
        hard_deadline = time.time() + max(30.0, startup_idle_timeout * 6.0)
        idle_deadline = time.time() + startup_idle_timeout
        last_seen_mtime = 0.0
        while time.time() < hard_deadline:
            if process.returncode is not None:
                return (
                    False,
                    self._ffmpeg_error_summary() or f"ffmpeg_exit:{process.returncode}",
                )
            if self.playlist_path.exists():
                try:
                    playlist_stat = self.playlist_path.stat()
                    last_seen_mtime = max(
                        last_seen_mtime, float(playlist_stat.st_mtime)
                    )
                    if int(playlist_stat.st_size or 0) > 0:
                        return True, ""
                except Exception:
                    pass
            try:
                child_mtime = max(
                    (
                        float(child.stat().st_mtime)
                        for child in self.output_dir.iterdir()
                    ),
                    default=0.0,
                )
            except Exception:
                child_mtime = 0.0
            if child_mtime > last_seen_mtime:
                last_seen_mtime = child_mtime
                idle_deadline = time.time() + startup_idle_timeout
            elif time.time() >= idle_deadline:
                break
            await asyncio.sleep(0.1)
        if process.returncode is not None:
            return (
                False,
                self._ffmpeg_error_summary() or f"ffmpeg_exit:{process.returncode}",
            )
        return False, self._ffmpeg_error_summary() or "startup_timeout_no_playlist"

    def _ffmpeg_error_summary(self) -> str:
        lines = [line for line in self._recent_ffmpeg_stderr if line]
        if not lines:
            return ""
        error_lines = [
            line
            for line in lines
            if any(
                token in line.lower()
                for token in ("error", "invalid", "failed", "could not", "unsupported")
            )
        ]
        selected = error_lines[-3:] if error_lines else lines[-3:]
        return " | ".join(selected)

    async def _stderr_loop(self, token: int, process):
        if process is None or process.stderr is None:
            return
        while True:
            try:
                line = await process.stderr.readline()
            except Exception:
                break
            if not line or token != self.process_token:
                break
            rendered = line.decode(errors="ignore").strip()
            if not rendered:
                continue
            self._recent_ffmpeg_stderr.append(rendered)
            self.last_activity = time.time()

    async def _wait_loop(self, token: int, process):
        try:
            return_code = await process.wait()
        except Exception:
            return_code = None
        if token != self.process_token:
            return
        if self.running and int(return_code or 0) != 0:
            self.last_error = "segmented_handoff_ended"
            logger.warning(
                "Segmented handoff ended unexpectedly key=%s return_code=%s stderr=%s",
                self.key,
                return_code,
                self._ffmpeg_error_summary() or "n/a",
            )
        self.running = False

    async def start(self):
        async with self.lock:
            if self.running:
                return True
            if not self.input_target:
                self.last_error = "missing_input_target"
                return False
            try:
                await self._ensure_capacity()
            except Exception:
                return False
            await self._prepare_output_dir()

            async def _attempt_start(effective_policy):
                builder = CsoFfmpegCommandBuilder(effective_policy)
                command = builder.build_hls_output_command(
                    self.output_dir,
                    input_target=self.input_target,
                    input_is_url=self.input_is_url,
                    start_seconds=self.start_seconds,
                    max_duration_seconds=self.max_duration_seconds,
                    realtime=self.realtime,
                    user_agent=self.user_agent,
                    request_headers=self.request_headers,
                )
                self._recent_ffmpeg_stderr.clear()
                logger.info(
                    "Starting segmented handoff key=%s input=%s policy=%s output_dir=%s command=%s",
                    self.key,
                    self.input_target,
                    dict(effective_policy or {}),
                    self.output_dir,
                    redact_ingest_command_for_log(command)
                    if self.input_is_url
                    else command,
                )
                process = await asyncio.create_subprocess_exec(
                    *command,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )
                self.process_token += 1
                token = self.process_token
                stderr_task = asyncio.create_task(
                    self._stderr_loop(token, process),
                    name=f"segmented-stderr-{self.key}",
                )
                wait_task = asyncio.create_task(
                    self._wait_loop(token, process), name=f"segmented-wait-{self.key}"
                )
                startup_timeout_seconds = 30.0 if self.input_is_url else 10.0
                started, failure_reason = await self._wait_for_startup_ready(
                    process,
                    timeout_seconds=startup_timeout_seconds,
                )
                if started:
                    return True, (process, stderr_task, wait_task), ""
                await self._cleanup_failed_start_attempt(
                    process, stderr_task, wait_task
                )
                return False, None, failure_reason

            (
                started,
                start_policy,
                result,
                failure_reason,
            ) = await start_ffmpeg_with_hw_decode_fallback(
                self.policy,
                self.input_target,
                _attempt_start,
            )
            self.policy = dict(start_policy or self.policy)
            if not started:
                log_hwaccel_failure(self.policy, self.key, failure_reason)
                self.last_error = failure_reason or "segmented_handoff_start_failed"
                self.running = False
                return False

            self.process, self.stderr_task, self.wait_task = result
            self.running = True
            self.last_error = None
            self.last_activity = time.time()
            return True

    async def _ffprobe_path(self, path: Path) -> dict[str, Any]:
        process = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v",
            "error",
            "-show_streams",
            "-show_format",
            "-print_format",
            "json",
            str(path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await process.communicate()
        if process.returncode != 0:
            return {}
        try:
            payload = json.loads(stdout.decode("utf-8", errors="ignore") or "{}")
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    async def detect_output_probe(self) -> dict[str, Any]:
        if self.output_probe:
            return dict(self.output_probe)
        deadline = time.time() + max(15.0, float(CSO_HLS_SEGMENT_SECONDS) * 8.0)
        probe_path = self.init_segment_path()
        while time.time() < deadline:
            if probe_path.exists() and probe_path.is_file():
                break
            segment_candidates = sorted(self.output_dir.glob("seg_*.m4s"))
            if segment_candidates:
                probe_path = segment_candidates[0]
                break
            await asyncio.sleep(0.1)
        if not probe_path.exists():
            return {}
        payload = await self._ffprobe_path(probe_path)
        streams = payload.get("streams") or []
        probe = {}
        for stream in streams:
            if not isinstance(stream, dict):
                continue
            codec_type = clean_key(stream.get("codec_type"))
            if codec_type == "video" and not probe.get("video_codec"):
                probe["video_codec"] = clean_key(stream.get("codec_name"))
                probe["width"] = int(stream.get("width") or 0)
                probe["height"] = int(stream.get("height") or 0)
                probe["pixel_format"] = clean_key(stream.get("pix_fmt"))
                avg_frame_rate = clean_text(
                    stream.get("avg_frame_rate") or stream.get("r_frame_rate")
                )
                fps_value = _parse_rate(avg_frame_rate)
                if fps_value > 0:
                    probe["fps"] = fps_value
                    probe["avg_frame_rate"] = avg_frame_rate
                sample_aspect_ratio = clean_text(stream.get("sample_aspect_ratio"))
                if sample_aspect_ratio:
                    probe["sample_aspect_ratio"] = sample_aspect_ratio
            elif codec_type == "audio" and not probe.get("audio_codec"):
                probe["audio_codec"] = clean_key(stream.get("codec_name"))
                probe["audio_sample_rate"] = int(stream.get("sample_rate") or 0)
                probe["audio_channels"] = int(stream.get("channels") or 0)
                channel_layout = clean_key(stream.get("channel_layout"))
                if channel_layout:
                    probe["audio_channel_layout"] = channel_layout
        self.output_probe = probe
        return dict(probe)

    async def stop(self, force: bool = False):
        async with self.lock:
            process = self.process
            self.process = None
            self.running = False
            self.process_token += 1
        for task in (self.stderr_task, self.wait_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except BaseException:
                    pass
        self.stderr_task = None
        self.wait_task = None
        if process is not None and process.returncode is None:
            try:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=2.0)
            except Exception:
                try:
                    process.kill()
                    await asyncio.wait_for(process.wait(), timeout=2.0)
                except Exception:
                    pass
        await remove_cso_cache_dir(self.output_dir, logger, f"segmented-handoff:{self.key}")
