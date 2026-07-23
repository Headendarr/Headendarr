import asyncio
import json
import logging
import os
import secrets
import shutil
import time
from collections import deque
from pathlib import Path
from typing import Any

from backend.http_headers import sanitise_headers
from backend.utils import clean_key, clean_text

from .common import bounded_log_value, prepare_cso_cache_dir, remove_cso_cache_dir
from .constants import (
    CSO_HLS_SEGMENT_SECONDS,
    CSO_SEGMENT_CACHE_MIN_FREE_BYTES,
    CSO_SEGMENT_CACHE_ROOT,
)
from .ffmpeg import (
    CsoFfmpegCommandBuilder,
    ffmpeg_failure_classification,
    hwaccel_failure_stage,
    log_ffmpeg_start_result_failures,
    redact_ffmpeg_command_for_log,
    redact_ffmpeg_error_for_log,
    start_ffmpeg_with_hw_decode_fallback,
    terminate_ffmpeg_process,
)
from .hls import HlsSelectedPresentation, increment_hls_runtime_counter
from .processes import (
    cancel_and_await_tasks,
    cso_lifecycle_cleanup_result,
    mark_cso_ffmpeg_process_exited,
    spawn_cso_ffmpeg_process,
)
from .types import CsoFfmpegAttemptResult

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
        selected_presentation: HlsSelectedPresentation | None = None,
        require_audio: bool = False,
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
        self.max_duration_seconds = None if max_duration_seconds is None else max(1, int(max_duration_seconds or 0))
        self.realtime = bool(realtime)
        self.selected_presentation = selected_presentation
        self.require_audio = bool(require_audio)
        self.required_audio_stream_count = 0
        if self.require_audio:
            self.required_audio_stream_count = 1
            if self.selected_presentation is not None:
                self.required_audio_stream_count = max(
                    1,
                    self.selected_presentation.expected_audio_stream_count,
                )
        self.selected_master_path = self.output_dir / "input" / "selected-master.m3u8"
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
        self._selected_master_server = None
        self._selected_master_url = ""
        self._selected_master_route = ""

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

    async def _materialise_selected_master(self):
        if self.selected_presentation is None:
            return
        playlist_text = self.selected_presentation.serialize()

        def _write():
            input_dir = self.selected_master_path.parent
            input_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(input_dir, 0o700)
            temporary_path = input_dir / ".selected-master.tmp"
            try:
                descriptor = os.open(temporary_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
                    handle.write(playlist_text)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.chmod(temporary_path, 0o600)
                os.replace(temporary_path, self.selected_master_path)
                os.chmod(self.selected_master_path, 0o600)
            finally:
                temporary_path.unlink(missing_ok=True)

        await asyncio.to_thread(_write)
        counts = self.selected_presentation.rendition_counts()
        await increment_hls_runtime_counter("hls_selected_masters_total")
        if counts["audio"]:
            await increment_hls_runtime_counter("hls_external_audio_selected_variants_total")
        elif self.selected_presentation.selected_variant.has_audio_metadata():
            await increment_hls_runtime_counter("hls_muxed_audio_selected_variants_total")
        if counts["audio"] > 1:
            await increment_hls_runtime_counter("hls_multiple_audio_presentations_total")
        if counts["subtitles"]:
            await increment_hls_runtime_counter("hls_subtitle_presentations_total")
        logger.info(
            "CSO selected HLS master materialised key=%s generation=%s path=input/selected-master.m3u8 "
            "variant_position=%s variant_count=%s audio_renditions=%s subtitle_renditions=%s "
            "video_renditions=%s closed_caption_renditions=%s expected_video_streams=1 "
            "expected_audio_streams=%s expected_subtitle_streams=0",
            bounded_log_value(self.key),
            self.process_token + 1,
            self.selected_presentation.variant_position,
            self.selected_presentation.variant_count,
            counts["audio"],
            counts["subtitles"],
            counts["video"],
            counts["closed-captions"],
            self.required_audio_stream_count,
        )

    async def _serve_selected_master(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        status = "404 Not Found"
        body = b""
        try:
            request = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=2.0)
            request_line = request.split(b"\r\n", 1)[0].decode("ascii", errors="replace")
            method, path, _ = request_line.split(" ", 2)
            if method in {"GET", "HEAD"} and path == self._selected_master_route:
                status = "200 OK"
                body = await asyncio.to_thread(self.selected_master_path.read_bytes)
                if method == "HEAD":
                    body = b""
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, TimeoutError, ValueError, OSError):
            status = "400 Bad Request"
        response_headers = [
            f"HTTP/1.1 {status}",
            "Content-Type: application/vnd.apple.mpegurl",
            f"Content-Length: {len(body)}",
            "Cache-Control: no-store",
            "Connection: close",
            "",
            "",
        ]
        writer.write("\r\n".join(response_headers).encode("ascii") + body)
        try:
            await writer.drain()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    async def _start_selected_master_server(self) -> str:
        if self.selected_presentation is None:
            return ""
        if self._selected_master_server is not None:
            return self._selected_master_url
        self._selected_master_route = f"/{secrets.token_urlsafe(24)}/selected-master.m3u8"
        server = await asyncio.start_server(
            self._serve_selected_master,
            host="127.0.0.1",
            port=0,
            limit=128 * 1024,
        )
        socket = server.sockets[0]
        port = int(socket.getsockname()[1])
        self._selected_master_server = server
        self._selected_master_url = f"http://127.0.0.1:{port}{self._selected_master_route}"
        return self._selected_master_url

    async def _close_selected_master_server(self):
        server = self._selected_master_server
        if server is None:
            return
        server.close()
        await server.wait_closed()
        self._selected_master_server = None
        self._selected_master_url = ""
        self._selected_master_route = ""

    async def _ensure_capacity(self):
        try:
            self.cache_root_dir.mkdir(parents=True, exist_ok=True)
            usage = await asyncio.to_thread(shutil.disk_usage, self.cache_root_dir)
        except Exception as exc:
            self.last_error = "segment_cache_unavailable"
            logger.error(
                "Segmented handoff unavailable because cache root could not be inspected cache_root=%s error=%s",
                self.cache_root_dir,
                exc,
            )
            raise RuntimeError(self.last_error) from exc
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
        teardown = await terminate_ffmpeg_process(process)
        task_cleanup = await cancel_and_await_tasks((stderr_task, wait_task))
        if self.selected_presentation is not None and (not teardown.confirmed or not task_cleanup.confirmed):
            await increment_hls_runtime_counter("hls_selected_master_cleanup_failures_total")
        if not task_cleanup.confirmed:
            pending_tasks = set(task_cleanup.pending_tasks)
            self.stderr_task = stderr_task if stderr_task in pending_tasks else None
            self.wait_task = wait_task if wait_task in pending_tasks else None
        return teardown, task_cleanup

    async def _wait_for_startup_ready(self, process, timeout_seconds: float = 10.0) -> tuple[bool, str]:
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
                    last_seen_mtime = max(last_seen_mtime, float(playlist_stat.st_mtime))
                    if int(playlist_stat.st_size or 0) > 0:
                        return True, ""
                except Exception:
                    pass
            try:
                child_mtime = max(
                    (float(child.stat().st_mtime) for child in self.output_dir.iterdir()),
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
            if any(token in line.lower() for token in ("error", "invalid", "failed", "could not", "unsupported"))
        ]
        selected = error_lines[-3:] if error_lines else lines[-3:]
        return " | ".join(selected)

    def _classified_start_failure(self, failure_reason: str) -> tuple[str, str, str]:
        raw_reason = clean_text(failure_reason)
        raw_reason_lower = raw_reason.lower()
        cleanup_reason = ""
        for marker in ("teardown_unconfirmed", "task_cleanup_unconfirmed"):
            if marker in raw_reason_lower:
                cleanup_reason = marker
                break
        missing_audio_map = (
            ("stream map '0:a:" in raw_reason_lower or 'stream map "0:a:' in raw_reason_lower)
            and "matches no streams" in raw_reason_lower
        ) or ("failed to set value '0:a:" in raw_reason_lower and "for option 'map'" in raw_reason_lower)
        if self.require_audio and (
            missing_audio_map
            or "stream map '0:a' matches no streams" in raw_reason_lower
            or 'stream map "0:a" matches no streams' in raw_reason_lower
            or "audio map matches no streams" in raw_reason_lower
            or "failed to set value '0:a' for option 'map'" in raw_reason_lower
        ):
            bounded_reason = "hls_required_audio_missing"
            if self.required_audio_stream_count > 1:
                bounded_reason = "hls_handoff_track_mismatch"
            if cleanup_reason:
                bounded_reason = f"{bounded_reason}:{cleanup_reason}"
            return bounded_reason.partition(":")[0], bounded_reason, ""
        classification = ffmpeg_failure_classification(raw_reason)
        hardware_stage = hwaccel_failure_stage(raw_reason)
        bounded_reason = classification
        if cleanup_reason:
            bounded_reason = f"{bounded_reason}:{cleanup_reason}"
        return classification, bounded_reason, hardware_stage

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
        process_exited = mark_cso_ffmpeg_process_exited(process)
        if token != self.process_token:
            return
        if self.running and int(return_code or 0) != 0:
            self.last_error = "segmented_handoff_ended"
            logger.warning(
                "Segmented handoff ended unexpectedly key=%s return_code=%s stderr=%s",
                bounded_log_value(self.key),
                return_code,
                redact_ffmpeg_error_for_log(self._ffmpeg_error_summary()) or "n/a",
            )
        async with self.lock:
            self.running = False
            if self.process is process and process_exited:
                self.process = None

    async def start(self) -> bool:
        async with self.lock:
            if self.running:
                return True
            retained_tasks = (self.stderr_task, self.wait_task)
            if self.process is not None or any(task is not None for task in retained_tasks):
                self.last_error = (
                    "previous_process_teardown_unconfirmed"
                    if self.process is not None
                    else "previous_task_cleanup_unconfirmed"
                )
                logger.error(
                    "Segmented handoff start blocked by retained lifecycle key=%s pid=%s tasks=%s",
                    self.key,
                    getattr(self.process, "pid", None),
                    sum(task is not None for task in retained_tasks),
                )
                return False
            if not self.input_target:
                self.last_error = "missing_input_target"
                return False
            try:
                await self._ensure_capacity()
            except Exception:
                return False
            await self._prepare_output_dir()
            self.output_probe = {}
            try:
                await self._materialise_selected_master()
            except Exception:
                self.last_error = "hls_selected_master_write_failed"
                logger.exception(
                    "Failed to materialise selected HLS master key=%s path=input/selected-master.m3u8",
                    bounded_log_value(self.key),
                )
                return False
            effective_input_target = self.input_target
            effective_input_is_url = self.input_is_url
            input_uses_network = self.input_is_url
            if self.selected_presentation is not None:
                try:
                    effective_input_target = await self._start_selected_master_server()
                except Exception:
                    self.last_error = "hls_selected_master_server_failed"
                    logger.exception(
                        "Failed to start selected HLS master server key=%s",
                        bounded_log_value(self.key),
                    )
                    return False
                effective_input_is_url = True
                input_uses_network = True

            async def _attempt_start(
                effective_policy: dict[str, Any],
            ) -> CsoFfmpegAttemptResult:
                builder = CsoFfmpegCommandBuilder(effective_policy)
                command = builder.build_hls_output_command(
                    self.output_dir,
                    input_target=effective_input_target,
                    input_is_url=effective_input_is_url,
                    input_uses_network=input_uses_network,
                    start_seconds=self.start_seconds,
                    max_duration_seconds=self.max_duration_seconds,
                    realtime=self.realtime,
                    user_agent=self.user_agent,
                    request_headers=self.request_headers,
                    input_protocol_whitelist=(
                        "http,https,tcp,tls,crypto" if self.selected_presentation is not None else ""
                    ),
                    require_video=self.selected_presentation is not None,
                    required_audio_stream_count=self.required_audio_stream_count,
                )
                self._recent_ffmpeg_stderr.clear()
                logger.info(
                    "Starting segmented handoff key=%s input=%s policy=%s output_dir=%s command=%s",
                    bounded_log_value(self.key),
                    (
                        "selected-master"
                        if self.selected_presentation is not None
                        else ("upstream" if self.input_is_url else "local")
                    ),
                    dict(effective_policy or {}),
                    self.output_dir,
                    redact_ffmpeg_command_for_log(command),
                )
                process = await spawn_cso_ffmpeg_process(
                    *command,
                    label=f"segmented-handoff:{self.key}",
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
                wait_task = asyncio.create_task(self._wait_loop(token, process), name=f"segmented-wait-{self.key}")
                startup_timeout_seconds = 30.0 if input_uses_network else 10.0
                try:
                    started, failure_reason = await self._wait_for_startup_ready(
                        process,
                        timeout_seconds=startup_timeout_seconds,
                    )
                except asyncio.CancelledError:
                    teardown, task_cleanup = await self._cleanup_failed_start_attempt(
                        process,
                        stderr_task,
                        wait_task,
                    )
                    if teardown.confirmed:
                        if self.process is process:
                            self.process = None
                    else:
                        self.process = process
                    raise
                if started:
                    return CsoFfmpegAttemptResult(
                        dict(effective_policy),
                        True,
                        (process, stderr_task, wait_task),
                        "",
                        "",
                        redact_ffmpeg_error_for_log(self._ffmpeg_error_summary()),
                        "",
                    )
                teardown, task_cleanup = await self._cleanup_failed_start_attempt(
                    process,
                    stderr_task,
                    wait_task,
                )
                if teardown.confirmed:
                    if self.process is process:
                        self.process = None
                else:
                    self.process = process
                if not teardown.confirmed or not task_cleanup.confirmed:
                    cleanup_reason = "teardown_unconfirmed" if not teardown.confirmed else "task_cleanup_unconfirmed"
                    failure_reason = f"{failure_reason or 'segmented_handoff_start_failed'}:{cleanup_reason}"
                classification, bounded_reason, hardware_stage = self._classified_start_failure(failure_reason)
                if classification in {"hls_required_audio_missing", "hls_handoff_track_mismatch"}:
                    await increment_hls_runtime_counter("hls_required_track_failures_total")
                return CsoFfmpegAttemptResult(
                    dict(effective_policy),
                    False,
                    None,
                    classification,
                    bounded_reason,
                    redact_ffmpeg_error_for_log(failure_reason),
                    hardware_stage,
                )

            start_result = await start_ffmpeg_with_hw_decode_fallback(
                self.policy,
                effective_input_target,
                _attempt_start,
            )
            self.policy = dict(start_result.policy)
            if not start_result.success:
                log_ffmpeg_start_result_failures(self.key, start_result)
                self.last_error = start_result.failure_reason or "segmented_handoff_start_failed"
                self.running = False
                if self.process is None and self.stderr_task is None and self.wait_task is None:
                    await self._close_selected_master_server()
                return False

            self.process, self.stderr_task, self.wait_task = start_result.runtime
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
            if not segment_candidates:
                segment_candidates = sorted(self.output_dir.glob("seg_*.ts"))
            if segment_candidates:
                probe_path = segment_candidates[0]
                break
            await asyncio.sleep(0.1)
        if not probe_path.exists():
            return {}
        payload = await self._ffprobe_path(probe_path)
        streams = payload.get("streams") or []
        probe = {
            "video_stream_count": 0,
            "audio_stream_count": 0,
            "subtitle_stream_count": 0,
            "audio_languages": [],
            "subtitle_languages": [],
        }
        for stream in streams:
            if not isinstance(stream, dict):
                continue
            codec_type = clean_key(stream.get("codec_type"))
            if codec_type == "video":
                probe["video_stream_count"] += 1
                if not probe.get("video_codec"):
                    probe["video_codec"] = clean_key(stream.get("codec_name"))
                    probe["width"] = int(stream.get("width") or 0)
                    probe["height"] = int(stream.get("height") or 0)
                    probe["pixel_format"] = clean_key(stream.get("pix_fmt"))
                    avg_frame_rate = clean_text(stream.get("avg_frame_rate") or stream.get("r_frame_rate"))
                    fps_value = _parse_rate(avg_frame_rate)
                    if fps_value > 0:
                        probe["fps"] = fps_value
                        probe["avg_frame_rate"] = avg_frame_rate
                    sample_aspect_ratio = clean_text(stream.get("sample_aspect_ratio"))
                    if sample_aspect_ratio:
                        probe["sample_aspect_ratio"] = sample_aspect_ratio
            elif codec_type == "audio":
                probe["audio_stream_count"] += 1
                language = clean_text((stream.get("tags") or {}).get("language"))
                if language:
                    probe["audio_languages"].append(language)
                if not probe.get("audio_codec"):
                    probe["audio_codec"] = clean_key(stream.get("codec_name"))
                    probe["audio_sample_rate"] = int(stream.get("sample_rate") or 0)
                    probe["audio_channels"] = int(stream.get("channels") or 0)
                    channel_layout = clean_key(stream.get("channel_layout"))
                    if channel_layout:
                        probe["audio_channel_layout"] = channel_layout
            elif codec_type == "subtitle":
                probe["subtitle_stream_count"] += 1
                language = clean_text((stream.get("tags") or {}).get("language"))
                if language:
                    probe["subtitle_languages"].append(language)
        probe["audio_languages"] = tuple(probe["audio_languages"])
        probe["subtitle_languages"] = tuple(probe["subtitle_languages"])
        self.output_probe = probe
        return dict(probe)

    async def release_cache(self):
        try:
            await remove_cso_cache_dir(self.output_dir, logger, f"segmented-handoff:{self.key}")
        except Exception:
            if self.selected_presentation is not None:
                await increment_hls_runtime_counter("hls_selected_master_cleanup_failures_total")
            raise

    async def stop(self, force: bool = False, release_cache: bool = True):
        async with self.lock:
            process = self.process
            self.running = False
            self.process_token += 1
            stderr_task = self.stderr_task
            wait_task = self.wait_task
        self.stderr_task = None
        self.wait_task = None
        teardown = await terminate_ffmpeg_process(process) if process is not None else None
        task_cleanup = await cancel_and_await_tasks((stderr_task, wait_task))
        if not task_cleanup.confirmed:
            pending_tasks = set(task_cleanup.pending_tasks)
            async with self.lock:
                self.stderr_task = stderr_task if stderr_task in pending_tasks else None
                self.wait_task = wait_task if wait_task in pending_tasks else None
        if process is not None and teardown is not None and teardown.confirmed:
            async with self.lock:
                if self.process is process:
                    self.process = None
        result = cso_lifecycle_cleanup_result(teardown, task_cleanup)
        if self.selected_presentation is not None and not result.confirmed:
            await increment_hls_runtime_counter("hls_selected_master_cleanup_failures_total")
        if result.confirmed:
            await self._close_selected_master_server()
            if release_cache:
                await self.release_cache()
        return result
