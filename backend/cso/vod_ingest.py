import asyncio
import logging
import math
import secrets
import shutil
import time
from collections import deque
from pathlib import Path
from typing import Any

from backend.utils import clean_key, clean_text

from .capacity import cso_capacity_registry, source_capacity_key, source_capacity_limit
from .common import (
    ByteBudgetQueue,
    prepare_cso_cache_dir,
    remove_cso_cache_dir,
)
from .constants import (
    CSO_HLS_LIST_SIZE,
    CSO_HLS_SEGMENT_SECONDS,
    CSO_INGEST_HISTORY_MAX_BYTES,
    CSO_INGEST_SUBSCRIBER_QUEUE_MAX_BYTES,
    MPEGTS_CHUNK_BYTES,
    VOD_CHANNEL_NEXT_SEGMENT_CACHE_SECONDS,
    VOD_CHANNEL_NEXT_SEGMENT_PRESTART_SECONDS,
    VOD_CHANNEL_STITCHED_FILLER_GRACE_SECONDS,
    VOD_CHANNEL_STITCHED_HLS_TARGET_DURATION_SECONDS,
    VOD_CHANNEL_STITCHED_MAX_SECONDS,
    VOD_CHANNEL_STITCHED_MIN_SECONDS,
    VOD_CHANNEL_STITCHED_SEGMENT_DELETE_GRACE_SECONDS,
    VOD_CHANNEL_STITCHED_TARGET_SECONDS,
)
from .ffmpeg import (
    CsoFfmpegCommandBuilder,
    ffmpeg_failure_classification,
    hwaccel_failure_stage,
    redact_ffmpeg_command_for_log,
    start_ffmpeg_with_hw_decode_fallback,
    terminate_ffmpeg_process,
    wait_for_process_output_start,
)
from .live_ingest import resolve_cso_ingest_headers, resolve_cso_ingest_user_agent
from .output import CsoOutputSession, policy_log_label
from .policy import (
    generate_vod_channel_ingest_policy,
    generate_vod_channel_segment_cache_policy,
    should_prefer_direct_vod_url_input,
)
from .processes import (
    cancel_and_await_tasks,
    combine_cso_lifecycle_cleanup_results,
    cso_lifecycle_cleanup_result,
    mark_cso_ffmpeg_process_exited,
    retained_cso_ffmpeg_process,
    spawn_cso_ffmpeg_process,
)
from .types import CsoFfmpegAttemptResult
from .segmented_handoff import SegmentedHandoffSession
from .sources import cso_source_from_vod_source
from .vod_cache import vod_cache_manager, warm_vod_cache


logger = logging.getLogger("cso")


class VodIngestSession:
    def __init__(
        self,
        key: str,
        config: Any,
        source: Any,
        upstream_url: str | None = None,
        profile: str | None = None,
        start_seconds: int = 0,
        duration_seconds: int | None = None,
        request_headers: dict[str, str] | None = None,
        output_policy: dict[str, Any] | None = None,
        realtime: bool = False,
    ):
        self.key = str(key)
        self.config = config
        self.source = source
        self.upstream_url = clean_text(upstream_url or getattr(source, "url", None))
        self.profile = profile
        self.start_seconds = max(0, int(start_seconds or 0))
        self.duration_seconds = duration_seconds
        self.request_headers = dict(request_headers or {})
        self.output_policy = dict(output_policy or {})
        self.realtime = realtime

        self.ingest_policy = dict(self.output_policy)
        if not self.ingest_policy:
            self.ingest_policy = dict(generate_vod_channel_ingest_policy(config, None))
        self.process = None
        self.stderr_task = None
        self.stdout_task = None
        self.startup_tasks = ()
        self.running = False
        self.lock = asyncio.Lock()
        self.last_activity = time.time()
        self.last_error = None
        self.last_reader_end_reason = None
        self.last_reader_end_return_code = None
        self.last_reader_end_ts = 0.0
        self.session_start_ts = 0.0
        self._recent_ffmpeg_stderr = deque(maxlen=50)
        self._warm_task = None
        self._output_queue = asyncio.Queue(maxsize=16)

    @property
    def source_id(self):
        return getattr(self.source, "id", None)

    async def _read_stderr(self, process):
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
                    "VOD ingest ffmpeg[%s][%s]: %s",
                    self.source_id,
                    self.key,
                    rendered,
                )

    async def _read_stdout(self, process):
        if process.stdout is None:
            return
        try:
            while self.running:
                chunk = await process.stdout.read(MPEGTS_CHUNK_BYTES)
                if not chunk:
                    break
                self.last_activity = time.time()
                await self._output_queue.put(chunk)
        finally:
            current_task = asyncio.current_task()
            if current_task is None or not current_task.cancelling():
                process_exited = mark_cso_ffmpeg_process_exited(process)
                async with self.lock:
                    self.running = False
                    if self.process is process and process_exited:
                        self.process = None
            try:
                self._output_queue.put_nowait(None)
            except Exception:
                pass

    async def start(self):
        async with self.lock:
            if self.running:
                return True
            previous_process = self.process
            if previous_process is None:
                previous_process = retained_cso_ffmpeg_process(f"vod-ingest:{self.key}")
            previous_stderr_task = self.stderr_task
            previous_stdout_task = self.stdout_task
            previous_warm_task = self._warm_task
            previous_startup_tasks = self.startup_tasks
            if previous_process is not None:
                self.process = previous_process
                teardown = await terminate_ffmpeg_process(
                    previous_process,
                    terminate_timeout_seconds=1.0,
                    kill_timeout_seconds=1.0,
                )
                if not teardown.confirmed:
                    self.last_error = "previous_process_teardown_unconfirmed"
                    logger.error(
                        "VOD ingest start blocked by retained process key=%s pid=%s failure=%s",
                        self.key,
                        getattr(previous_process, "pid", None),
                        teardown.failure,
                    )
                    return False
            retained_tasks = (
                previous_stderr_task,
                previous_stdout_task,
                previous_warm_task,
                *previous_startup_tasks,
            )
            if any(task is not None for task in retained_tasks):
                task_cleanup = await cancel_and_await_tasks(retained_tasks)
                if not task_cleanup.confirmed:
                    pending_tasks = set(task_cleanup.pending_tasks)
                    self.stderr_task = previous_stderr_task if previous_stderr_task in pending_tasks else None
                    self.stdout_task = previous_stdout_task if previous_stdout_task in pending_tasks else None
                    self._warm_task = previous_warm_task if previous_warm_task in pending_tasks else None
                    self.startup_tasks = tuple(task for task in previous_startup_tasks if task in pending_tasks)
                    self.last_error = "previous_task_cleanup_unconfirmed"
                    return False
            if self.process is previous_process:
                self.process = None
            if self.stderr_task is previous_stderr_task:
                self.stderr_task = None
            if self.stdout_task is previous_stdout_task:
                self.stdout_task = None
            if self._warm_task is previous_warm_task:
                self._warm_task = None
            self.startup_tasks = ()
            self.running = True
            self._output_queue = asyncio.Queue(maxsize=16)
            self.last_error = None
            self.session_start_ts = time.time()
            self.last_activity = self.session_start_ts
            self._recent_ffmpeg_stderr.clear()

            # Resolve input
            cache_entry = await vod_cache_manager.get_or_create(self.source, self.upstream_url)
            input_target = self.upstream_url
            input_is_url = True
            input_label = "upstream"
            if cache_entry.complete and cache_entry.final_path.exists():
                input_target = str(cache_entry.final_path)
                input_is_url = False
                input_label = "cache"
            elif not self.realtime and self.start_seconds <= 0 and cache_entry.part_path.exists():
                input_target = str(cache_entry.part_path)
                input_is_url = False
                input_label = "cache_part"

            source_probe = dict(self.source.probe_details or {})
            use_direct_upstream_input = bool(
                input_is_url
                and should_prefer_direct_vod_url_input(
                    self.source,
                    start_seconds=self.start_seconds,
                    source_probe=source_probe,
                )
            )
            source_identity = input_target or self.upstream_url
            base_policy = dict(self.ingest_policy)

            async def _attempt_start(
                effective_policy: dict[str, Any],
            ) -> CsoFfmpegAttemptResult:
                command = CsoFfmpegCommandBuilder(
                    effective_policy,
                    pipe_output_format=effective_policy.get("container", "mpegts"),
                    source_probe=source_probe,
                ).build_vod_channel_ingest_command(
                    input_target,
                    start_seconds=self.start_seconds,
                    max_duration_seconds=self.duration_seconds,
                    realtime=self.realtime,
                    input_is_url=input_is_url,
                    user_agent=resolve_cso_ingest_user_agent(self.config, self.source),
                    request_headers=resolve_cso_ingest_headers(self.source),
                    policy=effective_policy,
                    seekable_url_input=use_direct_upstream_input,
                )
                logger.info(
                    "Starting VOD ingest key=%s source_id=%s input=%s offset_seconds=%s duration_seconds=%s "
                    "policy=(%s) command=%s",
                    self.key,
                    self.source_id,
                    input_label,
                    self.start_seconds,
                    self.duration_seconds,
                    policy_log_label(effective_policy),
                    redact_ffmpeg_command_for_log(command),
                )
                process = await spawn_cso_ffmpeg_process(
                    *command,
                    label=f"vod-ingest:{self.key}",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                self.process = process
                stderr_task = asyncio.create_task(
                    self._read_stderr(process),
                    name=f"vod-ingest-stderr-{self.key}",
                )
                self.stderr_task = stderr_task
                startup_timeout_seconds = 20.0 if use_direct_upstream_input else 10.0
                startup_cleanup_results = []
                try:
                    startup_result = await wait_for_process_output_start(
                        process,
                        process.stdout,
                        timeout_seconds=startup_timeout_seconds,
                        cleanup_results=startup_cleanup_results,
                    )
                    started, startup_failure_reason, startup_chunk = startup_result[:3]
                    if len(startup_result) > 3 and not startup_cleanup_results:
                        startup_cleanup_results.append(startup_result[3])
                    self.startup_tasks = startup_cleanup_results[-1].pending_tasks if startup_cleanup_results else ()
                except asyncio.CancelledError:
                    self.startup_tasks = startup_cleanup_results[-1].pending_tasks if startup_cleanup_results else ()
                    teardown = await terminate_ffmpeg_process(
                        process,
                        terminate_timeout_seconds=1.0,
                        kill_timeout_seconds=1.0,
                    )
                    startup_tasks = self.startup_tasks
                    self.startup_tasks = ()
                    task_cleanup = await cancel_and_await_tasks((stderr_task, *startup_tasks))
                    if teardown.confirmed:
                        if self.process is process:
                            self.process = None
                    if teardown.confirmed and task_cleanup.confirmed:
                        if self.stderr_task is stderr_task:
                            self.stderr_task = None
                        self.startup_tasks = ()
                    else:
                        if not task_cleanup.confirmed:
                            self.stderr_task = stderr_task if stderr_task in task_cleanup.pending_tasks else None
                            self.startup_tasks = tuple(
                                task for task in startup_tasks if task in task_cleanup.pending_tasks
                            )
                        cleanup_reason = (
                            "teardown_unconfirmed" if not teardown.confirmed else "task_cleanup_unconfirmed"
                        )
                        self.last_error = f"ingest_start_cancelled:{cleanup_reason}"
                    raise
                if started:
                    return CsoFfmpegAttemptResult(
                        dict(effective_policy),
                        True,
                        (process, stderr_task, startup_chunk),
                        "",
                        "",
                        " | ".join(list(self._recent_ffmpeg_stderr)[-3:]),
                        "",
                    )

                logger.warning(
                    "VOD ingest start failed source_id=%s reason=%s",
                    self.source_id,
                    startup_failure_reason or "unknown",
                )
                teardown = await terminate_ffmpeg_process(
                    process,
                    terminate_timeout_seconds=1.0,
                    kill_timeout_seconds=1.0,
                )
                startup_tasks = self.startup_tasks
                self.startup_tasks = ()
                task_cleanup = await cancel_and_await_tasks((stderr_task, *startup_tasks))
                if teardown.confirmed:
                    if self.process is process:
                        self.process = None
                if teardown.confirmed and task_cleanup.confirmed:
                    if self.stderr_task is stderr_task:
                        self.stderr_task = None
                    self.startup_tasks = ()
                elif not teardown.confirmed:
                    startup_failure_reason = f"{startup_failure_reason or 'ingest_start_failed'}:teardown_unconfirmed"
                else:
                    self.stderr_task = stderr_task if stderr_task in task_cleanup.pending_tasks else None
                    self.startup_tasks = tuple(task for task in startup_tasks if task in task_cleanup.pending_tasks)
                    startup_failure_reason = (
                        f"{startup_failure_reason or 'ingest_start_failed'}:task_cleanup_unconfirmed"
                    )
                return CsoFfmpegAttemptResult(
                    dict(effective_policy),
                    False,
                    None,
                    ffmpeg_failure_classification(startup_failure_reason),
                    startup_failure_reason,
                    " | ".join(list(self._recent_ffmpeg_stderr)[-3:]),
                    hwaccel_failure_stage(startup_failure_reason),
                )

            # Respect global HW decode policy
            settings = self.config.read_settings()
            global_enable_hw_decode = bool(settings.get("settings", {}).get("enable_hw_decode", False))
            logger.debug(
                "VOD ingest HW decode policy key=%s global_enable_hw_decode=%s",
                self.key,
                global_enable_hw_decode,
            )
            if not global_enable_hw_decode:
                base_policy["hardware_decode"] = False

            start_result = await start_ffmpeg_with_hw_decode_fallback(
                base_policy,
                source_identity,
                _attempt_start,
            )
            if not start_result.success:
                self.running = False
                self.last_error = start_result.failure_reason or "ingest_start_failed"
                return False

            self.ingest_policy = dict(start_result.policy)
            self.process, self.stderr_task, startup_chunk = start_result.runtime

            if startup_chunk:
                await self._output_queue.put(startup_chunk)

            self.stdout_task = asyncio.create_task(
                self._read_stdout(self.process),
                name=f"vod-ingest-stdout-{self.key}",
            )

            # Warm cache in the background when we are reading from upstream.
            if input_is_url and not cache_entry.complete:
                self._warm_task = asyncio.create_task(
                    warm_vod_cache(self.source, self.upstream_url, owner_key=f"ingest-{self.key}"),
                    name=f"vod-ingest-warm-{self.key}",
                )

            return True

    async def iter_bytes(self):
        while True:
            chunk = await self._output_queue.get()
            if chunk is None:
                break
            self.last_activity = time.time()
            yield chunk

    async def stop(self, force: bool = False):
        async with self.lock:
            self.running = False
            process = self.process
            stderr_task = self.stderr_task
            stdout_task = self.stdout_task
            warm_task = self._warm_task
            startup_tasks = self.startup_tasks
            self.stderr_task = None
            self.stdout_task = None
            self._warm_task = None
            self.startup_tasks = ()
            self.last_reader_end_ts = time.time()
        teardown = None
        if process is not None:
            teardown = await terminate_ffmpeg_process(
                process,
                terminate_timeout_seconds=1.0,
                kill_timeout_seconds=1.0,
            )
        task_cleanup = await cancel_and_await_tasks((stderr_task, stdout_task, warm_task, *startup_tasks))
        if not task_cleanup.confirmed:
            pending_tasks = set(task_cleanup.pending_tasks)
            async with self.lock:
                self.stderr_task = stderr_task if stderr_task in pending_tasks else None
                self.stdout_task = stdout_task if stdout_task in pending_tasks else None
                self._warm_task = warm_task if warm_task in pending_tasks else None
                self.startup_tasks = tuple(task for task in startup_tasks if task in pending_tasks)
        if process is not None and teardown is not None and teardown.confirmed:
            async with self.lock:
                if self.process is process:
                    self.process = None
        return cso_lifecycle_cleanup_result(teardown, task_cleanup)


class Vod247ChannelManager:
    def __init__(
        self,
        key: object,
        config: Any,
        channel_id: int,
        stream_key: str | None = None,
        request_headers: dict[str, str] | None = None,
        output_policy: dict[str, Any] | None = None,
        requested_policy: dict[str, Any] | None = None,
    ):
        self.key = str(key)
        self.config = config
        self.channel_id = int(channel_id)
        self.output_policy = dict(output_policy or {})
        self.requested_policy = dict(requested_policy or self.output_policy)
        self.ingest_policy = generate_vod_channel_ingest_policy(config, self.requested_policy)
        self.stream_key = clean_text(stream_key)
        self.request_headers = dict(request_headers or {})
        self.process = None
        self.segment_task = None
        self.stderr_task = None
        self.running = False
        self.lifecycle_lock = asyncio.Lock()
        self.lock = asyncio.Lock()
        self.playlist_lock = asyncio.Lock()
        self.last_activity = time.time()
        self.subscribers = {}
        self.lifecycle_references = set()
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
        self._active_ingest = None
        self._active_runtime = None
        self._retained_runtimes = []
        self.segment_stitch_task = None
        self.canonical_output_shape = {}
        # Producer output can contain the full retained airing and belongs on
        # the timeshift filesystem. Only the bounded live playlist window is
        # copied into the RAM-backed generic CSO cache.
        self.segment_cache_root = vod_cache_manager.vod_channel_segment_cache_root / f"channel-{self.channel_id}"
        self.stitched_cache_root = vod_cache_manager.cso_segment_cache_root / self.key
        self.stitched_output_dir = self.stitched_cache_root / "stitched"
        self.stitched_playlist_path = self.stitched_output_dir / "index.m3u8"
        self._stitched_playlist_lines = []
        self._queued_stitched_segments = []
        self._stitched_playlist_segments = []
        self._retired_stitched_segments = []
        self._stitched_media_sequence = 0
        self._stitched_discontinuity_sequence = 0
        self._stitched_segment_names = set()
        self._stitched_segment_index = 0
        self._stitched_init_index = 0
        self._stitched_episode_count = 0
        self._last_real_stitched_segment_ts = 0.0
        self._last_filler_stitched_segment_ts = 0.0
        self._last_filler_stitched_segment_duration = 0.0
        self._last_stitched_playlist_publish_ts = 0.0
        self._previous_stitched_playlist_publish_ts = 0.0
        self._stitched_playlist_generation = 0
        self._previous_stitched_published_sequence = -1
        self._last_stitched_published_sequence = -1
        self._stitched_target_duration = max(
            1,
            int(VOD_CHANNEL_STITCHED_HLS_TARGET_DURATION_SECONDS),
        )
        self._stitched_playlist_ended = False
        self._stitched_published_total_duration = 0.0
        self._stitched_reader_started_ts = 0.0
        self._stitched_reader_start_total_duration = 0.0
        self._stitched_reader_start_buffer_duration = 0.0
        self._stitched_reader_trimmed_duration = 0.0
        self._stitched_minimum_published_ahead = float("inf")
        self._last_stitched_buffer_metric_ts = 0.0
        self._last_stitched_advance_risk_generation = -1
        self._stitched_ready_event = asyncio.Event()
        self._stitched_server = None
        self._stitched_server_route = ""
        self._stitched_server_url = ""
        self._boundary_retention_task = None
        self._retune_validation_lock = asyncio.Lock()
        self._validated_retention_task = None

    def _startup_boundary_skip_seconds(self) -> int:
        return max(30, int(VOD_CHANNEL_NEXT_SEGMENT_PRESTART_SECONDS) + 10)

    def get_output_input_target(self):
        return self._stitched_server_url or str(self.stitched_playlist_path)

    def get_output_hls_input_options(self) -> dict[str, Any]:
        # The stitched playlist owns reader-aware retention, so its oldest
        # advertised segment is deliberately the output start position.
        return {"live_start_index": 0, "prefer_x_start": False}

    def is_hunting_for_stream(self):
        if not self.running:
            return True
        if self.current_source is None:
            return True
        if not self.current_segment_healthy:
            return True
        return False

    @staticmethod
    def _safe_stitched_asset_name(path: str) -> str:
        name = str(path or "").rsplit("/", 1)[-1].split("?", 1)[0]
        if name == "index.m3u8":
            return name
        if name.startswith("init_") and name.endswith(".mp4") and name[5:-4].isdigit():
            return name
        if name.startswith("seg_") and name.endswith(".m4s") and name[4:-4].isdigit():
            return name
        return ""

    async def _serve_stitched_asset(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        status = "404 Not Found"
        content_type = "application/octet-stream"
        body = b""
        content_length = 0
        try:
            request = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=2.0)
            request_line = request.split(b"\r\n", 1)[0].decode("ascii", errors="replace")
            method, path, _ = request_line.split(" ", 2)
            route_prefix = f"/{self._stitched_server_route}/"
            if method in {"GET", "HEAD"} and path.startswith(route_prefix):
                asset_name = self._safe_stitched_asset_name(path[len(route_prefix) :])
                asset_path = self.stitched_output_dir / asset_name if asset_name else None
                if asset_name == "index.m3u8":
                    async with self.playlist_lock:
                        body = self._render_stitched_playlist(self._stitched_playlist_ended).encode("utf-8")
                    content_length = len(body)
                    status = "200 OK"
                    content_type = "application/vnd.apple.mpegurl"
                elif asset_path is not None and asset_path.is_file():
                    body = await asyncio.to_thread(asset_path.read_bytes)
                    content_length = len(body)
                    status = "200 OK"
                    if asset_name.endswith(".mp4") or asset_name.endswith(".m4s"):
                        content_type = "video/mp4"
                if method == "HEAD" and status == "200 OK":
                    body = b""
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, TimeoutError, ValueError, OSError):
            status = "400 Bad Request"
        response_headers = [
            f"HTTP/1.1 {status}",
            f"Content-Type: {content_type}",
            f"Content-Length: {content_length}",
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

    async def _start_stitched_server(self):
        if self._stitched_server is not None:
            return
        self._stitched_server_route = secrets.token_urlsafe(24)
        server = await asyncio.start_server(self._serve_stitched_asset, host="127.0.0.1", port=0, limit=128 * 1024)
        socket = server.sockets[0]
        port = int(socket.getsockname()[1])
        self._stitched_server = server
        self._stitched_server_url = f"http://127.0.0.1:{port}/{self._stitched_server_route}/index.m3u8"

    async def _close_stitched_server(self):
        server = self._stitched_server
        if server is None:
            return
        server.close()
        await server.wait_closed()
        self._stitched_server = None
        self._stitched_server_route = ""
        self._stitched_server_url = ""

    async def _prepare_stitched_output_dir(self):
        await vod_cache_manager.register_vod_channel_cache_path(
            self.stitched_cache_root,
            self.key,
            self.channel_id,
            "stitched",
        )
        await prepare_cso_cache_dir(self.stitched_output_dir, logger, f"vod-247-stitched:{self.key}")
        await self._start_stitched_server()
        self._stitched_playlist_lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:7",
            "#EXT-X-INDEPENDENT-SEGMENTS",
        ]
        self._queued_stitched_segments = []
        self._stitched_playlist_segments = []
        self._retired_stitched_segments = []
        self._stitched_media_sequence = 0
        self._stitched_discontinuity_sequence = 0
        self._stitched_segment_names.clear()
        self._stitched_segment_index = 0
        self._stitched_init_index = 0
        self._stitched_episode_count = 0
        self._last_real_stitched_segment_ts = 0.0
        self._last_filler_stitched_segment_ts = 0.0
        self._last_filler_stitched_segment_duration = 0.0
        self._last_stitched_playlist_publish_ts = 0.0
        self._previous_stitched_playlist_publish_ts = 0.0
        self._stitched_playlist_generation = 0
        self._previous_stitched_published_sequence = -1
        self._last_stitched_published_sequence = -1
        self._stitched_target_duration = max(
            1,
            int(VOD_CHANNEL_STITCHED_HLS_TARGET_DURATION_SECONDS),
        )
        self._stitched_playlist_ended = False
        self._stitched_published_total_duration = 0.0
        self._stitched_reader_started_ts = 0.0
        self._stitched_reader_start_total_duration = 0.0
        self._stitched_reader_start_buffer_duration = 0.0
        self._stitched_reader_trimmed_duration = 0.0
        self._stitched_minimum_published_ahead = float("inf")
        self._last_stitched_buffer_metric_ts = 0.0
        self._last_stitched_advance_risk_generation = -1
        self._stitched_ready_event = asyncio.Event()
        await self._write_stitched_playlist(endlist=False)

    def _render_stitched_playlist(self, endlist: bool = False) -> str:
        lines = list(self._stitched_playlist_lines)
        visible_segments = self._stitched_visible_segments()
        observed_target_duration = max(
            int(CSO_HLS_SEGMENT_SECONDS),
            math.ceil(max((float(segment.get("duration") or 0.0) for segment in visible_segments), default=0.0)),
        )
        # Keep TARGETDURATION stable at its conservative lifetime value for
        # normal copy-mode GOPs. If an exceptional source produces a segment
        # longer than that bound, increasing it is preferable to advertising an
        # invalid playlist; that condition is also made visible for diagnosis.
        if observed_target_duration > self._stitched_target_duration:
            logger.warning(
                "VOD channel stitched segment exceeded declared HLS target duration "
                "channel=%s previous_target_duration=%s observed_target_duration=%s",
                self.channel_id,
                self._stitched_target_duration,
                observed_target_duration,
            )
            self._stitched_target_duration = observed_target_duration
        lines.append(f"#EXT-X-TARGETDURATION:{self._stitched_target_duration}")
        lines.append(f"#EXT-X-MEDIA-SEQUENCE:{self._stitched_media_sequence}")
        if self._stitched_discontinuity_sequence:
            lines.append(f"#EXT-X-DISCONTINUITY-SEQUENCE:{self._stitched_discontinuity_sequence}")
        current_init_name = ""
        for segment in visible_segments:
            if segment["discontinuity"]:
                lines.append("#EXT-X-DISCONTINUITY")
            init_name = clean_text(segment.get("init_name"))
            if init_name and init_name != current_init_name:
                lines.append(f'#EXT-X-MAP:URI="{init_name}"')
                current_init_name = init_name
            lines.append(f"#EXTINF:{float(segment['duration']):.3f},")
            lines.append(segment["name"])
        if endlist and (not lines or lines[-1] != "#EXT-X-ENDLIST"):
            lines.append("#EXT-X-ENDLIST")
        return "\n".join(lines).rstrip() + "\n"

    async def _write_stitched_playlist(self, endlist: bool = False, media_update: bool = False):
        self._stitched_playlist_ended = bool(endlist)
        visible_segment_count = len(self._stitched_visible_segments())
        payload = self._render_stitched_playlist(endlist)
        temp_path = self.stitched_output_dir / "index.m3u8.tmp"
        await asyncio.to_thread(temp_path.write_text, payload, "utf-8")
        await asyncio.to_thread(temp_path.replace, self.stitched_playlist_path)
        if media_update:
            self._previous_stitched_playlist_publish_ts = self._last_stitched_playlist_publish_ts
            self._last_stitched_playlist_publish_ts = time.time()
            self._stitched_playlist_generation += 1
            self._previous_stitched_published_sequence = self._last_stitched_published_sequence
            self._last_stitched_published_sequence = self._stitched_media_sequence + visible_segment_count - 1

    def _stitched_visible_segments(self) -> list[dict[str, Any]]:
        return list(self._stitched_playlist_segments)

    def _stitched_retained_duration_seconds(self) -> float:
        return sum(max(0.0, float(segment.get("duration") or 0.0)) for segment in self._stitched_playlist_segments)

    def _stitched_target_window_start_index(self) -> int:
        """Choose the oldest retained segment by accumulated media duration."""
        accumulated_duration = 0.0
        start_index = len(self._stitched_playlist_segments)
        for index in range(len(self._stitched_playlist_segments) - 1, -1, -1):
            accumulated_duration += max(
                0.0,
                float(self._stitched_playlist_segments[index].get("duration") or 0.0),
            )
            start_index = index
            if accumulated_duration >= float(VOD_CHANNEL_STITCHED_TARGET_SECONDS):
                break
        return start_index

    def _stitched_published_ahead_seconds(self) -> float:
        if self._stitched_reader_started_ts <= 0.0:
            return self._stitched_retained_duration_seconds()
        newly_published = max(
            0.0,
            self._stitched_published_total_duration - self._stitched_reader_start_total_duration,
        )
        elapsed = max(0.0, time.time() - self._stitched_reader_started_ts)
        return max(0.0, self._stitched_reader_start_buffer_duration + newly_published - elapsed)

    def _start_stitched_reader_clock(self, runtime, *, event: str) -> None:
        retained_duration = self._stitched_retained_duration_seconds()
        self._stitched_reader_started_ts = time.time()
        self._stitched_reader_start_total_duration = self._stitched_published_total_duration
        self._stitched_reader_start_buffer_duration = retained_duration
        self._stitched_reader_trimmed_duration = 0.0
        self._stitched_minimum_published_ahead = retained_duration
        self._last_stitched_advance_risk_generation = -1
        self._log_stitched_buffer_metrics(runtime or {}, event=event, force=True)

    def _stop_stitched_reader_clock(self) -> None:
        self._stitched_reader_started_ts = 0.0
        self._stitched_reader_start_total_duration = self._stitched_published_total_duration
        self._stitched_reader_start_buffer_duration = self._stitched_retained_duration_seconds()
        self._stitched_reader_trimmed_duration = 0.0
        self._stitched_minimum_published_ahead = float("inf")
        self._last_stitched_advance_risk_generation = -1

    def _stitched_runtime_ready(self, runtime) -> bool:
        retained_duration = self._stitched_retained_duration_seconds()
        return retained_duration >= float(VOD_CHANNEL_STITCHED_TARGET_SECONDS)

    def _log_stitched_buffer_metrics(self, runtime, *, event: str, force: bool = False) -> None:
        now = time.time()
        published_ahead = self._stitched_published_ahead_seconds()
        retained_duration = self._stitched_retained_duration_seconds()
        if self._stitched_reader_started_ts > 0.0:
            self._stitched_minimum_published_ahead = min(
                self._stitched_minimum_published_ahead,
                published_ahead,
            )
        if not force and now - self._last_stitched_buffer_metric_ts < 10.0:
            return
        self._last_stitched_buffer_metric_ts = now
        minimum_ahead = self._stitched_minimum_published_ahead
        logger.info(
            "VOD channel stitched buffer metric channel=%s event=%s retained_duration_seconds=%.3f "
            "published_ahead_seconds=%.3f minimum_published_ahead_seconds=%s target_seconds=%s "
            "minimum_seconds=%s maximum_seconds=%s segments=%s queued_segments=%s generation=%s producer_complete=%s",
            self.channel_id,
            event,
            retained_duration,
            published_ahead,
            f"{minimum_ahead:.3f}" if math.isfinite(minimum_ahead) else "unobserved",
            int(VOD_CHANNEL_STITCHED_TARGET_SECONDS),
            int(VOD_CHANNEL_STITCHED_MIN_SECONDS),
            int(VOD_CHANNEL_STITCHED_MAX_SECONDS),
            len(self._stitched_playlist_segments),
            len(self._queued_stitched_segments),
            self._stitched_playlist_generation,
            bool(runtime.get("producer_complete")),
        )

    def _log_stitched_advance_risk(self, runtime) -> None:
        if self._stitched_reader_started_ts <= 0.0:
            return
        published_ahead = self._stitched_published_ahead_seconds()
        if published_ahead > float(VOD_CHANNEL_STITCHED_MIN_SECONDS):
            return
        if self._last_stitched_advance_risk_generation == self._stitched_playlist_generation:
            return
        self._last_stitched_advance_risk_generation = self._stitched_playlist_generation
        playlist_age = (
            max(0.0, time.time() - self._last_stitched_playlist_publish_ts)
            if self._last_stitched_playlist_publish_ts > 0.0
            else float("inf")
        )
        logger.warning(
            "VOD channel stitched playlist advance risk channel=%s published_ahead_seconds=%.3f "
            "minimum_seconds=%s playlist_age_seconds=%s generation=%s queued_segments=%s producer_complete=%s",
            self.channel_id,
            published_ahead,
            int(VOD_CHANNEL_STITCHED_MIN_SECONDS),
            f"{playlist_age:.3f}" if math.isfinite(playlist_age) else "unknown",
            self._stitched_playlist_generation,
            len(self._queued_stitched_segments),
            bool(runtime.get("producer_complete")),
        )

    async def _prune_stitched_segments(self):
        now = time.time()
        retained_duration = self._stitched_retained_duration_seconds()
        reader_elapsed = (
            max(0.0, now - self._stitched_reader_started_ts) if self._stitched_reader_started_ts > 0.0 else 0.0
        )
        trimmed_count = 0
        trimmed_duration = 0.0
        desired_removal_count = 0
        if retained_duration > float(VOD_CHANNEL_STITCHED_MAX_SECONDS):
            desired_removal_count = self._stitched_target_window_start_index()
            if desired_removal_count <= 0 and len(self._stitched_playlist_segments) > 1:
                first_duration = max(
                    0.0,
                    float(self._stitched_playlist_segments[0].get("duration") or 0.0),
                )
                if retained_duration - first_duration >= float(VOD_CHANNEL_STITCHED_MIN_SECONDS):
                    desired_removal_count = 1
        while self._stitched_playlist_segments and trimmed_count < desired_removal_count:
            oldest_duration = max(0.0, float(self._stitched_playlist_segments[0].get("duration") or 0.0))
            remaining_duration = retained_duration - oldest_duration
            if remaining_duration < float(VOD_CHANNEL_STITCHED_MIN_SECONDS):
                break
            if self._stitched_reader_started_ts <= 0.0:
                break
            if reader_elapsed + 0.5 < self._stitched_reader_trimmed_duration + oldest_duration:
                break
            segment = self._stitched_playlist_segments.pop(0)
            segment["retired_ts"] = now
            self._retired_stitched_segments.append(segment)
            self._stitched_media_sequence += 1
            if segment.get("discontinuity"):
                self._stitched_discontinuity_sequence += 1
            self._stitched_reader_trimmed_duration += oldest_duration
            retained_duration = remaining_duration
            trimmed_count += 1
            trimmed_duration += oldest_duration
        if trimmed_count:
            logger.info(
                "VOD channel stitched buffer trimmed channel=%s removed_segments=%s removed_duration_seconds=%.3f "
                "retained_duration_seconds=%.3f media_sequence=%s minimum_seconds=%s maximum_seconds=%s",
                self.channel_id,
                trimmed_count,
                trimmed_duration,
                retained_duration,
                self._stitched_media_sequence,
                int(VOD_CHANNEL_STITCHED_MIN_SECONDS),
                int(VOD_CHANNEL_STITCHED_MAX_SECONDS),
            )
        kept_retired_segments = []
        for segment in self._retired_stitched_segments:
            name = clean_text(segment.get("name"))
            if not name:
                continue
            if now - float(segment.get("retired_ts") or now) < float(VOD_CHANNEL_STITCHED_SEGMENT_DELETE_GRACE_SECONDS):
                kept_retired_segments.append(segment)
                continue
            self._stitched_segment_names.discard(name)
            try:
                await asyncio.to_thread((self.stitched_output_dir / name).unlink, missing_ok=True)
            except Exception:
                logger.debug(
                    "Failed to prune old VOD stitched segment channel=%s ingest_key=%s segment=%s",
                    self.channel_id,
                    self.key,
                    name,
                    exc_info=True,
                )
        self._retired_stitched_segments = kept_retired_segments
        referenced_init_names = {
            clean_text(segment.get("init_name"))
            for segment in (
                self._queued_stitched_segments + self._stitched_playlist_segments + self._retired_stitched_segments
            )
            if clean_text(segment.get("init_name"))
        }
        for init_path in self.stitched_output_dir.glob("init_*.mp4"):
            if init_path.name in referenced_init_names:
                continue
            await asyncio.to_thread(init_path.unlink, missing_ok=True)
        return trimmed_count > 0

    async def _queue_stitched_segment(
        self, runtime, duration_seconds: float, source_segment_path: Path, discontinuity: bool
    ):
        stitched_name = f"seg_{self._stitched_segment_index:06d}.m4s"
        self._stitched_segment_index += 1
        segment_has_discontinuity = discontinuity and not runtime.get("discontinuity_applied")
        if segment_has_discontinuity:
            runtime["discontinuity_applied"] = True
        self._queued_stitched_segments.append(
            {
                "duration": duration_seconds,
                "name": stitched_name,
                "discontinuity": segment_has_discontinuity,
                "init_name": clean_text(runtime.get("stitched_init_name")),
                "source_path": source_segment_path,
            }
        )
        self._last_real_stitched_segment_ts = time.time()

    async def _materialise_stitched_segment(self, segment: dict[str, Any]) -> bool:
        stitched_name = clean_text(segment.get("name"))
        source_path = segment.get("source_path")
        if not stitched_name or not isinstance(source_path, Path) or not source_path.is_file():
            return False
        stitched_path = self.stitched_output_dir / stitched_name
        try:
            await asyncio.to_thread(shutil.copyfile, source_path, stitched_path)
        except Exception:
            logger.warning(
                "Failed to materialise VOD stitched segment channel=%s source=%s target=%s",
                self.channel_id,
                source_path.name,
                stitched_name,
                exc_info=True,
            )
            return False
        self._stitched_segment_names.add(stitched_name)
        return True

    def _last_real_stitched_segment(self) -> dict[str, Any] | None:
        for segment in reversed(self._stitched_playlist_segments):
            if not segment.get("filler"):
                return segment
        for segment in reversed(self._retired_stitched_segments):
            if not segment.get("filler"):
                return segment
        return None

    def _should_queue_filler_segment(self, runtime) -> bool:
        if not runtime.get("allow_stitch_filler"):
            return False
        if not self._stitched_playlist_segments:
            return False
        if float(runtime.get("published_duration_seconds") or 0.0) <= 0.0:
            return False
        now = time.time()
        last_real_ts = float(self._last_real_stitched_segment_ts or 0.0)
        if last_real_ts <= 0.0:
            return False
        if now - last_real_ts < float(VOD_CHANNEL_STITCHED_FILLER_GRACE_SECONDS):
            return False
        last_filler_ts = float(self._last_filler_stitched_segment_ts or 0.0)
        filler_duration = float(self._last_filler_stitched_segment_duration or CSO_HLS_SEGMENT_SECONDS)
        if last_filler_ts > 0.0 and now - last_filler_ts < max(1.0, filler_duration - 0.5):
            return False
        return True

    async def _queue_filler_stitched_segment(self):
        source_segment = self._last_real_stitched_segment()
        if not source_segment:
            return False
        source_name = clean_text(source_segment.get("name"))
        if not source_name:
            return False
        source_path = self.stitched_output_dir / source_name
        if not source_path.exists():
            return False

        stitched_name = f"seg_{self._stitched_segment_index:06d}.m4s"
        self._stitched_segment_index += 1
        self._queued_stitched_segments.append(
            {
                "duration": float(source_segment["duration"]),
                "name": stitched_name,
                "discontinuity": True,
                "filler": True,
                "init_name": clean_text(source_segment.get("init_name")),
                "source_path": source_path,
            }
        )
        self._last_filler_stitched_segment_ts = time.time()
        self._last_filler_stitched_segment_duration = float(source_segment["duration"])
        logger.warning(
            "VOD channel inserted filler segment to avoid starving output channel=%s ingest_key=%s source_segment=%s filler_segment=%s",
            self.channel_id,
            self.key,
            source_name,
            stitched_name,
        )
        return True

    async def _publish_queued_stitched_segments(self, runtime):
        if not self._queued_stitched_segments:
            playlist_trimmed = await self._prune_stitched_segments()
            published_ahead = self._stitched_published_ahead_seconds()
            if published_ahead >= float(VOD_CHANNEL_STITCHED_TARGET_SECONDS):
                return playlist_trimmed
            self._log_stitched_advance_risk(runtime)
            if not self._should_queue_filler_segment(runtime):
                return playlist_trimmed
            if not await self._queue_filler_stitched_segment():
                return playlist_trimmed
            return bool(await self._publish_queued_stitched_segments(runtime) or playlist_trimmed)

        # Publish as fast as the producer can supply complete source segments
        # until the duration-owned target is met. Always admit the segment that
        # crosses the target: with stream-copy GOPs it may be much longer than
        # the nominal HLS time, and requiring it to fit would deadlock below the
        # target indefinitely.
        published_ahead = self._stitched_published_ahead_seconds()
        if published_ahead >= float(VOD_CHANNEL_STITCHED_TARGET_SECONDS):
            return await self._prune_stitched_segments()
        publish_count = 0
        consumed_duration_seconds = 0.0
        for segment in self._queued_stitched_segments:
            duration_seconds = float(segment.get("duration") or 0.0)
            consumed_duration_seconds += duration_seconds
            publish_count += 1
            if published_ahead + consumed_duration_seconds >= float(VOD_CHANNEL_STITCHED_TARGET_SECONDS):
                break
        publishable_segments = self._queued_stitched_segments[:publish_count]
        materialised_segments = []
        for segment in publishable_segments:
            if not await self._materialise_stitched_segment(segment):
                break
            materialised_segments.append(segment)
        if not materialised_segments:
            await self._prune_stitched_segments()
            return False
        self._stitched_playlist_segments.extend(materialised_segments)
        del self._queued_stitched_segments[: len(materialised_segments)]
        consumed_duration_seconds = sum(float(segment.get("duration") or 0.0) for segment in materialised_segments)
        runtime["published_duration_seconds"] = (
            float(runtime.get("published_duration_seconds") or 0.0) + consumed_duration_seconds
        )
        self._stitched_published_total_duration += consumed_duration_seconds
        await self._prune_stitched_segments()
        self._log_stitched_buffer_metrics(runtime, event="publish")
        return True

    @staticmethod
    def _parse_playlist_segments(playlist_text: str) -> list[tuple[float, str]]:
        segments = []
        pending_duration = 0.0
        for raw_line in str(playlist_text or "").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("#EXTINF:"):
                duration_text = line.split(":", 1)[1].split(",", 1)[0]
                try:
                    pending_duration = float(duration_text or 0.0)
                except Exception:
                    pending_duration = 0.0
                continue
            if line.startswith("#"):
                continue
            segments.append((pending_duration or 2.0, line.split("?", 1)[0]))
            pending_duration = 0.0
        return segments

    async def _runtime_source_segments(self, runtime: dict[str, Any] | None) -> list[dict[str, Any]]:
        segment_session = (runtime or {}).get("segment_session")
        if segment_session is None or not segment_session.playlist_path.is_file():
            return []
        try:
            playlist_text = await asyncio.to_thread(segment_session.playlist_path.read_text, "utf-8")
        except Exception:
            return []
        segments = []
        for duration_seconds, segment_name in self._parse_playlist_segments(playlist_text):
            source_path = segment_session.output_dir / segment_name
            if source_path.is_file():
                segments.append(
                    {
                        "duration": max(0.001, float(duration_seconds or CSO_HLS_SEGMENT_SECONDS)),
                        "name": segment_name,
                        "source_path": source_path,
                    }
                )
        return segments

    @staticmethod
    def _runtime_producer_return_code(runtime: dict[str, Any] | None) -> int | None:
        process = (runtime or {}).get("process")
        return getattr(process, "returncode", None) if process is not None else None

    async def _retained_runtime_health(self, runtime: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
        now = time.time()
        source_segments = await self._runtime_source_segments(runtime)
        cached_duration = sum(float(segment["duration"]) for segment in source_segments)
        segment_session = runtime.get("segment_session")
        expected_duration = float(getattr(segment_session, "max_duration_seconds", 0) or 0.0)
        max_segment_duration = max(
            (float(segment["duration"]) for segment in source_segments),
            default=float(CSO_HLS_SEGMENT_SECONDS),
        )
        duration_tolerance = max(2.0, max_segment_duration + 0.5)
        producer_return_code = self._runtime_producer_return_code(runtime)
        producer_complete = producer_return_code is not None
        producer_healthy = (not producer_complete) or producer_return_code == 0
        cache_reaches_boundary = expected_duration <= 0.0 or cached_duration + duration_tolerance >= expected_duration
        stitch_task = runtime.get("stitch_task")
        stitch_alive = stitch_task is not None and not stitch_task.done()
        playlist_age = (
            max(0.0, now - self._last_stitched_playlist_publish_ts)
            if self._last_stitched_playlist_publish_ts > 0.0
            else float("inf")
        )
        freshness_limit = max(12.0, max_segment_duration * 2.5)
        playlist_fresh = self.stitched_playlist_path.is_file() and playlist_age <= freshness_limit
        playlist_advanced = (
            self._stitched_playlist_generation >= 2
            and self._previous_stitched_playlist_publish_ts > 0.0
            and self._last_stitched_playlist_publish_ts > self._previous_stitched_playlist_publish_ts
            and self._last_stitched_published_sequence > self._previous_stitched_published_sequence
        )
        entry = runtime.get("entry") or {}
        boundary_ts = int(entry.get("stop_ts") or 0)
        required_ahead = max(0.0, float(boundary_ts) - now)
        runtime_offset = float(runtime.get("offset_seconds") or 0.0)
        elapsed_from_runtime_start = max(0.0, now - float(entry.get("start_ts") or 0) - runtime_offset)
        cached_ahead = max(0.0, cached_duration - elapsed_from_runtime_start)
        queue_has_headroom = cached_ahead >= min(required_ahead, max(10.0, max_segment_duration * 2.0))
        healthy = (
            stitch_alive
            and playlist_fresh
            and playlist_advanced
            and producer_complete
            and producer_healthy
            and queue_has_headroom
            and cache_reaches_boundary
        )
        return healthy, {
            "source_segments": source_segments,
            "cached_duration": cached_duration,
            "expected_duration": expected_duration,
            "cached_ahead": cached_ahead,
            "required_ahead": required_ahead,
            "producer_complete": producer_complete,
            "producer_return_code": producer_return_code,
            "cache_reaches_boundary": cache_reaches_boundary,
            "stitch_alive": stitch_alive,
            "playlist_age": playlist_age,
            "playlist_fresh": playlist_fresh,
            "playlist_advanced": playlist_advanced,
            "published_sequence": self._last_stitched_published_sequence,
            "queue_has_headroom": queue_has_headroom,
        }

    async def _mark_retune_discontinuity(self) -> None:
        async with self.playlist_lock:
            if self._stitched_playlist_segments:
                self._stitched_playlist_segments[0]["discontinuity"] = True
            await self._prune_stitched_segments()
            await self._write_stitched_playlist(endlist=False)

    async def _rebuild_stitched_window(self, runtime: dict[str, Any], health: dict[str, Any]) -> bool:
        source_segments = list(health.get("source_segments") or [])
        if not source_segments:
            return False
        entry = runtime.get("entry") or {}
        runtime_offset = float(runtime.get("offset_seconds") or 0.0)
        desired_duration = max(0.0, time.time() - float(entry.get("start_ts") or 0) - runtime_offset)
        cached_duration = float(health.get("cached_duration") or 0.0)
        max_segment_duration = max(
            (float(segment["duration"]) for segment in source_segments),
            default=float(CSO_HLS_SEGMENT_SECONDS),
        )
        if desired_duration > cached_duration + max_segment_duration:
            return False

        start_index = 0
        elapsed = 0.0
        for index, segment in enumerate(source_segments):
            segment_end = elapsed + float(segment["duration"])
            if segment_end > desired_duration:
                start_index = max(0, index - 1)
                break
            elapsed = segment_end
        else:
            start_index = max(0, len(source_segments) - 1)

        old_stitch_task = runtime.get("stitch_task")
        task_cleanup = await cancel_and_await_tasks((old_stitch_task,))
        if not task_cleanup.confirmed:
            return False
        await self._prepare_stitched_output_dir()
        runtime["seen_segment_names"] = {segment["name"] for segment in source_segments[:start_index]}
        runtime["buffered_segments"] = []
        runtime["discontinuity_applied"] = False
        runtime.pop("stitched_init_name", None)
        runtime["stitch_publish_started_ts"] = time.time()
        runtime["published_duration_seconds"] = 0.0
        stitch_task = asyncio.create_task(
            self._append_runtime_segments(runtime, discontinuity=True),
            name=f"vod-channel-stitch-{self.channel_id}-{int(runtime.get('segment_index') or 0)}-retune",
        )
        runtime["stitch_task"] = stitch_task
        self.segment_stitch_task = stitch_task
        try:
            await asyncio.wait_for(self._stitched_ready_event.wait(), timeout=20.0)
        except asyncio.TimeoutError:
            return False
        return stitch_task is not None and not stitch_task.done() and self.stitched_playlist_path.is_file()

    async def _prepare_retained_runtime_for_retune(self) -> bool:
        runtime = self._active_runtime
        if runtime is None:
            return False
        healthy, health = await self._retained_runtime_health(runtime)
        if healthy:
            await self._mark_retune_discontinuity()
            logger.info(
                "Reusing healthy retained VOD channel segment cache channel=%s cached_ahead_seconds=%s "
                "required_ahead_seconds=%s playlist_age_seconds=%.3f published_sequence=%s producer_complete=%s",
                self.channel_id,
                int(float(health["cached_ahead"])),
                int(float(health["required_ahead"])),
                float(health["playlist_age"]),
                int(health["published_sequence"]),
                bool(health["producer_complete"]),
            )
            return True
        rebuilt = await self._rebuild_stitched_window(runtime, health)
        logger.warning(
            "VOD channel retained cache validation failed channel=%s stitch_alive=%s playlist_fresh=%s "
            "playlist_advanced=%s published_sequence=%s cached_ahead_seconds=%s required_ahead_seconds=%s "
            "producer_complete=%s producer_return_code=%s cache_reaches_boundary=%s rebuilt=%s",
            self.channel_id,
            bool(health["stitch_alive"]),
            bool(health["playlist_fresh"]),
            bool(health["playlist_advanced"]),
            int(health["published_sequence"]),
            int(float(health["cached_ahead"])),
            int(float(health["required_ahead"])),
            bool(health["producer_complete"]),
            health["producer_return_code"],
            bool(health["cache_reaches_boundary"]),
            rebuilt,
        )
        return rebuilt

    def _canonical_shape_matches(self, probe: dict[str, Any]) -> bool:
        if not self.canonical_output_shape:
            return True
        for field_name in (
            "video_codec",
            "width",
            "height",
            "pixel_format",
            "audio_codec",
            "audio_sample_rate",
            "audio_channels",
        ):
            expected = self.canonical_output_shape.get(field_name)
            if expected in {None, "", 0}:
                continue
            if probe.get(field_name) != expected:
                return False
        expected_fps = float(self.canonical_output_shape.get("fps") or 0.0)
        observed_fps = float(probe.get("fps") or 0.0)
        if expected_fps > 0.0 and abs(expected_fps - observed_fps) > 0.05:
            return False
        return True

    def _segment_policy_for_runtime(
        self,
        source_probe: dict[str, Any] | None = None,
        *,
        optimistic_unknown: bool = False,
    ) -> dict[str, Any]:
        return generate_vod_channel_segment_cache_policy(
            self.config,
            source_probe=source_probe,
            canonical_probe=self.canonical_output_shape,
            optimistic_unknown=optimistic_unknown,
        )

    async def _append_runtime_segments(self, runtime, discontinuity: bool = False):
        segment_session = runtime.get("segment_session")
        if segment_session is None:
            return
        seen_segment_names = runtime.setdefault("seen_segment_names", set())
        buffered_segments = runtime.setdefault("buffered_segments", [])

        while self.running:
            if segment_session.playlist_path.exists():
                try:
                    playlist_text = await asyncio.to_thread(segment_session.playlist_path.read_text, "utf-8")
                except Exception:
                    playlist_text = ""

                for duration_seconds, segment_name in self._parse_playlist_segments(playlist_text):
                    source_segment_path = segment_session.output_dir / segment_name
                    if segment_name in seen_segment_names:
                        continue
                    if not source_segment_path.exists():
                        continue
                    seen_segment_names.add(segment_name)
                    buffered_segments.append((duration_seconds, source_segment_path))

            # Flush buffer if we are the active ingest
            if self._active_ingest == segment_session:
                async with self.playlist_lock:
                    dirty = False

                    if not runtime.get("stitched_init_name"):
                        init_path = segment_session.init_segment_path()
                        if init_path.is_file():
                            init_name = f"init_{self._stitched_init_index:06d}.mp4"
                            self._stitched_init_index += 1
                            await asyncio.to_thread(shutil.copyfile, init_path, self.stitched_output_dir / init_name)
                            runtime["stitched_init_name"] = init_name

                    if runtime.get("stitched_init_name") and buffered_segments:
                        for dur, source_segment_path in buffered_segments:
                            await self._queue_stitched_segment(runtime, dur, source_segment_path, discontinuity)
                        buffered_segments.clear()
                    if await self._publish_queued_stitched_segments(runtime):
                        dirty = True

                    if dirty:
                        await self._write_stitched_playlist(endlist=False, media_update=True)
                        if self._stitched_runtime_ready(runtime):
                            if not self._stitched_ready_event.is_set():
                                self._log_stitched_buffer_metrics(runtime, event="output_start_ready", force=True)
                            self._stitched_ready_event.set()
                        else:
                            self._log_stitched_buffer_metrics(runtime, event="output_start_gated")
                    else:
                        self._log_stitched_buffer_metrics(runtime, event="steady")
                        self._log_stitched_advance_risk(runtime)

            wait_task = segment_session.wait_task
            process_done = wait_task is not None and wait_task.done()
            if process_done:
                runtime["producer_complete"] = True
                if self._stitched_playlist_segments and self._stitched_runtime_ready(runtime):
                    if not self._stitched_ready_event.is_set():
                        self._log_stitched_buffer_metrics(runtime, event="producer_complete_ready", force=True)
                    self._stitched_ready_event.set()
                if self._active_ingest != segment_session:
                    # A pre-cached airing may finish well before its boundary.
                    # Keep its completed segments buffered on disk until activation.
                    await asyncio.sleep(0.2)
                    continue
                if not buffered_segments and not self._queued_stitched_segments:
                    break
            await asyncio.sleep(0.2)

    async def _wait_for_runtime_stitch_drain(self, runtime, timeout_seconds: float = 30.0) -> bool:
        stitch_task = runtime.get("stitch_task") if runtime else None
        if stitch_task is None:
            return True
        runtime["allow_stitch_filler"] = True
        deadline = time.time() + float(timeout_seconds)
        while self.running:
            if stitch_task.done():
                return True
            if time.time() >= deadline:
                logger.warning(
                    "VOD channel runtime stitch drain timed out channel=%s timeout_seconds=%s queued_segments=%s",
                    self.channel_id,
                    int(timeout_seconds),
                    len(self._queued_stitched_segments),
                )
                return False
            await asyncio.sleep(0.25)
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
        retention_task = None
        async with self.lock:
            retention_task = self._boundary_retention_task
            self._boundary_retention_task = None
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
        if retention_task is not None and retention_task is not asyncio.current_task():
            await cancel_and_await_tasks((retention_task,))
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
            lifecycle_references = len(self.lifecycle_references)
        logger.info(
            "VOD channel ingest subscriber removed channel=%s ingest_key=%s subscriber=%s subscribers=%s lifecycle_references=%s",
            self.channel_id,
            self.key,
            subscriber_id,
            remaining,
            lifecycle_references,
        )
        if remaining == 0 and lifecycle_references == 0:
            if not await self._retain_unowned_tail_until_boundary():
                await self.stop(force=True)
        return remaining

    async def add_lifecycle_reference(self, reference_id: str):
        retention_task = None
        async with self.lock:
            retention_task = self._boundary_retention_task
            self._boundary_retention_task = None
            self.lifecycle_references.add(str(reference_id))
            lifecycle_references = len(self.lifecycle_references)
            subscriber_count = len(self.subscribers)
            self.last_activity = time.time()
        if retention_task is not None and retention_task is not asyncio.current_task():
            await cancel_and_await_tasks((retention_task,))
        async with self.playlist_lock:
            # Each output opens the oldest segment currently advertised. Reset
            # the consumption clock for the newest reader so trimming cannot
            # race ahead of a later profile joining the same channel.
            self._start_stitched_reader_clock(self._active_runtime, event="output_reader_start")
        logger.info(
            "VOD channel ingest lifecycle reference added channel=%s ingest_key=%s reference=%s subscribers=%s lifecycle_references=%s",
            self.channel_id,
            self.key,
            reference_id,
            subscriber_count,
            lifecycle_references,
        )

    async def remove_lifecycle_reference(self, reference_id: str) -> int:
        async with self.lock:
            self.lifecycle_references.discard(str(reference_id))
            lifecycle_references = len(self.lifecycle_references)
            subscriber_count = len(self.subscribers)
        if lifecycle_references == 0:
            async with self.playlist_lock:
                self._stop_stitched_reader_clock()
        logger.info(
            "VOD channel ingest lifecycle reference removed channel=%s ingest_key=%s reference=%s subscribers=%s lifecycle_references=%s",
            self.channel_id,
            self.key,
            reference_id,
            subscriber_count,
            lifecycle_references,
        )
        if subscriber_count == 0 and lifecycle_references == 0:
            if not await self._retain_unowned_tail_until_boundary():
                await self.stop(force=True)
        return lifecycle_references

    async def _retain_unowned_tail_until_boundary(self) -> bool:
        async with self.lock:
            if self.subscribers or self.lifecycle_references or not self.running:
                return False
            runtime = self._active_runtime
            boundary_ts = int(((runtime or {}).get("entry") or {}).get("stop_ts") or 0)
            remaining_seconds = boundary_ts - int(time.time())
            if remaining_seconds <= 0:
                return False
            if self._boundary_retention_task is None or self._boundary_retention_task.done():
                self._boundary_retention_task = asyncio.create_task(
                    self._stop_retained_tail_at_boundary(boundary_ts),
                    name=f"vod-channel-tail-retention-{self.channel_id}-{boundary_ts}",
                )
        logger.info(
            "VOD channel retaining segmented tail until scheduled boundary channel=%s stop_ts=%s remaining_seconds=%s",
            self.channel_id,
            boundary_ts,
            remaining_seconds,
        )
        return True

    async def _stop_retained_tail_at_boundary(self, boundary_ts: int) -> None:
        remaining_seconds = int(boundary_ts) - int(time.time())
        if remaining_seconds > 0:
            await asyncio.sleep(remaining_seconds)
        async with self.lock:
            if self.subscribers or self.lifecycle_references:
                self._boundary_retention_task = None
                return
            self._boundary_retention_task = None
        await self.stop(force=True)

    async def start(self, _retune_restart_attempted: bool = False):
        startup_event = None
        validate_retained_runtime = False
        async with self.lifecycle_lock:
            async with self.lock:
                if self.segment_task is not None and self.segment_task.done():
                    self.segment_task = None
                if self.running and self.segment_task is not None and not self.segment_task.done():
                    startup_event = self._startup_event
                    validate_retained_runtime = bool(
                        not self.subscribers
                        and not self.lifecycle_references
                        and self._boundary_retention_task is not None
                    )
                elif (
                    self.process is not None
                    or self._active_runtime is not None
                    or self._active_ingest is not None
                    or self._retained_runtimes
                    or self.segment_task is not None
                    or self._warm_task is not None
                    or self.segment_stitch_task is not None
                ):
                    self.last_error = "previous_lifecycle_cleanup_unconfirmed"
                    logger.error(
                        "VOD channel start blocked by retained lifecycle channel=%s ingest_key=%s pid=%s "
                        "active_ingest=%s retained_runtimes=%s tasks=%s",
                        self.channel_id,
                        self.key,
                        getattr(self.process, "pid", None),
                        self._active_ingest is not None,
                        len(self._retained_runtimes),
                        sum(
                            task is not None
                            for task in (
                                self.segment_task,
                                self._warm_task,
                                self.segment_stitch_task,
                            )
                        ),
                    )
                else:
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
                    self.canonical_output_shape = {}
                    self._startup_event = asyncio.Event()
                    self._startup_succeeded = False
                    self.segment_task = asyncio.create_task(
                        self._run_loop(),
                        name=f"vod-channel-ingest-{self.channel_id}",
                    )
                    startup_event = self._startup_event
        if startup_event is None:
            return
        try:
            await startup_event.wait()
        except asyncio.CancelledError:
            await self.stop(force=True)
            raise
        if validate_retained_runtime and self.running:
            async with self._retune_validation_lock:
                retention_task = self._boundary_retention_task
                if (
                    retention_task is None
                    or self.subscribers
                    or self.lifecycle_references
                    or self._validated_retention_task is retention_task
                ):
                    return
                if await self._prepare_retained_runtime_for_retune():
                    self._validated_retention_task = retention_task
                    return
                if _retune_restart_attempted:
                    self.last_error = "retained_segment_cache_unhealthy"
                    return
                logger.warning(
                    "Discarding unhealthy retained VOD channel cache and restarting offset ingest channel=%s",
                    self.channel_id,
                )
                await self.stop(force=True)
                await self.start(_retune_restart_attempted=True)

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
        self._active_runtime = runtime
        self._active_ingest = runtime.get("segment_session")
        self.process = self._active_ingest.process if self._active_ingest else runtime.get("process")
        self.stderr_task = self._active_ingest.stderr_task if self._active_ingest else runtime.get("stderr_task")
        self._warm_task = None
        self.segment_stitch_task = runtime.get("stitch_task")
        self.current_source = runtime.get("source")
        self.current_source_url = clean_text(runtime.get("input_target"))
        runtime["stitch_publish_started_ts"] = time.time()
        runtime["published_duration_seconds"] = 0.0
        runtime_probe = dict(runtime.get("source_probe") or {})
        self.current_source_probe = {
            "container": "hls",
            "video_codec": clean_key(runtime_probe.get("video_codec"))
            or clean_key(self._active_ingest.policy.get("video_codec") if self._active_ingest else "")
            or "h264",
            "audio_codec": clean_key(runtime_probe.get("audio_codec"))
            or clean_key(self._active_ingest.policy.get("audio_codec") if self._active_ingest else "")
            or "aac",
        }
        self.ingest_policy = dict(runtime.get("segment_policy") or self.ingest_policy)
        self.ingest_policy["video_codec"] = self.current_source_probe["video_codec"]
        self.ingest_policy["audio_codec"] = self.current_source_probe["audio_codec"]

    async def _release_runtime_capacity(self, runtime, reason: str):
        if not runtime:
            return
        release_lock = runtime.get("capacity_release_lock")
        if release_lock is None:
            release_lock = asyncio.Lock()
            runtime["capacity_release_lock"] = release_lock
        async with release_lock:
            if runtime.get("capacity_released"):
                return
            capacity_key = runtime.get("capacity_key")
            owner_key = runtime.get("capacity_owner_key")
            slot_id = runtime.get("capacity_slot_id")
            if not capacity_key or not owner_key or slot_id is None:
                runtime["capacity_released"] = True
                return
            await cso_capacity_registry.release(capacity_key, owner_key, slot_id=slot_id)
            runtime["capacity_released"] = True
            logger.info(
                "VOD channel segment released upstream capacity channel=%s start_ts=%s capacity_key=%s reason=%s",
                self.channel_id,
                int((runtime.get("entry") or {}).get("start_ts") or 0),
                capacity_key,
                reason,
            )

    def _release_runtime_capacity_on_process_exit(self, runtime):
        wait_task = getattr(runtime.get("segment_session"), "wait_task", None)
        if wait_task is None:
            return

        def _release(_completed_task):
            upstream_process = runtime.get("process")
            if upstream_process is not None and getattr(upstream_process, "returncode", None) is None:
                logger.warning(
                    "VOD channel segment wait task ended before upstream exit was confirmed "
                    "channel=%s start_ts=%s pid=%s",
                    self.channel_id,
                    int((runtime.get("entry") or {}).get("start_ts") or 0),
                    getattr(upstream_process, "pid", None),
                )
                return
            asyncio.create_task(
                self._release_runtime_capacity(runtime, "producer_exit"),
                name=f"vod-channel-capacity-release-{self.channel_id}-{int((runtime.get('entry') or {}).get('start_ts') or 0)}",
            )

        wait_task.add_done_callback(_release)

    async def _close_runtime(self, runtime, *, retain_on_failure=True):
        if not runtime:
            return None
        ingest = runtime.get("segment_session")
        runtime_tasks = tuple(
            runtime.get(task_name)
            for task_name in (
                "prefetch_reader_task",
                "warm_task",
                "stitch_task",
                "prepared_task",
            )
        )
        if ingest is None:
            runtime_tasks += (runtime.get("stderr_task"),)
        task_cleanup = await cancel_and_await_tasks(runtime_tasks)
        prepared_runtime_cleanup = None
        prepared_task = runtime.get("prepared_task")
        if prepared_task is not None and prepared_task.done() and not prepared_task.cancelled():
            try:
                prepared_runtime = prepared_task.result()
            except BaseException:
                prepared_runtime = None
            if prepared_runtime is not None:
                runtime["prepared_runtime"] = prepared_runtime
                prepared_runtime_cleanup = await self._close_runtime(
                    prepared_runtime,
                    retain_on_failure=False,
                )
        queue = runtime.get("prefetch_queue")
        if queue is not None:
            try:
                await queue.put_eof()
            except Exception:
                pass
        if ingest is not None:
            if isinstance(ingest, SegmentedHandoffSession):
                ingest_cleanup = await ingest.stop(force=True, release_cache=False)
            else:
                ingest_cleanup = await ingest.stop(force=True)
        else:
            process = runtime.get("process")
            process_teardown = None
            if process is not None:
                process_teardown = await terminate_ffmpeg_process(
                    process,
                    terminate_timeout_seconds=1.0,
                    kill_timeout_seconds=1.0,
                )
                if process_teardown.confirmed and runtime.get("process") is process:
                    runtime["process"] = None
            ingest_cleanup = cso_lifecycle_cleanup_result(process_teardown)
        lifecycle_cleanup = combine_cso_lifecycle_cleanup_results(
            ingest_cleanup,
            prepared_runtime_cleanup,
            task_cleanup=task_cleanup,
        )
        upstream_process = runtime.get("process")
        if upstream_process is None and ingest is not None:
            upstream_process = ingest.process
        if upstream_process is None or getattr(upstream_process, "returncode", None) is not None:
            await self._release_runtime_capacity(runtime, "runtime_close")
        else:
            logger.warning(
                "VOD channel retaining upstream capacity after unconfirmed process teardown "
                "channel=%s start_ts=%s pid=%s",
                self.channel_id,
                int((runtime.get("entry") or {}).get("start_ts") or 0),
                getattr(upstream_process, "pid", None),
            )
        if lifecycle_cleanup.confirmed:
            segment_cache_path = runtime.get("segment_cache_path")
            if segment_cache_path is not None:
                await vod_cache_manager.release_vod_channel_cache_path(segment_cache_path)
                await vod_cache_manager.cleanup_vod_channel_segment_cache("runtime_close")
        if not lifecycle_cleanup.confirmed and retain_on_failure:
            if runtime not in self._retained_runtimes:
                self._retained_runtimes.append(runtime)
        elif runtime in self._retained_runtimes:
            self._retained_runtimes.remove(runtime)
        return lifecycle_cleanup

    async def _buffer_prefetched_runtime(self, runtime):
        ingest = runtime.get("segment_session")
        queue = runtime.get("prefetch_queue")
        if ingest is None or queue is None:
            return
        await queue.put_eof()

    async def _build_segment_runtime(
        self,
        playback,
        segment_index,
        activate_session_state=True,
        capacity_failure_is_error=True,
        capacity_purpose="interactive_playback",
    ):
        candidate = playback.get("candidate")
        upstream_url = clean_text(playback.get("upstream_url"))
        source_item = playback.get("source_item")
        entry = playback.get("entry") or {}
        entry_start_ts = int(entry.get("start_ts") or 0)
        entry_stop_ts = int(entry.get("stop_ts") or 0)
        entry_duration_seconds = max(1, entry_stop_ts - entry_start_ts)
        requested_offset_seconds = max(0, int(playback.get("offset_seconds") or 0))
        # Resolve the seek again immediately before starting FFmpeg so URL and
        # capacity lookup time does not make the oldest buffered segment stale.
        # The producer begins at the intended programme position and then runs
        # unpaced to materialise TARGET_SECONDS ahead of that position.
        live_offset_seconds = max(0, int(time.time()) - entry_start_ts) if entry_start_ts > 0 else 0
        offset_seconds = min(
            max(0, entry_duration_seconds - 1),
            max(requested_offset_seconds, live_offset_seconds),
        )
        remaining_seconds = max(1, entry_duration_seconds - offset_seconds)
        if candidate is None or not upstream_url or source_item is None:
            return None

        source = await cso_source_from_vod_source(candidate, upstream_url)
        if source is None:
            return None
        logger.info(
            "VOD channel segmented ingest seek channel=%s start_ts=%s requested_offset_seconds=%s "
            "producer_seek_offset_seconds=%s target_edge_offset_seconds=%s target_buffer_seconds=%s",
            self.channel_id,
            entry_start_ts,
            requested_offset_seconds,
            offset_seconds,
            min(entry_duration_seconds, offset_seconds + int(VOD_CHANNEL_STITCHED_TARGET_SECONDS)),
            int(VOD_CHANNEL_STITCHED_TARGET_SECONDS),
        )
        input_target = source.url
        capacity_key = source_capacity_key(source)
        capacity_owner_key = f"vod-channel-segment:{self.channel_id}:{entry_start_ts}"
        capacity_slot_id = source.id
        reserved = await vod_cache_manager.reserve_capacity(
            capacity_key,
            source_capacity_limit(source),
            capacity_owner_key,
            capacity_slot_id,
            purpose=capacity_purpose,
        )
        if not reserved:
            logger.info(
                "VOD channel segment cache waiting for upstream capacity channel=%s start_ts=%s capacity_key=%s purpose=%s",
                self.channel_id,
                entry_start_ts,
                capacity_key,
                capacity_purpose,
            )
            if capacity_failure_is_error:
                self.last_error = "capacity_blocked"
            return None

        source_probe = dict(source.probe_details or {})
        fallback_policy = self._segment_policy_for_runtime(source_probe)
        optimistic_unknown = not clean_key(source_probe.get("video_codec")) or not clean_key(
            source_probe.get("audio_codec")
        )
        segment_policy = self._segment_policy_for_runtime(
            source_probe,
            optimistic_unknown=optimistic_unknown,
        )

        def _new_segment_session(policy: dict[str, Any]) -> SegmentedHandoffSession:
            return SegmentedHandoffSession(
                key=f"{self.key}-segment-{segment_index}",
                policy=policy,
                input_target=input_target,
                input_is_url=bool(input_target.startswith("http://") or input_target.startswith("https://")),
                user_agent=resolve_cso_ingest_user_agent(self.config, source),
                request_headers=resolve_cso_ingest_headers(source),
                cache_root_dir=self.segment_cache_root,
                start_seconds=offset_seconds,
                max_duration_seconds=remaining_seconds,
                realtime=False,
                source_probe=source_probe,
            )

        segment_session = _new_segment_session(segment_policy)
        runtime = {
            "segment_session": segment_session,
            "entry": entry,
            "playback": playback,
            "source": source,
            "input_target": input_target,
            "pipe_container": "hls",
            "source_probe": source_probe,
            "segment_policy": segment_policy,
            "capacity_key": capacity_key,
            "capacity_owner_key": capacity_owner_key,
            "capacity_slot_id": capacity_slot_id,
            "capacity_release_lock": asyncio.Lock(),
            "capacity_released": False,
            "segment_index": segment_index,
            "offset_seconds": offset_seconds,
            "segment_cache_path": segment_session.output_dir,
        }
        await vod_cache_manager.register_vod_channel_cache_path(
            segment_session.output_dir,
            self.key,
            self.channel_id,
            "producer",
            scheduled_stop_ts=entry_stop_ts,
        )
        self._retained_runtimes.append(runtime)
        try:
            started = await segment_session.start()
            if not started and segment_policy != fallback_policy:
                optimistic_cleanup = await segment_session.stop(force=True)
                if not optimistic_cleanup.confirmed:
                    self.last_error = "vod_segment_optimistic_remux_cleanup_unconfirmed"
                    self.running = False
                    return None
                logger.info(
                    "VOD channel optimistic segment remux failed; retrying sequential transcode "
                    "channel=%s start_ts=%s source_item_id=%s",
                    self.channel_id,
                    entry_start_ts,
                    int(entry.get("source_item_id") or 0),
                )
                segment_policy = fallback_policy
                segment_session = _new_segment_session(segment_policy)
                runtime["segment_session"] = segment_session
                runtime["segment_policy"] = segment_policy
                started = await segment_session.start()
            if not started:
                cleanup = await self._close_runtime(runtime)
                if cleanup is not None and not cleanup.confirmed:
                    self.last_error = "vod_segment_start_failed:cleanup_unconfirmed"
                    self.running = False
                return None
            output_probe = await segment_session.detect_output_probe()
            detected_policy = self._segment_policy_for_runtime(output_probe)
            if segment_policy != fallback_policy and clean_key(detected_policy.get("output_mode")) != "force_remux":
                optimistic_cleanup = await segment_session.stop(force=True)
                if not optimistic_cleanup.confirmed:
                    self.last_error = "vod_segment_optimistic_remux_cleanup_unconfirmed"
                    self.running = False
                    return None
                logger.info(
                    "VOD channel optimistic segment remux was incompatible; retrying sequential transcode "
                    "channel=%s start_ts=%s source_item_id=%s detected_video=%s detected_audio=%s",
                    self.channel_id,
                    entry_start_ts,
                    int(entry.get("source_item_id") or 0),
                    clean_key(output_probe.get("video_codec")) or "unknown",
                    clean_key(output_probe.get("audio_codec")) or "unknown",
                )
                segment_policy = fallback_policy
                segment_session = _new_segment_session(segment_policy)
                runtime["segment_session"] = segment_session
                runtime["segment_policy"] = segment_policy
                if not await segment_session.start():
                    cleanup = await self._close_runtime(runtime)
                    if cleanup is not None and not cleanup.confirmed:
                        self.last_error = "vod_segment_fallback_start_failed:cleanup_unconfirmed"
                        self.running = False
                    return None
                output_probe = await segment_session.detect_output_probe()

            runtime["process"] = segment_session.process
            self._release_runtime_capacity_on_process_exit(runtime)
            if float(output_probe.get("fps") or 0.0) <= 0.0:
                fallback_fps = float(source_probe.get("fps") or 0.0)
                if fallback_fps > 0.0:
                    output_probe["fps"] = fallback_fps
                    avg_frame_rate = clean_text(source_probe.get("avg_frame_rate"))
                    if avg_frame_rate:
                        output_probe["avg_frame_rate"] = avg_frame_rate
            if not self.canonical_output_shape:
                self.canonical_output_shape = dict(output_probe or {})
            elif not self._canonical_shape_matches(output_probe):
                logger.warning(
                    "VOD channel prepared runtime shape mismatch channel=%s start_ts=%s expected=%s observed=%s",
                    self.channel_id,
                    entry_start_ts,
                    self.canonical_output_shape,
                    output_probe,
                )
                cleanup = await self._close_runtime(runtime)
                if cleanup is not None and not cleanup.confirmed:
                    self.last_error = "vod_segment_shape_mismatch:cleanup_unconfirmed"
                    self.running = False
                return None

            runtime["source_probe"] = dict(output_probe or source_probe or {})
            runtime["stitch_task"] = asyncio.create_task(
                self._append_runtime_segments(runtime, discontinuity=segment_index > 0),
                name=f"vod-channel-stitch-{self.channel_id}-{segment_index}",
            )
            if activate_session_state:
                self._activate_runtime(runtime)
            self._retained_runtimes.remove(runtime)
            return runtime
        except (asyncio.CancelledError, Exception):
            try:
                cleanup = await self._close_runtime(runtime)
            except Exception:
                logger.exception(
                    "Failed to clean VOD segment runtime after construction exception "
                    "channel=%s ingest_key=%s segment_index=%s",
                    self.channel_id,
                    self.key,
                    segment_index,
                )
                self.running = False
                self.last_error = "vod_segment_construction_exception:cleanup_failed"
            else:
                if cleanup is not None and not cleanup.confirmed:
                    self.running = False
                    self.last_error = "vod_segment_construction_exception:cleanup_unconfirmed"
            raise

    async def _skip_near_boundary_start(self, playback):
        entry = (playback or {}).get("entry") or {}
        next_entry = (playback or {}).get("next_entry") or {}
        if not entry or not next_entry:
            return playback
        stop_ts = int(entry.get("stop_ts") or 0)
        remaining_seconds = stop_ts - int(time.time())
        if remaining_seconds > self._startup_boundary_skip_seconds():
            return playback

        next_start_ts = int(next_entry.get("start_ts") or 0)
        if next_start_ts <= 0:
            return playback

        from backend.vod_channels import resolve_vod_channel_playback_target

        next_playback = await resolve_vod_channel_playback_target(
            self.config,
            self.channel_id,
            now_ts=max(next_start_ts, int(time.time())),
        )
        if not next_playback:
            return playback
        logger.info(
            "VOD channel skipping near-boundary startup channel=%s current_stop_ts=%s remaining_seconds=%s "
            "next_start_ts=%s source_item_id=%s",
            self.channel_id,
            stop_ts,
            max(0, remaining_seconds),
            next_start_ts,
            int(next_entry.get("source_item_id") or 0),
        )
        return next_playback

    async def _prepare_next_segment_runtime(self, playback, segment_index):
        next_entry = (playback or {}).get("next_entry")
        if not next_entry:
            return None

        next_identity = self._entry_identity(next_entry)
        next_start_ts = int(next_entry.get("start_ts") or 0)
        if next_start_ts <= 0:
            return None

        prestart_ts = max(0, next_start_ts - int(VOD_CHANNEL_NEXT_SEGMENT_CACHE_SECONDS))
        logger.info(
            "VOD channel next segmented cache scheduled channel=%s start_ts=%s source_item_id=%s cache_window_seconds=%s",
            self.channel_id,
            next_start_ts,
            int(next_entry.get("source_item_id") or 0),
            int(VOD_CHANNEL_NEXT_SEGMENT_CACHE_SECONDS),
        )
        while self.running:
            remaining_seconds = prestart_ts - int(time.time())
            if remaining_seconds <= 0:
                break
            await asyncio.sleep(min(1.0, float(remaining_seconds)))

        if not self.running:
            return None

        from backend.vod_channels import resolve_vod_channel_playback_target

        while self.running and int(time.time()) < next_start_ts:
            prepared_playback = await resolve_vod_channel_playback_target(
                self.config,
                self.channel_id,
                now_ts=next_start_ts,
            )
            if not prepared_playback:
                logger.warning(
                    "VOD channel could not resolve next playback during cache window channel=%s start_ts=%s",
                    self.channel_id,
                    next_start_ts,
                )
                return None
            if self._entry_identity((prepared_playback.get("entry") or {})) != next_identity:
                logger.warning(
                    "VOD channel next playback identity changed during cache window channel=%s expected_start_ts=%s resolved_start_ts=%s",
                    self.channel_id,
                    next_start_ts,
                    int((prepared_playback.get("entry") or {}).get("start_ts") or 0),
                )
                return None

            runtime = await self._build_segment_runtime(
                prepared_playback,
                segment_index,
                activate_session_state=False,
                capacity_failure_is_error=False,
                capacity_purpose="background_warm",
            )
            if runtime is not None:
                logger.info(
                    "Prepared next VOD channel segmented cache channel=%s start_ts=%s source_item_id=%s seconds_ahead=%s",
                    self.channel_id,
                    next_start_ts,
                    int(next_entry.get("source_item_id") or 0),
                    max(0, next_start_ts - int(time.time())),
                )
                return runtime
            remaining_seconds = next_start_ts - int(time.time())
            if remaining_seconds <= 1:
                break
            await asyncio.sleep(min(5, max(1, remaining_seconds - 1)))

        logger.warning(
            "VOD channel next segmented cache was not ready by boundary channel=%s start_ts=%s source_item_id=%s",
            self.channel_id,
            next_start_ts,
            int(next_entry.get("source_item_id") or 0),
        )
        return None

    async def _take_prepared_runtime(
        self,
        prepared_task: asyncio.Task[dict[str, Any] | None] | None,
        wait_timeout_seconds: float = 0.0,
    ) -> dict[str, Any] | None:
        if prepared_task is None:
            return None
        if not prepared_task.done():
            if wait_timeout_seconds > 0:
                try:
                    await asyncio.wait_for(prepared_task, timeout=float(wait_timeout_seconds))
                except asyncio.TimeoutError:
                    logger.warning(
                        "VOD channel prepared runtime did not finish before timeout channel=%s wait_timeout_seconds=%s",
                        self.channel_id,
                        int(wait_timeout_seconds),
                    )
                except BaseException:
                    pass
            if prepared_task.done():
                try:
                    return prepared_task.result()
                except BaseException:
                    return None
            task_cleanup = await cancel_and_await_tasks((prepared_task,))
            if not task_cleanup.confirmed:
                retained_runtime = {"prepared_task": prepared_task}
                if retained_runtime not in self._retained_runtimes:
                    self._retained_runtimes.append(retained_runtime)
            return None
        try:
            return prepared_task.result()
        except BaseException:
            return None

    async def _wait_for_next_playback(self, current_entry: dict[str, Any]) -> dict[str, Any] | None:
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
            if boundary_ts > 0 and int(time.time()) + int(VOD_CHANNEL_NEXT_SEGMENT_PRESTART_SECONDS) < boundary_ts:
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
        runtime = self._active_runtime
        if runtime is None:
            runtime = {
                "segment_session": self._active_ingest,
                "process": self.process,
                "stderr_task": self.stderr_task,
                "stitch_task": self.segment_stitch_task,
            }
        lifecycle_cleanup = await self._close_runtime(runtime, retain_on_failure=False)
        ingest = runtime.get("segment_session")
        retained_process = ingest.process if ingest is not None else runtime.get("process")
        if lifecycle_cleanup is not None and not lifecycle_cleanup.confirmed:
            self.last_error = (
                "vod_segment_teardown_unconfirmed"
                if any(not result.confirmed for result in lifecycle_cleanup.process_teardowns)
                else "vod_segment_task_cleanup_unconfirmed"
            )
            self.running = False
            self._active_runtime = runtime
            self._active_ingest = ingest
            self.process = retained_process
            self._warm_task = runtime.get("warm_task")
            self.stderr_task = ingest.stderr_task if ingest is not None else runtime.get("stderr_task")
            self.segment_stitch_task = runtime.get("stitch_task")
            logger.error(
                "VOD channel segment lifecycle cleanup unconfirmed channel=%s ingest_key=%s pid=%s failure=%s",
                self.channel_id,
                self.key,
                getattr(retained_process, "pid", None),
                lifecycle_cleanup.failure,
            )
            return lifecycle_cleanup
        self._active_runtime = None
        self._active_ingest = None
        self.process = None
        self._warm_task = None
        self.stderr_task = None
        self.segment_stitch_task = None
        return lifecycle_cleanup

    async def _release_completed_segment_task_owner(self):
        current_task = asyncio.current_task()
        async with self.lock:
            if self.segment_task is current_task:
                self.segment_task = None

    async def _run_loop(self):
        from backend.vod_channels import resolve_vod_channel_playback_target

        startup_event = self._startup_event
        await self._prepare_stitched_output_dir()
        playback = await resolve_vod_channel_playback_target(self.config, self.channel_id)
        if not playback:
            self.last_error = "no_scheduled_programme"
            self.running = False
            if startup_event is not None:
                startup_event.set()
            await self._finish_session()
            await self._release_completed_segment_task_owner()
            return
        playback = await self._skip_near_boundary_start(playback)
        if not playback:
            self.last_error = "no_scheduled_programme"
            self.running = False
            if startup_event is not None:
                startup_event.set()
            await self._finish_session()
            await self._release_completed_segment_task_owner()
            return

        segment_index = 0
        prepared_task = None
        try:
            prepared_runtime = None
            while self.running and playback:
                try:
                    current_playback = playback
                    runtime = prepared_runtime
                    if runtime is None:
                        runtime = await self._build_segment_runtime(current_playback, segment_index)
                    else:
                        self._activate_runtime(runtime)
                    prepared_runtime = None
                    if runtime is None:
                        if not self.last_error:
                            self.last_error = "vod_channel_segment_unavailable"
                        self.running = False
                        if startup_event is not None:
                            startup_event.set()
                        break
                    if startup_event is not None and not startup_event.is_set():
                        try:
                            await asyncio.wait_for(self._stitched_ready_event.wait(), timeout=20.0)
                            self._startup_succeeded = True
                        except asyncio.TimeoutError:
                            self.last_error = "vod_channel_segment_startup_timeout"
                            self.running = False
                        startup_event.set()
                        if not self.running:
                            break

                    current_entry = runtime["entry"]
                    prepared_task = asyncio.create_task(
                        self._prepare_next_segment_runtime(current_playback, segment_index + 1),
                        name=f"vod-channel-prepare-{self.channel_id}-{segment_index + 1}",
                    )
                    self.current_segment_healthy = False
                    boundary_ts = int(current_entry.get("stop_ts") or 0)
                    try:
                        while self.running:
                            if self._stitched_ready_event.is_set():
                                self.current_segment_healthy = True
                                if not self.first_healthy_stream_seen:
                                    logger.info(
                                        "VOD channel segmented playlist primed channel=%s ingest_key=%s elapsed_ms=%s",
                                        self.channel_id,
                                        self.key,
                                        int(max(0.0, time.time() - float(self.session_start_ts or time.time())) * 1000),
                                    )
                                    self.first_healthy_stream_seen = True
                            now_ts = int(time.time())
                            if boundary_ts > 0 and now_ts >= boundary_ts:
                                break
                            await asyncio.sleep(0.25)
                    finally:
                        self.last_reader_end_reason = "ingest_reader_ended"
                        self.last_reader_end_saw_data = bool(self._stitched_segment_names)
                        segment_session = runtime.get("segment_session")
                        self.last_reader_end_return_code = (
                            segment_session.process.returncode if segment_session and segment_session.process else None
                        )
                        self.last_reader_end_ts = time.time()
                    await self._wait_for_runtime_stitch_drain(
                        runtime,
                        timeout_seconds=max(30.0, float(CSO_HLS_LIST_SIZE) * float(CSO_HLS_SEGMENT_SECONDS)),
                    )

                    if not self.running:
                        await self._close_runtime(await self._take_prepared_runtime(prepared_task))
                        prepared_task = None
                        break
                    wait_timeout_seconds = 8.0 if current_playback.get("next_entry") else 0.0
                    prepared_runtime = await self._take_prepared_runtime(
                        prepared_task,
                        wait_timeout_seconds=wait_timeout_seconds,
                    )
                    prepared_task = None
                    if prepared_runtime is not None:
                        next_entry = prepared_runtime.get("entry") or {}
                        boundary_delta_seconds = 0
                        if boundary_ts > 0:
                            boundary_delta_seconds = int(time.time()) - boundary_ts
                        logger.info(
                            "VOD channel continuing with prepared runtime after current queue drained channel=%s "
                            "previous_stop_ts=%s next_start_ts=%s source_item_id=%s boundary_delta_seconds=%s",
                            self.channel_id,
                            boundary_ts,
                            int(next_entry.get("start_ts") or 0),
                            int(next_entry.get("source_item_id") or 0),
                            boundary_delta_seconds,
                        )
                        teardown = await self._close_active_segment()
                        if teardown is not None and not teardown.confirmed:
                            await self._close_runtime(prepared_runtime)
                            prepared_runtime = None
                            break
                        playback = prepared_runtime.get("playback")
                        segment_index += 1
                        continue

                    next_playback = await self._wait_for_next_playback(current_entry)
                    teardown = await self._close_active_segment()
                    if teardown is not None and not teardown.confirmed:
                        if prepared_runtime is not None:
                            await self._close_runtime(prepared_runtime)
                            prepared_runtime = None
                        break
                    if prepared_runtime is not None and (
                        next_playback is None
                        or self._entry_identity(prepared_runtime.get("entry"))
                        != self._entry_identity(next_playback.get("entry"))
                    ):
                        await self._close_runtime(prepared_runtime)
                        prepared_runtime = None
                    playback = next_playback
                    segment_index += 1
                except Exception as e:
                    logger.exception("VOD channel ingest loop error channel=%s", self.channel_id)
                    self.last_error = str(e)
                    break

            if startup_event is not None and not startup_event.is_set():
                startup_event.set()
        finally:
            self.running = False
            if prepared_task is not None:
                try:
                    await self._close_runtime(await self._take_prepared_runtime(prepared_task))
                except BaseException:
                    pass
            async with self.playlist_lock:
                await self._write_stitched_playlist(endlist=True)
            await self._finish_session()
            await self._release_completed_segment_task_owner()

    async def _finish_session(self):
        lifecycle_cleanup = await self._close_active_segment()
        async with self.lock:
            cleanup_confirmed = (
                lifecycle_cleanup is None or lifecycle_cleanup.confirmed
            ) and not self._retained_runtimes
            subscribers = list(self.subscribers.values())
            self.subscribers = {}
            self.lifecycle_references = set()
            self.history.clear()
            self.history_bytes = 0
            self.failover_exhausted = bool(self.last_error)
            if cleanup_confirmed:
                self.current_source = None
                self.current_source_url = ""
                self.current_source_probe = {}
                self.canonical_output_shape = {}
        for queue in subscribers:
            await queue.put_eof()
        if cleanup_confirmed:
            await self._close_stitched_server()
            await vod_cache_manager.release_vod_channel_cache_path(self.stitched_cache_root)
            await remove_cso_cache_dir(self.stitched_cache_root, logger, f"vod-247-stitched:{self.key}")
            await vod_cache_manager.cleanup_vod_channel_segment_cache("channel_finish")
        return lifecycle_cleanup

    async def stop(self, force: bool = False):
        if not force and await self._retain_unowned_tail_until_boundary():
            return None
        async with self.lifecycle_lock:
            async with self.lock:
                if (
                    not self.running
                    and self.segment_task is None
                    and not self.process
                    and not self._active_runtime
                    and not self._active_ingest
                    and not self._retained_runtimes
                    and not self.subscribers
                    and self._warm_task is None
                    and self.segment_stitch_task is None
                    and self._boundary_retention_task is None
                ):
                    return
                if not force and self.subscribers:
                    return
                self.running = False
                segment_task = self.segment_task
                self.segment_task = None
                boundary_retention_task = self._boundary_retention_task
                self._boundary_retention_task = None
                self._validated_retention_task = None
            retention_cleanup = None
            if boundary_retention_task is not asyncio.current_task():
                retention_cleanup = await cancel_and_await_tasks((boundary_retention_task,))
            lifecycle_results = [await self._close_active_segment()]
            for runtime in list(self._retained_runtimes):
                lifecycle_results.append(await self._close_runtime(runtime))
            segment_task_cleanup = await cancel_and_await_tasks((segment_task,))
            if not segment_task_cleanup.confirmed:
                async with self.lock:
                    self.segment_task = segment_task if segment_task in segment_task_cleanup.pending_tasks else None
            lifecycle_cleanup = combine_cso_lifecycle_cleanup_results(
                *lifecycle_results,
                task_cleanup=segment_task_cleanup,
            )
            if retention_cleanup is not None:
                lifecycle_cleanup = combine_cso_lifecycle_cleanup_results(
                    lifecycle_cleanup,
                    task_cleanup=retention_cleanup,
                )
            if lifecycle_cleanup.confirmed:
                async with self.lock:
                    self.current_source = None
                    self.current_source_url = ""
                    self.current_source_probe = {}
                    self.canonical_output_shape = {}
                await self._close_stitched_server()
                await vod_cache_manager.release_vod_channel_cache_path(self.stitched_cache_root)
                await remove_cso_cache_dir(self.stitched_cache_root, logger, f"vod-247-stitched:{self.key}")
                await vod_cache_manager.cleanup_vod_channel_segment_cache("channel_stop")
            return lifecycle_cleanup
