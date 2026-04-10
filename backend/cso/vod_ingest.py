import asyncio
import logging
import os
import shutil
import time
from collections import deque
from pathlib import Path
from typing import Any

from backend.utils import clean_key, clean_text

from .common import ByteBudgetQueue, wait_process_exit_with_timeout
from .constants import (
    CSO_INGEST_HISTORY_MAX_BYTES,
    CSO_INGEST_SUBSCRIBER_QUEUE_MAX_BYTES,
    MPEGTS_CHUNK_BYTES,
    CSO_SEGMENT_CACHE_ROOT,
    VOD_CHANNEL_NEXT_SEGMENT_BUFFER_BYTES,
    VOD_CHANNEL_NEXT_SEGMENT_PRESTART_SECONDS,
)
from .ffmpeg import (
    CsoFfmpegCommandBuilder,
    redact_ingest_command_for_log,
    start_ffmpeg_with_hw_decode_fallback,
    wait_for_process_output_start,
)
from .live_ingest import resolve_cso_ingest_headers, resolve_cso_ingest_user_agent
from .output import CsoOutputSession, policy_log_label
from .policy import generate_vod_channel_ingest_policy, should_prefer_direct_vod_url_input
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
            try:
                self._output_queue.put_nowait(None)
            except Exception:
                pass

    async def start(self):
        async with self.lock:
            if self.running:
                return True
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

            async def _attempt_start(effective_policy):
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
                    redact_ingest_command_for_log(command) if input_is_url else command,
                )
                process = await asyncio.create_subprocess_exec(
                    *command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stderr_task = asyncio.create_task(
                    self._read_stderr(process),
                    name=f"vod-ingest-stderr-{self.key}",
                )
                startup_timeout_seconds = 20.0 if use_direct_upstream_input else 10.0
                started, startup_failure_reason, startup_chunk = await wait_for_process_output_start(
                    process,
                    process.stdout,
                    timeout_seconds=startup_timeout_seconds,
                )
                if started:
                    return True, (process, stderr_task, startup_chunk), ""

                logger.warning(
                    "VOD ingest start failed source_id=%s reason=%s",
                    self.source_id,
                    startup_failure_reason or "unknown",
                )
                if stderr_task is not None and not stderr_task.done():
                    stderr_task.cancel()
                if process.returncode is None:
                    try:
                        process.terminate()
                        await wait_process_exit_with_timeout(process, timeout_seconds=1.0)
                    except Exception:
                        pass
                return False, None, startup_failure_reason

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

            started, start_policy, result, failure_reason = await start_ffmpeg_with_hw_decode_fallback(
                base_policy,
                source_identity,
                _attempt_start,
            )
            if not started:
                self.running = False
                self.last_error = failure_reason or "ingest_start_failed"
                return False

            self.ingest_policy = dict(start_policy)
            self.process, self.stderr_task, startup_chunk = result

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
            for task in (self.stderr_task, self.stdout_task, self._warm_task):
                if task is not None and not task.done():
                    task.cancel()
                    try:
                        await task
                    except BaseException:
                        pass
            if self.process is not None and self.process.returncode is None:
                try:
                    self.process.terminate()
                    await wait_process_exit_with_timeout(self.process, timeout_seconds=1.0)
                except Exception:
                    try:
                        self.process.kill()
                        await wait_process_exit_with_timeout(self.process, timeout_seconds=1.0)
                    except Exception:
                        pass
            self.process = None
            self.stderr_task = None
            self.stdout_task = None
            self._warm_task = None
            self.last_reader_end_ts = time.time()


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
        self._active_ingest = None
        self.canonical_output_shape = {}
        self.segment_cache_root = Path(CSO_SEGMENT_CACHE_ROOT) / self.key
        self.stitched_output_dir = self.segment_cache_root / "stitched"
        self.stitched_playlist_path = self.stitched_output_dir / "index.m3u8"
        self.stitched_init_path = self.stitched_output_dir / "init.mp4"
        self._stitched_playlist_lines = []
        self._stitched_segment_names = set()
        self._stitched_segment_index = 0
        self._stitched_episode_count = 0
        self._stitched_ready_event = asyncio.Event()

    def get_output_input_target(self):
        return str(self.stitched_playlist_path)

    def is_hunting_for_stream(self):
        if not self.running:
            return True
        if self.current_source is None:
            return True
        if not self.current_segment_healthy:
            return True
        return False

    async def _prepare_stitched_output_dir(self):
        if self.stitched_output_dir.exists():
            await asyncio.to_thread(shutil.rmtree, self.stitched_output_dir, True)
        self.stitched_output_dir.mkdir(parents=True, exist_ok=True)
        self._stitched_playlist_lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:7",
            "#EXT-X-PLAYLIST-TYPE:EVENT",
            "#EXT-X-INDEPENDENT-SEGMENTS",
            "#EXT-X-TARGETDURATION:2",
            "#EXT-X-MEDIA-SEQUENCE:0",
        ]
        self._stitched_segment_names.clear()
        self._stitched_segment_index = 0
        self._stitched_episode_count = 0
        self._stitched_ready_event = asyncio.Event()
        await self._write_stitched_playlist(endlist=False)

    async def _write_stitched_playlist(self, endlist: bool = False):
        lines = list(self._stitched_playlist_lines)
        if self.stitched_init_path.exists():
            map_line = '#EXT-X-MAP:URI="init.mp4"'
            if map_line not in lines:
                lines.insert(6, map_line)
        if endlist and (not lines or lines[-1] != "#EXT-X-ENDLIST"):
            lines.append("#EXT-X-ENDLIST")
        payload = "\n".join(lines).rstrip() + "\n"
        temp_path = self.stitched_output_dir / "index.m3u8.tmp"
        await asyncio.to_thread(temp_path.write_text, payload, "utf-8")
        await asyncio.to_thread(temp_path.replace, self.stitched_playlist_path)

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

    def _segment_policy_for_runtime(self) -> dict[str, Any]:
        policy = generate_vod_channel_ingest_policy(self.config, self.requested_policy)
        policy["container"] = "hls"
        policy["hls_segment_type"] = "fmp4"
        policy["hls_playlist_mode"] = "event"
        if self.canonical_output_shape:
            policy["target_width"] = int(self.canonical_output_shape.get("width") or 0)
            policy["target_height"] = int(self.canonical_output_shape.get("height") or 0)
            policy["output_pixel_format"] = clean_key(self.canonical_output_shape.get("pixel_format")) or "yuv420p"
            policy["output_fps"] = float(self.canonical_output_shape.get("fps") or 0.0)
            policy["audio_sample_rate"] = int(self.canonical_output_shape.get("audio_sample_rate") or 48000)
            policy["audio_channels"] = int(self.canonical_output_shape.get("audio_channels") or 2)
        return policy

    async def _append_runtime_segments(self, runtime, discontinuity: bool = False):
        segment_session = runtime.get("segment_session")
        if segment_session is None:
            return
        seen_segment_names = runtime.setdefault("seen_segment_names", set())
        if not self.stitched_init_path.exists():
            init_path = segment_session.init_segment_path()
            deadline = time.time() + 10.0
            while time.time() < deadline and not init_path.exists():
                await asyncio.sleep(0.1)
            if init_path.exists():
                await asyncio.to_thread(shutil.copyfile, init_path, self.stitched_init_path)
                await self._write_stitched_playlist(endlist=False)
        if discontinuity and self._stitched_segment_index > 0:
            self._stitched_playlist_lines.append("#EXT-X-DISCONTINUITY")
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
                    stitched_name = f"seg_{self._stitched_segment_index:06d}.m4s"
                    stitched_path = self.stitched_output_dir / stitched_name
                    if stitched_path.exists():
                        stitched_path.unlink(missing_ok=True)
                    try:
                        await asyncio.to_thread(os.link, source_segment_path, stitched_path)
                    except Exception:
                        await asyncio.to_thread(shutil.copyfile, source_segment_path, stitched_path)
                    seen_segment_names.add(segment_name)
                    self._stitched_segment_names.add(stitched_name)
                    self._stitched_playlist_lines.append(f"#EXTINF:{duration_seconds:.3f},")
                    self._stitched_playlist_lines.append(stitched_name)
                    self._stitched_segment_index += 1
                    await self._write_stitched_playlist(endlist=False)
                    self._stitched_ready_event.set()
            if segment_session.process is not None and segment_session.process.returncode is not None:
                break
            await asyncio.sleep(0.2)

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
        async with self.lifecycle_lock:
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
                self.canonical_output_shape = {}
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
        self._active_ingest = runtime.get("segment_session")
        self.process = self._active_ingest.process if self._active_ingest else runtime.get("process")
        self.stderr_task = self._active_ingest.stderr_task if self._active_ingest else runtime.get("stderr_task")
        self._warm_task = runtime.get("warm_task")
        self.current_source = runtime.get("source")
        self.current_source_url = clean_text(runtime.get("input_target"))
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

    async def _close_runtime(self, runtime):
        if not runtime:
            return
        for task_name in ("prefetch_reader_task", "warm_task", "stitch_task"):
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
        ingest = runtime.get("segment_session")
        if ingest is not None:
            await ingest.stop(force=True)
            return
        process = runtime.get("process")
        if process is not None and process.returncode is None:
            try:
                process.terminate()
                await wait_process_exit_with_timeout(process, timeout_seconds=1.0)
            except Exception:
                try:
                    process.kill()
                    await wait_process_exit_with_timeout(process, timeout_seconds=1.0)
                except Exception:
                    pass

    async def _buffer_prefetched_runtime(self, runtime):
        ingest = runtime.get("segment_session")
        queue = runtime.get("prefetch_queue")
        if ingest is None or queue is None:
            return
        await queue.put_eof()

    async def _warm_next_item_cache(self, next_entry, remaining_seconds):
        if not next_entry:
            return
        next_start_ts = int(next_entry.get("start_ts") or 0)
        from backend.vod_channels import NEXT_ITEM_CACHE_WARM_SECONDS, resolve_vod_channel_playback_target

        wait_seconds = max(0, int(remaining_seconds or 0) - int(NEXT_ITEM_CACHE_WARM_SECONDS))
        logger.info(
            "VOD channel next-item cache warm scheduled channel=%s start_ts=%s source_item_id=%s wait_seconds=%s "
            "warm_window_seconds=%s",
            self.channel_id,
            next_start_ts,
            int(next_entry.get("source_item_id") or 0),
            wait_seconds,
            int(NEXT_ITEM_CACHE_WARM_SECONDS),
        )
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
        owner_key = f"vod-channel-next-{self.channel_id}-{next_start_ts}"

        while self.running:
            now_ts = int(time.time())
            if next_start_ts > 0 and now_ts >= next_start_ts:
                logger.warning(
                    "VOD channel next-item cache warm window expired before fetch completed channel=%s start_ts=%s "
                    "source_item_id=%s",
                    self.channel_id,
                    next_start_ts,
                    int(next_entry.get("source_item_id") or 0),
                )
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
                    next_source = await cso_source_from_vod_source(next_candidate, next_upstream_url)
                    cache_state = "unknown"
                    if next_source is not None:
                        next_cache_entry = await vod_cache_manager.get_or_create(next_source, next_source.url)
                        if next_cache_entry.complete and next_cache_entry.final_path.exists():
                            cache_state = "complete"
                        elif next_cache_entry.part_path.exists():
                            cache_state = "partial"
                        else:
                            cache_state = "missing"
                    remaining_to_start = max(0, next_start_ts - now_ts) if next_start_ts > 0 else 0
                    logger.info(
                        "VOD channel fetching next episode ahead of playback channel=%s start_ts=%s "
                        "source_item_id=%s seconds_ahead=%s cache_state=%s upstream_url=%s",
                        self.channel_id,
                        next_start_ts,
                        int((next_playback.get("entry") or {}).get("source_item_id") or 0),
                        remaining_to_start,
                        cache_state,
                        next_upstream_url,
                    )
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

    async def _start_current_cache_download(self, candidate, upstream_url, entry, wait_for_ready=True):
        owner_key = f"vod-channel-current-{self.channel_id}-{int(entry.get('start_ts') or 0)}"
        source = await cso_source_from_vod_source(candidate, upstream_url)
        if source is None:
            return None, None
        cache_entry = await vod_cache_manager.get_or_create(source, source.url)
        warm_task = asyncio.create_task(
            warm_vod_cache(candidate, upstream_url, owner_key=owner_key),
            name=f"vod-channel-current-cache-{self.channel_id}-{int(entry.get('start_ts') or 0)}",
        )
        if wait_for_ready:
            try:
                await asyncio.wait_for(cache_entry.ready_event.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
            if not cache_entry.complete and not cache_entry.part_path.exists():
                for _ in range(5):
                    if cache_entry.part_path.exists():
                        break
                    await asyncio.sleep(0.2)
        if warm_task.done():
            try:
                await warm_task
            except Exception:
                pass
        return source, cache_entry

    async def _build_segment_runtime(self, playback, segment_index, activate_session_state=True):
        candidate = playback.get("candidate")
        upstream_url = clean_text(playback.get("upstream_url"))
        source_item = playback.get("source_item")
        entry = playback.get("entry") or {}
        offset_seconds = max(0, int(playback.get("offset_seconds") or 0))
        entry_start_ts = int(entry.get("start_ts") or 0)
        entry_stop_ts = int(entry.get("stop_ts") or 0)
        entry_duration_seconds = max(1, entry_stop_ts - entry_start_ts)
        remaining_seconds = max(1, entry_duration_seconds - offset_seconds)
        if candidate is None or not upstream_url or source_item is None:
            return None

        source, cache_entry = await self._start_current_cache_download(
            candidate,
            upstream_url,
            entry,
            wait_for_ready=offset_seconds <= 0,
        )
        input_target = source.url
        if cache_entry.complete and cache_entry.final_path.exists():
            input_target = str(cache_entry.final_path)

        segment_policy = self._segment_policy_for_runtime()
        segment_session = SegmentedHandoffSession(
            key=f"{self.key}-segment-{segment_index}",
            policy=segment_policy,
            input_target=input_target,
            input_is_url=bool(input_target.startswith("http://") or input_target.startswith("https://")),
            user_agent=resolve_cso_ingest_user_agent(self.config, source),
            request_headers=resolve_cso_ingest_headers(source),
            cache_root_dir=self.segment_cache_root,
            start_seconds=offset_seconds,
            max_duration_seconds=remaining_seconds,
        )
        started = await segment_session.start()
        if not started:
            return None
        output_probe = await segment_session.detect_output_probe()
        source_probe = dict(source.probe_details or {})
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
            await segment_session.stop(force=True)
            return None

        runtime = {
            "segment_session": segment_session,
            "entry": entry,
            "playback": playback,
            "source": source,
            "input_target": input_target,
            "pipe_container": "hls",
            "source_probe": dict(output_probe or source_probe or {}),
        }
        runtime["stitch_task"] = asyncio.create_task(
            self._append_runtime_segments(runtime, discontinuity=segment_index > 0),
            name=f"vod-channel-stitch-{self.channel_id}-{segment_index}",
        )
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
            logger.warning(
                "VOD channel could not resolve next playback during prestart channel=%s start_ts=%s",
                self.channel_id,
                next_start_ts,
            )
            return None
        if self._entry_identity((prepared_playback.get("entry") or {})) != next_identity:
            logger.warning(
                "VOD channel next playback identity changed during prestart channel=%s expected_start_ts=%s resolved_start_ts=%s",
                self.channel_id,
                next_start_ts,
                int((prepared_playback.get("entry") or {}).get("start_ts") or 0),
            )
            return None

        runtime = await self._build_segment_runtime(
            prepared_playback,
            segment_index,
            activate_session_state=False,
        )
        if runtime is None:
            logger.warning(
                "VOD channel next runtime could not be prepared channel=%s start_ts=%s source_item_id=%s",
                self.channel_id,
                next_start_ts,
                int(next_entry.get("source_item_id") or 0),
            )
            return None

        logger.info(
            "Prepared next VOD channel segment channel=%s start_ts=%s source_item_id=%s prestart_seconds=%s",
            self.channel_id,
            next_start_ts,
            int(next_entry.get("source_item_id") or 0),
            int(VOD_CHANNEL_NEXT_SEGMENT_PRESTART_SECONDS),
        )
        return runtime

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
        runtime = {
            "segment_session": self._active_ingest,
            "process": self.process,
            "warm_task": self._warm_task,
            "stderr_task": self.stderr_task,
        }
        self._active_ingest = None
        self.process = None
        self._warm_task = None
        self.stderr_task = None
        await self._close_runtime(runtime)

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
                    try:
                        await asyncio.wait_for(self._stitched_ready_event.wait(), timeout=12.0)
                        self._startup_succeeded = True
                    except asyncio.TimeoutError:
                        self.last_error = "vod_channel_segment_startup_timeout"
                        self.running = False
                    startup_event.set()
                    if not self.running:
                        break

                current_entry = runtime["entry"]
                remaining_seconds = max(0, int(current_entry.get("stop_ts") or 0) - int(time.time()))
                next_warm_task = asyncio.create_task(
                    self._warm_next_item_cache(current_playback.get("next_entry"), remaining_seconds),
                    name=f"vod-channel-next-cache-{self.channel_id}-{segment_index + 1}",
                )
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

                if not self.running:
                    if next_warm_task is not None and not next_warm_task.done():
                        next_warm_task.cancel()
                        try:
                            await next_warm_task
                        except BaseException:
                            pass
                    await self._close_runtime(await self._take_prepared_runtime(prepared_task))
                    break
                wait_timeout_seconds = 8.0 if current_playback.get("next_entry") else 0.0
                prepared_runtime = await self._take_prepared_runtime(
                    prepared_task,
                    wait_timeout_seconds=wait_timeout_seconds,
                )
                if next_warm_task is not None and not next_warm_task.done():
                    next_warm_task.cancel()
                    try:
                        await next_warm_task
                    except BaseException:
                        pass
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
                    await self._close_active_segment()
                    playback = prepared_runtime.get("playback")
                    segment_index += 1
                    continue

                next_playback = await self._wait_for_next_playback(current_entry)
                await self._close_active_segment()
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
            await self._write_stitched_playlist(endlist=True)
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
            self.canonical_output_shape = {}
        for queue in subscribers:
            await queue.put_eof()
        if self.segment_cache_root.exists():
            await asyncio.to_thread(shutil.rmtree, self.segment_cache_root, True)

    async def stop(self, force: bool = False):
        async with self.lifecycle_lock:
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
