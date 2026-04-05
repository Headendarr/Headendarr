import asyncio
import logging
import time
from collections import deque
from typing import Any

from backend.utils import clean_key, clean_text

from .common import ByteBudgetQueue, wait_process_exit_with_timeout
from .constants import (
    CSO_INGEST_HISTORY_MAX_BYTES,
    CSO_INGEST_SUBSCRIBER_QUEUE_MAX_BYTES,
    MPEGTS_CHUNK_BYTES,
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
from .policy import generate_vod_channel_ingest_policy
from .sources import cso_source_from_vod_source
from .vod_cache import vod_cache_manager, warm_vod_cache


logger = logging.getLogger("cso")


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
        self.ingest_policy = generate_vod_channel_ingest_policy(config, self.output_policy)
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
                await wait_process_exit_with_timeout(process, timeout_seconds=1.0)
            except Exception:
                try:
                    process.kill()
                    await wait_process_exit_with_timeout(process, timeout_seconds=1.0)
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
        next_entry = playback.get("next_entry")
        offset_seconds = max(0, int(playback.get("offset_seconds") or 0))
        remaining_seconds = max(1, int(entry.get("stop_ts") or 0) - int(time.time()))
        if candidate is None or not upstream_url or source_item is None:
            return None

        source, cache_entry = await self._start_current_cache_download(
            candidate,
            upstream_url,
            entry,
            wait_for_ready=offset_seconds <= 0,
        )
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
        use_direct_upstream_input = bool(input_is_url and offset_seconds > 0 and source.url)

        source_probe = dict(source.probe_details or {})
        source_identity = input_target or source.url
        base_policy = dict(self.ingest_policy)
        startup_chunk = b""

        async def _attempt_start(effective_policy):
            command = CsoFfmpegCommandBuilder(
                effective_policy,
                pipe_output_format="mpegts",
                source_probe=source_probe,
            ).build_vod_channel_ingest_command(
                input_target,
                start_seconds=offset_seconds,
                max_duration_seconds=remaining_seconds,
                realtime=True,
                input_is_url=input_is_url,
                user_agent=resolve_cso_ingest_user_agent(None, source),
                request_headers=resolve_cso_ingest_headers(source),
                policy=effective_policy,
                seekable_url_input=use_direct_upstream_input,
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
                policy_log_label(effective_policy),
                redact_ingest_command_for_log(command) if input_is_url else command,
            )
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stderr_task = asyncio.create_task(
                self._read_stderr(process, entry),
                name=f"vod-channel-stderr-{self.channel_id}-{segment_index}",
            )
            startup_timeout_seconds = 20.0 if use_direct_upstream_input else 8.0
            started, startup_failure_reason, startup_chunk = await wait_for_process_output_start(
                process,
                process.stdout,
                timeout_seconds=startup_timeout_seconds,
            )
            if started:
                return True, (process, stderr_task, startup_chunk), ""
            logger.warning(
                "VOD channel ingest start failed channel=%s source_id=%s reason=%s",
                self.channel_id,
                source.id,
                startup_failure_reason or "unknown",
            )
            await self._close_runtime(
                {
                    "process": process,
                    "stderr_task": stderr_task,
                }
            )
            return False, None, startup_failure_reason

        started, start_policy, result, _failure_reason = await start_ffmpeg_with_hw_decode_fallback(
            base_policy,
            source_identity,
            _attempt_start,
        )
        if not started:
            return None
        self.ingest_policy = dict(start_policy)
        process, stderr_task, startup_chunk = result
        warm_task = asyncio.create_task(
            self._warm_next_item_cache(next_entry, remaining_seconds),
            name=f"vod-channel-next-{self.channel_id}-{segment_index}",
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
        if startup_chunk:
            runtime["startup_chunk"] = startup_chunk
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
                    startup_chunk = runtime.pop("startup_chunk", b"")
                    if startup_chunk:
                        saw_data = True
                        self.current_segment_healthy = True
                        if not self.first_healthy_stream_seen:
                            logger.info(
                                "VOD channel ingest first chunk channel=%s ingest_key=%s bytes=%s elapsed_ms=%s",
                                self.channel_id,
                                self.key,
                                len(startup_chunk),
                                int(max(0.0, time.time() - float(self.session_start_ts or time.time())) * 1000),
                            )
                            self.first_healthy_stream_seen = True
                        await self._broadcast(startup_chunk)
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
