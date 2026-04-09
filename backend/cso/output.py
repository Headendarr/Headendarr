import asyncio
import logging
import re
import shutil
import time
from collections import deque
from pathlib import Path

from backend.config import enable_cso_output_command_debug_logging
from backend.http_headers import sanitise_headers
from backend.utils import clean_key, clean_text

from .common import ByteBudgetQueue, process_is_running, wait_process_exit_with_timeout
from .constants import (
    CSO_HLS_CLIENT_IDLE_SECONDS,
    CSO_INGEST_RECOVERY_RETRY_INTERVAL_SECONDS,
    CSO_INGEST_SUBSCRIBER_PREBUFFER_BYTES,
    CSO_OUTPUT_CLIENT_QUEUE_MAX_BYTES,
    CSO_OUTPUT_CLIENT_STALE_SECONDS,
    CSO_OUTPUT_CLIENT_STALE_SECONDS_TVH,
    CSO_OUTPUT_SLATE_POLL_INTERVAL_SECONDS,
)
from .events import emit_channel_stream_event, source_event_context
from .ffmpeg import (
    CsoFfmpegCommandBuilder,
    event_source_probe,
    log_hwaccel_failure,
    start_ffmpeg_with_hw_decode_fallback,
    terminate_ffmpeg_process,
)
from .policy import (
    effective_vod_hls_runtime_policy,
    policy_ffmpeg_format,
    policy_log_label,
    resolve_cso_output_policy,
    resolve_vod_pipe_container,
)

logger = logging.getLogger("cso")

SAFE_HLS_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


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
        self.output_policy = resolve_cso_output_policy(policy, self.use_slate_as_input)
        self.ingest_session = ingest_session
        self.slate_session = slate_session
        self.event_source = event_source
        self.process = None
        self.read_task = None
        self.write_task = None
        self.ingest_recovery_task = None
        self.stderr_task = None
        self.running = False
        self.lifecycle_lock = asyncio.Lock()
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
        self._first_output_event = asyncio.Event()

    async def _cleanup_failed_start_attempt(self, process, read_task, write_task, stderr_task):
        for task in (read_task, write_task, stderr_task):
            if task is not None and not task.done():
                task.cancel()
        await terminate_ffmpeg_process(process)

    def _recent_ingest_failover_active(self) -> bool:
        if self.ingest_session is None:
            return False
        now_value = time.time()
        if bool(self.ingest_session.failover_in_progress):
            return True
        if (now_value - float(self.ingest_session.failover_start_ts or 0.0)) <= 15.0:
            return True
        if self.ingest_session.last_reader_end_reason != "ingest_reader_ended":
            return False
        if self.ingest_session.last_reader_end_return_code in (None, 0):
            return False
        return (now_value - float(self.ingest_session.last_reader_end_ts or 0.0)) <= 30.0

    def _is_failover_remux_startup_failure(self) -> bool:
        if self.use_slate_as_input or self.first_output_chunk_logged:
            return False
        if not self.first_ingest_chunk_logged:
            return False
        if not self._recent_ingest_failover_active():
            return False
        ffmpeg_error = clean_key(self._ffmpeg_error_summary())
        if not ffmpeg_error:
            return False
        return any(
            token in ffmpeg_error
            for token in (
                "could not find codec parameters",
                "could not write header",
                "incorrect codec parameters",
                "unspecified sample format",
                "0 channels",
            )
        )

    async def _wait_for_startup_ready(self, process, timeout_seconds: float = 8.0) -> tuple[bool, str]:
        self._first_output_event = asyncio.Event()
        wait_task = asyncio.create_task(process.wait())
        output_task = asyncio.create_task(self._first_output_event.wait())
        done = set()
        done, pending = await asyncio.wait(
            {wait_task, output_task},
            timeout=max(1.0, float(timeout_seconds)),
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        if output_task in done and output_task.done() and not output_task.cancelled():
            wait_task.cancel()
            return True, ""
        if wait_task in done and wait_task.done() and not wait_task.cancelled():
            return False, self._ffmpeg_error_summary() or f"ffmpeg_exit:{process.returncode}"
        if process.returncode is not None:
            return False, self._ffmpeg_error_summary() or f"ffmpeg_exit:{process.returncode}"
        if self._is_failover_remux_startup_failure():
            return False, self._ffmpeg_error_summary() or "startup_failed_during_ingest_failover"
        if self.first_ingest_chunk_logged and not self._ffmpeg_error_summary():
            if self._recent_ingest_failover_active():
                return False, "startup_timeout_during_ingest_failover"
            logger.info(
                "CSO output startup timed out before first client-visible chunk but ingest is flowing; "
                "treating the output as started channel=%s output_key=%s",
                self.channel_id,
                self.key,
            )
            return True, ""
        return False, self._ffmpeg_error_summary() or "startup_timeout_no_output"

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
        async with self.lifecycle_lock:
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
                    if self.use_slate_as_input or self.ingest_session is None:
                        pipe_input_format = "mpegts"
                        source_probe = dict(getattr(self.slate_session, "media_hint", {}) or {})
                        source_identity = clean_text(getattr(self.slate_session, "key", "")) or self.key
                    else:
                        ingest_policy = dict(self.ingest_session.ingest_policy or {})
                        if ingest_policy:
                            pipe_input_format = policy_ffmpeg_format(ingest_policy)
                        else:
                            pipe_input_format = resolve_vod_pipe_container(
                                self.ingest_session.current_source,
                                source_probe=self.ingest_session.current_source_probe,
                            )
                        source_probe = dict(self.ingest_session.current_source_probe or {})
                        source_identity = self.ingest_session.current_source_url or clean_text(
                            getattr(self.ingest_session.current_source, "url", "")
                        )

                    async def _attempt_start(effective_policy):
                        command = CsoFfmpegCommandBuilder(
                            effective_policy,
                            pipe_input_format=pipe_input_format,
                            source_probe=source_probe,
                        ).build_output_command()
                        self._recent_ffmpeg_stderr.clear()
                        logger.info(
                            "Starting CSO output channel=%s output_key=%s policy=(%s) command=%s",
                            self.channel_id,
                            self.key,
                            policy_log_label(effective_policy),
                            command,
                        )
                        process = await asyncio.create_subprocess_exec(
                            *command,
                            stdin=asyncio.subprocess.PIPE,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                        )
                        self.process = process
                        read_task = asyncio.create_task(self._read_loop())
                        write_task = asyncio.create_task(self._write_loop())
                        stderr_task = asyncio.create_task(self._stderr_loop())
                        started, failure_reason = await self._wait_for_startup_ready(process)
                        if started:
                            return True, (process, read_task, write_task, stderr_task), ""
                        logger.warning(
                            "CSO output start failed channel=%s output_key=%s reason=%s",
                            self.channel_id,
                            self.key,
                            failure_reason or "unknown",
                        )
                        await self._cleanup_failed_start_attempt(process, read_task, write_task, stderr_task)
                        self.process = None
                        return False, None, failure_reason

                    started, start_policy, result, failure_reason = await start_ffmpeg_with_hw_decode_fallback(
                        self.output_policy,
                        source_identity,
                        _attempt_start,
                    )
                    if not started:
                        log_hwaccel_failure(start_policy, f"output:{self.key}", failure_reason)
                        self.running = False
                        self.last_error = failure_reason or "output_start_failed"
                        return
                    self.output_policy = dict(start_policy)
                    self.process, self.read_task, self.write_task, self.stderr_task = result
                    self.ingest_recovery_task = asyncio.create_task(self._ingest_recovery_loop())
                except Exception as exc:
                    self.running = False
                    self.last_error = f"output_start_failed:{exc}"
                    raise
                logger.info(
                    "CSO output started channel=%s output_key=%s policy=(%s) clients=%s",
                    self.channel_id,
                    self.key,
                    policy_log_label(self.output_policy),
                    len(self.clients),
                )
                if self.ingest_recovery_task is None:
                    self.ingest_recovery_task = asyncio.create_task(self._ingest_recovery_loop())

    async def _ingest_recovery_loop(self):
        if self.ingest_session is None:
            return
        retry_interval_seconds = max(1.0, float(CSO_INGEST_RECOVERY_RETRY_INTERVAL_SECONDS))
        while self.running:
            await asyncio.sleep(retry_interval_seconds)
            if not self.running:
                return
            if self.ingest_session.running or bool(self.ingest_session.failover_in_progress):
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
                                now_value - float(self.ingest_session.failover_start_ts or now_value),
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
                            time.time() - float(self.ingest_session.failover_start_ts or time.time()),
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
                                    now_value - float(self.ingest_session.failover_start_ts or now_value),
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
                            time.time() - float(self.ingest_session.failover_start_ts or time.time()),
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
                    self._first_output_event.set()
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
                intentional_failover = bool(self.ingest_session.health_failover_reason)
                ingest_graceful_reader_end = bool(
                    self.ingest_session is not None
                    and self.ingest_session.last_reader_end_reason == "ingest_reader_ended"
                    and bool(self.ingest_session.last_reader_end_saw_data)
                    and self.ingest_session.last_reader_end_return_code == 0
                    and (time.time() - float(self.ingest_session.last_reader_end_ts or 0.0)) <= 30.0
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
                elif self._is_failover_remux_startup_failure():
                    self.last_error = "output_reader_ended"
                    logger.info(
                        "CSO output reader ended during ingest failover handover; "
                        "treating as playback-unavailable fallback channel=%s output_key=%s return_code=%s",
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
                        source=(self.ingest_session.current_source or self.event_source),
                        session_id=self.key,
                        event_type="playback_unavailable",
                        severity=severity,
                        details={
                            "reason": "output_reader_ended",
                            "return_code": return_code,
                            "ffmpeg_error": ffmpeg_error or None,
                            "policy": self.policy,
                            **source_event_context(
                                self.ingest_session.current_source or self.event_source,
                                source_url=(
                                    self.ingest_session.current_source_url
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
            policy_log_label(self.output_policy),
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
            policy_log_label(self.output_policy),
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

    async def stop(self, force: bool = False):
        async with self.lifecycle_lock:
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
                policy_log_label(self.output_policy),
            )
            return_code = None
            if process:
                try:
                    if process.stdin:
                        process.stdin.close()
                except Exception:
                    pass
                try:
                    return_code = await wait_process_exit_with_timeout(process, timeout_seconds=0.75)
                except Exception:
                    try:
                        process.terminate()
                        return_code = await wait_process_exit_with_timeout(process, timeout_seconds=2.0)
                    except Exception:
                        try:
                            process.kill()
                            return_code = await wait_process_exit_with_timeout(process, timeout_seconds=6.0)
                        except Exception:
                            if process.returncode is not None or not process_is_running(process.pid):
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
                policy_log_label(self.output_policy),
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
        input_target=None,
        input_is_url=False,
        input_user_agent=None,
        input_request_headers=None,
        start_seconds=0,
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
        self.input_target = str(input_target or "").strip()
        self.input_is_url = bool(input_is_url)
        self.input_user_agent = str(input_user_agent or "").strip()
        self.input_request_headers = sanitise_headers(input_request_headers)
        self.start_seconds = max(0, int(start_seconds or 0))
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
        self.runtime_policy = dict(policy or {})

    async def _cleanup_failed_start_attempt(self, process, write_task, stderr_task, wait_task):
        for task in (write_task, stderr_task, wait_task):
            if task is not None and not task.done():
                task.cancel()
        await terminate_ffmpeg_process(process)

    def _startup_progress_marker(self) -> float:
        marker = float(self.last_activity or 0.0)
        try:
            if self.output_dir.exists():
                for child in self.output_dir.iterdir():
                    try:
                        marker = max(marker, float(child.stat().st_mtime))
                    except Exception:
                        continue
        except Exception:
            pass
        return marker

    async def _wait_for_startup_ready(self, process, timeout_seconds: float = 8.0) -> tuple[bool, str]:
        startup_idle_timeout = max(1.0, float(timeout_seconds))
        hard_deadline = time.time() + max(30.0, startup_idle_timeout * 6.0)
        idle_deadline = time.time() + startup_idle_timeout
        last_progress_marker = self._startup_progress_marker()
        while time.time() < hard_deadline:
            if process.returncode is not None:
                return False, self._ffmpeg_error_summary() or f"ffmpeg_exit:{process.returncode}"
            playlist_text = await self.read_playlist_text()
            if playlist_text:
                return True, ""
            progress_marker = self._startup_progress_marker()
            if progress_marker > last_progress_marker:
                last_progress_marker = progress_marker
                idle_deadline = time.time() + startup_idle_timeout
            elif time.time() >= idle_deadline:
                break
            await asyncio.sleep(0.1)
        if process.returncode is not None:
            return False, self._ffmpeg_error_summary() or f"ffmpeg_exit:{process.returncode}"
        return False, self._ffmpeg_error_summary() or "startup_timeout_no_playlist"

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
            use_direct_input = bool(self.input_target)
            if self.use_slate_as_input:
                await self.slate_session.start()
                if not self.slate_session.running:
                    self.last_error = "slate_not_running"
                    return
            elif not use_direct_input:
                await self.ingest_session.start()
                if not self.ingest_session.running:
                    self.last_error = self.ingest_session.last_error or "ingest_not_running"
                    return

            await self._prepare_output_dir()
            self.ingest_queue = None
            self.slate_queue = None
            if self.use_slate_as_input:
                self.slate_queue = await self.slate_session.add_subscriber(self.key, prebuffer_bytes=256 * 1024)
            elif not use_direct_input:
                self.ingest_queue = await self.ingest_session.add_subscriber(self.key, prebuffer_bytes=256 * 1024)
            self._pending_input_chunks.clear()
            primed_bytes = 0
            if use_direct_input:
                prime_deadline = time.time()
                target_prime_bytes = 0
            elif self.use_slate_as_input:
                prime_deadline = time.time() + 2.0
                target_prime_bytes = 128 * 1024
            else:
                prime_deadline = time.time() + 2.5
                target_prime_bytes = 256 * 1024
            input_queue = self.slate_queue if self.use_slate_as_input else self.ingest_queue
            while input_queue and time.time() < prime_deadline:
                probe_has_video = (
                    True if self.use_slate_as_input else self._probe_has_video(self.ingest_session.current_source_probe)
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
                        else self._probe_has_video(self.ingest_session.current_source_probe)
                    ),
                    int((time.time() - self.last_activity) * 1000),
                )
            pipe_input_format = None
            source = self.event_source or self.ingest_session.current_source
            if not use_direct_input:
                ingest_policy = dict(self.ingest_session.ingest_policy or {})
                if ingest_policy:
                    pipe_input_format = policy_ffmpeg_format(ingest_policy)
                else:
                    pipe_input_format = resolve_vod_pipe_container(
                        self.ingest_session.current_source,
                        source_probe=self.ingest_session.current_source_probe,
                    )
                source = self.ingest_session.current_source
            source_probe = (
                event_source_probe(source) if use_direct_input else dict(self.ingest_session.current_source_probe or {})
            )
            source_identity = (
                self.input_target
                if use_direct_input
                else (self.ingest_session.current_source_url or clean_text(getattr(source, "url", "")))
            )
            base_runtime_policy = effective_vod_hls_runtime_policy(self.policy, source)
            self.running = True
            self.last_error = None
            self.last_activity = time.time()
            await self._prepare_output_dir()

            async def _attempt_start(effective_policy):
                self.runtime_policy = dict(effective_policy)
                builder = CsoFfmpegCommandBuilder(
                    self.runtime_policy,
                    pipe_input_format=pipe_input_format,
                    source_probe=source_probe,
                )
                command = builder.build_hls_output_command(
                    self.output_dir,
                    input_target=self.input_target if use_direct_input else "",
                    input_is_url=self.input_is_url,
                    start_seconds=self.start_seconds,
                    user_agent=self.input_user_agent,
                    request_headers=self.input_request_headers,
                )
                self._recent_ffmpeg_stderr.clear()
                logger.info(
                    "Starting CSO HLS output channel=%s output_key=%s policy=(%s) command=%s",
                    self.channel_id,
                    self.key,
                    policy_log_label(self.runtime_policy),
                    command,
                )
                self.process = await asyncio.create_subprocess_exec(
                    *command,
                    stdin=asyncio.subprocess.DEVNULL if use_direct_input else asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.PIPE,
                )
                self.process_token += 1
                token = self.process_token
                write_task = None
                if not use_direct_input:
                    write_task = asyncio.create_task(self._write_loop(token, self.process))
                stderr_task = asyncio.create_task(self._stderr_loop(token, self.process))
                wait_task = asyncio.create_task(self._wait_loop(token, self.process))
                startup_timeout_seconds = 8.0
                if use_direct_input:
                    startup_timeout_seconds = 20.0 if self.start_seconds > 0 else 12.0
                started, failure_reason = await self._wait_for_startup_ready(
                    self.process,
                    timeout_seconds=startup_timeout_seconds,
                )
                if started:
                    return True, (self.process, write_task, stderr_task, wait_task), ""
                logger.warning(
                    "CSO HLS output start failed channel=%s output_key=%s reason=%s",
                    self.channel_id,
                    self.key,
                    failure_reason or "unknown",
                )
                await self._cleanup_failed_start_attempt(self.process, write_task, stderr_task, wait_task)
                self.process = None
                return False, None, failure_reason

            started, start_policy, result, failure_reason = await start_ffmpeg_with_hw_decode_fallback(
                base_runtime_policy,
                source_identity,
                _attempt_start,
            )
            if started:
                self.runtime_policy = dict(start_policy)
                self.process, self.write_task, self.stderr_task, self.wait_task = result
                return
            self.runtime_policy = dict(start_policy)
            log_hwaccel_failure(self.runtime_policy, f"hls:{self.key}", failure_reason)
            self.running = False
            self.last_error = failure_reason or "output_start_failed"

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
                self.last_activity = time.time()
                if enable_cso_output_command_debug_logging:
                    logger.info("CSO HLS output ffmpeg[%s][%s]: %s", self.channel_id, self.key, rendered)
        rendered = text_buffer.strip()
        if rendered and token == self.process_token:
            self._recent_ffmpeg_stderr.append(rendered)
            self.last_activity = time.time()
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
                policy_log_label(self.runtime_policy),
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
            policy_log_label(self.runtime_policy),
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
        if not name or not SAFE_HLS_SEGMENT_RE.match(name):
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
            policy_log_label(self.runtime_policy),
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
                policy_log_label(self.runtime_policy),
            )
        if process:
            try:
                process.terminate()
                await wait_process_exit_with_timeout(process, timeout_seconds=2.0)
            except Exception:
                try:
                    process.kill()
                    await wait_process_exit_with_timeout(process, timeout_seconds=2.0)
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
