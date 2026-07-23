import asyncio
import logging
import re
import time
import uuid
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend.config import enable_cso_output_command_debug_logging
from backend.http_headers import sanitise_headers
from backend.utils import clean_key, clean_text

from .common import (
    ByteBudgetQueue,
    bounded_log_value,
    prepare_cso_cache_dir,
    remove_cso_cache_dir,
)
from .capacity import cso_capacity_registry
from .vod_cache import vod_cache_manager
from .constants import (
    CSO_HLS_CLIENT_IDLE_SECONDS,
    CSO_INGEST_RECOVERY_RETRY_INTERVAL_SECONDS,
    CSO_INGEST_SUBSCRIBER_PREBUFFER_BYTES,
    CSO_OUTPUT_CLIENT_QUEUE_MAX_BYTES,
    CSO_OUTPUT_CLIENT_STALE_SECONDS,
    CSO_OUTPUT_CLIENT_STALE_SECONDS_TVH,
    CSO_OUTPUT_FPS_PROBE_SIZE,
    CSO_OUTPUT_SLATE_POLL_INTERVAL_SECONDS,
)
from .events import emit_channel_stream_event, source_event_context
from .ffmpeg import (
    CsoFfmpegCommandBuilder,
    event_source_probe,
    ffmpeg_failure_classification,
    hwaccel_failure_stage,
    log_ffmpeg_start_result_failures,
    redact_ffmpeg_command_for_log,
    redact_ffmpeg_error_for_log,
    startup_failure_diagnostics,
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
from .processes import (
    CsoLifecycleCleanupResult,
    cancel_and_await_tasks,
    cso_lifecycle_cleanup_result,
    mark_cso_ffmpeg_process_exited,
    spawn_cso_ffmpeg_process,
)
from .types import (
    CsoFfmpegAttemptResult,
    CsoFfmpegStartResult,
    CsoStartupEvidence,
)


logger = logging.getLogger("cso")

SAFE_HLS_SEGMENT_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


@dataclass
class CsoOutputStartupProbeResult:
    started: bool
    classification: str
    failure_reason: str
    stderr_summary: str


@dataclass(frozen=True)
class CsoHlsClientStartResult:
    running: bool
    output_started: bool
    output_reused: bool
    client_added: bool
    existing_client: bool
    input_kind: str
    capacity_key: str | None
    capacity_limit: int
    reservation_owner: str | None
    reservation_slot_id: str | None
    failure_reason: str | None


async def _detach_output_input_subscriptions(session):
    ingest_queue = session.ingest_queue
    ingest_lifecycle_reference = session.ingest_lifecycle_reference
    slate_queue = session.slate_queue

    if ingest_queue is not None:
        try:
            if session.ingest_session is not None:
                await session.ingest_session.remove_subscriber(session.key)
            else:
                await ingest_queue.close()
        except Exception:
            await ingest_queue.close()
            logger.exception(
                "CSO output failed to detach ingest subscriber channel=%s output_key=%s",
                session.channel_id,
                session.key,
            )
        else:
            session.ingest_queue = None
    if ingest_lifecycle_reference and session.ingest_session is not None:
        try:
            await session.ingest_session.remove_lifecycle_reference(session.key)
        except Exception:
            logger.exception(
                "CSO output failed to detach ingest lifecycle reference channel=%s output_key=%s",
                session.channel_id,
                session.key,
            )
        else:
            session.ingest_lifecycle_reference = False
    if slate_queue is not None:
        await slate_queue.close()
        if session.slate_session is not None:
            try:
                await session.slate_session.remove_subscriber(session.key)
            except Exception:
                logger.exception(
                    "CSO output failed to detach slate subscriber channel=%s output_key=%s",
                    session.channel_id,
                    session.key,
                )
            else:
                session.slate_queue = None
        else:
            session.slate_queue = None


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
        direct_input_realtime=False,
    ):
        self.key = key
        self.channel_id = channel_id
        self.policy = policy
        self.use_slate_as_input = bool(use_slate_as_input)
        self.direct_input_realtime = bool(direct_input_realtime)
        self.output_policy = resolve_cso_output_policy(policy, self.use_slate_as_input)
        self.ingest_session = ingest_session
        self.slate_session = slate_session
        self.event_source = event_source
        self.process = None
        self.read_task = None
        self.write_task = None
        self.ingest_recovery_task = None
        self.stderr_task = None
        self.startup_tasks = ()
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
        self.ingest_lifecycle_reference = False
        self.slate_queue = None
        self._recent_ffmpeg_stderr = deque(maxlen=30)
        self.client_drop_state = {}
        self.client_last_touch = {}
        self._input_mode = "slate" if self.use_slate_as_input else "ingest"
        self.start_ts = 0.0
        self.attempt_start_ts = 0.0
        self.first_ingest_chunk_ts = 0.0
        self.input_bytes_received = 0
        self.startup_process_pid = None
        self.first_output_chunk_logged = False
        self.first_ingest_chunk_logged = False
        self._last_ingest_recovery_attempt_ts = 0.0
        self._pending_input_chunks = deque()
        self._first_output_event = asyncio.Event()

    def _segmented_input_target(self) -> str:
        if self.ingest_session is None:
            return ""
        getter = getattr(self.ingest_session, "get_output_input_target", None)
        if not callable(getter):
            return ""
        try:
            return clean_text(getter())
        except Exception:
            return ""

    def _uses_direct_ingest_input(self) -> bool:
        return bool(self._segmented_input_target())

    async def _cleanup_failed_start_attempt(self, process, read_task, write_task, stderr_task):
        teardown = await terminate_ffmpeg_process(process)
        startup_tasks = self.startup_tasks
        self.startup_tasks = ()
        task_cleanup = await cancel_and_await_tasks((read_task, write_task, stderr_task, *startup_tasks))
        if not task_cleanup.confirmed:
            pending_tasks = set(task_cleanup.pending_tasks)
            self.read_task = read_task if read_task in pending_tasks else None
            self.write_task = write_task if write_task in pending_tasks else None
            self.stderr_task = stderr_task if stderr_task in pending_tasks else None
            self.startup_tasks = tuple(task for task in startup_tasks if task in pending_tasks)
        return teardown, task_cleanup

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

    def _classify_startup_failure(
        self,
        process: asyncio.subprocess.Process,
        failure_reason: str,
    ) -> tuple[str, str]:
        reason = clean_text(failure_reason)
        reason_lower = reason.lower()
        input_mode = "direct_url" if self._uses_direct_ingest_input() else self._input_mode
        classification = ffmpeg_failure_classification(reason)
        if classification == "first_output_timeout" and process.returncode is not None:
            classification = "process_exit"
        elif classification == "first_output_timeout" and input_mode == "ingest" and self.input_bytes_received <= 0:
            classification = "no_ingest_bytes"
        if reason and reason_lower != classification and not reason_lower.startswith(f"{classification}:"):
            return classification, f"{classification}:{reason}"
        return classification, reason or classification

    async def _wait_for_startup_ready(
        self,
        process: asyncio.subprocess.Process,
        stderr_task: asyncio.Task,
        timeout_seconds: float = 8.0,
    ) -> CsoOutputStartupProbeResult:
        self._first_output_event = asyncio.Event()
        wait_task = asyncio.create_task(process.wait())
        output_task = asyncio.create_task(self._first_output_event.wait())
        started = False
        failure_reason = ""
        try:
            done, _ = await asyncio.wait(
                {wait_task, output_task},
                timeout=max(1.0, float(timeout_seconds)),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if output_task in done and output_task.done() and not output_task.cancelled():
                started = True
            elif wait_task in done and wait_task.done() and not wait_task.cancelled():
                await asyncio.wait({stderr_task}, timeout=0.25)
                failure_reason = self._ffmpeg_error_summary() or f"ffmpeg_exit:{process.returncode}"
            elif process.returncode is not None:
                await asyncio.wait({stderr_task}, timeout=0.25)
                failure_reason = self._ffmpeg_error_summary() or f"ffmpeg_exit:{process.returncode}"
            elif self._is_failover_remux_startup_failure():
                failure_reason = self._ffmpeg_error_summary() or "startup_failed_during_ingest_failover"
            elif self.first_ingest_chunk_logged and not self._ffmpeg_error_summary():
                if self._recent_ingest_failover_active():
                    failure_reason = "startup_timeout_during_ingest_failover"
                else:
                    failure_reason = "first_output_timeout"
            else:
                failure_reason = self._ffmpeg_error_summary() or "startup_timeout_no_output"
        finally:
            task_cleanup = await cancel_and_await_tasks((wait_task, output_task))
            self.startup_tasks = task_cleanup.pending_tasks
        stderr_summary = self._ffmpeg_error_summary()
        if not task_cleanup.confirmed:
            failure_reason = f"{failure_reason or 'startup_probe_completed'}:task_cleanup_unconfirmed"
            classification, failure_reason = self._classify_startup_failure(process, failure_reason)
            return CsoOutputStartupProbeResult(False, classification, failure_reason, stderr_summary)
        if started:
            return CsoOutputStartupProbeResult(True, "", "", stderr_summary)
        classification, failure_reason = self._classify_startup_failure(process, failure_reason)
        return CsoOutputStartupProbeResult(False, classification, failure_reason, stderr_summary)

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

    def _record_input_chunk(self, chunk_mode: str, chunk: bytes):
        chunk_size = len(chunk or b"")
        if chunk_size <= 0:
            return
        self.input_bytes_received += chunk_size
        if chunk_mode == "ingest" and not self.first_ingest_chunk_logged:
            now_value = time.time()
            self.first_ingest_chunk_ts = now_value
            logger.info(
                "CSO output first ingest chunk channel=%s output_key=%s bytes=%s "
                "input_bytes=%s elapsed_ms=%s attempt_elapsed_ms=%s failover_elapsed_ms=%s",
                self.channel_id,
                self.key,
                chunk_size,
                self.input_bytes_received,
                int(max(0.0, now_value - float(self.start_ts or now_value)) * 1000),
                int(max(0.0, now_value - float(self.attempt_start_ts or now_value)) * 1000),
                int(
                    max(
                        0.0,
                        now_value - float(self.ingest_session.failover_start_ts or now_value),
                    )
                    * 1000
                ),
            )
            self.first_ingest_chunk_logged = True

    def _build_startup_evidence(
        self,
        process: asyncio.subprocess.Process,
        input_mode: str,
    ) -> CsoStartupEvidence:
        now_value = time.time()
        evidence = CsoStartupEvidence(
            first_ingest_chunk=self.first_ingest_chunk_ts or None,
            input_bytes=self.input_bytes_received,
            input_mode=input_mode,
            output_process_pid=process.pid,
            attempt_elapsed_ms=int(max(0.0, now_value - self.attempt_start_ts) * 1000),
        )
        if self.ingest_session is None:
            return evidence
        ingest_process = self.ingest_session.process
        evidence.ingest_process_pid = ingest_process.pid if ingest_process is not None else None
        evidence.ingest_running = self.ingest_session.running
        if self.ingest_session.last_chunk_ts:
            evidence.ingest_last_chunk_age_seconds = round(
                max(0.0, now_value - self.ingest_session.last_chunk_ts),
                3,
            )
        evidence.ingest_attempt_start = self.ingest_session.current_attempt_start_ts or None
        evidence.ingest_reader_end_reason = self.ingest_session.last_reader_end_reason
        evidence.ingest_reader_end_return_code = self.ingest_session.last_reader_end_return_code
        evidence.ingest_bytes_produced = self.ingest_session.current_attempt_bytes_produced
        evidence.ingest_bytes_added_to_history = self.ingest_session.current_attempt_bytes_added_to_history
        evidence.ingest_bytes_dispatched = self.ingest_session.current_attempt_bytes_dispatched
        evidence.ingest_subscriber_bytes_dispatched = (
            self.ingest_session.current_attempt_subscriber_bytes_dispatched.get(self.key, 0)
        )
        return evidence

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
        if self._segmented_input_target():
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
                retained_tasks = (
                    self.read_task,
                    self.write_task,
                    self.ingest_recovery_task,
                    self.stderr_task,
                    *self.startup_tasks,
                )
                if self.process is not None or any(task is not None for task in retained_tasks):
                    self.last_error = (
                        "previous_process_teardown_unconfirmed"
                        if self.process is not None
                        else "previous_task_cleanup_unconfirmed"
                    )
                    logger.error(
                        "CSO output start blocked by retained lifecycle output_key=%s pid=%s tasks=%s",
                        self.key,
                        getattr(self.process, "pid", None),
                        sum(task is not None for task in retained_tasks),
                    )
                    return
                if self.ingest_session is None and self.slate_session is None:
                    self.last_error = "no_input_session"
                    return
                self.start_ts = time.time()
                self.attempt_start_ts = 0.0
                self.first_ingest_chunk_ts = 0.0
                self.input_bytes_received = 0
                self.startup_process_pid = None
                self.first_output_chunk_logged = False
                self.first_ingest_chunk_logged = False
                self._input_mode = "slate" if self.use_slate_as_input else "ingest"
                segmented_input_target = "" if self.use_slate_as_input else self._segmented_input_target()
                try:
                    if self.ingest_session is not None:
                        await self.ingest_session.start()
                        if segmented_input_target:
                            await self.ingest_session.add_lifecycle_reference(self.key)
                            self.ingest_lifecycle_reference = True
                        else:
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
                            "CSO output primed slate input channel=%s output_key=%s "
                            "primed_bytes=%s pending_chunks=%s elapsed_ms=%s",
                            self.channel_id,
                            self.key,
                            primed_bytes,
                            len(self._pending_input_chunks),
                            int(max(0.0, time.time() - float(self.start_ts or time.time())) * 1000),
                        )
                except asyncio.CancelledError:
                    self.last_error = "output_start_cancelled"
                    await _detach_output_input_subscriptions(self)
                    raise
                except Exception as exc:
                    self.last_error = f"output_start_failed:{exc}"
                    await _detach_output_input_subscriptions(self)
                    raise
                self.running = True
                try:
                    if self.use_slate_as_input or self.ingest_session is None:
                        pipe_input_format = "mpegts"
                        source_probe = dict(getattr(self.slate_session, "media_hint", {}) or {})
                        source_identity = clean_text(getattr(self.slate_session, "key", "")) or self.key
                    else:
                        ingest_policy = dict(self.ingest_session.ingest_policy or {})
                        if segmented_input_target:
                            pipe_input_format = ""
                        elif ingest_policy:
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

                    async def _attempt_start(
                        effective_policy: dict[str, Any],
                    ) -> CsoFfmpegAttemptResult:
                        self.attempt_start_ts = time.time()
                        self.first_ingest_chunk_ts = 0.0
                        self.input_bytes_received = 0
                        self.first_ingest_chunk_logged = False
                        command = CsoFfmpegCommandBuilder(
                            effective_policy,
                            pipe_input_format=pipe_input_format,
                            source_probe=source_probe,
                        ).build_output_command(
                            input_target=segmented_input_target,
                            input_is_url=False,
                            realtime=self.direct_input_realtime and bool(segmented_input_target),
                        )
                        self._recent_ffmpeg_stderr.clear()
                        logger.info(
                            "Starting CSO output channel=%s output_key=%s policy=(%s) command=%s",
                            self.channel_id,
                            bounded_log_value(self.key),
                            policy_log_label(effective_policy),
                            redact_ffmpeg_command_for_log(command),
                        )
                        process = await spawn_cso_ffmpeg_process(
                            *command,
                            label=f"output:{self.key}",
                            stdin=asyncio.subprocess.DEVNULL if segmented_input_target else asyncio.subprocess.PIPE,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                        )
                        self.process = process
                        self.startup_process_pid = process.pid
                        read_task = asyncio.create_task(self._read_loop())
                        write_task = None if segmented_input_target else asyncio.create_task(self._write_loop())
                        stderr_task = asyncio.create_task(self._stderr_loop(process))
                        startup_timeout_seconds = 20.0 if segmented_input_target else 8.0
                        try:
                            probe_result = await self._wait_for_startup_ready(
                                process,
                                stderr_task,
                                timeout_seconds=startup_timeout_seconds,
                            )
                        except asyncio.CancelledError:
                            teardown, task_cleanup = await self._cleanup_failed_start_attempt(
                                process,
                                read_task,
                                write_task,
                                stderr_task,
                            )
                            if teardown.confirmed:
                                self.process = None
                            raise
                        input_mode = "direct_url" if segmented_input_target else self._input_mode
                        evidence = self._build_startup_evidence(process, input_mode)
                        hardware_stage = hwaccel_failure_stage(probe_result.failure_reason)
                        if probe_result.started:
                            return CsoFfmpegAttemptResult(
                                dict(effective_policy),
                                True,
                                (process, read_task, write_task, stderr_task),
                                "",
                                "",
                                probe_result.stderr_summary,
                                "",
                                evidence,
                            )
                        logger.warning(
                            "CSO output start failed channel=%s output_key=%s reason=%s",
                            self.channel_id,
                            self.key,
                            probe_result.failure_reason or "unknown",
                        )
                        teardown, task_cleanup = await self._cleanup_failed_start_attempt(
                            process,
                            read_task,
                            write_task,
                            stderr_task,
                        )
                        if teardown.confirmed:
                            self.process = None
                        failure_reason = probe_result.failure_reason
                        if not teardown.confirmed:
                            failure_reason = f"{failure_reason or 'output_start_failed'}:teardown_unconfirmed"
                        elif not task_cleanup.confirmed:
                            failure_reason = f"{failure_reason or 'output_start_failed'}:task_cleanup_unconfirmed"
                        return CsoFfmpegAttemptResult(
                            dict(effective_policy),
                            False,
                            None,
                            probe_result.classification,
                            failure_reason,
                            probe_result.stderr_summary,
                            hardware_stage,
                            evidence,
                        )

                    start_result = await start_ffmpeg_with_hw_decode_fallback(
                        self.output_policy,
                        source_identity,
                        _attempt_start,
                    )
                    if not start_result.success:
                        log_ffmpeg_start_result_failures(
                            f"output:{self.key}:channel={self.channel_id}",
                            start_result,
                        )
                        self.running = False
                        self.last_error = start_result.failure_reason or "output_start_failed"
                        await _detach_output_input_subscriptions(self)
                        return
                    hardware_failures = start_result.hardware_failures()
                    if hardware_failures:
                        logger.warning(
                            "CSO output hardware fallback recovered startup channel=%s output_key=%s "
                            "reason=%s fallback_policy=%s diagnostics=%s",
                            self.channel_id,
                            bounded_log_value(self.key),
                            redact_ffmpeg_error_for_log(hardware_failures[-1].failure_reason),
                            redact_ffmpeg_error_for_log(start_result.fallback_policy, max_length=128),
                            startup_failure_diagnostics(hardware_failures[-1], start_result),
                        )
                    self.output_policy = dict(start_result.policy)
                    self.process, self.read_task, self.write_task, self.stderr_task = start_result.runtime
                    self.ingest_recovery_task = asyncio.create_task(self._ingest_recovery_loop())
                except asyncio.CancelledError:
                    self.running = False
                    self.last_error = "output_start_cancelled"
                    await _detach_output_input_subscriptions(self)
                    raise
                except Exception as exc:
                    self.running = False
                    self.last_error = f"output_start_failed:{exc}"
                    await _detach_output_input_subscriptions(self)
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
        if self._uses_direct_ingest_input():
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

    async def _stderr_loop(self, process: asyncio.subprocess.Process):
        if process.stderr is None:
            return
        text_buffer = ""
        while True:
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
                    self._record_input_chunk(chunk_mode, chunk)
                    await self.touch_all_clients()
                except Exception:
                    return
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
                    self._record_input_chunk(chunk_mode, chunk)
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
        process = self.process
        try:
            while self.running and process and process.stdout:
                chunk = await process.stdout.read(16384)
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
            current_task = asyncio.current_task()
            if current_task is None or not current_task.cancelling():
                await self._handle_reader_exit(process)

    async def _handle_reader_exit(self, process):
        return_code = process.returncode if process is not None else None
        if process is not None:
            mark_cso_ffmpeg_process_exited(process)

        async with self.lock:
            client_count = len(self.clients)
            still_running = bool(self.running)

        if still_running and client_count > 0:
            if self._uses_direct_ingest_input():
                self.last_error = "output_reader_ended"
                await self.stop(force=True)
                return
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

    def _stale_seconds_for_connection(self, connection_id):
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
                bounded_log_value(connection_id),
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
            bounded_log_value(connection_id),
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
                bounded_log_value(connection_id),
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
            bounded_log_value(connection_id),
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
                bounded_log_value(connection_id),
                float(min_elapsed_seconds),
                int(min_drop_count),
            )
            await self.remove_client(connection_id)
            removed += 1
        return removed

    async def stop(self, force: bool = False):
        async with self.lifecycle_lock:
            async with self.lock:
                if (
                    not self.running
                    and not self.process
                    and not self.clients
                    and self.ingest_queue is None
                    and not self.ingest_lifecycle_reference
                    and self.slate_queue is None
                    and self.read_task is None
                    and self.write_task is None
                    and self.ingest_recovery_task is None
                    and self.stderr_task is None
                    and not self.startup_tasks
                ):
                    return
                if not force and self.clients:
                    return
                self.running = False
                process = self.process
                read_task = self.read_task
                self.read_task = None
                write_task = self.write_task
                self.write_task = None
                ingest_recovery_task = self.ingest_recovery_task
                self.ingest_recovery_task = None
                stderr_task = self.stderr_task
                self.stderr_task = None
                startup_tasks = self.startup_tasks
                self.startup_tasks = ()
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
            teardown = None
            if process:
                teardown = await terminate_ffmpeg_process(
                    process,
                    terminate_timeout_seconds=2.0,
                    kill_timeout_seconds=6.0,
                )
                return_code = teardown.return_code
            await _detach_output_input_subscriptions(self)
            task_cleanup = await cancel_and_await_tasks(
                (read_task, write_task, ingest_recovery_task, stderr_task, *startup_tasks)
            )
            if not task_cleanup.confirmed:
                pending_tasks = set(task_cleanup.pending_tasks)
                async with self.lock:
                    self.read_task = read_task if read_task in pending_tasks else None
                    self.write_task = write_task if write_task in pending_tasks else None
                    self.ingest_recovery_task = ingest_recovery_task if ingest_recovery_task in pending_tasks else None
                    self.stderr_task = stderr_task if stderr_task in pending_tasks else None
                    self.startup_tasks = tuple(task for task in startup_tasks if task in pending_tasks)
            if process is not None and teardown is not None and teardown.confirmed:
                async with self.lock:
                    if self.process is process:
                        self.process = None
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
            return cso_lifecycle_cleanup_result(teardown, task_cleanup)


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
        finite_event_output: bool = False,
        capacity_key: str | None = None,
        capacity_owner_key: str | None = None,
        capacity_limit: int = 0,
        capacity_slot_id: int | str | None = None,
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
        self.finite_event_output = bool(finite_event_output)
        self.capacity_key = clean_text(capacity_key)
        self.capacity_owner_key = clean_text(capacity_owner_key)
        self.capacity_limit = max(0, int(capacity_limit or 0))
        self.capacity_slot_id = clean_text(capacity_slot_id)
        self.capacity_required = bool(
            self.input_is_url
            and self.capacity_key
            and self.capacity_owner_key
            and self.capacity_slot_id
            and self.capacity_limit > 0
        )
        self._capacity_reserved = False
        self._capacity_lock = asyncio.Lock()
        self.process = None
        self.write_task = None
        self.stderr_task = None
        self.wait_task = None
        self.running = False
        self.lifecycle_lock = asyncio.Lock()
        self.lock = asyncio.Lock()
        self.last_activity = time.time()
        self.last_error = None
        self.ingest_queue = None
        self.ingest_lifecycle_reference = False
        self.slate_queue = None
        self._recent_ffmpeg_stderr = deque(maxlen=30)
        self.clients = {}
        self._pending_input_chunks = deque()
        self.process_token = 0
        self._last_good_playlist_text = None
        self._last_good_playlist_ts = 0.0
        self.playlist_generation = uuid.uuid4().hex
        self._retain_completed_output_until = 0.0
        self.completed = False
        self.completion_state = "idle"
        self._active_readers = 0
        self._active_readers_by_connection: dict[str, int] = {}
        self._accepting_readers = False
        self._readers_drained = asyncio.Event()
        self._readers_drained.set()
        self._reader_state_changed = asyncio.Event()
        self.runtime_policy = dict(policy or {})
        self._idle_cleanup_task = None

    async def _ensure_capacity_reservation(self) -> bool:
        if not self.capacity_required:
            return True
        async with self._capacity_lock:
            if self._capacity_reserved:
                return True
            usage_before = await cso_capacity_registry.get_usage(self.capacity_key)
            reserved = await vod_cache_manager.reserve_capacity(
                self.capacity_key,
                self.capacity_limit,
                self.capacity_owner_key,
                self.capacity_slot_id,
                purpose="interactive_playback",
            )
            usage_after = await cso_capacity_registry.get_usage(self.capacity_key)
            if reserved:
                self._capacity_reserved = True
            logger.info(
                "CSO VOD HLS capacity reservation output_key=%s capacity_key=%s "
                "reservation_owner=%s slot_id=%s limit=%s observed_usage=%s resulting_usage=%s "
                "reserved=%s input=upstream",
                bounded_log_value(self.key),
                bounded_log_value(self.capacity_key),
                bounded_log_value(self.capacity_owner_key),
                bounded_log_value(self.capacity_slot_id),
                self.capacity_limit,
                int(usage_before.get("total") or 0),
                int(usage_after.get("total") or 0),
                reserved,
            )
            return reserved

    async def _release_capacity_reservation(self, reason: str):
        if not self.capacity_required:
            return
        async with self._capacity_lock:
            if not self._capacity_reserved:
                return
            await cso_capacity_registry.release(
                self.capacity_key,
                self.capacity_owner_key,
                slot_id=self.capacity_slot_id,
            )
            self._capacity_reserved = False
            usage = await cso_capacity_registry.get_usage(self.capacity_key)
            logger.info(
                "CSO VOD HLS capacity released output_key=%s capacity_key=%s "
                "reservation_owner=%s slot_id=%s limit=%s observed_usage=%s reason=%s",
                bounded_log_value(self.key),
                bounded_log_value(self.capacity_key),
                bounded_log_value(self.capacity_owner_key),
                bounded_log_value(self.capacity_slot_id),
                self.capacity_limit,
                int(usage.get("total") or 0),
                bounded_log_value(clean_text(reason) or "unspecified"),
            )

    async def _release_capacity_if_upstream_gone(self, reason: str):
        upstream_gone = bool(
            self.process is None and self.write_task is None and self.stderr_task is None and self.wait_task is None
        )
        if upstream_gone:
            await self._release_capacity_reservation(reason)

    async def vod_hls_reuse_snapshot(self) -> dict[str, object]:
        async with self.lock:
            process = self.process
            process_active = bool(process is not None and getattr(process, "returncode", None) is None)
            lifecycle_clean = bool(
                self.completion_state
                not in {
                    "stopped",
                    "stopping",
                    "completion_invalid",
                    "teardown_unconfirmed",
                    "restart_pending",
                }
                and not clean_text(self.last_error)
            )
            completed_healthy = bool(
                self.completed
                and self.running
                and self._accepting_readers
                and self._last_good_playlist_text
                and self._retain_completed_output_until > time.time()
            )
            starting_or_active = bool(
                self.running
                and self._accepting_readers
                and self.completion_state == "starting"
                and (process_active or self.lifecycle_lock.locked())
            )
            reservation_safe = bool(not self.capacity_required or self._capacity_reserved)
            reusable = bool(lifecycle_clean and reservation_safe and (completed_healthy or starting_or_active))
            return {
                "reusable": reusable,
                "completion_state": self.completion_state,
                "completed": self.completed,
                "client_count": len(self.clients),
                "reservation_owner": self.capacity_owner_key if self._capacity_reserved else None,
                "reservation_slot_id": self.capacity_slot_id if self._capacity_reserved else None,
                "input_is_upstream": self.input_is_url,
            }

    def _segmented_input_target(self) -> str:
        if self.ingest_session is None:
            return ""
        getter = getattr(self.ingest_session, "get_output_input_target", None)
        if not callable(getter):
            return ""
        try:
            return clean_text(getter())
        except Exception:
            return ""

    async def _cleanup_failed_start_attempt(self, process, write_task, stderr_task, wait_task):
        teardown = await terminate_ffmpeg_process(process)
        task_cleanup = await cancel_and_await_tasks((write_task, stderr_task, wait_task))
        if not task_cleanup.confirmed:
            pending_tasks = set(task_cleanup.pending_tasks)
            self.write_task = write_task if write_task in pending_tasks else None
            self.stderr_task = stderr_task if stderr_task in pending_tasks else None
            self.wait_task = wait_task if wait_task in pending_tasks else None
        return teardown, task_cleanup

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
            # Startup already owns self.lock. Reading through read_playlist_text()
            # would try to acquire that lock again and leave the request, FFmpeg,
            # and idle cleanup waiting indefinitely.
            playlist_text = await self._read_valid_playlist_from_disk()
            if playlist_text:
                self._last_good_playlist_text = playlist_text
                self._last_good_playlist_ts = time.time()
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

    def _redacted_ffmpeg_error(self, value: object) -> str:
        sensitive_values = (
            self.input_target if self.input_is_url else "",
            self.input_user_agent,
            *self.input_request_headers.values(),
        )
        return redact_ffmpeg_error_for_log(
            value,
            sensitive_values=sensitive_values,
        )

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
        return self._redacted_ffmpeg_error(" | ".join(selected))

    def _output_mode(self) -> str:
        if self.use_slate_as_input:
            return "slate"
        if self.finite_event_output:
            return "finite"
        return "live"

    async def _acquire_reader(self, connection_id: str | None = None) -> bool:
        async with self.lock:
            if not self._accepting_readers:
                return False
            connection_key = str(connection_id) if connection_id is not None else None
            if connection_key is not None and connection_key not in self.clients:
                return False
            self._active_readers += 1
            if connection_key is not None:
                self._active_readers_by_connection[connection_key] = (
                    self._active_readers_by_connection.get(connection_key, 0) + 1
                )
            self._readers_drained.clear()
            return True

    async def _release_reader(self, connection_id: str | None = None):
        async with self.lock:
            connection_key = str(connection_id) if connection_id is not None else None
            if connection_key is not None:
                connection_readers = self._active_readers_by_connection.get(connection_key, 0)
                if connection_readers <= 1:
                    self._active_readers_by_connection.pop(connection_key, None)
                else:
                    self._active_readers_by_connection[connection_key] = connection_readers - 1
            self._active_readers = max(0, self._active_readers - 1)
            if self._active_readers == 0:
                self._readers_drained.set()
            reader_state_changed = self._reader_state_changed
            self._reader_state_changed = asyncio.Event()
            reader_state_changed.set()

    def _refresh_client_activity_locked(self, connection_id: str, now_value: float) -> bool:
        key = str(connection_id)
        entry = self.clients.get(key)
        if not isinstance(entry, dict):
            if self.finite_event_output and not self._accepting_readers:
                return False
            entry = {"last_touch": now_value, "on_disconnect": None}
            self.clients[key] = entry
        entry["last_touch"] = now_value
        self.last_activity = now_value
        if self.finite_event_output and self.completed:
            self._retain_completed_output_until = now_value + self._client_idle_seconds()
        return True

    async def _record_successful_read(self, connection_id: str | None = None):
        now_value = time.time()
        async with self.lock:
            if connection_id is None:
                self.last_activity = now_value
                if self.finite_event_output and self.completed:
                    self._retain_completed_output_until = now_value + self._client_idle_seconds()
            else:
                entry = self.clients.get(str(connection_id))
                if isinstance(entry, dict):
                    self._refresh_client_activity_locked(str(connection_id), now_value)
        self._schedule_idle_cleanup()

    def _client_idle_seconds(self) -> float:
        if self.finite_event_output:
            return max(5.0, float(CSO_HLS_CLIENT_IDLE_SECONDS) / 2.0)
        return float(CSO_HLS_CLIENT_IDLE_SECONDS)

    def _schedule_idle_cleanup(self):
        if not self.finite_event_output:
            return
        existing_task = self._idle_cleanup_task
        if existing_task is not None and not existing_task.done():
            return
        self._idle_cleanup_task = asyncio.create_task(self._idle_cleanup_loop())

    async def _idle_cleanup_loop(self):
        try:
            while True:
                async with self.lock:
                    if not self.clients:
                        return
                    idle_seconds = self._client_idle_seconds()
                    next_deadline = min(
                        float(entry.get("last_touch") or 0.0) + idle_seconds
                        if isinstance(entry, dict)
                        else idle_seconds
                        for entry in self.clients.values()
                    )
                delay_seconds = max(0.0, next_deadline - time.time())
                if delay_seconds > 0:
                    await asyncio.sleep(delay_seconds)
                await self.prune_idle_clients()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.warning(
                "CSO HLS idle cleanup failed channel=%s output_key=%s error=%s",
                self.channel_id,
                self.key,
                exc,
            )
        finally:
            if self._idle_cleanup_task is asyncio.current_task():
                self._idle_cleanup_task = None

    async def _prepare_output_dir(self):
        await prepare_cso_cache_dir(self.output_dir, logger, f"hls-output:{self.key}")
        self._last_good_playlist_text = None
        self._last_good_playlist_ts = 0.0
        self.playlist_generation = uuid.uuid4().hex
        self._retain_completed_output_until = 0.0
        self.completed = False
        self.completion_state = "starting"
        self._accepting_readers = True

    async def start(self):
        async with self.lifecycle_lock:
            await self._start_with_lifecycle_locked()

    async def start_and_add_client(
        self,
        connection_id: str,
        on_disconnect: Any = None,
    ) -> CsoHlsClientStartResult:
        async with self.lifecycle_lock:
            async with self.lock:
                was_running = bool(self.running)
                existing_client = str(connection_id) in self.clients
            if not await self._start_with_lifecycle_locked():
                return CsoHlsClientStartResult(
                    running=False,
                    output_started=False,
                    output_reused=False,
                    client_added=False,
                    existing_client=existing_client,
                    input_kind="upstream" if self.input_is_url else "completed_local_cache",
                    capacity_key=self.capacity_key or None,
                    capacity_limit=self.capacity_limit,
                    reservation_owner=self.capacity_owner_key if self._capacity_reserved else None,
                    reservation_slot_id=self.capacity_slot_id if self._capacity_reserved else None,
                    failure_reason=self.last_error or "output_not_running",
                )
            is_new_client = await self._add_client(connection_id, on_disconnect=on_disconnect)
            result = CsoHlsClientStartResult(
                running=True,
                output_started=not was_running,
                output_reused=was_running,
                client_added=is_new_client,
                existing_client=existing_client,
                input_kind="upstream" if self.input_is_url else "completed_local_cache",
                capacity_key=self.capacity_key or None,
                capacity_limit=self.capacity_limit,
                reservation_owner=self.capacity_owner_key if self._capacity_reserved else None,
                reservation_slot_id=self.capacity_slot_id if self._capacity_reserved else None,
                failure_reason=None,
            )
        self._schedule_idle_cleanup()
        return result

    async def _start_with_lifecycle_locked(self) -> bool:
        try:
            await self._start_locked()
        except asyncio.CancelledError:
            self.running = False
            self.last_error = "output_start_cancelled"
            await _detach_output_input_subscriptions(self)
            await self._release_capacity_if_upstream_gone("startup_cancelled")
            raise
        except Exception as exc:
            self.running = False
            self.last_error = f"output_start_failed:{exc}"
            await _detach_output_input_subscriptions(self)
            await self._release_capacity_if_upstream_gone("startup_exception")
            raise
        if self.running:
            return True
        await _detach_output_input_subscriptions(self)
        return False

    async def _start_locked(self):
        async with self.lock:
            if self.running:
                logger.info(
                    "Reusing CSO HLS output output_key=%s mode=%s completion_state=%s clients=%s readers=%s retention_deadline=%s",
                    bounded_log_value(self.key),
                    self._output_mode(),
                    self.completion_state,
                    len(self.clients),
                    self._active_readers,
                    int(self._retain_completed_output_until or 0),
                )
                return
            retained_tasks = (self.write_task, self.stderr_task, self.wait_task)
            if self.process is not None or any(task is not None for task in retained_tasks):
                self.last_error = (
                    "previous_process_teardown_unconfirmed"
                    if self.process is not None
                    else "previous_task_cleanup_unconfirmed"
                )
                logger.error(
                    "CSO HLS output start blocked by retained lifecycle output_key=%s pid=%s tasks=%s",
                    self.key,
                    getattr(self.process, "pid", None),
                    sum(task is not None for task in retained_tasks),
                )
                return
            if (
                self.finite_event_output
                and self.completed
                and self._retain_completed_output_until > time.time()
                and self._last_good_playlist_text
            ):
                self.running = True
                return
            segmented_input_target = "" if self.use_slate_as_input else self._segmented_input_target()
            use_direct_input = bool(self.input_target or segmented_input_target)
            if self.input_is_url and not await self._ensure_capacity_reservation():
                self.last_error = "capacity_blocked"
                return
            if self.use_slate_as_input:
                await self.slate_session.start()
                if not self.slate_session.running:
                    self.last_error = "slate_not_running"
                    return
            elif not use_direct_input:
                # Claim the ingest before starting it. Segmented HLS startup can
                # take several seconds, during which the periodic idle cleaner
                # must not mistake this session for an abandoned ingest.
                await self.ingest_session.add_lifecycle_reference(self.key)
                self.ingest_lifecycle_reference = True
                await self.ingest_session.start()
                if not self.ingest_session.running:
                    self.last_error = self.ingest_session.last_error or "ingest_not_running"
                    return
                # A segmented ingest can be recreated while recovering from a
                # cancelled/failed HLS request. Resolve its local playlist only
                # after start() so this output does not incorrectly subscribe to
                # the byte queue used by non-segmented ingests.
                segmented_input_target = self._segmented_input_target()
                use_direct_input = bool(self.input_target or segmented_input_target)

            await self._prepare_output_dir()
            self.ingest_queue = None
            self.slate_queue = None
            if self.use_slate_as_input:
                self.slate_queue = await self.slate_session.add_subscriber(
                    self.key,
                    prebuffer_bytes=256 * 1024,
                )
            elif segmented_input_target:
                if not self.ingest_lifecycle_reference:
                    await self.ingest_session.add_lifecycle_reference(self.key)
                    self.ingest_lifecycle_reference = True
            elif not use_direct_input:
                self.ingest_queue = await self.ingest_session.add_subscriber(
                    self.key,
                    prebuffer_bytes=256 * 1024,
                )
                if self.ingest_lifecycle_reference:
                    await self.ingest_session.remove_lifecycle_reference(self.key)
                    self.ingest_lifecycle_reference = False
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
            source = self.event_source
            if source is None and self.ingest_session is not None:
                source = self.ingest_session.current_source
            if self.use_slate_as_input:
                pipe_input_format = "mpegts"
            elif not use_direct_input:
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
                dict(getattr(self.slate_session, "media_hint", {}) or {})
                if self.use_slate_as_input
                else (
                    event_source_probe(source)
                    if use_direct_input
                    else dict(self.ingest_session.current_source_probe or {})
                )
            )
            source_identity = (
                self.input_target
                if use_direct_input
                else (
                    clean_text(getattr(self.slate_session, "key", "")) or self.key
                    if self.use_slate_as_input
                    else (self.ingest_session.current_source_url or clean_text(getattr(source, "url", "")))
                )
            )
            base_runtime_policy = effective_vod_hls_runtime_policy(self.policy, source)
            if self.use_slate_as_input:
                # Slates are already H.264/AAC MPEG-TS with output-safe stream
                # parameters. Remux them directly instead of decoding and
                # encoding the same synthetic media a second time.
                base_runtime_policy = {
                    **base_runtime_policy,
                    "output_mode": "force_remux",
                    "video_codec": "copy",
                    "audio_codec": "copy",
                    "subtitle_mode": "drop",
                    "hwaccel": False,
                    "hardware_decode": False,
                    "deinterlace": False,
                }
            self.running = True
            self.last_error = None
            self.last_activity = time.time()

            async def _attempt_start(
                effective_policy: dict[str, Any],
            ) -> CsoFfmpegAttemptResult:
                self.runtime_policy = dict(effective_policy)
                builder = CsoFfmpegCommandBuilder(
                    self.runtime_policy,
                    pipe_input_format=pipe_input_format,
                    source_probe=source_probe,
                )
                command = builder.build_hls_output_command(
                    self.output_dir,
                    input_target=(self.input_target or segmented_input_target) if use_direct_input else "",
                    input_is_url=self.input_is_url,
                    start_seconds=self.start_seconds,
                    user_agent=self.input_user_agent,
                    request_headers=self.input_request_headers,
                    pipe_probe_size_bytes=256 * 1024 if self.use_slate_as_input else 2 * 1024 * 1024,
                    pipe_analyse_duration_us=750_000 if self.use_slate_as_input else 5_000_000,
                    pipe_fps_probe_size=16 if self.use_slate_as_input else CSO_OUTPUT_FPS_PROBE_SIZE,
                )
                self._recent_ffmpeg_stderr.clear()
                logger.info(
                    "Starting CSO HLS output channel=%s output_key=%s mode=%s policy=(%s) command=%s",
                    self.channel_id,
                    bounded_log_value(self.key),
                    self._output_mode(),
                    policy_log_label(self.runtime_policy),
                    redact_ffmpeg_command_for_log(command),
                )
                self.process = await spawn_cso_ffmpeg_process(
                    *command,
                    label=f"hls-output:{self.key}",
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
                try:
                    started, failure_reason = await self._wait_for_startup_ready(
                        self.process,
                        timeout_seconds=startup_timeout_seconds,
                    )
                except asyncio.CancelledError:
                    teardown, task_cleanup = await self._cleanup_failed_start_attempt(
                        self.process,
                        write_task,
                        stderr_task,
                        wait_task,
                    )
                    if teardown.confirmed:
                        self.process = None
                    raise
                if started:
                    return CsoFfmpegAttemptResult(
                        dict(effective_policy),
                        True,
                        (self.process, write_task, stderr_task, wait_task),
                        "",
                        "",
                        self._ffmpeg_error_summary(),
                        "",
                    )
                logger.warning(
                    "CSO HLS output start failed channel=%s output_key=%s reason=%s",
                    self.channel_id,
                    self.key,
                    failure_reason or "unknown",
                )
                teardown, task_cleanup = await self._cleanup_failed_start_attempt(
                    self.process,
                    write_task,
                    stderr_task,
                    wait_task,
                )
                if teardown.confirmed:
                    self.process = None
                if not teardown.confirmed:
                    failure_reason = f"{failure_reason or 'output_start_failed'}:teardown_unconfirmed"
                elif not task_cleanup.confirmed:
                    failure_reason = f"{failure_reason or 'output_start_failed'}:task_cleanup_unconfirmed"
                return CsoFfmpegAttemptResult(
                    dict(effective_policy),
                    False,
                    None,
                    ffmpeg_failure_classification(failure_reason),
                    failure_reason,
                    self._ffmpeg_error_summary(),
                    hwaccel_failure_stage(failure_reason),
                )

            start_result = await start_ffmpeg_with_hw_decode_fallback(
                base_runtime_policy,
                source_identity,
                _attempt_start,
            )
            if start_result.success:
                self.runtime_policy = dict(start_result.policy)
                self.process, self.write_task, self.stderr_task, self.wait_task = start_result.runtime
                return
            self.runtime_policy = dict(start_result.policy)
            log_ffmpeg_start_result_failures(f"hls:{self.key}", start_result)
            self.running = False
            self.last_error = start_result.failure_reason or "output_start_failed"
            await self._release_capacity_if_upstream_gone("startup_failed")

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
                    logger.info(
                        "CSO HLS output ffmpeg[%s][%s]: %s",
                        self.channel_id,
                        bounded_log_value(self.key),
                        self._redacted_ffmpeg_error(rendered),
                    )
        rendered = text_buffer.strip()
        if rendered and token == self.process_token:
            self._recent_ffmpeg_stderr.append(rendered)
            self.last_activity = time.time()
            if enable_cso_output_command_debug_logging:
                logger.info(
                    "CSO HLS output ffmpeg[%s][%s]: %s",
                    self.channel_id,
                    bounded_log_value(self.key),
                    self._redacted_ffmpeg_error(rendered),
                )

    async def _read_valid_playlist_from_disk(self) -> str | None:
        if not self.playlist_path.exists():
            return None
        try:
            playlist_text = await asyncio.to_thread(self.playlist_path.read_text, "utf-8")
        except Exception:
            return None
        first_nonempty_line = next((line.strip() for line in playlist_text.splitlines() if line.strip()), "")
        if first_nonempty_line != "#EXTM3U":
            return None
        segment_names = self._playlist_segment_names(playlist_text)
        if not segment_names:
            return None
        output_dir = self.output_dir.resolve()
        for segment_name in segment_names:
            segment_path = (self.output_dir / segment_name).resolve()
            try:
                segment_path.relative_to(output_dir)
            except ValueError:
                return None
            if not segment_path.exists() or not segment_path.is_file():
                return None
            try:
                if int(segment_path.stat().st_size or 0) <= 0:
                    return None
            except Exception:
                return None
        return playlist_text

    async def _capture_finite_completed_playlist(self) -> bool:
        if not await self._acquire_reader():
            logger.error(
                "CSO HLS finite completion reader admission closed output_key=%s mode=%s completion_state=%s",
                self.key,
                self._output_mode(),
                self.completion_state,
            )
            return False
        try:
            playlist_text = await self._read_valid_playlist_from_disk()
            if not playlist_text:
                logger.error(
                    "CSO HLS finite completion missing valid playlist output_key=%s mode=%s expected_path=%s clients=%s readers=%s",
                    self.key,
                    self._output_mode(),
                    self.playlist_path,
                    len(self.clients),
                    self._active_readers,
                )
                return False
            source_lines = str(playlist_text).splitlines()
            lines = [line for line in source_lines if line.strip() != "#EXT-X-ENDLIST"]
            endlist_count = sum(1 for line in source_lines if line.strip() == "#EXT-X-ENDLIST")
            final_nonempty_line = next((line.strip() for line in reversed(source_lines) if line.strip()), "")
            endlist_already_terminal = endlist_count == 1 and final_nonempty_line == "#EXT-X-ENDLIST"
            completed_playlist = f"{chr(10).join(lines).rstrip()}\n#EXT-X-ENDLIST\n"
            temporary_playlist_path = self.playlist_path.with_name(f".{self.playlist_path.name}.completed.tmp")
            try:
                await asyncio.to_thread(temporary_playlist_path.write_text, completed_playlist, "utf-8")
                await asyncio.to_thread(temporary_playlist_path.replace, self.playlist_path)
            except Exception as exc:
                logger.error(
                    "CSO HLS finite completion playlist publish failed output_key=%s mode=%s expected_path=%s clients=%s readers=%s error=%s",
                    self.key,
                    self._output_mode(),
                    self.playlist_path,
                    len(self.clients),
                    self._active_readers,
                    exc,
                )
                return False
            self._last_good_playlist_text = completed_playlist
            self._last_good_playlist_ts = time.time()
            logger.info(
                "CSO HLS finite completion playlist finalized output_key=%s mode=%s endlist=%s clients=%s readers=%s",
                self.key,
                self._output_mode(),
                "already_present" if endlist_already_terminal else "added_or_normalized",
                len(self.clients),
                self._active_readers,
            )
            return True
        finally:
            await self._release_reader()

    async def _wait_loop(self, token, process):
        return_code = None
        try:
            if process:
                return_code = await process.wait()
        except Exception:
            return_code = None
        process_exited = mark_cso_ffmpeg_process_exited(process)
        if token != self.process_token:
            return
        if return_code == 0 and self.finite_event_output and not process_exited:
            self.running = False
            self.completed = False
            self.completion_state = "teardown_unconfirmed"
            self.last_error = "output_process_exit_unconfirmed"
            self._accepting_readers = False
            logger.error(
                "CSO HLS finite completion blocked by unconfirmed process exit output_key=%s mode=%s pid=%s return_code=%s clients=%s readers=%s",
                self.key,
                self._output_mode(),
                getattr(process, "pid", None),
                return_code,
                len(self.clients),
                self._active_readers,
            )
            await self.stop(force=True)
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
            reader_count = self._active_readers
            if process is self.process and process_exited:
                self.process = None
        if process_exited:
            await self._release_capacity_reservation("upstream_process_exited")
        logger.info(
            "CSO HLS process exited output_key=%s mode=%s pid=%s return_code=%s clients=%s readers=%s completion_state=%s",
            self.key,
            self._output_mode(),
            getattr(process, "pid", None),
            return_code,
            client_count,
            reader_count,
            self.completion_state,
        )
        if still_running and process_exited and return_code == 0 and self.finite_event_output:
            playlist_completed = await self._capture_finite_completed_playlist()
            if playlist_completed:
                retention_deadline = time.time() + self._client_idle_seconds()
                async with self.lock:
                    self.completed = True
                    self.completion_state = "completed"
                    self.running = True
                    self.last_error = None
                    self._retain_completed_output_until = retention_deadline
                    client_count = len(self.clients)
                    reader_count = self._active_readers
                logger.info(
                    "CSO HLS finite output completed and retained output_key=%s mode=%s pid=%s return_code=%s clients=%s readers=%s retention_deadline=%s",
                    self.key,
                    self._output_mode(),
                    getattr(process, "pid", None),
                    return_code,
                    client_count,
                    reader_count,
                    int(retention_deadline),
                )
                if client_count > 0 or reader_count > 0:
                    return
                await self.stop()
                return
            async with self.lock:
                self.completion_state = "completion_invalid"
                self.last_error = "output_completion_invalid_playlist"
            logger.error(
                "CSO HLS finite output completion rejected output_key=%s mode=%s pid=%s return_code=%s clients=%s readers=%s",
                self.key,
                self._output_mode(),
                getattr(process, "pid", None),
                return_code,
                client_count,
                reader_count,
            )
            await self.stop(force=True)
            return
        if (
            still_running
            and client_count > 0
            and has_completed_playlist
            and return_code == 0
            and self.use_slate_as_input
        ):
            if "#EXT-X-ENDLIST" not in str(self._last_good_playlist_text):
                self._last_good_playlist_text = f"{str(self._last_good_playlist_text).rstrip()}\n#EXT-X-ENDLIST\n"
            self._retain_completed_output_until = time.time() + max(15.0, float(CSO_HLS_CLIENT_IDLE_SECONDS))
            self.completion_state = "completed"
            logger.info(
                "CSO HLS output completed and retained channel=%s output_key=%s return_code=%s clients=%s retain_seconds=%s",
                self.channel_id,
                self.key,
                return_code,
                client_count,
                int(max(15.0, float(CSO_HLS_CLIENT_IDLE_SECONDS))),
            )
            return
        if still_running and client_count > 0 and return_code == 0 and not self.use_slate_as_input:
            async with self.lock:
                self.running = False
                self.completion_state = "restart_pending"
                self.last_error = "output_completed_restart_pending"
            logger.warning(
                "CSO HLS live output completed unexpectedly and will restart on next request channel=%s output_key=%s clients=%s",
                self.channel_id,
                self.key,
                client_count,
            )
            asyncio.create_task(self._restart_after_process_exit(token, process))
            return
        if still_running and client_count > 0:
            self.last_error = "output_reader_ended"
            logger.warning(
                "CSO HLS output ended unexpectedly channel=%s output_key=%s return_code=%s stderr=%s",
                self.channel_id,
                bounded_log_value(self.key),
                return_code,
                redact_ffmpeg_error_for_log(self._ffmpeg_error_summary()) or "n/a",
            )
        await self.stop(force=True)

    async def _restart_after_process_exit(self, token, process):
        # Let the wait task return before inspecting the previous generation.
        await asyncio.sleep(0)
        async with self.lifecycle_lock:
            async with self.lock:
                if (
                    token != self.process_token
                    or self.running
                    or not self.clients
                    or (self.process is not None and self.process is not process)
                ):
                    return
                write_task = self.write_task
                stderr_task = self.stderr_task
                wait_task = self.wait_task
                self.write_task = None
                self.stderr_task = None
                self.wait_task = None

            task_cleanup = await cancel_and_await_tasks((write_task, stderr_task, wait_task))
            if not task_cleanup.confirmed:
                pending_tasks = set(task_cleanup.pending_tasks)
                async with self.lock:
                    self.write_task = write_task if write_task in pending_tasks else None
                    self.stderr_task = stderr_task if stderr_task in pending_tasks else None
                    self.wait_task = wait_task if wait_task in pending_tasks else None
                    self.last_error = "previous_task_cleanup_unconfirmed"
                    self.completion_state = "restart_blocked"
                logger.error(
                    "CSO HLS output restart blocked by previous lifecycle cleanup "
                    "channel=%s output_key=%s pending_tasks=%s",
                    self.channel_id,
                    bounded_log_value(self.key),
                    len(task_cleanup.pending_tasks),
                )
                return

            async with self.lock:
                if token != self.process_token or self.running or not self.clients:
                    return
                self.completion_state = "restarting"
            await self._start_with_lifecycle_locked()

    async def _add_client(self, connection_id: str, on_disconnect: Any = None) -> bool:
        async with self.lock:
            key = str(connection_id)
            existed = key in self.clients
            previous = self.clients.get(key) or {}
            now_value = time.time()
            self.clients[key] = {
                "last_touch": now_value,
                "on_disconnect": on_disconnect if on_disconnect is not None else previous.get("on_disconnect"),
            }
            client_count = len(self.clients)
            self.last_activity = now_value
            if self.finite_event_output and self.completed:
                self._retain_completed_output_until = now_value + self._client_idle_seconds()
        if not existed:
            logger.info(
                "CSO HLS output client connected channel=%s output_key=%s connection_id=%s clients=%s policy=(%s)",
                self.channel_id,
                self.key,
                bounded_log_value(connection_id),
                client_count,
                policy_log_label(self.runtime_policy),
            )
        return not existed

    async def add_client(self, connection_id: str, on_disconnect: Any = None) -> bool:
        is_new_client = await self._add_client(connection_id, on_disconnect=on_disconnect)
        self._schedule_idle_cleanup()
        return is_new_client

    async def has_client(self, connection_id: str) -> bool:
        async with self.lock:
            return str(connection_id) in self.clients

    async def _invoke_disconnect_hook(self, connection_id: str, disconnect_hook: Any):
        if not callable(disconnect_hook):
            return
        try:
            await disconnect_hook(str(connection_id))
        except Exception as exc:
            logger.warning(
                "CSO HLS output disconnect hook failed channel=%s output_key=%s connection_id=%s error=%s",
                self.channel_id,
                self.key,
                bounded_log_value(connection_id),
                exc,
            )

    async def touch_client(self, connection_id: str):
        now_value = time.time()
        async with self.lock:
            refreshed = self._refresh_client_activity_locked(str(connection_id), now_value)
        if not refreshed:
            return
        self._schedule_idle_cleanup()

    async def _finish_client_removal(
        self,
        connection_id: str,
        removed: dict[str, Any] | None,
        remaining: int,
    ) -> int:
        key = str(connection_id)
        async with self.lock:
            if key in self.clients:
                restored = self.clients.get(key)
                if isinstance(restored, dict) and isinstance(removed, dict):
                    if restored.get("on_disconnect") is None:
                        restored["on_disconnect"] = removed.get("on_disconnect")
                return len(self.clients)
        disconnect_hook = None
        if isinstance(removed, dict):
            disconnect_hook = removed.get("on_disconnect")
        await self._invoke_disconnect_hook(connection_id, disconnect_hook)
        logger.info(
            "CSO HLS output client disconnected channel=%s output_key=%s connection_id=%s clients=%s policy=(%s)",
            self.channel_id,
            self.key,
            bounded_log_value(connection_id),
            remaining,
            policy_log_label(self.runtime_policy),
        )
        if remaining == 0:
            await asyncio.shield(self.stop())
        return remaining

    async def remove_client(self, connection_id: str) -> int:
        async with self.lock:
            removed = self.clients.pop(str(connection_id), None)
            remaining = len(self.clients)
        return await self._finish_client_removal(connection_id, removed, remaining)

    async def prune_idle_clients(self, now_ts: float | None = None):
        now_value = float(now_ts if now_ts is not None else time.time())
        idle_seconds = self._client_idle_seconds()
        while True:
            stale_ids = []
            wait_for_readers = False
            async with self.lock:
                reader_state_changed = self._reader_state_changed
                for connection_id, entry in list(self.clients.items()):
                    last_touch = float(entry.get("last_touch") or 0.0) if isinstance(entry, dict) else 0.0
                    if (now_value - last_touch) < idle_seconds:
                        continue
                    if self._active_readers_by_connection.get(connection_id, 0) > 0:
                        wait_for_readers = True
                        continue
                    stale_ids.append(connection_id)
            for connection_id in stale_ids:
                async with self.lock:
                    entry = self.clients.get(connection_id)
                    last_touch = float(entry.get("last_touch") or 0.0) if isinstance(entry, dict) else 0.0
                    if (now_value - last_touch) < idle_seconds:
                        continue
                    if self._active_readers_by_connection.get(connection_id, 0) > 0:
                        wait_for_readers = True
                        continue
                    removed = self.clients.pop(connection_id, None)
                    remaining = len(self.clients)
                logger.info(
                    "CSO HLS output dropping idle client channel=%s output_key=%s connection_id=%s idle_seconds=%s",
                    self.channel_id,
                    self.key,
                    bounded_log_value(connection_id),
                    int(idle_seconds),
                )
                await self._finish_client_removal(connection_id, removed, remaining)
            if not wait_for_readers:
                return
            await reader_state_changed.wait()

    async def read_playlist_text(self, connection_id: str | None = None) -> str | None:
        if not await self._acquire_reader(connection_id):
            return None
        try:
            if self.completed and self._last_good_playlist_text:
                if not self.playlist_path.exists():
                    logger.error(
                        "CSO HLS retained playlist missing output_key=%s mode=%s expected_path=%s clients=%s readers=%s retention_deadline=%s",
                        self.key,
                        self._output_mode(),
                        self.playlist_path,
                        len(self.clients),
                        self._active_readers,
                        int(self._retain_completed_output_until or 0),
                    )
                await self._record_successful_read(connection_id)
                return self._last_good_playlist_text
            playlist_text = await self._read_valid_playlist_from_disk()
            if not playlist_text:
                return self._last_good_playlist_text
            self._last_good_playlist_text = playlist_text
            self._last_good_playlist_ts = time.time()
            await self._record_successful_read(connection_id)
            return playlist_text
        finally:
            await self._release_reader(connection_id)

    async def read_segment_bytes(
        self,
        segment_name: str,
        connection_id: str | None = None,
    ) -> bytes | None:
        if not await self._acquire_reader(connection_id):
            return None
        try:
            name = clean_text(segment_name)
            if not name or not SAFE_HLS_SEGMENT_RE.match(name):
                return None
            output_dir = self.output_dir.resolve()
            segment_path = (self.output_dir / name).resolve()
            try:
                segment_path.relative_to(output_dir)
            except ValueError:
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
                if self.completed:
                    logger.error(
                        "CSO HLS retained segment missing output_key=%s mode=%s expected_path=%s clients=%s readers=%s retention_deadline=%s",
                        self.key,
                        self._output_mode(),
                        segment_path,
                        len(self.clients),
                        self._active_readers,
                        int(self._retain_completed_output_until or 0),
                    )
                return None
            payload = await asyncio.to_thread(segment_path.read_bytes)
            if not payload:
                if self.completed:
                    logger.error(
                        "CSO HLS retained segment empty output_key=%s mode=%s expected_path=%s clients=%s readers=%s retention_deadline=%s",
                        self.key,
                        self._output_mode(),
                        segment_path,
                        len(self.clients),
                        self._active_readers,
                        int(self._retain_completed_output_until or 0),
                    )
                return None
            await self._record_successful_read(connection_id)
            return payload
        finally:
            await self._release_reader(connection_id)

    async def stop(self, force: bool = False) -> CsoLifecycleCleanupResult | None:
        async with self.lifecycle_lock:
            return await self._stop_locked(force=force)

    async def _stop_locked(self, force: bool = False) -> CsoLifecycleCleanupResult | None:
        async with self.lock:
            if (
                not self.running
                and not self.process
                and not self.clients
                and not self.completed
                and self.write_task is None
                and self.stderr_task is None
                and self.wait_task is None
                and self.ingest_queue is None
                and not self.ingest_lifecycle_reference
                and self.slate_queue is None
                and not self._capacity_reserved
            ):
                return
            if not force and self.clients:
                return
            self._accepting_readers = False
            self.running = False
            self.completion_state = "stopping"
            process = self.process
            self.process_token += 1
            stop_token = self.process_token
            write_task = self.write_task
            self.write_task = None
            stderr_task = self.stderr_task
            self.stderr_task = None
            wait_task = self.wait_task
            self.wait_task = None
            self._retain_completed_output_until = 0.0
            idle_cleanup_task = self._idle_cleanup_task
            self._idle_cleanup_task = None
            client_count = len(self.clients)
            reader_count = self._active_readers
            disconnected_clients = list(self.clients.items())
            self.clients = {}
        logger.info(
            "Stopping CSO HLS output channel=%s output_key=%s mode=%s clients=%s readers=%s force=%s policy=(%s)",
            self.channel_id,
            self.key,
            self._output_mode(),
            client_count,
            reader_count,
            force,
            policy_log_label(self.runtime_policy),
        )
        if (
            idle_cleanup_task is not None
            and idle_cleanup_task is not asyncio.current_task()
            and not idle_cleanup_task.done()
        ):
            idle_cleanup_task.cancel()
        for disconnected_id, disconnected_entry in disconnected_clients:
            disconnect_hook = None
            if isinstance(disconnected_entry, dict):
                disconnect_hook = disconnected_entry.get("on_disconnect")
            await self._invoke_disconnect_hook(disconnected_id, disconnect_hook)
            logger.info(
                "CSO HLS output client disconnected channel=%s output_key=%s connection_id=%s clients=%s policy=(%s)",
                self.channel_id,
                self.key,
                bounded_log_value(disconnected_id),
                0,
                policy_log_label(self.runtime_policy),
            )
        teardown = None
        if process:
            terminate_timeout = 0.01 if force and self.finite_event_output else 2.0
            teardown = await terminate_ffmpeg_process(
                process,
                terminate_timeout_seconds=terminate_timeout,
                kill_timeout_seconds=2.0,
            )
        task_cleanup = await cancel_and_await_tasks((write_task, stderr_task, wait_task))
        if not task_cleanup.confirmed:
            pending_tasks = set(task_cleanup.pending_tasks)
            async with self.lock:
                self.write_task = write_task if write_task in pending_tasks else None
                self.stderr_task = stderr_task if stderr_task in pending_tasks else None
                self.wait_task = wait_task if wait_task in pending_tasks else None
        if process is not None and teardown is not None and teardown.confirmed:
            async with self.lock:
                if self.process is process:
                    self.process = None
        await _detach_output_input_subscriptions(self)
        if task_cleanup.confirmed and (teardown is None or teardown.confirmed):
            await self._release_capacity_if_upstream_gone("output_stopped")
        if not self._readers_drained.is_set():
            logger.info(
                "CSO HLS output cleanup waiting for readers output_key=%s mode=%s clients=%s readers=%s reason=active_readers",
                self.key,
                self._output_mode(),
                len(self.clients),
                self._active_readers,
            )
            await self._readers_drained.wait()
        async with self.lock:
            should_cleanup_output_dir = (
                self.process_token == stop_token
                and not self.running
                and not self.process
                and not self.clients
                and not self._active_readers
                and task_cleanup.confirmed
                and (teardown is None or teardown.confirmed)
            )
            cleanup_reason = "eligible" if should_cleanup_output_dir else "lifecycle_not_confirmed"
            if should_cleanup_output_dir:
                self.completed = False
                self.completion_state = "stopped"
                self._retain_completed_output_until = 0.0
        logger.info(
            "CSO HLS output cleanup eligibility output_key=%s mode=%s eligible=%s reason=%s clients=%s readers=%s process_retained=%s tasks_confirmed=%s teardown_confirmed=%s",
            self.key,
            self._output_mode(),
            should_cleanup_output_dir,
            cleanup_reason,
            len(self.clients),
            self._active_readers,
            self.process is not None,
            task_cleanup.confirmed,
            teardown is None or teardown.confirmed,
        )
        if should_cleanup_output_dir:
            await remove_cso_cache_dir(self.output_dir, logger, f"hls-output:{self.key}")
            self._last_good_playlist_text = None
            self._last_good_playlist_ts = 0.0
            logger.info(
                "CSO HLS output directory cleanup complete output_key=%s mode=%s path=%s teardown_confirmed=%s",
                self.key,
                self._output_mode(),
                self.output_dir,
                teardown is None or teardown.confirmed,
            )
        return cso_lifecycle_cleanup_result(teardown, task_cleanup)
