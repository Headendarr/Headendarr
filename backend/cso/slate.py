import asyncio
import logging
import re
import time
from collections import deque
from backend.config import enable_cso_slate_command_debug_logging
from backend.utils import clean_key, clean_text

from .common import ByteBudgetQueue, wait_process_exit_with_timeout
from .constants import (
    CSO_INGEST_SUBSCRIBER_QUEUE_MAX_BYTES,
    MPEGTS_CHUNK_BYTES,
    CSO_UNAVAILABLE_REASON_DURATIONS_SECONDS,
    CSO_UNAVAILABLE_SLATE_MESSAGES,
)
from .ffmpeg import CsoFfmpegCommandBuilder
from .types import CsoSource


logger = logging.getLogger("cso")


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

async def iter_cso_slate_source(config_path, reason, detail_hint=""):
    reason_key = clean_key(reason, fallback="playback_unavailable")
    resolved_duration = cso_unavailable_duration_seconds(reason_key)
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


def cso_unavailable_duration_seconds(reason_key):
    fallback = CSO_UNAVAILABLE_REASON_DURATIONS_SECONDS.get("default", 10)
    try:
        return int(CSO_UNAVAILABLE_REASON_DURATIONS_SECONDS.get(reason_key, fallback))
    except Exception:
        return int(fallback)


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
                await wait_process_exit_with_timeout(process, timeout_seconds=1.5)
            except Exception:
                try:
                    process.kill()
                    await wait_process_exit_with_timeout(process, timeout_seconds=1.5)
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
