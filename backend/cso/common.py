import asyncio
import logging
import os
import shutil
import time
from collections import deque
from pathlib import Path
from typing import cast

from backend.config import enable_cso_preserve_segment_cache

from quart import Quart
from werkzeug.local import LocalProxy

from .types import CsoStreamPlan


def current_quart_app_object() -> Quart:
    from quart import current_app

    app_proxy = cast(LocalProxy[Quart], current_app)
    return app_proxy._get_current_object()


def process_is_running(pid):
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except Exception:
        return True
    return True


async def wait_process_exit_with_timeout(process, timeout_seconds=2.0):
    if not process:
        return None
    return await asyncio.wait_for(process.wait(), timeout=float(timeout_seconds))


def _debug_archive_path(path: Path) -> Path:
    stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    base_path = path.with_name(f"{path.name}.debug-{stamp}-{os.getpid()}")
    candidate = base_path
    counter = 1
    while candidate.exists():
        candidate = path.with_name(f"{base_path.name}-{counter}")
        counter += 1
    return candidate


async def prepare_cso_cache_dir(path: Path | str, logger: logging.Logger, label: str):
    cache_path = Path(path)
    if cache_path.exists():
        if enable_cso_preserve_segment_cache:
            archive_path = _debug_archive_path(cache_path)
            await asyncio.to_thread(shutil.move, str(cache_path), str(archive_path))
            logger.info(
                "Preserved CSO segment cache before reuse label=%s path=%s preserved_path=%s",
                label,
                cache_path,
                archive_path,
            )
        else:
            await asyncio.to_thread(shutil.rmtree, cache_path, True)
    cache_path.mkdir(parents=True, exist_ok=True)


async def remove_cso_cache_dir(path: Path | str, logger: logging.Logger, label: str):
    cache_path = Path(path)
    if not cache_path.exists():
        return
    if enable_cso_preserve_segment_cache:
        logger.info(
            "Preserving CSO segment cache label=%s path=%s because ENABLE_CSO_PRESERVE_SEGMENT_CACHE=true",
            label,
            cache_path,
        )
        return
    await asyncio.to_thread(shutil.rmtree, cache_path, True)


class _SessionMap:
    def __init__(self):
        self.sessions = {}
        self.lock = asyncio.Lock()

    async def get_or_create(self, key, factory):
        async with self.lock:
            session = self.sessions.get(key)
            if session is not None:
                return session
            session = factory()
            self.sessions[key] = session
            return session

    async def cleanup_idle_streams(self, idle_timeout=300):
        now = time.time()
        async with self.lock:
            items = list(self.sessions.items())
        for key, session in items:
            prune_hook = getattr(session, "prune_idle_clients", None)
            if callable(prune_hook):
                await prune_hook(now)
            if session.running and (now - session.last_activity) < idle_timeout:
                continue
            async with session.lock:
                has_subscribers = bool(
                    getattr(session, "subscribers", None)
                    or getattr(session, "clients", None)
                    or getattr(session, "lifecycle_references", None)
                )
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
        self.slate = _SessionMap()
        self.output = _SessionMap()

    async def get_or_create_ingest(self, key, factory):
        return await self.ingest.get_or_create(key, factory)

    async def get_or_create_slate(self, key, factory):
        return await self.slate.get_or_create(key, factory)

    async def get_or_create_output(self, key, factory):
        return await self.output.get_or_create(key, factory)

    async def cleanup_idle_streams(self, idle_timeout=300):
        await self.output.cleanup_idle_streams(idle_timeout=idle_timeout)
        await self.slate.cleanup_idle_streams(idle_timeout=idle_timeout)
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

    async def has_active_ingest_for_source(self, source_id):
        prefix = f"cso-source-ingest-{int(source_id)}"
        async with self.ingest.lock:
            session = self.ingest.sessions.get(prefix)
            if not session:
                return False
            return bool(session.running and session.process)

    async def has_ingest_session_for_channel(self, channel_id):
        prefix = f"cso-ingest-{int(channel_id)}"
        async with self.ingest.lock:
            return self.ingest.sessions.get(prefix) is not None

    async def has_ingest_session_for_source(self, source_id):
        prefix = f"cso-source-ingest-{int(source_id)}"
        async with self.ingest.lock:
            return self.ingest.sessions.get(prefix) is not None

    async def disconnect_output_client(self, connection_id: str) -> bool:
        connection_key = str(connection_id or "").strip()
        if not connection_key:
            return False
        async with self.output.lock:
            sessions = list(self.output.sessions.values())
        for session in sessions:
            has_client_hook = getattr(session, "has_client", None)
            try:
                if callable(has_client_hook):
                    if not bool(await has_client_hook(connection_key)):
                        continue
                else:
                    async with session.lock:
                        clients = getattr(session, "clients", None) or {}
                        if connection_key not in clients:
                            continue
                remove_client_hook = getattr(session, "remove_client", None)
                if not callable(remove_client_hook):
                    continue
                await remove_client_hook(connection_key)
                return True
            except Exception:
                continue
        return False


cso_session_manager = CsoRuntimeManager()


class ByteBudgetQueue:
    """Leaky async queue bounded by payload bytes instead of item count."""

    def __init__(self, max_bytes):
        self.max_bytes = max(1, int(max_bytes or 1))
        self._items = deque()
        self._bytes = 0
        self._cond = asyncio.Condition()

    @staticmethod
    def _payload_size(payload):
        if payload is None:
            return 0
        try:
            return len(payload)
        except Exception:
            return 0

    async def put_drop_oldest(self, payload):
        now_value = time.time()
        size = self._payload_size(payload)
        dropped_items = 0
        dropped_bytes = 0
        payload_too_large = False
        async with self._cond:
            while payload is not None and self._items and (self._bytes + size) > self.max_bytes:
                old_payload, old_size, _ = self._items.popleft()
                if old_payload is not None:
                    self._bytes = max(0, self._bytes - old_size)
                    dropped_items += 1
                    dropped_bytes += int(old_size or 0)
            if payload is not None and size > self.max_bytes:
                while self._items:
                    old_payload, old_size, _ = self._items.popleft()
                    if old_payload is not None:
                        dropped_items += 1
                        dropped_bytes += int(old_size or 0)
                self._bytes = 0
                payload_too_large = True
            self._items.append((payload, size, now_value))
            if payload is not None:
                self._bytes += size
            self._cond.notify(1)
            queued_bytes = int(self._bytes)
            queued_items = len(self._items)
        return {
            "dropped_items": dropped_items,
            "dropped_bytes": dropped_bytes,
            "payload_too_large": payload_too_large,
            "queued_bytes": queued_bytes,
            "queued_items": queued_items,
            "max_bytes": int(self.max_bytes),
        }

    async def put_eof(self):
        await self.put_drop_oldest(None)

    async def get(self):
        async with self._cond:
            while not self._items:
                await self._cond.wait()
            payload, size, _ = self._items.popleft()
            if payload is not None:
                self._bytes = max(0, self._bytes - size)
            return payload

    async def iter_items(self):
        while True:
            item = await self.get()
            if item is None:
                break
            yield item

    async def stats(self):
        now_value = time.time()
        async with self._cond:
            oldest_age = 0.0
            if self._items:
                oldest_age = max(0.0, now_value - float(self._items[0][2] or now_value))
            return {
                "queued_items": len(self._items),
                "queued_bytes": int(self._bytes),
                "max_bytes": int(self.max_bytes),
                "oldest_age_seconds": oldest_age,
            }

    async def clear(self):
        async with self._cond:
            self._items.clear()
            self._bytes = 0


def build_cso_stream_plan(
    generator,
    content_type,
    error_message,
    status_code,
    headers=None,
    cutoff_seconds=None,
    final_status_code=None,
):
    return CsoStreamPlan(
        generator=generator,
        content_type=content_type,
        error_message=error_message,
        status_code=int(status_code or 500),
        headers=headers,
        cutoff_seconds=cutoff_seconds,
        final_status_code=final_status_code,
    )


def wrap_slate_words(text, max_chars=44, max_lines=2):
    words = [part for part in str(text or "").strip().split() if part]
    if not words:
        return []
    lines = []
    current = []
    for word in words:
        candidate = " ".join(current + [word]).strip()
        if len(candidate) <= max_chars or not current:
            current.append(word)
            continue
        lines.append(" ".join(current))
        current = [word]
        if len(lines) >= max_lines - 1:
            break
    if current and len(lines) < max_lines:
        lines.append(" ".join(current))
    return lines[:max_lines]


def resolve_cso_unavailable_logo_path():
    project_root = Path(__file__).resolve().parents[1]
    candidates = [
        project_root / "frontend/src/assets/icon.png",
        project_root / "logo.png",
        project_root / "frontend/public/icons/Headendarr-Logo.png",
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return str(candidate)
    return None
