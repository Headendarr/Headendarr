import asyncio
import json
import logging
import os
import shutil
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiofiles
import aiohttp
import requests
import urllib3

from backend.hls_multiplexer import get_header_value
from backend.stream_profiles import content_type_for_media_path
from backend.utils import clean_key, clean_text, convert_to_int

from .capacity import cso_capacity_registry, source_capacity_key, source_capacity_limit
from .constants import (
    VOD_CACHE_CHUNK_BYTES,
    VOD_CACHE_METADATA_TIMEOUT_SECONDS,
    VOD_CACHE_ROOT,
    VOD_CACHE_TTL_SECONDS,
    VOD_HEAD_PROBE_STATE_TTL_SECONDS,
)
from .sources import cso_source_from_vod_source
from .types import CsoSource, VodCacheEntry, VodHeadProbeStateEntry


logger = logging.getLogger("cso")


def _vod_head_probe_state_path() -> Path:
    home_dir = os.environ.get("HOME_DIR") or os.path.expanduser("~")
    return Path(home_dir) / ".tvh_iptv_config" / "cache" / "vod_head_probe_state.json"


def _vod_head_probe_cache_key(source: CsoSource, upstream_url: str) -> str:
    source_id = int(source.id or 0)
    parsed = urlparse(upstream_url or "")
    source_host = clean_text(parsed.netloc)
    return f"{source.source_type}:{source.playlist_id}:{source_id}:{source_host}"


async def _probe_vod_cache_metadata(source: CsoSource, upstream_url: str, request_headers=None):
    from .vod_proxy import filter_vod_proxy_request_headers

    headers = filter_vod_proxy_request_headers(request_headers, source)
    headers.pop("Range", None)
    timeout = aiohttp.ClientTimeout(total=VOD_CACHE_METADATA_TIMEOUT_SECONDS, connect=10, sock_connect=10, sock_read=10)
    async with aiohttp.ClientSession(timeout=timeout, auto_decompress=False) as session:
        skip_head = await vod_head_probe_state_store.should_skip_head(source, upstream_url)
        if skip_head:
            logger.debug(
                "Skipping VOD cache metadata HEAD probe source_id=%s upstream_url=%s due to cached unsupported state",
                source.id,
                upstream_url,
            )
        else:
            try:
                response = await session.request("HEAD", upstream_url, headers=headers, allow_redirects=True)
                try:
                    if int(response.status or 0) == 200:
                        size_header = clean_text(response.headers.get("Content-Length"))
                        if size_header.isdigit():
                            await vod_head_probe_state_store.mark_head_supported(source, upstream_url)
                            return {
                                "size": int(size_header),
                                "headers": dict(response.headers),
                                "status": int(response.status or 200),
                            }
                finally:
                    await response.release()
            except Exception as exc:
                logger.info(
                    "VOD cache metadata HEAD probe failed source_id=%s upstream_url=%s error=%s",
                    source.id,
                    upstream_url,
                    exc,
                )
                await vod_head_probe_state_store.mark_head_failed(source, upstream_url, str(exc))
        response = await session.get(
            upstream_url,
            headers={**headers, "Range": "bytes=0-0"},
            allow_redirects=True,
        )
        try:
            content_range = clean_text(response.headers.get("Content-Range"))
            total_size = None
            if "/" in content_range:
                tail = content_range.rsplit("/", 1)[-1].strip()
                if tail.isdigit():
                    total_size = int(tail)
            if total_size:
                return {
                    "size": int(total_size),
                    "headers": dict(response.headers),
                    "status": int(response.status or 206),
                }
        finally:
            await response.release()
    return {"size": None, "headers": {}, "status": 0}


async def _vod_cache_has_space(required_bytes: int):
    if required_bytes <= 0:
        return False
    usage = await asyncio.to_thread(shutil.disk_usage, str(VOD_CACHE_ROOT.parent))
    return int(usage.free or 0) >= int(required_bytes)


async def ensure_vod_cache_ready(
    entry: VodCacheEntry,
    request_headers=None,
    require_size=False,
):
    from .vod_proxy import proxy_response_headers

    async with entry.probe_lock:
        entry.touch()
        if entry.complete and entry.final_path.exists() and entry.expected_size:
            return {
                "cacheable": True,
                "size_known": True,
                "expected_size": int(entry.expected_size),
                "complete": True,
            }
        if entry.expected_size and entry.metadata_headers is not None:
            return {
                "cacheable": True,
                "size_known": True,
                "expected_size": int(entry.expected_size),
                "complete": False,
            }
        probe = await _probe_vod_cache_metadata(entry.source, entry.upstream_url, request_headers=request_headers)
        expected_size = int(probe.get("size") or 0)
        if expected_size <= 0:
            entry.failed_reason = "size_unknown"
            if require_size:
                return {
                    "cacheable": False,
                    "size_known": False,
                    "expected_size": None,
                    "reason": "size_unknown",
                }
            return {
                "cacheable": False,
                "size_known": False,
                "expected_size": None,
                "reason": "size_unknown",
            }
        has_space = await _vod_cache_has_space(expected_size * 2)
        if not has_space:
            eviction = await vod_cache_manager.purge_oldest_for_space(expected_size * 2, preserve_key=entry.key)
            has_space = bool(eviction.get("has_space"))
        if not has_space:
            entry.failed_reason = "insufficient_space"
            return {
                "cacheable": False,
                "size_known": True,
                "expected_size": expected_size,
                "reason": "insufficient_space",
            }
        entry.expected_size = expected_size
        entry.metadata_headers = proxy_response_headers(int(probe.get("status") or 200), probe.get("headers") or {})
        entry.content_type = clean_text(get_header_value(probe.get("headers") or {}, "Content-Type")) or None
        return {
            "cacheable": True,
            "size_known": True,
            "expected_size": expected_size,
            "complete": False,
        }


async def start_vod_cache_download(entry: VodCacheEntry, owner_key: str, request_headers=None):
    async with entry.state_lock:
        if entry.complete:
            return True
        if entry.downloader_running:
            return True
        if not entry.expected_size:
            return False
        reserved = await cso_capacity_registry.try_reserve(
            source_capacity_key(entry.source),
            owner_key,
            source_capacity_limit(entry.source),
            slot_id=owner_key,
        )
        if not reserved:
            entry.failed_reason = "capacity_blocked"
            return False
        entry.downloader_owner_key = owner_key
        entry.failed_reason = None
        entry.ready_event.clear()
        entry.progress_event.clear()
        entry.download_task = asyncio.create_task(
            _run_vod_cache_download(entry, owner_key, request_headers=request_headers),
            name=f"vod-cache-{entry.key}",
        )
        return True


async def _run_vod_cache_download(entry: VodCacheEntry, owner_key: str, request_headers=None):
    from .vod_proxy import filter_vod_proxy_request_headers, proxy_response_headers

    base_headers = filter_vod_proxy_request_headers(request_headers, entry.source)
    await asyncio.to_thread(entry.part_path.parent.mkdir, 0o755, True, True)
    max_attempts = 4
    attempt = 0
    try:
        while True:
            if entry.complete:
                break

            part_size = 0
            if entry.part_path.exists():
                try:
                    part_size = int(entry.part_path.stat().st_size or 0)
                except Exception:
                    part_size = 0
            entry.bytes_written = part_size

            range_start = max(0, int(entry.bytes_written or 0))
            headers = dict(base_headers)
            headers["Range"] = f"bytes={range_start}-"

            http_session = requests.Session()
            response = None
            iterator = None
            try:
                response = await asyncio.to_thread(
                    lambda session=http_session, request_headers=headers: session.get(
                        entry.upstream_url,
                        headers=request_headers,
                        allow_redirects=True,
                        stream=True,
                        timeout=(15, 30),
                    )
                )
                status_code = int(response.status_code or 502)
                if status_code >= 400:
                    entry.failed_reason = f"download_status_{status_code}"
                    entry.ready_event.set()
                    return

                if range_start > 0 and status_code == 200:
                    logger.warning(
                        "VOD cache resume was ignored by upstream; restarting download asset=%s source_id=%s offset=%s",
                        entry.key,
                        entry.source.id,
                        range_start,
                    )
                    if entry.part_path.exists():
                        await asyncio.to_thread(entry.part_path.unlink, True)
                    entry.bytes_written = 0
                    range_start = 0
                    continue

                entry.metadata_headers = proxy_response_headers(status_code, response.headers)
                entry.content_type = clean_text(response.headers.get("Content-Type")) or entry.content_type

                content_range = clean_text(response.headers.get("Content-Range"))
                if not entry.expected_size and "/" in content_range:
                    total_text = content_range.rsplit("/", 1)[-1].strip()
                    if total_text.isdigit():
                        entry.expected_size = int(total_text)
                if not entry.expected_size:
                    size_header = clean_text(response.headers.get("Content-Length"))
                    if size_header.isdigit():
                        content_length = int(size_header)
                        entry.expected_size = range_start + content_length if status_code == 206 else content_length

                entry.ready_event.set()
                open_mode = "ab" if range_start > 0 else "wb"
                iterator = response.iter_content(chunk_size=VOD_CACHE_CHUNK_BYTES)
                async with aiofiles.open(entry.part_path, open_mode) as handle:
                    while True:
                        try:
                            chunk = await asyncio.to_thread(next, iterator, None)
                        except (
                            requests.exceptions.ChunkedEncodingError,
                            requests.exceptions.ConnectionError,
                            requests.exceptions.ReadTimeout,
                            urllib3.exceptions.ProtocolError,
                            ConnectionResetError,
                            OSError,
                        ) as exc:
                            attempt += 1
                            entry.failed_reason = f"download_retry:{exc}"
                            if attempt >= max_attempts:
                                raise
                            logger.warning(
                                "VOD cache download interrupted; retrying asset=%s source_id=%s bytes_written=%s attempt=%s error=%s",
                                entry.key,
                                entry.source.id,
                                int(entry.bytes_written or 0),
                                attempt,
                                exc,
                            )
                            break
                        if not chunk:
                            break
                        await handle.write(chunk)
                        entry.bytes_written += len(chunk)
                        entry.touch()
                        entry.progress_event.set()
                        entry.progress_event = asyncio.Event()
                    await handle.flush()
            finally:
                try:
                    if response is not None:
                        await asyncio.to_thread(response.close)
                except Exception:
                    pass
                try:
                    await asyncio.to_thread(http_session.close)
                except Exception:
                    pass

            if entry.expected_size and int(entry.bytes_written or 0) >= int(entry.expected_size):
                break
            if attempt >= max_attempts:
                break
            if entry.failed_reason and str(entry.failed_reason).startswith("download_retry:"):
                await asyncio.sleep(0.5)
                continue
            entry.failed_reason = "download_incomplete"
            break

        if entry.expected_size and int(entry.bytes_written or 0) >= int(entry.expected_size):
            await asyncio.to_thread(os.replace, entry.part_path, entry.final_path)
            entry.complete = True
            entry.bytes_written = int(entry.expected_size)
            entry.failed_reason = None
            logger.info(
                "VOD cache completed asset=%s bytes=%s path=%s",
                entry.key,
                int(entry.bytes_written or 0),
                entry.final_path,
            )
        else:
            entry.failed_reason = "download_incomplete"
        entry.touch()
    except asyncio.CancelledError:
        entry.failed_reason = "cancelled"
        raise
    except Exception as exc:
        entry.failed_reason = f"download_failed:{exc}"
        logger.warning("VOD cache download failed asset=%s error=%s", entry.key, exc)
    finally:
        entry.ready_event.set()
        entry.progress_event.set()
        await cso_capacity_registry.release(source_capacity_key(entry.source), owner_key, slot_id=owner_key)
        async with entry.state_lock:
            entry.downloader_owner_key = None
            entry.download_task = None


async def cleanup_vod_proxy_cache():
    return await vod_cache_manager.cleanup()


async def warm_vod_cache(candidate, upstream_url, episode=None, owner_key=None, request_headers=None):
    if not candidate:
        return False
    source = await cso_source_from_vod_source(candidate, upstream_url)
    if not source or not source.url:
        return False
    entry = await vod_cache_manager.get_or_create(source, source.url)
    cache_meta = await ensure_vod_cache_ready(entry, request_headers=request_headers)
    if not cache_meta.get("cacheable"):
        return False
    owner = clean_text(owner_key) or f"vod-cache-warm-{source.id}"
    return await start_vod_cache_download(entry, owner, request_headers=request_headers)


def _vod_cache_asset_parts(source: CsoSource):
    source_type = clean_key(source.source_type)
    internal_id = int(source.cache_internal_id or source.internal_id or 0)
    if source_type == "vod_episode":
        return "episode", internal_id
    return "movie", internal_id


def _vod_cache_asset_key(source: CsoSource):
    asset_kind, internal_id = _vod_cache_asset_parts(source)
    return f"{asset_kind}:{internal_id}"


class VodHeadProbeStateStore:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._state: dict[str, VodHeadProbeStateEntry] | None = None

    async def _load_state(self) -> dict[str, VodHeadProbeStateEntry]:
        if self._state is not None:
            return self._state
        path = _vod_head_probe_state_path()
        payload: Any = {}
        if path.exists():
            try:
                payload = json.loads(await asyncio.to_thread(path.read_text, encoding="utf-8")) or {}
            except Exception:
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        cleaned: dict[str, VodHeadProbeStateEntry] = {}
        now_ts = int(time.time())
        for key, value in payload.items():
            if not isinstance(key, str) or not isinstance(value, dict):
                continue
            expires_at = convert_to_int(value.get("expires_at"), 0)
            if expires_at > 0 and expires_at < now_ts:
                continue
            cleaned[key] = {
                "expires_at": expires_at,
                "failure_reason": clean_text(value.get("failure_reason")) or "head_failed",
                "head_supported": bool(value.get("head_supported")),
                "last_failure_at": convert_to_int(value.get("last_failure_at"), 0),
            }
        self._state = cleaned
        return self._state

    async def _write_state(self, state: dict[str, VodHeadProbeStateEntry]):
        path = _vod_head_probe_state_path()
        await asyncio.to_thread(path.parent.mkdir, 0o755, True, True)
        payload = json.dumps(state, indent=2, sort_keys=True)
        await asyncio.to_thread(path.write_text, payload, encoding="utf-8")

    async def should_skip_head(self, source: CsoSource, upstream_url: str) -> bool:
        async with self._lock:
            state = await self._load_state()
            entry = state.get(_vod_head_probe_cache_key(source, upstream_url))
            if entry is None:
                return False
            return entry["head_supported"] is False

    async def mark_head_failed(self, source: CsoSource, upstream_url: str, reason: str):
        async with self._lock:
            state = await self._load_state()
            now_ts = int(time.time())
            state[_vod_head_probe_cache_key(source, upstream_url)] = {
                "head_supported": False,
                "last_failure_at": now_ts,
                "failure_reason": clean_text(reason) or "head_failed",
                "expires_at": now_ts + VOD_HEAD_PROBE_STATE_TTL_SECONDS,
            }
            await self._write_state(state)

    async def mark_head_supported(self, source: CsoSource, upstream_url: str):
        async with self._lock:
            state = await self._load_state()
            key = _vod_head_probe_cache_key(source, upstream_url)
            if key in state:
                state.pop(key, None)
                await self._write_state(state)


vod_head_probe_state_store = VodHeadProbeStateStore()


def _vod_cache_paths(source: CsoSource):
    asset_kind, internal_id = _vod_cache_asset_parts(source)
    final_path = VOD_CACHE_ROOT / asset_kind / str(internal_id)
    return final_path, final_path.with_name(f"{final_path.name}.part")


def _vod_content_type_for_source(source: CsoSource):
    extension = clean_key(source.container_extension)
    if extension:
        return content_type_for_media_path(extension)
    return None


class VodCacheManager:
    def __init__(self):
        self.entries = {}
        self.lock = asyncio.Lock()

    async def get(self, source: CsoSource):
        key = _vod_cache_asset_key(source)
        async with self.lock:
            entry = self.entries.get(key)
            if entry is not None:
                entry.touch()
            return entry

    async def get_or_create(self, source: CsoSource, upstream_url: str):
        key = _vod_cache_asset_key(source)
        async with self.lock:
            entry = self.entries.get(key)
            if entry is None:
                final_path, part_path = _vod_cache_paths(source)
                entry = VodCacheEntry(
                    key=key,
                    source=source,
                    upstream_url=clean_text(upstream_url),
                    final_path=final_path,
                    part_path=part_path,
                )
                self.entries[key] = entry
            else:
                entry.upstream_url = clean_text(upstream_url) or entry.upstream_url
                entry.source = source
                if entry.complete and entry.final_path.exists() and not entry.expected_size:
                    try:
                        entry.expected_size = int(entry.final_path.stat().st_size or 0)
                    except Exception:
                        entry.expected_size = None
            if entry.complete and not entry.content_type:
                entry.content_type = _vod_content_type_for_source(source)
            entry.touch()
            return entry

    async def import_existing_files(self):
        now_ts = time.time()
        imported = 0
        removed_parts = 0
        async with self.lock:
            for asset_kind in ("movie", "episode"):
                asset_dir = VOD_CACHE_ROOT / asset_kind
                if not asset_dir.exists() or not asset_dir.is_dir():
                    continue
                for path in sorted(asset_dir.iterdir()):
                    if not path.is_file():
                        continue
                    if path.suffix == ".part":
                        try:
                            path.unlink(missing_ok=True)
                            removed_parts += 1
                        except Exception:
                            logger.warning("Failed to remove orphaned VOD cache part file path=%s", path)
                        continue
                    file_name = clean_text(path.name)
                    if not file_name.isdigit():
                        continue
                    internal_id = int(file_name)
                    key = f"{asset_kind}:{internal_id}"
                    expected_size = 0
                    try:
                        expected_size = int(path.stat().st_size or 0)
                    except Exception:
                        expected_size = 0
                    if expected_size <= 0:
                        continue
                    source_type = "vod_movie" if asset_kind == "movie" else "vod_episode"
                    source = CsoSource(
                        id=internal_id,
                        source_type=source_type,
                        url="",
                        playlist_id=0,
                        internal_id=internal_id,
                    )
                    entry = self.entries.get(key)
                    if entry is None:
                        entry = VodCacheEntry(
                            key=key,
                            source=source,
                            upstream_url="",
                            final_path=path,
                            part_path=path.with_name(f"{path.name}.part"),
                        )
                        self.entries[key] = entry
                    else:
                        entry.source = source
                        entry.final_path = path
                        entry.part_path = path.with_name(f"{path.name}.part")
                    entry.expected_size = expected_size
                    entry.bytes_written = expected_size
                    entry.complete = True
                    entry.failed_reason = None
                    entry.metadata_headers = entry.metadata_headers or {}
                    entry.content_type = entry.content_type or _vod_content_type_for_source(source)
                    entry.last_access_ts = now_ts
                    imported += 1
        if imported or removed_parts:
            logger.info(
                "Imported existing VOD cache files imported=%s removed_orphan_parts=%s root=%s",
                imported,
                removed_parts,
                VOD_CACHE_ROOT,
            )
        return {"imported": imported, "removed_orphan_parts": removed_parts}

    async def attach_session(self, entry: VodCacheEntry):
        async with entry.state_lock:
            entry.active_sessions = int(entry.active_sessions or 0) + 1
            entry.touch()

    async def detach_session(self, entry: VodCacheEntry):
        async with entry.state_lock:
            entry.active_sessions = max(0, int(entry.active_sessions or 0) - 1)
            entry.touch()
            return

    async def cleanup(self, idle_seconds=VOD_CACHE_TTL_SECONDS):
        now_ts = time.time()
        async with self.lock:
            entries = list(self.entries.values())
        removed = 0
        for entry in entries:
            if entry.downloader_running or entry.active_readers > 0 or entry.active_sessions > 0:
                continue
            if (now_ts - float(entry.last_access_ts or 0)) < max(30, int(idle_seconds or 0)):
                continue
            removed_entry = await self._remove_entry(entry)
            if not removed_entry:
                continue
            removed += 1
        return removed

    async def purge_oldest_for_space(
        self, required_bytes: int, preserve_key: str | None = None
    ) -> dict[str, int | bool]:
        required = int(required_bytes or 0)
        if required <= 0:
            return {"has_space": True, "removed": 0, "freed_bytes": 0, "free_bytes": 0}

        usage = await asyncio.to_thread(shutil.disk_usage, str(VOD_CACHE_ROOT.parent))
        free_bytes = int(usage.free or 0)
        if free_bytes >= required:
            return {"has_space": True, "removed": 0, "freed_bytes": 0, "free_bytes": free_bytes}

        async with self.lock:
            candidates = sorted(self.entries.values(), key=lambda entry: float(entry.last_access_ts or 0))

        removed = 0
        freed_bytes = 0
        for entry in candidates:
            if preserve_key and entry.key == preserve_key:
                continue
            async with entry.state_lock:
                if entry.downloader_running or entry.active_readers > 0 or entry.active_sessions > 0:
                    continue
                estimated_bytes = 0
                if entry.final_path.exists():
                    try:
                        estimated_bytes += int(entry.final_path.stat().st_size or 0)
                    except Exception:
                        pass
                if entry.part_path.exists():
                    try:
                        estimated_bytes += int(entry.part_path.stat().st_size or 0)
                    except Exception:
                        pass
            removed_entry = await self._remove_entry(entry)
            if not removed_entry:
                continue
            removed += 1
            freed_bytes += estimated_bytes
            usage = await asyncio.to_thread(shutil.disk_usage, str(VOD_CACHE_ROOT.parent))
            free_bytes = int(usage.free or 0)
            if free_bytes >= required:
                logger.info(
                    "VOD cache eviction freed space removed=%s freed_bytes=%s free_bytes=%s required_bytes=%s",
                    removed,
                    freed_bytes,
                    free_bytes,
                    required,
                )
                return {
                    "has_space": True,
                    "removed": removed,
                    "freed_bytes": freed_bytes,
                    "free_bytes": free_bytes,
                }

        if removed > 0:
            logger.warning(
                "VOD cache eviction could not free enough space removed=%s freed_bytes=%s free_bytes=%s required_bytes=%s",
                removed,
                freed_bytes,
                free_bytes,
                required,
            )
        return {
            "has_space": free_bytes >= required,
            "removed": removed,
            "freed_bytes": freed_bytes,
            "free_bytes": free_bytes,
        }

    async def _remove_entry(self, entry: VodCacheEntry) -> bool:
        async with entry.state_lock:
            if entry.downloader_running or entry.active_readers > 0 or entry.active_sessions > 0:
                return False
            task = entry.download_task
            entry.download_task = None
        if task and not task.done():
            task.cancel()
            try:
                await task
            except BaseException:
                pass
        if entry.final_path.exists():
            await asyncio.to_thread(entry.final_path.unlink, True)
        if entry.part_path.exists():
            await asyncio.to_thread(entry.part_path.unlink, True)
        async with self.lock:
            current = self.entries.get(entry.key)
            if current is entry:
                self.entries.pop(entry.key, None)
        return True


vod_cache_manager = VodCacheManager()
