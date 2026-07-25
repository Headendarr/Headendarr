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
from .common import bounded_log_value, redacted_url_for_log
from .constants import (
    CSO_SEGMENT_CACHE_ROOT,
    VOD_CACHE_CHUNK_BYTES,
    VOD_CACHE_METADATA_TIMEOUT_SECONDS,
    VOD_CACHE_ROOT,
    VOD_CACHE_TTL_SECONDS,
    VOD_CHANNEL_SEGMENT_CACHE_ROOT,
    VOD_HEAD_PROBE_STATE_TTL_SECONDS,
)
from .sources import cso_source_from_vod_source
from .types import CsoSource, VodCacheEntry, VodHeadProbeStateEntry
from .vod_upstream import validate_vod_upstream_response


logger = logging.getLogger("cso")


def _vod_head_probe_state_path() -> Path:
    home_dir = os.environ.get("HOME_DIR") or os.path.expanduser("~")
    return Path(home_dir) / ".tvh_iptv_config" / "cache" / "vod_head_probe_state.json"


def _vod_head_probe_cache_key(source: CsoSource, upstream_url: str) -> str:
    source_id = int(source.id or 0)
    parsed = urlparse(upstream_url or "")
    source_host = clean_text(parsed.netloc)
    return f"{source.source_type}:{source.playlist_id}:{source_id}:{source_host}"


async def _probe_vod_cache_metadata(
    source: CsoSource,
    upstream_url: str,
    request_headers=None,
    require_body_validation: bool = False,
) -> dict[str, object]:
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
                redacted_url_for_log(upstream_url),
            )
        else:
            try:
                response = await session.request("HEAD", upstream_url, headers=headers, allow_redirects=True)
                try:
                    validation = validate_vod_upstream_response(
                        int(response.status or 0),
                        response.headers,
                        request_method="HEAD",
                    )
                    if validation.accepted:
                        size_header = clean_text(response.headers.get("Content-Length"))
                        if size_header.isdigit():
                            await vod_head_probe_state_store.mark_head_supported(source, upstream_url)
                            if not require_body_validation:
                                return {
                                    "size": int(size_header),
                                    "headers": dict(response.headers),
                                    "status": int(response.status or 200),
                                    "valid": True,
                                }
                finally:
                    await response.release()
            except Exception as exc:
                logger.info(
                    "VOD cache metadata HEAD probe failed source_id=%s upstream_url=%s error=%s",
                    source.id,
                    redacted_url_for_log(upstream_url),
                    exc,
                )
                await vod_head_probe_state_store.mark_head_failed(source, upstream_url, str(exc))
        response = await session.get(
            upstream_url,
            headers={**headers, "Range": "bytes=0-0"},
            allow_redirects=True,
        )
        try:
            prefix = await response.content.read(1024)
            validation = validate_vod_upstream_response(
                int(response.status or 0),
                response.headers,
                requested_offset=0,
                body_prefix=prefix,
            )
            total_size = validation.total_size
            if validation.accepted and not total_size and int(response.status or 0) == 200:
                content_length = clean_text(response.headers.get("Content-Length"))
                if content_length.isdigit():
                    total_size = int(content_length)
            if validation.accepted and total_size:
                return {
                    "size": int(total_size),
                    "headers": dict(response.headers),
                    "status": int(response.status or 206),
                    "valid": True,
                }
            if not validation.accepted:
                logger.warning(
                    "VOD upstream metadata response rejected source_id=%s status=%s content_type=%s "
                    "range_present=%s classification=%s upstream_url=%s",
                    source.id,
                    int(response.status or 0),
                    bounded_log_value(validation.content_type),
                    bool(clean_text(response.headers.get("Content-Range"))),
                    validation.classification,
                    redacted_url_for_log(upstream_url),
                )
                reason = (
                    "upstream_invalid_media_response"
                    if validation.classification != "upstream_status"
                    else f"upstream_status_{int(response.status or 0)}"
                )
                try:
                    from backend.source_media import persist_source_media_error

                    await persist_source_media_error(
                        source_id=source.id,
                        error_code=reason,
                        details={
                            "status": int(response.status or 0),
                            "content_type": validation.content_type,
                            "classification": validation.classification,
                        },
                        source_type=source.source_type,
                    )
                except Exception as exc:
                    logger.debug("Failed to persist VOD cache probe error: %s", exc)
                return {
                    "size": None,
                    "headers": dict(response.headers),
                    "status": int(response.status or 0),
                    "valid": False,
                    "reason": reason,
                }
            return {
                "size": None,
                "headers": dict(response.headers),
                "status": int(response.status or 0),
                "valid": True,
            }
        finally:
            await response.release()
    return {"size": None, "headers": {}, "status": 0, "valid": False}


async def probe_vod_upstream_response(
    source: CsoSource,
    upstream_url: str,
    request_headers=None,
) -> str | None:
    result = await _probe_vod_cache_metadata(
        source,
        upstream_url,
        request_headers=request_headers,
        require_body_validation=True,
    )
    reason = clean_text(result.get("reason"))
    if reason == "upstream_invalid_media_response":
        await vod_cache_manager.increment_runtime_counter("vod_upstream_invalid_responses_total")
    return reason or None


async def _vod_cache_has_space(required_bytes: int) -> bool:
    if required_bytes <= 0:
        return False
    usage = await asyncio.to_thread(shutil.disk_usage, str(VOD_CACHE_ROOT.parent))
    return int(usage.free or 0) >= int(required_bytes)


async def ensure_vod_cache_ready(
    entry: VodCacheEntry,
    request_headers=None,
    require_size=False,
    purpose: str = "background_warm",
) -> dict[str, object]:
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
        capacity_key = source_capacity_key(entry.source)
        probe_owner = f"vod-cache-probe:{entry.key}:{id(asyncio.current_task())}"
        reserved = await vod_cache_manager.reserve_capacity(
            capacity_key,
            source_capacity_limit(entry.source),
            probe_owner,
            probe_owner,
            purpose=purpose,
        )
        if not reserved:
            entry.failed_reason = "capacity_blocked"
            return {
                "cacheable": False,
                "size_known": False,
                "expected_size": None,
                "reason": "capacity_blocked",
            }
        try:
            probe = await _probe_vod_cache_metadata(
                entry.source,
                entry.upstream_url,
                request_headers=request_headers,
            )
        finally:
            await cso_capacity_registry.release(capacity_key, probe_owner, slot_id=probe_owner)
        expected_size = int(probe.get("size") or 0)
        if expected_size <= 0:
            probe_reason = clean_text(probe.get("reason"))
            if probe_reason == "upstream_invalid_media_response":
                await vod_cache_manager.increment_runtime_counter("vod_upstream_invalid_responses_total")
            entry.failed_reason = probe_reason or "size_unknown"
            if require_size:
                return {
                    "cacheable": False,
                    "size_known": False,
                    "expected_size": None,
                    "reason": entry.failed_reason,
                }
            return {
                "cacheable": False,
                "size_known": False,
                "expected_size": None,
                "reason": entry.failed_reason,
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


async def start_vod_cache_download(
    entry: VodCacheEntry,
    owner_key: str,
    request_headers=None,
    purpose: str = "background_warm",
) -> bool:
    capacity_key = source_capacity_key(entry.source)
    arbitration_lock = await vod_cache_manager.capacity_arbitration_lock(capacity_key)
    async with arbitration_lock:
        async with entry.state_lock:
            if entry.complete:
                return True
            if entry.downloader_running:
                return True
            if not entry.expected_size:
                return False
        reserved = await vod_cache_manager._reserve_or_preempt_locked(
            capacity_key,
            source_capacity_limit(entry.source),
            owner_key,
            owner_key,
            purpose,
        )
        if not reserved:
            async with entry.state_lock:
                entry.failed_reason = "capacity_blocked"
            return False
        async with entry.state_lock:
            entry.downloader_owner_key = owner_key
            entry.downloader_capacity_key = capacity_key
            entry.downloader_slot_id = owner_key
            entry.downloader_started_ts = time.time()
            entry.downloader_purpose = clean_text(purpose) or "background_warm"
            entry.preemption_requested = False
            entry.preemption_reason = None
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
                    return

                prefix = await asyncio.to_thread(response.raw.read, 1024, decode_content=True)
                validation = validate_vod_upstream_response(
                    status_code,
                    response.headers,
                    requested_offset=range_start,
                    body_prefix=prefix,
                    allow_restart_from_zero=True,
                )
                if not validation.accepted:
                    entry.failed_reason = "upstream_invalid_media_response"
                    await vod_cache_manager.increment_runtime_counter("vod_upstream_invalid_responses_total")
                    logger.warning(
                        "VOD cache upstream response rejected asset=%s source_id=%s status=%s content_type=%s "
                        "requested_offset=%s range_present=%s classification=%s upstream_url=%s",
                        entry.key,
                        entry.source.id,
                        status_code,
                        bounded_log_value(validation.content_type),
                        range_start,
                        bool(clean_text(response.headers.get("Content-Range"))),
                        validation.classification,
                        redacted_url_for_log(entry.upstream_url),
                    )
                    return

                if validation.restart_from_zero:
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
                if range_start > 0:
                    await vod_cache_manager.increment_runtime_counter("vod_cache_resumes_total")

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
                    if prefix:
                        await handle.write(prefix)
                        entry.bytes_written += len(prefix)
                        entry.progress_event.set()
                        entry.progress_event = asyncio.Event()
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
        entry.failed_reason = "preempted" if entry.preemption_requested else "cancelled"
        raise
    except Exception as exc:
        entry.failed_reason = f"download_failed:{exc}"
        logger.warning("VOD cache download failed asset=%s error=%s", entry.key, exc)
    finally:
        capacity_key = entry.downloader_capacity_key or source_capacity_key(entry.source)
        slot_id = entry.downloader_slot_id or owner_key
        await cso_capacity_registry.release(capacity_key, owner_key, slot_id=slot_id)
        async with entry.state_lock:
            entry.downloader_owner_key = None
            entry.downloader_capacity_key = None
            entry.downloader_slot_id = None
            entry.downloader_purpose = None
            entry.download_task = None
        entry.ready_event.set()
        entry.progress_event.set()


async def cleanup_vod_proxy_cache() -> int:
    return await vod_cache_manager.cleanup()


async def warm_vod_cache(
    candidate,
    upstream_url: str,
    episode=None,
    owner_key: str | None = None,
    request_headers=None,
) -> bool:
    if not candidate:
        return False
    source = await cso_source_from_vod_source(candidate, upstream_url)
    if not source or not source.url:
        return False
    entry = await vod_cache_manager.get_or_create(source, source.url)
    cache_meta = await ensure_vod_cache_ready(
        entry,
        request_headers=request_headers,
        purpose="background_warm",
    )
    if not cache_meta.get("cacheable"):
        return False
    owner = clean_text(owner_key) or f"vod-cache-warm-{source.id}"
    return await start_vod_cache_download(
        entry,
        owner,
        request_headers=request_headers,
        purpose="background_warm",
    )


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
    """Coordinate cache entries and source-capacity arbitration.

    Lock ordering is always the capacity-arbitration lock followed by an entry
    lock. The metadata-probe registry lock may be acquired while holding the
    arbitration lock, but probe cleanup never reacquires the arbitration lock.
    Downloader tasks are cancelled and awaited only after releasing the entry
    lock.
    """

    def __init__(self):
        self.entries = {}
        self.lock = asyncio.Lock()
        self._capacity_arbitration_locks = {}
        self._capacity_arbitration_locks_lock = asyncio.Lock()
        self._runtime_counters = {
            "vod_cache_preemptions_total": 0,
            "vod_cache_preemption_failures_total": 0,
            "vod_cache_resumes_total": 0,
            "vod_upstream_invalid_responses_total": 0,
            "vod_provider_capacity_rejections_total": 0,
        }
        self._runtime_counters_lock = asyncio.Lock()
        self._metadata_probes: dict[str, dict[str, tuple[asyncio.Task, str | int, float]]] = {}
        self._metadata_probes_lock = asyncio.Lock()
        self.vod_channel_segment_cache_root = Path(VOD_CHANNEL_SEGMENT_CACHE_ROOT)
        self.cso_segment_cache_root = Path(CSO_SEGMENT_CACHE_ROOT)
        self._vod_channel_cache_ledger: dict[str, dict[str, object]] = {}
        self._vod_channel_cache_ledger_lock = asyncio.Lock()

    @staticmethod
    def _vod_channel_cache_path_key(path: Path | str) -> str:
        return str(Path(path).resolve())

    async def register_vod_channel_cache_path(
        self,
        path: Path | str,
        owner_key: str,
        channel_id: int,
        cache_kind: str,
        scheduled_stop_ts: int = 0,
    ):
        cache_path = Path(path).resolve()
        path_key = self._vod_channel_cache_path_key(cache_path)
        now_ts = time.time()
        async with self._vod_channel_cache_ledger_lock:
            existing = self._vod_channel_cache_ledger.get(path_key)
            created_ts = float(existing.get("created_ts") or now_ts) if existing else now_ts
            self._vod_channel_cache_ledger[path_key] = {
                "path": cache_path,
                "owner_key": clean_text(owner_key),
                "channel_id": int(channel_id),
                "cache_kind": clean_key(cache_kind),
                "scheduled_stop_ts": int(scheduled_stop_ts or 0),
                "created_ts": created_ts,
                "last_used_ts": now_ts,
                "active": True,
            }

    async def release_vod_channel_cache_path(self, path: Path | str):
        path_key = self._vod_channel_cache_path_key(path)
        async with self._vod_channel_cache_ledger_lock:
            lease = self._vod_channel_cache_ledger.get(path_key)
            if lease is None:
                return
            lease["active"] = False
            lease["last_used_ts"] = time.time()

    @staticmethod
    def _remove_vod_channel_cache_tree(path: Path) -> tuple[int, int]:
        removed_files = 0
        removed_bytes = 0
        if not path.exists():
            return removed_files, removed_bytes
        for child in path.rglob("*"):
            if not child.is_file():
                continue
            removed_files += 1
            try:
                removed_bytes += int(child.stat().st_size)
            except OSError:
                pass
        try:
            shutil.rmtree(path, ignore_errors=True)
        except OSError:
            pass
        return removed_files, removed_bytes

    async def cleanup_vod_channel_segment_cache(
        self,
        reason: str = "periodic",
        force: bool = False,
    ) -> dict[str, int]:
        """Remove unowned 24/7 producer and stitched cache directories."""

        segment_root = self.vod_channel_segment_cache_root.resolve()
        stitched_root = self.cso_segment_cache_root.resolve()
        await asyncio.to_thread(segment_root.mkdir, 0o755, True, True)

        producer_paths = []
        for channel_path in segment_root.glob("channel-*"):
            if not channel_path.is_dir():
                continue
            producer_paths.extend(child.resolve() for child in channel_path.iterdir() if child.is_dir())
        stitched_paths = [path.resolve() for path in stitched_root.glob("cso-vod-channel-ingest-*") if path.is_dir()]
        candidate_paths = producer_paths + stitched_paths

        removed_paths = 0
        removed_files = 0
        removed_bytes = 0
        skipped_active = 0
        async with self._vod_channel_cache_ledger_lock:
            for cache_path in candidate_paths:
                path_key = self._vod_channel_cache_path_key(cache_path)
                lease = self._vod_channel_cache_ledger.get(path_key)
                if not force and lease is not None and bool(lease.get("active")):
                    skipped_active += 1
                    continue
                try:
                    path_files, path_bytes = await asyncio.to_thread(
                        self._remove_vod_channel_cache_tree,
                        cache_path,
                    )
                except Exception:
                    logger.warning(
                        "Failed to clean 24/7 VOD cache path reason=%s path=%s",
                        clean_text(reason) or "unspecified",
                        cache_path,
                        exc_info=True,
                    )
                    continue
                self._vod_channel_cache_ledger.pop(path_key, None)
                removed_paths += 1
                removed_files += path_files
                removed_bytes += path_bytes

            if force:
                self._vod_channel_cache_ledger.clear()
            else:
                missing_released_paths = [
                    path_key
                    for path_key, lease in self._vod_channel_cache_ledger.items()
                    if not bool(lease.get("active")) and not Path(lease["path"]).exists()
                ]
                for path_key in missing_released_paths:
                    self._vod_channel_cache_ledger.pop(path_key, None)

        for channel_path in segment_root.glob("channel-*"):
            try:
                await asyncio.to_thread(channel_path.rmdir)
            except OSError:
                pass

        if removed_paths or force:
            logger.info(
                "Cleaned 24/7 VOD cache reason=%s removed_paths=%s removed_files=%s removed_bytes=%s skipped_active=%s",
                clean_text(reason) or "unspecified",
                removed_paths,
                removed_files,
                removed_bytes,
                skipped_active,
            )
        return {
            "removed_paths": removed_paths,
            "removed_files": removed_files,
            "removed_bytes": removed_bytes,
            "skipped_active": skipped_active,
        }

    async def increment_runtime_counter(self, name: str) -> int:
        async with self._runtime_counters_lock:
            if name not in self._runtime_counters:
                raise ValueError(f"Unknown VOD runtime counter: {name}")
            self._runtime_counters[name] += 1
            return self._runtime_counters[name]

    async def runtime_metrics(self) -> dict[str, int]:
        async with self._runtime_counters_lock:
            return dict(self._runtime_counters)

    async def reserve_metadata_probe(
        self,
        capacity_key: str,
        limit: int,
        owner_key: str,
        slot_id: str | int,
        task: asyncio.Task,
    ) -> bool:
        arbitration_lock = await self.capacity_arbitration_lock(capacity_key)
        reserved = False
        try:
            async with arbitration_lock:
                reserved = await cso_capacity_registry.try_reserve(
                    capacity_key,
                    owner_key,
                    limit,
                    slot_id=slot_id,
                )
                if not reserved:
                    await self.increment_runtime_counter("vod_provider_capacity_rejections_total")
                    return False
                async with self._metadata_probes_lock:
                    probes = self._metadata_probes.setdefault(capacity_key, {})
                    probes[owner_key] = (task, slot_id, time.time())
                return True
        except BaseException:
            if reserved:
                await self.release_metadata_probe(capacity_key, owner_key, slot_id)
            raise

    async def release_metadata_probe(
        self,
        capacity_key: str,
        owner_key: str,
        slot_id: str | int,
    ):
        await cso_capacity_registry.release(capacity_key, owner_key, slot_id=slot_id)
        async with self._metadata_probes_lock:
            probes = self._metadata_probes.get(capacity_key)
            if probes is None:
                return
            probes.pop(owner_key, None)
            if not probes:
                self._metadata_probes.pop(capacity_key, None)

    async def _oldest_metadata_probe(
        self,
        capacity_key: str,
    ) -> tuple[str, asyncio.Task, str | int] | None:
        async with self._metadata_probes_lock:
            probes = self._metadata_probes.get(capacity_key, {})
            candidates = [
                (started_at, owner_key, task, slot_id)
                for owner_key, (task, slot_id, started_at) in probes.items()
                if not task.done()
            ]
        if not candidates:
            return None
        _, owner_key, task, slot_id = min(candidates, key=lambda item: (item[0], item[1]))
        return owner_key, task, slot_id

    async def capacity_arbitration_lock(self, capacity_key: str) -> asyncio.Lock:
        key = clean_text(capacity_key)
        async with self._capacity_arbitration_locks_lock:
            lock = self._capacity_arbitration_locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._capacity_arbitration_locks[key] = lock
            return lock

    async def _eligible_preemption_victims(self, capacity_key: str) -> list[VodCacheEntry]:
        async with self.lock:
            entries = list(self.entries.values())
        eligible = []
        for entry in entries:
            async with entry.state_lock:
                if (
                    entry.complete
                    or not entry.downloader_running
                    or entry.downloader_capacity_key != capacity_key
                    or entry.active_sessions > 0
                    or entry.active_readers > 0
                    or entry.waiting_consumers > 0
                    or entry.preemption_requested
                ):
                    continue
                eligible.append(entry)
        return sorted(
            eligible,
            key=lambda entry: (
                float(entry.last_consumer_detach_ts or 0),
                float(entry.downloader_started_ts or 0),
                entry.key,
            ),
        )

    async def has_preemptible_capacity(self, capacity_key: str) -> bool:
        arbitration_lock = await self.capacity_arbitration_lock(capacity_key)
        async with arbitration_lock:
            if await self._oldest_metadata_probe(capacity_key) is not None:
                return True
            return bool(await self._eligible_preemption_victims(capacity_key))

    async def _reserve_or_preempt_locked(
        self,
        capacity_key: str,
        limit: int,
        owner_key: str,
        slot_id: str | int,
        purpose: str,
    ) -> bool:
        usage_before = await cso_capacity_registry.get_usage(capacity_key)
        reserved = await cso_capacity_registry.try_reserve(capacity_key, owner_key, limit, slot_id=slot_id)
        if reserved or clean_key(purpose) != "interactive_playback":
            if not reserved:
                await self.increment_runtime_counter("vod_provider_capacity_rejections_total")
                logger.info(
                    "VOD provider capacity rejected capacity_key=%s limit=%s allocations=%s external=%s "
                    "requester_purpose=%s",
                    bounded_log_value(capacity_key),
                    int(limit or 0),
                    int(usage_before.get("allocations") or 0),
                    int(usage_before.get("external") or 0),
                    bounded_log_value(purpose),
                )
            return reserved
        if max(0, int(usage_before.get("total") or 0) - 1) < max(0, int(limit or 0)):
            metadata_probe = await self._oldest_metadata_probe(capacity_key)
            if metadata_probe is not None:
                probe_owner, probe_task, probe_slot = metadata_probe
                logger.info(
                    "VOD metadata probe yielding capacity to interactive playback capacity_key=%s",
                    bounded_log_value(capacity_key),
                )
                probe_task.cancel()
                try:
                    await probe_task
                except BaseException:
                    pass
                if await cso_capacity_registry.has_reservation(
                    capacity_key,
                    probe_owner,
                    slot_id=probe_slot,
                ):
                    await self.release_metadata_probe(capacity_key, probe_owner, probe_slot)
                reserved = await cso_capacity_registry.try_reserve(
                    capacity_key,
                    owner_key,
                    limit,
                    slot_id=slot_id,
                )
                if not reserved:
                    await self.increment_runtime_counter("vod_provider_capacity_rejections_total")
                return reserved
        if max(0, int(usage_before.get("total") or 0) - 1) >= max(0, int(limit or 0)):
            logger.info(
                "VOD cache preemption skipped because one release cannot satisfy capacity "
                "capacity_key=%s limit=%s allocations=%s external=%s requester_purpose=%s",
                bounded_log_value(capacity_key),
                int(limit or 0),
                int(usage_before.get("allocations") or 0),
                int(usage_before.get("external") or 0),
                bounded_log_value(purpose),
            )
            await self.increment_runtime_counter("vod_provider_capacity_rejections_total")
            return False

        victims = await self._eligible_preemption_victims(capacity_key)
        if not victims:
            await self.increment_runtime_counter("vod_provider_capacity_rejections_total")
            return False
        victim = victims[0]
        async with victim.state_lock:
            task = victim.download_task
            victim_still_eligible = (
                not victim.complete
                and task is not None
                and not task.done()
                and victim.downloader_capacity_key == capacity_key
                and victim.active_sessions == 0
                and victim.active_readers == 0
                and victim.waiting_consumers == 0
                and not victim.preemption_requested
            )
            if victim_still_eligible:
                victim.preemption_requested = True
                victim.preemption_reason = "interactive_playback"
                victim.failed_reason = "preempting"
                victim.progress_event.set()
            retained_bytes = int(victim.bytes_written or 0)
            victim_sessions = int(victim.active_sessions or 0)
            victim_readers = int(victim.active_readers or 0)
            victim_waiters = int(victim.waiting_consumers or 0)
            victim_owner = victim.downloader_owner_key
            victim_slot = victim.downloader_slot_id
        if not victim_still_eligible:
            reserved = await cso_capacity_registry.try_reserve(capacity_key, owner_key, limit, slot_id=slot_id)
            if not reserved:
                await self.increment_runtime_counter("vod_provider_capacity_rejections_total")
            return reserved
        logger.info(
            "VOD cache preemption requested capacity_key=%s requester_purpose=%s victim=%s "
            "active_sessions=%s active_readers=%s waiting_consumers=%s retained_bytes=%s",
            bounded_log_value(capacity_key),
            bounded_log_value(purpose),
            bounded_log_value(victim.key),
            victim_sessions,
            victim_readers,
            victim_waiters,
            retained_bytes,
        )
        task.cancel()
        try:
            await task
        except BaseException:
            pass
        if victim_owner and await cso_capacity_registry.has_reservation(
            capacity_key, victim_owner, slot_id=victim_slot
        ):
            await cso_capacity_registry.release(capacity_key, victim_owner, slot_id=victim_slot)
        async with victim.state_lock:
            if victim.download_task is task:
                victim.download_task = None
                victim.downloader_owner_key = None
                victim.downloader_capacity_key = None
                victim.downloader_slot_id = None
                victim.downloader_purpose = None
                victim.failed_reason = "preempted"
        if victim_owner and await cso_capacity_registry.has_reservation(
            capacity_key, victim_owner, slot_id=victim_slot
        ):
            logger.warning(
                "VOD cache preemption release unconfirmed capacity_key=%s victim=%s",
                bounded_log_value(capacity_key),
                bounded_log_value(victim.key),
            )
            await self.increment_runtime_counter("vod_cache_preemption_failures_total")
            await self.increment_runtime_counter("vod_provider_capacity_rejections_total")
            return False
        reserved = await cso_capacity_registry.try_reserve(capacity_key, owner_key, limit, slot_id=slot_id)
        await self.increment_runtime_counter(
            "vod_cache_preemptions_total" if reserved else "vod_cache_preemption_failures_total"
        )
        if not reserved:
            await self.increment_runtime_counter("vod_provider_capacity_rejections_total")
        logger.info(
            "VOD cache preemption completed capacity_key=%s victim=%s retained_bytes=%s replacement_reserved=%s",
            bounded_log_value(capacity_key),
            bounded_log_value(victim.key),
            retained_bytes,
            reserved,
        )
        return reserved

    async def reserve_capacity(
        self,
        capacity_key: str,
        limit: int,
        owner_key: str,
        slot_id: str | int,
        purpose: str = "interactive_playback",
    ) -> bool:
        arbitration_lock = await self.capacity_arbitration_lock(capacity_key)
        async with arbitration_lock:
            return await self._reserve_or_preempt_locked(
                capacity_key,
                limit,
                owner_key,
                slot_id,
                purpose,
            )

    async def get(self, source: CsoSource) -> VodCacheEntry | None:
        key = _vod_cache_asset_key(source)
        async with self.lock:
            entry = self.entries.get(key)
            if entry is not None:
                entry.touch()
            return entry

    async def get_or_create(self, source: CsoSource, upstream_url: str) -> VodCacheEntry:
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

    async def import_existing_files(self) -> dict[str, int]:
        now_ts = time.time()
        imported = 0
        retained_parts = 0
        async with self.lock:
            for asset_kind in ("movie", "episode"):
                asset_dir = VOD_CACHE_ROOT / asset_kind
                if not asset_dir.exists() or not asset_dir.is_dir():
                    continue
                for path in sorted(asset_dir.iterdir()):
                    if not path.is_file():
                        continue
                    if path.suffix == ".part":
                        retained_parts += 1
                        file_name = clean_text(path.stem)
                        if not file_name.isdigit():
                            continue
                        internal_id = int(file_name)
                        key = f"{asset_kind}:{internal_id}"
                        final_path = path.with_name(file_name)
                        if final_path.exists():
                            continue
                        try:
                            partial_size = int(path.stat().st_size or 0)
                            last_access_ts = float(path.stat().st_mtime or now_ts)
                        except Exception:
                            partial_size = 0
                            last_access_ts = now_ts
                        if partial_size <= 0:
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
                                final_path=final_path,
                                part_path=path,
                            )
                            self.entries[key] = entry
                        entry.bytes_written = partial_size
                        entry.complete = False
                        entry.failed_reason = "paused"
                        entry.last_access_ts = last_access_ts
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
        if imported or retained_parts:
            logger.info(
                "Imported existing VOD cache files imported=%s retained_partial_files=%s root=%s",
                imported,
                retained_parts,
                VOD_CACHE_ROOT,
            )
        return {"imported": imported, "retained_partial_files": retained_parts}

    async def attach_session(self, entry: VodCacheEntry):
        capacity_key = source_capacity_key(entry.source)
        arbitration_lock = await self.capacity_arbitration_lock(capacity_key)
        async with arbitration_lock:
            async with entry.state_lock:
                entry.active_sessions = int(entry.active_sessions or 0) + 1
                entry.last_consumer_attach_ts = time.time()
                entry.touch()

    async def detach_session(self, entry: VodCacheEntry):
        capacity_key = source_capacity_key(entry.source)
        arbitration_lock = await self.capacity_arbitration_lock(capacity_key)
        async with arbitration_lock:
            async with entry.state_lock:
                entry.active_sessions = max(0, int(entry.active_sessions or 0) - 1)
                entry.last_consumer_detach_ts = time.time()
                entry.touch()

    async def set_waiting(self, entry: VodCacheEntry, waiting: bool):
        capacity_key = source_capacity_key(entry.source)
        arbitration_lock = await self.capacity_arbitration_lock(capacity_key)
        async with arbitration_lock:
            async with entry.state_lock:
                if waiting:
                    entry.waiting_consumers = int(entry.waiting_consumers or 0) + 1
                else:
                    entry.waiting_consumers = max(0, int(entry.waiting_consumers or 0) - 1)

    async def set_reader_active(self, entry: VodCacheEntry, active: bool):
        capacity_key = source_capacity_key(entry.source)
        arbitration_lock = await self.capacity_arbitration_lock(capacity_key)
        async with arbitration_lock:
            async with entry.state_lock:
                if active:
                    entry.active_readers = int(entry.active_readers or 0) + 1
                else:
                    entry.active_readers = max(0, int(entry.active_readers or 0) - 1)
                entry.touch()

    async def cleanup(self, idle_seconds: int = VOD_CACHE_TTL_SECONDS) -> int:
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
        channel_cleanup = await self.cleanup_vod_channel_segment_cache()
        removed += int(channel_cleanup["removed_paths"])
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
