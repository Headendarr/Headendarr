import asyncio
import logging
from typing import Any

import aiofiles
import aiohttp
import requests
import urllib3

from backend.hls_multiplexer import get_header_value
from backend.utils import clean_key, clean_text

from .capacity import cso_capacity_registry, source_capacity_key, source_capacity_limit
from .constants import VOD_CACHE_CHUNK_BYTES
from .live_ingest import resolve_cso_ingest_headers, resolve_cso_ingest_user_agent
from .types import CsoSource
from .vod_cache import ensure_vod_cache_ready, start_vod_cache_download, vod_cache_manager


logger = logging.getLogger("cso")


def _build_vod_local_response_headers(
    total_size: int, metadata_headers=None, start=0, end=None, include_length=True, force_content_range=False
):
    headers = {}
    meta = dict(metadata_headers or {})
    for key in ("Content-Type", "Cache-Control", "Content-Disposition", "ETag", "Last-Modified"):
        value = clean_text(meta.get(key))
        if value:
            headers[key] = value
    headers["Accept-Ranges"] = "bytes"
    if end is None:
        end = max(0, int(total_size or 0) - 1)
    if include_length:
        headers["Content-Length"] = str(max(0, int(end) - int(start) + 1))
    if force_content_range or start > 0 or end < max(0, int(total_size or 0) - 1):
        headers["Content-Range"] = f"bytes {int(start)}-{int(end)}/{int(total_size)}"
    return headers


def filter_vod_proxy_request_headers(request_headers, source: CsoSource):
    headers = {}
    hop_by_hop = {
        "host",
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        "content-length",
    }
    allowed_passthrough = {"range", "if-range"}
    for key, value in (resolve_cso_ingest_headers(source) or {}).items():
        key_name = str(key or "").strip()
        if not key_name or clean_key(key_name) in hop_by_hop:
            continue
        text_value = clean_text(value)
        if text_value:
            headers[key_name] = text_value
    for key, value in (request_headers or {}).items():
        key_name = str(key or "").strip()
        lowered = clean_key(key_name)
        if not key_name or lowered in hop_by_hop or lowered not in allowed_passthrough:
            continue
        headers[key_name] = str(value or "")
    user_agent = resolve_cso_ingest_user_agent(None, source)
    if user_agent and "User-Agent" not in headers:
        headers["User-Agent"] = user_agent
    return headers


def _parse_range_request(range_header: str | None, total_size: int | None = None):
    text = clean_text(range_header)
    if not text or not text.lower().startswith("bytes="):
        return None
    spec = text[6:].strip()
    if "," in spec or "-" not in spec:
        return None
    start_text, end_text = spec.split("-", 1)
    start = None
    end = None
    if start_text.strip():
        if not start_text.strip().isdigit():
            return None
        start = int(start_text.strip())
    if end_text.strip():
        if not end_text.strip().isdigit():
            return None
        end = int(end_text.strip())
    if total_size is not None:
        if start is None:
            suffix = int(end or 0)
            if suffix <= 0:
                return None
            if suffix >= total_size:
                start = 0
            else:
                start = max(0, total_size - suffix)
            end = max(0, total_size - 1)
        else:
            if start >= total_size:
                return {"unsatisfied": True, "start": start, "end": None}
            if end is None or end >= total_size:
                end = total_size - 1
        if end is not None and start is not None and end < start:
            return None
    return {"start": start, "end": end, "raw": text}


def _is_from_start_request(request_headers=None):
    parsed = _parse_range_request(get_header_value(request_headers, "Range"))
    if parsed is None:
        return True
    start = parsed.get("start")
    return start in {None, 0}


def _sanitise_proxy_accept_ranges(value):
    text = clean_text(value)
    if not text:
        return None
    return "bytes" if "bytes" in text.lower() else None


def proxy_response_headers(status_code, upstream_headers, request_headers=None):
    response_status = int(status_code or 200)
    client_requested_range = bool(get_header_value(request_headers, "Range"))
    headers = {}

    content_type = clean_text(get_header_value(upstream_headers, "Content-Type"))
    if content_type:
        headers["Content-Type"] = content_type

    cache_control = clean_text(get_header_value(upstream_headers, "Cache-Control"))
    if cache_control:
        headers["Cache-Control"] = cache_control

    content_disposition = clean_text(get_header_value(upstream_headers, "Content-Disposition"))
    if content_disposition:
        headers["Content-Disposition"] = content_disposition

    etag = clean_text(get_header_value(upstream_headers, "ETag"))
    if etag:
        headers["ETag"] = etag

    last_modified = clean_text(get_header_value(upstream_headers, "Last-Modified"))
    if last_modified:
        headers["Last-Modified"] = last_modified

    accept_ranges = _sanitise_proxy_accept_ranges(get_header_value(upstream_headers, "Accept-Ranges"))
    content_range = clean_text(get_header_value(upstream_headers, "Content-Range"))
    content_length = clean_text(get_header_value(upstream_headers, "Content-Length"))

    if accept_ranges:
        headers["Accept-Ranges"] = accept_ranges
    elif content_range or client_requested_range or response_status in {206, 416}:
        headers["Accept-Ranges"] = "bytes"

    if response_status in {206, 416} and content_range:
        headers["Content-Range"] = content_range
        if content_length:
            headers["Content-Length"] = content_length
    elif response_status not in {204, 304} and content_length:
        headers["Content-Length"] = content_length

    return headers


class VodProxySession:
    def __init__(self, key, source: CsoSource, upstream_url: str, request_headers=None):
        self.key = str(key)
        self.source = source
        self.upstream_url = clean_text(upstream_url)
        self.request_headers = dict(request_headers or {})
        self.timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_connect=15, sock_read=None)
        self.http_session = None
        self.response = None
        self.blocking_session = None
        self.blocking_response = None
        self.blocking_iterator = None
        self.running = False
        self.capacity_key = source_capacity_key(source)
        self.capacity_limit = source_capacity_limit(source)
        self.owner_key = self.key
        self.status_code = 0
        self.content_type = None
        self.response_headers = {}
        self.last_error = None
        self.lock = asyncio.Lock()
        self.first_chunk_logged = False
        self.cache_entry = None
        self.local_only = False
        self.local_start = 0
        self.local_end = None
        self.local_size = None
        self.direct_owner_key = f"{self.key}:direct"
        self.cache_owner_key = f"{self.key}:cache"
        self.cache_session_attached = False
        self.direct_next_offset = 0
        self.requested_end = None
        self.direct_retry_attempts = 0
        self.max_direct_retry_attempts = 1

    async def start(self):
        startup_failed = False
        async with self.lock:
            if self.running:
                return True
            try:
                range_header = get_header_value(self.request_headers, "Range")
                self.cache_entry = await vod_cache_manager.get_or_create(self.source, self.upstream_url)
                await vod_cache_manager.attach_session(self.cache_entry)
                self.cache_session_attached = True
                self.cache_entry.touch()
                cache_meta = await ensure_vod_cache_ready(self.cache_entry, request_headers=self.request_headers)
                from_start = _is_from_start_request(self.request_headers)
                parsed_range = _parse_range_request(range_header, total_size=self.cache_entry.expected_size)
                self.direct_next_offset = int(parsed_range.get("start") or 0) if parsed_range else 0
                self.requested_end = (
                    int(parsed_range.get("end")) if parsed_range and parsed_range.get("end") is not None else None
                )

                if (
                    self.cache_entry.complete
                    and self.cache_entry.expected_size
                    and self.cache_entry.final_path.exists()
                ):
                    self.local_only = True
                    self.local_size = int(self.cache_entry.expected_size)
                    if parsed_range and parsed_range.get("unsatisfied"):
                        self.status_code = 416
                        self.content_type = self.cache_entry.content_type
                        self.response_headers = _build_vod_local_response_headers(
                            self.local_size,
                            metadata_headers=self.cache_entry.metadata_headers,
                            start=0,
                            end=max(0, self.local_size - 1),
                            include_length=False,
                        )
                        self.response_headers["Content-Range"] = f"bytes */{self.local_size}"
                    else:
                        start = int(parsed_range.get("start") or 0) if parsed_range else 0
                        end = (
                            int(parsed_range.get("end"))
                            if parsed_range and parsed_range.get("end") is not None
                            else max(0, self.local_size - 1)
                        )
                        self.local_start = start
                        self.local_end = end
                        self.status_code = 206 if parsed_range else 200
                        self.content_type = self.cache_entry.content_type
                        self.response_headers = _build_vod_local_response_headers(
                            self.local_size,
                            metadata_headers=self.cache_entry.metadata_headers,
                            start=start,
                            end=end,
                            force_content_range=bool(parsed_range),
                        )
                    self.running = True
                    logger.info(
                        "VOD proxy session serving local cache key=%s source_id=%s range=%s path=%s",
                        self.key,
                        getattr(self.source, "id", None),
                        range_header or None,
                        self.cache_entry.final_path,
                    )
                    return True
                if self.cache_entry.complete and not self.cache_entry.final_path.exists():
                    logger.warning(
                        "VOD cache entry marked complete but file is missing; falling back key=%s source_id=%s path=%s",
                        self.key,
                        getattr(self.source, "id", None),
                        self.cache_entry.final_path,
                    )
                    self.cache_entry.complete = False
                    self.cache_entry.bytes_written = 0

                if from_start and cache_meta.get("cacheable") and self.cache_entry.expected_size:
                    started_cache = await start_vod_cache_download(
                        self.cache_entry,
                        self.cache_owner_key,
                        request_headers=self.request_headers,
                    )
                    if started_cache:
                        await asyncio.wait_for(self.cache_entry.ready_event.wait(), timeout=15)
                        if self.cache_entry.failed_reason and not self.cache_entry.complete:
                            logger.warning(
                                "VOD cache start failed; falling back to direct proxy key=%s source_id=%s reason=%s",
                                self.key,
                                getattr(self.source, "id", None),
                                self.cache_entry.failed_reason,
                            )
                        else:
                            self.local_only = True
                            self.local_start = 0
                            self.local_end = max(0, int(self.cache_entry.expected_size or 0) - 1)
                            self.local_size = int(self.cache_entry.expected_size or 0)
                            self.status_code = 200
                            self.content_type = self.cache_entry.content_type
                            self.response_headers = _build_vod_local_response_headers(
                                self.local_size,
                                metadata_headers=self.cache_entry.metadata_headers,
                                start=0,
                                end=self.local_end,
                            )
                            self.running = True
                            logger.info(
                                "VOD proxy session started local-tail key=%s source_id=%s status=%s content_type=%s upstream_url=%s",
                                self.key,
                                getattr(self.source, "id", None),
                                self.status_code,
                                self.content_type,
                                self.upstream_url,
                            )
                            return True
                    else:
                        logger.warning(
                            "VOD cache downloader unavailable for start-of-file playback key=%s source_id=%s reason=%s",
                            self.key,
                            getattr(self.source, "id", None),
                            self.cache_entry.failed_reason,
                        )

                reserved = await cso_capacity_registry.try_reserve(
                    self.capacity_key,
                    self.owner_key,
                    self.capacity_limit,
                    slot_id=self.direct_owner_key,
                )
                if not reserved:
                    self.last_error = "capacity_blocked"
                    return False

                proxy_headers = filter_vod_proxy_request_headers(self.request_headers, self.source)
                if not get_header_value(proxy_headers, "Range"):
                    proxy_headers["Range"] = "bytes=0-"
                self.blocking_session = requests.Session()
                self.blocking_response = await asyncio.to_thread(
                    lambda: self.blocking_session.get(
                        self.upstream_url,
                        headers=proxy_headers,
                        allow_redirects=True,
                        stream=True,
                        timeout=(15, 30),
                    )
                )
                self.blocking_iterator = self.blocking_response.iter_content(chunk_size=64 * 1024)
                self.status_code = int(self.blocking_response.status_code or 502)
                self.content_type = clean_text(self.blocking_response.headers.get("Content-Type")) or None
                self.response_headers = proxy_response_headers(
                    self.status_code,
                    self.blocking_response.headers,
                    request_headers=self.request_headers,
                )
                self.running = True
                logger.info(
                    "VOD proxy session started key=%s source_id=%s status=%s client_range=%s content_type=%s content_range=%s accept_ranges=%s upstream_url=%s",
                    self.key,
                    getattr(self.source, "id", None),
                    self.status_code,
                    range_header or None,
                    self.content_type,
                    clean_text(self.blocking_response.headers.get("Content-Range")) or None,
                    clean_text(self.blocking_response.headers.get("Accept-Ranges")) or None,
                    self.upstream_url,
                )
                if (
                    not from_start
                    and cache_meta.get("cacheable")
                    and self.cache_entry
                    and not self.cache_entry.downloader_running
                ):
                    await start_vod_cache_download(
                        self.cache_entry,
                        self.cache_owner_key,
                        request_headers=self.request_headers,
                    )
                return True
            except Exception as exc:
                self.last_error = f"proxy_start_failed:{exc}"
                logger.warning(
                    "VOD proxy session failed to start key=%s source_id=%s error=%s",
                    self.key,
                    getattr(self.source, "id", None),
                    exc,
                )
                startup_failed = True
        if startup_failed:
            await self.stop(force=True)
        return False

    async def _close_direct_upstream(self):
        blocking_response = self.blocking_response
        self.blocking_response = None
        blocking_session = self.blocking_session
        self.blocking_session = None
        self.blocking_iterator = None
        try:
            if blocking_response is not None:
                await asyncio.to_thread(blocking_response.close)
        except Exception:
            pass
        try:
            if blocking_session is not None:
                await asyncio.to_thread(blocking_session.close)
        except Exception:
            pass

    async def _switch_to_local_from_offset(self, offset: int):
        entry = self.cache_entry
        if entry is None:
            return False
        current_written = int(entry.expected_size or 0) if entry.complete else int(entry.bytes_written or 0)
        if not entry.complete and current_written <= int(offset):
            return False
        self.local_only = True
        self.local_start = int(offset)
        self.local_end = self.requested_end
        if entry.complete and entry.expected_size:
            self.local_size = int(entry.expected_size)
        await self._close_direct_upstream()
        logger.info(
            "VOD proxy session switched to local cache key=%s source_id=%s offset=%s complete=%s",
            self.key,
            getattr(self.source, "id", None),
            int(offset),
            bool(entry.complete),
        )
        return True

    async def _retry_direct_upstream_from_offset(self, offset: int):
        if self.direct_retry_attempts >= self.max_direct_retry_attempts:
            return False
        proxy_headers = filter_vod_proxy_request_headers(self.request_headers, self.source)
        proxy_headers["Range"] = f"bytes={max(0, int(offset))}-"
        await self._close_direct_upstream()
        session = requests.Session()
        try:
            response = await asyncio.to_thread(
                lambda: session.get(
                    self.upstream_url,
                    headers=proxy_headers,
                    allow_redirects=True,
                    stream=True,
                    timeout=(15, 30),
                )
            )
        except Exception:
            try:
                await asyncio.to_thread(session.close)
            except Exception:
                pass
            raise
        status_code = int(response.status_code or 502)
        if status_code >= 400:
            try:
                await asyncio.to_thread(response.close)
            except Exception:
                pass
            try:
                await asyncio.to_thread(session.close)
            except Exception:
                pass
            self.last_error = f"proxy_retry_status_{status_code}"
            return False
        self.blocking_session = session
        self.blocking_response = response
        self.blocking_iterator = response.iter_content(chunk_size=64 * 1024)
        self.direct_retry_attempts += 1
        logger.warning(
            "VOD proxy upstream retry key=%s source_id=%s offset=%s status=%s attempt=%s",
            self.key,
            getattr(self.source, "id", None),
            int(offset),
            status_code,
            self.direct_retry_attempts,
        )
        return True

    async def iter_bytes(self):
        try:
            if self.local_only and self.cache_entry is not None:
                async for chunk in self._iter_local_bytes():
                    yield chunk
                return
            if self.blocking_response is None:
                return
            while True:
                if not self.running:
                    break
                try:
                    chunk = await asyncio.to_thread(next, self.blocking_iterator, None)
                except (
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ConnectionError,
                    urllib3.exceptions.ProtocolError,
                    ConnectionResetError,
                ) as exc:
                    if not self.running:
                        break
                    logger.warning(
                        "VOD proxy upstream read interrupted key=%s source_id=%s offset=%s error=%s",
                        self.key,
                        getattr(self.source, "id", None),
                        int(self.direct_next_offset),
                        exc,
                    )
                    if await self._switch_to_local_from_offset(self.direct_next_offset):
                        async for local_chunk in self._iter_local_bytes():
                            yield local_chunk
                        return
                    retried = await self._retry_direct_upstream_from_offset(self.direct_next_offset)
                    if retried:
                        continue
                    self.last_error = f"proxy_read_failed:{exc}"
                    break
                if chunk:
                    if not self.first_chunk_logged:
                        logger.info(
                            "VOD proxy first chunk key=%s source_id=%s bytes=%s status=%s",
                            self.key,
                            getattr(self.source, "id", None),
                            len(chunk),
                            self.status_code,
                        )
                        self.first_chunk_logged = True
                    self.direct_next_offset += len(chunk)
                    yield chunk
                else:
                    break
        finally:
            await self.stop(force=True)

    async def _iter_local_bytes(self):
        entry = self.cache_entry
        if entry is None:
            return
        if self.status_code == 416:
            return
        async with entry.state_lock:
            entry.active_readers += 1
            entry.touch()
        target_path = entry.final_path if entry.complete and entry.final_path.exists() else entry.part_path
        offset = int(self.local_start or 0)
        final_end = int(self.local_end) if self.local_end is not None else None
        try:
            while self.running:
                current_written = int(entry.expected_size or 0) if entry.complete else int(entry.bytes_written or 0)
                if final_end is not None and offset > final_end:
                    break
                if offset >= current_written and not entry.complete:
                    if entry.failed_reason and not entry.downloader_running:
                        break
                    await entry.progress_event.wait()
                    continue
                available_end = current_written - 1
                if final_end is not None:
                    available_end = min(available_end, final_end)
                if available_end < offset:
                    if entry.complete:
                        break
                    if entry.failed_reason and not entry.downloader_running:
                        break
                    await entry.progress_event.wait()
                    continue
                if not target_path.exists():
                    target_path = entry.final_path if entry.complete and entry.final_path.exists() else entry.part_path
                async with aiofiles.open(target_path, "rb") as handle:
                    await handle.seek(offset)
                    while self.running:
                        current_written = (
                            int(entry.expected_size or 0) if entry.complete else int(entry.bytes_written or 0)
                        )
                        max_end = current_written - 1
                        if final_end is not None:
                            max_end = min(max_end, final_end)
                        remaining = max_end - offset + 1
                        if remaining <= 0:
                            break
                        chunk = await handle.read(min(VOD_CACHE_CHUNK_BYTES, remaining))
                        if not chunk:
                            break
                        if not self.first_chunk_logged:
                            logger.info(
                                "VOD proxy first local chunk key=%s source_id=%s bytes=%s status=%s",
                                self.key,
                                getattr(self.source, "id", None),
                                len(chunk),
                                self.status_code,
                            )
                            self.first_chunk_logged = True
                        offset += len(chunk)
                        entry.touch()
                        yield chunk
                        if final_end is not None and offset > final_end:
                            return
                        if not self.running:
                            return
                if entry.complete and offset >= int(entry.expected_size or 0):
                    break
        finally:
            async with entry.state_lock:
                entry.active_readers = max(0, int(entry.active_readers or 0) - 1)
                entry.touch()

    async def stop(self, force=False):
        async with self.lock:
            if (
                not self.running
                and self.response is None
                and self.http_session is None
                and self.blocking_response is None
                and self.blocking_session is None
            ):
                return
            self.running = False
            response = self.response
            self.response = None
            http_session = self.http_session
            self.http_session = None
            blocking_response = self.blocking_response
            self.blocking_response = None
            blocking_session = self.blocking_session
            self.blocking_session = None
            self.blocking_iterator = None
        try:
            if response is not None:
                response.close()
        except Exception:
            pass
        try:
            if http_session is not None:
                await http_session.close()
        except Exception:
            pass
        try:
            if blocking_response is not None:
                await asyncio.to_thread(blocking_response.close)
        except Exception:
            pass
        try:
            if blocking_session is not None:
                await asyncio.to_thread(blocking_session.close)
        except Exception:
            pass
        logger.info(
            "VOD proxy session stopped key=%s source_id=%s status=%s",
            self.key,
            getattr(self.source, "id", None),
            self.status_code,
        )
        await cso_capacity_registry.release(self.capacity_key, self.owner_key, slot_id=self.direct_owner_key)
        if self.cache_entry is not None and self.cache_session_attached:
            await vod_cache_manager.detach_session(self.cache_entry)
            self.cache_session_attached = False
        await vod_proxy_session_manager.remove(self.key)


class VodProxySessionManager:
    def __init__(self):
        self.sessions = {}
        self.lock = asyncio.Lock()

    async def create(self, key, source: CsoSource, upstream_url: str, request_headers=None):
        session = VodProxySession(key, source, upstream_url, request_headers=request_headers)
        async with self.lock:
            self.sessions[str(key)] = session
        return session

    async def remove(self, key):
        async with self.lock:
            self.sessions.pop(str(key), None)


vod_proxy_session_manager = VodProxySessionManager()

_active_vod_proxy_output_disconnects: dict[str, Any] = {}
_active_vod_proxy_output_disconnects_lock = asyncio.Lock()


async def register_vod_proxy_output_disconnect(connection_id: str, disconnect_cb) -> None:
    key = str(connection_id or "").strip()
    if not key:
        return
    async with _active_vod_proxy_output_disconnects_lock:
        _active_vod_proxy_output_disconnects[key] = disconnect_cb


async def unregister_vod_proxy_output_disconnect(connection_id: str) -> None:
    key = str(connection_id or "").strip()
    if not key:
        return
    async with _active_vod_proxy_output_disconnects_lock:
        _active_vod_proxy_output_disconnects.pop(key, None)


async def disconnect_vod_proxy_output(connection_id: str) -> bool:
    key = str(connection_id or "").strip()
    if not key:
        return False
    async with _active_vod_proxy_output_disconnects_lock:
        disconnect_cb = _active_vod_proxy_output_disconnects.pop(key, None)
    if disconnect_cb is None:
        return False
    try:
        await disconnect_cb()
    except Exception as exc:
        logger.warning("Failed to disconnect active VOD proxy output connection_id=%s error=%s", key, exc)
    return True
