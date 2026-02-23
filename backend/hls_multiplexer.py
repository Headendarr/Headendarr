#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import base64
import logging
import os
import re
import time
import uuid
from collections import deque
from urllib.parse import urlencode, urljoin, urlparse

import aiohttp

proxy_logger = logging.getLogger("proxy")
ffmpeg_logger = logging.getLogger("ffmpeg")

"""
HLS Proxy Core Engine - Integration Guide

This module provides the core high-performance async multiplexing logic for both 
Headendarr (TIC) and the standalone HLS-Proxy project. 

REQUIRED ROUTES FOR INTEGRATION:

1. Playlist Endpoint (.m3u8):
   Route: /<path:encoded_url>.m3u8
   Logic: 
     - Base64 decode 'encoded_url'.
     - Call 'handle_m3u8_proxy(...)'.
     - Return 'Response(body, content_type=content_type, status=status, headers=headers)'.

2. Redirect/URL Proxy:
   Route: /proxy.m3u8?url=<source_url>
   Logic: 
     - Base64 encode 'url'.
     - Redirect to the Playlist Endpoint above.

3. Segment Endpoints (.ts, .key, .vtt):
   Route: /<path:encoded_url>.<ext>
   Logic:
     - Base64 decode 'encoded_url'.
     - Call 'handle_segment_proxy(...)'.
     - Return 'Response(content, content_type=ctype)'.

4. Shared Multiplexer Stream:
   Route: /stream/<path:encoded_url>
   Logic:
     - Base64 decode 'encoded_url'.
     - Call 'handle_multiplexed_stream(...)'.
     - Yield chunks from the returned async generator.
     - Return 'Response(generate(), content_type="video/mp2t")'.
"""


class BaseStreamMultiplexer:
    def __init__(self, decoded_url):
        self.decoded_url = decoded_url
        self.queues = {}  # connection_id -> asyncio.Queue
        self.running = False
        self.lock = asyncio.Lock()
        self.last_activity = time.time()
        self.history = deque()
        self.history_bytes = 0
        self.max_history_bytes = int(os.environ.get("HLS_PROXY_MAX_HISTORY_BYTES", 200 * 1024 * 1024))

    async def _broadcast(self, chunk):
        if not chunk:
            return
        self.last_activity = time.time()

        async with self.lock:
            # Update shared history
            self.history.append(chunk)
            self.history_bytes += len(chunk)
            while self.history_bytes > self.max_history_bytes:
                old = self.history.popleft()
                self.history_bytes -= len(old)

            # Push to all subscriber queues
            for q in list(self.queues.values()):
                try:
                    q.put_nowait(chunk)
                except asyncio.QueueFull:
                    # Leaky bucket: drop oldest chunk for this specific client
                    try:
                        q.get_nowait()
                        q.put_nowait(chunk)
                    except Exception:
                        pass

    async def add_queue(self, connection_id, prebuffer_bytes=0):
        async with self.lock:
            # 200MB cap. Use a high chunk count (100k) to handle small network packets.
            q = asyncio.Queue(maxsize=100000)

            # Prime the new queue with history for an instant cushion
            primed_bytes = 0
            if prebuffer_bytes > 0 and self.history:
                accumulated = 0
                to_prime = []
                # Grab the most recent data from history to fill the cushion
                for chunk in reversed(self.history):
                    to_prime.append(chunk)
                    accumulated += len(chunk)
                    if accumulated >= prebuffer_bytes:
                        break
                for chunk in reversed(to_prime):
                    try:
                        q.put_nowait(chunk)
                        primed_bytes += len(chunk)
                    except asyncio.QueueFull:
                        break

            self.queues[connection_id] = q
            proxy_logger.info(
                f"Added queue {connection_id} for {self.decoded_url}, count: {len(self.queues)} (primed: {primed_bytes} bytes)"
            )
            return q, primed_bytes

    async def remove_queue(self, connection_id):
        should_stop = False
        queue_count = 0
        async with self.lock:
            self.queues.pop(connection_id, None)
            queue_count = len(self.queues)
            proxy_logger.info(
                f"Removed queue {connection_id} for {self.decoded_url}, count: {queue_count}"
            )
            should_stop = queue_count == 0
        if should_stop:
            proxy_logger.info("No more connections for %s, stopping.", self.decoded_url)
            asyncio.create_task(self._stop_if_unsubscribed())
        return queue_count

    async def _stop_if_unsubscribed(self):
        async with self.lock:
            should_stop = self.running and not self.queues
        if should_stop:
            await self.stop(force=False)

    async def stop(self, force=False):
        raise NotImplementedError()


class AsyncFFmpegStream(BaseStreamMultiplexer):
    """
    FFmpeg Multiplexer Mode (Legacy/Compatibility Fallback)

    How it works:
    Spawns an external FFmpeg subprocess to fetch and remux the upstream source. 
    The raw data is piped from FFmpeg's stdout into TIC's Python buffer.

    Benefits:
    - Timestamp Correction: Smooths out jittery or resetting PTS/DTS timestamps common in low-quality IPTV.
    - Stream Normalisation: Ensures PAT/PMT tables are regularly injected, improving player compatibility.
    - Error Resilience: FFmpeg's internal demuxers can often handle stream corruption that raw socket reads cannot.

    Costs:
    - High CPU: Spawns a dedicated OS process per unique stream.
    - Context Switching: High overhead due to Inter-Process Communication (IPC) between FFmpeg and Python.
    - Scalability: System performance degrades linearly with the number of active processes.
    """

    def __init__(self, decoded_url, on_stop_callback=None):
        super().__init__(decoded_url)
        self.process = None
        self.read_task = None
        self.stderr_task = None
        self.on_stop_callback = on_stop_callback

    async def start(self):
        async with self.lock:
            if self.running:
                return

            # Optimised FFmpeg command for Live IPTV Streaming:
            # - reconnect*: Ensures the stream automatically recovers from network hiccups or dropped connections.
            # - probesize 5M: Small enough for fast startup, large enough to find PMT/PAT tables.
            # - analyseduration 2M: Provides 2s of context for FFmpeg to correctly identify stream metadata.
            # - muxdelay/muxpreload 0: Forces immediate delivery of chunks to the buffer (minimal internal buffering).
            command = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "warning",
                "-reconnect", "1",
                "-reconnect_at_eof", "1",
                "-reconnect_streamed", "1",
                "-reconnect_delay_max", "2",
                "-probesize", "5M",
                "-analyzeduration", "2000000",
                "-i", self.decoded_url,
                "-c", "copy",
                "-f", "mpegts",
                "-muxdelay", "0",
                "-muxpreload", "0",
                "pipe:1",
            ]
            ffmpeg_logger.info("Executing FFmpeg with command: %s", command)
            try:
                self.process = await asyncio.create_subprocess_exec(
                    *command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                self.running = True
                self.read_task = asyncio.create_task(self._read_loop())
                self.stderr_task = asyncio.create_task(self._log_stderr())
            except Exception as e:
                ffmpeg_logger.error("Failed to start FFmpeg for %s: %s", self.decoded_url, e)
                self.running = False

    async def _read_loop(self):
        chunk_size = 16384
        try:
            while self.running and self.process:
                chunk = await self.process.stdout.read(chunk_size)
                if not chunk:
                    ffmpeg_logger.warning("FFmpeg has finished streaming for %s", self.decoded_url)
                    break
                await self._broadcast(chunk)
        except Exception as e:
            ffmpeg_logger.error("Error reading FFmpeg stdout for %s: %s", self.decoded_url, e)
        finally:
            await self.stop(force=True)

    async def _log_stderr(self):
        while self.running and self.process:
            try:
                line = await self.process.stderr.readline()
                if not line:
                    break
                ffmpeg_logger.debug(
                    "FFmpeg [%s]: %s", self.decoded_url, line.decode("utf-8", errors="replace").strip()
                )
            except Exception:
                break

    async def stop(self, force=False):
        async with self.lock:
            if not self.running:
                return
            if not force and self.queues:
                return
            self.running = False
            if self.process:
                try:
                    self.process.terminate()
                    try:
                        await asyncio.wait_for(self.process.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        self.process.kill()
                except Exception:
                    pass

            # Wake up all waiting queues with None to signal EOF
            for q in self.queues.values():
                try:
                    q.put_nowait(None)
                except Exception:
                    pass
            self.queues.clear()
            self.history.clear()
            self.history_bytes = 0
            if self.on_stop_callback:
                await self.on_stop_callback(self.decoded_url, "ffmpeg")
        ffmpeg_logger.info("FFmpeg process for %s cleaned up.", self.decoded_url)


class AsyncDirectStream(BaseStreamMultiplexer):
    """
    Direct Multiplexer Mode (Default)

    How it works:
    TIC uses its native async event loop (via aiohttp) to fetch raw bits directly from the source.
    Data is shared among all connected clients using zero-overhead Python queues.

    Benefits:
    - Maximum Efficiency: Near-zero CPU usage. No external processes or pipes.
    - Shared Connection: Only one upstream request is made regardless of the number of TIC clients.
    - Jitter Protection: Inherits the same 200MB history buffer and configurable prebuffer cushion.
    - Scalability: Allows TIC to handle dozens of concurrent streams without impacting system responsiveness.

    Costs:
    - No Stream Cleaning: Passes any source timestamp errors or missing headers directly to the player.
    """

    def __init__(self, decoded_url, headers, on_stop_callback=None):
        super().__init__(decoded_url)
        self.headers = headers
        self.read_task = None
        self.on_stop_callback = on_stop_callback

    async def start(self):
        async with self.lock:
            if self.running:
                return
            self.running = True
            self.read_task = asyncio.create_task(self._read_loop())

    async def _read_loop(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.decoded_url, headers=self.headers) as resp:
                    if resp.status != 200:
                        proxy_logger.error("DirectStream upstream failed: status %s for %s",
                                           resp.status, self.decoded_url)
                        return
                    async for chunk in resp.content.iter_any():
                        if not self.running:
                            break
                        await self._broadcast(chunk)
        except Exception as e:
            proxy_logger.error("DirectStream read error for %s: %s", self.decoded_url, e)
        finally:
            await self.stop(force=True)

    async def stop(self, force=False):
        async with self.lock:
            if not self.running:
                return
            if not force and self.queues:
                return
            self.running = False
            # Signal EOF to all queues
            for q in self.queues.values():
                try:
                    q.put_nowait(None)
                except Exception:
                    pass
            self.queues.clear()
            self.history.clear()
            self.history_bytes = 0
            if self.on_stop_callback:
                await self.on_stop_callback(self.decoded_url, "direct")
        proxy_logger.info("DirectStream for %s cleaned up.", self.decoded_url)


class MultiplexerManager:
    def __init__(self):
        self.active_streams = {}
        self.lock = asyncio.Lock()

    async def get_stream(self, decoded_url, mode, headers=None):
        key = (decoded_url, mode)
        async with self.lock:
            if key not in self.active_streams:
                if mode == "ffmpeg":
                    stream = AsyncFFmpegStream(decoded_url, on_stop_callback=self._remove_stream_entry)
                else:
                    stream = AsyncDirectStream(decoded_url, headers, on_stop_callback=self._remove_stream_entry)
                self.active_streams[key] = stream
                await stream.start()
            return self.active_streams[key]

    async def _remove_stream_entry(self, decoded_url, mode):
        async with self.lock:
            self.active_streams.pop((decoded_url, mode), None)

    async def cleanup_idle_streams(self, idle_timeout=300):
        now = time.time()
        to_stop = []
        async with self.lock:
            for key, stream in list(self.active_streams.items()):
                if now - stream.last_activity > idle_timeout:
                    proxy_logger.info(f"Stream {key[0]} idle for {idle_timeout}s, stopping.")
                    to_stop.append(stream)
        for stream in to_stop:
            await stream.stop(force=True)


class SegmentCache:
    def __init__(self, ttl=3600):
        self.cache = {}
        self.expiration_times = {}
        self._lock = asyncio.Lock()
        self.ttl = ttl
        self.max_size = 200  # Limit cache size to prevent memory issues

    async def _cleanup_expired_items(self):
        current_time = time.time()
        expired_keys = [
            k for k, exp in self.expiration_times.items() if current_time > exp
        ]
        for k in expired_keys:
            val = self.cache.get(k)
            if isinstance(val, AsyncFFmpegStream):
                await val.stop()
            self.cache.pop(k, None)
            self.expiration_times.pop(k, None)
        return len(expired_keys)

    async def get(self, key):
        async with self._lock:
            if key in self.cache and time.time() <= self.expiration_times.get(key, 0):
                # Access refreshes TTL
                self.expiration_times[key] = time.time() + self.ttl
                return self.cache[key]
            return None

    async def set(self, key, value, expiration_time=None):
        async with self._lock:
            await self._cleanup_expired_items()
            if len(self.cache) >= self.max_size and self.expiration_times:
                oldest_key = min(self.expiration_times.items(), key=lambda x: x[1])[0]
                val = self.cache.get(oldest_key)
                if isinstance(val, AsyncFFmpegStream):
                    await val.stop()
                self.cache.pop(oldest_key, None)
                self.expiration_times.pop(oldest_key, None)
            ttl = expiration_time if expiration_time is not None else self.ttl
            self.cache[key] = value
            self.expiration_times[key] = time.time() + ttl

    async def exists(self, key):
        async with self._lock:
            await self._cleanup_expired_items()
            return key in self.cache

    async def evict_expired_items(self):
        async with self._lock:
            return await self._cleanup_expired_items()


# Shared Global Manager
mux_manager = MultiplexerManager()

# --- Shared Wrapper Logic ---


async def upsert_stream_activity(*args, **kwargs):
    """
    Optional hook for platform-specific activity tracking.
    TIC overrides this with functional logic.
    """
    pass


async def stop_stream_activity(*args, **kwargs):
    """
    Optional hook for platform-specific activity tracking.
    TIC overrides this with functional logic.
    """
    pass


def b64_urlsafe_encode(value):
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("utf-8")


def b64_urlsafe_decode(value):
    padded = value + "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode(padded).decode("utf-8")
    except Exception:
        # Fallback for older non-urlsafe tokens
        return base64.b64decode(padded).decode("utf-8")


def parse_size(size_str: str, default: int = 0) -> int:
    """Parse strings like '2M', '512K' into bytes."""
    if not size_str:
        return default
    size_str = size_str.upper().strip()
    try:
        if size_str.endswith("K"):
            return int(float(size_str[:-1]) * 1024)
        if size_str.endswith("M"):
            return int(float(size_str[:-1]) * 1024 * 1024)
        return int(size_str)
    except (ValueError, TypeError):
        return default


def infer_extension(url_value):
    parsed = urlparse(url_value)
    path = (parsed.path or "").lower()
    if path.endswith(".m3u8"):
        return "m3u8"
    if path.endswith(".key"):
        return "key"
    if path.endswith(".vtt"):
        return "vtt"
    return "ts"


def generate_proxy_url(base_url, encoded_url, extension, params=None):
    url = f"{base_url.rstrip('/')}/{encoded_url}.{extension}"
    if params:
        url = f"{url}?{urlencode(params)}"
    return url


async def handle_m3u8_proxy(
    decoded_url,
    request_host_url,
    hls_proxy_prefix,
    headers=None,
    instance_id=None,
    stream_key=None,
    username=None,
    connection_id=None,
    max_buffer_bytes=1048576,
    proxy_base_url=None,
    segment_cache=None,
    prefetch_segments_enabled=True,
):
    """
    Standard Logic for Playlist Proxying.
    Rewrites child URLs to point back to the proxy.
    """
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(decoded_url, headers=headers) as resp:
                if resp.status != 200:
                    return None, None, 502, {"X-Proxy-Error": "upstream-unreachable"}

                response_url = str(resp.url)
                content_type = resp.headers.get("Content-Type") or "text/plain"

                # Logic for determining if we stream or return string
                content_length = resp.content_length or 0
                if content_length > max_buffer_bytes:
                    # Return a generator for streaming large playlists
                    return _stream_rewrite_generator(
                        resp,
                        response_url,
                        request_host_url,
                        hls_proxy_prefix,
                        instance_id,
                        stream_key,
                        username,
                        connection_id,
                        proxy_base_url=proxy_base_url,
                    ), content_type, 200, {}

                # Small enough to process in memory
                playlist_content = await resp.text()
                modified, segment_urls = await _update_child_urls(
                    playlist_content,
                    response_url,
                    request_host_url,
                    hls_proxy_prefix,
                    instance_id,
                    stream_key,
                    username,
                    connection_id,
                    proxy_base_url=proxy_base_url,
                )
                if prefetch_segments_enabled and segment_cache and segment_urls:
                    asyncio.create_task(prefetch_segments(segment_urls, headers=headers, cache_obj=segment_cache))
                return modified, content_type, 200, {}
        except Exception as exc:
            proxy_logger.error(f"HLS proxy failed to fetch '{decoded_url}': {exc}")
            return None, None, 502, {"X-Proxy-Error": "upstream-unreachable"}


async def _update_child_urls(
    content,
    source_url,
    request_host_url,
    hls_proxy_prefix,
    instance_id,
    stream_key,
    username,
    connection_id,
    proxy_base_url=None,
):
    updated_lines = []
    segment_urls = []
    state = {"next_is_playlist": False, "next_is_segment": False}
    if proxy_base_url:
        base_proxy_url = proxy_base_url.rstrip("/")
    else:
        base_proxy_url = f"{request_host_url.rstrip('/')}{hls_proxy_prefix}"
        if instance_id:
            base_proxy_url = f"{base_proxy_url.rstrip('/')}/{instance_id}"

    for line in content.splitlines():
        updated_line, new_segment_urls = rewrite_playlist_line(
            line,
            source_url,
            base_proxy_url,
            state,
            stream_key,
            username,
            connection_id,
        )
        if updated_line:
            updated_lines.append(updated_line)
        if new_segment_urls:
            segment_urls.extend(new_segment_urls)
    return "\n".join(updated_lines), segment_urls


async def _stream_rewrite_generator(
    resp,
    source_url,
    request_host_url,
    hls_proxy_prefix,
    instance_id,
    stream_key,
    username,
    connection_id,
    proxy_base_url=None,
):
    buffer = ""
    state = {"next_is_playlist": False, "next_is_segment": False}
    if proxy_base_url:
        base_proxy_url = proxy_base_url.rstrip("/")
    else:
        base_proxy_url = f"{request_host_url.rstrip('/')}{hls_proxy_prefix}"
        if instance_id:
            base_proxy_url = f"{base_proxy_url.rstrip('/')}/{instance_id}"

    async for chunk in resp.content.iter_chunked(8192):
        buffer += chunk.decode("utf-8", errors="ignore")
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            updated_line, _ = rewrite_playlist_line(
                line, source_url, base_proxy_url, state, stream_key, username, connection_id)
            if updated_line:
                yield updated_line + "\n"
    if buffer:
        updated_line, _ = rewrite_playlist_line(buffer, source_url, base_proxy_url,
                                                state, stream_key, username, connection_id)
        if updated_line:
            yield updated_line + "\n"


def rewrite_playlist_line(line, source_url, base_proxy_url, state, stream_key=None, username=None, connection_id=None):
    stripped = line.strip()
    if not stripped:
        return None, []

    if stripped.startswith("#"):
        segment_urls = []
        upper = stripped.upper()
        if upper.startswith("#EXT-X-STREAM-INF"):
            state["next_is_playlist"] = True
        elif upper.startswith("#EXTINF"):
            state["next_is_segment"] = True

        def replace_uri(match):
            orig_uri = match.group(1)
            abs_url = urljoin(source_url, orig_uri)
            ext = infer_extension(abs_url)
            if "#EXT-X-KEY" in upper:
                ext = "key"
            elif "#EXT-X-MEDIA" in upper or "#EXT-X-I-FRAME-STREAM-INF" in upper:
                ext = "m3u8"
            if ext in ("ts", "vtt", "key"):
                segment_urls.append(abs_url)

            new_uri = generate_proxy_url(base_proxy_url, b64_urlsafe_encode(abs_url), ext,
                                         _build_params(stream_key, username, connection_id))
            return f'URI="{new_uri}"'

        return re.sub(r'URI="([^"]+)"', replace_uri, line), segment_urls

    abs_url = urljoin(source_url, stripped)
    ext = "m3u8" if state.get("next_is_playlist") else (
        "ts" if state.get("next_is_segment") else infer_extension(abs_url))
    state["next_is_playlist"] = state["next_is_segment"] = False
    segment_urls = [abs_url] if ext in ("ts", "vtt", "key") else []
    return generate_proxy_url(base_proxy_url, b64_urlsafe_encode(abs_url), ext, _build_params(stream_key, username, connection_id)), segment_urls


def _build_params(stream_key, username, connection_id):
    params = {}
    if stream_key:
        params["stream_key"] = stream_key
    if username:
        params["username"] = username
    if connection_id:
        params["connection_id"] = connection_id
    return params


async def prefetch_segments(segment_urls, headers=None, cache_obj=None):
    if not cache_obj or not segment_urls:
        return
    async with aiohttp.ClientSession() as session:
        for url in segment_urls:
            cached = await cache_obj.get(url)
            if cached is not None:
                continue
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        continue
                    content = await resp.read()
                    content_type = (resp.headers.get("Content-Type") or "").lower()
                    await cache_obj.set(
                        url,
                        {"body": content, "content_type": content_type},
                        expiration_time=30,
                    )
            except aiohttp.ClientError as exc:
                proxy_logger.debug("Failed to prefetch URL '%s': %s", url, exc)


async def handle_segment_proxy(decoded_url, headers, cache_obj):
    """
    Generic logic for fetching and caching .ts, .key, .vtt files.
    """
    cached = await cache_obj.get(decoded_url)
    if cached is not None:
        if isinstance(cached, dict):
            return cached.get("body"), 200, cached.get("content_type", "")
        return cached, 200, ""

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(decoded_url, headers=headers) as resp:
                if resp.status != 200:
                    return None, 404, ""
                content = await resp.read()
                content_type = (resp.headers.get("Content-Type") or "").lower()
                await cache_obj.set(
                    decoded_url,
                    {"body": content, "content_type": content_type},
                    expiration_time=30,
                )
                return content, 200, content_type
    except aiohttp.ClientError as exc:
        proxy_logger.warning("Segment fetch failed for '%s': %s", decoded_url, exc)
        return None, 502, ""


async def handle_multiplexed_stream(decoded_url, mode, headers, prebuffer_bytes, connection_id):
    """
    Core multiplexer delivery logic.
    """
    stream = await mux_manager.get_stream(decoded_url, mode, headers=headers)
    stream.last_activity = time.time()

    queue, primed_bytes = await stream.add_queue(connection_id, prebuffer_bytes=prebuffer_bytes)

    cushion_remaining = prebuffer_bytes - primed_bytes
    temp_buffer = []

    async def generate():
        nonlocal cushion_remaining, temp_buffer
        try:
            while True:
                chunk = await queue.get()
                if chunk is None:
                    break
                if cushion_remaining > 0:
                    temp_buffer.append(chunk)
                    cushion_remaining -= len(chunk)
                    if cushion_remaining <= 0:
                        for c in temp_buffer:
                            yield c
                        temp_buffer = None
                else:
                    yield chunk
        finally:
            await stream.remove_queue(connection_id)

    return generate()
