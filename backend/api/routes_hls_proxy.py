#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import base64
import logging
import os
import re
import subprocess
import threading
import time
import uuid
from collections import deque

from quart import current_app, Response, stream_with_context, request, redirect

from backend.api import blueprint
from backend.auth import stream_key_required, audit_stream_event, skip_stream_connect_audit
import aiohttp
from urllib.parse import urlparse, urljoin

# Test:
#       > mkfifo /tmp/ffmpegpipe
#       > ffmpeg -probesize 10M -analyzeduration 0 -fpsprobesize 0 -i "<URL>" -c copy -y -f mpegts /tmp/ffmpegpipe
#       > vlc /tmp/ffmpegpipe
#
#   Or:
#       > ffmpeg -probesize 10M -analyzeduration 0 -fpsprobesize 0 -i "<URL>" -c copy -y -f mpegts - | vlc -
#

hls_proxy_prefix = os.environ.get('HLS_PROXY_PREFIX', '/')
if not hls_proxy_prefix.startswith("/"):
    hls_proxy_prefix = "/" + hls_proxy_prefix

hls_proxy_host_ip = os.environ.get('HLS_PROXY_HOST_IP')
hls_proxy_port = os.environ.get('HLS_PROXY_PORT')
hls_proxy_max_buffer_bytes = int(os.environ.get('HLS_PROXY_MAX_BUFFER_BYTES', '262144'))

proxy_logger = logging.getLogger("proxy")
ffmpeg_logger = logging.getLogger("ffmpeg")
buffer_logger = logging.getLogger("buffer")

# A dictionary to keep track of active streams
active_streams = {}


class FFmpegStream:
    def __init__(self, decoded_url):
        self.decoded_url = decoded_url
        self.buffers = {}
        self.process = None
        self.running = True
        self.thread = threading.Thread(target=self.run_ffmpeg)
        self.connection_count = 0
        self.lock = threading.Lock()
        self.last_activity = time.time()  # Track last activity time
        self.thread.start()

    def run_ffmpeg(self):
        command = [
            'ffmpeg', '-hide_banner', '-loglevel', 'info', '-err_detect', 'ignore_err',
            '-probesize', '20M', '-analyzeduration', '0', '-fpsprobesize', '0',
            '-i', self.decoded_url,
            '-c', 'copy',
            '-f', 'mpegts', 'pipe:1'
        ]
        ffmpeg_logger.info("Executing FFmpeg with command: %s", command)
        self.process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Start a thread to log stderr
        stderr_thread = threading.Thread(target=self.log_stderr)
        stderr_thread.daemon = True  # Make this a daemon thread so it exits when main thread exits
        stderr_thread.start()

        chunk_size = 65536  # Read 64 KB at a time
        while self.running:
            try:
                # Use select to avoid blocking indefinitely
                import select
                ready, _, _ = select.select([self.process.stdout], [], [], 1.0)
                if not ready:
                    # No data available, check if we should terminate due to inactivity
                    if time.time() - self.last_activity > 300:  # 5 minutes of inactivity
                        ffmpeg_logger.info("No activity for 5 minutes, terminating FFmpeg stream")
                        self.stop()
                        break
                    continue
                
                chunk = self.process.stdout.read(chunk_size)
                if not chunk:
                    ffmpeg_logger.warning("FFmpeg has finished streaming.")
                    break
                
                # Update last activity time
                self.last_activity = time.time()
                
                # Append the chunk to all buffers
                with self.lock:  # Use lock when accessing buffers
                    for buffer in self.buffers.values():
                        buffer.append(chunk)
            except Exception as e:
                ffmpeg_logger.error("Error reading stdout: %s", e)
                break

        self.cleanup()

    def cleanup(self):
        """Clean up resources properly"""
        self.running = False
        if self.process:
            try:
                # Try to terminate the process gracefully first
                self.process.terminate()
                # Wait a bit for it to terminate
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # If it doesn't terminate, kill it
                    self.process.kill()
                    self.process.wait()
            except Exception as e:
                ffmpeg_logger.error("Error terminating FFmpeg process: %s", e)
            
            # Close file descriptors
            if self.process.stdout:
                self.process.stdout.close()
            if self.process.stderr:
                self.process.stderr.close()
            
        ffmpeg_logger.info("FFmpeg process cleaned up.")
        
        # Clear buffers
        with self.lock:
            self.buffers.clear()

    def stop(self):
        """Stop the FFmpeg process and clean up resources"""
        if self.running:
            self.running = False
            self.cleanup()

    def log_stderr(self):
        """Log stderr output from the FFmpeg process."""
        while self.running and self.process and self.process.stderr:
            try:
                line = self.process.stderr.readline()
                if not line:
                    break
                ffmpeg_logger.debug("FFmpeg: %s", line.decode('utf-8', errors='replace').strip())
            except Exception as e:
                ffmpeg_logger.error("Error reading stderr: %s", e)
                break

    def add_buffer(self, buffer_id):
        """Add a new per-connection TimeBuffer with proper locking."""
        with self.lock:
            if buffer_id not in self.buffers:
                self.buffers[buffer_id] = TimeBuffer()
                self.connection_count += 1
                ffmpeg_logger.info(f"Added buffer {buffer_id}, connection count: {self.connection_count}")
            return self.buffers[buffer_id]

    def remove_buffer(self, buffer_id):
        """Remove a buffer with proper locking"""
        with self.lock:
            if buffer_id in self.buffers:
                del self.buffers[buffer_id]
                self.connection_count -= 1
                ffmpeg_logger.info(f"Removed buffer {buffer_id}, connection count: {self.connection_count}")
                # If no more connections, stop the stream
                if self.connection_count <= 0:
                    ffmpeg_logger.info("No more connections, stopping FFmpeg stream")
                    # Schedule the stop to happen outside of the lock
                    threading.Thread(target=self.stop).start()


class TimeBuffer:
    def __init__(self, duration=60):  # Duration in seconds
        self.duration = duration
        self.buffer = deque()  # Use deque to hold (timestamp, chunk) tuples
        self.lock = threading.Lock()

    def append(self, chunk):
        current_time = time.time()
        with self.lock:
            # Append the current time and chunk to the buffer
            self.buffer.append((current_time, chunk))
            buffer_logger.debug("[Buffer] Appending chunk at time %f", current_time)

            # Remove chunks older than the specified duration
            while self.buffer and (current_time - self.buffer[0][0]) > self.duration:
                buffer_logger.info("[Buffer] Removing chunk older than %d seconds", self.duration)
                self.buffer.popleft()  # Remove oldest chunk

    def read(self):
        with self.lock:
            if self.buffer:
                # Return the oldest chunk
                return self.buffer.popleft()[1]  # Return the chunk, not the timestamp
            return b''  # Return empty bytes if no data


class Cache:
    def __init__(self, ttl=3600):
        self.cache = {}
        self.expiration_times = {}
        self._lock = asyncio.Lock()
        self.ttl = ttl
        self.max_size = 100  # Limit cache size to prevent memory issues

    async def _cleanup_expired_items(self):
        current_time = time.time()
        expired_keys = [k for k, exp in self.expiration_times.items() if current_time > exp]
        for k in expired_keys:
            if isinstance(self.cache.get(k), FFmpegStream):
                try:
                    self.cache[k].stop()
                except Exception:
                    pass
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
                if isinstance(self.cache.get(oldest_key), FFmpegStream):
                    try:
                        self.cache[oldest_key].stop()
                    except Exception:
                        pass
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

async def periodic_cache_cleanup():
    while True:
        try:
            evicted_count = await cache.evict_expired_items()
            if evicted_count > 0:
                proxy_logger.info(f"Cache cleanup: evicted {evicted_count} expired items")
            
            # Log current memory usage (optional)
            try:
                import psutil
                process = psutil.Process()
                memory_info = process.memory_info()
                proxy_logger.debug(f"Current memory usage: {memory_info.rss / (1024 * 1024):.2f} MB")
            except Exception as _e:
                # Silently skip if psutil not installed or fails
                pass
            
        except Exception as e:
            proxy_logger.error(f"Error during cache cleanup: {e}")
        
        await asyncio.sleep(60)


# Global cache instance (short default TTL for HLS segments)
cache = Cache(ttl=120)

# Register a startup hook to launch the periodic cache cleanup task once the app is ready.
@blueprint.record_once
def _register_startup(state):
    app = state.app

    @app.before_serving
    async def _start_periodic_cache_cleanup():
        asyncio.create_task(periodic_cache_cleanup())


async def prefetch_segments(segment_urls):
    async with aiohttp.ClientSession() as session:
        for url in segment_urls:
            if not await cache.exists(url):
                proxy_logger.info("[CACHE] Saved URL '%s' to cache", url)
                try:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            content = await resp.read()
                            await cache.set(url, content, expiration_time=30)  # Cache for 30 seconds
                except Exception as e:
                    proxy_logger.error("Failed to prefetch URL '%s': %s", url, e)


def _build_proxy_base_url():
    if hls_proxy_host_ip:
        host_base_url_prefix = 'http'
        host_base_url_port = ''
        if hls_proxy_port:
            if hls_proxy_port == '443':
                host_base_url_prefix = 'https'
            host_base_url_port = f':{hls_proxy_port}'
        return f'{host_base_url_prefix}://{hls_proxy_host_ip}{host_base_url_port}{hls_proxy_prefix}'
    return f'{request.host_url.rstrip("/")}{hls_proxy_prefix}'


def _infer_extension(url_value):
    parsed = urlparse(url_value)
    path = (parsed.path or '').lower()
    if path.endswith('.m3u8'):
        return 'm3u8'
    if path.endswith('.key'):
        return 'key'
    return 'ts'


def _b64_urlsafe_encode(value):
    return base64.urlsafe_b64encode(value.encode('utf-8')).decode('utf-8')


def _b64_urlsafe_decode(value):
    padded = value + "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode(padded).decode('utf-8')
    except Exception:
        # Fallback for older non-urlsafe tokens
        return base64.b64decode(padded).decode('utf-8')


def generate_base64_encoded_url(url_to_encode, extension, stream_key=None, username=None):
    full_url_encoded = _b64_urlsafe_encode(url_to_encode)
    base_url = _build_proxy_base_url().rstrip('/')
    url = f'{base_url}/{full_url_encoded}.{extension}'
    if stream_key:
        if username:
            return f'{url}?username={username}&password={stream_key}'
        return f'{url}?stream_key={stream_key}'
    return url


def _rewrite_uri_value(uri_value, source_url, stream_key=None, username=None, forced_extension=None):
    absolute_url = urljoin(source_url, uri_value)
    extension = forced_extension or _infer_extension(absolute_url)
    return generate_base64_encoded_url(absolute_url, extension, stream_key=stream_key, username=username), absolute_url, extension


def update_child_urls(playlist_content, source_url, stream_key=None, username=None):
    proxy_logger.debug(f"Original Playlist Content:\n{playlist_content}")

    updated_lines = []
    lines = playlist_content.splitlines()
    segment_urls = []
    state = {
        "next_is_playlist": False,
        "next_is_segment": False,
    }

    for line in lines:
        updated_line, new_segment_urls = rewrite_playlist_line(
            line,
            source_url,
            state,
            stream_key=stream_key,
            username=username,
        )
        if updated_line:
            updated_lines.append(updated_line)
        if new_segment_urls:
            segment_urls.extend(new_segment_urls)

    if segment_urls:
        asyncio.create_task(prefetch_segments(segment_urls))

    modified_playlist = "\n".join(updated_lines)
    proxy_logger.debug(f"Modified Playlist Content:\n{modified_playlist}")
    return modified_playlist


def rewrite_playlist_line(line, source_url, state, stream_key=None, username=None):
    stripped_line = line.strip()
    if not stripped_line:
        return None, []

    segment_urls = []
    if stripped_line.startswith('#'):
        upper_line = stripped_line.upper()
        if upper_line.startswith('#EXT-X-STREAM-INF'):
            state["next_is_playlist"] = True
        elif upper_line.startswith('#EXTINF'):
            state["next_is_segment"] = True

        def replace_uri(match):
            original_uri = match.group(1)
            forced_extension = None
            if '#EXT-X-KEY' in upper_line:
                forced_extension = 'key'
            elif '#EXT-X-MEDIA' in upper_line or '#EXT-X-I-FRAME-STREAM-INF' in upper_line:
                forced_extension = 'm3u8'
            new_uri, absolute_url, extension = _rewrite_uri_value(
                original_uri,
                source_url,
                stream_key=stream_key,
                username=username,
                forced_extension=forced_extension,
            )
            if extension == 'ts':
                segment_urls.append(absolute_url)
            return f'URI="{new_uri}"'

        updated_line = re.sub(r'URI="([^"]+)"', replace_uri, line)
        return updated_line, segment_urls

    absolute_url = urljoin(source_url, stripped_line)
    if state.get("next_is_playlist"):
        extension = 'm3u8'
        state["next_is_playlist"] = False
        state["next_is_segment"] = False
    elif state.get("next_is_segment"):
        extension = 'ts'
        state["next_is_segment"] = False
    else:
        extension = _infer_extension(absolute_url)
    if extension == 'ts':
        segment_urls.append(absolute_url)
    return generate_base64_encoded_url(absolute_url, extension, stream_key=stream_key, username=username), segment_urls


async def _stream_updated_playlist(resp, response_url, stream_key=None, username=None):
    buffer = ""
    state = {
        "next_is_playlist": False,
        "next_is_segment": False,
    }
    async for chunk in resp.content.iter_chunked(8192):
        buffer += chunk.decode('utf-8', errors='ignore')
        while '\n' in buffer:
            line, buffer = buffer.split('\n', 1)
            updated_line, _ = rewrite_playlist_line(
                line,
                response_url,
                state,
                stream_key=stream_key,
                username=username,
            )
            if updated_line:
                yield updated_line + "\n"
    if buffer:
        updated_line, _ = rewrite_playlist_line(
            buffer,
            response_url,
            state,
            stream_key=stream_key,
            username=username,
        )
        if updated_line:
            yield updated_line + "\n"


async def fetch_and_update_playlist(decoded_url, stream_key=None, username=None):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(decoded_url) as resp:
                if resp.status != 200:
                    return None, None, False

                response_url = str(resp.url)
                content_type = resp.headers.get('Content-Type') or 'application/vnd.apple.mpegurl'
                content_length = resp.content_length or 0

                if content_length and content_length > hls_proxy_max_buffer_bytes:
                    return _stream_updated_playlist(
                        resp,
                        response_url,
                        stream_key=stream_key,
                        username=username,
                    ), content_type, True

                playlist_content = await resp.text()
                updated_playlist = update_child_urls(
                    playlist_content,
                    response_url,
                    stream_key=stream_key,
                    username=username
                )
                return updated_playlist, content_type, False
        except aiohttp.ClientError as exc:
            proxy_logger.warning("HLS proxy failed to fetch '%s': %s", decoded_url, exc)
            return None, None, False


@blueprint.route(f'{hls_proxy_prefix.lstrip("/")}/<encoded_url>.m3u8', methods=['GET'])
@stream_key_required
async def proxy_m3u8(encoded_url):
    await audit_stream_event(request._stream_user, "hls_m3u8", request.path)
    # Decode the Base64 encoded URL
    decoded_url = _b64_urlsafe_decode(encoded_url)
    stream_key = request.args.get("stream_key") or request.args.get("password")
    username = request.args.get("username")

    updated_playlist, content_type, is_stream = await fetch_and_update_playlist(
        decoded_url,
        stream_key=stream_key,
        username=username
    )
    if updated_playlist is None:
        proxy_logger.error("Failed to fetch the original playlist '%s'", decoded_url)
        response = Response("Failed to fetch the original playlist.", status=502)
        response.headers['X-TIC-Proxy-Error'] = 'upstream-unreachable'
        return response

    proxy_logger.info(f"[MISS] Serving m3u8 URL '%s' without cache", decoded_url)
    if is_stream:
        return Response(updated_playlist, content_type=content_type)
    return Response(updated_playlist, content_type=content_type)


@blueprint.route(f'{hls_proxy_prefix.lstrip("/")}/proxy.m3u8', methods=['GET'])
@stream_key_required
async def proxy_m3u8_redirect():
    url = request.args.get('url')
    if not url:
        return Response("Missing url parameter.", status=400)
    encoded = _b64_urlsafe_encode(url)
    stream_key = request.args.get("stream_key") or request.args.get("password")
    username = request.args.get("username")
    target = f'{hls_proxy_prefix.rstrip("/")}/{encoded}.m3u8'
    if stream_key:
        if username:
            target = f'{target}?username={username}&password={stream_key}'
        else:
            target = f'{target}?stream_key={stream_key}'
    return redirect(target, code=302)


@blueprint.route(f'{hls_proxy_prefix.lstrip("/")}/<encoded_url>.key', methods=['GET'])
@stream_key_required
async def proxy_key(encoded_url):
    await audit_stream_event(request._stream_user, "hls_key", request.path)
    # Decode the Base64 encoded URL
    decoded_url = _b64_urlsafe_decode(encoded_url)

    # Check if the .key file is already cached
    if await cache.exists(decoded_url):
        proxy_logger.info(f"[HIT] Serving key URL from cache: %s", decoded_url)
        cached_content = await cache.get(decoded_url)
        return Response(cached_content, content_type='application/octet-stream')

    # If not cached, fetch the file and cache it
    proxy_logger.info(f"[MISS] Serving key URL '%s' without cache", decoded_url)
    async with aiohttp.ClientSession() as session:
        async with session.get(decoded_url) as resp:
            if resp.status != 200:
                proxy_logger.error("Failed to fetch key file '%s'", decoded_url)
                return Response("Failed to fetch the file.", status=404)
            content = await resp.read()
            await cache.set(decoded_url, content, expiration_time=30)  # Cache for 30 seconds
            return Response(content, content_type='application/octet-stream')


@blueprint.route(f'{hls_proxy_prefix.lstrip("/")}/<encoded_url>.ts', methods=['GET'])
@stream_key_required
async def proxy_ts(encoded_url):
    await audit_stream_event(request._stream_user, "hls_ts", request.path)
    # Decode the Base64 encoded URL
    decoded_url = _b64_urlsafe_decode(encoded_url)

    # Check if the .ts file is already cached
    if await cache.exists(decoded_url):
        proxy_logger.info(f"[HIT] Serving ts URL from cache: %s", decoded_url)
        cached_content = await cache.get(decoded_url)
        return Response(cached_content, content_type='video/mp2t')

    # If not cached, fetch the file and cache it
    proxy_logger.info(f"[MISS] Serving ts URL '%s' without cache", decoded_url)
    async with aiohttp.ClientSession() as session:
        async with session.get(decoded_url) as resp:
            if resp.status != 200:
                proxy_logger.error("Failed to fetch ts file '%s'", decoded_url)
                return Response("Failed to fetch the file.", status=404)
            content = await resp.read()
            await cache.set(decoded_url, content, expiration_time=30)  # Cache for 30 seconds
            return Response(content, content_type='video/mp2t')


@blueprint.route(f'{hls_proxy_prefix.lstrip("/")}/stream/<encoded_url>', methods=['GET'])
@stream_key_required
@skip_stream_connect_audit
async def stream_ts(encoded_url):
    await audit_stream_event(request._stream_user, "hls_stream", request.path)
    # Decode the Base64 encoded URL
    decoded_url = _b64_urlsafe_decode(encoded_url)

    # Generate a unique identifier (UUID) for the connection
    connection_id = str(uuid.uuid4())  # Use a UUID for the connection ID

    # Check if the stream is active and has connections
    if decoded_url not in active_streams or not active_streams[decoded_url].running or active_streams[
        decoded_url].connection_count == 0:
        buffer_logger.info("Creating new FFmpeg stream with connection ID %s.", connection_id)
        # Create a new stream if it does not exist or if there are no connections
        stream = FFmpegStream(decoded_url)
        active_streams[decoded_url] = stream
    else:
        buffer_logger.info("Connecting to existing FFmpeg stream with connection ID %s.", connection_id)

    # Get the existing stream
    stream = active_streams[decoded_url]
    stream.last_activity = time.time()  # Update last activity time

    # Add a new buffer for this connection
    stream.add_buffer(connection_id)
    await audit_stream_event(request._stream_user, "hls_stream_connect", request.path)

    # Create a generator to stream data from the connection-specific buffer
    @stream_with_context
    async def generate():
        try:
            while True:
                # Check if the buffer exists before reading
                if connection_id in stream.buffers:
                    data = stream.buffers[connection_id].read()
                    if data:
                        yield data
                    else:
                        # Check if FFmpeg is still running
                        if not stream.running:
                            buffer_logger.info("FFmpeg has stopped, closing stream.")
                            break
                        # Sleep briefly if no data is available
                        await asyncio.sleep(0.1)  # Wait before checking again
                else:
                    # If the buffer doesn't exist, break the loop
                    break
        finally:
            stream.remove_buffer(connection_id)  # Remove the buffer on connection close
            await audit_stream_event(request._stream_user, "hls_stream_disconnect", request.path)

    # Create a response object with the correct content type and set timeout to None
    response = Response(generate(), content_type='video/mp2t')
    response.timeout = None  # Disable timeout for streaming response
    return response
