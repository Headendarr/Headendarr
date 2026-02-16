#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import base64
import json
import logging
import os
import re
import subprocess
import time
import uuid
from collections import deque
from urllib.parse import urlencode, urljoin, urlparse

import aiohttp
from quart import Response, current_app, redirect, request, stream_with_context

from backend.api import blueprint
from backend.auth import (
    audit_stream_event,
    get_request_client_ip,
    is_tvh_backend_stream_user,
    skip_stream_connect_audit,
    stream_key_required,
)

# Test:
#       > mkfifo /tmp/ffmpegpipe
#       > ffmpeg -probesize 10M -analyzeduration 0 -fpsprobesize 0 -i "<URL>" -c copy -y -f mpegts /tmp/ffmpegpipe
#       > vlc /tmp/ffmpegpipe
#
#   Or:
#       > ffmpeg -probesize 10M -analyzeduration 0 -fpsprobesize 0 -i "<URL>" -c copy -y -f mpegts - | vlc -
#

hls_proxy_prefix = os.environ.get("HLS_PROXY_PREFIX", "/")
if not hls_proxy_prefix.startswith("/"):
    hls_proxy_prefix = "/" + hls_proxy_prefix

hls_proxy_host_ip = os.environ.get("HLS_PROXY_HOST_IP")
hls_proxy_port = os.environ.get("HLS_PROXY_PORT")
hls_proxy_max_buffer_bytes = int(os.environ.get("HLS_PROXY_MAX_BUFFER_BYTES", "1048576"))


def _get_instance_id():
    config = current_app.config.get("APP_CONFIG") if current_app else None
    if not config:
        return None
    return config.ensure_instance_id()


def _validate_instance_id(instance_id):
    """Internal proxy is scoped to this TIC instance only."""
    expected = _get_instance_id()
    if not expected or instance_id != expected:
        return Response("Not found", status=404)
    return None


proxy_logger = logging.getLogger("proxy")
ffmpeg_logger = logging.getLogger("ffmpeg")
buffer_logger = logging.getLogger("buffer")

# Track active stream activity to avoid per-segment audit logging.


class _AuditUser:
    def __init__(self, user_id, username, stream_key=None):
        self.id = user_id
        self.username = username
        self.streaming_key = stream_key
        self.is_active = True


class StreamActivityTracker:
    """
    In-memory tracker for HLS and TVHeadend playback activity used by audit logging and dashboard status.

    High-level model:
    - A playback session is uniquely identified by a `connection_id`.
    - Active sessions are stored in `self.sessions`.
    - Recently ended or expired sessions are moved to `self.history`.
    - Session data stores user/client/request fields required for:
      - emitting `stream_start` / `stream_stop` audit events
      - reporting active sessions in `/tic-api/dashboard/activity`

    Tracking mechanisms:
    1. connection_id (primary)
       - All tracking relies on a persistent `connection_id`.
       - For HLS proxy, this is appended to child URLs and passed back by the client.
       - For TVHeadend, this is derived from the subscription UUID or metadata.
       - This allows deterministic mapping of requests to a single logical session.

    2. Identity Resolution
       - The tracker resolves "segment" URLs back to their parent playlist URL
         using `self.playlist_parents`.
       - This ensures that even if a request is for a chunk, the session's primary
         `identity` remains the playlist URL, which is required for channel resolution.

    3. Session Rehydration
       - If a client disconnects (e.g. app restart or network blip) and resumes with
         the same `connection_id`, the session is moved from `history` back to `sessions`.
       - This preserves original metadata (identity, started_at, channel resolution)
         across restarts, ensuring the dashboard remains consistent.

    Persistence and Expiry:
    - Active sessions expire after `activity_ttl` seconds of inactivity (default 20s).
    - On expiry, a `stream_stop` audit event is emitted and the session moves to history.
    - History items are pruned after `history_ttl` seconds (default 1 hour).
    - Both `sessions` and `history` are persisted to `stream_activity_state.json`
      every 15 seconds, allowing full state recovery after a process restart.

    Concurrency model:
    - All state modifications are guarded by `self.lock`.
    - Public methods are async and safe to call from concurrent requests or background tasks.
    """

    def __init__(self, activity_ttl=20, history_ttl=3600):
        self.sessions = {}  # connection_id -> session_dict
        self.history = {}   # connection_id -> { 'last_seen': float, 'entry': dict }
        self.playlist_parents = {}  # child_url -> parent_url (short-lived)
        self.lock = asyncio.Lock()
        self.activity_ttl = activity_ttl
        self.history_ttl = history_ttl

    @staticmethod
    def _request_user():
        try:
            return getattr(request, "_stream_user", None) or getattr(request, "_current_user", None)
        except Exception:
            return None

    def _resolve_playlist_root(self, url: str) -> str:
        current = url
        seen = set()
        while current and current in self.playlist_parents and current not in seen:
            seen.add(current)
            current = self.playlist_parents[current]
        return current or url

    @staticmethod
    def _is_segment(url: str) -> bool:
        if not url:
            return False
        from backend.channels import normalize_url
        normalized = normalize_url(url)
        path = urlparse(normalized).path.lower()
        return path.endswith((".ts", ".vtt", ".key"))

    async def mark(
        self,
        identity,
        event_type="stream_start",
        connection_id=None,
        endpoint_override=None,
        user=None,
        ip_address=None,
        user_agent=None,
        perform_audit=True,
        details_override=None,
        channel_id=None,
        channel_name=None,
        channel_logo_url=None,
        stream_name=None,
        source_url=None,
        display_url=None,
    ):
        if not user:
            user = self._request_user()
        if is_tvh_backend_stream_user(user):
            return "ignored"

        user_id = getattr(user, "id", None)
        username = getattr(user, "username", None)
        stream_key = getattr(user, "streaming_key", None)

        now = time.time()
        if ip_address is None:
            ip_address = get_request_client_ip()
        if user_agent is None:
            try:
                user_agent = request.headers.get("User-Agent")
            except Exception:
                user_agent = None

        if not connection_id:
            connection_id = uuid.uuid4().hex

        from backend.channels import build_stream_source_index, resolve_stream_target, normalize_url
        normalized_identity = normalize_url(identity)
        # Resolve authoritative identity (playlist > segment)
        canonical_identity = self._resolve_playlist_root(normalized_identity)

        # Check existing state to see if we already have a resolved channel
        existing_channel_name = None
        async with self.lock:
            existing = self.sessions.get(connection_id)
            if not existing and connection_id in self.history:
                existing = self.history[connection_id]["entry"]
            if existing:
                existing_channel_name = existing.get("channel_name")

        # Try to resolve metadata if not provided and not already known
        resolved_stream_name = stream_name
        resolved_source_url = source_url
        resolved_display_url = display_url
        if not channel_name and not existing_channel_name:
            source_index = await build_stream_source_index()
            # Try resolution with both canonical and raw identity
            resolved = resolve_stream_target(canonical_identity, source_index, related_urls=[normalized_identity])
            channel_id = channel_id or resolved.get("channel_id")
            channel_name = channel_name or resolved.get("channel_name")
            channel_logo_url = channel_logo_url or resolved.get("channel_logo_url")
            resolved_stream_name = resolved_stream_name or resolved.get("stream_name")
            resolved_source_url = resolved_source_url or resolved.get("source_url")
            resolved_display_url = resolved_display_url or resolved.get("display_url")

        async with self.lock:
            # 1. Update existing active session
            session = self.sessions.get(connection_id)
            if session:
                session["last_seen"] = now
                session["ip_address"] = ip_address
                session["user_agent"] = user_agent
                if details_override:
                    session["details"] = details_override

                # Update identity only if the new one is "better" (not a segment) or we don't have one
                # OR if the new one resolves to a channel and the current one doesn't.
                is_better = canonical_identity and (not session.get(
                    "identity") or self._is_segment(session.get("identity")))
                if is_better and not self._is_segment(canonical_identity):
                    session["identity"] = canonical_identity

                if normalized_identity:
                    rel = session.setdefault("related_identities", [])
                    if normalized_identity not in rel:
                        rel.append(normalized_identity)

                # Enrichment (sticky)
                if channel_id and not session.get("channel_id"):
                    session["channel_id"] = channel_id
                if channel_name and not session.get("channel_name"):
                    session["channel_name"] = channel_name
                if channel_logo_url and not session.get("channel_logo_url"):
                    session["channel_logo_url"] = channel_logo_url
                if resolved_stream_name and not session.get("stream_name"):
                    session["stream_name"] = resolved_stream_name
                if resolved_source_url and not session.get("source_url"):
                    session["source_url"] = resolved_source_url
                if resolved_display_url and not session.get("display_url"):
                    session["display_url"] = resolved_display_url
                return "touched"

            # 2. Rehydrate from history
            history_item = self.history.pop(connection_id, None)
            if history_item:
                session = history_item["entry"]
                session["last_seen"] = now
                session["ip_address"] = ip_address
                session["user_agent"] = user_agent
                if details_override:
                    session["details"] = details_override

                # Enrichment (sticky)
                if channel_id and not session.get("channel_id"):
                    session["channel_id"] = channel_id
                if channel_name and not session.get("channel_name"):
                    session["channel_name"] = channel_name
                if channel_logo_url and not session.get("channel_logo_url"):
                    session["channel_logo_url"] = channel_logo_url
                if resolved_stream_name and not session.get("stream_name"):
                    session["stream_name"] = resolved_stream_name
                if resolved_source_url and not session.get("source_url"):
                    session["source_url"] = resolved_source_url
                if resolved_display_url and not session.get("display_url"):
                    session["display_url"] = resolved_display_url

                if canonical_identity and (not session.get("identity") or self._is_segment(session.get("identity"))):
                    if not self._is_segment(canonical_identity):
                        session["identity"] = canonical_identity
                if normalized_identity:
                    rel = session.setdefault("related_identities", [])
                    if normalized_identity not in rel:
                        rel.append(normalized_identity)

                self.sessions[connection_id] = session
                if perform_audit:
                    audit_user = user if user_id else _AuditUser(user_id, username, stream_key)
                    await audit_stream_event(
                        audit_user,
                        event_type,
                        endpoint_override or session.get("endpoint") or "",
                        details=session.get("details") or canonical_identity,
                    )
                return "rehydrated"

            # 3. Create new session
            base_endpoint = endpoint_override
            if not base_endpoint:
                try:
                    base_endpoint = request.path
                except Exception:
                    base_endpoint = ""

            session = {
                "connection_id": connection_id,
                "identity": canonical_identity,
                "details": details_override or canonical_identity,
                "started_at": now,
                "last_seen": now,
                "user_id": user_id,
                "username": username,
                "stream_key": stream_key,
                "endpoint": base_endpoint,
                "ip_address": ip_address,
                "user_agent": user_agent,
                "related_identities": [normalized_identity] if normalized_identity else [],
                "channel_id": channel_id,
                "channel_name": channel_name,
                "channel_logo_url": channel_logo_url,
                "stream_name": resolved_stream_name,
                "source_url": resolved_source_url,
                "display_url": resolved_display_url,
            }
            self.sessions[connection_id] = session

            if perform_audit:
                audit_user = user if user_id else _AuditUser(user_id, username, stream_key)
                await audit_stream_event(
                    audit_user,
                    event_type,
                    base_endpoint,
                    details=session["details"],
                )
            return "started"

    async def touch(self, connection_id, identity=None, ip_address=None, user_agent=None):
        if not connection_id:
            return False
        now = time.time()
        async with self.lock:
            if connection_id in self.sessions:
                session = self.sessions[connection_id]
                session["last_seen"] = now
                if ip_address:
                    session["ip_address"] = ip_address
                if user_agent:
                    session["user_agent"] = user_agent
                if identity:
                    rel = session.setdefault("related_identities", [])
                    if identity not in rel:
                        rel.append(identity)
                    # If current identity is a segment but new one isn't, upgrade it
                    if identity and not self._is_segment(identity) and self._is_segment(session.get("identity")):
                        session["identity"] = identity
                return True
        return False

    async def stop(
        self,
        connection_id,
        event_type="stream_stop",
        endpoint_override=None,
        user=None,
        ip_address=None,
        user_agent=None,
        perform_audit=True,
    ):
        if not connection_id:
            return False

        now = time.time()
        async with self.lock:
            session = self.sessions.pop(connection_id, None)
            if not session:
                return False
            self.history[connection_id] = {"last_seen": now, "entry": session}

        if perform_audit:
            user_id = session.get("user_id")
            username = session.get("username")
            stream_key = session.get("stream_key")
            audit_user = user if user is not None else _AuditUser(user_id, username, stream_key)
            await audit_stream_event(
                audit_user,
                event_type,
                endpoint_override or session.get("endpoint") or "",
                details=session.get("details") or session.get("identity"),
                ip_address=ip_address or session.get("ip_address"),
                user_agent=user_agent or session.get("user_agent"),
            )
        return True

    async def snapshot(self):
        now = time.time()
        async with self.lock:
            entries = []
            for cid, s in self.sessions.items():
                started_at = float(s.get("started_at") or now)
                entries.append(
                    {
                        "connection_id": cid,
                        "identity": s.get("identity"),
                        "user_id": s.get("user_id"),
                        "username": s.get("username"),
                        "stream_key": s.get("stream_key"),
                        "endpoint": s.get("endpoint"),
                        "details": s.get("details"),
                        "ip_address": s.get("ip_address"),
                        "user_agent": s.get("user_agent"),
                        "started_at": started_at,
                        "last_seen": s.get("last_seen"),
                        "active_seconds": max(int(now - started_at), 0),
                        "age_seconds": max(int(now - (s.get("last_seen") or now)), 0),
                        "related_urls": list(s.get("related_identities", [])),
                        "channel_id": s.get("channel_id"),
                        "channel_name": s.get("channel_name"),
                        "channel_logo_url": s.get("channel_logo_url"),
                        "stream_name": s.get("stream_name"),
                        "source_url": s.get("source_url"),
                        "display_url": s.get("display_url"),
                    }
                )
        entries.sort(key=lambda item: (item.get("started_at") or 0, str(item.get("connection_id") or "")))
        return entries

    async def cleanup(self):
        now = time.time()
        expired_active = []
        async with self.lock:
            # Expire active sessions
            for cid, s in list(self.sessions.items()):
                if now - s["last_seen"] > self.activity_ttl:
                    expired_active.append((cid, s))

            for cid, s in expired_active:
                self.sessions.pop(cid)
                self.history[cid] = {"last_seen": now, "entry": s}

            # Prune history
            for cid, h in list(self.history.items()):
                if now - h["last_seen"] > self.history_ttl:
                    self.history.pop(cid)

            # Prune playlist parents (simple time-based prune)
            if len(self.playlist_parents) > 1000:
                self.playlist_parents.clear()

        # Audit stops for expired sessions
        for cid, s in expired_active:
            if str(cid).startswith("tvh-"):
                # TVH sessions are explicitly audited by the poller task to avoid duplicates
                # caused by small TTL windows or app restarts.
                continue
            audit_user = _AuditUser(s.get("user_id"), s.get("username"), s.get("stream_key"))
            await audit_stream_event(
                audit_user,
                "stream_stop",
                s.get("endpoint") or "",
                details=s.get("details"),
                ip_address=s.get("ip_address"),
                user_agent=s.get("user_agent"),
            )

    async def register_playlist_parent(self, child_url: str, parent_url: str):
        if not child_url or not parent_url or child_url == parent_url:
            return
        async with self.lock:
            self.playlist_parents[child_url] = parent_url

    async def save_state(self, file_path: str):
        if not file_path:
            return False
        async with self.lock:
            payload = {
                "version": 3,
                "saved_at": time.time(),
                "sessions": self.sessions,
                "history": self.history,
            }

        directory = os.path.dirname(file_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        temp_path = f"{file_path}.tmp"
        try:
            with open(temp_path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, separators=(",", ":"), ensure_ascii=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(temp_path, file_path)
            return True
        except Exception:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass
            return False

    async def load_state(self, file_path: str):
        if not file_path or not os.path.exists(file_path):
            return False
        try:
            with open(file_path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except Exception:
            return False

        async with self.lock:
            self.sessions = payload.get("sessions") or {}
            self.history = payload.get("history") or {}
        return True


_stream_activity_tracker = StreamActivityTracker(activity_ttl=20)


async def _mark_stream_activity(decoded_url, event_type="stream_start"):
    connection_id = _get_connection_id(default_new=False)
    await _stream_activity_tracker.mark(decoded_url, event_type=event_type, connection_id=connection_id)


async def _touch_stream_activity(decoded_url):
    connection_id = _get_connection_id(default_new=False)
    ip_address = get_request_client_ip()
    user_agent = request.headers.get("User-Agent")
    await _stream_activity_tracker.touch(
        connection_id=connection_id,
        identity=decoded_url,
        ip_address=ip_address,
        user_agent=user_agent,
    )


async def _register_playlist_parent(child_url: str, parent_url: str):
    # This is now a no-op in the simplified tracker
    pass


async def _cleanup_stream_activity():
    await _stream_activity_tracker.cleanup()


async def get_stream_activity_snapshot():
    return await _stream_activity_tracker.snapshot()


def _stream_activity_state_path() -> str | None:
    app_config = current_app.config.get("APP_CONFIG") if current_app else None
    if not app_config:
        return None
    return os.path.join(app_config.config_path, "cache", "stream_activity_state.json")


async def persist_stream_activity_state():
    file_path = _stream_activity_state_path()
    if not file_path:
        return False
    success = await _stream_activity_tracker.save_state(file_path)
    if success:
        proxy_logger.debug(f"Persisted stream activity state to {file_path}")
    else:
        proxy_logger.warning(f"Failed to persist stream activity state to {file_path}")
    return success


async def load_stream_activity_state():
    file_path = _stream_activity_state_path()
    if not file_path:
        return False
    return await _stream_activity_tracker.load_state(file_path)


async def upsert_stream_activity(
    identity: str,
    connection_id: str | None = None,
    endpoint_override: str | None = None,
    start_event_type: str = "stream_start",
    user=None,
    ip_address=None,
    user_agent=None,
    perform_audit=True,
    details_override: str | None = None,
    channel_id=None,
    channel_name=None,
    channel_logo_url=None,
    stream_name=None,
    source_url=None,
    display_url=None,
):
    return await _stream_activity_tracker.mark(
        identity,
        event_type=start_event_type,
        connection_id=connection_id,
        endpoint_override=endpoint_override,
        user=user,
        ip_address=ip_address,
        user_agent=user_agent,
        perform_audit=perform_audit,
        details_override=details_override,
        channel_id=channel_id,
        channel_name=channel_name,
        channel_logo_url=channel_logo_url,
        stream_name=stream_name,
        source_url=source_url,
        display_url=display_url,
    )


async def stop_stream_activity(
    identity: str,
    connection_id: str | None = None,
    event_type: str = "stream_stop",
    endpoint_override: str | None = None,
    user=None,
    ip_address=None,
    user_agent=None,
    perform_audit=True,
):
    return await _stream_activity_tracker.stop(
        connection_id=connection_id,
        event_type=event_type,
        endpoint_override=endpoint_override,
        user=user,
        ip_address=ip_address,
        user_agent=user_agent,
        perform_audit=perform_audit,
    )
# A dictionary to keep track of active streams
active_streams = {}
active_streams_lock = asyncio.Lock()


class BaseStreamMultiplexer:
    def __init__(self, decoded_url):
        self.decoded_url = decoded_url
        self.queues = {}  # connection_id -> asyncio.Queue
        self.running = False
        self.lock = asyncio.Lock()
        self.last_activity = time.time()
        self.history = deque()
        self.history_bytes = 0
        self.max_history_bytes = 200 * 1024 * 1024  # 200MB total history cap

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
        async with self.lock:
            self.queues.pop(connection_id, None)
            proxy_logger.info(
                f"Removed queue {connection_id} for {self.decoded_url}, count: {len(self.queues)}"
            )
            if not self.queues:
                proxy_logger.info("No more connections for %s, stopping.", self.decoded_url)
                asyncio.create_task(self.stop())

    async def stop(self):
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

    def __init__(self, decoded_url):
        super().__init__(decoded_url)
        self.process = None
        self.read_task = None
        self.stderr_task = None

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
            await self.stop()

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

    async def stop(self):
        async with self.lock:
            if not self.running:
                return
            self.running = False
            if self.process:
                try:
                    self.process.terminate()
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

            async with active_streams_lock:
                key = (self.decoded_url, "ffmpeg")
                if active_streams.get(key) == self:
                    del active_streams[key]
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

    def __init__(self, decoded_url, headers):
        super().__init__(decoded_url)
        self.headers = headers
        self.read_task = None

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
            await self.stop()

    async def stop(self):
        async with self.lock:
            if not self.running:
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

            async with active_streams_lock:
                key = (self.decoded_url, "direct")
                if active_streams.get(key) == self:
                    del active_streams[key]
        proxy_logger.info("DirectStream for %s cleaned up.", self.decoded_url)


async def direct_proxy_generator(decoded_url, headers, prebuffer_bytes=0):
    # This is now a wrapper around AsyncDirectStream to support shared multiplexing even in "direct" mode
    connection_id = str(uuid.uuid4())
    key = (decoded_url, "direct")

    async with active_streams_lock:
        if key not in active_streams:
            stream = AsyncDirectStream(decoded_url, headers)
            active_streams[key] = stream
            await stream.start()
        else:
            stream = active_streams[key]

    queue, primed_bytes = await stream.add_queue(connection_id, prebuffer_bytes=prebuffer_bytes)

    # Strictly enforce prebuffer: wait for remaining cushion if history was insufficient
    cushion_remaining = prebuffer_bytes - primed_bytes
    temp_buffer = []

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


class Cache:
    def __init__(self, ttl=3600):
        self.cache = {}
        self.expiration_times = {}
        self._lock = asyncio.Lock()
        self.ttl = ttl
        self.max_size = 100  # Limit cache size to prevent memory issues

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


async def cleanup_hls_proxy_state():
    """Cleanup expired cache entries and idle stream activity."""
    try:
        evicted_count = await cache.evict_expired_items()
        if evicted_count > 0:
            proxy_logger.info(
                f"Cache cleanup: evicted {evicted_count} expired items"
            )

        await _cleanup_stream_activity()

        # Log current memory usage (optional)
        try:
            import psutil

            process = psutil.Process()
            memory_info = process.memory_info()
            proxy_logger.debug(
                f"Current memory usage: {memory_info.rss / (1024 * 1024):.2f} MB"
            )
        except Exception:
            # Silently skip if psutil not installed or fails
            pass

    except Exception as e:
        proxy_logger.error(f"Error during cache cleanup: {e}")


# Global cache instance (short default TTL for HLS segments)
cache = Cache(ttl=120)


async def prefetch_segments(segment_urls, headers=None):
    async with aiohttp.ClientSession() as session:
        for url in segment_urls:
            if not await cache.exists(url):
                proxy_logger.info("[CACHE] Saved URL '%s' to cache", url)
                try:
                    async with session.get(url, headers=headers) as resp:
                        if resp.status == 200:
                            content = await resp.read()
                            await cache.set(
                                url, content, expiration_time=30
                            )  # Cache for 30 seconds
                except Exception as e:
                    proxy_logger.error("Failed to prefetch URL '%s': %s", url, e)


def _build_proxy_base_url():
    instance_id = _get_instance_id()
    base_path = hls_proxy_prefix.rstrip("/")
    if instance_id:
        base_path = f"{base_path}/{instance_id}"
    if hls_proxy_host_ip:
        host_base_url_prefix = "http"
        host_base_url_port = ""
        if hls_proxy_port:
            if hls_proxy_port == "443":
                host_base_url_prefix = "https"
            host_base_url_port = f":{hls_proxy_port}"
        return f"{host_base_url_prefix}://{hls_proxy_host_ip}{host_base_url_port}{base_path}"
    return f"{request.host_url.rstrip('/')}{base_path}"


def _infer_extension(url_value):
    parsed = urlparse(url_value)
    path = (parsed.path or "").lower()
    if path.endswith(".m3u8"):
        return "m3u8"
    if path.endswith(".key"):
        return "key"
    if path.endswith(".vtt"):
        return "vtt"
    return "ts"


def _build_upstream_headers():
    headers = {}
    try:
        src = request.headers
    except Exception:
        return headers
    for name in ("User-Agent", "Referer", "Origin", "Accept", "Accept-Language"):
        value = src.get(name)
        if value:
            headers[name] = value
    return headers


def _b64_urlsafe_encode(value):
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("utf-8")


def _b64_urlsafe_decode(value):
    padded = value + "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode(padded).decode("utf-8")
    except Exception:
        # Fallback for older non-urlsafe tokens
        return base64.b64decode(padded).decode("utf-8")


def _get_connection_id(default_new=False):
    value = (request.args.get("connection_id") or request.args.get("cid") or "").strip()
    if value:
        return value
    if default_new:
        return uuid.uuid4().hex
    return None


def _parse_size(size_str: str, default: int = 0) -> int:
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


def generate_base64_encoded_url(
    url_to_encode, extension, stream_key=None, username=None, connection_id=None
):
    full_url_encoded = _b64_urlsafe_encode(url_to_encode)
    base_url = _build_proxy_base_url().rstrip("/")
    url = f"{base_url}/{full_url_encoded}.{extension}"
    query = []
    if username and stream_key:
        query.append(("username", username))
    if stream_key:
        query.append(("stream_key", stream_key))
    if connection_id:
        query.append(("connection_id", connection_id))
    if query:
        return f"{url}?{urlencode(query)}"
    return url


def _rewrite_uri_value(
    uri_value, source_url, stream_key=None, username=None, connection_id=None, forced_extension=None
):
    absolute_url = urljoin(source_url, uri_value)
    extension = forced_extension or _infer_extension(absolute_url)
    return (
        generate_base64_encoded_url(
            absolute_url,
            extension,
            stream_key=stream_key,
            username=username,
            connection_id=connection_id,
        ),
        absolute_url,
        extension,
    )


async def update_child_urls(
    playlist_content,
    source_url,
    stream_key=None,
    username=None,
    connection_id=None,
    headers=None,
):
    proxy_logger.debug(f"Original Playlist Content:\n{playlist_content}")

    updated_lines = []
    lines = playlist_content.splitlines()
    segment_urls = []
    state = {
        "next_is_playlist": False,
        "next_is_segment": False,
    }

    for line in lines:
        stripped_line = line.strip()
        if stripped_line and not stripped_line.startswith("#"):
            absolute_url = urljoin(source_url, stripped_line)
            await _register_playlist_parent(absolute_url, source_url)
        updated_line, new_segment_urls = rewrite_playlist_line(
            line,
            source_url,
            state,
            stream_key=stream_key,
            username=username,
            connection_id=connection_id,
        )
        if updated_line:
            updated_lines.append(updated_line)
        if new_segment_urls:
            segment_urls.extend(new_segment_urls)

    if segment_urls:
        asyncio.create_task(prefetch_segments(segment_urls, headers=headers))

    modified_playlist = "\n".join(updated_lines)
    proxy_logger.debug(f"Modified Playlist Content:\n{modified_playlist}")
    return modified_playlist


def rewrite_playlist_line(line, source_url, state, stream_key=None, username=None, connection_id=None):
    stripped_line = line.strip()
    if not stripped_line:
        return None, []

    segment_urls = []
    if stripped_line.startswith("#"):
        upper_line = stripped_line.upper()
        if upper_line.startswith("#EXT-X-STREAM-INF"):
            state["next_is_playlist"] = True
        elif upper_line.startswith("#EXTINF"):
            state["next_is_segment"] = True

        def replace_uri(match):
            original_uri = match.group(1)
            forced_extension = None
            if "#EXT-X-KEY" in upper_line:
                forced_extension = "key"
            elif (
                "#EXT-X-MEDIA" in upper_line
                or "#EXT-X-I-FRAME-STREAM-INF" in upper_line
            ):
                forced_extension = "m3u8"
            new_uri, absolute_url, extension = _rewrite_uri_value(
                original_uri,
                source_url,
                stream_key=stream_key,
                username=username,
                connection_id=connection_id,
                forced_extension=forced_extension,
            )
            if extension in ("ts", "vtt", "key"):
                segment_urls.append(absolute_url)
            return f'URI="{new_uri}"'

        updated_line = re.sub(r'URI="([^"]+)"', replace_uri, line)
        return updated_line, segment_urls

    absolute_url = urljoin(source_url, stripped_line)
    if state.get("next_is_playlist"):
        extension = "m3u8"
        state["next_is_playlist"] = False
        state["next_is_segment"] = False
    elif state.get("next_is_segment"):
        extension = "ts"
        state["next_is_segment"] = False
    else:
        extension = _infer_extension(absolute_url)
    if extension in ("ts", "vtt", "key"):
        segment_urls.append(absolute_url)
    return generate_base64_encoded_url(
        absolute_url,
        extension,
        stream_key=stream_key,
        username=username,
        connection_id=connection_id,
    ), segment_urls


async def _stream_updated_playlist(resp, response_url, stream_key=None, username=None, connection_id=None):
    buffer = ""
    state = {
        "next_is_playlist": False,
        "next_is_segment": False,
    }
    async for chunk in resp.content.iter_chunked(8192):
        buffer += chunk.decode("utf-8", errors="ignore")
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            updated_line, _ = rewrite_playlist_line(
                line,
                response_url,
                state,
                stream_key=stream_key,
                username=username,
                connection_id=connection_id,
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
            connection_id=connection_id,
        )
        if updated_line:
            yield updated_line + "\n"


async def fetch_and_update_playlist(decoded_url, stream_key=None, username=None, connection_id=None, headers=None):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(decoded_url, headers=headers) as resp:
                if resp.status != 200:
                    return None, None, False

                response_url = str(resp.url)
                content_type = (
                    resp.headers.get("Content-Type") or "application/vnd.apple.mpegurl"
                )
                content_length = resp.content_length or 0

                if content_length and content_length > hls_proxy_max_buffer_bytes:
                    return (
                        _stream_updated_playlist(
                            resp,
                            response_url,
                            stream_key=stream_key,
                            username=username,
                            connection_id=connection_id,
                        ),
                        content_type,
                        True,
                    )

                playlist_content = await resp.text()
                updated_playlist = await update_child_urls(
                    playlist_content,
                    response_url,
                    stream_key=stream_key,
                    username=username,
                    connection_id=connection_id,
                    headers=headers,
                )
                return updated_playlist, content_type, False
        except aiohttp.ClientError as exc:
            proxy_logger.warning("HLS proxy failed to fetch '%s': %s", decoded_url, exc)
            return None, None, False


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/<encoded_url>.m3u8",
    methods=["GET", "HEAD"],
)
@stream_key_required
@skip_stream_connect_audit
async def proxy_m3u8(instance_id, encoded_url):
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    connection_id = _get_connection_id(default_new=False)
    if not connection_id:
        params = [(k, v) for k, v in request.args.items() if k not in {"cid", "connection_id"}]
        connection_id = _get_connection_id(default_new=True)
        params.append(("connection_id", connection_id))
        target = f"{hls_proxy_prefix.rstrip('/')}/{instance_id}/{encoded_url}.m3u8?{urlencode(params)}"
        return redirect(target, code=302)

    decoded_url = _b64_urlsafe_decode(encoded_url)
    await _mark_stream_activity(decoded_url)
    stream_key = request.args.get("stream_key") or request.args.get("password")
    username = request.args.get("username")

    headers = _build_upstream_headers()
    updated_playlist, content_type, is_stream = await fetch_and_update_playlist(
        decoded_url,
        stream_key=stream_key,
        username=username,
        connection_id=connection_id,
        headers=headers,
    )
    if updated_playlist is None:
        proxy_logger.error("Failed to fetch the original playlist '%s'", decoded_url)
        response = Response("Failed to fetch the original playlist.", status=502)
        response.headers["X-TIC-Proxy-Error"] = "upstream-unreachable"
        return response

    proxy_logger.info(f"[MISS] Serving m3u8 URL '%s' without cache", decoded_url)
    if is_stream:
        return Response(stream_with_context(updated_playlist), content_type=content_type)
    return Response(updated_playlist, content_type=content_type)


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/proxy.m3u8",
    methods=["GET", "HEAD"],
)
@stream_key_required
@skip_stream_connect_audit
async def proxy_m3u8_redirect(instance_id):
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    url = request.args.get("url")
    if not url:
        return Response("Missing url parameter.", status=400)
    encoded = _b64_urlsafe_encode(url)
    stream_key = request.args.get("stream_key") or request.args.get("password")
    username = request.args.get("username")
    connection_id = _get_connection_id(default_new=True)
    target = f"{hls_proxy_prefix.rstrip('/')}/{instance_id}/{encoded}.m3u8"
    query = []
    if username and stream_key:
        query.append(("username", username))
    if stream_key:
        query.append(("stream_key", stream_key))
    if connection_id:
        query.append(("connection_id", connection_id))
    if query:
        target = f"{target}?{urlencode(query)}"
    return redirect(target, code=302)


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/<encoded_url>.key",
    methods=["GET", "HEAD"],
)
@stream_key_required
@skip_stream_connect_audit
async def proxy_key(instance_id, encoded_url):
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    # Decode the Base64 encoded URL
    decoded_url = _b64_urlsafe_decode(encoded_url)
    await _touch_stream_activity(decoded_url)

    # Check if the .key file is already cached
    if await cache.exists(decoded_url):
        proxy_logger.info(f"[HIT] Serving key URL from cache: %s", decoded_url)
        cached_content = await cache.get(decoded_url)
        return Response(cached_content, content_type="application/octet-stream")

    # If not cached, fetch the file and cache it
    proxy_logger.info(f"[MISS] Serving key URL '%s' without cache", decoded_url)
    headers = _build_upstream_headers()
    async with aiohttp.ClientSession() as session:
        async with session.get(decoded_url, headers=headers) as resp:
            if resp.status != 200:
                proxy_logger.error("Failed to fetch key file '%s'", decoded_url)
                return Response("Failed to fetch the file.", status=404)
            content = await resp.read()
            await cache.set(
                decoded_url, content, expiration_time=30
            )  # Cache for 30 seconds
            return Response(content, content_type="application/octet-stream")


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/<encoded_url>.ts",
    methods=["GET", "HEAD"],
)
@stream_key_required
@skip_stream_connect_audit
async def proxy_ts(instance_id, encoded_url):
    """
    TIC Legacy/Segment Proxy Endpoint

    This endpoint serves individual .ts segments for HLS or provides a 
    direct stream when configured.

    Parameters:
    - ffmpeg=true: (Optional) Fallback to FFmpeg remuxer for better compatibility.
    - prebuffer=X: (Optional) Buffer size cushion (e.g. 2M, 512K). Default: 1M.
    """
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    # Decode the Base64 encoded URL
    decoded_url = _b64_urlsafe_decode(encoded_url)
    await _touch_stream_activity(decoded_url)

    # Mode and prebuffer checks
    use_ffmpeg = request.args.get("ffmpeg", "false").lower() == "true"
    prebuffer_bytes = _parse_size(request.args.get("prebuffer"), default=1048576)

    # For .ts segments in a standard HLS playlist, we usually want direct speed.
    # However, if explicitly asked for ffmpeg mode or a prebuffer, we route
    # through the multiplexer logic.
    if use_ffmpeg or request.args.get("prebuffer"):
        proxy_logger.info("Routing .ts request through multiplexer (ffmpeg=%s, prebuffer=%d)",
                          use_ffmpeg, prebuffer_bytes)
        # We redirect to the /stream/ endpoint to ensure we use the shared multiplexer
        target = f"{hls_proxy_prefix.rstrip('/')}/{instance_id}/stream/{encoded_url}"
        if request.query_string:
            target = f"{target}?{request.query_string.decode()}"
        return redirect(target, code=302)

    # Check if the .ts file is already cached
    if await cache.exists(decoded_url):
        proxy_logger.info(f"[HIT] Serving ts URL from cache: %s", decoded_url)
        cached_content = await cache.get(decoded_url)
        return Response(cached_content, content_type="video/mp2t")

    # If not cached, fetch the file and cache it
    proxy_logger.info(f"[MISS] Serving ts URL '%s' without cache", decoded_url)
    headers = _build_upstream_headers()
    async with aiohttp.ClientSession() as session:
        async with session.get(decoded_url, headers=headers) as resp:
            if resp.status != 200:
                proxy_logger.error("Failed to fetch ts file '%s'", decoded_url)
                return Response("Failed to fetch the file.", status=404)
            content = await resp.read()
            content_type = (resp.headers.get("Content-Type") or "").lower()

            # If upstream is actually a playlist, redirect to the .m3u8 endpoint.
            # This handles cases where the proxy URL was built with a .ts suffix
            # but the upstream URL is a master/variant playlist.
            if "mpegurl" in content_type or content.lstrip().startswith(b"#EXTM3U"):
                target = f"{hls_proxy_prefix.rstrip('/')}/{instance_id}/{encoded_url}.m3u8"
                if request.query_string:
                    target = f"{target}?{request.query_string.decode()}"
                return redirect(target, code=302)

            await cache.set(
                decoded_url, content, expiration_time=30
            )  # Cache for 30 seconds
            return Response(content, content_type="video/mp2t")


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/<encoded_url>.vtt",
    methods=["GET", "HEAD"],
)
@stream_key_required
@skip_stream_connect_audit
async def proxy_vtt(instance_id, encoded_url):
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    # Decode the Base64 encoded URL
    decoded_url = _b64_urlsafe_decode(encoded_url)
    await _touch_stream_activity(decoded_url)

    # Check if the .vtt file is already cached
    if await cache.exists(decoded_url):
        proxy_logger.info(f"[HIT] Serving vtt URL from cache: %s", decoded_url)
        cached_content = await cache.get(decoded_url)
        return Response(cached_content, content_type="text/vtt")

    # If not cached, fetch the file and cache it
    proxy_logger.info(f"[MISS] Serving vtt URL '%s' without cache", decoded_url)
    headers = _build_upstream_headers()
    async with aiohttp.ClientSession() as session:
        async with session.get(decoded_url, headers=headers) as resp:
            if resp.status != 200:
                proxy_logger.error("Failed to fetch vtt file '%s'", decoded_url)
                return Response("Failed to fetch the file.", status=404)
            content = await resp.read()
            await cache.set(
                decoded_url, content, expiration_time=30
            )  # Cache for 30 seconds
            return Response(content, content_type="text/vtt")


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/stream/<encoded_url>",
    methods=["GET"],
)
@stream_key_required
@skip_stream_connect_audit
async def stream_ts(instance_id, encoded_url):
    """
    TIC Shared Multiplexer Stream Endpoint

    This endpoint provides a shared upstream connection for multiple TIC clients.

    Default Mode (Direct):
    Uses high-performance async socket reads. Best for 99% of streams.

    Fallback Mode (FFmpeg):
    Enabled by appending '?ffmpeg=true'. Uses an external FFmpeg process to 
    remux/clean the stream. Use this ONLY if 'direct' mode has playback issues.

    Parameters:
    - ffmpeg=true: (Optional) Fallback to FFmpeg remuxer for better compatibility.
    - prebuffer=X: (Optional) Buffer size cushion (e.g. 2M, 512K). Default: 1M.
    """
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    # Decode the Base64 encoded URL
    decoded_url = _b64_urlsafe_decode(encoded_url)
    await _mark_stream_activity(decoded_url)

    # Check for mode and prebuffer settings
    use_ffmpeg = request.args.get("ffmpeg", "false").lower() == "true"
    prebuffer_bytes = _parse_size(request.args.get("prebuffer"), default=1048576)
    headers = _build_upstream_headers()

    # Generate a unique identifier (UUID) for the connection
    connection_id = str(uuid.uuid4())
    mode = "ffmpeg" if use_ffmpeg else "direct"
    key = (decoded_url, mode)

    async with active_streams_lock:
        if key not in active_streams:
            if use_ffmpeg:
                stream = AsyncFFmpegStream(decoded_url)
            else:
                stream = AsyncDirectStream(decoded_url, headers)
            active_streams[key] = stream
            await stream.start()
        else:
            stream = active_streams[key]

    # Get the existing stream and update last activity time
    stream.last_activity = time.time()

    # Add a new queue for this connection with history priming
    queue, primed_bytes = await stream.add_queue(connection_id, prebuffer_bytes=prebuffer_bytes)
    if not is_tvh_backend_stream_user(getattr(request, "_stream_user", None)):
        await audit_stream_event(request._stream_user, "hls_stream_connect", request.path)

    # Strictly enforce prebuffer: wait for remaining cushion if history was insufficient
    cushion_remaining = prebuffer_bytes - primed_bytes
    temp_buffer = []

    # Create a generator to stream data from the connection-specific queue
    @stream_with_context
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

    # Create a response object with the correct content type and set timeout to None
    response = Response(generate(), content_type="video/mp2t")
    response.timeout = None  # Disable timeout for streaming response
    return response
