#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from urllib.parse import urlparse

from quart import current_app, request

from backend.auth import audit_stream_event, get_request_client_ip, is_tvh_backend_stream_user

logger = logging.getLogger("stream_activity")


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
        source_id=None,
        playlist_id=None,
        xc_account_id=None,
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

        from backend.channels import build_stream_source_index, normalize_url, resolve_stream_target
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
        resolved_source_id = source_id
        resolved_playlist_id = playlist_id
        resolved_xc_account_id = xc_account_id
        if not channel_name and not existing_channel_name:
            source_index = await build_stream_source_index()
            # Try resolution with both canonical and raw identity
            resolved = resolve_stream_target(canonical_identity, source_index, related_urls=[normalized_identity])
            channel_id = channel_id or resolved.get("channel_id")
            channel_name = channel_name or resolved.get("channel_name")
            channel_logo_url = channel_logo_url or resolved.get("channel_logo_url")
            resolved_source_id = resolved_source_id or resolved.get("source_id")
            resolved_playlist_id = resolved_playlist_id or resolved.get("playlist_id")
            resolved_xc_account_id = resolved_xc_account_id or resolved.get("xc_account_id")
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
                if resolved_source_id and not session.get("source_id"):
                    session["source_id"] = resolved_source_id
                if resolved_playlist_id and not session.get("playlist_id"):
                    session["playlist_id"] = resolved_playlist_id
                if resolved_xc_account_id and not session.get("xc_account_id"):
                    session["xc_account_id"] = resolved_xc_account_id
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
                if resolved_source_id and not session.get("source_id"):
                    session["source_id"] = resolved_source_id
                if resolved_playlist_id and not session.get("playlist_id"):
                    session["playlist_id"] = resolved_playlist_id
                if resolved_xc_account_id and not session.get("xc_account_id"):
                    session["xc_account_id"] = resolved_xc_account_id

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
                "source_id": resolved_source_id,
                "playlist_id": resolved_playlist_id,
                "xc_account_id": resolved_xc_account_id,
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
                        "source_id": s.get("source_id"),
                        "playlist_id": s.get("playlist_id"),
                        "xc_account_id": s.get("xc_account_id"),
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


def _stream_activity_state_path() -> str | None:
    app_config = current_app.config.get("APP_CONFIG") if current_app else None
    if not app_config:
        return None
    return os.path.join(app_config.config_path, "cache", "stream_activity_state.json")


async def cleanup_stream_activity():
    await _stream_activity_tracker.cleanup()


async def get_stream_activity_snapshot():
    return await _stream_activity_tracker.snapshot()


async def touch_stream_activity(connection_id: str | None, identity: str | None = None):
    if not connection_id:
        return False
    return await _stream_activity_tracker.touch(connection_id, identity=identity)


async def persist_stream_activity_state():
    file_path = _stream_activity_state_path()
    if not file_path:
        return False
    success = await _stream_activity_tracker.save_state(file_path)
    if success:
        logger.debug("Persisted stream activity state to %s", file_path)
    else:
        logger.warning("Failed to persist stream activity state to %s", file_path)
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
    source_id=None,
    playlist_id=None,
    xc_account_id=None,
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
        source_id=source_id,
        playlist_id=playlist_id,
        xc_account_id=xc_account_id,
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
