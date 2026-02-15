#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import base64
import json
import logging
import os
import re
import subprocess
import threading
import time
import uuid
from collections import deque
from urllib.parse import urlencode, urljoin, urlparse

import aiohttp
import time
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
    In-memory tracker for HLS playback activity used by audit logging and dashboard status.

    High-level model:
    - A playback session is represented by an entry in `self.activity`.
    - A session is keyed by "<actor_scope>:<activity_identity>" where:
      - actor_scope = user_id, username, or "anon"
      - activity_identity = normalized URL identity for the stream
    - Session data stores user/client/request fields required for:
      - emitting `stream_start` / `stream_stop` audit events
      - reporting active sessions in `/tic-api/dashboard/activity`

    Why this class exists:
    - HLS clients request multiple related URLs for one logical playback
      (master m3u8, variant m3u8, segments, keys, subtitles).
    - Without correlation, those requests can appear as multiple sessions.
    - This tracker correlates those requests into one logical session.

    Correlation mechanisms:
    1. connection_id (primary)
       - The proxy appends `connection_id` to rewritten child URLs.
       - `self.connection_index` maps "<actor_scope>:<connection_id>" to an active
         activity key for fast and deterministic lookup.

    2. URL aliases (secondary)
       - `self.aliases` maps multiple URL candidates (root/master/variant/decoded)
         to the same activity key.
       - Used when connection_id is absent or when requests arrive on related URLs.

    3. Playlist parent mapping
       - `self.playlist_parents` tracks child->parent URL relationships discovered
         while rewriting playlists.
       - This allows canonicalization to root playlist identity and alias linking.

    Resume behavior for paused/interrupted clients:
    - Active sessions expire after `activity_ttl` seconds of inactivity.
    - On expiry, `stream_stop` is emitted and the active entry is removed.
    - Connection context is retained in `self.connection_history` for
      `connection_history_ttl` seconds (default 1 hour).
    - If the same client resumes with the same connection_id, metadata is rehydrated
      and a new `stream_start` is logged with preserved context.

    Concurrency model:
    - `activity`, `aliases`, `connection_index`, and `connection_history` are guarded
      by `activity_lock`.
    - `playlist_parents` is guarded by `playlist_parent_lock`.
    - Public methods are async and safe to call from concurrent requests.

    Main entry points:
    - `mark(...)`:
      Called on session-start style requests (typically m3u8 entrypoint).
      Creates or refreshes an active session and emits start audit when newly active.
    - `touch(...)`:
      Called on child/segment requests (.ts/.key/.vtt) to keep session alive.
    - `register_playlist_parent(...)`:
      Records parent-child playlist relationships discovered during rewrite.
    - `snapshot()`:
      Returns current active sessions for dashboard activity API.
    - `cleanup()`:
      Expires stale entries, emits stop audit events, and prunes caches/history.
    """

    def __init__(self, activity_ttl=20, alias_ttl=600, parent_ttl=600, connection_history_ttl=3600):
        self.activity = {}
        self.aliases = {}
        self.connection_index = {}
        self.connection_history = {}
        self.playlist_parents = {}
        self.activity_lock = asyncio.Lock()
        self.playlist_parent_lock = asyncio.Lock()
        self.activity_ttl = activity_ttl
        self.alias_ttl = alias_ttl
        self.parent_ttl = parent_ttl
        self.connection_history_ttl = connection_history_ttl

    @staticmethod
    def _actor_scope(user_id, username) -> str:
        return str(user_id or username or "anon")

    @staticmethod
    def _request_user():
        try:
            return getattr(request, "_stream_user", None) or getattr(request, "_current_user", None)
        except Exception:
            return None

    @staticmethod
    def _alias_key(actor_scope: str, url: str) -> str:
        return f"{actor_scope}:{url}"

    @staticmethod
    def _maybe_decode_embedded_url(value: str) -> str | None:
        try:
            parsed = urlparse(value)
        except Exception:
            return None
        tail = (parsed.path or "").rsplit("/", 1)[-1]
        if not tail:
            return None
        token = tail.split(".", 1)[0]
        if not token:
            return None
        padded = token + "=" * (-len(token) % 4)
        for decoder in (base64.urlsafe_b64decode, base64.b64decode):
            try:
                decoded = decoder(padded.encode("utf-8")).decode("utf-8")
            except Exception:
                continue
            if decoded.startswith(("http://", "https://")):
                return decoded
        return None

    def _resolve_playlist_root_unlocked(self, url: str) -> str:
        current = url
        seen = set()
        while current and current not in seen:
            seen.add(current)
            parent_entry = self.playlist_parents.get(current)
            if not parent_entry:
                break
            parent_url = parent_entry.get("parent")
            if not parent_url:
                break
            current = parent_url
        return current or url

    def _canonical_url_unlocked(self, decoded_url: str) -> str:
        # Keep canonical identity in the proxied URL chain first so
        # master/variant aliases from the same upstream map to one session.
        root = self._resolve_playlist_root_unlocked(decoded_url)
        return root or decoded_url

    def _activity_identity_unlocked(self, decoded_url: str) -> str:
        normalized_url = self._canonical_url_unlocked(decoded_url)
        try:
            parsed = urlparse(normalized_url)
            if not parsed.scheme or not parsed.netloc:
                return normalized_url
            path = parsed.path or "/"
            return f"{parsed.scheme}://{parsed.netloc}{path}"
        except Exception:
            return normalized_url

    def _activity_key_unlocked(self, user_id, username, decoded_url: str) -> str:
        actor_scope = self._actor_scope(user_id, username)
        return f"{actor_scope}:{self._activity_identity_unlocked(decoded_url)}"

    def _activity_url_candidates_unlocked(self, decoded_url: str) -> list[str]:
        values = []
        seen = set()
        initial = [decoded_url, self._canonical_url_unlocked(decoded_url)]
        for value in initial:
            if value and value not in seen:
                values.append(value)
                seen.add(value)
        current = values[-1] if values else None
        while current:
            parent_entry = self.playlist_parents.get(current)
            if not parent_entry:
                break
            parent_url = parent_entry.get("parent")
            if not parent_url or parent_url in seen:
                break
            values.append(parent_url)
            seen.add(parent_url)
            current = parent_url
        return values

    def _register_aliases_unlocked(self, actor_scope: str, activity_key: str, decoded_url: str, now: float):
        for candidate_url in self._activity_url_candidates_unlocked(decoded_url):
            alias_key = self._alias_key(actor_scope, candidate_url)
            self.aliases[alias_key] = {"activity_key": activity_key, "last_seen": now}

    def _store_connection_history_unlocked(self, connection_idx_key: str, payload: dict, now: float, disconnected=False):
        if not connection_idx_key:
            return
        self.connection_history[connection_idx_key] = {
            "last_seen": now,
            "disconnected_at": now if disconnected else None,
            "entry": {
                "user_id": payload.get("user_id"),
                "username": payload.get("username"),
                "stream_key": payload.get("stream_key"),
                "endpoint": payload.get("endpoint"),
                "details": payload.get("details"),
                "ip_address": payload.get("ip_address"),
                "user_agent": payload.get("user_agent"),
                "connection_id": payload.get("connection_id"),
            },
        }

    async def mark(self, decoded_url, event_type="stream_start", connection_id=None, endpoint_override=None):
        user = self._request_user()
        if is_tvh_backend_stream_user(user):
            return
        user_id = getattr(user, "id", None)
        username = getattr(user, "username", None)
        stream_key = getattr(user, "streaming_key", None)
        actor_scope = self._actor_scope(user_id, username)
        connection_idx_key = f"{actor_scope}:{connection_id}" if connection_id else None
        key = None
        now = time.time()
        ip_address = get_request_client_ip()
        user_agent = request.headers.get("User-Agent")
        history_entry = None
        async with self.activity_lock:
            if connection_idx_key:
                existing_key = self.connection_index.get(connection_idx_key, {}).get("activity_key")
                if existing_key and existing_key in self.activity:
                    key = existing_key
                history_entry = self.connection_history.get(connection_idx_key, {}).get("entry")
            if not key:
                key = self._activity_key_unlocked(user_id, username, decoded_url)
            entry = self.activity.get(key)
            if not entry:
                base_endpoint = endpoint_override or request.path
                base_details = decoded_url
                base_user_agent = user_agent
                base_ip = ip_address
                if history_entry:
                    base_endpoint = history_entry.get("endpoint") or base_endpoint
                    base_details = history_entry.get("details") or base_details
                    base_user_agent = history_entry.get("user_agent") or base_user_agent
                    base_ip = history_entry.get("ip_address") or base_ip
                self.activity[key] = {
                    "started_at": now,
                    "last_seen": now,
                    "user_id": user_id,
                    "username": username,
                    "stream_key": stream_key,
                    "endpoint": base_endpoint,
                    "details": base_details,
                    "ip_address": base_ip,
                    "user_agent": base_user_agent,
                    "connection_id": connection_id,
                }
                audit_user = user if user_id else _AuditUser(user_id, username, stream_key)
                await audit_stream_event(
                    audit_user,
                    event_type,
                    endpoint_override or request.path,
                    details=decoded_url,
                )
                entry = self.activity[key]
            else:
                entry["last_seen"] = now
                entry["ip_address"] = ip_address
                entry["user_agent"] = user_agent
                if connection_id and not entry.get("connection_id"):
                    entry["connection_id"] = connection_id
            if connection_idx_key:
                self.connection_index[connection_idx_key] = {
                    "activity_key": key,
                    "last_seen": now,
                }
                self._store_connection_history_unlocked(connection_idx_key, entry, now, disconnected=False)
            self._register_aliases_unlocked(actor_scope, key, decoded_url, now)

    async def touch(self, decoded_url, connection_id=None):
        user = self._request_user()
        if is_tvh_backend_stream_user(user):
            return False
        user_id = getattr(user, "id", None)
        username = getattr(user, "username", None)
        actor_scope = self._actor_scope(user_id, username)
        connection_idx_key = f"{actor_scope}:{connection_id}" if connection_id else None
        key = self._activity_key_unlocked(user_id, username, decoded_url)
        ip_address = get_request_client_ip()
        user_agent = request.headers.get("User-Agent")
        now = time.time()
        async with self.activity_lock:
            entry = None
            if connection_idx_key:
                existing_key = self.connection_index.get(connection_idx_key, {}).get("activity_key")
                if existing_key and existing_key in self.activity:
                    entry = self.activity[existing_key]
                    key = existing_key
            if not entry:
                entry = self.activity.get(key)
            if not entry:
                for candidate_url in self._activity_url_candidates_unlocked(decoded_url):
                    alias = self.aliases.get(self._alias_key(actor_scope, candidate_url))
                    if not alias:
                        continue
                    alias_key = alias.get("activity_key")
                    if alias_key and alias_key in self.activity:
                        entry = self.activity[alias_key]
                        key = alias_key
                        break
            if entry:
                entry["last_seen"] = now
                entry["ip_address"] = ip_address
                entry["user_agent"] = user_agent
                if connection_id and not entry.get("connection_id"):
                    entry["connection_id"] = connection_id
                if connection_idx_key:
                    self.connection_index[connection_idx_key] = {
                        "activity_key": key,
                        "last_seen": now,
                    }
                    self._store_connection_history_unlocked(connection_idx_key, entry, now, disconnected=False)
                self._register_aliases_unlocked(actor_scope, key, decoded_url, now)
                return True
        return False

    async def stop(self, decoded_url, connection_id=None, event_type="stream_stop", endpoint_override=None):
        user = self._request_user()
        if is_tvh_backend_stream_user(user):
            return False

        user_id = getattr(user, "id", None)
        username = getattr(user, "username", None)
        stream_key = getattr(user, "streaming_key", None)
        actor_scope = self._actor_scope(user_id, username)
        connection_idx_key = f"{actor_scope}:{connection_id}" if connection_id else None
        now = time.time()
        matched_key = None
        matched_entry = None

        async with self.activity_lock:
            if connection_idx_key:
                existing_key = self.connection_index.get(connection_idx_key, {}).get("activity_key")
                if existing_key and existing_key in self.activity:
                    matched_key = existing_key
                    matched_entry = self.activity.get(existing_key)

            if not matched_entry:
                candidate_key = self._activity_key_unlocked(user_id, username, decoded_url)
                if candidate_key in self.activity:
                    matched_key = candidate_key
                    matched_entry = self.activity.get(candidate_key)

            if not matched_entry:
                for candidate_url in self._activity_url_candidates_unlocked(decoded_url):
                    alias = self.aliases.get(self._alias_key(actor_scope, candidate_url))
                    if not alias:
                        continue
                    alias_key = alias.get("activity_key")
                    if alias_key and alias_key in self.activity:
                        matched_key = alias_key
                        matched_entry = self.activity.get(alias_key)
                        break

            if not matched_entry or not matched_key:
                return False

            removed = self.activity.pop(matched_key, None)
            if not removed:
                return False

            for alias_key, value in list(self.aliases.items()):
                if value.get("activity_key") == matched_key:
                    self.aliases.pop(alias_key, None)
            for idx_key, value in list(self.connection_index.items()):
                if value.get("activity_key") == matched_key:
                    self.connection_index.pop(idx_key, None)

            history_key = connection_idx_key
            if not history_key and removed.get("connection_id"):
                history_key = f"{actor_scope}:{removed.get('connection_id')}"
            if history_key:
                self._store_connection_history_unlocked(history_key, removed, now, disconnected=True)

        audit_user = user if user_id else _AuditUser(user_id, username, stream_key)
        await audit_stream_event(
            audit_user,
            event_type,
            endpoint_override or removed.get("endpoint") or request.path,
            details=removed.get("details") or decoded_url,
            ip_address=removed.get("ip_address"),
            user_agent=removed.get("user_agent"),
        )
        return True

    async def register_playlist_parent(self, child_url: str, parent_url: str):
        if not child_url or not parent_url or child_url == parent_url:
            return
        async with self.playlist_parent_lock:
            self.playlist_parents[child_url] = {"parent": parent_url, "last_seen": time.time()}
        await self._link_child_alias_to_parent(child_url, parent_url)

    async def _link_child_alias_to_parent(self, child_url: str, parent_url: str):
        user = self._request_user()
        if is_tvh_backend_stream_user(user):
            return
        actor_scope = self._actor_scope(getattr(user, "id", None), getattr(user, "username", None))
        now = time.time()
        async with self.activity_lock:
            parent_alias = self.aliases.get(self._alias_key(actor_scope, parent_url))
            if not parent_alias:
                return
            activity_key = parent_alias.get("activity_key")
            if not activity_key or activity_key not in self.activity:
                return
            self._register_aliases_unlocked(actor_scope, activity_key, child_url, now)

    async def snapshot(self):
        now = time.time()
        async with self.activity_lock:
            entries = []
            aliases_by_activity_key = {}
            for alias_key, alias_value in self.aliases.items():
                activity_key = alias_value.get("activity_key")
                if not activity_key:
                    continue
                if activity_key not in aliases_by_activity_key:
                    aliases_by_activity_key[activity_key] = []
                try:
                    _, url_value = alias_key.split(":", 1)
                except ValueError:
                    continue
                aliases_by_activity_key[activity_key].append(url_value)

            for activity_key, value in self.activity.items():
                started_at = float(value.get("started_at") or value.get("last_seen") or now)
                related_urls = []
                seen_urls = set()
                primary_details = value.get("details")
                if primary_details:
                    related_urls.append(primary_details)
                    seen_urls.add(primary_details)
                for url_value in aliases_by_activity_key.get(activity_key, []):
                    if not url_value or url_value in seen_urls:
                        continue
                    related_urls.append(url_value)
                    seen_urls.add(url_value)
                entries.append(
                    {
                        "user_id": value.get("user_id"),
                        "username": value.get("username"),
                        "stream_key": value.get("stream_key"),
                        "endpoint": value.get("endpoint"),
                        "details": value.get("details"),
                        "ip_address": value.get("ip_address"),
                        "user_agent": value.get("user_agent"),
                        "connection_id": value.get("connection_id"),
                        "started_at": started_at,
                        "last_seen": value.get("last_seen"),
                        "active_seconds": max(int(now - started_at), 0),
                        "age_seconds": max(int(now - (value.get("last_seen") or now)), 0),
                        "related_urls": related_urls,
                    }
                )
        entries.sort(
            key=lambda item: (
                item.get("started_at") or 0,
                str(item.get("connection_id") or ""),
                str(item.get("details") or ""),
            )
        )
        return entries

    async def cleanup(self):
        now = time.time()
        expired = []
        async with self.activity_lock:
            for key, entry in list(self.activity.items()):
                if now - entry["last_seen"] > self.activity_ttl:
                    expired.append((key, entry))
            for key, _ in expired:
                self.activity.pop(key, None)
            for key, value in list(self.aliases.items()):
                if now - (value.get("last_seen") or 0) > self.alias_ttl:
                    self.aliases.pop(key, None)
            for key, value in list(self.connection_index.items()):
                activity_key = value.get("activity_key")
                if activity_key not in self.activity or now - (value.get("last_seen") or 0) > self.alias_ttl:
                    self.connection_index.pop(key, None)
            for key, value in list(self.connection_history.items()):
                if now - (value.get("last_seen") or 0) > self.connection_history_ttl:
                    self.connection_history.pop(key, None)
            for _, entry in expired:
                user_scope = self._actor_scope(entry.get("user_id"), entry.get("username"))
                connection_id = entry.get("connection_id")
                if connection_id:
                    history_key = f"{user_scope}:{connection_id}"
                    self._store_connection_history_unlocked(history_key, entry, now, disconnected=True)
        for _, entry in expired:
            audit_user = _AuditUser(entry["user_id"], entry["username"], entry.get("stream_key"))
            await audit_stream_event(
                audit_user,
                "stream_stop",
                entry.get("endpoint") or "",
                details=entry.get("details"),
                ip_address=entry.get("ip_address"),
                user_agent=entry.get("user_agent"),
            )
        async with self.playlist_parent_lock:
            for key, value in list(self.playlist_parents.items()):
                if now - (value.get("last_seen") or 0) > self.parent_ttl:
                    self.playlist_parents.pop(key, None)

    async def save_state(self, file_path: str):
        if not file_path:
            return False
        now = time.time()
        async with self.activity_lock:
            activity = {}
            for key, value in self.activity.items():
                last_seen = float(value.get("last_seen") or 0)
                if now - last_seen > self.activity_ttl:
                    continue
                activity[key] = dict(value)
            aliases = {}
            for key, value in self.aliases.items():
                last_seen = float(value.get("last_seen") or 0)
                if now - last_seen > self.alias_ttl:
                    continue
                aliases[key] = dict(value)
            connection_index = {}
            for key, value in self.connection_index.items():
                activity_key = value.get("activity_key")
                last_seen = float(value.get("last_seen") or 0)
                if activity_key not in activity or now - last_seen > self.alias_ttl:
                    continue
                connection_index[key] = dict(value)
            connection_history = {}
            for key, value in self.connection_history.items():
                last_seen = float(value.get("last_seen") or 0)
                if now - last_seen > self.connection_history_ttl:
                    continue
                connection_history[key] = dict(value)
        async with self.playlist_parent_lock:
            playlist_parents = {}
            for key, value in self.playlist_parents.items():
                last_seen = float(value.get("last_seen") or 0)
                if now - last_seen > self.parent_ttl:
                    continue
                playlist_parents[key] = dict(value)

        payload = {
            "version": 1,
            "saved_at": now,
            "activity": activity,
            "aliases": aliases,
            "connection_index": connection_index,
            "connection_history": connection_history,
            "playlist_parents": playlist_parents,
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

        now = time.time()
        activity = {}
        for key, value in (payload.get("activity") or {}).items():
            if not isinstance(value, dict):
                continue
            last_seen = float(value.get("last_seen") or 0)
            if now - last_seen > self.activity_ttl:
                continue
            entry = dict(value)
            entry["started_at"] = float(entry.get("started_at") or entry.get("last_seen") or now)
            activity[key] = entry

        aliases = {}
        for key, value in (payload.get("aliases") or {}).items():
            if not isinstance(value, dict):
                continue
            activity_key = value.get("activity_key")
            last_seen = float(value.get("last_seen") or 0)
            if activity_key not in activity or now - last_seen > self.alias_ttl:
                continue
            aliases[key] = dict(value)

        connection_index = {}
        for key, value in (payload.get("connection_index") or {}).items():
            if not isinstance(value, dict):
                continue
            activity_key = value.get("activity_key")
            last_seen = float(value.get("last_seen") or 0)
            if activity_key not in activity or now - last_seen > self.alias_ttl:
                continue
            connection_index[key] = dict(value)

        connection_history = {}
        for key, value in (payload.get("connection_history") or {}).items():
            if not isinstance(value, dict):
                continue
            last_seen = float(value.get("last_seen") or 0)
            if now - last_seen > self.connection_history_ttl:
                continue
            connection_history[key] = dict(value)

        playlist_parents = {}
        for key, value in (payload.get("playlist_parents") or {}).items():
            if not isinstance(value, dict):
                continue
            last_seen = float(value.get("last_seen") or 0)
            if now - last_seen > self.parent_ttl:
                continue
            playlist_parents[key] = dict(value)

        async with self.activity_lock:
            self.activity = activity
            self.aliases = aliases
            self.connection_index = connection_index
            self.connection_history = connection_history
        async with self.playlist_parent_lock:
            self.playlist_parents = playlist_parents
        return True


_stream_activity_tracker = StreamActivityTracker()


async def _mark_stream_activity(decoded_url, event_type="stream_start"):
    connection_id = _get_connection_id(default_new=False)
    await _stream_activity_tracker.mark(decoded_url, event_type=event_type, connection_id=connection_id)


async def _touch_stream_activity(decoded_url):
    connection_id = _get_connection_id(default_new=False)
    await _stream_activity_tracker.touch(decoded_url, connection_id=connection_id)


async def _register_playlist_parent(child_url: str, parent_url: str):
    await _stream_activity_tracker.register_playlist_parent(child_url, parent_url)


async def _cleanup_stream_activity():
    await _stream_activity_tracker.cleanup()


async def get_stream_activity_snapshot():
    return await _stream_activity_tracker.snapshot()


def _stream_activity_state_path() -> str | None:
    app_config = current_app.config.get("APP_CONFIG") if current_app else None
    if not app_config:
        return None
    return os.path.join(app_config.config_path, "stream_activity_state.json")


async def persist_stream_activity_state():
    file_path = _stream_activity_state_path()
    if not file_path:
        return False
    return await _stream_activity_tracker.save_state(file_path)


async def load_stream_activity_state():
    file_path = _stream_activity_state_path()
    if not file_path:
        return False
    return await _stream_activity_tracker.load_state(file_path)


async def upsert_stream_activity(
    decoded_url: str,
    connection_id: str | None = None,
    endpoint_override: str | None = None,
    start_event_type: str = "stream_start",
):
    touched = await _stream_activity_tracker.touch(decoded_url, connection_id=connection_id)
    if touched:
        return "touched"
    await _stream_activity_tracker.mark(
        decoded_url,
        event_type=start_event_type,
        connection_id=connection_id,
        endpoint_override=endpoint_override,
    )
    return "started"


async def stop_stream_activity(
    decoded_url: str,
    connection_id: str | None = None,
    event_type: str = "stream_stop",
    endpoint_override: str | None = None,
):
    return await _stream_activity_tracker.stop(
        decoded_url,
        connection_id=connection_id,
        event_type=event_type,
        endpoint_override=endpoint_override,
    )
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
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "info",
            "-err_detect",
            "ignore_err",
            "-probesize",
            "20M",
            "-analyzeduration",
            "0",
            "-fpsprobesize",
            "0",
            "-i",
            self.decoded_url,
            "-c",
            "copy",
            "-f",
            "mpegts",
            "pipe:1",
        ]
        ffmpeg_logger.info("Executing FFmpeg with command: %s", command)
        self.process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        # Start a thread to log stderr
        stderr_thread = threading.Thread(target=self.log_stderr)
        stderr_thread.daemon = (
            True  # Make this a daemon thread so it exits when main thread exits
        )
        stderr_thread.start()

        chunk_size = 65536  # Read 64 KB at a time
        while self.running:
            try:
                # Use select to avoid blocking indefinitely
                import select

                ready, _, _ = select.select([self.process.stdout], [], [], 1.0)
                if not ready:
                    # No data available, check if we should terminate due to inactivity
                    if (
                        time.time() - self.last_activity > 300
                    ):  # 5 minutes of inactivity
                        ffmpeg_logger.info(
                            "No activity for 5 minutes, terminating FFmpeg stream"
                        )
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
                ffmpeg_logger.debug(
                    "FFmpeg: %s", line.decode("utf-8", errors="replace").strip()
                )
            except Exception as e:
                ffmpeg_logger.error("Error reading stderr: %s", e)
                break

    def add_buffer(self, buffer_id):
        """Add a new per-connection TimeBuffer with proper locking."""
        with self.lock:
            if buffer_id not in self.buffers:
                self.buffers[buffer_id] = TimeBuffer()
                self.connection_count += 1
                ffmpeg_logger.info(
                    f"Added buffer {buffer_id}, connection count: {self.connection_count}"
                )
            return self.buffers[buffer_id]

    def remove_buffer(self, buffer_id):
        """Remove a buffer with proper locking"""
        with self.lock:
            if buffer_id in self.buffers:
                del self.buffers[buffer_id]
                self.connection_count -= 1
                ffmpeg_logger.info(
                    f"Removed buffer {buffer_id}, connection count: {self.connection_count}"
                )
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
                buffer_logger.info(
                    "[Buffer] Removing chunk older than %d seconds", self.duration
                )
                self.buffer.popleft()  # Remove oldest chunk

    def read(self):
        with self.lock:
            if self.buffer:
                # Return the oldest chunk
                return self.buffer.popleft()[1]  # Return the chunk, not the timestamp
            return b""  # Return empty bytes if no data


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
        return Response(updated_playlist, content_type=content_type)
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
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    # Decode the Base64 encoded URL
    decoded_url = _b64_urlsafe_decode(encoded_url)
    await _touch_stream_activity(decoded_url)

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
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    # Decode the Base64 encoded URL
    decoded_url = _b64_urlsafe_decode(encoded_url)
    await _mark_stream_activity(decoded_url)

    # Generate a unique identifier (UUID) for the connection
    connection_id = str(uuid.uuid4())  # Use a UUID for the connection ID

    # Check if the stream is active and has connections
    if (
        decoded_url not in active_streams
        or not active_streams[decoded_url].running
        or active_streams[decoded_url].connection_count == 0
    ):
        buffer_logger.info(
            "Creating new FFmpeg stream with connection ID %s.", connection_id
        )
        # Create a new stream if it does not exist or if there are no connections
        stream = FFmpegStream(decoded_url)
        active_streams[decoded_url] = stream
    else:
        buffer_logger.info(
            "Connecting to existing FFmpeg stream with connection ID %s.", connection_id
        )

    # Get the existing stream
    stream = active_streams[decoded_url]
    stream.last_activity = time.time()  # Update last activity time

    # Add a new buffer for this connection
    stream.add_buffer(connection_id)
    if not is_tvh_backend_stream_user(getattr(request, "_stream_user", None)):
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
            # Stop logging is handled by inactivity cleanup to avoid per-connection spam.

    # Create a response object with the correct content type and set timeout to None
    response = Response(generate(), content_type="video/mp2t")
    response.timeout = None  # Disable timeout for streaming response
    return response
