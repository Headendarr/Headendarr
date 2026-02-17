#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import base64
import asyncio
import time
from dataclasses import dataclass
from datetime import timedelta
from functools import wraps

from quart import request, jsonify, make_response, has_request_context, has_websocket_context, websocket, current_app
from sqlalchemy import delete, select, update, or_
from sqlalchemy.orm import selectinload

from backend import config
from backend.datetime_utils import utc_now_naive
from backend.models import Session, User, UserSession, StreamAuditLog
from backend.security import hash_session_token


class TvhStreamUser:
    def __init__(self, username, stream_key):
        self.id = None
        self.username = username
        self.streaming_key = stream_key
        self.is_active = True
        self.roles = []


def is_tvh_backend_stream_user(user) -> bool:
    if not user:
        return False
    if isinstance(user, TvhStreamUser):
        return True
    username = str(getattr(user, "username", "") or "")
    user_id = getattr(user, "id", None)
    return username.startswith("tic-tvh-") and not user_id


class _StreamKeyCache:
    def __init__(self, ttl_seconds=30):
        self.ttl_seconds = ttl_seconds
        self._cache = {}
        self._lock = asyncio.Lock()

    async def get(self, stream_key):
        async with self._lock:
            entry = self._cache.get(stream_key)
            if not entry:
                return None, False
            user, expires_at = entry
            if expires_at < time.time():
                self._cache.pop(stream_key, None)
                return None, False
            return user, True

    async def set(self, stream_key, user):
        async with self._lock:
            self._cache[stream_key] = (user, time.time() + self.ttl_seconds)


_stream_key_cache = _StreamKeyCache(ttl_seconds=30)


@dataclass
class _TokenAuthCacheEntry:
    user: User | None
    session_expires_at: object
    cache_expires_at_epoch: float


class _TokenAuthCache:
    def __init__(self, ttl_seconds=5):
        self.ttl_seconds = ttl_seconds
        self._cache = {}
        self._lock = asyncio.Lock()

    async def get(self, token_hash, now_utc):
        async with self._lock:
            entry = self._cache.get(token_hash)
            if not entry:
                return None, False
            if entry.cache_expires_at_epoch < time.time():
                self._cache.pop(token_hash, None)
                return None, False
            if entry.session_expires_at is not None and entry.session_expires_at < now_utc:
                self._cache.pop(token_hash, None)
                return None, False
            return entry, True

    async def set(self, token_hash, user, session_expires_at):
        async with self._lock:
            self._cache[token_hash] = _TokenAuthCacheEntry(
                user=user,
                session_expires_at=session_expires_at,
                cache_expires_at_epoch=time.time() + self.ttl_seconds,
            )

    async def invalidate(self, token_hash):
        async with self._lock:
            self._cache.pop(token_hash, None)


class _SessionLastUsedThrottle:
    def __init__(self, min_interval_seconds=60):
        self.min_interval_seconds = min_interval_seconds
        self._last_touches = {}
        self._lock = asyncio.Lock()

    async def should_touch(self, token_hash):
        now = time.time()
        async with self._lock:
            last = self._last_touches.get(token_hash)
            if last is not None and (now - last) < self.min_interval_seconds:
                return False
            self._last_touches[token_hash] = now
            return True

    async def clear(self, token_hash):
        async with self._lock:
            self._last_touches.pop(token_hash, None)


_token_auth_cache = _TokenAuthCache(ttl_seconds=5)
_session_last_used_throttle = _SessionLastUsedThrottle(min_interval_seconds=60)


def unauthorized_response(message="Unauthorized"):
    return jsonify({"success": False, "message": message}), 401


def forbidden_response(message="Forbidden"):
    return jsonify({"success": False, "message": message}), 403


def get_request_client_ip() -> str | None:
    if not has_request_context():
        return None
    try:
        # Respect reverse-proxy/client forwarding headers first.
        xff = (request.headers.get("X-Forwarded-For") or "").strip()
        if xff:
            # Format: client, proxy1, proxy2...
            candidate = xff.split(",")[0].strip()
            if candidate:
                return candidate
        forwarded = (request.headers.get("Forwarded") or "").strip()
        if forwarded:
            # RFC 7239 e.g. for=203.0.113.195;proto=https;by=...
            first = forwarded.split(",")[0]
            for part in first.split(";"):
                part = part.strip()
                if part.lower().startswith("for="):
                    candidate = part[4:].strip().strip('"')
                    if candidate.startswith("[") and "]" in candidate:
                        candidate = candidate[1:candidate.index("]")]
                    if ":" in candidate and candidate.count(":") == 1 and "." in candidate:
                        candidate = candidate.split(":", 1)[0]
                    if candidate:
                        return candidate
        for header in ("X-Real-IP", "CF-Connecting-IP", "True-Client-IP"):
            value = (request.headers.get(header) or "").strip()
            if value:
                return value
    except Exception:
        pass
    return getattr(request, "remote_addr", None)


def _get_bearer_token():
    auth = ""
    cookie_token = None
    if has_request_context():
        auth = request.headers.get("Authorization", "")
        cookie_token = request.cookies.get("tic_auth_token")
    elif has_websocket_context():
        auth = websocket.headers.get("Authorization", "")
        cookie_token = websocket.cookies.get("tic_auth_token")
    if auth.startswith("Bearer "):
        return auth[len("Bearer "):].strip()
    if cookie_token:
        return cookie_token
    return None


def get_authenticated_session_expires_at():
    if not has_request_context():
        return None
    return getattr(request, "_current_user_session_expires_at", None)


def _get_basic_auth_credentials():
    auth = ""
    if has_request_context():
        auth = request.headers.get("Authorization", "")
    elif has_websocket_context():
        auth = websocket.headers.get("Authorization", "")
    if auth.startswith("Basic "):
        try:
            username, password = base64.b64decode(auth[len("Basic "):].strip()).decode().split(':', 1)
            return username, password
        except Exception:
            return None, None
    return None, None


async def get_user_from_token():
    token = _get_bearer_token()
    if not token:
        return None
    token_hash = hash_session_token(token)
    now = utc_now_naive()

    # Reuse user in-request when available to avoid duplicate DB lookups.
    if has_request_context():
        cached_hash = getattr(request, "_current_user_token_hash", None)
        if cached_hash == token_hash and hasattr(request, "_current_user"):
            if not hasattr(request, "_current_user_session_expires_at"):
                request._current_user_session_expires_at = None
            return request._current_user

    cached_entry, has_cache = await _token_auth_cache.get(token_hash, now)
    if has_cache:
        cached_user = cached_entry.user
        session_expires_at = cached_entry.session_expires_at
        if has_request_context():
            request._current_user_token_hash = token_hash
            request._current_user = cached_user
            request._current_user_session_expires_at = session_expires_at
        if cached_user and await _session_last_used_throttle.should_touch(token_hash):
            async with Session() as session:
                async with session.begin():
                    await session.execute(
                        update(UserSession)
                        .where(UserSession.token_hash == token_hash)
                        .values(last_used_at=now)
                    )
        return cached_user

    async with Session() as session:
        result = await session.execute(
            select(User, UserSession.expires_at)
            .join(UserSession)
            .where(
                UserSession.token_hash == token_hash,
                UserSession.revoked == False,
                or_(UserSession.expires_at == None, UserSession.expires_at >= now),
            )
            .options(selectinload(User.roles))
        )
        row = result.first()
        user = row[0] if row else None
        session_expires_at = row[1] if row else None
        if not user or not user.is_active:
            await _token_auth_cache.set(token_hash, None, session_expires_at)
            if has_request_context():
                request._current_user_token_hash = token_hash
                request._current_user = None
                request._current_user_session_expires_at = session_expires_at
            return None
        if await _session_last_used_throttle.should_touch(token_hash):
            await session.execute(
                update(UserSession)
                .where(UserSession.token_hash == token_hash)
                .values(last_used_at=now)
            )
            await session.commit()
        await _token_auth_cache.set(token_hash, user, session_expires_at)
        if has_request_context():
            request._current_user_token_hash = token_hash
            request._current_user = user
            request._current_user_session_expires_at = session_expires_at
        return user


async def invalidate_auth_token_cache(token_hash: str):
    await _token_auth_cache.invalidate(token_hash)
    await _session_last_used_throttle.clear(token_hash)


def user_has_role(user: User, role_name: str) -> bool:
    return any(role.name == role_name for role in user.roles or [])


async def check_auth():
    user = await get_user_from_token()
    return user is not None


def admin_auth_required(func):
    @wraps(func)
    async def decorated_function(*args, **kwargs):
        user = await get_user_from_token()
        if not user:
            return unauthorized_response()
        if not user_has_role(user, "admin"):
            return forbidden_response()
        return await func(*args, **kwargs)

    return decorated_function


def user_auth_required(func):
    @wraps(func)
    async def decorated_function(*args, **kwargs):
        user = await get_user_from_token()
        if not user:
            return unauthorized_response()
        return await func(*args, **kwargs)

    return decorated_function


def streamer_or_admin_required(func):
    @wraps(func)
    async def decorated_function(*args, **kwargs):
        user = await get_user_from_token()
        if not user:
            return unauthorized_response()
        if not (user_has_role(user, "admin") or user_has_role(user, "streamer")):
            return forbidden_response()
        request._current_user = user
        return await func(*args, **kwargs)

    return decorated_function


def _extract_stream_key():
    if request.view_args and request.view_args.get("stream_key"):
        return request.view_args.get("stream_key")
    return request.args.get("stream_key") or request.args.get("password")


async def get_user_from_stream_key():
    user_from_token = await get_user_from_token()
    if user_from_token:
        return user_from_token
    # Extract the required stream key
    #   This will revert to using the password
    stream_key = _extract_stream_key()
    if not stream_key:
        basic_username, basic_password = _get_basic_auth_credentials()
        if basic_password:
            stream_key = basic_password
    if not stream_key:
        return None

    # First attempt to see if the user is the TVH user
    try:
        config = current_app.config.get("APP_CONFIG") if has_request_context() else None
    except Exception:
        config = None
    if config:
        try:
            tvh_stream_user = await config.get_tvh_stream_user()
            tvh_username = tvh_stream_user.get("username")
            tvh_stream_key = tvh_stream_user.get("stream_key")
            if tvh_stream_key and tvh_stream_key == stream_key:
                # Mock a real user with a TVH stream user class
                return TvhStreamUser(tvh_username, tvh_stream_key)
        except Exception:
            pass

    # Finally do a lookup for a user stream key (cached for a short TTL)
    cached_user, has_cache = await _stream_key_cache.get(stream_key)
    if has_cache:
        if cached_user is None:
            return None
        return cached_user

    from backend.users import get_user_by_stream_key
    user = await get_user_by_stream_key(stream_key)
    await _stream_key_cache.set(stream_key, user)
    return user


def stream_key_required(func):
    @wraps(func)
    async def decorated_function(*args, **kwargs):
        user = await get_user_from_stream_key()
        if not user or not user.is_active:
            return unauthorized_response()
        request._stream_user = user
        stream_key = _extract_stream_key()
        if not stream_key:
            _, basic_password = _get_basic_auth_credentials()
            stream_key = basic_password
        request._stream_key = stream_key
        ip_address = get_request_client_ip()
        user_agent = request.headers.get("User-Agent")
        should_audit = not getattr(func, "_skip_stream_connect_audit", False) and not is_tvh_backend_stream_user(user)
        if should_audit:
            await audit_stream_event(
                user,
                "stream_connect",
                request.path,
                ip_address=ip_address,
                user_agent=user_agent,
            )
        response = await func(*args, **kwargs)
        if should_audit:
            response = await make_response(response)
            try:
                response.call_on_close(
                    lambda: asyncio.create_task(
                        audit_stream_event(
                            user,
                            "stream_disconnect",
                            request.path,
                            ip_address=ip_address,
                            user_agent=user_agent,
                        )
                    )
                )
            except Exception:
                await audit_stream_event(
                    user,
                    "stream_disconnect",
                    request.path,
                    ip_address=ip_address,
                    user_agent=user_agent,
                )
        return response

    return decorated_function


def skip_stream_connect_audit(func):
    func._skip_stream_connect_audit = True
    return func


async def audit_stream_event(
    user: User,
    event_type: str,
    endpoint: str,
    details: str = None,
    ip_address: str = None,
    user_agent: str = None,
):
    if is_tvh_backend_stream_user(user):
        return
    async with Session() as session:
        async with session.begin():
            if has_request_context():
                ip_value = ip_address or get_request_client_ip()
                user_agent_value = user_agent
                if user_agent_value is None:
                    try:
                        user_agent_value = request.headers.get("User-Agent")
                    except Exception:
                        user_agent_value = None
            else:
                ip_value = ip_address
                user_agent_value = user_agent
            log = StreamAuditLog(
                user_id=user.id if user else None,
                event_type=event_type,
                endpoint=endpoint,
                ip_address=ip_value,
                user_agent=user_agent_value,
                details=details,
                created_at=utc_now_naive(),
            )
            session.add(log)


async def cleanup_stream_audit_logs(retention_days: int | None = None) -> int:
    settings = config.read_settings()
    configured_days = settings.get("settings", {}).get("audit_log_retention_days", 7)
    try:
        days = int(retention_days if retention_days is not None else configured_days)
    except (TypeError, ValueError):
        days = 7
    if days < 1:
        days = 1
    cutoff = utc_now_naive() - timedelta(days=days)
    async with Session() as session:
        result = await session.execute(
            delete(StreamAuditLog).where(StreamAuditLog.created_at < cutoff)
        )
        await session.commit()
        return int(result.rowcount or 0)
