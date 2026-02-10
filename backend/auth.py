#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import base64
import asyncio
import time
from datetime import datetime, timedelta
from functools import wraps

from quart import request, jsonify, make_response, has_request_context, has_websocket_context, websocket, current_app
from sqlalchemy import delete, select, update, or_
from sqlalchemy.orm import selectinload

from backend import config
from backend.models import Session, User, UserSession, StreamAuditLog
from backend.security import hash_session_token


class TvhStreamUser:
    def __init__(self, username, stream_key):
        self.id = None
        self.username = username
        self.streaming_key = stream_key
        self.is_active = True
        self.roles = []


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


def unauthorized_response(message="Unauthorized"):
    return jsonify({"success": False, "message": message}), 401


def forbidden_response(message="Forbidden"):
    return jsonify({"success": False, "message": message}), 403


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
    async with Session() as session:
        now = datetime.utcnow()
        result = await session.execute(
            select(User)
            .join(UserSession)
            .where(
                UserSession.token_hash == token_hash,
                UserSession.revoked == False,
                or_(UserSession.expires_at == None, UserSession.expires_at >= now),
            )
            .options(selectinload(User.roles))
        )
        user = result.scalars().first()
        if not user or not user.is_active:
            return None
        await session.execute(
            update(UserSession)
            .where(UserSession.token_hash == token_hash)
            .values(last_used_at=now)
        )
        await session.commit()
        return user


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
        ip_address = request.remote_addr
        user_agent = request.headers.get("User-Agent")
        if not getattr(func, "_skip_stream_connect_audit", False):
            await audit_stream_event(
                user,
                "stream_connect",
                request.path,
                ip_address=ip_address,
                user_agent=user_agent,
            )
        response = await func(*args, **kwargs)
        if not getattr(func, "_skip_stream_connect_audit", False):
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
    async with Session() as session:
        async with session.begin():
            if has_request_context():
                ip_value = ip_address or getattr(request, "remote_addr", None)
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
                created_at=datetime.utcnow(),
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
    cutoff = datetime.utcnow() - timedelta(days=days)
    async with Session() as session:
        result = await session.execute(
            delete(StreamAuditLog).where(StreamAuditLog.created_at < cutoff)
        )
        await session.commit()
        return int(result.rowcount or 0)
