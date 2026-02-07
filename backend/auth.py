#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import base64
from datetime import datetime
from functools import wraps

from quart import request, jsonify
from sqlalchemy import select, update, or_
from sqlalchemy.orm import selectinload

from backend.models import Session, User, UserSession, StreamAuditLog
from backend.security import hash_session_token


def unauthorized_response(message="Unauthorized"):
    return jsonify({"success": False, "message": message}), 401


def forbidden_response(message="Forbidden"):
    return jsonify({"success": False, "message": message}), 403


def _get_bearer_token():
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[len("Bearer "):].strip()
    cookie_token = request.cookies.get("tic_auth_token")
    if cookie_token:
        return cookie_token
    return None


def _get_basic_auth_credentials():
    auth = request.headers.get("Authorization", "")
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


def _extract_stream_key():
    if request.view_args and request.view_args.get("stream_key"):
        return request.view_args.get("stream_key")
    return request.args.get("stream_key") or request.args.get("password")


async def get_user_from_stream_key():
    user_from_token = await get_user_from_token()
    if user_from_token:
        return user_from_token
    username = request.args.get("username")
    stream_key = _extract_stream_key()
    if not stream_key:
        basic_username, basic_password = _get_basic_auth_credentials()
        if basic_password:
            username = username or basic_username
            stream_key = basic_password
    if not stream_key:
        return None

    if username:
        async with Session() as session:
            result = await session.execute(
                select(User).where(User.username == username).options(selectinload(User.roles))
            )
            user = result.scalars().first()
            if not user or not user.streaming_key:
                return None
            if user.streaming_key != stream_key:
                return None
            return user

    from backend.users import get_user_by_stream_key
    return await get_user_by_stream_key(stream_key)


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
        return await func(*args, **kwargs)

    return decorated_function


async def audit_stream_event(user: User, event_type: str, endpoint: str, details: str = None):
    async with Session() as session:
        async with session.begin():
            log = StreamAuditLog(
                user_id=user.id if user else None,
                event_type=event_type,
                endpoint=endpoint,
                ip_address=request.remote_addr,
                user_agent=request.headers.get("User-Agent"),
                details=details,
                created_at=datetime.utcnow(),
            )
            session.add(log)
