#!/usr/bin/env python3
# -*- coding:utf-8 -*-

from quart import request, jsonify, current_app
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from backend.api import blueprint
from backend.auth import (
    get_user_from_token,
    get_authenticated_session_expires_at,
    unauthorized_response,
    _get_bearer_token,
    get_request_client_ip,
    invalidate_auth_token_cache,
)
from backend.datetime_utils import to_utc_iso, utc_now_naive
from backend.models import Session, UserSession, User
from backend.security import (
    generate_session_token,
    hash_session_token,
    compute_session_expiry,
)
from backend.users import ensure_default_admin, verify_user_password_for_login


def _serialize_user(user: User):
    return {
        "id": user.id,
        "username": user.username,
        "roles": [role.name for role in user.roles] if user.roles else [],
        "is_active": user.is_active,
        "streaming_key": user.streaming_key,
        "streaming_key_created_at": to_utc_iso(user.streaming_key_created_at),
        "tvh_sync_status": user.tvh_sync_status,
        "tvh_sync_error": user.tvh_sync_error,
        "tvh_sync_updated_at": to_utc_iso(user.tvh_sync_updated_at),
    }


@blueprint.route('/tic-api/auth/login', methods=['POST'])
async def auth_login():
    config = current_app.config['APP_CONFIG']
    await ensure_default_admin(config)

    data = await request.get_json(force=True, silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not password:
        return jsonify({"success": False, "message": "Missing username or password"}), 400

    async with Session() as session:
        async with session.begin():
            result = await session.execute(
                select(User).where(User.username == username).options(selectinload(User.roles))
            )
            user = result.scalars().first()
            if not user or not user.is_active:
                return unauthorized_response("Invalid credentials")

            ok, needs_rehash = await verify_user_password_for_login(user, password)
            if not ok:
                return unauthorized_response("Invalid credentials")

            token = generate_session_token()
            token_hash = hash_session_token(token)
            expires_at = compute_session_expiry()
            session_obj = UserSession(
                user_id=user.id,
                token_hash=token_hash,
                created_at=utc_now_naive(),
                expires_at=expires_at,
                revoked=False,
                user_agent=request.headers.get("User-Agent"),
                ip_address=get_request_client_ip(),
            )
            user.last_login_at = utc_now_naive()
            if needs_rehash:
                from backend.security import hash_password
                user.password_hash = hash_password(password)

            session.add(session_obj)
            session.add(user)

    response = jsonify(
        {
            "success": True,
            "token": token,
            "session_expires_at": to_utc_iso(expires_at),
            "user": _serialize_user(user),
        }
    )
    # Cookie-based auth for iframe usage (e.g., /tic-tvh/).
    response.set_cookie(
        "tic_auth_token",
        token,
        httponly=True,
        samesite="Lax",
        path="/",
    )
    return response


@blueprint.route('/tic-api/auth/me', methods=['GET'])
async def auth_me():
    user = await get_user_from_token()
    if not user:
        return unauthorized_response()
    session_expires_at = get_authenticated_session_expires_at()
    return jsonify(
        {
            "success": True,
            "session_expires_at": to_utc_iso(session_expires_at),
            "user": _serialize_user(user),
        }
    )


@blueprint.route('/tic-api/auth/logout', methods=['POST'])
async def auth_logout():
    token_value = _get_bearer_token()
    if not token_value:
        return unauthorized_response()
    token_hash = hash_session_token(token_value)
    async with Session() as session:
        async with session.begin():
            result = await session.execute(
                select(UserSession).where(UserSession.token_hash == token_hash)
            )
            session_obj = result.scalars().first()
            if session_obj:
                session_obj.revoked = True
                session.add(session_obj)
    await invalidate_auth_token_cache(token_hash)
    response = jsonify({"success": True})
    response.delete_cookie("tic_auth_token", path="/")
    return response


@blueprint.route('/tic-api/auth/refresh', methods=['POST'])
async def auth_refresh():
    token_value = _get_bearer_token()
    if not token_value:
        return unauthorized_response()
    token_hash = hash_session_token(token_value)
    now = utc_now_naive()
    new_token = generate_session_token()
    new_token_hash = hash_session_token(new_token)
    new_expires_at = compute_session_expiry()
    async with Session() as session:
        async with session.begin():
            session_result = await session.execute(
                select(UserSession).where(
                    UserSession.token_hash == token_hash,
                    UserSession.revoked == False,
                    (UserSession.expires_at == None) | (UserSession.expires_at >= now),
                )
            )
            existing_session = session_result.scalars().first()
            if not existing_session:
                return unauthorized_response()

            user_result = await session.execute(
                select(User).where(User.id == existing_session.user_id).options(selectinload(User.roles))
            )
            user = user_result.scalars().first()
            if not user or not user.is_active:
                existing_session.revoked = True
                session.add(existing_session)
                return unauthorized_response()

            existing_session.revoked = True
            session.add(existing_session)
            session.add(
                UserSession(
                    user_id=user.id,
                    token_hash=new_token_hash,
                    created_at=now,
                    last_used_at=now,
                    expires_at=new_expires_at,
                    revoked=False,
                    user_agent=request.headers.get("User-Agent"),
                    ip_address=get_request_client_ip(),
                )
            )
    await invalidate_auth_token_cache(token_hash)
    response = jsonify(
        {
            "success": True,
            "token": new_token,
            "session_expires_at": to_utc_iso(new_expires_at),
            "user": _serialize_user(user),
        }
    )
    response.set_cookie(
        "tic_auth_token",
        new_token,
        httponly=True,
        samesite="Lax",
        path="/",
    )
    return response
