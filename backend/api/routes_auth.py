#!/usr/bin/env python3
# -*- coding:utf-8 -*-

from urllib.parse import quote_plus

from quart import request, jsonify, current_app, redirect
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
from backend.auth_rate_limit import (
    check_oidc_callback_rate_limit,
    check_oidc_start_rate_limit,
    precheck_login_rate_limit,
    record_login_failure,
    record_login_success,
)
from backend.utils import to_utc_iso, utc_now_naive
from backend.models import Session, UserSession, User
from backend.oidc import (
    OidcConfigurationError,
    OidcError,
    OidcValidationError,
    build_authorize_url,
    exchange_code_for_tokens,
    get_provider_metadata,
    load_oidc_config,
    validate_and_build_claims,
)
from backend.security import (
    generate_session_token,
    hash_session_token,
    compute_session_expiry,
)
from backend.users import ensure_default_admin, verify_user_password_for_login, provision_or_update_oidc_user


def _serialize_user(user: User):
    role_names = [role.name for role in user.roles] if user.roles else []
    is_admin = "admin" in role_names
    dvr_access_mode = "read_all_write_own" if is_admin else (user.dvr_access_mode or "none")
    vod_access_mode = "movies_series" if is_admin else (user.vod_access_mode or "none")
    last_login_at = user.last_login_at
    last_stream_key_used_at = user.last_stream_key_used_at
    last_logged_in_at = max(
        [value for value in (last_login_at, last_stream_key_used_at) if value is not None],
        default=None,
    )
    return {
        "id": user.id,
        "username": user.username,
        "roles": role_names,
        "is_active": user.is_active,
        "streaming_key": user.streaming_key,
        "streaming_key_created_at": to_utc_iso(user.streaming_key_created_at),
        "tvh_sync_status": user.tvh_sync_status,
        "tvh_sync_error": user.tvh_sync_error,
        "tvh_sync_updated_at": to_utc_iso(user.tvh_sync_updated_at),
        "last_login_at": to_utc_iso(last_login_at),
        "last_stream_key_used_at": to_utc_iso(last_stream_key_used_at),
        "last_logged_in_at": to_utc_iso(last_logged_in_at),
        "dvr_access_mode": dvr_access_mode,
        "dvr_retention_policy": user.dvr_retention_policy or "forever",
        "vod_access_mode": vod_access_mode,
    }


def _build_oidc_login_redirect(error: str | None = None):
    target = "/tic-web/login"
    if error:
        target = f"{target}?oidc_error={quote_plus(str(error))}"
    else:
        target = f"{target}?oidc=success"
    return target


def _clear_oidc_flow_cookies(response):
    secure = bool(current_app.config.get("AUTH_COOKIE_SECURE", False))
    response.delete_cookie("tic_oidc_state", path="/", secure=secure)
    response.delete_cookie("tic_oidc_nonce", path="/", secure=secure)
    response.delete_cookie("tic_oidc_next", path="/", secure=secure)
    return response


def _rate_limited_response(message: str, retry_after: int):
    response = jsonify({"success": False, "message": message})
    if retry_after > 0:
        response.headers["Retry-After"] = str(retry_after)
    return response, 429


@blueprint.route("/tic-api/auth/options", methods=["GET"])
async def auth_options():
    oidc_config = load_oidc_config()
    oidc_enabled = bool(oidc_config.enabled and oidc_config.configured)
    return jsonify(
        {
            "success": True,
            "local_login_enabled": bool(oidc_config.local_login_enabled),
            "oidc": {
                "enabled": oidc_enabled,
                "configured": bool(oidc_config.configured),
                "button_label": oidc_config.button_label,
            },
        }
    )


@blueprint.route("/tic-api/auth/login", methods=["POST"])
async def auth_login():
    oidc_config = load_oidc_config()
    if not oidc_config.local_login_enabled:
        return jsonify({"success": False, "message": "Local login is disabled"}), 403
    config = current_app.config["APP_CONFIG"]
    if not oidc_config.enabled:
        await ensure_default_admin(config)

    data = await request.get_json(force=True, silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    client_ip = get_request_client_ip()
    limiter_result = await precheck_login_rate_limit(client_ip, username)
    if not limiter_result.allowed:
        return _rate_limited_response("Too many login attempts. Please try again later.", limiter_result.retry_after)
    if not username or not password:
        await record_login_failure(client_ip, username)
        return jsonify({"success": False, "message": "Missing username or password"}), 400

    async with Session() as session:
        async with session.begin():
            result = await session.execute(
                select(User).where(User.username == username).options(selectinload(User.roles))
            )
            user = result.scalars().first()
            if not user or not user.is_active:
                await record_login_failure(client_ip, username)
                return unauthorized_response("Invalid credentials")

            ok, needs_rehash = await verify_user_password_for_login(user, password)
            if not ok:
                await record_login_failure(client_ip, username)
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
            await record_login_success(client_ip, username)

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
        expires=expires_at,
        secure=bool(current_app.config.get("AUTH_COOKIE_SECURE", False)),
    )
    return response


@blueprint.route("/tic-api/auth/oidc/start", methods=["GET"])
async def auth_oidc_start():
    rate_limit_result = await check_oidc_start_rate_limit(get_request_client_ip())
    if not rate_limit_result.allowed:
        return _rate_limited_response(
            "Too many OIDC start attempts. Please try again later.", rate_limit_result.retry_after
        )

    oidc_config = load_oidc_config()
    if not oidc_config.enabled:
        return jsonify({"success": False, "message": "OIDC is disabled"}), 404
    if not oidc_config.configured:
        return jsonify({"success": False, "message": "OIDC is not configured"}), 503

    try:
        metadata = await get_provider_metadata(oidc_config)
    except OidcConfigurationError as exc:
        return jsonify({"success": False, "message": str(exc)}), 503
    except OidcError:
        current_app.logger.exception("Failed to load OIDC metadata")
        return jsonify({"success": False, "message": "OIDC provider unavailable"}), 503

    import secrets

    state = secrets.token_urlsafe(32)
    nonce = secrets.token_urlsafe(32)
    next_path = str(request.args.get("next") or "").strip()
    if not next_path.startswith("/tic-web/"):
        next_path = "/tic-web/login"

    try:
        authorize_url = build_authorize_url(oidc_config, metadata, state, nonce)
    except OidcError:
        current_app.logger.exception("Failed to build OIDC authorisation URL")
        return jsonify({"success": False, "message": "Failed to start OIDC login"}), 503

    response = redirect(authorize_url)
    secure_cookie = bool(current_app.config.get("AUTH_COOKIE_SECURE", False))
    response.set_cookie(
        "tic_oidc_state", state, httponly=True, samesite="Lax", path="/", max_age=600, secure=secure_cookie
    )
    response.set_cookie(
        "tic_oidc_nonce", nonce, httponly=True, samesite="Lax", path="/", max_age=600, secure=secure_cookie
    )
    response.set_cookie(
        "tic_oidc_next", next_path, httponly=True, samesite="Lax", path="/", max_age=600, secure=secure_cookie
    )
    return response


@blueprint.route("/tic-api/auth/oidc/callback", methods=["GET"])
async def auth_oidc_callback():
    rate_limit_result = await check_oidc_callback_rate_limit(get_request_client_ip())
    if not rate_limit_result.allowed:
        return _rate_limited_response(
            "Too many OIDC callback attempts. Please try again later.",
            rate_limit_result.retry_after,
        )

    oidc_config = load_oidc_config()
    if not oidc_config.enabled or not oidc_config.configured:
        return _clear_oidc_flow_cookies(redirect(_build_oidc_login_redirect("oidc_unavailable")))

    if request.args.get("error"):
        err = str(request.args.get("error_description") or request.args.get("error") or "oidc_error")
        return _clear_oidc_flow_cookies(redirect(_build_oidc_login_redirect(err)))

    state = str(request.args.get("state") or "").strip()
    code = str(request.args.get("code") or "").strip()
    cookie_state = str(request.cookies.get("tic_oidc_state") or "").strip()
    nonce = str(request.cookies.get("tic_oidc_nonce") or "").strip()
    next_path = str(request.cookies.get("tic_oidc_next") or "/tic-web/login").strip()
    if not next_path.startswith("/tic-web/"):
        next_path = "/tic-web/login"

    if not code or not state or state != cookie_state:
        return _clear_oidc_flow_cookies(redirect(_build_oidc_login_redirect("invalid_state")))
    if not nonce:
        return _clear_oidc_flow_cookies(redirect(_build_oidc_login_redirect("invalid_nonce")))

    try:
        metadata = await get_provider_metadata(oidc_config)
        tokens = await exchange_code_for_tokens(oidc_config, metadata, code)
        claims = await validate_and_build_claims(oidc_config, metadata, tokens, nonce)
    except (OidcError, OidcValidationError):
        current_app.logger.exception("OIDC callback validation failed")
        return _clear_oidc_flow_cookies(redirect(_build_oidc_login_redirect("token_validation_failed")))

    user, provisioning_error = await provision_or_update_oidc_user(claims, oidc_config)
    if not user:
        return _clear_oidc_flow_cookies(
            redirect(_build_oidc_login_redirect(provisioning_error or "provisioning_failed"))
        )

    token = generate_session_token()
    token_hash = hash_session_token(token)
    expires_at = compute_session_expiry()
    user_id = user.id

    async with Session() as session:
        async with session.begin():
            session.add(
                UserSession(
                    user_id=user_id,
                    token_hash=token_hash,
                    created_at=utc_now_naive(),
                    expires_at=expires_at,
                    revoked=False,
                    user_agent=request.headers.get("User-Agent"),
                    ip_address=get_request_client_ip(),
                )
            )

    response = redirect(f"{next_path}?oidc=success")
    response.set_cookie(
        "tic_auth_token",
        token,
        httponly=True,
        samesite="Lax",
        path="/",
        expires=expires_at,
        secure=bool(current_app.config.get("AUTH_COOKIE_SECURE", False)),
    )
    return _clear_oidc_flow_cookies(response)


@blueprint.route("/tic-api/auth/me", methods=["GET"])
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


@blueprint.route("/tic-api/auth/logout", methods=["POST"])
async def auth_logout():
    token_value = _get_bearer_token()
    if not token_value:
        return unauthorized_response()
    token_hash = hash_session_token(token_value)
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(UserSession).where(UserSession.token_hash == token_hash))
            session_obj = result.scalars().first()
            if session_obj:
                session_obj.revoked = True
                session.add(session_obj)
    await invalidate_auth_token_cache(token_hash)
    response = jsonify({"success": True})
    response.delete_cookie("tic_auth_token", path="/", secure=bool(current_app.config.get("AUTH_COOKIE_SECURE", False)))
    return response


@blueprint.route("/tic-api/auth/refresh", methods=["POST"])
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
        expires=new_expires_at,
        secure=bool(current_app.config.get("AUTH_COOKIE_SECURE", False)),
    )
    return response
