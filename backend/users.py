#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import secrets
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import selectinload

from backend.datetime_utils import utc_now_naive
from backend.models import Session, User, Role
from backend.oidc import OidcConfig, extract_claim_value, map_roles_from_claims, resolve_username_from_claims
from backend.security import hash_password, verify_password, needs_rehash, generate_stream_key
from backend.dvr_profiles import normalize_retention_policy
from backend.utils import clean_key


DEFAULT_ROLES = ("admin", "streamer")
ALLOWED_DVR_ACCESS_MODES = ("none", "read_write_own", "read_all_write_own")
ALLOWED_VOD_ACCESS_MODES = ("none", "movies", "series", "movies_series")


def user_has_admin_role(user: User | None) -> bool:
    if not user or not user.roles:
        return False
    return any((role.name == "admin") for role in user.roles)


async def ensure_roles(session):
    existing = await session.execute(select(Role))
    roles = {role.name: role for role in existing.scalars().all()}
    for role_name in DEFAULT_ROLES:
        if role_name not in roles:
            role = Role(name=role_name)
            session.add(role)
            roles[role_name] = role
    return roles


async def ensure_default_admin(config):
    async with Session() as session:
        async with session.begin():
            roles = await ensure_roles(session)
            user_count = await session.scalar(select(func.count(User.id)))
            if user_count and user_count > 0:
                return
            settings = config.read_settings()
            admin_password = settings.get("settings", {}).get("admin_password", "admin")
            stream_key = generate_stream_key()
            admin_user = User(
                username="admin",
                password_hash=hash_password(admin_password),
                is_active=True,
                streaming_key=stream_key,
                streaming_key_created_at=utc_now_naive(),
            )
            admin_user.roles.append(roles["admin"])
            session.add(admin_user)


async def get_user_by_username(username: str):
    async with Session() as session:
        result = await session.execute(select(User).where(User.username == username).options(selectinload(User.roles)))
        return result.scalars().first()


async def get_user_by_id(user_id: int):
    async with Session() as session:
        result = await session.execute(select(User).where(User.id == user_id).options(selectinload(User.roles)))
        return result.scalars().first()


async def get_user_by_stream_key(stream_key: str):
    async with Session() as session:
        result = await session.execute(
            select(User).where(User.streaming_key == stream_key).options(selectinload(User.roles))
        )
        return result.scalars().first()


async def create_user(username: str, password: str, role_names=None):
    role_names = role_names or ["streamer"]
    async with Session() as session:
        async with session.begin():
            roles = await ensure_roles(session)
            stream_key = generate_stream_key()
            user = User(
                username=username,
                password_hash=hash_password(password),
                is_active=True,
                streaming_key=stream_key,
                streaming_key_created_at=utc_now_naive(),
                dvr_access_mode="none",
                dvr_retention_policy="forever",
                vod_access_mode="none",
                vod_generate_strm_files=False,
            )
            for role_name in role_names:
                if not isinstance(role_name, str):
                    continue
                role = roles.get(role_name)
                if role:
                    user.roles.append(role)
            if user_has_admin_role(user):
                # Admin users always have full DVR visibility semantics.
                user.dvr_access_mode = "read_all_write_own"
                user.vod_access_mode = "movies_series"
            session.add(user)
        return user, stream_key


async def update_user_roles(user_id: int, role_names):
    async with Session() as session:
        async with session.begin():
            roles = await ensure_roles(session)
            result = await session.execute(select(User).where(User.id == user_id).options(selectinload(User.roles)))
            user = result.scalars().first()
            if not user:
                return None
            user.roles.clear()
            for role_name in role_names:
                if not isinstance(role_name, str):
                    continue
                role = roles.get(role_name)
                if role:
                    user.roles.append(role)
            if user_has_admin_role(user):
                user.dvr_access_mode = "read_all_write_own"
                user.vod_access_mode = "movies_series"
            session.add(user)
            return user


def clean_dvr_access_mode(mode: str | None) -> str:
    cleaned = clean_key(mode)
    return cleaned if cleaned in ALLOWED_DVR_ACCESS_MODES else "none"


def clean_vod_access_mode(mode: str | None) -> str:
    cleaned = clean_key(mode)
    return cleaned if cleaned in ALLOWED_VOD_ACCESS_MODES else "none"


async def update_user_dvr_settings(
    user_id: int,
    dvr_access_mode: str | None = None,
    dvr_retention_policy: str | None = None,
):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(User).where(User.id == user_id).options(selectinload(User.roles)))
            user = result.scalars().first()
            if not user:
                return None
            if user_has_admin_role(user):
                # Keep admin DVR access fixed and non-configurable.
                user.dvr_access_mode = "read_all_write_own"
                session.add(user)
                return user
            if dvr_access_mode is not None:
                user.dvr_access_mode = clean_dvr_access_mode(dvr_access_mode)
            if dvr_retention_policy is not None:
                user.dvr_retention_policy = normalize_retention_policy(dvr_retention_policy)
            session.add(user)
            return user


async def update_user_vod_settings(
    user_id: int, vod_access_mode: str | None = None, vod_generate_strm_files: bool | None = None
):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(User).where(User.id == user_id).options(selectinload(User.roles)))
            user = result.scalars().first()
            if not user:
                return None
            if user_has_admin_role(user):
                # Keep admin VOD access fixed while allowing .strm export participation to be configured.
                user.vod_access_mode = "movies_series"
                if vod_generate_strm_files is not None:
                    user.vod_generate_strm_files = bool(vod_generate_strm_files)
                session.add(user)
                return user
            if vod_access_mode is not None:
                user.vod_access_mode = clean_vod_access_mode(vod_access_mode)
            if clean_vod_access_mode(user.vod_access_mode) == "none":
                user.vod_generate_strm_files = False
                session.add(user)
                return user
            if vod_generate_strm_files is not None:
                user.vod_generate_strm_files = bool(vod_generate_strm_files)
            session.add(user)
            return user


async def set_user_active(user_id: int, is_active: bool):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(User).where(User.id == user_id))
            user = result.scalars().first()
            if not user:
                return None
            user.is_active = is_active
            session.add(user)
            return user


async def reset_user_password(user_id: int, new_password: str):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(User).where(User.id == user_id))
            user = result.scalars().first()
            if not user:
                return None
            user.password_hash = hash_password(new_password)
            session.add(user)
            return user


async def change_user_password(user_id: int, current_password: str, new_password: str):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(User).where(User.id == user_id))
            user = result.scalars().first()
            if not user:
                return None, "not_found"
            if not verify_password(user.password_hash, current_password):
                return None, "invalid_password"
            user.password_hash = hash_password(new_password)
            session.add(user)
            return user, None


async def rotate_stream_key(user_id: int):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(User).where(User.id == user_id))
            user = result.scalars().first()
            if not user:
                return None, None
            stream_key = generate_stream_key()
            user.streaming_key = stream_key
            user.streaming_key_created_at = utc_now_naive()
            session.add(user)
            return user, stream_key


async def set_user_tvh_sync_status(user_id: int, status: str, error: str = None):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(User).where(User.id == user_id))
            user = result.scalars().first()
            if not user:
                return None
            user.tvh_sync_status = status
            user.tvh_sync_error = error
            user.tvh_sync_updated_at = utc_now_naive()
            session.add(user)
            return user


async def set_user_stream_key_last_used(user_id: int):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(User).where(User.id == user_id))
            user = result.scalars().first()
            if not user:
                return None
            user.last_stream_key_used_at = utc_now_naive()
            session.add(user)
            return user


async def delete_user(user_id: int):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(User).where(User.id == user_id).options(selectinload(User.roles)))
            user = result.scalars().first()
            if not user:
                return None, "not_found"

            if user_has_admin_role(user):
                admin_users_result = await session.execute(select(User).options(selectinload(User.roles)))
                admin_count = sum(
                    1 for existing_user in admin_users_result.scalars().all() if user_has_admin_role(existing_user)
                )
                if admin_count <= 1:
                    return None, "last_admin"

            username = user.username
            await session.delete(user)
            return username, None


async def verify_user_password_for_login(user: User, password: str):
    if not user or not user.password_hash:
        return False, False
    ok = verify_password(user.password_hash, password)
    if not ok:
        return False, False
    if needs_rehash(user.password_hash):
        return True, True
    return True, False


def _clean_username(value: str) -> str:
    cleaned = "".join(ch for ch in str(value or "") if ch.isalnum() or ch in ("-", "_", ".", "@")).strip()
    return (cleaned or "oidc-user")[:64]


async def _build_unique_username(session, base_username: str) -> str:
    candidate = _clean_username(base_username)
    suffix = 0
    while True:
        probe = candidate if suffix == 0 else f"{candidate[:57]}-{suffix:02d}"
        existing = await session.execute(select(User.id).where(User.username == probe))
        if existing.scalar_one_or_none() is None:
            return probe
        suffix += 1


def _claim_email(claims: dict, config: OidcConfig) -> str | None:
    email_value = extract_claim_value(claims, config.email_claim) or claims.get("email")
    if email_value is None:
        return None
    email = str(email_value).strip()
    return email[:255] if email else None


async def provision_or_update_oidc_user(claims: dict, config: OidcConfig):
    oidc_subject = str(claims.get("sub") or "").strip()
    if not oidc_subject:
        return None, "missing_subject"

    oidc_issuer = config.issuer_url
    desired_email = _claim_email(claims, config)
    desired_username_raw = resolve_username_from_claims(claims, config)

    for attempt in range(2):
        try:
            async with Session() as session:
                async with session.begin():
                    roles = await ensure_roles(session)

                    result = await session.execute(
                        select(User)
                        .where(User.oidc_issuer == oidc_issuer, User.oidc_subject == oidc_subject)
                        .options(selectinload(User.roles))
                    )
                    user = result.scalars().first()
                    is_new_user = user is None
                    mapped_role_names = map_roles_from_claims(claims, config)

                    if user is None:
                        if not config.auto_provision:
                            return None, "provisioning_disabled"

                        # Initial OIDC provisioning must always assign a usable role set.
                        # When role syncing is disabled, fall back to streamer-only access.
                        role_names_for_new_user = list(mapped_role_names)
                        if not role_names_for_new_user and not config.sync_roles_on_login:
                            role_names_for_new_user = ["streamer"]
                        if not role_names_for_new_user:
                            return None, "no_mapped_role"

                        username = await _build_unique_username(session, desired_username_raw)
                        user = User(
                            username=username,
                            password_hash=hash_password(secrets.token_urlsafe(24)),
                            is_active=True,
                            auth_source="oidc",
                            oidc_issuer=oidc_issuer,
                            oidc_subject=oidc_subject,
                            oidc_email=desired_email,
                            streaming_key=generate_stream_key(),
                            streaming_key_created_at=utc_now_naive(),
                            dvr_access_mode="none",
                            dvr_retention_policy="forever",
                            vod_access_mode="none",
                        )
                        session.add(user)
                        await session.flush()
                        for role_name in role_names_for_new_user:
                            role_obj = roles.get(role_name)
                            if role_obj:
                                user.roles.append(role_obj)

                    if not user.is_active:
                        return None, "inactive"

                    if config.sync_roles_on_login and not is_new_user:
                        if not mapped_role_names:
                            return None, "no_mapped_role"
                        user.roles.clear()
                        for role_name in mapped_role_names:
                            role_obj = roles.get(role_name)
                            if role_obj:
                                user.roles.append(role_obj)
                    elif not config.sync_roles_on_login and not user.roles:
                        # Safety net for previously provisioned users with empty role sets.
                        streamer_role = roles.get("streamer")
                        if streamer_role:
                            user.roles.append(streamer_role)

                    if user_has_admin_role(user):
                        user.dvr_access_mode = "read_all_write_own"
                    elif not user.dvr_access_mode:
                        user.dvr_access_mode = "none"
                    if not user.vod_access_mode:
                        user.vod_access_mode = "none"

                    user.auth_source = "oidc"
                    user.oidc_issuer = oidc_issuer
                    user.oidc_subject = oidc_subject
                    user.oidc_email = desired_email
                    user.last_login_at = utc_now_naive()
                    session.add(user)

                    return user, None
        except IntegrityError:
            if attempt == 0:
                # Retry once to recover from concurrent first-login provisioning races.
                continue
            return None, "provisioning_conflict"
