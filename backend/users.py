#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from datetime import datetime

from sqlalchemy import select, func
from sqlalchemy.orm import selectinload

from backend.models import Session, User, Role
from backend.security import hash_password, verify_password, needs_rehash, generate_stream_key
from backend.dvr_profiles import normalize_retention_policy


DEFAULT_ROLES = ("admin", "streamer")
ALLOWED_DVR_ACCESS_MODES = ("none", "read_write_own", "read_all_write_own")


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
            admin_password = settings.get('settings', {}).get('admin_password', 'admin')
            stream_key = generate_stream_key()
            admin_user = User(
                username='admin',
                password_hash=hash_password(admin_password),
                is_active=True,
                streaming_key=stream_key,
                streaming_key_created_at=datetime.utcnow(),
            )
            admin_user.roles.append(roles["admin"])
            session.add(admin_user)


async def get_user_by_username(username: str):
    async with Session() as session:
        result = await session.execute(
            select(User).where(User.username == username).options(selectinload(User.roles))
        )
        return result.scalars().first()


async def get_user_by_id(user_id: int):
    async with Session() as session:
        result = await session.execute(
            select(User).where(User.id == user_id).options(selectinload(User.roles))
        )
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
                streaming_key_created_at=datetime.utcnow(),
                dvr_access_mode="none",
                dvr_retention_policy="forever",
            )
            for role_name in role_names:
                role = roles.get(role_name)
                if role:
                    user.roles.append(role)
            if user_has_admin_role(user):
                # Admin users always have full DVR visibility semantics.
                user.dvr_access_mode = "read_all_write_own"
            session.add(user)
        return user, stream_key


async def update_user_roles(user_id: int, role_names):
    async with Session() as session:
        async with session.begin():
            roles = await ensure_roles(session)
            result = await session.execute(
                select(User).where(User.id == user_id).options(selectinload(User.roles))
            )
            user = result.scalars().first()
            if not user:
                return None
            user.roles.clear()
            for role_name in role_names:
                role = roles.get(role_name)
                if role:
                    user.roles.append(role)
            if user_has_admin_role(user):
                user.dvr_access_mode = "read_all_write_own"
            session.add(user)
            return user


def normalize_dvr_access_mode(mode: str | None) -> str:
    normalized = str(mode or "").strip().lower()
    return normalized if normalized in ALLOWED_DVR_ACCESS_MODES else "none"


async def update_user_dvr_settings(
    user_id: int,
    dvr_access_mode: str | None = None,
    dvr_retention_policy: str | None = None,
):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(
                select(User).where(User.id == user_id).options(selectinload(User.roles))
            )
            user = result.scalars().first()
            if not user:
                return None
            if user_has_admin_role(user):
                # Keep admin DVR access fixed and non-configurable.
                user.dvr_access_mode = "read_all_write_own"
                session.add(user)
                return user
            if dvr_access_mode is not None:
                user.dvr_access_mode = normalize_dvr_access_mode(dvr_access_mode)
            if dvr_retention_policy is not None:
                user.dvr_retention_policy = normalize_retention_policy(dvr_retention_policy)
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
            user.streaming_key_created_at = datetime.utcnow()
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
            user.tvh_sync_updated_at = datetime.utcnow()
            session.add(user)
            return user


async def verify_user_password_for_login(user: User, password: str):
    if not user or not user.password_hash:
        return False, False
    ok = verify_password(user.password_hash, password)
    if not ok:
        return False, False
    if needs_rehash(user.password_hash):
        return True, True
    return True, False
