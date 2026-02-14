#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from quart import request, jsonify, current_app
import sqlalchemy.exc
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from backend.api import blueprint
from backend.api.tasks import TaskQueueBroker, sync_user_to_tvh
from backend.auth import admin_auth_required, user_auth_required, get_user_from_token
from backend.datetime_utils import to_utc_iso
from backend.models import Session, User
from backend.users import (
    create_user,
    update_user_roles,
    set_user_active,
    reset_user_password,
    rotate_stream_key,
    change_user_password,
    get_user_by_id,
    set_user_tvh_sync_status,
)


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


async def _queue_user_sync(user_id: int, username: str):
    await set_user_tvh_sync_status(user_id, "queued", None)
    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task(
        {
            "name":     f"Sync TVH user {username}",
            "function": sync_user_to_tvh,
            "args":     [current_app.config["APP_CONFIG"], user_id],
        },
        priority=25,
    )


@blueprint.route('/tic-api/users', methods=['GET'])
@admin_auth_required
async def list_users():
    async with Session() as session:
        result = await session.execute(select(User).options(selectinload(User.roles)))
        users = result.scalars().all()
    return jsonify({"success": True, "data": [_serialize_user(u) for u in users]})


@blueprint.route('/tic-api/users', methods=['POST'])
@admin_auth_required
async def create_user_route():
    data = await request.get_json(force=True, silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    roles = data.get("roles") or []
    if not username or not password:
        return jsonify({"success": False, "message": "Username and password are required"}), 400
    try:
        user, stream_key = await create_user(username, password, roles)
    except sqlalchemy.exc.IntegrityError:
        return jsonify({"success": False, "message": "Username already exists"}), 409
    await _queue_user_sync(user.id, user.username)
    return jsonify({"success": True, "user": _serialize_user(user), "streaming_key": stream_key})


@blueprint.route('/tic-api/users/<int:user_id>', methods=['PUT'])
@admin_auth_required
async def update_user_route(user_id):
    data = await request.get_json(force=True, silent=True) or {}
    roles = data.get("roles") or []
    is_active = data.get("is_active")
    if roles is not None:
        await update_user_roles(user_id, roles)
    if is_active is not None:
        await set_user_active(user_id, bool(is_active))
    user = await get_user_by_id(user_id)
    if not user:
        return jsonify({"success": False, "message": "User not found"}), 404
    await _queue_user_sync(user.id, user.username)
    return jsonify({"success": True, "user": _serialize_user(user)})


@blueprint.route('/tic-api/users/<int:user_id>/reset-password', methods=['POST'])
@admin_auth_required
async def admin_reset_password(user_id):
    data = await request.get_json(force=True, silent=True) or {}
    new_password = data.get("password") or ""
    if not new_password:
        return jsonify({"success": False, "message": "Password is required"}), 400
    user = await reset_user_password(user_id, new_password)
    if not user:
        return jsonify({"success": False, "message": "User not found"}), 404
    return jsonify({"success": True})


@blueprint.route('/tic-api/users/<int:user_id>/rotate-stream-key', methods=['POST'])
@admin_auth_required
async def admin_rotate_stream_key(user_id):
    user, stream_key = await rotate_stream_key(user_id)
    if not user:
        return jsonify({"success": False, "message": "User not found"}), 404
    await _queue_user_sync(user.id, user.username)
    return jsonify({"success": True, "streaming_key": stream_key})


@blueprint.route('/tic-api/users/self', methods=['GET'])
@user_auth_required
async def user_self():
    user = await get_user_from_token()
    return jsonify({"success": True, "user": _serialize_user(user)})


@blueprint.route('/tic-api/users/self/change-password', methods=['POST'])
@user_auth_required
async def user_change_password():
    user = await get_user_from_token()
    data = await request.get_json(force=True, silent=True) or {}
    current_password = data.get("current_password") or ""
    new_password = data.get("new_password") or ""
    if not current_password or not new_password:
        return jsonify({"success": False, "message": "Current and new passwords are required"}), 400
    updated_user, error = await change_user_password(user.id, current_password, new_password)
    if error == "invalid_password":
        return jsonify({"success": False, "message": "Invalid current password"}), 400
    if not updated_user:
        return jsonify({"success": False, "message": "User not found"}), 404
    return jsonify({"success": True})


@blueprint.route('/tic-api/users/self/rotate-stream-key', methods=['POST'])
@user_auth_required
async def user_rotate_stream_key():
    user = await get_user_from_token()
    updated_user, stream_key = await rotate_stream_key(user.id)
    if not updated_user:
        return jsonify({"success": False, "message": "User not found"}), 404
    await _queue_user_sync(updated_user.id, updated_user.username)
    return jsonify({"success": True, "streaming_key": stream_key})
