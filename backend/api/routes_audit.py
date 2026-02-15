#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from quart import jsonify, request
from sqlalchemy import and_, or_, select

from backend.api import blueprint
from backend.api.routes_hls_proxy import stop_stream_activity, upsert_stream_activity
from backend.audit_view import serialize_audit_row
from backend.auth import admin_auth_required, streamer_or_admin_required, audit_stream_event
from backend.models import Session, StreamAuditLog, User


def _parse_iso_datetime(value: str | None) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except ValueError:
        return None


def _build_common_filters(params):
    filters = []
    event_type = (params.get("event_type") or "").strip()
    username = (params.get("username") or "").strip()
    search = (params.get("search") or "").strip()
    from_ts = _parse_iso_datetime(params.get("from_ts"))
    to_ts = _parse_iso_datetime(params.get("to_ts"))

    if event_type:
        filters.append(StreamAuditLog.event_type == event_type)
    if username:
        filters.append(User.username.ilike(f"%{username}%"))
    if from_ts:
        filters.append(StreamAuditLog.created_at >= from_ts)
    if to_ts:
        filters.append(StreamAuditLog.created_at <= to_ts)
    if search:
        like = f"%{search}%"
        filters.append(
            or_(
                StreamAuditLog.event_type.ilike(like),
                StreamAuditLog.endpoint.ilike(like),
                StreamAuditLog.details.ilike(like),
                StreamAuditLog.ip_address.ilike(like),
                User.username.ilike(like),
            )
        )
    return filters


async def _query_audit_rows(limit: int, filters, cursor_filter=None):
    stmt = (
        select(
            StreamAuditLog.id.label("id"),
            StreamAuditLog.created_at.label("created_at"),
            StreamAuditLog.event_type.label("event_type"),
            StreamAuditLog.endpoint.label("endpoint"),
            StreamAuditLog.details.label("details"),
            StreamAuditLog.ip_address.label("ip_address"),
            StreamAuditLog.user_agent.label("user_agent"),
            StreamAuditLog.user_id.label("user_id"),
            User.username.label("username"),
        )
        .select_from(StreamAuditLog)
        .outerjoin(User, User.id == StreamAuditLog.user_id)
    )
    for condition in filters:
        stmt = stmt.where(condition)
    if cursor_filter is not None:
        stmt = stmt.where(cursor_filter)
    stmt = stmt.order_by(StreamAuditLog.created_at.desc(), StreamAuditLog.id.desc()).limit(limit)

    async with Session() as session:
        result = await session.execute(stmt)
        rows = result.mappings().all()
    return rows


@blueprint.route('/tic-api/audit/logs', methods=['GET'])
@admin_auth_required
async def api_list_audit_logs():
    try:
        limit = int(request.args.get("limit", 50))
    except ValueError:
        limit = 50
    limit = max(1, min(limit, 200))

    before_created_at = _parse_iso_datetime(request.args.get("before_created_at"))
    before_id = request.args.get("before_id")
    try:
        before_id = int(before_id) if before_id is not None else None
    except ValueError:
        before_id = None

    filters = _build_common_filters(request.args)
    cursor_filter = None
    if before_created_at and before_id:
        cursor_filter = or_(
            StreamAuditLog.created_at < before_created_at,
            and_(
                StreamAuditLog.created_at == before_created_at,
                StreamAuditLog.id < before_id,
            ),
        )

    rows = await _query_audit_rows(limit=limit, filters=filters, cursor_filter=cursor_filter)
    data = [serialize_audit_row(dict(row)) for row in rows]
    return jsonify({"success": True, "data": data})


@blueprint.route('/tic-api/audit/logs/poll', methods=['GET'])
@admin_auth_required
async def api_poll_audit_logs():
    since_created_at = _parse_iso_datetime(request.args.get("since_created_at"))
    since_id = request.args.get("since_id")
    timeout = request.args.get("timeout", "25")
    try:
        since_id = int(since_id) if since_id is not None else None
    except ValueError:
        since_id = None
    try:
        timeout_value = int(timeout)
    except ValueError:
        timeout_value = 25
    timeout_value = max(1, min(timeout_value, 25))

    try:
        limit = int(request.args.get("limit", 100))
    except ValueError:
        limit = 100
    limit = max(1, min(limit, 200))

    filters = _build_common_filters(request.args)

    cursor_filter = None
    if since_created_at and since_id:
        cursor_filter = or_(
            StreamAuditLog.created_at > since_created_at,
            and_(
                StreamAuditLog.created_at == since_created_at,
                StreamAuditLog.id > since_id,
            ),
        )

    start = asyncio.get_running_loop().time()
    while True:
        rows = await _query_audit_rows(limit=limit, filters=filters, cursor_filter=cursor_filter)
        if rows:
            data = [serialize_audit_row(dict(row)) for row in rows]
            return jsonify({"success": True, "data": data})
        elapsed = asyncio.get_running_loop().time() - start
        if elapsed >= timeout_value:
            return jsonify({"success": True, "data": []})
        await asyncio.sleep(1)


@blueprint.route('/tic-api/audit/playback-start', methods=['POST'])
@streamer_or_admin_required
async def api_audit_playback_start():
    user = getattr(request, "_current_user", None)
    data = await request.get_json(force=True, silent=True) or {}
    url = (data.get("url") or "").strip()
    title = (data.get("title") or "").strip()
    if not url:
        return jsonify({"success": False, "message": "Missing playback url"}), 400
    details = url
    if title:
        details = f"{title}\n{url}"
    await audit_stream_event(
        user,
        "playback_start_direct",
        "/tic-web/player/direct",
        details=details,
    )
    return jsonify({"success": True})


@blueprint.route('/tic-api/audit/playback-heartbeat', methods=['POST'])
@streamer_or_admin_required
async def api_audit_playback_heartbeat():
    data = await request.get_json(force=True, silent=True) or {}
    url = (data.get("url") or "").strip()
    connection_id = (data.get("connection_id") or data.get("cid") or "").strip() or None
    if not url:
        return jsonify({"success": False, "message": "Missing playback url"}), 400
    state = await upsert_stream_activity(
        url,
        connection_id=connection_id,
        endpoint_override="/tic-web/player",
        start_event_type="stream_start",
    )
    return jsonify({"success": True, "state": state})


@blueprint.route('/tic-api/audit/playback-stop', methods=['POST'])
@streamer_or_admin_required
async def api_audit_playback_stop():
    data = await request.get_json(force=True, silent=True) or {}
    url = (data.get("url") or "").strip()
    connection_id = (data.get("connection_id") or data.get("cid") or "").strip() or None
    if not url:
        return jsonify({"success": False, "message": "Missing playback url"}), 400
    stopped = await stop_stream_activity(
        url,
        connection_id=connection_id,
        event_type="stream_stop",
        endpoint_override="/tic-web/player",
    )
    return jsonify({"success": True, "stopped": bool(stopped)})
