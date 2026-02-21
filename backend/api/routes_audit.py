#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

from quart import jsonify, request
from sqlalchemy import or_, select

from backend.api import blueprint
from backend.audit_view import build_activity_label, build_device_label, derive_audit_mode
from backend.auth import admin_auth_required, streamer_or_admin_required, audit_stream_event
from backend.datetime_utils import to_utc_iso
from backend.models import Channel, CsoEventLog, Session, StreamAuditLog, User
from backend.stream_activity import stop_stream_activity, upsert_stream_activity


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


_ENTRY_TYPE_STREAM_AUDIT = "stream_audit"
_ENTRY_TYPE_CSO_EVENT_LOG = "cso_event_log"
_VALID_ENTRY_TYPES = {_ENTRY_TYPE_STREAM_AUDIT, _ENTRY_TYPE_CSO_EVENT_LOG}


def _parse_entry_types(params):
    raw = (params.get("entry_types") or params.get("entry_type") or "").strip().lower()
    if not raw:
        return {_ENTRY_TYPE_STREAM_AUDIT, _ENTRY_TYPE_CSO_EVENT_LOG}
    parsed = {part.strip() for part in raw.split(",") if part.strip()}
    valid = parsed.intersection(_VALID_ENTRY_TYPES)
    if not valid:
        return {_ENTRY_TYPE_STREAM_AUDIT, _ENTRY_TYPE_CSO_EVENT_LOG}
    return valid


def _entry_sort_tuple(entry: dict):
    created_at = entry.get("_created_at_dt")
    if not isinstance(created_at, datetime):
        created_at = datetime.min
    type_rank = 0 if entry.get("entry_type") == _ENTRY_TYPE_STREAM_AUDIT else 1
    entry_id = int(entry.get("id") or 0)
    return (created_at, -type_rank, entry_id)


def _apply_unified_before_cursor(entries, before_created_at, before_id, before_entry_type):
    if not before_created_at or before_id is None:
        return entries
    before_type_rank = 0 if before_entry_type == _ENTRY_TYPE_STREAM_AUDIT else 1
    out = []
    for entry in entries:
        created_at = entry.get("_created_at_dt")
        if not isinstance(created_at, datetime):
            continue
        current_type_rank = 0 if entry.get("entry_type") == _ENTRY_TYPE_STREAM_AUDIT else 1
        current_id = int(entry.get("id") or 0)
        if created_at < before_created_at:
            out.append(entry)
            continue
        if created_at > before_created_at:
            continue
        if current_type_rank > before_type_rank:
            out.append(entry)
            continue
        if current_type_rank == before_type_rank and current_id < before_id:
            out.append(entry)
    return out


def _apply_unified_since_cursor(entries, since_created_at, since_id, since_entry_type):
    if not since_created_at or since_id is None:
        return entries
    since_type_rank = 0 if since_entry_type == _ENTRY_TYPE_STREAM_AUDIT else 1
    out = []
    for entry in entries:
        created_at = entry.get("_created_at_dt")
        if not isinstance(created_at, datetime):
            continue
        current_type_rank = 0 if entry.get("entry_type") == _ENTRY_TYPE_STREAM_AUDIT else 1
        current_id = int(entry.get("id") or 0)
        if created_at > since_created_at:
            out.append(entry)
            continue
        if created_at < since_created_at:
            continue
        if current_type_rank < since_type_rank:
            out.append(entry)
            continue
        if current_type_rank == since_type_rank and current_id > since_id:
            out.append(entry)
    return out


def _serialize_stream_audit_row(row):
    entry_id = row.get("id")
    created_at = row.get("created_at")
    event_type = row.get("event_type")
    endpoint = row.get("endpoint")
    details = row.get("details")
    user_agent = row.get("user_agent")
    return {
        "id": entry_id,
        "entry_id": entry_id,
        "entry_type": _ENTRY_TYPE_STREAM_AUDIT,
        "entry_key": f"{_ENTRY_TYPE_STREAM_AUDIT}:{entry_id}",
        "created_at": to_utc_iso(created_at),
        "_created_at_dt": created_at,
        "event_type": event_type,
        "endpoint": endpoint,
        "details": details,
        "ip_address": row.get("ip_address"),
        "user_agent": user_agent,
        "user_id": row.get("user_id"),
        "username": row.get("username"),
        "channel_id": None,
        "severity": "info",
        "audit_mode": derive_audit_mode(event_type, endpoint),
        "activity_label": build_activity_label(event_type, endpoint, details),
        "device_label": build_device_label(user_agent),
    }


def _serialize_channel_stream_row(row):
    entry_id = row.get("id")
    created_at = row.get("created_at")
    details_json = row.get("details_json")
    details_payload = {}
    if details_json:
        try:
            details_payload = json.loads(details_json)
        except Exception:
            details_payload = {"raw": str(details_json)}
    details_text = None
    if details_payload:
        reason = details_payload.get("reason") or details_payload.get("after_failure_reason")
        source_id = details_payload.get("source_id") or details_payload.get("failed_source_id")
        return_code = details_payload.get("return_code")
        ffmpeg_error = details_payload.get("ffmpeg_error")
        segments = []
        if reason:
            segments.append(f"reason={reason}")
        if source_id:
            segments.append(f"source={source_id}")
        if return_code is not None:
            segments.append(f"return_code={return_code}")
        if ffmpeg_error:
            segments.append(f"ffmpeg_error={ffmpeg_error}")
        details_text = " | ".join(segments) if segments else json.dumps(details_payload, sort_keys=True)
    endpoint = f"/tic-hls-proxy/channel/{row.get('channel_id')}" if row.get("channel_id") else "/tic-hls-proxy/channel"
    event_type = row.get("event_type")
    return {
        "id": entry_id,
        "entry_id": entry_id,
        "entry_type": _ENTRY_TYPE_CSO_EVENT_LOG,
        "entry_key": f"{_ENTRY_TYPE_CSO_EVENT_LOG}:{entry_id}",
        "created_at": to_utc_iso(created_at),
        "_created_at_dt": created_at,
        "event_type": event_type,
        "endpoint": endpoint,
        "details": details_text,
        "ip_address": None,
        "user_agent": None,
        "user_id": None,
        "username": None,
        "channel_id": row.get("channel_id"),
        "severity": row.get("severity") or "info",
        "audit_mode": None,
        "activity_label": build_activity_label(event_type, endpoint, details_text) or "Channel stream event",
        "device_label": "System",
    }


def _stream_filters_from_params(params):
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


def _channel_event_filters_from_params(params):
    filters = []
    event_type = (params.get("event_type") or "").strip()
    search = (params.get("search") or "").strip()
    from_ts = _parse_iso_datetime(params.get("from_ts"))
    to_ts = _parse_iso_datetime(params.get("to_ts"))
    if event_type:
        filters.append(CsoEventLog.event_type == event_type)
    if from_ts:
        filters.append(CsoEventLog.created_at >= from_ts)
    if to_ts:
        filters.append(CsoEventLog.created_at <= to_ts)
    if search:
        like = f"%{search}%"
        filters.append(
            or_(
                CsoEventLog.event_type.ilike(like),
                CsoEventLog.details_json.ilike(like),
                Channel.name.ilike(like),
            )
        )
    return filters


async def _query_unified_audit_rows(limit: int, params):
    included_types = _parse_entry_types(params)
    per_source_limit = max(100, min(500, limit * 4))
    rows = []

    async with Session() as session:
        if _ENTRY_TYPE_STREAM_AUDIT in included_types:
            stream_stmt = (
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
                .order_by(StreamAuditLog.created_at.desc(), StreamAuditLog.id.desc())
                .limit(per_source_limit)
            )
            for condition in _stream_filters_from_params(params):
                stream_stmt = stream_stmt.where(condition)
            stream_result = await session.execute(stream_stmt)
            rows.extend(_serialize_stream_audit_row(dict(item)) for item in stream_result.mappings().all())

        if _ENTRY_TYPE_CSO_EVENT_LOG in included_types:
            channel_stmt = (
                select(
                    CsoEventLog.id.label("id"),
                    CsoEventLog.created_at.label("created_at"),
                    CsoEventLog.channel_id.label("channel_id"),
                    CsoEventLog.event_type.label("event_type"),
                    CsoEventLog.severity.label("severity"),
                    CsoEventLog.details_json.label("details_json"),
                    Channel.name.label("channel_name"),
                )
                .select_from(CsoEventLog)
                .outerjoin(Channel, Channel.id == CsoEventLog.channel_id)
                .order_by(CsoEventLog.created_at.desc(), CsoEventLog.id.desc())
                .limit(per_source_limit)
            )
            for condition in _channel_event_filters_from_params(params):
                channel_stmt = channel_stmt.where(condition)
            channel_result = await session.execute(channel_stmt)
            rows.extend(_serialize_channel_stream_row(dict(item)) for item in channel_result.mappings().all())

    rows.sort(key=_entry_sort_tuple, reverse=True)

    before_created_at = _parse_iso_datetime(params.get("before_created_at"))
    before_id = params.get("before_id")
    before_entry_type = (params.get("before_entry_type") or "").strip().lower()
    try:
        before_id = int(before_id) if before_id is not None else None
    except ValueError:
        before_id = None
    if before_created_at and before_id is not None:
        rows = _apply_unified_before_cursor(rows, before_created_at, before_id, before_entry_type)

    since_created_at = _parse_iso_datetime(params.get("since_created_at"))
    since_id = params.get("since_id")
    since_entry_type = (params.get("since_entry_type") or "").strip().lower()
    try:
        since_id = int(since_id) if since_id is not None else None
    except ValueError:
        since_id = None
    if since_created_at and since_id is not None:
        rows = _apply_unified_since_cursor(rows, since_created_at, since_id, since_entry_type)

    out = []
    for row in rows[:limit]:
        row.pop("_created_at_dt", None)
        out.append(row)
    return out


@blueprint.route('/tic-api/audit/logs', methods=['GET'])
@admin_auth_required
async def api_list_audit_logs():
    try:
        limit = int(request.args.get("limit", 50))
    except ValueError:
        limit = 50
    limit = max(1, min(limit, 200))

    rows = await _query_unified_audit_rows(limit=limit, params=request.args)
    data = rows
    return jsonify({"success": True, "data": data})


@blueprint.route('/tic-api/audit/logs/poll', methods=['GET'])
@admin_auth_required
async def api_poll_audit_logs():
    timeout = request.args.get("timeout", "25")
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

    start = asyncio.get_running_loop().time()
    while True:
        rows = await _query_unified_audit_rows(limit=limit, params=request.args)
        if rows:
            return jsonify({"success": True, "data": rows})
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
