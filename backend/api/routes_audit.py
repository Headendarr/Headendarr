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
from backend.auth import admin_auth_required, audit_stream_event, get_request_user, streamer_or_admin_required
from backend.utils import to_utc_iso
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
_VALID_SEVERITIES = {"debug", "info", "warning", "error"}


def _parse_entry_types(params):
    raw = (params.get("entry_types") or params.get("entry_type") or "").strip().lower()
    if not raw:
        return {_ENTRY_TYPE_STREAM_AUDIT, _ENTRY_TYPE_CSO_EVENT_LOG}
    parsed = {part.strip() for part in raw.split(",") if part.strip()}
    valid = parsed.intersection(_VALID_ENTRY_TYPES)
    if not valid:
        return {_ENTRY_TYPE_STREAM_AUDIT, _ENTRY_TYPE_CSO_EVENT_LOG}
    return valid


def _parse_severities(params):
    raw = (params.get("severity") or "").strip().lower()
    if not raw:
        return set()
    parsed = {part.strip() for part in raw.split(",") if part.strip()}
    return parsed.intersection(_VALID_SEVERITIES)


def _parse_event_types(params):
    raw = (params.get("event_type") or "").strip().lower()
    if not raw:
        return set()
    return {part.strip() for part in raw.split(",") if part.strip()}


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
        "severity": str(row.get("severity") or "info").strip().lower() or "info",
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
        metrics = details_payload.get("metrics") or {}
        avg_speed = metrics.get("avg_speed")
        avg_bitrate = metrics.get("avg_bitrate")
        segments = []
        if reason:
            segments.append(f"reason={reason}")
        if source_id:
            segments.append(f"source={source_id}")
        if avg_speed is not None:
            try:
                speed_value = float(avg_speed)
            except (TypeError, ValueError):
                speed_value = 0.0
            if speed_value > 0:
                segments.append(f"avg_speed={speed_value:.2f}x")
        if avg_bitrate is not None:
            try:
                bitrate_value = float(avg_bitrate)
            except (TypeError, ValueError):
                bitrate_value = 0.0
            if bitrate_value > 0:
                segments.append(f"avg_bitrate={bitrate_value / 1000000:.2f} Mbps")
        if return_code is not None:
            segments.append(f"return_code={return_code}")
        if ffmpeg_error:
            segments.append(f"ffmpeg_error={ffmpeg_error}")
        details_text = " | ".join(segments) if segments else json.dumps(details_payload, sort_keys=True)
    endpoint = f"/tic-api/cso/channel/{row.get('channel_id')}" if row.get("channel_id") else "/tic-api/cso/channel"
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
    event_types = _parse_event_types(params)
    severities = _parse_severities(params)
    username = (params.get("username") or "").strip()
    search = (params.get("search") or "").strip()
    from_ts = _parse_iso_datetime(params.get("from_ts"))
    to_ts = _parse_iso_datetime(params.get("to_ts"))
    channel_id_raw = (params.get("channel_id") or "").strip()
    channel_id = None
    try:
        channel_id = int(channel_id_raw) if channel_id_raw else None
    except ValueError:
        channel_id = None
    if event_types:
        filters.append(StreamAuditLog.event_type.in_(list(event_types)))
    if severities:
        severity_filters = [StreamAuditLog.severity.in_(list(severities))]
        if "info" in severities:
            severity_filters.append(StreamAuditLog.severity.is_(None))
        filters.append(or_(*severity_filters))
    if username:
        filters.append(User.username.ilike(f"%{username}%"))
    if from_ts:
        filters.append(StreamAuditLog.created_at >= from_ts)
    if to_ts:
        filters.append(StreamAuditLog.created_at <= to_ts)
    if channel_id is not None:
        filters.append(StreamAuditLog.endpoint.ilike(f"%/channel/{channel_id}%"))
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
    event_types = _parse_event_types(params)
    severities = _parse_severities(params)
    search = (params.get("search") or "").strip()
    from_ts = _parse_iso_datetime(params.get("from_ts"))
    to_ts = _parse_iso_datetime(params.get("to_ts"))
    channel_id_raw = (params.get("channel_id") or "").strip()
    channel_id = None
    try:
        channel_id = int(channel_id_raw) if channel_id_raw else None
    except ValueError:
        channel_id = None
    if event_types:
        filters.append(CsoEventLog.event_type.in_(list(event_types)))
    if severities:
        severity_filters = [CsoEventLog.severity.in_(list(severities))]
        if "info" in severities:
            severity_filters.append(CsoEventLog.severity.is_(None))
        filters.append(or_(*severity_filters))
    if from_ts:
        filters.append(CsoEventLog.created_at >= from_ts)
    if to_ts:
        filters.append(CsoEventLog.created_at <= to_ts)
    if channel_id is not None:
        filters.append(CsoEventLog.channel_id == channel_id)
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
                    StreamAuditLog.severity.label("severity"),
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


async def _query_audit_event_types(params):
    values = set()
    async with Session() as session:
        stream_stmt = select(StreamAuditLog.event_type.label("event_type")).distinct()
        stream_stmt = stream_stmt.select_from(StreamAuditLog).outerjoin(User, User.id == StreamAuditLog.user_id)
        for condition in _stream_filters_from_params(params):
            stream_stmt = stream_stmt.where(condition)
        stream_result = await session.execute(stream_stmt)
        values.update(
            str(row.event_type or "").strip() for row in stream_result.all() if str(row.event_type or "").strip()
        )

        channel_stmt = select(CsoEventLog.event_type.label("event_type")).distinct().select_from(CsoEventLog)
        channel_stmt = channel_stmt.outerjoin(Channel, Channel.id == CsoEventLog.channel_id)
        for condition in _channel_event_filters_from_params(params):
            channel_stmt = channel_stmt.where(condition)
        channel_result = await session.execute(channel_stmt)
        values.update(
            str(row.event_type or "").strip() for row in channel_result.all() if str(row.event_type or "").strip()
        )

    return sorted(values, key=lambda value: value.lower())


@blueprint.route("/tic-api/audit/logs", methods=["GET"])
@admin_auth_required
async def api_list_audit_logs():
    include_all = str(request.args.get("include_all", "")).strip().lower() in {"1", "true", "yes", "on"}
    try:
        limit = int(request.args.get("limit", 50))
    except ValueError:
        limit = 50
    if include_all:
        limit = max(1, min(limit, 5000))
    else:
        limit = max(1, min(limit, 200))

    rows = await _query_unified_audit_rows(limit=limit, params=request.args)
    data = rows
    return jsonify({"success": True, "data": data})


@blueprint.route("/tic-api/audit/logs/poll", methods=["GET"])
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


@blueprint.route("/tic-api/audit/filter-options", methods=["GET"])
@admin_auth_required
async def api_audit_filter_options():
    args = request.args.to_dict(flat=True)
    args.pop("event_type", None)
    event_types = await _query_audit_event_types(args)
    return jsonify({"success": True, "data": {"event_types": event_types}})


@blueprint.route("/tic-api/audit/playback-start", methods=["POST"])
@streamer_or_admin_required
async def api_audit_playback_start():
    user = get_request_user()
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


@blueprint.route("/tic-api/audit/playback-heartbeat", methods=["POST"])
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


@blueprint.route("/tic-api/audit/playback-stop", methods=["POST"])
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
