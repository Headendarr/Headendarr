#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import unquote

from quart import current_app, jsonify
from sqlalchemy import select

from backend import config as backend_config
from backend.api import blueprint
from backend.api.routes_channels import _build_channel_status, _fetch_channel_suggestion_counts
from backend.api.routes_hls_proxy import get_stream_activity_snapshot
from backend.audit_view import build_device_label, serialize_audit_row
from backend.auth import streamer_or_admin_required
from backend.channels import read_config_all_channels, read_logo_health_map
from backend.models import Session, StreamAuditLog, User
from backend.tvheadend.tvh_requests import get_tvh


def _path_usage(path: str, label: str):
    payload = {
        "label": label,
        "path": path,
        "exists": False,
        "total_bytes": None,
        "used_bytes": None,
        "free_bytes": None,
    }
    if not path:
        return payload
    try:
        target = Path(path).resolve()
        payload["path"] = str(target)
        if not target.exists():
            return payload
        usage = os.statvfs(str(target))
        total = usage.f_blocks * usage.f_frsize
        free = usage.f_bavail * usage.f_frsize
        used = max(total - free, 0)
        payload["exists"] = True
        payload["total_bytes"] = int(total)
        payload["used_bytes"] = int(used)
        payload["free_bytes"] = int(free)
    except Exception:
        return payload
    return payload


def _parse_db_path():
    uri = backend_config.sqlalchemy_database_uri
    if not uri.startswith("sqlite"):
        return {"label": "Database", "uri": uri, "path": None}
    # sqlite path forms:
    # sqlite:////abs/path.db
    # sqlite:///relative/path.db
    raw = uri.replace("sqlite:///", "", 1)
    raw = unquote(raw)
    if raw.startswith("/"):
        path = raw
    else:
        path = str((Path.cwd() / raw).resolve())
    return {"label": "Database", "uri": uri, "path": path}


def _app_version_payload():
    version = os.environ.get("APP_VERSION")
    git_sha = os.environ.get("GIT_SHA")
    if version:
        return {"version": version, "git_sha": git_sha}
    package_path = Path(__file__).resolve().parents[2] / "frontend" / "package.json"
    try:
        import json

        with package_path.open("r", encoding="utf-8") as fh:
            package = json.load(fh)
        return {"version": package.get("version"), "git_sha": git_sha}
    except Exception:
        return {"version": None, "git_sha": git_sha}


def _region_label(ip_address: str | None) -> str:
    ip = (ip_address or "").strip()
    if not ip:
        return "Unknown region"
    if ip.startswith("10.") or ip.startswith("192.168."):
        return "Local network"
    if ip.startswith("172."):
        try:
            second_octet = int(ip.split(".")[1])
            if 16 <= second_octet <= 31:
                return "Local network"
        except Exception:
            pass
    if ip.startswith("fd") or ip.startswith("fc") or ip.startswith("fe80:"):
        return "Local network"
    if ip in {"127.0.0.1", "::1"}:
        return "Local host"
    return "Unknown region"


async def _recent_audit(limit: int = 10):
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
        .order_by(StreamAuditLog.created_at.desc(), StreamAuditLog.id.desc())
        .limit(limit)
    )
    async with Session() as session:
        result = await session.execute(stmt)
        rows = result.mappings().all()
    return [serialize_audit_row(dict(row)) for row in rows]


async def _channel_issue_summary():
    config = current_app.config["APP_CONFIG"]
    channels = await read_config_all_channels(include_status=True)
    mux_map = None
    try:
        async with await get_tvh(config) as tvh:
            muxes = await tvh.list_all_muxes()
        mux_map = {mux.get("uuid"): mux for mux in muxes if mux.get("uuid")}
    except Exception:
        mux_map = None

    logo_health_map = read_logo_health_map(config)
    suggestion_counts = _fetch_channel_suggestion_counts()
    issue_counts = {}
    warning_channels = 0
    for channel in channels:
        status = _build_channel_status(
            channel,
            mux_map,
            suggestion_counts.get(channel.get("id"), 0),
            logo_health=logo_health_map.get(str(channel.get("id")), {}),
        )
        if status.get("state") != "warning":
            continue
        warning_channels += 1
        for issue in status.get("issues") or []:
            issue_counts[issue] = issue_counts.get(issue, 0) + 1

    issue_list = []
    label_map = {
        "missing_tvh_mux": "TVHeadend mux scanning has issues",
        "tvh_mux_failed": "TVHeadend mux scan failures detected",
        "all_sources_disabled": "One or more channels only have disabled sources",
        "no_sources": "One or more channels have no sources configured",
        "channel_logo_unavailable": "Channel logo fetch failures detected",
    }
    for key, count in sorted(issue_counts.items(), key=lambda item: item[1], reverse=True):
        issue_list.append(
            {
                "issue_key": key,
                "label": label_map.get(key, key.replace("_", " ")),
                "count": count,
                "route": "/channels",
            }
        )

    return {
        "channel_count": len(channels),
        "warning_channel_count": warning_channels,
        "issues": issue_list,
    }


@blueprint.route('/tic-api/dashboard/activity', methods=['GET'])
@streamer_or_admin_required
async def api_dashboard_activity():
    activity_rows = await get_stream_activity_snapshot()
    data = []
    for row in activity_rows:
        ip_address = row.get("ip_address")
        user_agent = row.get("user_agent")
        data.append(
            {
                "user_id": row.get("user_id"),
                "username": row.get("username"),
                "stream_key": row.get("stream_key"),
                "endpoint": row.get("endpoint"),
                "details": row.get("details"),
                "ip_address": ip_address,
                "user_agent": user_agent,
                "device_label": build_device_label(user_agent),
                "last_seen": row.get("last_seen"),
                "age_seconds": row.get("age_seconds"),
                "region_label": _region_label(ip_address),
            }
        )
    return jsonify({"success": True, "data": data})


@blueprint.route('/tic-api/dashboard/summary', methods=['GET'])
@streamer_or_admin_required
async def api_dashboard_summary():
    app_config = current_app.config["APP_CONFIG"]
    db_info = _parse_db_path()
    storage_items = [
        _path_usage(app_config.config_path, "Configuration"),
        _path_usage(os.environ.get("TVH_RECORDINGS_PATH", "/recordings"), "Recordings"),
        _path_usage(os.environ.get("TVH_TIMESHIFT_PATH", "/timeshift"), "Timeshift"),
    ]
    if db_info.get("path"):
        storage_items.append(_path_usage(db_info["path"], "Database"))

    summary = {
        "version": _app_version_payload(),
        "recent_audit": await _recent_audit(limit=10),
        "storage": storage_items,
        "channels": await _channel_issue_summary(),
    }
    return jsonify({"success": True, "data": summary})
