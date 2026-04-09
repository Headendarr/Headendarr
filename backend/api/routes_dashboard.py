#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from __future__ import annotations

import base64
import asyncio
import os
import stat
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from quart import current_app, jsonify, request
from sqlalchemy import select

from backend.api import blueprint
# TODO: Rename these functions
from backend.api.routes_channels import (
    _build_channel_status,
    _fetch_channel_suggestion_counts,
    _fetch_cso_attention_map,
)
from backend.audit_view import build_device_label, serialize_audit_row
from backend.auth import admin_auth_required
from backend.cso import disconnect_active_stream_connection
from backend.channels import (
    build_stream_source_index,
    read_config_all_channels,
    read_logo_health_map,
    region_label,
    resolve_stream_target,
)
from backend.models import Channel, ChannelSource, Session, StreamAuditLog, User
from backend.stream_activity import get_stream_activity_snapshot, stop_stream_activity
from backend.tvheadend.tvh_requests import get_tvh
from backend.config import Config

_CHANNEL_ISSUE_SUMMARY_CACHE = {"expires_at": 0.0, "data": None}
_CHANNEL_ISSUE_SUMMARY_CACHE_LOCK = asyncio.Lock()
_STORAGE_SUMMARY_CACHE = {"expires_at": 0.0, "data": None}
_STORAGE_SUMMARY_CACHE_LOCK = asyncio.Lock()


def _can_force_disconnect_activity(row: dict) -> bool:
    connection_id = str(row.get("connection_id") or "").strip()
    return bool(connection_id)


def _measure_path_bytes(target: Path) -> int:
    try:
        stat_result = target.lstat()
    except OSError:
        return 0

    if stat.S_ISREG(stat_result.st_mode):
        blocks = getattr(stat_result, "st_blocks", None)
        if isinstance(blocks, int) and blocks > 0:
            return int(blocks * 512)
        return int(stat_result.st_size)

    if not stat.S_ISDIR(stat_result.st_mode):
        return 0

    total_bytes = 0
    stack = [target]
    seen_dirs: set[tuple[int, int]] = set()
    seen_files: set[tuple[int, int]] = set()
    while stack:
        current = stack.pop()
        try:
            current_stat = current.lstat()
        except OSError:
            continue
        current_key = (current_stat.st_dev, current_stat.st_ino)
        if current_key in seen_dirs:
            continue
        seen_dirs.add(current_key)
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        entry_stat = entry.stat(follow_symlinks=False)
                    except OSError:
                        continue
                    entry_key = (entry_stat.st_dev, entry_stat.st_ino)
                    if stat.S_ISDIR(entry_stat.st_mode):
                        if entry_key not in seen_dirs:
                            stack.append(Path(entry.path))
                        continue
                    if not stat.S_ISREG(entry_stat.st_mode) or entry_key in seen_files:
                        continue
                    seen_files.add(entry_key)
                    blocks = getattr(entry_stat, "st_blocks", None)
                    if isinstance(blocks, int) and blocks > 0:
                        total_bytes += int(blocks * 512)
                    else:
                        total_bytes += int(entry_stat.st_size)
        except OSError:
            continue
    return total_bytes


def _path_usage(path: str, label: str) -> dict[str, object]:
    payload = {
        "label": label,
        "path": path,
        "exists": False,
        "total_bytes": None,
        "filesystem_used_bytes": None,
        "available_bytes": None,
        "path_data_bytes": None,
        "other_used_bytes": None,
        "reserved_bytes": None,
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
        available = usage.f_bavail * usage.f_frsize
        filesystem_used = max((usage.f_blocks - usage.f_bfree) * usage.f_frsize, 0)
        path_data_bytes = _measure_path_bytes(target)
        other_used_bytes = max(filesystem_used - path_data_bytes, 0)
        reserved_bytes = max(total - filesystem_used - available, 0)
        payload["exists"] = True
        payload["total_bytes"] = int(total)
        payload["filesystem_used_bytes"] = int(filesystem_used)
        payload["available_bytes"] = int(available)
        payload["path_data_bytes"] = int(path_data_bytes)
        payload["other_used_bytes"] = int(other_used_bytes)
        payload["reserved_bytes"] = int(reserved_bytes)
    except Exception:
        return payload
    return payload


def _build_storage_items(app_config: Config) -> list[dict[str, object]]:
    return [
        _path_usage(app_config.config_path, "Configuration"),
        _path_usage(os.environ.get("TVH_RECORDINGS_PATH", "/recordings"), "Recordings"),
        _path_usage(os.environ.get("TVH_TIMESHIFT_PATH", "/timeshift"), "Timeshift"),
        _path_usage(os.environ.get("LIBRARY_EXPORT_PATH", "/library"), "Library"),
    ]


async def _storage_summary_cached(app_config: Config) -> list[dict[str, object]]:
    loop = asyncio.get_running_loop()
    now = loop.time()
    cached = _STORAGE_SUMMARY_CACHE.get("data")
    if cached is not None and now < float(_STORAGE_SUMMARY_CACHE.get("expires_at") or 0.0):
        return cached
    async with _STORAGE_SUMMARY_CACHE_LOCK:
        cached = _STORAGE_SUMMARY_CACHE.get("data")
        if cached is not None and now < float(_STORAGE_SUMMARY_CACHE.get("expires_at") or 0.0):
            return cached
        data = await asyncio.to_thread(_build_storage_items, app_config)
        _STORAGE_SUMMARY_CACHE["data"] = data
        _STORAGE_SUMMARY_CACHE["expires_at"] = now + 15.0
        return data


def _app_version_payload() -> dict[str, str | None]:
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


async def _recent_audit(limit: int = 10) -> list[dict[str, object]]:
    stmt = (
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
        .limit(limit)
    )
    async with Session() as session:
        result = await session.execute(stmt)
        rows = result.mappings().all()
    return [serialize_audit_row(dict(row)) for row in rows]


async def _channel_issue_summary() -> dict[str, object]:
    config = current_app.config["APP_CONFIG"]
    channels = await read_config_all_channels(include_status=True)
    mux_map = None
    tvh_mux_timeout_seconds = float(os.environ.get("DASHBOARD_TVH_MUX_TIMEOUT_SECONDS", "1.5") or 1.5)
    try:
        async with await get_tvh(config) as tvh:
            muxes = await asyncio.wait_for(tvh.list_all_muxes(), timeout=tvh_mux_timeout_seconds)
        mux_map = {mux.get("uuid"): mux for mux in muxes if mux.get("uuid")}
    except Exception:
        mux_map = None

    logo_health_map = read_logo_health_map(config)
    suggestion_counts = await _fetch_channel_suggestion_counts()
    cso_attention_map = await _fetch_cso_attention_map([channel.get("id") for channel in channels])
    issue_counts = {}
    warning_channels = 0
    for channel in channels:
        status = _build_channel_status(
            channel,
            mux_map,
            suggestion_counts.get(channel.get("id"), 0),
            logo_health=logo_health_map.get(str(channel.get("id")), {}),
            cso_health=cso_attention_map.get(channel.get("id"), {}),
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
        "cso_connection_issue": "CSO stream connection failures detected",
        "cso_stream_unhealthy": "CSO unhealthy stream conditions detected",
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


async def _channel_issue_summary_cached() -> dict[str, object]:
    ttl_seconds = float(os.environ.get("DASHBOARD_CHANNEL_ISSUE_CACHE_TTL_SECONDS", "10") or 10)
    loop = asyncio.get_running_loop()
    now = loop.time()
    cached = _CHANNEL_ISSUE_SUMMARY_CACHE.get("data")
    expires_at = _CHANNEL_ISSUE_SUMMARY_CACHE.get("expires_at", 0.0)
    if cached is not None and now < expires_at:
        return cached
    async with _CHANNEL_ISSUE_SUMMARY_CACHE_LOCK:
        now = loop.time()
        cached = _CHANNEL_ISSUE_SUMMARY_CACHE.get("data")
        expires_at = _CHANNEL_ISSUE_SUMMARY_CACHE.get("expires_at", 0.0)
        if cached is not None and now < expires_at:
            return cached
        data = await _channel_issue_summary()
        _CHANNEL_ISSUE_SUMMARY_CACHE["data"] = data
        _CHANNEL_ISSUE_SUMMARY_CACHE["expires_at"] = now + max(ttl_seconds, 0.5)
        return data


@blueprint.route("/tic-api/dashboard/activity", methods=["GET"])
@admin_auth_required
async def api_dashboard_activity():
    activity_rows = await get_stream_activity_snapshot()
    if not activity_rows:
        return jsonify({"success": True, "data": []})
    source_index = None
    data = []
    for row in activity_rows:
        ip_address = row.get("ip_address")
        user_agent = row.get("user_agent")
        client_hints = row.get("client_hints")

        # Prioritize metadata already in the tracker session (enriched during mark/upsert)
        channel_id = row.get("channel_id")
        channel_name = row.get("channel_name")
        channel_logo_url = row.get("channel_logo_url")
        display_url = row.get("display_url")
        source_url = row.get("source_url")
        stream_name = row.get("stream_name")

        if not channel_id or not channel_name:
            if source_index is None:
                source_index = await build_stream_source_index()
            resolved = resolve_stream_target(
                row.get("identity") or row.get("details"),
                source_index,
                related_urls=row.get("related_urls") or [],
            )
            channel_id = channel_id or resolved.get("channel_id")
            channel_name = channel_name or resolved.get("channel_name")
            channel_logo_url = channel_logo_url or resolved.get("channel_logo_url")
            display_url = display_url or resolved.get("display_url")
            source_url = source_url or resolved.get("source_url")
            stream_name = stream_name or resolved.get("stream_name")

        data.append(
            {
                "user_id": row.get("user_id"),
                "username": row.get("username"),
                "connection_id": row.get("connection_id"),
                "channel_id": channel_id,
                "channel_name": channel_name,
                "channel_logo_url": channel_logo_url,
                "stream_name": stream_name,
                "display_url": display_url,
                "source_url": source_url,
                "ip_address": ip_address,
                "user_agent": user_agent,
                "client_hints": client_hints,
                "device_label": build_device_label(user_agent, client_hints),
                "started_at": row.get("started_at"),
                "last_seen": row.get("last_seen"),
                "active_seconds": row.get("active_seconds"),
                "age_seconds": row.get("age_seconds"),
                "region_label": region_label(ip_address),
                "can_force_disconnect": _can_force_disconnect_activity(row),
            }
        )
    return jsonify({"success": True, "data": data})


@blueprint.route("/tic-api/dashboard/activity/disconnect", methods=["POST"])
@admin_auth_required
async def api_dashboard_activity_disconnect():
    payload = await request.get_json(force=True, silent=True) or {}
    connection_id = str(payload.get("connection_id") or payload.get("cid") or "").strip()
    if not connection_id:
        return jsonify({"success": False, "message": "Missing connection id"}), 400

    disconnected = await disconnect_active_stream_connection(connection_id)
    stopped = await stop_stream_activity(
        "",
        connection_id=connection_id,
        event_type="stream_stop",
        endpoint_override="/tic-api/dashboard/activity/disconnect",
    )
    if not disconnected and not stopped:
        return jsonify({"success": False, "message": "Active stream not found"}), 404
    return jsonify(
        {
            "success": True,
            "disconnected": bool(disconnected),
            "stopped": bool(stopped),
        }
    )


@blueprint.route("/tic-api/dashboard/summary", methods=["GET"])
@admin_auth_required
async def api_dashboard_summary():
    app_config = current_app.config["APP_CONFIG"]
    storage_items = await _storage_summary_cached(app_config)

    summary = {
        "version": _app_version_payload(),
        "recent_audit": await _recent_audit(limit=10),
        "storage": storage_items,
        "channels": await _channel_issue_summary_cached(),
    }
    return jsonify({"success": True, "data": summary})
