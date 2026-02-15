#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from __future__ import annotations

import base64
import os
from pathlib import Path
from urllib.parse import unquote, urlparse, urlunparse

from quart import current_app, jsonify
from sqlalchemy import select

from backend import config as backend_config
from backend.api import blueprint
from backend.api.routes_channels import _build_channel_status, _fetch_channel_suggestion_counts
from backend.api.routes_hls_proxy import get_stream_activity_snapshot
from backend.audit_view import build_device_label, serialize_audit_row
from backend.auth import streamer_or_admin_required
from backend.channels import read_config_all_channels, read_logo_health_map
from backend.models import Channel, ChannelSource, Session, StreamAuditLog, User
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


def _normalize_url(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = urlparse(raw)
    except Exception:
        return raw
    if not parsed.scheme or not parsed.netloc:
        return raw
    normalized = parsed._replace(fragment="", query="")
    return urlunparse(normalized)


def _maybe_decode_embedded_url_once(value: str | None) -> str | None:
    normalized = _normalize_url(value)
    if not normalized:
        return None
    try:
        parsed = urlparse(normalized)
    except Exception:
        return None
    tail = (parsed.path or "").rsplit("/", 1)[-1]
    if not tail:
        return None
    token = tail.split(".", 1)[0]
    if not token:
        return None
    padded = token + "=" * (-len(token) % 4)
    for decoder in (base64.urlsafe_b64decode, base64.b64decode):
        try:
            decoded = decoder(padded.encode("utf-8")).decode("utf-8")
        except Exception:
            continue
        decoded_norm = _normalize_url(decoded)
        if decoded_norm and decoded_norm.startswith(("http://", "https://")):
            return decoded_norm
    return None


def _candidate_urls(value: str | None) -> list[str]:
    candidates = []
    normalized = _normalize_url(value)
    if normalized:
        candidates.append(normalized)
    current = normalized
    seen = set(candidates)
    while current:
        decoded = _maybe_decode_embedded_url_once(current)
        decoded_norm = _normalize_url(decoded)
        if not decoded_norm or decoded_norm in seen:
            break
        candidates.append(decoded_norm)
        seen.add(decoded_norm)
        current = decoded_norm
    return candidates


def _priority_rank(value: str | None) -> int:
    try:
        return int(str(value or "").strip())
    except Exception:
        return 1_000_000


async def _build_stream_source_index():
    stmt = (
        select(
            Channel.id.label("channel_id"),
            Channel.name.label("channel_name"),
            Channel.logo_url.label("channel_logo_url"),
            ChannelSource.playlist_stream_name.label("stream_name"),
            ChannelSource.playlist_stream_url.label("stream_url"),
            ChannelSource.priority.label("priority"),
        )
        .select_from(ChannelSource)
        .join(Channel, Channel.id == ChannelSource.channel_id)
    )
    async with Session() as session:
        result = await session.execute(stmt)
        rows = result.mappings().all()

    exact_map = {}
    for row in rows:
        stream_candidates = _candidate_urls(row.get("stream_url"))
        if not stream_candidates:
            continue
        stream_url = stream_candidates[0]
        payload = {
            "channel_id": row.get("channel_id"),
            "channel_name": row.get("channel_name"),
            "channel_logo_url": row.get("channel_logo_url"),
            "stream_name": row.get("stream_name"),
            "stream_url": stream_url,
            "priority": str(row.get("priority") or ""),
        }
        priority_rank = _priority_rank(payload.get("priority"))
        for depth, candidate_url in enumerate(stream_candidates):
            ranking = (depth, priority_rank)
            existing = exact_map.get(candidate_url)
            if not existing or ranking < existing.get("_ranking", (1_000_000, 1_000_000)):
                exact_map[candidate_url] = {**payload, "_ranking": ranking}
    return {"exact": exact_map}


def _resolve_stream_target(details: str | None, source_index: dict, related_urls: list[str] | None = None) -> dict:
    candidates = _candidate_urls(details)
    for url_value in related_urls or []:
        for candidate in _candidate_urls(url_value):
            if candidate not in candidates:
                candidates.append(candidate)

    exact_map = source_index.get("exact", {})

    matched_source = None
    for candidate in candidates:
        matched_source = exact_map.get(candidate)
        if matched_source:
            break

    display_url = candidates[0] if candidates else None
    source_url = matched_source.get("stream_url") if matched_source else None
    if not display_url and source_url:
        display_url = source_url

    return {
        "channel_id": matched_source.get("channel_id") if matched_source else None,
        "channel_name": matched_source.get("channel_name") if matched_source else None,
        "channel_logo_url": matched_source.get("channel_logo_url") if matched_source else None,
        "stream_name": matched_source.get("stream_name") if matched_source else None,
        "source_url": source_url,
        "display_url": display_url,
    }


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
    source_index = await _build_stream_source_index()
    data = []
    for row in activity_rows:
        ip_address = row.get("ip_address")
        user_agent = row.get("user_agent")
        resolved = _resolve_stream_target(
            row.get("details"),
            source_index,
            related_urls=row.get("related_urls") or [],
        )
        data.append(
            {
                "user_id": row.get("user_id"),
                "username": row.get("username"),
                "stream_key": row.get("stream_key"),
                "channel_id": resolved.get("channel_id"),
                "channel_name": resolved.get("channel_name"),
                "channel_logo_url": resolved.get("channel_logo_url"),
                "stream_name": resolved.get("stream_name"),
                "display_url": resolved.get("display_url"),
                "source_url": resolved.get("source_url"),
                "ip_address": ip_address,
                "user_agent": user_agent,
                "device_label": build_device_label(user_agent),
                "started_at": row.get("started_at"),
                "last_seen": row.get("last_seen"),
                "active_seconds": row.get("active_seconds"),
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
