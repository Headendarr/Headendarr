#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import io
import json
import time
from datetime import datetime
from typing import Any
from backend.api import blueprint
from quart import request, jsonify, current_app, send_file
from urllib.parse import unquote, urlparse

from backend.auth import admin_auth_required, streamer_or_admin_required, audit_stream_event
from backend.channels import (
    read_config_all_channels,
    add_new_channel,
    read_config_one_channel,
    update_channel,
    delete_channel,
    add_bulk_channels,
    update_channels_order,
    queue_background_channel_update_tasks,
    read_channel_logo,
    add_channels_from_groups,
    read_logo_health_map,
    build_bulk_epg_match_preview,
    read_epg_match_candidate_preview,
    apply_bulk_epg_matches,
    apply_bulk_cso_settings,
    build_cso_channel_stream_url,
)
from backend.epgs import build_channel_logo_output_url
from backend.streaming import build_local_hls_proxy_url, normalize_local_proxy_url
from backend.url_resolver import get_request_base_url
from backend.utils import fast_url_hash, parse_entity_id, is_truthy, to_utc_iso
from backend.tvheadend.tvh_requests import get_tvh
from backend.models import Session, Channel, ChannelSource, ChannelSuggestion, PlaylistStreams, CsoEventLog, Playlist
from sqlalchemy import select, func
from sqlalchemy.orm import joinedload
from backend.vod_channels import is_vod_channel_type


async def _fetch_tvh_mux_map(config):
    try:
        async with await get_tvh(config) as tvh:
            muxes = await tvh.list_all_muxes()
        return {mux.get("uuid"): mux for mux in muxes if mux.get("uuid")}
    except Exception as exc:
        current_app.logger.warning("Failed to fetch TVH mux list: %s", exc)
        return None


def _parse_cso_event_details(details_json):
    if not details_json:
        return {}
    try:
        payload = json.loads(details_json)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _created_at_sort_key(payload):
    created_at = (payload or {}).get("_created_at")
    if isinstance(created_at, datetime):
        return created_at.timestamp()
    if isinstance(created_at, (int, float)):
        return float(created_at)
    return 0.0


async def _fetch_cso_attention_map(channel_ids):
    normalized_ids = []
    for channel_id in channel_ids or []:
        try:
            normalized_ids.append(int(channel_id))
        except Exception:
            continue
    if not normalized_ids:
        return {}

    async with Session() as session:
        result = await session.execute(
            select(CsoEventLog)
            .where(
                CsoEventLog.channel_id.in_(normalized_ids),
                CsoEventLog.event_type.in_(
                    [
                        "playback_unavailable",
                        "capacity_blocked",
                        "health_actioned",
                        "health_recovered",
                        "scheduled_health_failed",
                        "scheduled_health_recovered",
                        "switch_success",
                        "session_start",
                    ]
                ),
            )
            .order_by(CsoEventLog.created_at.asc())
        )
        rows = result.scalars().all()
        sources_result = await session.execute(
            select(
                ChannelSource.channel_id,
                ChannelSource.id,
                ChannelSource.playlist_id,
                Playlist.enabled,
            )
            .outerjoin(Playlist, ChannelSource.playlist_id == Playlist.id)
            .where(ChannelSource.channel_id.in_(normalized_ids))
        )
        active_sources_by_channel = {}
        for channel_id, source_id, playlist_id, playlist_enabled in sources_result.all():
            channel_key = int(channel_id or 0)
            source_key = int(source_id or 0)
            if channel_key <= 0 or source_key <= 0:
                continue
            if playlist_id is not None and not bool(playlist_enabled):
                continue
            active_sources_by_channel.setdefault(channel_key, set()).add(source_key)

    state = {}
    for row in rows:
        channel_id = int(row.channel_id or 0)
        if channel_id <= 0:
            continue
        channel_state = state.setdefault(
            channel_id,
            {
                "latest_connection_recovery_at": None,
                "connection_issue": None,
                "health_by_source": {},
            },
        )
        details = _parse_cso_event_details(row.details_json)
        reason = str(details.get("reason") or details.get("after_failure_reason") or "").strip().lower()
        event_type = str(row.event_type or "").strip().lower()
        severity = str(row.severity or "warning").strip().lower()
        created_at = row.created_at

        payload = {
            "event_type": event_type,
            "severity": severity,
            "reason": reason,
            "details": details,
            "created_at": to_utc_iso(created_at),
            "_created_at": created_at,
            "source_id": int(getattr(row, "source_id", 0) or 0),
        }

        if event_type in {"switch_success", "session_start"}:
            latest_recovery = channel_state.get("latest_connection_recovery_at")
            if latest_recovery is None or created_at > latest_recovery:
                channel_state["latest_connection_recovery_at"] = created_at
            continue

        if event_type == "playback_unavailable":
            current = channel_state.get("connection_issue")
            if current is None or created_at > current.get("_created_at"):
                channel_state["connection_issue"] = payload

        if event_type in {"health_actioned", "scheduled_health_failed"} or reason in {
            "under_speed",
            "stall_timeout",
            "too_slow",
            "unreachable",
            "unstable",
        }:
            source_id = int(payload.get("source_id") or int(details.get("source_id") or 0) or 0)
            health_state = channel_state["health_by_source"].setdefault(
                source_id,
                {
                    "latest_unhealthy": None,
                    "latest_recovery_at": None,
                },
            )
            current = health_state.get("latest_unhealthy")
            if current is None or created_at > current.get("_created_at"):
                health_state["latest_unhealthy"] = payload
            continue

        if event_type in {"health_recovered", "scheduled_health_recovered"}:
            source_id = int(payload.get("source_id") or int(details.get("source_id") or 0) or 0)
            health_state = channel_state["health_by_source"].setdefault(
                source_id,
                {
                    "latest_unhealthy": None,
                    "latest_recovery_at": None,
                },
            )
            latest_recovery = health_state.get("latest_recovery_at")
            if latest_recovery is None or created_at > latest_recovery:
                health_state["latest_recovery_at"] = created_at

    attention_map = {}
    for channel_id, channel_state in state.items():
        active_source_ids = active_sources_by_channel.get(channel_id, set())
        recovery_at = channel_state.get("latest_connection_recovery_at")
        issues = []
        latest_event = None

        connection_issue = channel_state.get("connection_issue")
        if connection_issue:
            issue_at = connection_issue.get("_created_at")
            if recovery_at is None or (issue_at and issue_at > recovery_at):
                issues.append("cso_connection_issue")
                latest_event = connection_issue

        unresolved_source_issues = []
        for source_id, source_state in (channel_state.get("health_by_source") or {}).items():
            source_key = int(source_id or 0)
            if source_key <= 0 or source_key not in active_source_ids:
                continue
            unhealthy_issue = source_state.get("latest_unhealthy")
            if not unhealthy_issue:
                continue
            issue_at = unhealthy_issue.get("_created_at")
            source_recovery_at = source_state.get("latest_recovery_at")
            if source_recovery_at is not None and issue_at and issue_at <= source_recovery_at:
                continue
            unresolved_source_issues.append(unhealthy_issue)

        if unresolved_source_issues:
            issues.append("cso_stream_unhealthy")
            for unresolved_issue in unresolved_source_issues:
                if latest_event is None or (
                    unresolved_issue.get("_created_at")
                    and unresolved_issue.get("_created_at") > latest_event.get("_created_at")
                ):
                    latest_event = unresolved_issue

        if not issues:
            continue
        if latest_event:
            latest_event.pop("_created_at", None)
        source_issues = []
        for unresolved_issue in sorted(
            unresolved_source_issues,
            key=_created_at_sort_key,
            reverse=True,
        ):
            payload = dict(unresolved_issue)
            payload.pop("_created_at", None)
            source_issues.append(payload)
        attention_map[channel_id] = {
            "issues": sorted(set(issues)),
            "latest_event": latest_event or {},
            "source_issues": source_issues,
        }
    for payload in attention_map.values():
        payload["issues"] = sorted(payload["issues"])
    return attention_map


def _build_channel_status(channel, mux_map, suggestion_count=0, logo_health=None, cso_health=None):
    if not channel.get("enabled"):
        return {
            "state": "disabled",
            "issues": [],
            "disabled_source_count": 0,
            "missing_mux_count": 0,
            "failed_mux_count": 0,
            "suggestion_count": suggestion_count,
        }

    if is_vod_channel_type(channel.get("channel_type")):
        issues = []
        source_logo_url = channel.get("source_logo_url") or channel.get("logo_url")
        if logo_health and logo_health.get("status") == "error" and source_logo_url:
            issues.append("channel_logo_unavailable")
        cso_issues = list((cso_health or {}).get("issues") or [])
        for issue in cso_issues:
            if issue not in issues:
                issues.append(issue)
        return {
            "state": "warning" if issues else "ok",
            "issues": issues,
            "disabled_source_count": 0,
            "missing_mux_count": 0,
            "failed_mux_count": 0,
            "missing_streams": [],
            "failed_streams": [],
            "logo_health": logo_health or {},
            "cso_health": cso_health or {},
            "suggestion_count": suggestion_count,
        }

    sources = channel.get("sources") or []
    if not sources:
        return {
            "state": "warning",
            "issues": ["no_sources"],
            "disabled_source_count": 0,
            "missing_mux_count": 0,
            "failed_mux_count": 0,
            "suggestion_count": suggestion_count,
        }

    disabled_sources = 0
    missing_muxes = 0
    failed_muxes = 0
    missing_streams = []
    failed_streams = []
    has_enabled_source = False
    cso_enabled = bool(channel.get("cso_enabled", False))

    for source in sources:
        if source.get("source_type") == "manual":
            if source.get("stream_url"):
                has_enabled_source = True
            continue

        playlist_enabled = source.get("playlist_enabled", True)
        if not playlist_enabled:
            disabled_sources += 1
            continue

        has_enabled_source = True
        if cso_enabled:
            continue
        tvh_uuid = source.get("tvh_uuid")
        if mux_map is None:
            continue
        if not tvh_uuid or tvh_uuid not in mux_map:
            missing_muxes += 1
            missing_streams.append(
                {
                    "stream_name": source.get("stream_name"),
                    "playlist_name": source.get("playlist_name"),
                }
            )
            continue
        mux_entry = mux_map.get(tvh_uuid) or {}
        if "enabled" in mux_entry and not is_truthy(mux_entry.get("enabled")):
            failed_muxes += 1
            failed_streams.append(
                {
                    "stream_name": source.get("stream_name"),
                    "playlist_name": source.get("playlist_name"),
                }
            )
        scan_result = mux_entry.get("scan_result")
        if scan_result == 2:
            failed_muxes += 1
            failed_streams.append(
                {
                    "stream_name": source.get("stream_name"),
                    "playlist_name": source.get("playlist_name"),
                }
            )

    issues = []
    if not has_enabled_source:
        issues.append("all_sources_disabled")
    if missing_muxes:
        issues.append("missing_tvh_mux")
    if failed_muxes:
        issues.append("tvh_mux_failed")
    source_logo_url = channel.get("source_logo_url") or channel.get("logo_url")
    if logo_health and logo_health.get("status") == "error" and source_logo_url:
        issues.append("channel_logo_unavailable")
    cso_issues = list((cso_health or {}).get("issues") or [])
    for issue in cso_issues:
        if issue not in issues:
            issues.append(issue)
    return {
        "state": "warning" if issues else "ok",
        "issues": issues,
        "disabled_source_count": disabled_sources,
        "missing_mux_count": missing_muxes,
        "failed_mux_count": failed_muxes,
        "missing_streams": missing_streams,
        "failed_streams": failed_streams,
        "logo_health": logo_health or {},
        "cso_health": cso_health or {},
        "suggestion_count": suggestion_count,
    }


def _build_backend_logo_url(config, request_base_url, channel_id, source_logo_url):
    return build_channel_logo_output_url(
        config,
        channel_id,
        request_base_url,
        source_logo_url,
    )


async def _fetch_channel_suggestion_counts():
    async with Session() as session:
        result = await session.execute(
            select(
                ChannelSuggestion.channel_id,
                func.count(ChannelSuggestion.id),
            )
            .where(ChannelSuggestion.dismissed.is_(False))
            .group_by(ChannelSuggestion.channel_id)
        )
        rows = result.all()
    return {row[0]: row[1] for row in rows}


@blueprint.route("/tic-api/channels/get", methods=["GET"])
@admin_auth_required
async def api_get_channels():
    include_status = request.args.get("include_status") == "true"
    channels_config = await read_config_all_channels(include_status=include_status)
    config = current_app.config["APP_CONFIG"]
    request_base_url = get_request_base_url(request)
    for channel in channels_config:
        source_logo_url = channel.get("logo_url")
        channel["source_logo_url"] = source_logo_url
        channel["logo_url"] = _build_backend_logo_url(
            config,
            request_base_url,
            channel.get("id"),
            source_logo_url,
        )
    if include_status:
        config = current_app.config["APP_CONFIG"]
        mux_map = await _fetch_tvh_mux_map(config)
        suggestion_counts = await _fetch_channel_suggestion_counts()
        logo_health_map = read_logo_health_map(config)
        cso_attention_map = await _fetch_cso_attention_map([channel.get("id") for channel in channels_config])
        for channel in channels_config:
            suggestion_count = suggestion_counts.get(channel.get("id"), 0)
            logo_health = logo_health_map.get(str(channel.get("id")), {})
            channel["status"] = _build_channel_status(
                channel,
                mux_map,
                suggestion_count,
                logo_health=logo_health,
                cso_health=cso_attention_map.get(channel.get("id"), {}),
            )
    return jsonify({"success": True, "data": channels_config})


@blueprint.route("/tic-api/channels/<channel_id>/stream-suggestions", methods=["GET"])
@admin_auth_required
async def api_get_channel_stream_suggestions(channel_id):
    channel_id = parse_entity_id(channel_id, "channel")
    async with Session() as session:
        result = await session.execute(
            select(ChannelSuggestion)
            .where(
                ChannelSuggestion.channel_id == channel_id,
                ChannelSuggestion.dismissed.is_(False),
            )
            .order_by(ChannelSuggestion.score.desc())
        )
        suggestions = result.scalars().all()

    # XC and similar providers may expose the same stream URL under multiple groups.
    # Keep the top-scored suggestion per URL so the UI only shows unique stream URLs.
    deduplicated_suggestions = []
    seen_stream_urls = set()
    for suggestion in suggestions:
        stream_url = (suggestion.stream_url or "").strip()
        if stream_url and stream_url in seen_stream_urls:
            continue
        if stream_url:
            seen_stream_urls.add(stream_url)
        deduplicated_suggestions.append(suggestion)

    return jsonify(
        {
            "success": True,
            "data": [
                {
                    "id": suggestion.id,
                    "channel_id": suggestion.channel_id,
                    "playlist_id": suggestion.playlist_id,
                    "stream_id": suggestion.stream_id,
                    "stream_name": suggestion.stream_name,
                    "stream_url": suggestion.stream_url,
                    "group_title": suggestion.group_title,
                    "playlist_name": suggestion.playlist_name,
                    "source_type": suggestion.source_type,
                    "score": suggestion.score,
                }
                for suggestion in deduplicated_suggestions
            ],
        }
    )


@blueprint.route("/tic-api/channels/<channel_id>/logo-suggestions", methods=["GET"])
@admin_auth_required
async def api_get_channel_logo_suggestions(channel_id):
    channel_id = parse_entity_id(channel_id, "channel")
    async with Session() as session:
        result = await session.execute(
            select(Channel)
            .options(joinedload(Channel.sources).joinedload(ChannelSource.playlist))
            .where(Channel.id == channel_id)
        )
        channel = result.scalars().unique().one_or_none()
    if not channel:
        return jsonify({"success": False, "message": "Channel not found"}), 404

    suggestions = []
    seen = set()

    def _add(url, source, label):
        if not url:
            return
        url = url.strip()
        if not url or url in seen:
            return
        seen.add(url)
        suggestions.append(
            {
                "url": url,
                "source": source,
                "label": label,
            }
        )

    # 1) Stream logos from linked playlist streams.
    for source in channel.sources or []:
        if not source.playlist_id:
            continue
        stream = None
        if source.playlist_stream_name:
            async with Session() as session:
                result = await session.execute(
                    select(PlaylistStreams).where(
                        PlaylistStreams.playlist_id == source.playlist_id,
                        PlaylistStreams.name == source.playlist_stream_name,
                    )
                )
                stream = result.scalars().first()
        if not stream and source.playlist_stream_url:
            stream_url_hash = fast_url_hash(source.playlist_stream_url)
            async with Session() as session:
                result = await session.execute(
                    select(PlaylistStreams).where(
                        PlaylistStreams.playlist_id == source.playlist_id,
                        PlaylistStreams.url_hash == stream_url_hash,
                        PlaylistStreams.url == source.playlist_stream_url,
                    )
                )
                stream = result.scalars().first()
        if stream and stream.tvg_logo:
            playlist_name = source.playlist.name if getattr(source, "playlist", None) else "playlist"
            _add(
                stream.tvg_logo,
                "stream",
                f"Stream logo ({playlist_name}: {source.playlist_stream_name or 'unknown'})",
            )

    # 2) EPG icon for mapped guide channel.
    if channel.guide_id and channel.guide_channel_id:
        from backend.epgs import load_preferred_epg_channel_row

        async with Session() as session:
            epg_channel = await load_preferred_epg_channel_row(
                session,
                epg_id=int(channel.guide_id),
                channel_id=str(channel.guide_channel_id),
            )
        if epg_channel and epg_channel.get("icon_url"):
            _add(
                epg_channel["icon_url"],
                "epg",
                f"EPG icon ({channel.guide_name or channel.guide_id}: {channel.guide_channel_id})",
            )

    # 3) Current channel logo last so it can still be selected if desired.
    if channel.logo_url:
        _add(channel.logo_url, "current", "Current channel logo")

    return jsonify({"success": True, "data": suggestions})


@blueprint.route("/tic-api/channels/<channel_id>/logo-suggestions/apply", methods=["POST"])
@admin_auth_required
async def api_apply_channel_logo_suggestion(channel_id):
    channel_id = parse_entity_id(channel_id, "channel")
    async with Session() as session:
        async with session.begin():
            result = await session.execute(
                select(Channel)
                .options(joinedload(Channel.sources).joinedload(ChannelSource.playlist))
                .where(Channel.id == channel_id)
            )
            channel = result.scalars().unique().one_or_none()
            if not channel:
                return jsonify({"success": False, "message": "Channel not found"}), 404

            # Reuse existing suggestion endpoint logic by generating the same candidate list inline.
            suggestions = []
            seen = set()

            def _add(url):
                if not url:
                    return
                url = url.strip()
                if not url or url in seen:
                    return
                seen.add(url)
                suggestions.append(url)

            for source in channel.sources or []:
                if not source.playlist_id:
                    continue
                stream = None
                if source.playlist_stream_name:
                    stream_result = await session.execute(
                        select(PlaylistStreams).where(
                            PlaylistStreams.playlist_id == source.playlist_id,
                            PlaylistStreams.name == source.playlist_stream_name,
                        )
                    )
                    stream = stream_result.scalars().first()
                if not stream and source.playlist_stream_url:
                    stream_url_hash = fast_url_hash(source.playlist_stream_url)
                    stream_result = await session.execute(
                        select(PlaylistStreams).where(
                            PlaylistStreams.playlist_id == source.playlist_id,
                            PlaylistStreams.url_hash == stream_url_hash,
                            PlaylistStreams.url == source.playlist_stream_url,
                        )
                    )
                    stream = stream_result.scalars().first()
                if stream and stream.tvg_logo:
                    _add(stream.tvg_logo)

            if channel.guide_id and channel.guide_channel_id:
                from backend.epgs import load_preferred_epg_channel_row

                epg_channel = await load_preferred_epg_channel_row(
                    session,
                    epg_id=int(channel.guide_id),
                    channel_id=str(channel.guide_channel_id),
                )
                if epg_channel and epg_channel.get("icon_url"):
                    _add(epg_channel["icon_url"])

            payload = await request.get_json(silent=True) or {}
            requested_url = (payload.get("url") or "").strip()
            current_logo = (channel.logo_url or "").strip()
            normalized_current_logo = unquote(current_logo)

            def _find_matching_suggestion(url):
                if not url:
                    return None
                normalized_url = unquote(url.strip())
                return next(
                    (candidate for candidate in suggestions if unquote((candidate or "").strip()) == normalized_url),
                    None,
                )

            chosen = None
            if requested_url:
                chosen = _find_matching_suggestion(requested_url)
                if not chosen:
                    return jsonify({"success": False, "message": "Requested logo URL is not a valid suggestion"}), 400
            else:
                chosen = next(
                    (url for url in suggestions if unquote((url or "").strip()) != normalized_current_logo),
                    None,
                )
                if not chosen and suggestions:
                    # Fallback: re-apply current suggestion URL to force a cache refresh.
                    chosen = suggestions[0]
            if not chosen:
                return jsonify({"success": False, "message": "No alternative logo suggestion found"}), 404

            channel.logo_url = chosen
            channel.logo_base64 = None

    config = current_app.config["APP_CONFIG"]
    await queue_background_channel_update_tasks(config)

    return jsonify({"success": True, "data": {"logo_url": chosen}})


@blueprint.route("/tic-api/channels/<channel_id>/stream-suggestions/<suggestion_id>/dismiss", methods=["POST"])
@admin_auth_required
async def api_dismiss_channel_stream_suggestion(channel_id, suggestion_id):
    channel_id = parse_entity_id(channel_id, "channel")
    suggestion_id = parse_entity_id(suggestion_id, "suggestion")
    async with Session() as session:
        async with session.begin():
            result = await session.execute(
                select(ChannelSuggestion).where(
                    ChannelSuggestion.id == suggestion_id,
                    ChannelSuggestion.channel_id == channel_id,
                )
            )
            suggestion = result.scalar_one_or_none()
            if not suggestion:
                return jsonify({"success": False, "message": "Suggestion not found"}), 404
            suggestion.dismissed = True
            await session.commit()
    return jsonify({"success": True})


@blueprint.route("/tic-api/channels/basic", methods=["GET"])
@streamer_or_admin_required
async def api_get_channels_basic():
    channels_config = await read_config_all_channels()
    config = current_app.config["APP_CONFIG"]
    request_base_url = get_request_base_url(request)
    basic = []
    for channel in channels_config:
        if not channel.get("enabled"):
            continue
        basic.append(
            {
                "id": channel.get("id"),
                "name": channel.get("name"),
                "number": channel.get("number"),
                "logo_url": _build_backend_logo_url(
                    config,
                    request_base_url,
                    channel.get("id"),
                    channel.get("logo_url"),
                ),
                "guide": channel.get("guide") or {},
            }
        )
    return jsonify({"success": True, "data": basic})


def _infer_stream_type(url: str) -> str:
    parsed = urlparse(url)
    if parsed.path.lower().endswith(".m3u8"):
        return "hls"
    if parsed.path.lower().endswith(".ts"):
        return "mpegts"
    if "/tic-hls-proxy/" in parsed.path and "/stream/" in parsed.path:
        return "mpegts"
    if "/tic-api/cso/channel/" in parsed.path:
        return "mpegts"
    if "/tic-api/cso/channel_stream/" in parsed.path:
        return "mpegts"
    return "auto"


def _build_preview_url_for_source(
    source: ChannelSource, user: Any, config: Any, request_base_url: str
) -> tuple[str, str]:
    instance_id = config.ensure_instance_id()

    is_manual = not source.playlist_id
    use_hls_proxy = bool(getattr(source, "use_hls_proxy", False)) if is_manual else False
    if is_manual and use_hls_proxy:
        preview_url = build_local_hls_proxy_url(
            request_base_url,
            instance_id,
            source.playlist_stream_url,
            stream_key=user.streaming_key,
        )
    else:
        preview_url = normalize_local_proxy_url(
            source.playlist_stream_url,
            base_url=request_base_url,
            instance_id=instance_id,
            stream_key=user.streaming_key,
        )
    stream_type = _infer_stream_type(preview_url)
    return preview_url, stream_type


@blueprint.route("/tic-api/channels/<int:channel_id>/preview", methods=["GET"])
@streamer_or_admin_required
async def api_get_channel_preview(channel_id):
    user = getattr(request, "_current_user", None)
    if not user or not user.streaming_key:
        return jsonify({"success": False, "message": "Streaming key missing"}), 400

    async with Session() as session:
        channel_result = await session.execute(select(Channel).where(Channel.id == channel_id))
        channel = channel_result.scalars().first()
        if not channel:
            return jsonify({"success": False, "message": "Channel not found"}), 404
        if is_vod_channel_type(channel.channel_type):
            config = current_app.config["APP_CONFIG"]
            request_base_url = get_request_base_url(request)
            preview_url = build_cso_channel_stream_url(
                base_url=request_base_url,
                channel_id=channel.id,
                stream_key=user.streaming_key,
                username=user.username,
                profile="default",
            )
            return jsonify(
                {"success": True, "preview_url": preview_url, "stream_type": _infer_stream_type(preview_url)}
            )
        result = await session.execute(
            select(ChannelSource).where(ChannelSource.channel_id == channel_id).order_by(ChannelSource.id.asc())
        )
        source = result.scalars().first()

    if not source or not source.playlist_stream_url:
        return jsonify({"success": False, "message": "Channel has no source URL"}), 404

    config = current_app.config["APP_CONFIG"]
    request_base_url = get_request_base_url(request)
    preview_url, stream_type = _build_preview_url_for_source(
        source=source,
        user=user,
        config=config,
        request_base_url=request_base_url,
    )
    return jsonify({"success": True, "preview_url": preview_url, "stream_type": stream_type})


@blueprint.route("/tic-api/channels/<int:channel_id>/sources/<int:source_id>/preview", methods=["GET"])
@streamer_or_admin_required
async def api_get_channel_source_preview(channel_id, source_id):
    user = getattr(request, "_current_user", None)
    if not user or not user.streaming_key:
        return jsonify({"success": False, "message": "Streaming key missing"}), 400

    async with Session() as session:
        channel_result = await session.execute(select(Channel).where(Channel.id == channel_id))
        channel = channel_result.scalars().first()
        if not channel:
            return jsonify({"success": False, "message": "Channel not found"}), 404

        result = await session.execute(
            select(ChannelSource).where(
                ChannelSource.id == source_id,
                ChannelSource.channel_id == channel_id,
            )
        )
        source = result.scalars().first()

    if not source or not source.playlist_stream_url:
        return jsonify({"success": False, "message": "Channel source not found"}), 404

    config = current_app.config["APP_CONFIG"]
    request_base_url = get_request_base_url(request)
    preview_url, stream_type = _build_preview_url_for_source(
        source=source,
        user=user,
        config=config,
        request_base_url=request_base_url,
    )
    return jsonify({"success": True, "preview_url": preview_url, "stream_type": stream_type})


@blueprint.route("/tic-api/channel-stream-events", methods=["GET"])
@admin_auth_required
async def api_get_channel_stream_events():
    try:
        channel_id = int(request.args.get("channel_id")) if request.args.get("channel_id") else None
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "Invalid channel_id"}), 400
    event_type = (request.args.get("event_type") or "").strip().lower() or None
    severity = (request.args.get("severity") or "").strip().lower() or None
    try:
        limit = int(request.args.get("limit") or 100)
    except (TypeError, ValueError):
        limit = 100
    limit = max(1, min(limit, 500))

    async with Session() as session:
        stmt = select(CsoEventLog).order_by(CsoEventLog.created_at.desc(), CsoEventLog.id.desc()).limit(limit)
        if channel_id is not None:
            stmt = stmt.where(CsoEventLog.channel_id == channel_id)
        if event_type:
            stmt = stmt.where(CsoEventLog.event_type == event_type)
        if severity:
            stmt = stmt.where(CsoEventLog.severity == severity)
        result = await session.execute(stmt)
        events = result.scalars().all()

    payload = []
    for event in events:
        details = None
        if event.details_json:
            try:
                details = json.loads(event.details_json)
            except Exception:
                details = {"raw": event.details_json}
        payload.append(
            {
                "id": event.id,
                "created_at": to_utc_iso(event.created_at),
                "channel_id": event.channel_id,
                "source_id": event.source_id,
                "playlist_id": event.playlist_id,
                "recording_id": event.recording_id,
                "tvh_subscription_id": event.tvh_subscription_id,
                "session_id": event.session_id,
                "event_type": event.event_type,
                "severity": event.severity,
                "details": details,
            }
        )
    return jsonify({"success": True, "data": payload})


@blueprint.route("/tic-api/channels/new", methods=["POST"])
@admin_auth_required
async def api_add_new_channel():
    json_data = await request.get_json()
    config = current_app.config["APP_CONFIG"]
    await add_new_channel(config, json_data)
    await queue_background_channel_update_tasks(config)
    return jsonify({"success": True})


@blueprint.route("/tic-api/channels/settings/<channel_id>", methods=["GET"])
@admin_auth_required
async def api_get_channel_config(channel_id):
    try:
        channel_id = int(channel_id)
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "Invalid channel id"}), 400
    channel_config = await read_config_one_channel(channel_id)
    return jsonify({"success": True, "data": channel_config})


@blueprint.route("/tic-api/channels/settings/<channel_id>/save", methods=["POST"])
@admin_auth_required
async def api_set_config_channels(channel_id):
    json_data = await request.get_json()
    config = current_app.config["APP_CONFIG"]
    try:
        channel_id = int(channel_id)
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "Invalid channel id"}), 400
    await update_channel(config, channel_id, json_data)
    await queue_background_channel_update_tasks(config)
    return jsonify({"success": True})


@blueprint.route("/tic-api/channels/settings/multiple/save-order", methods=["POST"])
@admin_auth_required
async def api_set_config_multiple_channels_order():
    json_data = await request.get_json()
    config = current_app.config["APP_CONFIG"]
    try:
        await update_channels_order(config, json_data.get("channels", {}))
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    await queue_background_channel_update_tasks(config)
    return jsonify({"success": True})


@blueprint.route("/tic-api/channels/settings/multiple/save", methods=["POST"])
@admin_auth_required
async def api_set_config_multiple_channels():
    json_data = await request.get_json()
    config = current_app.config["APP_CONFIG"]
    for channel_id in json_data.get("channels", {}):
        channel = json_data["channels"][channel_id]
        normalized = parse_entity_id(channel_id, "channel")
        await update_channel(config, normalized, channel)
    await queue_background_channel_update_tasks(config)
    return jsonify({"success": True})


@blueprint.route("/tic-api/channels/sync", methods=["POST"])
@admin_auth_required
async def api_sync_channels():
    config = current_app.config["APP_CONFIG"]
    await queue_background_channel_update_tasks(config)
    return jsonify({"success": True})


@blueprint.route("/tic-api/channels/settings/multiple/add", methods=["POST"])
@admin_auth_required
async def api_add_multiple_channels():
    json_data = await request.get_json()
    config = current_app.config["APP_CONFIG"]
    await add_bulk_channels(config, json_data.get("channels", []))
    await queue_background_channel_update_tasks(config)
    return jsonify({"success": True})


@blueprint.route("/tic-api/channels/bulk/epg-match/preview", methods=["POST"])
@admin_auth_required
async def api_bulk_epg_match_preview():
    payload = await request.get_json(silent=True) or {}
    channel_ids = payload.get("channel_ids", [])
    overwrite_existing = bool(payload.get("overwrite_existing", False))
    max_candidates_per_channel = payload.get("max_candidates_per_channel", 5)

    start_ts = time.perf_counter()
    preview_data = await build_bulk_epg_match_preview(
        channel_ids=channel_ids,
        overwrite_existing=overwrite_existing,
        max_candidates_per_channel=max_candidates_per_channel,
    )
    elapsed_ms = int((time.perf_counter() - start_ts) * 1000)
    summary = preview_data.get("summary", {})
    current_app.logger.info(
        "Bulk EPG preview completed in %sms (channels=%s, with_candidates=%s, without_candidates=%s)",
        elapsed_ms,
        summary.get("channels_considered", 0),
        summary.get("with_candidates", 0),
        summary.get("without_candidates", 0),
    )

    user = getattr(request, "_current_user", None)
    if user:
        details = (
            f"channels={summary.get('channels_considered', 0)};"
            f"with_candidates={summary.get('with_candidates', 0)};"
            f"without_candidates={summary.get('without_candidates', 0)};"
            f"duration_ms={elapsed_ms}"
        )
        await audit_stream_event(
            user,
            "bulk_epg_match_preview_requested",
            request.path,
            details=details,
        )

    return jsonify({"success": True, "data": preview_data})


@blueprint.route("/tic-api/channels/bulk/epg-match/candidate-preview", methods=["POST"])
@admin_auth_required
async def api_bulk_epg_match_candidate_preview():
    payload = await request.get_json(silent=True) or {}
    epg_channel_row_id = payload.get("epg_channel_row_id")
    try:
        epg_channel_row_id = int(epg_channel_row_id)
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "Invalid epg channel row id"}), 400

    candidate_preview = await read_epg_match_candidate_preview(epg_channel_row_id=epg_channel_row_id)
    if not candidate_preview:
        return jsonify({"success": False, "message": "EPG candidate not found"}), 404
    return jsonify({"success": True, "data": candidate_preview})


@blueprint.route("/tic-api/channels/bulk/epg-match/apply", methods=["POST"])
@admin_auth_required
async def api_bulk_epg_match_apply():
    payload = await request.get_json(silent=True) or {}
    updates = payload.get("updates", [])
    start_ts = time.perf_counter()
    apply_result = await apply_bulk_epg_matches(updates=updates)
    elapsed_ms = int((time.perf_counter() - start_ts) * 1000)
    summary = apply_result.get("summary", {})

    if summary.get("updated", 0) > 0:
        config = current_app.config["APP_CONFIG"]
        await queue_background_channel_update_tasks(config)

    current_app.logger.info(
        "Bulk EPG apply completed in %sms (updated=%s, skipped=%s, failed=%s)",
        elapsed_ms,
        summary.get("updated", 0),
        summary.get("skipped", 0),
        summary.get("failed", 0),
    )

    user = getattr(request, "_current_user", None)
    if user:
        details = (
            f"updated={summary.get('updated', 0)};"
            f"skipped={summary.get('skipped', 0)};"
            f"failed={summary.get('failed', 0)};"
            f"duration_ms={elapsed_ms}"
        )
        await audit_stream_event(
            user,
            "bulk_epg_match_applied",
            request.path,
            details=details,
        )

    return jsonify({"success": True, "data": apply_result})


@blueprint.route("/tic-api/channels/bulk/cso/apply", methods=["POST"])
@admin_auth_required
async def api_bulk_cso_apply():
    payload = await request.get_json(silent=True) or {}
    channel_ids = payload.get("channel_ids", [])
    cso_enabled = bool(payload.get("cso_enabled", False))
    cso_profile = payload.get("cso_profile")

    apply_result = await apply_bulk_cso_settings(
        channel_ids=channel_ids,
        cso_enabled=cso_enabled,
        cso_profile=cso_profile,
    )

    config = current_app.config["APP_CONFIG"]
    await queue_background_channel_update_tasks(config)

    user = getattr(request, "_current_user", None)
    if user:
        details = f"channels={len(channel_ids)};enabled={int(cso_enabled)};updated={apply_result.get('updated', 0)}"
        await audit_stream_event(
            user,
            "bulk_cso_settings_applied",
            request.path,
            details=details,
        )

    return jsonify({"success": True, "data": apply_result})


@blueprint.route("/tic-api/channels/settings/multiple/delete", methods=["POST"])
@admin_auth_required
async def api_delete_multiple_channels():
    json_data = await request.get_json()
    config = current_app.config["APP_CONFIG"]
    current_app.logger.warning(json_data)

    missing = []
    for channel_id in json_data.get("channels", {}):
        normalized = parse_entity_id(channel_id, "channel")
        deleted = await delete_channel(normalized)
        if not deleted:
            missing.append(normalized)

    # Queue background tasks to update TVHeadend
    await queue_background_channel_update_tasks(config)

    return jsonify({"success": True, "missing": missing})


@blueprint.route("/tic-api/channels/settings/<channel_id>/delete", methods=["DELETE"])
@admin_auth_required
async def api_delete_config_channels(channel_id):
    config = current_app.config["APP_CONFIG"]
    try:
        channel_id = parse_entity_id(channel_id, "channel")
    except ValueError:
        return jsonify({"success": False, "message": "Invalid channel id"}), 400
    deleted = await delete_channel(channel_id)
    if deleted:
        await queue_background_channel_update_tasks(config)
    return jsonify({"success": True, "deleted": bool(deleted)})


@blueprint.route("/tic-api/channels/<channel_id>/logo/<file_placeholder>", methods=["GET"])
async def api_get_channel_logo(channel_id, file_placeholder):
    try:
        channel_id = parse_entity_id(channel_id, "channel")
    except ValueError:
        return jsonify({"success": False, "message": "Invalid channel id"}), 400
    image_base64_string, mime_type = await read_channel_logo(channel_id)
    # Convert to a BytesIO object for sending file
    image_io = io.BytesIO(image_base64_string)
    image_io.seek(0)
    # Return file blob
    return await send_file(image_io, mimetype=mime_type)


@blueprint.route("/tic-api/channels/settings/groups/add", methods=["POST"])
@admin_auth_required
async def api_add_channels_from_groups():
    json_data = await request.get_json()
    groups = json_data.get("groups", [])

    if not groups:
        return jsonify({"success": False, "message": "No groups provided"}), 400

    config = current_app.config["APP_CONFIG"]

    # This function needs to be implemented in the channels module
    # It should add all channels from the specified groups
    added_count = await add_channels_from_groups(config, groups)

    await queue_background_channel_update_tasks(config)

    return jsonify({"success": True, "data": {"added_count": added_count}})
