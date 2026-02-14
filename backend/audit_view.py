#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from __future__ import annotations

from typing import Any


def derive_audit_mode(event_type: str | None, endpoint: str | None) -> str | None:
    event = (event_type or "").strip().lower()
    path = (endpoint or "").strip().lower()
    if event in {"stream_start", "stream_stop", "recording_start", "recording_stop", "hls_stream_connect"}:
        return "session_tracked"
    if event == "playback_start_direct":
        return "start_only"
    if event in {"stream_connect", "stream_disconnect"} and "/tic-hls-proxy/" in path:
        return "session_tracked"
    return None


def build_device_label(user_agent: str | None) -> str:
    ua = (user_agent or "").strip()
    if not ua:
        return "Unknown"
    ua_lc = ua.lower()
    if "tivimate" in ua_lc:
        return "TiviMate"
    if "vlc" in ua_lc or "libvlc" in ua_lc:
        return "VLC"
    if "tvheadend" in ua_lc:
        return "TVHeadend"
    if "firefox" in ua_lc:
        return "Firefox"
    if "edg/" in ua_lc or "edge/" in ua_lc:
        return "Edge"
    if "chrome/" in ua_lc and "edg/" not in ua_lc:
        return "Chrome"
    if "safari/" in ua_lc and "chrome/" not in ua_lc:
        return "Safari"
    if "android" in ua_lc:
        return "Android App"
    if "iphone" in ua_lc or "ipad" in ua_lc or "ios" in ua_lc:
        return "iOS App"
    # Keep unmapped but present user-agent visible for troubleshooting.
    return ua


def build_activity_label(event_type: str | None, endpoint: str | None, details: str | None = None) -> str:
    event = (event_type or "").strip().lower()
    path = (endpoint or "").strip().lower()
    detail = (details or "").strip().lower()

    if event in {"stream_connect", "hls_stream_connect", "stream_start"}:
        return "Playback session started"
    if event in {"stream_disconnect", "stream_stop"}:
        return "Playback session ended"
    if event == "recording_start":
        return "Recording started"
    if event == "recording_stop":
        return "Recording ended"
    if event in {"playlist_m3u", "playlist_m3u8"}:
        return "Playlist requested"
    if event in {"epg_xml", "xc_xmltv"}:
        return "EPG requested"
    if event.startswith("hdhr_"):
        return "HDHomeRun API request"
    if event == "xc_player_api":
        return "XC player API request"
    if event == "xc_panel_api":
        return "XC panel API request"
    if event == "xc_stream":
        return "XC stream requested"
    if event == "xc_get":
        return "XC API request"
    if event == "playback_start_direct":
        return "Playback started (direct source)"
    if event in {"settings_update", "settings_changed"}:
        return "Settings changed"

    if "playlist" in path:
        return "Playlist requested"
    if "xmltv" in path or "epg" in path:
        return "EPG requested"
    if "dvr" in path and "stream" in path:
        return "Recording playback started"
    if "hls" in path:
        return "HLS stream request"
    if "stream" in detail:
        return "Stream activity"
    return "Other activity"


def serialize_audit_row(row: dict[str, Any]) -> dict[str, Any]:
    created_at = row.get("created_at")
    user_agent = row.get("user_agent")
    event_type = row.get("event_type")
    endpoint = row.get("endpoint")
    details = row.get("details")
    return {
        "id": row.get("id"),
        "created_at": created_at.isoformat() if created_at else None,
        "event_type": event_type,
        "endpoint": endpoint,
        "details": details,
        "ip_address": row.get("ip_address"),
        "user_agent": user_agent,
        "user_id": row.get("user_id"),
        "username": row.get("username"),
        "activity_label": build_activity_label(event_type, endpoint, details),
        "device_label": build_device_label(user_agent),
        "audit_mode": derive_audit_mode(event_type, endpoint),
    }
