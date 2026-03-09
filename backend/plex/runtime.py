#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import hashlib
import json
import os
import re
from dataclasses import dataclass
from urllib.parse import urlsplit

from backend.utils import convert_to_bool, convert_to_int


@dataclass
class PlexRuntimeServer:
    server_id: str
    name: str
    base_url: str
    token: str
    verify_tls: bool
    timeout_seconds: int
    dvr_country: str
    dvr_language: str


def clean_plex_base_url(raw_value: str) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    value = value.rstrip("/")
    parsed = urlsplit(value)
    if not parsed.scheme or not parsed.netloc:
        return ""
    if parsed.scheme not in {"http", "https"}:
        return ""
    if parsed.path and parsed.path not in ("", "/"):
        return ""
    return f"{parsed.scheme}://{parsed.netloc}"


def clean_headendarr_base_url(raw_value: str) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    parsed = urlsplit(value)
    if not parsed.scheme or not parsed.netloc:
        return ""
    if parsed.scheme not in {"http", "https"}:
        return ""
    if parsed.query or parsed.fragment:
        return ""
    cleaned_path = str(parsed.path or "").rstrip("/")
    if cleaned_path and cleaned_path != "/":
        return f"{parsed.scheme}://{parsed.netloc}{cleaned_path}"
    return f"{parsed.scheme}://{parsed.netloc}"


def _slugify(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return text or "plex"


def _stable_server_id(name: str, base_url: str, index: int) -> str:
    digest = hashlib.sha1(f"{name}|{base_url}|{index}".encode("utf-8")).hexdigest()[:8]
    return f"{_slugify(name)}-{digest}"


def _build_runtime_server(entry: dict, index: int) -> PlexRuntimeServer | None:
    name = str(entry.get("name") or "").strip()
    token = str(entry.get("token") or "").strip()
    base_url = clean_plex_base_url(entry.get("base_url") or "")
    if not name or not token or not base_url:
        return None
    verify_tls = convert_to_bool(entry.get("verify_tls"), default=True)
    timeout_seconds = max(1, convert_to_int(entry.get("timeout_seconds"), 20))
    dvr_country = str(entry.get("dvr_country") or "nzl").strip().lower() or "nzl"
    dvr_language = str(entry.get("dvr_language") or "eng").strip().lower() or "eng"
    server_id = str(entry.get("server_id") or "").strip() or _stable_server_id(name, base_url, index)
    return PlexRuntimeServer(
        server_id=server_id,
        name=name,
        base_url=base_url,
        token=token,
        verify_tls=verify_tls,
        timeout_seconds=timeout_seconds,
        dvr_country=dvr_country,
        dvr_language=dvr_language,
    )


def _parse_plex_servers_json(raw_value: str) -> list[PlexRuntimeServer]:
    text = str(raw_value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    servers: list[PlexRuntimeServer] = []
    for index, entry in enumerate(parsed):
        if not isinstance(entry, dict):
            continue
        server = _build_runtime_server(entry, index)
        if server is None:
            continue
        servers.append(server)
    return servers


def get_runtime_plex_servers() -> list[PlexRuntimeServer]:
    return _parse_plex_servers_json(os.environ.get("PLEX_SERVERS_JSON", ""))


def plex_runtime_summary() -> dict:
    servers = get_runtime_plex_servers()
    return {
        "server_count": len(servers),
        "available_server_ids": [server.server_id for server in servers],
        "server_names": [server.name for server in servers],
        "servers": [
            {
                "server_id": server.server_id,
                "name": server.name,
                "base_url": server.base_url,
                "verify_tls": server.verify_tls,
                "timeout_seconds": server.timeout_seconds,
                "token_present": bool(server.token),
            }
            for server in servers
        ],
    }


def build_plex_settings_for_runtime(runtime_servers: list[PlexRuntimeServer], plex_settings: dict | None) -> dict:
    source = plex_settings if isinstance(plex_settings, dict) else {}
    legacy_default_mode = str(source.get("default_tuner_mode") or "per_source").strip().lower() or "per_source"
    legacy_default_profile = str(source.get("default_stream_profile") or "aac-mpegts").strip() or "aac-mpegts"
    legacy_enabled = bool(source.get("enabled", False))
    existing_server_settings = source.get("servers")
    existing_map = {}
    if isinstance(existing_server_settings, list):
        for item in existing_server_settings:
            if not isinstance(item, dict):
                continue
            key = str(item.get("server_id") or "").strip()
            if key:
                existing_map[key] = item

    servers = []
    for server in runtime_servers:
        existing = existing_map.get(server.server_id) or {}
        default_tuner_mode = (
            str(existing.get("default_tuner_mode") or existing.get("tuner_mode") or legacy_default_mode).strip().lower()
        )
        default_stream_profile = (
            str(
                existing.get("default_stream_profile") or existing.get("stream_profile") or legacy_default_profile
            ).strip()
            or legacy_default_profile
        )
        mode = default_tuner_mode
        if mode not in {"per_source", "combined"}:
            mode = "per_source"
        stream_user_id = existing.get("stream_user_id")
        try:
            stream_user_id = int(stream_user_id) if stream_user_id is not None else None
        except (TypeError, ValueError):
            stream_user_id = None
        if stream_user_id is not None and stream_user_id <= 0:
            stream_user_id = None
        servers.append(
            {
                "server_id": server.server_id,
                "name": server.name,
                "enabled": bool(existing.get("enabled", legacy_enabled)),
                "headendarr_base_url": clean_headendarr_base_url(existing.get("headendarr_base_url") or ""),
                "stream_user_id": stream_user_id,
                "default_stream_profile": default_stream_profile,
                "default_tuner_mode": mode,
                "dvr_min_video_quality": convert_to_int(existing.get("dvr_min_video_quality"), 0),
                "dvr_replace_lower_quality": convert_to_bool(existing.get("dvr_replace_lower_quality"), default=False),
                "dvr_record_partials": convert_to_bool(existing.get("dvr_record_partials"), default=True),
                "dvr_use_ump": convert_to_bool(existing.get("dvr_use_ump"), default=False),
                "dvr_postprocessing_script": str(existing.get("dvr_postprocessing_script") or "").strip(),
                "dvr_comskip_method": convert_to_int(existing.get("dvr_comskip_method"), 0),
                "dvr_refresh_guides_task": convert_to_bool(existing.get("dvr_refresh_guides_task"), default=True),
                "dvr_guide_refresh_time": convert_to_int(existing.get("dvr_guide_refresh_time"), 2),
                "dvr_xmltv_refresh_hours": convert_to_int(existing.get("dvr_xmltv_refresh_hours"), 24),
                "dvr_kids_categories": str(existing.get("dvr_kids_categories") or "kids").strip() or "kids",
                "dvr_news_categories": str(existing.get("dvr_news_categories") or "news").strip() or "news",
                "dvr_sports_categories": str(existing.get("dvr_sports_categories") or "sports").strip() or "sports",
                "tuner_transcode_during_record": convert_to_int(existing.get("tuner_transcode_during_record"), 0),
                "tuner_hardware_video_encoders": convert_to_bool(
                    existing.get("tuner_hardware_video_encoders"), default=True
                ),
                "tuner_transcode_quality": convert_to_int(existing.get("tuner_transcode_quality"), 99),
            }
        )

    return {
        "servers": servers,
    }
