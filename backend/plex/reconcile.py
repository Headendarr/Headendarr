#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import json
import logging
import os
import time
import uuid
from urllib.parse import quote

import aiohttp
from sqlalchemy import select

from backend.playlists import read_config_all_playlists
from backend.models import Session, User
from backend.plex.client import PlexClient, ensure_list, get_media_container
from backend.plex.runtime import (
    PlexRuntimeServer,
    get_runtime_plex_servers,
    build_plex_settings_for_runtime,
    clean_headendarr_base_url,
)

logger = logging.getLogger("tic.plex.reconcile")
DEFAULT_PLEX_GUIDE_TITLE = "Headendarr XMLTV Guide"


def _status_cache_path(config) -> str:
    return os.path.join(config.config_path, "cache", "plex_reconcile_status.json")


def _build_hdhr_base_url(headendarr_base_url: str, stream_key: str, source_id: str, profile: str | None) -> str:
    base = str(headendarr_base_url or "").rstrip("/")
    if source_id == "combined":
        path = f"/tic-api/hdhr_device/{stream_key}/combined"
    else:
        path = f"/tic-api/hdhr_device/{stream_key}/{source_id}"
    if profile and str(profile).strip():
        path = f"{path}/{str(profile).strip()}"
    return f"{base}{path}"


def _build_xmltv_url(headendarr_base_url: str, stream_key: str, xmltv_path: str = "/tic-api/epg/xmltv.xml") -> str:
    base = str(headendarr_base_url or "").rstrip("/")
    path = str(xmltv_path or "/tic-api/epg/xmltv.xml")
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base}{path}?stream_key={stream_key}"


def _build_lineup_id(xmltv_url: str) -> str:
    return (
        f"lineup://tv.plex.providers.epg.xmltv/{quote(xmltv_url, safe='')}"
        f"#{quote(DEFAULT_PLEX_GUIDE_TITLE, safe='')}"
    )


def _extract_dvrs(payload) -> list[dict]:
    return [item for item in ensure_list(get_media_container(payload).get("Dvr")) if isinstance(item, dict)]


def _flatten_devices(devices_payload, dvrs_payload) -> list[dict]:
    devices = []
    raw_devices = ensure_list(get_media_container(devices_payload).get("Device"))
    for item in raw_devices:
        if isinstance(item, dict):
            devices.append(item)
    for dvr in _extract_dvrs(dvrs_payload):
        dvr_key = str(dvr.get("key") or "")
        for item in ensure_list(dvr.get("Device")):
            if not isinstance(item, dict):
                continue
            if "parentID" not in item and dvr_key:
                next_item = dict(item)
                next_item["parentID"] = dvr_key
                item = next_item
            devices.append(item)
    unique = {}
    for item in devices:
        key = str(item.get("key") or "")
        if key:
            unique[key] = item
    return list(unique.values())


def _resolve_dvr_key(dvrs_payload, stream_key: str, expected_device_id: str, expected_model: str) -> str:
    dvrs = _extract_dvrs(dvrs_payload)
    if not dvrs:
        raise RuntimeError("No Plex DVR entries found")
    if len(dvrs) == 1:
        return str(dvrs[0].get("key") or "")

    for dvr in dvrs:
        lineup = str(dvr.get("lineup") or "")
        if stream_key and stream_key in lineup:
            key = str(dvr.get("key") or "")
            if key:
                return key

    for dvr in dvrs:
        lineup_title = str(dvr.get("lineupTitle") or "").strip().lower()
        if lineup_title and lineup_title == DEFAULT_PLEX_GUIDE_TITLE.strip().lower():
            key = str(dvr.get("key") or "")
            if key:
                return key

    for dvr in dvrs:
        for device in ensure_list(dvr.get("Device")):
            if not isinstance(device, dict):
                continue
            device_id = str(device.get("deviceId") or "").strip()
            model = str(device.get("model") or "").strip()
            if (expected_device_id and device_id == expected_device_id) or (expected_model and model == expected_model):
                key = str(dvr.get("key") or "")
                if key:
                    return key

    return str(dvrs[0].get("key") or "")


def _resolve_lineup_id_from_dvr(dvrs_payload, dvr_key: str, stream_key: str, fallback_lineup_id: str) -> str:
    selected = next((d for d in _extract_dvrs(dvrs_payload) if str(d.get("key") or "") == str(dvr_key)), None)
    if not selected:
        return fallback_lineup_id
    for entry in ensure_list(selected.get("Lineup")):
        if not isinstance(entry, dict):
            continue
        title = str(entry.get("title") or "").strip()
        lineup_id = str(entry.get("id") or "").strip()
        if title and lineup_id and title.lower() == DEFAULT_PLEX_GUIDE_TITLE.strip().lower():
            return lineup_id
    for entry in ensure_list(selected.get("Lineup")):
        if not isinstance(entry, dict):
            continue
        lineup_id = str(entry.get("id") or "").strip()
        if lineup_id and stream_key and stream_key in lineup_id:
            return lineup_id
    top_level = str(selected.get("lineup") or "").strip()
    if top_level:
        return top_level
    return fallback_lineup_id


def _extract_lineup_number_map(lineupchannels_payload) -> dict[str, str]:
    mapping = {}
    channels = ensure_list(get_media_container(lineupchannels_payload).get("Channel"))
    for channel in channels:
        if not isinstance(channel, dict):
            continue
        number = str(channel.get("number") or channel.get("channelNumber") or channel.get("channel") or "").strip()
        identifier = str(
            channel.get("lineupIdentifier")
            or channel.get("id")
            or channel.get("channelIdentifier")
            or channel.get("key")
            or ""
        ).strip()
        if number and identifier:
            mapping[number] = identifier
    return mapping


def _build_channelmap_payload(hdhr_lineup_payload, lineupchannels_payload):
    if not isinstance(hdhr_lineup_payload, list):
        return {}, [], []
    number_map = _extract_lineup_number_map(lineupchannels_payload)
    enabled_ids = []
    unmatched = []
    for channel in hdhr_lineup_payload:
        if not isinstance(channel, dict):
            continue
        guide_number = str(channel.get("GuideNumber") or channel.get("channel_number") or "").strip()
        if not guide_number:
            continue
        matched_id = number_map.get(guide_number) or guide_number
        if not matched_id:
            unmatched.append(guide_number)
            continue
        enabled_ids.append(str(matched_id))
    unique_enabled = []
    seen = set()
    for item in enabled_ids:
        if item in seen:
            continue
        seen.add(item)
        unique_enabled.append(item)
    payload = {"channelsEnabled": ",".join(unique_enabled)}
    for identifier in unique_enabled:
        payload[f"channelMappingByKey[{identifier}]"] = identifier
        payload[f"channelMapping[{identifier}]"] = identifier
    return payload, unique_enabled, unmatched


def _to_plex_bool(value, default: bool) -> str:
    if value is None:
        return "true" if default else "false"
    if isinstance(value, bool):
        return "true" if value else "false"
    return "true" if str(value).strip().lower() in {"1", "true", "yes", "on"} else "false"


def _to_int_string(value, default: int) -> str:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return str(parsed)


def _build_tuner_settings_query(server_settings: dict) -> dict:
    return {
        "transcodeDuringRecord": _to_int_string(server_settings.get("tuner_transcode_during_record"), 0),
        "hardwareVideoEncoders": _to_plex_bool(server_settings.get("tuner_hardware_video_encoders"), True),
        "transcodeQuality": _to_int_string(server_settings.get("tuner_transcode_quality"), 99),
    }


def _build_dvr_settings_query(server_settings: dict, app_dvr_settings: dict) -> dict:
    start_offset_minutes = 0
    end_offset_minutes = 0
    if isinstance(app_dvr_settings, dict):
        try:
            start_offset_minutes = int(app_dvr_settings.get("pre_padding_mins", 0) or 0)
        except (TypeError, ValueError):
            start_offset_minutes = 0
        try:
            end_offset_minutes = int(app_dvr_settings.get("post_padding_mins", 0) or 0)
        except (TypeError, ValueError):
            end_offset_minutes = 0
    return {
        "minVideoQuality": _to_int_string(server_settings.get("dvr_min_video_quality"), 0),
        "replaceLowerQuality": _to_plex_bool(server_settings.get("dvr_replace_lower_quality"), False),
        "recordPartials": _to_plex_bool(server_settings.get("dvr_record_partials"), True),
        "startOffsetMinutes": _to_int_string(start_offset_minutes, 0),
        "endOffsetMinutes": _to_int_string(end_offset_minutes, 0),
        "useUmp": _to_plex_bool(server_settings.get("dvr_use_ump"), False),
        "postprocessingScript": str(server_settings.get("dvr_postprocessing_script") or "").strip(),
        "comskipMethod": _to_int_string(server_settings.get("dvr_comskip_method"), 0),
        "ButlerTaskRefreshEpgGuides": _to_plex_bool(server_settings.get("dvr_refresh_guides_task"), True),
        "mediaProviderEpgXmltvGuideRefreshStartTime": _to_int_string(server_settings.get("dvr_guide_refresh_time"), 2),
        "xmltvCustomRefreshInHours": _to_int_string(server_settings.get("dvr_xmltv_refresh_hours"), 24),
        "kidsCategories": str(server_settings.get("dvr_kids_categories") or "kids").strip() or "kids",
        "newsCategories": str(server_settings.get("dvr_news_categories") or "news").strip() or "news",
        "sportsCategories": str(server_settings.get("dvr_sports_categories") or "sports").strip() or "sports",
    }


def _find_target_device(
    devices: list[dict],
    expected_device_id: str,
    expected_model: str,
    expected_uri: str,
):
    for device in devices:
        device_id = str(device.get("deviceId") or "").strip()
        model = str(device.get("model") or "").strip()
        uri = str(device.get("uri") or "").strip()
        if expected_device_id and device_id == expected_device_id:
            return device
        if expected_model and model == expected_model:
            return device
        if expected_uri and uri == expected_uri:
            return device
    return None


def _server_settings_lookup(plex_settings: dict, server_id: str) -> dict:
    for item in plex_settings.get("servers", []):
        if not isinstance(item, dict):
            continue
        if str(item.get("server_id") or "") == str(server_id):
            return item
    return {}


async def _http_json_get(url: str, timeout_seconds: int):
    timeout = aiohttp.ClientTimeout(total=max(1, int(timeout_seconds)))
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as response:
            text = await response.text()
            try:
                payload = json.loads(text) if text else {}
            except json.JSONDecodeError:
                payload = {"_raw": text}
            return int(response.status), payload


async def _apply_tuner_for_source(
    client: PlexClient,
    source_id: str,
    source_name: str | None,
    server_settings: dict,
    managed_prefix: str,
    headendarr_base_url: str,
    stream_key: str,
    timeout_seconds: int,
    app_dvr_settings: dict,
) -> dict:
    profile = str(server_settings.get("default_stream_profile") or "aac-mpegts").strip()
    dvr_country = str(client.server.dvr_country).strip().lower() or client.server.dvr_country
    dvr_language = str(client.server.dvr_language).strip().lower() or client.server.dvr_language
    if source_id == "combined":
        device_title = "Headendarr: Combined"
    else:
        source_label = str(source_name or source_id).strip()
        device_title = f"Headendarr: {source_label}" if source_label else f"Headendarr: {source_id}"

    hdhr_base_url = _build_hdhr_base_url(headendarr_base_url, stream_key, source_id, profile)
    discover_url = f"{hdhr_base_url}/discover.json"
    lineup_url = f"{hdhr_base_url}/lineup.json"
    xmltv_url = _build_xmltv_url(headendarr_base_url, stream_key)
    fallback_lineup_id = _build_lineup_id(xmltv_url)

    discover_status, discover_payload = await _http_json_get(discover_url, timeout_seconds)
    if not (200 <= discover_status < 300 and isinstance(discover_payload, dict)):
        return {
            "source_id": source_id,
            "status": "failed",
            "error": f"discover.json returned {discover_status}",
        }

    hdhr_lineup_status, hdhr_lineup_payload = await _http_json_get(lineup_url, timeout_seconds)
    if not (200 <= hdhr_lineup_status < 300 and isinstance(hdhr_lineup_payload, list)):
        return {
            "source_id": source_id,
            "status": "failed",
            "error": f"lineup.json returned {hdhr_lineup_status}",
        }

    devices_response = await client.get_devices()
    dvrs_response = await client.get_dvrs()
    if not (200 <= devices_response.status < 300 and 200 <= dvrs_response.status < 300):
        return {
            "source_id": source_id,
            "status": "failed",
            "error": "failed to fetch Plex devices or DVRs",
        }

    all_devices = _flatten_devices(devices_response.payload, dvrs_response.payload)
    expected_device_id = str(discover_payload.get("DeviceID") or "").strip()
    expected_model = str(discover_payload.get("FriendlyName") or "").strip()
    if expected_model and managed_prefix and not expected_model.startswith(managed_prefix):
        expected_model = f"{managed_prefix}{source_id}"
    expected_uri = hdhr_base_url
    target_device = _find_target_device(all_devices, expected_device_id, expected_model, expected_uri)
    recreated_due_to_uri_change = False

    if target_device is not None:
        current_uri = str(target_device.get("uri") or "").strip()
        if current_uri and current_uri != expected_uri:
            stale_device_key = str(target_device.get("key") or "").strip()
            stale_dvr_key = str(target_device.get("parentID") or "").strip()
            if not stale_dvr_key and stale_device_key:
                for dvr in _extract_dvrs(dvrs_response.payload):
                    dvr_key = str(dvr.get("key") or "").strip()
                    for dvr_device in ensure_list(dvr.get("Device")):
                        if not isinstance(dvr_device, dict):
                            continue
                        if str(dvr_device.get("key") or "").strip() == stale_device_key:
                            stale_dvr_key = dvr_key
                            break
                    if stale_dvr_key:
                        break
            if stale_dvr_key and stale_device_key:
                delete_response = await client.delete_dvr_device(stale_dvr_key, stale_device_key)
                if not (200 <= delete_response.status < 300 or delete_response.status == 404):
                    return {
                        "source_id": source_id,
                        "status": "failed",
                        "error": f"failed to remove stale tuner after profile change ({delete_response.status})",
                    }
            target_device = None
            recreated_due_to_uri_change = True

    if target_device is None and hdhr_lineup_payload:
        await client.try_create_device(hdhr_base_url, discover_payload)
        devices_response = await client.get_devices()
        dvrs_response = await client.get_dvrs()
        all_devices = _flatten_devices(devices_response.payload, dvrs_response.payload)
        target_device = _find_target_device(all_devices, expected_device_id, expected_model, expected_uri)

    if target_device is None:
        if not hdhr_lineup_payload:
            return {
                "source_id": source_id,
                "status": "skipped",
                "detail": "empty_lineup_no_device",
                "mapped_channels": 0,
            }
        return {
            "source_id": source_id,
            "status": "failed",
            "error": "unable to find or create matching Plex tuner device",
        }

    device_key = str(target_device.get("key") or "").strip()
    device_uuid = str(target_device.get("uuid") or "").strip()
    if not device_key:
        return {
            "source_id": source_id,
            "status": "failed",
            "error": "matched Plex device had no key",
        }

    dvrs_payload = dvrs_response.payload
    if not _extract_dvrs(dvrs_payload):
        if not device_uuid:
            return {
                "source_id": source_id,
                "status": "failed",
                "error": "cannot create DVR: device uuid missing",
            }
        create_dvr_response = await client.create_dvr(
            device_uuid=device_uuid,
            lineup_id=fallback_lineup_id,
            guide_title=DEFAULT_PLEX_GUIDE_TITLE,
            country=dvr_country,
            language=dvr_language,
        )
        if not (200 <= create_dvr_response.status < 300):
            return {
                "source_id": source_id,
                "status": "failed",
                "error": f"failed to create DVR: {create_dvr_response.status}",
            }
        dvrs_response = await client.get_dvrs()
        dvrs_payload = dvrs_response.payload

    resolved_dvr_key = _resolve_dvr_key(
        dvrs_payload=dvrs_payload,
        stream_key=stream_key,
        expected_device_id=expected_device_id,
        expected_model=expected_model,
    )
    if not resolved_dvr_key:
        return {
            "source_id": source_id,
            "status": "failed",
            "error": "failed to resolve Plex DVR key",
        }

    if not hdhr_lineup_payload:
        delete_response = await client.delete_dvr_device(resolved_dvr_key, device_key)
        if not (200 <= delete_response.status < 300 or delete_response.status == 404):
            return {
                "source_id": source_id,
                "status": "failed",
                "error": f"failed to delete empty tuner from DVR: {delete_response.status}",
            }
        return {
            "source_id": source_id,
            "status": "deleted",
            "device_key": device_key,
            "dvr_key": resolved_dvr_key,
            "mapped_channels": 0,
        }

    put_device_prefs_response = await client.put_device_prefs(device_key, _build_tuner_settings_query(server_settings))
    if not (200 <= put_device_prefs_response.status < 300):
        return {
            "source_id": source_id,
            "status": "failed",
            "error": f"PUT /media/grabbers/devices/{device_key}/prefs failed ({put_device_prefs_response.status})",
        }

    put_device_response = await client.put_device(device_key, device_title, enabled=1)
    if not (200 <= put_device_response.status < 300):
        return {
            "source_id": source_id,
            "status": "failed",
            "error": f"PUT /media/grabbers/devices/{device_key} failed ({put_device_response.status})",
        }

    attach_response = await client.put_dvr_device(resolved_dvr_key, device_key)
    if not (200 <= attach_response.status < 300):
        return {
            "source_id": source_id,
            "status": "failed",
            "error": f"PUT /livetv/dvrs/{resolved_dvr_key}/devices/{device_key} failed ({attach_response.status})",
        }

    dvr_settings_response = await client.put_dvr_prefs(
        resolved_dvr_key,
        _build_dvr_settings_query(server_settings, app_dvr_settings),
    )
    if not (200 <= dvr_settings_response.status < 300):
        logger.warning(
            "Failed to apply DVR settings for server=%s dvr=%s status=%s",
            client.server.server_id,
            resolved_dvr_key,
            dvr_settings_response.status,
        )

    lineup_id = _resolve_lineup_id_from_dvr(
        dvrs_payload=dvrs_payload,
        dvr_key=resolved_dvr_key,
        stream_key=stream_key,
        fallback_lineup_id=fallback_lineup_id,
    )
    lineupchannels_response = await client.get_lineupchannels(lineup_id)
    if not (200 <= lineupchannels_response.status < 300):
        return {
            "source_id": source_id,
            "status": "failed",
            "error": f"GET /livetv/epg/lineupchannels failed ({lineupchannels_response.status})",
        }

    channelmap_payload, mapped_ids, unmatched = _build_channelmap_payload(
        hdhr_lineup_payload, lineupchannels_response.payload
    )
    if not mapped_ids:
        return {
            "source_id": source_id,
            "status": "failed",
            "error": "no channels matched for Plex channelmap payload",
            "unmatched_channels": unmatched,
        }

    channelmap_response = await client.put_channelmap(device_key, channelmap_payload)
    if not (200 <= channelmap_response.status < 300):
        return {
            "source_id": source_id,
            "status": "failed",
            "error": f"PUT /media/grabbers/devices/{device_key}/channelmap failed ({channelmap_response.status})",
        }

    return {
        "source_id": source_id,
        "status": "updated",
        "device_key": device_key,
        "dvr_key": resolved_dvr_key,
        "mapped_channels": len(mapped_ids),
        "unmatched_channels": len(unmatched),
        "recreated_due_to_uri_change": recreated_due_to_uri_change,
        "device_title": device_title,
    }


async def _delete_stale_managed_tuners(
    client: PlexClient,
    managed_prefix: str,
    stream_key: str,
    desired_models: set[str],
) -> list[dict]:
    dvrs_response = await client.get_dvrs()
    if not (200 <= dvrs_response.status < 300):
        return [{"status": "failed", "error": f"failed to refresh DVRs ({dvrs_response.status})"}]
    deletions = []
    for dvr in _extract_dvrs(dvrs_response.payload):
        dvr_key = str(dvr.get("key") or "")
        for device in ensure_list(dvr.get("Device")):
            if not isinstance(device, dict):
                continue
            model = str(device.get("model") or "").strip()
            uri = str(device.get("uri") or "").strip()
            if not model.startswith(managed_prefix):
                continue
            if stream_key and f"/hdhr_device/{stream_key}/" not in uri:
                continue
            if model in desired_models:
                continue
            device_key = str(device.get("key") or "")
            if not dvr_key or not device_key:
                continue
            response = await client.delete_dvr_device(dvr_key, device_key)
            deletions.append(
                {
                    "status": "deleted" if 200 <= response.status < 300 or response.status == 404 else "failed",
                    "dvr_key": dvr_key,
                    "device_key": device_key,
                    "model": model,
                    "http_status": response.status,
                }
            )
    return deletions


async def reconcile_server(
    config,
    runtime_server: PlexRuntimeServer,
    plex_settings: dict,
    stream_key_by_user_id: dict[int, str],
    source_ids: list[int],
    source_name_map: dict[str, str],
    app_dvr_settings: dict,
) -> dict:
    started_at = time.monotonic()
    settings = _server_settings_lookup(plex_settings, runtime_server.server_id)
    if not settings:
        return {
            "server_id": runtime_server.server_id,
            "name": runtime_server.name,
            "status": "skipped",
            "reason": "server_not_configured_in_settings",
            "duration_ms": int((time.monotonic() - started_at) * 1000),
        }
    if not settings.get("enabled", False):
        return {
            "server_id": runtime_server.server_id,
            "name": runtime_server.name,
            "status": "skipped",
            "reason": "server_disabled",
            "duration_ms": int((time.monotonic() - started_at) * 1000),
        }
    client = PlexClient(runtime_server, client_identifier=f"headendarr-plex-{uuid.uuid4().hex[:12]}")
    machine_id, friendly_name = await client.get_identity()
    if not friendly_name:
        return {
            "server_id": runtime_server.server_id,
            "name": runtime_server.name,
            "status": "failed",
            "reason": "unable_to_read_server_identity",
            "duration_ms": int((time.monotonic() - started_at) * 1000),
        }
    actual_server_name = friendly_name.strip().lower()
    expected_server_name = runtime_server.name.strip().lower()
    if actual_server_name != expected_server_name:
        return {
            "server_id": runtime_server.server_id,
            "name": runtime_server.name,
            "status": "failed",
            "reason": f"server_name_mismatch expected={runtime_server.name} actual={friendly_name}",
            "duration_ms": int((time.monotonic() - started_at) * 1000),
        }

    mode = str(settings.get("default_tuner_mode") or "per_source").strip().lower()
    if mode not in {"per_source", "combined"}:
        mode = "per_source"
    managed_prefix = "Headendarr-"
    headendarr_base_url = clean_headendarr_base_url(settings.get("headendarr_base_url") or "")
    if not headendarr_base_url:
        return {
            "server_id": runtime_server.server_id,
            "name": runtime_server.name,
            "status": "failed",
            "reason": "missing_headendarr_base_url",
            "duration_ms": int((time.monotonic() - started_at) * 1000),
        }
    stream_user_id = settings.get("stream_user_id")
    try:
        stream_user_id = int(stream_user_id) if stream_user_id is not None else None
    except (TypeError, ValueError):
        stream_user_id = None
    if not stream_user_id:
        return {
            "server_id": runtime_server.server_id,
            "name": runtime_server.name,
            "status": "failed",
            "reason": "missing_stream_user_id",
            "duration_ms": int((time.monotonic() - started_at) * 1000),
        }
    stream_key = str(stream_key_by_user_id.get(stream_user_id) or "").strip()
    if not stream_key:
        return {
            "server_id": runtime_server.server_id,
            "name": runtime_server.name,
            "status": "failed",
            "reason": f"missing_stream_key_for_user_id={stream_user_id}",
            "duration_ms": int((time.monotonic() - started_at) * 1000),
        }

    desired_sources = ["combined"] if mode == "combined" else [str(item) for item in source_ids]
    desired_models = set()
    if mode == "combined":
        desired_models.add(f"{managed_prefix}Combined")
    else:
        desired_models.update(f"{managed_prefix}{source_id}" for source_id in desired_sources)

    per_source_results = []
    for source_id in desired_sources:
        source_name = source_name_map.get(str(source_id))
        result = await _apply_tuner_for_source(
            client=client,
            source_id=source_id,
            source_name=source_name,
            server_settings=settings,
            managed_prefix=managed_prefix,
            headendarr_base_url=headendarr_base_url,
            stream_key=stream_key,
            timeout_seconds=runtime_server.timeout_seconds,
            app_dvr_settings=app_dvr_settings,
        )
        per_source_results.append(result)

    stale_deletions = await _delete_stale_managed_tuners(
        client=client,
        managed_prefix=managed_prefix,
        stream_key=stream_key,
        desired_models=desired_models,
    )

    failed_items = [item for item in per_source_results if item.get("status") == "failed"]
    failed_items.extend([item for item in stale_deletions if item.get("status") == "failed"])
    status = "success" if not failed_items else "partial"
    duration_ms = int((time.monotonic() - started_at) * 1000)
    return {
        "server_id": runtime_server.server_id,
        "name": runtime_server.name,
        "friendly_name": friendly_name,
        "machine_identifier": machine_id,
        "status": status,
        "mode": mode,
        "per_source_results": per_source_results,
        "stale_deletions": stale_deletions,
        "duration_ms": duration_ms,
    }


async def _write_status_cache(config, payload: dict) -> None:
    path = _status_cache_path(config)
    directory = os.path.dirname(path)
    await asyncio.to_thread(os.makedirs, directory, exist_ok=True)
    text = json.dumps(payload, indent=2)
    await asyncio.to_thread(_write_text_file, path, text)


def _write_text_file(path: str, content: str) -> None:
    with open(path, "w") as file:
        file.write(content)


async def read_last_reconcile_status(config) -> dict:
    path = _status_cache_path(config)
    if not os.path.exists(path):
        return {"last_run_at": None, "results": []}
    try:
        text = await asyncio.to_thread(_read_text_file, path)
        payload = json.loads(text)
        if isinstance(payload, dict):
            return payload
    except Exception:
        logger.exception("Failed to read Plex reconcile status cache")
    return {"last_run_at": None, "results": []}


def _read_text_file(path: str) -> str:
    with open(path, "r") as file:
        return file.read()


async def reconcile_plex_servers(config, server_ids: list[str] | None = None) -> dict:
    settings = config.read_settings()
    runtime_servers = get_runtime_plex_servers()
    plex_settings = build_plex_settings_for_runtime(runtime_servers, settings.get("settings", {}).get("plex"))
    app_dvr_settings = settings.get("settings", {}).get("dvr", {})

    started_at = int(time.time())
    if not runtime_servers:
        result = {
            "last_run_at": started_at,
            "results": [],
            "summary": {"status": "skipped", "reason": "no_runtime_servers"},
        }
        await _write_status_cache(config, result)
        return result

    stream_key_by_user_id: dict[int, str] = {}
    async with Session() as session:
        rows = await session.execute(select(User.id, User.streaming_key))
        for user_id, streaming_key in rows.all():
            try:
                key = int(user_id)
            except (TypeError, ValueError):
                continue
            stream_key_by_user_id[key] = str(streaming_key or "").strip()

    playlist_configs = await read_config_all_playlists(config)
    enabled_source_ids = sorted(
        {
            int(item.get("id"))
            for item in playlist_configs
            if isinstance(item, dict) and item.get("enabled") and str(item.get("id") or "").isdigit()
        }
    )
    source_name_map = {
        str(int(item.get("id"))): str(item.get("name") or "").strip()
        for item in playlist_configs
        if isinstance(item, dict) and str(item.get("id") or "").isdigit()
    }

    selected_server_ids = {str(item) for item in (server_ids or []) if str(item).strip()}
    results = []
    for runtime_server in runtime_servers:
        if selected_server_ids and runtime_server.server_id not in selected_server_ids:
            continue
        try:
            result = await reconcile_server(
                config=config,
                runtime_server=runtime_server,
                plex_settings=plex_settings,
                stream_key_by_user_id=stream_key_by_user_id,
                source_ids=enabled_source_ids,
                source_name_map=source_name_map,
                app_dvr_settings=app_dvr_settings,
            )
        except Exception as exc:
            logger.exception("Plex reconcile failed for server_id=%s", runtime_server.server_id)
            result = {
                "server_id": runtime_server.server_id,
                "name": runtime_server.name,
                "status": "failed",
                "reason": str(exc),
            }
        results.append(result)

    failed_count = len([item for item in results if item.get("status") == "failed"])
    partial_count = len([item for item in results if item.get("status") == "partial"])
    status = "success"
    if failed_count:
        status = "failed"
    elif partial_count:
        status = "partial"
    payload = {
        "last_run_at": started_at,
        "results": results,
        "summary": {
            "status": status,
            "server_count": len(results),
            "failed_count": failed_count,
            "partial_count": partial_count,
        },
    }
    await _write_status_cache(config, payload)
    return payload
