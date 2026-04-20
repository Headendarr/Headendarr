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
from backend.config import get_runtime_plex_servers
from backend.models import Session, User
from backend.plex.client import PlexClient, ensure_list, get_media_container
from backend.plex.runtime import (
    PlexRuntimeServer,
    build_plex_settings_for_runtime,
    clean_headendarr_base_url,
    parse_plex_servers_json,
)

logger = logging.getLogger("tic.plex.reconcile")
DEFAULT_PLEX_GUIDE_TITLE = "Headendarr XMLTV Guide"


def _status_cache_path(config) -> str:
    return os.path.join(config.config_path, "cache", "plex_reconcile_status.json")


def _plex_client_identifier_path(config) -> str:
    return os.path.join(config.config_path, "cache", "plex_client_id")


def _read_plex_client_identifier(path: str) -> str | None:
    try:
        with open(path, "r") as infile:
            value = infile.read().strip()
    except FileNotFoundError:
        return None
    except OSError:
        logger.exception("Failed to read Plex client identifier from %s", path)
        return None
    if value:
        return value
    logger.warning("Ignoring empty Plex client identifier file at %s", path)
    return None


def get_plex_client_identifier(config) -> str:
    path = _plex_client_identifier_path(config)
    existing = _read_plex_client_identifier(path)
    if existing:
        return existing

    os.makedirs(os.path.dirname(path), exist_ok=True)
    generated = f"headendarr-plex-{uuid.uuid4().hex[:12]}"
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        existing = _read_plex_client_identifier(path)
        if existing:
            return existing
        try:
            with open(path, "w") as outfile:
                outfile.write(f"{generated}\n")
        except OSError:
            logger.exception("Failed to replace invalid Plex client identifier at %s", path)
        return generated
    except OSError:
        logger.exception("Failed to create Plex client identifier at %s", path)
        return generated

    try:
        with os.fdopen(fd, "w") as outfile:
            outfile.write(f"{generated}\n")
    except OSError:
        logger.exception("Failed to write Plex client identifier to %s", path)
    return generated


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
        f"lineup://tv.plex.providers.epg.xmltv/{quote(xmltv_url, safe='')}#{quote(DEFAULT_PLEX_GUIDE_TITLE, safe='')}"
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


def _extract_setting_map(setting_items) -> dict[str, str]:
    settings = {}
    for item in ensure_list(setting_items):
        if not isinstance(item, dict):
            continue
        setting_id = str(item.get("id") or "").strip()
        if not setting_id:
            continue
        settings[setting_id] = str(item.get("value") or "").strip()
    return settings


def _extract_channel_mapping_ids(device_payload: dict) -> list[str]:
    mapped_ids = []
    for item in ensure_list(device_payload.get("ChannelMapping")):
        if not isinstance(item, dict):
            continue
        enabled = str(item.get("enabled") or "1").strip()
        if enabled in {"0", "false", "False"}:
            continue
        identifier = str(item.get("lineupIdentifier") or item.get("channelKey") or "").strip()
        if identifier:
            mapped_ids.append(identifier)
    unique_ids = []
    seen = set()
    for item in mapped_ids:
        if item in seen:
            continue
        seen.add(item)
        unique_ids.append(item)
    return sorted(unique_ids)


def _extract_dvr_setting_map(dvrs_payload, dvr_key: str) -> dict[str, str]:
    for dvr in _extract_dvrs(dvrs_payload):
        if str(dvr.get("key") or "").strip() != str(dvr_key or "").strip():
            continue
        return _extract_setting_map(dvr.get("Setting"))
    return {}


def _count_dvr_devices(dvrs_payload, dvr_key: str) -> int:
    for dvr in _extract_dvrs(dvrs_payload):
        if str(dvr.get("key") or "").strip() != str(dvr_key or "").strip():
            continue
        return sum(1 for item in ensure_list(dvr.get("Device")) if isinstance(item, dict))
    return 0


def _can_remove_dvr_device(dvrs_payload, dvr_key: str, device_key: str) -> bool:
    device_count = _count_dvr_devices(dvrs_payload, dvr_key)
    if device_count <= 1:
        # Plex removes the DVR when its last tuner is removed, which also clears scheduled recordings.
        # Callers must create and attach a replacement first, or leave the last tuner in place.
        return False
    for dvr in _extract_dvrs(dvrs_payload):
        if str(dvr.get("key") or "").strip() != str(dvr_key or "").strip():
            continue
        return any(
            isinstance(item, dict) and str(item.get("key") or "").strip() == str(device_key or "").strip()
            for item in ensure_list(dvr.get("Device"))
        )
    return False


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


def _find_device_by_uri(devices: list[dict], expected_uri: str) -> dict | None:
    for device in devices:
        if str(device.get("uri") or "").strip() == str(expected_uri or "").strip():
            return device
    return None


def _find_healthy_replacement_device(
    devices: list[dict],
    expected_device_id: str,
    expected_model: str,
    expected_uri: str,
    old_device_key: str,
) -> dict | None:
    for device in devices:
        device_key = str(device.get("key") or "").strip()
        if old_device_key and device_key == old_device_key:
            continue
        if str(device.get("status") or "").strip().lower() == "dead":
            continue
        device_id = str(device.get("deviceId") or "").strip()
        model = str(device.get("model") or "").strip()
        uri = str(device.get("uri") or "").strip()
        if expected_uri and uri == expected_uri:
            return device
        if expected_device_id and device_id == expected_device_id:
            return device
        if expected_model and model == expected_model:
            return device
    return None


def _server_settings_lookup(plex_settings: dict, server_id: str) -> dict:
    for item in plex_settings.get("servers", []):
        if not isinstance(item, dict):
            continue
        if str(item.get("server_id") or "") == str(server_id):
            return item
    return {}


def _http_error_reason(exc: BaseException, timeout_seconds: int | None = None) -> str:
    if isinstance(exc, asyncio.TimeoutError):
        if timeout_seconds is not None:
            return f"request timed out after {max(1, int(timeout_seconds))}s"
        return "request timed out"
    message = str(exc).strip()
    if message:
        return f"{exc.__class__.__name__}: {message}"
    return exc.__class__.__name__


async def _http_json_get(url: str, timeout_seconds: int) -> tuple[int, object]:
    timeout = aiohttp.ClientTimeout(total=max(1, int(timeout_seconds)))
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                text = await response.text()
                try:
                    payload = json.loads(text) if text else {}
                except json.JSONDecodeError:
                    payload = {"_raw": text}
                return int(response.status), payload
    except asyncio.CancelledError:
        raise
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        return 0, {"_error": _http_error_reason(exc, timeout_seconds)}


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
        discover_error = ""
        if isinstance(discover_payload, dict):
            discover_error = str(discover_payload.get("_error") or "").strip()
        return {
            "source_id": source_id,
            "status": "failed",
            "error": discover_error or f"discover.json returned {discover_status}",
        }

    hdhr_lineup_status, hdhr_lineup_payload = await _http_json_get(lineup_url, timeout_seconds)
    if not (200 <= hdhr_lineup_status < 300 and isinstance(hdhr_lineup_payload, list)):
        lineup_error = ""
        if isinstance(hdhr_lineup_payload, dict):
            lineup_error = str(hdhr_lineup_payload.get("_error") or "").strip()
        return {
            "source_id": source_id,
            "status": "failed",
            "error": lineup_error or f"lineup.json returned {hdhr_lineup_status}",
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
    recreated_due_to_dead_device = False
    dead_device_to_remove: tuple[str, str] | None = None
    preserved_dead_device = False
    stale_device = None

    if target_device is not None:
        current_uri = str(target_device.get("uri") or "").strip()
        if current_uri and current_uri != expected_uri:
            stale_device = target_device
            target_device = None
        elif str(target_device.get("status") or "").strip().lower() == "dead" and hdhr_lineup_payload:
            dead_device_key = str(target_device.get("key") or "").strip()
            dead_dvr_key = str(target_device.get("parentID") or "").strip()
            if not dead_dvr_key and dead_device_key:
                for dvr in _extract_dvrs(dvrs_response.payload):
                    dvr_key = str(dvr.get("key") or "").strip()
                    for dvr_device in ensure_list(dvr.get("Device")):
                        if not isinstance(dvr_device, dict):
                            continue
                        if str(dvr_device.get("key") or "").strip() == dead_device_key:
                            dead_dvr_key = dvr_key
                            break
                    if dead_dvr_key:
                        break

            # Plex deletes the DVR, including scheduled recordings, when its last tuner is removed.
            # Always attempt to create a replacement before any delete, and never delete the final tuner.
            await client.try_create_device(hdhr_base_url, discover_payload)
            devices_response = await client.get_devices()
            dvrs_response = await client.get_dvrs()
            all_devices = _flatten_devices(devices_response.payload, dvrs_response.payload)
            replacement_device = _find_healthy_replacement_device(
                all_devices,
                expected_device_id,
                expected_model,
                expected_uri,
                dead_device_key,
            )
            if replacement_device is not None:
                recreated_due_to_dead_device = True
                target_device = replacement_device
                if dead_dvr_key and dead_device_key:
                    dead_device_to_remove = (dead_dvr_key, dead_device_key)
            else:
                return {
                    "source_id": source_id,
                    "status": "preserved",
                    "detail": "kept_dead_last_tuner_to_preserve_dvr",
                    "device_key": dead_device_key,
                    "dvr_key": dead_dvr_key,
                    "mapped_channels": len(hdhr_lineup_payload) if isinstance(hdhr_lineup_payload, list) else 0,
                }

    if target_device is None and hdhr_lineup_payload:
        # NOTE: Create before delete: if Plex accepts a replacement tuner we can preserve the DVR and schedules.
        await client.try_create_device(hdhr_base_url, discover_payload)
        devices_response = await client.get_devices()
        dvrs_response = await client.get_dvrs()
        all_devices = _flatten_devices(devices_response.payload, dvrs_response.payload)
        target_device = _find_device_by_uri(all_devices, expected_uri) or _find_target_device(
            all_devices,
            expected_device_id,
            expected_model,
            expected_uri,
        )

    if target_device is None and stale_device is not None:
        stale_device_key = str(stale_device.get("key") or "").strip()
        stale_dvr_key = str(stale_device.get("parentID") or "").strip()
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
        if (
            stale_dvr_key
            and stale_device_key
            and _can_remove_dvr_device(dvrs_response.payload, stale_dvr_key, stale_device_key)
        ):
            # Safe only after create-before-delete and the last-tuner guard above; removing the final
            # Plex DVR tuner deletes the DVR and clears scheduled recordings.
            delete_response = await client.delete_dvr_device(stale_dvr_key, stale_device_key)
            if not (200 <= delete_response.status < 300 or delete_response.status == 404):
                return {
                    "source_id": source_id,
                    "status": "failed",
                    "error": f"failed to remove stale tuner after profile change ({delete_response.status})",
                }
            recreated_due_to_uri_change = True
            await client.try_create_device(hdhr_base_url, discover_payload)
            devices_response = await client.get_devices()
            dvrs_response = await client.get_dvrs()
            all_devices = _flatten_devices(devices_response.payload, dvrs_response.payload)
            target_device = _find_device_by_uri(all_devices, expected_uri) or _find_target_device(
                all_devices,
                expected_device_id,
                expected_model,
                expected_uri,
            )
        else:
            return {
                "source_id": source_id,
                "status": "preserved",
                "detail": "kept_existing_tuner_to_preserve_dvr",
                "mapped_channels": len(hdhr_lineup_payload) if isinstance(hdhr_lineup_payload, list) else 0,
            }

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
        return {
            "source_id": source_id,
            "status": "preserved",
            "detail": "empty_lineup_kept_to_preserve_dvr",
            "device_key": device_key,
            "dvr_key": resolved_dvr_key,
            "mapped_channels": 0,
        }

    changed = {
        "device_prefs": False,
        "device_info": False,
        "dvr_attach": False,
        "dvr_prefs": False,
        "channelmap": False,
    }

    desired_tuner_settings = _build_tuner_settings_query(server_settings)
    current_tuner_settings = _extract_setting_map(target_device.get("Setting"))
    tuner_settings_changed = any(
        str(current_tuner_settings.get(setting_key) or "") != str(setting_value)
        for setting_key, setting_value in desired_tuner_settings.items()
    )
    if tuner_settings_changed:
        put_device_prefs_response = await client.put_device_prefs(device_key, desired_tuner_settings)
        if not (200 <= put_device_prefs_response.status < 300):
            return {
                "source_id": source_id,
                "status": "failed",
                "error": f"PUT /media/grabbers/devices/{device_key}/prefs failed ({put_device_prefs_response.status})",
            }
        changed["device_prefs"] = True

    current_title = str(target_device.get("title") or "").strip()
    current_state = str(target_device.get("state") or "").strip().lower()
    needs_device_info_update = current_title != device_title or current_state != "enabled"
    if needs_device_info_update:
        put_device_response = await client.put_device(device_key, device_title, enabled=1)
        if not (200 <= put_device_response.status < 300):
            return {
                "source_id": source_id,
                "status": "failed",
                "error": f"PUT /media/grabbers/devices/{device_key} failed ({put_device_response.status})",
            }
        changed["device_info"] = True

    current_parent_id = str(target_device.get("parentID") or "").strip()
    if current_parent_id != str(resolved_dvr_key):
        attach_response = await client.put_dvr_device(resolved_dvr_key, device_key)
        if not (200 <= attach_response.status < 300):
            return {
                "source_id": source_id,
                "status": "failed",
                "error": f"PUT /livetv/dvrs/{resolved_dvr_key}/devices/{device_key} failed ({attach_response.status})",
            }
        changed["dvr_attach"] = True

    if dead_device_to_remove is not None:
        dead_dvr_key, dead_device_key = dead_device_to_remove
        dvrs_response = await client.get_dvrs()
        if not (200 <= dvrs_response.status < 300):
            return {
                "source_id": source_id,
                "status": "failed",
                "error": f"failed to refresh DVRs before removing dead tuner ({dvrs_response.status})",
            }
        dvrs_payload = dvrs_response.payload
        if _can_remove_dvr_device(dvrs_payload, dead_dvr_key, dead_device_key):
            delete_response = await client.delete_dvr_device(dead_dvr_key, dead_device_key)
            if not (200 <= delete_response.status < 300 or delete_response.status == 404):
                return {
                    "source_id": source_id,
                    "status": "failed",
                    "error": f"failed to remove dead tuner device ({delete_response.status})",
                }
        else:
            preserved_dead_device = True

    desired_dvr_settings = _build_dvr_settings_query(server_settings, app_dvr_settings)
    current_dvr_settings = _extract_dvr_setting_map(dvrs_payload, resolved_dvr_key)
    dvr_settings_changed = any(
        str(current_dvr_settings.get(setting_key) or "") != str(setting_value)
        for setting_key, setting_value in desired_dvr_settings.items()
    )
    if dvr_settings_changed:
        dvr_settings_response = await client.put_dvr_prefs(
            resolved_dvr_key,
            desired_dvr_settings,
        )
        if not (200 <= dvr_settings_response.status < 300):
            logger.warning(
                "Failed to apply DVR settings for server=%s dvr=%s status=%s",
                client.server.server_id,
                resolved_dvr_key,
                dvr_settings_response.status,
            )
        else:
            changed["dvr_prefs"] = True

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

    current_mapped_ids = _extract_channel_mapping_ids(target_device)
    desired_mapped_ids = sorted(mapped_ids)
    if current_mapped_ids != desired_mapped_ids:
        channelmap_response = await client.put_channelmap(device_key, channelmap_payload)
        if not (200 <= channelmap_response.status < 300):
            return {
                "source_id": source_id,
                "status": "failed",
                "error": f"PUT /media/grabbers/devices/{device_key}/channelmap failed ({channelmap_response.status})",
            }
        changed["channelmap"] = True

    return {
        "source_id": source_id,
        "status": "updated"
        if any(changed.values()) or recreated_due_to_uri_change or recreated_due_to_dead_device
        else "unchanged",
        "device_key": device_key,
        "dvr_key": resolved_dvr_key,
        "mapped_channels": len(mapped_ids),
        "unmatched_channels": len(unmatched),
        "recreated_due_to_uri_change": recreated_due_to_uri_change,
        "recreated_due_to_dead_device": recreated_due_to_dead_device,
        "preserved_dead_device": preserved_dead_device,
        "device_title": device_title,
        "changed": changed,
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
            # WARNING: Do not remove the last tuner from a Plex DVR. Plex deletes the DVR and scheduled recordings.
            if not _can_remove_dvr_device(dvrs_response.payload, dvr_key, device_key):
                deletions.append(
                    {
                        "status": "preserved",
                        "dvr_key": dvr_key,
                        "device_key": device_key,
                        "model": model,
                        "detail": "kept_last_device_to_preserve_dvr",
                    }
                )
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
            if 200 <= response.status < 300 or response.status == 404:
                dvrs_response = await client.get_dvrs()
                if not (200 <= dvrs_response.status < 300):
                    deletions.append(
                        {
                            "status": "failed",
                            "error": f"failed to refresh DVRs after deletion ({dvrs_response.status})",
                        }
                    )
                    return deletions
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
    client = PlexClient(runtime_server, client_identifier=get_plex_client_identifier(config))
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

    stale_deletions = []
    can_delete_stale = all(item.get("status") not in {"failed", "preserved"} for item in per_source_results)
    if can_delete_stale:
        stale_deletions = await _delete_stale_managed_tuners(
            client=client,
            managed_prefix=managed_prefix,
            stream_key=stream_key,
            desired_models=desired_models,
        )

    failed_items = [item for item in per_source_results if item.get("status") == "failed"]
    failed_items.extend([item for item in stale_deletions if item.get("status") == "failed"])
    preserved_items = [item for item in per_source_results if item.get("status") == "preserved"]
    preserved_items.extend([item for item in stale_deletions if item.get("status") == "preserved"])
    status = "success" if not failed_items and not preserved_items else "partial"
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
    runtime_servers = parse_plex_servers_json(get_runtime_plex_servers())
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
        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            reason = _http_error_reason(exc)
            logger.warning("Plex reconcile failed for server_id=%s reason=%s", runtime_server.server_id, reason)
            result = {
                "server_id": runtime_server.server_id,
                "name": runtime_server.name,
                "status": "failed",
                "reason": reason,
            }
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
