#!/usr/bin/env python3
# -*- coding:utf-8 -*-

from flask import request
from quart import jsonify, render_template_string, Response, current_app

from backend.api import blueprint
from backend.api.connections_common import (
    get_channels_for_playlist,
    get_playlist_connection_count,
    get_tvh_settings,
    resolve_channel_stream_url,
)
from backend.auth import stream_key_required, audit_stream_event, is_tvh_backend_stream_user
from backend.channels import read_config_all_channels
from backend.epgs import generate_epg_channel_id
from backend.playlists import read_config_all_playlists


device_xml_template = """<?xml version="1.0" encoding="UTF-8"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
    <specVersion>
        <major>1</major>
        <minor>0</minor>
    </specVersion>
    <URLBase>{{ data.BaseURL }}</URLBase>
    <device>
        <deviceType>urn:schemas-upnp-org:device:MediaServer:1</deviceType>
        <friendlyName>{{ data.FriendlyName }}</friendlyName>
        <manufacturer>{{ data.Manufacturer }}</manufacturer>
        <modelName>{{ data.ModelNumber }}</modelName>
        <modelNumber>{{ data.ModelNumber }}</modelNumber>
        <serialNumber></serialNumber>
        <UDN>uuid:{{ data.DeviceID }}</UDN>
    </device>
</root>"""


def _requested_profile(path_profile=None):
    return (path_profile or request.args.get("profile") or "default").strip().lower()


async def _get_discover_data(playlist_id=0, stream_username=None, stream_key=None):
    config = current_app.config["APP_CONFIG"]
    tvh_settings = await get_tvh_settings(
        include_auth=True,
        stream_username=stream_username,
        stream_key=stream_key,
    )
    device_name = f"Headendarr-{playlist_id}"
    tuner_count = await get_playlist_connection_count(config, playlist_id)
    device_id = f"tic-12345678-{playlist_id}"
    device_auth = f"tic-{playlist_id}"
    if stream_key:
        base_url = f'{tvh_settings["tic_base_url"]}/tic-api/hdhr_device/{stream_key}/{playlist_id}'
    else:
        base_url = f'{tvh_settings["tic_base_url"]}/tic-api/hdhr_device/{playlist_id}'
    return {
        "FriendlyName": device_name,
        "Manufacturer": "Tvheadend",
        "ModelNumber": "HDTC-2US",
        "FirmwareName": "bin_2.2.0",
        "TunerCount": tuner_count,
        "FirmwareVersion": "2.2.0",
        "DeviceID": device_id,
        "DeviceAuth": device_auth,
        "BaseURL": base_url,
        "LineupURL": f"{base_url}/lineup.json",
    }


async def _get_lineup_list(playlist_id, stream_username=None, stream_key=None, requested_profile="default"):
    config = current_app.config["APP_CONFIG"]
    base_url = request.host_url.rstrip("/")
    lineup_list = []
    for channel_details in await get_channels_for_playlist(playlist_id):
        channel_id = generate_epg_channel_id(channel_details["number"], channel_details["name"])
        channel_url, _, _ = await resolve_channel_stream_url(
            config=config,
            channel_details=channel_details,
            base_url=base_url,
            stream_key=stream_key,
            username=stream_username,
            requested_profile=requested_profile,
            allow_tvh_profile=is_tvh_backend_stream_user(getattr(request, "_stream_user", None)),
        )
        if channel_url:
            lineup_list.append(
                {
                    "GuideNumber": channel_id,
                    "GuideName": channel_details["name"],
                    "URL": channel_url,
                }
            )
    return lineup_list


async def _get_combined_tuner_count(config):
    try:
        playlists = await read_config_all_playlists(config)
    except Exception:
        return 1
    total = 0
    for playlist in playlists:
        if not playlist.get("enabled", True):
            continue
        try:
            total += int(playlist.get("connections", 1) or 1)
        except (TypeError, ValueError):
            total += 1
    return max(total, 1)


async def _get_combined_lineup_list(stream_username=None, stream_key=None, requested_profile="default"):
    config = current_app.config["APP_CONFIG"]
    base_url = request.host_url.rstrip("/")
    channels = await read_config_all_channels()
    lineup_list = []
    for channel_details in channels:
        if not channel_details.get("enabled"):
            continue
        channel_id = generate_epg_channel_id(channel_details["number"], channel_details["name"])
        channel_url, _, _ = await resolve_channel_stream_url(
            config=config,
            channel_details=channel_details,
            base_url=base_url,
            stream_key=stream_key,
            username=stream_username,
            requested_profile=requested_profile,
            allow_tvh_profile=is_tvh_backend_stream_user(getattr(request, "_stream_user", None)),
            route_scope="combined",
        )
        if channel_url:
            lineup_list.append(
                {
                    "GuideNumber": channel_id,
                    "GuideName": channel_details["name"],
                    "URL": channel_url,
                }
            )
    return lineup_list


async def _get_combined_discover_data(stream_username=None, stream_key=None, profile=None):
    config = current_app.config["APP_CONFIG"]
    tvh_settings = await get_tvh_settings(
        include_auth=True,
        stream_username=stream_username,
        stream_key=stream_key,
    )
    tuner_count = await _get_combined_tuner_count(config)
    profile_suffix = f"/{profile}" if profile else ""
    if stream_key:
        base_url = f'{tvh_settings["tic_base_url"]}/tic-api/hdhr_device/{stream_key}/combined{profile_suffix}'
    else:
        base_url = f'{tvh_settings["tic_base_url"]}/tic-api/hdhr_device/combined{profile_suffix}'
    return {
        "FriendlyName": "Headendarr-Combined",
        "Manufacturer": "Tvheadend",
        "ModelNumber": "HDTC-2US",
        "FirmwareName": "bin_2.2.0",
        "TunerCount": tuner_count,
        "FirmwareVersion": "2.2.0",
        "DeviceID": "tic-12345678-combined",
        "DeviceAuth": "tic-combined",
        "BaseURL": base_url,
        "LineupURL": f"{base_url}/lineup.json",
    }


@blueprint.route("/tic-api/hdhr_device/<stream_key>/<playlist_id>/discover.json", methods=["GET"])
@stream_key_required
async def discover_json(playlist_id, stream_key=None):
    await audit_stream_event(request._stream_user, "hdhr_discover", request.path)
    discover_data = await _get_discover_data(
        playlist_id=playlist_id,
        stream_username=request._stream_user.username if request._stream_user else None,
        stream_key=request._stream_key,
    )
    return jsonify(discover_data)


@blueprint.route("/tic-api/hdhr_device/<stream_key>/<playlist_id>/lineup.json", methods=["GET"])
@stream_key_required
async def lineup_json(playlist_id, stream_key=None):
    await audit_stream_event(request._stream_user, "hdhr_lineup", request.path)
    lineup_list = await _get_lineup_list(
        playlist_id,
        stream_username=request._stream_user.username if request._stream_user else None,
        stream_key=request._stream_key,
        requested_profile=_requested_profile(),
    )
    return jsonify(lineup_list)


@blueprint.route("/tic-api/hdhr_device/<stream_key>/<playlist_id>/lineup_status.json", methods=["GET"])
@stream_key_required
async def lineup_status_json(playlist_id=None, stream_key=None):
    await audit_stream_event(request._stream_user, "hdhr_lineup_status", request.path)
    return jsonify(
        {
            "ScanInProgress": 0,
            "ScanPossible": 0,
            "Source": "Cable",
            "SourceList": ["Cable"],
        }
    )


@blueprint.route("/tic-api/hdhr_device/<stream_key>/<playlist_id>/lineup.post", methods=["GET", "POST"])
@stream_key_required
async def lineup_post(playlist_id=None, stream_key=None):
    await audit_stream_event(request._stream_user, "hdhr_lineup_post", request.path)
    return ""


@blueprint.route("/tic-api/hdhr_device/<stream_key>/<playlist_id>/device.xml", methods=["GET"])
@stream_key_required
async def device_xml(playlist_id, stream_key=None):
    await audit_stream_event(request._stream_user, "hdhr_device_xml", request.path)
    discover_data = await _get_discover_data(
        playlist_id,
        stream_username=request._stream_user.username if request._stream_user else None,
        stream_key=request._stream_key,
    )
    xml_content = await render_template_string(device_xml_template, data=discover_data)
    return Response(xml_content, mimetype="application/xml")


@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/discover.json", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/<profile>/discover.json", methods=["GET"])
@stream_key_required
async def discover_json_combined(stream_key=None, profile=None):
    await audit_stream_event(request._stream_user, "hdhr_discover_combined", request.path)
    selected_profile = _requested_profile(profile)
    discover_data = await _get_combined_discover_data(
        stream_username=request._stream_user.username if request._stream_user else None,
        stream_key=request._stream_key,
        profile=selected_profile,
    )
    return jsonify(discover_data)


@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/lineup.json", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/<profile>/lineup.json", methods=["GET"])
@stream_key_required
async def lineup_json_combined(stream_key=None, profile=None):
    await audit_stream_event(request._stream_user, "hdhr_lineup_combined", request.path)
    lineup_list = await _get_combined_lineup_list(
        stream_username=request._stream_user.username if request._stream_user else None,
        stream_key=request._stream_key,
        requested_profile=_requested_profile(profile),
    )
    return jsonify(lineup_list)


@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/lineup_status.json", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/<profile>/lineup_status.json", methods=["GET"])
@stream_key_required
async def lineup_status_json_combined(stream_key=None, profile=None):
    await audit_stream_event(request._stream_user, "hdhr_lineup_status_combined", request.path)
    return jsonify(
        {
            "ScanInProgress": 0,
            "ScanPossible": 0,
            "Source": "Cable",
            "SourceList": ["Cable"],
        }
    )


@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/lineup.post", methods=["GET", "POST"])
@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/<profile>/lineup.post", methods=["GET", "POST"])
@stream_key_required
async def lineup_post_combined(stream_key=None, profile=None):
    await audit_stream_event(request._stream_user, "hdhr_lineup_post_combined", request.path)
    return ""


@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/device.xml", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/<profile>/device.xml", methods=["GET"])
@stream_key_required
async def device_xml_combined(stream_key=None, profile=None):
    await audit_stream_event(request._stream_user, "hdhr_device_xml_combined", request.path)
    selected_profile = _requested_profile(profile)
    discover_data = await _get_combined_discover_data(
        stream_username=request._stream_user.username if request._stream_user else None,
        stream_key=request._stream_key,
        profile=selected_profile,
    )
    xml_content = await render_template_string(device_xml_template, data=discover_data)
    return Response(xml_content, mimetype="application/xml")
