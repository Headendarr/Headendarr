#!/usr/bin/env python3
# -*- coding:utf-8 -*-

from flask import request
from quart import jsonify, render_template_string, Response, current_app

from backend.api import blueprint
from backend.api.connections_common import (
    get_channels_for_playlist,
    get_playlist_connection_count,
    resolve_channel_stream_url,
)
from backend.auth import (
    audit_stream_event,
    get_request_stream_key,
    get_request_stream_user,
    is_tvh_backend_stream_user,
    skip_stream_connect_audit,
    stream_key_required,
)
from backend.channels import read_config_all_channels
from backend.epgs import generate_epg_channel_id
from backend.playlists import read_config_all_playlists
from backend.url_resolver import get_request_base_url


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


async def _get_discover_data(playlist_id=0, stream_username=None, stream_key=None, profile=None):
    config = current_app.config["APP_CONFIG"]
    external_base_url = get_request_base_url(request)
    device_name = f"Headendarr-{playlist_id}"
    tuner_count = await get_playlist_connection_count(config, playlist_id)
    device_id = f"tic-12345678-{playlist_id}"
    device_auth = f"tic-{playlist_id}"
    profile_suffix = f"/{profile}" if profile else ""
    if stream_key:
        base_url = f"{external_base_url}/tic-api/hdhr_device/{stream_key}/{playlist_id}{profile_suffix}"
    else:
        base_url = f"{external_base_url}/tic-api/hdhr_device/{playlist_id}{profile_suffix}"
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
    base_url = get_request_base_url(request)
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
            allow_tvh_profile=is_tvh_backend_stream_user(get_request_stream_user()),
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
    base_url = get_request_base_url(request)
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
            allow_tvh_profile=is_tvh_backend_stream_user(get_request_stream_user()),
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
    external_base_url = get_request_base_url(request)
    tuner_count = await _get_combined_tuner_count(config)
    profile_suffix = f"/{profile}" if profile else ""
    if stream_key:
        base_url = f"{external_base_url}/tic-api/hdhr_device/{stream_key}/combined{profile_suffix}"
    else:
        base_url = f"{external_base_url}/tic-api/hdhr_device/combined{profile_suffix}"
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
@blueprint.route("/tic-api/hdhr_device/<stream_key>/<playlist_id>/<profile>/discover.json", methods=["GET"])
@skip_stream_connect_audit
@stream_key_required
async def discover_json(playlist_id, stream_key=None, profile=None):
    stream_user = get_request_stream_user()
    await audit_stream_event(stream_user, "hdhr_discover", request.path, severity="debug")
    selected_profile = _requested_profile(profile)
    discover_data = await _get_discover_data(
        playlist_id=playlist_id,
        stream_username=stream_user.username if stream_user else None,
        stream_key=get_request_stream_key(),
        profile=selected_profile,
    )
    return jsonify(discover_data)


@blueprint.route("/tic-api/hdhr_device/<stream_key>/<playlist_id>/lineup.json", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/<stream_key>/<playlist_id>/<profile>/lineup.json", methods=["GET"])
@skip_stream_connect_audit
@stream_key_required
async def lineup_json(playlist_id, stream_key=None, profile=None):
    stream_user = get_request_stream_user()
    await audit_stream_event(stream_user, "hdhr_lineup", request.path, severity="debug")
    lineup_list = await _get_lineup_list(
        playlist_id,
        stream_username=stream_user.username if stream_user else None,
        stream_key=get_request_stream_key(),
        requested_profile=_requested_profile(profile),
    )
    return jsonify(lineup_list)


@blueprint.route("/tic-api/hdhr_device/<stream_key>/<playlist_id>/lineup_status.json", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/<stream_key>/<playlist_id>/<profile>/lineup_status.json", methods=["GET"])
@skip_stream_connect_audit
@stream_key_required
async def lineup_status_json(playlist_id=None, stream_key=None, profile=None):
    await audit_stream_event(get_request_stream_user(), "hdhr_lineup_status", request.path, severity="debug")
    return jsonify(
        {
            "ScanInProgress": 0,
            "ScanPossible": 0,
            "Source": "Cable",
            "SourceList": ["Cable"],
        }
    )


@blueprint.route("/tic-api/hdhr_device/<stream_key>/<playlist_id>/lineup.post", methods=["GET", "POST"])
@blueprint.route("/tic-api/hdhr_device/<stream_key>/<playlist_id>/<profile>/lineup.post", methods=["GET", "POST"])
@skip_stream_connect_audit
@stream_key_required
async def lineup_post(playlist_id=None, stream_key=None, profile=None):
    await audit_stream_event(get_request_stream_user(), "hdhr_lineup_post", request.path, severity="debug")
    return ""


@blueprint.route("/tic-api/hdhr_device/<stream_key>/<playlist_id>/device.xml", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/<stream_key>/<playlist_id>/<profile>/device.xml", methods=["GET"])
@skip_stream_connect_audit
@stream_key_required
async def device_xml(playlist_id, stream_key=None, profile=None):
    stream_user = get_request_stream_user()
    await audit_stream_event(stream_user, "hdhr_device_xml", request.path, severity="debug")
    selected_profile = _requested_profile(profile)
    discover_data = await _get_discover_data(
        playlist_id,
        stream_username=stream_user.username if stream_user else None,
        stream_key=get_request_stream_key(),
        profile=selected_profile,
    )
    xml_content = await render_template_string(device_xml_template, data=discover_data)
    return Response(xml_content, mimetype="application/xml")


@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/discover.json", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/<profile>/discover.json", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/combined/discover.json", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/combined/<profile>/discover.json", methods=["GET"])
@skip_stream_connect_audit
@stream_key_required
async def discover_json_combined(stream_key=None, profile=None):
    stream_user = get_request_stream_user()
    await audit_stream_event(stream_user, "hdhr_discover_combined", request.path, severity="debug")
    selected_profile = _requested_profile(profile)
    discover_data = await _get_combined_discover_data(
        stream_username=stream_user.username if stream_user else None,
        stream_key=get_request_stream_key(),
        profile=selected_profile,
    )
    return jsonify(discover_data)


@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/lineup.json", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/<profile>/lineup.json", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/combined/lineup.json", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/combined/<profile>/lineup.json", methods=["GET"])
@skip_stream_connect_audit
@stream_key_required
async def lineup_json_combined(stream_key=None, profile=None):
    stream_user = get_request_stream_user()
    await audit_stream_event(stream_user, "hdhr_lineup_combined", request.path, severity="debug")
    lineup_list = await _get_combined_lineup_list(
        stream_username=stream_user.username if stream_user else None,
        stream_key=get_request_stream_key(),
        requested_profile=_requested_profile(profile),
    )
    return jsonify(lineup_list)


@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/lineup_status.json", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/<profile>/lineup_status.json", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/combined/lineup_status.json", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/combined/<profile>/lineup_status.json", methods=["GET"])
@skip_stream_connect_audit
@stream_key_required
async def lineup_status_json_combined(stream_key=None, profile=None):
    await audit_stream_event(get_request_stream_user(), "hdhr_lineup_status_combined", request.path, severity="debug")
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
@blueprint.route("/tic-api/hdhr_device/combined/lineup.post", methods=["GET", "POST"])
@blueprint.route("/tic-api/hdhr_device/combined/<profile>/lineup.post", methods=["GET", "POST"])
@skip_stream_connect_audit
@stream_key_required
async def lineup_post_combined(stream_key=None, profile=None):
    await audit_stream_event(get_request_stream_user(), "hdhr_lineup_post_combined", request.path, severity="debug")
    return ""


@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/device.xml", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/<stream_key>/combined/<profile>/device.xml", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/combined/device.xml", methods=["GET"])
@blueprint.route("/tic-api/hdhr_device/combined/<profile>/device.xml", methods=["GET"])
@skip_stream_connect_audit
@stream_key_required
async def device_xml_combined(stream_key=None, profile=None):
    stream_user = get_request_stream_user()
    await audit_stream_event(stream_user, "hdhr_device_xml_combined", request.path, severity="debug")
    selected_profile = _requested_profile(profile)
    discover_data = await _get_combined_discover_data(
        stream_username=stream_user.username if stream_user else None,
        stream_key=get_request_stream_key(),
        profile=selected_profile,
    )
    xml_content = await render_template_string(device_xml_template, data=discover_data)
    return Response(xml_content, mimetype="application/xml")
