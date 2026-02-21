#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import re

import sqlalchemy.exc
from quart import current_app

from backend.config import is_tvh_process_running_locally
from backend.streaming import build_local_hls_proxy_url, normalize_local_proxy_url, append_stream_key
from backend.stream_profiles import resolve_cso_profile_name, resolve_tvh_profile_name


def build_proxy_stream_url(base_url, source_url, stream_key, instance_id, username=None):
    return normalize_local_proxy_url(
        source_url,
        base_url=base_url,
        instance_id=instance_id,
        stream_key=stream_key,
        username=username,
    )


async def get_tvh_settings(include_auth=True, stream_profile="pass", stream_username=None, stream_key=None):
    config = current_app.config["APP_CONFIG"]
    settings = config.read_settings()
    tic_base_url = settings["settings"]["app_url"]
    protocol_match = re.match(r"^(https?)://", settings["settings"]["app_url"])
    tic_base_url_protocol = protocol_match.group(1) if protocol_match else "http"

    tvh_host = settings["settings"]["tvheadend"]["host"]
    tvh_port = settings["settings"]["tvheadend"]["port"]
    tvh_path = settings["settings"]["tvheadend"]["path"]
    tvh_base_url = f"{tvh_host}:{tvh_port}{tvh_path}"
    if await is_tvh_process_running_locally():
        tvh_path = "/tic-tvh"
        app_url = re.sub(r"^https?://", "", settings["settings"]["app_url"])
        tvh_base_url = f"{app_url}{tvh_path}"

    tvh_api_url = f"{tic_base_url_protocol}://{tvh_base_url}/api"
    tvh_http_url = f"{tic_base_url_protocol}://{tvh_base_url}"
    if include_auth and stream_username and stream_key:
        tvh_http_url = f"{tic_base_url_protocol}://{stream_username}:{stream_key}@{tvh_base_url}"
    stream_priority = 300
    return {
        "tic_base_url": tic_base_url,
        "tvh_base_url": tvh_base_url,
        "tvh_path": tvh_path,
        "tvh_api_url": tvh_api_url,
        "tvh_http_url": tvh_http_url,
        "stream_profile": stream_profile,
        "stream_priority": stream_priority,
    }


async def get_channels_for_playlist(playlist_id):
    from backend.channels import read_config_all_channels

    return_channels = []
    playlist_id_int = int(playlist_id)
    channels_config = await read_config_all_channels(
        filter_playlist_ids=[playlist_id_int],
        include_manual_sources_when_filtered=True,
    )
    for channel in channels_config:
        if not channel["enabled"]:
            continue

        sources = channel.get("sources") or []
        playlist_sources = [source for source in sources if source.get("playlist_id") == playlist_id_int]
        manual_sources = [source for source in sources if source.get("playlist_id") is None]
        ordered_sources = playlist_sources + manual_sources
        if ordered_sources:
            channel["sources"] = ordered_sources
            return_channels.append(channel)
    return return_channels


async def get_playlist_connection_count(config, playlist_id):
    from backend.playlists import read_config_one_playlist

    try:
        playlist_config = await read_config_one_playlist(config, playlist_id)
        return playlist_config.get("connections", 1)
    except sqlalchemy.exc.NoResultFound:
        return 1


def _should_use_cso_for_channel(channel_details, resolved_profile, force_cso=False):
    if force_cso:
        return True
    if channel_details.get("cso_enabled"):
        return True
    return resolved_profile != "default"


async def resolve_channel_stream_url(
    *,
    config,
    channel_details,
    base_url,
    stream_key=None,
    username=None,
    requested_profile="default",
    allow_tvh_profile=False,
    route_scope="source",
):
    from backend.channels import build_cso_channel_stream_url

    settings = config.read_settings()
    use_tvh_source = settings["settings"].get("route_playlists_through_tvh", False)
    use_cso_combined = settings["settings"].get("route_playlists_through_cso", False)
    force_cso = bool(route_scope == "combined" and use_cso_combined)
    if route_scope == "combined":
        # Combined endpoints have their own CSO routing switch and are independent from per-source TVH routing.
        use_tvh_source = False
    instance_id = config.ensure_instance_id()

    resolved_profile = resolve_cso_profile_name(
        config,
        requested_profile,
        channel=channel_details,
    )
    tvh_stream_profile = resolve_tvh_profile_name(config, cso_profile=resolved_profile)

    channel_url = None
    if _should_use_cso_for_channel(channel_details, resolved_profile, force_cso=force_cso):
        channel_url = build_cso_channel_stream_url(
            base_url=base_url,
            channel_id=channel_details.get("id"),
            stream_key=stream_key,
            username=username,
            profile=resolved_profile,
        )
    elif use_tvh_source and channel_details.get("tvh_uuid"):
        channel_url = f"{base_url}/tic-api/tvh_stream/stream/channel/{channel_details['tvh_uuid']}"
        path_args = f"?profile={tvh_stream_profile}&weight=300"
        channel_url = f"{channel_url}{path_args}"
        if stream_key:
            channel_url = append_stream_key(channel_url, stream_key=stream_key)
    else:
        source = channel_details["sources"][0] if channel_details.get("sources") else None
        source_url = source.get("stream_url") if source else None
        if source_url:
            is_manual = source.get("source_type") == "manual"
            use_hls_proxy = bool(source.get("use_hls_proxy", False))
            if is_manual and not use_hls_proxy:
                channel_url = source_url
            elif is_manual and use_hls_proxy:
                channel_url = build_local_hls_proxy_url(
                    base_url,
                    instance_id,
                    source_url,
                    stream_key=stream_key,
                    username=username,
                )
            else:
                channel_url = build_proxy_stream_url(
                    base_url,
                    source_url,
                    stream_key,
                    instance_id,
                    username=username,
                )

    return channel_url, resolved_profile, tvh_stream_profile
