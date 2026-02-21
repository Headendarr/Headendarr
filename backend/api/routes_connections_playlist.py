#!/usr/bin/env python3
# -*- coding:utf-8 -*-

from flask import request
from quart import Response, current_app

from backend.api import blueprint
from backend.api.connections_common import get_channels_for_playlist, resolve_channel_stream_url
from backend.auth import stream_key_required, audit_stream_event, is_tvh_backend_stream_user
from backend.epgs import generate_epg_channel_id
from backend.playlists import build_tic_playlist_with_epg_content


def _requested_profile():
    return (request.args.get("profile") or "default").strip().lower()


async def _playlist_m3u_lines(playlist_id, *, stream_key=None, username=None, requested_profile="default"):
    config = current_app.config["APP_CONFIG"]
    base_url = request.host_url.rstrip("/")
    epg_url = f"{base_url}/tic-api/epg/xmltv.xml"
    if stream_key:
        if username:
            epg_url = f"{epg_url}?username={username}&password={stream_key}"
        else:
            epg_url = f"{epg_url}?stream_key={stream_key}"

    lines = [f'#EXTM3U url-tvg="{epg_url}"']
    for channel_details in await get_channels_for_playlist(playlist_id):
        channel_id = generate_epg_channel_id(channel_details["number"], channel_details["name"])
        channel_name = channel_details["name"]
        channel_logo_url = channel_details.get("logo_url") or ""
        channel_uuid = channel_details.get("tvh_uuid") or ""
        extinf = (
            f'#EXTINF:-1 tvg-name="{channel_name}" tvg-logo="{channel_logo_url}" '
            f'tvg-id="{channel_uuid}" tvg-chno="{channel_id}"'
        )
        if channel_details.get("tags"):
            extinf += f' group-title="{channel_details["tags"][0]}"'
        extinf += f" , {channel_name}"
        lines.append(extinf)

        stream_url, _, _ = await resolve_channel_stream_url(
            config=config,
            channel_details=channel_details,
            base_url=base_url,
            stream_key=stream_key,
            username=username,
            requested_profile=requested_profile,
            allow_tvh_profile=is_tvh_backend_stream_user(getattr(request, "_stream_user", None)),
        )
        if stream_url:
            lines.append(stream_url)

    return lines


@blueprint.route("/tic-api/playlist/combined.m3u", methods=["GET"])
@stream_key_required
async def combined_playlist_m3u():
    await audit_stream_event(request._stream_user, "playlist_m3u_combined", request.path)
    stream_key = request.args.get("stream_key") or request.args.get("password") or request._stream_key
    username = request._stream_user.username if stream_key and request._stream_user else request.args.get("username")
    profile = _requested_profile()

    content = await build_tic_playlist_with_epg_content(
        current_app.config["APP_CONFIG"],
        base_url=request.url_root.rstrip("/"),
        stream_key=stream_key,
        username=username,
        include_xtvg=True,
        requested_profile=profile,
        allow_tvh_profile=is_tvh_backend_stream_user(getattr(request, "_stream_user", None)),
    )
    response = Response(content, mimetype="application/vnd.apple.mpegurl")
    response.headers["Content-Disposition"] = 'attachment; filename="combined.m3u"'
    return response


@blueprint.route("/tic-api/playlist/<playlist_id>.m3u", methods=["GET"])
# TODO: Remove this backward-compat route. Remove after external clients migrate to /tic-api/playlist/<source_id>.m3u.
@blueprint.route("/tic-api/tvh_playlist/<playlist_id>/channels.m3u", methods=["GET"])
@stream_key_required
async def source_playlist_m3u(playlist_id):
    await audit_stream_event(request._stream_user, "playlist_m3u", request.path)
    stream_key = request.args.get("stream_key") or request.args.get("password") or request._stream_key
    username = request._stream_user.username if stream_key and request._stream_user else request.args.get("username")
    profile = _requested_profile()

    lines = await _playlist_m3u_lines(
        playlist_id,
        stream_key=stream_key,
        username=username,
        requested_profile=profile,
    )
    m3u_content = "\n".join(lines)
    response = Response(m3u_content, mimetype="text/plain")
    response.headers["Content-Disposition"] = f'attachment; filename="{playlist_id}_channels.m3u"'
    return response
