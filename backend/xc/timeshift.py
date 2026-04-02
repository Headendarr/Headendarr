#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import aiohttp
from quart import Response, current_app, jsonify, request

from backend.hls_multiplexer import open_segment_passthrough
from backend.playlists import _resolve_source_request_headers
from backend.stream_activity import stop_stream_activity, touch_stream_activity
from backend.stream_profiles import content_type_for_media_path
from backend.streaming import build_configured_hls_proxy_url

XC_TIMESHIFT_DATETIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d:%H-%M:%S",
    "%Y-%m-%d:%H-%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
)
XC_TIMESHIFT_FORMAT_CACHE_TTL_SECONDS = 86400


def parse_timeshift_timestring_with_format(value: str | None) -> datetime | None:
    """
    Parse a client-provided timeshift timestamp into a datetime.
    """
    text = str(value or "").strip()
    if not text:
        return None

    for candidate_format in XC_TIMESHIFT_DATETIME_FORMATS:
        try:
            return datetime.strptime(text, candidate_format)
        except ValueError:
            continue

    current_app.logger.warning(
        "XC timeshift timestamp '%s' did not match supported formats: %s",
        text,
        ", ".join(XC_TIMESHIFT_DATETIME_FORMATS),
    )
    return None


def match_xc_timeshift_datetime_format(sample_value: str | None) -> str | None:
    sample_text = str(sample_value or "").strip()
    if not sample_text:
        return None
    for candidate_format in XC_TIMESHIFT_DATETIME_FORMATS:
        try:
            parsed = datetime.strptime(sample_text, candidate_format)
        except ValueError:
            continue
        if parsed.strftime(candidate_format) == sample_text:
            return candidate_format
    return None


async def read_xc_timeshift_format_cache_file(cache_path: Path) -> dict[str, dict[str, str]]:
    """Read the persisted XC timeshift datetime-format cache from disk."""

    def _read():
        if not cache_path.exists():
            return {}
        try:
            payload = json.loads(cache_path.read_text())
        except Exception:
            return {}
        if not isinstance(payload, dict):
            return {}
        return {str(key): value for key, value in payload.items() if key and isinstance(value, dict)}

    return await asyncio.to_thread(_read)


async def write_xc_timeshift_format_cache_file(cache_path: Path, cache_data: dict[str, dict[str, str]]):
    """Write the persisted XC timeshift datetime-format cache to disk."""

    def _write():
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(cache_data, indent=2, sort_keys=True))

    await asyncio.to_thread(_write)


async def persist_xc_timeshift_datetime_format(playlist_id: str, datetime_format: str):
    """
    Persist the working timeshift datetime format for a playlist so later
    playback requests do not need to rediscover it.
    """
    cache_path = Path(current_app.config["APP_CONFIG"].config_path) / "cache" / "xc_timeshift_datetime_formats.json"
    cache_data = await read_xc_timeshift_format_cache_file(cache_path)
    cache_entry = {
        "timeshift_datetime_format": datetime_format,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if cache_data.get(playlist_id) == cache_entry:
        return
    cache_data[playlist_id] = cache_entry
    await write_xc_timeshift_format_cache_file(cache_path, cache_data)


async def detect_xc_timeshift_datetime_format(archive_source: dict, request_headers: dict[str, str]) -> str | None:
    """
    Detect the datetime format expected by the upstream XC provider for
    timeshift playback.
    """
    playlist_id = archive_source.get("playlist_id")
    host_url = str(archive_source.get("host_url") or "").rstrip("/")
    upstream_stream_id = archive_source.get("upstream_stream_id")
    account = archive_source.get("account")
    account_username = str(getattr(account, "username", "") or "").strip()
    account_password = str(getattr(account, "password", "") or "").strip()
    if (
        playlist_id is None
        or not host_url
        or upstream_stream_id is None
        or not account_username
        or not account_password
    ):
        return None

    # Read cached data on timestring format to use
    cache_key = str(playlist_id)
    cache_path = Path(current_app.config["APP_CONFIG"].config_path) / "cache" / "xc_timeshift_datetime_formats.json"
    cache_data = await read_xc_timeshift_format_cache_file(cache_path)
    cached_entry = cache_data.get(cache_key) or {}
    cached_format = str(cached_entry.get("timeshift_datetime_format") or "").strip()
    updated_at = str(cached_entry.get("updated_at") or "").strip()
    if cached_format and updated_at:
        try:
            updated_at_ts = datetime.fromisoformat(updated_at.replace("Z", "+00:00")).timestamp()
            if updated_at_ts is not None and (time.time() - updated_at_ts) < XC_TIMESHIFT_FORMAT_CACHE_TTL_SECONDS:
                return cached_format
        except ValueError:
            pass

    # If nothing is cached yet, probe the upstream short-EPG response for a
    # sample `start` value and match its shape.
    query_params = {
        "username": account_username,
        "password": account_password,
        "action": "get_short_epg",
        "stream_id": upstream_stream_id,
        "limit": 1,
    }
    probe_headers = {key: value for key, value in request_headers.items() if key not in {"Range", "If-Range"}}
    probe_url = f"{host_url}/player_api.php"
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as client_session:
            async with client_session.get(probe_url, params=query_params, headers=probe_headers) as response:
                if response.status >= 400:
                    return None
                payload = await response.json(content_type=None)
    except Exception:
        return None

    epg_listings = payload.get("epg_listings") if isinstance(payload, dict) else None
    if not isinstance(epg_listings, list) or not epg_listings:
        return None
    first_listing = epg_listings[0] if isinstance(epg_listings[0], dict) else {}
    detected_format = match_xc_timeshift_datetime_format(first_listing.get("start"))
    if not detected_format:
        return None

    # Persist the detected format so later playback requests can reuse it.
    await persist_xc_timeshift_datetime_format(cache_key, detected_format)
    return detected_format


def parse_xc_stream_reference(raw_stream: str | None, raw_ext: str | None = None) -> tuple[str, str | None]:
    stream_text = str(raw_stream or "").strip()
    ext_text = str(raw_ext or "").strip().lower().lstrip(".") or None
    if not stream_text:
        return "", ext_text
    if "." not in stream_text:
        return stream_text, ext_text
    stream_id, suffix = stream_text.rsplit(".", 1)
    suffix = str(suffix or "").strip().lower().lstrip(".") or None
    if stream_id.isdigit() and suffix:
        return stream_id, ext_text or suffix
    return stream_text, ext_text


def xc_timeshift_output_extension(requested_ext: str | None, stream_id: str) -> str | None:
    ext_text = str(requested_ext or "").strip().lower().lstrip(".") or "ts"
    if ext_text not in {"ts", "m3u8"}:
        current_app.logger.warning(
            "XC timeshift requested unsupported extension '%s' for stream_id=%s; rejecting request",
            ext_text,
            stream_id,
        )
        return None
    return ext_text


def build_xc_timeshift_proxy_url(
    playlist,
    upstream_url: str,
    base_url: str,
    instance_id: str,
    stream_key: str,
    username: str,
    headers: dict[str, str],
    force_internal_hls_proxy: bool,
    prefer_stream_endpoint: bool = True,
) -> str:
    """
    Build the effective proxy URL for an XC timeshift target.

    By default this follows the playlist proxy settings, but the direct TS path
    can disable ffmpeg/prebuffer so seek-friendly requests avoid the live
    multiplexer route.
    """
    use_ffmpeg = bool(getattr(playlist, "hls_proxy_use_ffmpeg", False))
    prebuffer = getattr(playlist, "hls_proxy_prebuffer", "1M")
    direct = False
    if not prefer_stream_endpoint:
        use_ffmpeg = False
        prebuffer = None
        direct = True

    return build_configured_hls_proxy_url(
        upstream_url,
        base_url=base_url,
        instance_id=instance_id,
        stream_key=stream_key,
        username=username,
        use_hls_proxy=force_internal_hls_proxy or bool(getattr(playlist, "use_hls_proxy", False)),
        use_custom_hls_proxy=bool(getattr(playlist, "use_custom_hls_proxy", False)),
        custom_hls_proxy_path=getattr(playlist, "hls_proxy_path", None),
        chain_custom_hls_proxy=bool(getattr(playlist, "chain_custom_hls_proxy", False)),
        ffmpeg=use_ffmpeg,
        prebuffer=prebuffer,
        headers=headers,
        prefer_stream_endpoint=prefer_stream_endpoint,
        direct=direct,
    )


def build_xc_timeshift_source_url(
    playlist,
    target_url: str,
    base_url: str,
    instance_id: str,
    stream_key: str,
    username: str,
    headers: dict[str, str],
    connection_id: str | None = None,
) -> str:
    """
    Build a child URL for a rewritten timeshift manifest entry.

    Child requests follow the standard proxy path for rewritten timeshift
    manifests.
    """
    source_url = build_xc_timeshift_proxy_url(
        playlist,
        target_url,
        base_url,
        instance_id,
        stream_key,
        username,
        headers,
        force_internal_hls_proxy=False,
    )

    if not (bool(getattr(playlist, "use_hls_proxy", False)) or bool(getattr(playlist, "use_custom_hls_proxy", False))):
        return source_url

    parsed = urlparse(source_url)
    query_items = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key not in {"prebuffer", "connection_id", "cid"}
    ]
    if connection_id:
        query_items.append(("connection_id", connection_id))
    query_items.append(("prebuffer", "0"))
    return urlunparse(parsed._replace(query=urlencode(query_items)))


def rewrite_xc_hls_tag_uri(line: str, rewrite_url) -> str:
    def _replace(match):
        original_uri = str(match.group(1) or "")
        return f'URI="{rewrite_url(original_uri)}"'

    return re.sub(r'URI="([^"]+)"', _replace, line)


def copy_xc_passthrough_response_headers(response, headers):
    # Preserve the headers that matter for progressive playback and range
    # handling without copying the entire upstream header set blindly.
    content_range = headers.get("Content-Range")
    if content_range and int(getattr(response, "status_code", 0) or 0) == 206:
        response.headers["Content-Range"] = content_range
        response.headers["Accept-Ranges"] = "bytes"
    else:
        accept_ranges = str(headers.get("Accept-Ranges") or "").strip().lower()
        if accept_ranges in {"bytes", "none"}:
            response.headers["Accept-Ranges"] = accept_ranges

    for name in ("Cache-Control", "Content-Length", "Content-Type", "ETag", "Last-Modified"):
        value = headers.get(name)
        if value:
            response.headers[name] = value
    return response


async def stream_xc_timeshift_response(
    upstream_url: str,
    headers: dict[str, str],
    identity: str,
    connection_id: str | None,
    user,
    request_client_ip: str | None,
    request_user_agent: str | None,
):
    """
    Stream a timeshift TS response back to the client while keeping the
    upstream response shape close enough for playback and seeking.
    """
    client_session = None
    upstream_response = None

    try:
        client_session, upstream_response = await open_segment_passthrough(
            upstream_url,
            headers=headers,
            method=request.method,
        )
    except (aiohttp.ClientError, asyncio.TimeoutError):
        upstream_response = None

    # Startup failures should tear down the activity entry immediately.
    if upstream_response is None:
        if upstream_response is not None:
            upstream_response.release()
        if client_session is not None:
            await client_session.close()
        await stop_stream_activity(
            identity,
            connection_id=connection_id,
            endpoint_override=identity,
            perform_audit=False,
            user=user,
            ip_address=request_client_ip,
            user_agent=request_user_agent,
        )
        response = jsonify({"error": "Unable to start timeshift playback"})
        response.status_code = 502
        return response

    if upstream_response.status >= 400:
        body = await upstream_response.read()
        content_type = upstream_response.headers.get("Content-Type", "text/plain")
        status = upstream_response.status
        upstream_response.release()
        if client_session is not None:
            await client_session.close()
        await stop_stream_activity(
            identity,
            connection_id=connection_id,
            endpoint_override=identity,
            perform_audit=False,
            user=user,
            ip_address=request_client_ip,
            user_agent=request_user_agent,
        )
        return Response(body or b"", content_type=content_type, status=status)

    # HEAD requests only need the upstream metadata, not the body stream.
    if request.method == "HEAD":
        response = Response(
            b"",
            content_type=upstream_response.headers.get("Content-Type") or content_type_for_media_path(upstream_url),
            status=upstream_response.status,
        )
        copy_xc_passthrough_response_headers(response, upstream_response.headers)
        upstream_response.release()
        if client_session is not None:
            await client_session.close()
        return response

    async def _generator():
        last_touch_ts = time.time()
        try:
            async for chunk in upstream_response.content.iter_chunked(64 * 1024):
                now = time.time()
                if (now - last_touch_ts) >= 5.0:
                    await touch_stream_activity(connection_id, identity=identity)
                    last_touch_ts = now
                yield chunk
        finally:
            try:
                upstream_response.close()
            except Exception:
                pass
            if client_session is not None:
                await client_session.close()
            await stop_stream_activity(
                identity,
                connection_id=connection_id,
                endpoint_override=identity,
                perform_audit=False,
                user=user,
                ip_address=request_client_ip,
                user_agent=request_user_agent,
            )

    response = Response(
        _generator(),
        content_type=upstream_response.headers.get("Content-Type") or content_type_for_media_path(upstream_url),
        status=upstream_response.status,
    )
    copy_xc_passthrough_response_headers(response, upstream_response.headers)
    response.timeout = None
    return response


def rewrite_xc_timeshift_manifest(
    playlist_text: str,
    manifest_url: str,
    playlist,
    request_base_url: str,
    instance_id: str,
    stream_key: str,
    username: str,
    request_headers: dict[str, str],
    connection_id: str,
) -> str:
    """
    Rewrite an upstream timeshift manifest so every nested URI continues back
    through TIC's proxy path.
    """

    def _proxy_url_for_target(target_url: str) -> str:
        absolute_url = urljoin(manifest_url, str(target_url or "").strip())
        return build_xc_timeshift_source_url(
            playlist,
            absolute_url,
            request_base_url,
            instance_id,
            stream_key,
            username,
            request_headers,
            connection_id,
        )

    # Tags may carry URIs inside attributes, while plain lines are the media
    # targets themselves.
    lines = []
    for raw_line in str(playlist_text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            lines.append(rewrite_xc_hls_tag_uri(raw_line, _proxy_url_for_target))
            continue
        lines.append(_proxy_url_for_target(line))
    return "\n".join(lines) + "\n"


async def fetch_and_rewrite_xc_timeshift_manifest(
    upstream_url: str,
    playlist,
    request_base_url: str,
    instance_id: str,
    stream_key: str,
    username: str,
    request_headers: dict[str, str],
    connection_id: str,
) -> Response:
    """
    Fetch an upstream XC timeshift manifest and rewrite it for TIC-managed
    follow-up requests.
    """
    current_app.logger.warning("XC timeshift upstream manifest request: %s", upstream_url)
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=120)
    async with aiohttp.ClientSession(headers=request_headers, timeout=timeout) as client_session:
        try:
            upstream_response = await client_session.get(upstream_url, allow_redirects=True)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            response = jsonify({"error": "Unable to start timeshift playback"})
            response.status_code = 502
            return response

        if upstream_response.status >= 400:
            body = await upstream_response.read()
            return Response(
                body or b"",
                content_type=upstream_response.headers.get("Content-Type", "text/plain"),
                status=upstream_response.status,
            )

        playlist_text = await upstream_response.text()
        rewritten_playlist = rewrite_xc_timeshift_manifest(
            playlist_text,
            str(upstream_response.url),
            playlist,
            request_base_url,
            instance_id,
            stream_key,
            username,
            request_headers,
            connection_id,
        )
        return Response(
            rewritten_playlist or "",
            content_type="application/vnd.apple.mpegurl",
            status=upstream_response.status,
        )


def build_xc_timeshift_request_headers(playlist) -> dict[str, str]:
    """Build the upstream request headers for timeshift playback."""

    request_headers = _resolve_source_request_headers(current_app.config["APP_CONFIG"].read_settings(), playlist)
    # Pass through range headers so direct TS playback stays seekable.
    for header_name in ("Range", "If-Range"):
        header_value = request.headers.get(header_name)
        if header_value:
            request_headers[header_name] = header_value
    return request_headers
