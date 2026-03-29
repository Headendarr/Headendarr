#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import logging
import os
import time
import uuid
from urllib.parse import urlencode, urlparse

import aiohttp
from quart import Response, current_app, redirect, request, stream_with_context
from backend.api import blueprint
from backend.auth import skip_stream_connect_audit, stream_key_required
from backend.hls_multiplexer import (
    handle_m3u8_proxy,
    open_segment_passthrough,
    handle_segment_proxy,
    handle_multiplexed_stream,
    b64_urlsafe_decode,
    b64_urlsafe_encode,
    parse_size,
    mux_manager,
    SegmentCache,
)
from backend.cso import (
    CSO_UNAVAILABLE_SHOW_SLATE,
    cleanup_channel_stream_events,
    cso_session_manager,
    latest_cso_playback_issue_hint as _latest_cso_playback_issue_hint,
    summarize_cso_playback_issue as _summarize_playback_issue,
)
from backend.http_headers import decode_headers_query_param, merge_headers
from backend.stream_activity import (
    cleanup_stream_activity,
    enrich_stream_activity_metadata,
    touch_stream_activity,
    upsert_stream_activity,
)
from backend.url_resolver import get_request_base_url, get_request_origin

# Global cache instance (short default TTL for HLS segments)
hls_segment_cache = SegmentCache(ttl=120)

hls_proxy_prefix = os.environ.get("HLS_PROXY_PREFIX", "/")
if not hls_proxy_prefix.startswith("/"):
    hls_proxy_prefix = "/" + hls_proxy_prefix

hls_proxy_host_ip = os.environ.get("HLS_PROXY_HOST_IP")
hls_proxy_port = os.environ.get("HLS_PROXY_PORT")
hls_proxy_max_buffer_bytes = int(os.environ.get("HLS_PROXY_MAX_BUFFER_BYTES", "1048576"))
hls_proxy_default_prebuffer = parse_size(
    os.environ.get("HLS_PROXY_DEFAULT_PREBUFFER", "0"),
    default=0,
)
_last_cso_event_cleanup_ts = 0.0


def _get_instance_id():
    config = current_app.config.get("APP_CONFIG") if current_app else None
    if not config:
        return None
    return config.ensure_instance_id()


def _validate_instance_id(instance_id):
    """Internal proxy is scoped to this TIC instance only."""
    expected = _get_instance_id()
    if not expected or instance_id != expected:
        return Response("Not found", status=404)
    return None


proxy_logger = logging.getLogger("proxy")
ffmpeg_logger = logging.getLogger("ffmpeg")
buffer_logger = logging.getLogger("buffer")


def _query_flag_enabled(name: str) -> bool:
    value = str(request.args.get(name, "") or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


async def cleanup_hls_proxy_state():
    """Cleanup expired cache entries and idle stream activity."""
    global _last_cso_event_cleanup_ts
    try:
        evicted_count = await hls_segment_cache.evict_expired_items()
        if evicted_count > 0:
            proxy_logger.info(f"Cache cleanup: evicted {evicted_count} expired items")
        await cleanup_stream_activity()
        # Cleanup idle multiplexer streams
        await mux_manager.cleanup_idle_streams(idle_timeout=300)
        await cso_session_manager.cleanup_idle_streams(idle_timeout=300)
        now = time.time()
        if now - _last_cso_event_cleanup_ts >= 3600:
            await cleanup_channel_stream_events(current_app.config["APP_CONFIG"])
            _last_cso_event_cleanup_ts = now
    except Exception as e:
        proxy_logger.error(f"Error during cache cleanup: {e}")


async def periodic_cache_cleanup():
    while True:
        try:
            await cleanup_hls_proxy_state()
        except Exception as e:
            proxy_logger.error("Error during cache cleanup: %s", e)
        await asyncio.sleep(60)


@blueprint.record_once
def _register_startup(state):
    app = state.app

    @app.before_serving
    async def _start_periodic_cache_cleanup():
        asyncio.create_task(periodic_cache_cleanup())


def _configured_upstream_headers_from_query():
    return decode_headers_query_param(request.args.get("h"))


def _build_upstream_headers(configured_headers=None):
    headers = {}
    try:
        src = request.headers
    except Exception:
        return merge_headers(preferred=configured_headers, fallback=headers)
    for name in ("User-Agent", "Referer", "Origin", "Accept", "Accept-Language", "Range", "If-Range"):
        value = src.get(name)
        if value:
            headers[name] = value
    return merge_headers(preferred=configured_headers, fallback=headers)


def _apply_passthrough_headers(response, upstream_headers):
    allowed = (
        "Accept-Ranges",
        "Cache-Control",
        "Content-Length",
        "Content-Range",
        "Content-Type",
        "ETag",
        "Last-Modified",
    )
    for name in allowed:
        value = upstream_headers.get(name)
        if value:
            response.headers[name] = value
    return response


def _build_proxy_base_url(instance_id=None):
    base_path = hls_proxy_prefix.rstrip("/")
    if instance_id:
        base_path = f"{base_path}/{instance_id}"
    if hls_proxy_host_ip:
        protocol = "http"
        host_port = ""
        if hls_proxy_port:
            if hls_proxy_port == "443":
                protocol = "https"
            host_port = f":{hls_proxy_port}"
        return f"{protocol}://{hls_proxy_host_ip}{host_port}{base_path}"
    return f"{get_request_base_url(request)}{base_path}"


async def _enrich_stream_activity_background(decoded_url, connection_id):
    try:
        await enrich_stream_activity_metadata(decoded_url, connection_id)
    except Exception as exc:
        proxy_logger.warning(
            "proxy_m3u8 background_enrich_failed connection_id=%s error=%s",
            connection_id,
            exc,
        )


def _get_connection_id(default_new=False):
    value = (request.args.get("connection_id") or request.args.get("cid") or "").strip()
    if value:
        return value
    if default_new:
        return uuid.uuid4().hex
    return None


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/<encoded_url>.m3u8",
    methods=["GET", "HEAD"],
)
@stream_key_required
@skip_stream_connect_audit
async def proxy_m3u8(instance_id, encoded_url):
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    connection_id = _get_connection_id(default_new=False)
    if not connection_id:
        params = [(k, v) for k, v in request.args.items() if k not in {"cid", "connection_id"}]
        connection_id = _get_connection_id(default_new=True)
        params.append(("connection_id", connection_id))
        target = f"{hls_proxy_prefix.rstrip('/')}/{instance_id}/{encoded_url}.m3u8?{urlencode(params)}"
        return redirect(target, code=302)

    decoded_url = b64_urlsafe_decode(encoded_url)
    await upsert_stream_activity(decoded_url, connection_id=connection_id, enrich_metadata=False)
    asyncio.create_task(_enrich_stream_activity_background(decoded_url, connection_id))

    stream_key = request.args.get("stream_key") or request.args.get("password")
    username = request.args.get("username")
    configured_headers = _configured_upstream_headers_from_query()

    headers = _build_upstream_headers(configured_headers=configured_headers)

    body, content_type, status, res_headers = await handle_m3u8_proxy(
        decoded_url,
        request_host_url=f"{get_request_origin(request)}/",
        hls_proxy_prefix=hls_proxy_prefix,
        headers=headers,
        instance_id=instance_id,
        stream_key=stream_key,
        username=username,
        headers_query_token=request.args.get("h"),
        connection_id=connection_id,
        max_buffer_bytes=hls_proxy_max_buffer_bytes,
        proxy_base_url=_build_proxy_base_url(instance_id=instance_id),
        segment_cache=hls_segment_cache,
        prefetch_segments_enabled=True,
    )

    if body is None:
        resp = Response("Failed to fetch the original playlist.", status=status)
        for k, v in res_headers.items():
            resp.headers[k] = v
        return resp

    if hasattr(body, "__aiter__"):

        @stream_with_context
        async def generate_playlist():
            async for chunk in body:
                yield chunk

        return Response(generate_playlist(), content_type=content_type)
    return Response(body, content_type=content_type)


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/proxy.m3u8",
    methods=["GET", "HEAD"],
)
@stream_key_required
@skip_stream_connect_audit
async def proxy_m3u8_redirect(instance_id):
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    url = request.args.get("url")
    if not url:
        return Response("Missing url parameter.", status=400)
    encoded = b64_urlsafe_encode(url)
    stream_key = request.args.get("stream_key") or request.args.get("password")
    username = request.args.get("username")
    connection_id = _get_connection_id(default_new=True)
    if connection_id == "tvh":
        # Treat "tvh" as a logical label only. Internally, each request gets a
        # unique client id to avoid teardown collisions across reconnects.
        connection_id = f"tvh-{uuid.uuid4().hex}"
        proxy_logger.info(
            "Remapped reserved connection_id requested=tvh effective=%s url=%s",
            connection_id,
            url,
        )
    target = f"{hls_proxy_prefix.rstrip('/')}/{instance_id}/{encoded}.m3u8"
    query = [("connection_id", connection_id)]
    if stream_key:
        query.append(("stream_key", stream_key))
    if username:
        query.append(("username", username))
    headers_token = (request.args.get("h") or "").strip()
    if headers_token:
        query.append(("h", headers_token))

    return redirect(f"{target}?{urlencode(query)}", code=302)


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/<encoded_url>.key",
    methods=["GET", "HEAD"],
)
@stream_key_required
@skip_stream_connect_audit
async def proxy_key(instance_id, encoded_url):
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    # Decode the Base64 encoded URL
    decoded_url = b64_urlsafe_decode(encoded_url)
    # Touch activity via connection_id if available
    await upsert_stream_activity(decoded_url, connection_id=_get_connection_id(), perform_audit=False)

    content, status, _ = await handle_segment_proxy(
        decoded_url,
        _build_upstream_headers(configured_headers=_configured_upstream_headers_from_query()),
        hls_segment_cache,
        headers_query_token=request.args.get("h"),
    )
    if content is None:
        return Response("Failed to fetch.", status=status)
    return Response(content, content_type="application/octet-stream")


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/<encoded_url>.ts",
    methods=["GET", "HEAD"],
)
@stream_key_required
@skip_stream_connect_audit
async def proxy_ts(instance_id, encoded_url):
    """
    TIC Legacy/Segment Proxy Endpoint

    This endpoint serves individual .ts segments for HLS or provides a
    direct stream when configured.

    Parameters:
    - ffmpeg=true: (Optional) Fallback to FFmpeg remuxer for better compatibility.
    - prebuffer=X: (Optional) Buffer size cushion (e.g. 2M, 512K). Default: 1M.
    - direct=1: (Optional) Stream the upstream .ts response through directly.
      This bypasses the live multiplexer and is intended for seekable archive/timeshift streams.
    """
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    # Decode the Base64 encoded URL
    decoded_url = b64_urlsafe_decode(encoded_url)
    await upsert_stream_activity(decoded_url, connection_id=_get_connection_id(), perform_audit=False)
    headers = _build_upstream_headers(configured_headers=_configured_upstream_headers_from_query())

    # Direct proxy routing
    if _query_flag_enabled("direct"):
        proxy_logger.warning(
            "proxy_ts direct passthrough request decoded_url=%s method=%s", decoded_url, request.method
        )
        try:
            session, upstream_response = await open_segment_passthrough(
                decoded_url,
                headers,
                method=request.method,
            )
        except aiohttp.ClientError:
            return Response("Failed to fetch.", status=502)
        except asyncio.TimeoutError:
            return Response("Failed to fetch.", status=504)

        if upstream_response.status >= 400:
            status = upstream_response.status
            proxy_logger.warning(
                "proxy_ts direct passthrough upstream status=%s decoded_url=%s final_url=%s",
                status,
                decoded_url,
                getattr(upstream_response, "url", decoded_url),
            )
            upstream_response.release()
            await session.close()
            return Response("Failed to fetch.", status=status)

        if request.method == "HEAD":
            response = Response(status=upstream_response.status)
            _apply_passthrough_headers(response, upstream_response.headers)
            upstream_response.release()
            await session.close()
            return response

        @stream_with_context
        async def generate_direct():
            try:
                async for chunk in upstream_response.content.iter_chunked(64 * 1024):
                    yield chunk
            finally:
                upstream_response.release()
                await session.close()

        response = Response(generate_direct(), status=upstream_response.status)
        _apply_passthrough_headers(response, upstream_response.headers)
        return response

    # Multiplexer routing
    if _query_flag_enabled("ffmpeg") or request.args.get("prebuffer"):
        target = f"{hls_proxy_prefix.rstrip('/')}/{instance_id}/stream/{encoded_url}"
        if request.query_string:
            target = f"{target}?{request.query_string.decode()}"
        return redirect(target, code=302)

    content, status, content_type = await handle_segment_proxy(
        decoded_url,
        headers,
        hls_segment_cache,
        headers_query_token=request.args.get("h"),
    )
    if content is None:
        return Response("Failed to fetch.", status=status)

    # Playlist detection
    if "mpegurl" in (content_type or "") or content.lstrip().startswith(b"#EXTM3U"):
        target = f"{hls_proxy_prefix.rstrip('/')}/{instance_id}/{encoded_url}.m3u8"
        if request.query_string:
            target = f"{target}?{request.query_string.decode()}"
        return redirect(target, code=302)

    return Response(content, content_type="video/mp2t")


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/<encoded_url>.vtt",
    methods=["GET", "HEAD"],
)
@stream_key_required
@skip_stream_connect_audit
async def proxy_vtt(instance_id, encoded_url):
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    # Decode the Base64 encoded URL
    decoded_url = b64_urlsafe_decode(encoded_url)
    await upsert_stream_activity(decoded_url, connection_id=_get_connection_id(), perform_audit=False)

    content, status, _ = await handle_segment_proxy(
        decoded_url,
        _build_upstream_headers(configured_headers=_configured_upstream_headers_from_query()),
        hls_segment_cache,
        headers_query_token=request.args.get("h"),
    )
    if content is None:
        return Response("Failed to fetch.", status=status)
    return Response(content, content_type="text/vtt")


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/stream/<encoded_url>",
    methods=["GET"],
)
@stream_key_required
@skip_stream_connect_audit
async def stream_ts(instance_id, encoded_url):
    """
    TIC Shared Multiplexer Stream Endpoint

    This endpoint provides a shared upstream connection for multiple TIC clients.

    Default Mode (Direct):
    Uses high-performance async socket reads. Best for 99% of streams.

    Fallback Mode (FFmpeg):
    Enabled by appending '?ffmpeg=true'. Uses an external FFmpeg process to
    remux/clean the stream. Use this ONLY if 'direct' mode has playback issues.

    Parameters:
    - ffmpeg=true: (Optional) Fallback to FFmpeg remuxer for better compatibility.
    - prebuffer=X: (Optional) Buffer size cushion (e.g. 2M, 512K). Default: 1M.
    """
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid

    decoded_url = b64_urlsafe_decode(encoded_url)
    connection_id = _get_connection_id(default_new=True)
    await upsert_stream_activity(decoded_url, connection_id=connection_id)

    use_ffmpeg = _query_flag_enabled("ffmpeg")
    prebuffer_bytes = parse_size(
        request.args.get("prebuffer"),
        default=hls_proxy_default_prebuffer,
    )
    mode = "ffmpeg" if use_ffmpeg else "direct"

    generator = await handle_multiplexed_stream(
        decoded_url,
        mode,
        _build_upstream_headers(configured_headers=_configured_upstream_headers_from_query()),
        prebuffer_bytes,
        connection_id,
        headers_query_token=request.args.get("h"),
    )

    @stream_with_context
    async def generate_stream():
        last_touch_ts = time.time()
        async for chunk in generator:
            now = time.time()
            if (now - last_touch_ts) >= 5.0:
                await touch_stream_activity(connection_id, identity=decoded_url)
                last_touch_ts = now
            yield chunk

    response = Response(generate_stream(), content_type="video/mp2t")
    response.timeout = None  # Disable timeout for streaming response
    return response
