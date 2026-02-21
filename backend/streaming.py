#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import base64
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


LOCAL_PROXY_HOST_PLACEHOLDER = "__TIC_HOST__"


async def get_tvh_stream_auth(config):
    stream_user = await config.get_tvh_stream_user()
    return stream_user.get("username"), stream_user.get("stream_key")


def _has_stream_auth(query_items):
    return any(key in ("stream_key", "password") for key, _ in query_items)


def _is_hls_source_url(url: str) -> bool:
    parsed = urlparse(url)
    return (parsed.path or "").lower().endswith(".m3u8")


def _coerce_custom_proxy_template_for_stream_endpoint(template: str) -> str:
    """
    Prefer custom proxy stream endpoints when the configured template is
    playlist-shaped.
    """
    if "[B64_URL].m3u8" in template:
        return template.replace("[B64_URL].m3u8", "stream/[B64_URL]")
    if "[URL].m3u8" in template:
        return template.replace("[URL].m3u8", "stream/[URL]")
    if "/proxy.m3u8?url=[URL]" in template:
        return template.replace("/proxy.m3u8?url=[URL]", "/stream/[B64_URL]")
    return template


def append_stream_key(url: str, stream_key: str = None, username: str = None) -> str:
    if not stream_key:
        return url
    parsed = urlparse(url)
    query_items = list(parse_qsl(parsed.query, keep_blank_values=True))
    if _has_stream_auth(query_items):
        return url
    if username:
        query_items.append(("username", username))
        query_items.append(("stream_key", stream_key))
    else:
        query_items.append(("stream_key", stream_key))
    new_query = urlencode(query_items)
    return urlunparse(parsed._replace(query=new_query))


def is_local_hls_proxy_url(url: str, instance_id: str | None) -> bool:
    if not instance_id:
        return False
    return f"/tic-hls-proxy/{instance_id}/" in url


def normalize_local_proxy_url(
    url: str,
    base_url: str,
    instance_id: str | None,
    stream_key: str | None = None,
    username: str | None = None,
) -> str:
    if not is_local_hls_proxy_url(url, instance_id):
        return url
    parsed = urlparse(url)
    query_items = list(parse_qsl(parsed.query, keep_blank_values=True))
    if stream_key and not _has_stream_auth(query_items):
        if username:
            query_items.append(("username", username))
            query_items.append(("stream_key", stream_key))
        else:
            query_items.append(("stream_key", stream_key))
    new_query = urlencode(query_items)
    base = base_url.rstrip("/")
    path = parsed.path or ""
    if path.startswith(LOCAL_PROXY_HOST_PLACEHOLDER):
        path = path[len(LOCAL_PROXY_HOST_PLACEHOLDER) :]
    if new_query:
        return f"{base}{path}?{new_query}"
    return f"{base}{path}"


def build_local_hls_proxy_url(
    base_url: str,
    instance_id: str,
    source_url: str,
    stream_key: str | None = None,
    username: str | None = None,
    ffmpeg: bool = False,
    prebuffer: str | None = None,
) -> str:
    parsed = urlparse(source_url)
    is_hls = (parsed.path or "").lower().endswith(".m3u8")
    encoded_url = base64.urlsafe_b64encode(source_url.encode("utf-8")).decode("utf-8")
    base = base_url.rstrip("/")
    if ffmpeg:
        url = f"{base}/tic-hls-proxy/{instance_id}/stream/{encoded_url}"
    elif is_hls:
        url = f"{base}/tic-hls-proxy/{instance_id}/{encoded_url}.m3u8"
    else:
        url = f"{base}/tic-hls-proxy/{instance_id}/stream/{encoded_url}"

    # Build query parameters
    query_items = []
    if ffmpeg:
        query_items.append(("ffmpeg", "true"))
    if prebuffer:
        query_items.append(("prebuffer", prebuffer))

    if query_items:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}{urlencode(query_items)}"

    return append_stream_key(url, stream_key=stream_key, username=username)


def build_custom_hls_proxy_url(
    source_url: str,
    hls_proxy_path: str | None,
    ffmpeg: bool = False,
) -> str:
    if not hls_proxy_path:
        return source_url
    template = hls_proxy_path
    if ffmpeg or not _is_hls_source_url(source_url):
        template = _coerce_custom_proxy_template_for_stream_endpoint(template)
    encoded_url = base64.urlsafe_b64encode(source_url.encode("utf-8")).decode("utf-8")
    return template.replace("[URL]", source_url).replace("[B64_URL]", encoded_url)


def build_configured_hls_proxy_url(
    source_url: str,
    base_url: str | None,
    instance_id: str | None,
    stream_key: str | None = None,
    username: str | None = None,
    use_hls_proxy: bool = False,
    use_custom_hls_proxy: bool = False,
    custom_hls_proxy_path: str | None = None,
    chain_custom_hls_proxy: bool = False,
    ffmpeg: bool = False,
    prebuffer: str | None = None,
) -> str:
    if not use_hls_proxy and not use_custom_hls_proxy:
        return source_url

    custom_url = None
    if use_custom_hls_proxy and custom_hls_proxy_path:
        custom_url = build_custom_hls_proxy_url(
            source_url,
            custom_hls_proxy_path,
            ffmpeg=ffmpeg,
        )

    if use_hls_proxy and base_url and instance_id:
        if use_custom_hls_proxy and chain_custom_hls_proxy and custom_url:
            return build_local_hls_proxy_url(
                base_url,
                instance_id,
                custom_url,
                stream_key=stream_key,
                username=username,
                ffmpeg=ffmpeg,
                prebuffer=prebuffer,
            )
        if not use_custom_hls_proxy or not custom_url:
            return build_local_hls_proxy_url(
                base_url,
                instance_id,
                source_url,
                stream_key=stream_key,
                username=username,
                ffmpeg=ffmpeg,
                prebuffer=prebuffer,
            )

    if use_custom_hls_proxy and custom_url:
        return custom_url

    return source_url


def is_tic_stream_url(url: str, instance_id: str | None = None) -> bool:
    if is_local_hls_proxy_url(url, instance_id):
        return True
    return any(
        marker in url
        for marker in (
            "/tic-api/epg/",
            "/tic-api/playlist/",
            "/tic-api/hdhr_device/",
            "/xmltv.php",
            "/get.php",
        )
    )
