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
        path = path[len(LOCAL_PROXY_HOST_PLACEHOLDER):]
    if new_query:
        return f"{base}{path}?{new_query}"
    return f"{base}{path}"


def build_local_hls_proxy_url(
    base_url: str,
    instance_id: str,
    source_url: str,
    stream_key: str | None = None,
    username: str | None = None,
) -> str:
    parsed = urlparse(source_url)
    is_hls = (parsed.path or "").lower().endswith(".m3u8")
    encoded_url = base64.urlsafe_b64encode(source_url.encode("utf-8")).decode("utf-8")
    base = base_url.rstrip("/")
    if is_hls:
        url = f"{base}/tic-hls-proxy/{instance_id}/{encoded_url}.m3u8"
    else:
        url = f"{base}/tic-hls-proxy/{instance_id}/stream/{encoded_url}"
    return append_stream_key(url, stream_key=stream_key, username=username)


def build_custom_hls_proxy_url(source_url: str, hls_proxy_path: str | None) -> str:
    if not hls_proxy_path:
        return source_url
    encoded_url = base64.urlsafe_b64encode(source_url.encode("utf-8")).decode("utf-8")
    return (
        hls_proxy_path
        .replace("[URL]", source_url)
        .replace("[B64_URL]", encoded_url)
    )


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
) -> str:
    if not use_hls_proxy and not use_custom_hls_proxy:
        return source_url

    custom_url = None
    if use_custom_hls_proxy and custom_hls_proxy_path:
        custom_url = build_custom_hls_proxy_url(source_url, custom_hls_proxy_path)

    if use_hls_proxy and base_url and instance_id:
        if use_custom_hls_proxy and chain_custom_hls_proxy and custom_url:
            return build_local_hls_proxy_url(
                base_url,
                instance_id,
                custom_url,
                stream_key=stream_key,
                username=username,
            )
        if not use_custom_hls_proxy or not custom_url:
            return build_local_hls_proxy_url(
                base_url,
                instance_id,
                source_url,
                stream_key=stream_key,
                username=username,
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
            "/tic-web/",
            "/xmltv.php",
            "/get.php",
        )
    )
