#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


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
        query_items.append(("password", stream_key))
    else:
        query_items.append(("stream_key", stream_key))
    new_query = urlencode(query_items)
    return urlunparse(parsed._replace(query=new_query))


def is_tic_stream_url(url: str) -> bool:
    return any(
        marker in url
        for marker in (
            "/tic-hls-proxy/",
            "/tic-web/",
            "/xmltv.php",
            "/get.php",
        )
    )
