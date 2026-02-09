#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import secrets
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from backend.users import create_user, get_user_by_username, rotate_stream_key

DEFAULT_TVH_STREAM_USERNAME = "tvh-streamer"


async def ensure_stream_user(username: str, role_names=None):
    role_names = role_names or ["streamer"]
    user = await get_user_by_username(username)
    if user and user.streaming_key:
        return user, user.streaming_key, False
    if user:
        user, stream_key = await rotate_stream_key(user.id)
        return user, stream_key, False
    password = secrets.token_urlsafe(18)
    user, stream_key = await create_user(username, password, role_names=role_names)
    return user, stream_key, True


async def get_tvh_stream_auth(config):
    settings = config.read_settings()
    username = (settings.get("settings") or {}).get("tvh_stream_username") or DEFAULT_TVH_STREAM_USERNAME
    user, stream_key, _created = await ensure_stream_user(username)
    return username, stream_key


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
