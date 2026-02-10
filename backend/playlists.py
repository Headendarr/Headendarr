#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import base64
import logging
import os
import time
from urllib.parse import urlparse
from operator import attrgetter

import aiofiles
import aiohttp
from sqlalchemy import delete, func, insert, or_, select
from sqlalchemy.orm import joinedload

from backend.ffmpeg import ffprobe_file
from backend.models import Playlist, PlaylistStreams, Session, XcAccount, db
from backend.streaming import build_configured_hls_proxy_url
from backend.tvheadend.tvh_requests import get_tvh, network_template

logger = logging.getLogger("tic.playlists")

XC_ACCOUNT_TYPE = "XC"
M3U_ACCOUNT_TYPE = "M3U"


def _normalize_xc_host(host_url):
    if not host_url:
        return host_url
    host_url = host_url.rstrip("/")
    if "://" in host_url:
        proto, rest = host_url.split("://", 1)
        host = rest.split("/", 1)[0]
        return f"{proto}://{host}"
    return host_url


async def _xc_request(session, host_url, params, retries=3):
    url = f"{host_url}/player_api.php"
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, params=params, timeout=30) as response:
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            last_error = exc
            if attempt < retries:
                await asyncio.sleep(attempt)
                continue
            raise
    if last_error:
        raise last_error


def _extract_xc_suffix(url):
    if not url:
        return ""
    path = urlparse(url).path
    if "." not in path:
        return ""
    ext = path.rsplit(".", 1)[1]
    return f".{ext}" if ext else ""


def _build_xc_url_template(host_url, stream_id, suffix):
    return f"{host_url}/live/{{username}}/{{password}}/{stream_id}{suffix}"


def _render_xc_url(template, username, password):
    if not template:
        return ""
    if "{username}" in template or "{password}" in template:
        return template.format(username=username, password=password)
    return template


def _get_primary_xc_account_sync(playlist_id):
    return (
        db.session.query(XcAccount)
        .filter(XcAccount.playlist_id == playlist_id, XcAccount.enabled.is_(True))
        .order_by(XcAccount.id.asc())
        .first()
    )


async def _get_primary_xc_account_async(playlist_id):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(
                select(XcAccount)
                .where(
                    XcAccount.playlist_id == playlist_id, XcAccount.enabled.is_(True)
                )
                .order_by(XcAccount.id.asc())
            )
            return result.scalars().first()


def _get_enabled_xc_accounts_sync(playlist_id):
    return (
        db.session.query(XcAccount)
        .filter(XcAccount.playlist_id == playlist_id, XcAccount.enabled.is_(True))
        .order_by(XcAccount.id.asc())
        .all()
    )


async def _get_enabled_xc_accounts_async(playlist_id):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(
                select(XcAccount)
                .where(
                    XcAccount.playlist_id == playlist_id, XcAccount.enabled.is_(True)
                )
                .order_by(XcAccount.id.asc())
            )
            return result.scalars().all()


async def _import_xc_playlist_streams(settings, playlist):
    host_url = _normalize_xc_host(playlist.url)
    account = await _get_primary_xc_account_async(playlist.id)
    if not account and playlist.xc_username and playlist.xc_password:
        account = type("LegacyAccount", (), {})()
        account.username = playlist.xc_username
        account.password = playlist.xc_password
    if not host_url or not account:
        logger.error("XC playlist %s missing host/credentials", playlist.id)
        return False

    user_agent = _resolve_user_agent(settings, playlist.user_agent)
    headers = {}
    if user_agent:
        headers["User-Agent"] = user_agent

    async with aiohttp.ClientSession(headers=headers) as session:
        auth_info = await _xc_request(
            session,
            host_url,
            {
                "username": account.username,
                "password": account.password,
            },
        )
        if not isinstance(auth_info, dict) or not auth_info.get("user_info"):
            logger.error("XC auth failed for playlist %s", playlist.id)
            return False

        categories = await _xc_request(
            session,
            host_url,
            {
                "username": account.username,
                "password": account.password,
                "action": "get_live_categories",
            },
        )
        if not isinstance(categories, list):
            logger.error("XC categories response invalid for playlist %s", playlist.id)
            return False
        category_map = {
            str(c.get("category_id")): c.get("category_name") for c in categories
        }

        streams = await _xc_request(
            session,
            host_url,
            {
                "username": account.username,
                "password": account.password,
                "action": "get_live_streams",
            },
        )
        if not isinstance(streams, list):
            logger.error("XC streams response invalid for playlist %s", playlist.id)
            return False

    items = []
    for stream in streams:
        stream_id = stream.get("stream_id")
        if not stream_id:
            continue
        category_id = stream.get("category_id")
        epg_id = (stream.get("epg_channel_id") or "").strip() or stream.get("tvg_id")
        tvg_logo = stream.get("tvg_logo") or stream.get("stream_icon")
        tvg_chno = stream.get("tvg_chno")
        try:
            tvg_chno = int(tvg_chno) if tvg_chno is not None else None
        except (TypeError, ValueError):
            tvg_chno = None

        # I'm not sure yet if this is required. I need to test more XC sources to know
        container_ext = (stream.get("container_extension") or "").strip().lstrip(".")
        suffix = f".{container_ext}" if container_ext else ""
        template_url = _build_xc_url_template(host_url, stream_id, suffix)
        items.append(
            {
                "playlist_id": playlist.id,
                "name": stream.get("name"),
                "url": template_url,
                "channel_id": stream.get("epg_channel_id") or stream.get("tvg_id"),
                "group_title": category_map.get(str(category_id)),
                "tvg_chno": tvg_chno,
                "tvg_id": epg_id,
                "tvg_logo": tvg_logo,
                "source_type": XC_ACCOUNT_TYPE,
                "xc_stream_id": stream_id,
                "xc_category_id": int(category_id)
                if category_id is not None and str(category_id).isdigit()
                else None,
            }
        )

    async with Session() as session:
        async with session.begin():
            stmt = delete(PlaylistStreams).where(
                PlaylistStreams.playlist_id == playlist.id
            )
            await session.execute(stmt)
            if items:
                await session.execute(insert(PlaylistStreams), items)
            await session.commit()

    logger.info("Imported %s XC streams for playlist #%s", len(items), playlist.id)
    return True


async def read_config_all_playlists(config, output_for_export=False):
    return_list = []
    async with Session() as session:
        async with session.begin():
            query = await session.execute(select(Playlist))
            results = query.scalars().all()
            for result in results:
                if output_for_export:
                    return_list.append(
                        {
                            "enabled": result.enabled,
                            "connections": result.connections,
                            "name": result.name,
                            "url": result.url,
                            "account_type": result.account_type,
                            "xc_username": result.xc_username,
                            "user_agent": result.user_agent,
                            "use_hls_proxy": result.use_hls_proxy,
                            "use_custom_hls_proxy": result.use_custom_hls_proxy,
                            "chain_custom_hls_proxy": result.chain_custom_hls_proxy,
                            "hls_proxy_path": result.hls_proxy_path
                            if result.hls_proxy_path
                            else "https://proxy.example.com/hls/[B64_URL].m3u8",
                        }
                    )
                    continue
                return_list.append(
                    {
                        "id": result.id,
                        "enabled": result.enabled,
                        "connections": result.connections,
                        "name": result.name,
                        "url": result.url,
                        "account_type": result.account_type,
                        "xc_username": result.xc_username,
                        "xc_password_set": bool(result.xc_password),
                        "user_agent": result.user_agent,
                        "use_hls_proxy": result.use_hls_proxy,
                        "use_custom_hls_proxy": result.use_custom_hls_proxy,
                        "chain_custom_hls_proxy": result.chain_custom_hls_proxy,
                        "hls_proxy_path": result.hls_proxy_path
                        if result.hls_proxy_path
                        else "https://proxy.example.com/hls/[B64_URL].m3u8",
                    }
                )
    return return_list


async def read_config_one_playlist(config, playlist_id):
    return_item = {}
    async with Session() as session:
        async with session.begin():
            query = await session.execute(
                select(Playlist).filter(Playlist.id == playlist_id)
            )
            result = query.scalar_one()

            if result:
                accounts_query = await session.execute(
                    select(XcAccount).where(XcAccount.playlist_id == result.id)
                )
                accounts = accounts_query.scalars().all()
                xc_accounts = [
                    {
                        "id": account.id,
                        "username": account.username,
                        "password_set": bool(account.password),
                        "enabled": account.enabled,
                        "connection_limit": account.connection_limit,
                        "label": account.label,
                    }
                    for account in accounts
                ]
                return_item = {
                    "id": result.id,
                    "enabled": result.enabled,
                    "name": result.name,
                    "url": result.url,
                    "connections": result.connections,
                    "account_type": result.account_type,
                    "xc_username": result.xc_username,
                    "xc_password_set": bool(result.xc_password),
                    "xc_accounts": xc_accounts,
                    "user_agent": result.user_agent,
                    "use_hls_proxy": result.use_hls_proxy,
                    "use_custom_hls_proxy": result.use_custom_hls_proxy,
                    "chain_custom_hls_proxy": result.chain_custom_hls_proxy,
                    "hls_proxy_path": result.hls_proxy_path
                    if result.hls_proxy_path
                    else "https://proxy.example.com/hls/[B64_URL].m3u8",
                }
    return return_item


async def add_new_playlist(config, data):
    async with Session() as session:
        async with session.begin():
            account_type = data.get("account_type", "M3U")
            playlist = Playlist(
                enabled=data.get("enabled"),
                name=data.get("name"),
                url=data.get("url"),
                account_type=account_type,
                xc_username=data.get("xc_username"),
                xc_password=data.get("xc_password"),
                connections=data.get("connections"),
                user_agent=data.get("user_agent"),
                use_hls_proxy=data.get("use_hls_proxy", False),
                use_custom_hls_proxy=data.get("use_custom_hls_proxy", False),
                chain_custom_hls_proxy=data.get("chain_custom_hls_proxy", False),
                hls_proxy_path=data.get(
                    "hls_proxy_path",
                    "https://proxy.example.com/hls/[B64_URL].m3u8",
                ),
            )
            # This is a new entry. Add it to the session before commit
            session.add(playlist)
            await session.flush()
            if account_type == XC_ACCOUNT_TYPE:
                await _upsert_xc_accounts(session, playlist, data)
    return playlist.id


async def update_playlist(config, playlist_id, data):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(
                select(Playlist).where(Playlist.id == playlist_id)
            )
            playlist = result.scalar_one()
            playlist.enabled = data.get("enabled", playlist.enabled)
            playlist.name = data.get("name", playlist.name)
            playlist.url = data.get("url", playlist.url)
            playlist.account_type = data.get("account_type", playlist.account_type)
            if "xc_username" in data:
                playlist.xc_username = data.get("xc_username")
            if data.get("xc_password"):
                playlist.xc_password = data.get("xc_password")
            playlist.connections = data.get("connections", playlist.connections)
            playlist.user_agent = data.get("user_agent", playlist.user_agent)
            playlist.use_hls_proxy = data.get("use_hls_proxy", playlist.use_hls_proxy)
            playlist.use_custom_hls_proxy = data.get(
                "use_custom_hls_proxy", playlist.use_custom_hls_proxy
            )
            playlist.chain_custom_hls_proxy = data.get(
                "chain_custom_hls_proxy", playlist.chain_custom_hls_proxy
            )
            playlist.hls_proxy_path = data.get(
                "hls_proxy_path", playlist.hls_proxy_path
            )
            if playlist.account_type == XC_ACCOUNT_TYPE:
                await _upsert_xc_accounts(session, playlist, data)


def _extract_xc_accounts_payload(data):
    return data.get("xc_accounts") or []


async def _upsert_xc_accounts(session, playlist, data):
    incoming = _extract_xc_accounts_payload(data)
    if not incoming and (data.get("xc_username") or data.get("xc_password")):
        incoming = [
            {
                "username": data.get("xc_username"),
                "password": data.get("xc_password"),
                "enabled": True,
                "connection_limit": 1,
            }
        ]
    existing_query = await session.execute(
        select(XcAccount).where(XcAccount.playlist_id == playlist.id)
    )
    existing = {account.id: account for account in existing_query.scalars().all()}
    keep_ids = set()
    total_connections = 0
    for account_data in incoming:
        account_id = account_data.get("id")
        username = account_data.get("username") or ""
        password = account_data.get("password") or ""
        enabled = bool(account_data.get("enabled", True))
        connection_limit = int(account_data.get("connection_limit") or 1)
        label = account_data.get("label")

        if account_id and account_id in existing:
            account = existing[account_id]
            account.username = username or account.username
            if password:
                account.password = password
            account.enabled = enabled
            account.connection_limit = connection_limit
            account.label = label
            keep_ids.add(account.id)
        else:
            if not username or not password:
                continue
            account = XcAccount(
                playlist_id=playlist.id,
                username=username,
                password=password,
                enabled=enabled,
                connection_limit=connection_limit,
                label=label,
            )
            session.add(account)
            await session.flush()
            keep_ids.add(account.id)

        if enabled:
            total_connections += connection_limit

    for account_id, account in existing.items():
        if account_id not in keep_ids:
            session.delete(account)

    playlist.connections = total_connections


async def delete_playlist(config, playlist_id):
    net_uuids = []
    async with Session() as session:
        async with session.begin():
            result = await session.execute(
                select(Playlist).where(Playlist.id == playlist_id)
            )
            playlist = result.scalar_one()
            if playlist.account_type == XC_ACCOUNT_TYPE:
                accounts_query = await session.execute(
                    select(XcAccount.tvh_uuid).where(
                        XcAccount.playlist_id == playlist.id
                    )
                )
                net_uuids = [row[0] for row in accounts_query.all() if row[0]]
            else:
                if playlist.tvh_uuid:
                    net_uuids = [playlist.tvh_uuid]
            # Remove cached copy of playlist
            cache_files = [
                os.path.join(
                    config.config_path, "cache", "playlists", f"{playlist_id}.m3u"
                ),
                os.path.join(
                    config.config_path, "cache", "playlists", f"{playlist_id}.yml"
                ),
            ]
            for f in cache_files:
                if os.path.isfile(f):
                    os.remove(f)
            # Remove from DB
            await session.delete(playlist)
    return net_uuids


def _resolve_user_agent(settings, user_agent):
    if user_agent:
        return user_agent
    defaults = settings.get("settings", {}).get("user_agents", [])
    if isinstance(defaults, list) and defaults:
        return defaults[0].get("value") or defaults[0].get("name")
    return "VLC/3.0.23 LibVLC/3.0.23"


async def download_playlist_file(settings, url, output, user_agent=None):
    logger.info("Downloading Playlist from url - '%s'", url)
    if not os.path.exists(os.path.dirname(output)):
        os.makedirs(os.path.dirname(output))
    headers = {
        "User-Agent": _resolve_user_agent(settings, user_agent),
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Connection": "keep-alive",
    }
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=300)
    last_error = None
    for attempt in range(1, 4):
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    url, headers=headers, allow_redirects=True
                ) as response:
                    if response.status < 200 or response.status >= 400:
                        # Some providers return non-standard status codes but still send a valid M3U.
                        peek = b""
                        try:
                            peek = await response.content.read(2048)
                        except Exception:
                            peek = b""
                        signature = peek.lstrip()[:7].upper()
                        if signature.startswith(b"#EXTM3U"):
                            logger.warning(
                                "Non-2xx status '%s' but playlist signature detected. Continuing download for url '%s'.",
                                response.status,
                                url,
                            )
                            async with aiofiles.open(output, "wb") as f:
                                if peek:
                                    await f.write(peek)
                                async for chunk in response.content.iter_chunked(8192):
                                    await f.write(chunk)
                            return output

                        body_preview = ""
                        try:
                            body_preview = (peek.decode(errors="ignore"))[:500]
                        except Exception:
                            body_preview = ""
                        logger.error(
                            "Failed to download playlist. status=%s reason='%s' url='%s' body_preview='%s'",
                            response.status,
                            response.reason,
                            url,
                            body_preview,
                        )
                        raise aiohttp.ClientResponseError(
                            response.request_info,
                            response.history,
                            status=response.status,
                            message=response.reason or "",
                            headers=response.headers,
                        )

                    async with aiofiles.open(output, "wb") as f:
                        async for chunk in response.content.iter_chunked(8192):
                            await f.write(chunk)
            return output
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            last_error = exc
            logger.warning(
                "Playlist download attempt %s/3 failed for url '%s': %s",
                attempt,
                url,
                exc,
            )
            if attempt < 3:
                await asyncio.sleep(attempt)
    raise last_error


async def store_playlist_streams(config, playlist_id):
    m3u_file = os.path.join(
        config.config_path, "cache", "playlists", f"{playlist_id}.m3u"
    )
    if not os.path.exists(m3u_file):
        logger.error("No such file '%s'", m3u_file)
        return False
    # Read cache file contents asynchronously
    async with aiofiles.open(m3u_file, mode="r", encoding="utf8", errors="ignore") as f:
        contents = await f.read()
    # noinspection PyPackageRequirements
    from ipytv import playlist

    pl = playlist.loads(contents)
    async with Session() as session:
        async with session.begin():
            # Delete all existing playlist streams
            stmt = delete(PlaylistStreams).where(
                PlaylistStreams.playlist_id == playlist_id
            )
            await session.execute(stmt)
            # Add an updated list of streams from the M3U file to the DB
            logger.info(
                "Updating list of available streams for playlist #%s from path - '%s'",
                playlist_id,
                m3u_file,
            )
            items = []
            for stream in pl:
                tvg_channel_number = stream.attributes.get("tvg-chno")
                items.append(
                    {
                        "playlist_id": playlist_id,
                        "name": stream.name,
                        "url": stream.url,
                        "channel_id": stream.attributes.get("channel-id"),
                        "group_title": stream.attributes.get("group-title"),
                        "tvg_chno": int(tvg_channel_number)
                        if tvg_channel_number is not None
                        else None,
                        "tvg_id": stream.attributes.get("tvg-id"),
                        "tvg_logo": stream.attributes.get("tvg-logo"),
                        "source_type": M3U_ACCOUNT_TYPE,
                        "xc_stream_id": None,
                        "xc_category_id": None,
                    }
                )
            # Perform bulk insert
            await session.execute(insert(PlaylistStreams), items)
            # Commit all updates to playlist sources
            await session.commit()
            logger.info(
                "Successfully imported %s streams from path - '%s'",
                len(items),
                m3u_file,
            )


def fetch_playlist_streams(playlist_id):
    return_list = {}
    for result in (
        db.session.query(PlaylistStreams)
        .where(PlaylistStreams.playlist_id == playlist_id)
        .all()
    ):
        return_list[result.name] = {
            "name": result.name,
            "url": result.url,
            "channel_id": result.channel_id,
            "group_title": result.group_title,
            "tvg_chno": result.tvg_chno,
            "tvg_id": result.tvg_id,
            "tvg_logo": result.tvg_logo,
            "source_type": result.source_type,
            "xc_stream_id": result.xc_stream_id,
        }
    return return_list


async def import_playlist_data(config, playlist_id):
    try:
        playlist_id = int(playlist_id)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid playlist id: {playlist_id}")
    settings = config.read_settings()
    async with Session() as session:
        async with session.begin():
            result = await session.execute(
                select(Playlist).where(Playlist.id == playlist_id)
            )
            playlist = result.scalar_one()

    if playlist.account_type == XC_ACCOUNT_TYPE:
        logger.info(
            "Updating XC playlist #%s from host - '%s'", playlist_id, playlist.url
        )
        await _import_xc_playlist_streams(settings, playlist)
        return

    # Download playlist data and save to YAML cache file
    logger.info(
        "Downloading updated M3U file for playlist #%s from url - '%s'",
        playlist_id,
        playlist.url,
    )
    start_time = time.time()
    m3u_file = os.path.join(
        config.config_path, "cache", "playlists", f"{playlist_id}.m3u"
    )
    await download_playlist_file(settings, playlist.url, m3u_file, playlist.user_agent)
    execution_time = time.time() - start_time
    logger.info(
        "Updated M3U file for playlist #%s was downloaded in '%s' seconds",
        playlist_id,
        int(execution_time),
    )
    # Parse the M3U file and cache the data in a YAML file for faster parsing
    logger.info("Importing updated data for playlist #%s", playlist_id)
    start_time = time.time()
    await store_playlist_streams(config, playlist_id)
    execution_time = time.time() - start_time
    logger.info(
        "Updated data for playlist #%s was imported in '%s' seconds",
        playlist_id,
        int(execution_time),
    )
    # Publish changes to TVH
    await publish_playlist_networks(config)


async def import_playlist_data_for_all_playlists(config):
    for playlist in db.session.query(Playlist).all():
        await import_playlist_data(config, playlist.id)


async def read_stream_details_from_all_playlists():
    playlist_streams = {"streams": []}
    primary_accounts = {}
    for account in (
        db.session.query(XcAccount)
        .filter(XcAccount.enabled.is_(True))
        .order_by(XcAccount.playlist_id.asc(), XcAccount.id.asc())
        .all()
    ):
        if account.playlist_id not in primary_accounts:
            primary_accounts[account.playlist_id] = account
    for result in db.session.query(PlaylistStreams).all():
        stream_url = result.url
        if result.source_type == XC_ACCOUNT_TYPE and result.xc_stream_id:
            account = primary_accounts.get(result.playlist_id)
            if account:
                stream_url = _render_xc_url(
                    _build_xc_url_template(
                        _normalize_xc_host(result.playlist.url),
                        result.xc_stream_id,
                        _extract_xc_suffix(result.url),
                    ),
                    account.username,
                    account.password,
                )
        playlist_streams["streams"].append(
            {
                "id": result.id,
                "playlist_id": result.playlist_id,
                "playlist_name": result.playlist.name,
                "name": result.name,
                "url": stream_url,
                "channel_id": result.channel_id,
                "group_title": result.group_title,
                "tvg_chno": result.tvg_chno,
                "tvg_id": result.tvg_id,
                "tvg_logo": result.tvg_logo,
                "source_type": result.source_type,
            }
        )
    return playlist_streams


def read_filtered_stream_details_from_all_playlists(
    request_json,
    *,
    base_url: str | None = None,
    instance_id: str | None = None,
    stream_key: str | None = None,
):
    results = {
        "streams": [],
        "records_total": 0,
        "records_filtered": 0,
    }
    primary_accounts = {}
    for account in (
        db.session.query(XcAccount)
        .filter(XcAccount.enabled.is_(True))
        .order_by(XcAccount.playlist_id.asc(), XcAccount.id.asc())
        .all()
    ):
        if account.playlist_id not in primary_accounts:
            primary_accounts[account.playlist_id] = account
    base_query = db.session.query(PlaylistStreams).options(joinedload(PlaylistStreams.playlist))
    # Get total records count
    results["records_total"] = base_query.count()
    # Build filters
    filters = []
    playlist_id = request_json.get("playlist_id")
    if playlist_id:
        filters.append(PlaylistStreams.playlist_id == playlist_id)
    group_title = request_json.get("group_title")
    if group_title:
        filters.append(PlaylistStreams.group_title == group_title)
    search_value = request_json.get("search_value")
    if search_value:
        playlist_rows = (
            db.session.query(Playlist)
            .where(Playlist.name.ilike(f"%{search_value}%"))
            .all()
        )
        filters.append(
            or_(
                PlaylistStreams.name.ilike(f"%{search_value}%"),
                PlaylistStreams.playlist_id.in_([p.id for p in playlist_rows]),
            )
        )
    query = base_query.filter(*filters)
    # Record filtered records count (distinct by playlist+URL)
    filtered_ids_query = (
        db.session.query(func.min(PlaylistStreams.id).label("id"))
        .filter(*filters)
        .group_by(PlaylistStreams.playlist_id, PlaylistStreams.url)
        .subquery()
    )
    results["records_filtered"] = (
        db.session.query(func.count()).select_from(filtered_ids_query).scalar()
    )

    # Get order by
    order_by_column = request_json.get("order_by")
    if not order_by_column:
        order_by_column = "name"
    if request_json.get("order_direction", "desc") == "asc":
        order_by = attrgetter(order_by_column)(PlaylistStreams).asc()
    else:
        order_by = attrgetter(order_by_column)(PlaylistStreams).desc()

    # Apply distinct-by-URL selection before pagination
    query = (
        db.session.query(PlaylistStreams)
        .join(filtered_ids_query, PlaylistStreams.id == filtered_ids_query.c.id)
        .order_by(order_by)
    )

    # Limit query results
    length = request_json.get("length", 0)
    start = request_json.get("start", 0)
    if length:
        query = query.limit(length).offset(start)
    # Fetch filtered results (already deduped by URL and playlist)
    for result in query.all():
        stream_url = result.url
        if result.source_type == XC_ACCOUNT_TYPE and result.xc_stream_id:
            account = primary_accounts.get(result.playlist_id)
            if account:
                stream_url = _render_xc_url(
                    _build_xc_url_template(
                        _normalize_xc_host(result.playlist.url),
                        result.xc_stream_id,
                        _extract_xc_suffix(result.url),
                    ),
                    account.username,
                    account.password,
                )
        playlist_info = result.playlist
        if playlist_info:
            stream_url = build_configured_hls_proxy_url(
                stream_url,
                base_url=base_url,
                instance_id=instance_id,
                stream_key=stream_key,
                use_hls_proxy=playlist_info.use_hls_proxy,
                use_custom_hls_proxy=playlist_info.use_custom_hls_proxy,
                custom_hls_proxy_path=playlist_info.hls_proxy_path,
                chain_custom_hls_proxy=playlist_info.chain_custom_hls_proxy,
            )
        results["streams"].append(
            {
                "id": result.id,
                "playlist_id": result.playlist_id,
                "playlist_name": result.playlist.name,
                "name": result.name,
                "url": stream_url,
                "channel_id": result.channel_id,
                "group_title": result.group_title,
                "tvg_chno": result.tvg_chno,
                "tvg_id": result.tvg_id,
                "tvg_logo": result.tvg_logo,
                "source_type": result.source_type,
            }
        )
    return results


async def delete_playlist_network_in_tvh(config, net_uuid):
    async with await get_tvh(config) as tvh:
        await tvh.delete_network(net_uuid)


async def publish_playlist_networks(config):
    logger.info("Publishing all playlist networks to TVH")
    async with await get_tvh(config) as tvh:
        # Loop over configured playlists
        existing_uuids = []
        net_priority = 0
        for result in db.session.query(Playlist).all():
            if result.account_type == XC_ACCOUNT_TYPE:
                accounts = (
                    db.session.query(XcAccount)
                    .filter(XcAccount.playlist_id == result.id)
                    .order_by(XcAccount.id.asc())
                    .all()
                )
                for account in accounts:
                    net_priority += 1
                    net_uuid = account.tvh_uuid
                    account_label = account.label or account.username or f"xc-{account.id}"
                    playlist_name = f"{result.name} ({account_label})"
                    max_streams = account.connection_limit
                    playlist_slug = (playlist_name or f"playlist_{result.id}").strip()
                    if not playlist_slug:
                        playlist_slug = f"playlist_{result.id}"
                    if playlist_slug.lower().startswith("tic-"):
                        tic_name = playlist_slug
                    else:
                        tic_name = f"tic-{playlist_slug}"
                    network_name = f"{tic_name}-xc-{account.id}"
                    logger.info("Publishing XC playlist to TVH - %s.", network_name)
                    if net_uuid:
                        found = False
                        for net in await tvh.list_cur_networks():
                            if net.get("uuid") == net_uuid:
                                found = True
                        if not found:
                            net_uuid = None
                    if not net_uuid:
                        net_uuid = await tvh.create_network(
                            tic_name, network_name, max_streams, net_priority
                        )
                    net_conf = network_template.copy()
                    net_conf["uuid"] = net_uuid
                    net_conf["enabled"] = bool(result.enabled and account.enabled)
                    net_conf["networkname"] = tic_name
                    net_conf["pnetworkname"] = network_name
                    net_conf["max_streams"] = max_streams
                    net_conf["priority"] = net_priority
                    await tvh.idnode_save(net_conf)
                    account.tvh_uuid = net_uuid
                    db.session.commit()
                    existing_uuids.append(net_uuid)
                continue
            net_priority += 1
            net_uuid = result.tvh_uuid
            playlist_name = result.name
            max_streams = result.connections
            playlist_slug = (playlist_name or f"playlist_{result.id}").strip()
            if not playlist_slug:
                playlist_slug = f"playlist_{result.id}"
            if playlist_slug.lower().startswith("tic-"):
                tic_name = playlist_slug
            else:
                tic_name = f"tic-{playlist_slug}"
            network_name = tic_name
            logger.info("Publishing playlist to TVH - %s.", network_name)
            if net_uuid:
                found = False
                for net in await tvh.list_cur_networks():
                    if net.get("uuid") == net_uuid:
                        found = True
                if not found:
                    net_uuid = None
            if not net_uuid:
                # No network exists, create one
                # Check if network exists with this playlist name
                net_uuid = await tvh.create_network(
                    tic_name, network_name, max_streams, net_priority
                )
            # Update network
            net_conf = network_template.copy()
            net_conf["uuid"] = net_uuid
            net_conf["enabled"] = result.enabled
            net_conf["networkname"] = tic_name
            net_conf["pnetworkname"] = network_name
            net_conf["max_streams"] = max_streams
            net_conf["priority"] = net_priority
            await tvh.idnode_save(net_conf)
            # Save network UUID against playlist in settings
            result.tvh_uuid = net_uuid
            db.session.commit()
            # Append to list of current network UUIDs
            existing_uuids.append(net_uuid)

        #  TODO: Remove any networks that are not managed. DONT DO THIS UNTIL THINGS ARE ALL WORKING!


async def probe_playlist_stream(playlist_stream_id):
    playlist_stream = (
        db.session.query(PlaylistStreams)
        .where(PlaylistStreams.id == playlist_stream_id)
        .one()
    )
    stream_url = playlist_stream.url
    if playlist_stream.source_type == XC_ACCOUNT_TYPE and playlist_stream.xc_stream_id:
        account = _get_primary_xc_account_sync(playlist_stream.playlist_id)
        if account:
            stream_url = _render_xc_url(
                _build_xc_url_template(
                    _normalize_xc_host(playlist_stream.playlist.url),
                    playlist_stream.xc_stream_id,
                    _extract_xc_suffix(playlist_stream.url),
                ),
                account.username,
                account.password,
            )
    probe_data = await ffprobe_file(stream_url)
    return probe_data


async def get_playlist_groups(
    config,
    playlist_id,
    start=0,
    length=10,
    search_value="",
    order_by="name",
    order_direction="asc",
):
    """
    Get all groups from a specific playlist with filtering, sorting and pagination
    """
    async with Session() as session:
        async with session.begin():
            # Get distinct group count query (this is what needs to be fixed)
            distinct_groups_count_query = select(func.count()).select_from(
                select(PlaylistStreams.group_title)
                .filter(
                    PlaylistStreams.playlist_id == playlist_id,
                    PlaylistStreams.group_title != None,
                    PlaylistStreams.group_title != "",
                )
                .group_by(PlaylistStreams.group_title)
                .subquery()
            )

            # Get the total count of unique groups
            total_groups = await session.scalar(distinct_groups_count_query)

            # Apply search filter to count if provided
            if search_value:
                filtered_groups_count_query = select(func.count()).select_from(
                    select(PlaylistStreams.group_title)
                    .filter(
                        PlaylistStreams.playlist_id == playlist_id,
                        PlaylistStreams.group_title != None,
                        PlaylistStreams.group_title != "",
                        PlaylistStreams.group_title.ilike(f"%{search_value}%"),
                    )
                    .group_by(PlaylistStreams.group_title)
                    .subquery()
                )
                filtered_groups = await session.scalar(filtered_groups_count_query)
            else:
                filtered_groups = total_groups

            # Get distinct group names and count channels in each group
            groups_query = select(
                PlaylistStreams.group_title.label("name"),
                func.count(PlaylistStreams.id).label("channel_count"),
            ).filter(
                PlaylistStreams.playlist_id == playlist_id,
                PlaylistStreams.group_title != None,  # Exclude streams without group
                PlaylistStreams.group_title != "",  # Exclude streams with empty group
            )

            # Apply search filter to groups
            if search_value:
                groups_query = groups_query.filter(
                    PlaylistStreams.group_title.ilike(f"%{search_value}%")
                )

            # Group by group name
            groups_query = groups_query.group_by(PlaylistStreams.group_title)

            # Apply ordering
            if order_by == "name":
                if order_direction.lower() == "desc":
                    groups_query = groups_query.order_by(
                        PlaylistStreams.group_title.desc()
                    )
                else:
                    groups_query = groups_query.order_by(
                        PlaylistStreams.group_title.asc()
                    )
            elif order_by == "channel_count":
                if order_direction.lower() == "desc":
                    groups_query = groups_query.order_by(
                        func.count(PlaylistStreams.id).desc()
                    )
                else:
                    groups_query = groups_query.order_by(
                        func.count(PlaylistStreams.id).asc()
                    )

            # Apply pagination (length=0 means no limit)
            if length:
                groups_query = groups_query.offset(start).limit(length)

            # Execute query
            result = await session.execute(groups_query)
            groups = result.all()

            # Format result
            group_list = []
            for group in groups:
                group_list.append(
                    {
                        "name": group.name or "Unknown",
                        "channel_count": group.channel_count,
                    }
                )

            return {
                "groups": group_list,
                "total": total_groups,
                "records_filtered": filtered_groups,
            }
