#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import base64
import logging
import os
import re
import time
from collections import OrderedDict
from mimetypes import guess_type
from urllib.parse import urlparse

import aiofiles
import aiohttp
import requests
from sqlalchemy import and_, delete, func, select
from sqlalchemy.orm import joinedload, selectinload

from backend.ffmpeg import generate_iptv_url
from backend.models import (
    Channel,
    ChannelSource,
    ChannelTag,
    Epg,
    EpgChannels,
    Playlist,
    PlaylistStreams,
    Recording,
    RecordingRule,
    Session,
    channels_tags_association_table,
    db,
)
from backend.playlists import fetch_playlist_streams
from backend.streaming import append_stream_key, get_tvh_stream_auth, is_local_hls_proxy_url
from backend.tvheadend.tvh_requests import get_tvh
from backend.utils import normalize_id

logger = logging.getLogger("tic.channels")

image_placeholder_base64 = "iVBORw0KGgoAAAANSUhEUgAAAGQAAABkCAIAAAD/gAIDAAACiUlEQVR4nO2cy46sMAxEzdX8/y/3XUSKUM+I7sLlF6qzYgE4HJwQEsLxer1MfMe/6gJMQrIAJAtAsgAkC0CyACQLQLIAJAtAsgAkC0CyACQLQLIAJAtAsgAkC0CyACQL4Md5/HEclFH84ziud+gwV+C61H2F905yFvTxDNDOQXjz4p6vddTt0M7Db0OoRN/7cmZi6Nm+iphWblbrlnnm90CsMBe+EmpNTsVk3pM/faXd9oRYzH7WLui2lmlqFeBjF8QD/2LKn/Fxd4jfgy/vPcblV+zrTmiluCDIF1/WqgW/269kInyRZZ3bi+f5Ysr63bI+zFf4EE25LyI0WRcP7FpfxOTiyPrYtXmGr7yR0gfUx9Rh5em+CLKg14sqX5SaWDBhMTe/vLLuvbWW+PInV9lU2MT8qpw3HOereJJ1li+XLMowW6YvZ7PVYvp+Sn61kGVDfHWRZRN8NZJl7X31kmW9fbWTZY19dZRlXX01lWUtffWVZf18tZZlzXy5ZEV/iLGjrA1/LOf7WffMWjTJrxmyrIevMbKsgS+vrJxm6xxubdwI6h9QmpRZi8L8IshKTi675YsyTjkvsxYl+TVVllX44sjKr4k77tq4js76JJeWWW19ET9eHlwNN2n1kbxooPBzyLXxVgDuN/HkzGrli756IGQxQvIqlLfQe5tehpA2q0N+RRDVwBf62nRfNHCmxFfo+o7YrkOyr+j1HRktceFKVvKq7AcsM70+M9FX6jO+avU9K25Nhyj/vw4UX2W9R0v/Y4jfV6WsMzn/ovH+D6aJrDQ8z5knDNFAPH9GugmSBSBZAJIFIFkAkgUgWQCSBSBZAJIFIFkAkgUgWQCSBSBZAJIFIFkA/wGlHK2X7Li2TQAAAABJRU5ErkJggg=="


# Simple in-process LRU cache for expensive list operations (not persistent, per worker)
class LRUCache:
    def __init__(self, max_size=32):
        self.max_size = max_size
        self.store = OrderedDict()

    def get(self, key):
        if key not in self.store:
            return None
        value, expires_at = self.store[key]
        if expires_at and expires_at < time.time():
            # Expired
            del self.store[key]
            return None
        # Move to end (recently used)
        self.store.move_to_end(key)
        return value

    def set(self, key, value, ttl=60):
        if key in self.store:
            self.store.move_to_end(key)
        self.store[key] = (value, time.time() + ttl if ttl else None)
        if len(self.store) > self.max_size:
            self.store.popitem(last=False)


_list_cache = LRUCache(max_size=8)


async def read_config_all_channels(
    filter_playlist_ids=None, output_for_export=False, include_status=False
):
    if filter_playlist_ids is None:
        filter_playlist_ids = []

    return_list = []

    async with Session() as session:
        async with session.begin():
            result = await session.execute(
                select(Channel)
                .options(
                    joinedload(Channel.tags),
                    joinedload(Channel.sources).subqueryload(ChannelSource.playlist),
                )
                .order_by(Channel.id)
            )
            channels = result.scalars().unique().all()

            for result in channels:
                tags = [tag.name for tag in result.tags]
                sources = []
                for source in result.sources:
                    # If filtering on playlist IDs, then only return sources from that playlist
                    if (
                        filter_playlist_ids
                        and source.playlist_id not in filter_playlist_ids
                    ):
                        continue
                    playlist_name = (
                        source.playlist.name if source.playlist else "Manual URL"
                    )
                    source_type = "playlist" if source.playlist_id else "manual"
                    if output_for_export:
                        sources.append(
                            {
                                "playlist_name": playlist_name,
                                "priority": source.priority,
                                "stream_name": source.playlist_stream_name,
                            }
                        )
                        continue
                    source_payload = {
                        "id": source.id,
                        "playlist_id": source.playlist_id,
                        "playlist_name": playlist_name,
                        "priority": source.priority,
                        "stream_name": source.playlist_stream_name,
                        "stream_url": source.playlist_stream_url,
                        "use_hls_proxy": bool(getattr(source, "use_hls_proxy", False)),
                        "source_type": source_type,
                    }
                    if include_status:
                        source_payload["tvh_uuid"] = source.tvh_uuid
                        source_payload["playlist_enabled"] = (
                            bool(source.playlist.enabled)
                            if source.playlist_id
                            else True
                        )
                    sources.append(source_payload)
                # Filter out this channel if we have provided a playlist ID filter list and no sources were found
                if filter_playlist_ids and not sources:
                    continue

                if output_for_export:
                    return_list.append(
                        {
                            "enabled": result.enabled,
                            "name": result.name,
                            "logo_url": result.logo_url,
                            "number": result.number,
                            "tags": tags,
                            "guide": {
                                "epg_name": result.guide_name,
                                "channel_id": result.guide_channel_id,
                            },
                            "sources": sources,
                        }
                    )
                    continue
                channel_payload = {
                    "id": result.id,
                    "enabled": result.enabled,
                    "tvh_uuid": result.tvh_uuid,
                    "name": result.name,
                    "logo_url": result.logo_url,
                    "number": result.number,
                    "tags": tags,
                    "guide": {
                        "epg_id": result.guide_id,
                        "epg_name": result.guide_name,
                        "channel_id": result.guide_channel_id,
                    },
                    "sources": sources,
                }
                return_list.append(channel_payload)

    return return_list


def read_config_one_channel(channel_id):
    return_item = {}
    channel_id = normalize_id(channel_id, "channel")
    result = (
        db.session.query(Channel)
        .options(
            joinedload(Channel.tags),
            joinedload(Channel.sources).subqueryload(ChannelSource.playlist),
        )
        .filter(Channel.id == channel_id)
        .order_by(Channel.id)
        .one()
    )
    if result:
        tags = []
        for tag in result.tags:
            tags.append(tag.name)
        sources = []
        for source in result.sources:
            playlist_name = source.playlist.name if source.playlist else "Manual URL"
            source_type = "playlist" if source.playlist_id else "manual"
            sources.append(
                {
                    "id": source.id,
                    "playlist_id": source.playlist_id,
                    "playlist_name": playlist_name,
                    "priority": source.priority,
                    "stream_name": source.playlist_stream_name,
                    "stream_url": source.playlist_stream_url,
                    "use_hls_proxy": bool(getattr(source, "use_hls_proxy", False)),
                    "source_type": source_type,
                }
            )
        return_item = {
            "id": result.id,
            "enabled": result.enabled,
            "name": result.name,
            "logo_url": result.logo_url,
            "number": result.number,
            "tags": tags,
            "guide": {
                "epg_id": result.guide_id,
                "epg_name": result.guide_name,
                "channel_id": result.guide_channel_id,
            },
            "sources": sources,
        }
    return return_item


def get_channel_image_path(config, channel_id):
    return os.path.join(
        config.config_path, "cache", "logos", f"channel_logo_{channel_id}"
    )


async def download_image_to_base64(image_source, timeout=10):
    # Image source is a URL
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as session:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36 Edg/96.0.1054.62"
            }
            async with session.get(image_source, headers=headers) as response:
                response.raise_for_status()
                image_data = await response.read()
                mime_type = response.headers.get(
                    "Content-Type", "image/jpeg"
                )  # Fallback to JPEG if no content-type header

                image_base64_string = base64.b64encode(image_data).decode()
    except Exception as e:
        logger.error("An error occurred while downloading image: %s", e)
        mime_type = "image/png"
        image_base64_string = image_placeholder_base64

    return image_base64_string, mime_type


async def parse_image_as_base64(image_source):
    try:
        if image_source.startswith("data:image/"):
            mime_type = image_source.split(";")[0].split(":")[1]
            image_base64_string = image_source.split("base64,")[1]
        elif image_source.startswith("http://") or image_source.startswith("https://"):
            image_base64_string, mime_type = await download_image_to_base64(
                image_source, timeout=3
            )
        else:
            # Handle other cases or raise an error
            raise ValueError("Unsupported image source format")
    except Exception as e:
        logger.error("An error occurred while updating channel image: %s", e)
        # Return the placeholder image
        mime_type = "image/png"
        image_base64_string = image_placeholder_base64

    # Prepend the MIME type and base64 header and return
    base64_string_with_header = f"data:{mime_type};base64,{image_base64_string}"
    return base64_string_with_header


async def read_base46_image_string(base64_string):
    # Extract the MIME type and decode the base64 string
    try:
        mime_type = base64_string.split(";")[0].split(":")[1]
        image_data = base64.b64decode(base64_string.split(",")[1])
    except Exception as e:
        logger.error("An error occurred while parsing base64 image string: %s", e)
        mime_type = None
        image_data = None
    return image_data, mime_type


async def read_channel_logo(channel_id):
    channel = db.session.query(Channel).where(Channel.id == channel_id).one()
    base64_string = channel.logo_base64
    if not base64_string:
        # Revert to using the logo url
        base64_string, mime_type = await download_image_to_base64(channel.logo_url)
    image_base64_string, mime_type = await read_base46_image_string(base64_string)
    return image_base64_string, mime_type


async def add_new_channel(config, data, commit=True, publish=True):
    """Create Channel row and optionally publish immediately to TVHeadend.

    Returns the Channel ORM object (with tvh_uuid if published).
    """
    settings = config.read_settings()
    instance_id = config.ensure_instance_id()
    channel = Channel(
        enabled=data.get("enabled"),
        name=data.get("name"),
        logo_url=data.get("logo_url"),
        number=data.get("number"),
    )
    # Tags
    for tag_name in data.get("tags", []):
        channel_tag = (
            db.session.query(ChannelTag)
            .filter(ChannelTag.name == tag_name)
            .one_or_none()
        )
        if not channel_tag:
            channel_tag = ChannelTag(name=tag_name)
            db.session.add(channel_tag)
        channel.tags.append(channel_tag)

    # Programme Guide
    guide_info = data.get("guide", {})
    if guide_info.get("epg_id"):
        channel_guide_source = (
            db.session.query(Epg).filter(Epg.id == guide_info["epg_id"]).one()
        )
        channel.guide_id = channel_guide_source.id
        channel.guide_name = channel_guide_source.name
        channel.guide_channel_id = guide_info["channel_id"]

    # Sources
    new_sources = []
    for source_info in data.get("sources", []):
        is_manual = source_info.get("source_type") == "manual" or not source_info.get(
            "playlist_id"
        )
        if is_manual:
            stream_url = (source_info.get("stream_url") or "").strip()
            if not stream_url:
                continue
            channel_source = ChannelSource(
                playlist_id=None,
                playlist_stream_name=source_info.get("stream_name") or "Manual URL",
                playlist_stream_url=stream_url,
                use_hls_proxy=bool(source_info.get("use_hls_proxy", False)),
            )
            new_sources.append(channel_source)
            continue

        playlist_info = (
            db.session.query(Playlist)
            .filter(Playlist.id == source_info["playlist_id"])
            .one()
        )
        playlist_streams = fetch_playlist_streams(playlist_info.id)
        playlist_stream = playlist_streams.get(source_info["stream_name"])
        if playlist_info.use_hls_proxy:
            if not playlist_info.use_custom_hls_proxy:
                app_url = settings["settings"]["app_url"]
                playlist_url = playlist_stream["url"]
                # Derive extension from URL path (query strings may be present).
                extension = "m3u8" if urlparse(playlist_url).path.lower().endswith(".m3u8") else "ts"
                encoded_url = base64.urlsafe_b64encode(
                    playlist_url.encode("utf-8")
                ).decode("utf-8")
                playlist_stream["url"] = (
                    f"{app_url}/tic-hls-proxy/{instance_id}/{encoded_url}.{extension}"
                )
            else:
                hls_proxy_path = playlist_info.hls_proxy_path
                playlist_url = playlist_stream["url"]
                encoded_url = base64.urlsafe_b64encode(
                    playlist_url.encode("utf-8")
                ).decode("utf-8")
                hls_proxy_path = hls_proxy_path.replace("[URL]", playlist_url)
                hls_proxy_path = hls_proxy_path.replace("[B64_URL]", encoded_url)
                playlist_stream["url"] = hls_proxy_path
        channel_source = ChannelSource(
            playlist_id=playlist_info.id,
            playlist_stream_name=source_info["stream_name"],
            playlist_stream_url=playlist_stream["url"],
        )
        new_sources.append(channel_source)
    if new_sources:
        channel.sources = new_sources

    db.session.add(channel)

    if publish:
        try:
            async with await get_tvh(config) as tvh:
                channel_uuid = await publish_channel_to_tvh(tvh, channel)
                channel.tvh_uuid = channel_uuid
        except Exception as e:
            logger.error(
                "Immediate publish failed for channel '%s': %s", channel.name, e
            )

    if commit:
        db.session.commit()
    return channel


async def update_channel(config, channel_id, data):
    settings = config.read_settings()
    instance_id = config.ensure_instance_id()
    async with Session() as session:
        async with session.begin():
            query = await session.execute(
                select(Channel)
                .where(Channel.id == channel_id)
                .options(selectinload(Channel.tags), selectinload(Channel.sources))
            )
            channel = query.scalar_one()
            channel.enabled = data.get("enabled")
            channel.name = data.get("name")
            channel.logo_url = data.get("logo_url")
            channel.number = data.get("number")

            # Category Tags
            # -- Remove existing tags
            channel.tags.clear()
            # -- Add tags
            new_tags = []
            for tag_name in data.get("tags", []):
                query = await session.execute(
                    select(ChannelTag).filter(ChannelTag.name == tag_name)
                )
                channel_tag = query.scalar_one_or_none()
                if not channel_tag:
                    channel_tag = ChannelTag(name=tag_name)
                    session.add(channel_tag)
                new_tags.append(channel_tag)
            channel.tags = new_tags

            # Programme Guide
            guide_info = data.get("guide", {})
            if guide_info.get("epg_id"):
                query = await session.execute(
                    select(Epg).filter(Epg.id == guide_info["epg_id"])
                )
                channel_guide_source = query.scalar_one_or_none()
                if channel_guide_source:
                    channel.guide_id = channel_guide_source.id
                    channel.guide_name = guide_info.get("epg_name")
                    channel.guide_channel_id = guide_info.get("channel_id")
                else:
                    channel.guide_id = None
                    channel.guide_name = None
                    channel.guide_channel_id = None

            # Sources
            new_source_ids = []
            new_sources = []
            priority = len(data.get("sources", []))
            logger.info("Updating channel sources")
            for source_info in data.get("sources", []):
                source_id = source_info.get("id")
                is_manual = source_info.get(
                    "source_type"
                ) == "manual" or not source_info.get("playlist_id")
                channel_source = None
                if source_id:
                    query = await session.execute(
                        select(ChannelSource).filter(ChannelSource.id == source_id)
                    )
                    channel_source = query.scalar_one_or_none()
                if not channel_source and not is_manual:
                    query = await session.execute(
                        select(ChannelSource).filter(
                            and_(
                                ChannelSource.channel_id == channel.id,
                                ChannelSource.playlist_id == source_info["playlist_id"],
                                ChannelSource.playlist_stream_name
                                == source_info["stream_name"],
                            )
                        )
                    )
                    channel_source = query.scalar_one_or_none()
                if not channel_source and is_manual:
                    query = await session.execute(
                        select(ChannelSource).filter(
                            and_(
                                ChannelSource.channel_id == channel.id,
                                ChannelSource.playlist_id.is_(None),
                                ChannelSource.playlist_stream_url
                                == source_info.get("stream_url"),
                            )
                        )
                    )
                    channel_source = query.scalar_one_or_none()

                if is_manual:
                    stream_url = (source_info.get("stream_url") or "").strip()
                    if not stream_url:
                        continue
                    if not channel_source:
                        logger.info(
                            "    - Adding new manual channel source for channel %s",
                            channel.name,
                        )
                        channel_source = ChannelSource(
                            playlist_id=None,
                            playlist_stream_name=source_info.get("stream_name")
                            or "Manual URL",
                            playlist_stream_url=stream_url,
                            use_hls_proxy=bool(source_info.get("use_hls_proxy", False)),
                        )
                    else:
                        logger.info(
                            "    - Updating manual channel source for channel %s",
                            channel.name,
                        )
                        channel_source.playlist_stream_name = (
                            source_info.get("stream_name") or "Manual URL"
                        )
                        channel_source.playlist_stream_url = stream_url
                        channel_source.use_hls_proxy = bool(
                            source_info.get("use_hls_proxy", False)
                        )
                    if channel_source.id:
                        new_source_ids.append(channel_source.id)
                else:
                    if not channel_source:
                        logger.info(
                            "    - Adding new channel source for channel %s 'Playlist:%s - %s'",
                            channel.name,
                            source_info["playlist_id"],
                            source_info["stream_name"],
                        )

                        query = await session.execute(
                            select(Playlist).filter(
                                Playlist.id == source_info["playlist_id"]
                            )
                        )
                        playlist_info = query.scalar_one()
                        playlist_streams = fetch_playlist_streams(playlist_info.id)
                        playlist_stream = playlist_streams.get(
                            source_info["stream_name"]
                        )
                        if playlist_info.use_hls_proxy:
                            if not playlist_info.use_custom_hls_proxy:
                                app_url = settings["settings"]["app_url"]
                                playlist_url = playlist_stream["url"]
                                # Derive extension from URL path (query strings may be present).
                                extension = "m3u8" if urlparse(playlist_url).path.lower().endswith(".m3u8") else "ts"
                                encoded_playlist_url = base64.urlsafe_b64encode(
                                    playlist_url.encode("utf-8")
                                ).decode("utf-8")
                                # noinspection HttpUrlsUsage
                                playlist_stream["url"] = (
                                    f"{app_url}/tic-hls-proxy/{instance_id}/{encoded_playlist_url}.{extension}"
                                )
                            else:
                                hls_proxy_path = playlist_info.hls_proxy_path
                                playlist_url = playlist_stream["url"]
                                encoded_playlist_url = base64.urlsafe_b64encode(
                                    playlist_url.encode("utf-8")
                                ).decode("utf-8")
                                hls_proxy_path = hls_proxy_path.replace(
                                    "[URL]", playlist_url
                                )
                                hls_proxy_path = hls_proxy_path.replace(
                                    "[B64_URL]", encoded_playlist_url
                                )
                                playlist_stream["url"] = hls_proxy_path
                        channel_source = ChannelSource(
                            playlist_id=playlist_info.id,
                            playlist_stream_name=source_info["stream_name"],
                            playlist_stream_url=playlist_stream["url"],
                        )
                    else:
                        logger.info(
                            "    - Found existing channel source for channel %s 'Playlist:%s - %s'",
                            channel.name,
                            source_info["playlist_id"],
                            source_info["stream_name"],
                        )
                        new_source_ids.append(channel_source.id)
                        # Filter sources to refresh here. Things not added to the new_source_ids list are removed and re-added
                        for refresh_source_info in data.get("refresh_sources", []):
                            if (
                                refresh_source_info["playlist_id"]
                                == source_info["playlist_id"]
                                and refresh_source_info["stream_name"]
                                == source_info["stream_name"]
                            ):
                                logger.info(
                                    "    - Channel %s source marked for refresh 'Playlist:%s - %s'",
                                    channel.name,
                                    source_info["playlist_id"],
                                    source_info["stream_name"],
                                )
                                query = await session.execute(
                                    select(Playlist).filter(
                                        Playlist.id == source_info["playlist_id"]
                                    )
                                )
                                playlist_info = query.scalar_one()
                                playlist_streams = fetch_playlist_streams(
                                    playlist_info.id
                                )
                                playlist_stream = playlist_streams.get(
                                    source_info["stream_name"]
                                )
                                if playlist_info.use_hls_proxy:
                                    if not playlist_info.use_custom_hls_proxy:
                                        app_url = settings["settings"]["app_url"]
                                        playlist_url = playlist_stream["url"]
                                        # Derive extension from URL path (query strings may be present).
                                        extension = "m3u8" if urlparse(playlist_url).path.lower().endswith(".m3u8") else "ts"
                                        encoded_playlist_url = base64.urlsafe_b64encode(
                                            playlist_url.encode("utf-8")
                                        ).decode("utf-8")
                                        # noinspection HttpUrlsUsage
                                        playlist_stream["url"] = (
                                            f"{app_url}/tic-hls-proxy/{instance_id}/{encoded_playlist_url}.{extension}"
                                        )
                                    else:
                                        hls_proxy_path = playlist_info.hls_proxy_path
                                        playlist_url = playlist_stream["url"]
                                        encoded_playlist_url = base64.urlsafe_b64encode(
                                            playlist_url.encode("utf-8")
                                        ).decode("utf-8")
                                        hls_proxy_path = hls_proxy_path.replace(
                                            "[URL]", playlist_url
                                        )
                                        hls_proxy_path = hls_proxy_path.replace(
                                            "[B64_URL]", encoded_playlist_url
                                        )
                                        playlist_stream["url"] = hls_proxy_path
                                # Update playlist stream url
                                logger.info(
                                    "    - Updating channel %s source from '%s' to '%s'",
                                    channel.name,
                                    channel_source.playlist_stream_url,
                                    playlist_stream["url"],
                                )
                                channel_source.playlist_stream_url = playlist_stream[
                                    "url"
                                ]
                                break
                # Update source priority (higher means higher priority)
                channel_source.priority = str(priority)
                priority -= 1
                # Append to list of new sources
                new_sources.append(channel_source)
            # Remove all old entries in the channel_sources table
            query = await session.execute(
                select(ChannelSource).filter_by(channel_id=channel.id)
            )
            current_sources = query.scalars().all()
            for source in current_sources:
                if source.id not in new_source_ids:
                    if source.tvh_uuid:
                        # Delete mux from TVH
                        await delete_channel_muxes(config, source.tvh_uuid)
                    await session.delete(source)
            if new_sources:
                channel.sources.clear()
                channel.sources = new_sources

            # Commit
            await session.commit()


async def add_bulk_channels(config, data):
    channel_number = db.session.query(func.max(Channel.number)).scalar()
    if channel_number is None:
        channel_number = 999

    added_channel_count = 0
    skipped_channel_count = 0

    new_channels = []
    for channel in data:
        # Fetch the playlist channel by ID
        playlist_stream = (
            db.session.query(PlaylistStreams)
            .where(PlaylistStreams.id == channel["stream_id"])
            .one()
        )

        # Check if Channel with this name already exists
        existing_channel = (
            db.session.query(Channel)
            .filter(Channel.name == playlist_stream.name.strip())
            .first()
        )

        if existing_channel:
            logger.info(f"Channel '{playlist_stream.name}' already exists, skipping")
            skipped_channel_count += 1
            continue

        # Make this new channel the next highest
        channel_number = channel_number + 1
        # Build new channel data
        new_channel_data = {
            "enabled": True,
            "tags": [],
            "sources": [],
        }
        # Auto assign the name
        new_channel_data["name"] = playlist_stream.name.strip()
        # Auto assign the image URL
        new_channel_data["logo_url"] = playlist_stream.tvg_logo
        # Auto assign the channel number to the next available number
        new_channel_data["number"] = int(channel_number)

        # Add group title as tag if it exists
        if playlist_stream.group_title and playlist_stream.group_title.strip():
            new_channel_data["tags"].append(playlist_stream.group_title.strip())

        # Find the best match for an EPG
        epg_match = (
            db.session.query(EpgChannels)
            .filter(EpgChannels.channel_id == playlist_stream.tvg_id)
            .first()
        )
        if epg_match is not None:
            new_channel_data["guide"] = {
                "channel_id": epg_match.channel_id,
                "epg_id": epg_match.epg_id,
                "epg_name": epg_match.name,
            }
        # Apply the stream to the channel
        new_channel_data["sources"].append(
            {
                "playlist_id": channel["playlist_id"],
                "playlist_name": playlist_stream.playlist.name,
                "stream_name": playlist_stream.name,
            }
        )
        channel_obj = await add_new_channel(
            config, new_channel_data, commit=False, publish=False
        )
        new_channels.append(channel_obj)
        added_channel_count += 1

    # Commit all new channels
    db.session.commit()

    # Batch publish
    try:
        await batch_publish_new_channels_to_tvh(config, new_channels)
    except Exception as e:
        logger.error("Batch publish (bulk) failed: %s", e)

    logger.info(
        f"Successfully added {added_channel_count} channels (skipped {skipped_channel_count} existing channels)"
    )
    return added_channel_count


async def delete_channel(config, channel_id):
    async with Session() as session:
        async with session.begin():
            # Use select() instead of query()
            result = await session.execute(
                select(Channel).where(Channel.id == channel_id)
            )
            channel = result.scalar_one_or_none()
            if channel is None:
                logger.warning("delete_channel: channel id %s not found", channel_id)
                return False

            # Delete recordings and rules tied to this channel to avoid FK violations
            await session.execute(
                delete(Recording).where(Recording.channel_id == channel.id)
            )
            await session.execute(
                delete(RecordingRule).where(RecordingRule.channel_id == channel.id)
            )

            # Remove channel from TVHeadend if it has a UUID
            if channel.tvh_uuid:
                async with await get_tvh(config) as tvh:
                    logger.info(
                        f"Removing channel '{channel.name}' (UUID: {channel.tvh_uuid}) from TVHeadend"
                    )
                    try:
                        await tvh.delete_channel(channel.tvh_uuid)
                    except Exception as exc:
                        logger.warning(
                            "TVH delete channel failed for uuid %s: %s",
                            channel.tvh_uuid,
                            exc,
                        )

            # Remove all source entries in the channel_sources table
            result = await session.execute(
                select(ChannelSource).filter_by(channel_id=channel.id)
            )
            current_sources = result.scalars().all()

            for source in current_sources:
                if source.tvh_uuid:
                    # Delete mux from TVH
                    await delete_channel_muxes(config, source.tvh_uuid)
                await session.delete(source)

            # Clear out association table. This fixes an issue where if multiple similar entries ended up in that table,
            # no more updates could be made to the channel.
            #   > sqlalchemy.orm.exc.StaleDataError:
            #   > DELETE statement on table 'channels_tags_group' expected to delete 1 row(s); Only 2 were matched.
            stmt = channels_tags_association_table.delete().where(
                channels_tags_association_table.c.channel_id == channel_id
            )
            await session.execute(stmt)

            # Remove channel from DB
            await session.delete(channel)
            await session.commit()
            return True


async def build_m3u_lines_for_channel(tic_base_url, channel_uuid, channel):
    playlist = []
    line = f'#EXTINF:-1 tvg-name="{channel.name}" tvg-logo="{channel.logo_url}" tvg-id="{channel_uuid}" tvg-chno="{channel.number}"'
    if channel.tags:
        line += f' group-title="{channel.tags[0]}"'
    line += f",{channel.name}"
    playlist.append(line)
    playlist.append(
        f"{tic_base_url}/tic-tvh/stream/channel/{channel_uuid}?profile=pass"
    )
    return playlist


async def publish_channel_to_tvh(tvh, channel):
    logger.info("Publishing channel to TVH - %s.", channel.name)
    # Check if channel exists with a matching UUID and create it if not
    channel_uuid = channel.tvh_uuid
    existing_channels = await tvh.list_all_channels()
    if channel_uuid:
        found = False
        for tvh_channel in existing_channels:
            if tvh_channel.get("uuid") == channel_uuid:
                found = True
        if not found:
            channel_uuid = None
    if not channel_uuid:
        # No channel exists, create one
        logger.info("   - Creating new channel in TVH")
        channel_uuid = await tvh.create_channel(
            channel.name, channel.number, channel.logo_url
        )
    else:
        logger.info("   - Found existing channel in TVH")
    channel_conf = {
        "enabled": True,
        "uuid": channel_uuid,
        "name": channel.name,
        "number": channel.number,
        "icon": channel.logo_url,
    }
    # Check for existing channel tags
    existing_tag_details = {}
    logger.info("   - Fetching details of channel tags in TVH")
    for tvh_channel_tag in await tvh.list_all_managed_channel_tags():
        existing_tag_details[tvh_channel_tag.get("name")] = tvh_channel_tag.get("uuid")
    # Create channel tags in TVH if missing
    channel_tag_uuids = []
    for tag in channel.tags:
        tag_uuid = existing_tag_details.get(tag.name)
        if not tag_uuid:
            # Create channel tag
            logger.info("Creating new channel tag '%s'", tag.name)
            tag_uuid = await tvh.create_channel_tag(tag.name)
        channel_tag_uuids.append(tag_uuid)
    # Apply channel tag UUIDs to chanel conf in TVH
    channel_conf["tags"] = channel_tag_uuids
    # Save channel info in TVH
    await tvh.idnode_save(channel_conf)
    return channel_uuid


async def batch_publish_new_channels_to_tvh(config, channels):
    """Batch publish a list of newly created Channel objects to TVHeadend.

    Reduces API calls by fetching existing channels and tags once and only creating
    what is missing.
    """
    if not channels:
        return
    async with await get_tvh(config) as tvh:
        logger.info("Batch publishing %d new channels to TVH", len(channels))
        existing_channels = await tvh.list_all_channels()
        existing_by_name = {
            c.get("name"): c.get("uuid") for c in existing_channels if c.get("name")
        }

        # Collect tag names
        unique_tags = set()
        for ch in channels:
            for tag in getattr(ch, "tags", []) or []:
                tag_name = getattr(tag, "name", None) or (
                    tag if isinstance(tag, str) else None
                )
                if tag_name:
                    unique_tags.add(tag_name)

        # Existing managed channel tags
        existing_tag_details = {}
        if unique_tags:
            for tvh_tag in await tvh.list_all_managed_channel_tags():
                existing_tag_details[tvh_tag.get("name")] = tvh_tag.get("uuid")

        # Create missing tags
        for tag_name in list(unique_tags):
            if tag_name not in existing_tag_details:
                try:
                    logger.info("Creating new channel tag '%s' (batch)", tag_name)
                    tag_uuid = await tvh.create_channel_tag(tag_name)
                    if tag_uuid:
                        existing_tag_details[tag_name] = tag_uuid
                except Exception as e:
                    logger.error("Failed to create channel tag '%s': %s", tag_name, e)

        # Publish each channel
        for ch in channels:
            if ch.tvh_uuid:
                continue
            existing_uuid = existing_by_name.get(ch.name)
            if not existing_uuid:
                try:
                    existing_uuid = await tvh.create_channel(
                        ch.name, ch.number, ch.logo_url
                    )
                    existing_by_name[ch.name] = existing_uuid
                except Exception as e:
                    logger.error("Failed creating channel '%s': %s", ch.name, e)
                    continue
            tag_uuids = []
            for tag in getattr(ch, "tags", []) or []:
                tag_name = getattr(tag, "name", None) or (
                    tag if isinstance(tag, str) else None
                )
                if tag_name and existing_tag_details.get(tag_name):
                    tag_uuids.append(existing_tag_details[tag_name])
            channel_conf = {
                "enabled": True,
                "uuid": existing_uuid,
                "name": ch.name,
                "number": ch.number,
                "icon": ch.logo_url,
                "tags": tag_uuids,
            }
            try:
                await tvh.idnode_save(channel_conf)
                ch.tvh_uuid = existing_uuid
            except Exception as e:
                logger.error("Failed saving channel '%s': %s", ch.name, e)
        db.session.commit()
        logger.info("Batch publish completed")


async def publish_bulk_channels_to_tvh_and_m3u(config):
    settings = config.read_settings()
    tic_base_url = settings["settings"]["app_url"]
    async with await get_tvh(config) as tvh:
        # Loop over configured channels
        managed_uuids = []
        results = (
            db.session.query(Channel)
            .options(
                joinedload(Channel.tags),
                joinedload(Channel.sources).subqueryload(ChannelSource.playlist),
            )
            .order_by(Channel.id, Channel.number.asc())
            .all()
        )
        # Fetch existing channels
        logger.info("Publishing all channels to TVH and M3U")
        playlist = [f'#EXTM3U url-tvg="{tic_base_url}/tic-web/epg.xml"']
        pending_commit = False
        for result in results:
            channel_uuid = await publish_channel_to_tvh(tvh, result)
            playlist += await build_m3u_lines_for_channel(
                tic_base_url, channel_uuid, result
            )
            result.tvh_uuid = channel_uuid
            result.logo_base64 = await parse_image_as_base64(result.logo_url)
            managed_uuids.append(channel_uuid)
            pending_commit = True
        if pending_commit:
            try:
                db.session.commit()
            except Exception:
                db.session.rollback()
                raise

        # Write playlist file
        custom_playlist_file = os.path.join(config.config_path, "playlist.m3u8")
        async with aiofiles.open(custom_playlist_file, "w", encoding="utf-8") as f:
            for item in playlist:
                await f.write(f"{item}\n")

        #  Remove any channels that are not managed.
        logger.info("Running cleanup task on current TVH channels")
        for existing_channel in await tvh.list_all_channels():
            if existing_channel.get("uuid") not in managed_uuids:
                logger.info(
                    "    - Removing channel UUID - %s", existing_channel.get("uuid")
                )
                await tvh.delete_channel(existing_channel.get("uuid"))


async def publish_channel_muxes(config):
    tvh_stream_username, tvh_stream_key = await get_tvh_stream_auth(config)
    async with await get_tvh(config) as tvh:
        # Fetch results with relationships
        results = (
            db.session.query(Channel)
            .options(
                joinedload(Channel.tags),
                joinedload(Channel.sources).subqueryload(ChannelSource.playlist),
            )
            .order_by(Channel.id, Channel.number.asc())
            .all()
        )

        # Cache existing mux list (LRU) to avoid repeat heavy calls within TTL
        cached_muxes = _list_cache.get("existing_muxes")
        if cached_muxes is None:
            cached_muxes = await tvh.list_all_muxes()
            _list_cache.set("existing_muxes", cached_muxes, ttl=30)
        existing_muxes = cached_muxes
        existing_mux_uuids = {m.get("uuid") for m in existing_muxes}

        managed_uuids = []
        sem = asyncio.Semaphore(8)  # limit concurrency
        mux_tasks = []

        async def process_source(channel_obj, source_obj):
            async with sem:
                net_uuid = source_obj.playlist.tvh_uuid
                if not net_uuid:
                    logger.debug(
                        "Playlist not configured on TVH for channel '%s'",
                        channel_obj.name,
                    )
                    return
                mux_uuid = source_obj.tvh_uuid
                run_mux_scan = False
                if mux_uuid and mux_uuid in existing_mux_uuids:
                    # Check scan_result
                    for mux in existing_muxes:
                        if mux.get("uuid") == mux_uuid and mux.get("scan_result") == 2:
                            run_mux_scan = True
                            break
                else:
                    mux_uuid = None
                if not mux_uuid:
                    logger.info(
                        "    - Creating new MUX for channel '%s'", channel_obj.name
                    )
                    try:
                        mux_uuid = await tvh.network_mux_create(net_uuid)
                        run_mux_scan = True
                    except Exception as e:
                        logger.error(
                            "Failed creating MUX for channel '%s': %s",
                            channel_obj.name,
                            e,
                        )
                        return
                else:
                    logger.debug(
                        "    - Updating existing MUX '%s' for '%s'",
                        mux_uuid,
                        channel_obj.name,
                    )

                service_name = (
                    f"{source_obj.playlist.name} - {source_obj.playlist_stream_name}"
                )
                stream_url = source_obj.playlist_stream_url
                instance_id = config.ensure_instance_id()
                if is_local_hls_proxy_url(stream_url, instance_id=instance_id):
                    stream_url = append_stream_key(
                        stream_url, tvh_stream_key, tvh_stream_username
                    )
                iptv_url = generate_iptv_url(
                    config,
                    url=stream_url,
                    service_name=service_name,
                )
                channel_id = f"{channel_obj.number}_{re.sub(r'[^a-zA-Z0-9]', '', channel_obj.name)}"
                mux_conf = {
                    "enabled": 1,
                    "uuid": mux_uuid,
                    "iptv_url": iptv_url,
                    "iptv_icon": channel_obj.logo_url,
                    "iptv_sname": channel_obj.name,
                    "iptv_muxname": service_name,
                    "channel_number": channel_obj.number,
                    "iptv_epgid": channel_id,
                    "priority": source_obj.priority,
                    "spriority": source_obj.priority,
                }
                if run_mux_scan:
                    mux_conf["scan_state"] = 1
                try:
                    await tvh.idnode_save(mux_conf)
                    source_obj.tvh_uuid = mux_uuid
                    managed_uuids.append(mux_uuid)
                except Exception as e:
                    logger.error(
                        "Failed saving MUX for channel '%s': %s", channel_obj.name, e
                    )

        # Schedule tasks
        for channel_obj in results:
            if not channel_obj.enabled:
                continue
            for source_obj in channel_obj.sources:
                mux_tasks.append(
                    asyncio.create_task(process_source(channel_obj, source_obj))
                )

        # Await all
        if mux_tasks:
            await asyncio.gather(*mux_tasks)
            db.session.commit()

        # Cleanup unused muxes
        logger.info("Running cleanup task on current TVH muxes")
        # Refresh existing mux list for cleanup (not from cache to be up-to-date)
        current_muxes = await tvh.list_all_muxes()
        for existing_mux in current_muxes:
            uuid = existing_mux.get("uuid")
            if uuid and uuid not in managed_uuids:
                try:
                    logger.info("    - Removing mux UUID - %s", uuid)
                    await tvh.delete_mux(uuid)
                except Exception as e:
                    logger.error("Failed removing mux '%s': %s", uuid, e)


async def delete_channel_muxes(config, mux_uuid):
    async with await get_tvh(config) as tvh:
        try:
            await tvh.delete_mux(mux_uuid)
        except Exception as exc:
            logger.warning("TVH delete mux failed for uuid %s: %s", mux_uuid, exc)


async def map_all_services(config):
    logger.info("Executing TVH Map all service")
    async with await get_tvh(config) as tvh:
        await tvh.map_all_services_to_channels()


async def cleanup_old_channels(config):
    logger.info("Cleaning up old TVH channels")
    async with await get_tvh(config) as tvh:
        for channel in await tvh.list_all_channels():
            if channel.get("name") == "{name-not-set}":
                logger.info("    - Removing channel UUID - %s", channel.get("uuid"))
                await tvh.delete_channel(channel.get("uuid"))


async def queue_background_channel_update_tasks(config):
    settings = config.read_settings()
    # Update TVH
    from backend.api.tasks import TaskQueueBroker

    task_broker = await TaskQueueBroker.get_instance()
    # Configure TVH with the list of channels
    await task_broker.add_task(
        {
            "name": "Configuring TVH channels",
            "function": publish_bulk_channels_to_tvh_and_m3u,
            "args": [config],
        },
        priority=11,
    )
    # Configure TVH with muxes
    await task_broker.add_task(
        {
            "name": "Configuring TVH muxes",
            "function": publish_channel_muxes,
            "args": [config],
        },
        priority=12,
    )
    # Map all services
    await task_broker.add_task(
        {
            "name": "Mapping all TVH services",
            "function": map_all_services,
            "args": [config],
        },
        priority=13,
    )
    # Clear out old channels
    await task_broker.add_task(
        {
            "name": "Cleanup old TVH channels",
            "function": cleanup_old_channels,
            "args": [config],
        },
        priority=14,
    )
    # Fetch additional EPG data from the internet
    from backend.epgs import update_channel_epg_with_online_data

    epg_settings = settings["settings"].get("epgs", {})
    if epg_settings.get("enable_tmdb_metadata") or epg_settings.get(
        "enable_google_image_search_metadata"
    ):
        await task_broker.add_task(
            {
                "name": "Update EPG Data with online metadata",
                "function": update_channel_epg_with_online_data,
                "args": [config],
            },
            priority=21,
        )
    # Generate 'epg.xml' file in .tvh_iptv_config directory
    from backend.epgs import build_custom_epg

    await task_broker.add_task(
        {
            "name": "Recreating static XMLTV file",
            "function": build_custom_epg,
            "args": [config],
        },
        priority=23,
    )
    # Ensure TVH XMLTV grabber URL stays in sync before triggering a grab
    from backend.tvheadend.tvh_requests import configure_tvh

    await task_broker.add_task(
        {
            "name": "Resetting TVH XMLTV URL",
            "function": configure_tvh,
            "args": [config],
        },
        priority=30,
    )
    # Trigger an update in TVH to fetch the latest EPG
    from backend.epgs import run_tvh_epg_grabbers

    await task_broker.add_task(
        {
            "name": "Triggering an update in TVH to fetch the latest XMLTV",
            "function": run_tvh_epg_grabbers,
            "args": [config],
        },
        priority=31,
    )


async def add_channels_from_groups(config, groups):
    """
    Add channels from specified groups to the channel list using the same process
    as add_new_channel to ensure full compatibility with TVHeadend
    """
    logger.info(f"Adding channels from {len(groups)} groups")
    settings = config.read_settings()
    added_channel_count = 0
    group_new_channels = []

    # First determine the starting channel number for the whole operation
    channel_number = db.session.query(func.max(Channel.number)).scalar()
    if channel_number is None:
        channel_number = 999

    for group_info in groups:
        playlist_id = group_info.get("playlist_id")
        group_name = group_info.get("group_name")

        if not playlist_id or not group_name:
            logger.warning(
                f"Missing playlist_id or group_name in group info: {group_info}"
            )
            continue

        # Get all streams from this group
        playlist_streams_query = (
            db.session.query(PlaylistStreams)
            .filter(PlaylistStreams.playlist_id == playlist_id)
            .filter(PlaylistStreams.group_title == group_name)
            .all()
        )

        logger.info(
            f"Found {len(playlist_streams_query)} streams in group '{group_name}' to add"
        )

        for stream in playlist_streams_query:
            try:
                # Check if Channel with this name already exists
                existing_channel = (
                    db.session.query(Channel)
                    .filter(Channel.name == stream.name.strip())
                    .first()
                )

                if existing_channel:
                    logger.info(f"Channel '{stream.name}' already exists, skipping")
                    continue

                # Increment the channel number for each new channel
                channel_number += 1

                # Create channel data structure similar to what's used in add_new_channel
                new_channel_data = {
                    "enabled": True,
                    "name": stream.name.strip(),
                    "logo_url": stream.tvg_logo,
                    "number": int(channel_number),
                    "tags": [group_name],  # Add group name as tag
                    "sources": [],
                }

                # Find the best match for an EPG
                epg_match = (
                    db.session.query(EpgChannels)
                    .filter(EpgChannels.channel_id == stream.tvg_id)
                    .first()
                )
                if epg_match is not None:
                    new_channel_data["guide"] = {
                        "channel_id": epg_match.channel_id,
                        "epg_id": epg_match.epg_id,
                        "epg_name": epg_match.name,
                    }

                # Get the playlist info
                playlist_info = (
                    db.session.query(Playlist).filter(Playlist.id == playlist_id).one()
                )

                # Add source information
                new_channel_data["sources"].append(
                    {
                        "playlist_id": playlist_id,
                        "playlist_name": playlist_info.name,
                        "stream_name": stream.name,
                    }
                )

                channel_obj = await add_new_channel(
                    config, new_channel_data, commit=False, publish=False
                )
                group_new_channels.append(channel_obj)
                added_channel_count += 1

            except Exception as e:
                logger.error(
                    f"Error adding channel '{stream.name}' from group '{group_name}': {str(e)}"
                )
                import traceback

                logger.error(traceback.format_exc())

    # Commit all new channels in one transaction
    db.session.commit()

    try:
        await batch_publish_new_channels_to_tvh(config, group_new_channels)
    except Exception as e:
        logger.error("Batch publish (groups) failed: %s", e)

    logger.info(f"Successfully added {added_channel_count} channels from groups")
    return added_channel_count
