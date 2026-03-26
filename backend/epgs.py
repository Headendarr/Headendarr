#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import gzip
import hashlib
import json
import logging
import os
import re
import shutil
import stat
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote, unquote

import aiofiles
import aiohttp
import asyncio
import sys
import time
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
from quart.utils import run_sync
from sqlalchemy.orm import joinedload
from sqlalchemy import and_, or_, delete, insert, select, text, func, cast, BigInteger, exists, update
from backend.dummy_epg import (
    DUMMY_EPG_XMLTV_DAYS,
    build_dummy_epg_programmes,
    sanitise_dummy_epg_interval,
    xmltv_datetime_from_timestamp,
)
from backend.channels import build_channel_logo_output_url, _read_channel_dummy_epg_settings
from backend.models import db, Session, Epg, Channel, EpgChannels, EpgChannelProgrammes, EpgProgrammeMetadataCache
from backend.tvheadend.tvh_requests import get_tvh
from backend.utils import as_naive_utc, parse_entity_id
from backend.vod_channels import build_xmltv_programmes, build_vod_channel_schedule, is_vod_channel_type

logger = logging.getLogger("tic.epgs")
XMLTV_UTC_FORMAT = "%Y%m%d%H%M%S +0000"
MATCHED_METADATA_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
NO_MATCH_METADATA_CACHE_TTL_SECONDS = 23 * 60 * 60
SKIPPED_METADATA_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
TMDB_TARGET_RATE_LIMIT_REQUESTS = 20
TMDB_RATE_LIMIT_PERIOD_SECONDS = 1.0
TMDB_RETRY_DELAYS_SECONDS = [5, 10, 15]
TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500"
TMDB_MAX_SERIES_RESULTS = 3
TMDB_MAX_EPISODE_MATCH_SEASONS = 60
TMDB_LOOKUP_CONCURRENCY = 8
SKIP_LOOKUP_KEYWORDS = (
    "a-league",
    "abc news",
    "afternoon briefing",
    "asian cup",
    "bloomberg",
    "business of war",
    "champions tour",
    "counting the cost",
    "cricket",
    "darts",
    "dp world tour",
    "evening news",
    "event highlights",
    "ext highlights",
    "extended highlights",
    "fox 5",
    "grand prix",
    "highlights",
    "home shopping",
    "horse race",
    "horse racing",
    "house of representatives",
    "infomercial",
    "inside story",
    "late news",
    "livezone",
    "motorsport classic",
    "national news",
    "news live",
    "news overnight",
    "news regional",
    "news update",
    "news with auslan",
    "newsbreak",
    "newshour",
    "nightly news",
    "paid programming",
    "paris-nice",
    "people and power",
    "premiership",
    "press club address",
    "question time",
    "resume at",
    "returns at",
    "roland-garros",
    "rugby",
    "sevens series",
    "shopping",
    "supercars championship",
    "tbc",
    "teleshopping",
    "the listening post",
    "timbersports",
    "to be advised",
    "world championship",
    "world cup",
)
DEFAULT_EPG_UPDATE_SCHEDULE = "12h"
EPG_UPDATE_SCHEDULE_SECONDS = {
    "1h": 3600,
    "2h": 7200,
    "3h": 10800,
    "6h": 21600,
    "12h": 43200,
    "24h": 86400,
    "2d": 172800,
    "3d": 259200,
    "4d": 345600,
    "5d": 432000,
    "6d": 518400,
    "7d": 604800,
    "14d": 1209600,
    "off": None,
}


def _parsed_epg_update_schedule(value):
    if value is None:
        return DEFAULT_EPG_UPDATE_SCHEDULE

    value_lower = str(value).strip().lower()
    aliases = {
        "1d": "24h",
        "weekly": "7d",
        "2w": "14d",
        "2weeks": "14d",
        "14days": "14d",
        "manual": "off",
        "none": "off",
        "disabled": "off",
    }
    value_lower = aliases.get(value_lower, value_lower)
    if value_lower in EPG_UPDATE_SCHEDULE_SECONDS:
        return value_lower
    return DEFAULT_EPG_UPDATE_SCHEDULE


def generate_epg_channel_id(number, name):
    # return f"{number}_{re.sub(r'[^a-zA-Z0-9]', '', name)}"
    return str(number)


def _epg_health_state_path(config):
    return os.path.join(config.config_path, "cache", "epg_health.json")


def _read_epg_health_map(config):
    try:
        path = _epg_health_state_path(config)
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict) and isinstance(payload.get("epgs"), dict):
            return payload["epgs"]
    except Exception:
        pass
    return {}


def _write_epg_health_map(config, epg_map):
    path = _epg_health_state_path(config)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"epgs": epg_map}, f, indent=2, sort_keys=True)


def _set_epg_health(config, epg_id, payload):
    epg_map = _read_epg_health_map(config)
    epg_key = str(epg_id)
    current = epg_map.get(epg_key, {})
    current.update(payload)
    epg_map[epg_key] = current
    _write_epg_health_map(config, epg_map)


def _clear_epg_health(config, epg_id):
    epg_map = _read_epg_health_map(config)
    epg_map.pop(str(epg_id), None)
    _write_epg_health_map(config, epg_map)


async def read_config_all_epgs(output_for_export=False, config=None):
    return_list = []
    epg_health_map = _read_epg_health_map(config) if config else {}
    async with Session() as session:
        async with session.begin():
            query = await session.execute(select(Epg))
            results = query.scalars().all()
            epg_ids = [result.id for result in results]
            review_stats_map = await _read_epg_review_stats_map(epg_ids)
            for result in results:
                if output_for_export:
                    return_list.append(
                        {
                            "enabled": result.enabled,
                            "name": result.name,
                            "url": result.url,
                            "user_agent": result.user_agent,
                            "update_schedule": _parsed_epg_update_schedule(result.update_schedule),
                        }
                    )
                    continue
                return_list.append(
                    {
                        "id": result.id,
                        "enabled": result.enabled,
                        "name": result.name,
                        "url": result.url,
                        "user_agent": result.user_agent,
                        "update_schedule": _parsed_epg_update_schedule(result.update_schedule),
                        "health": epg_health_map.get(str(result.id), {}),
                        "review": _build_epg_review_payload(
                            review_stats_map.get(result.id, {}),
                            epg_health_map.get(str(result.id), {}),
                        ),
                    }
                )
    return return_list


async def read_config_one_epg(epg_id, config=None):
    epg_id = parse_entity_id(epg_id, "epg")
    return_item = {}
    epg_health_map = _read_epg_health_map(config) if config else {}
    review_stats_map = await _read_epg_review_stats_map([epg_id])
    async with Session() as session:
        async with session.begin():
            query = await session.execute(select(Epg).where(Epg.id == epg_id))
            results = query.scalar_one_or_none()
            if results:
                return_item = {
                    "id": results.id,
                    "enabled": results.enabled,
                    "name": results.name,
                    "url": results.url,
                    "user_agent": results.user_agent,
                    "update_schedule": _parsed_epg_update_schedule(results.update_schedule),
                    "health": epg_health_map.get(str(results.id), {}),
                    "review": _build_epg_review_payload(
                        review_stats_map.get(results.id, {}),
                        epg_health_map.get(str(results.id), {}),
                    ),
                }
    return return_item


def _build_epg_review_payload(stats, health):
    channel_count = int(stats.get("channel_count") or 0)
    programme_count = int(stats.get("programme_count") or 0)
    has_successful_update = bool((health or {}).get("last_success_at"))
    has_data = channel_count > 0 and programme_count > 0
    return {
        "channel_count": channel_count,
        "programme_count": programme_count,
        "has_successful_update": has_successful_update,
        "has_data": has_data,
        "can_review": has_successful_update and has_data,
    }


async def _read_epg_review_stats_map(epg_ids):
    parsed_ids = [parse_entity_id(epg_id, "epg") for epg_id in (epg_ids or []) if epg_id is not None]
    if not parsed_ids:
        return {}

    stats_map = {epg_id: {"channel_count": 0, "programme_count": 0} for epg_id in parsed_ids}

    async with Session() as session:
        async with session.begin():
            channel_rows = await session.execute(
                select(
                    EpgChannels.epg_id,
                    func.count(EpgChannels.id).label("channel_count"),
                )
                .where(EpgChannels.epg_id.in_(parsed_ids))
                .group_by(EpgChannels.epg_id)
            )
            for row in channel_rows.all():
                stats_map[row.epg_id]["channel_count"] = int(row.channel_count or 0)

            programme_rows = await session.execute(
                select(
                    EpgChannels.epg_id,
                    func.count(EpgChannelProgrammes.id).label("programme_count"),
                )
                .select_from(EpgChannels)
                .join(EpgChannelProgrammes, EpgChannelProgrammes.epg_channel_id == EpgChannels.id)
                .where(EpgChannels.epg_id.in_(parsed_ids))
                .group_by(EpgChannels.epg_id)
            )
            for row in programme_rows.all():
                stats_map[row.epg_id]["programme_count"] = int(row.programme_count or 0)

    return stats_map


async def add_new_epg(data):
    async with Session() as session:
        async with session.begin():
            epg = Epg(
                enabled=data.get("enabled"),
                name=data.get("name"),
                url=data.get("url"),
                user_agent=data.get("user_agent"),
                update_schedule=_parsed_epg_update_schedule(data.get("update_schedule")),
            )
            # This is a new entry. Add it to the session before commit
            session.add(epg)


async def update_epg(epg_id, data):
    epg_id = parse_entity_id(epg_id, "epg")
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(Epg).where(Epg.id == epg_id))
            epg = result.scalar_one()
            epg.enabled = data.get("enabled")
            epg.name = data.get("name")
            epg.url = data.get("url")
            epg.user_agent = data.get("user_agent", epg.user_agent)
            epg.update_schedule = _parsed_epg_update_schedule(data.get("update_schedule", epg.update_schedule))


async def delete_epg(config, epg_id):
    epg_id = parse_entity_id(epg_id, "epg")
    async with Session() as session:
        async with session.begin():
            # Unlink channels before deleting the guide row to satisfy the FK on channels.guide_id.
            await session.execute(
                update(Channel)
                .where(Channel.guide_id == epg_id)
                .values(
                    guide_id=None,
                    guide_name=None,
                    guide_channel_id=None,
                    guide_offset_minutes=0,
                )
            )
            # Get all channel IDs for the given EPG
            # noinspection DuplicatedCode
            result = await session.execute(select(EpgChannels.id).where(EpgChannels.epg_id == epg_id))
            channel_ids = [row[0] for row in result.fetchall()]
            if channel_ids:
                # Delete all EpgChannelProgrammes where epg_channel_id is in the list of channel IDs
                await session.execute(
                    delete(EpgChannelProgrammes).where(EpgChannelProgrammes.epg_channel_id.in_(channel_ids))
                )
                # Delete all EpgChannels where id is in the list of channel IDs
                await session.execute(delete(EpgChannels).where(EpgChannels.id.in_(channel_ids)))
            # Delete the Epg entry
            await session.execute(delete(Epg).where(Epg.id == epg_id))

    # Remove cached copy of epg
    cache_files = [
        os.path.join(config.config_path, "cache", "epgs", f"{epg_id}.xml"),
        os.path.join(config.config_path, "cache", "epgs", f"{epg_id}.yml"),
    ]
    for f in cache_files:
        if os.path.isfile(f):
            os.remove(f)
    _clear_epg_health(config, epg_id)


def _resolve_user_agent(settings, user_agent):
    if user_agent:
        return user_agent
    defaults = settings.get("settings", {}).get("user_agents", [])
    if isinstance(defaults, list) and defaults:
        return defaults[0].get("value") or defaults[0].get("name")
    return "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"


def _is_file_epg_source(url):
    return str(url or "").strip().lower().startswith("file://")


def _resolve_file_epg_command(url):
    raw_value = str(url or "").strip()
    if not _is_file_epg_source(raw_value):
        raise ValueError("EPG source is not a file:// source")

    command_path = unquote(raw_value[len("file://") :]).strip()
    if not command_path:
        raise ValueError("file:// EPG sources must include a script or executable path")

    return os.path.expanduser(command_path)


async def _write_process_stdout_to_file(stream, output):
    async with aiofiles.open(output, "wb") as file_handle:
        while True:
            chunk = await stream.read(8192)
            if not chunk:
                break
            await file_handle.write(chunk)


async def _run_file_epg_source(url, output):
    command_path = _resolve_file_epg_command(url)
    logger.info("Running EPG file source - '%s'", command_path)

    if not os.path.exists(command_path):
        raise FileNotFoundError(f"EPG file source does not exist: '{command_path}'")
    if os.path.isdir(command_path):
        raise IsADirectoryError(f"EPG file source must be a file, not a directory: '{command_path}'")

    file_mode = os.stat(command_path).st_mode
    if not stat.S_ISREG(file_mode):
        raise ValueError(f"EPG file source must be a regular file: '{command_path}'")
    if not os.access(command_path, os.X_OK):
        raise PermissionError(f"EPG file source is not executable: '{command_path}'")

    process = await asyncio.create_subprocess_exec(
        command_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout_task = asyncio.create_task(_write_process_stdout_to_file(process.stdout, output))
    stderr_task = asyncio.create_task(process.stderr.read())
    await process.wait()
    await stdout_task
    stderr = await stderr_task

    if process.returncode != 0:
        stderr_text = (stderr or b"").decode("utf-8", errors="replace").strip()
        if len(stderr_text) > 800:
            stderr_text = f"{stderr_text[:800].rstrip()}..."
        if os.path.exists(output):
            try:
                os.remove(output)
            except OSError:
                pass
        raise RuntimeError(
            f"EPG file source exited with status {process.returncode}" + (f": {stderr_text}" if stderr_text else "")
        )

    if os.path.getsize(output) == 0:
        raise RuntimeError(f"EPG file source produced no output: '{command_path}'")


async def download_xmltv_epg(settings, url, output, user_agent=None):
    if not os.path.exists(os.path.dirname(output)):
        os.makedirs(os.path.dirname(output))

    if _is_file_epg_source(url):
        await _run_file_epg_source(url, output)
    else:
        logger.info("Downloading EPG from url - '%s'", url)
        headers = {"User-Agent": _resolve_user_agent(settings, user_agent)}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                async with aiofiles.open(output, "wb") as f:
                    async for chunk in response.content.iter_chunked(8192):
                        await f.write(chunk)
    await try_unzip(output)


async def try_unzip(output: str) -> None:
    def _maybe_unzip(path: str) -> bool:
        temp_path = f"{path}.tmp_unzip"
        try:
            with gzip.open(path, "rb") as src, open(temp_path, "wb") as dst:
                shutil.copyfileobj(src, dst, length=1024 * 1024)
            os.replace(temp_path, path)
            return True
        except Exception:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass
            return False

    loop = asyncio.get_running_loop()
    did_unzip = await loop.run_in_executor(None, _maybe_unzip, output)
    if did_unzip:
        logger.info("Downloaded file is gzipped. Unzipping")


def _derive_timestamp(raw_value):
    if not raw_value:
        return None
    try:
        return str(int(datetime.strptime(raw_value, "%Y%m%d%H%M%S %z").timestamp()))
    except Exception:
        return None


def _parse_timestamp(ts_value):
    if ts_value in (None, ""):
        return None
    try:
        return str(int(str(ts_value).strip()))
    except Exception:
        return None


def _xmltv_utc_from_timestamp(ts_value):
    parsed_value = _parse_timestamp(ts_value)
    if not parsed_value:
        return None
    try:
        return datetime.fromtimestamp(int(parsed_value), tz=timezone.utc).strftime(XMLTV_UTC_FORMAT)
    except Exception:
        return None


def _parse_xmltv_time(raw_value, ts_value):
    parsed_timestamp = _parse_timestamp(ts_value) or _derive_timestamp(raw_value)
    if not parsed_timestamp:
        return raw_value, None
    utc_value = _xmltv_utc_from_timestamp(parsed_timestamp)
    return (utc_value or raw_value), parsed_timestamp


def _shift_xmltv_window(start_value, stop_value, start_ts, stop_ts, offset_minutes):
    try:
        offset_seconds = int(offset_minutes or 0) * 60
    except Exception:
        offset_seconds = 0
    if offset_seconds == 0:
        return start_value, stop_value, start_ts, stop_ts

    shifted_start_ts = _parse_timestamp(start_ts)
    shifted_stop_ts = _parse_timestamp(stop_ts)
    if shifted_start_ts:
        shifted_start_ts = str(int(shifted_start_ts) + offset_seconds)
        shifted_start_value = _xmltv_utc_from_timestamp(shifted_start_ts) or start_value
    else:
        shifted_start_value, shifted_start_ts = _parse_xmltv_time(start_value, None)
        if shifted_start_ts:
            shifted_start_ts = str(int(shifted_start_ts) + offset_seconds)
            shifted_start_value = _xmltv_utc_from_timestamp(shifted_start_ts) or shifted_start_value

    if shifted_stop_ts:
        shifted_stop_ts = str(int(shifted_stop_ts) + offset_seconds)
        shifted_stop_value = _xmltv_utc_from_timestamp(shifted_stop_ts) or stop_value
    else:
        shifted_stop_value, shifted_stop_ts = _parse_xmltv_time(stop_value, None)
        if shifted_stop_ts:
            shifted_stop_ts = str(int(shifted_stop_ts) + offset_seconds)
            shifted_stop_value = _xmltv_utc_from_timestamp(shifted_stop_ts) or shifted_stop_value

    return shifted_start_value, shifted_stop_value, shifted_start_ts, shifted_stop_ts


def _clear_epg_channel_data_sync(epg_id):
    try:
        db.session.execute(
            text(
                """
                DELETE FROM epg_channel_programmes AS p
                USING epg_channels AS c
                WHERE p.epg_channel_id = c.id
                  AND c.epg_id = :epg_id
                """
            ),
            {"epg_id": epg_id},
        )
        db.session.execute(delete(EpgChannels).where(EpgChannels.epg_id == epg_id))
        db.session.commit()
    except Exception:
        # Fallback for non-Postgres engines used in local tooling/tests.
        db.session.rollback()
        channel_ids_subquery = select(EpgChannels.id).where(EpgChannels.epg_id == epg_id)
        db.session.execute(
            delete(EpgChannelProgrammes).where(EpgChannelProgrammes.epg_channel_id.in_(channel_ids_subquery))
        )
        db.session.execute(delete(EpgChannels).where(EpgChannels.epg_id == epg_id))
        db.session.commit()


def _clean_xmltv_text(value):
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _collect_xmltv_texts(elem, tag):
    values = []
    for child in elem.findall(tag):
        text_value = _clean_xmltv_text(child.text)
        if text_value:
            values.append(text_value)
    return values


def _clean_lookup_text(value):
    if value is None:
        return ""
    cleaned = re.sub(r"\s+", " ", str(value).strip().lower())
    cleaned = re.sub(r"[^a-z0-9\s]+", "", cleaned)
    return cleaned.strip()


def _clean_lookup_tokens(value):
    cleaned = _clean_lookup_text(value)
    if not cleaned:
        return []
    return cleaned.split()


EPISODE_MARKER_PATTERNS = (
    re.compile(r"(?i)\bseason\s*(?P<season>\d{1,2})\s*episode\s*(?P<episode>\d{1,3})\b"),
    re.compile(r"(?i)\bseason\s*(?P<season>\d{1,2})\s*ep\.?\s*(?P<episode>\d{1,3})\b"),
    re.compile(r"(?i)\bseries\s*(?P<season>\d{1,2})\s*episode\s*(?P<episode>\d{1,3})\b"),
    re.compile(r"(?i)\bseries\s*(?P<season>\d{1,2})\s*ep\.?\s*(?P<episode>\d{1,3})\b"),
    re.compile(r"(?i)\bstag\.?\s*(?P<season>\d{1,2})\s*ep\.?\s*(?P<episode>\d{1,3})\b"),
    re.compile(r"(?i)\bstg\.?\s*(?P<season>\d{1,2})\s*ep\.?\s*(?P<episode>\d{1,3})\b"),
    re.compile(r"(?i)\btp\s*(?P<season>\d{1,2})\s*[-:/]?\s*ep\.?\s*(?P<episode>\d{1,3})\b"),
    re.compile(r"(?i)\bs\.\s*(?P<season>\d{1,2})\s*ep\.?\s*(?P<episode>\d{1,3})\b"),
    re.compile(r"(?i)\b(?P<season>\d{1,2})x(?P<episode>\d{1,3})\b"),
    re.compile(r"(?i)\(\s*s(?P<season>\d{1,2})\s*[:;,]?\s*e[p]?\s*(?P<episode>\d{1,3})\s*\)"),
    re.compile(r"(?i)\bs(?P<season>\d{1,2})\s*e[p]?\s*(?P<episode>\d{1,3})\b"),
    re.compile(r"(?i)\bs(?P<season>\d{1,2})\s*ep\s*(?P<episode>\d{1,3})\b"),
    re.compile(r"(?i)\bs(?P<season>\d{1,2})e(?P<episode>\d{1,3})\b"),
)


def _strip_episode_marker_text(value):
    """Remove recognised season/episode markers from title or subtitle text.

    Supported marker styles currently include:
    - ``Season 1 Episode 9``
    - ``Season 1 Ep 9``
    - ``Series 1 Episode 9``
    - ``Series 1 Ep 9``
    - ``Stag. 1 Ep. 9``
    - ``Stg. 1 Ep. 9``
    - ``Tp02 - Ep02``
    - ``S.15 Ep.35``
    - ``1x09``
    - ``(S1:E9)`` and ``(S1, Ep9)``
    - ``S1 E9``
    - ``S1 Ep9``
    - ``S01E09``

    After marker removal, surrounding separator characters such as ``-``, ``:``,
    ``|``, ``,``, ``;``, ``/`` and parentheses are trimmed if they are left at
    the start or end of the remaining text.
    """
    if not value:
        return value
    text = str(value).strip()
    for pattern in EPISODE_MARKER_PATTERNS:
        text = pattern.sub("", text)
    text = re.sub(r"\s*[-:|,;/()]+\s*$", "", text)
    text = re.sub(r"^\s*[-:|,;/()]+\s*", "", text)
    text = re.sub(r"\s{2,}", " ", text).strip()
    return text or None


def extract_episode_marker_details(title=None, sub_title=None):
    values = [("title", title), ("sub_title", sub_title)]
    for source_name, source_value in values:
        if not source_value:
            continue
        for pattern in EPISODE_MARKER_PATTERNS:
            match = pattern.search(str(source_value))
            if not match:
                continue
            try:
                season_number = int(match.group("season"))
                episode_number = int(match.group("episode"))
            except (TypeError, ValueError):
                continue
            cleaned_value = _strip_episode_marker_text(source_value)
            return {
                "source": source_name,
                "season_number": season_number,
                "episode_number": episode_number,
                "cleaned_value": cleaned_value,
                "raw_value": source_value,
            }
    return None


def _derive_tmdb_search_title(programme_row):
    marker = extract_episode_marker_details(programme_row.get("title"), programme_row.get("sub_title"))
    cleaned_title = _strip_episode_marker_text(programme_row.get("title"))
    if marker and marker.get("source") == "title" and cleaned_title:
        return cleaned_title, marker
    return programme_row.get("title"), marker


def _derive_tmdb_episode_search_name(programme_row, marker=None):
    marker = marker or extract_episode_marker_details(programme_row.get("title"), programme_row.get("sub_title"))
    sub_title = programme_row.get("sub_title")
    if sub_title:
        cleaned_sub_title = _strip_episode_marker_text(sub_title)
        if cleaned_sub_title:
            return cleaned_sub_title
    if marker and marker.get("source") == "title":
        return None
    return sub_title


def _episode_search_names(programme_row, marker=None):
    title = programme_row.get("title")
    derived_sub_title = _derive_tmdb_episode_search_name(programme_row, marker)
    derived_clean = _clean_lookup_text(derived_sub_title)
    title_clean = _clean_lookup_text(title)
    if not derived_clean or derived_clean == title_clean:
        return []

    names = []
    seen = set()

    def _add_candidate(value):
        candidate = (value or "").strip()
        candidate_clean = _clean_lookup_text(candidate)
        if not candidate_clean or candidate_clean == title_clean or candidate_clean in seen:
            return
        seen.add(candidate_clean)
        names.append(candidate)

    _add_candidate(derived_sub_title)

    split_parts = re.split(r"\s*/\s*", derived_sub_title or "")
    if len(split_parts) > 1:
        for part in split_parts:
            _add_candidate(part)

    return names


async def _run_with_inflight_dedupe(run_state, inflight_map_name, cache_key, coro_factory):
    inflight_map = getattr(run_state, inflight_map_name)
    async with run_state.inflight_lock:
        existing_task = inflight_map.get(cache_key)
        if existing_task is not None:
            logger.debug("TMDB in-flight dedupe hit map=%s key=%r", inflight_map_name, cache_key)
            task = existing_task
        else:
            task = asyncio.create_task(coro_factory())
            inflight_map[cache_key] = task
    try:
        return await task
    finally:
        async with run_state.inflight_lock:
            if inflight_map.get(cache_key) is task:
                inflight_map.pop(cache_key, None)


def derive_metadata_lookup_hash(title, sub_title=None):
    cleaned_title = _clean_lookup_text(title)
    if not cleaned_title:
        return None
    cleaned_sub_title = _clean_lookup_text(sub_title)
    lookup_key = f"{cleaned_title}\n{cleaned_sub_title}" if cleaned_sub_title else cleaned_title
    return hashlib.sha256(lookup_key.encode("utf-8")).hexdigest()


def _programme_has_lookup_skip_keyword(title, sub_title=None):
    combined = " ".join(value for value in (_clean_lookup_text(title), _clean_lookup_text(sub_title)) if value).strip()
    if not combined:
        return False
    return any(keyword in combined for keyword in SKIP_LOOKUP_KEYWORDS)


def _programme_has_episode_numbers(programme_row):
    return bool(
        (programme_row.get("epnum_onscreen") or "").strip() or (programme_row.get("epnum_xmltv_ns") or "").strip()
    )


def _programme_missing_descriptive_fields(programme_row):
    for key in ("title", "sub_title", "desc", "series_desc", "icon_url"):
        if not (programme_row.get(key) or "").strip():
            return True
    return False


def _programme_is_plausibly_episodic(programme_row):
    if (programme_row.get("sub_title") or "").strip():
        return True
    categories = programme_row.get("categories_list") or []
    category_text = " ".join(_clean_lookup_text(item) for item in categories if item)
    if not category_text:
        return False
    episodic_keywords = ("series", "drama", "comedy", "sitcom", "reality", "soap", "entertainment")
    if any(keyword in category_text for keyword in episodic_keywords):
        return True
    movie_keywords = ("movie", "film", "cinema")
    if any(keyword in category_text for keyword in movie_keywords):
        return False
    return False


def _format_tmdb_image_url(path):
    if not path:
        return None
    return f"{TMDB_IMAGE_BASE_URL}{path}"


def _build_xmltv_ns(season_number, episode_number):
    if season_number is None or episode_number is None:
        return None
    try:
        season_value = int(season_number)
        episode_value = int(episode_number)
    except (TypeError, ValueError):
        return None
    if season_value < 1 or episode_value < 1:
        return None
    return f"{season_value - 1} . {episode_value - 1} ."


def _build_onscreen_epnum(season_number, episode_number):
    if season_number is None or episode_number is None:
        return None
    try:
        season_value = int(season_number)
        episode_value = int(episode_number)
    except (TypeError, ValueError):
        return None
    if season_value < 1 or episode_value < 1:
        return None
    return f"S{season_value:02d}E{episode_value:02d}"


def _parse_categories_json(value):
    try:
        parsed = json.loads(value or "[]")
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if item is not None]


def _parse_programme_start_year(programme_row):
    start_timestamp = programme_row.get("start_timestamp")
    if not start_timestamp:
        return None
    try:
        return datetime.fromtimestamp(int(start_timestamp), tz=timezone.utc).year
    except Exception:
        return None


def _programme_is_candidate(programme_row):
    if not (programme_row.get("title") or "").strip():
        return False
    if _programme_missing_descriptive_fields(programme_row):
        return True
    if _programme_has_episode_numbers(programme_row):
        return False
    return _programme_is_plausibly_episodic(programme_row)


def _parse_xmltv_keywords(elem):
    keywords = _collect_xmltv_texts(elem, "keyword")
    if not keywords:
        return None
    return json.dumps(keywords)


def _parse_xmltv_credits(elem):
    credits = elem.find("credits")
    if credits is None:
        return None
    credits_json = {}
    for credit_entry in list(credits):
        role = _clean_xmltv_text(credit_entry.tag)
        person = _clean_xmltv_text(credit_entry.text)
        if not role or not person:
            continue
        credits_json.setdefault(role, []).append(person)
    if not credits_json:
        return None
    return json.dumps(credits_json)


def _parse_xmltv_video(elem):
    video = elem.find("video")
    if video is None:
        return None, None, None
    return (
        _clean_xmltv_text(video.findtext("colour", default=None)),
        _clean_xmltv_text(video.findtext("aspect", default=None)),
        _clean_xmltv_text(video.findtext("quality", default=None)),
    )


def _parse_xmltv_episode_numbers(elem):
    onscreen_value = None
    xmltv_ns_value = None
    dd_progid_value = None
    for episode_num in elem.findall("episode-num"):
        episode_text = _clean_xmltv_text(episode_num.text)
        if not episode_text:
            continue
        episode_system = _clean_xmltv_text(episode_num.attrib.get("system", "")) or ""
        episode_system = episode_system.lower()
        if episode_system == "onscreen" and onscreen_value is None:
            onscreen_value = episode_text
        elif episode_system == "xmltv_ns" and xmltv_ns_value is None:
            xmltv_ns_value = episode_text
        elif episode_system == "dd_progid" and dd_progid_value is None:
            dd_progid_value = episode_text
    return onscreen_value, xmltv_ns_value, dd_progid_value


def _parse_xmltv_star_rating(elem):
    for star_rating in elem.findall("star-rating"):
        value_node = star_rating.find("value")
        rating_value = _clean_xmltv_text(value_node.text if value_node is not None else None)
        if rating_value:
            return rating_value
    return None


def _parse_xmltv_rating(elem):
    for rating in elem.findall("rating"):
        rating_value_node = rating.find("value")
        rating_value = _clean_xmltv_text(rating_value_node.text if rating_value_node is not None else None)
        if rating_value:
            rating_system = _clean_xmltv_text(rating.attrib.get("system", None))
            return rating_system, rating_value
    return None, None


def _import_epg_xml_sync(epg_id, xmltv_file, programme_batch_size=5000):
    phase_seconds = {}
    channel_count = 0
    programme_count = 0
    skipped_programmes = 0

    if not os.path.exists(xmltv_file):
        raise FileNotFoundError(f"No such file '{xmltv_file}'")

    logger.info("Importing channels for EPG #%s from path - '%s'", epg_id, xmltv_file)
    t0 = time.perf_counter()
    _clear_epg_channel_data_sync(epg_id)
    phase_seconds["clear_existing"] = time.perf_counter() - t0

    t_parse = time.perf_counter()
    channel_rows = []
    programme_rows = []
    channel_map = None

    def flush_channels():
        nonlocal channel_rows, channel_map
        if channel_rows:
            db.session.execute(insert(EpgChannels), channel_rows)
            db.session.commit()
            channel_rows = []
        channel_rows_result = db.session.execute(
            select(EpgChannels.channel_id, EpgChannels.id).where(EpgChannels.epg_id == epg_id)
        ).all()
        channel_map = dict(channel_rows_result)

    def flush_programmes():
        nonlocal programme_rows
        if programme_rows:
            db.session.execute(insert(EpgChannelProgrammes), programme_rows)
            db.session.commit()
            programme_rows = []

    t_map = 0.0
    t_prog = 0.0
    programme_rows = []
    for _, elem in ET.iterparse(xmltv_file, events=("end",)):
        if elem.tag == "channel":
            channel_id = elem.get("id")
            if channel_id:
                icon_node = elem.find("icon")
                channel_rows.append(
                    {
                        "epg_id": epg_id,
                        "channel_id": channel_id,
                        "name": (elem.findtext("display-name", default="") or "").strip(),
                        "icon_url": (icon_node.attrib.get("src", "") if icon_node is not None else ""),
                    }
                )
                channel_count += 1
                if channel_map is not None:
                    map_start = time.perf_counter()
                    flush_channels()
                    t_map += time.perf_counter() - map_start
            elem.clear()
            continue

        if elem.tag != "programme":
            continue

        if channel_map is None:
            map_start = time.perf_counter()
            flush_channels()
            t_map += time.perf_counter() - map_start

        external_channel_id = elem.attrib.get("channel")
        epg_channel_id = channel_map.get(external_channel_id)
        if not epg_channel_id:
            skipped_programmes += 1
            elem.clear()
            continue

        start = elem.attrib.get("start")
        stop = elem.attrib.get("stop")
        start_timestamp = elem.attrib.get("start_timestamp")
        stop_timestamp = elem.attrib.get("stop_timestamp")
        start, start_timestamp = _parse_xmltv_time(start, start_timestamp)
        stop, stop_timestamp = _parse_xmltv_time(stop, stop_timestamp)

        categories = _collect_xmltv_texts(elem, "category")
        keywords = _parse_xmltv_keywords(elem)
        credits_json = _parse_xmltv_credits(elem)
        video_colour, video_aspect, video_quality = _parse_xmltv_video(elem)
        subtitles = elem.find("subtitles")
        previously_shown = elem.find("previously-shown")
        onscreen_epnum, xmltv_ns_epnum, dd_progid_epnum = _parse_xmltv_episode_numbers(elem)
        star_rating = _parse_xmltv_star_rating(elem)
        rating_system, rating_value = _parse_xmltv_rating(elem)

        icon = elem.find("icon")
        title_text = _clean_xmltv_text(elem.findtext("title", default=None))
        sub_title_text = _clean_xmltv_text(elem.findtext("sub-title", default=None))
        programme_rows.append(
            {
                "epg_channel_id": epg_channel_id,
                "channel_id": external_channel_id,
                "title": title_text,
                "sub_title": sub_title_text,
                "desc": _clean_xmltv_text(elem.findtext("desc", default=None)),
                "series_desc": _clean_xmltv_text(elem.findtext("series-desc", default=None)),
                "icon_url": icon.attrib.get("src", None) if icon is not None else None,
                "country": _clean_xmltv_text(elem.findtext("country", default=None)),
                "start": start,
                "stop": stop,
                "start_timestamp": start_timestamp,
                "stop_timestamp": stop_timestamp,
                "categories": json.dumps(categories),
                "summary": _clean_xmltv_text(elem.findtext("summary", default=None)),
                "keywords": keywords,
                "credits_json": credits_json,
                "video_colour": video_colour,
                "video_aspect": video_aspect,
                "video_quality": video_quality,
                "subtitles_type": (
                    _clean_xmltv_text(subtitles.attrib.get("type", None)) if subtitles is not None else None
                ),
                "audio_described": elem.find("audio-described") is not None,
                "previously_shown_date": (
                    _clean_xmltv_text(previously_shown.attrib.get("start", None))
                    if previously_shown is not None
                    else None
                ),
                "premiere": elem.find("premiere") is not None,
                "is_new": elem.find("new") is not None,
                "epnum_onscreen": onscreen_epnum,
                "epnum_xmltv_ns": xmltv_ns_epnum,
                "epnum_dd_progid": dd_progid_epnum,
                "star_rating": star_rating,
                "production_year": _clean_xmltv_text(elem.findtext("date", default=None)),
                "rating_system": rating_system,
                "rating_value": rating_value,
                "metadata_lookup_hash": derive_metadata_lookup_hash(title_text, sub_title_text),
            }
        )
        programme_count += 1
        if len(programme_rows) >= programme_batch_size:
            prog_start = time.perf_counter()
            flush_programmes()
            t_prog += time.perf_counter() - prog_start
        elem.clear()

    if channel_map is None:
        map_start = time.perf_counter()
        flush_channels()
        t_map += time.perf_counter() - map_start
    if programme_rows:
        prog_start = time.perf_counter()
        flush_programmes()
        t_prog += time.perf_counter() - prog_start

    phase_seconds["parse_xml_stream"] = time.perf_counter() - t_parse
    phase_seconds["load_channel_map"] = t_map
    phase_seconds["insert_programmes"] = t_prog

    return {
        "channels": channel_count,
        "programmes": programme_count,
        "programmes_skipped": skipped_programmes,
        "phase_seconds": phase_seconds,
    }


async def import_epg_data(config, epg_id):
    epg = await read_config_one_epg(epg_id, config=config)
    settings = config.read_settings()
    # Fetch a new local cached copy of the EPG from either HTTP(S) or a local executable.
    logger.info("Fetching updated XMLTV file for EPG #%s from source - '%s'", epg_id, epg["url"])
    attempt_ts = int(time.time())
    try:
        start_time = time.time()
        xmltv_file = os.path.join(config.config_path, "cache", "epgs", f"{epg_id}.xml")
        await download_xmltv_epg(settings, epg["url"], xmltv_file, epg.get("user_agent"))
        execution_time = time.time() - start_time
        logger.info("Updated XMLTV file for EPG #%s was cached in '%s' seconds", epg_id, int(execution_time))
        # Read and save EPG data to DB (offloaded to worker thread)
        logger.info("Importing updated data for EPG #%s", epg_id)
        start_time = time.perf_counter()
        stats = await run_sync(_import_epg_xml_sync)(epg_id, xmltv_file)
        execution_time = time.perf_counter() - start_time
        logger.info(
            "EPG #%s import stats channels=%s programmes=%s skipped=%s phases=%s",
            epg_id,
            stats["channels"],
            stats["programmes"],
            stats["programmes_skipped"],
            {k: round(v, 2) for k, v in stats["phase_seconds"].items()},
        )
        logger.info("Updated data for EPG #%s was imported in '%s' seconds", epg_id, int(execution_time))
        _set_epg_health(
            config,
            epg_id,
            {
                "status": "ok",
                "error": None,
                "http_status": None,
                "last_attempt_at": attempt_ts,
                "last_success_at": int(time.time()),
                "source_url": epg.get("url"),
            },
        )
    except Exception as exc:
        _set_epg_health(
            config,
            epg_id,
            {
                "status": "error",
                "error": str(exc),
                "http_status": getattr(exc, "status", None),
                "last_attempt_at": attempt_ts,
                "last_failure_at": int(time.time()),
                "source_url": epg.get("url"),
            },
        )
        raise


async def import_epg_data_for_all_epgs(config):
    epg_health_map = _read_epg_health_map(config)
    now_ts = int(time.time())
    updated_count = 0
    skipped_not_due = 0
    skipped_disabled = 0

    async with Session() as session:
        result = await session.execute(select(Epg.id, Epg.update_schedule).where(Epg.enabled == True))
        epg_rows = result.all()

    for epg_id, configured_schedule in epg_rows:
        schedule = _parsed_epg_update_schedule(configured_schedule)
        if schedule == "off":
            skipped_disabled += 1
            logger.debug("Skipping EPG #%s update because update_schedule is off", epg_id)
            continue

        health = epg_health_map.get(str(epg_id), {})
        last_success_at = health.get("last_success_at") if isinstance(health, dict) else None
        try:
            last_success_at = int(last_success_at) if last_success_at is not None else None
        except (TypeError, ValueError):
            last_success_at = None

        schedule_seconds = EPG_UPDATE_SCHEDULE_SECONDS.get(schedule)
        is_due = last_success_at is None
        if not is_due and schedule_seconds is not None:
            is_due = (now_ts - last_success_at) >= schedule_seconds
        if not is_due:
            skipped_not_due += 1
            logger.debug(
                "Skipping EPG #%s update because it is not due yet (schedule=%s, last_success_at=%s)",
                epg_id,
                schedule,
                last_success_at,
            )
            continue

        try:
            await import_epg_data(config, epg_id)
            updated_count += 1
        except Exception as e:
            logger.error(f"Failed to import EPG data for EPG ID {epg_id}, continuing to next. Error: {e}")

    logger.info(
        "EPG update check complete updated=%s skipped_not_due=%s skipped_off=%s",
        updated_count,
        skipped_not_due,
        skipped_disabled,
    )
    return updated_count


async def read_channels_from_all_epgs(config):
    epgs_channels = {}
    async with Session() as session:
        query = await session.execute(select(Epg).options(joinedload(Epg.epg_channels)))
        epg_rows = query.scalars().unique().all()
    for result in epg_rows:
        epgs_channels[result.id] = []
        for epg_channel in result.epg_channels:
            epgs_channels[result.id].append(
                {
                    "channel_id": epg_channel.channel_id,
                    "display_name": epg_channel.name,
                    "icon": epg_channel.icon_url,
                }
            )
    return epgs_channels


async def read_epg_review_channels(epg_id, search_query="", has_data="any", limit=100, offset=0, now_ts=None):
    epg_id = parse_entity_id(epg_id, "epg")
    limit = max(1, min(int(limit or 100), 250))
    offset = max(0, int(offset or 0))
    now_ts = int(now_ts or time.time())
    has_data = (has_data or "any").strip().lower()
    if has_data not in {"any", "with_data", "without_data"}:
        has_data = "any"

    search_query = (search_query or "").strip()
    search_like = f"%{search_query.lower()}%"
    start_ts_expr = cast(func.nullif(EpgChannelProgrammes.start_timestamp, ""), BigInteger)
    stop_ts_expr = cast(func.nullif(EpgChannelProgrammes.stop_timestamp, ""), BigInteger)

    future_programme_exists = (
        select(EpgChannelProgrammes.id)
        .where(
            and_(
                EpgChannelProgrammes.epg_channel_id == EpgChannels.id,
                stop_ts_expr >= now_ts,
            )
        )
        .limit(1)
        .exists()
    )

    filters = [EpgChannels.epg_id == epg_id]
    if search_query:
        filters.append(
            or_(
                func.lower(EpgChannels.name).like(search_like),
                func.lower(EpgChannels.channel_id).like(search_like),
            )
        )
    if has_data == "with_data":
        filters.append(future_programme_exists)
    elif has_data == "without_data":
        filters.append(~future_programme_exists)

    async with Session() as session:
        async with session.begin():
            total_count_result = await session.execute(select(func.count(EpgChannels.id)).where(*filters))
            total_count = int(total_count_result.scalar() or 0)

            channel_rows_result = await session.execute(
                select(
                    EpgChannels.id,
                    EpgChannels.channel_id,
                    EpgChannels.name,
                    EpgChannels.icon_url,
                )
                .where(*filters)
                .order_by(EpgChannels.name.asc(), EpgChannels.channel_id.asc())
                .offset(offset)
                .limit(limit)
            )
            channel_rows = channel_rows_result.all()

            epg_total_channels_result = await session.execute(
                select(func.count(EpgChannels.id)).where(EpgChannels.epg_id == epg_id)
            )
            epg_total_channels = int(epg_total_channels_result.scalar() or 0)

            epg_channels_with_data_result = await session.execute(
                select(func.count(EpgChannels.id)).where(EpgChannels.epg_id == epg_id, future_programme_exists)
            )
            epg_channels_with_data = int(epg_channels_with_data_result.scalar() or 0)

            epg_programme_count_result = await session.execute(
                select(func.count(EpgChannelProgrammes.id))
                .select_from(EpgChannels)
                .join(EpgChannelProgrammes, EpgChannelProgrammes.epg_channel_id == EpgChannels.id)
                .where(EpgChannels.epg_id == epg_id)
            )
            epg_programme_count = int(epg_programme_count_result.scalar() or 0)

            row_ids = [row.id for row in channel_rows]
            stats_map = {}
            upcoming_map = {}

            if row_ids:
                per_channel_stats_result = await session.execute(
                    select(
                        EpgChannelProgrammes.epg_channel_id.label("epg_channel_id"),
                        func.count(EpgChannelProgrammes.id).label("total_programmes"),
                        func.count(EpgChannelProgrammes.id).filter(stop_ts_expr >= now_ts).label("future_programmes"),
                        func.max(stop_ts_expr).label("max_stop_ts"),
                    )
                    .where(EpgChannelProgrammes.epg_channel_id.in_(row_ids))
                    .group_by(EpgChannelProgrammes.epg_channel_id)
                )
                stats_map = {
                    int(row.epg_channel_id): {
                        "total_programmes": int(row.total_programmes or 0),
                        "future_programmes": int(row.future_programmes or 0),
                        "max_stop_ts": int(row.max_stop_ts or 0) if row.max_stop_ts else None,
                    }
                    for row in per_channel_stats_result.all()
                }

                ranked_upcoming_subquery = (
                    select(
                        EpgChannelProgrammes.epg_channel_id.label("epg_channel_id"),
                        EpgChannelProgrammes.title.label("title"),
                        start_ts_expr.label("start_ts"),
                        stop_ts_expr.label("stop_ts"),
                        func.row_number()
                        .over(
                            partition_by=EpgChannelProgrammes.epg_channel_id,
                            order_by=start_ts_expr.asc(),
                        )
                        .label("row_num"),
                    )
                    .where(
                        EpgChannelProgrammes.epg_channel_id.in_(row_ids),
                        stop_ts_expr >= now_ts,
                    )
                    .subquery()
                )
                upcoming_rows_result = await session.execute(
                    select(
                        ranked_upcoming_subquery.c.epg_channel_id,
                        ranked_upcoming_subquery.c.title,
                        ranked_upcoming_subquery.c.start_ts,
                        ranked_upcoming_subquery.c.stop_ts,
                        ranked_upcoming_subquery.c.row_num,
                    )
                    .where(ranked_upcoming_subquery.c.row_num <= 4)
                    .order_by(
                        ranked_upcoming_subquery.c.epg_channel_id.asc(),
                        ranked_upcoming_subquery.c.start_ts.asc(),
                    )
                )
                for row in upcoming_rows_result.all():
                    channel_items = upcoming_map.setdefault(int(row.epg_channel_id), [])
                    channel_items.append(
                        {
                            "title": row.title or "(Untitled)",
                            "start_ts": int(row.start_ts) if row.start_ts is not None else None,
                            "stop_ts": int(row.stop_ts) if row.stop_ts is not None else None,
                        }
                    )

    items = []
    for channel_row in channel_rows:
        channel_stats = stats_map.get(int(channel_row.id), {})
        channel_upcoming = upcoming_map.get(int(channel_row.id), [])
        now_programme = None
        next_programmes = []
        for programme in channel_upcoming:
            start_ts = programme.get("start_ts")
            stop_ts = programme.get("stop_ts")
            if now_programme is None and start_ts is not None and stop_ts is not None and start_ts <= now_ts < stop_ts:
                now_programme = programme
                continue
            if len(next_programmes) < 3:
                next_programmes.append(programme)

        max_stop_ts = channel_stats.get("max_stop_ts")
        horizon_hours = None
        if max_stop_ts and max_stop_ts >= now_ts:
            horizon_hours = round((max_stop_ts - now_ts) / 3600, 1)

        items.append(
            {
                "epg_channel_row_id": int(channel_row.id),
                "channel_id": channel_row.channel_id,
                "name": channel_row.name,
                "icon_url": channel_row.icon_url,
                "total_programmes": int(channel_stats.get("total_programmes") or 0),
                "programmes_now_to_future": int(channel_stats.get("future_programmes") or 0),
                "has_future_data": int(channel_stats.get("future_programmes") or 0) > 0,
                "future_horizon_hours": horizon_hours,
                "now_programme": now_programme,
                "next_programmes": next_programmes,
            }
        )

    return {
        "rows": items,
        "total_count": total_count,
        "offset": offset,
        "limit": limit,
        "summary": {
            "total_channels": epg_total_channels,
            "channels_with_future_data": epg_channels_with_data,
            "channels_without_future_data": max(0, epg_total_channels - epg_channels_with_data),
            "total_programmes": epg_programme_count,
        },
        "now_ts": now_ts,
    }


async def read_epg_online_metadata_no_matches(search_query="", limit=100, offset=0):
    search_value = (search_query or "").strip()
    limit_value = max(1, min(int(limit or 100), 1000))
    offset_value = max(0, int(offset or 0))

    filters = [EpgProgrammeMetadataCache.match_status == "no_match"]
    if search_value:
        like_value = f"%{search_value}%"
        filters.append(
            or_(
                EpgProgrammeMetadataCache.lookup_title.ilike(like_value),
                EpgProgrammeMetadataCache.lookup_sub_title.ilike(like_value),
            )
        )

    async with Session() as session:
        grouped_stmt = (
            select(
                EpgProgrammeMetadataCache.lookup_title.label("title"),
                EpgProgrammeMetadataCache.lookup_sub_title.label("sub_title"),
                func.count(EpgProgrammeMetadataCache.id).label("entry_count"),
                func.max(EpgProgrammeMetadataCache.last_checked_at).label("last_checked_at"),
                func.max(EpgProgrammeMetadataCache.updated_at).label("updated_at"),
            )
            .where(*filters)
            .group_by(
                EpgProgrammeMetadataCache.lookup_title,
                EpgProgrammeMetadataCache.lookup_sub_title,
            )
        )

        total_count_result = await session.execute(select(func.count()).select_from(grouped_stmt.subquery()))
        total_count = int(total_count_result.scalar() or 0)

        rows_result = await session.execute(
            grouped_stmt.order_by(
                func.count(EpgProgrammeMetadataCache.id).desc(),
                EpgProgrammeMetadataCache.lookup_title.asc(),
                EpgProgrammeMetadataCache.lookup_sub_title.asc(),
            )
            .offset(offset_value)
            .limit(limit_value)
        )

    rows = []
    for row in rows_result.mappings().all():
        rows.append(
            {
                "title": row["title"],
                "sub_title": row["sub_title"],
                "entry_count": int(row["entry_count"] or 0),
                "last_checked_at": row["last_checked_at"].isoformat() if row["last_checked_at"] else None,
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
            }
        )

    return {
        "rows": rows,
        "total_count": total_count,
        "offset": offset_value,
        "limit": limit_value,
        "search": search_value,
    }


# --- Cache ---
XMLTV_HOST_PLACEHOLDER = "__TIC_HOST__"


def _append_dummy_programmes_to_xml(output_root, channel_info, now_ts):
    interval_minutes = sanitise_dummy_epg_interval(
        channel_info.get("dummy_interval_minutes"),
    )
    window_start_ts = now_ts
    window_end_ts = now_ts + (DUMMY_EPG_XMLTV_DAYS * 24 * 60 * 60)
    dummy_programmes = build_dummy_epg_programmes(
        channel_id=channel_info["channel_id"],
        channel_name=channel_info["display_name"],
        start_ts=window_start_ts,
        end_ts=window_end_ts,
        interval_minutes=interval_minutes,
        offset_minutes=channel_info.get("guide_offset_minutes") or 0,
    )
    for dummy_programme in dummy_programmes:
        output_programme = ET.SubElement(output_root, "programme")
        output_programme.set("start", xmltv_datetime_from_timestamp(dummy_programme["start_ts"]) or "")
        output_programme.set("stop", xmltv_datetime_from_timestamp(dummy_programme["stop_ts"]) or "")
        output_programme.set("start_timestamp", str(dummy_programme["start_ts"]))
        output_programme.set("stop_timestamp", str(dummy_programme["stop_ts"]))
        output_programme.set("channel", str(channel_info["channel_id"]))

        title_el = ET.SubElement(output_programme, "title")
        title_el.text = dummy_programme["title"]
        title_el.set("lang", "en")

        desc_el = ET.SubElement(output_programme, "desc")
        desc_el.text = dummy_programme["desc"]
        desc_el.set("lang", "en")


def render_xmltv_payload(config, base_url: str) -> str:
    file_path = os.path.join(config.config_path, "epg.xml")
    with open(file_path, "r", encoding="utf-8") as epg_file:
        payload = epg_file.read()
    if base_url and XMLTV_HOST_PLACEHOLDER in payload:
        payload = payload.replace(XMLTV_HOST_PLACEHOLDER, base_url.rstrip("/"))
    return payload


async def build_custom_epg_subprocess(config):
    project_root = Path(__file__).resolve().parents[1]
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "backend.scripts.build_custom_epg",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(project_root),
    )

    async def _pipe(stream, level):
        while True:
            line = await stream.readline()
            if not line:
                break
            logger.log(level, "[epg-build] %s", line.decode().rstrip())

    await asyncio.gather(
        _pipe(proc.stdout, logging.INFO),
        _pipe(proc.stderr, logging.INFO),
    )
    rc = await proc.wait()
    if rc != 0:
        raise RuntimeError(f"EPG build subprocess failed with code {rc}")


async def build_custom_epg(config, throttle=False):
    loop = asyncio.get_running_loop()
    logger.info("Generating custom EPG for TVH based on configured channels.")
    total_start = time.perf_counter()
    phase_seconds = {}
    now_ts = int(datetime.now(tz=timezone.utc).timestamp())

    async def maybe_yield():
        if throttle:
            await asyncio.sleep(0.001)

    output_root = ET.Element("tv")
    output_root.set("generator-info-name", "Headendarr")
    output_root.set("source-info-name", "Headendarr - v0.1")

    t0 = time.perf_counter()
    configured_channels = []
    source_key_to_output_channels = defaultdict(list)
    logger.info("   - Loading configured channels and guide mappings.")
    async with Session() as session:
        query = await session.execute(select(Channel).options(joinedload(Channel.tags)).order_by(Channel.number.asc()))
        channel_rows = query.scalars().unique().all()
    for result in channel_rows:
        if not result.enabled:
            continue
        channel_id = generate_epg_channel_id(result.number, result.name)
        logo_url = build_channel_logo_output_url(
            config,
            result.id,
            XMLTV_HOST_PLACEHOLDER,
            result.logo_url or "",
        )
        configured_channels.append(
            {
                "channel_id": channel_id,
                "display_name": result.name,
                "logo_url": logo_url,
                "tags": [tag.name for tag in result.tags],
                "guide_offset_minutes": int(getattr(result, "guide_offset_minutes", 0) or 0),
                "source_key": (result.guide_id, result.guide_channel_id),
                "dummy_interval_minutes": (
                    (_read_channel_dummy_epg_settings(result.id, config) or {}).get("interval_minutes")
                ),
                "channel_type": str(getattr(result, "channel_type", "standard") or "standard"),
                "channel_row_id": int(result.id),
            }
        )
        if result.guide_id and result.guide_channel_id:
            source_key_to_output_channels[(result.guide_id, result.guide_channel_id)].append(channel_id)
        await maybe_yield()
    phase_seconds["load_configured_channels"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    programmes_by_output_channel = defaultdict(list)
    if source_key_to_output_channels:
        guide_ids = {key[0] for key in source_key_to_output_channels}
        guide_channel_ids = {key[1] for key in source_key_to_output_channels}
        logger.info("   - Loading programme rows for mapped guide channels.")
        async with Session() as session:
            query = await session.execute(
                select(EpgChannelProgrammes, EpgChannels.epg_id, EpgChannels.channel_id)
                .join(EpgChannels, EpgChannelProgrammes.epg_channel_id == EpgChannels.id)
                .where(
                    EpgChannels.epg_id.in_(guide_ids),
                    EpgChannels.channel_id.in_(guide_channel_ids),
                )
                .order_by(
                    EpgChannels.epg_id.asc(),
                    EpgChannels.channel_id.asc(),
                    EpgChannelProgrammes.start.asc(),
                )
            )
            rows = query.all()
        for programme, guide_id, guide_channel_id in rows:
            output_channels = source_key_to_output_channels.get((guide_id, guide_channel_id))
            if not output_channels:
                continue
            programme_data = {
                "start": programme.start,
                "stop": programme.stop,
                "start_timestamp": programme.start_timestamp,
                "stop_timestamp": programme.stop_timestamp,
                "title": programme.title,
                "sub-title": programme.sub_title,
                "desc": programme.desc,
                "series-desc": programme.series_desc,
                "country": programme.country,
                "icon_url": programme.icon_url,
                "categories": json.loads(programme.categories or "[]"),
                "summary": programme.summary,
                "keywords": programme.keywords,
                "credits_json": programme.credits_json,
                "video_colour": programme.video_colour,
                "video_aspect": programme.video_aspect,
                "video_quality": programme.video_quality,
                "subtitles_type": programme.subtitles_type,
                "audio_described": programme.audio_described,
                "previously_shown_date": programme.previously_shown_date,
                "premiere": programme.premiere,
                "is_new": programme.is_new,
                "epnum_onscreen": programme.epnum_onscreen,
                "epnum_xmltv_ns": programme.epnum_xmltv_ns,
                "epnum_dd_progid": programme.epnum_dd_progid,
                "star_rating": programme.star_rating,
                "production_year": programme.production_year,
                "rating_system": programme.rating_system,
                "rating_value": programme.rating_value,
            }
            for output_channel_id in output_channels:
                programmes_by_output_channel[output_channel_id].append(programme_data.copy())
    phase_seconds["load_programmes"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    logger.info("   - Generating XML channel info.")
    for channel_info in configured_channels:
        channel = ET.SubElement(output_root, "channel")
        channel.set("id", str(channel_info["channel_id"]))
        display_name = ET.SubElement(channel, "display-name")
        display_name.text = channel_info["display_name"].strip()
        icon = ET.SubElement(channel, "icon")
        icon.set("src", channel_info["logo_url"])
        live = ET.SubElement(channel, "live")
        live.text = "true"
        active = ET.SubElement(channel, "active")
        active.text = "true"
        await maybe_yield()

    logger.info("   - Generating XML channel programme data.")
    for channel_info in configured_channels:
        channel_id = channel_info["channel_id"]
        channel_tags = channel_info["tags"]
        guide_offset_minutes = int(channel_info.get("guide_offset_minutes") or 0)
        if channel_info.get("dummy_interval_minutes"):
            _append_dummy_programmes_to_xml(output_root, channel_info, now_ts)
            await maybe_yield()
            continue
        if is_vod_channel_type(channel_info.get("channel_type")):
            schedule = await build_vod_channel_schedule(config, int(channel_info["channel_row_id"]))
            for programme in build_xmltv_programmes(schedule, channel_tags):
                output_programme = ET.SubElement(output_root, "programme")
                output_programme.set("start", xmltv_datetime_from_timestamp(int(programme["start_ts"])) or "")
                output_programme.set("stop", xmltv_datetime_from_timestamp(int(programme["stop_ts"])) or "")
                output_programme.set("start_timestamp", str(int(programme["start_ts"])))
                output_programme.set("stop_timestamp", str(int(programme["stop_ts"])))
                output_programme.set("channel", str(channel_id))
                title_el = ET.SubElement(output_programme, "title")
                title_el.text = programme["title"]
                title_el.set("lang", "en")
                if programme.get("sub_title"):
                    sub_title_el = ET.SubElement(output_programme, "sub-title")
                    sub_title_el.text = programme["sub_title"]
                    sub_title_el.set("lang", "en")
                if programme.get("desc"):
                    desc_el = ET.SubElement(output_programme, "desc")
                    desc_el.text = programme["desc"]
                    desc_el.set("lang", "en")
                if programme.get("icon_url"):
                    icon_el = ET.SubElement(output_programme, "icon")
                    icon_el.set("src", programme["icon_url"])
                if programme.get("epnum_onscreen"):
                    epnum_el = ET.SubElement(output_programme, "episode-num")
                    epnum_el.set("system", "onscreen")
                    epnum_el.text = programme["epnum_onscreen"]
                for category in programme.get("categories") or []:
                    category_el = ET.SubElement(output_programme, "category")
                    category_el.text = category
                    category_el.set("lang", "en")
            await maybe_yield()
            continue
        for epg_channel_programme in programmes_by_output_channel.get(channel_id, []):
            # Create a <programme> element for the output file and copy the attributes from the input programme
            output_programme = ET.SubElement(output_root, "programme")
            # Build programmes from DB data (manually create attributes etc.
            start_value, start_ts = _parse_xmltv_time(
                epg_channel_programme.get("start"),
                epg_channel_programme.get("start_timestamp"),
            )
            stop_value, stop_ts = _parse_xmltv_time(
                epg_channel_programme.get("stop"),
                epg_channel_programme.get("stop_timestamp"),
            )
            start_value, stop_value, start_ts, stop_ts = _shift_xmltv_window(
                start_value,
                stop_value,
                start_ts,
                stop_ts,
                guide_offset_minutes,
            )
            if start_value:
                output_programme.set("start", start_value)
            if stop_value:
                output_programme.set("stop", stop_value)
            if start_ts:
                output_programme.set("start_timestamp", start_ts)
            if stop_ts:
                output_programme.set("stop_timestamp", stop_ts)
            # Set the "channel" ident here
            output_programme.set("channel", str(channel_id))
            # Loop through all child elements of the input programme and copy them to the output programme
            for child in ["title", "sub-title", "desc", "series-desc", "country"]:
                # Copy all other child elements to the output programme if they exist
                if child in epg_channel_programme and epg_channel_programme[child] is not None:
                    output_child = ET.SubElement(output_programme, child)
                    output_child.text = epg_channel_programme[child]
                    output_child.set("lang", "en")
            # Optional summary
            if epg_channel_programme.get("summary"):
                c = ET.SubElement(output_programme, "summary")
                c.text = epg_channel_programme["summary"]
                c.set("lang", "en")
            # If we have a programme icon, add it
            if epg_channel_programme["icon_url"]:
                output_child = ET.SubElement(output_programme, "icon")
                output_child.set("src", epg_channel_programme["icon_url"])
                output_child.set("height", "")
                output_child.set("width", "")
            # Keywords
            if epg_channel_programme.get("keywords"):
                try:
                    for kw in json.loads(epg_channel_programme["keywords"]):
                        if kw:
                            kc = ET.SubElement(output_programme, "keyword")
                            kc.text = kw
                            kc.set("lang", "en")
                except Exception:
                    pass
            # Credits
            if epg_channel_programme.get("credits_json"):
                try:
                    credits_data = json.loads(epg_channel_programme["credits_json"])
                    if isinstance(credits_data, dict) and credits_data:
                        credits_el = ET.SubElement(output_programme, "credits")
                        for role, people in credits_data.items():
                            if not people:
                                continue
                            for person in people:
                                pe = ET.SubElement(credits_el, role)
                                pe.text = person
                except Exception:
                    pass
            # Video
            if any(epg_channel_programme.get(k) for k in ["video_colour", "video_aspect", "video_quality"]):
                video_el = ET.SubElement(output_programme, "video")
                if epg_channel_programme.get("video_colour"):
                    c = ET.SubElement(video_el, "colour")
                    c.text = epg_channel_programme["video_colour"]
                if epg_channel_programme.get("video_aspect"):
                    a = ET.SubElement(video_el, "aspect")
                    a.text = epg_channel_programme["video_aspect"]
                if epg_channel_programme.get("video_quality"):
                    q = ET.SubElement(video_el, "quality")
                    q.text = epg_channel_programme["video_quality"]
            # Subtitles
            if epg_channel_programme.get("subtitles_type"):
                subs = ET.SubElement(output_programme, "subtitles")
                subs.set("type", epg_channel_programme["subtitles_type"])
            # Audio described
            if epg_channel_programme.get("audio_described"):
                ET.SubElement(output_programme, "audio-described")
            # Previously shown
            if epg_channel_programme.get("previously_shown_date"):
                ps = ET.SubElement(output_programme, "previously-shown")
                ps.set("start", epg_channel_programme["previously_shown_date"])
            # Premiere / New
            if epg_channel_programme.get("premiere"):
                ET.SubElement(output_programme, "premiere")
            if epg_channel_programme.get("is_new"):
                ET.SubElement(output_programme, "new")
            # Episode numbers
            if epg_channel_programme.get("epnum_onscreen"):
                e1 = ET.SubElement(output_programme, "episode-num")
                e1.set("system", "onscreen")
                e1.text = epg_channel_programme["epnum_onscreen"]
            if epg_channel_programme.get("epnum_xmltv_ns"):
                e2 = ET.SubElement(output_programme, "episode-num")
                e2.set("system", "xmltv_ns")
                e2.text = epg_channel_programme["epnum_xmltv_ns"]
            if epg_channel_programme.get("epnum_dd_progid"):
                e3 = ET.SubElement(output_programme, "episode-num")
                e3.set("system", "dd_progid")
                e3.text = epg_channel_programme["epnum_dd_progid"]
            # Star rating
            if epg_channel_programme.get("star_rating"):
                sr = ET.SubElement(output_programme, "star-rating")
                val = ET.SubElement(sr, "value")
                val.text = epg_channel_programme["star_rating"]
            # Production year
            if epg_channel_programme.get("production_year"):
                d = ET.SubElement(output_programme, "date")
                d.text = epg_channel_programme["production_year"]
            # Rating system
            if epg_channel_programme.get("rating_value"):
                rating_el = ET.SubElement(output_programme, "rating")
                if epg_channel_programme.get("rating_system"):
                    rating_el.set("system", epg_channel_programme["rating_system"])
                rv = ET.SubElement(rating_el, "value")
                rv.text = epg_channel_programme["rating_value"]
            # Loop through all categories for this programme and add them as "category" child elements
            if epg_channel_programme["categories"]:
                for category in epg_channel_programme["categories"]:
                    output_child = ET.SubElement(output_programme, "category")
                    output_child.text = category
                    output_child.set("lang", "en")
            # Loop through all tags for this channel and add them as "category" child elements
            for tag in channel_tags:
                output_child = ET.SubElement(output_programme, "category")
                output_child.text = tag
                output_child.set("lang", "en")
        await maybe_yield()
    phase_seconds["generate_xml_tree"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    logger.info("   - Writing out XMLTV file.")
    output_tree = ET.ElementTree(output_root)
    custom_epg_file = os.path.join(config.config_path, "epg.xml")
    await loop.run_in_executor(None, lambda: output_tree.write(custom_epg_file, encoding="UTF-8", xml_declaration=True))
    phase_seconds["write_xml_file"] = time.perf_counter() - t0
    execution_time = time.perf_counter() - total_start
    logger.info(
        "The custom XMLTV EPG file for TVH was generated in '%s' seconds (phases=%s)",
        int(execution_time),
        {k: round(v, 2) for k, v in phase_seconds.items()},
    )


# --- Online Metadata ---
class AsyncRateLimiter:
    def __init__(self, max_calls, period_seconds):
        self.max_calls = max_calls
        self.period_seconds = period_seconds
        self.calls = deque()
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                now = time.monotonic()
                while self.calls and (now - self.calls[0]) >= self.period_seconds:
                    self.calls.popleft()
                if len(self.calls) < self.max_calls:
                    self.calls.append(now)
                    return
                wait_seconds = self.period_seconds - (now - self.calls[0])
            await asyncio.sleep(max(wait_seconds, 0.01))


class TmdbRunState:
    def __init__(self, session, auth_value):
        self.session = session
        self.auth_value = (auth_value or "").strip()
        self.rate_limiter = AsyncRateLimiter(TMDB_TARGET_RATE_LIMIT_REQUESTS, TMDB_RATE_LIMIT_PERIOD_SECONDS)
        self.suspended = False
        self.auth_failed = False
        self.request_count = 0
        self.retry_count = 0
        self.search_cache = {}
        self.tv_details_cache = {}
        self.tv_season_cache = {}
        self.google_cache = {}
        self.inflight_lock = asyncio.Lock()
        self.inflight_search = {}
        self.inflight_tv_details = {}
        self.inflight_tv_seasons = {}
        self.inflight_google = {}


def tmdb_metadata_enabled(settings):
    epg_settings = settings["settings"].get("epgs", {})
    return bool(epg_settings.get("enable_tmdb_metadata") and (epg_settings.get("tmdb_api_key") or "").strip())


def _tmdb_auth_is_bearer_token(auth_value):
    value = (auth_value or "").strip()
    if not value:
        return False
    if value.lower().startswith("bearer "):
        return True
    return len(value) > 40


def _tmdb_build_request_auth(auth_value):
    value = (auth_value or "").strip()
    if not value:
        return {}, {}
    if _tmdb_auth_is_bearer_token(value):
        if not value.lower().startswith("bearer "):
            value = f"Bearer {value}"
        return {"Authorization": value}, {}
    return {}, {"api_key": value}


def epg_online_metadata_enabled(settings):
    epg_settings = settings["settings"].get("epgs", {})
    return bool(tmdb_metadata_enabled(settings) or epg_settings.get("enable_google_image_search_metadata"))


def _tmdb_cache_expiry(match_status="matched", now_utc=None):
    now = now_utc or datetime.now(timezone.utc)
    if match_status == "no_match":
        ttl_seconds = NO_MATCH_METADATA_CACHE_TTL_SECONDS
    elif match_status == "skipped":
        ttl_seconds = SKIPPED_METADATA_CACHE_TTL_SECONDS
    else:
        ttl_seconds = MATCHED_METADATA_CACHE_TTL_SECONDS
    return now + timedelta(seconds=ttl_seconds)


def _score_text_match(expected, actual):
    expected_clean = _clean_lookup_text(expected)
    actual_clean = _clean_lookup_text(actual)
    if not expected_clean or not actual_clean:
        return 0.0
    if expected_clean == actual_clean:
        return 1.0
    if expected_clean in actual_clean or actual_clean in expected_clean:
        shorter_length = min(len(expected_clean), len(actual_clean))
        longer_length = max(len(expected_clean), len(actual_clean))
        if shorter_length >= 12 and (shorter_length / max(longer_length, 1)) >= 0.75:
            return 0.92
        return 0.8
    expected_tokens_list = _clean_lookup_tokens(expected)
    actual_tokens_list = _clean_lookup_tokens(actual)
    if expected_tokens_list and actual_tokens_list:
        shorter_tokens = (
            expected_tokens_list if len(expected_tokens_list) <= len(actual_tokens_list) else actual_tokens_list
        )
        longer_tokens = actual_tokens_list if shorter_tokens is expected_tokens_list else expected_tokens_list
        if (
            len(shorter_tokens) >= 3
            and shorter_tokens == longer_tokens[: len(shorter_tokens)]
            and (len(shorter_tokens) / max(len(longer_tokens), 1)) >= 0.6
        ):
            return 0.9
    expected_tokens = set(expected_clean.split())
    actual_tokens = set(actual_clean.split())
    if not expected_tokens or not actual_tokens:
        return 0.0
    overlap = len(expected_tokens & actual_tokens) / max(len(expected_tokens), len(actual_tokens))
    return overlap


def _is_strong_episode_match(best_episode_score, episode_marker=None):
    if best_episode_score >= 0.85:
        return True
    if episode_marker and best_episode_score >= 0.8:
        return True
    return False


def _choose_tmdb_result(programme_row, tv_result, movie_result):
    tv_has_epnums = bool(
        tv_result and (tv_result.get("cached_epnum_onscreen") or tv_result.get("cached_epnum_xmltv_ns"))
    )
    movie_has_match = bool(movie_result)
    tv_has_match = bool(tv_result)
    if tv_has_epnums:
        return tv_result
    if tv_has_match and not movie_has_match:
        return tv_result
    if movie_has_match and not tv_has_match:
        return movie_result
    if not tv_has_match or not movie_has_match:
        return None

    has_explicit_episode_hint = bool(
        extract_episode_marker_details(programme_row.get("title"), programme_row.get("sub_title"))
        or (
            (programme_row.get("sub_title") or "").strip()
            and _clean_lookup_text(programme_row.get("sub_title")) != _clean_lookup_text(programme_row.get("title"))
        )
        or _programme_has_episode_numbers(programme_row)
    )
    tv_confidence = float(tv_result.get("source_confidence") or 0.0)
    movie_confidence = float(movie_result.get("source_confidence") or 0.0)
    if has_explicit_episode_hint and tv_confidence >= (movie_confidence + 0.05):
        logger.debug(
            "TMDB chooser selected tv title=%r subtitle=%r tv_confidence=%.3f movie_confidence=%.3f reason=explicit_episode_hint",
            programme_row.get("title"),
            programme_row.get("sub_title"),
            tv_confidence,
            movie_confidence,
        )
        return tv_result
    chosen_result = movie_result if movie_confidence >= tv_confidence else tv_result
    logger.debug(
        "TMDB chooser selected %s title=%r subtitle=%r tv_confidence=%.3f movie_confidence=%.3f explicit_episode_hint=%s",
        chosen_result.get("lookup_kind"),
        programme_row.get("title"),
        programme_row.get("sub_title"),
        tv_confidence,
        movie_confidence,
        has_explicit_episode_hint,
    )
    return chosen_result


def _pick_best_tmdb_result(results, expected_title, title_fields):
    best_result = None
    best_score = 0.0
    for result in results or []:
        candidate_score = 0.0
        for field_name in title_fields:
            candidate_score = max(candidate_score, _score_text_match(expected_title, result.get(field_name)))
        popularity = float(result.get("popularity") or 0.0)
        candidate_score += min(popularity / 1000.0, 0.05)
        if candidate_score > best_score:
            best_score = candidate_score
            best_result = result
    if best_score < 0.45:
        return None, 0.0
    return best_result, best_score


async def _tmdb_request(run_state, path, params=None):
    if not run_state.auth_value or run_state.suspended:
        logger.debug(
            "TMDB request skipped path=%s auth_present=%s suspended=%s",
            path,
            bool(run_state.auth_value),
            run_state.suspended,
        )
        return None
    request_params = dict(params or {})
    request_headers, auth_params = _tmdb_build_request_auth(run_state.auth_value)
    request_params.update(auth_params)
    url = f"https://api.themoviedb.org/3{path}"
    for retry_index in range(len(TMDB_RETRY_DELAYS_SECONDS) + 1):
        await run_state.rate_limiter.acquire()
        run_state.request_count += 1
        log_params = {key: value for key, value in request_params.items() if key != "api_key"}
        logger.debug("TMDB request path=%s attempt=%s params=%s", path, retry_index + 1, log_params)
        try:
            async with run_state.session.get(url, params=request_params, headers=request_headers) as response:
                if response.status == 200:
                    payload = await response.json()
                    logger.debug(
                        "TMDB request success path=%s attempt=%s result_keys=%s",
                        path,
                        retry_index + 1,
                        sorted(payload.keys()) if isinstance(payload, dict) else None,
                    )
                    return payload
                if response.status == 429:
                    if retry_index < len(TMDB_RETRY_DELAYS_SECONDS):
                        delay_seconds = TMDB_RETRY_DELAYS_SECONDS[retry_index]
                        run_state.retry_count += 1
                        logger.warning(
                            "TMDB rate limit hit for %s; retrying in %ss (%s/%s)",
                            path,
                            delay_seconds,
                            retry_index + 1,
                            len(TMDB_RETRY_DELAYS_SECONDS),
                        )
                        await asyncio.sleep(delay_seconds)
                        continue
                    run_state.suspended = True
                    logger.warning("TMDB rate limit persisted after retries; suspending TMDB lookups for this run")
                    return None
                if response.status in (401, 403):
                    run_state.suspended = True
                    run_state.auth_failed = True
                    logger.error(
                        "TMDB request unauthorised path=%s status=%s; suspending TMDB lookups until the next scan",
                        path,
                        response.status,
                    )
                    return None
                if response.status == 404:
                    logger.debug("TMDB request returned 404 path=%s params=%s", path, log_params)
                    return None
                logger.warning("TMDB request failed path=%s status=%s", path, response.status)
                return None
        except aiohttp.ClientError as exc:
            logger.warning("TMDB request error path=%s error=%s", path, exc)
            return None
    return None


async def search_tmdb_for_movie(auth_value, title, run_state):
    if not auth_value or not title:
        logger.debug("TMDB movie lookup skipped title=%r auth_present=%s", title, bool(auth_value))
        return None
    cache_key = ("movie_search", _clean_lookup_text(title))
    if cache_key in run_state.search_cache:
        logger.debug("TMDB movie search cache hit title=%r hit=%s", title, bool(run_state.search_cache[cache_key]))
        return run_state.search_cache[cache_key]
    payload = await _run_with_inflight_dedupe(
        run_state,
        "inflight_search",
        cache_key,
        lambda: _tmdb_request(run_state, "/search/movie", {"query": title}),
    )
    results = (payload or {}).get("results", [])
    logger.debug("TMDB movie search title=%r results=%s", title, len(results))
    best_result, confidence = _pick_best_tmdb_result(results, title, ("title", "original_title"))
    if not best_result:
        run_state.search_cache[cache_key] = None
        logger.debug("TMDB movie lookup no-match title=%r", title)
        return None
    movie_result = {
        "lookup_kind": "movie",
        "match_status": "matched",
        "provider": "tmdb",
        "provider_item_type": "movie",
        "provider_item_id": best_result.get("id"),
        "cached_sub_title": best_result.get("title"),
        "cached_desc": best_result.get("overview"),
        "cached_icon_url": _format_tmdb_image_url(best_result.get("poster_path")),
        "source_confidence": confidence,
        "raw_result_json": json.dumps({"movie": best_result}),
    }
    logger.debug(
        "TMDB movie lookup matched title=%r movie_id=%s confidence=%.3f matched_title=%r",
        title,
        best_result.get("id"),
        confidence,
        best_result.get("title"),
    )
    run_state.search_cache[cache_key] = movie_result
    return movie_result


async def _get_tmdb_tv_details(run_state, series_id):
    if series_id in run_state.tv_details_cache:
        return run_state.tv_details_cache[series_id]
    payload = await _run_with_inflight_dedupe(
        run_state,
        "inflight_tv_details",
        series_id,
        lambda: _tmdb_request(run_state, f"/tv/{series_id}"),
    )
    run_state.tv_details_cache[series_id] = payload
    return payload


async def _get_tmdb_tv_season_details(run_state, series_id, season_number):
    cache_key = (series_id, season_number)
    if cache_key in run_state.tv_season_cache:
        return run_state.tv_season_cache[cache_key]
    payload = await _run_with_inflight_dedupe(
        run_state,
        "inflight_tv_seasons",
        cache_key,
        lambda: _tmdb_request(run_state, f"/tv/{series_id}/season/{season_number}"),
    )
    run_state.tv_season_cache[cache_key] = payload
    return payload


async def search_tmdb_for_tv_programme(auth_value, programme_row, run_state):
    title, episode_marker = _derive_tmdb_search_title(programme_row)
    if not auth_value or not title:
        logger.debug("TMDB TV lookup skipped title=%r auth_present=%s", title, bool(auth_value))
        return None
    cache_key = ("tv_search", _clean_lookup_text(title))
    if cache_key in run_state.search_cache:
        search_payload = run_state.search_cache[cache_key]
        logger.debug("TMDB TV search cache hit title=%r", title)
    else:
        search_payload = await _run_with_inflight_dedupe(
            run_state,
            "inflight_search",
            cache_key,
            lambda: _tmdb_request(run_state, "/search/tv", {"query": title}),
        )
        run_state.search_cache[cache_key] = search_payload
    results = (search_payload or {}).get("results", [])
    logger.debug(
        "TMDB TV search title=%r subtitle=%r results=%s extracted_marker=%s",
        title,
        programme_row.get("sub_title"),
        len(results),
        episode_marker,
    )
    best_series, confidence = _pick_best_tmdb_result(results, title, ("name", "original_name"))
    if not best_series:
        logger.debug("TMDB TV lookup no-series-match title=%r", title)
        return None

    series_result = {
        "lookup_kind": "tv",
        "match_status": "matched",
        "provider": "tmdb",
        "provider_item_type": "tv",
        "provider_item_id": best_series.get("id"),
        "provider_series_id": best_series.get("id"),
        "cached_desc": None,
        "cached_series_desc": best_series.get("overview"),
        "cached_icon_url": _format_tmdb_image_url(best_series.get("poster_path")),
        "source_confidence": confidence,
        "raw_result_json": json.dumps({"series": best_series}),
    }

    episode_search_names = _episode_search_names(programme_row, episode_marker)
    if not episode_search_names:
        logger.debug(
            "TMDB TV lookup series-only title=%r series_id=%s confidence=%.3f",
            title,
            best_series.get("id"),
            confidence,
        )
        return series_result

    tv_details = await _get_tmdb_tv_details(run_state, best_series.get("id"))
    seasons = (tv_details or {}).get("seasons", [])
    if not seasons:
        return series_result

    best_episode = None
    best_episode_score = 0.0
    best_episode_search_name = None
    programme_start_year = _parse_programme_start_year(programme_row)
    seasons_checked = 0
    marker_season = episode_marker.get("season_number") if episode_marker else None
    marker_episode = episode_marker.get("episode_number") if episode_marker else None
    for season in seasons:
        season_number = season.get("season_number")
        if season_number is None:
            continue
        if marker_season is not None and season_number != marker_season:
            continue
        seasons_checked += 1
        if seasons_checked > TMDB_MAX_EPISODE_MATCH_SEASONS:
            break
        season_payload = await _get_tmdb_tv_season_details(run_state, best_series.get("id"), season_number)
        episodes = (season_payload or {}).get("episodes", [])
        for episode in episodes:
            episode_score = 0.0
            matched_search_name = None
            for candidate_name in episode_search_names:
                candidate_score = _score_text_match(candidate_name, episode.get("name"))
                if candidate_score > episode_score:
                    episode_score = candidate_score
                    matched_search_name = candidate_name
            if marker_season == episode.get("season_number") and marker_episode == episode.get("episode_number"):
                episode_score = max(episode_score, 0.95)
            if episode_score <= 0.0:
                continue
            air_date = episode.get("air_date")
            if programme_start_year and air_date:
                try:
                    air_year = int(str(air_date).split("-", 1)[0])
                    if abs(programme_start_year - air_year) <= 3:
                        episode_score += 0.1
                except (TypeError, ValueError):
                    pass
            if episode_score > best_episode_score:
                best_episode = episode
                best_episode_score = episode_score
                best_episode_search_name = matched_search_name

    if not best_episode or not _is_strong_episode_match(best_episode_score, episode_marker):
        logger.debug(
            "TMDB TV lookup no-episode-match title=%r subtitle=%r series_id=%s best_episode_score=%.3f",
            title,
            episode_search_names[0] if episode_search_names else None,
            best_series.get("id"),
            best_episode_score,
        )
        return series_result

    season_number = best_episode.get("season_number")
    episode_number = best_episode.get("episode_number")
    logger.debug(
        "TMDB TV lookup matched title=%r subtitle=%r series_id=%s season=%s episode=%s confidence=%.3f",
        title,
        best_episode_search_name or (episode_search_names[0] if episode_search_names else None),
        best_series.get("id"),
        season_number,
        episode_number,
        min(confidence + best_episode_score, 1.0),
    )
    return {
        "lookup_kind": "tv",
        "match_status": "matched",
        "provider": "tmdb",
        "provider_item_type": "episode",
        "provider_item_id": best_episode.get("id"),
        "provider_series_id": best_series.get("id"),
        "provider_season_number": season_number,
        "provider_episode_number": episode_number,
        "cached_sub_title": best_episode.get("name"),
        "cached_desc": best_episode.get("overview") or best_series.get("overview"),
        "cached_series_desc": best_series.get("overview"),
        "cached_icon_url": _format_tmdb_image_url(best_series.get("poster_path")),
        "cached_epnum_onscreen": _build_onscreen_epnum(season_number, episode_number),
        "cached_epnum_xmltv_ns": _build_xmltv_ns(season_number, episode_number),
        "source_confidence": min(confidence + best_episode_score, 1.0),
        "raw_result_json": json.dumps({"series": best_series, "episode": best_episode}),
    }


async def search_google_images(title, run_state):
    cache_key = _clean_lookup_text(title)
    if cache_key in run_state.google_cache:
        logger.debug("Google image cache hit title=%r hit=%s", title, bool(run_state.google_cache[cache_key]))
        return run_state.google_cache[cache_key]

    search_query = f'"{title}" television show'
    encoded_query = quote(search_query)
    search_url = f"https://www.google.com/search?tbm=isch&safe=active&tbs=isz:m&q={encoded_query}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }

    async def _fetch_google_image():
        try:
            async with run_state.session.get(search_url, headers=headers) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), "html.parser")
                    images = soup.find_all("img")
                    if len(images) > 1:
                        image_url = images[1]["src"]
                        logger.debug("Google image lookup matched title=%r", title)
                        return image_url
        except aiohttp.ClientError as exc:
            logger.warning("Google image search failed for '%s': %s", title, exc)
        logger.debug("Google image lookup no-match title=%r", title)
        return None

    image_url = await _run_with_inflight_dedupe(run_state, "inflight_google", cache_key, _fetch_google_image)
    run_state.google_cache[cache_key] = image_url
    return image_url


def _metadata_cache_row_to_dict(cache_row):
    return {
        "lookup_hash": cache_row.lookup_hash,
        "lookup_title": cache_row.lookup_title,
        "lookup_sub_title": cache_row.lookup_sub_title,
        "lookup_kind": cache_row.lookup_kind,
        "match_status": cache_row.match_status,
        "provider": cache_row.provider,
        "provider_item_type": cache_row.provider_item_type,
        "provider_item_id": cache_row.provider_item_id,
        "provider_series_id": cache_row.provider_series_id,
        "provider_season_number": cache_row.provider_season_number,
        "provider_episode_number": cache_row.provider_episode_number,
        "cached_sub_title": cache_row.cached_sub_title,
        "cached_desc": cache_row.cached_desc,
        "cached_series_desc": cache_row.cached_series_desc,
        "cached_icon_url": cache_row.cached_icon_url,
        "cached_epnum_onscreen": cache_row.cached_epnum_onscreen,
        "cached_epnum_xmltv_ns": cache_row.cached_epnum_xmltv_ns,
        "last_checked_at": cache_row.last_checked_at,
        "expires_at": cache_row.expires_at,
        "failure_count": cache_row.failure_count,
        "source_confidence": cache_row.source_confidence,
        "raw_result_json": cache_row.raw_result_json,
    }


def _build_cache_entry_payload(lookup_hash, programme_row, match_status, lookup_kind="unknown", **kwargs):
    now_utc = datetime.now(timezone.utc)
    created_at = as_naive_utc(kwargs.pop("created_at", now_utc))
    last_checked_at = as_naive_utc(kwargs.pop("last_checked_at", now_utc))
    expires_at = as_naive_utc(kwargs.pop("expires_at", _tmdb_cache_expiry(match_status, now_utc)))
    updated_at = as_naive_utc(kwargs.pop("updated_at", now_utc))
    payload = {
        "lookup_hash": lookup_hash,
        "lookup_title": programme_row.get("title"),
        "lookup_sub_title": programme_row.get("sub_title"),
        "lookup_kind": lookup_kind,
        "match_status": match_status,
        "provider": kwargs.pop("provider", "tmdb"),
        "provider_item_type": kwargs.pop("provider_item_type", None),
        "provider_item_id": kwargs.pop("provider_item_id", None),
        "provider_series_id": kwargs.pop("provider_series_id", None),
        "provider_season_number": kwargs.pop("provider_season_number", None),
        "provider_episode_number": kwargs.pop("provider_episode_number", None),
        "cached_sub_title": kwargs.pop("cached_sub_title", None),
        "cached_desc": kwargs.pop("cached_desc", None),
        "cached_series_desc": kwargs.pop("cached_series_desc", None),
        "cached_icon_url": kwargs.pop("cached_icon_url", None),
        "cached_epnum_onscreen": kwargs.pop("cached_epnum_onscreen", None),
        "cached_epnum_xmltv_ns": kwargs.pop("cached_epnum_xmltv_ns", None),
        "last_checked_at": last_checked_at,
        "expires_at": expires_at,
        "failure_count": kwargs.pop("failure_count", 0),
        "source_confidence": kwargs.pop("source_confidence", None),
        "raw_result_json": kwargs.pop("raw_result_json", None),
        "created_at": created_at,
        "updated_at": updated_at,
    }
    payload.update(kwargs)
    return payload


def _apply_cache_entry_to_programme_row(programme_row, cache_entry):
    updates = {}
    field_map = {
        "sub_title": "cached_sub_title",
        "desc": "cached_desc",
        "series_desc": "cached_series_desc",
        "icon_url": "cached_icon_url",
        "epnum_onscreen": "cached_epnum_onscreen",
        "epnum_xmltv_ns": "cached_epnum_xmltv_ns",
    }
    for programme_field, cache_field in field_map.items():
        if (programme_row.get(programme_field) or "").strip():
            continue
        cache_value = cache_entry.get(cache_field)
        if cache_value:
            updates[programme_field] = cache_value
    if programme_row.get("metadata_lookup_hash") != cache_entry.get("lookup_hash"):
        updates["metadata_lookup_hash"] = cache_entry.get("lookup_hash")
    return updates


async def _save_metadata_cache_entries(session, cache_entries):
    if not cache_entries:
        return
    hashes = [entry["lookup_hash"] for entry in cache_entries]
    existing_rows = await session.execute(
        select(EpgProgrammeMetadataCache).where(EpgProgrammeMetadataCache.lookup_hash.in_(hashes))
    )
    existing_map = {row.lookup_hash: row for row in existing_rows.scalars().all()}

    for entry in cache_entries:
        existing_row = existing_map.get(entry["lookup_hash"])
        if existing_row:
            for key, value in entry.items():
                if key == "created_at":
                    continue
                setattr(existing_row, key, value)
            continue
        session.add(EpgProgrammeMetadataCache(**entry))
    await session.flush()


def _cache_entry_is_fresh(cache_entry, now_utc):
    expires_at = cache_entry.get("expires_at")
    if not expires_at:
        return False
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return expires_at > now_utc


async def _lookup_online_metadata_for_hash(settings, programme_row, run_state):
    lookup_hash = programme_row.get("metadata_lookup_hash")
    if not lookup_hash:
        logger.debug("Online metadata lookup skipped row_id=%s reason=no_lookup_hash", programme_row.get("id"))
        return None
    if _programme_has_lookup_skip_keyword(programme_row.get("title"), programme_row.get("sub_title")):
        logger.debug(
            "Online metadata lookup skipped row_id=%s title=%r subtitle=%r reason=skip_keyword",
            programme_row.get("id"),
            programme_row.get("title"),
            programme_row.get("sub_title"),
        )
        return _build_cache_entry_payload(
            lookup_hash,
            programme_row,
            "skipped",
            lookup_kind="skip",
            raw_result_json=json.dumps({"reason": "skip_keyword"}),
        )

    tmdb_auth_value = settings["settings"].get("epgs", {}).get("tmdb_api_key", "")
    enable_tmdb_metadata = tmdb_metadata_enabled(settings)
    enable_google_images = bool(settings["settings"].get("epgs", {}).get("enable_google_image_search_metadata"))

    tv_result = None
    movie_result = None
    logger.debug(
        "Online metadata lookup begin row_id=%s hash=%s title=%r subtitle=%r missing_descriptive=%s has_epnum=%s episodic=%s",
        programme_row.get("id"),
        lookup_hash,
        programme_row.get("title"),
        programme_row.get("sub_title"),
        _programme_missing_descriptive_fields(programme_row),
        _programme_has_episode_numbers(programme_row),
        _programme_is_plausibly_episodic(programme_row),
    )
    if enable_tmdb_metadata:
        if _programme_has_episode_numbers(programme_row) and _programme_missing_descriptive_fields(programme_row):
            tv_result = await search_tmdb_for_tv_programme(tmdb_auth_value, programme_row, run_state)
        elif _programme_missing_descriptive_fields(programme_row):
            tv_result = await search_tmdb_for_tv_programme(tmdb_auth_value, programme_row, run_state)
            movie_result = await search_tmdb_for_movie(tmdb_auth_value, programme_row.get("title"), run_state)
        elif _programme_is_plausibly_episodic(programme_row):
            tv_result = await search_tmdb_for_tv_programme(tmdb_auth_value, programme_row, run_state)

    chosen_result = _choose_tmdb_result(programme_row, tv_result, movie_result)

    if chosen_result and enable_google_images and not chosen_result.get("cached_icon_url"):
        chosen_result["cached_icon_url"] = await search_google_images(programme_row.get("title"), run_state)

    if not chosen_result and enable_google_images and not (programme_row.get("icon_url") or "").strip():
        google_icon = await search_google_images(programme_row.get("title"), run_state)
        if google_icon:
            chosen_result = {
                "lookup_kind": "unknown",
                "match_status": "matched",
                "provider": "google_images",
                "cached_icon_url": google_icon,
                "raw_result_json": json.dumps({"google_image": True}),
            }

    if not chosen_result:
        if run_state.auth_failed:
            logger.error(
                "TMDB authorisation failed during online metadata lookup; stopping TMDB lookups until the next scan"
            )
            return None
        logger.debug(
            "Online metadata lookup no-match row_id=%s hash=%s title=%r subtitle=%r",
            programme_row.get("id"),
            lookup_hash,
            programme_row.get("title"),
            programme_row.get("sub_title"),
        )
        return _build_cache_entry_payload(
            lookup_hash,
            programme_row,
            "no_match",
            raw_result_json=json.dumps({"reason": "no_match"}),
            failure_count=1,
        )

    logger.debug(
        "Online metadata lookup matched row_id=%s hash=%s lookup_kind=%s provider=%s item_type=%s has_epnum_onscreen=%s has_epnum_xmltv_ns=%s",
        programme_row.get("id"),
        lookup_hash,
        chosen_result.get("lookup_kind"),
        chosen_result.get("provider"),
        chosen_result.get("provider_item_type"),
        bool(chosen_result.get("cached_epnum_onscreen")),
        bool(chosen_result.get("cached_epnum_xmltv_ns")),
    )
    chosen_result_payload = dict(chosen_result)
    match_status = chosen_result_payload.pop("match_status", "matched")
    return _build_cache_entry_payload(lookup_hash, programme_row, match_status, **chosen_result_payload)


async def _fetch_online_metadata_candidates(session):
    mapped_channel_exists = exists(
        select(Channel.id).where(
            and_(
                Channel.enabled == True,
                Channel.guide_id == EpgChannels.epg_id,
                Channel.guide_channel_id == EpgChannels.channel_id,
            )
        )
    )
    result = await session.execute(
        select(
            EpgChannelProgrammes.id,
            EpgChannelProgrammes.title,
            EpgChannelProgrammes.sub_title,
            EpgChannelProgrammes.desc,
            EpgChannelProgrammes.series_desc,
            EpgChannelProgrammes.icon_url,
            EpgChannelProgrammes.epnum_onscreen,
            EpgChannelProgrammes.epnum_xmltv_ns,
            EpgChannelProgrammes.categories,
            EpgChannelProgrammes.start_timestamp,
            EpgChannelProgrammes.metadata_lookup_hash,
        )
        .join(EpgChannels, EpgChannelProgrammes.epg_channel_id == EpgChannels.id)
        .where(mapped_channel_exists, EpgChannelProgrammes.title.is_not(None))
    )
    candidates = []
    for row in result.all():
        programme_row = {
            "id": row.id,
            "title": row.title,
            "sub_title": row.sub_title,
            "desc": row.desc,
            "series_desc": row.series_desc,
            "icon_url": row.icon_url,
            "epnum_onscreen": row.epnum_onscreen,
            "epnum_xmltv_ns": row.epnum_xmltv_ns,
            "categories_list": _parse_categories_json(row.categories),
            "start_timestamp": row.start_timestamp,
            "stored_metadata_lookup_hash": row.metadata_lookup_hash,
            "metadata_lookup_hash": row.metadata_lookup_hash or derive_metadata_lookup_hash(row.title, row.sub_title),
        }
        if _programme_is_candidate(programme_row):
            candidates.append(programme_row)
        else:
            logger.debug(
                "Online metadata candidate skipped row_id=%s title=%r subtitle=%r missing_descriptive=%s has_epnum=%s episodic=%s",
                programme_row.get("id"),
                programme_row.get("title"),
                programme_row.get("sub_title"),
                _programme_missing_descriptive_fields(programme_row),
                _programme_has_episode_numbers(programme_row),
                _programme_is_plausibly_episodic(programme_row),
            )
    return candidates


async def _bulk_update_programme_rows(session, update_rows):
    if not update_rows:
        return 0
    merged_updates = {}
    for row in update_rows:
        row_id = row.get("id")
        if row_id is None:
            continue
        merged_updates.setdefault(row_id, {"id": row_id}).update(row)
    if not merged_updates:
        return 0
    await session.execute(update(EpgChannelProgrammes), list(merged_updates.values()))
    return len(merged_updates)


async def _resolve_online_metadata_hashes(settings, unresolved_by_hash, run_state):
    if not unresolved_by_hash:
        return []

    semaphore = asyncio.Semaphore(TMDB_LOOKUP_CONCURRENCY)

    async def resolve_one(lookup_hash, candidate):
        async with semaphore:
            if run_state.suspended:
                return None
            cache_entry = await _lookup_online_metadata_for_hash(settings, candidate, run_state)
            if run_state.suspended and run_state.auth_failed:
                logger.debug("Stopping unresolved hash processing after TMDB auth failure hash=%s", lookup_hash)
            return (lookup_hash, cache_entry)

    tasks = [resolve_one(lookup_hash, candidate) for lookup_hash, candidate in unresolved_by_hash.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    resolved_entries = []
    for result in results:
        if isinstance(result, Exception):
            logger.error("Error resolving online metadata hash: %s", result)
            continue
        if not result:
            continue
        lookup_hash, cache_entry = result
        if cache_entry:
            resolved_entries.append((lookup_hash, cache_entry))
    return resolved_entries


async def update_channel_epg_with_online_data(config):
    settings = config.read_settings()
    if not epg_online_metadata_enabled(settings):
        epg_settings = settings["settings"].get("epgs", {})
        if epg_settings.get("enable_tmdb_metadata") and not (epg_settings.get("tmdb_api_key") or "").strip():
            logger.error("TMDB metadata enrichment is enabled but no TMDB API key is configured; skipping this scan")
        return

    start_time = time.perf_counter()
    phase_seconds = {}
    logger.info("Update EPG with cached and online metadata for configured channels.")

    async with Session() as session:
        t0 = time.perf_counter()
        candidates = await _fetch_online_metadata_candidates(session)
        phase_seconds["fetch_candidates"] = time.perf_counter() - t0

        if not candidates:
            logger.info("No EPG programmes require online metadata updates.")
            return

        logger.info("Online metadata candidate scan found %s programme rows needing enrichment.", len(candidates))

        now_utc = datetime.now(timezone.utc)
        hashes = sorted(
            {candidate["metadata_lookup_hash"] for candidate in candidates if candidate.get("metadata_lookup_hash")}
        )
        candidate_rows_by_hash = defaultdict(list)
        for candidate in candidates:
            candidate_rows_by_hash[candidate.get("metadata_lookup_hash")].append(candidate)

        t0 = time.perf_counter()
        cache_result = await session.execute(
            select(EpgProgrammeMetadataCache).where(EpgProgrammeMetadataCache.lookup_hash.in_(hashes))
        )
        cache_map = {}
        for cache_row in cache_result.scalars().all():
            cache_entry = _metadata_cache_row_to_dict(cache_row)
            if _cache_entry_is_fresh(cache_entry, now_utc):
                cache_map[cache_entry["lookup_hash"]] = cache_entry
                logger.debug(
                    "Online metadata cache fresh hash=%s match_status=%s lookup_kind=%s",
                    cache_entry["lookup_hash"],
                    cache_entry.get("match_status"),
                    cache_entry.get("lookup_kind"),
                )
            else:
                logger.debug(
                    "Online metadata cache stale hash=%s match_status=%s expires_at=%s",
                    cache_entry["lookup_hash"],
                    cache_entry.get("match_status"),
                    cache_entry.get("expires_at"),
                )
        phase_seconds["load_cache"] = time.perf_counter() - t0

        initial_updates = []
        unresolved_by_hash = {}
        for candidate in candidates:
            lookup_hash = candidate.get("metadata_lookup_hash")
            if not lookup_hash:
                continue
            if candidate.get("stored_metadata_lookup_hash") != lookup_hash:
                initial_updates.append({"id": candidate["id"], "metadata_lookup_hash": lookup_hash})
            cache_entry = cache_map.get(lookup_hash)
            if cache_entry:
                logger.debug(
                    "Online metadata cache apply row_id=%s hash=%s match_status=%s",
                    candidate["id"],
                    lookup_hash,
                    cache_entry.get("match_status"),
                )
                programme_updates = _apply_cache_entry_to_programme_row(candidate, cache_entry)
                if programme_updates:
                    initial_updates.append({"id": candidate["id"], **programme_updates})
                continue
            unresolved_by_hash.setdefault(lookup_hash, candidate)
            logger.debug(
                "Online metadata cache miss row_id=%s hash=%s title=%r subtitle=%r",
                candidate["id"],
                lookup_hash,
                candidate.get("title"),
                candidate.get("sub_title"),
            )

        t0 = time.perf_counter()
        initial_updated_count = await _bulk_update_programme_rows(session, initial_updates)
        phase_seconds["apply_cached_updates"] = time.perf_counter() - t0

        logger.info(
            "Online metadata cache phase cache_hits=%s unresolved_hashes=%s initial_row_updates=%s",
            len(candidates) - len(unresolved_by_hash),
            len(unresolved_by_hash),
            initial_updated_count,
        )

        new_cache_entries = []
        new_programme_updates = []
        async with aiohttp.ClientSession() as http_session:
            run_state = TmdbRunState(http_session, settings["settings"].get("epgs", {}).get("tmdb_api_key", ""))
            t0 = time.perf_counter()
            resolved_entries = await _resolve_online_metadata_hashes(settings, unresolved_by_hash, run_state)
            for lookup_hash, cache_entry in resolved_entries:
                new_cache_entries.append(cache_entry)
                for row in candidate_rows_by_hash.get(lookup_hash, []):
                    programme_updates = _apply_cache_entry_to_programme_row(row, cache_entry)
                    if programme_updates:
                        new_programme_updates.append({"id": row["id"], **programme_updates})
            phase_seconds["external_lookup"] = time.perf_counter() - t0
            logger.info(
                "Online metadata lookup stats candidates=%s cache_hits=%s unresolved=%s resolved=%s tmdb_requests=%s tmdb_retries=%s tmdb_suspended=%s concurrency=%s target_rps=%s",
                len(candidates),
                len(candidates) - len(unresolved_by_hash),
                len(unresolved_by_hash),
                len(resolved_entries),
                run_state.request_count,
                run_state.retry_count,
                run_state.suspended,
                TMDB_LOOKUP_CONCURRENCY,
                TMDB_TARGET_RATE_LIMIT_REQUESTS,
            )

        t0 = time.perf_counter()
        await _save_metadata_cache_entries(session, new_cache_entries)
        saved_update_count = await _bulk_update_programme_rows(session, new_programme_updates)
        await session.commit()
        phase_seconds["save_cache_and_updates"] = time.perf_counter() - t0

    execution_time = time.perf_counter() - start_time
    logger.info(
        "Updating online EPG data for configured channels took '%s' seconds (candidates=%s updated=%s cache_entries=%s phases=%s)",
        int(execution_time),
        len(candidates),
        initial_updated_count + saved_update_count,
        len(new_cache_entries),
        {key: round(value, 2) for key, value in phase_seconds.items()},
    )


# --- TVH Functions ---
async def run_tvh_epg_grabbers(config):
    # Trigger a re-grab of the EPG in TVH
    async with await get_tvh(config) as tvh:
        await tvh.run_internal_epg_grabber()
