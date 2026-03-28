#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import hashlib
import json
import logging
import random
import time
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import String, cast, select
from sqlalchemy.orm import joinedload, selectinload

from backend.models import (
    Channel,
    Session,
    VodCategory,
    VodCategoryEpisode,
    VodCategoryEpisodeSource,
    VodCategoryItem,
    VodCategoryItemSource,
    VodChannelRule,
    XcVodItem,
)
from backend.utils import clean_text, convert_to_int, utc_now
from backend.vod import (
    VOD_KIND_MOVIE,
    VOD_KIND_SERIES,
    VodPlaybackCandidate,
    _load_summary,
    _summary_info,
    fetch_xc_series_info_payload,
    resolve_xc_item_upstream_url,
)

logger = logging.getLogger("tic.vod_channels")

CHANNEL_TYPE_STANDARD = "standard"
CHANNEL_TYPE_VOD_24_7 = "vod_24_7"

SCHEDULE_MODE_SERIES_ORDER = "series_season_episode"
SCHEDULE_MODE_RELEASE_DATE = "release_date"
SCHEDULE_MODE_SEASON_AIR_DATE = "season_air_date"
SCHEDULE_MODE_EPISODE_AIR_DATE = "episode_air_date"
SCHEDULE_MODE_SHUFFLE = "shuffle"

DEFAULT_SCHEDULE_MODE = SCHEDULE_MODE_SERIES_ORDER
DEFAULT_SCHEDULE_DIRECTION = "asc"
DEFAULT_SCHEDULE_DAYS_BEHIND = 1
MIN_EPG_WINDOW_HOURS = 72
MAX_EPG_WINDOW_HOURS = 168
NEXT_ITEM_CACHE_WARM_SECONDS = 60


def is_vod_channel_type(value: object) -> bool:
    return clean_text(value).lower() == CHANNEL_TYPE_VOD_24_7


def vod_channel_cache_dir(config: Any) -> Path:
    path = Path(config.config_path) / "cache" / "vod_channels"
    path.mkdir(parents=True, exist_ok=True)
    return path


def vod_channel_schedule_path(config, channel_id: int) -> Path:
    return vod_channel_cache_dir(config) / f"schedule-{int(channel_id)}.json"


def _create_rule_payload(rule: dict[str, Any], position: int) -> dict[str, Any]:
    return {
        "position": int(position),
        "operator": clean_text(rule.get("operator") or "include").lower() or "include",
        "rule_type": clean_text(rule.get("rule_type") or rule.get("type")),
        "value": clean_text(rule.get("value")),
        "enabled": bool(rule.get("enabled", True)),
    }


def serialise_vod_channel_rules(channel: Channel) -> list[dict[str, Any]]:
    rows = sorted(
        list(getattr(channel, "vod_channel_rules", []) or []),
        key=lambda item: (convert_to_int(getattr(item, "position", 0), 0), convert_to_int(getattr(item, "id", 0), 0)),
    )
    return [
        {
            "id": row.id,
            "position": convert_to_int(row.position, 0),
            "operator": clean_text(row.operator or "include").lower() or "include",
            "rule_type": clean_text(row.rule_type),
            "value": clean_text(row.value),
            "enabled": bool(row.enabled),
        }
        for row in rows
        if clean_text(row.rule_type)
    ]


async def replace_vod_channel_rules(session: Any, channel: Channel, rules_payload: list[dict[str, Any]]):
    channel.vod_channel_rules.clear()
    new_rules = []
    for index, rule in enumerate(rules_payload or []):
        rule_payload = _create_rule_payload(rule, index)
        if not rule_payload["rule_type"]:
            continue
        new_rules.append(
            VodChannelRule(
                position=rule_payload["position"],
                operator=rule_payload["operator"],
                rule_type=rule_payload["rule_type"],
                value=rule_payload["value"],
                enabled=rule_payload["enabled"],
            )
        )
    channel.vod_channel_rules = new_rules


def default_vod_channel_settings() -> dict[str, Any]:
    return {
        "schedule_mode": DEFAULT_SCHEDULE_MODE,
        "schedule_direction": "asc",
    }


def read_vod_channel_settings(payload: dict[str, Any] | None) -> dict[str, Any]:
    incoming = payload if isinstance(payload, dict) else {}
    defaults = default_vod_channel_settings()
    schedule_mode = clean_text(incoming.get("schedule_mode") or defaults["schedule_mode"]).lower()
    if schedule_mode not in {
        SCHEDULE_MODE_SERIES_ORDER,
        SCHEDULE_MODE_RELEASE_DATE,
        SCHEDULE_MODE_SEASON_AIR_DATE,
        SCHEDULE_MODE_EPISODE_AIR_DATE,
        SCHEDULE_MODE_SHUFFLE,
    }:
        schedule_mode = defaults["schedule_mode"]
    direction = clean_text(incoming.get("schedule_direction") or defaults["schedule_direction"]).lower()
    if direction not in {"asc", "desc"}:
        direction = defaults["schedule_direction"]
    return {
        "schedule_mode": schedule_mode,
        "schedule_direction": direction,
    }


def _content_fingerprint(
    channel_id: int, settings: dict[str, Any], rules: list[dict[str, Any]], items: list[dict[str, Any]]
) -> str:
    digest = hashlib.sha256()
    digest.update(str(int(channel_id)).encode("utf-8"))
    digest.update(json.dumps(settings, sort_keys=True).encode("utf-8"))
    digest.update(json.dumps(rules, sort_keys=True).encode("utf-8"))
    digest.update(json.dumps(items, sort_keys=True).encode("utf-8"))
    return digest.hexdigest()


def _parse_duration_seconds(summary: dict[str, Any], item_type: str, fallback_minutes: int = 30) -> int:
    info = _summary_info(summary)
    for container in (summary, info):
        if not isinstance(container, dict):
            continue
        for key in ("duration_secs", "duration_seconds", "runtime_seconds"):
            value = container.get(key)
            if str(value).isdigit():
                return max(60, int(value))
        for key in ("duration", "runtime"):
            value = container.get(key)
            if str(value).isdigit():
                raw = int(value)
                if raw > 1000:
                    return raw
                return max(60, raw * 60)
        episode_run_time = container.get("episode_run_time")
        if isinstance(episode_run_time, list) and episode_run_time:
            raw = episode_run_time[0]
            if str(raw).isdigit():
                return max(60, int(raw) * 60)
    if item_type == VOD_KIND_MOVIE:
        return 90 * 60
    return max(60, int(fallback_minutes) * 60)


def _parse_release_sort_value(value: object) -> str:
    text = clean_text(value)
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits[:14] if digits else text.lower()


async def _query_rule_source_item_ids(session: Any, config: Any, rule: dict[str, Any]) -> set[int]:
    rule_type = clean_text(rule.get("rule_type")).lower()
    rule_value = clean_text(rule.get("value"))
    if not rule_type or not rule_value:
        return set()

    if rule_type in {"series_contains", "series_starts_with", "title_contains", "title_starts_with", "series_name"}:
        stmt = select(XcVodItem.id)
        if rule_type.startswith("series_"):
            stmt = stmt.where(XcVodItem.item_type == VOD_KIND_SERIES)
        if rule_type == "series_name":
            stmt = stmt.where(cast(XcVodItem.title, String).ilike(rule_value))
        elif rule_type.endswith("contains"):
            stmt = stmt.where(cast(XcVodItem.title, String).ilike(f"%{rule_value}%"))
        elif rule_type.endswith("starts_with"):
            stmt = stmt.where(cast(XcVodItem.title, String).ilike(f"{rule_value}%"))
        result = await session.execute(stmt)
        return {int(row[0]) for row in result.all() if row and row[0] is not None}

    return set()


async def resolve_vod_channel_item_pool(config: Any, channel: Channel) -> list[dict[str, Any]]:
    rules = [rule for rule in serialise_vod_channel_rules(channel) if rule.get("enabled")]
    if not rules:
        return []
    async with Session() as session:
        include_ids: set[int] = set()
        exclude_ids: set[int] = set()
        for rule in rules:
            matched_ids = await _query_rule_source_item_ids(session, config, rule)
            if rule["operator"] == "exclude":
                exclude_ids.update(matched_ids)
            else:
                include_ids.update(matched_ids)
        selected_ids = sorted(item_id for item_id in include_ids if item_id not in exclude_ids)
        if not selected_ids:
            return []
        result = await session.execute(
            select(XcVodItem)
            .where(XcVodItem.id.in_(selected_ids))
            .order_by(XcVodItem.sort_title.asc(), XcVodItem.title.asc())
        )
        rows = result.scalars().all()

    items: list[dict[str, Any]] = []
    for row in rows:
        summary = _load_summary(getattr(row, "summary_json", None))
        items.append(
            {
                "item_id": int(row.id),
                "source_item_id": int(row.id),
                "item_type": clean_text(row.item_type),
                "title": clean_text(row.title),
                "sort_title": clean_text(row.sort_title or row.title),
                "release_date": clean_text(row.release_date),
                "year": clean_text(row.year),
                "poster_url": clean_text(row.poster_url),
                "container_extension": clean_text(row.container_extension),
                "duration_seconds": _parse_duration_seconds(summary, clean_text(row.item_type)),
                "summary": summary,
            }
        )
    return items


async def vod_channel_has_playlist_items(config: Any, channel_id: int, playlist_id: int) -> bool:
    async with Session() as session:
        result = await session.execute(
            select(Channel).options(selectinload(Channel.vod_channel_rules)).where(Channel.id == int(channel_id))
        )
        channel = result.scalar_one_or_none()
    if channel is None or not is_vod_channel_type(getattr(channel, "channel_type", None)):
        return False

    items = await resolve_vod_channel_item_pool(config, channel)
    source_item_ids = [
        int(item.get("source_item_id") or 0) for item in items if int(item.get("source_item_id") or 0) > 0
    ]
    if not source_item_ids:
        return False

    async with Session() as session:
        result = await session.execute(
            select(XcVodItem.id)
            .where(
                XcVodItem.id.in_(source_item_ids),
                XcVodItem.playlist_id == int(playlist_id),
            )
            .limit(1)
        )
        return result.scalar_one_or_none() is not None


async def _load_series_episode_payloads(source_item_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    if not source_item_ids:
        return {}
    grouped: dict[int, list[dict[str, Any]]] = {}
    for source_item_id in source_item_ids:
        payload = await fetch_xc_series_info_payload(int(source_item_id))
        if not isinstance(payload, dict):
            continue
        episodes_by_season = payload.get("episodes")
        if not isinstance(episodes_by_season, dict):
            continue
        for season_key, entries in episodes_by_season.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                entry_info = entry.get("info") if isinstance(entry.get("info"), dict) else {}
                summary = entry_info or entry
                grouped.setdefault(int(source_item_id), []).append(
                    {
                        "upstream_episode_id": clean_text(entry.get("id") or entry.get("stream_id")),
                        "title": clean_text(entry.get("title")) or "Episode",
                        "season_number": convert_to_int(entry.get("season") or season_key, 0),
                        "episode_number": convert_to_int(entry.get("episode_num"), 0),
                        "release_date": clean_text(
                            entry_info.get("releaseDate")
                            or entry_info.get("releasedate")
                            or entry_info.get("air_date")
                            or entry.get("releaseDate")
                        ),
                        "duration_seconds": _parse_duration_seconds(summary, VOD_KIND_SERIES),
                        "container_extension": clean_text(entry.get("container_extension")),
                        "summary": summary,
                    }
                )
    return grouped


def _sort_entries(entries: list[dict[str, Any]], settings: dict[str, Any], channel_id: int) -> list[dict[str, Any]]:
    mode = settings["schedule_mode"]
    reverse = settings["schedule_direction"] == "desc"
    if mode == SCHEDULE_MODE_SHUFFLE:
        rng = random.Random(int(channel_id))
        copied = list(entries)
        rng.shuffle(copied)
        return copied

    def key(entry: dict[str, Any]):
        if mode == SCHEDULE_MODE_RELEASE_DATE:
            return (
                _parse_release_sort_value(entry.get("release_date")),
                clean_text(entry.get("sort_title") or entry.get("title")).lower(),
                convert_to_int(entry.get("season_number"), 0),
                convert_to_int(entry.get("episode_number"), 0),
            )
        if mode == SCHEDULE_MODE_SEASON_AIR_DATE:
            return (
                _parse_release_sort_value(entry.get("series_release_date")),
                convert_to_int(entry.get("season_number"), 0),
                _parse_release_sort_value(entry.get("release_date")),
                convert_to_int(entry.get("episode_number"), 0),
                clean_text(entry.get("title")).lower(),
            )
        if mode == SCHEDULE_MODE_EPISODE_AIR_DATE:
            return (
                _parse_release_sort_value(entry.get("release_date")),
                clean_text(entry.get("series_title") or entry.get("title")).lower(),
                convert_to_int(entry.get("season_number"), 0),
                convert_to_int(entry.get("episode_number"), 0),
            )
        return (
            clean_text(entry.get("series_title") or entry.get("sort_title") or entry.get("title")).lower(),
            convert_to_int(entry.get("season_number"), 0),
            convert_to_int(entry.get("episode_number"), 0),
            clean_text(entry.get("title")).lower(),
        )

    return sorted(entries, key=key, reverse=reverse)


async def build_vod_channel_schedule(config: Any, channel_id: int, force: bool = False) -> dict[str, Any]:
    async with Session() as session:
        result = await session.execute(
            select(Channel).options(selectinload(Channel.vod_channel_rules)).where(Channel.id == int(channel_id))
        )
        channel = result.scalars().unique().one_or_none()
    if channel is None or not is_vod_channel_type(getattr(channel, "channel_type", None)):
        return {"entries": [], "eligible_items": []}

    rules = serialise_vod_channel_rules(channel)
    settings = read_vod_channel_settings(
        {
            "schedule_mode": getattr(channel, "vod_schedule_mode", None),
            "schedule_direction": getattr(channel, "vod_schedule_direction", None),
        }
    )
    eligible_items = await resolve_vod_channel_item_pool(config, channel)
    fingerprint = _content_fingerprint(int(channel.id), settings, rules, eligible_items)
    schedule_path = vod_channel_schedule_path(config, int(channel.id))
    if not force and schedule_path.exists():
        try:
            cached = json.loads(schedule_path.read_text(encoding="utf-8"))
            if clean_text(cached.get("fingerprint")) == fingerprint:
                return cached
        except Exception:
            pass

    episode_map = await _load_series_episode_payloads(
        [row["source_item_id"] for row in eligible_items if row["item_type"] == VOD_KIND_SERIES]
    )
    entries: list[dict[str, Any]] = []
    for item in eligible_items:
        if item["item_type"] == VOD_KIND_MOVIE:
            entries.append(
                {
                    "entry_type": VOD_KIND_MOVIE,
                    "item_id": item["item_id"],
                    "source_item_id": item["source_item_id"],
                    "title": item["title"],
                    "series_title": item["title"],
                    "sort_title": item["sort_title"],
                    "release_date": item["release_date"],
                    "series_release_date": item["release_date"],
                    "duration_seconds": item["duration_seconds"],
                    "container_extension": item["container_extension"],
                    "poster_url": item["poster_url"],
                    "desc": clean_text(item["summary"].get("plot") or item["summary"].get("description")),
                }
            )
            continue
        for episode in episode_map.get(int(item["source_item_id"]), []):
            entries.append(
                {
                    "entry_type": VOD_KIND_SERIES,
                    "item_id": item["item_id"],
                    "source_item_id": item["source_item_id"],
                    "upstream_episode_id": episode["upstream_episode_id"],
                    "title": episode["title"] or item["title"],
                    "series_title": item["title"],
                    "sort_title": item["sort_title"],
                    "release_date": episode["release_date"] or item["release_date"],
                    "series_release_date": item["release_date"],
                    "season_number": episode["season_number"],
                    "episode_number": episode["episode_number"],
                    "duration_seconds": episode["duration_seconds"],
                    "container_extension": episode["container_extension"] or item["container_extension"],
                    "poster_url": item["poster_url"],
                    "desc": clean_text(episode["summary"].get("plot") or episode["summary"].get("description")),
                }
            )
    ordered_entries = _sort_entries(entries, settings, int(channel.id))
    ordered_entries = [entry for entry in ordered_entries if convert_to_int(entry.get("duration_seconds"), 0) >= 60]

    now = utc_now()
    window_start = (now - timedelta(days=DEFAULT_SCHEDULE_DAYS_BEHIND)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    cycle_duration_seconds = sum(max(0, convert_to_int(entry.get("duration_seconds"), 0)) for entry in ordered_entries)
    target_window_hours = MIN_EPG_WINDOW_HOURS
    if cycle_duration_seconds > 0:
        cycle_window_hours = max(1, (cycle_duration_seconds + 3599) // 3600)
        target_window_hours = max(MIN_EPG_WINDOW_HOURS, min(MAX_EPG_WINDOW_HOURS, int(cycle_window_hours)))
    window_end = now + timedelta(hours=target_window_hours)
    schedule_entries: list[dict[str, Any]] = []
    if ordered_entries:
        cursor = window_start
        max_loops = max(1, int((window_end - window_start).total_seconds() // 60))
        loop_count = 0
        while cursor < window_end and loop_count < max_loops:
            for entry in ordered_entries:
                duration_seconds = convert_to_int(entry.get("duration_seconds"), 0)
                if duration_seconds <= 0:
                    continue
                start_ts = int(cursor.timestamp())
                stop_ts = start_ts + duration_seconds
                schedule_entries.append(
                    {
                        **entry,
                        "start_ts": start_ts,
                        "stop_ts": stop_ts,
                    }
                )
                cursor = datetime.fromtimestamp(stop_ts, tz=timezone.utc)
                if cursor >= window_end:
                    break
            loop_count += 1

    payload = {
        "channel_id": int(channel.id),
        "channel_name": clean_text(channel.name),
        "fingerprint": fingerprint,
        "generated_at": int(time.time()),
        "settings": settings,
        "eligible_items": eligible_items,
        "entries": schedule_entries,
    }
    schedule_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return payload


async def read_vod_channel_schedule(config: Any, channel_id: int, force: bool = False) -> dict[str, Any]:
    if force:
        return await build_vod_channel_schedule(config, channel_id, force=True)
    schedule_path = vod_channel_schedule_path(config, int(channel_id))
    if schedule_path.exists():
        try:
            return json.loads(schedule_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return await build_vod_channel_schedule(config, channel_id, force=False)


async def read_vod_channel_current_programme(
    config: Any, channel_id: int, now_ts: int | None = None
) -> dict[str, Any] | None:
    schedule = await read_vod_channel_schedule(config, channel_id)
    current_ts = int(now_ts or time.time())
    for entry in schedule.get("entries") or []:
        if int(entry.get("start_ts") or 0) <= current_ts < int(entry.get("stop_ts") or 0):
            return entry
    return None


async def read_vod_channel_current_and_next_programme(
    config: Any, channel_id: int, now_ts: int | None = None
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    schedule = await read_vod_channel_schedule(config, channel_id)
    current_ts = int(now_ts or time.time())
    entries = schedule.get("entries") or []
    current_entry = None
    next_entry = None
    for index, entry in enumerate(entries):
        if int(entry.get("start_ts") or 0) <= current_ts < int(entry.get("stop_ts") or 0):
            current_entry = entry
            if index + 1 < len(entries):
                next_entry = entries[index + 1]
            break
    return current_entry, next_entry


async def resolve_vod_channel_playback_target(
    config: Any, channel_id: int, now_ts: int | None = None
) -> dict[str, Any] | None:
    entry, next_entry = await read_vod_channel_current_and_next_programme(config, channel_id, now_ts=now_ts)
    if entry is None:
        return None
    current_ts = int(now_ts or time.time())
    offset_seconds = max(0, current_ts - int(entry["start_ts"]))
    source_item, upstream_url, xc_account, selection_error = await resolve_xc_item_upstream_url(
        int(entry["source_item_id"]),
        clean_text(entry["entry_type"]),
        upstream_episode_id=entry.get("upstream_episode_id"),
        container_extension=entry.get("container_extension"),
    )
    candidate = None
    episode = None
    async with Session() as session:
        if clean_text(entry.get("entry_type")) == VOD_KIND_MOVIE:
            result = await session.execute(
                select(VodCategoryItemSource, VodCategoryItem, VodCategory)
                .join(VodCategoryItem, VodCategoryItem.id == VodCategoryItemSource.category_item_id)
                .join(VodCategory, VodCategory.id == VodCategoryItem.category_id)
                .where(VodCategoryItemSource.source_item_id == int(entry["source_item_id"]))
                .options(joinedload(VodCategoryItemSource.source_item))
                .order_by(VodCategoryItemSource.id.asc())
                .limit(1)
            )
            row = result.first()
            if row is not None:
                source_link, group_item, group = row
                candidate = VodPlaybackCandidate(
                    group_item=group_item,
                    source_link=source_link,
                    source_item=source_link.source_item,
                    group=group,
                    content_type=VOD_KIND_MOVIE,
                    xc_account=xc_account,
                    host_url=None,
                    episode_source=None,
                    episode=None,
                )
        else:
            result = await session.execute(
                select(
                    VodCategoryEpisode,
                    VodCategoryEpisodeSource,
                    VodCategoryItemSource,
                    VodCategoryItem,
                    VodCategory,
                )
                .join(VodCategoryEpisodeSource, VodCategoryEpisodeSource.episode_id == VodCategoryEpisode.id)
                .join(
                    VodCategoryItemSource, VodCategoryItemSource.id == VodCategoryEpisodeSource.category_item_source_id
                )
                .join(VodCategoryItem, VodCategoryItem.id == VodCategoryItemSource.category_item_id)
                .join(VodCategory, VodCategory.id == VodCategoryItem.category_id)
                .where(
                    VodCategoryItemSource.source_item_id == int(entry["source_item_id"]),
                    VodCategoryEpisodeSource.upstream_episode_id == clean_text(entry.get("upstream_episode_id")),
                )
                .options(joinedload(VodCategoryItemSource.source_item))
                .order_by(VodCategoryEpisodeSource.id.asc())
                .limit(1)
            )
            row = result.first()
            if row is not None:
                episode, episode_source, source_link, group_item, group = row
                candidate = VodPlaybackCandidate(
                    group_item=group_item,
                    source_link=source_link,
                    source_item=source_link.source_item,
                    group=group,
                    content_type=VOD_KIND_SERIES,
                    xc_account=xc_account,
                    host_url=None,
                    episode_source=episode_source,
                    episode=episode,
                )
    return {
        "entry": entry,
        "next_entry": next_entry,
        "offset_seconds": offset_seconds,
        "candidate": candidate,
        "episode": episode,
        "source_item": source_item,
        "xc_account": xc_account,
        "upstream_url": upstream_url,
        "selection_error": selection_error,
    }


def build_xmltv_programmes(schedule: dict[str, Any], channel_tags: list[str]) -> list[dict[str, Any]]:
    programmes = []
    for entry in schedule.get("entries") or []:
        categories = list(channel_tags or [])
        if entry.get("entry_type") == VOD_KIND_MOVIE:
            categories.append("Movie")
        else:
            categories.extend(["Series", "Episode"])
        title = clean_text(entry.get("series_title") or entry.get("title"))
        sub_title = clean_text(entry.get("title")) if entry.get("entry_type") == VOD_KIND_SERIES else ""
        epnum_onscreen = ""
        season_number = convert_to_int(entry.get("season_number"), 0)
        episode_number = convert_to_int(entry.get("episode_number"), 0)
        if season_number > 0 or episode_number > 0:
            epnum_onscreen = f"S{season_number:02d}E{episode_number:02d}"
        programmes.append(
            {
                "start_ts": int(entry["start_ts"]),
                "stop_ts": int(entry["stop_ts"]),
                "title": title,
                "sub_title": sub_title,
                "desc": clean_text(entry.get("desc")),
                "icon_url": clean_text(entry.get("poster_url")),
                "categories": sorted({category for category in categories if clean_text(category)}),
                "epnum_onscreen": epnum_onscreen,
            }
        )
    return programmes


async def subscribe_vod_channel_stream(
    config: Any,
    channel_id: int,
    stream_key: str | None = None,
    profile: str = "default",
    connection_id: str = "",
    request_headers: dict[str, Any] | None = None,
) -> tuple[AsyncIterator[bytes] | None, str | None, str | None, int | None]:
    from backend.cso import subscribe_vod_proxy_output_stream, warm_vod_cache

    playback = await resolve_vod_channel_playback_target(config, channel_id)
    if not playback:
        return None, None, "No scheduled VOD programme is currently available", 404
    source_item = playback.get("source_item")
    xc_account = playback.get("xc_account")
    upstream_url = clean_text(playback.get("upstream_url"))
    entry = playback["entry"]
    next_entry = playback.get("next_entry")
    offset_seconds = convert_to_int(playback.get("offset_seconds"), 0)
    remaining_seconds = max(1, convert_to_int(entry.get("stop_ts"), 0) - int(time.time()))
    if source_item is None or not upstream_url:
        return None, None, "Unable to resolve VOD playback source", 503
    candidate = playback.get("candidate")
    if candidate is None:
        return None, None, "Unable to resolve VOD playback mapping", 503

    async def _warm_next_item_cache():
        if not next_entry:
            return
        next_start_ts = int(next_entry.get("start_ts") or 0)
        wait_seconds = max(0, remaining_seconds - NEXT_ITEM_CACHE_WARM_SECONDS)
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
        owner_key = f"vod-channel-next-{int(channel_id)}-{next_start_ts}"
        while True:
            now_ts = int(time.time())
            if next_start_ts > 0 and now_ts >= next_start_ts:
                return

            next_playback = await resolve_vod_channel_playback_target(
                config,
                channel_id,
                now_ts=next_start_ts or now_ts,
            )
            if next_playback:
                next_candidate = next_playback.get("candidate")
                next_upstream_url = clean_text(next_playback.get("upstream_url"))
                next_episode = next_playback.get("episode")
                next_source_item = next_playback.get("source_item")
                if next_source_item is not None and next_upstream_url:
                    if next_candidate is None:
                        continue
                    warmed = await warm_vod_cache(
                        next_candidate, next_upstream_url, episode=next_episode, owner_key=owner_key
                    )
                    if warmed:
                        logger.info(
                            "VOD channel next-item cache warmed channel=%s start_ts=%s source_item_id=%s",
                            int(channel_id),
                            next_start_ts,
                            int(getattr(next_source_item, "id", 0) or 0),
                        )
                        return

            # If the initial warm attempt hits source capacity, keep retrying until the next
            # programme starts. This lets single-slot sources warm the next item as soon as the
            # current download releases its slot.
            if next_start_ts > 0:
                remaining_to_start = next_start_ts - now_ts
                if remaining_to_start <= 1:
                    return
                await asyncio.sleep(min(5, max(1, remaining_to_start - 1)))
            else:
                await asyncio.sleep(5)

    warm_task = asyncio.create_task(_warm_next_item_cache(), name=f"vod-channel-next-{int(channel_id)}")
    plan = await subscribe_vod_proxy_output_stream(
        config,
        candidate,
        upstream_url,
        stream_key,
        profile,
        connection_id or f"vod-channel-{int(channel_id)}",
        start_seconds=offset_seconds,
        max_duration_seconds=remaining_seconds,
        request_headers=request_headers,
    )
    if plan.generator is None:
        if not warm_task.done():
            warm_task.cancel()
        return plan.generator, plan.content_type, plan.error_message, plan.status_code

    async def _generator():
        try:
            async for chunk in plan.generator:
                yield chunk
        finally:
            if not warm_task.done():
                warm_task.cancel()

    return _generator(), plan.content_type, plan.error_message, plan.status_code
