#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import json
import logging
import os
import re
import shutil
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote, urlparse

import aiohttp
from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import joinedload, selectinload

from backend.models import (
    Playlist,
    Session,
    User,
    VodCategory,
    VodCategoryEpisode,
    VodCategoryEpisodeSource,
    VodCategoryItem,
    VodCategoryItemSource,
    VodCategoryXcCategory,
    XcAccount,
    XcVodCategory,
    XcVodItem,
    XcVodMetadataCache,
)
from backend.stream_activity import get_stream_activity_snapshot
from backend.stream_profiles import get_stream_profile_definitions
from backend.url_resolver import get_tvh_publish_base_url
from backend.users import user_has_admin_role
from backend.utils import as_naive_utc, clean_key, clean_text
from backend.xc_hosts import parse_xc_hosts

logger = logging.getLogger("tic.vod")

VOD_KIND_MOVIE = "movie"
VOD_KIND_SERIES = "series"
VOD_ACCESS_NONE = "none"
VOD_ACCESS_MOVIES = "movies"
VOD_ACCESS_SERIES = "series"
VOD_ACCESS_BOTH = "movies_series"
METADATA_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
VOD_SYNC_ITEM_BATCH_SIZE = 500
VOD_SYNC_SERIES_REFRESH_CONCURRENCY = 6
VOD_UPSTREAM_METADATA_RETRY_ATTEMPTS = 3
VOD_UPSTREAM_METADATA_RETRY_BASE_DELAY_SECONDS = 1.5
VOD_SYNC_BATCH_FAILURE_MIN_COUNT = 12
VOD_SYNC_BATCH_FAILURE_RATIO = 0.35

_CONTAINER_PROFILE_MAP = {
    "ts": "mpegts",
    "mpegts": "mpegts",
    "mkv": "matroska",
    "matroska": "matroska",
    "mp4": "mp4",
    "webm": "webm",
    "m3u8": "hls",
    "hls": "hls",
}
_SAFE_VOD_SOURCE_CONTAINERS = {"mp4", "mkv", "matroska"}
_FORCED_SAFE_VOD_PROFILE_BY_CONTAINER = {
    "avi": "matroska",
    "flv": "matroska",
}
_DEFAULT_UNSAFE_VOD_PROFILE = "matroska"
_VOD_STRM_ROOT = Path(os.environ.get("LIBRARY_EXPORT_PATH", "/library"))
_VOD_STRM_REGISTRY_FILE = ".tic-vod-registry.json"
_VOD_HTTP_LIBRARY_ROOT = _VOD_STRM_ROOT / ".tic-http-library"
_VOD_HTTP_LIBRARY_INDEX_FILE = "index.json"
_VOD_HTTP_MANIFEST_CACHE_TTL_SECONDS = 10
_vod_http_manifest_cache: dict[str, tuple[float, float, object]] = {}
_VOD_TITLE_MAX_LENGTH = 500


@dataclass
class VodCuratedPlaybackCandidate:
    group_item: VodCategoryItem
    source_link: VodCategoryItemSource
    source_item: XcVodItem
    group: VodCategory | None
    content_type: str
    xc_account: XcAccount | None
    host_url: str | None
    episode_source: VodCategoryEpisodeSource | None = None
    episode: VodCategoryEpisode | None = None


@dataclass
class VodSourcePlaybackCandidate:
    source_item: XcVodItem
    content_type: str
    xc_account: XcAccount | None
    host_url: str | None
    container_extension: str | None = None
    upstream_episode_id: str | None = None
    internal_id: int | None = None
    cache_internal_id: int | None = None


def _truncated_vod_text(value: object, limit: int = _VOD_TITLE_MAX_LENGTH) -> str:
    text = clean_text(value)
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit].rstrip()


def _vod_link_priority_value(link: VodCategoryXcCategory | None, fallback: int = 0) -> int:
    if link is None:
        return int(fallback)
    try:
        return int(getattr(link, "priority", fallback) or fallback)
    except Exception:
        return int(fallback)


def _ordered_vod_category_links(links) -> list[VodCategoryXcCategory]:
    ordered = list(links or [])
    if not ordered:
        return []
    fallback_rank = len(ordered)
    decorated = []
    for index, link in enumerate(ordered):
        priority = _vod_link_priority_value(link, fallback=fallback_rank - index)
        decorated.append((priority, index, link))
    decorated.sort(key=lambda item: (-item[0], item[1]))
    return [item[2] for item in decorated]


def _ordered_vod_category_ids(links) -> list[int]:
    return [
        int(link.xc_category_id) for link in _ordered_vod_category_links(links) if getattr(link, "xc_category_id", None)
    ]


def _vod_category_priority_map(links) -> dict[int, int]:
    ordered_links = _ordered_vod_category_links(links)
    fallback_rank = len(ordered_links)
    priority_map = {}
    for index, link in enumerate(ordered_links):
        category_id = int(getattr(link, "xc_category_id", 0) or 0)
        if category_id <= 0:
            continue
        priority_map[category_id] = _vod_link_priority_value(link, fallback=fallback_rank - index)
    return priority_map


def _build_category_config_map(raw_category_configs) -> dict[int, dict[str, object]]:
    category_config_map = {}
    category_count = len(raw_category_configs or [])
    for index, item in enumerate(raw_category_configs or []):
        if not isinstance(item, dict):
            continue
        category_id = item.get("category_id")
        if not str(category_id).isdigit():
            continue
        priority_value = item.get("priority")
        if str(priority_value).isdigit():
            priority = int(priority_value)
        else:
            priority = category_count - index
        category_config_map[int(category_id)] = {
            "priority": int(priority),
            "strip_title_prefixes": _strip_config_tokens(item.get("strip_title_prefixes")),
            "strip_title_suffixes": _strip_config_tokens(item.get("strip_title_suffixes")),
        }
    return category_config_map


def user_can_access_vod_kind(user: User | None, kind: str) -> bool:
    if user_has_admin_role(user):
        return True
    mode = str(getattr(user, "vod_access_mode", VOD_ACCESS_NONE) or VOD_ACCESS_NONE).strip().lower()
    if kind == VOD_KIND_MOVIE:
        return mode in {VOD_ACCESS_MOVIES, VOD_ACCESS_BOTH}
    if kind == VOD_KIND_SERIES:
        return mode in {VOD_ACCESS_SERIES, VOD_ACCESS_BOTH}
    return False


def user_has_vod_access(user: User | None) -> bool:
    if user_has_admin_role(user):
        return True
    mode = str(getattr(user, "vod_access_mode", VOD_ACCESS_NONE) or VOD_ACCESS_NONE).strip().lower()
    return mode in {VOD_ACCESS_MOVIES, VOD_ACCESS_SERIES, VOD_ACCESS_BOTH}


def require_vod_content_type(value) -> str:
    if value in {VOD_KIND_MOVIE, VOD_KIND_SERIES}:
        return value
    raise ValueError("content_type must be 'movie' or 'series'")


def build_vod_activity_metadata(candidate, episode=None) -> dict[str, str]:
    group_item = getattr(candidate, "group_item", None)
    series_title = clean_text(getattr(group_item, "title", ""))
    poster_url = clean_text(getattr(group_item, "poster_url", ""))
    release_year = clean_text(getattr(group_item, "year", ""))

    if episode is None:
        movie_title = series_title or "Movie"
        if release_year:
            movie_title = f"{movie_title} ({release_year})"
        return {
            "channel_name": movie_title,
            "channel_logo_url": poster_url,
            "stream_name": movie_title,
            "display_url": f"VOD Movie: {movie_title}",
        }

    episode_title = clean_text(getattr(episode, "title", ""))
    season_number = getattr(episode, "season_number", None)
    episode_number = getattr(episode, "episode_number", None)
    if season_number is not None and episode_number is not None:
        episode_label = f"S{int(season_number):02d}E{int(episode_number):02d}"
    elif episode_number is not None:
        episode_label = f"Episode {int(episode_number)}"
    else:
        episode_label = ""
    title_parts = [part for part in (series_title, episode_label) if part]
    activity_title = " - ".join(title_parts) or series_title or episode_title or "Series episode"
    display_parts = [part for part in (episode_label, episode_title) if part]
    display_value = f"VOD Series: {series_title}" if series_title else "Series episode"
    if display_parts:
        display_value = f"{display_value} ({' - '.join(display_parts)})"
    return {
        "channel_name": activity_title,
        "channel_logo_url": poster_url,
        "stream_name": episode_title or activity_title,
        "display_url": display_value,
    }


def build_local_cache_source(candidate: VodCuratedPlaybackCandidate, episode: VodCategoryEpisode | None = None):
    if candidate is None or getattr(candidate, "group_item", None) is None:
        return None
    internal_id = int(candidate.group_item.id)
    source_type = "vod_movie"
    if episode is not None:
        source_type = "vod_episode"
        internal_id = int(episode.id)
    from backend.cso import CsoSource

    return CsoSource(
        id=internal_id,
        source_type=source_type,
        url="",
        playlist_id=int(getattr(candidate.source_item, "playlist_id", 0) or 0),
        internal_id=internal_id,
        container_extension=getattr(candidate.episode_source, "container_extension", "")
        or getattr(candidate.source_item, "container_extension", "")
        or getattr(candidate.group_item, "container_extension", ""),
    )


async def vod_cache_is_complete(
    candidate: VodCuratedPlaybackCandidate, episode: VodCategoryEpisode | None = None
) -> bool:
    source = build_local_cache_source(candidate, episode=episode)
    if source is None:
        return False
    from backend.cso import vod_cache_manager

    entry = await vod_cache_manager.get(source)
    if entry is None:
        return False
    return bool(entry.complete and entry.final_path.exists())


async def vod_candidate_has_capacity(candidate: VodCuratedPlaybackCandidate, upstream_url: str) -> bool:
    from backend.cso import (
        cso_capacity_registry,
        cso_source_from_vod_source,
        source_capacity_key,
        source_capacity_limit,
    )

    source = await cso_source_from_vod_source(candidate, upstream_url)
    if source is None:
        return False
    limit = int(source_capacity_limit(source) or 0)
    if limit <= 0:
        return True
    usage = await cso_capacity_registry.get_usage(source_capacity_key(source))
    return int(usage.get("total") or 0) < limit


async def select_vod_playback_target(
    candidates: list[VodCuratedPlaybackCandidate],
    episode: VodCategoryEpisode | None = None,
    prefer_local_cache: bool = False,
) -> tuple[VodCuratedPlaybackCandidate | None, str | None, str | None]:
    if not candidates:
        return None, None, "not_found"
    preferred_candidate = candidates[0]
    if prefer_local_cache and await vod_cache_is_complete(preferred_candidate, episode=episode):
        return preferred_candidate, "", None

    blocked_capacity = False
    saw_stream_url = False
    for candidate in candidates:
        upstream_url = await build_upstream_playback_url(candidate, episode_mapping=episode)
        if not upstream_url:
            continue
        saw_stream_url = True
        if not await vod_candidate_has_capacity(candidate, upstream_url):
            blocked_capacity = True
            continue
        return candidate, upstream_url, None

    if blocked_capacity and saw_stream_url:
        return preferred_candidate, "", "capacity_blocked"
    return preferred_candidate, "", "stream_unavailable"


def _extract_year(payload: dict) -> str:
    for key in ("year", "releaseDate", "release_date", "releasedate"):
        value = clean_text(payload.get(key))
        if not value:
            continue
        digits = "".join(ch for ch in value if ch.isdigit())
        if len(digits) >= 4:
            return digits[:4]
    return ""


def _strip_config_tokens(raw_value: object) -> list[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return [clean_text(item) for item in raw_value if clean_text(item)]
    text = clean_text(raw_value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = None
    if isinstance(parsed, list):
        return [clean_text(item) for item in parsed if clean_text(item)]
    return [token.strip() for token in re.split(r"[\n,]+", text) if token.strip()]


def _store_strip_config(tokens: list[str] | None) -> str | None:
    cleaned = [clean_text(item) for item in (tokens or []) if clean_text(item)]
    if not cleaned:
        return None
    return json.dumps(cleaned, ensure_ascii=False)


def _group_category_strip_prefixes(group_category: VodCategoryXcCategory | None) -> list[str]:
    return _strip_config_tokens(getattr(group_category, "strip_title_prefixes", None))


def _group_category_strip_suffixes(group_category: VodCategoryXcCategory | None) -> list[str]:
    return _strip_config_tokens(getattr(group_category, "strip_title_suffixes", None))


def _remove_configured_affixes(value: str, prefixes: list[str] | None = None, suffixes: list[str] | None = None) -> str:
    text = clean_text(value)
    if not text:
        return ""
    changed = True
    prefix_tokens = [token for token in (prefixes or []) if token]
    suffix_tokens = [token for token in (suffixes or []) if token]

    def _remove_prefix_once(raw_text: str, token: str) -> str | None:
        token_value = clean_text(token)
        if not token_value:
            return None
        if raw_text[: len(token_value)].lower() == token_value.lower():
            return raw_text.removeprefix(raw_text[: len(token_value)])
        return None

    def _remove_suffix_once(raw_text: str, token: str) -> str | None:
        token_value = clean_text(token)
        if not token_value:
            return None
        if raw_text[-len(token_value) :].lower() == token_value.lower():
            return raw_text.removesuffix(raw_text[-len(token_value) :])
        return None

    while changed and text:
        changed = False
        for prefix in prefix_tokens:
            updated = _remove_prefix_once(text, prefix)
            if updated is not None:
                text = updated
                changed = True
                break
        if changed:
            continue
        for suffix in suffix_tokens:
            updated = _remove_suffix_once(text, suffix)
            if updated is not None:
                text = updated
                changed = True
                break
    return text


def _export_title_from_source_title(
    title: str, prefixes: list[str] | None = None, suffixes: list[str] | None = None
) -> str:
    cleaned_title = _remove_configured_affixes(title, prefixes=prefixes, suffixes=suffixes)
    cleaned_title = re.sub(r"\s+", " ", clean_text(cleaned_title))
    return cleaned_title or clean_text(title)


def _filtered_title(value: str, prefixes: list[str] | None = None, suffixes: list[str] | None = None) -> str:
    stripped = _remove_configured_affixes(value, prefixes=prefixes, suffixes=suffixes)
    filtered_text = stripped.casefold()
    filtered_text = re.sub(r"\s+", " ", filtered_text)
    return filtered_text.strip()


def _dedupe_key_from_values(
    kind: str,
    title: str,
    year: str,
    prefixes: list[str] | None = None,
    suffixes: list[str] | None = None,
) -> str:
    filtered_title = _filtered_title(title, prefixes=prefixes, suffixes=suffixes) or clean_key(title)
    return f"{kind}::{filtered_title}::{clean_text(year)}"


def _dedupe_key_for_item(item: XcVodItem, prefixes: list[str] | None = None, suffixes: list[str] | None = None) -> str:
    return _dedupe_key_from_values(item.item_type, item.title, item.year, prefixes=prefixes, suffixes=suffixes)


def _episode_dedupe_key(
    season_number: int | None,
    episode_number: int | None,
    title: str,
    prefixes: list[str] | None = None,
    suffixes: list[str] | None = None,
    tmdb_id: str | None = None,
) -> str:
    tmdb_value = clean_text(tmdb_id)
    if tmdb_value:
        return f"tmdb::{tmdb_value}"
    season_text = str(int(season_number)) if str(season_number).isdigit() else ""
    episode_text = str(int(episode_number)) if str(episode_number).isdigit() else ""
    if season_text or episode_text:
        return f"s{season_text}e{episode_text}"
    filtered_title = _filtered_title(title, prefixes=prefixes, suffixes=suffixes) or clean_key(title)
    return f"title::{filtered_title}"


def _summary_json(payload: dict[str, object]) -> str:
    return json.dumps(payload or {}, sort_keys=True)


def _load_summary(summary_json: str | None) -> dict[str, object]:
    try:
        parsed = json.loads(summary_json or "{}")
    except Exception:
        parsed = {}
    return parsed if isinstance(parsed, dict) else {}


def _summary_info(summary: dict[str, object]) -> dict[str, object]:
    info = summary.get("info")
    if isinstance(info, dict):
        return info
    if isinstance(info, list):
        for entry in info:
            if isinstance(entry, dict):
                return entry
    return {}


def _poster_url(payload: dict[str, object], kind: str) -> str:
    if kind == VOD_KIND_MOVIE:
        return clean_text(payload.get("stream_icon") or payload.get("cover") or payload.get("movie_image"))
    return clean_text(payload.get("cover") or payload.get("cover_big") or payload.get("stream_icon"))


def _container_extension(payload: dict[str, object]) -> str:
    text = clean_text(payload.get("container_extension")).lstrip(".").lower()
    if text:
        return text
    direct = clean_text(payload.get("direct_source"))
    if "." in urlparse(direct).path:
        return urlparse(direct).path.rsplit(".", 1)[1].strip().lower()
    return ""


def _profile_for_container(container_extension: str) -> str:
    return _CONTAINER_PROFILE_MAP.get(clean_text(container_extension).lower(), "mpegts")


def _profile_extension(profile_id: str, fallback_extension: str = "") -> str:
    definitions = {item["key"]: item for item in get_stream_profile_definitions()}
    profile = definitions.get(profile_id or "")
    container = clean_text((profile or {}).get("container"))
    mapping = {
        "mpegts": "ts",
        "matroska": "mkv",
        "mp4": "mp4",
        "webm": "webm",
        "hls": "m3u8",
    }
    return mapping.get(container, clean_text(fallback_extension).lower() or "ts")


def _resolve_group_output_profile_id(group_profile_id: str | None, source_container_extension: str) -> str:
    configured_profile = clean_text(group_profile_id)
    if configured_profile:
        return configured_profile
    return _fallback_profile_for_container(source_container_extension)


def _resolve_group_output_extension(group_profile_id: str | None, source_container_extension: str) -> str:
    effective_profile = _resolve_group_output_profile_id(group_profile_id, source_container_extension)
    return _profile_extension(effective_profile, fallback_extension=source_container_extension)


def _merge_strip_rules_for_source_categories(
    source_category_ids: list[int], strip_rules: dict[int, tuple[list[str], list[str]]]
) -> tuple[list[str], list[str]]:
    merged_prefixes: list[str] = []
    merged_suffixes: list[str] = []
    seen_prefixes: set[str] = set()
    seen_suffixes: set[str] = set()

    for source_category_id in source_category_ids or []:
        prefixes, suffixes = strip_rules.get(int(source_category_id), ([], []))
        for token in prefixes:
            token_key = str(token)
            if token_key in seen_prefixes:
                continue
            seen_prefixes.add(token_key)
            merged_prefixes.append(token)
        for token in suffixes:
            token_key = str(token)
            if token_key in seen_suffixes:
                continue
            seen_suffixes.add(token_key)
            merged_suffixes.append(token)

    return merged_prefixes, merged_suffixes


def _batched(values, size: int = 2000):
    batch_size = max(1, int(size or 2000))
    items = list(values or [])
    for index in range(0, len(items), batch_size):
        yield items[index : index + batch_size]


def _metadata_expiry(now_utc: datetime | None = None) -> datetime | None:
    now_value = now_utc or datetime.now(timezone.utc)
    return as_naive_utc(now_value + timedelta(seconds=METADATA_CACHE_TTL_SECONDS))


def _is_supported_passthrough_container(container_extension: str) -> bool:
    return clean_text(container_extension).lower() in _SAFE_VOD_SOURCE_CONTAINERS


def _fallback_profile_for_container(container_extension: str) -> str:
    container_key = clean_text(container_extension).lower()
    if _is_supported_passthrough_container(container_key):
        return _profile_for_container(container_key)
    return _FORCED_SAFE_VOD_PROFILE_BY_CONTAINER.get(container_key, _DEFAULT_UNSAFE_VOD_PROFILE)


def _load_payload_json(payload_json: str | None) -> dict | list | None:
    try:
        payload = json.loads(payload_json or "{}")
    except Exception:
        payload = None
    return payload if isinstance(payload, (dict, list)) else None


def _cache_payload_json(payload: object) -> str:
    return json.dumps(payload if payload is not None else {}, sort_keys=True)


def _vod_category_type_dir_name(content_type: str) -> str:
    return "Shows" if content_type == VOD_KIND_SERIES else "Movies"


def _vod_export_slug(value: object, fallback: str = "category") -> str:
    text = clean_text(value).casefold()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text or clean_text(fallback) or "category"


def _vod_safe_name(value: object, fallback: str = "item") -> str:
    text = clean_text(value) or clean_text(fallback) or "item"
    text = re.sub(r'[\\/:*?"<>|]+', " ", text)
    text = re.sub(r"\s+", " ", text).strip(" .")
    return text or clean_text(fallback) or "item"


def _vod_title_display_name(raw_title: object, explicit_year: object, fallback: str) -> str:
    title_text = clean_text(raw_title)
    suffix_match = re.search(r"(\s+\[[^\]]+\])\s*$", title_text)
    suffix = clean_text(suffix_match.group(1)) if suffix_match else ""
    title_without_suffix = title_text[: suffix_match.start()].strip() if suffix_match else title_text
    year = clean_text(explicit_year) or _extract_year_from_title(title_without_suffix)

    title = title_without_suffix
    if year:
        title = re.sub(rf"[\s\-()]*{re.escape(year)}\s*$", "", title).strip(" -_:|*[]()")
    title = _vod_safe_name(title, fallback=fallback)

    display_name = f"{title} ({year})" if year else title
    if suffix:
        display_name = f"{display_name} {suffix}"
    return _vod_safe_name(display_name, fallback=fallback)


def _vod_movie_display_name(
    item: VodCategoryItem, prefixes: list[str] | None = None, suffixes: list[str] | None = None
) -> str:
    title = _export_title_from_source_title(getattr(item, "title", ""), prefixes=prefixes, suffixes=suffixes)
    return _vod_title_display_name(title, getattr(item, "year", ""), fallback=f"Movie {int(item.id)}")


def _vod_series_display_name(
    item: VodCategoryItem, prefixes: list[str] | None = None, suffixes: list[str] | None = None
) -> str:
    title = _export_title_from_source_title(getattr(item, "title", ""), prefixes=prefixes, suffixes=suffixes)
    return _vod_title_display_name(title, getattr(item, "year", ""), fallback=f"Series {int(item.id)}")


def _vod_episode_display_name(
    series_name: str,
    episode: VodCategoryEpisode,
    prefixes: list[str] | None = None,
    suffixes: list[str] | None = None,
) -> str:
    season_number = int(getattr(episode, "season_number", 0) or 0)
    episode_number = int(getattr(episode, "episode_number", 0) or 0)
    # Jellyfin recommended: Series Name SXXEXX
    # We remove the (Year) from series_name for the file name if present
    base_series_name = re.sub(r"\s\(\d{4}\)$", "", series_name).strip()
    return f"{base_series_name} S{season_number:02d}E{episode_number:02d}"


def _vod_season_dir_name(episode: VodCategoryEpisode) -> str:
    season_number = int(getattr(episode, "season_number", 0) or 0)
    return f"Season {season_number:02d}"


def load_vod_strm_registry_sync() -> dict[str, object]:
    registry_path = _VOD_STRM_ROOT / _VOD_STRM_REGISTRY_FILE
    if not registry_path.exists():
        return {}
    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}

    registry: dict[str, object] = {}
    for key, entry in payload.items():
        if not isinstance(key, str) or not isinstance(entry, dict):
            continue
        relative_dir = entry.get("relative_dir")
        if not isinstance(relative_dir, str) or not relative_dir:
            continue
        files = entry.get("files")
        dirs = entry.get("dirs")
        registry[key] = {
            "relative_dir": relative_dir,
            "files": [value for value in files if isinstance(value, str) and value] if isinstance(files, list) else [],
            "dirs": [value for value in dirs if isinstance(value, str) and value] if isinstance(dirs, list) else [],
        }
    return registry


def write_vod_strm_registry_sync(registry: dict[str, object]):
    _VOD_STRM_ROOT.mkdir(parents=True, exist_ok=True)
    registry_path = _VOD_STRM_ROOT / _VOD_STRM_REGISTRY_FILE
    registry_path.write_text(json.dumps(registry, indent=2, sort_keys=True), encoding="utf-8")


def remove_vod_export_path_sync(relative_path: str):
    if not relative_path:
        return
    target_path = _VOD_STRM_ROOT / relative_path
    if target_path.exists():
        shutil.rmtree(target_path, ignore_errors=True)


def _vod_export_registry_key(user_id: int, category_id: int) -> str:
    return f"{int(user_id)}:{int(category_id)}"


def _scan_vod_export_tree_sync(relative_dir: str) -> tuple[set[str], set[str]]:
    if not relative_dir:
        return set(), set()
    target_dir = _VOD_STRM_ROOT / relative_dir
    if not target_dir.exists():
        return set(), set()

    tracked_dirs = {relative_dir}
    tracked_files = set()
    for current_root, dir_names, file_names in os.walk(target_dir):
        current_path = Path(current_root)
        current_relative = current_path.relative_to(_VOD_STRM_ROOT).as_posix()
        if current_relative:
            tracked_dirs.add(current_relative)
        for dir_name in dir_names:
            tracked_dirs.add((current_path / dir_name).relative_to(_VOD_STRM_ROOT).as_posix())
        for file_name in file_names:
            tracked_files.add((current_path / file_name).relative_to(_VOD_STRM_ROOT).as_posix())
    return tracked_files, tracked_dirs


def _remove_vod_export_file_sync(relative_path: str):
    if not relative_path:
        return
    target_path = _VOD_STRM_ROOT / relative_path
    if target_path.exists():
        target_path.unlink(missing_ok=True)


def _remove_vod_export_dir_if_empty_sync(relative_path: str):
    if not relative_path:
        return
    target_path = _VOD_STRM_ROOT / relative_path
    if not target_path.exists():
        return
    try:
        target_path.rmdir()
    except OSError:
        return


def _vod_export_base_url_from_settings(config) -> str:
    settings = config.read_settings() if config else {}
    app_url = clean_text((settings.get("settings") or {}).get("app_url"))
    parsed = urlparse(app_url)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return app_url.rstrip("/")
    return ""


def _category_requires_vod_library_sync(category: VodCategory | None) -> bool:
    if category is None:
        return False
    return bool(getattr(category, "generate_strm_files", False)) or bool(
        getattr(category, "expose_http_library", False)
    )


def _vod_http_library_index_path() -> Path:
    return _VOD_HTTP_LIBRARY_ROOT / _VOD_HTTP_LIBRARY_INDEX_FILE


def _vod_http_category_manifest_relpath(content_type: str, category_id: int) -> str:
    manifest_dir = "shows" if content_type == VOD_KIND_SERIES else "movies"
    return f"{manifest_dir}/{int(category_id)}.json"


def _vod_http_category_manifest_path(content_type: str, category_id: int) -> Path:
    return _VOD_HTTP_LIBRARY_ROOT / _vod_http_category_manifest_relpath(content_type, category_id)


def _load_json_file_sync(file_path: Path) -> dict[str, object]:
    if not file_path.exists():
        return {}
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json_file_sync(file_path: Path, payload: dict[str, object]):
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def load_vod_http_library_index_sync() -> dict[str, object]:
    return _load_json_file_sync(_vod_http_library_index_path())


def _write_vod_http_library_index_sync(index_payload: dict[str, object]):
    _VOD_HTTP_LIBRARY_ROOT.mkdir(parents=True, exist_ok=True)
    _write_json_file_sync(_vod_http_library_index_path(), index_payload)


def remove_vod_http_library_manifest_sync(content_type: str, category_id: int):
    manifest_path = _vod_http_category_manifest_path(content_type, category_id)
    if manifest_path.exists():
        manifest_path.unlink(missing_ok=True)
    cache_key = str(manifest_path)
    _vod_http_manifest_cache.pop(cache_key, None)


def _upsert_vod_http_library_index_entry_sync(category: VodCategory):
    index_payload = load_vod_http_library_index_sync()
    categories = [row for row in (index_payload.get("categories") or []) if isinstance(row, dict)]
    category_id = int(category.id)
    manifest_relpath = _vod_http_category_manifest_relpath(category.content_type, category_id)
    entry = {
        "category_id": category_id,
        "category_name": category.name,
        "content_type": category.content_type,
        "content_dir": _vod_category_type_dir_name(category.content_type),
        "category_slug": _vod_export_slug(category.name, fallback=f"category-{category_id}"),
        "root_path": f"{_vod_category_type_dir_name(category.content_type)}/{_vod_export_slug(category.name, fallback=f'category-{category_id}')}",
        "manifest_path": manifest_relpath,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    next_categories = [row for row in categories if int(row.get("category_id") or 0) != category_id]
    next_categories.append(entry)
    next_categories.sort(
        key=lambda row: (
            clean_key(row.get("content_type")),
            clean_text(row.get("category_name")).casefold(),
            int(row.get("category_id") or 0),
        )
    )
    index_payload["categories"] = next_categories
    index_payload["generated_at"] = datetime.now(timezone.utc).isoformat()
    _write_vod_http_library_index_sync(index_payload)


def remove_vod_http_library_index_entry_sync(category_id: int):
    index_payload = load_vod_http_library_index_sync()
    categories = [row for row in (index_payload.get("categories") or []) if isinstance(row, dict)]
    next_categories = [row for row in categories if int(row.get("category_id") or 0) != int(category_id)]
    index_payload["categories"] = next_categories
    index_payload["generated_at"] = datetime.now(timezone.utc).isoformat()
    _write_vod_http_library_index_sync(index_payload)


def _ensure_unique_relative_path(relative_path: Path, used_paths: set[str], unique_suffix: str) -> Path:
    suffix = relative_path.suffix
    stem = relative_path.stem
    parent = relative_path.parent
    candidate = relative_path
    if str(candidate) not in used_paths:
        used_paths.add(str(candidate))
        return candidate
    candidate = parent / f"{stem} [{unique_suffix}]{suffix}"
    counter = 2
    while str(candidate) in used_paths:
        candidate = parent / f"{stem} [{unique_suffix}-{counter}]{suffix}"
        counter += 1
    used_paths.add(str(candidate))
    return candidate


def _vod_manifest_rel_key(relative_path: Path | str | None = None) -> str:
    text = str(relative_path or "").replace("\\", "/").strip("/")
    return text


def _vod_http_manifest_dir_node() -> dict[str, object]:
    return {"type": "dir", "children": []}


def _vod_http_manifest_add_child(
    manifest: dict[str, object], parent_key: str, child_name: str, child_path: str, child_kind: str
):
    nodes = manifest.setdefault("nodes", {})
    parent_node = nodes.setdefault(parent_key, _vod_http_manifest_dir_node())
    children = parent_node.setdefault("children", [])
    child_entry = {"name": child_name, "path": child_path, "kind": child_kind}
    if child_entry not in children:
        children.append(child_entry)


def _vod_http_manifest_ensure_dir(manifest: dict[str, object], relative_dir: Path | str | None = None):
    nodes = manifest.setdefault("nodes", {})
    dir_key = _vod_manifest_rel_key(relative_dir)
    if dir_key in nodes:
        return
    nodes[dir_key] = _vod_http_manifest_dir_node()
    if not dir_key:
        return
    dir_path = Path(dir_key)
    parent_key = _vod_manifest_rel_key(dir_path.parent if str(dir_path.parent) != "." else "")
    _vod_http_manifest_ensure_dir(manifest, parent_key)
    _vod_http_manifest_add_child(manifest, parent_key, dir_path.name, f"{dir_key}/", "dir")


def _vod_http_manifest_add_file(
    manifest: dict[str, object], relative_path: Path, node_type: str, metadata: dict[str, object]
):
    relative_key = _vod_manifest_rel_key(relative_path)
    parent_key = _vod_manifest_rel_key(relative_path.parent if str(relative_path.parent) != "." else "")
    _vod_http_manifest_ensure_dir(manifest, parent_key)
    manifest.setdefault("nodes", {})[relative_key] = {"type": node_type, **metadata}
    _vod_http_manifest_add_child(manifest, parent_key, relative_path.name, relative_key, node_type)


def _build_vod_http_category_manifest(category: VodCategory) -> dict[str, object]:
    return {
        "category_id": int(category.id),
        "category_name": category.name,
        "category_slug": _vod_export_slug(category.name, fallback=f"category-{int(category.id)}"),
        "content_type": category.content_type,
        "content_dir": _vod_category_type_dir_name(category.content_type),
        "root_path": f"{_vod_category_type_dir_name(category.content_type)}/{_vod_export_slug(category.name, fallback=f'category-{int(category.id)}')}",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "nodes": {"": _vod_http_manifest_dir_node()},
    }


def _write_vod_http_category_manifest_sync(category: VodCategory, manifest: dict[str, object], include_in_index: bool):
    manifest_path = _vod_http_category_manifest_path(category.content_type, int(category.id))
    _write_json_file_sync(manifest_path, manifest)
    _vod_http_manifest_cache.pop(str(manifest_path), None)
    if include_in_index:
        _upsert_vod_http_library_index_entry_sync(category)
        return
    remove_vod_http_library_index_entry_sync(int(category.id))


def _write_text_file_if_changed(file_path: Path, content: str) -> bool:
    """Write a text file only when its content has actually changed."""
    full_path = _VOD_STRM_ROOT / file_path
    full_path.parent.mkdir(parents=True, exist_ok=True)
    if full_path.exists():
        try:
            if full_path.read_text(encoding="utf-8") == content:
                return False
        except Exception:
            pass
    full_path.write_text(content, encoding="utf-8")
    return True


def _sync_vod_category_export_files_sync(category_id: int, export_states: dict[str, dict[str, object]]):
    """Remove stale export paths for a category and write the files described by this run."""
    _VOD_STRM_ROOT.mkdir(parents=True, exist_ok=True)
    registry = load_vod_strm_registry_sync()
    category_suffix = f":{int(category_id)}"
    desired_keys = set(export_states.keys())

    # Remove old export folders that should not exist any more.
    for key in list(registry.keys()):
        if not key.endswith(category_suffix):
            continue
        entry = registry.get(key)
        old_rel_path = entry["relative_dir"] if entry else ""
        state = export_states.get(key)
        if state is None:
            if old_rel_path:
                remove_vod_export_path_sync(old_rel_path)
            registry.pop(key, None)
            continue

        new_rel_path = str(state["relative_dir"])
        if old_rel_path and new_rel_path and old_rel_path != new_rel_path:
            remove_vod_export_path_sync(old_rel_path)
            registry.pop(key, None)

    # Go through each export folder and compare it with what we just built.
    for key, state in export_states.items():
        relative_dir = state["relative_dir"]
        target_dir = _VOD_STRM_ROOT / relative_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        existing_entry = registry.get(key)
        tracked_files = set(existing_entry["files"]) if existing_entry else set()
        tracked_dirs = set(existing_entry["dirs"]) if existing_entry else set()
        if not tracked_files and not tracked_dirs:
            tracked_files, tracked_dirs = _scan_vod_export_tree_sync(relative_dir)

        expected_files = set(state["files"].keys())
        expected_dirs = set(state["dirs"])
        expected_dirs.add(relative_dir)

        # Delete files we had before but did not write this time.
        for stale_file in sorted(tracked_files - expected_files):
            _remove_vod_export_file_sync(stale_file)

        # Delete empty folders from the bottom up.
        stale_dirs = sorted(tracked_dirs - expected_dirs, key=lambda value: (value.count("/"), value), reverse=True)
        for stale_dir in stale_dirs:
            _remove_vod_export_dir_if_empty_sync(stale_dir)

        for file_path, content in state["files"].items():
            _write_text_file_if_changed(Path(file_path), content)

        registry[key] = {
            "relative_dir": relative_dir,
            "files": sorted(expected_files),
            "dirs": sorted(expected_dirs),
        }

    # Remove old registry entries after their folders are gone.
    for key in [
        entry_key
        for entry_key in registry.keys()
        if entry_key.endswith(category_suffix) and entry_key not in desired_keys
    ]:
        registry.pop(key, None)

    write_vod_strm_registry_sync(registry)


def _xml_text(parent, tag: str, text: object, attrib: dict[str, str] | None = None):
    if text:
        el = ET.SubElement(parent, tag, attrib or {})
        el.text = str(text)
        return el
    return None


def _xml_multi_text(parent, tag: str, values: list[str]):
    for value in values:
        _xml_text(parent, tag, value)


def _summary_movie_info(summary: dict[str, object]) -> dict[str, object]:
    movie_data = summary.get("movie_data")
    if isinstance(movie_data, dict):
        return movie_data
    return {}


def _summary_sources(summary: dict[str, object]) -> list[dict[str, object]]:
    sources = [summary, _summary_info(summary), _summary_movie_info(summary)]
    return [source for source in sources if isinstance(source, dict)]


def _first_summary_value(summary: dict[str, object], *keys: str):
    for source in _summary_sources(summary):
        for key in keys:
            value = source.get(key)
            if value not in (None, "", [], {}):
                return value
    return None


def _extract_year_from_title(value: str) -> str:
    text = clean_text(value)
    if not text:
        return ""
    match = re.search(r"(19|20)\d{2}\s*$", text)
    return match.group(0) if match else ""


def _nfo_year(item: VodCategoryItem, summary: dict[str, object]) -> str:
    return (
        clean_text(getattr(item, "year", ""))
        or _extract_year(summary)
        or _extract_year(_summary_info(summary))
        or _extract_year(_summary_movie_info(summary))
        or _extract_year_from_title(getattr(item, "title", ""))
    )


def _clean_nfo_title(title: str, year: str) -> str:
    if not year:
        return title
    # Strip year from end of title if it matches
    return re.sub(rf"[\s\-()]*{re.escape(str(year))}\s*$", "", title).strip(" -_:|*[]()")


def _add_nfo_unique_ids(root, summary: dict[str, object]):
    for id_key in ["imdb", "tmdb", "tvdb"]:
        val = clean_text(_first_summary_value(summary, f"{id_key}_id"))
        if val:
            # Kodi/Jellyfin format: <uniqueid type="imdb" default="true">tt12345</uniqueid>
            _xml_text(root, "uniqueid", val, {"type": id_key, "default": "true" if id_key == "imdb" else "false"})


def _nfo_text_values(value: object) -> list[str]:
    if value in (None, "", [], {}):
        return []
    if isinstance(value, list):
        values = []
        for item in value:
            if isinstance(item, dict):
                name = clean_text(item.get("name") or item.get("title") or item.get("value"))
                if name:
                    values.append(name)
            else:
                text = clean_text(item)
                if text:
                    values.append(text)
        return values
    text = clean_text(value)
    if not text:
        return []
    if "," in text:
        split_values = [clean_text(part) for part in text.split(",")]
        return [item for item in split_values if item]
    return [text]


def _add_nfo_cast(root, value: object):
    if value in (None, "", [], {}):
        return
    entries = value if isinstance(value, list) else [value]
    for entry in entries:
        actor_name = ""
        actor_role = ""
        if isinstance(entry, dict):
            actor_name = clean_text(entry.get("name") or entry.get("actor") or entry.get("title"))
            actor_role = clean_text(entry.get("role") or entry.get("character"))
        else:
            actor_name = clean_text(entry)
        if not actor_name:
            continue
        actor_el = ET.SubElement(root, "actor")
        _xml_text(actor_el, "name", actor_name)
        _xml_text(actor_el, "role", actor_role)


def _generate_movie_nfo(item: VodCategoryItem) -> str:
    summary = _load_summary(item.summary_json)
    root = ET.Element("movie")
    year = _nfo_year(item, summary)
    title = _clean_nfo_title(item.title, year)
    sort_title = _clean_nfo_title(item.sort_title or item.title, year)

    _xml_text(root, "title", title)
    _xml_text(root, "sorttitle", sort_title)
    _xml_text(root, "year", year)

    _add_nfo_unique_ids(root, summary)

    plot = _first_summary_value(summary, "plot", "description", "overview")
    _xml_text(root, "plot", plot)
    _xml_text(root, "outline", plot)
    _xml_text(root, "tagline", _first_summary_value(summary, "tagline"))
    _xml_text(root, "rating", _first_summary_value(summary, "rating", "rating_5based"))
    _xml_text(
        root,
        "premiered",
        clean_text(item.release_date) or clean_text(_first_summary_value(summary, "releaseDate", "release_date")),
    )
    _xml_multi_text(root, "genre", _nfo_text_values(_first_summary_value(summary, "genre", "genres")))
    _xml_multi_text(root, "director", _nfo_text_values(_first_summary_value(summary, "director", "directors")))
    _add_nfo_cast(root, _first_summary_value(summary, "cast", "actors"))
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _generate_series_nfo(item: VodCategoryItem) -> str:
    summary = _load_summary(item.summary_json)
    root = ET.Element("tvshow")
    year = _nfo_year(item, summary)
    title = _clean_nfo_title(item.title, year)
    sort_title = _clean_nfo_title(item.sort_title or item.title, year)

    _xml_text(root, "title", title)
    _xml_text(root, "sorttitle", sort_title)
    _xml_text(root, "year", year)

    _add_nfo_unique_ids(root, summary)

    _xml_text(root, "plot", _first_summary_value(summary, "plot", "description", "overview"))
    _xml_text(root, "rating", _first_summary_value(summary, "rating", "rating_5based"))
    _xml_text(
        root,
        "premiered",
        clean_text(item.release_date) or clean_text(_first_summary_value(summary, "releaseDate", "release_date")),
    )
    _xml_multi_text(root, "genre", _nfo_text_values(_first_summary_value(summary, "genre", "genres")))
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _generate_episode_nfo(episode: VodCategoryEpisode) -> str:
    summary = _load_summary(episode.summary_json)
    info = _summary_info(summary)
    root = ET.Element("episodedetails")
    _xml_text(root, "title", episode.title)

    show_title = None
    if hasattr(episode, "category_item") and episode.category_item:
        show_title = _clean_nfo_title(episode.category_item.title, clean_text(episode.category_item.year))

    _xml_text(root, "showtitle", show_title)
    _xml_text(root, "season", episode.season_number)
    _xml_text(root, "episode", episode.episode_number)

    # Episode specific unique IDs
    _add_nfo_unique_ids(root, summary)

    _xml_text(root, "plot", summary.get("plot") or info.get("plot"))
    _xml_text(root, "premiered", summary.get("releaseDate") or info.get("releaseDate"))
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


async def _get_db_cached_metadata(playlist_id: int, action: str, upstream_item_id: str) -> dict | list | None:
    now_utc = datetime.now(timezone.utc)
    async with Session() as session:
        result = await session.execute(
            select(XcVodMetadataCache).where(
                XcVodMetadataCache.playlist_id == int(playlist_id),
                XcVodMetadataCache.action == str(action),
                XcVodMetadataCache.upstream_item_id == str(upstream_item_id),
            )
        )
        cache_row = result.scalars().first()
        if cache_row is None:
            return None
        expires_at = cache_row.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at <= now_utc:
            await session.delete(cache_row)
            await session.commit()
            return None
        cache_row.last_requested_at = as_naive_utc(now_utc)
        cache_row.expires_at = _metadata_expiry(now_utc)
        await session.commit()
        return _load_payload_json(cache_row.payload_json)


async def _set_db_cached_metadata(playlist_id: int, action: str, upstream_item_id: str, payload: object):
    now_utc = datetime.now(timezone.utc)
    async with Session() as session:
        async with session.begin():
            result = await session.execute(
                select(XcVodMetadataCache).where(
                    XcVodMetadataCache.playlist_id == int(playlist_id),
                    XcVodMetadataCache.action == str(action),
                    XcVodMetadataCache.upstream_item_id == str(upstream_item_id),
                )
            )
            cache_row = result.scalars().first()
            payload_json = _cache_payload_json(payload)
            if cache_row is None:
                session.add(
                    XcVodMetadataCache(
                        playlist_id=int(playlist_id),
                        action=str(action),
                        upstream_item_id=str(upstream_item_id),
                        payload_json=payload_json,
                        last_requested_at=as_naive_utc(now_utc),
                        expires_at=_metadata_expiry(now_utc),
                        created_at=as_naive_utc(now_utc),
                        updated_at=as_naive_utc(now_utc),
                    )
                )
            else:
                cache_row.payload_json = payload_json
                cache_row.last_requested_at = as_naive_utc(now_utc)
                cache_row.expires_at = _metadata_expiry(now_utc)


async def cleanup_stale_vod_metadata_cache() -> int:
    started_at = time.perf_counter()
    now_utc = datetime.now(timezone.utc)
    async with Session() as session:
        async with session.begin():
            result = await session.execute(
                delete(XcVodMetadataCache).where(XcVodMetadataCache.expires_at <= as_naive_utc(now_utc))
            )
            deleted_count = int(result.rowcount or 0)
    logger.debug(
        "XC VOD metadata cache cleanup deleted=%s elapsed=%.2fs",
        deleted_count,
        time.perf_counter() - started_at,
    )
    return deleted_count


async def _touch_vod_metadata_cache_for_group_items(item_ids: list[int], action: str) -> int:
    valid_item_ids = []
    for item_id in item_ids:
        try:
            item_id_value = int(item_id)
        except Exception:
            continue
        if item_id_value > 0:
            valid_item_ids.append(item_id_value)
    valid_item_ids = sorted(set(valid_item_ids))
    action_text = clean_text(action)
    if not valid_item_ids or not action_text:
        return 0

    now_utc = datetime.now(timezone.utc)
    touched_count = 0
    async with Session() as session:
        async with session.begin():
            source_rows = await session.execute(
                select(XcVodItem.playlist_id, XcVodItem.upstream_item_id)
                .join(VodCategoryItemSource, VodCategoryItemSource.source_item_id == XcVodItem.id)
                .where(VodCategoryItemSource.category_item_id.in_(valid_item_ids))
                .distinct()
            )
            upstream_ids_by_playlist: dict[int, list[str]] = {}
            for playlist_id, upstream_item_id in source_rows.all():
                try:
                    playlist_key = int(playlist_id)
                except Exception:
                    continue
                upstream_item_text = clean_text(upstream_item_id)
                if not upstream_item_text:
                    continue
                upstream_ids_by_playlist.setdefault(playlist_key, []).append(upstream_item_text)

            for playlist_id, upstream_item_ids in upstream_ids_by_playlist.items():
                unique_upstream_ids = sorted(set(upstream_item_ids))
                for batch in _batched(unique_upstream_ids):
                    result = await session.execute(
                        update(XcVodMetadataCache)
                        .where(
                            XcVodMetadataCache.playlist_id == int(playlist_id),
                            XcVodMetadataCache.action == action_text,
                            XcVodMetadataCache.upstream_item_id.in_(batch),
                        )
                        .values(
                            last_requested_at=as_naive_utc(now_utc),
                            expires_at=_metadata_expiry(now_utc),
                            updated_at=as_naive_utc(now_utc),
                        )
                    )
                    touched_count += int(result.rowcount or 0)
    return touched_count


async def _load_vod_category_for_export(category_id: int) -> VodCategory | None:
    async with Session() as session:
        result = await session.execute(select(VodCategory).where(VodCategory.id == int(category_id)))
        return result.scalars().first()


async def eligible_vod_export_users(category: VodCategory) -> list[User]:
    async with Session() as session:
        result = await session.execute(select(User).options(selectinload(User.roles)).where(User.is_active.is_(True)))
        users = result.scalars().all()
    return [
        user
        for user in users
        if bool(getattr(user, "vod_generate_strm_files", False))
        and clean_text(getattr(user, "streaming_key", ""))
        and user_can_access_vod_kind(user, category.content_type)
    ]


def _build_vod_strm_url(base_url: str, user: User, content_type: str, item_id: int, extension: str) -> str:
    suffix = clean_text(extension).lstrip(".").lower() or "mp4"
    route_name = "series" if content_type == VOD_KIND_SERIES else "movie"
    return f"{str(base_url).rstrip('/')}/{route_name}/{quote(user.username)}/{quote(user.streaming_key)}/{int(item_id)}.{suffix}"


def _vod_sync_subprocess_log_level(line: str, fallback_level: int) -> int:
    text = str(line or "")
    match = re.search(r":(DEBUG|INFO|WARNING|ERROR|CRITICAL):", text)
    if match:
        return getattr(logging, match.group(1), fallback_level)
    return fallback_level


async def sync_vod_library_subprocess(category_id: int | None = None) -> bool:
    project_root = Path(__file__).resolve().parents[1]
    args = [sys.executable, "-m", "backend.scripts.sync_vod_library"]
    if category_id:
        args.append(str(category_id))

    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(project_root),
    )

    async def _pipe(stream, level):
        while True:
            line = await stream.readline()
            if not line:
                break
            text = line.decode().rstrip()
            logger.log(_vod_sync_subprocess_log_level(text, level), "[vod-sync] %s", text)

    await asyncio.gather(
        _pipe(proc.stdout, logging.INFO),
        _pipe(proc.stderr, logging.ERROR),
    )
    rc = await proc.wait()
    if rc != 0:
        label = f"category {category_id}" if category_id else "full library"
        raise RuntimeError(f"VOD sync subprocess for {label} failed with code {rc}")


async def _refresh_series_items(item_ids, concurrency: int = VOD_SYNC_SERIES_REFRESH_CONCURRENCY):
    semaphore = asyncio.Semaphore(max(1, int(concurrency or 1)))
    successes = 0
    failures = 0

    async def _runner(item_id: int):
        nonlocal successes, failures
        async with semaphore:
            payload = await fetch_series_info_payload(int(item_id))
        if isinstance(payload, dict):
            successes += 1
            return True
        failures += 1
        return False

    results = await asyncio.gather(*[_runner(int(item_id)) for item_id in item_ids])
    total = len(results)
    failure_ratio = (float(failures) / float(total)) if total else 0.0
    if failures >= int(VOD_SYNC_BATCH_FAILURE_MIN_COUNT) and failure_ratio >= float(VOD_SYNC_BATCH_FAILURE_RATIO):
        raise RuntimeError(
            "Series metadata batch failed too often "
            f"(failures={failures}/{total}, ratio={failure_ratio:.2f}, concurrency={int(concurrency or 1)})"
        )
    return {
        "total": int(total),
        "successes": int(successes),
        "failures": int(failures),
        "failure_ratio": float(failure_ratio),
    }


async def sync_vod_category_strm_files(config, category_id: int) -> bool:
    started_at = time.perf_counter()
    category = await _load_vod_category_for_export(int(category_id))
    category_name = (
        getattr(category, "name", f"Category {category_id}") if category is not None else f"Category {category_id}"
    )

    # If category is deleted, disabled, or has no library outputs enabled, clean up.
    if (
        category is None
        or not bool(getattr(category, "enabled", False))
        or not _category_requires_vod_library_sync(category)
    ):
        await asyncio.to_thread(_sync_vod_category_export_files_sync, int(category_id), {})
        await asyncio.to_thread(remove_vod_http_library_index_entry_sync, int(category_id))
        if category is not None:
            await asyncio.to_thread(
                remove_vod_http_library_manifest_sync,
                getattr(category, "content_type", ""),
                int(category.id),
            )
        if category is None:
            return {"category_id": int(category_id), "removed_only": True}

        logger.debug(
            "VOD library sync removed outputs for category='%s' elapsed=%.2fs",
            category_name,
            time.perf_counter() - started_at,
        )
        return {"category_id": int(category_id), "users": 0, "files": 0, "removed_only": True}

    http_library_enabled = bool(category.expose_http_library)
    strm_enabled = bool(category.generate_strm_files)
    content_type = category.content_type
    kind_label = "Movie" if content_type == VOD_KIND_MOVIE else "Series"

    # Work out which users currently need exported library files for this category.
    users = await eligible_vod_export_users(category)
    if not users and not http_library_enabled:
        # Nothing needs to be written, but we still keep an empty per-category manifest on disk.
        category_manifest = _build_vod_http_category_manifest(category)
        category_manifest["exports"] = []
        category_manifest["generated_at"] = datetime.now(timezone.utc).isoformat()
        await asyncio.to_thread(_sync_vod_category_export_files_sync, int(category.id), {})
        await asyncio.to_thread(_write_vod_http_category_manifest_sync, category, category_manifest, False)
        return {"category_id": int(category.id), "users": 0, "files": 0}

    # Prepare for export
    base_url = category.strm_base_url or ""
    if strm_enabled and (not base_url or not urlparse(base_url).netloc):
        # TODO: Remove get_tvh_publish_base_url fallback at a later date. This is slated for removal (maybe)
        base_url = _vod_export_base_url_from_settings(config) or await get_tvh_publish_base_url(config)

    content_type_dir = _vod_category_type_dir_name(category.content_type)
    category_slug = _vod_export_slug(category.name, fallback=f"category-{int(category.id)}")

    # This export map is the source of truth for the STRM/NFO cleanup and write pass later on.
    export_states = {}
    for user in users:
        relative_dir = (
            Path(_vod_safe_name(user.username, fallback=f"user-{user.id}")) / content_type_dir / category_slug
        )
        if not strm_enabled:
            continue
        export_states[_vod_export_registry_key(int(user.id), int(category.id))] = {
            "relative_dir": relative_dir.as_posix(),
            "files": {},
            "dirs": {relative_dir.as_posix()},
        }

    async with Session() as session:
        # Load the curated items and any title-strip rules once up front for the whole category.
        item_result = await session.execute(
            select(VodCategoryItem)
            .where(VodCategoryItem.category_id == int(category.id))
            .order_by(VodCategoryItem.sort_title.asc(), VodCategoryItem.title.asc(), VodCategoryItem.id.asc())
        )
        category_items = item_result.scalars().all()

        # Load strip rules
        link_result = await session.execute(
            select(VodCategoryXcCategory).where(VodCategoryXcCategory.category_id == int(category.id))
        )
        strip_rules = {
            int(link.xc_category_id): (_group_category_strip_prefixes(link), _group_category_strip_suffixes(link))
            for link in link_result.scalars().all()
        }

    total_files = 0
    # Build one per-category manifest in memory and use it for both HTTP library data and export summaries.
    category_manifest = _build_vod_http_category_manifest(category)
    category_manifest["exports"] = []
    used_http_paths: set[str] = set()
    item_count = len(category_items)
    for batch_index, item_batch in enumerate(_batched(category_items, size=VOD_SYNC_ITEM_BATCH_SIZE), start=1):
        await asyncio.sleep(0)
        item_ids = [int(item.id) for item in item_batch]
        metadata_touch_count = 0

        async with Session() as session:
            source_result = await session.execute(
                select(VodCategoryItemSource.category_item_id, XcVodItem.category_id)
                .join(XcVodItem, XcVodItem.id == VodCategoryItemSource.source_item_id)
                .where(VodCategoryItemSource.category_item_id.in_(item_ids))
                .order_by(VodCategoryItemSource.category_item_id.asc(), VodCategoryItemSource.id.asc())
            )
            source_categories_by_item_id = {}
            for category_item_id, source_category_id in source_result.all():
                category_item_key = int(category_item_id)
                source_category_key = int(source_category_id or 0)
                source_categories_by_item_id.setdefault(category_item_key, [])
                if source_category_key not in source_categories_by_item_id[category_item_key]:
                    source_categories_by_item_id[category_item_key].append(source_category_key)

        if content_type == VOD_KIND_SERIES and item_ids:
            # Series exports need fresh episode data before we can build either export output.
            refresh_stats = await _refresh_series_items(item_ids)
            async with Session() as session:
                episode_result = await session.execute(
                    select(VodCategoryEpisode)
                    .join(VodCategoryEpisodeSource, VodCategoryEpisodeSource.episode_id == VodCategoryEpisode.id)
                    .where(VodCategoryEpisode.category_item_id.in_(item_ids))
                    .order_by(
                        VodCategoryEpisode.category_item_id.asc(),
                        VodCategoryEpisode.season_number.asc(),
                        VodCategoryEpisode.episode_number.asc(),
                        VodCategoryEpisode.id.asc(),
                    )
                )
                episodes_by_item_id = {}
                for episode in episode_result.scalars().unique().all():
                    episodes_by_item_id.setdefault(int(episode.category_item_id), []).append(episode)
        else:
            refresh_stats = None
            episodes_by_item_id = {}

        if item_ids:
            metadata_touch_count = await _touch_vod_metadata_cache_for_group_items(
                item_ids,
                "get_vod_info" if content_type == VOD_KIND_MOVIE else "get_series_info",
            )

        # Build the export plan and HTTP nodes for this batch without touching disk yet.
        for item in item_batch:
            source_category_ids = source_categories_by_item_id.get(int(item.id), [])
            strip_prefixes, strip_suffixes = _merge_strip_rules_for_source_categories(source_category_ids, strip_rules)

            if content_type == VOD_KIND_MOVIE:
                display_name = _vod_movie_display_name(item, prefixes=strip_prefixes, suffixes=strip_suffixes)
                extension = _resolve_group_output_extension(category.profile_id, item.container_extension or "")
                movie_nfo = _generate_movie_nfo(item)
                http_movie_folder = _ensure_unique_relative_path(Path(display_name), used_http_paths, str(int(item.id)))
                logger.debug(
                    "Exporting VOD movie .strm category='%s' item_id=%s title='%s' users=%s extension=%s",
                    category_name,
                    int(item.id),
                    display_name,
                    len(users),
                    extension,
                )

                if strm_enabled:
                    for user in users:
                        relative_dir = (
                            Path(_vod_safe_name(user.username, fallback=f"user-{user.id}"))
                            / content_type_dir
                            / category_slug
                        )
                        movie_folder = relative_dir / display_name
                        strm_path = movie_folder / f"{display_name}.strm"
                        nfo_path = movie_folder / f"{display_name}.nfo"
                        export_state = export_states[_vod_export_registry_key(int(user.id), int(category.id))]
                        export_state["dirs"].update({movie_folder.as_posix()})
                        export_state["files"][strm_path.as_posix()] = (
                            f"{_build_vod_strm_url(base_url, user, VOD_KIND_MOVIE, item.id, extension)}\n"
                        )
                        if movie_nfo:
                            export_state["files"][nfo_path.as_posix()] = movie_nfo
                        total_files += 1

                if http_library_enabled:
                    _vod_http_manifest_ensure_dir(category_manifest, http_movie_folder)
                    _vod_http_manifest_add_file(
                        category_manifest,
                        http_movie_folder / f"{display_name}.{extension}",
                        "movie_file",
                        {
                            "item_id": int(item.id),
                            "extension": extension,
                            "content_type": VOD_KIND_MOVIE,
                        },
                    )
                continue

            episodes = episodes_by_item_id.get(int(item.id), [])
            skip_http_series_export = http_library_enabled and not episodes
            if skip_http_series_export:
                logger.debug(
                    "Skipping HTTP library export for series item without episode cache category='%s' item_id=%s title='%s'",
                    category_name,
                    int(item.id),
                    item.title,
                )
            series_display_name = _vod_series_display_name(item, prefixes=strip_prefixes, suffixes=strip_suffixes)
            series_nfo = _generate_series_nfo(item)
            http_series_folder = _ensure_unique_relative_path(
                Path(series_display_name), used_http_paths, str(int(item.id))
            )
            logger.debug(
                "Exporting VOD series .strm category='%s' item_id=%s title='%s' users=%s episodes=%s",
                category_name,
                int(item.id),
                series_display_name,
                len(users),
                len(episodes),
            )

            if strm_enabled:
                for user in users:
                    relative_dir = (
                        Path(_vod_safe_name(user.username, fallback=f"user-{user.id}"))
                        / content_type_dir
                        / category_slug
                    )
                    series_folder = relative_dir / series_display_name
                    export_state = export_states[_vod_export_registry_key(int(user.id), int(category.id))]
                    export_state["dirs"].update({series_folder.as_posix()})
                    export_state["files"][(series_folder / "tvshow.nfo").as_posix()] = series_nfo

                    for episode in episodes:
                        file_stem = _vod_episode_display_name(
                            series_display_name, episode, prefixes=strip_prefixes, suffixes=strip_suffixes
                        )
                        strm_name = f"{file_stem}.strm"
                        nfo_name = f"{file_stem}.nfo"
                        season_dir = _vod_season_dir_name(episode)
                        extension = _resolve_group_output_extension(
                            category.profile_id, episode.container_extension or ""
                        )

                        episode.category_item = item
                        episode_nfo = _generate_episode_nfo(episode)
                        file_path = series_folder / season_dir / strm_name
                        nfo_path = series_folder / season_dir / nfo_name
                        export_state["dirs"].add((series_folder / season_dir).as_posix())
                        export_state["files"][file_path.as_posix()] = (
                            f"{_build_vod_strm_url(base_url, user, VOD_KIND_SERIES, episode.id, extension)}\n"
                        )
                        export_state["files"][nfo_path.as_posix()] = episode_nfo
                        total_files += 1

            if http_library_enabled and not skip_http_series_export:
                _vod_http_manifest_ensure_dir(category_manifest, http_series_folder)
                episode_used_paths: set[str] = set()
                for episode in episodes:
                    file_stem = _vod_episode_display_name(
                        series_display_name, episode, prefixes=strip_prefixes, suffixes=strip_suffixes
                    )
                    season_dir = Path(_vod_season_dir_name(episode))
                    extension = _resolve_group_output_extension(category.profile_id, episode.container_extension or "")

                    media_path = _ensure_unique_relative_path(
                        http_series_folder / season_dir / f"{file_stem}.{extension}",
                        episode_used_paths,
                        str(int(episode.id)),
                    )
                    _vod_http_manifest_add_file(
                        category_manifest,
                        media_path,
                        "episode_file",
                        {
                            "episode_id": int(episode.id),
                            "extension": extension,
                            "content_type": VOD_KIND_SERIES,
                        },
                    )

        logger.info(
            "VOD library sync batch complete category='%s' batch=%s items=%s/%s total_files=%s refresh_successes=%s "
            "refresh_failures=%s metadata_touched=%s http_library=%s elapsed=%.2fs",
            category_name,
            batch_index,
            min(batch_index * int(VOD_SYNC_ITEM_BATCH_SIZE), item_count),
            item_count,
            total_files,
            refresh_stats["successes"] if refresh_stats else 0,
            refresh_stats["failures"] if refresh_stats else 0,
            metadata_touch_count,
            bool(http_library_enabled),
            time.perf_counter() - started_at,
        )

    category_manifest["exports"] = [
        {
            "key": key,
            "relative_dir": state["relative_dir"],
            "files": sorted(state["files"].keys()),
            "dirs": sorted(state["dirs"]),
        }
        for key, state in sorted(export_states.items())
    ]
    category_manifest["generated_at"] = datetime.now(timezone.utc).isoformat()

    # Apply the STRM/NFO export plan first, then persist the category manifest that describes the result.
    await asyncio.to_thread(
        _sync_vod_category_export_files_sync,
        int(category.id),
        export_states if strm_enabled else {},
    )
    await asyncio.to_thread(
        _write_vod_http_category_manifest_sync, category, category_manifest, bool(http_library_enabled)
    )

    logger.info(
        "VOD library sync completed for %s category='%s' users=%s items=%s total_files=%s http_library=%s elapsed=%.2fs",
        kind_label,
        category_name,
        len(users),
        len(category_items),
        total_files,
        bool(http_library_enabled),
        time.perf_counter() - started_at,
    )
    return {"category_id": int(category.id), "files": total_files}


async def queue_vod_category_strm_sync(category_id: int) -> bool:
    from backend.api.tasks import TaskQueueBroker

    category = await _load_vod_category_for_export(int(category_id))
    if category is None:
        return

    category_name = getattr(category, "name", f"Category {category_id}")
    content_type = category.content_type
    kind_label = "Movie" if content_type == VOD_KIND_MOVIE else "Series"

    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task(
        {
            "name": f"Sync VOD library outputs for {kind_label} category '{category_name}'",
            "function": sync_vod_library_subprocess,
            "args": [int(category_id)],
            "execution_mode": "concurrent",
            "task_key": "vod-sync",
        },
        priority=27,
    )


async def queue_all_vod_category_strm_syncs(config=None) -> int:
    from backend.api.tasks import TaskQueueBroker

    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task(
        {
            "name": "Sync all VOD library outputs (Full Library Reconcile)",
            "function": sync_vod_library_subprocess,
            "args": [],
            "execution_mode": "concurrent",
            "task_key": "vod-sync",
        },
        priority=28,
    )


async def _delete_ids_in_batches(session, model, ids):
    for batch in _batched(ids):
        await session.execute(delete(model).where(model.id.in_(batch)))


async def _get_primary_xc_account(playlist_id: int) -> XcAccount | None:
    async with Session() as session:
        result = await session.execute(
            select(XcAccount)
            .where(XcAccount.playlist_id == playlist_id, XcAccount.enabled.is_(True))
            .order_by(XcAccount.id.asc())
        )
        return result.scalars().first()


async def _xc_request(session, host_url: str, params: dict[str, str], retries: int = 3):
    url = f"{str(host_url).rstrip('/')}/player_api.php"
    last_error = None
    for attempt in range(1, int(retries) + 1):
        try:
            async with session.get(url, params=params, timeout=30) as response:
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            last_error = exc
            if attempt < int(retries):
                await asyncio.sleep(attempt)
                continue
            raise
    if last_error:
        raise last_error
    return {}


def _resolve_source_request_headers(settings, playlist: Playlist) -> dict[str, str]:
    headers = {}
    user_agent = clean_text(getattr(playlist, "user_agent", ""))
    if user_agent:
        headers["User-Agent"] = user_agent
    try:
        parsed = json.loads(getattr(playlist, "hls_proxy_headers", None) or "{}")
    except Exception:
        parsed = {}
    if isinstance(parsed, dict):
        for key, value in parsed.items():
            key_text = clean_text(key)
            value_text = clean_text(value)
            if key_text and value_text:
                headers[key_text] = value_text
    return headers


async def _choose_working_xc_host(playlist: Playlist) -> tuple[str | None, XcAccount | None]:
    account = await _get_primary_xc_account(int(playlist.id))
    if account is None:
        return None, None
    hosts = parse_xc_hosts(getattr(playlist, "url", ""))
    if not hosts:
        return None, None
    headers = _resolve_source_request_headers({}, playlist)
    async with aiohttp.ClientSession(headers=headers) as http_session:
        for host in hosts:
            try:
                auth_info = await _xc_request(
                    http_session,
                    host,
                    {"username": account.username, "password": account.password},
                )
                if isinstance(auth_info, dict) and auth_info.get("user_info"):
                    return str(host).rstrip("/"), account
            except Exception:
                continue
    return None, account


async def _choose_working_xc_host_for_account(
    playlist: Playlist, account: XcAccount
) -> tuple[str | None, XcAccount | None]:
    if playlist is None or account is None:
        return None, None
    hosts = parse_xc_hosts(getattr(playlist, "url", ""))
    if not hosts:
        return None, account
    headers = _resolve_source_request_headers({}, playlist)
    async with aiohttp.ClientSession(headers=headers) as http_session:
        for host in hosts:
            try:
                auth_info = await _xc_request(
                    http_session,
                    host,
                    {"username": account.username, "password": account.password},
                )
                if isinstance(auth_info, dict) and auth_info.get("user_info"):
                    return str(host).rstrip("/"), account
            except Exception:
                continue
    return None, account


async def _get_enabled_xc_accounts(session, playlist_id: int) -> list[XcAccount]:
    result = await session.execute(
        select(XcAccount)
        .where(XcAccount.playlist_id == int(playlist_id), XcAccount.enabled.is_(True))
        .order_by(XcAccount.id.asc())
    )
    return result.scalars().all()


async def _select_account_for_playlist(playlist: Playlist) -> tuple[str | None, XcAccount | None]:
    if playlist is None:
        return None, None
    async with Session() as session:
        accounts = await _get_enabled_xc_accounts(session, int(playlist.id))
    if not accounts:
        return None, None
    snapshot = await get_stream_activity_snapshot()
    active_counts = {}
    for item in snapshot or []:
        account_id = item.get("xc_account_id")
        if account_id is None:
            continue
        try:
            account_id = int(account_id)
        except Exception:
            continue
        active_counts[account_id] = active_counts.get(account_id, 0) + 1

    preferred = []
    fallback = []
    for account in accounts:
        limit_value = int(getattr(account, "connection_limit", 0) or 0)
        active_value = int(active_counts.get(int(account.id), 0))
        bucket = preferred if limit_value <= 0 or active_value < limit_value else fallback
        bucket.append(account)
    ordered_accounts = preferred + fallback
    for account in ordered_accounts:
        host_url, resolved_account = await _choose_working_xc_host_for_account(playlist, account)
        if host_url and resolved_account is not None:
            return host_url, resolved_account
    return None, None


def _build_upstream_movie_url(host_url: str, account: XcAccount, upstream_item_id: str, extension: str) -> str:
    suffix = clean_text(extension).lstrip(".").lower() or "mp4"
    return f"{str(host_url).rstrip('/')}/movie/{quote(account.username)}/{quote(account.password)}/{upstream_item_id}.{suffix}"


def _build_upstream_series_url(host_url: str, account: XcAccount, upstream_episode_id: str, extension: str) -> str:
    suffix = clean_text(extension).lstrip(".").lower() or "mp4"
    return f"{str(host_url).rstrip('/')}/series/{quote(account.username)}/{quote(account.password)}/{upstream_episode_id}.{suffix}"


async def sync_xc_vod_catalogue(playlist: Playlist):
    if not playlist or str(getattr(playlist, "account_type", "")).upper() != "XC":
        return
    overall_started_at = time.perf_counter()
    host_url, account = await _choose_working_xc_host(playlist)
    if not host_url or account is None:
        logger.warning("Skipping XC VOD sync; no working XC host playlist=%s", getattr(playlist, "id", None))
        return

    headers = _resolve_source_request_headers({}, playlist)
    fetch_started_at = time.perf_counter()
    async with aiohttp.ClientSession(headers=headers) as http_session:
        movie_categories = await _xc_request(
            http_session,
            host_url,
            {"username": account.username, "password": account.password, "action": "get_vod_categories"},
        )
        movie_items = await _xc_request(
            http_session,
            host_url,
            {"username": account.username, "password": account.password, "action": "get_vod_streams"},
        )
        series_categories = await _xc_request(
            http_session,
            host_url,
            {"username": account.username, "password": account.password, "action": "get_series_categories"},
        )
        series_items = await _xc_request(
            http_session,
            host_url,
            {"username": account.username, "password": account.password, "action": "get_series"},
        )
    logger.debug(
        "XC VOD fetch completed for playlist #%s in %.2fs (movie_categories=%s, movie_items=%s, series_categories=%s, series_items=%s)",
        getattr(playlist, "id", None),
        time.perf_counter() - fetch_started_at,
        len(movie_categories) if isinstance(movie_categories, list) else 0,
        len(movie_items) if isinstance(movie_items, list) else 0,
        len(series_categories) if isinstance(series_categories, list) else 0,
        len(series_items) if isinstance(series_items, list) else 0,
    )

    movie_upsert_started_at = time.perf_counter()
    await _upsert_vod_type(
        int(playlist.id),
        VOD_KIND_MOVIE,
        movie_categories if isinstance(movie_categories, list) else [],
        movie_items if isinstance(movie_items, list) else [],
    )
    logger.debug(
        "XC VOD movie upsert completed for playlist #%s in %.2fs",
        getattr(playlist, "id", None),
        time.perf_counter() - movie_upsert_started_at,
    )

    series_upsert_started_at = time.perf_counter()
    await _upsert_vod_type(
        int(playlist.id),
        VOD_KIND_SERIES,
        series_categories if isinstance(series_categories, list) else [],
        series_items if isinstance(series_items, list) else [],
    )
    logger.debug(
        "XC VOD series upsert completed for playlist #%s in %.2fs",
        getattr(playlist, "id", None),
        time.perf_counter() - series_upsert_started_at,
    )

    await rebuild_vod_group_caches_for_playlist(int(playlist.id))
    logger.debug(
        "XC VOD sync completed for playlist #%s in %.2fs",
        getattr(playlist, "id", None),
        time.perf_counter() - overall_started_at,
    )


async def _upsert_vod_type(playlist_id: int, kind: str, categories: list[dict], items: list[dict]):
    kind = require_vod_content_type(kind)
    async with Session() as session:
        async with session.begin():
            existing_categories_result = await session.execute(
                select(XcVodCategory).where(
                    XcVodCategory.playlist_id == int(playlist_id),
                    XcVodCategory.category_type == kind,
                )
            )
            existing_categories = {
                str(item.upstream_category_id): item for item in existing_categories_result.scalars().all()
            }
            seen_category_ids = set()
            category_by_upstream = {}
            for payload in categories or []:
                upstream_category_id = clean_text(payload.get("category_id"))
                if not upstream_category_id:
                    continue
                seen_category_ids.add(upstream_category_id)
                row = existing_categories.get(upstream_category_id)
                if row is None:
                    row = XcVodCategory(
                        playlist_id=int(playlist_id),
                        category_type=kind,
                        upstream_category_id=upstream_category_id,
                        name=_truncated_vod_text(payload.get("category_name")) or upstream_category_id,
                        parent_id=clean_text(payload.get("parent_id")),
                    )
                    session.add(row)
                    await session.flush()
                else:
                    row.name = _truncated_vod_text(payload.get("category_name")) or row.name
                    row.parent_id = clean_text(payload.get("parent_id"))
                category_by_upstream[upstream_category_id] = row
            stale_categories = [
                item.id for upstream_id, item in existing_categories.items() if upstream_id not in seen_category_ids
            ]
            if stale_categories:
                await _delete_ids_in_batches(session, XcVodCategory, stale_categories)

            existing_items_result = await session.execute(
                select(XcVodItem).where(
                    XcVodItem.playlist_id == int(playlist_id),
                    XcVodItem.item_type == kind,
                )
            )
            existing_items = {str(item.upstream_item_id): item for item in existing_items_result.scalars().all()}
            seen_item_ids = set()

            for payload in items or []:
                upstream_item_id = clean_text(payload.get("stream_id") or payload.get("series_id"))
                if not upstream_item_id or upstream_item_id in seen_item_ids:
                    continue
                seen_item_ids.add(upstream_item_id)
                row = existing_items.get(upstream_item_id)
                if row is None:
                    title = _truncated_vod_text(payload.get("name") or payload.get("title")) or upstream_item_id
                    row = XcVodItem(
                        playlist_id=int(playlist_id),
                        category_id=None,
                        item_type=kind,
                        upstream_item_id=upstream_item_id,
                        title=title,
                        sort_title=title,
                        release_date=clean_text(payload.get("releaseDate") or payload.get("release_date")),
                        year=_extract_year(payload),
                        rating=clean_text(payload.get("rating")),
                        poster_url=_poster_url(payload, kind),
                        container_extension=_container_extension(payload),
                        direct_source=clean_text(payload.get("direct_source")),
                        added=clean_text(payload.get("added")),
                        summary_json=_summary_json(payload),
                    )
                    session.add(row)
                else:
                    row.category_id = None
                    row.title = _truncated_vod_text(payload.get("name") or payload.get("title")) or row.title
                    row.sort_title = row.title
                    row.release_date = clean_text(payload.get("releaseDate") or payload.get("release_date"))
                    row.year = _extract_year(payload)
                    row.rating = clean_text(payload.get("rating"))
                    row.poster_url = _poster_url(payload, kind)
                    row.container_extension = _container_extension(payload)
                    row.direct_source = clean_text(payload.get("direct_source"))
                    row.added = clean_text(payload.get("added"))
                    row.summary_json = _summary_json(payload)

                category = category_by_upstream.get(clean_text(payload.get("category_id")))
                row.category_id = category.id if category is not None else None

            stale_item_db_ids = [
                item.id for upstream_id, item in existing_items.items() if upstream_id not in seen_item_ids
            ]
            stale_upstream_item_ids = [
                str(upstream_id) for upstream_id in existing_items.keys() if upstream_id not in seen_item_ids
            ]
            if stale_item_db_ids:
                await _delete_ids_in_batches(session, XcVodItem, stale_item_db_ids)
            for batch in _batched(stale_upstream_item_ids):
                await session.execute(
                    delete(XcVodMetadataCache).where(
                        XcVodMetadataCache.playlist_id == int(playlist_id),
                        XcVodMetadataCache.upstream_item_id.in_(batch),
                    )
                )


async def rebuild_vod_group_caches_for_playlist(playlist_id: int) -> int:
    async with Session() as session:
        result = await session.execute(
            select(VodCategory.id)
            .join(VodCategoryXcCategory, VodCategoryXcCategory.category_id == VodCategory.id)
            .join(XcVodCategory, XcVodCategory.id == VodCategoryXcCategory.xc_category_id)
            .where(XcVodCategory.playlist_id == int(playlist_id))
        )
        group_ids = sorted({int(row[0]) for row in result.all() if row and row[0] is not None})
    for group_id in group_ids:
        await queue_rebuild_vod_group_cache(group_id)


async def queue_rebuild_vod_group_cache(group_id: int) -> bool:
    from backend.api.tasks import TaskQueueBroker

    async with Session() as session:
        group = await session.get(VodCategory, int(group_id))
        if group is None:
            return
        category_name = group.name
        content_type = group.content_type
        kind_label = "Movie" if content_type == VOD_KIND_MOVIE else "Series"

    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task(
        {
            "name": f"Rebuild cache for {kind_label} category '{category_name}'",
            "function": sync_vod_library_subprocess,
            "args": [int(group_id)],
            "execution_mode": "concurrent",
            "task_key": "vod-sync",
        },
        priority=25,
    )


async def rebuild_vod_group_cache(group_id: int, queue_sync: bool = True) -> bool:
    async with Session() as session:
        async with session.begin():
            result = await session.execute(
                select(VodCategory)
                .options(selectinload(VodCategory.xc_category_links))
                .where(VodCategory.id == int(group_id))
            )
            group = result.scalars().first()
            if group is None:
                return False

            ordered_links = _ordered_vod_category_links(group.xc_category_links)
            category_ids = _ordered_vod_category_ids(ordered_links)
            if not category_ids:
                return True
            strip_rules_by_category_id = {
                int(link.xc_category_id): (
                    _group_category_strip_prefixes(link),
                    _group_category_strip_suffixes(link),
                )
                for link in ordered_links
            }
            category_priority_by_id = _vod_category_priority_map(ordered_links)

            source_result = await session.execute(
                select(XcVodItem).where(
                    XcVodItem.category_id.in_(category_ids),
                    XcVodItem.item_type == group.content_type,
                )
            )
            source_items = sorted(
                source_result.scalars().all(),
                key=lambda item: (
                    -int(category_priority_by_id.get(int(getattr(item, "category_id", 0) or 0), 0)),
                    clean_text(getattr(item, "title", "")).lower(),
                    int(getattr(item, "id", 0) or 0),
                ),
            )
            buckets = {}
            for source_item in source_items:
                strip_prefixes, strip_suffixes = strip_rules_by_category_id.get(
                    int(source_item.category_id or 0), ([], [])
                )
                dedupe_key = _dedupe_key_for_item(source_item, prefixes=strip_prefixes, suffixes=strip_suffixes)
                bucket = buckets.setdefault(dedupe_key, {"representative": source_item, "sources": []})
                bucket["sources"].append(source_item)

            existing_result = await session.execute(
                select(VodCategoryItem).where(VodCategoryItem.category_id == int(group_id))
            )
            existing_items_by_dedupe = {
                clean_text(item.dedupe_key): item
                for item in existing_result.scalars().all()
                if clean_text(item.dedupe_key)
            }

            seen_dedupe_keys = set()
            active_item_ids = []
            category_item_sources_to_add = []

            for dedupe_key, bucket in buckets.items():
                representative = bucket["representative"]
                strip_prefixes, strip_suffixes = strip_rules_by_category_id.get(
                    int(getattr(representative, "category_id", 0) or 0),
                    ([], []),
                )
                display_title = _export_title_from_source_title(
                    representative.title,
                    prefixes=strip_prefixes,
                    suffixes=strip_suffixes,
                )
                group_item = existing_items_by_dedupe.get(dedupe_key)
                if group_item is None:
                    group_item = VodCategoryItem(
                        category_id=int(group.id),
                        item_type=group.content_type,
                        dedupe_key=dedupe_key,
                    )
                    session.add(group_item)
                group_item.title = _truncated_vod_text(display_title or representative.title)
                group_item.sort_title = _truncated_vod_text(display_title or representative.sort_title)
                group_item.release_date = representative.release_date
                group_item.year = representative.year
                group_item.rating = representative.rating
                group_item.poster_url = representative.poster_url
                group_item.container_extension = representative.container_extension
                group_item.summary_json = representative.summary_json
                seen_dedupe_keys.add(dedupe_key)

            await session.flush()

            if seen_dedupe_keys:
                active_items_result = await session.execute(
                    select(VodCategoryItem).where(
                        VodCategoryItem.category_id == int(group_id),
                        VodCategoryItem.dedupe_key.in_(list(seen_dedupe_keys)),
                    )
                )
                active_items_by_dedupe = {
                    clean_text(item.dedupe_key): item
                    for item in active_items_result.scalars().all()
                    if clean_text(item.dedupe_key)
                }
            else:
                active_items_by_dedupe = {}
            active_item_ids = [int(item.id) for item in active_items_by_dedupe.values()]

            if active_item_ids:
                await session.execute(
                    delete(VodCategoryItemSource).where(VodCategoryItemSource.category_item_id.in_(active_item_ids))
                )

            for dedupe_key, bucket in buckets.items():
                group_item = active_items_by_dedupe.get(dedupe_key)
                if group_item is None:
                    continue
                for source_item in bucket["sources"]:
                    category_item_sources_to_add.append(
                        VodCategoryItemSource(
                            category_item_id=int(group_item.id),
                            source_item_id=int(source_item.id),
                        )
                    )

            if category_item_sources_to_add:
                session.add_all(category_item_sources_to_add)

            stale_item_ids = [
                int(item.id)
                for dedupe_key, item in existing_items_by_dedupe.items()
                if dedupe_key not in seen_dedupe_keys
            ]
            if stale_item_ids:
                await session.execute(delete(VodCategoryItem).where(VodCategoryItem.id.in_(stale_item_ids)))
        await session.commit()

    if queue_sync:
        await queue_vod_category_strm_sync(int(group_id))
    return True


async def get_vod_page_state() -> dict[str, bool]:
    async with Session() as session:
        playlist_result = await session.execute(select(func.count(Playlist.id)).where(Playlist.account_type == "XC"))
        category_result = await session.execute(select(func.count(XcVodCategory.id)))
        playlist_count = int(playlist_result.scalar() or 0)
        category_count = int(category_result.scalar() or 0)
        return {
            "has_xc_sources": playlist_count > 0,
            "has_vod_content": category_count > 0,
            "show_page": playlist_count > 0 and category_count > 0,
        }


async def get_library_page_state(user: User | None) -> dict[str, object]:
    movie_categories = await list_curated_library_categories(user, VOD_KIND_MOVIE)
    series_categories = await list_curated_library_categories(user, VOD_KIND_SERIES)
    movie_count = len(movie_categories)
    series_count = len(series_categories)
    return {
        "movie_category_count": int(movie_count),
        "series_category_count": int(series_count),
        "has_curated_content": bool(movie_count or series_count),
        "show_page": bool(movie_count or series_count),
    }


async def list_upstream_vod_categories(
    content_type: str, source_playlist_id: int | None = None
) -> list[dict[str, object]]:
    content_type = require_vod_content_type(content_type)
    async with Session() as session:
        stmt = (
            select(XcVodCategory, Playlist.name)
            .join(Playlist, Playlist.id == XcVodCategory.playlist_id)
            .where(XcVodCategory.category_type == content_type)
            .order_by(Playlist.name.asc(), XcVodCategory.name.asc())
        )
        if source_playlist_id:
            stmt = stmt.where(XcVodCategory.playlist_id == int(source_playlist_id))
        result = await session.execute(stmt)
        rows = []
        for category, playlist_name in result.all():
            count_result = await session.execute(
                select(func.count(XcVodItem.id)).where(XcVodItem.category_id == category.id)
            )
            rows.append(
                {
                    "id": category.id,
                    "playlist_id": category.playlist_id,
                    "playlist_name": playlist_name,
                    "upstream_category_id": category.upstream_category_id,
                    "name": category.name,
                    "item_count": int(count_result.scalar() or 0),
                }
            )
        return rows


async def list_vod_groups(content_type: str) -> list[dict[str, object]]:
    content_type = require_vod_content_type(content_type)
    async with Session() as session:
        stmt = (
            select(VodCategory)
            .options(
                selectinload(VodCategory.xc_category_links)
                .selectinload(VodCategoryXcCategory.xc_category)
                .selectinload(XcVodCategory.playlist)
            )
            .where(VodCategory.content_type == content_type)
            .order_by(VodCategory.sort_order.asc(), VodCategory.name.asc(), VodCategory.id.asc())
        )
        result = await session.execute(stmt)
        groups = []
        for group in result.scalars().all():
            count_result = await session.execute(
                select(func.count(VodCategoryItem.id)).where(VodCategoryItem.category_id == group.id)
            )
            groups.append(
                {
                    "id": group.id,
                    "content_type": group.content_type,
                    "name": group.name,
                    "sort_order": group.sort_order,
                    "enabled": bool(group.enabled),
                    "profile_id": group.profile_id,
                    "generate_strm_files": bool(group.generate_strm_files),
                    "expose_http_library": bool(group.expose_http_library),
                    "strm_base_url": group.strm_base_url or "",
                    "item_count": int(count_result.scalar() or 0),
                    "categories": [
                        {
                            "id": link.xc_category.id,
                            "priority": _vod_link_priority_value(link),
                            "playlist_id": link.xc_category.playlist_id,
                            "playlist_name": getattr(link.xc_category.playlist, "name", ""),
                            "name": link.xc_category.name,
                            "upstream_category_id": link.xc_category.upstream_category_id,
                            "strip_title_prefixes": _group_category_strip_prefixes(link),
                            "strip_title_suffixes": _group_category_strip_suffixes(link),
                        }
                        for link in _ordered_vod_category_links(group.xc_category_links)
                        if link.xc_category is not None
                    ],
                }
            )
        return groups


def _vod_item_summary_fields(summary_json: str | None) -> dict[str, object]:
    summary = _load_summary(summary_json)
    info = _summary_info(summary)
    plot = clean_text(
        info.get("plot")
        or summary.get("plot")
        or info.get("description")
        or summary.get("description")
        or info.get("overview")
        or summary.get("overview")
    )
    return {
        "plot": plot,
        "added": clean_text(summary.get("added") or info.get("added")),
        "genre": _nfo_text_values(info.get("genre") or summary.get("genre") or info.get("genres") or summary.get("genres")),
    }


def _flatten_series_episode_payload(payload: dict[str, object]) -> list[dict[str, object]]:
    episodes_by_season = payload.get("episodes")
    if not isinstance(episodes_by_season, dict):
        return []

    flattened = []
    for season_key, entries in episodes_by_season.items():
        season_number = int(season_key) if str(season_key).isdigit() else None
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            episode_number = int(entry.get("episode_num") or 0) or None
            flattened.append(
                {
                    "id": int(entry.get("id") or 0) if str(entry.get("id") or "").isdigit() else None,
                    "title": clean_text(entry.get("title")),
                    "season_number": season_number,
                    "episode_number": episode_number,
                    "container_extension": clean_text(entry.get("container_extension")).lstrip(".").lower() or "mp4",
                    "plot": clean_text(
                        entry.get("plot")
                        or ((entry.get("info") or {}) if isinstance(entry.get("info"), dict) else {}).get("plot")
                    ),
                }
            )
    flattened.sort(
        key=lambda row: (
            int(row.get("season_number") or 0),
            int(row.get("episode_number") or 0),
            clean_text(row.get("title")).casefold(),
        )
    )
    return flattened


async def list_upstream_vod_items(
    content_type: str,
    source_playlist_id: int | None = None,
    upstream_category_id: int | None = None,
    search_query: str | None = None,
    offset: int = 0,
    limit: int = 50,
) -> dict[str, object]:
    content_type = require_vod_content_type(content_type)
    resolved_offset = max(0, int(offset or 0))
    resolved_limit = max(1, min(int(limit or 50), 200))
    query_text = clean_text(search_query).casefold()

    async with Session() as session:
        stmt = (
            select(XcVodItem, XcVodCategory, Playlist)
            .join(XcVodCategory, XcVodCategory.id == XcVodItem.category_id, isouter=True)
            .join(Playlist, Playlist.id == XcVodItem.playlist_id)
            .where(
                XcVodItem.item_type == content_type,
                Playlist.enabled.is_(True),
            )
        )
        if source_playlist_id:
            stmt = stmt.where(XcVodItem.playlist_id == int(source_playlist_id))
        if upstream_category_id:
            stmt = stmt.where(XcVodItem.category_id == int(upstream_category_id))
        if query_text:
            pattern = f"%{query_text}%"
            stmt = stmt.where(
                func.lower(XcVodItem.title).like(pattern)
                | func.lower(func.coalesce(XcVodCategory.name, "")).like(pattern)
                | func.lower(Playlist.name).like(pattern)
                | func.lower(func.coalesce(XcVodItem.year, "")).like(pattern)
            )
        stmt = stmt.order_by(
            Playlist.name.asc(),
            XcVodCategory.name.asc(),
            XcVodItem.sort_title.asc(),
            XcVodItem.title.asc(),
            XcVodItem.id.asc(),
        )
        result = await session.execute(stmt.offset(resolved_offset).limit(resolved_limit + 1))
        rows = result.all()

        items = []
        for source_item, source_category, playlist in rows[:resolved_limit]:
            summary_fields = _vod_item_summary_fields(source_item.summary_json)
            items.append(
                {
                    "id": int(source_item.id),
                    "content_type": content_type,
                    "title": source_item.title,
                    "year": clean_text(source_item.year),
                    "rating": clean_text(source_item.rating),
                    "poster_url": clean_text(source_item.poster_url),
                    "release_date": clean_text(source_item.release_date),
                    "container_extension": clean_text(source_item.container_extension).lstrip(".").lower() or "mp4",
                    "playlist_id": int(playlist.id),
                    "playlist_name": clean_text(playlist.name),
                    "category_id": int(source_category.id) if source_category is not None else None,
                    "category_name": clean_text(getattr(source_category, "name", "")),
                    "upstream_item_id": clean_text(source_item.upstream_item_id),
                    "plot": summary_fields["plot"],
                    "genre": summary_fields["genre"],
                    "added": summary_fields["added"],
                }
            )

    has_more = len(rows) > resolved_limit
    return {
        "items": items,
        "offset": resolved_offset,
        "limit": resolved_limit,
        "has_more": has_more,
    }


async def fetch_upstream_vod_item_details(content_type: str, item_id: int) -> dict[str, object] | None:
    content_type = require_vod_content_type(content_type)
    async with Session() as session:
        result = await session.execute(
            select(XcVodItem, XcVodCategory, Playlist)
            .join(XcVodCategory, XcVodCategory.id == XcVodItem.category_id, isouter=True)
            .join(Playlist, Playlist.id == XcVodItem.playlist_id)
            .where(XcVodItem.id == int(item_id), XcVodItem.item_type == content_type)
        )
        row = result.first()
    if not row:
        return None

    source_item, source_category, playlist = row
    summary_fields = _vod_item_summary_fields(source_item.summary_json)
    metadata_payload = await _fetch_upstream_metadata(
        source_item,
        "get_vod_info" if content_type == VOD_KIND_MOVIE else "get_series_info",
        str(source_item.upstream_item_id),
        "vod_id" if content_type == VOD_KIND_MOVIE else "series_id",
    )
    info_payload = {}
    if isinstance(metadata_payload, dict):
        info_payload = _summary_info(metadata_payload)
        if not info_payload and isinstance(metadata_payload.get("movie_data"), dict):
            info_payload = metadata_payload.get("movie_data") or {}

    detail = {
        "id": int(source_item.id),
        "content_type": content_type,
        "title": clean_text(info_payload.get("name") or info_payload.get("title") or source_item.title),
        "year": clean_text(source_item.year) or _extract_year(info_payload),
        "rating": clean_text(info_payload.get("rating") or source_item.rating),
        "poster_url": clean_text(
            info_payload.get("movie_image")
            or info_payload.get("cover")
            or info_payload.get("cover_big")
            or source_item.poster_url
        ),
        "release_date": clean_text(info_payload.get("releaseDate") or info_payload.get("release_date") or source_item.release_date),
        "plot": clean_text(
            info_payload.get("plot")
            or info_payload.get("description")
            or info_payload.get("overview")
            or summary_fields["plot"]
        ),
        "genre": _nfo_text_values(info_payload.get("genre") or info_payload.get("genres") or summary_fields["genre"]),
        "cast": _nfo_text_values(info_payload.get("cast") or info_payload.get("actors")),
        "director": _nfo_text_values(info_payload.get("director") or info_payload.get("directors")),
        "playlist_id": int(playlist.id),
        "playlist_name": clean_text(playlist.name),
        "category_id": int(source_category.id) if source_category is not None else None,
        "category_name": clean_text(getattr(source_category, "name", "")),
        "container_extension": clean_text(source_item.container_extension).lstrip(".").lower() or "mp4",
        "upstream_item_id": clean_text(source_item.upstream_item_id),
        "episodes": [],
    }
    if content_type == VOD_KIND_SERIES and isinstance(metadata_payload, dict):
        detail["episodes"] = _flatten_series_episode_payload(metadata_payload)
    return detail


async def list_curated_library_categories(user: User | None, content_type: str) -> list[dict[str, object]]:
    content_type = require_vod_content_type(content_type)
    if not user_can_access_vod_kind(user, content_type):
        return []

    rows = await _group_rows_for_user(content_type)
    category_ids = [int(row.id) for row in rows]
    counts_by_category_id: dict[int, int] = {}
    if category_ids:
        async with Session() as session:
            result = await session.execute(
                select(VodCategoryItem.category_id, func.count(VodCategoryItem.id))
                .where(VodCategoryItem.category_id.in_(category_ids))
                .group_by(VodCategoryItem.category_id)
            )
            counts_by_category_id = {int(category_id): int(count or 0) for category_id, count in result.all()}

    return [
        {
            "id": int(row.id),
            "name": clean_text(row.name),
            "item_count": int(counts_by_category_id.get(int(row.id), 0)),
        }
        for row in rows
    ]


async def list_curated_library_items(
    user: User | None,
    content_type: str,
    category_id: int | None = None,
    search_query: str | None = None,
    offset: int = 0,
    limit: int = 50,
) -> dict[str, object]:
    content_type = require_vod_content_type(content_type)
    if not user_can_access_vod_kind(user, content_type):
        return {"items": [], "offset": 0, "limit": 0, "has_more": False}

    resolved_offset = max(0, int(offset or 0))
    resolved_limit = max(1, min(int(limit or 50), 200))
    query_text = clean_text(search_query).casefold()

    async with Session() as session:
        stmt = (
            select(VodCategoryItem, VodCategory)
            .join(VodCategory, VodCategory.id == VodCategoryItem.category_id)
            .join(VodCategoryItemSource, VodCategoryItemSource.category_item_id == VodCategoryItem.id)
            .join(XcVodItem, XcVodItem.id == VodCategoryItemSource.source_item_id)
            .join(Playlist, Playlist.id == XcVodItem.playlist_id)
            .where(
                VodCategory.enabled.is_(True),
                VodCategory.content_type == content_type,
                VodCategoryItem.item_type == content_type,
                Playlist.enabled.is_(True),
            )
            .order_by(
                VodCategory.sort_order.asc(),
                VodCategoryItem.sort_title.asc(),
                VodCategoryItem.title.asc(),
                VodCategoryItem.id.asc(),
            )
        )
        if category_id:
            stmt = stmt.where(VodCategory.id == int(category_id))
        if query_text:
            pattern = f"%{query_text}%"
            stmt = stmt.where(
                func.lower(VodCategoryItem.title).like(pattern)
                | func.lower(VodCategory.name).like(pattern)
                | func.lower(func.coalesce(VodCategoryItem.year, "")).like(pattern)
            )
        result = await session.execute(stmt)
        rows = result.all()

    deduped_items = []
    seen_item_ids: set[int] = set()
    for item, group in rows:
        item_id = int(item.id or 0)
        if item_id in seen_item_ids:
            continue
        seen_item_ids.add(item_id)
        summary_fields = _vod_item_summary_fields(item.summary_json)
        item_payload = {
            "id": item_id,
            "content_type": content_type,
            "title": clean_text(item.title),
            "year": clean_text(item.year),
            "rating": clean_text(item.rating),
            "poster_url": clean_text(item.poster_url),
            "release_date": clean_text(item.release_date),
            "category_id": int(group.id),
            "category_name": clean_text(group.name),
            "plot": summary_fields["plot"],
            "genre": summary_fields["genre"],
        }
        if content_type == VOD_KIND_MOVIE:
            item_payload["container_extension"] = _resolve_group_output_extension(group.profile_id, item.container_extension)
        deduped_items.append(item_payload)

    paged_items = deduped_items[resolved_offset : resolved_offset + resolved_limit]
    has_more = resolved_offset + resolved_limit < len(deduped_items)
    return {
        "items": paged_items,
        "offset": resolved_offset,
        "limit": resolved_limit,
        "has_more": has_more,
    }


async def fetch_curated_library_item_details(
    user: User | None, content_type: str, item_id: int
) -> dict[str, object] | None:
    content_type = require_vod_content_type(content_type)
    if not user_can_access_vod_kind(user, content_type):
        return None

    async with Session() as session:
        result = await session.execute(
            select(VodCategoryItem, VodCategory)
            .join(VodCategory, VodCategory.id == VodCategoryItem.category_id)
            .where(
                VodCategoryItem.id == int(item_id),
                VodCategoryItem.item_type == content_type,
                VodCategory.enabled.is_(True),
            )
        )
        row = result.first()
    if not row:
        return None

    item, category = row
    summary_fields = _vod_item_summary_fields(item.summary_json)
    detail = {
        "id": int(item.id),
        "content_type": content_type,
        "title": clean_text(item.title),
        "year": clean_text(item.year),
        "rating": clean_text(item.rating),
        "poster_url": clean_text(item.poster_url),
        "release_date": clean_text(item.release_date),
        "plot": summary_fields["plot"],
        "genre": summary_fields["genre"],
        "category_id": int(category.id),
        "category_name": clean_text(category.name),
        "episodes": [],
    }

    if content_type == VOD_KIND_MOVIE:
        payload = await fetch_vod_info_payload(int(item.id))
        if isinstance(payload, dict):
            info_payload = _summary_info(payload)
            movie_data = payload.get("movie_data") if isinstance(payload.get("movie_data"), dict) else {}
            detail["title"] = clean_text(info_payload.get("name") or movie_data.get("name") or item.title)
            detail["plot"] = clean_text(
                info_payload.get("plot")
                or movie_data.get("plot")
                or info_payload.get("description")
                or detail["plot"]
            )
            detail["poster_url"] = clean_text(
                info_payload.get("movie_image") or movie_data.get("movie_image") or item.poster_url
            )
            detail["genre"] = _nfo_text_values(
                info_payload.get("genre") or movie_data.get("genre") or info_payload.get("genres") or detail["genre"]
            )
        detail["container_extension"] = _resolve_group_output_extension(category.profile_id, item.container_extension)
        return detail

    payload = await fetch_series_info_payload(int(item.id))
    if isinstance(payload, dict):
        info_payload = _summary_info(payload)
        detail["title"] = clean_text(info_payload.get("name") or item.title)
        detail["plot"] = clean_text(
            info_payload.get("plot") or info_payload.get("description") or info_payload.get("overview") or detail["plot"]
        )
        detail["poster_url"] = clean_text(
            info_payload.get("cover") or info_payload.get("cover_big") or item.poster_url
        )
        detail["genre"] = _nfo_text_values(info_payload.get("genre") or info_payload.get("genres") or detail["genre"])
        detail["episodes"] = _flatten_series_episode_payload(payload)
    return detail


async def create_vod_group(payload: dict[str, object]) -> int:
    content_type = require_vod_content_type(payload.get("content_type"))
    category_ids = [int(item) for item in (payload.get("category_ids") or []) if str(item).isdigit()]
    raw_category_configs = payload.get("category_configs") or []
    category_config_map = _build_category_config_map(raw_category_configs)
    async with Session() as session:
        async with session.begin():
            group = VodCategory(
                content_type=content_type,
                name=clean_text(payload.get("name")) or "New Group",
                sort_order=int(payload.get("sort_order") or 0),
                enabled=bool(payload.get("enabled", True)),
                profile_id=clean_text(payload.get("profile_id")) or None,
                generate_strm_files=bool(payload.get("generate_strm_files", False)),
                expose_http_library=bool(payload.get("expose_http_library", False)),
                strm_base_url=clean_text(payload.get("strm_base_url")) or None,
            )
            session.add(group)
            await session.flush()
            for category_id in category_ids:
                category_config = category_config_map.get(category_id, {})
                session.add(
                    VodCategoryXcCategory(
                        category_id=group.id,
                        xc_category_id=category_id,
                        priority=int(category_config.get("priority") or 0),
                        strip_title_prefixes=_store_strip_config(category_config.get("strip_title_prefixes")),
                        strip_title_suffixes=_store_strip_config(category_config.get("strip_title_suffixes")),
                    )
                )
            group_id = int(group.id)
    await queue_rebuild_vod_group_cache(group_id)
    return group_id


async def update_vod_group(group_id: int, payload: dict[str, object]) -> bool:
    category_ids = [int(item) for item in (payload.get("category_ids") or []) if str(item).isdigit()]
    raw_category_configs = payload.get("category_configs") or []
    category_config_map = _build_category_config_map(raw_category_configs)
    async with Session() as session:
        async with session.begin():
            group = await session.get(VodCategory, int(group_id))
            if group is None:
                return False
            if "name" in payload:
                group.name = clean_text(payload.get("name")) or group.name
            if "sort_order" in payload:
                group.sort_order = int(payload.get("sort_order") or 0)
            if "enabled" in payload:
                group.enabled = bool(payload.get("enabled"))
            if "profile_id" in payload:
                group.profile_id = clean_text(payload.get("profile_id")) or None
            if "generate_strm_files" in payload:
                group.generate_strm_files = bool(payload.get("generate_strm_files"))
            if "expose_http_library" in payload:
                group.expose_http_library = bool(payload.get("expose_http_library"))
            if "strm_base_url" in payload:
                group.strm_base_url = clean_text(payload.get("strm_base_url")) or None
            if "category_ids" in payload:
                await session.execute(
                    delete(VodCategoryXcCategory).where(VodCategoryXcCategory.category_id == int(group_id))
                )
                for category_id in category_ids:
                    category_config = category_config_map.get(category_id, {})
                    session.add(
                        VodCategoryXcCategory(
                            category_id=int(group_id),
                            xc_category_id=category_id,
                            priority=int(category_config.get("priority") or 0),
                            strip_title_prefixes=_store_strip_config(category_config.get("strip_title_prefixes")),
                            strip_title_suffixes=_store_strip_config(category_config.get("strip_title_suffixes")),
                        )
                    )
    await queue_rebuild_vod_group_cache(int(group_id))
    return True


async def delete_vod_group(group_id: int) -> bool:
    async with Session() as session:
        async with session.begin():
            group = await session.get(VodCategory, int(group_id))
            if group is None:
                return False
            await session.delete(group)
    await queue_all_vod_category_strm_syncs()
    return True


def _load_cached_vod_http_manifest_sync(file_path: Path) -> dict[str, object]:
    cache_key = str(file_path)
    now_ts = time.time()
    mtime = file_path.stat().st_mtime if file_path.exists() else 0.0
    cached = _vod_http_manifest_cache.get(cache_key)
    if cached is not None:
        expires_at, cached_mtime, payload = cached
        if expires_at >= now_ts and cached_mtime == mtime and isinstance(payload, dict):
            return payload
    payload = _load_json_file_sync(file_path)
    _vod_http_manifest_cache[cache_key] = (now_ts + float(_VOD_HTTP_MANIFEST_CACHE_TTL_SECONDS), mtime, payload)
    return payload


async def load_vod_http_library_index() -> dict[str, object]:
    return await asyncio.to_thread(_load_cached_vod_http_manifest_sync, _vod_http_library_index_path())


async def resolve_vod_http_library_path(subpath: str) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    cleaned_subpath = str(subpath or "").strip().strip("/")
    parts = [clean_text(part) for part in cleaned_subpath.split("/") if clean_text(part)]
    index_payload = await load_vod_http_library_index()
    if not parts:
        return index_payload, {"type": "root"}

    content_dir = clean_text(parts[0])
    if content_dir not in {"Movies", "Shows"}:
        return index_payload, None
    if len(parts) == 1:
        return index_payload, {"type": "content_dir", "content_dir": content_dir}

    category_slug = clean_text(parts[1])
    categories = [row for row in (index_payload.get("categories") or []) if isinstance(row, dict)]
    category_entry = next(
        (
            row
            for row in categories
            if clean_text(row.get("content_dir")) == content_dir
            and clean_text(row.get("category_slug")) == category_slug
        ),
        None,
    )
    if category_entry is None:
        return index_payload, None

    manifest_relpath = clean_text(category_entry.get("manifest_path"))
    if not manifest_relpath:
        return index_payload, None
    manifest_payload = await asyncio.to_thread(
        _load_cached_vod_http_manifest_sync, _VOD_HTTP_LIBRARY_ROOT / manifest_relpath
    )
    nodes = manifest_payload.get("nodes") if isinstance(manifest_payload, dict) else {}
    if not isinstance(nodes, dict):
        return index_payload, None

    manifest_relpath_key = _vod_manifest_rel_key("/".join(parts[2:]))
    if not manifest_relpath_key:
        node = nodes.get("")
    else:
        node = nodes.get(manifest_relpath_key)
        if node is None:
            node = nodes.get(manifest_relpath_key.rstrip("/"))
    if not isinstance(node, dict):
        return index_payload, None

    return index_payload, {
        "type": clean_text(node.get("type")),
        "content_dir": content_dir,
        "category": category_entry,
        "manifest": manifest_payload,
        "node": node,
        "relative_path": manifest_relpath_key,
    }


async def _group_rows_for_user(kind: str) -> list[VodCategory]:
    async with Session() as session:
        stmt = (
            select(VodCategory)
            .join(VodCategoryItem, VodCategoryItem.category_id == VodCategory.id)
            .join(VodCategoryItemSource, VodCategoryItemSource.category_item_id == VodCategoryItem.id)
            .join(XcVodItem, XcVodItem.id == VodCategoryItemSource.source_item_id)
            .join(Playlist, Playlist.id == XcVodItem.playlist_id)
            .options(selectinload(VodCategory.xc_category_links))
            .where(
                VodCategory.content_type == kind,
                VodCategory.enabled.is_(True),
                Playlist.enabled.is_(True),
            )
            .distinct()
            .order_by(VodCategory.sort_order.asc(), VodCategory.name.asc(), VodCategory.id.asc())
        )
        result = await session.execute(stmt)
        return result.scalars().all()


async def build_curated_category_payloads(kind: str) -> list[dict[str, object]]:
    rows = await _group_rows_for_user(kind)
    return [
        {
            "category_id": str(row.id),
            "category_name": row.name,
            "parent_id": 0,
        }
        for row in rows
    ]


async def build_curated_item_payloads(kind: str, category_id: int | None = None) -> list[dict[str, object]]:
    kind = require_vod_content_type(kind)
    async with Session() as session:
        stmt = (
            select(VodCategoryItem, VodCategory)
            .join(VodCategory, VodCategory.id == VodCategoryItem.category_id)
            .join(VodCategoryItemSource, VodCategoryItemSource.category_item_id == VodCategoryItem.id)
            .join(XcVodItem, XcVodItem.id == VodCategoryItemSource.source_item_id)
            .join(Playlist, Playlist.id == XcVodItem.playlist_id)
            .where(
                VodCategory.enabled.is_(True),
                VodCategory.content_type == kind,
                VodCategoryItem.item_type == kind,
                Playlist.enabled.is_(True),
            )
            .order_by(
                VodCategory.sort_order.asc(),
                VodCategoryItem.sort_title.asc(),
                VodCategoryItem.title.asc(),
                VodCategoryItem.id.asc(),
            )
        )
        if category_id and str(category_id).isdigit():
            stmt = stmt.where(VodCategory.id == int(category_id))
        result = await session.execute(stmt)
        rows = result.all()

    results = []
    seen_item_ids: set[int] = set()
    for item, group in rows:
        item_id = int(item.id or 0)
        if item_id in seen_item_ids:
            continue
        seen_item_ids.add(item_id)
        summary = _load_summary(item.summary_json)
        output_extension = _resolve_group_output_extension(group.profile_id, item.container_extension)
        if kind == VOD_KIND_MOVIE:
            results.append(
                {
                    "num": 0,
                    "name": item.title,
                    "stream_type": "movie",
                    "stream_id": str(item.id),
                    "stream_icon": item.poster_url or "",
                    "category_id": str(group.id),
                    "container_extension": output_extension,
                    "rating": item.rating or "",
                    "added": clean_text(summary.get("added")),
                }
            )
        else:
            results.append(
                {
                    "num": 0,
                    "name": item.title,
                    "series_id": str(item.id),
                    "category_id": str(group.id),
                    "cover": item.poster_url or "",
                    "plot": clean_text(summary.get("plot")),
                    "rating": item.rating or "",
                    "releaseDate": item.release_date or "",
                    "container_extension": output_extension,
                }
            )
    return results


async def _get_group_item_source_rows(group_item_id: int, item_type: str) -> list:
    async with Session() as session:
        stmt = (
            select(VodCategoryItemSource, XcVodItem, VodCategoryItem, VodCategory)
            .join(XcVodItem, XcVodItem.id == VodCategoryItemSource.source_item_id)
            .join(Playlist, Playlist.id == XcVodItem.playlist_id)
            .join(VodCategoryItem, VodCategoryItem.id == VodCategoryItemSource.category_item_id)
            .join(VodCategory, VodCategory.id == VodCategoryItem.category_id)
            .where(
                VodCategoryItem.id == int(group_item_id),
                VodCategoryItem.item_type == item_type,
                VodCategory.enabled.is_(True),
                Playlist.enabled.is_(True),
            )
            .options(joinedload(XcVodItem.playlist), selectinload(VodCategory.xc_category_links))
            .order_by(VodCategoryItemSource.id.asc())
        )
        result = await session.execute(stmt)
        return result.all()


async def fetch_vod_info_payload(item_id: int) -> dict[str, object] | None:
    rows = await _get_group_item_source_rows(int(item_id), VOD_KIND_MOVIE)
    candidate = await _select_playback_candidate(rows, VOD_KIND_MOVIE)
    if candidate is None:
        return None
    payload = await _fetch_upstream_metadata(
        candidate.source_item,
        "get_vod_info",
        str(candidate.source_item.upstream_item_id),
        "vod_id",
    )
    if not isinstance(payload, dict):
        return None
    movie_data = payload.get("movie_data")
    if isinstance(movie_data, dict):
        movie_data["stream_id"] = str(candidate.group_item.id)
        movie_data["id"] = str(candidate.group_item.id)
        movie_data["name"] = candidate.group_item.title
        movie_data["title"] = candidate.group_item.title
        movie_data["container_extension"] = resolve_vod_output_extension(candidate)
    info = payload.get("info")
    if isinstance(info, dict):
        info["name"] = candidate.group_item.title
        if "movie_image" not in info and candidate.group_item.poster_url:
            info["movie_image"] = candidate.group_item.poster_url
    return payload


async def fetch_series_info_payload(item_id: int) -> dict[str, object] | None:
    rows = await _get_group_item_source_rows(int(item_id), VOD_KIND_SERIES)
    if not rows:
        return None
    payload = await _rebuild_series_episode_cache(int(item_id), rows)
    if not isinstance(payload, dict):
        return None
    return payload


async def fetch_xc_series_info_payload(source_item_id: int) -> dict[str, object] | None:
    async with Session() as session:
        source_item = await session.get(XcVodItem, int(source_item_id))
    if source_item is None or clean_text(source_item.item_type) != VOD_KIND_SERIES:
        return None
    payload = await _fetch_upstream_metadata(
        source_item,
        "get_series_info",
        str(source_item.upstream_item_id),
        "series_id",
    )
    return payload if isinstance(payload, dict) else None


async def resolve_xc_item_upstream_url(
    source_item_id: int,
    item_type: str,
    upstream_episode_id: str | None = None,
    container_extension: str | None = None,
) -> tuple[XcVodItem | None, str, XcAccount | None, str | None]:
    async with Session() as session:
        result = await session.execute(
            select(XcVodItem, Playlist)
            .join(Playlist, Playlist.id == XcVodItem.playlist_id)
            .where(XcVodItem.id == int(source_item_id))
            .options(joinedload(XcVodItem.playlist))
        )
        row = result.first()
    if not row:
        return None, "", None, "Imported XC VOD item was not found"

    source_item, playlist = row
    if playlist is None or not bool(getattr(playlist, "enabled", False)):
        return source_item, "", None, "Source playlist is disabled"
    host_url, account = await _select_account_for_playlist(playlist)
    if not host_url or account is None:
        return source_item, "", None, "No available XC account or reachable host was found"

    extension = clean_text(container_extension).lstrip(".").lower()
    if not extension:
        extension = clean_text(source_item.container_extension).lstrip(".").lower() or "mp4"

    if clean_text(item_type) == VOD_KIND_MOVIE:
        return (
            source_item,
            _build_upstream_movie_url(host_url, account, str(source_item.upstream_item_id), extension),
            account,
            None,
        )

    episode_id = clean_text(upstream_episode_id)
    if not episode_id:
        return source_item, "", account, "Series episode mapping is missing an upstream episode id"
    return (
        source_item,
        _build_upstream_series_url(host_url, account, episode_id, extension),
        account,
        None,
    )


async def _fetch_upstream_metadata(
    source_item: XcVodItem, action: str, upstream_item_id: str, param_name: str
) -> dict | list | None:
    cached = await _get_db_cached_metadata(int(source_item.playlist_id), action, upstream_item_id)
    if cached is not None:
        return cached
    async with Session() as session:
        playlist = await session.get(Playlist, int(source_item.playlist_id))
    if playlist is None:
        return None
    host_url, account = await _choose_working_xc_host(playlist)
    if not host_url or account is None:
        return None
    headers = _resolve_source_request_headers({}, playlist)
    request_params = {
        "username": account.username,
        "password": account.password,
        "action": action,
        param_name: upstream_item_id,
    }
    max_attempts = max(1, int(VOD_UPSTREAM_METADATA_RETRY_ATTEMPTS))
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            async with aiohttp.ClientSession(headers=headers) as http_session:
                payload = await _xc_request(
                    http_session,
                    host_url,
                    request_params,
                    retries=1,
                )
            if attempt > 1:
                logger.info(
                    "VOD upstream metadata request succeeded after retries playlist_id=%s action=%s upstream_item_id=%s attempts=%s",
                    int(source_item.playlist_id),
                    action,
                    upstream_item_id,
                    attempt,
                )
            else:
                logger.debug(
                    "VOD upstream metadata request succeeded playlist_id=%s action=%s upstream_item_id=%s attempt=%s",
                    int(source_item.playlist_id),
                    action,
                    upstream_item_id,
                    attempt,
                )
            await _set_db_cached_metadata(int(source_item.playlist_id), action, upstream_item_id, payload)
            return payload
        except aiohttp.ClientResponseError as exc:
            last_exc = exc
            should_retry = int(exc.status or 0) in {404, 429, 500, 502, 503, 504} and attempt < max_attempts
            if should_retry:
                delay = float(VOD_UPSTREAM_METADATA_RETRY_BASE_DELAY_SECONDS) * (2 ** (attempt - 1))
                logger.info(
                    "Retrying VOD upstream metadata request playlist_id=%s action=%s upstream_item_id=%s status=%s attempt=%s/%s delay=%.2fs",
                    int(source_item.playlist_id),
                    action,
                    upstream_item_id,
                    int(exc.status or 0),
                    attempt + 1,
                    max_attempts,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            logger.warning(
                "VOD upstream metadata request failed playlist_id=%s action=%s upstream_item_id=%s status=%s url=%s",
                int(source_item.playlist_id),
                action,
                upstream_item_id,
                int(exc.status or 0),
                str(exc.request_info.real_url) if exc.request_info is not None else None,
            )
            return None
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
            last_exc = exc
            if attempt < max_attempts:
                delay = float(VOD_UPSTREAM_METADATA_RETRY_BASE_DELAY_SECONDS) * (2 ** (attempt - 1))
                logger.info(
                    "Retrying VOD upstream metadata request playlist_id=%s action=%s upstream_item_id=%s attempt=%s/%s delay=%.2fs error=%s",
                    int(source_item.playlist_id),
                    action,
                    upstream_item_id,
                    attempt + 1,
                    max_attempts,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
                continue
            logger.warning(
                "VOD upstream metadata request failed playlist_id=%s action=%s upstream_item_id=%s error=%s",
                int(source_item.playlist_id),
                action,
                upstream_item_id,
                exc,
            )
            return None
    if last_exc:
        logger.warning(
            "VOD upstream metadata request exhausted retries playlist_id=%s action=%s upstream_item_id=%s error=%s",
            int(source_item.playlist_id),
            action,
            upstream_item_id,
            last_exc,
        )
    return None


async def _rebuild_series_episode_cache(group_item_id: int, rows) -> dict[str, object] | None:
    source_payloads = []
    representative_payload = None
    group_profile_id = ""
    strip_rules_by_category_id = {}
    for source_link, source_item, group_item, group in rows:
        group_profile_id = clean_text(getattr(group, "profile_id", "")) or group_profile_id
        if not strip_rules_by_category_id:
            strip_rules_by_category_id = {
                int(link.xc_category_id): (
                    _group_category_strip_prefixes(link),
                    _group_category_strip_suffixes(link),
                )
                for link in _ordered_vod_category_links(group.xc_category_links)
            }
        payload = await _fetch_upstream_metadata(
            source_item, "get_series_info", str(source_item.upstream_item_id), "series_id"
        )
        if not isinstance(payload, dict):
            continue
        source_payloads.append((source_link, source_item, payload))
        if representative_payload is None:
            representative_payload = json.loads(json.dumps(payload))

    if representative_payload is None:
        return None

    dedupe_buckets = {}
    for source_link, source_item, payload in source_payloads:
        episodes = payload.get("episodes")
        if not isinstance(episodes, dict):
            continue
        for season_key, entries in episodes.items():
            if not isinstance(entries, list):
                continue
            season_number = int(season_key) if str(season_key).isdigit() else None
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                upstream_episode_id = clean_text(entry.get("id") or entry.get("stream_id"))
                if not upstream_episode_id:
                    continue
                episode_info = entry.get("info") if isinstance(entry.get("info"), dict) else {}
                episode_number = int(entry.get("episode_num") or 0) or None
                title = clean_text(entry.get("title"))
                strip_prefixes, strip_suffixes = strip_rules_by_category_id.get(
                    int(source_item.category_id or 0), ([], [])
                )
                display_title = _export_title_from_source_title(
                    title,
                    prefixes=strip_prefixes,
                    suffixes=strip_suffixes,
                )
                dedupe_key = _episode_dedupe_key(
                    season_number,
                    episode_number,
                    title,
                    prefixes=strip_prefixes,
                    suffixes=strip_suffixes,
                    tmdb_id=episode_info.get("tmdb_id"),
                )
                bucket = dedupe_buckets.setdefault(
                    dedupe_key,
                    {
                        "representative": {
                            "season_number": season_number,
                            "episode_number": episode_number,
                            "title": display_title or title,
                            "container_extension": clean_text(entry.get("container_extension"))
                            or source_item.container_extension,
                            "summary_json": _summary_json(entry),
                        },
                        "sources": [],
                    },
                )
                bucket["sources"].append(
                    {
                        "source_link": source_link,
                        "source_item": source_item,
                        "upstream_episode_id": upstream_episode_id,
                        "season_number": season_number,
                        "episode_number": episode_number,
                        "title": display_title or title,
                        "tmdb_id": clean_text(episode_info.get("tmdb_id")),
                        "container_extension": clean_text(entry.get("container_extension"))
                        or source_item.container_extension,
                        "summary_json": _summary_json(entry),
                    }
                )

    episode_id_map = {}
    async with Session() as session:
        async with session.begin():
            existing_result = await session.execute(
                select(VodCategoryEpisode).where(VodCategoryEpisode.category_item_id == int(group_item_id))
            )
            existing_episodes_by_dedupe = {
                clean_text(row.dedupe_key): row for row in existing_result.scalars().all() if clean_text(row.dedupe_key)
            }
            seen_episode_dedupe_keys = set()
            pending_rows = []
            for dedupe_key, bucket in dedupe_buckets.items():
                representative = bucket["representative"]
                episode_row = existing_episodes_by_dedupe.get(dedupe_key)
                if episode_row is None:
                    episode_row = VodCategoryEpisode(
                        category_item_id=int(group_item_id),
                        dedupe_key=dedupe_key,
                    )
                    session.add(episode_row)
                episode_row.season_number = representative["season_number"]
                episode_row.episode_number = representative["episode_number"]
                episode_row.title = _truncated_vod_text(representative["title"])
                episode_row.container_extension = representative["container_extension"]
                episode_row.summary_json = representative["summary_json"]
                seen_episode_dedupe_keys.add(dedupe_key)
                pending_rows.append((dedupe_key, episode_row, bucket["sources"]))

            if pending_rows:
                await session.flush()

            active_episode_ids = [int(episode_row.id) for _, episode_row, _ in pending_rows]
            if active_episode_ids:
                await session.execute(
                    delete(VodCategoryEpisodeSource).where(VodCategoryEpisodeSource.episode_id.in_(active_episode_ids))
                )

            for dedupe_key, episode_row, source_entries in pending_rows:
                episode_id_map[dedupe_key] = int(episode_row.id)
                for source_entry in source_entries:
                    session.add(
                        VodCategoryEpisodeSource(
                            episode_id=int(episode_row.id),
                            category_item_source_id=int(source_entry["source_link"].id),
                            upstream_episode_id=source_entry["upstream_episode_id"],
                            season_number=source_entry["season_number"],
                            episode_number=source_entry["episode_number"],
                            title=_truncated_vod_text(source_entry["title"]),
                            container_extension=source_entry["container_extension"],
                            summary_json=source_entry["summary_json"],
                        )
                    )

            stale_episode_ids = [
                int(row.id)
                for dedupe_key, row in existing_episodes_by_dedupe.items()
                if dedupe_key not in seen_episode_dedupe_keys
            ]
            if stale_episode_ids:
                await session.execute(delete(VodCategoryEpisode).where(VodCategoryEpisode.id.in_(stale_episode_ids)))

    merged_episodes = {}
    for dedupe_key, bucket in dedupe_buckets.items():
        representative = bucket["representative"]
        season_key = str(representative["season_number"] or 0)
        entry = json.loads(json.dumps(_load_summary(representative["summary_json"])))
        if not isinstance(entry, dict):
            entry = {}
        local_episode_id = str(int(episode_id_map[dedupe_key]))
        entry["id"] = local_episode_id
        entry["stream_id"] = local_episode_id
        entry["title"] = representative["title"]
        entry["container_extension"] = _resolve_group_output_extension(
            group_profile_id,
            representative["container_extension"] or "",
        )
        if representative["episode_number"] is not None:
            entry["episode_num"] = int(representative["episode_number"])
        merged_episodes.setdefault(season_key, []).append(entry)
    for season_key, entries in merged_episodes.items():
        merged_episodes[season_key] = sorted(
            entries,
            key=lambda entry: (
                int(entry.get("episode_num") or 0),
                clean_text(entry.get("title")),
            ),
        )
    representative_payload["episodes"] = merged_episodes
    info_payload = representative_payload.get("info")
    if isinstance(info_payload, dict) and rows:
        info_payload["name"] = clean_text(getattr(rows[0][2], "title", "")) or info_payload.get("name")
    elif rows:
        representative_payload["info"] = {
            "name": clean_text(getattr(rows[0][2], "title", "")),
        }
    return representative_payload


async def resolve_movie_playback(item_id: int) -> VodCuratedPlaybackCandidate | None:
    candidates = await resolve_movie_playback_candidates(int(item_id))
    return candidates[0] if candidates else None


async def resolve_movie_playback_candidates(item_id: int) -> list[VodCuratedPlaybackCandidate]:
    rows = await _get_group_item_source_rows(int(item_id), VOD_KIND_MOVIE)
    return await _build_playback_candidates(rows, VOD_KIND_MOVIE)


async def resolve_episode_playback(
    episode_id: int,
) -> tuple[VodCuratedPlaybackCandidate | None, VodCategoryEpisode | None]:
    async with Session() as session:
        result = await session.execute(
            select(
                VodCategoryEpisode,
                VodCategoryEpisodeSource,
                VodCategoryItemSource,
                XcVodItem,
                VodCategoryItem,
                VodCategory,
            )
            .join(VodCategoryEpisodeSource, VodCategoryEpisodeSource.episode_id == VodCategoryEpisode.id)
            .join(VodCategoryItemSource, VodCategoryItemSource.id == VodCategoryEpisodeSource.category_item_source_id)
            .join(XcVodItem, XcVodItem.id == VodCategoryItemSource.source_item_id)
            .join(Playlist, Playlist.id == XcVodItem.playlist_id)
            .join(VodCategoryItem, VodCategoryItem.id == VodCategoryItemSource.category_item_id)
            .join(VodCategory, VodCategory.id == VodCategoryItem.category_id)
            .where(VodCategoryEpisode.id == int(episode_id), Playlist.enabled.is_(True), VodCategory.enabled.is_(True))
            .options(joinedload(XcVodItem.playlist))
        )
        rows = result.all()
        if not rows:
            return None, None
        episode_row = rows[0][0]
    candidate_rows = [
        (source_link, source_item, group_item, group, episode_source)
        for _episode, episode_source, source_link, source_item, group_item, group in rows
    ]
    candidates = await _build_playback_candidates(candidate_rows, VOD_KIND_SERIES)
    candidate = candidates[0] if candidates else None
    if candidate:
        candidate.episode = episode_row
    return candidate, episode_row


async def resolve_episode_playback_candidates(
    episode_id: int,
) -> tuple[list[VodCuratedPlaybackCandidate], VodCategoryEpisode | None]:
    async with Session() as session:
        result = await session.execute(
            select(
                VodCategoryEpisode,
                VodCategoryEpisodeSource,
                VodCategoryItemSource,
                XcVodItem,
                VodCategoryItem,
                VodCategory,
            )
            .join(VodCategoryEpisodeSource, VodCategoryEpisodeSource.episode_id == VodCategoryEpisode.id)
            .join(VodCategoryItemSource, VodCategoryItemSource.id == VodCategoryEpisodeSource.category_item_source_id)
            .join(XcVodItem, XcVodItem.id == VodCategoryItemSource.source_item_id)
            .join(Playlist, Playlist.id == XcVodItem.playlist_id)
            .join(VodCategoryItem, VodCategoryItem.id == VodCategoryItemSource.category_item_id)
            .join(VodCategory, VodCategory.id == VodCategoryItem.category_id)
            .where(VodCategoryEpisode.id == int(episode_id), Playlist.enabled.is_(True), VodCategory.enabled.is_(True))
            .options(joinedload(XcVodItem.playlist), selectinload(VodCategory.xc_category_links))
        )
        rows = result.all()
        if not rows:
            return [], None
        episode_row = rows[0][0]
    candidate_rows = [
        (source_link, source_item, group_item, group, episode_source)
        for _episode, episode_source, source_link, source_item, group_item, group in rows
    ]
    candidates = await _build_playback_candidates(candidate_rows, VOD_KIND_SERIES)
    for candidate in candidates:
        candidate.episode = episode_row
    return candidates, episode_row


def _candidate_row_priority(row) -> int:
    if len(row) == 5:
        _source_link, source_item, _group_item, group, _episode_source = row
    else:
        _source_link, source_item, _group_item, group = row
    priority_map = _vod_category_priority_map(getattr(group, "xc_category_links", None))
    return int(priority_map.get(int(getattr(source_item, "category_id", 0) or 0), 0))


def _ordered_playback_rows(rows) -> list:
    ordered_rows = list(rows or [])
    ordered_rows.sort(
        key=lambda row: (
            -_candidate_row_priority(row),
            int(getattr(row[0], "id", 0) or 0),
        )
    )
    return ordered_rows


async def _build_playback_candidates(rows, item_type: str) -> list[VodCuratedPlaybackCandidate]:
    candidates = []
    fallback = None
    for row in _ordered_playback_rows(rows):
        if len(row) == 5:
            source_link, source_item, group_item, group, episode_source = row
        else:
            source_link, source_item, group_item, group = row
            episode_source = None
        async with Session() as session:
            playlist = await session.get(Playlist, int(source_item.playlist_id))
        if playlist is None or not bool(getattr(playlist, "enabled", False)):
            continue
        host_url, xc_account = await _select_account_for_playlist(playlist)
        candidate = VodCuratedPlaybackCandidate(
            group_item=group_item,
            source_link=source_link,
            source_item=source_item,
            group=group,
            content_type=item_type,
            xc_account=xc_account,
            host_url=host_url,
            episode_source=episode_source,
        )
        if host_url and xc_account is not None:
            candidates.append(candidate)
            continue
        if fallback is None:
            fallback = candidate
    if candidates:
        return candidates
    return [fallback] if fallback is not None else []


async def _select_playback_candidate(rows, item_type: str) -> VodCuratedPlaybackCandidate | None:
    candidates = await _build_playback_candidates(rows, item_type)
    return candidates[0] if candidates else None


def resolve_vod_profile_id(candidate: VodCuratedPlaybackCandidate) -> str:
    source_container = (
        clean_text(getattr(candidate.episode_source, "container_extension", ""))
        or clean_text(getattr(candidate.source_item, "container_extension", ""))
        or clean_text(getattr(candidate.group_item, "container_extension", ""))
    )
    configured_profile = clean_text(getattr(candidate.group, "profile_id", "")) if candidate and candidate.group else ""
    return _resolve_group_output_profile_id(configured_profile, source_container)


def resolve_vod_output_extension(candidate: VodCuratedPlaybackCandidate) -> str:
    profile_id = resolve_vod_profile_id(candidate)
    fallback = (
        clean_text(getattr(candidate.episode_source, "container_extension", ""))
        or clean_text(getattr(candidate.source_item, "container_extension", ""))
        or clean_text(getattr(candidate.group_item, "container_extension", ""))
    )
    return _profile_extension(profile_id, fallback_extension=fallback)


async def build_upstream_playback_url(
    candidate: VodCuratedPlaybackCandidate, episode_mapping: VodCategoryEpisode | None = None
) -> str:
    host_url = clean_text(candidate.host_url)
    account = candidate.xc_account
    if not host_url or account is None:
        async with Session() as session:
            playlist = await session.get(Playlist, int(candidate.source_item.playlist_id))
        if playlist is None:
            return ""
        host_url, account = await _select_account_for_playlist(playlist)
    if not host_url or account is None:
        return ""
    extension = (
        clean_text(getattr(candidate.episode_source, "container_extension", ""))
        or clean_text(getattr(candidate.source_item, "container_extension", ""))
        or clean_text(getattr(candidate.group_item, "container_extension", ""))
    )
    if candidate.content_type == VOD_KIND_MOVIE:
        return _build_upstream_movie_url(host_url, account, str(candidate.source_item.upstream_item_id), extension)
    upstream_episode_id = ""
    if episode_mapping is not None:
        async with Session() as session:
            episode_source_result = await session.execute(
                select(VodCategoryEpisodeSource.upstream_episode_id, VodCategoryEpisodeSource.container_extension)
                .where(
                    VodCategoryEpisodeSource.episode_id == int(episode_mapping.id),
                    VodCategoryEpisodeSource.category_item_source_id == int(candidate.source_link.id),
                )
                .order_by(VodCategoryEpisodeSource.id.asc())
            )
            episode_source_row = episode_source_result.first()
            if episode_source_row:
                upstream_episode_id = clean_text(episode_source_row[0] or "")
                extension = clean_text(episode_source_row[1] or "") or extension
    return _build_upstream_series_url(host_url, account, upstream_episode_id, extension)
