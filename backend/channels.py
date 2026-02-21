#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import base64
import difflib
import hashlib
import json
import logging
import os
import re
import time
from collections import OrderedDict
from mimetypes import guess_type
from urllib.parse import urlparse, urlunparse

import aiofiles
import aiohttp
import requests
from sqlalchemy import BigInteger, and_, cast, delete, func, or_, select, tuple_
from sqlalchemy.orm import joinedload, selectinload

from backend.ffmpeg import generate_iptv_url
from backend.models import (
    Channel,
    ChannelSource,
    ChannelSuggestion,
    ChannelTag,
    Epg,
    EpgChannelProgrammes,
    EpgChannels,
    Playlist,
    PlaylistStreams,
    Recording,
    RecordingRule,
    XcAccount,
    Session,
    channels_tags_association_table,
    db,
)
from backend.playlists import (
    XC_ACCOUNT_TYPE,
    _build_xc_live_stream_url,
    _get_enabled_xc_accounts_async,
    _normalize_xc_host,
)
from backend.config import flask_run_port
from backend.cso import (
    build_cso_stream_query,
    cso_runtime_capabilities,
)
from backend.stream_profiles import profile_from_cso_policy
from backend.streaming import (
    LOCAL_PROXY_HOST_PLACEHOLDER,
    append_stream_key,
    build_configured_hls_proxy_url,
    get_tvh_stream_auth,
    is_local_hls_proxy_url,
    normalize_local_proxy_url,
)
from backend.tvheadend.tvh_requests import get_tvh
from backend.utils import normalize_id

logger = logging.getLogger("tic.channels")


def _extract_cso_payload(data, current_enabled=False, current_policy=None):
    raw_enabled = data.get("cso_enabled", current_enabled)
    cso_enabled = bool(raw_enabled)
    requested_profile = str(data.get("cso_profile") or "").strip().lower()
    if not requested_profile:
        requested_profile = profile_from_cso_policy(current_policy)
    cso_policy = json.dumps({"profile": requested_profile or "default"}, sort_keys=True)
    return cso_enabled, cso_policy


def _resolve_hls_proxy_prefix():
    prefix = os.environ.get("HLS_PROXY_PREFIX", "/")
    if not prefix.startswith("/"):
        prefix = "/" + prefix
    return prefix.rstrip("/")


def build_cso_channel_stream_url(
    *,
    base_url,
    channel_id,
    stream_key=None,
    username=None,
    profile="tvh",
    connection_id=None,
):
    prefix = _resolve_hls_proxy_prefix()
    path = f"{prefix}/channel/{channel_id}"
    query = build_cso_stream_query(
        profile=profile,
        connection_id=connection_id,
        stream_key=stream_key,
        username=username,
    )
    if query:
        return f"{base_url.rstrip('/')}{path}?{query}"
    return f"{base_url.rstrip('/')}{path}"


def _apply_playlist_hls_proxy(playlist_info, stream_url: str, instance_id: str) -> str:
    if not playlist_info:
        return stream_url
    return build_configured_hls_proxy_url(
        stream_url,
        base_url=LOCAL_PROXY_HOST_PLACEHOLDER,
        instance_id=instance_id,
        use_hls_proxy=bool(playlist_info.use_hls_proxy),
        use_custom_hls_proxy=bool(playlist_info.use_custom_hls_proxy),
        custom_hls_proxy_path=playlist_info.hls_proxy_path,
        chain_custom_hls_proxy=bool(playlist_info.chain_custom_hls_proxy),
        ffmpeg=bool(getattr(playlist_info, "hls_proxy_use_ffmpeg", False)),
        prebuffer=getattr(playlist_info, "hls_proxy_prebuffer", "1M"),
    )


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


def _channel_sync_state_path(config):
    return os.path.join(config.config_path, "cache", "channel_sync_state.json")


def _logo_source_state_path(config):
    return os.path.join(config.config_path, "cache", "channel_logo_sources.json")


def _logo_health_state_path(config):
    return os.path.join(config.config_path, "cache", "channel_logo_health.json")


def _read_json_file(path, default):
    try:
        if not os.path.exists(path):
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json_file(path, payload):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def read_logo_health_map(config):
    payload = _read_json_file(_logo_health_state_path(config), {})
    if isinstance(payload, dict):
        channels = payload.get("channels", {})
        if isinstance(channels, dict):
            return channels
    return {}


def _build_channel_sync_signature(channels, tic_base_url):
    digest = hashlib.sha1()
    digest.update(f"tic_base_url={tic_base_url or ''}".encode("utf-8"))
    for channel in channels:
        digest.update(
            "|".join(
                [
                    str(channel.id),
                    str(bool(channel.enabled)),
                    str(channel.name or ""),
                    str(channel.number or ""),
                    str(channel.logo_url or ""),
                    str(channel.tvh_uuid or ""),
                    str(channel.guide_id or ""),
                    str(channel.guide_channel_id or ""),
                ]
            ).encode("utf-8")
        )
        tag_names = sorted((tag.name or "") for tag in (channel.tags or []))
        digest.update(("tags:" + ",".join(tag_names)).encode("utf-8"))
        source_rows = sorted(
            (
                str(source.playlist_id or ""),
                str(source.xc_account_id or ""),
                str(source.priority or ""),
                str(source.playlist_stream_name or ""),
                str(source.playlist_stream_url or ""),
                str(bool(getattr(source, "use_hls_proxy", False))),
                str(source.tvh_uuid or ""),
            )
            for source in (channel.sources or [])
        )
        for row in source_rows:
            digest.update(("src:" + "|".join(row)).encode("utf-8"))
    return digest.hexdigest()


def _logo_cache_token(source_logo_url: str) -> str:
    return hashlib.sha1((source_logo_url or "").encode("utf-8")).hexdigest()[:12]


def build_channel_logo_proxy_url(channel_id, base_url, source_logo_url=""):
    base = (base_url or "").rstrip("/")
    token = _logo_cache_token(source_logo_url)
    return f"{base}/tic-api/channels/{channel_id}/logo/{token}.png"


_EPG_NAME_QUALITY_TOKENS = {"hd", "fhd", "uhd", "sd", "4k"}


def _safe_int(value, fallback=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _normalize_match_name(value):
    lowered = (value or "").strip().lower()
    lowered = re.sub(r"[^a-z0-9+\s]", " ", lowered)
    tokens = [token for token in lowered.split() if token and token not in _EPG_NAME_QUALITY_TOKENS]
    return " ".join(tokens).strip()


def _extract_plus_one_variant(value):
    lowered = (value or "").strip().lower()
    normalized = re.sub(r"[^a-z0-9+\s]", " ", lowered)
    plus_one = bool(re.search(r"(?:^|\s|\+)plus\s*1(?:\s|$)|(?:^|\s)\+\s*1(?:\s|$)", normalized))
    base = re.sub(r"(?:^|\s|\+)plus\s*1(?:\s|$)", " ", normalized)
    base = re.sub(r"(?:^|\s)\+\s*1(?:\s|$)", " ", base)
    base = _normalize_match_name(base)
    return base, plus_one


def _channel_source_sort_key(source):
    priority = _safe_int(getattr(source, "priority", None), fallback=9999)
    return priority, _safe_int(getattr(source, "id", None), fallback=0)


def _build_epg_channel_lookup(epg_rows):
    by_channel_id = {}
    by_normalized_name = {}
    by_base_name = {}
    for row in epg_rows:
        channel_key = (row["channel_id"] or "").strip().lower()
        if channel_key:
            by_channel_id.setdefault(channel_key, []).append(row)
        normalized_name = _normalize_match_name(row["name"])
        base_name, plus_one = _extract_plus_one_variant(row["name"])
        row["normalized_name"] = normalized_name
        row["base_name"] = base_name
        row["plus_one"] = plus_one
        if normalized_name:
            by_normalized_name.setdefault(normalized_name, []).append(row)
        if base_name:
            by_base_name.setdefault(base_name, []).append(row)
    return by_channel_id, by_normalized_name, by_base_name


def _dedupe_and_rank_candidates(raw_candidates, max_candidates_per_channel, programme_counts_map=None):
    programme_counts_map = programme_counts_map or {}
    best = {}
    for candidate in raw_candidates:
        key = (candidate["epg_id"], candidate["epg_channel_id"])
        existing = best.get(key)
        if not existing or candidate["score"] > existing["score"]:
            best[key] = candidate
    ranked = sorted(
        best.values(),
        key=lambda item: (
            -float(item.get("score") or 0),
            str(item.get("epg_name") or ""),
            str(item.get("epg_display_name") or ""),
            str(item.get("epg_channel_id") or ""),
        ),
    )
    output = []
    for index, candidate in enumerate(ranked[:max_candidates_per_channel], start=1):
        epg_channel_row_id = candidate.get("epg_channel_row_id")
        total_programmes = 0
        if epg_channel_row_id is not None:
            total_programmes = int(programme_counts_map.get(int(epg_channel_row_id), 0) or 0)
        output.append(
            {
                "rank": index,
                "score": round(float(candidate.get("score") or 0), 4),
                "reason": candidate.get("reason") or "name_fuzzy",
                "epg_id": candidate.get("epg_id"),
                "epg_name": candidate.get("epg_name"),
                "epg_channel_row_id": epg_channel_row_id,
                "epg_channel_id": candidate.get("epg_channel_id"),
                "epg_display_name": candidate.get("epg_display_name"),
                "total_programmes": total_programmes,
            }
        )
    return output


async def build_bulk_epg_match_preview(
    *,
    channel_ids=None,
    overwrite_existing=False,
    max_candidates_per_channel=5,
):
    normalized_ids = []
    for channel_id in channel_ids or []:
        try:
            normalized_ids.append(normalize_id(channel_id, "channel"))
        except ValueError:
            continue
    normalized_ids = sorted(set(normalized_ids))
    if not normalized_ids:
        return {"rows": [], "summary": {"channels_considered": 0, "with_candidates": 0, "without_candidates": 0}}

    max_candidates_per_channel = max(1, min(_safe_int(max_candidates_per_channel, 5), 10))

    async with Session() as session:
        channels_result = await session.execute(
            select(Channel)
            .options(joinedload(Channel.sources))
            .where(Channel.id.in_(normalized_ids))
            .order_by(Channel.number.asc(), Channel.name.asc())
        )
        channels = channels_result.scalars().unique().all()

        # Load enabled EPG channels once and perform matching in memory.
        epg_rows_result = await session.execute(
            select(
                EpgChannels.id.label("epg_channel_row_id"),
                EpgChannels.epg_id.label("epg_id"),
                Epg.name.label("epg_name"),
                EpgChannels.channel_id.label("channel_id"),
                EpgChannels.name.label("name"),
            )
            .join(Epg, Epg.id == EpgChannels.epg_id)
            .where(Epg.enabled.is_(True))
        )
        epg_rows = [
            {
                "epg_channel_row_id": int(row.epg_channel_row_id),
                "epg_id": int(row.epg_id),
                "epg_name": row.epg_name,
                "channel_id": row.channel_id or "",
                "name": row.name or "",
            }
            for row in epg_rows_result.all()
        ]

        stream_match_sources = []
        for channel in channels:
            for source in sorted(channel.sources or [], key=_channel_source_sort_key):
                playlist_id = getattr(source, "playlist_id", None)
                if not playlist_id:
                    continue
                source_name = (getattr(source, "playlist_stream_name", None) or "").strip()
                source_url = (getattr(source, "playlist_stream_url", None) or "").strip()
                if source_name:
                    stream_match_sources.append(("name", int(playlist_id), source_name))
                if source_url:
                    stream_match_sources.append(("url", int(playlist_id), source_url))

        stream_map = {}
        if stream_match_sources:
            name_pairs = sorted(
                {(playlist_id, value) for mode, playlist_id, value in stream_match_sources if mode == "name"}
            )
            url_pairs = sorted(
                {(playlist_id, value) for mode, playlist_id, value in stream_match_sources if mode == "url"}
            )
            match_clauses = []
            if name_pairs:
                name_condition = or_(
                    *[
                        and_(PlaylistStreams.playlist_id == playlist_id, PlaylistStreams.name == stream_name)
                        for playlist_id, stream_name in name_pairs
                    ]
                )
                match_clauses.append(name_condition)
            if url_pairs:
                url_condition = or_(
                    *[
                        and_(PlaylistStreams.playlist_id == playlist_id, PlaylistStreams.url == stream_url)
                        for playlist_id, stream_url in url_pairs
                    ]
                )
                match_clauses.append(url_condition)
            if match_clauses:
                streams_result = await session.execute(
                    select(
                        PlaylistStreams.id,
                        PlaylistStreams.playlist_id,
                        PlaylistStreams.name,
                        PlaylistStreams.url,
                        PlaylistStreams.channel_id,
                        PlaylistStreams.tvg_id,
                    ).where(or_(*match_clauses))
                )
                for row in streams_result.all():
                    playlist_id = int(row.playlist_id) if row.playlist_id is not None else None
                    if playlist_id is None:
                        continue
                    if row.name:
                        stream_map[("name", playlist_id, row.name.strip())] = row
                    if row.url:
                        stream_map[("url", playlist_id, row.url.strip())] = row

    by_channel_id, by_normalized_name, by_base_name = _build_epg_channel_lookup(epg_rows)
    normalized_name_keys = list(by_normalized_name.keys())
    epg_rows_by_tuple = {(row["epg_id"], row["channel_id"]): row for row in epg_rows}

    row_buffers = []
    with_candidates = 0
    without_candidates = 0

    for channel in channels:
        channel_name = (channel.name or "").strip()
        channel_name_normalized = _normalize_match_name(channel_name)
        channel_base_name, channel_plus_one = _extract_plus_one_variant(channel_name)
        raw_candidates = []

        def _append_candidate(epg_row, score, reason):
            raw_candidates.append(
                {
                    "score": score,
                    "reason": reason,
                    "epg_id": epg_row["epg_id"],
                    "epg_name": epg_row["epg_name"],
                    "epg_channel_row_id": epg_row["epg_channel_row_id"],
                    "epg_channel_id": epg_row["channel_id"],
                    "epg_display_name": epg_row["name"],
                }
            )

        # 1) Source linked identifiers.
        for source in sorted(channel.sources or [], key=_channel_source_sort_key):
            playlist_id = getattr(source, "playlist_id", None)
            if not playlist_id:
                continue
            source_name = (getattr(source, "playlist_stream_name", None) or "").strip()
            source_url = (getattr(source, "playlist_stream_url", None) or "").strip()
            stream_row = None
            if source_name:
                stream_row = stream_map.get(("name", int(playlist_id), source_name))
            if not stream_row and source_url:
                stream_row = stream_map.get(("url", int(playlist_id), source_url))
            if not stream_row:
                continue
            provider_channel_id = (stream_row.channel_id or "").strip()
            tvg_id = (stream_row.tvg_id or "").strip()
            if provider_channel_id:
                for epg_row in by_channel_id.get(provider_channel_id.lower(), []):
                    _append_candidate(epg_row, 1.0, "source_channel_id_exact")
            if tvg_id:
                for epg_row in by_channel_id.get(tvg_id.lower(), []):
                    _append_candidate(epg_row, 0.99, "source_tvg_id_exact")

        # 2) Existing mapping.
        if channel.guide_id and channel.guide_channel_id:
            existing = epg_rows_by_tuple.get((int(channel.guide_id), str(channel.guide_channel_id)))
            if existing:
                _append_candidate(existing, 0.97, "existing_mapping_valid")

        # 3) Exact normalized name.
        for epg_row in by_normalized_name.get(channel_name_normalized, []):
            if bool(epg_row["plus_one"]) == bool(channel_plus_one):
                _append_candidate(epg_row, 0.94, "name_exact")
            else:
                _append_candidate(epg_row, 0.9, "name_variant_plus_one_penalty")

        # Timeshift fallback: same base, different variant.
        for epg_row in by_base_name.get(channel_base_name, []):
            if not channel_base_name:
                continue
            if bool(epg_row["plus_one"]) != bool(channel_plus_one):
                _append_candidate(epg_row, 0.89, "name_variant_plus_one_penalty")

        # 4) Fuzzy pass.
        if channel_name_normalized and normalized_name_keys:
            fuzzy_names = difflib.get_close_matches(
                channel_name_normalized,
                normalized_name_keys,
                n=8,
                cutoff=0.88,
            )
            for matched_name in fuzzy_names:
                ratio = difflib.SequenceMatcher(None, channel_name_normalized, matched_name).ratio()
                if ratio < 0.88:
                    continue
                for epg_row in by_normalized_name.get(matched_name, []):
                    score = min(0.885, round(ratio, 4))
                    _append_candidate(epg_row, score, "name_fuzzy")

        candidates = _dedupe_and_rank_candidates(raw_candidates, max_candidates_per_channel)

        default_selected = bool(candidates)
        if channel.guide_id and channel.guide_channel_id and not overwrite_existing:
            default_selected = False

        if candidates:
            with_candidates += 1
        else:
            without_candidates += 1

        row_buffers.append(
            {
                "channel": {
                    "id": int(channel.id),
                    "number": channel.number,
                    "name": channel.name,
                    "current_guide": (
                        {
                            "epg_id": channel.guide_id,
                            "epg_name": channel.guide_name,
                            "channel_id": channel.guide_channel_id,
                        }
                        if channel.guide_id and channel.guide_channel_id
                        else None
                    ),
                },
                "default_selected": default_selected,
                "candidates": candidates,
            }
        )

    candidate_epg_channel_row_ids = sorted(
        {
            int(candidate["epg_channel_row_id"])
            for row in row_buffers
            for candidate in (row.get("candidates") or [])
            if candidate.get("epg_channel_row_id") is not None
        }
    )
    programme_counts_map = {}
    if candidate_epg_channel_row_ids:
        async with Session() as session:
            counts_result = await session.execute(
                select(
                    EpgChannelProgrammes.epg_channel_id.label("epg_channel_id"),
                    func.count(EpgChannelProgrammes.id).label("total_programmes"),
                )
                .where(EpgChannelProgrammes.epg_channel_id.in_(candidate_epg_channel_row_ids))
                .group_by(EpgChannelProgrammes.epg_channel_id)
            )
            for row in counts_result.all():
                programme_counts_map[int(row.epg_channel_id)] = int(row.total_programmes or 0)

    rows = []
    for row in row_buffers:
        rows.append(
            {
                **row,
                "candidates": _dedupe_and_rank_candidates(
                    row.get("candidates") or [],
                    max_candidates_per_channel,
                    programme_counts_map=programme_counts_map,
                ),
            }
        )

    return {
        "rows": rows,
        "summary": {
            "channels_considered": len(rows),
            "with_candidates": with_candidates,
            "without_candidates": without_candidates,
        },
    }


async def read_epg_match_candidate_preview(*, epg_channel_row_id, now_ts=None):
    now_ts = _safe_int(now_ts, int(time.time()))
    start_ts_expr = cast(func.nullif(EpgChannelProgrammes.start_timestamp, ""), BigInteger)
    stop_ts_expr = cast(func.nullif(EpgChannelProgrammes.stop_timestamp, ""), BigInteger)

    async with Session() as session:
        channel_result = await session.execute(
            select(
                EpgChannels.id.label("epg_channel_row_id"),
                EpgChannels.epg_id.label("epg_id"),
                EpgChannels.channel_id.label("channel_id"),
                EpgChannels.name.label("name"),
                EpgChannels.icon_url.label("icon_url"),
                Epg.name.label("epg_name"),
            )
            .join(Epg, Epg.id == EpgChannels.epg_id)
            .where(EpgChannels.id == epg_channel_row_id)
        )
        channel_row = channel_result.first()
        if not channel_row:
            return None

        stats_result = await session.execute(
            select(
                func.count(EpgChannelProgrammes.id).label("total_programmes"),
                func.count(EpgChannelProgrammes.id).filter(stop_ts_expr >= now_ts).label("future_programmes"),
                func.max(stop_ts_expr).label("max_stop_ts"),
            ).where(EpgChannelProgrammes.epg_channel_id == epg_channel_row_id)
        )
        stats = stats_result.first()

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
                EpgChannelProgrammes.epg_channel_id == epg_channel_row_id,
                stop_ts_expr >= now_ts,
            )
            .subquery()
        )
        upcoming_result = await session.execute(
            select(
                ranked_upcoming_subquery.c.title,
                ranked_upcoming_subquery.c.start_ts,
                ranked_upcoming_subquery.c.stop_ts,
                ranked_upcoming_subquery.c.row_num,
            )
            .where(ranked_upcoming_subquery.c.row_num <= 4)
            .order_by(ranked_upcoming_subquery.c.start_ts.asc())
        )
        upcoming_rows = upcoming_result.all()

    now_programme = None
    next_programmes = []
    for row in upcoming_rows:
        start_ts = int(row.start_ts) if row.start_ts is not None else None
        stop_ts = int(row.stop_ts) if row.stop_ts is not None else None
        programme = {
            "title": row.title or "(Untitled)",
            "start_ts": start_ts,
            "stop_ts": stop_ts,
        }
        if now_programme is None and start_ts is not None and stop_ts is not None and start_ts <= now_ts < stop_ts:
            now_programme = programme
            continue
        if len(next_programmes) < 3:
            next_programmes.append(programme)

    total_programmes = _safe_int(getattr(stats, "total_programmes", 0), 0)
    future_programmes = _safe_int(getattr(stats, "future_programmes", 0), 0)
    max_stop_ts = getattr(stats, "max_stop_ts", None)
    max_stop_ts = int(max_stop_ts) if max_stop_ts is not None else None
    horizon_hours = None
    if max_stop_ts and max_stop_ts >= now_ts:
        horizon_hours = round((max_stop_ts - now_ts) / 3600, 1)

    return {
        "epg_channel_row_id": int(channel_row.epg_channel_row_id),
        "epg_id": int(channel_row.epg_id),
        "epg_name": channel_row.epg_name,
        "channel_id": channel_row.channel_id,
        "name": channel_row.name,
        "icon_url": channel_row.icon_url,
        "total_programmes": total_programmes,
        "programmes_now_to_future": future_programmes,
        "future_horizon_hours": horizon_hours,
        "now_programme": now_programme,
        "next_programmes": next_programmes,
    }


async def apply_bulk_epg_matches(*, updates):
    normalized_updates = []
    for row in updates or []:
        channel_id = row.get("channel_id")
        epg_id = row.get("epg_id")
        epg_channel_id = (row.get("epg_channel_id") or "").strip()
        try:
            normalized_updates.append(
                {
                    "channel_id": normalize_id(channel_id, "channel"),
                    "epg_id": normalize_id(epg_id, "epg"),
                    "epg_channel_id": epg_channel_id,
                }
            )
        except ValueError:
            continue
    if not normalized_updates:
        return {"results": [], "summary": {"updated": 0, "skipped": 0, "failed": 0}}

    channel_ids = sorted({row["channel_id"] for row in normalized_updates})
    epg_keys = sorted({(row["epg_id"], row["epg_channel_id"]) for row in normalized_updates if row["epg_channel_id"]})

    async with Session() as session:
        results = []
        updated = 0
        skipped = 0
        failed = 0

        async with session.begin():
            channels_result = await session.execute(select(Channel).where(Channel.id.in_(channel_ids)))
            channels_map = {int(channel.id): channel for channel in channels_result.scalars().all()}

            epg_lookup = {}
            if epg_keys:
                epg_rows = await session.execute(
                    select(
                        EpgChannels.epg_id.label("epg_id"),
                        EpgChannels.channel_id.label("channel_id"),
                        Epg.name.label("epg_name"),
                    )
                    .join(Epg, Epg.id == EpgChannels.epg_id)
                    .where(tuple_(EpgChannels.epg_id, EpgChannels.channel_id).in_(epg_keys))
                )
                for row in epg_rows.all():
                    epg_lookup[(int(row.epg_id), str(row.channel_id))] = {"epg_name": row.epg_name}

            for row in normalized_updates:
                channel = channels_map.get(row["channel_id"])
                if not channel:
                    failed += 1
                    results.append(
                        {
                            "channel_id": row["channel_id"],
                            "status": "failed",
                            "reason": "channel_not_found",
                        }
                    )
                    continue

                mapping = epg_lookup.get((row["epg_id"], row["epg_channel_id"]))
                if not mapping:
                    failed += 1
                    results.append(
                        {
                            "channel_id": row["channel_id"],
                            "status": "failed",
                            "reason": "epg_mapping_not_found",
                        }
                    )
                    continue

                unchanged = (
                    _safe_int(channel.guide_id, 0) == row["epg_id"]
                    and (channel.guide_channel_id or "") == row["epg_channel_id"]
                )
                if unchanged:
                    skipped += 1
                    results.append(
                        {
                            "channel_id": row["channel_id"],
                            "status": "skipped",
                            "reason": "unchanged",
                        }
                    )
                    continue

                channel.guide_id = row["epg_id"]
                channel.guide_channel_id = row["epg_channel_id"]
                channel.guide_name = mapping.get("epg_name")
                updated += 1
                results.append(
                    {
                        "channel_id": row["channel_id"],
                        "status": "updated",
                    }
                )

    return {
        "results": results,
        "summary": {"updated": updated, "skipped": skipped, "failed": failed},
    }


async def apply_bulk_cso_settings(channel_ids, cso_enabled, cso_profile=None):
    normalized_ids = []
    for channel_id in channel_ids or []:
        try:
            normalized_ids.append(normalize_id(channel_id, "channel"))
        except ValueError:
            continue
    normalized_ids = sorted(set(normalized_ids))
    if not normalized_ids:
        return {"updated": 0}

    requested_profile = str(cso_profile or "").strip().lower()
    serialized_policy = json.dumps({"profile": requested_profile or "default"}, sort_keys=True)
    async with Session() as session:
        async with session.begin():
            channels_result = await session.execute(select(Channel).where(Channel.id.in_(normalized_ids)))
            channels = channels_result.scalars().all()
            for channel in channels:
                channel.cso_enabled = bool(cso_enabled)
                channel.cso_policy = serialized_policy

    return {"updated": len(channels)}


async def read_config_all_channels(
    filter_playlist_ids=None,
    output_for_export=False,
    include_status=False,
    include_manual_sources_when_filtered=False,
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
                    if filter_playlist_ids and source.playlist_id not in filter_playlist_ids:
                        if not (include_manual_sources_when_filtered and source.playlist_id is None):
                            continue
                    playlist_name = source.playlist.name if source.playlist else "Manual URL"
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
                        "xc_account_id": source.xc_account_id,
                    }
                    if include_status:
                        source_payload["tvh_uuid"] = source.tvh_uuid
                        source_payload["playlist_enabled"] = (
                            bool(source.playlist.enabled) if source.playlist_id else True
                        )
                    sources.append(source_payload)
                # Filter out this channel if we have provided a playlist ID filter list and no sources were found
                if filter_playlist_ids and not sources:
                    continue

                if output_for_export:
                    cso_profile = profile_from_cso_policy(getattr(result, "cso_policy", None))
                    return_list.append(
                        {
                            "enabled": result.enabled,
                            "name": result.name,
                            "logo_url": result.logo_url,
                            "number": result.number,
                            "cso_enabled": bool(getattr(result, "cso_enabled", False)),
                            "cso_policy": {"profile": cso_profile},
                            "cso_profile": cso_profile,
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
                    "cso_enabled": bool(getattr(result, "cso_enabled", False)),
                    "cso_policy": {"profile": profile_from_cso_policy(getattr(result, "cso_policy", None))},
                    "cso_profile": profile_from_cso_policy(getattr(result, "cso_policy", None)),
                    "cso_capabilities": cso_runtime_capabilities(),
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


async def read_config_one_channel(channel_id):
    return_item = {}
    channel_id = normalize_id(channel_id, "channel")
    async with Session() as session:
        result_query = await session.execute(
            select(Channel)
            .options(
                joinedload(Channel.tags),
                joinedload(Channel.sources).subqueryload(ChannelSource.playlist),
            )
            .where(Channel.id == channel_id)
            .order_by(Channel.id)
        )
        result = result_query.scalars().unique().one()
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
                    "playlist_user_agent": source.playlist.user_agent if source.playlist else None,
                    "priority": source.priority,
                    "stream_name": source.playlist_stream_name,
                    "stream_url": source.playlist_stream_url,
                    "use_hls_proxy": bool(getattr(source, "use_hls_proxy", False)),
                    "source_type": source_type,
                    "xc_account_id": source.xc_account_id,
                }
            )
        return_item = {
            "id": result.id,
            "enabled": result.enabled,
            "name": result.name,
            "logo_url": result.logo_url,
            "number": result.number,
            "cso_enabled": bool(getattr(result, "cso_enabled", False)),
            "cso_policy": {"profile": profile_from_cso_policy(getattr(result, "cso_policy", None))},
            "cso_profile": profile_from_cso_policy(getattr(result, "cso_policy", None)),
            "cso_capabilities": cso_runtime_capabilities(),
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
    return os.path.join(config.config_path, "cache", "logos", f"channel_logo_{channel_id}")


async def download_image_to_base64(image_source, timeout=10):
    # Image source is a URL
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
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
    base64_string_with_header, _, _ = await parse_image_as_base64_with_status(image_source)
    return base64_string_with_header


async def parse_image_as_base64_with_status(image_source):
    try:
        if not image_source:
            mime_type = "image/png"
            image_base64_string = image_placeholder_base64
            return f"data:{mime_type};base64,{image_base64_string}", "empty", None
        if image_source.startswith("data:image/"):
            mime_type = image_source.split(";")[0].split(":")[1]
            image_base64_string = image_source.split("base64,")[1]
            status = "ok"
            error = None
        elif image_source.startswith("http://") or image_source.startswith("https://"):
            image_base64_string, mime_type = await download_image_to_base64(image_source, timeout=3)
            # download_image_to_base64 falls back to placeholder on failure; detect that.
            if image_base64_string == image_placeholder_base64:
                status = "error"
                error = f"Failed to fetch logo from URL: {image_source}"
            else:
                status = "ok"
                error = None
        else:
            # Handle other cases or raise an error
            raise ValueError("Unsupported image source format")
    except Exception as e:
        logger.error("An error occurred while updating channel image: %s", e)
        # Return the placeholder image
        mime_type = "image/png"
        image_base64_string = image_placeholder_base64
        status = "error"
        error = str(e)

    # Prepend the MIME type and base64 header and return
    base64_string_with_header = f"data:{mime_type};base64,{image_base64_string}"
    return base64_string_with_header, status, error


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
    async with Session() as session:
        query = await session.execute(select(Channel).where(Channel.id == channel_id))
        channel = query.scalar_one()
    base64_string = channel.logo_base64
    if not base64_string:
        # Never force clients to fetch internet logos at request time.
        # If cache is missing, return placeholder and let background sync refresh cache.
        base64_string = f"data:image/png;base64,{image_placeholder_base64}"
    image_base64_string, mime_type = await read_base46_image_string(base64_string)
    if image_base64_string is None:
        image_base64_string = base64.b64decode(image_placeholder_base64)
        mime_type = "image/png"
    return image_base64_string, mime_type


async def add_new_channel(config, data, commit=True, publish=True):
    """Create Channel row and optionally publish immediately to TVHeadend.

    Returns the Channel ORM object (with tvh_uuid if published).
    """
    settings = config.read_settings()
    instance_id = config.ensure_instance_id()
    app_url = settings["settings"].get("app_url") or LOCAL_PROXY_HOST_PLACEHOLDER
    async with Session() as session:
        cso_enabled, cso_policy = _extract_cso_payload(data)
        channel = Channel(
            enabled=data.get("enabled"),
            name=data.get("name"),
            logo_url=data.get("logo_url"),
            number=data.get("number"),
            cso_enabled=cso_enabled,
            cso_policy=cso_policy,
        )

        for tag_name in data.get("tags", []):
            query = await session.execute(select(ChannelTag).where(ChannelTag.name == tag_name))
            channel_tag = query.scalar_one_or_none()
            if not channel_tag:
                channel_tag = ChannelTag(name=tag_name)
                session.add(channel_tag)
            channel.tags.append(channel_tag)

        guide_info = data.get("guide", {})
        if guide_info.get("epg_id"):
            query = await session.execute(select(Epg).where(Epg.id == guide_info["epg_id"]))
            channel_guide_source = query.scalar_one()
            channel.guide_id = channel_guide_source.id
            channel.guide_name = channel_guide_source.name
            channel.guide_channel_id = guide_info["channel_id"]

        new_sources = []
        playlist_stream_cache = {}
        for source_info in data.get("sources", []):
            is_manual = source_info.get("source_type") == "manual" or not source_info.get("playlist_id")
            if is_manual:
                stream_url = (source_info.get("stream_url") or "").strip()
                if not stream_url:
                    continue
                new_sources.append(
                    ChannelSource(
                        playlist_id=None,
                        playlist_stream_name=source_info.get("stream_name") or "Manual URL",
                        playlist_stream_url=stream_url,
                        use_hls_proxy=bool(source_info.get("use_hls_proxy", False)),
                    )
                )
                continue

            query = await session.execute(select(Playlist).where(Playlist.id == source_info["playlist_id"]))
            playlist_info = query.scalar_one()
            if playlist_info.id not in playlist_stream_cache:
                streams_query = await session.execute(
                    select(PlaylistStreams).where(PlaylistStreams.playlist_id == playlist_info.id)
                )
                playlist_stream_cache[playlist_info.id] = {
                    row.name: {
                        "url": row.url,
                        "xc_stream_id": row.xc_stream_id,
                    }
                    for row in streams_query.scalars().all()
                }
            playlist_stream = playlist_stream_cache[playlist_info.id].get(source_info["stream_name"])
            if not playlist_stream:
                continue

            if playlist_info.account_type == XC_ACCOUNT_TYPE:
                accounts = await _get_enabled_xc_accounts_async(playlist_info.id)
                for account in accounts:
                    template = playlist_stream.get("url")
                    if playlist_stream.get("xc_stream_id"):
                        stream_url = _build_xc_live_stream_url(
                            _normalize_xc_host(playlist_info.url),
                            playlist_stream["xc_stream_id"],
                            playlist_stream.get("url"),
                            account,
                            preferred_extension=playlist_info.xc_live_stream_format,
                        )
                    else:
                        stream_url = template
                    stream_url = _apply_playlist_hls_proxy(
                        playlist_info,
                        stream_url,
                        instance_id,
                    )
                    new_sources.append(
                        ChannelSource(
                            playlist_id=playlist_info.id,
                            xc_account_id=account.id,
                            playlist_stream_name=source_info["stream_name"],
                            playlist_stream_url=stream_url,
                        )
                    )
            else:
                stream_url = _apply_playlist_hls_proxy(
                    playlist_info,
                    playlist_stream["url"],
                    instance_id,
                )
                new_sources.append(
                    ChannelSource(
                        playlist_id=playlist_info.id,
                        playlist_stream_name=source_info["stream_name"],
                        playlist_stream_url=stream_url,
                    )
                )

        if new_sources:
            channel.sources = new_sources

        session.add(channel)
        if publish:
            await session.flush()

        if publish:
            try:
                async with await get_tvh(config) as tvh:
                    logo_proxy_url = build_channel_logo_proxy_url(
                        channel.id if channel.id else "new",
                        app_url,
                        channel.logo_url or "",
                    )
                    channel_uuid = await publish_channel_to_tvh(
                        tvh,
                        channel,
                        icon_url=logo_proxy_url,
                    )
                    channel.tvh_uuid = channel_uuid
            except Exception as e:
                logger.error("Immediate publish failed for channel '%s': %s", channel.name, e)

        if commit:
            await session.commit()
        else:
            await session.flush()
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
            cso_enabled, cso_policy = _extract_cso_payload(
                data,
                current_enabled=bool(getattr(channel, "cso_enabled", False)),
                current_policy=getattr(channel, "cso_policy", None),
            )
            channel.cso_enabled = cso_enabled
            channel.cso_policy = cso_policy
            # Channels API returns a rendered backend proxy URL in `logo_url` for UI display.
            # Prefer `source_logo_url` when present so bulk UI saves do not overwrite the
            # persisted source logo with a cache/proxy URL.
            logo_url = data.get("source_logo_url")
            if logo_url is None:
                logo_url = data.get("logo_url")
            channel.logo_url = logo_url
            number_value = data.get("number")
            if number_value in ("", None):
                channel.number = None
            else:
                try:
                    channel.number = int(number_value)
                except (TypeError, ValueError):
                    channel.number = None

            # Category Tags
            # -- Remove existing tags
            channel.tags.clear()
            # -- Add tags
            new_tags = []
            for tag_name in data.get("tags", []):
                query = await session.execute(select(ChannelTag).filter(ChannelTag.name == tag_name))
                channel_tag = query.scalar_one_or_none()
                if not channel_tag:
                    channel_tag = ChannelTag(name=tag_name)
                    session.add(channel_tag)
                new_tags.append(channel_tag)
            channel.tags = new_tags

            # Programme Guide
            guide_info = data.get("guide", {})
            if guide_info.get("epg_id"):
                query = await session.execute(select(Epg).filter(Epg.id == guide_info["epg_id"]))
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
            seen_xc_streams = set()
            playlist_stream_cache = {}

            def _playlist_stream_from_model(row):
                return {
                    "name": row.name,
                    "url": row.url,
                    "channel_id": row.channel_id,
                    "group_title": row.group_title,
                    "tvg_chno": row.tvg_chno,
                    "tvg_id": row.tvg_id,
                    "tvg_logo": row.tvg_logo,
                    "source_type": row.source_type,
                    "xc_stream_id": row.xc_stream_id,
                }

            async def _get_playlist_stream(playlist_info, source_info):
                stream_id = source_info.get("stream_id")
                stream_url = source_info.get("stream_url")
                stream_name = source_info.get("stream_name")
                if stream_id:
                    query = await session.execute(
                        select(PlaylistStreams).where(
                            PlaylistStreams.playlist_id == playlist_info.id,
                            PlaylistStreams.id == stream_id,
                        )
                    )
                    row = query.scalar_one_or_none()
                    if row:
                        return _playlist_stream_from_model(row)
                if stream_url:
                    query = await session.execute(
                        select(PlaylistStreams).where(
                            PlaylistStreams.playlist_id == playlist_info.id,
                            PlaylistStreams.url == stream_url,
                        )
                    )
                    row = query.scalar_one_or_none()
                    if row:
                        return _playlist_stream_from_model(row)
                if playlist_info.id not in playlist_stream_cache:
                    query = await session.execute(
                        select(PlaylistStreams).where(PlaylistStreams.playlist_id == playlist_info.id)
                    )
                    playlist_stream_cache[playlist_info.id] = {
                        row.name: _playlist_stream_from_model(row) for row in query.scalars().all()
                    }
                return playlist_stream_cache[playlist_info.id].get(stream_name)

            for source_info in data.get("sources", []):
                source_id = source_info.get("id")
                is_manual = source_info.get("source_type") == "manual" or not source_info.get("playlist_id")
                channel_source = None
                if source_id:
                    query = await session.execute(select(ChannelSource).filter(ChannelSource.id == source_id))
                    channel_source = query.scalar_one_or_none()
                if not channel_source and not is_manual:
                    stream_url = source_info.get("stream_url")
                    query = await session.execute(
                        select(ChannelSource).filter(
                            and_(
                                ChannelSource.channel_id == channel.id,
                                ChannelSource.playlist_id == source_info["playlist_id"],
                                (
                                    ChannelSource.playlist_stream_url == stream_url
                                    if stream_url
                                    else ChannelSource.playlist_stream_name == source_info["stream_name"]
                                ),
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
                                ChannelSource.playlist_stream_url == source_info.get("stream_url"),
                            )
                        )
                    )
                    channel_source = query.scalar_one_or_none()

                playlist_info = None
                playlist_stream = None
                if not is_manual:
                    query = await session.execute(select(Playlist).filter(Playlist.id == source_info["playlist_id"]))
                    playlist_info = query.scalar_one()
                    playlist_stream = await _get_playlist_stream(playlist_info, source_info)
                    if not playlist_stream:
                        logger.warning(
                            "Missing playlist stream for channel %s source playlist=%s stream='%s'; skipping source",
                            channel.name,
                            source_info.get("playlist_id"),
                            source_info.get("stream_name"),
                        )
                        continue
                    if playlist_info.account_type == XC_ACCOUNT_TYPE and not source_info.get("xc_account_id"):
                        stream_key = (
                            playlist_info.id,
                            source_info.get("stream_id") or source_info.get("stream_url") or source_info["stream_name"],
                        )
                        if stream_key in seen_xc_streams:
                            continue
                        seen_xc_streams.add(stream_key)
                        accounts = await _get_enabled_xc_accounts_async(playlist_info.id)
                        for account in accounts:
                            template = playlist_stream.get("url")
                            if playlist_stream.get("xc_stream_id"):
                                stream_url = _build_xc_live_stream_url(
                                    _normalize_xc_host(playlist_info.url),
                                    playlist_stream["xc_stream_id"],
                                    playlist_stream.get("url"),
                                    account,
                                    preferred_extension=playlist_info.xc_live_stream_format,
                                )
                            else:
                                stream_url = template
                            stream_url = _apply_playlist_hls_proxy(
                                playlist_info,
                                stream_url,
                                instance_id,
                            )
                            query = await session.execute(
                                select(ChannelSource).filter(
                                    and_(
                                        ChannelSource.channel_id == channel.id,
                                        ChannelSource.playlist_id == playlist_info.id,
                                        ChannelSource.playlist_stream_url == stream_url,
                                        ChannelSource.xc_account_id == account.id,
                                    )
                                )
                            )
                            account_source = query.scalar_one_or_none()
                            if not account_source:
                                account_source = ChannelSource(
                                    playlist_id=playlist_info.id,
                                    xc_account_id=account.id,
                                )
                            account_source.playlist_stream_name = source_info["stream_name"]
                            account_source.playlist_stream_url = stream_url
                            if account_source.id:
                                new_source_ids.append(account_source.id)
                            account_source.priority = str(priority)
                            priority -= 1
                            new_sources.append(account_source)
                        continue

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
                            playlist_stream_name=source_info.get("stream_name") or "Manual URL",
                            playlist_stream_url=stream_url,
                            use_hls_proxy=bool(source_info.get("use_hls_proxy", False)),
                        )
                    else:
                        logger.info(
                            "    - Updating manual channel source for channel %s",
                            channel.name,
                        )
                        channel_source.playlist_stream_name = source_info.get("stream_name") or "Manual URL"
                        channel_source.playlist_stream_url = stream_url
                        channel_source.use_hls_proxy = bool(source_info.get("use_hls_proxy", False))
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
                        if not playlist_info:
                            query = await session.execute(
                                select(Playlist).filter(Playlist.id == source_info["playlist_id"])
                            )
                            playlist_info = query.scalar_one()
                            playlist_stream = await _get_playlist_stream(playlist_info, source_info)
                        if playlist_info.account_type == XC_ACCOUNT_TYPE and source_info.get("xc_account_id"):
                            query = await session.execute(
                                select(XcAccount).where(XcAccount.id == source_info["xc_account_id"])
                            )
                            account = query.scalar_one_or_none()
                            if account:
                                template = playlist_stream.get("url")
                                if playlist_stream.get("xc_stream_id"):
                                    playlist_stream["url"] = _build_xc_live_stream_url(
                                        _normalize_xc_host(playlist_info.url),
                                        playlist_stream["xc_stream_id"],
                                        playlist_stream.get("url"),
                                        account,
                                        preferred_extension=playlist_info.xc_live_stream_format,
                                    )
                                else:
                                    playlist_stream["url"] = template
                        playlist_stream["url"] = _apply_playlist_hls_proxy(
                            playlist_info,
                            playlist_stream["url"],
                            instance_id,
                        )
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
                                refresh_source_info["playlist_id"] == source_info["playlist_id"]
                                and refresh_source_info["stream_name"] == source_info["stream_name"]
                            ):
                                logger.info(
                                    "    - Channel %s source marked for refresh 'Playlist:%s - %s'",
                                    channel.name,
                                    source_info["playlist_id"],
                                    source_info["stream_name"],
                                )
                                if not playlist_info:
                                    query = await session.execute(
                                        select(Playlist).filter(Playlist.id == source_info["playlist_id"])
                                    )
                                    playlist_info = query.scalar_one()
                                    playlist_stream = await _get_playlist_stream(playlist_info, source_info)
                                if not playlist_stream:
                                    logger.warning(
                                        "    - Missing playlist stream '%s' for playlist %s; leaving URL unchanged",
                                        source_info["stream_name"],
                                        source_info["playlist_id"],
                                    )
                                    continue
                                if playlist_info.account_type == XC_ACCOUNT_TYPE and source_info.get("xc_account_id"):
                                    query = await session.execute(
                                        select(XcAccount).where(XcAccount.id == source_info["xc_account_id"])
                                    )
                                    account = query.scalar_one_or_none()
                                    if account:
                                        template = playlist_stream.get("url")
                                        if playlist_stream.get("xc_stream_id"):
                                            playlist_stream["url"] = _build_xc_live_stream_url(
                                                _normalize_xc_host(playlist_info.url),
                                                playlist_stream["xc_stream_id"],
                                                playlist_stream.get("url"),
                                                account,
                                                preferred_extension=playlist_info.xc_live_stream_format,
                                            )
                                        else:
                                            playlist_stream["url"] = template
                                playlist_stream["url"] = _apply_playlist_hls_proxy(
                                    playlist_info,
                                    playlist_stream["url"],
                                    instance_id,
                                )
                                # Update playlist stream url
                                logger.info(
                                    "    - Updating channel %s source from '%s' to '%s'",
                                    channel.name,
                                    channel_source.playlist_stream_url,
                                    playlist_stream["url"],
                                )
                                channel_source.playlist_stream_url = playlist_stream["url"]
                                break
                # Update source priority (higher means higher priority)
                channel_source.priority = str(priority)
                priority -= 1
                # Append to list of new sources
                new_sources.append(channel_source)
            # Remove all old entries in the channel_sources table
            query = await session.execute(select(ChannelSource).filter_by(channel_id=channel.id))
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

            # Remove any suggestions that reference streams already added to this channel
            stream_name_pairs = set()
            stream_url_pairs = set()
            for source_info in data.get("sources", []):
                playlist_id = source_info.get("playlist_id")
                stream_name = source_info.get("stream_name")
                stream_url = source_info.get("stream_url")
                if playlist_id and stream_url:
                    stream_url_pairs.add((int(playlist_id), stream_url))
                elif playlist_id and stream_name:
                    stream_name_pairs.add((int(playlist_id), stream_name))
            if stream_url_pairs or stream_name_pairs:
                playlist_ids = {pair[0] for pair in stream_url_pairs | stream_name_pairs}
                stream_names = {pair[1] for pair in stream_name_pairs}
                stream_urls = {pair[1] for pair in stream_url_pairs}
                match_clauses = []
                if stream_names:
                    match_clauses.append(PlaylistStreams.name.in_(stream_names))
                if stream_urls:
                    match_clauses.append(PlaylistStreams.url.in_(stream_urls))
                result = await session.execute(
                    select(
                        PlaylistStreams.id,
                        PlaylistStreams.playlist_id,
                        PlaylistStreams.name,
                        PlaylistStreams.url,
                    ).where(
                        PlaylistStreams.playlist_id.in_(playlist_ids),
                        or_(*match_clauses),
                    )
                )
                stream_ids = [
                    row.id
                    for row in result
                    if (
                        (row.url and (row.playlist_id, row.url) in stream_url_pairs)
                        or (not row.url and (row.playlist_id, row.name) in stream_name_pairs)
                        or (row.playlist_id, row.name) in stream_name_pairs
                    )
                ]
                if stream_ids:
                    await session.execute(
                        delete(ChannelSuggestion)
                        .where(ChannelSuggestion.channel_id == channel.id)
                        .where(ChannelSuggestion.stream_id.in_(stream_ids))
                    )

            # Commit
            await session.commit()


async def add_bulk_channels(config, data):
    async with Session() as session:
        channel_number = await session.scalar(select(func.max(Channel.number)))
    if channel_number is None:
        channel_number = 999

    added_channel_count = 0
    skipped_channel_count = 0

    new_channels = []
    for channel in data:
        # Fetch the playlist channel by ID
        async with Session() as session:
            stream_query = await session.execute(
                select(PlaylistStreams)
                .options(joinedload(PlaylistStreams.playlist))
                .where(PlaylistStreams.id == channel["stream_id"])
            )
            playlist_stream = stream_query.scalar_one()

        # Check if Channel with this name already exists
        async with Session() as session:
            existing_query = await session.execute(select(Channel).where(Channel.name == playlist_stream.name.strip()))
            existing_channel = existing_query.scalars().first()

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
        async with Session() as session:
            epg_query = await session.execute(
                select(EpgChannels).where(EpgChannels.channel_id == playlist_stream.tvg_id)
            )
            epg_match = epg_query.scalars().first()
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
        channel_obj = await add_new_channel(config, new_channel_data, commit=True, publish=False)
        new_channels.append(channel_obj)
        added_channel_count += 1

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
            result = await session.execute(select(Channel).where(Channel.id == channel_id))
            channel = result.scalar_one_or_none()
            if channel is None:
                logger.warning("delete_channel: channel id %s not found", channel_id)
                return False

            # Delete recordings and rules tied to this channel to avoid FK violations
            await session.execute(delete(Recording).where(Recording.channel_id == channel.id))
            await session.execute(delete(RecordingRule).where(RecordingRule.channel_id == channel.id))

            # Remove channel from TVHeadend if it has a UUID
            if channel.tvh_uuid:
                async with await get_tvh(config) as tvh:
                    logger.info(f"Removing channel '{channel.name}' (UUID: {channel.tvh_uuid}) from TVHeadend")
                    try:
                        await tvh.delete_channel(channel.tvh_uuid)
                    except Exception as exc:
                        logger.warning(
                            "TVH delete channel failed for uuid %s: %s",
                            channel.tvh_uuid,
                            exc,
                        )

            # Remove all source entries in the channel_sources table
            result = await session.execute(select(ChannelSource).filter_by(channel_id=channel.id))
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


async def build_m3u_lines_for_channel(tic_base_url, channel_uuid, channel, logo_url=None):
    playlist = []
    tvg_logo = logo_url if logo_url is not None else channel.logo_url
    line = f'#EXTINF:-1 tvg-name="{channel.name}" tvg-logo="{tvg_logo}" tvg-id="{channel_uuid}" tvg-chno="{channel.number}"'
    if channel.tags:
        line += f' group-title="{channel.tags[0]}"'
    line += f",{channel.name}"
    playlist.append(line)
    playlist.append(f"{tic_base_url}/tic-tvh/stream/channel/{channel_uuid}?profile=pass")
    return playlist


async def publish_channel_to_tvh(
    tvh,
    channel,
    icon_url=None,
    existing_channels_by_uuid=None,
    existing_channels_by_name=None,
    existing_tag_details=None,
    api_calls=None,
):
    logger.info("Publishing channel to TVH - %s.", channel.name)
    if api_calls is None:
        api_calls = {}

    def _count(name):
        api_calls[name] = api_calls.get(name, 0) + 1

    # Fallback path for single-channel publish callers.
    if existing_channels_by_uuid is None or existing_channels_by_name is None:
        _count("list_all_channels")
        existing_channels = await tvh.list_all_channels()
        existing_channels_by_uuid = {c.get("uuid"): c for c in existing_channels if c.get("uuid")}
        existing_channels_by_name = {c.get("name"): c for c in existing_channels if c.get("name")}
    if existing_tag_details is None:
        _count("list_all_managed_channel_tags")
        existing_tag_details = {
            tag.get("name"): tag.get("uuid")
            for tag in await tvh.list_all_managed_channel_tags()
            if tag.get("name") and tag.get("uuid")
        }

    # Check if channel exists with a matching UUID and create it if not
    channel_uuid = channel.tvh_uuid
    if channel_uuid:
        if channel_uuid not in existing_channels_by_uuid:
            channel_uuid = None
    if not channel_uuid:
        by_name = existing_channels_by_name.get(channel.name)
        if by_name and by_name.get("uuid"):
            channel_uuid = by_name.get("uuid")
        else:
            # No channel exists, create one
            logger.info("   - Creating new channel in TVH")
            _count("create_channel")
            channel_uuid = await tvh.create_channel(
                channel.name, channel.number, (icon_url if icon_url is not None else channel.logo_url)
            )
            if channel_uuid:
                existing_channels_by_uuid[channel_uuid] = {"uuid": channel_uuid, "name": channel.name}
                existing_channels_by_name[channel.name] = {"uuid": channel_uuid, "name": channel.name}
    else:
        logger.info("   - Found existing channel in TVH")
    channel_conf = {
        "enabled": bool(channel.enabled),
        "uuid": channel_uuid,
        "name": channel.name,
        "number": channel.number,
        "icon": icon_url if icon_url is not None else channel.logo_url,
    }
    # Check for existing channel tags
    # Create channel tags in TVH if missing
    channel_tag_uuids = []
    for tag in channel.tags:
        tag_uuid = existing_tag_details.get(tag.name)
        if not tag_uuid:
            # Create channel tag
            logger.info("Creating new channel tag '%s'", tag.name)
            _count("create_channel_tag")
            tag_uuid = await tvh.create_channel_tag(tag.name)
            if tag_uuid:
                existing_tag_details[tag.name] = tag_uuid
        channel_tag_uuids.append(tag_uuid)
    # Apply channel tag UUIDs to chanel conf in TVH
    channel_conf["tags"] = channel_tag_uuids
    # Save channel info in TVH
    _count("idnode_save")
    await tvh.idnode_save(channel_conf)
    return channel_uuid


async def batch_publish_new_channels_to_tvh(config, channels):
    """Batch publish a list of newly created Channel objects to TVHeadend.

    Reduces API calls by fetching existing channels and tags once and only creating
    what is missing.
    """
    if not channels:
        return
    settings = config.read_settings()
    app_url = settings["settings"].get("app_url") or LOCAL_PROXY_HOST_PLACEHOLDER
    async with await get_tvh(config) as tvh:
        logger.info("Batch publishing %d new channels to TVH", len(channels))
        existing_channels = await tvh.list_all_channels()
        existing_by_name = {c.get("name"): c.get("uuid") for c in existing_channels if c.get("name")}

        # Collect tag names
        unique_tags = set()
        for ch in channels:
            for tag in getattr(ch, "tags", []) or []:
                tag_name = getattr(tag, "name", None) or (tag if isinstance(tag, str) else None)
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
            if not ch.enabled:
                continue
            if ch.tvh_uuid:
                continue
            existing_uuid = existing_by_name.get(ch.name)
            if not existing_uuid:
                try:
                    logo_proxy_url = build_channel_logo_proxy_url(
                        ch.id,
                        app_url,
                        ch.logo_url or "",
                    )
                    existing_uuid = await tvh.create_channel(ch.name, ch.number, logo_proxy_url)
                    existing_by_name[ch.name] = existing_uuid
                except Exception as e:
                    logger.error("Failed creating channel '%s': %s", ch.name, e)
                    continue
            tag_uuids = []
            for tag in getattr(ch, "tags", []) or []:
                tag_name = getattr(tag, "name", None) or (tag if isinstance(tag, str) else None)
                if tag_name and existing_tag_details.get(tag_name):
                    tag_uuids.append(existing_tag_details[tag_name])
            channel_conf = {
                "enabled": bool(ch.enabled),
                "uuid": existing_uuid,
                "name": ch.name,
                "number": ch.number,
                "icon": build_channel_logo_proxy_url(
                    ch.id,
                    app_url,
                    ch.logo_url or "",
                ),
                "tags": tag_uuids,
            }
            try:
                await tvh.idnode_save(channel_conf)
                ch.tvh_uuid = existing_uuid
            except Exception as e:
                logger.error("Failed saving channel '%s': %s", ch.name, e)
        async with Session() as session:
            async with session.begin():
                for ch in channels:
                    if not ch.id:
                        continue
                    channel_row = await session.get(Channel, ch.id)
                    if channel_row and ch.tvh_uuid:
                        channel_row.tvh_uuid = ch.tvh_uuid
        logger.info("Batch publish completed")


async def publish_bulk_channels_to_tvh_and_m3u(config, force=False, trigger="unknown"):
    total_start = time.perf_counter()
    phase_seconds = {}
    api_calls = {}
    logo_refresh_count = 0

    def _count(name):
        api_calls[name] = api_calls.get(name, 0) + 1

    settings = config.read_settings()
    tic_base_url = settings["settings"]["app_url"]
    sync_state_path = _channel_sync_state_path(config)
    logo_source_state_path = _logo_source_state_path(config)
    logo_health_state_path = _logo_health_state_path(config)

    t0 = time.perf_counter()
    managed_uuids = []
    async with Session() as session:
        query = await session.execute(
            select(Channel)
            .options(
                joinedload(Channel.tags),
                joinedload(Channel.sources).subqueryload(ChannelSource.playlist),
                joinedload(Channel.sources).subqueryload(ChannelSource.xc_account),
            )
            .order_by(Channel.id, Channel.number.asc())
        )
        results = query.scalars().unique().all()
    phase_seconds["load_channels"] = time.perf_counter() - t0

    current_signature = _build_channel_sync_signature(results, tic_base_url)
    previous_state = _read_json_file(sync_state_path, {})
    previous_signature = previous_state.get("signature")
    custom_playlist_file = os.path.join(config.config_path, "playlist.m3u8")
    playlist_exists = os.path.exists(custom_playlist_file)
    if not force and playlist_exists and previous_signature == current_signature:
        logger.info(
            "Skipping TVH channel sync (trigger=%s): no channel changes detected (%.2fs)",
            trigger,
            time.perf_counter() - total_start,
        )
        return

    logo_source_state_payload = _read_json_file(logo_source_state_path, {})
    if isinstance(logo_source_state_payload, dict):
        logo_source_state = logo_source_state_payload.get("sources", {})
    else:
        logo_source_state = {}
    logo_health_state_payload = _read_json_file(logo_health_state_path, {})
    if isinstance(logo_health_state_payload, dict):
        logo_health_state = logo_health_state_payload.get("channels", {})
    else:
        logo_health_state = {}
    t0 = time.perf_counter()
    async with await get_tvh(config) as tvh:
        # Prefetch TVH channels/tags once for this run.
        _count("list_all_channels")
        existing_channels = await tvh.list_all_channels()
        existing_channels_by_uuid = {c.get("uuid"): c for c in existing_channels if c.get("uuid")}
        existing_channels_by_name = {c.get("name"): c for c in existing_channels if c.get("name")}
        _count("list_all_managed_channel_tags")
        existing_tag_details = {
            tag.get("name"): tag.get("uuid")
            for tag in await tvh.list_all_managed_channel_tags()
            if tag.get("name") and tag.get("uuid")
        }
        phase_seconds["tvh_prefetch"] = time.perf_counter() - t0

        # Loop over configured channels
        logger.info(
            "Publishing all channels to TVH and M3U (trigger=%s, force=%s)",
            trigger,
            force,
        )
        playlist = [f'#EXTM3U url-tvg="{tic_base_url}/tic-api/epg/xmltv.xml"']
        pending_commit = False
        t_publish = time.perf_counter()
        for result in results:
            if not result.enabled:
                continue
            logo_proxy_url = build_channel_logo_proxy_url(
                result.id,
                tic_base_url or LOCAL_PROXY_HOST_PLACEHOLDER,
                result.logo_url or "",
            )
            channel_uuid = await publish_channel_to_tvh(
                tvh,
                result,
                icon_url=logo_proxy_url,
                existing_channels_by_uuid=existing_channels_by_uuid,
                existing_channels_by_name=existing_channels_by_name,
                existing_tag_details=existing_tag_details,
                api_calls=api_calls,
            )
            playlist += await build_m3u_lines_for_channel(tic_base_url, channel_uuid, result, logo_url=logo_proxy_url)
            result.tvh_uuid = channel_uuid
            logo_url = result.logo_url or ""
            last_logo_url = logo_source_state.get(str(result.id))
            logo_status = logo_health_state.get(str(result.id), {})
            if (not result.logo_base64) or (last_logo_url != logo_url):
                parsed_base64, parse_status, parse_error = await parse_image_as_base64_with_status(result.logo_url)
                result.logo_base64 = parsed_base64
                logo_source_state[str(result.id)] = logo_url
                logo_status = {
                    "status": parse_status,
                    "error": parse_error,
                    "source_logo_url": logo_url,
                    "updated_at": int(time.time()),
                }
                logo_health_state[str(result.id)] = logo_status
                logo_refresh_count += 1
            elif str(result.id) not in logo_health_state:
                # Backfill status for rows that already had cached logos before health tracking existed.
                cached_is_placeholder = isinstance(result.logo_base64, str) and result.logo_base64.endswith(
                    image_placeholder_base64
                )
                inferred_error = None
                inferred_status = "ok"
                if logo_url and (not result.logo_base64 or cached_is_placeholder):
                    inferred_status = "error"
                    inferred_error = "Logo cache contains placeholder due to previous fetch failure"
                logo_health_state[str(result.id)] = {
                    "status": inferred_status,
                    "error": inferred_error,
                    "source_logo_url": logo_url,
                    "updated_at": int(time.time()),
                }
            managed_uuids.append(channel_uuid)
            pending_commit = True
        phase_seconds["channel_publish_loop"] = time.perf_counter() - t_publish

        t_commit = time.perf_counter()
        if pending_commit:
            async with Session() as session:
                async with session.begin():
                    for result in results:
                        if not result.id:
                            continue
                        channel_row = await session.get(Channel, result.id)
                        if not channel_row:
                            continue
                        channel_row.tvh_uuid = result.tvh_uuid
                        channel_row.logo_base64 = result.logo_base64
        phase_seconds["db_commit"] = time.perf_counter() - t_commit

        # Write playlist file
        t_playlist = time.perf_counter()
        async with aiofiles.open(custom_playlist_file, "w", encoding="utf-8") as f:
            for item in playlist:
                await f.write(f"{item}\n")
        phase_seconds["write_playlist"] = time.perf_counter() - t_playlist

        #  Remove any channels that are not managed.
        t_cleanup = time.perf_counter()
        logger.info("Running cleanup task on current TVH channels")
        managed_uuid_set = set(managed_uuids)
        for existing_uuid in list(existing_channels_by_uuid.keys()):
            if existing_uuid not in managed_uuid_set:
                logger.info("    - Removing channel UUID - %s", existing_uuid)
                _count("delete_channel")
                await tvh.delete_channel(existing_uuid)
        phase_seconds["cleanup"] = time.perf_counter() - t_cleanup

    _write_json_file(
        sync_state_path,
        {
            "signature": current_signature,
            "updated_at": int(time.time()),
        },
    )
    _write_json_file(
        logo_source_state_path,
        {
            "sources": logo_source_state,
            "updated_at": int(time.time()),
        },
    )
    _write_json_file(
        logo_health_state_path,
        {
            "channels": logo_health_state,
            "updated_at": int(time.time()),
        },
    )

    execution_time = time.perf_counter() - total_start
    logger.info(
        "Configuring TVH channels finished in %.2fs (trigger=%s force=%s channels=%s logos_refreshed=%s phases=%s api_calls=%s)",
        execution_time,
        trigger,
        force,
        len(results),
        logo_refresh_count,
        {k: round(v, 2) for k, v in phase_seconds.items()},
        api_calls,
    )
    logger.info(
        "Configuring TVH channels took '%s' seconds",
        int(execution_time),
    )


async def publish_channel_muxes(config):
    tvh_stream_username, tvh_stream_key = await get_tvh_stream_auth(config)
    settings = config.read_settings()
    conn_settings = await config.tvh_connection_settings()
    if conn_settings.get("tvh_local"):
        tic_base_url = f"http://127.0.0.1:{flask_run_port}"
    else:
        tic_base_url = settings["settings"].get("app_url") or ""
    async with await get_tvh(config) as tvh:
        # Fetch results with relationships
        async with Session() as session:
            query = await session.execute(
                select(Channel)
                .options(
                    joinedload(Channel.tags),
                    joinedload(Channel.sources).subqueryload(ChannelSource.playlist),
                )
                .order_by(Channel.id, Channel.number.asc())
            )
            results = query.scalars().unique().all()
            account_query = await session.execute(select(XcAccount))
            xc_accounts = account_query.scalars().all()
        xc_by_playlist_and_username = {(account.playlist_id, account.username): account for account in xc_accounts}
        xc_by_id = {account.id: account for account in xc_accounts}

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
        cso_network_uuid = None
        instance_id = config.ensure_instance_id()

        async def ensure_cso_network():
            nonlocal cso_network_uuid
            if cso_network_uuid:
                return cso_network_uuid

            reserved_name = f"tic-system-cso-{instance_id[:8]}"
            networks = await tvh.list_cur_networks()
            for net in networks:
                if net.get("pnetworkname") == reserved_name or net.get("networkname") == reserved_name:
                    cso_network_uuid = net.get("uuid")
                    break
            if cso_network_uuid:
                return cso_network_uuid
            cso_network_uuid = await tvh.create_network(
                "tic-system-cso",
                reserved_name,
                999999,
                999,
            )
            return cso_network_uuid

        async def process_source(channel_obj, source_obj):
            async with sem:
                net_uuid = source_obj.playlist.tvh_uuid
                if source_obj.playlist and source_obj.playlist.account_type == XC_ACCOUNT_TYPE:
                    if source_obj.xc_account_id and xc_by_id.get(source_obj.xc_account_id):
                        net_uuid = xc_by_id[source_obj.xc_account_id].tvh_uuid
                    else:
                        try:
                            parsed = urlparse(source_obj.playlist_stream_url or "")
                            parts = parsed.path.split("/")
                            if "live" in parts:
                                idx = parts.index("live")
                                username = parts[idx + 1] if len(parts) > idx + 1 else None
                                if username:
                                    account = xc_by_playlist_and_username.get((source_obj.playlist_id, username))
                                    if account:
                                        net_uuid = account.tvh_uuid
                                        source_obj.xc_account_id = account.id
                        except Exception:
                            pass
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
                    logger.info("    - Creating new MUX for channel '%s'", channel_obj.name)
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

                service_name = f"{source_obj.playlist.name} - {source_obj.playlist_stream_name}"
                mux_name = service_name
                if source_obj.playlist_stream_url:
                    url_hash = hashlib.sha1(source_obj.playlist_stream_url.encode("utf-8")).hexdigest()[:8]
                    mux_name = f"{mux_name} [{url_hash}]"
                if not mux_name.lower().startswith("tic-"):
                    mux_name = f"tic-{mux_name}"
                stream_url = source_obj.playlist_stream_url
                if is_local_hls_proxy_url(stream_url, instance_id=instance_id):
                    stream_url = normalize_local_proxy_url(
                        stream_url,
                        base_url=tic_base_url,
                        instance_id=instance_id,
                        stream_key=tvh_stream_key,
                        username=tvh_stream_username,
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
                    "iptv_icon": build_channel_logo_proxy_url(
                        channel_obj.id,
                        tic_base_url or LOCAL_PROXY_HOST_PLACEHOLDER,
                        channel_obj.logo_url or "",
                    ),
                    "iptv_sname": channel_obj.name,
                    "iptv_muxname": mux_name,
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
                    logger.error("Failed saving MUX for channel '%s': %s", channel_obj.name, e)

        async def process_cso_channel(channel_obj):
            async with sem:
                net_uuid = await ensure_cso_network()
                if not net_uuid:
                    logger.error("Failed resolving CSO network for channel '%s'", channel_obj.name)
                    return

                mux_name = f"tic-cso-{channel_obj.id}-{re.sub(r'[^a-zA-Z0-9]', '', channel_obj.name)}"
                existing = next((m for m in existing_muxes if (m.get("iptv_muxname") or "") == mux_name), None)
                mux_uuid = existing.get("uuid") if existing else None
                run_mux_scan = False
                if not mux_uuid:
                    try:
                        mux_uuid = await tvh.network_mux_create(net_uuid)
                        run_mux_scan = True
                    except Exception as exc:
                        logger.error("Failed creating CSO mux for channel '%s': %s", channel_obj.name, exc)
                        return

                cso_url = build_cso_channel_stream_url(
                    base_url=tic_base_url,
                    channel_id=channel_obj.id,
                    stream_key=tvh_stream_key,
                    username=tvh_stream_username,
                    profile="tvh",
                    connection_id="tvh",
                )
                iptv_url = generate_iptv_url(
                    config,
                    url=cso_url,
                    service_name=f"CSO - {channel_obj.name}",
                )
                channel_id = f"{channel_obj.number}_{re.sub(r'[^a-zA-Z0-9]', '', channel_obj.name)}"
                mux_conf = {
                    "enabled": 1,
                    "uuid": mux_uuid,
                    "iptv_url": iptv_url,
                    "iptv_icon": build_channel_logo_proxy_url(
                        channel_obj.id,
                        tic_base_url or LOCAL_PROXY_HOST_PLACEHOLDER,
                        channel_obj.logo_url or "",
                    ),
                    "iptv_sname": channel_obj.name,
                    "iptv_muxname": mux_name,
                    "channel_number": channel_obj.number,
                    "iptv_epgid": channel_id,
                    "priority": "1",
                    "spriority": "1",
                }
                if run_mux_scan:
                    mux_conf["scan_state"] = 1
                try:
                    await tvh.idnode_save(mux_conf)
                    managed_uuids.append(mux_uuid)
                except Exception as exc:
                    logger.error("Failed saving CSO mux for channel '%s': %s", channel_obj.name, exc)

        # Schedule tasks
        for channel_obj in results:
            if not channel_obj.enabled:
                continue
            if bool(getattr(channel_obj, "cso_enabled", False)):
                mux_tasks.append(asyncio.create_task(process_cso_channel(channel_obj)))
                continue
            for source_obj in channel_obj.sources:
                mux_tasks.append(asyncio.create_task(process_source(channel_obj, source_obj)))

        # Await all
        if mux_tasks:
            await asyncio.gather(*mux_tasks)
            async with Session() as session:
                async with session.begin():
                    for channel_obj in results:
                        if bool(getattr(channel_obj, "cso_enabled", False)):
                            for source_obj in channel_obj.sources or []:
                                if not source_obj.id:
                                    continue
                                source_row = await session.get(ChannelSource, source_obj.id)
                                if not source_row:
                                    continue
                                source_row.tvh_uuid = None
                            continue
                        for source_obj in channel_obj.sources or []:
                            if not source_obj.id:
                                continue
                            source_row = await session.get(ChannelSource, source_obj.id)
                            if not source_row:
                                continue
                            source_row.tvh_uuid = source_obj.tvh_uuid
                            source_row.xc_account_id = source_obj.xc_account_id

        # Cleanup unused muxes
        logger.info("Running cleanup task on current TVH muxes")
        # Refresh existing mux list for cleanup (not from cache to be up-to-date)
        current_muxes = await tvh.list_all_muxes()
        for existing_mux in current_muxes:
            uuid = existing_mux.get("uuid")
            mux_name = existing_mux.get("iptv_muxname") or existing_mux.get("name") or ""
            if uuid and uuid not in managed_uuids and mux_name.lower().startswith("tic-"):
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
            "args": [config, True, "manual"],
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
    if epg_settings.get("enable_tmdb_metadata") or epg_settings.get("enable_google_image_search_metadata"):
        await task_broker.add_task(
            {
                "name": "Update EPG Data with online metadata",
                "function": update_channel_epg_with_online_data,
                "args": [config],
            },
            priority=21,
        )
    # Generate 'epg.xml' file in .tvh_iptv_config directory
    from backend.epgs import build_custom_epg_subprocess

    await task_broker.add_task(
        {
            "name": "Recreating static XMLTV file",
            "function": build_custom_epg_subprocess,
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
    added_channel_count = 0
    group_new_channels = []

    # First determine the starting channel number for the whole operation
    async with Session() as session:
        channel_number = await session.scalar(select(func.max(Channel.number)))
    if channel_number is None:
        channel_number = 999

    for group_info in groups:
        playlist_id = group_info.get("playlist_id")
        group_name = group_info.get("group_name")

        if not playlist_id or not group_name:
            logger.warning(f"Missing playlist_id or group_name in group info: {group_info}")
            continue

        # Get all streams from this group
        async with Session() as session:
            stream_query = await session.execute(
                select(PlaylistStreams).where(
                    PlaylistStreams.playlist_id == playlist_id,
                    PlaylistStreams.group_title == group_name,
                )
            )
            playlist_streams_query = stream_query.scalars().all()

        logger.info(f"Found {len(playlist_streams_query)} streams in group '{group_name}' to add")

        for stream in playlist_streams_query:
            try:
                # Check if Channel with this name already exists
                async with Session() as session:
                    existing_query = await session.execute(select(Channel).where(Channel.name == stream.name.strip()))
                    existing_channel = existing_query.scalars().first()

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
                async with Session() as session:
                    epg_query = await session.execute(
                        select(EpgChannels).where(EpgChannels.channel_id == stream.tvg_id)
                    )
                    epg_match = epg_query.scalars().first()
                if epg_match is not None:
                    new_channel_data["guide"] = {
                        "channel_id": epg_match.channel_id,
                        "epg_id": epg_match.epg_id,
                        "epg_name": epg_match.name,
                    }

                # Get the playlist info
                async with Session() as session:
                    playlist_query = await session.execute(select(Playlist).where(Playlist.id == playlist_id))
                    playlist_info = playlist_query.scalar_one()

                # Add source information
                new_channel_data["sources"].append(
                    {
                        "playlist_id": playlist_id,
                        "playlist_name": playlist_info.name,
                        "stream_name": stream.name,
                    }
                )

                channel_obj = await add_new_channel(config, new_channel_data, commit=True, publish=False)
                group_new_channels.append(channel_obj)
                added_channel_count += 1

            except Exception as e:
                logger.error(f"Error adding channel '{stream.name}' from group '{group_name}': {str(e)}")
                import traceback

                logger.error(traceback.format_exc())

    try:
        await batch_publish_new_channels_to_tvh(config, group_new_channels)
    except Exception as e:
        logger.error("Batch publish (groups) failed: %s", e)

    logger.info(f"Successfully added {added_channel_count} channels from groups")
    return added_channel_count


def region_label(ip_address: str | None) -> str:
    ip = (ip_address or "").strip()
    if not ip:
        return "Unknown region"
    if ip.startswith("10.") or ip.startswith("192.168."):
        return "Local network"
    if ip.startswith("172."):
        try:
            second_octet = int(ip.split(".")[1])
            if 16 <= second_octet <= 31:
                return "Local network"
        except Exception:
            pass
    if ip.startswith("fd") or ip.startswith("fc") or ip.startswith("fe80:"):
        return "Local network"
    if ip in {"127.0.0.1", "::1"}:
        return "Local host"
    return "Unknown region"


def normalize_url(value: str | None) -> str | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = urlparse(raw)
    except Exception:
        return raw
    if not parsed.scheme or not parsed.netloc:
        return raw
    normalized = parsed._replace(fragment="", query="")
    return urlunparse(normalized)


def maybe_decode_embedded_url_once(value: str | None) -> str | None:
    from urllib.parse import urlparse, urlunparse

    normalized = normalize_url(value)
    if not normalized:
        return None
    try:
        parsed = urlparse(normalized)
    except Exception:
        return None
    tail = (parsed.path or "").rsplit("/", 1)[-1]
    if not tail:
        return None
    token = tail.split(".", 1)[0]
    if not token:
        return None
    padded = token + "=" * (-len(token) % 4)
    for decoder in (base64.urlsafe_b64decode, base64.b64decode):
        try:
            decoded = decoder(padded.encode("utf-8")).decode("utf-8")
        except Exception:
            continue
        decoded_norm = normalize_url(decoded)
        if decoded_norm and decoded_norm.startswith(("http://", "https://")):
            return decoded_norm
    return None


def candidate_urls(value: str | None) -> list[str]:
    candidates = []
    normalized = normalize_url(value)
    if normalized:
        candidates.append(normalized)
    current = normalized
    seen = set(candidates)
    while current:
        decoded = maybe_decode_embedded_url_once(current)
        if not decoded:
            break
        decoded_norm = normalize_url(decoded)
        if not decoded_norm or decoded_norm in seen:
            break
        candidates.append(decoded_norm)
        seen.add(decoded_norm)
        current = decoded_norm
    return candidates


def priority_rank(value: str | None) -> int:
    try:
        return int(str(value or "").strip())
    except Exception:
        return 1_000_000


_SOURCE_INDEX_CACHE = {"index": None, "expires": 0}
_SOURCE_INDEX_BUILD_LOCK = asyncio.Lock()


def _build_stream_source_index_maps(rows, stream_rows):
    exact_map = {}
    tvh_uuid_map = {}
    name_map = {}

    for row in rows:
        stream_url = row.get("stream_url")
        stream_candidates = candidate_urls(stream_url) if stream_url else []
        payload = {
            "channel_id": row.get("channel_id"),
            "channel_name": row.get("channel_name"),
            "channel_logo_url": row.get("channel_logo_url"),
            "source_id": row.get("source_id"),
            "playlist_id": row.get("playlist_id"),
            "xc_account_id": row.get("xc_account_id"),
            "stream_name": row.get("stream_name"),
            "stream_url": stream_url,
            "priority": str(row.get("priority") or ""),
        }
        p_rank = priority_rank(payload.get("priority"))
        ranking = (0, p_rank)  # Default ranking for non-URL matches

        tvh_uuid = row.get("tvh_uuid")
        if tvh_uuid:
            tvh_uuid_map[tvh_uuid] = payload

        channel_name = row.get("channel_name")
        if channel_name:
            existing = name_map.get(channel_name)
            if not existing or ranking < existing.get("_ranking", (1_000_000, 1_000_000)):
                name_map[channel_name] = {**payload, "_ranking": ranking}

        if not stream_candidates:
            continue
        for depth, candidate_url in enumerate(stream_candidates):
            ranking = (depth, p_rank)
            existing = exact_map.get(candidate_url)
            if not existing or ranking < existing.get("_ranking", (1_000_000, 1_000_000)):
                exact_map[candidate_url] = {**payload, "_ranking": ranking}

    for row in stream_rows:
        s_url = row.get("stream_url")
        if not s_url:
            continue
        payload = {
            "channel_id": None,
            "channel_name": row.get("stream_name"),
            "channel_logo_url": row.get("stream_logo"),
            "stream_name": row.get("stream_name"),
            "stream_url": s_url,
            "_ranking": (100, 1_000_000),
        }
        for cand in candidate_urls(s_url):
            if cand not in exact_map:
                exact_map[cand] = payload

    return {"exact": exact_map, "tvh_uuid": tvh_uuid_map, "name": name_map}


async def build_stream_source_index():
    global _SOURCE_INDEX_CACHE
    now = time.time()
    if _SOURCE_INDEX_CACHE["index"] and now < _SOURCE_INDEX_CACHE["expires"]:
        return _SOURCE_INDEX_CACHE["index"]
    async with _SOURCE_INDEX_BUILD_LOCK:
        # Recheck cache after acquiring lock to avoid duplicate rebuild work.
        now = time.time()
        if _SOURCE_INDEX_CACHE["index"] and now < _SOURCE_INDEX_CACHE["expires"]:
            return _SOURCE_INDEX_CACHE["index"]

        stmt = (
            select(
                Channel.id.label("channel_id"),
                Channel.name.label("channel_name"),
                Channel.logo_url.label("channel_logo_url"),
                Channel.tvh_uuid.label("tvh_uuid"),
                ChannelSource.id.label("source_id"),
                ChannelSource.playlist_id.label("playlist_id"),
                ChannelSource.xc_account_id.label("xc_account_id"),
                ChannelSource.playlist_stream_name.label("stream_name"),
                ChannelSource.playlist_stream_url.label("stream_url"),
                ChannelSource.priority.label("priority"),
            )
            .select_from(Channel)
            .outerjoin(ChannelSource, Channel.id == ChannelSource.channel_id)
        )
        async with Session() as session:
            result = await session.execute(stmt)
            rows = result.mappings().all()

        # Also include PlaylistStreams for anonymous lookup
        async with Session() as session:
            stmt_streams = select(
                PlaylistStreams.url.label("stream_url"),
                PlaylistStreams.name.label("stream_name"),
                PlaylistStreams.tvg_logo.label("stream_logo"),
            ).where(PlaylistStreams.tvg_logo != None)
            result = await session.execute(stmt_streams)
            stream_rows = result.mappings().all()

        # Build maps in a worker thread to avoid blocking Quart's async loop.
        index = await asyncio.to_thread(_build_stream_source_index_maps, rows, stream_rows)
        _SOURCE_INDEX_CACHE = {"index": index, "expires": now + 60}
        return index


def resolve_stream_target(details: str | None, source_index: dict, related_urls: list[str] | None = None) -> dict:
    candidates = candidate_urls(details)
    for url_value in related_urls or []:
        for candidate in candidate_urls(url_value):
            if candidate not in candidates:
                candidates.append(candidate)

    exact_map = source_index.get("exact", {})
    tvh_uuid_map = source_index.get("tvh_uuid", {})
    name_map = source_index.get("name", {})

    matched_source = None
    for candidate in candidates:
        matched_source = exact_map.get(candidate) or tvh_uuid_map.get(candidate) or name_map.get(candidate)
        if matched_source:
            break

    display_url = candidates[0] if candidates else None
    source_url = matched_source.get("stream_url") if matched_source else None
    if not display_url and source_url:
        display_url = source_url

    return {
        "channel_id": matched_source.get("channel_id") if matched_source else None,
        "channel_name": matched_source.get("channel_name") if matched_source else None,
        "channel_logo_url": matched_source.get("channel_logo_url") if matched_source else None,
        "source_id": matched_source.get("source_id") if matched_source else None,
        "playlist_id": matched_source.get("playlist_id") if matched_source else None,
        "xc_account_id": matched_source.get("xc_account_id") if matched_source else None,
        "stream_name": matched_source.get("stream_name") if matched_source else None,
        "source_url": source_url,
        "display_url": display_url,
    }
