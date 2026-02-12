#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import re
import unicodedata

from sqlalchemy import delete
from sqlalchemy.orm import joinedload

from backend.models import Channel, ChannelSuggestion, PlaylistStreams, db


NOISE_TOKENS = {
    "hd", "fhd", "uhd", "4k", "8k", "sd", "hevc", "h264", "h265",
    "live", "channel", "ch", "tv", "stream",
}

REGION_ALIASES = {
    "nz": "NZ",
    "new zealand": "NZ",
    "au": "AU",
    "australia": "AU",
    "us": "US",
    "usa": "US",
    "united states": "US",
    "uk": "UK",
    "united kingdom": "UK",
    "lat": "LAT",
    "latam": "LAT",
    "latin america": "LAT",
    "caribbean": "CAR",
    "chile": "CL",
    "colombia": "CO",
    "brazil": "BR",
    "argentina": "AR",
    "peru": "PE",
    "mexico": "MX",
    "tanzania": "TZ",
    "africa": "AF",
    "south africa": "ZA",
    "netherlands": "NL",
    "nederland": "NL",
}


def _normalize_text(text):
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[|/_-]+", " ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _tokenize(text):
    normalized = _normalize_text(text)
    if not normalized:
        return set()
    tokens = set(normalized.split())
    cleaned = set()
    for token in tokens:
        if token in NOISE_TOKENS:
            continue
        if len(token) == 1 and not token.isdigit():
            continue
        cleaned.add(token)
    return cleaned


def _extract_region_tokens(text):
    normalized = _normalize_text(text)
    if not normalized:
        return set()
    tokens = set(normalized.split())
    regions = set()
    for token in tokens:
        alias = REGION_ALIASES.get(token)
        if alias:
            regions.add(alias)
    # Match multi-word aliases
    for alias, region in REGION_ALIASES.items():
        if " " in alias and alias in normalized:
            regions.add(region)
    return regions


def _jaccard(a, b):
    if not a or not b:
        return 0.0
    intersection = a.intersection(b)
    union = a.union(b)
    if not union:
        return 0.0
    return len(intersection) / len(union)


def _compute_score(channel_tokens, stream_tokens, category_tokens, group_tokens, existing_tokens_list):
    name_score = _jaccard(channel_tokens, stream_tokens)
    category_score = _jaccard(category_tokens, group_tokens)
    existing_score = 0.0
    if existing_tokens_list:
        existing_score = max((_jaccard(tokens, stream_tokens) for tokens in existing_tokens_list), default=0.0)
    return (0.55 * name_score) + (0.25 * category_score) + (0.20 * existing_score)


def _regions_match(channel_regions, stream_regions):
    if not channel_regions or not stream_regions:
        return False
    return bool(channel_regions.intersection(stream_regions))


def update_channel_suggestions_for_playlist(playlist_id, *, score_threshold=0.70, limit_per_channel=5):
    streams = (
        db.session.query(PlaylistStreams)
        .filter(PlaylistStreams.playlist_id == playlist_id)
        .all()
    )
    if not streams:
        return

    channels = (
        db.session.query(Channel)
        .options(joinedload(Channel.tags), joinedload(Channel.sources))
        .all()
    )

    stream_tokens_map = {}
    stream_regions_map = {}
    group_tokens_map = {}
    for stream in streams:
        stream_tokens_map[stream.id] = _tokenize(stream.name)
        group_tokens_map[stream.id] = _tokenize(stream.group_title)
        stream_regions = _extract_region_tokens(stream.name)
        stream_regions.update(_extract_region_tokens(stream.group_title))
        stream_regions_map[stream.id] = stream_regions

    matched_stream_ids = set()

    for channel in channels:
        channel_tokens = _tokenize(channel.name)
        category_tokens = set()
        channel_regions = _extract_region_tokens(channel.name)
        for tag in channel.tags or []:
            category_tokens |= _tokenize(tag.name)
            channel_regions |= _extract_region_tokens(tag.name)

        existing_tokens_list = []
        existing_source_name_pairs = set()
        existing_source_url_pairs = set()
        for source in channel.sources or []:
            if source.playlist_id and source.playlist_stream_url:
                existing_source_url_pairs.add((source.playlist_id, source.playlist_stream_url))
            elif source.playlist_id and source.playlist_stream_name:
                existing_source_name_pairs.add((source.playlist_id, source.playlist_stream_name))
            if source.playlist_stream_name:
                existing_tokens_list.append(_tokenize(source.playlist_stream_name))

        scored = []
        for stream in streams:
            if stream.url and (stream.playlist_id, stream.url) in existing_source_url_pairs:
                continue
            if not stream.url and (stream.playlist_id, stream.name) in existing_source_name_pairs:
                continue
            if not _regions_match(channel_regions, stream_regions_map.get(stream.id, set())):
                continue
            score = _compute_score(
                channel_tokens,
                stream_tokens_map.get(stream.id, set()),
                category_tokens,
                group_tokens_map.get(stream.id, set()),
                existing_tokens_list,
            )
            if score < score_threshold:
                continue
            scored.append((score, stream))

        scored.sort(key=lambda item: item[0], reverse=True)
        for score, stream in scored[:limit_per_channel]:
            matched_stream_ids.add(stream.id)
            existing = (
                db.session.query(ChannelSuggestion)
                .filter(
                    ChannelSuggestion.channel_id == channel.id,
                    ChannelSuggestion.playlist_id == stream.playlist_id,
                    ChannelSuggestion.stream_id == stream.id,
                )
                .one_or_none()
            )
            if existing:
                existing.stream_name = stream.name
                existing.stream_url = stream.url
                existing.group_title = stream.group_title
                existing.playlist_name = stream.playlist.name if stream.playlist else None
                existing.source_type = stream.source_type
                existing.score = score
            else:
                db.session.add(ChannelSuggestion(
                    channel_id=channel.id,
                    playlist_id=stream.playlist_id,
                    stream_id=stream.id,
                    stream_name=stream.name,
                    stream_url=stream.url,
                    group_title=stream.group_title,
                    playlist_name=stream.playlist.name if stream.playlist else None,
                    source_type=stream.source_type,
                    score=score,
                    dismissed=False,
                ))

    delete_query = delete(ChannelSuggestion).where(ChannelSuggestion.playlist_id == playlist_id)
    if matched_stream_ids:
        delete_query = delete_query.where(ChannelSuggestion.stream_id.notin_(matched_stream_ids))
    delete_query = delete_query.where(ChannelSuggestion.dismissed.is_(False))
    db.session.execute(delete_query)
    db.session.commit()
