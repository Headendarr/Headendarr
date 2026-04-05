import base64
import asyncio
import json
import logging
import time
from collections.abc import Iterable
from urllib.parse import parse_qs, urlparse

from sqlalchemy import select

from backend.models import ChannelSource, Playlist, Session, XcAccount
from backend.streaming import LOCAL_PROXY_HOST_PLACEHOLDER, append_stream_key, is_local_hls_proxy_url
from backend.utils import clean_key, clean_text, convert_to_int
from backend.vod import VodCuratedPlaybackCandidate, VodSourcePlaybackCandidate
from backend.xc_hosts import parse_xc_hosts

from .constants import (
    CSO_SOURCE_FAILURE_CACHE_TTL_SECONDS,
    CSO_SOURCE_FAILURE_PRIORITY_PENALTY,
    CSO_UNHEALTHY_SOURCE_PRIORITY_PENALTY,
)
from .types import CsoSource


logger = logging.getLogger("cso")
_cso_channel_failed_sources: dict[int, dict[int, float]] = {}
_cso_channel_failed_sources_lock = asyncio.Lock()


async def cso_source_from_vod_source(
    candidate: VodCuratedPlaybackCandidate | VodSourcePlaybackCandidate, upstream_url: str
) -> CsoSource:
    """Wrap a VOD item/episode candidate in a CsoSource adapter."""
    source_item = candidate.source_item
    if isinstance(candidate, VodCuratedPlaybackCandidate):
        episode_source = candidate.episode_source
        group_item = candidate.group_item
        episode_item = candidate.episode
    else:
        episode_source = None
        group_item = None
        episode_item = None

    source_playlist_id = convert_to_int(source_item.playlist_id, 0)
    async with Session() as session:
        playlist = await session.get(Playlist, source_playlist_id)

    xc_account = candidate.xc_account
    if xc_account is None:
        async with Session() as session:
            result = await session.execute(
                select(XcAccount)
                .where(XcAccount.playlist_id == source_playlist_id, XcAccount.enabled.is_(True))
                .order_by(XcAccount.id.asc())
            )
            xc_account = result.scalars().first()

    source_type = "vod_movie" if candidate.content_type == "movie" else "vod_episode"

    probe_details = None
    probe_at = None
    source_id = convert_to_int(source_item.id, 0)
    if candidate.content_type == "movie":
        probe_details = json.loads(source_item.stream_probe_details) if source_item.stream_probe_details else None
        probe_at = source_item.stream_probe_at

    internal_id = None
    cache_internal_id = None
    container_extension = source_item.container_extension
    if isinstance(candidate, VodCuratedPlaybackCandidate):
        if episode_item is not None:
            probe_details = json.loads(episode_item.stream_probe_details) if episode_item.stream_probe_details else None
            probe_at = episode_item.stream_probe_at
        if episode_source:
            source_id = convert_to_int(episode_source.id, 0)
            container_extension = episode_source.container_extension or container_extension
        group_item_is_curated = group_item is not None
        episode_item_is_curated = episode_item is not None
        if group_item is not None:
            container_extension = container_extension or group_item.container_extension
        if candidate.content_type == "movie":
            if group_item_is_curated:
                internal_id = group_item.id
            cache_internal_id = internal_id or source_id or None
        else:
            if episode_item_is_curated:
                internal_id = episode_item.id
            cache_internal_id = internal_id or source_id or None
    else:
        if candidate.content_type == "series":
            source_id = convert_to_int(candidate.cache_internal_id, 0) or source_id
        internal_id = candidate.internal_id
        cache_internal_id = candidate.cache_internal_id or source_id or None
        container_extension = container_extension or candidate.container_extension

    xc_account_id = None
    if xc_account is not None:
        xc_account_id = xc_account.id

    return CsoSource(
        id=source_id,
        source_type=source_type,
        url=str(upstream_url),
        playlist_id=source_playlist_id,
        playlist=playlist,
        xc_account_id=xc_account_id,
        xc_account=xc_account,
        use_hls_proxy=False,
        priority=0,
        channel_id=None,
        internal_id=internal_id,
        cache_internal_id=cache_internal_id,
        probe_details=probe_details,
        probe_at=probe_at,
        container_extension=clean_key(container_extension) or None,
    )


def cso_source_from_channel_source(source: ChannelSource) -> CsoSource:
    """Wrap a ChannelSource database model in a CsoSource adapter."""
    probe_details = None
    try:
        if hasattr(source, "stream_probe_details") and source.stream_probe_details:
            probe_details = json.loads(source.stream_probe_details)
    except Exception:
        pass

    return CsoSource(
        id=int(source.id),
        source_type="channel",
        url=str(getattr(source, "playlist_stream_url", "") or ""),
        playlist_id=int(getattr(source, "playlist_id", 0) or 0),
        playlist=getattr(source, "playlist", None),
        xc_account_id=getattr(source, "xc_account_id", None),
        xc_account=getattr(source, "xc_account", None),
        priority=convert_to_int(getattr(source, "priority", 0), 0),
        channel_id=getattr(source, "channel_id", None),
        use_hls_proxy=bool(getattr(source, "use_hls_proxy", False)),
        probe_details=probe_details,
        probe_at=getattr(source, "stream_probe_at", None),
        container_extension=None,
    )


async def _get_cso_channel_failed_source_ids(channel_id: int | str | None) -> set[int]:
    try:
        channel_id = int(channel_id)
    except Exception:
        return set()
    if channel_id <= 0:
        return set()

    now = time.time()
    async with _cso_channel_failed_sources_lock:
        failed_map = _cso_channel_failed_sources.get(channel_id) or {}
        active_ids = set()
        expired_ids = []
        for source_id, hold_until in failed_map.items():
            if float(hold_until or 0.0) > now:
                active_ids.add(int(source_id))
            else:
                expired_ids.append(int(source_id))
        for source_id in expired_ids:
            failed_map.pop(source_id, None)
        if failed_map:
            _cso_channel_failed_sources[channel_id] = failed_map
        else:
            _cso_channel_failed_sources.pop(channel_id, None)
        return active_ids


async def mark_cso_channel_source_temporarily_failed(
    channel_id: int | str | None, source_id: int | str | None, ttl_seconds: float = CSO_SOURCE_FAILURE_CACHE_TTL_SECONDS
) -> bool:
    try:
        channel_id = int(channel_id)
        source_id = int(source_id)
    except Exception:
        return False
    if channel_id <= 0 or source_id <= 0:
        return False

    hold_until = time.time() + max(1.0, float(ttl_seconds or CSO_SOURCE_FAILURE_CACHE_TTL_SECONDS))
    async with _cso_channel_failed_sources_lock:
        failed_map = _cso_channel_failed_sources.setdefault(channel_id, {})
        failed_map[source_id] = hold_until
    logger.info(
        "CSO source failure cache updated channel=%s source_id=%s ttl_seconds=%s",
        channel_id,
        source_id,
        int(max(1.0, float(ttl_seconds or CSO_SOURCE_FAILURE_CACHE_TTL_SECONDS))),
    )
    return True


def _cso_source_effective_priority(source: CsoSource, failed_source_ids: Iterable[int] | None = None) -> int:
    base_priority = convert_to_int(getattr(source, "priority", None), 0)
    source_id = source.id
    health_status = str(getattr(source, "last_health_check_status", "") or "").strip().lower()
    unhealthy_penalty = CSO_UNHEALTHY_SOURCE_PRIORITY_PENALTY if health_status == "unhealthy" else 0
    temporary_penalty = CSO_SOURCE_FAILURE_PRIORITY_PENALTY if source_id in set(failed_source_ids or set()) else 0
    return base_priority - unhealthy_penalty - temporary_penalty


async def order_cso_channel_sources(
    sources: Iterable[CsoSource], channel_id: int | str | None = None
) -> list[CsoSource]:
    candidates = list(sources or [])
    resolved_channel_id = channel_id
    if resolved_channel_id is None:
        for source in candidates:
            resolved_channel_id = getattr(source, "channel_id", None)
            if resolved_channel_id is not None:
                break
    failed_source_ids = await _get_cso_channel_failed_source_ids(resolved_channel_id)
    return sorted(
        candidates,
        key=lambda item: (
            _cso_source_effective_priority(item, failed_source_ids=failed_source_ids),
            convert_to_int(getattr(item, "priority", None), 0),
            -int(item.id or 0),
        ),
        reverse=True,
    )


def _unwrap_local_tic_hls_proxy_url(url, instance_id=None):
    if not url:
        return None
    parsed = urlparse(url)
    path = parsed.path or ""
    if "/tic-hls-proxy/" not in path:
        return None
    parts = [part for part in path.split("/") if part]
    try:
        proxy_idx = parts.index("tic-hls-proxy")
    except ValueError:
        return None

    if not instance_id:
        return None
    if len(parts) <= proxy_idx + 1:
        return None
    if parts[proxy_idx + 1] != str(instance_id):
        return None

    token = None
    if len(parts) > proxy_idx + 2 and parts[proxy_idx + 2] == "stream":
        if len(parts) > proxy_idx + 3:
            token = parts[proxy_idx + 3]
    elif len(parts) > proxy_idx + 2 and parts[proxy_idx + 2] == "proxy.m3u8":
        source = (parse_qs(parsed.query).get("url") or [None])[0]
        if source and source.startswith(("http://", "https://")):
            return source
        return None
    elif len(parts) > proxy_idx + 2:
        token = parts[proxy_idx + 2]

    if not token:
        return None
    token = token.split(".", 1)[0]
    if not token:
        return None
    try:
        padded = token + "=" * (-len(token) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8")
        if decoded.startswith(("http://", "https://")):
            return decoded
    except Exception:
        return None
    return None


def _resolve_source_url(source_url, base_url, instance_id, stream_key=None, username=None):
    resolved_url = clean_text(source_url)
    if not resolved_url:
        return resolved_url
    if LOCAL_PROXY_HOST_PLACEHOLDER in resolved_url:
        resolved_url = resolved_url.replace(LOCAL_PROXY_HOST_PLACEHOLDER, base_url)
    # Only unwrap this same instance's local TIC HLS proxy URL.
    for _ in range(4):
        next_url = _unwrap_local_tic_hls_proxy_url(resolved_url, instance_id=instance_id)
        if not next_url:
            break
        if next_url == resolved_url:
            break
        resolved_url = next_url

    if is_local_hls_proxy_url(resolved_url, instance_id=instance_id):
        # CSO ingest must never route through this same instance's HLS proxy.
        # If we still have a local proxy URL at this point, treat it as unresolved.
        logger.warning(
            "CSO source URL still points to local HLS proxy after unwrapping; skipping source url=%s", resolved_url
        )
        return ""
    elif stream_key and "/tic-hls-proxy/" in resolved_url and "stream_key=" not in resolved_url:
        resolved_url = append_stream_key(resolved_url, stream_key=stream_key, username=username)
    return resolved_url


def resolve_source_url_for_stream(source_url, base_url, instance_id, stream_key=None, username=None):
    return _resolve_source_url(
        source_url,
        base_url,
        instance_id,
        stream_key=stream_key,
        username=username,
    )


def resolve_source_url_candidates(source: CsoSource, base_url, instance_id, stream_key=None, username=None):
    primary_url = _resolve_source_url(
        source.url,
        base_url=base_url,
        instance_id=instance_id,
        stream_key=stream_key,
        username=username,
    )
    if not primary_url:
        return []

    if not source.xc_account_id:
        return [primary_url]

    playlist = source.playlist
    raw_hosts = getattr(playlist, "url", None) if playlist is not None else None
    host_list = parse_xc_hosts(raw_hosts)
    if len(host_list) <= 1:
        return [primary_url]

    parsed_primary = urlparse(primary_url)
    if not parsed_primary.scheme or not parsed_primary.netloc:
        return [primary_url]

    candidates = [primary_url]
    for host in host_list:
        parsed_host = urlparse(host if "://" in host else f"{parsed_primary.scheme}://{host}")
        if not parsed_host.scheme or not parsed_host.netloc:
            continue
        candidate = parsed_primary._replace(scheme=parsed_host.scheme, netloc=parsed_host.netloc).geturl()
        if candidate not in candidates:
            candidates.append(candidate)
    return candidates
