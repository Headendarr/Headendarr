import asyncio
import logging

from sqlalchemy import select
from sqlalchemy.orm import joinedload

from backend.models import Channel, ChannelSource, Session
from backend.utils import clean_key, clean_text, convert_to_int

from .common import cso_session_manager
from .sources import order_cso_channel_sources
from .types import CsoSource


logger = logging.getLogger("cso")


class CsoCapacityRegistry:
    def __init__(self):
        self._allocations = {}
        self._external_counts = {}
        self._lock = asyncio.Lock()

    async def try_reserve(self, key, owner_key, limit, slot_id=None):
        async with self._lock:
            # key -> {owner_key: {slot_id: 1}}
            current = self._allocations.setdefault(key, {})
            owner_slots = current.setdefault(owner_key, {})
            if slot_id in owner_slots:
                # Owner already holds this specific slot; do not ref-count leak.
                return True

            # Count total slots across ALL owners for this key
            total_active = sum(len(slots) for slots in current.values())
            external = int(self._external_counts.get(key) or 0)
            if (total_active + external) >= max(0, int(limit or 0)):
                return False

            owner_slots[slot_id] = 1
            return True

    async def release(self, key, owner_key, slot_id=None):
        async with self._lock:
            current = self._allocations.get(key)
            if not current:
                return
            owner_slots = current.get(owner_key)
            if not owner_slots:
                return
            owner_slots.pop(slot_id, None)
            if not owner_slots:
                current.pop(owner_key, None)
            if not current:
                self._allocations.pop(key, None)

    async def release_all(self, owner_key):
        """Release all slots held by a specific owner across all keys."""
        async with self._lock:
            for key in list(self._allocations.keys()):
                current = self._allocations[key]
                current.pop(owner_key, None)
                if not current:
                    self._allocations.pop(key, None)

    async def set_external_counts(self, counts):
        async with self._lock:
            external_counts = {}
            for key, count in (counts or {}).items():
                try:
                    value = int(count or 0)
                except Exception:
                    value = 0
                if value > 0:
                    external_counts[str(key)] = value
            self._external_counts = external_counts

    async def get_usage(self, key):
        key_name = str(key or "")
        if not key_name:
            return {"allocations": 0, "external": 0, "total": 0}
        async with self._lock:
            current = self._allocations.get(key_name, {})
            allocations = sum(len(slots) for slots in current.values())
            external = int(self._external_counts.get(key_name) or 0)
            return {
                "allocations": int(allocations),
                "external": int(external),
                "total": int(allocations + external),
            }


cso_capacity_registry = CsoCapacityRegistry()


def source_capacity_key(source: CsoSource):
    if source.xc_account_id:
        return f"xc:{int(source.xc_account_id)}"
    if source.playlist_id:
        return f"playlist:{int(source.playlist_id)}"
    return f"source:{int(source.id or 0)}"


def source_capacity_limit(source: CsoSource):
    xc_account = source.xc_account
    if xc_account:
        try:
            return max(0, int(getattr(xc_account, "connection_limit", 0) or 0))
        except Exception:
            return 0
    playlist = source.playlist
    if playlist:
        try:
            return max(0, int(getattr(playlist, "connections", 0) or 0))
        except Exception:
            return 0
    return 1_000_000


def source_should_use_cso_buffer(source: CsoSource, force_tvh_remux=False):
    if force_tvh_remux:
        return True
    playlist = source.playlist if source is not None else None
    return bool(
        getattr(source, "use_hls_proxy", False) and playlist and getattr(playlist, "hls_proxy_use_ffmpeg", False)
    )


def _increment_external_count(external_counts, key):
    if not key:
        return
    external_counts[key] = int(external_counts.get(key) or 0) + 1


def is_internal_cso_activity(endpoint: str, display_url: str = "") -> bool:
    endpoint_value = clean_text(endpoint)
    display_url_value = clean_key(display_url)
    if "/tic-api/cso/channel/" in endpoint_value or "/tic-api/cso/channel_stream/" in endpoint_value:
        return True
    if endpoint_value.startswith("/xc/movie/") or endpoint_value.startswith("/xc/series/"):
        return True
    if endpoint_value.startswith("/tic-tvh/") and "tic-cso-" in display_url_value:
        return True
    return False


async def reconcile_cso_capacity_with_tvh_channels(channel_ids, activity_sessions=None):
    external_counts = {}
    fallback_channel_ids = set()

    for value in channel_ids or []:
        parsed = convert_to_int(value, None)
        if parsed:
            fallback_channel_ids.add(parsed)

    for session in activity_sessions or []:
        if not isinstance(session, dict):
            continue
        endpoint = clean_text(session.get("endpoint"))
        display_url = clean_key(session.get("display_url"))
        # CSO endpoint usage is already tracked via in-process allocations.
        # TVH subscriptions against CSO mux should not count as additional external usage.
        if is_internal_cso_activity(endpoint, display_url):
            continue

        xc_account_id = convert_to_int(session.get("xc_account_id"), None)
        playlist_id = convert_to_int(session.get("playlist_id"), None)
        source_id = convert_to_int(session.get("source_id"), None)
        if xc_account_id:
            _increment_external_count(external_counts, f"xc:{xc_account_id}")
            continue
        if playlist_id:
            _increment_external_count(external_counts, f"playlist:{playlist_id}")
            continue
        if source_id:
            _increment_external_count(external_counts, f"source:{source_id}")
            continue

        channel_id = convert_to_int(session.get("channel_id"), None)
        if channel_id:
            fallback_channel_ids.add(channel_id)

    unresolved = []
    for channel_id in sorted(fallback_channel_ids):
        if not await cso_session_manager.has_active_ingest_for_channel(channel_id):
            unresolved.append(channel_id)
    if not unresolved:
        await cso_capacity_registry.set_external_counts(external_counts)
        return

    async with Session() as session:
        result = await session.execute(
            select(Channel)
            .options(
                joinedload(Channel.sources).joinedload(ChannelSource.playlist),
                joinedload(Channel.sources).joinedload(ChannelSource.xc_account),
            )
            .where(Channel.id.in_(unresolved))
        )
        channels = result.scalars().unique().all()
        for channel in channels:
            candidates = await order_cso_channel_sources(
                list(channel.sources or []), channel_id=getattr(channel, "id", None)
            )
            for source in candidates:
                playlist = getattr(source, "playlist", None)
                if playlist is not None and not bool(getattr(playlist, "enabled", False)):
                    continue
                stream_url = (getattr(source, "playlist_stream_url", None) or "").strip()
                if not stream_url:
                    continue
                key = source_capacity_key(source)
                _increment_external_count(external_counts, key)
                break
    await cso_capacity_registry.set_external_counts(external_counts)


async def preempt_backpressured_clients_for_capacity_key(capacity_key_name, min_elapsed_seconds=0.5, min_drop_count=3):
    if not capacity_key_name:
        return 0

    removed = 0
    async with cso_session_manager.output.lock:
        sessions = list(cso_session_manager.output.sessions.values())

    for session in sessions:
        try:
            source = getattr(session.ingest_session, "current_source", None)
            if source is None:
                continue
            if source_capacity_key(source) != str(capacity_key_name):
                continue
            removed += int(
                await session.drop_backpressured_clients(
                    min_elapsed_seconds=min_elapsed_seconds,
                    min_drop_count=min_drop_count,
                )
                or 0
            )
        except Exception as exc:
            logger.error(
                "Error preempting backpressured clients key=%s output_key=%s error=%s",
                capacity_key_name,
                getattr(session, "key", None),
                exc,
                exc_info=True,
            )
    return removed
