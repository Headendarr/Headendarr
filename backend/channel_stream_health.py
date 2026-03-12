#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import or_, select
from sqlalchemy.orm import joinedload

from backend.cso import (
    cso_capacity_registry,
    emit_channel_stream_event,
    is_internal_cso_activity,
    resolve_source_url_for_stream,
    source_capacity_key,
    source_capacity_limit,
)
from backend.datetime_utils import utc_now_naive
from backend.http_headers import parse_headers_json, sanitise_headers
from backend.models import Channel, ChannelSource, Playlist, Session
from backend.stream_activity import get_stream_activity_snapshot
from backend.stream_diagnostics import StreamProbe
from backend.tvheadend.tvh_requests import get_tvh

logger = logging.getLogger("tic.channel_stream_health")

CHANNEL_STREAM_HEALTH_CHECK_INTERVAL_HOURS = int(os.environ.get("CHANNEL_STREAM_HEALTH_CHECK_INTERVAL_HOURS", "6") or 6)
CHANNEL_STREAM_HEALTH_CHECK_PROBE_SECONDS = int(os.environ.get("CHANNEL_STREAM_HEALTH_CHECK_PROBE_SECONDS", "7") or 7)
CHANNEL_STREAM_HEALTH_CHECK_MAX_PER_RUN = int(os.environ.get("CHANNEL_STREAM_HEALTH_CHECK_MAX_PER_RUN", "10") or 10)
CHANNEL_STREAM_HEALTH_CHECK_CONCURRENCY = int(os.environ.get("CHANNEL_STREAM_HEALTH_CHECK_CONCURRENCY", "2") or 2)
CHANNEL_STREAM_HEALTH_CHECK_KILL_WAIT_SECONDS = float(
    os.environ.get("CHANNEL_STREAM_HEALTH_CHECK_KILL_WAIT_SECONDS", "2.0") or 2.0
)
_HEALTH_OWNER_PREFIX = "health-check-source-"


@dataclass
class ActiveHealthCheck:
    source_id: int
    channel_id: int
    capacity_key: str
    capacity_owner_key: str
    slot_id: int
    probe: StreamProbe
    started_monotonic: float


_active_health_checks_by_source: dict[int, ActiveHealthCheck] = {}
_health_checks_lock = asyncio.Lock()
_health_run_lock = asyncio.Lock()
_health_run_task: asyncio.Task | None = None
_scheduled_health_checks_by_source: dict[int, asyncio.Task] = {}


def _request_base_url():
    port = int(os.environ.get("FLASK_RUN_PORT", "9985") or 9985)
    return f"http://127.0.0.1:{port}"


def _count_external_source_connections(activity_sessions, source, capacity_key_name: str) -> int:
    count = 0
    expected_playlist_key = f"playlist:{int(source.playlist_id)}" if getattr(source, "playlist_id", None) else None
    expected_source_key = f"source:{int(source.id)}"
    expected_xc_key = f"xc:{int(source.xc_account_id)}" if getattr(source, "xc_account_id", None) else None

    for session in activity_sessions or []:
        if not isinstance(session, dict):
            continue
        endpoint = str(session.get("endpoint") or "")
        display_url = str(session.get("display_url") or "").lower()
        if is_internal_cso_activity(endpoint, display_url):
            continue

        session_key = None
        xc_account_id = session.get("xc_account_id")
        playlist_id = session.get("playlist_id")
        source_id = session.get("source_id")
        if xc_account_id:
            session_key = f"xc:{int(xc_account_id)}"
        elif playlist_id:
            session_key = f"playlist:{int(playlist_id)}"
        elif source_id:
            session_key = f"source:{int(source_id)}"
        if not session_key:
            continue
        if session_key != capacity_key_name:
            continue

        if expected_xc_key and session_key == expected_xc_key:
            count += 1
            continue
        if expected_playlist_key and session_key == expected_playlist_key:
            count += 1
            continue
        if session_key == expected_source_key:
            count += 1
    return count


def _classify_health_result(probe: StreamProbe):
    report = probe.report or {}
    probe_data = report.get("probe") or {}
    avg_speed = float(probe_data.get("avg_speed") or 0)
    avg_bitrate = float(probe_data.get("avg_bitrate") or 0)
    errors = [str(item).strip() for item in (report.get("errors") or []) if str(item).strip()]
    probe_health = str(probe_data.get("health") or "").strip().lower()

    if str(getattr(probe, "status", "")).strip().lower() == "cancelled":
        return "cancelled", "preempted", avg_speed, avg_bitrate, errors
    if errors:
        return "unhealthy", "unreachable", avg_speed, avg_bitrate, errors
    if avg_speed > 0 and avg_speed < 0.9:
        return "unhealthy", "too_slow", avg_speed, avg_bitrate, errors
    if probe_health in {"poor", "critical"}:
        return "unhealthy", "too_slow", avg_speed, avg_bitrate, errors
    if avg_bitrate <= 50_000:
        return "unhealthy", "unstable", avg_speed, avg_bitrate, errors
    return "healthy", "healthy", avg_speed, avg_bitrate, errors


def _same_stream_url(left: str, right: str) -> bool:
    return str(left or "").strip() == str(right or "").strip()


async def apply_tvh_mux_health_state_task(
    config,
    source_id: int,
    channel_id: int,
    mux_uuid: str,
    status: str,
    reason: str,
    enabled: bool,
    request_rescan: bool = True,
):
    mux_uuid_text = str(mux_uuid or "").strip()
    if not mux_uuid_text:
        logger.info(
            "Skipping TVH mux health update for periodic health transition source_id=%s channel_id=%s "
            "reason=%s status=%s enabled=%s (missing tvh_uuid)",
            source_id,
            channel_id,
            reason,
            status,
            enabled,
        )
        return False

    node = {"uuid": mux_uuid_text, "enabled": bool(enabled)}
    if request_rescan:
        node["scan_state"] = 1

    try:
        async with await get_tvh(config) as tvh:
            await tvh.idnode_save(node)
        logger.info(
            "Applied TVH mux health state for periodic health transition source_id=%s channel_id=%s mux_uuid=%s "
            "reason=%s status=%s enabled=%s request_rescan=%s",
            source_id,
            channel_id,
            mux_uuid_text,
            reason,
            status,
            enabled,
            request_rescan,
        )
        return True
    except Exception as ex:
        logger.warning(
            "Failed to apply TVH mux health state for periodic health transition source_id=%s channel_id=%s "
            "mux_uuid=%s reason=%s status=%s enabled=%s request_rescan=%s error=%s",
            source_id,
            channel_id,
            mux_uuid_text,
            reason,
            status,
            enabled,
            request_rescan,
            ex,
        )
        return False


async def queue_tvh_mux_health_state_update(
    config,
    source_id: int,
    channel_id: int,
    mux_uuid: str,
    status: str,
    reason: str,
    enabled: bool,
    request_rescan: bool = True,
):
    from backend.api.tasks import TaskQueueBroker

    mux_uuid_text = str(mux_uuid or "").strip()
    if not mux_uuid_text:
        logger.info(
            "Skipping queued TVH mux health update for source_id=%s channel_id=%s reason=%s status=%s enabled=%s "
            "(missing tvh_uuid)",
            source_id,
            channel_id,
            reason,
            status,
            enabled,
        )
        return False

    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task(
        {
            "name": f"Update TVH mux health - source:{int(source_id)} enabled:{int(bool(enabled))}",
            "function": apply_tvh_mux_health_state_task,
            "args": [config, source_id, channel_id, mux_uuid_text, status, reason, enabled, request_rescan],
        },
        priority=18,
    )
    logger.info(
        "Queued TVH mux health update for source_id=%s channel_id=%s mux_uuid=%s status=%s enabled=%s request_rescan=%s",
        source_id,
        channel_id,
        mux_uuid_text,
        status,
        enabled,
        request_rescan,
    )
    return True


async def apply_stream_probe_result_to_source(
    source_id,
    probe: StreamProbe,
    health_check_type="manual",
    tested_stream_url: str | None = None,
    require_exact_source_url_match: bool = False,
    config=None,
):
    try:
        source_id = int(source_id)
    except Exception:
        return False
    if source_id <= 0 or probe is None:
        return False

    async with Session() as session:
        result = await session.execute(
            select(ChannelSource)
            .options(
                joinedload(ChannelSource.channel),
                joinedload(ChannelSource.playlist),
            )
            .where(ChannelSource.id == source_id)
        )
        source = result.scalars().first()
    if not source:
        return False
    source_stream_url = str(getattr(source, "playlist_stream_url", "") or "").strip()
    if require_exact_source_url_match and not _same_stream_url(source_stream_url, tested_stream_url):
        logger.info(
            "Skipping diagnostics health-state write for source_id=%s because tested URL does not match source URL",
            source_id,
        )
        return False

    status, reason, avg_speed, avg_bitrate, errors = _classify_health_result(probe)
    if status == "cancelled":
        return False

    metrics_payload = {
        "avg_speed": avg_speed,
        "avg_bitrate": avg_bitrate,
        "probe_health": str((probe.report or {}).get("probe", {}).get("health") or ""),
        "media": (probe.report or {}).get("media") or {},
        "errors": errors[:5],
        "health_check_type": str(health_check_type or "manual"),
    }
    now_dt = utc_now_naive()
    previous_status = ""
    source_tvh_uuid = ""
    async with Session() as session:
        async with session.begin():
            current = await session.get(ChannelSource, source_id)
            if current:
                previous_status = str(getattr(current, "last_health_check_status", "") or "").strip().lower()
                source_tvh_uuid = str(getattr(current, "tvh_uuid", "") or "").strip()
                current.last_health_check_at = now_dt
                current.last_health_check_status = status
                current.last_health_check_reason = reason
                current.last_health_check_metrics = json.dumps(metrics_payload, sort_keys=True)
                media_shape = metrics_payload.get("media") or {}
                if media_shape:
                    current.stream_probe_at = now_dt
                    current.stream_probe_details = json.dumps(media_shape, sort_keys=True)

    playlist = getattr(source, "playlist", None)
    event_details = {
        "reason": reason,
        "source_id": source_id,
        "playlist_id": getattr(source, "playlist_id", None),
        "playlist_name": str(getattr(playlist, "name", "") or "").strip() or None,
        "stream_name": str(getattr(source, "playlist_stream_name", "") or "").strip() or None,
        "source_priority": getattr(source, "priority", None),
        "health_check_type": str(health_check_type or "manual"),
        "metrics": metrics_payload,
    }
    is_periodic_background = str(health_check_type or "").strip().lower() == "periodic_background"
    channel_id = int(getattr(source, "channel_id", 0) or 0)
    playlist_id = getattr(source, "playlist_id", None)
    session_id = f"health-check-source-{source_id}"
    if is_periodic_background:
        if status == "unhealthy" and previous_status != "unhealthy":
            if config is not None:
                await queue_tvh_mux_health_state_update(
                    config,
                    source_id,
                    channel_id,
                    source_tvh_uuid,
                    status,
                    reason,
                    enabled=False,
                    request_rescan=True,
                )
            await emit_channel_stream_event(
                channel_id=channel_id,
                source_id=source_id,
                playlist_id=playlist_id,
                session_id=session_id,
                event_type="scheduled_health_failed",
                severity="warning",
                details=event_details,
            )
        elif status == "healthy" and previous_status == "unhealthy":
            if config is not None:
                await queue_tvh_mux_health_state_update(
                    config,
                    source_id,
                    channel_id,
                    source_tvh_uuid,
                    status,
                    reason,
                    enabled=True,
                    request_rescan=True,
                )
            await emit_channel_stream_event(
                channel_id=channel_id,
                source_id=source_id,
                playlist_id=playlist_id,
                session_id=session_id,
                event_type="scheduled_health_recovered",
                severity="info",
                details=event_details,
            )
    else:
        if status == "unhealthy":
            await emit_channel_stream_event(
                channel_id=channel_id,
                source_id=source_id,
                playlist_id=playlist_id,
                session_id=session_id,
                event_type="health_actioned",
                severity="warning",
                details=event_details,
            )
        elif status == "healthy":
            await emit_channel_stream_event(
                channel_id=channel_id,
                source_id=source_id,
                playlist_id=playlist_id,
                session_id=session_id,
                event_type="health_recovered",
                severity="info",
                details=event_details,
            )
    return True


async def _active_health_checks_for_capacity_key(capacity_key_name: str) -> list[ActiveHealthCheck]:
    async with _health_checks_lock:
        return [entry for entry in _active_health_checks_by_source.values() if entry.capacity_key == capacity_key_name]


async def has_background_health_check_for_capacity_key(capacity_key_name: str) -> bool:
    checks = await _active_health_checks_for_capacity_key(capacity_key_name)
    return bool(checks)


async def schedule_background_health_check_for_source(app, source_id, reason="cso_failover", details=None) -> bool:
    try:
        source_id = int(source_id)
    except Exception:
        return False
    if source_id <= 0 or app is None:
        return False

    async with _health_checks_lock:
        active_check = _active_health_checks_by_source.get(source_id)
        scheduled_task = _scheduled_health_checks_by_source.get(source_id)
        if active_check is not None:
            return False
        if scheduled_task is not None and not scheduled_task.done():
            return False
        task = asyncio.create_task(
            _run_requested_source_health_check_worker(app, source_id, reason=reason, details=details or {})
        )
        _scheduled_health_checks_by_source[source_id] = task
    logger.info(
        "Queued background stream health check source_id=%s reason=%s details=%s",
        source_id,
        reason,
        details or {},
    )
    return True


async def cancel_background_health_checks_for_capacity_key(capacity_key_name: str, reason="playback_priority") -> int:
    checks = await _active_health_checks_for_capacity_key(capacity_key_name)
    if not checks:
        return 0

    logger.info(
        "Pre-empting %s background stream health check(s) for capacity key=%s reason=%s",
        len(checks),
        capacity_key_name,
        reason,
    )
    for check in checks:
        try:
            check.probe.cancel(reason=reason)
        except Exception:
            pass

    deadline = time.monotonic() + max(0.1, float(CHANNEL_STREAM_HEALTH_CHECK_KILL_WAIT_SECONDS))
    while time.monotonic() < deadline:
        await asyncio.sleep(0.05)
        if not await has_background_health_check_for_capacity_key(capacity_key_name):
            break

    # Defensive release if cancellation path has not finished yet.
    remaining = await _active_health_checks_for_capacity_key(capacity_key_name)
    for check in remaining:
        await cso_capacity_registry.release(
            check.capacity_key,
            check.capacity_owner_key,
            slot_id=check.slot_id,
        )
    return len(checks)


async def preempt_background_health_checks_for_channel(channel_id) -> int:
    # Always load sources in a fresh session with relationships needed by source_capacity_limit().
    # This avoids detached-instance lazy-load failures from request-level ORM objects.
    async with Session() as session:
        result = await session.execute(
            select(ChannelSource)
            .options(
                joinedload(ChannelSource.playlist),
                joinedload(ChannelSource.xc_account),
            )
            .where(ChannelSource.channel_id == channel_id)
        )
        sources = list(result.scalars().all())

    if not sources:
        return 0

    cancelled = 0
    seen_keys = set()
    for source in sources:
        key_name = source_capacity_key(source)
        if key_name in seen_keys:
            continue
        seen_keys.add(key_name)
        usage = await cso_capacity_registry.get_usage(key_name)
        capacity_limit = int(source_capacity_limit(source) or 0)
        if capacity_limit <= 0:
            continue
        if not await has_background_health_check_for_capacity_key(key_name):
            continue
        if int(usage.get("total") or 0) >= capacity_limit:
            cancelled += await cancel_background_health_checks_for_capacity_key(
                key_name,
                reason="channel_playback_priority",
            )
    return cancelled


async def _run_requested_source_health_check_worker(app, source_id, reason="cso_failover", details=None):
    try:
        async with Session() as session:
            result = await session.execute(
                select(ChannelSource)
                .options(
                    joinedload(ChannelSource.channel),
                    joinedload(ChannelSource.playlist),
                    joinedload(ChannelSource.xc_account),
                )
                .where(ChannelSource.id == int(source_id))
            )
            source = result.scalars().first()
        if not source:
            return

        status, outcome_reason = await _run_source_health_check(app.config["APP_CONFIG"], source)
        logger.info(
            "Background stream health check complete source_id=%s channel_id=%s trigger_reason=%s status=%s outcome=%s details=%s",
            int(source_id),
            int(getattr(source, "channel_id", 0) or 0),
            reason,
            status,
            outcome_reason,
            details or {},
        )
    except Exception as exc:
        logger.warning(
            "Background stream health check failed source_id=%s trigger_reason=%s error=%s details=%s",
            source_id,
            reason,
            exc,
            details or {},
        )
    finally:
        async with _health_checks_lock:
            task = _scheduled_health_checks_by_source.get(int(source_id))
            if task is asyncio.current_task():
                _scheduled_health_checks_by_source.pop(int(source_id), None)


async def _run_source_health_check(config, source):
    channel = getattr(source, "channel", None)
    if not channel or not bool(getattr(channel, "enabled", False)):
        return "skipped", "channel_disabled"
    playlist = getattr(source, "playlist", None)
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return "skipped", "playlist_disabled"

    capacity_key_name = source_capacity_key(source)
    capacity_limit = int(source_capacity_limit(source) or 0)
    if capacity_limit <= 0:
        return "skipped", "capacity_limit_zero"

    usage = await cso_capacity_registry.get_usage(capacity_key_name)
    activity_sessions = await get_stream_activity_snapshot()
    external_count = _count_external_source_connections(activity_sessions, source, capacity_key_name)
    active_checks = await _active_health_checks_for_capacity_key(capacity_key_name)
    diagnostic_allocations = len(active_checks)
    non_diagnostic_allocations = max(0, int(usage.get("allocations") or 0) - diagnostic_allocations)
    if (non_diagnostic_allocations + external_count) > 0:
        return "skipped", "source_busy"

    owner_key = f"{_HEALTH_OWNER_PREFIX}{int(source.id)}"
    reserved = await cso_capacity_registry.try_reserve(
        capacity_key_name,
        owner_key,
        capacity_limit,
        slot_id=int(source.id),
    )
    if not reserved:
        return "skipped", "capacity_blocked"

    check_url = resolve_source_url_for_stream(
        str(getattr(source, "playlist_stream_url", "") or ""),
        _request_base_url(),
        config.ensure_instance_id(),
    )
    if not check_url:
        await cso_capacity_registry.release(capacity_key_name, owner_key, slot_id=int(source.id))
        return "skipped", "invalid_stream_url"

    preferred_user_agent = str(getattr(playlist, "user_agent", "") or "").strip() or None
    try:
        preferred_headers = sanitise_headers(parse_headers_json(getattr(playlist, "hls_proxy_headers", None)))
    except ValueError:
        preferred_headers = {}
    probe = StreamProbe(
        check_url,
        bypass_proxies=False,
        request_host_url=f"{_request_base_url()}/",
        preferred_user_agent=preferred_user_agent,
        preferred_headers=preferred_headers,
        probe_window_seconds=CHANNEL_STREAM_HEALTH_CHECK_PROBE_SECONDS,
        hard_timeout_seconds=max(10, CHANNEL_STREAM_HEALTH_CHECK_PROBE_SECONDS + 15),
        include_geo_lookup=False,
    )

    active_entry = ActiveHealthCheck(
        source_id=int(source.id),
        channel_id=int(getattr(source, "channel_id", 0) or 0),
        capacity_key=capacity_key_name,
        capacity_owner_key=owner_key,
        slot_id=int(source.id),
        probe=probe,
        started_monotonic=time.monotonic(),
    )
    async with _health_checks_lock:
        _active_health_checks_by_source[int(source.id)] = active_entry

    try:
        await probe.run()
        status, reason, _, _, _ = _classify_health_result(probe)
        if status != "cancelled":
            await apply_stream_probe_result_to_source(
                int(source.id),
                probe,
                health_check_type="periodic_background",
                config=config,
            )
        return status, reason
    finally:
        async with _health_checks_lock:
            _active_health_checks_by_source.pop(int(source.id), None)
        await cso_capacity_registry.release(capacity_key_name, owner_key, slot_id=int(source.id))


async def run_periodic_channel_stream_health_checks(app):
    global _health_run_task
    async with _health_run_lock:
        if _health_run_task and not _health_run_task.done():
            logger.debug("Periodic channel stream health checks already running; skipping duplicate trigger.")
            return False
        loop = asyncio.get_running_loop()
        _health_run_task = loop.create_task(_run_periodic_channel_stream_health_checks_worker(app))
        return True


async def _run_periodic_channel_stream_health_checks_worker(app):
    config = app.config["APP_CONFIG"]
    settings = (config.read_settings() or {}).get("settings", {})
    if not bool(settings.get("periodic_channel_stream_health_checks", True)):
        logger.debug("Periodic channel stream health checks are disabled.")
        return

    max_checks = max(1, int(CHANNEL_STREAM_HEALTH_CHECK_MAX_PER_RUN))
    max_parallel = max(1, min(int(CHANNEL_STREAM_HEALTH_CHECK_CONCURRENCY), max_checks))
    now_dt = utc_now_naive()
    cutoff_dt = now_dt - timedelta(hours=max(1, int(CHANNEL_STREAM_HEALTH_CHECK_INTERVAL_HOURS)))

    candidate_limit = max(50, max_checks * 30)
    async with Session() as session:
        result = await session.execute(
            select(ChannelSource)
            .options(
                joinedload(ChannelSource.channel),
                joinedload(ChannelSource.playlist),
                joinedload(ChannelSource.xc_account),
            )
            .join(Channel, Channel.id == ChannelSource.channel_id)
            .where(
                Channel.enabled.is_(True),
                or_(
                    ChannelSource.playlist_id.is_(None),
                    ChannelSource.playlist.has(Playlist.enabled.is_(True)),
                ),
                ChannelSource.playlist_stream_url.is_not(None),
                ChannelSource.playlist_stream_url != "",
                or_(
                    ChannelSource.last_health_check_at.is_(None),
                    ChannelSource.last_health_check_at < cutoff_dt,
                ),
            )
            .order_by(ChannelSource.last_health_check_at.asc().nullsfirst(), ChannelSource.id.asc())
            .limit(candidate_limit)
        )
        sources = result.scalars().unique().all()

    # Keep one candidate per shared source-capacity key (playlist / XC account / source),
    # but do not cap this list up-front so we can keep drawing candidates if some are skipped.
    selected_sources = []
    selected_capacity_keys = set()
    for source in sources:
        capacity_key_name = source_capacity_key(source)
        if capacity_key_name in selected_capacity_keys:
            continue
        selected_sources.append(source)
        selected_capacity_keys.add(capacity_key_name)

    healthy = 0
    unhealthy = 0
    skipped = 0
    cancelled = 0
    selected = 0
    completed_checks = 0
    sem = asyncio.Semaphore(max_parallel)

    async def _run_one(source):
        source_id = int(getattr(source, "id", 0) or 0)
        channel_id = int(getattr(source, "channel_id", 0) or 0)
        if source_id <= 0:
            return "skipped", "invalid_source_id", source_id, channel_id
        async with sem:
            status, reason = await _run_source_health_check(config, source)
            return status, reason, source_id, channel_id

    next_index = 0
    in_flight: set[asyncio.Task] = set()
    while completed_checks < max_checks:
        while next_index < len(selected_sources) and len(in_flight) < max_parallel:
            task = asyncio.create_task(_run_one(selected_sources[next_index]))
            in_flight.add(task)
            next_index += 1
            selected += 1

        if not in_flight:
            break

        done, in_flight = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            try:
                item = task.result()
            except Exception as ex:
                logger.exception("Periodic channel stream health check worker failed: %s", ex)
                skipped += 1
                continue

            status, reason, source_id, channel_id = item
            if status == "healthy":
                healthy += 1
                completed_checks += 1
            elif status == "unhealthy":
                unhealthy += 1
                completed_checks += 1
            elif status == "cancelled":
                cancelled += 1
                completed_checks += 1
            else:
                skipped += 1
            logger.info(
                "Periodic channel stream health check source_id=%s channel_id=%s status=%s reason=%s",
                source_id,
                channel_id,
                status,
                reason,
            )

    # Drain in-flight tasks so capacity reservations and probe processes always clean up.
    if in_flight:
        remaining = await asyncio.gather(*in_flight, return_exceptions=True)
        for item in remaining:
            if isinstance(item, Exception):
                logger.exception("Periodic channel stream health check worker failed: %s", item)
                skipped += 1
                continue
            status, reason, source_id, channel_id = item
            if status == "healthy":
                healthy += 1
            elif status == "unhealthy":
                unhealthy += 1
            elif status == "cancelled":
                cancelled += 1
            else:
                skipped += 1
            logger.info(
                "Periodic channel stream health check source_id=%s channel_id=%s status=%s reason=%s",
                source_id,
                channel_id,
                status,
                reason,
            )

    logger.info(
        "Periodic channel stream health checks complete selected=%s parallel=%s checked=%s target=%s healthy=%s "
        "unhealthy=%s cancelled=%s skipped=%s",
        selected,
        max_parallel,
        completed_checks,
        max_checks,
        healthy,
        unhealthy,
        cancelled,
        skipped,
    )
