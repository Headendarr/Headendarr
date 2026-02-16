#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from asyncio import Lock, PriorityQueue
import itertools
import asyncio
import logging
import re
from types import SimpleNamespace
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select
from backend.utils import is_truthy

scheduler = AsyncIOScheduler()

logger = logging.getLogger('tic.tasks')


class TaskQueueBroker:
    __instance = None
    __lock = Lock()
    __logger = None

    def __init__(self, **kwargs):
        if TaskQueueBroker.__instance is not None:
            raise Exception("Singleton instance already exists!")
        else:
            # Create the singleton instance
            TaskQueueBroker.__instance = self
            # Create the queue
            self.__running_task = None
            self.__status = "running"
            self.__task_queue = PriorityQueue()
            self.__task_names = set()
            self.__priority_counter = itertools.count()

    @staticmethod
    def initialize(app_logger):
        TaskQueueBroker.__logger = app_logger

    @staticmethod
    async def get_instance():
        # Ensure no other coroutines can access this method at the same time
        async with TaskQueueBroker.__lock:
            # If the singleton instance has not been created yet, create it
            if TaskQueueBroker.__instance is None:
                TaskQueueBroker()
        return TaskQueueBroker.__instance

    def set_logger(self, app_logger):
        self.__logger = app_logger

    async def get_status(self):
        return self.__status

    async def toggle_status(self):
        if self.__status == "paused":
            self.__status = "running"
        else:
            self.__status = "paused"
        return self.__status

    async def add_task(self, task, priority=100):
        if task['name'] in self.__task_names:
            self.__logger.debug("Task already queued. Ignoring.")
            return
        await self.__task_queue.put((priority, next(self.__priority_counter), task))
        self.__task_names.add(task['name'])

    async def get_next_task(self):
        # Get the next task from the queue
        if not self.__task_queue.empty():
            task = await self.__task_queue.get()
            self.__task_names.remove(task['name'])
            return task
        else:
            return None

    async def execute_tasks(self):
        if self.__running_task is not None:
            self.__logger.warning("Another process is already running scheduled tasks.")
        if self.__task_queue.empty():
            self.__logger.debug("No pending tasks found.")
            return
        if self.__status == "paused":
            self.__logger.debug("Pending tasks queue paused.")
            return
        while not self.__task_queue.empty():
            if self.__status == "paused":
                break
            priority, i, task = await self.__task_queue.get()
            self.__task_names.remove(task['name'])
            self.__running_task = task['name']
            # Execute task here
            try:
                self.__logger.info("Executing task - %s.", task['name'])
                await task['function'](*task['args'])
            except Exception as e:
                self.__logger.exception("Failed to run task %s - %s", task['name'], str(e))
        self.__running_task = None

    async def get_currently_running_task(self):
        return self.__running_task

    async def get_pending_tasks(self):
        results = []
        async with self.__lock:
            # Temporarily hold tasks to restore them later
            temp_tasks = []
            while not self.__task_queue.empty():
                task = await self.__task_queue.get()
                temp_tasks.append(task)
                priority, i, task_data = task
                results.append(task_data['name'])
            # Put tasks back into the queue
            for task in temp_tasks:
                await self.__task_queue.put(task)
        return results


async def configure_tvh_with_defaults(app):
    logger.info("Configuring TVH")
    config = app.config['APP_CONFIG']
    from backend.tvheadend.tvh_requests import configure_tvh
    await configure_tvh(config)


async def update_playlists(app):
    logger.info("Updating Playlists")
    config = app.config['APP_CONFIG']
    from backend.playlists import import_playlist_data_for_all_playlists
    await import_playlist_data_for_all_playlists(config)


async def update_epgs(app):
    logger.info("Updating EPGs")
    config = app.config['APP_CONFIG']
    from backend.epgs import import_epg_data_for_all_epgs
    await import_epg_data_for_all_epgs(config)


async def scan_tvh_muxes(app):
    config = app.config['APP_CONFIG']
    settings = config.read_settings().get("settings", {})
    if not settings.get("periodic_mux_scan", False):
        logger.debug("Periodic TVH mux scanning is disabled.")
        return

    logger.info("Scheduling TVH mux scans")
    from backend.tvheadend.tvh_requests import get_tvh
    try:
        async with await get_tvh(config) as tvh:
            muxes = await tvh.list_all_muxes()
            updated = 0
            for mux in muxes:
                mux_uuid = mux.get("uuid")
                if not mux_uuid:
                    continue
                if "enabled" in mux and not is_truthy(mux.get("enabled")):
                    continue
                scan_state = mux.get("scan_state")
                pending_state = "PEND" if isinstance(scan_state, str) else 1
                await tvh.idnode_save({"uuid": mux_uuid, "scan_state": pending_state})
                updated += 1
        logger.info("Queued scans for %s TVH muxes", updated)
    except Exception as exc:
        logger.exception("Failed to queue TVH mux scans: %s", exc)


async def rebuild_custom_epg(app):
    logger.info("Rebuilding custom EPG (subprocess)")
    config = app.config['APP_CONFIG']
    from backend.epgs import build_custom_epg_subprocess
    await build_custom_epg_subprocess(config)


async def update_tvh_epg(app):
    logger.info("Triggering update of TVH EPG")
    config = app.config['APP_CONFIG']
    from backend.epgs import run_tvh_epg_grabbers
    await run_tvh_epg_grabbers(config)


async def update_tvh_networks(app):
    logger.info("Updating channels in TVH")
    config = app.config['APP_CONFIG']
    from backend.playlists import publish_playlist_networks
    await publish_playlist_networks(config)


async def update_tvh_channels(app):
    logger.info("Updating channels in TVH")
    config = app.config['APP_CONFIG']
    from backend.channels import publish_bulk_channels_to_tvh_and_m3u
    await publish_bulk_channels_to_tvh_and_m3u(config, False, "periodic")


async def update_tvh_muxes(app):
    logger.info("Updating muxes in TVH")
    config = app.config['APP_CONFIG']
    from backend.channels import publish_channel_muxes
    await publish_channel_muxes(config)


async def sync_user_to_tvh(config, user_id):
    logger.info("Syncing user to TVH - user_id=%s", user_id)
    from backend.users import set_user_tvh_sync_status
    await set_user_tvh_sync_status(user_id, "running", None)
    from backend.users import get_user_by_id
    user = await get_user_by_id(user_id)
    if not user:
        logger.warning("User not found for TVH sync - user_id=%s", user_id)
        await set_user_tvh_sync_status(user_id, "failed", "User not found")
        return
    if not user.streaming_key:
        logger.warning("User has no streaming key; skipping TVH sync - user_id=%s", user_id)
        await set_user_tvh_sync_status(user_id, "skipped", "User has no streaming key")
        return
    role_names = [role.name for role in user.roles] if user.roles else []
    is_admin = "admin" in role_names
    is_streamer = "streamer" in role_names or is_admin
    if not is_streamer:
        logger.info("User has no streaming roles; disabling in TVH - user_id=%s", user_id)
    from backend.tvheadend.tvh_requests import (
        ensure_tvh_sync_user,
        get_tvh,
        tvh_user_access_comment_prefix,
        tvh_user_password_comment_prefix,
    )
    from backend.dvr_profiles import (
        build_user_profile_name,
        normalize_retention_policy,
        read_recording_profiles_from_settings,
    )
    try:
        await ensure_tvh_sync_user(config)
        settings = config.read_settings()
        dvr_settings = settings.get("settings", {}).get("dvr", {}) or {}
        pre_padding = max(0, int(dvr_settings.get("pre_padding_mins", 2) or 2))
        post_padding = max(0, int(dvr_settings.get("post_padding_mins", 5) or 5))
        retention_policy = normalize_retention_policy(getattr(user, "dvr_retention_policy", "forever"))
        dvr_access_mode = str(getattr(user, "dvr_access_mode", "none") or "none").strip().lower()
        if is_admin:
            dvr_access_mode = "read_all_write_own"
        profile_templates = read_recording_profiles_from_settings(settings)
        default_template = profile_templates[0] if profile_templates else None
        default_dvr_config = ""
        dvr_permissions = []
        if dvr_access_mode == "read_write_own":
            dvr_permissions = ["basic", "htsp"]
        elif dvr_access_mode == "read_all_write_own":
            dvr_permissions = ["basic", "htsp", "all"]
        async with await get_tvh(config) as tvh:
            if dvr_permissions:
                for profile in profile_templates:
                    profile_name = build_user_profile_name(user.username, profile.get("name"))
                    ensured_profile = await tvh.ensure_user_recorder_profile(
                        user.username,
                        profile_name=profile_name,
                        pathname=profile.get("pathname"),
                        pre_padding_mins=pre_padding,
                        post_padding_mins=post_padding,
                        retention_policy=retention_policy,
                    )
                    if default_template and profile.get("key") == default_template.get("key"):
                        default_dvr_config = ensured_profile or ""
            await tvh.upsert_user(
                user.username,
                user.streaming_key,
                is_admin=is_admin,
                enabled=user.is_active and is_streamer,
                access_comment=f"{tvh_user_access_comment_prefix}:{user.username}",
                password_comment=f"{tvh_user_password_comment_prefix}:{user.username}",
                dvr_config=default_dvr_config or None,
                dvr_permissions=dvr_permissions,
            )
    except Exception as exc:
        logger.exception("Failed to sync TVH user - user_id=%s", user_id)
        await set_user_tvh_sync_status(user_id, "failed", str(exc))
        return
    await set_user_tvh_sync_status(user_id, "success", None)


async def sync_all_users_to_tvh(config):
    logger.info("Syncing all users to TVH")
    from backend.models import Session, User
    async with Session() as session:
        result = await session.execute(select(User.id))
        user_ids = [row[0] for row in result.all()]
    for user_id in user_ids:
        await sync_user_to_tvh(config, user_id)


async def map_new_tvh_services(app):
    logger.info("Mapping new services in TVH")
    config = app.config['APP_CONFIG']
    # Map any new services
    from backend.channels import map_all_services, cleanup_old_channels
    await map_all_services(config)
    # Clear out old channels
    await cleanup_old_channels(config)


async def reconcile_dvr_recordings(app):
    logger.info("Reconciling DVR recordings")
    config = app.config['APP_CONFIG']
    from backend.dvr import reconcile_tvh_recordings
    await reconcile_tvh_recordings(config)


async def apply_dvr_rules(app):
    logger.info("Applying DVR recording rules")
    config = app.config['APP_CONFIG']
    from backend.dvr import apply_recurring_rules
    await apply_recurring_rules(config)


_TVH_ACTIVE_SUBSCRIPTIONS = {}
_IPV4_PATTERN = re.compile(r"(?:\d{1,3}\.){3}\d{1,3}")


def _first_non_empty(entry, *keys):
    for key in keys:
        value = entry.get(key)
        if isinstance(value, dict):
            value = next((v for v in value.values() if v), None)
        if value is None:
            continue
        value = str(value).strip()
        if value:
            return value
    return None


def _extract_ipv4(value):
    if not value:
        return None
    match = _IPV4_PATTERN.search(str(value))
    return match.group(0) if match else None


def _extract_subscription_ip(entry):
    for key in ("ip", "ip_address", "peer", "client", "hostname", "host", "remote", "address"):
        found = _extract_ipv4(entry.get(key))
        if found:
            return found
    return None


def _subscription_identity(entry):
    direct = _first_non_empty(entry, "uuid", "id", "subscription_id", "identifier")
    if direct:
        return direct
    username = _first_non_empty(entry, "username", "user") or ""
    channel = _first_non_empty(entry, "channelname", "channel", "service") or ""
    user_agent = _first_non_empty(entry, "user_agent", "client", "hostname", "peer") or ""
    return f"{username}|{channel}|{user_agent}"


def _is_testing_state(entry):
    state = (_first_non_empty(entry, "state", "status") or "").lower()
    return state in {"test", "testing"}


def _build_active_recording_index(dvr_entries):
    channel_ids = set()
    channel_names = set()
    for rec in dvr_entries or []:
        state = str(rec.get("state") or "").lower()
        if state not in {"recording", "running"}:
            continue
        channel_id = str(rec.get("channel") or "").strip()
        channel_name = str(rec.get("channelname") or "").strip().lower()
        if channel_id:
            channel_ids.add(channel_id)
        if channel_name:
            channel_names.add(channel_name)
    return channel_ids, channel_names


def _is_recording_subscription(entry, recording_channel_ids, recording_channel_names):
    user_agent = _first_non_empty(entry, "user_agent", "client_user_agent")
    if user_agent:
        return False
    channel_id = str(entry.get("channel") or "").strip()
    channel_name = (_first_non_empty(entry, "channelname", "title", "service") or "").lower()
    if channel_id and channel_id in recording_channel_ids:
        return True
    if channel_name and channel_name in recording_channel_names:
        return True
    return False


def _build_subscription_details(entry, username, state, is_recording, channel_name=None):
    channel = channel_name or _first_non_empty(entry, "channelname", "channel", "service") or "Unknown"
    parts = [
        "TVHeadend recording" if is_recording else "TVHeadend subscription",
        f"Channel: {channel}",
    ]
    if username:
        parts.append(f"Username: {username}")
    if state:
        parts.append(f"State: {state}")
    return " | ".join(parts)


def _make_audit_user(user_id, username):
    if user_id is None:
        return None
    return SimpleNamespace(id=user_id, username=username or "", is_active=True, roles=[])


def _is_tvh_backend_username(username):
    return bool(username and str(username).strip().lower().startswith("tic-tvh-"))


async def poll_tvh_subscription_status(app):
    config = app.config['APP_CONFIG']
    from backend.auth import audit_stream_event
    from backend.tvheadend.tvh_requests import get_tvh
    from backend.users import get_user_by_username
    from backend.api.routes_hls_proxy import upsert_stream_activity, stop_stream_activity
    from backend.channels import read_config_all_channels

    global _TVH_ACTIVE_SUBSCRIPTIONS

    try:
        async with await get_tvh(config) as tvh:
            subscriptions = await tvh.list_status_subscriptions()
            dvr_entries = await tvh.list_dvr_entries()
    except Exception as exc:
        logger.debug("Skipping TVH status monitor poll: %s", exc)
        return

    # Build a map of TVH UUIDs and names to TIC channel names for better identity resolution
    channels_config = await read_config_all_channels()
    tvh_uuid_map = {c.get('tvh_uuid'): c.get('name') for c in channels_config if c.get('tvh_uuid')}
    channel_name_map = {c.get('name'): c.get('name') for c in channels_config if c.get('name')}

    recording_channel_ids, recording_channel_names = _build_active_recording_index(dvr_entries)
    observed = {}
    user_id_cache = {}

    for entry in subscriptions or []:
        if not isinstance(entry, dict):
            continue
        if _is_testing_state(entry):
            continue

        subscription_id = _subscription_identity(entry)
        if not subscription_id:
            continue

        username = _first_non_empty(entry, "username", "user")
        if _is_tvh_backend_username(username):
            continue
        if username not in user_id_cache:
            user = await get_user_by_username(username) if username else None
            user_id_cache[username] = user.id if user else None
        user_id = user_id_cache.get(username)

        state = _first_non_empty(entry, "state", "status")
        is_recording = _is_recording_subscription(entry, recording_channel_ids, recording_channel_names)
        event_type = "recording_start" if is_recording else "stream_start"
        stop_event_type = "recording_stop" if is_recording else "stream_stop"
        user_agent = _first_non_empty(entry, "user_agent", "client_user_agent", "client")
        if is_recording and not user_agent:
            user_agent = "TVHeadend Recorder"
        if not user_agent:
            user_agent = "TVHeadend"
        ip_address = _extract_subscription_ip(entry)
        tvh_channel_id = entry.get("channel")  # TVH status API 'channel' is often the name
        resolved_channel_name = tvh_uuid_map.get(tvh_channel_id) or channel_name_map.get(tvh_channel_id)
        details = _build_subscription_details(entry, username, state, is_recording, channel_name=resolved_channel_name)
        endpoint = f"/tic-tvh/api/status/subscriptions/{subscription_id}"

        observed[subscription_id] = {
            "user_id": user_id,
            "username": username,
            "event_type": event_type,
            "stop_event_type": stop_event_type,
            "endpoint": endpoint,
            "details": details,
            "tvh_channel_id": tvh_channel_id,
            "ip_address": ip_address,
            "user_agent": user_agent,
        }

    previous = _TVH_ACTIVE_SUBSCRIPTIONS
    started_ids = sorted(set(observed.keys()) - set(previous.keys()))
    stopped_ids = sorted(set(previous.keys()) - set(observed.keys()))

    for sub_id in started_ids:
        event = observed[sub_id]
        user = _make_audit_user(event.get("user_id"), event.get("username"))
        await audit_stream_event(
            user,
            event.get("event_type"),
            event.get("endpoint"),
            details=event.get("details"),
            ip_address=event.get("ip_address"),
            user_agent=event.get("user_agent"),
        )
        # Also register in the activity tracker for the dashboard
        # Use the TVH channel ID as the primary identity for resolution, but override details for auditing/display.
        await upsert_stream_activity(
            event.get("tvh_channel_id") or event.get("details"),
            connection_id=sub_id,
            endpoint_override=event.get("endpoint"),
            start_event_type=event.get("event_type"),
            user=user,
            ip_address=event.get("ip_address"),
            user_agent=event.get("user_agent"),
            perform_audit=False,  # Already audited above
            details_override=event.get("details"),
        )

    # Touch all currently active subscriptions in the tracker to keep them alive
    for sub_id, event in observed.items():
        if sub_id in started_ids:
            continue
        user = _make_audit_user(event.get("user_id"), event.get("username"))
        await upsert_stream_activity(
            event.get("tvh_channel_id") or event.get("details"),
            connection_id=sub_id,
            endpoint_override=event.get("endpoint"),
            start_event_type=event.get("event_type"),
            user=user,
            ip_address=event.get("ip_address"),
            user_agent=event.get("user_agent"),
            perform_audit=False,
            details_override=event.get("details"),
        )

    for sub_id in stopped_ids:
        event = previous[sub_id]
        user = _make_audit_user(event.get("user_id"), event.get("username"))
        await audit_stream_event(
            user,
            event.get("stop_event_type") or "stream_stop",
            event.get("endpoint"),
            details=event.get("details"),
            ip_address=event.get("ip_address"),
            user_agent=event.get("user_agent"),
        )
        # Also remove from the activity tracker
        await stop_stream_activity(
            event.get("tvh_channel_id") or event.get("details"),
            connection_id=sub_id,
            event_type=event.get("stop_event_type") or "stream_stop",
            endpoint_override=event.get("endpoint"),
            user=user,
            ip_address=event.get("ip_address"),
            user_agent=event.get("user_agent"),
            perform_audit=False,  # Already audited above
        )

    _TVH_ACTIVE_SUBSCRIPTIONS = observed
