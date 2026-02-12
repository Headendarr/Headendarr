#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import logging
import sys
from pathlib import Path
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from backend.utils import is_truthy

scheduler = AsyncIOScheduler()

logger = logging.getLogger('tic.tasks')

import itertools
from asyncio import Lock, PriorityQueue


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
    from backend.epgs import update_channel_epg_with_online_data
    await update_channel_epg_with_online_data(config)

    script_path = Path(__file__).resolve().parents[1] / "scripts" / "build_custom_epg.py"
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
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
    await publish_bulk_channels_to_tvh_and_m3u(config)


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
    try:
        await ensure_tvh_sync_user(config)
        async with await get_tvh(config) as tvh:
            await tvh.upsert_user(
                user.username,
                user.streaming_key,
                is_admin=is_admin,
                enabled=user.is_active and is_streamer,
                access_comment=f"{tvh_user_access_comment_prefix}:{user.username}",
                password_comment=f"{tvh_user_password_comment_prefix}:{user.username}",
            )
    except Exception as exc:
        logger.exception("Failed to sync TVH user - user_id=%s", user_id)
        await set_user_tvh_sync_status(user_id, "failed", str(exc))
        return
    await set_user_tvh_sync_status(user_id, "success", None)


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
