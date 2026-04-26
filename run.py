#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import asyncio
import json
import logging
import os
from typing import TypedDict
from urllib.parse import urlparse

from backend.api.tasks import (
    scheduler,
    update_playlists,
    map_new_tvh_services,
    update_epgs,
    update_tvh_muxes,
    configure_tvh_with_defaults,
    update_tvh_channels,
    update_tvh_networks,
    update_tvh_epg,
    TaskQueueBroker,
    reconcile_dvr_recordings,
    apply_dvr_rules,
    cleanup_vod_metadata_cache,
    poll_tvh_subscription_status,
    sync_all_users_to_tvh,
    run_periodic_channel_stream_health_checks,
    reconcile_plex_live_tv,
)
from backend.api.routes_hls_proxy import cleanup_hls_proxy_state
from backend.cso import cleanup_vod_proxy_cache, vod_cache_manager
from backend.stream_activity import load_stream_activity_state, persist_stream_activity_state
from backend.auth import cleanup_stream_audit_logs, audit_stream_event
from backend import create_app, config


bootstrap_logger = logging.getLogger("headendarr.bootstrap")


class SentryRuntimeConfig(TypedDict):
    SENTRY_DEBUG: bool
    SENTRY_DOCKER_IMAGE_TAG: str
    SENTRY_DSN: str
    SENTRY_ENVIRONMENT: str
    SENTRY_HOSTNAME: str | None
    SENTRY_PROFILES_SAMPLE_RATE: float
    SENTRY_RELEASE: str
    SENTRY_SERVICE_NAME: str
    SENTRY_TRANSPORT_TIMEOUT: float
    SENTRY_TRACES_SAMPLE_RATE: float
    enable_tracing: bool


def _is_valid_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _parse_sentry_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return default


def _parse_sentry_float(value: object, default: float) -> float | None:
    if value in (None, ""):
        return default
    if not isinstance(value, (int, float, str)):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _load_sentry_json_config() -> dict[str, object]:
    raw_sentry_config = os.environ.get("SENTRY_CONFIG", "")
    if len(raw_sentry_config) <= 10:
        if os.environ.get("SENTRY_CONFIG"):
            bootstrap_logger.warning("Ignoring SENTRY_CONFIG because it is too short to be valid JSON")
        return {}

    try:
        parsed_sentry_config = json.loads(raw_sentry_config)
    except json.JSONDecodeError:
        bootstrap_logger.warning("Ignoring SENTRY_CONFIG because it is not valid JSON")
        return {}

    if not isinstance(parsed_sentry_config, dict):
        bootstrap_logger.warning("Ignoring SENTRY_CONFIG because it is not a JSON object")
        return {}

    return parsed_sentry_config


def _load_sentry_config() -> SentryRuntimeConfig | None:
    parsed_sentry_config = _load_sentry_json_config()
    if parsed_sentry_config:
        bootstrap_logger.info("Detected SENTRY_CONFIG JSON with keys: %s", ", ".join(sorted(parsed_sentry_config.keys())))
    elif os.environ.get("SENTRY_DSN"):
        bootstrap_logger.info("Detected SENTRY_DSN environment variable")

    def _config_value(name: str, default: object = None) -> object:
        return parsed_sentry_config.get(name, os.environ.get(name, default))

    dsn = _config_value("SENTRY_DSN")
    if not isinstance(dsn, str) or not _is_valid_url(dsn):
        if parsed_sentry_config or os.environ.get("SENTRY_DSN"):
            bootstrap_logger.warning("Ignoring Sentry configuration because SENTRY_DSN is missing or invalid")
        return None

    traces_sample_rate = _parse_sentry_float(_config_value("SENTRY_TRACES_SAMPLE_RATE"), 0.2)
    profiles_sample_rate = _parse_sentry_float(_config_value("SENTRY_PROFILES_SAMPLE_RATE"), 0.2)
    tracing_enabled = (
        traces_sample_rate is not None
        and profiles_sample_rate is not None
        and traces_sample_rate > 0
        and profiles_sample_rate > 0
    )

    if not tracing_enabled:
        bootstrap_logger.warning(
            "SENTRY_CONFIG tracing disabled because one or more sample rates are missing, invalid, or non-positive"
        )

    sentry_traces_sample_rate: float = 0.0
    sentry_profiles_sample_rate: float = 0.0
    if tracing_enabled:
        assert traces_sample_rate is not None
        assert profiles_sample_rate is not None
        sentry_traces_sample_rate = traces_sample_rate
        sentry_profiles_sample_rate = profiles_sample_rate

    sentry_runtime_config: SentryRuntimeConfig = {
        "SENTRY_DEBUG": _parse_sentry_bool(_config_value("SENTRY_DEBUG"), default=False),
        "SENTRY_DOCKER_IMAGE_TAG": str(_config_value("SENTRY_DOCKER_IMAGE_TAG", "") or ""),
        "SENTRY_DSN": dsn,
        "enable_tracing": tracing_enabled,
        "SENTRY_ENVIRONMENT": str(_config_value("SENTRY_ENVIRONMENT", "production") or "production"),
        "SENTRY_HOSTNAME": str(_config_value("SENTRY_HOSTNAME", "") or "") or None,
        "SENTRY_PROFILES_SAMPLE_RATE": sentry_profiles_sample_rate,
        "SENTRY_RELEASE": str(_config_value("SENTRY_RELEASE", "unknown") or "unknown"),
        "SENTRY_SERVICE_NAME": str(_config_value("SENTRY_SERVICE_NAME", "headendarr") or "headendarr"),
        "SENTRY_TRANSPORT_TIMEOUT": _parse_sentry_float(_config_value("SENTRY_TRANSPORT_TIMEOUT"), 30.0) or 30.0,
        "SENTRY_TRACES_SAMPLE_RATE": sentry_traces_sample_rate,
    }
    bootstrap_logger.info(
        "Sentry runtime config accepted: environment=%s release=%s tracing=%s debug=%s service_name=%s transport_timeout=%ss",
        sentry_runtime_config["SENTRY_ENVIRONMENT"],
        sentry_runtime_config["SENTRY_RELEASE"],
        "enabled" if sentry_runtime_config["enable_tracing"] else "disabled",
        sentry_runtime_config["SENTRY_DEBUG"],
        sentry_runtime_config["SENTRY_SERVICE_NAME"],
        sentry_runtime_config["SENTRY_TRANSPORT_TIMEOUT"],
    )
    return sentry_runtime_config


def _initialise_sentry():
    sentry_runtime_config = _load_sentry_config()
    if not sentry_runtime_config:
        bootstrap_logger.info("Sentry is disabled for this process")
        return

    try:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration
        from sentry_sdk.integrations.quart import QuartIntegration
        from sentry_sdk.transport import HttpTransport
    except ImportError:
        bootstrap_logger.warning("SENTRY_CONFIG is set but sentry-sdk with Quart support is not installed")
        return

    transport_timeout = max(float(sentry_runtime_config["SENTRY_TRANSPORT_TIMEOUT"]), 0.1)

    class ConfiguredHttpTransport(HttpTransport):
        TIMEOUT = transport_timeout

    bootstrap_logger.info("Initialising sentry-sdk with Quart and logging integrations")
    sentry_sdk.init(
        dsn=sentry_runtime_config["SENTRY_DSN"],
        debug=sentry_runtime_config["SENTRY_DEBUG"],
        enable_tracing=sentry_runtime_config["enable_tracing"],
        environment=sentry_runtime_config["SENTRY_ENVIRONMENT"],
        integrations=[
            QuartIntegration(transaction_style="url"),
            LoggingIntegration(level=logging.INFO, event_level=logging.ERROR),
        ],
        profiles_sample_rate=sentry_runtime_config["SENTRY_PROFILES_SAMPLE_RATE"],
        release=sentry_runtime_config["SENTRY_RELEASE"],
        server_name=sentry_runtime_config["SENTRY_HOSTNAME"],
        transport=ConfiguredHttpTransport,
        traces_sample_rate=sentry_runtime_config["SENTRY_TRACES_SAMPLE_RATE"],
    )

    sentry_sdk.set_tag("service_name", sentry_runtime_config["SENTRY_SERVICE_NAME"])
    docker_image_tag = sentry_runtime_config["SENTRY_DOCKER_IMAGE_TAG"]
    if docker_image_tag:
        sentry_sdk.set_tag("docker_image_tag", docker_image_tag)
    if sentry_runtime_config["SENTRY_DEBUG"]:
        bootstrap_logger.info("SENTRY_DEBUG is enabled; sending bootstrap test message")
        sentry_sdk.capture_message("Headendarr Sentry bootstrap test", level="info")
        bootstrap_logger.info("Bootstrap test message submitted to sentry-sdk")
    bootstrap_logger.info("sentry-sdk initialised successfully")


_initialise_sentry()

# Create app
app = create_app()
if config.enable_app_debugging:
    app.logger.info(" DEBUGGING   = " + str(config.enable_app_debugging))
    app.logger.debug("DBMS        = " + config.sqlalchemy_database_uri)
    app.logger.debug("ASSETS_ROOT = " + config.assets_root)

task_logger = app.logger.getChild("tasks")
TaskQueueBroker.initialize(task_logger)


@scheduler.scheduled_job("interval", id="background_tasks", seconds=10)
async def background_tasks():
    async with app.app_context():
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.execute_tasks()


@scheduler.scheduled_job("interval", id="hls_proxy_cleanup", seconds=15, misfire_grace_time=60)
async def every_15_seconds_hls_cleanup():
    async with app.app_context():
        try:
            await asyncio.wait_for(cleanup_hls_proxy_state(), timeout=10.0)
        except asyncio.TimeoutError:
            app.logger.warning("HLS cleanup tick timed out after 10s")
        except Exception:
            app.logger.exception("HLS cleanup tick failed")
        try:
            await asyncio.wait_for(persist_stream_activity_state(), timeout=4.0)
        except asyncio.TimeoutError:
            app.logger.warning("Stream activity persist tick timed out after 4s")
        except Exception:
            app.logger.exception("Stream activity persist tick failed")


@scheduler.scheduled_job("interval", id="do_15_seconds", seconds=15, misfire_grace_time=15)
async def every_15_seconds():
    async with app.app_context():
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.add_task(
            {
                "name": "Polling TVHeadend subscription status",
                "function": poll_tvh_subscription_status,
                "args": [app],
                "task_key": "poll-tvh-subscription-status",
                "execution_mode": "concurrent",
            },
            priority=5,
        )


@scheduler.scheduled_job("interval", id="do_30_seconds", seconds=30, misfire_grace_time=15)
async def every_30_seconds():
    async with app.app_context():
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.add_task(
            {
                "name": "Reconciling DVR recordings",
                "function": reconcile_dvr_recordings,
                "args": [app],
                "task_key": "reconcile-dvr-recordings",
                "execution_mode": "concurrent",
            },
            priority=20,
        )


@scheduler.scheduled_job("interval", id="audit_log_cleanup", hours=6, misfire_grace_time=300)
async def every_6_hours():
    async with app.app_context():
        await cleanup_stream_audit_logs()


@scheduler.scheduled_job("interval", id="vod_metadata_cache_cleanup", hours=24, misfire_grace_time=300)
async def every_24_hours_vod_metadata_cleanup():
    async with app.app_context():
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.add_task(
            {
                "name": "Cleaning XC VOD metadata cache",
                "function": cleanup_vod_metadata_cache,
                "args": [app],
                "task_key": "cleanup-vod-metadata-cache",
                "execution_mode": "concurrent",
            },
            priority=26,
        )


@scheduler.scheduled_job("interval", id="do_60_seconds", seconds=60, misfire_grace_time=30)
async def every_60_seconds():
    async with app.app_context():
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.add_task(
            {
                "name": "Configuring TVH networks (periodic)",
                "function": update_tvh_networks,
                "args": [app],
            },
            priority=12,
        )
        await task_broker.add_task(
            {
                "name": "Cleaning VOD proxy cache",
                "function": cleanup_vod_proxy_cache,
                "args": [],
                "task_key": "vod-proxy-cache-cleanup",
                "execution_mode": "concurrent",
            },
            priority=12,
        )


@scheduler.scheduled_job("interval", id="do_5_mins", minutes=5, misfire_grace_time=60)
async def every_5_mins():
    async with app.app_context():
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.add_task(
            {
                "name": "Mapping all TVH services",
                "function": map_new_tvh_services,
                "args": [app],
            },
            priority=10,
        )
        await task_broker.add_task(
            {
                "name": "Reconciling Plex Live TV tuners (periodic)",
                "function": reconcile_plex_live_tv,
                "args": [app, None],
                "task_key": "reconcile-plex-live-tv-periodic",
                "execution_mode": "concurrent",
            },
            priority=24,
        )


@scheduler.scheduled_job("interval", id="do_15_mins", minutes=15, misfire_grace_time=120)
async def every_15_mins():
    async with app.app_context():
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.add_task(
            {
                "name": "Applying DVR recording rules",
                "function": apply_dvr_rules,
                "args": [app],
            },
            priority=19,
        )


@scheduler.scheduled_job("interval", id="do_5_mins_health_checks", minutes=5, misfire_grace_time=90)
async def every_5_mins_health_checks():
    async with app.app_context():
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.add_task(
            {
                "name": "Periodic channel stream health checks",
                "function": run_periodic_channel_stream_health_checks,
                "args": [app],
                "task_key": "periodic-channel-stream-health-checks",
                "execution_mode": "concurrent",
            },
            priority=22,
        )


@scheduler.scheduled_job("interval", id="do_60_mins", minutes=60, misfire_grace_time=300)
async def every_60_mins():
    async with app.app_context():
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.add_task(
            {
                "name": "Configuring TVH with global default",
                "function": configure_tvh_with_defaults,
                "args": [app],
            },
            priority=11,
        )
        await task_broker.add_task(
            {
                "name": "Configuring TVH networks",
                "function": update_tvh_networks,
                "args": [app],
            },
            priority=12,
        )
        await task_broker.add_task(
            {
                "name": "Configuring TVH channels",
                "function": update_tvh_channels,
                "args": [app],
            },
            priority=13,
        )
        await task_broker.add_task(
            {
                "name": "Configuring TVH muxes",
                "function": update_tvh_muxes,
                "args": [app],
            },
            priority=14,
        )
        await task_broker.add_task(
            {
                "name": "Triggering an update in TVH to fetch the latest XMLTV",
                "function": update_tvh_epg,
                "args": [app],
            },
            priority=30,
        )
        await task_broker.add_task(
            {
                "name": "Syncing all users to TVH",
                "function": sync_all_users_to_tvh,
                "args": [app.config["APP_CONFIG"]],
            },
            priority=17,
        )


@scheduler.scheduled_job("interval", id="hourly_playlist_check", hours=1, misfire_grace_time=900)
async def hourly_playlist_check():
    async with app.app_context():
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.add_task(
            {
                "name": "Updating all playlists",
                "function": update_playlists,
                "args": [app],
            },
            priority=100,
        )


@scheduler.scheduled_job("interval", id="hourly_epg_check", hours=1, misfire_grace_time=900)
async def hourly_epg_check():
    async with app.app_context():
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.add_task(
            {
                "name": "Updating all EPGs",
                "function": update_epgs,
                "args": [app],
            },
            priority=100,
        )


async def main():
    async with app.app_context():
        await load_stream_activity_state()
        await vod_cache_manager.import_existing_files()
        try:
            await audit_stream_event(
                None,
                "app_startup",
                "/tic-api/system/startup",
                details=f"pid={os.getpid()}",
            )
        except Exception:
            app.logger.exception("Failed to record app startup audit event")

    # Start scheduler inside a running event loop (required by APScheduler on Py 3.13+)
    app.logger.info("Starting scheduler...")
    scheduler.start()
    app.logger.info("Scheduler started.")

    try:
        # Start Quart server
        app.logger.info("Starting Quart server...")
        await app.run_task(host=config.flask_run_host, port=config.flask_run_port, debug=config.enable_app_debugging)
        app.logger.info("Quart server completed.")
    finally:
        async with app.app_context():
            await persist_stream_activity_state()


if __name__ == "__main__":
    asyncio.run(main())
