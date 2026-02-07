#!/usr/bin/env python3
# -*- coding:utf-8 -*-

from backend.api.tasks import scheduler, update_playlists, map_new_tvh_services, update_epgs, rebuild_custom_epg, \
    update_tvh_muxes, configure_tvh_with_defaults, update_tvh_channels, update_tvh_networks, update_tvh_epg, \
    TaskQueueBroker
from backend import create_app, config
import asyncio

# Create app
app = create_app()
if config.enable_app_debugging:
    app.logger.info(' DEBUGGING   = ' + str(config.enable_app_debugging))
    app.logger.debug('DBMS        = ' + config.sqlalchemy_database_uri)
    app.logger.debug('ASSETS_ROOT = ' + config.assets_root)

task_logger = app.logger.getChild('tasks')
TaskQueueBroker.initialize(task_logger)


@scheduler.scheduled_job('interval', id='background_tasks', seconds=10)
async def background_tasks():
    async with app.app_context():
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.execute_tasks()


@scheduler.scheduled_job('interval', id='do_5_mins', minutes=5, misfire_grace_time=60)
async def every_5_mins():
    async with app.app_context():
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.add_task({
            'name':     'Mapping all TVH services',
            'function': map_new_tvh_services,
            'args':     [app],
        }, priority=10)


@scheduler.scheduled_job('interval', id='do_60_mins', minutes=60, misfire_grace_time=300)
async def every_60_mins():
    async with app.app_context():
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.add_task({
            'name':     'Configuring TVH with global default',
            'function': configure_tvh_with_defaults,
            'args':     [app],
        }, priority=11)
        await task_broker.add_task({
            'name':     'Configuring TVH networks',
            'function': update_tvh_networks,
            'args':     [app],
        }, priority=12)
        await task_broker.add_task({
            'name':     'Configuring TVH channels',
            'function': update_tvh_channels,
            'args':     [app],
        }, priority=13)
        await task_broker.add_task({
            'name':     'Configuring TVH muxes',
            'function': update_tvh_muxes,
            'args':     [app],
        }, priority=14)
        await task_broker.add_task({
            'name':     'Triggering an update in TVH to fetch the latest XMLTV',
            'function': update_tvh_epg,
            'args':     [app],
        }, priority=30)


@scheduler.scheduled_job('cron', id='do_job_twice_a_day', hour='0/12', minute=1, misfire_grace_time=900)
async def every_12_hours():
    async with app.app_context():
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.add_task({
            'name':     f'Updating all playlists',
            'function': update_playlists,
            'args':     [app],
        }, priority=100)
        await task_broker.add_task({
            'name':     f'Updating all EPGs',
            'function': update_epgs,
            'args':     [app],
        }, priority=100)
        await task_broker.add_task({
            'name':     'Recreating static XMLTV file',
            'function': rebuild_custom_epg,
            'args':     [app],
        }, priority=200)


async def main():
    # Start scheduler inside a running event loop (required by APScheduler on Py 3.13+)
    app.logger.info("Starting scheduler...")
    scheduler.start()
    app.logger.info("Scheduler started.")

    # Start Quart server
    app.logger.info("Starting Quart server...")
    await app.run_task(host=config.flask_run_host, port=config.flask_run_port,
                       debug=config.enable_app_debugging)
    app.logger.info("Quart server completed.")


if __name__ == "__main__":
    asyncio.run(main())
