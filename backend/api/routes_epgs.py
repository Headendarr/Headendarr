#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from backend.api.tasks import TaskQueueBroker
from backend.auth import admin_auth_required
from backend.epgs import read_config_all_epgs, add_new_epg, read_config_one_epg, update_epg, delete_epg, \
    import_epg_data, read_channels_from_all_epgs
from backend.api import blueprint
from quart import request, jsonify, current_app


@blueprint.route('/tic-api/epgs/get', methods=['GET'])
@admin_auth_required
async def api_get_epgs_list():
    all_epg_configs = await read_config_all_epgs()
    return jsonify(
        {
            "success": True,
            "data":    all_epg_configs
        }
    )


@blueprint.route('/tic-api/epgs/settings/new', methods=['POST'])
@admin_auth_required
async def api_add_new_epg():
    json_data = await request.get_json()
    await add_new_epg(json_data)
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/epgs/settings/<epg_id>', methods=['GET'])
@admin_auth_required
async def api_get_epg_config(epg_id):
    try:
        epg_id = int(epg_id)
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "Invalid epg id"}), 400
    epg_config = await read_config_one_epg(epg_id)
    return jsonify(
        {
            "success": True,
            "data":    epg_config
        }
    )


@blueprint.route('/tic-api/epgs/settings/<epg_id>/save', methods=['POST'])
@admin_auth_required
async def api_set_epg_config(epg_id):
    json_data = await request.get_json()
    try:
        epg_id = int(epg_id)
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "Invalid epg id"}), 400
    await update_epg(epg_id, json_data)
    # TODO: Trigger an update of the cached EPG config
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/epgs/settings/<epg_id>/delete', methods=['DELETE'])
@admin_auth_required
async def api_delete_epg(epg_id):
    config = current_app.config['APP_CONFIG']
    try:
        epg_id = int(epg_id)
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "Invalid epg id"}), 400
    await delete_epg(config, epg_id)
    # TODO: Trigger an update of the cached EPG config
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/epgs/update/<epg_id>', methods=['POST'])
@admin_auth_required
async def api_update_epg(epg_id):
    config = current_app.config['APP_CONFIG']
    try:
        epg_id = int(epg_id)
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "Invalid epg id"}), 400
    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task({
        'name':     f'Update EPG - ID: {epg_id}',
        'function': import_epg_data,
        'args':     [config, epg_id],
    }, priority=20)
    return jsonify(
        {
            "success": True,
        }
    )


@blueprint.route('/tic-api/epgs/channels', methods=['GET'])
@admin_auth_required
async def api_get_all_epg_channels():
    config = current_app.config['APP_CONFIG']
    epgs_channels = await read_channels_from_all_epgs(config)
    return jsonify(
        {
            "success": True,
            "data":    epgs_channels
        }
    )
