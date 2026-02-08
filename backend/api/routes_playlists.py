#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import os

from backend.api.tasks import TaskQueueBroker
from backend.auth import admin_auth_required
from backend.channels import queue_background_channel_update_tasks
from backend.playlists import read_config_all_playlists, add_new_playlist, read_config_one_playlist, update_playlist, \
    delete_playlist, import_playlist_data, read_stream_details_from_all_playlists, probe_playlist_stream, \
    read_filtered_stream_details_from_all_playlists, get_playlist_groups, publish_playlist_networks, \
    delete_playlist_network_in_tvh

from backend.api import blueprint
from quart import request, jsonify, current_app

frontend_dir = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), 'frontend')
static_assets = os.path.join(frontend_dir, 'dist', 'spa')


@blueprint.route('/tic-api/playlists/get', methods=['GET'])
@admin_auth_required
async def api_get_playlists_list():
    config = current_app.config['APP_CONFIG']
    all_playlist_configs = await read_config_all_playlists(config)
    return jsonify(
        {
            "success": True,
            "data":    all_playlist_configs
        }
    )


@blueprint.route('/tic-api/playlists/new', methods=['POST'])
@admin_auth_required
async def api_add_new_playlist():
    json_data = await request.get_json()
    config = current_app.config['APP_CONFIG']
    playlist_id = await add_new_playlist(config, json_data)
    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task({
        'name':     f'Publish playlist networks - add {playlist_id}',
        'function': publish_playlist_networks,
        'args':     [config],
    }, priority=20)
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/playlists/settings/<playlist_id>', methods=['GET'])
@admin_auth_required
async def api_get_playlist_config(playlist_id):
    config = current_app.config['APP_CONFIG']
    playlist_config = await read_config_one_playlist(config, playlist_id)
    return jsonify(
        {
            "success": True,
            "data":    playlist_config
        }
    )


@blueprint.route('/tic-api/playlists/settings/<playlist_id>/save', methods=['POST'])
@admin_auth_required
async def api_set_config_playlists(playlist_id):
    json_data = await request.get_json()
    config = current_app.config['APP_CONFIG']
    await update_playlist(config, playlist_id, json_data)
    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task({
        'name':     f'Publish playlist networks - update {playlist_id}',
        'function': publish_playlist_networks,
        'args':     [config],
    }, priority=20)
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/playlists/<playlist_id>/delete', methods=['DELETE'])
@admin_auth_required
async def api_delete_playlist(playlist_id):
    config = current_app.config['APP_CONFIG']
    net_uuid = await delete_playlist(config, playlist_id)
    if net_uuid:
        task_broker = await TaskQueueBroker.get_instance()
        await task_broker.add_task({
            'name':     f'Delete playlist network - {playlist_id}',
            'function': delete_playlist_network_in_tvh,
            'args':     [config, net_uuid],
        }, priority=20)
    await queue_background_channel_update_tasks(config)
    return jsonify(
        {
            "success": True
        }
    )


@blueprint.route('/tic-api/playlists/update/<playlist_id>', methods=['POST'])
@admin_auth_required
async def api_update_playlist(playlist_id):
    config = current_app.config['APP_CONFIG']
    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task({
        'name':     f'Update playlist - ID: {playlist_id}',
        'function': import_playlist_data,
        'args':     [config, playlist_id],
    }, priority=20)
    return jsonify(
        {
            "success": True,
        }
    )


@blueprint.route('/tic-api/playlists/streams', methods=['POST'])
@admin_auth_required
async def api_get_filtered_playlist_streams():
    json_data = await request.get_json()
    results = read_filtered_stream_details_from_all_playlists(json_data)
    return jsonify(
        {
            "success": True,
            "data":    results
        }
    )


@blueprint.route('/tic-api/playlists/streams/all', methods=['GET'])
@admin_auth_required
async def api_get_all_playlist_streams():
    playlist_streams = await read_stream_details_from_all_playlists()
    return jsonify(
        {
            "success": True,
            "data":    playlist_streams
        }
    )


@blueprint.route('/tic-api/playlists/stream/probe/<playlist_stream_id>', methods=['GET'])
@admin_auth_required
async def api_probe_playlist_stream(playlist_stream_id):
    probe = await probe_playlist_stream(playlist_stream_id)
    return jsonify(
        {
            "success": True,
            "data":    probe
        }
    )

@blueprint.route('/tic-api/playlists/groups', methods=['POST'])
@admin_auth_required
async def api_get_playlist_groups():
    json_data = await request.get_json()
    playlist_id = json_data.get('playlist_id')
    
    if not playlist_id:
        return jsonify({
            "success": False,
            "message": "Playlist ID is required"
        }), 400
    
    config = current_app.config['APP_CONFIG']
    
    # Get search/filter parameters
    start = json_data.get('start', 0)
    length = json_data.get('length', 10)
    search_value = json_data.get('search_value', '')
    order_by = json_data.get('order_by', 'name')
    order_direction = json_data.get('order_direction', 'asc')
    
    # This function needs to be implemented in the playlists module
    # It should fetch all groups from a playlist with filtering/sorting/pagination
    groups_data = await get_playlist_groups(
        config, 
        playlist_id, 
        start=start, 
        length=length, 
        search_value=search_value,
        order_by=order_by,
        order_direction=order_direction
    )
    
    return jsonify({
        "success": True,
        "data": groups_data
    })
