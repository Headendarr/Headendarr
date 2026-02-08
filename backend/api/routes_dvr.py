#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio

from quart import request, jsonify, current_app

from backend.api import blueprint
from backend.auth import streamer_or_admin_required
from backend.api.tasks import TaskQueueBroker, reconcile_dvr_recordings, apply_dvr_rules
from backend.dvr import list_recordings, list_rules, create_recording, cancel_recording, create_rule, delete_rule


@blueprint.route('/tic-api/recordings', methods=['GET'])
@streamer_or_admin_required
async def api_list_recordings():
    records = await list_recordings()
    return jsonify({"success": True, "data": records})


@blueprint.route('/tic-api/recordings/poll', methods=['GET'])
@streamer_or_admin_required
async def api_poll_recordings():
    wait = request.args.get('wait', '0')
    timeout = request.args.get('timeout', '0')
    try:
        wait = int(wait)
    except ValueError:
        wait = 0
    try:
        timeout = int(timeout)
    except ValueError:
        timeout = 0

    data = await list_recordings()
    if wait and timeout:
        start = asyncio.get_event_loop().time()
        while True:
            await asyncio.sleep(1)
            updated = await list_recordings()
            if updated != data:
                data = updated
                break
            if (asyncio.get_event_loop().time() - start) >= timeout:
                break
    return jsonify({"success": True, "data": data}), 200


@blueprint.route('/tic-api/recordings', methods=['POST'])
@streamer_or_admin_required
async def api_create_recording():
    payload = await request.get_json()
    channel_id = payload.get("channel_id")
    title = payload.get("title")
    start_ts = payload.get("start_ts")
    stop_ts = payload.get("stop_ts")
    description = payload.get("description")
    epg_programme_id = payload.get("epg_programme_id")

    if not channel_id or not start_ts or not stop_ts:
        return jsonify({"success": False, "message": "Missing channel_id/start_ts/stop_ts"}), 400

    recording_id = await create_recording(
        channel_id=channel_id,
        title=title,
        start_ts=int(start_ts),
        stop_ts=int(stop_ts),
        description=description,
        epg_programme_id=epg_programme_id,
    )

    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task({
        'name': f'Reconcile DVR recordings',
        'function': reconcile_dvr_recordings,
        'args': [current_app],
    }, priority=20)

    return jsonify({"success": True, "recording_id": recording_id})


@blueprint.route('/tic-api/recordings/<int:recording_id>/cancel', methods=['POST'])
@streamer_or_admin_required
async def api_cancel_recording(recording_id):
    ok = await cancel_recording(recording_id)
    if not ok:
        return jsonify({"success": False, "message": "Recording not found"}), 404
    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task({
        'name': f'Reconcile DVR recordings',
        'function': reconcile_dvr_recordings,
        'args': [current_app],
    }, priority=20)
    return jsonify({"success": True})


@blueprint.route('/tic-api/recording-rules', methods=['GET'])
@streamer_or_admin_required
async def api_list_recording_rules():
    rules = await list_rules()
    return jsonify({"success": True, "data": rules})


@blueprint.route('/tic-api/recording-rules', methods=['POST'])
@streamer_or_admin_required
async def api_create_recording_rule():
    payload = await request.get_json()
    channel_id = payload.get("channel_id")
    title_match = payload.get("title_match")
    lookahead_days = payload.get("lookahead_days", 7)
    if not channel_id or not title_match:
        return jsonify({"success": False, "message": "Missing channel_id/title_match"}), 400
    rule_id = await create_rule(channel_id, title_match, lookahead_days=int(lookahead_days))
    task_broker = await TaskQueueBroker.get_instance()
    await task_broker.add_task({
        'name': f'Applying DVR recording rules',
        'function': apply_dvr_rules,
        'args': [current_app],
    }, priority=19)
    await task_broker.add_task({
        'name': f'Reconcile DVR recordings',
        'function': reconcile_dvr_recordings,
        'args': [current_app],
    }, priority=20)
    return jsonify({"success": True, "rule_id": rule_id})


@blueprint.route('/tic-api/recording-rules/<int:rule_id>', methods=['DELETE'])
@streamer_or_admin_required
async def api_delete_recording_rule(rule_id):
    ok = await delete_rule(rule_id)
    if not ok:
        return jsonify({"success": False, "message": "Rule not found"}), 404
    return jsonify({"success": True})
