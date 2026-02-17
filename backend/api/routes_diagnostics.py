#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from quart import jsonify, request, current_app
from backend.api import blueprint
from backend.stream_diagnostics import start_probe, get_probe_status, delete_probe
from backend.auth import admin_auth_required
from backend.streaming import append_stream_key, is_tic_stream_url


@blueprint.route('/tic-api/diagnostics/stream/test', methods=['POST'])
@admin_auth_required
async def test_stream():
    data = await request.get_json()
    stream_url = data.get('stream_url')
    bypass_proxies = data.get('bypass_proxies', False)
    if not stream_url:
        return jsonify({"success": False, "message": "Missing stream_url"}), 400

    user = getattr(request, "_current_user", None)
    stream_key = getattr(user, "streaming_key", None)
    app_config = current_app.config.get("APP_CONFIG")
    instance_id = app_config.ensure_instance_id() if app_config else None
    if stream_key and is_tic_stream_url(stream_url, instance_id=instance_id):
        stream_url = append_stream_key(stream_url, stream_key=stream_key)

    task_id = await start_probe(
        stream_url,
        bypass_proxies=bypass_proxies,
        request_host_url=request.host_url,
    )
    return jsonify({"success": True, "task_id": task_id})


@blueprint.route('/tic-api/diagnostics/stream/test/<task_id>', methods=['GET'])
@admin_auth_required
async def get_test_status(task_id):
    status = get_probe_status(task_id)
    if not status:
        return jsonify({"success": False, "message": "Task not found"}), 404
    return jsonify({"success": True, "data": status})


@blueprint.route('/tic-api/diagnostics/stream/test/<task_id>', methods=['DELETE'])
@admin_auth_required
async def delete_test(task_id):
    delete_probe(task_id)
    return jsonify({"success": True})
