#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from quart import jsonify, request, current_app
from backend.api import blueprint
from backend.stream_diagnostics import start_probe, get_probe_status, delete_probe
from backend.auth import admin_auth_required, get_request_user
from backend.http_headers import parse_headers_json, sanitise_headers
from backend.models import ChannelSource, Session
from backend.streaming import append_stream_key, is_tic_stream_url
from backend.url_resolver import get_request_origin
from backend.channel_stream_health import apply_stream_probe_result_to_source
from sqlalchemy import select
from sqlalchemy.orm import joinedload


@blueprint.route("/tic-api/diagnostics/stream/test", methods=["POST"])
@admin_auth_required
async def test_stream():
    data = await request.get_json()
    stream_url = data.get("stream_url")
    bypass_proxies = data.get("bypass_proxies", False)
    preferred_user_agent = (data.get("user_agent") or "").strip() or None
    preferred_headers = sanitise_headers(data.get("headers") or {})
    channel_source_id = data.get("channel_source_id")
    if not stream_url:
        return jsonify({"success": False, "message": "Missing stream_url"}), 400

    user = get_request_user()
    stream_key = getattr(user, "streaming_key", None)
    app_config = current_app.config.get("APP_CONFIG")
    instance_id = app_config.ensure_instance_id() if app_config else None
    if stream_key and is_tic_stream_url(stream_url, instance_id=instance_id):
        stream_url = append_stream_key(stream_url, stream_key=stream_key)

    source_id = None
    source = None
    if channel_source_id is not None:
        try:
            parsed_source_id = int(channel_source_id)
            if parsed_source_id > 0:
                source_id = parsed_source_id
        except Exception:
            source_id = None

    if source_id is not None:
        async with Session() as session:
            result = await session.execute(
                select(ChannelSource)
                .options(joinedload(ChannelSource.playlist))
                .where(ChannelSource.id == source_id)
            )
            source = result.scalar_one_or_none()
        playlist = getattr(source, "playlist", None) if source is not None else None
        if playlist is not None:
            if not preferred_user_agent:
                preferred_user_agent = str(getattr(playlist, "user_agent", "") or "").strip() or None
            if not preferred_headers:
                try:
                    preferred_headers = sanitise_headers(
                        parse_headers_json(getattr(playlist, "hls_proxy_headers", None))
                    )
                except ValueError:
                    preferred_headers = {}

    async def _on_probe_complete(probe):
        if source_id is None:
            return
        await apply_stream_probe_result_to_source(
            source_id,
            probe,
            health_check_type="manual",
            tested_stream_url=stream_url,
            require_exact_source_url_match=True,
        )

    task_id = await start_probe(
        stream_url,
        bypass_proxies=bypass_proxies,
        request_host_url=f"{get_request_origin(request)}/",
        preferred_user_agent=preferred_user_agent,
        preferred_headers=preferred_headers,
        on_complete=_on_probe_complete,
    )
    return jsonify({"success": True, "task_id": task_id})


@blueprint.route("/tic-api/diagnostics/stream/test/<task_id>", methods=["GET"])
@admin_auth_required
async def get_test_status(task_id):
    status = get_probe_status(task_id)
    if not status:
        return jsonify({"success": False, "message": "Task not found"}), 404
    return jsonify({"success": True, "data": status})


@blueprint.route("/tic-api/diagnostics/stream/test/<task_id>", methods=["DELETE"])
@admin_auth_required
async def delete_test(task_id):
    delete_probe(task_id)
    return jsonify({"success": True})
