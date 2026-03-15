#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from quart import jsonify, request

from backend.api import blueprint
from backend.auth import admin_auth_required
from backend.vod import (
    clean_vod_content_type,
    create_vod_group,
    delete_vod_group,
    get_vod_page_state,
    list_upstream_vod_categories,
    list_vod_groups,
    update_vod_group,
)


@blueprint.route("/tic-api/vod/status", methods=["GET"])
@admin_auth_required
async def vod_status():
    return jsonify({"success": True, "data": await get_vod_page_state()})


@blueprint.route("/tic-api/vod/categories", methods=["GET"])
@admin_auth_required
async def vod_categories():
    content_type = clean_vod_content_type(request.args.get("content_type"))
    playlist_id = request.args.get("playlist_id")
    return jsonify(
        {
            "success": True,
            "data": await list_upstream_vod_categories(
                content_type,
                source_playlist_id=int(playlist_id) if str(playlist_id or "").isdigit() else None,
            ),
        }
    )


@blueprint.route("/tic-api/vod/groups", methods=["GET"])
@admin_auth_required
async def vod_groups():
    content_type = clean_vod_content_type(request.args.get("content_type"))
    return jsonify({"success": True, "data": await list_vod_groups(content_type)})


@blueprint.route("/tic-api/vod/groups", methods=["POST"])
@admin_auth_required
async def create_vod_group_route():
    payload = await request.get_json(force=True, silent=True) or {}
    group_id = await create_vod_group(payload)
    return jsonify({"success": True, "group_id": group_id})


@blueprint.route("/tic-api/vod/groups/<int:group_id>", methods=["PUT"])
@admin_auth_required
async def update_vod_group_route(group_id):
    payload = await request.get_json(force=True, silent=True) or {}
    ok = await update_vod_group(int(group_id), payload)
    if not ok:
        return jsonify({"success": False, "message": "VOD group not found"}), 404
    return jsonify({"success": True})


@blueprint.route("/tic-api/vod/groups/<int:group_id>", methods=["DELETE"])
@admin_auth_required
async def delete_vod_group_route(group_id):
    ok = await delete_vod_group(int(group_id))
    if not ok:
        return jsonify({"success": False, "message": "VOD group not found"}), 404
    return jsonify({"success": True})
