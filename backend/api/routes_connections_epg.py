#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from quart import Response, current_app, request

from backend.api import blueprint
from backend.auth import stream_key_required, audit_stream_event
from backend.epgs import render_xmltv_payload


async def build_xmltv_response():
    config = current_app.config["APP_CONFIG"]
    base_url = request.url_root.rstrip("/")
    payload = render_xmltv_payload(config, base_url)
    return Response(payload, mimetype="application/xml")


@blueprint.route("/tic-api/epg/xmltv.xml")
@stream_key_required
async def serve_epg_xmltv():
    await audit_stream_event(request._stream_user, "epg_xml", request.path)
    return await build_xmltv_response()
