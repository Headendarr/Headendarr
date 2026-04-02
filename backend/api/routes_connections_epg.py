#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import unicodedata

from quart import Response, current_app, request

from backend.api import blueprint
from backend.auth import audit_stream_event, get_request_stream_user, stream_key_required
from backend.epgs import render_xmltv_payload
from backend.url_resolver import get_request_base_url


def _strip_unicode_format_chars(value: str) -> str:
    return "".join(ch for ch in str(value or "") if unicodedata.category(ch) != "Cf")


async def build_xmltv_response(sanitise_unicode: bool = False):
    config = current_app.config["APP_CONFIG"]
    base_url = get_request_base_url(request)
    payload = render_xmltv_payload(config, base_url)
    if sanitise_unicode:
        payload = _strip_unicode_format_chars(payload)
    return Response(payload, mimetype="application/xml")


@blueprint.route("/tic-api/epg/xmltv.xml")
@stream_key_required
async def serve_epg_xmltv():
    await audit_stream_event(get_request_stream_user(), "epg_xml", request.path)
    return await build_xmltv_response()
