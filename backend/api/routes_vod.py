#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import mimetypes
import xml.etree.ElementTree as ET
from urllib.parse import quote

from quart import Response, jsonify, request

from backend.api import blueprint
from backend.auth import (
    admin_auth_required,
    get_user_from_stream_key,
    mark_stream_key_usage,
    unauthorized_basic_auth_response,
)
from backend.vod import (
    create_vod_group,
    delete_vod_group,
    get_vod_page_state,
    list_upstream_vod_categories,
    list_vod_groups,
    require_vod_content_type,
    resolve_vod_http_library_path,
    user_has_vod_access,
    update_vod_group,
    user_can_access_vod_kind,
)

ignored_library_probe_suffixes = (
    "/.nomedia",
    "/video_ts.ifo",
    "/video_ts/video_ts.ifo",
    "/index.bdmv",
    "/bdmv/index.bdmv",
    "/index.bdm",
    "/bdmv/index.bdm",
)


@blueprint.route("/tic-api/vod/status", methods=["GET"])
@admin_auth_required
async def vod_status():
    return jsonify({"success": True, "data": await get_vod_page_state()})


@blueprint.route("/tic-api/vod/categories", methods=["GET"])
@admin_auth_required
async def vod_categories():
    try:
        content_type = require_vod_content_type(request.args.get("content_type"))
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
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
    try:
        content_type = require_vod_content_type(request.args.get("content_type"))
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    return jsonify({"success": True, "data": await list_vod_groups(content_type)})


@blueprint.route("/tic-api/vod/groups", methods=["POST"])
@admin_auth_required
async def create_vod_group_route():
    payload = await request.get_json(force=True, silent=True) or {}
    try:
        group_id = await create_vod_group(payload)
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
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


def _render_vod_http_directory(current_path: str, children: list[dict[str, object]]) -> str:
    base_path = str(current_path or "").strip() or "/"
    if not base_path.endswith("/"):
        base_path = f"{base_path}/"
    lines = [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        '<meta charset="utf-8">',
        f"<title>Index of {base_path}</title>",
        "</head>",
        "<body>",
        f"<h1>Index of {base_path}</h1>",
    ]
    if base_path != "/tic-api/library/":
        lines.append('<a href="../">../</a><br>')
    for child in children:
        name = str(child.get("name") or "")
        href = str(child.get("href") or child.get("path") or "").lstrip("/")
        if str(child.get("kind") or "") == "dir":
            if not href.endswith("/"):
                href = f"{href}/"
            if not name.endswith("/"):
                name = f"{name}/"
        lines.append(f'<a href="{quote(href, safe="/._-()[]")}">{name}</a><br>')
    lines.extend(["</body>", "</html>"])
    return "\n".join(lines)


def _category_children_for_content_dir(index_payload: dict[str, object], content_dir: str) -> list[dict[str, object]]:
    categories = [row for row in (index_payload.get("categories") or []) if isinstance(row, dict)]
    filtered = [
        row
        for row in categories
        if str(row.get("content_dir") or "") == content_dir
    ]
    filtered.sort(key=lambda row: (str(row.get("category_name") or "").casefold(), int(row.get("category_id") or 0)))
    return [
        {
            "name": str(row.get("category_slug") or row.get("category_name") or ""),
            "path": f"{row.get('category_slug')}/",
            "kind": "dir",
        }
        for row in filtered
    ]


def _vod_library_media_content_type(extension: str) -> str:
    guessed_type, _ = mimetypes.guess_type(f"file.{str(extension or '').strip('.')}")
    return guessed_type or "application/octet-stream"


def _is_unauthenticated_probe_path(subpath: str) -> bool:
    cleaned = str(subpath or "").strip().strip("/")
    if not cleaned:
        return False
    lower_path = cleaned.casefold()
    return lower_path.endswith(ignored_library_probe_suffixes)


def _vod_library_collection_children(index_payload: dict[str, object], resolved: dict[str, object], user) -> list[dict[str, object]]:
    node_type = str(resolved.get("type") or "")
    if node_type == "root":
        children = []
        if user_can_access_vod_kind(user, "movie"):
            movie_children = _category_children_for_content_dir(index_payload or {}, "Movies")
            if movie_children:
                children.append({"name": "Movies", "path": "Movies/", "kind": "dir"})
        if user_can_access_vod_kind(user, "series"):
            show_children = _category_children_for_content_dir(index_payload or {}, "Shows")
            if show_children:
                children.append({"name": "Shows", "path": "Shows/", "kind": "dir"})
        return children

    if node_type == "content_dir":
        content_dir = str(resolved.get("content_dir") or "")
        return _category_children_for_content_dir(index_payload or {}, content_dir)

    if node_type != "dir":
        return []

    node = resolved.get("node") or {}
    raw_children = list(node.get("children") or [])
    raw_children.sort(key=lambda child: (0 if child.get("kind") == "dir" else 1, str(child.get("name") or "").casefold()))
    children = []
    for child in raw_children:
        child_name = str(child.get("name") or "")
        child_kind = str(child.get("kind") or "")
        child_href = f"{child_name}/" if child_kind == "dir" else child_name
        children.append({**child, "href": child_href})
    return children


def _vod_library_propfind_href(path: str, child: dict[str, object] | None = None) -> str:
    base_path = str(path or "").strip() or "/"
    if child is None:
        if not base_path.endswith("/"):
            return f"{base_path}/"
        return base_path
    child_name = str(child.get("href") or child.get("path") or "").lstrip("/")
    href = f"{base_path.rstrip('/')}/{child_name}" if child_name else base_path
    if str(child.get("kind") or "") == "dir" and not href.endswith("/"):
        href = f"{href}/"
    return quote(href, safe="/._-()[]")


def _append_vod_library_prop(parent: ET.Element, request_path: str, child: dict[str, object] | None = None):
    response = ET.SubElement(parent, "{DAV:}response")
    ET.SubElement(response, "{DAV:}href").text = _vod_library_propfind_href(request_path, child)
    propstat = ET.SubElement(response, "{DAV:}propstat")
    prop = ET.SubElement(propstat, "{DAV:}prop")
    display_name = str(child.get("name") or "") if child is not None else request_path.rstrip("/").split("/")[-1] or "library"
    ET.SubElement(prop, "{DAV:}displayname").text = display_name
    resource_type = ET.SubElement(prop, "{DAV:}resourcetype")
    kind = str(child.get("kind") or "") if child is not None else "dir"
    if kind == "dir":
        ET.SubElement(resource_type, "{DAV:}collection")
        ET.SubElement(prop, "{DAV:}getcontenttype").text = "httpd/unix-directory"
    else:
        extension = str(child.get("extension") or "").strip(".") or "mp4"
        ET.SubElement(prop, "{DAV:}getcontenttype").text = _vod_library_media_content_type(extension)
    ET.SubElement(propstat, "{DAV:}status").text = "HTTP/1.1 200 OK"


def _build_vod_library_propfind_response(
    request_path: str, index_payload: dict[str, object], resolved: dict[str, object], user, depth: str
) -> str:
    multistatus = ET.Element("{DAV:}multistatus")
    _append_vod_library_prop(multistatus, request_path)
    if str(depth or "1").strip() == "0":
        return ET.tostring(multistatus, encoding="unicode", xml_declaration=True)
    for child in _vod_library_collection_children(index_payload, resolved, user):
        _append_vod_library_prop(multistatus, request_path, child)
    return ET.tostring(multistatus, encoding="unicode", xml_declaration=True)


def _vod_library_allowed_methods(node_type: str) -> str:
    methods = ["GET", "HEAD", "OPTIONS", "PROPFIND"]
    if node_type in {"movie_file", "episode_file"}:
        return ", ".join(methods)
    return ", ".join(methods)


@blueprint.route("/tic-api/library/", methods=["GET", "HEAD", "OPTIONS", "PROPFIND"])
@blueprint.route("/tic-api/library/<path:subpath>", methods=["GET", "HEAD", "OPTIONS", "PROPFIND"])
async def vod_http_library(subpath: str = ""):
    if _is_unauthenticated_probe_path(subpath):
        return Response("Not found", status=404, content_type="text/plain; charset=utf-8")

    user = await get_user_from_stream_key()
    if not user or not user.is_active:
        return unauthorized_basic_auth_response(realm="VOD Library")
    if not user_has_vod_access(user):
        return Response("Forbidden", status=403, content_type="text/plain; charset=utf-8")
    await mark_stream_key_usage(user)

    index_payload, resolved = await resolve_vod_http_library_path(subpath)
    if resolved is None:
        return Response("Not found", status=404, content_type="text/plain; charset=utf-8")

    node_type = str(resolved.get("type") or "")
    headers = {
        "DAV": "1",
        "Allow": _vod_library_allowed_methods(node_type),
        "Accept-Ranges": "bytes",
    }
    if request.method == "OPTIONS":
        return Response("", headers=headers)

    if node_type in {"content_dir", "dir"}:
        content_dir = str(resolved.get("content_dir") or "")
        if content_dir:
            content_kind = "movie" if content_dir == "Movies" else "series"
            if not user_can_access_vod_kind(user, content_kind):
                return Response("Forbidden", status=403, content_type="text/plain; charset=utf-8")

    category = resolved.get("category") or {}
    content_type = str(category.get("content_type") or "")
    if content_type and not user_can_access_vod_kind(user, content_type):
        return Response("Forbidden", status=403, content_type="text/plain; charset=utf-8")

    if request.method == "PROPFIND":
        body = _build_vod_library_propfind_response(
            request.path,
            index_payload or {},
            resolved,
            user,
            request.headers.get("Depth", "1"),
        )
        return Response(body, status=207, headers=headers, content_type='application/xml; charset="utf-8"')

    if node_type in {"root", "content_dir", "dir"}:
        children = _vod_library_collection_children(index_payload or {}, resolved, user)
        body = _render_vod_http_directory(request.path, children)
        return Response(body, headers=headers, content_type="text/html; charset=utf-8")

    if node_type in {"movie_file", "episode_file"}:
        node = resolved.get("node") or {}
        extension = str(node.get("extension") or "").strip(".") or "mp4"
        if request.method == "HEAD":
            return Response("", headers=headers, content_type=_vod_library_media_content_type(extension))
        if node_type == "movie_file":
            from backend.api.routes_connections_xc import xc_movie_stream

            return await xc_movie_stream(user.username, user.streaming_key, str(node.get("item_id") or "0"), extension)

        from backend.api.routes_connections_xc import xc_series_episode_stream

        return await xc_series_episode_stream(
            user.username,
            user.streaming_key,
            str(node.get("episode_id") or "0"),
            extension,
        )

    return Response("Not found", status=404, content_type="text/plain; charset=utf-8")
