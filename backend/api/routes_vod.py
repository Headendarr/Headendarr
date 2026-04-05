#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import mimetypes
import re
import xml.etree.ElementTree as ET
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse

from quart import Response, jsonify, request

from backend.api import blueprint
from backend.auth import (
    admin_auth_required,
    authenticate_stream_request,
    forbidden_response,
    get_user_from_token,
    mark_stream_key_usage,
    rate_limited_basic_auth_response,
    unauthorized_basic_auth_response,
    user_auth_required,
)
from backend.stream_profiles import SUPPORTED_STREAM_PROFILES, parse_stream_profile_request
from backend.url_resolver import get_request_base_url
from backend.vod import (
    VOD_KIND_MOVIE,
    VOD_KIND_SERIES,
    build_curated_episode_browser_playback,
    build_curated_movie_browser_playback,
    build_upstream_browser_playback,
    create_vod_group,
    delete_vod_group,
    fetch_curated_library_item_details,
    fetch_upstream_vod_item_details,
    get_library_page_state,
    get_vod_page_state,
    list_curated_library_categories,
    list_curated_library_items,
    list_upstream_vod_items,
    list_upstream_vod_categories,
    list_vod_groups,
    require_vod_content_type,
    resolve_vod_http_library_path,
    update_vod_group,
    user_can_access_vod_kind,
    user_has_vod_access,
)
from backend.users import user_has_admin_role
from backend.utils import convert_to_int, int_or_none

ignored_library_probe_suffixes = (
    "/.nomedia",
    "/video_ts.ifo",
    "/video_ts/video_ts.ifo",
    "/index.bdmv",
    "/bdmv/index.bdmv",
    "/index.bdm",
    "/bdmv/index.bdm",
)

_CURATED_CSO_VOD_PREVIEW_RE = re.compile(r"^/tic-api/cso/vod/(?P<content_type>movie|series)/(?P<item_id>\d+)$")
_UPSTREAM_CSO_VOD_PREVIEW_RE = re.compile(
    r"^/tic-api/cso/vod/upstream/(?P<source_id>\d+)/(?P<content_type>movie|series)/(?P<item_id>\d+)"
    r"(?:/(?P<upstream_episode_id>[^/]+))?$"
)


def _build_preview_candidate(
    url: str,
    stream_type: str,
    source_id: int | None = None,
    priority: int | None = 0,
    source_resolution=None,
    duration_seconds=None,
) -> dict[str, object]:
    resolved_priority = 0 if priority is None else int(priority)
    candidate = {
        "url": url,
        "stream_type": stream_type,
        "source_id": source_id,
        "priority": resolved_priority,
    }
    if source_resolution:
        candidate["source_resolution"] = source_resolution
    if duration_seconds is not None:
        candidate["duration_seconds"] = duration_seconds
    return candidate


def _build_preview_response(candidates: list[dict[str, object]]) -> dict[str, object]:
    return {"success": True, "candidates": candidates}


def _resolved_preview_profile(default_profile: str = "") -> str:
    requested = parse_stream_profile_request(request.args.get("profile"))
    if requested["profile_id"] in SUPPORTED_STREAM_PROFILES:
        return requested["raw"] or requested["profile_id"]
    return str(default_profile or "").strip().lower()


def _build_cso_vod_preview_url(
    request_base_url: str,
    user,
    stream_type: str,
    item_id: int,
    profile: str = "",
    source_id: int | None = None,
    upstream_episode_id: str | None = None,
    container_extension: str | None = None,
) -> str:
    base_path = f"/tic-api/cso/vod/{stream_type}/{int(item_id)}"
    if source_id is not None:
        base_path = f"/tic-api/cso/vod/upstream/{int(source_id)}/{stream_type}/{int(item_id)}"
        if stream_type == VOD_KIND_SERIES:
            resolved_episode_id = str(upstream_episode_id or "").strip()
            if resolved_episode_id:
                base_path = f"{base_path}/{quote(resolved_episode_id, safe='')}"
    query_items = {
        "stream_key": str(getattr(user, "streaming_key", "") or ""),
    }
    if profile:
        query_items["profile"] = str(profile)
    if container_extension:
        query_items["container_extension"] = str(container_extension).strip().lstrip(".").lower()
    return f"{request_base_url.rstrip('/')}{base_path}?{urlencode(query_items)}"


def _parse_vod_preview_reference(preview_url: str) -> dict[str, object] | None:
    parsed = urlparse(str(preview_url or "").strip())
    path = str(parsed.path or "").strip()
    query = parse_qs(parsed.query or "", keep_blank_values=False)

    match = _CURATED_CSO_VOD_PREVIEW_RE.match(path)
    if match:
        return {
            "mode": "curated",
            "content_type": match.group("content_type"),
            "item_id": int(match.group("item_id")),
            "container_extension": str((query.get("container_extension") or [""])[0] or "").strip(),
        }

    match = _UPSTREAM_CSO_VOD_PREVIEW_RE.match(path)
    if match:
        return {
            "mode": "upstream",
            "source_id": int(match.group("source_id")),
            "content_type": match.group("content_type"),
            "item_id": int(match.group("item_id")),
            "upstream_episode_id": unquote(str(match.group("upstream_episode_id") or "").strip()),
            "container_extension": str((query.get("container_extension") or [""])[0] or "").strip(),
        }

    return None


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


@blueprint.route("/tic-api/vod/browser/items", methods=["GET"])
@admin_auth_required
async def vod_browser_items():
    try:
        content_type = require_vod_content_type(request.args.get("content_type"))
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400

    playlist_id = request.args.get("playlist_id")
    category_id = request.args.get("category_id")
    payload = await list_upstream_vod_items(
        content_type,
        source_playlist_id=int_or_none(playlist_id),
        upstream_category_id=int_or_none(category_id),
        search_query=request.args.get("search"),
        offset=convert_to_int(request.args.get("offset"), default=0),
        limit=convert_to_int(request.args.get("limit"), default=50),
    )
    return jsonify({"success": True, "data": payload})


@blueprint.route("/tic-api/vod/browser/details/<int:item_id>", methods=["GET"])
@admin_auth_required
async def vod_browser_item_details(item_id: int):
    try:
        content_type = require_vod_content_type(request.args.get("content_type"))
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400

    payload = await fetch_upstream_vod_item_details(content_type, int(item_id))
    if payload is None:
        return jsonify({"success": False, "message": "VOD item not found"}), 404
    return jsonify({"success": True, "data": payload})


@blueprint.route("/tic-api/vod/movie/<int:item_id>/preview", methods=["GET"])
@user_auth_required
async def curated_movie_preview(item_id: int):
    user = await get_user_from_token()
    if not user or not getattr(user, "streaming_key", None):
        return jsonify({"success": False, "message": "Streaming key missing"}), 400
    payload = await build_curated_movie_browser_playback(user, int(item_id), allow_probe=False, background_probe=True)
    if payload is None:
        return jsonify({"success": False, "message": "VOD item not found"}), 404
    if not payload.get("success"):
        return jsonify({"success": False, "message": payload.get("message") or "Playback unavailable"}), int(
            payload.get("status_code") or 404
        )
    profile = _resolved_preview_profile()
    preview_url = _build_cso_vod_preview_url(
        get_request_base_url(request),
        user,
        VOD_KIND_MOVIE,
        int(item_id),
        profile=profile,
    )
    return jsonify(
        _build_preview_response(
            [
                _build_preview_candidate(
                    url=preview_url,
                    stream_type=payload.get("stream_type") or "auto",
                    source_resolution=payload.get("source_resolution") or {},
                    duration_seconds=payload.get("duration_seconds"),
                )
            ]
        )
    )


@blueprint.route("/tic-api/vod/series/<int:item_id>/preview", methods=["GET"])
@user_auth_required
async def curated_series_preview(item_id: int):
    user = await get_user_from_token()
    if not user or not getattr(user, "streaming_key", None):
        return jsonify({"success": False, "message": "Streaming key missing"}), 400
    payload = await build_curated_episode_browser_playback(user, int(item_id), allow_probe=False, background_probe=True)
    if payload is None:
        return jsonify({"success": False, "message": "Episode not found"}), 404
    if not payload.get("success"):
        return jsonify({"success": False, "message": payload.get("message") or "Playback unavailable"}), int(
            payload.get("status_code") or 404
        )
    profile = _resolved_preview_profile()
    preview_url = _build_cso_vod_preview_url(
        get_request_base_url(request),
        user,
        VOD_KIND_SERIES,
        int(item_id),
        profile=profile,
    )
    return jsonify(
        _build_preview_response(
            [
                _build_preview_candidate(
                    url=preview_url,
                    stream_type=payload.get("stream_type") or "auto",
                    source_resolution=payload.get("source_resolution") or {},
                    duration_seconds=payload.get("duration_seconds"),
                )
            ]
        )
    )


@blueprint.route("/tic-api/vod/upstream/movie/<int:item_id>/preview", methods=["GET"])
@admin_auth_required
async def upstream_movie_preview(item_id: int):
    user = await get_user_from_token()
    if not user or not getattr(user, "streaming_key", None):
        return jsonify({"success": False, "message": "Streaming key missing"}), 400
    payload = await build_upstream_browser_playback(
        int(item_id), VOD_KIND_MOVIE, allow_probe=False, background_probe=True
    )
    if not payload.get("success"):
        return jsonify({"success": False, "message": payload.get("message") or "Playback unavailable"}), int(
            payload.get("status_code") or 404
        )
    profile = _resolved_preview_profile()
    source_id = int(payload.get("source_id") or 0)
    source_item_id = int(payload.get("source_item_id") or 0)
    if source_id <= 0 or source_item_id <= 0:
        return jsonify({"success": False, "message": "Playback source unavailable"}), 404
    preview_url = _build_cso_vod_preview_url(
        get_request_base_url(request),
        user,
        VOD_KIND_MOVIE,
        source_item_id,
        profile=profile,
        source_id=source_id,
    )
    return jsonify(
        _build_preview_response(
            [
                _build_preview_candidate(
                    url=preview_url,
                    stream_type=payload.get("stream_type") or "auto",
                    source_id=source_id,
                    source_resolution=payload.get("source_resolution") or {},
                    duration_seconds=payload.get("duration_seconds"),
                )
            ]
        )
    )


@blueprint.route("/tic-api/vod/upstream/series/<int:item_id>/<upstream_episode_id>/preview", methods=["GET"])
@admin_auth_required
async def upstream_series_preview(item_id: int, upstream_episode_id: str):
    user = await get_user_from_token()
    if not user or not getattr(user, "streaming_key", None):
        return jsonify({"success": False, "message": "Streaming key missing"}), 400
    container_extension = request.args.get("container_extension")
    resolved_episode_id = convert_to_int(upstream_episode_id, default=0)
    if resolved_episode_id <= 0:
        return jsonify({"success": False, "message": "Series episode id is required"}), 400
    payload = await build_upstream_browser_playback(
        int(item_id),
        VOD_KIND_SERIES,
        upstream_episode_id=upstream_episode_id,
        container_extension=container_extension,
        allow_probe=False,
        background_probe=True,
    )
    if not payload.get("success"):
        return jsonify({"success": False, "message": payload.get("message") or "Playback unavailable"}), int(
            payload.get("status_code") or 404
        )
    source_id = int(payload.get("source_id") or 0)
    source_item_id = int(payload.get("source_item_id") or 0)
    if source_id <= 0 or source_item_id <= 0:
        return jsonify({"success": False, "message": "Playback source unavailable"}), 404
    profile = _resolved_preview_profile()
    preview_url = _build_cso_vod_preview_url(
        get_request_base_url(request),
        user,
        VOD_KIND_SERIES,
        source_item_id,
        profile=profile,
        source_id=source_id,
        upstream_episode_id=str(resolved_episode_id),
        container_extension=container_extension,
    )
    return jsonify(
        _build_preview_response(
            [
                _build_preview_candidate(
                    url=preview_url,
                    stream_type=payload.get("stream_type") or "auto",
                    source_id=source_id,
                    source_resolution=payload.get("source_resolution") or {},
                    duration_seconds=payload.get("duration_seconds"),
                )
            ]
        )
    )


@blueprint.route("/tic-api/vod/preview-metadata", methods=["POST"])
@user_auth_required
async def vod_preview_metadata():
    user = await get_user_from_token()
    payload = await request.get_json(force=True, silent=True) or {}
    preview_url = str(payload.get("preview_url") or "").strip()
    if not preview_url:
        return jsonify({"success": False, "message": "Preview URL is required"}), 400

    preview_ref = _parse_vod_preview_reference(preview_url)
    if preview_ref is None:
        return jsonify({"success": False, "message": "Unsupported preview URL"}), 400

    mode = str(preview_ref.get("mode") or "")
    content_type = str(preview_ref.get("content_type") or "")
    item_id = int(preview_ref.get("item_id") or 0)

    if mode == "curated":
        if content_type == VOD_KIND_MOVIE:
            result = await build_curated_movie_browser_playback(
                user,
                item_id,
                allow_probe=True,
                probe_wait_timeout_seconds=2.5,
            )
        else:
            result = await build_curated_episode_browser_playback(
                user,
                item_id,
                allow_probe=True,
                probe_wait_timeout_seconds=2.5,
            )
    elif mode == "upstream":
        if not user_has_admin_role(user):
            return forbidden_response("Admin access required")
        result = await build_upstream_browser_playback(
            item_id,
            content_type,
            upstream_episode_id=str(preview_ref.get("upstream_episode_id") or "").strip() or None,
            container_extension=str(preview_ref.get("container_extension") or "").strip() or None,
            allow_probe=True,
            probe_wait_timeout_seconds=2.5,
        )
    else:
        result = None

    if not result:
        return jsonify({"success": False, "message": "Preview metadata unavailable"}), 404
    if not result.get("success"):
        return jsonify({"success": False, "message": result.get("message") or "Preview metadata unavailable"}), int(
            result.get("status_code") or 404
        )

    metadata_pending = bool(result.get("metadata_pending"))
    has_duration = bool(result.get("duration_seconds"))
    resolution = result.get("source_resolution") or {}
    has_resolution = int(resolution.get("width") or 0) > 0 and int(resolution.get("height") or 0) > 0
    if metadata_pending and not (has_duration and has_resolution):
        return (
            jsonify(
                {
                    "success": True,
                    "pending": True,
                    "stream_type": result.get("stream_type") or "auto",
                    "source_resolution": resolution,
                    "duration_seconds": result.get("duration_seconds"),
                }
            ),
            202,
        )

    return jsonify(
        {
            "success": True,
            "pending": False,
            "stream_type": result.get("stream_type") or "auto",
            "source_resolution": result.get("source_resolution") or {},
            "duration_seconds": result.get("duration_seconds"),
        }
    )


@blueprint.route("/tic-api/library/categories", methods=["GET"])
@user_auth_required
async def curated_library_categories():
    try:
        content_type = require_vod_content_type(request.args.get("content_type"))
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400

    user = await get_user_from_token()
    return jsonify({"success": True, "data": await list_curated_library_categories(user, content_type)})


@blueprint.route("/tic-api/library/status", methods=["GET"])
@user_auth_required
async def curated_library_status():
    user = await get_user_from_token()
    return jsonify({"success": True, "data": await get_library_page_state(user)})


@blueprint.route("/tic-api/library/items", methods=["GET"])
@user_auth_required
async def curated_library_items():
    try:
        content_type = require_vod_content_type(request.args.get("content_type"))
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400

    user = await get_user_from_token()
    payload = await list_curated_library_items(
        user,
        content_type,
        category_id=int_or_none(request.args.get("category_id")),
        search_query=request.args.get("search"),
        offset=convert_to_int(request.args.get("offset"), default=0),
        limit=convert_to_int(request.args.get("limit"), default=50),
    )
    return jsonify({"success": True, "data": payload})


@blueprint.route("/tic-api/library/details/<int:item_id>", methods=["GET"])
@user_auth_required
async def curated_library_item_details(item_id: int):
    try:
        content_type = require_vod_content_type(request.args.get("content_type"))
    except ValueError as exc:
        return jsonify({"success": False, "message": str(exc)}), 400

    user = await get_user_from_token()
    payload = await fetch_curated_library_item_details(user, content_type, int(item_id))
    if payload is None:
        return jsonify({"success": False, "message": "VOD item not found"}), 404
    return jsonify({"success": True, "data": payload})


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
    filtered = [row for row in categories if str(row.get("content_dir") or "") == content_dir]
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


def _vod_library_collection_children(
    index_payload: dict[str, object], resolved: dict[str, object], user
) -> list[dict[str, object]]:
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
    raw_children.sort(
        key=lambda child: (0 if child.get("kind") == "dir" else 1, str(child.get("name") or "").casefold())
    )
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
    display_name = (
        str(child.get("name") or "") if child is not None else request_path.rstrip("/").split("/")[-1] or "library"
    )
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

    auth_result = await authenticate_stream_request()
    if auth_result.rate_limited:
        return rate_limited_basic_auth_response(
            "Too many invalid stream key attempts. Please try again later.",
            auth_result.retry_after,
        )
    user = auth_result.user
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
