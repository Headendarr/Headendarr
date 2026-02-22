#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
from pathlib import Path
from urllib.parse import urlencode, urlparse

import aiofiles
from quart import Response, current_app, redirect, request, stream_with_context
from sqlalchemy import select

from backend.api import blueprint
from backend.auth import audit_stream_event, is_tvh_backend_stream_user, skip_stream_connect_audit, stream_key_required
from backend.hls_multiplexer import (
    handle_m3u8_proxy,
    handle_segment_proxy,
    handle_multiplexed_stream,
    b64_urlsafe_decode,
    b64_urlsafe_encode,
    parse_size,
    mux_manager,
    SegmentCache,
)
from backend.cso import (
    CsoOutputFfmpegCommandBuilder,
    CsoOutputReaderEnded,
    cleanup_channel_stream_events,
    cso_session_manager,
    disconnect_output_client,
    policy_content_type,
    resolve_channel_for_stream,
    subscribe_channel_stream,
)
from backend.stream_profiles import generate_cso_policy_from_profile, resolve_cso_profile_name
from backend.stream_activity import (
    cleanup_stream_activity,
    stop_stream_activity,
    touch_stream_activity,
    upsert_stream_activity,
)
from backend.models import Session, CsoEventLog

# Global cache instance (short default TTL for HLS segments)
hls_segment_cache = SegmentCache(ttl=120)

hls_proxy_prefix = os.environ.get("HLS_PROXY_PREFIX", "/")
if not hls_proxy_prefix.startswith("/"):
    hls_proxy_prefix = "/" + hls_proxy_prefix

hls_proxy_host_ip = os.environ.get("HLS_PROXY_HOST_IP")
hls_proxy_port = os.environ.get("HLS_PROXY_PORT")
hls_proxy_max_buffer_bytes = int(os.environ.get("HLS_PROXY_MAX_BUFFER_BYTES", "1048576"))
hls_proxy_default_prebuffer = parse_size(
    os.environ.get("HLS_PROXY_DEFAULT_PREBUFFER", "0"),
    default=0,
)
_last_cso_event_cleanup_ts = 0.0


def _get_instance_id():
    config = current_app.config.get("APP_CONFIG") if current_app else None
    if not config:
        return None
    return config.ensure_instance_id()


def _validate_instance_id(instance_id):
    """Internal proxy is scoped to this TIC instance only."""
    expected = _get_instance_id()
    if not expected or instance_id != expected:
        return Response("Not found", status=404)
    return None


proxy_logger = logging.getLogger("proxy")
ffmpeg_logger = logging.getLogger("ffmpeg")
buffer_logger = logging.getLogger("buffer")


# CSO unavailable response behavior.
# Keep hard-coded defaults for now; can be moved into app settings/UI later.
CSO_UNAVAILABLE_SHOW_SLATE = True
CSO_UNAVAILABLE_REASON_DURATIONS_SECONDS = {
    "default": 10,
    "capacity_blocked": 10,
    "playback_unavailable": 3,
}
CSO_UNAVAILABLE_SLATE_CACHE_TTL_SECONDS = 30 * 60
CSO_UNAVAILABLE_SLATE_CACHE_VERSION = "v2"
CSO_UNAVAILABLE_SLATE_MESSAGES = {
    "capacity_blocked": {
        "title": "Channel Temporarily Unavailable",
        "subtitle": "Source connection limit reached. Please try again shortly.",
    },
    "playback_unavailable": {
        "title": "Playback Issue Detected",
        "subtitle": "Unable to start playback right now. Please try again shortly.",
    },
}


def _cso_unavailable_slate_message(reason: str, detail_hint: str = "") -> tuple[str, str]:
    payload = CSO_UNAVAILABLE_SLATE_MESSAGES.get(reason) or CSO_UNAVAILABLE_SLATE_MESSAGES["playback_unavailable"]
    title = str(payload.get("title") or "")
    subtitle = str(payload.get("subtitle") or "").strip()
    detail = str(detail_hint or "").strip()
    if detail:
        subtitle = f"{subtitle} {detail}".strip()
    return title, subtitle


def _cso_unavailable_duration_seconds(reason: str) -> int:
    fallback = CSO_UNAVAILABLE_REASON_DURATIONS_SECONDS.get("default", 10)
    try:
        return int(CSO_UNAVAILABLE_REASON_DURATIONS_SECONDS.get(reason, fallback))
    except Exception:
        return int(fallback)


def _summarize_playback_issue(raw_message: str) -> str:
    message = str(raw_message or "").strip()
    if not message:
        return ""
    lower = message.lower()
    if "connection limit" in lower:
        return "Source connection limit reached for this channel."
    if "matroska" in lower and ("aac extradata" in lower or "samplerate" in lower):
        return "Requested Matroska remux is not compatible with source audio. Try profile aac-matroska or default."
    if "could not write header" in lower and "matroska" in lower:
        return "Requested Matroska profile failed to initialize. Try default or aac-matroska."
    if "no available stream source" in lower or "no_available_source" in lower:
        return "No eligible upstream stream is currently available."
    if "output pipeline could not be started" in lower:
        return "Requested profile could not be started for this source. Try default profile."
    if "ingest_start_failed" in lower:
        return "Upstream ingest could not be started for this source."
    compact = " ".join(message.split())
    if len(compact) > 140:
        compact = compact[:140].rstrip() + "..."
    return compact


async def _latest_cso_playback_issue_hint(channel_id: int, session_id: str = "") -> str:
    try:
        async with Session() as session:
            stmt = (
                select(CsoEventLog)
                .where(
                    CsoEventLog.channel_id == int(channel_id),
                    CsoEventLog.event_type.in_(["playback_unavailable", "capacity_blocked", "switch_attempt"]),
                )
                .order_by(CsoEventLog.created_at.desc(), CsoEventLog.id.desc())
                .limit(10)
            )
            if session_id:
                stmt = stmt.where(CsoEventLog.session_id == session_id)
            result = await session.execute(stmt)
            rows = result.scalars().all()
    except Exception:
        return ""

    for row in rows:
        try:
            details = json.loads(row.details_json or "{}")
        except Exception:
            details = {}
        ffmpeg_error = str(details.get("ffmpeg_error") or "").strip()
        reason = str(details.get("reason") or details.get("after_failure_reason") or "").strip()
        if ffmpeg_error:
            return _summarize_playback_issue(ffmpeg_error)
        if reason:
            return _summarize_playback_issue(reason)
    return ""


def _resolve_cso_unavailable_logo_path() -> str | None:
    project_root = Path(__file__).resolve().parents[2]
    candidates = [
        project_root / "frontend/src/assets/icon.png",
        project_root / "logo.png",
        project_root / "frontend/public/icons/Headendarr-Logo.png",
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return str(candidate)
    return None


def _escape_ffmpeg_drawtext_text(value: str) -> str:
    text = str(value or "")
    # Escape characters significant to ffmpeg drawtext parser.
    text = text.replace("\\", "\\\\")
    text = text.replace(":", "\\:")
    text = text.replace("'", "\\'")
    text = text.replace(",", "\\,")
    text = text.replace("[", "\\[")
    text = text.replace("]", "\\]")
    return text


def _wrap_words(text: str, max_chars: int = 44, max_lines: int = 2) -> list[str]:
    words = [part for part in str(text or "").strip().split() if part]
    if not words:
        return []
    lines = []
    current = []
    for word in words:
        candidate = " ".join(current + [word]).strip()
        if len(candidate) <= max_chars or not current:
            current.append(word)
            continue
        lines.append(" ".join(current))
        current = [word]
        if len(lines) >= max_lines - 1:
            break
    if current and len(lines) < max_lines:
        lines.append(" ".join(current))
    return lines[:max_lines]


def _build_cso_unavailable_slate_command(
    reason: str, duration_seconds: int = 10, output_target: str = "pipe:1", detail_hint: str = ""
) -> list[str]:
    title, subtitle = _cso_unavailable_slate_message(reason, detail_hint=detail_hint)
    title = _escape_ffmpeg_drawtext_text(title)
    subtitle_lines = [_escape_ffmpeg_drawtext_text(line) for line in _wrap_words(subtitle, max_chars=84, max_lines=4)]
    drawtext_title = "drawtext=" f"text='{title}':" "fontcolor=white:" "fontsize=52:" "x=(w-text_w)/2:y=(h/2)-84"
    drawtext_subtitle_1 = (
        "drawtext="
        f"text='{subtitle_lines[0] if len(subtitle_lines) > 0 else ''}':"
        "fontcolor=white:fontsize=20:"
        "x=(w-text_w)/2:y=(h/2)+2"
    )
    drawtext_subtitle_2 = (
        "drawtext="
        f"text='{subtitle_lines[1] if len(subtitle_lines) > 1 else ''}':"
        "fontcolor=white:fontsize=20:"
        "x=(w-text_w)/2:y=(h/2)+30"
    )
    drawtext_subtitle_3 = (
        "drawtext="
        f"text='{subtitle_lines[2] if len(subtitle_lines) > 2 else ''}':"
        "fontcolor=white:fontsize=20:"
        "x=(w-text_w)/2:y=(h/2)+58"
    )
    drawtext_subtitle_4 = (
        "drawtext="
        f"text='{subtitle_lines[3] if len(subtitle_lines) > 3 else ''}':"
        "fontcolor=white:fontsize=20:"
        "x=(w-text_w)/2:y=(h/2)+86"
    )
    # Animated blurred blobs using TIC frontend palette.
    # Palette refs from frontend/src/css/app.scss:
    # --app-page-bg: #0b0f14, --q-primary: #21a3cf, --q-secondary: #79d2c0, --q-info: #6aa8ff
    draw_panel = "drawbox=x=70:y=(ih/2)-160:w=1140:h=340:color=0x0B0F14@0.64:t=fill"
    draw_border = "drawbox=x=70:y=(ih/2)-160:w=1140:h=340:color=0xE2E8F0@0.16:t=2"
    logo_path = _resolve_cso_unavailable_logo_path()
    filter_steps = [
        "[1:v]format=rgba,colorchannelmixer=aa=0.30,gblur=sigma=90[blob1]",
        "[2:v]format=rgba,colorchannelmixer=aa=0.26,gblur=sigma=105[blob2]",
        "[3:v]format=rgba,colorchannelmixer=aa=0.24,gblur=sigma=98[blob3]",
        "[0:v][blob1]overlay=x='(W-w)/2+sin(2*PI*t/10)*18':y='(H-h)/2+cos(2*PI*t/10)*14':shortest=1[bg1]",
        "[bg1][blob2]overlay=x='(W-w)/2+cos(2*PI*t/10+0.8)*20':y='(H-h)/2+sin(2*PI*t/10+0.8)*16':shortest=1[bg2]",
        "[bg2][blob3]overlay=x='(W-w)/2+sin(2*PI*t/10+1.6)*22':y='(H-h)/2+cos(2*PI*t/10+1.6)*15':shortest=1[bg3]",
        "[blob1]scale=w=240:h=240[blob1_side]",
        "[blob2]scale=w=210:h=210[blob2_side]",
        "[bg3][blob1_side]overlay=x='92+sin(2*PI*t/10+0.35)*10':y='H*0.24+cos(2*PI*t/10+0.95)*9':shortest=1[bg4]",
        "[bg4][blob2_side]overlay=x='W-w-108+cos(2*PI*t/10+1.15)*11':y='H*0.69+sin(2*PI*t/10+0.55)*8':shortest=1[bg5]",
        "[bg5]gblur=sigma=40:steps=2,fps=30[bg_blur]",
    ]
    final_video_label = "bg_blur"
    audio_input_index = 4
    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-f",
        "lavfi",
        "-i",
        f"color=c=#0b0f14:s=1280x720:r=30:d={duration_seconds}",
        "-f",
        "lavfi",
        "-i",
        f"color=c=#21a3cf:s=680x680:r=30:d={duration_seconds}",
        "-f",
        "lavfi",
        "-i",
        f"color=c=#79d2c0:s=760x760:r=30:d={duration_seconds}",
        "-f",
        "lavfi",
        "-i",
        f"color=c=#6aa8ff:s=620x620:r=30:d={duration_seconds}",
    ]
    if logo_path:
        command += ["-loop", "1", "-i", logo_path]
        filter_steps.append("[4:v]scale=w=92:h=-1:flags=lanczos,format=rgba,colorchannelmixer=aa=0.98[logo]")
        filter_steps.append("[bg_blur][logo]overlay=x=24:y=24:shortest=1[bg4]")
        final_video_label = "bg4"
        audio_input_index = 5
    filter_steps.append(
        (
            f"[{final_video_label}]"
            f"{draw_panel},{draw_border},{drawtext_title},"
            f"{drawtext_subtitle_1},{drawtext_subtitle_2},{drawtext_subtitle_3},{drawtext_subtitle_4},"
            "eq=brightness=-0.03:contrast=1.06:saturation=1.18[vout]"
        )
    )
    filter_chain = ";".join(filter_steps)
    command += [
        "-f",
        "lavfi",
        "-i",
        f"anullsrc=r=48000:cl=stereo:d={duration_seconds}",
        "-filter_complex",
        filter_chain,
        "-map",
        "[vout]",
        "-map",
        f"{audio_input_index}:a",
        "-c:v",
        "libx264",
        "-preset",
        "superfast",
        "-tune",
        "zerolatency",
        "-r",
        "30",
        "-pix_fmt",
        "yuv420p",
        "-g",
        "60",
        "-c:a",
        "aac",
        "-b:a",
        "96k",
        "-ar",
        "48000",
        "-ac",
        "2",
        "-shortest",
        "-f",
        "mpegts",
        output_target,
    ]
    return command


async def _iter_cso_unavailable_slate_source(reason: str, detail_hint: str = ""):
    resolved_duration = _cso_unavailable_duration_seconds(reason)
    config = current_app.config.get("APP_CONFIG") if current_app else None
    normalized_detail = str(detail_hint or "").strip()
    slate_file = None
    if not normalized_detail:
        slate_file = await _ensure_cso_unavailable_slate_asset(config, reason, resolved_duration)
    if slate_file and os.path.exists(slate_file):
        proxy_logger.info("Streaming cached CSO unavailable slate reason=%s file=%s", reason, slate_file)
        async with aiofiles.open(slate_file, "rb") as f:
            while True:
                chunk = await f.read(16384)
                if not chunk:
                    break
                yield chunk
        return

    command = _build_cso_unavailable_slate_command(
        reason,
        duration_seconds=resolved_duration,
        detail_hint=normalized_detail,
    )
    proxy_logger.info(
        "Starting CSO unavailable slate reason=%s duration=%ss command=%s",
        reason,
        resolved_duration,
        command,
    )
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def _stderr_reader():
        while True:
            try:
                line = await process.stderr.readline()
            except Exception:
                break
            if not line:
                break
            # Drain stderr to avoid blocking FFmpeg; line-by-line output is intentionally suppressed.

    stderr_task = asyncio.create_task(_stderr_reader())
    try:
        while process.stdout:
            chunk = await process.stdout.read(16384)
            if not chunk:
                break
            yield chunk
    finally:
        try:
            process.terminate()
            await asyncio.wait_for(process.wait(), timeout=1.5)
        except Exception:
            try:
                process.kill()
            except Exception:
                pass
        try:
            await asyncio.wait_for(stderr_task, timeout=0.5)
        except Exception:
            pass
        proxy_logger.info("CSO unavailable slate ended reason=%s", reason)


async def _cso_unavailable_slate_stream(
    reason: str,
    policy: dict | None = None,
    detail_hint: str = "",
):
    if not policy:
        async for chunk in _iter_cso_unavailable_slate_source(reason, detail_hint=detail_hint):
            yield chunk
        return

    effective_policy = dict(policy or {})
    container = str(effective_policy.get("container") or "mpegts")
    if container in {"matroska", "mp4"}:
        # Live fallback slates for non-TS containers are more reliable when audio is re-encoded.
        effective_policy["output_mode"] = "force_transcode"
        if not effective_policy.get("audio_codec"):
            effective_policy["audio_codec"] = "aac"
        if "video_codec" not in effective_policy:
            effective_policy["video_codec"] = ""
    command = CsoOutputFfmpegCommandBuilder(effective_policy).build_output_command()
    proxy_logger.info(
        "Starting CSO unavailable slate transform reason=%s duration=%ss policy=%s command=%s",
        reason,
        _cso_unavailable_duration_seconds(reason),
        effective_policy,
        command,
    )
    process = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def _writer():
        try:
            async for chunk in _iter_cso_unavailable_slate_source(reason, detail_hint=detail_hint):
                if not process.stdin:
                    break
                process.stdin.write(chunk)
                await process.stdin.drain()
        except Exception:
            pass
        finally:
            try:
                if process.stdin:
                    process.stdin.close()
            except Exception:
                pass

    async def _stderr_reader():
        while True:
            try:
                line = await process.stderr.readline()
            except Exception:
                break
            if not line:
                break
            # Drain stderr to avoid blocking FFmpeg; line-by-line output is intentionally suppressed.

    writer_task = asyncio.create_task(_writer())
    stderr_task = asyncio.create_task(_stderr_reader())
    emitted_bytes = 0
    try:
        while process.stdout:
            chunk = await process.stdout.read(16384)
            if not chunk:
                break
            emitted_bytes += len(chunk)
            yield chunk
    finally:
        try:
            await asyncio.wait_for(writer_task, timeout=2.0)
        except Exception:
            writer_task.cancel()
        try:
            process.terminate()
            await asyncio.wait_for(process.wait(), timeout=1.5)
        except Exception:
            try:
                process.kill()
            except Exception:
                pass
        try:
            await asyncio.wait_for(stderr_task, timeout=0.5)
        except Exception:
            pass
        proxy_logger.info("CSO unavailable slate transform ended reason=%s bytes=%s", reason, emitted_bytes)


async def _ensure_cso_unavailable_slate_asset(config, reason: str, duration_seconds: int) -> str | None:
    if not config:
        return None
    cache_dir = Path(config.config_path) / "cache" / "cso_slates"
    cache_dir.mkdir(parents=True, exist_ok=True)
    reason_key = str(reason or "playback_unavailable").strip().lower()
    await _cleanup_cso_unavailable_slate_cache(cache_dir, max_age_seconds=CSO_UNAVAILABLE_SLATE_CACHE_TTL_SECONDS)
    cache_hash = _cso_unavailable_slate_cache_hash(reason_key, int(duration_seconds))
    out_path = cache_dir / f"{reason_key}_{int(duration_seconds)}s_{cache_hash}.ts"
    if out_path.exists() and out_path.stat().st_size > 0:
        return str(out_path)

    command = _build_cso_unavailable_slate_command(
        reason_key,
        duration_seconds=duration_seconds,
        output_target=str(out_path),
    )
    proxy_logger.info("Rendering CSO unavailable slate asset reason=%s path=%s", reason_key, out_path)
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        _, stderr = await asyncio.wait_for(process.communicate(), timeout=max(15, int(duration_seconds) + 5))
    except asyncio.TimeoutError:
        try:
            process.kill()
        except Exception:
            pass
        return None

    if process.returncode != 0:
        stderr_text = (stderr or b"").decode("utf-8", errors="replace").strip()
        ffmpeg_logger.warning(
            "Failed rendering CSO unavailable slate reason=%s rc=%s stderr=%s",
            reason_key,
            process.returncode,
            stderr_text[-1000:],
        )
        try:
            out_path.unlink(missing_ok=True)
        except Exception:
            pass
        return None
    return str(out_path)


def _cso_unavailable_slate_cache_hash(reason: str, duration_seconds: int) -> str:
    # Use the generated command as cache key input so rendering/layout/text changes
    # invalidate old files without manual cache clears.
    command = _build_cso_unavailable_slate_command(
        reason,
        duration_seconds=duration_seconds,
        output_target="pipe:1",
        detail_hint="",
    )
    digest_input = json.dumps(
        {
            "version": CSO_UNAVAILABLE_SLATE_CACHE_VERSION,
            "reason": reason,
            "duration_seconds": int(duration_seconds),
            "command": command,
        },
        sort_keys=True,
    )
    return hashlib.sha1(digest_input.encode("utf-8")).hexdigest()[:12]


async def _cleanup_cso_unavailable_slate_cache(cache_dir: Path, max_age_seconds: int) -> None:
    now = time.time()
    ttl = max(60, int(max_age_seconds or CSO_UNAVAILABLE_SLATE_CACHE_TTL_SECONDS))
    try:
        for file_path in cache_dir.glob("*.ts"):
            try:
                if not file_path.is_file():
                    continue
                age = now - file_path.stat().st_mtime
                if age > ttl:
                    file_path.unlink(missing_ok=True)
            except Exception:
                continue
    except Exception:
        pass


async def cleanup_hls_proxy_state():
    """Cleanup expired cache entries and idle stream activity."""
    global _last_cso_event_cleanup_ts
    try:
        evicted_count = await hls_segment_cache.evict_expired_items()
        if evicted_count > 0:
            proxy_logger.info(f"Cache cleanup: evicted {evicted_count} expired items")
        await cleanup_stream_activity()
        # Cleanup idle multiplexer streams
        await mux_manager.cleanup_idle_streams(idle_timeout=300)
        await cso_session_manager.cleanup_idle_streams(idle_timeout=300)
        now = time.time()
        if now - _last_cso_event_cleanup_ts >= 3600:
            await cleanup_channel_stream_events(current_app.config["APP_CONFIG"])
            _last_cso_event_cleanup_ts = now
    except Exception as e:
        proxy_logger.error(f"Error during cache cleanup: {e}")


async def periodic_cache_cleanup():
    while True:
        try:
            await cleanup_hls_proxy_state()
        except Exception as e:
            proxy_logger.error("Error during cache cleanup: %s", e)
        await asyncio.sleep(60)


@blueprint.record_once
def _register_startup(state):
    app = state.app

    @app.before_serving
    async def _start_periodic_cache_cleanup():
        asyncio.create_task(periodic_cache_cleanup())


def _build_upstream_headers():
    headers = {}
    try:
        src = request.headers
    except Exception:
        return headers
    for name in ("User-Agent", "Referer", "Origin", "Accept", "Accept-Language"):
        value = src.get(name)
        if value:
            headers[name] = value
    return headers


def _build_proxy_base_url(instance_id=None):
    base_path = hls_proxy_prefix.rstrip("/")
    if instance_id:
        base_path = f"{base_path}/{instance_id}"
    if hls_proxy_host_ip:
        protocol = "http"
        host_port = ""
        if hls_proxy_port:
            if hls_proxy_port == "443":
                protocol = "https"
            host_port = f":{hls_proxy_port}"
        return f"{protocol}://{hls_proxy_host_ip}{host_port}{base_path}"
    return f"{request.host_url.rstrip('/')}{base_path}"


def _get_connection_id(default_new=False):
    value = (request.args.get("connection_id") or request.args.get("cid") or "").strip()
    if value:
        return value
    if default_new:
        return uuid.uuid4().hex
    return None


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/<encoded_url>.m3u8",
    methods=["GET", "HEAD"],
)
@stream_key_required
@skip_stream_connect_audit
async def proxy_m3u8(instance_id, encoded_url):
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    connection_id = _get_connection_id(default_new=False)
    if not connection_id:
        params = [(k, v) for k, v in request.args.items() if k not in {"cid", "connection_id"}]
        connection_id = _get_connection_id(default_new=True)
        params.append(("connection_id", connection_id))
        target = f"{hls_proxy_prefix.rstrip('/')}/{instance_id}/{encoded_url}.m3u8?{urlencode(params)}"
        return redirect(target, code=302)

    decoded_url = b64_urlsafe_decode(encoded_url)
    await upsert_stream_activity(decoded_url, connection_id=connection_id)

    stream_key = request.args.get("stream_key") or request.args.get("password")
    username = request.args.get("username")

    headers = _build_upstream_headers()

    body, content_type, status, res_headers = await handle_m3u8_proxy(
        decoded_url,
        request_host_url=request.host_url,
        hls_proxy_prefix=hls_proxy_prefix,
        headers=headers,
        instance_id=instance_id,
        stream_key=stream_key,
        username=username,
        connection_id=connection_id,
        max_buffer_bytes=hls_proxy_max_buffer_bytes,
        proxy_base_url=_build_proxy_base_url(instance_id=instance_id),
        segment_cache=hls_segment_cache,
        prefetch_segments_enabled=True,
    )

    if body is None:
        resp = Response("Failed to fetch the original playlist.", status=status)
        for k, v in res_headers.items():
            resp.headers[k] = v
        return resp

    if hasattr(body, "__aiter__"):

        @stream_with_context
        async def generate_playlist():
            async for chunk in body:
                yield chunk

        return Response(generate_playlist(), content_type=content_type)
    return Response(body, content_type=content_type)


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/proxy.m3u8",
    methods=["GET", "HEAD"],
)
@stream_key_required
@skip_stream_connect_audit
async def proxy_m3u8_redirect(instance_id):
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    url = request.args.get("url")
    if not url:
        return Response("Missing url parameter.", status=400)
    encoded = b64_urlsafe_encode(url)
    stream_key = request.args.get("stream_key") or request.args.get("password")
    username = request.args.get("username")
    connection_id = _get_connection_id(default_new=True)
    if connection_id == "tvh":
        # Treat "tvh" as a logical label only. Internally, each request gets a
        # unique client id to avoid teardown collisions across reconnects.
        connection_id = f"tvh-{uuid.uuid4().hex}"
        proxy_logger.info(
            "Remapped reserved connection_id requested=tvh effective=%s url=%s",
            connection_id,
            url,
        )
    target = f"{hls_proxy_prefix.rstrip('/')}/{instance_id}/{encoded}.m3u8"
    query = [("connection_id", connection_id)]
    if stream_key:
        query.append(("stream_key", stream_key))
    if username:
        query.append(("username", username))

    return redirect(f"{target}?{urlencode(query)}", code=302)


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/<encoded_url>.key",
    methods=["GET", "HEAD"],
)
@stream_key_required
@skip_stream_connect_audit
async def proxy_key(instance_id, encoded_url):
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    # Decode the Base64 encoded URL
    decoded_url = b64_urlsafe_decode(encoded_url)
    # Touch activity via connection_id if available
    await upsert_stream_activity(decoded_url, connection_id=_get_connection_id(), perform_audit=False)

    content, status, _ = await handle_segment_proxy(decoded_url, _build_upstream_headers(), hls_segment_cache)
    if content is None:
        return Response("Failed to fetch.", status=status)
    return Response(content, content_type="application/octet-stream")


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/<encoded_url>.ts",
    methods=["GET", "HEAD"],
)
@stream_key_required
@skip_stream_connect_audit
async def proxy_ts(instance_id, encoded_url):
    """
    TIC Legacy/Segment Proxy Endpoint

    This endpoint serves individual .ts segments for HLS or provides a
    direct stream when configured.

    Parameters:
    - ffmpeg=true: (Optional) Fallback to FFmpeg remuxer for better compatibility.
    - prebuffer=X: (Optional) Buffer size cushion (e.g. 2M, 512K). Default: 1M.
    """
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    # Decode the Base64 encoded URL
    decoded_url = b64_urlsafe_decode(encoded_url)
    await upsert_stream_activity(decoded_url, connection_id=_get_connection_id(), perform_audit=False)

    # Multiplexer routing
    if request.args.get("ffmpeg", "false").lower() == "true" or request.args.get("prebuffer"):
        target = f"{hls_proxy_prefix.rstrip('/')}/{instance_id}/stream/{encoded_url}"
        if request.query_string:
            target = f"{target}?{request.query_string.decode()}"
        return redirect(target, code=302)

    content, status, content_type = await handle_segment_proxy(
        decoded_url, _build_upstream_headers(), hls_segment_cache
    )
    if content is None:
        return Response("Failed to fetch.", status=status)

    # Playlist detection
    if "mpegurl" in (content_type or "") or content.lstrip().startswith(b"#EXTM3U"):
        target = f"{hls_proxy_prefix.rstrip('/')}/{instance_id}/{encoded_url}.m3u8"
        if request.query_string:
            target = f"{target}?{request.query_string.decode()}"
        return redirect(target, code=302)

    return Response(content, content_type="video/mp2t")


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/<encoded_url>.vtt",
    methods=["GET", "HEAD"],
)
@stream_key_required
@skip_stream_connect_audit
async def proxy_vtt(instance_id, encoded_url):
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid
    # Decode the Base64 encoded URL
    decoded_url = b64_urlsafe_decode(encoded_url)
    await upsert_stream_activity(decoded_url, connection_id=_get_connection_id(), perform_audit=False)

    content, status, _ = await handle_segment_proxy(decoded_url, _build_upstream_headers(), hls_segment_cache)
    if content is None:
        return Response("Failed to fetch.", status=status)
    return Response(content, content_type="text/vtt")


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/<instance_id>/stream/<encoded_url>",
    methods=["GET"],
)
@stream_key_required
@skip_stream_connect_audit
async def stream_ts(instance_id, encoded_url):
    """
    TIC Shared Multiplexer Stream Endpoint

    This endpoint provides a shared upstream connection for multiple TIC clients.

    Default Mode (Direct):
    Uses high-performance async socket reads. Best for 99% of streams.

    Fallback Mode (FFmpeg):
    Enabled by appending '?ffmpeg=true'. Uses an external FFmpeg process to
    remux/clean the stream. Use this ONLY if 'direct' mode has playback issues.

    Parameters:
    - ffmpeg=true: (Optional) Fallback to FFmpeg remuxer for better compatibility.
    - prebuffer=X: (Optional) Buffer size cushion (e.g. 2M, 512K). Default: 1M.
    """
    invalid = _validate_instance_id(instance_id)
    if invalid:
        return invalid

    decoded_url = b64_urlsafe_decode(encoded_url)
    connection_id = _get_connection_id(default_new=True)
    await upsert_stream_activity(decoded_url, connection_id=connection_id)

    use_ffmpeg = request.args.get("ffmpeg", "false").lower() == "true"
    prebuffer_bytes = parse_size(
        request.args.get("prebuffer"),
        default=hls_proxy_default_prebuffer,
    )
    mode = "ffmpeg" if use_ffmpeg else "direct"

    generator = await handle_multiplexed_stream(
        decoded_url, mode, _build_upstream_headers(), prebuffer_bytes, connection_id
    )

    if not is_tvh_backend_stream_user(getattr(request, "_stream_user", None)):
        await audit_stream_event(request._stream_user, "hls_stream_connect", request.path)

    @stream_with_context
    async def generate_stream():
        async for chunk in generator:
            yield chunk

    response = Response(generate_stream(), content_type="video/mp2t")
    response.timeout = None  # Disable timeout for streaming response
    return response


@blueprint.route(
    f"{hls_proxy_prefix.lstrip('/')}/channel/<channel_id>",
    methods=["GET"],
)
@stream_key_required
@skip_stream_connect_audit
async def stream_channel(channel_id):
    """
    TIC Channel Stream Organiser (CSO) playback endpoint.

    Route:
    - `GET /<hls_proxy_prefix>/channel/<channel_id>`

    Authentication:
    - Requires `stream_key_required`.

    Behavior:
    - Resolves the channel and starts/joins CSO sessions for that channel.
    - Uses `profile` query param to select output behavior (remux/transcode profile).
    - Supports shared upstream ingest per channel, with per-profile output pipelines.
    - Returns a continuous stream response with container/content type based on the
      resolved CSO profile.

    Query params:
    - `stream_key` (required): stream authentication token.
    - `profile` (optional): requested stream profile.
      Resolution order is: request profile -> channel profile -> `default`.
      Special case: `tvh` maps to the TVHeadend-oriented MPEG-TS CSO behavior.
    - `prebuffer` (optional): per-client prebuffer size for newly attached output
      clients (for example `50k`, `1M`).

    Failure behavior:
    - If CSO cannot start due to capacity or source playback failure, returns 503.
    - If `CSO_UNAVAILABLE_SHOW_SLATE` is enabled, 503 failures are replaced with a
      temporary MPEG-TS unavailable slate stream (HTTP 200).

    Notes:
    - This endpoint is CSO-specific and separate from direct HLS proxy passthrough
      routes that proxy encoded upstream URLs.
    """
    try:
        channel_id_int = int(channel_id)
    except (TypeError, ValueError):
        return Response("Invalid channel id", status=400)

    config = current_app.config["APP_CONFIG"]
    requested_profile = (request.args.get("profile") or "").strip().lower()
    prebuffer_bytes = parse_size(request.args.get("prebuffer"), default=0)
    channel = await resolve_channel_for_stream(channel_id_int)
    effective_profile = resolve_cso_profile_name(
        config,
        requested_profile,
        channel=channel,
    )
    effective_policy = generate_cso_policy_from_profile(config, effective_profile)

    connection_id = _get_connection_id(default_new=True)
    if connection_id == "tvh":
        # Treat "tvh" as a logical label only. Internally, each request gets a
        # unique client id to avoid teardown collisions across reconnects.
        connection_id = f"tvh-{uuid.uuid4().hex}"
        proxy_logger.info(
            "Remapped reserved connection_id requested=tvh effective=%s channel=%s",
            connection_id,
            channel_id_int,
        )
    generator, content_type, error_message, status = await subscribe_channel_stream(
        config=config,
        channel=channel,
        stream_key=getattr(request, "_stream_key", None),
        profile=effective_profile,
        connection_id=connection_id,
        prebuffer_bytes=prebuffer_bytes,
        request_base_url=request.host_url.rstrip("/"),
    )
    if not generator:
        message = (error_message or "").strip()
        if (status or 500) == 503 and CSO_UNAVAILABLE_SHOW_SLATE:
            lower_message = message.lower()
            reason = "capacity_blocked" if "connection limit" in lower_message else "playback_unavailable"
            detail_hint = _summarize_playback_issue(message) if reason == "playback_unavailable" else ""

            @stream_with_context
            async def generate_unavailable_slate():
                async for chunk in _cso_unavailable_slate_stream(
                    reason,
                    policy=effective_policy,
                    detail_hint=detail_hint,
                ):
                    yield chunk

            return Response(
                generate_unavailable_slate(),
                content_type=policy_content_type(effective_policy) or "application/octet-stream",
                status=200,
            )

        return Response(message or "Unable to start CSO stream", status=status or 500)

    activity_identity = f"{hls_proxy_prefix.rstrip('/')}/channel/{channel_id_int}"
    channel_name = getattr(channel, "name", None) if channel else None
    channel_logo_url = getattr(channel, "logo_url", None) if channel else None
    details_override = activity_identity
    if channel_name:
        details_override = f"{channel_name}\n{activity_identity}"

    await upsert_stream_activity(
        activity_identity,
        connection_id=connection_id,
        endpoint_override=request.path,
        start_event_type="stream_start",
        user=getattr(request, "_stream_user", None),
        details_override=details_override,
        channel_id=channel_id_int,
        channel_name=channel_name,
        channel_logo_url=channel_logo_url,
        stream_name=channel_name,
        display_url=activity_identity,
    )
    output_session_key = f"cso-output-{channel_id_int}-{effective_profile}"

    @stream_with_context
    async def generate_stream():
        touch_identity = activity_identity
        last_touch_ts = time.time()
        should_emit_failure_slate = False
        try:
            try:
                async for chunk in generator:
                    now = time.time()
                    if (now - last_touch_ts) >= 5.0:
                        await touch_stream_activity(connection_id, identity=touch_identity)
                        last_touch_ts = now
                    yield chunk
            except CsoOutputReaderEnded:
                should_emit_failure_slate = True

            if not should_emit_failure_slate:
                session = await cso_session_manager.get_output_session(output_session_key)
                should_emit_failure_slate = bool(
                    session and getattr(session, "last_error", "") == "output_reader_ended"
                )

            if should_emit_failure_slate and CSO_UNAVAILABLE_SHOW_SLATE:
                reason = "playback_unavailable"
                detail_hint = await _latest_cso_playback_issue_hint(
                    channel_id_int,
                    session_id=output_session_key,
                )
                async for chunk in _cso_unavailable_slate_stream(
                    reason,
                    policy=effective_policy,
                    detail_hint=detail_hint,
                ):
                    yield chunk
        finally:
            await stop_stream_activity(
                "",
                connection_id=connection_id,
                event_type="stream_stop",
                endpoint_override=request.path,
                user=getattr(request, "_stream_user", None),
            )

    response = Response(generate_stream(), content_type=content_type or "application/octet-stream")
    response.timeout = None
    return response
