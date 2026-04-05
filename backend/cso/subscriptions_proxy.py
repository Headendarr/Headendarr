import asyncio
import logging
from collections import deque
from typing import Any

from backend.stream_profiles import generate_cso_policy_from_profile
from backend.vod import VodCuratedPlaybackCandidate, VodSourcePlaybackCandidate

from .common import build_cso_stream_plan
from .events import emit_channel_stream_event, source_event_context
from .ffmpeg import (
    CsoFfmpegCommandBuilder,
    event_source_probe,
    redact_ingest_command_for_log,
    start_ffmpeg_with_hw_decode_fallback,
    terminate_ffmpeg_process,
)
from .output import CsoOutputSession
from .policy import pipe_container_from_content_type, policy_content_type, resolve_vod_pipe_container
from .sources import cso_source_from_vod_source
from .subscriptions_shared import should_use_vod_proxy_session
from .live_ingest import resolve_cso_ingest_headers, resolve_cso_ingest_user_agent
from .vod_cache import ensure_vod_cache_ready, start_vod_cache_download, vod_cache_manager
from .vod_proxy import (
    register_vod_proxy_output_disconnect,
    unregister_vod_proxy_output_disconnect,
    vod_proxy_session_manager,
)


logger = logging.getLogger("cso")


async def subscribe_vod_proxy_stream(
    candidate,
    upstream_url,
    connection_id,
    request_headers=None,
    episode=None,
    source_override=None,
):
    if not candidate:
        return build_cso_stream_plan(None, None, "VOD item not found", 404)

    if isinstance(candidate, VodCuratedPlaybackCandidate):
        item = candidate.group_item
        vod_category_id = item.category_id
        vod_item_id = item.id
    else:
        item = None
        vod_category_id = None
        vod_item_id = None
    source = source_override or await cso_source_from_vod_source(candidate, upstream_url)
    if not source:
        return build_cso_stream_plan(None, None, "Source not found", 404)

    playlist = source.playlist
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return build_cso_stream_plan(None, None, "Source playlist is disabled", 404)
    local_cache_ready = False
    if not source.url:
        entry = await vod_cache_manager.get_or_create(source, upstream_url or "")
        local_cache_ready = bool(entry.complete and entry.final_path.exists())
    if not source.url and not local_cache_ready:
        return build_cso_stream_plan(None, None, "No available stream source", 503)

    session_key = f"vod-proxy-{source.id}-{connection_id}"
    session = await vod_proxy_session_manager.create(
        session_key,
        source,
        source.url,
        request_headers=request_headers,
    )
    started = await session.start()
    if not started:
        reason = session.last_error or "proxy_start_failed"
        await emit_channel_stream_event(
            vod_category_id=vod_category_id,
            vod_item_id=vod_item_id,
            vod_episode_id=episode.id if episode else None,
            source=source,
            session_id=session_key,
            event_type="capacity_blocked" if reason == "capacity_blocked" else "playback_unavailable",
            severity="warning",
            details={
                "reason": reason,
                **source_event_context(source, source_url=source.url),
            },
        )
        await session.stop(force=True)
        return build_cso_stream_plan(
            None,
            None,
            "Source capacity limit reached" if reason == "capacity_blocked" else "Unable to start proxy stream",
            503 if reason == "capacity_blocked" else 502,
        )

    await emit_channel_stream_event(
        vod_category_id=vod_category_id,
        vod_item_id=vod_item_id,
        vod_episode_id=episode.id if episode else None,
        source=source,
        session_id=session_key,
        event_type="session_start",
        severity="info",
        details={
            "mode": "proxy",
            "connection_id": connection_id,
            **source_event_context(source, source_url=source.url),
        },
    )

    async def _generator():
        try:
            async for chunk in session.iter_bytes():
                yield chunk
        finally:
            await emit_channel_stream_event(
                vod_category_id=vod_category_id,
                vod_item_id=vod_item_id,
                vod_episode_id=getattr(episode, "id", None),
                source=source,
                session_id=session_key,
                event_type="session_end",
                severity="info",
                details={
                    "mode": "proxy",
                    "connection_id": connection_id,
                    **source_event_context(source, source_url=source.url),
                },
            )

    return build_cso_stream_plan(
        _generator(),
        session.content_type or policy_content_type({"container": source.container_extension or "matroska"}),
        None,
        int(session.status_code or 200),
        headers=session.response_headers,
    )


async def subscribe_vod_proxy_output_stream(
    config: Any,
    candidate: VodCuratedPlaybackCandidate | VodSourcePlaybackCandidate,
    upstream_url: str,
    profile: str,
    connection_id: str,
    start_seconds: int = 0,
    max_duration_seconds: int | None = None,
    request_headers: dict[str, str] | None = None,
) -> Any:
    if not candidate:
        return build_cso_stream_plan(None, None, "VOD item not found", 404)

    if isinstance(candidate, VodCuratedPlaybackCandidate):
        item = candidate.group_item
        item_id = item.id
    else:
        item = None
        item_id = None
    source = await cso_source_from_vod_source(candidate, upstream_url)
    if not source:
        return build_cso_stream_plan(None, None, "Source not found", 404)

    playlist = source.playlist
    if playlist is not None and not bool(getattr(playlist, "enabled", False)):
        return build_cso_stream_plan(None, None, "Source playlist is disabled", 404)

    policy = generate_cso_policy_from_profile(config, profile)
    source_probe = event_source_probe(source)
    proxy_session_key = f"vod-proxy-output-{source.id}-{connection_id}"
    cache_entry = await vod_cache_manager.get_or_create(source, upstream_url or source.url)
    local_cache_ready = bool(cache_entry.complete and cache_entry.final_path.exists())
    using_local_cache = local_cache_ready
    use_direct_upstream_input = bool(not using_local_cache and int(start_seconds or 0) > 0 and source.url)
    proxy_session = None

    if not using_local_cache and not source.url:
        return build_cso_stream_plan(None, None, "No available stream source", 503)
    source_identity = str(cache_entry.final_path) if using_local_cache else (upstream_url or source.url)

    base_policy = dict(policy)
    process = None
    writer_task = None
    stderr_task = None
    stdout_task = None
    recent_stderr: deque[str] = deque(maxlen=30)
    first_output_chunk = b""
    disconnect_lock = asyncio.Lock()
    should_start_cache_warm = False
    output_queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=8)

    async def _stop_proxy_session(force: bool = True) -> None:
        nonlocal proxy_session
        if proxy_session is None:
            return
        try:
            await proxy_session.stop(force=force)
        finally:
            proxy_session = None

    async def _disconnect_active_output() -> None:
        async with disconnect_lock:
            for task in (writer_task, stderr_task, stdout_task):
                if task is not None and not task.done():
                    task.cancel()
            for task in (writer_task, stderr_task, stdout_task):
                if task is None or task.done():
                    continue
                try:
                    await task
                except BaseException:
                    pass
            await _stop_proxy_session(force=True)
            if process is not None:
                await terminate_ffmpeg_process(process)

    async def _ensure_proxy_session():
        nonlocal proxy_session
        if using_local_cache or use_direct_upstream_input:
            return True, ""
        await _stop_proxy_session(force=True)
        proxy_request_headers = dict(request_headers or {})
        proxy_request_headers.pop("Range", None)
        proxy_session = await vod_proxy_session_manager.create(
            proxy_session_key,
            source,
            source.url,
            request_headers=proxy_request_headers,
        )
        started = await proxy_session.start()
        if started:
            return True, ""
        reason = proxy_session.last_error or "proxy_start_failed"
        await _stop_proxy_session(force=True)
        return False, reason

    async def _attempt_start(effective_policy):
        nonlocal \
            output_queue, \
            should_start_cache_warm, \
            process, \
            writer_task, \
            stderr_task, \
            stdout_task, \
            first_output_chunk
        output_queue = asyncio.Queue(maxsize=8)
        started, proxy_start_reason = await _ensure_proxy_session()
        if not started:
            return False, None, proxy_start_reason
        if using_local_cache:
            command = CsoFfmpegCommandBuilder(
                effective_policy,
                pipe_input_format=resolve_vod_pipe_container(source, source_probe=source_probe),
                source_probe=source_probe,
            ).build_local_output_command(
                cache_entry.final_path,
                start_seconds=start_seconds,
                max_duration_seconds=max_duration_seconds,
                realtime=True,
            )
            logger.info(
                "Starting VOD local output stream item=%s source_id=%s profile=%s start_seconds=%s duration_seconds=%s path=%s command=%s",
                item_id,
                source.id,
                profile,
                int(start_seconds or 0),
                int(max_duration_seconds or 0) if max_duration_seconds is not None else None,
                cache_entry.final_path,
                command,
            )
        elif use_direct_upstream_input:
            cache_meta = await ensure_vod_cache_ready(cache_entry, request_headers=request_headers)
            should_start_cache_warm = bool(
                cache_meta.get("cacheable") and not cache_entry.complete and not cache_entry.downloader_running
            )
            command = CsoFfmpegCommandBuilder(
                effective_policy,
                pipe_output_format=effective_policy.get("container") or "mpegts",
                source_probe=source_probe,
            ).build_vod_channel_ingest_command(
                source.url,
                start_seconds=start_seconds,
                max_duration_seconds=max_duration_seconds,
                input_is_url=True,
                user_agent=resolve_cso_ingest_user_agent(config, source),
                request_headers=resolve_cso_ingest_headers(source),
                policy=effective_policy,
                seekable_url_input=True,
            )
            logger.info(
                "Starting VOD direct output stream item=%s source_id=%s profile=%s start_seconds=%s duration_seconds=%s url=%s command=%s",
                item_id,
                source.id,
                profile,
                int(start_seconds or 0),
                int(max_duration_seconds or 0) if max_duration_seconds is not None else None,
                source.url,
                redact_ingest_command_for_log(command),
            )
        else:
            pipe_input_format = pipe_container_from_content_type(
                proxy_session.content_type
            ) or resolve_vod_pipe_container(
                source,
                source_probe=source_probe,
            )
            command = CsoFfmpegCommandBuilder(
                effective_policy,
                pipe_input_format=pipe_input_format,
                source_probe=source_probe,
            ).build_output_command(start_seconds=start_seconds, max_duration_seconds=max_duration_seconds)
            logger.info(
                "Starting VOD proxy output stream item=%s source_id=%s profile=%s start_seconds=%s duration_seconds=%s command=%s",
                item_id,
                source.id,
                profile,
                int(start_seconds or 0),
                int(max_duration_seconds or 0) if max_duration_seconds is not None else None,
                command,
            )
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE if not use_direct_upstream_input else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception as exc:
            await _stop_proxy_session(force=True)
            await emit_channel_stream_event(
                source=source,
                session_id=proxy_session_key,
                event_type="playback_unavailable",
                severity="warning",
                details={
                    "reason": f"output_start_failed:{exc}",
                    "profile": profile,
                    "mode": "proxy_output",
                    **source_event_context(source, source_url=source.url),
                },
            )
            return build_cso_stream_plan(None, None, "Unable to start CSO output stream", 503)

        async def _write_proxy_to_ffmpeg():
            if proxy_session is None:
                return
            try:
                async for chunk in proxy_session.iter_bytes():
                    if not isinstance(chunk, (bytes, bytearray, memoryview)):
                        continue
                    if process.stdin is None or process.returncode is not None:
                        break
                    process.stdin.write(chunk)
                    await process.stdin.drain()
            except Exception as exc:
                logger.warning(
                    "VOD proxy output writer interrupted item=%s source_id=%s error=%s",
                    item.id if item is not None else None,
                    source.id,
                    exc,
                )
            finally:
                if process.stdin is not None:
                    try:
                        process.stdin.close()
                    except Exception:
                        pass

        async def _log_stderr():
            if process.stderr is None:
                return
            while True:
                line = await process.stderr.readline()
                if not line:
                    break
                text = line.decode(errors="ignore").rstrip()
                if text:
                    recent_stderr.append(text)
                if CsoOutputSession._should_log_ffmpeg_stderr_line(text):
                    logger.info("[vod-proxy-output-ffmpeg] %s", text)

        async def _read_stdout():
            if process.stdout is None:
                await output_queue.put(None)
                return
            try:
                while True:
                    chunk = await process.stdout.read(64 * 1024)
                    if not chunk:
                        break
                    await output_queue.put(bytes(chunk))
            finally:
                await output_queue.put(None)

        recent_stderr.clear()
        if not use_direct_upstream_input:
            writer_task = asyncio.create_task(_write_proxy_to_ffmpeg(), name=f"vod-proxy-output-writer-{connection_id}")
        else:
            writer_task = None
        stderr_task = asyncio.create_task(_log_stderr(), name=f"vod-proxy-output-stderr-{connection_id}")
        stdout_task = asyncio.create_task(_read_stdout(), name=f"vod-proxy-output-stdout-{connection_id}")
        await register_vod_proxy_output_disconnect(connection_id, _disconnect_active_output)
        if use_direct_upstream_input:
            return True, (process, writer_task, stderr_task, stdout_task, b""), ""
        startup_timeout_seconds = 8.0
        if not using_local_cache and int(start_seconds or 0) > 0:
            startup_timeout_seconds = min(180.0, max(20.0, float(start_seconds) / 12.0))
        first_chunk_task = asyncio.create_task(output_queue.get())
        wait_task = asyncio.create_task(process.wait())
        try:
            done, pending = await asyncio.wait(
                {first_chunk_task, wait_task},
                timeout=max(1.0, float(startup_timeout_seconds)),
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            started = False
            startup_failure_reason = "startup_timeout_no_output"
            first_output_chunk = b""
            if first_chunk_task in done and not first_chunk_task.cancelled():
                try:
                    first_output_chunk = first_chunk_task.result() or b""
                except Exception:
                    first_output_chunk = b""
                if first_output_chunk:
                    started = True
                    startup_failure_reason = ""
            elif wait_task in done and not wait_task.cancelled():
                try:
                    return_code = wait_task.result()
                except Exception:
                    return_code = process.returncode
                startup_failure_reason = f"ffmpeg_exit:{return_code}"
        finally:
            if not first_chunk_task.done():
                first_chunk_task.cancel()
            if not wait_task.done():
                wait_task.cancel()
        if started:
            if should_start_cache_warm:
                await start_vod_cache_download(
                    cache_entry, f"{proxy_session_key}:cache-warm", request_headers=request_headers
                )
            return True, (process, writer_task, stderr_task, stdout_task, first_output_chunk), ""
        await unregister_vod_proxy_output_disconnect(connection_id)
        await terminate_ffmpeg_process(process)
        if stderr_task is not None and not stderr_task.done():
            try:
                await asyncio.wait_for(asyncio.shield(stderr_task), timeout=0.5)
            except Exception:
                pass
        for task in (writer_task, stderr_task, stdout_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except BaseException:
                    pass
        process = None
        failure_summary = " | ".join(recent_stderr) or startup_failure_reason
        return False, None, failure_summary

    started, start_policy, result, failure_summary = await start_ffmpeg_with_hw_decode_fallback(
        base_policy,
        source_identity,
        _attempt_start,
    )
    if not started:
        await _stop_proxy_session(force=True)
        await emit_channel_stream_event(
            source=source,
            session_id=proxy_session_key,
            event_type="playback_unavailable",
            severity="warning",
            details={
                "reason": failure_summary or "output_start_failed",
                "profile": profile,
                "mode": "proxy_output",
                **source_event_context(source, source_url=source.url),
            },
        )
        return build_cso_stream_plan(None, None, "Unable to start CSO output stream", 503)
    policy = dict(start_policy)
    process, writer_task, stderr_task, stdout_task, first_output_chunk = result

    await emit_channel_stream_event(
        source=source,
        session_id=proxy_session_key,
        event_type="session_start",
        severity="info",
        details={
            "profile": profile,
            "connection_id": connection_id,
            "mode": "proxy_output",
            **source_event_context(source, source_url=source.url),
        },
    )

    event_loop = asyncio.get_running_loop()

    async def _generator():
        nonlocal should_start_cache_warm
        closing_via_generator_exit = False

        async def _cleanup():
            await unregister_vod_proxy_output_disconnect(connection_id)
            await _disconnect_active_output()
            await emit_channel_stream_event(
                source=source,
                session_id=proxy_session_key,
                event_type="session_end",
                severity="info",
                details={
                    "profile": profile,
                    "connection_id": connection_id,
                    "mode": "proxy_output",
                    **source_event_context(source, source_url=source.url),
                },
            )

        def _schedule_cleanup() -> None:
            asyncio.create_task(_cleanup())

        try:
            if first_output_chunk:
                if should_start_cache_warm:
                    await start_vod_cache_download(
                        cache_entry, f"{proxy_session_key}:cache-warm", request_headers=request_headers
                    )
                    should_start_cache_warm = False
                yield first_output_chunk
            while True:
                chunk = await output_queue.get()
                if not chunk:
                    break
                if should_start_cache_warm:
                    await start_vod_cache_download(
                        cache_entry, f"{proxy_session_key}:cache-warm", request_headers=request_headers
                    )
                    should_start_cache_warm = False
                yield chunk
        except GeneratorExit:
            closing_via_generator_exit = True
            raise
        finally:
            if closing_via_generator_exit:
                event_loop.call_soon(_schedule_cleanup)
            else:
                await _cleanup()

    return build_cso_stream_plan(_generator(), policy_content_type(policy), None, 200)
