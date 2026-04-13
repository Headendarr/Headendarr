import asyncio
import logging
import re
import time
from collections import deque
from urllib.parse import urlparse

from sqlalchemy import select
from sqlalchemy.orm import joinedload

from backend.config import enable_cso_ingest_command_debug_logging
from backend.hls_multiplexer import get_header_value
from backend.http_headers import parse_headers_json
from backend.models import Channel, ChannelSource, Session
from backend.source_media import load_source_media_shape, persist_source_media_shape
from backend.utils import clean_key, clean_text, utc_now_naive

from .common import ByteBudgetQueue, build_cso_stream_plan, wait_process_exit_with_timeout
from .capacity import cso_capacity_registry, source_capacity_key, source_capacity_limit
from .constants import (
    CSO_HTTP_ERROR_THRESHOLD_DEFAULT,
    CSO_HTTP_ERROR_WINDOW_SECONDS_DEFAULT,
    CSO_INGEST_HISTORY_MAX_BYTES,
    CSO_INGEST_RECOVERY_RETRY_INTERVAL_SECONDS,
    CSO_INGEST_RECOVERY_RETRY_WINDOW_SECONDS,
    CSO_INGEST_SUBSCRIBER_QUEUE_MAX_BYTES,
    CSO_SOURCE_HOLD_DOWN_SECONDS,
    CSO_SPEED_STALE_SECONDS_DEFAULT,
    CSO_STALL_SECONDS_DEFAULT,
    CSO_STARTUP_GRACE_SECONDS_DEFAULT,
    CSO_UNAVAILABLE_SHOW_SLATE,
    CSO_UNDERSPEED_RATIO_DEFAULT,
    CSO_UNDERSPEED_WINDOW_SECONDS_DEFAULT,
    MPEGTS_CHUNK_BYTES,
)
from .events import emit_channel_stream_event, source_event_context
from .ffmpeg import (
    CsoFfmpegCommandBuilder,
    redact_ingest_command_for_log,
)
from .hls import discover_hls_variants
from .output import CsoOutputSession
from .policy import (
    policy_content_type,
    resolve_live_pipe_container,
    resolve_vod_pipe_container,
    segmented_hls_segment_type,
    source_uses_segmented_handoff,
)
from .segmented_handoff import SegmentedHandoffSession
from .slate import cso_unavailable_duration_seconds, should_allow_unavailable_slate
from .sources import (
    cso_source_from_channel_source,
    mark_cso_channel_source_temporarily_failed,
    order_cso_channel_sources,
    resolve_source_url_candidates,
)
from .types import CsoSource, CsoStartResult


logger = logging.getLogger("cso")

FFMPEG_SPEED_RE = re.compile(r"speed=\s*([0-9.]+)x")
HTTP_STATUS_CODE_RE = re.compile(r"\b([45]\d{2})\b")
FFMPEG_INPUT_RE = re.compile(r"^Input #\d+,\s*([^,]+)")
FFMPEG_VIDEO_STREAM_RE = re.compile(
    r"Stream #\d+:\d+(?:\[[^\]]+\])?: Video:\s*([a-zA-Z0-9_]+)(?:\s*\(([^)]*)\))?,\s*([^,]+),\s*(\d+)x(\d+)"
)
FFMPEG_AUDIO_STREAM_RE = re.compile(
    r"Stream #\d+:\d+(?:\[[^\]]+\])?: Audio:\s*([a-zA-Z0-9_]+)(?:\s*\(([^)]*)\))?,\s*(\d+)\s*Hz,\s*([^,]+)"
)
FFMPEG_FPS_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*fps")


async def resolve_channel_for_stream(channel_id):
    """Return the channel model for stream playback if it exists.

    This is the single channel lookup for CSO playback requests. Callers should
    reuse the returned model for profile resolution, activity metadata, and
    session subscription setup to avoid duplicate database queries.
    """
    async with Session() as session:
        result = await session.execute(
            select(Channel)
            .options(
                joinedload(Channel.sources).joinedload(ChannelSource.playlist),
                joinedload(Channel.sources).joinedload(ChannelSource.xc_account),
            )
            .where(Channel.id == channel_id)
        )
        return result.scalars().unique().one_or_none()


def resolve_cso_ingest_user_agent(config, source: CsoSource):
    playlist = source.playlist if source is not None else None
    playlist_user_agent = clean_text(getattr(playlist, "user_agent", ""))
    if playlist_user_agent:
        return playlist_user_agent

    settings = {}
    try:
        settings = config.read_settings() if config else {}
    except Exception:
        settings = {}
    defaults = settings.get("settings", {}).get("user_agents", [])
    if isinstance(defaults, list):
        for item in defaults:
            if not isinstance(item, dict):
                continue
            candidate = clean_text(item.get("value") or item.get("name"))
            if candidate:
                return candidate
    return "VLC/3.0.23 LibVLC/3.0.23"


def resolve_cso_ingest_headers(source: CsoSource):
    playlist = source.playlist if source is not None else None
    try:
        configured = parse_headers_json(getattr(playlist, "hls_proxy_headers", None))
    except ValueError:
        configured = {}
    return configured


class CsoIngestSession:
    def __init__(
        self,
        key,
        channel_id,
        sources,
        request_base_url,
        instance_id,
        capacity_owner_key,
        stream_key=None,
        username=None,
        allow_failover=True,
        ingest_user_agent=None,
        slate_session=None,
        vod_pipe_output_format_override="",
    ):
        self.key = key
        self.channel_id = channel_id
        self.sources = list(sources or [])
        self.request_base_url = request_base_url
        self.instance_id = instance_id
        self.capacity_owner_key = capacity_owner_key
        self.stream_key = stream_key
        self.username = username
        self.allow_failover = bool(allow_failover)
        self.ingest_user_agent = clean_text(ingest_user_agent)
        self.slate_session = slate_session
        self.process = None
        self.read_task = None
        self.stderr_task = None
        self.running = False
        self.lock = asyncio.Lock()
        self.last_activity = time.time()
        self.subscribers = {}
        self.lifecycle_references = set()
        self.history = deque()
        self.history_bytes = 0
        self.max_history_bytes = int(CSO_INGEST_HISTORY_MAX_BYTES)
        self.current_source = None
        self.current_source_url = ""
        self.current_capacity_key = None
        self.failed_source_until = {}
        self.last_error = None
        self.health_task = None
        self.last_chunk_ts = 0.0
        self.last_source_start_ts = 0.0
        self.low_speed_since = None
        self.last_ffmpeg_speed = None
        self.last_ffmpeg_speed_ts = 0.0
        self.health_failover_reason = None
        self.health_failover_details = None
        self.last_reader_end_reason = None
        self.last_reader_end_saw_data = False
        self.last_reader_end_return_code = None
        self.last_reader_end_ts = 0.0
        self._recent_ffmpeg_stderr = deque(maxlen=50)
        self.http_error_timestamps = deque(maxlen=200)
        self.hls_variants = []
        self.current_variant_position = None
        self.current_program_index = 0
        self.source_program_index = {}
        self.source_variant_position = {}
        self.startup_jump_done = False
        self.process_token = 0
        self.failover_failed_sources = set()
        self.failover_in_progress = False
        self.failover_exhausted = False
        self.session_start_ts = 0.0
        self.failover_start_ts = 0.0
        self.pending_switch_success = None
        self.current_attempt_start_ts = 0.0
        self.current_attempt_first_chunk_logged = False
        self.current_source_probe = {}
        self._current_source_probe_persisted = False
        self._current_source_probe_input_section_closed = False
        self.first_healthy_stream_seen = False
        self.vod_pipe_output_format_override = vod_pipe_output_format_override
        self.ingest_policy = {
            "output_mode": "force_remux",
            "container": "mpegts",
            "video_codec": "copy",
            "audio_codec": "copy",
            "subtitle_mode": "copy",
        }
        self.segmented_handoff_session = None

    def get_output_input_target(self):
        if self.segmented_handoff_session is None:
            return ""
        return self.segmented_handoff_session.input_path()

    def build_unavailable_stream_plan(
        self, policy, reason, detail_hint="", profile_name="", channel=None, source: CsoSource = None, status_code=503
    ):
        allow_unavailable_slate = should_allow_unavailable_slate(profile_name, channel=channel, source=source)
        if not CSO_UNAVAILABLE_SHOW_SLATE or not allow_unavailable_slate or self.slate_session is None:
            message = (
                "Channel unavailable due to connection limits"
                if reason == "capacity_blocked"
                else "Unable to start CSO stream"
            )
            return build_cso_stream_plan(None, None, message, status_code)

        reason_key = clean_key(reason, fallback="playback_unavailable")
        resolved_duration = cso_unavailable_duration_seconds(reason_key)
        unique_suffix = int(time.time() * 1000)

        async def _generator():
            self.slate_session.reason = reason_key
            self.slate_session.detail_hint = clean_text(detail_hint)
            self.slate_session.duration_seconds = resolved_duration
            output_session = CsoOutputSession(
                key=f"cso-terminal-output-{reason_key}-{unique_suffix}",
                channel_id=getattr(channel, "id", None) or (source.channel_id if source else self.channel_id),
                policy=policy,
                ingest_session=None,
                slate_session=self.slate_session,
                event_source=source,
                use_slate_as_input=True,
            )
            subscriber_id = f"{output_session.key}-subscriber"
            await output_session.start()
            queue = await output_session.add_client(subscriber_id, prebuffer_bytes=0)
            try:
                while True:
                    chunk = await queue.get()
                    if chunk is None:
                        break
                    yield chunk
            finally:
                try:
                    await output_session.remove_client(subscriber_id)
                except Exception:
                    pass
                try:
                    await output_session.stop(force=True)
                except Exception:
                    pass

        return build_cso_stream_plan(
            _generator(),
            policy_content_type(policy) or "application/octet-stream",
            None,
            200,
            cutoff_seconds=cso_unavailable_duration_seconds(reason),
            final_status_code=int(status_code or 503),
        )

    async def _refresh_sources_from_db(self):
        """Refresh the internal sources list from the database to capture state changes."""
        channel = await resolve_channel_for_stream(self.channel_id)
        if channel:
            async with self.lock:
                sources = await order_cso_channel_sources(list(channel.sources or []), channel_id=int(channel.id))
                self.sources = [cso_source_from_channel_source(s) for s in sources]
                logger.debug(
                    "CSO ingest refreshed sources channel=%s count=%s",
                    self.channel_id,
                    len(self.sources),
                )

    async def _handle_source_failure(self, source: CsoSource, reason, details=None):
        if source.source_type != "channel":
            return

        source_id = int(source.id or 0)
        if source_id <= 0 or int(self.channel_id or 0) <= 0:
            return

        await mark_cso_channel_source_temporarily_failed(self.channel_id, source_id)

        app = getattr(self, "app", None)
        if app is None:
            return

        try:
            from backend.channel_stream_health import schedule_background_health_check_for_source

            await schedule_background_health_check_for_source(
                app,
                source_id,
                reason=reason,
                details=details or {},
            )
        except Exception as exc:
            logger.warning(
                "CSO failed to queue background health check channel=%s source_id=%s reason=%s error=%s",
                self.channel_id,
                source_id,
                reason,
                exc,
            )

    async def start(self):
        async with self.lock:
            if self.running:
                return
            self.failover_failed_sources.clear()
            self.history.clear()
            self.history_bytes = 0
            logger.info(
                "CSO ingest start requested channel=%s sources=%s",
                self.channel_id,
                len(self.sources or []),
            )
            self.session_start_ts = time.time()
            self.failover_start_ts = 0.0
            self.failover_in_progress = False
            self.failover_exhausted = False
            self.pending_switch_success = None
            start_result = await self._start_best_source_unlocked(reason="initial_start")
            if not start_result.success:
                self.running = False
                self.last_error = start_result.reason or "no_available_source"
                self.failover_exhausted = True
                return

    async def _spawn_ingest_process(self, source_url, program_index, source: CsoSource = None):
        playlist = getattr(source, "playlist", None) if source is not None else None
        source_user_agent = clean_text(getattr(playlist, "user_agent", "")) or self.ingest_user_agent
        source_headers = resolve_cso_ingest_headers(source)
        source_user_agent = get_header_value(source_headers, "User-Agent") or source_user_agent

        # Load existing probe details from the adapter
        source_probe = {}
        if source is not None and source.probe_details:
            source_probe = source.probe_details
        elif source is not None:
            source_probe = load_source_media_shape(source)

        use_segmented_handoff = source_uses_segmented_handoff(source, source_probe=source_probe)
        if self.vod_pipe_output_format_override:
            pipe_format = self.vod_pipe_output_format_override
        else:
            pipe_format = resolve_live_pipe_container(source_probe=source_probe)
            if source is not None and source.source_type in {"vod_movie", "vod_episode"}:
                pipe_format = resolve_vod_pipe_container(
                    source,
                    source_probe=source_probe,
                )
        ingest_policy = {
            "output_mode": "force_remux",
            "container": pipe_format or "mpegts",
            "video_codec": "copy",
            "audio_codec": "copy",
            "subtitle_mode": "copy",
        }
        video_codec = clean_key(source_probe.get("video_codec"))
        audio_codec = clean_key(source_probe.get("audio_codec"))
        if video_codec:
            ingest_policy["video_codec"] = video_codec
        if audio_codec:
            ingest_policy["audio_codec"] = audio_codec
        self.ingest_policy = ingest_policy
        self.current_source_probe = dict(source_probe or {})
        self._current_source_probe_persisted = False
        self._current_source_probe_input_section_closed = False
        if use_segmented_handoff:
            segment_type = segmented_hls_segment_type(source, source_probe=source_probe)
            segmented_policy = {
                "output_mode": "force_remux",
                "container": "hls",
                "video_codec": "copy",
                "audio_codec": "copy",
                "subtitle_mode": "drop",
                "hls_segment_type": segment_type,
                "hls_playlist_mode": "live",
                "hls_list_size": 13,
            }
            self.ingest_policy = dict(segmented_policy)
            self.segmented_handoff_session = SegmentedHandoffSession(
                key=f"{self.key}-segmented",
                policy=segmented_policy,
                input_target=source_url,
                input_is_url=True,
                user_agent=source_user_agent,
                request_headers=source_headers,
            )
            logger.info(
                "Starting segmented CSO ingest channel=%s source=%s policy=(%s) source_probe=%s input=%s",
                self.channel_id,
                source.id if source is not None else getattr(self.current_source, "id", None),
                "segmented-handoff",
                self.current_source_probe or {},
                source_url,
            )
            started = await self.segmented_handoff_session.start()
            if not started:
                self.last_error = self.segmented_handoff_session.last_error or "segmented_handoff_start_failed"
                return None
            self.running = True
            self.process = self.segmented_handoff_session.process
            return self.process
        command = CsoFfmpegCommandBuilder(pipe_output_format=pipe_format).build_ingest_command(
            source_url,
            program_index=program_index,
            user_agent=source_user_agent,
            request_headers=source_headers,
        )
        logger.info(
            "Starting CSO ingest channel=%s source=%s policy=(%s) source_probe=%s command=%s",
            self.channel_id,
            source.id if source is not None else getattr(self.current_source, "id", None),
            "copy-only-ingest",
            self.current_source_probe or {},
            redact_ingest_command_for_log(command),
        )
        return await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    def _ingest_probe_is_complete(self, probe):
        data = dict(probe or {})
        return bool(
            clean_key(data.get("video_codec"))
            and int(data.get("width") or 0) > 0
            and int(data.get("height") or 0) > 0
            and float(data.get("fps") or 0.0) > 0.0
        )

    async def _persist_current_source_probe_if_ready(self):
        if self._current_source_probe_persisted:
            return
        if not self._ingest_probe_is_complete(self.current_source_probe):
            return

        source = self.current_source
        source_id = getattr(source, "id", None)
        source_type = getattr(source, "source_type", "channel")

        if not source_id:
            return

        persisted = False
        try:
            persisted = await persist_source_media_shape(
                source_id, self.current_source_probe, observed_at=utc_now_naive(), source_type=source_type
            )
        except Exception:
            pass

        if persisted:
            self._current_source_probe_persisted = True
            if enable_cso_ingest_command_debug_logging:
                logger.info(
                    "CSO ingest learned live media shape type=%s id=%s probe=%s",
                    source_type,
                    source_id,
                    dict(self.current_source_probe or {}),
                )
        elif enable_cso_ingest_command_debug_logging:
            logger.info(
                "CSO ingest learned live media shape but persist failed type=%s id=%s probe=%s",
                source_type,
                source_id,
                dict(self.current_source_probe or {}),
            )

    async def _update_current_source_probe_from_stderr(self, rendered):
        text = clean_text(rendered)
        if not text or self._current_source_probe_input_section_closed:
            return
        if text.startswith("Output #"):
            self._current_source_probe_input_section_closed = True
            if enable_cso_ingest_command_debug_logging and self.current_source_probe:
                logger.info(
                    "CSO ingest ffmpeg input inspection completed channel=%s source_id=%s probe=%s",
                    self.channel_id,
                    getattr(self.current_source, "id", None),
                    dict(self.current_source_probe or {}),
                )
            await self._persist_current_source_probe_if_ready()
            return

        updated = False
        input_match = FFMPEG_INPUT_RE.search(text)
        if input_match and not clean_key(self.current_source_probe.get("container")):
            self.current_source_probe["container"] = clean_key((input_match.group(1) or "").split(",", 1)[0])
            updated = True

        video_match = FFMPEG_VIDEO_STREAM_RE.search(text)
        if video_match:
            self.current_source_probe["video_codec"] = clean_key(video_match.group(1))
            self.current_source_probe["video_profile"] = clean_text(video_match.group(2))
            self.current_source_probe["pixel_format"] = clean_key(video_match.group(3).split("(", 1)[0])
            self.current_source_probe["width"] = int(video_match.group(4) or 0)
            self.current_source_probe["height"] = int(video_match.group(5) or 0)
            fps_match = FFMPEG_FPS_RE.search(text)
            if fps_match:
                try:
                    fps_value = float(fps_match.group(1))
                except Exception:
                    fps_value = 0.0
                if fps_value > 0:
                    self.current_source_probe["fps"] = fps_value
                    if not clean_text(self.current_source_probe.get("avg_frame_rate")):
                        rounded_fps = int(round(fps_value))
                        self.current_source_probe["avg_frame_rate"] = (
                            f"{rounded_fps}/1" if rounded_fps > 0 else clean_text(fps_match.group(1))
                        )
            updated = True

        audio_match = FFMPEG_AUDIO_STREAM_RE.search(text)
        if audio_match:
            self.current_source_probe["audio_codec"] = clean_key(audio_match.group(1))
            try:
                self.current_source_probe["audio_sample_rate"] = int(audio_match.group(3) or 0)
            except Exception:
                self.current_source_probe["audio_sample_rate"] = 0
            channel_layout = clean_key(audio_match.group(4))
            self.current_source_probe["audio_channel_layout"] = channel_layout
            if channel_layout == "mono":
                self.current_source_probe["audio_channels"] = 1
            elif channel_layout in {"stereo", "2 channels"}:
                self.current_source_probe["audio_channels"] = 2
            self.current_source_probe["has_audio"] = True
            updated = True

        if updated:
            if enable_cso_ingest_command_debug_logging:
                logger.info(
                    "CSO ingest ffmpeg metadata update channel=%s source_id=%s line=%s probe=%s",
                    self.channel_id,
                    getattr(self.current_source, "id", None),
                    text,
                    dict(self.current_source_probe or {}),
                )
            await self._persist_current_source_probe_if_ready()

    def is_hunting_for_stream(self):
        if self.failover_in_progress:
            return True
        if not self.first_healthy_stream_seen:
            return True
        if not self.running:
            return True
        if self.current_source is None:
            return True
        return False

    def _activate_process_unlocked(self, process):
        if self.segmented_handoff_session is not None:
            self.process = process
            self.running = True
            self.read_task = None
            self.stderr_task = None
            self.health_task = None
            self.history.clear()
            self.history_bytes = 0
            self.process_token += 1
            self.last_source_start_ts = time.time()
            self.current_attempt_start_ts = self.last_source_start_ts
            self.current_attempt_first_chunk_logged = False
            self.last_chunk_ts = self.last_source_start_ts
            self.low_speed_since = None
            self.last_ffmpeg_speed = None
            self.last_ffmpeg_speed_ts = self.last_source_start_ts
            self.http_error_timestamps.clear()
            self.health_failover_reason = None
            self.health_failover_details = None
            self.last_reader_end_reason = None
            self.last_reader_end_saw_data = False
            self.last_reader_end_return_code = None
            self.last_reader_end_ts = 0.0
            self.first_healthy_stream_seen = True
            return
        self.process = process
        self.running = True
        self.history.clear()
        self.history_bytes = 0
        self.process_token += 1
        token = self.process_token
        self.last_source_start_ts = time.time()
        self.current_attempt_start_ts = self.last_source_start_ts
        self.current_attempt_first_chunk_logged = False
        self.last_chunk_ts = self.last_source_start_ts
        self.low_speed_since = None
        self.last_ffmpeg_speed = None
        self.last_ffmpeg_speed_ts = self.last_source_start_ts
        self.http_error_timestamps.clear()
        self.health_failover_reason = None
        self.health_failover_details = None
        self.last_reader_end_reason = None
        self.last_reader_end_saw_data = False
        self.last_reader_end_return_code = None
        self.last_reader_end_ts = 0.0
        logger.info(
            "CSO ingest upstream connected channel=%s source_id=%s source_url=%s subscribers=%s elapsed_ms=%s failover_elapsed_ms=%s",
            self.channel_id,
            getattr(self.current_source, "id", None),
            self.current_source_url,
            len(self.subscribers),
            int(max(0.0, self.last_source_start_ts - float(self.session_start_ts or self.last_source_start_ts)) * 1000),
            int(
                max(0.0, self.last_source_start_ts - float(self.failover_start_ts or self.last_source_start_ts)) * 1000
            ),
        )
        self.read_task = asyncio.create_task(self._read_loop(token, process))
        self.stderr_task = asyncio.create_task(self._stderr_loop(token, process))
        self.health_task = asyncio.create_task(self._health_loop(token))

    def _eligible_source_ids_unlocked(self):
        eligible_ids = set()
        for source in self.sources:
            if source.id is None:
                continue
            playlist = source.playlist
            if playlist is not None and not bool(getattr(playlist, "enabled", False)):
                continue
            xc_account = source.xc_account
            if xc_account is not None and not bool(getattr(xc_account, "enabled", False)):
                continue
            if not source.url:
                continue
            eligible_ids.add(source.id)
        return eligible_ids

    async def _start_best_source_unlocked(
        self,
        reason,
        preferred_source_id=None,
        excluded_source_ids=None,
        ignore_hold_down=False,
    ):
        now = time.time()
        excluded_ids = set(excluded_source_ids or [])
        candidates = await order_cso_channel_sources(self.sources, channel_id=self.channel_id)
        if preferred_source_id is not None:
            preferred = [source for source in candidates if source.id == preferred_source_id]
            others = [source for source in candidates if source.id != preferred_source_id]
            candidates = preferred + others
        saw_capacity_block = False
        for source in candidates:
            if source.id in excluded_ids:
                continue
            hold_until = self.failed_source_until.get(source.id, 0)
            if not ignore_hold_down and hold_until > now:
                continue
            playlist = source.playlist
            if playlist is not None and not bool(getattr(playlist, "enabled", False)):
                continue
            xc_account = source.xc_account
            if xc_account is not None and not bool(getattr(xc_account, "enabled", False)):
                continue
            if not source.url:
                continue

            capacity_key = source_capacity_key(source)
            capacity_limit = source_capacity_limit(source)
            reserved = await cso_capacity_registry.try_reserve(
                capacity_key,
                self.capacity_owner_key,
                capacity_limit,
                slot_id=source.id,
            )
            if not reserved:
                saw_capacity_block = True
                continue

            source_urls = resolve_source_url_candidates(
                source,
                base_url=self.request_base_url,
                instance_id=self.instance_id,
                stream_key=self.stream_key,
                username=self.username,
            )
            if not source_urls:
                await cso_capacity_registry.release(capacity_key, self.capacity_owner_key, slot_id=source.id)
                continue

            process = None
            resolved_url = ""
            variants = []
            variant_position = None
            remembered_variant_position = self.source_variant_position.get(source.id)
            last_error = None
            for candidate_url in source_urls:
                variants = await discover_hls_variants(candidate_url)
                variant_position = None
                ingest_url = candidate_url
                url_path = urlparse(candidate_url).path.lower()
                if (url_path.endswith(".m3u8") or url_path.endswith(".m3u")) and variants:
                    if remembered_variant_position is not None and 0 <= int(remembered_variant_position) < len(
                        variants
                    ):
                        variant_position = int(remembered_variant_position)
                    if variant_position is None:
                        variant_position = len(variants) - 1
                    selected_variant = variants[variant_position]
                    program_index = int(selected_variant.get("ffmpeg_program_index") or 0)
                    ingest_url = (selected_variant.get("variant_url") or "").strip() or candidate_url
                    logger.info(
                        "CSO HLS ingest selected variant channel=%s source_id=%s "
                        "program_index=%s variant_position=%s variant_count=%s playlist_type=%s ingest_url=%s",
                        self.channel_id,
                        source.id,
                        program_index,
                        variant_position,
                        len(variants),
                        clean_text(selected_variant.get("playlist_type")) or "unknown",
                        ingest_url,
                    )
                else:
                    program_index = int(self.source_program_index.get(source.id) or 0)
                    if source.id is not None and source.id in self.source_program_index:
                        logger.info(
                            "CSO ingest variant discovery empty; reusing remembered program index "
                            "channel=%s source_id=%s program_index=%s",
                            self.channel_id,
                            source.id,
                            program_index,
                        )
                try:
                    process = await self._spawn_ingest_process(ingest_url, program_index, source=source)
                    resolved_url = ingest_url
                    break
                except Exception as exc:
                    last_error = exc
                    await self._handle_source_failure(source, "ingest_start_failed", {"error": str(exc)})
                    await emit_channel_stream_event(
                        channel_id=self.channel_id,
                        source=source,
                        session_id=self.key,
                        event_type="playback_unavailable",
                        severity="warning",
                        details={
                            "reason": "ingest_start_failed",
                            "pipeline": "ingest",
                            "error": str(exc),
                            **source_event_context(source, source_url=ingest_url),
                        },
                    )
                    continue

            if not process:
                if last_error:
                    logger.warning(
                        "CSO ingest failed for all URLs on source channel=%s source_id=%s error=%s",
                        self.channel_id,
                        source.id,
                        last_error,
                    )
                self.current_source = None
                self.current_source_url = ""
                self.current_capacity_key = None
                self.running = False
                self.process = None
                await cso_capacity_registry.release(capacity_key, self.capacity_owner_key, slot_id=source.id)
                continue
            old_capacity_key = self.current_capacity_key
            old_source_id = getattr(self.current_source, "id", None)
            self.current_source = source
            self.current_source_url = resolved_url
            self.current_capacity_key = capacity_key
            self.hls_variants = variants
            self.current_variant_position = variant_position
            self.current_program_index = program_index
            if source.id is not None:
                self.source_program_index[source.id] = int(program_index)
                if variant_position is not None:
                    self.source_variant_position[source.id] = int(variant_position)
            self.startup_jump_done = True
            if reason == "failover":
                self.pending_switch_success = {
                    "reason": reason,
                    "pipeline": "ingest",
                    "program_index": self.current_program_index,
                    "variant_count": len(self.hls_variants),
                }
            else:
                self.pending_switch_success = None
            self._activate_process_unlocked(process)
            if old_capacity_key:
                await cso_capacity_registry.release(old_capacity_key, self.capacity_owner_key, slot_id=old_source_id)
            return CsoStartResult(success=True)

        return CsoStartResult(success=False, reason="capacity_blocked" if saw_capacity_block else "no_available_source")

    async def _stderr_loop(self, token, process):
        if not process:
            return
        text_buffer = ""
        while True:
            try:
                chunk = await process.stderr.read(4096)
            except Exception:
                break
            if not chunk:
                break
            if token != self.process_token:
                break
            text_buffer += chunk.decode("utf-8", errors="replace")
            lines = re.split(r"[\r\n]+", text_buffer)
            text_buffer = lines.pop() if lines else ""
            for rendered in lines:
                rendered = rendered.strip()
                if not rendered:
                    continue
                self._recent_ffmpeg_stderr.append(rendered)
                await self._update_current_source_probe_from_stderr(rendered)
                progress_handled = False
                if "=" in rendered:
                    key, value = rendered.split("=", 1)
                    key = clean_key(key)
                    value = value.strip()
                    if key == "speed":
                        progress_handled = True
                        value = value.rstrip("xX")
                        try:
                            self.last_ffmpeg_speed = float(value)
                            self.last_ffmpeg_speed_ts = time.time()
                        except Exception:
                            self.last_ffmpeg_speed = None

                if not progress_handled:
                    speed_match = FFMPEG_SPEED_RE.search(rendered)
                    if speed_match:
                        try:
                            self.last_ffmpeg_speed = float(speed_match.group(1))
                            self.last_ffmpeg_speed_ts = time.time()
                        except Exception:
                            self.last_ffmpeg_speed = None

                lower = rendered.lower()
                if (
                    "http error" in lower
                    or "server returned" in lower
                    or "forbidden" in lower
                    or "unauthorized" in lower
                ):
                    status_codes = HTTP_STATUS_CODE_RE.findall(rendered)
                    if status_codes:
                        if any(code.startswith("4") or code.startswith("5") for code in status_codes):
                            self.http_error_timestamps.append(time.time())
                    else:
                        self.http_error_timestamps.append(time.time())
        rendered = text_buffer.strip()
        if rendered and token == self.process_token:
            self._recent_ffmpeg_stderr.append(rendered)
            await self._update_current_source_probe_from_stderr(rendered)
            lower = rendered.lower()
            if "http error" in lower or "server returned" in lower or "forbidden" in lower or "unauthorized" in lower:
                status_codes = HTTP_STATUS_CODE_RE.findall(rendered)
                if status_codes:
                    if any(code.startswith("4") or code.startswith("5") for code in status_codes):
                        self.http_error_timestamps.append(time.time())
                else:
                    self.http_error_timestamps.append(time.time())

    def _ffmpeg_error_summary(self):
        lines = [line for line in self._recent_ffmpeg_stderr if line]
        if not lines:
            return ""
        error_lines = [
            line
            for line in lines
            if any(token in line.lower() for token in ("error", "invalid", "failed", "could not", "unsupported"))
        ]
        selected = error_lines[-3:] if error_lines else lines[-3:]
        return " | ".join(selected)

    async def _health_loop(self, token):
        if not self.allow_failover:
            return
        while self.running and token == self.process_token:
            await asyncio.sleep(1.0)
            now = time.time()
            if (now - self.last_source_start_ts) < CSO_STARTUP_GRACE_SECONDS_DEFAULT:
                continue

            if self.http_error_timestamps:
                window_seconds = max(1, int(CSO_HTTP_ERROR_WINDOW_SECONDS_DEFAULT))
                threshold = max(1, int(CSO_HTTP_ERROR_THRESHOLD_DEFAULT))
                while self.http_error_timestamps and (now - self.http_error_timestamps[0]) > window_seconds:
                    self.http_error_timestamps.popleft()
                if len(self.http_error_timestamps) >= threshold:
                    await self._request_health_failover(
                        "http_error_burst",
                        {
                            "http_error_count": len(self.http_error_timestamps),
                            "threshold_count": threshold,
                            "window_seconds": window_seconds,
                        },
                    )
                    return

            # Treat stall as actionable only when we have sustained no-data and
            # ingest is not keeping up at realtime speed.
            if self.last_chunk_ts and (now - self.last_chunk_ts) >= CSO_STALL_SECONDS_DEFAULT:
                speed = self.last_ffmpeg_speed
                speed_age = now - float(self.last_ffmpeg_speed_ts or 0.0)
                speed_stale_seconds = max(1, int(CSO_SPEED_STALE_SECONDS_DEFAULT))
                speed_is_stale = speed_age >= speed_stale_seconds
                if speed is not None and not speed_is_stale and speed >= 1.0:
                    continue
                await self._request_health_failover(
                    "stall_timeout",
                    {
                        "stall_seconds": round(now - self.last_chunk_ts, 2),
                        "threshold_seconds": CSO_STALL_SECONDS_DEFAULT,
                        "speed": speed,
                        "speed_stale": speed_is_stale,
                        "speed_age_seconds": round(speed_age, 2),
                        "speed_stale_threshold_seconds": speed_stale_seconds,
                    },
                )
                return

            speed = self.last_ffmpeg_speed
            speed_age = now - float(self.last_ffmpeg_speed_ts or 0.0)
            if speed is None or speed_age >= max(1, int(CSO_SPEED_STALE_SECONDS_DEFAULT)):
                self.low_speed_since = None
                continue

            if speed < CSO_UNDERSPEED_RATIO_DEFAULT:
                if self.low_speed_since is None:
                    self.low_speed_since = now
                elif (now - self.low_speed_since) >= CSO_UNDERSPEED_WINDOW_SECONDS_DEFAULT:
                    await self._request_health_failover(
                        "under_speed",
                        {
                            "speed": speed,
                            "threshold_ratio": CSO_UNDERSPEED_RATIO_DEFAULT,
                            "window_seconds": CSO_UNDERSPEED_WINDOW_SECONDS_DEFAULT,
                        },
                    )
                    return
            else:
                self.low_speed_since = None

    async def _request_health_failover(self, reason, details):
        async with self.lock:
            if not self.running or not self.process:
                return
            if self.health_failover_reason:
                return
            self.health_failover_reason = reason
            self.health_failover_details = details or {}
            process = self.process
            source = self.current_source
            source_url = self.current_source_url

        logger.warning(
            "CSO ingest health-triggered failover channel=%s source_id=%s reason=%s details=%s",
            self.channel_id,
            getattr(self.current_source, "id", None),
            reason,
            details,
        )
        await emit_channel_stream_event(
            channel_id=self.channel_id,
            source=source,
            session_id=self.key,
            event_type="health_actioned",
            severity="warning",
            details={
                "reason": reason,
                "pipeline": "ingest",
                "action": "trigger_failover",
                **(details or {}),
                **source_event_context(source, source_url=source_url),
            },
        )
        try:
            process.terminate()
        except Exception:
            pass

    async def _read_loop(self, token, process):
        saw_data = False
        return_code = None
        try:
            while self.running and token == self.process_token and process and process.stdout:
                chunk = await process.stdout.read(MPEGTS_CHUNK_BYTES)
                if not chunk:
                    break
                if not saw_data and not self.current_attempt_first_chunk_logged:
                    now_value = time.time()
                    logger.info(
                        "CSO ingest first chunk channel=%s source_id=%s bytes=%s elapsed_ms=%s connect_elapsed_ms=%s failover_elapsed_ms=%s failover_in_progress=%s",
                        self.channel_id,
                        self.current_source.id if self.current_source is not None else None,
                        len(chunk),
                        int(max(0.0, now_value - float(self.session_start_ts or now_value)) * 1000),
                        int(max(0.0, now_value - float(self.current_attempt_start_ts or now_value)) * 1000),
                        int(max(0.0, now_value - float(self.failover_start_ts or now_value)) * 1000),
                        bool(self.failover_in_progress),
                    )
                    self.current_attempt_first_chunk_logged = True
                    self.first_healthy_stream_seen = True
                    pending_switch_success = self.pending_switch_success
                    self.pending_switch_success = None
                    if pending_switch_success:
                        await emit_channel_stream_event(
                            channel_id=self.channel_id,
                            source=self.current_source,
                            session_id=self.key,
                            event_type="switch_success",
                            severity="info",
                            details={
                                **pending_switch_success,
                                **source_event_context(
                                    self.current_source,
                                    source_url=self.current_source_url,
                                ),
                            },
                        )
                saw_data = True
                self.last_chunk_ts = time.time()
                await self._broadcast(chunk)
        finally:
            if process:
                try:
                    return_code = process.returncode
                    if return_code is None:
                        return_code = await process.wait()
                except Exception:
                    return_code = None

        if token != self.process_token:
            return

        self.last_reader_end_reason = "ingest_reader_ended"
        self.last_reader_end_saw_data = bool(saw_data)
        self.last_reader_end_return_code = return_code
        self.last_reader_end_ts = time.time()

        async with self.lock:
            has_subscribers = bool(self.subscribers or self.lifecycle_references)
        if not has_subscribers:
            logger.info(
                "CSO ingest channel=%s reader ended with no subscribers or lifecycle references (saw_data=%s return_code=%s)",
                self.channel_id,
                saw_data,
                return_code,
            )
            await self.stop(force=True)
            return

        failover_reason = self.health_failover_reason or "ingest_reader_ended"
        failover_details = self.health_failover_details or {}
        if return_code not in (None, 0):
            logger.warning(
                "CSO ingest non-zero exit channel=%s return_code=%s reason=%s stderr=%s",
                self.channel_id,
                return_code,
                failover_reason,
                self._ffmpeg_error_summary() or "n/a",
            )
        switched = await self._switch_source_after_failure(
            reason=failover_reason,
            return_code=return_code,
            saw_data=saw_data,
            details=failover_details,
        )
        if switched:
            return
        logger.info(
            "CSO ingest channel=%s reader ended (saw_data=%s return_code=%s)",
            self.channel_id,
            saw_data,
            return_code,
        )
        await self.stop(force=True)

    async def _switch_source_after_failure(self, reason, return_code, saw_data, details=None):
        graceful_reader_end = bool(reason == "ingest_reader_ended" and saw_data and return_code == 0)
        self.failover_exhausted = False
        async with self.lock:
            if not self.subscribers and not self.lifecycle_references:
                old_capacity_key = self.current_capacity_key
                self.current_source = None
                self.current_source_url = ""
                self.current_capacity_key = None
                self.process = None
                self.running = False
                if old_capacity_key:
                    await cso_capacity_registry.release(old_capacity_key, self.capacity_owner_key)
                return False

            failed_source = self.current_source
            failed_source_id = failed_source.id if failed_source is not None else None
            if failed_source_id is not None:
                self.failover_failed_sources.add(failed_source_id)
            ffmpeg_error = self._ffmpeg_error_summary()
            ffmpeg_error_lower = (ffmpeg_error or "").lower()
            is_connectivity_startup_failure = (
                reason == "ingest_reader_ended"
                and return_code not in (None, 0)
                and any(
                    token in ffmpeg_error_lower
                    for token in (
                        "connection refused",
                        "timed out",
                        "network is unreachable",
                        "name or service not known",
                        "could not resolve",
                        "forbidden",
                        "unauthorized",
                        "http error",
                        "server returned",
                        "invalid data",
                    )
                )
            )

            # Apply source hold-down only for health-triggered failover. For generic ingest
            # exits we allow immediate same-source restart to avoid tearing down clients,
            # except startup/connectivity failures where immediate same-source retry
            # causes endless loops on an unavailable upstream.
            multi_source_channel = len(self.sources or []) > 1
            hold_down_applicable = reason in {"under_speed", "stall_timeout"} or is_connectivity_startup_failure
            hold_down_applied = bool(failed_source_id and multi_source_channel and hold_down_applicable)
            terminal_startup_failure = bool(is_connectivity_startup_failure and not multi_source_channel)

            if hold_down_applied:
                self.failed_source_until[failed_source_id] = time.time() + CSO_SOURCE_HOLD_DOWN_SECONDS

            old_capacity_key = self.current_capacity_key
            self.current_source = None
            self.current_source_url = ""
            self.current_capacity_key = None
            self.hls_variants = []
            self.current_variant_position = None
            self.current_program_index = 0
            self.startup_jump_done = False
            self.pending_switch_success = None
            self.process = None
            self.running = bool(self.allow_failover and not terminal_startup_failure)
            if old_capacity_key:
                await cso_capacity_registry.release(
                    old_capacity_key,
                    self.capacity_owner_key,
                    slot_id=failed_source_id,
                )

        if not self.allow_failover:
            if graceful_reader_end:
                logger.info(
                    "CSO ingest graceful reader end channel=%s source_id=%s saw_data=%s return_code=%s",
                    self.channel_id,
                    failed_source_id,
                    saw_data,
                    return_code,
                )
                return False
            event_type = "capacity_blocked" if reason == "capacity_blocked" else "playback_unavailable"
            await emit_channel_stream_event(
                channel_id=self.channel_id,
                source=failed_source,
                session_id=self.key,
                event_type=event_type,
                severity="warning",
                details={
                    "reason": reason,
                    "return_code": return_code,
                    "saw_data": saw_data,
                    "pipeline": "ingest",
                    "ffmpeg_error": ffmpeg_error or None,
                    **(details or {}),
                    **source_event_context(failed_source),
                },
            )
            return False

        if terminal_startup_failure:
            await emit_channel_stream_event(
                channel_id=self.channel_id,
                source=failed_source,
                session_id=self.key,
                event_type="playback_unavailable",
                severity="warning",
                details={
                    "reason": reason,
                    "return_code": return_code,
                    "saw_data": saw_data,
                    "pipeline": "ingest",
                    "ffmpeg_error": ffmpeg_error or None,
                    **(details or {}),
                    **source_event_context(failed_source),
                },
            )
            self.failover_in_progress = False
            self.failover_exhausted = True
            self.running = False
            return False

        await emit_channel_stream_event(
            channel_id=self.channel_id,
            source=failed_source,
            session_id=self.key,
            event_type="switch_attempt",
            severity="warning",
            details={
                "reason": reason,
                "return_code": return_code,
                "saw_data": saw_data,
                "pipeline": "ingest",
                "ffmpeg_error": ffmpeg_error or None,
                **(details or {}),
                **source_event_context(failed_source),
            },
        )
        logger.info(
            "CSO ingest failover decision channel=%s reason=%s failed_source_id=%s hold_down_applied=%s elapsed_ms=%s",
            self.channel_id,
            reason,
            failed_source_id,
            hold_down_applied,
            int(max(0.0, time.time() - float(self.session_start_ts or time.time())) * 1000),
        )
        if failed_source is not None and reason != "capacity_blocked":
            await self._handle_source_failure(
                failed_source,
                reason,
                {
                    "return_code": return_code,
                    "saw_data": saw_data,
                    **(details or {}),
                },
            )
        self.failover_start_ts = time.time()
        self.failover_in_progress = True

        await self._refresh_sources_from_db()

        # If a single-source channel (or if only one source is currently enabled)
        # exits gracefully with code 0, allow an immediate restart of that same source
        # to bridge the upstream disconnection without cycling through others or holding down.
        if graceful_reader_end:
            async with self.lock:
                eligible_ids = self._eligible_source_ids_unlocked()
            if len(eligible_ids) == 1 and failed_source_id in eligible_ids:
                logger.info(
                    "CSO ingest immediate restart of only eligible source after graceful end channel=%s source_id=%s",
                    self.channel_id,
                    failed_source_id,
                )
                async with self.lock:
                    self.failover_failed_sources.clear()
                    start_result = await self._start_best_source_unlocked(reason="failover", ignore_hold_down=True)
                    if start_result.success:
                        self.running = True
                        return True

        deadline = time.time() + CSO_INGEST_RECOVERY_RETRY_WINDOW_SECONDS
        last_result = CsoStartResult(success=False, reason="no_available_source")
        while True:
            async with self.lock:
                has_subscribers = bool(self.subscribers or self.lifecycle_references)
                eligible_ids = self._eligible_source_ids_unlocked()
                cycle_failed_ids = set(self.failover_failed_sources).intersection(eligible_ids)
                untried_ids = eligible_ids.difference(cycle_failed_ids)
                recycle_failed_sources = bool(eligible_ids) and not bool(untried_ids)
                excluded_ids = cycle_failed_ids if untried_ids else set()
                if recycle_failed_sources:
                    # All currently eligible sources have failed at least once in this
                    # cycle, so recycle the list and allow immediate retries.
                    self.failover_failed_sources.clear()
                start_result = await self._start_best_source_unlocked(
                    reason="failover",
                    excluded_source_ids=excluded_ids,
                    ignore_hold_down=recycle_failed_sources,
                )
                if start_result.success:
                    logger.info(
                        "CSO ingest failover started replacement channel=%s recycled_cycle=%s elapsed_ms=%s failover_elapsed_ms=%s",
                        self.channel_id,
                        recycle_failed_sources,
                        int(max(0.0, time.time() - float(self.session_start_ts or time.time())) * 1000),
                        int(max(0.0, time.time() - float(self.failover_start_ts or time.time())) * 1000),
                    )
                    self.running = True
                    self.failover_in_progress = False
                    self.failover_exhausted = False
                    return True
            last_result = start_result

            if not has_subscribers:
                return False
            if time.time() >= deadline:
                break
            await asyncio.sleep(CSO_INGEST_RECOVERY_RETRY_INTERVAL_SECONDS)

        event_type = "capacity_blocked" if last_result.reason == "capacity_blocked" else "playback_unavailable"
        await emit_channel_stream_event(
            channel_id=self.channel_id,
            source=failed_source,
            session_id=self.key,
            event_type=event_type,
            severity="warning",
            details={
                "reason": last_result.reason,
                "after_failure_reason": reason,
                "pipeline": "ingest",
                "ffmpeg_error": ffmpeg_error or None,
                **(details or {}),
                **source_event_context(failed_source),
            },
        )
        self.failover_in_progress = False
        self.failover_exhausted = True
        self.running = False
        return False

    async def _broadcast(self, chunk):
        if not chunk:
            return
        self.last_activity = time.time()
        subscriber_queues = []
        async with self.lock:
            self.history.append(chunk)
            self.history_bytes += len(chunk)
            while self.history_bytes > self.max_history_bytes and self.history:
                old = self.history.popleft()
                self.history_bytes -= len(old)
            subscriber_queues = list(self.subscribers.values())
        for q in subscriber_queues:
            await q.put_drop_oldest(chunk)

    async def add_subscriber(self, subscriber_id, prebuffer_bytes=0):
        if self.segmented_handoff_session is not None:
            raise RuntimeError("segmented_handoff_has_no_subscriber_queue")
        async with self.lock:
            q = ByteBudgetQueue(max_bytes=CSO_INGEST_SUBSCRIBER_QUEUE_MAX_BYTES)
            if prebuffer_bytes > 0 and self.history:
                total = 0
                items = []
                for chunk in reversed(self.history):
                    items.append(chunk)
                    total += len(chunk)
                    if total >= prebuffer_bytes:
                        break
                for chunk in reversed(items):
                    await q.put_drop_oldest(chunk)
            self.subscribers[subscriber_id] = q
            subscriber_count = len(self.subscribers)
            source_id = getattr(self.current_source, "id", None)
            source_url = self.current_source_url
        logger.info(
            "CSO ingest subscriber added channel=%s ingest_key=%s subscriber=%s subscribers=%s source_id=%s source_url=%s",
            self.channel_id,
            self.key,
            subscriber_id,
            subscriber_count,
            source_id,
            source_url,
        )
        return q

    async def remove_subscriber(self, subscriber_id):
        async with self.lock:
            self.subscribers.pop(subscriber_id, None)
            remaining = len(self.subscribers)
            lifecycle_references = len(self.lifecycle_references)
            source_id = getattr(self.current_source, "id", None)
            source_url = self.current_source_url
        logger.info(
            "CSO ingest subscriber removed channel=%s ingest_key=%s subscriber=%s subscribers=%s lifecycle_references=%s source_id=%s source_url=%s",
            self.channel_id,
            self.key,
            subscriber_id,
            remaining,
            lifecycle_references,
            source_id,
            source_url,
        )
        if remaining == 0 and lifecycle_references == 0:
            await self.stop(force=True)
        return remaining

    async def add_lifecycle_reference(self, reference_id: str):
        async with self.lock:
            self.lifecycle_references.add(str(reference_id))
            lifecycle_references = len(self.lifecycle_references)
            subscriber_count = len(self.subscribers)
            source_id = getattr(self.current_source, "id", None)
            source_url = self.current_source_url
            self.last_activity = time.time()
        logger.info(
            "CSO ingest lifecycle reference added channel=%s ingest_key=%s reference=%s subscribers=%s lifecycle_references=%s source_id=%s source_url=%s",
            self.channel_id,
            self.key,
            reference_id,
            subscriber_count,
            lifecycle_references,
            source_id,
            source_url,
        )

    async def remove_lifecycle_reference(self, reference_id: str) -> int:
        async with self.lock:
            self.lifecycle_references.discard(str(reference_id))
            lifecycle_references = len(self.lifecycle_references)
            subscriber_count = len(self.subscribers)
            source_id = getattr(self.current_source, "id", None)
            source_url = self.current_source_url
        logger.info(
            "CSO ingest lifecycle reference removed channel=%s ingest_key=%s reference=%s subscribers=%s lifecycle_references=%s source_id=%s source_url=%s",
            self.channel_id,
            self.key,
            reference_id,
            subscriber_count,
            lifecycle_references,
            source_id,
            source_url,
        )
        if subscriber_count == 0 and lifecycle_references == 0:
            await self.stop(force=True)
        return lifecycle_references

    async def stop(self, force=False):
        async with self.lock:
            if not self.running and not self.process and not self.subscribers and not self.lifecycle_references:
                return
            if not force and (self.subscribers or self.lifecycle_references):
                return
            self.running = False
            process = self.process
            self.process = None
            capacity_key = self.current_capacity_key
            self.current_capacity_key = None
            source_id = getattr(self.current_source, "id", None)
            source_url = self.current_source_url
            self.current_source = None
            self.current_source_url = ""
            self.hls_variants = []
            self.current_variant_position = None
            self.current_program_index = 0
            self.startup_jump_done = False
            self.failover_failed_sources.clear()
            self.failover_in_progress = False
            self.failover_exhausted = False
            self.pending_switch_success = None
            segmented_handoff_session = self.segmented_handoff_session
            self.segmented_handoff_session = None
            subscriber_count = len(self.subscribers)
        # Release capacity immediately so other channels are not blocked while
        # this ingest session drains/tears down.
        if capacity_key:
            await cso_capacity_registry.release(
                capacity_key,
                self.capacity_owner_key,
                slot_id=source_id,
            )
        await cso_capacity_registry.release_all(self.capacity_owner_key)

        logger.info(
            "Stopping CSO ingest channel=%s ingest_key=%s source_id=%s source_url=%s subscribers=%s force=%s",
            self.channel_id,
            self.key,
            source_id,
            source_url,
            subscriber_count,
            force,
        )
        return_code = None
        if segmented_handoff_session is not None:
            await segmented_handoff_session.stop(force=True)
        if process:
            try:
                process.terminate()
                return_code = await wait_process_exit_with_timeout(process, timeout_seconds=2.0)
            except Exception:
                try:
                    process.kill()
                    return_code = await wait_process_exit_with_timeout(process, timeout_seconds=2.0)
                except Exception:
                    logger.warning(
                        "CSO ingest process did not exit after kill channel=%s ingest_key=%s",
                        self.channel_id,
                        self.key,
                    )
                    pass
        health_task = self.health_task
        self.health_task = None
        if health_task and not health_task.done():
            health_task.cancel()
            try:
                await health_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        logger.info(
            "CSO ingest upstream disconnected channel=%s ingest_key=%s source_id=%s return_code=%s",
            self.channel_id,
            self.key,
            source_id,
            return_code,
        )
        async with self.lock:
            self.history.clear()
            self.history_bytes = 0
            for q in self.subscribers.values():
                await q.put_eof()
