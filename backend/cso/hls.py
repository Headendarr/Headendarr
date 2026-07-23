import asyncio
import hashlib
import logging
import socket
import time
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from urllib.parse import urljoin, urlparse

import aiohttp

from backend.hls_multiplexer import get_header_value
from backend.http_headers import sanitise_headers

logger = logging.getLogger("cso")

HLS_VIDEO_CODEC_PREFIXES = (
    "av01",
    "avc1",
    "avc3",
    "dvh1",
    "dvhe",
    "h264",
    "hevc",
    "hev1",
    "hvc1",
    "mpeg2",
    "mp4v",
    "theora",
    "vp8",
    "vp08",
    "vp9",
    "vp09",
)
HLS_AUDIO_CODEC_PREFIXES = (
    "ac-3",
    "alac",
    "ec-3",
    "fla",
    "mp4a",
    "opus",
    "vorbis",
)
HLS_GROUP_ATTRIBUTE_BY_TYPE = {
    "AUDIO": "AUDIO",
    "SUBTITLES": "SUBTITLES",
    "VIDEO": "VIDEO",
    "CLOSED-CAPTIONS": "CLOSED-CAPTIONS",
}
HLS_RETAINED_MASTER_TAGS = (
    "#EXTM3U",
    "#EXT-X-VERSION:",
    "#EXT-X-INDEPENDENT-SEGMENTS",
    "#EXT-X-START:",
    "#EXT-X-DEFINE:",
    "#EXT-X-SESSION-DATA:",
    "#EXT-X-SESSION-KEY:",
    "#EXT-X-CONTENT-STEERING:",
)
HLS_MEDIA_PLAYLIST_TAGS = (
    "#EXTINF",
    "#EXT-X-TARGETDURATION",
    "#EXT-X-MEDIA-SEQUENCE",
    "#EXT-X-PLAYLIST-TYPE",
    "#EXT-X-ENDLIST",
    "#EXT-X-MAP",
    "#EXT-X-PART",
)
HLS_CHILD_PROBE_CONCURRENCY = 3
HLS_CHILD_PLAYLIST_MAX_BYTES = 128 * 1024
HLS_CHILD_MEDIA_MAX_BYTES = 256 * 1024
HLS_CHILD_PROBE_TIMEOUT_SECONDS = 3
HLS_CHILD_PROBE_CACHE_MAX_ENTRIES = 256
HLS_CHILD_PROBE_POSITIVE_TTL_SECONDS = 300
HLS_CHILD_PROBE_NEGATIVE_TTL_SECONDS = 15

_child_probe_cache: OrderedDict[str, tuple[float, bool]] = OrderedDict()
_hls_runtime_counters = {
    "hls_selected_masters_total": 0,
    "hls_muxed_audio_selected_variants_total": 0,
    "hls_external_audio_selected_variants_total": 0,
    "hls_multiple_audio_presentations_total": 0,
    "hls_subtitle_presentations_total": 0,
    "hls_ambiguous_probe_attempts_total": 0,
    "hls_ambiguous_probe_cache_hits_total": 0,
    "hls_ambiguous_probe_timeouts_total": 0,
    "hls_ambiguous_probe_failures_total": 0,
    "hls_required_track_failures_total": 0,
    "hls_selected_master_cleanup_failures_total": 0,
}
_hls_runtime_counters_lock = asyncio.Lock()


async def increment_hls_runtime_counter(name: str) -> int:
    async with _hls_runtime_counters_lock:
        if name not in _hls_runtime_counters:
            raise ValueError(f"Unknown HLS runtime counter: {name}")
        _hls_runtime_counters[name] += 1
        return _hls_runtime_counters[name]


async def hls_runtime_metrics() -> dict[str, int]:
    async with _hls_runtime_counters_lock:
        return dict(_hls_runtime_counters)


class HlsPresentationError(ValueError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class HlsMediaRendition:
    original_media_line: str
    attributes: dict[str, str]
    type: str
    group_id: str
    name: str
    language: str
    uri: str
    resolved_uri: str
    declaration_position: int


@dataclass(frozen=True)
class HlsVariant:
    original_stream_inf: str
    attributes: dict[str, str]
    uri: str
    resolved_uri: str
    bandwidth: int
    average_bandwidth: int
    width: int
    height: int
    frame_rate: float
    codecs: tuple[str, ...]
    declaration_position: int
    video_confirmed_by_probe: bool = False
    exclusion_reason: str = ""

    def referenced_group_id(self, media_type: str) -> str:
        value = self.attributes.get(HLS_GROUP_ATTRIBUTE_BY_TYPE[media_type], "")
        if media_type == "CLOSED-CAPTIONS" and value.upper() == "NONE":
            return ""
        return value

    def has_video_metadata(self) -> bool:
        return bool(
            (self.width > 0 and self.height > 0)
            or any(codec.lower().startswith(HLS_VIDEO_CODEC_PREFIXES) for codec in self.codecs)
            or self.attributes.get("VIDEO")
            or self.video_confirmed_by_probe
        )

    def has_audio_metadata(self) -> bool:
        return any(codec.lower().startswith(HLS_AUDIO_CODEC_PREFIXES) for codec in self.codecs)

    def is_standalone_audio_only(self) -> bool:
        return bool(
            self.codecs
            and not self.has_video_metadata()
            and all(codec.lower().startswith(HLS_AUDIO_CODEC_PREFIXES) for codec in self.codecs)
        )


@dataclass(frozen=True)
class HlsSelectedPresentation:
    final_url: str
    selected_variant: HlsVariant
    selected_groups: dict[tuple[str, str], tuple[HlsMediaRendition, ...]]
    master_tags: tuple[str, ...]
    variant_position: int
    variant_count: int
    selection_reason: str

    @property
    def expected_external_audio(self) -> bool:
        return any(media_type == "AUDIO" and renditions for (media_type, _), renditions in self.selected_groups.items())

    @property
    def expected_audio_stream_count(self) -> int:
        external_audio_count = sum(
            len(renditions) for (media_type, _), renditions in self.selected_groups.items() if media_type == "AUDIO"
        )
        if external_audio_count:
            return external_audio_count
        return 1 if self.selected_variant.has_audio_metadata() else 0

    @property
    def expected_subtitles(self) -> bool:
        return any(
            media_type == "SUBTITLES" and renditions for (media_type, _), renditions in self.selected_groups.items()
        )

    def rendition_counts(self) -> dict[str, int]:
        counts = {media_type.lower(): 0 for media_type in HLS_GROUP_ATTRIBUTE_BY_TYPE}
        for (media_type, _), renditions in self.selected_groups.items():
            counts[media_type.lower()] += len(renditions)
        return counts

    def serialize(self) -> str:
        lines = list(self.master_tags)
        if not lines or lines[0] != "#EXTM3U":
            lines.insert(0, "#EXTM3U")
        selected_renditions = sorted(
            (rendition for renditions in self.selected_groups.values() for rendition in renditions),
            key=lambda rendition: rendition.declaration_position,
        )
        for rendition in selected_renditions:
            media_line = rendition.original_media_line
            if rendition.uri:
                media_line = _replace_attribute(media_line, "URI", rendition.resolved_uri)
            lines.append(media_line)
        lines.append(self.selected_variant.original_stream_inf)
        lines.append(self.selected_variant.resolved_uri)
        return "\n".join(lines) + "\n"


@dataclass(frozen=True)
class HlsMasterPresentation:
    final_url: str
    playlist_type: str
    master_tags: tuple[str, ...] = ()
    variants: tuple[HlsVariant, ...] = ()
    media_groups: dict[tuple[str, str], tuple[HlsMediaRendition, ...]] = field(default_factory=dict)

    def selectable_variants(self) -> list[HlsVariant]:
        video_variants = [variant for variant in self.variants if variant.has_video_metadata()]
        return sorted(
            video_variants,
            key=lambda variant: (
                variant.bandwidth,
                variant.declaration_position,
            ),
        )

    def selection_exclusions(self) -> list[tuple[HlsVariant, str]]:
        candidates = self.selectable_variants()
        candidate_positions = {variant.declaration_position for variant in candidates}
        exclusions = []
        for variant in self.variants:
            if variant.declaration_position in candidate_positions:
                continue
            reason = variant.exclusion_reason
            if not reason and variant.is_standalone_audio_only():
                reason = "standalone_audio_only"
            exclusions.append((variant, reason or "not_video_quality"))
        return exclusions

    def select(self, variant_position: int) -> HlsSelectedPresentation:
        if self.playlist_type != "master":
            raise HlsPresentationError("hls_variant_selection_not_applicable")
        variants = self.selectable_variants()
        if not variants:
            raise HlsPresentationError("hls_no_playable_variants")
        position = int(variant_position)
        if position < 0 or position >= len(variants):
            raise HlsPresentationError("hls_variant_position_invalid")
        selected_variant = variants[position]
        selected_groups: dict[tuple[str, str], tuple[HlsMediaRendition, ...]] = {}
        for media_type in HLS_GROUP_ATTRIBUTE_BY_TYPE:
            group_id = selected_variant.referenced_group_id(media_type)
            if not group_id:
                continue
            group_key = (media_type, group_id)
            renditions = self.media_groups.get(group_key)
            if not renditions:
                reason_type = media_type.lower().replace("-", "_")
                raise HlsPresentationError(f"hls_selected_{reason_type}_group_missing")
            selected_groups[group_key] = renditions
        selection_reason = "highest_bandwidth_video"
        if selected_variant.video_confirmed_by_probe:
            selection_reason = "highest_bandwidth_probed_video"
        return HlsSelectedPresentation(
            final_url=self.final_url,
            selected_variant=selected_variant,
            selected_groups=selected_groups,
            master_tags=self.master_tags,
            variant_position=position,
            variant_count=len(variants),
            selection_reason=selection_reason,
        )


async def discover_hls_variants(
    url: str,
    user_agent: str | None = None,
    request_headers: dict[str, str] | None = None,
    request_context_identity: str = "",
) -> HlsMasterPresentation | None:
    """Fetch and parse an HLS presentation.

    The historical function name remains as the live-ingest integration point,
    while its result now retains the complete master relationships.
    """
    parsed = urlparse(url or "")
    if not parsed.path.lower().endswith((".m3u8", ".m3u")):
        return None
    headers = sanitise_headers(request_headers)
    if user_agent and not get_header_value(headers, "User-Agent"):
        headers["User-Agent"] = user_agent
    timeout = aiohttp.ClientTimeout(total=6)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as client:
            async with client.get(url, allow_redirects=True, headers=headers) as response:
                if response.status >= 400:
                    raise HlsPresentationError(f"hls_discovery_http_{response.status}")
                payload_bytes = await response.content.read(HLS_CHILD_PLAYLIST_MAX_BYTES + 1)
                if len(payload_bytes) > HLS_CHILD_PLAYLIST_MAX_BYTES:
                    raise HlsPresentationError("hls_master_playlist_too_large")
                payload = payload_bytes.decode(response.charset or "utf-8", errors="replace")
                resolved_url = str(response.url or url)
                etag = response.headers.get("ETag", "")
                last_modified = response.headers.get("Last-Modified", "")
            presentation = parse_hls_presentation(resolved_url, payload)
            if presentation.playlist_type == "master" and not presentation.selectable_variants():
                presentation = await _probe_metadata_incomplete_variants(
                    client,
                    presentation,
                    headers,
                    etag,
                    last_modified,
                    request_context_identity,
                )
                if not presentation.selectable_variants():
                    raise HlsPresentationError("hls_no_confirmed_video_variant")
            return presentation
    except HlsPresentationError:
        raise
    except Exception as exc:
        category, summary = _network_failure_details(exc)
        logger.warning(
            "CSO HLS presentation discovery failed category=%s cause=%s",
            category,
            summary,
        )
        raise HlsPresentationError("hls_discovery_failed") from exc


def parse_hls_presentation(base_url: str, payload: str) -> HlsMasterPresentation:
    lines = [line.strip() for line in (payload or "").splitlines() if line.strip()]
    if not lines or lines[0] != "#EXTM3U":
        raise HlsPresentationError("hls_playlist_invalid")
    final_url = _validated_remote_uri(base_url, base_url)
    master_tags: list[str] = []
    variants: list[HlsVariant] = []
    media_groups: dict[tuple[str, str], list[HlsMediaRendition]] = {}
    pending_stream_inf = ""
    pending_attributes: dict[str, str] = {}
    is_media_playlist = False
    media_position = 0

    for line in lines:
        if line.startswith(HLS_MEDIA_PLAYLIST_TAGS):
            is_media_playlist = True
        if line.startswith(HLS_RETAINED_MASTER_TAGS):
            uri_attribute = ""
            if line.startswith(("#EXT-X-SESSION-KEY:", "#EXT-X-SESSION-DATA:")):
                uri_attribute = "URI"
            elif line.startswith("#EXT-X-CONTENT-STEERING:"):
                uri_attribute = "SERVER-URI"
            if uri_attribute:
                attributes = _parse_attribute_list(line.partition(":")[2])
                session_uri = attributes.get(uri_attribute)
                if session_uri:
                    resolved_session_uri = _validated_remote_uri(final_url, session_uri)
                    line = _replace_attribute(line, uri_attribute, resolved_session_uri)
            master_tags.append(line)
            continue
        if line.startswith("#EXT-X-MEDIA:"):
            attributes = _parse_attribute_list(line.partition(":")[2])
            media_type = attributes.get("TYPE", "").upper()
            group_id = attributes.get("GROUP-ID", "")
            if media_type not in HLS_GROUP_ATTRIBUTE_BY_TYPE or not group_id:
                raise HlsPresentationError("hls_media_rendition_malformed")
            uri = attributes.get("URI", "")
            resolved_uri = _validated_remote_uri(final_url, uri) if uri else ""
            rendition = HlsMediaRendition(
                original_media_line=line,
                attributes=attributes,
                type=media_type,
                group_id=group_id,
                name=attributes.get("NAME", ""),
                language=attributes.get("LANGUAGE", ""),
                uri=uri,
                resolved_uri=resolved_uri,
                declaration_position=media_position,
            )
            media_groups.setdefault((media_type, group_id), []).append(rendition)
            media_position += 1
            continue
        if line.startswith(("#EXT-X-I-FRAME-STREAM-INF:", "#EXT-X-IMAGE-STREAM-INF:")):
            continue
        if line.startswith("#EXT-X-STREAM-INF:"):
            pending_stream_inf = line
            pending_attributes = _parse_attribute_list(line.partition(":")[2])
            continue
        if line.startswith("#"):
            continue
        if not pending_stream_inf:
            continue
        resolved_uri = _validated_remote_uri(final_url, line)
        width, height = _parse_resolution(pending_attributes.get("RESOLUTION", ""))
        codecs = tuple(codec.strip() for codec in pending_attributes.get("CODECS", "").split(",") if codec.strip())
        variant = HlsVariant(
            original_stream_inf=pending_stream_inf,
            attributes=pending_attributes,
            uri=line,
            resolved_uri=resolved_uri,
            bandwidth=_parse_int(pending_attributes.get("BANDWIDTH")),
            average_bandwidth=_parse_int(pending_attributes.get("AVERAGE-BANDWIDTH")),
            width=width,
            height=height,
            frame_rate=_parse_float(pending_attributes.get("FRAME-RATE")),
            codecs=codecs,
            declaration_position=len(variants),
        )
        if variant.is_standalone_audio_only():
            variant = replace(variant, exclusion_reason="standalone_audio_only")
        elif not variant.has_video_metadata():
            variant = replace(variant, exclusion_reason="ambiguous_playback_metadata")
        variants.append(variant)
        pending_stream_inf = ""
        pending_attributes = {}

    if pending_stream_inf:
        raise HlsPresentationError("hls_variant_uri_missing")
    if variants:
        return HlsMasterPresentation(
            final_url=final_url,
            playlist_type="master",
            master_tags=tuple(master_tags),
            variants=tuple(variants),
            media_groups={key: tuple(value) for key, value in media_groups.items()},
        )
    if is_media_playlist:
        return HlsMasterPresentation(final_url=final_url, playlist_type="media")
    raise HlsPresentationError("hls_playlist_type_unknown")


async def _probe_metadata_incomplete_variants(
    client: aiohttp.ClientSession,
    presentation: HlsMasterPresentation,
    headers: dict[str, str],
    etag: str,
    last_modified: str,
    request_context_identity: str,
) -> HlsMasterPresentation:
    semaphore = asyncio.Semaphore(HLS_CHILD_PROBE_CONCURRENCY)
    ambiguous_variants = [
        variant
        for variant in presentation.variants
        if not variant.has_video_metadata() and not variant.is_standalone_audio_only()
    ]

    async def _probe(variant: HlsVariant) -> HlsVariant:
        cache_key = _child_probe_cache_key(
            presentation.final_url,
            variant,
            headers,
            etag,
            last_modified,
            request_context_identity,
        )
        cached = _get_child_probe_cache(cache_key)
        if cached is not None:
            await increment_hls_runtime_counter("hls_ambiguous_probe_cache_hits_total")
            logger.info(
                "CSO HLS child probe result variant_position=%s cache_state=hit result=%s category=cache",
                variant.declaration_position,
                "video_confirmed" if cached else "video_not_confirmed",
            )
            return replace(
                variant,
                video_confirmed_by_probe=cached,
                exclusion_reason="" if cached else "child_video_not_confirmed",
            )
        await increment_hls_runtime_counter("hls_ambiguous_probe_attempts_total")
        async with semaphore:
            confirmed = await _child_playlist_has_video(client, variant, headers)
        if not confirmed:
            await increment_hls_runtime_counter("hls_ambiguous_probe_failures_total")
        logger.info(
            "CSO HLS child probe result variant_position=%s cache_state=miss result=%s category=completed",
            variant.declaration_position,
            "video_confirmed" if confirmed else "video_not_confirmed",
        )
        if not confirmed or etag or last_modified:
            _set_child_probe_cache(cache_key, confirmed)
        return replace(
            variant,
            video_confirmed_by_probe=confirmed,
            exclusion_reason="" if confirmed else "child_video_not_confirmed",
        )

    if not ambiguous_variants:
        return presentation
    tasks = {asyncio.create_task(_probe(variant)): variant for variant in ambiguous_variants}
    completed, pending = await asyncio.wait(tasks, timeout=HLS_CHILD_PROBE_TIMEOUT_SECONDS)
    for task in pending:
        task.cancel()
        await increment_hls_runtime_counter("hls_ambiguous_probe_timeouts_total")
        variant = tasks[task]
        logger.warning(
            "CSO HLS child probe result variant_position=%s cache_state=miss "
            "result=video_not_confirmed category=timeout",
            variant.declaration_position,
        )
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    probed_variants = [task.result() for task in completed]
    probed_by_position = {variant.declaration_position: variant for variant in probed_variants}
    variants = tuple(probed_by_position.get(variant.declaration_position, variant) for variant in presentation.variants)
    return replace(presentation, variants=variants)


async def _child_playlist_has_video(
    client: aiohttp.ClientSession,
    variant: HlsVariant,
    headers: dict[str, str],
) -> bool:
    deadline = time.monotonic() + HLS_CHILD_PROBE_TIMEOUT_SECONDS
    try:
        async with client.get(
            variant.resolved_uri,
            allow_redirects=True,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=max(0.1, deadline - time.monotonic())),
        ) as response:
            if response.status >= 400:
                logger.warning(
                    "CSO HLS child probe failed variant_position=%s category=http_status status=%s",
                    variant.declaration_position,
                    response.status,
                )
                return False
            playlist_bytes = await response.content.read(HLS_CHILD_PLAYLIST_MAX_BYTES + 1)
            if len(playlist_bytes) > HLS_CHILD_PLAYLIST_MAX_BYTES:
                logger.warning(
                    "CSO HLS child probe failed variant_position=%s category=playlist_too_large",
                    variant.declaration_position,
                )
                return False
            final_url = str(response.url or variant.resolved_uri)
        lines = [line.strip() for line in playlist_bytes.decode("utf-8", errors="replace").splitlines()]
        media_uri = _first_child_media_uri(lines)
        if not media_uri:
            logger.warning(
                "CSO HLS child probe failed variant_position=%s category=media_uri_missing",
                variant.declaration_position,
            )
            return False
        media_url = _validated_remote_uri(final_url, media_uri)
        media_headers = dict(headers)
        media_headers["Range"] = f"bytes=0-{HLS_CHILD_MEDIA_MAX_BYTES - 1}"
        async with client.get(
            media_url,
            allow_redirects=True,
            headers=media_headers,
            timeout=aiohttp.ClientTimeout(total=max(0.1, deadline - time.monotonic())),
        ) as response:
            if response.status >= 400:
                logger.warning(
                    "CSO HLS child media probe failed variant_position=%s category=http_status status=%s",
                    variant.declaration_position,
                    response.status,
                )
                return False
            media_bytes = await response.content.read(HLS_CHILD_MEDIA_MAX_BYTES + 1)
            if len(media_bytes) > HLS_CHILD_MEDIA_MAX_BYTES:
                media_bytes = media_bytes[:HLS_CHILD_MEDIA_MAX_BYTES]
    except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
        category, summary = _network_failure_details(exc)
        logger.warning(
            "CSO HLS child probe failed variant_position=%s category=%s cause=%s",
            variant.declaration_position,
            category,
            summary,
        )
        return False
    return _media_bytes_have_video(media_bytes)


def _child_probe_cache_key(
    final_url: str,
    variant: HlsVariant,
    headers: dict[str, str],
    etag: str,
    last_modified: str,
    request_context_identity: str,
) -> str:
    digest = hashlib.sha256()
    for value in (
        final_url,
        str(variant.declaration_position),
        variant.resolved_uri,
        etag,
        last_modified,
        request_context_identity,
    ):
        digest.update(value.encode("utf-8", errors="replace"))
        digest.update(b"\0")
    for name in sorted(headers, key=str.lower):
        digest.update(name.lower().encode("utf-8", errors="replace"))
        digest.update(b"\0")
    return digest.hexdigest()


def _get_child_probe_cache(cache_key: str) -> bool | None:
    cached = _child_probe_cache.get(cache_key)
    if cached is None:
        return None
    expires_at, confirmed = cached
    if expires_at <= time.monotonic():
        _child_probe_cache.pop(cache_key, None)
        return None
    _child_probe_cache.move_to_end(cache_key)
    return confirmed


def _set_child_probe_cache(cache_key: str, confirmed: bool):
    ttl = HLS_CHILD_PROBE_POSITIVE_TTL_SECONDS if confirmed else HLS_CHILD_PROBE_NEGATIVE_TTL_SECONDS
    _child_probe_cache[cache_key] = (time.monotonic() + ttl, confirmed)
    _child_probe_cache.move_to_end(cache_key)
    while len(_child_probe_cache) > HLS_CHILD_PROBE_CACHE_MAX_ENTRIES:
        _child_probe_cache.popitem(last=False)


def _network_failure_details(exc: Exception) -> tuple[str, str]:
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
        return "timeout", type(exc).__name__
    if isinstance(
        exc,
        (
            aiohttp.ClientConnectorCertificateError,
            aiohttp.ClientConnectorSSLError,
            aiohttp.ClientSSLError,
        ),
    ):
        return "tls_failure", type(exc).__name__
    os_error = getattr(exc, "os_error", None)
    if isinstance(os_error, socket.gaierror):
        return "dns_failure", f"{type(exc).__name__}:errno={os_error.errno}"
    if isinstance(exc, aiohttp.ClientConnectorError):
        errno = getattr(os_error, "errno", None)
        summary = type(exc).__name__
        if errno is not None:
            summary = f"{summary}:errno={errno}"
        return "connection_failure", summary
    if isinstance(exc, aiohttp.ClientError):
        return "connection_failure", type(exc).__name__
    return "unexpected_failure", type(exc).__name__


def _first_child_media_uri(lines: list[str]) -> str:
    for line in lines:
        if line.startswith("#EXT-X-MAP:"):
            return _parse_attribute_list(line.partition(":")[2]).get("URI", "")
    for line in lines:
        if line and not line.startswith("#"):
            return line
    return ""


def _media_bytes_have_video(payload: bytes) -> bool:
    if not payload:
        return False
    if any(marker in payload for marker in (b"vide", b"avc1", b"avc3", b"hvc1", b"hev1", b"av01", b"vp09")):
        return True
    return _mpeg_ts_has_video_stream(payload)


def _mpeg_ts_has_video_stream(payload: bytes) -> bool:
    video_stream_types = {0x01, 0x02, 0x10, 0x1B, 0x24, 0x42, 0xD1}
    sync_offset = next(
        (
            offset
            for offset in range(min(188, len(payload)))
            if offset + 376 < len(payload)
            and payload[offset] == 0x47
            and payload[offset + 188] == 0x47
            and payload[offset + 376] == 0x47
        ),
        -1,
    )
    if sync_offset < 0:
        return False
    pmt_pids: set[int] = set()
    for packet_start in range(sync_offset, len(payload) - 187, 188):
        packet = payload[packet_start : packet_start + 188]
        if packet[0] != 0x47:
            continue
        pid = ((packet[1] & 0x1F) << 8) | packet[2]
        payload_unit_start = bool(packet[1] & 0x40)
        adaptation_control = (packet[3] >> 4) & 0x03
        if adaptation_control not in {1, 3}:
            continue
        position = 4
        if adaptation_control == 3:
            position += 1 + packet[4]
        if position >= len(packet):
            continue
        if payload_unit_start:
            position += 1 + packet[position]
        if position + 3 >= len(packet):
            continue
        if pid == 0 and packet[position] == 0x00:
            section_length = ((packet[position + 1] & 0x0F) << 8) | packet[position + 2]
            program_position = position + 8
            section_end = min(len(packet), position + 3 + section_length - 4)
            while program_position + 3 < section_end:
                program_number = (packet[program_position] << 8) | packet[program_position + 1]
                if program_number:
                    pmt_pids.add(((packet[program_position + 2] & 0x1F) << 8) | packet[program_position + 3])
                program_position += 4
        elif pid in pmt_pids and position + 11 < len(packet) and packet[position] == 0x02:
            section_length = ((packet[position + 1] & 0x0F) << 8) | packet[position + 2]
            program_info_length = ((packet[position + 10] & 0x0F) << 8) | packet[position + 11]
            stream_position = position + 12 + program_info_length
            section_end = min(len(packet), position + 3 + section_length - 4)
            while stream_position + 4 < section_end:
                stream_type = packet[stream_position]
                if stream_type in video_stream_types:
                    return True
                elementary_info_length = ((packet[stream_position + 3] & 0x0F) << 8) | packet[stream_position + 4]
                stream_position += 5 + elementary_info_length
    return False


def _split_attribute_fields(value: str) -> list[str]:
    fields: list[str] = []
    start = 0
    quoted = False
    escaped = False
    for position, character in enumerate(value):
        if escaped:
            escaped = False
            continue
        if character == "\\" and quoted:
            escaped = True
            continue
        if character == '"':
            quoted = not quoted
            continue
        if character == "," and not quoted:
            fields.append(value[start:position])
            start = position + 1
    fields.append(value[start:])
    return fields


def _parse_attribute_list(value: str) -> dict[str, str]:
    attributes: dict[str, str] = {}
    for field_value in _split_attribute_fields(value):
        name, separator, raw_value = field_value.partition("=")
        name = name.strip().upper()
        if not separator or not name:
            continue
        parsed_value = raw_value.strip()
        if len(parsed_value) >= 2 and parsed_value[0] == '"' and parsed_value[-1] == '"':
            parsed_value = parsed_value[1:-1].replace('\\"', '"').replace("\\\\", "\\")
        attributes[name] = parsed_value
    return attributes


def _replace_attribute(line: str, attribute_name: str, value: str) -> str:
    prefix, separator, attribute_text = line.partition(":")
    if not separator:
        raise HlsPresentationError("hls_attribute_line_malformed")
    replacement = f'{attribute_name}="{value.replace(chr(34), chr(92) + chr(34))}"'
    fields = _split_attribute_fields(attribute_text)
    replaced = False
    for position, field_value in enumerate(fields):
        name, field_separator, _ = field_value.partition("=")
        if field_separator and name.strip().upper() == attribute_name.upper():
            fields[position] = replacement
            replaced = True
            break
    if not replaced:
        fields.append(replacement)
    return f"{prefix}:{','.join(fields)}"


def _validated_remote_uri(base_url: str, uri: str) -> str:
    resolved_uri = urljoin(base_url, uri)
    scheme = urlparse(resolved_uri).scheme.lower()
    if scheme not in {"http", "https"}:
        raise HlsPresentationError("hls_uri_scheme_unsupported")
    return resolved_uri


def _parse_resolution(value: str) -> tuple[int, int]:
    width_text, separator, height_text = str(value or "").lower().partition("x")
    if not separator:
        return 0, 0
    return _parse_int(width_text), _parse_int(height_text)


def _parse_int(value: object) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _parse_float(value: object) -> float:
    try:
        return max(0.0, float(value or 0.0))
    except (TypeError, ValueError):
        return 0.0
