import re
from dataclasses import dataclass

from backend.hls_multiplexer import get_header_value
from backend.utils import clean_text


_CONTENT_RANGE_RE = re.compile(r"^bytes\s+(\d+)-(\d+)/(\d+|\*)$", re.IGNORECASE)
_NON_MEDIA_CONTENT_TYPES = (
    "application/json",
    "application/problem+json",
    "application/xml",
    "text/",
)
_MEDIA_CONTENT_TYPES = (
    "audio/",
    "video/",
    "application/octet-stream",
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
)


@dataclass(frozen=True)
class VodUpstreamValidation:
    accepted: bool
    classification: str
    content_type: str
    range_start: int | None = None
    range_end: int | None = None
    total_size: int | None = None
    restart_from_zero: bool = False


def _content_type_base(headers) -> str:
    return clean_text(get_header_value(headers, "Content-Type")).partition(";")[0].strip().lower()


def _prefix_classification(prefix: bytes) -> str:
    inspected = bytes(prefix or b"")[:1024].lstrip(b"\xef\xbb\xbf\t\r\n ")
    lowered = inspected[:256].lower()
    if lowered.startswith((b"<!doctype html", b"<html", b"<head", b"<body")):
        return "html"
    if lowered.startswith((b"<?xml", b"<error", b"<response")):
        return "xml"
    if lowered.startswith((b"{", b"[")):
        return "json"
    if (
        inspected.startswith(b"\x1aE\xdf\xa3")
        or inspected.startswith((b"FLV", b"OggS", b"\x00\x00\x01\xba", b"\x00\x00\x01\xb3"))
        or inspected[4:8] == b"ftyp"
        or (inspected.startswith(b"RIFF") and inspected[8:12] in {b"AVI ", b"WAVE"})
        or inspected.startswith(b"ID3")
        or (len(inspected) > 188 and inspected[0] == 0x47 and inspected[188] == 0x47)
    ):
        return "media"
    if inspected and all(byte in b"\t\r\n" or 32 <= byte < 127 for byte in inspected[:128]):
        return "text"
    return "unknown"


def validate_vod_upstream_response(
    status_code: int,
    headers,
    requested_offset: int = 0,
    body_prefix: bytes = b"",
    request_method: str = "GET",
    allow_restart_from_zero: bool = False,
) -> VodUpstreamValidation:
    status = int(status_code or 0)
    offset = max(0, int(requested_offset or 0))
    method = clean_text(request_method).upper() or "GET"
    content_type = _content_type_base(headers)
    prefix_kind = _prefix_classification(body_prefix)

    if any(content_type.startswith(value) for value in _NON_MEDIA_CONTENT_TYPES):
        return VodUpstreamValidation(False, "non_media_content_type", content_type)
    if prefix_kind in {"html", "json", "xml", "text"}:
        return VodUpstreamValidation(False, f"{prefix_kind}_body", content_type)
    if status >= 400 or status <= 0:
        return VodUpstreamValidation(False, "upstream_status", content_type)

    content_range = clean_text(get_header_value(headers, "Content-Range"))
    if status == 206:
        match = _CONTENT_RANGE_RE.fullmatch(content_range)
        if match is None:
            return VodUpstreamValidation(False, "invalid_content_range", content_type)
        range_start = int(match.group(1))
        range_end = int(match.group(2))
        total_size = None if match.group(3) == "*" else int(match.group(3))
        if range_start != offset or range_end < range_start or (total_size is not None and range_end >= total_size):
            return VodUpstreamValidation(False, "mismatched_content_range", content_type)
        return VodUpstreamValidation(
            True,
            "partial_media",
            content_type,
            range_start=range_start,
            range_end=range_end,
            total_size=total_size,
        )

    if status != 200:
        return VodUpstreamValidation(False, "unsupported_status", content_type)
    if content_range:
        return VodUpstreamValidation(False, "unexpected_content_range", content_type)
    if offset > 0 and not allow_restart_from_zero:
        return VodUpstreamValidation(False, "range_ignored", content_type)

    declared_media = any(content_type.startswith(value) for value in _MEDIA_CONTENT_TYPES)
    if method == "HEAD":
        plausible_media = declared_media or not content_type
    elif content_type in {"", "application/octet-stream"}:
        plausible_media = prefix_kind == "media"
    else:
        plausible_media = declared_media
    if not plausible_media:
        return VodUpstreamValidation(False, "unrecognised_media_response", content_type)
    return VodUpstreamValidation(
        True,
        "full_media",
        content_type,
        restart_from_zero=offset > 0,
    )
