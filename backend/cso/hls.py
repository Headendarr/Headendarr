import re
from urllib.parse import urljoin, urlparse

import aiohttp

HLS_BANDWIDTH_RE = re.compile(r"BANDWIDTH=(\d+)")
HLS_RESOLUTION_RE = re.compile(r"RESOLUTION=(\d+)x(\d+)")


async def discover_hls_variants(url: str) -> list[dict[str, int | str]]:
    """Return HLS variant metadata for a playlist URL.

    For master playlists, this returns one entry per `#EXT-X-STREAM-INF` variant.
    For media playlists, this returns a single entry describing the concrete
    playlist URL so callers can treat it as an already-selected variant.
    """
    parsed = urlparse(url or "")
    if not parsed.path.lower().endswith(".m3u8"):
        return []
    timeout = aiohttp.ClientTimeout(total=6)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as client:
            async with client.get(url, allow_redirects=True) as response:
                if response.status >= 400:
                    return []
                payload = await response.text()
                resolved_url = str(response.url or url)
    except Exception:
        return []
    return parse_hls_playlist_variants(resolved_url, payload)


def parse_hls_playlist_variants(base_url: str, payload: str) -> list[dict[str, int | str]]:
    """Parse HLS playlist text into sorted variant descriptors.

    Master playlists are sorted by ascending `BANDWIDTH`, so callers that select
    the last entry get the highest-bandwidth rendition. `RESOLUTION` is parsed
    and recorded for diagnostics and future selection policies, but the current
    ordering logic uses `BANDWIDTH`.
    """
    lines = [line.strip() for line in (payload or "").splitlines() if line.strip()]
    variants: list[dict[str, int | str]] = []
    pending_bandwidth = None
    pending_width = 0
    pending_height = 0
    is_media_playlist = False
    for line in lines:
        if line.startswith("#EXT-X-STREAM-INF:"):
            bandwidth_match = HLS_BANDWIDTH_RE.search(line)
            pending_bandwidth = int(bandwidth_match.group(1)) if bandwidth_match else 0
            resolution_match = HLS_RESOLUTION_RE.search(line)
            pending_width = int(resolution_match.group(1)) if resolution_match else 0
            pending_height = int(resolution_match.group(2)) if resolution_match else 0
            continue
        if line.startswith(("#EXTINF", "#EXT-X-TARGETDURATION", "#EXT-X-MEDIA-SEQUENCE")):
            is_media_playlist = True
        if line.startswith("#"):
            continue
        if pending_bandwidth is None:
            # Not a master playlist variant entry.
            continue
        variant_url = urljoin(base_url, line)
        variants.append(
            {
                "bandwidth": pending_bandwidth,
                "width": pending_width,
                "height": pending_height,
                "program_index": len(variants),
                "variant_url": variant_url,
                "ffmpeg_program_index": 0,
                "playlist_type": "master",
            }
        )
        pending_bandwidth = None
        pending_width = 0
        pending_height = 0
    if variants:
        variants.sort(key=lambda item: int(item.get("bandwidth") or 0))
        return variants
    if is_media_playlist:
        return [
            {
                "bandwidth": 0,
                "width": 0,
                "height": 0,
                "program_index": 0,
                "variant_url": base_url,
                "ffmpeg_program_index": 0,
                "playlist_type": "media",
            }
        ]
    return []
