from pathlib import Path


# Whether VOD playback should prefer a managed proxy session when direct upstream input is not in use.
CS_VOD_USE_PROXY_SESSION = True


# How long a failed live source should be held down before it becomes eligible again.
CSO_SOURCE_HOLD_DOWN_SECONDS = 20

# How long a temporary source-failure marker should stay cached.
CSO_SOURCE_FAILURE_CACHE_TTL_SECONDS = 5 * 60

# Extra ordering penalty applied to sources that have just failed during selection.
CSO_SOURCE_FAILURE_PRIORITY_PENALTY = 1000

# Smaller ordering penalty applied to sources currently marked unhealthy.
CSO_UNHEALTHY_SOURCE_PRIORITY_PENALTY = 5


# Time window used when deciding whether repeated ingest recovery attempts are still part of one outage.
CSO_INGEST_RECOVERY_RETRY_WINDOW_SECONDS = 12

# Delay between output-side ingest recovery retry attempts.
CSO_INGEST_RECOVERY_RETRY_INTERVAL_SECONDS = 1

# Default no-data timeout before ingest is treated as stalled.
CSO_STALL_SECONDS_DEFAULT = 20

# Minimum realtime speed ratio before ingest is considered under-speed.
CSO_UNDERSPEED_RATIO_DEFAULT = 0.9

# Time a source must stay under the speed threshold before failover is triggered.
CSO_UNDERSPEED_WINDOW_SECONDS_DEFAULT = 12

# Grace period after ingest start before strict health checks apply.
CSO_STARTUP_GRACE_SECONDS_DEFAULT = 8

# Rolling window for HTTP error counting on ingest stderr.
CSO_HTTP_ERROR_WINDOW_SECONDS_DEFAULT = 10

# Number of HTTP errors within the window that triggers unhealthy handling.
CSO_HTTP_ERROR_THRESHOLD_DEFAULT = 4

# Maximum age of the last reported ffmpeg speed sample before it is treated as stale.
CSO_SPEED_STALE_SECONDS_DEFAULT = 6


# Max reconnect backoff between live ingest retry attempts.
CSO_INGEST_RECONNECT_DELAY_MAX_SECONDS = 2

# Read timeout used for URL-based ingest operations.
CSO_INGEST_RW_TIMEOUT_US = 15_000_000

# Connect/request timeout used for URL-based ingest operations.
CSO_INGEST_TIMEOUT_US = 10_000_000

# Probe size for ingest ffmpeg input inspection.
CSO_INGEST_PROBE_SIZE_BYTES = 1 * 1024 * 1024

# Analyse duration for ingest ffmpeg input inspection.
CSO_INGEST_ANALYSE_DURATION_US = 1_000_000

# Frame-rate probe sample size for ingest ffmpeg input inspection.
CSO_INGEST_FPS_PROBE_SIZE = 64

# Maximum retained shared ingest history for new subscribers.
CSO_INGEST_HISTORY_MAX_BYTES = 16 * 1024 * 1024

# Per-subscriber queue ceiling for ingest fan-out.
CSO_INGEST_SUBSCRIBER_QUEUE_MAX_BYTES = 90_000_000

# Initial history burst offered to new output subscribers before live chunks.
CSO_INGEST_SUBSCRIBER_PREBUFFER_BYTES = 512 * 1024


# Probe size for output-side ffmpeg startup and format detection.
CSO_OUTPUT_PROBE_SIZE_BYTES = 1 * 1024 * 1024

# Analyse duration for output-side ffmpeg startup and format detection.
CSO_OUTPUT_ANALYSE_DURATION_US = 2_000_000

# Frame-rate probe sample size for output-side ffmpeg startup and format detection.
CSO_OUTPUT_FPS_PROBE_SIZE = 32

# Per-client output queue ceiling before old data is dropped.
CSO_OUTPUT_CLIENT_QUEUE_MAX_BYTES = 90_000_000

# Idle/stale timeout for normal output clients before cleanup.
CSO_OUTPUT_CLIENT_STALE_SECONDS = 15.0

# Slightly longer idle timeout for TVHeadend-managed clients.
CSO_OUTPUT_CLIENT_STALE_SECONDS_TVH = 20.0

# Poll interval when an output is waiting on slate-origin input changes.
CSO_OUTPUT_SLATE_POLL_INTERVAL_SECONDS = 0.25

# Interval for verbose consumer progress logging during long-running streams.
CSO_CONSUMER_PROGRESS_LOG_INTERVAL_SECONDS = 10


# HLS segment duration used for generated CSO playlists.
CSO_HLS_SEGMENT_SECONDS = 3

# HLS live playlist depth exposed to clients.
CSO_HLS_LIST_SIZE = 5

# Idle timeout for HLS clients before an output can be cleaned up.
CSO_HLS_CLIENT_IDLE_SECONDS = max(10, int(CSO_HLS_SEGMENT_SECONDS) * 3)


# Lead time before starting the next VOD channel segment ingest.
VOD_CHANNEL_NEXT_SEGMENT_PRESTART_SECONDS = 20

# Buffer target for preparing the next VOD channel segment.
VOD_CHANNEL_NEXT_SEGMENT_BUFFER_BYTES = 256 * 1024 * 1024

# Root directory used for VOD cache and timeshift files.
VOD_CACHE_ROOT = Path("/timeshift/vod")

# How long unused VOD cache entries should be kept before cleanup.
VOD_CACHE_TTL_SECONDS = 10 * 60

# Chunk size used while downloading or serving cached VOD files.
VOD_CACHE_CHUNK_BYTES = 64 * 1024

# Timeout for VOD cache metadata HEAD/Range inspection.
VOD_CACHE_METADATA_TIMEOUT_SECONDS = 10

# How long to remember that an upstream does not support useful HEAD probing.
VOD_HEAD_PROBE_STATE_TTL_SECONDS = 7 * 24 * 60 * 60


# Default slate display durations by failure reason.
CSO_UNAVAILABLE_REASON_DURATIONS_SECONDS = {
    "default": 10,
    "capacity_blocked": 10,
    "playback_unavailable": 3,
    "startup_pending": 30,
}

# User-facing title/subtitle copy for common unavailable-slate reasons.
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

# Master feature flag for whether CSO is allowed to return the generated unavailable slate.
CSO_UNAVAILABLE_SHOW_SLATE = True


# MPEG-TS packet size used when choosing a sensible pipe read chunk size.
MPEGTS_PACKET_SIZE_BYTES = 188

# Shared MPEG-TS read chunk size used across ingest, slate, and ffmpeg startup reads.
MPEGTS_CHUNK_BYTES = MPEGTS_PACKET_SIZE_BYTES * 87


# Shared mapping from CSO container/profile names to ffmpeg muxer names.
CONTAINER_TO_FFMPEG_FORMAT = {
    "mpegts": "mpegts",
    "ts": "mpegts",
    "avi": "avi",
    "flv": "flv",
    "matroska": "matroska",
    "mkv": "matroska",
    "mp4": "mp4",
    "webm": "webm",
    "hls": "hls",
}
