from dataclasses import dataclass, field
from pathlib import Path
import time
from typing import Any, TypedDict

import asyncio


class VodHeadProbeStateEntry(TypedDict):
    expires_at: int
    failure_reason: str
    head_supported: bool
    last_failure_at: int


class HwaccelFailureStateEntry(TypedDict):
    failure_reason: str
    updated_at: str


@dataclass
class CsoSource:
    """
    Unified adapter for all CSO ingest sources (Live TV, VOD, etc).
    Decouples the core streaming engine from database models.
    """

    id: int
    source_type: str  # "channel", "vod_movie", or "vod_episode"
    url: str
    playlist_id: int
    playlist: object | None = None
    xc_account_id: int | None = None
    xc_account: object | None = None
    priority: int = 0
    channel_id: int | None = None
    internal_id: int | None = None
    cache_internal_id: int | None = None
    use_hls_proxy: bool = False
    probe_details: dict | None = None
    probe_at: object | None = None
    container_extension: str | None = None

    @property
    def playlist_stream_url(self):
        return self.url


@dataclass
class CsoStartResult:
    success: bool
    reason: str | None = None


@dataclass
class CsoStartupEvidence:
    first_ingest_chunk: float | None = None
    input_bytes: int = 0
    input_mode: str = "unknown"
    output_process_pid: int | None = None
    attempt_elapsed_ms: int = 0
    ingest_process_pid: int | None = None
    ingest_running: bool | None = None
    ingest_last_chunk_age_seconds: float | None = None
    ingest_attempt_start: float | None = None
    ingest_reader_end_reason: str | None = None
    ingest_reader_end_return_code: int | None = None
    ingest_bytes_produced: int = 0
    ingest_bytes_added_to_history: int = 0
    ingest_bytes_dispatched: int = 0
    ingest_subscriber_bytes_dispatched: int = 0


@dataclass
class CsoFfmpegAttemptResult:
    policy: dict[str, Any]
    success: bool
    runtime: Any = field(repr=False)
    classification: str
    failure_reason: str
    stderr_summary: str
    hardware_failure_stage: str
    evidence: CsoStartupEvidence = field(default_factory=CsoStartupEvidence)


@dataclass
class CsoFfmpegStartResult:
    success: bool
    policy: dict[str, Any]
    runtime: Any
    failure_reason: str
    attempts: tuple[CsoFfmpegAttemptResult, ...]
    fallback_policy: str

    def hardware_failures(self) -> tuple[CsoFfmpegAttemptResult, ...]:
        return tuple(attempt for attempt in self.attempts if attempt.hardware_failure_stage)


@dataclass
class CsoStreamPlan:
    generator: object | None
    content_type: str | None
    error_message: str | None
    status_code: int
    headers: dict | None = None
    cutoff_seconds: int | None = None
    final_status_code: int | None = None


@dataclass
class VodCacheEntry:
    key: str
    source: CsoSource
    upstream_url: str
    final_path: Path
    part_path: Path
    expected_size: int | None = None
    bytes_written: int = 0
    complete: bool = False
    failed_reason: str | None = None
    metadata_headers: dict | None = None
    content_type: str | None = None
    last_access_ts: float = 0.0
    active_sessions: int = 0
    active_readers: int = 0
    downloader_owner_key: str | None = None
    download_task: asyncio.Task | None = None
    probe_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    state_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    ready_event: asyncio.Event = field(default_factory=asyncio.Event)
    progress_event: asyncio.Event = field(default_factory=asyncio.Event)

    def __post_init__(self):
        if not self.last_access_ts:
            self.last_access_ts = time.time()

    def touch(self):
        self.last_access_ts = time.time()

    @property
    def downloader_running(self):
        return self.download_task is not None and not self.download_task.done()
