import asyncio
import hashlib
import json
import logging
import os
import re
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from urllib.parse import urlparse

from backend.config import enable_cso_ingest_command_debug_logging, enable_cso_output_command_debug_logging
from backend.hls_multiplexer import get_header_value
from backend.http_headers import sanitise_headers
from backend.utils import clean_key, clean_text, convert_to_int, utc_now_naive

from .common import resolve_cso_unavailable_logo_path, wrap_slate_words
from .constants import (
    CONTAINER_TO_FFMPEG_FORMAT,
    CSO_HLS_LIST_SIZE,
    CSO_HLS_SEGMENT_SECONDS,
    CSO_INGEST_ANALYSE_DURATION_US,
    CSO_INGEST_FPS_PROBE_SIZE,
    CSO_INGEST_PROBE_SIZE_BYTES,
    CSO_INGEST_RECONNECT_DELAY_MAX_SECONDS,
    CSO_INGEST_RW_TIMEOUT_US,
    CSO_INGEST_TIMEOUT_US,
    CSO_OUTPUT_ANALYSE_DURATION_US,
    CSO_OUTPUT_FPS_PROBE_SIZE,
    CSO_OUTPUT_PROBE_SIZE_BYTES,
    MPEGTS_CHUNK_BYTES,
)
from .policy import policy_ffmpeg_format
from .processes import CsoTaskCleanupResult, cancel_and_await_tasks, terminate_cso_ffmpeg_process
from .types import (
    CsoFfmpegAttemptResult,
    CsoFfmpegStartResult,
    HwaccelFailureStateEntry,
)


logger = logging.getLogger("cso")


def format_ffmpeg_request_headers(headers: dict[str, str] | None) -> str | None:
    lines = []
    for key, value in (headers or {}).items():
        key_name = clean_key(key)
        if key_name in {"user-agent", "referer"}:
            continue
        text = clean_text(value)
        if not text:
            continue
        lines.append(f"{key}: {text}")
    if not lines:
        return None
    # FFmpeg expects CRLF-separated request headers.
    return "\r\n".join(lines) + "\r\n"


def _escape_ffmpeg_drawtext_text(value):
    text = clean_text(value)
    text = text.replace("\\", "\\\\")
    text = text.replace(":", "\\:")
    text = text.replace("'", "\\'")
    text = text.replace(",", "\\,")
    text = text.replace("[", "\\[")
    text = text.replace("]", "\\]")
    return text


def detect_vaapi_device_path() -> str | None:
    for candidate in ("/dev/dri/renderD128", "/dev/dri/renderD129"):
        if Path(candidate).exists():
            return candidate
    for candidate in sorted(Path("/dev/dri").glob("renderD*")) if Path("/dev/dri").exists() else []:
        if candidate.exists():
            return str(candidate)
    return None


def detect_vaapi_device_fingerprint(device_path: str | None = None) -> str:
    resolved_device_path = clean_text(device_path) or clean_text(detect_vaapi_device_path())
    if not resolved_device_path:
        return ""
    device = Path(resolved_device_path)
    if not device.exists():
        return ""
    try:
        resolved = device.resolve(strict=True)
    except Exception:
        resolved = device
    render_name = resolved.name
    sys_device_path = Path("/sys/class/drm") / render_name / "device"
    fingerprint_parts = {
        "device_path": str(resolved),
        "render_name": render_name,
    }
    if sys_device_path.exists():
        for field_name in (
            "vendor",
            "device",
            "subsystem_vendor",
            "subsystem_device",
            "revision",
            "modalias",
        ):
            field_path = sys_device_path / field_name
            if field_path.exists():
                try:
                    fingerprint_parts[field_name] = field_path.read_text(encoding="utf-8").strip()
                except Exception:
                    continue
        driver_path = sys_device_path / "driver"
        if driver_path.exists():
            try:
                fingerprint_parts["driver"] = driver_path.resolve(strict=True).name
            except Exception:
                fingerprint_parts["driver"] = driver_path.name
        uevent_path = sys_device_path / "uevent"
        if uevent_path.exists():
            try:
                fingerprint_parts["uevent"] = uevent_path.read_text(encoding="utf-8").strip()
            except Exception:
                pass
    payload = json.dumps(fingerprint_parts, sort_keys=True)
    return hashlib.md5(payload.encode("utf-8"), usedforsecurity=False).hexdigest()


def _policy_uses_hw_video_pipeline(policy: dict[str, Any] | None) -> bool:
    data = dict(policy or {})
    return bool(data.get("hwaccel")) and bool(clean_key(data.get("video_codec")))


def _build_hwaccel_failure_source_identity(source_identity: str | None) -> str:
    text = clean_text(source_identity)
    if not text:
        return ""
    return hashlib.md5(text.encode("utf-8"), usedforsecurity=False).hexdigest()


def _encoder_policy_fingerprint(policy: dict[str, Any] | None) -> str:
    data = dict(policy or {})
    profile_fields = (
        "video_codec",
        "target_width",
        "target_height",
        "output_fps",
        "output_pixel_format",
        "deinterlace",
        "target_video_bitrate",
        "target_video_maxrate",
        "target_video_bufsize",
        "container",
    )
    payload = {field: data.get(field) for field in profile_fields if field in data}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.md5(encoded.encode("utf-8"), usedforsecurity=False).hexdigest()


def _build_hwaccel_failure_cache_key(
    policy: dict[str, Any] | None,
    source_identity: str | None,
    failure_stage: str,
) -> str:
    if not _policy_uses_hw_video_pipeline(policy):
        return ""
    stage = clean_key(failure_stage)
    if stage not in {"decoder", "encoder"}:
        return ""
    gpu_fingerprint = detect_vaapi_device_fingerprint()
    if not gpu_fingerprint:
        return ""
    hashed_source_identity = _build_hwaccel_failure_source_identity(source_identity)
    if not hashed_source_identity:
        return ""
    key = f"hwaccel-failure:{stage}:{gpu_fingerprint}:source={hashed_source_identity}"
    if stage == "encoder":
        key = f"{key}:policy={_encoder_policy_fingerprint(policy)}"
    return key


def hwaccel_failure_stage(failure_reason: str) -> str:
    text = clean_text(failure_reason).lower()
    if not text:
        return ""
    failure_tokens = (
        "error",
        "failed",
        "failure",
        "cannot",
        "can't",
        "could not",
        "unable",
        "unsupported",
        "not supported",
        "impossible",
        "invalid",
        "no device available",
        "no support",
    )
    if not any(token in text for token in failure_tokens):
        return ""
    hardware_tokens = (
        "vaapi",
        "hwaccel",
        "hwupload",
        "deinterlace_vaapi",
        "scale_vaapi",
        "failed setup for format vaapi",
        "invalid output format vaapi",
        "hardware decoder",
        "hardware encoder",
        "hardware device",
        "renderd",
        "/dev/dri",
        "h264_vaapi",
        "hevc_vaapi",
        "vp9_vaapi",
        "av1_vaapi",
    )
    if not any(token in text for token in hardware_tokens):
        return ""
    decoder_tokens = (
        "hardware decoder",
        "no device available for decoder",
        "device setup failed for decoder",
        "decoder device",
        "decoder initialization",
        "decoder initialisation",
        "failed setup for format vaapi",
        "failed to initialise vaapi connection",
        "failed to initialize vaapi connection",
        "hwaccel",
    )
    encoder_tokens = (
        "hardware encoder",
        "error while opening encoder",
        "failed to open encoder",
        "encoder setup",
        "encoder initialization",
        "encoder initialisation",
        "h264_vaapi",
        "hevc_vaapi",
        "vp9_vaapi",
        "av1_vaapi",
        "hwupload",
        "deinterlace_vaapi",
        "scale_vaapi",
        "invalid output format vaapi",
    )
    decoder_match = any(token in text for token in decoder_tokens)
    encoder_match = any(token in text for token in encoder_tokens)
    if decoder_match and not encoder_match:
        return "decoder"
    if encoder_match and not decoder_match:
        return "encoder"
    if decoder_match and encoder_match:
        if "decoder" in text and "encoder" not in text:
            return "decoder"
        if "encoder" in text and "decoder" not in text:
            return "encoder"
    return "unknown"


def ffmpeg_failure_classification(failure_reason: str) -> str:
    text = clean_text(failure_reason).lower()
    hardware_stage = hwaccel_failure_stage(text)
    hardware_classification = {
        "decoder": "decoder_initialization",
        "encoder": "encoder_initialization",
        "unknown": "hardware_initialization",
    }.get(hardware_stage, "")
    if hardware_classification:
        return hardware_classification
    if any(
        token in text
        for token in (
            "error initializing output stream",
            "error while opening encoder",
            "failed to open encoder",
            "encoder setup failed",
            "could not write header",
            "incorrect codec parameters",
        )
    ):
        return "encoder_initialization"
    if any(
        token in text
        for token in (
            "could not find codec parameters",
            "could not detect ts packet size",
            "error opening input",
            "invalid data found when processing input",
        )
    ):
        return "ffmpeg_input_probe"
    if "ffmpeg_exit:" in text:
        return "process_exit"
    return "first_output_timeout"


async def _prepare_hw_decode_policy(
    policy: dict[str, Any] | None,
    source_identity: str | None,
) -> tuple[dict[str, Any], dict[str, str]]:
    resolved_policy = dict(policy or {})
    cache_keys = {
        stage: _build_hwaccel_failure_cache_key(resolved_policy, source_identity, stage)
        for stage in ("decoder", "encoder")
    }
    if "hardware_decode" not in resolved_policy:
        resolved_policy["hardware_decode"] = True
    decoder_failure = (
        await hwaccel_failure_state_store.failure_reason(cache_keys["decoder"]) if cache_keys["decoder"] else ""
    )
    encoder_failure = (
        await hwaccel_failure_state_store.failure_reason(cache_keys["encoder"]) if cache_keys["encoder"] else ""
    )
    if decoder_failure:
        resolved_policy["hardware_decode"] = False
    if encoder_failure:
        resolved_policy["hwaccel"] = False
        resolved_policy["hardware_decode"] = False
    return resolved_policy, cache_keys


def event_source_probe(source: Any) -> dict[str, Any]:
    probe_details = getattr(source, "probe_details", None)
    return dict(probe_details or {})


async def start_ffmpeg_with_hw_decode_fallback(
    base_policy: dict[str, Any] | None,
    source_identity: str | None,
    attempt_start: Callable[[dict[str, Any]], Awaitable[CsoFfmpegAttemptResult]],
) -> CsoFfmpegStartResult:
    start_policy, hwaccel_failure_keys = await _prepare_hw_decode_policy(base_policy, source_identity)
    attempted_hw_decode = bool(start_policy.get("hardware_decode", True))
    attempted_hw_encode = bool(start_policy.get("hwaccel", False))
    attempts: list[CsoFfmpegAttemptResult] = []
    fallback_policy = "none"
    while True:
        attempt = await attempt_start(start_policy)
        attempts.append(attempt)
        if attempt.success:
            return CsoFfmpegStartResult(
                True,
                start_policy,
                attempt.runtime,
                "",
                tuple(attempts),
                fallback_policy,
            )
        if any(
            marker in attempt.failure_reason.lower() for marker in ("teardown_unconfirmed", "task_cleanup_unconfirmed")
        ):
            return CsoFfmpegStartResult(
                False,
                start_policy,
                None,
                attempt.failure_reason,
                tuple(attempts),
                fallback_policy,
            )
        if (
            attempt.hardware_failure_stage == "decoder"
            and attempted_hw_decode
            and bool(start_policy.get("hardware_decode", True))
        ):
            decoder_failure_key = hwaccel_failure_keys.get("decoder", "")
            if decoder_failure_key:
                await hwaccel_failure_state_store.mark_failed(decoder_failure_key, attempt.failure_reason)
            start_policy = dict(base_policy or {})
            start_policy["hardware_decode"] = False
            attempted_hw_decode = False
            fallback_policy = "disable_hardware_decode"
            continue
        if (
            attempt.hardware_failure_stage == "encoder"
            and attempted_hw_encode
            and bool(start_policy.get("hwaccel", False))
        ):
            encoder_failure_key = hwaccel_failure_keys.get("encoder", "")
            if encoder_failure_key:
                await hwaccel_failure_state_store.mark_failed(encoder_failure_key, attempt.failure_reason)
            start_policy = dict(base_policy or {})
            start_policy["hwaccel"] = False
            start_policy["hardware_decode"] = False
            attempted_hw_encode = False
            attempted_hw_decode = False
            fallback_policy = "software_encode"
            continue
        if (
            attempt.hardware_failure_stage == "unknown"
            and attempted_hw_encode
            and bool(start_policy.get("hwaccel", False))
        ):
            start_policy = dict(base_policy or {})
            start_policy["hwaccel"] = False
            start_policy["hardware_decode"] = False
            attempted_hw_encode = False
            attempted_hw_decode = False
            fallback_policy = "software_encode_unknown_hardware_stage"
            continue
        return CsoFfmpegStartResult(
            False,
            start_policy,
            None,
            attempt.failure_reason or "output_start_failed",
            tuple(attempts),
            fallback_policy,
        )


async def terminate_ffmpeg_process(
    process: Any,
    *,
    terminate_timeout_seconds: float = 2.0,
    kill_timeout_seconds: float = 2.0,
):
    return await terminate_cso_ffmpeg_process(
        process,
        terminate_timeout_seconds=terminate_timeout_seconds,
        kill_timeout_seconds=kill_timeout_seconds,
    )


async def wait_for_process_output_start(
    process: Any,
    stream: Any,
    timeout_seconds: float = 8.0,
    cleanup_results: list[CsoTaskCleanupResult] | None = None,
) -> tuple[bool, str, bytes, CsoTaskCleanupResult]:
    first_chunk = b""
    read_task = asyncio.create_task(stream.read(MPEGTS_CHUNK_BYTES))
    wait_task = asyncio.create_task(process.wait())
    started = False
    failure_reason = "startup_timeout_no_output"
    try:
        done, _ = await asyncio.wait(
            {read_task, wait_task},
            timeout=max(1.0, float(timeout_seconds)),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if read_task in done and not read_task.cancelled():
            try:
                first_chunk = read_task.result() or b""
            except Exception:
                first_chunk = b""
            if first_chunk:
                started = True
                failure_reason = ""
        if not started and wait_task in done and not wait_task.cancelled():
            try:
                return_code = wait_task.result()
            except Exception:
                return_code = process.returncode
            failure_reason = f"ffmpeg_exit:{return_code}"
    finally:
        task_cleanup = await cancel_and_await_tasks((read_task, wait_task))
        if cleanup_results is not None:
            cleanup_results.append(task_cleanup)
    if not task_cleanup.confirmed:
        started = False
        failure_reason = f"{failure_reason or 'startup_probe_completed'}:task_cleanup_unconfirmed"
    elif not started and process.returncode is not None:
        failure_reason = f"ffmpeg_exit:{process.returncode}"
    return started, failure_reason, first_chunk if started else b"", task_cleanup


def log_hwaccel_failure(
    context: str,
    attempt: CsoFfmpegAttemptResult,
    start_result: CsoFfmpegStartResult,
) -> bool:
    stage = attempt.hardware_failure_stage
    if stage not in {"decoder", "encoder", "unknown"} or not bool(attempt.policy.get("hwaccel", False)):
        return False
    if stage == "decoder" and not bool(attempt.policy.get("hardware_decode", True)):
        return False
    title = {
        "decoder": "CSO hardware-accelerated decode failed",
        "encoder": "CSO hardware-accelerated encode failed",
        "unknown": "CSO hardware acceleration failed",
    }[stage]
    advice = {
        "decoder": "The VAAPI decode path failed; the retry policy disables hardware decoding for this source.",
        "encoder": "The VAAPI encode path failed; disable hardware acceleration for this codec/profile if it persists.",
        "unknown": "VAAPI failure evidence was present, but the failing hardware stage could not be established.",
    }[stage]
    logger.error(
        f"{title} context=%s video_codec=%s reason=%s diagnostics=%s. {advice}",
        redact_ffmpeg_error_for_log(context, max_length=256),
        clean_key(attempt.policy.get("video_codec")) or "",
        redact_ffmpeg_error_for_log(attempt.failure_reason or "unknown"),
        startup_failure_diagnostics(attempt, start_result),
    )
    return True


def startup_failure_diagnostics(
    attempt: CsoFfmpegAttemptResult,
    start_result: CsoFfmpegStartResult,
) -> dict[str, Any]:
    evidence = attempt.evidence
    return {
        "classification": redact_ffmpeg_error_for_log(attempt.classification, max_length=128),
        "hardware_failure_stage": (
            redact_ffmpeg_error_for_log(attempt.hardware_failure_stage, max_length=128)
            if attempt.hardware_failure_stage
            else None
        ),
        "first_ingest_chunk": evidence.first_ingest_chunk,
        "input_bytes": evidence.input_bytes,
        "input_mode": redact_ffmpeg_error_for_log(evidence.input_mode, max_length=128),
        "output_process_pid": evidence.output_process_pid,
        "ingest_process_pid": evidence.ingest_process_pid,
        "ingest_running": evidence.ingest_running,
        "ingest_last_chunk_age_seconds": evidence.ingest_last_chunk_age_seconds,
        "ingest_attempt_start": evidence.ingest_attempt_start,
        "ingest_reader_end_reason": redact_ffmpeg_error_for_log(
            evidence.ingest_reader_end_reason,
            max_length=256,
        ),
        "ingest_reader_end_return_code": evidence.ingest_reader_end_return_code,
        "ingest_bytes_produced": evidence.ingest_bytes_produced,
        "ingest_bytes_added_to_history": evidence.ingest_bytes_added_to_history,
        "ingest_bytes_dispatched": evidence.ingest_bytes_dispatched,
        "ingest_subscriber_bytes_dispatched": evidence.ingest_subscriber_bytes_dispatched,
        "fallback_policy": redact_ffmpeg_error_for_log(start_result.fallback_policy, max_length=128),
        "attempts": [
            {
                "classification": redact_ffmpeg_error_for_log(item.classification, max_length=128),
                "failure_reason": redact_ffmpeg_error_for_log(item.failure_reason),
                "stderr_summary": redact_ffmpeg_error_for_log(item.stderr_summary),
                "hardware_failure_stage": redact_ffmpeg_error_for_log(
                    item.hardware_failure_stage,
                    max_length=128,
                ),
            }
            for item in start_result.attempts
        ],
    }


def log_ffmpeg_start_failure(
    context: str,
    attempt: CsoFfmpegAttemptResult,
    start_result: CsoFfmpegStartResult,
):
    logger.error(
        "CSO FFmpeg startup failed context=%s reason=%s diagnostics=%s",
        redact_ffmpeg_error_for_log(context, max_length=256),
        redact_ffmpeg_error_for_log(attempt.failure_reason or start_result.failure_reason or "output_start_failed"),
        startup_failure_diagnostics(attempt, start_result),
    )


def log_ffmpeg_start_result_failures(
    context: str,
    start_result: CsoFfmpegStartResult,
):
    final_attempt = start_result.attempts[-1]
    final_hardware_reported = False
    for hardware_attempt in start_result.hardware_failures():
        hardware_reported = log_hwaccel_failure(context, hardware_attempt, start_result)
        if hardware_attempt is final_attempt and hardware_reported:
            final_hardware_reported = True
    if not final_hardware_reported:
        log_ffmpeg_start_failure(context, final_attempt, start_result)


class HwaccelFailureStateStore:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._state: dict[str, HwaccelFailureStateEntry] | None = None
        home_dir = os.environ.get("HOME_DIR") or os.path.expanduser("~")
        self._path = Path(home_dir) / ".tvh_iptv_config" / "cache" / "hwaccel_failure_state.json"

    async def _load_state(self) -> dict[str, HwaccelFailureStateEntry]:
        if self._state is not None:
            return self._state
        payload: Any = {}
        if self._path.exists():
            try:
                payload = json.loads(await asyncio.to_thread(self._path.read_text, encoding="utf-8")) or {}
            except Exception:
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        cleaned: dict[str, HwaccelFailureStateEntry] = {}
        for key, value in payload.items():
            if not isinstance(key, str) or not isinstance(value, dict):
                continue
            cleaned[key] = {
                "failure_reason": clean_text(value.get("failure_reason")) or "hwaccel_failed",
                "updated_at": clean_text(value.get("updated_at")) or "",
            }
        self._state = cleaned
        return self._state

    async def _write_state(self, state: dict[str, HwaccelFailureStateEntry]):
        await asyncio.to_thread(self._path.parent.mkdir, 0o755, True, True)
        payload = json.dumps(state, indent=2, sort_keys=True)
        await asyncio.to_thread(self._path.write_text, payload, encoding="utf-8")

    async def failure_reason(self, cache_key: str) -> str:
        async with self._lock:
            state = await self._load_state()
            entry = state.get(clean_text(cache_key))
            return entry["failure_reason"] if entry is not None else ""

    async def mark_failed(self, cache_key: str, failure_reason: str):
        key = clean_text(cache_key)
        if not key:
            return
        async with self._lock:
            state = await self._load_state()
            state[key] = {
                "failure_reason": clean_text(failure_reason) or "hwaccel_failed",
                "updated_at": utc_now_naive().isoformat(),
            }
            await self._write_state(state)


hwaccel_failure_state_store = HwaccelFailureStateStore()


def _bounded_command_log_token(value: object, max_length: int = 256) -> str:
    text = "".join(character if character.isprintable() else "?" for character in str(value))
    if len(text) <= max_length:
        return text
    return f"{text[: max_length - 3]}..."


def redact_ffmpeg_error_for_log(
    value: object,
    sensitive_values: tuple[object, ...] = (),
    max_length: int = 1024,
) -> str:
    text = "".join(character if character.isprintable() else "?" for character in str(value or ""))
    for sensitive_value in sorted(
        {str(item) for item in sensitive_values if str(item or "")},
        key=len,
        reverse=True,
    ):
        text = text.replace(sensitive_value, "<redacted>")
    text = re.sub(
        r"(?i)\b(authorization|proxy-authorization|cookie|set-cookie|x-api-key|x-auth-token|x-token)"
        r"\s*:\s*[^|]+",
        r"\1: <redacted>",
        text,
    )
    text = re.sub(
        r"(?i)\b(?:file|https?|rtsp|rtmp|udp|tcp)://[^\s<>'\"]+",
        "<redacted-url>",
        text,
    )
    length_limit = max(4, int(max_length or 0))
    if len(text) <= length_limit:
        return text
    return f"{text[: length_limit - 3]}..."


def redact_ffmpeg_command_for_log(command: list[str] | tuple[str, ...]) -> list[str]:
    redacted: list[str] = []
    redact_next = False
    redact_input_next = False
    sensitive_value_flags = {
        "-authorization",
        "-cookies",
        "-headers",
        "-http_proxy",
        "-referer",
        "-user_agent",
    }
    for raw_token in command or ():
        token = str(raw_token)
        if redact_next:
            redacted.append("<redacted>")
            redact_next = False
            continue
        if redact_input_next:
            redacted.append("<redacted-input>")
            redact_input_next = False
            continue
        if token in sensitive_value_flags:
            redacted.append(token)
            redact_next = True
            continue
        if token == "-i":
            redacted.append(token)
            redact_input_next = True
            continue
        parsed = urlparse(token)
        if (parsed.scheme and parsed.netloc) or "://" in token:
            redacted.append("<redacted-url>")
            continue
        redacted.append(_bounded_command_log_token(token))
    return redacted


class CsoFfmpegCommandBuilder:
    """Structured FFmpeg command builder for ingest, output, HLS output, and slate sessions."""

    def __init__(self, policy=None, pipe_input_format="mpegts", pipe_output_format="mpegts", source_probe=None):
        self.policy = dict(policy or {})
        self.pipe_input_format = CONTAINER_TO_FFMPEG_FORMAT.get(clean_key(pipe_input_format), "mpegts")
        self.pipe_output_format = CONTAINER_TO_FFMPEG_FORMAT.get(clean_key(pipe_output_format), "mpegts")
        self.source_probe = dict(source_probe or {})

    @staticmethod
    def video_encoder_for_codec(video_codec: str) -> str:
        codec = video_codec or ""
        return {
            "h264": "libx264",
            "h265": "libx265",
            "av1": "libsvtav1",
            "vp8": "libvpx",
        }.get(codec, "libx264")

    @staticmethod
    def vaapi_encoder_for_codec(video_codec: str) -> str:
        codec = video_codec or ""
        return {
            "h264": "h264_vaapi",
            "h265": "hevc_vaapi",
            "av1": "av1_vaapi",
        }.get(codec, "h264_vaapi")

    @staticmethod
    def vaapi_default_qp(video_codec: str, target_width: int = 0) -> int:
        codec = clean_key(video_codec)
        if codec == "h265":
            return 25 if int(target_width or 0) > 0 else 23
        if codec == "av1":
            return 28 if int(target_width or 0) > 0 else 26
        return 23 if int(target_width or 0) > 0 else 21

    @staticmethod
    def audio_encoder_for_codec(audio_codec: str) -> str:
        codec = audio_codec or ""
        return {
            "aac": "aac",
            "ac3": "ac3",
            "opus": "libopus",
            "vorbis": "libvorbis",
        }.get(codec, "aac")

    @staticmethod
    def _software_deinterlace_filter() -> str:
        # Only deinterlace frames marked interlaced so progressive sources pass through untouched.
        return "bwdif=mode=send_frame:parity=auto:deint=interlaced"

    @staticmethod
    def _software_scale_filter(target_width: int, target_height: int = 0) -> str:
        width_value = max(0, int(target_width or 0))
        height_value = max(0, int(target_height or 0))
        if width_value > 0 and height_value > 0:
            return (
                f"scale=w={width_value}:h={height_value}:force_original_aspect_ratio=decrease:force_divisible_by=2,"
                f"pad={width_value}:{height_value}:(ow-iw)/2:(oh-ih)/2"
            )
        return f"scale=w='min({width_value},iw)':h=-2:force_original_aspect_ratio=decrease:force_divisible_by=2"

    def _vaapi_scale_filter(self, target_width: int, target_height: int = 0) -> str:
        width_value = max(0, int(target_width or 0))
        height_value = max(0, int(target_height or 0))
        if width_value > 0 and height_value > 0:
            return f"scale_vaapi=w={width_value}:h={height_value}"
        return f"scale_vaapi=w={width_value}:h=-2"

    def _input_hwaccel_args(self, policy=None) -> list[str]:
        effective_policy = dict(policy or self.policy or {})
        if not _policy_uses_hw_video_pipeline(effective_policy):
            return []
        if not bool(effective_policy.get("hardware_decode", True)):
            return []
        video_codec = clean_key(self.source_probe.get("video_codec"))
        if video_codec == "mpeg4":
            # VAAPI hardware decode for mpeg4 is often unsupported or unreliable.
            return []
        vaapi_device = detect_vaapi_device_path()
        if not vaapi_device:
            return []
        return [
            "-hwaccel",
            "vaapi",
            "-hwaccel_output_format",
            "vaapi",
            "-hwaccel_device",
            vaapi_device,
        ]

    @staticmethod
    def _build_slate_media_hint(media_hint):
        hint = dict(media_hint or {})
        width = max(16, int(hint.get("width") or 0))
        height = max(16, int(hint.get("height") or 0))
        fps_value = float(hint.get("fps") or 0.0)
        fps = int(round(fps_value)) if fps_value > 0 else 0
        pixel_format = clean_key(hint.get("pixel_format")) or "yuv420p"
        if width <= 16 or height <= 16:
            width = 1280
            height = 720
        if fps <= 0:
            avg_frame_rate = clean_text(hint.get("avg_frame_rate"))
            if avg_frame_rate and "/" in avg_frame_rate:
                try:
                    numerator, denominator = avg_frame_rate.split("/", 1)
                    denominator_value = max(1, int(float(denominator)))
                    fps = int(round(float(numerator) / float(denominator_value)))
                except Exception:
                    fps = 0
        if fps <= 0:
            fps = 25
        if fps > 60:
            fps = 60
        return {
            "width": width,
            "height": height,
            "fps": fps,
            "pixel_format": pixel_format,
        }

    @staticmethod
    def _ffmpeg_logging_command(debug_enabled, quiet_level="warning"):
        command = ["ffmpeg", "-hide_banner", "-loglevel", "info" if debug_enabled else quiet_level]
        if debug_enabled:
            command += ["-stats"]
        else:
            command += ["-nostats"]
        return command

    @staticmethod
    def _probe_flags(probe_size_bytes, analyse_duration_us, fps_probe_size):
        return [
            "-probesize",
            str(max(32_768, int(probe_size_bytes))),
            "-analyzeduration",
            str(max(250_000, int(analyse_duration_us))),
            "-fpsprobesize",
            str(max(0, int(fps_probe_size))),
        ]

    @staticmethod
    def _input_resilience_flags():
        return [
            "-fflags",
            "+discardcorrupt+genpts",
            "-err_detect",
            "ignore_err",
        ]

    @staticmethod
    def _drop_data_streams():
        return ["-dn"]

    @staticmethod
    def _mpegts_output_flags(zero_latency=True):
        command = [
            "-mpegts_flags",
            "+resend_headers",
        ]
        if zero_latency:
            command += [
                "-muxdelay",
                "0",
                "-muxpreload",
                "0",
            ]
        return command

    @staticmethod
    def _matroska_output_flags():
        # Configure Matroska for progressive/live output so players see clusters quickly
        # on non-seekable HTTP streams instead of waiting on larger default buffering.
        return [
            "-flush_packets",
            "1",
            "-cluster_time_limit",
            "1000",
            "-cluster_size_limit",
            "1048576",
            "-live",
            "1",
        ]

    @staticmethod
    def _pipe_output_target(ffmpeg_format, target="pipe:1"):
        command = []
        if ffmpeg_format == "mp4":
            command += ["-movflags", "+frag_keyframe+empty_moov+default_base_moof+delay_moov"]
        command += ["-f", ffmpeg_format, target]
        return command

    @staticmethod
    def _lavfi_input(spec):
        return ["-f", "lavfi", "-i", spec]

    def _slate_av_encode_flags(self, fps_value, pix_fmt, audio_bitrate, still_image=False):
        command = [
            "-c:v",
            "libx264",
            "-preset",
            "veryfast" if still_image else "superfast",
            "-tune",
            "stillimage" if still_image else "zerolatency",
            "-pix_fmt",
            pix_fmt,
            "-bf",
            "0",
            "-g",
            str(fps_value if still_image else max(fps_value * 2, fps_value)),
            "-keyint_min",
            str(fps_value if still_image else max(fps_value * 2, fps_value)),
            "-sc_threshold",
            "0",
            "-x264-params",
            "repeat-headers=1:scenecut=0",
            "-c:a",
            "aac",
            "-b:a",
            audio_bitrate,
            "-ar",
            "48000",
            "-ac",
            "2",
            "-shortest",
        ]
        return command

    def _apply_stream_selection(self, command, policy=None):
        effective_policy = policy or self.policy
        subtitle_mode = effective_policy["subtitle_mode"]
        if subtitle_mode != "drop":
            command += ["-map", "0:s?"]
        return subtitle_mode

    def _apply_transcode_options(self, command, subtitle_mode, policy=None):
        effective_policy = policy or self.policy
        container = str(effective_policy["container"]).strip().lower()
        video_codec = effective_policy["video_codec"]
        audio_codec = effective_policy["audio_codec"]
        target_width = max(0, int(effective_policy["target_width"] or 0))
        target_height = max(0, int(effective_policy.get("target_height") or 0))
        output_fps = float(effective_policy.get("output_fps") or 0.0)
        output_pixel_format = clean_key(effective_policy.get("output_pixel_format")) or "yuv420p"
        target_video_bitrate = effective_policy["target_video_bitrate"]
        target_video_maxrate = effective_policy["target_video_maxrate"]
        target_video_bufsize = effective_policy["target_video_bufsize"]
        target_audio_bitrate = effective_policy["audio_bitrate"]
        target_audio_sample_rate = max(0, int(effective_policy.get("audio_sample_rate") or 0))
        target_audio_channels = max(0, int(effective_policy.get("audio_channels") or 0))
        hwaccel_requested = bool(effective_policy["hwaccel"]) and bool(video_codec)
        hardware_decode = bool(effective_policy.get("hardware_decode", True))
        deinterlace = bool(effective_policy["deinterlace"]) and bool(video_codec)
        vaapi_device = detect_vaapi_device_path() if hwaccel_requested else None
        use_hw_encode = bool(vaapi_device) and clean_key(video_codec) in {"h264", "h265", "av1"}
        if hwaccel_requested and not vaapi_device:
            logger.info(
                "CSO hwaccel requested but no VAAPI device is available; falling back to software encode video_codec=%s container=%s",
                video_codec,
                container or "",
            )
        elif hwaccel_requested and not use_hw_encode:
            logger.info(
                "CSO hwaccel requested for a codec without VAAPI encode support in TIC; falling back to software encode video_codec=%s container=%s",
                video_codec,
                container or "",
            )

        if video_codec:
            filters = []
            if use_hw_encode:
                encoder = self.vaapi_encoder_for_codec(video_codec)
                if hardware_decode:
                    if deinterlace:
                        filters.append("deinterlace_vaapi=rate=field:auto=1")
                    if target_width > 0:
                        scale_filter = self._vaapi_scale_filter(target_width, target_height)
                        if scale_filter:
                            filters.append(scale_filter)
                else:
                    if deinterlace:
                        filters.append(self._software_deinterlace_filter())
                    if target_width > 0:
                        filters.append(self._software_scale_filter(target_width, target_height))
                    if deinterlace or target_width > 0:
                        filters.append("setsar=1")
                    filters += ["format=nv12", "hwupload"]
                command += ["-vaapi_device", vaapi_device]
                if filters:
                    command += ["-vf", ",".join(filters)]
                command += [
                    "-c:v",
                    encoder,
                    "-rc_mode",
                    "CQP",
                    "-qp",
                    str(self.vaapi_default_qp(video_codec, target_width)),
                ]
                if clean_key(video_codec) == "h264":
                    command += ["-profile:v", "high", "-level", "4.1"]
            else:
                if deinterlace:
                    filters.append(self._software_deinterlace_filter())
                if target_width > 0:
                    filters.append(self._software_scale_filter(target_width, target_height))
                    filters.append("setsar=1")
                if filters:
                    command += ["-vf", ",".join(filters)]
                sw_video_encoder = self.video_encoder_for_codec(video_codec)
                command += ["-c:v", sw_video_encoder]
                if sw_video_encoder == "libx264":
                    command += [
                        "-preset",
                        "veryfast",
                        "-tune",
                        "zerolatency",
                        "-pix_fmt",
                        output_pixel_format,
                        "-profile:v",
                        "high",
                        "-g",
                        "48",
                        "-keyint_min",
                        "48",
                        "-sc_threshold",
                        "0",
                        "-x264-params",
                        "repeat-headers=1:aud=1",
                        "-crf",
                        "21",
                    ]
                elif sw_video_encoder == "libsvtav1":
                    command += [
                        "-preset",
                        "8",
                        "-pix_fmt",
                        output_pixel_format,
                        "-g",
                        "48",
                        "-keyint_min",
                        "48",
                        "-svtav1-params",
                        "scd=0:enable-overlays=0",
                        "-crf",
                        "34",
                    ]
            if not use_hw_encode and sw_video_encoder != "libsvtav1":
                if target_video_bitrate:
                    command += ["-b:v", target_video_bitrate]
                if target_video_maxrate:
                    command += ["-maxrate", target_video_maxrate]
                if target_video_bufsize:
                    command += ["-bufsize", target_video_bufsize]
            if output_fps > 0:
                command += ["-r", f"{output_fps:.06f}"]
        else:
            command += ["-c:v", "copy"]

        if audio_codec:
            sw_audio_encoder = self.audio_encoder_for_codec(audio_codec)
            command += ["-c:a", sw_audio_encoder]
            command += ["-af", "aresample=async=1:first_pts=0"]
            if audio_codec == "aac":
                command += [
                    "-b:a",
                    target_audio_bitrate or "128k",
                    "-ar",
                    str(target_audio_sample_rate or 48000),
                    "-ac",
                    str(target_audio_channels or 2),
                ]
            elif audio_codec == "opus":
                command += [
                    "-b:a",
                    target_audio_bitrate or "96k",
                    "-ar",
                    str(target_audio_sample_rate or 48000),
                    "-ac",
                    str(target_audio_channels or 2),
                ]
        else:
            command += ["-c:a", "copy"]
        command += ["-c:s", "copy" if subtitle_mode != "drop" else "none"]
        if subtitle_mode == "drop":
            command.append("-sn")

    def _build_pipe_input(
        self,
        probe_size_bytes,
        analyse_duration_us,
        fps_probe_size,
        low_latency,
        pipe_format=None,
        input_hwaccel_args=None,
        include_stream_maps=True,
    ):
        pipe_format = CONTAINER_TO_FFMPEG_FORMAT.get(clean_key(pipe_format), self.pipe_input_format)
        command = []
        if low_latency:
            command += [
                "-fflags",
                "+nobuffer",
                "-flags",
                "low_delay",
            ]
        command += self._probe_flags(probe_size_bytes, analyse_duration_us, fps_probe_size)
        command += list(input_hwaccel_args or [])
        command += ["-f", pipe_format, "-i", "pipe:0"]
        if include_stream_maps:
            command += [
                "-map",
                "0:v:0?",
                "-map",
                "0:a?",
                "-max_muxing_queue_size",
                "4096",
            ]
        command += self._input_resilience_flags()
        return command

    def build_ingest_command(self, source_url, program_index=0, user_agent=None, request_headers=None):
        map_program = max(0, int(program_index or 0))
        is_hls_input = (urlparse(source_url or "").path or "").lower().endswith(".m3u8")
        probe_size_bytes = int(CSO_INGEST_PROBE_SIZE_BYTES)
        analyse_duration_us = int(CSO_INGEST_ANALYSE_DURATION_US)
        fps_probe_size = int(CSO_INGEST_FPS_PROBE_SIZE)
        header_values = sanitise_headers(request_headers)
        command = self._ffmpeg_logging_command(enable_cso_ingest_command_debug_logging, quiet_level="info")
        command += [
            "-progress",
            "pipe:2",
            "-reconnect",
            "1",
            "-reconnect_on_network_error",
            "1",
            "-reconnect_delay_max",
            str(max(1, int(CSO_INGEST_RECONNECT_DELAY_MAX_SECONDS))),
        ]
        user_agent_value = clean_text(user_agent) or get_header_value(header_values, "User-Agent")
        if user_agent_value:
            command += ["-user_agent", user_agent_value]
        referer_value = get_header_value(header_values, "Referer")
        if referer_value:
            command += ["-referer", referer_value]
        extra_headers = format_ffmpeg_request_headers(header_values)
        if extra_headers:
            command += ["-headers", extra_headers]
        if not is_hls_input:
            command += [
                "-reconnect_at_eof",
                "1",
                "-reconnect_streamed",
                "1",
                "-reconnect_on_http_error",
                "4xx,5xx",
            ]
        else:
            command += ["-reconnect_streamed", "0"]
            # NOTE: HLS playlists seem to need a larger initial probe window before AAC parameters such as sample rate and channel count are available to the output muxer.
            probe_size_bytes = max(probe_size_bytes, 5 * 1024 * 1024)
            analyse_duration_us = max(analyse_duration_us, 5_000_000)
            fps_probe_size = max(fps_probe_size, 128)
        command += self._input_resilience_flags()
        command += self._probe_flags(
            probe_size_bytes,
            analyse_duration_us,
            fps_probe_size,
        )
        if is_hls_input:
            stream_maps = [
                "-map",
                "0:v:0?",
                "-map",
                "0:a:0?",
                "-map",
                "0:s?",
            ]
        else:
            stream_maps = [
                "-map",
                f"0:p:{map_program}:v:0?",
                "-map",
                f"0:p:{map_program}:a?",
                "-map",
                f"0:p:{map_program}:s?",
            ]
        command += [
            "-rw_timeout",
            str(max(1_000_000, int(CSO_INGEST_RW_TIMEOUT_US))),
            "-timeout",
            str(max(1_000_000, int(CSO_INGEST_TIMEOUT_US))),
            "-i",
            source_url,
        ]
        command += stream_maps
        command += ["-c", "copy"]
        command += self._drop_data_streams()
        if self.pipe_output_format == "mpegts":
            command += self._mpegts_output_flags(zero_latency=True)
        elif self.pipe_output_format == "matroska":
            command += self._matroska_output_flags()
        command += self._pipe_output_target(self.pipe_output_format)
        return command

    def build_output_command(
        self,
        start_seconds=0,
        max_duration_seconds=None,
        input_target: str = "",
        input_is_url: bool = False,
        realtime: bool = False,
        user_agent: str | None = None,
        request_headers: dict[str, str] | None = None,
    ):
        command = self._ffmpeg_logging_command(enable_cso_output_command_debug_logging)
        probe_size_bytes = int(CSO_OUTPUT_PROBE_SIZE_BYTES)
        analyse_duration_us = int(CSO_OUTPUT_ANALYSE_DURATION_US)
        fps_probe_size = int(CSO_OUTPUT_FPS_PROBE_SIZE)
        use_direct_input = bool(clean_text(input_target))
        if (
            not use_direct_input
            and self.pipe_input_format == "mpegts"
            and (clean_key(self.source_probe.get("video_codec")) or clean_key(self.source_probe.get("audio_codec")))
        ):
            # TS is append-friendly for VOD handoff, but the output-side remux still needs
            # enough probe budget to recover AAC parameters from the live pipe.
            probe_size_bytes = min(probe_size_bytes, 512 * 1024)
            analyse_duration_us = min(analyse_duration_us, 1_000_000)
            fps_probe_size = min(fps_probe_size, 16)
        if use_direct_input:
            header_values = sanitise_headers(request_headers)
            user_agent_value = clean_text(user_agent) or get_header_value(header_values, "User-Agent")
            if input_is_url:
                command += ["-progress", "pipe:2"]
                if user_agent_value:
                    command += ["-user_agent", user_agent_value]
                referer_value = get_header_value(header_values, "Referer")
                if referer_value:
                    command += ["-referer", referer_value]
                extra_headers = format_ffmpeg_request_headers(header_values)
                if extra_headers:
                    command += ["-headers", extra_headers]
                command += [
                    "-rw_timeout",
                    str(max(1_000_000, int(CSO_INGEST_RW_TIMEOUT_US))),
                    "-timeout",
                    str(max(1_000_000, int(CSO_INGEST_TIMEOUT_US))),
                ]
            if realtime:
                command += ["-readrate", "1"]
            command += self._input_hwaccel_args()
            command += self._probe_flags(probe_size_bytes, analyse_duration_us, fps_probe_size)
            command += self._input_resilience_flags()
            command += ["-i", str(input_target), "-map", "0:v:0?", "-map", "0:a?", "-max_muxing_queue_size", "4096"]
        else:
            command += self._build_pipe_input(
                probe_size_bytes,
                analyse_duration_us,
                fps_probe_size,
                low_latency=True,
                pipe_format=self.pipe_input_format,
                input_hwaccel_args=self._input_hwaccel_args(),
            )
        start_value = max(0, int(start_seconds or 0))
        duration_value = max(1, int(max_duration_seconds or 0)) if max_duration_seconds is not None else None
        if start_value > 0:
            command += ["-ss", str(start_value)]
        if duration_value is not None:
            command += ["-t", str(duration_value)]
        subtitle_mode = self._apply_stream_selection(command)
        mode = self.policy.get("output_mode") or "force_remux"
        ffmpeg_format = policy_ffmpeg_format(self.policy)

        if mode == "force_transcode":
            self._apply_transcode_options(command, subtitle_mode)
        else:
            command += ["-c", "copy"]
            if ffmpeg_format in ["mp4"]:
                # TODO: We shouldonly do this if we have an AAC audio source
                command += ["-bsf:a", "aac_adtstoasc"]
            if subtitle_mode == "drop":
                command.append("-sn")

        command += self._drop_data_streams()
        if ffmpeg_format == "mpegts":
            command += self._mpegts_output_flags(zero_latency=True)
        elif ffmpeg_format == "matroska":
            command += self._matroska_output_flags()
        command += self._pipe_output_target(ffmpeg_format)
        return command

    def build_local_output_command(
        self,
        input_path: Path,
        start_seconds=0,
        max_duration_seconds=None,
        realtime=False,
    ):
        command = self._ffmpeg_logging_command(enable_cso_output_command_debug_logging)
        start_value = max(0, int(start_seconds or 0))
        duration_value = max(1, int(max_duration_seconds or 0)) if max_duration_seconds is not None else None
        if realtime:
            command += ["-re"]
        if start_value > 0:
            command += ["-ss", str(start_value)]
        command += self._input_hwaccel_args()
        command += self._probe_flags(
            CSO_OUTPUT_PROBE_SIZE_BYTES,
            CSO_OUTPUT_ANALYSE_DURATION_US,
            CSO_OUTPUT_FPS_PROBE_SIZE,
        )
        command += self._input_resilience_flags()
        command += [
            "-i",
            str(input_path),
            "-map",
            "0:v:0?",
            "-map",
            "0:a?",
            "-max_muxing_queue_size",
            "4096",
        ]
        if duration_value is not None:
            command += ["-t", str(duration_value)]
        subtitle_mode = self._apply_stream_selection(command)
        mode = self.policy.get("output_mode") or "force_remux"
        ffmpeg_format = policy_ffmpeg_format(self.policy)

        if mode == "force_transcode":
            self._apply_transcode_options(command, subtitle_mode)
        else:
            command += ["-c", "copy"]
            if ffmpeg_format in ["mp4"]:
                command += ["-bsf:a", "aac_adtstoasc"]
            if subtitle_mode == "drop":
                command.append("-sn")

        command += self._drop_data_streams()
        if ffmpeg_format == "mpegts":
            command += self._mpegts_output_flags(zero_latency=True)
        elif ffmpeg_format == "matroska":
            command += self._matroska_output_flags()
        command += self._pipe_output_target(ffmpeg_format)
        return command

    def build_vod_segment_ingest_command(
        self,
        input_target: str,
        start_seconds=0,
        max_duration_seconds=None,
        realtime=False,
        input_is_url=False,
        user_agent=None,
        request_headers=None,
    ):
        effective_policy = dict(self.policy or {})
        command = self._ffmpeg_logging_command(enable_cso_ingest_command_debug_logging, quiet_level="info")
        start_value = max(0, int(start_seconds or 0))
        duration_value = max(1, int(max_duration_seconds or 0)) if max_duration_seconds is not None else None
        input_seek_value = start_value
        trim_seek_value = 0
        if input_is_url and start_value > 0:
            trim_seek_value = min(2, start_value)
            input_seek_value = max(0, start_value - trim_seek_value)
        if realtime:
            command += ["-re"]
        if input_is_url:
            header_values = sanitise_headers(request_headers)
            user_agent_value = clean_text(user_agent) or get_header_value(header_values, "User-Agent")
            command += [
                "-progress",
                "pipe:2",
                "-reconnect",
                "1",
                "-reconnect_on_network_error",
                "1",
                "-reconnect_delay_max",
                str(max(1, int(CSO_INGEST_RECONNECT_DELAY_MAX_SECONDS))),
            ]
            if user_agent_value:
                command += ["-user_agent", user_agent_value]
            referer_value = get_header_value(header_values, "Referer")
            if referer_value:
                command += ["-referer", referer_value]
            extra_headers = format_ffmpeg_request_headers(header_values)
            if extra_headers:
                command += ["-headers", extra_headers]
            command += [
                "-reconnect_at_eof",
                "1",
                "-reconnect_streamed",
                "1",
                "-reconnect_on_http_error",
                "4xx,5xx",
                "-rw_timeout",
                str(max(1_000_000, int(CSO_INGEST_RW_TIMEOUT_US))),
                "-timeout",
                str(max(1_000_000, int(CSO_INGEST_TIMEOUT_US))),
            ]
        if input_seek_value > 0:
            command += ["-ss", str(input_seek_value)]
        command += self._input_hwaccel_args(policy=effective_policy)
        command += self._probe_flags(
            CSO_INGEST_PROBE_SIZE_BYTES,
            CSO_INGEST_ANALYSE_DURATION_US,
            CSO_INGEST_FPS_PROBE_SIZE,
        )
        command += self._input_resilience_flags()
        command += [
            "-i",
            str(input_target),
            "-map",
            "0:v:0?",
            "-map",
            "0:a?",
        ]
        if trim_seek_value > 0:
            command += ["-ss", str(trim_seek_value)]
        command += ["-c", "copy"]
        if duration_value is not None:
            command += ["-t", str(duration_value)]
        command += self._drop_data_streams()
        if self.pipe_output_format == "mpegts":
            command += self._mpegts_output_flags(zero_latency=True)
        elif self.pipe_output_format == "matroska":
            command += self._matroska_output_flags()
        command += self._pipe_output_target(self.pipe_output_format)
        return command

    def build_vod_channel_ingest_command(
        self,
        input_target: str,
        start_seconds: int = 0,
        max_duration_seconds: int | None = None,
        realtime: bool = False,
        input_is_url: bool = False,
        user_agent: str | None = None,
        request_headers: dict[str, str] | None = None,
        policy: dict[str, Any] | None = None,
        seekable_url_input: bool = False,
    ) -> list[str]:
        effective_policy = dict(policy or self.policy or {})
        command = self._ffmpeg_logging_command(enable_cso_ingest_command_debug_logging, quiet_level="info")
        start_value = max(0, int(start_seconds or 0))
        duration_value = max(1, int(max_duration_seconds or 0)) if max_duration_seconds is not None else None
        probe_size_bytes = int(CSO_INGEST_PROBE_SIZE_BYTES)
        analyse_duration_us = int(CSO_INGEST_ANALYSE_DURATION_US)
        fps_probe_size = int(CSO_INGEST_FPS_PROBE_SIZE)
        if start_value > 0:
            probe_size_bytes = min(probe_size_bytes, 512 * 1024)
            analyse_duration_us = min(analyse_duration_us, 750_000)
            fps_probe_size = min(fps_probe_size, 16)
        input_seek_value = start_value
        trim_seek_value = 0
        if start_value > 0:
            trim_seek_value = min(2, start_value)
            input_seek_value = max(0, start_value - trim_seek_value)
        if realtime:
            if start_value > 0:
                command += [
                    "-readrate",
                    "1",
                ]
            else:
                command += ["-re"]
        if input_is_url:
            header_values = sanitise_headers(request_headers)
            user_agent_value = clean_text(user_agent) or get_header_value(header_values, "User-Agent")
            command += ["-progress", "pipe:2"]
            if user_agent_value:
                command += ["-user_agent", user_agent_value]
            referer_value = get_header_value(header_values, "Referer")
            if referer_value:
                command += ["-referer", referer_value]
            extra_headers = format_ffmpeg_request_headers(header_values)
            if extra_headers:
                command += ["-headers", extra_headers]
            if seekable_url_input:
                command += [
                    "-rw_timeout",
                    str(max(1_000_000, int(CSO_INGEST_RW_TIMEOUT_US))),
                    "-timeout",
                    str(max(1_000_000, int(CSO_INGEST_TIMEOUT_US))),
                ]
            else:
                command += [
                    "-reconnect",
                    "1",
                    "-reconnect_on_network_error",
                    "1",
                    "-reconnect_delay_max",
                    str(max(1, int(CSO_INGEST_RECONNECT_DELAY_MAX_SECONDS))),
                    "-reconnect_at_eof",
                    "1",
                    "-reconnect_streamed",
                    "1",
                    "-reconnect_on_http_error",
                    "4xx,5xx",
                    "-rw_timeout",
                    str(max(1_000_000, int(CSO_INGEST_RW_TIMEOUT_US))),
                    "-timeout",
                    str(max(1_000_000, int(CSO_INGEST_TIMEOUT_US))),
                ]
        if input_seek_value > 0:
            command += ["-ss", str(input_seek_value)]
        command += self._input_hwaccel_args(policy=effective_policy)
        command += self._probe_flags(
            probe_size_bytes,
            analyse_duration_us,
            fps_probe_size,
        )
        command += self._input_resilience_flags()
        command += [
            "-i",
            str(input_target),
            "-map",
            "0:v:0?",
            "-map",
            "0:a:0?",
            "-map_metadata",
            "-1",
            "-map_chapters",
            "-1",
            "-max_muxing_queue_size",
            "4096",
        ]
        if trim_seek_value > 0:
            command += ["-ss", str(trim_seek_value)]
        subtitle_mode = effective_policy.get("subtitle_mode") or "drop"
        self._apply_transcode_options(command, subtitle_mode, policy=effective_policy)
        if duration_value is not None:
            command += ["-t", str(duration_value)]
        command += self._drop_data_streams()
        if self.pipe_output_format == "mpegts":
            command += self._mpegts_output_flags(zero_latency=True)
        elif self.pipe_output_format == "matroska":
            command += self._matroska_output_flags()
        command += self._pipe_output_target(self.pipe_output_format)
        return command

    def build_hls_output_command(
        self,
        output_dir: Path,
        input_target: str = "",
        input_is_url: bool = False,
        input_uses_network: bool | None = None,
        start_seconds: int = 0,
        max_duration_seconds: int | None = None,
        realtime: bool = False,
        user_agent: str | None = None,
        request_headers: dict[str, str] | None = None,
        input_protocol_whitelist: str = "",
        require_video: bool = False,
        required_audio_stream_count: int = 0,
        pipe_probe_size_bytes: int = 2 * 1024 * 1024,
        pipe_analyse_duration_us: int = 5_000_000,
        pipe_fps_probe_size: int = CSO_OUTPUT_FPS_PROBE_SIZE,
    ) -> list[str]:
        command = self._ffmpeg_logging_command(enable_cso_output_command_debug_logging)
        input_target_value = str(input_target or "").strip()
        input_is_hls = (urlparse(input_target_value).path or "").lower().endswith(".m3u8")
        nested_network_resources = input_is_url if input_uses_network is None else bool(input_uses_network)
        start_value = max(0, int(start_seconds or 0))
        input_seek_value = start_value
        trim_seek_value = 0
        if start_value > 0:
            trim_seek_value = min(2, start_value)
            input_seek_value = max(0, start_value - trim_seek_value)
        if input_target_value:
            if nested_network_resources:
                header_values = sanitise_headers(request_headers)
                user_agent_value = clean_text(user_agent) or get_header_value(header_values, "User-Agent")
                command += [
                    "-progress",
                    "pipe:2",
                ]
                # For a local HLS master, reconnect and connect-timeout options survive
                # playlist opening and are then rejected by the nested segment demuxer.
                # Keep them for an HTTP top-level input; rw_timeout and request headers
                # are accepted and propagated when the local input is forced to HLS.
                if input_is_url:
                    command += [
                        "-reconnect",
                        "1",
                        "-reconnect_on_network_error",
                        "1",
                        "-reconnect_delay_max",
                        str(max(1, int(CSO_INGEST_RECONNECT_DELAY_MAX_SECONDS))),
                    ]
                if user_agent_value:
                    command += ["-user_agent", user_agent_value]
                referer_value = get_header_value(header_values, "Referer")
                if referer_value:
                    command += ["-referer", referer_value]
                extra_headers = format_ffmpeg_request_headers(header_values)
                if extra_headers:
                    command += ["-headers", extra_headers]
                if input_is_url and input_is_hls:
                    command += ["-reconnect_streamed", "0"]
                elif input_is_url:
                    command += [
                        "-reconnect_at_eof",
                        "1",
                        "-reconnect_streamed",
                        "1",
                        "-reconnect_on_http_error",
                        "4xx,5xx",
                    ]
                command += [
                    "-rw_timeout",
                    str(max(1_000_000, int(CSO_INGEST_RW_TIMEOUT_US))),
                ]
                if input_is_url:
                    command += [
                        "-timeout",
                        str(max(1_000_000, int(CSO_INGEST_TIMEOUT_US))),
                    ]
            if input_seek_value > 0:
                command += ["-ss", str(input_seek_value)]
            if realtime:
                command += ["-readrate", "1"]
            command += self._input_hwaccel_args(policy=self.policy)
            probe_size_bytes = int(CSO_INGEST_PROBE_SIZE_BYTES)
            analyse_duration_us = int(CSO_INGEST_ANALYSE_DURATION_US)
            fps_probe_size = int(CSO_INGEST_FPS_PROBE_SIZE)
            if input_is_hls:
                probe_size_bytes = max(probe_size_bytes, 10 * 1024 * 1024)
                analyse_duration_us = max(analyse_duration_us, 10_000_000)
                fps_probe_size = max(fps_probe_size, 128)
            command += self._probe_flags(
                probe_size_bytes,
                analyse_duration_us,
                fps_probe_size,
            )
            command += self._input_resilience_flags()
            protocol_whitelist = clean_text(input_protocol_whitelist)
            if protocol_whitelist:
                command += ["-protocol_whitelist", protocol_whitelist]
            elif nested_network_resources and not input_is_url:
                command += [
                    "-protocol_whitelist",
                    "file,http,https,tcp,tls,crypto",
                ]
            if input_is_hls and not input_is_url:
                command += ["-f", "hls"]
            command += ["-i", input_target_value]
            if trim_seek_value > 0:
                command += ["-ss", str(trim_seek_value)]
        else:
            command += self._build_pipe_input(
                pipe_probe_size_bytes,
                pipe_analyse_duration_us,
                pipe_fps_probe_size,
                low_latency=False,
                pipe_format=self.pipe_input_format,
                input_hwaccel_args=self._input_hwaccel_args(),
                include_stream_maps=False,
            )
        video_map = "0:v:0" if require_video else "0:v:0?"
        command += ["-map", video_map]
        required_audio_streams = max(0, int(required_audio_stream_count or 0))
        if required_audio_streams:
            for stream_index in range(required_audio_streams):
                command += ["-map", f"0:a:{stream_index}"]
        else:
            command += ["-map", "0:a?"]
        command += ["-max_muxing_queue_size", "4096"]
        subtitle_mode = self._apply_stream_selection(command)
        hls_policy = dict(self.policy or {})
        mode = hls_policy.get("output_mode") or "force_remux"
        playlist_mode = hls_policy.get("hls_playlist_mode") or "live"
        playlist_is_event = playlist_mode == "event"
        list_size_value = convert_to_int(hls_policy.get("hls_list_size"), CSO_HLS_LIST_SIZE)
        segment_type = hls_policy.get("hls_segment_type") or "mpegts"
        segment_extension = "m4s" if segment_type == "fmp4" else "ts"

        if mode == "force_transcode":
            self._apply_transcode_options(command, subtitle_mode, policy=hls_policy)
        else:
            command += ["-c", "copy"]
            if segment_type == "fmp4":
                command += ["-bsf:a", "aac_adtstoasc"]
            if subtitle_mode == "drop":
                command.append("-sn")
            if segment_type == "mpegts":
                command += self._mpegts_output_flags(zero_latency=False)

        command += self._drop_data_streams()
        if max_duration_seconds is not None:
            command += ["-t", str(max(1, int(max_duration_seconds or 0)))]
        segment_pattern = str(output_dir / f"seg_%06d.{segment_extension}")
        playlist_path = str(output_dir / "index.m3u8")
        hls_flags = ["temp_file", "independent_segments"]
        if playlist_is_event:
            hls_flags.append("append_list")
        else:
            hls_flags.extend(["delete_segments", "omit_endlist"])
        command += [
            "-f",
            "hls",
            "-hls_time",
            str(max(1, int(CSO_HLS_SEGMENT_SECONDS))),
            "-hls_segment_type",
            segment_type,
            "-hls_list_size",
            str(0 if playlist_is_event else max(3, int(list_size_value or CSO_HLS_LIST_SIZE))),
            "-hls_flags",
            "+".join(hls_flags),
            "-hls_segment_filename",
            segment_pattern,
        ]
        if segment_type == "fmp4":
            command += ["-hls_fmp4_init_filename", "init.mp4"]
        if playlist_is_event:
            command += ["-hls_playlist_type", "event"]
        else:
            command += ["-hls_delete_threshold", "2"]
        command.append(playlist_path)
        return command

    def build_slate_command(
        self,
        slate_type,
        primary_text="",
        secondary_text="",
        duration_seconds=10,
        output_target="pipe:1",
        realtime=False,
        media_hint=None,
    ):
        reason_key = clean_key(slate_type, fallback="playback_unavailable")
        duration_value = None if duration_seconds is None else max(1, int(duration_seconds))
        slate_media_hint = self._build_slate_media_hint(media_hint)
        startup_width = int(slate_media_hint.get("width") or 1280)
        startup_height = int(slate_media_hint.get("height") or 720)
        startup_fps = int(slate_media_hint.get("fps") or 25)
        startup_pix_fmt = clean_key(slate_media_hint.get("pixel_format")) or "yuv420p"
        render_fps = startup_fps if float(dict(media_hint or {}).get("fps") or 0.0) > 0 else 60
        layout_scale = min(float(startup_width) / 1280.0, float(startup_height) / 720.0)
        title_font_size = max(28, int(round(52 * layout_scale)))
        subtitle_font_size = max(14, int(round(20 * layout_scale)))
        panel_x = max(24, int(round(70 * float(startup_width) / 1280.0)))
        panel_w = max(320, int(round(1140 * float(startup_width) / 1280.0)))
        panel_h = max(160, int(round(340 * float(startup_height) / 720.0)))
        panel_y = max(
            12, int(round((startup_height - panel_h) / 2.0 - (160 * float(startup_height) / 720.0) + panel_h / 2.0))
        )
        logo_width = max(52, int(round(92 * layout_scale)))
        logo_margin_x = max(24, int(round(42 * float(startup_width) / 1280.0)))
        logo_margin_y = max(24, int(round(34 * float(startup_height) / 720.0)))
        title_y = int(round((startup_height / 2.0) - (84 * float(startup_height) / 720.0)))
        subtitle_y_1 = int(round((startup_height / 2.0) + (2 * float(startup_height) / 720.0)))
        subtitle_y_2 = int(round((startup_height / 2.0) + (30 * float(startup_height) / 720.0)))
        subtitle_y_3 = int(round((startup_height / 2.0) + (58 * float(startup_height) / 720.0)))
        subtitle_y_4 = int(round((startup_height / 2.0) + (86 * float(startup_height) / 720.0)))
        blob1_size = max(220, int(round(680 * layout_scale)))
        blob2_size = max(240, int(round(760 * layout_scale)))
        blob3_size = max(210, int(round(620 * layout_scale)))
        blob1_side_size = max(80, int(round(240 * layout_scale)))
        blob2_side_size = max(72, int(round(210 * layout_scale)))
        startup_video = f"color=c=black:s={startup_width}x{startup_height}:r={startup_fps}"
        startup_audio = "anullsrc=channel_layout=stereo:sample_rate=48000"
        if duration_value is not None:
            startup_video = f"{startup_video}:d={duration_value}"
            startup_audio = f"{startup_audio}:d={duration_value}"
        if reason_key == "startup_pending":
            command = ["ffmpeg", "-hide_banner", "-loglevel", "warning"]
            if realtime:
                command += ["-re"]
            command += self._lavfi_input(startup_video)
            command += self._lavfi_input(startup_audio)
            command += self._slate_av_encode_flags(startup_fps, startup_pix_fmt, "128k", still_image=True)
            command += self._mpegts_output_flags(zero_latency=False)
            command += self._pipe_output_target("mpegts", target=output_target)
            return command

        title = _escape_ffmpeg_drawtext_text(clean_text(primary_text))
        subtitle_lines = [
            _escape_ffmpeg_drawtext_text(line)
            for line in wrap_slate_words(clean_text(secondary_text), max_chars=84, max_lines=4)
        ]
        drawtext_title = (
            f"drawtext=text='{title}':fontcolor=white:fontsize={title_font_size}:x=(w-text_w)/2:y={title_y}"
        )
        drawtext_subtitle_1 = (
            "drawtext="
            f"text='{subtitle_lines[0] if len(subtitle_lines) > 0 else ''}':"
            f"fontcolor=white:fontsize={subtitle_font_size}:"
            f"x=(w-text_w)/2:y={subtitle_y_1}"
        )
        drawtext_subtitle_2 = (
            "drawtext="
            f"text='{subtitle_lines[1] if len(subtitle_lines) > 1 else ''}':"
            f"fontcolor=white:fontsize={subtitle_font_size}:"
            f"x=(w-text_w)/2:y={subtitle_y_2}"
        )
        drawtext_subtitle_3 = (
            "drawtext="
            f"text='{subtitle_lines[2] if len(subtitle_lines) > 2 else ''}':"
            f"fontcolor=white:fontsize={subtitle_font_size}:"
            f"x=(w-text_w)/2:y={subtitle_y_3}"
        )
        drawtext_subtitle_4 = (
            "drawtext="
            f"text='{subtitle_lines[3] if len(subtitle_lines) > 3 else ''}':"
            f"fontcolor=white:fontsize={subtitle_font_size}:"
            f"x=(w-text_w)/2:y={subtitle_y_4}"
        )
        draw_panel = f"drawbox=x={panel_x}:y={panel_y}:w={panel_w}:h={panel_h}:color=0x0B0F14@0.64:t=fill"
        draw_border = f"drawbox=x={panel_x}:y={panel_y}:w={panel_w}:h={panel_h}:color=0xE2E8F0@0.16:t=2"
        logo_path = resolve_cso_unavailable_logo_path()
        filter_steps = [
            "[1:v]format=rgba,colorchannelmixer=aa=0.30,gblur=sigma=90[blob1]",
            "[2:v]format=rgba,colorchannelmixer=aa=0.26,gblur=sigma=105[blob2]",
            "[3:v]format=rgba,colorchannelmixer=aa=0.24,gblur=sigma=98[blob3]",
            "[0:v][blob1]overlay=x='(W-w)/2-W*0.14+sin(2*PI*t/12)*42':y='H*0.16+cos(2*PI*t/12)*24':shortest=1[bg1]",
            "[bg1][blob2]overlay=x='(W-w)/2+W*0.12+cos(2*PI*t/11+0.8)*46':y='H*0.18+sin(2*PI*t/11+0.8)*28':shortest=1[bg2]",
            "[bg2][blob3]overlay=x='(W-w)/2+W*0.02+sin(2*PI*t/13+1.6)*50':y='H*0.54+cos(2*PI*t/13+1.6)*22':shortest=1[bg3]",
            f"[blob1]scale=w={blob1_side_size}:h={blob1_side_size}[blob1_side]",
            f"[blob2]scale=w={blob2_side_size}:h={blob2_side_size}[blob2_side]",
            "[bg3][blob1_side]overlay=x='W*0.06+sin(2*PI*t/9+0.35)*18':y='H*0.28+cos(2*PI*t/9+0.95)*14':shortest=1[bg4]",
            "[bg4][blob2_side]overlay=x='W-w-W*0.07+cos(2*PI*t/9+1.15)*20':y='H*0.72+sin(2*PI*t/9+0.55)*12':shortest=1[bg5]",
            f"[bg5]gblur=sigma=42:steps=3,fps={render_fps}[bg_blur]",
        ]
        input_args = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "warning",
        ]
        input_args += self._lavfi_input(
            f"color=c=0x0B0F14:s={startup_width}x{startup_height}:r={render_fps}"
            + (f":d={duration_value}" if duration_value is not None else "")
        )
        input_args += self._lavfi_input(
            f"color=c=0x21A3CF:s={blob1_size}x{blob1_size}:r={render_fps}"
            + (f":d={duration_value}" if duration_value is not None else "")
        )
        input_args += self._lavfi_input(
            f"color=c=0x79D2C0:s={blob2_size}x{blob2_size}:r={render_fps}"
            + (f":d={duration_value}" if duration_value is not None else "")
        )
        input_args += self._lavfi_input(
            f"color=c=0x6AA8FF:s={blob3_size}x{blob3_size}:r={render_fps}"
            + (f":d={duration_value}" if duration_value is not None else "")
        )
        if logo_path:
            input_args += ["-loop", "1", "-i", logo_path]
            filter_steps.append(
                f"[4:v]scale=w={logo_width}:h=-1:flags=lanczos,format=rgba,colorchannelmixer=aa=0.98[logo]"
            )
            filter_steps.append(f"[bg_blur][logo]overlay=x={logo_margin_x}:y={logo_margin_y}:shortest=1[bg_logo]")
            background_label = "bg_logo"
        else:
            background_label = "bg_blur"
        filter_steps.append(f"[{background_label}]{draw_panel}[panel]")
        filter_steps.append(f"[panel]{draw_border}[panel2]")
        panel_label = "panel2"
        filter_steps += [
            f"[{panel_label}]{drawtext_title}[title1]",
            "[title1]" + drawtext_subtitle_1 + "[title2]",
            "[title2]" + drawtext_subtitle_2 + "[title3]",
            "[title3]" + drawtext_subtitle_3 + "[title4]",
            "[title4]" + drawtext_subtitle_4 + ",eq=brightness=-0.03:contrast=1.06:saturation=1.18[vout]",
        ]
        command = list(input_args)
        command += self._lavfi_input(
            "anullsrc=channel_layout=stereo:sample_rate=48000"
            + (f":d={duration_value}" if duration_value is not None else "")
        )
        command += [
            "-filter_complex",
            ";".join(filter_steps),
            "-map",
            "[vout]",
            "-map",
            f"{5 if logo_path else 4}:a",
        ]
        command += self._slate_av_encode_flags(render_fps, "yuv420p", "96k", still_image=False)
        command += self._mpegts_output_flags(zero_latency=False)
        command += self._pipe_output_target("mpegts", target=output_target)
        return command
