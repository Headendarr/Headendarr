import asyncio
import logging
import os
import signal
import time
import weakref
from dataclasses import dataclass
from typing import Any, Iterable


logger = logging.getLogger("cso")


@dataclass(frozen=True)
class CsoFfmpegTeardownResult:
    pid: int | None
    process_group_id: int | None
    signal_sequence: tuple[str, ...]
    return_code: int | None
    elapsed_seconds: float
    failure: str | None
    pid_gone: bool
    process_group_gone: bool
    confirmed: bool


@dataclass(frozen=True)
class CsoTaskCleanupResult:
    pending_tasks: tuple[Any, ...]
    pending_task_names: tuple[str, ...]
    failures: tuple[str, ...]
    elapsed_seconds: float
    confirmed: bool


@dataclass(frozen=True)
class CsoLifecycleCleanupResult:
    process_teardowns: tuple[CsoFfmpegTeardownResult, ...]
    task_cleanup: CsoTaskCleanupResult

    @property
    def confirmed(self) -> bool:
        return self.task_cleanup.confirmed and all(result.confirmed for result in self.process_teardowns)

    @property
    def process_cleanup_confirmed(self) -> bool:
        return all(result.confirmed for result in self.process_teardowns)

    @property
    def return_code(self) -> int | None:
        for result in reversed(self.process_teardowns):
            if result.return_code is not None:
                return result.return_code
        return None

    @property
    def failure(self) -> str | None:
        failures = [result.failure for result in self.process_teardowns if result.failure]
        if self.task_cleanup.pending_task_names:
            failures.append(f"pending_tasks:{','.join(self.task_cleanup.pending_task_names)}")
        failures.extend(self.task_cleanup.failures)
        return "; ".join(failures) if failures else None

    @property
    def process_teardown(self) -> CsoFfmpegTeardownResult | None:
        return self.process_teardowns[-1] if self.process_teardowns else None


@dataclass
class _TrackedCsoFfmpegProcess:
    process: Any
    label: str
    pid: int
    process_group_id: int | None
    started_at: float
    last_teardown: CsoFfmpegTeardownResult | None = None


def _pid_is_running(pid: int | None) -> bool:
    if pid is None or int(pid) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return True
    return True


def _process_group_is_running(process_group_id: int | None) -> bool:
    if process_group_id is None or int(process_group_id) <= 0:
        return False
    # Confirmation means that no process-group member remains in /proc,
    # including zombies. Zombies no longer own FFmpeg RSS, but reporting the
    # group gone before they are reaped violates the teardown result contract
    # and can hide descendant PID-table retention.
    try:
        with os.scandir("/proc") as entries:
            for entry in entries:
                if not entry.name.isdigit():
                    continue
                try:
                    with open(f"/proc/{entry.name}/stat", encoding="utf-8") as stat_file:
                        stat_text = stat_file.read()
                    _, separator, remainder = stat_text.rpartition(") ")
                    if not separator:
                        continue
                    fields = remainder.split()
                    if len(fields) < 3 or int(fields[2]) != int(process_group_id):
                        continue
                    return True
                except (FileNotFoundError, ProcessLookupError):
                    continue
                except (PermissionError, ValueError):
                    raise
        return False
    except Exception:
        pass
    try:
        os.killpg(int(process_group_id), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return True
    return True


def _safe_process_group_id(pid: int | None) -> int | None:
    if pid is None or int(pid) <= 0:
        return None
    try:
        process_group_id = int(os.getpgid(int(pid)))
    except Exception:
        return None
    # A CSO process group is deliberately created with the FFmpeg PID as its
    # leader. Never signal an inherited group that could contain Headendarr.
    if process_group_id != int(pid) or process_group_id == int(os.getpgrp()):
        return None
    return process_group_id


class CsoFfmpegProcessRegistry:
    def __init__(self):
        self._processes: dict[int, _TrackedCsoFfmpegProcess] = {}
        self._teardown_locks: weakref.WeakValueDictionary[int, asyncio.Lock] = weakref.WeakValueDictionary()
        self.teardown_failures = 0

    def register(self, process: Any, label: str) -> None:
        pid = getattr(process, "pid", None)
        if pid is None or int(pid) <= 0:
            return
        resolved_pid = int(pid)
        self._processes[resolved_pid] = _TrackedCsoFfmpegProcess(
            process=process,
            label=str(label or "cso-ffmpeg"),
            pid=resolved_pid,
            process_group_id=_safe_process_group_id(resolved_pid),
            started_at=time.monotonic(),
        )

    def process_group_id(self, process: Any) -> int | None:
        pid = getattr(process, "pid", None)
        if pid is None:
            return None
        tracked = self._processes.get(int(pid))
        if tracked is not None:
            return tracked.process_group_id
        return _safe_process_group_id(int(pid))

    def retained_process(self, label: str) -> Any | None:
        resolved_label = str(label or "cso-ffmpeg")
        for tracked in list(self._processes.values()):
            if tracked.label != resolved_label:
                continue
            if not self.mark_exited(tracked.process):
                return tracked.process
        return None

    def teardown_lock(self, process: Any) -> asyncio.Lock:
        pid = int(getattr(process, "pid", None) or id(process))
        lock = self._teardown_locks.get(pid)
        if lock is None:
            lock = asyncio.Lock()
            self._teardown_locks[pid] = lock
        return lock

    def mark_teardown(self, process: Any, result: CsoFfmpegTeardownResult) -> None:
        pid = result.pid
        if not result.confirmed:
            self.teardown_failures += 1
        if pid is None:
            return
        tracked = self._processes.get(int(pid))
        if result.confirmed:
            self._processes.pop(int(pid), None)
        elif tracked is not None:
            tracked.last_teardown = result

    def mark_exited(self, process: Any) -> bool:
        pid = getattr(process, "pid", None)
        if pid is None:
            return True
        tracked = self._processes.get(int(pid))
        process_group_id = tracked.process_group_id if tracked is not None else _safe_process_group_id(int(pid))
        confirmed = not _pid_is_running(int(pid)) and not _process_group_is_running(process_group_id)
        if confirmed:
            self._processes.pop(int(pid), None)
        return confirmed

    def snapshot(self) -> dict[str, Any]:
        for tracked in list(self._processes.values()):
            self.mark_exited(tracked.process)
        return {
            "live_ffmpeg_processes": len(self._processes),
            "ffmpeg_teardown_failures": int(self.teardown_failures),
            "live_ffmpeg_pids": sorted(self._processes),
        }

    def active_processes(self) -> tuple[Any, ...]:
        for tracked in list(self._processes.values()):
            self.mark_exited(tracked.process)
        return tuple(tracked.process for tracked in self._processes.values())

    def reset_for_tests(self) -> None:
        self._processes.clear()
        self._teardown_locks.clear()
        self.teardown_failures = 0


cso_ffmpeg_process_registry = CsoFfmpegProcessRegistry()


async def spawn_cso_ffmpeg_process(*command: str, label: str, **kwargs: Any):
    if "start_new_session" in kwargs:
        raise ValueError("CSO FFmpeg process groups are managed internally")
    process = await asyncio.create_subprocess_exec(
        *command,
        start_new_session=True,
        **kwargs,
    )
    cso_ffmpeg_process_registry.register(process, label)
    logger.info(
        "CSO FFmpeg process started label=%s pid=%s process_group_id=%s live_ffmpeg_processes=%s",
        label,
        getattr(process, "pid", None),
        cso_ffmpeg_process_registry.process_group_id(process),
        cso_ffmpeg_process_registry.snapshot()["live_ffmpeg_processes"],
    )
    return process


def mark_cso_ffmpeg_process_exited(process: Any) -> bool:
    return cso_ffmpeg_process_registry.mark_exited(process)


def retained_cso_ffmpeg_process(label: str) -> Any | None:
    return cso_ffmpeg_process_registry.retained_process(label)


def _task_name(task: Any) -> str:
    get_name = getattr(task, "get_name", None)
    if callable(get_name):
        try:
            return str(get_name())
        except Exception:
            pass
    return f"task-{id(task)}"


def combine_cso_task_cleanup_results(
    *results: CsoTaskCleanupResult | None,
) -> CsoTaskCleanupResult:
    present_results = tuple(result for result in results if result is not None)
    pending_tasks_by_id = {id(task): task for result in present_results for task in result.pending_tasks}
    pending_tasks = tuple(sorted(pending_tasks_by_id.values(), key=_task_name))
    failures = tuple(sorted({failure for result in present_results for failure in result.failures}))
    return CsoTaskCleanupResult(
        pending_tasks=pending_tasks,
        pending_task_names=tuple(_task_name(task) for task in pending_tasks),
        failures=failures,
        elapsed_seconds=sum(result.elapsed_seconds for result in present_results),
        confirmed=all(result.confirmed for result in present_results) and not pending_tasks,
    )


def cso_lifecycle_cleanup_result(
    process_teardown: CsoFfmpegTeardownResult | None,
    task_cleanup: CsoTaskCleanupResult | None = None,
) -> CsoLifecycleCleanupResult:
    return CsoLifecycleCleanupResult(
        process_teardowns=(process_teardown,) if process_teardown is not None else (),
        task_cleanup=combine_cso_task_cleanup_results(task_cleanup),
    )


def combine_cso_lifecycle_cleanup_results(
    *results: CsoLifecycleCleanupResult | CsoFfmpegTeardownResult | None,
    task_cleanup: CsoTaskCleanupResult | None = None,
) -> CsoLifecycleCleanupResult:
    present_results = tuple(
        result if isinstance(result, CsoLifecycleCleanupResult) else cso_lifecycle_cleanup_result(result)
        for result in results
        if result is not None
    )
    return CsoLifecycleCleanupResult(
        process_teardowns=tuple(
            process_teardown for result in present_results for process_teardown in result.process_teardowns
        ),
        task_cleanup=combine_cso_task_cleanup_results(
            *(result.task_cleanup for result in present_results),
            task_cleanup,
        ),
    )


async def cancel_and_await_tasks(
    tasks: Iterable[Any],
    *,
    timeout_seconds: float = 1.0,
) -> CsoTaskCleanupResult:
    started_at = time.monotonic()
    current_task = asyncio.current_task()
    active_tasks = [task for task in tasks if task is not None and task is not current_task]
    for task in active_tasks:
        if task.done():
            continue
        task.cancel()
    done = {task for task in active_tasks if task.done()}
    pending = {task for task in active_tasks if not task.done()}
    if pending:
        newly_done, pending = await asyncio.wait(
            pending,
            timeout=max(0.01, float(timeout_seconds)),
        )
        done.update(newly_done)
    failures = []
    for task in done:
        if task.cancelled():
            continue
        try:
            failure = task.exception()
            if failure is not None:
                failures.append(f"{_task_name(task)}:{type(failure).__name__}: {failure}")
        except BaseException as exc:
            failures.append(f"{_task_name(task)}:{type(exc).__name__}: {exc}")
    pending_tasks = tuple(sorted(pending, key=_task_name))
    pending_task_names = tuple(_task_name(task) for task in pending_tasks)
    result = CsoTaskCleanupResult(
        pending_tasks=pending_tasks,
        pending_task_names=pending_task_names,
        failures=tuple(sorted(failures)),
        elapsed_seconds=max(0.0, time.monotonic() - started_at),
        confirmed=not pending_tasks,
    )
    if result.pending_task_names or result.failures:
        logger.error(
            "CSO task cleanup confirmed=%s elapsed_ms=%s timeout_seconds=%s pending_tasks=%s failures=%s",
            result.confirmed,
            int(result.elapsed_seconds * 1000),
            float(timeout_seconds),
            ",".join(result.pending_task_names) or "none",
            "; ".join(result.failures) or "none",
        )
    return result


async def _close_process_stdin(process: Any) -> tuple[str, ...]:
    stdin = getattr(process, "stdin", None)
    if stdin is None:
        return ()
    failures = []
    try:
        stdin.close()
    except Exception as exc:
        failures.append(f"stdin.close:{type(exc).__name__}: {exc}")
    return tuple(failures)


def close_cso_ffmpeg_pipe_transports(process: Any, *, close_process_transport: bool = False) -> tuple[str, ...]:
    failures = []
    for stream_name in ("stdout", "stderr"):
        stream = getattr(process, stream_name, None)
        transport = getattr(stream, "_transport", None)
        close = getattr(transport, "close", None)
        if not callable(close):
            continue
        try:
            close()
        except Exception as exc:
            failures.append(f"{stream_name}.transport.close:{type(exc).__name__}: {exc}")
    if close_process_transport:
        transport = getattr(process, "_transport", None)
        close = getattr(transport, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:
                failures.append(f"process.transport.close:{type(exc).__name__}: {exc}")
    return tuple(failures)


def _send_process_signal(process: Any, process_group_id: int | None, signal_name: str) -> None:
    if process_group_id is not None:
        signum = signal.SIGTERM if signal_name == "SIGTERM" else signal.SIGKILL
        os.killpg(int(process_group_id), signum)
        return
    signal_method = getattr(process, "terminate" if signal_name == "SIGTERM" else "kill")
    signal_method()


async def _wait_for_process_and_group_exit(
    process: Any,
    pid: int | None,
    process_group_id: int | None,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + max(0.01, float(timeout_seconds))
    remaining = max(0.01, deadline - time.monotonic())
    await asyncio.wait_for(process.wait(), timeout=remaining)
    while time.monotonic() < deadline:
        if not _pid_is_running(pid) and not _process_group_is_running(process_group_id):
            return
        await asyncio.sleep(min(0.02, max(0.001, deadline - time.monotonic())))
    if _pid_is_running(pid) or _process_group_is_running(process_group_id):
        raise TimeoutError(f"process identity still present pid={pid} process_group_id={process_group_id}")


async def _terminate_cso_ffmpeg_process(
    process: Any,
    *,
    terminate_timeout_seconds: float = 2.0,
    kill_timeout_seconds: float = 2.0,
) -> CsoFfmpegTeardownResult:
    started_at = time.monotonic()
    if process is None:
        return CsoFfmpegTeardownResult(
            pid=None,
            process_group_id=None,
            signal_sequence=(),
            return_code=None,
            elapsed_seconds=0.0,
            failure=None,
            pid_gone=True,
            process_group_gone=True,
            confirmed=True,
        )

    pid_value = getattr(process, "pid", None)
    pid = int(pid_value) if pid_value is not None else None
    process_group_id = cso_ffmpeg_process_registry.process_group_id(process)
    tracked = cso_ffmpeg_process_registry._processes.get(int(pid or 0))
    process_label = tracked.label if tracked is not None else "cso-ffmpeg"
    signal_sequence = []
    failures = list(await _close_process_stdin(process))

    pid_gone = not _pid_is_running(pid)
    process_group_gone = not _process_group_is_running(process_group_id)
    if not (pid_gone and process_group_gone):
        signal_sequence.append("SIGTERM")
        try:
            _send_process_signal(process, process_group_id, "SIGTERM")
        except ProcessLookupError:
            pass
        except Exception as exc:
            failures.append(f"SIGTERM:{type(exc).__name__}: {exc}")
        try:
            await _wait_for_process_and_group_exit(
                process,
                pid,
                process_group_id,
                terminate_timeout_seconds,
            )
        except Exception as exc:
            failures.append(f"wait_after_SIGTERM:{type(exc).__name__}: {exc}")

    pid_gone = not _pid_is_running(pid)
    process_group_gone = not _process_group_is_running(process_group_id)
    if not (pid_gone and process_group_gone):
        signal_sequence.append("SIGKILL")
        try:
            _send_process_signal(process, process_group_id, "SIGKILL")
        except ProcessLookupError:
            pass
        except Exception as exc:
            failures.append(f"SIGKILL:{type(exc).__name__}: {exc}")
        try:
            await _wait_for_process_and_group_exit(
                process,
                pid,
                process_group_id,
                kill_timeout_seconds,
            )
        except Exception as exc:
            failures.append(f"wait_after_SIGKILL:{type(exc).__name__}: {exc}")

    pid_gone = not _pid_is_running(pid)
    process_group_gone = not _process_group_is_running(process_group_id)
    confirmed = bool(pid_gone and process_group_gone)
    failures.extend(close_cso_ffmpeg_pipe_transports(process, close_process_transport=confirmed))
    result = CsoFfmpegTeardownResult(
        pid=pid,
        process_group_id=process_group_id,
        signal_sequence=tuple(signal_sequence),
        return_code=getattr(process, "returncode", None),
        elapsed_seconds=max(0.0, time.monotonic() - started_at),
        failure="; ".join(failures) if failures else None,
        pid_gone=pid_gone,
        process_group_gone=process_group_gone,
        confirmed=confirmed,
    )
    cso_ffmpeg_process_registry.mark_teardown(process, result)
    log_method = logger.info if confirmed else logger.error
    log_method(
        "CSO FFmpeg teardown label=%s pid=%s process_group_id=%s signals=%s return_code=%s "
        "elapsed_ms=%s confirmed=%s pid_gone=%s process_group_gone=%s failure=%s "
        "live_ffmpeg_processes=%s ffmpeg_teardown_failures=%s",
        process_label,
        pid,
        process_group_id,
        ",".join(result.signal_sequence) or "none",
        result.return_code,
        int(result.elapsed_seconds * 1000),
        result.confirmed,
        result.pid_gone,
        result.process_group_gone,
        result.failure or "none",
        cso_ffmpeg_process_registry.snapshot()["live_ffmpeg_processes"],
        cso_ffmpeg_process_registry.teardown_failures,
    )
    return result


async def terminate_cso_ffmpeg_process(
    process: Any,
    *,
    terminate_timeout_seconds: float = 2.0,
    kill_timeout_seconds: float = 2.0,
) -> CsoFfmpegTeardownResult:
    if process is None:
        return await _terminate_cso_ffmpeg_process(
            process,
            terminate_timeout_seconds=terminate_timeout_seconds,
            kill_timeout_seconds=kill_timeout_seconds,
        )
    async with cso_ffmpeg_process_registry.teardown_lock(process):
        return await _terminate_cso_ffmpeg_process(
            process,
            terminate_timeout_seconds=terminate_timeout_seconds,
            kill_timeout_seconds=kill_timeout_seconds,
        )


async def kill_all_cso_ffmpeg_processes() -> tuple[CsoFfmpegTeardownResult, ...]:
    processes = cso_ffmpeg_process_registry.active_processes()
    if not processes:
        return ()
    logger.info("Fast-stopping CSO FFmpeg processes count=%s", len(processes))
    results = await asyncio.gather(
        *(
            terminate_cso_ffmpeg_process(
                process,
                terminate_timeout_seconds=0.05,
                kill_timeout_seconds=1.0,
            )
            for process in processes
        ),
        return_exceptions=True,
    )
    teardown_results = []
    for result in results:
        if isinstance(result, CsoFfmpegTeardownResult):
            teardown_results.append(result)
        else:
            logger.error("Failed to fast-stop CSO FFmpeg process", exc_info=result)
    return tuple(teardown_results)
