#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import os
import signal
import subprocess
import sys
import time

from watchgod import watch


IGNORE_SUBSTRS = (
    "/.git/",
    "/frontend/node_modules/",
    "/docs/node_modules/",
    "/dev_env/",
    "/migrations/",
)


def should_restart(changes):
    for _, path in changes:
        path = str(path)
        if any(token in path for token in IGNORE_SUBSTRS):
            continue
        return True
    return False


def start_process(cmd):
    return subprocess.Popen(cmd, env=os.environ.copy())


def stop_process(proc, timeout_seconds: float = 8.0):
    if not proc or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def handle_shutdown_signal(_signum, _frame):
    raise KeyboardInterrupt


def main():
    watch_path = os.getenv("WATCHGOD_PATH", "/app/backend")
    cmd = sys.argv[1:] or [sys.executable, os.getenv("FLASK_APP", "/app/run.py")]
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    proc = start_process(cmd)

    try:
        for changes in watch(watch_path, min_sleep=1):
            if not should_restart(changes):
                continue
            stop_process(proc)
            proc = start_process(cmd)
    except KeyboardInterrupt:
        pass
    finally:
        stop_process(proc)


if __name__ == "__main__":
    main()
