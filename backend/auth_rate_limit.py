#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
import time
from collections import deque
from dataclasses import dataclass

from backend import config


@dataclass
class RateLimitResult:
    allowed: bool
    retry_after: int = 0


class _SlidingWindowLimiter:
    def __init__(self, window_seconds: int, max_attempts: int):
        self.window_seconds = max(1, int(window_seconds))
        self.max_attempts = max(1, int(max_attempts))
        self._events = {}
        self._lock = asyncio.Lock()

    def _prune(self, history: deque, now: float):
        cutoff = now - self.window_seconds
        while history and history[0] < cutoff:
            history.popleft()

    async def check_and_record(self, key: str) -> RateLimitResult:
        now = time.time()
        async with self._lock:
            history = self._events.setdefault(key, deque())
            self._prune(history, now)
            if len(history) >= self.max_attempts:
                retry_after = max(1, int(self.window_seconds - (now - history[0])))
                return RateLimitResult(False, retry_after=retry_after)
            history.append(now)
            return RateLimitResult(True, retry_after=0)


class _FailureBackoffLimiter:
    def __init__(self, window_seconds: int, max_attempts: int, cooldown_base_seconds: int, cooldown_max_seconds: int):
        self.window_seconds = max(1, int(window_seconds))
        self.max_attempts = max(1, int(max_attempts))
        self.cooldown_base_seconds = max(1, int(cooldown_base_seconds))
        self.cooldown_max_seconds = max(self.cooldown_base_seconds, int(cooldown_max_seconds))
        self._events = {}
        self._cooldown_until = {}
        self._lock = asyncio.Lock()

    def _prune(self, history: deque, now: float):
        cutoff = now - self.window_seconds
        while history and history[0] < cutoff:
            history.popleft()

    async def precheck(self, key: str) -> RateLimitResult:
        now = time.time()
        async with self._lock:
            cooldown_until = self._cooldown_until.get(key, 0.0)
            if cooldown_until > now:
                return RateLimitResult(False, retry_after=max(1, int(cooldown_until - now)))
            history = self._events.setdefault(key, deque())
            self._prune(history, now)
            if len(history) >= self.max_attempts and history:
                retry_after = max(1, int(self.window_seconds - (now - history[0])))
                return RateLimitResult(False, retry_after=retry_after)
            return RateLimitResult(True, retry_after=0)

    async def record_failure(self, key: str):
        now = time.time()
        async with self._lock:
            history = self._events.setdefault(key, deque())
            self._prune(history, now)
            history.append(now)
            over_limit = max(0, len(history) - self.max_attempts)
            if over_limit > 0:
                cooldown = min(self.cooldown_max_seconds, self.cooldown_base_seconds * (2 ** (over_limit - 1)))
                self._cooldown_until[key] = now + cooldown

    async def record_success(self, key: str):
        async with self._lock:
            self._events.pop(key, None)
            self._cooldown_until.pop(key, None)


_login_ip_failures = _FailureBackoffLimiter(
    window_seconds=config.auth_login_ip_window_seconds,
    max_attempts=config.auth_login_ip_max_attempts,
    cooldown_base_seconds=config.auth_login_cooldown_base_seconds,
    cooldown_max_seconds=config.auth_login_cooldown_max_seconds,
)
_login_user_failures = _FailureBackoffLimiter(
    window_seconds=config.auth_login_user_window_seconds,
    max_attempts=config.auth_login_user_max_attempts,
    cooldown_base_seconds=config.auth_login_cooldown_base_seconds,
    cooldown_max_seconds=config.auth_login_cooldown_max_seconds,
)
_oidc_start_ip_limiter = _SlidingWindowLimiter(
    window_seconds=config.auth_oidc_start_ip_window_seconds,
    max_attempts=config.auth_oidc_start_ip_max_attempts,
)
_oidc_callback_ip_limiter = _SlidingWindowLimiter(
    window_seconds=config.auth_oidc_callback_ip_window_seconds,
    max_attempts=config.auth_oidc_callback_ip_max_attempts,
)


def _enabled() -> bool:
    return bool(getattr(config, "auth_rate_limit_enabled", True))


def _ip_key(ip_address: str | None) -> str:
    return str(ip_address or "unknown").strip() or "unknown"


def _user_key(username: str | None, ip_address: str | None) -> str:
    username_part = str(username or "").strip().lower() or "unknown"
    ip_part = _ip_key(ip_address)
    return f"{username_part}|{ip_part}"


async def precheck_login_rate_limit(ip_address: str | None, username: str | None) -> RateLimitResult:
    if not _enabled():
        return RateLimitResult(True, retry_after=0)
    ip_result = await _login_ip_failures.precheck(_ip_key(ip_address))
    if not ip_result.allowed:
        return ip_result
    user_result = await _login_user_failures.precheck(_user_key(username, ip_address))
    if not user_result.allowed:
        return user_result
    return RateLimitResult(True, retry_after=0)


async def record_login_failure(ip_address: str | None, username: str | None):
    if not _enabled():
        return
    await _login_ip_failures.record_failure(_ip_key(ip_address))
    await _login_user_failures.record_failure(_user_key(username, ip_address))


async def record_login_success(ip_address: str | None, username: str | None):
    if not _enabled():
        return
    await _login_ip_failures.record_success(_ip_key(ip_address))
    await _login_user_failures.record_success(_user_key(username, ip_address))


async def check_oidc_start_rate_limit(ip_address: str | None) -> RateLimitResult:
    if not _enabled():
        return RateLimitResult(True, retry_after=0)
    return await _oidc_start_ip_limiter.check_and_record(_ip_key(ip_address))


async def check_oidc_callback_rate_limit(ip_address: str | None) -> RateLimitResult:
    if not _enabled():
        return RateLimitResult(True, retry_after=0)
    return await _oidc_callback_ip_limiter.check_and_record(_ip_key(ip_address))
