#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import time
from typing import Any


class TtlCache:
    def __init__(self):
        self._store: dict[str, tuple[float | None, Any]] = {}

    def get(self, key: str):
        entry = self._store.get(key)
        if not entry:
            return None
        expires_at, value = entry
        if expires_at and expires_at < time.time():
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any, ttl_seconds: int = 30):
        expires_at = time.time() + ttl_seconds if ttl_seconds else None
        self._store[key] = (expires_at, value)


xc_cache = TtlCache()
