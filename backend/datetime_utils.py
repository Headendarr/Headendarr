#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from __future__ import annotations

from datetime import datetime, timezone


def utc_now_naive() -> datetime:
    """Return current UTC as naive datetime for DB fields stored without tzinfo."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def to_utc_iso(value: datetime | None) -> str | None:
    """Serialize datetime to explicit UTC ISO-8601 with trailing Z."""
    if value is None:
        return None
    if value.tzinfo is None:
        as_utc = value.replace(tzinfo=timezone.utc)
    else:
        as_utc = value.astimezone(timezone.utc)
    return as_utc.isoformat().replace("+00:00", "Z")
