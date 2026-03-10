#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import hashlib
from datetime import timezone


def parse_entity_id(value, name):
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {name} id: {value}")


def is_truthy(value):
    if isinstance(value, str):
        return value.lower() in ("1", "true", "yes", "on")
    return bool(value)


def convert_to_int(value, default=1):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def convert_to_bool(value, default=False):
    if value is None:
        return default
    return is_truthy(value)


def fast_url_hash(value):
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    return hashlib.md5(raw.encode("utf-8"), usedforsecurity=False).hexdigest()


def as_naive_utc(dt_value):
    if dt_value is None:
        return None
    if dt_value.tzinfo is None:
        return dt_value
    return dt_value.astimezone(timezone.utc).replace(tzinfo=None)
