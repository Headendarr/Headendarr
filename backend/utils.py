#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import hashlib
from datetime import datetime, timezone


def parse_entity_id(value: int | str, name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as err:
        raise ValueError(f"Invalid {name} id: {value}") from err


def is_truthy(value: object) -> bool:
    if isinstance(value, str):
        return value.lower() in ("1", "true", "yes", "on")
    return bool(value)


def convert_to_int(value: int | str | None, default: int = 1) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def convert_to_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    return is_truthy(value)


def clean_text(value: object) -> str:
    return str(value or "").strip()


def clean_key(value: object, fallback: object = "") -> str:
    cleaned = clean_text(value).lower()
    if cleaned:
        return cleaned
    return clean_text(fallback).lower()


def fast_url_hash(value: object) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    return hashlib.md5(raw.encode("utf-8"), usedforsecurity=False).hexdigest()


def as_naive_utc(dt_value: datetime | None) -> datetime | None:
    if dt_value is None:
        return None
    if dt_value.tzinfo is None:
        return dt_value
    return dt_value.astimezone(timezone.utc).replace(tzinfo=None)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_naive() -> datetime:
    return utc_now().replace(tzinfo=None)


def to_utc_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        as_utc = value.replace(tzinfo=timezone.utc)
    else:
        as_utc = value.astimezone(timezone.utc)
    return as_utc.isoformat().replace("+00:00", "Z")
