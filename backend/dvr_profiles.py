#!/usr/bin/env python3
# -*- coding:utf-8 -*-

from __future__ import annotations

import re

DEFAULT_RECORDING_PROFILES = [
    {
        "key": "default",
        "name": "Default",
        "pathname": "%F_%R $u$n.$x",
    },
    {
        "key": "shows",
        "name": "Shows",
        "pathname": "$Q$n.$x",
    },
    {
        "key": "movies",
        "name": "Movies",
        "pathname": "$Q$n.$x",
    },
]

_KEY_RE = re.compile(r"[^a-z0-9_\-]")

ALLOWED_RETENTION_POLICIES = (
    "1_day",
    "3_days",
    "5_days",
    "1_week",
    "2_weeks",
    "3_weeks",
    "1_month",
    "2_months",
    "3_months",
    "6_months",
    "1_year",
    "2_years",
    "3_years",
    "maintained_space",
    "forever",
)

DEFAULT_RETENTION_POLICY = "forever"

_RETENTION_DAYS_MAP = {
    "1_day": 1,
    "3_days": 3,
    "5_days": 5,
    "1_week": 7,
    "2_weeks": 14,
    "3_weeks": 21,
    "1_month": 31,
    "2_months": 62,
    "3_months": 93,
    "6_months": 186,
    "1_year": 365,
    "2_years": 730,
    "3_years": 1095,
}
_FOREVER_DAYS = 2147483646


def _sanitize_key(value: str | None, fallback: str) -> str:
    raw = str(value or "").strip().lower().replace(" ", "_")
    raw = _KEY_RE.sub("", raw)
    return raw or fallback


def normalize_recording_profiles(raw_profiles) -> list[dict]:
    profiles = []
    seen = set()

    for idx, raw in enumerate(raw_profiles or []):
        if not isinstance(raw, dict):
            continue
        key = _sanitize_key(raw.get("key") or raw.get("name"), f"profile_{idx + 1}")
        if key in seen:
            continue
        name = str(raw.get("name") or "").strip() or key.title()
        pathname = str(raw.get("pathname") or "").strip()
        if not pathname:
            continue
        profiles.append({"key": key, "name": name, "pathname": pathname})
        seen.add(key)

    if not profiles:
        return [item.copy() for item in DEFAULT_RECORDING_PROFILES]

    return profiles


def read_recording_profiles_from_settings(settings: dict) -> list[dict]:
    dvr_settings = (settings or {}).get("settings", {}).get("dvr", {})
    raw_profiles = dvr_settings.get("recording_profiles")
    if not isinstance(raw_profiles, list):
        return [item.copy() for item in DEFAULT_RECORDING_PROFILES]
    return normalize_recording_profiles(raw_profiles)


def get_profile_key_or_default(profile_key: str | None, profiles: list[dict]) -> str:
    if not profiles:
        return DEFAULT_RECORDING_PROFILES[0]["key"]
    normalized = _sanitize_key(profile_key, profiles[0]["key"])
    valid = {item["key"] for item in profiles}
    return normalized if normalized in valid else profiles[0]["key"]


def build_user_profile_name(username: str, template_name: str) -> str:
    safe_username = str(username or "user").strip() or "user"
    safe_name = str(template_name or "Default").strip() or "Default"
    return f"{safe_username} - {safe_name}"


def normalize_retention_policy(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in ALLOWED_RETENTION_POLICIES else DEFAULT_RETENTION_POLICY


def retention_policy_to_tvh_days(policy: str | None) -> tuple[int, int]:
    normalized = normalize_retention_policy(policy)
    if normalized == "maintained_space":
        # TVH value used to keep recordings until space pressure requires removal.
        return -1, -1
    if normalized == "forever":
        return _FOREVER_DAYS, _FOREVER_DAYS
    days = _RETENTION_DAYS_MAP.get(normalized, _FOREVER_DAYS)
    return days, days
