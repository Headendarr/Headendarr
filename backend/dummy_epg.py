#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from datetime import datetime, timezone


DUMMY_EPG_SOURCE_ID = "__dummy__"
DUMMY_EPG_SOURCE_NAME = "Dummy"
DUMMY_EPG_DEFAULT_INTERVAL_MINUTES = 30
DUMMY_EPG_XMLTV_DAYS = 3
DUMMY_EPG_FALLBACK_DESCRIPTION = "Programme information is unavailable for this channel."
DUMMY_EPG_INTERVAL_OPTIONS = [30] + [hours * 60 for hours in range(1, 25)]


def build_dummy_epg_channel_key(interval_minutes):
    minutes = sanitise_dummy_epg_interval(interval_minutes)
    if minutes == 30:
        return "dummy-30m"
    hours = minutes // 60
    return f"dummy-{hours}h"


def parse_dummy_epg_channel_key(value):
    raw_value = str(value or "").strip().lower()
    if not raw_value.startswith("dummy-"):
        return None
    interval_part = raw_value[6:]
    if interval_part.endswith("m"):
        return sanitise_dummy_epg_interval(interval_part[:-1])
    if interval_part.endswith("h"):
        try:
            return sanitise_dummy_epg_interval(int(interval_part[:-1]) * 60)
        except (TypeError, ValueError):
            return None
    return None


def sanitise_dummy_epg_interval(value, default=None):
    fallback = DUMMY_EPG_DEFAULT_INTERVAL_MINUTES if default is None else default
    try:
        minutes = int(value)
    except (TypeError, ValueError):
        return fallback
    if minutes in DUMMY_EPG_INTERVAL_OPTIONS:
        return minutes
    return fallback


def build_dummy_epg_programmes(channel_id, channel_name, start_ts, end_ts, interval_minutes, offset_minutes=0):
    try:
        start_ts = int(start_ts)
        end_ts = int(end_ts)
    except (TypeError, ValueError):
        return []
    if end_ts <= start_ts:
        return []

    interval_minutes = sanitise_dummy_epg_interval(interval_minutes)
    interval_seconds = interval_minutes * 60
    offset_seconds = int(offset_minutes or 0) * 60
    first_start = ((start_ts - offset_seconds) // interval_seconds) * interval_seconds + offset_seconds
    if first_start > start_ts:
        first_start -= interval_seconds

    title = str(channel_name or "Scheduled programming").strip() or "Scheduled programming"
    programmes = []
    current_start = first_start
    while current_start < end_ts:
        current_stop = current_start + interval_seconds
        if current_stop > start_ts:
            programmes.append(
                {
                    "id": f"dummy-{channel_id}-{current_start}",
                    "channel_id": channel_id,
                    "title": title,
                    "desc": DUMMY_EPG_FALLBACK_DESCRIPTION,
                    "start_ts": current_start,
                    "stop_ts": current_stop,
                }
            )
        current_start = current_stop
    return programmes


def xmltv_datetime_from_timestamp(timestamp):
    try:
        parsed_timestamp = int(timestamp)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(parsed_timestamp, tz=timezone.utc).strftime("%Y%m%d%H%M%S +0000")
