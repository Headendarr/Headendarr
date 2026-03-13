#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from collections import defaultdict
from datetime import datetime, timezone

from quart import request, jsonify
from sqlalchemy import select, and_, cast, Integer

from backend.api import blueprint
from backend.auth import streamer_or_admin_required
from backend.channels import read_config_all_channels
from backend.dummy_epg import DUMMY_EPG_SOURCE_ID, build_dummy_epg_programmes, sanitise_dummy_epg_interval
from backend.epgs import _shift_xmltv_window
from backend.models import Session, EpgChannels, EpgChannelProgrammes


def _now_ts():
    return int(datetime.now(tz=timezone.utc).timestamp())


def _shift_programme_window(programme, offset_minutes):
    start_value, stop_value, start_ts, stop_ts = _shift_xmltv_window(
        None,
        None,
        programme.get("start_ts"),
        programme.get("stop_ts"),
        offset_minutes,
    )
    try:
        shifted_start_ts = int(start_ts or 0)
    except (TypeError, ValueError):
        shifted_start_ts = 0
    try:
        shifted_stop_ts = int(stop_ts or 0)
    except (TypeError, ValueError):
        shifted_stop_ts = 0
    return shifted_start_ts, shifted_stop_ts


@blueprint.route("/tic-api/guide/grid", methods=["GET"])
@streamer_or_admin_required
async def api_guide_grid():
    start_ts = int(request.args.get("start_ts", _now_ts()))
    end_ts = int(request.args.get("end_ts", start_ts + 6 * 3600))

    channels = await read_config_all_channels()
    guide_channels = [
        channel
        for channel in channels
        if channel.get("enabled")
        and channel.get("guide", {}).get("epg_id")
        and channel.get("guide", {}).get("channel_id")
    ]
    dummy_guide_channels = [
        channel for channel in guide_channels if channel.get("guide", {}).get("epg_id") == DUMMY_EPG_SOURCE_ID
    ]
    mapped_guide_channels = [
        channel for channel in guide_channels if channel.get("guide", {}).get("epg_id") != DUMMY_EPG_SOURCE_ID
    ]

    if not guide_channels:
        return jsonify({"success": True, "channels": [], "programmes": []})

    pairs = {(c["guide"]["epg_id"], c["guide"]["channel_id"]) for c in mapped_guide_channels}
    epg_ids = {p[0] for p in pairs}
    channel_ids = {p[1] for p in pairs}
    offset_minutes = [int(channel.get("guide", {}).get("offset_minutes", 0) or 0) for channel in mapped_guide_channels]
    min_offset_seconds = min(offset_minutes, default=0) * 60
    max_offset_seconds = max(offset_minutes, default=0) * 60
    query_start_ts = start_ts - max_offset_seconds
    query_end_ts = end_ts - min_offset_seconds

    async with Session() as session:
        epg_channels = []
        if pairs:
            result = await session.execute(
                select(EpgChannels).where(
                    and_(
                        EpgChannels.epg_id.in_(epg_ids),
                        EpgChannels.channel_id.in_(channel_ids),
                    )
                )
            )
            epg_channels = result.scalars().all()
        epg_by_pair = {}
        for epg_channel in epg_channels:
            epg_by_pair[(epg_channel.epg_id, epg_channel.channel_id)] = epg_channel.id

        epg_channel_ids = [epg_by_pair.get(pair) for pair in pairs if epg_by_pair.get(pair)]
        programmes = []
        if epg_channel_ids:
            prog_result = await session.execute(
                select(EpgChannelProgrammes).where(
                    and_(
                        EpgChannelProgrammes.epg_channel_id.in_(epg_channel_ids),
                        cast(EpgChannelProgrammes.start_timestamp, Integer) <= query_end_ts,
                        cast(EpgChannelProgrammes.stop_timestamp, Integer) >= query_start_ts,
                    )
                )
            )
            for programme in prog_result.scalars().all():
                programmes.append(
                    {
                        "id": programme.id,
                        "epg_channel_id": programme.epg_channel_id,
                        "channel_id": programme.channel_id,
                        "title": programme.title,
                        "sub_title": programme.sub_title,
                        "desc": programme.desc,
                        "icon_url": programme.icon_url,
                        "start_ts": int(programme.start_timestamp or 0),
                        "stop_ts": int(programme.stop_timestamp or 0),
                    }
                )

        channel_pair_map = defaultdict(list)
        for channel in mapped_guide_channels:
            pair = (channel["guide"]["epg_id"], channel["guide"]["channel_id"])
            epg_channel_id = epg_by_pair.get(pair)
            if epg_channel_id:
                channel_pair_map[epg_channel_id].append(
                    {
                        "channel_id": channel["id"],
                        "offset_minutes": int(channel.get("guide", {}).get("offset_minutes", 0) or 0),
                    }
                )

        mapped_programmes = []
        for programme in programmes:
            mapped_channels = channel_pair_map.get(programme["epg_channel_id"])
            if not mapped_channels:
                continue
            for i, mapped_channel in enumerate(mapped_channels):
                shifted_start_ts, shifted_stop_ts = _shift_programme_window(
                    programme,
                    mapped_channel["offset_minutes"],
                )
                if shifted_start_ts > end_ts or shifted_stop_ts < start_ts:
                    continue
                prog_copy = programme.copy()
                prog_copy["channel_id"] = mapped_channel["channel_id"]
                prog_copy["epg_programme_id"] = programme["id"]
                prog_copy["start_ts"] = shifted_start_ts
                prog_copy["stop_ts"] = shifted_stop_ts
                # Ensure each programme object sent to the frontend has a unique ID,
                # as Vue uses it for list keys.
                prog_copy["id"] = f"{programme['id']}-{i}"
                mapped_programmes.append(prog_copy)

        for channel in dummy_guide_channels:
            interval_minutes = sanitise_dummy_epg_interval(
                channel.get("guide", {}).get("dummy_interval_minutes"),
            )
            mapped_programmes.extend(
                build_dummy_epg_programmes(
                    channel_id=channel["id"],
                    channel_name=channel.get("name"),
                    start_ts=start_ts,
                    end_ts=end_ts,
                    interval_minutes=interval_minutes,
                    offset_minutes=int(channel.get("guide", {}).get("offset_minutes", 0) or 0),
                )
            )

        return jsonify(
            {
                "success": True,
                "channels": guide_channels,
                "programmes": mapped_programmes,
                "start_ts": start_ts,
                "end_ts": end_ts,
            }
        )
