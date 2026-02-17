#!/usr/bin/env python3
# -*- coding:utf-8 -*-
from datetime import datetime, timezone

from quart import request, jsonify
from sqlalchemy import select, and_, cast, Integer

from backend.api import blueprint
from backend.auth import streamer_or_admin_required
from backend.channels import read_config_all_channels
from backend.models import Session, EpgChannels, EpgChannelProgrammes


def _now_ts():
    return int(datetime.now(tz=timezone.utc).timestamp())


@blueprint.route('/tic-api/guide/grid', methods=['GET'])
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

    if not guide_channels:
        return jsonify({"success": True, "channels": [], "programmes": []})

    pairs = {(c["guide"]["epg_id"], c["guide"]["channel_id"]) for c in guide_channels}
    epg_ids = {p[0] for p in pairs}
    channel_ids = {p[1] for p in pairs}

    async with Session() as session:
        result = await session.execute(
            select(EpgChannels)
            .where(
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
                select(EpgChannelProgrammes)
                .where(
                    and_(
                        EpgChannelProgrammes.epg_channel_id.in_(epg_channel_ids),
                        cast(EpgChannelProgrammes.start_timestamp, Integer) <= end_ts,
                        cast(EpgChannelProgrammes.stop_timestamp, Integer) >= start_ts,
                    )
                )
            )
            for programme in prog_result.scalars().all():
                programmes.append({
                    "id": programme.id,
                    "epg_channel_id": programme.epg_channel_id,
                    "channel_id": programme.channel_id,
                    "title": programme.title,
                    "sub_title": programme.sub_title,
                    "desc": programme.desc,
                    "icon_url": programme.icon_url,
                    "start_ts": int(programme.start_timestamp or 0),
                    "stop_ts": int(programme.stop_timestamp or 0),
                })

    # Map programmes to TIC channel ids
    channel_pair_map = {}
    for channel in guide_channels:
        pair = (channel["guide"]["epg_id"], channel["guide"]["channel_id"])
        epg_channel_id = epg_by_pair.get(pair)
        if epg_channel_id:
            channel_pair_map[epg_channel_id] = channel["id"]

    mapped_programmes = []
    for programme in programmes:
        channel_id = channel_pair_map.get(programme["epg_channel_id"])
        if not channel_id:
            continue
        programme["channel_id"] = channel_id
        mapped_programmes.append(programme)

    return jsonify({
        "success": True,
        "channels": guide_channels,
        "programmes": mapped_programmes,
        "start_ts": start_ts,
        "end_ts": end_ts,
    })
