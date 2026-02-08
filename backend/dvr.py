#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import logging
from datetime import datetime, timezone

from sqlalchemy import select, and_
from sqlalchemy.orm import selectinload

from backend.models import Session, Recording, RecordingRule, Channel, EpgChannels, EpgChannelProgrammes
from backend.tvheadend.tvh_requests import get_tvh, ensure_tvh_sync_user

logger = logging.getLogger('tic.dvr')


def _now_ts():
    return int(datetime.now(tz=timezone.utc).timestamp())


def _serialize_recording(rec: Recording):
    channel = rec.channel
    return {
        "id": rec.id,
        "channel_id": rec.channel_id,
        "channel_name": channel.name if channel else None,
        "channel_number": channel.number if channel else None,
        "channel_logo": channel.logo_url if channel else None,
        "title": rec.title,
        "description": rec.description,
        "start_ts": rec.start_ts,
        "stop_ts": rec.stop_ts,
        "epg_programme_id": rec.epg_programme_id,
        "status": rec.status,
        "sync_status": rec.sync_status,
        "sync_error": rec.sync_error,
        "tvh_uuid": rec.tvh_uuid,
        "rule_id": rec.rule_id,
    }


def _serialize_rule(rule: RecordingRule):
    channel = rule.channel
    return {
        "id": rule.id,
        "channel_id": rule.channel_id,
        "channel_name": channel.name if channel else None,
        "title_match": rule.title_match,
        "enabled": rule.enabled,
        "lookahead_days": rule.lookahead_days,
    }


async def list_recordings():
    async with Session() as session:
        result = await session.execute(
            select(Recording)
            .options(selectinload(Recording.channel))
            .order_by(Recording.start_ts.desc())
        )
        recordings = result.scalars().all()
        return [_serialize_recording(rec) for rec in recordings]


async def list_rules():
    async with Session() as session:
        result = await session.execute(
            select(RecordingRule)
            .options(selectinload(RecordingRule.channel))
            .order_by(RecordingRule.id.desc())
        )
        rules = result.scalars().all()
        return [_serialize_rule(rule) for rule in rules]


async def create_recording(channel_id, title, start_ts, stop_ts, description=None, epg_programme_id=None, rule_id=None):
    async with Session() as session:
        async with session.begin():
            recording = Recording(
                channel_id=channel_id,
                title=title,
                description=description,
                start_ts=start_ts,
                stop_ts=stop_ts,
                epg_programme_id=epg_programme_id,
                rule_id=rule_id,
                status="scheduled",
                sync_status="pending",
            )
            session.add(recording)
        await session.commit()
        await session.refresh(recording)
        return recording.id


async def cancel_recording(recording_id):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(Recording).where(Recording.id == recording_id))
            recording = result.scalar_one_or_none()
            if not recording:
                return False
            recording.status = "canceled"
            recording.sync_status = "pending"
        await session.commit()
        return True


async def delete_recording(recording_id):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(Recording).where(Recording.id == recording_id))
            recording = result.scalar_one_or_none()
            if not recording:
                return False
            recording.status = "deleted"
            recording.sync_status = "pending"
        await session.commit()
        return True


async def create_rule(channel_id, title_match, lookahead_days=7):
    async with Session() as session:
        async with session.begin():
            rule = RecordingRule(
                channel_id=channel_id,
                title_match=title_match,
                lookahead_days=lookahead_days,
                enabled=True,
            )
            session.add(rule)
        await session.commit()
        await session.refresh(rule)
        return rule.id


async def delete_rule(rule_id):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(RecordingRule).where(RecordingRule.id == rule_id))
            rule = result.scalar_one_or_none()
            if not rule:
                return False
            await session.delete(rule)
        await session.commit()
        return True


async def apply_recurring_rules(config):
    async with Session() as session:
        result = await session.execute(
            select(RecordingRule)
            .where(RecordingRule.enabled == True)
            .options(selectinload(RecordingRule.channel))
        )
        rules = result.scalars().all()

        if not rules:
            return

        for rule in rules:
            channel = rule.channel
            if not channel or not channel.guide_id or not channel.guide_channel_id:
                continue

            now_ts = _now_ts()
            end_ts = now_ts + (rule.lookahead_days * 86400)

            epg_channel_query = await session.execute(
                select(EpgChannels.id)
                .where(
                    and_(
                        EpgChannels.epg_id == channel.guide_id,
                        EpgChannels.channel_id == channel.guide_channel_id,
                    )
                )
            )
            epg_channel_id = epg_channel_query.scalar_one_or_none()
            if not epg_channel_id:
                continue

            programmes_query = await session.execute(
                select(EpgChannelProgrammes)
                .where(
                    and_(
                        EpgChannelProgrammes.epg_channel_id == epg_channel_id,
                        EpgChannelProgrammes.start_timestamp <= str(end_ts),
                        EpgChannelProgrammes.stop_timestamp >= str(now_ts),
                    )
                )
            )
            programmes = programmes_query.scalars().all()
            for programme in programmes:
                if rule.title_match and programme.title:
                    if rule.title_match.lower() not in programme.title.lower():
                        continue
                start_ts = int(programme.start_timestamp or 0)
                stop_ts = int(programme.stop_timestamp or 0)
                if start_ts <= 0 or stop_ts <= 0:
                    continue
                existing = await session.execute(
                    select(Recording).where(
                        and_(
                            Recording.channel_id == channel.id,
                            Recording.start_ts == start_ts,
                            Recording.stop_ts == stop_ts,
                        )
                    )
                )
                if existing.scalars().first():
                    continue
                session.add(
                    Recording(
                        channel_id=channel.id,
                        title=programme.title,
                        description=programme.desc,
                        start_ts=start_ts,
                        stop_ts=stop_ts,
                        epg_programme_id=programme.id,
                        rule_id=rule.id,
                        status="scheduled",
                        sync_status="pending",
                    )
                )
        await session.commit()


async def reconcile_tvh_recordings(config):
    await ensure_tvh_sync_user(config)
    async with await get_tvh(config) as tvh:
        tvh_entries = await tvh.list_dvr_entries()
        tvh_map = {entry.get("uuid"): entry for entry in tvh_entries if entry.get("uuid")}

        async with Session() as session:
            channel_result = await session.execute(select(Channel))
            channels = channel_result.scalars().all()
            channel_by_tvh = {c.tvh_uuid: c for c in channels if c.tvh_uuid}

            recordings_query = await session.execute(
                select(Recording).options(selectinload(Recording.channel))
            )
            recordings = recordings_query.scalars().all()

            # Process deletions first; only remove from TIC once TVH no longer has the entry.
            for rec in recordings:
                if rec.status != "deleted":
                    continue
                if rec.tvh_uuid and rec.tvh_uuid in tvh_map:
                    try:
                        await tvh.delete_dvr_entry(rec.tvh_uuid)
                    except Exception as exc:
                        logger.exception("Failed to delete DVR entry in TVH: %s", rec.tvh_uuid)
                        rec.sync_status = "failed"
                        rec.sync_error = str(exc)
                        continue
                    # Wait for TVH to drop the entry before removing from TIC.
                    rec.sync_status = "synced"
                    rec.sync_error = None
                    continue

                rec.sync_status = "synced"
                rec.sync_error = None
                await session.delete(rec)

            await session.commit()

            # Import TVH entries missing in TIC
            for entry in tvh_entries:
                tvh_uuid = entry.get("uuid")
                if not tvh_uuid:
                    continue
                existing = await session.execute(
                    select(Recording).where(Recording.tvh_uuid == tvh_uuid)
                )
                if existing.scalars().first():
                    continue

                channel = channel_by_tvh.get(entry.get("channel"))
                if not channel:
                    continue
                session.add(
                    Recording(
                        channel_id=channel.id,
                        title=entry.get("title"),
                        description=entry.get("description"),
                        start_ts=entry.get("start"),
                        stop_ts=entry.get("stop"),
                        status=entry.get("state") or "scheduled",
                        sync_status="synced",
                        tvh_uuid=tvh_uuid,
                    )
                )

            await session.commit()

            # Push TIC recordings to TVH
            recordings_query = await session.execute(
                select(Recording).options(selectinload(Recording.channel))
            )
            recordings = recordings_query.scalars().all()

            for rec in recordings:
                if rec.status == "deleted":
                    # Deletions are handled in the pre-pass; avoid overwriting status from TVH.
                    continue
                if rec.status == "canceled":
                    if rec.tvh_uuid:
                        try:
                            await tvh.delete_dvr_entry(rec.tvh_uuid)
                        except Exception as exc:
                            logger.exception("Failed to cancel DVR entry in TVH: %s", rec.tvh_uuid)
                            rec.sync_status = "failed"
                            rec.sync_error = str(exc)
                            continue
                        rec.sync_status = "synced"
                    continue

                if rec.tvh_uuid and rec.tvh_uuid in tvh_map:
                    entry = tvh_map.get(rec.tvh_uuid, {})
                    rec.status = entry.get("state") or rec.status
                    if entry.get("start"):
                        rec.start_ts = entry.get("start")
                    if entry.get("stop"):
                        rec.stop_ts = entry.get("stop")
                    if entry.get("title"):
                        rec.title = entry.get("title")
                    if entry.get("description"):
                        rec.description = entry.get("description")
                    rec.sync_status = "synced"
                    rec.sync_error = None
                    continue

                channel = rec.channel
                if not channel or not channel.tvh_uuid:
                    rec.sync_status = "failed"
                    rec.sync_error = "Channel not synced to TVH"
                    continue

                try:
                    tvh_uuid = await tvh.create_dvr_entry(
                        channel_uuid=channel.tvh_uuid,
                        start_ts=rec.start_ts,
                        stop_ts=rec.stop_ts,
                        title=rec.title or "Recording",
                        description=rec.description,
                    )
                    rec.tvh_uuid = tvh_uuid or rec.tvh_uuid
                    rec.sync_status = "synced"
                    rec.sync_error = None
                except Exception as exc:
                    rec.sync_status = "failed"
                    rec.sync_error = str(exc)

            await session.commit()
