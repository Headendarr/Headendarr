#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import gzip
import json
import logging
import os
import shutil
from collections import defaultdict
from datetime import datetime
from mimetypes import guess_extension
from pathlib import Path
from urllib.parse import quote

import aiofiles
import aiohttp
import asyncio
import sys
import time
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
from quart.utils import run_sync
from sqlalchemy.orm import joinedload
from sqlalchemy import and_, delete, insert, select, text
from backend.channels import read_base46_image_string
from backend.models import db, Session, Epg, Channel, EpgChannels, EpgChannelProgrammes
from backend.tvheadend.tvh_requests import get_tvh
from backend.utils import normalize_id

logger = logging.getLogger('tic.epgs')


def generate_epg_channel_id(number, name):
    # return f"{number}_{re.sub(r'[^a-zA-Z0-9]', '', name)}"
    return str(number)


async def read_config_all_epgs(output_for_export=False):
    return_list = []
    async with Session() as session:
        async with session.begin():
            query = await session.execute(select(Epg))
            results = query.scalars().all()
            for result in results:
                if output_for_export:
                    return_list.append({
                        'enabled': result.enabled,
                        'name':    result.name,
                        'url':     result.url,
                        'user_agent': result.user_agent,
                    })
                    continue
                return_list.append({
                    'id':      result.id,
                    'enabled': result.enabled,
                    'name':    result.name,
                    'url':     result.url,
                    'user_agent': result.user_agent,
                })
    return return_list


async def read_config_one_epg(epg_id):
    epg_id = normalize_id(epg_id, "epg")
    return_item = {}
    async with Session() as session:
        async with session.begin():
            query = await session.execute(select(Epg).where(Epg.id == epg_id))
            results = query.scalar_one_or_none()
            if results:
                return_item = {
                    'id':      results.id,
                    'enabled': results.enabled,
                    'name':    results.name,
                    'url':     results.url,
                    'user_agent': results.user_agent,
                }
    return return_item


async def add_new_epg(data):
    async with Session() as session:
        async with session.begin():
            epg = Epg(
                enabled=data.get('enabled'),
                name=data.get('name'),
                url=data.get('url'),
                user_agent=data.get('user_agent'),
            )
            # This is a new entry. Add it to the session before commit
            session.add(epg)


async def update_epg(epg_id, data):
    epg_id = normalize_id(epg_id, "epg")
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(Epg).where(Epg.id == epg_id))
            epg = result.scalar_one()
            epg.enabled = data.get('enabled')
            epg.name = data.get('name')
            epg.url = data.get('url')
            epg.user_agent = data.get('user_agent', epg.user_agent)


async def delete_epg(config, epg_id):
    epg_id = normalize_id(epg_id, "epg")
    async with Session() as session:
        async with session.begin():
            # Get all channel IDs for the given EPG
            # noinspection DuplicatedCode
            result = await session.execute(select(EpgChannels.id).where(EpgChannels.epg_id == epg_id))
            channel_ids = [row[0] for row in result.fetchall()]
            if channel_ids:
                # Delete all EpgChannelProgrammes where epg_channel_id is in the list of channel IDs
                await session.execute(
                    delete(EpgChannelProgrammes).where(EpgChannelProgrammes.epg_channel_id.in_(channel_ids)))
                # Delete all EpgChannels where id is in the list of channel IDs
                await session.execute(delete(EpgChannels).where(EpgChannels.id.in_(channel_ids)))
            # Delete the Epg entry
            await session.execute(delete(Epg).where(Epg.id == epg_id))

    # Remove cached copy of epg
    cache_files = [
        os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.xml"),
        os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.yml"),
    ]
    for f in cache_files:
        if os.path.isfile(f):
            os.remove(f)


def _resolve_user_agent(settings, user_agent):
    if user_agent:
        return user_agent
    defaults = settings.get('settings', {}).get('user_agents', [])
    if isinstance(defaults, list) and defaults:
        return defaults[0].get('value') or defaults[0].get('name')
    return "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0"


async def download_xmltv_epg(settings, url, output, user_agent=None):
    logger.info("Downloading EPG from url - '%s'", url)
    if not os.path.exists(os.path.dirname(output)):
        os.makedirs(os.path.dirname(output))
    headers = {"User-Agent": _resolve_user_agent(settings, user_agent)}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            async with aiofiles.open(output, 'wb') as f:
                async for chunk in response.content.iter_chunked(8192):
                    await f.write(chunk)
    await try_unzip(output)


async def try_unzip(output: str) -> None:
    def _maybe_unzip(path: str) -> bool:
        temp_path = f"{path}.tmp_unzip"
        try:
            with gzip.open(path, "rb") as src, open(temp_path, "wb") as dst:
                shutil.copyfileobj(src, dst, length=1024 * 1024)
            os.replace(temp_path, path)
            return True
        except Exception:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass
            return False

    loop = asyncio.get_running_loop()
    did_unzip = await loop.run_in_executor(None, _maybe_unzip, output)
    if did_unzip:
        logger.info("Downloaded file is gzipped. Unzipping")


def _derive_timestamp(raw_value):
    if not raw_value:
        return None
    try:
        return str(int(datetime.strptime(raw_value, "%Y%m%d%H%M%S %z").timestamp()))
    except Exception:
        return None


def _clear_epg_channel_data_sync(epg_id):
    try:
        db.session.execute(
            text(
                """
                DELETE FROM epg_channel_programmes AS p
                USING epg_channels AS c
                WHERE p.epg_channel_id = c.id
                  AND c.epg_id = :epg_id
                """
            ),
            {"epg_id": epg_id},
        )
        db.session.execute(delete(EpgChannels).where(EpgChannels.epg_id == epg_id))
        db.session.commit()
    except Exception:
        # Fallback for non-Postgres engines used in local tooling/tests.
        db.session.rollback()
        channel_ids_subquery = select(EpgChannels.id).where(EpgChannels.epg_id == epg_id)
        db.session.execute(
            delete(EpgChannelProgrammes).where(
                EpgChannelProgrammes.epg_channel_id.in_(channel_ids_subquery)
            )
        )
        db.session.execute(delete(EpgChannels).where(EpgChannels.epg_id == epg_id))
        db.session.commit()


def _import_epg_xml_sync(epg_id, xmltv_file, programme_batch_size=5000):
    phase_seconds = {}
    channel_count = 0
    programme_count = 0
    skipped_programmes = 0

    if not os.path.exists(xmltv_file):
        raise FileNotFoundError(f"No such file '{xmltv_file}'")

    logger.info("Importing channels for EPG #%s from path - '%s'", epg_id, xmltv_file)
    t0 = time.perf_counter()
    _clear_epg_channel_data_sync(epg_id)
    phase_seconds["clear_existing"] = time.perf_counter() - t0

    t_parse = time.perf_counter()
    channel_rows = []
    programme_rows = []
    channel_map = None

    def flush_channels():
        nonlocal channel_rows, channel_map
        if channel_rows:
            db.session.execute(insert(EpgChannels), channel_rows)
            db.session.commit()
            channel_rows = []
        channel_map = dict(
            db.session.query(EpgChannels.channel_id, EpgChannels.id)
            .filter(EpgChannels.epg_id == epg_id)
            .all()
        )

    def flush_programmes():
        nonlocal programme_rows
        if programme_rows:
            db.session.execute(insert(EpgChannelProgrammes), programme_rows)
            db.session.commit()
            programme_rows = []

    t_map = 0.0
    t_prog = 0.0
    programme_rows = []
    for _, elem in ET.iterparse(xmltv_file, events=("end",)):
        if elem.tag == "channel":
            channel_id = elem.get("id")
            if channel_id:
                icon_node = elem.find("icon")
                channel_rows.append(
                    {
                        "epg_id": epg_id,
                        "channel_id": channel_id,
                        "name": (elem.findtext("display-name", default="") or "").strip(),
                        "icon_url": (
                            icon_node.attrib.get("src", "") if icon_node is not None else ""
                        ),
                    }
                )
                channel_count += 1
                if channel_map is not None:
                    map_start = time.perf_counter()
                    flush_channels()
                    t_map += time.perf_counter() - map_start
            elem.clear()
            continue

        if elem.tag != "programme":
            continue

        if channel_map is None:
            map_start = time.perf_counter()
            flush_channels()
            t_map += time.perf_counter() - map_start

        external_channel_id = elem.attrib.get("channel")
        epg_channel_id = channel_map.get(external_channel_id)
        if not epg_channel_id:
            skipped_programmes += 1
            elem.clear()
            continue

        start = elem.attrib.get("start")
        stop = elem.attrib.get("stop")
        start_timestamp = elem.attrib.get("start_timestamp") or _derive_timestamp(start)
        stop_timestamp = elem.attrib.get("stop_timestamp") or _derive_timestamp(stop)
        icon = elem.find("icon")
        programme_rows.append(
            {
                "epg_channel_id": epg_channel_id,
                "channel_id": external_channel_id,
                "title": elem.findtext("title", default=None),
                "sub_title": elem.findtext("sub-title", default=None),
                "desc": elem.findtext("desc", default=None),
                "series_desc": elem.findtext("series-desc", default=None),
                "icon_url": icon.attrib.get("src", None) if icon is not None else None,
                "country": elem.findtext("country", default=None),
                "start": start,
                "stop": stop,
                "start_timestamp": start_timestamp,
                "stop_timestamp": stop_timestamp,
                "categories": json.dumps([cat.text for cat in elem.findall("category") if cat.text]),
            }
        )
        programme_count += 1
        if len(programme_rows) >= programme_batch_size:
            prog_start = time.perf_counter()
            flush_programmes()
            t_prog += time.perf_counter() - prog_start
        elem.clear()

    if channel_map is None:
        map_start = time.perf_counter()
        flush_channels()
        t_map += time.perf_counter() - map_start
    if programme_rows:
        prog_start = time.perf_counter()
        flush_programmes()
        t_prog += time.perf_counter() - prog_start

    phase_seconds["parse_xml_stream"] = time.perf_counter() - t_parse
    phase_seconds["load_channel_map"] = t_map
    phase_seconds["insert_programmes"] = t_prog

    return {
        "channels": channel_count,
        "programmes": programme_count,
        "programmes_skipped": skipped_programmes,
        "phase_seconds": phase_seconds,
    }


async def import_epg_data(config, epg_id):
    epg = await read_config_one_epg(epg_id)
    settings = config.read_settings()
    # Download a new local copy of the EPG
    logger.info("Downloading updated XMLTV file for EPG #%s from url - '%s'", epg_id, epg['url'])
    start_time = time.time()
    xmltv_file = os.path.join(config.config_path, 'cache', 'epgs', f"{epg_id}.xml")
    await download_xmltv_epg(settings, epg['url'], xmltv_file, epg.get('user_agent'))
    execution_time = time.time() - start_time
    logger.info("Updated XMLTV file for EPG #%s was downloaded in '%s' seconds", epg_id, int(execution_time))
    # Read and save EPG data to DB (offloaded to worker thread)
    logger.info("Importing updated data for EPG #%s", epg_id)
    start_time = time.perf_counter()
    stats = await run_sync(_import_epg_xml_sync)(epg_id, xmltv_file)
    execution_time = time.perf_counter() - start_time
    logger.info(
        "EPG #%s import stats channels=%s programmes=%s skipped=%s phases=%s",
        epg_id,
        stats["channels"],
        stats["programmes"],
        stats["programmes_skipped"],
        {k: round(v, 2) for k, v in stats["phase_seconds"].items()},
    )
    logger.info("Updated data for EPG #%s was imported in '%s' seconds", epg_id, int(execution_time))


async def import_epg_data_for_all_epgs(config):
    for epg in db.session.query(Epg).all():
        await import_epg_data(config, epg.id)


async def read_channels_from_all_epgs(config):
    epgs_channels = {}
    for result in db.session.query(Epg).options(joinedload(Epg.epg_channels)).all():
        epgs_channels[result.id] = []
        for epg_channel in result.epg_channels:
            epgs_channels[result.id].append({
                "channel_id":   epg_channel.channel_id,
                "display_name": epg_channel.name,
                "icon":         epg_channel.icon_url,
            })
    return epgs_channels


# --- Cache ---
XMLTV_HOST_PLACEHOLDER = "__TIC_HOST__"
async def build_custom_epg_subprocess(config):
    project_root = Path(__file__).resolve().parents[1]
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "backend.scripts.build_custom_epg",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(project_root),
    )

    async def _pipe(stream, level):
        while True:
            line = await stream.readline()
            if not line:
                break
            logger.log(level, "[epg-build] %s", line.decode().rstrip())

    await asyncio.gather(
        _pipe(proc.stdout, logging.INFO),
        _pipe(proc.stderr, logging.INFO),
    )
    rc = await proc.wait()
    if rc != 0:
        raise RuntimeError(f"EPG build subprocess failed with code {rc}")


async def build_custom_epg(config, throttle=False):
    loop = asyncio.get_running_loop()
    logger.info("Generating custom EPG for TVH based on configured channels.")
    total_start = time.perf_counter()
    phase_seconds = {}

    async def maybe_yield():
        if throttle:
            await asyncio.sleep(0.001)

    output_root = ET.Element("tv")
    output_root.set("generator-info-name", "TVH-IPTV-Config")
    output_root.set("source-info-name", "TVH-IPTV-Config - v0.1")

    t0 = time.perf_counter()
    configured_channels = []
    source_key_to_output_channels = defaultdict(list)
    logger.info("   - Loading configured channels and guide mappings.")
    for result in (
        db.session.query(Channel)
        .options(joinedload(Channel.tags))
        .order_by(Channel.number.asc())
        .all()
    ):
        if not result.enabled:
            continue
        channel_id = generate_epg_channel_id(result.number, result.name)
        _, mime_type = await read_base46_image_string(result.logo_base64)
        cache_buster = int(time.time())
        ext = guess_extension(mime_type or "image/png") or ".png"
        logo_url = (
            f"{XMLTV_HOST_PLACEHOLDER}/tic-api/channels/{result.id}/logo/{cache_buster}{ext}"
        )
        configured_channels.append(
            {
                "channel_id": channel_id,
                "display_name": result.name,
                "logo_url": logo_url,
                "tags": [tag.name for tag in result.tags],
                "source_key": (result.guide_id, result.guide_channel_id),
            }
        )
        if result.guide_id and result.guide_channel_id:
            source_key_to_output_channels[(result.guide_id, result.guide_channel_id)].append(channel_id)
        await maybe_yield()
    phase_seconds["load_configured_channels"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    programmes_by_output_channel = defaultdict(list)
    if source_key_to_output_channels:
        guide_ids = {key[0] for key in source_key_to_output_channels}
        guide_channel_ids = {key[1] for key in source_key_to_output_channels}
        logger.info("   - Loading programme rows for mapped guide channels.")
        rows = (
            db.session.query(EpgChannelProgrammes, EpgChannels.epg_id, EpgChannels.channel_id)
            .join(EpgChannels, EpgChannelProgrammes.epg_channel_id == EpgChannels.id)
            .filter(
                EpgChannels.epg_id.in_(guide_ids),
                EpgChannels.channel_id.in_(guide_channel_ids),
            )
            .order_by(EpgChannels.epg_id.asc(), EpgChannels.channel_id.asc(), EpgChannelProgrammes.start.asc())
            .all()
        )
        for programme, guide_id, guide_channel_id in rows:
            output_channels = source_key_to_output_channels.get((guide_id, guide_channel_id))
            if not output_channels:
                continue
            programme_data = {
                "start": programme.start,
                "stop": programme.stop,
                "start_timestamp": programme.start_timestamp,
                "stop_timestamp": programme.stop_timestamp,
                "title": programme.title,
                "sub-title": programme.sub_title,
                "desc": programme.desc,
                "series-desc": programme.series_desc,
                "country": programme.country,
                "icon_url": programme.icon_url,
                "categories": json.loads(programme.categories or "[]"),
                "summary": programme.summary,
                "keywords": programme.keywords,
                "credits_json": programme.credits_json,
                "video_colour": programme.video_colour,
                "video_aspect": programme.video_aspect,
                "video_quality": programme.video_quality,
                "subtitles_type": programme.subtitles_type,
                "audio_described": programme.audio_described,
                "previously_shown_date": programme.previously_shown_date,
                "premiere": programme.premiere,
                "is_new": programme.is_new,
                "epnum_onscreen": programme.epnum_onscreen,
                "epnum_xmltv_ns": programme.epnum_xmltv_ns,
                "epnum_dd_progid": programme.epnum_dd_progid,
                "star_rating": programme.star_rating,
                "production_year": programme.production_year,
                "rating_system": programme.rating_system,
                "rating_value": programme.rating_value,
            }
            for output_channel_id in output_channels:
                programmes_by_output_channel[output_channel_id].append(programme_data)
    phase_seconds["load_programmes"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    logger.info("   - Generating XML channel info.")
    for channel_info in configured_channels:
        channel = ET.SubElement(output_root, "channel")
        channel.set("id", str(channel_info["channel_id"]))
        display_name = ET.SubElement(channel, "display-name")
        display_name.text = channel_info["display_name"].strip()
        icon = ET.SubElement(channel, "icon")
        icon.set("src", channel_info["logo_url"])
        live = ET.SubElement(channel, "live")
        live.text = "true"
        active = ET.SubElement(channel, "active")
        active.text = "true"
        await maybe_yield()

    logger.info("   - Generating XML channel programme data.")
    for channel_info in configured_channels:
        channel_id = channel_info["channel_id"]
        channel_tags = channel_info["tags"]
        for epg_channel_programme in programmes_by_output_channel.get(channel_id, []):
            # Create a <programme> element for the output file and copy the attributes from the input programme
            output_programme = ET.SubElement(output_root, 'programme')
            # Build programmes from DB data (manually create attributes etc.
            if epg_channel_programme['start']:
                output_programme.set('start', epg_channel_programme['start'])
            if epg_channel_programme['stop']:
                output_programme.set('stop', epg_channel_programme['stop'])
            if epg_channel_programme['start_timestamp']:
                output_programme.set('start_timestamp', epg_channel_programme['start_timestamp'])
            if epg_channel_programme['stop_timestamp']:
                output_programme.set('stop_timestamp', epg_channel_programme['stop_timestamp'])
            # Set the "channel" ident here
            output_programme.set('channel', str(channel_id))
            # Loop through all child elements of the input programme and copy them to the output programme
            for child in ['title', 'sub-title', 'desc', 'series-desc', 'country']:
                # Copy all other child elements to the output programme if they exist
                if child in epg_channel_programme and epg_channel_programme[child] is not None:
                    output_child = ET.SubElement(output_programme, child)
                    output_child.text = epg_channel_programme[child]
                    output_child.set('lang', 'en')
            # Optional summary
            if epg_channel_programme.get('summary'):
                c = ET.SubElement(output_programme, 'summary')
                c.text = epg_channel_programme['summary']
                c.set('lang', 'en')
            # If we have a programme icon, add it
            if epg_channel_programme['icon_url']:
                output_child = ET.SubElement(output_programme, 'icon')
                output_child.set('src', epg_channel_programme['icon_url'])
                output_child.set('height', "")
                output_child.set('width', "")
            # Keywords
            if epg_channel_programme.get('keywords'):
                try:
                    for kw in json.loads(epg_channel_programme['keywords']):
                        if kw:
                            kc = ET.SubElement(output_programme, 'keyword')
                            kc.text = kw
                            kc.set('lang', 'en')
                except Exception:
                    pass
            # Credits
            if epg_channel_programme.get('credits_json'):
                try:
                    credits_data = json.loads(epg_channel_programme['credits_json'])
                    if isinstance(credits_data, dict) and credits_data:
                        credits_el = ET.SubElement(output_programme, 'credits')
                        for role, people in credits_data.items():
                            if not people:
                                continue
                            for person in people:
                                pe = ET.SubElement(credits_el, role)
                                pe.text = person
                except Exception:
                    pass
            # Video
            if any(epg_channel_programme.get(k) for k in ['video_colour','video_aspect','video_quality']):
                video_el = ET.SubElement(output_programme, 'video')
                if epg_channel_programme.get('video_colour'):
                    c = ET.SubElement(video_el, 'colour'); c.text = epg_channel_programme['video_colour']
                if epg_channel_programme.get('video_aspect'):
                    a = ET.SubElement(video_el, 'aspect'); a.text = epg_channel_programme['video_aspect']
                if epg_channel_programme.get('video_quality'):
                    q = ET.SubElement(video_el, 'quality'); q.text = epg_channel_programme['video_quality']
            # Subtitles
            if epg_channel_programme.get('subtitles_type'):
                subs = ET.SubElement(output_programme, 'subtitles')
                subs.set('type', epg_channel_programme['subtitles_type'])
            # Audio described
            if epg_channel_programme.get('audio_described'):
                ET.SubElement(output_programme, 'audio-described')
            # Previously shown
            if epg_channel_programme.get('previously_shown_date'):
                ps = ET.SubElement(output_programme, 'previously-shown')
                ps.set('start', epg_channel_programme['previously_shown_date'])
            # Premiere / New
            if epg_channel_programme.get('premiere'):
                ET.SubElement(output_programme, 'premiere')
            if epg_channel_programme.get('is_new'):
                ET.SubElement(output_programme, 'new')
            # Episode numbers
            if epg_channel_programme.get('epnum_onscreen'):
                e1 = ET.SubElement(output_programme, 'episode-num'); e1.set('system', 'onscreen'); e1.text = epg_channel_programme['epnum_onscreen']
            if epg_channel_programme.get('epnum_xmltv_ns'):
                e2 = ET.SubElement(output_programme, 'episode-num'); e2.set('system', 'xmltv_ns'); e2.text = epg_channel_programme['epnum_xmltv_ns']
            if epg_channel_programme.get('epnum_dd_progid'):
                e3 = ET.SubElement(output_programme, 'episode-num'); e3.set('system', 'dd_progid'); e3.text = epg_channel_programme['epnum_dd_progid']
            # Star rating
            if epg_channel_programme.get('star_rating'):
                sr = ET.SubElement(output_programme, 'star-rating')
                val = ET.SubElement(sr, 'value'); val.text = epg_channel_programme['star_rating']
            # Production year
            if epg_channel_programme.get('production_year'):
                d = ET.SubElement(output_programme, 'date'); d.text = epg_channel_programme['production_year']
            # Rating system
            if epg_channel_programme.get('rating_value'):
                rating_el = ET.SubElement(output_programme, 'rating')
                if epg_channel_programme.get('rating_system'):
                    rating_el.set('system', epg_channel_programme['rating_system'])
                rv = ET.SubElement(rating_el, 'value'); rv.text = epg_channel_programme['rating_value']
            # Loop through all categories for this programme and add them as "category" child elements
            if epg_channel_programme['categories']:
                for category in epg_channel_programme['categories']:
                    output_child = ET.SubElement(output_programme, 'category')
                    output_child.text = category
                    output_child.set('lang', 'en')
            # Loop through all tags for this channel and add them as "category" child elements
            for tag in channel_tags:
                output_child = ET.SubElement(output_programme, 'category')
                output_child.text = tag
                output_child.set('lang', 'en')
        await maybe_yield()
    phase_seconds["generate_xml_tree"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    logger.info("   - Writing out XMLTV file.")
    output_tree = ET.ElementTree(output_root)
    custom_epg_file = os.path.join(config.config_path, "epg.xml")
    await loop.run_in_executor(
        None, lambda: output_tree.write(custom_epg_file, encoding="UTF-8", xml_declaration=True)
    )
    phase_seconds["write_xml_file"] = time.perf_counter() - t0
    execution_time = time.perf_counter() - total_start
    logger.info(
        "The custom XMLTV EPG file for TVH was generated in '%s' seconds (phases=%s)",
        int(execution_time),
        {k: round(v, 2) for k, v in phase_seconds.items()},
    )


# --- Online Metadata ---
async def search_tmdb_for_movie(api_key, title, cache, lock, semaphore):
    async with semaphore:
        async with lock:
            if 'tmdb' not in cache:
                cache['tmdb'] = {}
            if title in cache['tmdb']:
                logger.debug("       - Fetching data for program '%s' from TMDB. [CACHED]", title)
                return cache['tmdb'][title]
        search_url = f'https://api.themoviedb.org/3/search/movie?api_key={api_key}&query={title}'
        async with aiohttp.ClientSession() as session:
            async with session.get(search_url) as response:
                if response.status == 200:
                    results = (await response.json()).get('results', [])
                    if results:
                        async with lock:
                            cache['tmdb'][title] = results[0]  # Cache the first search result
                        logger.debug("       - Fetching data for program '%s' from TMDB. [FETCHED]", title)
                        return results[0]
        async with lock:
            cache['tmdb'][title] = None  # Cache None if no results found
        logger.debug("       - Fetching data for program '%s' from TMDB. [NONE]", title)
        return None


async def search_google_images(title, cache, lock, semaphore):
    async with semaphore:
        async with lock:
            if 'google_images' not in cache:
                cache['google_images'] = {}
            if title in cache['google_images']:
                logger.debug("       - Fetching data for program '%s' from Google Images. [CACHED]", title)
                return cache['google_images'][title]

        search_query = f'"{title}" television show'
        encoded_query = quote(search_query)
        search_url = f'https://www.google.com/search?tbm=isch&safe=active&tbs=isz:m&q={encoded_query}'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(search_url, headers=headers) as response:
                if response.status == 200:
                    soup = BeautifulSoup(await response.text(), 'html.parser')
                    images = soup.find_all('img')
                    if images:
                        # The first image might be the Google logo, so we take the second one
                        image_url = images[1]['src']
                        async with lock:
                            cache['google_images'][title] = image_url  # Cache the first image URL
                        logger.debug("       - Fetching data for program '%s' from Google Images. [FETCHED]", title)
                        return image_url

        async with lock:
            cache['google_images'][title] = None  # Cache None if no results found
        logger.debug("       - Fetching data for program '%s' from Google Images. [NONE]", title)
        return None


async def update_programme_with_online_data(settings, programme, categories, cache, lock, semaphore):
    updated = False
    title = programme.title
    categories = [category.lower() for category in categories]

    # Fetch updated data from TMDB
    if not (programme.sub_title or programme.desc or programme.icon_url):
        if settings['settings'].get('epgs', {}).get('enable_tmdb_metadata'):
            api_key = settings['settings'].get('epgs', {}).get('tmdb_api_key', '')
            tmdb_data = await search_tmdb_for_movie(api_key, title, cache, lock, semaphore)
            if tmdb_data:
                # Update programme with fetched data if fields are missing
                if not programme.sub_title:
                    programme.sub_title = tmdb_data.get('title')
                if not programme.desc:
                    programme.desc = tmdb_data.get('overview')
                if not programme.icon_url:
                    programme.icon_url = f"https://image.tmdb.org/t/p/w500{tmdb_data.get('poster_path')}"

    # Fetch icon_url from Google Images if still missing
    if not programme.icon_url:
        if settings['settings'].get('epgs', {}).get('enable_google_image_search_metadata'):
            image_url = await search_google_images(title, cache, lock, semaphore)
            if image_url:
                programme.icon_url = image_url

    return programme


async def update_programmes_concurrently(settings, programmes, cache, lock):
    semaphore = asyncio.Semaphore(10)

    async def update_wrapper(programme):
        categories = json.loads(programme.categories)
        return await update_programme_with_online_data(settings, programme, categories, cache, lock, semaphore)

    tasks = [update_wrapper(programme) for programme in programmes]
    updated_programmes = await asyncio.gather(*tasks, return_exceptions=True)

    for i, result in enumerate(updated_programmes):
        if isinstance(result, Exception):
            logger.error(f"Error updating programme: {result}")
        else:
            programmes[i] = result

    return programmes


async def update_channel_epg_with_online_data(config):
    settings = config.read_settings()
    update_with_online_data = False
    if settings['settings'].get('epgs', {}).get('enable_tmdb_metadata'):
        update_with_online_data = True
    if settings['settings'].get('epgs', {}).get('enable_google_image_search_metadata'):
        update_with_online_data = True
    if not update_with_online_data:
        return
    start_time = time.time()
    cache = {}
    lock = asyncio.Lock()
    logger.info("Update EPG with missing data from online sources for each configured channel.")
    for result in db.session.query(Channel).order_by(Channel.number.asc()).all():
        if result.enabled:
            channel_id = generate_epg_channel_id(result.number, result.name)
            db_programmes_query = db.session.query(EpgChannelProgrammes) \
                .options(joinedload(EpgChannelProgrammes.channel)) \
                .filter(and_(EpgChannelProgrammes.channel.has(epg_id=result.guide_id),
                             EpgChannelProgrammes.channel.has(channel_id=result.guide_channel_id)
                             )) \
                .order_by(EpgChannelProgrammes.channel_id.asc(), EpgChannelProgrammes.start.asc())
            db_programmes = db_programmes_query.all()
            logger.info("   - Updating programme list for %s - %s.", channel_id, result.name)
            programmes = await update_programmes_concurrently(settings, db_programmes, cache, lock)
            # Save all new
            db.session.bulk_save_objects(programmes)
            # Commit all updates to channel programmes
            db.session.commit()
    execution_time = time.time() - start_time
    logger.info("Updating online EPG data for configured channels took '%s' seconds", int(execution_time))


# --- TVH Functions ---
async def run_tvh_epg_grabbers(config):
    # Trigger a re-grab of the EPG in TVH
    async with await get_tvh(config) as tvh:
        await tvh.run_internal_epg_grabber()
