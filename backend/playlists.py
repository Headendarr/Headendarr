#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import logging
import os

import aiofiles
import aiohttp
import time
from operator import attrgetter
from sqlalchemy import or_, select, delete, insert, func
from sqlalchemy.orm import joinedload

from backend.ffmpeg import ffprobe_file
from backend.models import db, Session, Playlist, PlaylistStreams
from backend.tvheadend.tvh_requests import get_tvh, network_template

logger = logging.getLogger('tic.playlists')


async def read_config_all_playlists(config, output_for_export=False):
    return_list = []
    settings = config.read_settings()
    app_url = settings['settings']['app_url']
    async with Session() as session:
        async with session.begin():
            query = await session.execute(select(Playlist))
            results = query.scalars().all()
            for result in results:
                if output_for_export:
                    return_list.append({
                        'enabled':              result.enabled,
                        'connections':          result.connections,
                        'name':                 result.name,
                        'url':                  result.url,
                        'use_hls_proxy':        result.use_hls_proxy,
                        'use_custom_hls_proxy': result.use_custom_hls_proxy,
                        'hls_proxy_path':       result.hls_proxy_path if result.hls_proxy_path else f'{app_url}/tic-hls-proxy/[B64_URL].m3u8',
                    })
                    continue
                return_list.append({
                    'id':                   result.id,
                    'enabled':              result.enabled,
                    'connections':          result.connections,
                    'name':                 result.name,
                    'url':                  result.url,
                    'use_hls_proxy':        result.use_hls_proxy,
                    'use_custom_hls_proxy': result.use_custom_hls_proxy,
                    'hls_proxy_path':       result.hls_proxy_path if result.hls_proxy_path else f'{app_url}/tic-hls-proxy/[B64_URL].m3u8',
                })
    return return_list


async def read_config_one_playlist(config, playlist_id):
    settings = config.read_settings()
    return_item = {}
    async with Session() as session:
        async with session.begin():
            query = await session.execute(select(Playlist).filter(Playlist.id == playlist_id))
            result = query.scalar_one()

            app_url = settings['settings']['app_url']
            if result:
                return_item = {
                    'id':                   result.id,
                    'enabled':              result.enabled,
                    'name':                 result.name,
                    'url':                  result.url,
                    'connections':          result.connections,
                    'use_hls_proxy':        result.use_hls_proxy,
                    'use_custom_hls_proxy': result.use_custom_hls_proxy,
                    'hls_proxy_path':       result.hls_proxy_path if result.hls_proxy_path else f'{app_url}/tic-hls-proxy/[B64_URL].m3u8',
                }
    return return_item


async def add_new_playlist(config, data):
    settings = config.read_settings()
    app_url = settings['settings']['app_url']
    async with Session() as session:
        async with session.begin():
            playlist = Playlist(
                enabled=data.get('enabled'),
                name=data.get('name'),
                url=data.get('url'),
                connections=data.get('connections'),
                use_hls_proxy=data.get('use_hls_proxy', False),
                use_custom_hls_proxy=data.get('use_custom_hls_proxy', False),
                hls_proxy_path=data.get('hls_proxy_path', f'{app_url}/tic-hls-proxy/[B64_URL].m3u8'),
            )
            # This is a new entry. Add it to the session before commit
            session.add(playlist)
    return playlist.id


async def update_playlist(config, playlist_id, data):
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(Playlist).where(Playlist.id == playlist_id))
            playlist = result.scalar_one()
            playlist.enabled = data.get('enabled', playlist.enabled)
            playlist.name = data.get('name', playlist.name)
            playlist.url = data.get('url', playlist.url)
            playlist.connections = data.get('connections', playlist.connections)
            playlist.use_hls_proxy = data.get('use_hls_proxy', playlist.use_hls_proxy)
            playlist.use_custom_hls_proxy = data.get('use_custom_hls_proxy', playlist.use_custom_hls_proxy)
            playlist.hls_proxy_path = data.get('hls_proxy_path', playlist.hls_proxy_path)


async def delete_playlist(config, playlist_id):
    net_uuid = None
    async with Session() as session:
        async with session.begin():
            result = await session.execute(select(Playlist).where(Playlist.id == playlist_id))
            playlist = result.scalar_one()
            net_uuid = playlist.tvh_uuid
            # Remove cached copy of playlist
            cache_files = [
                os.path.join(config.config_path, 'cache', 'playlists', f"{playlist_id}.m3u"),
                os.path.join(config.config_path, 'cache', 'playlists', f"{playlist_id}.yml"),
            ]
            for f in cache_files:
                if os.path.isfile(f):
                    os.remove(f)
            # Remove from DB
            await session.delete(playlist)
    return net_uuid


async def download_playlist_file(url, output):
    logger.info("Downloading Playlist from url - '%s'", url)
    if not os.path.exists(os.path.dirname(output)):
        os.makedirs(os.path.dirname(output))
    headers = {
        'User-Agent': 'VLC/3.0.0-git LibVLC/3.0.0-gi',
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            async with aiofiles.open(output, 'wb') as f:
                async for chunk in response.content.iter_chunked(8192):
                    await f.write(chunk)
    return output


async def store_playlist_streams(config, playlist_id):
    m3u_file = os.path.join(config.config_path, 'cache', 'playlists', f"{playlist_id}.m3u")
    if not os.path.exists(m3u_file):
        logger.error("No such file '%s'", m3u_file)
        return False
    # Read cache file contents asynchronously
    async with aiofiles.open(m3u_file, mode='r', encoding="utf8", errors='ignore') as f:
        contents = await f.read()
    # noinspection PyPackageRequirements
    from ipytv import playlist
    pl = playlist.loads(contents)
    async with Session() as session:
        async with session.begin():
            # Delete all existing playlist streams
            stmt = delete(PlaylistStreams).where(PlaylistStreams.playlist_id == playlist_id)
            await session.execute(stmt)
            # Add an updated list of streams from the M3U file to the DB
            logger.info("Updating list of available streams for playlist #%s from path - '%s'", playlist_id, m3u_file)
            items = []
            for stream in pl:
                tvg_channel_number = stream.attributes.get('tvg-chno')
                items.append({
                    'playlist_id': playlist_id,
                    'name':        stream.name,
                    'url':         stream.url,
                    'channel_id':  stream.attributes.get('channel-id'),
                    'group_title': stream.attributes.get('group-title'),
                    'tvg_chno':    int(tvg_channel_number) if tvg_channel_number is not None else None,
                    'tvg_id':      stream.attributes.get('tvg-id'),
                    'tvg_logo':    stream.attributes.get('tvg-logo'),
                })
            # Perform bulk insert
            await session.execute(insert(PlaylistStreams), items)
            # Commit all updates to playlist sources
            await session.commit()
            logger.info("Successfully imported %s streams from path - '%s'", len(items), m3u_file)


def fetch_playlist_streams(playlist_id):
    return_list = {}
    for result in db.session.query(PlaylistStreams).where(PlaylistStreams.playlist_id == playlist_id).all():
        return_list[result.name] = {
            'name':        result.name,
            'url':         result.url,
            'channel_id':  result.channel_id,
            'group_title': result.group_title,
            'tvg_chno':    result.tvg_chno,
            'tvg_id':      result.tvg_id,
            'tvg_logo':    result.tvg_logo,
        }
    return return_list


async def import_playlist_data(config, playlist_id):
    playlist = await read_config_one_playlist(config, playlist_id)
    # Download playlist data and save to YAML cache file
    logger.info("Downloading updated M3U file for playlist #%s from url - '%s'", playlist_id, playlist['url'])
    start_time = time.time()
    m3u_file = os.path.join(config.config_path, 'cache', 'playlists', f"{playlist_id}.m3u")
    await download_playlist_file(playlist['url'], m3u_file)
    execution_time = time.time() - start_time
    logger.info("Updated M3U file for playlist #%s was downloaded in '%s' seconds", playlist_id, int(execution_time))
    # Parse the M3U file and cache the data in a YAML file for faster parsing
    logger.info("Importing updated data for playlist #%s", playlist_id)
    start_time = time.time()
    await store_playlist_streams(config, playlist_id)
    execution_time = time.time() - start_time
    logger.info("Updated data for playlist #%s was imported in '%s' seconds", playlist_id, int(execution_time))
    # Publish changes to TVH
    await publish_playlist_networks(config)


async def import_playlist_data_for_all_playlists(config):
    for playlist in db.session.query(Playlist).all():
        await import_playlist_data(config, playlist.id)


async def read_stream_details_from_all_playlists():
    playlist_streams = {
        'streams': []
    }
    for result in db.session.query(PlaylistStreams).all():
        playlist_streams['streams'].append({
            'id':            result.id,
            'playlist_id':   result.playlist_id,
            'playlist_name': result.playlist.name,
            'name':          result.name,
            'url':           result.url,
            'channel_id':    result.channel_id,
            'group_title':   result.group_title,
            'tvg_chno':      result.tvg_chno,
            'tvg_id':        result.tvg_id,
            'tvg_logo':      result.tvg_logo,
        })
    return playlist_streams


def read_filtered_stream_details_from_all_playlists(request_json):
    results = {
        'streams':          [],
        'records_total':    0,
        'records_filtered': 0,
    }
    query = db.session.query(PlaylistStreams)
    # Get total records count
    results['records_total'] = query.count()
    # Filter results by search value
    search_value = request_json.get('search_value')
    if search_value:
        playlist_rows = (
            db.session.query(Playlist)
            .where(Playlist.name.contains(search_value))
            .all()
        )
        query = query.options(joinedload(PlaylistStreams.playlist)).where(
            or_(PlaylistStreams.name.contains(search_value),
                PlaylistStreams.playlist_id.in_([p.id for p in playlist_rows])))
    # Record filtered records count
    results['records_filtered'] = query.count()
    # Get order by
    order_by_column = request_json.get('order_by')
    if not order_by_column:
        order_by_column = 'name'
    if request_json.get('order_direction', 'desc') == 'asc':
        order_by = attrgetter(order_by_column)(PlaylistStreams).asc()
    else:
        order_by = attrgetter(order_by_column)(PlaylistStreams).desc()
    query = query.order_by(order_by)
    # Limit query results
    length = request_json.get('length', 0)
    start = request_json.get('start', 0)
    if length:
        query = query.limit(length).offset(start)
    # Fetch filtered results
    for result in query.all():
        results['streams'].append({
            'id':            result.id,
            'playlist_id':   result.playlist_id,
            'playlist_name': result.playlist.name,
            'name':          result.name,
            'url':           result.url,
            'channel_id':    result.channel_id,
            'group_title':   result.group_title,
            'tvg_chno':      result.tvg_chno,
            'tvg_id':        result.tvg_id,
            'tvg_logo':      result.tvg_logo,
        })
    return results


async def delete_playlist_network_in_tvh(config, net_uuid):
    async with await get_tvh(config) as tvh:
        await tvh.delete_network(net_uuid)


async def publish_playlist_networks(config):
    logger.info("Publishing all playlist networks to TVH")
    async with await get_tvh(config) as tvh:
        # Loop over configured playlists
        existing_uuids = []
        net_priority = 0
        for result in db.session.query(Playlist).all():
            net_priority += 1
            net_uuid = result.tvh_uuid
            playlist_name = result.name
            max_streams = result.connections
            network_name = f"playlist_{result.id}_{result.name}"
            logger.info("Publishing playlist to TVH - %s.", network_name)
            if net_uuid:
                found = False
                for net in await tvh.list_cur_networks():
                    if net.get('uuid') == net_uuid:
                        found = True
                if not found:
                    net_uuid = None
            if not net_uuid:
                # No network exists, create one
                # Check if network exists with this playlist name
                net_uuid = await tvh.create_network(playlist_name, network_name, max_streams, net_priority)
            # Update network
            net_conf = network_template.copy()
            net_conf['uuid'] = net_uuid
            net_conf['enabled'] = result.enabled
            net_conf['networkname'] = playlist_name
            net_conf['pnetworkname'] = network_name
            net_conf['max_streams'] = max_streams
            net_conf['priority'] = net_priority
            await tvh.idnode_save(net_conf)
            # Save network UUID against playlist in settings
            result.tvh_uuid = net_uuid
            db.session.commit()
            # Append to list of current network UUIDs
            existing_uuids.append(net_uuid)

        #  TODO: Remove any networks that are not managed. DONT DO THIS UNTIL THINGS ARE ALL WORKING!


async def probe_playlist_stream(playlist_stream_id):
    playlist_stream = db.session.query(PlaylistStreams).where(PlaylistStreams.id == playlist_stream_id).one()
    probe_data = await ffprobe_file(playlist_stream.url)
    return probe_data


async def get_playlist_groups(config, playlist_id, start=0, length=10, search_value='', order_by='name', order_direction='asc'):
    """
    Get all groups from a specific playlist with filtering, sorting and pagination
    """
    async with Session() as session:
        async with session.begin():
            # Get distinct group count query (this is what needs to be fixed)
            distinct_groups_count_query = select(func.count()).select_from(
                select(PlaylistStreams.group_title)
                .filter(
                    PlaylistStreams.playlist_id == playlist_id,
                    PlaylistStreams.group_title != None,
                    PlaylistStreams.group_title != ''
                )
                .group_by(PlaylistStreams.group_title)
                .subquery()
            )
            
            # Get the total count of unique groups
            total_groups = await session.scalar(distinct_groups_count_query)
            
            # Apply search filter to count if provided
            if search_value:
                filtered_groups_count_query = select(func.count()).select_from(
                    select(PlaylistStreams.group_title)
                    .filter(
                        PlaylistStreams.playlist_id == playlist_id,
                        PlaylistStreams.group_title != None,
                        PlaylistStreams.group_title != '',
                        PlaylistStreams.group_title.ilike(f'%{search_value}%')
                    )
                    .group_by(PlaylistStreams.group_title)
                    .subquery()
                )
                filtered_groups = await session.scalar(filtered_groups_count_query)
            else:
                filtered_groups = total_groups
            
            # Get distinct group names and count channels in each group
            groups_query = select(
                PlaylistStreams.group_title.label('name'),
                func.count(PlaylistStreams.id).label('channel_count')
            ).filter(
                PlaylistStreams.playlist_id == playlist_id,
                PlaylistStreams.group_title != None,  # Exclude streams without group
                PlaylistStreams.group_title != ''     # Exclude streams with empty group
            )
            
            # Apply search filter to groups
            if search_value:
                groups_query = groups_query.filter(PlaylistStreams.group_title.ilike(f'%{search_value}%'))
            
            # Group by group name
            groups_query = groups_query.group_by(PlaylistStreams.group_title)
            
            # Apply ordering
            if order_by == 'name':
                if order_direction.lower() == 'desc':
                    groups_query = groups_query.order_by(PlaylistStreams.group_title.desc())
                else:
                    groups_query = groups_query.order_by(PlaylistStreams.group_title.asc())
            elif order_by == 'channel_count':
                if order_direction.lower() == 'desc':
                    groups_query = groups_query.order_by(func.count(PlaylistStreams.id).desc())
                else:
                    groups_query = groups_query.order_by(func.count(PlaylistStreams.id).asc())
            
            # Apply pagination
            groups_query = groups_query.offset(start).limit(length)
            
            # Execute query
            result = await session.execute(groups_query)
            groups = result.all()
            
            # Format result
            group_list = []
            for group in groups:
                group_list.append({
                    'name': group.name or 'Unknown',
                    'channel_count': group.channel_count
                })
            
            return {
                'groups': group_list,
                'total': total_groups,
                'records_filtered': filtered_groups
            }
