---
title: Connectivity Endpoints
---

# Connectivity Endpoints

This page lists all client-facing connectivity endpoints and how `profile` and routing settings affect them.

## Core Endpoints

- **XMLTV EPG**: `/tic-api/epg/xmltv.xml`
- **Combined playlist**: `/tic-api/playlist/combined.m3u`
- **Per-source playlist**: `/tic-api/playlist/<source_id>.m3u`
- **Per-source playlist (compat alias)**: `/tic-api/tvh_playlist/<source_id>/channels.m3u`
- **CSO channel playback**: `/<hls_proxy_prefix>/channel/<channel_id>` (default: `/tic-hls-proxy/channel/<channel_id>`)

All stream/playlist endpoints support `stream_key` auth and accept optional `profile=<profile_id>`.

## HDHomeRun Endpoints

Per-source HDHR endpoints are under:

- `/tic-api/hdhr_device/<stream_key>/<source_id>/discover.json`
- `/tic-api/hdhr_device/<stream_key>/<source_id>/lineup.json`
- `/tic-api/hdhr_device/<stream_key>/<source_id>/lineup_status.json`
- `/tic-api/hdhr_device/<stream_key>/<source_id>/lineup.post`
- `/tic-api/hdhr_device/<stream_key>/<source_id>/device.xml`

Combined HDHR endpoints are under:

- `/tic-api/hdhr_device/<stream_key>/combined/discover.json`
- `/tic-api/hdhr_device/<stream_key>/combined/lineup.json`
- `/tic-api/hdhr_device/<stream_key>/combined/lineup_status.json`
- `/tic-api/hdhr_device/<stream_key>/combined/lineup.post`
- `/tic-api/hdhr_device/<stream_key>/combined/device.xml`

Combined HDHR also supports profile-scoped paths:

- `/tic-api/hdhr_device/<stream_key>/combined/<profile>/discover.json`
- `/tic-api/hdhr_device/<stream_key>/combined/<profile>/lineup.json`
- `/tic-api/hdhr_device/<stream_key>/combined/<profile>/lineup_status.json`
- `/tic-api/hdhr_device/<stream_key>/combined/<profile>/lineup.post`
- `/tic-api/hdhr_device/<stream_key>/combined/<profile>/device.xml`

`lineup.json` supports `profile=<profile_id>` and emits channel URLs using the selected profile rules.
For profile-scoped combined paths, `<profile>` is used as the effective profile for HDHR discovery/lineup flow.

## Xtream Codes Compatibility Endpoints

- `/player_api.php`
- `/panel_api.php`
- `/get.php`
- `/xmltv.php`
- `/live/<username>/<password>/<stream_id>`

Notes:

- `/get.php` serves the same playlist content model as `/tic-api/playlist/combined.m3u`.
- `/xmltv.php` serves the same XMLTV content model as `/tic-api/epg/xmltv.xml`.
- `/get.php` and `/xmltv.php` require XC credentials via query params: `?username=<username>&password=<stream_key>`.
- XC output format is TS-only (`allowed_output_formats=["ts"]`).
- XC profile selection is not client-driven. TIC chooses channel profile using channel CSO configuration:
  - Allowed for XC: `default`, `mpegts`, `h264-aac-mpegts`
  - Any other configured channel profile falls back to `default`

## Profile Behavior

Use `profile=<profile_id>` to request output behavior.

Always available:

- `default`: CSO auto/remux behavior (MPEG-TS target), TVH uses `pass`.
- `tvh`: TVH-only profile (only honored for TVH backend stream clients).

Configurable profiles (Application Settings -> Connections):

- `mpegts`
- `matroska`
- `h264-aac-mpegts`
- `h264-aac-matroska`
- `h264-aac-mp4`
- `vp8-vorbis-webm`
- `h265-aac-mp4`
- `h265-aac-matroska`
- `h265-ac3-mp4`
- `h265-ac3-matroska`

If a requested profile is unsupported/disabled, Headendarr falls back to `default`.
If a requested profile is disabled in **Stream Profiles**, Headendarr falls back to `default`.

## Route Playlists & HDHomeRun Through TVHeadend (Per-Source Only)

When enabled:

- Per-source playlist and per-source HDHR channel URLs point to `/tic-api/tvh_stream/stream/channel/<tvh_uuid>`.
- Headendarr maps requested `profile` values to TVHeadend profile IDs where supported.
- For unsupported TVH mappings, Headendarr falls back to TVH `pass`.

When disabled:

- Per-source playlist/HDHR endpoints can emit CSO channel URLs when CSO is forced on a channel or when a non-default profile is requested.
- Otherwise, they emit direct/proxy source URLs based on source configuration.

## Route Combined Playlists, XC, & HDHomeRun Through CSO (Combined Only)

When enabled:

- Combined playlist (`/tic-api/playlist/combined.m3u`) channel URLs are forced through CSO.
- XC combined routes (`/get.php`, XC stream routes) are forced through CSO.
- Combined HDHR lineup URLs are forced through CSO.

When disabled:

- Combined endpoints still use CSO when a channel explicitly has CSO enabled or when a non-default profile is requested.
- Otherwise, combined endpoints resolve to direct/proxy source URLs.

This setting is independent from per-source TVHeadend routing. Both toggles can be enabled at the same time because they target different endpoint families.

## Channel Stream Organiser (CSO)

Channel setting:

- **Enable Channel Stream Organiser**: force this channel to use CSO pipeline routing.
- **Preferred Stream Profile**: default profile used for generated channel CSO URLs.

Client requests can still override with `profile=<profile_id>` per request.
