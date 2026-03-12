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
- **CSO channel playback**: `/tic-api/cso/channel/<channel_id>`
- **CSO channel stream gate**: `/tic-api/cso/channel_stream/<stream_id>`

Connectivity endpoints support `stream_key` authentication. Profile overrides are supported where noted below.

CSO endpoint notes:

- `/tic-api/cso/channel/<channel_id>` supports `profile=<profile_id>`.
- `/tic-api/cso/channel_stream/<stream_id>` supports `profile=<profile_id>`.
- For HLS profiles, CSO exposes playlist + segment paths:
  - `/tic-api/cso/channel/<channel_id>/hls/<connection_id>/index.m3u8`
  - `/tic-api/cso/channel/<channel_id>/hls/<connection_id>/<segment_name>`
  - `/tic-api/cso/channel_stream/<stream_id>/hls/<connection_id>/index.m3u8`
  - `/tic-api/cso/channel_stream/<stream_id>/hls/<connection_id>/<segment_name>`
- Without `profile`, stream config controls whether the endpoint redirects or uses CSO buffering.

## HLS Proxy Endpoints

All HLS proxy endpoints are scoped by instance:

- `/<hls_proxy_prefix>/<instance_id>/<encoded_url>.m3u8`
- `/<hls_proxy_prefix>/<instance_id>/proxy.m3u8?url=<upstream_url>`
- `/<hls_proxy_prefix>/<instance_id>/<encoded_url>.key`
- `/<hls_proxy_prefix>/<instance_id>/<encoded_url>.ts`
- `/<hls_proxy_prefix>/<instance_id>/<encoded_url>.vtt`
- `/<hls_proxy_prefix>/<instance_id>/stream/<encoded_url>`

Default prefix is `/tic-hls-proxy`.

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
- `hls`
- `aac-hls`
- `h264-aac-hls`
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

- Per-source playlist/HDHR endpoints emit channel stream CSO gate URLs:
  - `/tic-api/cso/channel_stream/<stream_id>`
- The stream gate enforces source connection limits before redirecting.
- For external/non-monitorable targets (for example external proxy/upstream URLs), the source gate redirects directly.

Why you would enable this:

- You use TVHeadend clients and want TVHeadend to be the primary stream consumer.
- You prefer per-source playlist/HDHR traffic to be mediated by TVHeadend behaviour/policies.

## Route Combined Playlists, XC, & HDHomeRun Through CSO (Combined Only)

When enabled:

- Combined playlist (`/tic-api/playlist/combined.m3u`) channel URLs are resolved through per-source gate URLs.
- XC combined routes (`/get.php`, XC stream routes) are resolved through per-source gate URLs.
- Combined HDHR lineup URLs are resolved through per-source gate URLs.

When disabled:

- Combined endpoints still resolve through per-source gate URLs.
- Per-channel CSO profile behaviour applies to direct CSO channel requests (`/tic-api/cso/channel/<channel_id>`).

This setting is independent from per-source TVHeadend routing. Both toggles can be enabled at the same time because they target different endpoint families.

Why you would enable this:

- You want combined outputs to use CSO routing semantics.
- You want a single combined entrypoint for non-TVHeadend clients (for example Plex/Emby/Jellyfin playlist or HDHomeRun-style setups) while keeping CSO behaviour.

## Route All TVHeadend Mux Streams Through CSO Stream Buffer

When enabled:

- TVHeadend mux URLs published by Headendarr use `/tic-api/cso/channel_stream/<stream_id>?profile=tvh...`.
- TVHeadend pulls channel streams through the CSO channel-stream path with TVH remux profile behaviour.

When disabled:

- TVHeadend mux URLs are published using the standard per-source URL path strategy (local proxy/direct source based on source settings).

Why you would enable this:

- You want TVHeadend pulls to participate in CSO stream handling (buffering/remux/limit response behaviour).
- You want more consistent source-limit handling across mixed client types.

Trade-off:

- On exhausted limits or unavailable upstream, CSO returns an unavailable/limit response rather than allowing over-subscription.

## Channel Stream Organiser (CSO)

For a full CSO overview (runtime model, settings interactions, capacity behaviour, and routing guidance), see
[Channel Stream Organiser (CSO)](../channel-stream-organiser.md).

Channel-level setting highlights:

- **Enable Channel Stream Organiser**: force this channel to use CSO pipeline routing.
- **Preferred Stream Profile**: default profile used for direct channel CSO playback URLs.

Client requests can still override with `profile=<profile_id>` per request.
