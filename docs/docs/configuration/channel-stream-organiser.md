---
title: Channel Stream Organiser
---

# Channel Stream Organiser (CSO)

The **Channel Stream Organiser (CSO)** is Headendarr's stream orchestration pipeline.
It sits between client requests and upstream sources to provide consistent playback behaviour, source-capacity checks, and controlled failover/remux handling.

## What CSO does

CSO is responsible for:

- Selecting eligible sources for a channel and applying source order/failover rules.
- Enforcing source connection limits before and during playback.
- Running a shared ingest pipeline and fan-out output sessions for active clients.
- Applying profile-based output behaviour (for example remux/transcode targets).
- Returning explicit unavailable/limit responses when capacity is exhausted.

## Core CSO paths

- Channel playback path:
  - `/tic-api/cso/channel/<channel_id>`
- Per-source stream gate path:
  - `/tic-api/cso/channel_stream/<stream_id>`
- HLS output variants (when an HLS profile is used):
  - `/tic-api/cso/channel/<channel_id>/hls/<connection_id>/index.m3u8`
  - `/tic-api/cso/channel_stream/<stream_id>/hls/<connection_id>/index.m3u8`

For complete endpoint coverage, see [Connectivity Endpoints](./connectivity/endpoints.md).

## How CSO works at runtime

At a high level:

1. A client request enters a CSO route.
2. CSO resolves the effective profile/routing mode.
3. CSO starts or joins an ingest session for the selected source.
4. CSO starts or joins an output session for the requested profile/container.
5. The client is attached to that output session (with queue/backpressure handling).
6. On disconnect/idle/error, CSO tears down client/output/ingest state as needed.

This design allows multiple local clients to share upstream work while preserving source-limit behaviour.

## CSO-related settings

These settings control where CSO is used:

1. **Use CSO for combined playlists, XC, & combined HDHomeRun**
   - Applies to combined endpoints.
   - Enables CSO-centric routing semantics for combined outputs.

2. **Route per-source playlists & per-source HDHomeRun via TVHeadend**
   - Moves per-source playlist/HDHR client traffic through TVHeadend-facing paths.
   - Useful when TVHeadend is your primary stream-facing service.

3. **TVHeadend Settings -> Stream Buffer = CSO**
   - Routes TVHeadend mux pulls through CSO stream paths.
   - Improves consistency of buffering/limit handling for mixed-client setups.

See [Application Settings](./application-settings.md) and [TVHeadend Integration](./tvheadend.md) for exact setting definitions.

## Profiles and output behaviour

CSO honours `profile` arguments where supported and falls back to defaults when profiles are missing/unsupported.

- Typical remux/copy profiles: `default`, `mpegts`, `matroska`, `hls`
- Audio/video transform profiles: `aac-mpegts`, `h264-aac-mpegts`, etc.
- TVHeadend-only profile semantics: `tvh`

Profile details and endpoint-level behaviour are documented in [Connectivity Endpoints](./connectivity/endpoints.md).

## Failure and capacity behaviour

When source capacity is exhausted or playback cannot start:

- CSO blocks over-subscription instead of silently allowing extra upstream sessions.
- Client playback requests can receive unavailable/limit outcomes (depending on route/profile behaviour).
- In mixed-client deployments, this makes source limit enforcement explicit and predictable.

## When to use CSO-centric routing

Prefer CSO-centric paths when:

- You need strict source-limit enforcement across mixed client types.
- You want predictable channel failover and stream orchestration.
- You want consistent routing behaviour for combined outputs and TVHeadend mux pulls.
- You want stronger client observability (start/stop visibility) and more reliable stream audit coverage instead of opaque direct-to-source client pulls.
