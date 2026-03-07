---
title: Application Settings
---

# Application Settings

The **Settings** page allows you to configure global settings for Headendarr.

## UI Settings

These settings control the behaviour and appearance of the Headendarr web interface.

- **Highlight channels with source issues**: When enabled, the UI will display a warning highlight for channels that are linked to disabled sources or have failed TVHeadend muxes. This helps in quickly identifying problematic channels.
- **Start page after login**: Choose the default page that users will land on immediately after signing in.
  - **Options**: Dashboard, Sources, EPGs, Channels, TV Guide, DVR, Audit

## Connections

These settings define how Headendarr interacts with external services and how clients connect to Headendarr.

- **Headendarr Host**: Callback base URL used when TVHeadend is external (Lite deployment). Headendarr writes this into TVHeadend for XMLTV and stream callback URLs.
  - Availability: shown only for Lite/external-TVHeadend deployments.
  - In AIO/local-TVHeadend deployments this setting is hidden/ignored.
  - Purpose: provide the callback base URL written into TVHeadend for XMLTV and stream callback URLs when TVHeadend is external.
  - Client-facing playlist/XMLTV/HDHomeRun URLs are derived from the request host in AIO mode.
- Transcoding behavior is controlled per profile in **Stream Profiles**. Disable a transcoding profile there to prevent its use.
- **Enable periodic channel stream health checks**: Runs CSO-managed background diagnostics for channel streams when sources are idle.
  - Scheduler cadence: every 5 minutes.
  - Per-stream cooldown: 6 hours (a stream is skipped if it was checked within the last 6 hours).
  - Background sample window: 7 seconds of media sampling after first media bytes are received.
  - Startup guard: sampling does not begin until first media data arrives; checks abort if no media arrives within 30 seconds.
  - Parallelism: checks run in bounded parallel workers (default 2), capped per run (default 10 streams).
  - Playback priority: any background diagnostic using source capacity is pre-empted when real playback needs that capacity.
  - No-overlap worker: a new scheduler tick does not start a second worker while the previous health-check worker is still running.
- **Cache channel logos**: Controls whether Headendarr rewrites channel logos to cached `/tic-api/channels/<id>/logo/...` URLs.
  - Enabled (default): channel logos are served from Headendarr cache/proxy URLs.
  - Disabled: original source logo URLs are emitted in channel payloads, XMLTV, and generated playlists.
- **Stream Profiles table**:
  - Enable/disable each supported `profile` value exposed on stream and playlist URLs.
  - Configure per-profile hardware acceleration preference (`HW Accel`, VAAPI encode path) for transcoding profiles.
  - Configure per-profile `Deinterlace` for transcoding profiles.
  - `Deinterlace` adds a small processing overhead when enabled.
  - If a stream is not interlaced, enabling `Deinterlace` does not materially change visual output.
  - If a client requests a disabled profile, Headendarr falls back to `default`.

### Routing Toggles Explained

Headendarr exposes three routing toggles because they control different traffic families.
They are designed to be combined based on your playback setup.

1. **Use CSO for combined playlists, XC, & combined HDHomeRun**

- Scope: combined endpoints only (`/tic-api/playlist/combined.m3u`, XC combined output, combined HDHomeRun lineups).
- Use this when clients connect directly to Headendarr and you want CSO channel ordering/failover behaviour on combined outputs.
- This is useful for Plex, Emby, Jellyfin, and playlist clients that should use one combined entrypoint without relying on TVHeadend.

2. **Route per-source playlists & per-source HDHomeRun via TVHeadend**

- Scope: per-source endpoints only (for example `/tic-api/playlist/<id>.m3u` and `/tic-api/hdhr_device/<stream_key>/<id>/...`).
- Use this when your playback stack is TVHeadend-centric and you want TVHeadend to be the main streaming client.
- Benefit: TVHeadend applies its own buffering/scanning behaviour and centralises stream handling for TVH clients.
- Trade-off: adds an extra hop (client -> TVH -> Headendarr/upstream) and depends on TVHeadend reachability.

3. **Use CSO stream buffer for TVHeadend mux streams**

- Scope: TVHeadend mux URLs published by Headendarr.
- Use this when you want TVHeadend pulls to go through the CSO stream path (including TVH remux profile), not directly to source/HLS-proxy URLs.
- Benefit: connection-limit tracking and buffering behaviour are handled in the CSO stream path for TVHeadend pulls.
- Trade-off: when limits are exhausted, playback fails fast with a CSO unavailable/limit response rather than silently over-consuming upstream sessions.

### How To Choose

- **Direct clients, no TVHeadend required**:
  - Enable combined-through-CSO.
  - Disable per-source-through-TVHeadend.
  - Optional: disable TVH-through-CSO if TVHeadend is not in use.

- **TVHeadend-first deployment (most clients connect to TVHeadend)**:
  - Enable per-source-through-TVHeadend.
  - Optionally enable TVH-through-CSO for CSO-managed buffering/limit handling on TVH pulls.
  - Combined-through-CSO is optional for direct combined endpoints.

- **Mixed clients (TVHeadend + direct combined playlists/HDHomeRun)**:
  - Enable combined-through-CSO and per-source-through-TVHeadend together.
  - Enable TVH-through-CSO if you want TVH pulls and direct CSO paths to share CSO-side limit enforcement behaviour.

### Connection-Limit Behaviour Notes

- Without TVHeadend routing, direct per-source/combined clients are governed by Headendarr routing and CSO/source checks.
- With per-source-through-TVHeadend enabled, per-source client playback is mediated by TVHeadend.
- With TVH-through-CSO also enabled, TVHeadend pulls go through CSO stream handling, improving consistency for mixed-client limit enforcement.

## User Agents

This section allows you to define and manage custom User-Agent headers. These User-Agents are used by Headendarr when fetching data from your IPTV sources and EPG providers. Some providers may require a specific User-Agent to prevent blocking.

- **Add User Agent**: Click this button to add a new custom User-Agent entry.
- **Name**: A descriptive name for your User-Agent (e.g., "VLC Player", "My Custom UA").
- **User-Agent**: The actual User-Agent string (e.g., `VLC/3.0.23 LibVLC/3.0.23`).

## DVR Settings

These settings control the default behaviour for Digital Video Recorder (DVR) functionalities.

- **Pre-recording padding (minutes)**: Specify the number of minutes to begin recording _before_ a scheduled programme's start time. This helps ensure that the beginning of a programme is not missed due to scheduling inaccuracies.
- **Post-recording padding (minutes)**: Specify the number of minutes to continue recording _after_ a scheduled programme's end time. This is useful for capturing content that runs slightly over its scheduled slot.
- **Default recording retention**: Choose how long recorded programmes will be kept by default before being automatically deleted. This policy applies to TVHeadend recording profiles synced by Headendarr.
  - **Options**: 1 day, 3 days, 5 days, 1 week, 2 weeks, 3 weeks, 1 month, 2 months, 3 months, 6 months, 1 year, 2 years, 3 years, Maintained space, Forever.
- **Recording Profiles**: These profiles define the file and folder naming conventions for your recorded content. The first profile in the list is treated as the default for users and as a fallback for scheduling.
  - **Add Recording Profile**: Click this button to create a new recording profile.
  * **Profile Name**: A friendly name for the recording profile (e.g., "Movies", "Kids Shows").
  * **Pathname Format**: A format string that determines the directory and filename structure for recordings saved by this profile. Variables can be used to dynamically insert programme details. (e.g., `$Q$n.$x` for `[Episode Name].[Extension]`).

### Recording Profiles and Pathname Format

Headendarr stores recording profiles in **Application Settings -> DVR Settings** and syncs them to TVHeadend recorder profiles when recordings are scheduled.

- One required profile always exists: `Default`.
- Additional profiles can be added (for example `Shows`, `Movies`).
- When creating a one-time recording or series rule, users can choose the profile.
- If no profile is selected (or a saved key is invalid), Headendarr falls back to `default`.

### Default Profiles

| Key       | Name      | Default Pathname |
| --------- | --------- | ---------------- |
| `default` | `Default` | `%F_%R $u$n.$x`  |
| `shows`   | `Shows`   | `$Q$n.$x`        |
| `movies`  | `Movies`  | `$Q$n.$x`        |

### Retention Policy Values

These are the values shown in the DVR retention dropdowns (global default and per-user override).
They control how long recordings are kept before automatic cleanup.

Time-based options:

- `1 day`, `3 days`, `5 days`
- `1 week`, `2 weeks`, `3 weeks`
- `1 month`, `2 months`, `3 months`, `6 months`
- `1 year`, `2 years`, `3 years`

Special options:

- `Maintained space`: Keep recordings, but allow automatic cleanup when space management requires it.
  This is useful when you want to retain as much history as possible while still protecting free disk space.
- `Forever`: Do not auto-delete recordings by age.
  Use this only if you have enough storage and your own process for pruning old recordings.

Storage considerations:

- Shorter retention windows reduce disk usage growth.
- Longer retention windows require more storage capacity.
- `Forever` can fill disks over time if not manually managed.
- `Maintained space` is typically a safer default for systems with limited or shared storage.

Optional post-processing workflow:

- If you want to process recordings after completion (for example transcode, rename, or move to another location),
  consider using Unmanic: [https://docs.unmanic.app](https://docs.unmanic.app).

### TVHeadend Pathname Tokens

| Token | Description                            | Example                                |
| ----- | -------------------------------------- | -------------------------------------- |
| `$t`  | Event title                            | `Tennis - Wimbledon`                   |
| `$s`  | Event subtitle or summary              | `Live Tennis Broadcast from Wimbledon` |
| `$u`  | Event subtitle                         | `Tennis`                               |
| `$m`  | Event summary                          | `Live Tennis Broadcast from Wimbledon` |
| `$e`  | Event episode name                     | `S02-E06`                              |
| `$A`  | Event season number                    | `2`                                    |
| `$B`  | Event episode number                   | `6`                                    |
| `$c`  | Channel name                           | `SkySport`                             |
| `$g`  | Content type                           | `Movie : Science fiction`              |
| `$Q`  | Scraper-friendly name layout           | `Gladiator (2000)` / `Bones - S02E06`  |
| `$q`  | Scraper-friendly name with directories | `tvshows/Bones/Bones - S05E11`         |
| `$n`  | Unique suffix if file exists           | `-1`                                   |
| `$x`  | Output extension from muxer            | `mkv`                                  |
| `%F`  | ISO date (`strftime`)                  | `2011-03-19`                           |
| `%R`  | 24-hour time (`strftime`)              | `14:12`                                |

Common baseline pattern:

- `$t$n.$x` -> title + uniqueness + extension

### Delimiter Variants

Tokens like `$t` and `$s` support delimiter forms where the delimiter is only emitted when a value exists.

Examples:

- `$ t`
- `$-t`
- `$_t`
- `$.t`
- `$,t`
- `$;t`

Length limits can be applied for some tokens:

- `$99-t` limits output length to 99 characters.

### Scraper-Friendly Modes (`$q`, `$Q`)

`$q` and `$Q` support numeric variants:

| Variant | Behaviour                                   |
| ------- | ------------------------------------------- |
| `1`     | Force movie formatting (`$1q`, `$1Q`)       |
| `2`     | Force TV-series formatting (`$2q`, `$2Q`)   |
| `3`     | Alternative directory layout (`$3q`, `$3Q`) |

Examples:

- `$3q` -> `tvmovies/Gladiator (2000)/Gladiator (2000)`
- `$3q` -> `tvshows/Bones/Season 5/Bones - S05E11`
- `$3Q` -> `Gladiator (2000)/Gladiator (2000)`
- `$3Q` -> `Bones/Season 5/Bones - S05E11`

### Season/Episode Padding

`$A` and `$B` support zero-padding modifiers:

| Token | Meaning                           | Example |
| ----- | --------------------------------- | ------- |
| `$A`  | Season number (raw)               | `2`     |
| `$2A` | Season number padded to 2 digits  | `02`    |
| `$B`  | Episode number (raw)              | `6`     |
| `$3B` | Episode number padded to 3 digits | `006`   |

Example:

- Format: `$t/Season $A/$2B-$u$n.$x`
- Output: `/recordings/Bones/Season 2/06-The Girl in Suite 2103.ts`

## Audit Logging

These settings control the retention of audit logs within the Headendarr database.

- **Audit log retention (days)**: Define the number of days that audit log entries will be stored in the database. Older entries will be automatically purged to manage database size.
