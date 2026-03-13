---
title: Plex
---

# Plex

Use this guide to connect Plex Live TV & DVR to Headendarr using HDHomeRun tuner emulation and XMLTV guide data.

:::warning HLS Proxy Remux Limitation
Plex Live TV/DVR can have playback issues with HLS (`.m3u8`) channel streams.
For best compatibility, make sure Plex receives MPEG-TS (`.ts`) streams for those channels.

For many sources, forcing audio to AAC with an MPEG-TS output improves Plex stability significantly with minimal extra overhead compared with failed tune/retry loops.
:::

## Container configuration

Set `PLEX_SERVERS_JSON` in your Headendarr container environment, then restart the container.

Example `docker-compose.yml` snippet:

```yaml
services:
  headendarr:
    image: ghcr.io/headendarr/headendarr:latest
    environment:
      PLEX_SERVERS_JSON: >-
        [{"name":"Command","base_url":"http://192.168.7.234:32400","token":"YOUR_TOKEN","verify_tls":false}]
```

What this enables in Headendarr:

1. A dedicated **Plex Settings** page appears when `PLEX_SERVERS_JSON` is valid.
2. Per Plex server settings control whether tuner sync is enabled, tuner mode, model prefix, and stream profile.
3. Headendarr can automatically provision and keep Plex tuners in sync:
   - create missing managed tuners
   - update channel mappings after channel/source changes
   - remove stale managed tuners
   - remove managed tuners for enabled sources that currently publish an empty HDHomeRun lineup

:::info Server name matching
The `name` field in each `PLEX_SERVERS_JSON` entry should match that Plex server's current friendly name.
Headendarr currently validates the configured name against the live server identity before applying tuner changes.
:::

### PLEX_SERVERS_JSON Generator

Use this form to generate a valid `PLEX_SERVERS_JSON` environment variable value for one or more Plex servers.

<PlexServersJsonBuilder />

### Plex Settings Page

After `PLEX_SERVERS_JSON` is valid and the container has restarted, open **Plex Settings** in Headendarr.

Each configured Plex server exposes these groups of settings:

- **Connection**
  - **Enabled**: turns automatic tuner reconciliation on or off for that Plex server.
  - **Headendarr base URL**: the URL Plex should use to reach Headendarr's HDHomeRun and XMLTV endpoints.
  - **Stream user**: selects which Headendarr user's stream key is published to Plex.
- **Tuner Settings**
  - **Default tuner mode**:
    - **Per source** publishes one managed tuner per Headendarr source.
    - **Combined** publishes one combined managed tuner.
  - **Stream profile**: selects the profile segment used in generated HDHomeRun endpoints.
  - Optional recording-time transcode controls are also available for compatible Plex workflows.
- **DVR Settings**
  - Resolution preference, replace-lower-quality behaviour, partial-airing behaviour, guide enrichment, post-processing script, commercial detection mode, and guide refresh cadence.
  - Headendarr also pushes your global DVR start/end padding into Plex's DVR settings during reconcile.

Settings on this page auto-save. Headendarr then queues a Plex reconcile run after Plex settings are saved.

### Reconcile Behaviour

Plex tuner reconciliation runs whenever Plex settings are saved in Headendarr, and otherwise runs on the background scheduler every 5 minutes to keep Plex aligned with the current Headendarr configuration.

During reconciliation, Headendarr automatically keeps Plex Live TV in sync with the channels and mappings you manage in Headendarr. It can create missing managed tuners, attach new tuners to the correct Plex DVR, update existing tuners in place when settings change, and refresh channel mappings after source or channel changes. It also includes safety checks to avoid deleting the last tuner on a Plex DVR, which helps prevent Plex from dropping the whole DVR configuration and protects your existing DVR setup and recordings. The goal is that you manage channels, guide mappings, and source changes in Headendarr, and Headendarr handles keeping Plex up to date for you.

## Manually configure

### Steps

For Plex, the recommended workarounds are:

1. **Option #1 (most reliable):** Enable **Route per-source playlists & per-source HDHomeRun via TVHeadend** in **Application Settings**.
   This applies to per-source endpoints and makes TVHeadend the stream client for those routes.
   For best stability with CSO-backed mux paths, also enable **Use CSO stream buffer for TVHeadend mux streams**.
   Ensure **TVHeadend Settings** has an FFmpeg stream buffer enabled, for example:

```bash
-hide_banner -loglevel error -probesize 10M -analyzeduration 0 -fpsprobesize 0 -i [URL] -c copy -metadata service_name=[SERVICE_NAME] -f mpegts pipe:1
```

2. **Option #2:** Enable **Use HLS Proxy** and **Enable FFmpeg remux** on the source in **Stream Source Settings** so HLS sources are remuxed to MPEG-TS.

1. Navigate to the **Live TV & DVR** page in Plex.
1. Click **Set Up Plex Tuner**.

[![Plex add first tuner](/img/screenshots/plex-setup-add-first-tuner.png)](/img/screenshots/plex-setup-add-first-tuner.png)

3. Click **Don't see your HDHomeRun device? Enter its network address manually** to enter the address manually.

4. In Headendarr, open **Show connection details**.
5. Copy either a per-source HDHomeRun URL (best for source limits) or the combined HDHomeRun URL.
6. Paste that URL into the **HDHOMERUN DEVICE ADDRESS** field.

[![Copy HDHomeRun emulator URL](/img/screenshots/plex-setup-copy-hdhr-device-url.png)](/img/screenshots/plex-setup-copy-hdhr-device-url.png)

7. Click **Connect**. You should now see your tuner with its details and available tuners.
8. Click **Have an XMLTV guide on your server? Click here to use it.**

[![Click to use XMLTV](/img/screenshots/plex-setup-click-to-use-xmltv.png)](/img/screenshots/plex-setup-click-to-use-xmltv.png)

9. Copy the **XMLTV Guide** URL from **Show connection details** and paste it into the **XMLTV GUIDE** field.

[![Copy XMLTV URL](/img/screenshots/plex-setup-copy-xmltv-url.png)](/img/screenshots/plex-setup-copy-xmltv-url.png)

10. Click **Continue**. You will see a list of tuner channels and a list of EPG channels. Confirm they are paired correctly.
11. Click **Continue** again. Live TV should now be set up and connected to Headendarr.
12. Click **Add Another Device** and repeat for each HDHomeRun tuner source. Each source should enforce its own connection limits in Plex Live TV & DVR.

[![Add additional tuners](/img/screenshots/plex-setup-add-additional-tuners.png)](/img/screenshots/plex-setup-add-additional-tuners.png)
