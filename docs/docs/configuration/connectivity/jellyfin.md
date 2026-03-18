---
title: Jellyfin
---

# Jellyfin

Use this guide to connect Jellyfin Live TV and curated VOD to Headendarr.

:::warning Jellyfin stream compatibility
Jellyfin Live TV does **not** reliably accept HLS input in this setup. Prefer MPEG-TS based streams.

Recommended profile order for Jellyfin:

1. **`aac-mpegts`** (preferred default)
2. **`mpegts`**
3. **`matroska`** (use carefully; `aac-matroska` is usually safer than plain `matroska`)
   :::

## Option #1 (recommended): Jellyfin TVHeadend plugin

If you use the Jellyfin TVHeadend plugin, Jellyfin connects directly to the TVHeadend backend (not Headendarr playlist endpoints).

This is the recommended default if you want the most reliable live TV connection handling in Jellyfin today.

It also brings two practical benefits:

- Jellyfin can use TVHeadend's DVR workflow for recordings.
- If you already use TVHeadend for other clients, Jellyfin joins that same backend model instead of creating a separate direct-play path.

:::warning Plugin trade-offs
This plugin path is valid, but it has known downsides in some environments:

- Plugin compatibility can lag behind Jellyfin releases.
- It can force transcoding in cases where direct stream paths would not, so this path is not always the most efficient one.
- Channel tune/start can be slower than direct playlist paths.

:::

### Steps

1. Install the TVHeadend plugin in Jellyfin (if not already installed).
2. Ensure Jellyfin can reach TVHeadend on port `9981`.
3. In Headendarr **Users**, create a dedicated streaming-only user for Jellyfin.
4. Configure the plugin with:
   1. TVHeadend Hostname or IP Address
   2. Web Root
   3. Username
   4. Password (use the user's streaming key)

[![Jellyfin XMLTV provider setup with recommended fields](/img/screenshots/jellyfin-setup-configure-tvh-plugin-settings.png)](/img/screenshots/jellyfin-setup-configure-tvh-plugin-settings.png)

5. Save
6. Navigate to the **Live TV** settings page in Jellyfin and click "Refresh Guide Data".

Plugin references:

- Jellyfin TVHeadend plugin source: [github.com/jellyfin/jellyfin-plugin-tvheadend](https://github.com/jellyfin/jellyfin-plugin-tvheadend)
- Jellyfin TVHeadend plugin docs: [jellyfin.org/docs/general/server/plugins/tvheadend](https://jellyfin.org/docs/general/server/plugins/tvheadend/)

## Option #2: Per-source HDHomeRun via TVHeadend

This is a valid option if you want tuner-device behaviour and per-source connection-limit behaviour in Jellyfin.

Choose this option if you specifically want HDHomeRun-style tuner behaviour in Jellyfin and want each source to retain its own connection-limit behaviour.

:::warning Issue with Jellyfin client behaviour
Some Jellyfin client paths can keep HDHomeRun-style shared streams open after playback appears to stop. In practice this means stream slots can remain occupied longer than expected, which can affect connection limits.

This behaviour has been discussed in:

- GitHub Issue - '10.11.4 HDHomeRun tuner locking': [jellyfin/jellyfin#15769](https://github.com/jellyfin/jellyfin/issues/15769)

In testing, web playback (for example Chrome/web player) can behave better than some app clients.
:::

### Steps

1. In Headendarr **Application Settings**, enable **Route per-source playlists & per-source HDHomeRun via TVHeadend**.
2. In Headendarr **TVHeadend Settings**, set **Stream Buffer** to **CSO**.
3. In Jellyfin's **Live TV** settings page, click **Add tuner device** and complete these steps:
   1. On the next page, select **HD Homerun** as the tuner type.
   2. Copy the tuner URL from **HDHomeRun Tuner Emulators** in **Show connection details** and paste it into the **Tuner IP Address** field.
   3. Click **Save**.

[![Jellyfin M3U tuner setup with recommended fields](/img/screenshots/jellyfin-setup-copy-per-source-hdhr-url.png)](/img/screenshots/jellyfin-setup-copy-per-source-hdhr-url.png)

4. In Jellyfin's **Live TV** settings page, click **Add provider** and select **XMLTV**.
5. In the XMLTV provider dialog, complete these steps **before clicking Save**:
   1. Copy the **XMLTV Guide URL** from **Show connection details** and paste it into Jellyfin.
   2. Ensure **Enable for all tuner devices** is selected.

[![Jellyfin XMLTV provider setup with recommended fields](/img/screenshots/jellyfin-setup-copy-xmltv-url.png)](/img/screenshots/jellyfin-setup-copy-xmltv-url.png)

## Option #3: Combined M3U playlist

Multiple per-source M3U tuners can cause duplicate channels in Jellyfin (Jellyfin issue [#632](https://github.com/jellyfin/jellyfin/issues/632)). Using one combined M3U tuner avoids that duplication pattern.

Choose this option if you want the simplest direct Jellyfin setup and are happy to use one shared playlist instead of tuner-device behaviour.

:::note Duplicates with multiple M3U tuners
With multiple M3U tuner entries, Jellyfin treats playlist entries as channel records. Matching channels across separate playlists are usually shown as duplicates.

To avoid this, it is recommended to use one **combined M3U tuner** in Jellyfin instead of multiple per-source M3U tuners.

Reference: Jellyfin issue [#632](https://github.com/jellyfin/jellyfin/issues/632).
:::

### Steps

1. In Headendarr, open **Show connection details**.
2. In Jellyfin's **Live TV** settings page, click **Add tuner device** and complete these steps:
   1. On the next page, select **M3U Tuner** as the tuner type.
   2. Copy the tuner URL from **Combined Playlist** in **Show connection details** and paste it into the **File URL** field.
   3. Append your desired profile argument, for example `&profile=aac-mpegts`.
   4. Set **User Agent** to `Jellyfin`.
   5. Set **Simultaneous stream limit** for that source.
   6. Disable **Allow stream sharing**.
   7. Disable **Ignore DTS (decoding timestamp)**.
   8. Click **Save**.

[![Jellyfin M3U tuner setup with recommended fields](/img/screenshots/jellyfin-setup-copy-combined-m3u-url.png)](/img/screenshots/jellyfin-setup-copy-combined-m3u-url.png)

4. In Jellyfin's **Live TV** settings page, click **Add provider** and select **XMLTV**.
5. In the XMLTV provider dialog, complete these steps **before clicking Save**:
   1. Copy the **XMLTV Guide URL** from **Show connection details** and paste it into Jellyfin.
   2. Ensure **Enable for all tuner devices** is selected.

[![Jellyfin XMLTV provider setup with recommended fields](/img/screenshots/jellyfin-setup-copy-xmltv-url.png)](/img/screenshots/jellyfin-setup-copy-xmltv-url.png)

6. Run channel/guide mapping and test playback.

## Video On Demand (VOD)

This is the best option if you want Jellyfin to access the curated VOD as a normal media library.

With this approach, Headendarr generates `.strm` files and NFO metadata under the library export path, and Jellyfin scans those directories as movie and TV libraries.

The result feels much closer to a normal Jellyfin library than an IPTV catalogue. Users can browse curated films and shows with regular posters, metadata, seasons, and episode structure inside the standard Jellyfin interface.

### How it works

By default, Headendarr writes exports under:

`/library`

The generated structure is user-specific and grouped by content type and category slug:

- Movies:
  - `/library/<username>/Movies/<category-slug>/`
- Shows:
  - `/library/<username>/Shows/<category-slug>/`

Examples:

- `/library/myuser/Movies/curated-movies/`
- `/library/myuser/Shows/curated-shows/`

Within those category folders, Headendarr writes scraper-friendly directory structures such as:

- movie folder:
  - `Tenet (2020)/Tenet (2020).strm`
- show folder:
  - `3 Body Problem (2024)/Season 01/3 Body Problem S01E01.strm`

If Headendarr and Jellyfin run in separate containers, mount the same host library path into Jellyfin so it can read the exported files.

Example:

- Headendarr writes to a host path such as `/srv/media/tic-library`
- mount that same host path into Jellyfin
- then add the relevant subdirectories to Jellyfin as libraries

```
services:
  headendarr:
    image: ghcr.io/headendarr/headendarr:latest
    volumes:
      - type: bind
        source: /srv/media/tic-library
        target: /library
        bind:
          selinux: z
...
  jellyfin:
    image: ghcr.io/jellyfin/jellyfin:latest
    volumes:
      - type: bind
        source: /srv/media/tic-library
        target: /library
        bind:
          selinux: z
```

### Steps

In Headendarr:

1. Enable **Generate STRM files** on the VOD category you want to expose.
2. In **Users**, make sure one of the streamer users has the required VOD access configured and has **Create VOD .strm Files** enabled.

In Jellyfin:

3. Add a new media library:
   1. Use **Movies** for paths under `/library/<username>/Movies/...`
   2. Use **Shows** for paths under `/library/<username>/Shows/...`
4. Point Jellyfin at the relevant exported category path, for example:
   1. `/library/myuser/Movies/curated-movies/`
   2. `/library/myuser/Shows/curated-shows/`
5. Review the library settings before saving:
   1. Make sure **Save artwork into media folders** is disabled.
   2. Disable all trick-play image options:
      1. **Enable trickplay image extraction**
      2. **Extract trickplay images during library scan**
      3. **Save trickplay images next to media**
   3. Disable all chapter image extraction options:
      1. **Enable chapter image extraction**
      2. **Extract chapter images during library scan**
6. Save and let Jellyfin scan the library.
