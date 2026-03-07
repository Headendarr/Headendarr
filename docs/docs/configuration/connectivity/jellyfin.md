---
title: Jellyfin
---

# Jellyfin

Use this guide to connect Jellyfin Live TV to Headendarr.

:::warning Jellyfin stream compatibility
Jellyfin Live TV does **not** reliably accept HLS input in this setup. Prefer MPEG-TS based streams.

Recommended profile order for Jellyfin:

1. **`aac-mpegts`** (preferred default)
2. **`mpegts`**
3. **`matroska`** (use carefully; `aac-matroska` is usually safer than plain `matroska`)
   :::

## Option #1 (recommended): Jellyfin TVHeadend plugin

If you use the Jellyfin TVHeadend plugin, Jellyfin connects directly to the TVHeadend backend (not Headendarr playlist endpoints).

This is the recommended default for Jellyfin at this time.

:::warning Plugin trade-offs
This plugin path is valid, but it has known downsides in some environments:

- Plugin compatibility can lag behind Jellyfin releases.
- It can force transcoding in cases where direct stream paths would not.
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

## Option #2: Combined M3U playlist

Multiple per-source M3U tuners can cause duplicate channels in Jellyfin (Jellyfin issue [#632](https://github.com/jellyfin/jellyfin/issues/632)). Using one combined M3U tuner avoids that duplication pattern.

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

## Option #3: Per-source HDHomeRun via TVHeadend

This is a valid fallback option if you specifically want tuner-device behaviour and per-source connection limits in Jellyfin.

:::warning HDHR routing issues
Jellyfin can hold HDHomeRun-backed connections open when connecting directly to CSO endpoints.

Per-source HDHomeRun routed through TVHeadend can also show channel disconnect/drop issues in some deployments.

Use this option only if you accept extra operational tuning and troubleshooting across both routing paths.
:::

### Steps

1. In Headendarr, enable **Route per-source playlists & per-source HDHomeRun via TVHeadend**.
2. In Headendarr, enable **Use CSO stream buffer for TVHeadend mux streams**.
3. In Jellyfin, add HDHomeRun tuners using per-source HDHomeRun URLs.
4. Add XMLTV from Headendarr and test playback stability over time.
