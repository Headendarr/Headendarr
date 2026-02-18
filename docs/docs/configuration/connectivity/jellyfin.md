---
title: Jellyfin
draft: true
---

# Jellyfin

Use this guide to connect Jellyfin Live TV to Headendarr.

## Option 1: M3U Playlist (Recommended Starting Point)

:::warning MPEG-TS Transcoding Behavior
Jellyfin will generally transcode MPEG-TS (`.ts`) live streams.
To reduce unnecessary transcoding:

- for M3U setup, append `profile=matroska` to the Headendarr TVH playlist URL as a GET param
- for TVHeadend plugin setup, create a dedicated TVH user in the TVH UI and set that user's default stream profile to `matroska`

Do not use a Headendarr-managed user for this. Headendarr-managed TVH users use `pass` as the enforced default profile.
:::

Use a Headendarr TVHeadend playlist URL and force the TVHeadend stream profile to Matroska.

1. In Headendarr **Application Settings**, enable **Route playlists & HDHomeRun through TVHeadend**.
2. In Headendarr, open **Show Connection Details**.
3. Copy a **M3U Playlists** URL such as:
   `/tic-api/tvh_playlist/<source_id>/channels.m3u?stream_key=<key>`
4. Append `&profile=matroska` to the URL.
   Example:
   `/tic-api/tvh_playlist/1/channels.m3u?stream_key=<key>&profile=matroska`
5. In Jellyfin, go to **Dashboard -> Live TV -> Add Tuner Device**.
6. Choose **M3U Tuner**.
7. Paste the updated M3U URL.
8. Save the tuner.
9. In Headendarr, copy the **XMLTV Guide** URL from **Show Connection Details**.
10. In Jellyfin **Live TV**, add the XMLTV guide source and paste that URL.
11. Run channel/guide mapping in Jellyfin and verify playback.

## Option 2: Jellyfin TVHeadend Plugin

If you use the Jellyfin TVHeadend plugin, configure it to connect to TVHeadend provided by Headendarr.
The plugin does not provide a per-request stream-profile selector, so use a TVHeadend user whose default stream profile is `matroska`.

1. Install the TVHeadend plugin in Jellyfin (if not already installed).
2. In Headendarr, confirm **Route playlists & HDHomeRun through TVHeadend** is enabled.
3. In Headendarr **TVHeadend Settings**, ensure the TVH endpoint is reachable from Jellyfin.
4. In TVHeadend, create or use a dedicated user for Jellyfin and set that user/access entry default streaming profile to `matroska`.
5. In Jellyfin, configure the TVHeadend plugin with the TVH URL and those user credentials.
6. Save, trigger channel sync, and verify playback.

## Notes

- If playback still fails for specific channels, those channels may require remuxing/transcoding behavior outside of profile selection.
- You can test with both `profile=pass` and `profile=matroska` to compare direct-play/transcode behavior in Jellyfin.

### Observed Behavior (Current)

- In current testing, Jellyfin using the TVHeadend plugin often enters a full transcode path (video/audio re-encode), even when source streams are already playable.
- In current testing, Jellyfin using M3U URLs generated from Headendarr TVH playlists (`channels.m3u`) typically uses remux/transmux (codec copy) rather than full re-encode.
- This remux-only behavior was observed with both `profile=pass` and `profile=matroska`; the main difference is the input container/probing context, not whether Jellyfin necessarily re-encodes.

Plugin source/reference links:

- Jellyfin TVHeadend plugin source: <https://github.com/jellyfin/jellyfin-plugin-tvheadend>
- Jellyfin TVHeadend plugin docs: <https://jellyfin.org/docs/general/server/plugins/tvheadend/>
