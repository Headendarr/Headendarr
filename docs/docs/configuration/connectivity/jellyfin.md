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

- for M3U setup, append `profile=aac-matroska` to the Headendarr TVH playlist URL as a GET param
- for TVHeadend plugin setup, create a dedicated TVH user in the TVH UI and set that user's default stream profile to `matroska`

Do not use a Headendarr-managed user for this. Headendarr-managed TVH users use `pass` as the enforced default profile.
:::

Use a Headendarr TVHeadend playlist URL and force the TVHeadend stream profile to Matroska.

1. In Headendarr **Application Settings**, enable **Route per-source playlists & per-source HDHomeRun via TVHeadend**.
   This applies to per-source endpoints (for example `/tic-api/playlist/<id>.m3u`).
2. Optional but recommended: enable **Use CSO stream buffer for TVHeadend mux streams** for CSO-managed buffering/limit behaviour on TVHeadend pulls.
3. In Headendarr, open **Show Connection Details**.
4. Copy a **M3U Playlists** URL such as:
   `/tic-api/playlist/<source_id>.m3u?stream_key=<key>`
5. Append `&profile=aac-matroska` to the URL.
   Example:
   `/tic-api/playlist/1.m3u?stream_key=<key>&profile=aac-matroska`
6. In Jellyfin, go to **Dashboard -> Live TV -> Add Tuner Device**.
7. Choose **M3U Tuner**.
8. Paste the updated M3U URL.
9. Save the tuner.
10. In Headendarr, copy the **XMLTV Guide** URL from **Show Connection Details**.
11. In Jellyfin **Live TV**, add the XMLTV guide source and paste that URL.
12. Run channel/guide mapping in Jellyfin and verify playback.

## Option 2: Jellyfin TVHeadend Plugin

If you use the Jellyfin TVHeadend plugin, configure it to connect to TVHeadend provided by Headendarr.
The plugin does not provide a per-request stream-profile selector, so use a TVHeadend user whose default stream profile is `matroska`.

1. Install the TVHeadend plugin in Jellyfin (if not already installed).
2. In Headendarr, confirm **Route per-source playlists & per-source HDHomeRun via TVHeadend** is enabled.
3. Optional but recommended: enable **Use CSO stream buffer for TVHeadend mux streams**.
4. In Headendarr **TVHeadend Settings**, ensure the TVH endpoint is reachable from Jellyfin.
5. In TVHeadend, create or use a dedicated user for Jellyfin and set that user/access entry default streaming profile to `matroska`.
6. In Jellyfin, configure the TVHeadend plugin with the TVH URL and those user credentials.
7. Save, trigger channel sync, and verify playback.

## Notes

- If playback still fails for specific channels, those channels may require remuxing/transcoding behavior outside of profile selection.
- You can test with both `profile=pass` and `profile=aac-matroska` to compare direct-play/transcode behavior in Jellyfin.

### Observed Behavior (Current)

- In current testing, Jellyfin using the TVHeadend plugin often enters a full transcode path (video/audio re-encode), even when source streams are already playable.
- In current testing, Jellyfin using M3U URLs generated from Headendarr TVH playlists (`channels.m3u`) typically uses remux/transmux (codec copy) rather than full re-encode.
- This remux-only behavior was observed with both `profile=pass` and `profile=aac-matroska`; the main difference is the input container/probing context, not whether Jellyfin necessarily re-encodes.

Plugin source/reference links:

- Jellyfin TVHeadend plugin source: [github.com/jellyfin/jellyfin-plugin-tvheadend](https://github.com/jellyfin/jellyfin-plugin-tvheadend)
- Jellyfin TVHeadend plugin docs: [jellyfin.org/docs/general/server/plugins/tvheadend](https://jellyfin.org/docs/general/server/plugins/tvheadend/)
