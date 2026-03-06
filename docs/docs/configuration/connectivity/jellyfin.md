---
title: Jellyfin
---

# Jellyfin

Use this guide to connect Jellyfin Live TV to Headendarr.

:::warning Jellyfin Stream Compatibility
Jellyfin Live TV does **not** reliably accept HLS input in this setup. Prefer MPEG-TS based streams.

Recommended profile order for Jellyfin:

1. **`aac-mpegts`** (preferred default)
2. **`mpegts`**
3. **`matroska`** (use carefully; `aac-matroska` is usually safer than plain `matroska`)

Why `aac-mpegts` is preferred:

- Keeps output in MPEG-TS, which Jellyfin generally handles most consistently for Live TV.
- If source audio is already AAC, playback is often remux/copy with minimal overhead.
- If source audio is not AAC, this profile can avoid broader compatibility issues by ensuring AAC audio.

Matroska note:

- Plain `matroska` can be less predictable with some incoming audio formats in this flow.
- If you must use Matroska, prefer **`aac-matroska`** over plain `matroska`.

:::

## Recommended setup

Use **per-source M3U playlists** with profile **`aac-mpegts`**.

- Preferred method when copying manually from **Show connection details**:
  - **Per-source M3U playlists**
  - profile override **`aac-mpegts`**
- Why:
  - Jellyfin does not reliably support HLS in this flow.
  - MPEG-TS with AAC audio is typically the most reliable and lowest-overhead input for Jellyfin.
  - Per-source playlists let you set per-source tuner limits in Jellyfin, which aligns with source connection limits.

:::note Using a combined playlist is optional
If you prefer a simpler setup, you can use a single combined playlist in Jellyfin.
This can be fine for quick setup, but review [Combined vs per-source playlists in Jellyfin](#combined-vs-per-source-playlists-in-jellyfin) for why per-source playlists are usually the better option.
:::

1. In Headendarr, open **Show connection details**.
2. In Jellyfin, for each per-source playlist, click **Add tuner device** and complete these steps:
   1. On the next page, select **M3U Tuner** as the tuner type.
   2. Copy the tuner URL from **Connection-limited playlists / Per-source playlists** in **Show connection details** and paste it into the **File URL** field.
   3. Append your desired profile argument, for example `&profile=aac-mpegts`.
   4. Set **User Agent** to `Jellyfin`.
   5. Set **Simultaneous stream limit** for that source.
   6. Disable **Allow stream sharing**.
   7. Disable **Ignore DTS (decoding timestamp)**.
   8. Click **Save**.
3. Repeat the previous step for each of your per-source playlists from **Show connection details**.

[![Jellyfin M3U tuner setup with recommended fields](/img/screenshots/jellyfin-setup-copy-m3u-url.png)](/img/screenshots/jellyfin-setup-copy-m3u-url.png)

4. In Jellyfin Live TV, click **Add provider** and select **XMLTV**.
5. In the XMLTV provider dialog, complete these steps **before clicking Save**:
   1. Copy the **XMLTV Guide URL** from **Show connection details** and paste it into Jellyfin.
   2. Ensure **Enable for all tuner devices** is selected.

[![Jellyfin XMLTV provider setup with recommended fields](/img/screenshots/jellyfin-setup-copy-xmltv-url.png)](/img/screenshots/jellyfin-setup-copy-xmltv-url.png)

6. Run channel/guide mapping and test playback.

## Option 2: Jellyfin TVHeadend Plugin

If you use the Jellyfin TVHeadend plugin, Jellyfin connects directly to the TVHeadend backend (not to Headendarr playlist endpoints).

:::warning Plugin trade-offs
This plugin path is valid, but it is usually not the recommended default for Jellyfin.

Known downsides seen in real deployments:

- The plugin can have issues after updates, especially around major Jellyfin releases, and plugin compatibility can lag behind for days or weeks.
- It is known to force transcoding in cases where transcoding may not otherwise be required.
- Channel tune/start is generally slower than direct playlist-based setup, and this is often more noticeable for remote viewing.

Main upside:

- Native TVHeadend integration can be useful if you specifically want TVHeadend-centric workflows (for example backend-driven channel/recording ecosystem features).
:::

1. Install the TVHeadend plugin in Jellyfin (if not already installed).
2. In Headendarr, enable **Use CSO stream buffer for TVHeadend mux streams**.
3. Ensure Jellyfin can reach the TVHeadend backend directly on port `9981`.
4. In Headendarr **Users**, create a dedicated streaming-only user for Jellyfin.  
   Headendarr will sync this user to TVHeadend automatically.
5. In Jellyfin, configure the TVHeadend plugin with:
   - TVHeadend URL: `http://<your-host>:9981`
   - Username: the dedicated Jellyfin streaming user
   - Password: that user's Headendarr Streaming Key
6. Save, trigger channel sync, and verify playback.

Plugin source/reference links:

- Jellyfin TVHeadend plugin source: [github.com/jellyfin/jellyfin-plugin-tvheadend](https://github.com/jellyfin/jellyfin-plugin-tvheadend)
- Jellyfin TVHeadend plugin docs: [jellyfin.org/docs/general/server/plugins/tvheadend](https://jellyfin.org/docs/general/server/plugins/tvheadend/)

## Notes

### Application settings guidance

- Enable **Use CSO stream buffer for TVHeadend mux streams**.
- If you also use TVHeadend for other clients, enable:
  - **Route per-source playlists & per-source HDHomeRun via TVHeadend**

This combination generally gives better behaviour when TVHeadend and Jellyfin coexist.

### HDHomeRun caution for Jellyfin

HDHomeRun tuner emulation is not the preferred method for Jellyfin.

#### Why this is not ideal

- Jellyfin can intermittently keep HDHomeRun-backed sessions open after playback stops.
- Those stale sessions can hold source slots longer than expected.
- This can distort or exhaust per-source connection limits.

#### If you must use HDHomeRun

Route per-source HDHomeRun endpoints through the TVHeadend backend by enabling
**Route per-source playlists & per-source HDHomeRun via TVHeadend**.

This usually improves session lifecycle handling in mixed-client deployments because TVHeadend is better at stream session management and disconnect behaviour, which helps keep source-limit accounting cleaner.

#### Format guidance

When testing HDHomeRun playback behaviour in Jellyfin, use MPEG-TS-oriented profiles first:

- `aac-mpegts` (preferred)
- `mpegts`
- then other profiles as needed

:::note Combined endpoint is valid
Using a combined playlist or combined HDHomeRun endpoint is still valid for simple setups.
See [Combined vs per-source playlists in Jellyfin](#combined-vs-per-source-playlists-in-jellyfin) for trade-offs.
:::

### Jellyfin alongside TVHeadend clients

If you already use TVHeadend for other clients (for example Kodi, IPTV apps, or TVs connected to TVHeadend), there are two practical Jellyfin options:

1. **Use Jellyfin TVHeadend plugin**
   - Can work well in some setups.
   - Can be flaky across some Jellyfin/plugin updates.
   - Not usually the first recommendation unless you specifically want plugin-based TVH integration.

2. **Use Jellyfin M3U tuners, but route per-source playlists via TVHeadend**
   - In Headendarr, enable **Route per-source playlists & per-source HDHomeRun via TVHeadend**.
   - Jellyfin still uses per-source M3U tuner setup.
   - TVHeadend becomes the stream-facing client for those per-source routes, so limit/routing behaviour is unified for mixed-client environments.
   - Usually adds only a very small extra delay, but gives cleaner operational consistency when TVHeadend is already in use.

### Combined vs per-source playlists in Jellyfin

If all your providers share similar limits and you want the fastest setup, combined playlist is fine.
If you want precise source-limit handling in Jellyfin, use per-source playlists.

| Approach                     | Pros                                                                                                                                                          | Cons                                                                                                                                                                                            | Best fit                                                                       |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| **Combined M3U playlist**    | Fastest and simplest setup; one tuner to manage                                                                                                               | Jellyfin sees one global tuner limit and cannot model different source limits; you rely on [CSO](../channel-stream-organiser.md) channel-level protection/slate behaviour when a source is full | Quick and simple setup when limit precision is not critical                    |
| **Per-source M3U playlists** | Jellyfin can apply tuner limits per source; better awareness of mixed source capacities; better saturation across all sources without blind over-subscription | More setup effort (one tuner per source)                                                                                                                                                        | Recommended for most Jellyfin deployments, especially with mixed source limits |

#### Practical effect of combined playlist limits

- With combined playlist, Jellyfin does not know each upstream source's separate connection cap.
- When a channel is tuned and that source is already at capacity, [CSO](../channel-stream-organiser.md) can block playback and present an unavailable/slate outcome for that tune attempt.
- This is safe, but less graceful than Jellyfin proactively managing per-source limits itself.
