---
title: Plex
---

# Plex

Use this guide to connect Plex Live TV & DVR to Headendarr using HDHomeRun tuner emulation and XMLTV guide data.

:::warning HLS Proxy Remux Limitation
Plex Live TV/DVR can have playback issues with HLS (`.m3u8`) channel streams.
For best compatibility, make sure Plex receives MPEG-TS (`.ts`) streams for those channels.

For many sources, forcing audio to AAC with an MPEG-TS output improves Plex stability significantly with minimal extra overhead compared with failed tune/retry loops.

For Plex, the recommended workarounds are:

1. **Option #1 (most reliable):** Enable **Route per-source playlists & per-source HDHomeRun via TVHeadend** in **Application Settings**.
   This applies to per-source endpoints and makes TVHeadend the stream client for those routes.
   For best stability with CSO-backed mux paths, also enable **Use CSO stream buffer for TVHeadend mux streams**.
   Ensure **TVHeadend Settings** has an FFmpeg stream buffer enabled, for example:

```bash
-hide_banner -loglevel error -probesize 10M -analyzeduration 0 -fpsprobesize 0 -i [URL] -c copy -metadata service_name=[SERVICE_NAME] -f mpegts pipe:1
```

2. **Option #2:** Enable **Use HLS Proxy** and **Enable FFmpeg remux** on the source in **Stream Source Settings** so HLS sources are remuxed to MPEG-TS.

:::

1. Navigate to the **Live TV & DVR** page in Plex.
2. Click **Set Up Plex Tuner**.

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
