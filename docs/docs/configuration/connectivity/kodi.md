---
title: Kodi
---

# Kodi

Use this guide to connect Kodi to Headendarr for live channels and curated VOD.

Kodi is one of the strongest Headendarr front ends if you want a classic living-room media centre experience. With the right setup, you can get a proper TV guide, channel logos, recordings, timeshift support, and a separate browseable VOD library that feels closer to a normal media collection than a raw IPTV feed.

## Option #1 (recommended): TVHeadend HTSP Client addon

This is the recommended way to connect Kodi for live channels.

If you want Kodi to feel like a polished TV and DVR frontend, use this option.

Kodi's TVHeadend addon gives you the full PVR-style experience, including:

- channel list and guide data
- channel logos
- DVR access
- TVHeadend-backed timeshift support

In practice, this is the setup that feels the most complete and the most natural to use day to day.

### Steps

1. Install the **TVHeadend HTSP Client** addon in Kodi.
2. In Headendarr **Users**, create or choose a user for Kodi.
3. Use the following connection details in the Kodi addon:
   1. **Hostname / IP address**: the Headendarr server address
   2. **HTSP port**: `9982`
   3. **Username**: the Headendarr username
   4. **Password**: the user's **stream key**
4. Save the addon settings and let Kodi connect.

This is the same HTSP connection method described in [Connectivity Methods](/configuration/connectivity/methods/), and it is the preferred Kodi setup for the best live TV experience.

## Option #2: IPTV Simple Client

If you do not want to use the TVHeadend addon, Kodi can also use Headendarr via the **PVR IPTV Simple Client** addon.

This route is usually easier to understand because it uses a playlist plus guide URL, but it does not provide the same level of integration as the TVHeadend addon.

Choose this option if you specifically want a straightforward M3U + XMLTV setup. Choose the TVHeadend addon if you want the richer, more TV-like Kodi experience.

### Steps

1. Install the **PVR IPTV Simple Client** addon in Kodi.
2. In Headendarr, open **Show connection details**.
3. Copy:
   1. the **Combined Playlist** URL
   2. the **XMLTV Guide** URL
4. Configure the IPTV Simple Client addon with:
   1. the M3U playlist URL
   2. the XMLTV guide URL
5. Save the addon settings and refresh channels.

Use the combined playlist unless you have a specific reason to manage multiple per-source playlists yourself.

## Video On Demand (VOD)

This is the best option if you want Kodi to browse curated VOD over HTTP as a library source.

With this approach, Headendarr exposes curated VOD categories through a browsable HTTP directory using Basic auth, with the library root at:

`/tic-api/library/`

### How it works

Kodi connects to `/tic-api/library/` using HTTP Basic auth, with the Headendarr streaming username as the username and that user's stream key as the password. Once authenticated, Kodi can browse the exported `Movies/<category-slug>/...` and `Shows/<category-slug>/...` structure and use its normal online scrapers for artwork and metadata.

The result is a much cleaner VOD experience than treating everything like a flat playlist. Users can browse organised movie and show folders while still letting Kodi present the content like a normal library.

### Recommended Kodi settings

:::warning Disable Kodi file probing for VOD libraries
Kodi should not inspect the video files directly during library scans.

If Kodi probes the media files for metadata, artwork, or thumbnails, it can rapidly consume upstream XC connection limits.

Switch Kodi's settings level to **Advanced**, then disable all of the following:

- **Use video tags**
- **Extract video information from files**
- **Extract chapter thumbnails**
- **Extract thumbnails from video files**

Leaving these enabled can cause Kodi to open many VOD files during a scan, which can exhaust connection limits on the upstream source.
:::

[![Kodi advanced video settings showing extraction options disabled](/img/screenshots/kodi-setup-disable-video-file-extraction-1.png)](/img/screenshots/kodi-setup-disable-video-file-extraction-1.png)

[![Kodi advanced video settings showing extraction options disabled](/img/screenshots/kodi-setup-disable-video-file-extraction-2.png)](/img/screenshots/kodi-setup-disable-video-file-extraction-2.png)

### Steps

In Headendarr:

1. On the VOD category, enable **Expose HTTP Library**.
2. In Headendarr **Users**, choose a user with VOD access to the relevant content.
3. For series categories, make sure episode metadata has already been populated in Headendarr so the HTTP library can expose the show and season structure correctly.

In Kodi:

1. Go to **Videos**.
2. Select **Add video source**.
3. Click **Browse**.
4. Choose **Add network location...**

Then fill in the network location details:

- **Protocol**: `Web server directory (HTTP)`
  - If Headendarr is behind a reverse proxy with TLS, use `Web server directory (HTTPS)` instead.
- **Server address**: the Headendarr hostname or IP address
- **Port**:
  - `9985` for a default local Headendarr install
  - `80` or `443` if you are using a reverse proxy
- **Remote path**: `tic-api/library`
- **Username**: the Headendarr streaming username
- **Password**: the user's stream key

After entering those values, click **OK**.

[![Kodi Add network location dialog configured for Headendarr](/img/screenshots/kodi-setup-add-network-location.png)](/img/screenshots/kodi-setup-add-network-location.png)

Then complete the source setup in Kodi:

1. Choose a source name that makes sense to you, for example `Headendarr Movies` or `Headendarr Shows`.
2. Select the content type that matches the path you are adding:
   1. use a movie scraper for `Movies/...`
   2. use a TV show scraper for `Shows/...`
3. Review the scraper options and keep file-based metadata features disabled.
4. Save the source and let Kodi scan it into the library.

If you want both films and series, add them as separate Kodi sources so each one can use the correct content type and scraper.

### Example values

For a local install without a reverse proxy:

- **Protocol**: `Web server directory (HTTP)`
- **Server address**: `192.168.1.117`
- **Port**: `9985`
- **Remote path**: `tic-api/library`

That corresponds to the browsable library root:

`http://192.168.1.117:9985/tic-api/library/`

### Scraper behaviour

For the best results:

- keep local metadata extraction disabled in Kodi
- let Kodi scrape artwork and metadata from TMDb and other online scrapers
- avoid enabling options that inspect the media files directly

The HTTP library is intended to provide a stable, scraper-friendly structure for Kodi, not to act as a source for Kodi-side file probing.
