---
title: Connectivity Methods
---

# Connectivity Methods

:::tip Accessing Client Links
For quick access to the URLs and connection details described below, simply click the "Show connection details" dropdown located in the header of the Headendarr frontend. This feature provides direct links you can copy and paste into your client applications.
:::

Headendarr is a powerful "man-in-the-middle", and it offers several ways for client applications to access your curated channel list. The best method depends on the client you are using and the features you need.

## Method 1: TVHeadend HTSP (Recommended)

Connecting directly to the integrated TVHeadend server via HTSP (Home TV Streaming Protocol) is the most robust and feature-rich method. It provides a native PVR experience and is the only method that supports server-side timeshifting.

| Supported Features    |                                                            |
| :-------------------- | :--------------------------------------------------------- |
| **EPG / Guide**       | ✅ Full and automatic                                      |
| **Channel Logos**     | ✅ Yes                                                     |
| **Picon Support**     | ✅ Yes (via TVHeadend configuration)                       |
| **DVR**               | ✅ Full scheduling and management                          |
| **Timeshift**         | ✅ Server-Side                                             |
| **Connection Limits** | ✅ Yes (Enforced by TVHeadend)                             |
| **Stability**         | Excellent                                                  |

### Recommended Clients

-   **Android TV**:
    -   **Sparkle TV / PVR Live**: Excellent, modern apps that integrate directly with the Android TV "Live Channels" feature for a seamless "cable TV" experience.
    -   **Kodi**: The classic media centre with the "TVHeadend HTSP Client" addon.
-   **Android Mobile**:
    -   **TVHClient**: A great app for mobile devices to stream from and manage your TVHeadend server.
-   **Desktop**:
    -   **Kodi**: The desktop version provides the same great experience.

### Connection Details

Headendarr automatically creates users in the TVHeadend backend. To connect your HTSP client, use the following credentials:

-   **TVHeadend Hostname/IP**: The IP address of your Docker host.
-   **HTSP Port**: `9982`
-   **Username**: Your username in Headendarr.
-   **Password**: Your **Streaming Key** from the Headendarr Users page.

:::tip
Your main Headendarr password is **only** for logging into the web interface. Your **Streaming Key** is the password for all client applications (HTSP, XC API, etc.).
:::

---

## Method 2: Combined Playlist

This method provides a single M3U playlist and XMLTV URL containing all mapped channels.

| Supported Features    |                                                            |
| :-------------------- | :--------------------------------------------------------- |
| **EPG / Guide**       | ✅ Manual URL entry required                               |
| **Channel Logos**     | ✅ Yes (Headendarr-managed and cached)                            |
| **DVR**               | ❌ Not supported (client must have its own DVR)            |
| **Timeshift**         | ❌ Not supported (client must have its own Timeshift)      |
| **Connection Limits** | ❌ No                                                      |
| **Stability**         | Good (can depend on the client)                            |

### Recommended Clients
- **TiviMate**: A popular and highly customizable IPTV client for Android TV.
- **VLC**: A simple way to test streams on a desktop.
- Any client that does not support adding multiple M3U playlists as separate sources.

### Connection Details

-   **Combined Playlist URL**: `http://<your-ip>:9985/tic-api/playlist/combined.m3u?stream_key=<user_stream_key>`
-   **XMLTV EPG URL**: `http://<your-ip>:9985/tic-api/epg/xmltv.xml?stream_key=<user_stream_key>`
-   **Optional profile override**: append `&profile=<profile_id>` (example: `profile=h264-aac-mpegts`).

---

## Method 3: Per-Source M3U Playlists

This method generates a unique M3U playlist for each of your channel sources. Its primary advantage is that it allows Headendarr to properly enforce the connection limits you have set for each source.

| Supported Features    |                                                            |
| :-------------------- | :--------------------------------------------------------- |
| **EPG / Guide**       | ✅ Manual URL entry required                               |
| **Channel Logos**     | ✅ Yes (Headendarr-managed and cached)                            |
| **DVR**               | ❌ Not supported (client must have its own DVR)            |
| **Timeshift**         | ❌ Not supported (client must have its own Timeshift)      |
| **Connection Limits** | ✅ Yes                                                     |
| **Stability**         | Good (can depend on the client)                            |

### Recommended Clients
- **Jellyfin / Emby**: By adding each per-source playlist as a separate tuner, you can ensure you don't exceed connection limits from any single provider.
- Any client that supports adding multiple M3U playlists.

### Connection Details

:::tip Easy Copy
You can quickly copy the specific URL for each source from the "Show connection details" dropdown in the Headendarr frontend.
:::

-   **Per-Source M3U URL**: `http://<your-ip>:9985/tic-api/playlist/<source_id>.m3u?stream_key=<user_stream_key>`
-   **Optional profile override**: append `&profile=<profile_id>`.

You can find the `<source_id>` by navigating to the **Sources** page in Headendarr and viewing the ID column.

---

## Method 4: HDHomeRun Emulation

Headendarr can pretend to be an HDHomeRun network tuner. This is an excellent method for integrating IPTV into DVR software that has native support for HDHomeRun devices. This method also enforces connection limits.

| Supported Features    |                                                            |
| :-------------------- | :--------------------------------------------------------- |
| **EPG / Guide**       | ✅ Automatic via discovery                                 |
| **Channel Logos**     | ✅ Yes (Headendarr-managed and cached)                            |
| **DVR**               | ❌ Not supported (managed by client, e.g., Plex)             |
| **Timeshift**         | ❌ Not supported (managed by client, e.g., Plex)   |
| **Connection Limits** | ✅ Yes                                                     |
| **Stability**         | Excellent                                                  |

### Recommended Clients
- **Plex**: The primary choice for HDHomeRun emulation for its Live TV & DVR feature.
- **Emby / Jellyfin**: Both can also discover and use HDHomeRun tuners.
- **Channels DVR**: A premium DVR service that works well with HDHomeRun tuners.

### How It Works

:::tip Easy Copy
You can quickly copy the specific HDHomeRun Device URL for each source from the "Show connection details" dropdown in the Headendarr frontend.
:::

For clients like Plex, the most reliable way to add the emulated tuner is to do it manually by providing its network address.

While some clients may support auto-discovery, this can be unreliable in Docker environments. Manually adding the tuner ensures the client can find it. Provide the following URL to your client when it asks for a tuner's address:

- **Combined HDHomeRun Device URL**: `http://<your-ip>:9985/tic-api/hdhr_device/<user_stream_key>/combined`
- **Combined HDHomeRun Device URL (fixed profile)**: `http://<your-ip>:9985/tic-api/hdhr_device/<user_stream_key>/combined/<profile>`
- **HDHomeRun Device URL**: `http://<your-ip>:9985/tic-api/hdhr_device/<user_stream_key>/<source_id>`

Use the combined URL when you want one tuner endpoint that includes all channels. Use the per-source URL when you want source-specific connection-limit behavior.
Use the profile-scoped combined URL when your client (for example Plex) does not support selecting a stream profile in HDHR requests and you want to force a profile such as `matroska`.

You can find the `<source_id>` by going to the **Sources** page and viewing the ID column. Your client will use the selected base URL to discover `device.xml`, `lineup.json`, and other required endpoints.

---

## Method 5: Xtream Codes (XC) API

For clients designed for Xtream Codes, Headendarr provides a compatibility layer.

| Supported Features    |                                                            |
| :-------------------- | :--------------------------------------------------------- |
| **EPG / Guide**       | ✅ Automatic                                               |
| **Channel Logos**     | ✅ Yes (Headendarr-managed and cached)                            |
| **DVR**               | ❌ Not supported (client may have its own)                 |
| **Timeshift**         | ❌ Not supported (client must have its own Timeshift)      |
| **Connection Limits** | ❌ No                                                      |
| **Stability**         | Good                                                       |

### Recommended Clients
- **TiviMate**: Supports XC login natively and is a popular choice.
- **IPTV Smarters Pro** and similar apps.

### Connection Details

- **Server URL / Host**: `http://<your-ip>:9985`
- **Username**: Your username in Headendarr.
- **Password**: Your **Streaming Key** from the Headendarr Users page.

Compatibility routes:

- `http://<your-ip>:9985/get.php?username=<username>&password=<stream_key>`
- `http://<your-ip>:9985/xmltv.php?username=<username>&password=<stream_key>`

These map to the same data as `/tic-api/playlist/combined.m3u` and `/tic-api/epg/xmltv.xml`.

---

## Security Warning

It is **strongly recommended** that you only expose the Headendarr Web UI (`9985`) to the internet. These endpoints are secured with strict authentication.

**Do not expose the TVHeadend UI (`9981`) or HTSP port (`9982`) directly to the internet.** Doing so can be a significant security risk. If you need remote access, consider using a VPN like [Tailscale](https://tailscale.com/) to connect to your home network.
