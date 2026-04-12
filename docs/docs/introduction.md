---
sidebar_position: 1
title: Introduction
---

# Introduction

Welcome to Headendarr, a powerful and user-friendly tool designed to simplify the management of IPTV channels, EPG data, and curated VOD libraries for use with TVHeadend and other IPTV clients.

Headendarr acts as a central hub for your IPTV sources, allowing you to map channels from multiple M3U playlists and assign Electronic Programme Guide (EPG) data from various XMLTV sources. It can also import VOD catalogues from compatible providers and turn them into cleaner, curated libraries for your users. The result is a stable, curated TV and VOD experience that can be consumed by TVHeadend and a wide range of compatible clients.

## Key Features

- **Centralised IPTV Management**: Manage all your IPTV playlists and EPG sources in one place.
- **Flexible Channel Mapping**: Easily map channels from your sources to create a customised channel lineup.
- **Powerful EPG Integration**: Assign EPG data to your channels from multiple XMLTV sources.
- **VOD 24/7 Channels**: Create custom linear channels from your imported VOD pool with synthetic guide data and connection-saving caching. [Learn more...](/configuration/vod-24-7-channels)
- **Curated VOD Libraries**: Import VOD from compatible providers, organise it into cleaner categories, and publish it in ways that suit different clients.
- **Embedded TVHeadend**: The recommended All-in-One (AIO) installation includes a pre-configured TVHeadend instance, providing a robust and widely compatible backend for streaming and DVR.
- **Multiple Client Options**:
  - **HTSP**: Connect to the built-in TVHeadend server using clients like Kodi, Tvheadend HTSP Client, and more for a stable, full-featured experience.
  - **M3U Playlist**: Access your channel list via a generated M3U playlist URL, compatible with clients like VLC, Tivimate, and IPTV Smarters.
  - **HDHomeRun Emulation**: Emulate an HDHomeRun device, allowing clients like Plex, Emby, and Channels DVR to discover and use your channels.
  - **Xtream Codes API**: Emulate an Xtream Codes server, allowing IPTV apps and set-top boxes that connect to Xtream Codes platforms to access your live channels, EPG, and curated VOD.
- **Library and App Compatibility for VOD**:
  - **Jellyfin / Emby**: Publish VOD as `.strm` libraries that media servers can scan like normal film and series collections.
  - **Kodi**: Expose VOD through a browsable HTTP library so Kodi can present it like a media library instead of a flat IPTV list.
- **User Management**: Create and manage multiple user accounts with granular permissions, including whether each user can access live TV, VOD, both, or neither.
- **DVR Functionality**: Schedule and manage recordings directly within the application (when using the integrated TVHeadend).

## How It Works

Headendarr bridges the gap between raw, often unreliable IPTV sources and your end clients.

1.  **Add Sources**: You add your M3U playlists and XMLTV EPG files as sources.
2.  **Map Channels**: You browse the channels available from your sources and "map" the ones you want into your lineup.
3.  **Assign EPG**: You link the mapped channels to the corresponding EPG data from your XMLTV sources.
4.  **Publish TV and VOD**: Headendarr exposes your curated content to clients in several ways:
    - It can automatically configure the built-in TVHeadend server, which then provides streams via the robust HTSP protocol.
    - It can generate a custom M3U playlist file for clients to consume directly via HTTP.
    - It can pretend to be an HDHomeRun tuner, making your channels available to a wide range of DVR software.
    - It can expose curated VOD through Xtream Codes compatibility, `.strm` library exports, or an authenticated HTTP library depending on the client you want to support.

## Operating Modes

Headendarr can be run in two modes:

- **All-in-One (AIO)**: This is the **recommended** mode. It runs the Headendarr web interface and a dedicated TVHeadend instance in a single, easy-to-manage container. This guide will focus primarily on this mode.
- **Side-TVH**: In this mode, Headendarr operates as a standalone container, connecting to an existing TVHeadend installation. This advanced setup is typically for users with a pre-configured TVHeadend. Configuration details for Side-TVH are not covered in this documentation; please join our Discord server for support.
