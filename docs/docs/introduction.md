---
sidebar_position: 1
title: Introduction
---

# Introduction

Welcome to TVH-IPTV-Config (TIC), a powerful and user-friendly tool designed to simplify the management of IPTV channels and EPG data for use with TVHeadend and other IPTV clients.

TIC acts as a central hub for your IPTV sources, allowing you to map channels from multiple M3U playlists and assign Electronic Program Guide (EPG) data from various XMLTV sources. It then generates a stable, curated channel list that can be easily consumed by TVHeadend or any client that supports the M3U or HDHomeRun protocols.

## Key Features

- **Centralized IPTV Management**: Manage all your IPTV playlists and EPG sources in one place.
- **Flexible Channel Mapping**: Easily map channels from your sources to create a customized channel lineup.
- **Powerful EPG Integration**: Assign EPG data to your channels from multiple XMLTV sources.
- **Embedded TVHeadend**: The recommended All-in-One (AIO) installation includes a pre-configured TVHeadend instance, providing a robust and widely compatible backend for streaming and DVR.
- **Multiple Client Options**:
    - **HTSP**: Connect to the built-in TVHeadend server using clients like Kodi, Tvheadend HTSP Client, and more for a stable, full-featured experience.
    - **M3U Playlist**: Access your channel list via a generated M3U playlist URL, compatible with clients like VLC, Tivimate, and IPTV Smarters.
    - **HDHomeRun Emulation**: Emulate an HDHomeRun device, allowing clients like Plex, Emby, and Channels DVR to discover and use your channels.
    - **Xtreme Codes API**: Emulate an Xtreme Codes server, allowing IPTV apps and set-top boxes that connect to Xtreme Codes platforms to access your channel lineup and EPG.
- **User Management**: Create and manage multiple user accounts with granular permissions.
- **DVR Functionality**: Schedule and manage recordings directly within the application (when using the integrated TVHeadend).

## How It Works

TIC bridges the gap between raw, often unreliable IPTV sources and your end clients.

1.  **Add Sources**: You add your M3U playlists and XMLTV EPG files as sources.
2.  **Map Channels**: You browse the channels available from your sources and "map" the ones you want into your lineup.
3.  **Assign EPG**: You link the mapped channels to the corresponding EPG data from your XMLTV sources.
4.  **Connect Clients**: TIC exposes your curated channel list to clients in several ways:
    *   It can automatically configure the built-in TVHeadend server, which then provides streams via the robust HTSP protocol.
    *   It can generate a custom M3U playlist file for clients to consume directly via HTTP.
    *   It can pretend to be an HDHomeRun tuner, making your channels available to a wide range of DVR software.

## Operating Modes

TIC can be run in two modes:

-   **All-in-One (AIO)**: This is the **recommended** mode. It runs the TIC web interface and a dedicated TVHeadend instance in a single, easy-to-manage container. This guide will focus primarily on this mode.
-   **Side-TVH**: In this mode, TIC operates as a standalone container, connecting to an existing TVHeadend installation. This advanced setup is typically for users with a pre-configured TVHeadend. Configuration details for Side-TVH are not covered in this documentation; please join our Discord server for support.
