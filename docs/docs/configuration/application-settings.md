---
title: Application Settings
---

# Application Settings

The **Settings** page allows you to configure global settings for TIC.

## UI Settings

These settings control the behavior and appearance of the TIC web interface.

-   **Highlight channels with source issues**: When enabled, the UI will display a warning highlight for channels that are linked to disabled sources or have failed TVHeadend muxes. This helps in quickly identifying problematic channels.
-   **Start page after login**: Choose the default page that users will land on immediately after signing in.
    *   **Options**: Dashboard, Sources, EPGs, Channels, TV Guide, DVR, Audit

## Connections

These settings define how TIC interacts with external services and how clients connect to TIC.

-   **TIC Host**: This is the external host and port that client applications (like M3U players, HDHomeRun emulators, etc.) use to reach your TIC instance. It is used to generate correct URLs for playlist, XMLTV, and HDHomeRun endpoints. Ensure this is set to an address accessible by your clients.
-   **Route playlists & HDHomeRun through TVHeadend**: When this setting is enabled, all playlist and HDHomeRun streams are routed through the integrated TVHeadend server. This allows TVHeadend to enforce its own stream policies (e.g., connection limits, buffering) and can improve compatibility with certain clients. When disabled, clients connect directly to TIC for streams.

## User Agents

This section allows you to define and manage custom User-Agent headers. These User-Agents are used by TIC when fetching data from your IPTV sources and EPG providers. Some providers may require a specific User-Agent to prevent blocking.

-   **Add User Agent**: Click this button to add a new custom User-Agent entry.
-   **Name**: A descriptive name for your User-Agent (e.g., "VLC Player", "My Custom UA").
-   **User-Agent**: The actual User-Agent string (e.g., `VLC/3.0.23 LibVLC/3.0.23`).

## DVR Settings

These settings control the default behavior for Digital Video Recorder (DVR) functionalities.

-   **Pre-recording padding (minutes)**: Specify the number of minutes to begin recording *before* a scheduled program's start time. This helps ensure that the beginning of a program is not missed due to scheduling inaccuracies.
-   **Post-recording padding (minutes)**: Specify the number of minutes to continue recording *after* a scheduled program's end time. This is useful for capturing content that runs slightly over its scheduled slot.
-   **Default recording retention**: Choose how long recorded programs will be kept by default before being automatically deleted. This policy applies to TVHeadend recording profiles synced by TIC.
    *   **Options**: 1 day, 3 days, 5 days, 1 week, 2 weeks, 3 weeks, 1 month, 2 months, 3 months, 6 months, 1 year, 2 years, 3 years, Maintained space, Forever.
-   **Recording Profiles**: These profiles define the file and folder naming conventions for your recorded content. The first profile in the list is treated as the default for users and as a fallback for scheduling.
    -   **Add Recording Profile**: Click this button to create a new recording profile.
    *   **Profile Name**: A friendly name for the recording profile (e.g., "Movies", "Kids Shows").
    *   **Pathname Format**: A format string that determines the directory and filename structure for recordings saved by this profile. Variables can be used to dynamically insert program details. (e.g., `$Q$n.$x` for `[Episode Name].[Extension]`).

## Audit Logging

These settings control the retention of audit logs within the TIC database.

-   **Audit log retention (days)**: Define the number of days that audit log entries will be stored in the database. Older entries will be automatically purged to manage database size.
