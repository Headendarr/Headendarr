---
title: TVHeadend Integration
---

# TVHeadend Integration

A core feature of the recommended All-in-One (AIO) installation is the built-in, pre-configured TVHeadend server. While TIC manages the channels and EPG, TVHeadend does the heavy lifting of streaming, DVR, and client management.

## What is TVHeadend?

TVHeadend is a powerful, open-source TV streaming server and digital video recorder (DVR). It is a mature and stable project with a huge range of features and excellent compatibility with a vast number of clients.

By integrating TVHeadend, TIC can offer a robust and reliable backend for delivering your IPTV streams.

## How TIC uses TVHeadend

When you use the AIO installation, TIC automatically configures and manages the internal TVHeadend instance:

-   **Channel & EPG Sync**: When you map channels and sync them in TIC, it automatically creates the necessary configuration in TVHeadend.
-   **Streaming**: TVHeadend handles the actual streaming of channels to your clients via the HTSP protocol.
-   **DVR**: All DVR scheduling and recording is managed by TVHeadend's robust recording engine.
-   **Client Management**: TVHeadend manages connections from clients like Kodi, ensuring smooth streaming.

In short, TIC acts as the "brain" for configuration, while TVHeadend acts as the "engine" for delivery.

## Accessing the TVHeadend UI

You can access the full TVHeadend UI through the **SHOW TVHEADEND BACKEND** button in the header. This will open a pop-up that shows you the full TVHeadend UI that you can interact with. You can click on a button at the top right-hand corner of the pop-up to open this in a new window for full access and to adjust advanced settings.

:::note Important TVHeadend Settings
Some TVHeadend settings are actively managed and overwritten by TIC. Specifically:
*   Any networks prefixed with `tic-` will be managed by TIC.
*   Any Muxes prefixed with `tic-` will be managed by TIC.
*   Streaming profiles and recording profiles prefixed with usernames are managed by TIC.

Changes made to these specific items directly within the TVHeadend UI may be overwritten by TIC to maintain consistency and proper operation.
:::

### What can you do in the TVHeadend UI?

For the most part, you should not need to change settings in the TVHeadend UI directly, as TIC will manage the important parts for you. However, it can be useful for:

-   **Viewing Stream Statistics**: See who is watching what and check for any stream errors.
-   **Checking Logs**: The TVHeadend log is useful for diagnosing connection or streaming problems.
-   **Advanced EPG/DVR Settings**: Accessing more granular settings for the EPG and DVR that are not exposed in the TIC interface.
-   **Creating Client Users**: Creating specific user accounts for client applications with different levels of access.

:::warning
It is recommended to let TIC manage the core channel, mux, and EPG configuration. Manually changing these settings in the TVHeadend UI can cause conflicts with the settings synced from TIC.
:::

## Why use TVHeadend?

-   **Stability**: TVHeadend is exceptionally stable and designed for 24/7 operation.
-   **Compatibility**: It supports a huge range of clients via the HTSP protocol.
-   **Performance**: It is highly optimized for streaming and recording multiple channels simultaneously.
-   **Feature-Rich**: It includes a powerful DVR, time-shifting capabilities, and detailed stream analysis tools.

By leveraging the power of TVHeadend, TIC provides a best-in-class experience for managing and watching your IPTV content.
