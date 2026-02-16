---
title: Unraid
draft: true
---

:::info Work In Progress
This documentation for TrueNAS Scale installation is currently a work in progress and may not be fully accurate or complete.
:::

# Unraid Installation

Installing Headendarr on Unraid is straightforward using the Community Applications plugin.

This guide assumes you have the **Community Applications** plugin installed on your Unraid server.

## Installing from Community Applications

1.  Navigate to the **Apps** tab in your Unraid web interface.
2.  In the search box, type `Headendarr` and press Enter.
3.  Locate the official `tvh-iptv-config` application in the search results and click the **Install** button.

## Template Configuration

Unraid will present you with the Docker container template to configure. Here are the key settings you need to review:

| Parameter               | Description                                                                                                                                                                                               |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Name**                | `tvh-iptv-config` (or your preferred name).                                                                                                                                                               |
| **Repository**          | `ghcr.io/headendarr/headendarr-lite:latest`                                                                                                                                                                    |
| **Network Type**        | `Bridge` is the standard setting and will work for most users.                                                                                                                                            |
| **WebUI**               | The default is `http://[IP]:[PORT:9985]`. This link will take you to the Headendarr web interface.                                                                                                                |
| **Port: 9985 (TCP)**    | **Headendarr Web UI**: The main interface for the application.                                                                                                                                                   |
| **Port: 9981 (TCP)**    | **TVHeadend Web UI**: The interface for the built-in TVHeadend server.                                                                                                                                    |
| **Port: 9982 (TCP)**    | **TVHeadend HTSP**: The port for clients like Kodi to connect to.                                                                                                                            |
| **Path: /config**       | **Required.** This is where all application data is stored. Set this to a path on your cache or array. For example: `/mnt/user/appdata/tvh-iptv-config`. **Do not lose this data.**                       |
| **Path: /recordings**   | **Required.** This is the destination for DVR recordings. Set this to a path on your array where you want to store media. For example: `/mnt/user/data/recordings`.                                       |

### Example Path Mappings:

-   **Host Path for `/config`**: `/mnt/user/appdata/tvh-iptv-config`
-   **Host Path for `/recordings`**: `/mnt/user/media/dvr`

After configuring the ports and paths, click **Apply** to save the settings and start the Docker container.

## Accessing the Application

Once the container is running, you can access the Headendarr Web UI by clicking the container's icon and selecting **WebUI**. The URL will be `http://<your-unraid-ip>:9985`.

:::warning Direct TVHeadend Access
It is **not recommended** to expose the TVHeadend Web UI (`9981`) or HTSP port (`9982`) directly to the internet. These services are best accessed within your local network, or securely remotely via a VPN like [Tailscale](https://tailscale.com/).
:::
