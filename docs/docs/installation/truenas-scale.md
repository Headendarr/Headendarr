---
title: TrueNAS Scale
draft: true
---

:::info Work In Progress
This documentation for TrueNAS Scale installation is currently a work in progress and may not be fully accurate or complete.
:::

# TrueNAS Scale Installation

Installing Headendarr on TrueNAS Scale can be done by adding the TrueCharts catalog and installing the application from there.

This guide assumes you have a working TrueNAS Scale installation.

## 1. Add TrueCharts Catalog

If you haven't already, you need to add the TrueCharts catalog to your TrueNAS Scale instance.

1.  Go to **Apps** in the TrueNAS UI.
2.  Click on **Manage Catalogs**.
3.  Click **Add Catalog**.
4.  Fill in the fields:
    -   **Catalog Name**: `truecharts`
    -   **Repository**: `https://github.com/truecharts/catalog`
    -   **Preferred Trains**: `stable`
    -   **Branch**: `main`
5.  Click **Save**. TrueNAS will take a few minutes to sync the new catalog.

## 2. Install the Application

1.  Go to the **Apps** page and click the **Available Applications** tab.
2.  Search for `tvh-iptv-config`.
3.  Click on the application and then click **Install**.
4.  This will take you to the installation wizard.

## 3. Configure Installation Settings

The TrueNAS Scale installation wizard will guide you through the setup. Here are the most important sections to configure:

### Application Name
-   Choose a name for your installation, e.g., `tvh-iptv-config`.

### Workload Settings
-   You can generally leave the default CPU and memory resources as they are unless you have specific needs.

### Networking and Services
-   **Web Port**: This will be the port for the Headendarr Web UI (e.g., `9985`).
-   **Other Ports**: You need to add and expose the other necessary ports for TVHeadend and HDHomeRun emulation.
    -   **TVHeadend UI Port**: `9981`
    -   **TVHeadend HTSP Port**: `9982`

### Storage
-   This is the most critical part of the configuration. You must define persistent storage for your configuration data and recordings.
-   **Config Storage**:
    -   Click **Add** next to "Additional Storage".
    -   Select `PVC (Persistent Volume Claim)`.
    -   Choose a `Mount Path` inside the container: `/config`.
    -   Create a new PVC and specify a size (e.g., 5-10 GiB is usually sufficient for configuration data).
    -   **This is where your database and settings live. Use a reliable dataset.**
-   **Recordings Storage**:
    -   Click **Add** again.
    -   This time, you can use a `Host Path` if you want to point directly to a dataset on your ZFS pool.
    -   Choose a `Mount Path` inside the container: `/recordings`.
    -   Select the `Host Path` on your server where you want recordings to be saved (e.g., `/mnt/MyPool/Media/DVR`).

### Review and Install
-   Review all your settings and click **Save** or **Install**. TrueNAS will now deploy the application.

## Accessing the Application

Once the deployment is complete, the **Apps** -> **Installed Applications** page will show `tvh-iptv-config` as `ACTIVE`. Click the **Portal** button to open the Headendarr Web UI in a new tab.

:::warning Direct TVHeadend Access
It is **not recommended** to expose the TVHeadend Web UI (`9981`) or HTSP port (`9982`) directly to the internet. These services are best accessed within your local network, or securely remotely via a VPN like [Tailscale](https://tailscale.com/).
:::
