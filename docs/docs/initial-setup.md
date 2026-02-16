---
sidebar_position: 3
title: Initial Setup & Workflow
---

# Initial Setup & Core Workflow

After a fresh installation, follow these steps to get your Headendarr instance configured and ready to serve channels.

## 1. First Login & Password Change

When you first access the Headendarr Web UI at `http://<your-ip>:9985`, you will be presented with a login page.

Log in with these credentials:

-   **Default Username**: `admin`
-   **Default Password**: `admin`

Your first and most important step is to change the default password.

1.  Navigate to **Users** from the side menu.
2.  You will see the `admin` user. On the right side of the user entry, click the **Reset password** button, which has a key/password icon.
3.  A dialog will appear prompting you to "Enter a new password".
4.  Enter a new, strong password and click **OK**.

The next time you log in with that admin user, you will need to use that new password. Do not forget it.

:::tip Resetting the Admin Password
If you ever forget your admin password or need to reset it for any reason, you can do so by creating a file named `.reset-admin-password` in the `/config` directory of your Headendarr container. The next time the container starts, the system will detect this file, reset the `admin` user's password back to `admin`, and then automatically delete the `.reset-admin-password` file.
:::

## 2. Review Settings

Before adding any sources, it's a good idea to familiarize yourself with the application's settings.

1.  Navigate to **TVHeadend** the side menu. Review the TVHeadend server specific config options.
2.  Navigate to **Settings** the side menu. Review the options and adjust as you see fit. See the [Application Settings](./configuration/application-settings) guide for a full explanation of each option.

## 3. Add a Channel Source

Channel Sources are the M3U or Xtream Codes providers for your channels.

1.  Navigate to the **Sources** page.
2.  Click the **+ ADD SOURCE** button and enter the details for your M3U or XC source.
3.  After saving the source, it will appear in the list.
4.  **You must now manually fetch the channel data.** Click the **Update** button of the new source. The **Upcoming background tasks** will show the progress.

## 4. Add an EPG Source

EPG sources provide the guide data for your channels.

1.  Navigate to the **EPGs** page.
2.  Click the **+ ADD EPG** button and enter the URL for your XMLTV guide data.
3.  After saving, you must also manually refresh the EPG source. Click the **Update** button of the new EPG.

## 5. Configure Channels

This is the core of the configuration where you build your channel lineup on the **Channels** page. There are two primary ways to add channels:

-   **Method 1: Bulk Import (Recommended)**
    -   Click the **IMPORT CHANNELS FROM STREAM SOURCE** button.
    -   A dialog will appear with powerful filtering and search options, allowing you to browse all available channels from all your sources.
    -   Select the channels you want to add. Headendarr will automatically create them, populating the logo, source, and attempting to link the EPG data if a match is found. This is the easiest way to add many channels.

-   **Method 2: Manual Add**
    -   Click the **+ ADD CHANNEL** button.
    -   This opens a dialog with blank fields, allowing you to create a channel from scratch by manually filling in all the details (name, number, stream sources, EPG, etc.).

After creating channels, you can click on any of them to **edit** and refine missing information.

### Channel Priority & Failover

-   You can drag and drop channels in the list to change their order and channel number.
-   Within a channel's settings, you can add multiple stream sources from different sources. The stream at the top of the list has the highest priority. If a client requests a channel and the top priority stream is unavailable or has reached its connection limit (as defined on the Sources page), TVHeadend will automatically try the next stream in the list.

### Bulk Editing

-   Select multiple channels to perform bulk actions, such as adding/removing tags, deleting channels, or **refreshing the stream URLs**. This is useful if your provider has changed the stream links in their master playlist.

## 6. Sync and Watch

Once your channels are configured, automatic background tasks will synck your config with the TVHeadend backend. This pushes all your configuration to the TVHeadend backend.

Your channels are now ready! You can use the **TV Guide** to see what's on, schedule recordings, and [connect your clients](./client-connectivity).
