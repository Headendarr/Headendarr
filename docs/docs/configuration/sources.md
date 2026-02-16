---
title: Sources
---

# Channel Sources (M3U / XC)

"Sources" are your primary source for channel streams, which can be M3U playlists or Xtream Codes (XC) providers. This page is where you will add and manage them.

## Adding a New Source

1.  Navigate to the **Sources** page from the side menu.
2.  Click the **+ ADD SOURCE** button to open the "Add New Source" dialog.
3.  Select the **Source Type**:
    -   **M3U URL**: For standard `.m3u` or `.m3u8` playlist files.
    -   **XC Provider**: For IPTV providers that use the Xtream Codes API.

#### M3U URL Options

-   **Name**: A friendly name for this source (e.g., "My IPTV Provider").
-   **M3U URL**: The full URL to the `.m3u` or `.m3u8` file.
-   **Connection Limit**: The maximum number of concurrent connections allowed for this source. This is crucial for respecting provider limits and enabling stream failover.
-   **User Agent**: Select a user agent if your provider requires a specific one.

#### XC Provider Options

-   **Name**: A friendly name for this source.
-   **Host**: The server address, including the protocol and port (e.g., `http://provider.com:8080`).
-   **Username**: Your username for the XC service.
-   **Password**: Your password for the XC service.
-   **Connection Limit**: The maximum number of concurrent connections allowed for this source.
-   **User Agent**: Select a user agent if your provider requires a specific one.

:::note Increasing XC Connection Limits
For Xtreme Codes providers, you can configure a single source with multiple login credentials (i.e., different usernames and passwords for the same host) to effectively increase your concurrent connection limits. This is more efficient than creating separate sources for each login. Headendarr will automatically manage these connections, dynamically utilizing available credentials as needed to meet demand. This allows for flexible scaling of connections throughout the year without requiring multiple source entries.
:::

## Refreshing Sources

After adding or modifying a source, you must fetch its content to make its channels available for mapping.

-   **To refresh a single source**: Click the "Refresh" icon next to the source in the list.
-   **To refresh all sources**: Click the "Refresh All" button at the top of the page.

:::tip Manual Refresh Recommended
Headendarr will periodically refresh your sources in the background. However, it is **highly recommended** to perform a manual refresh after adding a new source to make its channels available for mapping immediately.
:::

You can monitor the refresh progress in the "Task Manager" at the top right of the UI. Once complete, the "Available Channels" count for the source will update.

---

Once you have added and refreshed your sources, you can proceed to add [EPG Sources](./epgs.md) and then [configure your channels](./channels.md).
