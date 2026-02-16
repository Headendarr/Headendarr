---
title: Channels
---

# Channel Configuration

This is the core of Headendarr, where you build your final channel lineup. The **Channels** page allows you to add channels, assign streams from your sources, link EPG data, and organize everything to your liking.

## Adding and Editing Channels

On the **Channels** page, there are two distinct buttons for adding channels, each with a different purpose.

### Method 1: Bulk Import via "Import channels from stream source"

This is the most powerful and recommended method for adding channels.

1.  Click the **Import channels from stream source** button.
2.  A dialog will appear, allowing you to search and filter through every channel available from all your configured sources. This is efficient for navigating hundreds of thousands of potential channels.
3.  Select one or more channels from the list.
4.  Click **Add**.

Headendarr will automatically create new channels, populating the name, logo, and stream source. It will also attempt to intelligently link EPG data by matching the channel's name to an EPG entry, if available.

### Method 2: Manual Creation via "Add Channel"

This method is for creating a single, custom channel from scratch.

1.  Click the **Add Channel** button.
2.  An empty channel configuration dialog will appear.
3.  You must manually fill in all the details, including the Channel Name, Channel Number, Stream Sources, Logo URL, EPG link, and any tags.

### Editing a Channel

After creating a channel using either method, you can click on it in the main list at any time to open the edit dialog and fix up any missing information or change its configuration.

## Stream Priority and Failover

Headendarr allows you to add **multiple stream sources** to a single channel. This creates a powerful failover system.

-   **Priority**: Inside a channel's settings, the streams are listed in order of priority (from top to bottom). The stream at the top of the list will always be tried first.
-   **Failover**: If a client requests a channel, TVHeadend will try the highest priority stream. If that stream is unavailable, or if the connection limit for that source (as configured on the **Sources** page) has been reached, TVHeadend will automatically move to the next stream in the priority list.
-   **Reordering**: You can drag and drop streams within the channel's settings to change their priority.

## Organizing and Reordering

-   **Channel Numbering**: The channel list is ordered by channel number.
-   **Drag-and-Drop**: You can easily reorder your channels by dragging and dropping them within the main channel list. This will automatically update their channel numbers.

## Bulk Updating Channels

You can edit multiple channels at once to efficiently manage your lineup.

1.  On the **Channels** page, check the boxes next to the channels you wish to modify.
2.  A "Bulk Edit" menu will appear at the top of the list.
3.  From here, you can:
    -   **Add/Remove Tags**: Add or remove tags in bulk.
    -   **Refresh Streams**: This is a powerful feature. If your IPTV provider changes the underlying stream URLs in their master source, this option will pull in the updated URLs for all selected channels without you having to edit them one by one.
    -   **Enable/Disable**: Toggle the active status of the channels.
    -   **Delete**: Remove the channel mappings.

## Syncing with TVHeadend

After you have made changes to your channels, you must sync them with the TVHeadend backend.

-   Click the **Sync with TVH** button at the top of the Channels page.

This action pushes your entire channel lineup, including channel numbers, tags, stream priorities, and EPG links, to TVHeadend, making them available to your clients.
