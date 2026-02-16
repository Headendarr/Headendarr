---
title: EPGs
---

# EPGs (XMLTV)

EPG (Electronic Programme Guide) sources provide the guide data (e.g., show titles, descriptions, and times) for your channels. This data is typically in the XMLTV format.

## Adding a New EPG

1.  Navigate to the **EPGs** page from the side menu.
2.  Click the **+ ADD EPG** button to open the "Add New EPG" dialog.
3.  Fill in the details:
    -   **Name**: A friendly name for this EPG source (e.g., "My EPG Source").
    -   **XMLTV URL**: The full URL to the `.xml` or `.xml.gz` file.

## Refreshing EPGs

Just like sources, EPGs need to be refreshed to load their guide data.

-   **To refresh a single EPG**: Click the "Refresh" icon next to the EPG in the list.
-   **To refresh all EPGs**: Click the "Refresh All" button at the top of the page.

:::tip Manual Refresh Recommended
Headendarr will periodically refresh your EPG sources in the background. However, it is **highly recommended** to perform a manual refresh after adding a new EPG source to make its data available for assignment immediately.
:::

After a successful refresh, the "Available Channels" and "Available Programmes" counts for the EPG will update, and the EPG data will be ready to be assigned to your mapped channels on the [Channels page](./channels.md).
