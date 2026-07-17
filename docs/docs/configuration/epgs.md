---
title: EPGs
---

# EPGs (XMLTV)

EPG (Electronic Programme Guide) sources provide the guide data (e.g., show titles, descriptions, and times) for your channels. This data is typically in the XMLTV format.

## Adding a New EPG

1.  Navigate to the **EPGs** page from the side menu.
2.  Click the **+ ADD EPG** button to open the "Add New EPG" dialog.
3.  Fill in the details:
    - **Name**: A friendly name for this EPG source (e.g., "My EPG Source").
    - **XMLTV URL**:
      - HTTP/HTTPS sources: the full URL to the `.xml` or `.xml.gz` file.
      - Local executable sources: a `file://` path to a local script or binary that prints XMLTV data to stdout.
        Example: `file:///config/scripts/fetch-guide.sh`
    - **User Agent**: Optional User-Agent used for HTTP/HTTPS fetches.
    - **Update schedule**: Controls how often Headendarr should refresh this EPG automatically.

:::info Local executable EPG sources
`file://` EPG sources are executed locally by Headendarr.

- The path must point to an executable file.
- The executable must write valid XMLTV data to stdout.
- Headendarr caches that stdout to its local EPG cache before importing it.
- User-Agent selection does not apply to `file://` sources.

:::

## Refreshing EPGs

Just like sources, EPGs need to be refreshed to load their guide data.

- **To refresh a single EPG**: Click the "Refresh" icon next to the EPG in the list.
- **To refresh all EPGs**: Click the "Refresh All" button at the top of the page.

:::tip Manual Refresh Recommended
Headendarr will periodically refresh your EPG sources in the background. However, it is **highly recommended** to perform a manual refresh after adding a new EPG source to make its data available for assignment immediately.
:::

After a successful refresh, the "Available Channels" and "Available Programmes" counts for the EPG will update, and the EPG data will be ready to be assigned to your mapped channels on the [Channels page](./channels.md).

## Download Size Limit

Headendarr limits the amount of data accepted from each HTTP/HTTPS or `file://` EPG source. The default maximum raw download size is **3 GiB** (`3221225472` bytes).

Advanced users can change this limit with the `EPG_DOWNLOAD_MAX_BYTES` environment variable on the Headendarr container. The value must be a positive integer expressed in bytes:

```yaml
services:
  headendarr:
    environment:
      EPG_DOWNLOAD_MAX_BYTES: "3221225472"
```

Restart the Headendarr container after changing the value. This setting applies to the downloaded response before gzip decompression and to XMLTV output produced by a local executable source. Gzip-expanded XMLTV data has a separate fixed safety limit of **2 GiB**.

If a source exceeds the configured limit, the refresh fails without retrying, its partial download is removed, and any previously imported cache remains untouched. Headendarr marks the EPG as needing attention and tries it again at its next configured update interval.

## Additional EPG Metadata

The **EPGs** page also includes optional guide-enrichment settings that run after an EPG refresh.

- **Fetch missing data from TMDB**:
  - Fills in missing programme metadata such as artwork, descriptions, and episode details when possible.
  - Requires `TMDB_API_KEY` to be configured in the Headendarr container environment.
  - Get your TMDB credential from `https://www.themoviedb.org/settings/api`.
  - Headendarr supports either the **API Read Access Token** or the **API Key** in `TMDB_API_KEY`.
  - Results are cached between refreshes so Headendarr does not have to look up the same programme repeatedly.
- **Attempt to fetch missing programme images from Google Image Search**:
  - Only intended as a fallback for missing artwork.
  - Can generate a high volume of external requests and may cause your IP to be flagged as bot traffic.

:::warning Refresh duration
TMDB enrichment and Google image fallback can make EPG refreshes noticeably slower on large guides.
:::

:::caution TMDB API key configuration
Configure `TMDB_API_KEY` in your container environment and restart Headendarr before enabling TMDB enrichment.

Set it to either your TMDB **API Read Access Token** or your TMDB **API Key** from `https://www.themoviedb.org/settings/api`.
:::

## Reviewing Imported Guide Coverage

Use the EPG **Review** action (from the row actions menu) to validate imported guide coverage without opening a full TV guide.

- The **Review** action is only enabled after that EPG has:
  - completed at least one successful update/import, and
  - imported channel/programme data into the database.
- The dialog is designed for very large guides:
  - lazy-loaded channel list (infinite scroll),
  - channel search,
  - coverage filter (**All**, **With guide data**, **Without guide data**).
- For each channel, you can quickly see:
  - programme count from now into the future,
  - total imported programme count,
  - what is on now,
  - what is coming up next,
  - approximate future coverage horizon.

This helps identify channels that currently have no guide data and need source/mapping checks.
