---
title: Video On Demand (VOD)
---

# Video On Demand (VOD)

Headendarr can import **Video On Demand (VOD)** content from **Xtream Codes (XC)** providers and turn it into a catalogue that feels like your own.

Instead of being stuck with whatever layout your provider gives you, you can decide how movies and shows should be organised, which categories are worth keeping, and how that content should be published to your users and apps.

With VOD enabled, Headendarr can:

- import the upstream XC movie and series catalogue
- let you build your own curated movie and show categories
- publish that curated catalogue through the XC-compatible API
- generate `.strm` files for library-based media apps
- expose a browsable HTTP library for clients such as Kodi

In other words, Headendarr sits between the upstream provider and your users, giving you control over how VOD content is presented and consumed.

## Why this is useful

Most VOD providers already organise their catalogue into useful categories, but they still tend to include a much broader mix of content than many people actually want to expose. That can mean extra languages, duplicated groupings, niche categories, or types of content such as animation that do not fit the library you want to build. Curating your VOD catalogue lets you keep the valuable structure from the provider while trimming it down into a smaller, cleaner, more intentional selection for your own users.

Headendarr solves that by importing the provider's VOD catalogue into its local database and then letting you build a cleaner, curated version on top of it.

That means you can:

- mirror the provider's categories when that already works well
- hide categories you do not want to expose
- combine several upstream categories into one cleaner collection
- prioritise one source over another when the same content appears more than once
- publish the final result in a way that suits your users and clients

The experience your users see is no longer limited by the way the upstream provider chose to organise things.

## How it works

When you add a source as an _"Xteam Codes"_ source type, Headendarr checks whether that source exposes VOD content. If it does, Headendarr imports the available movie and series categories along with the items inside them whenever the source refreshes.

That imported catalogue becomes the foundation for the **VOD** settings page. From there, you can choose what to expose, how to group it, and where it should appear.

If a provider does not offer VOD content, there is nothing for Headendarr to import for this feature.

## The imported VOD catalogue

A VOD provider can expose both movie and series categories, along with the items inside them. Headendarr imports that catalogue into its local database and keeps it in sync whenever the provider is refreshed, adding new content, updating existing entries, and removing anything that no longer exists upstream.

This imported catalogue acts as the source material for your curated VOD layout. You can think of it as the raw content pool, while your curated categories are the smaller, polished library your users actually browse.

To keep browsing fast without pulling in unnecessary data, Headendarr only stores the metadata it needs up front. Additional details are fetched and cached when they are needed, such as when generating `.strm` exports, exposing the HTTP library, or when a client requests more detailed VOD information through the XC API. That metadata cache is kept fresh while it is being used and is cleaned up automatically over time, helping Headendarr stay responsive without filling the database with unused data.

## Build your own curated categories

The **VOD** settings page lets you create your own categories for both **Movies** and **Shows**.

Each curated category can pull content from one or more upstream XC categories. This gives you a lot of flexibility without making the feature hard to manage.

For example, you might:

- keep an upstream category exactly as it is
- merge several similar upstream categories into one cleaner category
- ignore categories that are noisy or low value
- reuse the same upstream category in more than one curated view

A few common examples:

- `EN: ACTION`, and `EN: ACTION [4K]` upstream becomes a curated `Action Movies` category
- `EN: AWARD-WINNING`, `EN: TOP 500 [IMDB]`, and `EN: 4K [2024/2025]` are merged into a single `Popular Movies` category

## Source priority and failover

Each curated category keeps an ordered list of **Upstream Source Categories**, and that order directly controls how playback is selected.

When the same movie or show is available from multiple providers or categories, Headendarr will always try the highest-priority source first. If that source has already reached its connection limit, Headendarr automatically works down the list until it finds another source that can still serve the stream.

This makes it possible to combine duplicate VOD content from multiple providers while still preferring the one you trust most for quality, reliability, or availability. It also means playback can continue even when one provider is saturated, as long as another matching source still has capacity.

If no matching source has a connection available, Headendarr returns a connection-limit response instead. In that case, the client is shown a short three-second slate explaining that the connection limit has been reached, and then the stream ends.

## How your curated VOD can be published

Headendarr can expose curated VOD in three main ways. You can use just one of them, or combine them depending on how your users access content.

### XC-compatible API

Headendarr exposes curated VOD through its XC compatibility endpoints. To XC-capable apps, it behaves like a normal XC provider, but the catalogue they see is the one you created.

For users who have VOD access:

- movie categories are returned through `player_api.php?action=get_vod_categories`
- movie items are returned through `player_api.php?action=get_vod_streams`
- movie metadata is returned through `player_api.php?action=get_vod_info`
- series categories are returned through `player_api.php?action=get_series_categories`
- series items are returned through `player_api.php?action=get_series`
- series metadata is returned through `player_api.php?action=get_series_info`

Playback is available through the XC movie and series stream routes.

This is the simplest way to make your curated catalogue feel familiar to apps that already support Xtream Codes.

### `.strm` library export

If a curated category has **Generate STRM files** enabled, Headendarr can write that category out as a `.strm` library under the configured library path.

This is useful for media-library software such as Jellyfin and other tools that can scan `.strm` files from disk.

A `.strm` export is only generated when all of the following are true:

- the curated category has **Generate STRM files** enabled
- the user has VOD access for that content type
- the user has **Create VOD .strm Files** enabled

The library is written per user, for example:

- `/library/<username>/Movies/<category-slug>/...`
- `/library/<username>/Shows/<category-slug>/...`

This makes it possible to build user-specific exported libraries while still keeping access control in place.

### HTTP library export

If a curated category has **Expose HTTP Library** enabled, Headendarr includes that category in the browsable HTTP library at:

`/tic-api/library/`

This is designed for clients such as Kodi that can browse an authenticated HTTP directory tree.

The exposed structure looks like this:

- `/tic-api/library/Movies/<category-slug>/...`
- `/tic-api/library/Shows/<category-slug>/...`

The HTTP library is virtual. It does not rely on physical `.strm` files being present on disk. Instead, it is generated directly from Headendarr's VOD data.

## Authentication and access control

All client-facing VOD access is protected using a Headendarr user account together with that user's **Streaming Key**.

Those same streaming credentials are used across the available VOD access methods:

- **XC API** uses the username and streaming key
- **HTTP library** uses HTTP Basic authentication with the username and streaming key
- **`.strm` playback URLs** are generated using the user's streaming credentials

This is important because client apps do not need the user's main web login password. The **Streaming Key** is the credential intended for playback and app integrations.

## VOD permissions

VOD permissions are configured in the **VOD** section on the **User Settings Page** when creating or editing a user.

### VOD access options

| Setting                 | What it allows                                            |
| ----------------------- | --------------------------------------------------------- |
| **No VOD access**       | The user cannot browse or play any VOD content.           |
| **Movies only**         | The user can browse and play movie content only.          |
| **TV Shows only**       | The user can browse and play show content only.           |
| **Movies and TV Shows** | The user can browse and play both movie and show content. |

This access setting is enforced across every VOD output, so users only see and access the content types they are allowed to use.

For example:

- **XC API** responses only include the movie or show content the user is permitted to access.
- **Playback routes** reject requests for VOD content outside the user's allowed scope.
- The **HTTP library** only exposes the permitted content types.
- **`.strm` exports** are only generated for content the user is allowed to access.

### Create VOD `.strm` Files

Below the VOD access setting is the **Create VOD `.strm` Files** option.

Enable this only if you specifically want Headendarr to generate `.strm` exports for that user. In most cases, it is best left disabled unless you plan to use `.strm` libraries with that user, because enabling it increases metadata lookups and background processing.

## Caching and playback behaviour

When direct VOD proxy playback is enabled, Headendarr can cache VOD files locally in the timeshift area and serve playback from that local copy once it is complete.

This has two major benefits.

First, it reduces how long an upstream XC connection needs to stay open. If Headendarr finishes downloading a movie or episode into the local cache shortly after playback starts, it can switch over to the cached copy and release the upstream connection. That frees a connection slot on the provider for something else.

Second, it improves the playback experience. Once a complete local cache exists, seeking and repeat playback are much faster because the file is being served from local storage rather than repeatedly jumping around on the remote source.

A complete cache can also help when the upstream source has reached its connection limit. If Headendarr already has a finished local copy of the requested item, it can serve that instead of failing the request.

The cache is temporary. Cached movies and episodes remain available while they are actively being used, but by default they are automatically cleaned up after 10 minutes of inactivity. You can change that retention window with the `VOD_CACHE_RETENTION_MINUTES` environment variable. The value is in minutes, so setting `VOD_CACHE_RETENTION_MINUTES=10080` keeps cached VOD files for up to one week of inactivity.

When Headendarr needs space for a new VOD cache download, it now evicts the oldest inactive VOD cache files first. Active playback sessions, active readers, and in-progress downloads are left alone. If there is still not enough free space after those older cache files are removed, the new cache download is skipped and playback falls back to the normal non-cached behaviour.

The same cache system is also used by [VOD 24/7 Channels](/configuration/vod-24-7-channels/). In that mode, Headendarr uses the cache to hand linear-channel playback off from upstream to local disk as early as possible, which is a major part of how those channels avoid holding provider connections open for longer than necessary.

## Browser playback behaviour and profiles

The Headendarr web UI uses CSO-backed preview URLs for browser playback.

To keep playback responsive, the initial preview API does **not** wait for a full `ffprobe` run before returning the playback URL. Headendarr starts the background media probe separately, lets the browser player open the preview stream immediately, and then fills in richer metadata once the probe result is ready.

That means the flow is:

1. The browser requests a preview URL.
2. Headendarr returns a playable CSO VOD URL immediately.
3. The floating player opens that stream straight away.
4. The frontend then asks Headendarr for preview metadata in the background.
5. When probe data is ready, the player updates duration, source resolution, and any additional conversion profiles.

This avoids the older behaviour where preview startup could feel slow because `ffprobe` was being done inline before playback began.

### Browser playback profiles

The browser player always offers an **Original** profile first.

- The original stream may be played as a native file, MPEG-TS, or HLS depending on the resolved source and profile.
- When Headendarr knows the source resolution, the player also offers built-in conversion profiles:
  - `h264-aac-mp4[qty=1080p]`
  - `h264-aac-mp4[qty=720p]`
  - `h264-aac-mp4[qty=480p]`
- Those conversion options are only shown when they make sense for the source width.
- Converted browser VOD profiles use restart-based seeking because they are profile-scoped playback jobs, not simple native file seeks.

The browser player can also continue to use HLS-backed playback where appropriate, including fMP4-segment HLS outputs used by newer CSO browser profiles.

## Typical setup

A typical VOD setup looks like this:

1. Add a source as an **XC Provider**.
2. Refresh the source so Headendarr imports the upstream VOD catalogue.
3. Open the **VOD** settings page.
4. Create curated movie and show categories.
5. Choose which upstream categories should feed each curated category.
6. Set source priority where duplicate content or failover matters.
7. Decide how the curated catalogue should be published: XC API, `.strm` export, HTTP library, or any combination of those.
8. Configure user permissions so only the right users can access the content.

Once this is done, your users can browse a much cleaner VOD catalogue that reflects your own structure instead of the provider's raw layout.

## Related guides

- For building synthetic linear channels from imported VOD content, see [VOD 24/7 Channels](/configuration/vod-24-7-channels/).
- For Jellyfin `.strm` library import, see [Jellyfin](/configuration/connectivity/jellyfin/#video-on-demand-vod).
- For Kodi HTTP library browsing, see [Kodi](/configuration/connectivity/kodi/#video-on-demand-vod).
- For XC and other connectivity endpoint details, see [Connectivity Endpoints](/configuration/connectivity/endpoints/).
