---
title: VOD 24/7 Channels
---

# VOD 24/7 Channels

Headendarr can build a **24/7 linear channel** from imported XC VOD content.

Instead of publishing a normal live source list for that channel, Headendarr generates a repeating playback schedule from a curated VOD pool, publishes a synthetic XMLTV guide for that schedule, and starts playback only when a client actually tunes the channel.

This makes it possible to create channels such as:

- a science-fiction movie channel
- a sitcom rerun channel
- a franchise-specific channel
- a shuffled themed channel built from a curated content pool

## What makes a VOD 24/7 channel different

A standard channel is driven by one or more live stream sources.

A **VOD 24/7** channel is driven by:

- a set of **content rules** that decide which imported XC movies or series are eligible
- **schedule rules** that decide how those matched items are ordered
- a synthetic guide generated from that ordered schedule

There is no permanent live upstream stream sitting open for the channel. The channel behaves like a normal tuned channel to clients, but Headendarr only touches the upstream provider when someone actually starts watching.

## How to configure one

VOD 24/7 channels are configured from the normal **Channel Settings** dialog.

### 1. Create or open a channel

Open the **Channels** page and create a new channel, or edit an existing one.

### 2. Change the channel type

Set **Channel Type** to **VOD 24/7**.

When VOD content is available in Headendarr, this changes the dialog from live stream configuration to VOD rule configuration.

### 3. Add content rules

Use **VOD Content Rules** to build the eligible content pool for the channel.

Each rule has:

- **Rule Action**: `Include` or `Exclude`
- **Rule Type**: title and series matching variants such as `Contains` and `Starts With`
- **Rule Value**: the text used for matching

Rules are case-insensitive.

Typical examples:

- include all series where the title contains `Star Trek`
- include all movies where the title starts with `James Bond`
- exclude titles containing `Behind the Scenes`

The final pool is built by:

1. matching all enabled include rules
2. removing anything matched by enabled exclude rules

### 4. Choose the schedule order

Use **Schedule Rules** to decide how the matched content is turned into a repeating linear schedule.

Available ordering modes include:

- **Series, Season, Episode**
- **Release Date**
- **Season Air Date**
- **Episode Air Date**
- **Deterministic Shuffle**

You can then choose **Ascending** or **Descending** ordering.

This ordering is used to generate both:

- the playback schedule
- the synthetic XMLTV guide shown in the built-in TV Guide and exported XMLTV

## How the schedule works

Headendarr builds a schedule from the eligible VOD pool and stores it in the local cache.

That schedule contains:

- the ordered list of movies and episodes
- programme start and stop timestamps
- synthetic guide metadata such as title, subtitle, categories, poster, and episode numbering

For series, Headendarr expands each matched series into individual episodes before ordering them.

For movies, each movie becomes a single schedule entry.

The generated schedule is then repeated forward to build a continuous guide window.

## Guide behaviour

VOD 24/7 channels publish their own synthetic programme guide.

This means:

- they do not need a normal XMLTV source mapping
- they still appear in the built-in **TV Guide**
- they are included in generated XMLTV output with proper programme windows

The guide is based on the current saved rules and ordering settings, so changing those rules changes both the future playback order and the published guide.

## Runtime behaviour when nobody is watching

When a VOD 24/7 channel is **not tuned**, Headendarr does **not** keep an upstream provider connection open just to maintain the channel timeline.

The schedule continues to exist logically, but no live ingest session is held open for that channel while it is idle.

That means a VOD 24/7 channel does not permanently consume one of the provider's source connection slots just because the channel exists in your lineup.

## Runtime behaviour when a client tunes the channel

When a client tunes a VOD 24/7 channel:

1. Headendarr looks up the programme that should be airing **right now** from the synthetic schedule.
2. It calculates how far into that movie or episode the schedule currently is.
3. CSO starts VOD playback in the requested output format from that point in time.

Example:

- the schedule says an episode started 15 minutes ago
- a client tunes the channel now
- Headendarr starts playback at the 15-minute mark rather than from the beginning

This makes the channel behave like a real linear channel instead of a "start from the top every time" VOD list.

## Upstream usage and local cache handoff

When a VOD 24/7 channel is tuned, Headendarr uses the **Channel Stream Organiser (CSO)** VOD playback path rather than a permanent live stream source.

At a high level:

1. CSO starts playback for the currently airing movie or episode at the correct schedule offset.
2. Headendarr also tries to build a local cache copy for that item.
3. As soon as the local cache can take over, Headendarr switches playback to the cached copy and releases upstream usage as early as possible.

This is important for source-capacity management.

### If spare capacity is available

When the provider has enough available capacity, Headendarr can effectively use:

- one upstream connection for the live tuned playback path
- one upstream connection for the background cache fill

In this mode, the tuned client gets immediate playback at the correct schedule offset, while Headendarr simultaneously works to finish the local cache as quickly as possible.

Once the cached copy is ready far enough ahead, Headendarr can switch over to local serving and free those upstream connections again.

### If only one connection is available

If the provider does not have spare capacity for both activities, Headendarr does not require a second connection just to make the channel work.

In that case, it keeps serving the tuned playback with the single available upstream path and falls back to the simpler one-connection behaviour rather than failing the tune request just because the parallel cache fill could not be started.

The key point is:

- **two connections are preferred when available because they let Headendarr free the upstream sooner**
- **one connection is still enough for playback to continue**

## Upcoming programme pre-warm

To make programme boundaries smoother, Headendarr pre-warms the **next** scheduled movie or episode shortly before the current one ends.

The next item cache warm starts **60 seconds before** the current programme ends.

That gives Headendarr time to prepare the upcoming file so the handoff into the next scheduled item is as seamless as possible.

This is especially useful for:

- back-to-back episodes
- mixed movie schedules
- clients that stay tuned across programme boundaries for long periods

## Output formats and profiles

VOD 24/7 channels are served through the CSO channel playback endpoint:

- `/tic-api/cso/channel/<channel_id>`

Clients can still request supported CSO output profiles where applicable.

Because the source material is VOD rather than a normal live mux, Headendarr uses the VOD proxy/output path internally and starts the output stream at the correct in-programme offset for the current airing.

## When to use VOD 24/7 channels

VOD 24/7 channels are a good fit when you want:

- a linear "always on" experience built from VOD content
- themed rerun channels without managing manual schedules
- channel-style browsing for content that normally only exists as on-demand media
- better use of limited provider connections than a permanently open pseudo-live source would allow

## Related pages

- For standard channel setup, see [Channels](./channels.md).
- For imported VOD catalogues and curated VOD categories, see [Video On Demand](./video-on-demand.md).
- For CSO runtime behaviour, see [Channel Stream Organiser](./channel-stream-organiser.md).
- For playback endpoints, see [Connectivity Endpoints](./connectivity/endpoints.md).
