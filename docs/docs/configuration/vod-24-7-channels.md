---
title: VOD 24/7 Channels
---

# VOD 24/7 Channels

Headendarr can build a **24/7 linear channel** from imported XC VOD content.

Instead of publishing a normal live source list for that channel, Headendarr builds a repeatable playback schedule from a curated VOD pool, generates synthetic XMLTV guide data for that schedule, and only starts playback when a client actually tunes in.

This makes it possible to create channels such as:

- a science-fiction movie channel
- a sitcom rerun channel
- a franchise-specific marathon channel
- a shuffled themed channel built from a curated content pool

## What makes a VOD 24/7 channel different

A standard channel is driven by one or more live stream sources.

A **VOD 24/7** channel is driven by:

- **content rules** that decide which imported XC movies or series are eligible
- **schedule rules** that decide how those matched items are ordered
- a synthetic guide generated from that ordered schedule

There is no permanent live upstream stream sitting open for the channel. The channel behaves like a normal tuned channel to clients, but Headendarr only touches the upstream provider when someone actually starts watching.

## Before you set one up

VOD 24/7 channels depend on a few things being in place first.

### Required prerequisites

- You must have at least one **XC provider** configured as a Source.
- That source must have had its **VOD catalogue imported** into Headendarr.
- The container or host must have a **working `/timeshift` mount** with enough free space for temporary VOD caching.
- `/timeshift` should be on **fast local storage**. SSD, cache-pool storage, or memory-backed storage is strongly preferred.

### Why `/timeshift` matters

VOD 24/7 channels do not just read metadata from the VOD catalogue.

At runtime they also use the VOD proxy cache under:

- `/timeshift/vod/movie/<id>`
- `/timeshift/vod/episode/<id>`

That cache is what lets Headendarr:

- download the asset to disk as fast as possible
- hand playback over from upstream to local disk
- free the provider connection earlier
- make repeat playback and seeking much faster
- smooth out programme boundaries when the next item has already been warmed

If `/timeshift` is slow or undersized, the channel will still work, but it will spend more time serving directly from the upstream source and you will lose much of the connection-saving benefit.

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
- **Rule Type**:
  - `Series Title Contains`
  - `Series Title Starts With`
  - `Movie Title Contains`
  - `Movie Title Starts With`
- **Rule Value**: the text used for matching

Rules are case-insensitive.

Typical examples:

- include all series where the title contains `Star Trek`
- include all movies where the title starts with `James Bond`
- exclude titles containing `Behind the Scenes`

The final pool is built by:

1. matching all enabled include rules
2. removing anything matched by enabled exclude rules

### Content-rule gotchas

- If you add no enabled **include** rules, the channel has no playable pool.
- `Starts With` is strict. Some providers prefix titles with language tags or markers, so `Contains` is often more reliable.
- Series rules only match imported **series titles**. Movie rules only match imported **movie titles**.
- For series, Headendarr later expands each matched series into its individual episodes before building the schedule.

### 4. Choose the schedule order

Use **Schedule Rules** to decide how the matched content is turned into a repeating linear schedule.

Available ordering modes are:

- **Rule Evaluation Order**
- **Series, Season, Episode**
- **Release Date**
- **Season Air Date**
- **Episode Air Date**
- **Deterministic Shuffle**

You can then choose **Ascending** or **Descending** ordering.

### What each ordering mode means

- **Rule Evaluation Order**: groups content by the order of your include rules, then sorts naturally inside each rule group.
- **Series, Season, Episode**: plays each series in title order, then season order, then episode order.
- **Release Date**: sorts movies and episodes by release date, then falls back to title or episode order.
- **Season Air Date**: groups first by the series release date, then keeps season and episode order within that flow.
- **Episode Air Date**: mixes episodes together across the whole pool by each episode's own release date.
- **Deterministic Shuffle**: shuffles the pool into a stable, repeatable order for that channel.

### Ordering gotchas

- **Deterministic Shuffle** is stable, not freshly random on every request. It keeps the same repeatable order for the same channel until the eligible pool changes.
- **Descending** also applies to **Rule Evaluation Order**, so later include rules can be moved earlier in the final schedule.
- Series ordering depends on the upstream XC episode metadata being available and sensible.

## How the schedule is generated

Headendarr builds a schedule from the eligible VOD pool and stores it under:

- `/config/cache/vod_channels/schedule-<channel_id>.json`

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
- they are included in generated XMLTV output with programme windows, titles, artwork, and episode numbering where available

### Guide window details

The schedule is generated:

- from **midnight UTC of the previous day**
- forward for at least **72 hours**
- up to **168 hours** when the cycle is especially long

That gives Headendarr enough history and enough future guide data for normal guide browsing without trying to expand the schedule forever.

### Guide-update gotchas

- The guide reflects the **currently saved** rules and schedule settings.
- Changing the rules changes both the future playback order and the published guide.
- If the matched pool is empty, the synthetic guide will also be empty.

## Runtime behaviour when nobody is watching

When a VOD 24/7 channel is **not tuned**, Headendarr does **not** keep an upstream provider connection open just to maintain the channel timeline.

The schedule continues to exist logically, but no ingest session is held open for that channel while it is idle.

That means a VOD 24/7 channel does not permanently consume one of the provider's source connection slots just because the channel exists in your lineup.

This is one of the main advantages over a permanently running pseudo-live stream.

## Runtime behaviour when a client tunes the channel

When a client tunes a VOD 24/7 channel:

1. Headendarr looks up the programme that should be airing **right now** from the synthetic schedule.
2. It calculates how far into that movie or episode the schedule currently is.
3. CSO starts playback from that point in the item, not from the beginning.

Example:

- the schedule says an episode started 15 minutes ago
- a client tunes the channel now
- Headendarr starts playback at the 15-minute mark

This makes the channel behave like a real linear channel instead of a "start from the top every time" VOD list.

## How the cache handoff works

When a VOD 24/7 channel is tuned, Headendarr uses the **Channel Stream Organiser (CSO)** VOD playback path.

At a high level:

1. CSO starts playback for the currently airing item at the correct offset.
2. Headendarr tries to start a full local disk cache for that item under `/timeshift/vod/...`.
3. As soon as local cache playback is viable, Headendarr switches the output over to local disk and releases the upstream work as early as possible.

This is what lets the channel feel linear while still protecting upstream connection limits.

### Why this helps with connection limits

If the current programme is cached quickly, Headendarr can stop leaning on the provider much sooner.

In practice that means:

- the viewer keeps watching
- the file is now being served from local disk
- the provider slot can be freed for another viewer or another background warm job

### Two-connection and one-connection behaviour

If spare capacity is available, Headendarr can use:

- one upstream path for the active tuned playback
- one upstream path for the background disk-cache fill

That is the fastest path to freeing upstream usage, because the cache can be completed aggressively while playback is already in progress.

If spare capacity is **not** available, playback can still continue with the simpler path instead of failing just because the parallel cache fill could not get its own slot.

The practical rule is:

- **two upstream slots are preferred**
- **one upstream slot is still enough for playback**

## Pre-warming and smooth transitions

Headendarr has two separate mechanisms to make programme boundaries smoother.

### 1. Next-item cache warming

The next scheduled movie or episode is cache-warmed **120 seconds before** the current programme ends.

This gives Headendarr time to start pulling the upcoming asset into local storage before the boundary is reached.

### 2. Boundary pre-buffering

Roughly **20 seconds before** the next programme starts, Headendarr prepares the next ingest segment and begins pre-buffering output data for it.

This means the next programme is not being opened for the first time exactly at the boundary. A bounded amount of the next segment is already queued and ready to go.

### Why the transition feels smoother

These two layers work together:

- the next asset starts warming to disk well before the boundary
- the next playback segment is opened about 20 seconds early
- the next segment is prefetched into a bounded in-memory buffer before it is needed

That reduces visible stalls at programme boundaries and makes back-to-back episodes or movies behave more like a continuous linear channel.

### Current pre-buffer sizing

The prestarted next-segment buffer is intentionally bounded rather than unbounded.

- The next ingest starts about **20 seconds** before the boundary.
- The queue used for that prestart is capped at **256 MB** per channel prestart runtime.

This gives enough room for much higher bitrate content without turning every tuned VOD 24/7 channel into an unlimited memory consumer.

## Cache and storage behaviour

The VOD cache used by 24/7 channels is temporary.

### What gets cached

- full movie files
- full episode files
- partial in-progress downloads while a cache fill is still running

### Where it lives

- `/timeshift/vod/movie/<id>`
- `/timeshift/vod/episode/<id>`
- temporary in-progress files are written as `.part` files

### When Headendarr will cache

Headendarr only starts the VOD disk cache when it can safely treat the item as cacheable.

Important requirements include:

- the upstream item size must be discoverable
- there must be enough free space on the `/timeshift` filesystem

Headendarr checks for roughly **2x the asset size** in free space before starting the cache fill. That leaves room for the completed file and the in-progress temporary file during the handoff.

### Cache cleanup

Cached VOD items are removed after **10 minutes of inactivity** when they are no longer actively being watched or filled.

This keeps the cache useful for short-term replay and repeat viewing without filling the timeshift area indefinitely.

## Output and client behaviour

VOD 24/7 channels are served through the CSO channel playback endpoint:

- `/tic-api/cso/channel/<channel_id>`

At runtime, the VOD 24/7 ingest path always produces a stable stitched MPEG-TS feed with:

- one video stream
- one audio stream
- subtitles and extra data streams removed

This is done specifically to make the stitched channel behave more like one continuous live feed across programme boundaries.

### Direct-stream and HLS behaviour

VOD 24/7 channels now support both:

- direct CSO stream outputs such as MPEG-TS or Matroska
- CSO HLS outputs when the requested profile uses HLS

The important detail is that HLS is file-backed in the normal CSO HLS cache path. Headendarr does **not** try to mux HLS directly to stdout for VOD 24/7 channels.

### When the output remuxes versus retranscodes

The VOD 24/7 ingest path always transcodes into a stable intermediate stream for the current requested codec family.

After that:

- if the requested output profile already matches the ingest video/audio codecs, the output session remuxes
- if the requested output profile needs different codecs, the output session transcodes as required

This avoids unnecessary double transcoding for compatible profiles while still keeping the ingest stream stable across episode boundaries.

## Troubleshooting and gotchas

### The channel exists but nothing plays

Check the following first:

- the channel actually has at least one enabled include rule
- the rules match imported XC VOD titles
- the underlying XC source is enabled
- the provider still exposes the upstream VOD item URLs

### The guide is empty

Usually this means:

- no items matched your rules
- the matched items expanded to no playable entries
- the schedule cache needs rebuilding after a rules change or VOD refresh

### The channel works but still uses provider slots for longer than expected

Common reasons:

- `/timeshift` is too slow
- there is not enough free space for disk caching
- the upstream server does not expose reliable size metadata
- the viewer tuned in mid-programme, so Headendarr had to begin from an offset while the cache fill was still building separately

### Starts With rules are not matching what you expect

Some providers prefix titles with language markers, region tags, or other decorations. In those cases:

- use `Contains` instead, or
- include the exact prefix in the `Starts With` rule value

### Long mixed schedules do not look random enough

`Deterministic Shuffle` is intentionally stable. If you want a different shuffled order, change the content pool for that channel or use a second channel with different rules.

## When to use VOD 24/7 channels

VOD 24/7 channels are a good fit when you want:

- a linear "always on" experience built from VOD content
- themed rerun channels without maintaining manual schedules
- channel-style browsing for content that normally only exists as on-demand media
- better use of limited provider connections than a permanently open pseudo-live source would allow

## Related pages

- For standard channel setup, see [Channels](./channels.md).
- For imported VOD catalogues and VOD caching more broadly, see [Video On Demand](./video-on-demand.md).
- For CSO runtime behaviour, see [Channel Stream Organiser](./channel-stream-organiser.md).
- For install-time storage setup, see [Docker Compose Installation](../installation/docker-compose.md).
