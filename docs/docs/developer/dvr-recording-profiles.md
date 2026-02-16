# DVR Recording Profiles and Pathname Format

This page describes how Headendarr recording profiles map to TVHeadend DVR profile naming and pathname formatting.

## Overview

Headendarr now supports configurable recording pathname profiles in **Application Settings -> DVR Settings**.

- One required profile always exists: `Default`.
- Additional profiles can be added by the Headendarr admin (for example `Shows`, `Movies`).
- When creating a one-time recording or recording rule, Headendarr prompts for which profile to use.
- Headendarr stores the selected profile key with the recording/rule and uses that during TVH sync.

## Default Headendarr Profiles

The default profile set is:

| Key | Name | Default Pathname |
|---|---|---|
| `default` | `Default` | `%F_%R $u$n.$x` |
| `shows` | `Shows` | `$Q$n.$x` |
| `movies` | `Movies` | `$Q$n.$x` |

## Retention Policy Options

Headendarr stores retention as a strict enum and only accepts this whitelist:

- `1_day`, `3_days`, `5_days`
- `1_week`, `2_weeks`, `3_weeks`
- `1_month`, `2_months`, `3_months`, `6_months`
- `1_year`, `2_years`, `3_years`
- `maintained_space`
- `forever`

Any other value is rejected at save time and normalized to `forever`.

## TVHeadend Pathname Tokens

The following pathname tokens are available in TVHeadend format strings.

| Token | Description | Example |
|---|---|---|
| `$t` | Event title | `Tennis - Wimbledon` |
| `$s` | Event subtitle or summary | `Live Tennis Broadcast from Wimbledon` |
| `$u` | Event subtitle | `Tennis` |
| `$m` | Event summary | `Live Tennis Broadcast from Wimbledon` |
| `$e` | Event episode name | `S02-E06` |
| `$A` | Event season number | `2` |
| `$B` | Event episode number | `6` |
| `$c` | Channel name | `SkySport` |
| `$g` | Content type | `Movie : Science fiction` |
| `$Q` | Scraper-friendly name layout | `Gladiator (2000)` / `Bones - S02E06` |
| `$q` | Scraper-friendly name with directories | `tvshows/Bones/Bones - S05E11` |
| `$n` | Unique suffix if file exists | `-1` |
| `$x` | Output extension from muxer | `mkv` |
| `%F` | ISO date (`strftime`) | `2011-03-19` |
| `%R` | 24-hour time (`strftime`) | `14:12` |

## Standard Pattern

| Format | Description | Example |
|---|---|---|
| `$t$n.$x` | Basic title + uniqueness + extension | `Tennis - Wimbledon-1.mkv` |

## Delimiter Variants

Tokens like `$t` and `$s` support delimiter forms where the delimiter is only emitted when the value exists.

Examples:

- `$ t`
- `$-t`
- `$_t`
- `$.t`
- `$,t`
- `$;t`

Character limits can also be applied to some tokens:

- `$99-t` limits output length to 99 chars.

## Scraper-Friendly Modes (`$q`, `$Q`)

`$q` and `$Q` support numeric variants:

| Variant | Behavior |
|---|---|
| `1` | Force movie formatting (`$1q`, `$1Q`) |
| `2` | Force TV-series formatting (`$2q`, `$2Q`) |
| `3` | Alternative directory layout (`$3q`, `$3Q`) |

Examples:

- `$3q` -> `tvmovies/Gladiator (2000)/Gladiator (2000)`
- `$3q` -> `tvshows/Bones/Season 5/Bones - S05E11`
- `$3Q` -> `Gladiator (2000)/Gladiator (2000)`
- `$3Q` -> `Bones/Season 5/Bones - S05E11`

## Numeric Padding for Season/Episode

`$A` and `$B` support zero-padding modifiers:

| Token | Meaning | Example |
|---|---|---|
| `$A` | Season number (raw) | `2` |
| `$2A` | Season number padded to 2 digits | `02` |
| `$B` | Episode number (raw) | `6` |
| `$3B` | Episode number padded to 3 digits | `006` |

Example format:

`$t/Season $A/$2B-$u$n.$x`

Possible output:

`/recordings/Bones/Season 2/06-The Girl in Suite 2103.ts`

## How Headendarr Uses These Profiles

When Headendarr syncs recordings to TVHeadend:

- The selected Headendarr profile key is resolved to its pathname format.
- Headendarr uses that format to ensure the corresponding per-user TVH recorder profile exists.
- The recording is created in TVH using that per-user recorder profile.

If no profile is selected or a profile key is invalid, Headendarr falls back to `default`.
