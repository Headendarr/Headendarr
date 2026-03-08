---
title: Audit Event Troubleshooting
---

# Audit Event Troubleshooting

This guide helps you interpret common playback and CSO audit events, understand what they mean, and decide whether action is needed.

## How to read these events

- A single failover event can be normal.
- Repeated failovers in short windows usually indicate an upstream/provider issue.
- `switch_attempt` explains why CSO decided to move/restart.
- `switch_success` confirms CSO recovered (either to another source or by restarting the same one).

## Quick severity guide

- **No action**: occasional recoveries with stable playback.
- **Monitor**: repeated recoveries, but playback still mostly stable.
- **Action required**: frequent interruptions, no eligible sources, or authorization failures (manifesting as invalid data).

## Reason lookup table

| Reason / Event                                                       | What it usually means                                                | Typical user impact                                    | What to do                                                                                                    |
| -------------------------------------------------------------------- | -------------------------------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| `switch_attempt` + `reason=ingest_reader_ended`                      | Upstream ingest ended, CSO is recovering.                            | Brief pause/rebuffer possible.                         | Check the `return_code` and `ffmpeg_error` for details (see below).                                           |
| `return_code=0` + `progress=end`                                     | Upstream closed the connection gracefully (EOF).                     | Brief pause/rebuffer possible.                         | If frequent (e.g. every 5 mins): check upstream proxy/provider logs for session timeouts.                     |
| `unauthorized` / `forbidden` / `HTTP 401/403`                        | Upstream explicitly rejected authorisation/authentication.           | Playback fails immediately.                            | Verify subscription status, username/password/token, IP whitelist/geo policy, and provider connection limits. |
| `return_code=183` + `Invalid data` in `ffmpeg_error`                 | Upstream returned non-video payload (often HTML/error page).         | Playback fails or rapid failover churn.                | Treat as unauthorised/expired until proven otherwise. Open raw URL in browser/VLC and inspect response body.  |
| `switch_success` + `reason=failover`                                 | CSO recovered by switching/restarting ingest.                        | Usually brief disruption only.                         | No action if rare. Investigate if repeating often.                                                            |
| `health_actioned` / `scheduled_health_failed` + `reason=unreachable` | Scheduled health check could not read media bytes from source/proxy. | Source marked unhealthy; fallback planning may change. | Check source URL reachability, upstream proxy health, DNS/routing, and provider availability.                 |
| `under_speed`                                                        | Stream throughput dropped below real-time threshold.                 | Buffering or stutter.                                  | Run stream diagnostics, check routing/VPN, test alternate source.                                             |
| `stall_timeout`                                                      | Ingest stalled with no useful data progression.                      | Playback freeze or stop.                               | Check upstream source health, network route, and proxy behaviour.                                             |
| `capacity_blocked`                                                   | Source connection limit reached.                                     | Playback unavailable until slot frees.                 | Increase source limit (if permitted), reduce concurrent viewers, or add backup sources.                       |
| `no_available_source`                                                | No eligible/working source remained for the channel.                 | Playback fails.                                        | Verify channel source list, priorities, enabled flags, and source credentials.                                |
| `playback_unavailable`                                               | CSO could not provide a playable pipeline for the request.           | Playback fails immediately.                            | Check profile compatibility, source health, and recent failover reasons in audit details.                     |

## Repeated failover patterns

If you see the same `reason` and `source` repeating in a short interval (for example every 5 minutes), CSO is recovering but the upstream is unstable.

Recommended steps:

1. Confirm whether failover is to a different source or the same source ID.
2. Check whether disabled sources are still attached to the channel.
3. Probe the selected source outside playback for 10-15 minutes.
4. Review upstream proxy/provider logs at the exact failover timestamps.
5. If available, enable a second healthy source with lower priority as fallback.

## Disabled sources and failover eligibility

For failover selection, sources are treated as ineligible when either of these is disabled:

- `Playlist.enabled = false`
- `XcAccount.enabled = false`

## Identifying Authorisation and Expiration Issues

When an upstream provider expires or blocks access, audit events usually present as one of these patterns.

### Pattern A: Explicit auth rejection (401/403)

Typical indicators:

- `ffmpeg_error` contains `unauthorized`, `forbidden`, `http error`, or `server returned 401/403`.
- CSO may fail over to another source; if all sources are affected, playback fails.

Actions:

1. Verify account/subscription status at provider.
2. Verify auth credentials/tokens.
3. Check provider IP restrictions, geoblocking, and concurrent connection caps.

### Pattern B: `return_code=183` with `Invalid data`

Many providers do not send 401/403. They return `200 OK` with HTML/text (for example "Subscription expired", "Account blocked", "Login failed") instead of video.

Typical indicators:

- `switch_attempt` with `reason=ingest_reader_ended`.
- `return_code=183`.
- `ffmpeg_error` includes `Invalid data found when processing input`.
- Rapid failover across multiple sources can occur if several are unauthorised.

Your sample matches this pattern:

1. Source `280` ends cleanly (`return_code=0`, `progress=end`).
2. CSO tries `338`, then `337`; both fail with `return_code=183` + `Invalid data`.
3. CSO falls back to `280` (`switch_success reason=failover source=280`) and repeats later.

Actions:

1. Open the raw URL from audit logs in browser/VLC/curl and inspect payload/content-type.
2. If payload is HTML/text, treat source as unauthorised/expired.
3. Disable affected source(s), playlist, or XC account until renewed/fixed.
4. Keep at least one verified healthy source enabled to avoid repeated failover churn.

### Pattern C: clean EOF (`return_code=0` + `progress=end`)

This is a graceful upstream disconnect rather than an auth failure.

Actions:

1. Check upstream proxy/provider session timeout behaviour.
2. Check concurrent connection behaviour at provider.
3. Correlate timestamps between TIC and upstream proxy logs.
