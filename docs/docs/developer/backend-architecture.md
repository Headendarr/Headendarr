# Backend Architecture

This page documents backend runtime behaviour and integration boundaries.

## Task Queue + Scheduler

- APScheduler in `run.py` triggers the background-task loop every 10 seconds.
- Queue execution is managed by `backend/api/tasks.py` (`TaskQueueBroker`).
- Tasks execute sequentially (single active task at a time).
- If a run is already active, a concurrent scheduler tick is skipped.

## Background Task Patterns

- EPG updates (`/tic-api/epgs/update/<id>`) are queued.
- Playlist imports (`/tic-api/playlists/update/<id>`) are queued.
- Channel publish/sync tasks are queued by channel task helpers.
- DVR background tasks include:
  - `reconcile_dvr_recordings` (frequent status reconciliation)
  - `apply_dvr_rules` (scheduled recurring rule expansion)

## API + Polling Behaviour

- DVR UI uses long-polling endpoint `GET /tic-api/recordings/poll` for live status updates.
- Saving relevant settings queues TVHeadend configuration sync work.

## TVHeadend Proxy Boundaries

- TVHeadend HTTP proxy is served under `/tic-tvh/`.
- TVHeadend websocket/comet proxy is served under `/tic-tvh/<path>`.
- Proxy auth uses the internal sync user and bridges Headendarr auth to TVHeadend requests.

## Playback Path

- DVR playback endpoint: `/tic-api/recordings/<int:recording_id>/hls.m3u8`.
- Endpoint serves HLS wrapper output for TVHeadend recording files.

## Current Sync Caveat

- Some playlist lifecycle operations still call TVHeadend synchronously in `backend/playlists.py`.
- Prefer queue-based background execution for heavy or potentially slow sync paths.

## Event Loop Constraints

- Headendarr runs on a single Quart async loop by default.
- Keep request handlers and scheduled tasks non-blocking.
- Offload CPU-heavy or long blocking work to subprocesses/executors.

## Timestamp + Timezone Policy

- Persist application timestamps in UTC in the database (UTC at rest).
- Treat model date/time fields (`created_at`, `updated_at`, audit/session timestamps) as UTC values.
- API payloads should serialize timestamps as explicit UTC ISO-8601 (for example `2026-02-14T09:30:00Z`).
- Frontend/UI renders timestamps in the viewer's local timezone (or configured UI timezone setting when implemented), with 12h/24h formatting applied in UI.
- Container `TZ` controls process-local time behaviour for runtime tools/logs; DB/application timestamp persistence must still remain UTC.
