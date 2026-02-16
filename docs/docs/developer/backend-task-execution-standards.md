# Backend Task Execution Standards

These standards define how backend work should be executed to keep the Quart event loop responsive for:

- HLS proxy
- TVH proxy (HTTP + WebSocket)
- API/UI request handling

## Strict Rules

1. Do not run CPU-heavy work directly in request handlers.
2. Do not run CPU-heavy work directly on the Quart loop in background task execution.
3. Offload heavy work to:
   - subprocesses for larger/isolated workloads
   - executor/thread offload for limited blocking utilities
4. Long-running operations must have timing logs and phase-level logs where practical.
5. Background task changes must preserve queue safety (single-run semantics unless explicitly redesigned).

## Required Patterns

## Heavy Compute / Large Parsing

- Preferred: subprocess runner for heavy parse/build jobs.
- Use bounded handoff and capture structured logs/results.

## Blocking Utilities

- For blocking helpers (example: unzip helpers), use executor offload (`run_in_executor` style) so Quart loop remains free.

## Observability

- Include total elapsed time logs for long-running tasks.
- Include key phase timings for diagnosis and benchmarking.

## Scripts Location

- Backend operational scripts belong under:
  - `backend/scripts/`
- Historical one-off migration helpers may live under:
  - `migrations/` (example: `migrations/sqlite_to_pg.py`)

## Change Checklist

- [ ] Any new heavy code path is offloaded out of Quart loop.
- [ ] Logs include total duration and meaningful phase timing.
- [ ] Behavior is validated with a representative benchmark.
- [ ] Documentation is updated when execution pattern changes.
