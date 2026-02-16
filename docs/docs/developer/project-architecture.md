# Project Architecture

This page is the contributor-facing overview of Headendarr application structure.

## Runtime Components

- Backend: Quart app entrypoint in `run.py`.
- Frontend: Quasar SPA in `frontend/`.
- Database: Postgres only (SQLite is no longer supported).
- Migrations: Alembic files in `migrations/`.
- Optional TVHeadend service: launched by container entrypoint under `/tic-tvh` when enabled.

## Backend Layout

- `backend/`: API routes, models, services, background-task logic.
- `backend/models.py`: SQLAlchemy models.
- `backend/api/`: REST API endpoints and task queue APIs.
- `backend/scripts/`: backend subprocess/runner scripts.

## Frontend Layout

- `frontend/src/pages/`: top-level pages.
- `frontend/src/components/`: feature components.
- `frontend/src/components/ui/`: shared UI components and patterns.
- `frontend/src/router/`: route definitions.

## Supporting Paths

- `docker/`: Dockerfiles and compose files.
- `Taskfile.yml`: local task shortcuts for dev lifecycle commands.
- `migrations/sqlite_to_pg.py`: historical migration helper path.
- `docs/`: Docusaurus documentation source.

## Data + Build Notes

- Production frontend build output is served by the backend from `frontend/dist/spa`.
- Keep model or schema changes aligned with Alembic migrations.
