# Development Environment Setup

This file is intended for repository contributors and is not part of the published docs site.

## Run from source with Docker Compose

From the project root, run:

```bash
mkdir -p ./dev_env/config
docker compose -f ./docker/docker-compose.dev-aio.yml up --build
```

This creates a `./dev_env` directory in the project root for configuration and cache data.

## Backend quality checks

Run backend formatting and lint checks through the Docker Compose development image:

```bash
task backend:check
```

The command checks the complete repository by default. Pass one or more files or directories after `--` to limit the
check:

```bash
task backend:check -- backend/cso/common.py backend/cso/output.py
```

The format and lint phases are also available separately:

```bash
task backend:format:check
task backend:lint
```

These tasks run `uv sync --frozen` for the project `.venv` and use the cached `temp/cache/uv` package cache before
running Ruff inside a disposable backend container. They do not depend on tooling from a parent workspace.

To sync or audit the locked Python dependencies separately, run:

```bash
task backend:sync
task backend:audit
```

## Run from source with uv

First complete the normal source setup from the docs site:

- https://headendarr.github.io/Headendarr/run-from-source

Then run the local development environment script:

```bash
./devops/run_local_dev_env.sh
```
