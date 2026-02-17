# Development Environment Setup

This file is intended for repository contributors and is not part of the published docs site.

## Run from source with Docker Compose

From the project root, run:

```bash
mkdir -p ./dev_env/config
docker compose -f ./docker/docker-compose.dev-aio.yml up --build
```

This creates a `./dev_env` directory in the project root for configuration and cache data.

## Run from source with a Python venv

First complete the normal source setup from the docs site:

- https://headendarr.github.io/Headendarr/run-from-source

Then run the local development environment script:

```bash
./devops/run_local_dev_env.sh
```

## Updating packages

Activate your venv, then install the dev requirements:

```bash
python3 -m pip install -r ./requirements.txt -r ./requirements-dev.txt
```

Run `pip-audit` to identify outdated or vulnerable packages:

```bash
pip-audit -r ./requirements.txt -r ./requirements-dev.txt
```

After upgrading dependencies, refresh pinned requirements:

```bash
pip-compile ./requirements.in --upgrade
```
