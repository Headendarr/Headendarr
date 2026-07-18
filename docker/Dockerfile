ARG UV_VERSION=0.11.16
ARG BASE_IMAGE=python:3.13-trixie
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv
FROM ${BASE_IMAGE} AS base_image

#  _____ ____   ___  _   _ _____ _____ _   _ ____    ____ _____  _    ____ _____
# |  ___|  _ \ / _ \| \ | |_   _| ____| \ | |  _ \  / ___|_   _|/ \  / ___| ____|
# | |_  | |_) | | | |  \| | | | |  _| |  \| | | | | \___ \ | | / _ \| |  _|  _|
# |  _| |  _ <| |_| | |\  | | | | |___| |\  | |_| |  ___) || |/ ___ \ |_| | |___
# |_|   |_| \_\\___/|_| \_| |_| |_____|_| \_|____/  |____/ |_/_/   \_\____|_____|
#
FROM node:22-trixie AS frontend_build_stage

COPY ./frontend/package.json /build/
COPY ./frontend/package-lock.json /build/

WORKDIR /build

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    echo "** Install node build dependencies **" \
        && apt-get update \
        && apt-get install -y --no-install-recommends \
            build-essential \
            libffi-dev \
            libssl-dev \
            pkg-config \
            python3-dev

RUN --mount=type=cache,target=/root/.npm,sharing=locked \
    echo "** Install node dependencies **" \
        && npm ci --include dev \
    && \
    echo

COPY ./frontend /build
RUN \
    echo "** Build frontend **" \
        && npx quasar clean \
        && npx quasar build --publish \
    && \
    echo


#  ____    _    ____ _  _______ _   _ ____    ____ _____  _    ____ _____
# | __ )  / \  / ___| |/ / ____| \ | |  _ \  / ___|_   _|/ \  / ___| ____|
# |  _ \ / _ \| |   | ' /|  _| |  \| | | | | \___ \ | | / _ \| |  _|  _|
# | |_) / ___ \ |___| . \| |___| |\  | |_| |  ___) || |/ ___ \ |_| | |___
# |____/_/   \_\____|_|\_\_____|_| \_|____/  |____/ |_/_/   \_\____|_____|
#
FROM base_image AS backend_build_stage

USER root
WORKDIR /var

RUN \
    echo "** Install python build dependencies (only if base image is minimal python)**" \
        && apt-get update \
        && apt-get install -y --no-install-recommends \
            build-essential \
            libffi-dev \
            libssl-dev \
            pkg-config \
            python3-dev \
    && \
    echo

# Ensure deterministic installs and prevent uv from downloading a different Python.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/var/venv-docker \
    UV_PYTHON_DOWNLOADS=never

# Install python dependencies
COPY --from=uv /uv /uvx /usr/local/bin/
COPY pyproject.toml uv.lock /app/
WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/uv \
    echo "**** Install python dependencies ****" \
        && python3 -c 'import sys; assert sys.version_info[:2] == (3, 13), sys.version' \
        && uv sync --frozen --no-dev --no-install-project --compile-bytecode \
    && \
    echo


#  __  __    _    ___ _   _
# |  \/  |  / \  |_ _| \ | |
# | |\/| | / _ \  | ||  \| |
# | |  | |/ ___ \ | || |\  |
# |_|  |_/_/   \_\___|_| \_|
#
FROM base_image

USER root

# Runtime packages
RUN set -eux; \
    echo "**** update apt repositories & install base runtime packages ****"; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        ffmpeg \
        postgresql-17 \
        postgresql-contrib-17 \
        procps \
        tini \
        tzdata; \
    echo "**** conditional tvheadend dependency libraries (only if tvheadend missing) ****"; \
    if ! command -v tvheadend >/dev/null 2>&1; then \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
            libavahi-client3 \
            libdbus-1-3 \
            libdvbcsa1 \
            libopus0 \
            libpcre2-8-0 \
            liburiparser1 \
            libva2 \
            mesa-va-drivers \
            mesa-vdpau-drivers \
            zlib1g; \
    fi; \
    echo "**** optional nginx install (INSTALL_NGINX=1) ****"; \
    if [ "${INSTALL_NGINX:-0}" = "1" ]; then \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends nginx; \
    fi; \
    echo "**** optional Intel media drivers for hardware accel (x86_64 only; requires tvheadend present) ****"; \
    if [ "$(uname -m)" = 'x86_64' ] && command -v tvheadend >/dev/null 2>&1; then \
        DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends intel-media-va-driver i965-va-driver || echo "(Intel media drivers not available – continuing)"; \
    fi; \
    echo "**** cleanup apt cache ****"; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*; \
    echo

# Install gosu
COPY --from=tianon/gosu:trixie /gosu /usr/local/bin/
COPY --from=uv /uv /uvx /usr/local/bin/

# Install project
COPY --from=backend_build_stage /var/venv-docker /var/venv-docker
COPY --from=frontend_build_stage /build/dist/spa /app/frontend/dist/spa
COPY docker/overlay /
COPY backend /app/backend
COPY migrations /app/migrations
COPY alembic.ini /app/alembic.ini
COPY pyproject.toml uv.lock /app/
COPY run.py /app/run.py
COPY db-migrate.sh /app/db-migrate.sh

# Set environment variables (add venv bin to PATH for implicit execution)
ENV HOME="/config" \
    PATH="/var/venv-docker/bin:${PATH}" \
    UV_PROJECT_ENVIRONMENT="/var/venv-docker" \
    UV_PYTHON_DOWNLOADS="never"
ENV FLASK_APP="/app/run.py"
ENV FLASK_RUN_PORT="9985"
ENV FLASK_RUN_HOST="0.0.0.0"
ENV ENABLE_APP_DEBUGGING="false"
ENV ENABLE_SQLALCHEMY_DEBUGGING="false"
ENV SKIP_MIGRATIONS="false"
ENV HLS_PROXY_PREFIX="tic-hls-proxy"

# Set working directory
WORKDIR /app/

# Expose ports
EXPOSE 9985/tcp

# Set version label
ARG VERSION
ARG BUILD_DATE
ARG BASE_IMAGE
LABEL maintainer="Josh.5 <jsunnex@gmail.com>"
LABEL build="Version:- ${VERSION} Build-date:- ${BUILD_DATE} Base:- ${BASE_IMAGE}"

# Install entrypoint script
COPY ./docker/entrypoint.sh /entrypoint.sh
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ENTRYPOINT [ "/usr/bin/tini", "--", "/entrypoint.sh" ]
