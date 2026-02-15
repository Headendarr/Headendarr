#!/usr/bin/env bash
###
# File: entrypoint.sh
# Project: docker
# File Created: Monday, 13th May 2024 4:20:35 pm
# Author: Josh.5 (jsunnex@gmail.com)
# -----
# Last Modified: Monday, 9th February 2026 11:28:51 pm
# Modified By: Josh.5 (jsunnex@gmail.com)
###

set -e

# Ensure HOME is always set to /config
export HOME="/config"

# All printed log lines from this script should be formatted with this function
print_log() {
    local timestamp
    local pid
    local level
    local message
    timestamp="$(date +'%Y-%m-%d %H:%M:%S %z')"
    pid="$$"
    level="$1"
    message="${*:2}"
    echo "[${timestamp}] [${pid}] [${level^^}] ${message}"
}

configure_container_timezone() {
    local tz_name
    tz_name="${TZ:-Etc/UTC}"
    if [ -f "/usr/share/zoneinfo/${tz_name}" ]; then
        ln -snf "/usr/share/zoneinfo/${tz_name}" /etc/localtime
        echo "${tz_name}" >/etc/timezone
        print_log info "Configured container timezone to ${tz_name}"
    else
        print_log warn "TZ '${tz_name}' is invalid; falling back to Etc/UTC"
        export TZ="Etc/UTC"
        ln -snf "/usr/share/zoneinfo/Etc/UTC" /etc/localtime
        echo "Etc/UTC" >/etc/timezone
    fi
}

kill_pid() {
    local name="$1"
    local pid="$2"
    local timeout="${3:-5}"
    if [ -z "$pid" ]; then
        print_log warn "${name} PID not set; skipping"
        return
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
        print_log warn "${name} (PID ${pid}) not running; skipping"
        return
    fi
    print_log info "Sending TERM to ${name} (PID ${pid})"
    kill -TERM "$pid" 2>/dev/null || true
    local elapsed=0
    while kill -0 "$pid" 2>/dev/null && [ "$elapsed" -lt "$timeout" ]; do
        sleep 1
        elapsed=$((elapsed + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
        print_log warn "${name} (PID ${pid}) did not stop after ${timeout}s; sending KILL"
        kill -KILL "$pid" 2>/dev/null || true
    else
        print_log info "${name} (PID ${pid}) stopped"
    fi
}

# Catch term signal and terminate any child processes
_term() {
    print_log info "Received termination signal; shutting down services"
    kill_pid "tic" "$tic_pid"
    kill_pid "tvheadend" "$tvh_pid"
    if [ -n "${PG_BINDIR:-}" ] && [ -n "${POSTGRES_DIR:-}" ] && [ -f "${POSTGRES_DIR}/PG_VERSION" ]; then
        print_log info "Stopping Postgres via pg_ctl"
        "${PG_BINDIR}/pg_ctl" -D "${POSTGRES_DIR}" stop -m fast || true
    else
        kill_pid "postgres" "$pg_pid"
    fi
    kill_pid "proxy" "$proxy_pid"
}
trap _term SIGTERM SIGINT

install_packages() {
    if [ "${RUN_PIP_INSTALL}" = "true" ]; then
        python3 -m venv --symlinks --clear /var/venv-docker
        source /var/venv-docker/bin/activate
        python3 -m pip install --no-cache-dir -r /app/requirements.txt
    else
        source /var/venv-docker/bin/activate
    fi
}

prepare_dirs_root() {
    local app_user
    app_user="${APP_USER:-tic}"
    if ! getent group "${PGID:-1000}" >/dev/null 2>&1; then
        groupadd -g "${PGID:-1000}" "${app_user}"
    fi
    if ! id -u "${PUID:-1000}" >/dev/null 2>&1; then
        useradd -u "${PUID:-1000}" -g "${PGID:-1000}" -m -s /bin/bash "${app_user}"
    fi
    mkdir -p /config/.tvh_iptv_config
    chown "${PUID:-1000}:${PGID:-1000}" /config/.tvh_iptv_config

    mkdir -p /config/.postgres
    chown -R "${PUID:-1000}:${PGID:-1000}" /config/.postgres

    mkdir -p /tmp/nginx
    chown "${PUID:-1000}:${PGID:-1000}" /tmp/nginx

    if command -v tvheadend >/dev/null 2>&1; then
        mkdir -p /config/.tvheadend
        chown "${PUID:-1000}:${PGID:-1000}" /config/.tvheadend
        mkdir -p /recordings
        chown -R "${PUID:-1000}:${PGID:-1000}" /recordings
        mkdir -p /timeshift
        chown -R "${PUID:-1000}:${PGID:-1000}" /timeshift
    else
        print_log warn "tvheadend binary NOT found during root setup phase (PATH=$PATH)"
    fi
}

setup_postgres() {
    export POSTGRES_DB="${POSTGRES_DB:-tic}"
    export POSTGRES_USER="${POSTGRES_USER:-tic}"
    export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-tic}"
    export POSTGRES_HOST="${POSTGRES_HOST:-127.0.0.1}"
    export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
    export POSTGRES_DIR="/config/.postgres/db"
    export POSTGRES_SOCKET_DIR="/config/.postgres/var"

    export PG_VERSION
    PG_VERSION="$(ls /usr/lib/postgresql/ | sort -V | tail -n 1)"
    export PG_BINDIR="/usr/lib/postgresql/${PG_VERSION}/bin"

    if [ -f "/config/.tvh_iptv_config/db.sqlite3" ] && [ -s "${POSTGRES_DIR}/PG_VERSION" ]; then
        print_log warn "SQLite database detected; recreating Postgres data directory for clean migration"
        "${PG_BINDIR}/pg_ctl" -D "${POSTGRES_DIR}" stop -m fast || true
        rm -rf "/config/.postgres"
    fi

    if [ ! -s "${POSTGRES_DIR}/PG_VERSION" ]; then
        print_log info "Initializing Postgres data directory at ${POSTGRES_DIR}"
        mkdir -p "${POSTGRES_DIR}" "${POSTGRES_SOCKET_DIR}"
        chmod 700 "${POSTGRES_DIR}" "${POSTGRES_SOCKET_DIR}"
        "${PG_BINDIR}/initdb" -D "${POSTGRES_DIR}" -U "${POSTGRES_USER}" --encoding=UTF8
        {
            echo "listen_addresses='${POSTGRES_HOST}'"
            echo "port=${POSTGRES_PORT}"
            echo "unix_socket_directories='${POSTGRES_SOCKET_DIR}'"
        } >>"${POSTGRES_DIR}/postgresql.conf"
        {
            echo "local all all trust"
            echo "host all all 127.0.0.1/32 md5"
        } >>"${POSTGRES_DIR}/pg_hba.conf"
    fi

    print_log info "Starting Postgres"
    "${PG_BINDIR}/pg_ctl" -D "${POSTGRES_DIR}" start -w -t 60 -o "-c port=${POSTGRES_PORT} -c unix_socket_directories=${POSTGRES_SOCKET_DIR} -c timezone=UTC -c log_timezone=UTC"
    pg_pid="$("${PG_BINDIR}/pg_ctl" -D "${POSTGRES_DIR}" status | sed -n 's/.*PID: \([0-9]\+\).*/\1/p')"
    print_log info "Postgres started with PID ${pg_pid}"

    export PGHOST="${POSTGRES_HOST}"
    export PGPORT="${POSTGRES_PORT}"
    export PGUSER="${POSTGRES_USER}"

    if ! "${PG_BINDIR}/psql" -tAc "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'" | grep -q 1; then
        print_log info "Creating Postgres database ${POSTGRES_DB}"
        "${PG_BINDIR}/createdb" --encoding=UTF8 "${POSTGRES_DB}"
    fi

    print_log info "Ensuring Postgres user password"
    local password_sql
    password_sql="${POSTGRES_PASSWORD//\'/\'\'}"
    "${PG_BINDIR}/psql" -tAc "ALTER USER ${POSTGRES_USER} WITH PASSWORD '${password_sql}';" || true
}

run_migrations() {
    if [ "${SKIP_MIGRATIONS}" != "true" ]; then
        print_log info "Running TIC DB migrations"
        alembic upgrade head
    fi
}

rollback_last_migration() {
    print_log info "Current Alembic revision(s):"
    alembic current
    print_log info "Rolling back last Alembic migration"
    alembic downgrade -1
    print_log info "Alembic revision(s) after rollback:"
    alembic current
}

print_current_migration() {
    print_log info "Current Alembic revision(s):"
    alembic current
}

stop_postgres_for_migration_mode() {
    local mode_label="${1:-migration mode}"
    if [ -n "${PG_BINDIR:-}" ] && [ -n "${POSTGRES_DIR:-}" ] && [ -f "${POSTGRES_DIR}/PG_VERSION" ]; then
        print_log info "Stopping Postgres via pg_ctl (${mode_label})"
        "${PG_BINDIR}/pg_ctl" -D "${POSTGRES_DIR}" stop -m fast || true
    fi
}

migrate_sqlite_to_postgres() {
    if [ -f "/config/.tvh_iptv_config/db.sqlite3" ]; then
        print_log info "Running SQLite -> Postgres migration"
        python3 /app/migrations/sqlite_to_pg.py
    else
        print_log info "SQLite DB not found; skipping migration"
    fi
}

start_nginx() {
    if command -v nginx >/dev/null 2>&1; then
        mkdir -p /tmp/nginx/logs
        if [ -n "${FLASK_RUN_PORT}" ]; then
            sed "s/listen.*;/listen ${FLASK_RUN_PORT};/" /defaults/nginx/nginx.conf.template >/tmp/nginx/nginx.conf
        fi
        print_log info "Starting Nginx service"
        nginx -c /tmp/nginx/nginx.conf -p /tmp/nginx &
        proxy_pid=$!
        print_log info "Started Nginx service with PID $proxy_pid"
        export FLASK_RUN_PORT=9984
    fi
}

start_tvh() {
    if command -v tvheadend >/dev/null 2>&1; then
        if [ -f /config/.tvheadend/.lock ]; then
            print_log warn "Removing stale TVHeadend lock file: /config/.tvheadend/.lock"
            rm -f /config/.tvheadend/.lock || true
        fi
        if [ ! -f /config/.tvheadend/accesscontrol/83e4a7e5712d79a97b570b54e8e0e781 ]; then
            print_log info "Installing admin tvheadend accesscontrol"
            mkdir -p /config/.tvheadend/accesscontrol
            cp -rf /defaults/tvheadend/admin_accesscontrol /config/.tvheadend/accesscontrol/83e4a7e5712d79a97b570b54e8e0e781
        fi
        if [ ! -f /config/.tvheadend/passwd/c0a8261ea68035cd447a29a57d12ff7c ]; then
            print_log info "Installing admin tvheadend passwd"
            mkdir -p /config/.tvheadend/passwd
            cp -rf /defaults/tvheadend/admin_auth /config/.tvheadend/passwd/c0a8261ea68035cd447a29a57d12ff7c
        fi
        if [ ! -f /config/.tvheadend/config ]; then
            print_log info "Installing default tvheadend config"
            mkdir -p /config/.tvheadend
            cp -rf /defaults/tvheadend/config /config/.tvheadend/config
        fi
        print_log info "Starting tvheadend service"
        set +e
        tvheadend --version 2>/dev/null || print_log warn "Unable to display tvheadend version (non-fatal)"
        set -e
        tvheadend --config /config/.tvheadend --http_root /tic-tvh --nobackup --nosatipcli \
            >/tmp/tvh_stdout.log 2>/tmp/tvh_stderr.log &
        tvh_pid=$!
        sleep 1
        if kill -0 "$tvh_pid" 2>/dev/null; then
            print_log info "Started tvheadend service with PID $tvh_pid"
        else
            print_log error "tvheadend failed to start"
            print_log error "Stdout:"
            sed -e 's/^/[TVH-STDOUT] /' /tmp/tvh_stdout.log || true
            print_log error "Stderr:"
            sed -e 's/^/[TVH-STDERR] /' /tmp/tvh_stderr.log || true
        fi
    else
        print_log warn "tvheadend binary not found at application start (PATH=$PATH). Skipping TVH launch."
    fi
}

vacuum_sqlite_if_exists() {
    if [[ -f "/config/.tvh_iptv_config/db.sqlite3" ]]; then
        print_log info "Starting VACUUM on /config/.tvh_iptv_config/db.sqlite3"
        sqlite3 "/config/.tvh_iptv_config/db.sqlite3" "VACUUM;"
        print_log info "VACUUM completed for /config/.tvh_iptv_config/db.sqlite3"
    fi
}

cleanup_migrated_sqlite() {
    if [ -d "/config/.tvh_iptv_config" ]; then
        find /config/.tvh_iptv_config -maxdepth 1 -type f -name "db.sqlite3.migrated-*" -mtime +30 -print -delete || true
    fi
}

start_tic() {
    if [ "${ENABLE_APP_HOT_RELOAD}" = "true" ]; then
        print_log info "Starting TIC server with watchgod hot reload (watching /app/backend)"
        export ENABLE_APP_HOT_RELOAD=false
        python3 /app/backend/scripts/watchgod_reload.py python3 "${FLASK_APP:?}" &
    else
        print_log info "Starting TIC server"
        python3 "${FLASK_APP:?}" &
    fi
    tic_pid=$!
    print_log info "Started TIC server with PID $tic_pid"
}

# Root setup and drop privileges
if [ "$(id -u)" = "0" ]; then
    configure_container_timezone
    prepare_dirs_root
    exec gosu "${PUID:-1000}" env HOME="/config" "$0" "$@"
fi

# Print the current version (if the file exists)
if [[ -f /version.txt ]]; then
    cat /version.txt
fi

print_log info "ENABLE_APP_DEBUGGING: ${ENABLE_APP_DEBUGGING:-ENABLE_APP_DEBUGGING variable has not been set}"
print_log info "ENABLE_SQLALCHEMY_DEBUGGING: ${ENABLE_SQLALCHEMY_DEBUGGING:-ENABLE_SQLALCHEMY_DEBUGGING variable has not been set}"
print_log info "SKIP_MIGRATIONS: ${SKIP_MIGRATIONS:-SKIP_MIGRATIONS variable has not been set}"
print_log info "RUN_PIP_INSTALL: ${RUN_PIP_INSTALL:-RUN_PIP_INSTALL variable has not been set}"

mkdir -p /config/.tvh_iptv_config

# Exec provided command
if [ "X$*" != "X" ]; then
    print_log info "Running command '${*}'"
    exec "$*"
fi

install_packages
setup_postgres
if [ "${ROLLBACK_LAST_MIGRATION}" = "true" ] || [ "${PRINT_CURRENT_MIGRATION}" = "true" ]; then
    if [ "${ROLLBACK_LAST_MIGRATION}" = "true" ]; then
        rollback_last_migration
        stop_postgres_for_migration_mode "rollback mode"
    else
        print_current_migration
        stop_postgres_for_migration_mode "current migration mode"
    fi
    exit 0
fi
run_migrations
cleanup_migrated_sqlite
vacuum_sqlite_if_exists
migrate_sqlite_to_postgres
start_nginx
start_tvh
start_tic

wait "$tic_pid"
tic_exit=$?
print_log info "TIC server exited with code ${tic_exit}"
_term
exit "$tic_exit"
