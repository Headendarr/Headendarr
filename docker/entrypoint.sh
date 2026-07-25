#!/usr/bin/env bash
###
# File: entrypoint.sh
# Project: docker
# File Created: Monday, 13th May 2024 4:20:35 pm
# Author: Josh.5 (jsunnex@gmail.com)
# -----
# Last Modified: Monday, 3rd August 2026 10:02:01 am
# Modified By: Josh.5 (jsunnex@gmail.com)
###

set -e

# Ensure HOME is always set to /config
export HOME="/config"

# Print build/version once as the first startup log line when available.
if [[ -z "${HEADENDARR_VERSION_PRINTED:-}" ]] && [[ -f /version.txt ]]; then
    cat /version.txt
    export HEADENDARR_VERSION_PRINTED=1
fi

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

stop_postgres() {
    if [ -n "${PG_BINDIR:-}" ] && [ -n "${POSTGRES_DIR:-}" ] && [ -f "${POSTGRES_DIR}/PG_VERSION" ]; then
        print_log info "Stopping Postgres via pg_ctl"
        "${PG_BINDIR}/pg_ctl" -D "${POSTGRES_DIR}" stop -m fast -w -t 10 || true
    elif [ -n "${pg_pid:-}" ]; then
        kill_pid "postgres" "$pg_pid" 10
    else
        print_log info "Using external Postgres; no local Postgres process to stop"
    fi
}

# Catch term signal and terminate child processes
_term() {
    if [ "${shutdown_started:-false}" = "true" ]; then
        return
    fi
    shutdown_started=true
    trap '' SIGTERM SIGINT
    print_log info "Received termination signal; shutting down services"
    # TIC may need both TVHeadend and Postgres while Quart and application
    # contexts are closing. Stop it first; Python fast-kills registered FFmpeg
    # groups and leaves orphaned cache files for the next startup sweep.
    kill_pid "tic" "${tic_pid:-}" 12

    # Once TIC has exited, its dependencies can stop concurrently. The bounded
    # waits keep the whole sequence inside the documented 30-second container
    # stop grace period.
    kill_pid "tvheadend" "${tvh_pid:-}" 10 &
    local tvh_stop_pid=$!
    stop_postgres &
    local postgres_stop_pid=$!
    kill_pid "proxy" "$proxy_pid" &
    local proxy_stop_pid=$!
    wait "$tvh_stop_pid" "$postgres_stop_pid" "$proxy_stop_pid" 2>/dev/null || true
}
trap _term SIGTERM SIGINT

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

configure_runtime_user_identity() {
    local app_user
    local target_uid
    local target_gid
    local uid_owner
    local runtime_user

    app_user="${APP_USER:-tic}"
    target_uid="${PUID:-1000}"
    target_gid="${PGID:-1000}"

    export RUNTIME_APP_UID="${target_uid}"
    export RUNTIME_APP_GID="${target_gid}"

    if ! getent group "${target_gid}" >/dev/null 2>&1; then
        if getent group "${app_user}" >/dev/null 2>&1; then
            groupmod -g "${target_gid}" "${app_user}"
        else
            groupadd -g "${target_gid}" "${app_user}"
        fi
    fi

    uid_owner="$(getent passwd "${target_uid}" | cut -d: -f1)"

    if id "${app_user}" >/dev/null 2>&1; then
        if [ -n "${uid_owner}" ] && [ "${uid_owner}" != "${app_user}" ]; then
            runtime_user="${uid_owner}"
            print_log warn "PUID ${target_uid} is already owned by '${uid_owner}', using that user for runtime"
        else
            usermod -o -u "${target_uid}" -g "${target_gid}" -s /bin/bash "${app_user}"
            runtime_user="${app_user}"
        fi
    else
        if [ -n "${uid_owner}" ]; then
            runtime_user="${uid_owner}"
            print_log warn "APP_USER '${app_user}' does not exist and PUID ${target_uid} belongs to '${uid_owner}', using that user for runtime"
        else
            useradd -u "${target_uid}" -g "${target_gid}" -m -s /bin/bash "${app_user}"
            runtime_user="${app_user}"
        fi
    fi

    export RUNTIME_APP_USER="${runtime_user}"
    print_log info "Configured runtime user '${RUNTIME_APP_USER}' (uid=${RUNTIME_APP_UID}, gid=${RUNTIME_APP_GID})"
}

prepare_dirs_root() {
    local runtime_uid
    local runtime_gid
    runtime_uid="${RUNTIME_APP_UID:-${PUID:-1000}}"
    runtime_gid="${RUNTIME_APP_GID:-${PGID:-1000}}"

    mkdir -p /config/.tvh_iptv_config
    chown "${runtime_uid}:${runtime_gid}" /config/.tvh_iptv_config

    if [ -z "${POSTGRES_HOST:-}" ]; then
        mkdir -p /config/.postgres
        chown -R "${runtime_uid}:${runtime_gid}" /config/.postgres
    fi

    mkdir -p /tmp/nginx
    chown "${runtime_uid}:${runtime_gid}" /tmp/nginx

    if command -v tvheadend >/dev/null 2>&1; then
        mkdir -p /config/.tvheadend
        chown "${runtime_uid}:${runtime_gid}" /config/.tvheadend
        mkdir -p /recordings
        chown -R "${runtime_uid}:${runtime_gid}" /recordings
        mkdir -p /timeshift
        chown -R "${runtime_uid}:${runtime_gid}" /timeshift
        mkdir -p /library
        chown -R "${runtime_uid}:${runtime_gid}" /library
    else
        print_log warn "tvheadend binary NOT found during root setup phase (PATH=$PATH)"
    fi
}

configure_video_device_access() {
    local runtime_user
    local has_devices=0
    local added_group=0
    local dev
    local dev_gid
    local group_name

    runtime_user="${RUNTIME_APP_USER:-}"
    if [ -z "${runtime_user}" ] || ! id "${runtime_user}" >/dev/null 2>&1; then
        print_log warn "Runtime user not resolved; skipping device group setup"
        return
    fi

    while IFS= read -r dev; do
        [ -n "${dev}" ] || continue
        has_devices=1
        dev_gid="$(stat -c '%g' "${dev}")"

        if id -G "${runtime_user}" | tr ' ' '\n' | grep -qx "${dev_gid}"; then
            continue
        fi

        if [ "${dev_gid}" = "0" ]; then
            continue
        fi

        group_name="$(getent group "${dev_gid}" | awk -F: '{print $1}')"
        if [ -z "${group_name}" ]; then
            group_name="video${dev_gid}"
            if ! getent group "${group_name}" >/dev/null 2>&1; then
                groupadd -g "${dev_gid}" "${group_name}"
            fi
        fi

        usermod -a -G "${group_name}" "${runtime_user}"
        added_group=1
        print_log info "Added user '${runtime_user}' to group '${group_name}' for device ${dev}"
    done < <(find /dev/dri /dev/dvb -type c -print 2>/dev/null | sort -u)

    if [ "${has_devices}" = "1" ] && [ "${added_group}" = "0" ]; then
        if ! id -nG "${runtime_user}" | tr ' ' '\n' | grep -qx "root"; then
            usermod -a -G root "${runtime_user}"
            print_log info "All video device groups already present or root-owned; added '${runtime_user}' to root group as fallback"
        fi
    fi
}

sync_packages() {
    if [ "${RUN_UV_SYNC}" = "true" ] && [ "${UV_SYNC_COMPLETED:-false}" != "true" ]; then
        uv sync --frozen --no-dev --no-install-project
        export UV_SYNC_COMPLETED=true
    fi
    source /var/venv-docker/bin/activate
}

setup_postgres() {
    # Check if external PostgreSQL server is configured.
    # An external host is defined as a non-empty POSTGRES_HOST.
    local is_external=false
    if [ -n "${POSTGRES_HOST:-}" ]; then
        is_external=true
    fi

    export POSTGRES_DB="${POSTGRES_DB:-tic}"
    export POSTGRES_USER="${POSTGRES_USER:-tic}"
    export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-tic}"
    export POSTGRES_HOST="${POSTGRES_HOST:-127.0.0.1}"
    export POSTGRES_PORT="${POSTGRES_PORT:-5432}"

    if [ "${is_external}" = "true" ]; then
        print_log info "Using external PostgreSQL server at ${POSTGRES_HOST}:${POSTGRES_PORT}"
        return 0
    fi

    # Export PG* variables for internal PostgreSQL administration tools (psql, createdb, pg_ctl)
    export PGHOST="${POSTGRES_HOST}"
    export PGPORT="${POSTGRES_PORT}"
    export PGUSER="${POSTGRES_USER}"

    export POSTGRES_DIR="/config/.postgres/db"
    export POSTGRES_SOCKET_DIR="/config/.postgres/var"

    export PG_VERSION
    PG_VERSION="$(ls /usr/lib/postgresql/ | sort -V | tail -n 1)"
    export PG_BINDIR="/usr/lib/postgresql/${PG_VERSION}/bin"

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
        local before_current
        local after_current

        print_log info "Current Alembic revision(s) before upgrade:"
        before_current="$(alembic current 2>/dev/null || true)"
        printf '%s\n' "${before_current}"
        print_log info "Running TIC DB migrations"
        alembic upgrade head
        after_current="$(alembic current 2>/dev/null || true)"
        if [ "${after_current}" != "${before_current}" ]; then
            print_log info "Alembic revision(s) after upgrade:"
            printf '%s\n' "${after_current}"
        fi
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

reset_admin_password() {
    if [ -f "/config/.reset-admin-password" ]; then
        print_log info "Resetting admin password as '/config/.reset-admin-password' exists"
        python3 /app/backend/scripts/reset_admin_password.py
        rm -f "/config/.reset-admin-password"
        print_log info "Removed '/config/.reset-admin-password'"
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

seed_local_tvh_sync_user() {
    local sync_username
    local sync_password
    local sync_password2
    local sync_access_file
    local sync_passwd_file
    local sync_user_file
    local access_template
    local passwd_template
    local sync_user_template

    sync_username="${TVH_SYNC_USERNAME:-tic-admin}"
    sync_password="$(python3 -c 'import secrets; print(secrets.token_urlsafe(18))')"
    sync_password2="$(printf 'TVHeadend-Hide-%s' "${sync_password}" | base64 -w 0)"
    sync_access_file="/config/.tvheadend/accesscontrol/83e4a7e5712d79a97b570b54e8e0e781"
    sync_passwd_file="/config/.tvheadend/passwd/c0a8261ea68035cd447a29a57d12ff7c"
    sync_user_file="/config/.tvh_iptv_config/tvh_sync_user.json"
    access_template="/defaults/tvheadend/admin_accesscontrol"
    passwd_template="/defaults/tvheadend/admin_auth"
    sync_user_template="/defaults/tvh_iptv_config/tvh_sync_user.json.template"

    mkdir -p /config/.tvheadend/accesscontrol
    mkdir -p /config/.tvheadend/passwd
    mkdir -p /config/.tvh_iptv_config

    sed \
        -e "s|__TVH_SYNC_USERNAME__|${sync_username}|g" \
        "${access_template}" >"${sync_access_file}"
    sed \
        -e "s|__TVH_SYNC_USERNAME__|${sync_username}|g" \
        -e "s|__TVH_SYNC_PASSWORD2__|${sync_password2}|g" \
        "${passwd_template}" >"${sync_passwd_file}"
    sed \
        -e "s|__TVH_SYNC_USERNAME__|${sync_username}|g" \
        -e "s|__TVH_SYNC_PASSWORD__|${sync_password}|g" \
        "${sync_user_template}" >"${sync_user_file}"

    print_log info "Seeded local TVH sync user '${sync_username}' and rotated password for this start"
}

start_tvh() {
    if command -v tvheadend >/dev/null 2>&1; then
        if [ -f /config/.tvheadend/.lock ]; then
            print_log warn "Removing stale TVHeadend lock file: /config/.tvheadend/.lock"
            rm -f /config/.tvheadend/.lock || true
        fi
        seed_local_tvh_sync_user
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
            return 0
        else
            print_log error "tvheadend failed to start"
            print_log error "Stdout:"
            sed -e 's/^/[TVH-STDOUT] /' /tmp/tvh_stdout.log || true
            print_log error "Stderr:"
            sed -e 's/^/[TVH-STDERR] /' /tmp/tvh_stderr.log || true
            return 1
        fi
    else
        print_log warn "tvheadend binary not found at application start (PATH=$PATH). Skipping TVH launch."
        return 0
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

wait_for_child_exit() {
    local name="$1"
    local pid="$2"
    local exit_code
    set +e
    wait "$pid"
    exit_code=$?
    set -e
    print_log error "${name} process exited with code ${exit_code}"
    return "$exit_code"
}

monitor_services() {
    while true; do
        if [ -n "${tic_pid:-}" ] && ! kill -0 "$tic_pid" 2>/dev/null; then
            wait_for_child_exit "TIC" "$tic_pid"
            return $?
        fi
        if [ -n "${tvh_pid:-}" ] && ! kill -0 "$tvh_pid" 2>/dev/null; then
            wait_for_child_exit "TVHeadend" "$tvh_pid"
            return $?
        fi
        if [ -n "${pg_pid:-}" ] && ! kill -0 "$pg_pid" 2>/dev/null; then
            wait_for_child_exit "Postgres" "$pg_pid"
            return $?
        fi
        sleep 1
    done
}

# Root setup and drop privileges
if [ "$(id -u)" = "0" ]; then
    configure_container_timezone
    configure_runtime_user_identity
    prepare_dirs_root
    configure_video_device_access
    sync_packages
    if [ -n "${RUNTIME_APP_USER:-}" ]; then
        print_log info "Dropping privileges to '${RUNTIME_APP_USER}'"
        exec gosu "${RUNTIME_APP_USER}" env HOME="/config" "$0" "$@"
    fi
    print_log warn "RUNTIME_APP_USER is not set; falling back to PUID '${PUID:-1000}'"
    exec gosu "${PUID:-1000}" env HOME="/config" "$0" "$@"
fi

print_log info "ENABLE_APP_DEBUGGING: ${ENABLE_APP_DEBUGGING:-ENABLE_APP_DEBUGGING variable has not been set}"
print_log info "ENABLE_SQLALCHEMY_DEBUGGING: ${ENABLE_SQLALCHEMY_DEBUGGING:-ENABLE_SQLALCHEMY_DEBUGGING variable has not been set}"
print_log info "SKIP_MIGRATIONS: ${SKIP_MIGRATIONS:-SKIP_MIGRATIONS variable has not been set}"
print_log info "RUN_UV_SYNC: ${RUN_UV_SYNC:-RUN_UV_SYNC variable has not been set}"

mkdir -p /config/.tvh_iptv_config

# Exec provided command
if [ $# -gt 0 ]; then
    if [ -f /var/venv-docker/bin/activate ]; then
        source /var/venv-docker/bin/activate
    fi
    print_log info "Running command '$@'"
    exec "$@"
fi

sync_packages
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
reset_admin_password
start_nginx
if ! start_tvh; then
    print_log error "TVHeadend failed during startup; terminating container"
    _term
    exit 1
fi
start_tic

set +e
monitor_services
service_exit=$?
set -e
_term
exit "$service_exit"
