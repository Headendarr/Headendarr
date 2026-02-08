#!/usr/bin/env bash
###
# File: entrypoint.sh
# Project: docker
# File Created: Monday, 13th May 2024 4:20:35 pm
# Author: Josh.5 (jsunnex@gmail.com)
# -----
# Last Modified: Sunday, 3rd November 2024 2:39:11 pm
# Modified By: Josh5 (jsunnex@gmail.com)
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

# Catch term signal and terminate any child processes
_term() {
    kill -TERM "$proxy_pid" 2>/dev/null
    if [ -n "$tvh_pid" ]; then
        kill -SIGINT "$tvh_pid" 2>/dev/null
    fi
}
trap _term SIGTERM SIGINT

# If running as root, perform setup and re-run this script as the specified user
if [ "$(id -u)" = "0" ]; then
    # Create required directories
    mkdir -p /config/.tvh_iptv_config
    chown "${PUID:-1000}:${PGID:-1000}" /config/.tvh_iptv_config
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
    exec gosu "${PUID:-1000}" env HOME="/config" "$0" "$@"
fi

# Print the current version (if the file exists)
if [[ -f /version.txt ]]; then
    cat /version.txt
fi

# Ensure the customer is set
print_log info "ENABLE_APP_DEBUGGING: ${ENABLE_APP_DEBUGGING:-ENABLE_APP_DEBUGGING variable has not been set}"
print_log info "ENABLE_SQLALCHEMY_DEBUGGING: ${ENABLE_SQLALCHEMY_DEBUGGING:-ENABLE_SQLALCHEMY_DEBUGGING variable has not been set}"
print_log info "SKIP_MIGRATIONS: ${SKIP_MIGRATIONS:-SKIP_MIGRATIONS variable has not been set}"
print_log info "RUN_PIP_INSTALL: ${RUN_PIP_INSTALL:-RUN_PIP_INSTALL variable has not been set}"

# Configure required directories
mkdir -p /config/.tvh_iptv_config

# Exec provided command
if [ "X$*" != "X" ]; then
    print_log info "Running command '${*}'"
    exec "$*"
else
    # Install packages (if requested)
    if [ "${RUN_PIP_INSTALL}" = "true" ]; then
        python3 -m venv --symlinks --clear /var/venv-docker
        source /var/venv-docker/bin/activate
        python3 -m pip install --no-cache-dir -r /app/requirements.txt
    else
        source /var/venv-docker/bin/activate
    fi

    # Execute migrations
    if [ "${SKIP_MIGRATIONS}" != "true" ]; then
        print_log info "Running TVH-IPTV-Config DB migrations"
        alembic upgrade head
    fi

    # If the 'nginx' binary exists in the path, start it
    if command -v nginx >/dev/null 2>&1; then
        mkdir -p /tmp/nginx/logs
        # Replace the listen port in the Nginx configuration to whatever is set in FLASK_RUN_PORT
        if [ -n "${FLASK_RUN_PORT}" ]; then
            sed "s/listen.*;/listen ${FLASK_RUN_PORT};/" /defaults/nginx/nginx.conf.template > /tmp/nginx/nginx.conf
        fi
        # Start Nginx
        print_log info "Starting Nginx service"
        nginx -c /tmp/nginx/nginx.conf -p /tmp/nginx &
        proxy_pid=$!
        print_log info "Started Nginx service with PID $proxy_pid"
        # Update the Flask run port so that Nginx will proxy to whatever FLASK_RUN_PORT was set to
        export FLASK_RUN_PORT=9984
    fi

    # If the 'tvheadend' binary exists in the path, start it
    if command -v tvheadend >/dev/null 2>&1; then
        # Install default TVH config
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
            > /tmp/tvh_stdout.log 2> /tmp/tvh_stderr.log &
        tvh_pid=$!
        sleep 1
        if kill -0 "$tvh_pid" 2>/dev/null; then
            print_log info "Started tvheadend service with PID $tvh_pid"
        else
            print_log error "tvheadend failed to start";
            print_log error "Stdout:"; sed -e 's/^/[TVH-STDOUT] /' /tmp/tvh_stdout.log || true
            print_log error "Stderr:"; sed -e 's/^/[TVH-STDERR] /' /tmp/tvh_stderr.log || true
        fi
    else
        print_log warn "tvheadend binary not found at application start (PATH=$PATH). Skipping TVH launch."
    fi

    # Check if the database file exists
    if [[ -f "/config/.tvh_iptv_config/db.sqlite3" ]]; then
        echo "Starting VACUUM on /config/.tvh_iptv_config/db.sqlite3..."
        # Run VACUUM command on the database
        sqlite3 "/config/.tvh_iptv_config/db.sqlite3" "VACUUM;"
        echo "VACUUM completed for /config/.tvh_iptv_config/db.sqlite3."
    else
        echo "Database file not found at /config/.tvh_iptv_config/db.sqlite3. Skipping VACUUM."
    fi

    # Run TIC server
    if [ "${ENABLE_APP_HOT_RELOAD}" = "true" ]; then
        print_log info "Starting TIC server with watchgod hot reload (watching /app/backend)"
        export ENABLE_APP_HOT_RELOAD=false
        python3 /app/scripts/watchgod_reload.py python3 "${FLASK_APP:?}"
    else
        print_log info "Starting TIC server"
        python3 "${FLASK_APP:?}"
    fi

    # Terminate TVH process if TIC service ends
    if [ -n "$tvh_pid" ]; then
        kill -SIGINT "$tvh_pid"
    fi
fi
