#!/data/data/com.termux/files/usr/bin/bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTAINER="${FCP_PHONE_CONTAINER:-fcp-phone}"
STATE_DIR="${FCP_PHONE_STATE:-$HOME/fcp-phone-state}"
DATA_DIR="$STATE_DIR/data"
RESULTS_DIR="$STATE_DIR/results"
LOG_FILE="$RESULTS_DIR/termux-phone.log"
PID_FILE="$RESULTS_DIR/termux-phone.pid"
PORT="${FCP_PHONE_PORT:-5000}"
URL="http://127.0.0.1:$PORT"
STOP_WAIT_SECONDS="${FCP_PHONE_STOP_WAIT_SECONDS:-10}"
KILL_WAIT_SECONDS="${FCP_PHONE_KILL_WAIT_SECONDS:-3}"
PROC_ROOT="${FCP_PHONE_PROC_ROOT:-/proc}"

usage() {
    cat <<'USAGE'
Usage: bash termux/fcp-phone.sh COMMAND [ARGS]

Commands:
  doctor                 Check the Termux/PRoot installation.
  start                  Start FCP in the background.
  foreground             Run FCP in the foreground for debugging.
  stop                   Stop all FCP phone sessions.
  restart                Stop and start FCP.
  status                 Show PRoot and HTTP status.
  logs [LINES]           Show the latest log lines (default 120).
  open                   Open FCP in the Android browser.
  shell                  Open a shell inside the FCP Linux container.
  demo-reset             Replace data/demo with the bundled example data.
  cache-rebuild          Rebuild the Parquet/DuckDB telemetry cache.
  prep                    Run the one-shot orchestration CLI.
  observer-sync          Run one Observer Phoenix synchronization.
  recorder SOURCES       Run the MTConnect recorder in the foreground.
                         Example: 'IG500=http://host:5000/current'
  update                 Pull main; restart if running. Rebuild only if dependencies changed.
  rebuild                Force a clean Linux environment rebuild.
USAGE
}

container_exists() {
    proot-distro list -q 2>/dev/null | grep -Fxq "$CONTAINER"
}

require_ready() {
    command -v proot-distro >/dev/null 2>&1 || {
        echo "proot-distro is missing. Run: bash termux/setup-phone.sh" >&2
        exit 2
    }
    container_exists || {
        echo "Container '$CONTAINER' is missing. Run: bash termux/setup-phone.sh" >&2
        exit 2
    }
    mkdir -p "$DATA_DIR" "$RESULTS_DIR"
}

login_base=(
    proot-distro login
    --bind "$ROOT:/app"
    --bind "$DATA_DIR:/app/data"
    --bind "$RESULTS_DIR:/app/results"
    --work-dir /app
    --env "FLASK_RUN_HOST=0.0.0.0"
    --env "FLASK_RUN_PORT=$PORT"
    --env "FCP_FLASK_SECRET=fcp-phone-local"
    --env "MPLBACKEND=Agg"
    --env "PYTHONDONTWRITEBYTECODE=1"
)

login_supports_detach() {
    proot-distro login --help 2>&1 | grep -q -- '--detach'
}

proot_distro_supports_command() {
    proot-distro "$1" --help >/dev/null 2>&1
}

http_ready() {
    curl -fsS "$URL/" >/dev/null 2>&1
}

process_start_time() {
    local pid="$1"
    local stat_line=""
    local stat_fields=()

    [[ -r "$PROC_ROOT/$pid/stat" ]] || return 1
    IFS= read -r stat_line < "$PROC_ROOT/$pid/stat" || return 1
    stat_line="${stat_line#*) }"
    read -r -a stat_fields <<< "$stat_line"
    [[ "${stat_fields[19]:-}" =~ ^[0-9]+$ ]] || return 1
    printf '%s\n' "${stat_fields[19]}"
}

remember_server_pid() {
    local pid="$1"
    local start_time=""

    start_time="$(process_start_time "$pid")" || return 1
    printf '%s %s\n' "$pid" "$start_time" > "$PID_FILE"
}

tracked_server_pid() {
    local pid=""
    local expected_start=""
    local current_start=""

    [[ -f "$PID_FILE" ]] || return 1
    read -r pid expected_start < "$PID_FILE" || return 1
    [[ "$pid" =~ ^[0-9]+$ && "$expected_start" =~ ^[0-9]+$ ]] || return 1
    current_start="$(process_start_time "$pid")" || return 1
    [[ "$current_start" == "$expected_start" ]] || return 1
    printf '%s\n' "$pid"
}

container_session_pids() {
    local cmdline_file=""
    local command_line=""
    local pid=""
    local current_rootfs="/containers/$CONTAINER/rootfs"
    local legacy_rootfs="/installed-rootfs/$CONTAINER"

    # PRoot-Distro 5.3 has no `ps` or `kill` command. Its proot process still
    # exposes the container rootfs in /proc, which lets us find only sessions
    # belonging to this FCP container (including sessions started before the
    # PID file was introduced).
    for cmdline_file in "$PROC_ROOT"/[0-9]*/cmdline; do
        [[ -r "$cmdline_file" ]] || continue
        command_line="$(tr '\0' ' ' < "$cmdline_file" 2>/dev/null || true)"
        [[ "$command_line" == *proot* ]] || continue
        if [[ "$command_line" != *"$current_rootfs"* && \
              "$command_line" != *"$legacy_rootfs"* ]]; then
            continue
        fi
        pid="${cmdline_file#"$PROC_ROOT"/}"
        pid="${pid%/cmdline}"
        [[ "$pid" =~ ^[0-9]+$ && "$pid" != "$$" ]] || continue
        printf '%s\n' "$pid"
    done
}

signal_process_tree() {
    local root_pid="$1"
    local signal_name="$2"
    local child_pid=""
    local children=""

    [[ "$root_pid" =~ ^[0-9]+$ ]] || return 0
    if [[ -r "$PROC_ROOT/$root_pid/task/$root_pid/children" ]]; then
        IFS= read -r children < "$PROC_ROOT/$root_pid/task/$root_pid/children" || true
        for child_pid in $children; do
            signal_process_tree "$child_pid" "$signal_name"
        done
    fi
    kill -s "$signal_name" "$root_pid" 2>/dev/null || true
}

signal_phone_sessions() {
    local signal_name="$1"
    local tracked_pid=""
    local pid=""
    local seen=" "

    if [[ "$signal_name" == "TERM" ]] && proot_distro_supports_command kill; then
        proot-distro kill "$CONTAINER" >/dev/null 2>&1 || true
    fi

    if tracked_pid="$(tracked_server_pid)"; then
        signal_process_tree "$tracked_pid" "$signal_name"
        seen+="$tracked_pid "
    fi

    while IFS= read -r pid; do
        [[ "$seen" == *" $pid "* ]] && continue
        signal_process_tree "$pid" "$signal_name"
        seen+="$pid "
    done < <(container_session_pids)
}

wait_for_http_stop() {
    local wait_seconds="${1:-$STOP_WAIT_SECONDS}"
    local attempt=0

    [[ "$wait_seconds" =~ ^[0-9]+$ ]] || wait_seconds=10
    for ((attempt = 0; attempt < wait_seconds; attempt++)); do
        http_ready || return 0
        sleep 1
    done
    ! http_ready
}

stop_server() {
    local quiet="${1:-false}"

    require_ready
    signal_phone_sessions TERM
    if ! wait_for_http_stop; then
        signal_phone_sessions KILL
        wait_for_http_stop "$KILL_WAIT_SECONDS" || true
    fi

    if http_ready; then
        echo "FCP is still responding at $URL; stop was not confirmed." >&2
        return 1
    fi

    rm -f "$PID_FILE"
    if [[ "$quiet" != "true" && "${FCP_PHONE_STOP_QUIET:-0}" != "1" ]]; then
        echo "FCP stopped."
    fi
}

start_server_process() {
    local guest_command='exec python -m catalog.flask_app.app >> results/termux-phone.log 2>&1'

    if login_supports_detach; then
        "${login_base[@]}" --detach "$CONTAINER" -- bash -lc "$guest_command"
        return
    fi

    # PRoot-Distro 5.3 and older do not provide login --detach. Keep the
    # complete login process alive in the Termux background instead.
    nohup "${login_base[@]}" "$CONTAINER" -- bash -lc "$guest_command" \
        </dev/null >> "$LOG_FILE" 2>&1 &
    local server_pid=$!
    if ! remember_server_pid "$server_pid"; then
        echo "Could not record the FCP server process." >&2
        kill "$server_pid" 2>/dev/null || true
        return 1
    fi
    disown "$server_pid" 2>/dev/null || true
}

doctor() {
    local status=0
    echo "FCP phone doctor"
    echo "Repository: $ROOT"
    echo "Container:  $CONTAINER"
    echo "State:      $STATE_DIR"
    echo "URL:        $URL"
    echo
    for command_name in git curl proot-distro; do
        if command -v "$command_name" >/dev/null 2>&1; then
            echo "  [ok] $command_name -> $(command -v "$command_name")"
        else
            echo "  [missing] $command_name"
            status=1
        fi
    done
    if container_exists; then
        echo "  [ok] container is installed"
    else
        echo "  [missing] container '$CONTAINER'"
        status=1
    fi
    [[ -d "$DATA_DIR" ]] && echo "  [ok] data directory" || { echo "  [missing] $DATA_DIR"; status=1; }
    [[ -d "$RESULTS_DIR" ]] && echo "  [ok] results directory" || { echo "  [missing] $RESULTS_DIR"; status=1; }
    return "$status"
}

start_server() {
    require_ready
    stop_server true
    : > "$LOG_FILE"
    rm -f "$PID_FILE"
    start_server_process

    echo "Starting FCP at $URL ..."
    for _ in $(seq 1 30); do
        if http_ready; then
            echo "FCP is ready: $URL"
            return 0
        fi
        sleep 1
    done
    echo "FCP did not become ready within 30 seconds." >&2
    tail -n 120 "$LOG_FILE" 2>/dev/null || true
    stop_server true || true
    return 1
}

run_guest() {
    require_ready
    "${login_base[@]}" "$CONTAINER" -- "$@"
}

main() {
    local command_name="${1:-start}"
    local lines=""
    local session_pids=""
    local sources=""
    local update_was_running=false

    case "$command_name" in
    doctor)
        doctor
        ;;
    start)
        start_server
        ;;
    foreground)
        require_ready
        stop_server true
        "${login_base[@]}" "$CONTAINER" -- python -m catalog.flask_app.app
        ;;
    stop)
        stop_server
        ;;
    restart)
        start_server
        ;;
    status)
        require_ready
        if proot_distro_supports_command ps; then
            proot-distro ps || true
        else
            session_pids="$(container_session_pids)"
            if [[ -n "$session_pids" ]]; then
                session_pids="${session_pids//$'\n'/, }"
                echo "PRoot: FCP session found (PID $session_pids)"
            else
                echo "PRoot: no FCP session found"
            fi
        fi
        if http_ready; then
            echo "HTTP: ready at $URL"
        else
            echo "HTTP: not responding at $URL"
        fi
        ;;
    logs)
        lines="${2:-120}"
        tail -n "$lines" "$LOG_FILE"
        ;;
    open)
        if command -v termux-open-url >/dev/null 2>&1; then
            termux-open-url "$URL"
        else
            echo "$URL"
        fi
        ;;
    shell)
        run_guest bash -l
        ;;
    demo-reset)
        require_ready
        rm -rf "$DATA_DIR/demo"
        "${login_base[@]}" "$CONTAINER" -- bash -lc \
            'mkdir -p data/demo && cp -a example-data/. data/demo/'
        echo "Demo data restored under $DATA_DIR/demo"
        ;;
    cache-rebuild)
        run_guest python -m catalog.cache.rebuild_telemetry_cache
        ;;
    prep)
        run_guest python -m catalog.orchestrator.cli
        ;;
    observer-sync)
        run_guest python -m catalog.observer_phoenix.export_jsonl
        ;;
    recorder)
        sources="${2:-}"
        [[ -n "$sources" ]] || {
            echo "Provide recorder sources, e.g.:" >&2
            echo "  bash termux/fcp-phone.sh recorder 'IG500=http://host:5000/current'" >&2
            exit 2
        }
        require_ready
        "${login_base[@]}" \
            --env "FCP_RECORDER_SOURCES=$sources" \
            "$CONTAINER" -- python catalog/standalone-recorder_v2/standalone-recorder_v2.py
        ;;
    update)
        if container_exists && http_ready; then
            update_was_running=true
            stop_server true
        fi
        cd "$ROOT"
        if ! git pull --ff-only; then
            if [[ "$update_was_running" == "true" ]]; then
                start_server
            fi
            return 1
        fi
        FCP_PHONE_RESTART_AFTER_SETUP="$update_was_running" \
            bash termux/setup-phone.sh --update
        ;;
    rebuild)
        cd "$ROOT"
        bash termux/setup-phone.sh --rebuild
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        usage >&2
        return 2
        ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
