#!/data/data/com.termux/files/usr/bin/bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTAINER="${MSH_PHONE_CONTAINER:-msh-phone}"
STATE_DIR="${MSH_PHONE_STATE:-$HOME/msh-phone-state}"
DATA_DIR="$STATE_DIR/data"
RESULTS_DIR="$STATE_DIR/results"
LOG_FILE="$RESULTS_DIR/termux-phone.log"
PORT="${MSH_PHONE_PORT:-5000}"
URL="http://127.0.0.1:$PORT"

usage() {
    cat <<'USAGE'
Usage: bash termux/msh-phone.sh COMMAND [ARGS]

Commands:
  doctor                 Check the Termux/PRoot installation.
  start                  Start MSH in the background.
  foreground             Run MSH in the foreground for debugging.
  stop                   Stop all MSH phone sessions.
  restart                Stop and start MSH.
  status                 Show PRoot and HTTP status.
  logs [LINES]           Show the latest log lines (default 120).
  open                   Open MSH in the Android browser.
  shell                  Open a shell inside the MSH Linux container.
  demo-reset             Replace data/demo with the bundled example data.
  cache-rebuild          Rebuild the Parquet/DuckDB telemetry cache.
  prep                    Run the one-shot orchestration CLI.
  observer-sync          Run one Observer Phoenix synchronization.
  recorder SOURCES       Run the MTConnect recorder in the foreground.
                         Example: 'IG500=http://host:5000/current'
  update                 Pull MSH main and rebuild while preserving data/results.
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
    --bind "$DATA_DIR:/app/data"
    --bind "$RESULTS_DIR:/app/results"
    --work-dir /app
    --env "FLASK_RUN_HOST=0.0.0.0"
    --env "FLASK_RUN_PORT=$PORT"
    --env "MSH_FLASK_SECRET=msh-phone-local"
    --env "MPLBACKEND=Agg"
)

login_supports_detach() {
    proot-distro login --help 2>&1 | grep -q -- '--detach'
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
    disown "$server_pid" 2>/dev/null || true
}

doctor() {
    local status=0
    echo "MSH phone doctor"
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
    proot-distro kill "$CONTAINER" >/dev/null 2>&1 || true
    : > "$LOG_FILE"
    start_server_process

    echo "Starting MSH at $URL ..."
    for _ in $(seq 1 30); do
        if curl -fsS "$URL/" >/dev/null 2>&1; then
            echo "MSH is ready: $URL"
            return 0
        fi
        sleep 1
    done
    echo "MSH did not become ready within 30 seconds." >&2
    tail -n 120 "$LOG_FILE" 2>/dev/null || true
    return 1
}

run_guest() {
    require_ready
    "${login_base[@]}" "$CONTAINER" -- "$@"
}

command_name="${1:-start}"
case "$command_name" in
    doctor)
        doctor
        ;;
    start)
        start_server
        ;;
    foreground)
        require_ready
        proot-distro kill "$CONTAINER" >/dev/null 2>&1 || true
        "${login_base[@]}" "$CONTAINER" -- python -m catalog.flask_app.app
        ;;
    stop)
        require_ready
        proot-distro kill "$CONTAINER" >/dev/null 2>&1 || true
        echo "MSH stopped."
        ;;
    restart)
        require_ready
        proot-distro kill "$CONTAINER" >/dev/null 2>&1 || true
        start_server
        ;;
    status)
        require_ready
        proot-distro ps || true
        if curl -fsS "$URL/" >/dev/null 2>&1; then
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
            echo "  bash termux/msh-phone.sh recorder 'IG500=http://host:5000/current'" >&2
            exit 2
        }
        require_ready
        proot-distro login \
            --bind "$DATA_DIR:/app/data" \
            --work-dir /app \
            --env "MSH_RECORDER_SOURCES=$sources" \
            "$CONTAINER" -- python catalog/standalone-recorder_v2/standalone-recorder_v2.py
        ;;
    update)
        cd "$ROOT"
        git pull --ff-only
        bash termux/setup-phone.sh
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac
