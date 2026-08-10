#!/data/data/com.termux/files/usr/bin/bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTAINER="${MSH_PHONE_CONTAINER:-msh-phone}"
IMAGE="${MSH_PHONE_IMAGE:-msh-phone:latest}"
STATE_DIR="${MSH_PHONE_STATE:-$HOME/msh-phone-state}"
DATA_DIR="$STATE_DIR/data"
RESULTS_DIR="$STATE_DIR/results"
BUILD_SIGNATURE_FILE="$STATE_DIR/runtime-build.signature"
DEMO_READY_FILE="$STATE_DIR/demo-data.checked"
PORT="${MSH_PHONE_PORT:-5000}"
URL="http://127.0.0.1:$PORT"
FORCE_REBUILD=false
UPDATE_MODE=false
REBUILD_REASON=""

usage() {
    cat <<'USAGE'
Usage: bash termux/setup-phone.sh [--update | --rebuild]

With an existing compatible MSH container, setup reuses the installed Python
environment. --rebuild forces a clean environment rebuild.
USAGE
}

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

container_exists() {
    proot-distro list -q 2>/dev/null | grep -Fxq "$CONTAINER"
}

http_ready() {
    curl -fsS "$URL/" >/dev/null 2>&1
}

runtime_signature() {
    printf '%s\n' \
        "msh-phone-runtime-v2" \
        "$(git -C "$ROOT" hash-object Dockerfile)" \
        "$(git -C "$ROOT" hash-object requirements.txt)"
}

write_runtime_signature() {
    local temporary_signature="$BUILD_SIGNATURE_FILE.tmp"
    runtime_signature > "$temporary_signature"
    mv "$temporary_signature" "$BUILD_SIGNATURE_FILE"
}

runtime_signature_matches() {
    [[ -f "$BUILD_SIGNATURE_FILE" ]] || return 1
    [[ "$(cat "$BUILD_SIGNATURE_FILE")" == "$(runtime_signature)" ]]
}

ensure_termux_prerequisites() {
    local missing=false
    local command_name=""
    for command_name in git ssh curl proot-distro; do
        if ! command -v "$command_name" >/dev/null 2>&1; then
            missing=true
            break
        fi
    done
    if [[ "$missing" == "false" ]]; then
        echo "Termux prerequisites are already installed."
        return
    fi

    printf '\n== Installing missing Termux prerequisites ==\n'
    pkg update -y
    pkg install -y git openssh curl proot-distro
}

container_runtime_ready() {
    local expected_python=""
    expected_python="$(
        sed -n 's/^FROM python:\([0-9][0-9.]*\).*/\1/p' "$ROOT/Dockerfile" |
            head -n 1
    )"
    [[ -n "$expected_python" ]] || return 1

    proot-distro login \
        --bind "$ROOT:/app" \
        --work-dir /app \
        --env "MSH_EXPECTED_PYTHON=$expected_python" \
        "$CONTAINER" -- python -c '
from importlib import metadata
from pathlib import Path
import os
import re
import subprocess
import sys

expected = os.environ["MSH_EXPECTED_PYTHON"]
actual = f"{sys.version_info.major}.{sys.version_info.minor}"
if actual != expected:
    raise SystemExit(f"Python {actual} does not match required {expected}.")

missing = []
for raw_line in Path("requirements.txt").read_text(encoding="utf-8").splitlines():
    requirement = raw_line.split("#", 1)[0].strip()
    if not requirement or requirement.startswith("-"):
        continue
    name = re.split(r"(?:===|==|~=|!=|<=|>=|<|>|;|@|\[)", requirement, maxsplit=1)[0].strip()
    if not name:
        continue
    try:
        metadata.version(name)
    except metadata.PackageNotFoundError:
        missing.append(name)

if missing:
    raise SystemExit("Missing Python packages: " + ", ".join(missing))

check = subprocess.run(
    [sys.executable, "-m", "pip", "check"],
    capture_output=True,
    text=True,
)
if check.returncode:
    raise SystemExit(check.stdout.strip() or check.stderr.strip())
' >/dev/null
}

select_build_mode() {
    if [[ "$FORCE_REBUILD" == "true" ]]; then
        REBUILD_REASON="A clean rebuild was explicitly requested."
        return
    fi
    if ! container_exists; then
        REBUILD_REASON="The MSH Linux environment is not installed yet."
        return
    fi
    if [[ -f "$BUILD_SIGNATURE_FILE" ]] && ! runtime_signature_matches; then
        REBUILD_REASON="Dockerfile or requirements.txt changed."
        return
    fi
    if ! container_runtime_ready; then
        REBUILD_REASON="The installed Python environment is incomplete or incompatible."
        return
    fi
    if [[ ! -f "$BUILD_SIGNATURE_FILE" ]]; then
        echo "Existing Python environment is compatible; enabling fast updates."
        write_runtime_signature
    fi
}

parse_args() {
    while (($#)); do
        case "$1" in
        --update)
            UPDATE_MODE=true
            ;;
        --rebuild)
            FORCE_REBUILD=true
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            fail "Unknown option: $1"
            ;;
        esac
        shift
    done
}

main() {
    local currently_running=false
    local restart_after_setup=false

    parse_args "$@"
    if [[ "${PREFIX:-}" != *"com.termux"* ]]; then
        fail "Run this script inside the Termux app."
    fi
    [[ -f "$ROOT/Dockerfile" ]] || fail "Dockerfile not found at $ROOT/Dockerfile"
    [[ -f "$ROOT/requirements.txt" ]] || fail "requirements.txt not found at $ROOT/requirements.txt"

    ensure_termux_prerequisites
    mkdir -p "$DATA_DIR" "$RESULTS_DIR"
    if [[ "${MSH_PHONE_RESTART_AFTER_SETUP:-false}" == "true" ]]; then
        restart_after_setup=true
    fi
    if container_exists && http_ready; then
        currently_running=true
        restart_after_setup=true
    fi

    select_build_mode
    if [[ "$currently_running" == "true" ]]; then
        MSH_PHONE_STOP_QUIET=1 bash "$ROOT/termux/msh-phone.sh" stop
    fi

    if [[ -n "$REBUILD_REASON" ]]; then
        printf '\n== Building the MSH Linux environment ==\n'
        echo "$REBUILD_REASON"
        if ! proot-distro build --help >/dev/null 2>&1; then
            fail "This proot-distro version does not support Dockerfile builds. Run 'pkg upgrade -y' and retry."
        fi
        if container_exists; then
            echo "Replacing existing container: $CONTAINER"
            proot-distro remove "$CONTAINER"
        fi
        proot-distro build -t "$IMAGE" --install-as "$CONTAINER" "$ROOT"

        # Create nested bind targets in the image before /app is replaced by
        # the live repository checkout during normal commands.
        proot-distro login --work-dir /app "$CONTAINER" -- mkdir -p /app/data /app/results
        write_runtime_signature
    else
        printf '\n== Fast MSH update ==\n'
        echo "Runtime dependencies are unchanged; reusing the installed Linux environment."
    fi

    phone_login=(
        proot-distro login
        --bind "$ROOT:/app"
        --bind "$DATA_DIR:/app/data"
        --bind "$RESULTS_DIR:/app/results"
        --work-dir /app
    )

    printf '\n== Configuring MSH phone technical parameters ==\n'
    if [[ -f "$DATA_DIR/capabilities/config.json" ]]; then
        echo "Existing capability technical configuration was preserved."
    elif [[ -f "$DATA_DIR/server_setup/server_settings.json" ]]; then
        "${phone_login[@]}" "$CONTAINER" -- python setup_msh.py --migrate-legacy-phone-bootstrap
    else
        "${phone_login[@]}" "$CONTAINER" -- python setup_msh.py \
            --profile workbench --no-ai \
            --web-bind 0.0.0.0 --web-port "$PORT"
    fi

    if [[ ! -f "$DEMO_READY_FILE" ]]; then
        "${phone_login[@]}" "$CONTAINER" -- bash -lc '
            set -e
            if ! find data -type f -name "*.jsonl" -print -quit | grep -q .; then
                mkdir -p data/demo
                cp -a example-data/. data/demo/
                echo "Copied example data into data/demo/."
            fi
        '
        touch "$DEMO_READY_FILE"
    fi

    chmod +x "$ROOT/termux/msh-phone.sh"

    if [[ "$restart_after_setup" == "true" ]]; then
        bash "$ROOT/termux/msh-phone.sh" start
    fi

    printf '\n== MSH phone setup is ready ==\n'
    echo "Persistent data:    $DATA_DIR"
    echo "Persistent results: $RESULTS_DIR"
    echo "Capability choices are completed in the MSH onboarding UI; phone setup no longer creates a device role."
    if [[ "$restart_after_setup" == "true" ]]; then
        echo "MSH was updated and restarted at $URL"
    else
        echo
        echo "Start MSH:"
        echo "  bash termux/msh-phone.sh start"
        echo
        echo "Then open:"
        echo "  $URL"
    fi

    if [[ "$UPDATE_MODE" == "true" && -z "$REBUILD_REASON" ]]; then
        echo "Fast update complete; no dependency downloads were needed."
    fi
}

main "$@"
