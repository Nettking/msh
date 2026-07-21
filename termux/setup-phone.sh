#!/data/data/com.termux/files/usr/bin/bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTAINER="${MSH_PHONE_CONTAINER:-msh-phone}"
IMAGE="${MSH_PHONE_IMAGE:-msh-phone:latest}"
STATE_DIR="${MSH_PHONE_STATE:-$HOME/msh-phone-state}"
DATA_DIR="$STATE_DIR/data"
RESULTS_DIR="$STATE_DIR/results"

fail() {
    echo "ERROR: $*" >&2
    exit 1
}

if [[ "${PREFIX:-}" != *"com.termux"* ]]; then
    fail "Run this script inside the Termux app."
fi

[[ -f "$ROOT/Dockerfile" ]] || fail "Dockerfile not found at $ROOT/Dockerfile"
[[ -f "$ROOT/requirements.txt" ]] || fail "requirements.txt not found at $ROOT/requirements.txt"

printf '\n== Installing Termux prerequisites ==\n'
pkg update -y
pkg install -y git openssh curl proot-distro

if ! proot-distro build --help >/dev/null 2>&1; then
    fail "This proot-distro version does not support Dockerfile builds. Run 'pkg upgrade -y' and retry."
fi

mkdir -p "$DATA_DIR" "$RESULTS_DIR"

printf '\n== Building the MSH Linux image ==\n'
if proot-distro list -q 2>/dev/null | grep -Fxq "$CONTAINER"; then
    echo "Replacing existing container: $CONTAINER"
    proot-distro kill "$CONTAINER" >/dev/null 2>&1 || true
    proot-distro remove "$CONTAINER"
fi

proot-distro build -t "$IMAGE" --install-as "$CONTAINER" "$ROOT"

printf '\n== Configuring MSH without Ollama ==\n'
proot-distro login \
    --bind "$DATA_DIR:/app/data" \
    --bind "$RESULTS_DIR:/app/results" \
    --work-dir /app \
    "$CONTAINER" -- bash -lc '
        set -e
        python setup_msh.py --mode web-workbench --no-ai --web-bind 0.0.0.0 --web-port 5000
        if ! find data -type f -name "*.jsonl" -print -quit | grep -q .; then
            mkdir -p data/demo
            cp -a example-data/. data/demo/
            echo "Copied example data into data/demo/."
        fi
    '

chmod +x "$ROOT/termux/msh-phone.sh"

printf '\n== MSH phone setup is ready ==\n'
echo "Persistent data:    $DATA_DIR"
echo "Persistent results: $RESULTS_DIR"
echo
echo "Start MSH:"
echo "  bash termux/msh-phone.sh start"
echo
echo "Then open:"
echo "  http://127.0.0.1:5000"
