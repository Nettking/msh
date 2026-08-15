#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"

MODE=normal
case "${1:-}" in
  "") ;;
  --resume) MODE=resume ;;
  --help|-h)
    cat <<'EOF'
Usage:
  bash start.sh            Start FCP and preserve existing state.
  bash start.sh --resume   Reconnect saved Federation state before opening FCP.

The supported launcher also starts the bounded host-owned update agent used by
Federation > Update all. Linux/macOS hosts therefore require python3 in addition
to Git and Docker.
EOF
    exit 0
    ;;
  *)
    echo "Unknown option: $1" >&2
    echo "Run: bash start.sh --help" >&2
    exit 2
    ;;
esac

command -v git >/dev/null 2>&1 || { echo "Git was not found." >&2; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "Docker was not found." >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || {
  echo "python3 is required by the bounded FCP host update agent on Linux/macOS." >&2
  exit 1
}
docker info >/dev/null 2>&1 || { echo "Docker is not running." >&2; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "Docker Compose v2 was not found." >&2; exit 1; }

: "${FCP_WEB_BIND:=127.0.0.1}"
: "${FCP_RELAY_BIND:=127.0.0.1}"
: "${FCP_WEB_PORT:=5000}"
: "${COMPOSE_PROJECT_NAME:=fcp}"
: "${FCP_DATA_DIR:=$ROOT/data}"
: "${FCP_RESULTS_DIR:=$ROOT/results}"
: "${FCP_AI_MODEL:=llama3.2:3b}"
export FCP_WEB_BIND FCP_RELAY_BIND FCP_WEB_PORT COMPOSE_PROJECT_NAME FCP_DATA_DIR FCP_RESULTS_DIR FCP_AI_MODEL

FCP_BUILD_COMMIT=$(git rev-parse --verify 'HEAD^{commit}')
case "$FCP_BUILD_COMMIT" in
  *[!0-9a-fA-F]*|'')
    echo "FCP could not determine an immutable build commit." >&2
    exit 1
    ;;
esac
if [ "${#FCP_BUILD_COMMIT}" -ne 40 ]; then
  echo "FCP build commit is not a full Git object ID." >&2
  exit 1
fi
if [ -n "$(git status --porcelain=v1 --untracked-files=all)" ]; then
  echo "FCP refuses to label a build from a checkout with local changes." >&2
  exit 1
fi
FCP_BUILD_COMMIT=$(printf '%s' "$FCP_BUILD_COMMIT" | tr 'A-F' 'a-f')
export FCP_BUILD_COMMIT

AGENT_DIR="$FCP_DATA_DIR/federation/update-agent"
mkdir -p "$AGENT_DIR"
nohup python3 "$ROOT/scripts/posix/fcp_update_agent.py" \
  --repo-root "$ROOT" \
  --data-directory "$FCP_DATA_DIR" \
  >>"$AGENT_DIR/agent.log" 2>&1 </dev/null &

# The agent uses a non-blocking file lock, so repeated normal starts do not
# create multiple host mutators.
sleep 0.1

echo "Building FCP services from $FCP_BUILD_COMMIT ..."
docker compose build relay flask recorder

echo "Starting Federation relay, Ollama, and managed recorder ..."
docker compose up -d relay ollama recorder

if ! docker compose exec -T ollama ollama show "$FCP_AI_MODEL" >/dev/null 2>&1; then
  echo "Installing required Ollama model: $FCP_AI_MODEL"
  docker compose --profile model-install run --rm ollama-pull
  docker compose exec -T ollama ollama show "$FCP_AI_MODEL" >/dev/null 2>&1 || {
    echo "The configured Ollama model could not be verified." >&2
    exit 1
  }
fi

RESUME_EXIT=0
if [ "$MODE" = resume ]; then
  echo "Reconnecting saved Federation state before starting the webapp ..."
  docker compose stop flask >/dev/null 2>&1 || true
  set +e
  docker compose run --rm --no-deps --entrypoint python flask \
    -m catalog.flask_app.services.existing_setup_resume
  RESUME_EXIT=$?
  set -e
  case "$RESUME_EXIT" in
    0|2|4) ;;
    *) echo "Saved setup could not be resumed safely (exit $RESUME_EXIT)." >&2 ;;
  esac
fi

echo "Starting Flask workbench ..."
docker compose up -d flask

BASE_URL="http://127.0.0.1:$FCP_WEB_PORT"
DEADLINE=$(( $(date +%s) + 90 ))
while :; do
  if docker compose exec -T flask python -c \
    "import urllib.request; r=urllib.request.urlopen('http://127.0.0.1:5000/onboarding', timeout=2); assert 200 <= r.status < 500" \
    >/dev/null 2>&1; then
    break
  fi
  if [ "$(date +%s)" -ge "$DEADLINE" ]; then
    echo "The containers started, but FCP did not become ready." >&2
    docker compose logs --tail 60 flask >&2 || true
    exit 1
  fi
  sleep 1
done

docker compose ps relay ollama flask recorder
printf '\nFCP is running:       %s\n' "$BASE_URL"
printf 'Federation:           %s/federation\n' "$BASE_URL"
printf 'Running build commit: %s\n' "$FCP_BUILD_COMMIT"
printf 'Device data:          %s\n' "$FCP_DATA_DIR"
printf 'Update agent log:     %s/agent.log\n' "$AGENT_DIR"

if [ "$MODE" = resume ]; then
  case "$RESUME_EXIT" in
    0) echo "Saved identity, Federation membership, and capability evidence were resumed." ;;
    4) echo "Federation resumed; saved capability evidence needs explicit review." ;;
    2) echo "No saved Federation membership was found; onboarding remains required." ;;
    *) echo "Federation resume needs guided repair." ;;
  esac
fi
