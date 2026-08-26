#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"

MODE=normal
case "${1:-}" in
  "") ;;
  --fresh) MODE=fresh ;;
  --resume) MODE=resume ;;
  --help|-h)
    cat <<'EOF'
Usage:
  bash start.sh            Start FCP and preserve existing state.
  bash start.sh --resume   Reconnect saved Federation state before opening FCP.
  bash start.sh --fresh    Factory-reset mutable FCP state, verify it, then start FCP.

Normal and resume modes preserve identity, Federation membership, recordings,
source configuration, recorder checkpoints, results, and downloaded models.
Resume mode never runs inspection or benchmarks and never replaces Federation authority.
The --fresh option requires typing RESET. Machine recordings, integrity metadata,
Docker/model resources, source code, deployment settings, and immutable checkout
scaffolding are preserved.

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
if [ "$#" -gt 1 ]; then
  echo "Unknown option: $2" >&2
  echo "Run: bash start.sh --help" >&2
  exit 2
fi

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

confirm_fresh_reset() {
  echo
  echo "FRESH DEVICE INSTALL"
  echo "This permanently removes this checkout's mutable FCP application state:"
  echo "  - human administrators, passwords, authentication secrets, and login sessions"
  echo "  - FCP device identity and keys"
  echo "  - Federation membership, trust, pairing, discovery, onboarding, and authority state"
  echo "  - capability, contribution, benchmark, provider, Activity, and job state"
  echo "  - source configuration and recorder configuration, checkpoints, status, and runtime state"
  echo "  - analyses, results, digital-twin projections, and retained legacy setup state"
  echo
  echo "It preserves the machine recording corpus and its integrity metadata."
  echo "Docker images, downloaded model volumes, source code, and deployment settings are not application state and are not reset."
  echo
  printf 'Type RESET to continue: '
  IFS= read -r FCP_RESET_CONFIRM || FCP_RESET_CONFIRM=""
  FCP_RESET_CONFIRM=$(printf '%s' "$FCP_RESET_CONFIRM" | tr '[:lower:]' '[:upper:]')
  if [ "$FCP_RESET_CONFIRM" != RESET ]; then
    echo "Fresh install cancelled. No state was removed."
    return 2
  fi
  FCP_FRESH_RESET_CONFIRMED=1
  export FCP_FRESH_RESET_CONFIRMED
}

build_core_images() {
  echo "Building FCP core services through the serialized host build lifecycle ..."
  if [ "${FCP_HOST_MUTATION_LEASE_ACTIVE:-}" = "1" ]; then
    if ! FCP_BUILD_COMMIT=$(python3 -m catalog.federation.host_build \
      --repo-root "$ROOT" --lease-already-held); then
      echo "FCP images could not be built safely. Review the host-build error above." >&2
      return 1
    fi
  else
    if ! FCP_BUILD_COMMIT=$(python3 -m catalog.federation.host_build --repo-root "$ROOT"); then
      echo "FCP images could not be built safely. Review the host-build error above." >&2
      return 1
    fi
  fi
  export FCP_BUILD_COMMIT
  echo "Built FCP core images from $FCP_BUILD_COMMIT."
}

# Keep the human confirmation outside the host-mutation critical section so an
# unattended --fresh prompt cannot block unrelated update activity indefinitely.
if [ "$MODE" = fresh ] && [ "${FCP_FRESH_RESET_CONFIRMED:-}" != "1" ]; then
  confirm_fresh_reset || exit $?
fi

# The outer launcher becomes a tiny lease owner. The child reruns this same
# script with a marker inherited only from the lease process; from that point
# source proof/build, reset/resume, every Compose read, readiness, and updater
# startup all happen before the one checkout mutation boundary is released.
if [ "${FCP_HOST_MUTATION_LEASE_ACTIVE:-}" != "1" ]; then
  unset FCP_BUILD_COMMIT
  case "$MODE" in
    fresh)
      exec python3 -m catalog.federation.host_mutation \
        --repo-root "$ROOT" -- sh "$ROOT/start.sh" --fresh
      ;;
    resume)
      exec python3 -m catalog.federation.host_mutation \
        --repo-root "$ROOT" -- sh "$ROOT/start.sh" --resume
      ;;
    *)
      exec python3 -m catalog.federation.host_mutation \
        --repo-root "$ROOT" -- sh "$ROOT/start.sh"
      ;;
  esac
fi

if [ "$MODE" = fresh ]; then
  # Build while the current runtime is still available. The parent launcher
  # owns the checkout lease across this build, reset, and later activation; the
  # build primitive therefore reuses that lease rather than reacquiring it.
  build_core_images

  echo
  echo "Stopping FCP before resetting mutable application state..."
  if ! python3 "$ROOT/scripts/posix/stop_fcp_for_fresh_reset.py"; then
    echo "FCP containers could not be stopped safely. Nothing else was removed." >&2
    exit 1
  fi

  echo "Resolving and clearing mutable FCP state while preserving recordings..."
  if ! docker compose run --rm --no-deps --entrypoint python flask \
    -m catalog.flask_app.services.device_state_reset; then
    echo "Fresh factory reset did not complete. Review the specific path or recording-integrity error above." >&2
    echo "No FCP service will be started from an unverified reset." >&2
    exit 1
  fi

  echo "Verifying factory-reset state before any FCP service can recreate runtime state..."
  if ! docker compose run --rm --no-deps --entrypoint python flask \
    -m catalog.flask_app.services.device_state_reset --verify-fresh; then
    echo "Fresh factory reset could not be verified." >&2
    echo "No FCP service will be started. Review the specific verification failure above." >&2
    exit 1
  fi

  echo "Fresh factory reset completed and verified. FCP will now start with first-administrator bootstrap."
  echo
fi

if [ -z "${FCP_BUILD_COMMIT:-}" ]; then
  build_core_images
fi

AGENT_DIR="$FCP_DATA_DIR/federation/update-agent"
mkdir -p "$AGENT_DIR"

echo "Starting required Federation relay and managed recorder ..."
docker compose up -d relay recorder

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

FCP_AI_DEGRADED=0
echo "Starting optional Ollama service ..."
if ! docker compose up -d ollama; then
  FCP_AI_DEGRADED=1
  echo "WARNING: Ollama is unavailable; core FCP remains running." >&2
else
  echo "Ensuring optional Ollama model through host resource admission: $FCP_AI_MODEL"
  if ! python3 -m catalog.federation.model_resource_pull \
    --repo-root "$ROOT" --target ollama --model "$FCP_AI_MODEL"; then
    FCP_AI_DEGRADED=1
    echo "WARNING: AI capability is unavailable or resource-paused; core FCP remains running." >&2
  fi
fi

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

# The update agent must not inherit the launcher's lease marker: the outer lease
# process still owns the real lock for the few remaining lines, and later apply
# work must acquire that lock independently after this launcher returns.
FCP_HOST_MUTATION_LEASE_ACTIVE= FCP_HOST_MUTATION_LEASE_OWNER_PID= \
nohup python3 "$ROOT/scripts/posix/fcp_update_agent.py" \
  --repo-root "$ROOT" \
  --data-directory "$FCP_DATA_DIR" \
  >>"$AGENT_DIR/agent.log" 2>&1 </dev/null &

printf '\nFCP is running:       %s\n' "$BASE_URL"
printf 'Federation:           %s/federation\n' "$BASE_URL"
printf 'Running build commit: %s\n' "$FCP_BUILD_COMMIT"
printf 'Device data:          %s\n' "$FCP_DATA_DIR"
printf 'Update agent log:     %s/agent.log\n' "$AGENT_DIR"
if [ "$FCP_AI_DEGRADED" -eq 0 ]; then
  printf 'AI capability:        ready (%s)\n' "$FCP_AI_MODEL"
else
  printf 'AI capability:        unavailable; core FCP is healthy\n'
fi

if [ "$MODE" = resume ]; then
  case "$RESUME_EXIT" in
    0) echo "Saved identity, Federation membership, and capability evidence were resumed." ;;
    4) echo "Federation resumed; saved capability evidence needs explicit review." ;;
    2) echo "No saved Federation membership was found; onboarding remains required." ;;
    *) echo "Federation resume needs guided repair." ;;
  esac
fi
