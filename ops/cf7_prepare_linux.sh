#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 <local-ai|cnc-recorder|school-control> <40-char-commit> [operator] [init|preflight|gate|all]" >&2
}

if [[ $# -lt 2 || $# -gt 4 ]]; then
  usage
  exit 2
fi

machine="$1"
commit="$2"
operator="${3:-Martin}"
action="${4:-all}"

case "$machine" in
  local-ai|cnc-recorder|school-control) ;;
  *) usage; exit 2 ;;
esac

if [[ ! "$commit" =~ ^[0-9a-f]{40}$ ]]; then
  echo "Commit must be one lowercase 40-character SHA." >&2
  exit 2
fi

case "$action" in
  init|preflight|gate|all) ;;
  *) usage; exit 2 ;;
esac

checkout="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$checkout"

if ! command -v python >/dev/null 2>&1; then
  echo "Python is not available on PATH. Install Python 3.11 or newer." >&2
  exit 2
fi

run_readiness() {
  local subcommand="$1"
  local args=(
    -m ops.cf7_physical_readiness
    --checkout "$checkout"
    --evidence-root evidence
    "$subcommand"
    --machine "$machine"
    --commit "$commit"
  )
  if [[ "$subcommand" == "init" ]]; then
    args+=(--operator "$operator")
  fi
  python "${args[@]}"
}

case "$action" in
  init) run_readiness init ;;
  preflight) run_readiness preflight ;;
  gate) run_readiness gate ;;
  all)
    run_readiness init
    run_readiness preflight
    run_readiness gate
    ;;
esac

echo "CF7 readiness completed for $machine. This is not final physical acceptance."
