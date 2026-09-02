#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 <commit> <host-id> [role] [operator] [prepare|status|privacy|validate]" >&2
  exit 2
}

[[ $# -ge 2 ]] || usage
commit="$1"
host_id="$2"
role="${3:-physical-test-host}"
operator="${4:-Martin}"
action="${5:-prepare}"

[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || { echo "commit must be a 40-character lowercase SHA" >&2; exit 2; }
[[ "$host_id" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,47}$ ]] || { echo "invalid host id" >&2; exit 2; }

checkout="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$checkout"

command -v python >/dev/null 2>&1 || { echo "Python 3.11 or newer is required" >&2; exit 2; }

campaign() {
  python -m scripts.acceptance.v1_physical_campaign \
    --checkout "$checkout" \
    --evidence-root evidence/v1-physical \
    "$@"
}

case "$action" in
  prepare)
    campaign init --commit "$commit" --operator "$operator"
    campaign host --commit "$commit" --host "$host_id" --role "$role"
    campaign sample --commit "$commit" --host "$host_id" --scenario P01 --label pre-campaign-baseline
    campaign status --commit "$commit"
    echo "POSIX host prepared. Evidence is under evidence/v1-physical/."
    ;;
  status)
    campaign status --commit "$commit"
    ;;
  privacy)
    campaign privacy --commit "$commit"
    ;;
  validate)
    campaign validate --commit "$commit"
    ;;
  *)
    usage
    ;;
esac
