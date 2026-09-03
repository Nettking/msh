#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: v1_prepare_linux.sh <commit> <host-id> <profile> [operator] [action]

profiles: local-ai | cnc-recorder | school-control
actions:
  prepare   initialize the campaign, register this host, take a baseline sample
  automate  run every automated P01-P12 probe this host is allowed to run
  report    machine-readable campaign progress with the next command per assertion
  sample    record one resource sample with the P12 soak series attached
  status    base campaign status
  privacy   seal the redacted evidence tree
  validate  strict P07/P12 release decision
USAGE
  exit 2
}

[[ $# -ge 3 ]] || usage
commit="$1"
host_id="$2"
profile="$3"
operator="${4:-Martin}"
action="${5:-prepare}"

[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || { echo "commit must be a 40-character lowercase SHA" >&2; exit 2; }
[[ "$host_id" =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,47}$ ]] || { echo "invalid host id" >&2; exit 2; }
case "$profile" in
  local-ai|cnc-recorder|school-control) ;;
  *) echo "profile must be local-ai, cnc-recorder or school-control" >&2; exit 2 ;;
esac

checkout="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$checkout"

command -v python >/dev/null 2>&1 || { echo "Python 3.11 or newer is required" >&2; exit 2; }

campaign() {
  python -m scripts.acceptance.v1_physical_campaign \
    --checkout "$checkout" \
    --evidence-root evidence/v1-physical \
    "$@"
}

runner() {
  python -m scripts.acceptance.v1_physical_runner \
    --checkout "$checkout" \
    --evidence-root evidence/v1-physical \
    "$@"
}

strict() {
  python -m scripts.acceptance.v1_physical_campaign_strict \
    --checkout "$checkout" \
    --evidence-root evidence/v1-physical \
    "$@"
}

# Non-timed scenarios only. P07 and P12 evidence must be bound to an explicit
# begin/finish run id, so they are never started implicitly by a wrapper.
UNTIMED_SCENARIOS=(P01 P02 P03 P04 P05 P06 P08 P09 P10 P11)

case "$action" in
  prepare)
    campaign init --commit "$commit" --operator "$operator"
    campaign host --commit "$commit" --host "$host_id" --role "$profile" --profile "$profile"
    runner sample --commit "$commit" --host "$host_id" --scenario P01 --label pre-campaign-baseline
    runner report --commit "$commit" --host "$host_id" || true
    echo "POSIX host prepared. Evidence is under evidence/v1-physical/."
    ;;
  automate)
    failed=0
    for scenario in "${UNTIMED_SCENARIOS[@]}"; do
      runner scenario --commit "$commit" --host "$host_id" --scenario "$scenario" || failed=1
    done
    runner report --commit "$commit" --host "$host_id" || true
    # A non-zero automated pass is normal while physical work is outstanding.
    exit "$failed"
    ;;
  report)
    runner report --commit "$commit" --host "$host_id"
    ;;
  sample)
    runner sample --commit "$commit" --host "$host_id" --scenario P01 --label operator-sample
    ;;
  status)
    campaign status --commit "$commit"
    ;;
  privacy)
    campaign privacy --commit "$commit"
    ;;
  validate)
    strict validate --commit "$commit"
    ;;
  *)
    usage
    ;;
esac
