#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"

if ! command -v tailscale >/dev/null 2>&1; then
  echo "Tailscale was not found. Falling back to the normal FCP launcher." >&2
  exec sh "$ROOT/start.sh" "$@"
fi

FCP_TAILSCALE_IP=$(tailscale ip -4 2>/dev/null | sed -n '1p' || true)
if [ -z "$FCP_TAILSCALE_IP" ]; then
  echo "Tailscale is installed but no logged-in IPv4 tailnet address is available." >&2
  echo "Falling back to the normal FCP launcher." >&2
  exec sh "$ROOT/start.sh" "$@"
fi

: "${FCP_WEB_BIND:=$FCP_TAILSCALE_IP}"
: "${FCP_WEB_PORT:=5000}"
: "${FCP_DATA_DIR:=$ROOT/data}"
export FCP_WEB_BIND FCP_WEB_PORT FCP_DATA_DIR

DISCOVERY_FILE="$FCP_DATA_DIR/federation/onboarding/tailscale_discovery.json"
if command -v python3 >/dev/null 2>&1; then
  echo "Discovering FCP Federations through the existing Tailscale login..."
  if ! python3 -m catalog.federation.tailscale_host_discovery \
    --output "$DISCOVERY_FILE" \
    --web-port "$FCP_WEB_PORT"; then
    echo "Tailscale discovery failed safely; normal Federation onboarding remains available." >&2
  fi
else
  echo "python3 was not found, so pre-start Tailscale discovery is skipped." >&2
fi

echo "FCP Tailscale address: $FCP_TAILSCALE_IP"
exec sh "$ROOT/start.sh" "$@"
