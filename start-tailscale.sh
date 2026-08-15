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
: "${FCP_RELAY_BIND:=$FCP_TAILSCALE_IP}"
: "${FCP_DATA_DIR:=$ROOT/data}"
FCP_TAILSCALE_DISCOVERY_PORT=${FCP_WEB_PORT:-5000}
export FCP_WEB_BIND FCP_RELAY_BIND FCP_DATA_DIR

# Keep the public-safe discovery snapshot outside data/federation. A --fresh
# reset intentionally removes data/federation, but a fresh second device still
# needs this snapshot on its login screen to find the existing Federation.
DISCOVERY_FILE="$FCP_DATA_DIR/tailscale_discovery.json"
if command -v python3 >/dev/null 2>&1; then
  echo "Discovering FCP Federations through the existing Tailscale login..."
  # Execute the standalone stdlib-only file directly. Running it with -m would
  # first import catalog.federation.__init__ and accidentally require optional
  # PostgreSQL/libpq support on the host.
  if ! python3 "$ROOT/catalog/federation/tailscale_host_discovery.py" \
    --output "$DISCOVERY_FILE" \
    --web-port "$FCP_TAILSCALE_DISCOVERY_PORT"; then
    echo "Tailscale discovery failed safely; normal Federation onboarding remains available." >&2
  fi
else
  echo "python3 was not found, so pre-start Tailscale discovery is skipped." >&2
fi

echo "FCP Tailscale address: $FCP_TAILSCALE_IP"
exec sh "$ROOT/start.sh" "$@"
