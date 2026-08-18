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

# Automatic joining runs on the host because only here can tailscaled say who a
# connecting peer really is. The container sees the Docker gateway instead, so
# the identity check has to live outside it. Both steps are best effort: if
# either fails, manual pairing codes still work exactly as before.
FCP_AUTO_JOIN_PORT=${FCP_AUTO_JOIN_PORT:-5151}
export FCP_AUTO_JOIN_PORT
RESPONDER_PID_FILE="$FCP_DATA_DIR/tailnet_join_responder.pid"
if command -v python3 >/dev/null 2>&1; then
  if [ -f "$RESPONDER_PID_FILE" ]; then
    OLD_PID=$(cat "$RESPONDER_PID_FILE" 2>/dev/null || true)
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
      kill "$OLD_PID" 2>/dev/null || true
    fi
    rm -f "$RESPONDER_PID_FILE"
  fi

  if ! python3 "$ROOT/catalog/federation/tailnet_join_responder.py" --check; then
    echo "Automatic Federation joining is unavailable; manual pairing codes still work." >&2
  else
    python3 "$ROOT/catalog/federation/tailnet_join_responder.py" \
      --bind "$FCP_TAILSCALE_IP" \
      --port "$FCP_AUTO_JOIN_PORT" &
    echo $! > "$RESPONDER_PID_FILE"
    echo "Automatic Federation joining is listening on $FCP_TAILSCALE_IP:$FCP_AUTO_JOIN_PORT"
  fi

  # If another Federation was discovered and this device is not a member yet,
  # ask it to authorize this device. Nothing is written unless it agrees.
  #
  # Skipped for --fresh: the factory reset runs after this point and clears the
  # whole data directory, so a grant fetched now would be destroyed before it
  # could be redeemed. The next normal start joins automatically.
  FCP_FRESH_REQUESTED=0
  for argument in "$@"; do
    if [ "$argument" = "--fresh" ]; then
      FCP_FRESH_REQUESTED=1
    fi
  done
  if [ "$FCP_FRESH_REQUESTED" = "1" ]; then
    echo "Factory reset requested; automatic Federation joining runs on the next normal start."
  else
    python3 "$ROOT/catalog/federation/tailnet_join_client.py" \
      --snapshot "$DISCOVERY_FILE" || true
  fi
fi

echo "FCP Tailscale address: $FCP_TAILSCALE_IP"
exec sh "$ROOT/start.sh" "$@"
