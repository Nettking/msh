#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"

START_MODE=""
INITIALIZE_FEDERATION=0
for argument in "$@"; do
  case "$argument" in
    --fresh|--resume)
      if [ -n "$START_MODE" ]; then
        echo "Conflicting startup modes: $START_MODE and $argument" >&2
        exit 2
      fi
      START_MODE="$argument"
      ;;
    --initialize-federation)
      INITIALIZE_FEDERATION=1
      ;;
    --help|-h)
      cat <<'EOF'
Usage:
  sh start-tailscale.sh --fresh --initialize-federation
      First device only: reset, initialize the Federation, prompt once for admin credentials.
  sh start-tailscale.sh --fresh
      New trusted device: reset, discover, join, benchmark, and activate services automatically.
  sh start-tailscale.sh
      Normal restart with saved membership and evidence.
  sh start-tailscale.sh --resume
      Explicit reconnect before zero-touch reconciliation.
EOF
      exit 0
      ;;
    *)
      echo "Unknown option: $argument" >&2
      exit 2
      ;;
  esac
done

if ! command -v tailscale >/dev/null 2>&1; then
  echo "Tailscale was not found. Zero-touch Federation startup requires Tailscale." >&2
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "Python 3 is required for zero-touch Tailscale startup." >&2
  exit 2
fi

FCP_TAILSCALE_IP=$(tailscale ip -4 2>/dev/null | sed -n '1p' || true)
if [ -z "$FCP_TAILSCALE_IP" ]; then
  echo "Tailscale is installed but no logged-in IPv4 tailnet address is available." >&2
  exit 2
fi

: "${FCP_WEB_BIND:=$FCP_TAILSCALE_IP}"
: "${FCP_RELAY_BIND:=$FCP_TAILSCALE_IP}"
: "${FCP_WEB_PORT:=5000}"
: "${FCP_DATA_DIR:=$ROOT/data}"
: "${FCP_AUTO_JOIN_PORT:=5151}"
: "${FCP_FEDERATION_STORAGE_AUTHORITY_ENABLED:=1}"
: "${FCP_FEDERATION_STORAGE_AUTHORITY_RELAY:=ws://relay:8765}"
: "${FCP_PAIRING_RELAY_URL:=ws://$FCP_TAILSCALE_IP:8765}"
: "${FCP_HUMAN_AUTH_BASE_URL:=http://$FCP_TAILSCALE_IP:$FCP_WEB_PORT}"
FCP_TAILSCALE_DISCOVERY_PORT=$FCP_WEB_PORT
FCP_AUTO_JOIN_APP_URL="http://$FCP_TAILSCALE_IP:$FCP_WEB_PORT"
export FCP_WEB_BIND FCP_RELAY_BIND FCP_WEB_PORT FCP_DATA_DIR FCP_AUTO_JOIN_PORT
export FCP_FEDERATION_STORAGE_AUTHORITY_ENABLED FCP_FEDERATION_STORAGE_AUTHORITY_RELAY
export FCP_PAIRING_RELAY_URL FCP_HUMAN_AUTH_BASE_URL
export FCP_TAILSCALE_DISCOVERY_PORT FCP_AUTO_JOIN_APP_URL

# Factory reset happens first. Discovery/enrollment afterwards prevents --fresh
# from deleting the snapshot or one-use grant before it can be redeemed.
if [ -n "$START_MODE" ]; then
  sh "$ROOT/start.sh" "$START_MODE"
else
  sh "$ROOT/start.sh"
fi

if ! python3 "$ROOT/scripts/federation_host_runner.py" tailnet_join_responder --check; then
  echo "Automatic Federation joining is unavailable; refusing zero-touch startup." >&2
  exit 2
fi
python3 "$ROOT/scripts/federation_host_runner.py" tailnet_join_responder \
  --bind "$FCP_TAILSCALE_IP" \
  --port "$FCP_AUTO_JOIN_PORT" \
  --app-url "$FCP_AUTO_JOIN_APP_URL" &

if [ "$INITIALIZE_FEDERATION" = "1" ]; then
  python3 "$ROOT/scripts/zero_touch_federation_start.py" \
    --initialize-federation \
    --web-port "$FCP_TAILSCALE_DISCOVERY_PORT"
else
  python3 "$ROOT/scripts/zero_touch_federation_start.py" \
    --web-port "$FCP_TAILSCALE_DISCOVERY_PORT"
fi

echo "FCP Tailscale address: $FCP_TAILSCALE_IP"
echo "Zero-touch startup is complete."
