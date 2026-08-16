#!/usr/bin/env sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"
exec python3 "$ROOT/headless_fcp.py" connect "$@"
