#!/bin/sh
set -eu
cd "$(dirname "$0")"
command -v docker >/dev/null 2>&1 || { echo "Docker is required; no source build is needed." >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "Docker daemon unavailable." >&2; exit 1; }
sha256sum -c READY_RUN_SHA256SUMS
IMAGE_REF=$(cat IMAGE_REF)
case "$IMAGE_REF" in fcp-icse-reviewer:*) ;; *) echo "Invalid image reference" >&2; exit 1 ;; esac
docker load -i image.tar.gz
mkdir -p results
[ ! -e results/PROVENANCE.json ] || { echo "Move results/ before running again." >&2; exit 1; }
docker run --rm --network none --cap-drop ALL --security-opt no-new-privileges \
  --mount "type=bind,source=$(pwd),target=/input,readonly" \
  --mount "type=bind,source=$(pwd)/results,target=/output" \
  "$IMAGE_REF"
echo "PASS: open results/network/operator-report.html"
