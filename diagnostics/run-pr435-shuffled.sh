#!/usr/bin/env bash
# Run as Nettking-Linux service account gha. Only writes fresh task-owned paths.
set -euo pipefail
bundle=$1
root=$2
candidate=ba8a3b0b828f59c36c5aaaf6130480a2432a5578
main=6101c86d94294c70db47d1a8053cac93b9a41356
image=python:3.12.13-bookworm
test "$(id -un)" = gha
case "$root" in /home/gha/qualification/pr435-shuffle-*) ;; *) exit 90;; esac
test ! -e "$root"
mkdir -p "$root"
exec > >(tee "$root/controller.log") 2>&1
echo "CONTROLLER_PID=$$"
date -u +%FT%TZ
hostname
id
sha256sum "$bundle" "$0"
docker image inspect "$image" --format '{{.Id}} {{json .RepoDigests}}'
printf 'leg\tseed\tsha\tsetup_rc\tpytest_rc\tclean_rc\twall_seconds\n' > "$root/results.tsv"

snapshot() {
  date -u +%FT%TZ
  uptime
  free -b
  df -B1 "$root" /tmp
  cat /proc/pressure/cpu /proc/pressure/memory /proc/pressure/io
  cat /proc/diskstats
  ps -eo user,pid,ppid,stat,etime,pcpu,pmem,comm | grep -E 'python|pytest|Runner|docker' || true
  docker ps --format '{{.ID}} {{.Names}} {{.Status}}'
}

run_leg() {
  local label=$1 sha=$2 seed=$3
  local leg="$root/$label" sample_pid started ended setup_rc=0 pytest_rc=99 clean_rc=0
  mkdir "$leg" || exit 91
  git clone --no-checkout "$bundle" "$leg/checkout" > "$leg/checkout.log" 2>&1 || exit 92
  git -C "$leg/checkout" checkout --detach "$sha" >> "$leg/checkout.log" 2>&1 || exit 93
  test "$(git -C "$leg/checkout" rev-parse HEAD)" = "$sha" || exit 94
  test -z "$(git -C "$leg/checkout" status --porcelain)" || exit 95
  echo "START leg=$label sha=$sha seed=$seed"
  snapshot > "$leg/host-before.txt"
  started=$(date +%s)
  (while true; do snapshot; sleep 15; done) > "$leg/host-samples.txt" 2>&1 &
  sample_pid=$!
  set +e
  docker run --rm --name "${root##*/}-$label" --network host \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v "$leg/checkout:/workspace" -v "$leg:/evidence" \
    -e PYTHONPYCACHEPREFIX=/tmp/pycache -e SHUFFLE_SEED="$seed" \
    -w /workspace "$image" bash -lc '
      set -euo pipefail
      python --version
      python scripts/ci_release_disk_preflight.py /workspace /tmp
      python -m pip install --quiet --upgrade pip==26.2.1
      python -m pip install --quiet -r requirements.txt -c constraints-release.txt
      python -m pip install --quiet pytest==9.1.1 pytest-randomly==4.1.0 -c constraints-release.txt
      python -m pip freeze > /evidence/pip-freeze.txt
      echo SETUP_PASS
      printf "0\n" > /evidence/setup.rc
      set +e
      python -m pytest -o addopts= -o cache_dir=/tmp/pytest_cache -v --durations=25 -p randomly --randomly-seed="$SHUFFLE_SEED" --junitxml=/evidence/pytest.xml
      rc=$?
      printf "%s\n" "$rc" > /evidence/pytest.rc
      exit "$rc"
    ' > "$leg/test.log" 2>&1
  setup_rc=$?
  set -e
  kill "$sample_pid"
  wait "$sample_pid" 2>/dev/null || true
  ended=$(date +%s)
  if test -f "$leg/setup.rc"; then setup_rc=0; fi
  if test -f "$leg/pytest.rc"; then pytest_rc=$(cat "$leg/pytest.rc"); fi
  docker run --rm -v "$leg/checkout:/ws" "$image" chown -R "$(id -u):$(id -g)" /ws || exit 96
  git -C "$leg/checkout" rev-parse HEAD > "$leg/final-sha.txt" || exit 97
  git -C "$leg/checkout" status --porcelain > "$leg/final-status.txt" || exit 98
  if test -s "$leg/final-status.txt" || test "$(cat "$leg/final-sha.txt")" != "$sha"; then clean_rc=1; fi
  snapshot > "$leg/host-after.txt"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$label" "$seed" "$sha" "$setup_rc" "$pytest_rc" "$clean_rc" "$((ended-started))" >> "$root/results.tsv"
  tail -n 6 "$leg/test.log"
  echo "FINISH leg=$label setup_rc=$setup_rc pytest_rc=$pytest_rc clean_rc=$clean_rc"
  date -u +%FT%TZ
  if test "$setup_rc" != 0 || test "$pytest_rc" != 0 || test "$clean_rc" != 0; then return 1; fi
}

for seed in 20260813 15; do
  if run_leg "candidate-$seed" "$candidate" "$seed"; then
    :
  else
    echo "Candidate leg failed; identical same-host main control follows immediately."
    run_leg "main-$seed" "$main" "$seed" || true
  fi
done
echo ALL_LEGS_COMPLETED
cat "$root/results.tsv"
date -u +%FT%TZ
