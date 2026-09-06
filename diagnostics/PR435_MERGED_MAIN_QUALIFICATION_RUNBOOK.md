# Authoritative post-merge qualification runbook — merged main `bcf5c9ab`

Target identity, exact and non-negotiable:

```
MERGED_MAIN_SHA=bcf5c9ab2fb453cb26129b70d41fb64fc4863dd4
```

This identity does **not** inherit the candidate's PASS. Every leg below asserts
the exact SHA and a clean checkout before and after.

The merge session ran in an ephemeral cloud container with no route to
Nettking-Linux, Nettking Windows, Nitro or the MSH Recorder, and hosted GitHub
Actions remain blocked by CI001 (account-level Actions billing). The legs below
therefore have to be executed on the rig. Each is written to be run as-is.

## Common preamble (every Linux leg)

Fresh clone, exact SHA, clean tree, under runner account `gha`:

```sh
set -euo pipefail
MERGED=bcf5c9ab2fb453cb26129b70d41fb64fc4863dd4
git clone https://github.com/Nettking/msh checkout && cd checkout
git checkout "$MERGED"
test "$(git rev-parse HEAD)" = "$MERGED"          # exact-SHA assertion
test -z "$(git status --porcelain)"               # clean before
```

Toolchain, matching the qualified candidate legs exactly:

```
python:3.12.13-bookworm
pip==26.2.1
python -m pip install -r requirements.txt -c constraints-release.txt
python -m pip install pytest==9.1.1 ruff==0.16.3 -c constraints-release.txt
python -m pip install pytest-randomly==4.1.0 -c constraints-release.txt   # legs B and C only
```

Close every leg with:

```sh
test "$(git rev-parse HEAD)" = "$MERGED"          # exact-SHA assertion
test -z "$(git status --porcelain)"               # clean after
```

Run A, B and C **sequentially**, each in a fresh container from a fresh clone.

## Leg A — full default-order suite (Nettking-Linux)

```sh
python scripts/ci_release_disk_preflight.py
python -m pytest -o addopts= -o cache_dir=/tmp/pytest_cache \
  -p no:randomly -v --durations=50 --junitxml=/evidence/merged_default.xml
```

Expected, from the candidate's two passing legs: **3633 passed, 30 skipped**,
3663 collected, ~286-288s.

## Leg B — shuffled seed 20260813 (Nettking-Linux)

```sh
python -m pytest -o addopts= -o cache_dir=/tmp/pytest_cache \
  -p randomly --randomly-seed=20260813 -v --durations=25 \
  --junitxml=/evidence/merged_seed20260813.xml
```

Expected: **3633 passed, 30 skipped**, ~409s.

## Leg C — shuffled seed 15 (Nettking-Linux)

```sh
python -m pytest -o addopts= -o cache_dir=/tmp/pytest_cache \
  -p randomly --randomly-seed=15 -v --durations=25 \
  --junitxml=/evidence/merged_seed15.xml
```

Expected: **3633 passed, 30 skipped**, ~379s.

## Leg D — release/static checks (Nettking-Linux)

Same steps as the `Release matrix (ubuntu-latest)` job in
`.github/workflows/federation-v1-release.yml` on the merged tree:

```sh
python scripts/ci_release_disk_preflight.py     # host-resource precondition
python -m compileall -q catalog                 # compile maintained packages
# acceptance manifest shape: the inline python of the
#   "Validate acceptance manifest shape" step
python scripts/check_product_branding.py        # product-branding boundary
python -m ruff check <release scope> --ignore I001,RUF022,B008,C408,PLC0206,UP035
git diff --check HEAD^ HEAD                     # diff hygiene
(cd cmd/fcp-peer-sidecar && go mod tidy -diff && go test ./...)   # Go sidecar
```

Take the exact Ruff path list from the `Ruff maintained release boundaries`
step rather than retyping it.

All of leg D except the Go sidecar and Compose was already executed against the
exact merged SHA and passed; see `PR435_MERGE_AND_MERGED_MAIN_SCREEN.md`. Re-run
on the release host for the authoritative record.

## Leg E — actual Docker Compose validation (Nettking-Linux)

Not YAML parsing. The real plugin, as in the supplemental candidate check:

```sh
docker compose config --quiet
```

Record the Compose version and image digest, source mounted read-only, no
network and no Docker socket, with checkout SHA and cleanliness verified
before and after.

## Windows leg — Nettking Windows self-hosted rig

Same Windows release gate used for `ba8a3b0b`, on the exact merged SHA, Python
3.12.10. Run both steps from
`.github/workflows/federation-v1-release.yml`:

- `Windows capability and product release regressions` — expected **871 passed, 1 skipped** (~220s)
- `Windows transport, storage, and failover regressions` — expected **367 passed, 1 skipped** (~84s)

plus the Go, Ruff, Compose, diff-hygiene and storage steps of that job. This leg
covers `catalog/mtconnect_recorder/tests/test_multi_recorder_capability_identity.py`,
which PR435 deliberately added to the Windows matrix.

Do not substitute Beast-Windows: its last attempt failed in `setup.ps1` under
PowerShell execution policy before any product test ran, and that remains an
environment/setup failure, not evidence.

## PostgreSQL leg — existing gate/service

```sh
export FCP_TEST_POSTGRES_DSN=postgresql://fcp:fcp@127.0.0.1:5432/fcp_test
python -m pytest -o addopts= -q catalog/federation/tests/test_phase_d2_postgres.py
```

Service `postgres:16-alpine`, `POSTGRES_USER=fcp`, `POSTGRES_PASSWORD=fcp`,
`POSTGRES_DB=fcp_test`, healthcheck `pg_isready -U fcp -d fcp_test`.

## Classification rule for any fast-host failure

The merged tree is byte-identical to `ba8a3b0b` (tree
`bcbd778ba8009eb79e3349534e623da3861b0e45`, `git diff ba8a3b0b bcf5c9ab` empty).
A fast-host difference therefore cannot come from merge content and is
especially important. Before any code change:

1. Re-run the failing leg on the exact merged SHA.
2. Run the same leg on exact `ba8a3b0b` and on exact `6101c86d` as controls.
3. Classify as product / environment / infrastructure on that evidence.
4. Only then decide on a change.

Do not skip, disable, quarantine, retry-wrap or re-deadline any test to reach
green.

### Known non-blocking local artifact

`catalog/federation/tests/test_tailnet_join_responder.py::test_a_matching_child_process_instance_can_be_terminated`
failed once in the non-authoritative local screen because that screen's uv
`python-build-standalone` CPython 3.12.11 lacks `os.pidfd_open`, so the
product's fail-closed path returns `False` in 6ms. It passes on the same host
and tree under an interpreter that has the syscall. `python:3.12.13-bookworm`
has it, so this is not expected on the release host. If it **does** fail there,
treat it as new evidence under the rule above.

## Verdict

Record only after legs A-E, the Windows leg and the PostgreSQL leg all pass on
the exact merged SHA:

```
MERGED_MAIN_SOFTWARE_QUALIFICATION: PASS
```

Then update `coord/federation-v1-release` and proceed to mandatory controlled
physical acceptance on Nettking, Nitro and the MSH Recorder — all three on this
same `MERGED_MAIN_SHA`, with identity negative control, restart/reconnect,
cleanup/rollback and final-state evidence. Federation v1 is not accepted until
that three-host campaign passes.
