# Parallel release testing

The Federation v1 release gate is independent of any fixed runner device. It
uses fresh GitHub-hosted VMs, with the candidate's Python version pinned per
job. The separate `CI test sharding` workflow below remains a self-hosted pool
validation tool; its availability does not gate the v1 release run.

| Work | Independently scheduled jobs | Runner label |
| --- | ---: | --- |
| Linux regression suite | 4 disjoint shards | `ubuntu-24.04` |
| Windows release regressions | 3 existing test groups | `windows-2025` |
| Compile, Go, lint, manifest and Compose configuration | 1 per OS | `ubuntu-24.04` / `windows-2025` |
| Full-suite order independence | 2 complete runs, fixed and rotating seed | `ubuntu-24.04` |
| PostgreSQL integration | 1 disposable database job | `ubuntu-24.04` |

The release gate uses `.github/actions/hosted-python` to install the exact
Python version in a fresh job virtualenv, preserve the short Windows temp path
and Git Bash requirement, and put Linux test temporaries on runner disk. Before
storage-heavy tests, the workflow checks the product's existing disk threshold;
it does not lower product limits or depend on a host cleanup request. Test
evidence is uploaded as immutable GitHub Actions artifacts named with the run,
attempt and matrix item. The shard verifier checks exact candidate and coverage.
Public Actions artifacts have a maximum 90-day retention, and final release
closeout attaches the accepted evidence bundle to the GitHub Release asset.

The general-purpose `CI test sharding` workflow is separate and still uses the
registered `fcp-test-linux`, `fcp-test-windows`, and `fcp-docker-linux` pools.
Those pools require the interpreter and shell contract in
`.github/actions/self-hosted-python/action.yml`. To validate a particular
machine before admitting it to that workflow, dispatch it manually with
`target` set to `Nettking` or `Beast`; do not create duplicate runners to increase
parallelism. These shared pools do not run the Federation v1 release gate.

The v1 PostgreSQL integration job starts only its own disposable Docker
container on the GitHub-hosted Linux VM and verifies ownership before cleanup.
Windows release checks do not start Docker; Compose configuration validation
uses the installed CLI.

## Coverage and failure behavior

Every Linux shard collects the complete suite. `scripts/ci_pytest_shards.py`
groups collected test IDs by file, then balances files by test count. Allocation
is deterministic and independent of collection order. A file and its
module-scoped fixtures stay in one worker. Newly collected tests automatically
participate; there is no maintained allowlist that can omit new directories.

Each job uploads a manifest with the complete collection, selected tests,
actual Git commit/tree before and after pytest, and pytest exit code. The helper
requires a clean checkout root and rejects a mismatch with GitHub's candidate SHA.
A changed or dirty checkout cannot produce passing shard evidence; aggregates
also require identical, unchanged source identities across all shards. The aggregate refuses missing shards,
different collections or SHAs, duplicate or missing tests and any failed run.
JUnit reports and the 20 slowest tests are retained for tuning the allocation
against measured durations. Balancing by test count is a starting point, not a
claim that all shards take the same time.

The fixed-seed and rotating-seed order tests still run the **entire suite in one
pytest process per seed**. They are separate jobs with fresh checkouts. Sharding
does not replace these checks: splitting every test process would hide state
leaks between files. The test-cleanliness check runs even when pytest fails.

The existing check names `Release matrix (Linux)`, `Release matrix (Windows)`,
`Clean-checkout suite order independence` and
`Federation v1 automated release verdict` are retained as aggregate checks.
All shards and both full-order runs must succeed. The two release-matrix checks
wait for both OS regression groups and release checks; a failure on either OS
therefore fails both aggregate entries. No `continue-on-error`, test retries,
test exclusions or relaxed assertions are introduced.

Artifacts include the workflow run and attempt in their names, and are never
overwritten. A partial retry would lack artifacts from successful jobs in its
prior attempt, so rerun the full release workflow for a new attempt; it must
recreate the complete evidence set for one candidate/run. Earlier attempts and
their artifacts remain distinct until GitHub's retention deadline.

## Local checks

```sh
python -m pytest -o addopts= -q catalog/common/tests/test_ci_pytest_shards.py
python -m scripts.ci_pytest_shards run --index 0 --count 4 \
  --manifest /tmp/release-shards/shard-0.json -- -o addopts= -q
```

Run from the clean checkout root. To validate coverage, execute all four indexes
from the same exact commit, then:

```sh
python -m scripts.ci_pytest_shards verify --directory /tmp/release-shards \
  --count 4 --source-sha "$(git rev-parse HEAD)"
```

Full release qualification remains tied to one candidate commit. Existing
product failures must be fixed and requalified; changing job boundaries is not
evidence that those failures disappeared.
