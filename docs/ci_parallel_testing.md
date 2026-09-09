# Parallel release testing

The release workflow divides work into jobs so separate machines can contribute.
Each registered runner still executes one job at a time; Windows and WSL on the
same host share that host's resources.

| Work | Independently scheduled jobs | Runner label |
| --- | ---: | --- |
| Linux regression suite | 4 disjoint shards | `fcp-test-linux` |
| Windows release regressions | 3 existing test groups | `fcp-test-windows` |
| Compile, Go, lint, manifest and Compose configuration | 1 per OS | `fcp-test-linux` / `fcp-test-windows` |
| Full-suite order independence | 2 complete runs, fixed and rotating seed | `fcp-test-linux` |
| PostgreSQL integration | 1 disposable database job | `fcp-docker-linux` |

`fcp-test-linux` and `fcp-test-windows` are shared execution pools, not host names.
They require the interpreter and shell contract in
`.github/actions/self-hosted-python/action.yml`. The currently intended members
are Nettking and Beast on each OS. Add a machine only after the `CI test sharding`
workflow passes there and its release prerequisites are checked. Nitro is not
automatically admitted: its existing `fcp-linux` label alone does not establish
the pinned Python 3.12.13 contract. Do not create duplicate runners on a machine
just to increase the job count.

Windows test jobs do not start Docker containers. Nettking keeps its
NetworkService account; its Docker daemon access is not changed. Compose
configuration validation only uses the installed CLI. `fcp-docker-linux` requires
the same Linux contract plus successful Buildx and isolated PostgreSQL startup,
query and cleanup in `CI test sharding`. Docker integration can then use either
admitted Linux machine.

## Coverage and failure behavior

Every Linux shard collects the complete suite. `scripts/ci_pytest_shards.py`
groups collected test IDs by file, then balances files by test count. Allocation
is deterministic and independent of collection order. A file and its
module-scoped fixtures stay in one worker. Newly collected tests automatically
participate; there is no maintained allowlist that can omit new directories.

Each job uploads a manifest with the complete collection, selected tests,
candidate SHA and pytest exit code. The aggregate refuses missing shards,
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

Rerun failed jobs through GitHub Actions. Artifact names are stable within a run
and overwritten for the retried shard; successful shards from the same candidate
remain usable. Earlier run attempts still retain their job logs.

## Local checks

```sh
python -m pytest -o addopts= -q catalog/common/tests/test_ci_pytest_shards.py
python -m scripts.ci_pytest_shards run --index 0 --count 4 \
  --manifest /tmp/release-shards/shard-0.json -- -o addopts= -q
```

To validate coverage, execute all four indexes with the same `GITHUB_SHA`, then:

```sh
python -m scripts.ci_pytest_shards verify --directory /tmp/release-shards \
  --count 4 --source-sha "$GITHUB_SHA"
```

Full release qualification remains tied to one candidate commit. Existing
product failures must be fixed and requalified; changing job boundaries is not
evidence that those failures disappeared.
