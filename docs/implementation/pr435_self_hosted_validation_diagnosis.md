# PR 435 self-hosted validation: diagnosis of the red Windows storage gate

Status: the failure is **pre-existing and environment-determined**. It is not a
regression introduced by PR 435, and it is not flaky.

## The failure

`PR 435 self-hosted Federation v1 validation`, run
[33956840831](https://github.com/Nettking/msh/actions/runs/33956840831), job
**Release matrix (Windows)** on the `Nettking` runner, step 10 **Windows
transport, storage, and failover regressions**:

```
6 failed, 347 passed, 1 skipped, 14 errors in 72.33s (0:01:12)
```

Every one of the six failures and all fourteen setup errors carry the same
error, raised at the first `ingest_batch` of each scenario:

```
catalog\federation\phase_d_client.py:235: FederationValidationError
E  catalog.federation.errors.FederationValidationError: content: committing this
   batch would take the volume below its reserved free-space floor
   (allocation-exhausted)
```

Steps 1-9 of that job all passed, including **Windows capability and product
release regressions** (`871 passed, 1 skipped`).

The new PR 435 test file also passed, on the same runner, inside the same
failing step. Step 10's first progress line is

```
....s..............................................................F.FEE [ 19%]
```

and the collected counts for the files ahead of the first failure are
`test_identity.py` 11, `test_state.py` 16, `test_client.py` 21 and
`test_multi_recorder_capability_identity.py` 17, which is 65. The first `F` is
at position 68, the third test in `test_storage_agent.py`. **All 17 tests of
the capability-identity regression PR 435 adds passed on Windows.**

Steps 11-14 (Go, Ruff, Compose, diff hygiene) were skipped only because step 10
had already failed, which is why the wrapper now runs them first.

## Root cause

`StorageAllocation.claim` in `catalog/federation/storage_allocation.py` refuses,
on the unbudgeted path, any commit that would take the volume below a derived
free-space floor:

```python
free = self._volume_free()
if self.budget_bytes is None:
    if free - nbytes < self.floor_bytes:
        raise _exhausted(
            "content",
            "committing this batch would take the volume below its "
            "reserved free-space floor",
        )
```

The floor comes from `default_floor_bytes`, which is
`min(max(10 GiB, 5% of the volume), 64 GiB)`. The node storage tests construct
their agent with `storage_floor_bytes=None`, so they get that derived floor.
The outcome therefore depends only on the free space of the volume pytest writes
`tmp_path` into.

Measured on the `Nettking` runner (run
[33958109488](https://github.com/Nettking/msh/actions/runs/33958109488), step
**Measure the volumes the suite writes into**), for the workspace, `RUNNER_TEMP`
and the pytest temp root, which are all on `C:`:

```
All local volumes:
  C:\        18.99 GiB free of   1906.46 GiB

workspace: C:\actions-runner\_work\msh\msh
  volume            : 18.99 GiB free of 1906.46 GiB
  derived allocation floor : 64.00 GiB
  free minus floor         : -45.01 GiB
  admission pressure       : NORMAL
```

A 1906.46 GiB volume puts the proportional term at 95.32 GiB, so the floor is
the 64 GiB ceiling. The runner holds 18.99 GiB. **Every unbudgeted storage
commit on that host refuses, by design, and it is short by 45.01 GiB.**

### Why the existing preflight did not catch it

`scripts/ci_release_disk_preflight.py` asserts the *host-resource admission*
policy in `catalog/federation/host_resources.py`, whose PRESSURE threshold is
12 GiB free. 18.99 GiB clears it, so the preflight reports "Runner meets the
product host-resource precondition" while the *allocation* floor -- a separate,
volume-proportional bound -- is breached by 45 GiB. The two floors coincide only
on volumes below roughly 240 GiB. Hosted `windows-latest` images are small
enough that the release gate never met this case.

## Evidence that it is not a PR 435 regression

### 1. The PR does not touch any of the code involved

```
$ git diff --stat 6101c86d94294c70db47d1a8053cac93b9a41356...13967aea9f4561cea64b5427bdf572de823ec577
 .github/workflows/federation-v1-release.yml           |    1 +
 .github/workflows/recorder-federation-publication.yml |    2 +
 catalog/mtconnect_recorder/federation_node.py         |  211 +++-
 .../test_multi_recorder_capability_identity.py        | 1009 ++++++++++++++++++
 catalog/node/client.py                                |   17 +-
 docs/implementation/federated_session_contracts.md    |    9 +
```

`storage_allocation.py`, `local_storage.py`, `phase_d_client.py` and
`node/storage_agent.py` are untouched. The only non-recorder change is an
error-path branch in `RelayNodeClient` that drops a cached announcement on
`capability-identity-conflict`; it cannot reach disk accounting.

### 2. Unmodified `main` fails identically on the same runner

Run [33958109488](https://github.com/Nettking/msh/actions/runs/33958109488) ran
the same storage subset twice on `Nettking`, once at `main`
`6101c86d94294c70db47d1a8053cac93b9a41356` and once at the PR head
`13967aea9f4561cea64b5427bdf572de823ec577`:

```
python -m pytest -o addopts= -q \
  catalog/node/tests/test_storage_agent.py \
  catalog/node/tests/test_three_machine_deployment.py \
  catalog/node/tests/test_physical_storage_failover.py \
  catalog/node/tests/test_physical_storage_recovery.py \
  catalog/node/tests/test_live_storage_replication.py \
  catalog/node/tests/test_live_storage_failover.py \
  catalog/node/tests/test_live_storage_catchup.py \
  catalog/node/tests/test_live_storage_reinstatement.py
```

Both legs: `6 failed, 4 passed, 14 errors`, the same test ids, the same
`allocation-exhausted` message. The main baseline carries none of PR 435 and
fails exactly the same way.

### 3. The same subset passes on a host that clears the floor

On a 251.97 GiB volume with 27.84 GiB free (floor 12.60 GiB), the same command
gives `24 passed` at the PR head and `24 passed` at `main`.

### 4. Deterministic, not flaky

Reproduced by constraining the volume rather than by patching the product: a
392.65 GiB ext4 loopback filesystem reduced to 13.61 GiB free, which sits above
the 12 GiB admission floor and below the 19.63 GiB allocation floor -- the same
window the Nettking runner is in.

```
mkfs.ext4 -m 0 -F bigvol.img && mount -o loop bigvol.img bigvol
fallocate -l 379G bigvol/filler
python -m pytest -o addopts= -q --basetemp=bigvol/bt <the eight files above>
```

Result at the PR head and at `main`: `6 failed, 4 passed, 14 errors`, identical
ids to CI. Three consecutive repeats of the two-file subset gave
`2 failed, 3 passed` every time. The refusal is a pure comparison of free bytes
against the floor; there is no timing or ordering component.

## The Linux side of the same run

Two further wrapper defects, also environmental:

* **suite-order-independence** was cancelled by its own `timeout-minutes: 30`
  at 49% of the first of two full suites. Nitro runs one full suite in about
  52 minutes. Nothing had failed: the progress output is dots and skips only.
* **PostgreSQL storage release check** failed at `actions/checkout` with
  `EACCES: permission denied, unlink '.../.pytest_cache/.gitignore'`. The Linux
  suites run as root inside a container over the bind-mounted workspace, so the
  files they leave behind cannot be removed by the unprivileged runner account.

Nitro itself clears both floors comfortably: 832.19 GiB free of 915.81 GiB
against a 45.79 GiB allocation floor.

## What was changed

Only `.github/workflows/self-hosted-pr435-validation.yml`, on the temporary
`ci/self-hosted-pr435` branch. No product code, no thresholds, no test scope,
no machine-wide Windows configuration, no runner service account change. Every
job still checks out and verifies
`13967aea9f4561cea64b5427bdf572de823ec577`.

* Both preconditions are asserted, on Linux and Windows, with the product's own
  unchanged policy loaded from the validated checkout, so a short runner is
  reported as an infrastructure fact with its exact deficit instead of
  surfacing as a product error deep inside unrelated fixtures.
* The Windows job runs its disk-independent steps before the storage subset, so
  a short host no longer masks the Go, Ruff, Compose and diff-hygiene results.
* CI reclaims only its own scratch on Windows (stale pytest temp trees, pip
  cache). On this runner that is about 0.3 GiB and does not close a 45 GiB gap.
* Linux timeouts are sized from Nitro's measured throughput.
* The containerised suites keep their bytecode and pytest caches out of the
  bind mount, and workspace ownership is returned to the runner account both
  before checkout and after the suite, so a job stays recoverable whatever an
  earlier run left behind. The after-the-suite repair alone was not enough:
  run 33958636444's Linux job still failed at checkout on leftovers from run
  33956840831, because the repair could only run once checkout had already
  given up.

## A second, unrelated defect

While the gate was being diagnosed, the full suite turned up a separate flake
in PR 435's own new test: `test_local_state_restored_from_a_stale_backup_reconverges`
restores a WAL-mode SQLite database by copying only its main file, so the
reopened node state is a mixture of the backup and the live WAL and the node's
replay check refuses it. It has nothing to do with disk space and would have
reddened the Linux jobs on its own. See
[pr435_stale_backup_restore_flake.md](pr435_stale_backup_restore_flake.md).
It is fixed on the product branch, whose head is now
`b2a7c6e5fb68bc4d4dbbb57322ca496feb1438a9`; the validation branch validates
that SHA.

## What still blocks a green gate

An operator has to free at least **45.03 GiB on the Nettking `C:` volume**, or
put the runner's work directory and `TEMP` on a volume whose own derived floor
it can clear. Nothing inside CI can do this: the runner's own caches are about
0.3 GiB, and the remaining 1887 GiB is the workstation's data, which must be
preserved.

The floor is `min(max(10 GiB, 5% of the volume), 64 GiB)`, so a smaller volume
asks for less, not more:

| Volume | Free space the storage tests need |
| ---: | ---: |
| up to 200 GiB | 10 GiB |
| 400 GiB | 20 GiB |
| 900 GiB | 45 GiB |
| 1280 GiB and above | 64 GiB |

Nitro sits in the third row and clears it with 832 GiB free. `C:` on Nettking
is in the last row with 18.97 GiB.

Lowering `MAXIMUM_FLOOR_BYTES`, configuring `storage_floor_bytes` for CI, or
skipping the storage tests would all turn the gate green without changing the
fact the product is reporting, and are deliberately not done here.
