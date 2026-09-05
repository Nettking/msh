# Federation v1: interim engineering handoff

Written by the interim engineer while Astra is away. Astra owns merge,
deployment and acceptance; nothing here has been merged, deployed or accepted.
This is the state to resume from.

Last updated: 2026-09-05, during self-hosted validation run 33960401244.

## 1-5. Identifiers

| What | Value |
| --- | --- |
| main | `6101c86d94294c70db47d1a8053cac93b9a41356` |
| PR #435 head | `f0434e4a4fd4cf86b3574563151d1e6241b4e06c` |
| PR #435 branch | `claude/federation-recorder-capability-id-19tqkk` |
| Validation branch | `ci/self-hosted-pr435` @ `dc160d63960a2b65618e06fed3e9ef54d5009fff` |
| `VALIDATED_SHA` in that workflow | `f0434e4a4fd4cf86b3574563151d1e6241b4e06c` (matches the PR head) |
| Latest validation run | [33971811540](https://github.com/Nettking/msh/actions/runs/33971811540) (supersedes 33960401244) |
| Diagnosis branch / PR | `claude/pr435-validation-diagnosis-n285av`, PR #436 (docs only) |
| Rig baseline branch | `ci/rig-readonly-baseline` |

### 3. Commits added

On **`claude/federation-recorder-capability-id-19tqkk`** (product, 2 added):

| SHA | Subject |
| --- | --- |
| `b2a7c6e` | test: restore the whole node state database, not just its main file |
| `f0434e4` | test: move the node state through SQLite instead of over the filesystem |

Both touch only
`catalog/mtconnect_recorder/tests/test_multi_recorder_capability_identity.py`.
No product code was changed by the interim engineer. `13967ae` and `5ca4d11`
are the original PR 435 commits.

On **`ci/self-hosted-pr435`** (validation wrapper, 4 added): `b529f10`,
`fccaf20`, `ae77a33`, `87310df`.

On **`claude/pr435-validation-diagnosis-n285av`** (documentation, PR #436):
`744c9ca`, `d32240b`, `9232cea`, `b40a4ef`, `9929055`, `6de6049`, `08a61a3`,
`487610a`, `dfc2177`, plus this note.

On **`ci/rig-readonly-baseline`** (read-only inventory): `99a1339`, `6b2a94a`,
and two follow-ups.

## 6. Validation job results (run 33960401244, head `f0434e4`)

| Job | Result |
| --- | --- |
| Release matrix (Windows) | **success** — all 16 steps |
| Release matrix (Linux) | **cancelled at its timeout**, 64% through, with one unnamed failure — see below |
| Clean-checkout suite order independence | queued behind Nitro |
| PostgreSQL storage release check | queued behind Nitro |
| Federation v1 automated release verdict | not yet reached |

Windows detail, all green: capability/product regressions, Go tidy+test, Ruff,
Compose config, diff hygiene, storage allocation precondition, and the
transport/storage/failover subset at **367 passed, 1 skipped** in 141 s.

Nitro serialises the three Linux jobs on one runner: roughly 55 min for the
full suite, 110 min for the two shuffled suites, then the PostgreSQL check.

## 7-8. Remaining software failures and their root causes

None open. Three were found and closed:

**(C) Self-hosted infrastructure — Windows storage floor.** The storage
fixtures refused with `allocation-exhausted`. `StorageAllocation` derives a
floor of `min(max(10 GiB, 5% of the volume), 64 GiB)`; Nettking's 1906 GiB `C:`
held 18.97 GiB against a 64.00 GiB floor. Proven environmental: unmodified
`main` failed identically on the same runner, the same subset passed on a host
that clears the floor, and it reproduced deterministically on a constrained
loopback filesystem. Resolved by the operator freeing disk. Detail:
[pr435_self_hosted_validation_diagnosis.md](pr435_self_hosted_validation_diagnosis.md).

**(B) Test defect in PR 435 — stale-backup restore.**
`test_local_state_restored_from_a_stale_backup_reconverges` restored a WAL-mode
SQLite database by copying only its main file, so the reopened node state mixed
the backup's watermark with the live WAL's event log and the node's replay check
refused it. 50 failures in 300 repetitions; 2 of 5 full-suite runs. Fixed at
`b2a7c6e`, then corrected at `f0434e4` after the first fix used `unlink`, which
Windows refuses while a handle is open (`PermissionError: [WinError 32]`).
Detail: [pr435_stale_backup_restore_flake.md](pr435_stale_backup_restore_flake.md).

**(C) Self-hosted infrastructure — Linux wrapper.** The order-independence job
was cancelled by its own 30-minute budget mid-suite, and the containerised suite
left root-owned files in the bind-mounted workspace so the next job's checkout
failed with `EACCES`. Both fixed in the wrapper.

### Open external blocker: GitHub Actions billing

Every **hosted**-runner check in this repository fails without starting. All 18
on PR #435 and all of PR #436's failed 2-5 seconds in with no runner assigned
and no steps recorded, each carrying GitHub's own annotation:

> The job was not started because recent account payments have failed or your
> spending limit needs to be increased. Please check the 'Billing & plans'
> section in your settings

Classification: **(C) infrastructure**, account level, outside the repository.
Nothing in the code or the workflows can clear it, and the official release
gate on `federation-v1-release.yml` cannot produce a green result until it is
resolved. PR #436 changes three Markdown files and is red in exactly the same
way, which is the cleanest proof the cause is not content.

**Consequence for the merge decision.** If branch protection requires any of
those hosted checks, PR #435 cannot merge while the block stands, however green
the self-hosted gate is. Astra should check the required-checks list against
what the self-hosted gate actually produces before planning the merge. Resolving
Actions billing on the account is the only unblock.

No workflow triggers were narrowed to route around this. Making a docs-only PR
look green by reducing the official release scope would be exactly the kind of
gate weakening the interim brief rules out.

### Why the Linux suite takes ~175 minutes on Nitro

Worth knowing before anyone reads the runtime as a hang. The rate is wildly
uneven: 72-test chunks took 19.3, 14.0 and 14.0 minutes early on and under a
second later. Mapping those chunks onto collection order, the slow ones are
dominated by durable-SQLite work —
`catalog/capabilities/tests/test_analysis_scheduling.py`,
`test_analysis_workspace_reconciliation.py`, the `test_efficiency_*` stores,
`test_durable_sqlite_resource_admission.py`, and the
`cf7_acceptance/test_physical_*` set.

Those tests write SQLite under `tmp_path`, which inside the release container
is `/tmp`. The preflight in the same job reports `/workspace` on `device:2050`
(the bind-mounted host filesystem) but `/tmp` on `device:139` — the container's
own overlay layer. Fsync-heavy SQLite on overlayfs is slow in exactly this
shape, and the same suite takes about 3.5 minutes on a host running it against
native ext4.

**Not changed, deliberately.** The obvious lever — giving the container a
tmpfs or host-backed `/tmp` — would alter what `shutil.disk_usage` reports for
the volume behind `tmp_path`, and that is precisely the number the storage
allocation floor is derived from. Changing it risks turning the storage tests
into a different test. `--durations=25` is now enabled on those runs, so the
next completed run will name the slow tests directly rather than leaving this
inferred from chunk timings.

### Open: an unnamed failure in the Linux full suite

Run 33960401244's Linux job was **cancelled at its 120-minute budget** having
reached 64% of the suite, and its progress output contains **exactly one `F`,
at about 60%**. Because the job ran under `-q` and was cancelled before pytest
printed a summary, that failure was never named. This is a real open item, not
a resolved one.

What is established:

* The suite was **progressing, not hung** — progress lines continued right up
  to the cancellation.
* Nitro is far slower than the extrapolation the timeout was based on. 72-test
  chunks took **19.3, 14.0 and 14.0 minutes** early on and under a second
  later. One full suite is about **175 minutes**, not the 52 previously
  assumed.
* The failure is **not in the Windows subset** — that job passed 367/1 skipped
  — so only the Linux full suite exercises it.

What is *not* established: which test it is. By index the `F` is the 2162nd
test in collection order, which locally is
`catalog/flask_app/tests/test_federation_pairing_relay.py::test_signed_pairing_code_projects_same_members_from_both_viewpoints`.
Treat that as a **candidate, not an identification**: Nitro collected roughly
3667-3712 tests against 3662 locally, so the index can drift by up to ~50, and
the whole neighbourhood is `catalog/flask_app/tests/test_federation_*`.

The candidate is at least plausible. It starts a real `RelayServer` with hard
five-second `auth_timeout_seconds`, `send_timeout_seconds` and
`timeout_seconds`, and five seconds is a short budget on a machine where 72
tests can take 19 minutes. But it did not reproduce locally: **50 runs clean**
at the PR head (30 plain, 20 more with attempted CPU load).

Classification so far: **not PR 435**. The PR's diff is six files, and the
only one on this test's dependency path is `catalog/node/client.py`, whose
change runs solely inside the cached-capability replay loop on a
`capability-identity-conflict` — a path this test does not exercise. Whether
it is (D) pre-existing on main or (E) load-sensitive cannot be settled until
the test is named.

**How it gets named.** `ci/self-hosted-pr435` @ `dc160d6` raises the Linux
budgets to the measured rate (300 minutes for the full suite, 420 for the two
shuffled ones) and switches those runs from `-q` to `-v --durations=25`. `-v`
names each test as it runs, so even a cancelled run says what failed, and the
durations report gives the evidence for the 175-minute runtime. Scope,
selection and thresholds are unchanged. Run
[33971811540](https://github.com/Nettking/msh/actions/runs/33971811540) is the
first under those settings.

## 9. Focused and repeated test evidence

| Check | Result |
| --- | --- |
| Stale-backup scenario, 300 repetitions in one process, before fix | 50 failed, 250 passed |
| Same, after fix | **300 passed** |
| `test_multi_recorder_capability_identity.py` (whole file) | 17 passed |
| Windows transport/storage/failover subset on Nettking | 367 passed, 1 skipped |
| Windows capability/product regressions on Nettking | 871 passed, 1 skipped |
| Full local suite at the pre-fix head, 8 runs | 2 failed (the same test), 6 clean |
| Ruff on the changed file, diff hygiene | clean |

## 10. Windows runner (Nettking) state

`C:` 141.73 GiB free of 1906.46 GiB — clears the 64.00 GiB allocation floor by
77.73 GiB, and the precondition step passes on the workspace, `RUNNER_TEMP` and
the pytest temp root. Runner service account is `NT AUTHORITY\NETWORK SERVICE`,
which cannot reach the Docker API (`permission denied ... npipe`) and holds no
SSH key.

## 11. Linux runner (Nitro) state

`/dev/sda2` 832.19 GiB free of 915.81 GiB against a 45.79 GiB derived floor, so
both product floors are cleared with room. One full suite takes about 52
minutes. Runner user `martin`, workspace
`/home/martin/actions-runner/_work/msh/msh`. The rig checkout at
`/home/martin/fcp` has **not** been inventoried yet — the Linux baseline job was
deliberately not run while the release gate is serialising on this machine.

## 12. Read-only physical rig baseline

Collected through the self-hosted runners, because the tailnet is not reachable
from the engineering environment. Nothing was started, stopped or reconfigured.

### Nettking — inventoried

| Item | Value |
| --- | --- |
| hostname | `Nettking` |
| rig checkout `C:\wsl\msh` | branch `main` @ `6101c86…` — **clean, pre-PR-435** |
| sibling directories | `C:\wsl\msh-archive-20260903-2145` and `C:\wsl\msh-new`, **neither a git checkout** — each holds only `data/` and `results/` |
| `C:\msh\git` | absent (that path belongs to MSH Recorder) |
| ports 5000, 8765 | bound on `127.0.0.1` **and `100.70.61.68`** by `com.docker.backend.exe` (pid 2500) |
| port 11434 | `ollama.exe` (pid 19704), `wslrelay.exe` (pid 7756) |
| Docker inventory | unavailable to the runner account |
| device node id | `node-UTKPPKDmI2UhdO9vJ_PX75ETb6S8G86T4nFjBXuaZ-A` |
| enrollment / connection | `enrolled` / **`connecting`** |
| session | `session-b9e513bf936f48218fc84b6d649c0c0e`, joined, revision 2985 |
| federation id | `federation-cf76b0ca87dd554b4423362b7a421d44` |

Cached advertised capabilities on that node:

```
recorder-local                              type=recorder        status=ready
candidate-9000e13d60308965cec68058e1c5fd54  type=compute         status=registering
candidate-f1f0738abdd43f48634dee1652bef58e  type=language-model  status=ready
candidate-70e917cb607c277fca7821fa31fc1d35  type=storage         status=registering
```

A second, unrelated standalone coordinator also exists on this machine at
`results/capabilities/standalone_control.sqlite3`: node
`node-ykWYGTjULcsBX8PI2tILGwdJog9o83Ht-0s--uHgDJ4`, session
`session-standalone-92002a52a9cc49d0bb0492f41b3c5ee4`, one
`background-analysis` capability, connectivity `disconnected`.

### Nitro — not yet inventoried

Its runner is busy with the release gate. The same workflow will collect it.

### MSH Recorder — not reachable

`ssh martin@msh-recorder.tail4ccd2b.ts.net` from the runner returns
`Permission denied (publickey,password,keyboard-interactive)`: the runner
service account has no key. It must be inventoried from an interactive session
on Nettking, or by adding a job on a runner that has credentials.

## 13. Federation / session state as observed

Nettking's device node is **enrolled but stuck in `connecting`**, re-writing
`updated_at` every few seconds without reaching `connected`, while still
holding a **`recorder-local`** capability in its local cache from
2026-08-16. That is the pre-PR-435 identity model on a pre-PR-435 build, and it
is the live form of the previous physical blocker rather than a new fault. No
logical-storage group could be observed as ready.

### Why this baseline matters for acceptance

The symptom on Nettking is the one PR 435 was written for, and the PR has a
regression test that asserts exactly this recovery:
`test_connect_drops_a_cached_identity_the_coordinator_reassigned`. It puts a
node in the state Nettking is in -- a locally cached
`recorder-local` the coordinator has since assigned elsewhere -- reconnects it,
and asserts three things:

```python
assert reconnected.connected_event.is_set()
assert reconnected.state.advertised_capabilities(session_id=SESSION_ID) == ()
assert rig.rows()[LEGACY_RECORDER_CAPABILITY_ID]["node_id"] == owner
```

That is: the node reaches connected instead of being stranded, it drops the
stale cache entry, and it does not take the identity away from the node that
legitimately owns it. `test_connect_still_fails_closed_on_an_unrelated_rejection`
guards the other direction, so the swallow is scoped to
`capability-identity-conflict` and nothing else.

So the expected physical outcome is specific and checkable: after deploying the
candidate, Nettking's device node should move from `connecting` to `connected`,
its `advertised_capabilities` should lose the `recorder-local` row, and the
recorder should re-announce under `recorder-{node_id}`. If it does not, that is
a genuine physical finding rather than a repeat of the known blocker.

## 14. Legacy / candidate classification

| Instance | Classification |
| --- | --- |
| `C:\wsl\msh` @ main `6101c86`, running behind Docker on 5000/8765 | **CURRENT baseline** — the accepted pre-PR-435 build, not a candidate |
| `C:\wsl\msh-archive-20260903-2145` | **LEGACY state archive** — `data/` and `results/` only, no code, dated 2026-09-03 |
| `C:\wsl\msh-new` | **UNKNOWN, state-shaped** — also `data/` and `results/` only, no code |
| standalone analysis coordinator in `results/capabilities` | **LEGACY** — separate session, disconnected |
| ollama / wslrelay on 11434 | supporting services, not FCP nodes |
| Nitro rig `/home/martin/fcp` | **UNKNOWN** — not yet inventoried |
| MSH Recorder `C:\msh\git`, `C:\msh\acceptance-6101c86` | **UNKNOWN** — unreachable |

## 15. Proposed safe deployment and cleanup plan

For Astra to approve or reject. Nothing below has been done.

1. Finish the software gate and confirm the verdict is green on `f0434e4`.
2. Inventory Nitro and MSH Recorder read-only, including
   `C:\wsl\msh-new` on Nettking, before touching anything.
3. Decide the fate of `msh-archive-20260903-2145` and `msh-new`. Neither is a
   checkout: both hold only `data/` and `results/`, so what they carry is
   state, not code. The code rollback is trivial — `C:\wsl\msh` is git, on
   `main` @ `6101c86`, clean — but that archive may be the only copy of the
   pre-2026-09-03 rig state, which cannot be regenerated. Do not delete either
   until the physical test has passed. `msh-new` is unexplained and worth
   understanding before deployment: a candidate pointed at it would start
   against those directories rather than the live ones.
4. Deploy the candidate to a **new** checkout per host rather than over the
   running one, so the pre-PR-435 stack remains the rollback.
5. Stop the old stack per host only at deployment time, in the order
   recorder → node → relay, and keep `data/` directories in place: the whole
   point of the fix is that a node converges from cached legacy state.
6. Expect the migration itself to be the test. Nettking's node currently caches
   `recorder-local`; on the candidate it should drop that entry on
   `capability-identity-conflict` and re-announce as
   `recorder-{node_id}`. Capture the before/after `advertised_capabilities` rows.
7. Preserve all existing rig data. No `docker volume rm`, no `data/` deletion.

## 16. Exact next recommended action for Astra

1. **Resolve GitHub Actions billing.** It blocks every hosted check on both
   PRs and is the one blocker no engineering work can clear. See the section
   above.
2. Read the verdict of run 33960401244. If green, verify that the head it
   validated is still `f0434e4a4fd4cf86b3574563151d1e6241b4e06c` and that PR
   #435's head has not moved, then take the merge decision -- checking first
   whether branch protection requires any hosted check that cannot currently
   run.
3. Before any physical step, run the rig baseline workflow against Nitro and
   obtain an MSH Recorder inventory from a session that holds SSH credentials.
4. Treat Nettking's stuck `connecting` node with its cached `recorder-local`
   as the first physical acceptance case, not as an incident to clear by hand.

## Boundaries observed

Not merged, not deployed, no rig service stopped or reconfigured, no
force-push, no history rewrite, no branch-protection change, no test criteria
lowered, and no product code modified.
