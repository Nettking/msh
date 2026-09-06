# Merged-main qualification run 34019865738 — results and classification

Exact identity under test:

```
MERGED_MAIN_SHA=bcf5c9ab2fb453cb26129b70d41fb64fc4863dd4
```

Harness `.github/workflows/merged-main-qualification.yml` at
`074ceef9b32ace0fced1f18ff8d9c0e50d141aae`. Started 2026-09-06T07:42:24Z.

## Result summary

| Leg | Job | Runner | Result | Duration |
| --- | --- | --- | --- | --- |
| identity smoke | 101450326992 | Nettking-Linux (27) | SUCCESS | 6s |
| Windows release matrix | 101450327063 | Nettking (22, `fcp-windows`) | **SUCCESS** | 482s |
| PostgreSQL storage release check | 101450327045 | Nitro (21, `fcp-linux`) | **SUCCESS** | 204s |
| A full suite, default order | 101450344435 | Nettking-Linux (27) | **FAILURE — infrastructure** | 408s |
| B shuffled seed 20260813 | 101451171364 | — | skipped (gated on A) | — |
| C shuffled seed 15 | 101451171445 | — | skipped (gated on A) | — |
| D release/static gates | 101451171728 | — | skipped (gated on A) | — |
| E real Compose validation | 101451171874 | — | skipped (gated on A) | — |
| verdict | 101451300256 | — | stranded QUEUED | — |

## Windows — PASS on the exact merged SHA

Job 101450327063 on the Nettking Windows runner, all 19 steps SUCCESS,
Python 3.12.10 from the runner toolchain, exact-SHA and Python asserted in
step 4 with `VALIDATED_SHA: bcf5c9ab2fb453cb26129b70d41fb64fc4863dd4`.

Steps passing include both regression subsets, the second reporting
**367 passed, 1 skipped in 175.18s**, matching the candidate's Windows result
exactly. Also passing: compile, acceptance manifest, Go direct peer sidecar
tests, Ruff on the release scope, **real `docker compose config` (the Windows
host has the plugin)**, diff hygiene, and the Windows storage allocation
precondition (143.49 GiB free, admission pressure NORMAL on every measured
volume).

This is the same gate that qualified `ba8a3b0b`, re-run unchanged against the
merged identity.

## PostgreSQL — PASS on the exact merged SHA

Job 101450327045 on Nitro, all 11 steps SUCCESS, including the exact-SHA
assertion, the self-hosted Linux storage precondition, and the PostgreSQL
storage authority regression under Python 3.12.13 against the
`postgres:16-alpine` service in its proven configuration.

## Leg A — infrastructure kill, not a product failure

**No test failed.** The log shows continuous `PASSED` to 93% of the suite. The
process was then killed outright:

```
1588677 Killed    docker run --rm --network host ... python -m pytest ...
##[error]Process completed with exit code 137.
```

Exit **137 = 128 + 9 = SIGKILL**. The last test entered was
`catalog/node/tests/test_live_storage_replication.py::test_two_live_storage_nodes_replicate_and_replica_restarts`,
which never reported a result because the container died under it.

Host state captured by the very next step:

```
cpus 20
Mem:  15Gi total   14Gi used   91Mi free   1.0Gi buff/cache   837Mi available
/dev/sdd  1007G  72G  885G  8% /
```

837 MiB available on a 15 GiB host. Disk was not the constraint (885 GiB free,
well above the product's 12 GiB admission floor, and the storage precondition
passed at job start). This is memory exhaustion, and the kernel killed the
container.

Immediately afterwards:

```
##[error]The runner has received a shutdown signal. This can happen when the
runner service is stopped, or a manually started runner is canceled.
##[error]The operation was canceled.
```

**Classification: ENV — host memory exhaustion on Nettking-Linux followed by
loss of the runner service.** Not a merged-main regression, not a candidate
regression, and not a test defect. Supporting points:

- No assertion failed anywhere in the run; the failure is a signal, not a result.
- The merged tree is byte-identical to `ba8a3b0b` (tree `bcbd778b`,
  `git diff ba8a3b0b bcf5c9ab` empty), and that identical tree completed this
  same suite on this same host in 345s on 2026-09-05.
- The Windows and PostgreSQL legs of this very run passed on the merged SHA.
- This is consistent with, and sharpens, the existing ENV002 note: Astra
  recorded this box at 13 of 15 GiB already in use at smoke time, and the
  earlier Linux sampler showed sustained pressure. It reached 837 MiB here.

This is **not** the class of the earlier known slow-host timing failures, which
are 2s/3s/5s deadline expiries inside a live test. It is also unrelated to the
cloud-container `os.pidfd_open` artifact recorded previously.

No timeout was raised, no test skipped, no retry added, no flaky mark applied
and no coverage reduced in response. The harness change made afterwards adds a
read-only `free(1)` sampler to the three Linux suite legs so a repeat is
diagnosable; the pytest invocations are byte-identical and the PostgreSQL job
is untouched.

## Runner availability

| Runner | id | Label | State |
| --- | --- | --- | --- |
| Nettking-Linux | 27 | `nettking-linux` | **OFFLINE since 2026-09-06T07:49Z** |
| Nettking (Windows) | 22 | `fcp-windows` | ONLINE, leg passed |
| Nitro | 21 | `fcp-linux` | ONLINE, leg passed |
| Beast-Linux | — | `[self-hosted, beast-linux]` | OFFLINE; smoke job 101383784569 queued since 2026-09-05T22:06:47Z |

Nettking-Linux accepted the smoke job in 2 seconds while healthy. Its verdict
job 101451300256 has since sat QUEUED with no assigned runner for hours, which
is the evidence that the service did not come back after the shutdown signal.

Run 34019865738 could not be cancelled from this session: the GitHub
integration returns `403 Resource not accessible by integration` for
workflow-run cancellation. It therefore remains queued on its verdict job.

## Blocking issue and next action

The blocker is host-side and cannot be remediated from this session, which has
no shell on Nettking-Linux:

1. Bring the Nettking-Linux self-hosted runner service back up.
2. Free memory on that host before the retry. It was at 14 of 15 GiB used with
   the Federation services deployed at `6101c86d` also resident. A full suite
   plus the release container needs headroom that was not there.

Retry run **34025305598** is already queued at harness commit
`028c18174ebf6ebdbe121aedf9635b5fd24e8f01` and is held behind run 34019865738
by the `merged-main-qualification` concurrency group. When the runner returns,
run 34019865738 finishes its verdict job and run 34025305598 starts legs A-E
automatically, now with memory sampling.

If leg A is killed again with adequate free memory, that is new evidence and
must be investigated as such rather than retried further.
