# PR 435: naming the Linux gate's unnamed failure, and clearing PR 435 of it

The self-hosted Linux release job in run 33960401244 (job 101291502702) was
cancelled by its own 120-minute budget after 64% of the suite. One test had
already failed: an `F` sits in the progress stream. Because the job was killed
before pytest could print its summary, the log contains no node ID and no
traceback, and the coordination record carried the failure as
`exact test/root cause UNKNOWN`, with the explicit instruction not to write it
off as an infrastructure timeout and not to claim nothing had failed.

This note names that test from the cancelled log alone, and then answers the
only question that matters for the release: whether PR 435 caused it.

Result: the failing test is

```
catalog/flask_app/tests/test_federation_pairing_relay.py::test_signed_pairing_code_projects_same_members_from_both_viewpoints
```

and **PR 435 did not cause it**. The same failure mode is present, at
statistically indistinguishable rates, on unmodified `main`.

## Naming the test from a log that never named it

Under `-q` pytest prints one character per completed test and closes each line
with a percentage. The cancelled log has 33 such lines, all 72 characters wide,
and exactly one of them carries an `F`:

```
2026-09-05T12:21:55.6839934Z ........................................................................ [ 58%]
2026-09-05T12:22:44.4731454Z .F...................................................................... [ 60%]
2026-09-05T12:23:13.1017998Z ........................................................................ [ 62%]
```

The `F` is the second character of line 31, so the failing test is at 0-based
index `30 * 72 + 1` = **2161** in that run's collection order.

An index is only useful if it maps to the same order the job used, so three
things have to hold, and all three do.

**The collection total is exactly 3662.** Nothing in the log states it, but
every percentage annotation constrains it, and 11 of them are visible. With
3662 tests, line 23 gives `1656/3662 = 45.2%` -> `45`, line 30 gives
`2160/3662 = 59.0%` -> `58` (pytest floors), line 31 gives `2232/3662 = 60.9%`
-> `60`, and line 33 gives `2376/3662 = 64.9%` -> `64`. All eleven observed
annotations agree with 3662 and with no other plausible total.

**The order is the default one.** The job installs `pytest==9.1.1` and
`ruff==0.16.3` only. `pytest-randomly` is not present, so nothing shuffles;
collection is the ordinary deterministic directory walk.

**The same commit collects the same 3662 tests here.** Collecting f0434e4a
locally with `-p no:randomly` yields 3662 items, matching the total the log
implies.

Index 2161 in that collection is:

```
2160 catalog/flask_app/tests/test_federation_overview_route.py::test_update_panel_describes_verified_running_installation_update
2161 catalog/flask_app/tests/test_federation_pairing_relay.py::test_signed_pairing_code_projects_same_members_from_both_viewpoints
2162 catalog/flask_app/tests/test_federation_pairing_service.py::test_pairing_code_round_trip_is_signed_and_secret_safe_in_repr
```

It is the only test in its file, which makes the identification robust: an
off-by-one in either direction lands in a different file with a different name,
and the arithmetic above has no free parameters left to absorb one.

## PR 435 does not touch it

PR 435 changes six files:

```
.github/workflows/federation-v1-release.yml                       |    1 +
.github/workflows/recorder-federation-publication.yml             |    2 +
catalog/mtconnect_recorder/federation_node.py                     |  211 +++-
catalog/mtconnect_recorder/tests/test_multi_recorder_capability_identity.py | 1037 +++++++++
catalog/node/client.py                                            |   17 +-
docs/implementation/federated_session_contracts.md                |    9 +
```

None is under `catalog/flask_app/` or `catalog/relay/`, and

```
git diff --quiet 6101c86 f0434e4 -- catalog/flask_app/tests/test_federation_pairing_relay.py
```

exits 0: the failing test file is byte-identical on `main` and on the
candidate.

The one production file the test could reach transitively is
`catalog/node/client.py`, whose entire change is a `try`/`except
RelayRemoteError` around `announce_capability` in the initial-replay loop that
acts only on `capability-identity-conflict`. On the success path it is a no-op.
That is an argument, though, not evidence, so the rest of this note is the
experiment.

## The margin experiment

The test builds a relay and redeems a pairing code with three hard wall-clock
timeouts -- `auth_timeout_seconds=5`, `send_timeout_seconds=5` and the
runtime's own `timeout_seconds=5` -- and finishes in about 2.5 seconds on an
idle container. Reproducing a host slow enough to break it by slowing a
container down is impractical. Squeezing the timeouts is equivalent and exact:
a test that fails at `5/N` seconds is a test that fails on a host `N` times
slower, and running the identical squeeze against the candidate and against
unmodified `main` is what separates a product regression from a pre-existing
one.

An untracked copy of the test with the three timeouts read from an environment
variable was placed in two scratch worktrees, one at f0434e4a and one at
6101c86, and verified byte-identical between them. Neither worktree is a
release checkout and nothing here was committed; the real test's timeouts are
untouched.

The harness has a negative control. At `CI002_TIMEOUT=0.01` the copy fails, at
`0.1` it passes, and with the variable unset it raises `KeyError` -- so the
substituted value really is what the relay and the runtime use.

Runs at each value, per tree (10 at the two coarse values, 15 at the rest):

| timeout | candidate f0434e4a | unmodified main 6101c86 |
|--------:|-------------------:|------------------------:|
| 5.0 s   | 10/10 pass         | 10/10 pass              |
| 0.10 s  | 10/10 pass         | 10/10 pass              |
| 0.08 s  | 15/15 pass         | 15/15 pass              |
| 0.06 s  | 14/15 pass         | 15/15 pass              |
| 0.05 s  | 13/15 pass         | 14/15 pass              |
| 0.04 s  | 8/15 pass          | 4/15 pass               |
| 0.03 s  | 1/15 pass          | 1/15 pass               |
| 0.02 s  | 0/15 pass          | 0/15 pass               |

Both trees are clean down to 0.08 s, degrade across the same narrow band, and
are dead by 0.02 s. Every failure on both sides is the same `TimeoutError` /
`CancelledError` out of the timed relay operations. The two cells that differ
are within binomial noise at n=15 and point in opposite directions -- at 0.04 s
the *candidate* passed nearly twice as often as `main`.

There is no evidence that PR 435 makes this test more fragile, and a real
regression of this kind could not hide inside a margin this wide: the
candidate would have to shift the whole curve, not jitter one cell.

The experiment also corrects an assumption worth writing down. The test's 2.5
seconds are almost entirely import and fixture setup; the operations the
5-second timeouts actually guard complete in well under 100 ms. The headroom is
not the 2x that the wall-clock time suggests, it is roughly 50-500x. A host
that is merely *slower on average* does not break this test.

## What does break it on Nitro

Nitro is not uniformly slow, it stalls. The per-line deltas from the same
cancelled log, each covering 72 tests:

```
line  2  842.5s      line 10    3.8s      line 19  610.5s
line  3  838.5s      line 13    0.5s      line 30  101.5s
line  5 1156.5s      line 14   34.4s      line 31   48.8s  <- the F
```

72 tests in 0.5 seconds and 72 tests in 1156 seconds in the same run is not a
slow machine, it is a machine losing hundreds of seconds at a time to something
else. A single stall of that size landing inside any one of the three 5-second
windows fails this test, and the 50-500x margin is irrelevant against a stall
measured in minutes.

The read-only inventory of Nitro (job 101295293488) found a candidate for the
contention and did not touch it: container `kind_poitras` has been up 42 hours
running `cp -a /src/. /dst/`, with host PIDs still alive after 1d17h. A `cp`
that has not finished in 42 hours is hung, and it shares the disk with the
runner.

## Classification

- **Not a PR 435 product regression.** The file is byte-identical on `main` and
  the failure-rate curves are indistinguishable. This is the finding the merge
  decision needs.
- **Self-hosted infrastructure**, primarily. The trigger is Nitro losing
  hundreds of seconds at a time, on a host with a hung 42-hour `cp` sharing its
  disk.
- **Latent test defect**, secondarily and pre-existing on `main`: three hard
  wall-clock timeouts with no tolerance for host stalls. Worth fixing on its
  own schedule. It is not worth fixing by raising 5 to a bigger number, which
  only moves the stall size that breaks it.

## What this note does not establish

The identification of *which* test failed is arithmetic on the log and is as
firm as the log. The *cause* on Nitro is inference: no traceback from that host
has been read, because the job was cancelled before it could print one. Job
101321613451 in run 33971811540 is the same suite at the same product SHA with
a 300-minute budget and `-v --durations=25`, so it will either name the test
again with its traceback or pass. Confirm against that job before treating the
cause as settled; the classification above should not be recorded as final
until then.

Related: [pr435_self_hosted_validation_diagnosis.md](pr435_self_hosted_validation_diagnosis.md),
[pr435_stale_backup_restore_flake.md](pr435_stale_backup_restore_flake.md).
