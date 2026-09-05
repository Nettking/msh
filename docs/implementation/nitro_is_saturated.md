# The release gate's Linux host is saturated, and now we can prove it

Every explanation of the self-hosted Linux failures so far has rested on
inference: pytest's progress lines were far apart, so the host must be stalling.
Run 33975032244's Linux job carried a read-only `/proc` sampler for the length
of the suite, and inference can now be replaced with measurement.

**Nitro's disk was busy a mean of 83.4% of every 60-second interval for 69
consecutive minutes** — median 85.5%, minimum 57.2%, maximum 91.1%.
`MemAvailable` never rose above 2.13 GiB and averaged 1.88. Load average
averaged 2.31, so this is not a CPU shortage. The host is disk-saturated and
memory-starved for as long as a suite runs, continuously, with no idle
stretches at all.

On a disk that busy, an `fsync` can block for seconds. The suite is
fsync-heavy SQLite.

## What failed

Five distinct tests across the run's two Linux jobs, on candidate `ba8a3b0`:

| job | test | failure |
|---|---|---|
| Linux release, default order | `test_source_availability_retry.py::test_first_real_data_is_durably_written_with_its_raw_manifest` | 2-second helper deadline |
| order independence, shuffled | `test_source_availability_retry.py::test_recorder_starts_recording_when_the_source_appears` | 2-second helper deadline |
| order independence, shuffled | `test_phase2_integration.py::test_f2_006_revocation_rejects_traffic_and_reconnect_but_peer_survives` | `CancelledError` / `TimeoutError` |
| order independence, shuffled | `test_federation_pairing_relay.py::test_signed_pairing_code_projects_same_members_from_both_viewpoints` | `pairing-relay-timeout` |
| order independence, shuffled | `test_live_storage_catchup.py::test_live_catchup_repairs_only_missing_batches_and_keeps_node_unassigned` | `TimeoutError` |

**Five wall-clock timeouts. Zero assertion failures.** Nothing computed a wrong
answer; things ran out of time.

The clearest single case is the first. Its helper is:

```python
def _complete_scheduled_cycle(service, *, timeout: float = 2.0) -> None:
    service.run_fetch_cycle()
    deadline = perf_counter() + timeout
    while True:
        service._harvest_capture_results()
        with service.lock:
            if not service._capture_futures:
                return
        if perf_counter() >= deadline:
            pytest.fail("recorder capture did not finish within the test deadline")
```

and the same test's captured teardown reads:

```
[INFO] [mazak-cell] committed sequences 1-3 (3 observations)
```

The capture completed. It completed after the two seconds were up. That failure
landed at 18:54:31, inside the busiest disk minute of the entire run — the
18:54:02 sample recorded 90.5% busy, with `MemAvailable` at its second-lowest
reading of the run.

## It is not the candidate

All four failing test files and all four product modules they exercise are
byte-identical between unmodified `main` (`6101c86`) and the candidate
(`ba8a3b0`):

```
catalog/mtconnect_recorder/tests/test_source_availability_retry.py
catalog/relay/tests/test_phase2_integration.py
catalog/flask_app/tests/test_federation_pairing_relay.py
catalog/node/tests/test_live_storage_catchup.py
catalog/mtconnect_recorder/runtime.py
catalog/relay/service.py
catalog/node/storage_agent.py
catalog/flask_app/services/resilient_pairing_runtime.py
```

PR 435 changes none of them. Independently, all four named tests were run
together on a healthy Windows host on both trees and passed — 4 in 5.97 s on
the candidate, 4 in 6.40 s on `main`.

This also closes the mechanism question left open by
[pr435_linux_gate_failing_test.md](pr435_linux_gate_failing_test.md). That note
named the pairing-relay test by arithmetic and predicted, from a timeout-margin
sweep, that it fails on a timeout rather than on anything the candidate
changed. Its traceback from Nitro finally exists:
`FederationOperationError: the remote relay did not respond in time
(pairing-relay-timeout)`.

## It is not order-dependence either

The two jobs failed different, overlapping-but-unequal sets, and every failure
is a timeout. A shuffled suite going red is not, on this evidence, a sign that
collection order breaks the product.

## What was deliberately not done

No timeout was raised. No test was skipped, quarantined or retimed. No re-run
was spent.

Raising the 2-second and 5-second deadlines is the obvious way to make this
gate green, and it is the wrong one: it converts a measured host problem into a
hidden one, and moves the stall size that breaks the suite rather than removing
it. The deadlines are not generous — they are met with room to spare on every
healthy host tried, including a Windows runner and a container where the same
suite finishes in under four minutes.

## The consequence

While Nitro sustains ~85% disk busy with under 2 GiB available memory, this
gate cannot be relied on to go green for *any* candidate, and a re-run is a
coin toss rather than evidence. The standing suspect is recorded as ENV002(a)
in the coordination handoff: a container that has been running a `cp -a` for 42
hours on that same disk, with its host processes alive for over a day and a
half. It has not been touched — live-host changes are an operator decision.

That decision is worth making before another multi-hour run, not after.
