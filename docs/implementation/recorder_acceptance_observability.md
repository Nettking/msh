# Recorder acceptance observations

The Recorder exposes product-owned duration and managed-worker observations in
its existing `fcp.mtconnect_recorder.status.v2` heartbeat. This is additive
evidence for the existing [P07/P12 contract](v1_robustness_reconciliation.md#5-corrected-physical-robustness-campaign),
not a new acceptance scenario, latency threshold, or physical PASS.

## Existing requirements and exact boundaries

| Requirement | Product observation | Boundary and limitation |
| --- | --- | --- |
| P12 recorder corpus and per-poll recovery time | `recorder-recovery` | Entry through return/raise of `RecorderRuntime._recover_archived_batches`. Includes the actual frontier/legacy recovery work and checkpoint progression. Excludes HTTP fetch, ordinary capture, and inter-poll sleep. |
| P12 publication reconciliation duration/progress | `publication-reconcile` | Entry through return/raise of `IncrementalRecorderArchiveReconciler.reconcile`, including quarantine accounting. Both the connected and disconnected callers use this method. Excludes delivery/network work in the larger publication cycle. |
| P07 no silent required-worker death; P12 process/restart/required-thread health | Existing `workers` map, populated for the managed entrypoint | Actual companion/control threads and publication future plus event-loop thread state. A single snapshot establishes sampled state, not continuous liveness between samples. |
| P07 bounded polling/reconciliation latency | Exact recovery/reconciliation spans plus the rest of the campaign's source/progress evidence | A recovery span is not total source polling time. The observations do not by themselves establish the complete P07 latency assertion. |

Neither durable checkpoint timestamps, enqueue timestamps, container uptime nor
heartbeat age can reconstruct these operation durations. An actual invocation
that returns without work has valid zero progress; missing work or missing
telemetry is not a fabricated zero-duration invocation.

## Duration schema and provenance

`acceptance_observability` uses schema
`fcp.recorder.acceptance-observability.v1`. A successful snapshot includes:

- `available`, snapshot UTC and monotonic nanoseconds;
- `provenance`: exact `candidate_sha`, per-process `runtime_generation` UUID,
  `supervisor_generation` when a native supervisor actually supplies one, and
  the process PID;
- `operations`: the latest bounded operation records;
- `dropped_updates`, `max_operations`, `evicted_operations`, and
  `retention_truncated`.

Candidate identity comes from existing `FCP_BUILD_COMMIT` /
`FCP_RECORDER_BUILD_COMMIT` values; absent or conflicting valid values yield
`null`, not an inferred checkout SHA. Compose has no invented native supervisor
generation. A PID change establishes a new process generation and discards
inherited operation records. Managed worker snapshots use this same process
identity and separately identify each actual worker generation.

Each operation record includes `operation`, unique `operation_id`, process-local
`operation_sequence`, provenance, bounded `context`, start UTC and monotonic
nanoseconds, and `outcome`. Terminal records add end timestamps, exact
`duration_ns = ended_at_monotonic_ns - started_at_monotonic_ns`, progress, and an
exception class for failures. Exception text, endpoint URLs, credentials, and
source names are not added to this surface.

Recovery context carries a SHA-256 `source_alias`, Agent instance,
`next_sequence_before`, and the current in-memory Federation node/session when
available. Its result carries `next_sequence_after` and `advanced_sequences`.
Publication context comes directly from the active reconciler target's session,
Recorder node, and storage group. Its result carries scanned/eligible batch,
publication chunk, enqueued/already-enqueued, and quarantined counts.

An invocation begins as `incomplete`. Only a normal return with valid clocks
and real typed result counters becomes `completed`. A business exception is
`failed`; a business `BaseException` interruption is `interrupted`. The original
return value or exception is preserved. A new attempt replaces the previous
record for that scope before completion, and a late old attempt cannot replace
a newer attempt. Missing or malformed observer data never marks a completion.

## Bounded retention and failure behavior

At most 16 latest operation scopes are retained, keyed by kind, source alias,
session, node, and storage group. This is a constant-space sampled surface, not
an operation journal or a measurement of every historical poll. Evictions are
counted explicitly. Consumers must not treat retained scopes as complete source
coverage after truncation.

The registry uses nonblocking synchronization. Contention or an observer error
does not delay/retry the business operation; it leaves unavailable/incomplete
evidence and increments the lifetime `dropped_updates` counter where possible.
Every invocation captures `observation_loss_count` at its start and preserves it
through completion. A completed record is usable only when that count equals
the current snapshot's dropped count. Thus an old completion cannot hide a
later unobserved attempt, nor can an earlier in-flight operation's completion
hide a missed newer start. A fresh real invocation after the loss restores
usable evidence without clearing counters or restarting a workload. Refreshing
one scope does not refresh other scopes. This remains sampled evidence, not
proof that every poll was observed; the cumulative loss stays explicit.

The evidence reader rejects truncated retention. Registry snapshots are
detached copies. No new log, status file, background task, retry, scheduling
policy, or heartbeat write cadence is introduced.

## Managed worker semantics

The managed entrypoint registers read-only providers through the existing
Recorder heartbeat seams. `managed_companion` and `federation_control` observe
the real worker threads. `recorder_publication` requires an unfinished actual
future, a live event-loop thread, and a running event loop. Thread references
remain observable when an ordinary bounded stop did not join them; generation
overlap cannot be reported as current healthy execution.

`alive` describes actual execution ownership. `healthy` additionally requires a
current generation, no requested stop, no recorded failure, and a completed
successful cycle. Waiting for pairing, absent context, a retrying worker, or a
pending publication future cannot be upgraded into a successful operation.
An alive recovering worker is therefore distinct from a dead worker and from a
healthy completed operation. Current node/session correlation is read from the
already-connected node's in-memory snapshot, without another bootstrap or a
saved-state read.

## Evidence consumption

The read-only adapter is `scripts.acceptance.v1_recorder_observability`:

```text
python -m scripts.acceptance.v1_recorder_observability --status-file <heartbeat> --candidate <exact-sha> --session <current-session> --not-before-utc <campaign-start-utc>
```

The adapter validates candidate/process/worker identity, actual completed spans,
clock arithmetic, context and counters. Its output always has
`physical_assertion_pass: false`: valid observations are inputs to the physical
contract, not a substitute for it. The series consumer revalidates raw
observations, rejects reused completions, and requires fresh measured operations
after the preceding sample; baseline operations alone provide no timed-run
credit. Missing, failed, stale, partial, or contradictory observations remain
unavailable or failed rather than PASS.

The existing candidate/host/run binding, one-hour P07 outage, 24-hour P12 soak,
meaningful initial history, source approval, accelerated ceilings, growth review,
and evidence sealing requirements remain in force. These observations neither
manufacture source/history evidence nor authorize a restart, change, or campaign.
