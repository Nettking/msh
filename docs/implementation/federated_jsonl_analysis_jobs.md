# Federated JSONL analysis jobs

| Metadata | Value |
| --- | --- |
| Status | Active reference |
| Audience | Maintainers, reviewers, operators |
| Scope | How discovered or uploaded JSONL becomes a durable, federation-dispatched analysis job |
| Authority | The code in `catalog/capabilities/analysis/`, `catalog/orchestrator/analysis_runtime.py`, and the existing F7 capability contracts |
| Parent | [Implementation documentation](index.md) |

## Why this exists

Automatic JSONL discovery used to call `_run_for_date_slice()` on the machine that
found the data, and the upload workflow created a durable `JobContract` and then
immediately claimed it for a synthetic worker derived from the coordinator. Both
paths were local execution wearing different clothes, even though the repository
already contained durable job ownership (F7.3), dispatch (F7.4), lifecycle (F7.5),
and artifact authority (F7.6).

Discovering data no longer implies executing it.

## Roles

Three roles are kept strictly separate. They may be one machine or three.

| Role | Responsibility |
| --- | --- |
| **Data owner** | Holds the JSONL. Packs the slice, registers the input artifacts, and serves them only to an authorized worker. |
| **Coordinator** | Submits and owns the durable job, selects a provider, grants the ownership lease and artifact grant, and dispatches. |
| **Worker / provider** | Retrieves the authorized input, runs the existing analysis, and returns a bounded result. |

In this release the data owner and coordinator are the same node (the device that
discovered the data). The worker is whichever provider selection picks — possibly
that same node, possibly another.

## Flow

```
new JSONL on disk
      │
      ▼
discovery finds the date slice            catalog/orchestrator/pipeline.py
      │
      ▼
AnalysisWorkSlice  ──► deterministic job identity
      │
      ▼
pack slice + plan into artifacts          analysis/packaging.py, content_store.py
register them with the artifact authority analysis/gateway.py
      │
      ▼
JobContract (references only) ──► SQLiteJobLifecycleStore.submit + queue
      │
      ▼
select_provider(job, provider reports)    capabilities/provider_selection.py
      │
      ▼
store.claim(...) ──► ownership lease
      │
      ▼
authority.issue_grant(READ_INPUT, [plan, slice], expires ≤ lease)
      │
      ▼
ResilientDispatchCoordinator.dispatch ──► DispatchRequest ──► CapabilityWorker
      │
      ▼
worker fetches the two artifacts through the gateway (authorized, verified)
worker extracts them into an isolated workspace
worker runs `_run_for_date_slice` — the existing analysis, unchanged
      │
      ▼
ExecutionResult(details.result_reference) ──► store.commit_result
      │
      ▼
job succeeded; runtime state reconciled from the durable job
```

## Who owns the data, who schedules, who executes

* The **data owner** never hands out a filesystem path. It publishes a
  content-addressed artifact and answers `fetch(...)` only after the artifact
  authority approves the exact grant, worker node, provider, session, and
  artifact.
* The **coordinator** is the only node that may grant ownership or issue an
  artifact grant for a job, and the only node that may dispatch it
  (`dispatch-coordinator-mismatch`, `grant-coordinator-mismatch`).
* The **worker** executes only what the grant and the plan allow, in a workspace
  created for the attempt and deleted afterwards.

## Job identity and deduplication

Identity is a SHA-256 digest over everything that changes the meaning of the
result:

* federation session ID,
* analysis contract ID and contract version,
* slice kind and slice key,
* the exact target dates,
* the ordered script set,
* the runtime namespace,
* the source data signature for that slice.

From that digest come `job_id`, `request_id`, and `idempotency_key`. The job
store enforces uniqueness of `(session_id, idempotency_key)`, so:

```
poll 1 discovers slice X → job created
poll 2 discovers slice X → existing job recognized
poll 3 discovers slice X → existing job recognized
```

The trigger (`automatic-discovery` or `manual-upload`) is deliberately **not**
part of identity and is not in the plan artifact, so an uploaded batch and an
automatic discovery of the same slice resolve to one durable job. The trigger is
still recorded in the local job index for product views.

When the source data for a date changes materially, its signature changes, the
identity changes, and a new logical job is created. Discovery skips a date only
while a live job exists **for its current signature**.

## How a remote worker obtains the input

Large JSONL never travels in a control message. Two artifacts are registered per
job:

| Artifact | Schema | Contents |
| --- | --- | --- |
| Plan | `fcp.analysis-plan.v1` | ≤ 8 KiB JSON: dates, script keys, namespace, signature |
| Data slice | `fcp.analysis-data-slice.v1` | deterministic `tar.gz` of the source JSONL for those dates |

`JobContract.inputs` carries only `ArtifactReference` values with
`content_hash` and `size_bytes`. The worker derives the grant ID
deterministically from `(job_id, attempt_id)` and calls the data owner's gateway.
The grant ID is **not a bearer token**: every fetch is re-authorized against the
live ownership lease, the authenticated worker node, the provider identity, the
session, and an explicit artifact allowlist. Content is streamed in chunks and
its SHA-256 is verified end to end; a mismatch fails closed.

## Provider selection

Selection is the existing deterministic policy in
`catalog/capabilities/provider_selection.py`. A provider is eligible only if its
report matches the job's federation session, capability type, protocol and
protocol major/minor, all declared capability requirements, freshness, status,
and available capacity. Ranking prefers free capacity, then low queue depth and
utilization.

Report sources are composed per node:

* `LocalAnalysisProviderSource` publishes this node's own real analysis capacity.
* `FederatedProviderReportSource` adapts the existing
  `FederatedProviderHealthService.fresh_reports(...)` when provider health is
  wired in.

Local execution therefore happens because *the local provider advertised the
capability and selection chose it* — never because the data happened to be here.
Removing the local provider (`enable_local_provider=False`) removes local
execution and nothing else changes.

## What happens when no worker is available

The job stays `queued` with its `no-eligible-provider` decision visible. There is
no silent local fallback. If nothing becomes available inside the job's
`queue_timeout_seconds`, the existing lifecycle coordinator times it out.

Other failure paths reuse existing semantics:

| Situation | Behaviour |
| --- | --- |
| Worker unreachable | Attempt is lost, `schedule_retry` applies the retry policy, another provider can be selected |
| Lease expires mid-flight | Worker rejects with `lease-expired`; the response is treated as stale and the attempt is retried |
| Worker crashes | Durable job survives; heartbeat/lease timeouts reassign it |
| Coordinator restarts | Jobs, attempts, leases, and grants are durable SQLite state |
| Data owner unavailable | Worker fails with a bounded, retryable error code |
| Artifact authorization fails | Denied and audited; execution never starts |
| Duplicate dispatch | The dispatch inbox replays the recorded response; execution happens once |

## Single-node and standalone installations

A standalone install is modelled as a **federation of one**, not as an exemption.
`resolve_analysis_identity` binds the runtime to the real federation session when
the device is connected (the Flask application registers the supplier during
startup). Otherwise it creates and persists a single-node session and node
identity under `results/capabilities/analysis_identity.json`.

Either way the node runs one coordinator and one provider with distinct
identities — ownership can never be granted by the provider that receives it —
and every authority check is enforced unchanged. One compute-capable node means
one eligible provider, which selection then picks.

## Startup behaviour

Flask startup is unaffected. Discovery runs on the existing background poller,
submission is a local SQLite write, and scheduling plus execution run on a
separate thread (`AnalysisWorkService.request_scheduling_pass`). Startup never
waits for a federation job.

## What the Jobs view shows

The durable job remains the source of truth for the product surface:
`submitted`, `queued`, `active` (assigned → accepted → running), `retry-wait`,
`succeeded`, `failed`, `cancelled`, `timed-out`, and lost attempts.
`DiscoveryAnalysisGateway.job_view` additionally exposes the selected provider
and node, attempt count and status, lease expiry, input artifact references,
committed output artifact, error code, slice identity, and trigger origin.

## Bounds

| Bound | Default |
| --- | --- |
| Packed input slice | 512 MiB (`FCP_ANALYSIS_MAX_SLICE_BYTES`) |
| Files per slice | 4096 |
| Plan document | 8 KiB |
| Dispatch result details | 64 KiB |
| Ownership lease | 55 minutes (F7 maximum is 60) |
| Artifact grant | 50 minutes, never beyond the lease |
| Concurrent local analysis jobs | 1 (`FCP_ANALYSIS_MAX_CONCURRENT_JOBS`) |

## Known limitations

* Cross-node provider advertisement requires an operator to enrol a
  `background-analysis` provider through the existing provider
  enrolment/health path. Without that, the only report source is the local
  provider, so a stock two-device federation still runs analysis locally.
* Artifact bodies are fetched through the in-process gateway. A remote worker
  needs a transport adapter over the relay for the data plane; the authorization
  contract it must satisfy is already defined by `AnalysisInputTransport`.
* One attempt must complete inside one ownership lease (≤ 1 hour). Longer work
  fails with `lease-expired` and is retried.
* Analysis outputs are written under the executing node's own
  `results/workflows`, so playback on the data owner still requires that the
  data owner was the selected provider. Only the bounded result artifact travels
  back.
* Packing a slice copies and compresses its source JSONL once per new signature.
