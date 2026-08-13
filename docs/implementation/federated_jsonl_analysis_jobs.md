# Federated JSONL analysis jobs

| Metadata | Value |
| --- | --- |
| Status | Active reference |
| Audience | Maintainers, reviewers, operators |
| Scope | How discovered or uploaded JSONL becomes a durable, federation-dispatched analysis job |
| Authority | `catalog/capabilities/analysis/`, `catalog/orchestrator/analysis_federation.py`, `catalog/orchestrator/analysis_runtime.py`, and the existing F6/F7/F8 contracts |
| Parent | [Implementation documentation](index.md) |

## Why this exists

Automatic JSONL discovery used to call `_run_for_date_slice()` on the machine that
found the data, and the upload workflow created a durable `JobContract` and then
immediately claimed it for a synthetic worker derived from the coordinator. Both
paths were local execution wearing different clothes, even though the repository
already contained provider selection (F7.2), durable job ownership (F7.3),
dispatch (F7.4), lifecycle (F7.5), artifact authority (F7.6), verified object
transfer (F6), provider enrollment and health (F8.1/F8.2), and trusted compute
worker activation (F8.4).

Discovering data no longer implies executing it.

Analysis is **not** a new subsystem. It is a capability handler plus a thin
orchestration layer on top of the components above. There is no second scheduler,
no second lifecycle, no second worker, no second transfer protocol, and no new
network protocol.

## Roles

Three roles are kept strictly separate. They may be one machine or three.

| Role | Responsibility |
| --- | --- |
| **Data owner** | Holds the JSONL. Packs the slice, registers the input artifacts, and serves their bytes only to an authorized worker. |
| **Coordinator** | Submits and owns the durable job, selects a provider, grants the ownership lease and the artifact grant, and dispatches. |
| **Worker / provider** | Retrieves the authorized input, runs the existing analysis, and returns a bounded result reference. |

In this release the data owner and coordinator are the same node (the device that
discovered the data). The worker is whichever provider F7.2 selection picks —
possibly that same node, possibly another.

## Which existing components are reused

| Concern | Component | Where |
| --- | --- | --- |
| Provider selection | `select_provider`, `ProviderSelectionPolicy`, `ProviderResourceReport` | `capabilities/provider_selection.py`, `provider_reports.py` |
| Durable jobs, ownership, retry, result commit | `SQLiteJobLifecycleStore`, `JobLifecycleCoordinator`, `ResilientDispatchCoordinator` | `capabilities/lifecycle_store.py`, `lifecycle_coordinator.py` |
| Dispatch transport | `RelayLifecycleEndpoint` (F7.5 relay carrier) | `capabilities/relay_lifecycle.py` |
| Duplicate suppression | `SQLiteLifecycleDispatchInbox` | `capabilities/lifecycle_worker.py` |
| Worker | `CancellableCapabilityWorker` | `capabilities/lifecycle_worker.py` |
| Trusted activation | `LocalComputeHandlerDescriptor/Inventory`, `ComputeWorkerActivationAuthority`, `TrustedComputeWorkerBinder` | `capabilities/worker_activation.py` |
| Enrollment and health | `FederatedProviderEnrollmentService`, `FederatedProviderHealthService` | `capabilities/provider_enrollment.py`, `provider_health.py` |
| Artifact authorization | `SQLiteCapabilityArtifactAuthority`, `ArtifactGrant`, `ArtifactInputReference` | `capabilities/artifact_secure_runtime.py`, `artifact_contracts.py` |
| Byte transfer | `ObjectTransferManifest`, `ObjectTransferChunk`, `ObjectTransferReceiver`, `FilesystemObjectTransferChunkStore` | `federation/object_transfer.py` |
| Restart reconciliation | `TrustedProviderRuntimeReconciler` (F8.6) | `capabilities/reconciliation.py` |

## Flow

```
new JSONL on disk (or a completed upload batch)
      │
      ▼
discovery / upload finds the slice          orchestrator/pipeline.py, flask_app/services
      │
      ▼
AnalysisWorkSlice ──► deterministic job identity     analysis/contracts.py
      │
      ▼
pack slice + plan into artifacts             analysis/packaging.py, content_store.py
register them with the F7.6 authority        analysis/gateway.py
      │
      ▼
JobContract (references only) ──► SQLiteJobLifecycleStore.submit + queue
      │
      ▼
select_provider(job, F8.2 reports, now)      capabilities/provider_selection.py
      │
      ▼
store.claim(...) ──► ownership lease         (F7.3)
      │
      ▼
authority.authorize_input(READ_INPUT, [plan, slice], expires ≤ lease)   (F7.6)
      │
      ▼
ResilientDispatchCoordinator.dispatch        (F7.4/F7.5)
      └─► LifecycleTransport.request ─► RelayLifecycleEndpoint ─► relay
                                       └─► LocalLifecycleTransport (standalone)
      │
      ▼
activated CancellableCapabilityWorker        (F8.4 fenced, F7.5 contract)
      │
      ▼
FederatedAnalysisHandler
   ├─ pulls plan + slice as F6 manifest/chunks through the artifact carrier
   ├─ verifies and publishes them with FilesystemObjectTransferChunkStore
   ├─ extracts the slice into an attempt workspace
   └─ runs the existing analysis executor
      │
      ▼
ExecutionResult(details.result_reference)
      │
      ▼
JobLifecycleCoordinator.apply_response ──► the single result commit   (F7.5)
      │
      ▼
job succeeded; runtime state reconciled from the durable job
```

## Making analysis a trusted F8 capability

`AnalysisProviderProvisioner` (`analysis/provisioning.py`) drives the *existing*
F8 chain and adds no trust of its own:

```
inventory.register(descriptor, handler)      the handler is preinstalled, locally
      → coordinator.announce_capability(...)         F8 capability registry
      → enrollments.request(...)                     F8.1
      → approval by the session's authority          F8.1
      → health.publish(report)                       F8.2
      → binder.activate_on(endpoint, capability_id)  F8.4
```

Two properties matter and are tested:

* **Installing a handler is not approval.** Startup requests enrollment and then
  approves *only* when `coordinator.require_session_leader(...)` succeeds for this
  node. A device operating its own single-node session is that authority, so it
  self-approves legitimately. A device that joined a federation somebody else
  leads gets `provider-awaiting-approval`, publishes no health, activates nothing,
  and simply has no eligible provider report until an operator approves it.
* **Nothing executable ever crosses the wire.** The handler is identified by a
  logical `handler_id` naming a preinstalled local implementation, plus a
  descriptor fingerprint. No module path, filename, script text, shell command,
  import target, package name, or any other executable material is accepted from
  a peer, a plan, a dispatch request, or an artifact. `ComputeHandlerActivationReference`
  in the health report only *names* what the node already has.

`TrustedComputeWorkerBinder` gained one optional `worker_factory` argument so
activation can produce the F7.5 `CancellableCapabilityWorker` that lifecycle
endpoints require. The default is unchanged (`CapabilityWorker`), so every
existing F8.4 caller behaves exactly as before, and activation fencing is the same
`_ActivationFencedHandler` in both cases — it is not duplicated or weakened.
`ActivatedComputeWorker.cancel` refuses with `compute-worker-not-cancellable`
when the worker was not bound with a lifecycle-capable factory.

Restart rebinding is the existing F8.6 path, not a second one:
`RelayLifecycleEndpoint` gained the `unregister_worker`/`replace_workers`
surface `RelayDispatchEndpoint` already had, so
`TrustedProviderRuntimeReconciler` rebinds a lifecycle-capable analysis worker
through the same durable replay and checkpoint it uses for every other compute
provider.

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
store enforces uniqueness of `(session_id, idempotency_key)`, so re-discovering
the same slice recognizes the existing durable job instead of creating another.

The source signature comes from the repository's own conservative size/mtime
contract (`catalog/runner/data_filtering.py`), so the behaviour is:

| Change | Result |
| --- | --- |
| Same JSONL rediscovered | Same signature, same job, `created=False` |
| JSONL content modified | New signature, new durable job |
| A second JSONL file added for an already processed date | New signature, new durable job |
| Same data seen again after a restart | Same signature, still deduplicated |
| Analysis contract version bumped over identical data | New identity, new durable job |

The trigger (`automatic-discovery` or `manual-upload`) is deliberately **not**
part of identity and is not in the plan artifact, so an uploaded batch and an
automatic discovery of the same slice resolve to one durable job. The trigger is
still recorded in the local job index for product views.

Discovery skips a date only while a live job exists **for its current signature**.

## How a worker obtains the input

Large JSONL never travels in a control message. Two artifacts are registered per
job:

| Artifact | Schema | Contents |
| --- | --- | --- |
| Plan | `fcp.analysis-plan.v1` | ≤ 8 KiB JSON: dates, script keys, namespace, signature |
| Data slice | `fcp.analysis-data-slice.v1` | deterministic `tar.gz` of the source JSONL for those dates |

`JobContract.inputs` carries only `ArtifactReference` values with `content_hash`
and `size_bytes`.

Authorization is F7.6, unchanged. The grant is issued **after** provider selection
and ownership exist, is scoped `READ_INPUT` to exactly those two artifact
references, is bound to the owning provider, the worker node, the session, the
lease and its generation, and expires no later than the lease. Every single
manifest or chunk request is re-authorized through `authority.authorize_input`;
the deterministic grant ID derived from `(job_id, attempt_id)` is **not a bearer
token**, and knowing it grants nothing. When an attempt ends — completion,
failure, retry, or reassignment — the previous holder's next request fails with
`stale-artifact-grant`. Eligible providers are never pre-authorized during
scheduling.

Bytes move on the **existing F6 contract**: `ObjectTransferManifest`,
`ObjectTransferChunk`, `ObjectTransferReceiver`, and
`FilesystemObjectTransferChunkStore.publish_verified`. Chunk sizing, per-chunk
hashing, whole-object hashing, resumability, and integrity rules are F6's and are
unchanged — this feature invents none of them. What is new is only the *carrier*
that moves those same F6 values:

| Deployment | Carrier |
| --- | --- |
| Standalone (no relay) | `LocalAnalysisArtifactCarrier` — in-process, still through the artifact authority |
| Federated | `RelayAnalysisArtifactEndpoint` — the authenticated relay, same as dispatch |

The F6.2 `VerifiedChunkTransferEndpoint`/`ResumableChunkTransferEndpoint` are
bound to F6's direct-peer carrier, so they could not be reused as-is for
relay-carried transfer; the F6 *contracts* underneath them are what both carriers
speak. On the relay carrier the worker node identity used for authorization is
the authenticated relay actor, never a value supplied in the request body
(`artifact-worker-mismatch`).

A relay client has a single inbound message stream, so exactly one component may
consume it. `RelayLifecycleEndpoint` owns that stream and the artifact endpoint
chains off its `receive_other()`, the same composition the storage and recorder
relays already use.

## Result handling

The worker never declares the durable job complete. It returns an
`ExecutionResult` whose `details.result_reference` names a bounded result
artifact. F7.5 `apply_response` is the single authority that commits a result,
and it enforces the attempt, lease and lease generation, so a duplicate or stale
response cannot publish or commit a competing authoritative result. The dispatch
inbox replays the recorded response for a duplicate dispatch, so execution
happens once.

## Provider selection

Selection is the existing deterministic policy in
`catalog/capabilities/provider_selection.py`, fed by real F8.2 health reports via
`FederatedProviderReportSource`. A provider is eligible only if its report matches
the job's federation session, capability type, protocol and protocol
major/minor, all declared capability requirements, freshness, status, and
available capacity. Ranking prefers free capacity, then low queue depth and
utilization.

There is no synthetic local provider and no local-execution shortcut. Local
execution happens only because *this node advertised the capability through the
real F8 chain and selection chose it*. If the local provider's enrollment is not
approved, or its report is stale, or its capacity is exhausted, the job waits for
a provider like any other job.

## Single-node and standalone installations

A standalone install is modelled as a **federation of one**, not as an exemption.
`DeviceFederationAuthority.for_device` bootstraps a device-local
`SessionCoordinator` in its own `standalone_control.sqlite3`, enrols this device
using its real key-derived `IdentityStore` credentials, and creates the session
the device leads. Enrollment, approval, health, activation, ownership,
dispatch, artifact authorization and result commit are the real components.

Only the carrier differs: `LocalLifecycleTransport` instead of
`RelayLifecycleEndpoint`, and `LocalAnalysisArtifactCarrier` instead of
`RelayAnalysisArtifactEndpoint`. Everything above and below the carrier is
identical, there is a single worker implementation, and there is no
`if not federated: run_analysis_directly()` anywhere. When the device *is*
enrolled in a real federation, the genuine relay path is used even if the selected
provider is the coordinator's own node.

The standalone coordinator database is deliberately separate from the product's
federation coordinator so a standalone install never manufactures phantom
sessions or members in federation views. A device whose supplied identity is not
its own key-derived node ID cannot be enrolled at all; it fails closed with
`analysis-identity-not-enrollable`, has no provider, and its jobs queue durably.

## What happens when no worker is available

The job stays `queued` with its `no-eligible-provider` decision visible. There is
no silent local fallback. If nothing becomes available inside the job's
`queue_timeout_seconds`, the existing lifecycle coordinator times it out.

Other failure paths reuse existing semantics:

| Situation | Behaviour |
| --- | --- |
| Worker unreachable | Attempt is lost, `schedule_retry` applies the retry policy, another provider can be selected |
| Lease expires mid-flight | Worker rejects with `lease-expired`; the response is stale and the attempt is retried |
| Worker crashes | Durable job survives; heartbeat/lease timeouts reassign it |
| Coordinator restarts | Jobs, attempts, leases, and grants are durable SQLite state |
| Data owner unavailable | Worker fails with a bounded, retryable error code |
| Artifact authorization fails | Denied and audited; execution never starts |
| Duplicate dispatch | The dispatch inbox replays the recorded response; execution happens once |
| Result arrives twice | F7.5 commits once; the stale response is refused |

## Startup behaviour

Flask startup is unaffected. Provisioning is local SQLite work with no network
wait, discovery runs on the existing background poller, submission is a local
SQLite write, and scheduling plus execution run on a separate thread
(`AnalysisWorkService.request_scheduling_pass`). Startup never waits for a
federation job, a provider, or a peer. If provisioning cannot complete, the
runtime falls back to `DeviceFederationAuthority.without_authority`, records the
reason, and keeps queueing durable work.

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
| Pending relay artifact requests | 64 per endpoint |
| Concurrent local analysis jobs | 1 (`FCP_ANALYSIS_MAX_CONCURRENT_JOBS`) |

## Known limitations

* Cross-node analysis requires an operator to approve the remote provider's
  enrollment through the existing F8.1 path. That is deliberate: a device cannot
  approve itself into a federation it does not lead. Until approval, a stock
  two-device federation keeps running analysis on whichever node is approved.
* One attempt must complete inside one ownership lease (≤ 1 hour). Longer work
  fails with `lease-expired` and is retried.
* Analysis outputs are written under the executing node's own
  `results/workflows`, so playback on the data owner still requires that the data
  owner was the selected provider. Only the bounded result artifact travels back.
  Publishing full outputs back to the data owner would need an F7.6 output
  placement path and is out of scope here.
* Packing a slice copies and compresses its source JSONL once per new signature.
* Artifact transfer is pull-per-chunk over the carrier; F6 resumability is
  available through the same manifest, but no cross-process resume state is kept
  between attempts, so a retried attempt re-fetches its input.
