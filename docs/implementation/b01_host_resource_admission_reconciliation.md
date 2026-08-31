# B01 host-resource-admission reconciliation

Status: **draft; not merge-ready**

Reviewed: **2026-08-31 Europe/Oslo**

## Branch and baseline validation

The continuation started from the verified branch head
`08a89420d6610520698566af7a47caa0a85465f4`. The handoff named
`51d09b573d23909305662c911c9a051a828b758b` as current `main` after B05. During
this continuation, `origin/main` advanced through the B06 merge (#387) and the
ICSE demo merge (#381) to `954faa357638b13d7291e69ea98fa620c0c3d637`. The final
review therefore uses `954faa357638b13d7291e69ea98fa620c0c3d637` as the
current-main baseline and records `51d09b573d23909305662c911c9a051a828b758b`
as the supplied B05 checkpoint.

The branch was validated without merging or rebasing:

- branch: `codex/b01-host-resource-reconciliation-20260831`;
- initial verified branch head: `08a89420d6610520698566af7a47caa0a85465f4`;
- final head is recorded in the handoff section below;
- current-main baseline: `954faa357638b13d7291e69ea98fa620c0c3d637`;
- merge-base: `17e279c01ae6d48ca9c0f4a0b3eaddbb5922d0ef`;
- the B06 files introduced by #387 and ICSE demo files introduced by #381 are
  in `main`, not in this branch diff;
- no B06 health files, ICSE demo files, physical Federation machines, physical
  evidence, merge action, or release state were accessed.

The parallel-work boundary was respected. No continuation change touches
`catalog/federation/recorder_publication.py`, recorder publication discovery or
frontier design, or the B03 incremental publication seam. Those remain PR #384
work.

## Classification contract

Every writer boundary in the scorecard receives exactly one of these statuses:

- `PROVEN`: production code and consequence tests prove the applicable B01
  admission/refusal/unwind properties for the stated supported boundary.
- `PARTIAL`: a meaningful bounded admission exists, but a real applicable
  property remains unproved or is an explicit architectural boundary.
- `MISSING`: a supported writer still has no effective admission at its actual
  write boundary.
- `OUT-OF-SCOPE`: the path is not an instantiated/supported v1 writer for this
  B01 product contract, or is explicitly owned by another PR.

`PROVEN` is deliberately scoped to the named boundary. It is not a claim that
manual Docker commands, unrelated host processes, physical acceptance, or
unbounded historical retention are safe.

## Exact-main foundation

The shared resource authority is real production code:

- `catalog/federation/host_resources.py` measures the nearest existing ancestor
  of a requested destination, identifies its backing resource, and measures
  free bytes and free inodes where the platform exposes them.
- `NORMAL`, `WARNING`, `PRESSURE`, and `CRITICAL` are shared states. New work is
  refused at `PRESSURE` or worse and when its bounded reservation would consume
  the critical byte/inode floor.
- `catalog/federation/process_resource_admission.py` provides the process-wide
  `PROCESS_RESOURCE_ADMISSION`, serialized measurement/accounting, same-resource
  coalescing, atomic `reserve_many`, and exception-safe release.
- Focused tests cover byte pressure, inode-only exhaustion, unavailable/stale
  measurements, same-resource aggregation, distinct resources, serialization,
  cross-resource all-or-nothing refusal, and exception unwind.

This is process-local accounting. It does not reserve space against an
unrelated process, and measuring a missing leaf through its current ancestor is
not by itself a proof that a later-created or substituted destination remains on
that resource. The scorecard calls out the boundaries where that distinction
still matters.

## B01 writer scorecard

| Writer boundary | Consequence and remaining proof | Status |
|---|---|---|
| Shared process-wide admission (`host_resources.py`, `process_resource_admission.py`) | Measures bytes/inodes by backing-resource identity; serializes the decision; coalesces requirements; supports atomic multi-resource reservations and unwind. | **PROVEN** |
| Recorder capture/recovery/publication (`mtconnect_recorder/resource_pressure.py`, `_resource_pressure_impl.py`) | Sequence-bounded raw XML, manifests, observation NDJSON, normalized JSONL, probes, and checkpoints retain the inherited recorder budget/controller behavior. Recorder publication/outbox ownership and the excluded B03 seam are not re-opened here. | **PARTIAL** |
| Recorder Federation durable outbox (`catalog/federation/outbox.py`) | Initialization/migration and every state-changing transaction now use the shared controller with bounded byte/inode estimates, WAL autocheckpoint/journal-size limits, rollback, and backing-resource identity checks. Completed/retired payload compaction is bounded. Pending rows and the durable idempotency/tombstone history cannot be automatically deleted without changing at-least-once delivery or re-enqueue suppression. | **PARTIAL** |
| Federated JSONL local gzip cache (`_prepare_local_file`) | Inherited source/output bounds, stable-directory handling, source re-stat/hash validation, atomic publication, and SQLite transaction envelopes remain. Hard-kill temporary cleanup and cumulative `local_files`/WAL growth are not closed in this PR. | **PARTIAL** |
| Federated JSONL remote chunk staging (`_write_chunk`, `_record_remote_chunk`) | Inherited completion-at-`PRESSURE` fix admits chunk bytes/inodes and `seen_batches` mutation atomically and preserves identity checks. Hard-kill `fcp-chunk-*` cleanup and cumulative SQLite history remain unresolved. | **PARTIAL** |
| Federated JSONL reconstruction/materialization (`_try_materialize`) | Inherited encoded/raw bounds, mirror quota, atomic replacement, exact size/hash checks, and completion-at-`PRESSURE` transaction envelope remain. Hard-kill temporary cleanup and cumulative SQLite/WAL growth remain unresolved. | **PARTIAL** |
| Browser multipart parser (`flask_app/data_upload_routes.py`) | A declared `Content-Length` is rejected before Flask form/file parsing using the configured total-file ceiling plus bounded multipart overhead. Unknown-length/chunked requests can still be spooled by WSGI/Werkzeug before the route runs; that framework-controlled resource is not safely attributable to the configured upload roots. | **PARTIAL** |
| Browser upload staging/publication (`data_upload_service.py`, `data_upload_resource_admission.py`) | Staging, final publication, marker/metadata writes, and asynchronous import now reserve through the shared controller with byte/inode estimates; async pressure leaves durable work queued/hidden and retries after pressure clears. Multi-root requirements are coalesced atomically and existing durability ordering is retained. User-visible retained uploads and lifetime metadata have no product retention policy, and service paths do not yet have the storage-provider-level identity proof. | **PARTIAL** |
| Upload analysis-job metadata links (`upload_analysis_job_service.py`) | Database directory initialization and link insertion/WAL headroom are admitted and exception-safe through the shared controller. The link table is durable cumulative job history without an independent retention policy, so this boundary is not a full aggregate-growth proof. | **PARTIAL** |
| Analysis input workspace/data-owner publication (`capabilities/analysis/resource_admission.py`, scheduler) | Existing plan/slice publication is covered by bounded reservations and scheduler completion bookkeeping avoids nested re-admission. The complete data-owner metadata/result lifecycle and a destination identity proof across every publication root remain outside the consequence tests. | **PARTIAL** |
| Analysis runtime/session state (`orchestrator/pipeline.py`, `orchestrator/analysis_runtime.py`, managed session metadata) | Runtime workflow-root creation, runtime/startup state JSON, standalone identity, and the analysis capability root now use bounded shared admission; the pressure tests prove refusal before root/state creation. Session metadata writes are covered when entered through the admitted date-slice worker. Durable SQLite histories are scored separately below. | **PROVEN** |
| Analysis result artifact store (`capabilities/analysis/content_store.py`, worker result publication) | Direct artifact writes and worker result serialization have finite bounds, shared byte/inode reservations, atomic partial-to-final replacement, cleanup, and nested `admission_held` handling. Consequence tests cover refusal before publication and reservation release; the score is scoped to the managed artifact-root boundary. | **PROVEN** |
| Analysis executor/script workspaces (`orchestrator/analysis_runtime.py`, `runner/script_exec.py`) | Managed run directories and catalog copies use shared byte/inode admission; subprocess output is drained, checked live, terminated on workspace overflow, and cleaned. Tests prove pressure refusal and real output-over-limit refusal. This does not constrain a malicious/unsupported script that intentionally writes outside its managed workspace. | **PROVEN** |
| Analysis job/lifecycle SQLite stores (`capabilities/job_store.py`, `capabilities/lifecycle_store.py`) | Job, attempt, command, audit, heartbeat, cancellation, retry, and result-reference transactions now hold one shared bounded byte/inode reservation through commit, with rollback-safe unwind and WAL checkpoint/journal limits. The consequence suite proves startup/mutation refusal and exception unwind; durable job/audit history has no authorized aggregate retention/compaction policy, so main-file high-water growth remains unresolved. | **PARTIAL** |
| Analysis runtime authority SQLite stores (`capabilities/analysis/service.py`, `capabilities/provider_enrollment.py`, `capabilities/provider_health.py`, `capabilities/dispatch.py`, `capabilities/lifecycle_worker.py`, `federation/coordinator.py`, `federation/persistence.py`) | The analysis job index, standalone coordinator, provider enrollment/health stores, dispatch receipts, and cancellation tombstones now use the shared controller at initialization and their real mutation transactions, with bounded byte/inode estimates and WAL checkpoint/journal limits. Startup refusal and runtime integration tests cover the instantiated path; durable job/authority/audit histories still require a product retention/archive policy for a lifetime aggregate proof. | **PARTIAL** |
| Execution-efficiency SQLite learning store (`capabilities/efficiency/store.py`) | Initialization and every observation/profile/decision transaction now use shared bounded byte/inode admission; observation/decision rows have existing retention limits, oversized decisions are rejected, and WAL checkpoint/journal limits are explicit. SQLite high-water pages are not safely reclaimed by row pruning alone, so a bounded VACUUM/retention policy remains an architectural follow-up. | **PARTIAL** |
| Logical Federation storage provider (`federation/local_storage.py`) | Provider initialization and ingest now use the shared controller plus the existing `StorageAllocation`; bytes/inodes cover publication and SQLite/WAL headroom, identity is checked after mkdir and replacement, and checkpoint/WAL limits are configured without weakening immutable ingest/failover semantics. Durable batch files and the identity catalogue are intentionally cumulative, so a retention/compaction policy is still needed for a full lifetime aggregate proof. | **PARTIAL** |
| Observer Phoenix JSONL export (`observer_phoenix/export_jsonl.py`) | Each export and date-partitioned replacement has bounded record/byte/file limits, inode margins, atomic temporary replacement, shared admission, and cleanup/unwind tests. The export can still accumulate bounded date files indefinitely because no source-of-truth retention contract authorizes deletion. | **PARTIAL** |
| Telemetry Parquet cache rebuild (`common/telemetry_cache.py`) | Disposable rebuilds cap source bytes, duplicate-tree output bytes, and inodes; old and new trees are admitted together; temporary trees are cleaned on failure; source JSONL remains authoritative. Failure tests preserve an existing cache and no partial tree. | **PROVEN** |
| Controlled Docker image build/cache retirement | Supported launchers have host-resource preflight, pressure monitoring, writer stop/quiescence, post-build identity checks, and bounded cache/image policy. | **PROVEN** |
| Manual/arbitrary Docker build or arbitrary Docker storage configuration | No supported v1 boundary gives this process authority over arbitrary operator-invoked Docker writes. | **OUT-OF-SCOPE** |
| Supported model/provider download helper (`model_resource_pull.py`, setup handoff) | Host-owned backing-resource resolution, preflight, continuous polling, verified stop, and model verification are implemented for the supported helper path. | **PROVEN** |
| Raw Compose model-pull/service invocation outside the supported helper | The raw service is an implementation detail and does not expose the supported B01 admission contract. | **OUT-OF-SCOPE** |
| Agent and Docker json-file logs | Prior B07 rotation/copy/truncate boundaries are already bounded and are not an unresolved B01 writer in this continuation. | **OUT-OF-SCOPE** |
| Legacy `data_upload_records` full-payload duplication | The current import path does not persist the obsolete full payload column; this is no longer a supported writer boundary. | **OUT-OF-SCOPE** |
| Durable resumable transfer, backup/export/import, and migration helpers not instantiated by v1 | No supported installed-product invocation was identified. Claiming admission here would be speculation; adding one would be a separate product boundary. | **OUT-OF-SCOPE** |
| Crash-stranded `fcp-chunk-*`, `fcp-encoded-*`, `fcp-raw-*`, and cache temporary files | Normal context-manager unwinding is covered, but the generic temporary-file helper has no durable transaction ownership marker. Prefix/age deletion could remove ambiguous user or recovery data, so startup scavenging is intentionally not implemented. | **PARTIAL** |
| Cross-writer SQLite/WAL aggregate growth | Per-transaction shared admission and WAL/journal headroom now cover the outbox, upload metadata, storage provider, analysis job/lifecycle, analysis authority, dispatch, coordinator, provider enrollment/health, and efficiency stores, plus the admitted JSONL/cache paths. The remaining gap is aggregate growth after successful transactions: SQLite main files, durable pending/terminal history, JSONL indexes, storage idempotency rows, upload metadata, and user-visible data still need an authorized retention/archive contract. Safe deletion is constrained by delivery identity, ordering gaps, idempotency, tombstones, or source-of-truth semantics. | **PARTIAL** |

## Implemented in this continuation

The coherent implementation slices pushed to the branch are:

- `214277e` — analysis result artifact and script-workspace admission, bounded
  serialization/copy/output handling, live subprocess refusal, and unwind tests;
- `44055b7` — upload staging/publication and asynchronous metadata admission,
  earliest enforceable multipart limit, logical storage provider shared
  admission/inode/identity/WAL handling, and observer/cache implementation/tests;
- `d507ec5` — shared admission around durable outbox initialization/migration and
  all state-changing SQLite transactions, with pressure/refusal/unwind tests;
- `a0e9f04` — shared admission for analysis runtime workflow-root and
  runtime/startup state writes, with startup pressure refusal coverage;
- `a2dc64f` — shared admission for direct script-workspace helper entry points,
  closing the public-helper bypass found by adversarial scanning;
- `5dcca17` — shared admission for analysis runtime identity, job/lifecycle,
  provider-authority, dispatch-inbox, job-index, and efficiency SQLite writers,
  with startup/refusal/unwind consequence tests;
- `a4ba794` — deferred admitted analysis workspace reconciliation until after
  pure job validation, preserving refusal ordering and constructor side-effect
  behavior;
- `dabab65` — admitted runtime session-metadata reconciliation writes and
  refreshed the baseline to the latest fetched `main`;
- `b36799b` — mechanical normalization of the touched Python files plus the
  observer enumeration fix.

The earlier Federated JSONL completion-at-`PRESSURE` work is inherited by this
branch and was not reworked as a writer-ledger refinement.

## Verification evidence

The final focused consequence set collected **214 tests** and passed all 214:

```text
pytest --basetemp .pytest-b01-final-focus -q \
  catalog/capabilities/tests/test_analysis_resource_admission.py \
  catalog/capabilities/tests/test_durable_sqlite_resource_admission.py \
  catalog/capabilities/tests/test_analysis_publication_resource_admission.py \
  catalog/orchestrator/tests/test_analysis_runtime_integration.py \
  catalog/orchestrator/tests/test_pipeline_bootstrap.py \
  catalog/runner/tests/test_script_workspace_admission.py \
  catalog/flask_app/tests/test_data_upload_resource_admission.py \
  catalog/flask_app/tests/test_data_upload.py \
  catalog/flask_app/tests/test_upload_analysis_jobs.py \
  catalog/federation/tests/test_local_storage.py \
  catalog/federation/tests/test_storage_allocation.py \
  catalog/federation/tests/test_storage_discovery_regressions.py \
  catalog/federation/tests/test_storage_exhaustion_acceptance.py \
  catalog/federation/tests/test_outbox_resource_admission.py \
  catalog/federation/tests/test_outbox_compaction.py \
  catalog/federation/tests/test_outbox_retirement.py \
  catalog/federation/tests/test_phase1.py \
  catalog/common/tests/test_observer_and_cache_resource_admission.py \
  catalog/common/tests/test_telemetry_cache.py
```

Additional focused results were 10 analysis/script-admission tests, 8 upload
resource-admission tests, 13 local-storage tests, 33 storage regression and
exhaustion tests, 4 observer/cache consequence tests, 5 upload-analysis-job
tests, and 63 outbox/phase-1/compaction/retirement tests. These subsets overlap
the 214-test final set.

`python -m compileall -q catalog` passed. Ruff passed for the changed Python
files when the repository's existing baseline rules (`B008`, `S110`, `DTZ003`,
`BLE001`, and `RUF100`) were excluded; no new import/format/lint findings
remain. A repository-wide collection was attempted but cannot be a green
signal in this checkout because acceptance/Flask modules require uninstalled
`email_validator`/`flask_security` dependencies. No physical acceptance path
was accessed.

## Architectural blockers and exact residual work

1. **Pre-route multipart spooling:** only a declared request length can be
   rejected before Werkzeug/WSGI parsing. Proving control of unknown-length
   framework spooling requires a deployment-level bounded stream/temp-root
   contract. The smallest safe follow-up is to configure and verify that
   boundary in the supported WSGI deployment, then add an unknown-length
   consequence test; application-route admission alone cannot close it.
2. **Cumulative SQLite and durable retention:** checkpointing bounds transient
   WAL behavior, not the main database or durable history. The outbox cannot
   delete pending/terminal rows without changing at-least-once delivery,
   idempotency, or retirement-gap semantics; JSONL and storage indexes likewise
   need an explicit source-of-truth retention/archive contract. The smallest
   coherent follow-up is one product-level retention design that names the
   archive/identity replacement, then implements bounded compaction/retention
   for outbox, JSONL indexes, storage catalogue, and upload metadata together.
3. **Crash-stranded temporary files:** the generic helper creates names but no
   durable owner marker. A safe scavenger needs either a dedicated managed temp
   root with authenticated transaction markers or a durable ownership table,
   plus strict age/path/marker checks and crash/re-entry tests for all prefixes.
   Prefix/age deletion alone is explicitly unsafe.
4. **Recorder publication/B03 seam:** discovery/frontier and incremental
   publication remain owned by PR #384 and are not residual work to fold into
   this PR.

## Handoff facts

The exact final head SHA, current-main SHA, complete tracked changed-file list,
workflow/run identifiers, and adversarial findings are maintained in the final
PR #383 handoff after the ledger commit. This document does not accept physical
evidence, declare B01 complete, or authorize a merge.
