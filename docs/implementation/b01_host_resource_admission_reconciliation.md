# B01 host-resource-admission reconciliation

Status: **draft; not merge-ready**

Reviewed: **2026-08-31 Europe/Oslo**

## Branch and baseline validation

The prior phase ended at the verified branch head
`08a89420d6610520698566af7a47caa0a85465f4`. This continuation was reported
and verified to start at branch head
`0341166d5611d7bbd59616af8e6614a49a1aa26c`. The handoff named
`51d09b573d23909305662c911c9a051a828b758b` as current `main` after B05. During
this continuation, `origin/main` advanced through the B06 merge (#387) and the
ICSE demo merge (#381) to `954faa357638b13d7291e69ea98fa620c0c3d637`. The final
review therefore uses `954faa357638b13d7291e69ea98fa620c0c3d637` as the
current-main baseline and records `51d09b573d23909305662c911c9a051a828b758b`
as the supplied B05 checkpoint.

The branch was validated without merging or rebasing:

- branch: `codex/b01-host-resource-reconciliation-20260831`;
- prior-phase verified branch head: `08a89420d6610520698566af7a47caa0a85465f4`;
- continuation starting head: `0341166d5611d7bbd59616af8e6614a49a1aa26c`;
- override-control continuation starting head:
  `a1ea7acc28b6170d7df13805a70196545badd767`;
- final implementation head before the documentation-only handoff correction:
  `47fae81f4bc05a8b5ad78cc4927b61db78e53d56`;
- current continuation implementation head:
  `0dd64d4a4d8446e1ea6b5a085ef304cefc54d1f1`;
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

### Live-main changes checked for this continuation

The fetched `origin/main` range after the supplied B05 checkpoint consists of
the merged B06 semantic-service-health work (#387) and the merged ICSE demo
publication bundle (#381). Those changes add B06 health/probe surfaces and
`demo/icse`/publication metadata; they do not change the B01 upload, analysis
publication, logical-storage, observer/cache, or managed-temporary writers
implemented here. No continuation commit modifies those B06/ICSE areas.

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
- `catalog/federation/resource_override.py` provides the policy authority for
  authorized, exact-scope, expiring retention/maintenance decisions and the
  durable audit record. Its emergency lease is one-shot, process-local, and
  non-rehydratable; it is only consumed by the shared controller.
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
| Authorized resource-policy override control plane (`federation/resource_override.py`) | Admin-only/permission-gated retention and maintenance records are exact-scope, expiring, durable-audited, and revocable. Emergency leases are exact-operation/resource-set, one-shot, byte/inode-capped, process-local, and accepted only by the shared controller; PRESSURE may be overridden only above the CRITICAL floor. Consequence tests prove refusal, expiry, revocation, reuse, identity/scope mismatch, cap enforcement, audit-write pressure refusal, and exception unwind. | **PROVEN** |
| Recorder capture/recovery/publication (`mtconnect_recorder/resource_pressure.py`, `_resource_pressure_impl.py`) | Sequence-bounded raw XML, manifests, observation NDJSON, normalized JSONL, probes, and checkpoints retain the inherited recorder budget/controller behavior. Recorder publication/outbox ownership and the excluded B03 seam are not re-opened here. | **PARTIAL** |
| Recorder Federation durable outbox (`catalog/federation/outbox.py`) | Initialization/migration and every state-changing transaction now use the shared controller with bounded byte/inode estimates, WAL autocheckpoint/journal-size limits, rollback, and backing-resource identity checks. Completed/retired payload compaction is bounded. Pending rows and the durable idempotency/tombstone history cannot be automatically deleted without changing at-least-once delivery or re-enqueue suppression. | **PARTIAL** |
| Federated JSONL local gzip cache (`_prepare_local_file`) | Inherited source/output bounds, stable-directory handling, source re-stat/hash validation, atomic publication, and SQLite transaction envelopes remain. Authenticated owned `fcp-jsonl-*` cleanup is now bounded and proven; cumulative `local_files`/WAL growth is not closed in this PR. | **PARTIAL** |
| Federated JSONL remote chunk staging (`_write_chunk`, `_record_remote_chunk`) | Inherited completion-at-`PRESSURE` fix admits chunk bytes/inodes and `seen_batches` mutation atomically and preserves identity checks. Authenticated owned `fcp-chunk-*` cleanup is now bounded and proven; cumulative SQLite history remains unresolved. | **PARTIAL** |
| Federated JSONL reconstruction/materialization (`_try_materialize`) | Inherited encoded/raw bounds, mirror quota, atomic replacement, exact size/hash checks, and completion-at-`PRESSURE` transaction envelope remain. Authenticated owned `fcp-encoded-*`/`fcp-raw-*` cleanup is now bounded and proven; cumulative SQLite/WAL growth remains unresolved. | **PARTIAL** |
| Browser multipart parser (`flask_app/data_upload_routes.py`, `flask_app/request_resource_admission.py`) | Declared lengths are rejected before form/file parsing. Unknown-length input without `wsgi.input_terminated` fails closed through Werkzeug's safe fallback without FCP spool creation; a terminated streaming-equivalent request is parsed through an FCP-owned managed spool with request-wide/file-wide byte limits, inode reservation, pressure refusal, and unwind tests. A production WSGI server can still materialize bytes before application code and must provide the deployment-level bounded ingress/temp-root contract. | **PARTIAL** |
| Browser upload staging/publication (`data_upload_service.py`, `data_upload_resource_admission.py`) | Staging, final publication, marker/metadata writes, and asynchronous import now reserve through the shared controller with byte/inode estimates; async pressure leaves durable work queued/hidden and retries after pressure clears. Multi-root requirements are coalesced atomically and existing durability ordering is retained. User-visible retained uploads and lifetime metadata have no product retention policy, and service paths do not yet have the storage-provider-level identity proof. | **PARTIAL** |
| Upload analysis-job metadata links (`upload_analysis_job_service.py`) | Database directory initialization and link insertion/WAL headroom are admitted and exception-safe through the shared controller. The link table is durable cumulative job history without an independent retention policy, so this boundary is not a full aggregate-growth proof. | **PARTIAL** |
| Analysis input workspace/data-owner publication (`capabilities/analysis/resource_admission.py`, scheduler, content store, slice packaging, artifact authority) | Plan and slice publication now share one atomic multi-resource reservation with the discoverability/job mutation; descriptor registration/audit is admission-aware and receives the held reservation, so nested completion bookkeeping does not re-admit. Content-addressed bodies and deterministic archives use bounded managed temporaries, same-filesystem checks, atomic replacement, and cleanup. The real harness proves a successful plan/slice/registry path and pressure refusal before a second input or job is published; the separate registry call and cumulative history remain explicit boundaries. | **PARTIAL** |
| Analysis runtime/session state (`orchestrator/pipeline.py`, `orchestrator/analysis_runtime.py`, managed session metadata) | Runtime workflow-root creation, runtime/startup state JSON, standalone identity, and the analysis capability root now use bounded shared admission; the pressure tests prove refusal before root/state creation. Session metadata writes are covered when entered through the admitted date-slice worker. Durable SQLite histories are scored separately below. | **PROVEN** |
| Analysis result artifact store (`capabilities/analysis/content_store.py`, worker result publication) | Direct artifact writes and worker result serialization have finite bounds, shared byte/inode reservations, atomic partial-to-final replacement, cleanup, and nested `admission_held` handling. Consequence tests cover refusal before publication and reservation release; the score is scoped to the managed artifact-root boundary. | **PROVEN** |
| Analysis executor/script workspaces (`orchestrator/analysis_runtime.py`, `runner/script_exec.py`) | Managed run directories and catalog copies use shared byte/inode admission; subprocess output is drained, checked live, terminated on workspace overflow, and cleaned. Tests prove pressure refusal and real output-over-limit refusal. This does not constrain a malicious/unsupported script that intentionally writes outside its managed workspace. | **PROVEN** |
| Analysis job/lifecycle SQLite stores (`capabilities/job_store.py`, `capabilities/lifecycle_store.py`) | Job, attempt, command, audit, heartbeat, cancellation, retry, and result-reference transactions now hold one shared bounded byte/inode reservation through commit, with rollback-safe unwind and WAL checkpoint/journal limits. The consequence suite proves startup/mutation refusal and exception unwind; durable job/audit history has no authorized aggregate retention/compaction policy, so main-file high-water growth remains unresolved. | **PARTIAL** |
| Analysis runtime authority SQLite stores (`capabilities/analysis/service.py`, `capabilities/artifact_authority.py`, `capabilities/artifact_runtime.py`, `capabilities/provider_enrollment.py`, `capabilities/provider_health.py`, `capabilities/dispatch.py`, `capabilities/lifecycle_worker.py`, `federation/coordinator.py`, `federation/persistence.py`) | The analysis job index, artifact descriptor/grant/publication authority, standalone coordinator, provider enrollment/health stores, dispatch receipts, and cancellation tombstones now use the shared controller at initialization and their real mutation transactions, with bounded byte/inode estimates and WAL checkpoint/journal limits. Startup refusal and runtime integration tests cover the instantiated path; durable job/authority/audit histories still require a product retention/archive policy for a lifetime aggregate proof. | **PARTIAL** |
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
| Crash-stranded `fcp-chunk-*`, `fcp-encoded-*`, `fcp-raw-*`, and B01-owned cache temporaries | Supported JSONL staging/materialization uses authenticated stable-directory owner records; analysis, upload request, observer, source-sync, storage publication, session/filter/index, playback, basic-metrics, content, archive, and telemetry-cache temporaries use authenticated managed roots. Startup/re-entry traversal is bounded, locked active work is skipped, exact filesystem identity and namespace/proof checks are required, and symlinks/reparse points or malformed ownership are never deleted. Consequence tests cover abandoned reclaim, live preservation, unrelated preservation, directory cleanup, and symlink/root safety. Generic or unowned temporary names are deliberately never scavenged. | **PROVEN** |
| Cross-writer SQLite/WAL aggregate growth | Per-transaction shared admission and WAL/journal headroom now cover the outbox, upload metadata, storage provider, analysis job/lifecycle, analysis authority, dispatch, coordinator, provider enrollment/health, and efficiency stores, plus the admitted JSONL/cache paths. The remaining gap is aggregate growth after successful transactions: SQLite main files, durable pending/terminal history, JSONL indexes, storage idempotency rows, upload metadata, user-visible data, and the new override audit ledger still need an authorized retention/archive contract. Safe deletion is constrained by delivery identity, ordering gaps, idempotency, tombstones, source-of-truth semantics, or auditability. | **PARTIAL** |

## Retention and archive decisions requiring Martin

Admission and checkpointing are mechanical safety controls; they are not a
retention policy. No new age, row-count, archive-period, or user-data expiry is
invented by this PR. The table names the smallest product decision needed for
each cumulative store and the consequence of each alternative.

| Cumulative store | Safe mechanical state in this PR | Martin must decide | Recommended alternative and consequence |
|---|---|---|---|
| Analysis job/lifecycle, authority, dispatch, cancellation, result references, and audit SQLite tables | Shared transaction admission, bounded WAL/checkpoint behavior, and rollback/unwind; durable rows remain. | Lifetime retention, audited archive, or deletion frontier for terminal jobs and their idempotency/fencing evidence. | Retain active/pending and the minimum replay/idempotency evidence; archive terminal history to an operator-owned durable medium before deletion. Without the archive identity and replay contract, classify aggregate growth PARTIAL. |
| Federation outbox and retirement/tombstone history | Payload compaction is safe for completed/retired rows; pending rows and identity/tombstone columns remain. | Whether and how acknowledged terminal rows may be archived while preserving at-least-once delivery, ordering-gap detection, and re-enqueue suppression. | Keep pending and tombstone identity until an explicit successor archive is authoritative; compact payloads mechanically, but do not delete rows from this PR. |
| Federated JSONL `local_files`, `seen_batches`, and `materialized_files` SQLite/index state | Writes are admitted; temporary ownership is proven; raw/source and materialized files remain available. | Source-of-truth retention horizon and whether cache/index rows can be rebuilt from retained raw files or a manifest archive. | Treat raw source/accepted manifest as truth and make cache/index cleanup operator-controlled after an explicit replay horizon; automatic deletion before that would risk duplicate publication or incomplete reconstruction. |
| Logical storage provider batch files and `storage-index.sqlite3` | Shared multi-resource admission, inode accounting, backing-resource checks, WAL checkpointing, and immutable ingest semantics. | Whether committed batches are forever, exported to an authoritative archive, or deleted by dataset/batch policy. | Keep immutable batches and idempotency identity until an audited archive replacement exists; deletion without that replacement breaks replay and conflict detection. |
| Browser-uploaded files, upload records, and upload-analysis link metadata | Staging/publication/metadata transactions are admitted and bounded; retained user data is not silently deleted. | User-visible retention, archive/download contract, and whether analysis links survive file retirement. | Require an explicit operator/user retention setting plus link tombstone semantics; default to no deletion in B01. |
| Observer Phoenix JSONL source-of-truth export | Per-record/file/export limits, shared admission, atomic replacement, and managed temp cleanup; date files remain cumulative. | Whether old source exports are archived, retained forever, or deleted by an audited source watermark. | Retain raw observer export until an independently replayable archive exists; otherwise the source-of-truth contract is weakened. |
| Recorder raw evidence and recorder publication history | Recorder safety pauses before filesystem failure; B03 publication seam remains outside this PR. | Separate recorder evidence/archive policy, if any, and how publication replay remains possible. | Do not introduce automatic deletion under B01; make any archive/deletion policy a recorder/B03 decision with its own evidence. |
| Execution-efficiency observations and derived profiles | Existing configured observation retention and profile rebuild semantics remain; WAL is mechanically checkpointed. | Whether physical SQLite high-water space must be compacted, and the safe operational trigger for VACUUM/backup replacement. | Keep current logical retention; add explicit online backup/VACUUM mechanics only after Martin chooses an operational size/maintenance policy. |
| Telemetry Parquet cache and analysis derived/cache artifacts | Disposable and rebuildable; source JSONL/session data is authoritative; rebuild tree is bounded and authenticated. | No user-data retention decision is required for the cache itself; only whether operators want a maintenance trigger/telemetry for rebuild cost. | Keep cache disposable and rebuildable. This is not a cumulative-retention blocker for the cache boundary. |
| Session run outputs and playback/analysis exports | Per-run writes are admitted and bounded; completed user-visible outputs remain available for cache reuse. | Whether completed run outputs are retained forever, archived, or pruned when a session is closed. | Keep until an explicit session-output policy exists; deletion must update session metadata and preserve any required audit/replay references. |
| Resource override audit ledger | Each issuance/revocation rewrite is bounded, atomically published, identity-checked, and admitted through the shared controller; emergency leases are not rehydrated from it. | Required audit lifetime, archive medium, and whether an immutable external archive becomes the authoritative record. | Keep the local ledger append-history until an authoritative archive/checkpoint contract exists; then compact only behind that identity. Do not silently delete audit evidence as part of B01. |

The recommendation column is a design recommendation for Martin, not an
implemented retention policy. Until those decisions are made, aggregate-growth
rows remain PARTIAL even though active writes, WAL behavior, and crash-temp
ownership are bounded.

## Fresh adversarial review

The completed review attacked the implementation and the tests at the actual
write boundaries:

| Attack | Result and disposition |
|---|---|
| TOCTOU/path identity and cross-filesystem replacement | Stable JSONL writes use descriptor/handle-relative operations and compare the pinned backing-resource identity. Managed temporary writers compare exact `(st_dev, st_ino)` identities and reject root/path substitutions; publication refuses when the temporary and destination devices differ. |
| Inode exhaustion and write amplification | Reservations carry bytes and inodes; atomic peaks include old body plus replacement temporary, owner records, markers, directories, WAL margin, and bounded transfer populations. Live bounded writers reject output growth rather than trusting a nominal estimate. |
| WAL/journal growth | Supported SQLite writers set synchronous durability, bounded autocheckpoint and journal limits, and admit transaction headroom. Main-file high-water growth is intentionally not claimed closed without retention/compaction authority. |
| Symlink/reparse substitution | Stable boundaries use `O_NOFOLLOW`/handle-relative `OBJ_DONT_REPARSE`; managed roots reject symlink/reparse roots, sidecars, and children. Tests preserve unrelated and symlink-named entries. |
| Concurrent scavenging/writing and stale processes | Durable owner sidecars carry authenticated root/resource/file identity; exclusive owner locks make live files active, and re-entry only deletes an unlocked exact identity. Preparing records with no durable file identity are left ambiguous. |
| SIGKILL/crash leftovers | Ready owned files/directories are reclaimable at bounded startup/re-entry; unowned, malformed, ambiguous, and active names are never deleted. There is no prefix-and-age deletion fallback. |
| Reservation unwind and nested admission/deadlock | Failure tests prove reservations release; enclosing analysis publication passes `admission_held` through content, slice, store, and artifact registration so completion bookkeeping does not re-admit. Multi-resource reservations are grouped atomically by backing-resource identity. |
| Validation precedence | Pure job/plan/slice validation remains before host measurement. Multipart declared-length validation occurs before form/file access; unknown un-terminated requests fail closed without FCP spool creation. |
| Windows/Linux divergence | Windows temporary handles are closed before replacement, owner locks are released before sidecar deletion, and stable NT handle-relative replacement/deletion is retained. POSIX symlink/race tests run where the platform permits them; privilege-limited Windows symlink cases are explicit skips, not passing assertions. |
| Mocks hiding writer behavior | The added analysis test uses the real harness stack, content store, lifecycle/job database, artifact authority, and registry for success/refusal consequences. Existing unit tests remain focused on individual writer failure/unwind paths. |
| Override authority abuse, expiry, and hard-floor bypass | The lease is minted only by `ResourceOverrideAuthority`, requires the new admin-only `resource.override` permission (or an explicit trusted equivalent), binds to one exact operation and measured resource set, is one-shot and expiring, and cannot pass CRITICAL or exceed byte/inode caps. The 7-test consequence suite uses the real serialized controller and verifies audit-write refusal and unwind. |

The review found and fixed the Windows open-handle replacement failure, the
Windows sidecar cleanup ordering failure, malformed ownership/marker handling,
managed-allocation collision cleanup, storage exception cleanup that could
remove a replaced final path, the storage marker leaking into the batch
namespace, and a missing artifact-authority admission seam. The remaining
limitations are the production WSGI pre-route boundary, product retention
authority, and deliberately ambiguous temporary records; none is hidden by a
green unit test.

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
  observer enumeration fix;
- `a90f746` — admitted the remaining analysis result/script publication paths,
  including bounded metadata/index/basic-metrics/playback writes, nested
  admission propagation, managed atomic temporaries, and consequence tests;
- `cf3ed3b` — closed the next residual tranche: controlled request-body
  spooling for declared and terminated-stream multipart requests, authenticated
  bounded scavenging for supported B01 temporary roots, managed temporaries for
  observer/source-sync/storage/cache/JSONL paths, logical-storage identity-safe
  cleanup, and admission-aware artifact-authority registration/audit writes.
- `82e46d2` — normalized the touched B01 admission code to the repository lint
  baseline.
- `47fae81` — refreshed the scorecard, retention/archive decision table,
  live-main baseline, adversarial findings, and handoff language.
- `0dd64d4` — added the authorized, audited retention/maintenance policy
  override authority and one-shot bounded emergency admission lease on the
  shared process-wide controller, with pressure, hard-floor, scope, cap,
  expiry, revocation, audit-write, reuse, and unwind consequence tests.

The earlier Federated JSONL completion-at-`PRESSURE` work is inherited by this
branch and was not reworked as a writer-ledger refinement.

## Verification evidence

The pre-override focused consequence run after `82e46d2` collected **308 tests**
and passed all 308. It covers the directly affected analysis,
artifact-authority, upload, temporary-root, observer/cache, JSONL, storage, and
outbox regression files. That same set passed against the final implementation
head immediately before the documentation-only ledger/handoff commit; the
documentation-only commit does not change production or test code:

```text
pytest --basetemp .pytest-b01-final-focus9 -q \
  catalog/common/tests/test_managed_temporary.py \
  catalog/capabilities/tests/test_analysis_resource_admission.py \
  catalog/capabilities/tests/test_durable_sqlite_resource_admission.py \
  catalog/capabilities/tests/test_analysis_publication_resource_admission.py \
  catalog/capabilities/tests/test_artifact_authorization.py \
  catalog/capabilities/tests/test_artifact_logical_endpoints.py \
  catalog/capabilities/tests/test_analysis_slice_publication.py \
  catalog/capabilities/tests/test_analysis_registered_slice_stability.py \
  catalog/capabilities/tests/test_analysis_scheduling.py \
  catalog/orchestrator/tests/test_analysis_runtime_integration.py \
  catalog/orchestrator/tests/test_pipeline_bootstrap.py \
  catalog/runner/tests/test_script_workspace_admission.py \
  catalog/runner/tests/test_data_filtering.py \
  catalog/runner/tests/test_playback_strategy_cache.py \
  catalog/flask_app/tests/test_data_upload_resource_admission.py \
  catalog/flask_app/tests/test_data_upload.py \
  catalog/flask_app/tests/test_upload_analysis_jobs.py \
  catalog/federation/tests/test_stable_filesystem.py \
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

The 308-test collection includes 10 analysis-publication tests, 12 multipart
resource-admission tests, 4 managed-temporary tests, 7 stable-filesystem tests,
13 local-storage tests, 24 allocation tests, 9 runner filtering/playback/script
tests, 4 observer/cache-admission tests, 7 telemetry-cache tests, 12
upload-analysis-job tests, and the directly relevant outbox/phase-1 suites.
These groups overlap by design and are not additive beyond the 308 collection.

`python -m compileall -q catalog` passed before the ledger commit and is rerun
after it. Ruff is run on every changed Python file with the repository's
established baseline exclusions (`B008`, `S110`, `DTZ001`, `DTZ003`, `DTZ004`,
`BLE001`, and `RUF100`); no new findings are accepted. `git diff --check` is a
separate gate. A repository-wide collection was attempted but cannot be a
green signal in this checkout because acceptance/Flask modules require
uninstalled `email_validator`/`flask_security` dependencies. No physical
acceptance path was accessed.

The new override consequence suite passed **7 tests** on `0dd64d4`:

```text
python -m pytest -o addopts= --basetemp .pytest-b01-override -q \
  catalog/federation/tests/test_resource_overrides.py
```

The combined directly relevant collection on the implementation-plus-ledger
workspace collected **315 tests**, with **310 passed** and **5 platform skips**.

## Architectural blockers and exact residual work

1. **Pre-route multipart spooling:** the Flask/Werkzeug application now bounds
   declared requests before form parsing and, when the WSGI environment
   explicitly provides `wsgi.input_terminated`, owns a bounded FCP spool with
   request/file byte and inode reservations. An unknown-length request without
   that signal fails closed without creating the FCP spool. A production WSGI
   server or reverse proxy may still materialize bytes before application code;
   the smallest remaining follow-up is a deployment-level bounded ingress and
   temp-root contract, with evidence from the actual supported server.
2. **Cumulative SQLite and durable retention:** checkpointing bounds transient
   WAL behavior, not the main database or durable history. The outbox cannot
   delete pending/terminal rows without changing at-least-once delivery,
   idempotency, or retirement-gap semantics; JSONL and storage indexes likewise
   need an explicit source-of-truth retention/archive contract. The smallest
   coherent follow-up is one product-level retention design that names the
   archive/identity replacement, then implements bounded compaction/retention
   for outbox, JSONL indexes, storage catalogue, and upload metadata together.
   The override authority now provides the requested authorized, scoped,
   expiring, audited decision mechanism and a bounded emergency admission
   escape hatch, but it does not choose retention values or authorize deletion
   by itself. Its own audit ledger consequently needs the same explicit audit
   lifetime/archive decision.
3. **Supported crash-stranded temporary files:** authenticated managed roots
   and the stable JSONL directory boundary now reclaim abandoned owned files
   under bounded traversal, while locked/live, malformed, symlink/reparse,
   unrelated, and generic unowned files remain untouched. Arbitrary temporary
   files outside those explicit B01 roots are not safely attributable and are
   deliberately not scavenged.
4. **Recorder publication/B03 seam:** discovery/frontier and incremental
   publication remain owned by PR #384 and are not residual work to fold into
   this PR.

## Handoff facts

After the documentation-only correction is pushed, the exact final head SHA,
current-main SHA, complete tracked changed-file list, and exact-head workflow
identifiers will be recorded in this section. The prior implementation head is
`47fae81f4bc05a8b5ad78cc4927b61db78e53d56`; current main is
`954faa357638b13d7291e69ea98fa620c0c3d637`; the PR base and merge-base are
`17e279c01ae6d48ca9c0f4a0b3eaddbb5922d0ef`. This document does not accept
physical evidence, declare B01 complete, or authorize a merge.
