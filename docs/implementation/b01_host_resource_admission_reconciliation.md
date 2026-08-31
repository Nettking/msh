# B01 host-resource-admission reconciliation

Status: **in progress; draft evidence ledger**

Reviewed: **2026-08-31 Europe/Oslo**

Exact baseline: `main` at `17e279c01ae6d48ca9c0f4a0b3eaddbb5922d0ef`

This is a fresh reconciliation of B01 against production code and tests. It does
not treat the historical checklist, merged pull-request titles, or green CI as
proof that a writer is protected.

## Classification contract

Every supported persistent writer found by the review will receive exactly one
of these final classifications:

- `PROVEN`: the current production path and focused tests prove the applicable
  B01 properties.
- `PARTIAL`: some applicable properties are implemented, but at least one real
  safety property is not proved.
- `MISSING`: the supported writer has no effective B01 admission at its actual
  write boundary.
- `NOT_V1_SUPPORTED`: code exists, but the current v1 installed-product path
  does not expose or instantiate it.
- `OBSOLETE_REQUIREMENT`: the presumed writer/path no longer exists or no
  longer performs the write the old checklist attributed to it.

For each candidate the final ledger records what it writes, the backing
resource, its finite or streaming bound, byte/inode reservation, controller
sharing and atomicity, emergency completion reserve, refusal behavior,
temporary/reconstruction writes, unwind behavior, destination-identity risk,
and restart/crash consequences.

## Exact-main foundation already proved

The shared resource vocabulary and process-local reservation primitive are real
current production code, not merely a plan:

- `catalog/federation/host_resources.py` measures the nearest existing ancestor
  of the requested destination, identifies the mounted resource, measures free
  bytes and inodes where exposed, and classifies unavailable, invalid, stale, or
  implausibly future measurements as `CRITICAL`.
- The same module defines ordered `NORMAL`, `WARNING`, `PRESSURE`, and
  `CRITICAL` thresholds. New bounded work is refused at `PRESSURE` or worse and
  is also refused when its declared maximum would consume the `CRITICAL`
  byte/inode reserve.
- `catalog/federation/process_resource_admission.py` supplies the production
  process-wide singleton `PROCESS_RESOURCE_ADMISSION`. It serializes measurement
  with accounting, coalesces requirements sharing one resource, and publishes
  multi-resource reservations atomically through `reserve_many()`.
- Context-manager unwind releases reservations on both success and exceptions.
  Focused tests cover all four byte states, inode-only exhaustion, unavailable
  and stale measurements, same-resource aggregation, distinct resources,
  exception unwind, measurement/accounting serialization, same-resource
  coalescing, and cross-resource all-or-nothing refusal.

These primitives provide process-local accounting only. They do not reserve
space against unrelated host processes, and measuring a missing destination via
its current ancestor does not by itself prove that a later-created or substituted
destination remains on that resource. Each writer must therefore still be
proved at its real host/process boundary, including any destination-identity
check appropriate to that path.

## Classified writer ledger

The following ledger is based on the exact baseline above and names the actual
write boundary rather than the caller that requested it. `PROVEN` means proven
for the stated supported boundary only; it is not a claim that every manually
invoked Docker or host command is safe.

| Writer and evidence | Persistent write, bound, and backing resource | Admission, completion, crash/identity evidence | Classification |
|---|---|---|---|
| Shared primitive (`host_resources.py`, `process_resource_admission.py`) | Measures the nearest existing ancestor and accounts bytes plus inodes per mounted resource. | Serialized decision/accounting, same-resource coalescing, atomic `reserve_many`, and unwind are directly tested. Process-local only; no protection from unrelated processes. | **PROVEN foundation** |
| Recorder capture/recovery/publication (`mtconnect_recorder/resource_pressure.py`, `_resource_pressure_impl.py`) | Sequence-bounded raw XML, manifest, observation NDJSON, normalized JSONL, probe and checkpoint files. `RecorderResourceBudget` supplies finite byte/inode envelopes. | One aggregate controller is shared by recorder transaction writers; completion admission permits only already-durable recovery work while preserving the critical floor. Atomic replacement and focused refusal/unwind tests exist. Destination confinement and the recorder's own outbox are separate boundaries. | **PARTIAL** |
| Recorder Federation outbox (`catalog/federation/outbox.py`) | SQLite rows carry JSON payloads up to `MAX_PAYLOAD_BYTES` (1 MiB), but offline history is cumulative and SQLite/WAL growth is not globally bounded. | `BEGIN IMMEDIATE` gives transaction serialization, but this writer never calls `PROCESS_RESOURCE_ADMISSION`; no byte/inode admission or completion envelope exists. | **MISSING** |
| Federated JSONL local gzip cache (`_prepare_local_file`) | Source is capped by `FEDERATED_JSONL_MAX_FILE_BYTES`; gzip output is capped by `FEDERATED_JSONL_MAX_ENCODED_BYTES`; cache temp/final files share the configured cache resource. | Stable-directory handles, source re-stat/hash, bounded writer, and cache reservation are present. The SQLite `local_files` mutation is attempted through a nested normal reservation while the outer cache reservation is active; at `PRESSURE` that nested reservation refuses an operation that has already been admitted. | **PARTIAL** |
| Federated JSONL remote chunk staging (`_write_chunk`, `_record_remote_chunk`) | Each decoded chunk is bounded by the protocol/chunk limit and staged as a content-addressed `.chunk`; staged-cache byte/file quotas exist. | Stable boundary and atomic temp-to-final rename are present, but the chunk reservation ends before `seen_batches` SQLite bookkeeping is reserved. Hard-killed `fcp-chunk-*`/other temporary names are not covered by the restart scan, and the SQLite/WAL envelope is not proven for all batch histories. | **PARTIAL** |
| Federated JSONL reconstruction/materialization (`_try_materialize`) | Declared encoded and raw file sizes are bounded; one encoded gzip and one raw JSONL temp coexist before atomic publication. Mirror quota bounds retained remote bytes. | `reserve_many` covers encoded plus raw peaks, stable-directory identity checks, exact size/hash validation, and atomic replacement exist. The subsequent `materialized_files`/staged-row SQLite mutation uses a nested normal reservation and can be refused at `PRESSURE`; temporary reconstruction cleanup is only context-manager based, so a hard kill can strand names. | **PARTIAL; current implementation target** |
| Browser multipart parser (`flask_app/data_upload_routes.py` request parsing) | Werkzeug/WSGI may spool the request body to the OS temporary filesystem before the route's admission guard; app `MAX_CONTENT_LENGTH` is 1.1 GiB by default. | No shared byte/inode reservation can run before `request.form`/`request.files` parsing, and the parser temp resource is not pinned to the configured upload roots. | **MISSING** |
| Browser upload staging/publication (`data_upload_resource_admission.py`, `data_upload_service.py`) | Staging reserves configured total bytes and files/inodes; final uploaded data is intentionally cumulative and user-visible. | Shared admission and exception unwind cover staging. Reservation ends when enqueue returns, before asynchronous final-directory/marker and metadata writes; roots are independently configurable and only lexical checks/revalidation are used. SQLite metadata is cumulative without a lifetime admission budget. | **PARTIAL** |
| Analysis input workspace/data-owner publication (`capabilities/analysis/resource_admission.py`) | Input plan/slice and data-owner publication use bounded workspace/atomic-replacement envelopes. | Shared admission and worker/scheduler wrappers exist, but the stable destination boundary, all metadata writes, and result-output lifecycle are not covered by the same proof. | **PARTIAL** |
| Analysis result artifact store (`capabilities/analysis/content_store.py:LocalArtifactContentStore.write_bytes`, `worker.py:_result`) | Atomic `.partial` file is written under the artifact root; payload is not given a finite schema-wide cardinality bound before serialization (store has only a per-object byte ceiling). | No `PROCESS_RESOURCE_ADMISSION` reservation or pinned stable-directory identity at this write boundary; a failed write cleans its temp, but restart/quota accounting is not host-resource admission. | **MISSING** |
| Analysis executor/script workspaces (`orchestrator/analysis_runtime.py`, `runner/script_exec.py`) | Copies catalog/data and permits selected scripts to create arbitrary run outputs below `results/workflows`; no durable aggregate ceiling is enforced. | Directory creation and `copytree` are direct writes without admission. Arbitrary script output makes a safe finite envelope unproven. | **MISSING** |
| Logical Federation storage provider (`federation/storage_allocation.py`, `FilesystemBatchStorageProvider.ingest`) | One JSON batch is relay-bounded (65,536 bytes at the protocol), and a preallocated allocation file/floor limits the provider's own byte budget. | SQLite transaction serialization, temp/replace and byte claims exist, but this is not the shared process controller, does not account inodes/WAL, and does not prove destination identity across allocation/provider roots. | **PARTIAL** |
| Observer Phoenix JSONL export (`observer_phoenix/export_jsonl.py`, Compose `observer-sync`) | Appends deduplicated records to date-partitioned JSONL; record count and cumulative files are not bounded by admission. | No host-resource controller, atomic aggregate reservation, or restart cleanup for append growth. This is a supported Compose profile, not merely a test helper. | **MISSING** |
| Telemetry Parquet cache rebuild (`common/telemetry_cache.py:rebuild_cache`) | Rebuild writes a temporary cache tree and swaps it into place; temporary and retained cache can coexist and size is data-dependent. | No shared admission or inode accounting around the duplicate tree; swap is atomic at the directory-name level but not a host-space proof. | **MISSING** |
| Host Docker image builds/cache retirement (controlled build/update launchers) | Docker backing path is resolved, build context and image/tag lifecycles are bounded by the host build policy; cache/image writes occur in Docker's own resource. | Controlled launchers use host-resource preflight, pressure monitoring, writer stop/quiescence and post-build identity checks. Manual `docker build` or arbitrary Docker configuration is outside this supported boundary. | **PROVEN for controlled path; NOT_V1_SUPPORTED otherwise** |
| Model/provider download (`model_resource_pull.py`, `start.sh`, Windows update/setup handoff) | Model size is intentionally unknown; the pull is an optional writer into the Docker model volume. | Host-owned backing-resource resolution, NORMAL/WARNING preflight, continuous pressure polling, verified stop and model verification are implemented. The documentation still advertises direct `docker compose ... model-provider-install`/`ollama-pull` commands that bypass this helper. | **PARTIAL for documented direct commands; PROVEN for helper path** |
| Agent log and Docker json-file logs (`federation/agent_log.py`, `docker-compose.yml`) | Agent log is capped at 10 MiB plus bounded tail; Compose services use 10m/3-file rotation. | Rotation/copy/truncate behavior and tests are already in-tree. This is prior B07 hardening, not an unresolved B01 writer. | **PROVEN / outside remaining B01 scope** |
| Legacy upload payload duplication (`data_upload_records`) | Current import path streams and validates without persisting the old full payload column. | Existing regression asserts zero legacy payload rows. | **OBSOLETE_REQUIREMENT** |
| Durable resumable transfer, backup/export/import/migration paths not instantiated by the v1 product | Code/search finds helpers and migration tooling, but no supported installed-product runtime boundary with a current admission contract. | Until a supported invocation is identified, claiming either protection or a new fix would be speculation. | **NOT_V1_SUPPORTED pending evidence** |

## Implemented in this continuation

Commit `11c60cd` targets the concrete Federated JSONL completion bug above:
local-cache and materialization transactions now admit their bounded SQLite
bookkeeping in the same atomic reservation as the large filesystem peak, and
their inner bookkeeping path uses that already-held reservation instead of
re-admitting at `PRESSURE`. Both implementation variants (the stable-filesystem
public class and its implementation base) use the same identity-matched
reservation selection. A regression test exercises the real serialized
controller at the pressure boundary. This scoped change cannot close
chunk-ingest completion, uploads, analysis outputs, or cumulative outboxes.

Focused verification at this head:

- `python -m pytest -q --basetemp=.pytest-tmp catalog/flask_app/tests/test_federated_jsonl_resource_admission.py`
  — **15 passed, 1 skipped**;
- the combined Federated JSONL/model admission subset — **32 passed, 3
  skipped**; and
- `ruff check` on both bridge implementations and the focused test — **passed**.

The repository-wide collection was also attempted. It is not a valid green
signal in this checkout because 36 Flask/acceptance modules cannot import the
uninstalled `email_validator`/`flask_security` dependencies; no acceptance
machine or acceptance state was accessed.

## Open blockers and assumptions

- The ledger is intentionally conservative where a writer can be entered from
  a supported command but has no finite aggregate lifetime bound (outbox,
  observer export, telemetry cache, analysis scripts).
- A reservation against an ancestor is not treated as a destination identity
  proof. The JSONL paths that use `StableDirectory` are stronger; upload,
  analysis, storage-provider, and SQLite boundaries still need independent
  identity/restart work.
- The JSONL SQLite reserve is a bounded transaction envelope, not a proof that
  arbitrary historical SQLite/WAL growth fits that envelope. Chunk bookkeeping
  and hard-kill temporary cleanup remain unresolved.
- No physical Federation machine, acceptance state, merge action, or release
  state was touched. Draft PR #383 remains the only publication target.

The exact head SHA, focused/full test commands, CI run identifiers, findings
from the adversarial pass, and any new unresolved issue are recorded below as
each incremental commit lands.
