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

## Inventory scope now under review

The production-path inventory is covering at least:

- recorder capture, recovery, status/checkpoint completion, and publication;
- Federated JSONL cache, SQLite state, staging, reconstruction, and
  materialization;
- browser upload staging and publication;
- analysis workspaces, slices, input publication, and durable outputs;
- host Docker image builds, BuildKit cache, and image retirement;
- model/provider downloads and persistent model storage;
- supported backup, export, import, and migration paths; and
- additional large or cumulatively unbounded writers discovered by code search.

The classified writer ledger, smallest remaining blocker set, chosen
implementation scope, adversarial review, and exact-head test/CI evidence will
replace this in-progress section before the draft is ready for review.

## Current limitations of this checkpoint

No writer is declared fully reconciled by this initial checkpoint. No new B01
fix has been selected or implemented yet. The next checkpoint will publish the
complete code-backed writer ledger before substantial implementation begins.
