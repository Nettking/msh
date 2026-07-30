# Phase E7 returning former-primary recovery plan

Status: **implemented; pending final CI and merge validation**

Date: 2026-07-30

## Objective

Recover a returning former primary as a fenced replica after an E6 promotion. The provider must compare its durable state with the coordinator-owned authoritative manifest, receive only missing committed items through the existing relay-first logical storage/replication path, verify immutable identities and hashes, submit authenticated replica evidence, and remain ineligible until the coordinator confirms exact synchronization.

Phase F direct peer transport is explicitly excluded.

## Acceptance mapping

E7 must close the following matrix requirements:

- **F0-012:** nodes never self-promote; stale authority remains rejected.
- **F0-015:** fencing tokens/terms survive restart and reject obsolete writes.
- **F0-021:** manifest and compatibility evidence remain authoritative.
- **F3-007:** old primary restarts under a newer term and remains fenced.
- **F3-008:** logical routing uses the new primary without recorder configuration changes.
- **F3-009:** stale writes from the former primary are rejected.
- **F3-010:** former primary rejoins as replica; catch-up restores redundancy without overwriting committed data.
- **F4-003:** committed-hash conflict makes the provider ineligible until operator repair or quarantine.
- **F4-004:** missing committed ranges/items are repaired and hashes verified before eligibility.
- **F4-007:** authoritative manifest revision never decreases.
- **F4-008:** recovery is independent per storage group.

## Existing reusable runtime paths

E7 should reuse rather than replace:

- `AuthoritativeStorageManifest` and immutable `ManifestItem` identities;
- `BatchStorageProvider.read()` for source content retrieval;
- `BatchStorageProvider.committed_identity()` for target verification;
- `PhaseDStorageService` replica handling for authenticated `storage-replication` requests;
- current primary assignment, grant, term, and fencing token from `StorageControlPlaneSnapshot`;
- `StorageReplicaReport` and `StorageReplicaAssessment` as the final eligibility gate;
- relay-first `StorageRequestTransport` / replication interfaces;
- provider-local durable fence high-water state established by E6.

No backend-specific path, database credential, filesystem path, or direct peer address may enter the coordinator contract.

## Authoritative invariants

1. The returning provider is assigned replica before recovery starts and remains replica throughout recovery.
2. The old provider-local fence is never cleared, reduced, or replaced by its obsolete term.
3. The current primary is the only actor authorized to send repair writes.
4. The authoritative manifest is immutable input to one recovery attempt; any revision/hash change invalidates the plan and requires replanning.
5. Recovery never combines complementary partial replicas into fictional completeness.
6. Only authoritative committed items may be copied.
7. Existing target content with the exact immutable identity is idempotent success.
8. Missing target content may be delivered through the existing replication request path.
9. An existing target identity with a conflicting hash, schema, dataset, sequence range, or idempotency key is never overwritten automatically.
10. Unknown extra target hashes fail closed; the coordinator must not infer that they correspond to missing authoritative items.
11. Source content must reproduce the authoritative SHA-256 hash before delivery.
12. Target durable identity must match the complete authoritative item after delivery.
13. Eligibility remains false while planning, transferring, verifying, retrying, or awaiting a synchronized report.
14. Completion requires an authenticated report for the same provider/session/group whose manifest revision/hash, committed hashes, datasets, watermarks, missing ranges, integrity, and synchronization state exactly match authority.
15. Duplicate execution and restart reuse the same durable recovery identity and item ledger.
16. Recovery state is scoped by `(session_id, group_id, provider_id, recovery_id)`.
17. One group’s recovery must not affect another group’s authority, reports, or eligibility.
18. Direct peer transport, transport negotiation, NAT traversal, resumable transport chunks, and performance optimization remain Phase F.

## Durable model

### Recovery record

Persist one record containing at least:

- schema/version;
- recovery ID;
- session ID and group ID;
- returning provider ID and registered node ID;
- source current-primary provider/node ID;
- E6 promotion/finalization/publication identity;
- authoritative manifest revision/hash;
- current grant ID, term, fencing token, and lease binding;
- state;
- item counts by status;
- latest error code/reason;
- created/updated/completed timestamps.

Suggested states:

```text
planned
transferring
verifying
awaiting-report
completed
retryable
operator-attention
failed
```

### Immutable recovery-item ledger

Persist one row per authoritative item:

- recovery/session/group/provider identity;
- item ID and kind;
- dataset/schema identity;
- idempotency key;
- content hash and size;
- source ID and sequence range;
- commit state;
- status: `present`, `missing`, `delivered`, `verified`, `conflict`, `failed`;
- stable delivery identity / attempt count;
- last error and timestamp.

Rows are immutable in identity fields; only status/attempt diagnostics advance.

## Deterministic planning

Given the authoritative manifest and returning provider evidence:

1. Validate the provider is the former primary named by the E6 finalization/recovery obligation.
2. Validate it is currently assigned replica and not current primary.
3. Validate the durable provider fence covers the obsolete term.
4. Bind the plan to the current manifest and coordinator grant.
5. Compare the report’s sorted committed-hash multiset with authoritative hashes.
6. Mark exact target identities as `present` only after provider-local `committed_identity()` verification.
7. Mark absent authoritative identities as `missing`.
8. Mark any conflicting immutable identity or unknown extra hash as `conflict` and stop automatic repair.
9. Persist the complete plan before transferring any content.

Do not infer item identity from row counts, sequence maxima, filenames, database rows, or hash position.

## Relay-first catch-up

For each `missing` item in deterministic manifest order:

1. Read the item from the current primary’s backend through the provider-neutral API.
2. Recompute and verify the authoritative content hash.
3. Construct a standard replication request using the current coordinator authority.
4. Deliver it through the existing logical relay-first storage transport to the returning replica.
5. Authenticate the response sender/session/provider.
6. Validate the full immutable result identity.
7. Persist `delivered`, then independently confirm provider-local committed identity and persist `verified`.

A lost response is reconciled through target identity lookup; it must not create another logical item.

## Final verification and eligibility

After all items are verified:

1. Move the recovery to `awaiting-report`.
2. Require a new authenticated report revision from the returning provider.
3. Submit it through the existing coordinator assessment path.
4. Require `accepted=True`, `eligibility=True`, reason `eligible`, exact manifest revision/hash, exact committed hashes, exact datasets/watermarks/ranges, `integrity_verified=True`, and `synchronization_state="synchronized"`.
5. Mark recovery complete only after that assessment is durable.

The recovery transaction does not grant leadership. A recovered replica becomes merely eligible for a future coordinator-controlled selection.

## Failure behavior

### Retryable

- source/target provider temporarily unavailable;
- relay delivery unavailable;
- lost response with no contradictory evidence;
- report not yet received;
- current lease renewal that preserves the same logical authority binding, if explicitly supported.

### Replan required

- authoritative manifest revision/hash changed;
- assignment/grant changed without contradiction;
- current primary changed;
- report revision advanced while recovery is incomplete.

### Operator attention / fail closed

- stale old-primary write attempt;
- provider fence missing or lowered;
- source content hash mismatch;
- target immutable conflict;
- unknown extra committed hash;
- forged/wrong-node report;
- wrong session/group/provider;
- report manifest ahead or conflicting;
- corrupt durable recovery row/item ledger;
- contradictory E6 finalization/publication evidence.

## Checkpoints

### E7.0 — contracts and acceptance traceability

- Pure recovery record/item/status contracts.
- Deterministic planner inputs/outputs.
- Acceptance IDs in test names/comments.
- No transfer logic.

### E7.1 — durable planning and restart replay

- SQLite recovery store.
- Bind E6 finalization/publication, assignment, grant, manifest, provider, and fence.
- Persist complete item ledger before transfer.
- Duplicate/restart plan reuse.

### E7.2 — relay-first missing-item delivery

- Source read/hash verification.
- Existing replication request path only.
- Lost-response reconciliation and exact target identity verification.
- Retryable availability handling.

### E7.3 — conflict and corruption handling

- Immutable conflict, extra hash, source mismatch, stale authority, changed manifest/grant, and corrupt state.
- No automatic overwrite.
- Durable operator diagnostics.

### E7.4 — authenticated final report and eligibility

- New provider report revision.
- Exact assessment gate.
- Completion only after durable eligible synchronized assessment.

### E7.5 — restart, duplicate, and per-group independence

- Crash at each boundary.
- Concurrent/duplicate recovery convergence.
- Two storage groups recover independently.

### E7.6 — end-to-end acceptance

- F3-007 through F3-010.
- F4-003, F4-004, F4-007, F4-008.
- Phase 0/1/2 and Phase D/E regressions.
- Linux and Windows CI.

## First exact implementation unit

Begin with **E7.0 + E7.1 only**:

1. create `catalog/federation/former_primary_recovery.py`;
2. define validated immutable record/item contracts and durable SQLite store;
3. implement deterministic plan creation from E6 proof, current snapshot, manifest, provider fence, and returning report;
4. persist item statuses `present`, `missing`, or `conflict` without transferring data;
5. add focused tests for restart, duplicate plan, stale authority, missing fence, changed manifest, conflict, extra hash, and per-group isolation;
6. add the test file to Linux and Windows CI;
7. do not add relay transfer until E7.1 is green.

## Non-goals

- no direct peer connection;
- no new transport protocol;
- no transport performance work;
- no backend-specific inventory scan in coordinator code;
- no silent overwrite or deletion of conflicting provider data;
- no leader grant to the recovering provider;
- no manifest rewind;
- no Phase G work.

## Completion gate

E7 is complete only when all checkpoints are green, the authoritative progress document is updated, E8 diagnostics expose the recovery state and eligibility reasons, and the end-to-end matrix proves that a fenced former primary returns as an exact synchronized replica without regaining obsolete authority.


## Implementation completion

E7.2-E7.6 are implemented by `former_primary_repair.py` and
`former_primary_completion.py`. The implementation reuses the existing
`storage-replication` relay path, verifies source and target immutable identity,
reconciles lost responses, persists retry/conflict/completion evidence, requires a
new authenticated synchronized report, and exposes E8 operator diagnostics.

The checkpoint is accepted only after Linux and Windows CI are green and the
completion PR is merged to `main`. Phase F direct transport and Phase G scheduling
remain excluded.
