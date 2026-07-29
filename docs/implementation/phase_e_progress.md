# Phase E completeness-aware failover progress

Last updated: 2026-07-29

## Objective

Implement Phase E (the architecture documents call it Phase 4 / PR E):
authoritative per-storage-group manifests, committed hashes, contiguous
watermarks and explicit missing ranges, authenticated provider integrity and
eligibility reporting, deterministic completeness-aware candidate selection,
safe `storage-degraded` behavior, coordinator-controlled promotion, returning
former-primary recovery, and operator diagnostics.

Phase F / Phase 5 direct peer transport is explicitly excluded. Phase E remains
correct over the existing relay-first transport.

## Base, branch, and pull request

- Repository: `Nettking/msh`
- Exact base commit:
  `05bbfe40dafa2b4661aa313cf7dba25e01a5a90d`
- Base source: head of draft PR #121, `agent/complete-phase-d-integration`
- Default branch at start:
  `main` at `f85b10f2e197b8ebfa32bd530d93d00b6e5c3e80`
- Phase D status at start: PR #121 was open, draft, unreviewed, and unmerged.
  Its GitHub Actions run `30409541564` completed successfully on Linux and
  Windows.
- Current branch: `agent/phase-e-completeness-aware-failover`
- Draft pull request: pending creation after this bootstrap commit is pushed.
  Until PR #121 merges, the Phase E PR must target
  `agent/complete-phase-d-integration` so its diff contains only Phase E.

Do not rebase this branch onto `main` unless PR #121 has merged and the new base
has been verified. Do not build from the older Phase D branches.

## Checkpoint status

| Checkpoint | Status | Scope |
| --- | --- | --- |
| E0 | Current | Contract and acceptance mapping; pure completeness contracts |
| E1 | Remaining | Authoritative manifest persistence and storage-commit integration |
| E2 | Remaining | Watermarks, missing ranges, conflicts, restart persistence |
| E3 | Remaining | Authenticated replica integrity and eligibility reporting |
| E4 | Remaining | Deterministic complete-candidate selection |
| E5 | Remaining | Durable safe `storage-degraded` state |
| E6 | Remaining | Coordinator promotion transaction and idempotency |
| E7 | Remaining | Returning former primary and relay-first catch-up |
| E8 | Remaining | Diagnostics, end-to-end acceptance, full validation |

Completed checkpoints: none.

Current checkpoint: E0 — Contract and acceptance mapping.

## Acceptance mapping

The test-matrix scenario IDs must remain traceable in test names or comments.

| Checkpoint | Requirements and applicable matrix items |
| --- | --- |
| E0 | Define manifest, completeness, eligibility, degraded-state, fencing, persistence, compatibility, and relay-first invariants. Map F4-001–F4-008; selection/completeness portions of F0-001–F0-012 and F0-020–F0-022; model compatibility F0-017–F0-019; required negative assertions. |
| E1 | Durable per-group manifest, committed identities/idempotency keys/hashes, schema, required datasets, acknowledgement/commit state, monotonic revision/hash, atomic or recoverable commit update, duplicate and restart safety. F0-005, F0-017–F0-019, F0-021; F1-002–F1-004, F1-006; F2-009; F3-001, F3-002, F3-004, F3-005; F4-005–F4-008. |
| E2 | Contiguous committed watermarks, normalized inclusive missing ranges, deterministic merge/split, empty datasets, conflict detection, restart persistence, and no row-count inference. F0-004, F0-006, F0-009, F0-010, F0-021; F3-002; F4-002, F4-004, F4-007, F4-008. |
| E3 | Provider-scoped reports with session/group/provider/node identity, manifest revision/hash, committed hashes, watermarks, ranges, integrity, synchronization, role, term/token, eligibility and reasons. Authenticate actor binding and reject impersonation, wrong session/group, stale/conflicting reports, and unknown protocol majors. F0-007–F0-009, F0-012, F0-016, F0-019, F0-021; F2-004, F2-006–F2-009; F3-010; F4-003. |
| E4 | Pure deterministic selection after complete eligibility gating; sticky equivalent healthy primary; explicit reasons; no false union of complementary partial replicas. F0-001–F0-012, F0-020–F0-022; F4-001–F4-003. |
| E5 | Persist and expose `storage-degraded`; revoke/fence invalid authority; reject writes; retain manifests, recorder batches, replication obligations, and repair evidence. F0-010, F0-012–F0-016; F1-003; F3-003, F3-005; F4-002, F4-003, F4-005. |
| E6 | Idempotent coordinator command that revokes/fences old authority, advances term/token, changes assignment and grant consistently, rejects stale writes, and changes logical routing without recorder configuration. F0-001–F0-003, F0-012–F0-016, F0-022; F2-004, F2-009; F3-003, F3-006, F3-007, F3-009; F4-001, F4-007. |
| E7 | Former primary rejoins as replica, remains fenced, compares manifests, identifies missing/conflicting ranges, catches up through existing logical relay-first storage/replication interfaces, verifies integrity, and only then becomes eligible. F0-012, F0-015, F0-021; F3-007–F3-010; F4-003, F4-004, F4-007, F4-008. |
| E8 | Per-provider role/revision/watermark/ranges/integrity/sync/eligibility/reasons/current-primary/degraded diagnostics and all requested end-to-end cases. F4-001–F4-008 plus directly related F0, F2, and F3 regressions. |

Exact Phase 4 acceptance ledger:

- F4-001: promote the highest complete verified replica after primary expiry.
- F4-002: incomplete available replicas produce `storage-degraded`.
- F4-003: committed hash mismatch makes a replica ineligible until repaired.
- F4-004: repair missing committed ranges and verify hashes before eligibility.
- F4-005: a primary-only pre-ack batch is not session committed; recorder retry
  remains safe.
- F4-006: policy-acknowledged data survives failover on enough nodes.
- F4-007: authoritative manifest revision never decreases across restart or
  handover.
- F4-008: completeness and leadership are independent per storage group.

## Authoritative invariants and decisions

1. Manifest authority is coordinator-owned and scoped by
   `(session_id, group_id)`. Provider-local catalogues are evidence, not the
   authoritative session manifest.
2. Manifest revision is a per-group, monotonically increasing coordinator
   revision. Dataset revision and source sequence watermarks are separate
   values. Candidate-local state ahead of the authoritative manifest is not
   promoted automatically.
3. Only acknowledgement-policy-satisfied immutable storage commits may advance
   the authoritative committed manifest. Primary-local/provisional storage does
   not increase authoritative completeness.
4. Committed identities include batch/object identity, idempotency identity,
   immutable SHA-256 content hash, dataset/schema identity, commit state, and
   sequence coverage when the dataset defines a sequence space.
5. Missing ranges use inclusive integer endpoints, are sorted, disjoint, and
   non-adjacent after normalization. An empty dataset has no watermark and no
   missing range unless the authoritative contract explicitly requires a
   sequence interval.
6. A required dataset is complete only when its schema matches, committed
   identities and hashes match the authoritative manifest, its contiguous
   watermark matches, and it has no missing or conflicting committed range.
7. Completeness is evaluated per storage group. Complementary partial replicas
   are never combined into a fictional complete promotion candidate.
8. Eligibility is coordinator-computed from authenticated provider evidence.
   A provider may report evidence but may not grant itself eligibility or report
   for another provider.
9. Provider identity is distinct from node identity. Reports, diagnostics,
   rejection reasons, and promotion commands are keyed by provider ID and bind
   that provider to its registered node.
10. A healthy complete current primary remains preferred when no candidate is
    authoritatively better. A node never self-promotes.
11. When no complete eligible candidate exists, the group enters persisted
    `storage-degraded`, has no writable grant, retains all committed state and
    pending obligations, and exposes explicit reasons.
12. Promotion is coordinator-controlled and idempotent. Old authority is
    revoked/fenced before the replacement can accept writes; term and fencing
    token increase monotonically.
13. Every provider enforces a durable local authority fence in addition to
    consulting coordinator state, so a returning or isolated former primary
    cannot write using obsolete authority.
14. A returning former primary is assigned replica, compares its local
    manifest with authority, repairs through existing logical batch
    replication over relay, verifies hashes, and only then becomes eligible.
15. Unknown protocol major versions are rejected. Additive same-major fields
    remain compatible. Backend credentials, paths, and physical addresses stay
    local.
16. Existing recorder local-first ordering and durable acknowledgement
    semantics remain unchanged. Offline replicas remain explicit in
    completeness and acknowledgement accounting.
17. Direct peer transport, NAT traversal, peer ports, direct encrypted transfer
    streams, transport negotiation, resumable transport chunks, and transport
    performance changes are Phase F and excluded.

## Phase D extension points

- `PhaseDControlPlane` owns replayed assignments/grants plus durable fencing and
  acknowledgement policy. Its SQLite transaction boundary is the coordinator
  hook for manifests, degraded state, reports, and promotion commands.
- `PhaseDStorageService` is the active primary/replica request path. It prepares
  replication before provider mutation and reconciles interrupted prepared
  entries. Phase E must add manifest commit preparation/reconciliation without
  returning false commit success.
- `BatchIngestRequest` is the active immutable wire contract. Any schema,
  sequence, size, or source metadata added to it must also be preserved by
  `PhaseDLogicalStorageClient`, `DurableRecorderDeliveryQueue`, replication
  payloads, and both storage backends.
- `FilesystemBatchStorageProvider` and `PostgreSQLBatchStorageProvider` are the
  backend-neutral persistence points. Both need equivalent manifest metadata,
  listing, integrity verification, and durable local fencing behavior.
- `DurableAcknowledgementStore` preserves the assigned replica set and durable
  acknowledgement state, but is primary-local and is not coordinator authority.
- `RelayStorageEndpoint` is the existing relay-first transport. Phase E must
  bind responses to the authenticated expected sender/session/provider and
  validate returned immutable commit identity before accepting acknowledgement.
- Existing pure `select_storage_leader` logic is a starting point but is keyed
  by node rather than provider and is not connected to runtime state.
- `PhaseDControlPlane.complete_handover` already performs revoke, assignment,
  grant, and counter changes in one SQLite transaction, but it is not
  command-idempotent and cannot promote from a no-primary/degraded state.

## Files changed

- `docs/implementation/phase_e_progress.md` — created as the authoritative
  interruption-safe handoff record.

No runtime files have been changed yet.

## Tests and exact results

Baseline at exact base commit:

- Phase 0/1 regressions:
  `64 passed in 4.27s`.
- Phase D tests excluding PostgreSQL-backed cases:
  `49 passed in 5.42s`.
- PostgreSQL provider test file with no local PostgreSQL service:
  `2 passed, 4 skipped in 9.06s`.
- Phase 2 node/relay suite:
  `84 passed, 1 skipped in 47.46s`.
- GitHub Actions for PR #121 head: run `30409541564`, conclusion `success`
  on Linux and Windows.

Initial attempts before the local virtual environment was populated failed at
test collection because `psycopg` was absent. This was an environment setup
failure, not a repository failure; the repository dependencies were then
installed in ignored `.venv/` and the results above were obtained.

## Known failures and limitations

- PR #121 is not reviewed or merged. Phase E is therefore a stacked change and
  must retain its exact base until the dependency is resolved.
- A PostgreSQL server is not currently running locally, so four PostgreSQL
  cases skip. They passed in PR #121 Linux CI and must run again in Phase E CI.
- Docker is installed, but sandboxed commands warn that the user Docker config
  is inaccessible. Compose validation still needs to be run and recorded.
- The Phase D relay response path does not yet authenticate the responder
  against the expected target, and acknowledgement responses are not yet
  checked against batch identity. E3 must close this before reports or
  completeness depend on those responses.
- Phase D storage providers do not persist provider-local fencing high-water
  state. E6/E7 must add it.

## Unresolved questions

- Whether maintainers will merge PR #121 before Phase E review. If it merges,
  retarget this PR to `main` without rebasing or force-pushing checkpoint
  history unless a conflict requires an explicit reviewed migration.
- The authoritative documents leave manual degraded-mode override unspecified.
  Safe Phase E implements no automatic incomplete promotion and no write-enabled
  override.
- PostgreSQL-backed validation depends on CI or a locally available disposable
  PostgreSQL service.

## Exact next recommended action

Push this documentation-only bootstrap commit, create the draft PR titled
`Implement Phase E completeness-aware failover` against
`agent/complete-phase-d-integration`, replace the pending PR entry above with
the PR number, then complete E0 pure contracts and tests. Do not start E1 until
the green E0 commit has been pushed and the PR description updated.

## Safe to resume

Yes. This document-only branch state contains no runtime changes and is safe to
resume from. The exact base and stacked-PR dependency are recorded above.
