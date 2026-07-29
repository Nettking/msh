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
- Draft pull request: #122,
  `https://github.com/Nettking/msh/pull/122`
- PR title: `Implement Phase E completeness-aware failover`
- PR base: `agent/complete-phase-d-integration`. Until PR #121 merges, the
  Phase E PR must keep this target so its diff contains only Phase E.

Do not rebase this branch onto `main` unless PR #121 has merged and the new base
has been verified. Do not build from the older Phase D branches.

## Checkpoint status

| Checkpoint | Status | Scope |
| --- | --- | --- |
| E0 | Completed | Contract and acceptance mapping; pure completeness contracts |
| E1 | Completed | Authoritative manifest persistence and storage-commit integration |
| E2 | Completed | Watermarks, missing ranges, conflicts, restart persistence |
| E3 | Completed | Authenticated replica integrity and eligibility reporting |
| E4 | Completed | Deterministic complete-candidate selection |
| E5 | Completed | Durable safe `storage-degraded` state |
| E6 | Paused pending design approval | Coordinator promotion transaction and idempotency |
| E7 | Remaining | Returning former primary and relay-first catch-up |
| E8 | Remaining | Diagnostics, end-to-end acceptance, full validation |

Completed checkpoints: E0, E1, E2, E3, E4, E5.

Current checkpoint: E5 is complete. E6 implementation is paused pending design
approval for the coordinator-controlled promotion transaction.

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
18. The new richer manifest uses
    `msh.authoritative_storage_manifest.v1`; it does not reuse the incompatible
    legacy `msh.storage_manifest.v1` model. Existing Phase 0 public models remain
    backward compatible.
19. Canonical manifest hashes include schema, session/group, revision, term,
    predecessor hash, commit state, datasets, and committed items. They exclude
    wall-clock `updated_at`. Changing the leadership term therefore requires a
    new manifest revision even when data is unchanged.
20. `source_id` is a stable logical sequence namespace, never a node address,
    filesystem path, DSN, or transport endpoint. The E0 contract models one
    sequence namespace per dataset; multiple physical sources must use distinct
    dataset identities unless a later explicit schema revision generalizes it.
21. E1 persists coordinator-owned manifest genesis and every immutable revision
    in SQLite with `WAL`, `synchronous=FULL`, per-group monotonic revisions,
    predecessor hashes, verified materialized projections, and full-chain
    verification on both head and historical reads.
22. A storage write prepares a coordinator manifest intent before provider
    mutation. The intent contains immutable identity and commit-policy metadata,
    not the potentially large content body. Finalization updates the intent,
    manifest revision, head, datasets, and items in one SQLite transaction.
23. Manifest-intent replay is idempotent across restart, grant renewal, and
    handover. A policy-satisfied historical intent may be finalized after
    authority changes, but the resulting manifest term never decreases.
24. Existing Phase D groups with storage history but no E1 manifest are marked
    `inventory-required`; they do not receive a silently empty authoritative
    genesis. Provider inventory must be reconciled before later completeness or
    eligibility decisions can treat them as authoritative.
25. Dataset schema name/version are additive same-major request fields with
    defaults for old peers. They are preserved through recorder delivery,
    routing, acknowledgement state, replication, filesystem/PostgreSQL provider
    identity, manifest intent, and authoritative manifest publication.
26. Provider restart recovery uses a read-only committed-identity lookup. It
    never replays ingest for a prepared-but-absent write, and it fails on an
    immutable identity mismatch.
27. Relay responses are accepted only from the expected authenticated node and
    session and, when present for rolling compatibility, the expected provider.
    Malformed or spoofed frames cannot terminate the shared reader loop.
28. Recorder-facing logical success is accepted only after immutable batch,
    dataset, schema, item-kind, commit-state, manifest revision, and manifest
    hash evidence is verified against the coordinator-owned manifest history.
    A provider cannot self-assert commit and cause removal of a local recorder
    obligation.
29. Phase D service-owned replication entries carry an explicit owner marker,
    are term-scoped, and are not consumed by the generic Phase D replication
    worker. Replica results must match the full immutable batch identity before
    acknowledgement.

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

- `docs/implementation/phase_e_progress.md` — authoritative interruption-safe
  handoff record, acceptance mapping, E1 decisions, and exact validation.
- `catalog/federation/manifest.py` — pure E0 contracts for inclusive sequence
  ranges, immutable committed items, dataset coverage targets, canonical
  per-group manifest chains, and verified SHA-256 hashes.
- `catalog/federation/manifest_store.py` — E1 durable coordinator-owned
  manifests, materialized projections, prepared commit intents, corruption
  verification, atomic finalization, and fail-closed legacy migration state.
- `catalog/federation/phase_d_control.py` — manifest-store ownership, group
  genesis, commit-intent preparation/finalization, and manifest reads/history.
- `catalog/federation/phase_d_service.py` — recoverable manifest integration,
  read-only restart reconciliation, replica-response validation, and
  service-owned term-scoped replication delivery.
- `catalog/federation/phase_d_client.py` — authoritative manifest verification
  before recorder-visible commit success.
- `catalog/federation/storage_protocol.py` — additive dataset schema
  name/version fields.
- `catalog/federation/commit_tracking.py` — durable schema-aware immutable
  acknowledgement identity and legacy column migration.
- `catalog/federation/local_storage.py` — schema-aware read-only committed
  identity and SQLite migration.
- `catalog/federation/postgres_storage.py` — equivalent schema-aware identity
  and PostgreSQL migration.
- `catalog/federation/recorder_delivery.py` — dataset schema propagation while
  preserving local-first delivery.
- `catalog/federation/relay_storage.py` — authenticated response binding and
  malformed-response isolation.
- `catalog/federation/replication.py` — service-owned outbox isolation and
  immutable replica-result validation.
- `catalog/federation/__init__.py` — exports the additive E0/E1 contracts.
- `catalog/federation/tests/test_phase_e0_contracts.py` — E0 pure contract
  tests.
- `catalog/federation/tests/test_phase_e1_manifest_store.py` — genesis,
  monotonic revision, restart, duplicate, conflict, transaction, projection,
  chain-corruption, and legacy migration tests.
- `catalog/federation/tests/test_phase_e1_service_manifest.py` — primary and
  replicated commit integration, crash recovery, grant renewal/handover,
  response authentication, recorder evidence, and legacy acknowledgement tests.
- `catalog/federation/tests/test_local_storage.py` — dataset/schema identity
  and legacy filesystem-index migration coverage.
- `catalog/federation/tests/test_phase_d2_postgres.py` — shared backend schema
  identity plus isolated PostgreSQL legacy-schema migration coverage.
- `catalog/federation/tests/test_phase_d6_replication.py` — service-owned
  outbox isolation regression.
- `.github/workflows/phase2-federation.yml` — E0/E1 Linux and Windows test
  coverage.

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

E0 checkpoint validation:

- Focused E0 contracts:
  `9 passed in 1.62s`.
- E0 plus Phase 0/1 regression suite:
  `73 passed in 3.57s`.
- Phase D regression suite excluding PostgreSQL-backed cases:
  `49 passed in 6.74s`.
- Phase 2 node/relay suite:
  `84 passed, 1 skipped in 61.04s`.
- Python compilation:
  `python -m compileall -q catalog setup_msh.py` passed.
- Ruff on changed Python paths with the repository CI ignore set:
  passed with `All checks passed!`.
- Docker Compose base and `relay-dev` profile validation:
  passed.
- `git diff --check`:
  passed.

E1 checkpoint validation on the frozen candidate worktree:

- Python compilation:
  `python -m compileall -q catalog setup_msh.py` passed.
- Focused E0/E1, filesystem, PostgreSQL-provider, and replication tests:
  `53 passed, 7 skipped in 24.64s`.
  All seven skips are PostgreSQL cases because no local PostgreSQL server is
  running; the filesystem half of parameterized provider tests passed.
- Full Phase D storage suite plus E0/E1:
  `91 passed, 7 skipped in 29.08s`.
- Phase 0/1 regression suite:
  `64 passed in 4.68s`.
- Phase 2 federation/node/relay suite:
  `84 passed, 1 skipped in 48.50s`.
- Standalone recorder v2 plus recorder control-service compatibility:
  `20 passed in 1.16s`.
- Ruff on `catalog/federation`, `catalog/node`, and `catalog/relay` with the
  repository CI ignore set:
  passed with `All checks passed!`.
- Docker Compose base and `relay-dev` profile validation:
  passed.
- `git diff --check`:
  passed.
- PostgreSQL migration coverage was added to Linux CI using an isolated schema.
  E0/E1 tests were added to both Linux and Windows CI; remote results are
  pending the E1 push.

## Known failures and limitations

- PR #121 is not reviewed or merged. Phase E is therefore a stacked change and
  must retain its exact base until the dependency is resolved.
- A PostgreSQL server is not currently running locally, so seven PostgreSQL
  cases skip. The E1 PostgreSQL migration test and existing provider suite must
  pass in the draft PR's Linux CI before E1 is treated as independently
  reviewed.
- Existing pre-E1 groups are deliberately blocked in `inventory-required`
  state rather than receiving an unsafe empty manifest. The provider inventory
  reconciliation that clears this state belongs to later completeness/recovery
  checkpoints and is not implemented in E1.
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

After the E1 checkpoint commit `Add authoritative storage manifests` is pushed,
inspect its Linux and Windows checks on draft PR #122, especially the
PostgreSQL migration case. If either check fails, fix only E1 and publish
another coherent E1 checkpoint before beginning E2. If both are green, a new
Codex task may begin E2 by reading this file, the draft PR, and checkpoint
history. Do not start E2 in the current task run.

## Safe to resume

Yes, once the E1 checkpoint commit described above is pushed. The frozen E1
worktree is internally coherent and locally green; it changes runtime behavior
only as one integrated authoritative-manifest unit. If interruption occurs
before that push, the last remote-safe head remains green E0 commit
`dd923bc30dc5f60b0133bfd90cba312bd596a2b5`. The exact base and stacked-PR
dependency are recorded above.

## E2 checkpoint update

The current task is implementing E2 only. The working tree now contains a
coherent E2 coverage layer built on top of the frozen E1 manifest chain:

- `catalog/federation/manifest.py` adds inclusive range normalization,
  subtraction, and dataset coverage helpers.
- `catalog/federation/manifest_store.py` adds restart-safe dataset coverage
  updates persisted as new authoritative manifest revisions.
- `catalog/federation/tests/test_phase_e2_watermarks.py` covers empty datasets,
  first committed range, contiguous append, gaps, gap filling, adjacent merge,
  overlapping evidence, restart reconstruction, multi-dataset isolation, and
  malformed input.

Exact validation for the E2 checkpoint so far:

- `42 passed in 10.14s` for the focused E0/E1/E2 suite.
- `64 passed in 7.17s` for `catalog/federation/tests/test_phase0.py` and
  `catalog/federation/tests/test_phase1.py`.
- `28 passed in 8.71s` for `catalog/federation/tests/test_phase2_unit.py`.
- `5 passed in 3.85s` for `catalog/federation/tests/test_phase_d6_replication.py`.
- `10 passed in 3.50s` for `catalog/federation/tests/test_local_storage.py`.
- `All checks passed!` for Ruff on the changed Python files.
- `python -m compileall -q catalog setup_msh.py` passed.

Known limitation:

- `catalog/federation/tests/test_phase_d2_postgres.py` started locally but did
  not complete in this environment before the checkpoint was paused, so
  PostgreSQL-backed validation remains unresolved here.

Exact next recommended action:

Commit the E2 checkpoint, push it on `agent/phase-e-completeness-aware-failover`,
and update draft PR #122 with the E2 summary and the PostgreSQL environment
limitation.

## E3 checkpoint update

The current task moved on from the partial E3 draft by resetting to the clean
E2 head and implementing a coherent E3 report path on top of it:

- `catalog/federation/reporting.py` defines authenticated replica report and
  assessment contracts with canonical serialization and deterministic hashes.
- `catalog/federation/phase_d_control.py` persists the latest accepted report
  and assessment per `(session_id, group_id, provider_id)` and re-evaluates
  eligibility from authoritative manifests.
- `catalog/federation/phase_d_service.py` accepts `storage.health` as the
  relay-facing report submission path and binds the authenticated sender to the
  reported provider identity.
- `catalog/federation/tests/test_phase_e3_reports.py` covers canonical
  serialization, complete and empty reports, mismatch rejection, stale and
  conflicting revisions, and authenticated relay submission.

Exact validation for the E3 checkpoint so far:

- `47 passed in 8.52s` for the focused E0/E1/E2/E3 suite.
- `64 passed in 3.61s` for `catalog/federation/tests/test_phase0.py` and
  `catalog/federation/tests/test_phase1.py`.
- `43 passed in 7.83s` for `catalog/federation/tests/test_phase2_unit.py`,
  `catalog/federation/tests/test_phase_d6_replication.py`, and
  `catalog/federation/tests/test_local_storage.py`.
- `All checks passed!` for Ruff on the changed Python files.
- `python -m compileall -q catalog setup_msh.py` passed.

Known limitations:

- The E3 report tests are intentionally focused on the report/assessment
  boundary and do not yet introduce E4 selection logic.
- I did not run the PostgreSQL-specific regression file in this checkpoint
  because the local environment still does not provide a disposable PostgreSQL
  service.
- PR description updates were not possible from this environment because the
  GitHub CLI is unavailable.

Exact next recommended action:

Commit and push the E3 checkpoint with the specified message, then continue to
E4 only after the remote head is confirmed green.

## E3 checkpoint record

- Exact base commit: `357200d76f4a2441e7e505c76b6c94fbccdfc12a`
- Current branch: `agent/phase-e-completeness-aware-failover`
- Draft PR: #122, `Implement Phase E completeness-aware failover`
- Completed checkpoint: E3
- Current checkpoint: E3 is complete and the branch is ready to be pushed as a
  green checkpoint once commit/push finishes.
- Files changed:
  - `catalog/federation/reporting.py`
  - `catalog/federation/phase_d_control.py`
  - `catalog/federation/phase_d_service.py`
  - `catalog/federation/__init__.py`
  - `catalog/federation/tests/test_phase_e3_reports.py`
  - `docs/implementation/phase_e_progress.md`
- Tests run and exact results:
  - `python -m compileall -q catalog setup_msh.py` — passed
  - `pytest -q catalog/federation/tests/test_phase_e0_contracts.py catalog/federation/tests/test_phase_e1_manifest_store.py catalog/federation/tests/test_phase_e1_service_manifest.py catalog/federation/tests/test_phase_e2_watermarks.py catalog/federation/tests/test_phase_e3_reports.py` — `47 passed in 8.52s`
  - `pytest -q catalog/federation/tests/test_phase0.py catalog/federation/tests/test_phase1.py` — passed
  - `pytest -q catalog/federation/tests/test_phase2_unit.py catalog/federation/tests/test_phase_d6_replication.py catalog/federation/tests/test_local_storage.py` — passed
  - `ruff check catalog/federation` — passed after auto-fix
- Known failures / limitations:
  - `gh` is not installed in this environment, so the draft PR description could not be updated from here.
  - No PostgreSQL-backed suite was run in this checkpoint.
- Exact next recommended action:
  - Commit and push the E3 checkpoint, then update the PR description from a machine with GitHub CLI available.
- Safe to resume from current branch head:
  - yes, once the E3 commit is created and pushed.

## Push status after checkpoint commit

The E3 checkpoint commit was created locally as `a905461` with message
`Report replica completeness and eligibility` and is now pushed to
`origin/agent/phase-e-completeness-aware-failover`.

The Git HTTPS helper issue was resolved by pointing `GIT_EXEC_PATH` at the
bundled Git `bin` directory so `git-remote-https.exe` could be found. The
branch is ready for remote verification and CI follow-up.

## E4 checkpoint record

- Exact base commit: `8a6a23c` for the E4 worktree state; the remote branch
  already contains the E3 checkpoint `a905461` and the doc-only handoff commit
  `8a6a23c`.
- Current branch: `agent/phase-e-completeness-aware-failover`
- Draft PR: #122, `Implement Phase E completeness-aware failover`
- Completed checkpoint: E4
- Current checkpoint: E4 is complete and validated locally.
- Files changed:
  - `catalog/federation/selection.py`
  - `catalog/federation/__init__.py`
  - `catalog/federation/tests/test_phase_e4_selection.py`
  - `docs/implementation/phase_e_progress.md`
- Tests run and exact results:
  - `python -m compileall -q catalog setup_msh.py` via `.venv\Scripts\python.exe` — passed
  - `pytest -q catalog/federation/tests/test_phase_e0_contracts.py catalog/federation/tests/test_phase_e1_manifest_store.py catalog/federation/tests/test_phase_e1_service_manifest.py catalog/federation/tests/test_phase_e2_watermarks.py catalog/federation/tests/test_phase_e3_reports.py catalog/federation/tests/test_phase_e4_selection.py` — passed
  - `pytest -q catalog/federation/tests/test_phase0.py catalog/federation/tests/test_phase1.py` — passed
  - `pytest -q catalog/federation/tests/test_phase2_unit.py catalog/federation/tests/test_phase_d6_replication.py catalog/federation/tests/test_local_storage.py` — passed
  - `ruff check catalog/federation/__init__.py catalog/federation/selection.py catalog/federation/tests/test_phase_e4_selection.py catalog/federation/reporting.py` — passed
- Known failures / limitations:
  - None for the E4 slice after the final validation pass.
  - PR and CI verification still require GitHub access from a machine with GitHub CLI or browser access.
- Exact next recommended action:
  - Commit `Select complete storage promotion candidates`, push it, and then verify PR #122 and CI from GitHub.
- Safe to resume from current branch head:
  - yes, after the E4 commit is created and pushed.

## E5 checkpoint record

- Exact base commit: `07055b52e623414ec67c50bdee76bd98a4086c2f` for the E5 worktree state.
- Current branch: `agent/phase-e-completeness-aware-failover`
- Draft PR: #122, `Implement Phase E completeness-aware failover`
- Completed checkpoint: E5
- Current checkpoint: E5 is complete and validated locally.
- Files changed:
  - `catalog/federation/phase_d_control.py`
  - `catalog/federation/selection.py`
  - `catalog/federation/tests/test_phase_e4_selection.py`
  - `catalog/federation/tests/test_phase_e5_degraded_state.py`
  - `docs/implementation/phase_e_progress.md`
- Tests run and exact results:
  - `python -m compileall -q catalog setup_msh.py` via `.venv\Scripts\python.exe` — passed
  - `pytest -q catalog/federation/tests/test_phase_e0_contracts.py catalog/federation/tests/test_phase_e1_manifest_store.py catalog/federation/tests/test_phase_e1_service_manifest.py catalog/federation/tests/test_phase_e2_watermarks.py catalog/federation/tests/test_phase_e3_reports.py catalog/federation/tests/test_phase_e4_selection.py catalog/federation/tests/test_phase_e5_degraded_state.py` — passed
  - `pytest -q catalog/federation/tests/test_phase0.py catalog/federation/tests/test_phase1.py` — passed
  - `pytest -q catalog/federation/tests/test_phase2_unit.py catalog/federation/tests/test_phase_d6_replication.py catalog/federation/tests/test_local_storage.py` — passed
  - `ruff check catalog/federation/__init__.py catalog/federation/selection.py catalog/federation/phase_d_control.py catalog/federation/tests/test_phase_e4_selection.py catalog/federation/tests/test_phase_e5_degraded_state.py` — passed
- Known failures / limitations:
  - None for the E5 slice after the final validation pass.
  - PR and CI verification still require GitHub access from a machine with GitHub CLI or browser access.
- Remote push status:
  - E5 commit `231d39f` is pushed to `origin/agent/phase-e-completeness-aware-failover`.
  - GitHub Actions / PR status were not directly inspectable from this environment.
- Exact next recommended action:
  - Commit `Add safe storage degraded state`, push it, and then verify PR #122 and CI from GitHub.
- Safe to resume from current branch head:
  - yes, after the E5 commit is created and pushed.

## E6 status

E6 design has been approved, and E6.1 is now in progress. The promotion
transaction requirements are documented in
`docs/implementation/phase_e6_promotion_transaction_design.md`.

E6 runtime implementation should proceed only through the approved staged
checkpoints.

## E6.1 checkpoint record

- Exact base commit: `c296b0f` for the E6.1 worktree state before the design
  checkpoint and the current E6.1 edits.
- Current branch: `agent/phase-e-completeness-aware-failover`
- Draft PR: #122, `Implement Phase E completeness-aware failover`
- Completed checkpoint: E6.1
- Current checkpoint: E6.1 transaction model and persistence are complete and
  validated locally.
- Files changed:
  - `catalog/federation/promotion_transaction.py`
  - `catalog/federation/phase_d_control.py`
  - `catalog/federation/tests/test_phase_e6_promotion_transaction.py`
  - `docs/implementation/phase_e_progress.md`
- Tests run and exact results:
  - `python -m compileall -q catalog setup_msh.py` via `.venv\Scripts\python.exe` — passed
  - `pytest -q catalog/federation/tests/test_phase_e0_contracts.py catalog/federation/tests/test_phase_e1_manifest_store.py catalog/federation/tests/test_phase_e1_service_manifest.py catalog/federation/tests/test_phase_e2_watermarks.py catalog/federation/tests/test_phase_e3_reports.py catalog/federation/tests/test_phase_e4_selection.py catalog/federation/tests/test_phase_e5_degraded_state.py catalog/federation/tests/test_phase_e6_promotion_transaction.py` — passed
  - `pytest -q catalog/federation/tests/test_phase0.py catalog/federation/tests/test_phase1.py` — passed
  - `pytest -q catalog/federation/tests/test_phase2_unit.py catalog/federation/tests/test_phase_d6_replication.py catalog/federation/tests/test_local_storage.py` — passed
  - `ruff check catalog/federation/phase_d_control.py catalog/federation/promotion_transaction.py catalog/federation/tests/test_phase_e6_promotion_transaction.py` — passed
- Known failures / limitations:
  - None for the E6.1 slice after the final validation pass.
  - The coordinator promotion logic itself is not yet implemented; only the
    durable transaction model and persistence surface exist at this stage.
- Remote push status:
  - Pending for the E6.1 checkpoint.
- Exact next recommended action:
  - Commit `Add coordinator controlled storage promotion` for the E6.1
    persistence checkpoint, push it, and then continue with E6.2 validation
    and idempotent command creation.
- Safe to resume from current branch head:
  - yes, after the E6.1 commit is created and pushed.

## E6.2 checkpoint record

- Exact base commit: `d30367e` for the E6.2 worktree state before this
  checkpoint is committed.
- Current branch: `agent/phase-e-completeness-aware-failover`
- Draft PR: #122, `Implement Phase E completeness-aware failover`
- Completed checkpoint: E6.2
- Current checkpoint: E6.2 validation and idempotent command creation are
  complete and validated locally.
- Files changed:
  - `catalog/federation/phase_d_control.py`
  - `catalog/federation/tests/test_phase_e6_promotion_transaction.py`
  - `docs/implementation/phase_e_progress.md`
- Tests run and exact results:
  - `python -m compileall -q catalog setup_msh.py` via `.venv\Scripts\python.exe` — passed
  - `pytest -q catalog/federation/tests/test_phase_e6_promotion_transaction.py` — passed
  - `pytest -q catalog/federation/tests/test_phase_e0_contracts.py catalog/federation/tests/test_phase_e1_manifest_store.py catalog/federation/tests/test_phase_e1_service_manifest.py catalog/federation/tests/test_phase_e2_watermarks.py catalog/federation/tests/test_phase_e3_reports.py catalog/federation/tests/test_phase_e4_selection.py catalog/federation/tests/test_phase_e5_degraded_state.py catalog/federation/tests/test_phase_e6_promotion_transaction.py` — passed
  - `pytest -q catalog/federation/tests/test_phase0.py catalog/federation/tests/test_phase1.py` — passed
  - `pytest -q catalog/federation/tests/test_phase2_unit.py catalog/federation/tests/test_phase_d6_replication.py catalog/federation/tests/test_local_storage.py` — passed
  - `ruff check catalog/federation/phase_d_control.py catalog/federation/promotion_transaction.py catalog/federation/tests/test_phase_e6_promotion_transaction.py` — passed
- Known failures / limitations:
  - None for the E6.2 slice after the final validation pass.
  - This checkpoint does not yet fence or grant authority; that belongs to E6.3+.
- Remote push status:
  - Pending for the E6.2 checkpoint.
- Exact next recommended action:
  - Commit `Add coordinator controlled storage promotion` for the E6.2 validation checkpoint, push it, and then proceed to E6.3 old-authority fencing.
- Safe to resume from current branch head:
  - yes, after the E6.2 commit is created and pushed.

## E6.3 checkpoint record

- Phase E objective:
  - Promote storage authority only from complete, authenticated evidence while
    durably fencing obsolete writers and preserving manifest and recovery state.
- Exact base commit: `9dd9b26bbc432c07106052887f5c15dc12dd9e42`.
- Current branch: `agent/phase-e-completeness-aware-failover`.
- Draft PR: #122, `Implement Phase E completeness-aware failover`.
- Completed checkpoints: E0, E1, E2, E3, E4, E5, E6 design, E6.1, E6.2,
  and E6.3.
- Current checkpoint:
  - E6.3 provider-enforced old-authority fencing is complete, validated, and
    pushed as `7ecc60f3cabfc7750cfa0a8451a4a55406044ca7`.
- Remaining checkpoints:
  - E6.4 durable term reservation and idempotent new grant.
  - E6.5 finalization and degraded-state exit.
  - E6.6 full recovery and failure reconciliation.
  - E7 and E8 remain unstarted.
- Architectural decisions:
  - Provider fencing is stored in a provider-local SQLite ledger using
    `PRAGMA synchronous=FULL`; the provider acknowledges only after the
    transaction commits.
  - A fence command is derived only from the durable validated promotion
    transaction. Its stable idempotency key binds promotion, session, group,
    previous provider, and previous term.
  - The fencing high-water term equals the previous authority term for E6.3.
    Provider grant validation rejects that term and every lower term.
  - Coordinator command emission is not evidence. Coordinator advancement
    requires authenticated sender identity, exact command/acknowledgement
    binding, and a matching provider-local durable record.
  - Fencing delivery failure remains retryable at the `validated` transaction
    stage and does not clear `storage-degraded`, allocate a term, or create a
    grant.
- Files changed:
  - `catalog/federation/provider_fencing.py`
  - `catalog/federation/phase_d_control.py`
  - `catalog/federation/__init__.py`
  - `catalog/federation/tests/test_phase_e63_provider_fencing.py`
  - `docs/implementation/phase_e_progress.md`
- Exact provider-side fencing state:
  - session ID, storage group ID, provider ID, promotion ID, previous term,
    fencing high-water term, stable idempotency key, deterministic
    acknowledgement identity, and durable persistence timestamp.
- Acknowledgement validation:
  - promotion, session, group, provider, previous term, fencing high-water
    term, idempotency key, and acknowledgement identity must exactly match the
    command derived from the persisted transaction;
  - the authenticated node must own the previous provider;
  - the provider-local durable acknowledgement must equal the received
    acknowledgement;
  - stale, mismatched, unauthenticated, missing, and corrupt evidence fails
    closed without advancing the transaction.
- Tests run and exact results:
  - `.venv\Scripts\python.exe -m compileall -q catalog setup_msh.py` — passed.
  - `.venv\Scripts\python.exe -m pytest -q catalog/federation/tests/test_phase_e6_promotion_transaction.py catalog/federation/tests/test_phase_e63_provider_fencing.py`
    — 21 passed.
  - `.venv\Scripts\python.exe -m pytest -q catalog/federation/tests/test_phase_e0_contracts.py catalog/federation/tests/test_phase_e1_manifest_store.py catalog/federation/tests/test_phase_e1_service_manifest.py catalog/federation/tests/test_phase_e2_watermarks.py catalog/federation/tests/test_phase_e3_reports.py catalog/federation/tests/test_phase_e4_selection.py catalog/federation/tests/test_phase_e5_degraded_state.py catalog/federation/tests/test_phase_e6_promotion_transaction.py catalog/federation/tests/test_phase_e63_provider_fencing.py`
    — 76 passed.
  - `.venv\Scripts\python.exe -m pytest -q catalog/federation/tests/test_phase0.py catalog/federation/tests/test_phase1.py`
    — 64 passed.
  - `.venv\Scripts\python.exe -m pytest -q catalog/federation/tests/test_phase2_unit.py catalog/federation/tests/test_phase_d6_replication.py catalog/federation/tests/test_local_storage.py`
    — 43 passed.
  - `.venv\Scripts\python.exe -m ruff check catalog/federation/__init__.py catalog/federation/phase_d_control.py catalog/federation/provider_fencing.py catalog/federation/tests/test_phase_e6_promotion_transaction.py catalog/federation/tests/test_phase_e63_provider_fencing.py`
    — passed.
  - `git diff --check` — passed; Git reported only expected LF-to-CRLF
    working-copy notices.
- Known failures:
  - None in the required local E6.3 validation.
  - GitHub Actions is pending for the latest branch head; the run attached
    directly to `7ecc60f` was cancelled when the handoff-only follow-up commit
    superseded it. No remote test failure is known.
- Unresolved questions:
  - None for E6.3. E6.4 must consume the durable fenced stage without weakening
    provider-local high-water enforcement.
- Exact next recommended action:
  - Wait for the latest branch-head GitHub Actions run to finish, review E6.3,
    and only then explicitly approve E6.4.
- Safe to resume from current branch head:
  - yes; E6.3 is pushed and no E6.4 runtime work is present.

## E6.4 checkpoint record

- Phase E objective:
  - Promote storage authority only from complete, authenticated evidence while
    durably fencing obsolete writers, allocating monotonic terms, and
    preserving manifest and recovery state.
- Exact base commit: `c9bf9708c94ccfca1aa0c5cbbbdd5c0a30f55337`.
- Current branch: `agent/phase-e-completeness-aware-failover`.
- Draft PR: #122, `Implement Phase E completeness-aware failover`.
- Completed checkpoints: E0, E1, E2, E3, E4, E5, E6 design, E6.1, E6.2,
  E6.3, and E6.4.
- Current checkpoint:
  - E6.4 strictly increasing term reservation and idempotent provider-local
    new-authority grant are complete and validated locally. Commit and push
    remain for publication.
- Remaining checkpoints:
  - E6.5 finalization and degraded-state exit.
  - E6.6 full recovery and failure reconciliation.
  - E7 and E8 remain unstarted.
- Architectural decisions:
  - Term reservation is a coordinator SQLite `BEGIN IMMEDIATE` transaction
    that updates the promotion record and the group term counter together.
  - The reserved term is one greater than the maximum previous authority term,
    provider fencing high-water term, durable counter, all promotion
    reservations, and all observed control-plane grant events for the group.
  - Duplicate or restarted reservation reuses the immutable reserved term and
    stable grant ID; a promotion never allocates a second term.
  - Grant identity and idempotency identity bind promotion, session, group,
    selected provider, reserved term, manifest revision/hash, and selected
    report revision/hash.
  - The provider stores one logical grant durably with `synchronous=FULL`
    before acknowledging. Retry and lost-response recovery return the original
    persisted acknowledgement.
  - Provider-local fence and grant ledgers reject fenced, stale, duplicate, and
    conflicting authority. The coordinator advances only after authenticated,
    exact, provider-local durable grant evidence.
  - E6.4 does not modify assignment or coordinator leader-grant events, does
    not clear `storage-degraded`, and does not finalize the promotion.
- Files changed:
  - `catalog/federation/provider_fencing.py`
  - `catalog/federation/phase_d_control.py`
  - `catalog/federation/__init__.py`
  - `catalog/federation/tests/test_phase_e64_authority_grant.py`
  - `docs/implementation/phase_e_progress.md`
- Term-reservation persistence rules:
  - reservation is allowed only from `fenced-old-authority`;
  - the exact provider-local fence record and acknowledgement identity are
    revalidated before allocation and before grant delivery;
  - `reserved_term`, stable `grant_id`, `grant_status=reserved`, and
    `state=allocated-term` commit before any provider grant attempt;
  - corrupt or non-increasing reservations fail closed;
  - all later attempts reuse the original reservation.
- Provider-side durable grant state:
  - session, group, provider, promotion, term, manifest revision/hash, report
    revision/hash, stable grant ID, idempotency key, acknowledgement identity,
    and persistence timestamp.
- Grant acknowledgement validation:
  - the authenticated node must own the selected provider;
  - every immutable command field and acknowledgement identity must match the
    persisted E6 transaction;
  - current authoritative manifest and selected report must still match the
    transaction;
  - provider-local durable evidence must exactly equal the received
    acknowledgement;
  - missing, stale, corrupt, mismatched, conflicting, or unauthenticated
    evidence fails closed.
- Duplicate and restart behavior:
  - reservation survives restart and never allocates a replacement term;
  - duplicate grant delivery returns the same acknowledgement;
  - a lost acknowledgement is recovered from provider-local durable state;
  - restart before delivery, after provider persistence, and after coordinator
    acknowledgement all resume deterministically;
  - retryable delivery failure preserves the reservation, degraded state, and
    recovery obligations.
- Tests run and exact results:
  - `.venv\Scripts\python.exe -m compileall -q catalog setup_msh.py` — passed.
  - `.venv\Scripts\python.exe -m pytest -q catalog/federation/tests/test_phase_e6_promotion_transaction.py catalog/federation/tests/test_phase_e63_provider_fencing.py catalog/federation/tests/test_phase_e64_authority_grant.py`
    — 47 passed.
  - `.venv\Scripts\python.exe -m pytest -q catalog/federation/tests/test_phase_e0_contracts.py catalog/federation/tests/test_phase_e1_manifest_store.py catalog/federation/tests/test_phase_e1_service_manifest.py catalog/federation/tests/test_phase_e2_watermarks.py catalog/federation/tests/test_phase_e3_reports.py catalog/federation/tests/test_phase_e4_selection.py catalog/federation/tests/test_phase_e5_degraded_state.py catalog/federation/tests/test_phase_e6_promotion_transaction.py catalog/federation/tests/test_phase_e63_provider_fencing.py catalog/federation/tests/test_phase_e64_authority_grant.py`
    — 102 passed.
  - `.venv\Scripts\python.exe -m pytest -q catalog/federation/tests/test_phase0.py catalog/federation/tests/test_phase1.py`
    — 64 passed.
  - `.venv\Scripts\python.exe -m pytest -q catalog/federation/tests/test_phase2_unit.py catalog/federation/tests/test_phase_d6_replication.py catalog/federation/tests/test_local_storage.py`
    — 43 passed.
  - `.venv\Scripts\python.exe -m ruff check catalog/federation/__init__.py catalog/federation/phase_d_control.py catalog/federation/provider_fencing.py catalog/federation/tests/test_phase_e6_promotion_transaction.py catalog/federation/tests/test_phase_e63_provider_fencing.py catalog/federation/tests/test_phase_e64_authority_grant.py`
    — passed.
  - `git diff --check` — passed; only expected LF-to-CRLF working-copy
    notices were emitted.
- Known failures:
  - None in the required local E6.4 validation.
- Unresolved questions:
  - None for E6.4. E6.5 must finalize the already acknowledged logical grant
    without issuing a second grant or changing its reserved term.
- Exact next recommended action:
  - Commit as `Add idempotent storage authority grant`, push immediately,
    update PR #122, verify GitHub Actions, and wait for explicit E6.5 approval.
- Safe to resume from current branch head:
  - yes after the E6.4 commit is pushed; no E6.5 runtime behavior is present.
