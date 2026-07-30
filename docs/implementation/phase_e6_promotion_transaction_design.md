# Phase E6 promotion transaction design

Date: 2026-07-29

Branch: `agent/phase-e-completeness-aware-failover`

Checkpoint status: design only, implementation paused pending approval.

## Purpose

E6 needs a coordinator-controlled promotion transaction that is safe across
restart, duplicate delivery, partial failure, and manifest drift. The current
Phase D runtime can already:

- store authoritative manifests;
- report replica completeness and eligibility;
- choose a deterministic complete candidate; and
- persist a `storage-degraded` state.

What it does not yet have is a durable, restart-safe promotion transaction
state machine that can fence the previous writer, allocate a new term, grant the
new writer, and survive interruption at each stage without ever creating two
valid primaries at once.

This document describes the minimum architecture needed before E6 runtime code
should be resumed.

## 1. Limitations in the current `phase_d_control.py` architecture

The current control-plane facade is not yet structured for a safe promotion
transaction because:

1. It is event-log oriented for assignment and grants, but not transaction
   oriented for a multi-stage promotion.
2. `grant_leader()` and `revoke_leader()` are independent event appends; they do
   not form a durable transaction boundary with a visible in-progress state.
3. `complete_handover()` is designed for the older controlled-handover path and
   assumes the target is already operationally ready, not a degraded-state
   recovery path.
4. The current control plane has no canonical persisted promotion record that
   stores:
   - the exact selected candidate report;
   - the authoritative manifest hash and revision at selection time;
   - the transaction stage;
   - the fencing / grant outcomes for recovery.
5. There is no restart-safe replay logic for a partially completed promotion.
6. There is no single persisted source of truth that can answer:
   - “was old authority already fenced?”
   - “was the new term already allocated?”
   - “was the new grant already issued?”
   - “was the transaction finalized or only prepared?”
7. Failures currently surface as exceptions, but not as durable transaction
   states that can be resumed idempotently.
8. The promotion path is not yet isolated from unrelated snapshot changes during
   a restart window.
9. There is no persistent proof that the selected candidate report still matches
   the manifest state used for selection.
10. `storage-degraded` is persisted separately, but promotion does not yet
    atomically transition out of degraded mode only after the candidate is fully
    promoted.

## 2. Minimum architectural changes required

The smallest safe design is a dedicated, durable promotion transaction state
machine with explicit stages.

Required stages:

1. `pending`
2. `validated`
3. `fenced-old-authority`
4. `allocated-term`
5. `granted-new-authority`
6. `finalized`
7. `failed`

The transaction must persist enough data to resume each stage idempotently:

- session_id
- group_id
- promotion_id
- selected provider_id
- exact report JSON or canonical report hash
- exact manifest revision and manifest hash
- selected candidate eligibility result and reason
- old grant identity, if present
- new grant identity
- allocated term
- fencing token
- stage state
- failure code / reason
- timestamps

The transaction should be advanced only in the following order:

1. validate selected candidate and manifest binding;
2. persist pending transaction;
3. fence or revoke old authority;
4. allocate strictly increasing term;
5. grant new authority;
6. finalize and clear degraded state.

The transaction must be restart-safe after every stage:

- if it crashes before fencing, it should resume by revalidating the persisted
  selection and continue;
- if it crashes after fencing but before grant, it should resume by issuing the
  new grant exactly once;
- if it crashes after grant but before finalization, it should resume by
  finalizing without issuing a second grant;
- if it crashes after finalization, it should be idempotent and return the
  completed result.

The transaction must fail closed:

- stale report or manifest mismatch rejects promotion;
- stale term rejects promotion;
- any manifest drift detected during the transaction rejects or aborts the
  transaction;
- if fencing or grant delivery fails unrecoverably, the group remains or returns
  to `storage-degraded`.

## 3. Components and persisted data structures that must change

The following components need to change for the real E6 runtime implementation:

- `catalog/federation/phase_d_control.py`
  - should expose a coordinator transaction API;
  - should likely delegate stage persistence to a dedicated helper or module;
  - should provide durable read APIs for promotion state and degraded state.
- `catalog/federation/storage_control_plane.py`
  - may need a small extension if coordinator state needs a persistent grant /
    revocation record beyond the current event log.
- `catalog/federation/reporting.py`
  - may need a compact canonical report identity helper if the promotion state
    needs a stable persisted digest.
- `catalog/federation/selection.py`
  - should remain pure selection logic;
  - may need to emit a richer selection record that includes the manifest hash
    and report hash used for the decision.
- `catalog/federation/phase_d_handover.py`
  - should not own E6 promotion, but may need to coexist with it if tests use
    older handover paths.
- `docs/implementation/phase_e_progress.md`
  - must track the new paused state and the eventual staged E6 plan.

Persisted data structures that must likely be added or expanded:

- `storage_promotion_transactions`
  - durable promotion state machine record;
  - one row per promotion command id.
- `storage_promotion_events` or stage fields
  - optional if stage history needs to be auditable separately;
  - not strictly required if the stage fields and updated_at are sufficient.
- `storage_degraded_states`
  - should remain the degraded authority record;
  - may need a reference to the active or last attempted promotion id.
- `storage_fencing_counters`
  - may remain as-is if promotion always uses monotonic maxima from the current
    control plane;
  - may need an explicit reservation record if allocation has to be retried
    after restart without reusing a term.

## 4. Where the promotion logic should live

Recommendation: move the transaction logic into a dedicated module rather than
keeping the whole state machine inside `phase_d_control.py`.

Reason:

- `phase_d_control.py` is already the coordination facade for control state,
  manifest access, grants, and degraded-state persistence.
- E6 adds a multi-stage process with replay and recovery semantics that is
  easier to test and reason about as a separate transaction module.
- A dedicated module can stay focused on:
  - stage transitions;
  - idempotent recovery;
  - persisted transaction records;
  - manifest/report binding;
  - promotion result decoding.

Proposed split:

- `phase_d_control.py`
  - owns the coordinator API;
  - exposes `promote_storage_candidate(...)` as a thin orchestration entry
    point;
  - delegates stage management and persistence to a promotion-transaction
    helper.
- `promotion_transaction.py` or similar
  - owns the transaction state model;
  - owns transition validation;
  - owns resume logic;
  - owns serialization and recovery helpers.

This split is optional for the final runtime, but it is the safer design
boundary for E6.

## 5. Migration and compatibility implications for E0–E5 state

E6 must not invalidate the existing E0–E5 persistent state.

Compatibility rules:

1. Existing authoritative manifests remain untouched.
2. Existing replica reports remain untouched and continue to be readable.
3. Existing degraded-state rows remain valid and must be preserved until a
   promotion successfully finalizes.
4. Existing grant and fencing counter state remains the source for strictly
   increasing term allocation.
5. Promotion transaction tables must be additive and migration-safe.
6. On startup, absent promotion rows must be interpreted as “no active
   promotion”.
7. Corrupt promotion rows must fail closed rather than auto-repairing into
   authority.
8. Existing E0–E5 tests must keep passing unchanged except where E6 introduces
   explicit new states or APIs.

Migration approach:

- add new tables with `CREATE TABLE IF NOT EXISTS`;
- do not rewrite existing manifest or report tables;
- read-only code paths must tolerate the absence of promotion rows;
- promotion recovery must reconcile against the manifest head and degraded state
  already persisted by E5.

## 6. Staged implementation plan for E6

Stage E6.1 — transaction model and persistence

- Add a dedicated promotion transaction record with explicit stages.
- Add read APIs for the transaction state.
- Keep all existing runtime behavior unchanged.

Risks:

- schema drift;
- accidental coupling with existing grant logic.

Invariants:

- no promotion is attempted yet;
- no write authority is changed yet.

Stage E6.2 — validation and command idempotency

- Implement validation of selected candidate, manifest revision/hash, report
  hash, and group state before any fencing step.
- Make repeated commands with the same transaction id idempotent.
- Reject stale or conflicting commands.

Risks:

- false acceptance of stale manifest/report data;
- duplicate command ambiguity.

Invariants:

- a stale or altered candidate never advances.

Stage E6.3 — old-authority fencing

- Persist that the old authority has been fenced or revoked before the new
  grant can be issued.
- Ensure the stage is recoverable after restart.

Risks:

- a failed fence that is mistaken for success;
- transient split-brain during recovery.

Invariants:

- once the stage is durable, the old grant is no longer valid.

Stage E6.4 — new term allocation and grant issuance

- Allocate a strictly increasing term.
- Persist the term before or with the new grant.
- Issue the new grant once and only once.

Risks:

- term reuse;
- duplicate grant delivery;
- granting without recorded fencing.

Invariants:

- terms increase monotonically;
- the new grant never coexists with a still-valid old grant.

Stage E6.5 — finalization and degraded-state exit

- Mark the transaction completed only after the new authority is durable.
- Clear degraded state only after successful finalization.
- Preserve manifest state and recovery obligations on failure.

Risks:

- clearing degraded state too early;
- hiding a partially completed promotion.

Invariants:

- if finalization fails, the group remains safely degraded.

Stage E6.6 — restart recovery and duplicate reconciliation

- Resume pending, fenced, granted, or finalization-pending transactions.
- Reconcile duplicates and return the existing durable result.
- Fail closed on corrupt transaction rows.

Risks:

- inconsistent replay of partial state;
- ambiguous resume from a malformed row.

Invariants:

- recovery is deterministic and idempotent.

## 7. Risks, invariants, and failure modes by stage

Validation stage

- Risk: candidate report has gone stale after selection.
- Failure mode: reject and remain degraded.
- Invariant: transaction cannot start from mismatched manifest/report state.

Fencing stage

- Risk: old grant is not actually revoked.
- Failure mode: abort and remain degraded.
- Invariant: no path to a completed promotion without durable old-authority
  revocation.

Term allocation stage

- Risk: term number is not strictly higher than the previous highest term.
- Failure mode: reject and remain degraded.
- Invariant: each promotion attempt gets a monotonic term.

Grant stage

- Risk: grant delivery fails or is duplicated.
- Failure mode: retry idempotently if safe; otherwise fail closed and remain
  degraded.
- Invariant: only one valid primary authority exists at a time.

Finalization stage

- Risk: state is partially written and restart occurs.
- Failure mode: resume from the persisted stage.
- Invariant: completion is only recorded after all prior stages are durable.

Recovery stage

- Risk: corrupt persisted promotion row.
- Failure mode: fail closed and keep the group degraded.
- Invariant: corrupted state must never be auto-promoted.

## 8. Recommendation

The current scaffold shows that E6 cannot be safely completed as a small local
patch to `phase_d_control.py` alone. The minimum safe path is:

1. introduce a dedicated promotion transaction module;
2. add durable transaction-stage persistence;
3. make the control plane delegate to that module;
4. write restart/duplicate/failure tests around the stage model;
5. only then resume the runtime implementation.

Until that design is approved, E6 implementation should remain paused.
