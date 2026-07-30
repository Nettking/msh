# Phase E6 independent completion review

Date: 2026-07-30

## Verdict

**E6 is complete.**

The coordinator-controlled promotion transaction now has one interruption-safe end-to-end recovery path that fences obsolete authority, reserves one monotonic term, durably grants the selected provider, persists immutable finalization proof, publishes the new coordinator assignment and leader grant atomically, and clears only the matching `storage-degraded` state.

This review supersedes the stale E6 status lines near the beginning of `phase_e_progress.md` and the pre-implementation status header in `phase_e6_promotion_transaction_design.md`. Those files remain useful historical checkpoint records; this document is authoritative for E6 closure.

## Independent review scope

The review inspected:

- promotion transaction persistence and immutable bindings;
- provider-local old-authority fencing;
- monotonic term reservation and provider-local grant;
- durable finalization and degraded-state fingerprinting;
- restart and duplicate recovery;
- coordinator event publication and logical routing state;
- package/API call surface;
- Linux and Windows CI coverage;
- E6 acceptance requirements and negative cases.

## Defects found during completion review

### 1. Coordinator publication was missing

E6.6 could persist provider-local authority and finalization, then clear degraded state without appending the coordinator events that make the selected provider the active primary. Logical routing and write-authority validation could therefore continue to expose the old assignment/grant.

The repair in `672245313d4411d078ed4dc5150dba8415cc01e5` added a durable, idempotent publication stage that atomically:

1. revokes the old coordinator grant;
2. changes the assignment to the selected provider;
3. places the former primary once in the replica set;
4. grants the selected provider the already-reserved term;
5. allocates the next coordinator fencing token;
6. persists the exact publication proof and resulting control revision.

Degraded state is cleared only after this publication is present and consistent with the reconstructed coordinator snapshot.

### 2. Remote CI did not run E2-E6

The workflow previously stopped at E1. The Linux and Windows matrices now execute all E2-E6 tests, including the coordinator-publication tests.

## Authoritative completion path

The supported end-to-end entry point is:

```python
PhaseDControlPlane.recover_promotion(...)
```

It is also exported as `recover_storage_promotion` from `catalog.federation`.

`complete_promotion_finalization()` is a lower-level compatibility/stage primitive retained for focused restart-boundary tests and historical state reconciliation. It is not exported as the Phase E completion API. A pre-E6.7 database that was cleared through that lower-level primitive is repaired by the authoritative recovery entry point, which detects the missing publication and publishes the coordinator state idempotently.

The aggregate PR diff was audited for direct calls. Outside tests, the only caller is `promotion_recovery.py`, where publication occurs before degraded-state clearing. No active runtime path bypasses the recovery orchestrator.

## Closed invariants

1. **No self-promotion.** The coordinator selects and drives promotion.
2. **Old authority is fenced first.** Provider-local durable high-water state rejects the previous term and every lower term.
3. **One reserved term.** Retries and restarts reuse the same strictly increasing term and grant identity.
4. **Provider proof is required.** Advancement requires authenticated, exact, provider-durable fence and grant acknowledgements.
5. **Finalization is durable before exit.** Immutable authority proof and E7 obligations persist before degraded state can be cleared.
6. **Coordinator state changes atomically.** Revoke, assignment, grant, fencing-token advancement, and publication proof commit together.
7. **Logical routing changes without recorder reconfiguration.** The reconstructed control-plane snapshot points to the promoted provider.
8. **Duplicate and restart recovery converge.** No second term, grant, publication, finalization, or degraded-state clear is produced.
9. **Conflicting or corrupt evidence fails closed.** Recovery returns operator-attention and does not manufacture authority.
10. **Per-group state remains independent.** Promotion evidence and publication are scoped by session, group, and promotion identity.
11. **Manifest authority does not rewind.** Promotion does not reduce manifest revision or alter committed content.
12. **Phase F remains excluded.** E6 is correct over relay-first transport.

## Focused E6.7 acceptance evidence

The new tests verify:

- final coordinator assignment and leader grant;
- former primary retained once as replica;
- term advancement from 5 to 6;
- fencing-token advancement from 9 to 10;
- exact coordinator event counts;
- restart and duplicate idempotency;
- restart after durable finalization but before publication;
- repair of a legacy clear-before-publication state;
- fail-closed behavior when coordinator state no longer matches the validated source authority.

## Validation

GitHub Actions run `30581905829` on commit `672245313d4411d078ed4dc5150dba8415cc01e5` completed successfully on Linux and Windows.

Linux results:

```text
Compilation: passed
Phase 0/1: 64 passed in 0.85s
Expanded Phase D/E storage suite: 217 passed in 7.71s
Phase 2 unit/integration: 85 passed in 9.57s
Ruff: all checks passed
Docker Compose base and relay-dev: passed
Diff hygiene: passed
```

Windows result:

```text
Expanded identity/state/protocol/relay/Phase E test step: passed
Job conclusion: success
```

A later documentation-only branch head was also green in workflow run `30582295842`.

## Remaining work after E6

- E7: returning former-primary recovery and relay-first catch-up.
- E8: operator diagnostics and full end-to-end Phase E acceptance.

PR #122 must remain draft and stacked on PR #121 until the remaining Phase E work is complete and the base is safely reconciled.
