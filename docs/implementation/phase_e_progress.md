# Phase E completeness-aware failover progress

Last updated: 2026-07-31

## Status

Phase E implementation is complete on branch `agent/complete-phase-e-recovery` and is awaiting the final Linux/Windows CI gate and merge to `main`.

The implementation remains relay-first. Phase F direct peer transport and Phase G capability scheduling are explicitly excluded.

## Checkpoints

| Checkpoint | Status | Scope |
| --- | --- | --- |
| E0 | Completed | Contracts and acceptance traceability |
| E1 | Completed | Authoritative manifests and commit integration |
| E2 | Completed | Watermarks, missing ranges, conflicts, and restart persistence |
| E3 | Completed | Authenticated replica reports and coordinator eligibility |
| E4 | Completed | Deterministic completeness-aware selection |
| E5 | Completed | Durable `storage-degraded` behavior |
| E6 | Completed | Fencing, promotion, finalization, recovery, and coordinator publication |
| E7.0-E7.1 | Completed | Durable former-primary recovery planning and restart replay |
| E7.2 | Implemented; CI pending | Relay-first missing-item delivery and lost-response reconciliation |
| E7.3 | Implemented; CI pending | Conflict, corruption, stale-authority, and source/target mismatch handling |
| E7.4 | Implemented; CI pending | New authenticated report and exact coordinator eligibility gate |
| E7.5 | Implemented; CI pending | Restart, duplicate execution, and per-group isolation |
| E7.6 | Implemented; CI pending | End-to-end former-primary recovery acceptance |
| E8 | Implemented; CI pending | Operator diagnostics and final regression matrix |

## Completed recovery flow

1. The coordinator promotes a complete verified replica and durably fences the former primary.
2. The returning provider is assigned only as a replica and its old fence remains active.
3. A durable recovery plan binds the promotion, current authority, manifest, provider report, and item ledger.
4. Missing authoritative batches are read from the current primary, hash-verified, and delivered through the existing authenticated `storage-replication` relay path.
5. A lost response is reconciled using the target provider's immutable committed identity; the same logical item is never created twice.
6. Source corruption, target conflicts, stale grants, missing fencing, unknown extra hashes, and malformed durable state fail closed with durable diagnostics.
7. Every authoritative item is independently verified on the returning provider.
8. The provider submits a newer authenticated report for the exact manifest.
9. Completion is persisted only when the coordinator accepts the report as `eligible`, with exact hashes, schemas, datasets, watermarks, ranges, integrity, and `synchronized` state.
10. Recovery never grants leadership. The provider remains a replica that is merely eligible for a future coordinator-controlled selection.

## Acceptance coverage

The final tests cover:

- F0-012, F0-015, and F0-021;
- F3-007 through F3-010;
- F4-003, F4-004, F4-007, and F4-008;
- successful relay-first repair;
- retry and restart after relay unavailability;
- lost-response reconciliation without resend;
- duplicate execution;
- changed assignment or grant;
- missing former-primary fence;
- missing or conflicting source and target identities;
- forged or ineligible reports;
- manifest advancement during recovery;
- final synchronized eligibility;
- per-group independence;
- E8 diagnostics for role, current primary, manifest/report revisions, integrity, synchronization, eligibility, item state, degraded state, and errors.

## Final completion gate

Phase E may be declared complete only after all of the following are true:

- the Phase D/E regression matrix passes on Linux;
- the expanded Windows federation matrix passes;
- compilation, Ruff, Compose validation, and diff hygiene pass;
- the pull request is squash-merged to `main`;
- the temporary branch is deleted.

Until that gate is satisfied, the implementation status is **complete but not yet accepted**.

## Next phase

After Phase E is accepted and cleaned up, begin exactly one new branch for the next selected phase. Do not keep this branch open while Phase F or Phase G work starts.
