# B03 recorder publication reconciliation — 2026-08-31

Current review baseline: `origin/main` at `63d56ad068301665e19b4fdddb43e163196b2a55`.

PR #384 still has `main` base SHA `ba46294ee47208c21eebad893c40da54b0c76833`.
Since then, `main` advanced only with the repository-hygiene removal of the
isolated `new-stuff` experiments; that change has no path overlap with B03 and does
not invalidate the recorder assumptions. This review keeps the B03 branch isolated
and does not absorb unrelated cleanup. A final merge preflight should still
reconcile the moving base branch before merge.

This note reconciles the stale B03 status in `v1_robustness_reconciliation.md` against current production code and PR #384. It changes no acceptance flag and makes no physical-evidence claim.

## Current property status

| B03 property | Current assessment | Evidence / consequence |
| --- | --- | --- |
| Reconciliation progress is incremental rather than proportional to complete recorder history | **IMPLEMENTED; exact-head CI pending** | PR #384 adds a durable pending-publication frontier written by the recorder transaction. Existing installations pay one explicit, potentially archive-sized legacy migration scan per source/instance/archive alias; that cost is an upgrade/migration cost, not a steady-state claim. After the initialized marker is durable, each ordinary reconciliation pass decodes at most 64 pending records per source while enumerating only frontier names. The consequence regression forbids reopening an already-reconciled manifest or calling the lifetime archive iterator while publishing later evidence. |
| Frontier creation participates in recorder host-resource admission rather than introducing an unadmitted writer | **IMPLEMENTED; exact-head CI pending** | The recorder-side frontier composition extends the existing B01 transaction requirement by a fixed bounded byte/inode allowance before the transaction is admitted. The frontier write occurs while that reservation is already held; it does not open a nested process-resource reservation. Legacy migration metadata uses the shared process-wide admission controller explicitly. |
| A pre-checkpoint discovery record cannot publish uncommitted raw evidence | **IMPLEMENTED; focused consequence test present** | Pending evidence whose `next_sequence` exceeds the committed checkpoint remains in the frontier and is not enqueued. After the checkpoint advances, the same record becomes eligible. |
| Crash between outbox enqueue and frontier retirement is duplicate-safe | **IMPLEMENTED by ordering/idempotency; exact-head CI pending** | A frontier record is retired only after every publication chunk has a durable outbox representation, whether newly created or already present. A crash before retirement leaves the pointer and causes an idempotent replay; a crash before durable outbox representation cannot reach retirement. |
| One malformed/missing/oversized historical item is isolated without permanently blocking later eligible material | **PROVEN in merged publication semantics; retained by the incremental reconciler** | The new reconciler subclasses the existing publication reconciler and reuses its validation, chunking, quarantine, target, and delivery contracts rather than replacing them. During one-time migration, readable items are seeded before an explicit bounded blocked marker records unrepresentable items; later cycles do not repeat the lifetime scan and continue publishing seeded items while exposing the blocked state. |
| Publication-loop database/storage failures are caught at the required-thread boundary and surfaced/retried | **PROVEN** | Native and Flask-side publication-loop failure containment is merged; `sqlite3.Error` is in the cycle retry boundary and cycle health is observable. |
| Backlog catch-up makes forward progress after outage/restart | **PARTIAL / substantially proven** | Existing anti-starvation and restart backlog-first behavior remains. The B03 frontier removes lifetime archive rediscovery from ordinary cycles, but no new quantitative wall-clock catch-up bound is claimed. |
| Durable retirement/frontier/tombstone design exists before terminal identities can be retired | **FRONTIER IMPLEMENTED; terminal-row retention remains separate** | PR #384 adds the missing publication-discovery frontier. Existing durable completed/retired outbox identities remain unchanged; this PR does not delete terminal rows. |
| Session/destination/source duplicate-suppression semantics survive compaction | **PROVEN for existing rows** | Completed and retired payload compaction preserves the identity columns used by duplicate suppression. PR #384 does not weaken these identities. |

## Implemented frontier contract

`RecorderPublicationFrontier` is discovery metadata, not archive authority. Raw MTConnect evidence remains the primary record. Each bounded pending record identifies the canonical source, archive source/alias, agent instance, exact sequence envelope, raw digest, manifest schema, day, and raw/manifest basenames.

Normal publication does not derive an authority frontier from timestamp/day ordering. Sequence remains tied to the recorder agent instance, and archive day is used only to reconstruct the exact already-recorded path. This avoids skipping evidence after timestamp regression or backdating.

The recorder writes the pending record after immutable raw/detailed evidence exists but before the transaction checkpoint is allowed to make that batch publishable. Publication then follows this ordering:

1. enumerate pending discovery for the committed source/instance;
2. reject future-of-checkpoint records without retiring them;
3. reuse existing validation, chunking, quarantine and outbox idempotency semantics;
4. ensure every chunk has a durable outbox representation;
5. retire only the discovery pointer, never recorder evidence.

## Legacy migration

An upgraded source/instance/archive alias with no initialized marker performs one explicit historical archive scan. This scan is intentionally the migration cost and may be proportional to the existing archive; it is not part of the normal steady-state work bound. Every discovered raw reference is idempotently seeded into the bounded frontier through shared host-resource admission.

If the process crashes or admission refuses during migration, the initialized marker remains absent. A later cycle repeats the migration scan and safely overwrites identical pending identities rather than skipping evidence. If the completed scan finds malformed, missing, unsupported, or otherwise unrepresentable legacy items, valid references are retained and an explicit blocked marker records a bounded issue sample; later cycles report that state without rescanning the lifetime archive. Repair requires an operator to resolve the recorded item and rerun the migration path. Once initialized, ordinary cycles do not invoke the lifetime raw iterator for that alias.

## Production composition

The incremental reconciler is composed explicitly at the two production worker construction sites only:

- Flask recorder Federation publication;
- the standalone/headless recorder Federation publication worker.

Legacy/test callers of `RecorderArchiveReconciler` are not globally monkey-patched. This preserves existing diagnostic behavior outside the production composition points while making actual product publication use `IncrementalRecorderArchiveReconciler`.

Production composition is present on PR #384. The temporary one-shot workflow used to make the two surgical large-file edits deleted itself and is not part of the PR diff. A normal user-authored documentation commit follows this note so exact-head repository CI can run without GitHub's `action_required` treatment of the bot-authored composition commit.

## What B03 still does not claim

This implementation does not yet justify deleting completed/retired outbox identity rows. Terminal-row count retention remains a separate design problem: any future deletion must preserve the durable suppression and gap/tombstone semantics currently carried by those rows.

It also does not claim a quantitative catch-up SLA, physical Federation acceptance, or any physical-machine evidence.

## Scope boundary

PR #384 deliberately avoids the active B01 #383 implementation files. The recorder transaction composition only extends the already-installed B01 admission requirement and does not replace B01 policy. B05/B06, ICSE demo, compute-provider, and physical Federation state are out of scope.
