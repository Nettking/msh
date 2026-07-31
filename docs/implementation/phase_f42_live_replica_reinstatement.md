# Phase F4.2 — live former-primary replica reinstatement

F4.2 restores redundancy after the F3 failover and F4.1 data repair. It does not return leadership to the former primary.

## Safety boundary

The returning provider remains unassigned throughout F4.1. F4.2 begins only when the durable F4.1 journal contains a complete synchronized final report and every authoritative batch is marked verified.

The coordinator binds reinstatement to the exact:

- published F3 failover;
- current promoted primary and node identity;
- active grant, term, fencing token, and lease;
- authoritative manifest revision and hash;
- returning provider and stable node identity;
- pre-failover and degraded acknowledgement modes; and
- matching degraded-state record.

Any unrelated assignment, authority, manifest, report, policy, or degraded-state change stops the operation fail-closed.

## Admission sequence

1. Verify the durable F4.1 evidence while the former primary is unassigned.
2. Add the former primary to the replica set while the degraded acknowledgement policy remains active.
3. Publish the signed assignment to the current primary, retained replicas, and returning node.
4. Request a fresh authenticated report after the returning node has applied the replica assignment.
5. Require an accepted, eligible, synchronized report for the current authoritative manifest.
6. Atomically restore the pre-failover acknowledgement policy, remove only the exact matching degraded state, and persist the control-restored journal stage.
7. Publish the final signed control plan and persist completion.

The current promoted provider remains primary and keeps the existing grant, term, and fencing token.

## Concurrent writes and rollback

Writes may continue while the returning node is admitted because acknowledgement policy remains in its degraded mode. If the authoritative manifest advances before the fresh replica report is accepted, F4.2 removes the returning provider from the replica set again and publishes that rollback. The operation returns `retryable`; a new F4.1 pass repairs the tail before another F4.2 attempt.

This avoids declaring redundancy restored from stale evidence.

## Durable recovery

The `storage_live_reinstatements` journal is stored in the authoritative control database. It records:

- immutable failover and F4.1 bindings;
- pre- and post-reinstatement replica sets;
- acknowledgement-policy bindings;
- assignment, rollback, and final control publications;
- accepted final report revision and hash;
- current stage and diagnostics; and
- completion time.

Crash recovery reconciles assignment changes that committed immediately before their journal-stage update. Signed publications are replay-safe and may be resent after restart.

The acknowledgement-policy restoration, exact degraded-state deletion, and `control-restored` journal transition occur in one SQLite `BEGIN IMMEDIATE` transaction.

## Command

Run one catch-up/reinstatement pass from the control-authority node:

```bash
python -m catalog.node.storage_reinstatement \
  --relay-control-database runtime/relay/control.sqlite3 \
  --storage-control-database runtime/storage/control.sqlite3 \
  --failover-database runtime/storage/failover.sqlite3 \
  --catchup-database runtime/storage/catchup.sqlite3 \
  --publication-database runtime/storage/publications.sqlite3 \
  --state-dir runtime/reinstatement-node \
  --relay wss://relay.example.invalid \
  --display-name "Storage recovery authority" \
  --session-id SESSION_ID \
  --group-id storage-main \
  --returning-provider-id provider-primary
```

A `completed` result means the former primary is an eligible assigned replica, the previous acknowledgement mode is active, the matching degraded state is cleared, and final control has been acknowledged. A `retryable` result preserves or restores the safe degraded configuration. `operator-attention` indicates a conflicting or unverifiable binding.

## Explicit exclusions

F4.2 does not:

- promote the returning provider;
- revoke or replace the current primary grant;
- reserve a new term or fencing token;
- split a storage job across nodes; or
- add direct peer-to-peer transport.
