# Phase F4.1: live former-primary catch-up

F4.1 repairs the durable data of a returning former primary after F3 failover.
It does **not** add the provider back as a replica, restore acknowledgement
policy, clear degraded state, or grant leadership. Those control-plane changes
belong to F4.2.

## Safety boundary

The returning provider must be registered and online, but absent from both the
primary and replica assignments. Normal storage writes therefore continue to be
rejected. Recovery ingest is accepted only when all of the following hold:

- the authenticated relay sender is the node's pinned control authority;
- the target provider is still unassigned in the latest signed control state;
- the declared source is the currently assigned primary;
- grant id, term, fencing token, source node, and lease match signed control;
- immutable dataset, schema, batch, idempotency, and content-hash bindings match;
- the payload hashes to the authoritative manifest content hash.

Any identity or content conflict stops fail-closed with operator attention.

## Durable sequence

1. Locate the published F3 failover for the returning provider.
2. Verify that current primary authority still matches that failover.
3. Request an authenticated manifest-bound report from the returning provider.
4. Inspect every authoritative batch and persist a deterministic item ledger.
5. Read only missing batches from the current primary over `fcp-storage-v1`.
6. Ingest them through the dedicated coordinator-authorized recovery route.
7. Reconcile a lost response by inspecting durable target identity before resend.
8. Re-read every item and verify immutable identity and canonical content hash.
9. Request and persist a newer synchronized report for the exact manifest.
10. Finish in `caught-up` while the provider remains unassigned.

The catch-up database is restart-safe. A new coordinator process can continue
the same recovery id and item attempt counters without duplicating committed
batches.

## Running one catch-up

The relay/coordinator machine needs access to the relay control database, the
storage control database, the F3 failover database, and a persistent F4.1
catch-up database.

```bash
python -m catalog.node.storage_catchup \
  --relay-control-database state/relay-control.sqlite3 \
  --storage-control-database state/storage-control.sqlite3 \
  --failover-database state/failover.sqlite3 \
  --catchup-database state/catchup.sqlite3 \
  --state-dir state/catchup-authority \
  --relay ws://127.0.0.1:8765 \
  --display-name "Storage catch-up authority" \
  --session-id SESSION_ID \
  --group-id storage-main \
  --returning-provider-id provider-primary \
  --allow-insecure-local
```

On first startup, provide the existing protected environment inputs:

- `FCP_ENROLLMENT_TOKEN`
- `FCP_SESSION_INVITATION`

The command returns `caught-up`, `retryable`, or `operator-attention` as JSON.
`retryable` can be run again with the same databases. `caught-up` is evidence
for F4.2; it does not itself restore redundancy.

## Acceptance criteria

The automated acceptance test starts a real relay, authority, primary, and
replica. It performs F3 failover, commits two additional batches on the promoted
primary, restarts the old primary, transfers one missing item, restarts the
catch-up coordinator, and completes the remaining transfer. It verifies that:

- only missing batches are copied;
- attempt counters and progress survive restart;
- all target content matches the authoritative manifest;
- a final authenticated synchronized report is stored;
- the returning provider remains unassigned;
- acknowledgement policy stays in degraded primary-only mode;
- degraded state remains present for F4.2;
- no storage node opens an inbound application listener.
