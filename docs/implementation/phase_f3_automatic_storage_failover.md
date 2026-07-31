# Phase F3: automatic verified storage failover

Phase F3 turns the F2 primary/replica runtime into a fail-closed live storage group. The coordinator detects loss of the current primary through the authenticated relay, obtains fresh integrity evidence from assigned replicas, promotes only a complete candidate, and publishes a new signed control plan.

## Safety model

The relay remains the only application path to a storage node. Storage nodes do not open inbound application ports.

When a live storage node loses trusted relay/control availability, its provider-local write gate closes immediately and its advertised storage capability becomes unavailable. The node does not rewrite the signed control history. It can expose writes again only after it has verified a fresh signed control publication from the pinned authority.

This gives two independent protections against stale-primary writes:

1. The disconnected node is unreachable through the relay and closes its local mutation gate.
2. Promotion advances both the leader term and fencing token. A returning former primary receives the new signed assignment and cannot use its previous grant.

## Failure detection

The failover coordinator evaluates the relay coordinator's complete status snapshot. A storage provider is considered live only when:

- its node is authenticated and connected;
- the node is not revoked;
- it advertises the expected `storage-provider` capability;
- the capability is `ready` and is bound to the registered provider ID.

A disconnected node or unavailable capability makes the current primary a failover candidate. Detection alone never grants authority to another provider.

## Fresh replica evidence

For each connected and assigned replica, the coordinator sends an authenticated report request containing the exact authoritative manifest revision and hash.

The replica then:

- loads each authoritative batch's durable identity;
- checks dataset, schema, idempotency key, and content hash;
- reads the stored content;
- recalculates its SHA-256 content identity;
- reports health, synchronization state, and eligibility against the requested manifest.

The relay sender identity binds the report to the registered node. The coordinator persists and assesses the report through the existing Phase E report and selection contracts. A stale, mismatched, incomplete, or integrity-failing report is not eligible.

If no assigned replica provides complete evidence, the group remains blocked and a durable degraded-state obligation records that a complete replica must be restored or caught up. The coordinator does not guess.

## Durable authority transfer

The selected candidate is bound to a durable failover record containing:

- the failed provider and node;
- the exact source grant, term, and fencing token;
- the selected replica report revision and hash;
- the replacement provider and node;
- the new term, fencing token, and grant ID;
- the surviving replica set and effective acknowledgement policy;
- the signed publication and transaction state.

The transaction progresses through `detected`, `control-committed`, and `published`. Contradictory durable records fail closed.

Promotion uses the existing atomic Phase D handover path. The replacement term and fencing token must both be strictly greater than the failed primary's values. The failed provider is removed from the active assignment before new authority is published.

## Availability and degraded redundancy

If the promoted provider has no remaining replica, an acknowledgement policy such as `one-replica` cannot be satisfied. F3 then reduces the effective policy to `primary` so that verified storage can continue accepting writes, and records a durable degraded-state obligation to restore replication and the previous acknowledgement policy.

This is an explicit availability trade-off, not silent weakening. The failed provider cannot automatically return as a replica; it must first complete the separate verified catch-up process.

## Returning former primary

A node that restarts with cached primary state advertises itself as unavailable and asks the pinned authority for current control. The write gate stays closed while it waits.

After promotion, the returned node receives a newer signed publication in which it is unassigned. Attempts to write with its old grant are rejected as `not-primary`. Its stable node identity and stored data are retained for later recovery work.

## Runtime

The coordinator runtime is available through:

```text
python -m catalog.node.storage_failover [options] scan-once
python -m catalog.node.storage_failover [options] run
```

It requires the relay coordinator database, the authoritative storage-control database, publication and failover databases, the authority identity state directory, relay URL, and session ID. Enrollment and first session join continue to use protected environment variables.

## Acceptance coverage

The F3 acceptance test starts a real relay, one coordinator, one primary storage node, and one replica storage node. It verifies that:

- an initial batch commits only after replication;
- primary loss triggers a fresh manifest-bound replica report;
- the complete replica is promoted with a higher term and fencing token;
- the failed provider is excluded and degraded redundancy is explicit;
- a new write commits through the promoted node;
- the selected report and publication are durable;
- the former primary restarts as unassigned;
- its previous grant cannot write;
- no storage node opens an inbound application listener.

The test is included in both the Linux and Windows Phase 2/Federation matrices.

## Excluded from F3

F3 does not implement automatic former-primary catch-up, restoration of the original replica count, direct peer transport, or capability/job scheduling. Those remain separate phases so that authority transfer, data repair, and workload scheduling do not become one oversized trust boundary.
