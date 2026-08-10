# Federated Session Network: Phase 0 Contracts

Status: normative implementation contract for the first federated-session pull request.

This document freezes the minimum domain objects and invariants so an implementation agent can build Phase 0 without inventing network, database, or UI behavior.

## General rules

- All persisted and transmitted objects have a `schema` identifier with a major version.
- IDs are opaque, stable strings. Code must not infer topology or authorization from an ID format.
- Revisions, terms, sequences, and fencing tokens are non-negative integers and are monotonic within their defined scope.
- Timestamps are timezone-aware UTC values serialized as RFC 3339 strings.
- Domain models serialize to JSON-compatible dictionaries. Python pickle or framework-specific object serialization is not a protocol.
- Validation errors are structured and must identify the failing field or invariant.
- Unknown protocol major versions are rejected.
- Additive optional fields within one supported major version must not invalidate an otherwise valid object.

## Enumerations

Recommended values:

```text
SessionState:
  pending
  active
  paused
  closing
  closed
  degraded

CapabilityStatus:
  registering
  ready
  unavailable
  draining
  disabled
  revoked

StorageRole:
  unassigned
  primary
  replica
  partial-replica
  cache
  archive
  object-only
  spool

StorageGroupState:
  pending
  healthy
  under-replicated
  handover
  storage-degraded
  unavailable

CommitState:
  provisional
  pending-replication
  committed
  rejected
```

## Node identity

Schema: `fcp.node.v1`

Required fields:

```json
{
  "schema": "fcp.node.v1",
  "node_id": "node-opaque-id",
  "display_name": "Recorder laptop",
  "public_key": "base64-or-armored-public-key",
  "created_at": "2026-07-28T12:00:00Z",
  "identity_version": 1
}
```

Invariants:

- `node_id` is stable for the lifetime of the identity.
- The private key is never part of this model or capability announcements.
- Renaming a node does not change `node_id`.
- A replaced identity receives a new `node_id`; it is not silently reused.

## Session

Schema: `fcp.session.v1`

```json
{
  "schema": "fcp.session.v1",
  "session_id": "session-cnc-2026-07-28",
  "display_name": "Mazak tool-wear experiment",
  "state": "active",
  "revision": 219,
  "created_at": "2026-07-28T12:00:00Z",
  "created_by_node_id": "node-owner",
  "coordinator_id": "coordinator-main"
}
```

Invariants:

- `revision` is the last applied authoritative session-event revision.
- A node cannot derive membership from possession of a session ID alone.
- State transitions must be explicit session events.
- Closing or deleting a session does not silently delete replica data; retention/deletion is a separate audited policy.

## Capability announcement

Schema: `fcp.capability.v1`

```json
{
  "schema": "fcp.capability.v1",
  "capability_id": "storage-node-b-main",
  "node_id": "node-storage-b",
  "session_id": "session-cnc-2026-07-28",
  "type": "storage",
  "protocol": "fcp-storage-v1",
  "protocol_version": "1.0",
  "status": "ready",
  "properties": {
    "backend": "postgresql",
    "available_bytes": 450000000000,
    "supports_replication": true,
    "supports_objects": true
  },
  "announced_at": "2026-07-28T12:00:00Z"
}
```

Invariants:

- `(session_id, capability_id)` is unique.
- One node may announce multiple capabilities.
- Several nodes may announce the same capability type.
- `properties` must not contain secrets.
- Capability presence does not itself grant authorization or leader eligibility.

## Session event

Schema: `fcp.session_event.v1`

```json
{
  "schema": "fcp.session_event.v1",
  "session_id": "session-cnc-2026-07-28",
  "revision": 219,
  "event_id": "event-opaque-id",
  "event_type": "storage.leader.granted",
  "occurred_at": "2026-07-28T12:05:00Z",
  "actor_node_id": "control-plane",
  "payload": {
    "group_id": "storage-main",
    "node_id": "node-storage-b",
    "term": 15,
    "fencing_token": 9815
  }
}
```

Invariants:

- Revisions are strictly increasing within one session.
- `event_id` is globally unique enough for idempotency.
- Applying an already applied `event_id` is a no-op.
- A revision gap prevents normal application until missing events are replayed.
- Events are append-only; corrections are represented by later events.

## Dataset coverage

Schema: embedded in `fcp.storage_manifest.v1` and storage-node reports.

```json
{
  "dataset_id": "mazak-telemetry",
  "schema_name": "fcp.mtconnect.snapshot",
  "schema_version": 2,
  "required": true,
  "committed_revision": 1842,
  "last_contiguous_sequence": 15000,
  "missing_ranges": [],
  "batch_count": 927,
  "object_count": 0,
  "manifest_hash": "sha256:..."
}
```

Invariants:

- `missing_ranges` are normalized, sorted, non-overlapping inclusive ranges.
- A higher row count does not imply greater coverage.
- A required dataset with missing committed ranges makes a full-replica candidate incomplete.
- Hash verification status is reported separately from mere hash presence.
- Data categories may belong to different storage groups.

## Authoritative storage manifest

Schema: `fcp.storage_manifest.v1`

```json
{
  "schema": "fcp.storage_manifest.v1",
  "session_id": "session-cnc-2026-07-28",
  "group_id": "storage-main",
  "revision": 1842,
  "term": 15,
  "datasets": {
    "mazak-telemetry": {
      "dataset_id": "mazak-telemetry",
      "schema_name": "fcp.mtconnect.snapshot",
      "schema_version": 2,
      "required": true,
      "committed_revision": 1842,
      "last_contiguous_sequence": 15000,
      "missing_ranges": [],
      "batch_count": 927,
      "object_count": 0,
      "manifest_hash": "sha256:..."
    }
  },
  "manifest_hash": "sha256:...",
  "updated_at": "2026-07-28T12:10:00Z"
}
```

Invariants:

- Manifest revision never decreases.
- The manifest describes committed state only.
- Provisional local data is excluded until commit policy succeeds.
- Manifest hash is computed from a canonical representation that excludes the hash field itself.
- A manifest update is associated with an authoritative session event.

## Storage batch

Schema: `fcp.storage_batch.v1`

```json
{
  "schema": "fcp.storage_batch.v1",
  "session_id": "session-cnc-2026-07-28",
  "group_id": "storage-main",
  "dataset_id": "mazak-telemetry",
  "batch_id": "batch-opaque-id",
  "source_node_id": "node-recorder-a",
  "idempotency_key": "recorder-a:agent-42:10000-10999",
  "content_hash": "sha256:...",
  "record_count": 1000,
  "first_sequence": 10000,
  "last_sequence": 10999,
  "created_at": "2026-07-28T12:10:00Z"
}
```

Invariants:

- Repeating the same batch ID/idempotency key with the same content hash is idempotent.
- Reusing either identity with a different content hash is a conflict.
- Metadata commit does not prove payload durability; the storage provider reports durable acceptance explicitly.
- A recorder retains the local payload until acknowledgement policy succeeds.

## Storage-node state report

Schema: `fcp.storage_node_state.v1`

```json
{
  "schema": "fcp.storage_node_state.v1",
  "session_id": "session-cnc-2026-07-28",
  "group_id": "storage-main",
  "node_id": "node-storage-b",
  "role": "replica",
  "online": true,
  "integrity_verified": true,
  "schema_compatible": true,
  "leader_eligible": true,
  "authoritative_revision": 1842,
  "replication_lag": 0,
  "datasets": {},
  "term": 15,
  "last_fencing_token": 9815,
  "reported_at": "2026-07-28T12:11:00Z"
}
```

Invariants:

- `leader_eligible=true` is accepted only when eligibility can be independently recomputed from the report and policy.
- The coordinator does not trust a node's claimed role without a valid leadership grant.
- Wall-clock recency does not override revision, term, manifest coverage, or integrity.

## Leadership grant

Schema: `fcp.leadership_grant.v1`

```json
{
  "schema": "fcp.leadership_grant.v1",
  "session_id": "session-cnc-2026-07-28",
  "group_id": "storage-main",
  "node_id": "node-storage-b",
  "role": "primary",
  "term": 15,
  "fencing_token": 9815,
  "issued_at": "2026-07-28T12:12:00Z",
  "lease_expires_at": "2026-07-28T12:12:30Z",
  "grant_id": "grant-opaque-id"
}
```

Invariants:

- Term and fencing token increase whenever a new primary is granted.
- A grant is scoped to exactly one session, storage group, node, and role.
- A primary write requires a currently valid grant matching all four scopes.
- Expired grants never become valid again.
- A former primary returning with an old grant is treated as a replica candidate.

## Leader-selection result

The pure policy function should return a structured result, not just a node ID:

```json
{
  "decision": "retain-current",
  "selected_node_id": "node-storage-a",
  "eligible_node_ids": ["node-storage-a", "node-storage-b"],
  "rejected": {
    "node-storage-c": ["missing-required-dataset", "integrity-not-verified"]
  },
  "reason": "current-primary-is-healthy-complete-and-not-authoritatively-behind",
  "requires_new_grant": false
}
```

Recommended decisions:

```text
retain-current
select-candidate
no-qualified-candidate
synchronization-required
storage-degraded
authoritative-state-required
```

`authoritative-state-required` means selection found a qualified candidate but
cannot safely propose a new grant because neither a persisted authoritative
fencing counter nor a correctly scoped previous coordinator grant was supplied.
It returns no selected node, term, fencing token, or lease. Candidate-reported
terms and fencing tokens never substitute for this coordinator-owned state.

## Pure leader-selection algorithm

Inputs:

- authoritative storage manifest;
- current primary ID, if any;
- current leadership grant, if any;
- candidate storage-node reports;
- current coordinator time;
- storage-group policy.

Policy:

```text
1. Validate manifest and candidate reports.
2. Recompute candidate eligibility from required datasets, missing ranges,
   integrity, schema compatibility, authorization, and operational state.
3. Determine whether the current primary still has a valid lease and remains
   complete against the authoritative manifest.
4. If yes, retain it unless policy explicitly requires controlled handover.
5. If no valid current primary, order qualified candidates deterministically.
6. Select the highest-ranked qualified candidate.
7. If none is qualified but repair is possible, return synchronization-required.
8. If required committed data cannot be located, return storage-degraded.
9. Selection itself does not create authority; the coordinator must issue a new
   term, lease, and fencing token in a leadership grant.
10. Required-dataset committed revisions and contiguous watermarks must match
    the authoritative manifest exactly. Candidate-local data beyond those
    values is provisional reconciliation state and never increases authority.
11. If a new grant is required but authoritative fencing state is unavailable,
    return `authoritative-state-required` rather than restarting a fencing
    counter or deriving one from candidate reports.
```

The pure function must not perform I/O, mutate candidates, generate random IDs, or read global time.

## Lease and fencing validation

A storage write is accepted only when all checks pass:

```text
request.session_id == grant.session_id
request.group_id == grant.group_id
request.actor_node_id == grant.node_id
grant.role == primary
request.term == grant.term
request.fencing_token == grant.fencing_token
now < grant.lease_expires_at
grant is the coordinator's active grant for the group
request schema and authorization are valid
```

A failure returns a structured reason such as:

```text
lease-expired
stale-term
stale-fencing-token
wrong-session
wrong-storage-group
wrong-node
not-primary
unknown-grant
unauthorized
```

## Phase 0 exclusions

The first pull request must not implement:

- network transport;
- control-plane deployment;
- Flask routes or templates;
- database migrations;
- actual replication;
- recorder integration;
- Docker services;
- background threads;
- cryptographic key generation beyond an isolated interface or test-safe helper.

Phase 0 succeeds when these contracts, invariants, deterministic policy, serialization, and tests are implemented cleanly without changing runtime behavior.
