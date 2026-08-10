# Federated Session Network: Phase 3 Storage Plan

Status: incremental implementation plan for the former PR D scope.

The original Phase 3 proposal combined protocol design, provider implementations,
control-plane assignment, write fencing, relay routing, replication, acknowledgement
policy, and leadership handover.  Phase 3 is therefore split into independently
reviewable steps D0--D8.  Every step must leave `main` operational and must not pull
Phase 4 completeness-aware automatic failover into Phase 3.

## Sequence

- **D0:** freeze the versioned `fcp-storage-v1` application protocol.
- **D1:** local provider-neutral dispatch and filesystem conformance.
- **D2:** local PostgreSQL provider and transactional conformance.
- **D3:** storage capability registration and coordinator assignments.
- **D4:** term, lease, grant, and fencing enforcement on writes.
- **D5:** logical storage routing over the existing relay.
- **D6:** durable primary replication outbox and replica acknowledgement.
- **D7:** configurable acknowledgement policies.
- **D8:** controlled leadership handover between synchronized providers.

Phase 4 remains responsible for authoritative completeness manifests, missing-range
reconciliation, automatic candidate selection, degraded-mode promotion decisions,
and returning-former-leader recovery.

## D0 decisions

D0 is a pure contract change.  It adds no provider, network, coordinator, recorder,
Flask, Docker, or database behavior.

### Protocol identity

- Protocol: `fcp-storage-v1`
- Current version: `1.0`
- Unknown major versions are rejected.
- Additive optional fields are tolerated within major version 1.

### Envelope requirements

Every request carries:

- schema;
- request ID;
- protocol and protocol version;
- operation;
- session ID;
- actor node ID;
- authorization context;
- operation-specific payload.

Stable operations defined in D0 are:

- `storage.describe`
- `storage.health`
- `batch.ingest`
- `batch.exists`
- `batch.read`
- `batch.list`
- `replica.acknowledge`

### Write authority

Every mutating batch request carries:

- session ID;
- storage group ID;
- actor node ID;
- grant ID;
- term;
- fencing token;
- lease expiry.

D0 validates structure only.  D4 will validate the authority against current
coordinator state and provider state.

### Immutable batch identity

A batch ingest request carries:

- dataset ID;
- batch ID;
- idempotency key;
- canonical SHA-256 content hash;
- JSON-compatible content;
- creation timestamp;
- write authority.

Reusing an idempotency key succeeds only when both batch ID and content hash match
the previously committed record.  A different batch or different content hash is
an `idempotency-conflict`.  Providers added in D1 and D2 must enforce this rule
atomically.

### Response and error rules

A response contains either a result or a structured error, never both.  Error codes
are stable protocol values so callers must not parse human-readable messages.
Defined D0 codes include authorization and leadership failures, stale term/fencing
failures, lease/grant failures, batch lookup failures, idempotency conflicts,
content-hash mismatch, unsupported protocol/operation, and internal error.

## D0 exit criteria

- request, response, authority, batch ingest, and ingest-result models round-trip
  through JSON-compatible dictionaries;
- unknown protocol major versions are rejected;
- additive fields within protocol major 1 do not break parsing;
- canonical content hashes detect modified payloads;
- identical idempotent retries are distinguishable from conflicts;
- all existing behavior remains unchanged;
- no new third-party dependency is introduced.
