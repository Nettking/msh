# Federated Session Network: Agent Implementation Handoff

Status: implementation guide for `docs/federated_session_network.md`.

This document is intentionally prescriptive. It exists to reduce repository exploration, architecture re-design, and token use when an implementation agent begins work.

## Read first

The implementation agent must read these files before changing code:

1. `docs/federated_session_network.md`
2. `docs/architecture.md`
3. `docs/data_contract.md`
4. `docs/connected_capabilities.md`
5. `catalog/mtconnect_recorder/storage.py`
6. `catalog/flask_app/services/server_setup_service.py`
7. `setup_msh.py`
8. `docker-compose.yml`

The federated implementation extends the current system. It must not replace the local Flask orchestrator, current recorder behavior, JSONL compatibility view, or existing AI-provider flow in the first changes.

## Fixed architectural decisions

Do not reopen these decisions during implementation unless a concrete blocker is demonstrated:

- A session is a versioned logical context shared by authenticated nodes.
- Every node has one persistent cryptographic identity.
- One node can advertise several capabilities.
- Several providers of the same capability type may be active.
- Storage uses one writable primary per storage group and zero or more replicas.
- A storage node never self-promotes merely because another node is unreachable.
- Leadership is granted by the session coordinator with a term, lease, and fencing token.
- The preferred replacement leader is the most complete qualified replica.
- A healthy, complete current leader remains leader when candidates are equivalent.
- Completeness is measured against committed manifests, hashes, watermarks, and missing ranges, not row count.
- Recorder writes remain local-first and durable until the required remote acknowledgements exist.
- Databases are accessed through an MSH Storage API, never directly by unrelated application components.
- The first cross-network transport is relay-first with outbound node connections.
- Direct peer transport is a later optimization and must retain relay fallback.
- Existing local filesystem storage becomes a provider; it is not discarded.
- Use `primary` and `replica`, not `master` and `slave`.

## Required package boundaries

Prefer the following structure unless existing repository conventions strongly require a small adjustment:

```text
catalog/
  federation/
    __init__.py
    models.py
    errors.py
    leader_selection.py
    coordinator.py
    event_log.py
    persistence.py
    tests/

  capabilities/
    __init__.py
    contracts.py
    registry.py
    selection.py
    tests/

  storage/
    __init__.py
    contracts.py
    client.py
    outbox.py
    manifest.py
    replication.py
    providers/
      __init__.py
      filesystem.py
      sqlite.py
      postgres.py
      remote.py
    tests/

  node/
    __init__.py
    identity.py
    client.py
    heartbeat.py
    tests/
```

Early phases may keep all Phase 0 domain code under `catalog/federation/` to avoid premature package fragmentation. Split packages only when the interfaces are used by real implementations.

## Pull-request sequence

Each step must be independently reviewable and leave `main` working.

### PR A: Phase 0 domain contracts and deterministic policy

Implement only pure domain logic. No sockets, Flask routes, PostgreSQL, Docker services, or changes to the recorder runtime.

Deliver:

- immutable or carefully validated domain models for nodes, sessions, capabilities, session events, storage batches, dataset coverage, storage manifests, node storage state, and leadership grants;
- schema/version constants;
- deterministic storage-leader eligibility and candidate ordering;
- sticky-leader behavior;
- lease and fencing-token validation helpers;
- serialization round-trip support using JSON-compatible dictionaries;
- tests from `docs/implementation/federated_session_test_matrix.md` that do not require networking.

Exit criteria:

- all new tests pass;
- existing applicable tests still pass;
- no production behavior changes;
- no new third-party dependency unless clearly justified.

### PR B: Phase 1 local interfaces and durable outbox

Deliver:

- a `StorageProvider` contract;
- a `CapabilityProvider` contract;
- a local capability registry supporting multiple providers per type;
- an SQLite durable outbox with atomic enqueue, retry metadata, acknowledgement, and idempotent completion;
- a filesystem storage adapter wrapping current durable recorder storage rather than rewriting it;
- compatibility tests proving current recorder output paths and checkpoint semantics remain unchanged.

Do not move all Flask reads to the new storage layer yet.

Exit criteria:

- existing local recording and workbench flows behave as before;
- the recorder can enqueue a synthetic remote-delivery record without requiring a network;
- crash/restart tests show pending outbox entries survive.

### PR C: Phase 2 relay-first session network

Deliver a minimal deployable control plane and node client:

- node enrollment and revocation;
- session create/join using an invitation or enrollment token;
- capability announcements and heartbeats;
- ordered session event log and replay after `last_applied_revision`;
- permanent outbound authenticated WebSocket or HTTP/2 connection;
- relay-framed messages addressed by node ID and session ID;
- a simple UI/status surface showing session, node, capabilities, and connectivity.

Do not add direct P2P in this PR.

Exit criteria:

- two devices on unrelated networks can join one session and exchange authenticated test messages without port forwarding;
- reconnect replays missing session events in order;
- a revoked node cannot send accepted session traffic.

### PR D: Phase 3 Storage API and replication

Deliver:

- versioned `msh-storage-v1` API;
- filesystem and PostgreSQL storage providers;
- registration of multiple storage providers;
- coordinator-controlled primary/replica assignments;
- term, lease, and fencing-token checks on every write;
- immutable, idempotent batch ingest;
- replication outbox and configurable acknowledgement policy;
- logical session storage routing so clients do not persist a physical leader address.

Exit criteria:

- two storage nodes on separate networks replicate one recorder batch;
- duplicate delivery stores one logical batch;
- controlled leadership handover succeeds;
- an obsolete primary write is rejected.

### PR E: Phase 4 completeness-aware failover

Deliver:

- authoritative per-storage-group manifests;
- committed batch/object hashes;
- contiguous watermarks and explicit missing ranges;
- replica integrity and eligibility reporting;
- deterministic promotion of the most complete qualified replica;
- explicit `storage-degraded` state when no complete candidate exists;
- UI diagnostics showing why each node is or is not eligible.

Exit criteria:

- incomplete replicas are never silently promoted;
- complementary partial datasets cause synchronization or degraded mode, not a false winner;
- a returning former leader rejoins as a replica and catches up.

### PR F: Phase 5 direct peer transport

Deliver:

- dedicated node-agent boundary;
- direct encrypted streams when possible;
- relay fallback;
- resumable chunked large-object transfer;
- transport choice hidden behind the same logical capability client.

Direct transport is an optimization. Correctness must not depend on it.

### PR G: Phase 6 scheduling for additional capabilities

Deliver capability-specific scheduling for multiple AI and compute providers. Do not force storage primary/replica semantics onto stateless or parallelizable capabilities.

## Repository integration rules

- Preserve `catalog/orchestrator/` as the local workflow orchestrator. Do not turn it into the distributed coordinator.
- Keep the control-plane implementation separate from Flask application process state.
- Reuse the current recorder's durable write ordering: raw input, detailed observations, compatibility snapshots, then checkpoint.
- Keep current JSONL outputs during migration.
- Do not expose PostgreSQL, Ollama, or Flask ports publicly by default.
- Do not store long-lived secrets in `data/server_setup/server_settings.json` or committed files.
- Any new persistent local state must use atomic writes or transactions.
- Every network command must include request ID, session ID, actor node ID, schema version, and authorization context.
- Every storage write must include batch ID, idempotency key, content hash, term, and fencing token.
- Reject unknown protocol major versions; tolerate additive optional fields within the same major version.

## Stop rules for agents

Stop and report instead of broadening scope when:

- the requested PR exit criteria are met;
- the next change requires a later phase;
- a design decision conflicts with `docs/federated_session_network.md`;
- an existing behavior would have to be removed rather than adapted;
- production credentials or public infrastructure are unavailable;
- tests expose an unresolved split-brain or data-loss scenario.

Do not silently replace a required safety mechanism with a placeholder that appears operational.

## Validation commands

Adapt paths to the actual changed files, but complete at least:

```bash
python -m compileall catalog setup_msh.py
pytest -q catalog
ruff check <changed-python-paths>
docker compose config --quiet
git diff --check
```

Networking phases also require integration tests that create at least two independently persisted node states and reconnect them after interruption.

## Recommended first Codex task

Use the following as the first implementation request:

```text
Read docs/federated_session_network.md,
docs/implementation/federated_session_agent_handoff.md, and
docs/implementation/federated_session_test_matrix.md.

Implement PR A only: Phase 0 domain contracts and deterministic policy.
Do not implement networking, Flask UI, Docker services, PostgreSQL, recorder
integration, or a durable outbox in this task.

Inspect repository conventions first. Then implement validated JSON-compatible
models, deterministic completeness-aware storage leader selection, sticky
leadership, lease and fencing validation, and all Phase 0 test vectors.
Preserve every existing behavior. Run the full applicable tests, compilation,
Ruff for changed files, and git diff checks. Stop when PR A exit criteria pass
and document the next phase without implementing it.
```

## Expected agent report

Every phase report should state:

- files added or changed;
- invariants implemented;
- tests and exact results;
- compatibility impact;
- security limitations;
- intentionally deferred work;
- any deviation from this handoff and its concrete reason.
