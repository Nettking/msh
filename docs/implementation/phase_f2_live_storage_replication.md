# Phase F2: live storage control and primary-to-replica replication

Phase F2 makes two independently running storage-node processes share one
coordinator-issued control state and replicate committed batches over the existing
authenticated relay connection.

## Delivered

- A coordinator creates a complete storage control snapshot from its authoritative
  `PhaseDControlPlane`.
- Every publication receives a durable monotonically increasing publication
  revision.
- The snapshot is hashed and signed by the coordinator node's persistent Ed25519
  identity.
- Storage nodes pin the expected coordinator node ID and require both:
  - an authenticated relay message from that node; and
  - a valid signature from the public key that derives the same node ID.
- A node reconciles the full snapshot idempotently into its local Phase D/E
  control database.
- Stale revisions, conflicting duplicate revisions, invalid signatures, local
  extra state, and non-monotonic replacement grants fail closed.
- A bound acknowledgement is returned to the coordinator for every target node.
- The F1 storage service then uses the synchronized assignment and grant to route
  primary writes and replicate each complete batch to the assigned replica.
- Control publications and replica data survive process restart.

## Runtime layout

```text
Coordinator process
  RelayNodeClient
  authoritative PhaseDControlPlane
  StorageControlPublicationStore
           |
           | signed msh-storage-control-v1 plan
           v
Relay server
    |                         |
    v                         v
Primary storage process       Replica storage process
LiveStorageNodeAgent          LiveStorageNodeAgent
local control.sqlite3         local control.sqlite3
local filesystem provider     local filesystem provider
```

No storage node opens an inbound listener. Control messages, storage requests,
replication requests, responses, and acknowledgements all travel through each
node's outbound WebSocket connection to the relay.

## Coordinator workflow

The coordinator identity must already be enrolled and joined to the session. Its
authoritative control database must contain the group, providers, assignment,
acknowledgement policy, and active grant.

Issue a signed plan:

```bash
python -m catalog.node.storage_control issue \
  --control-database ./coordinator/storage-control.sqlite3 \
  --session-id SESSION_ID \
  --authority-state-dir ./coordinator/node-state \
  --authority-display-name "Storage coordinator" \
  --publication-database ./coordinator/publications.sqlite3 \
  --output ./coordinator/storage-plan.json
```

Start both storage nodes with their normal F1 configuration and the pinned
coordinator node ID:

```bash
python -m catalog.node.live_storage_agent \
  --config ./primary/storage-node.json \
  --control-authority-node-id COORDINATOR_NODE_ID \
  run
```

```bash
python -m catalog.node.live_storage_agent \
  --config ./replica/storage-node.json \
  --control-authority-node-id COORDINATOR_NODE_ID \
  run
```

On first startup, provide enrollment and invitation tokens through
`MSH_ENROLLMENT_TOKEN` and `MSH_SESSION_INVITATION`. Tokens are not stored in the
configuration.

Publish the plan after both nodes are connected and waiting for control:

```bash
python -m catalog.node.storage_control publish \
  --plan ./coordinator/storage-plan.json \
  --state-dir ./coordinator/node-state \
  --relay wss://relay.example/ws \
  --display-name "Storage coordinator" \
  --target-node-id PRIMARY_NODE_ID \
  --target-node-id REPLICA_NODE_ID
```

The command succeeds only after both nodes return acknowledgements bound to the
publication ID, revision, and content hash.

## Acceptance coverage

The automated acceptance test starts a real relay and three persistent node
identities:

1. coordinator;
2. primary storage provider;
3. replica storage provider.

It then:

1. provisions the authoritative control plane;
2. issues and publishes one signed plan;
3. waits for both storage nodes to apply and acknowledge it;
4. writes a batch to the logical storage group;
5. verifies the batch exists at both primary and replica;
6. restarts the replica with the same state directory;
7. verifies identity, control publication, and replicated data survive restart.

## Explicit exclusions

Phase F2 does not automatically promote the replica when the primary disappears.
Heartbeat-driven failure detection, candidate reports, fencing, promotion, and
route update across live processes are Phase F3.

Direct peer-to-peer transport and job scheduling remain out of scope.
