# Phase 2 relay-first session runbook

This runbook operates the Phase 2 federated-session control plane and two node
agents. The relay is an independent process; Flask remains the local MSH
workbench and is not the distributed coordinator.

The relay feature is opt-in. Ordinary `docker compose up` behavior is unchanged,
and a local MSH installation remains usable when no relay is running.

## Transport and identity choices

Phase 2 uses:

- `websockets` for one permanent outbound WebSocket connection from each node
  agent to the relay;
- Ed25519 from `cryptography` for persistent node identities, challenge signing,
  and connection authentication;
- SQLite transactions for relay authority and node restart state.

Ed25519 is used for signatures, not as a custom encryption scheme. Event order
comes from the coordinator-assigned session revision, never from a timestamp.
The Phase 1 delivery outbox is not the authoritative session event log.

On Windows, the private Ed25519 identity is protected for the current user with
DPAPI before it is written. On POSIX systems, the identity directory and file
are restricted to modes `0700` and `0600`. Identity creation uses a bounded
cross-process lock so two node processes cannot interleave key and metadata
creation.

The relay container listens on `0.0.0.0:8765` inside its private container
network, but Compose publishes it only on host loopback by default:

```text
127.0.0.1:8765
```

That default is suitable for local development and a TLS reverse proxy on the
same host. It is not a publicly safe plaintext deployment.

## Start the relay

Build and start only the opt-in relay service:

```bash
docker compose --profile relay up -d --build relay
docker compose --profile relay ps relay
docker compose --profile relay logs --tail 50 relay
```

The effective service command is:

```bash
python -m catalog.relay.service serve \
  --database /var/lib/msh-relay/control.sqlite3 \
  --host 0.0.0.0 \
  --port 8765 \
  --unsafe-development-plaintext
```

The `relay_state` volume is the relay's only mounted state. It preserves
enrollment, revocation, sessions, membership, capabilities, audit records, and
ordered events across container replacement.

The unsafe-development flag permits the relay to listen on the container's
network interface. Compose hard-codes the published host address to
`127.0.0.1`; this profile is not a public plaintext deployment.

For a local process instead of Docker:

```bash
python -m catalog.relay.service serve \
  --database data/federation/relay/control.sqlite3 \
  --host 127.0.0.1 \
  --port 8765
```

Use `ws://127.0.0.1:8765` only for a same-host test.

For a directly exposed relay, provide an administrator-managed certificate and
key; without both files a non-loopback start is rejected:

```bash
python -m catalog.relay.service serve \
  --database data/federation/relay/control.sqlite3 \
  --host 0.0.0.0 \
  --port 8765 \
  --tls-cert /secure/path/fullchain.pem \
  --tls-key /secure/path/private-key.pem
```

Nodes then use the certificate hostname and relay port, for example
`wss://relay.example.com:8765`, and omit `--allow-insecure-local`. The
certificate chain must be trusted by the node operating system; install a
private CA into that trust store before connecting. Add
`--tls-key-password-prompt` when the server key is encrypted.

## Public binding and TLS rules

The Compose profile cannot publish the plaintext relay on a non-loopback host
address. Before nodes connect across different networks, run the relay directly
with TLS or place its loopback backend behind a maintained TLS reverse proxy:

1. Put a maintained TLS reverse proxy on the relay host.
2. Keep the plaintext relay backend reachable only through loopback or a private
   container network.
3. Expose only the proxy's TLS port, normally TCP 443.
4. Configure nodes with a hostname-validated `wss://` URL.
5. Send enrollment and invitation tokens only over that authenticated TLS
   connection.

Do not edit the Compose loopback mapping to publish `ws://` on a public
address. If TLS is unavailable, stop rather than expose the relay publicly.
Production certificates and public infrastructure are not supplied by this PR.

Phase 2 is limited to control-plane and test-message traffic. Do not send
recorder batches, storage contents, AI prompts, credentials, or other sensitive
data-plane payloads through this deployment. Those application protocols and
their additional protection belong to later phases.

Never store a private identity, raw enrollment token, raw invitation token, or
authorization credential in:

- source control;
- `.env`;
- Docker Compose;
- `data/server_setup/server_settings.json`;
- capability properties;
- copied status output or logs.

Capability identifiers, protocols, and properties are bounded public logical
metadata. The relay rejects credential-shaped values, secret-key variants,
backend paths, database locations, and physical endpoint addresses in
capability or test-message payloads. A logical backend label such as
`{"backend":"postgresql"}` remains valid; it is not a physical locator.

## Prepare two independent nodes

For a local two-node exercise, create two independent state directories. They
simulate two devices; no file may be shared between them:

```bash
mkdir -p data/federation/node-a
mkdir -p data/federation/node-b
```

On separate physical devices, use one directory on each device instead.

Generate each identity once:

```bash
python -m catalog.node.client initialize-identity \
  --state-dir data/federation/node-a \
  --display-name "Node A"

python -m catalog.node.client initialize-identity \
  --state-dir data/federation/node-b \
  --display-name "Node B"
```

The two identity JSON results must show different node IDs. Restarting the
command with the same state directory must load the same ID. Do not copy an
identity file from one directory to the other. Missing, malformed, or replaced
identity state after enrollment must be treated as an error, not as a new node.

## Enroll both nodes

Create a short-lived, single-use token for Node A. When the relay runs in
Compose, run the administration command against the same relay volume:

```bash
docker compose --profile relay run --rm --no-deps relay \
  python -m catalog.relay.service create-enrollment-token \
  --database /var/lib/msh-relay/control.sqlite3 \
  --ttl-seconds 900 \
  --max-uses 1
```

The result is a JSON object displayed once. Copy only its `token` field value
and paste that value into the node client's protected prompt; do not put it in
the command line or shell history:

```bash
python -m catalog.node.client enroll \
  --state-dir data/federation/node-a \
  --relay ws://127.0.0.1:8765 \
  --display-name "Node A" \
  --allow-insecure-local
```

Repeat token creation and enrollment for Node B. Tokens are bounded, expiring,
and single-use. A reused or expired token must fail without enrolling a node.

For a relay started directly on the host, run the same administration operation
with:

```text
--database data/federation/relay/control.sqlite3
```

## Create one session and join it

Create a session as Node A. The creator becomes a member independently of node
enrollment:

```bash
python -m catalog.node.client create-session \
  --state-dir data/federation/node-a \
  --relay ws://127.0.0.1:8765 \
  --display-name "Node A" \
  --allow-insecure-local \
  --name "Phase 2 test"
```

Record the returned `session_id`. Create a short-lived, single-use invitation:

```bash
python -m catalog.node.client create-session-invitation \
  --state-dir data/federation/node-a \
  --relay ws://127.0.0.1:8765 \
  --display-name "Node A" \
  --allow-insecure-local \
  --session-id SESSION_ID \
  --ttl-seconds 900 \
  --max-uses 1
```

The invitation command also prints JSON. Copy only its `token` field value and
paste that value into Node B's protected prompt:

```bash
python -m catalog.node.client join-session \
  --state-dir data/federation/node-b \
  --relay ws://127.0.0.1:8765 \
  --display-name "Node B" \
  --allow-insecure-local
```

Enrollment and membership are separate checks. An enrolled node that has not
joined `SESSION_ID` must be rejected if it sends or routes traffic in that
session. An expired or reused invitation must not create membership.

If an invitation command is retried with the same request ID before use, the
coordinator atomically rotates the unrecoverable token digest and returns one
replacement; the earlier token immediately becomes invalid. This supports
recovery from a lost response without persisting a raw invitation. A retry
after use is rejected.

## Run the outbound agents and exchange a test message

Start Node B in one terminal. It stays connected, reconnects with bounded
backoff, and prints received test messages:

```bash
python -m catalog.node.client run \
  --state-dir data/federation/node-b \
  --relay ws://127.0.0.1:8765 \
  --display-name "Node B" \
  --allow-insecure-local
```

In another terminal, start Node A with one initial bounded test message. The
process remains connected after the acknowledgement:

```bash
python -m catalog.node.client run \
  --state-dir data/federation/node-a \
  --relay ws://127.0.0.1:8765 \
  --display-name "Node A" \
  --allow-insecure-local \
  --send-session-id SESSION_ID \
  --send-target-node-id NODE_B_ID \
  --send-payload '{"text":"phase-2 relay test"}'
```

Neither command accepts a listen address or publishes a service port. Both
connections are outbound to the relay. Node B reports the authenticated actor
node ID, target node ID, session ID, request ID, and test payload without
printing authorization material.

## Inspect non-secret status

Inspect each node:

```bash
python -m catalog.node.client local-status \
  --state-dir data/federation/node-a \
  --display-name "Node A"

python -m catalog.node.client local-status \
  --state-dir data/federation/node-b \
  --display-name "Node B"
```

`local-status` reads the durable non-secret state without opening another
network connection. To include the coordinator-visible view, stop that node's
running agent temporarily and use:

```bash
python -m catalog.node.client status \
  --state-dir data/federation/node-a \
  --relay ws://127.0.0.1:8765 \
  --display-name "Node A" \
  --allow-insecure-local
```

The combined status surface shows:

- local node ID and enrollment state;
- joined session IDs;
- connected, disconnected, replaying, revoked, or error state;
- last heartbeat;
- last applied revision per session;
- deterministically ordered local capabilities;
- coordinator-visible nodes and capabilities when authorized.

Coordinator-visible status uses authenticated, deterministic pagination. Each
wire page stays below the relay's 65 KiB envelope limit, while the node client
collects and validates every page before returning the existing aggregate
`sessions`, `nodes`, and `capabilities` shape. Reconnect reconciliation does
not add or remove any local session membership unless that complete snapshot
was received. If authorized membership, session revision, connectivity, or
capability status changes between pages, the client restarts the snapshot read
within a bounded retry limit.

It must not contain private keys, signatures, raw tokens, credential hashes,
authorization headers, backend paths, SQLite row IDs, or log tails.

## Test reconnect and ordered replay

1. Note Node B's durable `last_applied_revision`.
2. Stop Node B's agent.
3. Stop Node A's long-running command before using the one-shot append command,
   then append at least three session events from Node A:

   ```bash
   python -m catalog.node.client append-event \
     --state-dir data/federation/node-a \
     --relay ws://127.0.0.1:8765 \
     --display-name "Node A" \
     --allow-insecure-local \
     --session-id SESSION_ID \
     --event-type test.message \
     --payload '{"sequence": 1}'
   ```

   Repeat with sequence values 2 and 3. Each CLI invocation generates fresh
   request and event IDs automatically.

4. Restart Node A's long-running command.
5. Restart Node B with the same state directory and relay URL.
6. Read Node B status again.

Node B must reconnect with bounded backoff, request replay from its stored
revision, and durably apply the three missing revisions exactly once and in
order. Its stored revision must advance only after each corresponding event is
durably applied. Repeating delivery of an event must not advance it twice.

Replay is streamed in bounded pages while the authenticated heartbeat remains
active. Every page must make durable revision progress. Live control events and
a replay snapshot are serialized per connection, so an event cannot be placed
between replay data and its completion marker.

One replay pass is capped at 1,024 pages (32,768 events). If the authoritative
head continues moving beyond that bound, the client keeps its already durable
revision, reconnects within its bounded retry policy, and resumes from that
revision instead of holding one connection in an unbounded replay loop.

For the F2-003 acceptance vector, create authoritative revisions through 43,
stop Node B at revision 40, and verify that restart applies exactly 41, 42, and
43 in that order. Revision 43 must not be applied while 42 is missing; the node
must remain in replay/resynchronization state.

To test relay restart persistence:

```bash
docker compose --profile relay restart relay
```

Both agents should reconnect without new enrollment or membership and without
duplicating authoritative events.

## Revoke Node B

Revoke by stable node ID and record a reason:

```bash
docker compose --profile relay run --rm --no-deps relay \
  python -m catalog.relay.service revoke-node \
  --database /var/lib/msh-relay/control.sqlite3 \
  --node-id NODE_B_ID \
  --reason "Phase 2 revocation test"
```

After revocation:

- Node B's active traffic is rejected;
- Node B cannot establish a newly accepted connection;
- Node B cannot append events or route session messages;
- the rejection and revocation are present in the audit log;
- Node A remains connected and can continue accepted traffic.

Inspect the redacted audit view:

```bash
docker compose --profile relay run --rm --no-deps relay \
  python -m catalog.relay.service audit-status \
  --database /var/lib/msh-relay/control.sqlite3 \
  --node-id NODE_B_ID
```

Audit output must not reveal enrollment tokens, invitations, private identity
material, credentials, or complete rejected payloads.

Audit reads are limited in the database query, and the coordinator retains a
deterministic maximum of 100,000 recent audit rows. Status responses are
cursor-paged below the relay frame limit; the node validates and assembles one
complete authorized snapshot before reconciling durable memberships.

## Stop the relay

Stop the opt-in service without changing the default MSH services:

```bash
docker compose --profile relay stop relay
```

To remove the relay container while preserving its durable `relay_state`
volume:

```bash
docker compose --profile relay rm -f relay
```

Do not remove the relay volume unless permanent loss of enrollment, revocation,
membership, event, and audit state is explicitly intended.

## Explicit Phase 2 limits

This deployment does not implement PR D through PR G features:

- no `msh-storage-v1`, PostgreSQL provider, recorder-batch replication,
  primary/replica assignment, storage acknowledgement policy, or storage
  fencing writes from PR D;
- no completeness manifests, replica repair, automatic promotion, or storage
  failover from PR E;
- no direct peer-to-peer transport, NAT traversal, WebRTC, direct discovery,
  hole punching, or resumable direct object transfer from PR F;
- no distributed AI-provider, compute, or capability scheduling from PR G.

The relay-first control plane does not replace Flask, the local orchestrator,
recorder paths, recorder checkpoints, filesystem storage, JSONL compatibility
views, the workbench, or the existing AI-provider flow.
