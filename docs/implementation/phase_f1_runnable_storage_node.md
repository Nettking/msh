# Phase F1: runnable relay-first storage node

Phase F1 turns the existing Phase D/E storage implementation into a process that can run on a separate physical machine. It does not introduce direct peer-to-peer transport or change the storage protocol.

## What is delivered

- one persistent Ed25519 node identity;
- one outbound authenticated WebSocket connection to the relay;
- one filesystem-backed `msh-storage-v1` provider;
- the existing Phase D/E authority, fencing, lease, replication, acknowledgement, manifest, and recovery rules;
- durable local control, outbox, acknowledgement, node-state, and provider data;
- automatic capability re-announcement after reconnect;
- restart reconciliation for prepared primary writes;
- a public local status command that does not expose credentials or local paths;
- no inbound listening ports on the storage machine.

Phase F2 will connect independently running primary and replica processes to a live coordinator control stream and exercise continuous replication between them.

## Configuration

Create a local JSON file. Relative paths are resolved relative to the configuration file.

```json
{
  "schema": "msh.storage_node_config.v1",
  "state_directory": "state",
  "relay_url": "wss://relay.example.org/federation",
  "display_name": "Storage laptop A",
  "provider_id": "provider-storage-a",
  "session_id": "session-example",
  "control_database": "state/control.sqlite3",
  "storage_directory": "state/storage",
  "outbox_database": "state/outbox.sqlite3",
  "acknowledgements_database": "state/acks.sqlite3",
  "allow_insecure_local": false,
  "heartbeat_interval": 10,
  "request_timeout": 15
}
```

Credentials must not be stored in this file. The loader rejects token, password, secret, and private-key fields.

Plain `ws://` is accepted only for an explicitly enabled loopback relay. Physical machines should use `wss://`.

## Commands

Validate the configuration without creating an identity:

```text
python -m catalog.node.storage_agent --config storage-node.json validate
```

Create or display the stable public identity:

```text
python -m catalog.node.storage_agent --config storage-node.json initialize
```

The coordinator must register the returned `node_id` against the configured `provider_id`. The node-local Phase D control database must contain the same provider registration and assignment before `run` starts. Live distribution of coordinator control changes is the next Phase F step.

Inspect local public status:

```text
python -m catalog.node.storage_agent --config storage-node.json status
```

First startup uses protected environment input:

Windows CMD:

```cmd
set MSH_ENROLLMENT_TOKEN=<one-time-enrollment-token>
set MSH_SESSION_INVITATION=<one-time-session-invitation>
python -m catalog.node.storage_agent --config storage-node.json run
```

PowerShell:

```powershell
$env:MSH_ENROLLMENT_TOKEN = "<one-time-enrollment-token>"
$env:MSH_SESSION_INVITATION = "<one-time-session-invitation>"
python -m catalog.node.storage_agent --config storage-node.json run
```

After enrollment and session join, the tokens are no longer needed. The identity, joined session, and capability announcement are durable and reused after restart.

## F1 acceptance boundary

F1 is complete when an integration test starts a real relay, enrolls a recorder and storage node, routes a primary ingest over the relay, verifies durable provider data, observes the advertised storage capability, restarts the storage process, and confirms that both identity and data survive.

F1 does not yet claim live multi-machine failover. That requires the next steps: control-plane propagation, separately running primary/replica processes, failure detection, promotion, and former-primary recovery.
