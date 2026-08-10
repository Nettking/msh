# Phase F5.1: reproducible three-machine deployment

F5.1 packages the completed relay-first storage federation for a controlled test on three separate physical machines.

```text
Machine A  relay + coordinator + storage authority
Machine B  storage primary
Machine C  storage replica
```

This phase proves stable deployment, normal replication, and restart persistence. It does **not** disconnect the primary, promote the replica, or run former-primary recovery. Those failure and recovery exercises belong to F5.2 and F5.3.

## Delivered tooling

The deployment CLI is:

```text
python -m catalog.node.storage_deployment <command>
```

It provides:

- strict, secret-free storage-node configuration rendering;
- authority enrollment, session creation, and one-use session invitations;
- a portable public topology bound to all three permanent node identities;
- idempotent initial provider/group/assignment/grant provisioning;
- signed control publication to both storage nodes;
- one normal write that must receive a replica acknowledgement;
- public authority and storage evidence files;
- a verifier for before/after-restart identity, role, control, and data evidence.

No command stores enrollment tokens, invitation tokens, passwords, private keys, or TLS-key material in deployment JSON.

## Security boundary

For physical machines:

- use `wss://` for the relay;
- expose only the relay TLS port on Machine A;
- storage nodes make outbound relay connections and open no application listener;
- keep the relay database, authority identity, TLS key, and storage state directories private;
- transfer one-use tokens through a protected channel and clear the environment variables after first startup;
- do not redirect the output of `bootstrap-authority`, because it intentionally prints two one-use session invitations.

Plain `ws://` is accepted only for an explicitly enabled loopback test.

## Prerequisites on all machines

Use the same tested FCP commit on all three machines:

```text
9b0e828e22a0129c18958b75819e5326a660dbf9 or a later commit containing F5.1
```

Install Python 3.12 and repository dependencies:

```bash
python -m pip install -r requirements.txt -c constraints-phase2.txt
```

Choose permanent directories. Do not delete or copy one machine's `state` directory to another machine. The Ed25519 identity in that directory defines the node.

The examples below use:

```text
Relay URL:       wss://relay.example.org:8765
Session name:    Physical storage acceptance
Group:           storage-main
Primary ID:      provider-primary
Replica ID:      provider-replica
Probe batch:     f51-probe-1
```

## 1. Start Machine A

Create TLS material through the organization's normal certificate process. Then start the relay:

```bash
python -m catalog.relay.service serve \
  --database f51/authority/relay-control.sqlite3 \
  --host 0.0.0.0 \
  --port 8765 \
  --tls-cert /protected/path/relay-cert.pem \
  --tls-key /protected/path/relay-key.pem
```

Only the relay port needs inbound firewall access. The coordinator database is local and must not be shared over the network.

Create one enrollment token for the authority:

```bash
python -m catalog.relay.service create-enrollment-token \
  --database f51/authority/relay-control.sqlite3 \
  --ttl-seconds 600 \
  --max-uses 1
```

Set the returned token only in the current shell.

PowerShell:

```powershell
$env:FCP_ENROLLMENT_TOKEN = "<authority-enrollment-token>"
```

Bash:

```bash
export FCP_ENROLLMENT_TOKEN='<authority-enrollment-token>'
```

Bootstrap the permanent authority identity, create the session, and issue one invitation for each storage machine:

```bash
python -m catalog.node.storage_deployment bootstrap-authority \
  --relay wss://relay.example.org:8765 \
  --state-dir f51/authority/state \
  --display-name "Storage authority" \
  --session-name "Physical storage acceptance" \
  --public-output f51/authority/bootstrap-public.json
```

The public file contains only the authority node ID, relay URL, and session ID. The command output also contains:

```text
primary_session_invitation
replica_session_invitation
```

Copy each invitation to its intended machine through a protected channel. Do not add the output to Git, chat logs, screenshots, or the evidence directory.

Clear the authority enrollment token:

```powershell
Remove-Item Env:FCP_ENROLLMENT_TOKEN
```

```bash
unset FCP_ENROLLMENT_TOKEN
```

## 2. Configure Machine B

Render the primary configuration using the session ID from `bootstrap-public.json`:

```bash
python -m catalog.node.storage_deployment render-storage-config \
  --output f51/storage-node.json \
  --relay wss://relay.example.org:8765 \
  --session-id <session-id> \
  --display-name "Storage machine B" \
  --provider-id provider-primary
```

Validate and initialize it:

```bash
python -m catalog.node.storage_agent --config f51/storage-node.json validate
python -m catalog.node.storage_agent --config f51/storage-node.json initialize
```

Record the returned public `node_id`. Keep the entire `f51/state` directory on Machine B.

## 3. Configure Machine C

Render the replica configuration:

```bash
python -m catalog.node.storage_deployment render-storage-config \
  --output f51/storage-node.json \
  --relay wss://relay.example.org:8765 \
  --session-id <session-id> \
  --display-name "Storage machine C" \
  --provider-id provider-replica
```

Validate and initialize it:

```bash
python -m catalog.node.storage_agent --config f51/storage-node.json validate
python -m catalog.node.storage_agent --config f51/storage-node.json initialize
```

Record Machine C's public `node_id`.

The two node IDs must differ. Neither may equal the authority node ID.

## 4. Create enrollment tokens for B and C

On Machine A, create two separate one-use enrollment tokens:

```bash
python -m catalog.relay.service create-enrollment-token \
  --database f51/authority/relay-control.sqlite3 \
  --ttl-seconds 600 \
  --max-uses 1
```

Run the command twice and send one result to each storage machine.

## 5. Start B and C in control-waiting state

On Machine B, set its enrollment token and the **primary** invitation:

```powershell
$env:FCP_ENROLLMENT_TOKEN = "<machine-b-enrollment-token>"
$env:FCP_SESSION_INVITATION = "<primary-session-invitation>"
python -m catalog.node.live_storage_agent `
  --config f51/storage-node.json `
  --control-authority-node-id <authority-node-id> `
  run
```

On Machine C, use its enrollment token and the **replica** invitation:

```powershell
$env:FCP_ENROLLMENT_TOKEN = "<machine-c-enrollment-token>"
$env:FCP_SESSION_INVITATION = "<replica-session-invitation>"
python -m catalog.node.live_storage_agent `
  --config f51/storage-node.json `
  --control-authority-node-id <authority-node-id> `
  run
```

Equivalent Bash command:

```bash
python -m catalog.node.live_storage_agent \
  --config f51/storage-node.json \
  --control-authority-node-id <authority-node-id> \
  run
```

Both nodes initially wait for the authority's first signed control publication. Leave the processes running.

## 6. Render the public topology on A

Use the permanent node IDs returned by `initialize`:

```bash
python -m catalog.node.storage_deployment render-topology \
  --bootstrap f51/authority/bootstrap-public.json \
  --output f51/deployment.json \
  --group-id storage-main \
  --primary-node-id <machine-b-node-id> \
  --primary-provider-id provider-primary \
  --primary-display-name "Storage machine B" \
  --replica-node-id <machine-c-node-id> \
  --replica-provider-id provider-replica \
  --replica-display-name "Storage machine C"
```

Validate it:

```bash
python -m catalog.node.storage_deployment validate \
  --deployment f51/deployment.json
```

The topology is safe to copy to all three machines. It contains no credentials or physical filesystem paths.

## 7. Provision and publish initial control

While B and C are waiting, run on A:

```bash
python -m catalog.node.storage_deployment provision \
  --deployment f51/deployment.json \
  --authority-state-dir f51/authority/state \
  --control-database f51/authority/storage-control.sqlite3 \
  --publication-database f51/authority/publications.sqlite3 \
  --lease-seconds 86400
```

The command creates or verifies:

- group `storage-main`;
- B as primary;
- C as replica;
- `ONE_REPLICA` acknowledgement policy;
- an initial term and fencing token;
- a signed plan acknowledged by both node IDs.

A conflicting existing assignment, provider registration, or leader grant fails closed.

Once both storage processes report ready, clear their first-start environment variables. They are no longer needed after enrollment and session membership become durable.

## 8. Perform a normal replicated probe

On A:

```bash
python -m catalog.node.storage_deployment probe-write \
  --deployment f51/deployment.json \
  --authority-state-dir f51/authority/state \
  --control-database f51/authority/storage-control.sqlite3 \
  --acknowledgements-database f51/authority/acks.sqlite3 \
  --dataset-id f51-deployment \
  --batch-id f51-probe-1 \
  --idempotency-key f51:probe:1 \
  --content-json '{"deployment":"normal","sequence":1}'
```

The command succeeds only when:

- the primary commits the batch;
- the configured replica acknowledges it;
- the coordinator acknowledgement journal commits it;
- the authoritative manifest contains the same content hash.

Expected public result includes:

```json
{
  "required_replica_acks": 1,
  "acknowledged_replica_ids": ["provider-replica"]
}
```

## 9. Capture evidence before restart

Copy `f51/deployment.json` to B and C.

On B:

```bash
python -m catalog.node.storage_deployment capture-storage \
  --deployment f51/deployment.json \
  --role primary \
  --config f51/storage-node.json \
  --stage before-restart \
  --batch-id f51-probe-1 \
  --output f51/evidence-primary-before.json
```

On C:

```bash
python -m catalog.node.storage_deployment capture-storage \
  --deployment f51/deployment.json \
  --role replica \
  --config f51/storage-node.json \
  --stage before-restart \
  --batch-id f51-probe-1 \
  --output f51/evidence-replica-before.json
```

Evidence contains only public IDs, control hashes, roles, health, and immutable batch identity. It does not contain batch content or local paths.

## 10. Restart both storage nodes normally

Stop B and C cleanly. Start the same live-agent command again with no enrollment or invitation environment variables:

```bash
python -m catalog.node.live_storage_agent \
  --config f51/storage-node.json \
  --control-authority-node-id <authority-node-id> \
  run
```

The primary closes its local write gate until it receives fresh signed control. Re-run `provision` on A to publish an idempotent current plan:

```bash
python -m catalog.node.storage_deployment provision \
  --deployment f51/deployment.json \
  --authority-state-dir f51/authority/state \
  --control-database f51/authority/storage-control.sqlite3 \
  --publication-database f51/authority/publications.sqlite3 \
  --lease-seconds 86400
```

This must not create another identity, provider, group, or assignment.

## 11. Capture evidence after restart

On B:

```bash
python -m catalog.node.storage_deployment capture-storage \
  --deployment f51/deployment.json \
  --role primary \
  --config f51/storage-node.json \
  --stage after-restart \
  --batch-id f51-probe-1 \
  --output f51/evidence-primary-after.json
```

On C:

```bash
python -m catalog.node.storage_deployment capture-storage \
  --deployment f51/deployment.json \
  --role replica \
  --config f51/storage-node.json \
  --stage after-restart \
  --batch-id f51-probe-1 \
  --output f51/evidence-replica-after.json
```

Copy the four storage evidence files back to A.

Capture the authority state while B and C are connected:

```bash
python -m catalog.node.storage_deployment capture-authority \
  --deployment f51/deployment.json \
  --authority-state-dir f51/authority/state \
  --control-database f51/authority/storage-control.sqlite3 \
  --output f51/evidence-authority.json
```

## 12. Generate the F5.1 pass/fail report

On A:

```bash
python -m catalog.node.storage_deployment verify \
  --deployment f51/deployment.json \
  --authority f51/evidence-authority.json \
  --primary-before f51/evidence-primary-before.json \
  --replica-before f51/evidence-replica-before.json \
  --primary-after f51/evidence-primary-after.json \
  --replica-after f51/evidence-replica-after.json \
  --output f51/f51-report.json
```

F5.1 passes only when the report has:

```json
{"passed": true}
```

The verifier checks:

- all evidence belongs to the same deployment;
- authority, primary, and replica identities are distinct and stable;
- B remains primary before and after restart;
- C remains replica before and after restart;
- both nodes have verified signed control;
- both providers remain registered and assignable;
- both storage nodes are connected and advertise a `ready` storage capability;
- neither storage node exposes an inbound application port;
- `ONE_REPLICA` remains active;
- the primary grant remains bound to B;
- no degraded state exists;
- B, C, and the authoritative manifest contain the same probe batch hash.

## F5.1 exit criteria

F5.1 is complete when the generated report passes on three physical machines and the evidence directory is retained for the later F5.2 test.

Do not disconnect B as part of F5.1. F5.2 starts from this verified normal state and deliberately tests physical primary loss, promotion, continued writes, and stale-primary fencing.
