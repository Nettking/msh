# Phase F5.2: reproducible physical primary failover

F5.2 begins with the retained, passed F5.1 three-machine deployment and proves
that the existing F3 production failover path works when the physical primary
disappears.

```text
Machine A  relay + coordinator + storage authority
Machine B  original storage primary
Machine C  original storage replica, then promoted primary
```

F5.2 ends deliberately degraded. Machine C is primary, no active replica is
assigned, acknowledgement mode is `PRIMARY`, and the durable degraded-state
obligation remains present. Machine B reconnects with its original identity but
remains `unassigned`, fenced, and behind the post-failover manifest. Catch-up,
replica reassignment, restoration of `ONE_REPLICA`, and degraded-state cleanup
belong to F5.3 and must not be performed here.

The F5.2 evidence CLI is:

```text
python -m catalog.node.storage_failover_drill <command>
```

The actual promotion continues to use:

```text
python -m catalog.node.storage_failover ... run
```

No F5.2 command edits assignment, grant, capability, relay, manifest, or
failover database records directly.

## Retained F5.1 inputs

Keep the following F5.1 public files on Machine A:

```text
f51/deployment.json
f51/f51-report.json
f51/evidence-authority.json
```

Keep each machine's private persistent state in its original location. Do not
copy Machine B's state to Machine C or vice versa.

The baseline report must contain:

```json
{"schema": "fcp.storage_three_machine_report.v1", "passed": true}
```

The baseline authority evidence must still show:

- Machine B as primary;
- Machine C as the sole replica;
- acknowledgement mode `one-replica`;
- no degraded state;
- the original grant, term, fencing token, and authoritative manifest identity.

## Security and process boundary

Physical deployments continue to require `wss://`. Plain `ws://` is accepted
only by explicit loopback tests.

The authority state directory contains one permanent node identity. Do not run
two authority clients against that state directory at the same time. In this
runbook:

1. the failover coordinator owns the authority connection during detection,
   promotion, and Machine B's refresh after reconnect;
2. stop the coordinator cleanly before running a separate probe, stale-grant
   check, or authority-capture command;
3. restart the coordinator only when Machine B needs its current signed control.

No enrollment or session-invitation token is required after F5.1. Do not put
credentials, TLS private keys, or state directories in the evidence folder.

## 1. Validate the retained baseline on Machine A

Use the same FCP commit on all three machines. Then run:

```bash
python -m catalog.node.storage_failover_drill validate-baseline \
  --deployment f51/deployment.json \
  --baseline-report f51/f51-report.json \
  --baseline-authority f51/evidence-authority.json
```

Do not proceed unless the command exits with status `0`.

Leave the F5.1 storage agents running on B and C:

```bash
python -m catalog.node.live_storage_agent \
  --config f51/storage-node.json \
  --control-authority-node-id <authority-node-id> \
  run
```

Machine B must still be primary and Machine C must still be replica before the
failure is injected.

## 2. Start the failover coordinator on Machine A

Use the same authority state, relay control database, storage control database,
and publication database used by F5.1. Use a durable F5.2 failover database:

```bash
python -m catalog.node.storage_failover \
  --relay-control-database f51/authority/relay-control.sqlite3 \
  --storage-control-database f51/authority/storage-control.sqlite3 \
  --publication-database f51/authority/publications.sqlite3 \
  --failover-database f52/authority/failover.sqlite3 \
  --state-dir f51/authority/state \
  --relay wss://relay.example.org:8765 \
  --display-name "Storage authority" \
  --session-id <session-id> \
  --scan-interval 2 \
  --lease-seconds 300 \
  run
```

Keep this process running. It reads the real relay/coordinator status, requests
a fresh manifest-bound integrity report from assigned replicas, and invokes the
existing `StorageFailoverCoordinator` transaction.

## 3. Deliberately disconnect Machine B

Disconnect only Machine B. Accepted physical failure injections include:

- stopping the live storage-agent process;
- disabling Machine B's network connection;
- powering Machine B off.

Do not revoke the node, change capability rows, edit SQLite files, change the
assignment, expire the grant manually, or stop Machine C.

The coordinator must observe Machine B as disconnected or its storage
capability as unavailable through the relay. It must then request fresh proof
from Machine C and promote Machine C.

## 4. Verify that Machine C was promoted

After at least one complete scan and report round, inspect Machine C's public
local status:

```bash
python -m catalog.node.live_storage_agent \
  --config f51/storage-node.json \
  --control-authority-node-id <authority-node-id> \
  status
```

Proceed only when `storage-main` is shown with role `primary`.

Stop the failover coordinator cleanly with `Ctrl+C`. This releases the
permanent authority identity for the next command. Do not stop Machine C.

## 5. Commit a post-failover write on Machine A

Run one write through the normal logical storage route:

```bash
python -m catalog.node.storage_failover_drill post-failover-probe \
  --deployment f51/deployment.json \
  --baseline-report f51/f51-report.json \
  --baseline-authority f51/evidence-authority.json \
  --authority-state-dir f51/authority/state \
  --control-database f51/authority/storage-control.sqlite3 \
  --acknowledgements-database f51/authority/acks.sqlite3 \
  --dataset-id f52-failover \
  --batch-id f52-after-1 \
  --idempotency-key f52:after:1 \
  --content-json '{"stage":"after-failover","sequence":2}' \
  --output f52/evidence-post-failover-probe.json
```

The command fails unless:

- Machine C is the active primary;
- no replica is assigned;
- acknowledgement mode is explicitly `PRIMARY`;
- degraded reason is `automatic-failover-redundancy-lost`;
- the batch commits and appears in the authoritative manifest;
- zero replica acknowledgements are required or recorded.

The output is public and contains only deployment identifiers, batch identity,
content hash, acknowledgement result, and manifest identity.

## 6. Restart the coordinator, then reconnect Machine B

Restart the same failover-coordinator command from step 2. It must use the same
F5.2 failover database.

Start Machine B with its original state and configuration, but with no
enrollment or session-invitation environment variables:

```bash
python -m catalog.node.live_storage_agent \
  --config f51/storage-node.json \
  --control-authority-node-id <authority-node-id> \
  run
```

Machine B starts from cached primary control, closes its local write gate,
announces itself unavailable, and asks the pinned authority for current signed
control. The coordinator publishes the current plan. Machine B must retain its
original node ID and become `unassigned`.

Verify Machine B's local status:

```bash
python -m catalog.node.live_storage_agent \
  --config f51/storage-node.json \
  --control-authority-node-id <authority-node-id> \
  status
```

Proceed only when `storage-main` is shown with role `unassigned`.

Stop the failover coordinator cleanly again before the remaining authority-side
commands. Leave both storage agents connected.

## 7. Prove that Machine B's old authority is rejected

On Machine A, send one mutation carrying the original F5.1 grant to Machine B:

```bash
python -m catalog.node.storage_failover_drill stale-authority-probe \
  --deployment f51/deployment.json \
  --baseline-report f51/f51-report.json \
  --baseline-authority f51/evidence-authority.json \
  --authority-state-dir f51/authority/state \
  --batch-id f52-stale-1 \
  --idempotency-key f52:stale:1 \
  --content-json '{"stale":true}' \
  --output f52/evidence-stale-authority.json
```

The command exits successfully only when the write is rejected. Accepted strict
errors are `not-primary`, `stale-term`, `stale-fencing-token`, `lease-expired`,
or `unknown-grant`. An accepted write is a hard F5.2 failure.

## 8. Capture storage evidence

Copy `f51/deployment.json`, `f51/f51-report.json`, and
`f51/evidence-authority.json` to the storage machines. These files contain no
credentials.

On Machine C:

```bash
python -m catalog.node.storage_failover_drill capture-storage \
  --deployment f51/deployment.json \
  --baseline-report f51/f51-report.json \
  --baseline-authority f51/evidence-authority.json \
  --role promoted-primary \
  --config f51/storage-node.json \
  --pre-failure-batch-id <f51-probe-batch-id> \
  --post-failover-batch-id f52-after-1 \
  --output f52/evidence-promoted-primary.json
```

On Machine B:

```bash
python -m catalog.node.storage_failover_drill capture-storage \
  --deployment f51/deployment.json \
  --baseline-report f51/f51-report.json \
  --baseline-authority f51/evidence-authority.json \
  --role former-primary \
  --config f51/storage-node.json \
  --pre-failure-batch-id <f51-probe-batch-id> \
  --post-failover-batch-id f52-after-1 \
  --output f52/evidence-former-primary.json
```

The expected data split at the F5.2 exit point is intentional:

- Machine C contains the pre-failure and post-failover batches;
- Machine B contains the pre-failure batch but not the post-failover batch.

Do not copy the missing batch to Machine B.

## 9. Capture authority and durable failover evidence

Copy both storage evidence files back to Machine A. With both storage agents
still connected and the failover coordinator stopped, run:

```bash
python -m catalog.node.storage_failover_drill capture-authority \
  --deployment f51/deployment.json \
  --baseline-report f51/f51-report.json \
  --baseline-authority f51/evidence-authority.json \
  --authority-state-dir f51/authority/state \
  --control-database f51/authority/storage-control.sqlite3 \
  --failover-database f52/authority/failover.sqlite3 \
  --post-failover-batch-id f52-after-1 \
  --output f52/evidence-authority.json
```

This evidence binds:

- original and promoted providers and nodes;
- old and new grants, terms, and fencing tokens;
- the selected report revision and hash;
- the report's pre-failure manifest revision and hash;
- the published durable failover transaction;
- current assignment, policy, degraded state, and manifest;
- the post-failover batch identity;
- relay connections and storage capabilities.

## 10. Generate the deterministic F5.2 report

On Machine A:

```bash
python -m catalog.node.storage_failover_drill verify \
  --deployment f51/deployment.json \
  --baseline-report f51/f51-report.json \
  --baseline-authority f51/evidence-authority.json \
  --authority f52/evidence-authority.json \
  --promoted-primary f52/evidence-promoted-primary.json \
  --former-primary f52/evidence-former-primary.json \
  --post-failover-probe f52/evidence-post-failover-probe.json \
  --stale-authority-probe f52/evidence-stale-authority.json \
  --output f52/f52-report.json
```

F5.2 passes only when:

```json
{"schema": "fcp.storage_physical_failover_report.v1", "passed": true}
```

The verifier fails closed for foreign deployment IDs, unchanged term or fencing
token, absent or stale selected replica evidence, missing degraded state,
incorrect assignment, a former primary that still holds authority, missing
post-failover data on Machine C, unexpected catch-up on Machine B, accepted old
authority, missing relay identity, or an inbound storage listener.

## F5.2 exit criteria

Retain the complete `f51` and `f52` public evidence directories. F5.2 is
complete only when the report passes and the live state remains:

```text
Machine C: primary
Machine B: unassigned and fenced
Replicas:  none
Policy:    PRIMARY
Degraded:  automatic-failover-redundancy-lost
```

Stop here. Do not catch Machine B up, add it as a replica, restore
`ONE_REPLICA`, clear the degraded state, or begin any F5.3 operation.
