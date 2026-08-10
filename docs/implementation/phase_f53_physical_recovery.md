# Phase F5.3: reproducible physical recovery and replica reinstatement

F5.3 begins with the retained, passed F5.2 deployment and proves that the
existing F4.1 and F4.2 production paths restore normal redundancy after the
physical primary has returned.

```text
Machine A  relay + coordinator + storage authority
Machine B  returning former primary, initially unassigned and behind
Machine C  promoted primary, remains primary throughout F5.3
```

F5.3 ends in normal redundant operation:

```text
Machine C: primary
Machine B: replica
Policy:    ONE_REPLICA
Degraded:  none
```

Leadership does not move back to Machine B. Machine C keeps the F5.2 grant,
term, and fencing token. F5.3 does not add direct peer transport, capability
scheduling, workload partitioning, or any Phase F6 behavior.

The F5.3 evidence CLI is:

```text
python -m catalog.node.storage_recovery_drill <command>
```

Recovery continues to use the production coordinators:

- `LiveFormerPrimaryCatchupCoordinator` from F4.1;
- `LiveFormerPrimaryReinstatementCoordinator` from F4.2.

No F5.3 command directly edits assignment, grant, acknowledgement, degraded,
manifest, catch-up, reinstatement, relay, or capability database rows.

## Retained F5.2 inputs

Keep the complete F5.1 and F5.2 evidence directories on Machine A. F5.3
requires these public F5.2 files:

```text
f51/deployment.json
f52/f52-report.json
f52/evidence-authority.json
f52/evidence-promoted-primary.json
f52/evidence-former-primary.json
```

The F5.2 report must contain:

```json
{"schema": "fcp.storage_physical_failover_report.v1", "passed": true}
```

The retained evidence must still show:

- Machine C as primary;
- no assigned replica;
- acknowledgement mode `primary`;
- degraded reason `automatic-failover-redundancy-lost`;
- Machine B connected with its stable identity and role `unassigned`;
- Machine B missing the post-failover batch;
- the F5.2 promoted grant, term, fencing token, and manifest identity.

Do not modify these files. They are the immutable baseline for the final F5.3
report.

## Security and process boundary

Physical deployments require `wss://`. Plain `ws://` remains loopback-test-only.

The authority state directory contains one permanent node identity. Never run
two authority clients using that state directory at the same time. The safe
ownership sequence is:

1. the F5.2 failover coordinator owns the authority connection while normal
   monitoring is active;
2. stop it cleanly before the F5.3 recovery command;
3. the recovery command owns the authority identity while catch-up and
   reinstatement run;
4. after recovery exits, restart the failover coordinator before restarting
   Machine C, because a cached primary requires a fresh signed-control refresh;
5. stop the coordinator again before separate probe or authority-capture
   commands.

Do not set `FCP_ENROLLMENT_TOKEN` or `FCP_SESSION_INVITATION` during F5.3.
Machine B, Machine C, and the authority must use their retained identities and
session memberships. Never place tokens, passwords, private keys, TLS keys, or
state directories in the evidence folder.

## Process placement

### Machine A

Run:

- the relay server and its durable relay-control database;
- the F5.2/F5.3 authority state directory;
- the storage control, publication, acknowledgement, failover, and catch-up
  databases;
- either the failover coordinator or one F5.3 authority command, never both at
  once.

### Machine B

Run only the existing live storage agent with the retained F5.1 configuration
and state. At F5.3 entry it must be `unassigned` and behind the authoritative
manifest.

### Machine C

Run only the existing live storage agent with the retained F5.1 configuration
and state. It remains the active primary throughout F5.3.

Neither storage machine opens an inbound application listener. All application,
catch-up, report, and control traffic continues through the relay.

## 1. Quiesce application writes

Pause external writers before validating or starting recovery. F4.2 can roll
back safely when the manifest advances during admission, but the physical F5.3
drill must be deterministic. Do not resume application writes until the
post-recovery probe in step 7.

Leave Machine B and Machine C connected. Verify locally that B is `unassigned`
and C is `primary`.

## 2. Validate the retained F5.2 baseline on Machine A

```bash
python -m catalog.node.storage_recovery_drill validate-baseline \
  --deployment f51/deployment.json \
  --f52-report f52/f52-report.json \
  --f52-authority f52/evidence-authority.json \
  --f52-promoted-primary f52/evidence-promoted-primary.json \
  --f52-former-primary f52/evidence-former-primary.json
```

Do not proceed unless the command exits with status `0`.

The validator fails closed for a foreign deployment, a non-passing report,
missing F5.2 evidence, a changed promoted grant, a missing degraded state,
Machine B already assigned, or any contradiction between the F5.2 report and
its authority/storage evidence.

## 3. Stop the failover coordinator on Machine A

Stop the F5.2 `catalog.node.storage_failover ... run` process cleanly with
`Ctrl+C`. Keep the relay running. Keep both storage agents running.

This releases the permanent authority identity for the recovery command.

## 4. Run production catch-up and reinstatement on Machine A

Use the same durable relay, storage-control, publication, and failover databases
used by F5.2. Add one persistent F5.3 catch-up database:

```bash
python -m catalog.node.storage_recovery_drill recover \
  --deployment f51/deployment.json \
  --f52-report f52/f52-report.json \
  --f52-authority f52/evidence-authority.json \
  --f52-promoted-primary f52/evidence-promoted-primary.json \
  --f52-former-primary f52/evidence-former-primary.json \
  --relay-control-database f51/authority/relay-control.sqlite3 \
  --storage-control-database f51/authority/storage-control.sqlite3 \
  --failover-database f52/authority/failover.sqlite3 \
  --catchup-database f53/authority/catchup.sqlite3 \
  --publication-database f51/authority/publications.sqlite3 \
  --authority-state-dir f51/authority/state \
  --catchup-limit 100 \
  --output f53/evidence-recovery-operation.json
```

The command first runs F4.1 catch-up while Machine B remains unassigned. It:

1. locates the published F5.2 failover;
2. binds the repair to Machine C's active grant and the retained F5.2 manifest;
3. requests an authenticated report from Machine B;
4. reads only missing batches from Machine C through `fcp-storage-v1`;
5. writes them through the coordinator-authorized recovery route;
6. verifies every immutable batch identity and content hash;
7. persists a fresh synchronized final report.

It then runs F4.2 reinstatement. It:

1. adds Machine B as a replica while policy remains `PRIMARY`;
2. publishes signed control to B and C;
3. requests fresh assigned-replica integrity evidence;
4. atomically restores `ONE_REPLICA`, clears only the matching degraded state,
   and persists `control-restored`;
5. publishes final signed control and persists `completed`.

The public output must contain:

```json
{"schema": "fcp.storage_physical_recovery_operation.v1", "status": "completed"}
```

`retryable` or `operator-attention` is not a passing F5.3 result. Re-run the same
command with the same databases after correcting a transient condition. The
operation is restart-safe and idempotent.

## 5. Verify the live roles

On Machine C:

```bash
python -m catalog.node.live_storage_agent \
  --config f51/storage-node.json \
  --control-authority-node-id <authority-node-id> \
  status
```

`storage-main` must show role `primary`.

On Machine B:

```bash
python -m catalog.node.live_storage_agent \
  --config f51/storage-node.json \
  --control-authority-node-id <authority-node-id> \
  status
```

`storage-main` must show role `replica`.

Machine C's grant ID, term, and fencing token must be unchanged from the F5.2
report. Machine B must not become primary.

## 6. Prove durable restart without first-start credentials

Restart the failover coordinator on Machine A using the exact F5.2 command and
databases:

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

Then restart Machine B and Machine C with their original state and configuration.
Do not provide enrollment or invitation credentials:

```bash
python -m catalog.node.live_storage_agent \
  --config f51/storage-node.json \
  --control-authority-node-id <authority-node-id> \
  run
```

Machine C starts from cached primary control, fences itself until it receives a
fresh signed refresh, and then returns as primary. Machine B returns directly as
replica. Both node IDs and provider IDs must remain unchanged.

After both agents are ready, stop the failover coordinator cleanly again. Leave
both storage agents connected for the remaining evidence steps.

## 7. Commit a normal replicated post-recovery write

On Machine A:

```bash
python -m catalog.node.storage_recovery_drill post-recovery-probe \
  --deployment f51/deployment.json \
  --f52-report f52/f52-report.json \
  --f52-authority f52/evidence-authority.json \
  --f52-promoted-primary f52/evidence-promoted-primary.json \
  --f52-former-primary f52/evidence-former-primary.json \
  --authority-state-dir f51/authority/state \
  --control-database f51/authority/storage-control.sqlite3 \
  --acknowledgements-database f51/authority/acks.sqlite3 \
  --dataset-id f53-recovery \
  --batch-id f53-restored-1 \
  --idempotency-key f53:restored:1 \
  --content-json '{"stage":"restored-redundancy","sequence":3}' \
  --output f53/evidence-post-recovery-probe.json
```

The command fails unless:

- Machine C is primary;
- Machine B is the sole replica;
- Machine C retains the F5.2 grant, term, and fencing token;
- acknowledgement mode is `ONE_REPLICA`;
- no degraded state remains;
- the write commits through Machine C;
- exactly one replica acknowledgement is required and received from Machine B;
- the authoritative manifest contains the new batch and content hash.

## 8. Capture storage evidence

Copy the retained public F5.2 files and the post-recovery probe evidence to the
storage machines. Do not copy private authority or storage state.

On Machine C:

```bash
python -m catalog.node.storage_recovery_drill capture-storage \
  --deployment f51/deployment.json \
  --f52-report f52/f52-report.json \
  --f52-authority f52/evidence-authority.json \
  --f52-promoted-primary f52/evidence-promoted-primary.json \
  --f52-former-primary f52/evidence-former-primary.json \
  --role current-primary \
  --config f51/storage-node.json \
  --pre-failure-batch-id <f51-probe-batch-id> \
  --post-failover-batch-id f52-after-1 \
  --post-recovery-batch-id f53-restored-1 \
  --output f53/evidence-current-primary.json
```

On Machine B:

```bash
python -m catalog.node.storage_recovery_drill capture-storage \
  --deployment f51/deployment.json \
  --f52-report f52/f52-report.json \
  --f52-authority f52/evidence-authority.json \
  --f52-promoted-primary f52/evidence-promoted-primary.json \
  --f52-former-primary f52/evidence-former-primary.json \
  --role reinstated-replica \
  --config f51/storage-node.json \
  --pre-failure-batch-id <f51-probe-batch-id> \
  --post-failover-batch-id f52-after-1 \
  --post-recovery-batch-id f53-restored-1 \
  --output f53/evidence-reinstated-replica.json
```

Both machines must contain matching hashes for all three batches. Both evidence
files must show verified signed control and an empty inbound-listener list.

## 9. Capture authority, catch-up, and reinstatement evidence

Copy both storage evidence files back to Machine A. With both storage agents
connected and the failover coordinator stopped, run:

```bash
python -m catalog.node.storage_recovery_drill capture-authority \
  --deployment f51/deployment.json \
  --f52-report f52/f52-report.json \
  --f52-authority f52/evidence-authority.json \
  --f52-promoted-primary f52/evidence-promoted-primary.json \
  --f52-former-primary f52/evidence-former-primary.json \
  --authority-state-dir f51/authority/state \
  --control-database f51/authority/storage-control.sqlite3 \
  --failover-database f52/authority/failover.sqlite3 \
  --catchup-database f53/authority/catchup.sqlite3 \
  --post-recovery-batch-id f53-restored-1 \
  --output f53/evidence-authority.json
```

This public evidence binds:

- the retained published F5.2 failover;
- the complete F4.1 catch-up item ledger and final report;
- the completed F4.2 reinstatement transaction and publications;
- the final assigned-replica assessment;
- Machine C's preserved grant, term, and fencing token;
- the restored assignment and acknowledgement policy;
- absence of degraded state;
- the final authoritative manifest and post-recovery batch;
- relay connections and ready storage capabilities.

## 10. Generate the deterministic F5.3 report

```bash
python -m catalog.node.storage_recovery_drill verify \
  --deployment f51/deployment.json \
  --f52-report f52/f52-report.json \
  --f52-authority f52/evidence-authority.json \
  --f52-promoted-primary f52/evidence-promoted-primary.json \
  --f52-former-primary f52/evidence-former-primary.json \
  --recovery-operation f53/evidence-recovery-operation.json \
  --authority f53/evidence-authority.json \
  --current-primary f53/evidence-current-primary.json \
  --reinstated-replica f53/evidence-reinstated-replica.json \
  --post-recovery-probe f53/evidence-post-recovery-probe.json \
  --output f53/f53-report.json
```

F5.3 passes only when:

```json
{"schema": "fcp.storage_physical_recovery_report.v1", "passed": true}
```

The verifier fails closed for a foreign F5.2 baseline, incomplete catch-up,
stale or foreign manifest evidence, missing synchronized reports, a changed
primary grant, a former primary incorrectly restored as leader, missing replica
assignment, policy not restored, degraded state still present, a final write
without Machine B acknowledgement, inconsistent storage data, unavailable relay
identities, or an inbound storage listener.

## F5.3 exit criteria

Retain the complete `f51`, `f52`, and `f53` public evidence directories. F5.3
is complete only when the report passes and the live state is:

```text
Machine C: primary, using the F5.2 promoted grant
Machine B: replica, fully caught up and eligible
Replicas:  provider-primary
Policy:    ONE_REPLICA
Degraded:  none
```

Application writes may now resume. Do not begin Phase F6 in this runbook.
Repository branch cleanup and establishment of a fresh `main` baseline are a
separate operation after F5.3 has been merged and validated.
