# Recorder-to-Federation delivery design

| Metadata | Value |
| --- | --- |
| Status | Proposed reference design |
| Audience | Recorder, node-client, and Federation storage maintainers |
| Scope | Add Federation delivery without changing local MTConnect capture semantics |
| Authority | Informative; active Federation plans still govern implementation order and product behavior |
| Reviewed | 2026-08-09 UTC |

## Recommendation

Keep recording and Federation delivery as two independently restartable pipelines:

```text
MTConnect Agent
    -> existing capture and validation
    -> immutable raw + observations + JSONL
    -> existing local checkpoint                 (capture commit)
    -> committed-batch reconciler
    -> durable delivery outbox
    -> logical Federation storage client
    -> primary/replica acknowledgement policy   (Federation commit)
```

The recorder must continue to consider a batch captured when its existing local
writes and checkpoint succeed. Federation availability must never delay polling
the Agent, consume the Agent's finite-buffer safety margin, or move the local
checkpoint backward. Delivery is a second commitment with its own cursor and
status.

This design uses the facilities that already exist:

- `DurableRecorderStore.store_batch()` preserves the intentional raw,
  observation, compatibility-JSONL write order;
- the recorder advances its checkpoint only after those writes succeed;
- `SQLiteOutbox` already supplies durable idempotent retry state; and
- `DurableRecorderDeliveryQueue` and `PhaseDLogicalStorageClient` already retain
  a batch until the configured remote acknowledgement policy reports commit and
  resolve the current storage authority dynamically.

The missing part is therefore an integration adapter, not a recorder rewrite.

## Required invariants

1. **Local capture is primary.** A disconnected, unjoined, degraded, or
   reconfiguring Federation cannot stop local recording.
2. **Only locally committed batches are publishable.** A delivery candidate is
   eligible only when its sequence range is covered by the recorder checkpoint
   for the same source and Agent instance.
3. **The local archive is retained through remote commit.** Federation delivery
   acknowledges the outbox item; it does not delete raw XML, normalized data,
   probes, gaps, or events.
4. **Delivery is idempotent.** Reconciliation and retries may repeat after any
   crash without creating another logical batch.
5. **Routing remains logical.** The recorder stores a session and storage group,
   never a physical provider address, grant, term, or fencing token.
6. **Consent and authority remain explicit.** Recording does not imply sharing.
   Delivery starts only for an enabled recorder contribution with a selected
   session, dataset policy, and storage group.
7. **A remote failure is observable, not lossy.** Backlog size, oldest pending
   age, last committed batch, last error, and retry time belong in recorder
   status separately from capture health.

## Batch contract

Publish the detailed observation representation as the canonical telemetry
dataset. Keep the wide JSONL files locally for current workbench compatibility,
and send raw XML and probe documents as optional provenance objects rather than
duplicating all three representations into the telemetry stream.

Use stable identities derived only from durable facts:

| Field | Proposed value |
| --- | --- |
| Dataset ID | `mtconnect:<recorder-node-id>:<source-slug>` |
| Dataset schema | `msh.mtconnect.observations` version `1` |
| Batch ID | `<source-slug>:<agent-instance-id>:<first-sequence>:<last-sequence>:<raw-sha256>` |
| Idempotency key | `<session-id>:<dataset-id>:<batch-id>` |
| Created time | recorder receipt time from the raw manifest |
| Content hash | canonical hash calculated by `BatchIngestRequest` |

Each telemetry content envelope should contain source identity, Agent instance,
sequence bounds, probe hash, raw hash, observation count, and observations. Gaps
and Agent lifecycle events should use separate versioned dataset schemas so a
consumer cannot mistake a discontinuity for ordinary telemetry.

The current SQLite outbox caps an inline JSON payload at 1 MiB. The adapter must
not assume an MTConnect batch fits. It should deterministically split normalized
observations at record boundaries into sub-batches below the configured safe
limit. Large immutable raw/probe artifacts should use the Federation's
resumable object-transfer path, with their hashes referenced by the telemetry
envelope. Do not raise the SQLite limit and place arbitrary compressed files in
the control-plane database.

## Commit and crash recovery

The filesystem recorder checkpoint and SQLite outbox cannot share one atomic
transaction. Avoid pretending they can. Add a reconciler that scans deterministic
raw manifests and compares them with the committed recorder checkpoint:

1. capture and store the batch exactly as today;
2. commit the existing recorder checkpoint exactly as today;
3. wake the reconciler (an optimization only);
4. enumerate manifests whose sequence range is covered by that checkpoint;
5. construct deterministic sub-batches and enqueue them idempotently;
6. deliver due entries without holding the capture lock;
7. mark an entry complete only after `PhaseDIngestOutcome.committed` is true.

If the process dies between steps 2 and 5, the next reconciliation discovers the
batch. If it dies during delivery, the pending outbox entry is retried. If the
remote side committed but the local acknowledgement was not saved, the same
idempotency key makes the retry safe. A manifest beyond the checkpoint is ignored
until replay/recovery finishes and the checkpoint covers it.

Reconciliation should use a durable high-water hint per source for efficiency,
but manifests and the capture checkpoint remain the recovery truth. It must also
detect an idempotency conflict as a quarantined integrity error rather than
silently generating a new key.

## Runtime ownership

Put the adapter in the node runtime, or in a dedicated delivery worker sharing
the mounted data directory, rather than in the MTConnect fetch thread. It needs:

- read-only access to recorder archives and checkpoints;
- write access to its own outbox and publication cursor database;
- the existing authenticated outbound Federation client;
- current session, contribution-consent, dataset, and logical-storage-group
  configuration; and
- bounded CPU, memory, delivery concurrency, and bandwidth.

When delivery falls behind, prioritize capture. Apply backpressure by slowing
Federation sends, never by stopping Agent reads. Operators may pause sharing
without pausing recording, then resume from the durable backlog.

## Status and controls

Expose capture and delivery as distinct states:

```json
{
  "capture": {"state": "recording", "next_sequence": 12001},
  "federation_delivery": {
    "enabled": true,
    "state": "backlogged",
    "pending_batches": 14,
    "oldest_pending_at": "2026-08-09T10:00:00+00:00",
    "last_committed_batch_id": "mazak:42:10901:11000:...",
    "last_error": "storage acknowledgement is pending"
  }
}
```

Required controls are enable sharing, pause delivery, resume delivery, select
session/group/dataset policy, and retry now. Disabling contribution stops new
network delivery but preserves the outbox unless the operator separately and
explicitly discards it. Source URL credentials and raw authorization material
must not appear in envelopes, status, or logs.

## Incremental implementation

1. **Reconciler and packager:** discover only checkpoint-covered manifests,
   produce deterministic bounded telemetry sub-batches, and enqueue without a
   network dependency.
2. **Delivery worker:** connect the existing durable recorder queue to the node's
   authenticated logical storage client and run retries independently of capture.
3. **Configuration and status:** require explicit sharing consent and publish
   separate delivery health/backlog fields.
4. **Provenance objects:** add optional raw XML and probe transfer by content hash
   after telemetry delivery is reliable.
5. **Operational hardening:** quota alerts, bandwidth limits, retention policy,
   corrupt-file quarantine, credential redaction, and restart/upgrade recovery.

## Acceptance scenarios

- Record continuously while the relay and every storage provider are offline;
  local checkpoints advance and the delivery backlog grows.
- Restore the Federation; the backlog drains in order and every logical batch is
  committed once despite repeated requests.
- Kill the process after local checkpoint commit but before enqueue; restart
  discovers and delivers the batch.
- Kill it after remote commit but before local outbox acknowledgement; restart
  retries without producing a duplicate.
- Change storage primary while batches are pending; logical routing uses the new
  authority without editing queued recorder data.
- Feed a normalized batch larger than 1 MiB; deterministic sub-batches all fit
  the outbox limit and preserve contiguous sequence coverage.
- Restart the MTConnect Agent and record a buffer-overflow gap; instance identity
  changes and the gap is delivered explicitly.
- Pause or revoke recorder sharing while capture continues; no new delivery is
  attempted and no local archive is removed.

## Rejected alternatives

- **Send inside `capture_source()`:** couples polling latency and Agent buffer
  loss risk to network/storage availability.
- **Advance the recorder checkpoint only after Federation commit:** turns an
  optional distributed dependency into the local capture commit boundary.
- **Tail JSONL alone:** loses raw hashes, probe linkage, gap semantics, and the
  detailed one-observation-per-sequence representation.
- **Address a storage node directly:** bypasses failover, leases, grants, and
  fencing already enforced by logical storage routing.
- **Delete local files after upload:** changes recorder retention behavior and
  makes a remote acknowledgement an unsafe garbage-collection policy.
