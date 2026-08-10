# Federated Session Network: Deterministic Test Matrix

Status: acceptance tests and reusable test vectors for `docs/federated_session_network.md`.

The scenarios below define expected behavior. Implementation agents should translate them into focused unit tests first and integration tests in later phases. Scenario IDs should be retained in test names or comments so failures can be traced to this matrix.

## Common definitions

A storage node report contains at least:

```text
node_id
session_id
group_id
online
integrity_verified
schema_compatible
required_datasets_present
session_revision
dataset watermarks
missing committed ranges
replication_lag
leader_eligible
current_role
term
lease expiry
fencing token
```

A node is **qualified** only when all required eligibility checks pass. Local uncommitted data never increases authoritative completeness.

Candidate ordering after eligibility:

1. highest authoritative revision;
2. complete required-dataset coverage;
3. highest contiguous committed watermarks;
4. fewest missing committed batches or objects;
5. lowest replication lag;
6. operational stability and capacity;
7. stable node ID as final deterministic tie-breaker.

Sticky leadership overrides equivalent candidate ordering: a healthy, complete current primary remains primary when no candidate is authoritatively better.

## Phase 0: pure domain and policy tests

### F0-001 First qualified storage node

Given no current primary and one qualified node at revision 10, selecting a leader returns that node.

Expected:

- selected role is `primary`;
- term is greater than the previous group term;
- fencing token is greater than the previous fencing token;
- a finite lease is present.

### F0-002 New equivalent node becomes replica

Given node A is a healthy qualified primary at revision 10 and node B joins with the same complete revision and coverage, node A remains primary.

Expected:

- no term increment;
- no fencing-token increment;
- B is eligible but assigned replica;
- reason identifies sticky leadership.

### F0-003 More authoritative revision wins during required handover

Given no valid current lease, node A has revision 10 and node B has revision 11, both complete and verified, node B is selected.

### F0-004 More rows do not beat higher authoritative revision

Given node A reports 1,000,000 local rows at authoritative revision 10 and node B reports 900,000 rows at authoritative revision 11, node B is selected. Row count is not a selection criterion.

### F0-005 Uncommitted local batches do not create authority

Given node A is complete at revision 10 and node B is at revision 9 with 20 local uncommitted batches, A remains preferred.

### F0-006 Missing committed range disqualifies candidate

Given node A has revision 12 but is missing committed sequence 500-600 and node B has revision 12 with no missing ranges, B is selected.

### F0-007 Integrity failure disqualifies candidate

A node with `integrity_verified=false` cannot be primary even if its reported revision is highest.

### F0-008 Schema incompatibility disqualifies candidate

A node using an unsupported storage protocol major version or incompatible required schema cannot be primary.

### F0-009 Required dataset missing

A node that has telemetry but lacks a required operator-knowledge dataset is not a full replica and cannot lead that storage group.

### F0-010 Complementary partial datasets have no automatic winner

Given A has complete telemetry but incomplete audio, and B has complete audio but incomplete telemetry, neither can become primary for a group requiring both.

Expected result: `storage-degraded` or synchronization-required, not an arbitrary leader.

### F0-011 Stable tie-breaker

Given two eligible candidates with identical authoritative state and no current primary, selection is deterministic by stable node ID. Reordering input candidates does not change the result.

### F0-012 Current primary behind manifest

A current primary whose local committed revision is behind the authoritative session manifest is no longer eligible. A complete replica may replace it after lease invalidation.

### F0-013 Expired lease rejects write

A write carrying an otherwise current term and fencing token is rejected after lease expiry.

### F0-014 Old term rejects write

After term 15 is granted, a write using term 14 is rejected.

### F0-015 Old fencing token rejects write

After fencing token 9815 is active, a write using token 9814 is rejected, even when the sender was previously primary.

### F0-016 Future or unrelated token rejects write

A token not issued for the same session, storage group, node, and term is rejected.

### F0-017 Serialization round trip

Every Phase 0 model serializes to a JSON-compatible dictionary and reconstructs an equal validated model. Unknown required fields, invalid enums, malformed IDs, negative revisions, and invalid date/time values fail clearly.

### F0-018 Additive optional fields

Unknown optional fields from the same protocol major version may be retained or ignored according to the chosen compatibility policy without breaking required-field validation.

### F0-019 Protocol major mismatch

An unsupported major version is rejected with a structured compatibility error.

### F0-020 Clock skew does not determine authority

Wall-clock timestamp alone never outranks authoritative revision, term, fencing token, or committed watermarks.

### F0-021 Candidate-local coverage does not increase authority

Given node A exactly matches the authoritative required-dataset revision,
watermark, and hash, while node B reports the same authoritative node revision
and hash but a higher dataset revision or watermark, node B is rejected as
`required-dataset-authoritatively-ahead`. Local or provisional coverage does not
improve leadership rank.

### F0-022 New grant requires authoritative fencing state

Given a qualified candidate but no active/last scoped coordinator grant and no
persisted authoritative fencing counter, selection returns
`authoritative-state-required`. It does not select a node or propose a term,
fencing token, or lease. Supplying the persisted counter allows the next token
to increment monotonically.

## Phase 1: local interface and outbox tests

### F1-001 Existing recorder output compatibility

Writing one synthetic MTConnect batch through the filesystem provider produces the same raw, observation, normalized, and checkpoint semantics as the current recorder store.

### F1-002 Atomic outbox enqueue

A local data change and corresponding outbox entry are either both durably visible after restart or neither is.

### F1-003 Pending entries survive restart

Unacknowledged entries remain pending after process restart.

### F1-004 Acknowledgement is idempotent

Acknowledging an already completed outbox item does not duplicate side effects or fail.

### F1-005 Retry metadata

A failed delivery increments attempts, records a bounded error summary, and schedules retry with bounded exponential backoff.

### F1-006 Content mismatch on reused idempotency key

Reusing an idempotency key with a different content hash is rejected as a conflict.

### F1-007 Multiple capabilities of one type

The local registry stores and returns two storage providers and two language-model providers without overwriting one another.

### F1-008 Capability removal

Removing one provider does not remove other providers of the same type.

## Phase 2: relay-first network tests

### F2-001 Different networks join one session

Two nodes with no shared private subnet connect using outbound connections, join the same session, and exchange an authenticated test message through relay.

### F2-002 No port forwarding required

Neither node exposes an inbound public service port for the test.

### F2-003 Ordered event replay

A disconnected node at `last_applied_revision=40` reconnects and receives revisions 41 onward exactly once in order.

### F2-004 Duplicate event delivery

Receiving the same session event twice applies it once logically.

### F2-005 Revision gap

Receiving revision 43 when revision 42 is missing triggers replay/resynchronization, not silent application.

### F2-006 Revoked node

A revoked node's connection or session message is rejected, and the action is auditable.

### F2-007 Wrong-session message

A valid node cannot route a message into a session it has not joined.

### F2-008 Capability heartbeat expiry

A provider whose heartbeats expire becomes unavailable but its persistent identity and prior audit records remain.

### F2-009 Coordinator restart

After restart, the coordinator reconstructs sessions, memberships, event order, and active non-expired leases from durable state.

### F2-010 Relay interruption

Nodes reconnect with backoff and resume event synchronization without creating duplicate session events.

## Phase 3: storage and replication tests

### F3-001 Idempotent batch ingest

Delivering the same batch ID, idempotency key, and content hash twice creates one logical committed batch and returns the same commit result.

### F3-002 Conflicting duplicate

The same batch ID or idempotency key with different content hash is rejected.

### F3-003 Primary-only write authority

A replica cannot accept a globally committed write without a valid coordinator grant.

### F3-004 Primary plus one acknowledgement

With policy `write_acknowledgement=2`, the recorder receives commit success only after primary and one replica have durably acknowledged.

### F3-005 Replica offline

With insufficient acknowledgements, the recorder retains the local batch and the write remains pending rather than falsely committed.

### F3-006 Controlled handover

A healthy primary drains writes, catches the target replica up, receives a new grant for the target, and routing moves without accepting stale-primary writes.

### F3-007 Old primary reconnects

A former primary reconnects using an old term/token, is rejected for writes, and rejoins as a replica.

### F3-008 Replication interruption and resume

A large or multi-record batch interrupted during replication resumes or safely restarts without a duplicate logical commit.

### F3-009 Logical routing

A recorder addresses the session storage group, not a physical IP. A leadership change requires no recorder configuration change.

### F3-010 Provider heterogeneity

A filesystem provider and PostgreSQL provider exchange the same versioned FCP batch contract without leaking backend-specific details to the recorder.

## Phase 4: completeness-aware failover tests

### F4-001 Complete replica promotion

After primary lease expiry, the available complete verified replica with the highest committed state is promoted.

### F4-002 Incomplete replica never silently promoted

When every available replica has missing committed data, the group enters `storage-degraded`.

### F4-003 Hash corruption

A replica with a mismatched committed object or batch hash is ineligible until repaired.

### F4-004 Missing ranges repaired before promotion

A candidate receives missing committed batches, verifies hashes, updates its manifest state, and only then becomes eligible.

### F4-005 Primary failure before replica acknowledgement

A batch stored only by the failed primary is not represented as quorum-committed. The recorder's retained local copy can safely retry.

### F4-006 Primary failure after required acknowledgement

A batch acknowledged according to policy exists on enough surviving nodes to remain committed after failover.

### F4-007 Manifest monotonicity

Authoritative manifest revision never decreases, including after coordinator restart or leader handover.

### F4-008 Per-group completeness

A node may lead telemetry storage while another leads audio/object storage. Completeness is evaluated independently per storage group.

## Phase 5: direct transport tests

### F5-001 Direct preferred

When a direct encrypted stream is available, data uses it while logical addressing remains unchanged.

### F5-002 Relay fallback

When direct establishment fails, the same operation succeeds through relay without application reconfiguration.

### F5-003 Relay fails while direct survives

An established direct stream continues when relay becomes unavailable, while control-plane limitations are surfaced accurately.

### F5-004 Chunk verification

A corrupt chunk is rejected and retransmitted; the final object hash must match before commit.

### F5-005 Resumable transfer

An interrupted large-object transfer resumes from a verified cursor rather than restarting blindly or committing a partial object.

## Phase 6: other capability tests

### F6-001 Multiple AI providers

Two AI providers remain simultaneously registered and a scheduler selects according to explicit policy rather than primary/replica semantics.

### F6-002 Job ownership

One compute job has one active owner at a time, while several workers may be available.

### F6-003 Worker loss

A lost worker causes retry or reassignment according to job policy without duplicating an externally visible committed result.

### F6-004 Capability-specific authorization

A node authorized to provide compute cannot automatically read restricted storage datasets.

## Required negative assertions

Tests should explicitly demonstrate that the system does **not**:

- select a leader by row count;
- allow self-promotion after a connectivity failure;
- accept writes with expired leases or stale fencing tokens;
- mark under-replicated writes as fully committed;
- expose backend credentials through capability announcements;
- use a physical IP as the durable session storage identity;
- treat all capability types as primary/replica groups;
- delete local recorder batches before acknowledgement policy is satisfied;
- continue silently when manifest or revision gaps are detected.
