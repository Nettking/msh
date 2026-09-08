# C03 public journal continuity

The configured Federation v1 release runtime commits a complete product operation
to the fixed voter quorum before acknowledging it. The operation includes its
authority commands, exact public event rows, and encrypted private pairing/request
receipts. A successor can therefore replay the same event IDs, revisions, actors,
timestamps, and payloads to a client that retains its existing journal.

The smaller C03 authority-event projection is not the client replay journal.
Automatic leader transitions allocate their public row in the same committed
state change. Follower socket liveness cannot independently append public health
or leadership history. Full capability declarations survive projection; observed
local connectivity remains local.

Configured Flask writers use the authenticated relay that owns the coordinator
view. An explicitly saved/configured relay binding is required. Pairing issuance
uses the authenticated current leader identity; the server does not accept a
caller-supplied leader identity. Session and leadership reads share one committed
database snapshot. Standalone mode retains its existing path.

## Atomicity and recovery

One outer SQLite stage contains the entire operation, including both halves of
pairing-material issuance. Nested failures invalidate the entire stage. A durable
outbox, `product_journal_pending.sqlite3`, retains exact proposed command bytes
before replication, including event identities and encryption nonces. Retrying an
uncertain proposal does not generate a competing revision or reset a client.

A replicated log receipt alone does not prove domain acceptance. Successful
product outcomes retain the exact command hash in the replicated state. The
runtime checks this proof before acknowledging or clearing a pending operation.
A failed local projection is repaired from committed authority before another
operation can allocate a public revision. Proven overwritten proposals report an
aborted outcome; an outcome outside the retained proof window fails closed.

The existing 128 KiB command, 8 MiB state/snapshot, and 4,096 outcome-receipt
bounds remain in force. The complete journal and encrypted receipt namespace
consume that state budget. This implementation does not claim unbounded journal
retention or an automatic archive/pruning policy. Reaching a bound refuses the
operation; it does not silently drop evidence or weaken replay checks.

## Witnessed legacy cutover

Legacy import first verifies the complete public wire journal against the
authenticated matching witness quorum. Every voter derives the same canonical
import rows. An explicit provenance marker identifies the witnessed prefix;
subsequent migration leader transitions are derived from their actual committed
C03 commands. Initial bootstrap and interrupted migration must preserve exact
command identity when another voter resumes them. An inherited proposal is
settled by a real current-term quorum commit before deriving another leadership
transition. An interrupted multi-chunk import completes its original inactive
prefix before a successor appends new public leadership history.

Public witnesses do not contain the original private request receipts or raw
pairing grants. The witnessed prefix keeps all public event fields unchanged;
its internal request index and content hash become deterministic import values.
Any existing local prefix must match every public field before those two internal
columns can be reconciled. Later rows cannot use this migration exception.

After certified initialization, the committed private namespace is authoritative.
Unrepresented legacy enrollment tokens, invitations, join-request receipts, and
accepted-request receipts are invalidated in the coordinator view, including on
an old voter that later returns. Obtain fresh pairing grants and use new request
IDs after this cutover. Public witnesses never manufacture private receipts.

A witnessed legacy capability lacking complete versioned metadata retains its
identity and owner but remains unavailable until its owner publishes a complete
announcement. Import does not invent execution readiness.

Human login credentials use their separate certified credential snapshot and
restore contract. This cutover does not modify that database, password salt,
Recorder measurements, storage volumes, or acceptance evidence.

## Backup and qualification

Include the actual configured coordinator/replica databases and the sibling
`product_journal_pending.sqlite3` with the existing identity, transport secret,
replay guard, and private human-credential snapshot state. Quiesce all owners
before a coordinated backup and prove isolated restore; a partial database copy
is not an accepted backup. Each voter requires its own state directory because
private sidecars have fixed sibling names.

Relevant software regressions cover exact client replay after automatic quorum
failover, follower lifecycle behavior, separate-process Flask writes, request
deduplication, consumed grants, nested rollback, pending proposal recovery,
witnessed migration, and snapshot validation. Passing those tests is software
evidence only. P01–P12, B01–B09, CF7 physical checks, real duration measurements,
and strict validation against the final merged candidate remain mandatory.
