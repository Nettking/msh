# Phase F6.5: durable resumable verified object transfer

## Scope

F6.5 extends the encrypted and verified F6.4.2 object-transfer path with durable
restart recovery. It does not change storage authority, leases, fencing,
promotion, replication, relay routing, NAT traversal, or the Go/libp2p
sidecar.

The implementation is exposed as `ResumableChunkTransferEndpoint`. The
existing `VerifiedChunkTransferEndpoint` remains unchanged.

## Durable state

Each endpoint receives an explicit `durable_root`. The endpoint stores:

- an incoming manifest bound to the enrolled source node;
- the contiguous prefix of accepted chunk indexes, hashes, and lengths;
- an outgoing manifest bound to target, request, object, source hash, size,
  and chunk geometry;
- the sender's last acknowledged cursor;
- completion receipts;
- managed source bytes used by `send_bytes()` until completion or cleanup.

Metadata is written as canonical JSON through a temporary file, `fsync`, and
an atomic same-directory replacement. Chunk files retain the atomic write and
`fsync` behavior introduced in F6.4.2.

One active endpoint process owns a durable root. Concurrent processes sharing
the same root are outside the F6.5 contract.

## Restart negotiation

A restarted sender creates a fresh signed stream opening, fresh X25519
ephemeral key, fresh AEAD keys, and fresh frame sequences. It reuses only the
durable logical transfer ID.

The sender transmits the original manifest. The receiver reloads the durable
cursor and returns:

- accepted chunk count;
- remaining chunk count;
- first missing chunk index.

F6.5 requires the accepted indexes to form a contiguous prefix. The sender
resumes at the first missing index. The receiver is authoritative when the
local and remote cursors differ.

This supports:

- sender-process restart while the receiver remains online;
- receiver-process or machine restart while the sender remains online;
- restart of both endpoints;
- loss of the final acknowledgement after publication.

Exact encrypted-frame replay remains rejected. A restart always uses a new
stream opening and new cryptographic material.

## Completion recovery

Completion uses a durable two-phase state:

1. the verified receipt is journaled as `publishing`;
2. the object is atomically published;
3. the journal is changed to `completed`.

After a crash in this window, restart reconciliation checks the published
object's full size and SHA-256. A matching publication completes the durable
receipt and removes residual staging. A conflicting publication fails closed.

No object is visible before all chunks, total size, and the final object hash
have been verified.

## Source safety

An interrupted outgoing transfer can resume only when the source still has
the same:

- object ID;
- total size;
- SHA-256;
- chunk size and count;
- target and request identity.

A changed source fails with
`object-transfer-resume-source-mismatch`. The receiver's accepted data is not
silently overwritten.

Durable incoming state is also bound to the enrolled source node. Another
node cannot claim an existing transfer ID.

## Abandoned transfer cleanup

`cleanup_abandoned()` removes only stale active or publishing transfers.

It removes:

- stale incoming metadata and staged chunks;
- stale outgoing metadata;
- managed source files created by `send_bytes()`.

Completed receipts and published objects are preserved. The caller controls
the age threshold; the default is 24 hours. Cleanup is a storage-space
maintenance action only; it does not alter membership or write authority.

## Acceptance coverage

The focused Linux and Windows matrix verifies:

- sender restart from the receiver's durable cursor;
- receiver restart with chunk-index reconstruction;
- completed receipt replay without network retransmission;
- fail-closed source mutation;
- fail-closed durable chunk tampering;
- lost completion acknowledgement followed by restart of both endpoints;
- stale active-transfer cleanup;
- preservation of completed receipts and published objects;
- durable source-node binding.

## Deferred

F6.5 does not add:

- cross-network peer discovery;
- NAT traversal or circuit relay;
- concurrent multi-process ownership of one durable root;
- distributed metadata consensus;
- branch cleanup.

Those remain separate Phase F closeout work.