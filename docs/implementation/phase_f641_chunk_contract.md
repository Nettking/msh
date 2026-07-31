# Phase F6.4.1: verified chunk contract

Status: implementation and acceptance boundary for F6.4.1.

## Scope

F6.4.1 defines a transport-independent protocol and receiver state machine for
one bounded object transfer. It proves that malformed, incomplete, conflicting,
or corrupted chunks cannot produce a publishable object receipt.

This phase does not send chunks over libp2p or relay. It does not add automatic
retransmission, timeouts, transport fallback, process-restart recovery, durable
cursors, filesystem staging, object publication, NAT traversal, discovery, or
capability scheduling.

## Contracts

The implementation adds three versioned public objects:

```text
msh.object_transfer.manifest.v1
msh.object_transfer.chunk.v1
msh.object_transfer.receipt.v1
```

The manifest binds:

```text
protocol version
transfer ID
logical object ID
total object size
final SHA-256
chunk size
chunk count
```

The chunk binds:

```text
protocol version
transfer ID
logical object ID
chunk index
byte offset
decoded chunk length
chunk SHA-256
canonical base64url data
```

A receipt is created only after every accepted chunk has been read back from the
chunk store, checked again against its accepted length and hash, streamed through
a final SHA-256 calculation, and matched against the manifest total size and
object hash.

## Bounds

F6.4.1 deliberately freezes bounded defaults before transport integration:

- maximum chunk data: 32,768 bytes;
- maximum chunks per transfer: 65,536;
- maximum declared object size: 2 GiB;
- maximum public identifier field: 512 UTF-8 bytes;
- default process-local test store: 16 MiB.

The protocol-level object bound is larger than the process-local store. This is
intentional. F6.4.2 will use an isolated staging implementation rather than
holding a large object in memory.

## Receiver rules

`ObjectTransferReceiver` accepts chunks in any order but requires every chunk to
match the manifest exactly:

- transfer and object identities must match;
- protocol major versions must agree;
- index must be within the manifest;
- offset must equal `index * chunk_size`;
- every non-final chunk must have the configured chunk size;
- the final chunk must have the exact remaining size;
- decoded data length and chunk SHA-256 must match the chunk contract.

An identical duplicate chunk is idempotent and returns `False` without changing
state. A duplicate index with different bytes, length, or hash fails closed as a
conflicting duplicate.

This distinction is important for F6.4.2: transport-frame replay remains a
security failure, while a newly authenticated delivery of the same logical chunk
may be handled idempotently by the object-transfer layer.

## Completion and publication boundary

Receiving all chunk indices is not sufficient for publication. The receiver
first reports `chunks_complete`, then `verify_complete()` must succeed before
`publishable` becomes true.

Final verification:

1. reads chunks back in manifest order;
2. verifies that stored bytes have not changed after acceptance;
3. streams all bytes through SHA-256 without assembling another complete copy;
4. compares the verified byte count with `total_size`;
5. compares the final digest with `object_hash`;
6. creates an immutable verified receipt.

Incomplete transfers, final size mismatch, final hash mismatch, missing stored
chunks, or post-acceptance store tampering never create a receipt.

## Storage boundary

F6.4.1 includes `ObjectTransferChunkStore` as a narrow process-local interface and
`InMemoryObjectTransferChunkStore` as a bounded test implementation.

The in-memory store is not the future large-object backend. It exists to prove the
contract and capacity behavior without introducing filesystem or database
persistence. Durable state and restart-safe cursors remain F6.5 work.

`abort()` deletes the transfer's process-local chunks and resets receiver state.
A verified receiver is immutable and rejects later chunks.

## Acceptance coverage

Focused tests prove:

1. manifest layout and schema round-trip;
2. inconsistent chunk counts are rejected;
3. unsupported schemas fail closed;
4. chunk data uses canonical base64url and a bound SHA-256;
5. tampered chunk data is rejected before acceptance;
6. chunks may arrive out of order and missing indices are deterministic;
7. identical duplicates are idempotent;
8. conflicting duplicates are rejected;
9. wrong transfer, object, offset, size, or index is rejected;
10. incomplete transfers are not publishable;
11. final object hash mismatch is rejected;
12. post-acceptance store tampering is detected;
13. process-local storage capacity is bounded;
14. abort clears state;
15. zero-byte objects verify without chunks;
16. a verified transfer is immutable.

Linux and Windows CI run the focused F6.4.1 test file alongside the complete
existing federation and Phase F regression matrices.

## Preserved invariants

F6.4.1 changes no storage request, replication, lease, term, fencing token,
authority grant, promotion, failover, direct transport, relay fallback, libp2p
sidecar, or control-plane behavior.

No runtime component imports or instantiates the new receiver yet.

## Next step

F6.4.2 may carry this manifest and chunk protocol over authenticated encrypted
peer frames, add bounded acknowledgements and retransmission within one process
lifetime, and stage data outside memory. It must use this verifier as the
publication gate and must not begin durable restart recovery, which remains
F6.5.
