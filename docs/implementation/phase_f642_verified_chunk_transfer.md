# Phase F6.4.2: verified encrypted chunk transfer

## Scope

F6.4.2 connects the transport-independent F6.4.1 manifest/chunk verifier to authenticated encrypted peer frames. It adds bounded acknowledgements, bounded retransmission, process-local disk staging, and atomic publication of a fully verified object.

It does not add durable restart cursors. F6.5 owns persistence and recovery after process or machine restart.

## Runtime components

`VerifiedChunkTransferEndpoint` uses the existing `DirectPacketChannel`, `DirectPeerDescriptor`, signed `PeerStreamOpen`, and `PeerStreamFrame` contracts.

`FilesystemObjectTransferChunkStore` implements the F6.4.1 chunk-store interface using a private process-specific staging directory. It writes each chunk through a temporary file and `os.replace()`. A completed object is assembled into a temporary file in the publication directory and is made visible with one final `os.replace()` only after size and SHA-256 are rechecked.

The endpoint can use a dedicated direct packet channel or a future packet router that multiplexes schemas. This PR does not modify the existing storage-request endpoint or its current channel ownership.

## Encrypted message sequence

One transfer uses one signed stream open, one ephemeral X25519 sender key, one receiver X25519 endpoint key, and one derived key for each direction.

The logical sequence is:

1. encrypted manifest message;
2. encrypted chunk messages;
3. encrypted completion message;
4. encrypted acknowledgement for every delivered message.

Every delivery attempt receives a new monotonically increasing frame sequence and therefore a new canonical AEAD nonce. A retry never reuses the previous frame or nonce.

## Retry and replay semantics

Two failure cases are intentionally distinct:

- **Dropped before delivery:** the receiver never observes the old sequence. The retry uses a higher sequence. A bounded sequence gap is accepted.
- **Acknowledgement lost after acceptance:** the receiver has already accepted the logical chunk. The retry again uses a higher sequence. F6.4.1 identifies the same logical chunk as an idempotent duplicate and returns a new acknowledgement.

An exact transport-frame replay is always rejected. A frame sequence must be greater than the last successfully decrypted sequence for that transfer, and the gap is bounded.

Retries are bounded by `max_attempts` (default 4, maximum 32). Exhaustion raises `object-transfer-retry-exhausted`; it does not silently publish or downgrade to an unauthenticated path.

## Acknowledgements

`msh.direct_object_transfer.ack.v1` binds the acknowledgement to:

- transfer ID;
- object ID;
- action (`manifest`, `chunk`, `complete`, or `abort`);
- chunk index where relevant;
- accepted/retryable/duplicate state;
- accepted and remaining chunk counts;
- the next missing index;
- a stable error code;
- the F6.4.1 verified receipt after completion.

The sender rejects acknowledgements for a different action, object, transfer, attempt sequence, or chunk index.

## Fail-closed checks

The runtime rejects:

- an unenrolled libp2p peer identity;
- an invalid stream-open signature;
- an invalid frame signature;
- AEAD authentication failure;
- a frame bound to another stream, request, session, key, or node;
- exact frame replay or an excessive sequence gap;
- oversized request or acknowledgement packets;
- malformed or conflicting manifests and chunks;
- incomplete objects;
- staging mutation detected during final verification;
- publication under an object ID already bound to different content;
- source files that grow during transfer.

A failed transfer performs a best-effort encrypted abort. Receiver staging is also removed when an endpoint closes.

## Preserved storage invariants

F6.4.2 changes no storage authority rule. It does not modify:

- terms, leases, fencing tokens, grants, or promotion;
- replication acknowledgement policy;
- failover or reinstatement;
- logical storage request routing;
- the F6.3 relay/control interruption model;
- the Go/libp2p sidecar implementation.

A verified object receipt proves byte completeness only. It cannot create, renew, or extend storage authority.

## Explicitly deferred

F6.5 remains responsible for:

- durable transfer manifests and cursors;
- recovery after process or machine restart;
- negotiation of already verified chunks after restart;
- expiration and garbage collection of abandoned durable transfers.

Cross-network discovery, NAT traversal, circuit-relay integration, and physical multi-network acceptance remain separate Phase F closeout work.
