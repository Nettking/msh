# Phase F6.1: transport foundation and secure peer-frame contract

Status: implementation boundary for F6.1.

## Numbering

The original federated-session proposal names direct peer transport **Phase 5 / PR F** and additional-capability scheduling **Phase 6 / PR G**. The implemented repository sequence used F1-F5.3 for the physical storage path. From this point:

- implementation **F6** means the original direct-transport Phase 5;
- implementation **F7** means the original additional-capability scheduling Phase 6.

This keeps the repository's chronological branch and pull-request sequence unambiguous.

## Scope

F6.1 creates the stable transport boundary required before any direct network path is introduced. It does not open a listening socket, exchange candidates, traverse NAT, create a direct peer connection, transfer large objects, or schedule AI/compute capabilities.

All production storage operations still use the existing authenticated relay path.

## Delivered boundary

`catalog.federation.adaptive_transport`, `catalog.federation.peer_stream`, and `catalog.federation.peer_stream_verifier` provide:

- `AdaptiveStorageTransport`, implementing the existing storage request transport shape;
- explicit direct-transport diagnostic states: `disabled`, `connecting`, `ready`, and `unavailable`;
- deterministic, public transport decisions and counters;
- a hard F6.1 invariant that every request is sent through relay, even if diagnostics report that direct reachability is ready;
- a signed, ciphertext-only `msh.peer_stream.frame.v1` contract for F6.2 and later;
- session, source, target, stream, request, key, sequence, length, hash, nonce, protocol, and cipher-suite binding;
- connection-local fail-closed verification for identity mismatch, signature failure, replay, sequence gaps, stream exhaustion, and encryption-key reuse across streams.

The current `PhaseDLogicalStorageClient` already depends only on a transport with:

```python
async def request(*, target_node_id, envelope): ...
```

`AdaptiveStorageTransport` preserves that contract. Storage authority, leader grants, leases, fencing tokens, acknowledgement policy, manifest commits, failover, catch-up, and reinstatement do not depend on transport selection.

## Relay-only F6.1 policy

The selection policy is intentionally simple:

| Direct diagnostic state | Selected transport | Reason |
| --- | --- | --- |
| `disabled` | relay | `direct-disabled-f61` |
| `connecting` | relay | `direct-connecting-relay-selected` |
| `ready` | relay | `direct-not-enabled-until-f62` |
| `unavailable` | relay | `direct-unavailable-relay-selected` |

Diagnostic observers are isolated from the request path. A failed observer cannot prevent a relay request. Relay failures remain visible and are never converted into false success.

## Secure peer-frame contract

F6.1 freezes the application frame that future direct and end-to-end encrypted relay paths may carry. The frame contains ciphertext only; plaintext storage requests are not embedded in the public frame structure.

Required fields:

```text
schema
protocol_version
cipher_suite
session_id
stream_id
request_id
source_node_id
target_node_id
sequence
key_id
nonce
ciphertext_length
ciphertext_hash
ciphertext
signature
```

The supported F6.1 contract is:

```text
schema: msh.peer_stream.frame.v1
protocol: 1.x
key agreement: ephemeral X25519
key derivation: HKDF-SHA256 with session/node/stream context
payload encryption: ChaCha20-Poly1305
identity signature: persistent node Ed25519 identity
```

The persistent Ed25519 identity key must never be converted into or reused as an X25519 key. Each stream receives an independent ephemeral encryption key. The source signs the complete canonical ciphertext frame with its enrolled Ed25519 identity.

For one per-stream AEAD key, the nonce is canonical and derived from the unsigned 64-bit sequence:

```text
00000000 || sequence_as_8_byte_big_endian
```

A `key_id` may protect only one stream. This prevents accidental nonce reuse across streams. A new connection or restarted stream must use a fresh key agreement and key ID.

## Replay boundary

`PeerStreamVerifier` is connection-local in F6.1. It requires sequence zero for a new authenticated stream and then exact monotonic increments. It rejects:

- already accepted sequences;
- gaps;
- a key ID reused for another stream;
- unknown source identities;
- a source other than the expected peer;
- a target other than the local node;
- another session;
- invalid signatures;
- unsupported protocol major versions or cipher suites;
- ciphertext length or hash disagreement.

F6.2 must bind verifier lifetime to one authenticated peer connection. Durable resumable-transfer cursors belong to F6.5, not this request-frame replay window.

## Threat model

F6.1 protects the future frame contract against:

- relay or network modification of ciphertext or routing metadata;
- node identity substitution;
- cross-session and cross-node replay;
- duplicate and out-of-order request-frame acceptance;
- accidental encryption-key/nonce reuse across streams;
- oversized frame allocation;
- physical addresses, backend paths, or credentials appearing in public frame identity fields or transport status.

F6.1 does not claim protection against a compromised authorized node, malicious plaintext produced by an authorized application, traffic analysis, endpoint compromise, denial of service beyond bounded parsing/state, or Byzantine session members.

## Direct-transport technology decision

F6.1 adds no new networking dependency. F6.2 should prototype the direct node-agent data plane as a dedicated **Go libp2p sidecar**, behind the Python transport boundary introduced here.

Reasons:

- libp2p provides encrypted connections, stream multiplexing, relay support, reachability detection, and standard DCUtR hole punching;
- the repository architecture already identifies a Go/libp2p node agent as the target direction;
- implementing NAT traversal, candidate synchronization, and relay upgrade directly on top of a raw QUIC library would create a new custom traversal protocol;
- the official Python libp2p implementation remains under development, so F6.2 must not make storage correctness depend on it.

Primary references:

- https://docs.libp2p.io/
- https://docs.libp2p.io/concepts/hole-punching/
- https://docs.libp2p.io/concepts/circuit-relay/
- https://github.com/libp2p/py-libp2p
- https://aioquic.readthedocs.io/en/latest/

The F6.2 prototype must demonstrate Windows and Linux packaging before this preference becomes a permanent runtime dependency. If the sidecar cannot meet that exit criterion, stop and record the blocker rather than implementing custom hole punching in Python.

## Acceptance mapping

F6.1 is a prerequisite for, but does not itself claim, the original direct-transport scenarios:

- direct preferred;
- relay fallback;
- relay loss while direct survives;
- chunk verification;
- resumable transfer.

Its own exit criteria are:

1. relay remains the only operational storage transport;
2. existing relay storage behavior is unchanged;
3. transport choice is explicit and observable without exposing endpoints;
4. a diagnostic `ready` state cannot activate direct traffic;
5. malformed, tampered, foreign, replayed, gapped, or key-reusing peer frames fail closed;
6. Linux and Windows CI execute the focused F6.1 tests;
7. no F6.2 sockets, rendezvous, candidate exchange, sidecar, or dependency is introduced.

## Next step

F6.2 may add only a directly reachable encrypted peer stream behind `AdaptiveStorageTransport`. It must keep relay available and must not begin general NAT traversal or resumable large-object transfer.
