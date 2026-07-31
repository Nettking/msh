# Phase F6.2: directly reachable encrypted peer stream

Status: implementation and acceptance boundary for F6.2.

## Scope

F6.2 adds the first operational direct data path behind `AdaptiveStorageTransport`.
It is deliberately limited to two enrolled nodes that already have direct network
reachability and whose private route descriptors have been exchanged explicitly.

F6.2 does not add discovery, rendezvous, circuit relay, AutoNAT, UPnP, hole
punching, DCUtR, public candidate exchange, chunk retransmission, resumable
large-object transfer, or AI/compute scheduling.

## Delivered components

- `catalog.federation.direct_peer.DirectStorageEndpoint`
  - preserves the existing `request(target_node_id, envelope)` storage shape;
  - encrypts every storage request and response before handing it to the sidecar;
  - binds the libp2p peer identity to the enrolled MSH node identity;
  - dispatches only to an explicitly registered local storage provider;
  - retains a bounded accepted-key replay window.
- `catalog.federation.libp2p_sidecar.Libp2pSidecarClient`
  - owns one local Go sidecar subprocess;
  - uses bounded JSON-lines IPC over stdin/stdout;
  - multiplexes outbound responses and inbound request callbacks;
  - does not expose peer addresses through adaptive public status.
- `cmd/msh-peer-sidecar`
  - creates a Go libp2p host on one explicit listen multiaddress;
  - disables libp2p relay and identify-based address discovery for F6.2;
  - accepts only an explicit target peer ID plus matching `/p2p/...` multiaddress;
  - opens `/msh/direct-storage/1.0.0` streams;
  - keeps application payloads opaque to the sidecar.

The sidecar is a data-plane tunnel. It does not decide storage leadership,
leases, fencing, acknowledgement policy, or provider authorization.

## Direct route lifecycle

1. Each Python endpoint starts its local sidecar.
2. The sidecar returns an authenticated libp2p peer ID and direct listen address.
3. The Python endpoint creates a process-lifetime X25519 receiver key.
4. The endpoint constructs a private `msh.direct_peer.descriptor.v1` containing:
   - MSH node ID;
   - libp2p peer ID;
   - directly reachable multiaddress;
   - X25519 receiver public key.
5. The two descriptors are exchanged by an explicit trusted setup step.
6. Each endpoint binds the remote libp2p peer ID to the expected enrolled MSH
   node ID before accepting application data.

The descriptor is local route configuration. It is intentionally absent from
`AdaptiveTransportStatus` and other public evidence because it contains a
physical network address.

## Application encryption

For each storage request, the source generates a fresh X25519 key pair and a
fresh key ID. The signed `msh.peer_stream.open.v1` object binds:

```text
session
stream
request
source MSH node
target MSH node
key ID
source ephemeral X25519 public key
protocol version
```

The source signs this object with its persistent enrolled Ed25519 identity. The
persistent Ed25519 key is never converted into or reused as an X25519 key.

The source ephemeral private key and target process-lifetime X25519 public key
produce the shared secret. HKDF-SHA256 derives independent request and response
keys using session, source, target, stream, request, key ID, and direction as
context. ChaCha20-Poly1305 encrypts the canonical storage envelope. The existing
`msh.peer_stream.frame.v1` signs and binds the resulting ciphertext.

Request and response directions both use sequence zero, but use independent
HKDF-derived keys and therefore do not reuse an AEAD key/nonce pair.

## Identity and downgrade rules

A direct request is accepted only when all of the following agree:

- transport-authenticated libp2p peer ID;
- explicitly registered peer descriptor;
- signed MSH source node ID;
- enrolled Ed25519 public identity;
- session, source, target, request, stream, and key bindings;
- ciphertext hash, signature, and AEAD authentication;
- storage envelope actor and session;
- requested local provider ID.

Only `DirectTransportUnavailable` permits relay fallback. Identity mismatch,
signature failure, malformed frames, ciphertext tampering, wrong actor/session,
or replay fail closed and are not hidden by relay downgrade.

## Adaptive selection

| Condition | Selected path | Reason |
| --- | --- | --- |
| direct transport configured and state `ready` | direct | `direct-ready-f62` |
| direct establishment/liveness failure | relay fallback | `direct-failed-relay-fallback` |
| state `connecting` | relay | `direct-connecting-relay-selected` |
| state `unavailable` | relay | `direct-unavailable-relay-selected` |
| no direct transport configured | existing F6.1 relay behavior | existing reason |

Storage responses and logical addressing remain unchanged. The application does
not persist peer IDs, multiaddresses, or leader IP addresses.

## Sidecar IPC

The local sidecar speaks one bounded JSON object per line.

Commands:

```text
request  -> target peer ID, target multiaddress, opaque packet
respond  -> response to an inbound correlation ID
shutdown -> graceful process stop
```

Events:

```text
ready            -> peer ID and listen addresses
incoming_request -> authenticated remote peer ID and opaque packet
response         -> correlated opaque response packet
error            -> stable transport or remote-rejection category
```

The sidecar never parses storage envelopes, credentials, provider IDs, or
plaintext. Those remain inside Python after application-level decryption.

## Acceptance mapping

F6.2 implements the repository's original direct-transport scenario F5-001 and
the establishment-failure portion of F5-002:

1. a ready direct route is preferred without changing logical storage addressing;
2. request and response plaintext are absent from the sidecar packet;
3. expected direct reachability failure falls back to relay;
4. security/protocol failures do not downgrade to relay;
5. libp2p peer identity must match the registered MSH node descriptor;
6. replaying an already accepted stream key is rejected before a second provider dispatch;
7. the Go sidecar establishes a real direct encrypted libp2p stream on loopback;
8. Linux and Windows build and test the sidecar and run focused Python contracts.

## Operational example

Build the sidecar from the repository root:

```bash
cd cmd/msh-peer-sidecar
go build -trimpath -o ../../build/msh-peer-sidecar .
```

Windows:

```powershell
cd cmd/msh-peer-sidecar
go build -trimpath -o ..\..\build\msh-peer-sidecar.exe .
```

The node process should launch the binary through `Libp2pSidecarClient`, exchange
private descriptors through trusted configuration, register the peer descriptor,
and only then report `DirectTransportState.READY`.

## Exit state

- relay remains configured and available;
- direct is used only for an explicitly registered, already reachable peer;
- all storage authority and replication invariants are unchanged;
- no NAT traversal or large-object protocol exists;
- F6.1 and F6.2 branches remain until all of F6 is complete, per project workflow.

## Next step

F6.3 should prove that an established direct stream continues useful data-plane
work when the relay/control connection is interrupted, while surfacing control-
plane limitations accurately. It must not begin chunking or resumable transfer.
