# Phase F6.6.1: circuit-relay and hole-punching foundation

Status: implementation and acceptance boundary for F6.6.1.

## Purpose

F6.6.1 begins alternative A from the F6 closeout decision: automatic cross-network connectivity remains part of F6 rather than being reclassified as later operational hardening.

This step establishes the libp2p circuit-v2 and DCUtR foundation inside the existing Go node-agent sidecar. It does not yet implement session rendezvous, automatic peer-route exchange, public deployment, or the final physical multi-network acceptance run.

## Connectivity roles

The sidecar now has three mutually exclusive roles:

| Role | Configuration | Behavior |
| --- | --- | --- |
| `direct-only` | no relay flags | preserves the F6.2 directly reachable peer behavior and keeps relay disabled |
| `relay-client` | one or more `--relay` flags | enables circuit-v2 transport, static AutoRelay candidates and DCUtR hole punching |
| `relay-service` | `--relay-service` | runs a bounded public circuit-v2 reservation and forwarding service plus an AutoNAT helper service |

Relay client and relay service roles cannot be combined in one process. Static bootstrap addresses must identify the relay directly and must not already contain `/p2p-circuit`.

## Command-line surface

A relay service can be started with an internet-reachable listen address:

```text
fcp-peer-sidecar \
  --listen /ip4/0.0.0.0/tcp/4001 \
  --relay-service
```

A node can use one or more known relay candidates:

```text
fcp-peer-sidecar \
  --listen /ip4/0.0.0.0/tcp/0 \
  --relay /dns4/relay.example.org/tcp/4001/p2p/<relay-peer-id>
```

`--relay` is repeatable and is bounded to eight unique relay peer identities.

`--force-private-reachability` is available only in relay-client mode. It is intended for deterministic tests or deployments that already know the node is behind NAT. Normal deployments should allow AutoNAT to determine reachability.

## Relay client behavior

Relay-client mode enables:

- the libp2p circuit-v2 transport;
- AutoRelay with the explicitly configured static relay candidates;
- immediate candidate evaluation without the default multi-minute boot delay;
- DCUtR hole punching for attempted upgrade from a relayed connection to a direct connection;
- identify-based address observations required by reachability and hole-punching mechanisms.

A relay address is a transport route only. Existing FCP application encryption, signed stream openings, enrolled node identities, replay checks and storage authority validation remain unchanged.

## Bounded relay service

The relay service uses explicit limits rather than unbounded forwarding:

- reservation TTL: 30 minutes;
- maximum active reservations: 32;
- maximum circuits per peer: 8;
- maximum reservations per peer: 1;
- maximum reservations per source IP: 4;
- maximum reservations per ASN: 16;
- maximum circuit duration: 10 minutes;
- maximum relayed data per direction and circuit: 64 MiB;
- bounded AutoNAT service request rates.

Private relay addresses remain filtered in normal service mode. The test-only host configuration may accept loopback reservation addresses so GitHub Actions can exercise the same circuit-v2 protocol without public infrastructure.

The pinned go-libp2p AutoRelay implementation deliberately removes private and loopback relay addresses from its advertised address set. The focused CI therefore does not pretend that a loopback AutoRelay advertisement is equivalent to a routable deployment. It performs an explicit circuit-v2 reservation and dial for the local datapath proof, while separately validating that production relay-client mode configures static AutoRelay candidates and DCUtR correctly.

## Ready event

The existing sidecar `ready` event remains backward compatible and adds private node-agent diagnostics:

```text
connectivity_mode
static_relay_peer_ids
hole_punching_enabled
relay_service_enabled
private_reachability_forced
```

The Python client continues to consume `peer_id` and `listen_addrs`; additive fields do not change the existing packet-channel contract. Public FCP transport status still must not expose peer IDs, multiaddresses or backend locations.

## Acceptance coverage

The focused Go tests prove:

1. the original directly reachable encrypted stream still works;
2. target peer identity must still match the supplied multiaddress;
3. a bounded relay service accepts an explicit circuit-v2 reservation from a private test client;
4. a second relay-capable client can dial the reserved peer through a constructed circuit-v2 route;
5. the existing opaque FCP wire message crosses a connection reported by libp2p as `Limited`, proving circuit-v2 carriage rather than a direct socket;
6. the reservation returns the configured circuit duration and data limits;
7. relay-client readiness reports static AutoRelay, hole-punching and reachability configuration accurately;
8. malformed, duplicate, conflicting or circuit-containing relay bootstrap configuration fails closed;
9. relay resource limits remain explicit and bounded.

The focused workflow builds and tests the Go sidecar on Linux and Windows. Existing Phase 2 and F6 closeout workflows also run because the sidecar changed.

## Preserved invariants

F6.6.1 does not:

- change the Python storage request contract;
- change logical storage addressing;
- issue or refresh leases, grants, terms or fencing tokens;
- promote a storage provider;
- weaken application-level encryption or node identity checks;
- introduce a committed public relay address or credential;
- claim physical multi-network acceptance.

## Next step

F6.6.2 should integrate session-scoped rendezvous and authenticated route-descriptor exchange. Nodes must learn only routes for enrolled peers in the same session, and physical addresses must remain outside durable logical storage identity and public status surfaces.
