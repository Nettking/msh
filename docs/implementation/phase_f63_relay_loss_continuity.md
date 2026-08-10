# Phase F6.3: relay-loss direct continuity

Status: implementation and acceptance boundary for F6.3.

## Scope

F6.3 proves scenario `F5-003` from the federated-session test matrix: an
established direct data path remains useful when the relay/control connection is
interrupted, while the node reports exactly which control-plane functions are no
longer available.

This phase does not add NAT traversal, peer discovery, new route exchange,
chunking, retransmission, resumable transfer, or scheduling.

## Core rule

A relay interruption changes connectivity, not storage authority.

A ready direct path may continue carrying storage requests and responses. The
provider receiving those requests still validates the existing grant, lease,
term, fencing token, acknowledgement policy, and operation authorization. Direct
continuity cannot issue, refresh, extend, or replace any of those controls.

An expired lease therefore remains expired even when the direct stream itself is
healthy.

## Control-plane states

`AdaptiveStorageTransport` now accepts an explicitly reported control-plane
state:

| State | Meaning |
| --- | --- |
| `available` | relay/control synchronization and relay fallback are usable |
| `interrupted` | relay/control is unavailable; an existing direct data path may remain usable |
| `recovering` | control reconnection is in progress but is not yet trusted as available |

The state is diagnostic input from the relay/control connection owner. It is not
inferred from a failed storage operation and it does not alter authority.

## Continuity status

`fcp.direct_continuity.status.v1` reports only logical availability and stable
limitation codes. It contains no peer ID, IP address, multiaddress, credentials,
or backend location.

During `interrupted` or `recovering`, it reports:

```text
direct_data_plane_available
relay_fallback_available = false
authority_refresh_available = false
membership_updates_available = false
coordinated_failover_available = false
```

The stable limitations are:

```text
authority-refresh-unavailable
membership-updates-unavailable
route-updates-unavailable
coordinated-failover-unavailable
relay-fallback-unavailable
```

These limitations prevent a healthy direct stream from being mistaken for a
healthy federated control plane.

## Transport selection

| Direct state | Control state | Result |
| --- | --- | --- |
| ready | available | direct, reason `direct-ready-f62` |
| ready | interrupted | direct, reason `direct-ready-control-interrupted-f63` |
| ready | recovering | direct, reason `direct-ready-control-recovering-f63` |
| unavailable/fails | available | bounded relay fallback |
| unavailable/fails | interrupted or recovering | fail closed with no relay attempt |
| no direct route | interrupted or recovering | fail closed with no relay attempt |

When both paths are unavailable, the transport raises a structured
`TransportPathUnavailable`. It does not retry a relay path already known to be
unavailable and does not reinterpret the failure as permission to self-promote.

## Recovery

The owner of the control connection reports `available` only after authenticated
relay reconnection and required state synchronization have completed. At that
point:

- relay fallback becomes available again;
- authority refresh may resume through the existing control mechanisms;
- membership, route, and coordinated failover updates may resume;
- direct remains preferred when it is still ready.

F6.3 does not itself reconnect the relay or replay control events. Those behaviors
remain owned by the existing relay/control client.

## Acceptance coverage

Focused tests prove that:

1. an established direct operation succeeds while the relay object is offline;
2. no relay request is attempted during successful direct continuity;
3. public continuity status lists all unavailable control functions;
4. `recovering` does not interrupt a healthy direct data path;
5. direct failure during a control interruption fails without a hidden relay attempt;
6. no-direct plus no-control fails without touching relay;
7. authenticated control recovery restores bounded relay fallback;
8. an expired-authority provider response remains rejected and is not renewed by transport.

The complete Linux and Windows Phase F matrices run these tests alongside F6.1
and F6.2.

## Exit state

- F6.1 transport and peer-frame contracts remain intact;
- F6.2 direct encrypted transport remains preferred when ready;
- F6.3 exposes control-plane degradation without stopping a healthy direct path;
- authority and failover remain coordinator-owned;
- all F6 branches remain until the full F6 sequence is complete.

## Next step

F6.4 should implement bounded chunk framing and verified retransmission for one
object transfer. It must verify every chunk and the final object hash before
commit, without yet adding resumable cursors across process restart.
