# Phase F6 closeout: implementation complete and operational-hardening gate

Status: F6 implementation complete. Final closeout requires this document and the consolidated Linux/Windows gate to merge successfully; implementation branches may then be deleted.

Baseline: `main` at `8e775010ce2b479b64d30416530c59c5ac939689` after F6.6.2.

## Purpose

Implementation F6 maps to Phase 5 / PR F in the federated-session architecture. Its software deliverables are:

- a dedicated node-agent boundary;
- direct encrypted streams when possible;
- bounded relay fallback and circuit-v2 carriage;
- authenticated session-scoped route exchange;
- resumable chunked large-object transfer;
- transport choice hidden behind the logical capability client.

This closeout consolidates the automated evidence, records the architectural exit decision, and separates implementation completion from public deployment and physical-network operations. It does not add F7 scheduling or change storage authority.

## Delivered implementation sequence

| Implementation step | Pull request | Main commit | Delivered boundary |
| --- | ---: | --- | --- |
| F6.1 | #134 | `c4c4b41d5550b0993becb20a9fba84688a0e2c05` | transport-neutral logical storage boundary and signed ciphertext peer-frame contract |
| F6.2 | #135 | `e54c39256932c3cd1f2a6b36d063ee4be5d8e8d8` | directly reachable encrypted libp2p stream with bounded relay fallback |
| F6.3 | #136 | `c75364f7e85adfe6fa04770aa5bcf05f8cdb7ede` | direct continuity during relay/control interruption without authority changes |
| F6.4.1 | #137 | `a4136e111c19704d9c7352590b8727c4c32c320d` | bounded manifest/chunk contract and final object verification |
| F6.4.2 | #138 | `d0913864259ea8f466124426b97f06dc67f23378` | encrypted delivery, acknowledgement, retransmission, staging, and atomic publication |
| F6.5 | #139 | `9fb6e01f6d8e0a98a58209327ebb2ae182b725ab` | durable cursors, restart negotiation, restart recovery, receipts, and abandoned-transfer cleanup |
| F6.6.1 | #141 | `cff31a9764f380fe4dbe3ec083c3277d8699f13f` | bounded circuit-v2 relay service/client roles, static AutoRelay candidates and DCUtR configuration |
| F6.6.2 | #142 | `8e775010ce2b479b64d30416530c59c5ac939689` | signed, session-scoped publish/resolve/withdraw of direct or circuit-v2 route descriptors |

## Phase 5 acceptance mapping

| Scenario | Automated evidence |
| --- | --- |
| `F5-001 Direct preferred` | F6.2 proves that a ready configured direct path is selected while logical storage addressing remains unchanged. |
| `F5-002 Relay fallback` | F6.2 proves that an expected direct reachability failure uses the authenticated application relay without application reconfiguration; security or protocol failures do not downgrade. |
| `F5-003 Relay fails while direct survives` | F6.3 proves that an established direct data path remains usable during control interruption and exposes unavailable control functions. |
| `F5-004 Chunk verification` | F6.4.1 and F6.4.2 reject corrupt chunks, retransmit with fresh authenticated frames, and publish only after final size and SHA-256 verification. |
| `F5-005 Resumable transfer` | F6.5 resumes from the receiver's durable verified cursor after sender, receiver, or machine restart and never publishes a partial object. |
| Circuit-v2 carriage | F6.6.1 proves an explicit reservation and opaque FCP packet transfer over a libp2p connection reported as `Limited`, with bounded relay resources. |
| Session route exchange | F6.6.2 proves that only enrolled members of the same session can publish and resolve signed routes, with expiry and generation rollback protection. |

The closeout workflow runs the complete F6 Python matrix and Go/libp2p sidecar tests on Linux and Windows. It compiles and lints every F6 Python boundary, verifies Go module-lock stability, builds the sidecar, and checks diff hygiene.

## Invariants preserved across F6

Transport selection and rendezvous remain data-plane concerns. They cannot:

- issue, refresh, extend, or replace a leadership grant or lease;
- change a term or fencing token;
- promote a storage provider;
- weaken acknowledgement policy;
- reinterpret a provider rejection as success;
- make a route descriptor into storage authorization;
- expose peer addresses, backend paths, object plaintext, or credentials in public transport status;
- publish an object before all chunks, total size, and final hash are verified.

A verified transfer receipt proves byte completeness only. A signed route proves who advertised a short-lived transport path only. Neither grants storage authority.

## Architectural closeout decision

F6 is closed as a **software implementation milestone** when the final closeout workflow is green and this decision is merged.

The following work is explicitly reclassified as a later **operational-hardening and deployment acceptance milestone**, not as unfinished F6 implementation:

- operating an internet-reachable relay/rendezvous service with deployment-specific DNS, firewall, monitoring and incident ownership;
- proving AutoNAT/DCUtR behavior between independently operated machines behind unrelated restrictive NATs;
- producing a physical multi-network evidence bundle from an approved external environment or self-hosted runners;
- automatic route republishing after relay restart or descriptor expiry;
- policy for selecting among several simultaneously valid routes;
- production capacity, abuse, latency, availability and upgrade testing for relay infrastructure.

### Rationale

The repository now contains the required transport mechanisms and fail-closed protocol boundaries: direct encrypted streams, application-relay fallback, circuit-v2 client/service support, DCUtR configuration, signed session rendezvous, verified chunking and durable resume. A public relay deployment and two unrelated physical networks are environment resources with operational owners, credentials and risk controls; GitHub-hosted CI cannot honestly manufacture that evidence.

Reclassification does not claim that restrictive-NAT production deployment has been proven. Until the operational-hardening gate passes, deployments must describe cross-network support as implemented but not physically accepted for their environment.

This decision also does not weaken any security or authority acceptance criterion. Identity, signature, protocol, ciphertext, membership or authorization failures remain fail-closed and must never trigger an insecure downgrade.

## Operational-hardening acceptance run

A later deployment gate should use at least two persisted FCP node states on unrelated networks and an internet-reachable relay/rendezvous service. It should capture deterministic evidence that:

1. both nodes enroll in the same session without exposing database or Flask ports;
2. signed routes are visible only to active members of the same session;
3. a direct encrypted route is preferred when available;
4. circuit-v2 carries the same opaque FCP packet when a direct path is unavailable;
5. DCUtR attempts an upgrade when the network permits it;
6. security, identity, signature, protocol or ciphertext failure does not downgrade;
7. relay/control interruption does not create storage authority or silent continuity claims;
8. multi-chunk corruption is rejected and publication requires final SHA-256 verification;
9. sender and receiver restart resumes from the receiver's verified durable cursor;
10. terms, leases, fencing tokens, acknowledgement policy and promotion state remain unchanged by transport events.

The evidence bundle should redact private keys, credentials, IP addresses, multiaddresses, backend paths and object plaintext.

## Branch cleanup authorization

After the final closeout PR is merged and its exact head has green Linux and Windows closeout plus full Phase 2 validation, the following implementation branches may be deleted:

- `agent/phase-f61-transport-foundation`
- `agent/phase-f62-direct-peer-stream`
- `agent/phase-f63-relay-loss-continuity`
- `agent/phase-f641-chunk-contract`
- `agent/phase-f642-verified-chunk-transfer`
- `agent/phase-f65-durable-resume`
- `agent/phase-f6-closeout`
- `agent/phase-f661-circuit-relay-foundation`
- `agent/phase-f662-session-route-rendezvous`
- `agent/phase-f6-final-closeout`

The commits remain permanently reachable from `main`; deleting these branches removes only obsolete implementation refs.

F7 capability scheduling is outside this closeout change and must begin only as a separate step after F6 cleanup is verified.
