# Phase F6 closeout: direct transport acceptance and remaining operational proof

Status: closeout in progress.

Baseline: `main` at `9fb6e01f6d8e0a98a58209327ebb2ae182b725ab` after F6.5.

## Purpose

Implementation F6 maps to Phase 5 / PR F in the federated-session architecture. The required deliverables are:

- a dedicated node-agent boundary;
- direct encrypted streams when possible;
- relay fallback;
- resumable chunked large-object transfer;
- transport choice hidden behind the logical capability client.

This closeout consolidates the automated evidence for those requirements and records the remaining physical multi-network acceptance honestly. It does not add F7 scheduling, change storage authority, or delete F6 branches before the closeout decision is complete.

## Delivered implementation sequence

| Implementation step | Pull request | Main commit | Delivered boundary |
| --- | ---: | --- | --- |
| F6.1 | #134 | `c4c4b41d5550b0993becb20a9fba84688a0e2c05` | transport-neutral logical storage boundary and signed ciphertext peer-frame contract |
| F6.2 | #135 | `e54c39256932c3cd1f2a6b36d063ee4be5d8e8d8` | directly reachable encrypted libp2p stream with bounded relay fallback |
| F6.3 | #136 | `c75364f7e85adfe6fa04770aa5bcf05f8cdb7ede` | direct continuity during relay/control interruption without authority changes |
| F6.4.1 | #137 | `a4136e111c19704d9c7352590b8727c4c32c320d` | bounded manifest/chunk contract and final object verification |
| F6.4.2 | #138 | `d0913864259ea8f466124426b97f06dc67f23378` | encrypted delivery, acknowledgement, retransmission, staging, and atomic publication |
| F6.5 | #139 | `9fb6e01f6d8e0a98a58209327ebb2ae182b725ab` | durable cursors, restart negotiation, restart recovery, receipts, and abandoned-transfer cleanup |

## Phase 5 test-matrix mapping

The original deterministic matrix defines five direct-transport scenarios.

| Scenario | Automated evidence |
| --- | --- |
| `F5-001 Direct preferred` | F6.2 proves that a ready configured direct path is selected while logical storage addressing remains unchanged. |
| `F5-002 Relay fallback` | F6.2 proves that an expected direct reachability failure uses relay without application reconfiguration; security or protocol failures do not downgrade. |
| `F5-003 Relay fails while direct survives` | F6.3 proves that an established direct data path remains usable during control interruption and exposes the unavailable control functions. |
| `F5-004 Chunk verification` | F6.4.1 and F6.4.2 reject corrupt chunks, retransmit with fresh authenticated frames, and publish only after final size and SHA-256 verification. |
| `F5-005 Resumable transfer` | F6.5 resumes from the receiver's durable verified cursor after sender, receiver, or machine restart and never publishes a partial object. |

The closeout workflow runs the complete F6 Python matrix and the Go/libp2p sidecar tests on Linux and Windows. It also compiles and lints every F6 boundary file and checks diff hygiene.

## Invariants preserved across F6

Transport selection remains a data-plane concern. It cannot:

- issue, refresh, extend, or replace a leadership grant or lease;
- change a term or fencing token;
- promote a storage provider;
- weaken acknowledgement policy;
- reinterpret a provider rejection as success;
- expose peer addresses, backend paths, or credentials in public transport status;
- publish an object before all chunks, total size, and final hash are verified.

A verified transfer receipt proves byte completeness only. It grants no storage authority.

## Remaining operational proof

F6.2 deliberately requires private descriptor exchange and an already reachable peer. Its Go sidecar disables identify-based address discovery and libp2p relay. F6.4.2 and F6.5 explicitly deferred cross-network discovery, NAT traversal, circuit-relay integration, and physical multi-network acceptance to closeout.

GitHub-hosted CI can prove the protocol, cryptography, fallback policy, interruption behavior, chunk safety, and restart semantics. It cannot honestly prove two independently operated machines behind unrelated restrictive NATs without one of the following:

- an internet-reachable rendezvous and circuit-relay deployment;
- credentials for a temporary external test environment;
- two appropriately configured self-hosted runners on unrelated networks;
- an approved equivalent physical test setup.

No such infrastructure or credentials are committed to the repository. Under the implementation stop rules, closeout must record this rather than replace the physical proof with a loopback test that appears equivalent.

## Required physical acceptance run

Use at least two persisted MSH node states on unrelated networks. A third internet-reachable relay/rendezvous node is required if automatic traversal or circuit-relay behavior is part of the final F6 exit decision.

Capture deterministic evidence for:

1. both nodes enroll in the same session without exposing database or Flask ports;
2. logical storage requests retain session/group addressing and contain no durable physical endpoint;
3. a directly reachable encrypted path is preferred when a valid route exists;
4. an unavailable direct route falls back to the authenticated relay without application reconfiguration;
5. a security, identity, signature, protocol, or ciphertext failure does not downgrade to relay;
6. an established direct path remains usable while relay/control connectivity is interrupted;
7. public continuity status reports authority refresh, membership, route updates, coordinated failover, and relay fallback as unavailable during interruption;
8. a multi-chunk object rejects corruption and publishes only after final SHA-256 verification;
9. interruption followed by sender and receiver restart resumes from the receiver's verified durable cursor;
10. terms, leases, fencing tokens, acknowledgement policy, and promotion state remain unchanged by transport events.

The evidence bundle should include node IDs, session ID, transfer ID, selected logical transport, stable reason codes, accepted and remaining chunk counts, final object size/hash, process restart points, and test timestamps. It must redact private keys, credentials, IP addresses, multiaddresses, backend paths, and object plaintext.

## Closeout decision rule

F6 may be marked complete and its implementation branches deleted only when both conditions hold:

1. the consolidated Linux/Windows closeout workflow is green; and
2. the physical multi-network requirement is either successfully evidenced or explicitly reclassified by an architectural decision as a later operational-hardening milestone.

Until then, all F6 branches remain preserved:

- `agent/phase-f61-transport-foundation`
- `agent/phase-f62-direct-peer-stream`
- `agent/phase-f63-relay-loss-continuity`
- `agent/phase-f641-chunk-contract`
- `agent/phase-f642-verified-chunk-transfer`
- `agent/phase-f65-durable-resume`
- `agent/phase-f6-closeout`

F7 capability scheduling must not begin as part of this closeout change.
