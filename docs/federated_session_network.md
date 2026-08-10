# Federated FCP network reference

Status: **current technical reference**

Reviewed: **2026-08-07 Europe/Oslo**

This document describes the merged trusted Federation network. It is not a future implementation proposal and does not define the next delivery sequence. Current work is governed by the active Federation plans and acceptance manifest.

## Scope

The Federation connects independently persisted FCP devices across approved private networks or authenticated transport. A device may contribute several capabilities while preserving explicit membership, provider, storage, job, artifact, lease, term, and fencing authorities.

Federation v1 is not public anonymous infrastructure. It assumes explicitly trusted devices and providers.

## Core terminology

- **Device/node** — one FCP installation with a persistent cryptographic identity.
- **Federation** — the user-facing trusted collaboration boundary.
- **Session** — the retained internal protocol and isolation boundary corresponding to one Federation during the compatible migration.
- **Capability** — a supported service such as recorder, language model, registered compute, or storage.
- **Control plane** — membership, revisions, ordered events, authority assignments, provider state, jobs, terms, leases, and fencing.
- **Data plane** — telemetry, objects, model requests, job inputs/results, and replication traffic.
- **Coordinator** — authority that serializes control-plane changes.
- **Relay** — authenticated fallback and control-plane transport for outbound node connections.
- **Storage primary/replica** — coordinator-assigned storage roles for a specific group and term.

## Identity and trust

Each device keeps a persistent private key locally. The public identity is used for authenticated enrollment, signed challenges, pairing, reconnect, and actor checks.

Discovery returns candidates only. Membership requires one of:

- an existing trusted binding;
- explicit authorized acceptance;
- signed expiring pairing material carrying bounded one-use enrollment and invitation data;
- another reviewed authenticated adapter to the existing membership authority.

A device ID, IP address, hostname, relay presence, or benchmark result never grants membership or capability authority.

## Federation/session compatibility

The UI uses `federation_id`. Protocol messages, durable events, membership, provider bindings, storage assignments, jobs, artifacts, replay, and isolation continue to use the internal `session_id` boundary.

Removing or renaming that internal boundary requires a separate protocol-major compatibility plan. Product documentation should not require ordinary users to manage technical sessions directly.

## Control-plane state

The control plane durably owns or validates:

- device enrollment and revocation;
- Federation/session creation and membership;
- ordered per-session events and revisions;
- invitation and pairing redemption;
- capability announcements;
- provider enrollment, policy, health, and capacity;
- storage assignments, terms, leases, and fencing tokens;
- jobs, attempts, cancellation, retry, ownership, and committed results;
- artifact grants and publication;
- request deduplication and audit records.

Flask does not own this distributed authority.

## Transport model

Devices initiate outbound authenticated connections. Correctness does not require a stable public address or inbound port forwarding.

The supported model is:

```text
outbound authenticated node connection
  -> control plane and relay route
  -> direct encrypted peer stream when available
  -> authenticated relay fallback when direct transport is unavailable
```

Transport includes bounded framing, protocol-major validation, route/rendezvous verification, reconnect, ordered replay, and verified resumable object transfer.

Direct transport is an optimization. Membership and capability authority remain control-plane decisions regardless of the selected route.

## Capability model

One device may advertise and contribute several independent capability types.

Capability state is separated into:

1. local availability and inspection;
2. optional benchmark evidence;
3. operator contribution intent;
4. Federation policy decision;
5. provider activation;
6. authenticated health and capacity;
7. runtime selection or assignment;
8. disable, suspend, expiry, revocation, and reconciliation.

No earlier state implies a later authority state automatically.

## Storage network

Storage uses a logical API and coordinator-controlled assignments.

For each storage group:

- exactly one assigned writable primary may accept new writes for the current term and lease;
- zero or more replicas follow committed data;
- every write carries idempotency, content identity, term, and fencing context;
- stale or obsolete primary writes are rejected;
- authoritative manifests record batches, hashes, watermarks, and missing ranges;
- promotion selects a qualified complete candidate deterministically;
- no candidate self-promotes because another device is unreachable;
- no complete candidate produces explicit degraded state;
- returning former primaries rejoin without retaining obsolete authority.

Recorder delivery remains local-first and durable until the configured acknowledgement policy is satisfied.

## AI and compute network

AI and compute do not use storage primary/replica semantics.

- AI providers expose authenticated logical model service routes.
- Compute providers expose only explicitly registered local handlers.
- FCP does not deliver arbitrary executable code to workers.
- Jobs use versioned requirements, ownership, attempts, heartbeat, retry, timeout, cancellation, stale-worker fencing, duplicate suppression, and one logical committed result.
- Artifact access is least-privilege and bound to the authorized job or publication action.

## Recorder and source capabilities

Recorder capabilities identify configured supported sources, retain explicit enablement, preserve local durability, and may coexist with AI, compute, storage-candidate, or workbench capability on the same device.

Source discovery does not start recording. Disabling recorder contribution fences future Federation use without deleting local data, checkpoints, membership, or unrelated contributions.

## Failure behavior

The network fails closed for authority.

- Disconnection does not create leadership or provider authority.
- Expired health removes runtime eligibility without silently deleting durable trust.
- Revocation fences reconnect and future accepted traffic.
- Missing revisions require replay before dependent actions continue.
- Unknown protocol major versions are rejected.
- Incomplete storage state causes synchronization or degraded mode, not false promotion.
- Duplicate deliveries and commands use durable idempotency boundaries.
- Private endpoints, credentials, pairing material, keys, and database locations are excluded from public projections and acceptance evidence.

## Supported trust boundary

Federation v1 supports trusted private deployments and separately approved authenticated transport. It does not claim Byzantine-fault tolerance, anonymous participation, decentralized consensus without stable authority, multi-primary distributed SQL, or safe execution of untrusted remote code.

## Current acceptance status

Automated contract and product-composition coverage is implemented. Complete physical Windows/Linux, multi-host, MTConnect, Ollama/accelerator, storage, and browser acceptance remains unaccepted.

See:

- [Current architecture](architecture.md)
- [Active Federation plans](implementation/federation/active/)
- [Federation acceptance documentation](implementation/federation/acceptance/)
- [Federation v1 scope](releases/federation_v1_scope.md)
