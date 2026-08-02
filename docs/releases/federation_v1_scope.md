# MSH Federation v1.0 scope

Status: release scope definition. This document defines the intended v1 product boundary; it does not declare that `v1.0.0` has been released.

## Release identity

Product milestone: **MSH Federation v1.0**

Release intent: provide a stable trusted federation for MSH devices that contribute storage, language-model, and compute capabilities while preserving explicit authority, bounded recovery, and local-first compatibility.

## Supported v1 capability boundary

### Identity and sessions

- persistent node identities;
- explicit enrollment and revocation;
- session creation and invitation-based joining;
- authenticated membership and actor checks;
- ordered durable session events and replay;
- reconnect without inventing authority from connectivity alone.

### Federation transport

- outbound node connections;
- authenticated relay transport;
- direct encrypted peer transport when available;
- relay fallback;
- signed route/rendezvous information;
- verified, bounded, resumable object transfer;
- restart-safe transfer state where implemented.

### Federated storage

- logical storage API rather than direct application access to physical databases;
- filesystem and supported database providers;
- one coordinator-authorized writable primary per storage group;
- zero or more replicas;
- terms, leases, fencing tokens, and rejection of stale primary writes;
- immutable and idempotent replication boundaries;
- acknowledgement policy;
- authoritative manifests, hashes, watermarks, and missing ranges;
- completeness-aware failover;
- explicit degraded state when no complete qualified candidate exists;
- recovery and returning-former-primary behavior without self-promotion.

### Capability scheduling

- versioned job and attempt contracts;
- capability-specific requirements;
- several AI or compute providers without primary/replica semantics;
- deterministic eligibility and provider ranking;
- coordinator-owned durable job ownership;
- authenticated dispatch to explicitly registered local handlers;
- duplicate suppression;
- bounded retry, timeout, heartbeat-loss handling, cancellation, and reassignment;
- stale-worker fencing;
- at most one logical committed result;
- least-privilege job-scoped artifact authorization and verified publication.

### Trusted provider federation

- explicit durable provider request, approval, suspension, and revocation;
- fresh authenticated provider health and capacity reports;
- simultaneous trusted providers of the same type;
- remote language-model invocation through logical authenticated routes;
- local compute-handler activation without transferring executable code;
- operator-safe provider status and controls;
- restart/reconnect reconciliation from durable authority and ordered events;
- natural health expiry without deleting durable approval.

### Compatibility

- current Flask-first workbench remains the supported application surface;
- existing recorder durability and JSONL compatibility outputs remain supported;
- local-first workflows remain possible without federation;
- configured local or connected Ollama use remains supported;
- federation capability registration does not grant storage authority;
- private service endpoints remain private by default.

## Trust model

Federation v1 is for **explicitly trusted devices and providers**.

V1 assumes:

- the federation owner controls enrollment and approval;
- nodes are operated on trusted private networks, VPNs, or separately approved authenticated transport;
- contributed compute handlers are preinstalled and explicitly registered locally;
- provider operators are known and trusted;
- private database, Ollama, Flask, relay, worker, and storage ports are not exposed publicly by default.

V1 does not treat connection, announcement, approval, health, selection, successful execution, or artifact access as equivalent authorities.

## Explicitly outside v1

The following are not supported claims for v1:

- anonymous or public provider participation;
- arbitrary remotely supplied code, package, module, image, shell command, or process-launch execution;
- production sandboxing for unknown third-party workloads;
- marketplace, payment, billing, reputation, dispute, or settlement systems;
- internet-wide automatic endpoint exposure;
- a full Kubernetes-style scheduler;
- production cost, energy, fairness, quota, preemption, accelerator, and placement optimization;
- durable distributed interactive AI queues and complete streaming/model lifecycle orchestration;
- complete public-relay operations, restrictive-NAT certification, or every physical topology;
- production SLO, incident-management, abuse, denial-of-service, soak, chaos, and upgrade certification;
- multi-organization policy management or comprehensive role-based administration.

These items belong to later roadmaps and cannot be implied by the v1 tag.

## Public v1 terminology

Use these product concepts consistently:

- Federation
- Device
- Session
- Owner
- Member
- Provider
- Storage provider
- Primary
- Replica
- AI provider
- Compute provider
- Pending approval
- Approved
- Suspended
- Revoked
- Available
- Unavailable
- Degraded

Internal fields such as terms, generations, revisions, leases, and fencing tokens remain essential, but user-facing material should expose them only when needed for advanced diagnostics.

## Required release evidence

`v1.0.0` may be published only after:

- repository cleanup and documentation consolidation are complete;
- one permanent Federation v1 regression gate is green on Linux and Windows;
- clean installation succeeds from a fresh checkout;
- first-run setup succeeds without existing state;
- at least two independently persisted devices complete create/join/reconnect/restart acceptance;
- storage replication and controlled failover are demonstrated;
- at least one AI provider and one compute provider are approved, used, suspended or revoked, and recovered safely;
- documentation links and commands are validated;
- generated output and unsupported experiments are absent from the production tree;
- security and limitation statements match actual behavior;
- release notes and changelog are complete;
- the exact release commit is tagged and verified.

## Versioning policy

- `1.0.x`: compatible fixes, documentation corrections, regression strengthening, and security hardening without deliberate public contract expansion;
- `1.x`: compatible product improvements, including future guided UI and integrated documentation where they preserve v1 protocol semantics;
- `2.0`: intentionally broader federation or trust model requiring a new compatibility and migration decision.

The post-v1 roadmap is maintained separately so release cleanup does not silently expand this scope.
