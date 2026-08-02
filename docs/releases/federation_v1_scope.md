# MSH Federation v1.0 scope

Status: pre-release scope definition. This document defines the intended trusted-federation boundary; it does not declare that `v1.0.0` has been released.

## Release identity

Product milestone: **MSH Federation v1.0 technical baseline** with capability-first onboarding completed before release publication.

Release intent: provide a stable trusted federation for MSH devices that can contribute storage, language-model, compute, recording, and other supported capabilities while preserving explicit authority, bounded recovery, and local-first compatibility.

## Product model

Every installation is one persistent MSH device.

A device may contribute several supported capabilities simultaneously. The user is not required to select one permanent deployment role during first-run setup.

The intended product flow is:

```text
load or create device identity
  -> discover, verify, join, or create a federation
  -> inspect the device
  -> run suitable bounded benchmarks
  -> select one or more contributions
  -> reconnect and reconcile automatically on later starts
```

Benchmarks describe suitability and capacity. They do not grant membership, provider authority, storage assignment, job ownership, or artifact access.

## Supported capability boundary

### Identity and federation membership

- persistent node identities;
- explicit enrollment and revocation;
- authenticated federation membership and actor checks;
- discovery of federation candidates without granting trust;
- verified first-time join;
- local federation creation when no candidate exists;
- ordered durable events and replay;
- reconnect without inventing authority from connectivity alone.

The current implementation uses an internal session boundary. During the compatible capability-first migration, one user-facing federation maps to one existing internal session. `session_id` remains an internal protocol and isolation field until a separately planned protocol-major migration.

### Federation transport

- outbound node connections;
- authenticated relay transport;
- direct encrypted peer transport when available;
- relay fallback;
- signed route/rendezvous information;
- verified, bounded, resumable object transfer;
- restart-safe transfer state where implemented.

### Device inspection and benchmarks

- bounded local inspection of supported hardware, services, handlers, storage candidates, network paths, and data sources;
- versioned benchmark definitions and results;
- expiring and invalidatable benchmark evidence;
- safe capacity recommendations;
- no credentials, private endpoint disclosure, arbitrary remote code, or automatic authority from benchmark success.

### Contributions

- one device may contribute several capabilities simultaneously;
- contribution intent is separate from benchmark evidence and federation policy;
- enabling one contribution grants no unrelated authority;
- contributions can be disabled or suspended without deleting unrelated device membership;
- health and capacity remain fresh, authenticated, and expiring.

### Federated storage

- logical storage API rather than direct application access to physical databases;
- filesystem and supported database providers;
- storage benchmark results create candidates only;
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

- authenticated contribution enrollment and policy decision;
- fresh provider health and capacity reports;
- simultaneous trusted providers of the same type;
- remote language-model invocation through logical authenticated routes;
- local compute-handler activation without transferring executable code;
- operator-safe provider status and controls;
- restart/reconnect reconciliation from durable authority and ordered events;
- natural health expiry without deleting durable trust or membership.

### Recorder and data-source contribution

- MTConnect and future supported data-source discovery;
- stable source identity;
- explicit source selection before recording begins;
- crash-safe local recording and compatibility outputs;
- recorder contribution may coexist with AI, compute, workbench, or other capabilities on the same device.

### Compatibility

- the current Flask-first workbench remains the supported application surface;
- existing recorder durability and JSONL compatibility outputs remain supported;
- local-first workflows remain possible without connected remote providers;
- configured local or connected Ollama use remains supported during migration;
- old deployment-mode settings remain readable and migrate deterministically;
- migration does not silently enable a new contribution;
- federation capability registration does not grant storage authority;
- private service endpoints remain private by default.

## Trust model

Federation v1 is for **explicitly trusted devices and providers**.

V1 assumes:

- an authorized operator or explicit private policy controls first-time device acceptance;
- nodes are operated on trusted private networks, VPNs, or separately approved authenticated transport;
- contributed compute handlers are preinstalled and explicitly registered locally;
- provider operators are known and trusted;
- private database, Ollama, Flask, relay, worker, and storage ports are not exposed publicly by default.

V1 does not treat discovery, connection, benchmark success, contribution intent, provider health, selection, successful execution, or artifact access as equivalent authorities.

## Explicitly outside v1

The following are not supported claims for v1:

- anonymous or public provider participation;
- authority granted solely because a device is visible on the network;
- arbitrary remotely supplied code, package, module, image, shell command, or process-launch execution;
- production sandboxing for unknown third-party workloads;
- marketplace, payment, billing, reputation, dispute, or settlement systems;
- internet-wide automatic endpoint exposure;
- a full Kubernetes-style scheduler;
- production cost, energy, fairness, quota, preemption, accelerator, and placement optimization;
- durable distributed interactive AI queues and complete streaming/model lifecycle orchestration;
- complete public-relay operations, restrictive-NAT certification, or every physical topology;
- production SLO, incident-management, abuse, denial-of-service, soak, chaos, and upgrade certification;
- multi-organization policy management or comprehensive role-based administration;
- removal or protocol renaming of the internal session boundary without a versioned migration plan.

## Public terminology

Use these product concepts consistently:

- Federation
- Device
- This device
- Connected device
- Contribution
- Service
- Benchmark
- Recommended
- Enabled
- Disabled
- Temporarily unavailable
- Storage candidate
- Primary
- Replica
- AI service
- Compute service
- Recorder
- Access removed
- Degraded

Use only in advanced or administrative contexts:

- internal session ID;
- owner/member authority;
- provider enrollment records;
- terms, generations, revisions, leases, and fencing tokens;
- descriptor fingerprints and reconciliation cursors.

## Required release evidence

The release may be published only after:

- repository cleanup and documentation consolidation are complete;
- capability-first onboarding is implemented and accepted;
- one permanent Federation regression gate is green on Linux and Windows;
- clean installation succeeds from a fresh checkout;
- first-run setup succeeds without selecting a permanent role;
- at least two independently persisted devices complete discovery/join/reconnect/restart acceptance;
- safe local federation creation is demonstrated when no candidate exists;
- migration from every supported old deployment mode is demonstrated;
- one device contributes at least two capabilities simultaneously;
- benchmark expiry, invalidation, rerun, skip, and failure behavior are demonstrated;
- storage replication and controlled failover are demonstrated;
- at least one AI and one compute contribution are enabled, used, disabled or revoked, and recovered safely;
- recorder plus another contribution is demonstrated on one device;
- documentation links and commands are validated;
- generated output and unsupported experiments are absent from the production tree;
- security and limitation statements match actual behavior;
- release notes and changelog are complete;
- the exact release commit is tagged and verified.

## Versioning policy

- `1.0.x`: compatible fixes, documentation corrections, regression strengthening, and security hardening without deliberate public contract expansion;
- `1.x`: compatible product improvements, including capability-first onboarding, guided Federation UI, benchmarks, and integrated documentation where protocol authority semantics remain compatible;
- `2.0`: intentionally broader federation, trust, or protocol model requiring a new compatibility and migration decision.
