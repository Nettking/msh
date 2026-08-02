# MSH Federation v1.0 scope

Status: pre-release scope definition. This document defines the intended trusted-federation boundary; it does not declare that `v1.0.0` has been released.

Reality-audit baseline: `ba954c91fa5f0cbd075b2210fbb1fcc717df8fa8` on `main`.

Current implementation evidence is summarized in:

- `docs/implementation/capability_first_reality_audit.md`.

## Release identity

Product milestone: **MSH Federation v1.0 technical authority baseline plus integrated and accepted capability-first product flow**.

The authority baseline through F8.7 exists. The capability-first release layer is not yet integrated or accepted. CF1-CF6 currently provide isolated contracts, services, a UI shell, and safe projections rather than a supported end-to-end product.

## Intended product model

Every installation is one persistent MSH device. A device may contribute several supported capabilities simultaneously. The intended first-run flow is:

```text
load or create device identity
  -> discover, verify, join, or create a federation
  -> inspect the device
  -> run suitable bounded benchmarks
  -> select one or more contributions
  -> reconnect and reconcile automatically on later starts
```

At the audited baseline, the supported setup remains role-first and persists `deployment_mode`. The intended flow becomes release scope only after integration and CF7 acceptance.

## Supported authority boundary

Federation v1 is for explicitly trusted devices and providers.

The following existing authority properties remain in scope:

- persistent node identities;
- explicit enrollment, membership, invitation, revocation, ordered events, and replay;
- authenticated relay and supported direct transport;
- storage primary/replica assignment, terms, leases, fencing, completeness, failover, and recovery;
- durable capability-specific job ownership, dispatch, retry, cancellation, stale-worker fencing, and artifact authorization;
- trusted provider enrollment, expiring health, remote AI binding, registered compute activation, operator projections, and restart reconciliation;
- recorder durability and supported compatibility outputs.

Capability-first integration must compose these authorities rather than create replacements.

## Capability-first scope and current status

### Contracts and compatibility

In scope and isolated implementation complete:

- versioned discovery, federation binding, inspection, benchmark, contribution candidate, and intent contracts;
- deterministic `federation_id` mapping to the existing internal session boundary;
- read-only migration preview for supported legacy deployment modes;
- no protocol or existing persistence field removal.

Not yet release-complete:

- actual onboarding-state persistence and migration writes;
- compatibility-tested upgrade and rollback behavior.

### Device inspection and benchmarks

In scope and partly implemented:

- generic trusted-local benchmark registry/runner;
- fingerprints, expiry, invalidation, safe diagnostics, and durable results;
- generic device inspection seams.

Still required for v1 claims:

- concrete AI benchmark integrated from the legacy setup probe;
- concrete MTConnect/data-source inspection adapter;
- registered-compute benchmark;
- storage-candidate benchmark;
- authenticated-network benchmark;
- supported persistence/composition and cross-platform composed gates.

Benchmark timeout currently bounds caller wait and does not establish sandboxing or hard termination of a non-cooperative trusted local probe.

### Federation discovery and connection

In scope and isolated implementation complete:

- bounded configured/relay-resolver discovery;
- safe candidate projection;
- verified join through existing enrollment/invitation authority;
- reconnect through existing membership authority;
- safe local federation creation when discovery completes with no candidate;
- several-candidate selection.

Still required:

- supported Flask/setup integration;
- real independently persisted multi-device acceptance on Windows and Linux;
- accurate public documentation of supported discovery transports.

### Contributions

In scope and isolated implementation complete:

- one device may express several contribution intents;
- candidate generation from inspection and benchmark evidence;
- local intent storage and policy evaluation;
- enable, disable, suspend, and reconcile seams;
- AI adapter without compute/storage authority;
- compute adapter restricted to registered handlers;
- candidate-only storage adapter that cannot assign authority;
- disabling/suspending through fencing callbacks without membership deletion.

Still required:

- binding to actual supported recorder, AI, compute, and storage authorities;
- stale/replay/restart proof;
- simultaneous multi-capability end-to-end acceptance.

### Product UI

In scope but not integrated:

- six-step onboarding template/static shell;
- Federation overview template shell;
- framework-neutral safe projections for Overview, This device, Devices, Services, Benchmarks, Storage, Jobs, Activity, and Settings.

No supported Flask route currently composes these components. The UI shell and projections must not be described as a reachable Federation product until integrated.

## Mandatory authority separations

Federation v1 must preserve all of the following:

- benchmark evidence gives no authority;
- discovery or network presence gives no membership;
- contribution intent is not federation approval;
- provider health is not membership, storage leadership, or job ownership;
- AI gives no compute or storage authority;
- compute can invoke only explicitly registered handlers;
- storage candidates receive no primary/replica authority without the existing control plane;
- disabling or suspending a contribution fences future use without deleting unrelated membership;
- `session_id` remains the internal protocol/isolation field;
- `deployment_mode` remains during the compatibility period;
- role-first setup remains until CF7 succeeds;
- protocol and persistence migration require separate compatibility plans.

## Compatibility boundary

- The current Flask-first application remains the supported surface.
- Existing setup settings remain readable.
- Existing recorder and AI configurations remain preserved.
- Migration never silently enables compute or storage.
- Local-first operation remains possible.
- Private service endpoints remain private by default.
- Existing technical Federation regressions remain mandatory during capability-first integration.

## Required release evidence

Release publication requires all of the following:

- bounded integration of CF1-CF6 into the supported Flask/setup/runtime path;
- concrete v1 benchmark adapters;
- explicit compatible persistence for onboarding, benchmarks, and contribution intents;
- one permanent composed Federation regression gate on Linux and Windows;
- fresh real Windows installation and manual verification;
- fresh real Linux installation and manual verification;
- first-run setup without selecting a permanent role;
- at least two independently persisted devices completing verified discovery/join/reconnect/restart;
- safe local federation creation when no candidate exists;
- migration from every supported old deployment mode;
- one device contributing at least recorder plus AI;
- separate AI, registered-compute, and storage-candidate participation;
- benchmark expiry, invalidation, rerun, skip, cancellation, timeout, and failure;
- safe enable/use/disable/suspend/recovery for supported contributions;
- storage replication and controlled failover through existing authority;
- revocation and controlled rejoin;
- desktop and mobile browser acceptance;
- backup/recovery and corrupted-state rehearsal;
- documentation commands and links checked;
- cleanup and canonical documentation complete;
- release notes and changelog matching actual limitations;
- exact verified release commit and tag.

Component workflows, merge count, fixture rendering, and earlier authority-core acceptance are not substitutes for this evidence.

## Explicitly outside v1

- anonymous or public provider participation;
- authority from network presence;
- arbitrary remotely supplied code, package, image, command, or process execution;
- production sandboxing for unknown workloads;
- marketplace, billing, reputation, or dispute systems;
- internet-wide automatic endpoint exposure;
- full Kubernetes-style scheduling;
- advanced cost, energy, fairness, quota, preemption, and placement optimization;
- complete public-relay/restrictive-NAT certification;
- production SLO, incident, abuse, soak, chaos, and broad upgrade certification;
- multi-organization policy management;
- removal or protocol renaming of the internal session boundary without a versioned migration plan.

## Versioning policy

- `1.0.x`: compatible fixes, documentation corrections, regression strengthening, and security hardening without deliberate public-contract expansion;
- `1.x`: compatible product improvements after v1 publication where protocol authority semantics remain stable;
- `2.0`: broader trust, protocol, or compatibility model requiring explicit migration planning.

## Current release decision

Federation v1 is not release-ready at the audited baseline. The next release-progress boundary is a read-only Federation overview integrated into the supported Flask application, followed by the remaining bounded integration and CF7 acceptance work.
