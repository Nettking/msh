# MSH Federation v1 scope

Status: **pre-release scope definition**  
Reviewed: **2026-08-07 Europe/Oslo**

This document defines the intended trusted Federation v1 boundary. It does not declare that a release tag exists or that end-to-end physical acceptance has passed.

## Release identity

Product milestone: **MSH Federation v1 technical baseline with capability-first onboarding**.

Current release state:

- capability-first implementation: merged;
- automated foundation and product-composition evidence: implemented;
- complete physical CF7 evidence: not accepted;
- Federation v1 end-to-end acceptance: false;
- CF8 role-first compatibility retirement: blocked;
- release tag: not created.

## Product model

Every installation is one persistent MSH device. A device may use the workbench and independently contribute any supported combination of recording, language-model, registered-compute, storage-candidate, or future versioned capabilities.

The required first-run flow is:

```text
load or create device identity
  -> discover, verify, join, reconnect to, or create a Federation
  -> inspect the device
  -> finish setup
  -> open the Federation workbench
```

Benchmarks and contribution decisions are optional follow-up work. They never approve membership, grant provider authority, assign storage, dispatch jobs, or grant artifact access by themselves.

## Supported boundary

### Identity and Federation membership

- persistent cryptographic device identity;
- explicit enrollment and revocation;
- authenticated membership and actor checks;
- candidate discovery without automatic trust;
- verified first-time join and signed expiring pairing;
- trusted reconnect;
- safe local Federation creation;
- ordered durable events and replay;
- controlled rejoin after revocation.

The user-facing `federation_id` maps to the existing internal `session_id` compatibility boundary. Removing or renaming the internal boundary requires a separate protocol-major migration.

### Transport

- outbound node connections;
- authenticated relay transport;
- direct encrypted peer transport where available;
- relay fallback;
- signed route and rendezvous information;
- bounded verified resumable object transfer;
- restart-safe transfer state where implemented.

### Device inspection and benchmarks

- bounded inspection of supported hardware, services, handlers, storage candidates, network paths, and data sources;
- versioned benchmark plans and results;
- expiry, invalidation, cancellation, and explicit rerun;
- safe capacity and suitability recommendations;
- no arbitrary remote code or authority from benchmark success.

### Contributions

- several independent contributions on one device;
- contribution intent separate from benchmark evidence and Federation policy;
- enable, disable, suspend, and reconcile behavior without deleting unrelated membership;
- authenticated expiring health and capacity;
- no authority leakage between capability types.

### Storage

- logical storage API rather than direct application access to physical databases;
- supported filesystem and database providers;
- benchmark results create candidates only;
- coordinator-authorized primary and replica assignments;
- terms, leases, fencing tokens, and stale-write rejection;
- immutable idempotent replication boundaries;
- manifests, hashes, watermarks, and missing ranges;
- completeness-aware failover and explicit degraded state;
- recovery without self-promotion.

### AI, compute, jobs, and artifacts

- simultaneous trusted providers;
- remote language-model invocation through authenticated logical routes;
- compute limited to explicitly registered local handlers;
- versioned job and attempt contracts;
- deterministic eligibility and provider selection;
- durable job ownership, duplicate suppression, retry, timeout, cancellation, and reassignment;
- stale-worker fencing;
- at most one logical committed result;
- least-privilege job-scoped artifact access and verified publication.

### Recorder and data sources

- MTConnect and future supported source discovery;
- stable source identity and explicit source selection;
- crash-safe local recording and compatibility output;
- recorder contribution coexisting with workbench, AI, compute, or other capabilities.

### Product and migration compatibility

- Flask-first workbench remains the supported application surface;
- local-first workflows remain supported;
- current recorder durability and JSONL compatibility remain supported;
- configured local and connected Ollama paths remain supported;
- supported old setup settings remain readable during migration;
- migration does not silently enable a contribution;
- private service endpoints remain private by default;
- retained role-first settings are compatibility data, not current product identity or authority.

## Trust model

Federation v1 is for explicitly trusted devices and providers on trusted private networks, approved VPNs, or separately reviewed authenticated transport.

V1 does not claim:

- public anonymous participation;
- arbitrary untrusted provider execution;
- Byzantine-fault tolerance;
- decentralized consensus without stable coordination;
- transparent distributed SQL;
- multi-primary storage across intermittently connected devices;
- safe public exposure of internal service ports.

## Release acceptance

A release tag may be created only after one exact candidate has:

- green required Ubuntu and Windows gates;
- fresh physical Windows and Linux checkout evidence;
- real multi-host Federation evidence;
- real MTConnect and target Ollama/accelerator observations;
- desktop and mobile browser review;
- recorder-plus-AI and separate AI/compute/storage-device scenarios;
- restart, expiry, disable/re-enable, revocation, fencing, and controlled-rejoin evidence;
- a complete redacted physical evidence document validating against the candidate commit;
- no unresolved authority, privacy, data-loss, platform, or browser blocker.

Acceptance truth is recorded in `catalog/federation/tests/cf7_acceptance/scenarios.json`. The false flags must remain false until a separate evidence-backed review changes them.