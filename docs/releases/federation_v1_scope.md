# FCP Federation v1 scope

Status: **pre-release scope definition**

Reviewed: **2026-08-16 Europe/Oslo**

This document defines the intended trusted Federation v1 boundary. It does not declare that a release tag exists or that end-to-end physical acceptance has passed.

## Release identity

Product milestone: **FCP Federation v1 technical baseline with capability-first onboarding**.

Current release state:

- capability-first implementation: merged;
- role-first installed-product runtime retirement (CF8): merged;
- automated foundation and product-composition evidence: implemented;
- verified manual Federation-wide runtime-update capability: merged;
- standalone recorder Federation bootstrap/publication and bounded remote recorder control: merged;
- MTConnect operational segmentation S1-S7 and read-only closeout validation: merged;
- Federation-scoped human sign-in with leader-held credentials and signed member assertions: implemented;
- complete physical CF7 evidence: not accepted;
- Federation v1 end-to-end acceptance: false;
- release tag: not created.

## Product model

Every installation is one persistent FCP device. A device may use the workbench and independently contribute any supported combination of recording, language-model, registered-compute, storage-candidate, or future versioned capabilities.

The required first-run flow is:

```text
load or create device identity
  -> discover, verify, join, reconnect to, or create a Federation
  -> inspect the device
  -> finish setup
  -> open the Federation workbench
```

Benchmarks and contribution decisions are optional follow-up work. They never approve membership, grant provider authority, assign storage, dispatch jobs, grant artifact access, or authorize a software update by themselves.

The former role-first installed-product runtime is not part of the current product authority model. Retained old setup state/readers and command aliases are migration/administration compatibility only.

## Supported boundary

### Identity and Federation membership

- persistent cryptographic device identity;
- explicit enrollment and revocation;
- authenticated membership and actor checks;
- candidate discovery without automatic trust;
- verified first-time join and signed expiring pairing;
- trusted reconnect;
- safe local Federation creation;
- ordered durable events and replay; and
- controlled rejoin after revocation.

The current browser pairing flow issues signed one-use `FCP1-...` codes valid for up to 10 minutes and permits a fresh code to be generated when another join attempt is needed.

The user-facing `federation_id` maps to the existing internal `session_id` compatibility boundary. Removing or renaming the internal boundary requires a separate protocol-major migration.

### Transport

- outbound node connections;
- authenticated relay transport;
- direct encrypted peer transport where available;
- relay fallback;
- signed route and rendezvous information;
- bounded verified resumable object transfer;
- restart-safe transfer state where implemented; and
- bounded authenticated application/event messages for current distributed control/data paths.

### Device inspection and benchmarks

- bounded inspection of supported hardware, services, handlers, storage candidates, network paths, and data sources;
- versioned benchmark plans and results;
- invalidation, cancellation, and explicit rerun;
- safe capacity and suitability recommendations; and
- no arbitrary remote code or authority from benchmark success.

### Contributions

- several independent contributions on one device;
- contribution intent separate from benchmark evidence and Federation policy;
- enable, disable, suspend, and reconcile behavior without deleting unrelated membership;
- authenticated health and capacity; and
- no authority leakage between capability types.

### Storage

- logical storage API rather than direct application access to physical databases;
- supported filesystem and database providers;
- benchmark results create candidates only;
- coordinator-authorized primary and replica assignments;
- terms, leases, fencing tokens, and stale-write rejection;
- immutable idempotent replication boundaries;
- manifests, hashes, watermarks, and missing ranges;
- completeness-aware failover and explicit degraded state; and
- recovery without self-promotion.

### AI, compute, jobs, and artifacts

- simultaneous trusted providers;
- remote language-model invocation through authenticated logical routes;
- compute limited to explicitly registered local handlers;
- versioned job and attempt contracts;
- deterministic eligibility and provider selection;
- durable job ownership, duplicate suppression, retry, timeout, cancellation, and reassignment;
- stale-worker fencing;
- at most one logical committed result; and
- least-privilege job-scoped artifact access and verified publication.

### Recorder and data sources

- MTConnect and future supported source discovery;
- stable source identity and explicit source selection;
- crash-safe local loss-aware recording and compatibility output;
- recorder contribution coexisting with workbench, AI, compute, or other capabilities;
- a headless standalone recorder that can create/reuse a stable FCP identity and join through the normal signed pairing flow;
- default bounded private-network MTConnect discovery during first standalone-recorder configuration;
- first-configuration auto-selection without silently repopulating a later deliberately empty source set;
- checkpoint-gated durable publication through Federation logical-storage authority; and
- bounded Federation recorder control from any trusted member, where scans execute on the recorder host and additions select only IDs returned by that recorder's latest scan.

Recorder control does not provide arbitrary shell execution, unrestricted network scanning, or remote arbitrary URL/credential injection.

### MTConnect operational segmentation

Federation v1 includes a synchronous, machine-neutral interpretation layer over
canonical MTConnect observations. The accepted v1 pipeline is:

```text
immutable recorder evidence
  -> canonical observations
  -> semantic roles
  -> per-device execution/context timeline
  -> MachineRun
  -> ProductionCycle
  -> OperationalEpisode
  -> durable derived projection
```

The operational segmentation boundary includes:

- Agent-wide sequence continuity established before device partitioning;
- device-isolated execution and manufacturing context reconstruction;
- deterministic `MachineRun` boundaries and exact duration accounting;
- conservative `ProductionCycle` classification using explicit process-motion and manufacturing-context evidence rather than `ACTIVE == production`;
- deterministic `OperationalEpisode` segmentation at cycle edges and direct concrete resolved tool transitions;
- explicit partial/gap/timestamp-regression semantics instead of silent continuity repair;
- deterministic identities, content fingerprints, provenance, and rebuild behaviour;
- a disposable derived SQLite projection separate from canonical/raw recorder evidence;
- bounded queries for runs, cycles, episodes, context transitions, boundaries, provenance, and projection/device status; and
- read-only end-to-end/reference-shape validation through the existing raw-to-canonical pipeline.

The v1 interpretation is deliberately conservative. Ambiguous or multi-channel
execution authority fails closed. Unknown tool context does not fabricate a
precise tool change. Process motion is evidence of process activity, not proof of
cutting or material removal. Full multi-path operational lanes, a separate
inferred subprogram role, activity phases, learned feature vectors, behavioural
baselines, anomaly detection, prediction, operator recommendations, OSL/SysML
integration, and new scheduling/Federation semantics remain outside this release
boundary.

### Manual Federation-wide software updates

The coordinator/session creator may explicitly run **Check for updates** and **Update all devices** for normal updater-capable FCP installations.

The v1 update boundary includes:

- exact approved source-repository `main` commit targeting;
- per-host clean-checkout, trusted-remote, ancestry, and fast-forward validation;
- no peer-supplied repository/branch/path/executable/command authority;
- a separate host-owned update agent rather than Git/Docker authority inside Flask;
- rebuild/restart of the normal `relay`, `flask`, and Compose-managed `recorder` runtime;
- saved-setup resume and required-model verification; and
- success only after the running runtime proves the exact requested commit.

The internal successful state is `runtime_verified`; the product presents it as **Updated**.

The update is explicit/manual, not automatic on startup/reconnect. Offline devices are not silently queued for later.

A standalone recorder launched directly through `python start_recorder.py` does not currently host the normal Flask/host-agent update pipeline and therefore remains outside automatic process restart by **Update all devices**.

### Human authentication and authorization

- browser users sign in with human accounts that never reuse Federation node IDs, pairing grants, recorder keys, or device identities;
- the Federation creator/leader is the human credential authority for a connected Federation;
- passwords and password hashes remain only in the leader's local authentication database and are never replicated through Federation state;
- trusted member devices use a browser redirect to the leader and accept only short-lived Ed25519-signed assertions bound to the Federation session, target node, human subject, roles, active state, random browser state, and expiry;
- non-secret human authorization metadata (`email`, `active`, and role names), the leader's public sign-in identity/origin, and each member's own browser origin are carried on authenticated Federation session events;
- coordinator policy permits only the Federation creator to publish human authority/user state, while members may advertise only their own browser endpoint;
- `viewer`, `operator`, and `admin` map onto a central server-side permission policy that routes check by permission rather than by role name;
- `/admin/users` is authoritative on the leader; member user-administration requests redirect to the leader or fail closed for unsafe writes;
- member-local password login is disabled by default, including during Federation outages, so an old local credential store cannot silently bypass Federation revocation; and
- an explicit `FCP_HUMAN_AUTH_LOCAL_FALLBACK=1` recovery setting can re-enable local member password login when an operator accepts that weaker boundary.

Human authentication and device/Federation authority remain distinct. A human `admin` permission allows the web application to request Federation administration, but does not create node membership or bypass coordinator authorization.

The coordinator event audit still identifies the acting **device** for shared operations. End-to-end attribution of every non-auth Federation write to the initiating human is separate work; the new human-auth events themselves carry the human subject/authorization metadata needed for sign-in and revocation projection.

### Shared operator knowledge

- operator confirmations, first-part checks, quality outcomes, machine notes,
  and operator strategy records are shared through the authoritative session
  log;
- local JSON files remain compatibility caches and migration inputs;
- credential-shaped content is refused rather than written to an append-only log
  that has no retention; and
- a device with an unreachable Federation keeps using its local cache and offers
  unshared documents again on the next successful read.

Machine and sensor inventory remains device-local, so shared records carry a
denormalized machine label. Sharing the inventory itself is open work.

### Federated JSONL corpus

- ordinary JSONL under `data/` is published as content-addressed chunks so the
  existing analysis stack can read another device's data unchanged;
- the active recorder JSONL path and the local Federation mirror are excluded;
- browser uploads are included by default and can be withheld per installation;
  and
- the local mirror of remote files is bounded by a total quota.

This path applies no content-schema allowlist and is authorized by Federation
membership. It is weaker than the recorder telemetry contract by design.

### Product and migration compatibility

- Flask-first workbench remains the supported application surface;
- local-first workflows remain supported;
- current recorder durability and JSONL compatibility remain supported;
- configured local and connected Ollama paths remain supported;
- older Windows installations may use the conservative one-shot migration bootstrap to preserve identity/Federation/data while joining the current launcher/update-agent path;
- migration does not silently enable a contribution;
- private service endpoints remain private by default; and
- retained role-first settings are migration data, not current product identity or authority.

## Trust model

Federation v1 is for explicitly trusted devices and providers on trusted private networks, approved VPNs, or separately reviewed authenticated transport.

V1 does not claim:

- public anonymous participation;
- arbitrary untrusted provider execution;
- Byzantine-fault tolerance;
- decentralized consensus without stable coordination;
- transparent distributed SQL;
- multi-primary storage across intermittently connected devices;
- safe public exposure of internal service ports; or
- a general remote shell/host administration protocol.

## Release acceptance

A release tag may be created only after one exact candidate has:

- green required Ubuntu and Windows gates;
- fresh physical Windows and Linux checkout evidence;
- real multi-host Federation evidence;
- real MTConnect and target Ollama/accelerator observations;
- desktop and mobile browser review;
- recorder-plus-AI and separate AI/compute/storage-device scenarios;
- restart, evidence invalidation/rerun, disable/re-enable, revocation, fencing, and controlled-rejoin evidence;
- physical review of the current update/pairing/standalone-recorder/human-sign-in surfaces relevant to the candidate;
- a complete redacted physical evidence document validating against the candidate commit; and
- no unresolved authority, privacy, data-loss, platform, browser, migration, update, recorder-control, or human-authentication blocker.

Acceptance truth is recorded in `catalog/federation/tests/cf7_acceptance/scenarios.json`. The false flags must remain false until a separate evidence-backed review changes them.
