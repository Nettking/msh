# Post-v1 product roadmap

Status: active product roadmap. The repository owner has explicitly promoted capability-first onboarding and Federation UI into current planned work.

This roadmap separates product experience from the internal authority model. User-facing flows should be simple, but every action must still use authenticated, revision-fenced, fail-closed backend authority.

## Planning principles

- Treat every installation as one persistent MSH device, not one permanent deployment role.
- Let one device contribute several independent capabilities simultaneously.
- Discover and benchmark before asking the user what the device should contribute.
- Keep connection, benchmark evidence, contribution intent, provider health, selection, and authority as separate concepts.
- Preserve the validated Federation v1 protocol and authority core while simplifying the product surface.
- Keep `session_id` as an internal compatibility boundary until a separate protocol-major migration is approved.
- Prefer small compatible `1.x` updates before broader trust or protocol changes.
- Integrate UI and documentation into the existing Flask-first application.
- Do not expose private endpoints or weaken authority boundaries to simplify setup.
- Structure implementation so independent agents can work in parallel on non-overlapping paths.

## Completed V1.1 foundation — integrated documentation

The read-only documentation browser is implemented and accepted at `/docs` in the existing Flask application.

Delivered behavior includes:

- canonical repository `docs/` content;
- nested navigation, titles, breadcrumbs, and responsive layout;
- Markdown tables, code, headings, lists, blockquotes, and admonitions;
- relative document links and restricted local images;
- path-traversal and symlink-escape protection;
- no second Flask server;
- no external font or CDN requirement;
- availability before runtime startup.

The standalone `new-stuff/md_viewer/` prototype is now superseded and may be removed in a separately approved cleanup change after final comparison.

## Active V1.1 direction — capability-first onboarding and Federation UI

Purpose: make MSH quick to start and understandable without requiring users to choose a technical device role, manually manage a session, or understand provider enrollment before the system has inspected the machine.

The authoritative implementation plan is:

- `docs/implementation/capability_first_federation_plan.md`

### Product model

An MSH device may contribute any supported combination of:

- workbench/UI;
- recording and data-source access;
- language-model service;
- registered compute handlers;
- storage capacity;
- explicitly configured network/relay assistance;
- future supported capabilities.

Storage primary/replica, job owner, administrator, membership, lease, fencing, and artifact-grant roles remain internal authority states. They are not the permanent product identity of the device.

### First-run flow

```text
Identity
  -> discover, verify, join, or create a federation
  -> inspect this device
  -> run recommended benchmarks
  -> choose one or more contributions
  -> finish on Federation overview
```

Returning trusted devices reconnect automatically and rerun only expired or invalidated checks.

### Federation discovery

Discovery should:

- find existing trusted federation candidates where supported;
- show a friendly identity and verification code;
- require one first-time confirmation by default;
- reconnect automatically after trust is persisted;
- create a local federation when none exists;
- support explicit authenticated auto-accept policy only in controlled deployments;
- never treat network presence as authority.

### Benchmarks

Add a versioned local benchmark framework for:

- language models;
- explicitly registered compute handlers;
- storage-provider candidates;
- authenticated network paths;
- data-source and recorder suitability.

Benchmarks describe suitability and capacity. They never approve membership, grant provider authority, assign storage roles, dispatch jobs, or grant artifact access by themselves.

### Contribution activation

The user chooses which suitable capabilities the device should contribute. Federation policy then determines whether the contribution can activate automatically or requires an administrative decision.

One device must be able to contribute several capabilities at the same time. Enabling one contribution must not grant unrelated authority.

### Federation information architecture

```text
Federation
  Overview
  This device
  Devices
  Services
  Benchmarks
  Storage
  Jobs
  Activity
  Settings
```

#### Overview

Show:

- connection state;
- connected and unavailable devices;
- active contributions;
- pending or blocked actions;
- storage health and degraded state;
- active AI and compute capacity;
- one recommended next action.

#### This device

Show:

- stable friendly identity;
- inspection state;
- available contribution candidates;
- benchmark freshness and recommendations;
- enabled contributions;
- safe enable, disable, rerun, and repair actions.

#### Devices

Show:

- this device versus connected devices;
- friendly name and verified logical identity;
- member/connectivity state;
- contributed service types;
- safe revoke or repair actions where authorized;
- advanced identity and revision details only on demand.

#### Services

Show:

- available language-model, compute, recorder/data-source, and other service contributions;
- contributing device;
- current availability and expiry;
- safe reason codes translated into user guidance;
- no credentials, private endpoints, prompts, results, handler paths, or storage authority leakage.

#### Benchmarks

Show:

- recommended, running, passed, expired, skipped, and failed checks;
- bounded metrics and clear recommendations;
- why a result became invalid;
- rerun and cancel controls;
- clear separation between benchmark success and contribution activation.

#### Storage

Show:

- storage candidates separately from assigned primary/replica state;
- current primary and replicas;
- completeness and synchronization status;
- degraded state and why no candidate is eligible;
- safe controlled handover/recovery actions already supported;
- advanced terms, leases, fencing, watermarks, and missing ranges behind an expert panel.

#### Jobs

Show:

- bounded safe job status;
- selected service type and contributing device;
- queued, running, retrying, cancelled, succeeded, and failed state;
- no prompt, artifact, credential, or private handler leakage by default.

#### Activity

Show a bounded safe timeline of:

- joins and revocations;
- inspection and benchmark outcomes;
- contribution enable/disable decisions;
- provider health expiry and recovery;
- storage assignment and failover decisions;
- reconnect/reconciliation outcomes.

#### Settings

Show:

- federation identity and trust policy;
- first-time device acceptance policy;
- contribution defaults;
- benchmark expiry/rerun policy;
- advanced compatibility details;
- no raw secret material.

### User-language policy

Prefer:

- This device
- Federation
- Connected device
- Available contribution
- Run benchmark
- Recommended
- Enabled
- Temporarily unavailable
- Access removed
- Storage is synchronized
- Storage needs attention
- Try connection again

Hide by default:

- internal session ID;
- generation fencing;
- report revision;
- lease generation;
- descriptor fingerprint;
- reconciliation cursor;
- protocol-major mismatch internals.

The internal values remain available in advanced diagnostics and logs.

### V1.1 acceptance

- a fresh user completes setup without selecting a permanent device role;
- an existing trusted federation is discovered and joined through a verified flow;
- no candidate causes a local federation to be created safely;
- one device contributes recorder and AI simultaneously;
- benchmark success alone grants no authority;
- old deployment-mode settings migrate without data loss or silent new contributions;
- a returning device reconnects automatically;
- Federation UI works on desktop and mobile;
- every error state offers a safe next action and relevant `/docs` link;
- existing v1 regression gates remain green;
- no new authority source is introduced by the UI.

## V1.2 — operational administration and observability

Purpose: make a trusted federation easier to operate over time without changing its trust model.

Planned areas:

- persistent safe health dashboard;
- clearer component readiness and dependency state;
- structured operational logs;
- bounded metrics and alert hooks;
- contribution, storage, and job history;
- backup status and recovery rehearsal guidance;
- upgrade readiness and compatibility checks;
- safe diagnostics bundles;
- soak, restart, and controlled failure testing;
- clearer relay/direct-route visibility without broadly exposing private addresses.

## V1.3 — improved local and connected AI experience

Planned areas:

- richer model suitability benchmarks;
- clearer model/provider readiness;
- safe provider-choice explanation;
- streaming responses where compatible;
- cancellation UX;
- bounded durable request history without storing secret prompts accidentally;
- model lifecycle and warm-up status;
- better fallback explanation;
- explicit context/privacy controls.

This update must not turn a language-model contribution into storage, artifact, or command-execution authority.

## V2.0 candidate — broader network operation

Potential scope:

- operational public relay and rendezvous deployment;
- restrictive NAT and unrelated-network acceptance;
- automatic authenticated route publication and renewal;
- certificate and key rotation workflows;
- multi-route policy;
- stronger production abuse controls;
- upgrade and protocol negotiation across deployed versions;
- possible internal `session_id` terminology or protocol migration.

A V2.0 plan must define migration and compatibility before implementation begins.

## V2.x candidate — advanced scheduling

Potential scope:

- cost and latency policy;
- data locality;
- energy-aware scheduling;
- GPU/accelerator requirements;
- quotas, priorities, fairness, affinity, and preemption;
- durable distributed interactive queues;
- model warm pools and lifecycle orchestration;
- broader streaming support;
- production load and SLO acceptance.

## V2.x candidate — organizations and policy

Potential scope:

- multiple administrators and delegated roles;
- organization/project boundaries;
- policy-based provider and artifact sharing;
- durable audit and compliance views;
- retention and data-governance policy;
- federation-to-federation trust decisions.

## Future research boundary — less-trusted external providers

This remains deliberately late and separately gated:

- sandboxing and isolation;
- signed packages and provenance;
- supply-chain verification;
- resource enforcement;
- reputation and dispute systems;
- billing and marketplace behavior;
- anonymous or public participation;
- execution of externally supplied code.

No current v1 or planned 1.x UI should imply that unknown third-party execution is safe.

## Promotion and parallel-work rule

A roadmap item becomes active work only when:

1. the repository owner explicitly selects it;
2. a bounded implementation plan defines authority and compatibility impact;
3. current v1 tests remain mandatory;
4. work is separated from unrelated cleanup;
5. exit criteria and deferrals are written before implementation;
6. parallel agents receive non-overlapping file ownership and a frozen shared contract;
7. one integration agent owns shared Flask/setup files.
