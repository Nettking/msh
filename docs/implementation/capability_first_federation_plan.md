# Capability-first federation onboarding plan

Status: active integration and acceptance plan after Wave 2.

Original plan baseline: approved before CF1 implementation.

Reality-audit baseline: `ba954c91fa5f0cbd075b2210fbb1fcc717df8fa8` on `main`.

Current repository assessment:

- CF1 contracts and compatibility preview are merged;
- the generic CF2 benchmark/inspection kernel is merged, but concrete v1 benchmark adapters remain incomplete;
- CF3 discovery/join/reconnect services are merged as isolated services;
- CF4 contribution recommendation, intent, policy, and authority adapters are merged as isolated services;
- CF5 is a template/static UI shell, not a routed onboarding flow;
- CF6 is a framework-neutral projection layer, not a reachable Federation UI;
- shared Flask/setup/runtime integration has not started;
- capability-first end-to-end acceptance has not started.

The detailed repository classification is maintained in:

- `docs/implementation/capability_first_reality_audit.md`

## Objective

Replace the current role-first setup experience with a capability-first onboarding flow where every MSH installation is one persistent device that may contribute several independent services.

The target product flow remains:

```text
Start MSH
  -> load or create the stable device identity
  -> discover an existing federation
       -> one trusted candidate: offer a verified join
       -> several candidates: require selection
       -> none: create a local federation safely
  -> inspect local supported services, handlers, data sources, and resources
  -> run suitable bounded benchmarks
  -> show contribution candidates
  -> let the user enable one or more contributions
  -> publish health and capacity through existing authority paths
  -> open the Federation overview
```

## Fixed product and authority decisions

An MSH device does not have one mutually exclusive product role. A device may simultaneously provide the Flask workbench, recording, language-model service, registered compute handlers, storage capacity, and other explicitly supported capabilities.

Internal authority roles remain necessary. Storage primary/replica assignment, job ownership, membership administration, provider suspension, leases, terms, fencing, and artifact grants continue to be controlled by their existing authoritative components.

The following boundaries are non-negotiable:

- benchmark evidence never grants authority;
- discovery or network presence never grants membership;
- AI never grants compute or storage authority;
- compute exposes only locally registered handlers;
- storage candidates never receive primary or replica authority without the existing storage control plane;
- disabling or suspending a contribution fences future use without deleting unrelated device membership;
- role-first setup remains available until CF7 acceptance succeeds;
- `deployment_mode` remains readable and writable through the compatibility period;
- `session_id` remains the internal protocol and isolation boundary;
- no protocol or persistence migration occurs without a separate compatibility plan.

## Compatibility model

For the compatible implementation:

- the user-facing concept is `federation_id`;
- one federation maps to one existing internal session boundary;
- existing protocol messages, persistence, membership checks, replay, job ownership, storage authority, provider binding, and artifact authorization continue to use internal `session_id`;
- existing `server_settings.json` remains readable;
- legacy deployment modes are previewed deterministically as contribution intents;
- migration preserves configured AI, recorder, and workbench behavior;
- migration never silently enables compute or storage;
- the old setup path is retained as a fallback until independent acceptance proves the replacement.

## Repository reality after Wave 2

### CF0 — plan and contract direction

State: **contract complete**.

The direction, terminology, compatibility rules, and authority boundaries remain valid. Earlier statements that implementation had not started are superseded by this status update and the reality audit.

### CF1 — additive onboarding contracts and compatibility preview

State: **contract complete**, **isolated implementation complete**, and **regression tested**.

Delivered:

- versioned discovery, federation binding, inspection, benchmark, candidate, and intent models;
- canonical serialization and validation;
- bounded public-data and redaction checks;
- `federation_id` to internal-session mapping;
- read-only migration preview for supported legacy setup modes;
- focused malformed-state and compatibility tests;
- dedicated Ubuntu and Windows component workflow.

Not delivered:

- an authoritative persisted onboarding document;
- actual migration writes;
- Flask/setup/runtime integration;
- end-to-end migration acceptance.

### CF2 — inspection and benchmark framework

State: the **generic kernel is isolated implementation complete** and has focused tests. The full planned CF2 deliverable is not complete.

Delivered:

- trusted local benchmark registry and runner;
- bounded caller wait, cooperative cancellation, execution-slot limits, expiry, fingerprints, and safe diagnostics;
- SQLite result and run-reservation store;
- generic device-inspection probes and aggregation;
- validity and invalidation helpers;
- focused deterministic tests.

Still required:

- concrete AI benchmark migrated from the legacy setup probe;
- MTConnect/data-source candidate adapter;
- concrete registered-compute benchmark;
- concrete storage-candidate benchmark;
- concrete authenticated-network benchmark;
- supported persistence/composition decisions;
- dedicated Linux/Windows CF2 gate or inclusion in a permanent composed gate.

A timeout result must not be described as hard process termination or sandboxing. The current trusted-local thread boundary can return after a timeout while a non-cooperative probe continues.

### CF3 — federation discovery and verified join

State: **contract complete**, **isolated implementation complete**, and **regression tested**.

Delivered:

- bounded discovery service;
- configured and relay-resolver sources;
- safe public discovery results with private join material retained internally;
- several-candidate selection;
- verification-code enforcement for first trust unless explicit authenticated auto-accept policy applies;
- verified join through existing enrollment/invitation authority;
- automatic reconnect through existing membership authority;
- local federation creation through existing authority when discovery safely finds no candidate;
- dedicated Ubuntu and Windows component workflow.

Still required:

- supported Flask/setup composition;
- real independently persisted multi-device discovery/join/reconnect acceptance;
- proof for revocation, expiry, identity mismatch, and several candidates on supported installations;
- clear documentation of which discovery transports are actually supported in v1.

### CF4 — contribution recommendation and activation service

State: **contract complete**, **isolated implementation complete**, and **regression tested**.

Delivered:

- candidate generation from inspection and benchmark evidence;
- local SQLite intent persistence;
- policy evaluation;
- enable, disable, ask-later, suspend, and reconcile behavior;
- evidence-expiry suspension;
- recorder callback adapter;
- AI runtime-registration adapter with no compute/storage operation;
- registered-handler-only compute adapter with descriptor-fingerprint checks;
- candidate-only storage adapter that observes, but cannot create, control-plane assignment;
- dedicated Ubuntu and Windows component workflow with selected affected regressions.

Still required:

- supported composition to the actual recorder, AI, compute, and storage authorities;
- persistence-path, locking, upgrade, corruption, backup, and recovery decisions;
- proof that disable/suspend fences future use and stale work across restart;
- simultaneous multi-capability end-to-end acceptance.

### CF5 — capability-first onboarding UI shell

State: **isolated implementation complete as a UI shell** and **regression tested**.

Delivered:

- six-step onboarding templates and partials;
- Federation overview template shell;
- responsive CSS and progressive JavaScript;
- fixture view models;
- standalone Jinja rendering tests;
- Ubuntu and Windows component workflow.

Not delivered:

- Flask routes;
- server-side flow state;
- authoritative form handlers;
- CF1-CF4 composition;
- real refresh/restart persistence;
- browser end-to-end acceptance;
- navigation replacement.

The shell must not be described as the implemented onboarding product.

### CF6 — safe Federation projections

State: **contract complete**, **isolated implementation complete**, and **regression tested**.

Delivered:

- framework-neutral projections for all planned Federation sections;
- read-only adapters over existing federation, provider, storage, job, benchmark, and onboarding state;
- public-data safety checks;
- empty, degraded, and repair states;
- Ubuntu and Windows component workflow.

Not delivered:

- Flask instantiation;
- authorized actor/session composition in the app;
- routes or navigation;
- rendering through the supported application;
- end-to-end operator acceptance.

### CF7 — migration and end-to-end acceptance

State: not started for capability-first onboarding.

Earlier technical Federation F8 acceptance remains valuable authority-core evidence, but it does not prove the new setup, benchmark, contribution, projection, or UI composition.

Required acceptance remains:

- fresh Windows and Linux installations;
- no existing state;
- stable identity creation;
- existing federation discovery and verified join;
- no candidate and safe local federation creation;
- several-candidate selection;
- returning-device reconnect;
- migration from every supported deployment mode;
- recorder plus AI on one device;
- separate AI, registered-compute, and storage-candidate devices;
- benchmark expiry, invalidation, rerun, skip, cancellation, and failure;
- contribution disable/re-enable/suspend;
- restart and reconciliation;
- revocation and controlled rejoin;
- storage control-plane assignment without self-promotion;
- desktop and mobile UI;
- backup/recovery and malformed-state behavior;
- the complete permanent Federation v1 regression matrix.

### CF8 — retire role-first setup

State: correctly deferred.

Only after CF7 passes:

- stop writing `deployment_mode` for new installations;
- retain a bounded legacy reader for supported upgrades;
- remove role-specific gates only after equivalent capability gates exist;
- update command setup and `.env` compatibility;
- remove obsolete role tests only after replacement coverage exists;
- delete the old setup path in a separate cleanup change.

## Revised integration sequence

The original single CFI change is too broad. Integration must be split by authority and failure domain.

### CFI-1 — read-only Federation overview integration

Compose CF6 with existing authorized read-only services, expose a narrow `/federation` route, and render the CF5 overview shell. Preserve all existing setup and runtime behavior. No mutation endpoints, setup migration, or navigation replacement.

### CFI-2 — identity, compatibility preview, discovery, and connection

Compose stable identity, CF1 legacy preview, CF3 discovery, verified join/local creation, and reconnect behind supported routes. Retain role-first setup as fallback. Do not add contribution mutation or remove `deployment_mode`.

### CFI-3 — concrete benchmark integration

Add the missing concrete v1 benchmark adapters, supported store paths, and run/skip/rerun/cancel handlers. Benchmark results remain evidence only.

### CFI-4 — contribution authority binding

Bind CF4 adapters to the actual recorder, AI runtime, registered compute inventory/activation, and storage control-plane observation/fencing boundaries. Add stale/replay/restart tests before exposing actions.

### CFI-5 — compatibility-controlled startup transition

Make capability-first state the preferred setup path while retaining legacy role-first fallback and `deployment_mode`. Update navigation and command setup only after the previous integration boundaries are green.

### CFA — independent acceptance

Run CF7 independently against the integrated application. Report failures instead of changing production behavior inside the acceptance change.

## Validation rules for integration

Every integration PR must include:

- compile and Ruff checks for changed Python boundaries;
- focused route/service tests;
- affected existing Flask/setup/runtime regressions;
- authorization, redaction, stale revision, duplicate submission, and restart checks where applicable;
- Ubuntu and Windows runners;
- `docker compose config --quiet` when setup or packaging is affected;
- documentation-link and command checks when public instructions change;
- `git diff --check`;
- no weakening of existing Federation authority gates.

Manual verification is recorded separately and must name the operating system, installation method, persisted-state starting condition, devices, network constraints, and scenarios exercised.

## Stop conditions

Stop and report rather than broaden scope when:

- discovery would require unauthenticated trust;
- a benchmark would run arbitrary downloaded or remotely supplied code;
- benchmark evidence is treated as authority;
- migration would silently enable a previously unconfigured capability;
- AI activation would create compute or storage authority;
- compute would expose an unregistered handler;
- storage candidate activation would assign primary or replica authority;
- a UI simplification would bypass membership, revocation, lease, fencing, job ownership, provider health, or artifact checks;
- a protocol or persistence migration is needed without a compatibility plan;
- role-first setup would need removal before CF7;
- physical multi-device acceptance cannot be distinguished from a loopback fixture.

## Progress reporting

Do not use merged PR count as the Federation v1 completion measure.

Report progress using these milestones:

1. reusable contracts and isolated components;
2. supported application integration;
3. authority and persistence proof;
4. automated cross-platform composed regressions;
5. real Windows and Linux manual verification;
6. complete CF7 end-to-end acceptance;
7. release documentation and exact publication evidence.

At the current baseline, milestone 1 is substantially complete, milestone 2 has not started, and milestones 3-7 remain incomplete for capability-first onboarding.

## Exact next implementation unit

Proceed with **CFI-1 — read-only Federation overview integration** only.

The change should instantiate CF6 projections from existing authorized read-only services, add a narrow Federation blueprint/route, render the existing CF5 overview shell, and register the blueprint in the Flask application. It must not add onboarding writes, benchmark execution, contribution mutations, setup migration, role-gate removal, navigation replacement, protocol changes, or persistence changes.
