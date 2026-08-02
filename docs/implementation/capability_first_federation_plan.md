# Capability-first federation onboarding plan

Status: approved product-direction plan. Implementation has not started.

Baseline: `main` after the integrated `/docs` reader was accepted on the repository owner's laptop.

## Objective

Replace the current role-first setup experience with a capability-first onboarding flow where every MSH installation is one persistent device that may contribute several independent services.

The user should not need to decide whether a machine is a recorder, workbench, language-model provider, compute worker, or storage node before MSH has inspected it. MSH should discover or create a federation, inspect the device, run suitable benchmarks, recommend contributions, and let the user activate the contributions they want.

The product flow becomes:

```text
Start MSH
  -> load or create the stable device identity
  -> discover an existing federation
       -> one trusted candidate: offer a one-action verified join
       -> several candidates: let the user choose
       -> none: create a local federation
  -> inspect local services, hardware, data sources, and connectivity
  -> recommend and run bounded benchmarks
  -> show contribution candidates
  -> let the user enable one or more contributions
  -> publish authenticated health and capacity through existing authority paths
  -> open the Federation overview
```

## Product decision

An MSH device does not have one mutually exclusive product role.

A device may simultaneously provide any supported combination of:

- the Flask workbench;
- MTConnect or other data-source recording;
- a language-model service;
- registered compute handlers;
- storage capacity;
- relay or transport assistance where explicitly configured;
- future supported capabilities.

Internal authority roles remain necessary. Storage primary/replica assignment, job ownership, membership administration, provider suspension, leases, terms, fencing, and artifact grants continue to be controlled by their existing authoritative components. They are not presented as the permanent identity of the device.

## Compatibility decision

The existing `session_id` boundary is not removed in this migration.

For the first compatible implementation:

- the user-facing concept is `federation_id`;
- one federation maps to one existing internal session boundary;
- existing protocol messages, persistence, membership checks, replay, job ownership, storage authority, provider binding, and artifact authorization continue to use the internal session ID;
- UI and public documentation stop asking users to create or resume a technical session;
- a later protocol-major migration may rename or generalize the internal field only after a separate compatibility plan.

This provides the simpler product model without rewriting the validated federation core.

## Trust decision

Discovery may be automatic. Trust must remain authenticated.

The default flow is:

1. discover a federation candidate;
2. display a friendly name and short verification code;
3. require one local confirmation the first time a device joins;
4. persist the device identity and membership;
5. reconnect automatically on later starts.

A controlled private deployment may enable a policy that automatically accepts new authenticated devices, but that is an explicit federation policy. Network presence alone never grants membership, provider authority, storage access, job execution, or artifact access.

## Benchmark decision

A benchmark is evidence of suitability and capacity. It is not authority.

Every benchmark result must be:

- bound to the stable device identity;
- bound to a benchmark definition and version;
- timestamped and expiring;
- bound to relevant software, model, handler, hardware, and configuration fingerprints;
- bounded in duration and resource use;
- safe to rerun;
- free of credentials and private endpoint disclosure;
- invalidated when a relevant dependency changes;
- separate from contribution activation and federation approval policy.

A successful benchmark can create a contribution candidate. Only the user's contribution intent and the federation's policy may activate it.

## Existing implementation that can be reused

The migration should compose existing functionality rather than duplicate it:

- stable Ed25519 node identities and durable enrollment state;
- session membership, invitation, revocation, ordered events, and replay;
- authenticated relay and direct transport;
- F7 provider resource reports, eligibility, ranking, jobs, dispatch, retry, cancellation, and artifact authorization;
- F8 provider enrollment, health, remote AI binding, compute activation, operator projection, and restart reconciliation;
- local and connected Ollama connection testing;
- the existing setup AI response-time probe as the seed for a general AI benchmark;
- MTConnect network discovery and stable machine identity extraction;
- recorder durability and status services;
- storage health, replication, completeness, failover, and recovery;
- the integrated Flask application and `/docs` browser.

## Current constraints that must be migrated

The current setup is centered on `ServerSetupSettings.deployment_mode`, `DEPLOYMENT_MODES`, and `ROLE_CAPABILITIES`. UI, runtime gates, navigation, recorder startup, AI visibility, setup tests, `.env` defaults, and command setup all depend on those values.

The migration must therefore be additive first. Removing the old deployment-mode field before compatibility adapters and state migration exist would break existing installations.

## Target persisted model

Introduce a versioned onboarding document separate from the current setup file.

Suggested schema:

```json
{
  "schema": "msh.onboarding.v1",
  "device_id": "node-...",
  "federation": {
    "federation_id": "federation-...",
    "internal_session_id": "session-...",
    "state": "connected"
  },
  "inspection_revision": 3,
  "contribution_intents": {
    "recorder": "enabled",
    "language-model": "enabled",
    "compute": "disabled",
    "storage": "recommended"
  },
  "completed": true,
  "updated_at": "..."
}
```

The old `server_settings.json` remains readable during migration. A deterministic adapter maps existing deployment modes to initial contribution intents:

| Existing mode | Initial contribution intents |
| --- | --- |
| `full-server` | workbench, runtime, recorder, configured AI |
| `web-workbench` | workbench, runtime, configured AI |
| `web-ui-only` | workbench/read-only UI, configured AI |
| `recorder-only` | recorder and recorder status |
| `language-model-provider` | language-model contribution |

Migration must preserve configured Ollama information, selected model, recorder sources, polling settings, and existing data. It must not silently enable a new storage or compute contribution.

## Core contracts

### Federation discovery result

Required safe fields:

- discovery ID;
- friendly federation label;
- stable federation fingerprint;
- transport kind;
- verification code;
- whether a prior trusted relationship exists;
- expiry;
- safe reason when joining is unavailable.

Discovery output must not contain reusable enrollment secrets, credentials, private service URLs, database locations, or provider-local configuration.

### Device inspection snapshot

Required fields:

- device ID;
- inspection revision;
- operating-system family and architecture;
- bounded CPU, memory, GPU/accelerator, disk, and network observations;
- detected local supported services;
- detected registered handlers;
- detected data sources;
- recommended benchmark IDs;
- safe warnings;
- creation and expiry times.

Raw hardware details that are unnecessary for eligibility should stay local.

### Benchmark definition

Required fields:

- benchmark ID and schema version;
- capability type and protocol;
- implementation version;
- resource and duration limits;
- required local prerequisites;
- result metrics and pass/recommendation rules;
- invalidation inputs;
- privacy classification.

### Benchmark result

Required fields:

- run ID;
- device ID;
- benchmark ID/version;
- target logical service ID;
- start/end times;
- result state;
- bounded metrics;
- recommendation;
- expiry;
- dependency fingerprint;
- safe diagnostics.

### Contribution candidate

Required fields:

- candidate ID;
- device ID;
- capability type and protocol;
- safe display label;
- supporting inspection and benchmark revisions;
- recommended capacity envelope;
- prerequisites still missing;
- activation policy state;
- no endpoint, credential, handler path, or executable payload.

### Contribution intent

Required fields:

- candidate ID;
- desired state: disabled, enabled, or ask-later;
- local user decision revision;
- federation policy result;
- activation state;
- safe reason when activation is blocked.

## Benchmark families

### Language-model benchmark

Build from the existing Ollama connection and response-time probe, then add:

- model availability;
- cold and warm response latency;
- bounded generation throughput;
- declared context and modality compatibility where safely testable;
- one-request and bounded-concurrency behavior;
- timeout and malformed-response handling;
- recommendation for interactive, batch, or unsuitable use.

The benchmark uses the private local provider adapter. It never publishes the Ollama URL.

### Compute benchmark

Measure only explicitly supported local handlers:

- handler availability and descriptor fingerprint;
- bounded synthetic work;
- latency and safe concurrency;
- memory/resource failure behavior;
- cancellation support;
- recommendation per handler/capability protocol.

It must not import, download, install, or execute code supplied by another node.

### Storage benchmark

Evaluate a local storage provider candidate using the existing storage contract:

- durable write/read round trip;
- immutable/idempotent behavior;
- restart persistence where the provider supports it;
- bounded throughput and latency;
- available capacity reported in coarse safe bands;
- synchronization prerequisites;
- suitability as a candidate only.

The benchmark never self-assigns primary or replica authority.

### Network benchmark

Measure the authenticated federation path:

- relay reachability;
- direct-path availability where configured;
- bounded latency and throughput samples;
- connection stability;
- suitability warnings for interactive AI, compute dispatch, storage replication, and object transfer.

Private addresses and route descriptors remain in restricted diagnostics.

### Data-source inspection

MTConnect and future source discovery produce contribution candidates rather than a machine role:

- stable source identities;
- reachability;
- protocol compatibility;
- recording prerequisites;
- explicit source selection before recording starts.

## Target UI

The main navigation should expose:

```text
Home
Monitor
Knowledge
Federation
System
Docs
```

The Federation section should contain:

```text
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

### First-run onboarding

The old first step, "What should this computer do?", is replaced by:

1. **Identity** — create or load this device;
2. **Federation** — discover, verify, join, or create locally;
3. **Inspect** — identify supported local services and data sources;
4. **Benchmarks** — run recommended checks, with skip and rerun behavior;
5. **Contributions** — enable any combination of suitable services;
6. **Finish** — show the connected state and next recommended action.

### Returning-device startup

A returning trusted device should:

- load its stable identity;
- reconnect to its saved federation automatically;
- reconcile membership and active contributions;
- rerun only expired or invalidated health/benchmark checks;
- open the Federation overview;
- show a guided repair action if reconnection fails.

A runtime progress choice may remain where the workbench genuinely needs it, but it must not be described as creating or resuming the federation.

## Implementation sequence

### CF0 — plan and contract freeze

Deliverables:

- this plan;
- roadmap, release-scope, closeout, and handoff amendments;
- fixed terminology and compatibility decisions;
- parallel file-ownership plan.

Exit criteria:

- no runtime behavior changes;
- no protocol field removed or renamed;
- the first implementation contracts are unambiguous.

### CF1 — additive onboarding contracts and compatibility adapter

Deliver:

- pure versioned models for discovery, inspection, benchmark results, candidates, and intents;
- canonical serialization and validation;
- `federation_id` to internal-session mapping contract;
- legacy deployment-mode to contribution-intent adapter;
- read-only migration preview;
- tests for every existing setup mode and malformed state.

Do not change the browser wizard in CF1.

Exit criteria:

- old setup files remain readable;
- no contribution is newly activated by migration;
- existing v1 tests remain green;
- unsupported protocol majors fail closed.

### CF2 — device inspection and benchmark framework

Deliver:

- benchmark registry and runner;
- bounded execution, cancellation, expiry, fingerprints, and safe diagnostics;
- device inspection service;
- initial AI benchmark migrated from the setup probe;
- MTConnect discovery adapter producing candidates;
- extension seams for compute, storage, and network benchmarks.

Exit criteria:

- benchmark execution is local and bounded;
- results are durable and restart-safe;
- benchmark success grants no authority;
- tests cover expiry, invalidation, timeout, duplicate run IDs, and redaction.

### CF3 — federation discovery and verified join

Deliver:

- bounded local discovery interface;
- discovery adapters for already-supported relay/configuration paths;
- one-action verified join using existing enrollment/invitation authority;
- automatic reconnect for already trusted devices;
- local federation creation when no candidate exists;
- several-candidate selection;
- safe failure reasons.

Exit criteria:

- discovery alone grants nothing;
- first trust requires verification unless explicit policy allows authenticated auto-accept;
- reconnect does not mint new authority;
- internal session behavior remains compatible.

### CF4 — contribution recommendation and activation service

Deliver:

- candidate generation from inspections and benchmark results;
- local contribution-intent persistence;
- policy evaluation;
- adapters to existing recorder, AI provider, compute-handler, and storage-provider authorities;
- enable, disable, suspend, and reconcile behavior;
- simultaneous multi-capability contribution from one device.

Exit criteria:

- enabling AI grants no storage or compute authority;
- enabling compute exposes only registered handlers;
- storage remains candidate-only until control-plane assignment;
- disabling a contribution fences future use without deleting unrelated device membership.

### CF5 — capability-first onboarding UI

Deliver:

- the new six-step onboarding flow;
- no deployment-role choice;
- progress that survives refresh/restart;
- benchmark progress and rerun controls;
- contribution selection;
- migration display for existing installations;
- accessible desktop and mobile layouts;
- direct links to `/docs`.

The old wizard remains behind a compatibility fallback until CF7 acceptance.

Exit criteria:

- a fresh user can complete setup without understanding session, provider enrollment, primary/replica, or handler terminology;
- a returning installation retains all previous configured behavior;
- skipped benchmarks do not silently enable contributions.

### CF6 — Federation product UI

Deliver:

- Federation in desktop and mobile navigation;
- Overview, This device, Devices, Services, Benchmarks, Storage, Jobs, Activity, and Settings shells;
- live projections from existing safe operator services;
- user-language status and one recommended next action;
- advanced technical details on demand;
- graceful empty/degraded states.

Exit criteria:

- the current provider operator surface is reachable through the product UI;
- missing runtime context produces a useful onboarding or repair action rather than a raw 503 page;
- no private endpoint or credential is rendered.

### CF7 — migration and end-to-end acceptance

Required acceptance:

- fresh Windows and Linux checkout;
- no existing state;
- existing federation discovered and joined;
- no federation found and local federation created;
- returning trusted device reconnect;
- migration from each old deployment mode;
- one device contributing recorder plus AI simultaneously;
- separate devices contributing AI, compute, and storage candidates;
- benchmark expiry and rerun;
- contribution disable/re-enable;
- restart/reconciliation;
- revocation and rejoin behavior;
- mobile and desktop UI;
- complete v1 regression gates.

### CF8 — retire role-first setup

Only after CF7 passes:

- stop writing `deployment_mode` for new installations;
- retain a bounded legacy reader for supported upgrades;
- remove role-specific UI gates replaced by contribution checks;
- update command setup and `.env` compatibility;
- remove obsolete role tests only after equivalent capability tests exist;
- delete the old setup path in a separately reviewed cleanup change.

## Parallel multi-agent execution plan

Parallel work is allowed only after CF1 contracts are merged and frozen.

### Wave 0 — one contract agent

**Agent CF1 — contracts and migration preview**

Owns:

- `catalog/federation/onboarding_models.py`;
- `catalog/federation/onboarding_compat.py`;
- focused new tests under `catalog/federation/tests/`;
- contract sections of this plan if corrections are required.

Must not edit Flask templates, setup routes, benchmark implementation, transport, provider runtime, or existing authority stores.

CF1 must merge before the parallel branches are created.

### Wave 1 — three parallel agents

All Wave 1 branches start from the exact CF1 merge commit.

#### Agent CF2-A — inspection and benchmark engine

Owns only:

- `catalog/capabilities/benchmarking/**`;
- `catalog/capabilities/tests/test_benchmarking_*.py`;
- benchmark fixtures under a new dedicated test-data directory.

May import frozen CF1 contracts. Must not edit setup, Flask navigation, federation discovery, existing AI runtime selection, or provider enrollment.

#### Agent CF3-A — federation discovery and onboarding authority adapter

Owns only:

- `catalog/federation/onboarding_discovery/**`;
- `catalog/federation/tests/test_onboarding_discovery_*.py`;
- narrow adapters to existing coordinator APIs in new files.

Must not change relay protocol schemas, existing session persistence, setup UI, benchmark code, or provider health logic.

#### Agent CF5-A — UI shell and static behavior

Owns only new UI files:

- `catalog/flask_app/templates/onboarding.html`;
- `catalog/flask_app/templates/federation_overview.html`;
- new partials under `catalog/flask_app/templates/federation/`;
- `catalog/flask_app/static/css/federation.css`;
- `catalog/flask_app/static/js/onboarding.js`;
- template/static-focused tests in new files.

Uses documented view-model fixtures and must not edit `app.py`, `routes.py`, `server_setup_routes.py`, `server_setup_service.py`, `base.html`, or `startup.html` during Wave 1.

### Wave 2 — two parallel agents

Starts after CF2-A and CF3-A are merged.

#### Agent CF4-A — contribution service

Owns:

- new contribution service modules under `catalog/capabilities/contributions/**`;
- adapters in new files for recorder, AI, compute, and storage candidates;
- focused contribution tests.

Must not edit shared Flask/setup files.

#### Agent CF6-A — safe Federation projections

Owns:

- new framework-neutral projection services;
- new tests for overview/device/service/benchmark states;
- adapters to the existing provider operator surface in new files.

Must not edit shared Flask/setup files.

### Wave 3 — one integration agent

**Agent CFI — Flask/setup integration and migration** is the only agent allowed to modify shared integration files:

- `catalog/flask_app/app.py`;
- `catalog/flask_app/routes.py`;
- `catalog/flask_app/server_setup_routes.py`;
- `catalog/flask_app/services/server_setup_service.py`;
- `catalog/flask_app/templates/base.html`;
- `catalog/flask_app/templates/startup.html`;
- `setup_msh.py`;
- `.env.example`;
- existing setup/navigation tests that must be migrated.

The integration agent composes already-merged contracts, services, templates, and projections. It must not redesign their contracts during integration. Contract changes require a separate CF1 amendment reviewed before continuing.

### Wave 4 — independent acceptance agent

**Agent CFA — regression and acceptance** owns:

- new end-to-end acceptance tests;
- CI workflow additions;
- migration fixtures for every old setup mode;
- documentation-command validation;
- no production behavior except minimal test seams approved in review.

This agent validates the integrated result independently and reports gaps instead of silently broadening scope.

## File-conflict rules for concurrent agents

1. Every agent uses a separate branch created from the documented baseline commit.
2. Two active agents must not own the same production file.
3. Shared files are reserved for the integration wave.
4. Existing protocol, authority, and persistence modules are read-only unless the plan explicitly assigns a narrow adapter file.
5. New contracts are merged before dependent parallel work starts.
6. Each PR states its owned paths and proves that no other path changed.
7. Each PR includes focused tests and runs the applicable existing regression subset.
8. Agents do not merge another agent's branch into their own branch; they rebase or recreate from updated `main` after prerequisite merges.
9. Cross-agent interface changes require a small contract PR, not ad hoc changes in both branches.
10. The integration agent merges only green, independently reviewable units.

## Suggested branch and PR sequence

```text
agent/cf1-onboarding-contracts
  -> merge

parallel from the CF1 merge:
  agent/cf2-benchmark-engine
  agent/cf3-federation-discovery
  agent/cf5-ui-shell
  -> merge independently when green

parallel from updated main:
  agent/cf4-contribution-service
  agent/cf6-federation-projections
  -> merge independently when green

agent/cfi-capability-onboarding-integration
  -> compose shared setup and Flask files
  -> merge after full matrix

agent/cfa-capability-onboarding-acceptance
  -> independent acceptance and release-plan update
```

At least three agents can work concurrently in Wave 1 without overlapping production files. Two can work concurrently in Wave 2. The shared setup migration is intentionally serialized because splitting edits to the same Flask and setup files would create avoidable conflicts and inconsistent gating.

## Required validation throughout

Every implementation PR must include:

- Python compilation for changed packages;
- Ruff for changed Python files;
- focused deterministic tests;
- protocol-major and malformed-input rejection where contracts are involved;
- redaction checks;
- restart/idempotency checks where state is durable;
- Windows and Linux coverage when platform behavior differs;
- `docker compose config --quiet` where packaging or setup is affected;
- `git diff --check`;
- no weakening of existing Federation v1 regression gates.

The integration and acceptance waves must run the full permanent federation matrix, Flask/setup tests, recorder tests, AI runtime tests, provider federation tests, storage tests, direct/relay transport tests, and documentation-link checks.

## Stop conditions

Stop and report rather than broaden scope when:

- discovery would require unauthenticated trust;
- a benchmark would require arbitrary downloaded or remotely supplied code;
- a benchmark result is being used as authority by itself;
- migration would silently enable storage, compute, recorder, or AI contributions not previously configured;
- a user-facing simplification would remove membership, revocation, fencing, lease, or artifact checks;
- two active agents need to edit the same shared integration file;
- removing `session_id` becomes necessary for a compatible unit;
- a unit requires a protocol-major change not covered by this plan;
- acceptance cannot distinguish a real multi-device path from a loopback-only simulation.

## Exact next implementation unit

Proceed with **CF1 only**:

1. add pure onboarding contracts;
2. add the `federation_id` compatibility mapping;
3. add the legacy deployment-mode migration preview;
4. add exhaustive contract and migration tests;
5. do not change the current setup UI;
6. merge CF1 before creating the parallel CF2, CF3, and CF5 branches.
