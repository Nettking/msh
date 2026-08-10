# Capability-first Federation implementation and acceptance plan

Status: **active authoritative plan**.

Last updated: **2026-08-05 Europe/Oslo**.

Current merged baseline: `main` at `d9d6c895962ccc11de68f04a8ff4414df1681e4b` after PR #185.

This document replaces the original pre-implementation sequencing in earlier
versions of this file. Capability-first onboarding is implemented on `main`.
The remaining work is runtime-parity validation, documentation reconciliation,
complete physical CF7 acceptance, and only then a separately reviewed CF8
retirement of role-first setup.

## How to use this document

This file is the canonical source for:

- durable product and authority decisions;
- current capability-first product behavior;
- implementation status;
- remaining implementation and acceptance sequence;
- stop conditions for future agents.

The following sources have narrower responsibilities:

- `catalog/federation/tests/cf7_acceptance/scenarios.json` is the machine-readable
  source of truth for acceptance claims;
- `docs/implementation/cf7b_product_physical_acceptance.md` is the detailed
  physical acceptance runbook;
- `docs/implementation/cf7c_physical_test_readiness.md` is the operator
  preparation guide;
- phase-specific CFI and CF7 documents are historical delivery records unless
  they explicitly say that they describe the current merged baseline.

When a historical delivery note conflicts with this file or with
`scenarios.json`, this file defines the intended product behavior and the
manifest defines what has actually been accepted.

## Current repository reality

The capability-first implementation is no longer a future design.

PR #185 merged the complete capability-first Federation onboarding baseline to
`main`, including:

- stable device identity and Federation connection composition;
- signed pairing and trusted reconnect;
- device inspection;
- benchmark planning and execution;
- contribution recommendation and intent handling;
- compatibility-controlled startup;
- the Federation read-only product surface;
- recovery and fresh-reset behavior;
- permanent Ubuntu and Windows gates;
- automated CF7 product-composition coverage;
- physical-test readiness tooling;
- the streamlined three-step first-run experience.

The current mandatory first-run flow is:

```text
Identity
  -> Federation
  -> Inspect
  -> finish setup
  -> open Federation
```

A current device inspection is sufficient to finish setup.

Benchmarks and optional contribution decisions are available as follow-up work.
They do not block access to the normal FCP workbench.

Fast completion:

- enables the workbench and runtime;
- grants no recorder, language-model, compute, or storage contribution
  authority;
- records those optional capabilities as `ask-later`;
- preserves contribution choices when the operator has already completed a
  fully reviewed contribution flow;
- retains the compatible role-first settings carrier until CF8.

Physical pairing and Federation connection have been verified by the operator.
That verification is useful evidence, but it is not complete CF7 physical
acceptance.

The acceptance manifest still correctly records:

```json
{
  "federation_v1_end_to_end_accepted": false,
  "capability_first_onboarding_end_to_end_accepted": false,
  "physical_evidence_accepted": false
}
```

## Product objective

Every FCP installation is one persistent device.

A device may contribute several independent capabilities simultaneously. The
user must not be forced to choose one permanent technical role before FCP has
identified the device and connected it to a Federation.

The supported product model is:

```text
Start FCP
  -> create or load stable device identity
  -> discover, join, reconnect to, or create a Federation
  -> inspect local supported capabilities
  -> open the normal FCP workbench
  -> optionally run or rerun bounded benchmarks
  -> optionally enable, disable, suspend, or reconcile contributions
  -> publish authenticated health and capacity through existing authority paths
```

## Fixed product decisions

### A device is not one role

One device may provide any supported combination of:

- the Flask workbench;
- runtime/orchestration;
- MTConnect or another supported recorder/data-source capability;
- a language-model service;
- explicitly registered compute handlers;
- storage capacity as a candidate;
- explicitly configured relay or transport assistance;
- future versioned capabilities.

Internal authority roles remain necessary. Storage primary/replica assignment,
job ownership, membership administration, provider suspension, leases, terms,
fencing, and artifact grants remain controlled by their existing authoritative
components. They are not the permanent product identity of a device.

### Federation is the user-facing concept

The existing `session_id` boundary is retained.

For this compatible implementation:

- the UI uses `federation_id`;
- one user-facing Federation maps to one existing internal session boundary;
- protocol messages, persistence, replay, membership checks, provider binding,
  storage authority, jobs, and artifact authorization continue to use the
  internal session ID;
- public onboarding must not ask ordinary users to create or resume a technical
  session;
- renaming or removing the internal session boundary requires a separate
  protocol-major compatibility plan.

### Discovery is not trust

Discovery may be automatic. Membership may not be.

First-time connection must use an authenticated path such as:

- an existing trusted binding;
- an explicitly configured trusted candidate;
- a signed, expiring pairing code containing one-use enrollment and invitation
  material;
- another reviewed adapter to the existing membership authority.

A public device ID or network presence identifies a candidate only. It never
grants:

- membership;
- provider authority;
- storage access or assignment;
- compute execution;
- job ownership;
- artifact access.

### Benchmark evidence is not authority

A benchmark result describes suitability and capacity.

It may support a contribution candidate, but it may not by itself:

- create or change Federation membership;
- activate a contribution;
- enroll a provider;
- assign storage primary or replica authority;
- dispatch compute;
- create job ownership;
- grant a lease, term, fencing token, or artifact permission.

### Optional contributions remain independent

Enabling one contribution must not grant another.

In particular:

- AI grants no compute or storage authority;
- compute exposes only explicitly registered local handlers;
- storage remains candidate-only until the existing storage control plane
  assigns authority;
- recorder activation requires an explicitly selected supported source;
- disable and suspend operations fence future use without deleting unrelated
  device membership or unrelated contributions.

## Current persisted model

Capability-first startup uses a versioned local `fcp.onboarding.v1` document.

It records only the compatible startup state needed to reopen FCP safely:

- stable device ID;
- public Federation ID;
- existing internal session ID;
- connected Federation state;
- current inspection revision;
- generic contribution intents;
- completion state;
- migration source metadata;
- UTC update time.

It must not store:

- credentials;
- invitation or enrollment secrets;
- private endpoints;
- recorder source URLs;
- Ollama URLs;
- handler paths;
- executable payloads;
- storage assignments;
- job authority;
- grants, leases, terms, or fencing tokens.

The persisted device and Federation binding are immutable. Corrupt, oversized,
unsupported, contradictory, or partially written state fails closed.

## Compatibility boundary

The old `server_settings.json` remains a compatibility carrier until CF8.

Legacy deployment modes are mapped deterministically:

| Existing mode | Compatible initial behavior |
| --- | --- |
| `full-server` | workbench, runtime, recorder, configured AI |
| `web-workbench` | workbench, runtime, configured AI |
| `web-ui-only` | workbench/read-only UI, configured AI |
| `recorder-only` | recorder and recorder status |
| `language-model-provider` | language-model contribution |

Migration must preserve existing configured behavior and data.

Migration must never silently enable:

- a new recorder source;
- compute;
- storage;
- an unconfigured language model;
- any other contribution not represented by the old configuration.

The explicit role-first fallback remains available through the bounded
compatibility path until CF8 is accepted.

## Current first-run behavior

### Step 1 — Identity

FCP creates or reopens the stable Ed25519 device identity.

Identity creation grants no Federation membership or contribution authority.

### Step 2 — Federation

The device may:

- reconnect to its saved trusted Federation;
- discover and select an existing candidate;
- join through verified trust;
- redeem a signed, short-lived pairing code;
- create a local Federation when no candidate exists.

Ambiguous candidates require operator selection. A broken saved binding must be
repaired; it must not be silently replaced.

### Step 3 — Inspect

FCP performs one bounded local inspection and persists a device-bound snapshot.

Inspection may include:

- operating-system family and architecture;
- coarse CPU, memory, accelerator, disk, and network observations;
- configured local supported services;
- explicitly registered handlers;
- saved bounded MTConnect discovery evidence;
- recommended benchmark definitions;
- safe warnings and expiry.

Inspection does not:

- scan arbitrary networks unless a separately reviewed adapter explicitly does
  so;
- start recording;
- invoke AI inference;
- execute a compute handler;
- assign storage;
- activate a contribution.

### Finish after inspection

A current inspection permits setup completion.

When no fully reviewed optional contribution choices exist, the completed state
is:

- `workbench`: enabled;
- `runtime`: enabled;
- `recorder`: ask-later;
- `language-model`: ask-later;
- `compute`: ask-later;
- `storage`: ask-later.

The compatibility settings are written so the existing runtime can start
without granting optional authority.

Successful fresh completion opens the Federation landing page.

## Optional follow-up behavior

### Benchmarks

The operator may run, skip, cancel, rerun, and review registered bounded checks
after inspection.

Every benchmark result must be:

- bound to the stable device identity;
- bound to a benchmark definition and implementation version;
- bound to one safe logical target;
- timestamped and expiring;
- invalidated by declared dependency changes;
- bounded in time, payload, response size, resource use, and parallelism;
- restart-safe where reservations or durable results are involved;
- free of credentials and private endpoint disclosure;
- separate from contribution activation.

Historical results remain visible but cannot satisfy current-evidence
requirements after expiry or invalidation.

### Contribution review

Candidates are derived from current inspection and relevant benchmark evidence.

The operator may:

- leave a contribution as ask-later;
- enable it when prerequisites and policy allow;
- disable it;
- suspend it;
- reconcile persisted intent after restart or evidence changes.

A completed contribution review is preserved when setup finishes. Incomplete
or absent optional review does not block the workbench and does not activate
anything.

## Returning-device behavior

A completed returning device should:

- reopen its stable identity;
- revalidate the saved Federation membership;
- reconnect without minting new authority;
- reconcile existing explicit contribution intent;
- suspend use when required evidence has expired or become invalid;
- open the Federation overview on the normal startup path;
- show a guided repair state when identity, membership, or persisted startup
  state cannot be trusted.

## Federation product surface

The current product surface includes:

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

The surface is read-only unless a specific mutation is implemented through an
existing authority seam.

Public projections must not render:

- private endpoints;
- credentials;
- pairing or enrollment material;
- internal session bindings;
- local paths;
- handler paths or payloads;
- provider-local configuration;
- grants, leases, terms, or fencing tokens;
- private job inputs or artifacts.

Missing optional services are empty states, not false system failures.
Configured projection failures produce bounded degraded states.

## Implemented component status

### CF0 — product direction and contract decisions

State: complete, with this document now reflecting the current implementation.

### CF1 — onboarding contracts and compatibility mapping

State: implemented and merged.

Includes versioned discovery, Federation binding, inspection, benchmark,
candidate, and intent models plus deterministic legacy migration preview.

### CF2 — inspection and benchmark framework

State: implemented and merged.

Includes the registry, runner, durable results, cancellation, expiry,
invalidation, safe diagnostics, and concrete adapters.

Current follow-up: PR #186 addresses Docker/native Ollama runtime parity and
cold-model benchmark timing. It remains subject to physical Docker and native
`beast` retesting before merge.

### CF3 — discovery, verified join, pairing, and reconnect

State: implemented and merged.

Pairing uses signed, expiring, bounded material and existing coordinator/session
authority. Public identity alone grants nothing.

### CF4 — contribution recommendation and activation

State: implemented and merged.

Recorder, AI, registered-compute, and storage-candidate paths delegate to
existing authority seams.

### CF5 — onboarding UI

State: implemented and merged.

The original six-stage architecture remains available as functional services,
but the required first-run product journey is now the streamlined
Identity/Federation/Inspect flow. Benchmarks and contributions are optional
follow-up activities.

### CF6 — Federation projections and pages

State: implemented and merged.

All nine Federation projection pages are registered through bounded GET/HEAD
routes with safe empty, connected, and degraded semantics.

### CFI-1 through CFI-6 — supported Flask composition

State: implemented and merged.

The supported Flask application composes identity, Federation, inspection,
benchmarks, contributions, compatibility migration, startup routing, runtime
intent, and navigation.

### CF7-A and CF7-B — automated acceptance foundation

State: implemented and merged.

CI covers component contracts and bounded product-composition scenarios on
Ubuntu and Windows.

Automated acceptance does not prove real hardware, real services, real
multi-host networking, or real browser behavior.

### CF7 physical readiness and product corrections

State: repository preparation and several physical-browser corrections are
implemented and merged through PR #185.

Complete evidence-backed physical acceptance remains open.

### CF8 — retire role-first setup

State: blocked.

CF8 may not begin solely because CI is green or pairing worked once.

## Acceptance truth boundary

Acceptance is intentionally layered.

### Layer 1 — component and contract evidence

Proves isolated contracts, persistence, redaction, authority boundaries,
restart behavior, and platform compatibility.

State: implemented and green on the merged baseline.

### Layer 2 — automated product-composition evidence

Proves supported Flask composition with bounded deterministic fixtures and
independent state roots.

State: implemented and green on the merged baseline.

### Layer 3 — physical smoke evidence

Proves selected real paths, such as physical browser interaction, pairing, or a
configured service, on operator hardware.

State: partial. Pairing and Federation connection have been physically
verified, and physical testing exposed the runtime-parity work in PR #186.

Partial smoke evidence must not be reported as complete CF7 acceptance.

### Layer 4 — complete commit-bound CF7 physical acceptance

Requires every mandatory physical scenario to pass on one exact frozen commit,
with a complete redacted evidence document accepted by the strict validator.

State: not accepted.

### Layer 5 — CF8 retirement approval

Requires a separate review of the complete CF7 evidence and an explicit
decision to retire role-first setup.

State: blocked.

## Required physical acceptance

The frozen candidate must pass all required observations, including:

- fresh physical Windows checkout;
- fresh physical Linux checkout;
- independent multi-host Federation transport;
- real or approved MTConnect source;
- target Ollama model and accelerator;
- real desktop browser;
- real mobile browser;
- recorder plus AI on one physical device;
- separate physical AI, registered-compute, and storage-candidate devices;
- benchmark expiry or dependency invalidation and explicit rerun;
- contribution disable and re-enable;
- restart and reconciliation;
- revocation, route fencing, and controlled verified rejoin;
- storage remaining candidate-only until existing assignment authority acts;
- no private data or unrelated authority leakage.

All environments and scenarios must use the same exact commit.

## Current exact implementation sequence

### 1. Complete benchmark runtime parity

Use draft PR #186 as the focused unit.

Required before merge:

- retest the Ollama benchmark in the supported Docker deployment;
- retest it in the native `beast` runtime;
- confirm the configured endpoint resolves correctly in both contexts;
- confirm the selected model remains visible when AI contribution intent is
  `ask-later`;
- confirm the bounded cold-start window completes or fails with safe actionable
  diagnostics;
- confirm no benchmark result grants AI, compute, or storage authority;
- keep all existing Ubuntu and Windows gates green.

Do not broaden PR #186 into general onboarding, provider, or storage changes.

### 2. Reconcile current documentation

After runtime-parity behavior is settled:

- mark phase-specific branch delivery notes as historical where appropriate;
- point them to this plan for current behavior;
- update `current_task_handoff.md`;
- update `federation_v1_closeout_plan.md`;
- update the post-v1 roadmap and release scope where they still describe
  benchmarks and contribution selection as mandatory before first completion;
- add a durable note for the recovery and fast-onboarding decisions merged in
  PR #185;
- remove obsolete “implement CF1 next” instructions.

This plan update is the first part of that reconciliation.

### 3. Update the physical acceptance campaign

Update issue #180 and the physical handoff so they refer to:

- the merged PR #185 baseline;
- the final merged runtime-parity commit;
- one newly frozen exact acceptance candidate;
- the current three-step onboarding behavior;
- the full unchanged CF7 physical scenario set.

Do not combine evidence from old stacked branch heads with the new candidate.

### 4. Freeze one acceptance candidate

Freeze one exact lowercase 40-character commit after:

- PR #186 is merged or deliberately rejected;
- all documentation required to operate the test is current;
- CF7-A, CF7-B, runtime-parity, Federation, Flask, recorder, provider, storage,
  and broad regression gates are green;
- there are no known unresolved authority, privacy, data-loss, or platform
  defects.

### 5. Execute complete physical CF7 acceptance

Use the current readiness tooling and strict evidence contract.

Every required scenario must genuinely pass. Pending, skipped, simulated, or
CI-only evidence is insufficient.

### 6. Review the acceptance decision

Only a separate evidence-backed review may change the manifest flags to `true`.

The review must verify:

- exact commit binding;
- complete scenario coverage;
- redaction;
- cross-platform evidence;
- real service and real multi-host evidence;
- no unresolved defect;
- no overclaim beyond the tested topology.

### 7. Plan CF8 separately

Only after the acceptance review succeeds, create a new CF8 implementation plan
and branch.

CF8 must not be folded into the acceptance PR.

## CF8 retirement scope

When unblocked, CF8 may:

- stop writing `deployment_mode` for new installations;
- retain a bounded legacy reader for supported upgrades;
- replace remaining role-derived runtime and navigation checks with
  capability-intent checks;
- update command setup and `.env` compatibility;
- migrate or remove obsolete role-specific tests only after equivalent
  capability coverage exists;
- remove the old setup path in a separately reviewed cleanup change;
- update user documentation so no active guide instructs a new user to choose a
  permanent device role.

CF8 must not:

- remove `session_id`;
- alter protocol authority;
- invent a second persistence or membership source;
- silently activate optional contributions;
- remove recovery or compatibility behavior without migration evidence.

## Validation required for every remaining implementation PR

Every focused PR must include, as applicable:

- Python compilation for changed packages;
- Ruff for changed Python files;
- focused deterministic tests;
- malformed-input and unsupported-version rejection;
- private-data redaction checks;
- restart and idempotency checks for durable state;
- Linux and Windows coverage when platform behavior differs;
- Docker Compose validation when setup or packaging changes;
- `git diff --check`;
- no weakening of Federation authority gates;
- a PR description that distinguishes automated evidence from physical
  evidence.

Documentation-only reconciliation PRs must at minimum verify:

- referenced paths exist;
- commands match current entry points;
- links are valid;
- current-state claims match `main`;
- no historical document is presented as the current next task.

## File-ownership policy for future agents

The original Wave 0–4 ownership plan successfully reduced conflicts and is now
historical.

For remaining work:

1. create every branch from current `main`;
2. keep each PR focused on one explicit defect, acceptance unit, or
   documentation reconciliation;
3. do not stack new work on superseded pre-PR-185 branches;
4. declare owned paths in the PR body;
5. do not let two agents edit the same shared Flask/setup file concurrently;
6. do not redesign frozen contracts inside an integration fix;
7. use a separate contract amendment when a shared interface must change;
8. keep acceptance-only work from introducing new production authority;
9. open draft PRs until the relevant automated and physical conditions are met;
10. never merge or mark acceptance complete solely because one automated matrix
    is green.

## Stop conditions

Stop and report rather than broaden scope when:

- discovery or pairing would require unauthenticated trust;
- a public device ID or network presence is being treated as membership;
- a benchmark would run arbitrary downloaded or remotely supplied code;
- benchmark evidence is being used as authority;
- optional fast completion would activate recorder, AI, compute, or storage;
- migration would silently enable a contribution not present in the old setup;
- AI activation would imply compute or storage authority;
- compute would expose an unregistered handler;
- storage would self-assign primary or replica authority;
- disabling a contribution would remove membership or unrelated capability
  state;
- a user-facing simplification would bypass membership, revocation, fencing,
  lease, job, storage, or artifact checks;
- the same physical acceptance campaign would use different commits;
- evidence cannot distinguish a real path from fixtures, loopback, CI,
  containers, or WSL where physical hardware is required;
- private endpoints, credentials, identities, local paths, or database locations
  cannot be safely redacted;
- an unresolved authority, privacy, data-loss, cross-platform, browser,
  MTConnect, Ollama, restart, revocation, or multi-host defect remains;
- CF8 is proposed before the acceptance manifest is changed through a separate
  evidence-backed review.

## Superseded instructions

The following instructions from older revisions of this plan are obsolete:

- “implementation has not started”;
- “proceed with CF1 only”;
- “merge CF1 before creating Wave 1 branches”;
- treating the six-stage onboarding architecture as six mandatory first-run
  screens;
- requiring benchmark or contribution review before the user can open FCP;
- describing PRs #175 through #179 as the current unmerged implementation
  stack.

Those statements describe historical sequencing, not the current repository.

## Current next action

Complete the focused physical Docker/native retest of PR #186.

After that result is known, merge or revise PR #186, reconcile the remaining
current-state documentation, freeze one exact acceptance candidate, and execute
the complete CF7 physical acceptance campaign.

Do not begin CF8.
