# Capability-first repository reality audit

Status: post-Wave-2 repository reality audit.

Audit baseline: `ba954c91fa5f0cbd075b2210fbb1fcc717df8fa8` on `main`.

Audit date: 2026-08-02, Europe/Oslo.

This document records what the capability-first work actually provides after the merged CF1-CF6 branches. It is based on repository code, tests, workflow definitions and recorded workflow runs, Flask composition, setup behavior, and persisted-state boundaries at the baseline commit. Pull-request descriptions were used as navigation aids and were checked against code and available GitHub Actions evidence.

## Classification vocabulary

The audit uses the following terms literally:

- **contract complete** — versioned models, validation, compatibility rules, or a sufficiently fixed interface exist for the audited scope;
- **isolated implementation complete** — the bounded component exists and its own intended behavior is implemented, but it may not be composed into the supported application;
- **integrated** — the component is instantiated and used by the supported Flask/setup/runtime path;
- **regression tested** — deterministic automated tests cover the bounded implementation and affected existing behavior; this does not imply end-to-end acceptance;
- **end-to-end accepted** — the supported user journey has been exercised through real composition, persistence, restart, authority, and UI boundaries;
- **manually verified on a real Windows installation** — a human has exercised the supported flow on a real Windows installation and recorded the result;
- **manually verified on a real Linux installation** — a human has exercised the supported flow on a real Linux installation and recorded the result.

A workflow matrix using Windows runners is not manual Windows verification. A workflow matrix using Ubuntu runners is not manual Linux installation verification.

## Executive finding

Wave 2 did not complete capability-first onboarding or the Federation product UI. It completed a set of intentionally isolated components:

- CF1 contracts and a read-only legacy migration preview;
- a generic CF2 inspection and benchmark kernel;
- a CF3 discovery/join/reconnect service over existing federation authority;
- a CF4 contribution recommendation, intent, policy, and adapter layer;
- a CF5 template/static UI shell rendered from fixtures;
- CF6 framework-neutral safe projections.

At the audited baseline, none of these components is composed into `catalog/flask_app/app.py`, the supported setup routes, the startup gate, `setup_msh.py`, navigation, or the supported runtime composition. The supported application still reads and writes `ServerSetupSettings.deployment_mode`, uses `DEPLOYMENT_MODES` and `ROLE_CAPABILITIES`, and routes startup behavior by the selected legacy role.

The correct overall classification is therefore:

- contracts and isolated implementation: materially advanced;
- supported product integration: not started;
- capability-first end-to-end acceptance: not started;
- physical Windows/Linux capability-first verification: not recorded.

## Phase and deliverable classification

| Phase or deliverable | Repository reality | Classification |
| --- | --- | --- |
| CF0 — product direction, terminology, compatibility, ownership plan | The direction and core boundaries are documented. The original plan status and next-action text became stale after CF1-CF6 merged. | **contract complete** |
| CF1 — onboarding models | Versioned discovery, federation binding, inspection, benchmark, candidate, and intent models exist with canonical serialization, validation, bounded public data, and protocol-major rejection. | **contract complete**; **isolated implementation complete**; **regression tested** |
| CF1 — federation/session compatibility | A deterministic public `federation_id` mapping over the existing internal session boundary exists. No protocol or persistence field is renamed. | **contract complete**; **isolated implementation complete**; **regression tested** |
| CF1 — legacy migration | A read-only migration preview exists for supported legacy modes. It preserves existing configured behavior in preview form and keeps compute and storage disabled. It does not write a new onboarding document or migrate a running installation. | **contract complete**; **isolated implementation complete**; **regression tested** |
| CF2 — generic benchmark registry, runner, validity, safety, and store | The repository contains a generic trusted-local-probe framework with timeout/cancellation signaling, concurrency bounds, fingerprints, safe diagnostics, immutable SQLite results, and restart-safe run-ID reservations. | **contract complete** for the kernel; **isolated implementation complete** for the kernel; **regression tested** |
| CF2 — device inspection framework | A generic inspection service and probe seam exist. | **contract complete** for the seam; **isolated implementation complete**; **regression tested** |
| CF2 — concrete benchmark families | The planned migrated AI benchmark, MTConnect candidate adapter, and concrete compute, storage, and authenticated-network benchmark implementations are not present in the CF2 package. The current setup AI probe remains separate in the legacy setup service. | not complete |
| CF2 — platform gate | PR #164 added no dedicated CF2 workflow. Its head nevertheless ran successfully on Ubuntu and Windows through the existing Phase F8.5 operator-surface matrix, which executes `catalog/capabilities/tests` and therefore included the focused CF2 tests. | **regression tested** across CI platforms; no named CF2 gate; not a composed capability-first gate |
| CF3 — bounded discovery and candidate selection | Configured and relay-resolver discovery adapters exist; safe public results exclude private join material. Several candidates require selection, and discovery failure blocks unsafe local creation. | **contract complete**; **isolated implementation complete**; **regression tested** |
| CF3 — verified join, reconnect, and local creation | The service composes existing `SessionCoordinator` enrollment, invitation, membership, and session authority. Reconnect revalidates existing membership and does not mint trust. | **isolated implementation complete**; **regression tested** |
| CF3 — product discovery | No supported Flask/setup flow instantiates the discovery service. The implemented sources are configured/relay adapters, not proof of general local-network discovery on real installations. | not **integrated**; not **end-to-end accepted** |
| CF4 — candidate generation, intent persistence, policy, and lifecycle | Candidate generation, SQLite intent persistence, policy evaluation, enable/disable/ask-later/suspend/reconcile behavior, and evidence-expiry suspension exist. | **contract complete**; **isolated implementation complete**; **regression tested** |
| CF4 — recorder adapter | A callback adapter can enable, fence, suspend, and reconcile recorder contribution state. The supported Flask recorder service is not yet bound through capability-first composition. | **isolated implementation complete**; **regression tested**; not **integrated** |
| CF4 — AI adapter | The adapter registers or unregisters only an AI provider through an injected runtime-manager boundary. It contains no compute or storage authority operation. | **isolated implementation complete**; **regression tested**; not **integrated** |
| CF4 — compute adapter | Candidate generation is restricted to handlers already present in the trusted local inventory and binds activation to the descriptor fingerprint. | **isolated implementation complete**; **regression tested**; not **integrated** |
| CF4 — storage adapter | Storage is explicitly candidate-only. Enable/reconcile can only observe existing control-plane assignment; the adapter cannot assign primary or replica authority. | **isolated implementation complete**; **regression tested**; not **integrated** |
| CF5 — six-step onboarding presentation | New templates, partials, CSS, JavaScript, and fixture view models render the intended six-step flow. Tests use a standalone Jinja environment and a synthetic base template. | **isolated implementation complete** as a UI shell; **regression tested** |
| CF5 — functional onboarding | No Flask route renders the shell, no server-side flow consumes CF1-CF4, no mutation endpoint behind the forms exists, and no authoritative progress persistence is connected. JavaScript stores presentation progress only. | not **integrated**; not **end-to-end accepted** |
| CF6 — Federation projections | Framework-neutral adapters and view-model builders exist for Overview, This device, Devices, Services, Benchmarks, Storage, Jobs, Activity, and Settings, with a public-data safety boundary and degraded states. | **contract complete**; **isolated implementation complete**; **regression tested** |
| CF6 — Federation product UI | The projection service is not instantiated by the Flask application; no supported Federation route renders the existing shell; Federation navigation is not registered. | not **integrated**; not **end-to-end accepted** |
| CFI — shared Flask/setup/runtime integration | No capability-first composition root, route wiring, setup migration, startup transition, runtime binding, or authoritative form handling exists on `main`. | not started |
| CF7 — migration and end-to-end acceptance | None of the capability-first acceptance scenarios has been recorded through the integrated supported application. Earlier F8 technical-federation acceptance does not substitute for CF7. | not started |
| CF8 — retire role-first setup | `deployment_mode` remains the active setup and runtime compatibility field and must remain until CF7 succeeds. | not started; correctly deferred |

No CF1-CF6 deliverable is classified as **manually verified on a real Windows installation** or **manually verified on a real Linux installation**. The repository records manual acceptance of the integrated `/docs` reader on the owner's laptop, but that is not capability-first verification.

## Automated gates that actually exist

The audit verified successful workflow conclusions on the merged PR heads:

- CF1: the dedicated CF1 Ubuntu/Windows workflow and the existing Phase 2 federation workflow succeeded;
- CF2: the existing Phase F8.5 Ubuntu/Windows workflow succeeded and included the focused CF2 tests through `catalog/capabilities/tests`; no dedicated CF2 workflow exists;
- CF3: the dedicated CF3 Ubuntu/Windows workflow and the existing Phase 2 federation workflow succeeded;
- CF4: the dedicated CF4 Ubuntu/Windows workflow succeeded, together with the existing affected capability/provider workflows triggered by the changed package;
- CF5: the dedicated CF5 Ubuntu/Windows workflow and the existing Phase F8.5 workflow succeeded;
- CF6: the dedicated CF6 Ubuntu/Windows workflow and the existing Phase 2 federation workflow succeeded.

The dedicated component workflow definitions cover:

- CF1 contracts: compile, focused tests, Ruff, and diff hygiene;
- CF3 discovery: compile, focused discovery tests, an affected Phase 2 authority subset, Ruff, and diff hygiene;
- CF4 contribution service: compile, focused tests, selected authority/reconciliation/AI/recorder regressions, Ruff, and diff hygiene;
- CF5 UI shell: standalone template tests, JavaScript syntax, fixture parsing, Ruff, and diff hygiene;
- CF6 projections: compile, focused tests, Ruff, and diff hygiene.

No permanent capability-first release workflow composes CF1-CF6 through Flask/setup/runtime on both operating systems.

These are valuable component gates. They are not evidence for:

- clean installation;
- actual browser routing;
- server-side form execution;
- real persistence locations and upgrades;
- process restart and automatic reconciliation through the supported launcher;
- physical multi-device discovery and join;
- live AI, compute, storage, or recorder use;
- end-to-end revocation or fencing;
- mobile browser acceptance;
- public release readiness.

## Product composition reality

The supported application still:

- registers the existing docs, setup, main web, source, operator, AI, and provider-federation blueprints only;
- injects `DEPLOYMENT_MODES` into templates;
- checks `deployment_mode` in the application context and startup gate;
- uses role capabilities to decide AI, recorder, and runtime behavior;
- writes `msh.server_setup.v3` settings through the existing setup service;
- contains no route reference to the new onboarding or Federation overview templates;
- contains no production instantiation of the CF2 runner, CF3 onboarding discovery service, CF4 contribution service, or CF6 projection service.

Accordingly, CF5 is currently a UI shell, and CF6 is currently a framework-neutral projection layer. Neither is a reachable product surface.

## Authority and security boundaries that are preserved in code

The isolated code preserves the required design boundaries:

- benchmark results are evidence objects and have no authority operation;
- discovery returns safe candidates while enrollment and invitation material remain private;
- network/configuration presence is not membership;
- AI activation is limited to AI runtime registration and exposes no storage or compute operation;
- compute candidates are derived only from registered local handlers and checked against descriptor fingerprints;
- storage contribution remains candidate-only until existing control-plane assignment is observed;
- disable and suspend paths call fencing callbacks rather than deleting device membership;
- the internal session boundary remains unchanged;
- no protocol or existing persistence migration was introduced by CF1-CF6.

## Authority and security proof still required

The following properties are designed but not yet proven through real composition:

1. Flask route and form authorization must bind every action to the correct authenticated device, federation, actor, revision, and existing authority service.
2. CSRF, replay, duplicate submission, stale-page, and concurrent action behavior must be tested for all future POST endpoints.
3. Discovery must be exercised across independently persisted devices, including expired material, wrong verification codes, identity mismatch, revoked membership, and several candidates.
4. Benchmark timeout currently bounds the caller's wait around a trusted local probe. A non-cooperative thread may continue after the result is timed out, so hard process/resource isolation is not established and must not be claimed.
5. Concrete AI, recorder, compute, storage, and network probes must prove their own bounded resource behavior and redaction.
6. CF4 callback bindings must be shown to call the actual existing authoritative services, not parallel state or UI-only state.
7. Disable and suspend must prove that future dispatch/use is fenced, including restart and stale in-flight state, without deleting unrelated membership.
8. Storage assignment must remain exclusively in the existing control plane under all UI and reconciliation paths.
9. Projection adapters must be instantiated with authorized actor/session context; safe rendering alone does not prove authorization.
10. Onboarding, benchmark, and contribution state paths need an explicit compatibility, backup, corruption, locking, and upgrade decision before they become supported persistent state.

## End-to-end scenarios not yet run for capability-first

No repository evidence at the audited baseline establishes completion of these CF7 scenarios:

- fresh supported checkout and first run on Windows;
- fresh supported checkout and first run on Linux;
- no prior state and stable identity creation;
- discovery and verified join between real independently persisted devices;
- safe local federation creation when no candidate exists;
- several-candidate selection;
- returning-device automatic reconnect;
- migration from every supported legacy deployment mode through actual writes;
- recorder plus AI simultaneously on one device;
- separate AI, registered-compute, and storage-candidate devices;
- concrete benchmark execution, expiry, invalidation, rerun, skip, cancellation, and failure;
- contribution disable, re-enable, suspend, and restart reconciliation;
- membership revocation and controlled rejoin;
- storage candidate-to-control-plane assignment without self-promotion;
- desktop and mobile browser flows;
- backup/recovery and corrupted-state handling;
- the complete permanent Federation v1 regression gate.

## Is CF0-CF8 still useful?

The labels remain useful for traceability, but they must not be treated as a linear completion counter.

Recommended interpretation:

- CF0 and CF1 remain coherent contract phases;
- CF2 should distinguish the completed generic kernel from the still-missing concrete benchmark adapters;
- CF3 remains a coherent isolated authority-adapter phase;
- CF4 remains coherent but is incomplete as a product until actual authority bindings are composed;
- CF5 should be named **UI shell** until it has real routes and server-side state;
- CF6 should be named **framework-neutral projections** until those projections reach the supported UI;
- CFI must be split into bounded integration changes;
- CF7 remains the independent acceptance phase;
- CF8 remains correctly gated on CF7.

Progress should be reported by demonstrated capability and acceptance evidence, not by merged PR count or phase number.

## Required CFI decomposition

A single CFI pull request would combine too many failure domains: app factory registration, route authorization, projection composition, onboarding state, benchmark execution, contribution mutations, legacy migration, startup gating, runtime activation, setup command compatibility, navigation, and persisted-state transition.

The integration should be divided into independently reviewable boundaries:

1. read-only Federation composition and route integration;
2. identity, legacy-preview, discovery, join/create, and reconnect integration without contribution mutation;
3. concrete benchmark adapters and authoritative benchmark run/skip/rerun integration;
4. contribution action binding to existing recorder, AI, registered-compute, and storage control-plane boundaries;
5. compatibility-controlled startup/setup transition while retaining `deployment_mode`;
6. independent CF7 acceptance and only then CF8 cleanup.

Each integration change must preserve the old role-first fallback until CF7 passes and must avoid protocol or persistence migration unless a separate compatibility plan has been approved.

## Necessary before Federation v1 publication

Federation v1 still requires:

- concrete, supported benchmark adapters for the v1 capability claims;
- bounded integration of CF1-CF6 into the Flask-first application;
- explicit persisted-state compatibility for onboarding, benchmarks, and contribution intents;
- actual authority-service bindings and fencing proof;
- a permanent Linux/Windows release gate covering the composed application;
- clean-install, migration, restart, multi-device, storage, AI, compute, recorder, and recovery acceptance;
- real Windows and Linux manual verification with recorded constraints;
- documentation consolidation, command/link validation, release notes, changelog, and an exact verified release commit.

## Appropriate post-v1 deferrals

The following should remain post-v1 unless needed to correct a v1 safety defect:

- public or anonymous providers;
- arbitrary remotely supplied code or packages;
- production sandboxing for unknown workloads;
- internet-wide relay/NAT certification;
- cost, energy, fairness, quota, preemption, and advanced placement policy;
- marketplace, billing, reputation, or organizational policy;
- broad streaming/model-lifecycle orchestration;
- renaming or removing the internal `session_id` boundary;
- retiring `deployment_mode` before CF7 acceptance.

## Realistic progress statement

At this baseline, capability-first work has completed most of its reusable contract and isolated service foundation, but zero of the supported capability-first user journey has been accepted end to end. The next progress milestone is not “another CF phase merged”; it is the first safe composition of an existing isolated layer into the supported Flask application with cross-platform route-level regressions and no authority mutation.

## One recommended next implementation PR

Implement **CFI-1: read-only Federation overview integration**.

The PR should instantiate CF6 projections from existing authorized read-only services, add a narrowly scoped `/federation` blueprint/route, render the existing CF5 Federation overview shell, and register that blueprint in the Flask application. It should not add contribution mutations, onboarding writes, benchmark execution, navigation replacement, setup migration, or role-gate removal.

Required proof for that PR:

- authorized actor/session composition;
- useful no-context and degraded states;
- no private endpoint, credential, session-binding, or fencing-detail leakage;
- existing app/startup/setup routes unchanged in behavior;
- route and template tests on Linux and Windows;
- `deployment_mode` retained;
- no protocol or persistence migration.

This is the smallest integration boundary that converts one Wave 2 artifact into a reachable supported product surface without prematurely coupling all onboarding and runtime mutations.
