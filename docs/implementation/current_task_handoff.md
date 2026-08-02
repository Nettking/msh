# Current task handoff

Last updated: 2026-08-02, Europe/Oslo.

## Repository state

- Repository: `Nettking/msh`
- Default branch: `main`
- Audited `main` SHA: `ba954c91fa5f0cbd075b2210fbb1fcc717df8fa8`
- Audit branch: `agent/capability-first-reality-audit`
- Technical Federation authority baseline: complete through F8.7
- Capability-first isolated foundation: CF1-CF6 components merged
- Capability-first Flask/setup/runtime integration: not started
- Capability-first CF7 acceptance: not started
- Published release tag: not created

## Authoritative current documents

- `docs/implementation/capability_first_reality_audit.md`
- `docs/implementation/capability_first_federation_plan.md`
- `docs/implementation/federation_v1_closeout_plan.md`
- `docs/releases/federation_v1_scope.md`
- `docs/roadmap/post_v1_product_roadmap.md`

## What is actually complete

### Existing Federation authority core

The existing validated technical baseline includes:

- persistent identity, enrollment, membership/session compatibility, ordered events, replay, and revocation;
- storage primary/replica authority, replication, fencing, completeness-aware failover, and recovery;
- direct encrypted transport, relay fallback, rendezvous, and verified resumable transfer;
- durable AI/compute scheduling, dispatch, retry, cancellation, stale-worker fencing, and artifact authorization;
- trusted-provider enrollment, expiring health, remote AI binding, registered compute activation, operator projection, and restart reconciliation.

### Capability-first isolated foundation

CF1:

- versioned onboarding/discovery/inspection/benchmark/candidate/intent contracts;
- deterministic federation/internal-session compatibility mapping;
- read-only legacy migration preview;
- focused Ubuntu and Windows component workflow.

CF2:

- generic inspection and trusted-local benchmark kernel;
- durable SQLite results and run reservations;
- expiry, invalidation, cancellation signaling, concurrency limits, fingerprints, and safe diagnostics;
- focused tests;
- no dedicated merged CF2 cross-platform workflow found;
- concrete AI, MTConnect, compute, storage, and network benchmark adapters remain missing.

CF3:

- bounded configured/relay discovery;
- several-candidate selection;
- verified join through existing enrollment/invitation authority;
- reconnect through existing membership authority;
- safe local federation creation;
- focused Ubuntu and Windows component workflow.

CF4:

- candidate generation, intent persistence, policy, enable/disable/ask-later/suspend/reconcile;
- recorder callback adapter;
- AI runtime-registration adapter without compute/storage authority;
- registered-handler-only compute adapter;
- candidate-only storage adapter;
- focused Ubuntu and Windows component workflow with selected authority regressions.

CF5:

- six-step onboarding and Federation overview templates;
- responsive CSS, JavaScript, fixture view models, and standalone Jinja tests;
- focused Ubuntu and Windows component workflow;
- UI shell only: no Flask routes or authoritative server-side flow.

CF6:

- framework-neutral safe projections for all planned Federation sections;
- read-only adapters over existing state and authority surfaces;
- degraded/repair states and public-data safety checks;
- focused Ubuntu and Windows component workflow;
- not instantiated by the Flask application.

## What is not integrated

At the audited `main` SHA:

- `catalog/flask_app/app.py` does not register a capability-first or Federation overview blueprint;
- the current setup service still reads/writes `ServerSetupSettings.deployment_mode`;
- `DEPLOYMENT_MODES` and `ROLE_CAPABILITIES` still control setup and runtime behavior;
- no supported route renders the CF5 onboarding or Federation overview templates;
- no production composition instantiates the CF2 runner, CF3 discovery service, CF4 contribution service, or CF6 projection service;
- no authoritative onboarding progress or migration write exists;
- no capability-first end-to-end scenario has been accepted.

Do not describe CF1-CF6 as integrated, end-to-end accepted, or manually verified on real Windows/Linux installations.

## Required boundaries

- Benchmark evidence never grants authority.
- Discovery or network presence never grants membership.
- AI never grants compute or storage authority.
- Compute exposes only registered local handlers.
- Storage candidates never gain primary/replica authority without the existing control plane.
- Disable/suspend fences future use without deleting unrelated membership.
- `session_id` remains the internal protocol and isolation boundary.
- `deployment_mode` remains through the compatibility period.
- Role-first setup remains until CF7 passes.
- No protocol or persistence migration occurs without a separate compatibility plan.

## Revised integration approach

Do not implement one large CFI pull request. Split integration into bounded authority/failure domains:

1. read-only Federation overview route and composition;
2. identity, legacy preview, discovery, verified join/local creation, and reconnect;
3. concrete benchmark adapters and lifecycle endpoints;
4. contribution actions bound to actual recorder, AI, registered-compute, and storage authorities;
5. compatibility-controlled startup transition retaining role-first fallback;
6. independent CF7 acceptance;
7. CF8 cleanup only after acceptance.

## Current exact next implementation unit

Implement **CFI-1: read-only Federation overview integration** only.

Expected boundary:

- add a narrow Federation blueprint/route;
- instantiate CF6 projections from existing authorized read-only services;
- render the existing CF5 Federation overview shell;
- register the blueprint in the Flask app;
- prove safe no-context and degraded states;
- run route/template and affected app regressions on Ubuntu and Windows.

Explicit exclusions:

- no onboarding writes;
- no benchmark execution;
- no contribution mutations;
- no setup migration;
- no navigation replacement;
- no role-gate removal;
- no protocol changes;
- no persistence changes.

## Acceptance still required before Federation v1

- real fresh Windows and Linux installation;
- no-state identity creation;
- real independently persisted devices completing discovery/join/reconnect/restart;
- safe local federation creation;
- migration from every supported legacy deployment mode;
- recorder plus AI on one device;
- AI, registered compute, and storage candidates on separate devices;
- concrete benchmark lifecycle;
- contribution fencing and recovery;
- revocation and controlled rejoin;
- storage assignment/failover through existing authority;
- desktop/mobile UI;
- backup/recovery and malformed-state handling;
- permanent composed Linux/Windows release gate;
- documentation command/link checks;
- manual Windows and Linux verification records.

## Resume safety

- Safe to resume: yes.
- Start from updated `main`, not from an earlier CF branch.
- Preserve the exact authority and compatibility boundaries above.
- Treat the reality audit as the source of truth when older PR descriptions or phase wording imply more completion than the repository demonstrates.
