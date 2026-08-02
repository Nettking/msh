# Product roadmap after the Federation authority baseline

Status: active product roadmap aligned with the post-Wave-2 reality audit.

Reality-audit baseline: `ba954c91fa5f0cbd075b2210fbb1fcc717df8fa8` on `main`.

This roadmap separates product experience from internal authority. A simple UI is not an authority source, and an isolated component is not a supported product feature until it is composed, regression tested, and accepted through the supported application.

Current detailed evidence:

- `docs/implementation/capability_first_reality_audit.md`;
- `docs/implementation/capability_first_federation_plan.md`.

## Planning principles

- Treat every installation as one persistent MSH device, not one permanent deployment role.
- Allow one device to contribute several independent capabilities.
- Keep discovery, benchmark evidence, contribution intent, provider health, selection, and authority separate.
- Preserve the validated Federation authority core while simplifying the product surface.
- Keep `session_id` as the internal compatibility boundary until a separately approved protocol migration.
- Retain `deployment_mode` and role-first setup through the compatibility period and until CF7 acceptance.
- Integrate into the existing Flask-first application rather than creating a second product runtime.
- Never expose private endpoints, credentials, handler paths, or storage authority to simplify setup.
- Measure progress by integrated and accepted behavior, not by merged PR count.

## Completed product foundation

### Integrated documentation browser

The read-only documentation browser is implemented in the existing Flask application at `/docs` and was manually accepted on the repository owner's laptop.

This acceptance applies to the documentation browser only. It is not evidence for capability-first onboarding, Federation UI, or multi-device acceptance.

### Capability-first isolated foundation

Merged reusable components include:

- CF1 versioned onboarding contracts and a read-only legacy migration preview;
- a generic CF2 inspection/benchmark kernel with durable results;
- CF3 discovery, verified join, reconnect, and local-creation services over existing authority;
- CF4 recommendation, intent, policy, and recorder/AI/registered-compute/storage adapters;
- a CF5 six-step onboarding and Federation overview UI shell;
- CF6 framework-neutral safe projections.

These are isolated foundations. They are not currently reachable as one supported user journey.

## Active pre-v1 product work — capability-first integration

Purpose: make MSH understandable without requiring a new user to select a permanent technical role, while preserving every existing authority and compatibility boundary.

### Current product gap

The supported application still:

- asks for and persists `deployment_mode`;
- uses `DEPLOYMENT_MODES` and `ROLE_CAPABILITIES`;
- gates runtime, recorder, and AI behavior by the legacy role;
- has no capability-first onboarding route;
- has no supported Federation overview route using CF6 projections;
- does not instantiate CF2, CF3, CF4, or CF6 in the Flask composition root.

### Required integration slices

Capability-first integration should proceed through bounded changes:

1. **Read-only Federation overview** — compose CF6 from authorized read-only services and render the existing CF5 overview shell.
2. **Identity and federation connection** — integrate stable identity, legacy preview, discovery, verified join/local creation, and reconnect while retaining role-first fallback.
3. **Concrete benchmarks** — add the missing v1 AI, MTConnect/data-source, registered-compute, storage-candidate, and authenticated-network benchmark adapters and supported lifecycle endpoints.
4. **Contribution actions** — bind CF4 to actual recorder, AI runtime, registered compute, and storage control-plane boundaries.
5. **Compatibility-controlled startup transition** — prefer capability-first setup while retaining `deployment_mode` and the old setup path until acceptance.
6. **Independent acceptance** — complete CF7 on real Windows and Linux installations before CF8 cleanup.

### Product information architecture

The intended Federation section remains:

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

The CF5 shell and CF6 projections support this structure in isolation. Reachability, authorization, navigation, mutations, persistence, and repair flows remain integration work.

### V1 acceptance for capability-first product behavior

Before release publication, demonstrate:

- fresh Windows and Linux installations;
- stable identity and no-state first run;
- real independently persisted devices completing verified discovery/join/reconnect;
- safe local federation creation when no candidate exists;
- deterministic migration from every supported legacy deployment mode;
- recorder plus AI simultaneously on one device;
- separate AI, registered-compute, and storage-candidate devices;
- benchmark expiry, invalidation, rerun, skip, cancellation, timeout, and failure;
- contribution disable/re-enable/suspend and restart reconciliation;
- storage assignment only through the existing control plane;
- desktop and mobile UI;
- safe degraded and repair states;
- no authority or private-data leakage;
- one permanent composed Linux/Windows Federation regression gate.

## V1 release boundary

Required for Federation v1 publication:

- capability-first integration and CF7 acceptance;
- role-first compatibility retained until that acceptance is complete;
- canonical user and operator documentation matching actual supported behavior;
- backup/recovery and malformed-state rehearsal;
- release notes, changelog, exact release commit, and verified tag;
- no claim that component workflows or PR count equal release readiness.

The following authority boundaries remain mandatory:

- benchmark evidence never grants authority;
- discovery/presence never grants membership;
- AI never grants compute/storage authority;
- compute uses only registered handlers;
- storage candidates never self-promote;
- disabling contributions fences future use without deleting membership;
- protocol and persistence changes require separate compatibility plans.

## Post-v1 — operational administration and observability

After v1 acceptance, improve trusted-federation operation without changing the trust model:

- persistent safe health dashboard;
- clearer dependency and readiness state;
- structured operational logs and bounded metrics;
- contribution, storage, and job history;
- backup status and recovery rehearsal guidance;
- upgrade readiness and compatibility checks;
- safe diagnostics bundles;
- soak, restart, and controlled failure testing;
- clearer relay/direct-route visibility without exposing private addresses.

## Post-v1 — improved AI experience

Potential compatible improvements:

- richer model suitability benchmarks;
- model/provider warm-up and readiness;
- safe provider-choice explanations;
- streaming where existing protocols support it;
- cancellation UX;
- bounded request history with explicit privacy controls;
- clearer fallback and failure explanations.

AI must remain unable to create storage authority, compute authority, artifact authority, or arbitrary command execution.

## V2 candidate — broader network operation

Potential scope requiring a new compatibility plan:

- operational public relay and rendezvous deployment;
- restrictive-NAT and unrelated-network certification;
- automatic authenticated route publication and renewal;
- certificate/key rotation workflows;
- multi-route policy;
- stronger production abuse controls;
- deployed-version negotiation;
- possible protocol-level session terminology migration.

## V2 candidate — advanced scheduling

Potential scope:

- cost, latency, locality, and energy-aware policy;
- GPU/accelerator requirements;
- quotas, fairness, priority, affinity, and preemption;
- durable distributed interactive queues;
- model warm pools and broader lifecycle orchestration;
- production load and SLO acceptance.

## V2 candidate — organizations and policy

Potential scope:

- delegated administration;
- organization/project boundaries;
- policy-based sharing;
- durable compliance views;
- retention and data governance;
- federation-to-federation trust.

## Future research boundary — less-trusted external providers

Deliberately outside v1 and normal 1.x integration:

- sandboxing and isolation for unknown workloads;
- signed packages and supply-chain provenance;
- externally supplied executable code;
- reputation, dispute, billing, and marketplace behavior;
- anonymous or public participation.

No current UI should imply that unknown third-party execution is safe.

## Promotion rule

A roadmap item becomes supported current work only when:

1. the repository owner selects it;
2. a bounded plan defines compatibility and authority impact;
3. file ownership avoids overlapping active changes;
4. affected existing regressions remain mandatory;
5. integration and acceptance criteria are written before implementation;
6. public documentation does not claim behavior before it is demonstrated.

## Next product milestone

The next milestone is **a reachable, read-only Federation overview composed from existing authorized services**, not completion of another numbered CF phase.
