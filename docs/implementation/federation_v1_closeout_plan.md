# Federation v1 closeout plan

Status: active stabilization plan after the capability-first Wave 2 reality audit.

Reality-audit baseline: `ba954c91fa5f0cbd075b2210fbb1fcc717df8fa8` on `main`.

The technical Federation authority baseline through F8.7 remains valid. Release publication remains blocked because capability-first onboarding and Federation UI are not integrated or accepted end to end.

Authoritative current documents:

- `docs/implementation/capability_first_federation_plan.md`;
- `docs/implementation/capability_first_reality_audit.md`;
- `docs/releases/federation_v1_scope.md`.

## Current reality

Merged CF work provides reusable isolated components:

- CF1 contracts and legacy migration preview;
- a generic CF2 benchmark/inspection kernel;
- CF3 discovery, verified join, reconnect, and local-creation services;
- CF4 recommendation, intent, policy, and contribution adapters;
- a CF5 UI shell;
- CF6 safe framework-neutral projections.

The supported application still uses role-first setup and `deployment_mode`. No capability-first route, composition root, setup migration, runtime binding, or supported Federation overview is present on `main`.

Therefore:

- CF1-CF6 component completion is not product completion;
- focused Linux/Windows workflow matrices are not end-to-end acceptance;
- earlier F8 authority-core acceptance is not CF7 onboarding acceptance;
- no capability-first flow is recorded as manually verified on a real Windows or Linux installation.

## Fixed release decisions

- Federation v1 is for explicitly trusted devices and providers.
- Benchmark evidence never grants authority.
- Discovery or network presence never grants membership.
- AI never grants compute or storage authority.
- Compute exposes only registered local handlers.
- Storage candidates remain candidate-only until the existing control plane assigns primary or replica authority.
- Disabling or suspending a contribution fences future use without deleting unrelated membership.
- `session_id` remains the internal compatibility and isolation boundary.
- `deployment_mode` remains through the compatibility period.
- Role-first setup remains until CF7 acceptance passes.
- No protocol or persistence migration occurs without a separate compatibility plan.
- No release tag is created before exact release acceptance.

## Closeout work packages

### V1-A — repository and release-boundary audit

Status: completed historically and updated by the capability-first reality audit.

The new audit supersedes any earlier implication that merged CF pull requests mean integrated product readiness.

### V1-B — cleanup manifest

Status: completed as a decision manifest.

Cleanup must not overlap active integration files or remove compatibility paths required by the capability-first transition.

### V1-C — generated output and experiment cleanup

Progress already recorded:

- generated Graphify output was removed and ignored;
- the integrated `/docs` reader was implemented and manually accepted;
- remaining experiment and duplicate-implementation cleanup requires separate approval and dependency proof.

This cleanup is not a substitute for capability-first integration.

### CFI — capability-first integration

Status: not started.

The original single integration wave is replaced by bounded integration units:

1. read-only Federation overview composition and route;
2. identity, legacy preview, discovery, join/create, and reconnect;
3. concrete v1 benchmark adapters and authoritative run lifecycle;
4. contribution actions bound to existing recorder, AI, registered-compute, and storage authorities;
5. compatibility-controlled startup/setup transition while preserving `deployment_mode` and role-first fallback.

Each integration unit must be independently reviewable and cross-platform tested. Contract redesign, protocol migration, and persistence migration remain separate decisions.

### CF7 / V1-G — independent end-to-end acceptance

Status: not started for capability-first onboarding.

Required evidence includes:

- fresh checkout installation on real Windows and real Linux;
- first run with no existing state;
- stable identity creation;
- real independently persisted devices completing discovery, verified join, reconnect, and restart;
- safe local federation creation when no candidate exists;
- several-candidate selection;
- actual migration from every supported legacy deployment mode;
- one device contributing recorder plus AI;
- separate AI, registered-compute, and storage-candidate devices;
- concrete benchmark expiry, invalidation, rerun, skip, cancellation, timeout, and failure;
- contribution disable, re-enable, suspend, and restart reconciliation;
- revocation and controlled rejoin;
- storage assignment and failover through existing authority only;
- desktop and mobile browser flows;
- backup/recovery and corrupted-state rehearsal;
- complete permanent Linux/Windows Federation v1 regression gate;
- documentation commands and links checked.

Record exact hardware and network constraints. Do not overclaim public-internet or unknown-provider support.

### CF8 — role-first retirement

Status: blocked on CF7.

Only after CF7 passes may a separate cleanup change stop writing `deployment_mode`, remove role-specific UI gates, or delete the old setup path. A bounded legacy reader remains for supported upgrades.

### V1-D — obsolete implementation cleanup

Status: deferred around active integration boundaries.

Delete or consolidate only after dependency proof and equivalent regression coverage. Do not remove old setup behavior while it remains the supported fallback.

### V1-E — documentation consolidation

Status: incomplete.

Canonical documentation must describe actual supported behavior, clearly distinguish isolated components from integrated features, and avoid instructing users to use capability-first onboarding before it is reachable and accepted.

Required release documentation includes:

- product overview and quick start;
- installation and setup compatibility;
- federation discovery, verification, join, reconnect, and local creation;
- device inspection and benchmarks;
- contribution management;
- storage, AI, compute, and recorder guides;
- security/trust model;
- backup, restart, recovery, and upgrade;
- troubleshooting;
- compatibility/protocol reference;
- developer architecture and test guide.

### V1-F — permanent regression gate

Status: incomplete.

Component workflows exist for CF1, CF3, CF4, CF5, and CF6. CF2 has focused tests but no dedicated merged cross-platform workflow. No permanent release workflow currently proves the composed capability-first application.

The permanent gate must cover Linux and Windows and include identity, membership/session compatibility, discovery, benchmarks, contribution intent and fencing, AI, compute, recorder, storage, artifacts, Flask, migration, restart, documentation links, Compose validation, compile, Ruff, and diff hygiene.

### V1-H — release publication

Status: blocked.

Required outputs:

- completed cleanup and canonical documentation;
- `CHANGELOG.md` and release notes matching actual limitations;
- exact validated release commit;
- verified release tag;
- no hidden mandatory integration or acceptance work.

## Progress measure

Do not report Federation v1 progress by PR count.

Use these evidence milestones:

1. isolated contracts/components;
2. supported Flask/setup/runtime integration;
3. authority, persistence, fencing, and restart proof;
4. composed Linux/Windows automated regression;
5. real Windows/Linux manual installation verification;
6. full CF7 acceptance;
7. release documentation and publication.

The audited repository is substantially through milestone 1 and has not entered milestone 2.

## Stop conditions

Stop and report rather than broaden scope when:

- discovery would grant trust from presence;
- benchmark evidence would become authority;
- a benchmark would execute arbitrary remotely supplied code;
- AI would gain compute or storage authority;
- compute would expose an unregistered handler;
- storage would self-assign authority;
- migration would silently enable a new contribution;
- a UI action would bypass membership, revocation, provider health, job ownership, lease, fencing, storage, or artifact checks;
- role-first compatibility would be removed before CF7;
- protocol or persistence migration would occur without a compatibility plan;
- an automated fixture would be presented as physical multi-device acceptance.

## Next exact action

Proceed with **CFI-1: read-only Federation overview integration** only.

Compose CF6 through authorized read-only services, expose one narrow `/federation` route, render the existing CF5 overview shell, and register the blueprint. Do not add mutation endpoints, onboarding writes, benchmark execution, contribution activation, setup migration, navigation replacement, role-gate removal, protocol changes, or persistence changes.
