# Federation v1 closeout plan

Status: amended stabilization plan. The technical Federation v1 baseline remains valid, but release publication is paused while the explicitly approved capability-first onboarding work is implemented and accepted.

## Owner-approved amendment — 2026-08-02

The original feature freeze has been explicitly changed by the repository owner.

Capability-first onboarding and the Federation product UI are now approved compatible `1.x` product work. The authoritative plan is:

- `docs/implementation/capability_first_federation_plan.md`

The amendment changes the product path, not the validated authority core:

- users will no longer choose one mutually exclusive permanent deployment role;
- devices may contribute several capabilities simultaneously;
- setup will discover or create a federation, inspect the device, run suitable benchmarks, and ask which contributions to enable;
- `session_id` remains an internal compatibility and isolation boundary during this migration;
- membership, provider health, storage authority, job ownership, leases, fencing, and artifact grants remain authoritative;
- benchmark results provide evidence only and never grant authority by themselves;
- release-candidate acceptance must use the new onboarding flow rather than the old role-first wizard.

Cleanup work may continue only when it does not overlap active onboarding implementation. Shared setup, Flask navigation, and federation product files are reserved for the capability-first plan until its integration wave is complete.

## Objective

Convert the completed federation implementation into a stable, documented, supportable MSH Federation release with a capability-first user experience.

The closeout remains a sequence of small, independently reviewable changes. Cleanup must never remove a path merely because it looks old. Every deletion requires dependency evidence and regression validation.

## Fixed decisions

- The completed federation technical baseline remains **MSH Federation v1.0** through F8.7.
- V1 is for explicitly trusted devices and providers.
- Runtime authority and security boundaries from the completed phases remain unchanged.
- Capability-first onboarding is an approved compatible product-layer change before release publication.
- The internal session boundary remains during the compatible migration.
- UI and documentation improvements must not invent a second authority source.
- No implementation branch is deleted without separate explicit owner approval.
- No release tag is created before exact release acceptance.

## Work packages

### V1-A — audit and release boundary

Completed deliverables:

- repository audit;
- v1 scope definition;
- closeout plan;
- post-v1 product roadmap;
- current handoff replacement;
- first deletion-candidate classification.

### V1-B — exact path-level cleanup manifest

Completed deliverable: one path-level manifest covering proposed deletion, relocation, consolidation, archive, and defer decisions.

The manifest remains useful for cleanup, but any setup/UI decision that conflicts with the newer capability-first plan is superseded by that plan.

### V1-C — generated output and experiment cleanup

Progress:

- generated `graphify-out/` content was removed and ignored;
- the integrated `/docs` reader was implemented and accepted;
- the standalone Markdown viewer is now a separately approved deletion candidate;
- other `new-stuff/` experiments remain separate owner decisions.

Remaining cleanup must not edit the shared onboarding integration files while capability-first implementation is active.

### CF0-CF8 — capability-first onboarding and Federation UI

The complete decomposition, compatibility rules, parallel-agent ownership, acceptance, and stop conditions are defined in:

- `docs/implementation/capability_first_federation_plan.md`

This work is inserted before release-candidate acceptance.

### V1-D — obsolete implementation cleanup

Expected scope after dependency proof:

- remove duplicate old web implementation if `catalog/webapp/` has no supported entry point;
- reduce `legacy/` to explicitly justified historical/reference material;
- remove obsolete scripts and stale documentation;
- update AI grounding/indexing when indexed paths change;
- remove dead tests only when equivalent product behavior remains covered.

Exit criteria:

- one supported Flask application path;
- no hidden duplicate behavior relied upon by setup or Compose;
- full affected regression suites green.

### V1-E — documentation consolidation

Deliver a canonical structure such as:

```text
docs/
  index.md
  getting-started/
  user-guides/
  federation-v1/
  administration/
  troubleshooting/
  developer/
  reference/
  history/
```

Required public documents now include:

- product overview;
- quick start;
- installation and capability-first onboarding;
- federation discovery, verification, join, reconnect, and local creation;
- device inspection and benchmarks;
- contribution management;
- device and provider administration;
- storage, AI, compute, and recorder guides;
- security/trust model;
- backup, restart, recovery, and upgrade;
- troubleshooting;
- compatibility/protocol reference;
- developer architecture and test guide.

Required cleanup:

- replace the oversized root README with a concise entry point;
- move durable design evidence to history/decisions;
- delete superseded phase plans and handoffs after link verification;
- mark retained history as non-current;
- validate all links and commands.

Exit criteria:

- a new user can identify the correct first document;
- current behavior is described without phase archaeology;
- setup documentation does not instruct a new user to select a permanent device role;
- technical session terminology is kept out of ordinary onboarding guidance;
- `/docs` exposes the canonical structure correctly.

### V1-F — permanent regression gate

Deliver:

- one named Federation release workflow;
- Linux and Windows coverage;
- complete identity, membership/session compatibility, relay, transport, storage, failover, capability, benchmark, contribution, AI, compute, recorder, artifact, Flask, onboarding, migration, and compatibility matrix;
- compile, Ruff, Compose, diff hygiene, and documentation-link checks;
- retained focused component workflows only where they add fault isolation.

Exit criteria:

- the release gate is equivalent to or stronger than the union of required closeout gates;
- completed phase names are no longer the only permanent quality signal;
- no workflow is deleted before replacement evidence exists.

### V1-G — release candidate acceptance

Required acceptance:

- fresh checkout installation on Linux and Windows;
- first-run capability-first onboarding without old state;
- stable device identity creation;
- existing federation discovery and verified join;
- safe local federation creation when no candidate exists;
- returning-device reconnect and restart;
- migration from every supported old deployment mode;
- one device contributing multiple capabilities simultaneously;
- benchmark execution, expiry, invalidation, rerun, skip, and failure behavior;
- storage replication and controlled failover;
- AI contribution enable/use/disable/recovery;
- compute contribution enable/dispatch/duplicate suppression/disable/recovery;
- recorder plus AI on the same device;
- safe diagnostics without secret/private endpoint leakage;
- backup/recovery rehearsal;
- all user commands and docs links checked.

Record exact hardware/network constraints and do not overclaim unsupported public-internet acceptance.

### V1-H — release publication

Deliver:

- `CHANGELOG.md`;
- release notes;
- exact version declaration;
- verified release commit;
- release tag;
- updated original federation issue/status;
- post-release branch-cleanup proposal presented separately.

Exit criteria:

- the tag points to the validated commit;
- release notes match actual scope and limitations;
- no pending mandatory onboarding, migration, or closeout action is hidden in a historical plan.

## Parallel-work policy

The capability-first plan is designed for parallel agents, but parallelism must not create overlapping edits.

- CF1 contracts merge first.
- Benchmarking, discovery, and new UI-shell work may then run concurrently on separate branches.
- Contribution services and safe projections may run concurrently after their prerequisites merge.
- one integration agent exclusively owns shared Flask/setup files;
- one independent acceptance agent owns the final regression and migration proof;
- every agent declares exact path ownership in its PR.

## Stop conditions

Stop and report rather than broaden scope when:

- discovery would require unauthenticated trust;
- a benchmark would run arbitrary remotely supplied code;
- a benchmark result is treated as authority;
- migration would silently enable a contribution not previously configured;
- a user-facing simplification would weaken membership, revocation, lease, fencing, storage, job, or artifact checks;
- two active agents need to edit the same shared integration file;
- removing `session_id` becomes necessary for a compatible unit;
- public documentation would require claiming behavior not demonstrated by acceptance;
- cleanup requires a new unrelated feature;
- a test indicates lost compatibility, split brain, stale execution, or data risk.

## Next exact action

Proceed with **CF1 only**:

1. add pure capability-first onboarding contracts;
2. add `federation_id` to internal-session compatibility mapping;
3. add a read-only migration preview from every existing deployment mode;
4. add exhaustive tests;
5. do not change the current setup UI in CF1;
6. merge CF1 before creating the parallel benchmark, discovery, and UI-shell branches.
