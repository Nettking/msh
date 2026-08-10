# Federation v1 closeout plan

Status: **active release-closeout plan**.

Reviewed: **2026-08-06 Europe/Oslo**.

This document governs the remaining work required to turn the merged Federation
implementation into a stable, documented, supportable FCP Federation v1
release.

Use these sources together:

- `docs/implementation/federation/active/capability_first_federation_plan.md` is authoritative for
  current capability-first product behavior, compatibility, authority, and
  acceptance sequencing;
- `catalog/federation/tests/cf7_acceptance/scenarios.json` is authoritative for
  recorded automated and physical acceptance claims;
- this document is authoritative for repository cleanup, documentation,
  release-gate, release-candidate, and publication work.

Historical phase plans, completion reviews, test matrices, and agent handoffs
provide delivery evidence. They do not override the sources above.

## Current repository state

The technical Federation baseline and the capability-first product baseline are
merged.

The supported mandatory first-run flow is:

```text
Identity
  -> Federation
  -> Inspect
  -> finish setup
  -> open Federation
```

Benchmarks and contribution decisions are optional follow-up work. They do not
block access to the normal FCP workbench and do not grant authority by
themselves.

Current release state:

- the completed Federation technical baseline through F8.7 remains valid;
- the capability-first CF0-CF7 implementation baseline is merged;
- the integrated `/docs` reader is implemented;
- complete physical CF7 acceptance is **not accepted**;
- CF8 retirement of the retained role-first compatibility path is **blocked**
  until CF7 is accepted;
- no Federation v1 release tag has been created.

The earlier instructions to implement CF1, launch capability-first parallel
waves, or replace the setup UI are historical and must not be followed.

## Objective

Complete the remaining validation, cleanup, documentation, and release work
without weakening the merged Federation authority and compatibility model.

The closeout remains a sequence of small, independently reviewable changes.
Cleanup must never remove a path merely because it looks old. Every deletion
requires dependency evidence and regression validation.

## Fixed decisions

- The completed Federation technical baseline remains **FCP Federation v1.0**
  through F8.7.
- V1 is for explicitly trusted devices and providers.
- Runtime authority and security boundaries from the completed phases remain
  unchanged.
- Capability-first onboarding is the supported product path.
- The internal session boundary remains during the compatible migration.
- UI and documentation improvements must not invent a second authority source.
- Benchmark evidence must never grant membership or contribution authority.
- No implementation branch is deleted without separate explicit owner approval.
- No release tag is created before exact release acceptance.
- CF8 must not begin before evidence-backed CF7 acceptance.

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

Completed deliverable: one path-level manifest covering proposed deletion,
relocation, consolidation, archive, and defer decisions.

The manifest remains useful for cleanup. Product or setup assumptions that
conflict with the active capability-first plan are superseded by that plan.

### V1-C — generated output and experiment cleanup

Completed or accepted:

- generated `graphify-out/` content was removed and ignored;
- the integrated `/docs` reader was implemented and accepted;
- the standalone Markdown viewer is a documented deletion candidate.

Remaining work:

- verify and remove the superseded standalone Markdown viewer in a separate,
  bounded cleanup change;
- classify other `new-stuff/` experiments individually;
- preserve any experiment that still has a supported runtime, test, migration,
  or documentation dependency.

### CF0-CF7 — capability-first onboarding and Federation UI

Implementation status: **merged baseline**.

The complete product behavior, compatibility rules, authority boundaries, and
remaining acceptance requirements are defined in:

- `docs/implementation/federation/active/capability_first_federation_plan.md`

Do not restart the old CF1-first implementation sequence.

Remaining capability-first work is limited to:

- resolving verified post-merge runtime, persistence, platform, privacy, or
  compatibility defects;
- reconciling documentation with the merged product;
- freezing one exact acceptance candidate;
- executing the complete physical CF7 campaign on that candidate;
- updating acceptance claims only through a separate evidence-backed review.

### CF8 — retained role-first compatibility retirement

Status: **blocked**.

CF8 may be planned and implemented only after complete CF7 acceptance confirms
that the capability-first path safely covers supported fresh setup, reconnect,
migration, restart, and contribution behavior.

CF8 must remain a separately reviewed change. It must not be folded into
cleanup, documentation reconciliation, or acceptance evidence work.

### V1-D — obsolete implementation cleanup

Expected scope after dependency proof:

- remove duplicate old web implementation if `catalog/webapp/` has no supported
  entry point;
- reduce `legacy/` to explicitly justified historical/reference material;
- remove obsolete scripts and stale documentation;
- update AI grounding/indexing when indexed paths change;
- remove dead tests only when equivalent product behavior remains covered.

Exit criteria:

- one supported Flask application path;
- no hidden duplicate behavior relied upon by setup or Compose;
- full affected regression suites green.

### V1-E — documentation consolidation

Target canonical structure:

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

Required public documents include:

- product overview;
- quick start;
- installation and capability-first onboarding;
- Federation discovery, verification, join, reconnect, and local creation;
- device inspection and optional benchmarks;
- contribution management;
- device and provider administration;
- storage, AI, compute, and recorder guides;
- security/trust model;
- backup, restart, recovery, and upgrade;
- troubleshooting;
- compatibility/protocol reference;
- developer architecture and test guide.

Required cleanup:

- keep `docs/index.md` as the canonical documentation entry point;
- replace the oversized root README with a concise repository entry point;
- move durable design evidence to history/decisions;
- remove or archive superseded phase plans and handoffs after link verification;
- mark retained history as non-current;
- validate all links and commands.

Exit criteria:

- a new user can identify the correct first document;
- current behavior is described without phase archaeology;
- setup documentation does not instruct a new user to select a permanent device
  role;
- technical session terminology is kept out of ordinary onboarding guidance;
- `/docs` exposes the canonical structure correctly.

### V1-F — permanent regression gate

Deliver:

- one named Federation release workflow;
- Linux and Windows coverage;
- complete identity, membership/session compatibility, relay, transport,
  storage, failover, capability, benchmark, contribution, AI, compute,
  recorder, artifact, Flask, onboarding, migration, and compatibility matrix;
- compile, Ruff, Compose, diff hygiene, and documentation-link checks;
- retained focused component workflows only where they add fault isolation.

Exit criteria:

- the release gate is equivalent to or stronger than the union of required
  closeout gates;
- completed phase names are no longer the only permanent quality signal;
- no workflow is deleted before replacement evidence exists.

### V1-G — release candidate acceptance

Required acceptance:

- fresh checkout installation on Linux and Windows;
- first-run capability-first onboarding without old state;
- stable device identity creation;
- existing Federation discovery and verified join;
- safe local Federation creation when no candidate exists;
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
- all user commands and documentation links checked.

Record exact hardware and network constraints. Do not overclaim unsupported
public-internet acceptance.

### V1-H — release publication

Deliver:

- `CHANGELOG.md`;
- release notes;
- exact version declaration;
- verified release commit;
- release tag;
- updated original Federation issue/status;
- post-release branch-cleanup proposal presented separately.

Exit criteria:

- the tag points to the validated commit;
- release notes match actual scope and limitations;
- no pending mandatory onboarding, migration, or closeout action is hidden in a
  historical plan.

## Work-isolation policy

The earlier capability-first parallel-wave instructions are historical.
Remaining closeout work must use bounded branches and non-overlapping ownership.

- scope each PR to one defect, acceptance unit, documentation reconciliation,
  cleanup unit, or release deliverable;
- do not combine CF7 evidence, CF8 retirement, documentation reorganization,
  and obsolete-code deletion in one PR;
- declare shared Flask, setup, navigation, persistence, security, and workflow
  files before editing them;
- do not run parallel changes that need the same shared files;
- preserve draft-PR review and cross-platform validation before merge.

## Stop conditions

Stop and report rather than broaden scope when:

- discovery would require unauthenticated trust;
- a benchmark would run arbitrary remotely supplied code;
- a benchmark result is treated as authority;
- migration would silently enable a contribution not previously configured;
- a user-facing simplification would weaken membership, revocation, lease,
  fencing, storage, job, or artifact checks;
- two active changes need to edit the same shared integration file;
- removing `session_id` becomes necessary for a compatible unit;
- public documentation would require claiming behavior not demonstrated by
  acceptance;
- cleanup requires a new unrelated feature;
- a test indicates lost compatibility, split brain, stale execution, or data
  risk.

## Next exact actions

Proceed in this order:

1. complete the bounded documentation reconciliation so current startup and
   planning documents describe the merged capability-first product;
2. resolve any verified post-merge runtime-parity, persisted-provider,
   native-host translation, platform, privacy, or compatibility defects;
3. freeze one exact release-candidate commit only after known blockers are
   closed;
4. execute the complete physical CF7 acceptance campaign on that commit;
5. update acceptance claims only through a separate evidence-backed review;
6. plan CF8 separately after CF7 is accepted;
7. continue V1-D through V1-H as separately reviewable closeout units.

Do **not** implement CF1 again. Do **not** begin CF8 early.
