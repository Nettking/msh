# Federation v1 closeout plan

Status: **active release-closeout plan**.

Reviewed: **2026-08-11 Europe/Oslo**.

This document governs the remaining work required to turn the merged Federation implementation into a stable, documented, supportable FCP Federation v1 release.

Use these sources together:

- `docs/implementation/current_task_handoff.md` and `docs/implementation/index.md` define the current merged status and supersede pre-CF8 sequencing statements in older plans;
- `docs/implementation/federation/active/capability_first_federation_plan.md` remains the durable capability-first product/authority reference where its status text has not been superseded;
- `catalog/federation/tests/cf7_acceptance/scenarios.json` is authoritative for recorded acceptance claims;
- this document is authoritative for remaining repository cleanup, documentation, release-gate, release-candidate, and publication work.

Historical phase plans, completion reviews, test matrices, and agent handoffs provide delivery evidence. They do not override the current status sources above.

## Current repository state

The technical Federation baseline, capability-first product baseline, and CF8 retirement of the role-first installed-product runtime are merged.

The supported mandatory first-run flow is:

```text
Identity
  -> Federation
  -> Inspect
  -> finish setup
  -> open Federation
```

Benchmarks and contribution decisions are optional follow-up work. They do not block access to the normal FCP workbench and do not grant authority by themselves.

The post-CF8 baseline also includes:

- capability-scoped runtime/configuration rather than permanent device roles;
- verified manual Federation-wide software updates through bounded host-owned update agents;
- conservative Windows migration onto the current launcher/update-agent path;
- headless standalone-recorder Federation pairing;
- checkpoint-gated recorder publication through logical-storage authority;
- startup bounded MTConnect discovery/first-selection for the standalone recorder; and
- bounded Federation-wide recorder scan/source control from trusted members.

Current release state:

- complete physical CF7 acceptance is **not accepted**;
- complete Federation v1 end-to-end acceptance remains **false**;
- no Federation v1 release tag has been created.

CF8 is no longer open work. Any old statement that CF8 is blocked or must not begin is historical sequencing, not current status.

## Objective

Complete remaining validation, acceptance reconciliation, cleanup, documentation, and release work without weakening the merged Federation authority model.

Closeout remains a sequence of small, independently reviewable changes. Cleanup must never remove a path merely because it looks old. Every deletion requires dependency evidence and regression validation.

## Fixed decisions

- V1 is for explicitly trusted devices and providers.
- Capability-first onboarding/runtime is the supported product path.
- The internal session boundary remains during compatible v1 operation.
- UI/documentation improvements must not invent a second authority source.
- Benchmark evidence must never grant membership or contribution authority.
- Federation software updates remain explicit/manual, exact-commit, and host-validated; they are not a generic remote shell.
- Recorder control remains bounded to recorder-local scans and latest-scan source selection; it is not arbitrary network/URL/process authority.
- A standalone `python start_recorder.py` process is not currently restarted by the normal Flask/Compose **Update all devices** activation path.
- No implementation branch is deleted without separate explicit owner approval.
- No release tag is created before exact release acceptance.

## Completed/merged closeout work

The following are no longer future work:

- capability-first onboarding and product composition;
- deletion of confirmed dead role-first Flask/product paths;
- retirement of the role-first installed-product runtime (CF8);
- role-free command/Termux bootstrap composition;
- current capability configuration store and recorder/AI technical configuration migration;
- product rename to Federated Capability Platform (FCP);
- verified Federation-wide runtime update implementation and Windows hardening/migration bootstrap;
- headless standalone-recorder Federation bootstrap/publication;
- Federation-wide standalone recorder discovery/source control;
- integrated `/docs` reader and current documentation entry point.

Do not restart these implementation waves unless a concrete regression/defect is reproduced against current `main`.

## Remaining work packages

### V1-D — obsolete implementation cleanup

Continue only after dependency proof. Candidate work may include:

- remaining superseded experiments/viewers with no supported runtime/test/migration/documentation dependency;
- obsolete scripts or stale generated artifacts;
- archived historical material that can be moved without breaking current references; and
- dead tests only when equivalent current behavior remains covered.

Exit criteria:

- one supported Flask application path;
- no hidden duplicate behavior relied upon by startup, migration, Compose, Federation, or recorder operation;
- full affected regression suites green.

### V1-E — documentation consolidation

Current user documentation must describe the post-CF8 product directly, including:

- normal Windows/POSIX startup and resume/reset boundaries;
- conservative Windows migration;
- current pairing lifetime/reissue behavior;
- manual Federation-wide software updates and per-device success/failure semantics;
- headless standalone-recorder first start with `python start_recorder.py FCP1-...`;
- recorder startup scan, remote scan/source control, and local-first Federation publication;
- distinction between Compose-managed recorder updates and independently launched standalone recorder process administration;
- current capability-first authority boundaries and the fact that complete physical CF7 acceptance remains false.

Exit criteria:

- a new user can identify the correct first document;
- current behavior is described without phase archaeology;
- no current guide instructs a new user to select a permanent device role;
- technical session terminology is kept out of ordinary onboarding guidance where unnecessary;
- `/docs` exposes the canonical structure correctly;
- current implementation/acceptance indexes do not claim CF8 is still blocked.

### V1-F — permanent regression gate

Maintain one Federation release workflow with Linux and Windows coverage strong enough to protect:

- identity/membership/session compatibility;
- relay/transport/storage/failover;
- capability inspection/benchmark/contribution;
- AI/compute/jobs/artifacts;
- recorder capture/publication/control;
- software update/migration boundaries;
- Flask/onboarding/product navigation;
- compile, Ruff, Compose, diff hygiene, and documentation-link checks.

Do not remove focused workflows until replacement evidence is equivalent or stronger.

### V1-G — release candidate acceptance

Freeze one exact candidate only after known blockers are closed. Required physical/review evidence must cover the current product, not the pre-CF8 baseline.

At minimum validate, where applicable to the acceptance contract:

- fresh Windows and Linux installation;
- capability-first first-run onboarding;
- persistent identity and Federation join/reconnect;
- safe local Federation creation;
- migration from supported older installations;
- one device contributing multiple capabilities;
- benchmark/reconciliation behavior after relevant dependency changes;
- storage replication/failover/recovery;
- AI and registered-compute contribution lifecycles;
- recorder + AI coexistence;
- headless standalone-recorder pairing/startup scan/local capture/publication;
- remote recorder scan/source control from another trusted Federation device;
- coordinator-owned software update check/rollout where the target is an updater-capable normal FCP installation;
- restart, disable/re-enable, revocation, fencing, and controlled rejoin;
- mobile and desktop browser review;
- diagnostics without secret/private endpoint/source-URL leakage;
- backup/recovery rehearsal; and
- all user commands/documentation links used during the campaign.

A successful feature-specific live test or green CI run is useful evidence but does not independently change the acceptance manifest.

### V1-H — release publication

After exact candidate acceptance:

- finalize `CHANGELOG.md`/release notes;
- declare exact version;
- identify verified release commit;
- create release tag;
- update original Federation issue/status;
- present post-release branch cleanup separately.

The tag must point to the validated commit and release notes must match actual scope/limitations.

## Work-isolation policy

- scope each PR to one defect, acceptance unit, documentation reconciliation, cleanup unit, or release deliverable;
- do not combine physical acceptance evidence, broad documentation reorganization, unrelated runtime fixes, and obsolete-code deletion in one PR;
- declare shared Flask/setup/navigation/persistence/security/update/recorder-control/workflow files before editing them;
- preserve draft-PR review and cross-platform validation before merge;
- do not use migration/update/recorder control as a reason to grant broader host or network authority.

## Stop conditions

Stop and report rather than broaden scope when:

- discovery would require unauthenticated trust;
- a benchmark would run arbitrary remotely supplied code;
- a benchmark result is treated as authority;
- migration would silently enable a contribution or require destructive state guessing;
- an update path would require peer-supplied shell/Docker/Git authority beyond the fixed approved operation;
- recorder control would require arbitrary source URLs, credentials, shell execution, or unrestricted scanning;
- a user-facing simplification would weaken membership, revocation, lease, fencing, storage, job, artifact, update, or recorder-control checks;
- removing `session_id` becomes necessary for a compatible unit;
- public documentation would require claiming behavior not demonstrated by acceptance; or
- a test indicates lost compatibility, split brain, stale execution, data loss, or authority leakage.

## Next exact actions

Proceed in this order:

1. complete documentation/acceptance-runbook reconciliation against the current post-CF8, update-capable, recorder-capable product;
2. resolve any verified current-main runtime, platform, privacy, migration, update, recorder-control, or persistence defects;
3. freeze one exact release-candidate commit after known blockers are closed;
4. execute the complete physical CF7 acceptance campaign on that same commit;
5. update acceptance claims only through a separate evidence-backed review; and
6. continue V1-D/V1-F/V1-H as separate bounded closeout units and create the release tag only after acceptance.

Do **not** restart CF1-CF8 implementation waves merely because older plans still describe them as future work.
