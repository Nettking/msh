# OSL integration implementation plan

## Status

All requested planning documents 00-10 are complete and committed on
`agent/osl-language-and-paper-workflow-plan`. This directory contains planning
and documentation only; it does not implement OSL production code.

Analyzed repository baselines:

- MSH: `f580c71f7269643a077cc7e7db8ba9bf6050bb6a`;
- systems-paper: `ff098ce52f15b489b6a07d5b55c6c788d862e3be`;
- paper-repo: `abe3fbcddee590c3f399b06f63cb329e8615977c`.

## Reading order and commit ledger

| Order | Document | Status | Commit |
|---:|---|---|---|
| 00 | [Scope and sources](00_scope_and_sources.md) | complete | `a84374e51d2318782fb4b798703c156859514f3e` |
| 01 | [Language requirements](01_language_requirements.md) | complete | `8d09bc95c737948b434f64f3d0c52412c1cd23e8` |
| 02 | [Current MSH architecture](02_current_msh_architecture.md) | complete | `a5b3f098c427e18aff76577a21ce16d6ed466cfb` |
| 03 | [Target architecture](03_target_architecture.md) | complete | `f437833ab6e886eb1b8ae67e4501fa0dd7130c83` |
| 04 | [Notebook-to-OSL workflow](04_notebook_to_osl_workflow.md) | complete | `3c42455253897d43895bb67f5d1890ea9659946d` |
| 05 | [Data model and contracts](05_data_model_and_contracts.md) | complete | `cd1f8e330e0c05e07163c7a483d940b0150e3870` |
| 06 | [Repository file plan](06_repository_file_plan.md) | complete | `a1dc0deb1178d6171981ad8f1d8d085a3374b0d8` |
| 07 | [API, UI, and user journeys](07_api_ui_and_user_journeys.md) | complete | `81852d33553a12b9984ac2a6f9d64d8e842e62cf` |
| 08 | [Validation, testing, and CI](08_validation_testing_and_ci.md) | complete | `e36a9b8d5291d58f495062e1344c872724113744` |
| 09 | [Migration and compatibility](09_migration_and_compatibility.md) | complete | `cbba254bcdede391a87eb80e5bd6966055cd2848` |
| 10 | [Phased implementation roadmap](10_phased_implementation_roadmap.md) | complete | `ab52961d21208d2b231b9c22c1480d096c125e96` |
| index | `README.md` | complete; initial index | `34b7b609d63cb3cc67cbbb421c4d4faa2269bb97` |

Each document is useful on its own and was committed immediately after its
document-level consistency check. Later commits do not squash or rewrite the
earlier planning history.

## How to use the plan

1. Read 00 for source authority, scope and marker definitions.
2. Read 01 and 04 before interpreting paper requirements.
3. Read 02 before changing existing MSH services/routes/storage.
4. Use 03 and 05 for component and contract boundaries.
5. Use 06 as the concrete file-by-file implementation backlog.
6. Use 07-09 for exposure, permanent gates and compatibility.
7. Implement only one reviewable phase from 10 at a time.

All documents consistently distinguish `paper-defined`,
`existing-in-MSH`, `proposed-for-MSH` and
`requires-research-clarification`.

## Important decisions

- OSL is planned as a versioned bounded context under `catalog/osl/`, not as an
  extension of the current mutable operator-note JSON service.
- The first profile is source-pinned to the analyzed systems-paper research
  artefact and explicitly bounded; it is not marketed as a stable OSL 1.0.
- Abstract syntax, relations, semantic WF rules, maturity and provenance are
  language concerns. JSON and SysML v2 are versioned representations/adapters.
- Decision and OperatorAction remain different; a candidate/selected Action is
  not a recommendation, human approval, authorized action or execution.
- Raw source, excerpt, candidate, immutable draft revision, review, approval,
  publication, deprecation/supersession and feedback are separate records.
- Original source/evidence and all canonical revisions are immutable. Correction
  and migration create new linked objects.
- Validation is deterministic evidence about one profile/revision/hash. It is
  not truth, evidence acceptance, safety assurance or approval.
- Review, approval and publication are distinct authenticated human commands on
  an exact unchanged revision. Publication grants no operational, compute,
  storage, capability or federation authority.
- The complete manual Notebook-to-OSL path precedes AI. AI may propose,
  classify, structure and explain candidates only.
- MSH's existing AI runtime, capability job store and federation projections
  supply useful adapter patterns, not OSL domain authority.
- Canonical JSON is first. SysML v2 is optional export-only until independent
  fidelity evidence exists; SysML is not the whole language.
- Existing operator notes and paper examples migrate conservatively as source,
  candidates, metadata and findings. `reusable`/`validated`/`model-ready` never
  becomes local approval/publication.
- No legacy/canonical dual write, no source overwrite and no destructive normal
  rollback.
- Permanent gates cover WF1-WF15, authority, provenance, leakage, migrations,
  deterministic exports, accessibility/mobile and Linux/Windows behavior.

## Open questions requiring a decision

### Research/profile

- public profile/version name and exact first bounded semantic subset;
- mandatory applicability semantics;
- whether `ValidationNeed` alone satisfies provisional maturity;
- stable element/relation identity and revision semantics;
- relation qualification, element-level composition and rich review scope;
- quantitative confidence and extension namespace governance;
- criteria/toolchain for supported SysML v2 export and any future import.

### Product and security

- authenticated human identity provider and source/role/scope model;
- mandatory separation of reviewer, approver and publisher, including small
  deployments and recent/two-person authentication;
- source classifications permitted for local/remote AI;
- capture media, size and excerpt selector types;
- provenance/reviewer identity visibility by audience;
- legal retention, consent withdrawal and erasure without false history;
- supported UI locales and browser/accessibility audit tooling.

### Compatibility and operations

- canonical JSON Unicode/number/time/hash details;
- supported reader/exporter retention windows and same-major promises;
- profile deprecation/removal ownership;
- branching/merge behavior beyond parent lineage and supersession;
- legacy read-only/cutover window and operator acceptance owner.

Every unresolved item has a fail-closed or explicitly deferred behavior in the
plan; none should be silently decided by an implementation convenience.

## Recommended first implementation delivery

Implement **Phase 0 only** from
[10_phased_implementation_roadmap.md](10_phased_implementation_roadmap.md):

1. `docs/osl_language_profile.md`;
2. `docs/osl_authority_boundary.md`;
3. `docs/osl_compatibility_policy.md`;
4. update the legacy SysML alignment note;
5. obtain research/domain/security/product review.

After Phase 0 is accepted, the first code PR is Phase 1A: pure immutable IDs,
source/evidence/fragment contracts and tests. It must contain no Flask, AI,
SQLite, SysML, migration, federation, capability or operational binding.

## Final verification status

Verification completed on branch
`agent/osl-language-and-paper-workflow-plan` after the initial index commit:

- **Document set:** passed; all 12 expected Markdown files (00-10 plus this
  index) exist and no unexpected file is present in the plan directory.
- **Internal Markdown links:** passed; zero unresolved local link targets.
- **Planned JSON examples:** passed; all three fenced JSON examples parse.
- **Whitespace/diff hygiene:** `git diff --check origin/main...HEAD` passed.
- **Source/marker audit:** all documents record the relevant exact source
  baseline and use the classification markers according to their scope; the
  scope/index records all three full repository SHAs.
- **Incremental history:** passed; 12 pre-status commits exist after updated
  `main` and each introduced exactly one bounded planning document.
- **Docs-only audit:** passed; the 12 changed files before this status update are
  all under `docs/implementation/osl_integration/`. No production code,
  workflow or external paper repository was changed.
- **Focused existing baseline:** passed; 8 tests across
  `test_operator_strategy_service.py`, `test_osl_sysml_export.py` and
  `test_operator_strategy_lifecycle.py` passed on the Windows host in an
  isolated Python 3.12/pytest runtime.
- **Markdown linter:** no Markdown-specific linter is configured in the analyzed
  repository; link, JSON, encoding-sentinel and Git whitespace checks were used
  instead.
- **Temporary test data:** removed after the test run.

The Git commit containing this section is the separate final status update.
Remote push and final clean-worktree verification occur after that commit and
are reported in the task handoff, avoiding a self-referential third README
status commit.
