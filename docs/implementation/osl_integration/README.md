# OSL integration plan

Status: **active planning package; production implementation not started**.  
Reviewed: **2026-08-06 Europe/Oslo**.  
Authoritative sequence: [10_phased_implementation_roadmap.md](10_phased_implementation_roadmap.md).  
Current next delivery: **D0-A — profile, authority, and compatibility decisions only**.

## How to use this directory

This directory contains one active execution plan and source-pinned supporting
analysis. It is not a collection of independent backlogs.

Use the material in this order:

1. [10 — implementation execution plan](10_phased_implementation_roadmap.md)
   decides status, implementation order, acceptance gates, and stop conditions.
2. This README is the single entry point for the planning package and its
   current status.
3. Documents `00`-`09` are detailed source, architecture, contract, workflow,
   UI, validation, migration, and file-planning references.
4. `docs/planned-work/method-osl-msh-alignment.md` defines the W3 end-to-end
   acceptance scenario. It is not a separate implementation plan, the current
   entry point, or permission to implement W3 in one PR.

When a supporting document conflicts with the execution plan, stop and update
the plan. Do not choose the more permissive interpretation.

## Document map

| Document | Role |
| --- | --- |
| [10 — implementation execution plan](10_phased_implementation_roadmap.md) | **Authoritative:** status, order, next delivery, gates, and stop conditions |
| [00 — scope and sources](00_scope_and_sources.md) | source pins, classification markers, and claim boundaries |
| [01 — language requirements](01_language_requirements.md) | source-derived OSL requirements and unresolved research questions |
| [02 — current MSH architecture](02_current_msh_architecture.md) | analyzed MSH snapshot; revalidate against current `main` before implementation |
| [03 — target architecture](03_target_architecture.md) | proposed component and authority boundaries |
| [04 — Notebook-to-OSL workflow](04_notebook_to_osl_workflow.md) | proposed research-to-product workflow mapping |
| [05 — data model and contracts](05_data_model_and_contracts.md) | proposed contract shapes and examples |
| [06 — repository file plan](06_repository_file_plan.md) | candidate file-level detail; paths are not authoritative until revalidated |
| [07 — API, UI, and user journeys](07_api_ui_and_user_journeys.md) | proposed surfaces and failure states |
| [08 — validation, testing, and CI](08_validation_testing_and_ci.md) | required evidence and permanent gates |
| [09 — migration and compatibility](09_migration_and_compatibility.md) | compatibility, migration, rollback, and cutover analysis |

## Planning baselines

The detailed source analysis was pinned to:

- MSH: `f580c71f7269643a077cc7e7db8ba9bf6050bb6a`;
- systems-paper: `ff098ce52f15b489b6a07d5b55c6c788d862e3be`;
- paper-repo: `abe3fbcddee590c3f399b06f63cb329e8615977c`.

The execution plan was reconciled against MSH `main` at
`349da7cd0007bf3e0f97127ea6696fd325e3c583` on 2026-08-06.

The paper pins remain deliberate research inputs until explicitly revised. The
old MSH pin is an analyzed snapshot, not permission to implement against stale
routes, services, files, tests, or security assumptions. Every delivery must
start from current `main` and re-audit all affected integration points.

The supporting documents preserve detailed candidate lifecycle analysis that is
still valuable, including:

- low-friction source capture and immutable preservation;
- stable excerpts and candidate extraction with provenance;
- committed immutable OSL revisions distinct from autosaved client drafts;
- deterministic profile validation without treating validation as truth or
  approval;
- authenticated human review of an exact unchanged revision;
- separate approval and publication commands;
- correction, rejection, deprecation, supersession, migration, and audit
  history;
- AI limited to attributed suggestions and explanations.

These are planned requirements and designs, not claims of implemented MSH
behavior.

## Fixed decisions and boundaries

- OSL is a versioned, non-executing bounded context under `catalog/osl/`.
- JSON and SysML v2 are representations or adapters, not the language itself.
- Raw source, excerpts, candidates, immutable revisions, validation, human
  review, approval, publication, and feedback remain separate.
- Decision and OperatorAction remain distinct.
- Validation is not truth, safety assurance, evidence acceptance, or approval.
- Review, approval, and publication bind an authenticated human decision to an
  exact unchanged revision.
- Publication grants no operational, compute, storage, provider, capability,
  job, Federation, or machine authority.
- The manual Notebook-to-OSL workflow precedes AI.
- AI remains candidate-only and cannot sign, approve, publish, or create
  canonical state.
- Canonical JSON precedes optional one-way SysML v2 export.
- Legacy migration is conservative and never inflates `structured`, `reusable`,
  `validated`, or `model-ready` into approval or publication.
- No legacy/canonical dual write, source overwrite, or destructive normal
  rollback.

## Current status

Completed:

- source and architecture analysis;
- proposed contracts and file map;
- workflow, UI, validation, migration, and delivery planning;
- W3 acceptance-scenario definition.

Not completed:

- accepted first OSL profile;
- accepted authority and compatibility policies;
- production OSL contracts, persistence, lifecycle, API, UI, AI, migration, or
  SysML v2 adapter;
- proof that existing operator-note or legacy SysML behavior conforms to OSL.

Do not infer from these planning files that save, completion, review, approval,
publication, migration, or SysML/OSL conformance has been implemented.

## Current next action

Implement **D0-A only** from the execution plan:

1. create `docs/osl_language_profile.md`;
2. create `docs/osl_authority_boundary.md`;
3. create `docs/osl_compatibility_policy.md`;
4. update `docs/agent_notes/osl_sysml_alignment.md`;
5. obtain research/domain and security/product review;
6. merge the documentation decisions before starting D1-A.

D0-A must contain no production code. The first code delivery after D0-A is
accepted and merged is D1-A: pure immutable identifiers, source, evidence, and
strategy-fragment contracts with focused tests and no Flask, SQLite, AI, SysML,
migration, Federation, capability, provider, storage, or runtime imports.

## Historical record

Documents `00`-`10` were produced incrementally from pinned research and
repository sources. Git history preserves that planning record. Historical
commit order and old status notes do not define current implementation status;
the execution plan does.
