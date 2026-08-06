# OSL integration planning scope and source baseline

Status: planning scope established; implementation has not started.

This document fixes the evidence baseline, authority boundary, and production
order for a docs-only implementation plan. It is useful independently as the
scope contract for later planning and does not define an implemented MSH
contract.

## Classification markers

The plan uses these markers whenever a statement could otherwise blur research,
current software, or a product proposal:

- `paper-defined`: stated by an analyzed paper or one of its authoritative
  supporting artefacts. This does not by itself make the statement an MSH
  product guarantee.
- `existing-in-MSH`: verified in the analyzed MSH source tree at the SHA below.
- `proposed-for-MSH`: a design or delivery recommendation made by this plan,
  not current behavior.
- `requires-research-clarification`: ambiguous, inconsistent, unevaluated, or
  outside the papers' demonstrated claims; it must not silently become a
  product guarantee.

## Goal

Produce a source-traceable, phased implementation plan for making OSL a real,
versioned language capability in MSH and for supporting the Notebook-to-OSL
elicitation workflow. The plan must preserve the separation between evidence,
interpretation, recommendation, human approval, authorized action, and
observed outcome.

The target starts with a non-executing, authority-safe language core. It must
describe language semantics and lifecycle in addition to serialization. SysML
v2 is treated as an optional realization and interoperability adapter rather
than as the whole language or its primary contribution.

## Scope

- Extract the normative and non-normative OSL requirements from
  `systems-paper`, including abstract syntax, relations, well-formedness,
  lifecycle, review meaning, provenance, uncertainty, and representations.
- Map relevant MSH capture, storage, provenance, AI, authorization, Flask/UI,
  federation, capability, event, audit, import/export, test, and CI behavior.
- Translate the Notebook-to-OSL method in `paper-repo` into an explicit MSH
  candidate workflow while preserving unchanged source material.
- Define target components, contracts, file locations, boundaries, migrations,
  user journeys, validation, tests, and small implementation phases.
- Record contradictions and unanswered questions instead of resolving them by
  invention.

## Explicit non-goals

- No production code, schema migration, route, template, workflow automation,
  or CI workflow is implemented in this branch.
- No change is made to `systems-paper` or `paper-repo`.
- No claim in either paper is promoted to an implemented guarantee without
  source and implementation evidence.
- No automatic approval, publication, operational execution, compute/storage
  authority assignment, or evidence verification is delegated to AI.
- No first delivery binds an OSL model directly to machinery or other operator
  action.
- No GitHub Actions workflow is planned for `paper-repo`; "workflow" means the
  domain Notebook-to-OSL process.
- OSL is not reduced to JSON, YAML, SysML v2, or another single serialization.
- UI state is not the source of truth, source evidence is not overwritten, and
  historical OSL versions are not mutated in place.

## Frozen repository snapshots

All source citations in the plan must resolve against these exact commits.
Later upstream changes are out of scope unless this table is deliberately
updated with an explanation.

| Repository | Analyzed commit SHA | Role in this plan | Initial source orientation |
| --- | --- | --- | --- |
| `Nettking/msh` | `f580c71f7269643a077cc7e7db8ba9bf6050bb6a` | Application and implementation target | `existing-in-MSH`: current `main` already contains operator capture/review/strategy services, OSL/SysML export surfaces, local/federated storage, capability authority, AI integration, Flask UI, and Linux/Windows checks; exact guarantees still require file-level analysis. |
| `Nettking/systems-paper` | `ff098ce52f15b489b6a07d5b55c6c788d862e3be` | OSL v0.1 representation source | `paper-defined`: repository instructions identify `sysml/osl-core.sysml` as the preliminary authoritative abstract syntax and the paper sections and validation artefacts as its semantic and evidence boundary. |
| `Nettking/paper-repo` | `abe3fbcddee590c3f399b06f63cb329e8615977c` | Notebook-to-OSL elicitation source | `paper-defined`: repository instructions assign this paper the field-note workflow, annotation schema, candidate clarification/validation, and model-readiness criteria. |

"Analyzed commit" means the immutable source snapshot used by the completed
plan. At this first checkpoint, only repository instructions, structure, and
source ownership have been oriented; later documents add file-and-section
citations and may refine the preliminary observations without changing the SHA.

## Contribution and authority boundaries

- `paper-defined`: `paper-repo` owns elicitation up to model-ready candidate
  strategy fragments; `systems-paper` begins from candidate fragments and owns
  the preliminary OSL representation proposal.
- `paper-defined`: OSL v0.1 is design-time and preliminary. Domain review of
  represented meaning is not operational validation, safety assurance, or
  recommender performance evidence.
- `existing-in-MSH`: similarly named capture, strategy, lifecycle, export, AI,
  storage, capability, and federation functions already exist. Name overlap
  does not establish semantic conformance to either paper.
- `proposed-for-MSH`: generated candidates remain non-canonical until explicit
  human review and approval; approval still does not grant operational,
  compute, storage, or execution authority.
- `requires-research-clarification`: any operational binding, executable OSL
  subset, safety semantics, or meaning of publication/activation beyond
  design-time use needs separate research and product authorization.

## Central terms to keep distinct

| Term | Scope meaning |
| --- | --- |
| Source artefact / raw capture | Immutable original notebook, note, transcript, or observation material plus acquisition metadata. |
| Source excerpt | Addressed selection from a source artefact; it never replaces the original. |
| Observation / evidence | Recorded input or traceable support; neither is automatically a verified fact. |
| Interpretation / assessment | A human or AI-produced reading of evidence, explicitly attributed and revisable. |
| Candidate | Extracted or generated content that has no canonical authority. |
| Strategy fragment | A typed OSL unit with explicit context, conditions, decision/action structure, outcomes, rationale, evidence, and gaps as required by the language baseline. |
| `Decision` | A choice or selection commitment; it is not itself the physical or digital act that follows. |
| `OperatorAction` | A represented operator act; representation or approval is not execution authority. |
| Recommendation | Advice derived from a model or interpretation; it is not approval or authorization. |
| Review / approval | Human decisions over represented meaning and lifecycle; they must identify reviewer, scope, time, and version. |
| Authorized action | A separately governed permission to execute; out of scope for the initial OSL language core. |
| Outcome / feedback | Expected or observed consequence, with observed feedback separately sourced and never backfilled as an expected fact. |

## Preliminary source set

The exact section-level map will be recorded in the following documents.

- `systems-paper`: `REPO_INDEX.md`, `publication-readiness.md`,
  `planning/2026-07-29-osl-detailed-revision-plan.md`,
  `sysml/osl-core.sysml`, `sysml/osl-keywords.sysml`,
  `sysml/osl-semantic-projections.sysml`,
  `sysml/traceability-mapping.md`, validation scenarios and negative examples,
  `tex/sections/osl_core_definition.tex`,
  `tex/sections/osl_capabilities.tex`,
  `tex/sections/osl_workflow_roles.tex`, evaluation contracts/matrices, and
  selected examples.
- `paper-repo`: `papers/notebook-to-osl/capture-schema.md`,
  `concept-brief.md`, `knowledge-base.md`, `outline.md`, and manuscript sections
  `03_research_design.tex` through `07_discussion.tex`, especially
  `04_method.tex`, `05_annotation_schema.tex`, and `06_illustrative_case.tex`.
- `msh`: repository and architecture docs; operator capture, review, strategy,
  lifecycle, and OSL export routes/services/templates/tests; storage and data
  contracts; AI routes/services; capability and federation authority code;
  event/audit paths; import/export code; and `.github/workflows`.

## Planned document set and production order

Each document is completed and committed before the next begins.

1. `00_scope_and_sources.md` — this scope contract and SHA baseline.
2. `01_language_requirements.md` — OSL language core and traceability matrix.
3. `02_current_msh_architecture.md` — verified reusable and incompatible MSH
   surfaces.
4. `03_target_architecture.md` — component boundaries, authority, and read/write
   flows.
5. `04_notebook_to_osl_workflow.md` — end-to-end elicitation and lifecycle flow.
6. `05_data_model_and_contracts.md` — planned contracts and examples.
7. `06_repository_file_plan.md` — exact proposed file changes by delivery.
8. `07_api_ui_and_user_journeys.md` — routes, screens, failure states, and
   journeys.
9. `08_validation_testing_and_ci.md` — test layers and permanent merge gates.
10. `09_migration_and_compatibility.md` — versions, legacy data, rollback, and
    extension handling.
11. `10_phased_implementation_roadmap.md` — small sequential implementation
    phases.
12. `README.md` — final index with actual per-document commit SHAs and status.

## Planning risks

- Paper prose, SysML abstract syntax, examples, and validation artefacts may use
  different terms or strength of obligation.
- The Notebook-to-OSL paper may demonstrate a research method without defining
  production lifecycle, authorization, or storage requirements.
- Existing MSH operator-strategy names may conceal materially different domain
  semantics or lifecycle states.
- Existing mutable files or projections may not preserve immutable evidence,
  version lineage, and reviewer attribution.
- MSH federation and capability authority could be accidentally confused with
  OSL approval; the plan must fence these concepts explicitly.
- A serializer-first design could make SysML v2 or JSON accidental sources of
  truth and lose language semantics.
- AI-assisted extraction can introduce prompt-injection, cross-tenant leakage,
  fabricated evidence links, overconfidence, and implicit authority.
- Cross-platform persistence, path, encoding, and time behavior may differ on
  Windows and Linux.

## Questions requiring paper or architecture clarification

- `requires-research-clarification`: Which OSL artefacts and well-formedness
  rules are normative when paper prose, `osl-core.sysml`, semantic projections,
  examples, or evaluation tables disagree?
- `requires-research-clarification`: Is OSL v0.1 the first supported external
  language identifier, or should MSH distinguish paper artefact version from
  its own compatibility profile?
- `requires-research-clarification`: What exact act changes a candidate from the
  elicitation method into an OSL source fragment, and whose review is required?
- `requires-research-clarification`: Does "validated" mean only content
  commitments plus intended domain review in every paper artefact, and should
  MSH avoid that overloaded label in favor of distinct structural and review
  states?
- `requires-research-clarification`: Are fragment identity, composition,
  supersession, and version compatibility defined strongly enough for durable
  product contracts?
- `requires-research-clarification`: Which existing MSH records are genuine
  evidence candidates versus operational telemetry or UI projections that must
  remain only externally referenced?
- `requires-research-clarification`: Is publication an MSH-local canonical state,
  a federation-visible projection, or a future governance concern?
- `requires-research-clarification`: Should SysML v2 import be supported in the
  first compatibility profile, or export only until round-trip semantics are
  demonstrated?

## Completion discipline

Only documentation under `docs/implementation/osl_integration/` may change.
Every bounded document receives its own commit. The final checks must prove a
docs-only diff from `main`, a clean worktree, the expected chronological commit
series, and successful push of the requested branch without merge or production
implementation.
