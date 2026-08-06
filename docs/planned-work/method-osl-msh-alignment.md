# Planned work: Notebook-to-OSL vertical slice in MSH

Programme plan: `Nettking/phd-research/plans/method-osl-msh-alignment.md`  
Workstream: W3  
Status: planning may begin; semantic implementation depends on W1 and W2 contracts  
Date opened: 2026-08-02

## Mission

Implement one complete, intuitive, traceable path from a raw operator statement to a conformant current OSL/SysML export.

MSH must support both:

1. the Notebook-to-OSL method owned by `paper-repo`; and
2. the OSL language owned by `systems-paper`.

MSH must not silently redefine either one.

## Confirmed problem

The current implementation has three central gaps:

- the record lifecycle is shortened to `captured -> structured -> reusable` and lacks explicit clarification and validation evidence;
- the internal record is flatter than the current OSL strategy-path and relation structure;
- the exporter and its tests target an obsolete OSL package/keyword style.

## Start condition

The agent may immediately:

- inspect current records, UI, exporter, tests, migrations, and documentation;
- design adapters and a neutral model behind new files;
- prepare migration and fixture plans;
- identify exact incompatibilities.

The agent must not finalize method statuses, required fields, or OSL mapping by guessing. Final semantic implementation must use the approved or clearly identified draft W1 and W2 contracts.

## Required vertical slice

```text
raw statement
  -> method-conformant stored note
  -> annotation/clarification/validation or explicit provisional state
  -> neutral MSH strategy and strategy path
  -> intuitive human-facing structuring UI
  -> current OSL model
  -> pinned SysML v2 conformance validation
  -> trace manifest
```

## Internal model requirements

Do not use generated SysML text as the primary data model. Introduce or adapt a neutral representation capable of storing:

- strategy identity and version;
- one or more strategy paths;
- situation;
- observations and triggers;
- contexts;
- hypotheses;
- goals;
- decisions;
- candidate actions;
- selected actions;
- expected outcomes;
- rationale;
- evidence, evidence status, confidence, and source references;
- risks and trade-offs;
- open questions;
- review needs;
- validation needs and outcomes;
- downstream artifact references;
- explicit semantic relations;
- source-note, annotation, validation, contract-version, and export provenance.

## UI requirements

The UI must remain understandable to a researcher or engineer who does not write SysML.

Recommended user-facing grouping:

### What happened?

- situation;
- observation;
- trigger;
- context.

### What might it mean?

- hypothesis or interpretation;
- uncertainty;
- alternative interpretation;
- open questions.

### What was being decided?

- goal;
- decision;
- actions considered;
- actions selected;
- rationale.

### What should happen?

- expected outcome;
- evidence;
- risk;
- trade-off.

### What still needs checking?

- clarification;
- operator/domain-expert validation;
- confidentiality;
- model readiness;
- downstream trace target.

The distinction between actions considered and actions selected must be visible and intuitive.

## Export requirements

Replace the obsolete exporter with output based on the W2 contract. At minimum the final output must use:

- `OSLCore`;
- `OSLKeywords`;
- `#osl_strategy`;
- `#osl_path`;
- current `#osl_*` element keywords;
- explicit typed relations;
- `#osl_considers` for candidate actions;
- `#osl_selects` for selected actions;
- evidence, confidence, review/validation needs, open questions, and traces where present.

Do not claim that valid SysML means the operator strategy is correct, safe, or approved.

## Provisional and reviewed boundaries

MSH should support two clearly different outputs:

### Provisional path

- researcher-interpreted or incomplete;
- missing information represented explicitly;
- open questions or review needs retained;
- never labelled operator-validated;
- unavailable for automatic runtime authority.

### Reviewed/model-ready path

- required method-contract evidence exists;
- validation outcome and provenance are recorded;
- confidentiality permits the intended export;
- eligible for the stronger reviewed OSL contract.

`reusable` may remain temporarily for migration/UI compatibility but must not silently mean validated, model-ready, export-authoritative, or runtime-approved.

## Migration requirements

- Existing v3 records must remain readable.
- Migration must preserve raw statements and identifiers.
- Existing `structured` or `reusable` records must not be upgraded to validated/model-ready without evidence.
- Unknown provenance must remain unknown.
- Deletion or replacement must not erase research history silently.
- The migration and new schema need focused Linux and Windows tests.

## Conformance requirements

String assertions alone are insufficient.

The test must:

1. generate an OSL/SysML file from an MSH fixture;
2. load the exact W2 OSL files in the declared order;
3. load the generated file in the pinned environment;
4. fail on unexpected parser/validation diagnostics;
5. verify candidate and selected action relations;
6. verify source and version provenance;
7. retain the generated fixture or diagnostic output when useful.

## Runtime and AI boundaries

- A model-ready OSL path is not automatically a runtime recommendation.
- Candidate support artifacts require a separate policy/review step.
- AI may propose annotations or mappings, but cannot validate a fragment, select an action authoritatively, or grant runtime authority.
- Federation, storage authority, benchmarking, provider onboarding, and unrelated Flask navigation are outside this workstream.

## Acceptance criteria

- One representative CNC statement can be captured, structured, and exported through the UI.
- The stored record conforms to the W1 contract.
- The generated model conforms to the W2 contract.
- The model validates in the pinned Systems-paper environment with zero unexpected diagnostics.
- Candidate and selected actions remain distinct.
- Provisional incompleteness is explicit.
- Reviewed/model-ready output requires method evidence.
- Existing records migrate without claim inflation.
- Every exported artifact records the source note, record/schema version, method contract, OSL contract, exporter version, and output identity.
- Focused tests pass on Linux and Windows.

## Files this agent may change

- operator-knowledge schemas, services, adapters, and migrations;
- focused capture/review/structure UI needed by the vertical slice;
- OSL exporter and conformance adapter;
- focused tests and fixtures;
- documentation for this research workflow.

Prefer new files and bounded adapters where possible.

## Files and areas this agent must not change

- federation, capability-first onboarding, storage authority, provider runtime, or benchmark semantics;
- scientific definitions in `paper-repo` or `systems-paper`;
- unrelated Flask navigation/templates/setup unless strictly required by the vertical slice and explicitly justified;
- tool-paper results;
- runtime recommendation authority beyond clear gating and claim-safe labels.

## Handoff to W0 and W4

The completed PR must report:

- exact W1 and W2 contract versions;
- MSH schema and migration version;
- exact MSH commit;
- conformance environment and results;
- retained example/trace manifest;
- unsupported method and OSL features;
- claim and runtime boundaries;
- whether the baseline is ready to freeze for evaluation.

## Agent prompt

> Work on `Nettking/msh`, branch from updated `main`, and read `docs/planned-work/method-osl-msh-alignment.md`. Implement only W3, using the approved or clearly identified draft W1 Notebook-to-OSL contract and W2 OSL integration contract. Deliver one vertical slice from raw statement through an intuitive neutral strategy-path UI/model to current `#osl_*` SysML with typed relations and pinned conformance validation. Preserve provisional versus reviewed/model-ready status, provenance, safe migration, and runtime authority boundaries. Do not change federation or redefine the papers. Add focused Linux and Windows tests, open a draft PR, and do not merge.
