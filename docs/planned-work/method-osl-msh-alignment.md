# W3 acceptance scenario: Notebook-to-OSL in MSH

Programme plan: `Nettking/phd-research/plans/method-osl-msh-alignment.md`  
Workstream: W3  
Status: **acceptance scenario; implementation is governed by the OSL execution plan**  
Date opened: 2026-08-02  
Reconciled: 2026-08-06

Authoritative MSH implementation sequence:
`docs/implementation/osl_integration/10_phased_implementation_roadmap.md`.

This file defines the end-to-end behavior W3 must eventually demonstrate. It is
not a second implementation plan, does not authorize a single vertical-slice
PR, and does not override delivery dependencies or security gates.

## Mission

Demonstrate one intuitive, traceable path from an unchanged operator source to a
versioned OSL representation and, when the declared adapter is supported, a
conformant SysML v2 projection.

MSH must consume rather than redefine:

1. the Notebook-to-OSL method owned by `paper-repo`; and
2. the OSL language/profile owned by `systems-paper` and the accepted MSH
   compatibility profile.

## Problem demonstrated by the scenario

The legacy MSH path is not sufficient evidence of OSL conformance:

- `captured -> structured -> reusable` does not preserve explicit clarification,
  validation, review, approval, or publication evidence;
- the current operator record is flatter than the planned strategy-path and
  typed-relation structure;
- the legacy exporter and string-oriented tests target older OSL/SysML naming
  and do not prove conformance to the selected profile;
- mutable records and generated text cannot substitute for immutable source,
  revisions, provenance, and exact human decisions.

These findings justify the scenario. They do not authorize implementation to
bypass the accepted profile, persistence, authority, security, or migration
sequence.

## Required end-to-end observation

```text
unchanged source artefact
  -> exact source excerpt
  -> attributed annotation and clarification
  -> explicit provisional gaps or validation needs
  -> candidate strategy content
  -> immutable neutral OSL strategy-fragment revision
  -> deterministic profile validation
  -> exact-revision human review and model-readiness evidence
  -> authorized product lifecycle decision where applicable
  -> intuitive human-facing projection
  -> canonical JSON bundle
  -> optional declared SysML v2 projection
  -> retained trace manifest and diagnostics
```

## Semantic requirements

The accepted representation must be able to preserve, where present:

- strategy identity, profile, and revision;
- one or more strategy paths;
- situation, observations, triggers, and contexts;
- hypotheses or interpretations and uncertainty;
- goals and decisions;
- candidate actions and selected actions as distinct concepts;
- expected outcomes;
- rationale;
- evidence references and independent evidence dimensions;
- confidence without treating confidence as verified truth;
- risks and trade-offs;
- open questions, gaps, review needs, and validation needs;
- downstream trace targets;
- explicit typed relations;
- source, annotation, validation, review, contract, tool, and export provenance.

A Decision is not an OperatorAction. A represented selected action is not a
recommendation, approval, authorization, or execution.

## User-facing requirements

The workflow must remain understandable to a researcher or engineer who does
not write SysML. A suitable projection may group information as:

### What happened?

- situation;
- observation;
- trigger;
- context.

### What might it mean?

- hypothesis or interpretation;
- uncertainty;
- alternatives;
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
- domain review;
- evidence review;
- confidentiality;
- model readiness;
- downstream trace target.

The UI must preserve the distinction among source, candidate, draft, validation,
review, approval, publication, and export. UI state is not canonical state.

## Provisional and reviewed boundaries

### Provisional output

- may be researcher-interpreted or incomplete;
- records missing information and uncertainty explicitly;
- retains open questions and review/validation needs;
- is never labelled operator-validated without exact evidence;
- grants no runtime or resource authority.

### Reviewed or model-ready evidence

- binds the required method and review evidence to an exact unchanged revision;
- records reviewer, scope, outcome, time, and provenance;
- records the relevant structural validation result separately;
- respects confidentiality and intended audience;
- does not imply approval, publication, safety, operational validation, or
  execution unless a separate exact product record states that decision.

Legacy `structured`, `reusable`, `validated`, or `model-ready` labels must not be
silently promoted to canonical review, approval, publication, or authority.

## Export requirements

Canonical JSON is the first required representation.

A later SysML v2 export, when supported, must:

- name the exact OSL profile and exporter version;
- use the declared current OSL packages and element/relation mapping;
- preserve candidate versus selected action relations;
- include supported evidence, confidence, gap, review/validation-need, and trace
  information;
- emit explicit diagnostics for unsupported semantics instead of guessing;
- be deterministic and pass the pinned parser/toolchain checks;
- leave canonical state and lifecycle unchanged.

Parser acceptance alone does not prove semantic equivalence, domain
correctness, safety, evidence acceptance, review, or approval.

## Migration requirements

- Existing operator records remain readable during the supported compatibility
  window.
- Migration preserves original bytes or exact source representation, IDs where
  valid, and provenance.
- Derived mappings are candidates with field-level source trace and findings.
- Unknown provenance remains unknown.
- Migration does not overwrite or delete research history.
- Dry run, replay, restart, reconciliation, and rollback are explicit.
- Focused migration behavior is tested on Ubuntu and Windows.

## Runtime, AI, and authority boundaries

- W3 is design-time knowledge transformation, not machine control.
- A model-ready OSL fragment is not automatically a runtime recommendation.
- AI may propose attributed annotations, structures, mappings, or explanations
  only after the complete manual path exists.
- AI cannot validate meaning, sign review, approve, publish, select an action
  authoritatively, or grant runtime/resource authority.
- OSL content cannot grant Federation membership, provider activation, compute,
  storage, job, artifact, lease, fencing, or machine authority.
- Federation, capability-first onboarding, benchmark semantics, provider
  runtime, and storage authority remain outside W3.

## Delivery mapping

W3 becomes executable incrementally through the authoritative OSL deliveries:

| W3 capability | Required delivery |
| --- | --- |
| accepted semantic/profile boundary | D0-A |
| immutable source/evidence/strategy contracts | D1-A and D1-B |
| canonical JSON and deterministic validation | D2-A and D2-B |
| immutable storage, revisions, provenance, and audit | D3-A and D3-B |
| lifecycle and safe read projections | D4-A |
| manual capture, excerpt, annotation, clarification, candidate, and draft | D5-A and D5-B |
| exact human review, approval, publication, and feedback | D6-A and D6-B |
| authenticated read and mutation UI | D7-P, D7-A, and D7-B |
| optional candidate-only AI | D8-A |
| canonical bundle, SysML projection, and legacy migration | D9-A, D9-B, and D9-C |
| cross-platform hardening, recovery, cutover, and retained evidence | D10-A |

No agent should be instructed to implement all of W3 in one branch or PR.

## Final acceptance criteria

W3 is accepted only when one representative CNC source can be exercised through
the supported product path and the evidence demonstrates that:

- the original source is unchanged and traceable;
- excerpt and derived content have exact provenance;
- provisional incompleteness remains explicit;
- candidate and selected actions remain distinct;
- the stored canonical revision conforms to the accepted MSH profile contract;
- deterministic validation produces no unexpected findings for the supported
  scenario;
- human review/model-readiness evidence is exact and cannot be forged by the
  client or AI;
- any approval/publication record is distinct from structural validation and
  grants no runtime authority;
- the UI remains understandable without requiring SysML knowledge;
- canonical JSON is deterministic;
- the optional SysML output, when included, passes the pinned declared
  conformance environment with no unexpected diagnostics;
- migration does not inflate legacy claims;
- the trace manifest records source, record/schema, method, profile, validator,
  review, exporter, output, and exact MSH versions;
- focused and permanent tests pass on Ubuntu and Windows;
- retained evidence distinguishes fixtures, automated tests, real browser/tool
  observations, and human review;
- no private source, credentials, endpoints, identities, local paths, or
  unrelated authority leak.

## Handoff to the research programme

The final W3 evidence must report:

- exact W1 method contract and W2/OSL profile versions;
- exact source repository commits;
- MSH schema, contract, validator, migration, and exporter versions;
- exact MSH commit and supported deployment topology;
- conformance environment and diagnostics;
- retained example and trace manifest;
- unsupported method, profile, UI, migration, and export features;
- claim, privacy, AI, and runtime-authority boundaries;
- whether the baseline is sufficiently stable to freeze for evaluation.

## Current action

Do not start a W3 implementation branch. Complete D0-A from the authoritative
OSL execution plan first.
