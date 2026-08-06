# OSL language requirements

Status: source-derived implementation requirements; no language runtime or
product contract is implemented by this document.

Analyzed source: `Nettking/systems-paper` at commit
`ff098ce52f15b489b6a07d5b55c6c788d862e3be`. Line references in this document
refer to that immutable snapshot.

This document extracts the OSL language boundary before it is mapped onto MSH.
It deliberately distinguishes the research language, its current SysML v2
realization, and additions that MSH would need for durable product use.

## Classification and source precedence

The marker meanings are defined in `00_scope_and_sources.md`. In this document:

1. `paper-defined`: `evaluation/osl-semantic-contract.md` sections 1--16 is the
   intended semantic contract and defines the force of `shall`, `should`, `may`,
   and `shall not` (lines 8--21).
2. `paper-defined`: `sysml/osl-core.sysml` is the authoritative but preliminary
   implemented abstract syntax (lines 1--15).
3. `paper-defined`: `evaluation/osl-final-reassessment.md` records which parts of
   the wider semantic contract are actually present in the bounded v0.1
   research profile (lines 35--92).
4. `paper-defined`: the current manuscript presentation is in
   `tex/sections/osl_core_definition.tex`, `osl_capabilities.tex`, and
   `osl_workflow_roles.tex`.
5. `paper-defined` examples, projections, corpus tables, and validation
   manifests demonstrate or test selected behavior; they do not override the
   abstract syntax.
6. `requires-research-clarification`: where those sources disagree, MSH must not
   invent a merged normative rule. It must select and name a compatibility
   profile, retain the source commit, and record the deviation.

`publication-readiness.md` lines 1--6 and
`evaluation/osl-final-reassessment.md` lines 14--16 label the artefact
`OSL v0.1-alpha / research prototype`. That label supports a language-engineering
and reproducibility claim, not product maturity, operational validity, or a
stable external compatibility promise.

## Purpose and claim boundary

- `paper-defined`: OSL preserves context-dependent operator-strategy knowledge
  as a connected, typed, design-time reasoning structure. It is not a bag of
  labels and must not reduce knowledge to isolated symptom/action rules
  (`evaluation/osl-semantic-contract.md` sections 1--2, lines 8--42).
- `paper-defined`: where source material supplies the information, a model must
  make inspectable the situation, observable basis, interpretation, context,
  goals, decisions, candidate and selected responses, action structure,
  expected consequences, rationale, evidence, uncertainty, risks, trade-offs,
  gaps, review obligations, and downstream trace needs (same source,
  lines 23--42).
- `paper-defined`: the systems paper begins after an external elicitation method
  has produced candidate strategy fragments
  (`tex/sections/osl_workflow_roles.tex` lines 1--5). The Notebook-to-OSL
  capture method is an upstream input, not part of the OSL core.
- `paper-defined`: OSL supports design-time representation, checking,
  comparison, specialization, provenance-bearing composition, explicit gap and
  conflict inspection, optional trace derivation, and source-bound secondary
  views (`tex/sections/osl_capabilities.tex` lines 17--40).
- `paper-defined`: OSL does not define runtime path activation, automatic
  strategy selection, machine control, command execution, executable workflow
  semantics, scheduling, ranking, automatic conflict resolution, factual truth,
  causal proof, operational correctness, safety assurance, stakeholder
  agreement, or automatic Digital Twin implementation
  (`evaluation/osl-semantic-contract.md` section 13, lines 468--489).
- `proposed-for-MSH`: every user-visible conformance result must repeat the
  relevant boundary: structural/semantic conformance is not evidence truth,
  domain approval, operational validation, or execution permission.

## Abstract syntax

### Aggregate and path

| Construct | `paper-defined` meaning | Cardinality or distinction | Source |
| --- | --- | --- | --- |
| `OperatorStrategy` | Container for related, alternative, or composite strategy paths; containment does not imply simultaneous applicability or preference. | Contains `1..* StrategyPath`. | `evaluation/osl-semantic-contract.md` 3.1, lines 46--50; `sysml/osl-core.sysml` lines 1049--1051 |
| `StrategyPath` | Principal unit: a connected, typed reasoning graph with path-local identity. | Exactly one primary situation; zero or more typed elements and relations; observable basis required. | Contract 3.2--3.3, lines 52--75; core lines 922--982 |
| `ProvisionalStrategyPath` | An intentionally incomplete path that preserves observable basis and a target-specific unresolved obligation. | Current core requires an observation or trigger and an `OpenQuestion` or `ReviewNeed`. | Contract 6.1, lines 263--266; core lines 984--996 |
| `StructurallyCompleteStrategyPath` | A connected path with response-related content and compatible consequence semantics. | At least one decision, action, and typed consequence; external semantic checks remain necessary. | Contract 6.2, lines 267--271; core lines 998--1015 |
| `DomainReviewedStrategyPath` | A structurally complete path with an explicit represented domain review. | At least one `ReviewRecord`; not operational validation. | Contract 6.3--6.4, lines 273--288; core lines 1017--1027 |

`paper-defined`: containment answers which elements belong to a path; typed
relations answer how those elements participate. Non-empty collections do not
establish a reasoning route (`evaluation/osl-semantic-contract.md` lines 54--75).

### Reasoning and qualification elements

| Construct family | `paper-defined` semantics | Critical separation | Source |
| --- | --- | --- | --- |
| `Situation` | Primary setting in which a path may apply. | A label alone is not a complete applicability model. | Contract 4.1, lines 79--85 |
| `Observation` | Something observed, measured, reported, inspected, or retrieved. | Not a trigger, interpretation, verified fact, or evidence record. | Contract 4.2, lines 87--93 |
| `Trigger` | Attention condition derived from observations, events, thresholds, or context. | May be based on an observation but must retain its different role. | Contract 4.2; core lines 273--286 |
| `Context` | Qualifies applicability, interpretation, a decision, action, or criterion. | Not runtime activation state. | Core lines 243--271 |
| `Hypothesis` | Possible explanation, diagnosis, interpretation, or assumption. | Never implies factual truth. Competing hypotheses are allowed. | Contract 4.3, lines 95--101 |
| `StrategyGoal` | Desired priority or value that motivates a decision. | Different from a post-response consequence. | Contract 4.4, lines 103--109 |
| `SelectionCriterion` and specializations | Discriminates paths, options, continuation, escalation, or verification. | Missing criterion is an explicit targeted gap, not an inferred default. | Contract 4.9, lines 172--178; core lines 179--184 |
| `StrategyDecision` | Represented judgement, choice structure, or commitment point. | A decision is not an `OperatorAction`. | Contract 4.5, lines 111--117; core lines 194--209 |
| `OperatorAction` and role specializations | Descriptive response with an explicit or inferable semantic role. | Representation is not recommendation, authorization, or execution. | Contract 4.7, lines 127--156; core lines 196--209 |
| `ExpectedConsequence` family | Prospective operational outcome or diagnostic, monitoring, verification, or organisational information result. | Not a measured actual outcome or feedback event. | Contract 4.8, lines 158--170; core lines 186--192 |
| `Rationale` | Reason that can justify a hypothesis, goal, decision, action, or criterion. | Not restricted to decision justification. | Contract 4.10, lines 180--195 |
| `Risk` | Possible unwanted exposure with a source and affected target. | Mere containment is not a represented risk relation. | Contract 4.11, lines 197--201 |
| `TradeOff` | Tension among at least two goals, actions, or consequences. | No universal numerical trade-off function is defined. | Contract 4.12, lines 203--207 |
| `OpenQuestion`, `ReviewNeed`, `ValidationNeed` | Distinct missing-information or follow-up obligations. | Each must target affected semantics and state its blocking impact. | Contract 4.13, lines 209--217; core lines 136--151 |
| `Evidence` | Epistemic qualification with independent derivation, source, review, confidence, provenance, assessor, and scope dimensions. | Evidence records a represented basis; it is not proof. | Contract section 5, lines 219--257; core lines 158--173 |
| `ReviewRecord` | A represented human/domain review of a path/version. | Separate from evidence review, product approval, and operational validation. | Core lines 145--151, 673--676 |
| `Conflict` | Explicitly retained incompatibility with reviewable status. | Composition does not resolve it automatically. | Core lines 153--156, 725--765 |
| `DownstreamArtifactReference` | Typed design-time reference to a candidate engineering artefact. | A trace does not claim that the artefact exists or is implemented. | Core lines 211--232; contract section 8 |

### Decision, response, recommendation, and result

These distinctions are non-negotiable for MSH:

- `paper-defined`: `DecisionOption` connects a decision to a considered
  candidate action; `ActionSelection` connects it to a represented selected
  action (`sysml/osl-core.sysml` lines 312--321).
- `paper-defined`: candidate and selected semantics remain distinct. Selection
  is descriptive and implies neither runtime recommendation nor execution
  (`evaluation/osl-semantic-contract.md` lines 119--125).
- `paper-defined`: the core contains `RecommendationArtifactReference` as an
  optional downstream target, but no canonical `Recommendation` entity or
  recommendation authority (`sysml/osl-core.sysml` lines 215--232).
- `paper-defined`: the consequence types are prospective design-time
  requirements. The language contains no actual-result or feedback-event type.
- `proposed-for-MSH`: a product recommendation, human approval, authorized
  operational action, recorded action occurrence, and observed result must be
  separate contracts outside the OSL path's descriptive selection relation.
- `proposed-for-MSH`: actual outcome/feedback may later become new, explicitly
  sourced evidence for a new fragment revision. It must never rewrite the
  earlier expected consequence.

## Relation semantics

The core defines typed relations with named, cardinality-one ends. A canonical
MSH model must preserve relation identity, type, direction/roles, and endpoints;
flattening them into fields loses language meaning.

| Relation family | Representative definitions | `paper-defined` requirement | Core lines |
| --- | --- | --- | --- |
| Applicability and context | `SituationAppliesToPath`, `ContextQualifies*`, `ApplicabilityCriterionQualifiesPath` | State what makes a path or interpretation relevant without claiming runtime activation. | 238--271 |
| Observation and interpretation | `ObservationIndication`, `ObservationContradiction`, `TriggerBasis`, `HypothesisBasis` | Preserve support, contradiction, and the route into a decision. | 273--291 |
| Goals and decisions | `GoalMotivation`, `GoalPriority` | Keep priorities and competing goals explicit. | 293--301 |
| Candidate and selection | `DecisionOption`, `ActionSelection`, criterion relations | Candidate is not selected; a selected action is not executed. | 312--341 |
| Descriptive action structure | `ActionPrecedence`, `ActionEnabling`, `ActionDependency`, `ActionCondition` | Express ordering and dependency without executable control flow. | 347--365 |
| Action/consequence | Five `ActionProduces*` relations plus information, verification, mitigation, correction, and escalation | Action roles must be compatible with the kind of consequence or information produced. | 367--415 |
| Evidence targeting | Twelve element-specific support relations plus path-level `EvidenceSupport` | Evidence must identify what it qualifies. Prefer element-level targeting when known. | 430--499 |
| Rationale/risk/trade-off | Justification, source/target, and pairwise comparison relations | Qualification elements must be connected, not loose annotations. | 505--576 |
| Gaps, review, validation | Target-specific question, review, review-record, and validation relations | Missing or disputed semantics remain attached and inspectable. | 578--706 |
| Alternative/composition/conflict | `PathAlternative`, `PathComposition`, seven conflict relations | Preserve separate identity, provenance, participation, and known incompatibility. | 712--765 |
| Downstream trace | Path-level plus observation, trigger, context, goal, decision, action, consequence, rationale, risk, question, and evidence traces | Use granular source identity where known; trace is not implementation. | 771--829 |

`paper-defined`: specialization, alternatives, and composition are not
interchangeable. Specialization inherits and deliberately redefines semantics;
alternatives retain separate identities for comparison; composition relates
independent source paths to a composite while preserving provenance and
conflicts (`evaluation/osl-semantic-contract.md` section 7, lines 290--331).

## Semantic well-formedness

The first MSH profile must assign durable rule identifiers matching these
commitments rather than treating parser acceptance as sufficient.

| Rule | `paper-defined` commitment | Planned enforcement |
| --- | --- | --- |
| WF1 | An `OperatorStrategy` contains at least one path. | Model cardinality and semantic validator. |
| WF2 | A path contains exactly one primary situation and may have applicability/context qualifiers. | Model cardinality; warn when a bare situation supplies no usable applicability relation. |
| WF3 | Every non-empty path has at least one observation or trigger. | Model and validator. |
| WF4 | At least one connected route runs from observable basis to a decision, response, consequence, or target-specific gap. | Graph validator; no collection-count shortcut. |
| WF5 | Missing content is represented by a gap attached to the affected element, relation, criterion, or path commitment. | Target reference and blocking-impact validator. |
| WF6 | Each structurally complete decision connects to at least one candidate or selected response. | Relation and maturity validator. |
| WF7 | Candidate and selected relations have typed decision/response endpoints and retain different meanings. | Relation-type and endpoint validator. |
| WF8 | Each concrete operator action has a declared or inferable role; consequence relations are role-compatible. | Action-role compatibility table. |
| WF9 | An action connects only to a consequence it can produce, enable, assess, verify, mitigate, or inform. | Relation/consequence compatibility validator. |
| WF10 | Evidence identifies its qualified target and provenance/source basis where available. | Target and provenance findings; never infer truth. |
| WF11 | Risk identifies source and affected target; trade-off identifies at least two competing elements. | Connectivity/cardinality validator. |
| WF12 | A domain-reviewed path includes a review record and exposes unresolved blocking issues. | Review-scope/disposition and blocker validator. |
| WF13 | A trace identifies source and typed target; known element sources are not hidden in ambiguous path-only traces. | Trace granularity validator. |
| WF14 | Composition preserves source identity/provenance and known conflicts. | Composition and conflict-preservation validator. |
| WF15 | Path-internal reasoning relations do not leak between unrelated paths. | Ownership/inheritance/reference locality validator. |

Source: `evaluation/osl-semantic-contract.md` section 10, lines 364--426;
manuscript summary in `tex/sections/osl_core_definition.tex` lines 91--103.

### Validation meaning and layers

- `paper-defined`: native SysML multiplicities and constraints express only the
  subset consistently supported by the pinned Pilot; connectedness and
  compatibility use a separate source-profile validator
  (`sysml/osl-core.sysml` lines 831--927).
- `paper-defined`: the current validator checks a bounded repository profile,
  not arbitrary SysML v2, factual truth, causality, safe operation, or universal
  tool equivalence (`evaluation/osl-semantic-validation.md` lines 7--32 and
  81--102).
- `paper-defined`: graph connectivity is effectively undirected in the current
  validation implementation; relation definitions and named endpoints carry
  the semantic direction. Passing connectivity is not causal proof
  (`scripts/validate_osl_semantics.py` lines 809--833).
- `paper-defined`: final reassessment treats WF1--WF15 as implemented in the
  bounded profile, while several negative witnesses, target matrices, and
  composition semantics remain partial
  (`evaluation/osl-final-reassessment.md` lines 60--92).
- `proposed-for-MSH`: a `ValidationResult` is immutable and includes language
  profile/version, validator name/version, rule ID, severity, message, affected
  element/relation IDs, input revision/hash, timestamp, and deterministic
  result hash.
- `proposed-for-MSH`: validation has at least syntax/contract, reference,
  structural, semantic, lifecycle-precondition, and export-compatibility
  categories. Only the first four can contribute to OSL structural maturity.
  None verifies evidence truth or grants approval.

## Evidence, provenance, and uncertainty

`paper-defined` evidence dimensions are independent:

- derivation: `direct`, `inferred`, `reconstructed`, `assumed`, `unknown`;
- source kind: operator statement, observation, telemetry, document,
  literature, experiment, analysis source, model output, organisational record,
  other, or unknown;
- evidence review: `unreviewed`, `reviewed`, `accepted`, `rejected`,
  `disputed`, `superseded`, or `unknown`;
- confidence: `low`, `medium`, `high`, or `unknown`;
- source/provenance references;
- assessor/originator and qualification scope.

Sources: `evaluation/osl-semantic-contract.md` lines 219--257 and
`sysml/osl-core.sysml` lines 25--173.

Consequences for MSH:

- `paper-defined`: the dimensions can coexist. An inferred, document-backed,
  engineer-reviewed hypothesis may remain disputed by an operator.
- `paper-defined`: evidence review status is neither fragment lifecycle nor
  approval authority.
- `paper-defined`: `EvidenceStatus` and `evidence_status` collapse independent
  dimensions and are explicitly legacy compatibility surfaces
  (`sysml/osl-core.sysml` lines 105--121 and 171--173).
- `requires-research-clarification`: the semantic contract permits qualitative
  or quantitative confidence with an identified basis, but the core implements
  only a four-value enum and has no structured confidence-basis field.
- `requires-research-clarification`: the core's `source_refs`, `assessor_ref`,
  and `scope_note` are free strings; they do not define immutable evidence
  identity or chain of custody.
- `proposed-for-MSH`: source artefacts are immutable. Evidence references use a
  stable artefact/revision ID, excerpt/span and content hash where applicable,
  capture actor/time, classification/consent scope, and provenance events.
- `proposed-for-MSH`: an AI output uses `source_kind=model_output` and carries
  model/run/prompt/input/output hashes. It cannot set evidence review to
  `accepted` or convert a claim into verified fact.

## Identity and references

### What the paper establishes

- `paper-defined`: a strategy path has path-local identity; alternative paths
  keep separate identity (`evaluation/osl-semantic-contract.md` lines 52--58
  and 298--309).
- `paper-defined`: composition relates independently identified source paths to
  a composite and preserves source identity and provenance (lines 311--331).
- `paper-defined`: element-level traces preserve known source-element identity
  and connect it to a typed downstream reference (section 8, lines 333--352).
- `paper-defined`: downstream references provide a typed class and optional
  `target_uri`; evidence provides string `source_refs`; review records a string
  `reviewed_version` (`sysml/osl-core.sysml` lines 145--173 and 215--232).

### What remains undefined

The following are all `requires-research-clarification`:

- stable identifier syntax and namespace;
- logical fragment identity versus immutable revision identity;
- identity of contained elements and relation usages outside a SysML model;
- whether display or qualified SysML names may change;
- external-reference resolution and integrity policy;
- canonical hashes and equality rules;
- revision lineage, branching, merge, supersession, and deprecation;
- language-version declaration in an instance.

### Product requirement

`proposed-for-MSH`:

- allocate opaque, immutable IDs for source artefacts, excerpts, fragments,
  fragment revisions, contained nodes, relation usages, reviews, approvals,
  validation results, and exports;
- keep a stable logical fragment ID separate from every immutable revision ID;
- store parent/base revision and explicit `supersedes` relations;
- use human-readable names as labels, never sole identity;
- treat external references as typed objects with resolver namespace, target ID
  or URI, optional expected version/hash, access scope, and resolution state;
- preserve IDs through supported round trips, including unknown extensions.

## Language versioning

`requires-research-clarification`: `v0.1-alpha` is a paper/repository maturity
label. The paper defines no machine-readable language-version registry,
compatibility policy, migration algorithm, canonical serialization version, or
extension negotiation.

`proposed-for-MSH` requirements:

1. Freeze the first supported profile to the analyzed systems-paper commit.
2. Record four distinct values:

   - OSL language/profile identifier;
   - systems-paper source commit;
   - canonical MSH contract/serialization version;
   - producer and validator implementation versions.

3. Give the profile a provisional internal identifier until the research owner
   approves a public version string. Do not imply that paper label `v0.1-alpha`
   already guarantees MSH compatibility.
4. Reject unknown major profiles for validation or publication. Permit
   read-only inspection and lossless quarantine where safe.
5. Preserve recognized and policy-allowed unknown extensions during
   decode/encode; never silently reinterpret them.
6. Make migrations explicit functions from one immutable revision to a new
   revision, with findings and provenance. Never mutate the old revision.
7. Version serializer formats independently of language semantics so that a
   JSON encoding change does not become a language change.

The first profile must state whether it implements the whole aspirational
semantic contract or the bounded implemented v0.1 profile. The current evidence
supports only the latter plus explicitly documented MSH extensions.

## Maturity and lifecycle

### Paper-defined language maturity

| Maturity | Entry meaning | What it does not mean |
| --- | --- | --- |
| Provisional | Observable basis plus a connected partial route or targeted unresolved gap. | Reviewed, complete, true, safe, or actionable. |
| Structurally complete | WF commitments, connected reasoning, response semantics, consequence typing, and evidence/gap rules pass for the selected profile. | Correct, accepted, safe, operationally validated, approved, or published. |
| Domain reviewed | Structurally complete plus an explicit domain-review record. | Operational validation, safety approval, recommendation authority, or execution permission. |

Source: `evaluation/osl-semantic-contract.md` section 6, lines 259--288.

### MSH workflow lifecycle is separate

`proposed-for-MSH` lifecycle states such as `extracted_candidate`, `draft`,
`in_review`, `reviewed`, `approved`, `published`, `rejected`, `deprecated`, and
`superseded` are product workflow states, not paper-defined OSL constructs.
They are orthogonal to language maturity:

- a draft can be structurally complete but not reviewed;
- a reviewed revision can have a correction/rejection disposition;
- approval targets one immutable revision and does not alter language maturity;
- publication exposes an approved revision but grants no operational authority;
- deprecation/supersession preserves every historical revision and review.

`requires-research-clarification`: the paper does not define product approval,
publication, deprecation, supersession, withdrawal, or operational binding.
Operational binding must not be added as a path-maturity value.

## Human review, approval, and authority

- `paper-defined`: the primary author is an OSL modeller or DT engineer.
  Operators/domain experts review whether interpretations, decisions, actions,
  and missing-information statements preserve intended meaning; they are not
  expected to author SysML v2
  (`tex/sections/osl_workflow_roles.tex` lines 5--22).
- `paper-defined`: domain review is represented by reviewer, reviewed version,
  date, disposition, and note, with a relation to the path
  (`sysml/osl-core.sysml` lines 145--151 and 673--676).
- `requires-research-clarification`: the semantic contract expects review scope,
  stakeholder role, unresolved blockers, and element-level
  accepted/disputed/rejected dispositions that the current core does not fully
  model (`evaluation/osl-semantic-contract.md` lines 273--284;
  `evaluation/osl-final-reassessment.md` lines 62--69).
- `requires-research-clarification`: the workflow table says “reviewed or
  validated OSL model,” while its caption narrows validation to review of
  represented meaning (`tex/sections/osl_workflow_roles.tex` lines 14--22).
  MSH must avoid an unqualified `validated` state.
- `proposed-for-MSH`: review and approval are two explicit human commands with
  authenticated actor, role/scope, target revision, decision/disposition,
  rationale, timestamp, and unresolved issues. AI cannot issue either.
- `proposed-for-MSH`: OSL approval can authorize publication of represented
  knowledge only. It never grants storage leadership/write authority, compute
  authority, artifact access, machine-control authority, or permission to
  perform an `OperatorAction`.

## Representations and serialization

| Representation | Classification | Authority |
| --- | --- | --- |
| Natural-language source fragment | `paper-defined` input from an external elicitation method | Evidence/input, never an OSL model by itself. |
| App form or `OSLPathRecord` | `paper-defined` capture/exchange representation | Non-authoritative; no current schema is defined. |
| OSL abstract syntax and semantic commitments | `paper-defined` language core | Normative research core within the selected profile. |
| SysML v2-compatible text | `paper-defined` primary engineering notation and current realization | Authoritative model notation in the paper; parser success alone is insufficient. |
| `#osl_*` keyword layer | `paper-defined` vocabulary grounded in native SysML usages and typed connection prototypes | Convenience notation, not a parallel metamodel. |
| Reasoning-path, alternative-comparison, decision-review views | `paper-defined` source-bound semantic projections | Secondary views; cannot redefine or copy canonical semantics. |
| Pilot tree rendering | `paper-defined` debugging/renderability evidence | Not a communication-usability or semantic guarantee. |
| JSON validation manifests | `paper-defined` test configuration | Not OSL serialization. |
| JSON/YAML canonical codec in MSH | `proposed-for-MSH` product adapter | Must preserve the graph, version, IDs, provenance, and extensions; schema alone is not OSL. |

Sources: `tex/sections/osl_workflow_roles.tex` lines 27--39,
`tex/sections/osl_capabilities.tex` lines 1--17, and
`sysml/osl-paper-view.sysml` lines 1--80.

`requires-research-clarification`: the repository contains no current
`OSLPathRecord` definition, canonical JSON/YAML mapping, ordering rules,
language-version field, or round-trip contract. MSH must not treat paper
examples or test-manifest JSON as such a contract.

## Known inconsistencies and bounded research debt

| ID | Classification | Conflict or missing definition | Planning treatment |
| --- | --- | --- | --- |
| LQ-01 | `requires-research-clarification` | The semantic contract is wider than the implemented research profile (`evaluation/osl-final-reassessment.md` lines 14--16, 60--72). | Name and freeze a bounded MSH profile; list every intentional extension/deviation. |
| LQ-02 | `requires-research-clarification` | Core retains legacy `EvidenceStatus`, `ContextConstraint`, `ExpectedEffect`, `ExplicitIncompleteness`, `ReviewedPathContent`, and `ReviewedStrategyPath` (core lines 105--121, 303--310, 417--424, 878--916, 1029--1047). | Never make these canonical MSH types; import only through a compatibility adapter with warnings. |
| LQ-03 | `requires-research-clarification` | Some instructions/planning artefacts refer to WF1--WF7 and `ModelStatus.validated`, while current core/manuscript use WF1--WF15 and maturity types. | Use current contract/core/reassessment; record older forms as migration-only. |
| LQ-04 | `requires-research-clarification` | `sysml/traceability-mapping.md` and `evaluation/osl-requirements-traceability.md` retain old reviewed/evidence/effect terms, older view counts, and suite counts. | Do not source product enums from those stale rows; cite them only where reconciled. |
| LQ-05 | `requires-research-clarification` | Contract says a situation label alone is incomplete applicability; WF2/native core require only exactly one situation. | Validator warning in first profile; do not invent a mandatory relation until clarified. |
| LQ-06 | `requires-research-clarification` | `ProvisionalPathBasis` counts `OpenQuestion` and `ReviewNeed` but not `ValidationNeed` (core lines 844--852, 989--996). | Preserve all three gap types; profile explicitly states which satisfy provisional maturity. |
| LQ-07 | `requires-research-clarification` | Arbitrary relation qualification, context-to-consequence, several rationale targets, and element-scoped review dispositions are incomplete. | Represent supported target types explicitly and fail/warn on unsupported claims; do not claim generic coverage. |
| LQ-08 | `requires-research-clarification` | Composition has path-level provenance/participation notes but no element-to-element mapping or conflict-resolution policy. | Call it provenance-bearing composition only; no merge operation. |
| LQ-09 | `requires-research-clarification` | Confidence basis and quantitative confidence are not implemented. | First contract preserves qualitative value plus separate optional basis/extension; no ungrounded conversion. |
| LQ-10 | `requires-research-clarification` | Identity, versioning, immutable revisions, canonical serialization, compatibility, and deprecation are undefined. | Add explicitly versioned MSH product contracts without labeling them paper-defined. |
| LQ-11 | `requires-research-clarification` | Current connectivity validation is undirected and negative-witness coverage is incomplete. | Preserve endpoint semantics and add MSH-native positive/negative tests; do not claim causal validation. |
| LQ-12 | `requires-research-clarification` | Core defines more granular trace types than the keyword layer exposes. | Canonical model uses core relation types; SysML adapter emits only supported faithful forms or diagnostics. |

## Paper-to-MSH traceability matrix

Every component in the final column is `proposed-for-MSH`. The matrix allocates
responsibility; it does not assert that those components exist.

| Requirement | Paper evidence | Planned MSH component responsibility | Verification evidence to require |
| --- | --- | --- | --- |
| R1 situations | `tex/sections/research_gap.tex` lines 31--35; core lines 929--936 | OSL domain model, applicability query, semantic validator | Exactly-one-situation contract tests; missing/duplicate negative cases |
| R2 observations and triggers | `research_gap.tex` R2; contract lines 87--93 | Separate node types, `TriggerBasis` relation, capture mapping | Round trip preserves type; no implicit observation-to-trigger conversion |
| R3 context and applicability | `research_gap.tex` R3; core lines 238--271 | Context/criterion nodes, relation repository, applicability projection | Endpoint/locality tests and bare-situation diagnostic |
| R4 hypotheses | `research_gap.tex` R4; contract lines 95--101 | Hypothesis node plus indication/contradiction edges | Competing/disputed hypothesis fixtures; no truth flag inference |
| R5 goals, decisions, actions | `research_gap.tex` R5; core lines 179--209, 312--341 | Separate types, candidate/selection relations, authority boundary | Decision/action IDs differ; selection cannot call execution |
| R6 rationale and evidence | `research_gap.tex` R6; contract section 5 | Immutable source/provenance service and qualification graph | Targeted evidence, independent dimensions, immutable source tests |
| R7 consequences, risks, trade-offs | `research_gap.tex` R7; core lines 186--192, 367--415, 530--576 | Typed consequence model and compatibility validator | Invalid action/consequence and unattached risk/trade-off negatives |
| R8 uncertainty and progressive maturity | `research_gap.tex` R8; core lines 984--1027 | Maturity evaluator, gap model, separate product lifecycle service | Maturity does not imply review/approval/authority |
| R9 reuse, variation, alternatives | `research_gap.tex` R9; contract section 7 | Specialization resolver, alternative/composition store, conflict preservation | Separate identity and no automatic ranking/merge |
| R10 checking, composition, traces | `research_gap.tex` R10; contract sections 8--10 | Versioned validator, typed traces, import diagnostics | WF1--WF15 fixtures and granular trace checks |
| R11 embedded vocabulary and views | `research_gap.tex` R11; capabilities lines 1--17 | Language registry, codecs, SysML v2 adapter, query/projection layer | Codec round trip; deterministic faithful export; view/source trace |
| Path identity and provenance | Contract lines 52--58, 311--331 | Versioned repository and immutable revision graph | IDs survive serialization; composition preserves source versions |
| Human domain review | `osl_workflow_roles.tex` lines 14--22; contract section 6 | Review service and records | Actor/scope/disposition/revision required; AI denied |
| Explicit non-goals | Contract section 13 | Lifecycle policy, authorization guards, audit | No API or adapter grants execution/compute/storage authority |
| Representation stack | `osl_workflow_roles.tex` lines 27--39 | Canonical domain graph plus independent codecs/projections | UI/SysML/JSON are never mutation sources without commands |

## Acceptance boundary for later implementation

A future first language-core delivery is acceptable only when it can show:

- `proposed-for-MSH` a named, source-pinned language profile and registry entry;
- `proposed-for-MSH` separate typed identities for Decision and OperatorAction,
  candidate and selected response relations, evidence, review, and consequences;
- `proposed-for-MSH` lossless model/codec round trips with version and provenance;
- `proposed-for-MSH` deterministic findings for the selected WF1--WF15 profile;
- `proposed-for-MSH` explicit representation of unsupported/deferred paper
  semantics instead of silent weakening;
- `proposed-for-MSH` no lifecycle transition, AI adapter, serializer, projection,
  or SysML adapter with approval or operational authority;
- `proposed-for-MSH` no production binding, recommender activation, or execution
  surface.

The concrete MSH mapping, file placement, and delivery sequence are specified in
the remaining plan documents. This document remains the language-semantics
baseline against which those later proposals must be checked.
