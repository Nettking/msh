# Planned OSL data model and contracts

Status: conceptual and wire-contract plan; all examples are
`proposed-for-FCP` and **not implemented contracts**.

Analyzed baselines:

- `systems-paper`
  `ff098ce52f15b489b6a07d5b55c6c788d862e3be`;
- `paper-repo`
  `abe3fbcddee590c3f399b06f63cb329e8615977c`;
- `fcp` `f580c71f7269643a077cc7e7db8ba9bf6050bb6a`.

This document fixes semantic responsibilities and invariants. Proposed names are
working names until phase 1 freezes the profile and contracts. A field is not
required merely because it appears in an example below.

## Contract design rules

1. `paper-defined` OSL is a typed relational graph. The canonical product model
   must preserve typed nodes, typed relation usages, identity, path membership,
   cardinality, gaps, evidence targeting, review, alternatives, conflicts,
   composition, and traces. A flat JSON object is insufficient.
2. `proposed-for-FCP` source artefacts, excerpts, fragment revisions, validation
   results, reviews, approvals, publications, provenance events, and exports are
   immutable after commit.
3. `proposed-for-FCP` stable logical identity and immutable revision identity are
   separate.
4. `proposed-for-FCP` language/profile version, product contract/codec version,
   source-paper commit, and producer version are separate.
5. `proposed-for-FCP` every derived claim can identify its source excerpt(s),
   generator/editor, and uncertainty. Missing provenance is a finding, never
   silently fabricated.
6. `paper-defined` Decision, candidate response, selected response, and
   OperatorAction remain distinct.
7. `proposed-for-FCP` Recommendation, approval, authorized action, action
   occurrence, expected outcome, and observed outcome/feedback are separate
   objects outside paper-defined selection semantics.
8. `paper-defined` evidence dimensions are independent and evidence is not
   proof.
9. `proposed-for-FCP` workflow state, OSL maturity, reviewer disposition,
   validation status, evidence review, and confidentiality are orthogonal.
10. `proposed-for-FCP` client/UI fields never establish actor identity, role,
    approval, or authority; those come from authenticated server context.
11. `proposed-for-FCP` unknown required major versions fail closed. Permitted
    unknown extensions are preserved losslessly and never interpreted as core
    semantics.
12. `proposed-for-FCP` no initial contract contains executable code, tool calls,
    shell commands, machine addresses, or operational authority.

## Shared identity and revision envelope

Every persisted aggregate uses the following conceptual envelope. Names are
tentative; semantic responsibilities are fixed.

| Conceptual field | Cardinality | Rationale/classification |
| --- | --- | --- |
| Contract/codec identifier | exactly one | `proposed-for-FCP` fail-closed wire/storage decoding; distinct from OSL language. |
| Object kind | exactly one | `proposed-for-FCP` bounded discriminator; never dynamic class/module name. |
| Logical ID | exactly one | `proposed-for-FCP` stable identity across revisions. |
| Immutable revision ID | exactly one for versioned objects | `proposed-for-FCP` target for review, approval, trace, and export. |
| Aggregate sequence/revision | exactly one where commands mutate aggregate history | `proposed-for-FCP` optimistic concurrency and ordered events. |
| Parent/base revision | zero or one for linear edit; explicit multiple parents only if later merge is designed | `proposed-for-FCP` lineage; the papers do not define merge semantics. |
| Supersedes references | zero or more | `proposed-for-FCP` non-destructive lifecycle lineage. |
| Created time | exactly one UTC instant | Existing FCP convention; source-local time can be separate. |
| Created actor/tool | exactly one attributed principal or tool-run reference | `paper-defined` assessor/originator and method traceability; `proposed-for-FCP` server-bound identity. |
| Content hash | exactly one for immutable content | `proposed-for-FCP` bind validation, review, approval, publication, and export. |
| Classification/access-policy reference | exactly one for sensitive aggregates | `paper-defined` confidentiality concern; `proposed-for-FCP` orthogonal policy. |
| Extensions | zero or more namespaced values | `proposed-for-FCP` forwards compatibility under profile policy. |

IDs should be opaque application identifiers. Display names and SysML qualified
names are labels, never identity. Hash algorithm and canonical byte rules are
part of the contract/codec version.

## Capture and source contracts

### CaptureSession

`paper-defined` basis: method phase 1 requires focus, roles, consent, and
confidentiality (`paper-repo` `04_method.tex` lines 35--37).

`proposed-for-FCP` responsibilities:

| Concept | Requiredness | Notes |
| --- | --- | --- |
| Session logical/revision ID | required | Amendments create a new policy revision. |
| Scope | required | Site/task/machine/operation purpose; avoid proprietary detail in general audit. |
| Participant role references | required as policy demands | Role references, not necessarily personally identifying labels in projections. |
| Consent/policy reference and version/hash | required | Content may live in a protected external policy store. |
| Permitted/prohibited capture kinds | required | Text, image, audio, file, photography, etc. |
| Confidentiality and publication boundary | required | Orthogonal to fragment review. |
| AI processing policy | required | Local/remote/disabled and allowed sensitivity; AI cannot infer consent. |
| Retention/access policy | required | Does not authorize destructive mutation of provenance. |
| Session state | required | Draft/open/closed/restricted; not fragment lifecycle. |

### SourceArtefact

`paper-defined` basis: raw note ID, timestamp, raw text, setting, task phase,
speaker role, artefact reference, confidentiality, and capture confidence are
proposed in `05_annotation_schema.tex` lines 27--50. The raw layer remains
available after interpretation.

| Concept | Cardinality | Rationale |
| --- | --- | --- |
| Source logical/revision ID | 1 | `proposed-for-FCP` immutable identity/lineage. |
| Capture session/policy revision | 1 | `paper-defined` session trace; exact policy binding is product hardening. |
| Original content storage reference | 1 | `proposed-for-FCP` bytes remain unchanged and access-controlled. |
| Content hash, size, media type, encoding | 1 each where applicable | `proposed-for-FCP` integrity and bounded decoding. |
| Client capture time and server receipt time | 0..1 and 1 | Separate observed source time from durable receipt. |
| Setting/task phase | 0..1 each | `paper-defined` context preservation. |
| Speaker/source role | 0..1 | Not an authenticated reviewer identity. |
| External artefact reference | 0..* | `paper-defined` optional capture context; typed as planned external reference. |
| Capture fidelity/confidence | 0..1 plus basis | Confidence that capture is accurate, not truth of content. |
| Classification/consent reference | 1 | `paper-defined` confidentiality; `proposed-for-FCP` server policy. |
| Capture channel/tool | 1 | `proposed-for-FCP` manual/import/OCR/transcription provenance. |
| Correction/supersession relation | 0..* | New source revision/annotation; never overwrite original. |

Source content is not a Decision, Observation, evidence fact, recommendation, or
OSL fragment by itself.

### SourceExcerpt

`requires-research-clarification`: the papers do not define segmentation or
selector syntax. `proposed-for-FCP` requires:

| Concept | Cardinality | Notes |
| --- | --- | --- |
| Excerpt ID | 1 | Immutable. |
| Exact source revision/hash | 1 | Prevents excerpt drift. |
| Selector kind and selector | 1 | Character/byte/time range or bounded format-specific selector. |
| Excerpt hash | 1 | Detects decoder/selector mismatch. |
| Label/task phase override | 0..1 | Derived annotation, not original content. |
| Creator/generator reference | 1 | Human, deterministic tool, or AI run. |
| Creation/acceptance decision | 1 | AI-proposed segments require human acceptance in the first release. |
| Classification override | 0..1, only more restrictive without policy authority | Cannot relax source classification. |

The query layer may materialize excerpt text for an authorized reader. The
canonical excerpt contract references the source rather than becoming a second
mutable copy.

## Candidate-layer contracts

### RelevanceDecision

Representation-neutral upstream decision:

- excerpt IDs;
- taxonomy/profile version;
- selected/not-selected/needs-clarification decision;
- suggested categories;
- rationale and uncertainty;
- human/AI origin;
- actor/run/time and superseded prior decision.

`paper-defined` note selection categories appear in `04_method.tex` lines 47--49.
`proposed-for-FCP` not-selected content remains retained under source policy.

### CandidateExtraction

This object is explicitly non-canonical.

| Concept | Cardinality | Notes |
| --- | --- | --- |
| Candidate ID/version | 1 | Candidate edits are versioned or appended. |
| Source excerpt references | 1..* | A candidate without a source can be imported only as provenance-deficient quarantine. |
| Proposed claims | 0..* | Typed candidate claims; each records origin and excerpt support. |
| Proposed relations | 0..* | Endpoint IDs must belong to candidate claim set. |
| Missing/ambiguous items | 0..* | Never auto-completed. |
| Downstream representation hint | 0..* | OSL is one option, not automatic. |
| Generator/editor provenance | 1..* | Human or AI model/run/prompt/result hashes. |
| Candidate confidence/uncertainty | 0..* | Separate from evidence confidence and capture fidelity. |
| Explanation | 0..1 per suggestion | Supports human verification, not authority. |

Candidate claims include representation-neutral categories for cue,
condition/context, interpretation, decision, recommendation, action,
rationale, exception, uncertainty, risk, responsibility, escalation, expected
outcome, observed feedback, and validation question. Mapping them to OSL types
is a deliberate human-authored transformation.

### ConditionClaim

The Notebook-to-OSL schema uses a broad `condition`, while OSL has `Context`,
`ApplicabilityCriterion`, `SelectionCriterion`, continuation/escalation/
verification criteria, and `ActionCondition` relations.

`proposed-for-FCP`:

- keep `ConditionClaim` only in the upstream candidate layer;
- require the modeller to classify it into a paper-defined canonical type/
  relation or explicit unresolved gap;
- do not introduce a generic canonical OSL `Condition` node without a named
  profile extension and research approval.

## Canonical OSL aggregate

### StrategyFragmentRevision

`paper-defined` systems-paper defines `OperatorStrategy` containing `1..*`
`StrategyPath` values. `paper-repo` calls its unit an operator-strategy fragment.

`proposed-for-FCP` aggregate decision:

- a `StrategyFragmentRevision` is the product/version envelope for one
  `OperatorStrategy` graph with one or more related paths;
- a Notebook-to-OSL candidate normally begins as one provisional path;
- alternatives may require multiple paths to preserve distinct identities;
- review, validation, approval, and publication always identify exact revision,
  path/element scope, and content hash.

Conceptual contents:

| Concept | Cardinality | Source/rationale |
| --- | --- | --- |
| Shared identity/revision envelope | 1 | Product versioning and provenance. |
| Language profile + systems-paper SHA | 1 | `proposed-for-FCP` source-pinned semantics. |
| `OperatorStrategy` root ID/label | 1 | `paper-defined` container. |
| `StrategyPath` records | 1..* | `paper-defined` WF1. |
| Typed semantic elements | 1..* as required | Paper-defined element inventory. |
| Typed relation usages | 0..* subject to WF rules | Paper-defined connected semantics. |
| Path memberships/ownership | explicit | Needed for WF15 and stable graph reconstruction. |
| OSL maturity assessment | derived/persisted result reference | Never an uncontrolled user string. |
| Product lifecycle | one current aggregate state plus event history | `proposed-for-FCP` separate from maturity. |
| Validation result references | 0..* | Exact revision/hash. |
| Candidate/source derivation references | 1..* for notebook-derived content | `paper-defined` method traceability. |
| Extension values | 0..* | Namespaced, versioned, policy-controlled. |

The aggregate contains no executable function, action endpoint, machine command,
compute/storage grant, or implied recommendation policy.

### Common SemanticElement envelope

`proposed-for-FCP` all canonical nodes share:

- immutable element ID;
- paper-defined element kind;
- path ownership or explicit shared/reference scope;
- human label;
- a textual/structured content payload sufficient to preserve meaning;
- element-level provenance/source-claim references;
- optional namespaced extensions.

`requires-research-clarification`: systems-paper primarily expresses meaning
through SysML named usages and does not define a universal text/value field for
every construct. Phase 1 must decide the minimal content representation without
mistaking labels for complete semantics.

### Observation

`paper-defined`: observed/measured/reported/inspected/retrieved information,
separate from Trigger, Hypothesis, and Evidence.

Planned semantic fields:

- element identity/path;
- content/statement or structured observed-value reference;
- observation modality/source role as a product extension;
- optional time/window/external telemetry reference;
- element-level evidence references and uncertainty;
- no verified-fact boolean.

An external telemetry candidate can support an Observation, but does not
automatically create one or mark it true.

### Context

`paper-defined` qualifies path, hypothesis, decision, action, or criterion and
must not be treated as runtime activation.

Planned fields:

- element identity/path;
- stated contextual boundary;
- optional structured external context reference;
- typed qualification relations;
- source/evidence references.

Context-to-consequence is `requires-research-clarification` because the current
paper profile does not implement that dedicated relation.

### Decision

`paper-defined` `StrategyDecision` is a represented judgement or choice
structure.

Planned fields:

- element ID/path and decision statement;
- criteria, goal, hypothesis, rationale, evidence, and context relations;
- candidate `DecisionOption` and selected `ActionSelection` relation IDs;
- explicit gaps if no response commitment exists.

It has no action-role field and cannot be used as an OperatorAction occurrence.

### OperatorAction

`paper-defined` descriptive response with role:
intervention, protective, inspection, diagnostic, monitoring, logging,
escalation, verification, corrective, continuation, other, or unknown.

Planned fields:

- element ID/path and action description;
- action role and optional specialized OSL kind;
- candidate/selection, condition, ordering/enabling/dependency, consequence,
  risk, rationale, evidence, trace, and context relations;
- an explicit representation modality if needed as a profile extension.

The contract does not say the action was recommended, authorized, performed, or
successful.

### RecommendationCandidate

`proposed-for-FCP` product/downstream object, not part of the paper-defined OSL
core:

- exact source fragment revision and Decision/Action relation reference;
- proposed guidance text and audience;
- generator/author and evidence basis;
- status always candidate until separately governed;
- no execution endpoint or authority.

`RecommendationArtifactReference` may trace an OSL element to this object after
it exists. The trace does not approve the recommendation.

### ExpectedOutcome and other expected consequences

`paper-defined` `ExpectedConsequence` is specialized into operational outcome,
diagnostic result, monitoring result, verification result, and organisational
result.

Planned fields:

- element ID/path;
- consequence kind and statement/criterion;
- producing/informing/checking action relation;
- assessment/verification criterion reference where known;
- uncertainty/evidence/gaps.

Expected content never receives observed time/value fields that imply it
occurred.

### ObservedOutcome / FeedbackRecord

`proposed-for-FCP` separate post-use/evidence contract:

- feedback ID and source artefact/excerpt;
- exact related fragment revision, action claim/occurrence if known, expected
  consequence reference, context and time/window;
- reporter/assessor and evidence review;
- observed result claim, measurement/external reference, uncertainty;
- validation/review state.

It cannot mutate an ExpectedOutcome or published fragment. It may seed a new
draft revision.

### Rationale

`paper-defined` can justify hypothesis, goal, decision, action, or criterion.

Planned fields:

- element ID/path and rationale statement;
- one or more typed justification relations;
- source/evidence/uncertainty;
- explicit unsupported-target finding for relation categories not implemented
  in the selected profile.

### Risk and TradeOff

`paper-defined`:

- Risk identifies what gives rise to it and what it threatens;
- TradeOff connects at least two goals, actions, or consequences.

Planned content is a statement plus typed relation usages and evidence. A
severity/likelihood scoring extension is not part of the paper core and must be
versioned/justified separately.

### Gaps and uncertainty

`paper-defined` `OpenQuestion`, `ReviewNeed`, and `ValidationNeed` are separate
`Gap` types with impact:

- blocks structural completion;
- blocks domain review;
- blocks operational use;
- refinement only;
- unknown.

`proposed-for-FCP` gap fields include immutable ID, kind, statement, impact,
target element/relation/path commitment, origin, source, and resolution
reference. Resolving a gap creates a new fragment revision; history remains.

Uncertainty remains separate across:

- raw capture fidelity;
- candidate/generator uncertainty;
- claim/evidence confidence with basis;
- explicit gap;
- evidence review/dispute;
- reviewer disposition.

No generic “confidence” field may collapse these.

## RelationUsage

Each typed edge requires:

| Concept | Cardinality | Rationale |
| --- | --- | --- |
| Relation usage ID | 1 | Stable target for review, evidence, provenance and comparison. |
| Paper-defined relation type | 1 | Carries semantics; never arbitrary string in a published profile. |
| Named ends and endpoint IDs | exactly as profile defines | Preserves direction/role and endpoint typing. |
| Owning path or explicit cross-path scope | 1 | WF15 locality and allowed alternatives/composition/conflicts/traces. |
| Relation attributes | profile-specific | E.g. provenance/participation note on composition. |
| Source/evidence/uncertainty | 0..* | Product provenance; generic relation-level evidence may be profile-limited. |
| Extensions | 0..* | Namespaced/preserved under policy. |

The repository must reject a relation whose endpoint revision differs from the
containing immutable graph unless the relation type/profile explicitly permits
an external or cross-version reference.

## EvidenceReference and epistemic qualification

### EvidenceReference

`proposed-for-FCP` durable bridge between an OSL `Evidence` element/qualification
and source material:

- evidence reference ID;
- source artefact/excerpt/external-evidence revision;
- target element/relation/path ID and qualification relation type;
- source kind and derivation status;
- evidence review status;
- qualitative confidence plus explicit basis, or a namespaced quantitative
  extension;
- assessor/originator;
- scope/time/window;
- content/expected hash and resolution state;
- classification/access policy.

`paper-defined` independent enumerations come from
`sysml/osl-core.sysml` lines 25--173. `EvidenceStatus` is legacy and must not be
accepted in new canonical contracts.

### Evidence invariants

- Evidence without a target produces a WF10 finding.
- A string saying “source-backed” is not a source reference.
- AI/model output uses `source_kind=model_output` and never evidence review
  `accepted` by default.
- Source access is checked independently from fragment access.
- Evidence supersession does not erase the prior assessment.
- External resolution failure becomes unresolved/stale, not silently valid.
- A reviewer's acceptance of represented meaning does not prove evidence truth.

## Review, approval, and lifecycle contracts

### ReviewerDecision

`paper-defined` minimum review concepts: reviewer reference, reviewed version or
date, disposition, note, and path link
(`sysml/osl-core.sysml` lines 145--151 and 673--676).

`proposed-for-FCP` richer contract:

| Concept | Cardinality | Notes |
| --- | --- | --- |
| Review ID | 1 | Immutable. |
| Exact fragment revision/profile/hash | 1 | Reject stale content. |
| Reviewer principal and role | 1 | Server-bound authenticated identity; not form text. |
| Review scope | 1..* targets/path/whole revision | Addresses paper's partial review-scope model. |
| Disposition | 1 overall plus 0..* element dispositions | Accepted/corrected/narrowed/rejected/disputed/pending. |
| Rationale/note | required for non-acceptance; policy-defined otherwise | Protected content. |
| Unresolved blocking issues | 0..* gap references | Required to assess WF12/approval. |
| Source/projection set shown | 0..* references | Records basis without copying unrestricted source. |
| Review time and policy version | 1 each | Auditability. |

`sensitive` is not a disposition. Correction/narrowing results in a new draft
revision; this decision remains bound to the old content.

### Approval

`proposed-for-FCP`, not paper-defined:

- approval ID;
- exact fragment revision/profile/content hash;
- approver principal/role/scope and policy revision;
- review and validation result IDs;
- decision/rationale/time;
- allowed publication audience constraints;
- revocation/invalidated-by reference where governance requires.

Approval cannot be created by AI, import, serializer, validator, UI-provided
signer text, domain review alone, or current FCP `reusable` status.

### PublicationRecord

`proposed-for-FCP`:

- exact approval and fragment revision/hash;
- publisher principal/scope and audience;
- publication sequence/time/policy;
- projections/export records;
- deprecation/supersession lineage.

Publication has no activation/execution field.

### LifecycleState and TransitionEvent

`proposed-for-FCP` states:

- `draft`;
- `in_review`;
- `reviewed`;
- `approved`;
- `published`;
- `rejected`;
- `deprecated`;
- `superseded`.

`extracted_candidate` belongs to the candidate layer, not canonical fragment
lifecycle. `abandoned` can be a draft workflow terminal without deleting data.

Every transition event includes:

- event/aggregate sequence;
- from/to states;
- exact revision/hash;
- actor/role/scope;
- command ID and payload fingerprint;
- time/reason/policy version;
- validation/review/approval references;
- redaction-safe result/failure code.

The legal state graph belongs to `lifecycle.py`, not client code or database
strings.

## ProvenanceChain

`paper-defined` method traceability links raw note, session, validation status,
rationale, uncertainty, responsible actor, and downstream candidate
(`03_research_design.tex` lines 31--40;
`04_method.tex` lines 74--76).

`proposed-for-FCP` provenance is an append-only directed acyclic derivation
graph, distinct from the OSL reasoning graph:

### Provenance entity kinds

- capture session/policy revision;
- source artefact and excerpt;
- relevance decision;
- candidate extraction and generator run;
- clarification question/answer;
- fragment revision;
- validation result;
- reviewer decision;
- approval/publication;
- import/migration;
- export/projection;
- feedback and supersession/deprecation.

### Provenance edge kinds

- `captured_under`;
- `excerpt_of`;
- `derived_from`;
- `generated_by`;
- `edited_from`;
- `clarified_by`;
- `validated_by`;
- `reviewed_by`;
- `approved_by`;
- `published_as`;
- `exported_as`;
- `supersedes`;
- `supported_by`.

Edge names are product provenance terms, not OSL core relations. They must not
be serialized as OSL reasoning edges.

### Provenance invariants

- every derived object identifies at least one immediate input or an explicit
  `provenance_missing` finding for quarantined legacy imports;
- actor/tool/run identity and time are recorded at the derivation edge;
- hashes bind immutable inputs/outputs;
- provenance cannot contain a cycle;
- audit-safe event details and user-visible provenance are separate projections;
- redaction hides content but preserves permitted existence/lineage markers;
- deleting a projection/cache never deletes provenance.

## ExternalModelReference

`paper-defined` OSL defines typed downstream artefact references with optional
`target_uri` (`sysml/osl-core.sysml` lines 215--232) and both path- and
element-level trace relations.

`proposed-for-FCP` contract:

| Concept | Cardinality | Notes |
| --- | --- | --- |
| Reference ID/type | 1 | Requirement, telemetry, monitoring, training, recommendation, procedure, task, configuration, KPI, explanation, risk, evidence, review, validation, or approved extension. |
| Resolver namespace | 1 | Prevents treating arbitrary URI as executable resolver. |
| Target ID/URI | 1 | Stored as data; no network resolution by default. |
| Expected target version/hash | 0..1 | Detects drift. |
| Source element/path relation | 1 via trace | Prefer element granularity when known. |
| Resolution status/time | 0..1 | Unresolved/resolved/stale/forbidden; not implementation proof. |
| Access/classification scope | 1 | Avoid data leakage through resolution. |

A reference does not prove the target exists, is implemented, verified, safe,
or authorized. Import/export never dereferences external URIs automatically.

## ValidationResult

`proposed-for-FCP` immutable deterministic contract:

- validation result ID;
- exact fragment revision/profile/content hash;
- validator implementation/version and ruleset hash;
- started/completed time and invocation actor/tool;
- overall status: pass/fail/incomplete/unsupported profile;
- ordered findings;
- resource-limit/internal-error indication;
- result content hash.

Each `ValidationFinding` includes:

- stable rule ID (WF1--WF15 or product contract rule);
- severity/category;
- element/relation/path target IDs;
- stable message code plus human explanation;
- source/profile citation where available;
- remediation hint that does not invent content;
- suppression/waiver reference only if a separately approved policy allows it.

Validation cannot set reviewer disposition, approval, publication, evidence
truth, operational validity, or safety.

## Import, migration, and export contracts

### ImportReport

- input artefact ID/hash/media type/size;
- detected contract and language versions;
- decode/extension/reference/semantic diagnostics;
- proposed mapping and provenance;
- disposition: quarantine, candidate, draft-eligible, or rejected;
- human import decision.

External claims of `approved` or `published` are imported as source metadata,
never trusted local lifecycle.

### LegacyOperatorRecordMapping

`existing-in-FCP` fields map conservatively:

| Legacy value | Planned result |
| --- | --- |
| `raw_statement` | Immutable source artefact/excerpt. |
| `decision` | Unverified candidate Decision **and/or** action ambiguity finding; never canonical automatically. |
| `action_type` | Candidate action role hint. |
| `observation`, `trigger`, `context`, `hypothesis`, `goal`, `rationale`, `risk`, `trade_off` | Candidate claims with legacy-record provenance. |
| `expected_outcome` | Candidate expected consequence. |
| `outcome`/`worked` | Candidate feedback assertion, not expected outcome or verified fact. |
| `evidence`/`confidence`/`trace_target` | Free-text candidate metadata with missing structured-reference findings. |
| `reusable_strategy=true` | `legacy_reusable` annotation only. |
| confirmation/quality/first-part IDs | Unresolved external evidence candidates until integrity/access checks. |

### ExportRecord

- export ID;
- exact fragment revision/profile/hash;
- serializer/exporter format and version;
- producer version;
- audience/classification;
- output storage reference/hash/size;
- source-to-output ID map;
- diagnostics;
- requester/authorization/time.

An export is immutable and cannot change fragment lifecycle.

## Planned command envelope

All state-changing application commands use:

- command ID;
- command kind/version;
- authenticated actor principal and authorized scope from server context;
- target aggregate/logical ID;
- expected aggregate revision;
- payload fingerprint;
- reason and policy version where required;
- bounded command-specific payload.

The repository replays the prior result only for identical command ID and
fingerprint. A collision rejects. Commands do not carry client-asserted
`approved_by`, reviewer role, storage authority, compute grant, or execution
permission.

## Planned contract example 1: source and excerpt

The following is a **planned example, not an implemented or final contract**.
Names and version identifiers intentionally use `-plan`.

~~~json
{
  "contract": "fcp.osl.source-artefact.v1-plan",
  "status": "planned-not-implemented",
  "source_id": "src_01",
  "source_revision_id": "srcrev_01",
  "capture_session_revision_id": "sessionrev_01",
  "content": {
    "media_type": "text/plain",
    "encoding": "utf-8",
    "byte_length": 118,
    "sha256": "<content-hash>",
    "storage_ref": "<protected-local-reference>"
  },
  "captured_at": "2026-08-05T09:00:00Z",
  "received_at": "2026-08-05T09:00:03Z",
  "setting": "training-session-alias",
  "task_phase": "inspection",
  "speaker_role": "operator",
  "classification_policy_ref": "policyrev_01",
  "capture_fidelity": {
    "value": "medium",
    "basis": "typed during live explanation"
  },
  "excerpt": {
    "excerpt_id": "excerpt_01",
    "selector": {"kind": "character-range", "start": 0, "end": 74},
    "sha256": "<excerpt-hash>",
    "accepted_by": "principal_annotator"
  }
}
~~~

The example does not embed consent content, real names, proprietary machine
data, or a Decision derived from the raw text.

## Planned contract example 2: provisional fragment graph

The following is a **planned example, not an implemented or normative OSL
serialization**. It illustrates semantic separation, not final field names.

~~~json
{
  "contract": "fcp.osl.fragment-revision.v1-plan",
  "status": "planned-not-implemented",
  "language_profile": {
    "id": "fcp-osl-research-profile-plan",
    "systems_paper_commit": "ff098ce52f15b489b6a07d5b55c6c788d862e3be"
  },
  "fragment_id": "fragment_01",
  "revision_id": "fragmentrev_01",
  "parent_revision_id": null,
  "content_hash": "<fragment-content-hash>",
  "workflow_state": "draft",
  "strategy": {
    "id": "strategy_01",
    "paths": [
      {
        "id": "path_01",
        "maturity": "provisional",
        "element_ids": [
          "situation_01",
          "observation_01",
          "hypothesis_01",
          "decision_01",
          "action_01",
          "result_01",
          "evidence_01",
          "question_01"
        ]
      }
    ]
  },
  "elements": [
    {"id": "situation_01", "kind": "Situation", "content": "During an inspection task"},
    {"id": "observation_01", "kind": "Observation", "content": "A changed cutting sound was reported"},
    {"id": "hypothesis_01", "kind": "Hypothesis", "content": "Tool wear may be one explanation"},
    {"id": "decision_01", "kind": "StrategyDecision", "content": "Decide whether inspection is warranted"},
    {"id": "action_01", "kind": "InspectionAction", "action_role": "inspection", "content": "Inspect the tool condition"},
    {"id": "result_01", "kind": "DiagnosticResult", "content": "Information about tool condition becomes available"},
    {"id": "evidence_01", "kind": "Evidence", "derivation_status": "direct", "source_kind": "operator_statement", "review_status": "unreviewed", "confidence": "unknown"},
    {"id": "question_01", "kind": "OpenQuestion", "impact": "blocks_domain_review", "content": "Under which materials is the cue meaningful?"}
  ],
  "relations": [
    {"id": "rel_01", "kind": "ObservationIndication", "ends": {"observation": "observation_01", "hypothesis": "hypothesis_01"}},
    {"id": "rel_02", "kind": "HypothesisBasis", "ends": {"hypothesis": "hypothesis_01", "decision": "decision_01"}},
    {"id": "rel_03", "kind": "DecisionOption", "ends": {"decision": "decision_01", "candidate_action": "action_01"}},
    {"id": "rel_04", "kind": "ActionProducesDiagnosticResult", "ends": {"diagnostic_action": "action_01", "result": "result_01"}},
    {"id": "rel_05", "kind": "EvidenceSupportsObservation", "ends": {"evidence": "evidence_01", "observation": "observation_01"}},
    {"id": "rel_06", "kind": "OpenQuestionConcernsContext", "ends": {"question": "question_01", "context": "<context-to-be-modelled>"}}
  ],
  "source_trace": [
    {"element_id": "evidence_01", "excerpt_id": "excerpt_01", "derivation_event_id": "event_10"}
  ],
  "recommendation_candidates": []
}
~~~

This example intentionally uses `DecisionOption` rather than
`ActionSelection`, leaves evidence unreviewed/confidence unknown, and exposes an
unresolved context question. A real validator would reject the placeholder
relation endpoint until the Context element exists; the example demonstrates
that missing semantics produce a finding rather than invented content.

## Planned contract example 3: validation and review

The following is a **planned example, not an implemented contract**.

~~~json
{
  "validation_result": {
    "contract": "fcp.osl.validation-result.v1-plan",
    "status": "fail",
    "fragment_revision_id": "fragmentrev_01",
    "fragment_content_hash": "<fragment-content-hash>",
    "language_profile": "fcp-osl-research-profile-plan",
    "validator_version": "plan",
    "findings": [
      {
        "rule_id": "WF5",
        "severity": "error",
        "target_id": "rel_06",
        "code": "dangling-gap-target",
        "message": "The open question does not yet target a represented Context."
      }
    ]
  },
  "reviewer_decision": {
    "contract": "fcp.osl.reviewer-decision.v1-plan",
    "status": "planned-not-implemented",
    "fragment_revision_id": "fragmentrev_01",
    "fragment_content_hash": "<fragment-content-hash>",
    "reviewer_principal": "principal_domain_reviewer",
    "scope": ["path_01"],
    "disposition": "corrected",
    "unresolved_issue_ids": ["question_01"],
    "reviewed_at": "2026-08-05T11:00:00Z"
  },
  "approval": null
}
~~~

The failed validation and corrected review cannot produce approval or
publication. A new draft revision must address the finding and be revalidated
and reviewed.

## Required contract tests

The future contracts must have:

- exact `to_dict`/`from_dict` or codec round trips;
- deterministic canonical hash tests across Linux and Windows;
- missing/extra/wrong-type/bounds/duplicate-key/duplicate-ID tests;
- unknown major and extension preservation/rejection tests;
- ID/reference/path-locality and relation endpoint tests;
- source/excerpt immutability and selector/hash tests;
- independent evidence-dimension tests;
- Decision versus OperatorAction and candidate versus selection tests;
- expected versus observed outcome tests;
- lifecycle/review/approval separation tests;
- actor/scope cannot be forged through payload tests;
- provenance acyclicity and complete immediate-input tests;
- redaction/data-leakage tests for source, prompts, errors, audit, and exports;
- legacy mapping tests that never auto-approve `reusable` records;
- no executable payload/tool/authority surface tests.

The exact Python classes, SQL tables, JSON property names, and contract fixtures
are assigned by phase in `06_repository_file_plan.md` and
`10_phased_implementation_roadmap.md`. Phase 1 must update this plan if research
owners resolve any `requires-research-clarification` item before implementation.
