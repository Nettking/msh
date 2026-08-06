# Notebook-to-OSL workflow in MSH

Status: source-traceable product workflow proposal; no workflow state or service
is implemented by this document.

Primary method source: `Nettking/paper-repo` at commit
`abe3fbcddee590c3f399b06f63cb329e8615977c`.

Language boundary source: `Nettking/systems-paper` at commit
`ff098ce52f15b489b6a07d5b55c6c788d862e3be`.

MSH baseline: `f580c71f7269643a077cc7e7db8ba9bf6050bb6a`.

## What the paper establishes

`paper-defined` here means a claim or proposal made by the Notebook-to-OSL
paper. The repository labels the paper `drafting` and the manuscript a
`strengthened-first-draft`; empirical material and operator/domain-expert
validation remain future work (`papers/notebook-to-osl/paper.yaml` lines 1--3
and 50--61). These are method requirements and research proposals, not
empirically validated MSH guarantees.

### Six design requirements

`papers/notebook-to-osl/manuscript/sections/03_research_design.tex`,
“Design objective,” lines 29--40 defines:

1. `paper-defined` low-friction capture during training;
2. `paper-defined` preservation of task phase, setting, artefact, and role;
3. `paper-defined` traceability from structured fragment to raw note, source,
   session, validation status, and downstream candidate;
4. `paper-defined` preservation of uncertain, contextual, and disputed content;
5. `paper-defined` operator/domain-expert review before model-ready treatment;
6. `paper-defined` enough structure for later OSL, requirements, SysML v2, rule,
   graph, or other Digital Twin representations.

### Eight research-method phases

| Phase | `paper-defined` activity and output | Source |
| --- | --- | --- |
| 1. Scope and consent | Define a narrow focus, roles, and what may be captured, anonymized, photographed, retained, or excluded. | `04_method.tex` lines 35--37 |
| 2. Situated training | Operator trains the researcher in real/simulated work; questions remain short and context-sensitive. | lines 39--41 |
| 3. Low-friction capture | Store note ID, time, raw text, optional task phase/artefact; do not complete the full schema live. | lines 43--45 |
| 4. Note selection | Select notes containing/implying cues, conditions, interpretations, decisions, actions, exceptions, rationale, risk, validation need, responsibility, trust, or escalation. | lines 47--49 |
| 5. Annotation | Researcher interprets selected notes into fragments while preserving ambiguity and explicit clarification needs. | lines 51--53 |
| 6. CTA-style clarification | Ask about first signs, novice misses, alternative meanings, invalid/unsafe conditions, and escalation; retain uncertainty and questions. | lines 55--68 |
| 7. Operator/domain review | Review interpretation, context, action, threshold, risk, exception, responsibility, and confidentiality; outcome may be accepted, corrected, narrowed, rejected, or sensitive. | lines 70--72 |
| 8. Model-ready transformation | Transform validated or “explicitly bounded” content into a traceable semi-structured downstream candidate. | lines 74--76 |

`paper-defined`: the primary analysis unit is a fragment derived from one or
more notes and relating cue, context, interpretation, possible action, rationale,
exception, uncertainty, risk, responsibility, and validation state
(`03_research_design.tex` lines 42--44).

`paper-defined`: the method intentionally separates raw note, structured
interpretation, validation outcome, and candidate model fragment
(`07_discussion.tex` lines 4--8 and 16--22).

## What the paper does not establish

- No complete OSL or normative OSL syntax
  (`03_research_design.tex` lines 89--93;
  `06_illustrative_case.tex` lines 49--52).
- No empirically evaluated industrial workflow; that is the next study
  (`08_conclusion.tex` lines 4--8).
- No software/API/storage contract, immutable byte/hash scheme, concurrency
  model, or access-control system.
- No segment/excerpt offset model or algorithm.
- No language/fragment version, migration, rollback, supersession, deprecation,
  or post-publication feedback lifecycle.
- No separate Decision, Recommendation, observed action, expected outcome, and
  actual outcome model.
- No structured evidence reference beyond raw-note/session/artefact/source trace
  concepts.
- No formal semantic validator or readiness predicate.
- No product approval, publication, activation, execution, compute/storage
  authority, or operational binding.
- No role for AI in segmentation, classification, annotation, clarification,
  review, or transformation.

Every product mechanism below is therefore `proposed-for-MSH` unless explicitly
marked otherwise.

## Required orthogonal state dimensions

The paper's status lists mix transformation stage, review outcome, and
confidentiality. MSH must not implement one overloaded enum.

| Dimension | Proposed values | Meaning |
| --- | --- | --- |
| Capture session | `draft`, `open`, `closed`, `restricted` | Whether scoped capture may occur; not fragment status. |
| Source artefact | `captured`, `sealed`, `quarantined`, `access_restricted` | Integrity/access state of original material; bytes remain immutable. |
| Extraction | `unassessed`, `selected`, `not_selected`, `extracted_candidate`, `discarded_candidate` | Relevance/derivation stage; none is canonical OSL. |
| Fragment workflow | `draft`, `in_review`, `reviewed`, `approved`, `published`, `rejected`, `deprecated`, `superseded` | MSH product lifecycle over immutable revisions. |
| OSL maturity | provisional, structurally complete, domain reviewed | `paper-defined` systems-paper language maturity, evaluated separately. |
| Reviewer disposition | `accepted`, `corrected`, `narrowed`, `rejected`, `disputed`, `pending` | Human decision over an exact scope/revision. |
| Confidentiality | deployment-approved taxonomy, including a sensitive/restricted class | Orthogonal access/export constraint; not a lifecycle state. |
| Validation | `not_run`, `pass`, `fail`, `incomplete`, `unsupported_profile` | Deterministic conformance result; not factual truth. |
| Operational binding | absent in the initial product | If ever introduced, a separate authority-controlled object, never a fragment state. |

“Model-ready” is not used as a stored boolean. The UI may show a derived
readiness explanation such as “eligible for domain review” or “eligible for
approval,” with the exact predicate/profile.

## Actors

| Actor | Responsibility | Authority boundary |
| --- | --- | --- |
| Operator/trainer | Explain situated work and later review represented meaning. | Does not need to author SysML; review does not execute action. |
| Capturer/researcher | Configure consent scope and capture low-friction source notes. | Cannot silently interpret raw capture or self-grant broad source access. |
| Annotator/OSL modeller | Segment, select, annotate, clarify, create/edit draft revisions. | Cannot make AI/source claims canonical without provenance; edit is not approval. |
| Domain reviewer | Review exact meaning and scope using operational knowledge. | Human-only review; not publication or operational authorization. |
| Approver | Apply product governance to an exact reviewed revision. | Separate permission; approval remains design-time. |
| Publisher | Expose an approved revision to an allowed audience. | Does not activate or execute it. |
| Deterministic validator | Check selected OSL profile rules. | No truth, review, approval, or lifecycle authority. |
| AI candidate generator | Suggest excerpts, classifications, fields, questions, and candidate structure. | Candidate-only; no canonical writes, review, approval, evidence verification, publication, or execution. |
| System/repository | Enforce immutability, revision, idempotency, referential integrity, and event append. | Does not decide domain truth or user permission. |

## End-to-end MSH workflow

### Step 0: establish capture scope, roles, consent, and confidentiality

- **Paper basis:** `paper-defined` phases 1--2 require narrow scope, roles, and
  confidentiality boundaries before capture (`04_method.tex` lines 35--41).
- **Primary actor:** capturer/researcher with operator/stakeholder participation;
  a data owner approves policy where required.
- **Input:** site/organization, purpose, machine/task scope, participant roles,
  permitted source types, consent reference/version, retention, anonymity,
  confidentiality, photography/file rules, and allowed AI locality.
- **Output:** immutable `CaptureSession` policy revision with IDs for scope,
  actors/roles, consent and classification.
- **Status transition:** session `draft -> open` after required policy checks.
- **Persistence:** repository stores policy metadata and references; sensitive
  consent documents may live in a protected external store referenced by ID.
- **Provenance:** creator, participants/roles, policy version/hash, time, reason,
  and later policy amendments. An amendment creates a new revision.
- **Allowed AI:** none required; AI may not infer consent or sensitivity.
- **Mandatory human control:** explicit acknowledgement/authorization under
  deployment policy before opening capture.
- **Failure/recovery:** absent/expired/conflicting consent keeps the session
  draft or restricted; do not capture material. Policy service outage supports
  a paper/manual fallback but no later upload until policy is reconciled.
- **Audit:** record IDs, policy hash, actor/scope, allow/deny reason; no private
  consent content in general audit.
- **UI:** short pre-session checklist with plain-language audience/AI disclosure,
  offline-printable summary, and visible “capture not open” state.
- **Backend:** `SourceService`, authorization/policy port, repository,
  provenance service.

### Step 1: capture a low-friction raw note or source artefact

- **Paper basis:** `paper-defined` phase 3 requests note ID, timestamp, raw text,
  optional task phase/artefact, and save without live annotation
  (`04_method.tex` lines 43--45). The broader raw schema also proposes session,
  setting, speaker role, confidentiality, and capture confidence
  (`05_annotation_schema.tex` “Raw capture layer,” lines 27--50).
- **Primary actor:** capturer during operator training; operator is the knowledge
  source, not necessarily the UI user.
- **Input:** open capture session, bounded raw text/file, capture time, setting,
  task phase, speaker role, optional artefact reference, and capture confidence.
- **Output:** capture command receipt and pending `SourceArtefact` identity.
- **Status transition:** no fragment transition; source enters `captured`.
- **Persistence:** write content exactly as submitted with encoding/MIME and
  server receipt time. Do not copy it to Decision or action fields.
- **Provenance:** capture actor, session/policy revision, source/origin role,
  client capture time and server time, device/import channel, original filename
  only after safe normalization.
- **Allowed AI:** none in the critical live path. Optional speech/OCR is a
  separate derived capture with original media preserved and clearly attributed.
- **Mandatory human control:** capturer confirms the saved content/artefact and
  can add a non-destructive correction note.
- **Failure/recovery:** invalid size/type, closed session, or connectivity
  failure leaves no server artefact; UI retains a bounded local draft with a
  clear unsynced state. Retry uses the same command ID.
- **Audit:** source ID/hash after sealing, session, actor, size/type, outcome;
  never raw content.
- **UI:** one prominent raw field/save action, large touch target, offline/error
  state, time/source context collapsed by default, no required OSL fields.
- **Backend:** capture route/application facade and `SourceService` only.

### Step 2: seal and preserve the unchanged original

- **Paper basis:** `paper-defined` raw capture must remain available because
  later interpretation may change (`05_annotation_schema.tex` lines 27--50;
  `07_discussion.tex` lines 4--8). Cryptographic immutability is
  `proposed-for-MSH` hardening.
- **Primary actor:** system transaction following an authorized capture; human
  can inspect but not alter bytes.
- **Input:** captured content, metadata, session/policy revision, command ID.
- **Output:** immutable `SourceArtefact` with content hash, byte length,
  encoding/MIME, storage reference, and integrity result.
- **Status transition:** source `captured -> sealed`; policy failure may yield
  `quarantined` or `access_restricted`.
- **Persistence:** content-addressed or equivalently integrity-checked local
  storage plus transactional metadata/event. Corrections are new artefacts or
  annotations linked to the original.
- **Provenance:** capture/seal hashes, source and storage versions, actor/tool,
  time, parent/correction relation.
- **Allowed AI:** none.
- **Mandatory human control:** no approval step for integrity; a human decides
  whether quarantined/sensitive content may be processed.
- **Failure/recovery:** hash mismatch or partial write rolls back and alerts the
  capturer; duplicate identical command replays; conflicting duplicate is
  rejected. Never silently accept changed bytes.
- **Audit:** seal success/failure, IDs/hashes, size/classification; no content.
- **UI:** “saved and sealed,” “pending,” “quarantined,” or “restricted,” with
  recovery guidance and a visible immutable-original label.
- **Backend:** source repository/unit of work and provenance/audit service.

### Step 3: create stable excerpts or segments

- **Paper basis:** `paper-defined` a fragment may derive from one or more raw
  notes (`03_research_design.tex` lines 42--44), but the paper defines no
  segmentation algorithm or offsets.
- **Primary actor:** annotator; AI may propose, never finalize silently.
- **Input:** authorized source artefact revision and one or more byte/character/
  time-range or selector proposals.
- **Output:** immutable `SourceExcerpt` records with exact source revision,
  range/selector, excerpt hash, and optional human label.
- **Status transition:** source remains sealed; extraction remains `unassessed`.
- **Persistence:** excerpt references and derived preview; do not duplicate raw
  content as a new source of truth.
- **Provenance:** creator type (human/AI/import), algorithm/model version if
  suggested, time, source hash, and acceptance/edit history.
- **Allowed AI:** suggest boundaries and labels with confidence/explanation;
  only references within the supplied source are accepted.
- **Mandatory human control:** human accepts/edits segmentation before it
  supports a candidate in the first release.
- **Failure/recovery:** invalid/out-of-range/stale selector is rejected; changed
  source cannot occur, but a new source revision requires a new excerpt. Manual
  full-note excerpt remains available.
- **Audit:** source/excerpt IDs, ranges/hashes, actor, decision; redact preview.
- **UI:** source viewer with keyboard/touch selection, accessible excerpt list,
  AI suggestion label, and side-by-side immutable original.
- **Backend:** `SourceService`, query/projection, optional AI generator.

### Step 4: identify relevant statements without deleting background

- **Paper basis:** `paper-defined` phase 4 selects notes that contain or imply
  strategy-relevant categories while retaining background without forcing it
  into the schema (`04_method.tex` lines 47--49).
- **Primary actor:** annotator/researcher.
- **Input:** excerpts plus session/task context.
- **Output:** relevance decision with categories, rationale, and either
  `selected`, `not_selected`, or `needs_clarification`; no OSL fragment yet.
- **Status transition:** extraction `unassessed -> selected/not_selected`.
- **Persistence:** append classification decision; a later reassessment adds a
  new decision rather than deleting source.
- **Provenance:** classifier/human, taxonomy/profile version, supporting
  excerpt, confidence/uncertainty, time and reason.
- **Allowed AI:** suggest relevance/categories and explain; cannot discard,
  redact, or change source and cannot mark a claim true.
- **Mandatory human control:** first-release selection is confirmed by a human.
- **Failure/recovery:** classifier unavailable/malformed becomes manual;
  ambiguous content uses `needs_clarification`; sensitive content is restricted,
  not treated as irrelevant.
- **Audit:** decision IDs/category codes/actor; no raw excerpt text.
- **UI:** inbox filters, bulk triage with undo-by-new-decision, visible
  background/not-selected counts, no destructive delete.
- **Backend:** candidate service, classification policy, provenance repository.

### Step 5: generate an extracted candidate

- **Paper basis:** `paper-defined` phase 5 annotation is interpretive and must
  preserve ambiguity rather than fill missing cue/threshold/rationale
  (`04_method.tex` lines 51--53). The paper assigns interpretation to the
  researcher; it defines no AI role.
- **Primary actor:** annotator; AI or deterministic helpers are optional
  suggestion producers.
- **Input:** selected excerpts, capture context, allowed target profile, and any
  explicit prior clarification.
- **Output:** `CandidateExtraction` containing separately typed proposed cue/
  observation, condition/context, interpretation/hypothesis, Decision,
  recommendation candidate, OperatorAction candidate, rationale, exception,
  uncertainty, risk, trust concern, responsible actor, escalation, gaps, and
  downstream-type hint.
- **Status transition:** extraction `selected -> extracted_candidate`.
- **Persistence:** candidate store separate from canonical fragment revisions;
  retain all field-level excerpt/evidence links and generator output metadata.
- **Provenance:** human/AI origin per claim, prompt/model/run for AI, source
  excerpt IDs/hashes, transformations, confidence and explanation.
- **Allowed AI:** propose structure/classes/relations/questions. It cannot create
  domain review, approval, publication, selected action authority, or a
  verified evidence status.
- **Mandatory human control:** annotator sees and accepts/edits every proposed
  claim before draft creation.
- **Failure/recovery:** malformed/unsupported/fabricated references reject AI
  output; low confidence becomes a gap; manual entry remains available.
- **Audit:** generator/run/result hash, source ID allowlist, failure code; no raw
  prompt/result in general audit.
- **UI:** source and proposed claims side by side; origin badges, confidence,
  explanation, accept/edit/reject per claim, no “approve all as final.”
- **Backend:** `CandidateService`, `CandidateGenerator` port, structured codec,
  data-classification policy.

### Step 6: link evidence and perform CTA-style clarification

- **Paper basis:** `paper-defined` phase 6 asks about first indications,
  novice misses, alternative explanations, invalid/unsafe conditions, and
  escalation (`04_method.tex` lines 55--68). The expected pilot material
  includes a clarification log (`03_research_design.tex` lines 64--82).
- **Primary actor:** annotator asks; operator/domain participant answers.
- **Input:** candidate claims, missing/ambiguous fields, source excerpts, and
  generated/manual questions.
- **Output:** immutable `ClarificationRecord`, answers/source references, revised
  candidate claims, explicit alternatives, and `OpenQuestion`/`ReviewNeed`/
  `ValidationNeed` gaps.
- **Status transition:** candidate remains `extracted_candidate`; individual
  questions move open/answered/withdrawn, never silently disappear.
- **Persistence:** questions, answers, participants/roles, time, source channel,
  and claim/evidence links are append-only.
- **Provenance:** question origin, answer actor/role, recording/capture source,
  edit/transcription history, candidate version and affected claim IDs.
- **Allowed AI:** propose questions, summarize an answer as a candidate, and
  identify contradictions. AI cannot answer on the operator's behalf or close a
  gap without human evidence.
- **Mandatory human control:** answer attribution and semantic incorporation
  require human confirmation.
- **Failure/recovery:** unavailable operator leaves an explicit gap; conflicting
  answers become alternative/disputed evidence; sensitive answer is separately
  classified and may be withheld from AI/export.
- **Audit:** question/answer record IDs, actor/role, sensitivity, disposition;
  content stays in protected provenance.
- **UI:** question queue, mobile-friendly response capture, contradiction view,
  explicit “unknown/not available,” and visible source/answer attribution.
- **Backend:** candidate/clarification service, source service, provenance,
  authorization/redaction.

### Step 7: create or edit an immutable draft OSL fragment revision

- **Paper boundary:** `paper-defined` systems-paper allows an OSL modeller to
  author a provisional model with gaps before domain review
  (`systems-paper/tex/sections/osl_workflow_roles.tex` lines 14--18).
  `paper-repo` expects review before model-ready treatment. The combined
  product behavior below is `proposed-for-MSH`.
- **Primary actor:** OSL modeller/annotator.
- **Input:** selected candidate claims, explicit element/relation types, source/
  evidence references, language profile, gaps, and optional base revision.
- **Output:** immutable `StrategyFragmentRevision` in `draft` with stable logical
  fragment ID, unique revision/node/relation IDs, and provenance.
- **Status transition:** new fragment `none -> draft` or existing
  `draft/reviewed/rejected -> new draft revision`. Old content never changes.
- **Persistence:** repository transaction writes revision graph plus event;
  candidate remains preserved as derivation input.
- **Provenance:** actor/reason, base/candidate/source IDs, profile/version,
  element-level origin, command ID, expected aggregate revision and content hash.
- **Allowed AI:** only via candidate results; AI cannot invoke the draft command
  directly or become the recorded human editor.
- **Mandatory human control:** human deliberately selects/edits content and
  language profile before command submission.
- **Failure/recovery:** stale base produces conflict/diff; invalid references or
  duplicate IDs reject transaction; a semantically incomplete draft is allowed
  only with explicit gaps.
- **Audit:** fragment/revision/hash, actor, base, command result; no sensitive
  content.
- **UI:** structured editor with source trace, Decision/OperatorAction split,
  relation editor, gap fields, autosaved client draft distinct from committed
  revision, accessible validation summary.
- **Backend:** `FragmentService`, domain model, repository, provenance, registry.

This step can produce a provisional OSL draft before domain review, but it must
not label it model-ready, reviewed, approved, or published. That conservative
rule makes the paper order conflict visible instead of using “explicitly
bounded” as an approval bypass.

### Step 8: perform deterministic semantic validation

- **Paper basis:** OSL WF rules are `paper-defined` in systems-paper; the
  Notebook-to-OSL paper does not define semantic validation.
- **Primary actor:** deterministic validator invoked by modeller/system; no AI.
- **Input:** exact immutable draft revision/hash and language profile.
- **Output:** persisted `ValidationResult` and ordered findings with target IDs.
- **Status transition:** validation dimension
  `not_run -> pass/fail/incomplete/unsupported_profile`; fragment remains draft
  unless separately submitted for review.
- **Persistence:** result binds validator/rule/profile versions and input hash;
  rerun creates a new result, not an overwrite.
- **Provenance:** invoker, tool/version, rule-set hash, time, result hash.
- **Allowed AI:** explain findings in a clearly labeled view only; no rule
  decision or automatic repair.
- **Mandatory human control:** human decides edits; lifecycle policy requires
  relevant pass before review/approval.
- **Failure/recovery:** validator internal/resource failure is `incomplete`, not
  pass. Unsupported profile blocks review/publication; draft remains intact.
- **Audit:** validation ID/hash/status/rule counts; detailed sensitive messages
  remain access-controlled.
- **UI:** findings grouped by rule/target with source context, clear distinction
  between structural conformance and truth/safety, retry/manual navigation.
- **Backend:** semantic validator, registry, repository/provenance, query layer.

### Step 9: human domain review

- **Paper basis:** `paper-defined` phase 7 requires operator/domain-expert
  review and the systems paper defines domain-reviewed OSL as review of
  represented meaning, not operational validation.
- **Primary actor:** authenticated operator/domain reviewer; modeller prepares
  but cannot impersonate reviewer.
- **Input:** exact semantically eligible revision/hash, source/excerpt evidence,
  gaps, validation results, reviewer scope/role, and disclosure-safe view.
- **Output:** immutable `ReviewerDecision` with accepted/corrected/narrowed/
  rejected/disputed/pending dispositions, scope, rationale, element targets,
  unresolved blockers, time and reviewer.
- **Status transition:** `draft -> in_review -> reviewed` for a completed scoped
  decision, or `in_review -> rejected`. Correction/narrowing creates a new draft
  and does not transfer review automatically.
- **Persistence:** append review record and lifecycle event atomically; retain
  prior reviews.
- **Provenance:** authenticated reviewer identity/role/scope, exact content hash,
  source materials shown, decision/rationale, time, policy version.
- **Allowed AI:** prepare a comparison or explain fields; never submit a review,
  choose a disposition, or infer reviewer identity.
- **Mandatory human control:** entire decision is human-only and bound to the
  reviewed content.
- **Failure/recovery:** stale/changed revision rejects submission; absent source
  access renders an explicit incomplete review; reviewer conflict stays
  disputed; sensitive finding updates classification through a separate policy
  flow.
- **Audit:** review command allow/deny, reviewer/scope, revision/hash,
  disposition IDs; protect notes and source.
- **UI:** distraction-free review with source/interpretation split, per-element
  decisions, unresolved items, save-draft and explicit final submit, keyboard/
  screen-reader support, no preselected acceptance.
- **Backend:** `ReviewService`, lifecycle, authorization, repository,
  provenance, safe projections.

### Step 10: product approval

- **Paper boundary:** product approval is not defined by either paper.
  `proposed-for-MSH` adds it so domain review cannot silently become publication
  or authority.
- **Primary actor:** authenticated approver with configured scope and separation
  of duties.
- **Input:** exact unchanged reviewed revision/hash, accepted review coverage,
  passing validation, unresolved-blocker summary, audience/classification, and
  approval rationale.
- **Output:** immutable `Approval` bound to revision/profile/hash and policy.
- **Status transition:** `reviewed -> approved`; denial leaves reviewed state and
  records reason.
- **Persistence:** approval and event in one transaction.
- **Provenance:** approver identity/role/scope, review/result IDs, policy
  revision, content hash, time, rationale.
- **Allowed AI:** may summarize non-sensitive review material; cannot issue or
  recommend an authoritative decision without human evaluation and never signs.
- **Mandatory human control:** explicit human command; deployment may forbid
  self-approval.
- **Failure/recovery:** missing/incomplete review, failed/stale validation,
  sensitivity/audience conflict, changed content, or insufficient role rejects
  without changing state. New content requires new review.
- **Audit:** all allow/deny decisions with stable reason; no raw source.
- **UI:** approval checklist shows exactly what approval means and a warning
  that it is not operational/safety/execution authorization.
- **Backend:** `PublicationService` approval command, lifecycle, auth policy,
  repository/provenance.

### Step 11: publish a design-time fragment or export projection

- **Paper boundary:** the papers do not define publication or activation.
  `proposed-for-MSH` supports publication only. Runtime activation is a non-goal.
- **Primary actor:** authenticated publisher; automated retry may complete the
  same already-authorized idempotent command.
- **Input:** exact approved revision/hash, audience/site scope, publication
  metadata, and optional requested export formats.
- **Output:** immutable `PublicationRecord` and read-only published projection;
  optional versioned exports.
- **Status transition:** `approved -> published`.
- **Persistence:** publication/event atomically; export artefacts have separate
  immutable records and hashes.
- **Provenance:** publisher, approval/review/validation IDs, profile/version,
  audience/policy, content hash, time, export producer versions.
- **Allowed AI:** none in the transition. AI may later explain a published
  projection under read policy.
- **Mandatory human control:** initial publication command and audience choice
  are human-controlled.
- **Failure/recovery:** storage/export failure leaves approval intact and no
  false published state; retry is idempotent. Partial external publication must
  be reported and reconciled before success.
- **Audit:** publication/export success/denial, IDs/hashes/audience; no content.
- **UI:** immutable version badge, source/provenance/history, download formats,
  visible non-executable boundary, no “activate” button.
- **Backend:** publication service, query/projection, export service, repository,
  provenance.

Published OSL may feed read-only operator support, comparison, or candidate
downstream trace views. It may not directly feed the current recommender
artifact or support-card services until those consumers are changed to preserve
revision, evidence, and authority boundaries.

### Step 12: record feedback, revise, supersede, or deprecate

- **Paper boundary:** post-candidate feedback/lifecycle is not defined in
  `paper-repo`. `proposed-for-MSH` adds a durable loop.
- **Primary actor:** operator/researcher records source/feedback; modeller
  revises; reviewer/approver/publisher govern later transitions.
- **Input:** published revision reference, new source artefact/excerpt,
  observed-action claim, actual outcome/feedback, correction, conflict, or
  policy/deprecation reason.
- **Output:** separate immutable `FeedbackRecord`/evidence, optional new draft
  revision, and later supersession/deprecation event.
- **Status transition:** feedback does not change published content.
  `published -> deprecated` by explicit command, or `published -> superseded`
  only when a replacement revision is separately published.
- **Persistence:** append feedback/source/provenance and revision lineage; retain
  prior exports and review.
- **Provenance:** actor/source/time/context, expected-versus-observed distinction,
  affected revision/elements, validation of the feedback claim, reason.
- **Allowed AI:** cluster/summarize/propose a revision candidate; cannot declare
  outcome true, deprecate, supersede, approve, or publish.
- **Mandatory human control:** confirm feedback attribution and every lifecycle
  transition; new revision repeats validation/review/approval/publication.
- **Failure/recovery:** uncertain outcome stays an unverified claim; conflicting
  feedback becomes separate evidence/conflict; withdrawn source restricts
  future exposure according to policy without rewriting history.
- **Audit:** feedback and lifecycle IDs/hashes/reasons; content protected.
- **UI:** expected outcome versus observed feedback side by side, lineage graph,
  compare versions, create-revision action, deprecation/supersession rationale.
- **Backend:** feedback/source service, fragment/review/publication services,
  repository/provenance/projections.

## Paper status and schema inconsistencies

These issues remain visible requirements, not silent implementation choices.

| ID | `requires-research-clarification` | Evidence | Conservative MSH treatment |
| --- | --- | --- | --- |
| WQ-01 | Validation order conflicts. | `outline.md` lines 35--42 creates OSL candidates before review; `04_method.tex` lines 70--76 reviews before model-ready transformation. | Allow non-canonical extracted candidate/provisional draft before review; forbid approved/model-ready/published treatment before human review. |
| WQ-02 | Illustrative case emits OSL despite an unresolved validation need. | `06_illustrative_case.tex` lines 42 and 49--65. | Treat case as a draft fixture only; unresolved needs remain gaps and block approval per policy. |
| WQ-03 | “Validated or explicitly bounded” is undefined and could bypass R5. | `04_method.tex` line 76 versus `03_research_design.tex` line 38. | “Bounded” permits a provisional draft with explicit gaps, not approval/publication. |
| WQ-04 | Two incompatible status sets. | `capture-schema.md` lines 33--42 versus `05_annotation_schema.tex` lines 83--106. | Do not import either as one enum; use orthogonal dimensions and retain legacy source status. |
| WQ-05 | “Model-ready” is both pre- and post-transformation. | `05_annotation_schema.tex` line 105 versus `04_method.tex` lines 74--76. | Derive named eligibility predicates; store no ambiguous boolean. |
| WQ-06 | Field names drift. | `validation_need`/`candidate_osl_type` in `capture-schema.md` lines 28--31 and case lines 42--44 versus `validation_question`/`candidate_model_type` in manuscript schema. | Import aliases into a report; require human mapping to canonical types. |
| WQ-07 | Decision is used in selection/figure but absent from annotation schema. | `04_method.tex` line 49 and workflow figure versus `05_annotation_schema.tex` lines 53--79. | Add separate proposed Decision and OperatorAction candidates; do not collapse them. |
| WQ-08 | Action mixes actual, considered, and advised behavior. | `05_annotation_schema.tex` action definition around line 68. | Require explicit modality/claim type and separate recommendation/occurrence from represented OperatorAction. |
| WQ-09 | Raw text is truncated in the illustrative annotation. | Original `06_illustrative_case.tex` lines 14--16 versus `raw_text` at line 33. | Store original immutable content and reference excerpts; never replace it with an illustrative summary. |
| WQ-10 | Candidate example omits claimed source/review trace. | Candidate at `06_illustrative_case.tex` lines 53--64. | Product contract requires source/excerpt, generation, validation and review references. |
| WQ-11 | Confidence concepts differ. | raw capture confidence, fragment uncertainty, review confidence, and illustrative OSL confidence. | Store capture fidelity, claim uncertainty, evidence confidence/basis, and review disposition separately. |
| WQ-12 | `sensitive` is presented as a review outcome/state. | `04_method.tex` line 72 and `05_annotation_schema.tex` status table. | Treat confidentiality as orthogonal policy; review can be both accepted and sensitive. |
| WQ-13 | Required live-capture fields are unclear. | Minimal interface in `04_method.tex` lines 43--45 versus broader raw schema. | Require only policy/session, ID/time/content at capture; enrich context later without mutating source. |
| WQ-14 | OSL is only one downstream target. | `03_research_design.tex` lines 89--93 and `04_method.tex` line 76. | Keep upstream candidate schema representation-neutral; create an OSL draft through an explicit mapping step. |

## AI authority contract for the workflow

`paper-defined`: the paper assigns interpretation and validation to humans and
does not evaluate AI. Therefore all AI behavior is `proposed-for-MSH` and must
meet these invariants:

- AI may suggest source segments, relevance categories, field values,
  relationships, missing-information questions, comparisons, and explanations.
- Every suggestion identifies the exact authorized excerpt and generator
  provenance.
- AI output is stored as a candidate, never a canonical fact or revision.
- AI cannot mark evidence accepted/verified, submit reviewer decisions, approve,
  publish, deprecate, supersede, create operational bindings, execute
  OperatorAction, or assign compute/storage/artifact/federation authority.
- AI cannot request sources outside the provided allowlist or override
  confidentiality/locality policy.
- Manual capture, annotation, validation-navigation, review, and publication
  remain usable when AI is absent or fails.

## Workflow-level acceptance criteria

A later implementation of this workflow is incomplete unless tests demonstrate:

- raw source bytes/text survive every annotation, review, migration, and export
  unchanged;
- one fragment can cite multiple notes/excerpts and one excerpt can support
  multiple explicitly targeted claims;
- Decision, candidate response, selected response, recommendation,
  OperatorAction, expected outcome, action occurrence, and observed outcome are
  not interchangeable;
- every AI contribution remains an attributed candidate and cannot invoke
  lifecycle commands;
- semantic validation cannot become review, and review cannot become approval;
- approval/publication bind an exact immutable revision and grant no operational
  authority;
- correction/narrowing creates a new revision and forces revalidation/review;
- sensitive material is filtered from lists, search, errors, audit, AI context,
  and export by server-side policy;
- failures at every step leave earlier durable stages consistent and retryable;
- no workflow path provides runtime activation or execution in the first
  implementation.

The detailed object contracts are specified in
`05_data_model_and_contracts.md`. Exact route, service, repository, and UI file
changes are assigned later in `06_repository_file_plan.md` and
`07_api_ui_and_user_journeys.md`.
