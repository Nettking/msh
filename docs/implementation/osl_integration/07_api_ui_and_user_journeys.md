# OSL API, UI, and user journeys

## Purpose and source basis

This document plans how MSH exposes the versioned OSL language model and the
Notebook-to-OSL product workflow. The browser and HTTP representations are
adapters; the canonical model, lifecycle and provenance remain in the OSL
subsystem planned in [03_target_architecture.md](03_target_architecture.md) and
[05_data_model_and_contracts.md](05_data_model_and_contracts.md).

| Repository | Analyzed commit | Use here |
|---|---|---|
| `Nettking/systems-paper` | `ff098ce52f15b489b6a07d5b55c6c788d862e3be` | semantic distinctions and design-time boundary traced in `01_language_requirements.md` |
| `Nettking/paper-repo` | `abe3fbcddee590c3f399b06f63cb329e8615977c` | Notebook-to-OSL stages and evidence retention traced in `04_notebook_to_osl_workflow.md` |
| `Nettking/msh` | `f580c71f7269643a077cc7e7db8ba9bf6050bb6a` | Flask routes, services, templates, AI runtime and responsive test conventions |

The markers `paper-defined`, `existing-in-MSH`, `proposed-for-MSH` and
`requires-research-clarification` are used as defined in
[00_scope_and_sources.md](00_scope_and_sources.md).

## Non-negotiable exposure boundaries

- `paper-defined`: preserve Situation/Observation/Evidence, assessment or
  Hypothesis, Decision, candidate and selected OperatorAction, rationale,
  expected Outcome, Result/feedback and explicit gap distinctions. See
  `systems-paper/evaluation/osl-semantic-contract.md` sections 5-11 and
  `systems-paper/sysml/osl-core.sysml`.
- `paper-defined`: the analyzed systems-paper profile is design-time. Its
  semantic contract section 13 excludes runtime activation, execution, factual
  truth, causal proof, safety assurance and automatic recommendation.
- `proposed-for-MSH`: HTML, JavaScript and API payloads are not a source of
  truth. They can request application commands and render authorized
  projections only.
- `proposed-for-MSH`: a checkbox, AI result, support-card confirmation,
  `reusable_strategy` value, syntactically successful import or SysML export
  cannot constitute review, approval, publication or operational authorization.
- `proposed-for-MSH`: published means an authorized knowledge artefact. It does
  not mean active, safe, executable or authorized for action.
- `proposed-for-MSH`: no initial route executes OperatorAction, invokes the
  intervention runner, dispatches a capability job, chooses a provider, grants
  storage/compute authority or creates a machine-control binding.
- `proposed-for-MSH`: every mutation requires authenticated server-derived
  actor/scope, object authorization, CSRF protection for browser sessions,
  idempotency key and expected revision/content hash.
- `proposed-for-MSH`: source bytes, accepted excerpts and prior fragment
  revisions are immutable. A submitted form can only create a new object,
  command result or revision.
- `proposed-for-MSH`: manual capture/edit/review remains functional when AI is
  absent. AI is a candidate generator and explainer with no lifecycle port.

## Existing MSH surface and gaps

### Operator-strategy routes

At the analyzed MSH commit,
`catalog/flask_app/operator_strategy_routes.py` exposes the following legacy
surface:

| Existing route/source | Current behavior | OSL planning consequence |
|---|---|---|
| `GET /operator-strategies`, lines 23-50 | counts `captured`, `structured` and `reusable` notes | `existing-in-MSH` navigation pattern is reusable; state vocabulary is not the OSL lifecycle |
| `GET /operator-strategies/capture`, lines 53-69 | shows a statement form and recent raw text | `existing-in-MSH` source-first pattern is useful; raw recent text must not leak in an unscoped list |
| `GET /operator-strategies/review`, lines 72-86 | lists notes for structuring | `existing-in-MSH` queue pattern is useful; assignment and authorization are missing |
| `GET /operator-strategies/structure/<record_id>`, lines 89-101 | combines source and derived fields | retain source visibility, but split evidence, interpretation, Decision and Action |
| `POST /operator-strategies/save`, lines 104-112 | writes one mutable note | must not back canonical capture: no actor binding, CSRF, bounds, revision or idempotency |
| `POST .../structure` and `.../outcome`, lines 115-134 | overwrites fields | canonical edits and feedback append new revisions/records |
| `POST .../reusable`, lines 137-146 | directly toggles reusable status | never map to review, approval, publication or paper maturity |
| `POST /operator-strategies/delete/<record_id>`, lines 149-157 | destructively deletes | no equivalent for immutable source evidence or revisions |

`catalog/flask_app/operator_support_routes.py:26-113` exposes adjacent support
confirmation, first-part, quality outcome, machine-note, recommender and export
routes. These are `existing-in-MSH` product features but do not supply OSL
authority. In particular, `GET /osl-export` and `POST /osl-export/run` at lines
84-94 produce the current legacy export; success does not validate language
semantics.

### Templates and interaction conventions

- `existing-in-MSH` `operator_capture.html:3-38` provides a compact
  raw-statement capture pattern; lines 54-65 disclose recent raw statements.
- `existing-in-MSH` `operator_review.html:14-46` provides a queue, but also
  offers direct reusable/delete actions that cannot be copied.
- `existing-in-MSH` `operator_structure.html:10-52` keeps the original visible
  while editing; line 39 labels a combined "Decision / strategy action" field,
  which conflicts with OSL separation.
- `existing-in-MSH` `strategy_comparison.html:3-31` offers a useful comparison
  layout over legacy, loose fields.
- `existing-in-MSH` `assist.html:3-38` communicates that assistance does not
  decide, but its confirmation is not authenticated review or approval.
- `existing-in-MSH` `base.html:2,5,51-57` and
  `catalog/flask_app/tests/test_mobile_layout_contract.py:4-39` establish
  viewport/navigation/responsive conventions.
- `existing-in-MSH` `osl_export.html:3-19` previews the heuristic export and
  overstates the confidence of inferred evidence status; it is a legacy surface.

### Security and degraded-state gaps

- `existing-in-MSH` `catalog/flask_app/app.py:37` defaults `SECRET_KEY` to
  `msh-dev` and line 125 binds to `0.0.0.0` by default.
- `existing-in-MSH` the operator routes have no authenticated human principal,
  role/scope authorization or CSRF boundary.
- `existing-in-MSH` `operator_strategy_routes.py:67,85` passes backend storage
  paths to templates; OSL projections must never expose them.
- `existing-in-MSH` `_source_inventory()` at lines 16-20 turns any exception
  into an empty list, conflating failure with "no data".
- `existing-in-MSH` `capability_onboarding_routes.py:36-60,122-143` and
  `provider_federation_routes.py:39-52,94-105,145-147` contain useful CSRF and
  server-bound-context patterns. Their capability/federation permissions are
  not OSL semantic authority.
- `existing-in-MSH` federation projection models distinguish empty, degraded
  and repair states (`catalog/federation/projections/models.py:249-390`); OSL
  views should reuse the explicit-state convention, not federation meaning.

## Planned adapter layout

Use the file boundaries committed in
[06_repository_file_plan.md](06_repository_file_plan.md):

- `catalog/flask_app/osl_routes.py`: one Blueprint containing server-rendered
  routes and a clearly separated `/api/osl/v1` JSON section. Split into
  `osl_api_routes.py` later only if size/ownership requires it, with no duplicated
  policy logic.
- `catalog/flask_app/services/osl_application_service.py`: translate bounded
  request DTOs into domain commands/queries, bind server principal, and map safe
  errors. It must not infer lifecycle or import SQLite/AI provider internals.
- `catalog/osl/query_service.py` plus projections/redaction: authorized read
  model; Flask does not query tables directly.
- `catalog/osl/*_service.py`: capture, candidate, fragment, review, approval,
  publication, feedback, import and export command owners.
- `catalog/ai/osl_candidate_adapter.py`: optional adapter to the existing
  versioned AI runtime. It implements the candidate inference port only.

`requires-research-clarification`: MSH has no adequate authenticated human
identity mechanism at the analyzed commit. Read-only redacted routes may be
built behind an accepted principal adapter, but production-capable mutation
routes remain disabled until authentication, secure session configuration,
authorization and CSRF are established outside the OSL domain.

## Server-rendered route plan

All URL identifiers are opaque domain IDs. No filesystem path, source label,
operator name or raw text appears in a URL/query string.

### Read-only projections

| Method and route | Authorized projection and behavior |
|---|---|
| `GET /osl` | overview counts by lifecycle, assigned work, degraded notices and one safe next action; counts are scope-filtered |
| `GET /osl/sources` | metadata-only source list; content requires a separate authorized detail/reveal |
| `GET /osl/sources/<source_id>` | source identity, classification, provenance, revisions and permitted excerpts; raw content withheld by default |
| `GET /osl/candidates` | candidate queue filtered by origin, state, assignee and unresolved issues |
| `GET /osl/candidates/<candidate_id>` | candidate, exact excerpts, field origins, explanation, gaps and permitted actions |
| `GET /osl/fragments` | fragment list by lifecycle, paper maturity, language profile and validation status as distinct filters |
| `GET /osl/fragments/<fragment_id>` | current authorized projection, never an editable source-of-truth object |
| `GET /osl/fragments/<fragment_id>/versions/<revision_id>` | one immutable revision with exact profile/hash |
| `GET /osl/fragments/<fragment_id>/history` | revision, validation, review, approval, publication, feedback and deprecation events |
| `GET /osl/fragments/<fragment_id>/compare?left=<revision>&right=<revision>` | semantic comparison after independent authorization of both revisions |
| `GET /osl/fragments/<fragment_id>/provenance` | redacted source-to-excerpt-to-candidate-to-revision lineage |
| `GET /osl/validation-runs/<validation_id>` | rule/profile/validator-bound findings for one exact hash |
| `GET /osl/imports/<import_id>` | dry-run/result disposition and diagnostics; no implied canonical write |
| `GET /osl/exports` | authorized export history, representation/profile and diagnostics |
| `GET /osl/exports/<export_id>` | exact input revision, adapter version, hash, redaction manifest and outcome |
| `GET /osl/exports/<export_id>/download` | authorization rechecked before immutable artefact download |

### Capture and excerpt commands

| Method and route | Command semantics |
|---|---|
| `GET /osl/sources/new` | capture form with scope, consent/classification and immutable-source explanation |
| `POST /osl/sources` | create capture session/source revision and immutable blob; never create an OSL claim |
| `GET /osl/sources/<source_id>/excerpts/new` | select or enter an excerpt against one exact source revision |
| `POST /osl/sources/<source_id>/excerpts` | create immutable selector/hash and provenance |
| `POST /osl/sources/<source_id>/corrections` | create a new related source revision/artefact; never replace original bytes |

There is no source-byte update or delete route. Retention/privacy action is a
separate policy problem marked `requires-research-clarification`.

### Candidate and draft commands

| Method and route | Command semantics |
|---|---|
| `POST /osl/excerpts/<excerpt_id>/candidate-jobs` | request bounded AI/deterministic candidate extraction after disclosure and policy checks |
| `GET /osl/candidate-jobs/<job_id>` | safe queued/running/completed/failed projection; no prompt/provider internals |
| `POST /osl/candidates/<candidate_id>/rejections` | append human rejection/reason without deletion |
| `POST /osl/candidates/<candidate_id>/drafts` | explicit human acceptance/edit creates a new draft revision |
| `POST /osl/excerpts/<excerpt_id>/drafts` | manual authoring path that does not require AI |
| `GET /osl/fragments/<fragment_id>/edit` | edit one authorized draft base revision |
| `POST /osl/fragments/<fragment_id>/versions` | append a draft revision with expected revision/hash; no overwrite |

### Validation, review, approval, publication and feedback

| Method and route | Command semantics and authority |
|---|---|
| `POST .../versions/<revision_id>/validation-runs` | deterministic validation of exact profile/hash; result is not approval |
| `GET .../versions/<revision_id>/review` | exact immutable content, evidence access and assigned review scope |
| `POST .../versions/<revision_id>/review-decisions` | append accept/request-changes/reject within human review scope |
| `GET .../versions/<revision_id>/approval` | show exact reviewed revision, validation and unmet prerequisites; no editing |
| `POST .../versions/<revision_id>/approval-decisions` | authorized human approve/reject; server actor and separation policy enforced |
| `POST .../versions/<revision_id>/publications` | distinct publisher command for exact approved revision/audience |
| `POST /osl/fragments/<fragment_id>/deprecations` | append reasoned deprecation; retain content and history |
| `POST /osl/fragments/<fragment_id>/supersessions` | relate independently created replacement; no rewrite |
| `POST /osl/fragments/<fragment_id>/feedback` | append reported Result/feedback and evidence; expected outcome stays unchanged |

Explicit command routes are intentionally preferred to a generic
`PATCH status` endpoint: each transition has different preconditions, actor
permissions, events and failure recovery.

### Import and export

| Method and route | Command semantics |
|---|---|
| `GET /osl/imports/new` | supported profile/media/size explanation and upload/paste form |
| `POST /osl/imports/validation-runs` | bounded parse and dry run; no source, draft, approval or publication side effect |
| `POST /osl/imports/<import_id>/drafts` | explicit authorized disposition creates only sources/candidates/drafts |
| `GET /osl/exports/new` | select exact eligible revision, audience and representation |
| `POST /osl/exports` | create immutable audited projection with diagnostics |

Imported lifecycle claims are untrusted external metadata. Export success never
increases semantic validity or lifecycle authority.

## Versioned JSON API

Use `/api/osl/v1` for the first HTTP API. HTTP API `v1`, OSL language profile,
canonical document contract, repository schema and exporter version are
independent axes. Example resources mirror the HTML commands:

- `POST /api/osl/v1/source-artifacts`;
- `POST /api/osl/v1/source-artifacts/<source_id>/excerpts`;
- `POST /api/osl/v1/excerpts/<excerpt_id>/candidate-jobs` and
  `GET /api/osl/v1/candidate-jobs/<job_id>`;
- `GET /api/osl/v1/candidates` and
  `POST /api/osl/v1/candidates/<candidate_id>/drafts`;
- `GET /api/osl/v1/fragments/<fragment_id>/versions/<revision_id>`;
- `POST /api/osl/v1/fragments/<fragment_id>/versions`;
- exact-revision `validation-runs`, `review-decisions`,
  `approval-decisions` and `publications` subresources;
- `POST /api/osl/v1/fragments/<fragment_id>/feedback`;
- `POST /api/osl/v1/imports/validation-runs` and explicit import disposition;
- `POST /api/osl/v1/exports` and `GET /api/osl/v1/exports/<export_id>`.

### API contract rules

- Require an explicit supported language/profile identifier on fragment import
  and creation. Never choose "latest" for a write.
- Return API schema, language profile, canonical contract version and aggregate
  revision separately.
- Use stable error codes and a safe request/correlation ID; do not expose stack,
  SQL, paths, source excerpts, prompts, provider endpoints or hidden IDs.
- Return `201` for created immutable resources and `202` for an accepted queued
  candidate job.
- Use `400` for malformed request envelopes, `401` for missing authentication
  where disclosure is safe, privacy-preserving `403`/`404` per policy, `409`
  for lifecycle/revision/idempotency conflicts, `413` for bounds, `415` for
  media type and `422` for parseable contract/semantic violations.
- Use `ETag`/`If-Match` or an explicit expected aggregate revision on every
  command against a changing head. The exact immutable target revision/hash is
  still required for validation, review, approval and publication.
- Require `Idempotency-Key` on create and lifecycle commands. Same key and
  payload replays; same key and different fingerprint returns conflict.
- Reject client-controlled actor, role, reviewer, approver, signer, site scope,
  authority, publication, evidence-verification and action-selection fields.
- Apply field/collection authorization before serialization. Filtering after a
  full response object exists in a browser is not a security boundary.
- Set private/no-store cache policy on raw source, candidate, review, approval
  and private provenance responses.

## Templates, partials and assets

Create server-rendered templates under `catalog/flask_app/templates/osl/` as
the phases in `06_repository_file_plan.md` land:

| Template | Primary projection/action |
|---|---|
| `index.html` | scoped overview and next work |
| `source_list.html`, `source_capture.html`, `source_detail.html` | source metadata/capture/explicit reveal |
| `excerpt_create.html` | exact source revision and selector |
| `candidate_queue.html`, `candidate_detail.html`, `candidate_review.html` | origin/provenance, manual/AI suggestion review |
| `fragment_list.html`, `fragment_detail.html`, `fragment_edit.html` | lifecycle-filtered read and typed draft edit |
| `review.html` and `approval.html` | separate exact-revision human decisions |
| `history.html`, `compare.html`, `provenance.html` | immutable time/semantic/lineage projections |
| `validation_detail.html` | syntax/contract/semantic/incomplete findings |
| `import_export.html` and export detail partial | dry run, compatibility, eligibility and diagnostics |

Reusable partials should have one responsibility:
`_status_badge.html`, `_source_reference.html`,
`_provenance_timeline.html`, `_validation_summary.html`,
`_review_decision.html`, `_ai_disclosure.html`,
`_ai_origin_badge.html`, `_empty_state.html`,
`_degraded_notice.html`, `_revision_conflict.html` and
`_redacted_content.html`. Status labels must distinguish language maturity,
workflow state, validation outcome, evidence review and publication.

`catalog/flask_app/static/css/osl.css` owns responsive/high-contrast styling.
`catalog/flask_app/static/js/osl-editor.js` is optional progressive enhancement
for typed relations/comparison only. Server validation, policy, lifecycle and
persistence must work without or despite JavaScript.

`proposed-for-MSH`: sensitive OSL pages load no new third-party analytics,
fonts or scripts. `base.html` currently includes CDN-hosted assets; the
implementation must self-host necessary assets or use a sensitive-page base
that omits unnecessary third-party requests.

## Information architecture and semantic labels

Add an "OSL" or "Operator knowledge" entry under the existing Knowledge
navigation only when its landing route exists. It separates:

1. sources;
2. excerpt/annotation and candidate extraction;
3. draft fragments;
4. semantic validation;
5. review;
6. approval;
7. published fragments;
8. history, provenance and feedback;
9. import/export.

Every fragment page shows, without relying on color:

- logical fragment ID and exact immutable revision;
- OSL language profile and product contract version;
- workflow state, paper maturity and validation result as separate labels;
- human, tool, import and/or AI origin;
- validation time/ruleset and any incomplete checks;
- review scope/decision and authorized identity visibility;
- approval/publication target and audience;
- evidence/source links subject to independent access;
- parent/supersedes/superseded-by links;
- an explicit statement that publication does not authorize execution.

Typed sections visibly separate Observation/Evidence, assessment/Hypothesis,
Decision, candidate/selected OperatorAction, recommendation, expected Outcome,
reported Result/feedback, human review, human approval and any future
operational binding. "Action selected in a representation" must never be
rendered as "action authorized" or "action performed".

## Actor and authority matrix

This is a `proposed-for-MSH` product authorization model, not paper-defined
semantics:

| Actor/capability | Read | Capture/candidate | Draft | Review | Approve | Publish/export | Operational action |
|---|---|---|---|---|---|---|---|
| published viewer | scoped published projection | no | no | no | no | export only if separately granted | no |
| knowledge capturer | scoped sources | capture; request/author candidate | optional explicit permission | no | no | no | no |
| fragment editor | scoped source/candidate/fragment | reject/correct candidate | append draft revisions | no implied right | no | no | no |
| reviewer | exact assigned revision/evidence | no implied right | requests new draft, does not edit target | scoped decision | no implied right | no | no |
| approver | exact reviewed revision/findings | no | cannot edit target | read | human decision | no implied publish | no |
| publisher/curator | approved safe projection | no | no | no | no implied approval | publish/audience; export if granted | no |
| AI/provider | explicitly allowlisted excerpt only | returns candidate data | no | no | no | no | no |
| MSH administrator | operational administration | no semantic permission by default | no | no | no | policy-dependent export only | no OSL-derived right |

`requires-research-clarification`: decide whether reviewer, approver and
publisher must be different people, whether small sites may combine human roles,
and which source scopes (deployment/site/machine/project/notebook/team) are
needed. The domain must support separation even when local policy permits role
combination.

## Empty, missing-context and degraded states

| State | User-facing behavior | Mandatory safety behavior |
|---|---|---|
| no sources | explain immutable capture and classification | do not fabricate demo content or call it failure |
| no candidates | offer manual extraction | do not imply AI examined or validated all sources |
| no fragments | link to manual/candidate draft flows | do not display candidates as fragments |
| source redacted | show permitted metadata and reason category | no leak through title, URL, counts, HTML comments or adjacent IDs |
| missing source/context | show unresolved-reference/context finding | block review/approval/publication according to explicit rule/policy |
| AI unavailable | show manual path and retry status | capture/edit/deterministic validation/review remain usable |
| validator unavailable/incomplete | explicit degraded/incomplete result | block new approval/publication; never reuse stale result silently |
| unsupported profile | preserve bounded raw input/quarantine and migration guidance | no automatic normalization to current profile |
| repository read error | retry/support notice without path/partial certainty | never render partial data as complete or empty |
| repository write error | retain an explicitly unsaved local form where safe | never claim success or fabricate an ID |
| revision conflict | current revision plus semantic comparison/reapply path | no last-write-wins |
| expired session/CSRF | preserve safe local draft, require authentication | execute no command |
| import failure | bounded diagnostics and original hash | no candidate/draft/approval/publication side effect |
| export adapter failure | retry/diagnostics; canonical revision unchanged | no partial artefact presented as valid |
| external evidence unavailable | unresolved/temporarily unavailable | absence is not negative evidence |
| forbidden object | policy-safe not-found/forbidden page | no existence oracle through detail or timing class |

## Security and data-leakage controls

### Identity, request integrity and object policy

- Bind actor, roles and deployment/site scope on the server. Session presence,
  displayed operator name, node identity, capability ownership or federation
  membership is not semantic authority.
- Recheck object-level access on every source, excerpt, candidate, revision,
  review, history, comparison and download. Fragment access does not imply
  source access.
- Use CSRF for every cookie-authenticated mutation, secure/HTTP-only/SameSite
  cookies, expected revision/hash and per-form idempotency key.
- No GET has side effects. Bound content length, field count, graph size and
  parsing work before storage or AI calls.
- Record allowed/denied lifecycle commands with safe IDs/reason codes, not raw
  source or protected reviewer text.

### Sensitive content

- Lists expose metadata by default. Raw source has an explicit reveal boundary.
- Never put source text in page titles, URLs, breadcrumbs, flash messages,
  logs, metrics labels, audit summaries or exception detail.
- Never expose `records_path`, database/blob/export paths, prompts, provider
  credentials/endpoints or federation namespaces.
- Escape all source, candidate, rationale, import, validator, review and
  model-generated text in HTML/attributes/download filenames/SysML adapters.
- Treat notebook content as prompt-injection-capable untrusted data.
- Export excludes raw source and private reviewer notes unless each inclusion is
  explicit and separately authorized.
- Apply authorization independently to both sides of a comparison and every
  provenance edge; omitted data uses a safe withheld state.
- No third-party analytics on OSL screens; sensitive responses are not shared
  cacheable.

## Explaining AI suggestions

`existing-in-MSH`: the current AI explainer is read-only/non-operational
(`docs/ai_explainer.md:3,15,49-52,92,106,147` and
`catalog/ai/prompts.py:3-13`). Its repository index excludes `data`
(`catalog/ai/repo_index.py:12-18,43-57`), so it is not an existing
Notebook-to-OSL implementation.

Every AI candidate view shows:

- "AI-generated candidate — not reviewed or approved";
- exact permitted source excerpt identity;
- each AI-proposed field and each later human edit;
- model/provider and prompt-template version subject to safe disclosure;
- generation time/request identity and generator provenance;
- parser/semantic findings, missing context and assumptions;
- confidence/uncertainty as generator output, not factual confidence;
- a plain explanation of why text was mapped to each concept;
- reject, correct, ignore and manual-draft controls.

Never show "verified" because output parsed, "source-backed" because a free-text
evidence field exists, "approved by AI", or automatic publish/activate/execute.
Provider failure must not silently regenerate through a different locality or
provider with a different disclosure policy.

## Accessibility and mobile use

New surfaces target WCAG 2.2 AA as a `proposed-for-MSH` product requirement:

- one descriptive heading and logical hierarchy; full labels, not placeholders;
- semantic `fieldset`/`legend` groups for evidence, Decision, Action, review and
  approval;
- error summary receives focus and fields reference errors with
  `aria-describedby`;
- queued/completed/failed AI and validation status use a restrained live region;
- lifecycle, validation, origin and diffs are not color-only;
- full keyboard path, skip link and visible focus;
- source line breaks/long IDs wrap without viewport overflow;
- diff/provenance has a textual ordered-list alternative;
- upload/excerpt selection does not require drag-and-drop;
- reduced motion and usable touch targets;
- at 320 CSS pixels, comparison becomes sequential "A / B / changes",
  provenance becomes an ordered list, and validation tables become labeled
  cards or a named scroll region;
- sticky actions never cover focus or validation feedback;
- core flow works without JavaScript.

`base.html:2` hard-codes `lang="en"`. `requires-research-clarification`:
decide UI locales and terminology translation. OSL identifiers/serialized tokens
remain profile-defined even when operator labels are localized.

## Concrete user journeys

### Journey 1: unchanged notebook text to source and excerpts

1. A knowledge capturer opens `GET /osl/sources/new`. The page explains
   `paper-defined` capture purpose and `proposed-for-MSH` immutability,
   classification, consent and the fact that capture does not create a strategy.
2. The user pastes text or uploads an allowed media type and supplies context
   known at capture time. UI does not require OSL concepts yet.
3. Before `POST /osl/sources`, the page states whether remote AI could ever see
   this classification; AI processing is not implied by capture.
4. The service writes exact bytes/hash, capture session revision, actor, received
   time and protected content reference atomically. The UI returns a source ID,
   not the filesystem path.
5. The source detail renders permitted metadata; content is revealed only after
   an explicit access check.
6. The user selects a bounded passage against the exact source revision and
   confirms selector/text/hash. Creating the excerpt never changes the source.

Recovery:

- unsupported type/encoding/size rejects before a partial canonical record;
- duplicate content produces an explicit authorized duplicate/link decision,
  not hidden deduplication;
- repository failure reports "not saved" and preserves a local form only where
  its classification permits;
- a correction creates new source lineage;
- loss of access fails closed without revealing an existing duplicate.

### Journey 2: excerpt to AI-assisted or manual candidate

1. An authorized editor opens an excerpt and chooses "write manually" or
   "request AI candidate".
2. The AI path shows provider locality, exact data category, purpose, retention
   policy, model/template identity and "unverified candidate" status before the
   request.
3. The candidate job records queued/running/completed/failed independently of
   fragment lifecycle. Only allowlisted excerpt content is sent.
4. On completion, the candidate page places exact source beside proposed
   Situation/Observation/Hypothesis/Decision/OperatorAction/Outcome mappings.
   Each inferred field, assumption, missing context and source reference is
   individually attributed.
5. A human may reject with reason, edit into a draft, create a separate manual
   draft, or leave unresolved. Rejection preserves candidate/provenance.
6. Accepting/editing issues a human draft command. The AI result itself cannot
   invoke it and model confidence does not become evidence confidence.

If AI is unavailable, the excerpt and manual path remain. The degraded state
must not imply that candidate extraction or source validation failed.

### Journey 3: draft authoring and semantic validation

1. The edit view loads one immutable base revision and its expected aggregate
   revision.
2. It visibly separates source/evidence, interpretation, Decision, candidate or
   selected Action, rationale, conditions/context, expected Outcome and gaps.
3. Save appends a new draft revision with human/tool field provenance. It never
   overwrites the base.
4. A `409` conflict shows the submitted values, current revision and semantic
   comparison. The user deliberately reapplies or discards changes.
5. Validation runs against the exact new content hash/profile. The result
   separates malformed contract, semantic error, warning, missing context,
   external reference unavailable and validator-incomplete states.
6. Findings link to exact elements/relations and relevant authorized source, but
   validation cannot change the graph or submit review.

Any future autosave must use the same expected-revision/idempotency boundary and
must be clearly labelled as a draft, not a committed reviewed revision.

### Journey 4: review, approval and publication

1. An editor submits an exact draft revision for review after required
   validation. The assignment records review scope and policy version.
2. The reviewer sees immutable content, authorized evidence, findings, source
   gaps and field origins; they accept, request changes or reject within scope.
3. "Request changes" creates work for a new draft. The reviewer cannot edit the
   reviewed bytes inside the decision.
4. Any new content revision makes prior validation/review inapplicable to that
   revision; history remains.
5. An authorized human approver sees the exact unchanged reviewed revision,
   content hash, scope coverage and remaining blockers. They approve or reject;
   there is no edit or execute control.
6. A separately authorized publisher selects the exact approved revision and
   audience, then appends a publication record.
7. The published page is immutable, links history/provenance as authorized and
   states "publication does not authorize or perform an operator action".

Stale hash, missing validation, incomplete review scope, separation-of-duties
failure, session expiry or repository failure leaves the earlier valid state
unchanged and supplies a safe recovery action.

### Journey 5: version comparison, feedback and supersession

1. A reader selects two independently authorized revisions from history.
2. Comparison separates semantic element/relation changes, source/evidence
   changes, uncertainty, validation and review/approval applicability. It does
   not leak a source visible in only one revision.
3. An authorized user reports an actual Result/feedback assertion with its own
   time, actor, source and validation status. It is shown separately from the
   fragment's expected Outcome and does not prove the strategy correct.
4. If feedback motivates change, an editor creates a new draft and repeats
   validation/review/approval.
5. A replacement is approved/published independently. A curator then appends a
   supersession relationship and optional deprecation reason.
6. Old content, evidence, events and exports remain addressable under policy.

### Journey 6: dry-run import and safe draft disposition

1. An importer selects a representation and declares/detects a profile. Upload
   bounds and archive/path safety run before parsing.
2. Dry run reports parsed objects, original hash, unsupported versions or
   extensions, identity collisions, ambiguous/lost concepts, semantic findings
   and proposed dispositions.
3. No source, fragment or lifecycle record is created by validation alone.
4. Explicit commit creates allowed sources, quarantined objects, candidates or
   drafts. External `approved`/`published`/`validated`/`reusable` values are
   retained only as untrusted source metadata.
5. A paper example records repository SHA/path and illustrative/non-normative
   status. It never acquires local human approval from manuscript wording.

### Journey 7: versioned export

1. A permitted user selects an exact eligible revision, representation and
   audience; raw evidence is excluded unless explicitly and separately allowed.
2. UI displays language profile and exporter/serializer version. SysML v2 is
   labelled an interoperability projection, not the full language.
3. The adapter validates support and returns diagnostics before download.
   Unsupported semantics fail or produce an explicitly non-canonical preview;
   no invented Decision, selection or evidence status is allowed.
4. Successful output is immutable and records requester, input hash, options,
   output hash/media type, adapter version and diagnostics.
5. Export failure or download denial changes no fragment lifecycle or content.

## Legacy coexistence

- `proposed-for-MSH`: keep `/operator-strategies/*` clearly labelled as legacy
  during migration. Do not reinterpret `reusable` as approved/published.
- Do not redirect a legacy record to canonical OSL without an explicit migration
  map. Do not dual-write legacy and canonical stores.
- When migration is accepted, make legacy mutation routes read-only before
  removal; preserve read/history long enough for the documented compatibility
  window.
- Replace `/osl-export` only after the canonical exporter exists. Until then,
  label it as heuristic legacy output and do not call it validated OSL.
- Change navigation/docs only in the delivery where the corresponding route is
  real. Relevant later files include `base.html`, `guide.html`,
  `knowledge.html`, `overview.html`, `docs/operator_guide.md` and
  `docs/msh_workflow_navigation.md`.

## Planned UI/API tests

| Proposed test path | Required coverage |
|---|---|
| `catalog/flask_app/test_osl_read_routes.py` | list/detail/history/compare/provenance; safe cache/error/status behavior; no GET side effects |
| `catalog/flask_app/tests/test_osl_read_ui.py` | semantic labels, escaping, empty/degraded/withheld states and non-executable notice |
| `catalog/flask_app/tests/test_osl_write_routes.py` | every command's auth, CSRF, idempotency, expected revision, size/media bounds and safe recovery |
| `catalog/flask_app/tests/test_osl_user_journeys.py` | source -> excerpt -> manual/AI candidate -> draft -> validate -> review -> approve -> publish; feedback/supersession/import/export |
| `catalog/flask_app/tests/test_osl_authorization.py` | role/scope/object matrix, forged actor/role, IDOR, review/approve/publish separation |
| `catalog/flask_app/tests/test_osl_data_leakage.py` | no raw text/path/prompt/private note/hidden ID or inaccessible counts in HTML/JSON/errors/logs |
| `catalog/flask_app/tests/test_osl_ai_candidate_routes.py` | disclosure, provider failure/manual fallback, candidate-only output and no lifecycle authority |
| `catalog/flask_app/tests/test_osl_import_export_routes.py` | dry-run no write, exact revision eligibility, download auth and diagnostics |
| `catalog/flask_app/tests/test_osl_degraded_states.py` | empty, repository/AI/validator/export failure, unsupported profile, missing source, conflict |
| `catalog/flask_app/tests/test_osl_accessibility_contract.py` | labels/headings/legends/error linkage/status text/live region/keyboard-safe markup |
| `catalog/flask_app/tests/test_osl_mobile_layout_contract.py` | 320px/breakpoints, wrapping, sequential compare, touch controls and no viewport overflow |
| `catalog/flask_app/tests/test_osl_legacy_compatibility.py` | legacy remains distinguishable and cannot obtain canonical authority |

Use existing `test_client`, `tmp_path`, `monkeypatch` and deterministic service
fakes. Browser automation/accessibility tooling is `requires-research-clarification`;
semantic HTML/static CSS tests are an initial gate, not a substitute for later
manual and browser-level accessibility verification.

Permanent merge gates for these surfaces include unauthenticated/cross-scope
denial, CSRF, source-leakage sentinel absence, expected-revision conflicts,
exact-revision review/approval/publication, AI non-authority/manual fallback,
no-JavaScript core flow, import dry-run without canonical side effects,
export eligibility and Ubuntu/Windows responsive contracts.

## Decisions fixed by this plan

- `proposed-for-MSH`: canonical state is below Flask; UI/API never derives or
  sets lifecycle directly.
- `proposed-for-MSH`: HTML and JSON use the same application services and
  authorization/redaction projections.
- `proposed-for-MSH`: source access is independent of fragment access.
- `proposed-for-MSH`: every content edit appends a revision; every review,
  approval and publication binds an exact hash.
- `proposed-for-MSH`: manual workflow is first-class; AI is disclosed and
  candidate-only.
- `proposed-for-MSH`: import is dry-run then source/candidate/draft only; export
  is a non-mutating, immutable projection.
- `proposed-for-MSH`: empty, withheld, incomplete and degraded are distinct.
- `proposed-for-MSH`: publishing cannot expose an execute/activate control.
- `proposed-for-MSH`: legacy and canonical routes coexist without semantic
  aliasing or dual writes.

## Open paper and product decisions

- `requires-research-clarification`: which paper lifecycle/readiness terms are
  normative language semantics versus method examples or MSH product states?
- `requires-research-clarification`: which identity provider and role/scope
  model protects Flask routes, and must approval/publication require recent
  authentication or two-person control?
- `requires-research-clarification`: which source classifications may be sent
  to which local/remote AI providers?
- `requires-research-clarification`: first-release media types, sizes and
  selectors (character/line/page/notebook-cell/time range).
- `requires-research-clarification`: review scope, reviewer/approver/publisher
  separation and visibility of identities/reasons in published projections.
- `requires-research-clarification`: retention, consent withdrawal and legal
  erasure behavior for immutable raw capture.
- `requires-research-clarification`: UI locales and approved translations of
  operator-facing terms.
- `requires-research-clarification`: which import/serialization formats are
  normative, optional or unsupported, and what SysML v2 toolchain establishes
  export conformance?
- `requires-research-clarification`: browser/a11y tooling and manual audit
  ownership for final WCAG assurance.

## Acceptance criteria for later implementation

1. A user can go from immutable source to published fragment without browser
   state becoming canonical and without requiring AI.
2. Evidence, interpretation, Decision, OperatorAction, recommendation, human
   approval, authorized action and Result remain visibly and contractually
   distinct.
3. Every edit is a new version; validation/review/approval/publication target an
   exact unchanged revision.
4. Mutations are authenticated, authorized, CSRF-protected, bounded,
   idempotent and revision-fenced.
5. AI, UI, import, export, capability and federation paths cannot approve,
   publish, execute, verify evidence or grant authority.
6. Raw sources/private provenance are absent from unauthorized projections,
   counts, errors, logs, AI calls and exports.
7. Empty/degraded/incomplete/withheld states are explicit and recoverable.
8. Core journeys work without JavaScript, with keyboard input and on narrow
   mobile layouts.
9. Legacy `reusable` never silently becomes Approved/Published.
10. SysML v2 is presented as a versioned adapter with diagnostics, not the whole
    language or an operational binding.
