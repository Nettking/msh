# Phased OSL implementation roadmap

## Purpose and baseline

This roadmap turns the preceding plan into small, sequential, reviewable MSH
deliveries. It is not an implementation schedule or resource estimate.

| Repository | Analyzed commit |
|---|---|
| `Nettking/msh` | `f580c71f7269643a077cc7e7db8ba9bf6050bb6a` |
| `Nettking/systems-paper` | `ff098ce52f15b489b6a07d5b55c6c788d862e3be` |
| `Nettking/paper-repo` | `abe3fbcddee590c3f399b06f63cb329e8615977c` |

The markers `paper-defined`, `existing-in-MSH`, `proposed-for-MSH` and
`requires-research-clarification` follow
[00_scope_and_sources.md](00_scope_and_sources.md).

`paper-defined`: the selected systems-paper source describes a design-time
reasoning language/research profile, and Notebook-to-OSL describes a draft
human knowledge-transformation method. `proposed-for-MSH` lifecycle,
authorization, product UI, storage and AI behavior are deliberately separate.

## Sequencing invariants

1. Freeze a bounded, source-pinned profile and authority policy before code.
2. Build pure, non-executing contracts before persistence or UI.
3. Build canonical codec and semantic validator before accepting canonical
   writes or imports.
4. Make source/revision/provenance immutable and recoverable before workflow.
5. Make read projections safe before exposing HTTP.
6. Implement the complete manual Notebook-to-OSL path before AI assistance.
7. Implement review/approval/publication as exact human commands before UI
   offers those controls.
8. Require authenticated principal, authorization and CSRF before any
   production-capable Flask mutation.
9. Import never transfers external lifecycle; export never changes lifecycle.
10. SysML v2 remains a versioned adapter and operational binding remains a
    non-goal through this roadmap.

No phase may bypass an earlier acceptance gate by adding a permissive fallback.
Every phase leaves `main` usable and has one primary concern per PR.

## Roadmap summary

| Phase | Reviewable outcome | Primary PR split |
|---|---|---|
| 0 | accepted language/profile, authority and compatibility decisions | 1 docs-only PR |
| 1 | pure source/fragment/evidence/version contracts | 2 small core PRs |
| 2 | exact profile registry and canonical JSON | 1 codec PR |
| 3 | deterministic WF1-WF15 semantic validation | 1-2 rule PRs |
| 4 | immutable local storage, repository, provenance and audit | 2 adapter/service PRs |
| 5 | lifecycle and authorization-safe read projections | 2 service/read PRs |
| 6 | manual Notebook-to-OSL capture/candidate/draft workflow | 2 workflow PRs |
| 7 | human review, approval, publication and feedback | 2 authority PRs |
| 8 | authenticated read/write API and accessible UI | auth prerequisite + 2 UI PRs |
| 9 | optional candidate-only AI assistance | 1 separately switchable PR |
| 10 | canonical import/export, legacy migration, optional SysML export | 3-4 adapter PRs |
| 11 | cutover, compatibility hardening and permanent CI | several operationally narrow PRs |

## Phase 0: freeze concepts, profile and authority

**Classification:** `proposed-for-MSH` decisions grounded in `paper-defined`
sources; unresolved items remain `requires-research-clarification`.

**Goal:** establish one implementable, explicitly bounded first profile without
turning paper proposals into accidental public guarantees.

**Files:**

- create `docs/osl_language_profile.md`;
- create `docs/osl_authority_boundary.md`;
- create `docs/osl_compatibility_policy.md`;
- update `docs/agent_notes/osl_sysml_alignment.md` to label legacy behavior.

**Dependencies:** this plan; systems-paper semantic contract/core/reassessment;
paper-repo method boundaries; language/domain/security/product reviewers.

**Explicit non-goals:** production modules, Flask, AI, database, parser,
serializer, SysML adapter, migration, operational binding.

**Acceptance criteria:**

- exact internal profile ID and paper SHA;
- included element/relation/rule table, WF1-WF15 disposition and deviations;
- Decision/OperatorAction, candidate/selection, evidence dimensions, maturity
  and product lifecycle are unambiguous;
- applicability, `ValidationNeed`, review scope, stable identity, extensions and
  canonicalization are decided or explicitly deferred fail-closed;
- AI/human/application/capability/federation authority matrix denies execution
  and resource grants;
- public compatibility wording approved.

**Tests/checks:** documentation links/source citations; profile decision table
completeness; threat-model/authority-denial review; no code diff.

**Documentation:** the four files above plus decision owners/date/rationale.

**Main risks:** research vocabulary frozen too broadly; SysML mistaken for the
language; "model-ready" mistaken for Approved; missing identity owner.

**Commit/PR boundary:** one docs-only PR, ideally commits by profile, authority
and compatibility decision. Do not mix opportunistic paper edits.

## Phase 1: pure non-executing domain contracts and versioning

### Phase 1A: source, identifiers, evidence and graph

**Goal:** represent immutable sources and the selected abstract syntax without
storage/transport/framework coupling.

**Files:** `catalog/osl/__init__.py`, `errors.py`, `identifiers.py`,
`source_models.py`, `strategy_models.py`, `evidence_models.py`,
`versioning.py` and their focused tests listed in
[06_repository_file_plan.md](06_repository_file_plan.md).

**Dependencies:** accepted Phase 0 profile.

**Non-goals:** lifecycle services, JSON codec, cross-graph semantic validation,
SQLite, Flask, AI, SysML, runtime action methods.

**Acceptance:** frozen typed IDs/revisions/source selectors; Decision cannot be
constructed as Action; independent evidence dimensions; no execute/authority/
provider fields; no outward imports.

**Tests:** model invariants, identity/reference/immutability, positive/negative
paper-derived synthetic examples, import-boundary test.

**Docs:** link public symbols to profile table; mark product envelope versus
language graph.

**Risk:** overfitting field names to illustrative paper schemas. Mitigate with
typed semantic objects and documented rationale for every non-paper envelope.

**PR:** one pure-core PR, approximately one model concern per commit.

### Phase 1B: workflow/review/validation result/commands

**Goal:** add transport-neutral product records without implementing transitions.

**Files:** `workflow_models.py`, `review_models.py`,
`validation_models.py`, `provenance_models.py`, `commands.py` and tests.

**Dependencies:** Phase 1A.

**Non-goals:** approval decisions by a service, repositories, HTTP or AI.

**Acceptance:** paper maturity and product lifecycle cannot alias; all
review/approval/publication records bind exact revision/hash; command envelope
rejects client actor/authority fields and has fingerprint/expected revision.

**Tests:** lifecycle/maturity matrix, exact target, stable errors, command replay
contract and forbidden authority fields.

**Docs:** generated/reference contract examples remain labelled planned until
implemented, then update status accurately.

**Risk:** product policy leaking into language semantics. Keep separate modules
and marker/citation reviews.

**PR:** second pure-core PR; no persistence or route changes.

## Phase 2: language registry and canonical JSON

**Goal:** select an exact source-pinned profile and encode/decode a deterministic
product representation without claiming semantic validity.

**Files:** `catalog/osl/language_registry.py`,
`profiles/__init__.py`, `profiles/research_v0_1.py`,
`serialization/__init__.py`, `serialization/contracts.py`,
`serialization/json_codec.py`, fixtures and tests.

**Dependencies:** Phase 1; approved canonicalization/extension decisions.

**Non-goals:** YAML, SysML, repository writes, semantic repair, auto-upgrade,
approval or import of external lifecycle.

**Acceptance:**

- descriptor pins paper SHA and exact enabled/deferred rules;
- unknown profile/required extension fails closed without closest-version
  fallback;
- optional extensions preserve per policy;
- canonical bytes/hash deterministic across Ubuntu/Windows;
- decoder produces typed data but no valid/reviewed/approved status.

**Tests:** registry duplication/version matrix; round trip; duplicate keys;
Unicode/CRLF/limits; canonical hash and extension cases.

**Docs:** codec media type/version, canonicalization and compatibility table.

**Risk:** JSON mistaken for OSL itself; nondeterministic hash rules. State the
representation boundary in module/docs and gate both platforms.

**PR:** one codec/profile PR. If canonicalization review is large, merge the
descriptor first as data-only and codec second; neither accepts production data.

## Phase 3: deterministic semantic validation

**Goal:** implement the selected WF rule profile as pure diagnostics for an
immutable graph.

**Files:** `catalog/osl/semantic_validation.py` and, only when useful,
`validation_rules/graph_rules.py`, `strategy_rules.py`,
`evidence_rules.py`, `validation_rules/__init__.py`; rule fixtures/tests.

**Dependencies:** Phases 1-2 and Phase 0 rule dispositions.

**Non-goals:** factual/evidence verification, machining correctness, safety,
auto-fix, repository mutation, human review, approval, SysML-wide validation.

**Acceptance:**

- WF1-WF15 selected rules have stable IDs/source trace;
- every enabled rule has positive and isolated negative fixture;
- deterministic target/finding order and ruleset hash;
- unsupported/incomplete validation is never pass;
- validator cannot mutate graph or lifecycle;
- domain-reviewed/structurally-complete remain distinct from Approved.

**Tests:** full matrices in `08_validation_testing_and_ci.md`, including resource
bounds, gaps, candidate/selection, relation locality and review scope.

**Docs:** generated rule catalogue with normative/deferred/unsupported status
and deviations from paper aspirations.

**Risk:** local tests overclaim whole-paper conformance. Use source-pinned
profile language and separately review any rule deviation.

**PR:** one validator PR if reviewable; otherwise graph/basis rules followed by
response/evidence/composition rules, with each intermediate ruleset versioned
and complete for its declared subset.

## Phase 4: local persistence, immutable sources and provenance

### Phase 4A: repository and blob adapter

**Goal:** persist exact bytes/immutable revisions with recoverable transactions
and no workflow/UI.

**Files:** `catalog/osl/ports.py`, `repository.py`, `blob_store.py`,
`sqlite_repository.py`, `schema.py`, repository contract/blob/SQLite/migration
tests.

**Dependencies:** Phases 1-3; configured DB/blob paths; backup/permission policy.

**Non-goals:** candidates, review, publication, federation, AI, Flask, legacy
migration.

**Acceptance:**

- content-addressed source bytes verified and path-contained;
- append-only source/fragment metadata, commands and events transactionally
  stored;
- same command/fingerprint replays, collision rejects;
- expected-revision conflicts never overwrite;
- schema v1 opens/restarts/upgrades deterministically;
- locked/corrupt/partial failure creates no false committed state.

**Tests:** repository contract, tamper/path traversal, concurrency/restart,
failure injection, schema fixtures and Windows/Linux filesystem/SQLite.

**Docs:** configuration, storage layout without leaking paths, backup/restore
prototype and schema ownership.

**Risk:** DB/blob split atomicity and Windows lock behavior. Use staged blob
write/hash, transactional metadata and explicit unreferenced-object recovery.

**PR:** one repository protocol/in-memory contract PR, then one SQLite/blob PR
if needed. Do not combine with application workflow.

### Phase 4B: source/fragment services, provenance and audit

**Goal:** accept authenticated service commands for immutable capture/excerpt/
draft revisions and complete lineage, still without HTTP.

**Files:** `source_service.py`, `fragment_service.py`, `provenance.py`,
`events.py` and service/provenance/event tests.

**Dependencies:** Phase 4A and authorizer/clock/ID test adapters.

**Non-goals:** candidate AI, review, approval, publication, query UI.

**Acceptance:** source correction/new draft appends; source-excerpt-revision
lineage traverses; actor/tool distinctions preserved; event/state atomic;
general audit redacted; no provenance edge upgrades truth/review.

**Tests:** replay, stale edit, cycles, cross-scope references, source correction,
failure at every transaction boundary and sentinel leakage.

**Docs:** event/provenance contract, protected source handling and incident
recovery.

**Risk:** audit becoming a second source of sensitive content. Store safe IDs,
hashes/codes and protected detail separately.

**PR:** one service/provenance PR; source then fragment commits are acceptable
when each has its complete tests.

## Phase 5: lifecycle and safe read-only projections

### Phase 5A: lifecycle policy service

**Goal:** enforce explicit product transitions and exact-target invalidation
without exposing HTTP controls.

**Files:** `catalog/osl/lifecycle.py`, `policy.py` if first needed here, and
`test_lifecycle.py`.

**Dependencies:** Phases 1-4; accepted human role/separation policy.

**Non-goals:** complete review/approval service, UI, AI, execution/activation.

**Acceptance:** candidate/draft/reviewed/approved/published/deprecated/
superseded transition table; no generic status setter; validation/review
staleness on content change; publication cannot create operational state.

**Tests:** every allowed/denied transition, stale hashes, command replay,
principal/scope denial and capability/federation authority negatives.

**Docs:** state machine and distinction from paper maturity.

**Risk:** one "status" enum collapses independent facts. Keep lifecycle events,
validation, evidence review and maturity orthogonal.

**PR:** one lifecycle-only PR.

### Phase 5B: redacted query projections

**Goal:** provide authorized list/detail/history/compare/provenance views without
making projections canonical.

**Files:** `catalog/osl/redaction.py`, `projections.py`,
`query_service.py` and tests.

**Dependencies:** Phase 5A; source/fragment read policy.

**Non-goals:** Flask templates/routes, search infrastructure, federation.

**Acceptance:** latest-visible behavior; independent source/fragment access;
safe list/count/detail/history comparison; explicit empty/degraded/withheld;
projection rebuild; no existence oracle or sensitive sentinel.

**Tests:** role/scope/IDOR matrix, pagination, both sides of compare, redaction
in errors/audit and degraded repository.

**Docs:** projection DTO/version and disclosure policy.

**Risk:** count/history leakage and cached stale objects. Authorize before
projection and bind cache to revision/policy.

**PR:** one read-model PR; no navigation exposure yet.

## Phase 6: manual Notebook-to-OSL candidate workflow

### Phase 6A: capture, segmentation and annotation

**Goal:** support paper-informed raw capture, context, excerpt selection,
annotation and clarification without AI or model-ready overclaim.

**Files:** `catalog/osl/capture_service.py`, `segmentation.py`,
`annotation_models.py` and tests.

**Dependencies:** Phases 4-5; consent/classification/source-selector decisions.

**Non-goals:** AI, approval, publication, paper schema as canonical OSL.

**Acceptance:** unchanged original, exact excerpt selectors/hashes, Unicode/
line-ending correctness, context/consent/classification, interrupted session
recovery and correction lineage.

**Tests:** offset/boundary/overlap, missing context, consent denial, restart,
large/malformed source and immutable byte preservation.

**Docs:** operator capture policy and explicit `paper-defined` versus product
additions.

**Risk:** truncation or annotation overwrites raw notes. Store derivatives
separately and prove hashes.

**PR:** one capture/segmentation PR.

### Phase 6B: manual candidates, drafts and work queue

**Goal:** turn selected statements into attributed candidates and explicitly
human-created drafts.

**Files:** `catalog/osl/candidate_service.py`, `work_queue.py` and tests;
`fragment_service.py` only for narrow accepted-candidate command extension.

**Dependencies:** Phase 6A and semantic validator.

**Non-goals:** AI provider, review/approval/publication, UI.

**Acceptance:** human extraction/rejection/correction retained; exact evidence
links; candidate acceptance requires human command and creates draft revision;
generator confidence never becomes evidence review; queue scope safe.

**Tests:** rejected/edited/accepted candidates, missing/fabricated refs, stale
excerpt, queue count leakage and recovery.

**Docs:** manual workflow runbook and candidate/draft distinction.

**Risk:** "candidate" becomes implicit canonical content. Keep separate storage
type/service and explicit promotion event.

**PR:** one candidate/work-queue PR; this is the first complete backend manual
Notebook-to-OSL slice, still not exposed publicly.

## Phase 7: human review, approval, publication and feedback

### Phase 7A: review and approval

**Goal:** implement scoped human decisions on an exact unchanged revision.

**Files:** `catalog/osl/review_service.py`, `approval_service.py`,
`policy.py` and tests.

**Dependencies:** Phases 3-6; authenticated principal/policy adapter contract;
review-scope and separation decision.

**Non-goals:** publication audience, UI, AI review, operational validation.

**Acceptance:** reviewer scope/disposition, current validation prerequisite,
content hash binding, changed content invalidation, server actor and separation;
AI/client cannot sign.

**Tests:** stale/missing/incomplete scope, reject/request-change, role
combinations, forged signer and provider/capability/federation negatives.

**Docs:** reviewer/approver guide and exact claim limitations.

**Risk:** paper domain review conflated with product approval. Separate records,
labels and policy gates.

**PR:** review service PR followed by approval PR if either exceeds one reviewable
policy change.

### Phase 7B: publication, feedback and supersession

**Goal:** publish exact approved knowledge to an audience and retain later
feedback without modifying it.

**Files:** `catalog/osl/publication_service.py`, `feedback_service.py` and tests.

**Dependencies:** Phase 7A; audience/classification policy.

**Non-goals:** activation/execution, intervention runner, federation publishing,
automatic recommendation.

**Acceptance:** distinct publisher command; immutable publication; withdrawal/
deprecation/supersession events; feedback Result separate from expected Outcome;
published state gives no operational/resource authority.

**Tests:** audience/role, double publish/retry, failed publish, concurrent
supersession, conflicting feedback and exact source visibility.

**Docs:** publication/feedback policy and explicit non-execution warning.

**Risk:** "published" treated as active. Do not add active/executable fields or
consumer hooks.

**PR:** publication then feedback/supersession as two focused PRs if needed.

## Phase 8: authenticated API and accessible UI

### Phase 8 prerequisite: application identity and request security

**Goal:** provide a server-verifiable human principal/scope, secure session
configuration, authorization adapter and CSRF before OSL mutation routes.

**Files:** application-wide auth/config files are `requires-research-clarification`
and must be planned/owned outside OSL; OSL consumes only a narrow principal/
authorizer interface in `ports.py`.

**Dependencies:** product security/identity decision.

**Non-goals:** using node identity, operator display name, federation membership
or capability role as approval authority.

**Acceptance/tests:** anonymous/forged/expired/wrong-scope denial, secure secret/
cookie deployment, CSRF and object policy. Until passed, write routes stay
disabled.

**Risk:** enabling canonical mutations on today's unauthenticated Flask surface.
This is a hard stop, not a warning to defer.

**PR:** separate application-security PR; do not hide it inside OSL UI.

### Phase 8A: read-only API/UI

**Goal:** expose authorized lists/detail/history/compare/provenance and validation
with explicit empty/degraded states.

**Files:** `catalog/flask_app/osl_routes.py`,
`services/osl_application_service.py`, `app.py` registration,
read-only `templates/osl/*`, `static/css/osl.css` and read/UI tests.

**Dependencies:** Phase 5B and identity prerequisite.

**Non-goals:** POST commands, AI generation, import/export upload, navigation to
unimplemented actions.

**Acceptance:** server-side authorization/redaction, no paths/source leaks, no
GET side effects, lifecycle/maturity/validation separated, explicit
non-executable warning, keyboard/no-JS/mobile safe.

**Tests:** read routes, scope/IDOR/sentinel, empty/degraded/withheld,
accessibility/mobile/escaping/cache.

**Docs:** read user journey, privacy/disclosure and support states.

**Risk:** source leakage in list/count/history or CDN requests. Metadata-only
defaults, private cache and no unnecessary third party.

**PR:** one read-only surface PR.

### Phase 8B: capture/edit/validate/review UI/API

**Goal:** expose the already-tested commands without duplicating domain policy.

**Files:** mutate `osl_routes.py`/application facade; add capture, excerpts,
candidate review, editor, review, approval and import/export-later templates;
optional progressive `osl-editor.js`; write/journey/security tests.

**Dependencies:** Phase 8A, Phases 6-7 and identity/CSRF prerequisite.

**Non-goals:** AI assistance, SysML, execution, UI-owned lifecycle.

**Acceptance:** CSRF/idempotency/expected revision; exact human actor; recoverable
conflicts; source/candidate/draft distinction; review/approval cannot edit;
complete manual raw-to-published journey without JavaScript.

**Tests:** full `07_api_ui_and_user_journeys.md` matrix on Ubuntu/Windows.

**Docs:** operator/editor/reviewer/approver journeys and error recovery.

**Risk:** giant UI PR and browser state becoming canonical. Split capture/edit
from review/approval if needed; both use the same application facade.

**PR:** at least two reviewable UI PRs: capture/candidate/draft/validate, then
review/approval/publication/history.

## Phase 9: optional AI candidate assistance

**Goal:** add bounded, disclosed suggestions after the manual path is stable.

**Files:** `catalog/osl/ai_contracts.py`, `ai_candidate_generator.py`,
`ai_explanation.py`, `catalog/ai/osl_candidate_adapter.py`, tests and narrow
candidate UI/route additions.

**Dependencies:** Phase 6B, Phase 8B, provider locality/classification policy and
accepted AI evaluation gate.

**Explicit non-goals:** canonical writes by provider, source discovery beyond
allowlist, evidence verification, review, approval, publication, selection,
execution or authority grants.

**Acceptance:** versioned bounded contract; allowlisted/minimized excerpt;
model/provider/template provenance; fabricated refs reject; valid output remains
candidate; manual fallback; feature can be disabled without disabling workflow.

**Tests:** prompt injection, malformed/oversized/timeout/cross-session, forged
lifecycle/actor/authority, source leakage sentinel and manual fallback.

**Docs:** disclosure, data locality/retention, limitations and incident disable.

**Risk:** persuasive UI or provider output gains de facto authority. Field-level
origin, "not reviewed" label, no lifecycle port and human draft command.

**PR:** one feature-flagged AI adapter/candidate PR; provider-specific support is
separate from core service if it adds new data policy.

## Phase 10: interoperability and migration adapters

### Phase 10A: canonical JSON bundle import/export

**Goal:** exchange exact versioned, authorized bundles without lifecycle
elevation.

**Files:** `import_service.py`, `export_service.py`,
`bundle_manifest.py`, tests and import/export routes/template.

**Dependencies:** Phases 2-8; compatibility/extension/audience policy.

**Non-goals:** SysML, remote URI resolution, external approval trust.

**Acceptance:** bounded dry run, no partial write, explicit draft disposition,
deterministic exact-revision export, checksum/redaction manifest and immutable
history.

**Tests:** malformed/archive/SSRF/collision/version/extension, dry-run no write,
OS determinism, audience leakage and download authorization.

**Docs:** media/profile support and compatibility matrix.

**Risk:** untrusted import status escalates. Strip authority from disposition
and require local human workflow.

**PR:** import and export may be separate PRs; manifest contract lands first.

### Phase 10B: one-way SysML v2 adapter

**Goal:** produce a faithful versioned interoperability projection for the
supported subset.

**Files:** `catalog/osl/exporters/__init__.py`, `sysml_v2.py`, optional
`sysml_mapping.py` and tests; safe Flask response adapter.

**Dependencies:** Phase 10A and pinned SysML tool/profile conformance evidence.

**Non-goals:** SysML import/round trip, SysML as language/source of truth,
operational model binding.

**Acceptance:** every supported kind maps once; named ends/IDs/escaping correct;
unsupported semantics diagnostic not guessed; deterministic bytes; pinned
parser accepts output; canonical state unchanged.

**Tests:** golden/parser, collision, injection, unsupported extension and OS
bytes.

**Docs:** exact supported subset, losses and legacy exporter distinction.

**Risk:** parseability marketed as semantic equivalence. Gate claims and display
adapter/profile versions.

**PR:** one exporter PR. Defer entirely if toolchain cannot be made reproducible.

### Phase 10C: explicit legacy MSH/paper migration

**Goal:** preserve and conservatively map existing operator records/examples to
source/candidates/drafts.

**Files:** `catalog/osl/legacy/operator_note_v3.py`, optional approved
`paper_examples.py`, `migration_report.py`, `catalog/osl/cli.py` and tests.

**Dependencies:** Phase 10A, `09_migration_and_compatibility.md` decisions,
backups and operator review.

**Non-goals:** in-place rewrite, dual-write, automatic approval/publication,
legacy generated SysML as canonical OSL.

**Acceptance:** unchanged input hash; list/object forms; ambiguity findings;
`reusable` non-authority; dry-run/replay/reconcile; explicit human batch commit;
rollback retains original and reports.

**Tests:** every legacy status/field conflict, duplicate/missing IDs, restart,
partial failure, source immutability and authority negative.

**Docs:** migration runbook/report interpretation and cutover prerequisites.

**Risk:** inferred legacy fields presented as facts. Everything derived is a
candidate with field-level source provenance/finding.

**PR:** adapter/report first, CLI dry-run second, commit mode third. No cutover in
the same PR.

## Phase 11: cutover, hardening and permanent CI

**Goal:** make the completed local OSL workflow supportable, compatible and
permanently gated before legacy mutation retirement.

**Files:**

- `.github/workflows/osl-core.yml` and optional `osl-ui.yml`;
- adversarial/authority/compatibility/backup tests;
- `docs/architecture.md`, `docs/data_contract.md`,
  `docs/osl_operations.md` and legacy/export docs;
- later, narrowly modify legacy operator route/service/templates only after
  verified migration.

**Dependencies:** all shipped phases; operations/security/operator acceptance;
verified backups and compatibility matrix.

**Explicit non-goals:** federation, automatic recommendation, machine execution,
operational binding, removal of retained readers/evidence, destructive rollback.

**Acceptance:**

- stable `osl-required` gate on all PRs, Python 3.12 Ubuntu/Windows;
- complete authority/leakage/adversarial/migration/backup/restore gates;
- dry-run counts/hashes reconcile and legacy source stays unchanged;
- canonical writes enabled only after legacy writes become read-only;
- no dual write; safe read-only degraded mode rehearsed;
- legacy exporter clearly labelled or compatibility-routed;
- operations backup/restore/migrate/verify/deprecate incident runbook rehearsed;
- repository profile/codec/schema/export support matrix published.

**Tests:** complete permanent matrix in `08_validation_testing_and_ci.md` plus
baseline legacy compatibility and focused full-repository regression.

**Docs:** architecture, data contracts, operator migration, supported versions,
security/AI disclosure, recovery and deprecation.

**Risks:** cutover data loss, rollback re-enables unsafe mutation, permanent CI
is path-skipped, legacy bookmark meaning changes. Use explicit gates, no path
filter, immutable backup/report and staged read-only coexistence.

**PR boundary:**

1. permanent CI/security hardening;
2. backup/restore and compatibility fixtures;
3. legacy read-only cutover;
4. navigation/docs/default-path switch;
5. later deprecation cleanup only after the promised window.

No PR merges merely because the final roadmap phase is reached; each gate must
pass independently.

## Deferred work requiring a new plan

The following are outside this roadmap and must not be smuggled into a listed
phase:

- runtime/executable or operational binding;
- automatic recommendation or Action execution;
- compute/storage/capability authority derived from OSL;
- federation of published OSL artefacts;
- SysML v2 import or claimed lossless round trip;
- semantic merge of branching revisions;
- external standardization/public OSL stability claims;
- automatic evidence truth verification;
- legal-erasure implementation without an approved privacy model.

Each needs its own threat/authority model, contracts, migration/rollback and
evidence plan.

## Cross-phase review gates

| Gate | Required before | Blocks on |
|---|---|---|
| research/profile | Phase 1 | unresolved normative subset hidden as code default |
| security/identity | Phase 8 mutations | no server-verifiable human principal/scope/CSRF |
| storage/recovery | Phase 6 writes | source/revision/event partial-write or untested restore |
| semantic conformance | Phase 6 drafts/import | enabled rule without source/positive/negative coverage |
| human authority | Phase 7/8 approval UI | AI/client/capability/federation can sign or publish |
| privacy/leakage | Phase 8 and AI | raw source/private data in unauthorized projection/provider |
| interoperability | Phase 10 | unsupported/lossy mapping hidden or toolchain not reproducible |
| cutover | Phase 11 | unreconciled hashes/counts, dual-write or destructive rollback |

## Recommended first implementation delivery

Start with **Phase 0 only**: the three OSL decision/policy documents and legacy
alignment note. This resolves high-cost semantics before a public Python API
exists.

After acceptance, the first code PR is **Phase 1A only**: pure immutable IDs,
source/evidence/fragment contracts and tests. It contains no Flask, AI, SQLite,
SysML, migration, federation, capabilities or operational binding. Its success
criterion is faithful representability and authority absence—not a working UI.

## Roadmap completion criteria

The roadmap is complete only when all implemented phases have:

- exact files/ownership and no miscellaneous cross-domain module;
- explicit dependencies and non-goals;
- acceptance tests merged as permanent gates;
- current documentation with `paper-defined` versus `proposed-for-MSH` claims;
- migration/rollback for changed persistent contracts;
- no AI or application path with ungranted human/resource/action authority;
- small commit/PR history that can be reviewed and reverted without removing
  prior evidence.
