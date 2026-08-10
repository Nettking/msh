# OSL repository file plan

## Purpose and planning baseline

This document converts the target architecture into reviewable file changes for
later implementation in FCP. It is a plan only: none of the paths described as
new exist at the analyzed FCP commit, and this planning task does not add
production code.

Source baseline:

- FCP: `f580c71f7269643a077cc7e7db8ba9bf6050bb6a`;
- systems-paper: `ff098ce52f15b489b6a07d5b55c6c788d862e3be`;
- paper-repo: `abe3fbcddee590c3f399b06f63cb329e8615977c`.

The classifications `paper-defined`, `existing-in-FCP`,
`proposed-for-FCP`, and `requires-research-clarification` have the meanings
established in [00_scope_and_sources.md](00_scope_and_sources.md). Paper source
citations and their normative status are traced in
[01_language_requirements.md](01_language_requirements.md) and the method/source
boundary is traced in [04_notebook_to_osl_workflow.md](04_notebook_to_osl_workflow.md).

## File-plan rules

1. `proposed-for-FCP`: put the language and workflow in a new
   `catalog/osl/` package. Flask routes, current operator-note JSON, capability
   jobs, federation storage, telemetry and AI providers are adapters or adjacent
   systems, never the canonical language model.
2. `paper-defined`: preserve typed elements, typed relations, maturity,
   explicit incompleteness, evidence dimensions and Decision/OperatorAction
   separation from `systems-paper/sysml/osl-core.sysml` and
   `systems-paper/evaluation/osl-semantic-contract.md`. Do not flatten them into
   the current note form.
3. `proposed-for-FCP`: every canonical write is an authenticated command with
   expected revision, command ID and server-derived principal. The initial core
   is non-executing and cannot grant approval, compute, storage or action
   authority.
4. `proposed-for-FCP`: immutable source blobs, immutable fragment revisions and
   append-only audit records precede AI, UI and migration work.
5. `existing-in-FCP`: reuse patterns from `catalog/capabilities/job_store.py`,
   federation local storage/projections and the versioned AI runtime only behind
   OSL-owned ports. Do not import their domain authority.
6. `requires-research-clarification`: SysML v2 import, operational bindings,
   federation and automatic recommendations remain deferred. Export alone does
   not establish lossless round trip.
7. Each delivery below is a separate PR unless a smaller split is explicitly
   shown. A PR must leave a usable, tested state and must not pre-create empty
   miscellaneous/helper modules.

## Delivery map

| Delivery | Outcome | Dependency | Explicit non-goals |
|---|---|---|---|
| D0-A | Freeze the first FCP-supported research profile and authority policy | this plan and paper clarification | runtime code, UI, persistence |
| D1-A | Pure identifiers, source and fragment contracts | accepted D0-A | parsing, writes, Flask, AI |
| D1-B | Review, lifecycle and validation-result contracts | D1-A | lifecycle commands or approval routes |
| D2-A | Profile registry and canonical JSON codec | D1 | semantic validation, YAML, SysML |
| D2-B | Semantic validator and machine-readable rule catalogue | D2-A | repository writes, auto-repair |
| D3-A | Immutable source blob adapter and repository schema | D1, D2 | candidates, publication, federation |
| D3-B | Fragment repository, provenance and audit transaction | D3-A | review/approval, UI |
| D4-A | Lifecycle service and safe query projections | D2, D3 | HTTP exposure, AI |
| D5-A | Manual Notebook-to-OSL capture, excerpts and candidates | D4 | AI extraction, approval, publishing |
| D6-A | Review, approval and publication services | D5 | execution/activation, recommendation |
| D7-A | Authenticated read-only API/UI | D4 and application auth prerequisite | mutation routes |
| D7-B | Authenticated capture/edit/validate/review UI/API | D5, D6, D7-A | AI generation |
| D8-A | Candidate-only AI adapter | D5, accepted AI evaluation gate | canonical writes, approval |
| D9-A | Versioned JSON bundle import/export | D2-D6 | SysML import, legacy auto-promotion |
| D9-B | One-way SysML v2 export adapter | D9-A and conformance evidence | round-trip claim, execution |
| D9-C | Explicit legacy note migration tool | D9-A | in-place overwrite, implicit publication |
| D10-A | Compatibility hardening and permanent Linux/Windows gates | all shipped deliveries | operational binding, federation |

## D0-A: freeze the implementable language and authority boundary

This delivery is deliberately documentation-only. It prevents code from
silently deciding unresolved research and product policy.

| Path | Action | Responsibility and contents | Dependencies | Tests/checks | Migration | Phase |
|---|---|---|---|---|---|---|
| `docs/osl_language_profile.md` | create | `proposed-for-FCP` supported profile identifier; exact included element/relation kinds; WF1-WF15 interpretation; maturity and evidence dimensions; extension policy; canonical JSON status; link to analyzed paper SHA | 01 and accepted research decisions | reviewed decision table; link/source check | none | D0-A |
| `docs/osl_authority_boundary.md` | create | `proposed-for-FCP` principal/role vocabulary; capture, edit, review, approve, publish and read-sensitive permissions; explicit denial of AI, execution, compute and storage authority | FCP deployment/auth decision | threat-model review; deny matrix completeness | none | D0-A |
| `docs/osl_compatibility_policy.md` | create | language/profile/contract version meanings, reader/writer compatibility, extension preservation, deprecation window, export stability and migration ownership | language profile | examples for supported/unsupported version pairs | defines future migrations | D0-A |
| `docs/agent_notes/osl_sysml_alignment.md` | change | mark current legacy exporter facts against the selected profile; preserve it as historical/legacy behavior rather than canonical conformance evidence | existing note and D0 decisions | documentation diff review | none | D0-A |

Acceptance gate:

- every unresolved item in `01_language_requirements.md` is either decided for
  the first profile or explicitly deferred without a permissive default;
- "model-ready", "approved", "published", "active" and "executable" have
  distinct product meanings;
- a security owner confirms that authentication and authorization are a hard
  prerequisite for mutation routes;
- no production module is introduced.

## D1-A: pure core identifiers and source/fragment contracts

All files in this delivery are new and have no Flask, SQLite, AI, capability or
federation imports.

| Proposed path | Responsibility | Key public symbols | Allowed dependencies | Tests | Migration |
|---|---|---|---|---|---|
| `catalog/osl/__init__.py` | expose only stable public contract/version names | `SUPPORTED_LANGUAGE_PROFILES` or a narrow export list after registry exists | sibling core modules only | package import and no-side-effect test | none |
| `catalog/osl/errors.py` | typed, transport-neutral domain failures | `OslError`, `ContractError`, `UnsupportedProfileError`, `ConflictError`, `AuthorizationError`; stable error codes | standard library | code/message/redaction tests | none |
| `catalog/osl/identifiers.py` | validate typed opaque IDs and revision IDs without deriving meaning from display text | `SourceId`, `ExcerptId`, `FragmentId`, `RevisionId`, `ElementId`, `RelationId`, `EventId`, parse/new helpers | standard library only | valid/invalid/collision/serialization tests | none |
| `catalog/osl/source_models.py` | immutable capture session, source artefact/revision, protected content reference and excerpt selector contracts | `CaptureSessionRevision`, `SourceArtefact`, `SourceRevision`, `ContentDigest`, `SourceExcerpt`, selector union | identifiers, enums/value types | dataclass/model invariants; offsets; hashes; source immutability | none |
| `catalog/osl/strategy_models.py` | typed fragment graph, context, conditions, decisions, operator actions, outcomes, rationale and uncertainty | `StrategyFragmentRevision`, `StrategyPath`, `OslElement` subtypes, `OslRelation` subtypes, `ExtensionValue` | identifiers and pure core types | kind-specific invariants; Decision/Action separation; relation endpoints; frozen values | none |
| `catalog/osl/evidence_models.py` | evidence reference and independent derivation/source/review/confidence dimensions | `EvidenceReference`, `DerivationStatus`, `EvidenceSourceKind`, `EvidenceReviewStatus`, `ConfidenceAssessment` | identifiers/source models | no collapsed status; unknown is explicit; source reference required | none |
| `catalog/osl/versioning.py` | immutable fragment identity/revision lineage and SemVer-like product contract versions without inventing paper semantics | `LanguageProfileId`, `ContractVersion`, `RevisionLineage`, version parsing/comparison | standard library, identifiers | version ordering; invalid identifiers; lineage cycle rejection | none |
| `catalog/osl/tests/test_identifiers.py` | verify ID boundaries and stable wire rendering | test cases only | core package | pytest | none |
| `catalog/osl/tests/test_source_models.py` | source, excerpt, content-hash and correction lineage contracts | test fixtures/builders local to test | core package | pytest | none |
| `catalog/osl/tests/test_strategy_models.py` | abstract-syntax and relation contract invariants | paper-derived positive/negative examples | core package | pytest | none |
| `catalog/osl/tests/test_evidence_models.py` | independent evidence dimensions and provenance references | focused fixtures | core package | pytest | none |
| `catalog/osl/tests/test_versioning.py` | language/contract/revision version distinction | supported/unsupported pairs | core package | pytest | none |

Design constraints:

- `paper-defined` element names are preserved where the first profile adopts
  them. Product-only objects such as SourceArtefact and FragmentRevision are
  outside the language graph and marked `proposed-for-FCP`.
- A raw excerpt is not an Observation; an Observation is not Evidence; a
  Decision is not an OperatorAction; a candidate action is not a selected,
  recommended or authorized action.
- No model exposes `execute()`, `activate()`, provider IDs, job authority or
  mutable lifecycle setters.
- Constructor invariants cover local shape only. Cross-graph WF rules belong to
  D2-B so decoding never silently asserts semantic validity.

## D1-B: review, lifecycle and result contracts

| Proposed path | Responsibility | Key public symbols | Allowed dependencies | Tests | Migration |
|---|---|---|---|---|---|
| `catalog/osl/workflow_models.py` | candidate/draft/reviewed/approved/published/deprecated/superseded workflow records, separate from paper maturity | `CandidateRevision`, `WorkflowState`, `LifecycleRecord`, `Supersession` | D1-A contracts | no state aliasing; immutable records | none |
| `catalog/osl/review_models.py` | scoped reviewer and approver decisions bound to exact content hash/revision | `ReviewDecision`, `ReviewScope`, `ApprovalDecision`, `PublicationRecord` | identifiers, versioning | stale-target and scope coverage contracts | none |
| `catalog/osl/validation_models.py` | structured, non-mutating validation result/finding | `ValidationResult`, `ValidationFinding`, `Severity`, `ValidationTarget` | identifiers/versioning | stable codes; safe messages; hash binding | none |
| `catalog/osl/provenance_models.py` | actor/tool/source/derivation lineage references without treating an event as proof | `ProvenanceRef`, `ActorRef`, `ToolRef`, `DerivationRef` | identifiers/source models | required ref types and redaction-safe representation | none |
| `catalog/osl/commands.py` | transport-neutral immutable command envelope and narrow payloads | `CommandEnvelope`, capture/edit/review/approve/publish command dataclasses | D1 contracts | fingerprint, expected revision, client authority-field rejection | none |
| `catalog/osl/tests/test_workflow_models.py` | lifecycle/maturity orthogonality | state matrix | D1 | none |
| `catalog/osl/tests/test_review_models.py` | exact revision/hash and scoped decisions | positive/stale/incomplete cases | D1 | none |
| `catalog/osl/tests/test_validation_models.py` | finding contract and error redaction | stable-code snapshots | D1 | none |
| `catalog/osl/tests/test_commands.py` | command identity/fingerprint and forbidden actor claims | replay/collision cases | D1 | none |

## D2-A: profile registry and canonical JSON codec

| Proposed path | Responsibility | Key public symbols | Dependencies | Tests | Migration |
|---|---|---|---|---|---|
| `catalog/osl/language_registry.py` | register immutable descriptors, choose an exact supported reader/writer, never silently upgrade | `LanguageProfile`, `LanguageRegistry`, `get_profile` | D1, profile descriptors | supported/unknown/duplicate/downgrade tests | version dispatch point |
| `catalog/osl/profiles/__init__.py` | profile registration without wildcard plugin loading | explicit built-in registry | D1 | deterministic registration | none |
| `catalog/osl/profiles/research_v0_1.py` | machine-readable first profile derived from approved D0 table | `RESEARCH_V0_1` descriptor with kinds, rules and extension policy | D1, no paper repo at runtime | descriptor golden test against docs | first supported version |
| `catalog/osl/serialization/__init__.py` | narrow public codec exports | reader/writer protocols | D1 | import test | none |
| `catalog/osl/serialization/contracts.py` | envelope fields, media types, canonicalization and size/depth limits | `DocumentEnvelope`, `DecodeLimits`, `EncodeOptions` | D1 | bounds and version tests | wire contract v1 |
| `catalog/osl/serialization/json_codec.py` | lossless canonical JSON decode/encode; unknown extensions per profile; no validity claim | `decode_document`, `encode_document`, `canonical_json_bytes` | D1 and registry | round trip, key ordering, duplicate keys, numeric/text edge cases | reads/writes v1 bundles |
| `catalog/osl/tests/fixtures/profiles/research_v0_1/*.json` | minimal valid/invalid/gap/extension fixtures with provenance labels | data only; synthetic or licensed paper-derived | codec/profile | fixture manifest check | none |
| `catalog/osl/tests/test_language_registry.py` | profile selection and no implicit fallback | registry cases | D2 | none |
| `catalog/osl/tests/test_json_codec.py` | canonical bytes and lossless round trips | fixtures, property-based cases if dependency accepted | D2 | none |

YAML is not included: `systems-paper` does not define a canonical YAML wire
format. A later adapter may wrap the same envelope only after duplicate-key,
type and canonicalization rules are specified.

## D2-B: semantic validation

| Proposed path | Responsibility | Key public symbols | Dependencies | Tests | Migration |
|---|---|---|---|---|---|
| `catalog/osl/semantic_validation.py` | run deterministic profile rules against an immutable graph and return findings without repair | `SemanticValidator`, `validate_fragment` | D1, registry, validation models | full rule matrix, deterministic order | validator version persisted later |
| `catalog/osl/validation_rules/graph_rules.py` | uniqueness, reference integrity, path membership and graph completeness rules | rule functions with stable IDs | D1 | WF1/WF3/WF5/WF6/WF11/WF12 cases | none |
| `catalog/osl/validation_rules/strategy_rules.py` | Decision/action distinction, candidate vs selection, maturity, gaps and context applicability | rule functions | D1 | WF2/WF4/WF7/WF8/WF13/WF14 cases | none |
| `catalog/osl/validation_rules/evidence_rules.py` | independent evidence dimensions, traceability and review-scope rules | rule functions | D1 | WF9/WF10/WF15 plus unresolved-profile cases | none |
| `catalog/osl/validation_rules/__init__.py` | ordered rule set per exact profile | `rules_for_profile` | rule modules | no duplicate/stale rule ID | none |
| `catalog/osl/tests/test_semantic_validation.py` | positive, negative and multi-finding behavior | all profile fixtures | D2 | none |
| `catalog/osl/tests/test_validation_rule_traceability.py` | assert every selected profile rule has docs/source, implementation and tests | D0 profile table | D2 | none |

Do not split rule modules until the approved profile warrants it; if one
`semantic_validation.py` remains clearer, keep it and retain the same stable
rule-ID boundary. Validator output is evidence about a specific revision and
validator version, not approval.

## D3-A: immutable source storage and repository foundation

`existing-in-FCP` patterns in `catalog/capabilities/job_store.py` and
`catalog/federation/local_storage.py` justify SQLite transactions, optimistic
revision checks and content-addressed local objects. Their job, capability,
membership and storage authority must not leak into OSL contracts.

| Proposed path | Responsibility | Key public symbols | Dependencies | Tests | Migration |
|---|---|---|---|---|---|
| `catalog/osl/ports.py` | protocols for clock, ID generation, principal/policy checks, blob storage, repository and AI inference; capability-free vocabulary | `BlobStore`, `OslRepository`, `Authorizer`, `Clock`, `CandidateInferencePort` | D1 only | protocol/fake conformance tests | none |
| `catalog/osl/repository.py` | repository commands/results, unit-of-work boundary and atomicity contract; no SQLite statements | `RepositoryTransaction`, `StoredRevision`, `ReplayResult` | D1, ports | in-memory contract suite | none |
| `catalog/osl/blob_store.py` | content-addressed immutable source bytes with size/media/hash verification and opaque protected refs | `LocalBlobStore`, `BlobWriteResult` | ports, standard library | tamper, partial write, duplicate hash, path traversal, permissions | source blobs v1 |
| `catalog/osl/sqlite_repository.py` | first local adapter for metadata/revisions/events/command dedupe; never store secret source text in query columns | `SqliteOslRepository` | repository protocol, sqlite3 | transaction, locking, restart, corruption and concurrency | schema v1 |
| `catalog/osl/schema.py` | ordered, explicit schema migrations and current schema version; no generic migration framework unless justified | `SCHEMA_VERSION`, `apply_migrations` | sqlite3 | new DB, each upgrade, idempotent reopen, future-version refusal | owns OSL DB migrations |
| `catalog/osl/tests/repository_contract.py` | reusable behavior suite for in-memory and SQLite adapters | contract mixin/helpers | D3 | pytest | none |
| `catalog/osl/tests/test_blob_store.py` | immutable byte and containment guarantees | temporary directories | D3 | none |
| `catalog/osl/tests/test_sqlite_repository.py` | repository contract, replay and restart | temporary DB | D3 | schema v1 |
| `catalog/osl/tests/test_schema_migrations.py` | fixture DBs for every supported schema version | binary/SQL fixtures if needed | D3 | upgrade/rollback export tests | all schema versions |

Planned storage boundaries:

- default metadata database: configured path such as
  `FCP_OSL_DB_PATH`, not a hard-coded home-directory location;
- default protected blob root: separately configured
  `FCP_OSL_BLOB_ROOT` with resolved-path containment checks;
- raw bytes are immutable after hash verification. A correction creates a new
  SourceRevision and lineage; it never replaces a blob reference;
- metadata rows bind serialized canonical content hash, language profile,
  aggregate revision and timestamps;
- command dedupe stores command ID, kind/version, payload fingerprint and
  result reference. Same ID/different fingerprint is a conflict;
- DB and blob-store failure between stages is recovered through staged writes
  and garbage collection of unreferenced temporary objects, never deletion of
  committed evidence.

### Planned schema ownership

The exact SQL is an implementation decision, but ownership must be separated:

| Logical table/group | Owner | Required properties |
|---|---|---|
| source artefacts/revisions/excerpts | source repository methods | immutable revisions, protected blob ref, content hash, scope/classification |
| fragment logical IDs/revisions | fragment repository methods | append-only content revisions, parent/supersedes lineage, profile and hash |
| workflow records | lifecycle/review repository methods | append-only transitions and exact target revision |
| validation results | validator result repository methods | validator/profile version and exact target hash |
| provenance edges | provenance repository methods | typed from/to refs and derivation metadata |
| audit events | event append/query methods | append-only event ID, actor/tool, command, target and redacted details |
| commands | command replay methods | unique command ID plus fingerprint and atomic result ref |
| projection checkpoints | projection owner | rebuildable, never source of truth |

## D3-B: fragment revisions, provenance and audit

| Proposed path | Responsibility | Key public symbols | Dependencies | Tests | Migration |
|---|---|---|---|---|---|
| `catalog/osl/provenance.py` | validate and append typed source-to-excerpt-to-candidate-to-fragment lineage; traverse without upgrading truth status | `ProvenanceService`, `append_derivation`, `trace_lineage` | D1, repository, authorizer | broken/cyclic/cross-scope/redacted chains | schema v2 if separated |
| `catalog/osl/events.py` | domain/audit event vocabulary and safe public detail projections | `OslEvent` and event-kind constructors | D1, repository | append order, redaction and event-version tests | event contract v1 |
| `catalog/osl/source_service.py` | capture session/source/excerpt commands with consent/classification checks and immutable byte commit | `SourceService` | authorizer, blob/repository, clock/IDs | command replay, offsets, source correction, failure recovery | schema v1/v2 |
| `catalog/osl/fragment_service.py` | create immutable draft revisions from human edits or accepted candidates; never approve or publish | `FragmentService` | repository, validator optional only as explicit call, authorizer | optimistic concurrency, no in-place edit, provenance required | schema v2 |
| `catalog/osl/tests/test_source_service.py` | service-level source/excerpt and failure atomicity | fakes + SQLite contract | D3 | none |
| `catalog/osl/tests/test_fragment_service.py` | immutable revision and stale-edit behavior | D3 fixtures | D3 | none |
| `catalog/osl/tests/test_provenance.py` | preservation, traversal, leakage and meaning boundaries | chain fixtures | D3 | none |
| `catalog/osl/tests/test_events.py` | complete/redacted audit facts and versioning | event fixtures | D3 | none |

## D4-A: lifecycle, projections and safe reads

| Proposed path | Responsibility | Key public symbols | Dependencies | Tests | Migration |
|---|---|---|---|---|---|
| `catalog/osl/lifecycle.py` | enforce candidate/draft/review/approval/publication/deprecation/supersession state transitions separately from paper maturity | `LifecycleService`, `allowed_transitions` | D1, repository, validator, authorizer | transition table, stale target, idempotency, separation of duties | schema v3 |
| `catalog/osl/redaction.py` | policy-driven projection of source text, identities, AI prompts, review reasons and existence | `RedactionPolicy`, `redact_projection` | D1, authorizer port | field and collection leakage, no existence oracle | none |
| `catalog/osl/projections.py` | rebuildable read DTOs for list/detail/history/compare/provenance/work queues | `FragmentSummary`, `FragmentDetail`, `VersionComparison`, `ProvenanceView` | D1, redaction | deterministic projection and unknown kind state | optional projection tables |
| `catalog/osl/query_service.py` | authorized pagination/filter/detail/history reads; queries source-of-truth repository then redacts | `OslQueryService` | repository, projections, authorizer | pagination, cross-scope denial, degraded projection | none |
| `catalog/osl/tests/test_lifecycle.py` | exhaustive state/event/permission matrix | model-based table | D4 | schema v3 fixtures |
| `catalog/osl/tests/test_redaction.py` | field, count, timing/error and export leakage cases | principals/scopes | D4 | none |
| `catalog/osl/tests/test_projections.py` | read-only DTO stability and rebuild | revisions/events | D4 | none |
| `catalog/osl/tests/test_query_service.py` | server-side scope, pagination and not-found/forbidden equivalence | repository fakes | D4 | none |

Lifecycle implementation must use commands such as "submit exact revision for
review" rather than generic status setters. Approval requires a passed current
validation result, complete configured review scope, an unchanged content hash,
server-derived approver permission and any required separation of duties.
Publication is a separate command and event. "Active" and "executable" remain
unsupported states in the first profile.

## D5-A: manual Notebook-to-OSL workflow

This delivery implements the product workflow proposed in
`04_notebook_to_osl_workflow.md` without AI and without approval/publication.
It supports the paper's capture, selection, annotation, clarification and human
validation method while preserving that `paper-repo` is a draft method, not an
empirically validated product protocol.

| Proposed path | Responsibility | Key public symbols | Dependencies | Tests | Migration |
|---|---|---|---|---|---|
| `catalog/osl/capture_service.py` | open/close capture session, record scope/consent/training/context metadata and source revisions | `CaptureWorkflowService` | source service, authorizer | incomplete consent, interruption/resume, correction | schema v4 metadata |
| `catalog/osl/segmentation.py` | deterministic/manual excerpt segmentation utilities; preserve stable selectors and hashes | `SegmentProposal`, `segment_text`, `accept_excerpt` | source models only | Unicode, line endings, offset drift, overlap | none |
| `catalog/osl/candidate_service.py` | record human-extracted or externally generated candidates; link exact excerpts; promote only via explicit human command | `CandidateService`, `create_candidate`, `accept_candidate_as_draft` | repository, provenance, fragment service, authorizer | rejected/edited/accepted candidates, no auto-promotion | schema v4 |
| `catalog/osl/annotation_models.py` | notebook annotation and CTA clarification records, not canonical OSL elements until mapped | `SourceAnnotation`, `ClarificationRequest`, `ClarificationResponse` | source/identifier models | field/state distinction, unresolved flags | schema v4 |
| `catalog/osl/work_queue.py` | authorized capture/annotation/clarification/candidate queues with stable reasons | `WorkItem`, `WorkQueueService` | query, redaction | queue scope/count leakage and stale items | projection only |
| `catalog/osl/tests/test_capture_workflow.py` | end-to-end raw source through manually accepted draft | D3-D5 services | D5 | schema v4 |
| `catalog/osl/tests/test_segmentation.py` | exact source preservation and selectors | Unicode/adversarial text corpus | D5 | none |
| `catalog/osl/tests/test_candidate_service.py` | candidate authority/provenance/state transitions | human/AI-labelled fixtures | D5 | none |
| `catalog/osl/tests/test_work_queue.py` | safe task projection and recovery | policy fixtures | D5 | none |

Failure/recovery rules implemented here:

- a failed segmentation or classification never changes the source;
- an accepted excerpt remains addressable against the exact source revision;
- changing an excerpt makes a new excerpt/revision and marks dependent work for
  re-evaluation rather than rewriting its provenance;
- candidate rejection is retained as an audit fact but is not canonical OSL;
- accepting a candidate creates a new draft revision with an explicit human
  event; it does not inherit generator confidence as evidence confidence;
- human validation of a source statement is represented as a scoped validation
  record, not approval or publication of a strategy.

## D6-A: review, approval, publication and feedback

| Proposed path | Responsibility | Key public symbols | Dependencies | Tests | Migration |
|---|---|---|---|---|---|
| `catalog/osl/review_service.py` | request review and record scoped accept/request-change/reject decisions against exact revision | `ReviewService` | lifecycle, authorizer, repository, events | scope, separation, stale revision, withdrawal | schema v5 |
| `catalog/osl/approval_service.py` | record authorized human approval only after current validation and review policy pass | `ApprovalService` | lifecycle, review, validator, authorizer | AI/client cannot approve, stale/hash changes invalidate | schema v5 |
| `catalog/osl/publication_service.py` | publish an exact approved revision to an authorized audience; revoke/deprecate/supersede without erasure | `PublicationService` | lifecycle, repository, authorizer, events | audience, double publish, revoke, concurrent supersede | schema v5 |
| `catalog/osl/feedback_service.py` | link operational observations/results/feedback to published revision without mutating its claims | `FeedbackService` | source/provenance/repository | conflicting feedback, source visibility, later revision | schema v5 |
| `catalog/osl/policy.py` | product policy evaluation for review scopes and separation; not a language rule | `ReviewPolicy`, `PublicationPolicy` | pure D1 types | policy-version and deny-by-default tests | policy ref persisted |
| `catalog/osl/tests/test_review_service.py` | review command and exact-target behavior | D6 | D6 | schema v5 |
| `catalog/osl/tests/test_approval_service.py` | human/AI authority boundary and invalidation | D6 | D6 | schema v5 |
| `catalog/osl/tests/test_publication_service.py` | audience, lineage, deprecation and immutable publication | D6 | D6 | schema v5 |
| `catalog/osl/tests/test_feedback_service.py` | feedback preserves original claims and evidence | D6 | D6 | schema v5 |

There is intentionally no `activation_service.py` or executor. A published
fragment is an authorized knowledge artefact, not permission to operate a
machine or allocate resources.

## D7-A: authenticated read-only HTTP and UI adapter

### Hard prerequisite outside OSL

At analyzed FCP SHA `f580c71f7269643a077cc7e7db8ba9bf6050bb6a`,
the Flask operator routes do not provide a sufficient authenticated principal,
CSRF boundary or production-secret guarantee for canonical OSL writes. D7-A
may add read-only, redacted surfaces only after the application supplies a
tested server-derived principal/scope contract. D7-B mutation routes remain
blocked until CSRF, secure sessions and permission enforcement are accepted.
This is `existing-in-FCP` risk plus `proposed-for-FCP` prerequisite, not a claim
that OSL should own general application authentication.

| Proposed path | Action | Responsibility/key symbols | Dependencies | Tests | Migration |
|---|---|---|---|---|---|
| `catalog/flask_app/services/osl_application_service.py` | create | translate HTTP DTOs to query/command calls; map safe domain errors; never import SQLite/AI implementations | query/application services, principal adapter | facade and error mapping | none |
| `catalog/flask_app/osl_routes.py` | create | `osl` Blueprint; authenticated list/detail/history/compare/provenance/validation GET routes; bounded pagination | application service, Flask auth/CSRF integration | route permission, safe status/error/cache headers | none |
| `catalog/flask_app/app.py` | change | register blueprint and injected OSL facade/config; fail closed when write prerequisites absent | existing app factory | startup/config tests | none |
| `catalog/flask_app/templates/osl/index.html` | create | read-only work/list view with clear workflow and maturity labels | safe projections only | template/accessibility snapshots | none |
| `catalog/flask_app/templates/osl/detail.html` | create | element/relation detail, validation summary, source visibility boundary, non-executable warning | safe detail projection | escaping/redaction/empty-state tests | none |
| `catalog/flask_app/templates/osl/history.html` | create | immutable revision/event timeline and supersession | history projection | ordering/redaction tests | none |
| `catalog/flask_app/templates/osl/compare.html` | create | semantic/version diff without exposing unauthorized source content | comparison projection | added/removed/unknown kinds | none |
| `catalog/flask_app/templates/osl/provenance.html` | create | accessible lineage graph/table with withheld/degraded states | provenance projection | no existence leakage | none |
| `catalog/flask_app/templates/osl/_status_badge.html` | create | shared distinct lifecycle, maturity and validation labels | presentation enums | label/style contract | none |
| `catalog/flask_app/static/css/osl.css` | create | responsive, high-contrast, keyboard-visible layout; no semantic status by color alone | existing CSS conventions | static token/mobile checks | none |
| `catalog/flask_app/test_osl_read_routes.py` | create | server-side auth/scope, route behavior, errors and headers | Flask fixtures | pytest | none |
| `catalog/flask_app/tests/test_osl_read_ui.py` | create | escaping, accessibility landmarks, empty/degraded/mobile contract | templates | pytest/HTML assertions | none |

Existing templates `operator_capture.html`, `operator_structure.html`,
`operator_review.html` and `osl_export.html` remain legacy note surfaces until
D9-C. They must not import canonical repository objects or display a legacy note
as approved OSL.

## D7-B: mutation API and complete user workflow

| Proposed path | Action | Responsibility/key symbols | Dependencies | Tests | Migration |
|---|---|---|---|---|---|
| `catalog/flask_app/osl_routes.py` | change | POST capture/excerpt/candidate/draft/validate/review/approve/publish/deprecate commands; CSRF, size limits, command ID, expected revision | D5-D7 facade, auth/CSRF | deny/stale/replay/oversize/error recovery | none |
| `catalog/flask_app/services/osl_application_service.py` | change | route-specific request parsing and server-derived command envelope; preserve submitted draft on recoverable errors | D5-D6 services | client actor/role field rejection | none |
| `catalog/flask_app/templates/osl/capture.html` | create | consent/scope/context/source capture and immutable-original confirmation | capture workflow | accessibility, upload/text bounds | none |
| `catalog/flask_app/templates/osl/excerpts.html` | create | source/excerpt selection with exact source revision and withheld-text state | source/query services | Unicode offsets, no-JS operation | none |
| `catalog/flask_app/templates/osl/candidate_review.html` | create | compare source, annotations and candidate; accept/edit/reject with AI/tool label | candidate service | source access and action labels | none |
| `catalog/flask_app/templates/osl/edit.html` | create | typed fragment editor preserving IDs/unknown extensions and showing Decision/Action distinction | fragment/validator services | round-trip, stale edit, validation focus | none |
| `catalog/flask_app/templates/osl/review.html` | create | scoped review decision and unresolved findings; cannot approve | review service | permission/scope/separation | none |
| `catalog/flask_app/templates/osl/approval.html` | create | exact revision/hash, review/validation prerequisites and publication separation | approval service | human confirmation, changed target | none |
| `catalog/flask_app/templates/osl/import_export.html` | create later in D9-A | version/profile-aware upload/download and dry-run errors | import/export services | file safety | D9 |
| `catalog/flask_app/static/js/osl-editor.js` | create only if justified | progressive enhancement for typed relation editing; server remains authority/source of truth | no direct API secrets | no-JS parity, DOM injection, stale response | none |
| `catalog/flask_app/tests/test_osl_write_routes.py` | create | every command's auth, CSRF, replay, stale revision, data leakage and error mapping | D7 | pytest | none |
| `catalog/flask_app/tests/test_osl_user_journeys.py` | create | raw source to published revision plus interruption/recovery paths | full local stack | pytest | schema fixtures |
| `catalog/flask_app/tests/test_osl_accessibility_contract.py` | create | labels, focus targets, keyboard path, status text, reduced-motion/mobile rules | templates/assets | automated contract; manual audit checklist | none |

JSON API routes, if needed, live in the same blueprint under a versioned
`/api/osl/v1/...` prefix and consume the same application facade. HTML form and
JSON handlers must not duplicate authorization or lifecycle logic.

## D8-A: candidate-only AI assistance

| Proposed path | Action | Responsibility/key symbols | Dependencies | Tests | Migration |
|---|---|---|---|---|---|
| `catalog/osl/ai_contracts.py` | create | versioned bounded input/output DTOs, source minimization, prompt/evaluation metadata and candidate-only result | D1, ports | schema/bounds/forbidden authority fields | AI contract v1 |
| `catalog/osl/ai_candidate_generator.py` | create | call injected inference port; validate response shape; attach tool/model/prompt provenance; persist candidate only after service command | AI contracts, candidate service, validator for diagnostics | timeout/malformed/hallucinated refs/provider swap | none |
| `catalog/ai/osl_candidate_adapter.py` | create | adapt existing versioned AI runtime to OSL inference port; redact/minimize inputs; no OSL repository access | existing `catalog/ai/runtime.py` contracts and OSL port | provider/unavailable/degraded/cancellation | none |
| `catalog/osl/ai_explanation.py` | create | deterministic explanation DTO connecting each suggestion to excerpts, assumptions, gaps and validation findings | D1/provenance/validation | missing rationale/source and safe fallback | none |
| `catalog/osl/tests/test_ai_candidate_generator.py` | create | AI can propose/classify/explain but cannot write draft/review/approval/publication | fake inference port | pytest | none |
| `catalog/ai/tests/test_osl_candidate_adapter.py` | create | runtime-version bounds, minimization, failure and no cross-session leakage | fake providers | pytest | none |
| `catalog/flask_app/osl_routes.py` | change | optional generate-candidate command/status endpoint with rate/size limits and explicit tool label | D8 service | CSRF/auth/replay/timeout | none |
| `catalog/flask_app/templates/osl/candidate_review.html` | change | show provenance, explanation, uncertainty, editable/reject controls; never default approval | D8 projections | deceptive-state and accessibility tests | none |

The adapter must reject model-supplied `approved_by`, workflow status,
confidence-as-fact, new source IDs, action authorization, capability/storage
grants and unsupported element/relation kinds. Provider availability is an
optional degraded feature: manual capture and review remain usable.

## D9-A: canonical bundle import and export

| Proposed path | Action | Responsibility/key symbols | Dependencies | Tests | Migration |
|---|---|---|---|---|---|
| `catalog/osl/import_service.py` | create | bounded parse, profile selection, dry-run validation, extension policy, source-reference resolution and explicit import-as-draft command | codec/registry/validator/repository/authorizer | zip bomb/oversize/duplicate IDs/unknown version/cross-scope refs | import contract v1 |
| `catalog/osl/export_service.py` | create | authorized deterministic bundle of exact revision, validation/provenance manifest and redacted/withheld sources | query/redaction/codec | reproducible bytes, audience leakage, hash manifest | export contract v1 |
| `catalog/osl/bundle_manifest.py` | create | media types, checksums, included/withheld entries and compatibility metadata | D1/versioning | canonical manifest, tamper and future extension | bundle v1 |
| `catalog/osl/tests/test_import_service.py` | create | dry-run/no-partial-write, malformed/adversarial and compatibility | D9 | pytest | fixture versions |
| `catalog/osl/tests/test_export_service.py` | create | stable deterministic exports and redaction | D9 | pytest | golden bundles |
| `catalog/osl/tests/test_bundle_manifest.py` | create | checksum/path/duplicate entry safety | D9 | pytest | bundle v1 |
| `catalog/flask_app/osl_routes.py` | change | safe download and bounded upload/dry-run/confirm endpoints | D9 service | auth/CSRF/content disposition/MIME | none |
| `catalog/flask_app/templates/osl/import_export.html` | create | version/profile compatibility, dry-run findings and redaction disclosure | safe DTOs | empty/error/unknown extension states | none |

Imports always create new local IDs/revisions or explicitly resolve allowed
stable identities; they never overwrite an existing revision. A structurally
valid import remains a draft until local review/approval policy is satisfied.

## D9-B: optional SysML v2 export adapter

| Proposed path | Action | Responsibility/key symbols | Dependencies | Tests | Migration |
|---|---|---|---|---|---|
| `catalog/osl/exporters/__init__.py` | create | explicit exporter registry, no dynamic loading | export protocol | import test | none |
| `catalog/osl/exporters/sysml_v2.py` | create | map the selected profile to parseable SysML v2 text with stable identifiers and explicit unsupported/lossy findings | D1, profile, validator | parseability, semantic pattern, escaping, deterministic output | exporter version v1 |
| `catalog/osl/exporters/sysml_mapping.py` | create only if mapping size warrants | explicit element/relation mapping and unsupported-extension policy | D1/profile | every supported kind mapped once | none |
| `catalog/osl/tests/test_sysml_v2_export.py` | create | selected positive/negative fixtures, golden snippets and downstream parser when available | D9 | pytest/external parser gate if reproducible | none |
| `catalog/flask_app/services/osl_export_adapter.py` | create or fold into application facade | safe media response and finding mapping; replace no existing service yet | exporter | response tests | none |

`existing-in-FCP` `catalog/flask_app/services/osl_export_service.py` remains the
legacy operator-note exporter. Do not rewrite it into the canonical adapter in
the same PR. It uses heuristic inference and legacy concepts such as
`EvidenceStatus`, so treating it as the new serializer would create false
conformance. A later deprecation shim may call the explicit legacy migration
tool and then the canonical exporter.

SysML v2 import is `requires-research-clarification` and outside D9-B. It needs
a parser, a proven mapping for every supported semantic distinction, extension
handling and round-trip/fidelity criteria before any file path is committed.

## D9-C: explicit legacy note migration

| Proposed path | Action | Responsibility/key symbols | Dependencies | Tests | Migration |
|---|---|---|---|---|---|
| `catalog/osl/legacy/__init__.py` | create | label all adapters as legacy/non-canonical | none | import test | none |
| `catalog/osl/legacy/operator_note_v3.py` | create | read current `operator_notes.v3` records and produce candidate mappings plus ambiguity findings; never invent approval/evidence | D1, candidate service | all existing note states, missing fields, heuristic-action ambiguity | note v3 import |
| `catalog/osl/legacy/paper_examples.py` | create only with approved fixtures | import known paper examples as research fixtures/drafts with exact source SHA and declared losses | codec/registry | fixture provenance and expected findings | paper example profile |
| `catalog/osl/legacy/migration_report.py` | create | per-record dry-run/result/skip/error report with source hash and created IDs | legacy adapters | restart/replay/report stability | migration run v1 |
| `catalog/osl/cli.py` | create | offline `legacy-notes dry-run/import` and database `verify/export-backup` commands; explicit paths and confirmation | services, no Flask internals | temp-path/idempotency/exit-code tests | operator-run tool |
| `catalog/osl/tests/test_legacy_operator_note_v3.py` | create | raw statement remains evidence/source; inferred action remains candidate; deletes not replayed as facts | legacy fixtures | pytest | note v3 |
| `catalog/osl/tests/test_cli.py` | create | dry run, backup, resume, partial failure and no overwrite | temp repo | pytest | all migrations |

Do not change `catalog/flask_app/services/operator_strategy_service.py` in this
delivery. Migration reads a snapshot through an explicit adapter; the legacy
UI remains available until operators accept results and rollback/export has
been demonstrated. Existing note deletion semantics cannot be translated into
canonical evidence deletion.

## D10-A: consumers, hardening and permanent CI

| Path | Action | Responsibility | Dependencies | Tests/gates | Migration |
|---|---|---|---|---|---|
| `catalog/flask_app/services/operator_strategy_service.py` | change only after D9-C | display deprecation/banner/link and optional read-only migration status; no dual-write | legacy migration accepted | legacy regression suite | staged retirement |
| `catalog/flask_app/operator_strategy_routes.py` | change only after D9-C | prevent legacy surface from claiming canonical export; preserve read-only access during transition | D9-C | route compatibility | staged retirement |
| `catalog/flask_app/templates/osl_export.html` | change only after D9-C | label legacy output and direct users to canonical export/migration | D9-C | UI wording/escaping | none |
| `docs/operator_strategy_capture.md` | change | document legacy status and safe migration path | D9-C | docs link check | none |
| `docs/wiki/SysML-Export.md` | change | distinguish legacy heuristic exporter from canonical profile-bound adapter | D9-B/C | source/profile links | none |
| `docs/architecture.md` | change | add OSL bounded context, authority and storage boundaries | shipped components | architecture review | none |
| `docs/data_contract.md` | change | link versioned OSL contracts without folding them into telemetry schema | D1-D9 | docs/contract review | none |
| `docs/osl_operations.md` | create | backup, restore, verify, migrate, deprecate and incident procedures | D3-D9 | runbook rehearsal | all schema versions |
| `.github/workflows/osl-core.yml` | create | permanent Ubuntu/Windows core, codec, validator, lifecycle, migration and leakage gates; pinned supported Python matrix | test paths | required check | none |
| `.github/workflows/osl-ui.yml` | create if split is needed | Flask route/template/accessibility and legacy compatibility gates on Ubuntu/Windows | D7 | required check | none |
| `catalog/osl/tests/test_adversarial_inputs.py` | create | depth/size/Unicode/duplicate/path/extension and resource bounds | D2-D9 | permanent | none |
| `catalog/osl/tests/test_authority_boundaries.py` | create | exhaustive human/AI/client/capability/federation denial cases | D4-D9 | permanent | none |
| `catalog/osl/tests/test_compatibility_matrix.py` | create | every supported reader/writer/schema/profile/bundle pair | all version fixtures | permanent | all |
| `catalog/osl/tests/test_backup_restore.py` | create | restore exact hashes, history, events and source refs | operations/CLI | permanent | all |

Do not create a federation OSL adapter in D10. If later required, plan a
separate bounded context that accepts only explicitly published projections and
uses a knowledge-specific audience, redaction, consistency and revocation
policy. Federation membership or storage write authority is never OSL approval.

## Existing files that must not be coupled directly

| Existing path | Why it is adjacent | Prohibited shortcut |
|---|---|---|
| `catalog/flask_app/services/operator_strategy_service.py` | raw operator-note capture and current statuses | using mutable note JSON as canonical OSL repository or auto-migrating notes as approved fragments |
| `catalog/flask_app/services/osl_export_service.py` | legacy SysML-like text generation | treating string generation or `SourceBacked` inference as semantic validation |
| `catalog/common/intervention_strategies.yaml` and `intervention_strategy_runner.py` | machine intervention configuration/execution | turning an OSL action/candidate into runnable control or equating publication with execution authority |
| `catalog/capabilities/*` | strong command/revision/audit patterns and compute/storage services | importing provider, job, compute or storage authority into OSL workflow state |
| `catalog/federation/*` | immutable storage/projection patterns | treating replication membership as source visibility, review or publication authority |
| `catalog/common/state_events.py` and telemetry models | operational observations/events | automatically asserting telemetry as reviewed Evidence or verified fact |
| `catalog/flask_app/services/operator_support_service.py` | support-card projections | treating a generated card or recommender artefact as canonical StrategyFragment |
| `catalog/ai/*` | versioned AI runtime/provider boundary | giving a provider direct repository writes or accepting model-supplied lifecycle/actor fields |
| `catalog/flask_app/templates/*` | usable responsive surface conventions | making browser state/source fields authoritative or rendering unredacted content client-side |

## Dependency and import enforcement

The intended dependency direction is:

~~~text
pure models + versions + errors
        ^
registry / codecs / validator
        ^
repository ports + application services + policies
        ^
SQLite/blob / AI / SysML / Flask adapters
~~~

Later implementation must add an import-boundary test, either a small AST test
under `catalog/osl/tests/test_import_boundaries.py` or an accepted repository
tool, that proves:

- core models do not import Flask, sqlite3, `catalog.ai`,
  `catalog.capabilities`, `catalog.federation` or telemetry modules;
- domain/application services do not import Flask templates/routes;
- Flask, AI and exporter adapters depend inward through ports;
- no OSL package imports intervention execution or capability-dispatch code;
- tests fail if a convenience import reverses these boundaries.

## Migration ordering and rollback ownership

1. D3 schema v1 is deployed dark with backup/verify tooling before any UI
   writes.
2. Each schema change creates a new forward migration and fixture. Never edit a
   released migration in place.
3. Application rollback may use the prior reader only while the compatibility
   matrix proves it can read the current schema. Otherwise rollback means
   restore the verified pre-migration backup and preserve an export of newer
   records.
4. Language/profile migration creates new fragment revisions and provenance; it
   does not mutate canonical old bytes.
5. Legacy note migration is dry-run first, resumable by source hash/command ID,
   and candidate/draft only. Its rollback removes only newly created local
   projections/records when policy permits, never the original note snapshot or
   source blobs.
6. Projection tables and work queues are rebuildable; audit, provenance,
   publication and source records are not.

## Per-delivery definition of done

Every implementation PR derived from this file must include:

- paths and scope matching one delivery or a documented smaller subset;
- updated profile/rule/contract traceability when semantics change;
- new and changed tests colocated as listed above;
- deterministic failure codes and redaction-safe errors;
- Linux and Windows results for filesystem, SQLite, encoding or line-ending
  sensitive work;
- migration fixture and backup/restore evidence for schema changes;
- documentation that states `paper-defined` versus `proposed-for-FCP` claims;
- a diff audit showing no accidental execution, compute, storage, review,
  approval or publication authority;
- no empty future modules, unrelated refactors or silent legacy behavior
  changes.

## Recommended first implementation PR

Implement D0-A only:

1. add `docs/osl_language_profile.md`;
2. add `docs/osl_authority_boundary.md`;
3. add `docs/osl_compatibility_policy.md`;
4. resolve or explicitly defer the first profile identifier, applicability
   rule, `ValidationNeed` versus maturity, review-scope model, stable identity,
   extension preservation and export claim;
5. obtain language/domain/security review.

The next PR, D1-A, should add only pure, non-executing contracts and their
tests. It must not add Flask, AI, SQLite, SysML, legacy migration, federation or
operational binding. This sequencing makes the smallest code delivery
authority-safe and prevents unresolved paper vocabulary from becoming an
accidental permanent API.
