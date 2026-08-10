# Target architecture for OSL in FCP

Status: proposed target architecture; no component described here exists unless
explicitly marked `existing-in-FCP`.

Source baselines:

- `this repository`
  `f580c71f7269643a077cc7e7db8ba9bf6050bb6a`;
- `Nettking/systems-paper`
  `ff098ce52f15b489b6a07d5b55c6c788d862e3be`;
- `Nettking/paper-repo`
  `abe3fbcddee590c3f399b06f63cb329e8615977c`.

The language requirements and current-system findings in
`01_language_requirements.md` and `02_current_fcp_architecture.md` are
prerequisites for this design.

## Architectural outcome

`proposed-for-FCP`: OSL becomes a local-first, versioned, non-executing domain
subsystem under `catalog/osl/`. Its canonical state is an immutable,
source-traceable domain graph and lifecycle history held behind a repository
port. Flask, AI, JSON/YAML, SysML v2, support cards, recommender artefacts, and
federation are adapters or consumers. None is the source of truth.

The first implementation has no operational binding. A represented
`OperatorAction` is descriptive content. An approved or published fragment
still cannot execute an action or grant compute, storage, artifact, federation,
or machine-control authority.

## Layering and dependency rule

~~~text
Flask/UI     CLI/import     AI runtime     SysML/export
    \             |             |              /
     \       adapter/application boundary     /
      +---------------------------------------+
      | capture | draft | validate | review   |
      | approve | publish | query | migrate   |
      +---------------------------------------+
                      |
      +---------------------------------------+
      | domain model | language registry      |
      | lifecycle policy | provenance/events  |
      +---------------------------------------+
                      |
           repository port / unit of work
                      |
             local SQLite adapter first
~~~

Mandatory dependency direction:

1. Domain modules use the Python standard library only and do not import Flask,
   AI, federation, capabilities, storage-provider, template, or SysML code.
2. Application services depend on domain modules and narrow ports.
3. Persistence, AI, serialization, import/export, and Flask are outer adapters
   that depend inward.
4. A projection may depend on canonical read models; canonical writes cannot
   depend on a projection.
5. An authority decision arrives through a policy port bound to an
   authenticated principal. Domain lifecycle policy checks semantic
   preconditions but never authenticates or grants external authority.
6. No adapter receives more capability than its job requires. In particular,
   AI and serializers receive no approval/publication command port.

## Authority vocabulary

`proposed-for-FCP` defines these separate scopes:

| Scope | Permits | Explicitly does not permit |
| --- | --- | --- |
| `osl.source.capture` | Create immutable source artefacts under an allowed capture session. | Read every source, create a canonical fragment, or approve. |
| `osl.source.read` | Read source content within a data/classification scope. | Export, infer remote-processing consent, or edit source bytes. |
| `osl.candidate.create` | Store human- or AI-produced extracted candidates. | Create/replace a canonical revision. |
| `osl.fragment.edit` | Append a draft revision to an allowed logical fragment. | Review, approve, publish, or mutate prior revisions. |
| `osl.fragment.review` | Record a human reviewer decision for an exact revision and scope. | Product approval or publication. |
| `osl.fragment.approve` | Approve an exact reviewed, semantically acceptable revision. | Publish or execute it. |
| `osl.fragment.publish` | Publish an exact approved revision to an allowed audience. | Machine action, federation membership, storage/compute/artifact authority. |
| `osl.fragment.deprecate` | Deprecate or supersede a published revision with reason and lineage. | Delete history or source evidence. |
| `osl.export` | Produce an allowed format from an exact revision. | Change lifecycle or infer missing semantics. |

These scopes are product proposals, not `paper-defined` OSL constructs. The
eventual authentication mechanism and role assignments require product
ownership decisions, but the separation is an implementation invariant.

## Component 1: OSL domain model

- **Classification:** `proposed-for-FCP`, new.
- **Proposed placement:** `catalog/osl/source_models.py`,
  `catalog/osl/strategy_models.py`, `catalog/osl/review_models.py`, and
  `catalog/osl/identifiers.py`.
- **Responsibility:** represent immutable source metadata/excerpts and the
  paper-defined typed OSL nodes, relations, path membership, IDs, versions,
  gaps, evidence dimensions, reviews, external references, and extensions.
- **Allowed dependencies:** standard-library dataclasses/enums/typing and shared
  time/identifier utilities only if they carry no Flask/federation semantics.
- **Input:** already-decoded, bounded primitive values plus explicit
  language/profile identifier.
- **Output:** frozen domain objects and deterministic primitive projections for
  codecs/repositories.
- **Authority:** none. Constructors validate shape; they cannot review, approve,
  publish, execute, or grant access.
- **Failure modes:** invalid enum/type/cardinality, duplicate IDs, dangling
  local endpoint, unsupported required extension, or oversized value produces
  a typed domain error. It never fills missing semantics with guessed content.
- **Security boundary:** no secrets, raw credentials, provider endpoints, or
  executable callables in domain objects. Source content is referenced by ID
  and read under a separate source permission.
- **Existing basis:** frozen/versioned FCP contract conventions are
  `existing-in-FCP` in `catalog/federation/models.py` and
  `catalog/capabilities/jobs.py`; their domain types are not reused.

The canonical graph must give every path element and relation usage an immutable
ID. `DecisionOption` and `ActionSelection` remain distinct relation types;
`Decision` and `OperatorAction` remain distinct nodes.

## Component 2: language and version registry

- **Classification:** `proposed-for-FCP`, new.
- **Proposed placement:** `catalog/osl/language_registry.py` and profile
  descriptors under `catalog/osl/profiles/`.
- **Responsibility:** register supported OSL profiles, source paper SHA,
  supported node/relation types, WF rules, extension policy, compatible codec
  versions, migration edges, and export capabilities.
- **Allowed dependencies:** domain identifiers and immutable profile
  definitions; no repository, Flask, AI, or lifecycle service.
- **Input:** language/profile ID plus optional codec/export version.
- **Output:** immutable `LanguageProfile` or a precise unsupported-version
  diagnostic.
- **Authority:** none. Registration is deploy-time code/config, not a runtime
  user mutation in the first release.
- **Failure modes:** unknown major/profile, duplicate registration, ambiguous
  migration, missing source SHA, or incompatible codec fails closed.
- **Security boundary:** profile lookup cannot load arbitrary Python modules or
  fetch remote schemas from an instance-provided URL.
- **Paper boundary:** `requires-research-clarification`: the public first-version
  identifier is not defined. The initial internal profile must be explicitly
  pinned to the systems-paper commit and labeled provisional.

## Component 3: parser and canonical serializer

- **Classification:** `proposed-for-FCP`, new.
- **Proposed placement:** `catalog/osl/serialization/json_codec.py` plus
  `catalog/osl/serialization/contracts.py`. YAML is deferred unless an actual
  compatibility need is approved.
- **Responsibility:** decode/encode a versioned, non-executable exchange
  representation into/from the domain graph without changing semantics.
- **Allowed dependencies:** domain model, language registry, canonical
  JSON/hash utilities. It cannot depend on repository, lifecycle, AI, Flask, or
  SysML adapter.
- **Input:** bounded bytes/text and an explicit or embedded codec/language
  version.
- **Output:** domain object plus decode diagnostics, or deterministic canonical
  bytes plus content hash.
- **Authority:** none; parsing cannot create approval, publication, review, or
  operational binding.
- **Failure modes:** malformed syntax, duplicate keys/IDs, unknown required
  type, unsupported major, dangling reference, excessive nesting/size, invalid
  Unicode, or extension collision. Failure returns diagnostics and no partial
  canonical write.
- **Security boundary:** no object hooks, dynamic imports, URL dereferencing,
  entity expansion, code execution, or unbounded allocation. Unknown extensions
  are preserved only under profile policy.
- **Normativity:** `paper-defined` language semantics come from the selected
  profile, not the JSON shape. The codec is a product serialization.

The parser interface must also support a quarantine result for inspection of an
unknown/invalid import without claiming that the object is an OSL fragment.

## Component 4: semantic validator

- **Classification:** `proposed-for-FCP`, new implementation of the selected
  `paper-defined` profile.
- **Proposed placement:** `catalog/osl/semantic_validation.py` and rule modules
  under `catalog/osl/validation_rules/` only when size warrants the split.
- **Responsibility:** deterministically evaluate syntax-independent reference,
  cardinality, graph, locality, action/consequence, evidence, review, trace, and
  composition rules.
- **Allowed dependencies:** domain model and language registry only.
- **Input:** exact immutable fragment revision and language profile.
- **Output:** immutable `ValidationResult` with validator version/hash and
  ordered `ValidationFinding` values.
- **Authority:** none. The validator cannot mutate the fragment or lifecycle,
  decide evidence truth, perform human review, or approve.
- **Failure modes:** unsupported profile, internal rule/config error, resource
  limit, or invalid graph yields an explicit failed/incomplete result. It never
  changes “unknown” into pass.
- **Security boundary:** pure local processing with bounded graph size and
  deterministic traversal; no network, filesystem, AI, or external command.
- **Existing basis:** systems-paper's validator is research evidence and a test
  oracle source, not code to import blindly. FCP must encode and test the named
  selected profile.

The first validator covers WF1--WF15 as scoped in
`01_language_requirements.md`, with explicit deviations for applicability,
`ValidationNeed` maturity, generic relation qualification, rich review scope,
and element-level composition.

## Component 5: lifecycle policy

- **Classification:** `proposed-for-FCP`, new.
- **Proposed placement:** `catalog/osl/lifecycle.py`.
- **Responsibility:** define legal transitions and semantic preconditions for
  candidate/draft/review/approval/publication/deprecation states, separately
  from OSL path maturity.
- **Allowed dependencies:** domain lifecycle/review/validation summary types and
  language registry. It receives a policy decision; it does not authenticate.
- **Input:** current state/revision, requested transition, validation summary,
  review/approval records, and authenticated authorization decision.
- **Output:** allowed transition plan or typed rejection with stable reason code.
- **Authority:** policy evaluation only. The application service/repository
  records an allowed command; this component cannot grant external authority.
- **Failure modes:** stale revision, missing/failed validation, absent review,
  unresolved blocking issue, wrong prior state, rejected/sensitive policy, or
  insufficient scope.
- **Security boundary:** fail closed; no UI boolean or AI claim can satisfy actor
  or review requirements.
- **Existing basis:** `existing-in-FCP` job lifecycle validation and expected
  revisions are pattern references; job states and semantics are not reused.

Proposed primary transitions:

~~~text
extracted_candidate -> draft -> in_review -> reviewed -> approved -> published
                          |          |           |
                          |          +-> rejected+
                          +-> abandoned

published -> deprecated
published -> superseded (requires another published revision)
~~~

Correction/narrowing always creates a new draft revision and invalidates review
and approval for the older content hash. “Executable” is not a state.

## Component 6: repository port and local persistence adapter

- **Classification:** `proposed-for-FCP`, new; reuses `existing-in-FCP`
  transaction patterns.
- **Proposed placement:** `catalog/osl/repository.py` for protocols/commands and
  `catalog/osl/sqlite_repository.py` for the first adapter.
- **Responsibility:** atomically persist immutable source artefacts/excerpts,
  candidates, logical fragments, immutable revisions, validations, reviews,
  approvals, lifecycle/provenance events, and export records.
- **Allowed dependencies:** repository port depends on domain only; SQLite
  adapter depends on domain/ports plus `sqlite3` and canonical codec/hash
  helpers. No Flask, AI, template, SysML, capability, or federation imports.
- **Input:** bounded command with command ID, authenticated actor/scope
  reference, expected aggregate revision, payload fingerprint, and event data.
- **Output:** immutable command result/snapshot and next revision, or replay of
  the identical prior result.
- **Authority:** it enforces already-decided preconditions, uniqueness,
  immutability, expected revision, and idempotency. It does not decide whether a
  principal is allowed.
- **Failure modes:** revision conflict, command-ID fingerprint conflict,
  uniqueness/reference violation, disk full, locked/corrupt database, hash
  mismatch, or transaction failure. Roll back the entire command.
- **Security boundary:** parameterized SQL, explicit schema migration/version,
  no arbitrary filesystem paths from instance data, encrypted/restricted
  storage as required by deployment policy, and redacted errors.
- **Existing basis:** `SQLiteJobStore` transaction/idempotency/audit pattern and
  federation local immutable ingest. OSL uses separate tables and retention.

The repository must expose no “update source bytes,” “replace published
revision,” or “delete provenance” operation. Retention/redaction uses explicit
governance commands that preserve tombstone and lineage, not silent deletion.

## Component 7: source artefact and excerpt service

- **Classification:** `proposed-for-FCP`, new; replaces the legacy record as the
  capture boundary.
- **Proposed placement:** `catalog/osl/source_service.py`.
- **Responsibility:** capture source bytes/text and metadata unchanged, compute
  identity/hash, validate session/consent/classification, and create stable
  excerpts/spans.
- **Allowed dependencies:** domain source models, repository port, authorization
  and clock/ID ports. No OSL semantic interpretation or AI.
- **Input:** capture session, bounded content/MIME/encoding, source metadata,
  consent/classification reference, actor, command ID; excerpt range/selector.
- **Output:** immutable `SourceArtefact`/`SourceExcerpt` and provenance event.
- **Authority:** requires source-capture/read scopes; cannot create a strategy,
  review, approve, publish, or export.
- **Failure modes:** invalid/missing consent, disallowed sensitivity, size/type
  limit, invalid span/encoding, duplicate command, storage failure, or hash
  conflict.
- **Security boundary:** source content never appears in general logs/audit;
  readers require server-side scope; corrections create new artefact/revision
  with lineage.
- **Paper boundary:** immutable bytes/hashes are `proposed-for-FCP` hardening of
  the `paper-defined` requirement that raw capture remain available.

## Component 8: provenance and audit service

- **Classification:** `proposed-for-FCP`, new.
- **Proposed placement:** `catalog/osl/provenance.py` and
  `catalog/osl/events.py`.
- **Responsibility:** define transformation lineage and security/audit events,
  actor/tool attribution, input/output hashes, reason codes, and redaction-safe
  summaries.
- **Allowed dependencies:** domain/event types and repository unit-of-work
  port. No raw Flask request, template, or provider object.
- **Input:** command context plus exact source/revision/validation/review/export
  references.
- **Output:** append-only provenance event and separately classified security
  audit event.
- **Authority:** none beyond append requirement inside an authorized
  transaction.
- **Failure modes:** missing actor/tool/source reference, non-monotonic aggregate
  sequence, hash mismatch, forbidden raw payload in audit, or persistence
  failure causes the state-changing command to fail atomically.
- **Security boundary:** user-facing provenance may include allowed excerpts;
  general audit contains IDs/hashes/reason codes, not sensitive source, prompts,
  credentials, private endpoints, or generated raw text.
- **Existing basis:** capability/federation ordered audit patterns; their
  bounded retention and event types are not reused.

## Component 9: candidate and draft application service

- **Classification:** `proposed-for-FCP`, new.
- **Proposed placement:** `catalog/osl/candidate_service.py` and
  `catalog/osl/fragment_service.py`.
- **Responsibility:** create human/AI extracted candidates from excerpts and
  turn an explicitly selected candidate/edit into a new draft fragment revision.
- **Allowed dependencies:** source/repository ports, domain model, validator
  port, authorization/clock/ID ports, and an optional read-only
  `CandidateGenerator` port.
- **Input:** excerpt IDs, proposed typed claims/relations, field-level evidence
  links, actor, reason, base revision, expected repository revision.
- **Output:** non-canonical candidate or immutable draft revision plus
  provenance. A candidate never appears in published queries.
- **Authority:** candidate creation and draft editing only. No review, approval,
  publication, or operational authority.
- **Failure modes:** inaccessible/dangling excerpt, invalid relation endpoints,
  stale base revision, AI output failure, validation findings, or idempotency
  conflict. Validation failure may preserve a draft; it cannot silently repair
  it.
- **Security boundary:** field-level source access is rechecked server-side;
  generated values are labeled with origin; source text is not copied more
  broadly than policy allows.
- **Existing basis:** legacy form concepts and current capture UX become input
  mapping only.

## Component 10: AI candidate generator

- **Classification:** `proposed-for-FCP`, new outer adapter over
  `existing-in-FCP` AI runtime.
- **Proposed placement:** `catalog/osl/ai_candidate_generator.py` plus a narrow
  inference protocol in `catalog/osl/ports.py`.
- **Responsibility:** propose segmentation, classification, typed fields,
  relations, clarification questions, and explanations from an authorized
  minimum source excerpt.
- **Allowed dependencies:** candidate-generation port, codec for bounded
  structured output, data-classification/redaction policy, and
  `catalog.ai` runtime adapter. It cannot import repository mutation or
  lifecycle/review/approval services.
- **Input:** redacted/authorized excerpt, target language profile, explicit
  prompt version, allowed field/type vocabulary, provider locality policy.
- **Output:** `CandidateGenerationResult` with suggestions, uncertainty,
  supporting excerpt IDs, model/provider/run metadata, diagnostics, and hashes.
- **Authority:** strictly none. Output remains `extracted_candidate`.
- **Failure modes:** unavailable provider, policy forbids remote processing,
  timeout, malformed/adversarial output, unsupported type, fabricated source
  reference, prompt injection, or low/unknown confidence. Return diagnostics and
  manual fallback.
- **Security boundary:** minimum context, no repository credentials, no tools,
  no raw prompt/result in general logs, bounded response, exact source-ID
  allowlist, and reject references not present in the request.
- **Paper boundary:** AI participation is not `paper-defined` by
  `paper-repo`. It is a product proposal subject to human verification.

## Component 11: review, approval, and publication services

- **Classification:** `proposed-for-FCP`, new.
- **Proposed placement:** `catalog/osl/review_service.py` and
  `catalog/osl/publication_service.py`.
- **Responsibility:** record domain-review decisions, separately approve an
  exact revision, publish an approved revision/audience, and later
  deprecate/supersede without mutation.
- **Allowed dependencies:** repository, lifecycle policy, validator, provenance,
  authorization, clock/ID ports, and safe notification port if added later.
- **Input:** authenticated principal, explicit scope/role, exact revision/hash,
  expected aggregate revision, structured disposition/decision, rationale,
  unresolved issues, command ID.
- **Output:** immutable review/approval/publication record and event; safe
  snapshot/projection reference.
- **Authority:** only these application services may request their corresponding
  lifecycle transitions after external authorization. Review cannot approve;
  approval cannot publish unless policy combines roles explicitly; publication
  cannot execute.
- **Failure modes:** stale or changed content, absent validation, blocking
  findings, missing/insufficient review scope, self-approval policy violation,
  sensitive audience violation, idempotency conflict, or transaction failure.
- **Security boundary:** server-bound actor/role/scope, separation of duties
  where configured, CSRF for browser commands, no client-supplied signer, and
  exact content hash binding.
- **Paper boundary:** domain-review semantics are `paper-defined`; approval and
  publication are `proposed-for-FCP` workflow concepts.

If a reviewed revision is corrected or narrowed, the service creates/requests a
new draft revision. Prior review is retained and does not transfer automatically.

## Component 12: query and projection layer

- **Classification:** `proposed-for-FCP`, new; reuses `existing-in-FCP` safe
  projection conventions.
- **Proposed placement:** `catalog/osl/projections.py` and
  `catalog/osl/query_service.py`.
- **Responsibility:** produce role/audience-specific read models for lists,
  detail, validation, lineage, history, comparison, export selection, empty,
  degraded, and unsupported-version states.
- **Allowed dependencies:** read-only repository/query port, domain types,
  authorization decision, redaction/classification policy. No write services.
- **Input:** authenticated principal/scope, query/filter, exact or latest
  allowed revision selection.
- **Output:** frozen projection containing only allowed fields and explicit
  redaction/degraded notices.
- **Authority:** read-only; projection cannot transition state or become a
  canonical serialization.
- **Failure modes:** not found versus forbidden (without oracle leakage),
  unsupported version, missing source, corrupt projection dependency, stale
  index, or partial external reference. Return bounded explicit state.
- **Security boundary:** server-side filtering, default-deny source text and AI
  prompts, no secret/location/backend fields, and tests for list/search/error
  leakage.
- **Existing basis:** federation `FederationViewModel` and redaction patterns;
  no federation model is imported into OSL.

## Component 13: import/export coordinator

- **Classification:** `proposed-for-FCP`, new.
- **Proposed placement:** `catalog/osl/import_service.py` and
  `catalog/osl/export_service.py`.
- **Responsibility:** coordinate bounded decode, profile lookup, validation,
  quarantine/draft import, exact revision selection, serializer/exporter
  invocation, artifact hashing, and event recording.
- **Allowed dependencies:** registry, codecs/exporter ports, validator,
  repository, provenance, authorization. No lifecycle shortcut.
- **Input:** uploaded bytes or exact authorized fragment revision plus requested
  format/version.
- **Output:** import report and quarantined/draft object, or immutable export
  artifact/diagnostics.
- **Authority:** import may create quarantine or draft only; export is read-only.
  Neither may approve, publish, or execute.
- **Failure modes:** malformed/oversized input, unknown profile, unsupported
  extension, semantic failure, collision, partial external reference, exporter
  incompatibility, or storage failure.
- **Security boundary:** content-type/size/decompression limits, no path
  traversal, safe filenames, no remote URI resolution by default, source
  classification/audience enforcement, and no error echo of sensitive input.
- **Compatibility:** legacy FCP note import is a separate adapter that produces
  conservative findings; `reusable` never maps to approval.

## Component 14: SysML v2 adapter

- **Classification:** `proposed-for-FCP`, new replacement behind a compatibility
  facade; `existing-in-FCP` `OslExportService` is migration-only.
- **Proposed placement:** `catalog/osl/exporters/sysml_v2.py`.
- **Responsibility:** deterministically render a supported canonical OSL
  revision to SysML v2-compatible OSL text and produce mapping diagnostics.
- **Allowed dependencies:** read-only domain graph, language registry, stable
  naming/collision utility. No repository write, lifecycle, Flask, AI, or
  recommender service.
- **Input:** exact validated revision, supported profile/export version, export
  policy, and optional package/name mapping.
- **Output:** deterministic text bytes, content hash, source-to-output ID map,
  and diagnostics.
- **Authority:** none. It cannot invent evidence status, selected action,
  downstream target, approval, or operational semantics.
- **Failure modes:** unsupported construct/extension, identifier collision,
  incomplete faithful mapping, unknown profile, invalid character, or output
  limit. Fail explicitly rather than emit misleading fallback text.
- **Security boundary:** comments/identifiers are escaped; no filesystem path
  comes from model text; content is exported only for an authorized audience.
- **Normativity:** SysML v2 is a paper-defined primary engineering notation and
  a product interoperability adapter, not the FCP repository/lifecycle.

A future import adapter requires independent fidelity evidence; export support
does not imply safe round trip.

## Component 15: Flask/API and UI adapter

- **Classification:** `proposed-for-FCP`, new routes/templates that can reuse
  `existing-in-FCP` navigation and responsive UI conventions.
- **Proposed placement:** `catalog/flask_app/osl_routes.py`,
  `catalog/flask_app/services/osl_application_service.py`, templates under
  `catalog/flask_app/templates/osl/`, and dedicated CSS/limited progressive JS.
- **Responsibility:** translate authenticated HTTP commands/queries into
  application DTOs and render safe projections.
- **Allowed dependencies:** application/query facade, auth/CSRF/session
  integration, Flask rendering. It cannot import SQLite adapter internals or
  AI provider objects.
- **Input:** bounded form/JSON payload, CSRF/idempotency/expected revision,
  authenticated server context.
- **Output:** redirect or JSON/HTML projection with stable error code and no
  sensitive internals.
- **Authority:** none beyond forwarding the server-derived principal and
  enforcing HTTP protections. Client fields cannot assert roles, review,
  approval, publication, or signer identity.
- **Failure modes:** unauthenticated/forbidden, CSRF, stale revision,
  validation/review conflict, unavailable AI/export, unsupported version, or
  repository degradation; preserve user draft and render recovery action.
- **Security boundary:** source/text access filtered server-side, secure
  session/secret requirements, content-security/escaping, upload limits, safe
  downloads, and no existence oracle across scopes.
- **Existing boundary:** do not expand the current unauthenticated operator-note
  routes to serve canonical OSL commands.

## Optional future component: federation adapter

- **Classification:** `proposed-for-FCP`, deferred and
  `requires-research-clarification`.
- **Responsibility:** replicate or expose explicitly published OSL artefacts
  under a knowledge-specific membership/access/consistency policy.
- **Allowed dependencies:** read/export repository port and dedicated
  federation transport adapter; no direct reuse of storage-batch or job models.
- **Authority:** publication permission does not grant federation membership or
  remote storage write; federation authority does not grant source read,
  review, or approval.
- **Failure modes:** partition, stale replica, unknown profile, redaction policy
  mismatch, revoked audience, or incomplete provenance must produce explicit
  degraded/withheld state.
- **Initial status:** non-goal until local language, provenance, review,
  authorization, migration, and leakage behavior are accepted.

## Write dataflow

### A. Capture and excerpt

~~~text
authenticated capture request
 -> HTTP bounds + CSRF + server-derived principal/scope
 -> SourceService validates capture session, consent and classification
 -> repository transaction writes immutable bytes/hash + SourceCaptured event
 -> optional ExcerptCreated command records stable span/selector + hash
 -> safe projection returns IDs, not unrestricted source content
~~~

Failure behavior:

- retry with the same command ID/payload returns the same result;
- conflicting payload under the command ID is rejected;
- failure before commit writes neither artefact nor event;
- invalid excerpt leaves the source untouched;
- a correction creates a new artefact/revision and lineage.

### B. Candidate to published revision

~~~text
authorized source excerpt
 -> human extraction OR AI CandidateGenerator
 -> extracted candidate + generator/source provenance
 -> human selects/edits candidate into immutable draft revision
 -> SemanticValidator persists findings for exact revision/hash
 -> human submits exact revision for review
 -> reviewer records scoped decision
 -> authorized approver approves exact unchanged revision
 -> authorized publisher publishes exact approved revision/audience
 -> query projections and exporters consume that immutable version
~~~

Invariants:

- candidate creation never writes a canonical/published revision;
- a changed draft requires new validation and review;
- reviewer correction/narrowing creates a new revision;
- approval and publication bind exact content hash/profile;
- publication grants no operational authority;
- every accepted transition and denial receives a redaction-safe audit event.

Recovery:

- AI timeout/malformed output returns to manual extraction;
- failed validation preserves the draft and findings;
- stale edit returns current revision for deliberate rebase/new revision;
- review rejection preserves revision/evidence and may seed a new draft;
- publish storage failure leaves approval intact but no false published event;
- exporter failure leaves publication intact and records failed export attempt.

### C. Import

~~~text
authorized upload
 -> size/type/decompression checks
 -> codec identifies serialization + language profile
 -> decode into isolated object + diagnostics
 -> semantic validation
 -> quarantine if unknown/invalid; otherwise explicit human import decision
 -> new draft revision + import/source provenance
~~~

Import never creates `reviewed`, `approved`, `published`, or operational state,
even if the external payload claims such a state.

## Read dataflow

~~~text
authenticated query
 -> server binds principal, organization/site and requested audience
 -> QueryService obtains allowed fragment/version/source/event rows
 -> ProjectionBuilder applies field-level access and redaction
 -> projection states unknown versions, gaps and degraded dependencies
 -> Flask/API renders only the projection
~~~

Read invariants:

- list/search and counts cannot reveal forbidden source/fragment existence;
- source text, AI prompt/result, reviewer identity, and audit details each have
  independent disclosure policy;
- “latest” resolves to latest revision visible to the principal, not an
  unrestricted global latest;
- a cached projection is disposable and carries source revision/hash;
- UI/API cannot write a projection back as canonical state.

## AI dataflow

~~~text
CandidateService requests exact excerpt IDs
 -> source authorization + classification/locality policy
 -> minimum excerpts are redacted for the chosen provider
 -> bounded versioned AI runtime request
 -> structured output decoded against allowed candidate schema
 -> source-reference allowlist + uncertainty checks
 -> candidate and model/run provenance stored
 -> human review/edit remains mandatory
~~~

The AI adapter never sees an approval/publication/repository command capability.
Prompt injection that asks for tools, other sources, approval, or execution is
data and cannot alter this topology.

## Export dataflow

~~~text
authorized exact revision request
 -> confirm audience and allowed lifecycle state
 -> validate cached result matches revision/profile/validator version
 -> select codec or SysML adapter
 -> deterministic bytes + mapping + diagnostics + content hash
 -> immutable export record and provenance event
 -> safe download projection
~~~

Draft export may be allowed for a clearly watermarked private engineering
preview. Public/canonical export requires policy-approved lifecycle state.
Neither path may invent missing semantics.

## Cross-cutting failure and security rules

- Fail closed on unknown language/profile major, unknown required extension,
  missing authority context, stale revision, or incomplete security policy.
- Preserve raw source and prior revisions through all failures.
- Use stable machine-readable error codes and redacted user messages.
- Limit source/import/AI/export sizes, graph nodes/edges, nesting, and
  validation work.
- Bind commands to actor, scope, command ID, expected revision, and payload
  fingerprint.
- Bind review/approval/publication/export to exact immutable content hash and
  language profile.
- Store time in UTC with explicit source-local context where captured.
- Avoid ambient global services in domain code; inject ports for clock, IDs,
  authorization, inference, and repository.
- Never log raw sensitive source, credentials, private endpoints, full AI
  prompts/results, or unrestricted projections.
- Preserve manual operation when AI, remote providers, SysML tooling, or
  federation is unavailable.

## Architectural decisions and open decisions

### Fixed by this plan

- `proposed-for-FCP` canonical OSL is a typed, versioned domain graph, not a
  flat record or serialization schema.
- `proposed-for-FCP` source artefacts and fragment revisions are immutable.
- `proposed-for-FCP` initial persistence is local and transactional.
- `proposed-for-FCP` language maturity, product lifecycle, review, approval,
  publication, and external authority are separate.
- `proposed-for-FCP` AI is candidate-only; SysML is adapter-only; Flask is
  delivery-only; projections are read-only.
- `proposed-for-FCP` there is no operational binding in the initial roadmap.

### Still open

- `requires-research-clarification` public identifier and exact scope for the
  first FCP OSL language profile.
- `requires-research-clarification` whether `ValidationNeed` alone satisfies
  provisional-path gap requirements.
- `requires-research-clarification` applicability warning versus mandatory
  relation in the first profile.
- `requires-research-clarification` extension mechanism for quantitative
  confidence and richer review scope.
- `requires-research-clarification` whether SysML v2 import can ever preserve
  complete round-trip semantics; export-only is the safe first position.
- `requires-research-clarification` organizational roles, separation-of-duties
  policy, retention, consent, and confidentiality taxonomy.
- `requires-research-clarification` whether and how published OSL may be
  federation-visible.

The next workflow and contract documents apply these boundaries to each
Notebook-to-OSL step and data object. The file-by-file implementation allocation
is deferred to `06_repository_file_plan.md`.
