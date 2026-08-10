# Current FCP architecture relevant to OSL

Status: verified architecture inventory at FCP commit
`f580c71f7269643a077cc7e7db8ba9bf6050bb6a`. This document describes current
behavior and implementation patterns; it does not authorize production changes.

## Executive finding

`existing-in-FCP` has a useful statement-first operator-note workflow, related
operator-support views, one-way SysML text generation, robust patterns in other
subsystems for versioned contracts and transactional audit, and a bounded
language-model runtime. It does **not** have a canonical OSL language subsystem.

At the analyzed commit there is no `catalog/osl/` package, OSL language/version
registry, parser, canonical serializer, semantic validator, immutable strategy
fragment/version repository, OSL import, domain-review/approval service, or
source-to-fragment provenance graph.

The existing “OSL” path is:

~~~text
HTML form
  -> mutable flat operator-note JSON record
  -> captured / structured / reusable flag
  -> heuristic one-way SysML string generation
~~~

That path is a migration source and UI prototype. It cannot be promoted to the
OSL source of truth because it conflates raw evidence, Decision, and
OperatorAction; permits unreviewed “reusable” records; mutates records in place;
and provides neither paper-conformant semantics nor durable provenance.

## Repository-level architecture

- `existing-in-FCP`: `docs/architecture.md` lines 1--3 describe FCP as a
  Flask-first orchestration and analysis system for CNC telemetry.
- `existing-in-FCP`: `catalog/flask_app/` is the main UI/application layer;
  `catalog/common/` owns shared telemetry preparation; `catalog/orchestrator/`
  and the runner/session layer coordinate analysis; federation and capability
  packages own distributed storage, sessions, providers, jobs, and explicit
  authority (`docs/architecture.md` sections “Dataflow” and “Components,”
  lines 54--112).
- `existing-in-FCP`: raw telemetry JSONL remains source of truth while Parquet
  and session artefacts are rebuildable projections/caches
  (`docs/architecture.md` lines 100--119; `docs/data_contract.md` lines 5--38).
- `existing-in-FCP`: operator knowledge intentionally uses JSON rather than
  JSONL so recursive telemetry discovery does not ingest field notes
  (`catalog/flask_app/services/operator_strategy_service.py` lines 1--21;
  `docs/operator_strategy_capture.md` lines 35--45).
- `proposed-for-FCP`: place the canonical OSL domain/application layer outside
  Flask and outside telemetry/federation domain packages. Reuse their proven
  design patterns, not their domain models or authority meanings.

## Current surface inventory

| Area | Verified implementation | Reuse | Required extension or boundary |
| --- | --- | --- | --- |
| Knowledge capture | `existing-in-FCP` `OperatorStrategyService.add_from_form` stores one fast raw statement and optional fields. | Capture UX, timestamps, source inventory lookup, non-JSONL location. | `proposed-for-FCP` immutable source artefact/session and excerpt layer; never auto-map raw text to Decision. |
| Record structuring | `existing-in-FCP` flat `OperatorStrategyRecord` with paper-like strings. | Legacy import vocabulary and human-facing prompts. | Typed nodes/relations, immutable revisions, explicit missing fields, source trace. |
| Lifecycle | `existing-in-FCP` `captured`, `structured`, `reusable` plus boolean `reusable_strategy`. | Wording only as a legacy migration hint. | Separate extraction, draft, review, approval, publication, supersession, and OSL maturity. |
| Storage | `existing-in-FCP` whole JSON-file read/modify/temp-replace. | Atomic file replacement for a small legacy store. | Transactional repository, optimistic revision, command idempotency, append-only history. |
| Provenance | `existing-in-FCP` IDs/timestamps and free-text evidence/trace fields. | Original legacy record ID as import provenance. | Source/excerpt hashes, actors, transformations, reviews, revisions, validation and export lineage. |
| AI | `existing-in-FCP` read-only repository explainer and bounded local/remote provider runtime. | Provider/runtime contracts, session fencing, attempts/results, safe presentation. | New candidate-only application service with data classification, structured validation, and no lifecycle authority. |
| Authorization | `existing-in-FCP` strong authority mechanisms in capability/federation subsystems, but none on operator-note routes. | Fail-closed server-bound actor/scope patterns. | Human OSL policy/authentication and CSRF; never reuse storage/compute authority as strategy approval. |
| Flask/API | `existing-in-FCP` server-rendered capture/review/structure POST routes. | Blueprint, thin-route, service and test-client conventions. | Authenticated command/query routes with expected revision and server-derived actor/scope. |
| UI | `existing-in-FCP` statement-first capture and raw-text display. | Navigation, responsive patterns, empty/error conventions. | Side-by-side source/derivation, provenance, validation, version comparison, review/approval, accessibility. |
| Federation | `existing-in-FCP` authenticated, revisioned storage/session protocols and public projections. | Persistence, redaction and degraded-state design patterns. | Keep initial OSL local; federation is an optional later repository adapter/projection with separate policy. |
| Capabilities | `existing-in-FCP` versioned jobs, AI/compute providers, idempotent state machines, artifact grants. | Contract and transaction patterns. | OSL-specific types and authority; AI capability cannot gain storage, approval, or execution rights. |
| Events/audit | `existing-in-FCP` append audit in capability/federation stores. | Ordered, actor/command-bound events. | Unbounded/retention-governed OSL provenance distinct from operational audit. |
| Import/export | `existing-in-FCP` one-way heuristic SysML and recommender JSON generation; no OSL import. | Compatibility facade and legacy test fixtures. | Canonical codecs, deterministic faithful adapters, validation, history, collision handling, import quarantine. |
| Tests/CI | `existing-in-FCP` pytest/Ruff conventions and multiple Ubuntu/Windows workflow matrices. | Cross-platform gate structure and focused tests. | Permanent OSL gate; current operator/OSL tests are not selected by any workflow. |

## Knowledge capture, observations, and records

### Existing behavior

`existing-in-FCP` `catalog/flask_app/services/operator_strategy_service.py`:

- lines 19--21 define
  `data/operator_strategy_records/operator_strategies.json` and schema
  `fcp.operator_strategy_records.v3`;
- lines 28--70 define a frozen but flat `OperatorStrategyRecord` containing raw,
  contextual, interpretive, action, outcome, evidence, trace, and workflow
  strings in one record;
- lines 82--95 accept a top-level list or object with `records` and normalize
  every dictionary;
- lines 119--183 capture a form record;
- lines 185--231 update structure, outcome, and reusable state in place;
- lines 233--251 permanently delete or rewrite the whole collection;
- lines 255--303 normalize missing fields and infer status;
- lines 329--341 infer an action type from text;
- lines 366--368 validate only that a decision or raw statement exists.

The UI direction is valuable:

- `existing-in-FCP` `operator_capture.html` lines 13--42 requests one raw
  statement first.
- `existing-in-FCP` `operator_structure.html` lines 10--16 shows the original
  while structuring.
- `existing-in-FCP` `docs/operator_strategy_capture.md` lines 23--45 and 61--92
  says the raw statement is not yet a strategy, remains separate from telemetry,
  and may be structured later with missing fields.
- `existing-in-FCP` design intent in
  `docs/fcp_operator_support_plan.md` lines 23--59 and 119--127 explicitly says
  to keep raw statements separate from interpretation.

### Semantic and durability gaps

- `existing-in-FCP` contradiction: `add_from_form` lines 136--137 copies
  `raw_statement` into `decision` when no decision is supplied, and line 166 may
  infer an action type from it. The stored record therefore interprets evidence
  at capture time despite the documented statement-first boundary.
- `existing-in-FCP` `operator_structure.html` line 39 combines “Decision /
  strategy action,” so `Decision` and `OperatorAction` have neither separate
  identity nor a typed relationship.
- `existing-in-FCP` `evidence`, `trace_target`, `confidence`,
  `possible_cause`, and outcome fields are free text. No field-level source link
  explains which excerpt supports which interpretation.
- `existing-in-FCP` `update_structure` and `update_outcome` overwrite prior
  values; there is no author, base revision, change reason, or previous version.
- `existing-in-FCP` `mark_reusable` lines 222--231 can move any record directly
  to reusable; `update_outcome` can also make it reusable. There is no semantic
  validation, reviewer identity, review decision, or approval precondition.
- `existing-in-FCP` `delete` erases the record and its source text. This is
  incompatible with immutable evidence and durable provenance.
- `existing-in-FCP` whole-file read/modify/write has no lock, expected revision,
  or idempotency command and can lose concurrent changes.
- `existing-in-FCP` unknown per-record schema/status values are normalized
  permissively rather than rejected, quarantined, or migrated explicitly.

`proposed-for-FCP`: retain this file as a read-only legacy source during
migration. A legacy `raw_statement` becomes an immutable source artefact/excerpt;
every other field becomes an unverified candidate mapping. `reusable=true` is
only a `legacy_reusable` annotation, never approval or publication.

## Adjacent observation, outcome, and support stores

The following are useful sources or presentation models, not OSL authority:

- `existing-in-FCP` `MachineNotesService` stores machine-scoped text,
  applicability, caution, and a user-supplied source in
  `data/source_config/machine_notes.json`
  (`catalog/flask_app/services/machine_notes_service.py` lines 10--61).
- `existing-in-FCP` `operator_support_model.py` lines 19--130 defines frozen
  presentation types such as `ProblemObservation`, `CauseHypothesis`,
  `RecommendedAction`, `EvidenceItem`, `OperatorConfirmation`,
  `QualityOutcome`, and `SupportCard`.
- `existing-in-FCP` `OperatorSupportService` lines 49--95 combines seed rules
  and reusable notes into recommendation cards. It synthesizes text such as a
  count of related notes as “evidence.”
- `existing-in-FCP` `OperatorConfirmationService` lines 22--64 accepts
  `accepted`, `rejected`, `do_later`, `needs_supervisor`, or `already_done`, but
  actor identity is form supplied and unrelated to a strategy revision.
- `existing-in-FCP` first-part, quality-outcome, and confirmation records are
  separate loose JSON stores and use user-entered links/signers.
- `existing-in-FCP` `StrategyComparisonService` lines 13--58 groups notes by
  loose situation/action strings and treats the note's `decision` as its action.

`proposed-for-FCP`:

- import these only as explicitly classified source/evidence candidates;
- never translate confirmation into OSL review or product approval;
- never translate `worked` or `outcome` into a verified actual result without
  actor, time, context, and validation;
- later make operator-support/recommender views consume approved read-only OSL
  projections instead of mutable note records.

## Telemetry and intervention candidates

- `existing-in-FCP` raw telemetry JSONL and MTConnect observations have their own
  source, timestamp, normalization, gap, and playback contracts
  (`docs/data_contract.md` lines 5--108 and 147--157).
- `existing-in-FCP` intervention strategies are YAML-configured heuristics that
  emit `process_event_candidate` and `operator_intervention_candidate` rows,
  signatures, scores, and human-review fields
  (`docs/intervention_strategies.md` lines 1--20 and 30--74;
  `catalog/common/intervention_strategy_runner.py` lines 269--292 and 367--478).
- `existing-in-FCP` the documentation explicitly says these candidates are not
  ground truth and remain separate from a structured operator strategy.

Reusable:

- stable candidate/config signatures;
- source/time windows and gap awareness;
- human-review-ready candidate convention;
- telemetry references that can remain external to OSL.

Must not be coupled directly:

- no inferred state or candidate row automatically becomes a verified
  `Observation`, `Decision`, `OperatorAction`, or approved fragment;
- telemetry storage must not ingest notebook/source artefacts;
- OSL storage must not become a telemetry cache;
- an external telemetry reference needs a source schema/version/window and
  access check, not a copied truth claim.

## Storage APIs and persistence

### Existing operator-note persistence

`existing-in-FCP` is a local JSON document written through a temporary file and
replace (`OperatorStrategyService._write_records`, lines 242--251). This avoids
partial file contents for one writer but provides no transaction across source,
fragment, review, provenance, or audit records and no concurrent revision
fencing.

### Reusable persistence patterns elsewhere

- `existing-in-FCP` `catalog/federation/models.py` lines 132--158 uses frozen
  domain models, explicit `SCHEMA` constants, and fail-closed schema checks.
- `existing-in-FCP` `catalog/capabilities/jobs.py` lines 21--33 defines named
  protocol schemas and validates major versions; its frozen contracts validate
  bounded fields and transitions.
- `existing-in-FCP` `SQLiteJobStore`
  (`catalog/capabilities/job_store.py` lines 443--559) separates durable state
  from authority and stores command results and audit. Mutations use
  `BEGIN IMMEDIATE`, expected revisions, command IDs, fingerprints, and
  idempotent replay checks (lines 571--684 and 823 onward).
- `existing-in-FCP` `catalog/capabilities/contributions/store.py` lines 91--159
  uses a transaction and deterministic revision history for local intent.
- `existing-in-FCP` `catalog/federation/local_storage.py` lines 1--4 and
  172--278 demonstrates immutable ingest, content hashes, atomic publication,
  and durable idempotency.
- `existing-in-FCP` federation logical storage requires separate session,
  provider, group, and write-authority context.

`proposed-for-FCP`: build a dedicated local transactional OSL repository using
the revision/idempotency/audit patterns. Do not store OSL domain rows in the job
store, telemetry store, federation batch tables, or capability artifact tables.
Federated storage can become a later adapter after OSL access, consistency, and
retention policy are defined.

## Provenance, metadata, events, and audit

### Current state

- `existing-in-FCP` operator notes have a UUID, capture/decision timestamps,
  optional machine/sensor IDs, schema string, update timestamps, and free-text
  evidence/trace fields. They do not record capture actor, consent,
  confidentiality, source revision/hash, excerpt, transformation, reviewer,
  validation run, or export lineage.
- `existing-in-FCP` `SessionEvent` is a frozen revisioned federation model
  (`catalog/federation/models.py` lines 279--299), but it describes federation
  sessions, not knowledge provenance.
- `existing-in-FCP` `SQLiteJobStore` appends actor/command-bound ordered
  `JobAuditEvent` rows in the same state transaction
  (`catalog/capabilities/job_store.py` lines 389--444, 527--545, 647--684).
- `existing-in-FCP` `catalog/federation/persistence.py` lines 447--513 and
  522--584 stores and queries authorization/audit events, but deliberately
  prunes to bounded retention.
- `existing-in-FCP` artifact authority audits grants, denials, publication, and
  access (`catalog/capabilities/artifact_authority.py` lines 1--53 and
  141--230).

`proposed-for-FCP`:

- create OSL provenance events with their own schema, retention, redaction, and
  access policy;
- append source capture, excerpt, candidate generation, draft revision,
  validation, review, approval, publication, export, supersession, and
  deprecation events transactionally with the affected revision;
- keep a user-facing provenance chain separate from security/audit details;
- do not copy raw/sensitive source text, AI prompts, credentials, private
  endpoints, or model payloads into general audit events;
- do not reuse bounded federation-audit retention for evidence lineage.

## AI-assisted processing

### Existing capability

- `existing-in-FCP` the AI explainer is read-only and grounded in selected
  repository context. It must not change files, run operational actions, or
  inspect raw telemetry by default (`docs/ai_explainer.md` lines 1--15,
  42--52, and 92--106).
- `existing-in-FCP` `catalog/ai/prompts.py` lines 1--13 reinforces that the
  model explains rather than mutates.
- `existing-in-FCP` `catalog/ai/repo_index.py` excludes data/results paths, so
  the current explainer cannot process operator captures.
- `existing-in-FCP` `catalog/ai/runtime_contracts.py` defines bounded,
  protocol-versioned requests, provider attempts, and results; `runtime.py`
  rejects cross-session providers and requests (lines 180--225 and 347--350).
- `existing-in-FCP` connected provider documentation says a language-model
  provider receives only application-selected context and gains no storage,
  leadership, artifact, database, shell, or administrative authority
  (`docs/connected_capabilities.md` lines 65--97).

### Reuse and boundary

`proposed-for-FCP` reuse the runtime/provider ports for inference, not the
explainer's repository-indexing flow. Add a separate OSL candidate-generation
application service whose only output is an `extracted_candidate` with:

- exact source excerpt/revision;
- selected data-classification/locality policy;
- model/provider logical ID and protocol version;
- prompt template/version and input/output hashes;
- attempts, timing, structured-output validation, uncertainty, and explanation.

The AI adapter must not receive the lifecycle command repository, approval
service, publication command, operational binding, compute authority, or
storage authority. It may propose segments, classifications, fields, questions,
and draft content. A failure leaves the source intact and the manual workflow
available.

Data-leakage risk is material: a future remote provider would receive selected
operator knowledge. `proposed-for-FCP` therefore requires source-level
classification/consent, minimum necessary excerpt selection, server-side
redaction, provider locality policy, no raw prompt/error logging, and explicit
human disclosure before remote processing.

## Authorization and authority boundaries

### Operator routes are currently unprotected

- `existing-in-FCP` `operator_strategy_routes.py` lines 23--101 exposes list,
  capture, review, and structure views; lines 104--157 expose save, update,
  reusable, and delete POST operations.
- `existing-in-FCP` none of those routes performs authentication, actor/role
  authorization, CSRF verification, expected-revision checks, content-size
  enforcement, or server-side ownership binding.
- `existing-in-FCP` forms in `operator_capture.html`,
  `operator_review.html`, and `operator_structure.html` contain no CSRF token.
- `existing-in-FCP` `create_app` defaults to `SECRET_KEY=fcp-dev` and the
  runnable app defaults to `0.0.0.0`
  (`catalog/flask_app/app.py` lines 35--44 and 123--175).

This means reachable local/LAN users can read and mutate raw operator knowledge
under the current deployment assumptions. It is not an acceptable baseline for
review, approval, or sensitive source handling.

### Strong but semantically different authority exists elsewhere

- `existing-in-FCP` `SQLiteArtifactAuthority` is fail-closed and binds grants,
  scopes, placement, expiry, hashes, revocation, and audit
  (`catalog/capabilities/artifact_authority.py` lines 53--165 and 321--1017).
- `existing-in-FCP` federation write authority uses session/group/actor,
  current grants, terms, fencing tokens, and lease expiry
  (`catalog/federation/write_authority.py` lines 1--110).
- `existing-in-FCP` capability/job ownership and provider selection use explicit
  transitions and server-side state rather than UI assertions.

`proposed-for-FCP` borrow fail-closed actor/scope/fencing principles, but define
separate OSL permissions such as source read, draft edit, domain review,
approve, publish, deprecate, and export. Never equate:

- domain review with approval;
- approval with publication;
- publication with execution;
- OSL permissions with federation membership, storage leadership/write,
  compute dispatch, model-provider registration, artifact grants, or machine
  control.

## Flask routes, templates, and UI

### What can be reused

- `existing-in-FCP` Flask blueprints and service-oriented routes are registered
  centrally in `catalog/flask_app/app.py` lines 107--119.
- `existing-in-FCP` statement-first capture is fast and mobile-oriented.
- `existing-in-FCP` review pages already expose empty/load-error states and
  preserve visible raw text.
- `existing-in-FCP` base templates include responsive viewport/navigation, and
  the test suite contains responsive/mobile/static CSS contract tests.
- `existing-in-FCP` source inventory can suggest machine/sensor references.

### What must change later

- `existing-in-FCP` `operator_review.html` lines 38--46 can mark reusable or
  delete immediately; `operator_structure.html` lines 16--60 posts a combined
  edit/reusable form.
- `existing-in-FCP` routes instantiate local file services directly and pass raw
  records to templates rather than authorization-filtered projections.
- `existing-in-FCP` `_source_inventory` catches every exception and returns an
  empty inventory (`operator_strategy_routes.py` lines 16--20), masking cause
  and freshness.
- `proposed-for-FCP` keep routes thin: commands carry CSRF, idempotency key, and
  expected revision; actor/scope comes from authenticated server context;
  queries return explicit safe projections.
- `proposed-for-FCP` UI is never source of truth. It must display source versus
  derivation, language/profile version, lifecycle and maturity separately,
  validation findings, AI provenance, review scope, approval, history,
  degraded/unknown-version states, and redaction.

## Federation and capability systems

`existing-in-FCP` federation and capabilities are relevant as architectural
examples and downstream consumers:

- strict versioned models and envelopes;
- local-first operation and explicit degraded states;
- idempotent transactional mutations;
- signed/fenced write authority;
- safe public projections and redaction;
- session-bound language-model providers;
- explicit separation among discovery, suitability, membership, assignment,
  artifact access, storage, compute, and execution.

`catalog/federation/projections/models.py` lines 249--390 provides a frozen view
envelope with explicit normal/degraded/repair notices and a public-projection
assertion. `catalog/federation/redaction.py` lines 75--204 classifies and removes
secret/location data. Those are useful patterns for operator-knowledge
projections and redaction.

Must not be coupled directly:

- OSL models do not belong in federation `DomainModel` classes;
- OSL lifecycle is not a capability job state;
- an AI provider is inference only;
- a storage batch is not a source artefact or fragment version;
- a capability artifact grant is not strategy approval;
- network availability is not evidence or review state;
- initial OSL persistence should not require a federation.

## Import and export

### Existing SysML export

`existing-in-FCP` `catalog/flask_app/services/osl_export_service.py`:

- lines 10--26 overwrite one
  `data/sysml/operator_strategies.sysml` file from records marked reusable;
- lines 29--48 construct a package/import string;
- lines 51--93 heuristically map flat note fields to keyword names;
- lines 96--119 derive keyword and downstream target types from strings;
- lines 122--139 sanitize confidence and identifiers.

Material correctness risks:

- line 67 maps any non-empty free-text evidence to
  `EvidenceStatus = SourceBacked`, assigning unwarranted epistemic status;
- lines 58--59 invent/default decision and action values;
- lines 65 and 102--119 guess downstream artefact types from words;
- it uses legacy imports/keywords and flat feature notation rather than the
  current connected OSL core;
- identifiers can collide after sanitization;
- there is no language/profile version, parser or semantic validation,
  deterministic content identity, provenance, review binding, extension
  preservation, export history, or round trip.

`existing-in-FCP` `catalog/flask_app/test_osl_sysml_export.py` lines 13--60
checks string snippets only; it does not parse the result or test semantic
relations, collisions, adversarial values, or provenance.

### Other derived artefacts

- `existing-in-FCP` `RecommenderArtifactService` lines 18--40 overwrites derived
  JSON from reusable notes and invents fallback evidence/reason text.
- `existing-in-FCP` there is no OSL import path.

`proposed-for-FCP`:

- SysML v2 is a deterministic exporter/interoperability adapter downstream of
  canonical validated revisions, not the repository or language definition;
- serializers/importers return diagnostics and never change lifecycle;
- exports identify exact fragment revision, language/profile and serializer
  versions, content hash, time, producer, and source lineage;
- import of legacy/current-unknown syntax goes to quarantine or draft with
  findings, never approved/published;
- do not invent fallback semantics to produce syntactically complete output.

## Existing schema and domain-model conventions

### Weak current OSL-adjacent convention

`existing-in-FCP` `fcp.operator_strategy_records.v3` identifies only the outer
legacy file shape. Record normalization silently fills fields, uses loose
strings, and carries no per-instance compatibility negotiation.

### Strong reusable conventions

- frozen dataclasses and explicit enums;
- `SCHEMA` constants such as `fcp.node.v1`, `fcp.session.v1`, and
  `fcp.job.v1`;
- required-key/type/bounds validation at construction;
- fail-closed unknown-major handling;
- `to_dict`/`from_dict` round-trip tests;
- content hashes and canonicalized request fingerprints;
- expected revisions and command IDs;
- immutable event/audit records.

`proposed-for-FCP` apply those conventions in a new OSL package, with language
version kept distinct from storage/codec schema version.

## Tests and cross-platform CI

### Current tests

- `existing-in-FCP` `pytest.ini` discovers `catalog/**/test_*.py`;
  `requirements-dev.txt` includes pytest and Ruff.
- `existing-in-FCP` tests commonly use `tmp_path`, `monkeypatch`, Flask
  `test_client`, frozen-contract round trips, malformed/size/idempotency/restart
  cases, and targeted concurrency/race tests.
- `existing-in-FCP` `test_operator_strategy_service.py` verifies fast capture
  and prevents a capture form from skipping directly to reusable, but also
  freezes the raw-to-decision copy as current behavior.
- `existing-in-FCP` `test_operator_strategy_lifecycle.py` permits
  outcome/reusable transitions without semantic review.
- `existing-in-FCP` `test_osl_sysml_export.py` is string-only.
- `existing-in-FCP` route/template tests cover capture text, review wording,
  basic navigation, responsive layout, and static assets.

### CI

- `existing-in-FCP` workflows such as
  `.github/workflows/phase-f77-ai-runtime-integration.yml` lines 39--99 and
  `cfi1-federation-overview.yml` lines 31--85 run Python 3.12 on Ubuntu and
  Windows, install constrained dependencies, compile, run focused/affected
  pytest suites, run Ruff, and check the Git diff.
- `existing-in-FCP` no workflow path filter or test command selects the current
  operator-strategy/OSL files. They therefore lack a permanent merge gate.
- `existing-in-FCP` `docs/ci-lint-baseline.md` lines 3--7 treats
  workflow-specific Ruff exclusions as temporary, not precedent for new debt.

`proposed-for-FCP` later add one permanent cross-platform OSL workflow covering
contracts/codecs, semantic rules, lifecycle and authority, provenance,
repository concurrency/idempotency, routes/templates, leakage/redaction,
adversarial import, AI non-authority, deterministic SysML export, and legacy
migration. Details are specified in `08_validation_testing_and_ci.md`.

## Reuse, extend, and do-not-couple summary

### Reuse as patterns

- statement-first, low-friction capture and visible original text;
- local-first operation and explicit degraded states;
- frozen versioned contracts and fail-closed decoding;
- SQLite transactions, expected revisions, command idempotency, and ordered
  audit;
- content hashing and immutable ingest;
- safe projections and server-side redaction;
- bounded AI runtime/provider attempts and session fencing;
- Flask blueprint/service/test conventions;
- Ubuntu/Windows focused-gate convention.

### Extend behind new OSL-specific boundaries

- source inventory into typed external references;
- capture into source artefacts/excerpts;
- operator support into read-only projections;
- AI runtime into candidate-only generation;
- transactional persistence into an OSL repository;
- public projections into OSL-specific query models;
- SysML output into a versioned faithful exporter;
- legacy note service into a read-only migration adapter.

### Never make canonical or directly authoritative

- the legacy flat JSON record/file;
- `reusable_strategy` or `review_status=reusable`;
- a Flask form or template;
- free-text evidence, trace, confirmation, signer, or confidence;
- intervention candidate or inferred telemetry state;
- support/recommender card;
- generated SysML text;
- capability job/provider/benchmark status;
- storage/federation write authority;
- artifact grant;
- AI response;
- observed network/provider availability.

## Architecture consequences for the remaining plan

1. `proposed-for-FCP` introduce a new `catalog/osl/` domain/application boundary;
   do not grow `OperatorStrategyRecord` into the canonical model.
2. `proposed-for-FCP` use a local transactional repository first; add federation
   only through a later authorized adapter.
3. `proposed-for-FCP` preserve the legacy store untouched during idempotent
   migration and map ambiguity to findings.
4. `proposed-for-FCP` require authentication/authorization and CSRF before
   exposing sensitive source, review, approval, or publication commands.
5. `proposed-for-FCP` make operator-support, recommender, comparison, UI, API,
   AI, and SysML consumers of explicit OSL ports/projections, never independent
   sources of truth.
6. `requires-research-clarification` decide whether federation-visible
   publication is needed at all; current FCP federation has mechanisms but no
   operator-knowledge access/governance contract.

The next document defines the target component boundaries and dataflows that
implement these consequences without importing unrelated authority semantics.
