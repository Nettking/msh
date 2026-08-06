# OSL validation, testing, and cross-platform CI plan

## Status and analyzed baseline

This is a `proposed-for-MSH` implementation plan. No test, fixture, workflow,
profile or product guarantee described here exists unless marked
`existing-in-MSH`.

| Repository | Analyzed commit |
|---|---|
| `Nettking/msh` | `f580c71f7269643a077cc7e7db8ba9bf6050bb6a` |
| `Nettking/systems-paper` | `ff098ce52f15b489b6a07d5b55c6c788d862e3be` |
| `Nettking/paper-repo` | `abe3fbcddee590c3f399b06f63cb329e8615977c` |

The markers `paper-defined`, `existing-in-MSH`, `proposed-for-MSH` and
`requires-research-clarification` follow
[00_scope_and_sources.md](00_scope_and_sources.md).

## Evidence and claim boundary

- `paper-defined`: `systems-paper/evaluation/osl-semantic-contract.md`,
  "Core well-formedness commitments" (lines 364-426 in the analyzed version),
  defines WF1-WF15 for containment, applicability, observable basis, connected
  reasoning, gaps, response commitment, candidate/selection typing,
  action/consequence integrity, evidence, risk/trade-off, review, traces,
  composition and locality.
- `paper-defined`: `evaluation/osl-semantic-validation.md:7-32,81-102` says the
  validator is a bounded source-profile validator, not a general SysML v2
  interpreter and not evidence of factual truth, machining correctness, safety
  or operational validity.
- `paper-defined`: `evaluation/osl-final-reassessment.md:58-92` reports eight
  isolated negative witnesses; they do not exhaust every rule or relation
  combination. MSH therefore needs independent positive/negative product tests.
- `paper-defined`: Notebook-to-OSL requires low-friction raw capture, context,
  traceability, preserved uncertainty and human validation before
  model-ready treatment (`paper-repo/.../03_research_design.tex:29-44`,
  `04_method.tex:43-80`, `05_annotation_schema.tex:27-51`).
- `requires-research-clarification`: neither paper defines final MSH approval,
  publication, operational authority, canonical JSON or repository migrations.
- `proposed-for-MSH`: passing tests establishes conformance to the selected,
  source-pinned MSH profile and explicit product invariants only. Test names and
  reports must not call content true, safe, executable, operationally validated
  or approved without the separate exact lifecycle records.

## Existing MSH test and CI baseline

- `existing-in-MSH`: `pytest.ini` discovers `catalog/**/test_*.py`;
  `requirements-dev.txt` includes pytest and Ruff.
- `existing-in-MSH`: current suites use `tmp_path`, `monkeypatch`, Flask
  `test_client`, malformed inputs, frozen contract round trips,
  revision/idempotency, recovery and focused race tests.
- `existing-in-MSH`: `catalog/flask_app/test_operator_strategy_service.py:8-16,
  35-56,76-92` preserves the raw statement but also copies it into `decision`
  and can mark a note `reusable`. These are legacy behavior tests, not OSL
  contract evidence.
- `existing-in-MSH`: `test_operator_strategy_lifecycle.py:16-55` chains note
  reuse, support cards, export and recommender output without semantic review,
  approval or authority separation.
- `existing-in-MSH`: `test_osl_sysml_export.py:13-60` asserts string fragments
  and freezes legacy/collapsed evidence syntax. Keep it as compatibility
  coverage; do not rename it canonical conformance.
- `existing-in-MSH`: workflows such as
  `.github/workflows/phase-f77-ai-runtime-integration.yml:39-99` and
  `cfi1-federation-overview.yml:37-85` demonstrate Python 3.12 on Ubuntu and
  Windows, constrained installs, focused pytest, Ruff, compilation and diff
  hygiene.
- `existing-in-MSH`: no permanent workflow selects operator-strategy/OSL-adjacent
  tests. `docs/ci-lint-baseline.md:3-7` says current workflow-local lint
  exclusions are debt, not precedent for new files.

## Test architecture

Use five layers with deterministic clocks, IDs, policies, actors and fakes:

1. **Pure domain/contracts:** no filesystem, network, Flask, AI, SQLite, SysML
   tool, ambient clock or random ID.
2. **Adapters:** codecs, blob/SQLite, import/export, AI and identity adapters
   tested against narrow ports.
3. **Application services:** commands, lifecycle policy, authorization,
   transaction/event atomicity, projections, failure and recovery.
4. **HTTP/UI:** Flask client, templates/assets, auth, CSRF, conflicts,
   redaction, accessibility and mobile contracts.
5. **Focused workflow:** synthetic source through excerpt, candidate/manual
   draft, validation, review, approval, publication/export, feedback and
   supersession, with no operational binding.

Fixtures contain synthetic or public, source-pinned examples only. Never commit
private notes, credentials, endpoints, real identities or production data.

## Exact proposed test and fixture paths

### Domain, version, profile and codec

| Proposed path | Permanent responsibility |
|---|---|
| `catalog/osl/tests/conftest.py` | deterministic clock/ID/policy/principal builders, network deny fixture and explicit semantic builders |
| `catalog/osl/tests/test_identifiers.py` | logical versus revision IDs; labels/SysML names are not identity; duplicates/cross-revision refs reject |
| `catalog/osl/tests/test_source_models.py` | capture session, source, excerpt selector/hash/classification and frozen values |
| `catalog/osl/tests/test_strategy_models.py` | element/relation construction, Decision/Action, candidate/selection and expected/observed separation; no authority/executable fields |
| `catalog/osl/tests/test_evidence_models.py` | derivation/source/review/confidence axes remain independent and source references explicit |
| `catalog/osl/tests/test_workflow_models.py` | paper maturity versus product lifecycle; no aliases such as model-ready=approved |
| `catalog/osl/tests/test_review_models.py` | review scope, exact hash, approval/publication and evidence review remain orthogonal |
| `catalog/osl/tests/test_language_registry.py` | source SHA, supported types/rules/codecs, duplicate registry, unknown-major fail-closed and explicit migration edges |
| `catalog/osl/tests/test_json_codec.py` | domain/canonical JSON round trip, ordering, duplicate keys, Unicode, extension policy and no lifecycle elevation |
| `catalog/osl/tests/test_canonical_hash.py` | identical bytes/hash across OS, locale, path separator, insertion order, newline and timezone |
| `catalog/osl/tests/test_import_boundaries.py` | core imports no Flask/AI/capability/federation/SQLite/templates/SysML; adapters depend inward; no intervention execution import |

### Semantic validation

The first implementation may keep these cases in
`catalog/osl/tests/test_semantic_validation.py`. Split only when size warrants,
without changing stable rule IDs or coverage:

| Proposed path after split | Rule responsibility |
|---|---|
| `test_semantic_validation_basis.py` | WF1-WF5 |
| `test_semantic_validation_response.py` | WF6-WF9 |
| `test_semantic_validation_evidence_review.py` | WF10-WF12 |
| `test_semantic_validation_trace_composition_locality.py` | WF13-WF15 |
| `test_semantic_validation_determinism.py` | finding order/targets/codes, ruleset hash, resource bounds and repeatability |
| `test_validation_rule_traceability.py` | every enabled rule has source, profile decision, implementation, positive and isolated negative test |

### Source, repository, provenance and lifecycle

| Proposed path | Permanent responsibility |
|---|---|
| `catalog/osl/tests/repository_contract.py` | reusable adapter behavior suite; immutable revisions/events and exact round trips |
| `catalog/osl/tests/test_blob_store.py` | exact bytes/hash, containment, tamper/partial write, permissions and duplicate content |
| `catalog/osl/tests/test_source_service.py` | capture/seal/excerpt/correction, bounds, consent/classification and no partial write |
| `catalog/osl/tests/test_provenance.py` | inputs/actor/tool/run/time/hash, acyclic lineage, redacted traversal and preservation |
| `catalog/osl/tests/test_sqlite_repository.py` | persistence/reload, constraints, schema, command replay and immutable state/events |
| `catalog/osl/tests/test_sqlite_repository_concurrency.py` | simultaneous edit/review/approval/publication/feedback races with expected revision |
| `catalog/osl/tests/test_sqlite_repository_recovery.py` | injected failure at transaction boundaries, lock/disk/hash/corruption/restart; no half state/event |
| `catalog/osl/tests/test_schema_migrations.py` | empty and every retained schema fixture, idempotent restart, partial/corrupt/future-version refusal |
| `catalog/osl/tests/test_lifecycle.py` | every legal/illegal transition, state/maturity separation, stale invalidation and no executable state |
| `catalog/osl/tests/test_review_service.py` | exact revision/hash/scope/human binding, dispositions, blockers and stale rejection |
| `catalog/osl/tests/test_approval_service.py` | current validation/review policy, server principal, separation, no AI/client approval |
| `catalog/osl/tests/test_publication_service.py` | approval/publication separation, audience, retry, deprecation/supersession without erasure |
| `catalog/osl/tests/test_notebook_to_osl_workflow.py` | synthetic end-to-end workflow and recovery at every durable boundary |

### Query, authorization, leakage and AI

| Proposed path | Permanent responsibility |
|---|---|
| `catalog/osl/tests/test_query_service.py` | independently authorized source/fragment reads, pagination, latest-visible semantics and safe not-found |
| `catalog/osl/tests/test_redaction.py` | list/search/count/detail/history/compare/error/audit/export redaction and no existence oracle |
| `catalog/osl/tests/test_candidate_service.py` | human/AI candidates attributed, exact excerpt allowlist and no auto-promotion |
| `catalog/osl/tests/test_ai_candidate_generator.py` | valid suggestions, timeout/manual fallback, malformed/fabricated refs and locality denial |
| `catalog/osl/tests/test_authority_boundaries.py` | client/AI/capability/federation attempts to review, approve, publish, execute or grant authority all fail |
| `catalog/osl/tests/test_audit_redaction.py` | no raw source, prompts/results, credentials, private endpoints or protected reviewer notes in general audit/errors |
| `catalog/ai/tests/test_osl_candidate_adapter.py` | version bounds, allowlisted/minimized input, provider swap/cancellation/failure and no cross-session result |

### Import, export, migration and compatibility

| Proposed path | Permanent responsibility |
|---|---|
| `catalog/osl/tests/test_import_service.py` | quarantine/draft-only import, collisions, unsupported profile and external lifecycle as metadata |
| `catalog/osl/tests/test_export_service.py` | exact authorized revision, deterministic bytes/hash, diagnostics/event and no lifecycle mutation |
| `catalog/osl/tests/test_bundle_manifest.py` | checksum, duplicate/archive member and traversal protection |
| `catalog/osl/tests/test_sysml_v2_export.py` | relation/end mapping, stable names, escaping, unsupported semantics and deterministic output |
| `catalog/osl/tests/test_legacy_operator_note_v3.py` | legacy raw source retained; Decision/action ambiguity; reusable non-authority; outcome as feedback candidate |
| `catalog/osl/tests/test_compatibility_matrix.py` | profile/codec/schema/bundle reader-writer pairs, explicit deprecation and unknown-major behavior |
| `catalog/osl/tests/test_backup_restore.py` | restore exact hashes, history, source refs, decisions, publications and export records |
| `catalog/osl/tests/test_adversarial_inputs.py` | bounded malformed/import/extension/URI/path/Unicode/nesting/ID cases |

### Flask, templates and user journeys

| Proposed path | Permanent responsibility |
|---|---|
| `catalog/flask_app/test_osl_read_routes.py` | authorized read views, safe status/errors/cache and no GET mutation |
| `catalog/flask_app/tests/test_osl_write_routes.py` | bounded capture/candidate/edit/validate/review/approve/publish/import/export, status codes, replay/conflict |
| `catalog/flask_app/tests/test_osl_authorization.py` | unauthenticated, IDOR, wrong scope, forged actor/role/signer and independent source access |
| `catalog/flask_app/tests/test_osl_csrf.py` | every browser mutation rejects missing/stale/cross-session tokens |
| `catalog/flask_app/tests/test_osl_user_journeys.py` | complete manual and AI-optional workflow, feedback/revision/supersession |
| `catalog/flask_app/tests/test_osl_read_ui.py` | semantic labels, immutable version, provenance/history/compare and safe escaping |
| `catalog/flask_app/tests/test_osl_degraded_states.py` | empty, unavailable AI/exporter/validator, locked repository, unsupported profile, missing/redacted source and conflict |
| `catalog/flask_app/tests/test_osl_data_leakage.py` | source/AI/import strings, paths and protected identities absent from HTML/JSON/hidden inputs/errors/logs |
| `catalog/flask_app/tests/test_osl_accessibility_contract.py` | landmarks/headings/labels/legends/errors/table/status/keyboard-safe markup |
| `catalog/flask_app/tests/test_osl_mobile_layout_contract.py` | responsive stacking, touch targets, wrap/overflow, sequential compare and no hover-only action |
| `catalog/flask_app/tests/test_osl_legacy_compatibility.py` | old routes/tests remain legacy and cannot acquire canonical lifecycle semantics |

## Semantic rule fixture matrix

Create a source-pinned manifest:
`catalog/osl/tests/fixtures/profiles/research_v0_1/manifest.json`. It records the
selected internal profile ID, systems-paper SHA, rule citations, enforcement
status and explicit deviations/open questions.

Positive fixtures under `.../valid/`:

- `minimal_provisional.json`;
- `structurally_complete.json`;
- `domain_reviewed.json`;
- `alternatives_conflicts_composition.json`;
- `granular_traces.json`.

Each is a planned canonical contract fixture, not a verified operator strategy.

| Isolated negative fixture under `.../invalid/` | Required finding |
|---|---|
| `wf01_strategy_without_path.json` | no path |
| `wf02_zero_or_multiple_situations.json` | primary Situation cardinality |
| `wf03_no_observation_or_trigger.json` | no observable basis |
| `wf04_disconnected_reasoning.json` | no connected reasoning route |
| `wf05_unattached_gap.json` | gap has no affected target |
| `wf06_decision_without_response.json` | complete Decision has no candidate/selection |
| `wf07_invalid_candidate_selection_ends.json` | wrong endpoint/type or collapsed candidate/selection |
| `wf08_action_role_mismatch.json` | missing/incompatible action role |
| `wf09_inspection_causes_operational_outcome.json` | action/consequence type mismatch |
| `wf10_untargeted_or_unsourced_evidence.json` | evidence target/source missing |
| `wf11_unconnected_risk_tradeoff.json` | missing risk source/target or second trade-off element |
| `wf12_incomplete_domain_review.json` | review record/scope/blocker integrity |
| `wf13_ambiguous_path_trace.json` | known source collapsed into path-level trace |
| `wf14_composition_without_provenance.json` | provenance/participation/conflict not preserved |
| `wf15_cross_path_relation_leak.json` | relation violates path locality |

For unresolved applicability, `ValidationNeed`/maturity, generic relation
qualification, richer review scope and element-level composition, the manifest
must say `enforced`, `warning`, `deferred` or `unsupported`. Do not hide an
unresolved research decision with `xfail` or an undocumented implementation
default.

## Model and contract test obligations

Permanent tests prove:

- frozen objects cannot mutate after construction;
- required/missing/extra/wrong-type/bounds behavior is explicit;
- duplicate JSON key, object ID, element ID, relation ID and named end reject;
- language profile, canonical codec, storage schema, producer, validator and
  source-paper versions remain independent;
- unknown required profile/major fails closed;
- allowed optional extensions preserve namespace/value/canonical bytes and do
  not influence core validation;
- relation endpoints belong to the exact graph revision unless the selected
  profile explicitly supports an external/cross-path relation;
- source bytes/excerpt hashes survive drafts, review, migration, export,
  feedback and supersession;
- Decision is not OperatorAction; DecisionOption is not ActionSelection;
- a selected Action is not recommendation, approval, authorization, occurrence
  or execution;
- expected consequence is not observed Result;
- evidence review, validator result, reviewer disposition, approval,
  publication, confidentiality and paper maturity are not one status;
- no contract deserializes executable code, dynamic class/module names, tool
  calls, machine endpoints, credentials or authority grants.

## Parser and serializer tests

Required permanent cases:

- domain -> canonical JSON -> domain semantic equality;
- accepted non-canonical JSON -> canonical bytes -> stable reparse;
- deterministic ordering, UTF-8/LF policy and content hash;
- CRLF input, Unicode/combining characters, non-ASCII labels, UTC offsets and
  locale independence;
- duplicate keys/IDs, invalid encoding/surrogates and oversized scalar;
- excessive nesting, nodes, edges, text and total input size;
- unknown profile major and unknown required/optional extension behavior;
- dangling/locality/cross-version reference handling;
- object hooks, dynamic imports, URI dereferencing and code-like values remain
  inert or reject;
- parse failure creates diagnostics and no repository write;
- imported `approved`/`published`/actor data cannot set local lifecycle.

Round trip proves fidelity to the selected MSH codec. It does not prove that the
papers define JSON/YAML, that content is semantically valid, or that SysML v2 is
equivalent. YAML remains deferred. SysML conformance has its own adapter tests.

## Semantic validation tests

The validator is pure, deterministic, offline and resource bounded. Assert:

- exact WF rule IDs, stable machine codes, explanations, target element/relation
  IDs and deterministic finding order;
- identical revision/profile yields identical result and ruleset hash;
- unsupported profile returns `unsupported_profile`, never pass;
- resource/internal inability returns `incomplete`, never pass;
- validator writes no repository state and makes no lifecycle transition;
- it never infers factual truth, accepted evidence, human review, safety or
  operational validity;
- structurally-complete/domain-reviewed paper maturity remains separate from
  MSH Approved/Published.

Test the `paper-defined` distinctions directly:

- Observation versus Trigger versus Hypothesis versus Evidence;
- Goal versus expected consequence;
- Decision versus response;
- candidate versus selected response;
- inspection/diagnosis/monitoring/verification/organisational Result versus
  operational Outcome;
- source-backed versus accepted/reviewed Evidence;
- domain-reviewed path versus operational validation.

## Lifecycle and human-authority tests

Every state-changing case supplies a server-derived authenticated principal,
authorized scope, command ID/fingerprint, exact target revision/hash, expected
aggregate revision and policy version.

Permanent negative tests prove:

- client-supplied reviewer/approver/publisher identity rejects;
- capture cannot create candidate, draft, review, approval or publication;
- candidate generation cannot create a canonical revision;
- validation cannot submit review;
- paper domain review cannot create MSH approval;
- approval cannot publish without a separate authorized command/policy;
- publication cannot activate or execute;
- import/export cannot alter lifecycle;
- correction/narrowing creates a new draft and does not transfer decisions;
- any content/profile change makes old validation/review/approval stale;
- AI cannot review, approve, publish, deprecate, supersede, verify evidence,
  select/authorize Action or assign authority;
- federation membership/write authority, capability job ownership, compute
  provider registration and artefact grants satisfy no OSL permission;
- OSL publication grants no storage, compute, federation, artefact or machine
  authority;
- no initial model/service/route/event/export exposes an operational binding.

This enforces the paper boundary that selected responses are descriptive and
domain review is not operational validation
(`systems-paper/evaluation/osl-semantic-contract.md:119-156,259-288`).

## Provenance preservation tests

Permanent assertions derived from the Notebook-to-OSL source boundary:

- exact source bytes/text never change after sealing;
- correction creates a new source revision and lineage;
- excerpt selector binds an exact source revision and content hash;
- multiple excerpts may support a specifically targeted claim and one excerpt
  may support multiple distinct candidate claims;
- candidate/revision/review/export identifies immediate inputs or an explicit
  quarantine finding;
- human, deterministic tool, import and AI derivations remain distinguishable;
- AI provider/model/template/run provenance is retained without leaking full
  prompt/result into general audit;
- provenance rejects cycles;
- validation/review/approval/publication/export binds exact profile/hash;
- evidence supersession retains prior assessments;
- deleting/rebuilding a cache/projection cannot delete evidence/provenance;
- migration retains legacy source ID/hash and paper repository/SHA/path;
- a failed state/event transaction commits neither half.

These trace to `paper-repo/.../03_research_design.tex:31-44`,
`04_method.tex:43-80` and `05_annotation_schema.tex:27-49,110-129`.

## Authorization and data-leakage tests

Use a principal matrix with anonymous, authenticated/no-scope, wrong site,
expired/revoked policy, reviewer-only, approver-only, publisher-only,
source-reader-only and fragment-reader-only cases. Cover:

- IDOR attempts for source, excerpt, candidate, revision, review, export and
  history;
- not-found/forbidden public shape and timing class where practical;
- list/search/count cannot reveal forbidden existence;
- "latest" means latest visible revision, not global latest;
- source permission independent of fragment permission;
- redaction of raw source, prompt/result, reviewer identity/note, provider/path,
  private external references and audit detail;
- safe errors without payload excerpt, SQL, path, stack, credentials or hashes
  that become oracles;
- export audience/classification independently authorized;
- cached projection revision/hash never accepted as a canonical write.

Put `SENSITIVE_OSL_SENTINEL_DO_NOT_EXPOSE` in denied synthetic data and assert
its absence from HTML, JSON, hidden inputs, logs, exceptions, audit, counts,
search, unauthorized AI requests and exports.

## AI adversarial and non-authority tests

Fake providers return cases that attempt to:

- obey notebook prompt injection to read other sources, call tools, write
  storage, approve or execute;
- cite excerpt/source IDs outside the allowlist;
- fabricate plausible evidence;
- set `review_status=accepted`, `approved_by`, `published`,
  `selected_action` or capability/storage authority;
- return oversized/deep/unknown types, cross-session results, HTML/script/SysML
  delimiter injection, unknown confidence, timeout or partial output.

Expected behavior:

- source instructions are treated as source data;
- only policy-permitted allowlisted excerpts reach the provider;
- fabricated references reject;
- valid output persists only as an attributed candidate;
- lifecycle/authority fields reject or remain inert untrusted payload;
- no canonical revision exists until an explicit human draft command;
- manual segmentation/edit/validation/review/publication navigation remains
  available when AI is disabled or fails.

`paper-defined`: paper-repo assigns interpretation and validation to humans and
does not evaluate AI. All AI behavior here is `proposed-for-MSH` with mandatory
human control.

## Repository and storage adapter tests

Inject failures and concurrency; happy paths are insufficient:

- source + hash + provenance event atomicity;
- revision/validation/review/approval/publication/export record + event
  atomicity;
- same command/fingerprint replay versus command-ID collision;
- expected-revision conflict returns a safe current projection;
- concurrent edits never overwrite and review/approval of a changed revision
  fails;
- simultaneous publications converge idempotently or one conflicts;
- failed publication cannot create a published event;
- restart reconstructs identical current state and ordered events;
- locked DB, transaction failure, integrity error, hash mismatch, corrupt row
  and migration failure fail closed;
- no update/delete API exists for a sealed source or fragment revision;
- parameterized SQL and path containment reject injection/traversal;
- Windows and Linux file locking/path behavior both pass.

## API, route, template, accessibility and mobile tests

Assert application behavior and semantic labels rather than template internals:

- unauthenticated/forbidden commands reject and all browser mutations require
  valid CSRF;
- server context supplies actor/role/scope; client context cannot override it;
- expected revision and idempotency token are mandatory;
- stale edit/review preserves draft input and offers comparison;
- oversized upload rejects before decode/AI;
- import creates quarantine/source/candidate/draft only;
- AI failure preserves manual workflow;
- findings link to exact element/relation and authorized context;
- review UI distinguishes source, interpretation, candidate/selection,
  validation and human review;
- approval says it is not operational/safety/execution authorization;
- published detail is immutable and has permitted version/provenance/history;
- comparisons identify added/removed/changed semantic elements/relations;
- empty, unavailable, unsupported, missing, redacted and repository-degraded
  states have explicit recovery;
- all untrusted text escapes in HTML, attributes, filenames and errors;
- fields/errors/headings/tables/status/focus/keyboard paths are accessible;
- narrow viewport stacks panes, wraps IDs/findings, retains primary actions and
  uses no hover-only control.

`requires-research-clarification`: MSH has no current browser automation/a11y
dependency. Initial permanent gates use Flask/semantic HTML/static CSS contract
tests. A later pinned browser and automated a11y engine supplements, never
replaces, server authorization/leakage tests.

## Import, export, SysML and adversarial tests

Import coverage:

- media type, size, nesting, object/decompression limits;
- duplicate keys/IDs, collisions, archive traversal/unsafe names;
- no remote URI resolution or SSRF;
- exact unknown profile/extension policy and quarantined bytes/hash;
- external reviewed/approved/published remains untrusted metadata;
- ambiguous legacy mappings require human disposition.

SysML v2 export coverage:

- exact selected revision/profile/exporter version and source-to-output ID map;
- faithful keyword/type/relation/named-end mapping;
- deterministic collision handling or explicit failure;
- escaping of strings/comments/identifiers;
- no guessed Decision, selection, evidence review/confidence, trace target or
  expected consequence;
- unsupported semantics fail with diagnostics rather than fallback text;
- exporter failure cannot change revision/publication;
- a pinned supported parser/profile accepts output before MSH calls the adapter
  supported.

SysML import/full round trip is `requires-research-clarification` and deferred
until independent fidelity evidence exists.

Adversarial fixtures under `catalog/osl/tests/fixtures/adversarial/`:

- `duplicate_json_keys.json`, `duplicate_element_ids.json`,
  `deep_nesting.json`, `unknown_required_extension.json`;
- `dangling_cross_revision_reference.json`, `forged_lifecycle_and_actor.json`,
  `external_uri_ssrf.json`;
- `html_script_injection.json`, `sysml_identifier_comment_injection.json`,
  `provenance_cycle.json`;
- `ai_fabricated_source_reference.json` and
  `legacy_reusable_auto_approval.json`.

Generate very large/resource-limit inputs in tests instead of committing huge
payloads. Fixtures contain no live endpoint, credential or executable code.

## Migration and backwards-compatibility tests

Keep five mechanisms distinct in test names and reports:

1. repository schema migration;
2. product contract/canonical codec migration;
3. OSL language-profile migration;
4. legacy operator-note or paper-example mapping;
5. exporter compatibility/stability.

For every supported edge:

- input fixture is immutable and identifies schema/profile/hash;
- dry run lists mapping, loss, unknowns, collisions and human decisions;
- migration is atomic, idempotent and restart-safe;
- failed migration leaves prior store/read path usable;
- rollback preserves newly captured source and does not erase reports;
- logical identity/lineage and exact revisions remain explicit;
- source, excerpt, provenance, review, audit and export history survive;
- optional extensions survive per policy; required unknown semantics fail;
- no approval, publication, evidence verification, Action selection or authority
  is copied;
- old reader/writer support exists only for a documented window and returns
  explicit deprecated/unsupported diagnostics.

The paper-repo conflicts—different status sets, `sensitive` mixed with review,
ambiguous action modality and field-name drift—become explicit legacy mapping
fixtures, not one canonical enum. Sources:
`paper-repo/papers/notebook-to-osl/capture-schema.md:16-42` and
`.../manuscript/sections/05_annotation_schema.tex:53-108`.

## Linux and Windows matrix

Every OSL implementation PR runs supported Python 3.12 on both
`ubuntu-latest` and `windows-latest`. Both execute:

- compile of `catalog/osl/` and OSL Flask/AI adapters/tests;
- complete `catalog/osl/tests`;
- complete `catalog/flask_app/test_osl*.py` and
  `catalog/flask_app/tests/test_osl_*.py`;
- canonical codec/hash fixtures;
- SQLite transaction, replay, restart and migration cases;
- import/export escaping/path/legacy mapping;
- workflow, authorization, provenance and AI fencing.

Cross-platform assertions:

- canonical serialization is specified UTF-8 with LF bytes;
- hash is independent of CRLF checkout/native separator;
- no locale-dependent sort, number or timestamp;
- UTC and source-local context remain explicit;
- use `pathlib`/temporary dirs; no hard-coded slash, drive or home path;
- test Windows reserved/case/non-ASCII/long path components and file locks;
- bound SQLite concurrency/retry on both systems;
- no core test depends on Bash, symlink, fork, signals or `/tmp`;
- SysML output bytes are identical across matrix.

A platform skip needs a documented unsupported capability plus a tested
degraded state. Core contracts, WF rules, authority, provenance, codec,
repository, migration and deterministic export cannot skip either platform.

## Permanent GitHub Actions plan

Create `.github/workflows/osl-core.yml` as the stable required workflow. An
optional `.github/workflows/osl-ui.yml` may split UI/browser runtime only after
the aggregate requirement remains reliable.

### Trigger and security

- all pull requests without a path filter, so required check is never absent;
- pushes to `main` and optional manual dispatch;
- `permissions: contents: read`;
- concurrency with cancel-in-progress;
- Python/dependencies constrained consistently with current MSH workflows;
- explicit timeouts, no `continue-on-error`;
- no live AI, remote source, federation, database server or machine connection.

### Jobs

1. `osl-cross-platform`
   - Ubuntu/Windows matrix;
   - compilation, all focused core/Flask tests, canonical hashes and migrations.
2. `osl-security-adversarial`
   - authority, leakage, AI fencing, malformed/import, XSS/SysML escaping and
     resource bounds; OS-sensitive cases also stay in the matrix.
3. `osl-lint-hygiene`
   - Ruff on every new OSL/core/adapter/test file with no new ignore list;
   - profile/fixture manifest consistency, import boundary and diff whitespace.
4. `osl-sysml-conformance`, added with D9-B
   - pinned parser/profile, golden mappings and parser acceptance;
   - named as adapter conformance, not universal SysML equivalence.
5. `osl-required`
   - stable aggregate depending on every applicable job;
   - fails if a dependency fails, is cancelled or unexpectedly skipped;
   - this one name becomes branch-protection requirement.

The existing full repository suite remains separate/additive. Focused OSL CI
must not mask an unrelated repository failure.

## Permanent merge gates by delivery

| Delivery | Tests that become non-optional at that merge |
|---|---|
| D0/D1 concepts/profile/contracts | models, IDs, profile source manifest, import boundary, canonical codec/hash |
| D2 semantic validation | WF1-WF15 positive and isolated negative, traceability and deterministic/incomplete behavior |
| D3 persistence/provenance | blob/SQLite, immutability, replay, concurrency, failure atomicity, migration and audit redaction |
| D4 lifecycle/read projections | transition/stale target, authorization, latest-visible, no-oracle and redaction |
| D5 candidate workflow | source/excerpt/candidate provenance, manual workflow, recovery |
| D6 review/approval/publication | exact hash/scope, human authority, separation and no execution |
| D7 UI/API | auth, CSRF, leakage, routes, accessibility/mobile and degraded states |
| D8 AI | prompt injection, allowlist, candidate-only output, authority fencing and manual fallback |
| D9 import/export/SysML/migration | adversarial import, deterministic export, pinned SysML, legacy mapping and compatibility matrix |
| D10 hardening | every retained migration/reader edge, backup/restore and complete Linux/Windows aggregate |

Once a permanent test lands, do not remove/weaken it merely to pass a later
phase. A semantic expectation change requires source/profile decision and
citation, new profile/ruleset version if applicable, compatibility assessment,
updated positive/negative fixtures and explicit claim-boundary review.

## Merge-blocking conditions

The stable gate blocks merge on:

- contract/codec/hash mismatch or OS nondeterminism;
- undocumented profile rule/deviation;
- enabled WF rule without positive and isolated negative coverage;
- validator nondeterminism or incomplete treated as pass;
- source/provenance mutation, missing lineage or partial state/event write;
- lifecycle/authority escalation or stale write accepted;
- forged actor/scope/signer accepted;
- leakage sentinel in forbidden response/log/error/export/AI request;
- AI/import/serializer/validator/UI/federation/capability path gaining review,
  approval, publication, execution or resource authority;
- repository replay collision, recovery or migration loss;
- malformed/adversarial crash, unbounded work, SSRF, traversal, XSS, SQL or
  SysML injection;
- unsupported SysML semantics silently guessed or supported output rejected by
  the pinned environment;
- Ruff/compile/import-boundary/diff hygiene failure.

No coverage percentage alone is a merge gate. Meaningful completeness metrics
are semantic-rule positive/negative coverage, transition coverage,
authority-negative coverage, migration-edge coverage and exact failure
invariants.

## CI reports and data handling

Retain concise, non-sensitive artifacts:

- JUnit results;
- profile/rule coverage summary;
- semantic positive/negative case summary;
- migration compatibility matrix;
- canonical hash summary by OS;
- SysML adapter diagnostics when applicable.

Artifacts contain IDs, codes, fixture names, counts and non-sensitive hashes
only. Never upload raw protected source, prompt/results, credentials, private
endpoints, unrestricted databases or generated operator content.

## Decisions fixed by this plan

- `proposed-for-MSH`: tests target a source-pinned bounded profile, not an
  unqualified OSL standard claim.
- `proposed-for-MSH`: every enabled semantic rule has positive and isolated
  negative evidence.
- `proposed-for-MSH`: validator failure/incompleteness is never pass.
- `proposed-for-MSH`: authority, leakage, provenance and recovery negatives are
  permanent gates, not optional security follow-up.
- `proposed-for-MSH`: canonical bytes/hashes and SQLite/migration behavior run on
  Linux and Windows.
- `proposed-for-MSH`: existing legacy operator/export tests remain labelled
  legacy until explicit migration.
- `proposed-for-MSH`: one stable `osl-required` check protects the branch while
  internal jobs may evolve.

## Open decisions

- `requires-research-clarification`: first profile disposition of applicability,
  `ValidationNeed`/maturity, relation qualification, review scope and
  element-level composition.
- `requires-research-clarification`: public profile/version name and supported
  reader/export retention window.
- `requires-research-clarification`: canonical Unicode, numeric/time and
  extension rules needed for cross-platform hashes.
- `requires-research-clarification`: human identity/policy provider and mandatory
  reviewer/approver/publisher separation.
- `requires-research-clarification`: accepted browser/accessibility toolchain and
  manual audit owner.
- `requires-research-clarification`: pinned SysML v2 parser/tool/profile that
  defines supported export conformance.
- `requires-research-clarification`: privacy/retention cases including consent
  withdrawal and encrypted erasure without false provenance claims.

## Acceptance criteria for later implementation

This plan is satisfied only when:

1. every requested level has an owned test path;
2. WF1-WF15 have positive and isolated negative coverage for the selected
   profile, and open semantics are declared rather than guessed;
3. source/provenance, evidence dimensions, Decision/Action,
   expected/observed, review/approval and authority boundaries have permanent
   negative tests;
4. malformed/import/AI inputs cannot escalate lifecycle or authority;
5. persistence/migration prove atomicity, replay, restart, rollback and history
   preservation;
6. API/UI prove auth, CSRF, leakage, degraded states, accessibility and mobile;
7. canonical bytes/hash, repository, migration and export pass Ubuntu/Windows;
8. `.github/workflows/osl-core.yml` supplies stable `osl-required` branch
   protection;
9. passing results are described as profile/product conformance only, never
   operational truth, safety or execution readiness.
