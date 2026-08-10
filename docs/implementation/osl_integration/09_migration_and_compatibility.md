# OSL migration and compatibility plan

## Status and analyzed sources

This is an implementation plan only. It does not implement a migration, freeze
an external compatibility promise or make a paper example canonical.

| Repository | Analyzed commit |
|---|---|
| `Nettking/msh` | `f580c71f7269643a077cc7e7db8ba9bf6050bb6a` |
| `Nettking/systems-paper` | `ff098ce52f15b489b6a07d5b55c6c788d862e3be` |
| `Nettking/paper-repo` | `abe3fbcddee590c3f399b06f63cb329e8615977c` |

The markers `paper-defined`, `existing-in-FCP`, `proposed-for-FCP` and
`requires-research-clarification` follow
[00_scope_and_sources.md](00_scope_and_sources.md).

Core rule: language semantics, product contract, repository schema, producer,
validator and exporter are versioned independently. A migration produces a new
immutable revision, a report and provenance. It never rewrites source evidence,
silently transfers approval or grants operational authority.

## Source and claim boundary

- `paper-defined`: `systems-paper/publication-readiness.md:1-6` and
  `evaluation/osl-final-reassessment.md:14-16` label the artefact
  "OSL v0.1-alpha / research prototype", not a stable public compatibility
  contract.
- `paper-defined`: `evaluation/osl-semantic-contract.md` sections 1-16 are the
  intended semantic commitments; `sysml/osl-core.sysml:1-15` is the preliminary
  authoritative abstract syntax; `evaluation/osl-final-reassessment.md:35-92`
  bounds what the current profile demonstrates. These sources do not all have
  the same authority.
- `paper-defined`: semantic contract section 13 (lines 468-489) excludes
  execution, factual truth, causal proof, safety assurance, operational
  correctness, automatic conflict resolution and automatic Digital Twin
  implementation.
- `paper-defined`: raw Notebook-to-OSL material remains available after
  annotation because interpretation can change
  (`paper-repo/.../05_annotation_schema.tex:27-50`).
- `paper-defined`: the method does not deliver a complete OSL and treats OSL as
  one possible downstream representation
  (`03_research_design.tex:89-93`); the illustrative case syntax is not final
  (`06_illustrative_case.tex:49-52`).
- `existing-in-FCP`: `data/operator_strategy_records/operator_strategies.json`
  uses outer schema `fcp.operator_strategy_records.v3` and mutable, permissively
  normalized flat records
  (`catalog/flask_app/services/operator_strategy_service.py:19-95,233-303`).
- `existing-in-FCP`: `catalog/flask_app/services/osl_export_service.py:10-139`
  overwrites one generated file and has no language/profile version,
  provenance, semantic validation, extension policy, export history or round
  trip. It also invents/defaults semantics and is not a migration source of
  truth.

## Independent version axes

| Axis | First planned working value | Meaning and compatibility boundary |
|---|---|---|
| language/profile | `fcp.osl.profile.systems-paper-bounded.v1-plan` | `proposed-for-FCP` internal bounded profile; not a claim of OSL 1.0 |
| research label | `OSL v0.1-alpha` | `paper-defined` label only; no product stability implication |
| research source | systems-paper SHA `ff098ce...` | exact semantic evidence baseline |
| canonical contract/codec | `fcp.osl.graph.v1-plan` | `proposed-for-FCP` JSON/wire representation, not language semantics |
| repository schema | monotonically numbered OSL SQLite schema | physical persistence only |
| producer/validator | implementation name/version/ruleset hash | reproducibility and diagnostics |
| export representation | format/profile/adapter version | JSON bundle, SysML v2 or future projection; never whole language |
| HTTP API | `/api/osl/v1` | transport contract, independent from all axes above |

The `-plan` suffixes mean these are planned identifiers, not implemented or
accepted contracts. `requires-research-clarification`: the research/product
owners must approve any public-facing OSL version name.

`proposed-for-FCP`: the first registry descriptor in
`catalog/osl/profiles/research_v0_1.py` implements only the bounded research
profile supported by the analyzed core/reassessment, selected WF1-WF15
interpretations and explicit FCP envelope extensions. It must not claim every
aspirational semantic-contract concept.

Revision identity, content hashes, product lifecycle, approval/publication,
access policy, provenance storage and idempotency are `proposed-for-FCP` product
contracts. They must not be documented as `paper-defined` OSL semantics.

## Introducing the first supported version

Use staged gates, each independently reversible/reviewable:

1. **Decision gate (D0):** approve the source-pinned bounded profile,
   authority policy, version axes, canonicalization and extension rules in docs.
2. **Read-only profile gate (D1/D2):** register profile, pure models, canonical
   JSON decoder/encoder and positive/negative validator fixtures; accept no
   production writes.
3. **Repository gate (D3/D4):** deploy empty versioned repository/blob storage,
   backup/verify tooling and read-only projections; keep legacy source writable
   until cutover policy is ready.
4. **Workflow gate (D5/D6):** enable new canonical capture/drafts and human
   decisions only after auth/policy, provenance and recovery tests pass.
5. **Legacy report gate (D9-C):** inventory/hash and dry-run legacy mapping;
   resolve duplicates/ambiguity without canonical writes.
6. **Cutover gate (D10):** make legacy writes read-only, enable canonical writer,
   retain a legacy read projection and never dual-write.

Initial constraints:

- canonical JSON is the first product codec;
- YAML is deferred until a real compatibility requirement and duplicate/type/
  canonicalization policy are accepted;
- SysML v2 is export-only initially;
- no SysML or illustrative paper syntax is a canonical import;
- no operational binding/executable payload exists;
- unknown profile majors and unknown required extensions fail closed;
- bounded unknown input may enter quarantine only;
- import creates source/candidate/draft at most;
- validation, review, approval and publication bind exact revision/hash.

## Compatibility policy

### Exact known profile and codec

A known profile plus compatible codec is decoded without fallback, structurally
checked and semantically validated under that exact profile. Lifecycle/access
policy is evaluated separately. Decoding cannot create review or approval.

A retained older codec may read its own bytes. Converting to a newer codec or
profile creates a new immutable serialized artefact or fragment revision with a
`MigrationReport`; historical bytes/hash stay unchanged.

### Unknown language/profile

- Unknown major: reject canonical validation, review eligibility, approval,
  publication and canonical export.
- Record original bounded bytes, detected identifier and hash in protected
  quarantine when policy permits.
- Do not normalize the identifier or choose the numerically nearest profile.
- Offer only metadata/raw preservation and an explicit future migration path.

### Additive/same-major change

Numeric similarity is insufficient. Compatibility exists only when the
registry declares a directed edge and its tests. An optional addition is
preserved only when:

- core profile is known;
- namespace/version is explicit and cannot shadow core;
- required/optional status and bounded schema are declared;
- payload is policy-permitted;
- canonical hash behavior is defined;
- validation reports extension semantics as validated or unvalidated honestly.

### Readers, writers and downgrade

- A reader may support multiple declared profiles/codecs; a writer selects one
  exact active target and never writes "latest" ambiguously.
- A decoder/profile cannot be removed while persisted revisions depend on it
  unless an audited migration, retained original bytes, fixtures, historical
  export policy and approved deprecation decision exist.
- There is no implicit downgrade. Down-export succeeds only if every construct
  maps faithfully.
- A lossy preview may omit constructs only when watermarked non-canonical,
  every loss is listed, it cannot support review/approval/publication, and the
  source revision is unchanged.

`requires-research-clarification`: define minimum reader/export retention
windows and future same-major promises. This plan does not promise indefinite
external compatibility.

## Explicit language/profile migration

A migration is a named directed registry edge, not generic normalization. Its
input includes:

- exact source profile/codec/revision/content hash;
- exact target profile/codec;
- migrator name/version and mapping policy;
- authenticated command ID/fingerprint/expected revision;
- optional explicit human mapping choices for declared ambiguities.

It produces atomically:

- new immutable draft revision;
- `MigrationReport` with stable finding codes;
- source-to-target element/relation ID maps;
- preserved optional extensions;
- every unsupported, lossy or ambiguous mapping;
- provenance from each output to immediate input;
- deterministic output hash when the mapping is deterministic.

It must not:

- mutate/delete source revision or source artefact;
- copy review, approval, publication or audience to the target;
- turn Evidence into verified fact;
- collapse Decision into OperatorAction or candidate into selection;
- resolve gaps, conflict, uncertainty or ambiguous relations automatically;
- invent rationale, evidence, actor, outcome or external target;
- grant execution, storage, compute, federation or artefact authority.

Even lossless conversion requires target-profile validation. Old review/approval
remain historical records for the old hash. Lossy/ambiguous conversion stops at
quarantine/draft and requires a human mapping decision. Same command/source/
target/migrator/fingerprint replays the same result.

## Existing FCP operator records

### Preserve the legacy source first

Before parsing, ingest unchanged legacy JSON bytes as a restricted migration
SourceArtefact with file hash, size, detected encoding, capture path held in
protected metadata, ingest actor/time and claimed source schema. Do not rewrite
the existing file.

The adapter must read both accepted outer forms—a top-level list and an object
with `records`—because `operator_strategy_service.py:82-95` accepts both. Each
legacy record receives:

- original record ID/value when present;
- new opaque import identity;
- canonical snapshot/hash of parsed legacy record;
- link to unchanged file artefact;
- exact original field/value map in protected import evidence;
- parse, normalization, collision and mapping findings.

Missing/duplicate legacy IDs never overwrite. Assign a new identity and record
the conflict.

### Conservative field mapping

| Legacy input | Planned migration disposition |
|---|---|
| `raw_statement` | immutable SourceExcerpt against the unchanged file source |
| missing `raw_statement` | preserve whole record snapshot; finding `missing_raw_source`; fabricate no Observation |
| `decision` | unverified Decision candidate and/or `decision_action_ambiguous` finding |
| `action_type` | candidate action-role hint only |
| `observation`, `trigger`, `context`, `hypothesis`, `goal`, `rationale`, `risk`, `trade_off` | candidate claims with field-level legacy provenance |
| `expected_outcome` | candidate expected consequence |
| `outcome` or `worked` | unverified feedback/Result candidate, not expected outcome or verified fact |
| `evidence`, `confidence`, `trace_target` | free-text candidate metadata plus missing structured-reference/review findings |
| confirmation, quality, first-part, telemetry, machine IDs | unresolved ExternalModelReference candidates until schema/time/access/integrity checks pass |
| `reusable_strategy=true` or reusable status | `legacy_reusable` source annotation only; never review/approval/publication/maturity |
| captured/structured/other legacy status | preserved source metadata only |
| generated SysML/recommender artefact | historical derived artefact at most; never canonical graph or evidence truth |

This conservatism is required because current FCP can copy raw text into
`decision`, infer action type, overwrite structure/outcome, mark reusable
without semantic review and delete source
(`operator_strategy_service.py:119-251,329-368`).

Telemetry, intervention configurations, support/recommender cards, capability
jobs, provider results, federation authority, artefact grants and generated
SysML remain in their owning stores. OSL may later hold typed, authorized
external references; migration must not copy them into canonical ownership.

### Legacy dry-run and cutover sequence

1. inventory and hash legacy JSON and known generated artefacts;
2. parse/map dry-run with zero canonical writes;
3. report disposition/finding counts, duplicate IDs, missing sources,
   ambiguity and sensitivity;
4. require explicit human batch decision for unresolved high-severity cases;
5. transactionally commit source/excerpt/candidate/provenance plus command result;
6. rerun same batch to prove idempotency;
7. reconcile input/output/skip/error counts and hashes;
8. make legacy writes read-only before enabling canonical capture;
9. serve legacy history through a clearly labelled read projection;
10. retire legacy mutation/export only after parity, authorization and rollback
    verification.

There is no canonical/legacy dual-write. It would create two mutable sources of
truth and incompatible lifecycle semantics.

## Paper-based and older examples

`paper-defined`: the Notebook-to-OSL CNC example is generic, sanitized and
illustrative (`paper-repo/.../06_illustrative_case.tex:1-8`). `paper.yaml:50-61`
still requires empirical material and operator/domain validation.

`proposed-for-FCP` compatibility treatment:

- paper examples are test fixtures, never production seed strategies;
- every fixture records repo, commit, path/line range, synthetic/sanitized
  status and absence of real operator approval;
- preserve the full quote at `06_illustrative_case.tex:14-16` as source;
- the shorter `raw_text` at line 33 is a derived excerpt, not replacement;
- the OSL block at lines 53-64 is illustrative legacy syntax/quarantine, not
  conformant canonical JSON or systems-paper profile;
- `capture-schema.md:33-42` and
  `05_annotation_schema.tex:83-106` remain separate fixtures because their
  status sets conflict;
- `validated`, `formalized`, `accepted` or `model-ready` in paper content never
  becomes local approval/publication;
- systems-paper examples/corpus entries are source-pinned conformance fixtures,
  not verified operational strategies.

The optional `catalog/osl/legacy/paper_examples.py` adapter in D9-C is enabled
only for explicitly approved fixture formats. Unknown paper syntax remains raw
quarantine with diagnostics.

## Immutable evidence through migration

After commit, these are immutable:

- original source bytes/text and hash;
- source excerpts/selectors/hashes;
- fragment revisions;
- validation results;
- reviewer and approval decisions;
- publication/deprecation/supersession records;
- migration reports/provenance events;
- export artefacts and records.

A correction or redacted derivative is a new object linked to the original.
Access-policy change restricts retrieval but does not rewrite evidence. A
profile/codec migration may change representation but cannot change the excerpt
it cites.

Every migrated claim retains immediate source/excerpt, original legacy field or
paper location, migration batch/migrator version, finding/disposition,
actor/tool/time and input/output hashes.

`requires-research-clarification`: legal erasure, consent withdrawal and
encrypted deletion require a separate privacy/retention policy. Do not invent a
hard-delete or tombstone rule under the label of OSL migration.

## Repository schema migration

Repository schema version is independent from language and codec. Migrations in
`catalog/osl/schema.py` are:

- ordered and explicit; released steps are never edited in place;
- transactional where SQLite permits;
- additive before cutover;
- restart-safe and idempotent;
- tested against empty, every retained version, partial and corrupt stores;
- fail-closed on unknown domain versions;
- recorded with from/to schema and application build version.

A storage migration may add tables/indexes or copy encoded bytes, but cannot
silently alter canonical content/hash. A required canonical encoding change is
a codec/profile migration producing a new artefact/report, not a hidden SQL
rewrite.

## Rollback and recovery

Rollback is non-destructive:

- profile migration rollback selects the preserved prior revision; migrated
  revision/report remain;
- publication rollback appends withdrawal/deprecation/supersession, not delete
  or mutable status reset;
- export rollback retrieves a stored older export; a new exporter never
  regenerates bytes and claims they are the old artefact;
- failed/aborted import retains source and diagnostics but no partial aggregate;
- transaction failure commits neither state nor event;
- cutover failure after canonical writes does not re-enable legacy mutation or
  dual-write. Safe degraded mode is read-only plus disabled capture until the
  canonical writer recovers;
- application rollback uses additive schema/retained readers. Destructive down
  migration is not the normal path;
- corrupt migration targets quarantine while valid sources remain usable.

Before cutover prove:

- legacy input hash unchanged;
- all counts reconcile;
- every candidate/draft has source provenance;
- no legacy reusable/status became review/approval/publication;
- no source/revision mutated;
- restart/retry is deterministic.

## Export stability

Each export record includes:

- language/profile ID and systems-paper source commit;
- canonical contract/codec and validator/ruleset;
- exporter name/version;
- exact fragment revision/content hash;
- audience/classification and requester authorization;
- output hash, byte size, media type and protected storage ref;
- source-to-output ID map;
- warnings, unsupported constructs and extension policy;
- timestamp outside canonical content where determinism requires it.

Canonical JSON specifies UTF-8, LF, key ordering, numeric/date/Unicode rules,
duplicate rejection and hash input. These are `proposed-for-FCP` codec rules,
not paper semantics.

The same revision/profile/exporter/options yields byte-identical deterministic
output on Linux and Windows. Generation time and request identity belong in the
ExportRecord, not canonical bytes.

SysML v2 is a versioned projection:

- never repository source of truth;
- emits only faithfully supported concepts;
- unsupported relations/extensions produce diagnostics;
- identifier collision fails or uses a deterministic recorded map;
- no fallback Decision, Action, selection, evidence status or target type is
  invented;
- import remains deferred until independent fidelity/round-trip evidence.

Export changes no validation, review, lifecycle, evidence or authority.

## Unknown extensions

Every supported extension registers:

- namespaced identifier and version;
- owning profile/organization;
- required or optional status;
- bounded payload schema/size and allowed target kinds;
- canonicalization/hash behavior;
- validation, migration and export policy.

Rules:

- core names cannot be shadowed or reinterpreted;
- unknown optional extensions are preserved losslessly and reported
  semantically unvalidated;
- unknown required extensions block validation pass, review eligibility,
  approval, publication and canonical export;
- extension rules cannot weaken WF1-WF15 selected core behavior;
- extension content cannot carry executable code, tool calls, machine address,
  secret, signer assertion or authority grant;
- conversion uses a registered handler or preserves unchanged with finding;
- removal is a named lossy projection, never silent normalization;
- extension bytes/values participate in revision identity/hash.

`requires-research-clarification`: quantitative confidence, operational
thresholds, arbitrary relation qualification and richer review dispositions
need research/profile approval before becoming extensions.

## Deprecation

Language/profile, codec, exporter, extension and fragment lifecycle deprecation
are distinct. A registry version may be:

| Registry state | New authoring | Read/inspect | Validate/export | Migration |
|---|---|---|---|---|
| `active` | allowed | allowed | allowed | as declared |
| `deprecated` | warning or policy-disabled | allowed | retained where compatible | offered |
| `read_only` | disabled | allowed | historical export only | offered when faithful |
| `unsupported` | disabled | protected raw/quarantine metadata only | no canonical claim | future/manual only |

Deprecation requires an immutable decision/reason, replacement or explicit none,
faithful migration edge where possible, affected-revision inventory, API/UI
diagnostics, retained decoder/fixtures, export preservation and approved
earliest-removal release.

A version ID is never reused with changed semantics. Old fragment revisions are
never relabelled. Fragment Deprecated/Superseded does not deprecate its language
profile; profile deprecation does not assert that represented knowledge is
false.

## Planned implementation ownership

The exact file plan is authoritative in
[06_repository_file_plan.md](06_repository_file_plan.md). Migration and
compatibility responsibilities are owned as follows:

| Planned path | Responsibility |
|---|---|
| `catalog/osl/versioning.py` | separate profile/contract/revision identities and lineage |
| `catalog/osl/language_registry.py` | exact readers/writers, compatibility edges, registry/deprecation state and fail-closed lookup |
| `catalog/osl/profiles/research_v0_1.py` | first source-pinned bounded descriptor and explicit deviations |
| `catalog/osl/serialization/contracts.py` | version envelope, canonical bounds/media and extension envelope |
| `catalog/osl/serialization/json_codec.py` | strict canonical JSON decoding/encoding/hash input |
| `catalog/osl/schema.py` | ordered repository schema migrations and future-version refusal |
| `catalog/osl/repository.py` / `sqlite_repository.py` | atomic immutable revisions, command replay, migration/report persistence |
| `catalog/osl/import_service.py` | bounded dry-run, profile dispatch, quarantine/draft disposition and registered mapping coordination |
| `catalog/osl/export_service.py` | exact-revision export and immutable ExportRecord |
| `catalog/osl/exporters/sysml_v2.py` | deterministic, diagnostic export-only adapter |
| `catalog/osl/legacy/operator_note_v3.py` | conservative current FCP record mapping |
| `catalog/osl/legacy/paper_examples.py` | source-pinned approved fixture mappings only |
| `catalog/osl/legacy/migration_report.py` | batch disposition/finding/id-map/report contracts |
| `catalog/osl/cli.py` | offline inventory, backup, dry-run, commit, verify and report commands |
| `docs/osl_compatibility_policy.md` | supported/deprecated profile, codec, importer/exporter and extension policy |

Do not create a generic profile-to-profile migrator module before a second
profile and explicit mapping edge exist. At that point, add a narrowly named
module and update `06_repository_file_plan.md` before implementation; do not
hide semantic conversion in a generic helper.

No migration owner imports Flask templates, AI provider objects, federation
leadership, capability authority, recommender artefacts or machine-control
surfaces.

## Planned compatibility fixtures and tests

| Proposed path | Required coverage |
|---|---|
| `catalog/osl/tests/test_language_registry.py` | exact/unknown profile, declared edges, deprecation and source SHA |
| `catalog/osl/tests/test_json_codec.py` | canonical round trip/hash, malformed/unknown/duplicate and extension behavior |
| `catalog/osl/tests/test_schema_migrations.py` | empty/current/every retained/partial/corrupt/restart/concurrent migration |
| `catalog/osl/tests/test_legacy_operator_note_v3.py` | list/object roots, missing source, duplicate IDs, Decision/action ambiguity and reusable non-authority |
| `catalog/osl/tests/test_import_service.py` | quarantine, unknown major, external lifecycle as metadata and explicit human disposition |
| `catalog/osl/tests/test_compatibility_matrix.py` | every promised reader/writer/profile/codec/schema/export pair |
| `catalog/osl/tests/test_backup_restore.py` | exact source/revision/event/decision/export recovery |
| `catalog/osl/tests/test_export_service.py` | exact input, stable bytes/hash, historical artefact retrieval and no lifecycle effect |
| `catalog/osl/tests/test_sysml_v2_export.py` | mappings, unsupported constructs/collisions, no invented semantics, OS determinism |
| `catalog/osl/tests/test_adversarial_inputs.py` | extension namespace/size/required rules, executable/authority denial and import bounds |

Fixture roots:

- `catalog/osl/tests/fixtures/compatibility/paper_repo_abe3fbcd/`: full source,
  derived excerpt, conflicting statuses and illustrative syntax, all sanitized;
- `.../systems_paper_ff098ce/`: bounded positive/negative profile fixtures and
  source-pinned exports;
- `.../fcp_operator_records_v3/`: list/object/missing/collision/malformed legacy
  fixtures;
- `.../repository_schema_v*/`: database fixtures for each retained schema.

Permanent gates prove codec/hash on Ubuntu/Windows, unknown-major failure,
optional extension preservation, required extension failure, migration replay/
restart, unchanged legacy source, no imported authority, evidence hashes across
conversion, old revision query after rollback, deterministic export, every
schema fixture, malformed bounds and no executable payload.

## Decisions fixed by this plan

- `proposed-for-FCP`: first support is a source-pinned bounded FCP profile, not
  an unqualified stable OSL v0.1 claim.
- `proposed-for-FCP`: profile, source commit, codec, schema, producer, validator,
  exporter and API versions remain distinct.
- `proposed-for-FCP`: conversion creates immutable target plus report/provenance.
- `proposed-for-FCP`: legacy FCP/paper values import as source, candidates,
  metadata and findings only.
- `proposed-for-FCP`: no external status, reusable flag or approval transfers to
  local review/approval/publication.
- `proposed-for-FCP`: no dual-write cutover and no source overwrite.
- `proposed-for-FCP`: canonical JSON first; SysML export-only; YAML/SysML import
  deferred.
- `proposed-for-FCP`: unknown major/required semantics fail closed; safe bytes
  may be quarantined.
- `proposed-for-FCP`: exports are immutable versioned projections; rollback
  selects preserved history instead of erasing it.

## Open research and product decisions

- `requires-research-clarification`: approve public profile/version naming and
  which aspirational semantic-contract parts join the first bounded profile.
- `requires-research-clarification`: same-major compatibility promises and
  minimum decoder/export retention windows.
- `requires-research-clarification`: canonical key/Unicode/numeric/timestamp/
  hash rules.
- `requires-research-clarification`: extension namespace registry/ownership and
  first permitted extensions.
- `requires-research-clarification`: quantitative confidence support.
- `requires-research-clarification`: privacy/legal erasure/consent withdrawal
  while preserving honest provenance.
- `requires-research-clarification`: faithful SysML v2 import criteria and
  toolchain, if ever supported.
- `requires-research-clarification`: branching/merge semantics; first support is
  parent lineage and supersession, not semantic merge.
- `requires-research-clarification`: resolve paper status/readiness conflicts
  before any paper-schema adapter advances beyond fixtures.

## Acceptance criteria for later implementation

1. the first registry profile is source-pinned and explicitly bounded;
2. every persisted object identifies applicable profile/codec/revision/hash and
   producer where relevant;
3. legacy import is dry-runnable, atomic, replay-safe and auditable;
4. original FCP/paper evidence remains byte/hash-identical;
5. ambiguous values become findings, not invented semantics;
6. migration copies no review, approval, publication or authority;
7. unknown profiles/extensions follow the explicit fail/preserve matrix;
8. old revisions/exports remain inspectable after migration and rollback;
9. canonical exports are stable across Linux/Windows;
10. deprecation is non-destructive and identifiers are never reused;
11. permanent CI covers codec, schema, legacy mapping, evidence, extension,
    export, rollback and authority boundaries.
