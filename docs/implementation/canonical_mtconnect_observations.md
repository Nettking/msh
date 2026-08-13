# Canonical MTConnect observations

This is the foundational recorder → telemetry layer a behavioural digital twin
will later be built on. It stops deliberately at a queryable observation set:

```text
MTConnect raw recorder batches
        ↓
verified compatible batch reader
        ↓
canonical observations
        ↓
durable/queryable/rebuildable observation projection
```

Nothing above that boundary exists yet. There is no operation segmentation, no
cycle detection, no baseline, no anomaly threshold, no episode model and no
export. The observation model is meant to be inspected before those semantics
are designed against it.

## Raw capture is the source of truth

The recorder's compressed `MTConnectStreams` responses and their
`.manifest.json` sidecars are the immutable evidence layer. The canonical
observation database is a **projection**: derived, disposable, and rebuildable.

| | Raw capture | Canonical projection |
| --- | --- | --- |
| Authority | source of truth | derived cache |
| Mutability | never modified | delete and rebuild freely |
| Integrity | `raw_sha256` over the uncompressed XML | traceable to that digest |
| Loss on deletion | unrecoverable | none |

Rebuilding never requires editing a capture. `rebuild_from_recorder_archive`
opens the archive read-only, and a regression test asserts the manifest and
payload bytes are unchanged after repeated projection runs.

## Manifest compatibility across the product rename

Captures recorded before the product rename declare the retired product prefix
in their manifest `schema`. The reader previously accepted only the current
`fcp.` name and `continue`d past anything else, so a pre-rename capture was
invisible to `iter_raw_batches` — real data silently skipped.

`catalog/mtconnect_recorder/schema_compat.py` is now the single place that
states what a reader may accept. The policy is:

1. **Current is canonical.** Every new write uses the `fcp.` name. Nothing in
   the compatibility layer causes a legacy name to be written.
2. **Legacy is readable, never rewritten.** A historical capture stays
   byte-for-byte as recorded. Readability is a property of the reader, not a
   reason to migrate immutable evidence.
3. **Schemas are enumerated, never prefix-matched.** "Anything starting with the
   old product name" would accept schemas that never existed and would hide a
   genuinely unknown future version behind a successful read. Each supported
   name is listed in full.
4. **Both generations get identical validation.** Only the schema string
   differs; digest verification, required fields and structural checks are the
   same. A legacy manifest is not trusted more because it is old.
5. **Unsupported is not corruption.** A manifest that parses but declares an
   unknown schema is reported as `unsupported-manifest-schema`. One that cannot
   be read, is not JSON, is not an object, or is missing a required field is
   reported as `malformed-manifest`. A supported manifest whose payload is
   absent is `raw-payload-missing`. The three are never collapsed into "skip".

### Which recorder schemas needed this

The rename predates this repository's git history, so the audit went by what the
recorder writes and what is read back with a schema filter:

| Schema | Written | Read with a schema filter | Action |
| --- | --- | --- | --- |
| `…mtconnect.raw_batch_manifest.v1` | raw batch sidecar | `iter_raw_batches` | **fixed** — the observed defect |
| `…mtconnect_recorder.checkpoints.v3` | checkpoint state | `RecorderRuntime.load_state` **and** `RecorderArchiveReconciler` | **fixed** — see below |
| `…mtconnect.probe_manifest.v1` | probe sidecar | not filtered on read today | enumerated for consistency |
| `fcp.mtconnect.observation.v2` | federation payload | `validate_recorder_payload` | **deliberately unchanged** |
| `fcp.mtconnect.snapshot.v2` | wide JSONL | write-only | none needed |
| `fcp.mtconnect.gap.v1`, `fcp.mtconnect.recorder_event.v1` | event/gap records | write-only | none needed |

Two notes on that table.

The **checkpoint** case was a genuine second defect. `RecorderRuntime.load_state`
already accepts a pre-rename checkpoint and logs that it deliberately leaves the
file untouched until the next successful commit. During that window
`RecorderArchiveReconciler._checkpoints` — reading the *same file* — raised
`unsupported-recorder-state`, so an upgraded installation could record fine and
fail to publish. Both readers now share one enumeration.

`fcp.mtconnect.observation.v2` is **not** loosened. It validates a federation
wire payload, not historical on-disk data. Pre-rename data does not travel over
the current protocol, and relaxing a protocol boundary to accommodate archived
files would weaken a check that has nothing to do with the problem.

### A relocated capture

Manifests record an absolute `raw_file` path from the machine that recorded
them, which resolves to nothing once a capture is copied elsewhere — exactly the
historical-capture case. The existing fallback was broken: manifest names are
derived with `Path.with_suffix('.manifest.json')`, which replaces only the final
`.gz`, so stripping the manifest suffix from `seq-…-<digest>.xml.manifest.json`
yields `seq-…-<digest>.xml` and misses the actual `…xml.gz`. The reader now
offers the compressed sibling first, so a relocated capture stays readable.

## The canonical observation

One row per MTConnect observation, generic across machines. There is no column
per DataItem: the model carries MTConnect *concepts*, and anything without a
dedicated field survives verbatim in `attributes`.

| Field | Meaning |
| --- | --- |
| `source_key`, `agent_instance_id`, `sequence` | durable natural identity (below) |
| `source_name`, `raw_batch_sha256` | capture identity and provenance |
| `observed_at`, `observed_at_us`, `timestamp_source` | canonical UTC text, exact microseconds, and where the time came from |
| `machine_id`, `device_uuid`, `device_name` | device identity |
| `component_type`, `component_id`, `component_name`, `component_path` | component and its path |
| `data_item_id`, `data_item_name` | data item identity |
| `category`, `observation_type`, `mtconnect_type`, `sub_type` | `SAMPLE`/`EVENT`/`CONDITION`, the XML element name, and probe-declared type/sub type |
| `value_kind`, `value_number`, `value_text`, `native_value` | typed value plus the exact original text |
| `units`, `native_units` | units where the probe declares them |
| `condition_state`, `condition_native_code`, `condition_native_severity`, `condition_qualifier` | condition fields where applicable |
| `attributes` | every remaining MTConnect attribute, as sorted JSON |

`value_kind` is one of `number`, `text`, `unavailable` or `empty`. The MTConnect
`UNAVAILABLE` sentinel is its own kind rather than the literal string or a
silent null, and `native_value` always keeps the exact text so nothing is lost
to type inference.

`observed_at` is normalised to one fixed-width UTC spelling so text ordering
cannot disagree with time ordering; `observed_at_us` is exact integer
microseconds so range queries never depend on float comparison.

Probe-derived fields (`data_item_name`, `mtconnect_type`, `units`,
`native_units`) are null for a capture with no archived `/probe`. Nothing that
can be absent participates in identity. `component_path` is derived from the
stream document alone, so it is present either way.

## Observation identity

```text
(source_key, agent_instance_id, sequence)
```

**Why `sequence` is not enough.** MTConnect assigns a distinct sequence number
to every observation an Agent publishes, but the numbering belongs to the
Agent's buffer instance. When an Agent restarts it reinitialises that buffer,
publishes a new `instanceId`, and restarts sequence numbering. Sequence alone
therefore collides across restarts.

**Why `(instanceId, sequence)` is not enough.** `instanceId` is conventionally
the Agent's start time in seconds, so two Agents that start in the same second
share it exactly. That is a plausible collision between two machines, not a
theoretical one.

**Why `source_key` closes it.** `source_key` is `_slug(source_name)` — the same
partition the recorder already writes raw batches under
(`raw/<source_key>/<instance_id>/`). Using the recorder's own partitioning rather
than inventing a notion of machine identity means the projection and the raw
evidence layer agree by construction, and recorder source names are already
validated unique per installation by `_validate_source_storage_names`.

One Agent serving several devices is covered without a device component in the
key: those devices share one sequence space, so their observations already have
distinct sequences. A device column exists for querying, not for identity.

No random identifiers are generated. Every field is a pure function of the raw
batch.

### What happens if the invariant is violated

The empirical evidence for one-observation-per-sequence is strong — the
reference capture held 79 758 observations over sequences 1–79 758 with no gaps
and no repeats — but the recorder's own `validate_batch_continuity` compares a
*set* of sequences and so would not notice a repeat. The projection therefore
checks rather than assumes:

- Two observations in one batch claiming the same sequence reject the batch with
  `duplicate-sequence-in-batch`.
- An identity already stored, arriving with different content, rejects the batch
  with `overlapping-observation-conflict`.

Both are surfaced loudly rather than deduplicated away, because either would
mean the identity model does not describe the Agent that produced the data.
Modelling the violation as normal — by adding the data item to the key — would
hide it instead.

## Storage and projection

SQLite, versioned through the repository's existing
`catalog/federation/sqlite_schema.py` helper. Two tables:

`canonical_observations`
: one row per observation, primary key `(source_key, agent_instance_id,
  sequence)`, indexed for machine/time, data-item/time, global time, and batch
  digest.

`projected_batches`
: the ledger. One row per raw batch considered, keyed `(source_key,
  raw_sha256)`, recording manifest and payload paths, which manifest generation
  was read, both observation counts, and status `projected` or `rejected` with a
  reason.

### Properties, and what makes them true

**Deterministic.** Every field derives from the immutable inputs — the
compressed XML, its manifest, and the archived probe. No wall clock is read
while deriving an observation: a document that omits a timestamp falls back to
the manifest's recorded `received_at` rather than to `now()`. Only the ledger's
`projected_at` is wall-clock, and it is excluded from `content_fingerprint()`.

**Idempotent.** The ledger is keyed by the digest of the uncompressed payload,
so replaying a batch recognises it and does nothing. Overlapping batches are
handled at observation granularity: repeated identities are verified to carry
identical content and then ignored, and provenance stays with the batch that
first carried the observation.

**Rebuildable.** Delete the SQLite file and run again. Nothing about the
reconstruction depends on state that lives only in the projection.

**Incremental.** A batch already recorded as `projected` is skipped on its
digest alone — never decompressed, never parsed. A rejected batch *is* retried,
so restoring a truncated payload from backup is enough to bring it in.

**Ordered.** `observations_for_agent_instance` orders by sequence, which within
one instance is the Agent's own publication order. Cross-instance queries order
by `(observed_at_us, source_key, agent_instance_id, sequence)` — a total order,
not merely a stable one.

**Auditable.** Every observation row carries the `raw_sha256` of its batch;
`batch_for_digest` maps that to the manifest and payload on disk.

**Failure safe.** Each batch is validated and committed in one transaction. A
rejected batch leaves no rows and is recorded with a reason; every other batch
still projects.

### Query surface

Deliberately narrow — slices over observations, not analytics:

```python
store.observations_for_machine("MACHINE-A", start=t1, end=t2)
store.observations_for_data_item("xl", start=t1, end=t2)
store.observations_for_agent_instance(source_key="MACHINE-A", agent_instance_id=1786093233)
store.query_observations(source_key="MACHINE-A", category="CONDITION")
store.latest_observation("exec", source_key="MACHINE-A")
```

Time windows are inclusive of `start` and exclusive of `end`, so adjacent
windows tile without double-counting a boundary observation. Nothing is
aggregated away.

## Rebuilding

```python
from catalog.mtconnect_recorder.canonical import rebuild_from_recorder_archive

rebuild_from_recorder_archive(
    data_dir="data",
    database_path="data/sources/mtconnect_recorder/canonical/observations.sqlite3",
)
```

## Integration seam

The projection is a plain synchronous component with one entry point per Agent
instance. It owns **no scheduler, no worker pool, no transport and no job
lifecycle** — those already exist in this repository, and this layer
deliberately does not add a second one.

`CanonicalObservationProjection.project_instance` is shaped to be driven by the
existing durable-job machinery later: it is deterministic, resumable, safe to
re-run, and returns a structured `ProjectionReport`. A future federation-
dispatched contract can call it without this module knowing anything about
providers, artifact authorisation or F6 transfer. Canonicalising local recorder
data does not require remote execution, so this layer does not depend on it.

## Limitations

- **The reference capture was not committed.** It identifies a specific machine,
  its programs and tool numbers, and four hours of production activity. Fixtures
  are anonymised documents of the same shape; no production value appears in the
  repository.
- **No probe means fewer columns.** A capture without an archived `/probe` loses
  `units`, `native_units`, `mtconnect_type` and probe-declared `data_item_name`.
  The projection still works and identity is unaffected.
- **`source_key` follows the recorder's partitioning.** If a source is renamed —
  which `reconcile_checkpoint_aliases` does when discovery replaces a manual
  alias with an MTConnect UUID — historical raw batches stay under the old slug
  and project under the old key. Nothing collides and nothing is lost, but the
  same physical machine can appear under two source keys. Correlating them is a
  device-identity question, deliberately left to the layer that will define
  machine identity properly.
- **Sub-microsecond timestamps are truncated** to microseconds by
  `datetime.fromisoformat`. `native_value` and the raw XML retain full text.
- **Multi-value samples** such as a three-space `PathPosition` classify as
  `text`. Representing vector samples typed is a later decision, not a silent
  one: `native_value` keeps the exact payload.
