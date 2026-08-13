# Digital twin from recorder telemetry

This note profiles a real FCP recorder capture and proposes how to use that
class of data as the measurement side of a digital twin in this repository. It
describes what the data supports today, what it does not support, and the
smallest useful implementation sequence.

Reproduce the profile on any raw batch directory with:

```bash
python scripts/profile_mtconnect_raw_batches.py <directory>
python scripts/profile_mtconnect_raw_batches.py <directory> --json
```

## The reference capture

The capture examined here is one recorder day directory: 1315 gzipped
`MTConnectStreams` responses with their `.manifest.json` sidecars, about 11 MB
compressed.

| Property | Value |
| --- | --- |
| Source | `M8015RW221N` (Mazak, MTConnect 1.3, agent `MFMS0-MC1`) |
| Agent instance | `1786093233`, single instance across the whole capture |
| Observations | 79 758 |
| Sequence coverage | 1 – 79 758, **zero gaps** across 1315 batches |
| Wall-clock span | 2026-08-07 09:00:33Z – 13:00:09Z (4 h) |
| Integrity | 1315/1315 manifests match `raw_sha256` |
| `UNAVAILABLE` values | 118 (0.15 %), clustered at start and shutdown |
| Distinct data items | 75 |

Two properties matter more than the volume. Sequence continuity is complete, so
the batch stream is a lossless replay of what the agent published — the
loss-aware recorder contract held. And `raw_sha256` verifies for every batch,
so the capture is usable as a fixture that the existing Federation verification
path will accept unchanged.

### What is actually measured

Observation counts by component:

| Component | Observations | Useful signals |
| --- | --- | --- |
| `Linear/X`, `Linear/Y`, `Linear/Z` | 69 632 | `Load`, `AxisFeedrate`, `AxisState` |
| `Path/path` | 5 834 | `PathFeedrate`, `Line`, `Program`, `ProgramComment`, feed override |
| `Rotary/C` | 2 491 | spindle `Load`, `RotaryVelocity`, `Temperature` |
| `Controller/controller` | 1 489 | `Execution`, `ControllerMode`, `AccumulatedTime`, conditions |
| `Rotary/B` | 279 | `AngularVelocity`, `AxisState` |
| `Device`, `Coolant`, `Door`, `Electric`, `Hydraulic`, `Lubrication`, `Pneumatic` | 26 | availability, e-stop, door state, coolant concentration |

Continuous channels arrive at a **median 0.46 s cadence** (p95 ≈ 2 s), with
occasional multi-minute holds when the machine is idle — MTConnect only
publishes on change. Spindle load (`sl`) is slower at ~2 s, spindle temperature
(`ctemp`) ~4 s.

Load distributions over the capture:

| Channel | Median | p95 | Max |
| --- | --- | --- | --- |
| `xl` X load | 3 | 7 | 217 |
| `yl` Y load | 39 | 46 | 204 |
| `zl` Z load | 3 | 27 | 188 |
| `sl` spindle load | 1 | 49 | 120 |

The Y axis sitting at a sustained ~39 while X and Z sit near 3 is exactly the
kind of standing asymmetry a twin should baseline rather than alarm on.

### The state timeline

`Execution` transitions 31 times in four hours, with this dwell:

| State | Seconds | Share |
| --- | --- | --- |
| `ACTIVE` | 7 507 | 52 % |
| `READY` | 5 779 | 40 % |
| `PROGRAM_STOPPED` | 1 048 | 7 % |
| `STOPPED` / `FEED_HOLD` / `UNAVAILABLE` | 42 | < 1 % |

Alongside it: `ControllerMode` moves between `AUTOMATIC`, `MANUAL_DATA_INPUT`
and `MANUAL`; `pgm` names main programs (`36`, then `4`); `spgm` records 99
subprogram entries; `ln` gives 197 block-line transitions; `tid` gives 29 tool
changes; `pltnum` shows pallet 4 → 2 → 4 → 2. Condition channels carry
Norwegian-language controller text (`VEDLIKEHOLDSJEKK ADVARSEL`,
`FRESEBLOKKSTART SPERRET`, `LADE ST.DØR ÅPEN`, `ENERGI SPARE MODUS`).

That combination — program, subprogram, line number, tool, pallet, execution
state, and per-axis load/feed at sub-second cadence — is enough to segment the
capture into **tool-and-operation-level cutting segments**. That segmentation is
the unit a twin should model against.

## What this data does and does not support

Supports:

- A **behavioural twin**: expected load/feed/duration envelopes per
  `(program, subprogram, line range, tool)` segment, learned from history and
  compared against a live run.
- **State and cycle-time modelling**: utilisation, dwell per execution state,
  cycle and segment durations, pallet-change and tool-change overhead.
- **Deviation detection**: current run against the per-segment baseline
  (drift, load creep, feed-override deviation, a segment running long).
- **Deterministic replay** for regression tests and demos.

Does not support, and should not be claimed:

- No `PathPosition` or per-axis absolute position in this capture (`Position`
  appears 12 times only). There is **no geometric or kinematic twin** here —
  no toolpath reconstruction, no collision or material-removal simulation.
- No power, no vibration, no part measurements, no tool-wear ground truth, no
  quality outcome. Anything about tool wear is inference from load, not
  measurement.
- Four hours from one machine on one day. Enough to build and test the
  pipeline; not enough to fit a model anyone should trust operationally.

## How it fits the existing repository

The pieces already exist and are not currently joined into a twin:

- `catalog/mtconnect_recorder/` — fetches, parses (`parsing.py`), and stores
  raw batches plus manifests (`storage.py`), which is exactly the shape of this
  capture.
- `catalog/federation/recorder_publication.py`, `telemetry_mirror.py` —
  checkpoint-committed publication and hash-verified materialization into a
  member's local workbench.
- `catalog/flask_app/services/live_service.py` and the Monitor surface —
  current values and playback.
- `docs/fcp_operator_support_plan.md` — operator strategies are already
  intended to feed a "Digital Twin / recommender", with a `DT/SysML trace
  target` field on each structured note.
- `docs/implementation/osl_integration/` — the planned OSL/SysML modelling
  layer, not yet implemented.

The gap is the middle: nothing turns verified raw batches into a **segmented,
queryable operational history** that a model or a recommender can be fitted to
and evaluated against. That is the piece worth building, and it is the piece
this data is well suited to.

## Proposed use, in order

### 1. Land the capture as a versioned reference dataset

Store one recorder day like this one as the canonical fixture for telemetry
work, addressed by `raw_sha256` so it stays verifiable. Use it for recorder
regression tests, Federation materialization tests, Monitor/playback demos, and
as the input every later stage is measured on.

**Blocker, now fixed:** these manifests declare the pre-rename schema name,
while `storage.py` accepted only the current `fcp.` name and silently skipped
anything else, making any pre-rename capture invisible to `iter_raw_batches`.
Both generations are now read through one explicit enumeration, and an
unsupported schema is reported distinctly from corrupt data rather than being
skipped in silence. See
[Canonical MTConnect observations](canonical_mtconnect_observations.md).

### 2. Derive a canonical observation table

**Delivered** — see
[Canonical MTConnect observations](canonical_mtconnect_observations.md) for the
schema, the identity invariant, and the projection's guarantees.

One replayable projection from raw batches, keyed by
`(source_key, agent_instance_id, sequence)`. Note the correction to the proposal
above: `(agent_instance_id, sequence)` alone is **not** sufficient. `instanceId`
is conventionally the Agent's start time in seconds, so two machines whose
Agents started in the same second share it. The recorder's own source partition
closes that gap.

This replaces ad-hoc XML parsing at every call site and gives the flat form the
existing `example-data/*.jsonl` wide format cannot represent well (that format
loses per-channel timestamps by flattening to a row per sequence).

### 3. Segment into operations

From the state channels, cut the observation stream into segments at
transitions of `Execution`, `Program`/`ProgramComment`, `ToolNumber`, and
`PalletId`, and attach per-segment aggregates over each continuous channel
(duration, mean/p95/max load per axis, feed distribution, override use). The
capture yields on the order of tens of segments in four hours — small enough to
inspect by hand while the logic is being validated.

This is the twin's state vector. Everything downstream reads segments, not raw
XML.

### 4. Baseline and compare

Build per-`(program, tool)` envelopes from historical segments, then score a
live or replayed run against them. Start with a plain statistical baseline —
median and p95 per channel, expected duration — not a learned model. With four
hours of data a threshold model is honest and a fitted model is not. Surface
the comparison on the existing Monitor surface as a deviation panel.

### 5. Connect to operator knowledge

Each structured operator note already carries context/trigger/expected-outcome
fields and a `DT/SysML trace target`. Bind that target to a segment selector
(program, tool, execution state) so a captured strategy such as *"thermal drift
during the first 30 minutes"* becomes a testable predicate over the segment
history — with `ctemp`, elapsed-since-start, and per-segment load available to
evaluate it. This closes the capture → structure → validate loop in
`docs/fcp_operator_support_plan.md` using measured data rather than assertion.

### 6. Only then, OSL/SysML export

The OSL package (`docs/implementation/osl_integration/`) describes the modelling
and export layer. It should consume segments and validated strategies from
steps 3–5. Exporting before there is a segment model would export structure
with nothing measured behind it.

## Recommended next step

Steps 1 and 2 are self-contained, testable against this exact capture, and
unblock everything after them. Step 1 additionally fixes a real data-visibility
defect. Suggested first delivery: accept both manifest schema prefixes on read,
land the capture as a reference fixture, and add the observation projection with
a test that rebuilds it from the fixture and asserts 79 758 rows over sequences
1–79 758 with no gaps.

## Data handling note

This capture identifies a specific machine (`M8015RW221N`), its programs, tool
numbers, and Norwegian-language controller messages, and it reveals four hours
of production activity. Before it is committed to the repository or published
through Federation storage as a shared fixture, confirm with the machine owner
that this is acceptable, or anonymise source name and program identifiers.
