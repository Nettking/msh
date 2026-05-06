# Data contract

This document describes the data shapes MSH expects and the artifacts it creates. It is descriptive, not a promise that every historical file is clean. For session lifecycle and cache behavior, see [Workflow sessions](workflow_sessions.md).

## Raw telemetry input

Raw source telemetry is expected as JSON Lines (`*.jsonl`) under `data/` or another configured scan/input root. Each line should be a JSON object representing one telemetry sample.

Common fields used by shared code and scripts include:

- `timestamp` — parseable timestamp. Date discovery and filtering prefer this over filenames when present.
- machine identifier — one of `machine_id`, `machine`, or `resource` depending on source/script path.
- sequence number — commonly `sequence` when sequence-gap analyses are run.
- signal/context fields — examples include `Srpm`, `Sload`, `Sovr`, `Fovr`, `Frapidovr`, `execution`, `mode`, and `program`.

If records in a file do not contain parseable timestamps, some date discovery and filtering paths can fall back to dates encoded in filenames. This is a compatibility behavior for historical data, not the preferred contract.

## Normalized telemetry assumptions

Shared telemetry preparation normalizes the data enough for scripts to reuse it:

- timestamps are parsed and invalid timestamps can be dropped for timeline-oriented operations.
- machine IDs are stripped string values, with `unknown` used only where a machine column cannot be found.
- numeric signal fields are coerced to numeric values.
- unavailable textual context values are normalized before timeline/state inference.

Scripts may still impose their own additional requirements. When adding scripts, prefer using `catalog/common/` helpers instead of reimplementing parsing rules.

## Workflow session-filtered data

A workflow session stores its filtered raw JSONL copy under:

```text
results/workflows/<session-id>/data/
```

Filtering is based on `filter.start_date`, `filter.end_date`, and optional same-day `start_hour`/`end_hour` in `session_state.json`. The session config signature is derived from that filter payload and is used to decide whether filtered data and playback exports can be reused.

## Derived metrics artifact

Bootstrap/catch-up orchestration creates a compact shared metrics artifact under the workflow session data directory:

```text
results/workflows/<session-id>/data/_derived/basic_metrics.csv
```

It contains the compact columns needed by automatic health scripts: timestamp, machine, and sequence. This avoids repeated full JSONL parsing during bootstrap/catch-up.

## Playback-ready contract

A workflow session satisfies the playback-ready contract when:

1. `filter_result.matched_records` is greater than zero.
2. the workflow session filtered data directory exists.
3. `exports/timeline/timeline_rows.csv` exists or can be generated from the filtered data.
4. `exports/timeline/manifest.json` matches the workflow session filter signature and filtered-data generation timestamp when reusing cache.

The playback source schema requires at least:

- `timestamp`
- `machine_id`
- `state`

Timeline exports may include additional columns such as `date`, boolean state flags, `event_score`, `fired_rules`, signal columns, and context columns. Consumers should tolerate extra columns.

## Raw vs normalized validation

Health scripts often validate raw telemetry availability and sequence behavior. Playback validates normalized timeline fields. These checks answer different questions: a raw file can exist and still fail playback if timestamps, machine identifiers, or states cannot be normalized into the playback schema.

## Limitations

- Cache reuse is based on workflow session metadata signatures, timestamps, and output existence; it is not a full semantic validation of changed source data or changed script code.
- JSONL records with inconsistent field names may require script-specific handling.
- Filename-date fallback can include records without timestamps when the whole file has no parseable timestamp and the filename date is in scope.
