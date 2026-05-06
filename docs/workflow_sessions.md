# Workflow sessions

Workflow sessions are the unit of reproducibility for filtered data, script runs, and playback exports. For operator terminology, see the [Operator guide](operator_guide.md#core-concepts).

## Session layout

A typical session lives at:

```text
results/workflows/<session-id>/
├── data/                  # filtered JSONL copy for the session scope
│   └── _derived/           # shared derived metrics such as basic_metrics.csv
├── exports/timeline/       # playback exports and manifest
├── runs/<script>/<time>/   # per-script isolated run workspaces and outputs
├── session_state.json      # canonical session metadata
└── session.json            # legacy metadata mirror
```

## Session metadata

`session_state.json` records:

- session ID, creation/update timestamps, and metadata version.
- filter scope: start/end date and optional same-day hour range.
- session config signature.
- relative paths for filtered data, runs, and playback exports.
- filter results: matched records/files and generated timestamp.
- runtime namespace.
- per-script status, output path, duration, exit code, and last-run timestamp.

The legacy `session.json` mirror exists for backward compatibility.

## Source data index

The runner maintains `results/runner/data_index.json` as a lightweight metadata index for source JSONL files. Each entry records file identity metadata such as path, size, mtime, filename-derived date, timestamp bounds, record count, and machine IDs when those values are available while parsing.

Date discovery and session filtering refresh this index incrementally: unchanged files reuse cached metadata, changed or new files are reparsed, and deleted files are removed from the index. During filtering, the runner uses timestamp bounds to prune files that cannot overlap the requested date range before opening/parsing JSONL. If timestamp bounds are unavailable, filename dates may be used as a fallback; files with unknown metadata are still opened conservatively so historical data is not silently dropped. JSONL files remain the canonical input and source of truth.

## Cache reuse and invalidation

MSH reuses session data and script outputs when metadata says they match the requested scope and the expected output folders still exist. It invalidates stale script statuses when a script is marked `done` but its output path is missing.

Playback exports have their own manifest. They are reused only when the manifest session config signature and filtered-data generation timestamp match current session metadata.

This cache model is intentionally lightweight. It does not prove that script code has not changed or that external source files are semantically identical.

## Bootstrap sessions

At startup, orchestration discovers source dates and creates/reuses a deterministic automatic session for the latest discovered day. The runtime namespace is part of the generated session ID so multiple logical runtimes can avoid colliding.

Bootstrap prioritizes fast operator visibility:

- Flask starts first.
- latest-day filtered data is prepared.
- automatic scripts run best-effort to satisfy the playback-ready contract.
- playback exports are created or reused.
- failures are recorded while Flask remains available.

## Catch-up behavior

After bootstrap, historical catch-up processes available source days incrementally, one day per cycle. Current policy is reverse chronological catch-up after the latest-day bootstrap. When catch-up is complete, the runtime polls for newly arriving source days and processes new slices rather than recomputing all history.

## Manual sessions and runs

From `/control`, operators can:

- select an existing session.
- use latest-day or selected-day scope.
- provide a custom date range.
- create/reuse a full historical range session.
- run startup-safe checks, the workflow order, or one script.

Manual scripts, including deep/exploratory scripts, are not run during bootstrap/catch-up but are tracked in session metadata when executed.

## Script categories

- **Automatic script** — startup-safe and included in bootstrap/catch-up for the bounded playback-ready contract.
- **Manual script** — available on demand from `/control`, excluded from bootstrap/catch-up.
- **Deep/exploratory script** — a manual script that may be slower, research-oriented, or less operationally bounded.
- **Legacy script** — retained for historical or compatibility value but not recommended as the main workflow path.

The canonical script list and stage order live in [catalog/README.md](../catalog/README.md#runner-visible-scripts).
