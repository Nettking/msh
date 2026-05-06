# Workflow sessions

Workflow sessions are the unit of reproducibility for filtered data, script runs, and playback exports.

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

## Cache reuse and invalidation

MSH reuses session data and script outputs when metadata says they match the requested scope and the expected output folders still exist. It invalidates stale script statuses when a script is marked `done` but its output path is missing.

Playback exports have their own manifest. They are reused only when the manifest session config signature and filtered-data generation timestamp match current session metadata.

This cache model is intentionally lightweight. It does not prove that script code has not changed or that external source files are semantically identical.

## Bootstrap sessions

At startup, orchestration discovers source dates and creates/reuses a deterministic automatic session for the latest discovered day. The runtime namespace is part of the generated session ID so multiple logical runtimes can avoid colliding.

Bootstrap prioritizes fast operator visibility:

- Flask starts first.
- latest-day filtered data is prepared.
- automatic playback-ready scripts run best-effort.
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

Manual/deep scripts are not run by default during bootstrap but are tracked in session metadata when executed.

## Script categories

- **Automatic script** — part of the bounded runtime playback-ready contract and safe to run during bootstrap/catch-up.
- **Manual/deep script** — available on demand, often heavier or exploratory.
- **Legacy script** — retained for historical or compatibility value but not recommended as the main workflow path.
