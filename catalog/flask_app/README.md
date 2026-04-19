# MSH Flask Web App

Primary web interface for browsing scanned artifacts, playback-capable datasets, and generic data exploration.

## Routes

- `/` overview
- `/analyses` analysis browser
- `/machine` machine/day trends
- `/playback` playback-compatible inspection
- `/exploration` generic table exploration with charts
- `/status` scan/system status
- `POST /rescan` explicit rescan trigger

## Runtime (prepare + serve)

```bash
MSH_SCAN_DIRS=results,data python -m catalog.flask_app.app
```

Startup now performs automatic orchestration before Flask serve:

- scan configured roots
- discover source dates in `data/` and bootstrap only the latest day
- create/reuse per-day auto workflow session
- run startup-safe analysis preparation for that bootstrap slice
- prepare playback exports
- start Flask quickly
- continue polling for newly available data and process only new slices incrementally

Open http://localhost:5000.

Implementation note: shared registry/data logic is centralized in `catalog/common/artifact_registry.py`.

## Artifact visibility defaults

The scanner classifies each indexed file into one of three user-facing categories:

- `source_data`: primary source files under `data/`
- `derived_output`: analysis outputs meant for inspection (summaries, plots tables, playback exports, etc.)
- `internal_metadata`: runtime/session state files (for example `runtime_state.json` and `session_state.json`)

Additionally, copied workflow raw-data files under workflow-internal `.../data/...` folders are tagged as `workflow_data_copy` and hidden from the default overview to reduce duplicate noise versus the original source file.

Overview (`/`) shows only default-visible artifacts (`source_data` + `derived_output`), while runtime/session metadata remains visible on `/status`.


## Startup coupling note

By default, Flask startup runs a minimal bootstrap orchestration first. This is intentional and keeps startup bounded to the latest day. Incremental updates continue in the background and runtime state is persisted in `results/workflows/runtime_state.json` for status visibility. Set `MSH_SKIP_ORCHESTRATION=1` to skip that pre-start phase when needed.
