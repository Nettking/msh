# MSH Flask Web App

Primary web interface for browsing scanned artifacts, playback-capable datasets, and generic data exploration.

## Routes

- `/` overview
- `/analyses` analysis browser
- `/machine` machine/day trends
- `/playback` playback-compatible inspection
- `/exploration` generic table exploration with charts
- `/status` scan/system status
- `/control` runtime + workflow + script control panel
- `POST /rescan` explicit rescan trigger
- `POST /control/action` trigger runtime/workflow actions
- `POST /control/script/<script_key>/run` trigger individual scripts

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

## Operator control panel (terminal controls moved to web)

`/control` is now the primary operator surface for analysis control. It replaces the old terminal/menu control flow with explicit web actions:

- runtime state visibility (mode, processed range, update-running, last refresh/failure)
- active/latest workflow session metadata visibility
- explicit workflow actions:
  - Run refresh now
  - Run startup-safe health checks
  - Re-run latest session workflow scripts (reruns scripts in latest existing session)
- script-level manual run buttons with status/last-run/output path
- recent control activity history (action, timing, status, message, output path)

Automatic bootstrap + incremental background updates still run at Flask startup; the control panel adds manual operator overrides without reintroducing terminal prompts.

## Current MVP limits

The control panel is intentionally lightweight:

- execution model is single-process + in-process background threads
- actions are best-effort and are not a durable job queue
- recent control activity is held in memory only (not persisted across process restart)
- recent stdout/stderr snippets are truncated, practical diagnostics rather than full logs
- manual script/workflow actions may refresh filtered session data and playback exports as part of a run

This is an initial web operator surface and not yet a full distributed control system.
