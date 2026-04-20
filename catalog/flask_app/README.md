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
- `POST /control/action` trigger runtime/workflow actions with explicit target session scope
- `POST /control/script/<script_key>/run` trigger individual scripts against selected/created session scope

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
- continue background historical catch-up one day at a time (reverse chronological) until all discovered dates are covered
- after catch-up completes, keep polling for newly arriving dates and process one new day per cycle

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

By default, Flask startup runs a minimal bootstrap orchestration first. This is intentional and keeps startup bounded to the latest day. Incremental updates continue in the background and runtime state is persisted in `results/workflows/runtime_state.json` for status visibility.

Runtime phases are explicit:

- **Bootstrap phase**: process only the latest discovered source day first.
- **Historical catch-up phase**: process exactly one pending day per poll cycle, in reverse chronological order.
- **Steady incremental phase**: once historical catch-up is complete, continue polling for newly arriving days and process them one day per cycle.

Bootstrap behavior is explicitly **refresh-latest-on-startup**: even when latest day was previously processed, startup still re-runs that day to keep newest data fresh while remaining bounded to one day.

State tracks discovered source range, processed day set, pending count, next queued day, last catch-up success timestamp, and completion status so progress survives restart and is visible in `/status` and `/control`.

`processed_dates` reflects **verified processed outputs**, not just attempted runs.

Verification uses the bounded automatic coverage contract (`startup_safe_automatic_outputs`): only the startup-safe scripts used by automatic incremental catch-up are required for a day to count as covered. This avoids requiring manual/heavier workflow outputs for automatic progress.

Runtime reconciliation checks session metadata and required automatic-coverage outputs on disk; if a previously tracked day is missing/corrupt/incomplete for that automatic contract, it is automatically re-queued in historical catch-up.

Set `MSH_SKIP_ORCHESTRATION=1` to skip that pre-start phase when needed.

## Operator control panel (terminal controls moved to web)

`/control` is now the primary operator surface for analysis control. It replaces the old terminal/menu control flow with explicit web actions:

- runtime state visibility (bootstrap vs catch-up phase, discovered source range, processed/pending progress, next queued day, catch-up status, last refresh/failure)
- active/latest workflow session metadata visibility
- explicit workflow actions:
  - Run refresh now
  - Rerun latest bootstrap session workflow
  - Run startup-safe health checks for selected session
  - Run workflow for selected session
  - Create/reuse session for latest day, selected day, custom range, or full-range rebuild and run workflow
- session inventory table (session id, processed date range, updated timestamp, workflow status summary) with selectable target
- script-level manual run buttons that target the selected session/scope
- recent control activity history (action, target session id, target range, status, message, output path, stdout/stderr snippets)

Automatic bootstrap + incremental background updates still run at Flask startup; the control panel adds manual operator overrides without reintroducing terminal prompts.

Manual historical processing now works directly in `/control`: choose a session from the inventory or choose a preset scope (latest day, selected day, custom range, full range), submit, and the app will create/reuse `results/workflows/<session>/`, prepare filtered data for that scope if needed, then run workflow/scripts against that selected session.

Action validation/targeting notes:

- Selected-session actions now require an explicit existing session id (no silent fallback to latest).
- Scope actions validate date inputs (`YYYY-MM-DD`, required fields, and end date not before start date).
- After submitting an action, `/control` redirects to the actual resolved/created target session so the UI reflects what the run is operating on.
- Full-range actions are explicitly marked as manual and potentially slow.

## Current MVP limits

The control panel is intentionally lightweight:

- execution model is single-process + in-process background threads
- actions are best-effort and are not a durable job queue
- recent control activity is held in memory only (not persisted across process restart)
- recent stdout/stderr snippets are truncated, practical diagnostics rather than full logs
- manual script/workflow actions may refresh filtered session data and playback exports as part of a run

This is an initial web operator surface and not yet a full distributed control system.
