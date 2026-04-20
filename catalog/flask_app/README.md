# MSH Flask Web App

Primary Flask operator interface for runtime visibility, dashboard-style overview, playback-capable datasets, and generic data exploration.

## Routes

- `/` operator overview dashboard (current activity + runtime progress + readiness + next actions)
- `/analyses` analysis browser
- `/machine` machine/day trends
- `/playback` playback-compatible inspection
- `/exploration` generic table exploration with charts
- `/status` scan/system status
- `/control` runtime + workflow + script control panel
- `POST /rescan` explicit rescan trigger
- `POST /control/action` trigger runtime/workflow actions with explicit target session scope
- `POST /control/script/<script_key>/run` trigger individual scripts against selected/created session scope


## Overview dashboard model (`/`)

The overview page is intentionally an **operator-facing system dashboard** rather than a file browser.

It now renders a compact `overview snapshot` assembled at request time by `catalog/flask_app/services/overview_service.py` with five sections:

1. headline summary (runtime phase, processed progress, catch-up completion, queue/failure)
   - includes inventory counters in the snapshot contract (`visible/source_artifacts/derived_artifacts/playback_compatible_count/read_error_count + hidden-by-default counts`) rendered from `overview.headline.*`
2. what is happening now (latest known timestamp + machine last-seen/freshness when derivable)
3. runtime progress summary (phase/date/progress/next/last step/failure)
4. view readiness (`/machine`, `/playback`, `/analyses`, plus startup-safe `/status` and `/control`)
5. next actions / open views (navigation with readiness hints)

The snapshot intentionally reuses existing runtime manager state (`state_snapshot`) and existing artifact scans.
Machine/activity signals on `/` are metadata-driven to keep the landing page cheap and predictable under frequent refresh.
The overview intentionally avoids dataframe loading for “what is happening now”; detailed per-machine last-seen data remains available in dedicated downstream views.
Readiness and activity now prefer the runtime/current session context; when only historical artifacts exist, the dashboard labels that explicitly as historical fallback instead of presenting it as current-session readiness.
When runtime session id is unavailable, fallback session selection is deterministic (sorted by session metadata freshness signals such as `updated_at`/`created_at`, then session id) and the UI messages explicitly call out that fallback provenance.

This keeps `/` useful before discovery/session outputs exist, during catch-up, and after data is fully processed, without duplicating the full debug detail shown on `/status`.

## Runtime (prepare + serve)

```bash
MSH_SCAN_DIRS=results,data python -m catalog.flask_app.app
```

Startup now performs **webapp-first runtime**:

- Flask starts immediately
- runtime manager starts in background (non-blocking)
- background discovery scans source dates in `data/`
- bootstrap latest day runs in background
- historical catch-up continues one day per cycle (reverse chronological)
- after catch-up completes, runtime keeps polling for new days and processes them incrementally

Open http://localhost:5000.

Implementation note: shared registry/data logic is centralized in `catalog/common/artifact_registry.py`.

## Artifact visibility defaults

The scanner classifies each indexed file into one of three user-facing categories:

- `source_data`: primary source files under `data/`
- `derived_output`: analysis outputs meant for inspection (summaries, plots tables, playback exports, etc.)
- `internal_metadata`: runtime/session state files (for example `runtime_state.json` and `session_state.json`)

Additionally, copied workflow raw-data files under workflow-internal `.../data/...` folders are tagged as `workflow_data_copy` and hidden from the default overview to reduce duplicate noise versus the original source file.

Overview (`/`) shows only default-visible artifacts (`source_data` + `derived_output`), while runtime/session metadata remains visible on `/status`.


## Availability vs readiness model

Flask availability and processed-data readiness are intentionally separate:

- webapp availability: app process up + pages render
- runtime readiness: background orchestrator status/progress
- view readiness: artifact-specific contracts (for example `data_pr_day` for `/machine`)

Runtime state now explicitly tracks:

- `app_started_at`, `runtime_started_at`
- `discovery_complete`, `bootstrap_complete`, `historical_catch_up_complete`
- `current_processing_phase`, `currently_processing_date`
- `last_completed_step`, `last_completed_date`, `next_queued_date`
- `fully_processed_days_count`, `total_available_days`

Runtime phases are explicit:

- **Bootstrap phase**: process only the latest discovered source day first.
- **Historical catch-up phase**: process exactly one pending day per poll cycle, in reverse chronological order.
- **Steady incremental phase**: once historical catch-up is complete, continue polling for newly arriving days and process them one day per cycle.

Bootstrap behavior is explicitly **refresh-latest-on-startup**: even when latest day was previously processed, startup still re-runs that day to keep newest data fresh while remaining bounded to one day.

State tracks discovered source range, processed day set, pending count, next queued day, last catch-up success timestamp, and completion status so progress survives restart and is visible in `/status` and `/control`.

`processed_dates` reflects **verified processed outputs**, not just attempted runs.

Verification uses the bounded automatic coverage contract (`startup_safe_automatic_outputs`): only the startup-safe scripts used by automatic incremental catch-up are required for a day to count as covered. This avoids requiring manual/heavier workflow outputs for automatic progress.

`data_pr_day` is **not** part of that automatic coverage contract. The `/machine` page is tied specifically to `analyses/data_pr_day/machine_day_summary.csv`, so `/machine` now treats machine/day-readiness separately from session existence:

- all workflow sessions remain selectable in the session picker
- selecting a non-ready session reports an explicit readiness reason inline (not generated, invalid CSV/schema, or no usable rows)
- ready sessions render the machine/day chart from the canonical `data_pr_day` CSV path

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

Automatic bootstrap + incremental background updates run after Flask startup in background threads; the control panel adds manual operator overrides without reintroducing terminal prompts.

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
