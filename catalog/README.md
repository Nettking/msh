# Python Script Catalog

This folder contains analysis and ingestion scripts for MTConnect telemetry.
The goal of this index is to provide a **practical first-pass workflow** from raw data to interpretable findings, while keeping existing scripts separate.

## Standard analysis workflow (recommended)

Use this sequence for most investigations. Move to the next stage when the current stage no longer answers your question.

### 1) Data health checks (run first)
Purpose: verify that the dataset is usable before interpretation.

Run:
- `machines_active_per_day`
- `sampling_rate_analysis`

Typical outputs:
- daily CSV summaries (`*.csv`)
- quick PNG trends (`*.png`)
- missing-data and sampling-quality signals

Move to stage 2 when:
- active-day coverage looks plausible
- missing-sequence patterns are understood well enough to continue
- sampling rate is acceptable (or at least known)

### 2) Raw inspection
Purpose: look directly at signal behavior per machine/day before applying stop logic.

Run:
- `data_pr_day`

Typical outputs:
- per-machine/day raw signal plots under `graphs/<machine>/<YYYY-MM-DD>/`

Move to stage 3 when:
- you need stop-focused timelines rather than raw traces
- raw plots suggest likely stop windows worth targeted inspection

### 3) Stop-focused inspection
Purpose: inspect heuristic stop intervals in a timeline format.

Run:
- `find_stops`

Typical outputs:
- hour-bucketed stop timeline plots under `plots/<YYYY-MM-DD>/<machine>/<HH>.png`

Move to stage 4 when:
- you need broader exploratory interpretation
- you need candidate events, richer diagnostics, or ML baselines

### 4) Deeper exploratory analysis
Purpose: explore hypotheses beyond first-pass checks.

Run as needed:
- `data_visualizer` (state timelines + candidate rows)
- `data_analysis` (terminal-heavy exploratory diagnostics)
- `ml_analysis` (per-machine predictive baseline)
- `data_simulator` (interactive Streamlit exploration)
- `webapp` (timeline/state playback view for processed exports)

Typical outputs:
- timeline images / candidate CSVs
- richer console reports
- ML artifacts under `ml_results/`

## Legacy and non-standard workflow tools

- `corrolation_machine_pairs`: **legacy** pairwise stop-correlation heatmap exploration (kept for compatibility/reference).
- `interventions`: environment-specific script (hardcoded Windows/WSL assumptions).
- `standalone_recorder`: legacy recorder retained for compatibility.
- `auto_connect`: desktop automation helper (not an analysis tool).

## Recorder / ingestion tools

These are ingestion tools, not one-shot analysis scripts:

- **Preferred recorder:** `standalone-recorder_v2`
- **Legacy recorder:** `standalone_recorder`

## Orchestration behavior (default)

The default runtime now uses **automatic orchestration** under `catalog/orchestrator/` and file-based sessions under `results/workflows/<session-id>`.

This orchestrator is currently an automatic wrapper over existing `catalog/runner/*` filtering/session/script-execution primitives (not a full runner-internals rewrite yet).

Runner internals are split into focused modules under `catalog/runner/`:
- `script_catalog.py`: script discovery and runner-visible script metadata
- `session_store.py`: session metadata lifecycle, normalization, and stale output invalidation
- `data_filtering.py`: date discovery/cache plus session filtered-data creation/reuse
- `script_exec.py`: run workspace preparation and subprocess execution semantics
- `ui.py`: numbered menu rendering and input helpers


Each session stores:
- selected date range (and optional same-day hour range)
- one filtered dataset copy (`data/`) reused by later script runs in that session
- per-script execution status (`not_run`, `done`, `failed`) plus run metadata
- script run outputs under `runs/<script>/<timestamp>/`
- playback-ready timeline exports under `exports/timeline/` (generated on demand)
- session metadata in `session_state.json` (with legacy mirror `session.json`)
- a lightweight session config signature for the selected filter config

Workflow guidance for unattended startup is now deliberately narrow:
- **Step 1:** startup-safe health checks

Execution and caching are script-level:
- step completion is derived from script statuses
- completed scripts are skipped by default unless rerun is requested
- failed scripts are visible in session status and can be rerun
- script status view includes `last_run_at`, `duration`, and output path
- skipped runs are shown as cached; forced reruns are shown as recomputed

Automatic orchestration actions include:
- apply full discovered source-date range as the default session filter policy
- execute scripts in best-effort mode (continue after failures)
- hand off to Flask even when some scripts fail (partial-prep visibility over hard stop)
- scan configured roots (`results`, `data`, plus `MSH_SCAN_DIRS`)
- discover available dates from source data
- create/reuse a deterministic auto session
- prepare filtered session data
- run standard workflow scripts in order
- execute workflow scripts with non-interactive subprocess stdin (Docker-safe unattended execution)
- skip already-fresh outputs when script cache is valid
- generate/reuse playback timeline exports
- hand off to Flask as the primary interface
- capture script stdout/stderr in orchestration logs for clearer failure context

### Session playback integration

The Streamlit playback app (`catalog/webapp/app.py`) is integrated with session outputs.

Playback export location (per session):
- `results/workflows/<session-id>/exports/timeline/`

Playback-ready session (practical readiness check):
- session filtered data exists

Notes:
- playback exports are generated from session-filtered data using shared timeline export/inference helpers
- if reusable exports already exist for the current session filter signature, they are reused

When orchestration prepares playback exports:
- readiness is validated from session-filtered data
- timeline exports are generated only when needed (reused if still valid)
- Flask playback views can consume scan-discovered playback-compatible outputs

Default precompute scope is limited to startup-safe scripts:
- `machines_active_per_day`
- `sampling_rate_analysis`

Startup precompute first writes a compact shared dataset at `results/workflows/<session-id>/data/_derived/basic_metrics.csv` and startup-safe scripts consume this artifact instead of re-parsing full JSONL payloads in separate passes.

Intentionally excluded from unattended default precompute/workflow path:
- heavier exploratory scripts (`data_pr_day`, `find_stops`, `data_visualizer`, `data_analysis`, `ml_analysis`)
- legacy `corrolation_machine_pairs` (still runnable explicitly)
- Streamlit and environment/recorder tools (`data_simulator`, `interventions`, recorders, `auto_connect`)

### What is cached vs not cached

Cached per session:
- the filtered dataset under `results/workflows/<session-id>/data/`
- script execution metadata in `session_state.json` (status, timing, output path, last run timestamp)
- script outputs under `runs/<script>/<timestamp>/`

Not cached (by design):
- script correctness/validity checks against changed code or changed source data
- cache invalidation decisions beyond explicit rerun requests
- pipeline scheduling/dependency logic (this runner is workflow-guided, not a workflow engine)

Precompute is intended to front-load script execution so later interactive review is fast, but it uses the same synchronous script runner and same session cache model as manual step/script runs.


## Deprecated menu path

`catalog/runner/menu.py` is now deprecated and retained only for backward compatibility messaging.
It is no longer the primary operational path.
