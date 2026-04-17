# Python Script Catalog

This folder contains analysis and ingestion scripts for MTConnect telemetry.
The goal of this index is to provide a **practical first-pass workflow** from raw data to interpretable findings, while keeping existing scripts separate.

## Standard analysis workflow (recommended)

Use this sequence for most investigations. Move to the next stage when the current stage no longer answers your question.

### 1) Data health checks (run first)
Purpose: verify that the dataset is usable before interpretation.

Run:
- `machines_active_per_day`
- `analyze_missing_sequence_number`
- `missing_per_day_by_machine`
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

## Runner behavior (`catalog/runner/menu.py`)

The interactive runner now uses **file-based analysis sessions** under `results/workflows/<session-id>/`.

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
- session metadata in `session_state.json` (with legacy mirror `session.json`)
- a lightweight session config signature for the selected filter config

Workflow guidance remains step-based:
- **Step 1:** health checks
- **Step 2:** raw inspection
- **Step 3:** stop-focused inspection
- **Step 4:** deeper exploratory analysis

Execution and caching are script-level:
- step completion is derived from script statuses
- completed scripts are skipped by default unless rerun is requested
- failed scripts are visible in session status and can be rerun
- script status view includes `last_run_at`, `duration`, and output path
- skipped runs are shown as cached; forced reruns are shown as recomputed

Runner actions include:
- create a new session or resume an existing session (with date range, completed count, last updated timestamp)
- run next workflow step
- run a selected workflow step (batch run stops on first failure)
- run one selected script
- precompute full workflow (Steps 1-4)
- precompute workflow up to step N
- show session status / cached outputs

Default precompute scope includes the standard runner-visible workflow scripts:
- `machines_active_per_day`
- `analyze_missing_sequence_number`
- `missing_per_day_by_machine`
- `sampling_rate_analysis`
- `data_pr_day`
- `find_stops`
- `data_visualizer`
- `data_analysis`
- `ml_analysis`

Intentionally excluded from default precompute/workflow path:
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
