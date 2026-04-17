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

The interactive runner presents only one-shot analysis scripts and keeps them grouped for navigation:

- **Simple — Stage 1–3 first-pass workflow**
- **Advanced — Stage 4 deeper exploration**
- **Legacy — compatibility / reference**

Streamlit, recorder, and environment-specific tools remain documented here but intentionally outside the runner’s default one-shot path.
