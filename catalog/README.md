# Script catalog and analysis workflow

`catalog/` contains the analysis scripts and shared helpers used by the Flask-first MSH workflow. This page is intentionally focused on script discovery, script categories, and how analysis outputs fit into a workflow session. Runtime architecture and operator procedures live in the main `docs/` directory; use this page as the canonical script catalog.

## How scripts are discovered

Runner-visible scripts follow the directory convention:

```text
catalog/<script_name>/<script_name>.py
```

or, less commonly:

```text
catalog/<script_name>/main.py
```

`catalog/runner/script_catalog.py` discovers these folders, applies script metadata, and hides folders that are not safe or meaningful as one-shot analysis scripts.

## Workflow stages

The workflow session process is staged so operators can run fast health checks before heavier analysis:

1. **Health checks** — automatic scripts for startup-safe data availability and sequence/sampling diagnostics.
2. **Playback timeline** — automatic script for timeline and candidate-event export generation for `/playback`.
3. **Manual raw inspection** — operator-triggered machine/day summaries outside bootstrap/catch-up.
4. **Stop-focused inspection** — operator-triggered hourly stop-interval summaries.
5. **Deep/exploratory analysis** — heavier or research-oriented manual scripts.

The automatic runtime uses only stages 1 and 2 for the bounded playback-ready contract. Operators can run manual, deep/exploratory, and legacy scripts from `/control` when needed.

## Runner-visible scripts

| Script | Category | Default role |
| --- | --- | --- |
| `machines_active_per_day` | Simple | Automatic health check: count distinct active machines per day. |
| `analyze_missing_sequence_number` | Simple | Automatic health check: summarize missing sequence numbers per day. |
| `missing_per_day_by_machine` | Simple | Automatic health check: per-machine missing sequence summary by day. |
| `sampling_rate_analysis` | Simple | Automatic health check: average telemetry sampling rate per day. |
| `data_visualizer` | Simple | Automatic playback step: state timelines and candidate-event export. |
| `data_pr_day` | Simple | Manual raw inspection: machine/day summary CSV used by `/machine`. |
| `find_stops` | Simple | Manual stop-focused inspection: hourly stop-interval summary CSV. |
| `data_analysis` | Advanced | Deep/exploratory diagnostics and summaries. |
| `ml_analysis` | Advanced | Deep/exploratory per-machine ML baseline for future-stop prediction. |
| `corrolation_machine_pairs` | Legacy | Legacy pairwise machine stop-correlation exploration. |

See each script directory's README for script-specific inputs, outputs, and interpretation notes.

## Hidden or non-workflow folders

The following folders are intentionally excluded from runner discovery:

- `runner` — runner/session implementation internals.
- `auto_connect` — desktop automation helper, not telemetry analysis.
- `data_simulator` — Streamlit/simulation tool, not a one-shot session script.
- `interventions` — environment-specific helper.
- `standalone_recorder` — legacy ingestion tool.
- `standalone-recorder_v2` — preferred ingestion tool, but still not an analysis script.

Hidden tools may still be useful, but they should be operated from their own README files rather than from the session workflow.

## Script execution model

When a script runs for a workflow session, MSH creates an isolated workspace under:

```text
results/workflows/<session-id>/runs/<script>/<timestamp>/
```

The repository `catalog/` tree is copied into that workspace. Workflow session-filtered data is linked or copied into the workspace as `data/`. Environment variables such as `MSH_SESSION_ID`, `MSH_SESSION_DIR`, and `MSH_RUN_DIR` identify the active workflow session and run directory.

Scripts should write outputs inside their run workspace unless they intentionally use documented shared paths. Script status, exit code, output path, duration, and last-run time are tracked in `session_state.json`.

## Shared helpers for new scripts

Prefer shared modules in `catalog/common/` when creating or maintaining scripts:

- `data_loading.py` — JSONL/table loading helpers.
- `telemetry_prep.py` — timestamp, machine, and signal normalization.
- `basic_metrics.py` — compact timestamp/machine/sequence derived metrics.
- `state_inference.py` and `state_events.py` — state and candidate-event inference.
- `timeline_exports.py` — playback-compatible timeline exports.

This keeps script behavior aligned with the data contract and playback views.

## More documentation

- Terminology and daily operation: [`docs/operator_guide.md`](../docs/operator_guide.md)
- Runtime architecture: [`docs/architecture.md`](../docs/architecture.md)
- Workflow sessions and cache behavior: [`docs/workflow_sessions.md`](../docs/workflow_sessions.md)
- Data contracts and playback schema: [`docs/data_contract.md`](../docs/data_contract.md)
