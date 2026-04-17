# Telemetry Playback Web App

A lightweight Streamlit page for replaying processed telemetry/state exports over time.

## Run

```bash
streamlit run catalog/webapp/app.py
```

### Launch preloaded from a workflow session export directory

```bash
streamlit run catalog/webapp/app.py -- --session-export-dir results/workflows/<session-id>/exports/timeline
```

You can also preload via environment variable:

```bash
MSH_PLAYBACK_EXPORT_DIR=results/workflows/<session-id>/exports/timeline streamlit run catalog/webapp/app.py
```

The app will automatically look for session export files named:

- `timeline_rows.csv`
- `timeline_rows.parquet`
- `timeline_rows.jsonl`
- `timeline_rows.json`

## Expected input

Provide a timeline export (`.csv`, `.parquet`, `.jsonl`, or `.json`) containing row-level fields such as:

- `timestamp`
- `machine_id`
- `date`
- `state`
- `active`
- `dense_idle`
- `intervention_candidate`
- `stopped` (optional)
- `event_score`
- `fired_rules`
- `Srpm`, `Sload`, `Sovr`, `Fovr`, `Frapidovr`
- `execution`, `mode`, `program`

In session-integrated mode these fields come from session-generated exports under:

- `results/workflows/<session-id>/exports/timeline/`
- manifest metadata: `results/workflows/<session-id>/exports/timeline/manifest.json`

## Shared helpers

`catalog/common/timeline_exports.py` provides:

- `infer_timeline_rows` to generate row-level timeline state outputs from telemetry
- `build_state_interval_export` for interval-level state bands
- `load_timeline_export` for robust loading/normalization

## Playback behavior

The player supports:

- **Row-based** playback (uniform stepping)
- **Time-based** playback (sleep duration scaled by actual timestamp gaps)

Time-based mode is still discrete per row, but better reflects real telemetry spacing.
