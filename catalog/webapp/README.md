# Telemetry Playback Web App

A lightweight Streamlit page for replaying processed telemetry/state exports over time.

## Run

```bash
streamlit run catalog/webapp/app.py
```

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
