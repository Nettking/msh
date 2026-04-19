# MSH MTConnect Data Tools

Flask-first repository for recording, scanning, and inspecting MTConnect telemetry analyses through a continuous digital twin interface.

## Default workflow (automatic orchestration + Flask)

Run one command:

```bash
python -m catalog.flask_app.app
```

The startup flow is now non-interactive and automatic (with explicit default policies):

1. scans configured roots from `MSH_SCAN_DIRS` (default `results,data`)
2. discovers available dates in `data/`
3. bootstraps only the **latest discovered day** into an auto session under `results/workflows/`
4. prepares filtered session data + derived metrics for that bootstrap slice
5. runs startup-safe health-check analyses only for the bootstrap slice
6. prepares playback exports for web views
7. starts Flask on port 5000
8. continues polling for new data and incrementally processes only newly discovered days

Current defaults are intentional:
- **startup date policy:** latest discovered day only (`latest_discovered_day_only`)
- **execution policy:** best-effort pipeline (continue after individual script failures)
- **handoff policy:** Flask still starts after bootstrap orchestration, including partial-failure cases
- **update policy:** local polling loop (`poll_for_new_data_then_process_new_slice`) that avoids full historical recomputation

Open http://localhost:5000.

Default startup scope is intentionally limited to startup-safe health checks:
- `machines_active_per_day`
- `sampling_rate_analysis`

To avoid repeated full JSONL scans during startup, orchestration builds one compact shared dataset at `results/workflows/<session>/data/_derived/basic_metrics.csv` (timestamp, machine, sequence) and startup scripts read from that file.

Runtime update state is persisted at `results/workflows/runtime_state.json` so the app can surface:
- bootstrap mode and policy
- current processed range
- last successful refresh
- running/idle update state
- new-data detection and failures

Heavier exploratory scripts remain available for explicit/manual execution, but are excluded from automatic startup so `docker compose up --build webapp` remains reliable in unattended environments.

Full historical rebuild is now a deliberate/manual operation rather than the default web startup path.


Terminal output is status-oriented (discovery, processing, skipped/ran steps, outputs, failures, Flask readiness).
Workflow subprocesses run with stdin disabled (non-interactive by default) and now stream both stdout/stderr into orchestration logs for clearer failure diagnosis.

Implementation note: orchestration currently reuses substantial `catalog/runner/*` execution/session components under a non-interactive wrapper, rather than replacing all runner internals yet.

## Docker quick start (recommended)

```bash
docker compose up --build webapp
```

This now runs **prepare + serve** automatically (no menu interaction).

Optional prep-only one-shot run:

```bash
docker compose run --rm prep
```

## Deprecated path: interactive runner menu

The old numeric menu runner (`catalog/runner/menu.py`) is deprecated as an operational path.
It no longer serves as the primary decision UI and now only prints deprecation guidance plus orchestration fallback behavior.

## Flask app structure

The primary web interface lives in `catalog/flask_app/` and is organized into:

- `app.py`: Flask app factory + orchestration-aware startup
- `routes.py`: page routes + rescan endpoint
- `services/`: scanning/index, playback validation, chart data prep
- `templates/`: overview, analyses, machine view, playback, exploration, status
- `static/`: lightweight CSS

Shared backend logic is in `catalog/common/artifact_registry.py`.

## Legacy Streamlit app (transitional only)

The old Streamlit app remains at `catalog/webapp/app.py` only for transition/backward compatibility.
