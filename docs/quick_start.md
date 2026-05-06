# Quick start

This guide gets the Flask-first MSH runtime running from a fresh checkout and explains what should happen during the first few minutes.

## Prerequisites

- Docker and Docker Compose for the recommended path.
- Python dependencies from `requirements.txt` only if running without Docker.
- JSONL telemetry files in `data/` when you want real analysis results. A small example file is available under `example-data/` for development.

## Start with Docker

```bash
docker compose up --build flask
```

Open <http://localhost:5000> once Flask starts. The app is designed to become available before all data preparation finishes.

## Start without Docker

Use this for local development or troubleshooting only:

```bash
python -m catalog.flask_app.app
```

Flask reads these useful environment variables:

- `FLASK_RUN_HOST` — defaults to `0.0.0.0`.
- `FLASK_RUN_PORT` — defaults to `5000`.
- `FLASK_DEBUG` — set to `1` for Flask debug mode.
- `MSH_FLASK_SECRET` — Flask secret key; defaults to `msh-dev` for development.
- `MSH_SKIP_ORCHESTRATION=1` — starts Flask without the background runtime.
- `MSH_SCAN_DIRS` — comma-separated artifact scan roots; defaults are supplemented with `data` and `results` by the runtime.

## What startup does

The default path is webapp-first:

1. Flask starts and registers the operator routes.
2. The runtime manager records app/runtime milestones in `results/workflows/runtime_state.json`.
3. Source dates are discovered from JSONL telemetry in `data/`.
4. A deterministic automatic workflow session is created or reused for the latest discovered day.
5. Session-filtered data is prepared under `results/workflows/<session-id>/data/`.
6. A compact derived metrics artifact is created at `data/_derived/basic_metrics.csv` inside the session.
7. The automatic playback-ready script set runs in best-effort mode.
8. Playback timeline exports are generated or reused under `exports/timeline/` inside the session.
9. Historical catch-up proceeds one day at a time in the background, then the runtime polls for newly arriving source days.

The automatic script set is intentionally bounded: `machines_active_per_day`, `analyze_missing_sequence_number`, `missing_per_day_by_machine`, `sampling_rate_analysis`, and `data_visualizer`.

## First pages to open

- <http://localhost:5000/status> — verify discovery, bootstrap, catch-up, failures, and readiness.
- <http://localhost:5000/control> — select sessions, trigger refreshes, and run workflows or scripts.
- <http://localhost:5000/playback> — inspect playback-ready exports after filtered session data exists.
- <http://localhost:5000/analyses> — browse discovered CSV/JSON artifacts.

## Optional one-shot preparation

```bash
docker compose run --rm prep
```

Use this when you want to run preparation without keeping the Flask container attached.

## Windows helper

`ops/start-system.ps1` is a host-side wrapper around the recommended Docker command. It can optionally launch a VPN monitor script if you pass a valid `-VpnReconnectScript`, but no VPN helper is required by default.
