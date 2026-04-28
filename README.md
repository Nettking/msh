# MSH MTConnect Data Tools

Flask-first repository for recording, scanning, and inspecting MTConnect telemetry analyses through a continuous digital twin interface.

## Quick start (Docker, recommended)

From a fresh checkout, run:

```bash
docker compose up --build flask
```

Open http://localhost:5000.

### What this Docker startup does

The startup flow is now non-interactive and automatic with **webapp-first startup**:

1. scans configured roots from `MSH_SCAN_DIRS` (default `results,data`)
2. starts Flask on port 5000 immediately
3. starts runtime/orchestration manager in the background
4. discovers available dates in `data/`
5. discovers the latest source-data day and creates/reuses that day’s workflow session
6. runs a **full initial one-day analysis pass** for that latest day (background)
7. runs historical catch-up incrementally one day at a time (background)
8. keeps polling for newly arriving days after catch-up completes (background)

Current defaults are intentional:
- **startup date policy:** latest discovered day only (`latest_discovered_day_only`)
- **execution policy:** best-effort pipeline (continue after individual script failures)
- **handoff policy:** webapp first, data later (`webapp_first_data_later`)
- **update policy:** local polling loop (`poll_for_new_data_then_process_new_slice`) that avoids full historical recomputation

Primary operator controls are now available in the Flask UI at `/control` (runtime refresh/session-aware workflow runs/script runs/recent control history), replacing the deprecated terminal menu as the main control surface.

The `/control` panel is currently an MVP: single-process threaded execution, best-effort action handling, and in-memory recent activity/log snippets (not restart-persistent).

Initial latest-day bootstrap now runs the full runner-supported analysis script set for that one day before historical catch-up begins.

Automatic **historical catch-up verification** remains intentionally limited to startup-safe health checks:
- `machines_active_per_day`
- `analyze_missing_sequence_number`
- `missing_per_day_by_machine`
- `sampling_rate_analysis`

Scripts intentionally hidden from runner discovery (`auto_connect`, recorders, simulator, interventions) remain excluded by design.

To avoid repeated full JSONL scans during startup, orchestration builds one compact shared dataset at `results/workflows/<session>/data/_derived/basic_metrics.csv` (timestamp, machine, sequence) and startup scripts read from that file.

Runtime update state is persisted at `results/workflows/runtime_state.json` so the app can surface explicit availability vs readiness:
- app/runtime startup milestones (`app_started_at`, `runtime_started_at`)
- discovery/bootstrap/catch-up progress and timestamps
- currently processing date, next queued date, and last completed step/date
- verified processed-day counts vs total discovered days
- last successful refresh and last failure
- view contract readiness (`/status`, `/control`, `/playback`, catch-up contract)

Latest-day bootstrap analysis runs automatically at startup; full historical rebuild remains a deliberate/manual operation rather than the default web startup path.

`/control` now supports explicit manual scope selection beyond bootstrap latest-day behavior:
- select an existing workflow session from inventory
- create/reuse a session for latest day, selected day, or custom date range
- run an explicit full historical range session (manual heavy operation)
- run startup-safe checks, full workflow, or individual scripts against the chosen session scope
- validate scope inputs (required dates, ISO format, and start/end ordering) before dispatch
- redirect back to the actual resolved/created target session after control actions


Terminal output is status-oriented (Flask immediate startup, background runtime start, discovery, current processing day, progress updates, outputs, failures).
Workflow subprocesses run with stdin disabled (non-interactive by default) and now stream both stdout/stderr into orchestration logs for clearer failure diagnosis.

Implementation note: orchestration currently reuses substantial `catalog/runner/*` execution/session components under a non-interactive wrapper, rather than replacing all runner internals yet.

### Useful URLs

- Flask UI: http://localhost:5000
- Control panel: http://localhost:5000/control
- Status view: http://localhost:5000/status
- Playback view: http://localhost:5000/playback

## Optional developer fallback (without Docker)

Use this only for local development/troubleshooting when you explicitly do not want Docker:

```bash
python -m catalog.flask_app.app
```

## Optional Windows/PowerShell helper

For Windows host operation, use:

```powershell
./ops/start-system.ps1
```

This helper is **host-side only** and intentionally small in scope:
- runs the current recommended startup command by default: `docker compose up --build flask`
- requires no VPN script by default
- optionally starts a VPN monitor script first **only if you pass a valid `-VpnReconnectScript` path**

Why Docker default in this wrapper?
- this aligns with the repository's documented **recommended quick start**
- if needed, operators can override startup command components via script parameters (`-StartupExecutable`, `-StartupArguments`)

Lifecycle note:
- if you provide `-VpnReconnectScript`, the VPN monitor is launched as a separate background host process and continues running if the main startup command exits

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
- `templates/`: overview, status, control, playback
- `static/`: lightweight CSS

Shared backend logic is in `catalog/common/artifact_registry.py`.

## Legacy Streamlit app (archived only)

The old Streamlit workspace under `catalog/webapp/` is archived for source reference only and is out of the runtime path.
