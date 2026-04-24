# MSH MTConnect Data Tools

Flask-first repository for recording, scanning, and inspecting MTConnect telemetry analyses through a continuous digital twin interface.

## Default workflow (automatic orchestration + Flask)

Run one command:

```bash
python -m catalog.flask_app.app
```

The startup flow is now non-interactive and automatic with **webapp-first startup**:

1. scans configured roots from `MSH_SCAN_DIRS` (default `results,data`)
2. starts Flask on port 5000 immediately
3. starts runtime/orchestration manager in the background
4. discovers available dates in `data/`
5. bootstraps only the **latest discovered day** into an auto session under `results/workflows/` (background)
6. runs historical catch-up incrementally one day at a time (background)
7. keeps polling for newly arriving days after catch-up completes (background)

Current defaults are intentional:
- **startup date policy:** latest discovered day only (`latest_discovered_day_only`)
- **execution policy:** best-effort pipeline (continue after individual script failures)
- **handoff policy:** webapp first, data later (`webapp_first_data_later`)
- **update policy:** local polling loop (`poll_for_new_data_then_process_new_slice`) that avoids full historical recomputation

Open http://localhost:5000.

Primary operator controls are now available in the Flask UI at `/control` (runtime refresh/session-aware workflow runs/script runs/recent control history), replacing the deprecated terminal menu as the main control surface.

The `/control` panel is currently an MVP: single-process threaded execution, best-effort action handling, and in-memory recent activity/log snippets (not restart-persistent).

Default startup scope is intentionally limited to startup-safe health checks:
- `machines_active_per_day`
- `sampling_rate_analysis`

`data_pr_day` remains a manual/heavier workflow output (not automatic startup coverage). Machine/day diagnostics from `data_pr_day` remain available for debugging, but playback is now the primary operator workflow.

To avoid repeated full JSONL scans during startup, orchestration builds one compact shared dataset at `results/workflows/<session>/data/_derived/basic_metrics.csv` (timestamp, machine, sequence) and startup scripts read from that file.

Runtime update state is persisted at `results/workflows/runtime_state.json` so the app can surface explicit availability vs readiness:
- app/runtime startup milestones (`app_started_at`, `runtime_started_at`)
- discovery/bootstrap/catch-up progress and timestamps
- currently processing date, next queued date, and last completed step/date
- verified processed-day counts vs total discovered days
- last successful refresh and last failure
- view contract readiness (`/status`, `/control`, `/playback`, catch-up contract)

Heavier exploratory scripts remain available for explicit/manual execution, but are excluded from automatic startup so `docker compose up --build flask` remains reliable in unattended environments.

Full historical rebuild is now a deliberate/manual operation rather than the default web startup path.

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

## Host-side Windows startup helper (VPN monitor + existing startup)

For Windows host operation, use:

```powershell
./ops/start-system.ps1
```

This helper is **host-side only** and intentionally small in scope:
- checks `ops/vpn/reconnect-vpn.ps1` exists
- starts `ops/vpn/reconnect-vpn.ps1` in a separate PowerShell process first (unless already running)
- then runs the current recommended startup command by default: `docker compose up --build flask`

Why Docker default in this wrapper?
- this aligns with the repository's documented **recommended quick start**
- if needed, operators can override startup command components via script parameters (`-StartupExecutable`, `-StartupArguments`)

Lifecycle notes:
- VPN monitor is launched as a separate background host process and continues running if the main startup command exits
- stop it explicitly when needed (for example with `Stop-Process -Id <pid>`)
- re-running `./ops/start-system.ps1` will reuse existing monitor process(es) instead of starting duplicates

Out of scope for this change:
- VPN profile configuration or embedding secrets
- moving VPN handling into Docker
- adding record/live orchestration or redesigning startup architecture

## Docker quick start (recommended)

```bash
docker compose up --build flask
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
- `templates/`: overview, status, control, playback
- `static/`: lightweight CSS

Shared backend logic is in `catalog/common/artifact_registry.py`.

## Legacy Streamlit app (archived only)

The old Streamlit workspace under `catalog/webapp/` is archived for source reference only and is out of the runtime path.
