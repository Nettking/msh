# MSH CNC Telemetry Workbench

MSH is a Flask-first workbench for orchestrating, replaying, and analyzing MTConnect-style CNC telemetry. It grew from standalone analysis scripts into a session-based runtime that can discover JSONL source data, prepare filtered workflow datasets, run a bounded automatic analysis pass, export playback timelines, and expose operator/developer views through Flask.

The repository is intended for practical operation and as a research artifact: raw telemetry remains in `data/`, derived workflow artifacts are written under `results/workflows/`, and reusable analysis scripts live in `catalog/`.

## Quick start

Recommended Docker startup:

```bash
docker compose up --build flask
```

Then open <http://localhost:5000>.

Local developer fallback:

```bash
python -m catalog.flask_app.app
```

Optional one-shot preparation container:

```bash
docker compose run --rm prep
```

For detailed setup, environment variables, and expected first-run behavior, see [docs/quick_start.md](docs/quick_start.md).

## Main Flask URLs

- `/` — operator overview and current runtime/session summary.
- `/control` — manual refresh, session selection, workflow runs, and individual script runs.
- `/status` — runtime milestones, catch-up state, discovered artifacts, and readiness signals.
- `/playback` — playback-compatible timeline exports and machine/day replay views.
- `/analyses` — discovered analysis artifacts and basic chart previews.
- `/live` — recent telemetry snapshot from scan-discovered JSONL sources.
- `/startup` — startup mode choice when an existing runtime namespace requires an operator decision.

## Repository map

- `catalog/flask_app/` — primary Flask application, routes, templates, and UI-facing services.
- `catalog/orchestrator/` — non-interactive runtime/bootstrap/catch-up orchestration.
- `catalog/runner/` — session metadata, date filtering, script discovery, script execution, and playback export helpers.
- `catalog/common/` — shared telemetry loading, normalization, state inference, metrics, and timeline export utilities.
- `catalog/*/` — runnable analysis scripts and their script-specific README files.
- `data/` — local raw JSONL telemetry input location; not intended for committed production data.
- `results/` — generated analysis outputs, workflow sessions, runtime state, and artifact scans.
- `example-data/` — small sample JSONL input for development and documentation.
- `ops/` — host-side operational helpers.
- `legacy/` — retained historical notes or deprecated material.

See [catalog/README.md](catalog/README.md) for the script catalog and analysis workflow.

## Detailed documentation

- [Quick start](docs/quick_start.md) — install/run commands and first-run expectations.
- [Operator guide](docs/operator_guide.md) — daily UI workflow, sessions, playback, and controls.
- [Data contract](docs/data_contract.md) — raw JSONL assumptions, normalized fields, derived artifacts, and playback-ready contract.
- [Workflow sessions](docs/workflow_sessions.md) — session layout, cache reuse, script status, bootstrap, and catch-up behavior.
- [Architecture](docs/architecture.md) — system components, dataflow diagram, policies, and design intent.
- [Troubleshooting](docs/troubleshooting.md) — common startup, data, playback, Docker, and script-run issues.

## Deprecated interactive menu

`catalog/runner/menu.py` is retained for backward compatibility, but Flask `/control` is the primary operational surface. New operation and documentation should assume the Flask-first workflow unless explicitly maintaining legacy behavior.
