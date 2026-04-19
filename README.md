# MSH MTConnect Data Tools

Flask-first repository for recording, scanning, and inspecting MTConnect telemetry analyses through a continuous digital twin interface.

## Primary web application (Flask)

The primary web interface now lives in `catalog/flask_app/` and is organized into:

- `app.py`: Flask app factory + runtime startup
- `routes.py`: page routes + rescan endpoint
- `services/`: scanning/index, playback validation, chart data prep
- `templates/`: overview, analyses, machine view, playback, exploration, status
- `static/`: lightweight CSS

### Run locally

```bash
python -m catalog.flask_app.app
```

Open http://localhost:5000.

Configured scan roots come from `MSH_SCAN_DIRS` (default: `results,data`).
Shared backend logic is in `catalog/common/artifact_registry.py` so Flask is not coupled to Streamlit modules.

### Docker quick start (recommended)

```bash
docker compose up --build webapp
```

Open http://localhost:5000.

Note: Docker currently runs the Flask **development server** for a practical first iteration.

The `webapp` service mounts:

- `./results:/app/results`
- `./data:/app/data`

## Legacy Streamlit app (transitional only)

The old Streamlit app remains at `catalog/webapp/app.py` only for transition/backward compatibility. It is now legacy and no longer the primary architectural direction.

## Secondary workflow: CLI runner (`msh`)

Build and run the interactive script runner:

```bash
docker compose build msh
docker compose run --rm msh
```

## Notes

- Flask pages explicitly distinguish playback-compatible datasets vs general tabular datasets.
- Scanning is practical and explicit (`/rescan` endpoint/button) and independent from Streamlit rerun/session-state patterns.
- Invalid files are reported as read errors and should not crash the app.
