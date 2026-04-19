# MSH Flask Web App

Primary web interface for browsing scanned artifacts, playback-capable datasets, and generic data exploration.

## Routes

- `/` overview
- `/analyses` analysis browser
- `/machine` machine/day trends
- `/playback` playback-compatible inspection
- `/exploration` generic table exploration with charts
- `/status` scan/system status
- `POST /rescan` explicit rescan trigger

## Runtime

```bash
MSH_SCAN_DIRS=results,data python -m catalog.flask_app.app
```

Open http://localhost:5000.


Implementation note: shared registry/data logic is centralized in `catalog/common/artifact_registry.py` and reused by both Flask and legacy Streamlit surfaces.
