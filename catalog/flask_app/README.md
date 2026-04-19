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

## Runtime (prepare + serve)

```bash
MSH_SCAN_DIRS=results,data python -m catalog.flask_app.app
```

Startup now performs automatic orchestration before Flask serve:

- scan configured roots
- discover source dates in `data/`
- create/reuse auto workflow session
- run analysis preparation pipeline with cache-aware skips
- prepare playback exports
- start Flask

Open http://localhost:5000.

Implementation note: shared registry/data logic is centralized in `catalog/common/artifact_registry.py`.


## Startup coupling note

By default, Flask startup runs orchestration first. This is intentional and means startup time/failure context includes preparation work. Set `MSH_SKIP_ORCHESTRATION=1` to skip that pre-start phase when needed.
