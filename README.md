# MSH CNC Telemetry Server

MSH is a server-oriented CNC telemetry workbench. A single checkout can run as an always-on Flask server, an MTConnect data recorder, a full recorder-plus-workbench server, or a one-shot preparation/synchronization job.

The main deployment pattern is: keep raw telemetry under `data/`, keep generated workflow/session artifacts under `results/workflows/`, run selected services with Docker Compose profiles, and access the Flask workbench from another computer through a browser.

## Quick start

Recommended server setup:

```bash
python setup_msh.py
docker compose up -d --build
```

Then open the web UI, if the selected mode includes it:

```text
http://localhost:5000
```

From another computer on the same network:

```text
http://<server-ip>:5000
```

The setup helper lets you choose what this checkout should activate:

- full server: Flask workbench plus MTConnect recorder.
- web workbench: Flask, orchestration, playback, source settings, and analysis UI.
- web UI only: Flask without background orchestration.
- recorder only: MTConnect data recorder without web UI.
- one-shot prep or Observer Phoenix sync.

For detailed setup, ports, profiles, and firewall notes, see [docs/server_setup.md](docs/server_setup.md) and [docs/quick_start.md](docs/quick_start.md).

Local developer fallback:

```bash
python -m catalog.flask_app.app
```

## AI explainer

MSH includes a first read-only local AI explainer for asking system-understanding questions about the repository. It indexes selected documentation and code, retrieves relevant context, and sends that context to a local Ollama model.

Preview retrieved context without calling Ollama:

```bash
python -m catalog.ai.ask --dry-run "How does data flow through MSH?"
```

Ask through Ollama:

```bash
python -m catalog.ai.ask "What does /control do?"
```

Use `MSH_AI_MODEL` or `--model` to select the Ollama model. See [docs/ai_explainer.md](docs/ai_explainer.md) for scope, exclusions, and safety boundaries.

## Main Flask URLs

- `/` — operator overview and current runtime/workflow session summary.
- `/control` — manual refresh, workflow session selection, workflow runs, and individual script runs.
- `/status` — runtime milestones, catch-up state, discovered artifacts, and readiness signals.
- `/playback` — playback-compatible timeline exports and machine/day replay views.
- `/analyses` — discovered analysis artifacts and basic chart previews.
- `/live` — recent telemetry snapshot from scan-discovered JSONL sources.
- `/operator-strategies` — OSL-style operator strategy and decision/action capture for MSH field work.
- `/sources/observer-phoenix` — Observer Phoenix credential/status page.
- `/ai` — read-only local AI explainer for system-understanding questions.
- `/startup` — startup mode choice when an existing runtime namespace requires an operator decision.

## Repository map

- `setup_msh.py` — interactive local deployment setup that writes ignored Docker Compose settings.
- `docker-compose.yml` — profile-driven server components: web, recorder, full, prep, and observer-sync.
- `catalog/flask_app/` — primary Flask application, routes, templates, and UI-facing services.
- `catalog/orchestrator/` — non-interactive bootstrap/catch-up orchestration.
- `catalog/runner/` — workflow session metadata, date filtering, script discovery/execution, and playback export helpers.
- `catalog/common/` — shared telemetry loading, normalization, source synchronization state, state inference, metrics, and timeline export utilities.
- `catalog/observer_phoenix/` — source connector that synchronizes SKF Observer Phoenix trend measurements into MSH-normalized JSONL.
- `catalog/standalone-recorder_v2/` — configurable MTConnect recorder for recorder-only or full-server deployments.
- `catalog/ai/` — read-only repository explanation helpers backed by local Ollama.
- `catalog/*/` — runner-visible automatic, manual, deep/exploratory, and legacy scripts plus script-specific README files.
- `data/` — local raw JSONL telemetry input and source-specific landing location; not intended for committed production data.
- `data/sources/` — source-specific normalized JSONL landing area for synchronized/recorded external sources.
- `data/source_state/` — source synchronization and recorder watermarks/state; not telemetry input.
- `data/operator_strategy_records/` — local operator strategy capture notes; JSON, not telemetry JSONL.
- `results/` — generated analysis outputs, workflow sessions, runtime state, and discovered artifacts.
- `example-data/` — small sample JSONL input for development and documentation.
- `ops/` — host-side operational helpers.
- `legacy/` — retained historical notes or deprecated material, not the current workflow path.

See [catalog/README.md](catalog/README.md) for the script catalog and analysis workflow.

## Detailed documentation

- [Server setup](docs/server_setup.md) — deployment modes, LAN access, Docker Compose profiles, and recorder setup.
- [Quick start](docs/quick_start.md) — setup commands and first-run expectations.
- [Operator guide](docs/operator_guide.md) — daily UI workflow, sessions, playback, and controls.
- [Operator strategy capture](docs/operator_strategy_capture.md) — OSL-style field-capture page for operator decisions and action timing.
- [Data contract](docs/data_contract.md) — raw JSONL assumptions, normalized fields, derived artifacts, and playback-ready contract.
- [Source synchronization](docs/source_synchronization.md) — multi-source landing layout, watermarks, and synchronization policy.
- [SKF Observer Phoenix integration](docs/integrations/skf_observer_phoenix.md) — Observer Phoenix setup, export command, and mapping to MSH JSONL.
- [Intervention strategies](docs/intervention_strategies.md) — config-driven candidate event labels, strategies, and review-ready output schema.
- [Workflow sessions](docs/workflow_sessions.md) — session layout, cache reuse, script status, bootstrap, and catch-up behavior.
- [Architecture](docs/architecture.md) — system components, dataflow diagram, policies, and design intent.
- [AI explainer](docs/ai_explainer.md) — read-only Ollama/RAG explainer scope, indexed sources, exclusions, and usage.
- [Troubleshooting](docs/troubleshooting.md) — common startup, data, playback, Docker, and script-run issues.

## Data layout note

Current runtime and cache paths generally support recursive `data/**/*.jsonl` discovery. Some older manual or legacy scripts may still assume a flat `data/*.jsonl` layout. Prefer the shared helpers in `catalog/common/` and `catalog/runner/` for new work so recursive discovery, timestamp parsing, and machine normalization stay consistent.

For synchronized external sources, write only MSH-normalized telemetry JSONL under `data/sources/<source>/jsonl/`. Keep connector state, API metadata, and raw non-telemetry payloads out of `.jsonl` files under `data/`.

## Deprecated interactive menu

`catalog/runner/menu.py` is retained for backward compatibility, but Flask `/control` is the primary operational surface. New operation and documentation should assume the Flask-first workflow unless explicitly maintaining legacy behavior.

## Telemetry analytics cache (Parquet + DuckDB)

Raw JSONL telemetry files in `data/` remain the source of truth. The project also includes an analytical read cache that converts those JSONL records into partitioned Parquet files and queries them with DuckDB. Fresh caches are now used by selected production Flask paths, including live/latest telemetry, playback machine/day loading, exploration filtering, and machine/day summaries, while JSONL/session files remain the fallback when the cache is missing, stale, or lacks required fields.

Cache layout:

```text
data/cache/parquet/machine_id=<machine>/date=<YYYY-MM-DD>/part.parquet
```

The cache is safe to delete and rebuild because it is derived entirely from raw JSONL. Existing scripts can continue reading JSONL directly; cache-aware Flask routes check freshness first and fall back conservatively. Rebuilds also write `data/cache/parquet/_manifest.json` with source JSONL paths, mtimes, sizes, imported row count, and rebuild time so freshness/status checks can detect changed, renamed, or deleted source files.

### Rebuild the cache

From `/control`, use **Rebuild telemetry analytics cache** to run a rebuild and show the result in recent control activity. From the host or inside the Flask container, you can also run:

```bash
python -m catalog.cache.rebuild_telemetry_cache
```

The command recursively scans `data/**/*.jsonl`, loads the raw rows, writes partitioned Parquet under `data/cache/parquet/`, prints the imported row count, and prints the output cache path. It rewrites the cache from source JSONL on each run, so it is safe to run multiple times without appending duplicate cache rows. This is still a full rebuild, not incremental ingestion.

Custom paths are available for development and tests:

```bash
python -m catalog.cache.rebuild_telemetry_cache --data-dir data --cache-dir data/cache/parquet
```

### Docker relationship

The standard server startup is:

```bash
python setup_msh.py
docker compose up -d --build
```

The Flask image installs the same Python dependencies as local development, including `duckdb` and `pyarrow`. The Docker services mount `./data:/app/data`, so raw JSONL, source configuration, recorder state, and the derived cache remain persistent on the host across container rebuilds.

Cache rebuild is manual-only for now: starting the server does not automatically refresh `data/cache/parquet/`. Use `/control` or run `python -m catalog.cache.rebuild_telemetry_cache` whenever new raw telemetry should be reflected in DuckDB/Parquet queries. `/status` shows whether the cache is missing, fresh, or stale, plus source file and cached-row counts.

### Querying the cache

Use `catalog.common.telemetry_cache.TelemetryCache` for DuckDB-backed helper queries:

- latest sample per machine
- recent samples per machine for live/latest UI state
- playback timeline derivation for cache-covered machine/day slices
- exploration samples filtered by date window
- samples by machine and timestamp range
- samples by date range
- machine/day row counts
- machine activity summary
- optional pandas DataFrame output via `as_dataframe=True`

If the Parquet cache is absent, helper queries return empty results rather than failing. Cache-aware Flask paths additionally require a fresh cached status check before using the cache and log when they use DuckDB/Parquet versus JSONL or session-export fallback paths. Cache-status scans are protected by a short TTL in request paths so frequently refreshed pages do not recursively inspect every JSONL/Parquet file on every request.

### Limitations and future path

- The cache is rebuilt from JSONL and is not an operational write-ahead store.
- Rebuilds currently load JSONL into pandas before writing Parquet; streaming/incremental rebuilds are future work.
- Queries currently open short-lived in-memory DuckDB connections; a persistent connection/cache manager is future work if UI refresh frequency requires it.
- Live/latest telemetry currently uses a DuckDB window query over cache rows; a rolling latest-row cache or partition-pruned latest-query strategy is future work for multi-month datasets.
- Freshness is based on source JSONL and manifest file identity; rebuild after new raw telemetry arrives. Request paths use a short cache-status TTL, so the UI can lag by a few seconds before reporting newly stale/fresh state.
- Missing supported fields are stored as NULL, but analytics that require those values still need to handle NULLs.
- This does not add TimescaleDB, PostgreSQL, Redis, or another live storage service. If future requirements need operational/live telemetry storage, retention policies, concurrent ingestion, or low-latency stateful queries, TimescaleDB/PostgreSQL can be evaluated as a separate architecture path while keeping JSONL export as the source-of-truth archive or interchange layer.
