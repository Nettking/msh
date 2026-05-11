# MSH CNC Telemetry Workbench

MSH is a Flask-first workbench for orchestrating, replaying, and analyzing MTConnect-style CNC telemetry. It grew from standalone analysis scripts into a session-based runtime that can discover JSONL source data, prepare filtered workflow datasets, run bounded automatic scripts, export playback timelines, and expose operator/developer views through Flask.

The repository is intended for practical operation and as a research artifact: raw telemetry remains in `data/`, workflow session artifacts are written under `results/workflows/`, and reusable analysis scripts live in `catalog/`.

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

- `/` — operator overview and current runtime/workflow session summary.
- `/control` — manual refresh, workflow session selection, workflow runs, and individual script runs.
- `/status` — runtime milestones, catch-up state, discovered artifacts, and readiness signals.
- `/playback` — playback-compatible timeline exports and machine/day replay views.
- `/analyses` — discovered analysis artifacts and basic chart previews.
- `/live` — recent telemetry snapshot from scan-discovered JSONL sources.
- `/startup` — startup mode choice when an existing runtime namespace requires an operator decision.

## Repository map

- `catalog/flask_app/` — primary Flask application, routes, templates, and UI-facing services.
- `catalog/orchestrator/` — non-interactive bootstrap/catch-up orchestration.
- `catalog/runner/` — workflow session metadata, date filtering, script discovery/execution, and playback export helpers.
- `catalog/common/` — shared telemetry loading, normalization, state inference, metrics, and timeline export utilities.
- `catalog/*/` — runner-visible automatic, manual, deep/exploratory, and legacy scripts plus script-specific README files.
- `data/` — local raw JSONL telemetry input location; not intended for committed production data.
- `results/` — generated analysis outputs, workflow sessions, runtime state, and discovered artifacts.
- `example-data/` — small sample JSONL input for development and documentation.
- `ops/` — host-side operational helpers.
- `legacy/` — retained historical notes or deprecated material, not the current workflow path.

See [catalog/README.md](catalog/README.md) for the script catalog and analysis workflow.

## Detailed documentation

- [Quick start](docs/quick_start.md) — install/run commands and first-run expectations.
- [Operator guide](docs/operator_guide.md) — daily UI workflow, sessions, playback, and controls.
- [Data contract](docs/data_contract.md) — raw JSONL assumptions, normalized fields, derived artifacts, and playback-ready contract.
- [Intervention strategies](docs/intervention_strategies.md) — config-driven candidate event labels, strategies, and review-ready output schema.
- [Workflow sessions](docs/workflow_sessions.md) — session layout, cache reuse, script status, bootstrap, and catch-up behavior.
- [Architecture](docs/architecture.md) — system components, dataflow diagram, policies, and design intent.
- [Troubleshooting](docs/troubleshooting.md) — common startup, data, playback, Docker, and script-run issues.

## Deprecated interactive menu

`catalog/runner/menu.py` is retained for backward compatibility, but Flask `/control` is the primary operational surface. New operation and documentation should assume the Flask-first workflow unless explicitly maintaining legacy behavior.

## Telemetry analytics cache (Parquet + DuckDB)

Raw JSONL telemetry files in `data/` remain the source of truth. The project also includes an optional analytical cache that converts those JSONL records into partitioned Parquet files and queries them with DuckDB. This is intended to improve repeated analytical queries over the same telemetry without changing the existing JSONL workflows.

Cache layout:

```text
data/cache/parquet/machine_id=<machine>/date=<YYYY-MM-DD>/part.parquet
```

The cache is safe to delete and rebuild because it is derived entirely from raw JSONL. Existing scripts can continue reading JSONL directly; the cache is a modular helper for analytical reads.

### Rebuild the cache

From the host or inside the Flask container, run:

```bash
python -m catalog.cache.rebuild_telemetry_cache
```

The command recursively scans `data/**/*.jsonl`, writes partitioned Parquet under `data/cache/parquet/`, prints the imported row count, and prints the output cache path. It rewrites the cache from source JSONL on each run, so it is safe to run multiple times without appending duplicate cache rows.

Custom paths are available for development and tests:

```bash
python -m catalog.cache.rebuild_telemetry_cache --data-dir data --cache-dir data/cache/parquet
```

### Docker relationship

The standard Flask startup remains:

```bash
docker compose up --build flask
```

The Flask image installs the same Python dependencies as local development, including `duckdb` and `pyarrow`. The existing `docker-compose.yml` Flask service mounts `./data:/app/data`, so raw JSONL and the derived cache remain persistent on the host across container rebuilds.

Cache rebuild is manual-only for now: `docker compose up --build flask` starts Flask and does not automatically refresh `data/cache/parquet/`. Run `python -m catalog.cache.rebuild_telemetry_cache` whenever new raw telemetry should be reflected in DuckDB/Parquet queries.

### Querying the cache

Use `catalog.common.telemetry_cache.TelemetryCache` for DuckDB-backed helper queries:

- latest sample per machine
- samples by machine and timestamp range
- samples by date range
- machine activity summary
- optional pandas DataFrame output via `as_dataframe=True`

If the Parquet cache is absent, helper queries return empty results rather than failing. Existing JSONL-based code paths remain available as the fallback behavior.

### Limitations and future path

- The cache is rebuilt from JSONL and is not an operational write-ahead store.
- Freshness is based on source JSONL and Parquet file modification times; rebuild after new raw telemetry arrives.
- Missing supported fields are stored as NULL, but analytics that require those values still need to handle NULLs.
- This does not add TimescaleDB, PostgreSQL, Redis, or another live storage service. If future requirements need operational/live telemetry storage, retention policies, concurrent ingestion, or low-latency stateful queries, TimescaleDB/PostgreSQL can be evaluated as a separate architecture path while keeping JSONL export as the source-of-truth archive or interchange layer.
