# Observer Phoenix source connector

This package contains the first MSH connector for SKF Observer Phoenix Data Service REST API data.

It is a source adapter, not an analysis script. It is intentionally not runner-visible from `/control`; run it as a preparation/synchronization step before rebuilding the telemetry analytics cache or starting a workflow session.

## What it exports

The first implementation exports trend measurements only:

```text
Observer Phoenix /v1/points/{pointId}/trendMeasurements
        -> data/sources/observer_phoenix/jsonl/<YYYY-MM-DD>.jsonl
```

The output is normalized to the MSH JSONL contract with fields such as `timestamp`, `machine_id`, `source`, `source_record_id`, `point_id`, `measurement_type`, `channel`, `value`, and `unit`.

Spectrum/TWF, diagnoses, captures, notes, and alarms are deliberately not mixed into the first trend telemetry stream. They should be added later as separate source artifacts or annotation streams.

## Environment variables

```bash
export OBSERVER_PHOENIX_BASE_URL="https://localhost:14050"
export OBSERVER_PHOENIX_USERNAME="observer-user"
export OBSERVER_PHOENIX_PASSWORD="observer-password"
export OBSERVER_PHOENIX_VERIFY_TLS="true"
```

For local test systems with a self-signed certificate, either set `OBSERVER_PHOENIX_VERIFY_TLS=false` or pass `--no-verify-tls`.

## First run

Start with a dry run over a small explicit window:

```bash
python -m catalog.observer_phoenix.export_jsonl \
  --from-utc 2026-07-05T00:00:00Z \
  --to-utc 2026-07-05T01:00:00Z \
  --machine-id 87 \
  --dry-run
```

Then write normalized JSONL:

```bash
python -m catalog.observer_phoenix.export_jsonl \
  --from-utc 2026-07-05T00:00:00Z \
  --to-utc 2026-07-05T01:00:00Z \
  --machine-id 87
```

If no `--from-utc` is supplied, the exporter uses the saved source watermark. On the first run, when no watermark exists, it uses `--lookback-hours`.

## Synchronization state

The connector writes state to:

```text
data/source_state/observer_phoenix.json
```

This file stores watermarks and run metadata. It is JSON, not JSONL, so MSH's recursive telemetry scanner will not treat it as raw telemetry.

## After export

Rebuild the derived Parquet/DuckDB cache when new source data should be available through cache-aware Flask paths:

```bash
python -m catalog.cache.rebuild_telemetry_cache
```

The raw JSONL remains the source of truth; the cache remains derived and disposable.
