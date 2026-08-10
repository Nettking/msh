# SKF Observer Phoenix integration

This document summarizes how FCP should ingest data from SKF Observer Phoenix Data Service REST API without committing the vendor reference PDF or binding the core pipeline to one vendor schema.

## Integration position

Observer Phoenix is an additional telemetry source. It should be synchronized into FCP before workflow sessions are prepared and before the Parquet/DuckDB analytics cache is rebuilt.

```text
Observer Phoenix REST API
  -> catalog.observer_phoenix exporter
  -> data/sources/observer_phoenix/jsonl/*.jsonl
  -> existing FCP cache/session/playback pipeline
```

## Relevant API areas

The first implementation uses only the parts that map cleanly to the current FCP scalar telemetry contract.

| API area | Endpoint | FCP use |
| --- | --- | --- |
| Authentication | `POST /token` | Get OAuth bearer token. |
| Machines | `GET /v1/machines/` | Discover machine IDs and names. |
| Points | `GET /v1/machines/{machineId}/points/` | Discover measurement points for a machine. |
| Trend measurements | `GET /v1/points/{pointId}/trendMeasurements` | Export scalar trend telemetry to JSONL. |
| Alarms | `GET /v2/alarms` | Future annotation stream; not mixed into first telemetry stream. |
| Dynamic measurements | `GET /v1/points/{pointId}/dynamicMeasurements` | Future high-volume artifact path for spectrum/TWF data. |

Trend measurements are the correct starting point because they are scalar values with UTC reading timestamps, point IDs, speed/process/digital context, channel-level measurements, and units.

## Enable the API in Observer

The Observer Phoenix REST interface must be enabled in Observer Monitor Service before the connector can be used. Use the Observer UI under:

```text
Database > Options > Monitor service > Phoenix Internal Web Service
```

Recommended configuration:

- enable Phoenix Internal Web Service.
- generate the authentication key.
- bind the service to an HTTPS listening URL.
- use the default port `14050` unless the environment requires a different port.
- avoid wildcard interfaces unless the deployment has an explicit network/security reason.

## Credentials

Use a dedicated Observer user with the minimum permissions needed to read machines, points, and measurements.

The Flask UI exposes a local source settings page:

```text
/sources/observer-phoenix
```

The page shows whether credentials are set from environment variables, set from a local runtime file, or still using the default/unset state. It also has a connection test that authenticates and calls the machine-list endpoint.

Local credentials are written to the ignored runtime path:

```text
data/source_config/observer_phoenix.json
```

Complete environment variables take precedence over the local runtime file:

```bash
export OBSERVER_PHOENIX_BASE_URL="https://localhost:14050"
export OBSERVER_PHOENIX_USERNAME="observer-user"
export OBSERVER_PHOENIX_PASSWORD="observer-password"
export OBSERVER_PHOENIX_VERIFY_TLS="true"
```

For a local test system with a self-signed certificate:

```bash
export OBSERVER_PHOENIX_VERIFY_TLS="false"
```

Do not use `false` in production unless the network is otherwise protected and the risk is accepted.

## Export command

Dry run first:

```bash
python -m catalog.observer_phoenix.export_jsonl \
  --from-utc 2026-07-05T00:00:00Z \
  --to-utc 2026-07-05T01:00:00Z \
  --machine-id 87 \
  --dry-run
```

Write normalized JSONL:

```bash
python -m catalog.observer_phoenix.export_jsonl \
  --from-utc 2026-07-05T00:00:00Z \
  --to-utc 2026-07-05T01:00:00Z \
  --machine-id 87
```

If CLI credentials are omitted, the exporter uses the same precedence as the Flask page: complete environment variables first, then the local runtime settings file.

After successful export, rebuild the analytics cache when cache-aware UI paths should see the new data:

```bash
python -m catalog.cache.rebuild_telemetry_cache
```

## Output shape

Each channel-level trend measurement becomes one JSONL record. Example:

```json
{
  "timestamp": "2026-07-05T00:15:00Z",
  "machine_id": "87",
  "machine": "Example Machine",
  "source": "observer_phoenix",
  "source_record_id": "observer_phoenix:trend:92:2026-07-05T00:15:00Z:1:0",
  "source_machine_path": "Company\\Plant\\Line\\Example Machine",
  "point_id": "92",
  "point_name": "Dynamic TWF",
  "measurement_type": "trend",
  "channel": "1",
  "channel_name": "Channel 1",
  "value": 0.42,
  "unit": "g",
  "speed": 659.0,
  "speed_units": "RPM"
}
```

The fields beyond the core FCP contract are intentionally source-specific. Shared scripts should continue to tolerate extra columns.

## Synchronization behavior

The exporter writes source state to:

```text
data/source_state/observer_phoenix.json
```

The state file stores the trend-measurement watermark. Repeated runs use that watermark with a small overlap and skip already written records by `source_record_id`.

## Why dynamic/spectrum data is deferred

Spectrum and time-waveform measurements are not scalar telemetry samples. They can contain arrays, UFF payloads, and large measurement bodies. Flattening them into the normal JSONL stream would make the current cache/playback pipeline slower and less predictable.

Treat them as a second integration step:

```text
data/sources/observer_phoenix/artifacts/dynamic_measurements/...
```

Then add explicit analysis scripts that understand those artifacts.

## Security notes

- Do not commit Observer credentials.
- Do not commit customer telemetry unless the repository is intended to hold that data.
- Do not commit the vendor PDF into this public repository unless redistribution is explicitly allowed.
- Prefer HTTPS; the API exchanges username/password during token acquisition.
