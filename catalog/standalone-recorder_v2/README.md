# standalone-recorder_v2

## Status

**Active recorder component**

This recorder is intended to run either by itself on a data-capture server or together with the Flask workbench in the full MSH server mode.

## Script path

```text
catalog/standalone-recorder_v2/standalone-recorder_v2.py
```

## Docker profile

The recommended server invocation is through setup and Docker Compose:

```bash
python setup_msh.py
docker compose up -d --build
```

Choose either:

- `full-server` for Flask workbench + recorder.
- `recorder-only` for data recording without the web UI.

Manual profile selection is also possible:

```bash
docker compose --profile recorder up -d --build recorder
```

## Configuration

The recorder reads MTConnect `/current` endpoint configuration from environment variables.

Preferred simple form:

```text
MSH_RECORDER_SOURCES=IG500=http://192.168.200.251:5000/current;VTC=http://192.168.200.252:5000/current
```

JSON form:

```text
MSH_RECORDER_SOURCES_JSON={"IG500":"http://192.168.200.251:5000/current","VTC":"http://192.168.200.252:5000/current"}
```

If neither variable is set, the historical built-in lab defaults are used.

Other variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `MSH_RECORDER_DATA_DIR` | `data` | Root data directory. |
| `MSH_RECORDER_STATE_FILE` | `data/source_state/mtconnect_recorder_state.json` | Persisted sequence state. |
| `MSH_RECORDER_POLL_INTERVAL` | `0.2` | Poll interval in seconds. |
| `MSH_RECORDER_FLUSH_INTERVAL` | `1.0` | Flush interval in seconds. |
| `MSH_RECORDER_REQUEST_TIMEOUT` | `1.0` | HTTP request timeout in seconds. |
| `MSH_RECORDER_INCLUDE_CONDITION` | `false` | Include MTConnect condition values. |

## Output

The recorder writes normalized source JSONL under:

```text
data/sources/mtconnect_recorder/jsonl/<machine>/<YYYY-MM-DD>.jsonl
```

It also stores sequence state under:

```text
data/source_state/mtconnect_recorder_state.json
```

Both paths are under `data/`, so they persist through the Docker volume mount and remain ignored by git.

## Behavior

- Polls each configured MTConnect source at the configured interval.
- Parses `/current` XML into flat JSON records.
- Uses `Header.lastSequence` when available for duplicate suppression.
- Adds `timestamp`, `machine`, `machine_id`, and `source=mtconnect_recorder`.
- Buffers records in memory and flushes them to JSONL.
- Saves last-seen sequence numbers to reduce duplicates after restart.

This is practical recording infrastructure, not a fully fault-tolerant ingestion service.
