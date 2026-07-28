# standalone-recorder_v2

## Status

**Active loss-aware MTConnect recorder component**

This recorder captures MTConnect observations by sequence. It archives each
`/sample` response before advancing a durable checkpoint, stores the `/probe`
model, preserves all Samples, Events, Conditions, attributes, and timestamps,
and writes an explicit gap record when an Agent buffer has already overwritten
required sequences.

The recorder is lossless relative to observations still retained by the
MTConnect Agent. No recorder can retrieve observations that were overwritten
before they were fetched.

## Fast local test: one command

From the repository root:

```bash
python start_recorder.py Mazak=http://192.168.200.249:5000
```

The URL may be the Agent root or may end in `/current`, `/sample`, or `/probe`.
The launcher normalizes it automatically.

Run one catch-up cycle and exit:

```bash
python start_recorder.py Mazak=http://192.168.200.249:5000 --once
```

Record several Agents in the same process:

```bash
python start_recorder.py \
  QuickTurn=http://192.168.200.249:5000 \
  IG500=http://192.168.200.251:5000 \
  VTC=http://192.168.200.252:5000
```

Use `Ctrl+C` for a clean stop. The checkpoint is already committed after each
batch, so an abrupt restart replays only uncommitted data.

## Docker/server mode

The recorder remains available as the managed Docker Compose service:

```bash
python setup_msh.py

docker compose up -d --build
```

Choose either:

- `full-server` for Flask workbench plus recorder;
- `recorder-only` for data capture without the web UI.

Existing source strings ending in `/current` remain valid. The recorder derives
and calls `/probe`, `/current`, and `/sample` itself.

## Capture algorithm

For every configured source, the recorder:

1. reads `/current` to discover the Agent instance and buffer window;
2. stores a versioned and checksummed `/probe` document;
3. requests `/sample?from=<checkpoint>&count=<batch-size>`;
4. validates sequence continuity;
5. atomically stores the original XML and its manifest;
6. atomically stores one normalized JSONL batch;
7. fsyncs both artifacts;
8. advances the checkpoint only after the durable writes succeed.

If the process stops after raw XML is stored but before the checkpoint is
advanced, the next run recovers the archived batch before requesting newer
data. Normalized batch filenames are deterministic, so replay does not append
duplicate records.

## Output

### Normalized telemetry

One observation per line:

```text
data/sources/mtconnect_recorder/jsonl/
  <source>/<agent-instance>/<date>/seq-<first>-<last>-next-<next>.jsonl
```

Each record retains, when supplied by the Agent or `/probe` model:

- `agent_instance_id` and observation `sequence`;
- original observation timestamp and recorder receipt time;
- `dataItemId`, name, category, type, subtype, and condition level;
- numeric/typed value plus original text value;
- units, native units, coordinate system, and constraints;
- complete observation attributes, including vendor-specific attributes;
- device UUID, serial number, component stream, and component path;
- the exact `/probe` SHA-256 used for enrichment.

### Immutable raw archive

```text
data/sources/mtconnect_recorder/raw/
  <source>/<agent-instance>/<date>/*.xml.gz
```

Every raw batch has a JSON manifest containing sequence bounds, Agent buffer
bounds, request origin, observation count, and SHA-256.

### Probe archive

```text
data/sources/mtconnect_recorder/probe/
  <source>/<agent-instance>/probe-<sha256>.xml.gz
```

### Gaps and Agent lifecycle events

```text
data/sources/mtconnect_recorder/gaps/
data/sources/mtconnect_recorder/events/
```

A buffer-overflow gap is never hidden. It is stored with the missing sequence
range and reason.

### Durable state and status

```text
data/source_state/mtconnect_recorder_state.json
data/source_state/mtconnect_recorder_status.json
data/source_state/mtconnect_recorder.log
```

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `MSH_RECORDER_SOURCES` | built-in lab sources | `Name=http://agent;Other=http://agent` |
| `MSH_RECORDER_DATA_DIR` | `data` | Root data directory. |
| `MSH_RECORDER_STATE_FILE` | `data/source_state/mtconnect_recorder_state.json` | Durable committed checkpoint. |
| `MSH_RECORDER_POLL_INTERVAL` | `0.2` | Delay between catch-up cycles. |
| `MSH_RECORDER_BATCH_SIZE` | `1000` | Maximum observations requested per `/sample` call. |
| `MSH_RECORDER_MAX_BATCHES_PER_CYCLE` | `20` | Catch-up batches per source before yielding. |
| `MSH_RECORDER_REQUEST_TIMEOUT` | `10.0` | HTTP request timeout in seconds. |
| `MSH_RECORDER_ONCE` | `false` | Run one catch-up cycle and exit. |

Conditions are always recorded. The legacy
`MSH_RECORDER_INCLUDE_CONDITION` setting is no longer used because excluding
Conditions would make the recorder intentionally lossy.

## Operational requirement

The MTConnect Agent in the provided example reports `bufferSize="4096"`.
Recorder downtime must remain shorter than the time required for the Agent to
produce 4096 observations, or the Agent buffer must be increased. When this
limit is exceeded, MSH records the exact unrecoverable sequence gap.
