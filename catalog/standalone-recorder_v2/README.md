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

## Join the Federation and publish recorded data

The same launcher can run the recorder as a headless FCP device. Generate the
normal **Pair another device** code from the Federation owner. The code begins
with `FCP1-`, is signed, short-lived, and one-use. Pass it only on the recorder's
first join:

```bash
python start_recorder.py \
  Mazak=http://192.168.200.249:5000 \
  --federation-key "FCP1-..."
```

That one command performs the headless bootstrap before recording begins:

1. creates or loads the recorder's stable FCP device identity;
2. redeems the signed pairing code and joins its Federation;
3. persists only the public-safe reconnect binding, never the pairing code,
   enrollment token, or invitation token;
4. announces a `recorder` / `mtconnect` capability with source names and count,
   but never MTConnect source URLs or credentials;
5. starts the normal loss-aware recorder; and
6. independently reconciles checkpoint-committed observations into a durable
   Federation publication outbox.

On later starts, omit the key. The saved device identity and trusted Federation
binding are reused:

```bash
python start_recorder.py Mazak=http://192.168.200.249:5000
```

If the Federation has exactly one ready logical-storage group, the recorder
selects it automatically. When several groups are available, select one by
logical ID:

```bash
python start_recorder.py \
  Mazak=http://192.168.200.249:5000 \
  --storage-group telemetry
```

`--federation-key` may also be supplied once through
`FCP_RECORDER_FEDERATION_KEY`. The launcher removes that environment value after
bootstrap so it is not inherited by the long-running recorder. The key is never
written into recorder configuration, status, telemetry, the publication outbox,
or Federation messages.

### Capture and Federation delivery are separate commitments

Local recording remains authoritative for capture. A batch is recorded when its
raw archive, detailed observations, compatibility JSONL, and recorder checkpoint
commit. Federation availability is not part of that commit boundary.

Only after that local checkpoint covers a batch does the publication reconciler
create deterministic, idempotent telemetry chunks. Those chunks stay in a local
SQLite outbox until the Federation's logical-storage authority reports a
committed write. If the relay, authority, primary storage, or replica storage is
offline, recording continues and the backlog retries later.

The recorder does not address a physical storage provider. It sends bounded
application messages to the Federation owner's advertised logical-storage
authority. That authority routes the batch through the existing storage control
plane, grant/fencing rules, acknowledgement policy, primary/replica routing, and
authoritative manifest commit.

If no ready logical-storage authority is currently available, the recorder can
still identify, join, advertise its recorder capability, and capture locally.
Delivery waits until storage authority appears. Use `--require-federation` only
when you deliberately want initial Federation join/reconnect failure to stop the
process instead of falling back to local recording.

## Docker/server mode

The recorder remains available as the managed Docker Compose service:

```bash
python setup_fcp.py

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
6. atomically stores the complete observation NDJSON batch;
7. atomically stores the FCP-compatible wide JSONL batch;
8. advances the checkpoint only after all durable writes succeed.

If the process stops after raw XML is stored but before the checkpoint is
advanced, the next run recovers the archived batch before requesting newer
data. Normalized batch filenames are deterministic, so replay does not append
duplicate records.

## Output

### FCP-compatible telemetry JSONL

The regular FCP scan path receives one wide state snapshot for every MTConnect
sequence:

```text
data/sources/mtconnect_recorder/jsonl/
  <source>/<agent-instance>/<date>/seq-<first>-<last>-next-<next>.jsonl
```

Each observation updates its named signal and carries forward the latest known
values for that machine. Existing FCP consumers therefore continue to see
columns such as `Srpm`, `execution`, and `Xabs`, while every source sequence is
still represented. The carried state is persisted in the durable checkpoint so
it survives normal restarts. It is reset after an Agent restart or an
unrecoverable sequence gap rather than carrying stale values across missing
data.

### Complete normalized observations

Every Sample, Event, and Condition is also stored as a detailed record outside
the automatic wide-telemetry scan:

```text
data/sources/mtconnect_recorder/observations/
  <source>/<agent-instance>/<date>/seq-<first>-<last>-next-<next>.ndjson
```

Each detailed record retains, when supplied by the Agent or `/probe` model:

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

Headless Federation mode additionally keeps the stable device identity,
public-safe pairing state, and durable publication outbox under:

```text
data/federation/device/
data/federation/onboarding/
data/federation/recorder_publication/
```

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `FCP_RECORDER_SOURCES` | built-in lab sources | `Name=http://agent;Other=http://agent` |
| `FCP_RECORDER_DATA_DIR` | `data` | Root data directory. |
| `FCP_RECORDER_STATE_FILE` | `data/source_state/mtconnect_recorder_state.json` | Durable committed checkpoint. |
| `FCP_RECORDER_POLL_INTERVAL` | `0.2` | Delay between catch-up cycles. |
| `FCP_RECORDER_BATCH_SIZE` | `1000` | Maximum observations requested per `/sample` call. |
| `FCP_RECORDER_MAX_BATCHES_PER_CYCLE` | `20` | Catch-up batches per source before yielding. |
| `FCP_RECORDER_REQUEST_TIMEOUT` | `10.0` | HTTP request timeout in seconds. |
| `FCP_RECORDER_ONCE` | `false` | Run one catch-up cycle and exit. |
| `FCP_RECORDER_FEDERATION_KEY` | unset | Optional first-join `FCP1-...` key; removed from the process environment after bootstrap. |

Conditions are always recorded. The legacy
`FCP_RECORDER_INCLUDE_CONDITION` setting is no longer used because excluding
Conditions would make the recorder intentionally lossy.

## Operational requirement

The MTConnect Agent in the provided example reports `bufferSize="4096"`.
Recorder downtime must remain shorter than the time required for the Agent to
produce 4096 observations, or the Agent buffer must be increased. When this
limit is exceeded, FCP records the exact unrecoverable sequence gap.
