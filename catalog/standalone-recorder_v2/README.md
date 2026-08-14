# standalone-recorder_v2

## Status

**Active loss-aware MTConnect recorder component**

This recorder captures MTConnect observations by sequence. It archives each `/sample` response before advancing a durable checkpoint, stores the `/probe` model, preserves Samples, Events, Conditions, attributes, and timestamps, and writes an explicit gap record when an Agent buffer has already overwritten required sequences.

The recorder is lossless relative to observations still retained by the MTConnect Agent. No recorder can retrieve observations that were overwritten before they were fetched.

For the operator-oriented guide, see [`docs/standalone_recorder.md`](../../docs/standalone_recorder.md).

## Simplest Federation first start

From the repository root, generate a normal Federation `FCP1-...` pairing code and run:

```bash
python start_recorder.py FCP1-...
```

The pairing code is the only required argument for the normal first-run path when the recorder can infer its private network.

The launcher then:

1. creates or loads the recorder's stable FCP identity;
2. runs the existing bounded private-network MTConnect scan by default;
3. auto-selects discovered sources only on the first completed configuration;
4. joins the Federation using the signed one-use pairing code;
5. starts the normal loss-aware recorder;
6. starts the Federation recorder-control worker; and
7. independently reconciles checkpoint-committed observations into a durable Federation publication outbox.

Current browser-generated pairing codes are signed, one-use, valid for up to 10 minutes, and can be generated again when another pairing attempt is needed. The recorder never persists the pairing code, enrollment token, or invitation token.

If no suitable private IPv4 `/24` can be inferred, the recorder can still join. Start a scan later from another trusted Federation device or provide `--scan-cidr` explicitly.

## Later starts

After the first successful join, omit the key. The saved device identity, trusted Federation binding, and source configuration are reused:

```bash
python start_recorder.py
```

The launcher uses a durable first-configuration marker so a source set that was deliberately emptied later is not silently repopulated on restart.

Use `Ctrl+C` for a clean stop. The checkpoint is committed after each batch, so an abrupt restart replays only uncommitted data.

## Explicit local sources remain supported

For a controlled local-only or explicit deployment:

```bash
python start_recorder.py Mazak=http://192.168.200.249:5000
```

The URL may be the Agent root or may end in `/current`, `/sample`, or `/probe`; the launcher normalizes it automatically.

Record several Agents in the same process:

```bash
python start_recorder.py \
  QuickTurn=http://192.168.200.249:5000 \
  IG500=http://192.168.200.251:5000 \
  VTC=http://192.168.200.252:5000
```

Run one catch-up cycle and exit:

```bash
python start_recorder.py Mazak=http://192.168.200.249:5000 --once
```

Use an explicit scan range when auto-inference is not appropriate:

```bash
python start_recorder.py FCP1-... --scan-cidr 192.168.200.0/24
```

Use `--no-auto-scan` only when startup discovery should be skipped deliberately.

## Federation recorder control

Any trusted device in the same Federation can open:

```text
/federation/recorders
```

and select a connected standalone recorder to:

- request a new bounded network scan;
- review the discovered machine identities reported by that recorder;
- add selected discovered sources; or
- remove currently configured sources.

The scan executes on the recorder host. Federation control messages never carry shell text or arbitrary source URLs.

Source additions identify only opaque IDs from the recorder's own latest scan. The recorder resolves and revalidates those IDs locally before changing configuration. Remote members cannot inject arbitrary URLs/credentials or request an unrestricted scan.

The scan retains the existing discovery safety boundary: validated RFC1918 private IPv4 networks of `/24` or smaller, bounded address counts, request timeouts, redirect rejection, and response-size limits.

Removing a source changes future capture only. Already recorded telemetry and historical checkpoints are retained.

The control worker persists pending result state so a relay/reporting failure can retry publication without unnecessarily rerunning an already-completed scan or source mutation.

## Federation capability and credential boundary

On successful join the recorder announces a `recorder` / `mtconnect` capability with public-safe source names/count only. It never publishes MTConnect source URLs or credentials.

`--federation-key` may also be supplied once through `FCP_RECORDER_FEDERATION_KEY`. The launcher removes that environment value after bootstrap so it is not inherited by the long-running recorder process.

Use `--require-federation` only when initial join/reconnect failure should stop capture instead of falling back to local recording.

## Capture and Federation delivery are separate commitments

Local recording remains authoritative for capture. A batch is recorded when its raw archive, detailed observations, compatibility JSONL, and recorder checkpoint commit. Federation availability is not part of that commit boundary.

Only after the local checkpoint covers a batch does the publication reconciler create deterministic, idempotent telemetry chunks. Those chunks stay in a local SQLite outbox until the Federation's logical-storage authority reports a committed write.

If the relay, authority, primary storage, or replica storage is offline, recording continues and the backlog retries later. Federation outages never move the recorder checkpoint backward and never block normal MTConnect polling.

## Federation logical storage

The recorder does not address a physical storage provider. It sends bounded authenticated application messages to the Federation owner's advertised logical-storage authority. That authority routes the batch through the existing storage control plane, grant/fencing rules, acknowledgement policy, primary/replica routing, and authoritative manifest commit.

If exactly one ready logical-storage group exists, the recorder selects it automatically. When several groups are available, select one by logical ID:

```bash
python start_recorder.py --storage-group telemetry
```

If no ready logical-storage authority/group is available, the recorder can still identify, join, advertise itself, and capture locally. Delivery waits until storage authority appears.

## Software update note

A standalone recorder launched directly with `python start_recorder.py` is not the normal Flask/Compose host-update agent. Federation **Update all devices** rebuilds the Compose-managed recorder on a normal FCP installation, but it does not currently restart an independently launched standalone recorder process. Update the standalone recorder checkout/process through its host administration path.

## Docker/server mode

The recorder also remains available as the managed Docker Compose service inside a normal FCP installation. That Compose-managed recorder is rebuilt and restarted as part of a verified Federation runtime update for its containing FCP device.

Command/bootstrap compatibility profiles may select a recorder process locally, but process selection does not grant recorder contribution authority or define a permanent device role.

## Capture algorithm

For every configured source, the recorder:

1. reads `/current` to discover the Agent instance and buffer window;
2. stores a versioned and checksummed `/probe` document;
3. requests `/sample?from=<checkpoint>&count=<batch-size>`;
4. validates sequence continuity;
5. atomically stores the original XML and its manifest;
6. atomically stores the complete observation NDJSON batch;
7. atomically stores the FCP-compatible wide JSONL batch; and
8. advances the checkpoint only after all durable writes succeed.

If the process stops after raw XML is stored but before the checkpoint is advanced, the next run recovers the archived batch before requesting newer data. Normalized batch filenames are deterministic, so replay does not append duplicate records.

## Output

### FCP-compatible telemetry JSONL

```text
data/sources/mtconnect_recorder/jsonl/
  <source>/<agent-instance>/<date>/seq-<first>-<last>-next-<next>.jsonl
```

Each observation updates its named signal and carries forward the latest known values for that machine while every source sequence remains represented. Carried state is persisted in the durable checkpoint and reset after an Agent restart or unrecoverable sequence gap.

### Complete normalized observations

```text
data/sources/mtconnect_recorder/observations/
  <source>/<agent-instance>/<date>/seq-<first>-<last>-next-<next>.ndjson
```

Detailed records retain, when supplied by the Agent or `/probe` model, sequence/instance identity, original timestamp, recorder receipt time, DataItem/component metadata, values, units, attributes, condition state, device metadata, and the exact `/probe` SHA-256 used for enrichment.

### Immutable raw archive

```text
data/sources/mtconnect_recorder/raw/
  <source>/<agent-instance>/<date>/*.xml.gz
```

Every raw batch has a JSON manifest containing sequence bounds, Agent buffer bounds, request origin, observation count, and SHA-256.

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

A buffer-overflow gap is never hidden. It is stored with the missing sequence range and reason.

### Durable state and status

```text
data/capabilities/config.json
data/source_state/mtconnect_recorder_state.json
data/source_state/mtconnect_recorder_status.json
data/source_state/mtconnect_recorder.log
data/source_state/mtconnect_recorder_autoconfig.json
```

Federation mode additionally keeps stable identity, public-safe pairing state, recorder-control state, and the durable publication outbox under the FCP data tree, including:

```text
data/federation/device/
data/federation/onboarding/
data/federation/recorder_publication/
```

## Useful launcher options

| Option | Purpose |
| --- | --- |
| positional `FCP1-...` or `--federation-key` | First Federation join only. |
| positional `NAME=URL` | Explicit MTConnect source(s). |
| `--device-name` | Stable recorder device display label. |
| `--storage-group` | Explicit logical Federation storage group. |
| `--federation-timeout` | Federation request timeout. |
| `--require-federation` | Stop if initial join/reconnect fails instead of recording locally. |
| `--require-data-sharing` | Also require a ready logical-storage publication route before capture. |
| `--sharing-timeout` | Bounded wait for required data sharing; defaults to 45 seconds. |
| `--scan-cidr` | Explicit private IPv4 scan network. |
| `--scan-port` | MTConnect discovery port; defaults to 5000. |
| `--no-auto-scan` | Skip default startup discovery. |
| `--once` | Run one catch-up cycle and exit. |

## Recorder environment

| Variable | Default | Purpose |
| --- | --- | --- |
| `FCP_RECORDER_SOURCES` | configured capability sources | `Name=http://agent;Other=http://agent` compatibility input |
| `FCP_RECORDER_DATA_DIR` | `data` | Root data directory. |
| `FCP_RECORDER_STATE_FILE` | `data/source_state/mtconnect_recorder_state.json` | Durable committed checkpoint. |
| `FCP_RECORDER_POLL_INTERVAL` | `0.2` | Delay between catch-up cycles. |
| `FCP_RECORDER_BATCH_SIZE` | `1000` | Maximum observations requested per `/sample` call. |
| `FCP_RECORDER_MAX_BATCHES_PER_CYCLE` | `20` | Catch-up batches per source before yielding. |
| `FCP_RECORDER_REQUEST_TIMEOUT` | `10.0` | HTTP request timeout in seconds. |
| `FCP_RECORDER_ONCE` | `false` | Run one catch-up cycle and exit. |
| `FCP_RECORDER_FEDERATION_KEY` | unset | Optional first-join key; removed from the process environment after bootstrap. |

Conditions are always recorded. The legacy `FCP_RECORDER_INCLUDE_CONDITION` setting is no longer used because excluding Conditions would make the recorder intentionally lossy.

## Operational requirement

The MTConnect Agent in the provided example reports `bufferSize="4096"`. Recorder downtime must remain shorter than the time required for the Agent to produce 4096 observations, or the Agent buffer must be increased. When this limit is exceeded, FCP records the exact unrecoverable sequence gap.
