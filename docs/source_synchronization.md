# Source synchronization

MSH originally treated `data/**/*.jsonl` as one local telemetry corpus. External sources such as MTConnect adapters and Observer Phoenix change that assumption: telemetry can arrive from several systems, with their own identifiers, clock behavior, polling windows, connection paths, and API limits.

This document defines the source-synchronization layer that sits before workflow sessions and before the Parquet/DuckDB analytics cache.

## Goals

- Keep raw telemetry ingestion source-specific at the boundary.
- Convert each source into the existing MSH JSONL contract before shared analysis code sees it.
- Track a separate synchronization watermark per source.
- Keep vendor API metadata out of recursive JSONL telemetry scans.
- Make repeated synchronization runs idempotent where possible.
- Keep machine/source configuration visible from the Flask UI.
- Provide simple connection tests for MTConnect and machine-network reachability.

## Source configuration in the app

System -> Sources is the user-facing source configuration page.

It stores machine and sensor inventory under:

```text
data/source_config/machines_and_sensors.json
```

Machine entries can include:

- machine name.
- machine type.
- controller/adapter.
- MTConnect URL.
- VPN/network test host.
- VPN/network test port.
- notes.

Vibration sensor entries can include:

- sensor name.
- assigned machine.
- source system.
- signal/channel.
- axis.
- unit.
- sampling rate.
- enabled/disabled state.
- notes.

### MTConnect connection test

The **Test MTConnect** button tests the configured machine MTConnect endpoint from the Flask server/container.

If the stored URL is a base adapter URL, MSH tests `/current` automatically.

Example:

```text
10.0.0.20:5000
-> http://10.0.0.20:5000/current
```

A successful test means MSH reached the adapter endpoint and received an HTTP response. An HTTP error still means the host was reachable, but the adapter returned an error response.

### VPN/network reachability test

The **Test VPN/network** button opens a TCP connection from the Flask server/container to the configured host/port.

This does not prove the VPN client is connected at the operating-system level. It proves the useful operational question for MSH: whether the app can reach the configured machine-network target from where MSH is running.

If no VPN/network host is configured, the test falls back to the MTConnect host/port when possible.

## Dataflow

```mermaid
flowchart LR
    A[External source APIs and adapters] --> B[Source connector]
    B --> C[MSH-normalized JSONL]
    B --> D[Source sync state]
    C --> E[Telemetry analytics cache]
    C --> F[Workflow session filtering]
    F --> G[Derived metrics and playback exports]
    E --> H[Cache-aware Flask views]
    G --> H
    I[System -> Sources] --> B
```

The key boundary is `MSH-normalized JSONL`. Anything under `data/` with a `.jsonl` suffix must be safe for the existing recursive telemetry scanner. Connector metadata, raw API pages, and watermarks should be JSON or another non-JSONL format.

## Directory convention

```text
data/
  sources/
    <source-name>/
      jsonl/
        <YYYY-MM-DD>.jsonl
  source_state/
    <source-name>.json
  source_config/
    machines_and_sensors.json
```

For Observer Phoenix this becomes:

```text
data/sources/observer_phoenix/jsonl/2026-07-05.jsonl
data/source_state/observer_phoenix.json
```

## Canonical source fields

Source connectors should emit the normal MSH fields where possible and add source-specific detail as extra columns. Consumers should tolerate extra fields.

Required or strongly recommended fields:

| Field | Purpose |
| --- | --- |
| `timestamp` | UTC measurement timestamp, parseable as ISO 8601. |
| `machine_id` | Stable machine identifier after source mapping. |
| `source` | Source system name, for example `observer_phoenix` or `mtconnect`. |
| `source_record_id` | Stable record key used for idempotent append/deduplication. |
| `measurement_type` | Source measurement family, for example `trend`. |
| `value` | Scalar value when the record represents one scalar signal. |
| `unit` | Measurement unit when known. |

Useful optional fields:

| Field | Purpose |
| --- | --- |
| `machine` | Human-readable machine name. |
| `source_machine_path` | Source hierarchy/path for traceability. |
| `point_id` | Measurement point ID in the source system. |
| `point_name` | Measurement point name. |
| `channel` | Source channel/direction. |
| `channel_name` | Human-readable source channel name. |
| `alarm_info` | Source alarm payload when explicitly requested. |

## Synchronization policy

Each connector should follow the same conservative pattern:

1. Read the source-specific state file.
2. Determine the synchronization window from explicit CLI parameters or the saved watermark.
3. Apply a small overlap to the previous watermark to tolerate late-arriving data.
4. Fetch source data in bounded windows.
5. Normalize records into the MSH JSONL contract.
6. Append only records with unseen `source_record_id` values to the daily JSONL file.
7. Update the source watermark only after successful writes.
8. Rebuild the telemetry analytics cache when the new source data should be visible through cache-aware paths.

This is not a live streaming architecture. It is a repeatable synchronization step that preserves the repository's current JSONL-first and cache-derived design.

## Rethinking the pipeline

The practical change is that `data/` should no longer be treated as a single undifferentiated dump folder. It should be treated as a normalized landing zone containing multiple source partitions.

The current architecture remains valid if we introduce one extra layer before cache rebuild and workflow filtering:

```text
source connector -> normalized JSONL landing zone -> cache/session pipeline
```

Later improvements can build on this without breaking the contract:

- source mapping tables for reconciling vendor machine IDs with local MSH machine aliases.
- a global deduplication index instead of per-file `source_record_id` scanning.
- incremental Parquet updates instead of full cache rebuilds.
- separate annotation streams for alarms, notes, event cases, and operator interventions.
- separate high-volume artifact stores for spectrum/TWF data rather than flattening it into scalar trend JSONL.

## Non-goals for the first implementation

- It does not commit vendor reference PDFs or proprietary data.
- It does not replace JSONL as the source-of-truth archive.
- It does not make the Parquet cache the ingestion store.
- It does not automatically reconcile clocks across systems beyond preserving UTC timestamps and source IDs.
- It does not attempt to flatten large spectrum/TWF arrays into the normal scalar trend stream.
- The VPN/network test does not prove OS-level VPN state; it only checks reachability from MSH to the configured machine-network target.
