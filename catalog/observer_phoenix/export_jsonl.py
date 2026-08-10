"""Export Observer Phoenix trend measurements into FCP-compatible JSONL.

This command is intentionally source-specific at the boundary and conservative at
FCP's core boundary: only normalized telemetry JSONL is written under ``data/``.
Source state and watermarks are stored separately as JSON so recursive telemetry
scans do not ingest connector metadata as if it were machine data.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import timedelta
import json
from pathlib import Path
from typing import Any, Iterable

from catalog.common.source_sync import format_utc, load_state, parse_utc, save_state, subtract_overlap, utc_now
from catalog.observer_phoenix.client import ObserverPhoenixClient, ObserverPhoenixConfig
from catalog.observer_phoenix.settings import resolve_runtime_config


SOURCE_NAME = "observer_phoenix"
WATERMARK_NAME = "trend_measurements"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Observer Phoenix trend measurements to FCP JSONL.")
    parser.add_argument("--base-url", default=None, help="Observer Phoenix base URL. Defaults to runtime settings or OBSERVER_PHOENIX_BASE_URL.")
    parser.add_argument("--username", default=None, help="Observer username. Defaults to runtime settings or OBSERVER_PHOENIX_USERNAME.")
    parser.add_argument("--password", default=None, help="Observer password. Defaults to runtime settings or OBSERVER_PHOENIX_PASSWORD.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="FCP data directory.")
    parser.add_argument("--from-utc", default=None, help="Inclusive UTC start timestamp. Defaults to saved watermark or lookback window.")
    parser.add_argument("--to-utc", default=None, help="Inclusive UTC end timestamp. Defaults to current UTC time.")
    parser.add_argument("--lookback-hours", type=int, default=24, help="First-run lookback when no watermark exists.")
    parser.add_argument("--overlap-minutes", type=int, default=5, help="Overlap applied to saved watermark to tolerate late data.")
    parser.add_argument("--machine-id", action="append", default=[], help="Machine ID to export. Repeat to export several machines.")
    parser.add_argument("--point-id", action="append", default=[], help="Point ID to export directly. Repeat to export several points.")
    parser.add_argument("--include-alarm-info", action="store_true", help="Ask Observer Phoenix to include alarm info in trend responses.")
    parser.add_argument("--include-raw", action="store_true", help="Include raw Observer Phoenix response fragments in each output record.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and normalize records, but do not write JSONL or state.")
    parser.add_argument("--no-verify-tls", action="store_true", help="Disable TLS certificate verification for test environments.")
    return parser.parse_args()


def build_client(args: argparse.Namespace) -> ObserverPhoenixClient:
    if args.base_url or args.username or args.password:
        if not args.base_url or not args.username or not args.password:
            raise SystemExit("Provide --base-url, --username, and --password together, or use environment/local runtime settings.")
        config = ObserverPhoenixConfig(
            base_url=args.base_url,
            username=args.username,
            password=args.password,
            verify_tls=not args.no_verify_tls,
        )
    else:
        config, _ = resolve_runtime_config(args.data_dir)
        if args.no_verify_tls:
            config.verify_tls = False
    return ObserverPhoenixClient(config)


def _field(payload: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in payload:
            return payload[name]
    return None


def _string_id(value: Any, fallback: str = "unknown") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text or fallback


def _record_id(source_name: str, point_id: str, timestamp: str, channel: str, index: int) -> str:
    return f"{source_name}:trend:{point_id}:{timestamp}:{channel}:{index}"


def normalize_trend_measurement(
    *,
    source_name: str,
    machine: dict[str, Any],
    point: dict[str, Any],
    reading: dict[str, Any],
    include_raw: bool = False,
) -> list[dict[str, Any]]:
    timestamp_value = _field(reading, "ReadingTimeUTC", "readingTimeUTC", "timestamp")
    if not timestamp_value:
        return []
    timestamp = format_utc(timestamp_value)
    machine_id = _string_id(_field(machine, "ID", "id"))
    point_id = _string_id(_field(reading, "PointID", "pointID", "pointId") or _field(point, "ID", "id"))
    measurements = _field(reading, "Measurements", "measurements") or []
    if not isinstance(measurements, list):
        measurements = []
    if not measurements:
        measurements = [{}]

    records: list[dict[str, Any]] = []
    for index, measurement in enumerate(measurements):
        if not isinstance(measurement, dict):
            measurement = {}
        channel = _string_id(_field(measurement, "Channel", "channel"), fallback=str(index))
        record: dict[str, Any] = {
            "timestamp": timestamp,
            "machine_id": machine_id,
            "machine": _field(machine, "Name", "name"),
            "source": source_name,
            "source_record_id": _record_id(source_name, point_id, timestamp, channel, index),
            "source_machine_path": _field(machine, "Path", "path"),
            "point_id": point_id,
            "point_name": _field(point, "Name", "name"),
            "point_description": _field(point, "Description", "description"),
            "point_node_type": _field(point, "NodeType", "nodeType"),
            "point_node_type_name": _field(point, "NodeTypeName", "nodeTypeName"),
            "measurement_type": "trend",
            "channel": channel,
            "channel_name": _field(measurement, "ChannelName", "channelName"),
            "value": _field(measurement, "Level", "level"),
            "unit": _field(measurement, "Units", "units") or _field(point, "EU", "eu"),
            "bov": _field(measurement, "BOV", "bov"),
            "speed": _field(reading, "Speed", "speed"),
            "speed_units": _field(reading, "SpeedUnits", "speedUnits"),
            "process": _field(reading, "Process", "process"),
            "process_units": _field(reading, "ProcessUnits", "processUnits"),
            "digital": _field(reading, "Digital", "digital"),
            "number_of_channels": _field(reading, "NumberOfChannels", "numberOfChannels"),
        }
        alarm_info = _field(reading, "AlarmInfo", "alarmInfo")
        if alarm_info:
            record["alarm_info"] = alarm_info
        if include_raw:
            record["observer_phoenix_raw"] = {"reading": reading, "measurement": measurement}
        records.append(record)
    return records


def target_jsonl_dir(data_dir: Path, source_name: str) -> Path:
    return data_dir / "sources" / source_name / "jsonl"


def _existing_record_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    existing: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            source_record_id = payload.get("source_record_id")
            if source_record_id:
                existing.add(str(source_record_id))
    return existing


def append_unique_jsonl(records: Iterable[dict[str, Any]], *, data_dir: Path, source_name: str) -> tuple[int, dict[str, int]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        date_key = parse_utc(str(record["timestamp"])).date().isoformat()
        grouped[date_key].append(record)

    written = 0
    written_by_file: dict[str, int] = {}
    root = target_jsonl_dir(data_dir, source_name)
    root.mkdir(parents=True, exist_ok=True)
    for date_key, date_records in sorted(grouped.items()):
        path = root / f"{date_key}.jsonl"
        existing_ids = _existing_record_ids(path)
        new_records = [record for record in date_records if str(record.get("source_record_id")) not in existing_ids]
        if not new_records:
            written_by_file[path.as_posix()] = 0
            continue
        with path.open("a", encoding="utf-8") as handle:
            for record in new_records:
                handle.write(json.dumps(record, sort_keys=True, ensure_ascii=False) + "\n")
        written += len(new_records)
        written_by_file[path.as_posix()] = len(new_records)
    return written, written_by_file


def _selected_machines(client: ObserverPhoenixClient, machine_ids: list[str]) -> list[dict[str, Any]]:
    machines = client.list_machines()
    if not machine_ids:
        return machines
    requested = {str(machine_id) for machine_id in machine_ids}
    return [machine for machine in machines if _string_id(_field(machine, "ID", "id")) in requested]


def _iter_points(client: ObserverPhoenixClient, machine: dict[str, Any], point_ids: list[str]) -> list[dict[str, Any]]:
    if point_ids:
        requested = {str(point_id) for point_id in point_ids}
        points = client.list_machine_points(_field(machine, "ID", "id"))
        return [point for point in points if _string_id(_field(point, "ID", "id")) in requested]
    return client.list_machine_points(_field(machine, "ID", "id"))


def export_trends(client: ObserverPhoenixClient, args: argparse.Namespace) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    state = load_state(args.data_dir, SOURCE_NAME)
    end = parse_utc(args.to_utc) if args.to_utc else utc_now()
    if args.from_utc:
        start = parse_utc(args.from_utc)
    elif state.get_watermark(WATERMARK_NAME):
        start = subtract_overlap(state.get_watermark(WATERMARK_NAME) or "", args.overlap_minutes)
    else:
        start = end - timedelta(hours=max(1, args.lookback_hours))

    records: list[dict[str, Any]] = []
    point_count = 0
    reading_count = 0
    machines = _selected_machines(client, args.machine_id)
    if not machines and args.point_id:
        machines = [{"ID": "unknown", "Name": "unknown"}]

    for machine in machines:
        points = _iter_points(client, machine, args.point_id) if _field(machine, "ID", "id") != "unknown" else []
        if not points and args.point_id:
            points = [{"ID": point_id, "Name": f"point-{point_id}"} for point_id in args.point_id]
        for point in points:
            point_count += 1
            point_id = _field(point, "ID", "id")
            readings = client.trend_measurements(
                point_id,
                from_utc=start,
                to_utc=end,
                include_alarm_info=args.include_alarm_info,
                descending=False,
            )
            reading_count += len(readings)
            for reading in readings:
                records.extend(
                    normalize_trend_measurement(
                        source_name=SOURCE_NAME,
                        machine=machine,
                        point=point,
                        reading=reading,
                        include_raw=args.include_raw,
                    )
                )

    metadata = {
        "from_utc": format_utc(start),
        "to_utc": format_utc(end),
        "machines_scanned": len(machines),
        "points_scanned": point_count,
        "readings_received": reading_count,
        "records_normalized": len(records),
    }
    return records, metadata


def main() -> int:
    args = parse_args()
    client = build_client(args)
    records, metadata = export_trends(client, args)
    if args.dry_run:
        print(json.dumps({"dry_run": True, **metadata}, indent=2, sort_keys=True))
        return 0

    written, written_by_file = append_unique_jsonl(records, data_dir=args.data_dir, source_name=SOURCE_NAME)
    state = load_state(args.data_dir, SOURCE_NAME)
    state.set_watermark(WATERMARK_NAME, metadata["to_utc"])
    state.mark_success(
        source_name=SOURCE_NAME,
        written_records=written,
        written_by_file=written_by_file,
        **metadata,
    )
    state_path = save_state(args.data_dir, state)
    print(json.dumps({"written_records": written, "state_path": state_path.as_posix(), **metadata}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
