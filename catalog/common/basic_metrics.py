from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Iterator

from catalog.common.data_loading import iter_records_with_parsed_timestamps

DERIVED_DIRNAME = "_derived"
BASIC_METRICS_FILENAME = "basic_metrics.csv"


def basic_metrics_path(filtered_data_dir: Path) -> Path:
    return filtered_data_dir / DERIVED_DIRNAME / BASIC_METRICS_FILENAME


def build_basic_metrics_dataset(filtered_data_dir: Path) -> tuple[Path, int]:
    """Create a compact CSV used by startup-safe analyses.

    The CSV contains only: timestamp, machine, sequence.
    """
    output_path = basic_metrics_path(filtered_data_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    written_rows = 0
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "machine", "sequence"])

        for _, record in iter_records_with_parsed_timestamps(
            filtered_data_dir,
            recursive=True,
            allow_z_suffix=True,
        ):
            timestamp = record.get("timestamp")
            if timestamp is None:
                continue
            machine = record.get("machine")
            sequence = record.get("sequence")
            writer.writerow([
                timestamp.isoformat(),
                "" if machine is None else str(machine),
                "" if sequence is None else str(sequence),
            ])
            written_rows += 1

    return output_path, written_rows


def iter_basic_metrics_rows(filtered_data_dir: Path) -> Iterator[tuple[datetime, str | None, int | None]]:
    """Iterate compact metric rows from the derived CSV."""
    source = basic_metrics_path(filtered_data_dir)
    with source.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            raw_timestamp = (row.get("timestamp") or "").strip()
            if not raw_timestamp:
                continue
            try:
                timestamp = datetime.fromisoformat(raw_timestamp)
            except ValueError:
                continue
            machine = (row.get("machine") or "").strip() or None
            raw_sequence = (row.get("sequence") or "").strip()
            try:
                sequence = int(raw_sequence) if raw_sequence else None
            except ValueError:
                sequence = None
            yield timestamp, machine, sequence
