"""Compact derived metrics shared by startup-safe health analyses.

The automatic runtime builds this once per session so several health scripts can
read timestamp/machine/sequence rows without repeatedly parsing full JSONL
payloads. It is a performance artifact, not a replacement for raw telemetry.
"""

from __future__ import annotations

import csv
import io
import os
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

from catalog.common.data_loading import iter_records_with_parsed_timestamps
from catalog.common.managed_temporary import (
    ManagedTemporaryFile,
    ManagedTemporaryRoot,
    scavenge_managed_temporary_root,
)

DERIVED_DIRNAME = "_derived"
BASIC_METRICS_FILENAME = "basic_metrics.csv"
MAX_BASIC_METRICS_BYTES = 64 * 1024 * 1024
MAX_BASIC_METRICS_INODES = 8
_BASIC_METRICS_TEMP_NAMESPACE = "analysis-basic-metrics"
_BASIC_METRICS_TEMP_TRAVERSAL_LIMIT = 256


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def basic_metrics_path(filtered_data_dir: Path) -> Path:
    """Return the conventional derived metrics path inside a filtered data dir."""
    return filtered_data_dir / DERIVED_DIRNAME / BASIC_METRICS_FILENAME


def build_basic_metrics_dataset(
    filtered_data_dir: Path,
    *,
    resource_admission=None,
    admission_held: bool = False,
) -> tuple[Path, int]:
    """Create the compact CSV consumed by startup-safe analyses.

    The CSV intentionally contains only ``timestamp``, ``machine``, and
    ``sequence``. Analyses requiring richer fields should read the session JSONL
    data directly rather than expanding this bootstrap artifact.
    """
    if not admission_held:
        from catalog.capabilities.analysis.resource_admission import (
            reserve_analysis_requirements,
        )
        from catalog.federation.process_resource_admission import (
            PROCESS_RESOURCE_ADMISSION,
        )

        with reserve_analysis_requirements(
            resource_admission or PROCESS_RESOURCE_ADMISSION,
            [(filtered_data_dir, 2 * MAX_BASIC_METRICS_BYTES, MAX_BASIC_METRICS_INODES)],
        ):
            return build_basic_metrics_dataset(
                filtered_data_dir,
                resource_admission=resource_admission,
                admission_held=True,
            )

    output_path = basic_metrics_path(filtered_data_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_root = ManagedTemporaryRoot(
        output_path.parent / ".fcp-basic-metrics-tmp",
        namespace=_BASIC_METRICS_TEMP_NAMESPACE,
    )
    scavenge_managed_temporary_root(
        temporary_root.root,
        namespace=_BASIC_METRICS_TEMP_NAMESPACE,
        max_entries=_BASIC_METRICS_TEMP_TRAVERSAL_LIMIT,
    )
    temporary: ManagedTemporaryFile | None = None

    written_rows = 0
    try:
        temporary = temporary_root.allocate(prefix="fcp-basic-metrics-", suffix=".partial")
        handle = io.TextIOWrapper(
            temporary.handle,
            encoding="utf-8",
            newline="",
            write_through=False,
        )
        try:
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
                if handle.tell() > MAX_BASIC_METRICS_BYTES:
                    raise ValueError("basic metrics output exceeded its bounded envelope")
                written_rows += 1
            handle.flush()
            os.fsync(handle.buffer.fileno())
        finally:
            if not handle.closed:
                handle.close()
        temporary.prepare_for_replace()
        temporary.path.replace(output_path)
        _fsync_directory(output_path.parent)

    except BaseException:
        if temporary is not None:
            temporary.close()
        raise
    finally:
        if temporary is not None:
            temporary.close()
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
