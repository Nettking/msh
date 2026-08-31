from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.common import telemetry_cache
from catalog.common.telemetry_cache import rebuild_cache
from catalog.federation.host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureThresholds,
)
from catalog.federation.process_resource_admission import SerializedProcessResourceAdmission
from catalog.observer_phoenix import export_jsonl


NOW = datetime(2026, 8, 31, tzinfo=timezone.utc)


def _admission(free_bytes: int) -> SerializedProcessResourceAdmission:
    return SerializedProcessResourceAdmission(
        thresholds=PressureThresholds(
            critical_free_bytes=100,
            pressure_free_bytes=200,
            warning_free_bytes=300,
            critical_free_inodes=0,
            pressure_free_inodes=0,
            warning_free_inodes=0,
            max_measurement_age_seconds=60,
        ),
        measurer=lambda _path: FilesystemMeasurement(
            resource_id="observer-resource",
            observed_at=NOW,
            total_bytes=10_000_000,
            free_bytes=free_bytes,
            total_inodes=None,
            free_inodes=None,
            available=True,
        ),
        clock=lambda: NOW,
    )


def _record(record_id: str = "observer:record:1") -> dict[str, object]:
    return {
        "timestamp": "2026-08-31T10:00:00Z",
        "machine_id": "M1",
        "source_record_id": record_id,
        "value": 42,
    }


def test_observer_jsonl_refusal_happens_before_root_creation(tmp_path: Path) -> None:
    with pytest.raises(HostResourceRefused):
        export_jsonl.append_unique_jsonl(
            [_record()],
            data_dir=tmp_path / "data",
            source_name="observer_phoenix",
            resource_admission=_admission(200),
        )

    assert not (tmp_path / "data" / "sources").exists()


def test_observer_jsonl_replacement_failure_preserves_old_file_and_unwinds(
    tmp_path: Path, monkeypatch
) -> None:
    data_dir = tmp_path / "data"
    path = export_jsonl.target_jsonl_dir(data_dir, "observer_phoenix") / "2026-08-31.jsonl"
    path.parent.mkdir(parents=True)
    original = b'{"old":true}\n'
    path.write_bytes(original)

    def fail_replace(*_args, **_kwargs):
        raise OSError("injected observer publication failure")

    monkeypatch.setattr(export_jsonl.os, "replace", fail_replace)
    with pytest.raises(OSError, match="injected observer publication failure"):
        export_jsonl.append_unique_jsonl(
            [_record()],
            data_dir=data_dir,
            source_name="observer_phoenix",
            resource_admission=_admission(1_000_000_000),
        )

    assert path.read_bytes() == original
    assert list(path.parent.glob("*.partial")) == []


def test_telemetry_cache_refusal_leaves_no_rebuild_directory(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source = data_dir / "telemetry.jsonl"
    source.parent.mkdir(parents=True)
    source.write_text(json.dumps(_record()) + "\n", encoding="utf-8")

    with pytest.raises(HostResourceRefused):
        rebuild_cache(
            data_dir,
            resource_admission=_admission(200),
        )

    assert not (data_dir / "cache").exists()


def test_telemetry_cache_failure_preserves_old_cache_and_unwinds(
    tmp_path: Path, monkeypatch
) -> None:
    data_dir = tmp_path / "data"
    source = data_dir / "telemetry.jsonl"
    source.parent.mkdir(parents=True)
    source.write_text(json.dumps(_record()) + "\n", encoding="utf-8")
    output = data_dir / "cache" / "parquet"
    output.mkdir(parents=True)
    marker = output / "old.marker"
    marker.write_text("old-cache", encoding="utf-8")

    def fail_writer(*_args, **_kwargs):
        raise OSError("injected parquet build failure")

    monkeypatch.setattr(telemetry_cache, "_write_partitioned_parquet", fail_writer)
    with pytest.raises(OSError, match="injected parquet build failure"):
        rebuild_cache(
            data_dir,
            cache_dir=output,
            resource_admission=_admission(1_000_000_000),
        )

    assert marker.read_text(encoding="utf-8") == "old-cache"
    assert list(output.parent.glob(".parquet-*")) == []
