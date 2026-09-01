from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.common import basic_metrics
from catalog.federation.host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureThresholds,
)
from catalog.federation.process_resource_admission import (
    SerializedProcessResourceAdmission,
)
from catalog.runner import data_filtering, playback

NOW = datetime(2026, 8, 31, tzinfo=timezone.utc)
THRESHOLDS = PressureThresholds(
    critical_free_bytes=100,
    pressure_free_bytes=200,
    warning_free_bytes=300,
    critical_free_inodes=0,
    pressure_free_inodes=0,
    warning_free_inodes=0,
    max_measurement_age_seconds=60,
)


def _controller(free_bytes: int) -> SerializedProcessResourceAdmission:
    return SerializedProcessResourceAdmission(
        thresholds=THRESHOLDS,
        measurer=lambda _path: FilesystemMeasurement(
            resource_id="analysis-resource",
            observed_at=NOW,
            total_bytes=free_bytes + 10_000,
            free_bytes=free_bytes,
            total_inodes=1_000_000,
            free_inodes=100_000,
            available=True,
        ),
        clock=lambda: NOW,
    )


def _metadata() -> dict:
    return {
        "paths": {"filtered_data_dir": "data"},
        "filter": {"start_date": "2026-08-31", "end_date": "2026-08-31"},
        "filter_result": {},
        "scripts": {},
        "runtime": {},
    }


def test_basic_metrics_refuses_before_creating_derived_directory(tmp_path: Path) -> None:
    filtered = tmp_path / "filtered"
    controller = _controller(150)

    with pytest.raises(HostResourceRefused):
        basic_metrics.build_basic_metrics_dataset(
            filtered,
            resource_admission=controller,
        )

    assert not filtered.exists()
    assert controller.assessment(filtered).reserved_bytes == 0


def test_filter_refuses_before_destination_or_index_write(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "2026-08-31.jsonl").write_text(
        '{"timestamp":"2026-08-31T00:00:00Z"}\n',
        encoding="utf-8",
    )
    destination = tmp_path / "session" / "data"
    monkeypatch.setattr(data_filtering, "DATA_INDEX_FILE", tmp_path / "index.json")
    required = data_filtering.MAX_FILTERED_DATA_BYTES + (2 * 1024 * 1024)
    controller = _controller(required + 99)

    with pytest.raises(HostResourceRefused):
        data_filtering.filter_data_by_date_range(
            source,
            destination,
            start_date="2026-08-31",
            end_date="2026-08-31",
            resource_admission=controller,
        )

    assert not destination.exists()
    assert not (tmp_path / "index.json").exists()
    assert controller.assessment(destination).reserved_bytes == 0


def test_playback_refuses_before_export_tree_creation(tmp_path: Path) -> None:
    session = tmp_path / "session"
    filtered = session / "data"
    filtered.mkdir(parents=True)
    (filtered / "2026-08-31.jsonl").write_text(
        '{"timestamp":"2026-08-31T00:00:00Z","machine":"m"}\n',
        encoding="utf-8",
    )
    metadata = _metadata()
    controller = _controller(playback.MAX_PLAYBACK_EXPORT_BYTES + 99)

    with pytest.raises(HostResourceRefused):
        playback.prepare_session_playback_exports(
            session,
            metadata,
            resource_admission=controller,
        )

    assert not (session / "playback").exists()
    assert controller.assessment(session).reserved_bytes == 0


def test_metadata_reservation_unwinds_after_json_serialization_failure(tmp_path: Path) -> None:
    session = tmp_path / "session"
    session.mkdir()
    controller = _controller(10_000_000_000)
    metadata = {"not-json": {"bad": object()}}

    with pytest.raises(TypeError):
        from catalog.runner.session_store import write_session_metadata

        write_session_metadata(
            session,
            metadata,
            resource_admission=controller,
        )

    assert tuple(session.iterdir()) == ()
    assert controller.assessment(session).reserved_bytes == 0


def test_session_filter_uses_one_enclosing_reservation_for_nested_writers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "2026-08-31.jsonl").write_text(
        '{"timestamp":"2026-08-31T00:00:00Z","machine":"m"}\n',
        encoding="utf-8",
    )
    session = tmp_path / "session"
    session.mkdir()
    monkeypatch.setattr(data_filtering, "DATA_INDEX_FILE", tmp_path / "index.json")
    outer_bytes = (
        data_filtering.MAX_FILTERED_DATA_BYTES
        + (2 * 1024 * 1024)
        + data_filtering.MAX_DATA_INDEX_BYTES
    )
    controller = _controller(outer_bytes + 101)

    result = data_filtering.ensure_session_filtered_data(
        source_data_dir=source,
        session_dir=session,
        metadata=_metadata(),
        resource_admission=controller,
    )

    assert result[2] == "created"
    assert controller.assessment(session).reserved_bytes == 0
