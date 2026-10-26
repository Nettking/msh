from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from catalog.flask_app.services.digital_twin_service import (
    DigitalTwinDataError,
    DigitalTwinInputError,
    DigitalTwinService,
    _classify_rows,
)


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )


def test_execution_state_precedes_signal_fallback_and_unknown_is_not_idle() -> None:
    frame = pd.DataFrame(
        [
            {"execution": "ACTIVE", "Srpm": 0, "Sload": 0},
            {"execution": "UNAVAILABLE", "Srpm": 101, "Sload": 0},
            {"execution": "UNAVAILABLE", "Srpm": 0, "Sload": 0},
            {"execution": "OTHER", "Srpm": 800, "Sload": 8},
            {"execution": "UNAVAILABLE", "Srpm": None, "Sload": None},
        ]
    )

    classified = _classify_rows(frame)

    assert classified["state_class"].tolist()[:3] == ["active", "active", "non_active"]
    assert classified["state_label"].tolist()[:3] == [
        "ACTIVE",
        "ACTIVE (signals inferred)",
        "NON-ACTIVE (signals inferred)",
    ]
    assert classified["state_class"].isna().tolist() == [
        False,
        False,
        False,
        True,
        True,
    ]


def test_date_filter_splits_midnight_drops_subsecond_and_long_gaps_and_simulates(
    tmp_path: Path,
) -> None:
    source = tmp_path / "data" / "nested" / "samples.jsonl"
    records = [
        {"timestamp": "2025-01-01T23:59:58Z", "machine": "Mill", "execution": "ACTIVE"},
        {"timestamp": "2025-01-02T00:00:03Z", "machine": "Mill", "execution": "ACTIVE"},
        {
            "timestamp": "2025-01-02T00:00:03Z",
            "machine": "Mill",
            "execution": "STOPPED",
        },
        {
            "timestamp": "2025-01-02T00:00:03.050Z",
            "machine": "Mill",
            "execution": "STOPPED",
        },
        {"timestamp": "2025-01-02T00:00:04Z", "machine": "Mill", "execution": "READY"},
        {"timestamp": "2025-01-02T00:00:35Z", "machine": "Mill", "execution": "ACTIVE"},
    ]
    _write_jsonl(source, records)
    service = DigitalTwinService(tmp_path / "data")

    jan_1 = service.analyze(
        machine_id="Mill", start_date="2025-01-01", end_date="2025-01-01"
    )
    jan_2 = service.analyze(
        machine_id="Mill",
        start_date="2025-01-02",
        end_date="2025-01-02",
        recovery_percent=50,
    )

    assert jan_1["active_seconds"] == pytest.approx(2)
    assert jan_1["non_active_seconds"] == pytest.approx(0)
    assert jan_2["active_seconds"] == pytest.approx(3)
    assert jan_2["non_active_seconds"] == pytest.approx(0.95)
    assert jan_2["positive_intervals"] == 3
    assert jan_2["usable_intervals"] == 1
    assert jan_2["classified_intervals"] == 1
    assert jan_2["long_gaps"] == 1
    assert jan_2["continuity"] == pytest.approx(1 / 3)
    assert jan_2["sufficient_data"] is False
    assert jan_2["simulated_active_share"] is None


def test_scenario_formula_requires_thirty_classified_intervals_and_is_bounded(
    tmp_path: Path,
) -> None:
    source = tmp_path / "data" / "machine.jsonl"
    first = datetime(2025, 5, 1, tzinfo=timezone.utc)
    records = [
        {
            "timestamp": (first + timedelta(seconds=index))
            .isoformat()
            .replace("+00:00", "Z"),
            "machine_id": "VTC",
            "execution": "ACTIVE" if index % 2 == 0 else "STOPPED",
        }
        for index in range(31)
    ]
    _write_jsonl(source, records)
    service = DigitalTwinService(tmp_path / "data")

    scenario = service.analyze(machine_id="VTC", recovery_percent=50)
    baseline = service.analyze(machine_id="VTC", recovery_percent=0)

    assert scenario["classified_intervals"] == 30
    assert scenario["sufficient_data"] is True
    assert scenario["observed_seconds"] == pytest.approx(30)
    assert scenario["active_seconds"] == pytest.approx(15)
    assert scenario["non_active_seconds"] == pytest.approx(15)
    assert scenario["recovered_seconds"] == pytest.approx(7.5)
    assert scenario["simulated_active_seconds"] == pytest.approx(22.5)
    assert scenario["observed_active_share"] == pytest.approx(0.5)
    assert scenario["simulated_active_share"] == pytest.approx(0.75)
    assert scenario["active_share_change_pp"] == pytest.approx(25)
    assert baseline["simulated_active_seconds"] == pytest.approx(
        baseline["active_seconds"]
    )

    for invalid in (-5, 5.5, 55, float("nan"), "not-a-number"):
        with pytest.raises(DigitalTwinInputError, match="0 and 50"):
            service.analyze(machine_id="VTC", recovery_percent=invalid)


def test_recursive_discovery_accepts_supported_machine_fields_and_hides_incomplete_uploads(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    first = datetime(2025, 6, 1, tzinfo=timezone.utc)
    source_rows = []
    for machine_field, machine_id in (
        ("machine_id", "Mill"),
        ("machine", "Lathe"),
        ("resource", "Grinder"),
    ):
        for index in range(2):
            source_rows.append(
                {
                    "timestamp": (first + timedelta(seconds=index))
                    .isoformat()
                    .replace("+00:00", "Z"),
                    machine_field: machine_id,
                    "execution": "ACTIVE",
                }
            )
    _write_jsonl(data_dir / "sources" / "batch.jsonl", source_rows)
    incomplete_dir = data_dir / "uploads" / "upload-pending"
    incomplete_dir.mkdir(parents=True)
    (incomplete_dir / ".fcp-importing").write_text("in progress", encoding="utf-8")
    _write_jsonl(
        incomplete_dir / "hidden.jsonl",
        [
            {
                "timestamp": "2025-06-01T00:00:00Z",
                "machine": "Hidden",
                "execution": "ACTIVE",
            },
            {
                "timestamp": "2025-06-01T00:00:01Z",
                "machine": "Hidden",
                "execution": "ACTIVE",
            },
        ],
    )

    model = DigitalTwinService(data_dir).analyze()

    assert {machine["machine_id"] for machine in model["machines"]} == {
        "Mill",
        "Lathe",
        "Grinder",
    }
    assert model["source_file_count"] == 1


def test_invalid_machine_date_and_reversed_range_are_rejected(tmp_path: Path) -> None:
    source = tmp_path / "data" / "samples.jsonl"
    _write_jsonl(
        source,
        [
            {
                "timestamp": "2025-01-01T00:00:00Z",
                "machine": "Mill",
                "execution": "ACTIVE",
            },
            {
                "timestamp": "2025-01-01T00:00:01Z",
                "machine": "Mill",
                "execution": "ACTIVE",
            },
        ],
    )
    service = DigitalTwinService(tmp_path / "data")

    with pytest.raises(DigitalTwinInputError, match="selected machine"):
        service.analyze(machine_id="Unknown")
    with pytest.raises(DigitalTwinInputError, match="Start date"):
        service.analyze(machine_id="Mill", start_date="not-a-date")
    with pytest.raises(DigitalTwinInputError, match="Start date"):
        service.analyze(machine_id="Mill", start_date="2025-1-1")
    with pytest.raises(DigitalTwinInputError, match="on or before"):
        service.analyze(
            machine_id="Mill", start_date="2025-01-02", end_date="2025-01-01"
        )


def test_source_size_limit_fails_with_a_safe_data_error(
    tmp_path: Path, monkeypatch
) -> None:
    from catalog.flask_app.services import digital_twin_service

    source = tmp_path / "data" / "samples.jsonl"
    _write_jsonl(
        source,
        [
            {
                "timestamp": "2025-01-01T00:00:00Z",
                "machine": "Mill",
                "execution": "ACTIVE",
            }
        ],
    )
    monkeypatch.setattr(digital_twin_service, "MAX_SOURCE_BYTES", 1)

    with pytest.raises(DigitalTwinDataError, match="2 GiB v1 analysis limit"):
        DigitalTwinService(tmp_path / "data").analyze()
