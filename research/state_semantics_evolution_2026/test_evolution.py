"""Evolution experiments against the actual FCP DigitalTwinService.

Run: python -m pytest -q research/state_semantics_evolution_2026/test_evolution.py

Only synthetic JSONL is generated; no production telemetry, provider or device is modified.
These are semantic regression experiments, not labels of machining stability.
"""
from __future__ import annotations

import json
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from catalog.flask_app.services.digital_twin_service import (
    DigitalTwinService,
    _classify_rows,
)

MACHINE = "EVOLUTION_TEST"


def _records() -> list[dict]:
    start = datetime(2026, 1, 15, 12, tzinfo=timezone.utc)
    rows = []
    for i in range(51):
        if i < 10:
            execution, rpm, load = "ACTIVE", 200, 2
        elif i < 20:
            execution, rpm, load = "STOPPED", (400 if i == 10 else 0), (2 if i == 10 else 0)
        elif i < 30:
            execution = "UNAVAILABLE"
            rpm = 100 if i == 20 else (100.1 if i == 21 else 130 if i % 3 else 0)
            load = 1
        elif i < 35:
            execution, rpm, load = "OTHER", 800, 8
        elif i < 40:
            execution, rpm, load = "UNAVAILABLE", None, None
        else:
            execution, rpm, load = "EXECUTING", None, None
        rows.append(
            {
                "timestamp": (start + timedelta(seconds=i)).isoformat().replace("+00:00", "Z"),
                "machine_id": MACHINE,
                "execution": execution,
                "Srpm": rpm,
                "Sload": load,
            }
        )
    return rows


def _write_model(tmp_path: Path, name: str, records: list[dict]) -> dict:
    root = tmp_path / name
    root.mkdir()
    with (root / "trace.jsonl").open("w", encoding="utf-8") as stream:
        for record in records:
            stream.write(json.dumps(record, separators=(",", ":")) + "\n")
    return DigitalTwinService(root).analyze(machine_id=MACHINE, recovery_percent=0)


def _row_states(records: list[dict]) -> list[str]:
    frame = pd.DataFrame.from_records(records)
    classified = _classify_rows(frame)
    return classified["state_class"].fillna("unknown").tolist()


def _assert_metrics_equal(a: dict, b: dict) -> None:
    for field in (
        "active_seconds",
        "non_active_seconds",
        "observed_seconds",
        "positive_intervals",
        "usable_intervals",
        "classified_intervals",
        "long_gaps",
    ):
        assert b[field] == pytest.approx(a[field]), field
    assert b["continuity"] == pytest.approx(a["continuity"])


def _to_provider_representation(records: list[dict]) -> list[dict]:
    return [
        {
            "timestamp": row["timestamp"],
            "resource": row["machine_id"],
            "spindle_rpm": row["Srpm"],
            "spindle_load": row["Sload"],
            "execution_state": row["execution"],
        }
        for row in records
    ]


def _canonicalize_provider(records: list[dict]) -> list[dict]:
    return [
        {
            "timestamp": row["timestamp"],
            "machine_id": row["resource"],
            "Srpm": row["spindle_rpm"],
            "Sload": row["spindle_load"],
            "execution": row["execution_state"],
        }
        for row in records
    ]


def test_e1_renaming_execution_preserves_row_and_time_semantics(tmp_path: Path):
    original = _records()
    renamed = [{("machine_execution" if k == "execution" else k): v
                for k, v in row.items()} for row in original]
    adapted = [{**{k: v for k, v in row.items() if k != "machine_execution"},
                "execution": row["machine_execution"]} for row in renamed]

    assert _row_states(adapted) == _row_states(original)
    _assert_metrics_equal(
        _write_model(tmp_path, "e1-original", original),
        _write_model(tmp_path, "e1-adapted", adapted),
    )


def test_e2_rpm_to_radians_per_second_with_inverse_adapter_preserves_meaning(tmp_path: Path):
    original = _records()
    rad_s = [{**r, "Srpm": None if r["Srpm"] is None else
              r["Srpm"] * (2 * math.pi / 60)} for r in original]
    restored = [{**r, "Srpm": None if r["Srpm"] is None else
                 r["Srpm"] * (60 / (2 * math.pi))} for r in rad_s]

    assert _row_states(restored) == _row_states(original)
    _assert_metrics_equal(
        _write_model(tmp_path, "e2-original", original),
        _write_model(tmp_path, "e2-restored", restored),
    )


def test_e3_equivalent_provider_adapter_preserves_semantics(tmp_path: Path):
    original = _records()
    provider = _to_provider_representation(original)
    adapted = _canonicalize_provider(provider)
    assert _row_states(adapted) == _row_states(original)
    _assert_metrics_equal(
        _write_model(tmp_path, "e3-original", original),
        _write_model(tmp_path, "e3-provider", adapted),
    )


def test_e4_missing_execution_changes_only_affected_rows_and_coverage(tmp_path: Path):
    original = _records()
    degraded = [dict(r) for r in original]
    for i in (10, 11, 20, 40):
        degraded[i]["execution"] = None
    expected_affected = {10, 11, 20, 40}

    original_states = _row_states(original)
    changed_states = _row_states(degraded)
    assert original_states[10] == "non_active" and changed_states[10] == "active"
    assert original_states[11] == changed_states[11] == "non_active"
    assert original_states[20] == changed_states[20] == "non_active"
    assert original_states[40] == "active" and changed_states[40] == "unknown"
    assert all(original_states[i] == changed_states[i]
               for i in range(len(original)) if i not in expected_affected)

    a = _write_model(tmp_path, "e4-original", original)
    b = _write_model(tmp_path, "e4-degraded", degraded)
    assert b["active_seconds"] == pytest.approx(a["active_seconds"])  # +1 active, -1 unknown
    assert b["non_active_seconds"] == pytest.approx(a["non_active_seconds"] - 1)
    assert b["observed_seconds"] == pytest.approx(a["observed_seconds"] - 1)
    assert b["long_gaps"] == a["long_gaps"]


def test_e5_reorder_and_identical_duplicate_preserve_time_accounting(tmp_path: Path):
    original = _records()
    shuffled = [dict(r) for r in original]
    shuffled.append(dict(original[8]))
    random.Random(2026).shuffle(shuffled)
    _assert_metrics_equal(
        _write_model(tmp_path, "e5-original", original),
        _write_model(tmp_path, "e5-shuffled", shuffled),
    )


def test_e5_long_gap_is_excluded_not_interpreted_as_downtime(tmp_path: Path):
    original = _records()
    altered = [dict(r) for r in original]
    for row in altered[25:]:
        dt = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
        row["timestamp"] = (dt + timedelta(seconds=60)).isoformat().replace("+00:00", "Z")

    a = _write_model(tmp_path, "e5-gap-original", original)
    b = _write_model(tmp_path, "e5-gap-altered", altered)
    assert b["long_gaps"] == a["long_gaps"] + 1
    assert b["usable_intervals"] == a["usable_intervals"] - 1
    assert b["observed_seconds"] == pytest.approx(a["observed_seconds"] - 1)
    assert b["non_active_seconds"] <= a["non_active_seconds"]


def test_e6_deliberate_threshold_change_is_reported_as_semantic_change():
    original = _records()
    baseline = _row_states(original)
    evolved = list(baseline)

    # A controlled threshold mutation, *not* the production classifier:
    # from Srpm > 100 to Srpm > 150, with Sload <= 1.
    affected = []
    for i, r in enumerate(original):
        if (r["execution"] == "UNAVAILABLE" and r["Srpm"] is not None
                and 100 < r["Srpm"] <= 150 and (r["Sload"] is None or r["Sload"] <= 1)):
            evolved[i] = "non_active"
            affected.append(i)

    assert affected
    assert all(baseline[i] == "active" and evolved[i] == "non_active"
               for i in affected)
    assert all(baseline[i] == evolved[i]
               for i in range(len(original)) if i not in affected)


def test_equivalence_check_detects_broken_unit_adapter():
    original = _records()
    # Deliberate defect: consuming rad/s numerically as though it were rpm.
    bad = [{**r, "Srpm": None if r["Srpm"] is None else
            r["Srpm"] * (2 * math.pi / 60)} for r in original]
    first = _row_states(original)
    altered = _row_states(bad)
    assert first != altered
    assert any(first[i] == "active" and altered[i] == "non_active"
               for i in range(len(original)))


def test_unknown_explicit_execution_does_not_silently_fall_back():
    rows = _records()
    classified = _row_states(rows)
    assert all(classified[i] == "unknown" for i in range(30, 40))
