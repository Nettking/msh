from __future__ import annotations

import json
import importlib.util
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(".").resolve()))

_PLAYBACK_MODULE_PATH = Path("catalog/flask_app/services/playback_service.py")
_PLAYBACK_SPEC = importlib.util.spec_from_file_location("playback_service", _PLAYBACK_MODULE_PATH)
assert _PLAYBACK_SPEC and _PLAYBACK_SPEC.loader
_PLAYBACK_MOD = importlib.util.module_from_spec(_PLAYBACK_SPEC)
sys.modules[_PLAYBACK_SPEC.name] = _PLAYBACK_MOD
_PLAYBACK_SPEC.loader.exec_module(_PLAYBACK_MOD)

_ARTIFACT_MODULE_PATH = Path("catalog/common/artifact_registry.py")
_ARTIFACT_SPEC = importlib.util.spec_from_file_location("artifact_registry", _ARTIFACT_MODULE_PATH)
assert _ARTIFACT_SPEC and _ARTIFACT_SPEC.loader
_ARTIFACT_MOD = importlib.util.module_from_spec(_ARTIFACT_SPEC)
sys.modules[_ARTIFACT_SPEC.name] = _ARTIFACT_MOD
_ARTIFACT_SPEC.loader.exec_module(_ARTIFACT_MOD)

default_live_signal_columns = _PLAYBACK_MOD.default_live_signal_columns
load_playback_frame = _PLAYBACK_MOD.load_playback_frame
playback_context = _PLAYBACK_MOD.playback_context
playback_day_counts_by_machine = _PLAYBACK_MOD.playback_day_counts_by_machine
playback_days_by_machine = _PLAYBACK_MOD.playback_days_by_machine
playback_field_groups = _PLAYBACK_MOD.playback_field_groups
prepare_playback_frame = _PLAYBACK_MOD.prepare_playback_frame
resample_playback_timeline = _PLAYBACK_MOD.resample_playback_timeline
validate_playback_frame = _PLAYBACK_MOD.validate_playback_frame
validate_playback_source = _PLAYBACK_MOD.validate_playback_source
scan_artifacts = _ARTIFACT_MOD.scan_artifacts


def test_valid_timeline_csv_is_playback_valid(tmp_path: Path) -> None:
    path = tmp_path / "timeline_rows.csv"
    pd.DataFrame(
        {
            "timestamp": ["2026-03-01T10:00:00Z", "2026-03-01T11:00:00Z"],
            "machine_id": ["M1", "M1"],
            "state": ["run", "idle"],
        }
    ).to_csv(path, index=False)

    source_validation = validate_playback_source(str(path))
    frame, error = load_playback_frame(str(path))

    assert source_validation.is_valid is True
    assert error is None
    assert frame is not None
    assert validate_playback_frame(frame).is_valid is True


def test_empty_timeline_csv_with_required_columns_has_no_playable_rows(tmp_path: Path) -> None:
    path = tmp_path / "timeline_rows.csv"
    pd.DataFrame(columns=["timestamp", "machine_id", "state"]).to_csv(path, index=False)

    frame, error = load_playback_frame(str(path))

    assert error is None
    assert frame is not None
    assert validate_playback_source(str(path)).is_valid is True
    assert validate_playback_frame(frame).is_valid is False
    assert prepare_playback_frame(frame).empty is True


def test_scalar_json_manifest_is_not_indexed_as_playback_and_does_not_crash(tmp_path: Path) -> None:
    path = tmp_path / "timeline_manifest.json"
    path.write_text(json.dumps({"name": "manifest", "version": 1}), encoding="utf-8")

    source_validation = validate_playback_source(str(path))
    artifacts, warnings = scan_artifacts([str(tmp_path)])

    assert source_validation.is_valid is False
    assert "Unable to inspect source columns" in source_validation.reason
    assert warnings == []
    assert len(artifacts) == 1
    assert artifacts[0]["playback_compatible"] is False


def test_machine_day_detection_returns_only_days_with_rows() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": [
                "2026-03-01T10:00:00Z",
                "2026-03-02T10:00:00Z",
                "2026-03-03T09:00:00Z",
            ],
            "machine_id": ["M1", "M1", "M2"],
            "state": ["run", "idle", "run"],
        }
    )

    prepared = prepare_playback_frame(frame)
    machine_days = playback_days_by_machine(prepared)
    context = playback_context(prepared)

    assert machine_days == {"M1": ["2026-03-01", "2026-03-02"], "M2": ["2026-03-03"]}
    assert context["machines"] == ["M1", "M2"]


def test_invalid_timestamp_rows_are_ignored() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": ["bad", "2026-03-01T10:00:00Z", ""],
            "machine_id": ["M1", "M1", "M1"],
            "state": ["run", "idle", "run"],
        }
    )

    prepared = prepare_playback_frame(frame)
    machine_days = playback_days_by_machine(prepared)

    assert len(prepared) == 1
    assert prepared.iloc[0]["day"] == "2026-03-01"
    assert machine_days == {"M1": ["2026-03-01"]}


def test_playback_field_groups_are_deterministically_ordered() -> None:
    grouped = playback_field_groups(
        [
            "event_score",
            "Sload",
            "state",
            "intervention_candidate",
            "custom_field",
            "Srpm",
            "execution",
        ]
    )

    assert list(grouped.keys()) == ["Signals", "State/context", "Detection/diagnostics", "Other fields"]
    assert grouped["Signals"][:2] == ["Srpm", "Sload"]
    assert grouped["State/context"] == ["execution", "state"]
    assert grouped["Detection/diagnostics"] == ["event_score", "intervention_candidate"]
    assert grouped["Other fields"] == ["custom_field"]


def test_day_counts_by_machine_uses_only_playable_rows() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": ["bad", "2026-03-01T10:00:00Z", "2026-03-01T11:00:00Z", "2026-03-02T08:00:00Z"],
            "machine_id": ["M1", "M1", "M1", "M2"],
            "state": ["run", "idle", "run", "run"],
        }
    )

    day_counts = playback_day_counts_by_machine(frame)

    assert day_counts == {
        "M1": {"2026-03-01": 2},
        "M2": {"2026-03-02": 1},
    }


def test_default_live_signal_columns_only_returns_numeric_core_signals() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": ["2026-03-01T10:00:00Z", "2026-03-01T10:00:01Z"],
            "machine_id": ["M1", "M1"],
            "state": ["run", "run"],
            "Srpm": ["1200", "1210"],
            "Sload": [10.5, 11.0],
            "Sovr": ["-", "-"],
            "Fovr": [None, None],
            "Frapidovr": ["95", "97"],
            "execution": ["AUTO", "AUTO"],
            "intervention_candidate": [False, True],
        }
    )

    assert default_live_signal_columns(frame) == ["Srpm", "Sload", "Frapidovr"]


def test_resample_playback_timeline_adds_200ms_ticks_and_marks_synthetic_rows() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": [
                "2026-03-01T10:00:00.000Z",
                "2026-03-01T10:00:00.400Z",
            ],
            "machine_id": ["M1", "M1"],
            "state": ["run", "idle"],
            "execution": ["AUTO", "MDI"],
            "Srpm": [1200, 1300],
        }
    )

    resampled = resample_playback_timeline(frame)

    assert resampled["timestamp"].dt.strftime("%H:%M:%S.%f").str[:-3].tolist() == [
        "10:00:00.000",
        "10:00:00.200",
        "10:00:00.400",
    ]
    assert resampled["is_synthetic_tick"].tolist() == [False, True, False]
    assert resampled["source_timestamp"].dt.strftime("%H:%M:%S.%f").str[:-3].tolist() == [
        "10:00:00.000",
        "10:00:00.000",
        "10:00:00.400",
    ]
    assert resampled["state"].tolist() == ["run", "run", "idle"]
    assert resampled["execution"].tolist() == ["AUTO", "AUTO", "MDI"]


def test_playback_subset_returns_resampled_timeline() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": [
                "2026-03-01T10:00:00.000Z",
                "2026-03-01T10:00:01.000Z",
            ],
            "machine_id": ["M1", "M1"],
            "state": ["run", "run"],
        }
    )

    subset = _PLAYBACK_MOD.playback_subset(frame, machine_id="M1", day="2026-03-01")

    assert len(subset) == 6
    assert subset["is_synthetic_tick"].sum() == 4


def test_resample_playback_timeline_does_not_pad_before_first_or_after_last_sample() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": [
                "2026-03-01T10:00:00.050Z",
                "2026-03-01T10:00:00.450Z",
            ],
            "machine_id": ["M1", "M1"],
            "state": ["run", "idle"],
        }
    )

    resampled = resample_playback_timeline(frame)

    assert resampled["timestamp"].dt.strftime("%H:%M:%S.%f").str[:-3].tolist() == [
        "10:00:00.050",
        "10:00:00.250",
        "10:00:00.450",
    ]
    assert resampled.iloc[0]["source_timestamp"] == pd.Timestamp("2026-03-01T10:00:00.050Z")
    assert resampled.iloc[-1]["source_timestamp"] == pd.Timestamp("2026-03-01T10:00:00.450Z")
    assert bool(resampled.iloc[0]["is_synthetic_tick"]) is False
    assert bool(resampled.iloc[-1]["is_synthetic_tick"]) is False
