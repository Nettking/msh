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

load_playback_frame = _PLAYBACK_MOD.load_playback_frame
playback_context = _PLAYBACK_MOD.playback_context
playback_days_by_machine = _PLAYBACK_MOD.playback_days_by_machine
prepare_playback_frame = _PLAYBACK_MOD.prepare_playback_frame
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
