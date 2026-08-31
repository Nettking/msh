from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation.host_resources import FilesystemMeasurement, PressureThresholds
from catalog.federation.process_resource_admission import (
    PROCESS_RESOURCE_ADMISSION,
    SerializedProcessResourceAdmission,
)
from catalog.mtconnect_recorder import resource_pressure as pressure


def _measurement(resource_id: str, free_bytes: int) -> FilesystemMeasurement:
    return FilesystemMeasurement(
        resource_id=resource_id,
        observed_at=datetime.now(timezone.utc),
        total_bytes=1_000,
        free_bytes=free_bytes,
        total_inodes=None,
        free_inodes=None,
        available=True,
    )


def _thresholds() -> PressureThresholds:
    return PressureThresholds(
        critical_free_bytes=100,
        pressure_free_bytes=200,
        warning_free_bytes=300,
        critical_free_inodes=0,
        pressure_free_inodes=0,
        warning_free_inodes=0,
    )


def _runtime() -> SimpleNamespace:
    return SimpleNamespace(
        checkpoints={},
        sources={},
        probes={},
        store=SimpleNamespace(),
    )


def _requirement(path: str, bytes_required: int) -> SimpleNamespace:
    return SimpleNamespace(
        path=Path(path),
        bytes_required=bytes_required,
        inodes_required=0,
    )


def test_default_recorder_guard_shares_process_wide_controller() -> None:
    guard = pressure.RecorderResourceGuard(_runtime())

    assert guard.controller is PROCESS_RESOURCE_ADMISSION


def test_new_capture_admission_is_atomic_across_resources() -> None:
    def measure(path: Path | str) -> FilesystemMeasurement:
        if str(path) == "healthy":
            return _measurement("device:healthy", 1_000)
        return _measurement("device:critical", 100)

    controller = SerializedProcessResourceAdmission(
        thresholds=_thresholds(),
        measurer=measure,
    )
    guard = pressure.RecorderResourceGuard(_runtime(), controller=controller)

    with pytest.raises(pressure._impl._RecorderPauseSignal):
        guard._begin(
            (
                _requirement("healthy", 100),
                _requirement("critical", 1),
            ),
            completion=False,
        )

    assert controller._reserved == {}


def test_new_capture_coalesces_same_resource_in_one_transaction() -> None:
    def measure(_path: Path | str) -> FilesystemMeasurement:
        return _measurement("device:data", 350)

    controller = SerializedProcessResourceAdmission(
        thresholds=_thresholds(),
        measurer=measure,
    )
    guard = pressure.RecorderResourceGuard(_runtime(), controller=controller)

    guard._begin(
        (
            _requirement("data", 150),
            _requirement("state", 50),
        ),
        completion=False,
    )
    assert controller._reserved == {"device:data": (200, 0)}

    guard.end_transaction()
    assert controller._reserved == {}


def test_completion_may_start_at_pressure_but_preserves_critical_floor() -> None:
    def measure(_path: Path | str) -> FilesystemMeasurement:
        return _measurement("device:data", 180)

    controller = SerializedProcessResourceAdmission(
        thresholds=_thresholds(),
        measurer=measure,
    )
    guard = pressure.RecorderResourceGuard(_runtime(), controller=controller)

    guard._begin((_requirement("data", 50),), completion=True)
    assert controller._reserved == {"device:data": (50, 0)}
    guard.end_transaction()

    with pytest.raises(pressure._impl._RecorderPauseSignal):
        guard._begin((_requirement("data", 81),), completion=True)

    assert controller._reserved == {}
