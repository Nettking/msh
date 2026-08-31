from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation.host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureThresholds,
)
from catalog.federation.process_resource_admission import (
    SerializedProcessResourceAdmission,
)


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


def test_reserve_remeasures_after_prior_writer_consumes_and_releases() -> None:
    free_bytes = 350

    def measure(_path: Path | str) -> FilesystemMeasurement:
        return _measurement("device:test", free_bytes)

    admission = SerializedProcessResourceAdmission(
        thresholds=_thresholds(),
        measurer=measure,
    )

    with admission.reserve(Path("writer-a"), bytes_required=150):
        free_bytes -= 150

    with pytest.raises(HostResourceRefused) as captured, admission.reserve(
        Path("writer-b"), bytes_required=150
    ):
        pass

    assert captured.value.code == "resource_pressure"


def test_measurement_is_inside_the_same_lock_as_accounting() -> None:
    measuring = threading.Event()
    allow_measurement = threading.Event()
    second_measurer_entered = threading.Event()
    calls = 0
    calls_lock = threading.Lock()

    def measure(_path: Path | str) -> FilesystemMeasurement:
        nonlocal calls
        with calls_lock:
            calls += 1
            call = calls
        if call == 1:
            measuring.set()
            assert allow_measurement.wait(timeout=5)
        else:
            second_measurer_entered.set()
        return _measurement("device:test", 1_000)

    admission = SerializedProcessResourceAdmission(
        thresholds=_thresholds(),
        measurer=measure,
    )
    errors: list[Exception] = []

    def reserve(path: str) -> None:
        try:
            with admission.reserve(Path(path), bytes_required=1):
                pass
        except Exception as exc:  # pragma: no cover - failure transport
            errors.append(exc)

    first = threading.Thread(target=reserve, args=("first",))
    second = threading.Thread(target=reserve, args=("second",))
    first.start()
    assert measuring.wait(timeout=5)
    second.start()

    # The second reservation cannot even measure until the first admission
    # decision leaves the controller critical section.
    assert not second_measurer_entered.wait(timeout=0.1)
    allow_measurement.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert second_measurer_entered.is_set()


def test_reserve_many_coalesces_one_filesystem_before_pressure_gate() -> None:
    def measure(_path: Path | str) -> FilesystemMeasurement:
        return _measurement("device:test", 350)

    admission = SerializedProcessResourceAdmission(
        thresholds=_thresholds(),
        measurer=measure,
    )

    # Individually nesting 150 then 50 would make the second operation begin at
    # PRESSURE (350 - 150 == 200). As one logical transaction the combined 200
    # leaves 150, above the 100-byte emergency reserve, so it is admissible.
    with admission.reserve_many(
        (
            (Path("encoded"), 150, 1),
            (Path("raw"), 50, 2),
        )
    ) as reservations:
        assert len(reservations) == 1
        reservation = reservations[0]
        assert reservation.resource_id == "device:test"
        assert reservation.reserved_bytes == 200
        assert reservation.reserved_inodes == 3

    assert admission._reserved == {}


def test_reserve_many_is_all_or_nothing_across_resources() -> None:
    def measure(path: Path | str) -> FilesystemMeasurement:
        name = str(path)
        if name == "healthy":
            return _measurement("device:healthy", 1_000)
        return _measurement("device:pressured", 150)

    admission = SerializedProcessResourceAdmission(
        thresholds=_thresholds(),
        measurer=measure,
    )

    with pytest.raises(HostResourceRefused), admission.reserve_many(
        (
            (Path("healthy"), 100, 0),
            (Path("pressured"), 1, 0),
        )
    ):
        pass

    assert admission._reserved == {}
