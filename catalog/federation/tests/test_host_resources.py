from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation import host_resources
from catalog.federation.host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureLevel,
    PressureThresholds,
    ProcessResourceAdmission,
    assess_measurement,
    measure_filesystem,
)

NOW = datetime(2026, 8, 25, 12, 0, tzinfo=timezone.utc)


def thresholds() -> PressureThresholds:
    return PressureThresholds(
        critical_free_bytes=100,
        pressure_free_bytes=200,
        warning_free_bytes=300,
        critical_free_inodes=10,
        pressure_free_inodes=20,
        warning_free_inodes=30,
        max_measurement_age_seconds=10,
        future_measurement_tolerance_seconds=2,
    )


def measurement(
    *,
    resource_id: str = "device:1",
    free_bytes: int | None = 1000,
    free_inodes: int | None = 1000,
    observed_at: datetime = NOW,
    available: bool = True,
    error_code: str | None = None,
) -> FilesystemMeasurement:
    return FilesystemMeasurement(
        resource_id=resource_id,
        observed_at=observed_at,
        total_bytes=2000 if free_bytes is not None else None,
        free_bytes=free_bytes,
        total_inodes=2000 if free_inodes is not None else None,
        free_inodes=free_inodes,
        available=available,
        error_code=error_code,
    )


def test_thresholds_are_ordered_and_integer_bounded() -> None:
    with pytest.raises(ValueError, match="byte thresholds must be ordered"):
        PressureThresholds(
            critical_free_bytes=200,
            pressure_free_bytes=100,
            warning_free_bytes=300,
        )
    with pytest.raises(ValueError, match="inode thresholds must be ordered"):
        PressureThresholds(
            critical_free_inodes=20,
            pressure_free_inodes=10,
            warning_free_inodes=30,
        )
    with pytest.raises(ValueError, match="non-negative integers"):
        PressureThresholds(critical_free_bytes=1.5)  # type: ignore[arg-type]


def test_byte_pressure_state_has_all_four_levels() -> None:
    policy = thresholds()
    expected = {
        301: PressureLevel.NORMAL,
        300: PressureLevel.WARNING,
        201: PressureLevel.WARNING,
        200: PressureLevel.PRESSURE,
        101: PressureLevel.PRESSURE,
        100: PressureLevel.CRITICAL,
        0: PressureLevel.CRITICAL,
    }
    for free_bytes, level in expected.items():
        result = assess_measurement(
            measurement(free_bytes=free_bytes, free_inodes=None),
            thresholds=policy,
            now=NOW,
        )
        assert result.level == level


def test_inode_pressure_can_be_stricter_than_bytes() -> None:
    policy = thresholds()
    result = assess_measurement(
        measurement(free_bytes=1000, free_inodes=9),
        thresholds=policy,
        now=NOW,
    )
    assert result.level == PressureLevel.CRITICAL
    assert "inodes_critical" in result.reasons


def test_filesystem_without_inode_accounting_does_not_invent_inodes() -> None:
    result = assess_measurement(
        measurement(free_bytes=1000, free_inodes=None),
        thresholds=thresholds(),
        now=NOW,
    )
    assert result.level == PressureLevel.NORMAL
    assert result.effective_free_inodes is None


def test_unavailable_measurement_is_critical_not_healthy() -> None:
    result = assess_measurement(
        measurement(
            free_bytes=None,
            free_inodes=None,
            available=False,
            error_code="measurement_unavailable",
        ),
        thresholds=thresholds(),
        now=NOW,
    )
    assert result.level == PressureLevel.CRITICAL
    assert result.reasons == ("measurement_unavailable",)


def test_malformed_capacity_is_critical_not_an_exception() -> None:
    malformed = FilesystemMeasurement(
        resource_id="device:bad",
        observed_at=NOW,
        total_bytes=2000,
        free_bytes=-1,
        total_inodes=2000,
        free_inodes=100,
        available=True,
    )
    result = assess_measurement(malformed, thresholds=thresholds(), now=NOW)
    assert result.level == PressureLevel.CRITICAL
    assert result.reasons == ("measurement_invalid",)


def test_stale_future_and_naive_measurements_fail_critical() -> None:
    policy = thresholds()
    stale = assess_measurement(
        measurement(observed_at=NOW - timedelta(seconds=11)),
        thresholds=policy,
        now=NOW,
    )
    future = assess_measurement(
        measurement(observed_at=NOW + timedelta(seconds=3)),
        thresholds=policy,
        now=NOW,
    )
    naive = assess_measurement(
        measurement(observed_at=NOW.replace(tzinfo=None)),
        thresholds=policy,
        now=NOW,
    )
    assert stale.level == PressureLevel.CRITICAL
    assert stale.reasons == ("measurement_stale",)
    assert future.level == PressureLevel.CRITICAL
    assert future.reasons == ("measurement_time_invalid",)
    assert naive.level == PressureLevel.CRITICAL
    assert naive.reasons == ("measurement_time_invalid",)


def test_active_reservations_change_pressure_before_new_work() -> None:
    result = assess_measurement(
        measurement(free_bytes=350, free_inodes=None),
        thresholds=thresholds(),
        reserved_bytes=150,
        now=NOW,
    )
    assert result.effective_free_bytes == 200
    assert result.level == PressureLevel.PRESSURE


def test_warning_allows_bounded_reservation_that_keeps_emergency_floor() -> None:
    source = measurement(free_bytes=280, free_inodes=100)
    controller = ProcessResourceAdmission(
        thresholds=thresholds(),
        measurer=lambda _path: source,
        clock=lambda: NOW,
    )
    with controller.reserve("/data", bytes_required=100, inodes_required=5) as held:
        assert held.resource_id == "device:1"
        during = controller.assessment("/data")
        assert during.reserved_bytes == 100
        assert during.reserved_inodes == 5
        assert during.level == PressureLevel.PRESSURE
    after = controller.assessment("/data")
    assert after.reserved_bytes == 0
    assert after.level == PressureLevel.WARNING


def test_new_work_is_refused_once_existing_reservations_reach_pressure() -> None:
    source = measurement(free_bytes=350, free_inodes=None)
    controller = ProcessResourceAdmission(
        thresholds=thresholds(),
        measurer=lambda _path: source,
        clock=lambda: NOW,
    )
    with controller.reserve("/data/a", bytes_required=150):
        with (
            pytest.raises(HostResourceRefused) as raised,
            controller.reserve("/data/b", bytes_required=1),
        ):
            pass
        assert raised.value.code == "resource_pressure"
        assert raised.value.assessment.level == PressureLevel.PRESSURE


def test_one_transaction_cannot_spend_the_emergency_reserve() -> None:
    source = measurement(free_bytes=350, free_inodes=None)
    controller = ProcessResourceAdmission(
        thresholds=thresholds(),
        measurer=lambda _path: source,
        clock=lambda: NOW,
    )
    with (
        pytest.raises(HostResourceRefused) as raised,
        controller.reserve("/data", bytes_required=250),
    ):
        pass
    assert raised.value.code == "emergency_reserve"
    assert raised.value.assessment.effective_free_bytes == 100


def test_paths_on_same_resource_share_one_active_envelope() -> None:
    source = measurement(resource_id="device:77", free_bytes=500, free_inodes=None)
    controller = ProcessResourceAdmission(
        thresholds=thresholds(),
        measurer=lambda _path: source,
        clock=lambda: NOW,
    )
    with (
        controller.reserve("/data", bytes_required=100),
        controller.reserve("/results", bytes_required=50),
    ):
        result = controller.assessment("/uploads")
        assert result.resource_id == "device:77"
        assert result.reserved_bytes == 150


def test_distinct_backing_resources_do_not_share_reservations() -> None:
    def fake(path: Path | str) -> FilesystemMeasurement:
        value = str(path)
        return measurement(
            resource_id="device:data" if "data" in value else "device:results",
            free_bytes=350,
            free_inodes=None,
        )

    controller = ProcessResourceAdmission(
        thresholds=thresholds(),
        measurer=fake,
        clock=lambda: NOW,
    )
    with controller.reserve("/data", bytes_required=150):
        data = controller.assessment("/data")
        results = controller.assessment("/results")
        assert data.level == PressureLevel.PRESSURE
        assert data.reserved_bytes == 150
        assert results.level == PressureLevel.NORMAL
        assert results.reserved_bytes == 0


def test_reservation_is_released_when_writer_raises() -> None:
    source = measurement(free_bytes=500, free_inodes=None)
    controller = ProcessResourceAdmission(
        thresholds=thresholds(),
        measurer=lambda _path: source,
        clock=lambda: NOW,
    )
    with (
        pytest.raises(RuntimeError, match="boom"),
        controller.reserve("/data", bytes_required=100),
    ):
        raise RuntimeError("boom")
    assert controller.assessment("/data").reserved_bytes == 0


def test_measurer_failure_is_conservatively_refused() -> None:
    def broken(_path: Path | str) -> FilesystemMeasurement:
        raise OSError("gone")

    controller = ProcessResourceAdmission(
        thresholds=thresholds(),
        measurer=broken,
        clock=lambda: NOW,
    )
    with (
        pytest.raises(HostResourceRefused) as raised,
        controller.reserve("/data", bytes_required=1),
    ):
        pass
    assert raised.value.code == "resource_pressure"
    assert raised.value.assessment.level == PressureLevel.CRITICAL


def test_measurement_uses_nearest_existing_parent_and_backing_device(
    tmp_path: Path,
) -> None:
    target = tmp_path / "not-created" / "yet"
    result = measure_filesystem(target, observed_at=NOW)
    assert result.available is True
    assert result.resource_id.startswith(("device:", "volume:"))
    assert result.total_bytes is not None and result.total_bytes > 0
    assert result.free_bytes is not None and result.free_bytes >= 0


def test_inode_probe_failure_keeps_valid_byte_measurement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def broken_statvfs(_path: Path) -> object:
        raise OSError("unsupported")

    monkeypatch.setattr(host_resources.os, "statvfs", broken_statvfs, raising=False)
    result = measure_filesystem(tmp_path, observed_at=NOW)
    assert result.available is True
    assert result.free_bytes is not None
    assert result.total_inodes is None
    assert result.free_inodes is None


def test_byte_measurement_failure_is_explicitly_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def broken_disk_usage(_path: Path) -> object:
        raise OSError("gone")

    monkeypatch.setattr(host_resources.shutil, "disk_usage", broken_disk_usage)
    result = measure_filesystem(tmp_path, observed_at=NOW)
    assert result.available is False
    assert result.error_code == "measurement_unavailable"
    assert result.free_bytes is None


def test_inode_measurement_prefers_available_inodes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        host_resources.os,
        "statvfs",
        lambda _path: SimpleNamespace(f_files=1000, f_favail=321, f_ffree=400),
        raising=False,
    )
    result = measure_filesystem(tmp_path, observed_at=NOW)
    assert result.total_inodes == 1000
    assert result.free_inodes == 321


def test_zero_available_inodes_never_falls_back_to_root_reserved_inodes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        host_resources.os,
        "statvfs",
        lambda _path: SimpleNamespace(f_files=1000, f_favail=0, f_ffree=400),
        raising=False,
    )
    result = measure_filesystem(tmp_path, observed_at=NOW)
    assert result.total_inodes == 1000
    assert result.free_inodes == 0
    assessed = assess_measurement(result, thresholds=thresholds(), now=NOW)
    assert assessed.level == PressureLevel.CRITICAL
    assert "inodes_critical" in assessed.reasons
