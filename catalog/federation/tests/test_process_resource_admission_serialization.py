from __future__ import annotations

import threading
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureThresholds,
    ProcessResourceAdmission,
)
from catalog.federation.process_resource_admission import (
    SerializedProcessResourceAdmission,
)
from catalog.federation.resource_override import (
    OperatorPrincipal,
    ResourceOverrideAuthority,
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
    errors: list[HostResourceRefused] = []

    def reserve(path: str) -> None:
        try:
            with admission.reserve(Path(path), bytes_required=1):
                pass
        except HostResourceRefused as exc:  # pragma: no cover - failure transport
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


@pytest.mark.parametrize("admission_type", [ProcessResourceAdmission, SerializedProcessResourceAdmission])
def test_bounded_relay_control_reservation_survives_pressure_and_unwinds(
    admission_type: type[ProcessResourceAdmission],
) -> None:
    free_bytes = 150
    measured: list[int] = []

    def measure(_path: Path | str) -> FilesystemMeasurement:
        measured.append(free_bytes)
        return _measurement("device:control", free_bytes)

    admission = admission_type(thresholds=_thresholds(), measurer=measure)
    with pytest.raises(RuntimeError, match="control SQL failed"), admission.reserve(
        "control.sqlite3", bytes_required=20, inodes_required=2,
        operation="relay-control-transaction",
    ) as reservation:
        assert reservation.reserved_bytes == 20
        assert admission._reserved == {"device:control": (20, 2)}
        raise RuntimeError("control SQL failed")
    assert admission._reserved == {}

    # A later control transaction must measure again rather than reuse admission.
    free_bytes = 100
    with pytest.raises(HostResourceRefused), admission.reserve(
        "control.sqlite3", bytes_required=1, operation="relay-control-transaction",
    ):
        pass
    assert measured == [150, 100]
    assert admission._reserved == {}


@pytest.mark.parametrize("operation", [None, "relay-control-transaction-unknown"])
@pytest.mark.parametrize("admission_type", [ProcessResourceAdmission, SerializedProcessResourceAdmission])
def test_pressure_still_refuses_default_and_unknown_control_operations(
    admission_type: type[ProcessResourceAdmission], operation: str | None,
) -> None:
    admission = admission_type(
        thresholds=_thresholds(), measurer=lambda _path: _measurement("device:control", 150),
    )
    arguments = {} if operation is None else {"operation": operation}
    with pytest.raises(HostResourceRefused) as refused, admission.reserve(
        "control.sqlite3", bytes_required=1, **arguments,
    ):
        pass
    assert refused.value.code == "resource_pressure"
    assert admission._reserved == {}


@pytest.mark.parametrize("bytes_required,inodes_required", [(8 * 1024**2 + 1, 4), (1, 5)])
@pytest.mark.parametrize("admission_type", [ProcessResourceAdmission, SerializedProcessResourceAdmission])
def test_relay_control_label_does_not_admit_oversized_work(
    admission_type: type[ProcessResourceAdmission], bytes_required: int, inodes_required: int,
) -> None:
    admission = admission_type(measurer=lambda _path: replace(
        _measurement("device:control", 11 * 1024**3), total_bytes=32 * 1024**3,
        total_inodes=10_000, free_inodes=2_048,
    ))
    with pytest.raises(HostResourceRefused) as refused, admission.reserve(
        "control.sqlite3", bytes_required=bytes_required, inodes_required=inodes_required,
        operation="relay-control-transaction",
    ):
        pass
    assert refused.value.code == "resource_pressure"
    assert admission._reserved == {}


@pytest.mark.parametrize(
    "free_bytes,free_inodes,bytes_required,inodes_required,available,error_code",
    [
        (100, 25, 1, 1, True, "resource_pressure"),
        (150, 25, 50, 1, True, "emergency_reserve"),
        (150, 10, 1, 1, True, "resource_pressure"),
        (150, 14, 1, 4, True, "emergency_reserve"),
        (150, 25, 1, 1, False, "resource_pressure"),
    ],
    ids=["byte-floor", "would-cross-byte-floor", "inode-floor", "would-cross-inode-floor", "unavailable"],
)
@pytest.mark.parametrize("admission_type", [ProcessResourceAdmission, SerializedProcessResourceAdmission])
def test_relay_control_cannot_spend_emergency_reserve(
    admission_type: type[ProcessResourceAdmission], free_bytes: int, free_inodes: int,
    bytes_required: int, inodes_required: int, available: bool, error_code: str,
) -> None:
    admission = admission_type(
        thresholds=replace(_thresholds(), critical_free_inodes=10,
                           pressure_free_inodes=20, warning_free_inodes=30),
        measurer=lambda _path: replace(_measurement("device:control", free_bytes),
            total_inodes=100, free_inodes=free_inodes, available=available),
    )
    with pytest.raises(HostResourceRefused) as refused, admission.reserve(
        "control.sqlite3", bytes_required=bytes_required, inodes_required=inodes_required,
        operation="relay-control-transaction",
    ):
        pass
    assert refused.value.code == error_code
    assert admission._reserved == {}


def test_relay_control_coalesces_aliases_and_counts_other_active_control_work() -> None:
    admission = SerializedProcessResourceAdmission(
        thresholds=_thresholds(), measurer=lambda _path: _measurement("device:control", 190),
    )
    with admission.reserve_many(
        (("control.sqlite3", 40, 1), ("control.sqlite3-wal", 20, 1)),
        operation="relay-control-transaction",
    ) as reservations:
        assert len(reservations) == 1
        assert admission._reserved == {"device:control": (60, 2)}
        with pytest.raises(HostResourceRefused) as refused, admission.reserve(
            "control.sqlite3-shm", bytes_required=30, operation="relay-control-transaction",
        ):
            pass
        assert refused.value.code == "emergency_reserve"
        assert admission._reserved == {"device:control": (60, 2)}
    assert admission._reserved == {}

    # Neither alias individually exceeds the cap; their coalesced request does.
    with pytest.raises(HostResourceRefused) as oversized, admission.reserve_many(
        (("control.sqlite3", 4 * 1024**2, 1), ("control.sqlite3-wal", 4 * 1024**2 + 1, 1)),
        operation="relay-control-transaction",
    ):
        pass
    assert oversized.value.code == "resource_pressure"
    assert admission._reserved == {}


@pytest.mark.parametrize("admission_type", [ProcessResourceAdmission, SerializedProcessResourceAdmission])
def test_relay_control_refuses_failed_filesystem_measurement(
    admission_type: type[ProcessResourceAdmission],
) -> None:
    def measure(_path: Path | str) -> FilesystemMeasurement:
        raise OSError("filesystem unavailable")

    admission = admission_type(thresholds=_thresholds(), measurer=measure)
    with pytest.raises(HostResourceRefused) as refused, admission.reserve(
        "control.sqlite3", bytes_required=1, operation="relay-control-transaction",
    ):
        pass
    assert refused.value.code == "resource_pressure"
    assert admission._reserved == {}


@pytest.mark.parametrize("invalid_lease", ["operation", "revoked", "expired"])
def test_relay_control_label_still_validates_a_supplied_override(
    tmp_path: Path, invalid_lease: str,
) -> None:
    now = datetime.now(timezone.utc)
    clock = now
    admission = SerializedProcessResourceAdmission(
        thresholds=_thresholds(), measurer=lambda _path: replace(
            _measurement("device:control", 150), observed_at=clock), clock=lambda: clock,
    )
    audit_admission = SerializedProcessResourceAdmission(
        thresholds=_thresholds(), measurer=lambda _path: replace(
            _measurement("device:audit", 1024**3), total_bytes=2 * 1024**3,
            observed_at=clock), clock=lambda: clock,
    )
    authority = ResourceOverrideAuthority(
        tmp_path / "overrides.json", resource_admission=audit_admission, clock=lambda: clock,
    )
    lease = authority.issue_emergency_admission(
        principal=OperatorPrincipal("admin@example.test", is_admin=True),
        operation="other-operation" if invalid_lease == "operation" else "relay-control-transaction",
        resource_ids=("device:control",), max_bytes=20, max_inodes=2,
        expires_at=now + timedelta(seconds=30), reason="bounded control regression",
    )
    if invalid_lease == "revoked":
        authority.revoke(principal=OperatorPrincipal("admin@example.test", is_admin=True),
                         override_id=lease.override_id)
    elif invalid_lease == "expired":
        clock = now + timedelta(seconds=31)
    with pytest.raises(HostResourceRefused) as refused, admission.reserve(
        "control.sqlite3", bytes_required=20, inodes_required=2,
        operation="relay-control-transaction", override=lease,
    ):
        pass
    expected = "operation_scope" if invalid_lease == "operation" else invalid_lease
    assert refused.value.code == "emergency_override_" + expected
    assert admission._reserved == {}
