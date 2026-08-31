from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.errors import AuthorizationError
from catalog.federation.host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureThresholds,
)
from catalog.federation.process_resource_admission import (
    SerializedProcessResourceAdmission,
)
from catalog.federation.resource_override import (
    MAINTENANCE_POLICY,
    RETENTION_POLICY,
    OperatorPrincipal,
    ResourceOverrideAuthority,
)

NOW = datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc)
ADMIN = OperatorPrincipal("admin@example.test", is_admin=True)
VIEWER = OperatorPrincipal("viewer@example.test")


def _measurement(
    resource_id: str,
    free_bytes: int,
    free_inodes: int | None = 100,
) -> FilesystemMeasurement:
    return FilesystemMeasurement(
        resource_id=resource_id,
        observed_at=NOW,
        total_bytes=10_000,
        free_bytes=free_bytes,
        total_inodes=10_000 if free_inodes is not None else None,
        free_inodes=free_inodes,
        available=True,
    )


def _thresholds() -> PressureThresholds:
    return PressureThresholds(
        critical_free_bytes=100,
        pressure_free_bytes=300,
        warning_free_bytes=500,
        critical_free_inodes=10,
        pressure_free_inodes=30,
        warning_free_inodes=50,
    )


def _audit_admission() -> SerializedProcessResourceAdmission:
    return SerializedProcessResourceAdmission(
        thresholds=_thresholds(),
        measurer=lambda _path: _measurement("device:audit", 10_000, 10_000),
        clock=lambda: NOW,
    )


def _authority(tmp_path: Path, *, clock=lambda: NOW) -> ResourceOverrideAuthority:
    return ResourceOverrideAuthority(
        tmp_path / "resource-overrides.json",
        resource_admission=_audit_admission(),
        clock=clock,
    )


def test_only_an_authorized_operator_can_issue_audited_policy_override(
    tmp_path: Path,
) -> None:
    authority = _authority(tmp_path)
    with pytest.raises(AuthorizationError) as raised:
        authority.issue_policy_override(
            principal=VIEWER,
            kind=RETENTION_POLICY,
            policy_name="analysis-results",
            scope="global",
            value={"mode": "operator-selected"},
            expires_at=NOW + timedelta(hours=1),
            reason="test decision",
        )
    assert getattr(raised.value, "code", None) == "resource-override-forbidden"
    assert not authority.audit_path.exists()

    record = authority.issue_policy_override(
        principal=ADMIN,
        kind=RETENTION_POLICY,
        policy_name="analysis-results",
        scope="global",
        value={"mode": "operator-selected", "limit": "configured-elsewhere"},
        expires_at=NOW + timedelta(hours=1),
        reason="Martin selected the retention policy",
    )
    assert record.kind == RETENTION_POLICY
    assert authority.resolve_policy(
        kind=RETENTION_POLICY,
        policy_name="analysis-results",
        scope="global",
        default={"mode": "unconfigured"},
    ) == {"mode": "operator-selected", "limit": "configured-elsewhere"}
    assert len(authority.audit_records()) == 1

    reopened = _authority(tmp_path)
    assert reopened.resolve_policy(
        kind=RETENTION_POLICY,
        policy_name="analysis-results",
        scope="global",
    ) == {"mode": "operator-selected", "limit": "configured-elsewhere"}


def test_policy_override_is_expiring_and_revocable(tmp_path: Path) -> None:
    now = [NOW]
    authority = _authority(tmp_path, clock=lambda: now[0])
    record = authority.issue_policy_override(
        principal=ADMIN,
        kind=MAINTENANCE_POLICY,
        policy_name="cache-rebuild",
        scope="telemetry",
        value={"enabled": True},
        expires_at=NOW + timedelta(minutes=5),
        reason="bounded maintenance window",
    )
    assert authority.resolve_policy(
        kind=MAINTENANCE_POLICY,
        policy_name="cache-rebuild",
        scope="telemetry",
    ) == {"enabled": True}

    now[0] = NOW + timedelta(minutes=5)
    assert authority.resolve_policy(
        kind=MAINTENANCE_POLICY,
        policy_name="cache-rebuild",
        scope="telemetry",
        default={"enabled": False},
    ) == {"enabled": False}
    now[0] = NOW
    revoked = authority.revoke(principal=ADMIN, override_id=record.override_id)
    assert revoked.revoked_by == ADMIN.actor_id
    assert authority.resolve_policy(
        kind=MAINTENANCE_POLICY,
        policy_name="cache-rebuild",
        scope="telemetry",
        default={"enabled": False},
    ) == {"enabled": False}


def test_audit_publication_is_itself_refused_under_pressure(tmp_path: Path) -> None:
    pressured_admission = SerializedProcessResourceAdmission(
        thresholds=_thresholds(),
        measurer=lambda _path: _measurement("device:pressure", 250, 25),
        clock=lambda: NOW,
    )
    authority = ResourceOverrideAuthority(
        tmp_path / "resource-overrides.json",
        resource_admission=pressured_admission,
        clock=lambda: NOW,
    )
    with pytest.raises(HostResourceRefused) as raised:
        authority.issue_policy_override(
            principal=ADMIN,
            kind=RETENTION_POLICY,
            policy_name="analysis-results",
            scope="global",
            value={"mode": "operator-selected"},
            expires_at=NOW + timedelta(hours=1),
            reason="audit write must remain bounded",
        )
    assert raised.value.code == "resource_pressure"
    assert not authority.audit_path.exists()


def test_emergency_lease_admits_one_pressured_transaction_and_unwinds(
    tmp_path: Path,
) -> None:
    def measure(_path: Path | str) -> FilesystemMeasurement:
        return _measurement("device:pressure", 250, 25)

    admission = SerializedProcessResourceAdmission(
        thresholds=_thresholds(), measurer=measure, clock=lambda: NOW
    )
    authority = ResourceOverrideAuthority(
        tmp_path / "resource-overrides.json",
        resource_admission=_audit_admission(),
        clock=lambda: NOW,
    )
    lease = authority.issue_emergency_admission(
        principal=ADMIN,
        operation="analysis.result-publication",
        resource_ids=("device:pressure",),
        max_bytes=40,
        max_inodes=2,
        expires_at=NOW + timedelta(minutes=5),
        reason="finish one already-approved publication",
    )
    with pytest.raises(HostResourceRefused) as normal, admission.reserve(
        "result.json",
        bytes_required=40,
        inodes_required=2,
        operation="analysis.result-publication",
    ):
        pass
    assert normal.value.code == "resource_pressure"

    with admission.reserve(
        "result.json",
        bytes_required=40,
        inodes_required=2,
        operation="analysis.result-publication",
        override=lease,
    ):
        assert admission._reserved == {"device:pressure": (40, 2)}
        with pytest.raises(RuntimeError, match="writer failed"):
            raise RuntimeError("writer failed")
    assert admission._reserved == {}
    assert lease.used is True

    with pytest.raises(HostResourceRefused) as reused, admission.reserve(
        "result.json",
        bytes_required=1,
        inodes_required=1,
        operation="analysis.result-publication",
        override=lease,
    ):
        pass
    assert reused.value.code == "emergency_override_already_used"


def test_emergency_lease_never_overrides_critical_or_wrong_identity(tmp_path: Path) -> None:
    def critical(_path: Path | str) -> FilesystemMeasurement:
        return _measurement("device:critical", 90, 9)

    admission = SerializedProcessResourceAdmission(
        thresholds=_thresholds(), measurer=critical, clock=lambda: NOW
    )
    authority = _authority(tmp_path)
    lease = authority.issue_emergency_admission(
        principal=ADMIN,
        operation="analysis.result-publication",
        resource_ids=("device:critical",),
        max_bytes=10,
        max_inodes=1,
        expires_at=NOW + timedelta(minutes=5),
        reason="must still stop at the emergency floor",
    )
    with pytest.raises(HostResourceRefused) as raised, admission.reserve(
        "result.json",
        bytes_required=1,
        inodes_required=1,
        operation="analysis.result-publication",
        override=lease,
    ):
        pass
    assert raised.value.code == "resource_pressure"
    assert lease.used is False

    def pressured(_path: Path | str) -> FilesystemMeasurement:
        return _measurement("device:other", 250, 25)

    other_admission = SerializedProcessResourceAdmission(
        thresholds=_thresholds(), measurer=pressured, clock=lambda: NOW
    )
    with pytest.raises(HostResourceRefused) as mismatch, other_admission.reserve(
        "result.json",
        bytes_required=1,
        inodes_required=1,
        operation="analysis.result-publication",
        override=lease,
    ):
        pass
    assert mismatch.value.code == "emergency_override_resource_scope"
    assert other_admission._reserved == {}


def test_emergency_lease_caps_atomic_multi_resource_bytes_and_inodes(
    tmp_path: Path,
) -> None:
    def measure(path: Path | str) -> FilesystemMeasurement:
        resource = "device:a" if "a" in str(path) else "device:b"
        return _measurement(resource, 250, 25)

    admission = SerializedProcessResourceAdmission(
        thresholds=_thresholds(), measurer=measure, clock=lambda: NOW
    )
    authority = _authority(tmp_path)
    lease = authority.issue_emergency_admission(
        principal=ADMIN,
        operation="analysis.cross-resource-publication",
        resource_ids=("device:a", "device:b"),
        max_bytes=100,
        max_inodes=2,
        expires_at=NOW + timedelta(minutes=5),
        reason="bounded two-resource transaction",
    )
    with pytest.raises(HostResourceRefused) as raised, admission.reserve_many(
        (("a.json", 60, 1), ("b.json", 60, 1)),
        operation="analysis.cross-resource-publication",
        override=lease,
    ):
        pass
    assert raised.value.code == "emergency_override_byte_cap"
    assert admission._reserved == {}


def test_emergency_lease_expires_and_revocation_is_enforced(tmp_path: Path) -> None:
    now = [NOW]
    admission = SerializedProcessResourceAdmission(
        thresholds=_thresholds(),
        measurer=lambda _path: FilesystemMeasurement(
            resource_id="device:pressure",
            observed_at=now[0],
            total_bytes=10_000,
            free_bytes=250,
            total_inodes=10_000,
            free_inodes=25,
            available=True,
        ),
        clock=lambda: now[0],
    )
    authority = _authority(tmp_path, clock=lambda: now[0])
    lease = authority.issue_emergency_admission(
        principal=ADMIN,
        operation="analysis.result-publication",
        resource_ids=("device:pressure",),
        max_bytes=10,
        max_inodes=1,
        expires_at=NOW + timedelta(minutes=5),
        reason="short-lived emergency lease",
    )
    authority.revoke(principal=ADMIN, override_id=lease.override_id)
    with pytest.raises(HostResourceRefused) as revoked, admission.reserve(
        "result.json",
        bytes_required=1,
        inodes_required=1,
        operation="analysis.result-publication",
        override=lease,
    ):
        pass
    assert revoked.value.code == "emergency_override_revoked"

    replacement = authority.issue_emergency_admission(
        principal=ADMIN,
        operation="analysis.result-publication",
        resource_ids=("device:pressure",),
        max_bytes=10,
        max_inodes=1,
        expires_at=NOW + timedelta(minutes=5),
        reason="replacement lease for expiry test",
    )
    now[0] = NOW + timedelta(minutes=5)
    with pytest.raises(HostResourceRefused) as expired, admission.reserve(
        "result.json",
        bytes_required=1,
        inodes_required=1,
        operation="analysis.result-publication",
        override=replacement,
    ):
        pass
    assert expired.value.code == "emergency_override_expired"
