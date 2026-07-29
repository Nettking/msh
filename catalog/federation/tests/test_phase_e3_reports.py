from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.federation.errors import FederationValidationError
from catalog.federation.manifest import DatasetManifest
from catalog.federation.phase_d_control import PhaseDControlPlane
from catalog.federation.reporting import (
    StorageReplicaReport,
    assess_storage_replica_report,
)
from catalog.federation.storage_control_plane import StorageProviderRegistration
from catalog.federation.storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
)

NOW = datetime(2026, 7, 29, 15, 0, tzinfo=timezone.utc)


def _control(database: Path) -> PhaseDControlPlane:
    control = PhaseDControlPlane(database)
    control.create_group("session-1", "coordinator", "storage-main")
    control.register_provider(
        "session-1",
        "coordinator",
        StorageProviderRegistration(
            session_id="session-1",
            provider_id="provider-a",
            node_id="node-a",
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            authorized=True,
            status="ready",
        ),
    )
    control.change_assignment("session-1", "coordinator", "storage-main", "provider-a", ())
    return control


def _report(*, revision: int = 1, manifest_revision: int = 1, manifest_hash: str, datasets: tuple[DatasetManifest, ...], committed_hashes: tuple[str, ...], eligible: bool = True, reasons: tuple[str, ...] = ("eligible",), integrity_verified: bool = True, synchronization_state: str = "synchronized", provider_id: str = "provider-a", session_id: str = "session-1", group_id: str = "storage-main") -> StorageReplicaReport:
    return StorageReplicaReport(
        schema="msh.storage_replica_report.v1",
        protocol_version="1.0",
        session_id=session_id,
        group_id=group_id,
        provider_id=provider_id,
        report_revision=revision,
        manifest_revision=manifest_revision,
        manifest_hash=manifest_hash,
        committed_item_hashes=committed_hashes,
        datasets=datasets,
        integrity_verified=integrity_verified,
        synchronization_state=synchronization_state,
        eligible=eligible,
        eligibility_reasons=reasons,
        reported_at=NOW,
    )


def test_e3_report_round_trip_is_deterministic() -> None:
    report = _report(
        manifest_hash="sha256:" + "a" * 64,
        datasets=(),
        committed_hashes=(),
    )
    assert StorageReplicaReport.from_dict(report.to_dict()) == report
    assert report.report_hash() == StorageReplicaReport.from_dict(report.to_dict()).report_hash()


def test_e3_complete_empty_dataset_is_eligible(tmp_path: Path) -> None:
    control = _control(tmp_path / "e3.sqlite3")
    manifest = control.manifest("session-1", "storage-main")
    report = _report(
        revision=manifest.revision,
        manifest_revision=manifest.revision,
        manifest_hash=manifest.manifest_hash,
        datasets=manifest.datasets,
        committed_hashes=tuple(item.content_hash for item in manifest.items),
    )

    assessment = control.submit_storage_replica_report(report, actor_node_id="node-a")

    assert assessment.accepted
    assert assessment.eligibility
    assert assessment.eligibility_reason == "eligible"


def test_e3_rejects_wrong_session_group_sender_and_unknown_protocol() -> None:
    report = _report(
        manifest_hash="sha256:" + "a" * 64,
        datasets=(),
        committed_hashes=(),
    )
    with pytest.raises(FederationValidationError):
        StorageReplicaReport(
            schema="msh.storage_replica_report.v9",
            protocol_version="1.0",
            session_id="session-1",
            group_id="storage-main",
            provider_id="provider-a",
            report_revision=1,
            manifest_revision=1,
            manifest_hash="sha256:" + "a" * 64,
            committed_item_hashes=(),
            datasets=(),
            integrity_verified=True,
            synchronization_state="synchronized",
            eligible=True,
            eligibility_reasons=("eligible",),
            reported_at=NOW,
        )
    assessment = assess_storage_replica_report(
        report,
        None,
        assigned_provider_ids=("provider-a",),
        expected_session_id="session-1",
        expected_group_id="storage-main",
    )
    assert not assessment.accepted
    assert assessment.eligibility_reason == "authoritative-manifest-unavailable"

    with pytest.raises(FederationValidationError):
        StorageReplicaReport(
            schema="msh.storage_replica_report.v1",
            protocol_version="2.0",
            session_id="session-1",
            group_id="storage-main",
            provider_id="provider-a",
            report_revision=1,
            manifest_revision=1,
            manifest_hash="sha256:" + "a" * 64,
            committed_item_hashes=(),
            datasets=(),
            integrity_verified=True,
            synchronization_state="synchronized",
            eligible=True,
            eligibility_reasons=("eligible",),
            reported_at=NOW,
        )


def test_e3_lower_revision_unknown_revision_and_hash_conflict(tmp_path: Path) -> None:
    control = _control(tmp_path / "e3.sqlite3")
    manifest = control.manifest("session-1", "storage-main")
    good = _report(
        manifest_hash=manifest.manifest_hash,
        datasets=manifest.datasets,
        committed_hashes=tuple(item.content_hash for item in manifest.items),
    )
    control.submit_storage_replica_report(good, actor_node_id="node-a")

    stale = _report(
        revision=0,
        manifest_hash=manifest.manifest_hash,
        datasets=manifest.datasets,
        committed_hashes=tuple(item.content_hash for item in manifest.items),
    )
    with pytest.raises(FederationValidationError) as error:
        control.submit_storage_replica_report(stale, actor_node_id="node-a")
    assert error.value.code == "stale-report"


def test_e3_conflicting_duplicate_revision_fails_closed(tmp_path: Path) -> None:
    control = _control(tmp_path / "e3.sqlite3")
    manifest = control.manifest("session-1", "storage-main")
    report = _report(
        manifest_hash=manifest.manifest_hash,
        datasets=manifest.datasets,
        committed_hashes=tuple(item.content_hash for item in manifest.items),
    )
    control.submit_storage_replica_report(report, actor_node_id="node-a")
    conflicting = _report(
        manifest_hash="sha256:" + "b" * 64,
        datasets=manifest.datasets,
        committed_hashes=tuple(item.content_hash for item in manifest.items),
    )
    with pytest.raises(FederationValidationError) as error:
        control.submit_storage_replica_report(conflicting, actor_node_id="node-a")
    assert error.value.code == "conflicting-report-revision"
