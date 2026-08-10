"""Provider replica reports and coordinator-derived eligibility assessment."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Self

from .errors import FederationValidationError, ProtocolCompatibilityError
from .manifest import AuthoritativeStorageManifest, DatasetManifest
from .storage_protocol import validate_protocol_version

REPORT_SCHEMA = "fcp.storage_replica_report.v1"
REPORT_PROTOCOL_VERSION = "1.0"


def _required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(ord(char) < 32 for char in value):
        raise FederationValidationError("invalid-id", field, "must be non-empty opaque text")
    return value


def _uint(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError("invalid-non-negative-integer", field, "must be a non-negative integer")
    return value


def _utc(value: Any, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError("invalid-timestamp", field, "must be RFC 3339") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError("invalid-timestamp", field, "must be timezone-aware")
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return _utc(value, "reported_at").isoformat().replace("+00:00", "Z")


def _digest(value: dict[str, Any]) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


@dataclass(frozen=True)
class StorageReplicaReport:
    schema: str
    protocol_version: str
    session_id: str
    group_id: str
    provider_id: str
    report_revision: int
    manifest_revision: int
    manifest_hash: str
    committed_item_hashes: tuple[str, ...]
    datasets: tuple[DatasetManifest, ...]
    integrity_verified: bool
    synchronization_state: str
    eligible: bool
    eligibility_reasons: tuple[str, ...]
    reported_at: datetime

    def __post_init__(self) -> None:
        if self.schema != REPORT_SCHEMA:
            raise ProtocolCompatibilityError("unsupported-schema", "schema", f"expected {REPORT_SCHEMA}")
        object.__setattr__(self, "protocol_version", validate_protocol_version(self.protocol_version))
        for field in ("session_id", "group_id", "provider_id", "manifest_hash", "synchronization_state"):
            object.__setattr__(self, field, _required_text(getattr(self, field), field))
        object.__setattr__(self, "report_revision", _uint(self.report_revision, "report_revision"))
        object.__setattr__(self, "manifest_revision", _uint(self.manifest_revision, "manifest_revision"))
        if not isinstance(self.integrity_verified, bool):
            raise FederationValidationError("invalid-boolean", "integrity_verified", "must be boolean")
        if not isinstance(self.eligible, bool):
            raise FederationValidationError("invalid-boolean", "eligible", "must be boolean")
        if not self.eligibility_reasons:
            raise FederationValidationError("missing-field", "eligibility_reasons", "at least one reason is required")
        object.__setattr__(self, "eligibility_reasons", tuple(_required_text(reason, "eligibility_reasons") for reason in self.eligibility_reasons))
        object.__setattr__(self, "committed_item_hashes", tuple(sorted(_required_text(value, "committed_item_hashes") for value in self.committed_item_hashes)))
        object.__setattr__(
            self,
            "datasets",
            tuple(
                sorted(
                    [
                        dataset
                        if isinstance(dataset, DatasetManifest)
                        else DatasetManifest.from_dict(dataset)
                        for dataset in self.datasets
                    ],
                    key=lambda value: (value.dataset_id, value.schema_name, value.schema_version),
                )
            ),
        )
        object.__setattr__(self, "reported_at", _utc(self.reported_at, "reported_at"))

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "storage_replica_report", "must be an object")
        required = {
            "schema",
            "protocol_version",
            "session_id",
            "group_id",
            "provider_id",
            "report_revision",
            "manifest_revision",
            "manifest_hash",
            "committed_item_hashes",
            "datasets",
            "integrity_verified",
            "synchronization_state",
            "eligible",
            "eligibility_reasons",
            "reported_at",
        }
        missing = sorted(required.difference(value))
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        return cls(
            schema=value["schema"],
            protocol_version=value["protocol_version"],
            session_id=value["session_id"],
            group_id=value["group_id"],
            provider_id=value["provider_id"],
            report_revision=value["report_revision"],
            manifest_revision=value["manifest_revision"],
            manifest_hash=value["manifest_hash"],
            committed_item_hashes=tuple(value["committed_item_hashes"]),
            datasets=tuple(DatasetManifest.from_dict(item) for item in value["datasets"]),
            integrity_verified=value["integrity_verified"],
            synchronization_state=value["synchronization_state"],
            eligible=value["eligible"],
            eligibility_reasons=tuple(value["eligibility_reasons"]),
            reported_at=value["reported_at"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "protocol_version": self.protocol_version,
            "session_id": self.session_id,
            "group_id": self.group_id,
            "provider_id": self.provider_id,
            "report_revision": self.report_revision,
            "manifest_revision": self.manifest_revision,
            "manifest_hash": self.manifest_hash,
            "committed_item_hashes": list(self.committed_item_hashes),
            "datasets": [dataset.to_dict() for dataset in self.datasets],
            "integrity_verified": self.integrity_verified,
            "synchronization_state": self.synchronization_state,
            "eligible": self.eligible,
            "eligibility_reasons": list(self.eligibility_reasons),
            "reported_at": _timestamp(self.reported_at),
        }

    def report_hash(self) -> str:
        return _digest(self.to_dict())


@dataclass(frozen=True)
class StorageReplicaAssessment:
    session_id: str
    group_id: str
    provider_id: str
    report_revision: int
    report_hash: str
    accepted: bool
    eligibility: bool
    eligibility_reason: str
    assessed_at: datetime
    report: StorageReplicaReport | None = None

    def __post_init__(self) -> None:
        for field in ("session_id", "group_id", "provider_id", "report_hash", "eligibility_reason"):
            object.__setattr__(self, field, _required_text(getattr(self, field), field))
        object.__setattr__(self, "report_revision", _uint(self.report_revision, "report_revision"))
        if not isinstance(self.accepted, bool):
            raise FederationValidationError("invalid-boolean", "accepted", "must be boolean")
        if not isinstance(self.eligibility, bool):
            raise FederationValidationError("invalid-boolean", "eligibility", "must be boolean")
        object.__setattr__(self, "assessed_at", _utc(self.assessed_at, "assessed_at"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "group_id": self.group_id,
            "provider_id": self.provider_id,
            "report_revision": self.report_revision,
            "report_hash": self.report_hash,
            "accepted": self.accepted,
            "eligibility": self.eligibility,
            "eligibility_reason": self.eligibility_reason,
            "assessed_at": _timestamp(self.assessed_at),
            "report": None if self.report is None else self.report.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: Any) -> Self:
        if not isinstance(value, dict):
            raise FederationValidationError("invalid-object", "storage_replica_assessment", "must be an object")
        required = {
            "session_id",
            "group_id",
            "provider_id",
            "report_revision",
            "report_hash",
            "accepted",
            "eligibility",
            "eligibility_reason",
            "assessed_at",
            "report",
        }
        missing = sorted(required.difference(value))
        if missing:
            raise FederationValidationError("missing-field", missing[0], "is required")
        return cls(
            session_id=value["session_id"],
            group_id=value["group_id"],
            provider_id=value["provider_id"],
            report_revision=value["report_revision"],
            report_hash=value["report_hash"],
            accepted=value["accepted"],
            eligibility=value["eligibility"],
            eligibility_reason=value["eligibility_reason"],
            assessed_at=value["assessed_at"],
            report=None if value["report"] is None else StorageReplicaReport.from_dict(value["report"]),
        )


def assess_storage_replica_report(
    report: StorageReplicaReport,
    authoritative_manifest: AuthoritativeStorageManifest | None,
    *,
    assigned_provider_ids: tuple[str, ...],
    expected_session_id: str,
    expected_group_id: str,
) -> StorageReplicaAssessment:
    if authoritative_manifest is None:
        return StorageReplicaAssessment(
            session_id=report.session_id,
            group_id=report.group_id,
            provider_id=report.provider_id,
            report_revision=report.report_revision,
            report_hash=report.report_hash(),
            accepted=False,
            eligibility=False,
            eligibility_reason="authoritative-manifest-unavailable",
            assessed_at=report.reported_at,
            report=report,
        )
    if report.session_id != expected_session_id:
        reason = "session-mismatch"
    elif report.group_id != expected_group_id:
        reason = "storage-group-mismatch"
    elif report.provider_id not in assigned_provider_ids:
        reason = "provider-not-assigned"
    elif report.manifest_revision < authoritative_manifest.revision:
        reason = "manifest-revision-behind"
    elif report.manifest_revision > authoritative_manifest.revision:
        reason = "manifest-revision-ahead"
    elif report.manifest_hash != authoritative_manifest.manifest_hash:
        reason = "manifest-hash-mismatch"
    elif not report.integrity_verified:
        reason = "integrity-failure"
    elif report.synchronization_state != "synchronized":
        reason = "synchronization-in-progress"
    else:
        reason = "eligible"
    eligibility = reason == "eligible"
    if eligibility:
        authoritative_datasets = {dataset.dataset_id: dataset for dataset in authoritative_manifest.datasets}
        for dataset_id, dataset in authoritative_datasets.items():
            report_dataset = next((value for value in report.datasets if value.dataset_id == dataset_id), None)
            if report_dataset is None:
                eligibility = False
                reason = "missing-ranges"
                break
            if (
                report_dataset.schema_name != dataset.schema_name
                or report_dataset.schema_version != dataset.schema_version
                or report_dataset.source_id != dataset.source_id
                or report_dataset.last_contiguous_sequence != dataset.last_contiguous_sequence
                or report_dataset.missing_ranges != dataset.missing_ranges
            ):
                eligibility = False
                reason = "missing-ranges" if report_dataset.missing_ranges else "committed-hash-conflict"
                break
        if eligibility:
            authoritative_hashes = tuple(sorted(item.content_hash for item in authoritative_manifest.items))
            if tuple(report.committed_item_hashes) != authoritative_hashes:
                eligibility = False
                reason = "committed-hash-conflict"
    return StorageReplicaAssessment(
        session_id=report.session_id,
        group_id=report.group_id,
        provider_id=report.provider_id,
        report_revision=report.report_revision,
        report_hash=report.report_hash(),
        accepted=True,
        eligibility=eligibility,
        eligibility_reason=reason,
        assessed_at=report.reported_at,
        report=report,
    )
