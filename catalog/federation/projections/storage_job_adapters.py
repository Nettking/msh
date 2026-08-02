"""Adapters for existing storage and durable job authorities."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone

from .adapter_common import (
    _enum_value,
    _optional_public_text,
    _public_text,
    _sequence,
    _timestamp,
    _value,
)
from .safety import safe_reason_code


@dataclass(frozen=True)
class StorageProviderRecord:
    provider_id: str
    node_id: str
    protocol: str
    protocol_version: str
    status: str
    authorized: bool
    assignable: bool
    roles: tuple[str, ...]


@dataclass(frozen=True)
class StorageGroupRecord:
    group_id: str
    primary_provider_id: str | None
    replica_provider_ids: tuple[str, ...]
    leader_active: bool


@dataclass(frozen=True)
class StorageAuthoritySnapshot:
    available: bool
    reason_code: str
    revision: int | None = None
    providers: tuple[StorageProviderRecord, ...] = ()
    groups: tuple[StorageGroupRecord, ...] = ()


class StorageAuthorityAdapter:
    """Project assignment state without private write-grant details."""

    def __init__(self, store: object | None, *, internal_session_id: str) -> None:
        self._store = store
        self._internal_session_id = internal_session_id

    def snapshot(self) -> StorageAuthoritySnapshot:
        if self._store is None:
            return StorageAuthoritySnapshot(False, "storage-authority-unavailable")
        try:
            snapshot = self._store.snapshot(self._internal_session_id)
            groups: list[StorageGroupRecord] = []
            roles: dict[str, set[str]] = {}
            active_groups = set(_value(snapshot, "leader_grants", {}) or {})
            for group in _sequence(_value(snapshot, "groups", {})):
                group_id = _public_text(_value(group, "group_id"), "unknown-group")
                primary = _optional_public_text(_value(group, "primary_provider_id"))
                replicas = tuple(
                    _public_text(item, "unknown-provider")
                    for item in _sequence(_value(group, "replica_provider_ids", ()))
                )
                if primary is not None:
                    roles.setdefault(primary, set()).add("primary")
                for replica in replicas:
                    roles.setdefault(replica, set()).add("replica")
                groups.append(
                    StorageGroupRecord(
                        group_id=group_id,
                        primary_provider_id=primary,
                        replica_provider_ids=replicas,
                        leader_active=group_id in active_groups,
                    )
                )
            providers: list[StorageProviderRecord] = []
            for provider in _sequence(_value(snapshot, "providers", {})):
                provider_id = _public_text(
                    _value(provider, "provider_id"),
                    "unknown-provider",
                )
                providers.append(
                    StorageProviderRecord(
                        provider_id=provider_id,
                        node_id=_public_text(_value(provider, "node_id"), "unknown-device"),
                        protocol=_public_text(_value(provider, "protocol"), "unknown-protocol"),
                        protocol_version=_public_text(
                            _value(provider, "protocol_version"),
                            "unknown",
                        ),
                        status=_public_text(_value(provider, "status"), "unknown"),
                        authorized=bool(_value(provider, "authorized", False)),
                        assignable=bool(_value(provider, "assignable", False)),
                        roles=tuple(sorted(roles.get(provider_id, ()))),
                    )
                )
            revision = _value(snapshot, "revision")
            return StorageAuthoritySnapshot(
                True,
                "current",
                revision if isinstance(revision, int) else None,
                tuple(sorted(providers, key=lambda item: item.provider_id)),
                tuple(sorted(groups, key=lambda item: item.group_id)),
            )
        except Exception as exc:  # noqa: BLE001
            return StorageAuthoritySnapshot(
                False,
                safe_reason_code(
                    getattr(exc, "code", None),
                    "storage-projection-failed",
                ),
            )


@dataclass(frozen=True)
class JobRecord:
    job_id: str
    capability_type: str
    status: str
    provider_id: str | None
    attempt_count: int
    updated_at: datetime | None
    reason_code: str | None


@dataclass(frozen=True)
class JobAuthoritySnapshot:
    available: bool
    reason_code: str
    jobs: tuple[JobRecord, ...] = ()


class JobAuthorityAdapter:
    """Project durable job snapshots supplied by the existing lifecycle store."""

    def __init__(
        self,
        snapshot_supplier: Callable[[], Iterable[object]] | None,
    ) -> None:
        self._snapshot_supplier = snapshot_supplier

    def snapshot(self) -> JobAuthoritySnapshot:
        if self._snapshot_supplier is None:
            return JobAuthoritySnapshot(False, "job-authority-unavailable")
        try:
            jobs: list[JobRecord] = []
            for snapshot in self._snapshot_supplier():
                job = _value(snapshot, "job", snapshot)
                ownership = _value(snapshot, "ownership")
                attempts = _sequence(_value(job, "attempts", ()))
                latest = attempts[-1] if attempts else None
                reason = _value(latest, "error_code") if latest is not None else None
                jobs.append(
                    JobRecord(
                        job_id=_public_text(_value(job, "job_id"), "unknown-job"),
                        capability_type=_public_text(
                            _value(
                                job,
                                "capability_type",
                                _value(job, "capability_id", "unknown-capability"),
                            ),
                            "unknown-capability",
                        ),
                        status=_enum_value(_value(job, "status"), "unknown"),
                        provider_id=_optional_public_text(
                            _value(ownership, "owner_provider_id")
                        ),
                        attempt_count=len(attempts),
                        updated_at=_timestamp(_value(snapshot, "updated_at")),
                        reason_code=(
                            None if reason is None else safe_reason_code(reason)
                        ),
                    )
                )
            jobs.sort(
                key=lambda item: (
                    item.updated_at or datetime.min.replace(tzinfo=timezone.utc),
                    item.job_id,
                ),
                reverse=True,
            )
            return JobAuthoritySnapshot(True, "current", tuple(jobs))
        except Exception as exc:  # noqa: BLE001
            return JobAuthoritySnapshot(
                False,
                safe_reason_code(
                    getattr(exc, "code", None),
                    "job-projection-failed",
                ),
            )
