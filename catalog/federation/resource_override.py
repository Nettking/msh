"""Audited operator overrides for bounded host-resource policy.

An override is a policy authority, not a second resource authority. All actual
emergency work still enters ``PROCESS_RESOURCE_ADMISSION``. The only exception
is a one-transaction lease that can pass the non-critical PRESSURE gate after
the shared controller has measured every backing resource. The lease cannot
pass CRITICAL, exceed its byte/inode caps, or survive process restart.

Retention and maintenance overrides are durable, exact-scope policy records.
They do not invent a retention policy or delete data; a store must explicitly
resolve and apply its named policy. This keeps an operator's decision visible
and auditable without silently changing product semantics for any cumulative
store that still needs a product decision.
"""

from __future__ import annotations

import json
import os
import stat
import tempfile
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any

from .errors import AuthorizationError
from .host_resources import measure_filesystem
from .process_resource_admission import (
    _EMERGENCY_LEASE_CREATION_TOKEN,
    PROCESS_RESOURCE_ADMISSION,
    EmergencyAdmissionLease,
    SerializedProcessResourceAdmission,
)

RESOURCE_OVERRIDE_PERMISSION = "resource.override"
ADMISSION_EMERGENCY = "admission-emergency"
RETENTION_POLICY = "retention-policy"
MAINTENANCE_POLICY = "maintenance-policy"
_AUDIT_SCHEMA = "fcp-resource-override-audit-v1"
_MAX_ACTOR_LENGTH = 256
_MAX_OPERATION_LENGTH = 256
_MAX_SCOPE_LENGTH = 512
_MAX_REASON_LENGTH = 4096
_MAX_POLICY_NAME_LENGTH = 256
_MAX_POLICY_VALUE_BYTES = 16 * 1024
_DEFAULT_MAX_EMERGENCY_LEASE_SECONDS = 15 * 60


@dataclass(frozen=True)
class OperatorPrincipal:
    """Authorization facts supplied by the already-authenticated app layer."""

    actor_id: str
    permissions: frozenset[str] = frozenset()
    is_admin: bool = False

    @property
    def may_override_resources(self) -> bool:
        return self.is_admin or RESOURCE_OVERRIDE_PERMISSION in self.permissions


@dataclass(frozen=True)
class OverrideAuditRecord:
    """Durable issuance/revocation evidence for one override decision."""

    override_id: str
    kind: str
    actor_id: str
    target: str
    scope: str
    reason: str
    issued_at: datetime
    expires_at: datetime
    max_bytes: int | None = None
    max_inodes: int | None = None
    resource_ids: tuple[str, ...] = ()
    value: dict[str, Any] | None = None
    revoked_at: datetime | None = None
    revoked_by: str | None = None

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "override_id": self.override_id,
            "kind": self.kind,
            "actor_id": self.actor_id,
            "target": self.target,
            "scope": self.scope,
            "reason": self.reason,
            "issued_at": self.issued_at.astimezone(timezone.utc).isoformat(),
            "expires_at": self.expires_at.astimezone(timezone.utc).isoformat(),
            "max_bytes": self.max_bytes,
            "max_inodes": self.max_inodes,
            "resource_ids": list(self.resource_ids),
            "value": self.value,
            "revoked_at": (
                self.revoked_at.astimezone(timezone.utc).isoformat()
                if self.revoked_at is not None
                else None
            ),
            "revoked_by": self.revoked_by,
        }
        return payload


def _required_text(value: object, *, field: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be text")
    normalized = value.strip()
    if not normalized or len(normalized) > maximum:
        raise ValueError(f"{field} must be non-empty and at most {maximum} characters")
    return normalized


def _bounded_non_negative(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _aware_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _parse_timestamp(value: object, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be an ISO timestamp")
    try:
        return _aware_utc(datetime.fromisoformat(value), field=field)
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO timestamp") from exc


def _plain_directory(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return False
    return stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode) and not bool(
        getattr(metadata, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    )


def _plain_file(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return False
    return stat.S_ISREG(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode) and not bool(
        getattr(metadata, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    )


def _identity(path: Path) -> tuple[int, int]:
    metadata = path.lstat()
    return int(getattr(metadata, "st_dev", 0)), int(getattr(metadata, "st_ino", 0))


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _record_from_dict(payload: object) -> OverrideAuditRecord:
    if not isinstance(payload, dict):
        raise TypeError("override audit record must be an object")
    override_id = _required_text(payload.get("override_id"), field="override_id", maximum=128)
    kind = _required_text(payload.get("kind"), field="kind", maximum=64)
    if kind not in {ADMISSION_EMERGENCY, RETENTION_POLICY, MAINTENANCE_POLICY}:
        raise ValueError("override audit record kind is unknown")
    actor_id = _required_text(payload.get("actor_id"), field="actor_id", maximum=_MAX_ACTOR_LENGTH)
    target = _required_text(payload.get("target"), field="target", maximum=_MAX_OPERATION_LENGTH)
    scope = _required_text(payload.get("scope"), field="scope", maximum=_MAX_SCOPE_LENGTH)
    reason = _required_text(payload.get("reason"), field="reason", maximum=_MAX_REASON_LENGTH)
    issued_at = _parse_timestamp(payload.get("issued_at"), field="issued_at")
    expires_at = _parse_timestamp(payload.get("expires_at"), field="expires_at")
    if expires_at <= issued_at:
        raise ValueError("override expiry must follow issuance")
    max_bytes = payload.get("max_bytes")
    max_inodes = payload.get("max_inodes")
    if max_bytes is not None:
        max_bytes = _bounded_non_negative(max_bytes, field="max_bytes")
    if max_inodes is not None:
        max_inodes = _bounded_non_negative(max_inodes, field="max_inodes")
    raw_resource_ids = payload.get("resource_ids", [])
    if not isinstance(raw_resource_ids, list) or any(
        not isinstance(value, str) or not value for value in raw_resource_ids
    ):
        raise ValueError("resource_ids must be a list of non-empty strings")
    resource_ids = tuple(sorted(set(raw_resource_ids)))
    value = payload.get("value")
    if value is not None and not isinstance(value, dict):
        raise ValueError("override policy value must be an object")
    revoked_at = payload.get("revoked_at")
    parsed_revoked_at = (
        _parse_timestamp(revoked_at, field="revoked_at")
        if revoked_at is not None
        else None
    )
    revoked_by = payload.get("revoked_by")
    if revoked_by is not None:
        revoked_by = _required_text(revoked_by, field="revoked_by", maximum=_MAX_ACTOR_LENGTH)
    if (parsed_revoked_at is None) != (revoked_by is None):
        raise ValueError("revocation fields must be provided together")
    return OverrideAuditRecord(
        override_id=override_id,
        kind=kind,
        actor_id=actor_id,
        target=target,
        scope=scope,
        reason=reason,
        issued_at=issued_at,
        expires_at=expires_at,
        max_bytes=max_bytes,
        max_inodes=max_inodes,
        resource_ids=resource_ids,
        value=dict(value) if value is not None else None,
        revoked_at=parsed_revoked_at,
        revoked_by=revoked_by,
    )


class ResourceOverrideAuthority:
    """Issue and resolve scoped, expiring, auditable operator overrides."""

    def __init__(
        self,
        audit_path: Path | str,
        *,
        resource_admission: SerializedProcessResourceAdmission | None = None,
        clock: Any | None = None,
        max_emergency_lease_seconds: float = _DEFAULT_MAX_EMERGENCY_LEASE_SECONDS,
    ) -> None:
        self.audit_path = Path(audit_path)
        self.resource_admission = resource_admission or PROCESS_RESOURCE_ADMISSION
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        if (
            isinstance(max_emergency_lease_seconds, bool)
            or not isinstance(max_emergency_lease_seconds, int | float)
            or max_emergency_lease_seconds <= 0
        ):
            raise ValueError("emergency lease duration must be positive")
        self.max_emergency_lease_seconds = float(max_emergency_lease_seconds)
        self._lock = RLock()
        self._records = self._load_records()
        self._live_leases: dict[str, EmergencyAdmissionLease] = {}

    def _now(self) -> datetime:
        return _aware_utc(self.clock(), field="clock")

    def _load_records(self) -> list[OverrideAuditRecord]:
        try:
            self.audit_path.lstat()
        except FileNotFoundError:
            return []
        except OSError as exc:
            raise ValueError("resource override audit path could not be inspected") from exc
        if not _plain_file(self.audit_path):
            raise ValueError("resource override audit path is not a plain file")
        try:
            payload = json.loads(self.audit_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ValueError("resource override audit file is unreadable") from exc
        if not isinstance(payload, dict) or payload.get("schema") != _AUDIT_SCHEMA:
            raise ValueError("resource override audit schema is invalid")
        raw_records = payload.get("records")
        if not isinstance(raw_records, list):
            raise TypeError("resource override audit records are invalid")
        records = [_record_from_dict(item) for item in raw_records]
        if len({record.override_id for record in records}) != len(records):
            raise ValueError("resource override audit contains duplicate ids")
        return records

    def _authorize(self, principal: OperatorPrincipal) -> str:
        actor_id = _required_text(
            principal.actor_id, field="actor_id", maximum=_MAX_ACTOR_LENGTH
        )
        if not principal.may_override_resources:
            raise AuthorizationError(
                "resource-override-forbidden",
                "the authenticated operator lacks resource override authority",
                field="actor_id",
            )
        return actor_id

    def _persist(self, records: Iterable[OverrideAuditRecord]) -> None:
        parent = self.audit_path.parent
        if not _plain_directory(parent):
            raise OSError("resource override audit parent must be an existing plain directory")
        payload = {
            "schema": _AUDIT_SCHEMA,
            "records": [record.to_dict() for record in records],
        }
        encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode(
            "utf-8"
        )
        if len(encoded) > _MAX_POLICY_VALUE_BYTES * 4:
            raise ValueError("resource override audit is too large")

        # The reservation precedes creation of the replacement inode. The
        # estimate covers the complete replacement payload and the temporary
        # inode; the controller measures the actual parent backing resource.
        with self.resource_admission.reserve(
            parent,
            bytes_required=len(encoded),
            inodes_required=2,
            operation="resource-override-audit",
        ):
            parent_identity = _identity(parent)
            if self.audit_path.is_symlink():
                raise OSError("resource override audit path cannot be a symlink")
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{self.audit_path.name}.",
                suffix=".tmp",
                dir=str(parent),
            )
            temporary = Path(temporary_name)
            temporary_identity = _identity(temporary)
            try:
                if _identity(parent) != parent_identity:
                    raise OSError("resource override audit parent identity changed")
                if not _plain_file(temporary):
                    raise OSError("resource override audit temporary is not a plain file")
                parent_measurement = measure_filesystem(parent)
                temporary_measurement = measure_filesystem(temporary)
                if (
                    not parent_measurement.available
                    or not temporary_measurement.available
                    or parent_measurement.resource_id != temporary_measurement.resource_id
                ):
                    raise OSError("resource override audit backing resource changed")
                with os.fdopen(descriptor, "wb") as handle:
                    descriptor = -1
                    handle.write(encoded)
                    handle.flush()
                    os.fsync(handle.fileno())
                if _identity(parent) != parent_identity or self.audit_path.is_symlink():
                    raise OSError("resource override audit publication identity changed")
                os.replace(temporary, self.audit_path)
                _fsync_directory(parent)
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
                try:
                    if _plain_file(temporary) and _identity(temporary) == temporary_identity:
                        temporary.unlink()
                except OSError:
                    pass

    def _append(self, record: OverrideAuditRecord) -> None:
        previous = self._records
        self._records = [*previous, record]
        try:
            self._persist(self._records)
        except BaseException:
            self._records = previous
            raise

    def issue_emergency_admission(
        self,
        *,
        principal: OperatorPrincipal,
        operation: str,
        resource_ids: Iterable[str],
        max_bytes: int,
        max_inodes: int,
        expires_at: datetime,
        reason: str,
    ) -> EmergencyAdmissionLease:
        """Issue a one-shot lease for one exact operation/resource set."""

        actor_id = self._authorize(principal)
        operation = _required_text(
            operation, field="operation", maximum=_MAX_OPERATION_LENGTH
        )
        reason = _required_text(reason, field="reason", maximum=_MAX_REASON_LENGTH)
        resources = frozenset(
            _required_text(value, field="resource_id", maximum=_MAX_SCOPE_LENGTH)
            for value in resource_ids
        )
        if not resources:
            raise ValueError("emergency admission requires an exact resource set")
        max_bytes = _bounded_non_negative(max_bytes, field="max_bytes")
        max_inodes = _bounded_non_negative(max_inodes, field="max_inodes")
        if max_bytes == 0 and max_inodes == 0:
            raise ValueError("emergency admission requires a non-zero byte or inode cap")
        issued_at = self._now()
        expires_at = _aware_utc(expires_at, field="expires_at")
        if expires_at <= issued_at:
            raise ValueError("emergency admission expiry must be in the future")
        if (
            expires_at.timestamp() - issued_at.timestamp()
            > self.max_emergency_lease_seconds
        ):
            raise ValueError("emergency admission expiry exceeds the configured lease bound")
        override_id = uuid.uuid4().hex
        record = OverrideAuditRecord(
            override_id=override_id,
            kind=ADMISSION_EMERGENCY,
            actor_id=actor_id,
            target=operation,
            scope="exact-resource-set",
            reason=reason,
            issued_at=issued_at,
            expires_at=expires_at,
            max_bytes=max_bytes,
            max_inodes=max_inodes,
            resource_ids=tuple(sorted(resources)),
        )
        with self._lock:
            self._append(record)
            lease = EmergencyAdmissionLease(
                override_id=override_id,
                actor_id=actor_id,
                operation=operation,
                resource_ids=resources,
                max_bytes=max_bytes,
                max_inodes=max_inodes,
                issued_at=issued_at,
                expires_at=expires_at,
                _creation_token=_EMERGENCY_LEASE_CREATION_TOKEN,
            )
            self._live_leases[override_id] = lease
            return lease

    def issue_policy_override(
        self,
        *,
        principal: OperatorPrincipal,
        kind: str,
        policy_name: str,
        scope: str,
        value: dict[str, Any],
        expires_at: datetime,
        reason: str,
    ) -> OverrideAuditRecord:
        """Record an exact-scope retention or maintenance policy decision."""

        actor_id = self._authorize(principal)
        if kind not in {RETENTION_POLICY, MAINTENANCE_POLICY}:
            raise ValueError("policy override kind is unsupported")
        policy_name = _required_text(
            policy_name, field="policy_name", maximum=_MAX_POLICY_NAME_LENGTH
        )
        scope = _required_text(scope, field="scope", maximum=_MAX_SCOPE_LENGTH)
        reason = _required_text(reason, field="reason", maximum=_MAX_REASON_LENGTH)
        if not isinstance(value, dict):
            raise TypeError("policy override value must be an object")
        try:
            encoded_value = json.dumps(value, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("policy override value must be JSON-serializable") from exc
        if len(encoded_value) > _MAX_POLICY_VALUE_BYTES:
            raise ValueError("policy override value is too large")
        issued_at = self._now()
        expires_at = _aware_utc(expires_at, field="expires_at")
        if expires_at <= issued_at:
            raise ValueError("policy override expiry must be in the future")
        record = OverrideAuditRecord(
            override_id=uuid.uuid4().hex,
            kind=kind,
            actor_id=actor_id,
            target=policy_name,
            scope=scope,
            reason=reason,
            issued_at=issued_at,
            expires_at=expires_at,
            value=json.loads(encoded_value),
        )
        with self._lock:
            self._append(record)
            return record

    def revoke(self, *, principal: OperatorPrincipal, override_id: str) -> OverrideAuditRecord:
        actor_id = self._authorize(principal)
        override_id = _required_text(override_id, field="override_id", maximum=128)
        now = self._now()
        with self._lock:
            current = next(
                (record for record in self._records if record.override_id == override_id),
                None,
            )
            if current is None:
                raise ValueError("resource override does not exist")
            if current.revoked_at is not None:
                return current
            replacement = OverrideAuditRecord(
                override_id=current.override_id,
                kind=current.kind,
                actor_id=current.actor_id,
                target=current.target,
                scope=current.scope,
                reason=current.reason,
                issued_at=current.issued_at,
                expires_at=current.expires_at,
                max_bytes=current.max_bytes,
                max_inodes=current.max_inodes,
                resource_ids=current.resource_ids,
                value=current.value,
                revoked_at=now,
                revoked_by=actor_id,
            )
            previous = self._records
            self._records = [
                replacement if record.override_id == override_id else record
                for record in previous
            ]
            try:
                self._persist(self._records)
            except BaseException:
                self._records = previous
                raise
            lease = self._live_leases.get(override_id)
            if lease is not None:
                lease.revoke()
            return replacement

    def resolve_policy(
        self,
        *,
        kind: str,
        policy_name: str,
        scope: str,
        default: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        """Return the latest active exact-scope policy, or its default."""

        if kind not in {RETENTION_POLICY, MAINTENANCE_POLICY}:
            raise ValueError("policy override kind is unsupported")
        policy_name = _required_text(
            policy_name, field="policy_name", maximum=_MAX_POLICY_NAME_LENGTH
        )
        scope = _required_text(scope, field="scope", maximum=_MAX_SCOPE_LENGTH)
        current = _aware_utc(now or self._now(), field="now")
        with self._lock:
            matches = [
                record
                for record in self._records
                if record.kind == kind
                and record.target == policy_name
                and record.scope == scope
                and record.revoked_at is None
                and record.issued_at <= current < record.expires_at
                and record.value is not None
            ]
            if not matches:
                return dict(default) if default is not None else None
            selected = max(matches, key=lambda record: (record.issued_at, record.override_id))
            return dict(selected.value or {})

    def audit_records(self) -> tuple[OverrideAuditRecord, ...]:
        with self._lock:
            return tuple(self._records)


__all__ = [
    "ADMISSION_EMERGENCY",
    "MAINTENANCE_POLICY",
    "RESOURCE_OVERRIDE_PERMISSION",
    "RETENTION_POLICY",
    "OperatorPrincipal",
    "OverrideAuditRecord",
    "ResourceOverrideAuthority",
]
