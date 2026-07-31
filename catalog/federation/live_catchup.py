"""Live relay-first catch-up for an unassigned former storage primary.

F4.1 repairs data only.  The returning provider stays unassigned and cannot
participate in acknowledgement policy or receive write authority.  Every repair
request is authenticated as coming from the pinned control authority and is
revalidated against the node's latest signed control state before local ingest.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .errors import FederationValidationError
from .live_failover import (
    LIVE_FAILOVER_SCHEMA,
    LiveFailoverRecord,
    LiveFailoverStore,
    build_live_replica_report,
)
from .manifest import AuthoritativeStorageManifest, ManifestItem, ManifestItemKind
from .reporting import StorageReplicaAssessment, StorageReplicaReport
from .storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    BatchIngestResult,
    StorageErrorCode,
    StorageOperation,
    StorageRequestEnvelope,
    StorageResponseEnvelope,
    WriteAuthority,
)

LIVE_CATCHUP_KIND = "msh-storage-catchup-v1"
LIVE_CATCHUP_SCHEMA = "msh.live_storage_catchup.v1"
LIVE_CATCHUP_RESULT_SCHEMA = "msh.live_storage_catchup_result.v1"

_STATE_PLANNED = "planned"
_STATE_TRANSFERRING = "transferring"
_STATE_VERIFYING = "verifying"
_STATE_RETRYABLE = "retryable"
_STATE_CAUGHT_UP = "caught-up"
_STATE_OPERATOR = "operator-attention"
_VALID_STATES = {
    _STATE_PLANNED,
    _STATE_TRANSFERRING,
    _STATE_VERIFYING,
    _STATE_RETRYABLE,
    _STATE_CAUGHT_UP,
    _STATE_OPERATOR,
}

_ITEM_PRESENT = "present"
_ITEM_MISSING = "missing"
_ITEM_DELIVERED = "delivered"
_ITEM_VERIFIED = "verified"
_ITEM_CONFLICT = "conflict"
_VALID_ITEM_STATES = {
    _ITEM_PRESENT,
    _ITEM_MISSING,
    _ITEM_DELIVERED,
    _ITEM_VERIFIED,
    _ITEM_CONFLICT,
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _utc(value: datetime | str, field: str = "timestamp") -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-timestamp", field, "must be RFC 3339"
            ) from exc
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise FederationValidationError(
            "invalid-timestamp", field, "must be timezone-aware"
        )
    return value.astimezone(timezone.utc)


def _stamp(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-live-catchup-field", field, "must be non-empty text"
        )
    return value


def _uint(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-live-catchup-counter", field, "must be non-negative"
        )
    return value


def _json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _success(
    envelope: StorageRequestEnvelope,
    result: dict[str, Any],
) -> StorageResponseEnvelope:
    return StorageResponseEnvelope(
        request_id=envelope.request_id,
        protocol=STORAGE_PROTOCOL,
        protocol_version=envelope.protocol_version,
        ok=True,
        result=result,
    )


def _identity_dict(identity: Any) -> dict[str, Any]:
    return {
        "dataset_id": identity.dataset_id,
        "dataset_schema_name": identity.dataset_schema_name,
        "dataset_schema_version": identity.dataset_schema_version,
        "batch_id": identity.batch_id,
        "idempotency_key": identity.idempotency_key,
        "content_hash": identity.content_hash,
    }


def dispatch_live_catchup_request(
    service: Any,
    envelope: StorageRequestEnvelope,
) -> StorageResponseEnvelope | None:
    """Handle coordinator-authorized recovery operations on a live provider.

    The hosting ``PhaseDStorageService`` calls this before normal routing.  The
    method returns ``None`` for unrelated traffic so existing behavior remains
    unchanged.
    """

    context = envelope.authorization_context
    if context.get("kind") != LIVE_CATCHUP_KIND:
        return None
    authority_node_id = getattr(service, "recovery_authority_node_id", None)
    if not isinstance(authority_node_id, str) or not authority_node_id:
        raise FederationValidationError(
            "live-catchup-disabled",
            "recovery_authority_node_id",
            "provider has no pinned recovery authority",
        )
    if envelope.actor_node_id != authority_node_id:
        raise FederationValidationError(
            StorageErrorCode.UNAUTHORIZED.value,
            "actor_node_id",
            "recovery request did not come from the pinned control authority",
        )
    if context.get("provider_id") != service.provider_id:
        raise FederationValidationError(
            StorageErrorCode.UNAUTHORIZED.value,
            "provider_id",
            "recovery route targets another provider",
        )
    action = context.get("action")
    if action == "inspect":
        if envelope.operation is not StorageOperation.BATCH_READ:
            raise FederationValidationError(
                "invalid-live-catchup-operation",
                "operation",
                "inspect requires batch.read",
            )
        group_id = _text(envelope.payload.get("group_id"), "group_id")
        batch_id = _text(envelope.payload.get("batch_id"), "batch_id")
        identity = service.provider.committed_identity(
            session_id=envelope.session_id,
            group_id=group_id,
            batch_id=batch_id,
        )
        if identity is None:
            raise FederationValidationError(
                StorageErrorCode.BATCH_NOT_FOUND.value,
                "batch_id",
                "batch does not exist",
            )
        content = service.provider.read(
            session_id=envelope.session_id,
            group_id=group_id,
            batch_id=batch_id,
        )
        if content is None:
            raise FederationValidationError(
                "live-catchup-content-missing",
                "batch_id",
                "committed identity exists without readable content",
            )
        return _success(
            envelope,
            {"identity": _identity_dict(identity), "content": content},
        )
    if action == "report":
        if envelope.operation is not StorageOperation.HEALTH:
            raise FederationValidationError(
                "invalid-live-catchup-operation",
                "operation",
                "report requires storage.health",
            )
        frame = envelope.payload.get("manifest_frame")
        if not isinstance(frame, str):
            raise FederationValidationError(
                "invalid-live-catchup-manifest",
                "manifest_frame",
                "report requires a JSON manifest frame",
            )
        try:
            manifest = AuthoritativeStorageManifest.from_dict(json.loads(frame))
        except json.JSONDecodeError as exc:
            raise FederationValidationError(
                "invalid-live-catchup-manifest",
                "manifest_frame",
                "manifest frame is not valid JSON",
            ) from exc
        if manifest.session_id != envelope.session_id:
            raise FederationValidationError(
                "live-catchup-session-mismatch",
                "session_id",
                "manifest belongs to another session",
            )
        report_revision = envelope.payload.get("report_revision")
        if (
            isinstance(report_revision, bool)
            or not isinstance(report_revision, int)
            or report_revision < 1
        ):
            raise FederationValidationError(
                "invalid-live-report-revision",
                "report_revision",
                "must be a positive integer",
            )
        report = build_live_replica_report(
            service.provider,
            manifest,
            provider_id=service.provider_id,
            report_revision=report_revision,
            reported_at=envelope.payload.get("reported_at"),
        )
        return _success(envelope, {"report": report.to_dict()})
    if action != "ingest" or envelope.operation is not StorageOperation.BATCH_INGEST:
        raise FederationValidationError(
            "invalid-live-catchup-operation",
            "action",
            "recovery supports inspect, report, and batch ingest only",
        )

    request = BatchIngestRequest.from_dict(envelope.payload)
    request.validate_content_hash()
    if request.authority.session_id != envelope.session_id:
        raise FederationValidationError(
            "live-catchup-session-mismatch",
            "session_id",
            "envelope and write authority session differ",
        )
    group_id = _text(context.get("group_id"), "group_id")
    source_provider_id = _text(
        context.get("source_provider_id"), "source_provider_id"
    )
    source_node_id = _text(context.get("source_node_id"), "source_node_id")
    if request.authority.group_id != group_id:
        raise FederationValidationError(
            "live-catchup-group-mismatch",
            "group_id",
            "request authority and recovery route differ",
        )
    snapshot = service.control_plane.snapshot(envelope.session_id)
    assignment = snapshot.groups.get(group_id)
    if assignment is None or assignment.primary_provider_id != source_provider_id:
        raise FederationValidationError(
            "live-catchup-source-changed",
            "source_provider_id",
            "recovery source is no longer the assigned primary",
        )
    if (
        service.provider_id == assignment.primary_provider_id
        or service.provider_id in assignment.replica_provider_ids
    ):
        raise FederationValidationError(
            "live-catchup-target-assigned",
            "provider_id",
            "F4.1 target must remain unassigned during repair",
        )
    source = snapshot.providers.get(source_provider_id)
    if source is None or source.node_id != source_node_id:
        raise FederationValidationError(
            "live-catchup-source-changed",
            "source_node_id",
            "recovery source node differs from signed control",
        )
    grant = snapshot.leader_grants.get(group_id)
    authority = request.authority
    if (
        grant is None
        or grant.get("provider_id") != source_provider_id
        or grant.get("grant_id") != authority.grant_id
        or int(grant.get("term", -1)) != authority.term
        or int(grant.get("fencing_token", -1)) != authority.fencing_token
        or _utc(str(grant.get("lease_expires_at")), "lease_expires_at")
        != authority.lease_expires_at
        or authority.actor_node_id != source_node_id
    ):
        raise FederationValidationError(
            StorageErrorCode.UNKNOWN_GRANT.value,
            "authority",
            "recovery request is not bound to current primary authority",
        )
    if service.clock().astimezone(timezone.utc) >= authority.lease_expires_at:
        raise FederationValidationError(
            StorageErrorCode.LEASE_EXPIRED.value,
            "authority.lease_expires_at",
            "active recovery source lease has expired",
        )
    expected = {
        "item_id": request.batch_id,
        "dataset_id": request.dataset_id,
        "schema_name": request.dataset_schema_name,
        "schema_version": request.dataset_schema_version,
        "idempotency_key": request.idempotency_key,
        "content_hash": request.content_hash,
    }
    for field, value in expected.items():
        if context.get(field) != value:
            raise FederationValidationError(
                "live-catchup-item-mismatch",
                field,
                "recovery context differs from immutable batch identity",
            )
    _text(context.get("recovery_id"), "recovery_id")
    _uint(context.get("manifest_revision"), "manifest_revision")
    _text(context.get("manifest_hash"), "manifest_hash")
    result = service.provider.ingest(request)
    return _success(envelope, result.to_dict())


@dataclass(frozen=True)
class LiveCatchupItem:
    item_id: str
    dataset_id: str
    schema_name: str
    schema_version: int
    idempotency_key: str
    content_hash: str
    status: str
    attempt_count: int = 0
    last_error_code: str | None = None
    last_error_reason: str | None = None

    def __post_init__(self) -> None:
        for field in (
            "item_id",
            "dataset_id",
            "schema_name",
            "idempotency_key",
            "content_hash",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        object.__setattr__(self, "schema_version", _uint(self.schema_version, "schema_version"))
        object.__setattr__(self, "attempt_count", _uint(self.attempt_count, "attempt_count"))
        if self.schema_version < 1:
            raise FederationValidationError(
                "invalid-live-catchup-schema-version",
                "schema_version",
                "must be positive",
            )
        if self.status not in _VALID_ITEM_STATES:
            raise FederationValidationError(
                "invalid-live-catchup-item-state", "status", "unknown item state"
            )
        if (self.last_error_code is None) != (self.last_error_reason is None):
            raise FederationValidationError(
                "invalid-live-catchup-error",
                "last_error_code",
                "error code and reason must appear together",
            )

    @classmethod
    def from_manifest(cls, item: ManifestItem, *, status: str) -> LiveCatchupItem:
        if item.kind is not ManifestItemKind.BATCH or item.idempotency_key is None:
            raise FederationValidationError(
                "live-catchup-item-unsupported",
                "item_id",
                "F4.1 currently repairs committed batch items only",
            )
        return cls(
            item_id=item.item_id,
            dataset_id=item.dataset_id,
            schema_name=item.schema_name,
            schema_version=item.schema_version,
            idempotency_key=item.idempotency_key,
            content_hash=item.content_hash,
            status=status,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "dataset_id": self.dataset_id,
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "idempotency_key": self.idempotency_key,
            "content_hash": self.content_hash,
            "status": self.status,
            "attempt_count": self.attempt_count,
            "last_error_code": self.last_error_code,
            "last_error_reason": self.last_error_reason,
        }

    @classmethod
    def from_dict(cls, value: Any) -> LiveCatchupItem:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-live-catchup-item", "item", "must be an object"
            )
        return cls(**value)


@dataclass(frozen=True)
class LiveCatchupRecord:
    schema: str
    recovery_id: str
    session_id: str
    group_id: str
    failover_id: str
    source_provider_id: str
    source_node_id: str
    returning_provider_id: str
    returning_node_id: str
    manifest_revision: int
    manifest_hash: str
    grant_id: str
    term: int
    fencing_token: int
    lease_expires_at: datetime
    initial_report_revision: int
    initial_report_hash: str
    state: str
    items: tuple[LiveCatchupItem, ...]
    final_report_revision: int | None
    final_report_hash: str | None
    latest_error_code: str | None
    latest_error_reason: str | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if self.schema != LIVE_CATCHUP_SCHEMA:
            raise FederationValidationError(
                "unsupported-live-catchup", "schema", f"expected {LIVE_CATCHUP_SCHEMA}"
            )
        for field in (
            "recovery_id",
            "session_id",
            "group_id",
            "failover_id",
            "source_provider_id",
            "source_node_id",
            "returning_provider_id",
            "returning_node_id",
            "manifest_hash",
            "grant_id",
            "initial_report_hash",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        for field in (
            "manifest_revision",
            "term",
            "fencing_token",
            "initial_report_revision",
        ):
            object.__setattr__(self, field, _uint(getattr(self, field), field))
        if self.state not in _VALID_STATES:
            raise FederationValidationError(
                "invalid-live-catchup-state", "state", "unknown catch-up state"
            )
        ordered = tuple(sorted(self.items, key=lambda item: item.item_id))
        if len(ordered) != len({item.item_id for item in ordered}):
            raise FederationValidationError(
                "duplicate-live-catchup-item", "items", "item ids must be unique"
            )
        object.__setattr__(self, "items", ordered)
        object.__setattr__(self, "lease_expires_at", _utc(self.lease_expires_at))
        object.__setattr__(self, "created_at", _utc(self.created_at))
        object.__setattr__(self, "updated_at", _utc(self.updated_at))
        if (self.final_report_revision is None) != (self.final_report_hash is None):
            raise FederationValidationError(
                "invalid-live-catchup-final-report",
                "final_report_revision",
                "final report revision and hash must appear together",
            )
        if self.final_report_revision is not None:
            object.__setattr__(
                self,
                "final_report_revision",
                _uint(self.final_report_revision, "final_report_revision"),
            )
            object.__setattr__(
                self,
                "final_report_hash",
                _text(self.final_report_hash, "final_report_hash"),
            )
        if (self.latest_error_code is None) != (self.latest_error_reason is None):
            raise FederationValidationError(
                "invalid-live-catchup-error",
                "latest_error_code",
                "error code and reason must appear together",
            )

    def immutable_binding(self) -> tuple[Any, ...]:
        return (
            self.schema,
            self.recovery_id,
            self.session_id,
            self.group_id,
            self.failover_id,
            self.source_provider_id,
            self.source_node_id,
            self.returning_provider_id,
            self.returning_node_id,
            self.manifest_revision,
            self.manifest_hash,
            self.grant_id,
            self.term,
            self.fencing_token,
            self.lease_expires_at,
            self.initial_report_revision,
            self.initial_report_hash,
            tuple(
                (
                    item.item_id,
                    item.dataset_id,
                    item.schema_name,
                    item.schema_version,
                    item.idempotency_key,
                    item.content_hash,
                )
                for item in self.items
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "recovery_id": self.recovery_id,
            "session_id": self.session_id,
            "group_id": self.group_id,
            "failover_id": self.failover_id,
            "source_provider_id": self.source_provider_id,
            "source_node_id": self.source_node_id,
            "returning_provider_id": self.returning_provider_id,
            "returning_node_id": self.returning_node_id,
            "manifest_revision": self.manifest_revision,
            "manifest_hash": self.manifest_hash,
            "grant_id": self.grant_id,
            "term": self.term,
            "fencing_token": self.fencing_token,
            "lease_expires_at": _stamp(self.lease_expires_at),
            "initial_report_revision": self.initial_report_revision,
            "initial_report_hash": self.initial_report_hash,
            "state": self.state,
            "items": [item.to_dict() for item in self.items],
            "final_report_revision": self.final_report_revision,
            "final_report_hash": self.final_report_hash,
            "latest_error_code": self.latest_error_code,
            "latest_error_reason": self.latest_error_reason,
            "created_at": _stamp(self.created_at),
            "updated_at": _stamp(self.updated_at),
        }

    @classmethod
    def from_dict(cls, value: Any) -> LiveCatchupRecord:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-live-catchup", "record", "must be an object"
            )
        restored = dict(value)
        restored["items"] = tuple(
            LiveCatchupItem.from_dict(item) for item in value.get("items", ())
        )
        return cls(**restored)


class LiveCatchupStore:
    """Durable, replay-safe F4.1 progress in one canonical JSON record."""

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """CREATE TABLE IF NOT EXISTS storage_live_catchups (
                    recovery_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    returning_provider_id TEXT NOT NULL,
                    manifest_hash TEXT NOT NULL,
                    state TEXT NOT NULL,
                    record_json TEXT NOT NULL CHECK(json_valid(record_json)),
                    updated_at TEXT NOT NULL,
                    UNIQUE(
                        session_id, group_id, returning_provider_id, manifest_hash
                    )
                )"""
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def get(self, recovery_id: str) -> LiveCatchupRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT record_json FROM storage_live_catchups WHERE recovery_id=?",
                (recovery_id,),
            ).fetchone()
        if row is None:
            return None
        try:
            return LiveCatchupRecord.from_dict(json.loads(row["record_json"]))
        except (json.JSONDecodeError, FederationValidationError) as exc:
            raise FederationValidationError(
                "corrupt-live-catchup",
                "record_json",
                "persisted catch-up record is malformed",
            ) from exc

    def save(self, record: LiveCatchupRecord) -> LiveCatchupRecord:
        existing = self.get(record.recovery_id)
        if (
            existing is not None
            and existing.immutable_binding() != record.immutable_binding()
        ):
            raise FederationValidationError(
                "conflicting-live-catchup",
                "recovery_id",
                "durable recovery identity was reused with different bindings",
            )
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO storage_live_catchups
                   (recovery_id, session_id, group_id, returning_provider_id,
                    manifest_hash, state, record_json, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(recovery_id) DO UPDATE SET
                     state=excluded.state,
                     record_json=excluded.record_json,
                     updated_at=excluded.updated_at""",
                (
                    record.recovery_id,
                    record.session_id,
                    record.group_id,
                    record.returning_provider_id,
                    record.manifest_hash,
                    record.state,
                    _json(record.to_dict()),
                    _stamp(record.updated_at),
                ),
            )
        return record


@dataclass(frozen=True)
class LiveCatchupResult:
    status: str
    code: str
    reason: str
    recovery_id: str | None
    attempted: int
    delivered: int
    reconciled: int
    record: LiveCatchupRecord | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": LIVE_CATCHUP_RESULT_SCHEMA,
            "status": self.status,
            "code": self.code,
            "reason": self.reason,
            "recovery_id": self.recovery_id,
            "attempted": self.attempted,
            "delivered": self.delivered,
            "reconciled": self.reconciled,
            "record": None if self.record is None else self.record.to_dict(),
        }


class LiveFormerPrimaryCatchupCoordinator:
    """Repair an online former primary without assigning it any active role."""

    def __init__(
        self,
        *,
        session_coordinator: Any,
        control_plane: Any,
        failover_store: LiveFailoverStore,
        transport: Any,
        credentials: Any,
        catchup_store: LiveCatchupStore,
        session_id: str,
        clock: Any = _now,
    ) -> None:
        self.session_coordinator = session_coordinator
        self.control_plane = control_plane
        self.failover_store = failover_store
        self.transport = transport
        self.credentials = credentials
        self.catchup_store = catchup_store
        self.session_id = _text(session_id, "session_id")
        self.clock = clock

    async def run_once(
        self,
        *,
        group_id: str,
        returning_provider_id: str,
        limit: int = 100,
    ) -> LiveCatchupResult:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise FederationValidationError(
                "invalid-limit", "limit", "must be a positive integer"
            )
        try:
            binding = self._binding(group_id, returning_provider_id)
            recovery_id = self._recovery_id(binding)
            record = self.catchup_store.get(recovery_id)
            if record is None:
                record = await self._plan(recovery_id, binding)
            else:
                self._validate_record(record, binding)
            if record.state == _STATE_CAUGHT_UP:
                return self._result(
                    "caught-up",
                    "live-catchup-complete",
                    "former primary is already complete and remains unassigned",
                    record,
                )
            if record.state == _STATE_OPERATOR:
                return self._result(
                    "operator-attention",
                    record.latest_error_code or "live-catchup-conflict",
                    record.latest_error_reason or "catch-up requires operator attention",
                    record,
                )
            attempted = delivered = reconciled = 0
            pending = tuple(
                item for item in record.items if item.status == _ITEM_MISSING
            )[:limit]
            record = self.catchup_store.save(
                replace(
                    record,
                    state=_STATE_TRANSFERRING if pending else _STATE_VERIFYING,
                    latest_error_code=None,
                    latest_error_reason=None,
                    updated_at=self.clock(),
                )
            )
            for pending_item in pending:
                current = self._item(record, pending_item.item_id)
                inspected = await self._inspect(
                    binding["returning"].node_id,
                    returning_provider_id,
                    group_id,
                    current.item_id,
                )
                if inspected is not None:
                    self._validate_inspection(current, inspected)
                    record = self._replace_item(
                        record,
                        replace(current, status=_ITEM_DELIVERED),
                    )
                    record = self.catchup_store.save(
                        replace(record, updated_at=self.clock())
                    )
                    reconciled += 1
                    continue
                attempted += 1
                record = self._replace_item(
                    record,
                    replace(
                        current,
                        attempt_count=current.attempt_count + 1,
                        last_error_code=None,
                        last_error_reason=None,
                    ),
                )
                record = self.catchup_store.save(
                    replace(record, updated_at=self.clock())
                )
                try:
                    source = await self._inspect(
                        binding["source"].node_id,
                        binding["source"].provider_id,
                        group_id,
                        current.item_id,
                    )
                    if source is None:
                        raise FederationValidationError(
                            "live-catchup-source-item-missing",
                            "item_id",
                            "current primary does not hold an authoritative item",
                        )
                    self._validate_inspection(current, source)
                    await self._deliver(binding, record, current, source["content"])
                    target = await self._inspect(
                        binding["returning"].node_id,
                        returning_provider_id,
                        group_id,
                        current.item_id,
                    )
                    if target is None:
                        raise FederationValidationError(
                            "live-catchup-target-evidence-missing",
                            "item_id",
                            "target did not expose durable identity after success",
                        )
                    self._validate_inspection(current, target)
                    record = self._replace_item(
                        record,
                        replace(current, status=_ITEM_DELIVERED),
                    )
                    record = self.catchup_store.save(
                        replace(record, updated_at=self.clock())
                    )
                    delivered += 1
                except FederationValidationError as exc:
                    fatal = exc.code in {
                        "live-catchup-source-item-missing",
                        "live-catchup-identity-conflict",
                        "live-catchup-target-evidence-missing",
                        "live-catchup-response-mismatch",
                        StorageErrorCode.IDEMPOTENCY_CONFLICT.value,
                        StorageErrorCode.CONTENT_HASH_MISMATCH.value,
                        StorageErrorCode.UNAUTHORIZED.value,
                        StorageErrorCode.NOT_PRIMARY.value,
                        StorageErrorCode.UNKNOWN_GRANT.value,
                        StorageErrorCode.STALE_TERM.value,
                        StorageErrorCode.STALE_FENCING_TOKEN.value,
                    }
                    updated = replace(
                        self._item(record, current.item_id),
                        status=_ITEM_CONFLICT if fatal else _ITEM_MISSING,
                        last_error_code=exc.code,
                        last_error_reason=str(exc),
                    )
                    record = self._replace_item(record, updated)
                    record = self.catchup_store.save(
                        replace(
                            record,
                            state=_STATE_OPERATOR if fatal else _STATE_RETRYABLE,
                            latest_error_code=exc.code,
                            latest_error_reason=str(exc),
                            updated_at=self.clock(),
                        )
                    )
                    if fatal:
                        return LiveCatchupResult(
                            "operator-attention",
                            exc.code,
                            str(exc),
                            recovery_id,
                            attempted,
                            delivered,
                            reconciled,
                            record,
                        )
                except (OSError, RuntimeError, TimeoutError, TypeError, ValueError) as exc:
                    updated = replace(
                        self._item(record, current.item_id),
                        status=_ITEM_MISSING,
                        last_error_code="live-catchup-delivery-failed",
                        last_error_reason=str(exc) or type(exc).__name__,
                    )
                    record = self._replace_item(record, updated)
                    record = self.catchup_store.save(
                        replace(
                            record,
                            state=_STATE_RETRYABLE,
                            latest_error_code="live-catchup-delivery-failed",
                            latest_error_reason=str(exc) or type(exc).__name__,
                            updated_at=self.clock(),
                        )
                    )
            if any(item.status == _ITEM_MISSING for item in record.items):
                record = self.catchup_store.save(
                    replace(
                        record,
                        state=_STATE_RETRYABLE,
                        latest_error_code="live-catchup-incomplete",
                        latest_error_reason="authoritative items remain undelivered",
                        updated_at=self.clock(),
                    )
                )
                return LiveCatchupResult(
                    "retryable",
                    "live-catchup-incomplete",
                    "authoritative items remain undelivered",
                    recovery_id,
                    attempted,
                    delivered,
                    reconciled,
                    record,
                )
            record = await self._verify(binding, record)
            return LiveCatchupResult(
                "caught-up",
                "live-catchup-complete",
                "former primary is complete, verified, and still unassigned",
                recovery_id,
                attempted,
                delivered,
                reconciled,
                record,
            )
        except FederationValidationError as exc:
            return LiveCatchupResult(
                "operator-attention",
                exc.code,
                str(exc),
                locals().get("recovery_id"),
                0,
                0,
                0,
                locals().get("record"),
            )

    async def _plan(self, recovery_id: str, binding: dict[str, Any]) -> LiveCatchupRecord:
        report, assessment = await self._request_report(binding)
        items: list[LiveCatchupItem] = []
        conflict: FederationValidationError | None = None
        for manifest_item in binding["manifest"].items:
            candidate = LiveCatchupItem.from_manifest(
                manifest_item,
                status=_ITEM_MISSING,
            )
            inspected = await self._inspect(
                binding["returning"].node_id,
                binding["returning"].provider_id,
                binding["group_id"],
                candidate.item_id,
            )
            if inspected is None:
                items.append(candidate)
                continue
            try:
                self._validate_inspection(candidate, inspected)
                items.append(replace(candidate, status=_ITEM_PRESENT))
            except FederationValidationError as exc:
                conflict = exc
                items.append(
                    replace(
                        candidate,
                        status=_ITEM_CONFLICT,
                        last_error_code=exc.code,
                        last_error_reason=str(exc),
                    )
                )
        created = _utc(self.clock())
        record = LiveCatchupRecord(
            schema=LIVE_CATCHUP_SCHEMA,
            recovery_id=recovery_id,
            session_id=self.session_id,
            group_id=binding["group_id"],
            failover_id=binding["failover"].failover_id,
            source_provider_id=binding["source"].provider_id,
            source_node_id=binding["source"].node_id,
            returning_provider_id=binding["returning"].provider_id,
            returning_node_id=binding["returning"].node_id,
            manifest_revision=binding["manifest"].revision,
            manifest_hash=binding["manifest"].manifest_hash,
            grant_id=str(binding["grant"]["grant_id"]),
            term=int(binding["grant"]["term"]),
            fencing_token=int(binding["grant"]["fencing_token"]),
            lease_expires_at=str(binding["grant"]["lease_expires_at"]),
            initial_report_revision=assessment.report_revision,
            initial_report_hash=assessment.report_hash,
            state=_STATE_OPERATOR if conflict is not None else _STATE_PLANNED,
            items=tuple(items),
            final_report_revision=None,
            final_report_hash=None,
            latest_error_code=None if conflict is None else conflict.code,
            latest_error_reason=None if conflict is None else str(conflict),
            created_at=created,
            updated_at=created,
        )
        if report.manifest_revision != record.manifest_revision:
            raise FederationValidationError(
                "live-catchup-report-mismatch",
                "manifest_revision",
                "initial report differs from authoritative manifest",
            )
        return self.catchup_store.save(record)

    async def _verify(
        self,
        binding: dict[str, Any],
        record: LiveCatchupRecord,
    ) -> LiveCatchupRecord:
        verified: list[LiveCatchupItem] = []
        for item in record.items:
            inspected = await self._inspect(
                binding["returning"].node_id,
                binding["returning"].provider_id,
                binding["group_id"],
                item.item_id,
            )
            if inspected is None:
                raise FederationValidationError(
                    "live-catchup-verification-missing",
                    "item_id",
                    "repaired item disappeared before final report",
                )
            self._validate_inspection(item, inspected)
            verified.append(
                replace(
                    item,
                    status=_ITEM_VERIFIED,
                    last_error_code=None,
                    last_error_reason=None,
                )
            )
        verifying = self.catchup_store.save(
            replace(
                record,
                state=_STATE_VERIFYING,
                items=tuple(verified),
                latest_error_code=None,
                latest_error_reason=None,
                updated_at=self.clock(),
            )
        )
        report, assessment = await self._request_report(binding)
        if (
            not assessment.accepted
            or report.manifest_revision != verifying.manifest_revision
            or report.manifest_hash != verifying.manifest_hash
            or not report.integrity_verified
            or report.synchronization_state != "synchronized"
            or set(report.committed_item_hashes)
            != {item.content_hash for item in verifying.items}
        ):
            raise FederationValidationError(
                "live-catchup-final-report-incomplete",
                "report",
                "returning provider did not prove complete synchronized content",
            )
        latest = self._binding(
            verifying.group_id,
            verifying.returning_provider_id,
        )
        self._validate_record(verifying, latest)
        return self.catchup_store.save(
            replace(
                verifying,
                state=_STATE_CAUGHT_UP,
                final_report_revision=assessment.report_revision,
                final_report_hash=assessment.report_hash,
                updated_at=self.clock(),
            )
        )

    async def _request_report(
        self,
        binding: dict[str, Any],
    ) -> tuple[StorageReplicaReport, StorageReplicaAssessment]:
        latest = self.control_plane.latest_storage_replica_assessment(
            self.session_id,
            binding["group_id"],
            binding["returning"].provider_id,
        )
        revision = 1 if latest is None else latest.report_revision + 1
        response = await self.transport.request(
            target_node_id=binding["returning"].node_id,
            envelope=StorageRequestEnvelope(
                request_id=f"catchup-report-{uuid.uuid4().hex}",
                protocol=STORAGE_PROTOCOL,
                protocol_version=STORAGE_PROTOCOL_VERSION,
                operation=StorageOperation.HEALTH,
                session_id=self.session_id,
                actor_node_id=self.credentials.identity.node_id,
                authorization_context={
                    "kind": LIVE_CATCHUP_KIND,
                    "action": "report",
                    "provider_id": binding["returning"].provider_id,
                    "group_id": binding["group_id"],
                },
                payload={
                    "manifest_frame": _json(binding["manifest"].to_dict()),
                    "report_revision": revision,
                    "reported_at": _stamp(self.clock()),
                },
            ),
        )
        self._require_ok(response)
        assert response.result is not None
        report = StorageReplicaReport.from_dict(response.result.get("report"))
        if (
            report.session_id != self.session_id
            or report.group_id != binding["group_id"]
            or report.provider_id != binding["returning"].provider_id
            or report.report_revision != revision
            or report.manifest_revision != binding["manifest"].revision
            or report.manifest_hash != binding["manifest"].manifest_hash
        ):
            raise FederationValidationError(
                "live-catchup-report-mismatch",
                "report",
                "authenticated report is not bound to the requested recovery",
            )
        assessment = self.control_plane.submit_storage_replica_report(
            report,
            actor_node_id=binding["returning"].node_id,
        )
        return report, assessment

    async def _inspect(
        self,
        node_id: str,
        provider_id: str,
        group_id: str,
        item_id: str,
    ) -> dict[str, Any] | None:
        response = await self.transport.request(
            target_node_id=node_id,
            envelope=StorageRequestEnvelope(
                request_id=f"catchup-inspect-{uuid.uuid4().hex}",
                protocol=STORAGE_PROTOCOL,
                protocol_version=STORAGE_PROTOCOL_VERSION,
                operation=StorageOperation.BATCH_READ,
                session_id=self.session_id,
                actor_node_id=self.credentials.identity.node_id,
                authorization_context={
                    "kind": LIVE_CATCHUP_KIND,
                    "action": "inspect",
                    "provider_id": provider_id,
                    "group_id": group_id,
                },
                payload={"group_id": group_id, "batch_id": item_id},
            ),
        )
        if not response.ok:
            if (
                response.error is not None
                and response.error.code is StorageErrorCode.BATCH_NOT_FOUND
            ):
                return None
            self._require_ok(response)
        assert response.result is not None
        identity = response.result.get("identity")
        if not isinstance(identity, dict) or "content" not in response.result:
            raise FederationValidationError(
                "live-catchup-inspection-malformed",
                "result",
                "provider inspection lacks identity or content",
            )
        return {"identity": identity, "content": response.result["content"]}

    async def _deliver(
        self,
        binding: dict[str, Any],
        record: LiveCatchupRecord,
        item: LiveCatchupItem,
        content: Any,
    ) -> None:
        authority = WriteAuthority(
            session_id=self.session_id,
            group_id=record.group_id,
            actor_node_id=record.source_node_id,
            grant_id=record.grant_id,
            term=record.term,
            fencing_token=record.fencing_token,
            lease_expires_at=record.lease_expires_at,
        )
        request = BatchIngestRequest(
            authority=authority,
            dataset_id=item.dataset_id,
            batch_id=item.item_id,
            idempotency_key=item.idempotency_key,
            content_hash=item.content_hash,
            content=content,
            created_at=self.clock(),
            dataset_schema_name=item.schema_name,
            dataset_schema_version=item.schema_version,
        )
        response = await self.transport.request(
            target_node_id=record.returning_node_id,
            envelope=StorageRequestEnvelope(
                request_id=f"catchup-deliver-{record.recovery_id}-{item.item_id}",
                protocol=STORAGE_PROTOCOL,
                protocol_version=STORAGE_PROTOCOL_VERSION,
                operation=StorageOperation.BATCH_INGEST,
                session_id=self.session_id,
                actor_node_id=self.credentials.identity.node_id,
                authorization_context={
                    "kind": LIVE_CATCHUP_KIND,
                    "action": "ingest",
                    "provider_id": record.returning_provider_id,
                    "group_id": record.group_id,
                    "source_provider_id": record.source_provider_id,
                    "source_node_id": record.source_node_id,
                    "recovery_id": record.recovery_id,
                    "manifest_revision": record.manifest_revision,
                    "manifest_hash": record.manifest_hash,
                    "item_id": item.item_id,
                    "dataset_id": item.dataset_id,
                    "schema_name": item.schema_name,
                    "schema_version": item.schema_version,
                    "idempotency_key": item.idempotency_key,
                    "content_hash": item.content_hash,
                },
                payload=request.to_dict(),
            ),
        )
        self._require_ok(response)
        assert response.result is not None
        result = BatchIngestResult.from_dict(response.result)
        if (
            result.batch_id != item.item_id
            or result.idempotency_key != item.idempotency_key
            or result.content_hash != item.content_hash
        ):
            raise FederationValidationError(
                "live-catchup-response-mismatch",
                "result",
                "target response differs from immutable recovery identity",
            )

    def _binding(
        self,
        group_id: str,
        returning_provider_id: str,
    ) -> dict[str, Any]:
        group_id = _text(group_id, "group_id")
        returning_provider_id = _text(
            returning_provider_id, "returning_provider_id"
        )
        snapshot = self.control_plane.snapshot(self.session_id)
        assignment = snapshot.groups.get(group_id)
        if assignment is None or assignment.primary_provider_id is None:
            raise FederationValidationError(
                "live-catchup-group-unavailable",
                "group_id",
                "storage group has no current primary",
            )
        if (
            returning_provider_id == assignment.primary_provider_id
            or returning_provider_id in assignment.replica_provider_ids
        ):
            raise FederationValidationError(
                "live-catchup-target-assigned",
                "returning_provider_id",
                "F4.1 requires the former primary to remain unassigned",
            )
        source = snapshot.providers.get(assignment.primary_provider_id)
        returning = snapshot.providers.get(returning_provider_id)
        grant = snapshot.leader_grants.get(group_id)
        if source is None or returning is None or grant is None:
            raise FederationValidationError(
                "live-catchup-control-incomplete",
                "group_id",
                "provider registration or active grant is missing",
            )
        if (
            grant.get("provider_id") != source.provider_id
            or source.node_id != grant.get("actor_node_id", source.node_id)
        ):
            raise FederationValidationError(
                "live-catchup-source-changed",
                "grant_id",
                "active grant differs from assigned primary",
            )
        if self.clock().astimezone(timezone.utc) >= _utc(
            str(grant["lease_expires_at"]), "lease_expires_at"
        ):
            raise FederationValidationError(
                StorageErrorCode.LEASE_EXPIRED.value,
                "lease_expires_at",
                "current primary lease has expired",
            )
        status = self._status_snapshot()
        if not self._provider_online(source.provider_id, source.node_id, status):
            raise FederationValidationError(
                "live-catchup-source-unavailable",
                "source_provider_id",
                "current primary is not connected and ready",
            )
        if not self._provider_online(returning.provider_id, returning.node_id, status):
            raise FederationValidationError(
                "live-catchup-target-unavailable",
                "returning_provider_id",
                "former primary is not connected and ready",
            )
        failover = self._latest_published_failover(
            group_id,
            returning_provider_id,
        )
        if (
            failover.promoted_provider_id != source.provider_id
            or failover.promoted_node_id != source.node_id
            or failover.failed_node_id != returning.node_id
            or failover.grant_id != grant.get("grant_id")
            or failover.term != int(grant["term"])
            or failover.fencing_token != int(grant["fencing_token"])
        ):
            raise FederationValidationError(
                "live-catchup-failover-mismatch",
                "failover_id",
                "published failover differs from current storage authority",
            )
        manifest = self.control_plane.manifest(self.session_id, group_id)
        return {
            "group_id": group_id,
            "snapshot": snapshot,
            "assignment": assignment,
            "source": source,
            "returning": returning,
            "grant": grant,
            "failover": failover,
            "manifest": manifest,
        }

    def _latest_published_failover(
        self,
        group_id: str,
        returning_provider_id: str,
    ) -> LiveFailoverRecord:
        with sqlite3.connect(self.failover_store.database, timeout=30) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """SELECT record_json FROM storage_live_failovers
                   WHERE session_id=? AND group_id=? AND state='published'
                   ORDER BY updated_at DESC""",
                (self.session_id, group_id),
            ).fetchall()
        for row in rows:
            try:
                record = LiveFailoverRecord.from_dict(json.loads(row["record_json"]))
            except (json.JSONDecodeError, FederationValidationError) as exc:
                raise FederationValidationError(
                    "corrupt-live-failover",
                    "record_json",
                    "published failover record is malformed",
                ) from exc
            if (
                record.schema == LIVE_FAILOVER_SCHEMA
                and record.failed_provider_id == returning_provider_id
            ):
                return record
        raise FederationValidationError(
            "live-catchup-failover-required",
            "returning_provider_id",
            "no published failover exists for the former primary",
        )

    def _status_snapshot(self) -> dict[str, Any]:
        result = {"sessions": [], "nodes": [], "capabilities": []}
        cursor: str | None = None
        for _ in range(1_024):
            page = self.session_coordinator.status(
                actor_node_id=self.credentials.identity.node_id,
                cursor=cursor,
            )
            for section in result:
                values = page.get(section)
                if isinstance(values, list):
                    result[section].extend(values)
            pagination = page.get("pagination")
            if not isinstance(pagination, dict) or not pagination.get("has_more"):
                return result
            cursor = pagination.get("next_cursor")
            if not isinstance(cursor, str) or not cursor:
                break
        raise FederationValidationError(
            "coordinator-status-pagination-failed",
            "status",
            "could not read a complete status snapshot",
        )

    @staticmethod
    def _provider_online(
        provider_id: str,
        node_id: str,
        status: dict[str, Any],
    ) -> bool:
        node = next(
            (
                item
                for item in status.get("nodes", ())
                if isinstance(item, dict) and item.get("node_id") == node_id
            ),
            None,
        )
        if (
            not isinstance(node, dict)
            or node.get("connection_state") != "connected"
            or node.get("revoked") is True
        ):
            return False
        return any(
            isinstance(item, dict)
            and item.get("node_id") == node_id
            and item.get("type") == "storage-provider"
            and item.get("status") == "ready"
            and isinstance(item.get("properties"), dict)
            and item["properties"].get("provider_id") == provider_id
            for item in status.get("capabilities", ())
        )

    @staticmethod
    def _recovery_id(binding: dict[str, Any]) -> str:
        value = (
            f"{binding['manifest'].session_id}\0{binding['group_id']}\0"
            f"{binding['failover'].failover_id}\0"
            f"{binding['returning'].provider_id}\0"
            f"{binding['manifest'].manifest_hash}"
        ).encode()
        return "live-catchup-" + hashlib.sha256(value).hexdigest()

    @staticmethod
    def _validate_record(
        record: LiveCatchupRecord,
        binding: dict[str, Any],
    ) -> None:
        if (
            record.failover_id != binding["failover"].failover_id
            or record.source_provider_id != binding["source"].provider_id
            or record.source_node_id != binding["source"].node_id
            or record.returning_provider_id != binding["returning"].provider_id
            or record.returning_node_id != binding["returning"].node_id
            or record.manifest_revision != binding["manifest"].revision
            or record.manifest_hash != binding["manifest"].manifest_hash
            or record.grant_id != binding["grant"]["grant_id"]
            or record.term != int(binding["grant"]["term"])
            or record.fencing_token != int(binding["grant"]["fencing_token"])
            or record.lease_expires_at
            != _utc(str(binding["grant"]["lease_expires_at"]))
        ):
            raise FederationValidationError(
                "live-catchup-authority-changed",
                "recovery_id",
                "manifest or active storage authority changed during catch-up",
            )

    @staticmethod
    def _validate_inspection(
        item: LiveCatchupItem,
        inspected: dict[str, Any],
    ) -> None:
        identity = inspected.get("identity")
        content = inspected.get("content")
        if not isinstance(identity, dict):
            raise FederationValidationError(
                "live-catchup-inspection-malformed",
                "identity",
                "inspection identity must be an object",
            )
        expected = {
            "dataset_id": item.dataset_id,
            "dataset_schema_name": item.schema_name,
            "dataset_schema_version": item.schema_version,
            "batch_id": item.item_id,
            "idempotency_key": item.idempotency_key,
            "content_hash": item.content_hash,
        }
        if any(identity.get(field) != value for field, value in expected.items()):
            raise FederationValidationError(
                "live-catchup-identity-conflict",
                "item_id",
                "provider identity differs from the authoritative manifest",
            )
        if BatchIngestRequest.calculate_content_hash(content) != item.content_hash:
            raise FederationValidationError(
                "live-catchup-identity-conflict",
                "content_hash",
                "provider content differs from the authoritative manifest",
            )

    @staticmethod
    def _item(record: LiveCatchupRecord, item_id: str) -> LiveCatchupItem:
        for item in record.items:
            if item.item_id == item_id:
                return item
        raise FederationValidationError(
            "unknown-live-catchup-item", "item_id", "item is not in recovery plan"
        )

    @staticmethod
    def _replace_item(
        record: LiveCatchupRecord,
        updated: LiveCatchupItem,
    ) -> LiveCatchupRecord:
        return replace(
            record,
            items=tuple(
                updated if item.item_id == updated.item_id else item
                for item in record.items
            ),
        )

    @staticmethod
    def _require_ok(response: StorageResponseEnvelope) -> None:
        if response.ok:
            return
        assert response.error is not None
        raise FederationValidationError(
            response.error.code.value,
            response.error.field or "storage",
            response.error.message,
        )

    @staticmethod
    def _result(
        status: str,
        code: str,
        reason: str,
        record: LiveCatchupRecord,
    ) -> LiveCatchupResult:
        return LiveCatchupResult(
            status,
            code,
            reason,
            record.recovery_id,
            0,
            0,
            0,
            record,
        )
