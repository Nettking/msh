"""Durable relay-first recovery for a returning former storage primary."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from .errors import FederationValidationError
from .local_storage import BatchStorageProvider
from .provider_fencing import ProviderFenceStore
from .replication import ReplicationTransport
from .reporting import StorageReplicaAssessment, StorageReplicaReport
from .storage_protocol import (
    STORAGE_PROTOCOL,
    STORAGE_PROTOCOL_VERSION,
    BatchIngestRequest,
    BatchIngestResult,
    StorageOperation,
    StorageRequestEnvelope,
    WriteAuthority,
)

FORMER_PRIMARY_RECOVERY_SCHEMA = "msh.storage_former_primary_recovery.v1"
_RECOVERY_STATES = {
    "waiting-report",
    "planned",
    "repairing",
    "awaiting-verification",
    "completed",
    "failed",
}
_ITEM_STATES = {"pending", "delivered", "verified", "conflict"}


def _utc(value: datetime | str | None = None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-timestamp", "timestamp", "must be RFC 3339"
            ) from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError(
            "invalid-timestamp", "timestamp", "must be timezone-aware"
        )
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(ord(char) < 32 for char in value):
        raise FederationValidationError("invalid-id", field, "must be non-empty opaque text")
    return value


def _recovery_id(
    promotion_id: str,
    session_id: str,
    group_id: str,
    former_provider_id: str,
    manifest_hash: str,
) -> str:
    value = (
        f"{promotion_id}\0{session_id}\0{group_id}\0"
        f"{former_provider_id}\0{manifest_hash}"
    ).encode()
    return "former-primary-" + hashlib.sha256(value).hexdigest()


@dataclass(frozen=True)
class FormerPrimaryRepairItem:
    item_id: str
    dataset_id: str
    idempotency_key: str
    content_hash: str
    schema_name: str
    schema_version: int
    committed_at: datetime
    state: str = "pending"
    attempt_count: int = 0
    last_error: str | None = None

    def __post_init__(self) -> None:
        for field in (
            "item_id",
            "dataset_id",
            "idempotency_key",
            "content_hash",
            "schema_name",
            "state",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        if self.state not in _ITEM_STATES:
            raise FederationValidationError(
                "invalid-recovery-state", "state", "unknown repair item state"
            )
        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version <= 0
        ):
            raise FederationValidationError(
                "invalid-positive-integer", "schema_version", "must be positive"
            )
        if (
            isinstance(self.attempt_count, bool)
            or not isinstance(self.attempt_count, int)
            or self.attempt_count < 0
        ):
            raise FederationValidationError(
                "invalid-non-negative-integer",
                "attempt_count",
                "must be non-negative",
            )
        if self.last_error is not None:
            object.__setattr__(self, "last_error", _text(self.last_error, "last_error"))
        object.__setattr__(self, "committed_at", _utc(self.committed_at))

    def to_dict(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "dataset_id": self.dataset_id,
            "idempotency_key": self.idempotency_key,
            "content_hash": self.content_hash,
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "committed_at": _timestamp(self.committed_at),
            "state": self.state,
            "attempt_count": self.attempt_count,
            "last_error": self.last_error,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> FormerPrimaryRepairItem:
        return cls(**value)


@dataclass(frozen=True)
class FormerPrimaryRecoveryRecord:
    recovery_id: str
    promotion_id: str
    session_id: str
    group_id: str
    former_provider_id: str
    active_provider_id: str
    manifest_revision: int
    manifest_hash: str
    state: str
    created_at: datetime
    updated_at: datetime
    items: tuple[FormerPrimaryRepairItem, ...]
    failure_code: str | None = None
    failure_reason: str | None = None
    schema: str = FORMER_PRIMARY_RECOVERY_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != FORMER_PRIMARY_RECOVERY_SCHEMA:
            raise FederationValidationError(
                "unsupported-schema",
                "schema",
                f"expected {FORMER_PRIMARY_RECOVERY_SCHEMA}",
            )
        for field in (
            "recovery_id",
            "promotion_id",
            "session_id",
            "group_id",
            "former_provider_id",
            "active_provider_id",
            "manifest_hash",
            "state",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        if self.state not in _RECOVERY_STATES:
            raise FederationValidationError(
                "invalid-recovery-state", "state", "unknown recovery state"
            )
        if (
            isinstance(self.manifest_revision, bool)
            or not isinstance(self.manifest_revision, int)
            or self.manifest_revision < 0
        ):
            raise FederationValidationError(
                "invalid-non-negative-integer",
                "manifest_revision",
                "must be non-negative",
            )
        if (self.failure_code is None) != (self.failure_reason is None):
            raise FederationValidationError(
                "invalid-recovery-failure",
                "failure_code",
                "failure code and reason must appear together",
            )
        if self.failure_code is not None:
            object.__setattr__(self, "failure_code", _text(self.failure_code, "failure_code"))
            object.__setattr__(self, "failure_reason", _text(self.failure_reason, "failure_reason"))
        object.__setattr__(self, "created_at", _utc(self.created_at))
        object.__setattr__(self, "updated_at", _utc(self.updated_at))
        object.__setattr__(self, "items", tuple(self.items))

    @property
    def pending_items(self) -> tuple[FormerPrimaryRepairItem, ...]:
        return tuple(item for item in self.items if item.state == "pending")

    @property
    def conflict_items(self) -> tuple[FormerPrimaryRepairItem, ...]:
        return tuple(item for item in self.items if item.state == "conflict")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "recovery_id": self.recovery_id,
            "promotion_id": self.promotion_id,
            "session_id": self.session_id,
            "group_id": self.group_id,
            "former_provider_id": self.former_provider_id,
            "active_provider_id": self.active_provider_id,
            "manifest_revision": self.manifest_revision,
            "manifest_hash": self.manifest_hash,
            "state": self.state,
            "failure_code": self.failure_code,
            "failure_reason": self.failure_reason,
            "created_at": _timestamp(self.created_at),
            "updated_at": _timestamp(self.updated_at),
            "items": [item.to_dict() for item in self.items],
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> FormerPrimaryRecoveryRecord:
        data = dict(value)
        data["items"] = tuple(
            FormerPrimaryRepairItem.from_dict(item) for item in data["items"]
        )
        return cls(**data)


@dataclass(frozen=True)
class FormerPrimaryRecoveryRunResult:
    status: str
    code: str
    reason: str
    attempted: int
    delivered: int
    record: FormerPrimaryRecoveryRecord


class RecoveryControlPlane(Protocol):
    database: str

    def snapshot(self, session_id: str): ...

    def manifest(self, session_id: str, group_id: str): ...

    def promotion_transaction(self, session_id: str, group_id: str, promotion_id: str): ...

    def promotion_finalization(self, session_id: str, group_id: str, promotion_id: str): ...

    def latest_storage_replica_assessment(
        self, session_id: str, group_id: str, provider_id: str
    ) -> StorageReplicaAssessment | None: ...

    def submit_storage_replica_report(
        self, report: StorageReplicaReport, *, actor_node_id: str
    ) -> StorageReplicaAssessment: ...


class FormerPrimaryRecoveryStore:
    """SQLite recovery ledger sharing the coordinator database."""

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS storage_former_primary_recoveries (
                    recovery_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    promotion_id TEXT NOT NULL,
                    former_provider_id TEXT NOT NULL,
                    manifest_revision INTEGER NOT NULL CHECK(manifest_revision >= 0),
                    manifest_hash TEXT NOT NULL,
                    state TEXT NOT NULL,
                    record_json TEXT NOT NULL CHECK(json_valid(record_json)),
                    updated_at TEXT NOT NULL
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def get(self, recovery_id: str) -> FormerPrimaryRecoveryRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT record_json FROM storage_former_primary_recoveries
                   WHERE recovery_id=?""",
                (recovery_id,),
            ).fetchone()
        return (
            None
            if row is None
            else FormerPrimaryRecoveryRecord.from_dict(json.loads(row["record_json"]))
        )

    def put(
        self,
        record: FormerPrimaryRecoveryRecord,
        *,
        only_if_state: str | None = None,
    ) -> FormerPrimaryRecoveryRecord:
        encoded = json.dumps(
            record.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """SELECT record_json, state FROM storage_former_primary_recoveries
                   WHERE recovery_id=?""",
                (record.recovery_id,),
            ).fetchone()
            if existing is None:
                connection.execute(
                    """INSERT INTO storage_former_primary_recoveries
                       (recovery_id, session_id, group_id, promotion_id,
                        former_provider_id, manifest_revision, manifest_hash,
                        state, record_json, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        record.recovery_id,
                        record.session_id,
                        record.group_id,
                        record.promotion_id,
                        record.former_provider_id,
                        record.manifest_revision,
                        record.manifest_hash,
                        record.state,
                        encoded,
                        _timestamp(record.updated_at),
                    ),
                )
            elif only_if_state is not None and existing["state"] != only_if_state:
                connection.rollback()
                restored = self.get(record.recovery_id)
                assert restored is not None
                return restored
            else:
                connection.execute(
                    """UPDATE storage_former_primary_recoveries
                       SET state=?, record_json=?, updated_at=?
                       WHERE recovery_id=?""",
                    (
                        record.state,
                        encoded,
                        _timestamp(record.updated_at),
                        record.recovery_id,
                    ),
                )
            connection.commit()
        restored = self.get(record.recovery_id)
        assert restored is not None
        return restored


class FormerPrimaryRecoveryCoordinator:
    """Plan, deliver, and verify former-primary catch-up over relay replication."""

    def __init__(
        self,
        *,
        control: RecoveryControlPlane,
        source_provider: BatchStorageProvider,
        transport: ReplicationTransport,
        former_provider_fence_store: ProviderFenceStore,
        clock=lambda: datetime.now(timezone.utc),
    ) -> None:
        self.control = control
        self.source_provider = source_provider
        self.transport = transport
        self.former_provider_fence_store = former_provider_fence_store
        self.clock = clock
        self.store = FormerPrimaryRecoveryStore(control.database)

    def plan(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
        *,
        former_provider_id: str | None = None,
    ) -> FormerPrimaryRecoveryRecord:
        finalization = self.control.promotion_finalization(
            session_id, group_id, promotion_id
        )
        transaction = self.control.promotion_transaction(
            session_id, group_id, promotion_id
        )
        if finalization is None or transaction is None:
            raise FederationValidationError(
                "unknown-promotion-finalization",
                "promotion_id",
                "completed promotion evidence is required before recovery",
            )
        if not finalization.degraded_cleared or transaction.state != "finalized":
            raise FederationValidationError(
                "promotion-not-complete",
                "promotion_id",
                "former-primary recovery begins only after completed promotion",
            )
        former_provider_id = former_provider_id or transaction.previous_provider_id
        if former_provider_id is None or transaction.previous_term is None:
            raise FederationValidationError(
                "missing-former-primary",
                "former_provider_id",
                "promotion has no former primary authority",
            )
        self._validate_fence(
            session_id,
            group_id,
            former_provider_id,
            transaction.previous_term,
        )
        snapshot = self.control.snapshot(session_id)
        assignment = snapshot.groups.get(group_id)
        grant = snapshot.leader_grants.get(group_id)
        if (
            assignment is None
            or assignment.primary_provider_id != finalization.selected_provider_id
            or former_provider_id not in assignment.replica_provider_ids
            or grant is None
            or grant.get("provider_id") != finalization.selected_provider_id
            or int(grant.get("term", -1)) != finalization.new_authority_term
        ):
            raise FederationValidationError(
                "former-primary-assignment-mismatch",
                "group_id",
                "promotion must be published and former primary assigned replica",
            )
        manifest = self.control.manifest(session_id, group_id)
        recovery_id = _recovery_id(
            promotion_id,
            session_id,
            group_id,
            former_provider_id,
            manifest.manifest_hash,
        )
        existing = self.store.get(recovery_id)
        if existing is not None and existing.state != "waiting-report":
            return existing

        now = _utc(self.clock())
        assessment = self.control.latest_storage_replica_assessment(
            session_id, group_id, former_provider_id
        )
        report = None if assessment is None else assessment.report
        if report is None:
            waiting = self._record(
                recovery_id,
                promotion_id,
                session_id,
                group_id,
                former_provider_id,
                finalization.selected_provider_id,
                manifest,
                "waiting-report",
                now,
                existing=existing,
            )
            return self.store.put(waiting)

        if report.provider_id != former_provider_id:
            raise FederationValidationError(
                "returning-report-provider-mismatch",
                "provider_id",
                "report does not belong to the former primary",
            )
        authoritative = Counter(item.content_hash for item in manifest.items)
        reported = Counter(report.committed_item_hashes)
        failure_code: str | None = None
        failure_reason: str | None = None
        if not report.integrity_verified:
            failure_code = "returning-provider-integrity-failure"
            failure_reason = "returning provider inventory is not integrity verified"
        elif reported - authoritative:
            failure_code = "returning-provider-extra-or-conflicting-items"
            failure_reason = (
                "returning provider reports hashes outside authoritative state"
            )

        available = Counter(report.committed_item_hashes)
        items: list[FormerPrimaryRepairItem] = []
        for manifest_item in manifest.items:
            state = "conflict" if failure_code is not None else "pending"
            if failure_code is None and available[manifest_item.content_hash] > 0:
                available[manifest_item.content_hash] -= 1
                state = "verified"
            items.append(
                FormerPrimaryRepairItem(
                    item_id=manifest_item.item_id,
                    dataset_id=manifest_item.dataset_id,
                    idempotency_key=(
                        manifest_item.idempotency_key or manifest_item.item_id
                    ),
                    content_hash=manifest_item.content_hash,
                    schema_name=manifest_item.schema_name,
                    schema_version=manifest_item.schema_version,
                    committed_at=manifest_item.committed_at,
                    state=state,
                )
            )
        state = (
            "failed"
            if failure_code is not None
            else (
                "planned"
                if any(item.state == "pending" for item in items)
                else "awaiting-verification"
            )
        )
        planned = FormerPrimaryRecoveryRecord(
            recovery_id=recovery_id,
            promotion_id=promotion_id,
            session_id=session_id,
            group_id=group_id,
            former_provider_id=former_provider_id,
            active_provider_id=finalization.selected_provider_id,
            manifest_revision=manifest.revision,
            manifest_hash=manifest.manifest_hash,
            state=state,
            failure_code=failure_code,
            failure_reason=failure_reason,
            created_at=now if existing is None else existing.created_at,
            updated_at=now,
            items=tuple(items),
        )
        return self.store.put(
            planned,
            only_if_state="waiting-report" if existing is not None else None,
        )

    async def run_once(
        self,
        recovery_id: str,
        *,
        limit: int = 100,
    ) -> FormerPrimaryRecoveryRunResult:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise FederationValidationError("invalid-limit", "limit", "must be positive")
        record = self._required(recovery_id)
        if record.state == "completed":
            return self._result(
                "completed", "recovery-complete", "recovery already completed", record
            )
        if record.state == "failed":
            return self._result(
                "operator-attention",
                record.failure_code or "recovery-failed",
                record.failure_reason or "recovery failed",
                record,
            )
        if record.state == "waiting-report":
            return self._result(
                "retryable",
                "returning-report-required",
                "returning provider must submit inventory evidence before repair",
                record,
            )
        self._revalidate(record)
        if not record.pending_items:
            waiting = self._save(replace(record, state="awaiting-verification"))
            return self._result(
                "retryable",
                "verification-report-required",
                "synchronized provider report is required",
                waiting,
            )

        record = self._save(replace(record, state="repairing"))
        attempted = 0
        delivered = 0
        for item in record.pending_items[:limit]:
            attempted += 1
            try:
                await self._deliver(record, item)
                record = self._replace_item(
                    record,
                    replace(
                        item,
                        state="delivered",
                        attempt_count=item.attempt_count + 1,
                        last_error=None,
                    ),
                )
                delivered += 1
            except FederationValidationError as exc:
                if exc.code in {
                    "recovery-source-item-missing",
                    "recovery-source-hash-mismatch",
                    "recovery-source-identity-mismatch",
                    "repair-identity-mismatch",
                }:
                    record = self._replace_item(
                        record,
                        replace(
                            item,
                            state="conflict",
                            attempt_count=item.attempt_count + 1,
                            last_error=str(exc),
                        ),
                    )
                    failed = self._save(
                        replace(
                            record,
                            state="failed",
                            failure_code=exc.code,
                            failure_reason=str(exc),
                        )
                    )
                    return FormerPrimaryRecoveryRunResult(
                        "operator-attention",
                        exc.code,
                        str(exc),
                        attempted,
                        delivered,
                        failed,
                    )
                record = self._retry_item(record, item, exc)
            except (
                OSError,
                RuntimeError,
                TimeoutError,
                TypeError,
                ValueError,
            ) as exc:
                record = self._retry_item(record, item, exc)

        record = self._save(
            replace(
                record,
                state=(
                    "planned"
                    if record.pending_items
                    else "awaiting-verification"
                ),
            )
        )
        return FormerPrimaryRecoveryRunResult(
            "retryable",
            (
                "repair-incomplete"
                if record.pending_items
                else "verification-report-required"
            ),
            (
                "one or more authoritative items remain undelivered"
                if record.pending_items
                else "missing items delivered; synchronized report is required"
            ),
            attempted,
            delivered,
            record,
        )

    def verify(
        self,
        recovery_id: str,
        report: StorageReplicaReport,
        *,
        actor_node_id: str,
    ) -> FormerPrimaryRecoveryRecord:
        record = self._required(recovery_id)
        if record.state == "completed":
            return record
        self._revalidate(record)
        if record.pending_items:
            raise FederationValidationError(
                "recovery-items-pending",
                "recovery_id",
                "all missing items must be delivered before verification",
            )
        if (
            report.session_id != record.session_id
            or report.group_id != record.group_id
            or report.provider_id != record.former_provider_id
            or report.manifest_revision != record.manifest_revision
            or report.manifest_hash != record.manifest_hash
        ):
            raise FederationValidationError(
                "recovery-report-mismatch",
                "report",
                "verification report differs from durable recovery bindings",
            )
        assessment = self.control.submit_storage_replica_report(
            report,
            actor_node_id=actor_node_id,
        )
        if not assessment.accepted:
            return self._save(
                replace(
                    record,
                    state="failed",
                    failure_code="returning-provider-report-rejected",
                    failure_reason=assessment.eligibility_reason,
                )
            )
        if not assessment.eligibility:
            if assessment.eligibility_reason in {
                "committed-hash-conflict",
                "integrity-failure",
            }:
                return self._save(
                    replace(
                        record,
                        state="failed",
                        failure_code="returning-provider-verification-failed",
                        failure_reason=assessment.eligibility_reason,
                    )
                )
            return self._save(replace(record, state="awaiting-verification"))
        verified = tuple(replace(item, state="verified", last_error=None) for item in record.items)
        return self._save(replace(record, state="completed", items=verified))

    def _record(
        self,
        recovery_id: str,
        promotion_id: str,
        session_id: str,
        group_id: str,
        former_provider_id: str,
        active_provider_id: str,
        manifest: Any,
        state: str,
        now: datetime,
        *,
        existing: FormerPrimaryRecoveryRecord | None,
    ) -> FormerPrimaryRecoveryRecord:
        return FormerPrimaryRecoveryRecord(
            recovery_id=recovery_id,
            promotion_id=promotion_id,
            session_id=session_id,
            group_id=group_id,
            former_provider_id=former_provider_id,
            active_provider_id=active_provider_id,
            manifest_revision=manifest.revision,
            manifest_hash=manifest.manifest_hash,
            state=state,
            created_at=now if existing is None else existing.created_at,
            updated_at=now,
            items=tuple(
                FormerPrimaryRepairItem(
                    item_id=item.item_id,
                    dataset_id=item.dataset_id,
                    idempotency_key=item.idempotency_key or item.item_id,
                    content_hash=item.content_hash,
                    schema_name=item.schema_name,
                    schema_version=item.schema_version,
                    committed_at=item.committed_at,
                )
                for item in manifest.items
            ),
        )

    def _required(self, recovery_id: str) -> FormerPrimaryRecoveryRecord:
        record = self.store.get(recovery_id)
        if record is None:
            raise FederationValidationError(
                "unknown-former-primary-recovery",
                "recovery_id",
                "recovery record does not exist",
            )
        return record

    def _save(
        self,
        record: FormerPrimaryRecoveryRecord,
    ) -> FormerPrimaryRecoveryRecord:
        return self.store.put(replace(record, updated_at=_utc(self.clock())))

    def _replace_item(
        self,
        record: FormerPrimaryRecoveryRecord,
        replacement: FormerPrimaryRepairItem,
    ) -> FormerPrimaryRecoveryRecord:
        items = tuple(
            replacement if item.item_id == replacement.item_id else item
            for item in record.items
        )
        return self._save(replace(record, items=items))

    def _retry_item(
        self,
        record: FormerPrimaryRecoveryRecord,
        item: FormerPrimaryRepairItem,
        error: Exception,
    ) -> FormerPrimaryRecoveryRecord:
        return self._replace_item(
            record,
            replace(
                item,
                state="pending",
                attempt_count=item.attempt_count + 1,
                last_error=str(error),
            ),
        )

    @staticmethod
    def _result(
        status: str,
        code: str,
        reason: str,
        record: FormerPrimaryRecoveryRecord,
    ) -> FormerPrimaryRecoveryRunResult:
        return FormerPrimaryRecoveryRunResult(status, code, reason, 0, 0, record)

    def _validate_fence(
        self,
        session_id: str,
        group_id: str,
        former_provider_id: str,
        previous_term: int,
    ) -> None:
        store = self.former_provider_fence_store
        if (
            store.provider_id != former_provider_id
            or store.session_id != session_id
            or store.high_water_term(group_id) < previous_term
        ):
            raise FederationValidationError(
                "former-primary-not-fenced",
                "former_provider_id",
                "durable former-primary fence is missing or stale",
            )

    def _revalidate(self, record: FormerPrimaryRecoveryRecord) -> None:
        transaction = self.control.promotion_transaction(
            record.session_id,
            record.group_id,
            record.promotion_id,
        )
        if transaction is None or transaction.previous_term is None:
            raise FederationValidationError(
                "promotion-state-missing",
                "promotion_id",
                "durable promotion state is unavailable",
            )
        self._validate_fence(
            record.session_id,
            record.group_id,
            record.former_provider_id,
            transaction.previous_term,
        )
        manifest = self.control.manifest(record.session_id, record.group_id)
        if (
            manifest.revision != record.manifest_revision
            or manifest.manifest_hash != record.manifest_hash
        ):
            raise FederationValidationError(
                "recovery-manifest-changed",
                "manifest_hash",
                "authoritative manifest changed during recovery",
            )
        snapshot = self.control.snapshot(record.session_id)
        assignment = snapshot.groups.get(record.group_id)
        grant = snapshot.leader_grants.get(record.group_id)
        if (
            assignment is None
            or assignment.primary_provider_id != record.active_provider_id
            or record.former_provider_id not in assignment.replica_provider_ids
            or grant is None
            or grant.get("provider_id") != record.active_provider_id
        ):
            raise FederationValidationError(
                "recovery-authority-changed",
                "group_id",
                "assignment or active authority changed during recovery",
            )

    async def _deliver(
        self,
        record: FormerPrimaryRecoveryRecord,
        item: FormerPrimaryRepairItem,
    ) -> None:
        snapshot = self.control.snapshot(record.session_id)
        active = snapshot.providers.get(record.active_provider_id)
        returning = snapshot.providers.get(record.former_provider_id)
        grant = snapshot.leader_grants.get(record.group_id)
        if active is None or returning is None or grant is None:
            raise FederationValidationError(
                "recovery-provider-unavailable",
                "provider_id",
                "active or returning provider registration is unavailable",
            )
        identity = self.source_provider.committed_identity(
            session_id=record.session_id,
            group_id=record.group_id,
            batch_id=item.item_id,
        )
        if (
            identity is None
            or identity.dataset_id != item.dataset_id
            or identity.dataset_schema_name != item.schema_name
            or identity.dataset_schema_version != item.schema_version
            or identity.idempotency_key != item.idempotency_key
            or identity.content_hash != item.content_hash
        ):
            raise FederationValidationError(
                "recovery-source-identity-mismatch",
                "item_id",
                "active primary identity differs from the manifest",
            )
        content = self.source_provider.read(
            session_id=record.session_id,
            group_id=record.group_id,
            batch_id=item.item_id,
        )
        if content is None:
            raise FederationValidationError(
                "recovery-source-item-missing",
                "item_id",
                "active primary cannot read an authoritative item",
            )
        if BatchIngestRequest.calculate_content_hash(content) != item.content_hash:
            raise FederationValidationError(
                "recovery-source-hash-mismatch",
                "content_hash",
                "active primary content differs from the manifest",
            )
        request = BatchIngestRequest(
            authority=WriteAuthority(
                session_id=record.session_id,
                group_id=record.group_id,
                actor_node_id=active.node_id,
                grant_id=str(grant["grant_id"]),
                term=int(grant["term"]),
                fencing_token=int(grant["fencing_token"]),
                lease_expires_at=grant["lease_expires_at"],
            ),
            dataset_id=item.dataset_id,
            batch_id=item.item_id,
            idempotency_key=item.idempotency_key,
            content_hash=item.content_hash,
            content=content,
            created_at=item.committed_at,
            dataset_schema_name=item.schema_name,
            dataset_schema_version=item.schema_version,
        )
        envelope = StorageRequestEnvelope(
            request_id=(
                f"former-primary-repair-{record.recovery_id}-"
                f"{item.item_id}-{item.attempt_count}"
            ),
            protocol=STORAGE_PROTOCOL,
            protocol_version=STORAGE_PROTOCOL_VERSION,
            operation=StorageOperation.BATCH_INGEST,
            session_id=record.session_id,
            actor_node_id=active.node_id,
            authorization_context={
                "kind": "storage-replication",
                "group_id": record.group_id,
                "provider_id": record.former_provider_id,
                "recovery_id": record.recovery_id,
                "promotion_id": record.promotion_id,
            },
            payload=request.to_dict(),
        )
        response = await self.transport.request(
            target_node_id=returning.node_id,
            envelope=envelope,
        )
        if response.request_id != envelope.request_id:
            raise FederationValidationError(
                "response-request-mismatch",
                "request_id",
                "repair response belongs to another request",
            )
        if not response.ok:
            assert response.error is not None
            raise FederationValidationError(
                response.error.code.value,
                response.error.field or "recovery",
                response.error.message,
            )
        result = BatchIngestResult.from_dict(response.result)
        if (
            result.batch_id != request.batch_id
            or result.idempotency_key != request.idempotency_key
            or result.content_hash != request.content_hash
        ):
            raise FederationValidationError(
                "repair-identity-mismatch",
                "result",
                "returning provider response differs from authoritative item",
            )
