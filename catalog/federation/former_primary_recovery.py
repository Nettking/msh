"""Durable E7.0/E7.1 planning for a returning former storage primary.

This module deliberately contains no transfer logic. It binds a recovery plan to
completed E6 authority evidence, current coordinator state, the authoritative
manifest, provider-local fencing, and authenticated returning-provider evidence.
The complete item ledger is persisted atomically before later checkpoints may
move data.
"""

from __future__ import annotations

import hashlib
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

from .errors import FederationValidationError
from .local_storage import BatchStorageProvider, CommittedBatchIdentity
from .manifest import ManifestItem, ManifestItemKind
from .models import CommitState
from .promotion_publication import (
    PromotionPublicationRecord,
    promotion_publication,
)
from .provider_fencing import ProviderFenceStore
from .reporting import StorageReplicaAssessment

FORMER_PRIMARY_RECOVERY_SCHEMA = "msh.storage_former_primary_recovery.v1"


def _text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-id",
            field,
            "must be non-empty opaque text",
        )
    return value


def _uint(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-non-negative-integer",
            field,
            "must be a non-negative integer",
        )
    return value


def _utc(value: datetime | str | None = None, field: str = "timestamp") -> datetime:
    if value is None:
        value = datetime.now(timezone.utc)
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-timestamp",
                field,
                "must be RFC 3339",
            ) from exc
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise FederationValidationError(
            "invalid-timestamp",
            field,
            "must be timezone-aware",
        )
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _optional_text(value: Any, field: str) -> str | None:
    return None if value is None else _text(value, field)


def _optional_uint(value: Any, field: str) -> int | None:
    return None if value is None else _uint(value, field)


def _recovery_id(
    *,
    session_id: str,
    group_id: str,
    returning_provider_id: str,
    promotion_id: str,
    final_authority_identity: str,
    manifest_hash: str,
) -> str:
    payload = (
        f"{session_id}\0{group_id}\0{returning_provider_id}\0"
        f"{promotion_id}\0{final_authority_identity}\0{manifest_hash}"
    ).encode()
    return "former-primary-" + hashlib.sha256(payload).hexdigest()


def _delivery_id(recovery_id: str, item_id: str) -> str:
    payload = f"{recovery_id}\0{item_id}".encode()
    return "repair-item-" + hashlib.sha256(payload).hexdigest()


class FormerPrimaryRecoveryState(str, Enum):
    PLANNED = "planned"
    TRANSFERRING = "transferring"
    VERIFYING = "verifying"
    AWAITING_REPORT = "awaiting-report"
    COMPLETED = "completed"
    RETRYABLE = "retryable"
    OPERATOR_ATTENTION = "operator-attention"
    FAILED = "failed"


class FormerPrimaryItemStatus(str, Enum):
    PRESENT = "present"
    MISSING = "missing"
    DELIVERED = "delivered"
    VERIFIED = "verified"
    CONFLICT = "conflict"
    FAILED = "failed"


@dataclass(frozen=True)
class FormerPrimaryRecoveryItem:
    recovery_id: str
    session_id: str
    group_id: str
    returning_provider_id: str
    item_id: str
    kind: ManifestItemKind
    dataset_id: str
    schema_name: str
    schema_version: int
    idempotency_key: str | None
    content_hash: str
    size_bytes: int
    source_id: str | None
    first_sequence: int | None
    last_sequence: int | None
    commit_state: CommitState
    status: FormerPrimaryItemStatus
    delivery_id: str
    attempt_count: int
    last_error_code: str | None
    last_error_reason: str | None
    updated_at: datetime

    def __post_init__(self) -> None:
        for field in (
            "recovery_id",
            "session_id",
            "group_id",
            "returning_provider_id",
            "item_id",
            "dataset_id",
            "schema_name",
            "content_hash",
            "delivery_id",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        try:
            object.__setattr__(self, "kind", ManifestItemKind(self.kind))
            object.__setattr__(self, "commit_state", CommitState(self.commit_state))
            object.__setattr__(
                self,
                "status",
                FormerPrimaryItemStatus(self.status),
            )
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-recovery-enum",
                "recovery_item",
                "contains an unknown enum value",
            ) from exc
        if self.commit_state is not CommitState.COMMITTED:
            raise FederationValidationError(
                "recovery-item-not-committed",
                "commit_state",
                "only authoritative committed items may be recovered",
            )
        object.__setattr__(
            self,
            "schema_version",
            _uint(self.schema_version, "schema_version"),
        )
        if self.schema_version == 0:
            raise FederationValidationError(
                "invalid-positive-integer",
                "schema_version",
                "must be greater than zero",
            )
        object.__setattr__(self, "size_bytes", _uint(self.size_bytes, "size_bytes"))
        object.__setattr__(
            self,
            "attempt_count",
            _uint(self.attempt_count, "attempt_count"),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            _optional_text(self.idempotency_key, "idempotency_key"),
        )
        object.__setattr__(
            self,
            "source_id",
            _optional_text(self.source_id, "source_id"),
        )
        object.__setattr__(
            self,
            "first_sequence",
            _optional_uint(self.first_sequence, "first_sequence"),
        )
        object.__setattr__(
            self,
            "last_sequence",
            _optional_uint(self.last_sequence, "last_sequence"),
        )
        if (self.first_sequence is None) != (self.last_sequence is None):
            raise FederationValidationError(
                "invalid-sequence-range",
                "first_sequence",
                "sequence endpoints must appear together",
            )
        if (
            self.first_sequence is not None
            and self.last_sequence is not None
            and self.last_sequence < self.first_sequence
        ):
            raise FederationValidationError(
                "invalid-sequence-range",
                "last_sequence",
                "must not precede first_sequence",
            )
        if (self.last_error_code is None) != (self.last_error_reason is None):
            raise FederationValidationError(
                "invalid-recovery-error",
                "last_error_code",
                "error code and reason must appear together",
            )
        object.__setattr__(
            self,
            "last_error_code",
            _optional_text(self.last_error_code, "last_error_code"),
        )
        object.__setattr__(
            self,
            "last_error_reason",
            _optional_text(self.last_error_reason, "last_error_reason"),
        )
        object.__setattr__(
            self,
            "updated_at",
            _utc(self.updated_at, "updated_at"),
        )

    @classmethod
    def from_manifest(
        cls,
        *,
        recovery_id: str,
        session_id: str,
        group_id: str,
        returning_provider_id: str,
        item: ManifestItem,
        status: FormerPrimaryItemStatus,
        now: datetime,
        error_code: str | None = None,
        error_reason: str | None = None,
    ) -> "FormerPrimaryRecoveryItem":
        return cls(
            recovery_id=recovery_id,
            session_id=session_id,
            group_id=group_id,
            returning_provider_id=returning_provider_id,
            item_id=item.item_id,
            kind=item.kind,
            dataset_id=item.dataset_id,
            schema_name=item.schema_name,
            schema_version=item.schema_version,
            idempotency_key=item.idempotency_key,
            content_hash=item.content_hash,
            size_bytes=item.size_bytes,
            source_id=item.source_id,
            first_sequence=item.first_sequence,
            last_sequence=item.last_sequence,
            commit_state=item.commit_state,
            status=status,
            delivery_id=_delivery_id(recovery_id, item.item_id),
            attempt_count=0,
            last_error_code=error_code,
            last_error_reason=error_reason,
            updated_at=now,
        )

    def immutable_binding(self) -> tuple[Any, ...]:
        return (
            self.recovery_id,
            self.session_id,
            self.group_id,
            self.returning_provider_id,
            self.item_id,
            self.kind.value,
            self.dataset_id,
            self.schema_name,
            self.schema_version,
            self.idempotency_key,
            self.content_hash,
            self.size_bytes,
            self.source_id,
            self.first_sequence,
            self.last_sequence,
            self.commit_state.value,
            self.delivery_id,
        )


@dataclass(frozen=True)
class FormerPrimaryRecoveryRecord:
    schema: str
    recovery_id: str
    session_id: str
    group_id: str
    returning_provider_id: str
    returning_node_id: str
    source_provider_id: str
    source_node_id: str
    promotion_id: str
    final_authority_identity: str
    publication_control_revision: int
    manifest_revision: int
    manifest_hash: str
    grant_id: str
    term: int
    fencing_token: int
    lease_expires_at: datetime
    report_revision: int
    report_hash: str
    state: FormerPrimaryRecoveryState
    item_count: int
    present_count: int
    missing_count: int
    conflict_count: int
    latest_error_code: str | None
    latest_error_reason: str | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.schema != FORMER_PRIMARY_RECOVERY_SCHEMA:
            raise FederationValidationError(
                "unsupported-schema",
                "schema",
                f"expected {FORMER_PRIMARY_RECOVERY_SCHEMA}",
            )
        for field in (
            "recovery_id",
            "session_id",
            "group_id",
            "returning_provider_id",
            "returning_node_id",
            "source_provider_id",
            "source_node_id",
            "promotion_id",
            "final_authority_identity",
            "manifest_hash",
            "grant_id",
            "report_hash",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        for field in (
            "publication_control_revision",
            "manifest_revision",
            "term",
            "fencing_token",
            "report_revision",
            "item_count",
            "present_count",
            "missing_count",
            "conflict_count",
        ):
            object.__setattr__(self, field, _uint(getattr(self, field), field))
        try:
            object.__setattr__(
                self,
                "state",
                FormerPrimaryRecoveryState(self.state),
            )
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-recovery-state",
                "state",
                "unknown former-primary recovery state",
            ) from exc
        if (
            self.present_count + self.missing_count + self.conflict_count
            != self.item_count
        ):
            raise FederationValidationError(
                "invalid-recovery-counts",
                "item_count",
                "item status counts must equal item_count",
            )
        if (self.latest_error_code is None) != (
            self.latest_error_reason is None
        ):
            raise FederationValidationError(
                "invalid-recovery-error",
                "latest_error_code",
                "error code and reason must appear together",
            )
        object.__setattr__(
            self,
            "latest_error_code",
            _optional_text(self.latest_error_code, "latest_error_code"),
        )
        object.__setattr__(
            self,
            "latest_error_reason",
            _optional_text(self.latest_error_reason, "latest_error_reason"),
        )
        object.__setattr__(
            self,
            "lease_expires_at",
            _utc(self.lease_expires_at, "lease_expires_at"),
        )
        object.__setattr__(
            self,
            "created_at",
            _utc(self.created_at, "created_at"),
        )
        object.__setattr__(
            self,
            "updated_at",
            _utc(self.updated_at, "updated_at"),
        )
        if self.completed_at is not None:
            object.__setattr__(
                self,
                "completed_at",
                _utc(self.completed_at, "completed_at"),
            )
        if (
            self.state is FormerPrimaryRecoveryState.COMPLETED
            and self.completed_at is None
        ):
            raise FederationValidationError(
                "missing-completion-time",
                "completed_at",
                "completed recovery requires a completion timestamp",
            )

    def immutable_binding(self) -> tuple[Any, ...]:
        return (
            self.schema,
            self.recovery_id,
            self.session_id,
            self.group_id,
            self.returning_provider_id,
            self.returning_node_id,
            self.source_provider_id,
            self.source_node_id,
            self.promotion_id,
            self.final_authority_identity,
            self.publication_control_revision,
            self.manifest_revision,
            self.manifest_hash,
            self.grant_id,
            self.term,
            self.fencing_token,
            self.lease_expires_at,
            self.report_revision,
            self.report_hash,
        )


@dataclass(frozen=True)
class FormerPrimaryRecoveryPlan:
    record: FormerPrimaryRecoveryRecord
    items: tuple[FormerPrimaryRecoveryItem, ...]


class RecoveryControlPlane(Protocol):
    database: str

    def snapshot(self, session_id: str): ...

    def manifest(self, session_id: str, group_id: str): ...

    def promotion_transaction(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
    ): ...

    def promotion_finalization(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
    ): ...

    def latest_storage_replica_assessment(
        self,
        session_id: str,
        group_id: str,
        provider_id: str,
    ) -> StorageReplicaAssessment | None: ...


class FormerPrimaryRecoveryStore:
    """Durable recovery and immutable item ledger in the coordinator database."""

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS storage_former_primary_recoveries (
                    recovery_id TEXT PRIMARY KEY,
                    schema TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    returning_provider_id TEXT NOT NULL,
                    returning_node_id TEXT NOT NULL,
                    source_provider_id TEXT NOT NULL,
                    source_node_id TEXT NOT NULL,
                    promotion_id TEXT NOT NULL,
                    final_authority_identity TEXT NOT NULL,
                    publication_control_revision INTEGER NOT NULL
                        CHECK(publication_control_revision >= 0),
                    manifest_revision INTEGER NOT NULL
                        CHECK(manifest_revision >= 0),
                    manifest_hash TEXT NOT NULL,
                    grant_id TEXT NOT NULL,
                    term INTEGER NOT NULL CHECK(term >= 0),
                    fencing_token INTEGER NOT NULL CHECK(fencing_token >= 0),
                    lease_expires_at TEXT NOT NULL,
                    report_revision INTEGER NOT NULL CHECK(report_revision >= 0),
                    report_hash TEXT NOT NULL,
                    state TEXT NOT NULL,
                    item_count INTEGER NOT NULL CHECK(item_count >= 0),
                    present_count INTEGER NOT NULL CHECK(present_count >= 0),
                    missing_count INTEGER NOT NULL CHECK(missing_count >= 0),
                    conflict_count INTEGER NOT NULL CHECK(conflict_count >= 0),
                    latest_error_code TEXT,
                    latest_error_reason TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT,
                    UNIQUE(
                        session_id,
                        group_id,
                        returning_provider_id,
                        promotion_id,
                        manifest_hash
                    )
                );
                CREATE TABLE IF NOT EXISTS storage_former_primary_recovery_items (
                    recovery_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    returning_provider_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    schema_name TEXT NOT NULL,
                    schema_version INTEGER NOT NULL CHECK(schema_version > 0),
                    idempotency_key TEXT,
                    content_hash TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
                    source_id TEXT,
                    first_sequence INTEGER,
                    last_sequence INTEGER,
                    commit_state TEXT NOT NULL,
                    status TEXT NOT NULL,
                    delivery_id TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL CHECK(attempt_count >= 0),
                    last_error_code TEXT,
                    last_error_reason TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(recovery_id, item_id),
                    UNIQUE(recovery_id, delivery_id),
                    FOREIGN KEY(recovery_id)
                        REFERENCES storage_former_primary_recoveries(recovery_id)
                        ON DELETE CASCADE
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def create_plan(
        self,
        record: FormerPrimaryRecoveryRecord,
        items: tuple[FormerPrimaryRecoveryItem, ...],
    ) -> FormerPrimaryRecoveryPlan:
        ordered = tuple(sorted(items, key=lambda item: item.item_id))
        if len(ordered) != record.item_count:
            raise FederationValidationError(
                "recovery-item-count-mismatch",
                "items",
                "item ledger does not match the recovery record",
            )
        if any(
            item.recovery_id != record.recovery_id
            or item.session_id != record.session_id
            or item.group_id != record.group_id
            or item.returning_provider_id != record.returning_provider_id
            for item in ordered
        ):
            raise FederationValidationError(
                "recovery-item-scope-mismatch",
                "items",
                "item ledger differs from recovery scope",
            )
        counts = Counter(item.status for item in ordered)
        if (
            counts[FormerPrimaryItemStatus.PRESENT] != record.present_count
            or counts[FormerPrimaryItemStatus.MISSING] != record.missing_count
            or counts[FormerPrimaryItemStatus.CONFLICT] != record.conflict_count
        ):
            raise FederationValidationError(
                "recovery-item-count-mismatch",
                "items",
                "item statuses do not match recovery counts",
            )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """SELECT recovery_id FROM storage_former_primary_recoveries
                   WHERE recovery_id=?""",
                (record.recovery_id,),
            ).fetchone()
            if existing is not None:
                connection.commit()
                restored = self.get(record.recovery_id)
                if restored is None:
                    raise FederationValidationError(
                        "corrupt-former-primary-recovery",
                        "recovery_id",
                        "existing recovery disappeared",
                    )
                if restored.record.immutable_binding() != record.immutable_binding():
                    raise FederationValidationError(
                        "conflicting-former-primary-recovery",
                        "recovery_id",
                        "recovery identity is bound to different authority evidence",
                    )
                return restored
            self._insert_record(connection, record)
            for item in ordered:
                self._insert_item(connection, item)
            connection.commit()
        restored = self.get(record.recovery_id)
        if restored is None:
            raise FederationValidationError(
                "corrupt-former-primary-recovery",
                "recovery_id",
                "persisted recovery could not be restored",
            )
        return restored

    def get(self, recovery_id: str) -> FormerPrimaryRecoveryPlan | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM storage_former_primary_recoveries
                   WHERE recovery_id=?""",
                (recovery_id,),
            ).fetchone()
            if row is None:
                return None
            item_rows = connection.execute(
                """SELECT * FROM storage_former_primary_recovery_items
                   WHERE recovery_id=? ORDER BY item_id""",
                (recovery_id,),
            ).fetchall()
        try:
            record = self._record_from_row(row)
            items = tuple(self._item_from_row(item) for item in item_rows)
        except (
            KeyError,
            TypeError,
            ValueError,
            FederationValidationError,
        ) as exc:
            raise FederationValidationError(
                "corrupt-former-primary-recovery",
                "storage_former_primary_recoveries",
                "durable recovery state is malformed",
            ) from exc
        if len(items) != record.item_count:
            raise FederationValidationError(
                "corrupt-former-primary-recovery",
                "item_count",
                "durable recovery item count is inconsistent",
            )
        return FormerPrimaryRecoveryPlan(record, items)

    def recoveries(
        self,
        *,
        session_id: str,
        group_id: str,
        returning_provider_id: str,
    ) -> tuple[FormerPrimaryRecoveryPlan, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT recovery_id FROM storage_former_primary_recoveries
                   WHERE session_id=? AND group_id=?
                     AND returning_provider_id=?
                   ORDER BY created_at, recovery_id""",
                (session_id, group_id, returning_provider_id),
            ).fetchall()
        return tuple(
            plan
            for row in rows
            if (plan := self.get(str(row["recovery_id"]))) is not None
        )

    @staticmethod
    def _insert_record(
        connection: sqlite3.Connection,
        record: FormerPrimaryRecoveryRecord,
    ) -> None:
        connection.execute(
            """INSERT INTO storage_former_primary_recoveries
               (recovery_id, schema, session_id, group_id,
                returning_provider_id, returning_node_id,
                source_provider_id, source_node_id, promotion_id,
                final_authority_identity, publication_control_revision,
                manifest_revision, manifest_hash, grant_id, term,
                fencing_token, lease_expires_at, report_revision,
                report_hash, state, item_count, present_count,
                missing_count, conflict_count, latest_error_code,
                latest_error_reason, created_at, updated_at, completed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                record.recovery_id,
                record.schema,
                record.session_id,
                record.group_id,
                record.returning_provider_id,
                record.returning_node_id,
                record.source_provider_id,
                record.source_node_id,
                record.promotion_id,
                record.final_authority_identity,
                record.publication_control_revision,
                record.manifest_revision,
                record.manifest_hash,
                record.grant_id,
                record.term,
                record.fencing_token,
                _timestamp(record.lease_expires_at),
                record.report_revision,
                record.report_hash,
                record.state.value,
                record.item_count,
                record.present_count,
                record.missing_count,
                record.conflict_count,
                record.latest_error_code,
                record.latest_error_reason,
                _timestamp(record.created_at),
                _timestamp(record.updated_at),
                (
                    None
                    if record.completed_at is None
                    else _timestamp(record.completed_at)
                ),
            ),
        )

    @staticmethod
    def _insert_item(
        connection: sqlite3.Connection,
        item: FormerPrimaryRecoveryItem,
    ) -> None:
        connection.execute(
            """INSERT INTO storage_former_primary_recovery_items
               (recovery_id, session_id, group_id, returning_provider_id,
                item_id, kind, dataset_id, schema_name, schema_version,
                idempotency_key, content_hash, size_bytes, source_id,
                first_sequence, last_sequence, commit_state, status,
                delivery_id, attempt_count, last_error_code,
                last_error_reason, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       ?, ?, ?, ?, ?, ?)""",
            (
                item.recovery_id,
                item.session_id,
                item.group_id,
                item.returning_provider_id,
                item.item_id,
                item.kind.value,
                item.dataset_id,
                item.schema_name,
                item.schema_version,
                item.idempotency_key,
                item.content_hash,
                item.size_bytes,
                item.source_id,
                item.first_sequence,
                item.last_sequence,
                item.commit_state.value,
                item.status.value,
                item.delivery_id,
                item.attempt_count,
                item.last_error_code,
                item.last_error_reason,
                _timestamp(item.updated_at),
            ),
        )

    @staticmethod
    def _record_from_row(row: sqlite3.Row) -> FormerPrimaryRecoveryRecord:
        return FormerPrimaryRecoveryRecord(
            schema=row["schema"],
            recovery_id=row["recovery_id"],
            session_id=row["session_id"],
            group_id=row["group_id"],
            returning_provider_id=row["returning_provider_id"],
            returning_node_id=row["returning_node_id"],
            source_provider_id=row["source_provider_id"],
            source_node_id=row["source_node_id"],
            promotion_id=row["promotion_id"],
            final_authority_identity=row["final_authority_identity"],
            publication_control_revision=int(row["publication_control_revision"]),
            manifest_revision=int(row["manifest_revision"]),
            manifest_hash=row["manifest_hash"],
            grant_id=row["grant_id"],
            term=int(row["term"]),
            fencing_token=int(row["fencing_token"]),
            lease_expires_at=row["lease_expires_at"],
            report_revision=int(row["report_revision"]),
            report_hash=row["report_hash"],
            state=row["state"],
            item_count=int(row["item_count"]),
            present_count=int(row["present_count"]),
            missing_count=int(row["missing_count"]),
            conflict_count=int(row["conflict_count"]),
            latest_error_code=row["latest_error_code"],
            latest_error_reason=row["latest_error_reason"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
        )

    @staticmethod
    def _item_from_row(row: sqlite3.Row) -> FormerPrimaryRecoveryItem:
        return FormerPrimaryRecoveryItem(
            recovery_id=row["recovery_id"],
            session_id=row["session_id"],
            group_id=row["group_id"],
            returning_provider_id=row["returning_provider_id"],
            item_id=row["item_id"],
            kind=row["kind"],
            dataset_id=row["dataset_id"],
            schema_name=row["schema_name"],
            schema_version=int(row["schema_version"]),
            idempotency_key=row["idempotency_key"],
            content_hash=row["content_hash"],
            size_bytes=int(row["size_bytes"]),
            source_id=row["source_id"],
            first_sequence=row["first_sequence"],
            last_sequence=row["last_sequence"],
            commit_state=row["commit_state"],
            status=row["status"],
            delivery_id=row["delivery_id"],
            attempt_count=int(row["attempt_count"]),
            last_error_code=row["last_error_code"],
            last_error_reason=row["last_error_reason"],
            updated_at=row["updated_at"],
        )


class FormerPrimaryRecoveryPlanner:
    """Create and revalidate deterministic E7.0/E7.1 recovery plans."""

    def __init__(
        self,
        *,
        control: RecoveryControlPlane,
        returning_provider: BatchStorageProvider,
        former_provider_fence_store: ProviderFenceStore,
        clock=lambda: datetime.now(timezone.utc),
    ) -> None:
        self.control = control
        self.returning_provider = returning_provider
        self.former_provider_fence_store = former_provider_fence_store
        self.clock = clock
        self.store = FormerPrimaryRecoveryStore(control.database)

    def plan(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
        *,
        returning_provider_id: str | None = None,
    ) -> FormerPrimaryRecoveryPlan:
        binding = self._binding(
            session_id,
            group_id,
            promotion_id,
            returning_provider_id=returning_provider_id,
        )
        recovery_id = _recovery_id(
            session_id=session_id,
            group_id=group_id,
            returning_provider_id=binding["returning_provider_id"],
            promotion_id=promotion_id,
            final_authority_identity=binding["finalization"].final_authority_identity,
            manifest_hash=binding["manifest"].manifest_hash,
        )
        existing = self.store.get(recovery_id)
        if existing is not None:
            self._validate_existing(existing, binding)
            return existing

        report = binding["assessment"].report
        assert report is not None
        authoritative_hashes = Counter(
            item.content_hash for item in binding["manifest"].items
        )
        reported_hashes = Counter(report.committed_item_hashes)
        unknown_hashes = reported_hashes - authoritative_hashes
        global_error_code: str | None = None
        global_error_reason: str | None = None
        if not report.integrity_verified:
            global_error_code = "returning-provider-integrity-failure"
            global_error_reason = (
                "returning provider inventory is not integrity verified"
            )
        elif report.manifest_revision > binding["manifest"].revision:
            global_error_code = "returning-provider-manifest-ahead"
            global_error_reason = (
                "returning provider reports a manifest revision ahead of authority"
            )
        elif (
            report.manifest_revision == binding["manifest"].revision
            and report.manifest_hash != binding["manifest"].manifest_hash
        ):
            global_error_code = "returning-provider-manifest-conflict"
            global_error_reason = (
                "returning provider reports a conflicting current manifest hash"
            )
        elif unknown_hashes:
            global_error_code = "returning-provider-extra-hash"
            global_error_reason = (
                "returning provider reports committed hashes outside authority"
            )

        now = _utc(self.clock())
        items: list[FormerPrimaryRecoveryItem] = []
        available_report_hashes = Counter(report.committed_item_hashes)
        for manifest_item in binding["manifest"].items:
            identity = self.returning_provider.committed_identity(
                session_id=session_id,
                group_id=group_id,
                batch_id=manifest_item.item_id,
            )
            status, item_error_code, item_error_reason = self._classify_item(
                manifest_item,
                identity,
                available_report_hashes,
            )
            items.append(
                FormerPrimaryRecoveryItem.from_manifest(
                    recovery_id=recovery_id,
                    session_id=session_id,
                    group_id=group_id,
                    returning_provider_id=binding["returning_provider_id"],
                    item=manifest_item,
                    status=status,
                    now=now,
                    error_code=item_error_code,
                    error_reason=item_error_reason,
                )
            )

        counts = Counter(item.status for item in items)
        if counts[FormerPrimaryItemStatus.CONFLICT] and global_error_code is None:
            global_error_code = "returning-provider-immutable-conflict"
            global_error_reason = (
                "returning provider contains conflicting immutable item state"
            )
        state = (
            FormerPrimaryRecoveryState.OPERATOR_ATTENTION
            if global_error_code is not None
            else FormerPrimaryRecoveryState.PLANNED
        )
        publication: PromotionPublicationRecord = binding["publication"]
        assessment: StorageReplicaAssessment = binding["assessment"]
        record = FormerPrimaryRecoveryRecord(
            schema=FORMER_PRIMARY_RECOVERY_SCHEMA,
            recovery_id=recovery_id,
            session_id=session_id,
            group_id=group_id,
            returning_provider_id=binding["returning_provider_id"],
            returning_node_id=binding["returning"].node_id,
            source_provider_id=binding["source"].provider_id,
            source_node_id=binding["source"].node_id,
            promotion_id=promotion_id,
            final_authority_identity=(
                binding["finalization"].final_authority_identity
            ),
            publication_control_revision=publication.control_revision,
            manifest_revision=binding["manifest"].revision,
            manifest_hash=binding["manifest"].manifest_hash,
            grant_id=publication.grant_id,
            term=publication.term,
            fencing_token=publication.fencing_token,
            lease_expires_at=publication.lease_expires_at,
            report_revision=assessment.report_revision,
            report_hash=assessment.report_hash,
            state=state,
            item_count=len(items),
            present_count=counts[FormerPrimaryItemStatus.PRESENT],
            missing_count=counts[FormerPrimaryItemStatus.MISSING],
            conflict_count=counts[FormerPrimaryItemStatus.CONFLICT],
            latest_error_code=global_error_code,
            latest_error_reason=global_error_reason,
            created_at=now,
            updated_at=now,
        )
        return self.store.create_plan(record, tuple(items))

    def validate_plan(self, recovery_id: str) -> FormerPrimaryRecoveryPlan:
        plan = self.store.get(recovery_id)
        if plan is None:
            raise FederationValidationError(
                "unknown-former-primary-recovery",
                "recovery_id",
                "durable recovery plan does not exist",
            )
        binding = self._binding(
            plan.record.session_id,
            plan.record.group_id,
            plan.record.promotion_id,
            returning_provider_id=plan.record.returning_provider_id,
        )
        self._validate_existing(plan, binding)
        return plan

    @staticmethod
    def _classify_item(
        item: ManifestItem,
        identity: CommittedBatchIdentity | None,
        available_report_hashes: Counter[str],
    ) -> tuple[
        FormerPrimaryItemStatus,
        str | None,
        str | None,
    ]:
        if identity is None:
            return FormerPrimaryItemStatus.MISSING, None, None
        expected = (
            item.dataset_id,
            item.schema_name,
            item.schema_version,
            item.item_id,
            item.idempotency_key,
            item.content_hash,
        )
        actual = (
            identity.dataset_id,
            identity.dataset_schema_name,
            identity.dataset_schema_version,
            identity.batch_id,
            identity.idempotency_key,
            identity.content_hash,
        )
        if actual != expected:
            return (
                FormerPrimaryItemStatus.CONFLICT,
                "returning-provider-immutable-conflict",
                "provider-local identity differs from the authoritative item",
            )
        if available_report_hashes[item.content_hash] <= 0:
            return (
                FormerPrimaryItemStatus.CONFLICT,
                "returning-report-identity-mismatch",
                "provider-local identity is absent from authenticated report evidence",
            )
        available_report_hashes[item.content_hash] -= 1
        return FormerPrimaryItemStatus.PRESENT, None, None

    def _binding(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
        *,
        returning_provider_id: str | None,
    ) -> dict[str, Any]:
        transaction = self.control.promotion_transaction(
            session_id,
            group_id,
            promotion_id,
        )
        finalization = self.control.promotion_finalization(
            session_id,
            group_id,
            promotion_id,
        )
        publication = promotion_publication(
            self.control,
            session_id,
            group_id,
            promotion_id,
        )
        if (
            transaction is None
            or finalization is None
            or publication is None
            or transaction.state != "finalized"
            or not finalization.degraded_cleared
            or transaction.previous_provider_id is None
            or transaction.previous_term is None
        ):
            raise FederationValidationError(
                "promotion-not-complete",
                "promotion_id",
                "E7 requires completed E6 finalization and publication",
            )
        expected_returning = transaction.previous_provider_id
        returning_provider_id = returning_provider_id or expected_returning
        if (
            returning_provider_id != expected_returning
            or publication.previous_provider_id != expected_returning
            or publication.selected_provider_id != finalization.selected_provider_id
            or publication.final_authority_identity
            != finalization.final_authority_identity
        ):
            raise FederationValidationError(
                "former-primary-binding-mismatch",
                "returning_provider_id",
                "returning provider differs from E6 authority evidence",
            )
        snapshot = self.control.snapshot(session_id)
        assignment = snapshot.groups.get(group_id)
        grant = snapshot.leader_grants.get(group_id)
        returning = snapshot.providers.get(returning_provider_id)
        source = snapshot.providers.get(publication.selected_provider_id)
        if (
            assignment is None
            or assignment.primary_provider_id != publication.selected_provider_id
            or returning_provider_id not in assignment.replica_provider_ids
            or returning is None
            or source is None
            or grant is None
            or grant.get("provider_id") != publication.selected_provider_id
            or grant.get("grant_id") != publication.grant_id
            or int(grant.get("term", -1)) != publication.term
            or int(grant.get("fencing_token", -1))
            != publication.fencing_token
            or _utc(
                str(grant.get("lease_expires_at")),
                "lease_expires_at",
            )
            != publication.lease_expires_at
        ):
            raise FederationValidationError(
                "former-primary-authority-mismatch",
                "group_id",
                "current assignment or grant differs from E6 publication",
            )
        self._validate_fence(
            session_id,
            group_id,
            returning_provider_id,
            promotion_id,
            transaction.previous_term,
        )
        manifest = self.control.manifest(session_id, group_id)
        assessment = self.control.latest_storage_replica_assessment(
            session_id,
            group_id,
            returning_provider_id,
        )
        if assessment is None or assessment.report is None:
            raise FederationValidationError(
                "returning-report-required",
                "returning_provider_id",
                "authenticated returning-provider evidence is required",
            )
        if (
            not assessment.accepted
            or assessment.report.session_id != session_id
            or assessment.report.group_id != group_id
            or assessment.report.provider_id != returning_provider_id
        ):
            raise FederationValidationError(
                "returning-report-rejected",
                "report",
                "latest returning-provider evidence is not accepted for this scope",
            )
        return {
            "transaction": transaction,
            "finalization": finalization,
            "publication": publication,
            "snapshot": snapshot,
            "assignment": assignment,
            "grant": grant,
            "returning": returning,
            "source": source,
            "manifest": manifest,
            "assessment": assessment,
            "returning_provider_id": returning_provider_id,
        }

    def _validate_fence(
        self,
        session_id: str,
        group_id: str,
        returning_provider_id: str,
        promotion_id: str,
        previous_term: int,
    ) -> None:
        store = self.former_provider_fence_store
        evidence = store.acknowledgement(group_id)
        if (
            store.session_id != session_id
            or store.provider_id != returning_provider_id
            or evidence is None
            or evidence.promotion_id != promotion_id
            or evidence.provider_id != returning_provider_id
            or evidence.previous_term != previous_term
            or evidence.fencing_high_water_term < previous_term
            or store.high_water_term(group_id) < previous_term
        ):
            raise FederationValidationError(
                "former-primary-not-fenced",
                "returning_provider_id",
                "durable former-primary fence is missing, stale, or unrelated",
            )

    @staticmethod
    def _validate_existing(
        plan: FormerPrimaryRecoveryPlan,
        binding: dict[str, Any],
    ) -> None:
        record = plan.record
        publication: PromotionPublicationRecord = binding["publication"]
        assessment: StorageReplicaAssessment = binding["assessment"]
        if (
            record.final_authority_identity
            != binding["finalization"].final_authority_identity
            or record.publication_control_revision
            != publication.control_revision
            or record.manifest_revision != binding["manifest"].revision
            or record.manifest_hash != binding["manifest"].manifest_hash
            or record.grant_id != publication.grant_id
            or record.term != publication.term
            or record.fencing_token != publication.fencing_token
            or record.lease_expires_at != publication.lease_expires_at
        ):
            raise FederationValidationError(
                "former-primary-replan-required",
                "recovery_id",
                "manifest or active authority changed after planning",
            )
        if (
            record.report_revision != assessment.report_revision
            or record.report_hash != assessment.report_hash
        ):
            raise FederationValidationError(
                "former-primary-report-changed",
                "report_revision",
                "returning-provider evidence changed after planning",
            )
