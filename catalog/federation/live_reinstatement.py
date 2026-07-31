"""Safe live F4.2 reinstatement of a caught-up former primary as replica.

F4.2 consumes the durable F4.1 catch-up evidence, publishes a replica
assignment while acknowledgement policy remains degraded, obtains fresh live
proof from the assigned node, and then atomically restores the pre-failover
acknowledgement policy and clears only the matching degraded state. Leadership
never moves in this phase.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

from .acknowledgement import AcknowledgementMode
from .control_sync import StorageControlPlan, StorageControlPublicationStore
from .errors import FederationValidationError
from .live_catchup import (
    LiveCatchupRecord,
    LiveCatchupStore,
    LiveFormerPrimaryCatchupCoordinator,
)
from .live_failover import (
    LIVE_FAILOVER_SCHEMA,
    LiveFailoverRecord,
    LiveFailoverStore,
    StorageControlRelayChannel,
)
from .phase_d_control import PhaseDControlPlane
from .reporting import StorageReplicaAssessment, StorageReplicaReport

LIVE_REINSTATEMENT_SCHEMA = "msh.live_storage_reinstatement.v1"
LIVE_REINSTATEMENT_RESULT_SCHEMA = "msh.live_storage_reinstatement_result.v1"

_STATE_PLANNED = "planned"
_STATE_ASSIGNED = "assigned"
_STATE_PUBLISHED = "published"
_STATE_REPORT_ACCEPTED = "report-accepted"
_STATE_CONTROL_RESTORED = "control-restored"
_STATE_ROLLBACK_PENDING = "rollback-pending"
_STATE_ROLLED_BACK = "rolled-back"
_STATE_COMPLETED = "completed"
_STATE_OPERATOR = "operator-attention"
_VALID_STATES = {
    _STATE_PLANNED,
    _STATE_ASSIGNED,
    _STATE_PUBLISHED,
    _STATE_REPORT_ACCEPTED,
    _STATE_CONTROL_RESTORED,
    _STATE_ROLLBACK_PENDING,
    _STATE_ROLLED_BACK,
    _STATE_COMPLETED,
    _STATE_OPERATOR,
}
_TERMINAL_STATES = {_STATE_ROLLED_BACK, _STATE_COMPLETED, _STATE_OPERATOR}
_TRANSIENT_ERROR_CODES = {
    "storage-control-channel-closed",
    "storage-control-channel-not-started",
    "storage-control-delivery-failed",
    "live-replica-report-delivery-failed",
}


def _retryable_validation(error: FederationValidationError) -> bool:
    return (
        error.code in _TRANSIENT_ERROR_CODES
        or error.code.endswith("-delivery-failed")
    )


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
            "invalid-live-reinstatement-field", field, "must be non-empty text"
        )
    return value


def _uint(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FederationValidationError(
            "invalid-live-reinstatement-counter", field, "must be non-negative"
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


def _degraded_hash(state: dict[str, Any]) -> str:
    return "sha256:" + hashlib.sha256(_json(state).encode()).hexdigest()


@dataclass(frozen=True)
class LiveReinstatementRecord:
    schema: str
    reinstatement_id: str
    session_id: str
    group_id: str
    failover_id: str
    catchup_recovery_id: str
    source_provider_id: str
    source_node_id: str
    returning_provider_id: str
    returning_node_id: str
    previous_replica_provider_ids: tuple[str, ...]
    restored_replica_provider_ids: tuple[str, ...]
    previous_acknowledgement_mode: AcknowledgementMode
    effective_acknowledgement_mode: AcknowledgementMode
    manifest_revision: int
    manifest_hash: str
    grant_id: str
    term: int
    fencing_token: int
    lease_expires_at: datetime
    catchup_report_revision: int
    catchup_report_hash: str
    degraded_state_hash: str
    state: str
    assignment_publication: StorageControlPlan | None
    rollback_publication: StorageControlPlan | None
    final_publication: StorageControlPlan | None
    final_report_revision: int | None
    final_report_hash: str | None
    latest_error_code: str | None
    latest_error_reason: str | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None

    def __post_init__(self) -> None:
        if self.schema != LIVE_REINSTATEMENT_SCHEMA:
            raise FederationValidationError(
                "unsupported-live-reinstatement",
                "schema",
                f"expected {LIVE_REINSTATEMENT_SCHEMA}",
            )
        for field in (
            "reinstatement_id",
            "session_id",
            "group_id",
            "failover_id",
            "catchup_recovery_id",
            "source_provider_id",
            "source_node_id",
            "returning_provider_id",
            "returning_node_id",
            "manifest_hash",
            "grant_id",
            "catchup_report_hash",
            "degraded_state_hash",
        ):
            object.__setattr__(self, field, _text(getattr(self, field), field))
        for field in (
            "manifest_revision",
            "term",
            "fencing_token",
            "catchup_report_revision",
        ):
            object.__setattr__(self, field, _uint(getattr(self, field), field))
        previous = tuple(
            sorted(_text(value, "previous_replica_provider_ids") for value in self.previous_replica_provider_ids)
        )
        restored = tuple(
            sorted(_text(value, "restored_replica_provider_ids") for value in self.restored_replica_provider_ids)
        )
        if len(previous) != len(set(previous)) or len(restored) != len(set(restored)):
            raise FederationValidationError(
                "invalid-live-reinstatement-replicas",
                "replica_provider_ids",
                "replica IDs must be unique",
            )
        if self.returning_provider_id in previous:
            raise FederationValidationError(
                "invalid-live-reinstatement-replicas",
                "previous_replica_provider_ids",
                "returning provider must be unassigned before F4.2",
            )
        if set(restored) != {*previous, self.returning_provider_id}:
            raise FederationValidationError(
                "invalid-live-reinstatement-replicas",
                "restored_replica_provider_ids",
                "restored replicas must add exactly the returning provider",
            )
        if self.source_provider_id in restored:
            raise FederationValidationError(
                "invalid-live-reinstatement-replicas",
                "restored_replica_provider_ids",
                "current primary cannot also be a replica",
            )
        object.__setattr__(self, "previous_replica_provider_ids", previous)
        object.__setattr__(self, "restored_replica_provider_ids", restored)
        try:
            object.__setattr__(
                self,
                "previous_acknowledgement_mode",
                AcknowledgementMode(self.previous_acknowledgement_mode),
            )
            object.__setattr__(
                self,
                "effective_acknowledgement_mode",
                AcknowledgementMode(self.effective_acknowledgement_mode),
            )
        except ValueError as exc:
            raise FederationValidationError(
                "invalid-live-reinstatement-mode",
                "acknowledgement_mode",
                "unknown acknowledgement mode",
            ) from exc
        if self.state not in _VALID_STATES:
            raise FederationValidationError(
                "invalid-live-reinstatement-state", "state", "unknown state"
            )
        object.__setattr__(self, "lease_expires_at", _utc(self.lease_expires_at))
        object.__setattr__(self, "created_at", _utc(self.created_at))
        object.__setattr__(self, "updated_at", _utc(self.updated_at))
        if self.completed_at is not None:
            object.__setattr__(self, "completed_at", _utc(self.completed_at))
        if (self.final_report_revision is None) != (self.final_report_hash is None):
            raise FederationValidationError(
                "invalid-live-reinstatement-report",
                "final_report",
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
                "invalid-live-reinstatement-error",
                "latest_error_code",
                "error code and reason must appear together",
            )
        if self.state == _STATE_COMPLETED and self.completed_at is None:
            raise FederationValidationError(
                "invalid-live-reinstatement-completion",
                "completed_at",
                "completed records require a completion timestamp",
            )

    def immutable_binding(self) -> tuple[Any, ...]:
        return (
            self.schema,
            self.reinstatement_id,
            self.session_id,
            self.group_id,
            self.failover_id,
            self.catchup_recovery_id,
            self.source_provider_id,
            self.source_node_id,
            self.returning_provider_id,
            self.returning_node_id,
            self.previous_replica_provider_ids,
            self.restored_replica_provider_ids,
            self.previous_acknowledgement_mode,
            self.effective_acknowledgement_mode,
            self.manifest_revision,
            self.manifest_hash,
            self.grant_id,
            self.term,
            self.fencing_token,
            self.lease_expires_at,
            self.catchup_report_revision,
            self.catchup_report_hash,
            self.created_at,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "reinstatement_id": self.reinstatement_id,
            "session_id": self.session_id,
            "group_id": self.group_id,
            "failover_id": self.failover_id,
            "catchup_recovery_id": self.catchup_recovery_id,
            "source_provider_id": self.source_provider_id,
            "source_node_id": self.source_node_id,
            "returning_provider_id": self.returning_provider_id,
            "returning_node_id": self.returning_node_id,
            "previous_replica_provider_ids": list(self.previous_replica_provider_ids),
            "restored_replica_provider_ids": list(self.restored_replica_provider_ids),
            "previous_acknowledgement_mode": self.previous_acknowledgement_mode.value,
            "effective_acknowledgement_mode": self.effective_acknowledgement_mode.value,
            "manifest_revision": self.manifest_revision,
            "manifest_hash": self.manifest_hash,
            "grant_id": self.grant_id,
            "term": self.term,
            "fencing_token": self.fencing_token,
            "lease_expires_at": _stamp(self.lease_expires_at),
            "catchup_report_revision": self.catchup_report_revision,
            "catchup_report_hash": self.catchup_report_hash,
            "degraded_state_hash": self.degraded_state_hash,
            "state": self.state,
            "assignment_publication": (
                None if self.assignment_publication is None else self.assignment_publication.to_dict()
            ),
            "rollback_publication": (
                None if self.rollback_publication is None else self.rollback_publication.to_dict()
            ),
            "final_publication": (
                None if self.final_publication is None else self.final_publication.to_dict()
            ),
            "final_report_revision": self.final_report_revision,
            "final_report_hash": self.final_report_hash,
            "latest_error_code": self.latest_error_code,
            "latest_error_reason": self.latest_error_reason,
            "created_at": _stamp(self.created_at),
            "updated_at": _stamp(self.updated_at),
            "completed_at": None if self.completed_at is None else _stamp(self.completed_at),
        }

    @classmethod
    def from_dict(cls, value: Any) -> LiveReinstatementRecord:
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-live-reinstatement", "record", "must be an object"
            )
        restored = dict(value)
        for field in (
            "assignment_publication",
            "rollback_publication",
            "final_publication",
        ):
            publication = value.get(field)
            restored[field] = (
                None if publication is None else StorageControlPlan.from_dict(publication)
            )
        return cls(**restored)


class LiveReinstatementStore:
    """Durable F4.2 progress stored with the authoritative control database."""

    def __init__(self, database: str) -> None:
        self.database = str(database)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """CREATE TABLE IF NOT EXISTS storage_live_reinstatements (
                    reinstatement_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    returning_provider_id TEXT NOT NULL,
                    failover_id TEXT NOT NULL,
                    catchup_recovery_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    record_json TEXT NOT NULL CHECK(json_valid(record_json)),
                    updated_at TEXT NOT NULL,
                    UNIQUE(session_id, group_id, returning_provider_id, catchup_recovery_id)
                )"""
            )
            connection.execute(
                """CREATE INDEX IF NOT EXISTS storage_live_reinstatements_active
                   ON storage_live_reinstatements(
                       session_id, group_id, returning_provider_id, state, updated_at
                   )"""
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @staticmethod
    def _decode(row: sqlite3.Row) -> LiveReinstatementRecord:
        try:
            record = LiveReinstatementRecord.from_dict(json.loads(row["record_json"]))
        except (json.JSONDecodeError, FederationValidationError) as exc:
            raise FederationValidationError(
                "corrupt-live-reinstatement",
                "record_json",
                "persisted reinstatement record is malformed",
            ) from exc
        if (
            row["reinstatement_id"] != record.reinstatement_id
            or row["session_id"] != record.session_id
            or row["group_id"] != record.group_id
            or row["returning_provider_id"] != record.returning_provider_id
            or row["failover_id"] != record.failover_id
            or row["catchup_recovery_id"] != record.catchup_recovery_id
            or row["state"] != record.state
            or row["updated_at"] != _stamp(record.updated_at)
        ):
            raise FederationValidationError(
                "live-reinstatement-row-mismatch",
                "record_json",
                "indexed metadata differs from its canonical record",
            )
        return record

    def get(self, reinstatement_id: str) -> LiveReinstatementRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM storage_live_reinstatements
                   WHERE reinstatement_id=?""",
                (reinstatement_id,),
            ).fetchone()
        return None if row is None else self._decode(row)

    def latest(
        self,
        session_id: str,
        group_id: str,
        returning_provider_id: str,
    ) -> LiveReinstatementRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM storage_live_reinstatements
                   WHERE session_id=? AND group_id=? AND returning_provider_id=?
                   ORDER BY updated_at DESC LIMIT 1""",
                (session_id, group_id, returning_provider_id),
            ).fetchone()
        return None if row is None else self._decode(row)

    def active(
        self,
        session_id: str,
        group_id: str,
        returning_provider_id: str,
    ) -> LiveReinstatementRecord | None:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT * FROM storage_live_reinstatements
                   WHERE session_id=? AND group_id=? AND returning_provider_id=?
                     AND state NOT IN ('rolled-back','completed','operator-attention')
                   ORDER BY updated_at DESC""",
                (session_id, group_id, returning_provider_id),
            ).fetchall()
        if len(rows) > 1:
            raise FederationValidationError(
                "multiple-active-live-reinstatements",
                "group_id",
                "more than one unfinished reinstatement exists",
            )
        return None if not rows else self._decode(rows[0])

    def save(self, record: LiveReinstatementRecord) -> LiveReinstatementRecord:
        existing = self.get(record.reinstatement_id)
        if existing is not None and existing.immutable_binding() != record.immutable_binding():
            raise FederationValidationError(
                "conflicting-live-reinstatement",
                "reinstatement_id",
                "durable reinstatement identity was reused with different bindings",
            )
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO storage_live_reinstatements
                   (reinstatement_id, session_id, group_id, returning_provider_id,
                    failover_id, catchup_recovery_id, state, record_json, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(reinstatement_id) DO UPDATE SET
                     state=excluded.state,
                     record_json=excluded.record_json,
                     updated_at=excluded.updated_at""",
                (
                    record.reinstatement_id,
                    record.session_id,
                    record.group_id,
                    record.returning_provider_id,
                    record.failover_id,
                    record.catchup_recovery_id,
                    record.state,
                    _json(record.to_dict()),
                    _stamp(record.updated_at),
                ),
            )
        return record

    def restore_control_atomically(
        self,
        record: LiveReinstatementRecord,
        *,
        control: PhaseDControlPlane,
        failover: LiveFailoverRecord,
        assessment: StorageReplicaAssessment,
        now: datetime,
    ) -> LiveReinstatementRecord:
        if (
            assessment.report is None
            or not assessment.accepted
            or not assessment.eligibility
            or assessment.eligibility_reason != "eligible"
        ):
            raise FederationValidationError(
                "reinstatement-report-not-eligible",
                "assessment",
                assessment.eligibility_reason,
            )
        report = assessment.report
        completed = replace(
            record,
            state=_STATE_CONTROL_RESTORED,
            final_report_revision=assessment.report_revision,
            final_report_hash=assessment.report_hash,
            latest_error_code=None,
            latest_error_reason=None,
            updated_at=_utc(now),
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                snapshot = control.snapshot(record.session_id)
                assignment = snapshot.groups.get(record.group_id)
                grant = snapshot.leader_grants.get(record.group_id)
                if (
                    assignment is None
                    or assignment.primary_provider_id != record.source_provider_id
                    or tuple(assignment.replica_provider_ids)
                    != record.restored_replica_provider_ids
                ):
                    raise FederationValidationError(
                        "reinstatement-assignment-changed",
                        "group_id",
                        "replica assignment changed before redundancy restoration",
                    )
                if (
                    grant is None
                    or grant.get("provider_id") != record.source_provider_id
                    or grant.get("grant_id") != record.grant_id
                    or int(grant.get("term", -1)) != record.term
                    or int(grant.get("fencing_token", -1)) != record.fencing_token
                    or _utc(str(grant.get("lease_expires_at")))
                    != record.lease_expires_at
                ):
                    raise FederationValidationError(
                        "reinstatement-authority-changed",
                        "grant_id",
                        "current primary authority changed before restoration",
                    )
                head = connection.execute(
                    """SELECT revision, manifest_hash FROM storage_manifest_heads
                       WHERE session_id=? AND group_id=?""",
                    (record.session_id, record.group_id),
                ).fetchone()
                stored_report = connection.execute(
                    """SELECT report_revision, report_hash, assessment_json
                       FROM storage_replica_reports
                       WHERE session_id=? AND group_id=? AND provider_id=?""",
                    (
                        record.session_id,
                        record.group_id,
                        record.returning_provider_id,
                    ),
                ).fetchone()
                policy = connection.execute(
                    """SELECT mode FROM storage_ack_policies
                       WHERE session_id=? AND group_id=?""",
                    (record.session_id, record.group_id),
                ).fetchone()
                degraded_row = connection.execute(
                    """SELECT * FROM storage_degraded_states
                       WHERE session_id=? AND group_id=?""",
                    (record.session_id, record.group_id),
                ).fetchone()
                if (
                    head is None
                    or int(head["revision"]) != record.manifest_revision
                    or str(head["manifest_hash"]) != record.manifest_hash
                    or report.manifest_revision != record.manifest_revision
                    or report.manifest_hash != record.manifest_hash
                ):
                    raise FederationValidationError(
                        "reinstatement-manifest-changed",
                        "manifest_hash",
                        "authoritative manifest advanced after replica proof",
                    )
                if (
                    stored_report is None
                    or int(stored_report["report_revision"])
                    != assessment.report_revision
                    or str(stored_report["report_hash"]) != assessment.report_hash
                ):
                    raise FederationValidationError(
                        "reinstatement-report-changed",
                        "report_revision",
                        "stored replica proof changed before restoration",
                    )
                stored_assessment = StorageReplicaAssessment.from_dict(
                    json.loads(stored_report["assessment_json"])
                )
                if stored_assessment.to_dict() != assessment.to_dict():
                    raise FederationValidationError(
                        "reinstatement-report-changed",
                        "assessment",
                        "stored assessment differs from the accepted proof",
                    )
                if (
                    policy is None
                    or policy["mode"] != failover.effective_acknowledgement_mode.value
                ):
                    raise FederationValidationError(
                        "reinstatement-policy-changed",
                        "acknowledgement_mode",
                        "degraded acknowledgement policy changed concurrently",
                    )
                if degraded_row is None:
                    raise FederationValidationError(
                        "reinstatement-degraded-state-missing",
                        "storage_degraded_state",
                        "matching degraded state disappeared before restoration",
                    )
                degraded = {
                    "session_id": str(degraded_row["session_id"]),
                    "group_id": str(degraded_row["group_id"]),
                    "manifest_revision": int(degraded_row["manifest_revision"]),
                    "manifest_hash": str(degraded_row["manifest_hash"]),
                    "reason_code": str(degraded_row["reason_code"]),
                    "reason_detail": str(degraded_row["reason_detail"]),
                    "missing_ranges": json.loads(degraded_row["missing_ranges_json"]),
                    "obligations": json.loads(degraded_row["obligations_json"]),
                    "updated_at": str(degraded_row["updated_at"]),
                }
                if _degraded_hash(degraded) != record.degraded_state_hash:
                    raise FederationValidationError(
                        "reinstatement-degraded-state-changed",
                        "storage_degraded_state",
                        "degraded state is newer or unrelated",
                    )
                connection.execute(
                    """UPDATE storage_ack_policies SET mode=?
                       WHERE session_id=? AND group_id=? AND mode=?""",
                    (
                        failover.previous_acknowledgement_mode.value,
                        record.session_id,
                        record.group_id,
                        failover.effective_acknowledgement_mode.value,
                    ),
                )
                if connection.total_changes != 1:
                    raise FederationValidationError(
                        "reinstatement-policy-changed",
                        "acknowledgement_mode",
                        "acknowledgement policy changed during restoration",
                    )
                cursor = connection.execute(
                    """DELETE FROM storage_degraded_states
                       WHERE session_id=? AND group_id=? AND manifest_revision=?
                         AND manifest_hash=? AND reason_code=? AND reason_detail=?
                         AND missing_ranges_json=? AND obligations_json=? AND updated_at=?""",
                    (
                        degraded["session_id"],
                        degraded["group_id"],
                        degraded["manifest_revision"],
                        degraded["manifest_hash"],
                        degraded["reason_code"],
                        degraded["reason_detail"],
                        _json(degraded["missing_ranges"]),
                        _json(degraded["obligations"]),
                        degraded["updated_at"],
                    ),
                )
                if cursor.rowcount != 1:
                    raise FederationValidationError(
                        "reinstatement-degraded-state-changed",
                        "storage_degraded_state",
                        "degraded state changed before it could be cleared",
                    )
                cursor = connection.execute(
                    """UPDATE storage_live_reinstatements
                       SET state=?, record_json=?, updated_at=?
                       WHERE reinstatement_id=? AND state='report-accepted'""",
                    (
                        completed.state,
                        _json(completed.to_dict()),
                        _stamp(completed.updated_at),
                        completed.reinstatement_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise FederationValidationError(
                        "concurrent-live-reinstatement-change",
                        "reinstatement_id",
                        "durable reinstatement changed during control restoration",
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return completed


@dataclass(frozen=True)
class LiveReinstatementResult:
    status: str
    code: str
    reason: str
    reinstatement_id: str | None
    record: LiveReinstatementRecord | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": LIVE_REINSTATEMENT_RESULT_SCHEMA,
            "status": self.status,
            "code": self.code,
            "reason": self.reason,
            "reinstatement_id": self.reinstatement_id,
            "record": None if self.record is None else self.record.to_dict(),
        }


class LiveFormerPrimaryReinstatementCoordinator:
    """Restore one F4.1-complete former primary as a verified live replica."""

    def __init__(
        self,
        *,
        control_plane: PhaseDControlPlane,
        publication_store: StorageControlPublicationStore,
        failover_store: LiveFailoverStore,
        catchup_store: LiveCatchupStore,
        catchup: LiveFormerPrimaryCatchupCoordinator,
        channel: StorageControlRelayChannel,
        credentials: Any,
        session_id: str,
        clock: Any = _now,
    ) -> None:
        self.control_plane = control_plane
        self.publication_store = publication_store
        self.failover_store = failover_store
        self.catchup_store = catchup_store
        self.catchup = catchup
        self.channel = channel
        self.credentials = credentials
        self.session_id = _text(session_id, "session_id")
        self.clock = clock
        self.store = LiveReinstatementStore(control_plane.database)

    async def run_once(
        self,
        *,
        group_id: str,
        returning_provider_id: str,
        catchup_limit: int = 100,
    ) -> LiveReinstatementResult:
        group_id = _text(group_id, "group_id")
        returning_provider_id = _text(
            returning_provider_id, "returning_provider_id"
        )
        try:
            record = self.store.active(
                self.session_id, group_id, returning_provider_id
            )
            if record is None:
                latest = self.store.latest(
                    self.session_id, group_id, returning_provider_id
                )
                if latest is not None and latest.state == _STATE_COMPLETED:
                    return self._result(
                        "completed",
                        "live-reinstatement-complete",
                        "former primary is already a verified replica",
                        latest,
                    )
                catchup = await self.catchup.run_once(
                    group_id=group_id,
                    returning_provider_id=returning_provider_id,
                    limit=catchup_limit,
                )
                if catchup.status != "caught-up" or catchup.record is None:
                    return LiveReinstatementResult(
                        catchup.status,
                        catchup.code,
                        catchup.reason,
                        None,
                        None,
                    )
                binding = self._binding(catchup.record)
                record = self.store.save(self._new_record(catchup.record, binding))
            return await self._resume(record)
        except FederationValidationError as exc:
            record = locals().get("record")
            retryable = _retryable_validation(exc)
            if isinstance(record, LiveReinstatementRecord):
                try:
                    record = self.store.save(
                        replace(
                            record,
                            state=(record.state if retryable else _STATE_OPERATOR),
                            latest_error_code=exc.code,
                            latest_error_reason=str(exc),
                            updated_at=self.clock(),
                        )
                    )
                except FederationValidationError:
                    pass
            return LiveReinstatementResult(
                "retryable" if retryable else "operator-attention",
                exc.code,
                str(exc),
                None if record is None else record.reinstatement_id,
                record,
            )
        except (OSError, RuntimeError, TimeoutError, TypeError, ValueError) as exc:
            record = locals().get("record")
            reason = str(exc) or type(exc).__name__
            if isinstance(record, LiveReinstatementRecord):
                record = self.store.save(
                    replace(
                        record,
                        latest_error_code="live-reinstatement-retryable",
                        latest_error_reason=reason,
                        updated_at=self.clock(),
                    )
                )
            return LiveReinstatementResult(
                "retryable",
                "live-reinstatement-retryable",
                reason,
                None if record is None else record.reinstatement_id,
                record,
            )

    async def _resume(
        self, record: LiveReinstatementRecord
    ) -> LiveReinstatementResult:
        if record.state == _STATE_COMPLETED:
            return self._result(
                "completed",
                "live-reinstatement-complete",
                "former primary is already a verified replica",
                record,
            )
        if record.state == _STATE_OPERATOR:
            return self._result(
                "operator-attention",
                record.latest_error_code or "live-reinstatement-conflict",
                record.latest_error_reason or "reinstatement requires operator attention",
                record,
            )
        if record.state == _STATE_ROLLED_BACK:
            return self._result(
                "retryable",
                record.latest_error_code or "reinstatement-catchup-required",
                record.latest_error_reason or "run catch-up again before reinstatement",
                record,
            )
        if record.state == _STATE_ROLLBACK_PENDING:
            return await self._publish_rollback(record)

        failover = self._failover(record)
        if record.state == _STATE_PLANNED:
            replica_state = self._replica_state(record)
            if replica_state == "previous":
                self._validate_current(record, assigned=False, require_manifest=True)
                self.control_plane.change_assignment(
                    record.session_id,
                    self.credentials.identity.node_id,
                    record.group_id,
                    record.source_provider_id,
                    record.restored_replica_provider_ids,
                )
            elif replica_state == "restored":
                self._validate_current(record, assigned=True, require_manifest=True)
            else:
                raise FederationValidationError(
                    "reinstatement-assignment-changed",
                    "group_id",
                    "replica assignment is neither the pre-F4.2 nor restored set",
                )
            record = self.store.save(
                replace(
                    record,
                    state=_STATE_ASSIGNED,
                    latest_error_code=None,
                    latest_error_reason=None,
                    updated_at=self.clock(),
                )
            )

        if record.state == _STATE_ASSIGNED:
            self._validate_current(record, assigned=True, require_manifest=False)
            plan = record.assignment_publication
            if plan is None:
                plan = self.publication_store.issue(
                    self.control_plane,
                    self.credentials,
                    record.session_id,
                    now=self.clock(),
                )
                record = self.store.save(
                    replace(
                        record,
                        assignment_publication=plan,
                        updated_at=self.clock(),
                    )
                )
            await self.channel.publish(plan, self._target_nodes(record, include_returning=True))
            record = self.store.save(
                replace(
                    record,
                    state=_STATE_PUBLISHED,
                    latest_error_code=None,
                    latest_error_reason=None,
                    updated_at=self.clock(),
                )
            )

        if record.state == _STATE_PUBLISHED:
            replica_state = self._replica_state(record)
            if replica_state == "previous":
                record = self.store.save(
                    replace(
                        record,
                        state=_STATE_ROLLBACK_PENDING,
                        latest_error_code="reinstatement-tail-catchup-required",
                        latest_error_reason=(
                            "replica assignment was rolled back before its "
                            "durable stage update"
                        ),
                        updated_at=self.clock(),
                    )
                )
                return await self._publish_rollback(record)
            if replica_state != "restored":
                raise FederationValidationError(
                    "reinstatement-assignment-changed",
                    "group_id",
                    "replica assignment changed during admission",
                )
            self._validate_current(record, assigned=True, require_manifest=False)
            manifest = self.control_plane.manifest(record.session_id, record.group_id)
            if (
                manifest.revision != record.manifest_revision
                or manifest.manifest_hash != record.manifest_hash
            ):
                return await self._rollback(
                    record,
                    "reinstatement-tail-catchup-required",
                    "authoritative manifest advanced during replica admission",
                )
            previous = self.control_plane.latest_storage_replica_assessment(
                record.session_id,
                record.group_id,
                record.returning_provider_id,
            )
            revision = max(
                record.catchup_report_revision,
                0 if previous is None else previous.report_revision,
            ) + 1
            report = await self.channel.request_replica_report(
                failover_id=record.reinstatement_id,
                manifest=manifest,
                provider_id=record.returning_provider_id,
                node_id=record.returning_node_id,
                report_revision=revision,
                reported_at=_utc(self.clock()),
            )
            assessment = self.control_plane.submit_storage_replica_report(
                report,
                actor_node_id=record.returning_node_id,
            )
            if not self._eligible(assessment, report):
                return await self._rollback(
                    record,
                    "reinstatement-tail-catchup-required",
                    assessment.eligibility_reason,
                )
            degraded = self.control_plane.storage_degraded_state(
                record.session_id, record.group_id
            )
            if not self._matching_degraded_record(degraded, record, failover):
                raise FederationValidationError(
                    "reinstatement-degraded-state-mismatch",
                    "storage_degraded_state",
                    "degraded state is missing or unrelated to this failover",
                )
            record = self.store.save(
                replace(
                    record,
                    state=_STATE_REPORT_ACCEPTED,
                    final_report_revision=assessment.report_revision,
                    final_report_hash=assessment.report_hash,
                    degraded_state_hash=_degraded_hash(degraded),
                    latest_error_code=None,
                    latest_error_reason=None,
                    updated_at=self.clock(),
                )
            )

        if record.state == _STATE_REPORT_ACCEPTED:
            assessment = self.control_plane.latest_storage_replica_assessment(
                record.session_id,
                record.group_id,
                record.returning_provider_id,
            )
            if (
                assessment is None
                or assessment.report_revision != record.final_report_revision
                or assessment.report_hash != record.final_report_hash
                or assessment.report is None
            ):
                raise FederationValidationError(
                    "reinstatement-report-changed",
                    "report_revision",
                    "accepted live report is no longer current",
                )
            try:
                record = self.store.restore_control_atomically(
                    record,
                    control=self.control_plane,
                    failover=failover,
                    assessment=assessment,
                    now=self.clock(),
                )
            except FederationValidationError as exc:
                if exc.code == "reinstatement-manifest-changed":
                    return await self._rollback(record, exc.code, str(exc))
                raise

        if record.state == _STATE_CONTROL_RESTORED:
            self._validate_restored(record)
            plan = record.final_publication
            if plan is None:
                plan = self.publication_store.issue(
                    self.control_plane,
                    self.credentials,
                    record.session_id,
                    now=self.clock(),
                )
                record = self.store.save(
                    replace(record, final_publication=plan, updated_at=self.clock())
                )
            await self.channel.publish(plan, self._target_nodes(record, include_returning=True))
            record = self.store.save(
                replace(
                    record,
                    state=_STATE_COMPLETED,
                    completed_at=self.clock(),
                    latest_error_code=None,
                    latest_error_reason=None,
                    updated_at=self.clock(),
                )
            )
        return self._result(
            "completed",
            "live-reinstatement-complete",
            "former primary is a verified replica and redundancy policy is restored",
            record,
        )

    async def _rollback(
        self,
        record: LiveReinstatementRecord,
        code: str,
        reason: str,
    ) -> LiveReinstatementResult:
        snapshot = self.control_plane.snapshot(record.session_id)
        assignment = snapshot.groups.get(record.group_id)
        if assignment is None or assignment.primary_provider_id != record.source_provider_id:
            raise FederationValidationError(
                "reinstatement-assignment-changed",
                "group_id",
                "cannot roll back after primary assignment changed",
            )
        replicas = tuple(assignment.replica_provider_ids)
        if replicas == record.restored_replica_provider_ids:
            self.control_plane.change_assignment(
                record.session_id,
                self.credentials.identity.node_id,
                record.group_id,
                record.source_provider_id,
                record.previous_replica_provider_ids,
            )
        elif replicas != record.previous_replica_provider_ids:
            raise FederationValidationError(
                "reinstatement-assignment-changed",
                "group_id",
                "cannot roll back an unrelated replica assignment",
            )
        record = self.store.save(
            replace(
                record,
                state=_STATE_ROLLBACK_PENDING,
                latest_error_code=code,
                latest_error_reason=reason,
                updated_at=self.clock(),
            )
        )
        return await self._publish_rollback(record)

    async def _publish_rollback(
        self, record: LiveReinstatementRecord
    ) -> LiveReinstatementResult:
        self._validate_current(record, assigned=False, require_manifest=False)
        plan = record.rollback_publication
        if plan is None:
            plan = self.publication_store.issue(
                self.control_plane,
                self.credentials,
                record.session_id,
                now=self.clock(),
            )
            record = self.store.save(
                replace(record, rollback_publication=plan, updated_at=self.clock())
            )
        await self.channel.publish(plan, self._target_nodes(record, include_returning=True))
        record = self.store.save(
            replace(record, state=_STATE_ROLLED_BACK, updated_at=self.clock())
        )
        return self._result(
            "retryable",
            record.latest_error_code or "reinstatement-catchup-required",
            record.latest_error_reason or "run F4.1 catch-up again",
            record,
        )

    def _binding(self, catchup: LiveCatchupRecord) -> dict[str, Any]:
        durable = self.catchup_store.get(catchup.recovery_id)
        if durable is None or durable.to_dict() != catchup.to_dict():
            raise FederationValidationError(
                "reinstatement-catchup-evidence-missing",
                "catchup_recovery_id",
                "F4.1 evidence is not present unchanged in the durable journal",
            )
        catchup = durable
        if (
            catchup.session_id != self.session_id
            or catchup.state != "caught-up"
            or catchup.final_report is None
            or catchup.final_report_revision is None
            or catchup.final_report_hash is None
            or catchup.final_report.report_hash() != catchup.final_report_hash
            or not catchup.final_report.integrity_verified
            or catchup.final_report.synchronization_state != "synchronized"
            or not catchup.final_report.eligible
            or catchup.final_report.eligibility_reasons != ("eligible",)
            or sorted(catchup.final_report.committed_item_hashes)
            != sorted(item.content_hash for item in catchup.items)
            or any(item.status != "verified" for item in catchup.items)
        ):
            raise FederationValidationError(
                "reinstatement-catchup-incomplete",
                "catchup_recovery_id",
                "F4.2 requires complete durable F4.1 evidence",
            )
        failover = self.failover_store.get(
            catchup.session_id, catchup.group_id, catchup.failover_id
        )
        if failover is None or failover.state != "published":
            raise FederationValidationError(
                "reinstatement-failover-required",
                "failover_id",
                "F4.2 requires the exact published F3 failover",
            )
        snapshot = self.control_plane.snapshot(catchup.session_id)
        assignment = snapshot.groups.get(catchup.group_id)
        source = snapshot.providers.get(catchup.source_provider_id)
        returning = snapshot.providers.get(catchup.returning_provider_id)
        grant = snapshot.leader_grants.get(catchup.group_id)
        if (
            assignment is None
            or assignment.primary_provider_id != catchup.source_provider_id
            or tuple(assignment.replica_provider_ids)
            != failover.retained_replica_provider_ids
            or catchup.returning_provider_id in assignment.replica_provider_ids
            or source is None
            or returning is None
            or source.node_id != catchup.source_node_id
            or returning.node_id != catchup.returning_node_id
            or grant is None
            or grant.get("provider_id") != catchup.source_provider_id
            or grant.get("grant_id") != catchup.grant_id
            or int(grant.get("term", -1)) != catchup.term
            or int(grant.get("fencing_token", -1)) != catchup.fencing_token
            or _utc(str(grant.get("lease_expires_at"))) != catchup.lease_expires_at
        ):
            raise FederationValidationError(
                "reinstatement-authority-changed",
                "catchup_recovery_id",
                "assignment or current primary authority differs from F4.1",
            )
        if (
            failover.schema != LIVE_FAILOVER_SCHEMA
            or failover.failed_provider_id != catchup.returning_provider_id
            or failover.failed_node_id != catchup.returning_node_id
            or failover.promoted_provider_id != catchup.source_provider_id
            or failover.promoted_node_id != catchup.source_node_id
            or failover.grant_id != catchup.grant_id
            or failover.term != catchup.term
            or failover.fencing_token != catchup.fencing_token
        ):
            raise FederationValidationError(
                "reinstatement-failover-mismatch",
                "failover_id",
                "published failover differs from F4.1 evidence",
            )
        manifest = self.control_plane.manifest(catchup.session_id, catchup.group_id)
        if (
            manifest.revision != catchup.manifest_revision
            or manifest.manifest_hash != catchup.manifest_hash
        ):
            raise FederationValidationError(
                "reinstatement-catchup-stale",
                "manifest_hash",
                "authoritative manifest advanced after F4.1",
            )
        if (
            self.control_plane.acknowledgement_policy(
                catchup.session_id, catchup.group_id
            ).mode
            is not failover.effective_acknowledgement_mode
        ):
            raise FederationValidationError(
                "reinstatement-policy-changed",
                "acknowledgement_mode",
                "degraded acknowledgement policy differs from F3",
            )
        degraded = self.control_plane.storage_degraded_state(
            catchup.session_id, catchup.group_id
        )
        if not self._matching_degraded(degraded, catchup, failover):
            raise FederationValidationError(
                "reinstatement-degraded-state-mismatch",
                "storage_degraded_state",
                "degraded state is missing, newer, or unrelated",
            )
        return {
            "failover": failover,
            "degraded": degraded,
        }

    def _new_record(
        self,
        catchup: LiveCatchupRecord,
        binding: dict[str, Any],
    ) -> LiveReinstatementRecord:
        failover = binding["failover"]
        degraded = binding["degraded"]
        created = _utc(self.clock())
        reinstatement_id = "live-reinstatement-" + hashlib.sha256(
            (
                f"{catchup.session_id}\0{catchup.group_id}\0{catchup.failover_id}\0"
                f"{catchup.recovery_id}\0{catchup.returning_provider_id}"
            ).encode()
        ).hexdigest()
        restored = tuple(
            sorted((*failover.retained_replica_provider_ids, catchup.returning_provider_id))
        )
        return LiveReinstatementRecord(
            schema=LIVE_REINSTATEMENT_SCHEMA,
            reinstatement_id=reinstatement_id,
            session_id=catchup.session_id,
            group_id=catchup.group_id,
            failover_id=catchup.failover_id,
            catchup_recovery_id=catchup.recovery_id,
            source_provider_id=catchup.source_provider_id,
            source_node_id=catchup.source_node_id,
            returning_provider_id=catchup.returning_provider_id,
            returning_node_id=catchup.returning_node_id,
            previous_replica_provider_ids=failover.retained_replica_provider_ids,
            restored_replica_provider_ids=restored,
            previous_acknowledgement_mode=failover.previous_acknowledgement_mode,
            effective_acknowledgement_mode=failover.effective_acknowledgement_mode,
            manifest_revision=catchup.manifest_revision,
            manifest_hash=catchup.manifest_hash,
            grant_id=catchup.grant_id,
            term=catchup.term,
            fencing_token=catchup.fencing_token,
            lease_expires_at=catchup.lease_expires_at,
            catchup_report_revision=catchup.final_report_revision,
            catchup_report_hash=catchup.final_report_hash,
            degraded_state_hash=_degraded_hash(degraded),
            state=_STATE_PLANNED,
            assignment_publication=None,
            rollback_publication=None,
            final_publication=None,
            final_report_revision=None,
            final_report_hash=None,
            latest_error_code=None,
            latest_error_reason=None,
            created_at=created,
            updated_at=created,
            completed_at=None,
        )

    def _replica_state(self, record: LiveReinstatementRecord) -> str:
        assignment = self.control_plane.snapshot(record.session_id).groups.get(
            record.group_id
        )
        if assignment is None or assignment.primary_provider_id != record.source_provider_id:
            return "other"
        replicas = tuple(assignment.replica_provider_ids)
        if replicas == record.previous_replica_provider_ids:
            return "previous"
        if replicas == record.restored_replica_provider_ids:
            return "restored"
        return "other"

    def _validate_current(
        self,
        record: LiveReinstatementRecord,
        *,
        assigned: bool,
        require_manifest: bool,
    ) -> None:
        failover = self._failover(record)
        snapshot = self.control_plane.snapshot(record.session_id)
        assignment = snapshot.groups.get(record.group_id)
        source = snapshot.providers.get(record.source_provider_id)
        returning = snapshot.providers.get(record.returning_provider_id)
        grant = snapshot.leader_grants.get(record.group_id)
        expected_replicas = (
            record.restored_replica_provider_ids
            if assigned
            else record.previous_replica_provider_ids
        )
        if (
            assignment is None
            or assignment.primary_provider_id != record.source_provider_id
            or tuple(assignment.replica_provider_ids) != expected_replicas
            or source is None
            or returning is None
            or source.node_id != record.source_node_id
            or returning.node_id != record.returning_node_id
            or grant is None
            or grant.get("provider_id") != record.source_provider_id
            or grant.get("grant_id") != record.grant_id
            or int(grant.get("term", -1)) != record.term
            or int(grant.get("fencing_token", -1)) != record.fencing_token
            or _utc(str(grant.get("lease_expires_at"))) != record.lease_expires_at
        ):
            raise FederationValidationError(
                "reinstatement-authority-changed",
                "reinstatement_id",
                "assignment, provider identity, or primary authority changed",
            )
        if (
            self.control_plane.acknowledgement_policy(
                record.session_id, record.group_id
            ).mode
            is not record.effective_acknowledgement_mode
        ):
            raise FederationValidationError(
                "reinstatement-policy-changed",
                "acknowledgement_mode",
                "policy changed before F4.2 restoration",
            )
        degraded = self.control_plane.storage_degraded_state(
            record.session_id, record.group_id
        )
        if not self._matching_degraded_record(degraded, record, failover):
            raise FederationValidationError(
                "reinstatement-degraded-state-changed",
                "storage_degraded_state",
                "degraded state is missing or unrelated to this failover",
            )
        if require_manifest:
            manifest = self.control_plane.manifest(record.session_id, record.group_id)
            if (
                manifest.revision != record.manifest_revision
                or manifest.manifest_hash != record.manifest_hash
            ):
                raise FederationValidationError(
                    "reinstatement-catchup-stale",
                    "manifest_hash",
                    "authoritative manifest advanced before assignment",
                )
        if failover.previous_acknowledgement_mode is not record.previous_acknowledgement_mode:
            raise FederationValidationError(
                "reinstatement-failover-mismatch",
                "failover_id",
                "failover acknowledgement binding changed",
            )

    def _validate_restored(self, record: LiveReinstatementRecord) -> None:
        snapshot = self.control_plane.snapshot(record.session_id)
        assignment = snapshot.groups.get(record.group_id)
        if (
            assignment is None
            or assignment.primary_provider_id != record.source_provider_id
            or tuple(assignment.replica_provider_ids)
            != record.restored_replica_provider_ids
        ):
            raise FederationValidationError(
                "reinstatement-assignment-changed",
                "group_id",
                "restored replica assignment changed before final publication",
            )
        if (
            self.control_plane.acknowledgement_policy(
                record.session_id, record.group_id
            ).mode
            is not record.previous_acknowledgement_mode
        ):
            raise FederationValidationError(
                "reinstatement-policy-not-restored",
                "acknowledgement_mode",
                "pre-failover acknowledgement policy was not restored",
            )
        if self.control_plane.storage_degraded_state(record.session_id, record.group_id) is not None:
            raise FederationValidationError(
                "reinstatement-degraded-state-not-cleared",
                "storage_degraded_state",
                "matching degraded state was not cleared",
            )

    def _failover(self, record: LiveReinstatementRecord) -> LiveFailoverRecord:
        failover = self.failover_store.get(
            record.session_id, record.group_id, record.failover_id
        )
        if (
            failover is None
            or failover.state != "published"
            or failover.failed_provider_id != record.returning_provider_id
            or failover.promoted_provider_id != record.source_provider_id
            or failover.grant_id != record.grant_id
            or failover.term != record.term
            or failover.fencing_token != record.fencing_token
        ):
            raise FederationValidationError(
                "reinstatement-failover-mismatch",
                "failover_id",
                "published F3 failover changed or disappeared",
            )
        return failover

    def _target_nodes(
        self,
        record: LiveReinstatementRecord,
        *,
        include_returning: bool,
    ) -> tuple[str, ...]:
        snapshot = self.control_plane.snapshot(record.session_id)
        provider_ids = (
            record.source_provider_id,
            *record.previous_replica_provider_ids,
            *((record.returning_provider_id,) if include_returning else ()),
        )
        nodes: list[str] = []
        for provider_id in provider_ids:
            provider = snapshot.providers.get(provider_id)
            if provider is None:
                raise FederationValidationError(
                    "reinstatement-provider-missing",
                    "provider_id",
                    f"provider {provider_id} is no longer registered",
                )
            nodes.append(provider.node_id)
        return tuple(dict.fromkeys(nodes))

    @staticmethod
    def _eligible(
        assessment: StorageReplicaAssessment,
        report: StorageReplicaReport,
    ) -> bool:
        return (
            assessment.accepted
            and assessment.eligibility
            and assessment.eligibility_reason == "eligible"
            and assessment.report is not None
            and assessment.report_hash == report.report_hash()
            and assessment.report.to_dict() == report.to_dict()
        )

    @staticmethod
    def _matching_degraded_record(
        degraded: dict[str, Any] | None,
        record: LiveReinstatementRecord,
        failover: LiveFailoverRecord,
    ) -> bool:
        if degraded is None:
            return False
        obligations = degraded.get("obligations")
        return (
            degraded.get("session_id") == record.session_id
            and degraded.get("group_id") == record.group_id
            and degraded.get("reason_code")
            == "automatic-failover-redundancy-lost"
            and isinstance(obligations, dict)
            and obligations.get("action")
            == "restore-replication-and-acknowledgement-policy"
            and obligations.get("failed_provider_id")
            == record.returning_provider_id
            and obligations.get("previous_acknowledgement_mode")
            == failover.previous_acknowledgement_mode.value
        )

    @staticmethod
    def _matching_degraded(
        degraded: dict[str, Any] | None,
        catchup: LiveCatchupRecord,
        failover: LiveFailoverRecord,
    ) -> bool:
        if degraded is None:
            return False
        obligations = degraded.get("obligations")
        return (
            degraded.get("session_id") == catchup.session_id
            and degraded.get("group_id") == catchup.group_id
            and degraded.get("reason_code")
            == "automatic-failover-redundancy-lost"
            and isinstance(obligations, dict)
            and obligations.get("action")
            == "restore-replication-and-acknowledgement-policy"
            and obligations.get("failed_provider_id")
            == catchup.returning_provider_id
            and obligations.get("previous_acknowledgement_mode")
            == failover.previous_acknowledgement_mode.value
        )

    @staticmethod
    def _result(
        status: str,
        code: str,
        reason: str,
        record: LiveReinstatementRecord,
    ) -> LiveReinstatementResult:
        return LiveReinstatementResult(
            status,
            code,
            reason,
            record.reinstatement_id,
            record,
        )
