"""Operational Phase D control-plane facade with durable fencing and handover.

The original D3 event store intentionally modelled assignments and grants in isolation.
This facade adds the missing operational invariants required before Phase E:
monotonic term/fencing counters survive revocation, acknowledgement policy is durable,
and controlled handover is committed as one SQLite transaction.
"""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .acknowledgement import AcknowledgementMode, AcknowledgementPolicy
from .commit_tracking import StorageCommitStatus
from .errors import FederationValidationError
from .manifest import AuthoritativeStorageManifest, ManifestItemKind
from .manifest_store import (
    AuthoritativeManifestStore,
    ManifestCommitIntent,
)
from .models import SessionEvent
from .promotion_finalization import (
    PROMOTION_FINALIZATION_SCHEMA,
    PromotionFinalizationRecord,
)
from .promotion_recovery import PromotionRecoveryResult, recover_storage_promotion
from .promotion_transaction import PromotionTransactionRecord
from .provider_fencing import (
    ProviderFenceStore,
    StorageAuthorityGrantAcknowledgement,
    StorageAuthorityGrantCommand,
    StorageFenceAcknowledgement,
    StorageFenceCommand,
)
from .reporting import (
    StorageReplicaAssessment,
    StorageReplicaReport,
    assess_storage_replica_report,
)
from .storage_control_plane import (
    STORAGE_ASSIGNMENT_CHANGED,
    STORAGE_LEADER_GRANTED,
    STORAGE_LEADER_REVOKED,
    StorageControlPlaneSnapshot,
    StorageControlPlaneStore,
    StorageProviderRegistration,
)
from .storage_protocol import BatchIngestRequest, StorageOperation


def _utc(value: datetime | None = None) -> datetime:
    result = value or datetime.now(timezone.utc)
    if result.tzinfo is None or result.utcoffset() is None:
        raise FederationValidationError("invalid-timestamp", "occurred_at", "must be timezone-aware")
    return result.astimezone(timezone.utc)


@dataclass(frozen=True)
class HandoverCommit:
    session_id: str
    group_id: str
    source_provider_id: str
    target_provider_id: str
    grant_id: str
    term: int
    fencing_token: int
    revision: int


class PhaseDControlPlane:
    """Composition facade around :class:`StorageControlPlaneStore`.

    The underlying ordered event log remains the source of assignment/grant state.
    Two small metadata tables retain monotonic counters and per-group acknowledgement
    policy without introducing Phase E manifests or automatic promotion.
    """

    def __init__(self, database: Path | str) -> None:
        self.database = str(database)
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self.store = StorageControlPlaneStore(self.database)
        self.manifests = AuthoritativeManifestStore(self.database)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS storage_fencing_counters (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    last_term INTEGER NOT NULL CHECK(last_term >= 0),
                    last_fencing_token INTEGER NOT NULL CHECK(last_fencing_token >= 0),
                    PRIMARY KEY(session_id, group_id)
                );
                CREATE TABLE IF NOT EXISTS storage_ack_policies (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    mode TEXT NOT NULL CHECK(mode IN ('primary','one-replica','quorum','all')),
                    PRIMARY KEY(session_id, group_id)
                );
                CREATE TABLE IF NOT EXISTS storage_replica_reports (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    report_revision INTEGER NOT NULL CHECK(report_revision >= 0),
                    report_hash TEXT NOT NULL,
                    report_json TEXT NOT NULL CHECK(json_valid(report_json)),
                    assessment_json TEXT NOT NULL CHECK(json_valid(assessment_json)),
                    PRIMARY KEY(session_id, group_id, provider_id)
                );
                CREATE TABLE IF NOT EXISTS storage_degraded_states (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    manifest_revision INTEGER NOT NULL CHECK(manifest_revision >= 0),
                    manifest_hash TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    reason_detail TEXT NOT NULL,
                    missing_ranges_json TEXT NOT NULL CHECK(json_valid(missing_ranges_json)),
                    obligations_json TEXT NOT NULL CHECK(json_valid(obligations_json)),
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, group_id)
                );
                CREATE TABLE IF NOT EXISTS storage_promotion_transactions (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    promotion_id TEXT NOT NULL,
                    selected_provider_id TEXT NOT NULL,
                    selected_report_hash TEXT NOT NULL,
                    selected_report_revision INTEGER NOT NULL CHECK(selected_report_revision >= 0),
                    selected_manifest_revision INTEGER NOT NULL CHECK(selected_manifest_revision >= 0),
                    selected_manifest_hash TEXT NOT NULL,
                    previous_provider_id TEXT,
                    previous_term INTEGER,
                    reserved_term INTEGER,
                    fencing_status TEXT NOT NULL,
                    fencing_acknowledged INTEGER NOT NULL CHECK(fencing_acknowledged IN (0, 1)),
                    fencing_ack_identity TEXT,
                    grant_id TEXT,
                    grant_status TEXT NOT NULL,
                    grant_acknowledged INTEGER NOT NULL CHECK(grant_acknowledged IN (0, 1)),
                    grant_ack_identity TEXT,
                    state TEXT NOT NULL,
                    failure_code TEXT,
                    failure_reason TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, group_id, promotion_id)
                );
                CREATE TABLE IF NOT EXISTS storage_promotion_finalizations (
                    session_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    promotion_id TEXT NOT NULL,
                    selected_provider_id TEXT NOT NULL,
                    new_authority_term INTEGER NOT NULL CHECK(new_authority_term >= 0),
                    manifest_revision INTEGER NOT NULL CHECK(manifest_revision >= 0),
                    manifest_hash TEXT NOT NULL,
                    report_revision INTEGER NOT NULL CHECK(report_revision >= 0),
                    report_hash TEXT NOT NULL,
                    fencing_ack_identity TEXT NOT NULL,
                    grant_ack_identity TEXT NOT NULL,
                    final_authority_identity TEXT NOT NULL,
                    degraded_state_hash TEXT NOT NULL,
                    missing_ranges_json TEXT NOT NULL CHECK(json_valid(missing_ranges_json)),
                    recovery_obligations_json TEXT NOT NULL CHECK(json_valid(recovery_obligations_json)),
                    completed_at TEXT NOT NULL,
                    degraded_cleared INTEGER NOT NULL CHECK(degraded_cleared IN (0, 1)),
                    PRIMARY KEY(session_id, group_id, promotion_id),
                    UNIQUE(session_id, group_id, final_authority_identity)
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

    def snapshot(self, session_id: str) -> StorageControlPlaneSnapshot:
        return self.store.snapshot(session_id)

    def create_group(self, session_id: str, actor_node_id: str, group_id: str) -> SessionEvent:
        event = self.store.create_group(session_id, actor_node_id, group_id)
        try:
            self.manifests.ensure_genesis(
                session_id,
                group_id,
                term=0,
                expected_control_revision=event.revision,
                now=event.occurred_at,
            )
        except FederationValidationError as exc:
            if exc.code != "concurrent-control-change":
                raise
            self.manifests.ensure_genesis(
                session_id,
                group_id,
                term=0,
                expected_control_revision=self.snapshot(session_id).revision,
                now=event.occurred_at,
            )
        self.set_acknowledgement_policy(session_id, group_id, AcknowledgementMode.PRIMARY)
        return event

    def manifest(
        self,
        session_id: str,
        group_id: str,
    ) -> AuthoritativeStorageManifest:
        snapshot = self.snapshot(session_id)
        if group_id not in snapshot.groups:
            raise FederationValidationError(
                "unknown-storage-group",
                "group_id",
                "storage group is not registered",
            )
        try:
            return self.manifests.head(session_id, group_id)
        except FederationValidationError as exc:
            if exc.code != "manifest-not-found":
                raise
        return self.manifests.ensure_genesis(
            session_id,
            group_id,
            term=0,
            expected_control_revision=snapshot.revision,
            now=datetime.now(timezone.utc),
        )

    def manifest_history(
        self,
        session_id: str,
        group_id: str,
    ) -> tuple[AuthoritativeStorageManifest, ...]:
        self.manifest(session_id, group_id)
        return self.manifests.history(session_id, group_id)

    @staticmethod
    def _validate_commit_status_identity(
        request: BatchIngestRequest,
        status: StorageCommitStatus,
    ) -> None:
        authority = request.authority
        if (
            status.session_id != authority.session_id
            or status.group_id != authority.group_id
            or status.batch_id != request.batch_id
            or status.idempotency_key != request.idempotency_key
            or status.content_hash != request.content_hash
            or status.dataset_id != request.dataset_id
            or status.dataset_schema_name != request.dataset_schema_name
            or status.dataset_schema_version
            != request.dataset_schema_version
        ):
            raise FederationValidationError(
                "manifest-commit-evidence-mismatch",
                "status",
                "acknowledgement evidence does not match the immutable request",
            )

    def prepare_batch_manifest(
        self,
        request: BatchIngestRequest,
        status: StorageCommitStatus,
        *,
        primary_provider_id: str,
        now: datetime,
    ) -> ManifestCommitIntent:
        """Persist immutable coordinator intent before provider mutation."""
        if not isinstance(request, BatchIngestRequest):
            raise FederationValidationError(
                "invalid-object",
                "request",
                "must be BatchIngestRequest",
            )
        if not isinstance(status, StorageCommitStatus):
            raise FederationValidationError(
                "invalid-object",
                "status",
                "must be StorageCommitStatus",
            )
        now = _utc(now)
        request.validate_content_hash()
        self._validate_commit_status_identity(request, status)
        authority = request.authority
        snapshot = self.snapshot(authority.session_id)
        assignment = snapshot.groups.get(authority.group_id)
        if (
            assignment is None
            or assignment.primary_provider_id != primary_provider_id
        ):
            raise FederationValidationError(
                "manifest-primary-mismatch",
                "primary_provider_id",
                "provider is not the assigned primary",
            )
        provider = snapshot.providers.get(primary_provider_id)
        if (
            provider is None
            or not provider.assignable
            or provider.node_id != authority.actor_node_id
        ):
            raise FederationValidationError(
                "manifest-primary-unavailable",
                "primary_provider_id",
                "primary provider identity is not active and assignable",
            )
        grant = snapshot.leader_grants.get(authority.group_id)
        if (
            grant is None
            or grant.get("provider_id") != primary_provider_id
            or grant.get("grant_id") != authority.grant_id
            or int(grant.get("term", -1)) != authority.term
            or int(grant.get("fencing_token", -1)) != authority.fencing_token
            or datetime.fromisoformat(
                str(grant.get("lease_expires_at")).replace("Z", "+00:00")
            )
            != authority.lease_expires_at
        ):
            raise FederationValidationError(
                "manifest-authority-mismatch",
                "authority",
                "commit was not accepted under the active coordinator grant",
            )
        assigned_replicas = tuple(sorted(assignment.replica_provider_ids))
        if status.assigned_replica_ids != assigned_replicas:
            raise FederationValidationError(
                "manifest-replica-set-mismatch",
                "assigned_replica_ids",
                "commit evidence does not match the current frozen replica set",
            )
        if not set(status.acknowledged_replica_ids).issubset(
            assigned_replicas
        ):
            raise FederationValidationError(
                "manifest-unexpected-acknowledgement",
                "acknowledged_replica_ids",
                "commit evidence contains an unassigned provider",
            )
        expected_required = self.acknowledgement_policy(
            authority.session_id,
            authority.group_id,
        ).required_replica_acks(len(assigned_replicas))
        if status.required_replica_acks != expected_required:
            raise FederationValidationError(
                "manifest-ack-policy-mismatch",
                "required_replica_acks",
                "commit evidence does not match the frozen policy",
            )

        self.manifest(authority.session_id, authority.group_id)
        encoded_content = json.dumps(
            request.content,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        return self.manifests.prepare_intent(
            ManifestCommitIntent(
                session_id=authority.session_id,
                group_id=authority.group_id,
                item_id=request.batch_id,
                kind=ManifestItemKind.BATCH,
                dataset_id=request.dataset_id,
                idempotency_key=request.idempotency_key,
                content_hash=request.content_hash,
                size_bytes=len(encoded_content),
                schema_name=request.dataset_schema_name,
                schema_version=request.dataset_schema_version,
                source_id=None,
                first_sequence=None,
                last_sequence=None,
                dataset_required=True,
                primary_provider_id=primary_provider_id,
                assigned_replica_ids=assigned_replicas,
                required_replica_acks=status.required_replica_acks,
                grant_id=authority.grant_id,
                term=authority.term,
                fencing_token=authority.fencing_token,
                lease_expires_at=authority.lease_expires_at,
                prepared_control_revision=snapshot.revision,
                prepared_at=now,
            )
        )

    @staticmethod
    def _request_matches_intent(
        request: BatchIngestRequest,
        intent: ManifestCommitIntent,
    ) -> bool:
        encoded_content = json.dumps(
            request.content,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        return (
            request.authority.session_id == intent.session_id
            and request.authority.group_id == intent.group_id
            and request.batch_id == intent.item_id
            and request.dataset_id == intent.dataset_id
            and request.idempotency_key == intent.idempotency_key
            and request.content_hash == intent.content_hash
            and len(encoded_content) == intent.size_bytes
            and request.dataset_schema_name == intent.schema_name
            and request.dataset_schema_version == intent.schema_version
            and intent.kind is ManifestItemKind.BATCH
        )

    @staticmethod
    def _validate_status_for_intent(
        intent: ManifestCommitIntent,
        status: StorageCommitStatus,
    ) -> None:
        if (
            status.session_id != intent.session_id
            or status.group_id != intent.group_id
            or status.batch_id != intent.item_id
            or status.idempotency_key != intent.idempotency_key
            or status.content_hash != intent.content_hash
            or status.dataset_id != intent.dataset_id
            or status.dataset_schema_name != intent.schema_name
            or status.dataset_schema_version != intent.schema_version
            or status.assigned_replica_ids != intent.assigned_replica_ids
            or status.required_replica_acks != intent.required_replica_acks
        ):
            raise FederationValidationError(
                "manifest-commit-evidence-mismatch",
                "status",
                "acknowledgement evidence differs from the prepared intent",
            )
        if not status.committed:
            raise FederationValidationError(
                "manifest-commit-not-ready",
                "status",
                "acknowledgement policy is not durably satisfied",
            )

    def finalize_batch_manifest_intent(
        self,
        intent: ManifestCommitIntent,
        status: StorageCommitStatus,
        *,
        now: datetime,
    ) -> AuthoritativeStorageManifest:
        self._validate_status_for_intent(intent, status)
        snapshot = self.snapshot(intent.session_id)
        current_grant = snapshot.leader_grants.get(intent.group_id)
        manifest_term = (
            intent.term
            if current_grant is None
            else max(intent.term, int(current_grant["term"]))
        )
        result = self.manifests.finalize_intent(
            intent,
            acknowledged_provider_ids=(
                intent.primary_provider_id,
                *status.acknowledged_replica_ids,
            ),
            expected_control_revision=snapshot.revision,
            manifest_term=manifest_term,
            now=_utc(now),
        )
        return result.manifest

    def commit_batch_manifest(
        self,
        request: BatchIngestRequest,
        status: StorageCommitStatus,
        *,
        primary_provider_id: str,
        now: datetime,
    ) -> AuthoritativeStorageManifest:
        """Finalize previously authorized evidence, including after handover."""

        if not isinstance(request, BatchIngestRequest):
            raise FederationValidationError(
                "invalid-object",
                "request",
                "must be BatchIngestRequest",
            )
        if not isinstance(status, StorageCommitStatus):
            raise FederationValidationError(
                "invalid-object",
                "status",
                "must be StorageCommitStatus",
            )
        self._validate_commit_status_identity(request, status)
        intent = self.manifests.intent(
            request.authority.session_id,
            request.authority.group_id,
            request.batch_id,
        )
        if (
            intent.primary_provider_id != primary_provider_id
            or not self._request_matches_intent(request, intent)
        ):
            raise FederationValidationError(
                "manifest-intent-conflict",
                "request",
                "request does not match the coordinator-prepared intent",
            )
        return self.finalize_batch_manifest_intent(
            intent,
            status,
            now=now,
        )

    def pending_batch_manifest_intents(
        self,
        *,
        primary_provider_id: str,
    ) -> tuple[ManifestCommitIntent, ...]:
        return self.manifests.pending_intents(
            primary_provider_id=primary_provider_id,
        )

    def register_provider(
        self,
        session_id: str,
        actor_node_id: str,
        provider: StorageProviderRegistration,
    ) -> SessionEvent:
        return self.store.register_provider(session_id, actor_node_id, provider)

    def remove_provider(self, session_id: str, actor_node_id: str, provider_id: str) -> SessionEvent:
        return self.store.remove_provider(session_id, actor_node_id, provider_id)

    def change_assignment(
        self,
        session_id: str,
        actor_node_id: str,
        group_id: str,
        primary_provider_id: str | None,
        replica_provider_ids: tuple[str, ...] = (),
    ) -> SessionEvent:
        return self.store.change_assignment(
            session_id,
            actor_node_id,
            group_id,
            primary_provider_id,
            replica_provider_ids,
        )

    def set_acknowledgement_policy(
        self,
        session_id: str,
        group_id: str,
        mode: AcknowledgementMode | str,
    ) -> AcknowledgementPolicy:
        policy = AcknowledgementPolicy(AcknowledgementMode(mode))
        if group_id not in self.snapshot(session_id).groups:
            raise FederationValidationError("unknown-storage-group", "group_id", "storage group is not registered")
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO storage_ack_policies(session_id, group_id, mode)
                   VALUES (?, ?, ?)
                   ON CONFLICT(session_id, group_id) DO UPDATE SET mode=excluded.mode""",
                (session_id, group_id, policy.mode.value),
            )
        return policy

    def acknowledgement_policy(self, session_id: str, group_id: str) -> AcknowledgementPolicy:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT mode FROM storage_ack_policies WHERE session_id=? AND group_id=?",
                (session_id, group_id),
            ).fetchone()
        return AcknowledgementPolicy(
            AcknowledgementMode.PRIMARY if row is None else AcknowledgementMode(row["mode"])
        )

    def _assigned_provider_ids(self, session_id: str, group_id: str) -> tuple[str, ...]:
        snapshot = self.snapshot(session_id)
        assignment = snapshot.groups.get(group_id)
        if assignment is None:
            return ()
        return tuple(
            provider_id
            for provider_id in (assignment.primary_provider_id, *assignment.replica_provider_ids)
            if provider_id is not None
        )

    def latest_storage_replica_report(
        self,
        session_id: str,
        group_id: str,
        provider_id: str,
    ) -> StorageReplicaReport | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT report_json FROM storage_replica_reports
                   WHERE session_id=? AND group_id=? AND provider_id=?""",
                (session_id, group_id, provider_id),
            ).fetchone()
        if row is None:
            return None
        return StorageReplicaReport.from_dict(json.loads(row["report_json"]))

    def latest_storage_replica_assessment(
        self,
        session_id: str,
        group_id: str,
        provider_id: str,
    ) -> StorageReplicaAssessment | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT assessment_json FROM storage_replica_reports
                   WHERE session_id=? AND group_id=? AND provider_id=?""",
                (session_id, group_id, provider_id),
            ).fetchone()
        if row is None:
            return None
        return StorageReplicaAssessment.from_dict(json.loads(row["assessment_json"]))

    def storage_degraded_state(self, session_id: str, group_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM storage_degraded_states
                   WHERE session_id=? AND group_id=?""",
                (session_id, group_id),
            ).fetchone()
        if row is None:
            return None
        try:
            missing_ranges = json.loads(row["missing_ranges_json"])
            obligations = json.loads(row["obligations_json"])
        except json.JSONDecodeError as exc:
            raise FederationValidationError(
                "corrupt-storage-degraded-state",
                "storage_degraded_states",
                "persisted degraded state is malformed",
            ) from exc
        return {
            "session_id": str(row["session_id"]),
            "group_id": str(row["group_id"]),
            "manifest_revision": int(row["manifest_revision"]),
            "manifest_hash": str(row["manifest_hash"]),
            "reason_code": str(row["reason_code"]),
            "reason_detail": str(row["reason_detail"]),
            "missing_ranges": missing_ranges,
            "obligations": obligations,
            "updated_at": str(row["updated_at"]),
        }

    def set_storage_degraded_state(
        self,
        session_id: str,
        group_id: str,
        *,
        manifest: AuthoritativeStorageManifest,
        reason_code: str,
        reason_detail: str,
        missing_ranges: tuple[dict[str, Any], ...],
        obligations: dict[str, Any],
        now: datetime | None = None,
    ) -> dict[str, Any]:
        session_id = str(session_id)
        group_id = str(group_id)
        now = _utc(now)
        if manifest.session_id != session_id or manifest.group_id != group_id:
            raise FederationValidationError("manifest-mismatch", "manifest", "degraded state must match the authoritative manifest")
        if manifest.commit_state.value != "committed":
            raise FederationValidationError("manifest-not-committed", "manifest", "authoritative manifest must remain committed")
        payload = {
            "session_id": session_id,
            "group_id": group_id,
            "manifest_revision": manifest.revision,
            "manifest_hash": manifest.manifest_hash,
            "reason_code": reason_code,
            "reason_detail": reason_detail,
            "missing_ranges": list(missing_ranges),
            "obligations": obligations,
            "updated_at": now.isoformat().replace("+00:00", "Z"),
        }
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """SELECT * FROM storage_degraded_states
                       WHERE session_id=? AND group_id=?""",
                    (session_id, group_id),
                ).fetchone()
                if row is not None:
                    existing = {
                        "session_id": str(row["session_id"]),
                        "group_id": str(row["group_id"]),
                        "manifest_revision": int(row["manifest_revision"]),
                        "manifest_hash": str(row["manifest_hash"]),
                        "reason_code": str(row["reason_code"]),
                        "reason_detail": str(row["reason_detail"]),
                        "missing_ranges": json.loads(row["missing_ranges_json"]),
                        "obligations": json.loads(row["obligations_json"]),
                    }
                    if existing == {k: payload[k] for k in existing}:
                        connection.commit()
                        return {**existing, "updated_at": str(row["updated_at"])}
                connection.execute(
                    """INSERT INTO storage_degraded_states
                       (session_id, group_id, manifest_revision, manifest_hash,
                        reason_code, reason_detail, missing_ranges_json,
                        obligations_json, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(session_id, group_id) DO UPDATE SET
                           manifest_revision=excluded.manifest_revision,
                           manifest_hash=excluded.manifest_hash,
                           reason_code=excluded.reason_code,
                           reason_detail=excluded.reason_detail,
                           missing_ranges_json=excluded.missing_ranges_json,
                           obligations_json=excluded.obligations_json,
                           updated_at=excluded.updated_at""",
                    (
                        session_id,
                        group_id,
                        manifest.revision,
                        manifest.manifest_hash,
                        reason_code,
                        reason_detail,
                        json.dumps(list(missing_ranges), sort_keys=True, separators=(",", ":"), allow_nan=False),
                        json.dumps(obligations, sort_keys=True, separators=(",", ":"), allow_nan=False),
                        payload["updated_at"],
                    ),
                )
                connection.commit()
                return payload
            except Exception:
                connection.rollback()
                raise

    def promotion_transaction(self, session_id: str, group_id: str, promotion_id: str) -> PromotionTransactionRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM storage_promotion_transactions
                   WHERE session_id=? AND group_id=? AND promotion_id=?""",
                (session_id, group_id, promotion_id),
            ).fetchone()
        if row is None:
            return None
        return PromotionTransactionRecord(
            schema="fcp.storage_promotion_transaction.v1",
            promotion_id=row["promotion_id"],
            session_id=row["session_id"],
            group_id=row["group_id"],
            selected_provider_id=row["selected_provider_id"],
            selected_report_hash=row["selected_report_hash"],
            selected_report_revision=row["selected_report_revision"],
            selected_manifest_revision=row["selected_manifest_revision"],
            selected_manifest_hash=row["selected_manifest_hash"],
            previous_provider_id=row["previous_provider_id"],
            previous_term=row["previous_term"],
            reserved_term=row["reserved_term"],
            fencing_status=row["fencing_status"],
            fencing_acknowledged=bool(row["fencing_acknowledged"]),
            fencing_ack_identity=row["fencing_ack_identity"],
            grant_id=row["grant_id"],
            grant_status=row["grant_status"],
            grant_acknowledged=bool(row["grant_acknowledged"]),
            grant_ack_identity=row["grant_ack_identity"],
            state=row["state"],
            failure_code=row["failure_code"],
            failure_reason=row["failure_reason"],
            updated_at=row["updated_at"],
        )

    def create_promotion_transaction(self, record: PromotionTransactionRecord) -> PromotionTransactionRecord:
        existing = self.promotion_transaction(record.session_id, record.group_id, record.promotion_id)
        if existing is not None:
            if existing == record:
                return existing
            raise FederationValidationError(
                "conflicting-promotion-command",
                "promotion_id",
                "promotion id already exists with different immutable inputs",
            )
        return self.upsert_promotion_transaction(record)

    def upsert_promotion_transaction(self, record: PromotionTransactionRecord) -> PromotionTransactionRecord:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO storage_promotion_transactions
                   (session_id, group_id, promotion_id, selected_provider_id,
                    selected_report_hash, selected_report_revision,
                    selected_manifest_revision, selected_manifest_hash,
                    previous_provider_id, previous_term, reserved_term,
                    fencing_status, fencing_acknowledged, fencing_ack_identity,
                    grant_id, grant_status, grant_acknowledged, grant_ack_identity,
                    state, failure_code, failure_reason, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(session_id, group_id, promotion_id) DO UPDATE SET
                       selected_provider_id=excluded.selected_provider_id,
                       selected_report_hash=excluded.selected_report_hash,
                       selected_report_revision=excluded.selected_report_revision,
                       selected_manifest_revision=excluded.selected_manifest_revision,
                       selected_manifest_hash=excluded.selected_manifest_hash,
                       previous_provider_id=excluded.previous_provider_id,
                       previous_term=excluded.previous_term,
                       reserved_term=excluded.reserved_term,
                       fencing_status=excluded.fencing_status,
                       fencing_acknowledged=excluded.fencing_acknowledged,
                       fencing_ack_identity=excluded.fencing_ack_identity,
                       grant_id=excluded.grant_id,
                       grant_status=excluded.grant_status,
                       grant_acknowledged=excluded.grant_acknowledged,
                       grant_ack_identity=excluded.grant_ack_identity,
                       state=excluded.state,
                       failure_code=excluded.failure_code,
                       failure_reason=excluded.failure_reason,
                       updated_at=excluded.updated_at""",
                (
                    record.session_id,
                    record.group_id,
                    record.promotion_id,
                    record.selected_provider_id,
                    record.selected_report_hash,
                    record.selected_report_revision,
                    record.selected_manifest_revision,
                    record.selected_manifest_hash,
                    record.previous_provider_id,
                    record.previous_term,
                    record.reserved_term,
                    record.fencing_status,
                    int(record.fencing_acknowledged),
                    record.fencing_ack_identity,
                    record.grant_id,
                    record.grant_status,
                    int(record.grant_acknowledged),
                    record.grant_ack_identity,
                    record.state,
                    record.failure_code,
                    record.failure_reason,
                    record.updated_at.isoformat().replace("+00:00", "Z"),
                ),
            )
            connection.commit()
        return record

    def validate_promotion_transaction_inputs(
        self,
        session_id: str,
        group_id: str,
        selection: Any,
        *,
        promotion_id: str,
        reported_at: datetime | None = None,
    ) -> PromotionTransactionRecord:
        manifest = self.manifest(session_id, group_id)
        if selection.selected_provider_id is None:
            raise FederationValidationError("no-qualified-candidate", "selection", selection.reason)
        assessment = self.latest_storage_replica_assessment(session_id, group_id, selection.selected_provider_id)
        if assessment is None or assessment.report is None:
            raise FederationValidationError("missing-assessment", "selection", "selected candidate report is unavailable")
        if not assessment.accepted or not assessment.eligibility:
            raise FederationValidationError("candidate-not-eligible", "selection", assessment.eligibility_reason)
        report = assessment.report
        if report.manifest_revision != manifest.revision or report.manifest_hash != manifest.manifest_hash:
            raise FederationValidationError("stale-candidate-report", "report", "selected candidate no longer matches the authoritative manifest")
        if report.session_id != session_id or report.group_id != group_id:
            raise FederationValidationError("candidate-mismatch", "report", "selected candidate report does not match the target scope")
        if report.provider_id != selection.selected_provider_id:
            raise FederationValidationError("candidate-mismatch", "provider_id", "selected candidate changed")
        if selection.decision == "storage-degraded":
            raise FederationValidationError("storage-degraded", "selection", selection.reason)
        if selection.selected_provider_id not in selection.candidate_provider_ids:
            raise FederationValidationError("candidate-mismatch", "selection", "selected candidate is not in the candidate set")
        current = self.snapshot(session_id).leader_grants.get(group_id)
        previous_provider_id = None if current is None else str(current["provider_id"])
        previous_term = None if current is None else int(current["term"])
        return PromotionTransactionRecord(
            schema="fcp.storage_promotion_transaction.v1",
            promotion_id=promotion_id,
            session_id=session_id,
            group_id=group_id,
            selected_provider_id=report.provider_id,
            selected_report_hash=report.report_hash(),
            selected_report_revision=report.report_revision,
            selected_manifest_revision=manifest.revision,
            selected_manifest_hash=manifest.manifest_hash,
            previous_provider_id=previous_provider_id,
            previous_term=previous_term,
            reserved_term=None,
            fencing_status="not-started",
            fencing_acknowledged=False,
            fencing_ack_identity=None,
            grant_id=None,
            grant_status="not-started",
            grant_acknowledged=False,
            grant_ack_identity=None,
            state="validated",
            failure_code=None,
            failure_reason=None,
            updated_at=reported_at or datetime.now(timezone.utc),
        )

    def revalidate_promotion_bindings(self, record: PromotionTransactionRecord) -> None:
        manifest = self.manifest(record.session_id, record.group_id)
        assessment = self.latest_storage_replica_assessment(
            record.session_id,
            record.group_id,
            record.selected_provider_id,
        )
        if (
            manifest.revision != record.selected_manifest_revision
            or manifest.manifest_hash != record.selected_manifest_hash
            or assessment is None
            or assessment.report is None
            or assessment.report_revision != record.selected_report_revision
            or assessment.report_hash != record.selected_report_hash
            or not assessment.accepted
            or not assessment.eligibility
        ):
            raise FederationValidationError(
                "stale-promotion-binding",
                "promotion_id",
                "durable manifest or selected report no longer matches the promotion",
            )

    @staticmethod
    def _promotion_fence_idempotency_key(record: PromotionTransactionRecord) -> str:
        payload = (
            f"{record.promotion_id}\0{record.session_id}\0{record.group_id}\0"
            f"{record.previous_provider_id}\0{record.previous_term}"
        ).encode()
        return "sha256:" + hashlib.sha256(payload).hexdigest()

    def promotion_fencing_command(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
    ) -> StorageFenceCommand:
        record = self.promotion_transaction(session_id, group_id, promotion_id)
        if record is None:
            raise FederationValidationError("unknown-promotion", "promotion_id", "promotion transaction does not exist")
        if record.state not in (
            "validated",
            "fenced-old-authority",
            "allocated-term",
            "granted-new-authority",
            "finalized",
        ):
            raise FederationValidationError("invalid-promotion-stage", "state", "transaction is not ready for fencing")
        if record.previous_provider_id is None or record.previous_term is None:
            raise FederationValidationError(
                "missing-previous-authority",
                "previous_provider_id",
                "validated transaction has no previous authority to fence",
            )
        return StorageFenceCommand(
            promotion_id=record.promotion_id,
            session_id=record.session_id,
            group_id=record.group_id,
            provider_id=record.previous_provider_id,
            previous_term=record.previous_term,
            fencing_high_water_term=record.previous_term,
            idempotency_key=self._promotion_fence_idempotency_key(record),
        )

    def _verify_persisted_promotion_fence(
        self,
        record: PromotionTransactionRecord,
        provider_fence_store: ProviderFenceStore,
    ) -> StorageFenceAcknowledgement:
        if (
            record.state not in ("fenced-old-authority", "allocated-term", "granted-new-authority", "finalized")
            or not record.fencing_acknowledged
            or record.fencing_ack_identity is None
        ):
            raise FederationValidationError(
                "promotion-not-fenced",
                "state",
                "provider-enforced fencing is not durably confirmed",
            )
        command = self.promotion_fencing_command(record.session_id, record.group_id, record.promotion_id)
        evidence = provider_fence_store.acknowledgement(record.group_id)
        if (
            evidence is None
            or evidence.acknowledgement_id != record.fencing_ack_identity
            or evidence.acknowledgement_id != ProviderFenceStore.acknowledgement_identity(command)
            or {
                "promotion_id": evidence.promotion_id,
                "session_id": evidence.session_id,
                "group_id": evidence.group_id,
                "provider_id": evidence.provider_id,
                "previous_term": evidence.previous_term,
                "fencing_high_water_term": evidence.fencing_high_water_term,
                "idempotency_key": evidence.idempotency_key,
            }
            != command.binding()
        ):
            raise FederationValidationError(
                "unverifiable-provider-fence",
                "fencing_ack_identity",
                "durable provider fencing evidence no longer matches the promotion",
            )
        return evidence

    def acknowledge_promotion_fencing(
        self,
        acknowledgement: StorageFenceAcknowledgement,
        *,
        actor_node_id: str,
        provider_fence_store: ProviderFenceStore,
    ) -> PromotionTransactionRecord:
        record = self.promotion_transaction(
            acknowledgement.session_id,
            acknowledgement.group_id,
            acknowledgement.promotion_id,
        )
        if record is None:
            raise FederationValidationError("unknown-promotion", "promotion_id", "promotion transaction does not exist")
        command = self.promotion_fencing_command(record.session_id, record.group_id, record.promotion_id)
        provider = self.snapshot(record.session_id).providers.get(command.provider_id)
        if provider is None or provider.node_id != actor_node_id:
            raise FederationValidationError(
                "unauthenticated-fencing-ack",
                "actor_node_id",
                "authenticated sender does not own the previous provider",
            )
        expected_ack = ProviderFenceStore.acknowledgement_identity(command)
        expected = {
            **command.binding(),
            "acknowledgement_id": expected_ack,
        }
        actual = {
            "promotion_id": acknowledgement.promotion_id,
            "session_id": acknowledgement.session_id,
            "group_id": acknowledgement.group_id,
            "provider_id": acknowledgement.provider_id,
            "previous_term": acknowledgement.previous_term,
            "fencing_high_water_term": acknowledgement.fencing_high_water_term,
            "idempotency_key": acknowledgement.idempotency_key,
            "acknowledgement_id": acknowledgement.acknowledgement_id,
        }
        if actual != expected:
            raise FederationValidationError(
                "fencing-ack-mismatch",
                "acknowledgement",
                "provider acknowledgement is not bound to the exact durable fencing command",
            )
        durable_evidence = provider_fence_store.acknowledgement(command.group_id)
        if durable_evidence != acknowledgement:
            raise FederationValidationError(
                "missing-durable-fencing-evidence",
                "acknowledgement",
                "provider-local durable fence does not match the acknowledgement",
            )
        if record.state == "fenced-old-authority":
            if record.fencing_ack_identity != acknowledgement.acknowledgement_id:
                raise FederationValidationError(
                    "fencing-ack-mismatch",
                    "acknowledgement_id",
                    "completed fencing stage has different durable evidence",
                )
            return record
        updated = PromotionTransactionRecord(
            **{
                **record.to_dict(),
                "fencing_status": "acknowledged",
                "fencing_acknowledged": True,
                "fencing_ack_identity": acknowledgement.acknowledgement_id,
                "state": "fenced-old-authority",
                "failure_code": None,
                "failure_reason": None,
                "updated_at": acknowledgement.persisted_at,
            }
        )
        return self.upsert_promotion_transaction(updated)

    def record_promotion_fencing_delivery_failure(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
        *,
        reason: str,
        now: datetime | None = None,
    ) -> PromotionTransactionRecord:
        record = self.promotion_transaction(session_id, group_id, promotion_id)
        if record is None:
            raise FederationValidationError("unknown-promotion", "promotion_id", "promotion transaction does not exist")
        if record.state != "validated":
            return record
        updated = PromotionTransactionRecord(
            **{
                **record.to_dict(),
                "fencing_status": "delivery-failed-retryable",
                "failure_code": "fencing-delivery-failed",
                "failure_reason": reason,
                "updated_at": _utc(now),
            }
        )
        return self.upsert_promotion_transaction(updated)

    def reserve_promotion_term(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
        *,
        provider_fence_store: ProviderFenceStore,
        now: datetime | None = None,
    ) -> PromotionTransactionRecord:
        record = self.promotion_transaction(session_id, group_id, promotion_id)
        if record is None:
            raise FederationValidationError("unknown-promotion", "promotion_id", "promotion transaction does not exist")
        fence = self._verify_persisted_promotion_fence(record, provider_fence_store)
        if record.reserved_term is not None:
            if record.reserved_term <= max(record.previous_term or 0, fence.fencing_high_water_term):
                raise FederationValidationError(
                    "corrupt-term-reservation",
                    "reserved_term",
                    "reserved term does not exceed durable fencing state",
                )
            return record
        if record.state != "fenced-old-authority":
            raise FederationValidationError("invalid-promotion-stage", "state", "transaction is not ready to reserve a term")
        updated_at = _utc(now)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """SELECT reserved_term, state FROM storage_promotion_transactions
                       WHERE session_id=? AND group_id=? AND promotion_id=?""",
                    (session_id, group_id, promotion_id),
                ).fetchone()
                if row is None:
                    raise FederationValidationError("unknown-promotion", "promotion_id", "promotion transaction disappeared")
                if row["reserved_term"] is not None:
                    connection.commit()
                    return self.promotion_transaction(session_id, group_id, promotion_id)
                counter = connection.execute(
                    """SELECT COALESCE(MAX(last_term), 0) FROM storage_fencing_counters
                       WHERE session_id=? AND group_id=?""",
                    (session_id, group_id),
                ).fetchone()[0]
                reservations = connection.execute(
                    """SELECT COALESCE(MAX(reserved_term), 0) FROM storage_promotion_transactions
                       WHERE session_id=? AND group_id=?""",
                    (session_id, group_id),
                ).fetchone()[0]
                event_rows = connection.execute(
                    """SELECT payload_json FROM storage_control_events
                       WHERE session_id=? AND event_type=?""",
                    (session_id, STORAGE_LEADER_GRANTED),
                ).fetchall()
                observed = 0
                for event_row in event_rows:
                    payload = json.loads(event_row["payload_json"])
                    if payload.get("group_id") == group_id:
                        observed = max(observed, int(payload["term"]))
                reserved_term = max(
                    record.previous_term or 0,
                    fence.fencing_high_water_term,
                    int(counter),
                    int(reservations),
                    observed,
                ) + 1
                grant_id = self._promotion_grant_id(record, reserved_term)
                connection.execute(
                    """UPDATE storage_promotion_transactions
                       SET reserved_term=?, grant_id=?, grant_status='reserved',
                           state='allocated-term', failure_code=NULL, failure_reason=NULL,
                           updated_at=?
                       WHERE session_id=? AND group_id=? AND promotion_id=?
                         AND reserved_term IS NULL AND state='fenced-old-authority'""",
                    (
                        reserved_term,
                        grant_id,
                        updated_at.isoformat().replace("+00:00", "Z"),
                        session_id,
                        group_id,
                        promotion_id,
                    ),
                )
                if connection.total_changes != 1:
                    raise FederationValidationError(
                        "concurrent-promotion-change",
                        "promotion_id",
                        "term reservation lost a concurrent update",
                    )
                connection.execute(
                    """INSERT INTO storage_fencing_counters
                       (session_id, group_id, last_term, last_fencing_token)
                       VALUES (?, ?, ?, 0)
                       ON CONFLICT(session_id, group_id) DO UPDATE SET
                           last_term=MAX(last_term, excluded.last_term)""",
                    (session_id, group_id, reserved_term),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        reserved = self.promotion_transaction(session_id, group_id, promotion_id)
        if reserved is None:
            raise FederationValidationError("corrupt-term-reservation", "promotion_id", "reserved transaction is missing")
        return reserved

    @staticmethod
    def _promotion_grant_id(record: PromotionTransactionRecord, reserved_term: int) -> str:
        return "promotion-grant-" + hashlib.sha256(
            (
                f"{record.promotion_id}\0{record.session_id}\0{record.group_id}\0"
                f"{record.selected_provider_id}\0{reserved_term}"
            ).encode()
        ).hexdigest()

    @staticmethod
    def _promotion_grant_idempotency_key(record: PromotionTransactionRecord) -> str:
        payload = (
            f"{record.promotion_id}\0{record.session_id}\0{record.group_id}\0"
            f"{record.selected_provider_id}\0{record.reserved_term}\0"
            f"{record.selected_manifest_revision}\0{record.selected_manifest_hash}\0"
            f"{record.selected_report_revision}\0{record.selected_report_hash}"
        ).encode()
        return "sha256:" + hashlib.sha256(payload).hexdigest()

    def promotion_grant_command(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
        *,
        provider_fence_store: ProviderFenceStore,
    ) -> StorageAuthorityGrantCommand:
        record = self.promotion_transaction(session_id, group_id, promotion_id)
        if record is None:
            raise FederationValidationError("unknown-promotion", "promotion_id", "promotion transaction does not exist")
        self._verify_persisted_promotion_fence(record, provider_fence_store)
        if record.state not in ("allocated-term", "granted-new-authority", "finalized"):
            raise FederationValidationError("invalid-promotion-stage", "state", "transaction has no durable term reservation")
        if record.reserved_term is None or record.grant_id is None:
            raise FederationValidationError("corrupt-term-reservation", "reserved_term", "durable grant binding is incomplete")
        if record.grant_id != self._promotion_grant_id(record, record.reserved_term):
            raise FederationValidationError(
                "corrupt-term-reservation",
                "grant_id",
                "durable grant identity does not match the reserved term and promotion binding",
            )
        manifest = self.manifest(session_id, group_id)
        assessment = self.latest_storage_replica_assessment(session_id, group_id, record.selected_provider_id)
        if (
            manifest.revision != record.selected_manifest_revision
            or manifest.manifest_hash != record.selected_manifest_hash
            or assessment is None
            or assessment.report is None
            or assessment.report_revision != record.selected_report_revision
            or assessment.report_hash != record.selected_report_hash
            or not assessment.accepted
            or not assessment.eligibility
        ):
            raise FederationValidationError(
                "stale-promotion-binding",
                "promotion_id",
                "manifest or selected report changed after transaction validation",
            )
        return StorageAuthorityGrantCommand(
            promotion_id=record.promotion_id,
            session_id=record.session_id,
            group_id=record.group_id,
            provider_id=record.selected_provider_id,
            term=record.reserved_term,
            manifest_revision=record.selected_manifest_revision,
            manifest_hash=record.selected_manifest_hash,
            report_revision=record.selected_report_revision,
            report_hash=record.selected_report_hash,
            grant_id=record.grant_id,
            idempotency_key=self._promotion_grant_idempotency_key(record),
        )

    def acknowledge_promotion_grant(
        self,
        acknowledgement: StorageAuthorityGrantAcknowledgement,
        *,
        actor_node_id: str,
        provider_fence_store: ProviderFenceStore,
        selected_provider_store: ProviderFenceStore,
    ) -> PromotionTransactionRecord:
        record = self.promotion_transaction(
            acknowledgement.session_id,
            acknowledgement.group_id,
            acknowledgement.promotion_id,
        )
        if record is None:
            raise FederationValidationError("unknown-promotion", "promotion_id", "promotion transaction does not exist")
        command = self.promotion_grant_command(
            record.session_id,
            record.group_id,
            record.promotion_id,
            provider_fence_store=provider_fence_store,
        )
        provider = self.snapshot(record.session_id).providers.get(record.selected_provider_id)
        if provider is None or provider.node_id != actor_node_id:
            raise FederationValidationError(
                "unauthenticated-grant-ack",
                "actor_node_id",
                "authenticated sender does not own the selected provider",
            )
        expected = {
            **command.binding(),
            "acknowledgement_id": ProviderFenceStore.grant_acknowledgement_identity(command),
        }
        actual = {
            **{
                field: getattr(acknowledgement, field)
                for field in command.binding()
            },
            "acknowledgement_id": acknowledgement.acknowledgement_id,
        }
        if actual != expected:
            raise FederationValidationError(
                "grant-ack-mismatch",
                "acknowledgement",
                "provider acknowledgement is not bound to the exact durable grant",
            )
        durable = selected_provider_store.grant_acknowledgement(record.group_id, record.promotion_id)
        if durable != acknowledgement:
            raise FederationValidationError(
                "missing-durable-grant-evidence",
                "acknowledgement",
                "provider-local durable grant does not match the acknowledgement",
            )
        if record.state == "granted-new-authority":
            if record.grant_ack_identity != acknowledgement.acknowledgement_id:
                raise FederationValidationError("grant-ack-mismatch", "acknowledgement_id", "durable grant evidence changed")
            return record
        updated = PromotionTransactionRecord(
            **{
                **record.to_dict(),
                "grant_status": "acknowledged",
                "grant_acknowledged": True,
                "grant_ack_identity": acknowledgement.acknowledgement_id,
                "state": "granted-new-authority",
                "failure_code": None,
                "failure_reason": None,
                "updated_at": acknowledgement.persisted_at,
            }
        )
        return self.upsert_promotion_transaction(updated)

    def record_promotion_grant_delivery_failure(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
        *,
        reason: str,
        now: datetime | None = None,
    ) -> PromotionTransactionRecord:
        record = self.promotion_transaction(session_id, group_id, promotion_id)
        if record is None:
            raise FederationValidationError("unknown-promotion", "promotion_id", "promotion transaction does not exist")
        if record.state != "allocated-term":
            return record
        return self.upsert_promotion_transaction(
            PromotionTransactionRecord(
                **{
                    **record.to_dict(),
                    "grant_status": "delivery-failed-retryable",
                    "failure_code": "grant-delivery-failed",
                    "failure_reason": reason,
                    "updated_at": _utc(now),
                }
            )
        )

    @staticmethod
    def _degraded_state_hash(state: dict[str, Any]) -> str:
        payload = json.dumps(state, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        return "sha256:" + hashlib.sha256(payload).hexdigest()

    @staticmethod
    def _final_authority_identity(record: PromotionTransactionRecord) -> str:
        payload = (
            f"{record.promotion_id}\0{record.session_id}\0{record.group_id}\0"
            f"{record.selected_provider_id}\0{record.reserved_term}\0"
            f"{record.grant_id}\0{record.grant_ack_identity}"
        ).encode()
        return "sha256:" + hashlib.sha256(payload).hexdigest()

    def promotion_finalization(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
    ) -> PromotionFinalizationRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM storage_promotion_finalizations
                   WHERE session_id=? AND group_id=? AND promotion_id=?""",
                (session_id, group_id, promotion_id),
            ).fetchone()
        if row is None:
            return None
        try:
            missing_ranges = json.loads(row["missing_ranges_json"])
            obligations = json.loads(row["recovery_obligations_json"])
            return PromotionFinalizationRecord(
                schema=PROMOTION_FINALIZATION_SCHEMA,
                promotion_id=row["promotion_id"],
                session_id=row["session_id"],
                group_id=row["group_id"],
                selected_provider_id=row["selected_provider_id"],
                new_authority_term=row["new_authority_term"],
                manifest_revision=row["manifest_revision"],
                manifest_hash=row["manifest_hash"],
                report_revision=row["report_revision"],
                report_hash=row["report_hash"],
                fencing_ack_identity=row["fencing_ack_identity"],
                grant_ack_identity=row["grant_ack_identity"],
                final_authority_identity=row["final_authority_identity"],
                degraded_state_hash=row["degraded_state_hash"],
                missing_ranges=tuple(missing_ranges),
                recovery_obligations=obligations,
                completed_at=row["completed_at"],
                degraded_cleared=bool(row["degraded_cleared"]),
            )
        except (json.JSONDecodeError, TypeError, ValueError, FederationValidationError) as exc:
            raise FederationValidationError(
                "corrupt-promotion-finalization",
                "storage_promotion_finalizations",
                "durable finalization state is malformed",
            ) from exc

    def _validate_promotion_finalization_chain(
        self,
        record: PromotionTransactionRecord,
        *,
        provider_fence_store: ProviderFenceStore,
        selected_provider_store: ProviderFenceStore,
    ) -> StorageAuthorityGrantAcknowledgement:
        if (
            record.state not in ("granted-new-authority", "finalized")
            or record.reserved_term is None
            or not record.grant_acknowledged
            or record.grant_ack_identity is None
            or record.fencing_ack_identity is None
        ):
            raise FederationValidationError(
                "promotion-not-granted",
                "state",
                "transaction does not contain a complete durable grant chain",
            )
        self._verify_persisted_promotion_fence(record, provider_fence_store)
        command = self.promotion_grant_command(
            record.session_id,
            record.group_id,
            record.promotion_id,
            provider_fence_store=provider_fence_store,
        )
        grant = selected_provider_store.grant_acknowledgement(record.group_id, record.promotion_id)
        if (
            grant is None
            or grant.acknowledgement_id != record.grant_ack_identity
            or grant.acknowledgement_id != ProviderFenceStore.grant_acknowledgement_identity(command)
            or {
                field: getattr(grant, field)
                for field in command.binding()
            }
            != command.binding()
            or not selected_provider_store.has_valid_grant(
                record.group_id,
                record.promotion_id,
                record.reserved_term,
            )
        ):
            raise FederationValidationError(
                "unverifiable-provider-grant",
                "grant_ack_identity",
                "durable provider grant no longer matches the promotion transaction",
            )
        return grant

    def persist_promotion_finalization(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
        *,
        provider_fence_store: ProviderFenceStore,
        selected_provider_store: ProviderFenceStore,
        now: datetime | None = None,
    ) -> PromotionFinalizationRecord:
        record = self.promotion_transaction(session_id, group_id, promotion_id)
        if record is None:
            raise FederationValidationError("unknown-promotion", "promotion_id", "promotion transaction does not exist")
        self._validate_promotion_finalization_chain(
            record,
            provider_fence_store=provider_fence_store,
            selected_provider_store=selected_provider_store,
        )
        existing = self.promotion_finalization(session_id, group_id, promotion_id)
        if existing is not None:
            expected_identity = self._final_authority_identity(record)
            if (
                existing.selected_provider_id != record.selected_provider_id
                or existing.new_authority_term != record.reserved_term
                or existing.manifest_revision != record.selected_manifest_revision
                or existing.manifest_hash != record.selected_manifest_hash
                or existing.report_revision != record.selected_report_revision
                or existing.report_hash != record.selected_report_hash
                or existing.fencing_ack_identity != record.fencing_ack_identity
                or existing.grant_ack_identity != record.grant_ack_identity
                or existing.final_authority_identity != expected_identity
            ):
                raise FederationValidationError(
                    "conflicting-promotion-finalization",
                    "promotion_id",
                    "durable finalization conflicts with the promotion transaction",
                )
            return existing
        degraded = self.storage_degraded_state(session_id, group_id)
        if degraded is None:
            raise FederationValidationError(
                "missing-storage-degraded-state",
                "group_id",
                "promotion finalization requires the degraded state it will exit",
            )
        if (
            degraded["manifest_revision"] != record.selected_manifest_revision
            or degraded["manifest_hash"] != record.selected_manifest_hash
        ):
            raise FederationValidationError(
                "degraded-state-mismatch",
                "storage_degraded_state",
                "degraded state is not bound to the promotion manifest",
            )
        completed_at = _utc(now)
        finalization = PromotionFinalizationRecord(
            schema=PROMOTION_FINALIZATION_SCHEMA,
            promotion_id=record.promotion_id,
            session_id=record.session_id,
            group_id=record.group_id,
            selected_provider_id=record.selected_provider_id,
            new_authority_term=record.reserved_term,
            manifest_revision=record.selected_manifest_revision,
            manifest_hash=record.selected_manifest_hash,
            report_revision=record.selected_report_revision,
            report_hash=record.selected_report_hash,
            fencing_ack_identity=record.fencing_ack_identity,
            grant_ack_identity=record.grant_ack_identity,
            final_authority_identity=self._final_authority_identity(record),
            degraded_state_hash=self._degraded_state_hash(degraded),
            missing_ranges=tuple(degraded["missing_ranges"]),
            recovery_obligations=degraded["obligations"],
            completed_at=completed_at,
            degraded_cleared=False,
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                head = connection.execute(
                    """SELECT revision, manifest_hash FROM storage_manifest_heads
                       WHERE session_id=? AND group_id=?""",
                    (session_id, group_id),
                ).fetchone()
                report = connection.execute(
                    """SELECT report_revision, report_hash FROM storage_replica_reports
                       WHERE session_id=? AND group_id=? AND provider_id=?""",
                    (session_id, group_id, record.selected_provider_id),
                ).fetchone()
                if (
                    head is None
                    or int(head["revision"]) != record.selected_manifest_revision
                    or str(head["manifest_hash"]) != record.selected_manifest_hash
                    or report is None
                    or int(report["report_revision"]) != record.selected_report_revision
                    or str(report["report_hash"]) != record.selected_report_hash
                ):
                    raise FederationValidationError(
                        "stale-promotion-binding",
                        "promotion_id",
                        "manifest or selected report changed before finalization commit",
                    )
                connection.execute(
                    """INSERT INTO storage_promotion_finalizations
                       (session_id, group_id, promotion_id, selected_provider_id,
                        new_authority_term, manifest_revision, manifest_hash,
                        report_revision, report_hash, fencing_ack_identity,
                        grant_ack_identity, final_authority_identity,
                        degraded_state_hash, missing_ranges_json,
                        recovery_obligations_json, completed_at, degraded_cleared)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                    (
                        finalization.session_id,
                        finalization.group_id,
                        finalization.promotion_id,
                        finalization.selected_provider_id,
                        finalization.new_authority_term,
                        finalization.manifest_revision,
                        finalization.manifest_hash,
                        finalization.report_revision,
                        finalization.report_hash,
                        finalization.fencing_ack_identity,
                        finalization.grant_ack_identity,
                        finalization.final_authority_identity,
                        finalization.degraded_state_hash,
                        json.dumps(list(finalization.missing_ranges), sort_keys=True, separators=(",", ":"), allow_nan=False),
                        json.dumps(finalization.recovery_obligations, sort_keys=True, separators=(",", ":"), allow_nan=False),
                        finalization.completed_at.isoformat().replace("+00:00", "Z"),
                    ),
                )
                connection.execute(
                    """UPDATE storage_promotion_transactions
                       SET state='finalized', updated_at=?
                       WHERE session_id=? AND group_id=? AND promotion_id=?
                         AND state='granted-new-authority'""",
                    (
                        finalization.completed_at.isoformat().replace("+00:00", "Z"),
                        session_id,
                        group_id,
                        promotion_id,
                    ),
                )
                if connection.total_changes != 2:
                    raise FederationValidationError(
                        "concurrent-promotion-change",
                        "promotion_id",
                        "promotion changed during finalization",
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return finalization

    def complete_promotion_finalization(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
        *,
        provider_fence_store: ProviderFenceStore,
        selected_provider_store: ProviderFenceStore,
        now: datetime | None = None,
    ) -> PromotionFinalizationRecord:
        finalization = self.persist_promotion_finalization(
            session_id,
            group_id,
            promotion_id,
            provider_fence_store=provider_fence_store,
            selected_provider_store=selected_provider_store,
            now=now,
        )
        if finalization.degraded_cleared:
            return finalization
        degraded = self.storage_degraded_state(session_id, group_id)
        if degraded is None or self._degraded_state_hash(degraded) != finalization.degraded_state_hash:
            raise FederationValidationError(
                "degraded-state-mismatch",
                "storage_degraded_state",
                "current degraded state is missing, newer, or unrelated",
            )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = connection.execute(
                    """DELETE FROM storage_degraded_states
                       WHERE session_id=? AND group_id=? AND manifest_revision=?
                         AND manifest_hash=? AND reason_code=? AND reason_detail=?
                         AND missing_ranges_json=? AND obligations_json=? AND updated_at=?""",
                    (
                        session_id,
                        group_id,
                        degraded["manifest_revision"],
                        degraded["manifest_hash"],
                        degraded["reason_code"],
                        degraded["reason_detail"],
                        json.dumps(degraded["missing_ranges"], sort_keys=True, separators=(",", ":"), allow_nan=False),
                        json.dumps(degraded["obligations"], sort_keys=True, separators=(",", ":"), allow_nan=False),
                        degraded["updated_at"],
                    ),
                )
                if cursor.rowcount != 1:
                    raise FederationValidationError(
                        "degraded-state-mismatch",
                        "storage_degraded_state",
                        "degraded state changed before it could be cleared",
                    )
                connection.execute(
                    """UPDATE storage_promotion_finalizations SET degraded_cleared=1
                       WHERE session_id=? AND group_id=? AND promotion_id=?
                         AND degraded_state_hash=? AND degraded_cleared=0""",
                    (session_id, group_id, promotion_id, finalization.degraded_state_hash),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        completed = self.promotion_finalization(session_id, group_id, promotion_id)
        if completed is None or not completed.degraded_cleared:
            raise FederationValidationError(
                "corrupt-promotion-finalization",
                "degraded_cleared",
                "degraded exit was not durably recorded",
            )
        return completed

    def promotion_recovery_obligations(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
    ) -> dict[str, Any]:
        finalization = self.promotion_finalization(session_id, group_id, promotion_id)
        if finalization is None:
            raise FederationValidationError("unknown-finalization", "promotion_id", "promotion is not finalized")
        return {
            "missing_ranges": list(finalization.missing_ranges),
            "obligations": finalization.recovery_obligations,
        }

    def fail_promotion_transaction(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
        *,
        failure_code: str,
        failure_reason: str,
        now: datetime | None = None,
    ) -> PromotionTransactionRecord:
        record = self.promotion_transaction(session_id, group_id, promotion_id)
        if record is None:
            raise FederationValidationError("unknown-promotion", "promotion_id", "promotion transaction does not exist")
        if record.state == "finalized":
            raise FederationValidationError("promotion-finalized", "state", "completed promotion cannot be failed")
        if record.state == "failed":
            if record.failure_code == failure_code and record.failure_reason == failure_reason:
                return record
            raise FederationValidationError(
                "conflicting-promotion-failure",
                "failure_code",
                "failed transaction already has different durable diagnostics",
            )
        return self.upsert_promotion_transaction(
            PromotionTransactionRecord(
                **{
                    **record.to_dict(),
                    "state": "failed",
                    "failure_code": failure_code,
                    "failure_reason": failure_reason,
                    "updated_at": _utc(now),
                }
            )
        )

    def recover_promotion(
        self,
        session_id: str,
        group_id: str,
        promotion_id: str,
        *,
        provider_fence_store: ProviderFenceStore,
        selected_provider_store: ProviderFenceStore,
        fencing_delivery_available: bool = True,
        grant_delivery_available: bool = True,
        now: datetime | None = None,
    ) -> PromotionRecoveryResult:
        return recover_storage_promotion(
            self,
            session_id,
            group_id,
            promotion_id,
            provider_fence_store=provider_fence_store,
            selected_provider_store=selected_provider_store,
            fencing_delivery_available=fencing_delivery_available,
            grant_delivery_available=grant_delivery_available,
            now=now,
        )

    def submit_storage_replica_report(
        self,
        report: StorageReplicaReport,
        *,
        actor_node_id: str,
    ) -> StorageReplicaAssessment:
        if not isinstance(report, StorageReplicaReport):
            raise FederationValidationError("invalid-object", "report", "must be StorageReplicaReport")
        if not isinstance(actor_node_id, str) or not actor_node_id.strip():
            raise FederationValidationError("invalid-id", "actor_node_id", "must be non-empty opaque text")
        snapshot = self.snapshot(report.session_id)
        assignment = snapshot.groups.get(report.group_id)
        provider = snapshot.providers.get(report.provider_id)
        if provider is None:
            assessment = StorageReplicaAssessment(
                session_id=report.session_id,
                group_id=report.group_id,
                provider_id=report.provider_id,
                report_revision=report.report_revision,
                report_hash=report.report_hash(),
                accepted=False,
                eligibility=False,
                eligibility_reason="provider-not-assigned",
                assessed_at=report.reported_at,
                report=report,
            )
        else:
            if provider.node_id != actor_node_id:
                raise FederationValidationError(
                    "provider-identity-mismatch",
                    "provider_id",
                    "authenticated sender does not match the registered provider",
                )
            if report.session_id != provider.session_id:
                raise FederationValidationError("session-mismatch", "session_id", "provider belongs to another session")
            if assignment is None or report.group_id != assignment.group_id or report.provider_id not in self._assigned_provider_ids(report.session_id, report.group_id):
                assessment = StorageReplicaAssessment(
                    session_id=report.session_id,
                    group_id=report.group_id,
                    provider_id=report.provider_id,
                    report_revision=report.report_revision,
                    report_hash=report.report_hash(),
                    accepted=False,
                    eligibility=False,
                    eligibility_reason="provider-not-assigned",
                    assessed_at=report.reported_at,
                    report=report,
                )
            else:
                authoritative = self.manifest(report.session_id, report.group_id)
                assessment = assess_storage_replica_report(
                    report,
                    authoritative,
                    assigned_provider_ids=self._assigned_provider_ids(report.session_id, report.group_id),
                    expected_session_id=report.session_id,
                    expected_group_id=report.group_id,
                )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """SELECT report_revision, report_hash, report_json, assessment_json
                       FROM storage_replica_reports
                       WHERE session_id=? AND group_id=? AND provider_id=?""",
                    (report.session_id, report.group_id, report.provider_id),
                ).fetchone()
                if row is not None:
                    existing = StorageReplicaReport.from_dict(json.loads(row["report_json"]))
                    if report.report_revision < existing.report_revision:
                        raise FederationValidationError("stale-report", "report_revision", "report revision is older than the stored report")
                    if report.report_revision == existing.report_revision:
                        if report.report_hash() != existing.report_hash():
                            raise FederationValidationError("conflicting-report-revision", "report_revision", "duplicate report revision conflicts with stored evidence")
                        connection.commit()
                        return StorageReplicaAssessment.from_dict(json.loads(row["assessment_json"]))
                if assessment.accepted:
                    connection.execute(
                        """INSERT INTO storage_replica_reports
                           (session_id, group_id, provider_id, report_revision, report_hash, report_json, assessment_json)
                           VALUES (?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT(session_id, group_id, provider_id)
                           DO UPDATE SET
                               report_revision=excluded.report_revision,
                               report_hash=excluded.report_hash,
                               report_json=excluded.report_json,
                               assessment_json=excluded.assessment_json""",
                        (
                            report.session_id,
                            report.group_id,
                            report.provider_id,
                            report.report_revision,
                            report.report_hash(),
                            json.dumps(report.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False),
                            json.dumps(assessment.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False),
                        ),
                    )
                connection.commit()
                return assessment
            except Exception:
                connection.rollback()
                raise

    def _historical_maxima(self, session_id: str, group_id: str) -> tuple[int, int]:
        term = 0
        token = 0
        for event in self.store.events(session_id):
            if event.event_type != STORAGE_LEADER_GRANTED or event.payload.get("group_id") != group_id:
                continue
            term = max(term, int(event.payload["term"]))
            token = max(token, int(event.payload["fencing_token"]))
        with self._connect() as connection:
            row = connection.execute(
                """SELECT last_term, last_fencing_token FROM storage_fencing_counters
                   WHERE session_id=? AND group_id=?""",
                (session_id, group_id),
            ).fetchone()
        if row is not None:
            term = max(term, int(row["last_term"]))
            token = max(token, int(row["last_fencing_token"]))
        return term, token

    def grant_leader(
        self,
        session_id: str,
        actor_node_id: str,
        group_id: str,
        provider_id: str,
        grant_id: str,
        term: int,
        fencing_token: int,
        *,
        lease_expires_at: datetime | None = None,
        scopes: tuple[str, ...] = (StorageOperation.BATCH_INGEST.value,),
        occurred_at: datetime | None = None,
    ) -> SessionEvent:
        last_term, last_token = self._historical_maxima(session_id, group_id)
        if term <= last_term:
            raise FederationValidationError("stale-term", "term", "must exceed every previously issued term")
        if fencing_token <= last_token:
            raise FederationValidationError(
                "stale-fencing-token",
                "fencing_token",
                "must exceed every previously issued fencing token",
            )
        event = self.store.grant_leader(
            session_id,
            actor_node_id,
            group_id,
            provider_id,
            grant_id,
            term,
            fencing_token,
            lease_expires_at=lease_expires_at,
            scopes=scopes,
            occurred_at=occurred_at,
        )
        self._record_counter(session_id, group_id, term, fencing_token)
        return event

    def _record_counter(self, session_id: str, group_id: str, term: int, fencing_token: int) -> None:
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO storage_fencing_counters
                       (session_id, group_id, last_term, last_fencing_token)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(session_id, group_id) DO UPDATE SET
                       last_term=MAX(last_term, excluded.last_term),
                       last_fencing_token=MAX(last_fencing_token, excluded.last_fencing_token)""",
                (session_id, group_id, term, fencing_token),
            )

    def revoke_leader(
        self,
        session_id: str,
        actor_node_id: str,
        group_id: str,
        grant_id: str,
    ) -> SessionEvent:
        return self.store.revoke_leader(session_id, actor_node_id, group_id, grant_id)

    def complete_handover(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        group_id: str,
        target_provider_id: str,
        grant_id: str,
        term: int,
        fencing_token: int,
        lease_expires_at: datetime | None = None,
        scopes: tuple[str, ...] = (StorageOperation.BATCH_INGEST.value,),
        occurred_at: datetime | None = None,
    ) -> HandoverCommit:
        """Atomically demote the source, promote the target, and issue its grant."""

        issued_at = _utc(occurred_at)
        expiry = _utc(lease_expires_at or issued_at + timedelta(minutes=5))
        snapshot = self.snapshot(session_id)
        assignment = snapshot.groups.get(group_id)
        if assignment is None or assignment.primary_provider_id is None:
            raise FederationValidationError("handover-no-primary", "group_id", "storage group has no assigned primary")
        source_provider_id = assignment.primary_provider_id
        if target_provider_id not in assignment.replica_provider_ids:
            raise FederationValidationError(
                "handover-target-not-replica",
                "target_provider_id",
                "target must be an assigned replica",
            )
        target = snapshot.providers.get(target_provider_id)
        if target is None or not target.assignable:
            raise FederationValidationError(
                "handover-target-unavailable", "target_provider_id", "target provider is not assignable"
            )
        active = snapshot.leader_grants.get(group_id)
        if active is None:
            raise FederationValidationError("unknown-grant", "group_id", "source primary has no active grant")
        last_term, last_token = self._historical_maxima(session_id, group_id)
        if term <= last_term:
            raise FederationValidationError("stale-term", "term", "must exceed every previously issued term")
        if fencing_token <= last_token:
            raise FederationValidationError(
                "stale-fencing-token", "fencing_token", "must exceed every previously issued fencing token"
            )
        if expiry <= issued_at:
            raise FederationValidationError("invalid-lease", "lease_expires_at", "must be later than issuance")

        new_replicas = tuple(
            provider_id
            for provider_id in (source_provider_id, *assignment.replica_provider_ids)
            if provider_id != target_provider_id
        )
        start_revision = snapshot.revision
        event_specs: list[tuple[str, dict[str, Any]]] = [
            (STORAGE_LEADER_REVOKED, {"group_id": group_id, "grant_id": active["grant_id"]}),
            (
                STORAGE_ASSIGNMENT_CHANGED,
                {
                    "group_id": group_id,
                    "primary_provider_id": target_provider_id,
                    "replica_provider_ids": list(dict.fromkeys(new_replicas)),
                },
            ),
            (
                STORAGE_LEADER_GRANTED,
                {
                    "group_id": group_id,
                    "provider_id": target_provider_id,
                    "grant_id": grant_id,
                    "term": term,
                    "fencing_token": fencing_token,
                    "lease_expires_at": expiry.isoformat(),
                    "scopes": list(scopes),
                },
            ),
        ]
        candidate = copy.deepcopy(snapshot)
        events: list[SessionEvent] = []
        for offset, (event_type, payload) in enumerate(event_specs, start=1):
            event = SessionEvent(
                session_id=session_id,
                revision=start_revision + offset,
                event_id=uuid.uuid4().hex,
                event_type=event_type,
                occurred_at=issued_at,
                actor_node_id=actor_node_id,
                payload=payload,
            )
            candidate.apply(event)
            events.append(event)

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current_revision = connection.execute(
                "SELECT COALESCE(MAX(revision), 0) FROM storage_control_events WHERE session_id=?",
                (session_id,),
            ).fetchone()[0]
            if current_revision != start_revision:
                connection.rollback()
                raise FederationValidationError(
                    "concurrent-control-change",
                    "revision",
                    "storage control state changed while handover was being prepared",
                )
            for event in events:
                connection.execute(
                    "INSERT INTO storage_control_events VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        event.session_id,
                        event.revision,
                        event.event_id,
                        event.event_type,
                        event.occurred_at.isoformat(),
                        event.actor_node_id,
                        json.dumps(event.payload, sort_keys=True, separators=(",", ":"), allow_nan=False),
                    ),
                )
            connection.execute(
                """INSERT INTO storage_fencing_counters
                       (session_id, group_id, last_term, last_fencing_token)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(session_id, group_id) DO UPDATE SET
                       last_term=excluded.last_term,
                       last_fencing_token=excluded.last_fencing_token""",
                (session_id, group_id, term, fencing_token),
            )
            connection.commit()

        return HandoverCommit(
            session_id,
            group_id,
            source_provider_id,
            target_provider_id,
            grant_id,
            term,
            fencing_token,
            events[-1].revision,
        )
