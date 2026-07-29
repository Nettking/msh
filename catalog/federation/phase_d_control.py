"""Operational Phase D control-plane facade with durable fencing and handover.

The original D3 event store intentionally modelled assignments and grants in isolation.
This facade adds the missing operational invariants required before Phase E:
monotonic term/fencing counters survive revocation, acknowledgement policy is durable,
and controlled handover is committed as one SQLite transaction.
"""

from __future__ import annotations

import copy
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
