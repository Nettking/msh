"""Durable, fail-closed F7.6 artifact grants, access checks, and audit."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any

from catalog.federation.errors import FederationValidationError

from .artifact_contracts import (
    MAX_ARTIFACT_GRANT_SECONDS,
    ArtifactDescriptor,
    ArtifactGrant,
    ArtifactGrantScope,
    ArtifactInputReference,
    ArtifactPublication,
    OutputPlacementPolicy,
    _canonical,
    _text,
    _timestamp,
    _utc,
)
from .job_store import DurableJobSnapshot
from .lifecycle_store import SQLiteJobLifecycleStore

ARTIFACT_AUTHORITY_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ArtifactAuditEvent:
    sequence: int
    event_type: str
    session_id: str
    job_id: str
    actor_node_id: str
    event_at: datetime
    grant_id: str | None = None
    artifact_id: str | None = None
    details: dict[str, Any] | None = None


@dataclass(frozen=True)
class ArtifactPublicationResult:
    publication: ArtifactPublication
    changed: bool


class SQLiteArtifactAuthority:
    """Store artifact authority beside the durable F7 job state.

    The job store remains the source of truth for ownership. Every access check
    re-reads that state, so issuing a grant never transfers or extends a lease.
    """

    def __init__(
        self,
        job_store: SQLiteJobLifecycleStore,
        *,
        database_path: str | Path | None = None,
    ) -> None:
        self.job_store = job_store
        self.database_path = Path(
            database_path if database_path is not None else job_store.database_path
        )
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database_path,
            timeout=30,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS capability_artifact_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS capability_artifact_grants (
                    grant_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    lease_id TEXT NOT NULL,
                    lease_generation INTEGER NOT NULL,
                    provider_id TEXT NOT NULL,
                    worker_node_id TEXT NOT NULL,
                    endpoint_id TEXT NOT NULL,
                    grant_json TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    issued_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    revoked_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_capability_artifact_grants_job
                    ON capability_artifact_grants(job_id, attempt_id);
                CREATE TABLE IF NOT EXISTS capability_artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    source_job_id TEXT NOT NULL,
                    endpoint_id TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    schema_id TEXT NOT NULL,
                    descriptor_json TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    registered_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_capability_artifacts_session
                    ON capability_artifacts(session_id, source_job_id);
                CREATE TABLE IF NOT EXISTS capability_artifact_publications (
                    publication_id TEXT PRIMARY KEY,
                    grant_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    artifact_id TEXT NOT NULL,
                    publication_json TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    published_at TEXT NOT NULL,
                    FOREIGN KEY(grant_id)
                        REFERENCES capability_artifact_grants(grant_id)
                );
                CREATE INDEX IF NOT EXISTS idx_capability_publications_job
                    ON capability_artifact_publications(job_id, attempt_id);
                CREATE TABLE IF NOT EXISTS capability_artifact_audit (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    actor_node_id TEXT NOT NULL,
                    event_at TEXT NOT NULL,
                    grant_id TEXT,
                    artifact_id TEXT,
                    details_json TEXT
                );
                INSERT OR IGNORE INTO capability_artifact_meta(key, value)
                    VALUES('schema_version', '1');
                COMMIT;
                """
            )
            row = connection.execute(
                "SELECT value FROM capability_artifact_meta WHERE key='schema_version'"
            ).fetchone()
            if row is None or int(row["value"]) != ARTIFACT_AUTHORITY_SCHEMA_VERSION:
                raise FederationValidationError(
                    "unsupported-artifact-authority-schema",
                    "schema_version",
                    "artifact authority database schema is unsupported",
                )

    @staticmethod
    def _fingerprint(value: dict[str, Any]) -> str:
        return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()

    @staticmethod
    def _snapshot_attempt(snapshot: DurableJobSnapshot) -> tuple[str, str, int, str]:
        ownership = snapshot.ownership
        if ownership is None:
            raise FederationValidationError(
                "job-not-owned", "job_id", "artifact grants require active ownership"
            )
        return (
            ownership.attempt_id,
            ownership.lease_id,
            ownership.lease_generation,
            ownership.owner_provider_id,
        )

    @staticmethod
    def _audit(
        connection: sqlite3.Connection,
        *,
        event_type: str,
        session_id: str,
        job_id: str,
        actor_node_id: str,
        event_at: datetime,
        grant_id: str | None = None,
        artifact_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        connection.execute(
            """INSERT INTO capability_artifact_audit(
                   event_type, session_id, job_id, actor_node_id, event_at,
                   grant_id, artifact_id, details_json
               ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                _text(event_type, "event_type"),
                _text(session_id, "session_id"),
                _text(job_id, "job_id"),
                _text(actor_node_id, "actor_node_id"),
                _timestamp(_utc(event_at, "event_at")),
                grant_id,
                artifact_id,
                None if details is None else _canonical(details),
            ),
        )

    def _deny(
        self,
        *,
        code: str,
        field: str,
        message: str,
        session_id: str,
        job_id: str,
        actor_node_id: str,
        event_at: datetime,
        grant_id: str | None = None,
        artifact_id: str | None = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._audit(
                connection,
                event_type="access-denied",
                session_id=session_id,
                job_id=job_id,
                actor_node_id=actor_node_id,
                event_at=event_at,
                grant_id=grant_id,
                artifact_id=artifact_id,
                details={"reason_code": code, "field": field},
            )
            connection.commit()
        raise FederationValidationError(code, field, message)

    def register_artifact(
        self,
        descriptor: ArtifactDescriptor,
        *,
        authority_node_id: str,
        now: datetime,
    ) -> ArtifactDescriptor:
        authority_node_id = _text(authority_node_id, "authority_node_id")
        now = _utc(now, "now")
        payload = descriptor.to_dict()
        fingerprint = self._fingerprint(payload)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT descriptor_json, fingerprint FROM capability_artifacts WHERE artifact_id=?",
                (descriptor.artifact_id,),
            ).fetchone()
            if existing is not None:
                if existing["fingerprint"] != fingerprint:
                    connection.rollback()
                    raise FederationValidationError(
                        "artifact-identity-conflict",
                        "artifact_id",
                        "artifact ID is already bound to different content",
                    )
                connection.commit()
                return ArtifactDescriptor.from_dict(json.loads(existing["descriptor_json"]))
            connection.execute(
                """INSERT INTO capability_artifacts(
                       artifact_id, session_id, source_job_id, endpoint_id,
                       content_hash, size_bytes, schema_id, descriptor_json,
                       fingerprint, registered_at
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    descriptor.artifact_id,
                    descriptor.session_id,
                    descriptor.job_id,
                    descriptor.endpoint_id,
                    descriptor.content_hash,
                    descriptor.size_bytes,
                    descriptor.schema_id,
                    _canonical(payload),
                    fingerprint,
                    _timestamp(now),
                ),
            )
            self._audit(
                connection,
                event_type="artifact-registered",
                session_id=descriptor.session_id,
                job_id=descriptor.job_id,
                actor_node_id=authority_node_id,
                event_at=now,
                artifact_id=descriptor.artifact_id,
                details={
                    "endpoint_id": descriptor.endpoint_id,
                    "content_hash": descriptor.content_hash,
                    "size_bytes": descriptor.size_bytes,
                    "schema_id": descriptor.schema_id,
                },
            )
            connection.commit()
        return descriptor

    def artifact(self, artifact_id: str) -> ArtifactDescriptor:
        artifact_id = _text(artifact_id, "artifact_id")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT descriptor_json FROM capability_artifacts WHERE artifact_id=?",
                (artifact_id,),
            ).fetchone()
        if row is None:
            raise FederationValidationError(
                "artifact-not-found", "artifact_id", "artifact is not registered"
            )
        return ArtifactDescriptor.from_dict(json.loads(row["descriptor_json"]))

    def issue_grant(
        self,
        job_id: str,
        *,
        coordinator_node_id: str,
        grant_id: str,
        worker_node_id: str,
        endpoint_id: str,
        scopes: tuple[ArtifactGrantScope, ...],
        input_references: tuple[ArtifactInputReference, ...] = (),
        placement_policy: OutputPlacementPolicy | None = None,
        expires_at: datetime,
        now: datetime,
    ) -> ArtifactGrant:
        job_id = _text(job_id, "job_id")
        coordinator_node_id = _text(coordinator_node_id, "coordinator_node_id")
        worker_node_id = _text(worker_node_id, "worker_node_id")
        endpoint_id = _text(endpoint_id, "endpoint_id")
        now = _utc(now, "now")
        expires_at = _utc(expires_at, "expires_at")
        snapshot = self.job_store.snapshot(job_id)
        ownership = snapshot.ownership
        if ownership is None:
            raise FederationValidationError(
                "job-not-owned", "job_id", "artifact grants require active ownership"
            )
        if ownership.granted_by_coordinator_id != coordinator_node_id:
            raise FederationValidationError(
                "grant-coordinator-mismatch",
                "coordinator_node_id",
                "only the granting coordinator may issue artifact authority",
            )
        if now >= ownership.lease_expires_at:
            raise FederationValidationError(
                "ownership-lease-expired",
                "lease_expires_at",
                "cannot issue a grant for expired ownership",
            )
        if expires_at > ownership.lease_expires_at:
            raise FederationValidationError(
                "grant-outlives-ownership",
                "expires_at",
                "artifact grant must expire no later than the ownership lease",
            )
        if (expires_at - now).total_seconds() > MAX_ARTIFACT_GRANT_SECONDS:
            raise FederationValidationError(
                "grant-window-too-large",
                "expires_at",
                f"must not exceed {MAX_ARTIFACT_GRANT_SECONDS} seconds",
            )
        if snapshot.job.session_id is None:
            raise FederationValidationError(
                "job-session-missing", "session_id", "job has no session"
            )
        input_ids: list[str] = []
        for reference in input_references:
            if not isinstance(reference, ArtifactInputReference):
                raise FederationValidationError(
                    "invalid-input-reference",
                    "input_references",
                    "must contain ArtifactInputReference values",
                )
            if reference.session_id != snapshot.job.session_id:
                raise FederationValidationError(
                    "cross-session-artifact-access",
                    "session_id",
                    "input artifact belongs to a different session",
                )
            if reference.endpoint_id != endpoint_id:
                raise FederationValidationError(
                    "grant-endpoint-mismatch",
                    "endpoint_id",
                    "all input references must use the grant endpoint",
                )
            descriptor = self.artifact(reference.artifact_id)
            if (
                descriptor.session_id != reference.session_id
                or descriptor.content_hash != reference.content_hash
                or descriptor.schema_id != reference.schema_id
                or descriptor.endpoint_id != reference.endpoint_id
            ):
                raise FederationValidationError(
                    "input-reference-mismatch",
                    "input_references",
                    "input reference does not match the registered artifact",
                )
            input_ids.append(reference.artifact_id)
        output_namespace: str | None = None
        max_output_bytes = 0
        output_scopes = {
            ArtifactGrantScope.WRITE_OUTPUT,
            ArtifactGrantScope.PUBLISH_RESULT,
        }
        normalized_scopes = tuple(ArtifactGrantScope(scope) for scope in scopes)
        if output_scopes.intersection(normalized_scopes):
            if placement_policy is None:
                raise FederationValidationError(
                    "output-placement-required",
                    "placement_policy",
                    "output authority requires an explicit placement policy",
                )
            if (
                placement_policy.session_id != snapshot.job.session_id
                or placement_policy.job_id != job_id
            ):
                raise FederationValidationError(
                    "placement-context-mismatch",
                    "placement_policy",
                    "placement policy must match the job session and ID",
                )
            if placement_policy.endpoint_id != endpoint_id:
                raise FederationValidationError(
                    "placement-endpoint-mismatch",
                    "endpoint_id",
                    "placement endpoint differs from the grant endpoint",
                )
            if now >= placement_policy.expires_at:
                raise FederationValidationError(
                    "placement-policy-expired",
                    "expires_at",
                    "placement policy has expired",
                )
            output_namespace = placement_policy.namespace
            max_output_bytes = placement_policy.max_artifact_bytes
        elif placement_policy is not None:
            raise FederationValidationError(
                "unexpected-placement-policy",
                "placement_policy",
                "read-only grants cannot contain output placement authority",
            )
        grant = ArtifactGrant(
            grant_id=grant_id,
            session_id=snapshot.job.session_id,
            job_id=job_id,
            attempt_id=ownership.attempt_id,
            lease_id=ownership.lease_id,
            lease_generation=ownership.lease_generation,
            provider_id=ownership.owner_provider_id,
            worker_node_id=worker_node_id,
            endpoint_id=endpoint_id,
            scopes=normalized_scopes,
            input_artifact_ids=tuple(input_ids),
            output_namespace=output_namespace,
            max_output_bytes=max_output_bytes,
            issued_at=now,
            expires_at=expires_at,
        )
        payload = grant.to_dict()
        fingerprint = self._fingerprint(payload)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT grant_json, fingerprint FROM capability_artifact_grants WHERE grant_id=?",
                (grant.grant_id,),
            ).fetchone()
            if existing is not None:
                if existing["fingerprint"] != fingerprint:
                    connection.rollback()
                    raise FederationValidationError(
                        "grant-id-conflict",
                        "grant_id",
                        "grant ID is already bound to different authority",
                    )
                connection.commit()
                return ArtifactGrant.from_dict(json.loads(existing["grant_json"]))
            connection.execute(
                """INSERT INTO capability_artifact_grants(
                       grant_id, session_id, job_id, attempt_id, lease_id,
                       lease_generation, provider_id, worker_node_id, endpoint_id,
                       grant_json, fingerprint, issued_at, expires_at, revoked_at
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
                (
                    grant.grant_id,
                    grant.session_id,
                    grant.job_id,
                    grant.attempt_id,
                    grant.lease_id,
                    grant.lease_generation,
                    grant.provider_id,
                    grant.worker_node_id,
                    grant.endpoint_id,
                    _canonical(payload),
                    fingerprint,
                    _timestamp(grant.issued_at),
                    _timestamp(grant.expires_at),
                ),
            )
            self._audit(
                connection,
                event_type="grant-issued",
                session_id=grant.session_id,
                job_id=grant.job_id,
                actor_node_id=coordinator_node_id,
                event_at=now,
                grant_id=grant.grant_id,
                details={
                    "attempt_id": grant.attempt_id,
                    "provider_id": grant.provider_id,
                    "worker_node_id": grant.worker_node_id,
                    "endpoint_id": grant.endpoint_id,
                    "scopes": [scope.value for scope in grant.scopes],
                    "input_artifact_ids": list(grant.input_artifact_ids),
                    "output_namespace": grant.output_namespace,
                    "expires_at": _timestamp(grant.expires_at),
                },
            )
            connection.commit()
        return grant

    def grant(self, grant_id: str) -> ArtifactGrant:
        grant_id = _text(grant_id, "grant_id")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT grant_json, revoked_at FROM capability_artifact_grants WHERE grant_id=?",
                (grant_id,),
            ).fetchone()
        if row is None:
            raise FederationValidationError(
                "grant-not-found", "grant_id", "artifact grant does not exist"
            )
        grant = ArtifactGrant.from_dict(json.loads(row["grant_json"]))
        if row["revoked_at"] is not None and grant.revoked_at is None:
            grant = replace(
                grant,
                revoked_at=datetime.fromisoformat(row["revoked_at"].replace("Z", "+00:00")),
            )
        return grant

    def _validate_context(
        self,
        grant: ArtifactGrant,
        *,
        authenticated_session_id: str,
        authenticated_worker_node_id: str,
        provider_id: str,
        now: datetime,
    ) -> DurableJobSnapshot:
        authenticated_session_id = _text(
            authenticated_session_id, "authenticated_session_id"
        )
        authenticated_worker_node_id = _text(
            authenticated_worker_node_id, "authenticated_worker_node_id"
        )
        provider_id = _text(provider_id, "provider_id")
        now = _utc(now, "now")
        if authenticated_session_id != grant.session_id:
            self._deny(
                code="cross-session-artifact-access",
                field="authenticated_session_id",
                message="grant belongs to a different session",
                session_id=grant.session_id,
                job_id=grant.job_id,
                actor_node_id=authenticated_worker_node_id,
                event_at=now,
                grant_id=grant.grant_id,
            )
        if authenticated_worker_node_id != grant.worker_node_id:
            self._deny(
                code="artifact-worker-mismatch",
                field="authenticated_worker_node_id",
                message="grant belongs to a different worker",
                session_id=grant.session_id,
                job_id=grant.job_id,
                actor_node_id=authenticated_worker_node_id,
                event_at=now,
                grant_id=grant.grant_id,
            )
        if provider_id != grant.provider_id:
            self._deny(
                code="artifact-provider-mismatch",
                field="provider_id",
                message="grant belongs to a different provider",
                session_id=grant.session_id,
                job_id=grant.job_id,
                actor_node_id=authenticated_worker_node_id,
                event_at=now,
                grant_id=grant.grant_id,
            )
        if not grant.active_at(now):
            self._deny(
                code=("artifact-grant-revoked" if grant.revoked_at else "artifact-grant-expired"),
                field="grant_id",
                message="artifact grant is not active",
                session_id=grant.session_id,
                job_id=grant.job_id,
                actor_node_id=authenticated_worker_node_id,
                event_at=now,
                grant_id=grant.grant_id,
            )
        snapshot = self.job_store.snapshot(grant.job_id)
        ownership = snapshot.ownership
        if (
            snapshot.job.session_id != grant.session_id
            or ownership is None
            or ownership.attempt_id != grant.attempt_id
            or ownership.lease_id != grant.lease_id
            or ownership.lease_generation != grant.lease_generation
            or ownership.owner_provider_id != grant.provider_id
            or now >= ownership.lease_expires_at
        ):
            self._deny(
                code="stale-artifact-grant",
                field="grant_id",
                message="grant no longer matches active job ownership",
                session_id=grant.session_id,
                job_id=grant.job_id,
                actor_node_id=authenticated_worker_node_id,
                event_at=now,
                grant_id=grant.grant_id,
            )
        return snapshot

    def authorize_input(
        self,
        grant_id: str,
        reference: ArtifactInputReference,
        *,
        authenticated_session_id: str,
        authenticated_worker_node_id: str,
        provider_id: str,
        now: datetime,
    ) -> ArtifactDescriptor:
        grant = self.grant(grant_id)
        now = _utc(now, "now")
        self._validate_context(
            grant,
            authenticated_session_id=authenticated_session_id,
            authenticated_worker_node_id=authenticated_worker_node_id,
            provider_id=provider_id,
            now=now,
        )
        if reference.session_id != grant.session_id:
            self._deny(
                code="cross-session-artifact-access",
                field="session_id",
                message="input reference belongs to another session",
                session_id=grant.session_id,
                job_id=grant.job_id,
                actor_node_id=authenticated_worker_node_id,
                event_at=now,
                grant_id=grant.grant_id,
                artifact_id=reference.artifact_id,
            )
        if reference.endpoint_id != grant.endpoint_id or not grant.permits_input(
            reference.artifact_id
        ):
            self._deny(
                code="artifact-input-not-granted",
                field="artifact_id",
                message="input artifact is not explicitly allowlisted",
                session_id=grant.session_id,
                job_id=grant.job_id,
                actor_node_id=authenticated_worker_node_id,
                event_at=now,
                grant_id=grant.grant_id,
                artifact_id=reference.artifact_id,
            )
        descriptor = self.artifact(reference.artifact_id)
        if (
            descriptor.session_id != reference.session_id
            or descriptor.content_hash != reference.content_hash
            or descriptor.schema_id != reference.schema_id
            or descriptor.endpoint_id != reference.endpoint_id
        ):
            self._deny(
                code="input-reference-mismatch",
                field="input_reference",
                message="input reference does not match registered artifact identity",
                session_id=grant.session_id,
                job_id=grant.job_id,
                actor_node_id=authenticated_worker_node_id,
                event_at=now,
                grant_id=grant.grant_id,
                artifact_id=reference.artifact_id,
            )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._audit(
                connection,
                event_type="input-authorized",
                session_id=grant.session_id,
                job_id=grant.job_id,
                actor_node_id=authenticated_worker_node_id,
                event_at=now,
                grant_id=grant.grant_id,
                artifact_id=descriptor.artifact_id,
                details={"endpoint_id": descriptor.endpoint_id},
            )
            connection.commit()
        return descriptor

    def authorize_output(
        self,
        grant_id: str,
        descriptor: ArtifactDescriptor,
        placement_policy: OutputPlacementPolicy,
        *,
        authenticated_session_id: str,
        authenticated_worker_node_id: str,
        provider_id: str,
        now: datetime,
    ) -> None:
        grant = self.grant(grant_id)
        now = _utc(now, "now")
        self._validate_context(
            grant,
            authenticated_session_id=authenticated_session_id,
            authenticated_worker_node_id=authenticated_worker_node_id,
            provider_id=provider_id,
            now=now,
        )
        allowed = (
            ArtifactGrantScope.WRITE_OUTPUT in grant.scopes
            and grant.permits_output(descriptor)
            and placement_policy.accepts(descriptor, now=now)
            and placement_policy.endpoint_id == grant.endpoint_id
            and placement_policy.namespace == grant.output_namespace
        )
        if not allowed:
            self._deny(
                code="artifact-output-not-granted",
                field="descriptor",
                message="output does not match the explicit grant and placement policy",
                session_id=grant.session_id,
                job_id=grant.job_id,
                actor_node_id=authenticated_worker_node_id,
                event_at=now,
                grant_id=grant.grant_id,
                artifact_id=descriptor.artifact_id,
            )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            count = connection.execute(
                "SELECT COUNT(*) AS count FROM capability_artifact_publications WHERE grant_id=?",
                (grant.grant_id,),
            ).fetchone()["count"]
            if count >= placement_policy.max_artifacts:
                self._audit(
                    connection,
                    event_type="access-denied",
                    session_id=grant.session_id,
                    job_id=grant.job_id,
                    actor_node_id=authenticated_worker_node_id,
                    event_at=now,
                    grant_id=grant.grant_id,
                    artifact_id=descriptor.artifact_id,
                    details={"reason_code": "artifact-count-limit"},
                )
                connection.commit()
                raise FederationValidationError(
                    "artifact-count-limit",
                    "max_artifacts",
                    "placement policy artifact count is exhausted",
                )
            self._audit(
                connection,
                event_type="output-authorized",
                session_id=grant.session_id,
                job_id=grant.job_id,
                actor_node_id=authenticated_worker_node_id,
                event_at=now,
                grant_id=grant.grant_id,
                artifact_id=descriptor.artifact_id,
                details={
                    "endpoint_id": descriptor.endpoint_id,
                    "object_key": descriptor.object_key,
                    "size_bytes": descriptor.size_bytes,
                    "schema_id": descriptor.schema_id,
                },
            )
            connection.commit()

    def publish(
        self,
        publication: ArtifactPublication,
        placement_policy: OutputPlacementPolicy,
        *,
        authenticated_session_id: str,
        authenticated_worker_node_id: str,
        provider_id: str,
        now: datetime,
    ) -> ArtifactPublicationResult:
        now = _utc(now, "now")
        grant = self.grant(publication.grant_id)
        if ArtifactGrantScope.PUBLISH_RESULT not in grant.scopes:
            self._deny(
                code="artifact-publication-not-granted",
                field="scopes",
                message="grant does not include publish-result authority",
                session_id=grant.session_id,
                job_id=grant.job_id,
                actor_node_id=authenticated_worker_node_id,
                event_at=now,
                grant_id=grant.grant_id,
                artifact_id=publication.descriptor.artifact_id,
            )
        if (
            publication.session_id != grant.session_id
            or publication.job_id != grant.job_id
            or publication.attempt_id != grant.attempt_id
            or publication.lease_id != grant.lease_id
            or publication.lease_generation != grant.lease_generation
            or publication.provider_id != grant.provider_id
            or publication.worker_node_id != grant.worker_node_id
        ):
            self._deny(
                code="artifact-publication-context-mismatch",
                field="publication",
                message="publication does not match the exact grant authority",
                session_id=grant.session_id,
                job_id=grant.job_id,
                actor_node_id=authenticated_worker_node_id,
                event_at=now,
                grant_id=grant.grant_id,
                artifact_id=publication.descriptor.artifact_id,
            )
        self.authorize_output(
            grant.grant_id,
            publication.descriptor,
            placement_policy,
            authenticated_session_id=authenticated_session_id,
            authenticated_worker_node_id=authenticated_worker_node_id,
            provider_id=provider_id,
            now=now,
        )
        payload = publication.to_dict()
        fingerprint = self._fingerprint(payload)
        descriptor_payload = publication.descriptor.to_dict()
        descriptor_fingerprint = self._fingerprint(descriptor_payload)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """SELECT publication_json, fingerprint
                   FROM capability_artifact_publications WHERE publication_id=?""",
                (publication.publication_id,),
            ).fetchone()
            if existing is not None:
                if existing["fingerprint"] != fingerprint:
                    connection.rollback()
                    raise FederationValidationError(
                        "publication-id-conflict",
                        "publication_id",
                        "publication ID is already bound to different content",
                    )
                connection.commit()
                return ArtifactPublicationResult(
                    ArtifactPublication.from_dict(json.loads(existing["publication_json"])),
                    changed=False,
                )
            artifact = connection.execute(
                "SELECT fingerprint FROM capability_artifacts WHERE artifact_id=?",
                (publication.descriptor.artifact_id,),
            ).fetchone()
            if artifact is not None and artifact["fingerprint"] != descriptor_fingerprint:
                connection.rollback()
                raise FederationValidationError(
                    "artifact-identity-conflict",
                    "artifact_id",
                    "artifact ID is already bound to different content",
                )
            if artifact is None:
                connection.execute(
                    """INSERT INTO capability_artifacts(
                           artifact_id, session_id, source_job_id, endpoint_id,
                           content_hash, size_bytes, schema_id, descriptor_json,
                           fingerprint, registered_at
                       ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        publication.descriptor.artifact_id,
                        publication.descriptor.session_id,
                        publication.descriptor.job_id,
                        publication.descriptor.endpoint_id,
                        publication.descriptor.content_hash,
                        publication.descriptor.size_bytes,
                        publication.descriptor.schema_id,
                        _canonical(descriptor_payload),
                        descriptor_fingerprint,
                        _timestamp(now),
                    ),
                )
            connection.execute(
                """INSERT INTO capability_artifact_publications(
                       publication_id, grant_id, session_id, job_id, attempt_id,
                       artifact_id, publication_json, fingerprint, published_at
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    publication.publication_id,
                    publication.grant_id,
                    publication.session_id,
                    publication.job_id,
                    publication.attempt_id,
                    publication.descriptor.artifact_id,
                    _canonical(payload),
                    fingerprint,
                    _timestamp(publication.published_at),
                ),
            )
            self._audit(
                connection,
                event_type="artifact-published",
                session_id=publication.session_id,
                job_id=publication.job_id,
                actor_node_id=authenticated_worker_node_id,
                event_at=now,
                grant_id=publication.grant_id,
                artifact_id=publication.descriptor.artifact_id,
                details={
                    "publication_id": publication.publication_id,
                    "content_hash": publication.descriptor.content_hash,
                    "size_bytes": publication.descriptor.size_bytes,
                    "schema_id": publication.descriptor.schema_id,
                },
            )
            connection.commit()
        return ArtifactPublicationResult(publication, changed=True)

    def revoke(
        self,
        grant_id: str,
        *,
        coordinator_node_id: str,
        reason: str,
        now: datetime,
    ) -> ArtifactGrant:
        grant = self.grant(grant_id)
        coordinator_node_id = _text(coordinator_node_id, "coordinator_node_id")
        reason = _text(reason, "reason")
        now = _utc(now, "now")
        snapshot = self.job_store.snapshot(grant.job_id)
        ownership = snapshot.ownership
        if ownership is not None and ownership.granted_by_coordinator_id != coordinator_node_id:
            raise FederationValidationError(
                "grant-coordinator-mismatch",
                "coordinator_node_id",
                "only the granting coordinator may revoke artifact authority",
            )
        if grant.revoked_at is not None:
            return grant
        revoked = replace(grant, revoked_at=now)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE capability_artifact_grants SET revoked_at=? WHERE grant_id=?",
                (_timestamp(now), grant.grant_id),
            )
            self._audit(
                connection,
                event_type="grant-revoked",
                session_id=grant.session_id,
                job_id=grant.job_id,
                actor_node_id=coordinator_node_id,
                event_at=now,
                grant_id=grant.grant_id,
                details={"reason": reason},
            )
            connection.commit()
        return revoked

    def expire(self, *, now: datetime, actor_node_id: str) -> tuple[str, ...]:
        now = _utc(now, "now")
        actor_node_id = _text(actor_node_id, "actor_node_id")
        expired: list[str] = []
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """SELECT grant_id, session_id, job_id FROM capability_artifact_grants
                   WHERE revoked_at IS NULL AND expires_at <= ?""",
                (_timestamp(now),),
            ).fetchall()
            for row in rows:
                connection.execute(
                    "UPDATE capability_artifact_grants SET revoked_at=? WHERE grant_id=?",
                    (_timestamp(now), row["grant_id"]),
                )
                self._audit(
                    connection,
                    event_type="grant-expired",
                    session_id=row["session_id"],
                    job_id=row["job_id"],
                    actor_node_id=actor_node_id,
                    event_at=now,
                    grant_id=row["grant_id"],
                )
                expired.append(row["grant_id"])
            connection.commit()
        return tuple(expired)

    def audit_trail(self, job_id: str) -> tuple[ArtifactAuditEvent, ...]:
        job_id = _text(job_id, "job_id")
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT sequence, event_type, session_id, job_id, actor_node_id,
                          event_at, grant_id, artifact_id, details_json
                   FROM capability_artifact_audit WHERE job_id=? ORDER BY sequence""",
                (job_id,),
            ).fetchall()
        events: list[ArtifactAuditEvent] = []
        for row in rows:
            events.append(
                ArtifactAuditEvent(
                    sequence=row["sequence"],
                    event_type=row["event_type"],
                    session_id=row["session_id"],
                    job_id=row["job_id"],
                    actor_node_id=row["actor_node_id"],
                    event_at=datetime.fromisoformat(row["event_at"].replace("Z", "+00:00")),
                    grant_id=row["grant_id"],
                    artifact_id=row["artifact_id"],
                    details=(
                        None
                        if row["details_json"] is None
                        else json.loads(row["details_json"])
                    ),
                )
            )
        return tuple(events)
