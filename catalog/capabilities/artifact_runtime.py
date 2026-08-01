"""Runtime binding for F7.6 artifact authority and durable grant issuers."""

from __future__ import annotations

from pathlib import Path

from catalog.federation.errors import FederationValidationError

from .artifact_authority import SQLiteArtifactAuthority
from .artifact_contracts import (
    ArtifactGrant,
    ArtifactGrantScope,
    ArtifactInputReference,
    OutputPlacementPolicy,
    _text,
)
from .lifecycle_store import SQLiteJobLifecycleStore


class SQLiteCapabilityArtifactAuthority(SQLiteArtifactAuthority):
    """Use the F7 database path and retain the granting coordinator durably."""

    def __init__(
        self,
        job_store: SQLiteJobLifecycleStore,
        *,
        database_path: str | Path | None = None,
    ) -> None:
        resolved = job_store.database if database_path is None else database_path
        super().__init__(job_store, database_path=resolved)
        with self._connect() as connection:
            connection.execute(
                """CREATE TABLE IF NOT EXISTS capability_artifact_grant_issuers (
                       grant_id TEXT PRIMARY KEY,
                       coordinator_node_id TEXT NOT NULL,
                       FOREIGN KEY(grant_id)
                           REFERENCES capability_artifact_grants(grant_id)
                   )"""
            )

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
        expires_at,
        now,
    ) -> ArtifactGrant:
        coordinator_node_id = _text(coordinator_node_id, "coordinator_node_id")
        snapshot = self.job_store.snapshot(job_id)
        declared_inputs = {
            (
                reference.reference_id,
                reference.session_id,
                reference.schema_name,
                reference.content_hash,
                reference.size_bytes,
            )
            for reference in snapshot.job.inputs
        }
        for reference in input_references:
            descriptor = self.artifact(reference.artifact_id)
            declared = (
                reference.artifact_id,
                reference.session_id,
                reference.schema_id,
                reference.content_hash,
                descriptor.size_bytes,
            )
            if declared not in declared_inputs:
                raise FederationValidationError(
                    "undeclared-job-input",
                    "input_references",
                    "artifact input is not declared by the job contract",
                )
        grant = super().issue_grant(
            job_id,
            coordinator_node_id=coordinator_node_id,
            grant_id=grant_id,
            worker_node_id=worker_node_id,
            endpoint_id=endpoint_id,
            scopes=scopes,
            input_references=input_references,
            placement_policy=placement_policy,
            expires_at=expires_at,
            now=now,
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """SELECT coordinator_node_id
                   FROM capability_artifact_grant_issuers WHERE grant_id=?""",
                (grant.grant_id,),
            ).fetchone()
            if row is not None and row["coordinator_node_id"] != coordinator_node_id:
                connection.rollback()
                raise FederationValidationError(
                    "grant-issuer-conflict",
                    "coordinator_node_id",
                    "grant ID is already bound to another coordinator",
                )
            connection.execute(
                """INSERT OR IGNORE INTO capability_artifact_grant_issuers
                   (grant_id, coordinator_node_id) VALUES(?, ?)""",
                (grant.grant_id, coordinator_node_id),
            )
            connection.commit()
        return grant

    def grant_issuer(self, grant_id: str) -> str:
        grant_id = _text(grant_id, "grant_id")
        with self._connect() as connection:
            row = connection.execute(
                """SELECT coordinator_node_id
                   FROM capability_artifact_grant_issuers WHERE grant_id=?""",
                (grant_id,),
            ).fetchone()
        if row is None:
            raise FederationValidationError(
                "grant-issuer-missing",
                "grant_id",
                "artifact grant has no durable issuer",
            )
        return row["coordinator_node_id"]

    def revoke(
        self,
        grant_id: str,
        *,
        coordinator_node_id: str,
        reason: str,
        now,
    ) -> ArtifactGrant:
        coordinator_node_id = _text(coordinator_node_id, "coordinator_node_id")
        if self.grant_issuer(grant_id) != coordinator_node_id:
            raise FederationValidationError(
                "grant-coordinator-mismatch",
                "coordinator_node_id",
                "only the durable grant issuer may revoke artifact authority",
            )
        return super().revoke(
            grant_id,
            coordinator_node_id=coordinator_node_id,
            reason=reason,
            now=now,
        )
