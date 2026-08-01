"""Runtime binding for F7.6 artifact authority and durable grant issuers."""

from __future__ import annotations

import json
from pathlib import Path

from catalog.federation.errors import FederationValidationError

from .artifact_authority import ArtifactPublicationResult, SQLiteArtifactAuthority
from .artifact_contracts import (
    ArtifactDescriptor,
    ArtifactGrant,
    ArtifactGrantScope,
    ArtifactInputReference,
    ArtifactPublication,
    OutputPlacementPolicy,
    _canonical,
    _text,
)
from .lifecycle_store import SQLiteJobLifecycleStore


class SQLiteCapabilityArtifactAuthority(SQLiteArtifactAuthority):
    """Use the F7 database path and retain issuers and policies durably."""

    def __init__(
        self,
        job_store: SQLiteJobLifecycleStore,
        *,
        database_path: str | Path | None = None,
    ) -> None:
        resolved = job_store.database if database_path is None else database_path
        super().__init__(job_store, database_path=resolved)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS capability_artifact_grant_issuers (
                    grant_id TEXT PRIMARY KEY,
                    coordinator_node_id TEXT NOT NULL,
                    FOREIGN KEY(grant_id)
                        REFERENCES capability_artifact_grants(grant_id)
                );
                CREATE TABLE IF NOT EXISTS capability_artifact_grant_policies (
                    grant_id TEXT PRIMARY KEY,
                    policy_json TEXT NOT NULL CHECK(json_valid(policy_json)),
                    fingerprint TEXT NOT NULL,
                    FOREIGN KEY(grant_id)
                        REFERENCES capability_artifact_grants(grant_id)
                );
                """
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
            if descriptor.job_id != reference.job_id:
                raise FederationValidationError(
                    "input-reference-provenance-mismatch",
                    "input_references",
                    "input reference source job does not match the registered artifact",
                )
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
        policy_json = None if placement_policy is None else _canonical(placement_policy.to_dict())
        policy_fingerprint = (
            None
            if placement_policy is None
            else self._fingerprint(placement_policy.to_dict())
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            issuer = connection.execute(
                """SELECT coordinator_node_id
                   FROM capability_artifact_grant_issuers WHERE grant_id=?""",
                (grant.grant_id,),
            ).fetchone()
            if issuer is not None and issuer["coordinator_node_id"] != coordinator_node_id:
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
            persisted = connection.execute(
                """SELECT policy_json, fingerprint
                   FROM capability_artifact_grant_policies WHERE grant_id=?""",
                (grant.grant_id,),
            ).fetchone()
            if policy_json is None:
                if persisted is not None:
                    connection.rollback()
                    raise FederationValidationError(
                        "grant-policy-conflict",
                        "placement_policy",
                        "grant replay changed its placement authority",
                    )
            elif persisted is None:
                connection.execute(
                    """INSERT INTO capability_artifact_grant_policies
                       (grant_id, policy_json, fingerprint) VALUES(?, ?, ?)""",
                    (grant.grant_id, policy_json, policy_fingerprint),
                )
            elif persisted["fingerprint"] != policy_fingerprint:
                connection.rollback()
                raise FederationValidationError(
                    "grant-policy-conflict",
                    "placement_policy",
                    "grant replay changed its placement authority",
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

    def grant_policy(self, grant_id: str) -> OutputPlacementPolicy | None:
        grant_id = _text(grant_id, "grant_id")
        with self._connect() as connection:
            row = connection.execute(
                """SELECT policy_json FROM capability_artifact_grant_policies
                   WHERE grant_id=?""",
                (grant_id,),
            ).fetchone()
        return (
            None
            if row is None
            else OutputPlacementPolicy.from_dict(json.loads(row["policy_json"]))
        )

    def _fence_policy(
        self,
        grant_id: str,
        supplied: OutputPlacementPolicy,
    ) -> OutputPlacementPolicy:
        if not isinstance(supplied, OutputPlacementPolicy):
            raise FederationValidationError(
                "invalid-placement-policy",
                "placement_policy",
                "must be an OutputPlacementPolicy",
            )
        issued = self.grant_policy(grant_id)
        if issued is None:
            raise FederationValidationError(
                "placement-policy-not-granted",
                "grant_id",
                "grant has no output placement authority",
            )
        broader = (
            supplied.policy_id != issued.policy_id
            or supplied.session_id != issued.session_id
            or supplied.job_id != issued.job_id
            or supplied.endpoint_kind != issued.endpoint_kind
            or supplied.endpoint_id != issued.endpoint_id
            or supplied.namespace != issued.namespace
            or not set(supplied.allowed_schema_ids).issubset(issued.allowed_schema_ids)
            or supplied.max_artifact_bytes > issued.max_artifact_bytes
            or supplied.max_artifacts > issued.max_artifacts
            or supplied.resumable_threshold_bytes > issued.resumable_threshold_bytes
            or supplied.expires_at > issued.expires_at
        )
        if broader:
            raise FederationValidationError(
                "placement-policy-widening",
                "placement_policy",
                "worker-supplied placement policy exceeds the issued authority",
            )
        return supplied

    def authorize_output(
        self,
        grant_id: str,
        descriptor: ArtifactDescriptor,
        placement_policy: OutputPlacementPolicy,
        *,
        authenticated_session_id: str,
        authenticated_worker_node_id: str,
        provider_id: str,
        now,
    ) -> None:
        fenced = self._fence_policy(grant_id, placement_policy)
        return super().authorize_output(
            grant_id,
            descriptor,
            fenced,
            authenticated_session_id=authenticated_session_id,
            authenticated_worker_node_id=authenticated_worker_node_id,
            provider_id=provider_id,
            now=now,
        )

    def publish(
        self,
        publication: ArtifactPublication,
        placement_policy: OutputPlacementPolicy,
        *,
        authenticated_session_id: str,
        authenticated_worker_node_id: str,
        provider_id: str,
        now,
    ) -> ArtifactPublicationResult:
        fenced = self._fence_policy(publication.grant_id, placement_policy)
        return super().publish(
            publication,
            fenced,
            authenticated_session_id=authenticated_session_id,
            authenticated_worker_node_id=authenticated_worker_node_id,
            provider_id=provider_id,
            now=now,
        )

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
