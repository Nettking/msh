from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.capabilities.artifact_contracts import (
    ArtifactDescriptor,
    ArtifactEndpointKind,
    ArtifactGrant,
    ArtifactGrantScope,
    ArtifactInputReference,
    ArtifactPublication,
    OutputPlacementPolicy,
)
from catalog.capabilities.artifact_publication import ArtifactResultCoordinator
from catalog.capabilities.artifact_runtime import SQLiteCapabilityArtifactAuthority
from catalog.capabilities.artifact_transfer import (
    ArtifactTransferMode,
    F6ArtifactTransferPlanner,
)
from catalog.capabilities.jobs import (
    ArtifactReference,
    AttemptStatus,
    CapabilityRequirement,
    JobContract,
    JobStatus,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore
from catalog.federation.errors import FederationValidationError
from catalog.federation.object_transfer import ObjectTransferManifest

NOW = datetime(2026, 8, 1, 13, 0, tzinfo=timezone.utc)
SESSION = "session-artifact-1"
JOB_ID = "job-artifact-1"
INPUT_ID = "input-artifact-1"
OUTPUT_ID = "output-artifact-1"
INPUT_HASH = "sha256:" + "b" * 64
OUTPUT_HASH = "sha256:" + "a" * 64
ENDPOINT = "storage-endpoint-1"


def _job() -> JobContract:
    return JobContract(
        job_id=JOB_ID,
        session_id=SESSION,
        request_id="request-artifact-1",
        idempotency_key=f"{SESSION}:request-artifact-1",
        capability=CapabilityRequirement(
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            requirements={"operation": "transform"},
        ),
        inputs=(
            ArtifactReference(
                reference_id=INPUT_ID,
                session_id=SESSION,
                schema_name="fcp.input.v1",
                media_type="application/json",
                content_hash=INPUT_HASH,
                size_bytes=64,
            ),
        ),
        outputs=(
            ArtifactReference(
                reference_id=OUTPUT_ID,
                session_id=SESSION,
                schema_name="fcp.output.v1",
                media_type="application/json",
            ),
        ),
        retry_policy=RetryPolicy(
            max_attempts=2,
            backoff_seconds=(2,),
            retryable_error_codes=("transient-worker-error",),
        ),
        timeout_policy=TimeoutPolicy(
            overall_timeout_seconds=120,
            queue_timeout_seconds=20,
            start_timeout_seconds=10,
            run_timeout_seconds=30,
            cancellation_grace_seconds=5,
        ),
    )


def _store(tmp_path: Path) -> SQLiteJobLifecycleStore:
    return SQLiteJobLifecycleStore(tmp_path / "jobs.sqlite3")


def _running(store: SQLiteJobLifecycleStore) -> None:
    job = _job()
    store.submit(job, coordinator_id="coordinator", now=NOW)
    store.queue(
        job.job_id,
        coordinator_id="coordinator",
        command_id="queue-artifact",
        expected_revision=0,
        now=NOW,
    )
    store.claim(
        job.job_id,
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-1",
        lease_id="lease-1",
        command_id="claim-artifact",
        expected_revision=1,
        lease_expires_at=NOW + timedelta(seconds=60),
        now=NOW,
    )
    store.record_attempt_status(
        job.job_id,
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-1",
        lease_id="lease-1",
        command_id="accepted-artifact",
        expected_revision=2,
        target_status=AttemptStatus.ACCEPTED,
        now=NOW + timedelta(seconds=1),
    )
    store.record_attempt_status(
        job.job_id,
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-1",
        lease_id="lease-1",
        command_id="running-artifact",
        expected_revision=3,
        target_status=AttemptStatus.RUNNING,
        now=NOW + timedelta(seconds=2),
    )


def _authority(tmp_path: Path) -> tuple[SQLiteJobLifecycleStore, SQLiteCapabilityArtifactAuthority]:
    store = _store(tmp_path)
    _running(store)
    authority = SQLiteCapabilityArtifactAuthority(store)
    authority.register_artifact(
        _input_descriptor(), authority_node_id="storage-authority", now=NOW
    )
    return store, authority


def _input_descriptor(**overrides: object) -> ArtifactDescriptor:
    values: dict[str, object] = {
        "artifact_id": INPUT_ID,
        "session_id": SESSION,
        "job_id": "producer-job-1",
        "schema_id": "fcp.input.v1",
        "media_type": "application/json",
        "content_hash": INPUT_HASH,
        "size_bytes": 64,
        "endpoint_id": ENDPOINT,
        "object_key": "sessions/session-artifact-1/inputs/input-artifact-1",
        "created_at": NOW - timedelta(seconds=10),
        "transfer_id": "transfer-input-1",
    }
    values.update(overrides)
    return ArtifactDescriptor(**values)  # type: ignore[arg-type]


def _input_reference(**overrides: object) -> ArtifactInputReference:
    values: dict[str, object] = {
        "artifact_id": INPUT_ID,
        "session_id": SESSION,
        "job_id": "producer-job-1",
        "content_hash": INPUT_HASH,
        "schema_id": "fcp.input.v1",
        "endpoint_id": ENDPOINT,
    }
    values.update(overrides)
    return ArtifactInputReference(**values)  # type: ignore[arg-type]


def _placement(**overrides: object) -> OutputPlacementPolicy:
    values: dict[str, object] = {
        "policy_id": "placement-1",
        "session_id": SESSION,
        "job_id": JOB_ID,
        "endpoint_kind": ArtifactEndpointKind.LOGICAL_STORAGE,
        "endpoint_id": ENDPOINT,
        "namespace": "sessions/session-artifact-1/jobs/job-artifact-1/outputs",
        "allowed_schema_ids": ("fcp.output.v1",),
        "max_artifact_bytes": 4096,
        "max_artifacts": 2,
        "resumable_threshold_bytes": 1024,
        "expires_at": NOW + timedelta(seconds=55),
    }
    values.update(overrides)
    return OutputPlacementPolicy(**values)  # type: ignore[arg-type]


def _output_descriptor(**overrides: object) -> ArtifactDescriptor:
    values: dict[str, object] = {
        "artifact_id": OUTPUT_ID,
        "session_id": SESSION,
        "job_id": JOB_ID,
        "schema_id": "fcp.output.v1",
        "media_type": "application/json",
        "content_hash": OUTPUT_HASH,
        "size_bytes": 128,
        "endpoint_id": ENDPOINT,
        "object_key": (
            "sessions/session-artifact-1/jobs/job-artifact-1/outputs/"
            "output-artifact-1"
        ),
        "created_at": NOW + timedelta(seconds=4),
        "transfer_id": "transfer-output-1",
    }
    values.update(overrides)
    return ArtifactDescriptor(**values)  # type: ignore[arg-type]


def _publication(**overrides: object) -> ArtifactPublication:
    values: dict[str, object] = {
        "publication_id": "publication-1",
        "grant_id": "grant-output-1",
        "session_id": SESSION,
        "job_id": JOB_ID,
        "attempt_id": "attempt-1",
        "lease_id": "lease-1",
        "lease_generation": 1,
        "provider_id": "provider-a",
        "worker_node_id": "worker-a",
        "descriptor": _output_descriptor(),
        "published_at": NOW + timedelta(seconds=5),
    }
    values.update(overrides)
    return ArtifactPublication(**values)  # type: ignore[arg-type]


def _read_grant(authority: SQLiteCapabilityArtifactAuthority) -> ArtifactGrant:
    return authority.issue_grant(
        JOB_ID,
        coordinator_node_id="coordinator",
        grant_id="grant-read-1",
        worker_node_id="worker-a",
        endpoint_id=ENDPOINT,
        scopes=(ArtifactGrantScope.READ_INPUT,),
        input_references=(_input_reference(),),
        expires_at=NOW + timedelta(seconds=50),
        now=NOW + timedelta(seconds=3),
    )


def _output_grant(authority: SQLiteCapabilityArtifactAuthority) -> ArtifactGrant:
    return authority.issue_grant(
        JOB_ID,
        coordinator_node_id="coordinator",
        grant_id="grant-output-1",
        worker_node_id="worker-a",
        endpoint_id=ENDPOINT,
        scopes=(
            ArtifactGrantScope.WRITE_OUTPUT,
            ArtifactGrantScope.PUBLISH_RESULT,
        ),
        placement_policy=_placement(),
        expires_at=NOW + timedelta(seconds=50),
        now=NOW + timedelta(seconds=3),
    )


def test_f76_contract_roundtrips_and_rejects_unsafe_identity() -> None:
    descriptor = _output_descriptor()
    placement = _placement()
    reference = _input_reference()
    publication = _publication()

    assert ArtifactDescriptor.from_dict(descriptor.to_dict()) == descriptor
    assert OutputPlacementPolicy.from_dict(placement.to_dict()) == placement
    assert ArtifactInputReference.from_dict(reference.to_dict()) == reference
    assert ArtifactPublication.from_dict(publication.to_dict()) == publication

    with pytest.raises(FederationValidationError) as unsafe:
        _output_descriptor(object_key="../private/database")
    assert unsafe.value.code == "invalid-logical-object-key"

    with pytest.raises(FederationValidationError) as digest:
        _output_descriptor(content_hash="sha256:not-a-digest")
    assert digest.value.code == "invalid-content-hash"


def test_f76_transfer_plan_reuses_f6_manifest_and_selects_resumable() -> None:
    planner = F6ArtifactTransferPlanner(chunk_size=256)
    small = planner.plan(_output_descriptor(), _placement(), now=NOW + timedelta(seconds=4))
    large_descriptor = _output_descriptor(
        size_bytes=2048,
        content_hash="sha256:" + "c" * 64,
        transfer_id="transfer-large-1",
    )
    large = planner.plan(
        large_descriptor,
        _placement(),
        now=NOW + timedelta(seconds=4),
    )

    assert isinstance(small.manifest, ObjectTransferManifest)
    assert small.mode is ArtifactTransferMode.VERIFIED_CHUNK
    assert large.mode is ArtifactTransferMode.RESUMABLE_CHUNK
    assert large.resumable
    assert large.manifest.object_id == large_descriptor.artifact_id
    assert large.manifest.object_hash == large_descriptor.content_hash
    assert large.manifest.total_size == large_descriptor.size_bytes
    assert large.manifest.chunk_count == 8


def test_f76_compute_ownership_does_not_imply_storage_read(tmp_path: Path) -> None:
    _, authority = _authority(tmp_path)
    _output_grant(authority)

    with pytest.raises(FederationValidationError) as denied:
        authority.authorize_input(
            "grant-output-1",
            _input_reference(),
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-a",
            provider_id="provider-a",
            now=NOW + timedelta(seconds=4),
        )
    assert denied.value.code == "artifact-input-not-granted"


def test_f76_output_grant_requires_explicit_placement(tmp_path: Path) -> None:
    _, authority = _authority(tmp_path)

    with pytest.raises(FederationValidationError) as missing:
        authority.issue_grant(
            JOB_ID,
            coordinator_node_id="coordinator",
            grant_id="grant-no-placement",
            worker_node_id="worker-a",
            endpoint_id=ENDPOINT,
            scopes=(ArtifactGrantScope.WRITE_OUTPUT,),
            expires_at=NOW + timedelta(seconds=50),
            now=NOW + timedelta(seconds=3),
        )
    assert missing.value.code == "output-placement-required"


def test_f76_only_declared_allowlisted_input_is_authorized(tmp_path: Path) -> None:
    _, authority = _authority(tmp_path)
    grant = _read_grant(authority)
    authorized = authority.authorize_input(
        grant.grant_id,
        _input_reference(),
        authenticated_session_id=SESSION,
        authenticated_worker_node_id="worker-a",
        provider_id="provider-a",
        now=NOW + timedelta(seconds=4),
    )
    assert authorized.artifact_id == INPUT_ID

    undeclared = _input_descriptor(
        artifact_id="undeclared-input",
        content_hash="sha256:" + "d" * 64,
        object_key="sessions/session-artifact-1/inputs/undeclared-input",
        transfer_id="transfer-undeclared",
    )
    authority.register_artifact(
        undeclared,
        authority_node_id="storage-authority",
        now=NOW + timedelta(seconds=1),
    )
    with pytest.raises(FederationValidationError) as rejected:
        authority.issue_grant(
            JOB_ID,
            coordinator_node_id="coordinator",
            grant_id="grant-undeclared",
            worker_node_id="worker-a",
            endpoint_id=ENDPOINT,
            scopes=(ArtifactGrantScope.READ_INPUT,),
            input_references=(
                ArtifactInputReference(
                    artifact_id=undeclared.artifact_id,
                    session_id=SESSION,
                    job_id=undeclared.job_id,
                    content_hash=undeclared.content_hash,
                    schema_id=undeclared.schema_id,
                    endpoint_id=ENDPOINT,
                ),
            ),
            expires_at=NOW + timedelta(seconds=50),
            now=NOW + timedelta(seconds=3),
        )
    assert rejected.value.code == "undeclared-job-input"


def test_f76_cross_session_and_wrong_actor_are_denied_and_audited(tmp_path: Path) -> None:
    _, authority = _authority(tmp_path)
    grant = _read_grant(authority)

    with pytest.raises(FederationValidationError) as cross_session:
        authority.authorize_input(
            grant.grant_id,
            _input_reference(session_id="session-other"),
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-a",
            provider_id="provider-a",
            now=NOW + timedelta(seconds=4),
        )
    assert cross_session.value.code == "cross-session-artifact-access"

    with pytest.raises(FederationValidationError) as wrong_worker:
        authority.authorize_input(
            grant.grant_id,
            _input_reference(),
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-b",
            provider_id="provider-a",
            now=NOW + timedelta(seconds=4),
        )
    assert wrong_worker.value.code == "artifact-worker-mismatch"

    with pytest.raises(FederationValidationError) as wrong_provider:
        authority.authorize_input(
            grant.grant_id,
            _input_reference(),
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-a",
            provider_id="provider-b",
            now=NOW + timedelta(seconds=4),
        )
    assert wrong_provider.value.code == "artifact-provider-mismatch"

    denied = [
        event
        for event in authority.audit_trail(JOB_ID)
        if event.event_type == "access-denied"
    ]
    assert len(denied) == 3


def test_f76_revocation_is_durable_and_only_issuer_can_revoke(tmp_path: Path) -> None:
    store, authority = _authority(tmp_path)
    grant = _read_grant(authority)

    with pytest.raises(FederationValidationError) as wrong_issuer:
        authority.revoke(
            grant.grant_id,
            coordinator_node_id="coordinator-other",
            reason="not-authorized",
            now=NOW + timedelta(seconds=4),
        )
    assert wrong_issuer.value.code == "grant-coordinator-mismatch"

    authority.revoke(
        grant.grant_id,
        coordinator_node_id="coordinator",
        reason="operator-request",
        now=NOW + timedelta(seconds=4),
    )
    reopened = SQLiteCapabilityArtifactAuthority(store)
    assert reopened.grant_issuer(grant.grant_id) == "coordinator"
    assert reopened.grant(grant.grant_id).revoked_at == NOW + timedelta(seconds=4)

    with pytest.raises(FederationValidationError) as revoked:
        reopened.authorize_input(
            grant.grant_id,
            _input_reference(),
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-a",
            provider_id="provider-a",
            now=NOW + timedelta(seconds=5),
        )
    assert revoked.value.code == "artifact-grant-revoked"


def test_f76_expiry_and_stale_lease_fail_closed(tmp_path: Path) -> None:
    store, authority = _authority(tmp_path)
    grant = _read_grant(authority)

    with pytest.raises(FederationValidationError) as expired:
        authority.authorize_input(
            grant.grant_id,
            _input_reference(),
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-a",
            provider_id="provider-a",
            now=NOW + timedelta(seconds=51),
        )
    assert expired.value.code == "artifact-grant-expired"

    store.release(
        JOB_ID,
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-1",
        lease_id="lease-1",
        command_id="release-artifact",
        expected_revision=4,
        now=NOW + timedelta(seconds=10),
    )
    with pytest.raises(FederationValidationError) as stale:
        authority.authorize_input(
            grant.grant_id,
            _input_reference(),
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-a",
            provider_id="provider-a",
            now=NOW + timedelta(seconds=11),
        )
    assert stale.value.code == "stale-artifact-grant"


def test_f76_output_policy_enforces_schema_size_and_count(tmp_path: Path) -> None:
    _, authority = _authority(tmp_path)
    _output_grant(authority)
    policy = _placement(max_artifacts=1)

    with pytest.raises(FederationValidationError) as schema:
        authority.authorize_output(
            "grant-output-1",
            _output_descriptor(schema_id="fcp.private.v1"),
            policy,
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-a",
            provider_id="provider-a",
            now=NOW + timedelta(seconds=4),
        )
    assert schema.value.code == "artifact-output-not-granted"

    with pytest.raises(FederationValidationError) as size:
        authority.authorize_output(
            "grant-output-1",
            _output_descriptor(size_bytes=5000),
            policy,
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-a",
            provider_id="provider-a",
            now=NOW + timedelta(seconds=4),
        )
    assert size.value.code == "artifact-output-not-granted"

    authority.publish(
        _publication(),
        policy,
        authenticated_session_id=SESSION,
        authenticated_worker_node_id="worker-a",
        provider_id="provider-a",
        now=NOW + timedelta(seconds=5),
    )
    second_descriptor = _output_descriptor(
        artifact_id="output-artifact-2",
        content_hash="sha256:" + "e" * 64,
        object_key=(
            "sessions/session-artifact-1/jobs/job-artifact-1/outputs/"
            "output-artifact-2"
        ),
        transfer_id="transfer-output-2",
    )
    with pytest.raises(FederationValidationError) as count:
        authority.publish(
            _publication(
                publication_id="publication-2",
                descriptor=second_descriptor,
            ),
            policy,
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-a",
            provider_id="provider-a",
            now=NOW + timedelta(seconds=6),
        )
    assert count.value.code == "artifact-count-limit"


def test_f76_publication_commits_one_fenced_job_result_and_replays(tmp_path: Path) -> None:
    store, authority = _authority(tmp_path)
    grant = _output_grant(authority)
    publication = _publication(grant_id=grant.grant_id)
    coordinator = ArtifactResultCoordinator(
        authority,
        store,
        coordinator_node_id="coordinator",
    )

    first = coordinator.publish_result(
        publication,
        _placement(),
        authenticated_session_id=SESSION,
        authenticated_worker_node_id="worker-a",
        provider_id="provider-a",
        command_id="commit-artifact-result",
        now=NOW + timedelta(seconds=5),
    )
    replay = coordinator.publish_result(
        publication,
        _placement(),
        authenticated_session_id=SESSION,
        authenticated_worker_node_id="worker-a",
        provider_id="provider-a",
        command_id="commit-artifact-result",
        now=NOW + timedelta(seconds=5),
    )

    assert first.changed
    assert not replay.changed
    assert first.result.reference.reference_id == OUTPUT_ID
    assert first.result.reference.content_hash == OUTPUT_HASH
    assert first.result.reference.size_bytes == 128
    assert store.snapshot(JOB_ID).job.status is JobStatus.SUCCEEDED
    assert store.result_commit(JOB_ID) == first.result
    events = [event.event_type for event in authority.audit_trail(JOB_ID)]
    assert "grant-issued" in events
    assert "output-authorized" in events
    assert "artifact-published" in events


def test_f76_conflicting_late_result_is_rejected(tmp_path: Path) -> None:
    store, authority = _authority(tmp_path)
    _output_grant(authority)
    coordinator = ArtifactResultCoordinator(
        authority,
        store,
        coordinator_node_id="coordinator",
    )
    coordinator.publish_result(
        _publication(),
        _placement(),
        authenticated_session_id=SESSION,
        authenticated_worker_node_id="worker-a",
        provider_id="provider-a",
        command_id="commit-artifact-result",
        now=NOW + timedelta(seconds=5),
    )

    conflicting = _publication(
        publication_id="publication-conflict",
        descriptor=_output_descriptor(content_hash="sha256:" + "f" * 64),
    )
    with pytest.raises(FederationValidationError) as conflict:
        coordinator.publish_result(
            conflicting,
            _placement(),
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-a",
            provider_id="provider-a",
            command_id="commit-artifact-conflict",
            now=NOW + timedelta(seconds=6),
        )
    assert conflict.value.code == "result-commit-conflict"


def test_f76_declared_output_schema_and_media_are_fenced(tmp_path: Path) -> None:
    store, authority = _authority(tmp_path)
    _output_grant(authority)
    coordinator = ArtifactResultCoordinator(
        authority,
        store,
        coordinator_node_id="coordinator",
    )
    publication = _publication(
        descriptor=_output_descriptor(media_type="application/octet-stream")
    )

    with pytest.raises(FederationValidationError) as mismatch:
        coordinator.publish_result(
            publication,
            _placement(),
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-a",
            provider_id="provider-a",
            command_id="commit-wrong-media",
            now=NOW + timedelta(seconds=5),
        )
    assert mismatch.value.code == "declared-output-identity-mismatch"


def test_f76_grants_and_artifact_identity_survive_restart(tmp_path: Path) -> None:
    store, authority = _authority(tmp_path)
    grant = _read_grant(authority)
    reopened_store = SQLiteJobLifecycleStore(store.database)
    reopened = SQLiteCapabilityArtifactAuthority(reopened_store)

    assert reopened.grant(grant.grant_id) == grant
    assert reopened.grant_issuer(grant.grant_id) == "coordinator"
    assert reopened.artifact(INPUT_ID) == _input_descriptor()
    assert reopened.authorize_input(
        grant.grant_id,
        _input_reference(),
        authenticated_session_id=SESSION,
        authenticated_worker_node_id="worker-a",
        provider_id="provider-a",
        now=NOW + timedelta(seconds=4),
    ).artifact_id == INPUT_ID
