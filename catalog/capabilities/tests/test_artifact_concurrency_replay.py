from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from threading import Barrier

import pytest

from catalog.capabilities.artifact_contracts import ArtifactGrantScope
from catalog.capabilities.artifact_publication import ArtifactResultCoordinator
from catalog.capabilities.tests.test_artifact_authorization import (
    ENDPOINT,
    JOB_ID,
    NOW,
    SESSION,
    _authority,
    _output_descriptor,
    _placement,
    _publication,
)
from catalog.federation.errors import FederationValidationError


def _output_grant(authority, *, max_artifacts: int = 1) -> None:
    authority.issue_grant(
        JOB_ID,
        coordinator_node_id="coordinator",
        grant_id="grant-output-1",
        worker_node_id="worker-a",
        endpoint_id=ENDPOINT,
        scopes=(
            ArtifactGrantScope.WRITE_OUTPUT,
            ArtifactGrantScope.PUBLISH_RESULT,
        ),
        placement_policy=_placement(max_artifacts=max_artifacts),
        expires_at=NOW + timedelta(seconds=50),
        now=NOW + timedelta(seconds=3),
    )


def test_f76_parallel_publications_cannot_exceed_artifact_count(
    tmp_path: Path,
) -> None:
    _, authority = _authority(tmp_path)
    _output_grant(authority, max_artifacts=1)
    policy = _placement(max_artifacts=1)
    barrier = Barrier(2)
    publications = (
        _publication(),
        _publication(
            publication_id="publication-2",
            descriptor=_output_descriptor(
                artifact_id="output-artifact-2",
                content_hash="sha256:" + "e" * 64,
                object_key=(
                    "sessions/session-artifact-1/jobs/job-artifact-1/outputs/"
                    "output-artifact-2"
                ),
                transfer_id="transfer-output-2",
            ),
        ),
    )

    def publish(publication):
        barrier.wait(timeout=5)
        try:
            result = authority.publish(
                publication,
                policy,
                authenticated_session_id=SESSION,
                authenticated_worker_node_id="worker-a",
                provider_id="provider-a",
                now=NOW + timedelta(seconds=5),
            )
            return ("success", result.publication.publication_id)
        except FederationValidationError as error:
            return (error.code, publication.publication_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(publish, publications))

    assert [outcome[0] for outcome in outcomes].count("success") == 1
    assert [outcome[0] for outcome in outcomes].count("artifact-count-limit") == 1


def test_f76_stale_publication_reservation_is_recovered(tmp_path: Path) -> None:
    _, authority = _authority(tmp_path)
    _output_grant(authority)
    publication = _publication()
    with authority._connect() as connection:
        connection.execute(
            """INSERT INTO capability_artifact_publication_reservations(
                   publication_id, grant_id, artifact_id,
                   reservation_token, reserved_at
               ) VALUES(?, ?, ?, ?, ?)""",
            (
                publication.publication_id,
                publication.grant_id,
                publication.descriptor.artifact_id,
                "abandoned-token",
                "2026-08-01T12:59:00Z",
            ),
        )

    result = authority.publish(
        publication,
        _placement(max_artifacts=1),
        authenticated_session_id=SESSION,
        authenticated_worker_node_id="worker-a",
        provider_id="provider-a",
        now=NOW + timedelta(seconds=5),
    )

    assert result.changed
    with authority._connect() as connection:
        remaining = connection.execute(
            """SELECT COUNT(*) AS count
               FROM capability_artifact_publication_reservations"""
        ).fetchone()["count"]
    assert remaining == 0


def test_f76_terminal_replay_requires_same_authenticated_worker(tmp_path: Path) -> None:
    store, authority = _authority(tmp_path)
    _output_grant(authority)
    coordinator = ArtifactResultCoordinator(
        authority,
        store,
        coordinator_node_id="coordinator",
    )
    publication = _publication()
    coordinator.publish_result(
        publication,
        _placement(max_artifacts=1),
        authenticated_session_id=SESSION,
        authenticated_worker_node_id="worker-a",
        provider_id="provider-a",
        command_id="commit-artifact-result",
        now=NOW + timedelta(seconds=5),
    )

    with pytest.raises(FederationValidationError) as wrong_worker:
        coordinator.publish_result(
            publication,
            _placement(max_artifacts=1),
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-b",
            provider_id="provider-a",
            command_id="commit-artifact-result",
            now=NOW + timedelta(seconds=6),
        )
    assert wrong_worker.value.code == "artifact-worker-mismatch"


def test_f76_terminal_replay_requires_same_durable_publication(tmp_path: Path) -> None:
    store, authority = _authority(tmp_path)
    _output_grant(authority)
    coordinator = ArtifactResultCoordinator(
        authority,
        store,
        coordinator_node_id="coordinator",
    )
    publication = _publication()
    coordinator.publish_result(
        publication,
        _placement(max_artifacts=1),
        authenticated_session_id=SESSION,
        authenticated_worker_node_id="worker-a",
        provider_id="provider-a",
        command_id="commit-artifact-result",
        now=NOW + timedelta(seconds=5),
    )

    with pytest.raises(FederationValidationError) as missing:
        coordinator.publish_result(
            _publication(publication_id="publication-forged-replay"),
            _placement(max_artifacts=1),
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-a",
            provider_id="provider-a",
            command_id="commit-artifact-result",
            now=NOW + timedelta(seconds=6),
        )
    assert missing.value.code == "publication-not-found"


def test_f76_terminal_replay_succeeds_with_max_artifacts_one(tmp_path: Path) -> None:
    store, authority = _authority(tmp_path)
    _output_grant(authority)
    coordinator = ArtifactResultCoordinator(
        authority,
        store,
        coordinator_node_id="coordinator",
    )
    publication = _publication()
    first = coordinator.publish_result(
        publication,
        _placement(max_artifacts=1),
        authenticated_session_id=SESSION,
        authenticated_worker_node_id="worker-a",
        provider_id="provider-a",
        command_id="commit-artifact-result",
        now=NOW + timedelta(seconds=5),
    )
    replay = coordinator.publish_result(
        publication,
        _placement(max_artifacts=1),
        authenticated_session_id=SESSION,
        authenticated_worker_node_id="worker-a",
        provider_id="provider-a",
        command_id="commit-artifact-result",
        now=NOW + timedelta(seconds=6),
    )

    assert first.changed
    assert not replay.changed
    assert replay.artifact_publication.publication == publication


def test_f76_malformed_input_reference_fails_closed(tmp_path: Path) -> None:
    _, authority = _authority(tmp_path)

    with pytest.raises(FederationValidationError) as invalid:
        authority.issue_grant(
            JOB_ID,
            coordinator_node_id="coordinator",
            grant_id="grant-malformed-input",
            worker_node_id="worker-a",
            endpoint_id=ENDPOINT,
            scopes=(ArtifactGrantScope.READ_INPUT,),
            input_references=(object(),),  # type: ignore[arg-type]
            expires_at=NOW + timedelta(seconds=50),
            now=NOW + timedelta(seconds=3),
        )
    assert invalid.value.code == "invalid-input-reference"
