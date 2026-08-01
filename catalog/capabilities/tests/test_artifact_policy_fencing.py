from __future__ import annotations

from datetime import timedelta
from pathlib import Path

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


def _grant_output(authority) -> None:
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
        placement_policy=_placement(),
        expires_at=NOW + timedelta(seconds=50),
        now=NOW + timedelta(seconds=3),
    )


def test_f76_worker_cannot_widen_issued_placement_policy(tmp_path: Path) -> None:
    _, authority = _authority(tmp_path)
    _grant_output(authority)
    widened = _placement(
        allowed_schema_ids=("msh.output.v1", "msh.private.v1"),
        max_artifact_bytes=8192,
        max_artifacts=10,
        resumable_threshold_bytes=2048,
    )

    with pytest.raises(FederationValidationError) as rejected:
        authority.authorize_output(
            "grant-output-1",
            _output_descriptor(),
            widened,
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-a",
            provider_id="provider-a",
            now=NOW + timedelta(seconds=4),
        )
    assert rejected.value.code == "placement-policy-widening"


def test_f76_worker_may_narrow_issued_placement_policy(tmp_path: Path) -> None:
    _, authority = _authority(tmp_path)
    _grant_output(authority)
    narrowed = _placement(max_artifact_bytes=1024, max_artifacts=1)

    authority.authorize_output(
        "grant-output-1",
        _output_descriptor(),
        narrowed,
        authenticated_session_id=SESSION,
        authenticated_worker_node_id="worker-a",
        provider_id="provider-a",
        now=NOW + timedelta(seconds=4),
    )


def test_f76_publication_at_lease_expiry_is_stale(tmp_path: Path) -> None:
    store, authority = _authority(tmp_path)
    _grant_output(authority)
    coordinator = ArtifactResultCoordinator(
        authority,
        store,
        coordinator_node_id="coordinator",
    )
    publication = _publication(published_at=NOW + timedelta(seconds=60))

    with pytest.raises(FederationValidationError) as stale:
        coordinator.publish_result(
            publication,
            _placement(expires_at=NOW + timedelta(seconds=60)),
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-a",
            provider_id="provider-a",
            command_id="commit-at-expiry",
            now=NOW + timedelta(seconds=60),
        )
    assert stale.value.code == "artifact-publication-after-lease"


def test_f76_future_publication_timestamp_is_rejected(tmp_path: Path) -> None:
    store, authority = _authority(tmp_path)
    _grant_output(authority)
    coordinator = ArtifactResultCoordinator(
        authority,
        store,
        coordinator_node_id="coordinator",
    )
    publication = _publication(published_at=NOW + timedelta(seconds=6))

    with pytest.raises(FederationValidationError) as future:
        coordinator.publish_result(
            publication,
            _placement(),
            authenticated_session_id=SESSION,
            authenticated_worker_node_id="worker-a",
            provider_id="provider-a",
            command_id="commit-future",
            now=NOW + timedelta(seconds=5),
        )
    assert future.value.code == "artifact-publication-in-future"
