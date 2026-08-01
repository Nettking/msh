from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from catalog.capabilities import (
    F6ArtifactTransferPlanner,
    SQLiteCapabilityArtifactAuthority,
    validate_logical_artifact_endpoint,
)
from catalog.capabilities.artifact_contracts import ArtifactGrantScope
from catalog.capabilities.tests.test_artifact_authorization import (
    JOB_ID,
    NOW,
    _input_descriptor,
    _output_descriptor,
    _placement,
    _running,
    _store,
)
from catalog.federation.errors import FederationValidationError


@pytest.mark.parametrize(
    "endpoint_id",
    (
        "https://storage.example/artifacts",
        "http://10.0.0.8:9000",
        "10.0.0.8",
        "storage.internal:9000",
        "user@storage-endpoint",
        "../storage-endpoint",
        "/absolute/storage-endpoint",
    ),
)
def test_f76_network_or_credential_endpoint_ids_are_rejected(
    endpoint_id: str,
) -> None:
    with pytest.raises(FederationValidationError) as invalid:
        validate_logical_artifact_endpoint(endpoint_id)
    assert invalid.value.code == "invalid-logical-endpoint-id"


def test_f76_simple_logical_endpoint_ids_are_accepted() -> None:
    assert (
        validate_logical_artifact_endpoint("storage-cluster/artifacts-1")
        == "storage-cluster/artifacts-1"
    )


def test_f76_public_authority_rejects_address_based_artifact_registration(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    _running(store)
    authority = SQLiteCapabilityArtifactAuthority(store)

    with pytest.raises(FederationValidationError) as invalid:
        authority.register_artifact(
            _input_descriptor(endpoint_id="http://10.0.0.8:9000"),
            authority_node_id="storage-authority",
            now=NOW,
        )
    assert invalid.value.code == "invalid-logical-endpoint-id"


def test_f76_public_authority_rejects_address_based_grant(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _running(store)
    authority = SQLiteCapabilityArtifactAuthority(store)

    with pytest.raises(FederationValidationError) as invalid:
        authority.issue_grant(
            JOB_ID,
            coordinator_node_id="coordinator",
            grant_id="grant-address-endpoint",
            worker_node_id="worker-a",
            endpoint_id="storage.internal:9000",
            scopes=(ArtifactGrantScope.WRITE_OUTPUT,),
            placement_policy=_placement(endpoint_id="storage.internal:9000"),
            expires_at=NOW + timedelta(seconds=50),
            now=NOW + timedelta(seconds=3),
        )
    assert invalid.value.code == "invalid-logical-endpoint-id"


def test_f76_f6_transfer_plan_rejects_address_based_endpoint() -> None:
    planner = F6ArtifactTransferPlanner(chunk_size=256)

    with pytest.raises(FederationValidationError) as invalid:
        planner.plan(
            _output_descriptor(endpoint_id="10.0.0.8"),
            _placement(endpoint_id="10.0.0.8"),
            now=NOW + timedelta(seconds=4),
        )
    assert invalid.value.code == "invalid-logical-endpoint-id"
