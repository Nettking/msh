from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.capabilities.analysis.contracts import analysis_provider_attributes
from catalog.capabilities.provider_enrollment import (
    FederatedProviderEnrollmentService,
    ProviderEnrollmentState,
    SQLiteProviderEnrollmentStore,
)
from catalog.capabilities.provider_health import (
    FederatedProviderHealthService,
    SQLiteProviderHealthStore,
)
from catalog.capabilities.registered_compute_provider import RegisteredComputeProviderRuntime
from catalog.capabilities.worker_activation import LocalComputeHandlerDescriptor
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import FederationValidationError
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
    ContributionIntent,
    ContributionPolicyState,
)
from catalog.node.identity import IdentityStore

NOW = datetime(2026, 9, 1, 8, 30, tzinfo=timezone.utc)
SESSION_ID = "session-compute-decision-order"
HANDLER_ID = "ordered-analysis"


def _candidate(node_id: str) -> ContributionCandidate:
    descriptor = LocalComputeHandlerDescriptor(
        handler_id=HANDLER_ID,
        capability_type="background-analysis",
        protocol="fcp-background-analysis",
        protocol_version="1.0",
        attributes=analysis_provider_attributes(handler_revision=1),
    )
    return ContributionCandidate(
        candidate_id="candidate-compute-decision-order",
        device_id=node_id,
        capability_type="compute",
        capability_protocol=descriptor.protocol,
        display_label="Ordered analysis",
        inspection_revision=1,
        benchmark_run_ids=(),
        capacity_envelope={
            "kind": "registered-compute-handler",
            "handler_id": descriptor.handler_id,
            "handler_capability_type": descriptor.capability_type,
            "protocol_version": descriptor.protocol_version,
            "descriptor_fingerprint": descriptor.descriptor_fingerprint,
            "handler_attributes": descriptor.attributes,
        },
        missing_prerequisites=(),
        policy_state=ContributionPolicyState.ALLOWED,
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=15),
    )


def _intent(node_id: str, *, active: bool, revision: int) -> ContributionIntent:
    return ContributionIntent(
        candidate_id="candidate-compute-decision-order",
        device_id=node_id,
        desired_state=(
            ContributionDesiredState.ENABLED
            if active
            else ContributionDesiredState.DISABLED
        ),
        decision_revision=revision,
        policy_state=ContributionPolicyState.ALLOWED,
        activation_state=(
            ContributionActivationState.ACTIVE
            if active
            else ContributionActivationState.INACTIVE
        ),
        reason=None,
        decided_at=NOW + timedelta(seconds=revision),
    )


def _runtime(tmp_path: Path):
    coordinator = SessionCoordinator(tmp_path / "coordinator.sqlite3", clock=lambda: NOW)
    owner = IdentityStore(tmp_path / "owner", display_name="Owner").create(now=NOW)
    provider = IdentityStore(tmp_path / "provider", display_name="Provider").create(now=NOW)
    for credentials in (owner, provider):
        token = coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
        coordinator.enroll_node(credentials.identity, token=token)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="Decision ordering",
        request_id="create-session",
        session_id=SESSION_ID,
    )
    invitation = coordinator.create_invitation(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        request_id="invite-provider",
    )
    coordinator.join_session(
        node_id=provider.identity.node_id,
        token=invitation["token"],
        request_id="join-provider",
        expected_session_id=SESSION_ID,
    )
    enrollments = FederatedProviderEnrollmentService(
        coordinator,
        SQLiteProviderEnrollmentStore(tmp_path / "enrollment.sqlite3"),
        clock=lambda: NOW,
    )
    health = FederatedProviderHealthService(
        enrollments,
        SQLiteProviderHealthStore(tmp_path / "health.sqlite3"),
        clock=lambda: NOW,
    )
    return (
        coordinator,
        owner,
        provider,
        enrollments,
        RegisteredComputeProviderRuntime(
            enrollments=enrollments,
            health=health,
            clock=lambda: NOW,
        ),
    )


def test_delayed_older_active_decision_cannot_resurrect_disabled_provider(
    tmp_path: Path,
) -> None:
    coordinator, owner, provider, enrollments, runtime = _runtime(tmp_path)
    node_id = provider.identity.node_id
    candidate = _candidate(node_id)

    binding, _ready, requested = runtime.reconcile_contribution(
        candidate,
        _intent(node_id, active=True, revision=1),
        session_id=SESSION_ID,
        node_id=node_id,
    )
    assert requested is not None
    approved = enrollments.approve(
        session_id=SESSION_ID,
        capability_id=binding.capability_id,
        actor_node_id=owner.identity.node_id,
        command_id="approve-provider",
        expected_revision=requested.revision,
    )
    assert approved.state is ProviderEnrollmentState.APPROVED

    _binding, disabled, disabled_enrollment = runtime.reconcile_contribution(
        candidate,
        _intent(node_id, active=False, revision=4),
        session_id=SESSION_ID,
        node_id=node_id,
    )
    assert disabled.status.value == "disabled"
    assert disabled.properties["decision_revision"] == 4
    assert disabled_enrollment is None

    with pytest.raises(FederationValidationError) as stale:
        runtime.reconcile_contribution(
            candidate,
            _intent(node_id, active=True, revision=3),
            session_id=SESSION_ID,
            node_id=node_id,
        )
    assert stale.value.code == "stale-registered-compute-decision"

    current = {
        item.capability_id: item
        for item in coordinator.store.list_capabilities(session_id=SESSION_ID)
    }[binding.capability_id]
    assert current.status.value == "disabled"
    assert current.properties["decision_revision"] == 4
    preserved = enrollments.store.get(
        session_id=SESSION_ID,
        capability_id=binding.capability_id,
    )
    assert preserved is not None
    assert preserved.state is ProviderEnrollmentState.APPROVED
