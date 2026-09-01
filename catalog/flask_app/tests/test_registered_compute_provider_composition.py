from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from catalog.capabilities.analysis.contracts import analysis_provider_attributes
from catalog.capabilities.provider_enrollment import (
    ProviderEnrollmentState,
    SQLiteProviderEnrollmentStore,
)
from catalog.capabilities.worker_activation import LocalComputeHandlerDescriptor
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionDesiredState,
    ContributionIntent,
    ContributionPolicyState,
)
from catalog.flask_app.services.registered_compute_provider_composition import (
    reconcile_registered_compute_provider,
)
from catalog.node.identity import IdentityStore
from catalog.relay.provider_service import provider_authority_paths

NOW = datetime(2026, 9, 1, 8, 45, tzinfo=timezone.utc)
SESSION_ID = "session-production-compute-composition"
HANDLER_ID = "production-safe-handler"


def _candidate(node_id: str) -> ContributionCandidate:
    descriptor = LocalComputeHandlerDescriptor(
        handler_id=HANDLER_ID,
        capability_type="background-analysis",
        protocol="fcp-background-analysis",
        protocol_version="1.0",
        attributes=analysis_provider_attributes(handler_revision=7),
    )
    return ContributionCandidate(
        candidate_id="candidate-production-compute",
        device_id=node_id,
        capability_type="compute",
        capability_protocol=descriptor.protocol,
        display_label="Production compute",
        inspection_revision=7,
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
        candidate_id="candidate-production-compute",
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


def _onboarding(tmp_path: Path):
    coordinator = SessionCoordinator(tmp_path / "coordinator.sqlite3", clock=lambda: NOW)
    credentials = IdentityStore(tmp_path / "node", display_name="Compute node").create(
        now=NOW
    )
    token = coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
    coordinator.enroll_node(credentials.identity, token=token)
    coordinator.create_session(
        actor_node_id=credentials.identity.node_id,
        display_name="Production compute composition",
        request_id="create-session",
        session_id=SESSION_ID,
    )
    context = SimpleNamespace(
        credentials=credentials,
        binding=SimpleNamespace(internal_session_id=SESSION_ID),
        coordinator=coordinator,
    )
    onboarding = SimpleNamespace(
        authorized_context=lambda: context,
        _clock=lambda: NOW,
    )
    return coordinator, credentials, onboarding


def test_production_composition_announces_and_requests_real_f8_enrollment(
    tmp_path: Path,
) -> None:
    coordinator, credentials, onboarding = _onboarding(tmp_path)
    node_id = credentials.identity.node_id
    candidate = _candidate(node_id)
    intent = _intent(node_id, active=True, revision=9)

    reconcile_registered_compute_provider(
        onboarding_service=onboarding,
        candidate=candidate,
        intent=intent,
        clock=lambda: NOW,
    )

    announcements = coordinator.store.list_capabilities(session_id=SESSION_ID)
    registered = [
        item
        for item in announcements
        if item.properties.get("kind") == "registered-compute-handler"
    ]
    assert len(registered) == 1
    announcement = registered[0]
    assert announcement.node_id == node_id
    assert announcement.status.value == "ready"
    assert announcement.properties["decision_revision"] == 9

    enrollment_path, _health_path = provider_authority_paths(coordinator)
    enrollment = SQLiteProviderEnrollmentStore(enrollment_path).get(
        session_id=SESSION_ID,
        capability_id=announcement.capability_id,
    )
    assert enrollment is not None
    assert enrollment.state is ProviderEnrollmentState.PENDING
    assert enrollment.requested_by_node_id == node_id
    assert enrollment.approved_by_node_id is None


def test_never_active_compute_intent_does_not_create_provider_state(
    tmp_path: Path,
) -> None:
    coordinator, credentials, onboarding = _onboarding(tmp_path)
    node_id = credentials.identity.node_id

    reconcile_registered_compute_provider(
        onboarding_service=onboarding,
        candidate=_candidate(node_id),
        intent=_intent(node_id, active=False, revision=1),
        clock=lambda: NOW,
    )

    assert coordinator.store.list_capabilities(session_id=SESSION_ID) == ()
