from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.capabilities.provider_auto_enrollment import (
    AUTO_ENROLLMENT_SCHEMA,
    TrustedGreenProviderAutoEnrollment,
    parse_auto_enrollment_evidence,
)
from catalog.capabilities.provider_enrollment import (
    ProviderEnrollmentState,
    SQLiteProviderEnrollmentStore,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.identity import IdentityStore, NodeCredentials

NOW = datetime(2026, 8, 17, 1, 0, tzinfo=timezone.utc)


def _credentials(tmp_path: Path, name: str) -> NodeCredentials:
    return IdentityStore(
        tmp_path / f"identity-{name.casefold()}",
        display_name=name,
    ).create(now=NOW)


def _enroll(coordinator: SessionCoordinator, node: NodeCredentials) -> None:
    token = coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
    coordinator.enroll_node(node.identity, token=token)


def _fixture(tmp_path: Path):
    current = [NOW]
    coordinator = SessionCoordinator(
        tmp_path / "coordinator.sqlite3",
        clock=lambda: current[0],
    )
    owner = _credentials(tmp_path, "owner")
    provider = _credentials(tmp_path, "provider")
    _enroll(coordinator, owner)
    _enroll(coordinator, provider)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="Auto enrollment",
        request_id="create-session",
        session_id="session-auto",
    )
    invitation = coordinator.create_invitation(
        session_id="session-auto",
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        request_id="invite-provider",
    )
    coordinator.join_session(
        node_id=provider.identity.node_id,
        token=invitation["token"],
        request_id="join-provider",
        expected_session_id="session-auto",
    )
    coordinator.connected(
        node_id=owner.identity.node_id,
        connection_id="owner-live",
    )
    coordinator.connected(
        node_id=provider.identity.node_id,
        connection_id="provider-live",
    )
    store = SQLiteProviderEnrollmentStore(tmp_path / "provider-enrollment.sqlite3")
    policy = TrustedGreenProviderAutoEnrollment(
        coordinator,
        store,
        clock=lambda: current[0],
    )
    return coordinator, store, policy, owner, provider, current


def _properties(*, decision_revision: int = 4) -> dict[str, object]:
    return {
        "kind": "capability-first-candidate",
        "candidate_id": "candidate-ai",
        "label": "Language model",
        "auto_enrollment": {
            "schema": AUTO_ENROLLMENT_SCHEMA,
            "eligible": True,
            "benchmark_state": "green",
            "inspection_revision": 3,
            "benchmark_run_ids": ["benchmark-green-1"],
            "decision_revision": decision_revision,
            "desired_state": "enabled",
            "policy_state": "allowed",
        },
    }


def _announcement(
    provider: NodeCredentials,
    *,
    status: CapabilityStatus,
    announced_at: datetime = NOW,
    capability_type: str = "language-model",
    properties: dict[str, object] | None = None,
) -> CapabilityAnnouncement:
    return CapabilityAnnouncement(
        capability_id="candidate-ai",
        node_id=provider.identity.node_id,
        session_id="session-auto",
        type=capability_type,
        protocol="fcp-language-model",
        protocol_version="1.0",
        status=status,
        properties=_properties() if properties is None else properties,
        announced_at=announced_at,
    )


def _publish(
    coordinator: SessionCoordinator,
    announcement: CapabilityAnnouncement,
    request_id: str,
) -> None:
    coordinator.announce_capability(
        announcement,
        actor_node_id=announcement.node_id,
        request_id=request_id,
    )


def test_green_registering_contribution_auto_enrolls_but_cannot_bind(
    tmp_path: Path,
) -> None:
    coordinator, _store, policy, owner, provider, _current = _fixture(tmp_path)
    announcement = _announcement(provider, status=CapabilityStatus.REGISTERING)
    _publish(coordinator, announcement, "announce-registering")

    enrolled = policy.reconcile(announcement)

    assert enrolled is not None
    assert enrolled.state is ProviderEnrollmentState.APPROVED
    assert enrolled.approved_by_node_id == owner.identity.node_id
    assert enrolled.reason_code == "trusted-green-auto-enrollment"
    assert enrolled.announcement_status is CapabilityStatus.REGISTERING
    assert enrolled.eligible_for_resource_binding is False


def test_ready_refresh_is_idempotent_and_becomes_bindable(tmp_path: Path) -> None:
    coordinator, store, policy, _owner, provider, current = _fixture(tmp_path)
    registering = _announcement(provider, status=CapabilityStatus.REGISTERING)
    _publish(coordinator, registering, "announce-registering")
    first = policy.reconcile(registering)
    replay = policy.reconcile(registering)
    assert first is not None and replay is not None
    assert replay == first

    current[0] += timedelta(seconds=1)
    ready = _announcement(
        provider,
        status=CapabilityStatus.READY,
        announced_at=current[0],
    )
    _publish(coordinator, ready, "announce-ready")
    refreshed = policy.reconcile(ready)
    assert refreshed is not None
    assert refreshed.state is ProviderEnrollmentState.APPROVED
    assert refreshed.eligible_for_resource_binding is True

    replay_ready = policy.reconcile(ready)
    assert replay_ready == refreshed
    stored = store.get(session_id="session-auto", capability_id="candidate-ai")
    assert stored == refreshed


def test_missing_or_malformed_attestation_never_creates_enrollment(
    tmp_path: Path,
) -> None:
    coordinator, store, policy, _owner, provider, _current = _fixture(tmp_path)
    ordinary = _announcement(
        provider,
        status=CapabilityStatus.READY,
        properties={
            "kind": "capability-first-candidate",
            "candidate_id": "candidate-ai",
        },
    )
    _publish(coordinator, ordinary, "announce-no-attestation")
    assert policy.reconcile(ordinary) is None
    assert store.get(session_id="session-auto", capability_id="candidate-ai") is None

    malformed_properties = _properties()
    malformed_properties["auto_enrollment"] = {
        **malformed_properties["auto_enrollment"],  # type: ignore[arg-type]
        "benchmark_run_ids": [],
    }
    malformed = _announcement(
        provider,
        status=CapabilityStatus.READY,
        announced_at=NOW + timedelta(seconds=1),
        properties=malformed_properties,
    )
    _publish(coordinator, malformed, "announce-malformed-attestation")
    assert parse_auto_enrollment_evidence(malformed) is None
    assert policy.reconcile(malformed) is None
    assert store.get(session_id="session-auto", capability_id="candidate-ai") is None


def test_storage_is_never_auto_enrolled_even_with_green_attestation(
    tmp_path: Path,
) -> None:
    coordinator, store, policy, _owner, provider, _current = _fixture(tmp_path)
    storage = _announcement(
        provider,
        status=CapabilityStatus.READY,
        capability_type="storage",
    )
    _publish(coordinator, storage, "announce-storage")

    assert parse_auto_enrollment_evidence(storage) is None
    assert policy.reconcile(storage) is None
    assert store.get(session_id="session-auto", capability_id="candidate-ai") is None
