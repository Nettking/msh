"""Activity must record material capability change, not liveness refreshes.

Physical acceptance showed roughly fifty ``capability.status.changed`` events in
eleven seconds, all meaning "a trusted device refreshed service metadata". That
buries real events and makes the audit trail useless, so an announcement that
carries no material change must not append a session event.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.identity import IdentityStore, NodeCredentials

NOW = datetime(2026, 8, 17, 1, 0, tzinfo=timezone.utc)
SESSION_ID = "session-activity"
CAPABILITY_ID = "candidate-storage"


def _credentials(tmp_path: Path, name: str) -> NodeCredentials:
    return IdentityStore(
        tmp_path / f"identity-{name.casefold()}",
        display_name=name,
    ).create(now=NOW)


def _fixture(tmp_path: Path):
    current = [NOW]
    coordinator = SessionCoordinator(
        tmp_path / "coordinator.sqlite3",
        clock=lambda: current[0],
    )
    owner = _credentials(tmp_path, "owner")
    member = _credentials(tmp_path, "member")
    for node in (owner, member):
        token = coordinator.create_enrollment_token(ttl_seconds=60, max_uses=1)["token"]
        coordinator.enroll_node(node.identity, token=token)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="Activity",
        request_id="create-session",
        session_id=SESSION_ID,
    )
    invitation = coordinator.create_invitation(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        request_id="invite-member",
    )
    coordinator.join_session(
        node_id=member.identity.node_id,
        token=invitation["token"],
        request_id="join-member",
        expected_session_id=SESSION_ID,
    )
    return coordinator, member, current


def _announcement(
    member: NodeCredentials,
    *,
    status: CapabilityStatus = CapabilityStatus.READY,
    label: str = "Local FCP data storage",
    announced_at: datetime = NOW,
) -> CapabilityAnnouncement:
    return CapabilityAnnouncement(
        capability_id=CAPABILITY_ID,
        node_id=member.identity.node_id,
        session_id=SESSION_ID,
        type="storage",
        protocol="fcp-storage-v1",
        protocol_version="1.0",
        status=status,
        properties={"kind": "capability-first-candidate", "label": label},
        announced_at=announced_at,
    )


def _capability_events(coordinator: SessionCoordinator) -> tuple[str, ...]:
    return tuple(
        event.event_type
        for event in coordinator.store.replay_events(
            session_id=SESSION_ID,
            last_applied_revision=0,
        )
        if event.event_type.startswith("capability.")
    )


def test_repeated_identical_refresh_creates_no_new_activity(tmp_path: Path) -> None:
    coordinator, member, current = _fixture(tmp_path)
    coordinator.announce_capability(
        _announcement(member),
        actor_node_id=member.identity.node_id,
        request_id="announce-initial",
    )
    assert _capability_events(coordinator) == ("capability.registered",)

    # Fifty refreshes, each with a distinct request ID and a moving clock, but
    # no material change: exactly what flooded the physical Activity view.
    for index in range(50):
        current[0] = NOW.replace(second=index % 60)
        coordinator.announce_capability(
            _announcement(member, announced_at=current[0]),
            actor_node_id=member.identity.node_id,
            request_id=f"refresh-{index}",
        )

    assert _capability_events(coordinator) == ("capability.registered",)


def test_material_status_change_creates_exactly_one_activity_event(
    tmp_path: Path,
) -> None:
    coordinator, member, _current = _fixture(tmp_path)
    coordinator.announce_capability(
        _announcement(member),
        actor_node_id=member.identity.node_id,
        request_id="announce-initial",
    )
    for index in range(3):
        coordinator.announce_capability(
            _announcement(member),
            actor_node_id=member.identity.node_id,
            request_id=f"refresh-{index}",
        )

    coordinator.announce_capability(
        _announcement(member, status=CapabilityStatus.UNAVAILABLE),
        actor_node_id=member.identity.node_id,
        request_id="announce-unavailable",
    )

    assert _capability_events(coordinator) == (
        "capability.registered",
        "capability.status.changed",
    )


def test_material_property_change_creates_exactly_one_activity_event(
    tmp_path: Path,
) -> None:
    coordinator, member, _current = _fixture(tmp_path)
    coordinator.announce_capability(
        _announcement(member),
        actor_node_id=member.identity.node_id,
        request_id="announce-initial",
    )
    coordinator.announce_capability(
        _announcement(member, label="Renamed storage"),
        actor_node_id=member.identity.node_id,
        request_id="announce-renamed",
    )
    for index in range(3):
        coordinator.announce_capability(
            _announcement(member, label="Renamed storage"),
            actor_node_id=member.identity.node_id,
            request_id=f"refresh-{index}",
        )

    assert _capability_events(coordinator) == (
        "capability.registered",
        "capability.status.changed",
    )


def test_unchanged_refresh_still_records_liveness(tmp_path: Path) -> None:
    """Suppressing the event must not suppress the heartbeat behind it."""

    coordinator, member, current = _fixture(tmp_path)
    coordinator.announce_capability(
        _announcement(member),
        actor_node_id=member.identity.node_id,
        request_id="announce-initial",
    )
    first = coordinator.store.list_capabilities(session_id=SESSION_ID)

    current[0] = NOW.replace(minute=30)
    coordinator.announce_capability(
        _announcement(member),
        actor_node_id=member.identity.node_id,
        request_id="refresh-later",
    )

    latest = coordinator.store.list_capabilities(session_id=SESSION_ID)
    assert len(latest) == len(first) == 1
    assert _capability_events(coordinator) == ("capability.registered",)
