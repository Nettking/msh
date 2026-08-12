from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import AuthorizationError
from catalog.federation.session_leadership import (
    LEADER_CHANGED_EVENT,
    LEADERSHIP_SCHEMA,
)
from catalog.node.identity import IdentityStore

NOW = datetime(2026, 8, 12, 13, 30, tzinfo=timezone.utc)
FAILOVER_TIMEOUT_SECONDS = 300


def _enroll(coordinator: SessionCoordinator, root: Path, name: str):
    credentials = IdentityStore(root / name, display_name=name).load_or_create(now=NOW)
    token = coordinator.create_enrollment_token(ttl_seconds=300, max_uses=1)
    coordinator.enroll_node(credentials.identity, token=str(token["token"]))
    return credentials


def _two_member_session(tmp_path: Path):
    coordinator = SessionCoordinator(tmp_path / "control.sqlite3", clock=lambda: NOW)
    creator = _enroll(coordinator, tmp_path, "creator")
    member = _enroll(coordinator, tmp_path, "member")
    session = coordinator.create_session(
        actor_node_id=creator.identity.node_id,
        display_name="Failover Federation",
        request_id="create-federation",
    )
    coordinator.connected(
        node_id=creator.identity.node_id,
        connection_id="creator-connection",
    )
    invitation = coordinator.create_invitation(
        session_id=session.session_id,
        actor_node_id=creator.identity.node_id,
        ttl_seconds=300,
        max_uses=1,
        request_id="invite-member",
    )
    coordinator.join_session(
        node_id=member.identity.node_id,
        token=str(invitation["token"]),
        request_id="join-member",
        expected_session_id=session.session_id,
    )
    coordinator.connected(
        node_id=member.identity.node_id,
        connection_id="member-connection",
    )
    return coordinator, session, creator, member


def _expire_offline_leader(coordinator: SessionCoordinator, member_node_id: str):
    future = NOW + timedelta(seconds=FAILOVER_TIMEOUT_SECONDS + 1)
    coordinator._clock = lambda: future
    # The surviving candidate is still healthy at the election boundary.
    coordinator.heartbeat(member_node_id)
    return coordinator.sweep_stale(
        heartbeat_timeout_seconds=FAILOVER_TIMEOUT_SECONDS
    )


def test_creator_is_initial_leader_and_offline_creator_fails_over_after_timeout(
    tmp_path: Path,
) -> None:
    coordinator, session, creator, member = _two_member_session(tmp_path)

    initial = coordinator.session_leadership(session.session_id)
    assert initial.creator_node_id == creator.identity.node_id
    assert initial.leader_node_id == creator.identity.node_id
    assert initial.term == 1
    assert initial.leader_connected is True

    # A clean/transient disconnect is not an immediate authority election.
    assert coordinator.disconnected(node_id=creator.identity.node_id) == ()
    during_grace = coordinator.session_leadership(session.session_id)
    assert during_grace.leader_node_id == creator.identity.node_id
    assert during_grace.term == 1
    assert during_grace.leader_connected is False

    _stale, events = _expire_offline_leader(coordinator, member.identity.node_id)
    changed = [event for event in events if event.event_type == LEADER_CHANGED_EVENT]
    assert len(changed) == 1
    assert changed[0].actor_node_id == coordinator.coordinator_id
    assert changed[0].payload == {
        "schema": LEADERSHIP_SCHEMA,
        "session_id": session.session_id,
        "previous_leader_node_id": creator.identity.node_id,
        "leader_node_id": member.identity.node_id,
        "term": 2,
        "reason": "leader-offline-timeout",
    }

    current = coordinator.session_leadership(session.session_id)
    assert current.creator_node_id == creator.identity.node_id
    assert current.leader_node_id == member.identity.node_id
    assert current.term == 2
    assert current.leader_connected is True


def test_former_leader_reconnect_does_not_reclaim_authority(tmp_path: Path) -> None:
    coordinator, session, creator, member = _two_member_session(tmp_path)
    coordinator.disconnected(node_id=creator.identity.node_id)
    _expire_offline_leader(coordinator, member.identity.node_id)

    coordinator.connected(
        node_id=creator.identity.node_id,
        connection_id="creator-reconnected",
    )
    current = coordinator.session_leadership(session.session_id)
    assert current.leader_node_id == member.identity.node_id
    assert current.term == 2

    with pytest.raises(AuthorizationError, match="current Federation leader"):
        coordinator.create_invitation(
            session_id=session.session_id,
            actor_node_id=creator.identity.node_id,
            request_id="stale-leader-invite",
        )

    invitation = coordinator.create_invitation(
        session_id=session.session_id,
        actor_node_id=member.identity.node_id,
        request_id="new-leader-invite",
    )
    assert invitation["session_id"] == session.session_id
    assert str(invitation["token"]).startswith("fcp_join_")


def test_active_leader_must_transfer_before_self_removal(tmp_path: Path) -> None:
    coordinator, session, creator, member = _two_member_session(tmp_path)
    coordinator.disconnected(node_id=creator.identity.node_id)
    _expire_offline_leader(coordinator, member.identity.node_id)

    with pytest.raises(AuthorizationError) as error:
        coordinator.remove_member(
            session_id=session.session_id,
            actor_node_id=member.identity.node_id,
            target_node_id=member.identity.node_id,
            request_id="leader-leave",
            reason="leave",
        )
    assert error.value.code == "leader-self-removal-requires-transfer"

    coordinator.connected(
        node_id=creator.identity.node_id,
        connection_id="creator-reconnected",
    )
    transferred, event = coordinator.transfer_session_leader(
        session_id=session.session_id,
        actor_node_id=member.identity.node_id,
        target_node_id=creator.identity.node_id,
        request_id="handover-back",
    )
    assert event is not None
    assert transferred.leader_node_id == creator.identity.node_id
    assert transferred.term == 3

    assert coordinator.remove_member(
        session_id=session.session_id,
        actor_node_id=member.identity.node_id,
        target_node_id=member.identity.node_id,
        request_id="member-leave-after-transfer",
        reason="leave",
    ) is True


def test_member_cannot_spoof_coordinator_leadership_event(tmp_path: Path) -> None:
    coordinator, session, creator, member = _two_member_session(tmp_path)
    coordinator.append_event(
        session_id=session.session_id,
        actor_node_id=member.identity.node_id,
        request_id="spoof-leader",
        event_type=LEADER_CHANGED_EVENT,
        payload={
            "schema": LEADERSHIP_SCHEMA,
            "session_id": session.session_id,
            "previous_leader_node_id": creator.identity.node_id,
            "leader_node_id": member.identity.node_id,
            "term": 999,
            "reason": "spoof",
        },
    )
    current = coordinator.session_leadership(session.session_id)
    assert current.leader_node_id == creator.identity.node_id
    assert current.term == 1


def test_relay_restart_does_not_elect_from_reconnect_order(tmp_path: Path) -> None:
    coordinator, session, creator, member = _two_member_session(tmp_path)
    coordinator.relay_started()

    coordinator.connected(
        node_id=member.identity.node_id,
        connection_id="member-after-restart",
    )
    current = coordinator.session_leadership(session.session_id)
    assert current.leader_node_id == creator.identity.node_id
    assert current.term == 1
    assert current.leader_connected is False

    coordinator.connected(
        node_id=creator.identity.node_id,
        connection_id="creator-after-restart",
    )
    restored = coordinator.session_leadership(session.session_id)
    assert restored.leader_node_id == creator.identity.node_id
    assert restored.term == 1
    assert restored.leader_connected is True


def test_no_connected_successor_does_not_invent_leader(tmp_path: Path) -> None:
    coordinator, session, creator, _member = _two_member_session(tmp_path)
    coordinator.relay_started()

    current = coordinator.session_leadership(session.session_id)
    assert current.leader_node_id == creator.identity.node_id
    assert current.term == 1
    assert current.leader_connected is False
