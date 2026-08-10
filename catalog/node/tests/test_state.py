from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.federation.models import (
    CapabilityAnnouncement,
    CapabilityStatus,
    SessionEvent,
)
from catalog.node.state import (
    MAX_EVENT_BYTES,
    ConnectionState,
    EnrollmentState,
    EventApplyStatus,
    NodeState,
    NodeStateError,
    ReconnectPolicy,
    SessionMembershipState,
)

NOW = datetime(2026, 7, 28, 12, tzinfo=timezone.utc)


def event(revision: int, *, event_id: str | None = None, payload=None) -> SessionEvent:
    return SessionEvent(
        session_id="session-a",
        revision=revision,
        event_id=event_id or f"event-{revision}",
        event_type="test.message",
        occurred_at=NOW,
        actor_node_id="node-a",
        payload=payload or {"revision": revision},
    )


def enrolled_state(tmp_path: Path) -> NodeState:
    state = NodeState(tmp_path / "node.db", node_id="node-a", now=NOW)
    state.set_enrollment_state(EnrollmentState.ENROLLED, now=NOW)
    state.set_connection_state(ConnectionState.CONNECTED, now=NOW)
    state.join_session("session-a", now=NOW)
    return state


def test_state_survives_restart_with_membership_and_status(tmp_path: Path) -> None:
    path = tmp_path / "node.db"
    state = NodeState(path, node_id="node-a", now=NOW)
    state.set_enrollment_state(EnrollmentState.ENROLLED, now=NOW)
    state.set_connection_state(ConnectionState.CONNECTED, now=NOW)
    state.record_heartbeat(now=NOW + timedelta(seconds=5))
    state.join_session("session-b", now=NOW)
    state.join_session("session-a", now=NOW)
    state.apply_event(event(1), now=NOW)

    restarted = NodeState(path, node_id="node-a", now=NOW)
    status = restarted.status()

    assert status["enrollment_state"] == "enrolled"
    assert status["connection_state"] == "connected"
    assert status["last_heartbeat"] == "2026-07-28T12:00:05Z"
    assert [item["session_id"] for item in status["joined_sessions"]] == [
        "session-a",
        "session-b",
    ]
    assert restarted.last_applied_revision("session-a") == 1


def test_event_is_applied_once_and_duplicate_is_a_noop(tmp_path: Path) -> None:
    state = enrolled_state(tmp_path)
    first = state.apply_event(event(1), now=NOW)
    duplicate = state.apply_event(event(1), now=NOW + timedelta(seconds=1))

    assert first.status is EventApplyStatus.APPLIED
    assert duplicate.status is EventApplyStatus.DUPLICATE
    assert duplicate.last_applied_revision == 1
    assert [item.revision for item in state.applied_events("session-a")] == [1]


def test_revision_gap_marks_replaying_without_advancing_then_recovers_in_order(
    tmp_path: Path,
) -> None:
    state = enrolled_state(tmp_path)
    state.apply_event(event(1), now=NOW)

    gap = state.apply_event(event(3), now=NOW)

    assert gap.status is EventApplyStatus.GAP
    assert gap.expected_revision == 2
    assert state.last_applied_revision("session-a") == 1
    assert state.get_session("session-a").replaying
    assert state.status()["connection_state"] == "replaying"
    assert [item.revision for item in state.applied_events("session-a")] == [1]

    state.apply_event(event(2), now=NOW)
    assert state.get_session("session-a").replaying
    state.apply_event(event(3), now=NOW)

    assert not state.get_session("session-a").replaying
    assert state.status()["connection_state"] == "connected"
    assert [item.revision for item in state.applied_events("session-a")] == [
        1,
        2,
        3,
    ]


def test_event_insert_and_revision_advance_survive_restart_atomically(
    tmp_path: Path,
) -> None:
    path = tmp_path / "node.db"
    state = enrolled_state(tmp_path)
    state.apply_event(event(1), now=NOW)

    restarted = NodeState(path, node_id="node-a", now=NOW)

    assert restarted.last_applied_revision("session-a") == 1
    assert restarted.applied_events("session-a") == (event(1),)


def test_event_and_revision_roll_back_together_on_transaction_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "node.db"
    state = enrolled_state(tmp_path)
    original_transaction = state._transaction

    class FailingConnection:
        def __init__(self, database: sqlite3.Connection) -> None:
            self.database = database

        def execute(self, statement: str, parameters=()):
            normalized = " ".join(statement.split())
            if (
                normalized.startswith("UPDATE joined_sessions")
                and "SET last_applied_revision=" in normalized
            ):
                raise sqlite3.OperationalError(
                    "injected failure before revision advancement"
                )
            return self.database.execute(statement, parameters)

    @contextmanager
    def failing_transaction() -> Iterator[FailingConnection]:
        with original_transaction() as database:
            yield FailingConnection(database)

    monkeypatch.setattr(state, "_transaction", failing_transaction)

    with pytest.raises(
        sqlite3.OperationalError,
        match="injected failure before revision advancement",
    ):
        state.apply_event(event(1), now=NOW)

    restarted = NodeState(path, node_id="node-a", now=NOW)
    assert restarted.last_applied_revision("session-a") == 0
    assert restarted.applied_events("session-a") == ()


def test_conflicting_event_identity_and_revision_are_rejected(tmp_path: Path) -> None:
    state = enrolled_state(tmp_path)
    state.apply_event(event(1), now=NOW)

    with pytest.raises(NodeStateError) as identity_conflict:
        state.apply_event(event(1, payload={"changed": True}), now=NOW)
    assert identity_conflict.value.code == "event-id-conflict"

    with pytest.raises(NodeStateError) as revision_conflict:
        state.apply_event(event(1, event_id="different-event"), now=NOW)
    assert revision_conflict.value.code == "event-revision-conflict"
    assert state.last_applied_revision("session-a") == 1


def test_unjoined_or_removed_session_events_are_rejected(tmp_path: Path) -> None:
    state = NodeState(tmp_path / "node.db", node_id="node-a", now=NOW)
    with pytest.raises(NodeStateError) as unjoined:
        state.apply_event(event(1), now=NOW)
    assert unjoined.value.code == "session-not-joined"

    state.join_session("session-a", now=NOW)
    removed = state.remove_session("session-a", now=NOW)
    assert removed.membership_state is SessionMembershipState.REMOVED
    with pytest.raises(NodeStateError) as rejected:
        state.apply_event(event(1), now=NOW)
    assert rejected.value.code == "session-not-joined"


def test_replaced_identity_cannot_open_existing_node_state(tmp_path: Path) -> None:
    path = tmp_path / "node.db"
    NodeState(path, node_id="node-a", now=NOW)

    with pytest.raises(NodeStateError) as raised:
        NodeState(path, node_id="node-b", now=NOW)

    assert raised.value.code == "node-state-identity-mismatch"


def test_revoked_state_blocks_accepted_connection_and_heartbeat(tmp_path: Path) -> None:
    state = NodeState(tmp_path / "node.db", node_id="node-a", now=NOW)
    state.set_enrollment_state(EnrollmentState.REVOKED, now=NOW)

    with pytest.raises(NodeStateError, match="node-revoked"):
        state.set_connection_state(ConnectionState.CONNECTED, now=NOW)
    with pytest.raises(NodeStateError, match="node-revoked"):
        state.record_heartbeat(now=NOW)
    assert state.status()["connection_state"] == "revoked"


def test_status_is_deterministic_and_contains_no_events_paths_or_secrets(
    tmp_path: Path,
) -> None:
    state = enrolled_state(tmp_path)
    secret = "enrollment-token-that-must-not-leak"
    state.apply_event(event(1, payload={"private": secret}), now=NOW)

    first = state.status()
    second = state.status()
    encoded = json.dumps(first, sort_keys=True)

    assert first == second
    assert secret not in encoded
    assert str(tmp_path) not in encoded
    assert "event-1" not in encoded
    assert set(first) == {
        "schema",
        "node_id",
        "enrollment_state",
        "connection_state",
        "last_heartbeat",
        "last_error_code",
        "joined_sessions",
        "locally_advertised_capabilities",
    }


def test_capabilities_and_replay_completion_are_durable(tmp_path: Path) -> None:
    database = tmp_path / "node.db"
    state = enrolled_state(tmp_path)
    first = CapabilityAnnouncement(
        capability_id="storage-a",
        node_id="node-a",
        session_id="session-a",
        type="storage",
        protocol="fcp-storage-test",
        protocol_version="1.0",
        status=CapabilityStatus.READY,
        properties={"label": "local filesystem"},
        announced_at=NOW,
    )
    second = CapabilityAnnouncement(
        capability_id="storage-b",
        node_id="node-a",
        session_id="session-a",
        type="storage",
        protocol="fcp-storage-test",
        protocol_version="1.0",
        status=CapabilityStatus.READY,
        properties={"label": "local archive"},
        announced_at=NOW,
    )
    state.save_capability(second, now=NOW)
    state.save_capability(first, now=NOW)
    state.mark_session_replaying(
        "session-a", now=NOW, target_revision=1
    )
    state.apply_event(event(1), now=NOW)
    completed = state.complete_replay(
        "session-a", final_revision=1, now=NOW
    )

    assert completed.replaying is False
    assert state.advertised_capabilities() == (first, second)
    assert [
        item["capability_id"]
        for item in state.status()["locally_advertised_capabilities"]
    ] == ["storage-a", "storage-b"]

    restarted = NodeState(database, node_id="node-a", now=NOW)
    assert restarted.advertised_capabilities() == (first, second)
    assert restarted.last_applied_revision("session-a") == 1
    assert restarted.remove_capability(
        session_id="session-a", capability_id="storage-a"
    )
    assert restarted.advertised_capabilities() == (second,)


def test_replay_completion_keeps_gap_state_until_all_events_are_durable(
    tmp_path: Path,
) -> None:
    state = enrolled_state(tmp_path)
    state.mark_session_replaying(
        "session-a", now=NOW, target_revision=2
    )
    state.apply_event(event(1), now=NOW)

    with pytest.raises(NodeStateError) as gap:
        state.complete_replay("session-a", final_revision=2, now=NOW)

    assert gap.value.code == "revision-gap"
    assert state.get_session("session-a").replaying is True
    assert state.last_applied_revision("session-a") == 1


def test_replay_completion_preserves_a_newer_live_gap_target(
    tmp_path: Path,
) -> None:
    state = enrolled_state(tmp_path)
    state.mark_session_replaying(
        "session-a", now=NOW, target_revision=4
    )
    for revision in (1, 2, 3):
        state.apply_event(event(revision), now=NOW)

    incomplete = state.complete_replay(
        "session-a",
        final_revision=3,
        now=NOW,
    )

    assert incomplete.replaying is True
    assert incomplete.replay_target_revision == 4
    completed = state.apply_event(event(4), now=NOW)
    assert completed.status is EventApplyStatus.APPLIED
    assert state.get_session("session-a").replaying is False
    assert state.last_applied_revision("session-a") == 4


def test_malformed_and_oversized_events_are_rejected_without_advancing(
    tmp_path: Path,
) -> None:
    state = enrolled_state(tmp_path)
    with pytest.raises(NodeStateError) as malformed:
        state.apply_event(event(1, payload={"bad": object()}), now=NOW)
    assert malformed.value.code == "invalid-event-payload"

    with pytest.raises(NodeStateError) as oversized:
        state.apply_event(
            event(1, payload={"value": "x" * (MAX_EVENT_BYTES + 1)}),
            now=NOW,
        )
    assert oversized.value.code == "event-too-large"
    assert state.last_applied_revision("session-a") == 0


def test_reconnect_policy_is_finite_and_capped_without_sleeping() -> None:
    policy = ReconnectPolicy(
        initial_delay_seconds=1,
        multiplier=3,
        max_delay_seconds=10,
        max_attempts=5,
    )

    assert policy.delays() == (1, 3, 9, 10, 10)
    with pytest.raises(NodeStateError, match="out-of-range"):
        policy.delay_for_attempt(5)
    for invalid in (
        {"initial_delay_seconds": float("nan")},
        {"multiplier": float("inf")},
        {"max_delay_seconds": 3_601},
        {"max_attempts": 101},
    ):
        with pytest.raises(NodeStateError) as rejected:
            ReconnectPolicy(**invalid)
        assert rejected.value.code == "invalid-reconnect-policy"


def test_unsupported_node_state_schema_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "node.db"
    NodeState(path, node_id="node-a", now=NOW)
    with sqlite3.connect(path) as database:
        database.execute("UPDATE node_state_schema SET version=99")

    with pytest.raises(NodeStateError) as raised:
        NodeState(path, node_id="node-a", now=NOW)

    assert raised.value.code == "unsupported-node-state-schema"
