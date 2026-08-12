from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation.session_leadership import (
    LEADER_CHANGED_EVENT,
    LEADERSHIP_SCHEMA,
)
from catalog.federation.software_update import UpdateInspection
from catalog.flask_app.services import federation_active_leader_runtime as active
from catalog.flask_app.services import federation_update_events as events

SESSION = "session-one"
CREATOR = "node-creator"
SUCCESSOR = "node-successor"
COORDINATOR = "fcp-relay-coordinator"
TARGET = "a" * 40


def _created(revision: int = 1):
    return SimpleNamespace(
        revision=revision,
        session_id=SESSION,
        event_type="session.created",
        actor_node_id=CREATOR,
        payload={"session_id": SESSION},
    )


def _leader_changed(revision: int = 2, *, actor: str = COORDINATOR):
    return SimpleNamespace(
        revision=revision,
        session_id=SESSION,
        event_type=LEADER_CHANGED_EVENT,
        actor_node_id=actor,
        payload={
            "schema": LEADERSHIP_SCHEMA,
            "session_id": SESSION,
            "previous_leader_node_id": CREATOR,
            "leader_node_id": SUCCESSOR,
            "term": 2,
            "reason": "leader-offline-timeout",
        },
    )


def _context(actor: str, replay):
    session = SimpleNamespace(created_by_node_id=CREATOR)
    coordinator = SimpleNamespace(
        coordinator_id=COORDINATOR,
        store=SimpleNamespace(get_session=lambda _session_id: session),
        replay_page=lambda **_kwargs: (replay, replay[-1].revision if replay else 0),
    )
    return SimpleNamespace(
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id=actor)),
        binding=SimpleNamespace(internal_session_id=SESSION),
        coordinator=coordinator,
    )


def test_update_service_accepts_successor_and_fences_former_creator(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    replay = (_created(), _leader_changed())
    successor = _context(SUCCESSOR, replay)
    monkeypatch.setattr(
        active.update_service,
        "get_capability_onboarding_service",
        lambda: SimpleNamespace(authorized_context=lambda: successor),
    )
    service = active.ActiveLeaderFederationUpdateService(
        SimpleNamespace(), tmp_path / "update.json"
    )

    context, actor = service._context()
    assert context is successor
    assert actor == SUCCESSOR

    creator = _context(CREATOR, replay)
    monkeypatch.setattr(
        active.update_service,
        "get_capability_onboarding_service",
        lambda: SimpleNamespace(authorized_context=lambda: creator),
    )
    with pytest.raises(PermissionError, match="federation_leader_required"):
        service._context()


def test_successor_update_intent_uses_authenticated_remote_event_append(
    tmp_path: Path,
) -> None:
    replay = (_created(), _leader_changed())
    context = _context(SUCCESSOR, replay)
    recorded: list[dict[str, object]] = []

    class Runtime:
        def append_session_event(self, remote, **kwargs):
            recorded.append({"remote": remote, **kwargs})

    # RemoteCoordinatorFacade exposes runtime/state but intentionally no local
    # append_event authority.
    context.coordinator = SimpleNamespace(
        coordinator_id=COORDINATOR,
        store=context.coordinator.store,
        replay_page=context.coordinator.replay_page,
        runtime=Runtime(),
        state="saved-remote",
    )
    service = active.ActiveLeaderFederationUpdateService(
        SimpleNamespace(), tmp_path / "update.json"
    )
    now = datetime.now(timezone.utc)

    service._append_request_event(
        context,
        SUCCESSOR,
        event_type=events.CHECK_REQUEST_EVENT,
        request_id="successor-check",
        target=TARGET,
        target_node_ids=(CREATOR,),
        now=now,
    )

    assert len(recorded) == 1
    item = recorded[0]
    assert item["remote"] == "saved-remote"
    assert item["session_id"] == SESSION
    assert item["event_type"] == events.CHECK_REQUEST_EVENT
    assert item["request_id"] == f"{events.CHECK_REQUEST_EVENT}-successor-check"
    payload = item["payload"]
    assert isinstance(payload, dict)
    assert payload["target_node_ids"] == [CREATOR]
    assert payload["target_commit"] == TARGET


def test_update_processor_follows_coordinator_leader_term_and_ignores_old_leader(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    now = datetime.now(timezone.utc)
    old_payload = events.command_payload(
        request_id="old-leader-check",
        target_commit=TARGET,
        target_node_ids=("node-target",),
        created_at=now,
        expires_at=now + timedelta(minutes=2),
    )
    new_payload = events.command_payload(
        request_id="new-leader-check",
        target_commit=TARGET,
        target_node_ids=("node-target",),
        created_at=now,
        expires_at=now + timedelta(minutes=2),
    )
    replay = (
        _created(),
        _leader_changed(),
        SimpleNamespace(
            revision=3,
            session_id=SESSION,
            event_type=events.CHECK_REQUEST_EVENT,
            actor_node_id=CREATOR,
            payload=old_payload,
        ),
        SimpleNamespace(
            revision=4,
            session_id=SESSION,
            event_type=events.CHECK_REQUEST_EVENT,
            actor_node_id=SUCCESSOR,
            payload=new_payload,
        ),
    )

    class Handoff:
        def __init__(self) -> None:
            self.inspect_calls: list[str] = []

        def latest_result(self):
            return None

        def inspect(self, *, target: str, fetch: bool):
            assert fetch is True
            self.inspect_calls.append(target)
            return UpdateInspection(
                "up_to_date",
                TARGET,
                TARGET,
                running_commit=TARGET,
            )

    handoff = Handoff()
    reports: list[dict[str, object]] = []

    def append_report(
        _service,
        _context,
        _event_type: str,
        payload: dict[str, object],
        _request_id: str,
    ) -> None:
        reports.append(payload)

    monkeypatch.setattr(events, "_append_remote_event", append_report)
    coordinator = SimpleNamespace(
        coordinator_id=COORDINATOR,
        replay_page=lambda **_kwargs: (replay, 4),
    )
    context = SimpleNamespace(
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id="node-target")),
        binding=SimpleNamespace(internal_session_id=SESSION),
        coordinator=coordinator,
    )
    service = SimpleNamespace(remote_store=SimpleNamespace(load=lambda: object()))
    state_file = tmp_path / "processor.json"
    processor = active.ActiveLeaderFederationUpdateEventProcessor(
        service, handoff, state_file
    )

    processor.process(context)

    state = events._read_state(state_file)
    assert state["authority_node_id"] == SUCCESSOR
    assert state["authority_term"] == 2
    assert state["coordinator_id"] == COORDINATOR
    assert state["last_revision"] == 4
    assert handoff.inspect_calls == [TARGET]
    assert [report["request_id"] for report in reports] == ["new-leader-check"]


def test_member_spoof_cannot_advance_processor_leader_term() -> None:
    state: dict[str, object] = {
        "authority_node_id": CREATOR,
        "authority_term": 1,
        "coordinator_id": COORDINATOR,
    }

    authority = active._pin_leadership_event(
        state,
        _leader_changed(actor="node-attacker"),
    )

    assert authority == CREATOR
    assert state["authority_node_id"] == CREATOR
    assert state["authority_term"] == 1
