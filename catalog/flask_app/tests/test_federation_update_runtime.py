from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation.software_update import UpdateInspection
from catalog.flask_app.services import federation_update_events as events
from catalog.flask_app.services.federation_update_events import (
    APPLY_REPORT_EVENT,
    CHECK_REQUEST_EVENT,
    EVENT_SCHEMA,
    SESSION_CREATED_EVENT,
    command_payload,
    report_payload,
    validate_command_payload,
)
from catalog.flask_app.services.federation_update_handoff import (
    ACTIVATION_GRACE_SECONDS,
    RESULT_SCHEMA,
    HostUpdateHandoff,
)

TARGET = "a" * 40
NODE = "node-target"
AUTHORITY = "node-authority"
ATTACKER = "node-attacker"


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def test_handoff_check_is_bounded_and_matches_exact_result(tmp_path: Path) -> None:
    handoff = HostUpdateHandoff(tmp_path, timeout=1.0, poll_interval=0.02)
    captured: dict[str, object] = {}

    def write(value: dict[str, object]) -> None:
        captured.update(value)
        handoff.result_file.write_text(
            json.dumps(
                {
                    "schema": RESULT_SCHEMA,
                    "request_id": value["request_id"],
                    "state": "up_to_date",
                    "current_commit": TARGET,
                    "target_commit": TARGET,
                    "running_commit": TARGET,
                    "code": None,
                    "message": "verified",
                }
            ),
            encoding="utf-8",
        )

    handoff.directory.mkdir(parents=True, exist_ok=True)
    handoff._write_request = write  # type: ignore[method-assign]

    result = handoff.inspect(target=TARGET)

    assert captured["action"] == "check"
    assert captured["repository"] == "Nettking/msh"
    assert captured["branch"] == "main"
    assert captured["target_commit"] == TARGET
    assert "activate_after" not in captured
    assert result.state == "up_to_date"
    assert result.running_commit == TARGET


def test_handoff_apply_has_short_activation_grace(tmp_path: Path) -> None:
    handoff = HostUpdateHandoff(tmp_path)

    result = handoff.apply(TARGET, request_id="request-1")
    request = json.loads(handoff.request_file.read_text(encoding="utf-8"))

    created = _parse(request["created_at"])
    activate_after = _parse(request["activate_after"])
    assert request["action"] == "apply"
    assert request["request_id"] == "request-1"
    assert request["target_commit"] == TARGET
    assert 0 < (activate_after - created).total_seconds() <= ACTIVATION_GRACE_SECONDS
    assert result.state == "activation_queued"
    assert result.request_id == "request-1"


def test_handoff_rejects_non_object_id_without_writing_request(tmp_path: Path) -> None:
    handoff = HostUpdateHandoff(tmp_path)

    result = handoff.apply("main; rm -rf /")

    assert result.code == "target_unavailable"
    assert not handoff.request_file.exists()


def test_command_event_contains_no_executable_or_transport_authority() -> None:
    now = datetime.now(timezone.utc)
    payload = command_payload(
        request_id="check-1",
        target_commit=TARGET,
        target_node_ids=(NODE,),
        created_at=now,
        expires_at=now + timedelta(minutes=2),
    )

    assert payload["schema"] == EVENT_SCHEMA
    assert payload["repository"] == "Nettking/msh"
    assert payload["branch"] == "main"
    assert set(payload) == {
        "schema",
        "request_id",
        "repository",
        "branch",
        "target_commit",
        "target_node_ids",
        "created_at",
        "expires_at",
    }
    assert validate_command_payload(payload) == payload


def test_command_event_rejects_expired_or_unapproved_input() -> None:
    now = datetime.now(timezone.utc)
    payload = command_payload(
        request_id="check-1",
        target_commit=TARGET,
        target_node_ids=(NODE,),
        created_at=now - timedelta(minutes=3),
        expires_at=now - timedelta(minutes=1),
    )
    with pytest.raises(ValueError, match="expired_or_invalid_request"):
        validate_command_payload(payload)

    modified = dict(payload)
    modified["expires_at"] = (now + timedelta(minutes=1)).isoformat()
    modified["repository"] = "attacker/msh"
    with pytest.raises(ValueError, match="unapproved_source"):
        validate_command_payload(modified)


def test_remote_processor_pins_creator_from_authenticated_session_event(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    now = datetime.now(timezone.utc)
    malicious = command_payload(
        request_id="attacker-check",
        target_commit=TARGET,
        target_node_ids=(NODE,),
        created_at=now,
        expires_at=now + timedelta(minutes=2),
    )
    authorized = command_payload(
        request_id="authority-check",
        target_commit=TARGET,
        target_node_ids=(NODE,),
        created_at=now,
        expires_at=now + timedelta(minutes=2),
    )
    replay = (
        SimpleNamespace(
            revision=1,
            event_type=SESSION_CREATED_EVENT,
            actor_node_id=AUTHORITY,
            payload={"session_id": "session-one"},
        ),
        SimpleNamespace(
            revision=2,
            event_type=CHECK_REQUEST_EVENT,
            actor_node_id=ATTACKER,
            payload=malicious,
        ),
        SimpleNamespace(
            revision=3,
            event_type=CHECK_REQUEST_EVENT,
            actor_node_id=AUTHORITY,
            payload=authorized,
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
    recorded: list[tuple[str, dict[str, object]]] = []

    def append(
        _service,
        _context,
        event_type: str,
        payload: dict[str, object],
        _request_id: str,
    ) -> None:
        recorded.append((event_type, payload))

    monkeypatch.setattr(events, "_append_remote_event", append)
    coordinator = SimpleNamespace(
        replay_page=lambda **_kwargs: (replay, 3)
    )
    context = SimpleNamespace(
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id=NODE)),
        binding=SimpleNamespace(internal_session_id="session-one"),
        coordinator=coordinator,
    )
    service = SimpleNamespace(remote_store=SimpleNamespace(load=lambda: object()))
    state_file = tmp_path / "processor.json"
    processor = events.FederationUpdateEventProcessor(
        service,
        handoff,
        state_file,
    )

    processor.process(context)

    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state["authority_node_id"] == AUTHORITY
    assert state["last_revision"] == 3
    assert handoff.inspect_calls == [TARGET]
    assert len(recorded) == 1
    assert recorded[0][1]["request_id"] == "authority-check"


def test_processor_replays_from_zero_if_creator_pin_is_missing(
    tmp_path: Path,
) -> None:
    state_file = tmp_path / "processor.json"
    state_file.write_text(
        json.dumps(
            {
                "schema": events.PROCESSOR_SCHEMA,
                "last_revision": 99,
                "pending": {},
            }
        ),
        encoding="utf-8",
    )
    revisions: list[int] = []

    def replay_page(**kwargs):
        revisions.append(kwargs["last_applied_revision"])
        return (), 0

    context = SimpleNamespace(
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id=NODE)),
        binding=SimpleNamespace(internal_session_id="session-one"),
        coordinator=SimpleNamespace(replay_page=replay_page),
    )
    service = SimpleNamespace(remote_store=SimpleNamespace(load=lambda: object()))
    handoff = SimpleNamespace(latest_result=lambda: None)
    processor = events.FederationUpdateEventProcessor(
        service,
        handoff,
        state_file,
    )

    processor.process(context)

    assert revisions == [0]


def test_queued_and_verified_apply_reports_have_distinct_idempotency_keys(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorded: list[tuple[str, str, dict[str, object]]] = []

    def append(
        _service,
        _context,
        event_type: str,
        payload: dict[str, object],
        request_id: str,
    ) -> None:
        recorded.append((event_type, request_id, payload))

    monkeypatch.setattr(events, "_append_remote_event", append)
    service = SimpleNamespace(remote_store=SimpleNamespace(load=lambda: object()))
    processor = events.FederationUpdateEventProcessor(
        service,
        SimpleNamespace(),
        tmp_path / "state.json",
    )
    context = SimpleNamespace(
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id=NODE))
    )

    processor._report(
        context,
        event_type=APPLY_REPORT_EVENT,
        federation_request_id="apply-1",
        result=UpdateInspection(
            "activation_queued",
            target_commit=TARGET,
            request_id="host-1",
        ),
    )
    processor._report(
        context,
        event_type=APPLY_REPORT_EVENT,
        federation_request_id="apply-1",
        result=UpdateInspection(
            "runtime_verified",
            TARGET,
            TARGET,
            "updated",
            "verified",
            TARGET,
            "host-1",
        ),
    )

    assert len(recorded) == 2
    assert recorded[0][0] == APPLY_REPORT_EVENT
    assert recorded[1][0] == APPLY_REPORT_EVENT
    assert recorded[0][1] != recorded[1][1]
    assert recorded[1][2]["running_commit"] == TARGET


def test_report_payload_is_bounded_to_safe_update_fields() -> None:
    payload = report_payload(
        request_id="apply-1",
        node_id=NODE,
        result=UpdateInspection(
            "runtime_verified",
            TARGET,
            TARGET,
            "updated",
            "verified",
            TARGET,
        ),
    )

    assert set(payload) == {
        "schema",
        "request_id",
        "node_id",
        "state",
        "current_commit",
        "target_commit",
        "running_commit",
        "code",
        "message",
        "reported_at",
    }
