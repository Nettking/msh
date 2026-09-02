from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.federation.software_update import UpdateInspection
from catalog.flask_app.services import federation_capability_requests as capability
from catalog.flask_app.services import federation_update_events as update

TARGET = "a" * 40
NODE = "node-target"
AUTHORITY = "node-authority"


def _context(events: tuple[object, ...], *, last_revision: int = 2):
    def replay_page(**kwargs):
        start = kwargs["last_applied_revision"]
        if start >= last_revision:
            return (), last_revision
        return tuple(event for event in events if event.revision > start), last_revision

    return SimpleNamespace(
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id=NODE)),
        binding=SimpleNamespace(internal_session_id="session-one"),
        coordinator=SimpleNamespace(
            replay_page=replay_page,
            append_event=lambda **_kwargs: None,
        ),
    )


def _service() -> SimpleNamespace:
    return SimpleNamespace(
        remote_store=SimpleNamespace(load=lambda: object()),
        relay_runtime=SimpleNamespace(append_session_event=lambda *args, **kwargs: None),
    )


def _command_payload(request_id: str) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    return update.command_payload(
        request_id=request_id,
        target_commit=TARGET,
        target_node_ids=(NODE,),
        created_at=now,
        expires_at=now + timedelta(minutes=2),
    )


def _events(event_type: str, request_id: str) -> tuple[object, ...]:
    return (
        SimpleNamespace(
            revision=1,
            event_type=update.SESSION_CREATED_EVENT,
            actor_node_id=AUTHORITY,
            payload={"session_id": "session-one"},
        ),
        SimpleNamespace(
            revision=2,
            event_type=event_type,
            actor_node_id=AUTHORITY,
            payload=_command_payload(request_id),
        ),
    )


def _capability_events(event_type: str, request_id: str) -> tuple[object, ...]:
    now = datetime.now(timezone.utc)
    return (
        SimpleNamespace(
            revision=1,
            event_type=capability.SESSION_CREATED_EVENT,
            actor_node_id=AUTHORITY,
            payload={"session_id": "session-one"},
        ),
        SimpleNamespace(
            revision=2,
            event_type=event_type,
            actor_node_id=AUTHORITY,
            payload=capability.request_payload(
                request_id=request_id,
                target_node_ids=(NODE,),
                created_at=now,
                expires_at=now + timedelta(minutes=2),
            ),
        ),
    )


def _capability_report(request_id: str) -> dict[str, object]:
    return capability.report_payload(
        request_id=request_id,
        node_id=NODE,
        state="completed",
        benchmarks_attempted=0,
        benchmarks_passed=0,
        benchmark_errors=0,
        contribution_candidates=0,
        contributions_enabled=0,
        contributions_blocked=0,
        contribution_errors=0,
        message="completed",
    )


def test_capability_state_write_failure_replays_before_local_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request_id = "capability-state-write"
    replay = _capability_events(capability.REQUEST_EVENT, request_id)
    context = _context(replay)
    service = _service()
    calls: list[str] = []
    monkeypatch.setattr(
        capability.FederationCapabilityRequestProcessor,
        "_execution_report",
        staticmethod(lambda request, _node: calls.append(request) or _capability_report(request)),
    )
    processor = capability.FederationCapabilityRequestProcessor(
        service,
        tmp_path / "capability.json",
    )
    original_save = processor._save

    def fail_marker_save(state: dict[str, object]) -> None:
        if state.get("in_flight") is not None:
            raise RuntimeError("state write interrupted")
        original_save(state)

    monkeypatch.setattr(processor, "_save", fail_marker_save)
    with pytest.raises(RuntimeError, match="state write interrupted"):
        processor.process(context)
    assert calls == []
    persisted = json.loads(processor.state_file.read_text(encoding="utf-8"))
    assert persisted["last_revision"] == 1

    monkeypatch.setattr(processor, "_save", original_save)
    processor.process(context)

    assert calls == [request_id]
    state = json.loads(processor.state_file.read_text(encoding="utf-8"))
    assert state["last_revision"] == 2
    assert state["in_flight"] is None


def test_capability_report_failure_is_retried_without_reexecuting(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request_id = "capability-report-write"
    replay = _capability_events(capability.REQUEST_EVENT, request_id)
    context = _context(replay)
    service = _service()
    executions: list[str] = []
    monkeypatch.setattr(
        capability.FederationCapabilityRequestProcessor,
        "_execution_report",
        staticmethod(
            lambda request, _node: executions.append(request)
            or _capability_report(request)
        ),
    )
    append_calls = 0

    def append_once_then_succeed(*args, **kwargs):
        nonlocal append_calls
        append_calls += 1
        if append_calls == 1:
            raise RuntimeError("report write interrupted")

    monkeypatch.setattr(capability, "_append_remote_event", append_once_then_succeed)
    processor = capability.FederationCapabilityRequestProcessor(
        service,
        tmp_path / "capability.json",
    )
    with pytest.raises(RuntimeError, match="report write interrupted"):
        processor.process(context)

    state = json.loads(processor.state_file.read_text(encoding="utf-8"))
    assert state["last_revision"] == 2
    assert state["in_flight"] is None
    assert request_id in state["pending_reports"]
    assert executions == [request_id]

    processor.process(context)

    assert executions == [request_id]
    state = json.loads(processor.state_file.read_text(encoding="utf-8"))
    assert state["last_revision"] == 2
    assert state["in_flight"] is None
    assert append_calls == 2


class _Handoff:
    def __init__(self) -> None:
        self.inspect_calls: list[str] = []

    def inspect(
        self,
        *,
        target: str,
        fetch: bool,
        request_id: str | None = None,
    ) -> UpdateInspection:
        assert fetch is True
        assert request_id is not None
        self.inspect_calls.append(request_id)
        return UpdateInspection(
            "up_to_date",
            current_commit=TARGET,
            target_commit=target,
            running_commit=TARGET,
            request_id=request_id,
        )

    def result_for(self, _request_id: str) -> None:
        return None

    def latest_result(self) -> None:
        return None


def test_update_state_write_failure_replays_before_host_handoff(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request_id = "update-state-write"
    replay = _events(update.CHECK_REQUEST_EVENT, request_id)
    context = _context(replay)
    service = _service()
    handoff = _Handoff()
    processor = update.FederationUpdateEventProcessor(
        service,
        handoff,
        tmp_path / "update.json",
    )
    original_write = update._write_state
    failed = False

    def fail_marker_write(path: Path, state: dict[str, object]) -> None:
        nonlocal failed
        if state.get("in_flight") is not None and not failed:
            failed = True
            raise RuntimeError("state write interrupted")
        original_write(path, state)

    monkeypatch.setattr(update, "_write_state", fail_marker_write)
    with pytest.raises(RuntimeError, match="state write interrupted"):
        processor.process(context)
    assert handoff.inspect_calls == []

    monkeypatch.setattr(update, "_write_state", original_write)
    processor.process(context)

    assert len(handoff.inspect_calls) == 1
    state = json.loads(processor.state_file.read_text(encoding="utf-8"))
    assert state["last_revision"] == 2
    assert state["in_flight"] is None


def test_update_report_failure_retries_stored_outcome_without_rechecking(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request_id = "update-report-write"
    replay = _events(update.CHECK_REQUEST_EVENT, request_id)
    context = _context(replay)
    service = _service()
    handoff = _Handoff()
    append_calls = 0

    def append_once_then_succeed(*args, **kwargs):
        nonlocal append_calls
        append_calls += 1
        if append_calls == 1:
            raise RuntimeError("report write interrupted")

    monkeypatch.setattr(update, "_append_remote_event", append_once_then_succeed)
    processor = update.FederationUpdateEventProcessor(
        service,
        handoff,
        tmp_path / "update.json",
    )
    with pytest.raises(RuntimeError, match="report write interrupted"):
        processor.process(context)

    state = json.loads(processor.state_file.read_text(encoding="utf-8"))
    assert state["last_revision"] == 1
    assert state["in_flight"]["request_id"] == request_id
    assert len(handoff.inspect_calls) == 1

    processor.process(context)

    assert len(handoff.inspect_calls) == 1
    state = json.loads(processor.state_file.read_text(encoding="utf-8"))
    assert state["last_revision"] == 2
    assert state["in_flight"] is None
    assert append_calls == 2
