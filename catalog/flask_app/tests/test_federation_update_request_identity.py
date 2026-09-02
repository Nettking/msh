"""The deterministic host request identity carried across the check contract.

``FederationUpdateEventProcessor`` derives one stable ``host_request_id`` per
accepted Federation command and persists it in the pre-side-effect in-flight
marker before it asks the host to do anything. The identity is only useful if
it actually reaches the host handoff: the host publishes its result under that
identity, and a restart reads the result back by the same identity instead of
reissuing an accepted command.

These tests pin that end to end against the real ``HostUpdateHandoff`` rather
than a stand-in, and pin the adapter signature itself so an implementation can
never again silently drop the parameter.
"""

from __future__ import annotations

import hashlib
import inspect as inspect_module
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from catalog.federation.software_update import GitUpdateAdapter
from catalog.flask_app.services import federation_update_events as update
from catalog.flask_app.services.federation_update_handoff import (
    RESULT_SCHEMA,
    HostUpdateHandoff,
)
from catalog.flask_app.services.federation_update_service import LocalUpdateAdapter

TARGET = "a" * 40
CURRENT = "b" * 40
NODE = "node-target"
AUTHORITY = "node-authority"
SESSION = "session-one"


# ---- adapter conformance ------------------------------------------------
#
# The defect this module guards against was not a logic error. The check
# contract was widened to carry a request identity, the producer and the real
# handoff were updated, and several conforming implementations were not. No
# type check ran over them, so the mismatch only surfaced as a TypeError at the
# call site. Comparing signatures against the declared protocol catches it at
# the point the contract changes.


def _keyword_parameters(function: Any) -> dict[str, Any]:
    return {
        name: parameter
        for name, parameter in inspect_module.signature(function).parameters.items()
        if parameter.kind is inspect_module.Parameter.KEYWORD_ONLY
    }


@pytest.mark.parametrize(
    "implementation",
    [HostUpdateHandoff, GitUpdateAdapter],
    ids=["host-handoff", "git-adapter"],
)
def test_every_local_update_adapter_accepts_the_declared_check_parameters(
    implementation: type,
) -> None:
    declared = _keyword_parameters(LocalUpdateAdapter.inspect)
    actual = _keyword_parameters(implementation.inspect)

    assert set(declared) <= set(actual), (
        f"{implementation.__name__}.inspect is missing "
        f"{sorted(set(declared) - set(actual))} from the LocalUpdateAdapter contract"
    )
    for name, parameter in declared.items():
        assert actual[name].default == parameter.default


def test_the_declared_check_contract_carries_a_request_identity() -> None:
    # Guards the protocol itself: dropping this parameter from the declaration
    # is what let non-conforming implementations through unnoticed.
    assert "request_id" in _keyword_parameters(LocalUpdateAdapter.inspect)


# ---- replay harness -----------------------------------------------------


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
            payload={"session_id": SESSION},
        ),
        SimpleNamespace(
            revision=2,
            event_type=event_type,
            actor_node_id=AUTHORITY,
            payload=_command_payload(request_id),
        ),
    )


def _context(events: tuple[object, ...], *, last_revision: int = 2):
    def replay_page(**kwargs):
        start = kwargs["last_applied_revision"]
        if start >= last_revision:
            return (), last_revision
        return tuple(event for event in events if event.revision > start), last_revision

    return SimpleNamespace(
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id=NODE)),
        binding=SimpleNamespace(internal_session_id=SESSION),
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


class _RecordingHostAgent(HostUpdateHandoff):
    """The real handoff with the separately owned host agent's turn inlined.

    Everything production does still runs here: the bounded request document,
    the writer lock and atomic publication, the per-request result addressing,
    and the polling read back. Only the agent's *scheduling* is made
    deterministic, so a test never races a background thread. Set ``offline``
    to model an agent that never answers -- the request is published and
    nothing comes back.
    """

    def __init__(self, directory: Path, **kwargs: Any) -> None:
        super().__init__(directory, timeout=1.0, poll_interval=0.02, **kwargs)
        self.published: list[dict[str, Any]] = []
        self.offline = False

    def _write_request(self, value: dict[str, object]) -> None:
        super()._write_request(value)
        self.published.append(dict(value))
        if not self.offline:
            self._agent_turn(dict(value))

    def _agent_turn(self, request: dict[str, Any]) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        request_id = str(request["request_id"])
        result = {
            "schema": RESULT_SCHEMA,
            "request_id": request_id,
            "state": "up_to_date",
            "current_commit": TARGET,
            "target_commit": request.get("target_commit"),
            "running_commit": TARGET,
        }
        self._result_path(request_id).write_text(
            json.dumps(result),
            encoding="utf-8",
        )
        # A real agent claims the request before it answers.
        self.request_file.unlink(missing_ok=True)

    @property
    def check_requests(self) -> list[dict[str, Any]]:
        return [item for item in self.published if item.get("action") == "check"]

    @property
    def apply_requests(self) -> list[dict[str, Any]]:
        return [item for item in self.published if item.get("action") == "apply"]


@pytest.fixture
def reports(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, object]]:
    captured: list[dict[str, object]] = []

    def append_report(_service, _context, event_type, payload, _request_id):
        captured.append({"event_type": event_type, **payload})

    monkeypatch.setattr(update, "_append_remote_event", append_report)
    return captured


def _processor(handoff: HostUpdateHandoff, state_file: Path):
    return update.FederationUpdateEventProcessor(_service(), handoff, state_file)


def _expected_host_request_id(federation_request_id: str) -> str:
    digest = hashlib.sha256(
        f"host\0{federation_request_id}\0{NODE}".encode()
    ).hexdigest()[:40]
    return f"fed-{digest}"


# ---- the identity reaches the real host handoff -------------------------


def test_a_check_reaches_the_real_handoff_under_the_deterministic_identity(
    tmp_path: Path,
    reports: list[dict[str, object]],
) -> None:
    handoff = _RecordingHostAgent(tmp_path / "agent")
    processor = _processor(handoff, tmp_path / "processor.json")

    processor.process(_context(_events(update.CHECK_REQUEST_EVENT, "update-1")))

    assert len(handoff.check_requests) == 1
    published = handoff.check_requests[0]
    assert published["request_id"] == _expected_host_request_id("update-1")
    assert published["target_commit"] == TARGET
    # The outcome came back through the real per-request result channel.
    assert [report["state"] for report in reports] == ["up_to_date"]


def test_the_host_identity_is_the_same_after_a_restart_from_empty_state(
    tmp_path: Path,
    reports: list[dict[str, object]],
) -> None:
    first = _RecordingHostAgent(tmp_path / "agent-one")
    _processor(first, tmp_path / "one.json").process(
        _context(_events(update.CHECK_REQUEST_EVENT, "update-1"))
    )

    # A replacement process with no inherited state at all: a reinstall, a
    # different container, a wiped processor file. The identity is derived from
    # the accepted command and this node, so it must be reproduced exactly.
    second = _RecordingHostAgent(tmp_path / "agent-two")
    _processor(second, tmp_path / "two.json").process(
        _context(_events(update.CHECK_REQUEST_EVENT, "update-1"))
    )

    assert first.check_requests[0]["request_id"] == (
        second.check_requests[0]["request_id"]
    )
    assert first.check_requests[0]["request_id"] == _expected_host_request_id("update-1")


def test_distinct_commands_and_nodes_never_share_a_host_identity() -> None:
    assert update._host_request_id("update-1", NODE) != update._host_request_id(
        "update-2", NODE
    )
    assert update._host_request_id("update-1", NODE) != update._host_request_id(
        "update-1", "other-node"
    )


# ---- crash windows ------------------------------------------------------


def _crash_after_marker(processor: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Let the in-flight marker land, then die before the host is asked."""

    original = update._write_state
    state_file = processor.state_file

    def write_then_crash(path: Path, state: dict[str, object]) -> None:
        original(path, state)
        if path == state_file and state.get("in_flight") is not None:
            raise RuntimeError("crashed after the durable marker")

    monkeypatch.setattr(update, "_write_state", write_then_crash)


def test_a_crash_before_the_host_answered_reports_the_window_without_reissuing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reports: list[dict[str, object]],
) -> None:
    handoff = _RecordingHostAgent(tmp_path / "agent")
    state_file = tmp_path / "processor.json"
    processor = _processor(handoff, state_file)
    context = _context(_events(update.CHECK_REQUEST_EVENT, "update-1"))

    _crash_after_marker(processor, monkeypatch)
    with pytest.raises(RuntimeError, match="crashed after the durable marker"):
        processor.process(context)

    # The marker is durable and the host was never asked.
    marker = json.loads(state_file.read_text(encoding="utf-8"))["in_flight"]
    assert marker["host_request_id"] == _expected_host_request_id("update-1")
    assert handoff.published == []

    monkeypatch.undo()
    monkeypatch.setattr(update, "_append_remote_event", lambda *a, **k: reports.append(
        {"event_type": a[2], **a[3]}
    ))
    _processor(handoff, state_file).process(context)

    # Recovery reported the unknown outcome and never issued the command. An
    # accepted command whose local effect is unknown is reported, not retried.
    assert handoff.published == []
    assert [report["code"] for report in reports] == ["processor-crash-window"]
    assert json.loads(state_file.read_text(encoding="utf-8"))["in_flight"] is None


def test_a_crash_after_the_host_answered_recovers_the_real_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reports: list[dict[str, object]],
) -> None:
    handoff = _RecordingHostAgent(tmp_path / "agent")
    state_file = tmp_path / "processor.json"
    context = _events(update.CHECK_REQUEST_EVENT, "update-1")

    # The host completed the check and published its result under the
    # deterministic identity; the processor died before reporting it.
    host_request_id = _expected_host_request_id("update-1")
    handoff._agent_turn(
        {"request_id": host_request_id, "target_commit": TARGET, "action": "check"}
    )
    handoff.published.clear()
    state_file.write_text(
        json.dumps(
            {
                "schema": update.PROCESSOR_SCHEMA,
                "last_revision": 1,
                "authority_node_id": AUTHORITY,
                "pending": {},
                "in_flight": {
                    "kind": "check",
                    "request_id": "update-1",
                    "node_id": NODE,
                    "revision": 2,
                    "host_request_id": host_request_id,
                    "target_commit": TARGET,
                },
            }
        ),
        encoding="utf-8",
    )

    _processor(handoff, state_file).process(_context(context))

    # This is what the deterministic identity buys: the completed outcome is
    # found and reported, instead of degrading to an unknown crash window, and
    # the command is never reissued against the host.
    assert handoff.published == []
    assert [report["state"] for report in reports] == ["up_to_date"]
    assert [report["code"] for report in reports] != ["processor-crash-window"]
    assert json.loads(state_file.read_text(encoding="utf-8"))["in_flight"] is None


def test_a_check_is_not_reissued_when_the_command_is_replayed_again(
    tmp_path: Path,
    reports: list[dict[str, object]],
) -> None:
    handoff = _RecordingHostAgent(tmp_path / "agent")
    state_file = tmp_path / "processor.json"
    context = _context(_events(update.CHECK_REQUEST_EVENT, "update-1"))

    _processor(handoff, state_file).process(context)
    _processor(handoff, state_file).process(context)

    # The checkpointed revision, not the host, is what stops the second pass.
    assert len(handoff.check_requests) == 1
    assert len(reports) == 1


# ---- apply and trial semantics ------------------------------------------


def test_an_apply_queues_once_under_the_deterministic_identity(
    tmp_path: Path,
    reports: list[dict[str, object]],
) -> None:
    handoff = _RecordingHostAgent(tmp_path / "agent")
    handoff.offline = True  # An apply is queued; the agent activates later.
    state_file = tmp_path / "processor.json"

    _processor(handoff, state_file).process(
        _context(_events(update.APPLY_REQUEST_EVENT, "update-1"))
    )

    assert len(handoff.apply_requests) == 1
    host_request_id = _expected_host_request_id("update-1")
    assert handoff.apply_requests[0]["request_id"] == host_request_id
    assert handoff.apply_requests[0]["action"] == "apply"
    # The durable pending row is keyed by the same identity, so a replacement
    # process can reconcile the activation it did not start.
    persisted = json.loads(state_file.read_text(encoding="utf-8"))
    assert persisted["pending"]["update-1"] == {
        "host_request_id": host_request_id,
        "target_commit": TARGET,
    }
    assert [report["state"] for report in reports] == ["activation_queued"]


def test_a_replayed_apply_adopts_the_published_result_instead_of_reapplying(
    tmp_path: Path,
    reports: list[dict[str, object]],
) -> None:
    handoff = _RecordingHostAgent(tmp_path / "agent")
    handoff.offline = True
    state_file = tmp_path / "processor.json"
    context = _context(_events(update.APPLY_REQUEST_EVENT, "update-1"))
    host_request_id = _expected_host_request_id("update-1")

    _processor(handoff, state_file).process(context)
    assert len(handoff.apply_requests) == 1

    # The host finished the activation and published under the same identity.
    handoff._agent_turn(
        {"request_id": host_request_id, "target_commit": TARGET, "action": "apply"}
    )
    # A replacement process replays the same accepted command from scratch.
    handoff.published.clear()
    fresh = tmp_path / "replacement.json"
    _processor(handoff, fresh).process(context)

    # The published result is adopted; the host is never asked to apply twice.
    assert handoff.apply_requests == []
    assert reports[-1]["state"] == "up_to_date"
    assert json.loads(fresh.read_text(encoding="utf-8"))["pending"] == {}
