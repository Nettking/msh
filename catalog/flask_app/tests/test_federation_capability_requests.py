from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from catalog.federation.onboarding_models import (
    BenchmarkState,
    ContributionActivationState,
    ContributionDesiredState,
    ContributionPolicyState,
)
from catalog.flask_app.services import federation_capability_requests as module
from catalog.flask_app.services.federation_capability_requests import (
    REQUEST_EVENT,
    SESSION_CREATED_EVENT,
    FederationCapabilityRequestProcessor,
    FederationCapabilityRequestService,
    report_payload,
    request_payload,
    validate_request_payload,
)

ACTOR = "node-owner"
REMOTE = "node-remote"
OFFLINE = "node-offline"


class _Coordinator:
    def __init__(self, creator: str = ACTOR) -> None:
        self.events: list[dict[str, Any]] = []
        self.session = SimpleNamespace(created_by_node_id=creator)
        self.store = self

    def get_session(self, session_id: str) -> object:
        assert session_id == "session-one"
        return self.session

    def append_event(self, **kwargs: Any) -> object:
        self.events.append(dict(kwargs))
        return SimpleNamespace(revision=len(self.events))

    def replay_page(self, **_kwargs: Any) -> tuple[tuple[object, ...], int]:
        return (), 0


class _Authority:
    devices: tuple[object, ...] = ()

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        pass

    def snapshot(self) -> object:
        return SimpleNamespace(available=True, devices=self.devices)


def _device(node_id: str, state: str, label: str) -> object:
    return SimpleNamespace(node_id=node_id, state=state, label=label)


def _install_context(
    monkeypatch: pytest.MonkeyPatch,
    coordinator: _Coordinator,
    devices: tuple[object, ...],
) -> None:
    context = SimpleNamespace(
        coordinator=coordinator,
        binding=SimpleNamespace(internal_session_id="session-one"),
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id=ACTOR)),
    )
    onboarding = SimpleNamespace(authorized_context=lambda: context)
    monkeypatch.setattr(module, "get_capability_onboarding_service", lambda: onboarding)
    _Authority.devices = devices
    monkeypatch.setattr(module, "FederationAuthorityAdapter", _Authority)


def test_request_all_targets_only_connected_remote_members(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    coordinator = _Coordinator()
    _install_context(
        monkeypatch,
        coordinator,
        (
            _device(ACTOR, "connected", "Owner"),
            _device(REMOTE, "connected", "Remote"),
            _device(OFFLINE, "disconnected", "Offline"),
        ),
    )
    service = FederationCapabilityRequestService(tmp_path / "capabilities.json")

    snapshot = service.request_all()

    request_events = [
        item for item in coordinator.events if item["event_type"] == REQUEST_EVENT
    ]
    assert len(request_events) == 1
    assert request_events[0]["actor_node_id"] == ACTOR
    assert request_events[0]["payload"]["actions"] == ["benchmark", "contribute"]
    assert request_events[0]["payload"]["target_node_ids"] == [REMOTE]
    by_id = {item["node_id"]: item for item in snapshot["devices"]}
    assert ACTOR not in by_id
    assert by_id[REMOTE]["state"] == "requested"
    assert by_id[OFFLINE]["state"] == "offline"
    assert by_id[OFFLINE]["reachable"] is False


def test_request_all_requires_active_federation_leader(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    coordinator = _Coordinator(creator="another-node")
    _install_context(monkeypatch, coordinator, ())
    service = FederationCapabilityRequestService(tmp_path / "capabilities.json")

    with pytest.raises(PermissionError, match="federation_leader_required"):
        service.request_all()

    assert coordinator.events == []


def test_request_payload_does_not_allow_action_override() -> None:
    now = datetime.now(timezone.utc)
    payload = request_payload(
        request_id="request-one",
        target_node_ids=(REMOTE,),
        created_at=now,
        expires_at=now + timedelta(minutes=5),
    )
    payload["actions"] = ["benchmark", "contribute", "run-arbitrary-command"]

    with pytest.raises(ValueError, match="unsupported_actions"):
        validate_request_payload(payload)


def test_member_execution_runs_bounded_plan_and_enables_only_allowed_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = SimpleNamespace(device_id=REMOTE)
    runnable = SimpleNamespace(
        runnable=True,
        benchmark_id="benchmark.safe.v1",
        target_service_id="service-safe",
    )
    blocked_plan = SimpleNamespace(
        runnable=False,
        benchmark_id="benchmark.unavailable.v1",
        target_service_id="service-unavailable",
    )

    class _Inspection:
        def run(self) -> object:
            return snapshot

    class _Benchmarks:
        inspection_service = _Inspection()

        def plan(self, observed: object) -> tuple[object, ...]:
            assert observed is snapshot
            return runnable, blocked_plan

        def run(self, *, benchmark_id: str, target_service_id: str) -> object:
            assert benchmark_id == "benchmark.safe.v1"
            assert target_service_id == "service-safe"
            return SimpleNamespace(state=BenchmarkState.PASSED)

    allowed = SimpleNamespace(
        candidate_id="candidate-allowed",
        policy_state=ContributionPolicyState.ALLOWED,
        missing_prerequisites=(),
    )
    blocked = SimpleNamespace(
        candidate_id="candidate-blocked",
        policy_state=ContributionPolicyState.BLOCKED,
        missing_prerequisites=("approval",),
    )

    class _Contributions:
        def __init__(self) -> None:
            self.applied: list[dict[str, object]] = []

        def recommend(self, *, require_benchmark_review: bool) -> tuple[object, ...]:
            assert require_benchmark_review is True
            return allowed, blocked

        def apply_choices(self, choices: dict[str, object]) -> tuple[object, ...]:
            self.applied.append(dict(choices))
            assert choices == {
                "candidate-allowed": ContributionDesiredState.ENABLED.value
            }
            return (
                SimpleNamespace(
                    desired_state=ContributionDesiredState.ENABLED,
                    activation_state=ContributionActivationState.ACTIVE,
                ),
            )

    contributions = _Contributions()
    monkeypatch.setattr(module, "get_capability_benchmark_service", lambda: _Benchmarks())
    monkeypatch.setattr(
        module,
        "get_capability_contribution_service",
        lambda: contributions,
    )

    payload = FederationCapabilityRequestProcessor._execution_report(
        "request-one",
        REMOTE,
    )

    parsed = module.parse_report(payload)
    assert parsed is not None
    _request_id, _node_id, report = parsed
    assert report["state"] == "completed"
    assert report["benchmarks_attempted"] == 1
    assert report["benchmarks_passed"] == 1
    assert report["benchmark_errors"] == 0
    assert report["contribution_candidates"] == 2
    assert report["contributions_enabled"] == 1
    assert report["contributions_blocked"] == 1
    assert report["contribution_errors"] == 0
    assert contributions.applied == [
        {"candidate-allowed": ContributionDesiredState.ENABLED.value}
    ]


class _ReplayCoordinator:
    def __init__(self, request_actor: str) -> None:
        now = datetime.now(timezone.utc)
        self.events = (
            SimpleNamespace(
                revision=1,
                event_type=SESSION_CREATED_EVENT,
                actor_node_id=ACTOR,
                payload={},
            ),
            SimpleNamespace(
                revision=2,
                event_type=REQUEST_EVENT,
                actor_node_id=request_actor,
                payload=request_payload(
                    request_id="request-event-one",
                    target_node_ids=(REMOTE,),
                    created_at=now,
                    expires_at=now + timedelta(minutes=5),
                ),
            ),
        )

    def replay_page(
        self,
        *,
        last_applied_revision: int,
        **_kwargs: Any,
    ) -> tuple[tuple[object, ...], int]:
        remaining = tuple(
            event for event in self.events if event.revision > last_applied_revision
        )
        return remaining, 2


class _RemoteStore:
    def load(self) -> object:
        return SimpleNamespace(relay_url="wss://relay.invalid")


class _RelayRuntime:
    def __init__(self) -> None:
        self.appended: list[dict[str, Any]] = []

    def append_session_event(self, _remote: object, **kwargs: Any) -> object:
        self.appended.append(dict(kwargs))
        return SimpleNamespace(revision=3)


def _remote_context(coordinator: object) -> object:
    return SimpleNamespace(
        coordinator=coordinator,
        binding=SimpleNamespace(internal_session_id="session-one"),
        credentials=SimpleNamespace(identity=SimpleNamespace(node_id=REMOTE)),
    )


def _completed_report(request_id: str, node_id: str) -> dict[str, object]:
    return report_payload(
        request_id=request_id,
        node_id=node_id,
        state="completed",
        benchmarks_attempted=1,
        benchmarks_passed=1,
        benchmark_errors=0,
        contribution_candidates=1,
        contributions_enabled=1,
        contributions_blocked=0,
        contribution_errors=0,
        message="done",
    )


def test_member_processor_executes_only_session_creator_request(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    coordinator = _ReplayCoordinator(ACTOR)
    runtime = _RelayRuntime()
    service = SimpleNamespace(remote_store=_RemoteStore(), relay_runtime=runtime)
    processor = FederationCapabilityRequestProcessor(
        service,
        tmp_path / "processor.json",
    )
    executions: list[tuple[str, str]] = []

    def execute(request_id: str, node_id: str) -> dict[str, object]:
        executions.append((request_id, node_id))
        return _completed_report(request_id, node_id)

    monkeypatch.setattr(
        FederationCapabilityRequestProcessor,
        "_execution_report",
        staticmethod(execute),
    )

    processor.process(_remote_context(coordinator))

    assert executions == [("request-event-one", REMOTE)]
    assert len(runtime.appended) == 1
    assert runtime.appended[0]["event_type"] == module.REPORT_EVENT
    assert runtime.appended[0]["payload"]["request_id"] == "request-event-one"


def test_member_processor_ignores_request_from_non_creator(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    coordinator = _ReplayCoordinator("node-attacker")
    runtime = _RelayRuntime()
    service = SimpleNamespace(remote_store=_RemoteStore(), relay_runtime=runtime)
    processor = FederationCapabilityRequestProcessor(
        service,
        tmp_path / "processor.json",
    )
    executions: list[tuple[str, str]] = []

    def execute(request_id: str, node_id: str) -> dict[str, object]:
        executions.append((request_id, node_id))
        return _completed_report(request_id, node_id)

    monkeypatch.setattr(
        FederationCapabilityRequestProcessor,
        "_execution_report",
        staticmethod(execute),
    )

    processor.process(_remote_context(coordinator))

    assert executions == []
    assert runtime.appended == []
