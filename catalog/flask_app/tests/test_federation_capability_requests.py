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
    FederationCapabilityRequestProcessor,
    FederationCapabilityRequestService,
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


def test_request_all_requires_session_creator_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    coordinator = _Coordinator(creator="another-node")
    _install_context(monkeypatch, coordinator, ())
    service = FederationCapabilityRequestService(tmp_path / "capabilities.json")

    with pytest.raises(PermissionError, match="capability_request_authority_required"):
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
