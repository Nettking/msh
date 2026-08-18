from __future__ import annotations

from types import SimpleNamespace

import pytest

from catalog.federation.errors import FederationOperationError
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionDesiredState,
    ContributionPolicyState,
)
from catalog.flask_app.services import automatic_capability_bootstrap as bootstrap


def test_bootstrap_runs_every_applicable_benchmark_and_enables_available_services(
    monkeypatch,
) -> None:
    snapshot = SimpleNamespace(device_id="node-a")
    benchmark_runs: list[tuple[str, str]] = []
    choices: list[dict[str, str]] = []

    transition = SimpleNamespace(
        load=lambda: None,
        complete_current=lambda: SimpleNamespace(
            inspection_revision=7,
            completed=True,
        ),
    )
    onboarding = SimpleNamespace(
        authorized_context=lambda: SimpleNamespace(
            credentials=SimpleNamespace(identity=SimpleNamespace(node_id="node-a"))
        )
    )
    inspection = SimpleNamespace(
        load=lambda: None,
        run=lambda: snapshot,
        state=lambda _snapshot: "current",
    )

    class Benchmarks:
        def view_model(self, _snapshot, *, connected):
            assert connected is True
            if len(benchmark_runs) < 2:
                return (
                    {"state": "pending"},
                    [
                        {
                            "benchmark_id": "compute",
                            "target_service_id": "cpu",
                            "state": "pending",
                            "can_run": True,
                        },
                        {
                            "benchmark_id": "storage",
                            "target_service_id": "local",
                            "state": "pending",
                            "can_run": True,
                        },
                    ],
                    False,
                )
            return ({"state": "complete"}, [], True)

        def run(self, *, benchmark_id, target_service_id):
            benchmark_runs.append((benchmark_id, target_service_id))

    available = SimpleNamespace(
        candidate_id="compute-candidate",
        policy_state=ContributionPolicyState.ALLOWED,
    )
    recorder_without_source = SimpleNamespace(
        candidate_id="recorder-candidate",
        policy_state=ContributionPolicyState.ALLOWED,
    )

    class Contributions:
        def recommend(self, *, require_benchmark_review):
            assert require_benchmark_review is True
            return (available, recorder_without_source)

        def apply_choices(self, value):
            choices.append(dict(value))
            candidate_id, desired = next(iter(value.items()))
            if candidate_id == "recorder-candidate" and desired == "enabled":
                return (
                    SimpleNamespace(
                        policy_state=ContributionPolicyState.BLOCKED,
                        activation_state=ContributionActivationState.BLOCKED,
                    ),
                )
            return (
                SimpleNamespace(
                    policy_state=ContributionPolicyState.ALLOWED,
                    activation_state=(
                        ContributionActivationState.ACTIVE
                        if desired == "enabled"
                        else ContributionActivationState.INACTIVE
                    ),
                ),
            )

        def view_model(self, _snapshot, *, connected, benchmark_complete):
            assert connected is True
            assert benchmark_complete is True
            return ({"state": "complete"}, [], True)

    monkeypatch.setattr(
        bootstrap, "get_capability_startup_transition_service", lambda: transition
    )
    monkeypatch.setattr(bootstrap, "get_capability_onboarding_service", lambda: onboarding)
    monkeypatch.setattr(bootstrap, "get_capability_inspection_service", lambda: inspection)
    monkeypatch.setattr(bootstrap, "get_capability_benchmark_service", Benchmarks)
    monkeypatch.setattr(bootstrap, "get_capability_contribution_service", Contributions)

    result = bootstrap.bootstrap_capabilities()

    assert result.startup_completed is True
    assert result.inspection_revision == 7
    assert result.benchmarks_run == 2
    assert result.contributions_enabled == 1
    assert result.contributions_disabled == 1
    assert benchmark_runs == [("compute", "cpu"), ("storage", "local")]
    assert choices == [
        {"compute-candidate": ContributionDesiredState.ENABLED.value},
        {"recorder-candidate": ContributionDesiredState.ENABLED.value},
        {"recorder-candidate": ContributionDesiredState.DISABLED.value},
    ]


def test_bootstrap_fails_closed_without_trusted_federation(monkeypatch) -> None:
    monkeypatch.setattr(
        bootstrap,
        "get_capability_startup_transition_service",
        lambda: SimpleNamespace(load=lambda: None),
    )
    monkeypatch.setattr(
        bootstrap,
        "get_capability_onboarding_service",
        lambda: SimpleNamespace(authorized_context=lambda: None),
    )

    with pytest.raises(FederationOperationError) as caught:
        bootstrap.bootstrap_capabilities()

    assert caught.value.code == "automatic-bootstrap-federation-required"


def test_unrunnable_benchmark_prevents_false_startup_completion(monkeypatch) -> None:
    service = SimpleNamespace(
        view_model=lambda _snapshot, connected: (
            {"state": "pending"},
            [
                {
                    "benchmark_id": "missing-target",
                    "target_service_id": "gone",
                    "state": "blocked",
                    "can_run": False,
                }
            ],
            False,
        ),
        run=lambda **_kwargs: pytest.fail("blocked benchmark must not run"),
    )
    monkeypatch.setattr(bootstrap, "get_capability_benchmark_service", lambda: service)

    with pytest.raises(FederationOperationError) as caught:
        bootstrap._run_all_benchmarks(SimpleNamespace())

    assert caught.value.code == "automatic-benchmarks-incomplete"
