from __future__ import annotations

from types import SimpleNamespace

import pytest

from catalog.federation.errors import FederationOperationError
from catalog.federation.onboarding_models import (
    BenchmarkRecommendation,
    BenchmarkState,
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


def _storage_repair_fixture(monkeypatch, *, result_state=BenchmarkState.PASSED):
    node_id = "node-a"
    local_provider_id = "fcp-local-data-storage"
    provider_id = bootstrap.federation_storage_provider_id(node_id, local_provider_id)
    candidate = SimpleNamespace(
        candidate_id="candidate-77e622",
        capability_type="storage",
        capacity_envelope={"provider_id": local_provider_id},
    )
    intent = SimpleNamespace(
        candidate_id=candidate.candidate_id,
        desired_state=ContributionDesiredState.ENABLED,
    )
    reconciled: list[bool] = []
    contribution = SimpleNamespace(
        intents=lambda: (intent,),
        recommend=lambda **_kwargs: (candidate,),
        reconcile=lambda: reconciled.append(True) or (intent,),
    )
    store = SimpleNamespace(
        database="control.sqlite3",
        get_session=lambda _session_id: SimpleNamespace(created_by_node_id=node_id),
    )
    context = SimpleNamespace(
        binding=SimpleNamespace(internal_session_id="session-a"),
        coordinator=SimpleNamespace(store=store),
    )
    onboarding = SimpleNamespace(authorized_context=lambda: context)
    assignment = SimpleNamespace(primary_provider_id=provider_id)
    control_snapshot = SimpleNamespace(
        groups={bootstrap.DEFAULT_STORAGE_GROUP_ID: assignment},
        leader_grants={},
    )
    monkeypatch.setattr(
        bootstrap,
        "PhaseDControlPlane",
        lambda _database: SimpleNamespace(snapshot=lambda _session_id: control_snapshot),
    )
    benchmark_item = SimpleNamespace(
        benchmark_id="benchmark.storage.candidate-roundtrip.v1",
        target_service_id=local_provider_id,
        runnable=True,
        definition=SimpleNamespace(capability_type="storage"),
        dependency_inputs={"candidate_fingerprint": "same"},
    )
    runs: list[tuple[str, str]] = []
    result = SimpleNamespace(
        state=result_state,
        recommendation=BenchmarkRecommendation.RECOMMENDED,
    )
    benchmarks = SimpleNamespace(
        plan=lambda _snapshot: (benchmark_item,),
        run=lambda *, benchmark_id, target_service_id: (
            runs.append((benchmark_id, target_service_id)) or result
        ),
    )
    monkeypatch.setattr(
        bootstrap,
        "evaluate_result",
        lambda *_args, **_kwargs: SimpleNamespace(current=True),
    )
    monkeypatch.setattr(
        bootstrap, "get_capability_contribution_service", lambda: contribution
    )
    monkeypatch.setattr(
        bootstrap, "get_capability_onboarding_service", lambda: onboarding
    )
    monkeypatch.setattr(
        bootstrap, "get_capability_benchmark_service", lambda: benchmarks
    )
    return runs, reconciled


def test_completed_creator_primary_without_grant_refreshes_green_storage_evidence(
    monkeypatch,
) -> None:
    runs, reconciled = _storage_repair_fixture(monkeypatch)

    count = bootstrap._repair_creator_storage_authority_evidence(
        SimpleNamespace(device_id="node-a"),
        node_id="node-a",
    )

    assert count == 1
    assert runs == [
        ("benchmark.storage.candidate-roundtrip.v1", "fcp-local-data-storage")
    ]
    assert reconciled == [True]


def test_storage_repair_refuses_to_claim_authority_when_fresh_check_is_not_green(
    monkeypatch,
) -> None:
    runs, reconciled = _storage_repair_fixture(
        monkeypatch,
        result_state=BenchmarkState.FAILED,
    )

    with pytest.raises(FederationOperationError) as caught:
        bootstrap._repair_creator_storage_authority_evidence(
            SimpleNamespace(device_id="node-a"),
            node_id="node-a",
        )

    assert caught.value.code == "automatic-storage-benchmark-not-green"
    assert len(runs) == 1
    assert reconciled == []


def test_existing_completed_startup_reports_storage_evidence_repair(monkeypatch) -> None:
    existing = SimpleNamespace(completed=True, inspection_revision=4)
    transition = SimpleNamespace(load=lambda: existing)
    snapshot = SimpleNamespace(device_id="node-a", revision=9)
    monkeypatch.setattr(bootstrap, "CapabilityStartupState", object)
    monkeypatch.setattr(
        bootstrap, "get_capability_startup_transition_service", lambda: transition
    )
    monkeypatch.setattr(bootstrap, "_connected_device_id", lambda: "node-a")
    monkeypatch.setattr(bootstrap, "_current_inspection", lambda _node_id: snapshot)
    monkeypatch.setattr(
        bootstrap,
        "_repair_creator_storage_authority_evidence",
        lambda _snapshot, *, node_id: 1,
    )

    result = bootstrap.bootstrap_capabilities()

    assert result.state == "repaired-storage-authority-evidence"
    assert result.inspection_revision == 9
    assert result.benchmarks_run == 1
    assert result.startup_completed is True
