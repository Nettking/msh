from __future__ import annotations

from types import SimpleNamespace

import pytest

from catalog.capabilities.benchmarking.adapters.compute import (
    RegisteredComputeHandlerAdapter,
)
from catalog.capabilities.benchmarking.adapters.mtconnect import MtconnectSourceAdapter
from catalog.capabilities.benchmarking.adapters.network import (
    AuthenticatedNetworkPathAdapter,
)
from catalog.capabilities.benchmarking.adapters.ollama import OllamaBenchmarkAdapter
from catalog.capabilities.benchmarking.adapters.storage import StorageCandidateAdapter
from catalog.federation.errors import AuthenticationError
from catalog.federation.onboarding_models import ContributionDesiredState
from catalog.flask_app.app import (
    _resume_persisted_contributions_safely,
    create_app,
)
from catalog.flask_app.services.existing_setup_resume import (
    ExistingSetupRequired,
    ExistingSetupResumeError,
    ExistingSetupResumeService,
)

_DURABLE_EVIDENCE_TTL_SECONDS = 315_360_000


class _Onboarding:
    def __init__(self, *, failures: list[BaseException] | None = None) -> None:
        self.failures = list(failures or [])
        self.reconnect_calls = 0
        self.context_calls = 0
        self.identity = SimpleNamespace(identity=SimpleNamespace(node_id="node-existing"))
        self.binding = SimpleNamespace(federation_id="federation-existing")
        self.context = SimpleNamespace(
            credentials=self.identity,
            binding=self.binding,
        )

    def identity_or_none(self) -> object:
        return self.identity

    def binding_or_none(self) -> object:
        return self.binding

    def reconnect(self) -> object:
        self.reconnect_calls += 1
        return self.binding

    def authorized_context(self) -> object:
        self.context_calls += 1
        if self.failures:
            raise self.failures.pop(0)
        return self.context


class _Inspection:
    def __init__(self, *, state: str = "current") -> None:
        self.load_calls = 0
        self.run_calls = 0
        self.snapshot = SimpleNamespace(revision=7, device_id="node-existing")
        self.snapshot_state = state

    def load(self) -> object:
        self.load_calls += 1
        return self.snapshot

    def state(self, snapshot: object) -> str:
        assert snapshot is self.snapshot
        return self.snapshot_state

    def run(self) -> object:  # pragma: no cover - resume must never run inspection
        self.run_calls += 1
        raise AssertionError("resume must reuse saved inspection evidence")


class _Benchmark:
    def __init__(self, *, complete: bool = True) -> None:
        self.list_calls = 0
        self.run_calls = 0
        self.plan_calls = 0
        self.view_calls = 0
        self.complete = complete
        self.results = (
            SimpleNamespace(
                device_id="node-existing",
                benchmark_id="benchmark.ai",
                target_service_id="ollama-configured",
                state=SimpleNamespace(value="passed"),
            ),
            SimpleNamespace(
                device_id="node-existing",
                benchmark_id="benchmark.compute",
                target_service_id="msh-system-summary",
                state=SimpleNamespace(value="passed"),
            ),
            SimpleNamespace(
                device_id="another-device",
                benchmark_id="benchmark.other",
                target_service_id="other-target",
                state=SimpleNamespace(value="passed"),
            ),
        )

    def list_results(self) -> tuple[object, ...]:
        self.list_calls += 1
        return self.results

    def view_model(self, snapshot: object, *, connected: bool):
        self.view_calls += 1
        assert connected is True
        assert snapshot is not None
        return {}, [], self.complete

    def plan(self, _snapshot: object) -> tuple[object, ...]:  # pragma: no cover
        self.plan_calls += 1
        raise AssertionError("resume must not plan benchmark execution")

    def run(self, **_kwargs: object) -> object:  # pragma: no cover
        self.run_calls += 1
        raise AssertionError("resume must not execute a benchmark")


class _Contribution:
    def __init__(self) -> None:
        self.reconcile_calls = 0
        self.suspend_calls: list[str] = []
        self.saved_intents = (
            SimpleNamespace(
                candidate_id="candidate-ai",
                desired_state=ContributionDesiredState.ENABLED,
            ),
            SimpleNamespace(
                candidate_id="candidate-disabled",
                desired_state=ContributionDesiredState.DISABLED,
            ),
        )

    def has_persisted_intents(self) -> bool:
        return True

    def intents(self) -> tuple[object, ...]:
        return self.saved_intents

    def reconcile(self) -> tuple[object, ...]:
        self.reconcile_calls += 1
        return (object(), object())

    def suspend(self, candidate_id: str) -> object:
        self.suspend_calls.append(candidate_id)
        return object()


def test_production_evidence_defaults_survive_normal_restarts() -> None:
    app = create_app()

    assert (
        app.config["CAPABILITY_ONBOARDING_INSPECTION_TTL_SECONDS"]
        == _DURABLE_EVIDENCE_TTL_SECONDS
    )
    assert OllamaBenchmarkAdapter.result_ttl_seconds == _DURABLE_EVIDENCE_TTL_SECONDS
    assert MtconnectSourceAdapter.result_ttl_seconds == _DURABLE_EVIDENCE_TTL_SECONDS
    assert (
        RegisteredComputeHandlerAdapter.result_ttl_seconds
        == _DURABLE_EVIDENCE_TTL_SECONDS
    )
    assert StorageCandidateAdapter.result_ttl_seconds == _DURABLE_EVIDENCE_TTL_SECONDS
    assert (
        AuthenticatedNetworkPathAdapter.result_ttl_seconds
        == _DURABLE_EVIDENCE_TTL_SECONDS
    )


def test_resume_retries_reconnect_then_reuses_existing_evidence() -> None:
    onboarding = _Onboarding(failures=[OSError("relay starting"), TimeoutError()])
    inspection = _Inspection()
    benchmark = _Benchmark()
    contribution = _Contribution()
    current = [0.0]
    sleeps: list[float] = []
    progress: list[str] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        current[0] += seconds

    report = ExistingSetupResumeService(
        onboarding_service=onboarding,
        inspection_service=inspection,
        benchmark_service=benchmark,
        contribution_service=contribution,
        reconnect_timeout_seconds=10,
        reconnect_interval_seconds=1,
        monotonic=lambda: current[0],
        sleep=sleep,
        progress=progress.append,
    ).resume()

    assert onboarding.reconnect_calls == 3
    assert onboarding.context_calls == 3
    assert sleeps == [1, 1]
    assert inspection.load_calls == 1
    assert inspection.run_calls == 0
    assert benchmark.list_calls == 1
    assert benchmark.plan_calls == 0
    assert benchmark.run_calls == 0
    assert contribution.reconcile_calls == 0
    assert contribution.suspend_calls == []
    assert report.device_id == "node-existing"
    assert report.federation_id == "federation-existing"
    assert report.inspection_revision == 7
    assert report.benchmark_runs == (
        ("benchmark.ai", "ollama-configured", "passed"),
        ("benchmark.compute", "msh-system-summary", "passed"),
    )
    assert report.unavailable_benchmarks == ()
    assert report.reconciled_contributions == 0
    assert report.partial is False
    assert progress[0].startswith("[1/4] Reconnecting")
    assert any(message.startswith("[2/4] Loading") for message in progress)
    assert any(message.startswith("[3/4] Loading") for message in progress)
    assert any(message.startswith("[4/4] Leaving") for message in progress)


def test_safe_flask_reconcile_uses_current_saved_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inspection = _Inspection()
    benchmark = _Benchmark(complete=True)
    contribution = _Contribution()
    monkeypatch.setattr(
        "catalog.flask_app.app.get_capability_inspection_service",
        lambda: inspection,
    )
    monkeypatch.setattr(
        "catalog.flask_app.app.get_capability_benchmark_service",
        lambda: benchmark,
    )
    monkeypatch.setattr(
        "catalog.flask_app.app.get_capability_contribution_service",
        lambda: contribution,
    )

    reconciled, suspended = _resume_persisted_contributions_safely()

    assert (reconciled, suspended) == (2, 0)
    assert inspection.load_calls == 1
    assert inspection.run_calls == 0
    assert benchmark.view_calls == 1
    assert benchmark.run_calls == 0
    assert contribution.reconcile_calls == 1
    assert contribution.suspend_calls == []


def test_safe_flask_reconcile_suspends_enabled_intent_when_benchmark_is_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inspection = _Inspection()
    benchmark = _Benchmark(complete=False)
    contribution = _Contribution()
    monkeypatch.setattr(
        "catalog.flask_app.app.get_capability_inspection_service",
        lambda: inspection,
    )
    monkeypatch.setattr(
        "catalog.flask_app.app.get_capability_benchmark_service",
        lambda: benchmark,
    )
    monkeypatch.setattr(
        "catalog.flask_app.app.get_capability_contribution_service",
        lambda: contribution,
    )

    reconciled, suspended = _resume_persisted_contributions_safely()

    assert (reconciled, suspended) == (0, 1)
    assert inspection.run_calls == 0
    assert benchmark.view_calls == 1
    assert benchmark.run_calls == 0
    assert contribution.reconcile_calls == 0
    assert contribution.suspend_calls == ["candidate-ai"]


def test_resume_never_creates_identity_or_federation_when_state_is_missing() -> None:
    class MissingOnboarding:
        def identity_or_none(self) -> None:
            return None

        def binding_or_none(self) -> None:
            return None

        def reconnect(self) -> object:  # pragma: no cover - must not run
            raise AssertionError("reconnect must not run without saved state")

        def authorized_context(self) -> object:  # pragma: no cover - must not run
            raise AssertionError("authorization must not run without saved state")

    inspection = _Inspection()
    service = ExistingSetupResumeService(
        onboarding_service=MissingOnboarding(),
        inspection_service=inspection,
        benchmark_service=_Benchmark(),
    )

    with pytest.raises(ExistingSetupRequired):
        service.resume()

    assert inspection.load_calls == 0
    assert inspection.run_calls == 0


def test_terminal_membership_failure_does_not_retry_or_replace_authority() -> None:
    onboarding = _Onboarding(
        failures=[
            AuthenticationError(
                "revoked-node",
                "node identity was revoked",
                "actor_node_id",
            )
        ]
    )
    sleeps: list[float] = []
    service = ExistingSetupResumeService(
        onboarding_service=onboarding,
        inspection_service=_Inspection(),
        benchmark_service=_Benchmark(),
        reconnect_timeout_seconds=10,
        reconnect_interval_seconds=1,
        monotonic=lambda: 0,
        sleep=sleeps.append,
    )

    with pytest.raises(ExistingSetupResumeError) as error:
        service.resume()

    assert error.value.code == "revoked-node"
    assert onboarding.reconnect_calls == 1
    assert onboarding.context_calls == 1
    assert sleeps == []


def test_expired_inspection_is_reported_but_never_rerun_automatically() -> None:
    inspection = _Inspection(state="expired")
    benchmark = _Benchmark()
    contribution = _Contribution()

    report = ExistingSetupResumeService(
        onboarding_service=_Onboarding(),
        inspection_service=inspection,
        benchmark_service=benchmark,
        contribution_service=contribution,
    ).resume()

    assert inspection.load_calls == 1
    assert inspection.run_calls == 0
    assert benchmark.list_calls == 1
    assert benchmark.run_calls == 0
    assert contribution.reconcile_calls == 0
    assert report.inspection_revision == 7
    assert report.warnings == ("inspection-evidence-expired",)
    assert report.reconciled_contributions == 0
    assert report.partial is True


def test_resume_allows_no_saved_benchmarks_without_running_any() -> None:
    benchmark = _Benchmark()
    benchmark.results = ()

    report = ExistingSetupResumeService(
        onboarding_service=_Onboarding(),
        inspection_service=_Inspection(),
        benchmark_service=benchmark,
        contribution_service=_Contribution(),
    ).resume()

    assert benchmark.list_calls == 1
    assert benchmark.plan_calls == 0
    assert benchmark.run_calls == 0
    assert report.benchmark_runs == ()
    assert report.warnings == ()
    assert report.partial is False
