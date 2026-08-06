from __future__ import annotations

from types import SimpleNamespace

import pytest

from catalog.federation.errors import AuthenticationError
from catalog.flask_app.services.existing_setup_resume import (
    ExistingSetupRequired,
    ExistingSetupResumeError,
    ExistingSetupResumeService,
)


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
    def __init__(self) -> None:
        self.calls = 0
        self.snapshot = SimpleNamespace(revision=7)

    def run(self) -> object:
        self.calls += 1
        return self.snapshot


class _Benchmark:
    def __init__(self) -> None:
        self.runs: list[tuple[str, str]] = []
        self.items = (
            SimpleNamespace(
                benchmark_id="benchmark.ai",
                target_service_id="ollama-configured",
                runnable=True,
            ),
            SimpleNamespace(
                benchmark_id="benchmark.compute",
                target_service_id="msh-system-summary",
                runnable=True,
            ),
            SimpleNamespace(
                benchmark_id="benchmark.network",
                target_service_id="unavailable-target",
                runnable=False,
            ),
        )

    def plan(self, snapshot: object) -> tuple[object, ...]:
        assert getattr(snapshot, "revision") == 7
        return self.items

    def run(self, *, benchmark_id: str, target_service_id: str) -> object:
        self.runs.append((benchmark_id, target_service_id))
        return SimpleNamespace(state=SimpleNamespace(value="passed"))


class _Contribution:
    def __init__(self) -> None:
        self.reconcile_calls = 0

    def has_persisted_intents(self) -> bool:
        return True

    def reconcile(self) -> tuple[object, ...]:
        self.reconcile_calls += 1
        return (object(), object())


def test_resume_retries_reconnect_then_refreshes_existing_setup() -> None:
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
    assert inspection.calls == 1
    assert benchmark.runs == [
        ("benchmark.ai", "ollama-configured"),
        ("benchmark.compute", "msh-system-summary"),
    ]
    assert contribution.reconcile_calls == 1
    assert report.device_id == "node-existing"
    assert report.federation_id == "federation-existing"
    assert report.inspection_revision == 7
    assert len(report.benchmark_runs) == 2
    assert report.unavailable_benchmarks == (
        ("benchmark.network", "unavailable-target"),
    )
    assert report.reconciled_contributions == 2
    assert report.partial is True
    assert progress[0].startswith("[1/4] Reconnecting")
    assert any(message.startswith("[2/4] Inspecting") for message in progress)
    assert any(message.startswith("[3/4] Planning") for message in progress)
    assert any(message.startswith("[4/4] Reconciling") for message in progress)


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

    assert inspection.calls == 0


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


def test_one_benchmark_failure_is_reported_without_losing_other_refreshes() -> None:
    benchmark = _Benchmark()
    original_run = benchmark.run

    def run(*, benchmark_id: str, target_service_id: str) -> object:
        if benchmark_id == "benchmark.compute":
            raise AuthenticationError(
                "benchmark-target-rejected",
                "target rejected the bounded check",
                "benchmark_id",
            )
        return original_run(
            benchmark_id=benchmark_id,
            target_service_id=target_service_id,
        )

    benchmark.run = run  # type: ignore[method-assign]
    contribution = _Contribution()

    report = ExistingSetupResumeService(
        onboarding_service=_Onboarding(),
        inspection_service=_Inspection(),
        benchmark_service=benchmark,
        contribution_service=contribution,
    ).resume()

    assert len(report.benchmark_runs) == 1
    assert report.warnings == ("benchmark-target-rejected",)
    assert report.reconciled_contributions == 2
    assert report.partial is True
