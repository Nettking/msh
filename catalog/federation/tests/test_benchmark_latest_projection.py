from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from catalog.federation.projections import (
    BenchmarkResultsAdapter,
    FederationAuthoritySnapshot,
    FederationProjectionService,
    JobAuthoritySnapshot,
    OnboardingSnapshot,
    ProjectionAdapters,
    ProviderSnapshot,
    StorageAuthoritySnapshot,
)

NOW = datetime(2026, 8, 6, 16, 56, tzinfo=timezone.utc)


class _StaticAdapter:
    def __init__(self, value: object) -> None:
        self.value = value

    def snapshot(self) -> object:
        return self.value


def _result(
    *,
    run_id: str,
    benchmark_id: str,
    target_service_id: str,
    finished_at: datetime,
    expires_at: datetime,
) -> object:
    return SimpleNamespace(
        run_id=run_id,
        device_id="node-local",
        benchmark_id=benchmark_id,
        target_service_id=target_service_id,
        state="passed",
        recommendation="recommended",
        finished_at=finished_at,
        expires_at=expires_at,
        metrics={},
        diagnostics=(),
    )


def test_historical_expired_runs_do_not_create_false_rerun_work() -> None:
    definitions = (
        ("benchmark-storage", "msh-local-data-storage"),
        ("benchmark-compute", "msh-system-summary"),
        ("benchmark-ai", "ollama-configured"),
    )
    stored: list[object] = []
    for index, (benchmark_id, target) in enumerate(definitions):
        stored.append(
            _result(
                run_id=f"current-{index}",
                benchmark_id=benchmark_id,
                target_service_id=target,
                finished_at=NOW - timedelta(seconds=index),
                expires_at=NOW + timedelta(minutes=14),
            )
        )
    for index in range(7):
        benchmark_id, target = definitions[index % len(definitions)]
        stored.append(
            _result(
                run_id=f"historical-{index}",
                benchmark_id=benchmark_id,
                target_service_id=target,
                finished_at=NOW - timedelta(minutes=30 + index),
                expires_at=NOW - timedelta(minutes=15 + index),
            )
        )

    benchmarks = BenchmarkResultsAdapter(
        SimpleNamespace(list_results=lambda: tuple(stored)),
        now=lambda: NOW,
    )
    snapshot = benchmarks.snapshot()

    assert len(snapshot.results) == 3
    assert {result.run_id for result in snapshot.results} == {
        "current-0",
        "current-1",
        "current-2",
    }
    assert all(result.current for result in snapshot.results)

    service = FederationProjectionService(
        ProjectionAdapters(
            onboarding=_StaticAdapter(
                OnboardingSnapshot(
                    available=True,
                    reason_code="current",
                    federation_id="federation-test",
                    connection_state="connected",
                    trusted=True,
                    device_id="node-local",
                )
            ),
            providers=_StaticAdapter(ProviderSnapshot(True, "current")),
            benchmarks=benchmarks,
            federation=_StaticAdapter(
                FederationAuthoritySnapshot(
                    available=True,
                    reason_code="current",
                    label="Test Federation",
                    state="active",
                )
            ),
            storage=_StaticAdapter(StorageAuthoritySnapshot(True, "current")),
            jobs=_StaticAdapter(JobAuthoritySnapshot(True, "current")),
        ),
        now=lambda: NOW,
    )

    detail = service.benchmarks().to_dict()
    overview = service.overview().to_dict()

    assert len(detail["items"]) == 3
    assert all(item["state"] == "passed" for item in detail["items"])
    assert "need rerun" not in detail["state_label"].casefold()
    assert overview["summary_cards"][2]["value"] == "3 of 3"
    assert overview["content"]["benchmarks"]["state_label"] == "Current"
    assert overview["recommended_action"] is None
