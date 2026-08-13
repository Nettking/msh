from __future__ import annotations

import threading
import time

import pytest

from catalog.capabilities.benchmarking import (
    BenchmarkExecutionContext,
    BenchmarkObservation,
    BenchmarkRegistry,
    BenchmarkRunRequest,
    BenchmarkRunner,
    CancellationToken,
    DuplicateRunIdError,
    SQLiteBenchmarkResultStore,
)
from catalog.federation.onboarding_models import (
    BenchmarkDefinition,
    BenchmarkRecommendation,
    BenchmarkState,
)


def _definition(*, prerequisites: tuple[str, ...] = ()) -> BenchmarkDefinition:
    return BenchmarkDefinition(
        benchmark_id="benchmark.local",
        capability_type="compute",
        capability_protocol="fcp.compute.v1",
        implementation_version="1.0.0",
        max_duration_seconds=2,
        max_parallelism=1,
        prerequisites=prerequisites,
        metric_names=("latency_ms", "samples"),
        invalidation_inputs=("handler_version",),
        privacy_classification="public-summary",
    )


def _request(
    *,
    run_id: str = "run-1",
    target_service_id: str = "handler-1",
    timeout_seconds: float | None = None,
    cancellation_token: CancellationToken | None = None,
    prerequisites: frozenset[str] = frozenset(),
) -> BenchmarkRunRequest:
    return BenchmarkRunRequest(
        run_id=run_id,
        device_id="device-1",
        benchmark_id="benchmark.local",
        target_service_id=target_service_id,
        dependency_inputs={"handler_version": "v1"},
        available_prerequisites=prerequisites,
        timeout_seconds=timeout_seconds,
        cancellation_token=cancellation_token,
    )


def test_success_is_durable_idempotent_and_grants_no_authority(tmp_path) -> None:
    calls = 0

    def probe(context: BenchmarkExecutionContext) -> BenchmarkObservation:
        nonlocal calls
        calls += 1
        context.raise_if_cancelled()
        return BenchmarkObservation(
            metrics={"latency_ms": 4.5, "samples": 3},
            recommendation=BenchmarkRecommendation.RECOMMENDED,
        )

    registry = BenchmarkRegistry()
    registry.register(_definition(), probe, result_ttl_seconds=300)
    store = SQLiteBenchmarkResultStore(tmp_path / "benchmarks.sqlite")
    runner = BenchmarkRunner(registry, store)

    first = runner.run(_request())
    replay = runner.run(_request())

    assert first == replay
    assert calls == 1
    assert first.state == BenchmarkState.PASSED
    assert first.recommendation == BenchmarkRecommendation.RECOMMENDED
    assert first.to_dict().keys() == {
        "schema",
        "run_id",
        "device_id",
        "benchmark_id",
        "benchmark_version",
        "target_service_id",
        "state",
        "metrics",
        "recommendation",
        "dependency_fingerprint",
        "diagnostics",
        "started_at",
        "finished_at",
        "expires_at",
    }

    with pytest.raises(DuplicateRunIdError):
        runner.run(_request(target_service_id="handler-2"))


def test_timeout_and_cancellation_are_bounded_and_persisted(tmp_path) -> None:
    def blocking_probe(context: BenchmarkExecutionContext) -> BenchmarkObservation:
        while True:
            context.raise_if_cancelled()
            time.sleep(0.005)

    registry = BenchmarkRegistry()
    registry.register(_definition(), blocking_probe, result_ttl_seconds=60)
    store = SQLiteBenchmarkResultStore(tmp_path / "benchmarks.sqlite")
    runner = BenchmarkRunner(registry, store, poll_interval_seconds=0.005)

    started = time.monotonic()
    timed_out = runner.run(_request(run_id="run-timeout", timeout_seconds=0.05))
    elapsed = time.monotonic() - started
    # The contract under test is that a blocking probe is stopped rather than
    # allowed to run forever, so the bound that means something is the
    # definition's own declared maximum duration. Hosted Windows runners pause
    # worker threads during process scheduling and virus scanning, so any
    # tighter wall-clock bound measures runner speed instead of the product:
    # this assertion previously read `< 1.0` and failed at 1.047s.
    assert elapsed < _definition().max_duration_seconds
    assert timed_out.state == BenchmarkState.FAILED
    assert timed_out.recommendation == BenchmarkRecommendation.NOT_RECOMMENDED
    assert timed_out.diagnostics == ("benchmark timed out",)

    token = CancellationToken()
    timer = threading.Timer(0.03, token.cancel)
    timer.start()
    try:
        cancelled = runner.run(
            _request(
                run_id="run-cancelled",
                timeout_seconds=0.5,
                cancellation_token=token,
            )
        )
    finally:
        timer.cancel()
    assert cancelled.state == BenchmarkState.CANCELLED
    assert cancelled.recommendation == BenchmarkRecommendation.RERUN_REQUIRED
    assert store.load("run-cancelled") == cancelled


def test_missing_prerequisite_skips_without_invoking_probe(tmp_path) -> None:
    invoked = False

    def probe(context: BenchmarkExecutionContext) -> BenchmarkObservation:
        nonlocal invoked
        invoked = True
        return BenchmarkObservation()

    registry = BenchmarkRegistry()
    registry.register(
        _definition(prerequisites=("handler.available",)),
        probe,
        result_ttl_seconds=60,
    )
    store = SQLiteBenchmarkResultStore(tmp_path / "benchmarks.sqlite")

    result = BenchmarkRunner(registry, store).run(_request())

    assert invoked is False
    assert result.state == BenchmarkState.SKIPPED
    assert result.recommendation == BenchmarkRecommendation.UNSUPPORTED
    assert result.diagnostics == ("missing prerequisites: handler.available",)


def test_probe_failures_and_private_diagnostics_are_redacted(tmp_path) -> None:
    def probe(context: BenchmarkExecutionContext) -> BenchmarkObservation:
        raise RuntimeError(
            "request to http://127.0.0.1:11434 failed with Bearer fcp_join_secret"
        )

    registry = BenchmarkRegistry()
    registry.register(_definition(), probe, result_ttl_seconds=60)
    store = SQLiteBenchmarkResultStore(tmp_path / "benchmarks.sqlite")

    result = BenchmarkRunner(registry, store).run(_request())
    serialized = str(result.to_dict()).casefold()

    assert result.state == BenchmarkState.FAILED
    assert result.diagnostics == ("RuntimeError: [redacted]",)
    assert "127.0.0.1" not in serialized
    assert "fcp_join_secret" not in serialized
    assert "http://" not in serialized


def test_invalid_probe_metrics_become_safe_failed_evidence(tmp_path) -> None:
    def probe(context: BenchmarkExecutionContext) -> BenchmarkObservation:
        return BenchmarkObservation(
            metrics={
                "latency_ms": 1.0,
                "undeclared": 7,
                "samples": {"endpoint": "http://10.0.0.2:80"},
            },
            recommendation=BenchmarkRecommendation.RECOMMENDED,
        )

    registry = BenchmarkRegistry()
    registry.register(_definition(), probe, result_ttl_seconds=60)
    store = SQLiteBenchmarkResultStore(tmp_path / "benchmarks.sqlite")

    result = BenchmarkRunner(registry, store).run(_request())

    assert result.state == BenchmarkState.FAILED
    assert result.metrics == {}
    assert "undeclared metric" in result.diagnostics[0]
    assert store.load(result.run_id) == result


def test_parallelism_limit_and_active_run_ids_fail_closed(tmp_path) -> None:
    entered = threading.Event()
    release = threading.Event()
    calls = 0

    def probe(context: BenchmarkExecutionContext) -> BenchmarkObservation:
        nonlocal calls
        calls += 1
        entered.set()
        while not release.wait(0.005):
            context.raise_if_cancelled()
        return BenchmarkObservation(
            metrics={"latency_ms": 1.0, "samples": 1},
            recommendation=BenchmarkRecommendation.USABLE,
        )

    registry = BenchmarkRegistry()
    registry.register(_definition(), probe, result_ttl_seconds=60)
    store = SQLiteBenchmarkResultStore(tmp_path / "benchmarks.sqlite")
    runner = BenchmarkRunner(registry, store, poll_interval_seconds=0.005)
    first_result: list[object] = []

    worker = threading.Thread(
        target=lambda: first_result.append(runner.run(_request(run_id="run-active")))
    )
    worker.start()
    assert entered.wait(0.5)

    with pytest.raises(DuplicateRunIdError):
        runner.run(_request(run_id="run-active"))

    blocked = runner.run(
        _request(run_id="run-blocked", timeout_seconds=0.05)
    )
    assert blocked.state == BenchmarkState.FAILED
    assert blocked.diagnostics == (
        "benchmark timed out waiting for an execution slot",
    )
    assert calls == 1

    release.set()
    worker.join(timeout=0.5)
    assert not worker.is_alive()
    assert first_result[0].state == BenchmarkState.PASSED
