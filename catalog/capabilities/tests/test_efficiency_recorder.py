"""Completed dispatches become observations without changing job outcomes."""

from __future__ import annotations

from datetime import timedelta

from catalog.capabilities.dispatch import (
    DispatchEvent,
    DispatchResponse,
    DispatchState,
)
from catalog.capabilities.efficiency import (
    ExecutionEnvironment,
    ExecutionObservationRecorder,
    default_workload_descriptor,
    measurements_from_dispatch,
)
from catalog.capabilities.efficiency.recorder import RESULT_METRICS_KEY

from catalog.capabilities.tests.efficiency_harness import (
    NOW,
    build_job,
    build_store,
)

QUEUED_AT = NOW
ACCEPTED_AT = NOW + timedelta(seconds=2)
RUNNING_AT = NOW + timedelta(seconds=3)
FINISHED_AT = NOW + timedelta(seconds=11)


def _response(
    *, succeeded: bool = True, details: dict | None = None
) -> DispatchResponse:
    terminal = DispatchEvent(
        3,
        DispatchState.SUCCEEDED if succeeded else DispatchState.FAILED,
        FINISHED_AT,
        reason_code=None if succeeded else "worker-handler-failed",
        details=details,
    )
    return DispatchResponse(
        dispatch_id="dispatch-1",
        session_id="session-efficiency",
        job_id="job-001",
        provider_id="cap-node-a",
        attempt_id="attempt-1",
        events=(
            DispatchEvent(1, DispatchState.ACCEPTED, ACCEPTED_AT),
            DispatchEvent(2, DispatchState.RUNNING, RUNNING_AT),
            terminal,
        ),
    )


def test_dispatch_timestamps_become_queue_and_execution_timings() -> None:
    measurements = measurements_from_dispatch(
        _response(), queued_at=QUEUED_AT, retries=1
    )
    assert measurements.succeeded is True
    assert measurements.queue_millis == 2_000
    assert measurements.execution_millis == 8_000
    assert measurements.total_millis == 11_000
    assert measurements.retries == 1


def test_a_failed_dispatch_keeps_its_reason_and_its_cost() -> None:
    measurements = measurements_from_dispatch(_response(succeeded=False))
    assert measurements.succeeded is False
    assert measurements.error_code == "worker-handler-failed"
    # The time a failure burned is still real scheduling cost.
    assert measurements.execution_millis == 8_000


def test_missing_queue_information_yields_a_partial_observation() -> None:
    measurements = measurements_from_dispatch(_response())
    assert measurements.queue_millis is None
    assert measurements.total_millis == 9_000


def test_optional_worker_metrics_are_read_and_bounded() -> None:
    measurements = measurements_from_dispatch(
        _response(
            details={
                RESULT_METRICS_KEY: {
                    "gpu_utilization_millis": 812.4,
                    "vram_peak_bytes": 6_442_450_944,
                    "output_units": 480,
                    "throughput_units_per_second": 60.0,
                    "unknown_metric": 5,
                }
            }
        ),
        queued_at=QUEUED_AT,
    )
    assert measurements.gpu_utilization_millis == 812
    assert measurements.vram_peak_bytes == 6_442_450_944
    assert measurements.throughput_units_per_second == 60.0


def test_recording_a_dispatch_teaches_the_store(tmp_path) -> None:
    store = build_store(tmp_path)
    recorder = ExecutionObservationRecorder(store, node_id="node-a")
    job = build_job()

    observation = recorder.record_dispatch(
        job,
        _response(),
        node_id="node-a",
        queued_at=QUEUED_AT,
        environment=ExecutionEnvironment(model_id="qwen3:4b"),
    )
    assert store.observation_count() == 1
    assert observation.node_id == "node-a"
    assert observation.capability_id == "cap-node-a"
    assert observation.workload == default_workload_descriptor(job)
    assert observation.requested_protocol == job.capability.protocol
    assert observation.requirements_fingerprint is not None

    # A redelivered response is the same attempt, so it is not learned twice.
    recorder.record_dispatch(
        job,
        _response(),
        node_id="node-a",
        queued_at=QUEUED_AT,
        environment=ExecutionEnvironment(model_id="qwen3:4b"),
    )
    assert store.observation_count() == 1
