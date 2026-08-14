"""Turn completed executions into observations.

This is the "every useful execution teaches the federation something" edge of
the design. It reads what the existing dispatch contracts already record -
accepted/running/terminal timestamps, success, reason code and the worker's
own result details - and converts it into one bounded observation.

Recording is strictly additive: it never changes a job's outcome and any
failure to record is swallowed by the caller's own error handling, never by
the job path.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import datetime
from typing import Any

from catalog.federation.errors import FederationValidationError

from ..dispatch import DispatchResponse, DispatchState
from ..jobs import JobContract
from .contracts import (
    ExecutionEnvironment,
    ExecutionMeasurements,
    ExecutionObservation,
    WorkloadDescriptor,
    requirements_fingerprint,
)
from .store import SQLiteExecutionLearningStore

#: Worker result key under which optional resource metrics may be reported.
RESULT_METRICS_KEY = "metrics"

_METRIC_FIELDS = (
    "cpu_utilization_millis",
    "gpu_utilization_millis",
    "memory_peak_bytes",
    "vram_peak_bytes",
    "input_units",
    "output_units",
    "energy_millijoules",
)
_FLOAT_METRIC_FIELDS = ("throughput_units_per_second", "quality_score")


def observation_id_for(
    *, session_id: str, job_id: str, attempt_id: str
) -> str:
    """Stable identity so a replayed report is the same observation."""

    digest = hashlib.sha256(
        f"{session_id}|{job_id}|{attempt_id}".encode()
    ).hexdigest()
    return f"obs-{digest[:32]}"


def _millis(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    delta = (end - start).total_seconds() * 1000.0
    if delta < 0:
        return None
    return round(delta)


def _optional_metrics(details: Any) -> dict[str, Any]:
    """Read the optional, bounded metric block a worker may return."""

    if not isinstance(details, dict):
        return {}
    metrics = details.get(RESULT_METRICS_KEY)
    if not isinstance(metrics, dict):
        return {}
    values: dict[str, Any] = {}
    for name in _METRIC_FIELDS:
        raw = metrics.get(name)
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            continue
        # Bounds are enforced again by ExecutionMeasurements; rounding here
        # keeps a float report from being rejected outright.
        values[name] = round(raw)
    for name in _FLOAT_METRIC_FIELDS:
        raw = metrics.get(name)
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            continue
        values[name] = float(raw)
    return values


def measurements_from_dispatch(
    response: DispatchResponse,
    *,
    queued_at: datetime | None = None,
    retries: int = 0,
) -> ExecutionMeasurements:
    """Derive measurements from one dispatch response.

    Timestamps that the transport did not provide simply stay absent; a
    partial observation is still useful evidence.
    """

    if not isinstance(response, DispatchResponse):
        raise FederationValidationError(
            "invalid-dispatch-response", "response", "must be a DispatchResponse"
        )
    accepted_at: datetime | None = None
    running_at: datetime | None = None
    terminal = response.events[-1]
    for event in response.events:
        if event.state is DispatchState.ACCEPTED:
            accepted_at = event.occurred_at
        elif event.state is DispatchState.RUNNING:
            running_at = event.occurred_at
    succeeded = terminal.state is DispatchState.SUCCEEDED
    started_at = running_at or accepted_at
    values = _optional_metrics(terminal.details)
    execution_millis = _millis(started_at, terminal.occurred_at)
    queue_millis = _millis(queued_at, accepted_at)
    total_millis = _millis(queued_at or accepted_at, terminal.occurred_at)
    return ExecutionMeasurements(
        succeeded=succeeded,
        error_code=None if succeeded else (terminal.reason_code or "dispatch-failed"),
        retries=retries,
        queue_millis=queue_millis,
        execution_millis=execution_millis,
        total_millis=total_millis,
        **values,
    )


class ExecutionObservationRecorder:
    """Build and persist observations for executions this node performed."""

    def __init__(
        self,
        store: SQLiteExecutionLearningStore,
        *,
        node_id: str,
        workload_resolver: Callable[[JobContract], WorkloadDescriptor] | None = None,
    ) -> None:
        self.store = store
        self.node_id = node_id
        self._workload_resolver = workload_resolver

    def _workload(
        self, job: JobContract, workload: WorkloadDescriptor | None
    ) -> WorkloadDescriptor:
        if workload is not None:
            return workload
        if self._workload_resolver is not None:
            return self._workload_resolver(job)
        # Imported lazily: the default resolver lives with the ranker so the
        # two always agree on how a workload class is derived.
        from .ranking import default_workload_descriptor

        return default_workload_descriptor(job)

    def build(
        self,
        job: JobContract,
        *,
        attempt_id: str,
        capability_id: str,
        measurements: ExecutionMeasurements,
        observed_at: datetime,
        node_id: str | None = None,
        workload: WorkloadDescriptor | None = None,
        environment: ExecutionEnvironment | None = None,
    ) -> ExecutionObservation:
        """Assemble one observation without persisting it."""

        return ExecutionObservation(
            observation_id=observation_id_for(
                session_id=job.session_id,
                job_id=job.job_id,
                attempt_id=attempt_id,
            ),
            session_id=job.session_id,
            job_id=job.job_id,
            attempt_id=attempt_id,
            node_id=node_id or self.node_id,
            capability_id=capability_id,
            workload=self._workload(job, workload),
            environment=environment or ExecutionEnvironment(),
            measurements=measurements,
            observed_at=observed_at,
            requested_protocol=job.capability.protocol,
            requested_protocol_version=job.capability.protocol_version,
            requirements_fingerprint=requirements_fingerprint(
                job.capability.requirements
            ),
        )

    def record(
        self,
        job: JobContract,
        *,
        attempt_id: str,
        capability_id: str,
        measurements: ExecutionMeasurements,
        observed_at: datetime,
        node_id: str | None = None,
        workload: WorkloadDescriptor | None = None,
        environment: ExecutionEnvironment | None = None,
    ) -> ExecutionObservation:
        """Assemble and persist one observation."""

        observation = self.build(
            job,
            attempt_id=attempt_id,
            capability_id=capability_id,
            measurements=measurements,
            observed_at=observed_at,
            node_id=node_id,
            workload=workload,
            environment=environment,
        )
        self.store.record(observation, now=observed_at)
        return observation

    def record_dispatch(
        self,
        job: JobContract,
        response: DispatchResponse,
        *,
        node_id: str,
        queued_at: datetime | None = None,
        retries: int = 0,
        workload: WorkloadDescriptor | None = None,
        environment: ExecutionEnvironment | None = None,
    ) -> ExecutionObservation:
        """Record the observation implied by one completed dispatch."""

        measurements = measurements_from_dispatch(
            response, queued_at=queued_at, retries=retries
        )
        return self.record(
            job,
            attempt_id=response.attempt_id,
            capability_id=response.provider_id,
            measurements=measurements,
            observed_at=response.events[-1].occurred_at,
            node_id=node_id,
            workload=workload,
            environment=environment,
        )


__all__ = [
    "RESULT_METRICS_KEY",
    "ExecutionObservationRecorder",
    "measurements_from_dispatch",
    "observation_id_for",
]
