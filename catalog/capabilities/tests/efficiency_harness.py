"""Shared builders for the federation efficiency-learning tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from catalog.capabilities.efficiency import (
    EfficiencyPolicy,
    ExecutionEnvironment,
    ExecutionMeasurements,
    ExecutionObservation,
    SQLiteExecutionLearningStore,
    WorkloadDescriptor,
    observation_id_for,
)
from catalog.capabilities.jobs import (
    CapabilityRequirement,
    JobContract,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.provider_reports import (
    ProviderResourceReport,
    ProviderStatus,
)

NOW = datetime(2026, 8, 14, 9, 0, tzinfo=timezone.utc)
SESSION = "session-efficiency"
CAPABILITY_TYPE = "language-model"
PROTOCOL = "ollama-chat"
PROTOCOL_VERSION = "1.0"


def build_job(
    *,
    job_id: str = "job-001",
    requirements: dict[str, Any] | None = None,
    session_id: str = SESSION,
    capability_type: str = CAPABILITY_TYPE,
    protocol: str = PROTOCOL,
    protocol_version: str = PROTOCOL_VERSION,
) -> JobContract:
    return JobContract(
        job_id=job_id,
        session_id=session_id,
        request_id=f"request-{job_id}",
        idempotency_key=f"{session_id}:{job_id}",
        capability=CapabilityRequirement(
            capability_type=capability_type,
            protocol=protocol,
            protocol_version=protocol_version,
            requirements=(
                {"model": "qwen3:4b", "job_kind": "summarize"}
                if requirements is None
                else requirements
            ),
        ),
        inputs=(),
        outputs=(),
        retry_policy=RetryPolicy(
            max_attempts=2,
            backoff_seconds=(1,),
            retryable_error_codes=("worker-handler-failed",),
        ),
        timeout_policy=TimeoutPolicy(
            overall_timeout_seconds=600,
            queue_timeout_seconds=120,
            start_timeout_seconds=120,
            run_timeout_seconds=600,
            cancellation_grace_seconds=5,
        ),
    )


def build_report(
    *,
    capability_id: str,
    node_id: str,
    now: datetime = NOW,
    session_id: str = SESSION,
    capability_type: str = CAPABILITY_TYPE,
    protocol: str = PROTOCOL,
    protocol_version: str = PROTOCOL_VERSION,
    status: ProviderStatus = ProviderStatus.READY,
    max_concurrent_jobs: int = 4,
    active_jobs: int = 0,
    queue_depth: int = 0,
    utilization_millis: int = 0,
    attributes: dict[str, Any] | None = None,
) -> ProviderResourceReport:
    return ProviderResourceReport(
        capability_id=capability_id,
        node_id=node_id,
        session_id=session_id,
        capability_type=capability_type,
        protocol=protocol,
        protocol_version=protocol_version,
        status=status,
        report_revision=1,
        max_concurrent_jobs=max_concurrent_jobs,
        active_jobs=active_jobs,
        queue_depth=queue_depth,
        utilization_millis=utilization_millis,
        attributes=(
            {"model": "qwen3:4b", "job_kind": "summarize"}
            if attributes is None
            else attributes
        ),
        reported_at=now,
        expires_at=now + timedelta(minutes=5),
    )


def build_workload(
    *,
    capability_type: str = CAPABILITY_TYPE,
    job_kind: str = "summarize",
    features: dict[str, Any] | None = None,
) -> WorkloadDescriptor:
    return WorkloadDescriptor(
        capability_type=capability_type,
        job_kind=job_kind,
        features={"model": "qwen3:4b"} if features is None else features,
    )


def build_observation(
    *,
    node_id: str,
    capability_id: str,
    observed_at: datetime,
    total_millis: int | None = 1_000,
    succeeded: bool = True,
    error_code: str | None = None,
    job_id: str | None = None,
    attempt_id: str = "attempt-1",
    session_id: str = SESSION,
    workload: WorkloadDescriptor | None = None,
    environment: ExecutionEnvironment | None = None,
    measurements: ExecutionMeasurements | None = None,
) -> ExecutionObservation:
    resolved_job_id = job_id or f"job-{node_id}-{int(observed_at.timestamp())}"
    if measurements is None:
        measurements = ExecutionMeasurements(
            succeeded=succeeded,
            error_code=None if succeeded else (error_code or "worker-handler-failed"),
            total_millis=total_millis,
            execution_millis=total_millis,
        )
    return ExecutionObservation(
        observation_id=observation_id_for(
            session_id=session_id, job_id=resolved_job_id, attempt_id=attempt_id
        ),
        session_id=session_id,
        job_id=resolved_job_id,
        attempt_id=attempt_id,
        node_id=node_id,
        capability_id=capability_id,
        workload=workload or build_workload(),
        environment=environment or ExecutionEnvironment(model_id="qwen3:4b"),
        measurements=measurements,
        observed_at=observed_at,
    )


def build_store(
    tmp_path: Path,
    *,
    policy: EfficiencyPolicy | None = None,
    name: str = "efficiency.sqlite3",
    **kwargs: Any,
) -> SQLiteExecutionLearningStore:
    return SQLiteExecutionLearningStore(
        tmp_path / name, policy=policy, **kwargs
    )


__all__ = [
    "CAPABILITY_TYPE",
    "NOW",
    "PROTOCOL",
    "PROTOCOL_VERSION",
    "SESSION",
    "build_job",
    "build_observation",
    "build_report",
    "build_store",
    "build_workload",
]
