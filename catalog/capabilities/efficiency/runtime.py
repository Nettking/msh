"""Production composition for execution-efficiency learning.

The learning engine owns no authority and no transport. This module composes a
session-local store/ranker/recorder and decorates the existing lifecycle
transport so authenticated dispatch responses become observations without
creating a second execution path.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Protocol

from ..dispatch import DispatchRequest, DispatchResponse, DispatchState
from ..lifecycle_contracts import CancellationRequest, CancellationResponse
from ..lifecycle_coordinator import LifecycleTransport
from ..provider_reports import ProviderResourceReport
from .contracts import ExecutionEnvironment
from .estimator import PerformanceEstimator
from .policy import EfficiencyPolicy
from .ranking import LearnedProviderRanker, default_environment
from .recorder import ExecutionObservationRecorder
from .store import SQLiteExecutionLearningStore


class ProviderReportSource(Protocol):
    def reports(
        self, *, session_id: str, capability_type: str
    ) -> tuple[ProviderResourceReport, ...]: ...


class LearningLifecycleTransport:
    """Observe real dispatches while preserving the existing transport contract.

    The delegate remains the only component that sends or authenticates traffic.
    A response is structurally validated against the outgoing request before it
    is considered evidence. Recording is best-effort and can never change the
    job result or make dispatch fail.
    """

    def __init__(
        self,
        delegate: LifecycleTransport,
        *,
        recorder: ExecutionObservationRecorder,
        report_source: ProviderReportSource | None = None,
    ) -> None:
        self.delegate = delegate
        self.recorder = recorder
        self.report_source = report_source

    def _environment(self, request: DispatchRequest) -> ExecutionEnvironment:
        source = self.report_source
        if source is None:
            return ExecutionEnvironment()
        try:
            reports = source.reports(
                session_id=request.session_id,
                capability_type=request.job.capability.capability_type,
            )
        except Exception:  # noqa: BLE001 - environment metadata is optional
            return ExecutionEnvironment()
        for report in reports:
            if (
                report.capability_id == request.provider_id
                and report.node_id == request.target_node_id
            ):
                return default_environment(request.job, report)
        return ExecutionEnvironment()

    async def request(
        self,
        *,
        target_node_id: str,
        request: DispatchRequest,
    ) -> DispatchResponse:
        response = await self.delegate.request(
            target_node_id=target_node_id,
            request=request,
        )
        validated = response.validate_for(request)
        terminal = validated.events[-1]
        if (
            terminal.state
            in {DispatchState.SUCCEEDED, DispatchState.FAILED, DispatchState.REJECTED}
            and terminal.occurred_at < request.lease_expires_at
        ):
            # This is coordinator-observed evidence from the authenticated
            # dispatch path. The deterministic observation id makes a replay a
            # no-op in the store.
            with contextlib.suppress(Exception):
                self.recorder.record_dispatch(
                    request.job,
                    validated,
                    node_id=request.target_node_id,
                    queued_at=request.sent_at,
                    retries=max(0, request.attempt_number - 1),
                    environment=self._environment(request),
                )
        return response

    async def cancel(
        self,
        *,
        target_node_id: str,
        request: CancellationRequest,
    ) -> CancellationResponse:
        return await self.delegate.cancel(
            target_node_id=target_node_id,
            request=request,
        )

    def close(self) -> None:
        close = getattr(self.delegate, "close", None)
        if callable(close):
            close()


class ExecutionEfficiencyRuntime:
    """One session-local learning store and its production adapters."""

    def __init__(
        self,
        database: Path | str,
        *,
        node_id: str,
        policy: EfficiencyPolicy | None = None,
    ) -> None:
        self.policy = policy or EfficiencyPolicy()
        self.store = SQLiteExecutionLearningStore(database, policy=self.policy)
        self.estimator = PerformanceEstimator(self.store, policy=self.policy)
        self.ranker = LearnedProviderRanker(
            self.estimator,
            policy=self.policy,
            decision_sink=self.store.record_decision,
        )
        self.recorder = ExecutionObservationRecorder(self.store, node_id=node_id)

    def wrap_transport(
        self,
        transport: LifecycleTransport,
        *,
        report_source: ProviderReportSource | None = None,
    ) -> LearningLifecycleTransport:
        return LearningLifecycleTransport(
            transport,
            recorder=self.recorder,
            report_source=report_source,
        )


__all__ = [
    "ExecutionEfficiencyRuntime",
    "LearningLifecycleTransport",
    "ProviderReportSource",
]
