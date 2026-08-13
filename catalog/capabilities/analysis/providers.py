"""Provider report sources for analysis scheduling.

Local compute is not a special case. A node that can run analysis publishes a
real provider report describing real capacity, and the same deterministic
selection pass in :mod:`catalog.capabilities.provider_selection` decides whether
it wins. Removing the local provider removes local execution; nothing else in the
pipeline changes.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import timedelta

from ..provider_reports import (
    ProviderResourceReport,
    ProviderStatus,
)
from .contracts import (
    ANALYSIS_CAPABILITY_TYPE,
    ANALYSIS_PROTOCOL,
    ANALYSIS_PROTOCOL_VERSION,
    analysis_provider_attributes,
)

DEFAULT_REPORT_TTL_SECONDS = 300


class LocalAnalysisProviderSource:
    """Publish the local node's own analysis capacity as a provider report."""

    def __init__(
        self,
        *,
        session_id: str,
        node_id: str,
        provider_id: str,
        clock,
        max_concurrent_jobs: int = 1,
        active_jobs: Callable[[], int] | None = None,
        available: Callable[[], bool] | None = None,
        report_ttl_seconds: int = DEFAULT_REPORT_TTL_SECONDS,
        report_revision: int = 1,
    ) -> None:
        self.session_id = session_id
        self.node_id = node_id
        self.provider_id = provider_id
        self.clock = clock
        self.max_concurrent_jobs = max(int(max_concurrent_jobs), 1)
        self._active_jobs = active_jobs or (lambda: 0)
        self._available = available or (lambda: True)
        self.report_ttl_seconds = int(report_ttl_seconds)
        self.report_revision = int(report_revision)

    def reports(
        self,
        *,
        session_id: str,
        capability_type: str,
    ) -> tuple[ProviderResourceReport, ...]:
        if session_id != self.session_id or capability_type != ANALYSIS_CAPABILITY_TYPE:
            return ()
        if not self._available():
            return ()
        now = self.clock()
        active = min(max(int(self._active_jobs()), 0), self.max_concurrent_jobs)
        return (
            ProviderResourceReport(
                capability_id=self.provider_id,
                node_id=self.node_id,
                session_id=self.session_id,
                capability_type=ANALYSIS_CAPABILITY_TYPE,
                protocol=ANALYSIS_PROTOCOL,
                protocol_version=ANALYSIS_PROTOCOL_VERSION,
                status=ProviderStatus.READY,
                report_revision=self.report_revision,
                max_concurrent_jobs=self.max_concurrent_jobs,
                active_jobs=active,
                queue_depth=0,
                utilization_millis=int(
                    1_000 * active / self.max_concurrent_jobs
                ),
                attributes=analysis_provider_attributes(),
                reported_at=now,
                expires_at=now + timedelta(seconds=self.report_ttl_seconds),
            ),
        )


class FederatedProviderReportSource:
    """Adapt the existing provider health authority to analysis scheduling."""

    def __init__(self, health, *, actor_node_id: str) -> None:
        self.health = health
        self.actor_node_id = actor_node_id

    def reports(
        self,
        *,
        session_id: str,
        capability_type: str,
    ) -> tuple[ProviderResourceReport, ...]:
        return tuple(
            self.health.fresh_reports(
                session_id=session_id,
                actor_node_id=self.actor_node_id,
                capability_type=capability_type,
            )
        )


class CompositeProviderReportSource:
    """Merge report sources, keeping one report per capability identity."""

    def __init__(self, sources: Sequence[object]) -> None:
        self.sources = tuple(sources)

    def reports(
        self,
        *,
        session_id: str,
        capability_type: str,
    ) -> tuple[ProviderResourceReport, ...]:
        merged: dict[str, ProviderResourceReport] = {}
        for source in self.sources:
            for report in source.reports(  # type: ignore[attr-defined]
                session_id=session_id,
                capability_type=capability_type,
            ):
                existing = merged.get(report.capability_id)
                if existing is None or existing.report_revision <= report.report_revision:
                    merged[report.capability_id] = report
        return tuple(merged[key] for key in sorted(merged))


__all__ = [
    "CompositeProviderReportSource",
    "FederatedProviderReportSource",
    "LocalAnalysisProviderSource",
]
