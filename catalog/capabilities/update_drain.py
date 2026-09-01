"""Workload-drain primitives for rolling Federation software updates.

An update drain is a scheduling fence, not a cancellation mechanism.  The node
continues to own and execute work it already holds while its providers become
ineligible for new selection.  Replacement may begin only after durable job
ownership proves that every provider identity belonging to the node is
quiescent.

This module intentionally does not own update orchestration or activation.  It
provides the small domain boundary those layers need: convert READY reports to
DRAINING without changing live-work metrics, and query the existing F7 durable
job store for active ownership held by a bounded set of provider identities.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from catalog.federation.errors import FederationValidationError

from .job_store import SQLiteJobStore
from .provider_reports import ProviderResourceReport, ProviderStatus

MAX_DRAIN_PROVIDER_IDS = 128
_MAX_TEXT_BYTES = 512


def _text(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-text",
            field,
            "must be non-empty trimmed text without control characters",
        )
    if len(value.encode("utf-8")) > _MAX_TEXT_BYTES:
        raise FederationValidationError(
            "text-too-large",
            field,
            f"must not exceed {_MAX_TEXT_BYTES} UTF-8 bytes",
        )
    return value


@dataclass(frozen=True)
class NodeUpdateDrainTarget:
    """The durable provider identities that must quiesce before one node switches."""

    node_id: str
    provider_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "node_id", _text(self.node_id, "node_id"))
        provider_ids = tuple(
            _text(value, f"provider_ids[{index}]")
            for index, value in enumerate(self.provider_ids)
        )
        if len(provider_ids) > MAX_DRAIN_PROVIDER_IDS:
            raise FederationValidationError(
                "too-many-drain-providers",
                "provider_ids",
                f"must not exceed {MAX_DRAIN_PROVIDER_IDS} provider identities",
            )
        if len(provider_ids) != len(set(provider_ids)):
            raise FederationValidationError(
                "duplicate-drain-provider",
                "provider_ids",
                "must not contain duplicate provider identities",
            )
        object.__setattr__(self, "provider_ids", tuple(sorted(provider_ids)))

    def apply_to_report(self, report: ProviderResourceReport) -> ProviderResourceReport:
        """Fence new scheduling while preserving the provider's live-work metrics.

        Only an actually READY provider becomes DRAINING.  An unavailable,
        disabled or revoked provider must keep its stronger state instead of
        being made to look merely drained.  Reports outside this node/provider
        set are returned unchanged.
        """

        if not isinstance(report, ProviderResourceReport):
            raise FederationValidationError(
                "invalid-provider-report",
                "report",
                "must be a ProviderResourceReport",
            )
        if report.node_id != self.node_id or report.capability_id not in self.provider_ids:
            return report
        if report.status is ProviderStatus.DRAINING:
            return report
        if report.status is not ProviderStatus.READY:
            return report
        return replace(report, status=ProviderStatus.DRAINING)

    def active_ownership_count(self, store: SQLiteJobStore) -> int:
        """Count active F7 ownership for this node's providers from durable state.

        This is deliberately a single bounded aggregate query.  It does not
        enumerate job history, trust worker-local counters or infer quiescence
        from provider-health freshness.  A stale/missing health report therefore
        cannot hide a lease that still exists in the authoritative job store.
        """

        if not isinstance(store, SQLiteJobStore):
            raise FederationValidationError(
                "invalid-job-store",
                "store",
                "must be a SQLiteJobStore",
            )
        if not self.provider_ids:
            return 0
        placeholders = ",".join("?" for _ in self.provider_ids)
        with store._connect() as connection:  # package-internal read-only seam
            row = connection.execute(
                f"""SELECT COUNT(*) AS active_count
                    FROM capability_jobs
                    WHERE active_attempt_id IS NOT NULL
                      AND active_owner_provider_id IN ({placeholders})""",
                self.provider_ids,
            ).fetchone()
        assert row is not None
        return int(row["active_count"])

    def is_quiescent(self, store: SQLiteJobStore) -> bool:
        """Return true only when no durable active attempt belongs to this node."""

        return self.active_ownership_count(store) == 0


__all__ = ["MAX_DRAIN_PROVIDER_IDS", "NodeUpdateDrainTarget"]
