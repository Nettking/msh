"""Deterministic provider eligibility and ranking for capability jobs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from catalog.federation.errors import FederationValidationError

from .jobs import JobContract
from .provider_reports import (
    ProviderResourceReport,
    ProviderSelectionPolicy,
    ProviderStatus,
    _array,
    _canonical,
    _utc,
    provider_protocol_satisfies,
)


def _requirement_reasons(
    required: Any,
    offered: Any,
    *,
    path: str = "requirements",
) -> tuple[str, ...]:
    if isinstance(required, dict):
        if not isinstance(offered, dict):
            return (f"requirement-type-mismatch:{path}",)
        reasons: list[str] = []
        for key in sorted(required):
            child_path = f"{path}.{key}"
            if key not in offered:
                reasons.append(f"requirement-missing:{child_path}")
            else:
                reasons.extend(
                    _requirement_reasons(
                        required[key], offered[key], path=child_path
                    )
                )
        return tuple(reasons)
    if isinstance(required, list):
        if not isinstance(offered, (list, tuple)):
            return (f"requirement-type-mismatch:{path}",)
        offered_values = {_canonical(item) for item in offered}
        missing = [
            item for item in required if _canonical(item) not in offered_values
        ]
        return () if not missing else (f"requirement-mismatch:{path}",)
    if _canonical(required) == _canonical(offered):
        return ()
    return (f"requirement-mismatch:{path}",)


@dataclass(frozen=True)
class ProviderCandidate:
    capability_id: str
    report: ProviderResourceReport
    eligible: bool
    reasons: tuple[str, ...]
    rank: tuple[Any, ...]


@dataclass(frozen=True)
class ProviderSelection:
    decision: str
    selected_capability_id: str | None
    selected_node_id: str | None
    eligible_capability_ids: tuple[str, ...]
    rejected: dict[str, tuple[str, ...]]
    reason: str
    evaluated_at: datetime


def evaluate_provider_candidate(
    job: JobContract,
    report: ProviderResourceReport,
    *,
    evaluated_at: datetime,
    policy: ProviderSelectionPolicy | None = None,
) -> ProviderCandidate:
    """Evaluate one provider without reserving it or mutating the job."""

    if not isinstance(job, JobContract):
        raise FederationValidationError(
            "invalid-job", "job", "must be a JobContract"
        )
    if not isinstance(report, ProviderResourceReport):
        raise FederationValidationError(
            "invalid-provider-report",
            "report",
            "must be a ProviderResourceReport",
        )
    policy = policy or ProviderSelectionPolicy()
    if not isinstance(policy, ProviderSelectionPolicy):
        raise FederationValidationError(
            "invalid-selection-policy",
            "policy",
            "must be a ProviderSelectionPolicy",
        )
    observed_at = _utc(evaluated_at, "evaluated_at")
    reasons: list[str] = []
    requirement = job.capability

    if report.capability_id in policy.excluded_capability_ids:
        reasons.append("provider-excluded")
    if report.session_id != job.session_id:
        reasons.append("session-mismatch")
    if report.status != ProviderStatus.READY:
        reasons.append(f"provider-status-{report.status.value}")
    if observed_at < report.reported_at:
        reasons.append("provider-report-from-future")
    elif observed_at >= report.expires_at:
        reasons.append("provider-report-expired")
    if report.capability_type != requirement.capability_type:
        reasons.append("capability-type-mismatch")
    if report.protocol != requirement.protocol:
        reasons.append("capability-protocol-mismatch")
    elif not provider_protocol_satisfies(
        requirement.protocol_version,
        report.protocol_version,
    ):
        reasons.append("capability-protocol-version-incompatible")
    reasons.extend(
        _requirement_reasons(requirement.requirements, report.attributes)
    )
    if report.available_slots < policy.minimum_available_slots:
        reasons.append("insufficient-available-slots")
    if report.queue_depth > policy.max_queue_depth:
        reasons.append("provider-queue-limit-exceeded")
    if report.utilization_millis > policy.max_utilization_millis:
        reasons.append("provider-utilization-limit-exceeded")

    unique_reasons = tuple(dict.fromkeys(reasons))
    eligible = not unique_reasons
    preferred_rank = (
        0
        if policy.preferred_node_id is None
        or report.node_id == policy.preferred_node_id
        else 1
    )
    rank = (
        0 if eligible else 1,
        preferred_rank,
        -report.available_slots,
        report.queue_depth,
        report.utilization_millis,
        report.active_jobs,
        report.capability_id,
    )
    return ProviderCandidate(
        capability_id=report.capability_id,
        report=report,
        eligible=eligible,
        reasons=("eligible",) if eligible else unique_reasons,
        rank=rank,
    )


def select_provider(
    job: JobContract,
    reports: tuple[ProviderResourceReport, ...],
    *,
    evaluated_at: datetime,
    policy: ProviderSelectionPolicy | None = None,
) -> ProviderSelection:
    """Return a deterministic recommendation, never an ownership grant."""

    policy = policy or ProviderSelectionPolicy()
    report_values = tuple(_array(reports, "reports"))
    if any(
        not isinstance(report, ProviderResourceReport)
        for report in report_values
    ):
        raise FederationValidationError(
            "invalid-provider-report",
            "reports",
            "must contain provider reports",
        )
    identities = [report.capability_id for report in report_values]
    if len(identities) != len(set(identities)):
        raise FederationValidationError(
            "duplicate-provider-report",
            "reports",
            "must contain at most one report per capability_id",
        )
    observed_at = _utc(evaluated_at, "evaluated_at")
    candidates: list[ProviderCandidate] = []
    rejected: dict[str, tuple[str, ...]] = {}
    for report in sorted(
        report_values, key=lambda value: value.capability_id
    ):
        candidate = evaluate_provider_candidate(
            job,
            report,
            evaluated_at=observed_at,
            policy=policy,
        )
        if candidate.eligible:
            candidates.append(candidate)
        else:
            rejected[candidate.capability_id] = candidate.reasons
    candidates.sort(key=lambda candidate: candidate.rank)
    eligible_ids = tuple(candidate.capability_id for candidate in candidates)
    if not candidates:
        return ProviderSelection(
            decision="no-eligible-provider",
            selected_capability_id=None,
            selected_node_id=None,
            eligible_capability_ids=(),
            rejected=rejected,
            reason="all-provider-reports-rejected",
            evaluated_at=observed_at,
        )
    selected = candidates[0]
    preferred = (
        policy.preferred_node_id is not None
        and selected.report.node_id == policy.preferred_node_id
    )
    return ProviderSelection(
        decision="selected",
        selected_capability_id=selected.capability_id,
        selected_node_id=selected.report.node_id,
        eligible_capability_ids=eligible_ids,
        rejected=rejected,
        reason=(
            "preferred-node-provider-selected"
            if preferred
            else "highest-ranked-eligible-provider"
        ),
        evaluated_at=observed_at,
    )
