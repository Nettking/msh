"""E3: durable provider knowledge is distinct from short-lived runtime eligibility."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from catalog.capabilities.analysis.contracts import (
    analysis_capability_requirement,
    analysis_provider_attributes,
    analysis_retry_policy,
    analysis_timeout_policy,
)
from catalog.capabilities.jobs import JobContract
from catalog.capabilities.provider_reports import ProviderResourceReport, ProviderStatus
from catalog.capabilities.provider_selection import select_provider

NOW = datetime(2026, 8, 31, 10, 0, tzinfo=timezone.utc)
SESSION_ID = "session-icse-runtime"
CAPABILITY_ID = "compute-provider-a"
NODE_ID = "node-provider-a"


def _job() -> JobContract:
    return JobContract(
        job_id="job-e3-runtime-eligibility",
        session_id=SESSION_ID,
        request_id="request-e3-runtime-eligibility",
        idempotency_key="idempotency-e3-runtime-eligibility",
        capability=analysis_capability_requirement(),
        inputs=(),
        outputs=(),
        retry_policy=analysis_retry_policy(),
        timeout_policy=analysis_timeout_policy(),
    )


def _report(
    *,
    status: ProviderStatus,
    reported_at: datetime = NOW,
    expires_at: datetime | None = None,
) -> ProviderResourceReport:
    requirement = analysis_capability_requirement()
    return ProviderResourceReport(
        capability_id=CAPABILITY_ID,
        node_id=NODE_ID,
        session_id=SESSION_ID,
        capability_type=requirement.capability_type,
        protocol=requirement.protocol,
        protocol_version=requirement.protocol_version,
        status=status,
        report_revision=1,
        max_concurrent_jobs=2,
        active_jobs=0,
        queue_depth=0,
        utilization_millis=100,
        attributes=analysis_provider_attributes(),
        reported_at=reported_at,
        expires_at=expires_at or (reported_at + timedelta(minutes=5)),
    )


def runtime_eligibility(_root=None) -> dict[str, object]:
    """Show that the same provider identity can move in and out of eligibility."""

    job = _job()
    ready = select_provider(job, (_report(status=ProviderStatus.READY),), evaluated_at=NOW)
    draining = select_provider(
        job,
        (_report(status=ProviderStatus.DRAINING),),
        evaluated_at=NOW,
    )
    expired = select_provider(
        job,
        (
            _report(
                status=ProviderStatus.READY,
                reported_at=NOW - timedelta(minutes=10),
                expires_at=NOW - timedelta(minutes=5),
            ),
        ),
        evaluated_at=NOW,
    )

    if ready.decision != "selected" or ready.selected_capability_id != CAPABILITY_ID:
        raise AssertionError("ready provider was not selected")
    if draining.decision != "no-eligible-provider":
        raise AssertionError("draining provider remained eligible")
    if "provider-status-draining" not in draining.rejected.get(CAPABILITY_ID, ()):
        raise AssertionError("draining rejection reason was not preserved")
    if expired.decision != "no-eligible-provider":
        raise AssertionError("expired provider report remained eligible")
    if "provider-report-expired" not in expired.rejected.get(CAPABILITY_ID, ()):
        raise AssertionError("expiry rejection reason was not preserved")

    return {
        "claim": "provider identity is distinct from current runtime eligibility",
        "provider_identity_constant": CAPABILITY_ID,
        "observations": {
            "ready": {
                "decision": ready.decision,
                "selected": ready.selected_capability_id,
            },
            "draining": {
                "decision": draining.decision,
                "reasons": list(draining.rejected[CAPABILITY_ID]),
            },
            "expired": {
                "decision": expired.decision,
                "reasons": list(expired.rejected[CAPABILITY_ID]),
            },
        },
    }
