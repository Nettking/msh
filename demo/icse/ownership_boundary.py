"""E4: provider selection is distinct from durable ownership and lease authority."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from catalog.capabilities.job_store import SQLiteJobStore
from catalog.capabilities.jobs import AttemptStatus
from catalog.capabilities.provider_reports import ProviderStatus
from catalog.capabilities.provider_selection import select_provider
from catalog.federation.errors import FederationValidationError
from demo.icse.runtime_eligibility import CAPABILITY_ID, NOW, _job, _report

COORDINATOR_ID = "coordinator-e4"
ATTEMPT_ID = "attempt-e4-1"
LEASE_ID = "lease-e4-1"


def ownership_boundary(root: Path) -> dict[str, object]:
    """Show recommendation, ownership, and lease-authorized progress as separate stages."""

    job = _job()
    selection = select_provider(
        job,
        (_report(status=ProviderStatus.READY),),
        evaluated_at=NOW,
    )
    if selection.decision != "selected" or selection.selected_capability_id != CAPABILITY_ID:
        raise AssertionError("eligible provider was not recommended")

    store = SQLiteJobStore(root / "jobs.sqlite3")
    submitted = store.submit(job, coordinator_id=COORDINATOR_ID, now=NOW)
    if submitted.snapshot.ownership is not None:
        raise AssertionError("provider recommendation incorrectly created ownership")

    queued = store.queue(
        job.job_id,
        coordinator_id=COORDINATOR_ID,
        command_id="queue-e4",
        expected_revision=submitted.snapshot.revision,
        now=NOW,
    )
    if queued.snapshot.ownership is not None:
        raise AssertionError("queued job acquired ownership without an explicit claim")

    lease_expires_at = NOW + timedelta(minutes=5)
    claimed = store.claim(
        job.job_id,
        coordinator_id=COORDINATOR_ID,
        owner_provider_id=CAPABILITY_ID,
        attempt_id=ATTEMPT_ID,
        lease_id=LEASE_ID,
        command_id="claim-e4",
        expected_revision=queued.snapshot.revision,
        lease_expires_at=lease_expires_at,
        now=NOW,
    )
    ownership = claimed.snapshot.ownership
    if ownership is None or ownership.owner_provider_id != CAPABILITY_ID:
        raise AssertionError("explicit coordinator claim did not create ownership")

    rejection_code = None
    try:
        store.record_attempt_status(
            job.job_id,
            coordinator_id=COORDINATOR_ID,
            owner_provider_id=CAPABILITY_ID,
            attempt_id=ATTEMPT_ID,
            lease_id=LEASE_ID,
            command_id="expired-progress-e4",
            expected_revision=claimed.snapshot.revision,
            target_status=AttemptStatus.ACCEPTED,
            now=lease_expires_at + timedelta(seconds=1),
        )
    except FederationValidationError as exc:
        rejection_code = exc.code

    if rejection_code != "ownership-lease-expired":
        raise AssertionError(
            f"expired ownership was not rejected as expected: {rejection_code}"
        )

    final = store.snapshot(job.job_id)
    if final.job.attempts[-1].status is not AttemptStatus.ASSIGNED:
        raise AssertionError("expired progress mutated the durable attempt")

    return {
        "claim": "provider recommendation does not grant ownership or execution progress authority",
        "selection": {
            "decision": selection.decision,
            "provider": selection.selected_capability_id,
            "ownership_after_selection": False,
        },
        "ownership": {
            "created_by_explicit_claim": True,
            "provider": ownership.owner_provider_id,
            "attempt_id": ownership.attempt_id,
            "lease_generation": ownership.lease_generation,
        },
        "expired_lease": {
            "progress_rejected": True,
            "rejection_code": rejection_code,
            "attempt_status_after_rejection": final.job.attempts[-1].status.value,
        },
    }
