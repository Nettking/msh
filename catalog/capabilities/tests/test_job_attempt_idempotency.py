from __future__ import annotations

from catalog.capabilities.jobs import AttemptStatus, JobAttempt


def test_f71_024_idempotent_attempt_transition_preserves_error_identity() -> None:
    failed = JobAttempt(
        attempt_id="attempt-1",
        attempt_number=1,
        status=AttemptStatus.FAILED,
        error_code="worker-lost",
    )

    assert failed.transition_to(AttemptStatus.FAILED) == failed
