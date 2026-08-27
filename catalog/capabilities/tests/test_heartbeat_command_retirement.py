"""Heartbeat command rows outlive every attempt that could ever read them.

``capability_job_heartbeat_commands`` is the only lifecycle table nothing
retires, and it is the one with a row per heartbeat rather than a row per job.
Terminalizing an attempt already drops its heartbeat row and its retry state; the
command rows it wrote stay behind for the life of the device.

Retiring them cannot re-admit anything. The table is read in exactly one place -
``record_heartbeat`` - and only after ``_exact_ownership`` has succeeded, which
requires ``snapshot.ownership`` to be present. ``DurableJobSnapshot`` permits
that only while the job has exactly one non-terminal attempt naming that same
attempt id and lease. So the tests below assert both halves: the rows go, and
every replay that was refused before is refused afterwards with the same code.

These run against the real store, so they describe the consequence and not the
shape of a helper, and this module imports nothing that exists only on this
branch.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.capabilities.jobs import (
    ArtifactReference,
    AttemptStatus,
    CapabilityRequirement,
    JobContract,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.lifecycle_contracts import JobHeartbeat
from catalog.capabilities.lifecycle_store import SQLiteJobLifecycleStore
from catalog.federation.errors import FederationValidationError

NOW = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)
JOB_ID = "job-heartbeat-retirement-1"
SESSION_ID = "session-heartbeat-retirement-1"


def _job() -> JobContract:
    return JobContract(
        job_id=JOB_ID,
        session_id=SESSION_ID,
        request_id="request-heartbeat-retirement-1",
        idempotency_key=f"{SESSION_ID}:request-heartbeat-retirement-1",
        capability=CapabilityRequirement(
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            requirements={"operation": "echo", "modalities": ["json"]},
        ),
        inputs=(),
        outputs=(
            ArtifactReference(
                reference_id="output-heartbeat-retirement-1",
                session_id=SESSION_ID,
                schema_name="fcp.synthetic-output.v1",
                media_type="application/json",
            ),
        ),
        retry_policy=RetryPolicy(
            max_attempts=3,
            backoff_seconds=(2, 5),
            retryable_error_codes=("transient-worker-error",),
        ),
        timeout_policy=TimeoutPolicy(
            overall_timeout_seconds=120,
            queue_timeout_seconds=20,
            start_timeout_seconds=10,
            run_timeout_seconds=30,
            cancellation_grace_seconds=5,
        ),
    )


def _database(tmp_path: Path) -> Path:
    return tmp_path / "jobs.sqlite3"


def _command_rows(database: Path) -> int:
    connection = sqlite3.connect(database)
    try:
        return int(
            connection.execute(
                "SELECT COUNT(*) FROM capability_job_heartbeat_commands WHERE job_id=?",
                (JOB_ID,),
            ).fetchone()[0]
        )
    finally:
        connection.close()


def _claim(
    store: SQLiteJobLifecycleStore,
    *,
    attempt_id: str,
    lease_id: str,
    expected_revision: int,
    now: datetime,
) -> None:
    store.claim(
        JOB_ID,
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id=attempt_id,
        lease_id=lease_id,
        command_id=f"claim-{attempt_id}",
        expected_revision=expected_revision,
        lease_expires_at=now + timedelta(seconds=600),
        now=now,
    )


def _running_attempt(tmp_path: Path) -> SQLiteJobLifecycleStore:
    store = SQLiteJobLifecycleStore(_database(tmp_path))
    job = _job()
    store.submit(job, coordinator_id="coordinator", now=NOW)
    store.queue(
        JOB_ID,
        coordinator_id="coordinator",
        command_id="queue-1",
        expected_revision=0,
        now=NOW,
    )
    _claim(
        store, attempt_id="attempt-1", lease_id="lease-1", expected_revision=1, now=NOW
    )
    store.record_attempt_status(
        JOB_ID,
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-1",
        lease_id="lease-1",
        command_id="accepted-1",
        expected_revision=2,
        target_status=AttemptStatus.ACCEPTED,
        now=NOW + timedelta(seconds=1),
    )
    store.record_attempt_status(
        JOB_ID,
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-1",
        lease_id="lease-1",
        command_id="running-1",
        expected_revision=3,
        target_status=AttemptStatus.RUNNING,
        now=NOW + timedelta(seconds=2),
    )
    return store


def _heartbeat(
    sequence: int,
    *,
    attempt_id: str = "attempt-1",
    lease_id: str = "lease-1",
    lease_generation: int = 1,
) -> JobHeartbeat:
    return JobHeartbeat(
        heartbeat_id=f"heartbeat-{attempt_id}-{sequence}",
        session_id=SESSION_ID,
        worker_node_id="node-a",
        provider_id="provider-a",
        job_id=JOB_ID,
        attempt_id=attempt_id,
        lease_id=lease_id,
        lease_generation=lease_generation,
        sequence=sequence,
        occurred_at=NOW + timedelta(seconds=10 + sequence),
    )


def _beat(store: SQLiteJobLifecycleStore, heartbeat: JobHeartbeat) -> bool:
    return store.record_heartbeat(
        heartbeat,
        coordinator_id="coordinator",
        authenticated_worker_node_id="node-a",
        authenticated_session_id=SESSION_ID,
    )


def _result_reference() -> ArtifactReference:
    return ArtifactReference(
        reference_id="output-heartbeat-retirement-1",
        session_id=SESSION_ID,
        schema_name="fcp.synthetic-output.v1",
        media_type="application/json",
        content_hash="sha256:" + "a" * 64,
        size_bytes=128,
    )


def test_a_committed_result_retires_the_heartbeat_commands_it_outlived(
    tmp_path: Path,
) -> None:
    """One row per heartbeat, kept forever, on a device that keeps running jobs.
    The heartbeat row itself is already dropped here; its command rows were not."""

    store = _running_attempt(tmp_path)
    for sequence in range(1, 6):
        assert _beat(store, _heartbeat(sequence))
    assert _command_rows(_database(tmp_path)) == 5

    store.commit_result(
        JOB_ID,
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-1",
        lease_id="lease-1",
        lease_generation=1,
        command_id="result-1",
        expected_revision=store.snapshot(JOB_ID).revision,
        reference=_result_reference(),
        now=NOW + timedelta(seconds=30),
    )

    assert _command_rows(_database(tmp_path)) == 0


def test_a_cancelled_job_retires_them_too(tmp_path: Path) -> None:
    store = _running_attempt(tmp_path)
    assert _beat(store, _heartbeat(1))
    assert _command_rows(_database(tmp_path)) == 1

    store.request_cancellation(
        JOB_ID,
        coordinator_id="coordinator",
        cancellation_id="cancellation-1",
        command_id="cancel-1",
        expected_revision=store.snapshot(JOB_ID).revision,
        reason="operator stopped the analysis",
        now=NOW + timedelta(seconds=20),
    )
    store.finalize_cancellation(
        JOB_ID,
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-1",
        lease_id="lease-1",
        lease_generation=1,
        command_id="cancel-1:complete",
        expected_revision=store.snapshot(JOB_ID).revision,
        now=NOW + timedelta(seconds=21),
    )

    assert _command_rows(_database(tmp_path)) == 0


def test_a_retried_attempt_retires_only_what_it_can_no_longer_reach(
    tmp_path: Path,
) -> None:
    """The frontier is per attempt, not per job. A retried job is still live, so
    this is the case where retiring too eagerly would re-admit a replay."""

    store = _running_attempt(tmp_path)
    assert _beat(store, _heartbeat(1))

    store.schedule_retry(
        JOB_ID,
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-1",
        lease_id="lease-1",
        lease_generation=1,
        command_id="retry-1",
        expected_revision=store.snapshot(JOB_ID).revision,
        terminal_status=AttemptStatus.FAILED,
        error_code="transient-worker-error",
        now=NOW + timedelta(seconds=20),
    )
    assert _command_rows(_database(tmp_path)) == 0

    store.activate_retry(
        JOB_ID,
        coordinator_id="coordinator",
        command_id="activate-retry-1",
        expected_revision=store.snapshot(JOB_ID).revision,
        now=NOW + timedelta(seconds=30),
    )
    _claim(
        store,
        attempt_id="attempt-2",
        lease_id="lease-2",
        expected_revision=store.snapshot(JOB_ID).revision,
        now=NOW + timedelta(seconds=31),
    )

    # The retired row belonged to attempt-1, and attempt-1 is terminal. Replaying
    # it is refused by the attempt it names, not by the row that is now gone.
    with pytest.raises(FederationValidationError) as replayed:
        _beat(store, _heartbeat(1))
    assert replayed.value.code == "stale-job-attempt"

    # And the live attempt is unaffected.
    live = store.snapshot(JOB_ID).ownership
    assert live is not None
    assert _beat(
        store,
        _heartbeat(
            1,
            attempt_id="attempt-2",
            lease_id="lease-2",
            lease_generation=live.lease_generation,
        ),
    )


def test_a_replay_after_terminalization_is_refused_exactly_as_before(
    tmp_path: Path,
) -> None:
    """The proof that the retirement is invisible. This passes on a tree that
    keeps the rows forever, because the row was never what refused the replay -
    the missing ownership was."""

    store = _running_attempt(tmp_path)
    assert _beat(store, _heartbeat(1))
    store.commit_result(
        JOB_ID,
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-1",
        lease_id="lease-1",
        lease_generation=1,
        command_id="result-1",
        expected_revision=store.snapshot(JOB_ID).revision,
        reference=_result_reference(),
        now=NOW + timedelta(seconds=30),
    )

    with pytest.raises(FederationValidationError) as replayed:
        _beat(store, _heartbeat(1))
    assert replayed.value.code == "job-not-owned"


def test_duplicate_suppression_is_untouched_while_the_attempt_is_live(
    tmp_path: Path,
) -> None:
    """Nothing here may make a live attempt's heartbeats less idempotent."""

    store = _running_attempt(tmp_path)
    heartbeat = _heartbeat(1)

    assert _beat(store, heartbeat) is True
    assert _beat(store, heartbeat) is False

    values = heartbeat.to_dict()
    values.pop("schema")
    values.pop("protocol")
    values["sequence"] = 2
    values["occurred_at"] = NOW + timedelta(seconds=40)
    with pytest.raises(FederationValidationError) as conflict:
        _beat(store, JobHeartbeat(**values))  # type: ignore[arg-type]
    assert conflict.value.code == "heartbeat-id-conflict"

    assert _command_rows(_database(tmp_path)) == 1


def test_authoritative_outcomes_are_not_retired_with_the_liveness(
    tmp_path: Path,
) -> None:
    """Cancellations and committed results are outcomes, not liveness. Bounding
    the store must not reach them."""

    store = _running_attempt(tmp_path)
    assert _beat(store, _heartbeat(1))
    store.commit_result(
        JOB_ID,
        coordinator_id="coordinator",
        owner_provider_id="provider-a",
        attempt_id="attempt-1",
        lease_id="lease-1",
        lease_generation=1,
        command_id="result-1",
        expected_revision=store.snapshot(JOB_ID).revision,
        reference=_result_reference(),
        now=NOW + timedelta(seconds=30),
    )

    committed = store.result_commit(JOB_ID)
    assert committed is not None
    assert committed.attempt_id == "attempt-1"
    assert committed.reference.content_hash == "sha256:" + "a" * 64
