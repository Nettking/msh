from __future__ import annotations

import time
from pathlib import Path

from catalog.capabilities.jobs import AttemptStatus, JobStatus
from catalog.federation.projections.storage_job_adapters import JobAuthorityAdapter
from catalog.flask_app.services.upload_analysis_job_service import (
    UploadAnalysisJobService,
)


class _Runtime:
    def __init__(self) -> None:
        self.state = {
            "update_running": False,
            "last_update_check_at": "2026-08-06T10:00:00Z",
            "last_successful_refresh": None,
            "last_failure": None,
            "failed_scripts": [],
            "current_processing_phase": "polling_new_data",
            "completed_execution_id": None,
            "completed_execution_succeeded": None,
        }

    def state_snapshot(self) -> dict[str, object]:
        return dict(self.state)


def _batch(batch_id: str = "upload-abc123") -> dict[str, object]:
    return {
        "batch_id": batch_id,
        "file_count": 2,
        "imported_records": 7,
        "total_bytes": 512,
    }


def _service(
    tmp_path: Path,
    runtime: _Runtime,
    *,
    session_id: str = "session-upload-tests",
    coordinator_id: str = "node-upload-tests",
) -> UploadAnalysisJobService:
    return UploadAnalysisJobService(
        database=tmp_path / "uploads.sqlite3",
        runtime_manager=runtime,
        context_supplier=lambda: (session_id, coordinator_id),
        poll_seconds=0.01,
        monitor_timeout_seconds=5,
    )


def _wait_for_terminal(
    service: UploadAnalysisJobService,
    job_id: str,
) -> object:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        snapshot = service.store.snapshot(job_id)
        if snapshot.job.terminal:
            return snapshot
        time.sleep(0.01)
    raise AssertionError("analysis job did not reach a terminal state")


def test_upload_analysis_job_moves_from_queued_to_active_to_succeeded(
    tmp_path: Path,
) -> None:
    runtime = _Runtime()
    service = _service(tmp_path, runtime)

    job_id = service.submit_batch(_batch())
    queued = service.store.snapshot(job_id)

    assert queued.job.status is JobStatus.QUEUED
    assert queued.ownership is None
    assert service.snapshots("other-session") == ()

    service.start_tracking(job_id)
    active = service.store.snapshot(job_id)

    assert active.job.status is JobStatus.ACTIVE
    assert active.job.attempts[-1].status is AttemptStatus.RUNNING
    assert active.ownership is not None

    runtime.state.update(
        {
            "update_running": False,
            "completed_execution_id": job_id,
            "completed_execution_succeeded": True,
        }
    )
    completed = _wait_for_terminal(service, job_id)

    assert completed.job.status is JobStatus.SUCCEEDED
    assert completed.job.attempts[-1].status is AttemptStatus.SUCCEEDED
    assert completed.ownership is None

    projected = JobAuthorityAdapter(
        lambda: service.snapshots("session-upload-tests")
    ).snapshot()
    assert projected.available is True
    assert len(projected.jobs) == 1
    assert projected.jobs[0].job_id == job_id
    assert projected.jobs[0].capability_type == "background-analysis"
    assert projected.jobs[0].status == "succeeded"
    assert projected.jobs[0].attempt_count == 1


def test_runtime_failure_terminalizes_upload_analysis_job(
    tmp_path: Path,
) -> None:
    runtime = _Runtime()
    service = _service(tmp_path, runtime)
    job_id = service.submit_batch(_batch("upload-failure"))
    service.start_tracking(job_id)

    runtime.state.update(
        {
            "update_running": False,
            "completed_execution_id": job_id,
            "completed_execution_succeeded": False,
        }
    )
    completed = _wait_for_terminal(service, job_id)

    assert completed.job.status is JobStatus.FAILED
    assert completed.job.attempts[-1].status is AttemptStatus.FAILED
    assert completed.job.attempts[-1].error_code == "analysis-step-failed"


def test_unrelated_runtime_completion_cannot_complete_job(tmp_path: Path) -> None:
    runtime = _Runtime()
    service = _service(tmp_path, runtime)
    job_id = service.submit_batch(_batch("upload-owned"))
    service.start_tracking(job_id)

    runtime.state.update(
        {
            "completed_execution_id": "analysis-some-other-upload",
            "completed_execution_succeeded": True,
        }
    )
    time.sleep(0.05)
    assert service.store.snapshot(job_id).job.status is JobStatus.ACTIVE

    runtime.state.update(
        {"completed_execution_id": job_id, "completed_execution_succeeded": True}
    )
    assert _wait_for_terminal(service, job_id).job.status is JobStatus.SUCCEEDED


def test_restart_fails_interrupted_job_and_preserves_session_filter(
    tmp_path: Path,
) -> None:
    runtime = _Runtime()
    original = _service(tmp_path, runtime)
    job_id = original.submit_batch(_batch("upload-interrupted"))
    assert original.store.snapshot(job_id).job.status is JobStatus.QUEUED

    restarted = _service(tmp_path, runtime)
    recovered = restarted.store.snapshot(job_id)

    assert recovered.job.status is JobStatus.FAILED
    assert recovered.job.attempts[-1].error_code == "analysis-interrupted"
    assert len(restarted.snapshots("session-upload-tests")) == 1
    assert restarted.snapshots("different-federation-session") == ()
