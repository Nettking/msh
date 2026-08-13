"""Uploaded JSONL analysis uses the shared federated job path."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from catalog.capabilities.analysis.contracts import (
    ANALYSIS_CAPABILITY_TYPE,
    ORIGIN_AUTOMATIC_DISCOVERY,
    ORIGIN_MANUAL_UPLOAD,
)
from catalog.capabilities.jobs import JobStatus
from catalog.federation.projections.storage_job_adapters import JobAuthorityAdapter
from catalog.flask_app.services.upload_analysis_job_service import (
    UploadAnalysisJobError,
    UploadAnalysisJobService,
)
from catalog.orchestrator.analysis_runtime import (
    AnalysisIdentity,
    AnalysisRuntime,
    DiscoveryAnalysisGateway,
)
from catalog.runner.data_filtering import (
    date_range_source_signature,
    source_date_signatures,
)

NOW = datetime(2026, 8, 13, 9, 0, tzinfo=timezone.utc)
SESSION = "session-upload-tests"


def _runtime(tmp_path: Path) -> AnalysisRuntime:
    return AnalysisRuntime(
        root=tmp_path,
        identity=AnalysisIdentity(
            session_id=SESSION,
            node_id="node-upload-tests",
            provider_id="analysis-provider-upload-tests",
            standalone=True,
        ),
        clock=lambda: NOW,
    )


def _published(tmp_path: Path, batch_id: str, *, day: str = "2026-08-13") -> Path:
    directory = tmp_path / "data" / "uploads" / batch_id
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{day}.jsonl").write_text(
        f'{{"timestamp":"{day}T10:00:00Z","machine":"A"}}\n', encoding="utf-8"
    )
    return directory


def _batch(tmp_path: Path, batch_id: str = "upload-abc123", **overrides):
    payload = {
        "batch_id": batch_id,
        "file_count": 1,
        "imported_records": 1,
        "total_bytes": 64,
        "published_path": str(_published(tmp_path, batch_id)),
    }
    payload.update(overrides)
    return payload


def _service(tmp_path: Path, runtime: AnalysisRuntime) -> UploadAnalysisJobService:
    return UploadAnalysisJobService(
        database=tmp_path / "imports" / "uploads.sqlite3",
        gateway=DiscoveryAnalysisGateway(runtime),
        data_dir=tmp_path / "data",
        clock=lambda: NOW,
        script_keys=("data_pr_day",),
    )


def test_uploaded_batch_becomes_a_durable_queued_job(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    service = _service(tmp_path, runtime)

    job_id = service.submit_batch(_batch(tmp_path))
    snapshot = runtime.store.snapshot(job_id)

    assert snapshot.job.status is JobStatus.QUEUED
    assert snapshot.job.capability.capability_type == ANALYSIS_CAPABILITY_TYPE
    assert service.job_ids(SESSION) == (job_id,)


def test_no_synthetic_local_worker_claims_the_job(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    service = _service(tmp_path, runtime)

    job_id = service.submit_batch(_batch(tmp_path))
    snapshot = runtime.store.snapshot(job_id)

    # Submission never grants ownership: a provider must be selected first.
    assert snapshot.ownership is None
    assert snapshot.job.attempts == ()
    assert not hasattr(service, "_worker_id")


def test_upload_and_discovery_converge_on_one_durable_job(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    service = _service(tmp_path, runtime)
    data_dir = tmp_path / "data"
    batch = _batch(tmp_path, "upload-shared")
    day = date(2026, 8, 13)
    signature = date_range_source_signature(
        source_date_signatures(data_dir), day, day
    )

    discovered = DiscoveryAnalysisGateway(runtime).submit_date_slice(
        data_dir=data_dir,
        target_day=day,
        script_keys=("data_pr_day",),
        runtime_namespace="default",
        source_signature=signature,
        origin=ORIGIN_AUTOMATIC_DISCOVERY,
    )
    uploaded = service.submit_batch(batch)

    assert discovered.job_id == uploaded
    assert len(runtime.service.registry.records()) == 1
    assert runtime.service.registry.records()[0].origin == ORIGIN_AUTOMATIC_DISCOVERY


def test_uploaded_job_is_eligible_for_federation_dispatch(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    service = _service(tmp_path, runtime)
    executed: list[object] = []

    class _Executor:
        def execute(self, *, plan, data_dir, workspace):
            from catalog.capabilities.analysis import AnalysisExecutionReport

            executed.append(plan)
            return AnalysisExecutionReport(
                succeeded=True, processed_dates=plan.target_dates
            )

    runtime.transport.local_workers[
        runtime.identity.provider_id
    ].handler.executor = _Executor()
    job_id = service.submit_batch(_batch(tmp_path))

    outcomes = runtime.service.run_scheduling_pass()

    assert [item.decision for item in outcomes] == ["dispatched"]
    assert runtime.store.snapshot(job_id).job.status is JobStatus.SUCCEEDED
    assert len(executed) == 1


def test_batch_without_published_files_fails_closed(tmp_path: Path) -> None:
    service = _service(tmp_path, _runtime(tmp_path))

    with pytest.raises(UploadAnalysisJobError) as error:
        service.submit_batch({"batch_id": "upload-missing", "published_path": None})
    assert error.value.code == "analysis-batch-not-published"


def test_batch_without_analysable_dates_fails_closed(tmp_path: Path) -> None:
    service = _service(tmp_path, _runtime(tmp_path))
    empty = tmp_path / "data" / "uploads" / "upload-empty"
    empty.mkdir(parents=True)

    with pytest.raises(UploadAnalysisJobError) as error:
        service.submit_batch(
            {"batch_id": "upload-empty", "published_path": str(empty)}
        )
    assert error.value.code == "analysis-batch-undated"


def test_unidentified_batch_is_refused(tmp_path: Path) -> None:
    service = _service(tmp_path, _runtime(tmp_path))

    with pytest.raises(UploadAnalysisJobError) as error:
        service.submit_batch({"batch_id": "  "})
    assert error.value.code == "analysis-batch-required"


def test_route_failure_never_terminalizes_distributed_work(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    service = _service(tmp_path, runtime)
    job_id = service.submit_batch(_batch(tmp_path))

    service.fail(job_id, "upload-not-ready")

    assert runtime.store.snapshot(job_id).job.status is JobStatus.QUEUED


def test_restart_preserves_the_queued_upload_job(tmp_path: Path) -> None:
    original = _service(tmp_path, _runtime(tmp_path))
    job_id = original.submit_batch(_batch(tmp_path, "upload-interrupted"))

    restarted_runtime = _runtime(tmp_path)
    restarted = _service(tmp_path, restarted_runtime)

    assert restarted_runtime.store.snapshot(job_id).job.status is JobStatus.QUEUED
    assert restarted.job_ids(SESSION) == (job_id,)


def test_job_projection_still_renders_for_the_session(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    service = _service(tmp_path, runtime)
    job_id = service.submit_batch(_batch(tmp_path))

    projected = JobAuthorityAdapter(lambda: service.snapshots(SESSION)).snapshot()

    assert projected.available is True
    assert [item.job_id for item in projected.jobs] == [job_id]
    assert projected.jobs[0].capability_type == ANALYSIS_CAPABILITY_TYPE
    assert projected.jobs[0].status == "queued"
    assert service.snapshots("different-federation-session") == ()


def test_manual_upload_origin_is_recorded_for_product_views(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    service = _service(tmp_path, runtime)

    job_id = service.submit_batch(_batch(tmp_path))
    record = runtime.service.registry.record_for(job_id)

    assert record is not None
    assert record.origin == ORIGIN_MANUAL_UPLOAD
    assert record.target_dates == ("2026-08-13",)
