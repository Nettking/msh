from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from catalog.capabilities.analysis.content_store import LocalArtifactContentStore
from catalog.capabilities.analysis.contracts import (
    MAX_PLAN_BYTES,
    ORIGIN_AUTOMATIC_DISCOVERY,
    SLICE_KIND_DATE,
    AnalysisWorkSlice,
    build_analysis_job,
)
from catalog.capabilities.analysis.packaging import MAX_SLICE_ENTRIES
from catalog.capabilities.analysis.resource_admission import (
    FederatedAnalysisHandler,
    analysis_workspace_resource_requirement,
)
from catalog.capabilities.analysis.worker import (
    FederatedAnalysisHandler as _BaseHandler,
)
from catalog.capabilities.dispatch import ExecutionResult
from catalog.capabilities.jobs import AttemptStatus, JobAttempt, JobStatus
from catalog.federation.host_resources import (
    FilesystemMeasurement,
    PressureThresholds,
    ProcessResourceAdmission,
)
from catalog.federation.object_transfer import MAX_TRANSFER_CHUNKS

NOW = datetime(2026, 8, 28, 13, 0, tzinfo=timezone.utc)


def _measurement(resource_id: str, *, free_bytes: int) -> FilesystemMeasurement:
    return FilesystemMeasurement(
        resource_id=resource_id,
        observed_at=NOW,
        total_bytes=100_000,
        free_bytes=free_bytes,
        total_inodes=None,
        free_inodes=None,
        available=True,
    )


def _admission(measurer: Any) -> ProcessResourceAdmission:
    return ProcessResourceAdmission(
        thresholds=PressureThresholds(
            critical_free_bytes=100,
            pressure_free_bytes=200,
            warning_free_bytes=300,
            critical_free_inodes=0,
            pressure_free_inodes=0,
            warning_free_inodes=0,
            max_measurement_age_seconds=60,
        ),
        measurer=measurer,
        clock=lambda: NOW,
    )


def _job(*, plan_size: int = 1, slice_size: int = 1):
    work = AnalysisWorkSlice(
        session_id="session-resource-admission",
        slice_kind=SLICE_KIND_DATE,
        slice_key="2026-08-28",
        target_dates=("2026-08-28",),
        script_keys=("data_pr_day",),
        runtime_namespace="default",
        source_signature="a" * 64,
        origin=ORIGIN_AUTOMATIC_DISCOVERY,
    )
    job = build_analysis_job(
        work,
        plan_hash="sha256:" + ("1" * 64),
        plan_size=plan_size,
        slice_hash="sha256:" + ("2" * 64),
        slice_size=slice_size,
    )
    return replace(
        job,
        status=JobStatus.ACTIVE,
        attempts=(JobAttempt("attempt-1", 1, AttemptStatus.ASSIGNED),),
    )


def _handler(
    workspace_root: Path,
    admission: ProcessResourceAdmission,
    *,
    max_slice_bytes: int = 1,
) -> FederatedAnalysisHandler:
    return FederatedAnalysisHandler(
        session_id="session-resource-admission",
        node_id="node-worker",
        provider_id="provider-worker",
        artifact_transport=None,
        executor=None,
        workspace_root=workspace_root,
        content_store=None,
        clock=lambda: NOW,
        data_owner_node_id=lambda _job: "node-owner",
        max_slice_bytes=max_slice_bytes,
        resource_admission=admission,
    )


def test_workspace_requirement_covers_both_publication_peaks() -> None:
    plan_dominant_bytes, plan_dominant_inodes = analysis_workspace_resource_requirement(512)
    assert plan_dominant_bytes == 2 * MAX_PLAN_BYTES
    assert plan_dominant_inodes == MAX_PLAN_BYTES + MAX_SLICE_ENTRIES + 16

    slice_dominant_bytes, _ = analysis_workspace_resource_requirement(
        512,
        plan_bytes=1,
    )
    assert slice_dominant_bytes == 1 + (2 * 512)

    _, maximum_inodes = analysis_workspace_resource_requirement(
        MAX_TRANSFER_CHUNKS + 1,
        plan_bytes=1,
    )
    assert maximum_inodes == MAX_TRANSFER_CHUNKS + MAX_SLICE_ENTRIES + 16


def test_resource_pressure_refuses_before_workspace_writer_starts(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspaces"
    admission = _admission(lambda _path: _measurement("data", free_bytes=102))
    handler = _handler(workspace_root, admission)

    result = asyncio.run(handler.execute(_job()))

    assert result.succeeded is False
    assert result.reason_code == "analysis-resource-pressure"
    assert not workspace_root.exists()


def test_declared_slice_validation_precedes_resource_admission(tmp_path: Path) -> None:
    def must_not_measure(_path):
        raise AssertionError("resource measurement must follow pure job validation")

    handler = _handler(
        tmp_path / "workspaces",
        _admission(must_not_measure),
        max_slice_bytes=1,
    )

    result = asyncio.run(handler.execute(_job(slice_size=2)))

    assert result.succeeded is False
    assert result.reason_code == "analysis-slice-too-large"


def test_concurrent_attempts_cannot_double_spend_one_workspace_resource(
    tmp_path: Path, monkeypatch,
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    admission = _admission(lambda _path: _measurement("data", free_bytes=203))
    first = _handler(tmp_path / "workspaces-a", admission)
    second = _handler(tmp_path / "workspaces-b", admission)

    async def admitted_execute(_self, job):
        entered.set()
        await release.wait()
        return ExecutionResult(True, {"job_id": job.job_id})

    monkeypatch.setattr(_BaseHandler, "_execute", admitted_execute)

    async def scenario() -> tuple[ExecutionResult, ExecutionResult]:
        first_task = asyncio.create_task(first.execute(_job()))
        await asyncio.wait_for(entered.wait(), timeout=5)
        second_result = await second.execute(_job())
        release.set()
        first_result = await asyncio.wait_for(first_task, timeout=5)
        return first_result, second_result

    first_result, second_result = asyncio.run(scenario())

    assert first_result.succeeded is True
    assert second_result.succeeded is False
    assert second_result.reason_code == "analysis-resource-pressure"


def test_reservation_releases_after_materialization_failure(tmp_path: Path, monkeypatch) -> None:
    admission = _admission(lambda _path: _measurement("data", free_bytes=203))
    handler = _handler(tmp_path / "workspaces", admission)
    calls = 0

    async def fail_once(_self, job):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("simulated materialization failure")
        return ExecutionResult(True, {"job_id": job.job_id})

    monkeypatch.setattr(_BaseHandler, "_execute", fail_once)

    first = asyncio.run(handler.execute(_job()))
    second = asyncio.run(handler.execute(_job()))

    assert first.succeeded is False
    assert first.reason_code == "analysis-input-unavailable"
    assert second.succeeded is True
    assert calls == 2


def test_distinct_workspace_resources_have_independent_envelopes(
    tmp_path: Path, monkeypatch,
) -> None:
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"

    def measure(path: Path | str) -> FilesystemMeasurement:
        text = str(path)
        resource_id = "a" if text.endswith("a") else "b"
        return _measurement(resource_id, free_bytes=203)

    admission = _admission(measure)
    first = _handler(root_a, admission)
    second = _handler(root_b, admission)
    both_entered = asyncio.Event()
    release = asyncio.Event()
    entered = 0

    async def admitted_execute(_self, job):
        nonlocal entered
        entered += 1
        if entered == 2:
            both_entered.set()
        await release.wait()
        return ExecutionResult(True, {"job_id": job.job_id})

    monkeypatch.setattr(_BaseHandler, "_execute", admitted_execute)

    async def scenario() -> tuple[ExecutionResult, ExecutionResult]:
        first_task = asyncio.create_task(first.execute(_job()))
        second_task = asyncio.create_task(second.execute(_job()))
        await asyncio.wait_for(both_entered.wait(), timeout=5)
        release.set()
        return await asyncio.gather(first_task, second_task)

    first_result, second_result = asyncio.run(scenario())

    assert first_result.succeeded is True
    assert second_result.succeeded is True


def test_result_publication_pressure_refuses_before_executor_side_effect(
    tmp_path: Path, monkeypatch
) -> None:
    admission = _admission(lambda _path: _measurement("data", free_bytes=102))
    store = LocalArtifactContentStore(tmp_path / "artifacts")
    handler = FederatedAnalysisHandler(
        session_id="session-resource-admission",
        node_id="node-worker",
        provider_id="provider-worker",
        artifact_transport=None,
        executor=None,
        workspace_root=tmp_path / "workspaces",
        content_store=store,
        clock=lambda: NOW,
        data_owner_node_id=lambda _job: "node-owner",
        resource_admission=admission,
    )
    called = False

    async def must_not_execute(_self, _job):
        nonlocal called
        called = True
        return ExecutionResult(True, {})

    monkeypatch.setattr(_BaseHandler, "_execute", must_not_execute)

    result = asyncio.run(handler.execute(_job()))

    assert result.succeeded is False
    assert result.reason_code == "analysis-resource-pressure"
    assert called is False
    assert list(store.root.rglob("*.partial")) == []


def test_result_reservation_unwinds_after_executor_exception(
    tmp_path: Path, monkeypatch
) -> None:
    admission = _admission(lambda _path: _measurement("data", free_bytes=1_000_000))
    store = LocalArtifactContentStore(tmp_path / "artifacts")
    handler = FederatedAnalysisHandler(
        session_id="session-resource-admission",
        node_id="node-worker",
        provider_id="provider-worker",
        artifact_transport=None,
        executor=None,
        workspace_root=tmp_path / "workspaces",
        content_store=store,
        clock=lambda: NOW,
        data_owner_node_id=lambda _job: "node-owner",
        resource_admission=admission,
    )
    calls = 0

    async def fail_once(_self, _job):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("simulated result publication failure")
        return ExecutionResult(True, {"job_id": "ok"})

    monkeypatch.setattr(_BaseHandler, "_execute", fail_once)

    first = asyncio.run(handler.execute(_job()))
    second = asyncio.run(handler.execute(_job()))

    assert first.succeeded is False
    assert first.reason_code == "analysis-input-unavailable"
    assert second.succeeded is True
    assert calls == 2
