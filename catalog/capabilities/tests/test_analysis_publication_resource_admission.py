from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.capabilities.analysis.content_store import ContentIdentity
from catalog.capabilities.analysis.contracts import ANALYSIS_PLAN_SCHEMA
from catalog.capabilities.analysis.resource_admission import (
    FederatedAnalysisScheduler,
    analysis_publication_resource_requirement,
)
from catalog.capabilities.analysis.scheduler import (
    FederatedAnalysisScheduler as BaseFederatedAnalysisScheduler,
)
from catalog.capabilities.tests.analysis_harness import build_stack, work_slice
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureThresholds,
    ProcessResourceAdmission,
)

NOW = datetime(2026, 8, 29, 9, 45, tzinfo=timezone.utc)
THRESHOLDS = PressureThresholds(
    critical_free_bytes=100,
    pressure_free_bytes=200,
    warning_free_bytes=300,
    critical_free_inodes=1,
    pressure_free_inodes=2,
    warning_free_inodes=3,
)


class _Work:
    def __init__(self, payload: bytes = b"p") -> None:
        self.payload = payload

    def plan_bytes(self) -> bytes:
        return self.payload


class _InvalidWork:
    def plan_bytes(self) -> bytes:
        raise FederationValidationError(
            "analysis-plan-invalid",
            "plan",
            "invalid plan",
        )


def _measurement(resource_id: str, free_bytes: int) -> FilesystemMeasurement:
    return FilesystemMeasurement(
        resource_id=resource_id,
        observed_at=NOW,
        total_bytes=1_000,
        free_bytes=free_bytes,
        total_inodes=None,
        free_inodes=None,
        available=True,
    )


def _admission(measurer) -> ProcessResourceAdmission:
    return ProcessResourceAdmission(
        thresholds=THRESHOLDS,
        measurer=measurer,
        clock=lambda: NOW,
    )


def _scheduler(
    root: Path,
    *,
    admission: ProcessResourceAdmission,
    max_bytes: int = 2,
) -> FederatedAnalysisScheduler:
    scheduler = object.__new__(FederatedAnalysisScheduler)
    scheduler.resource_admission = admission
    scheduler.gateway = SimpleNamespace(
        content_store=SimpleNamespace(root=root, max_bytes=max_bytes)
    )
    return scheduler


def test_publication_requirement_covers_atomic_replacement_peaks() -> None:
    assert analysis_publication_resource_requirement(
        7,
        max_slice_bytes=11,
    ) == (29, 8)


def test_pressure_refuses_before_base_submission_starts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def base_submit(self, work, *, slice_files, slice_root, **_kwargs):
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr(BaseFederatedAnalysisScheduler, "submit", base_submit)
    scheduler = _scheduler(
        tmp_path,
        admission=_admission(lambda _path: _measurement("disk", 102)),
        max_bytes=2,
    )

    with pytest.raises(FederationOperationError) as captured:
        scheduler.submit(_Work(), slice_files=(), slice_root=tmp_path)

    assert captured.value.code == "analysis-resource-pressure"
    assert called is False


def test_warning_with_safe_emergency_reserve_allows_submission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = object()

    def base_submit(self, work, *, slice_files, slice_root, **_kwargs):
        return sentinel

    monkeypatch.setattr(BaseFederatedAnalysisScheduler, "submit", base_submit)
    scheduler = _scheduler(
        tmp_path,
        admission=_admission(lambda _path: _measurement("disk", 203)),
        max_bytes=2,
    )

    assert scheduler.submit(_Work(), slice_files=(), slice_root=tmp_path) is sentinel


def test_concurrent_submissions_cannot_double_spend_one_resource(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    first_result: list[object] = []

    def base_submit(self, work, *, slice_files, slice_root, **_kwargs):
        entered.set()
        assert release.wait(timeout=5)
        return object()

    monkeypatch.setattr(BaseFederatedAnalysisScheduler, "submit", base_submit)
    admission = _admission(lambda _path: _measurement("disk", 203))
    scheduler = _scheduler(tmp_path, admission=admission, max_bytes=2)

    def run_first() -> None:
        first_result.append(
            scheduler.submit(_Work(), slice_files=(), slice_root=tmp_path)
        )

    thread = threading.Thread(target=run_first)
    thread.start()
    assert entered.wait(timeout=5)
    try:
        with pytest.raises(FederationOperationError) as captured:
            scheduler.submit(_Work(), slice_files=(), slice_root=tmp_path)
        assert captured.value.code == "analysis-resource-pressure"
    finally:
        release.set()
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert len(first_result) == 1


def test_distinct_backing_resources_have_independent_envelopes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    entered_a = threading.Event()
    entered_b = threading.Event()
    release = threading.Event()

    def measurer(path: Path | str) -> FilesystemMeasurement:
        path = Path(path)
        resource = "disk-a" if path == root_a else "disk-b"
        return _measurement(resource, 203)

    def base_submit(self, work, *, slice_files, slice_root, **_kwargs):
        if self.gateway.content_store.root == root_a:
            entered_a.set()
        else:
            entered_b.set()
        assert release.wait(timeout=5)
        return object()

    monkeypatch.setattr(BaseFederatedAnalysisScheduler, "submit", base_submit)
    admission = _admission(measurer)
    scheduler_a = _scheduler(root_a, admission=admission, max_bytes=2)
    scheduler_b = _scheduler(root_b, admission=admission, max_bytes=2)

    thread_a = threading.Thread(
        target=lambda: scheduler_a.submit(_Work(), slice_files=(), slice_root=root_a)
    )
    thread_b = threading.Thread(
        target=lambda: scheduler_b.submit(_Work(), slice_files=(), slice_root=root_b)
    )
    thread_a.start()
    thread_b.start()
    try:
        assert entered_a.wait(timeout=5)
        assert entered_b.wait(timeout=5)
    finally:
        release.set()
        thread_a.join(timeout=5)
        thread_b.join(timeout=5)

    assert not thread_a.is_alive()
    assert not thread_b.is_alive()


def test_reservation_releases_after_publication_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = 0

    def base_submit(self, work, *, slice_files, slice_root, **_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("publication failed")
        return "ok"

    monkeypatch.setattr(BaseFederatedAnalysisScheduler, "submit", base_submit)
    scheduler = _scheduler(
        tmp_path,
        admission=_admission(lambda _path: _measurement("disk", 203)),
        max_bytes=2,
    )

    with pytest.raises(OSError):
        scheduler.submit(_Work(), slice_files=(), slice_root=tmp_path)

    assert scheduler.submit(_Work(), slice_files=(), slice_root=tmp_path) == "ok"


def test_validation_precedes_resource_measurement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    measurements = 0

    def measurer(_path) -> FilesystemMeasurement:
        nonlocal measurements
        measurements += 1
        return _measurement("disk", 102)

    def base_submit(self, work, *, slice_files, slice_root, **_kwargs):
        raise AssertionError("base submit must not run for invalid work")

    monkeypatch.setattr(BaseFederatedAnalysisScheduler, "submit", base_submit)
    scheduler = _scheduler(tmp_path, admission=_admission(measurer), max_bytes=2)

    with pytest.raises(FederationValidationError) as captured:
        scheduler.submit(_InvalidWork(), slice_files=(), slice_root=tmp_path)

    assert captured.value.code == "analysis-plan-invalid"
    assert measurements == 0


def test_existing_registered_slice_validation_error_is_preserved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def base_submit(self, work, *, slice_files, slice_root, **_kwargs):
        raise FederationValidationError(
            "analysis-slice-registered-content-invalid",
            "object_key",
            "registered analysis slice does not match its deterministic input",
        )

    monkeypatch.setattr(BaseFederatedAnalysisScheduler, "submit", base_submit)
    scheduler = _scheduler(
        tmp_path,
        admission=_admission(lambda _path: _measurement("disk", 400)),
        max_bytes=2,
    )

    with pytest.raises(FederationValidationError) as captured:
        scheduler.submit(_Work(), slice_files=(), slice_root=tmp_path)

    assert captured.value.code == "analysis-slice-registered-content-invalid"


def test_real_stack_pressure_refuses_before_input_or_job_publication(
    tmp_path: Path,
) -> None:
    state = {"free_bytes": 1_900_000_000}

    def measure(_path: Path | str) -> FilesystemMeasurement:
        return FilesystemMeasurement(
            resource_id="analysis-disk",
            observed_at=NOW,
            total_bytes=2_000_000_000,
            free_bytes=state["free_bytes"],
            total_inodes=10_000,
            free_inodes=10_000,
            available=True,
        )

    admission = _admission(measure)
    stack = build_stack(
        tmp_path,
        activate_provider=False,
        resource_admission=admission,
    )
    first_work = work_slice(session_id=stack.session_id, target_date="2026-08-14")
    first = stack.submit(first_work)
    assert first.created is True
    assert stack.service.registry.record_for(first_work.job_id) is not None
    assert (
        stack.content_store.root / first_work.object_key_prefix / "plan.json"
    ).is_file()

    work = work_slice(session_id=stack.session_id, target_date="2026-08-15")
    state["free_bytes"] = 102

    with pytest.raises(FederationOperationError) as captured:
        stack.submit(work)

    assert captured.value.code == "analysis-resource-pressure"
    assert len(stack.service.registry.records(session_id=stack.session_id)) == 1
    assert not (
        stack.content_store.root / work.object_key_prefix / "plan.json"
    ).exists()
    with pytest.raises(FederationValidationError) as missing:
        stack.store.snapshot(work.job_id)
    assert missing.value.code == "job-not-found"


def test_direct_artifact_registration_is_admitted_and_refused_under_pressure(
    tmp_path: Path,
) -> None:
    state = {"free_bytes": 1_900_000_000}

    def measure(_path: Path | str) -> FilesystemMeasurement:
        return FilesystemMeasurement(
            resource_id="analysis-disk",
            observed_at=NOW,
            total_bytes=2_000_000_000,
            free_bytes=state["free_bytes"],
            total_inodes=10_000,
            free_inodes=10_000,
            available=True,
        )

    admission = _admission(measure)
    stack = build_stack(
        tmp_path,
        activate_provider=False,
        resource_admission=admission,
    )
    work = work_slice(session_id=stack.session_id, target_date="2026-08-16")
    state["free_bytes"] = 102

    with pytest.raises(HostResourceRefused):
        stack.gateway.register_input(
            artifact_id=f"{work.job_id}-plan",
            session_id=stack.session_id,
            job_id=work.job_id,
            schema_id=ANALYSIS_PLAN_SCHEMA,
            media_type="application/json",
            object_key=f"{work.object_key_prefix}/plan.json",
            identity=ContentIdentity("sha256:" + ("1" * 64), 1),
            authority_node_id=stack.node_id,
            now=NOW,
        )

    with pytest.raises(FederationValidationError) as missing:
        stack.authority.artifact(f"{work.job_id}-plan")
    assert missing.value.code == "artifact-not-found"
