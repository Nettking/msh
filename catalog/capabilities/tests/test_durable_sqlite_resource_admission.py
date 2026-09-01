from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from catalog.capabilities.efficiency.store import SQLiteExecutionLearningStore
from catalog.capabilities.job_store import SQLiteJobStore
from catalog.capabilities.jobs import (
    CapabilityRequirement,
    JobContract,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.federation.errors import FederationValidationError
from catalog.federation.host_resources import (
    FilesystemMeasurement,
    HostResourceRefused,
    PressureThresholds,
    ProcessResourceAdmission,
)
from catalog.orchestrator import analysis_runtime
from catalog.orchestrator.analysis_runtime import (
    AnalysisIdentity,
    AnalysisRuntime,
    resolve_analysis_identity,
)

NOW = datetime(2026, 8, 31, tzinfo=timezone.utc)


def _admission(free_bytes: int) -> ProcessResourceAdmission:
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
        measurer=lambda _path: FilesystemMeasurement(
            resource_id="analysis-sqlite-resource",
            observed_at=NOW,
            total_bytes=10_000_000,
            free_bytes=free_bytes,
            total_inodes=None,
            free_inodes=None,
            available=True,
        ),
        clock=lambda: NOW,
    )


def _job(job_id: str = "job-resource-admission") -> JobContract:
    return JobContract(
        job_id=job_id,
        session_id="session-resource-admission",
        request_id=f"request-{job_id}",
        idempotency_key=f"session-resource-admission:{job_id}",
        capability=CapabilityRequirement(
            capability_type="test-capability",
            protocol="test-protocol",
            protocol_version="1.0",
            requirements={},
        ),
        inputs=(),
        outputs=(),
        retry_policy=RetryPolicy(
            max_attempts=1,
            backoff_seconds=(),
            retryable_error_codes=(),
        ),
        timeout_policy=TimeoutPolicy(
            overall_timeout_seconds=60,
            queue_timeout_seconds=10,
            start_timeout_seconds=10,
            run_timeout_seconds=30,
            cancellation_grace_seconds=5,
        ),
    )


def test_analysis_runtime_startup_refuses_before_capability_root_creation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "runtime"

    with pytest.raises(HostResourceRefused):
        AnalysisRuntime(
            root=root,
            identity=AnalysisIdentity(
                session_id="session-runtime-pressure",
                node_id="node-runtime-pressure",
                provider_id="provider-runtime-pressure",
                standalone=True,
            ),
            enable_local_provider=False,
            resource_admission=_admission(200),
        )

    assert not (root / "results" / "capabilities").exists()


@pytest.fixture()
def standalone_identity_binding(monkeypatch: pytest.MonkeyPatch):
    """Pin the process-wide bindings resolve_analysis_identity short-circuits on.

    ``create_app()`` registers a process-global identity supplier, and any
    earlier test in the session that builds a Flask application leaves it
    installed. ``resolve_analysis_identity`` consults the environment and that
    supplier before it reaches the standalone writer, so without this the test
    silently stops exercising the admitted path and passes for the wrong
    reason -- or, as here, fails depending on suite order. This mirrors the
    isolation ``catalog/orchestrator/tests/conftest.py`` already applies.
    """

    monkeypatch.setattr(analysis_runtime, "_IDENTITY_SUPPLIER", None)
    monkeypatch.delenv("FCP_ANALYSIS_SESSION_ID", raising=False)
    monkeypatch.delenv("FCP_ANALYSIS_NODE_ID", raising=False)


def test_standalone_identity_refuses_before_state_parent_creation(
    tmp_path: Path,
    standalone_identity_binding: None,
) -> None:
    state = tmp_path / "identity-state" / "analysis_identity.json"

    with pytest.raises(HostResourceRefused):
        resolve_analysis_identity(state, resource_admission=_admission(200))

    assert not state.parent.exists()


def test_a_registered_identity_supplier_bypasses_the_standalone_writer(
    tmp_path: Path,
    standalone_identity_binding: None,
) -> None:
    """Why the fixture above is required, stated as a consequence.

    A bound federation identity is resolved without writing standalone state at
    all, so no host resource is measured and nothing is refused. That is
    correct -- there is no writer to admit -- but it means a leaked supplier
    turns the refusal test above into a test of nothing.
    """

    state = tmp_path / "identity-state" / "analysis_identity.json"
    analysis_runtime.register_identity_supplier(
        lambda: ("session-bound", "node-bound")
    )
    try:
        identity = resolve_analysis_identity(
            state, resource_admission=_admission(200)
        )
    finally:
        analysis_runtime.register_identity_supplier(None)

    assert identity.standalone is False
    assert identity.session_id == "session-bound"
    assert not state.parent.exists()


def test_job_store_mutation_refusal_leaves_state_unchanged(tmp_path: Path) -> None:
    admission = _admission(1_000_000_000)
    store = SQLiteJobStore(
        tmp_path / "jobs.sqlite3",
        resource_admission=admission,
    )
    admission.measurer = lambda _path: FilesystemMeasurement(
        resource_id="analysis-sqlite-resource",
        observed_at=NOW,
        total_bytes=10_000_000,
        free_bytes=200,
        total_inodes=None,
        free_inodes=None,
        available=True,
    )

    with pytest.raises(HostResourceRefused):
        store.submit(_job(), coordinator_id="coordinator", now=NOW)

    with pytest.raises(FederationValidationError, match="job does not exist"):
        store.snapshot("job-resource-admission")


def test_job_store_exception_unwinds_reservation_for_next_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    admission = _admission(1_000_000_000)
    store = SQLiteJobStore(
        tmp_path / "jobs.sqlite3",
        resource_admission=admission,
    )
    original_connect = store._connect

    def fail_connect():
        raise OSError("injected job-store connection failure")

    monkeypatch.setattr(store, "_connect", fail_connect)
    with pytest.raises(OSError, match="injected job-store connection failure"):
        store.submit(_job(), coordinator_id="coordinator", now=NOW)

    monkeypatch.setattr(store, "_connect", original_connect)
    submitted = store.submit(_job(), coordinator_id="coordinator", now=NOW)
    assert submitted.changed is True


def test_efficiency_mutation_refusal_and_recovery_are_bounded(
    tmp_path: Path,
) -> None:
    admission = _admission(1_000_000_000)
    store = SQLiteExecutionLearningStore(
        tmp_path / "efficiency.sqlite3",
        resource_admission=admission,
    )

    admission.measurer = lambda _path: FilesystemMeasurement(
        resource_id="analysis-sqlite-resource",
        observed_at=NOW,
        total_bytes=10_000_000,
        free_bytes=200,
        total_inodes=None,
        free_inodes=None,
        available=True,
    )
    with pytest.raises(HostResourceRefused):
        store.rebuild_profiles()

    admission.measurer = lambda _path: FilesystemMeasurement(
        resource_id="analysis-sqlite-resource",
        observed_at=NOW,
        total_bytes=10_000_000,
        free_bytes=1_000_000_000,
        total_inodes=None,
        free_inodes=None,
        available=True,
    )
    assert store.rebuild_profiles() == 0
    assert admission.assessment(store.database).reserved_bytes == 0
