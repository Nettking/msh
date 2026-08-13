"""End-to-end: a single-node federation still analyses its own JSONL."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from catalog.capabilities.analysis.contracts import (
    ANALYSIS_CAPABILITY_TYPE,
    ORIGIN_AUTOMATIC_DISCOVERY,
)
from catalog.capabilities.jobs import JobStatus
from catalog.orchestrator import analysis_runtime
from catalog.capabilities.analysis.provisioning import analysis_capability_id
from catalog.orchestrator.analysis_runtime import (
    AnalysisIdentity,
    AnalysisRuntime,
    DiscoveryAnalysisGateway,
    resolve_analysis_identity,
)

NOW = datetime(2026, 8, 13, 9, 0, tzinfo=timezone.utc)

def _identity(tmp_path: Path, session_id: str):
    """Build an identity whose node ID is a real key-derived device identity.

    F8 membership is only possible for a genuine node identity, so tests use the
    same IdentityStore the product uses rather than inventing a node ID.
    """

    from catalog.node.identity import IdentityStore

    credentials = IdentityStore(
        tmp_path / "results" / "capabilities" / "standalone_identity",
        display_name="Test device",
    ).load_or_create(now=NOW)
    return AnalysisIdentity(
        session_id=session_id,
        node_id=credentials.identity.node_id,
        provider_id=analysis_capability_id(credentials.identity.node_id),
        standalone=True,
    )



def _runtime(tmp_path: Path) -> AnalysisRuntime:
    return AnalysisRuntime(
        root=tmp_path,
        identity=_identity(tmp_path, "session-standalone-tests"),
        clock=lambda: NOW,
    )


def _write_day(data_dir: Path, day: str) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / f"{day}.jsonl").write_text(
        f'{{"timestamp":"{day}T10:00:00Z","machine":"A"}}\n', encoding="utf-8"
    )


def test_standalone_identity_is_persisted_and_stable(tmp_path: Path) -> None:
    state = tmp_path / "identity.json"

    first = resolve_analysis_identity(state)
    second = resolve_analysis_identity(state)

    assert first == second
    assert first.standalone is True
    # Ownership can never be granted by the provider that receives it.
    assert first.provider_id != first.node_id


def test_registered_supplier_binds_the_runtime_to_the_federation(
    tmp_path: Path,
) -> None:
    analysis_runtime.register_identity_supplier(
        lambda: ("session-real-federation", "node-real-device")
    )

    identity = resolve_analysis_identity(tmp_path / "identity.json")

    assert identity.session_id == "session-real-federation"
    assert identity.node_id == "node-real-device"
    assert identity.standalone is False
    assert not (tmp_path / "identity.json").exists()


def test_a_failing_supplier_falls_back_to_the_single_node_session(
    tmp_path: Path,
) -> None:
    def broken():
        raise RuntimeError("federation unreachable")

    analysis_runtime.register_identity_supplier(broken)
    identity = resolve_analysis_identity(tmp_path / "identity.json")

    assert identity.standalone is True


def test_local_provider_is_advertised_as_an_ordinary_provider(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)

    reports = runtime.report_source.reports(
        session_id=runtime.identity.session_id,
        capability_type=ANALYSIS_CAPABILITY_TYPE,
    )

    assert len(reports) == 1
    assert reports[0].capability_id == runtime.identity.provider_id
    assert reports[0].node_id == runtime.identity.node_id
    assert reports[0].session_id == runtime.identity.session_id


def test_a_federation_of_one_schedules_and_runs_its_own_slice(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    data_dir = tmp_path / "data"
    _write_day(data_dir, "2026-08-13")
    gateway = DiscoveryAnalysisGateway(runtime)
    executed: list[dict] = []

    class _Executor:
        def execute(self, *, plan, data_dir, workspace):
            from catalog.capabilities.analysis import AnalysisExecutionReport

            executed.append({"plan": plan, "files": sorted(data_dir.rglob("*.jsonl"))})
            return AnalysisExecutionReport(
                succeeded=True,
                processed_dates=plan.target_dates,
                analysis_session_ids=("auto_default_20260813_20260813",),
            )

    binding = runtime.federation.inventory.get("jsonl-analysis-handler")
    binding.handler.executor = _Executor()

    outcome = gateway.submit_date_slice(
        data_dir=data_dir,
        target_day=date(2026, 8, 13),
        script_keys=("data_pr_day",),
        runtime_namespace="default",
        source_signature="b" * 64,
        origin=ORIGIN_AUTOMATIC_DISCOVERY,
    )
    assert outcome is not None
    results = gateway.run_scheduling_pass()

    view = gateway.job_view(outcome.job_id)
    assert [item.decision for item in results] == ["dispatched"]
    assert view["status"] == JobStatus.SUCCEEDED.value
    assert view["provider_id"] == runtime.identity.provider_id
    assert view["node_id"] == runtime.identity.node_id
    assert view["output_artifact"] is not None
    assert view["target_dates"] == ["2026-08-13"]
    assert len(executed) == 1
    assert executed[0]["files"]


def test_no_provider_means_the_job_waits_rather_than_running_locally(
    tmp_path: Path,
) -> None:
    runtime = AnalysisRuntime(
        root=tmp_path,
        identity=_identity(tmp_path, "session-standalone-tests"),
        clock=lambda: NOW,
        enable_local_provider=False,
    )
    data_dir = tmp_path / "data"
    _write_day(data_dir, "2026-08-13")
    gateway = DiscoveryAnalysisGateway(runtime)

    outcome = gateway.submit_date_slice(
        data_dir=data_dir,
        target_day=date(2026, 8, 13),
        script_keys=("data_pr_day",),
        runtime_namespace="default",
        source_signature="b" * 64,
    )
    results = gateway.run_scheduling_pass()

    assert [item.decision for item in results] == ["no-eligible-provider"]
    assert gateway.job_view(outcome.job_id)["status"] == JobStatus.QUEUED.value


def test_submission_without_matching_source_files_creates_no_job(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    gateway = DiscoveryAnalysisGateway(runtime)

    outcome = gateway.submit_date_slice(
        data_dir=data_dir,
        target_day=date(2026, 8, 13),
        script_keys=("data_pr_day",),
        runtime_namespace="default",
        source_signature="b" * 64,
    )

    assert outcome is None
    assert runtime.service.registry.records() == ()


@pytest.mark.parametrize("attempts", (1, 2, 3))
def test_repeated_submission_is_idempotent_across_runtime_instances(
    tmp_path: Path, attempts: int
) -> None:
    data_dir = tmp_path / "data"
    _write_day(data_dir, "2026-08-13")
    job_ids = set()
    for _ in range(attempts):
        runtime = _runtime(tmp_path)
        outcome = DiscoveryAnalysisGateway(runtime).submit_date_slice(
            data_dir=data_dir,
            target_day=date(2026, 8, 13),
            script_keys=("data_pr_day",),
            runtime_namespace="default",
            source_signature="b" * 64,
        )
        job_ids.add(outcome.job_id)

    assert len(job_ids) == 1
