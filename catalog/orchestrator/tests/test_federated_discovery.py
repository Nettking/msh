"""Automatic JSONL discovery creates durable jobs instead of running analysis."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from catalog.capabilities.analysis.scheduler import SubmissionOutcome
from catalog.capabilities.jobs import JobStatus
from catalog.orchestrator import pipeline


class _RecordingGateway:
    """Stand in for the federation work gateway and record what discovery asks."""

    def __init__(self) -> None:
        self.submissions: list[dict[str, object]] = []
        self.scheduling_passes = 0
        self.views: dict[str, dict[str, object]] = {}
        self._known: set[str] = set()

    def submit_date_slice(
        self,
        *,
        data_dir: Path,
        target_day: date,
        script_keys,
        runtime_namespace: str,
        source_signature: str,
    ) -> SubmissionOutcome:
        job_id = f"analysis-{target_day.isoformat()}-{source_signature[:8]}"
        self.submissions.append(
            {
                "data_dir": data_dir,
                "target_day": target_day,
                "script_keys": tuple(script_keys),
                "runtime_namespace": runtime_namespace,
                "source_signature": source_signature,
                "job_id": job_id,
            }
        )
        created = job_id not in self._known
        self._known.add(job_id)
        return SubmissionOutcome(
            job_id=job_id,
            created=created,
            status=JobStatus.QUEUED,
            idempotency_key=f"analysis:1:{job_id}",
        )

    def request_scheduling_pass(self) -> bool:
        self.scheduling_passes += 1
        return True

    def job_view(self, job_id: str):
        return self.views.get(job_id)


def _orchestrator(tmp_path: Path, monkeypatch, gateway: _RecordingGateway):
    monkeypatch.setattr(pipeline, "repo_root", lambda: tmp_path)
    orchestrator = pipeline.RuntimeOrchestrator(
        poll_interval_seconds=60, analysis_gateway=gateway
    )
    orchestrator._state.startup_mode = pipeline.STARTUP_MODE_CONTINUE
    return orchestrator


def _write_day(data_dir: Path, day: str, *, machine: str = "A") -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / f"{day}.jsonl"
    path.write_text(
        f'{{"timestamp":"{day}T10:00:00Z","machine":"{machine}"}}\n',
        encoding="utf-8",
    )
    return path


@pytest.fixture
def discovery(tmp_path: Path, monkeypatch):
    gateway = _RecordingGateway()
    orchestrator = _orchestrator(tmp_path, monkeypatch, gateway)
    executed: list[object] = []
    monkeypatch.setattr(
        pipeline,
        "_run_for_date_slice",
        lambda **kwargs: executed.append(kwargs),
    )
    monkeypatch.setattr(
        pipeline,
        "discover_runnable_scripts",
        lambda _root: [
            pipeline_script(key) for key in pipeline.AUTO_COVERAGE_SCRIPT_KEYS
        ],
    )
    return orchestrator, gateway, executed


def pipeline_script(key: str):
    from catalog.runner.script_catalog import ScriptOption

    return ScriptOption(
        number=1,
        key=key,
        script_path=Path(f"catalog/{key}/{key}.py"),
        description=key,
        category="Simple",
    )


def test_new_data_becomes_a_durable_job_and_is_never_run_by_discovery(
    tmp_path: Path, discovery
) -> None:
    orchestrator, gateway, executed = discovery
    _write_day(tmp_path / "data", "2026-08-13")

    orchestrator._run_update(bootstrap=True)

    assert len(gateway.submissions) == 1
    submission = gateway.submissions[0]
    assert submission["target_day"] == date(2026, 8, 13)
    assert submission["script_keys"] == pipeline.AUTO_COVERAGE_SCRIPT_KEYS
    assert executed == []
    assert gateway.scheduling_passes == 1
    state = orchestrator.state_snapshot()
    assert state["pending_analysis_jobs"] == [submission["job_id"]]
    assert state["pending_analysis_slices"] == {
        "2026-08-13": submission["source_signature"]
    }
    assert state["analysis_dispatch_mode"] == pipeline.ANALYSIS_DISPATCH_POLICY


def test_repeated_discovery_of_the_same_slice_does_not_create_new_work(
    tmp_path: Path, discovery
) -> None:
    orchestrator, gateway, executed = discovery
    _write_day(tmp_path / "data", "2026-08-13")

    orchestrator._run_update(bootstrap=True)
    orchestrator._run_update(bootstrap=False)
    orchestrator._run_update(bootstrap=False)

    job_ids = {item["job_id"] for item in gateway.submissions}
    assert len(job_ids) == 1
    assert executed == []
    assert orchestrator.state_snapshot()["pending_analysis_jobs"] == sorted(job_ids)
    # The first cycle queued the day; later cycles recognized the live job.
    assert len(gateway.submissions) == 1


def test_materially_changed_data_produces_new_work(
    tmp_path: Path, discovery
) -> None:
    orchestrator, gateway, _executed = discovery
    data_dir = tmp_path / "data"
    _write_day(data_dir, "2026-08-13")
    orchestrator._run_update(bootstrap=True)
    first_signature = gateway.submissions[0]["source_signature"]

    _write_day(data_dir, "2026-08-13", machine="B")
    orchestrator._run_update(bootstrap=False)

    signatures = {item["source_signature"] for item in gateway.submissions}
    assert first_signature in signatures
    assert len(signatures) == 2
    assert len({item["job_id"] for item in gateway.submissions}) == 2


def test_discovery_queues_one_day_per_cycle(tmp_path: Path, discovery) -> None:
    orchestrator, gateway, _executed = discovery
    data_dir = tmp_path / "data"
    for day in ("2026-08-11", "2026-08-12", "2026-08-13"):
        _write_day(data_dir, day)

    orchestrator._run_update(bootstrap=True)
    orchestrator._run_update(bootstrap=False)

    queued = [item["target_day"] for item in gateway.submissions]
    assert queued == [date(2026, 8, 13), date(2026, 8, 12)]


def test_a_failing_submission_is_reported_without_stopping_discovery(
    tmp_path: Path, discovery
) -> None:
    orchestrator, gateway, executed = discovery
    _write_day(tmp_path / "data", "2026-08-13")

    def explode(**_kwargs):
        raise RuntimeError("federation unavailable")

    gateway.submit_date_slice = explode
    orchestrator._run_update(bootstrap=True)

    state = orchestrator.state_snapshot()
    assert "federation unavailable" in str(state["last_failure"])
    assert state["pending_analysis_jobs"] == []
    assert executed == []


def test_successful_job_completion_advances_runtime_progress(
    tmp_path: Path, discovery
) -> None:
    orchestrator, gateway, _executed = discovery
    _write_day(tmp_path / "data", "2026-08-13")
    orchestrator._run_update(bootstrap=True)
    job_id = gateway.submissions[0]["job_id"]
    gateway.views[job_id] = {
        "terminal": True,
        "succeeded": True,
        "status": "succeeded",
        "provider_id": "provider-remote",
        "node_id": "node-remote",
        "target_dates": ["2026-08-13"],
        "source_signature": gateway.submissions[0]["source_signature"],
    }

    orchestrator._run_update(bootstrap=False)

    state = orchestrator.state_snapshot()
    assert state["last_analysis_job_id"] == job_id
    assert state["last_analysis_provider_id"] == "provider-remote"
    assert state["last_analysis_node_id"] == "node-remote"
    assert state["last_processed_date"] == "2026-08-13"
    assert state["bootstrap_complete"] is True
    assert state["pending_analysis_jobs"] == []


def test_failed_job_surfaces_the_error_without_local_fallback(
    tmp_path: Path, discovery
) -> None:
    orchestrator, gateway, executed = discovery
    _write_day(tmp_path / "data", "2026-08-13")
    orchestrator._run_update(bootstrap=True)
    job_id = gateway.submissions[0]["job_id"]
    gateway.views[job_id] = {
        "terminal": True,
        "succeeded": False,
        "status": "failed",
        "error_code": "worker-handler-failed",
        "provider_id": "provider-remote",
        "node_id": "node-remote",
        "target_dates": ["2026-08-13"],
        "source_signature": gateway.submissions[0]["source_signature"],
    }

    orchestrator._run_update(bootstrap=False)

    state = orchestrator.state_snapshot()
    assert "failed" in str(state["last_failure"])
    assert "worker-handler-failed" in str(state["last_failure"])
    assert executed == []
