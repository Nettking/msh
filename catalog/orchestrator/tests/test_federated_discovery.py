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


def _record_batch(data_dir: Path, day: str, batch: int) -> Path:
    """Add one more recorded batch for a day, the way a live recorder does."""

    day_dir = data_dir / day
    day_dir.mkdir(parents=True, exist_ok=True)
    path = day_dir / f"batch-{batch:03d}.jsonl"
    path.write_text(
        f'{{"timestamp":"{day}T10:{batch:02d}:00Z","machine":"A"}}\n',
        encoding="utf-8",
    )
    return path


def _settle(gateway: _RecordingGateway, job_id: str, *, succeeded: bool = True) -> None:
    """Let one durable job reach a terminal state, as a provider would."""

    submission = next(
        item for item in gateway.submissions if item["job_id"] == job_id
    )
    gateway.views[job_id] = {
        "terminal": True,
        "succeeded": succeeded,
        "status": "succeeded" if succeeded else "failed",
        "provider_id": "provider-remote",
        "node_id": "node-remote",
        "target_dates": [submission["target_day"].isoformat()],
        "source_signature": submission["source_signature"],
    }


def _settle_all(gateway: _RecordingGateway) -> None:
    for submission in list(gateway.submissions):
        job_id = str(submission["job_id"])
        if job_id not in gateway.views:
            _settle(gateway, job_id)


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
    # The queued job settles first: changed data is new work for that day, but it
    # is queued once, after the analysis already in flight for it has finished.
    _settle(gateway, str(gateway.submissions[0]["job_id"]))
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


def test_catch_up_advances_while_the_newest_day_keeps_receiving_data(
    tmp_path: Path, discovery
) -> None:
    """A recorder writing today must not starve the rest of the backlog.

    Every poll cycle sees a new batch for the newest day, so that day's source
    signature keeps moving. Catch-up still has to work through the older days.
    """

    orchestrator, gateway, _executed = discovery
    data_dir = tmp_path / "data"
    for day in ("2026-08-10", "2026-08-11", "2026-08-12"):
        _record_batch(data_dir, day, 0)
    _record_batch(data_dir, "2026-08-13", 0)

    orchestrator._run_update(bootstrap=True)
    for batch in range(1, 6):
        _settle_all(gateway)
        _record_batch(data_dir, "2026-08-13", batch)
        orchestrator._run_update(bootstrap=False)

    queued_days = {item["target_day"].isoformat() for item in gateway.submissions}
    assert queued_days == {"2026-08-10", "2026-08-11", "2026-08-12", "2026-08-13"}
    state = orchestrator.state_snapshot()
    assert {"2026-08-10", "2026-08-11", "2026-08-12"} <= set(state["processed_dates"])
    # The day still being recorded is refreshed as its new batches land, so the
    # backlog draining never costs the operator a stale view of today.
    live_day_jobs = {
        item["job_id"]
        for item in gateway.submissions
        if item["target_day"] == date(2026, 8, 13)
    }
    assert len(live_day_jobs) > 1


def test_a_day_still_receiving_data_keeps_one_job_in_flight(
    tmp_path: Path, discovery
) -> None:
    """Discovery must not stack a job per poll for a source that keeps growing."""

    orchestrator, gateway, _executed = discovery
    data_dir = tmp_path / "data"
    _record_batch(data_dir, "2026-08-13", 0)

    orchestrator._run_update(bootstrap=True)
    for batch in range(1, 5):
        _record_batch(data_dir, "2026-08-13", batch)
        orchestrator._run_update(bootstrap=False)

    assert len({item["job_id"] for item in gateway.submissions}) == 1
    state = orchestrator.state_snapshot()
    assert len(state["pending_analysis_jobs"]) == 1


def test_data_that_arrived_while_a_job_ran_is_queued_once_it_settles(
    tmp_path: Path, discovery
) -> None:
    """Waiting for the in-flight job must not lose the batches that arrived."""

    orchestrator, gateway, _executed = discovery
    data_dir = tmp_path / "data"
    _record_batch(data_dir, "2026-08-13", 0)
    orchestrator._run_update(bootstrap=True)
    first_job = str(gateway.submissions[0]["job_id"])

    _record_batch(data_dir, "2026-08-13", 1)
    orchestrator._run_update(bootstrap=False)
    assert len(gateway.submissions) == 1

    _settle(gateway, first_job)
    orchestrator._run_update(bootstrap=False)

    assert len({item["job_id"] for item in gateway.submissions}) == 2
    latest = gateway.submissions[-1]
    assert latest["target_day"] == date(2026, 8, 13)
    assert latest["source_signature"] != gateway.submissions[0]["source_signature"]
    # The day is not claimed as processed while its newest data is still queued.
    assert orchestrator.state_snapshot()["processed_dates"] == []


def test_a_cycle_with_nothing_new_still_drives_jobs_already_in_flight(
    tmp_path: Path, discovery
) -> None:
    """The lifecycle driver has to stay alive on quiet cycles too."""

    orchestrator, gateway, _executed = discovery
    _record_batch(tmp_path / "data", "2026-08-13", 0)

    orchestrator._run_update(bootstrap=True)
    passes_after_bootstrap = gateway.scheduling_passes
    orchestrator._run_update(bootstrap=False)

    assert len(gateway.submissions) == 1
    assert gateway.scheduling_passes == passes_after_bootstrap + 1


def test_no_pending_jobs_leaves_a_quiet_cycle_quiet(
    tmp_path: Path, discovery
) -> None:
    orchestrator, gateway, _executed = discovery
    _record_batch(tmp_path / "data", "2026-08-13", 0)

    orchestrator._run_update(bootstrap=True)
    _settle_all(gateway)
    orchestrator._run_update(bootstrap=False)
    passes_after_settle = gateway.scheduling_passes
    orchestrator._run_update(bootstrap=False)

    assert gateway.scheduling_passes == passes_after_settle
    assert orchestrator.state_snapshot()["pending_analysis_jobs"] == []
