from __future__ import annotations

from datetime import date
from pathlib import Path
import threading

from catalog.orchestrator import pipeline
from catalog.runner.script_catalog import ScriptOption
from catalog.runner.session_store import AUTOMATIC_RUNTIME_SCRIPT_KEYS, WORKFLOW_STEPS


def _option(number: int, key: str) -> ScriptOption:
    return ScriptOption(
        number=number,
        key=key,
        script_path=Path(f"catalog/{key}/{key}.py"),
        description=key,
        category="Simple",
    )


def test_automatic_runtime_contract_includes_playback_and_excludes_heavy_scripts():
    assert "data_visualizer" in pipeline.AUTO_COVERAGE_SCRIPT_KEYS
    assert pipeline.AUTO_COVERAGE_SCRIPT_KEYS == AUTOMATIC_RUNTIME_SCRIPT_KEYS
    assert "ml_analysis" not in pipeline.AUTO_COVERAGE_SCRIPT_KEYS
    assert "data_analysis" not in pipeline.AUTO_COVERAGE_SCRIPT_KEYS
    assert "corrolation_machine_pairs" not in pipeline.AUTO_COVERAGE_SCRIPT_KEYS
    assert "find_stops" not in pipeline.AUTO_COVERAGE_SCRIPT_KEYS


def test_workflow_groups_health_first_then_playback_generation():
    assert WORKFLOW_STEPS[0][0] == "Step 1: Startup-safe health checks"
    assert WORKFLOW_STEPS[1] == ("Step 2: Playback/timeline generation", ["data_visualizer"])


def test_bootstrap_analysis_uses_automatic_playback_ready_contract_in_contract_order():
    orchestrator = pipeline.RuntimeOrchestrator(poll_interval_seconds=60)
    script_options = [
        _option(1, "machines_active_per_day"),
        _option(2, "data_pr_day"),
        _option(3, "data_visualizer"),
        _option(4, "ml_analysis"),
        _option(5, "find_stops"),
        _option(6, "analyze_missing_sequence_number"),
        _option(7, "corrolation_machine_pairs"),
        _option(8, "missing_per_day_by_machine"),
        _option(9, "sampling_rate_analysis"),
        _option(10, "data_analysis"),
    ]
    assert orchestrator._bootstrap_full_analysis_script_keys(script_options) == pipeline.AUTO_COVERAGE_SCRIPT_KEYS


def test_reused_auto_session_metadata_is_updated_to_active_runtime_namespace(tmp_path: Path):
    workflows_root = tmp_path / "workflows"
    active_namespace = "clean_20260302T000000Z"
    session_id = pipeline._auto_session_id("2026-03-02", "2026-03-02", runtime_namespace=active_namespace)
    session_dir = workflows_root / session_id
    session_dir.mkdir(parents=True)
    metadata = {
        "version": 2,
        "session_id": session_id,
        "filter": {"start_date": "2026-03-02", "end_date": "2026-03-02", "start_hour": None, "end_hour": None},
        "paths": {"filtered_data_dir": "data", "runs_dir": "runs", "playback_exports_dir": "exports/timeline"},
        "filter_result": {"filtered_data_path": "data"},
        "runtime": {},
        "scripts": {},
    }
    (session_dir / "session_state.json").write_text(pipeline.json.dumps(metadata) + "\n", encoding="utf-8")

    _, _, reused_metadata, session_mode = pipeline._load_or_create_auto_session(
        workflows_root=workflows_root,
        start_date=date(2026, 3, 2),
        end_date=date(2026, 3, 2),
        script_options=[],
        runtime_namespace=active_namespace,
    )

    assert session_mode == "reused"
    assert reused_metadata["runtime"]["runtime_namespace"] == active_namespace
    persisted = pipeline.json.loads((session_dir / "session_state.json").read_text(encoding="utf-8"))
    assert persisted["runtime"]["runtime_namespace"] == active_namespace


def test_runtime_state_snapshot_exposes_playback_filter_contract_keys(tmp_path: Path):
    orchestrator = pipeline.RuntimeOrchestrator.__new__(pipeline.RuntimeOrchestrator)
    orchestrator._lock = threading.Lock()
    orchestrator.workflows_root = tmp_path / "workflows"
    orchestrator.workflows_root.mkdir(parents=True)
    orchestrator._state = pipeline.RuntimeOrchestrator._default_state(orchestrator)
    orchestrator._state.startup_mode = pipeline.STARTUP_MODE_CLEAN
    orchestrator._state.active_runtime_namespace = "clean_20260302T000000Z"
    orchestrator._state.session_id = "auto_clean_20260302T000000Z_20260302_20260302"

    snapshot = pipeline.RuntimeOrchestrator.state_snapshot(orchestrator)

    assert snapshot["startup_mode"] == pipeline.STARTUP_MODE_CLEAN
    assert snapshot["active_runtime_namespace"] == "clean_20260302T000000Z"
    assert snapshot["session_id"] == "auto_clean_20260302T000000Z_20260302_20260302"
    assert "view_contracts" in snapshot


def test_correlated_execution_reservation_blocks_uncorrelated_update(
    tmp_path: Path,
) -> None:
    orchestrator = pipeline.RuntimeOrchestrator.__new__(pipeline.RuntimeOrchestrator)
    orchestrator._lock = threading.Lock()
    orchestrator.workflows_root = tmp_path / "workflows"
    orchestrator.workflows_root.mkdir(parents=True)
    orchestrator._state = pipeline.RuntimeOrchestrator._default_state(orchestrator)
    orchestrator._state.startup_mode = pipeline.STARTUP_MODE_CLEAN
    orchestrator._state.active_execution_id = "analysis-upload-race"

    result = pipeline.RuntimeOrchestrator._run_update(
        orchestrator, bootstrap=False, execution_id=None
    )

    assert result.session_id == "none"
    assert orchestrator._state.active_execution_id == "analysis-upload-race"
    assert orchestrator._state.update_running is False

def test_load_state_clears_process_local_execution_reservation(tmp_path: Path) -> None:
    orchestrator = pipeline.RuntimeOrchestrator.__new__(pipeline.RuntimeOrchestrator)
    orchestrator.state_path = tmp_path / "runtime_state.json"
    state = pipeline.RuntimeOrchestrator._default_state(orchestrator)
    state.update_running = True
    state.active_execution_id = "analysis-upload-crashed"
    state.completed_execution_id = "analysis-upload-previous"
    state.completed_execution_succeeded = True
    orchestrator.state_path.write_text(
        pipeline.json.dumps(state.__dict__) + "\n", encoding="utf-8"
    )

    loaded = pipeline.RuntimeOrchestrator._load_state(orchestrator)

    assert loaded.update_running is False
    assert loaded.active_execution_id is None
    assert loaded.completed_execution_id == "analysis-upload-previous"
    assert loaded.completed_execution_succeeded is True


def test_uncorrelated_refresh_reserves_scheduler_before_worker_start(monkeypatch) -> None:
    orchestrator = pipeline.RuntimeOrchestrator.__new__(pipeline.RuntimeOrchestrator)
    orchestrator._lock = threading.Lock()
    orchestrator._state = pipeline.RuntimeOrchestrator._default_state(orchestrator)
    orchestrator._state.startup_mode = pipeline.STARTUP_MODE_CLEAN
    orchestrator._persist_state = lambda: None
    created = []

    class _DeferredThread:
        def __init__(self, *, target, kwargs, daemon):
            created.append((target, kwargs, daemon))

        def start(self):
            return None

    monkeypatch.setattr(pipeline.threading, "Thread", _DeferredThread)

    accepted = pipeline.RuntimeOrchestrator.request_refresh(orchestrator)

    assert accepted is True
    assert orchestrator._state.active_execution_id.startswith("runtime-refresh-")
    assert len(created) == 1
    assert created[0][1]["execution_id"] == orchestrator._state.active_execution_id
    assert created[0][2] is True


def test_refresh_worker_start_failure_releases_scheduler_reservation(monkeypatch) -> None:
    orchestrator = pipeline.RuntimeOrchestrator.__new__(pipeline.RuntimeOrchestrator)
    orchestrator._lock = threading.Lock()
    orchestrator._state = pipeline.RuntimeOrchestrator._default_state(orchestrator)
    orchestrator._state.startup_mode = pipeline.STARTUP_MODE_CLEAN
    orchestrator._persist_state = lambda: None

    class _BrokenThread:
        def __init__(self, *, target, kwargs, daemon):
            pass

        def start(self):
            raise RuntimeError("thread start failed")

    monkeypatch.setattr(pipeline.threading, "Thread", _BrokenThread)

    try:
        pipeline.RuntimeOrchestrator.request_refresh(orchestrator)
    except RuntimeError as exc:
        assert str(exc) == "thread start failed"
    else:
        raise AssertionError("request_refresh should propagate worker start failure")

    assert orchestrator._state.active_execution_id is None
    assert orchestrator._state.update_running is False
