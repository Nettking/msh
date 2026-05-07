from __future__ import annotations

from datetime import date
from pathlib import Path

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
