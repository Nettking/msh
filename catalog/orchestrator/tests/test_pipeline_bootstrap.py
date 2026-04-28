from __future__ import annotations

from pathlib import Path

from catalog.orchestrator import pipeline
from catalog.runner.script_catalog import ScriptOption


def _option(number: int, key: str) -> ScriptOption:
    return ScriptOption(
        number=number,
        key=key,
        script_path=Path(f"catalog/{key}/{key}.py"),
        description=key,
        category="Simple",
    )


def test_bootstrap_full_analysis_uses_discovered_order_minus_explicit_exclusions(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "BOOTSTRAP_FULL_ANALYSIS_EXCLUDED_SCRIPT_KEYS",
        ("ml_analysis",),
    )
    orchestrator = pipeline.RuntimeOrchestrator(poll_interval_seconds=60)
    script_options = [
        _option(1, "machines_active_per_day"),
        _option(2, "data_pr_day"),
        _option(3, "ml_analysis"),
        _option(4, "find_stops"),
    ]
    assert orchestrator._bootstrap_full_analysis_script_keys(script_options) == (
        "machines_active_per_day",
        "data_pr_day",
        "find_stops",
    )
