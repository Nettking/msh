from __future__ import annotations

from catalog.orchestrator import policies


def test_runtime_policy_summary_contains_expected_policy_names() -> None:
    assert policies.RUNTIME_POLICY_SUMMARY["bootstrap_date_policy"] == "latest_discovered_day_only"
    assert policies.RUNTIME_POLICY_SUMMARY["execution_policy"] == "best_effort_continue_on_failure"
    assert policies.RUNTIME_POLICY_SUMMARY["flask_handoff_policy"] == "always_handoff"
    assert policies.RUNTIME_POLICY_SUMMARY["automatic_coverage_contract"] == "runtime_playback_ready_outputs"


def test_startup_modes_are_explicit() -> None:
    assert policies.STARTUP_MODE_PENDING in policies.RUNTIME_POLICY_SUMMARY["startup_modes"]
    assert policies.STARTUP_MODE_CONTINUE in policies.RUNTIME_POLICY_SUMMARY["startup_modes"]
    assert policies.STARTUP_MODE_CLEAN in policies.RUNTIME_POLICY_SUMMARY["startup_modes"]
