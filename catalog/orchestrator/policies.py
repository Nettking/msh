"""Documented runtime policy names for the FCP orchestrator.

This module is intentionally side-effect free. It gives documentation, tests, and
future refactors a stable place to reference policy names without importing the
full runtime state machine from ``catalog.orchestrator.pipeline``.
"""

DATE_POLICY_BOOTSTRAP_LATEST_DAY = "latest_discovered_day_only"
EXECUTION_POLICY_BEST_EFFORT = "best_effort_continue_on_failure"
FLASK_HANDOFF_POLICY_ALWAYS = "always_handoff"
UPDATE_POLICY_INCREMENTAL = "poll_for_new_data_then_process_new_slice"
HISTORICAL_CATCH_UP_POLICY = "reverse_chronological_one_day_per_cycle"
BOOTSTRAP_REFRESH_POLICY = "always_refresh_latest_day_on_startup"
AUTO_COVERAGE_CONTRACT = "runtime_playback_ready_outputs"
BOOTSTRAP_FULL_ANALYSIS_POLICY = "latest_day_playback_ready_analysis_before_catch_up"
STARTUP_MODE_PENDING = "pending_choice"
STARTUP_MODE_CONTINUE = "continue_existing"
STARTUP_MODE_CLEAN = "start_clean"

RUNTIME_POLICY_SUMMARY = {
    "bootstrap_date_policy": DATE_POLICY_BOOTSTRAP_LATEST_DAY,
    "execution_policy": EXECUTION_POLICY_BEST_EFFORT,
    "flask_handoff_policy": FLASK_HANDOFF_POLICY_ALWAYS,
    "update_policy": UPDATE_POLICY_INCREMENTAL,
    "historical_catch_up_policy": HISTORICAL_CATCH_UP_POLICY,
    "bootstrap_refresh_policy": BOOTSTRAP_REFRESH_POLICY,
    "automatic_coverage_contract": AUTO_COVERAGE_CONTRACT,
    "bootstrap_full_analysis_policy": BOOTSTRAP_FULL_ANALYSIS_POLICY,
    "startup_modes": (
        STARTUP_MODE_PENDING,
        STARTUP_MODE_CONTINUE,
        STARTUP_MODE_CLEAN,
    ),
}
