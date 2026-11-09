"""Witness tests for the historical experiment's measurement/oracle policy."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from catalog.flask_app.services import digital_twin_service as production
from research.state_semantics_evolution_2026.metrics import evaluate_window, summarize, threshold_classifier


def _rows(signals):
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [{"machine_id": "SYNTHETIC", "timestamp": (start+timedelta(seconds=i)).isoformat(),
             "execution": e, "Srpm": rpm, "Sload": load}
            for i,(e,rpm,load) in enumerate(signals)]


def test_threshold_clone_changes_only_spindle_boundary_and_preserves_production():
    rows = _rows([("UNAVAILABLE", 100, 0), ("UNAVAILABLE", 100.1, 0),
                  ("UNAVAILABLE", 150, "UNAVAILABLE"), ("UNAVAILABLE", 150.1, 0),
                  ("UNAVAILABLE", 120, 2), ("ACTIVE", 120, 0), ("OTHER", 120, 0)])
    frame = pd.DataFrame(rows)
    baseline = production._classify_rows(frame)["state_class"].fillna("unclassified").tolist()
    evolved = threshold_classifier(150)(frame)["state_class"].fillna("unclassified").tolist()
    assert baseline == ["non_active", "active", "active", "active", "active", "active", "unclassified"]
    assert evolved == ["non_active", "non_active", "non_active", "active", "active", "active", "unclassified"]
    assert production._classify_rows(frame)["state_class"].fillna("unclassified").tolist() == baseline
    result = evaluate_window(rows)
    assert result["experiments"]["E6"]["expected_changed_row_refs"] == [1,2]
    assert result["experiments"]["E6"]["verdict"] == "pass"


def test_interval_accounting_excludes_gaps_and_unknown_without_counting_downtime():
    rows = _rows([("ACTIVE", 0, 0), ("OTHER", 900, 9), ("STOPPED", 0, 0), (None,None,None)])
    rows[2]["timestamp"] = "2026-01-01T00:01:01Z"
    rows[3]["timestamp"] = "2026-01-01T00:01:01.050Z"
    result = summarize(rows)
    assert result["durations_seconds"] == dict(active=1, non_active=0, unclassified=0)
    assert result["excluded"]["long_gap_seconds"] == 60
    assert result["excluded"]["subminimum_seconds"] == pytest.approx(.05)
    assert result["excluded"]["long_gaps"] == 1
    assert result["usable_intervals"] == 1
    assert result["positive_span_seconds"] == pytest.approx(61.05)
    assert result["wall_span_coverage_fraction"] == pytest.approx(1/61.05)
    assert result["continuity"] == pytest.approx(1/3)


def test_unclassified_usable_time_and_midnight_split_are_measured():
    rows = _rows([("OTHER", 900, 9), ("ACTIVE", 0, 0), ("STOPPED", 0, 0)])
    rows[0]["timestamp"] = "2025-12-31T23:59:59Z"
    rows[1]["timestamp"] = "2026-01-01T00:00:01Z"
    rows[2]["timestamp"] = "2026-01-01T00:00:02Z"
    result = summarize(rows)
    assert result["durations_seconds"] == dict(active=1,non_active=0,unclassified=2)
    assert result["coverage_fraction"] == pytest.approx(1/3)


def test_conflicting_timestamp_permutation_is_conditional_and_safe_variant_preserves():
    rows = _rows([("ACTIVE",0,0), ("STOPPED",0,0), ("ACTIVE",0,0), ("STOPPED",0,0)])
    rows[1]["timestamp"] = rows[0]["timestamp"]
    result = evaluate_window(rows)
    assert result["baseline"]["conflicting_state_timestamps"] == 1
    assert result["experiments"]["E5a"]["verdict"] == "pass"
    assert result["experiments"]["E5_conflict"]["verdict"] == "ambiguous_ties"
    assert result["experiments"]["E5b"]["verdict"] == "pass"


def test_four_mutants_activate_on_witnesses_and_correct_changes_do_not_raise_alarms():
    rows = _rows([("ACTIVE",0,0), (None,120,0), (None,0,2), ("OTHER",0,0),
                  ("STOPPED",0,0), (None,None,None), ("ACTIVE",0,0)])
    result = evaluate_window(rows)
    assert all(item["activated"] and item["detected"] for item in result["defects"].values())
    assert result["unexpected_experiment_outcomes"] == 0
    assert result["experiments"]["E4"]["unexpected_row_refs"] == []


def test_unactivated_mutants_are_not_reported_as_detected():
    rows = _rows([("ACTIVE",0,0)]*4)
    result = evaluate_window(rows)
    assert result["defects"]["wrong_unit_adapter"]["activated"] is False
    assert result["defects"]["wrong_unit_adapter"]["detected"] is False
    assert result["defects"]["omitted_provider_load"]["detected"] is False


def test_gap_without_distinct_timestamps_is_explicitly_unactivated():
    rows = _rows([("ACTIVE",0,0)]*2)
    rows[1]["timestamp"] = rows[0]["timestamp"]
    assert evaluate_window(rows)["experiments"]["E5b"]["verdict"] == "not_activated"


def test_transition_uses_final_tied_state_that_owns_outgoing_interval():
    rows = _rows([("ACTIVE",0,0), ("ACTIVE",0,0), ("STOPPED",0,0), ("STOPPED",0,0)])
    rows[2]["timestamp"] = rows[1]["timestamp"]
    result = summarize(rows)
    assert result["transition_boundaries"] == [("2026-01-01T00:00:01+00:00","active","non_active")]
    evaluated = evaluate_window(rows)
    assert evaluated["baseline"]["service_crosscheck_passed"]
    trace = evaluated["defects"]["omitted_rename_adapter"]["changed_row_trace"]
    assert trace[0] == {"row_ref":0,"before":"active","after":"non_active"}
