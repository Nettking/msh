from __future__ import annotations

import pandas as pd

from catalog.common.state_events import extract_intervention_candidates
from catalog.common.state_models import StateInferenceConfig, infer_states_for_machine


def _base_frame(periods: int = 25) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range(
                "2026-01-01 00:00:00", periods=periods, freq="1s"
            ),
            "machine_id": ["M1"] * periods,
            "date": [pd.Timestamp("2026-01-01").date()] * periods,
            "Srpm": [1000.0] * periods,
            "Sload": [10.0] * periods,
            "Sovr": [100.0] * periods,
            "Fovr": [100.0] * periods,
            "Frapidovr": [100.0] * periods,
            "execution": ["ACTIVE"] * periods,
            "mode": ["AUTO"] * periods,
            "program": ["P1"] * periods,
        }
    )


def _infer(frame: pd.DataFrame) -> pd.DataFrame:
    return infer_states_for_machine(
        frame,
        config=StateInferenceConfig(
            rpm_rate_q=0.5,
            load_rate_q=0.5,
            ovr_rate_q=0.5,
            fovr_rate_q=0.5,
            frapidovr_rate_q=0.5,
        ),
    )


def test_pure_rate_load_jump_is_process_event_not_operator_intervention() -> None:
    frame = _base_frame()
    frame.loc[12, "Sload"] = 20.0
    frame.loc[13:, "Sload"] = 20.0

    inferred = _infer(frame)
    row = inferred.iloc[12]

    assert bool(row["process_event_candidate"]) is True
    assert bool(row["operator_intervention_candidate"]) is False
    assert bool(row["intervention_candidate"]) is False
    assert row["state"] == "process_event_candidate"
    assert "rate_load_jump" in row["fired_rules"]


def test_override_drop_is_operator_intervention_candidate() -> None:
    frame = _base_frame()
    frame.loc[12:, "Sovr"] = 80.0

    inferred = _infer(frame)
    row = inferred.iloc[12]

    assert bool(row["operator_intervention_candidate"]) is True
    assert bool(row["intervention_candidate"]) is True
    assert row["state"] == "intervention_candidate"
    assert "ovr_drop" in row["fired_rules"]


def test_weak_event_adjacent_to_strong_event_becomes_operator_intervention_candidate() -> (
    None
):
    frame = _base_frame()
    frame.loc[11, "Sload"] = 20.0
    frame.loc[12:, "Sload"] = 20.0
    frame.loc[12:, "Sovr"] = 80.0

    inferred = _infer(frame)
    weak_row = inferred.iloc[11]

    assert bool(weak_row["process_event_candidate"]) is False
    assert bool(weak_row["operator_intervention_candidate"]) is True
    assert bool(weak_row["intervention_candidate"]) is True
    assert weak_row["state"] == "intervention_candidate"
    assert "rate_load_jump" in weak_row["fired_rules"]


def test_extract_intervention_candidates_excludes_process_only_events() -> None:
    frame = _base_frame()
    frame.loc[10, "Sload"] = 20.0
    frame.loc[11:, "Sload"] = 20.0
    frame.loc[15:, "Sovr"] = 80.0

    inferred = _infer(frame)
    candidates = extract_intervention_candidates(inferred)

    assert len(candidates) == 1
    row = candidates.iloc[0]
    assert row["timestamp"] == frame.loc[15, "timestamp"]
    assert bool(row["operator_intervention_candidate"]) is True
    assert "process_event_candidate" in candidates.columns
    assert frame.loc[10, "timestamp"] not in set(candidates["timestamp"])
