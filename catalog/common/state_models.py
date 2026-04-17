"""Machine state/situation heuristic models for telemetry analysis."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from catalog.common.state_events import build_fired_rules


@dataclass(frozen=True)
class StateInferenceConfig:
    """Configuration for machine state and intervention-candidate inference."""

    dense_dt_sec: float = 5.0
    rpm_active_threshold: float = 100.0
    load_active_threshold: float = 1.0

    rpm_rate_q: float = 0.95
    load_rate_q: float = 0.95
    ovr_rate_q: float = 0.95
    fovr_rate_q: float = 0.95
    frapidovr_rate_q: float = 0.95

    ovr_drop_threshold: float = -10.0
    fovr_drop_threshold: float = -10.0
    rpm_collapse_ratio: float = 0.5
    load_collapse_ratio: float = 0.5

    merge_gap_sec: float = 30.0


def infer_states_for_machine(
    g: pd.DataFrame,
    *,
    config: StateInferenceConfig = StateInferenceConfig(),
    time_col: str = "timestamp",
    rpm_col: str = "Srpm",
    load_col: str = "Sload",
    ovr_col: str = "Sovr",
    exec_col: str = "execution",
    mode_col: str = "mode",
    prog_col: str = "program",
    fovr_col: str = "Fovr",
    frapidovr_col: str = "Frapidovr",
) -> pd.DataFrame:
    """Infer row-level states and candidate intervention flags for one machine/day."""
    g = g.sort_values(time_col).copy()
    g["dt_sec"] = g[time_col].diff().dt.total_seconds()

    dense = g["dt_sec"].fillna(np.inf) < config.dense_dt_sec

    rpm_active = (
        g[rpm_col].fillna(0) > config.rpm_active_threshold
        if rpm_col in g.columns else pd.Series(False, index=g.index, dtype=bool)
    )
    load_active = (
        g[load_col].fillna(0) > config.load_active_threshold
        if load_col in g.columns else pd.Series(False, index=g.index, dtype=bool)
    )

    g["dense"] = dense.fillna(False)
    g["active"] = (dense & (rpm_active | load_active)).fillna(False)
    g["dense_idle"] = (dense & ~(rpm_active | load_active)).fillna(False)

    valid_dt = g["dt_sec"].between(0.1, 30.0)
    rate_specs = [
        (rpm_col, "rate_rpm", config.rpm_rate_q),
        (load_col, "rate_load", config.load_rate_q),
        (ovr_col, "rate_ovr", config.ovr_rate_q),
        (fovr_col, "rate_fovr", config.fovr_rate_q),
        (frapidovr_col, "rate_frapidovr", config.frapidovr_rate_q),
    ]

    thresholds = {}
    for raw_col, rate_col, q in rate_specs:
        if raw_col in g.columns:
            g[rate_col] = np.where(valid_dt, g[raw_col].diff() / g["dt_sec"], np.nan)
            s = g[rate_col].abs().dropna()
            thresholds[rate_col] = s.quantile(q) if len(s) > 20 else np.nan
        else:
            g[rate_col] = np.nan
            thresholds[rate_col] = np.nan

    events = pd.DataFrame(index=g.index)
    events["execution_change"] = (g[exec_col] != g[exec_col].shift(1)).fillna(False) if exec_col in g.columns else False
    events["mode_change"] = (g[mode_col] != g[mode_col].shift(1)).fillna(False) if mode_col in g.columns else False
    events["program_change"] = (g[prog_col] != g[prog_col].shift(1)).fillna(False) if prog_col in g.columns else False

    for _, rate_col, _ in rate_specs:
        thr = thresholds.get(rate_col, np.nan)
        events[f"{rate_col}_jump"] = (g[rate_col].abs() > thr) if pd.notna(thr) and rate_col in g.columns else False

    if ovr_col in g.columns:
        g["d_ovr"] = g[ovr_col].diff()
        events["ovr_drop"] = g["d_ovr"] <= config.ovr_drop_threshold
    else:
        events["ovr_drop"] = False

    if fovr_col in g.columns:
        g["d_fovr"] = g[fovr_col].diff()
        events["fovr_drop"] = g["d_fovr"] <= config.fovr_drop_threshold
    else:
        events["fovr_drop"] = False

    if rpm_col in g.columns:
        prev_rpm = g[rpm_col].shift(1)
        events["rpm_collapse"] = (
            g["active"].shift(1).fillna(False)
            & prev_rpm.notna()
            & g[rpm_col].notna()
            & (prev_rpm > 0)
            & (g[rpm_col] < config.rpm_collapse_ratio * prev_rpm)
        )
    else:
        events["rpm_collapse"] = False

    if load_col in g.columns:
        prev_load = g[load_col].shift(1)
        events["load_collapse"] = (
            g["active"].shift(1).fillna(False)
            & prev_load.notna()
            & g[load_col].notna()
            & (prev_load > 0)
            & (g[load_col] < config.load_collapse_ratio * prev_load)
        )
    else:
        events["load_collapse"] = False

    active_prev = g["active"].shift(1).fillna(False)
    active_now = g["active"].fillna(False)
    events["active_to_inactive"] = active_prev & (~active_now)
    events["inactive_to_active"] = (~active_prev) & active_now

    events = events.fillna(False).astype(bool)
    if len(events) > 0:
        events.iloc[0] = False

    weights = {
        "execution_change": 2,
        "mode_change": 2,
        "program_change": 1,
        "rate_rpm_jump": 1,
        "rate_load_jump": 1,
        "rate_ovr_jump": 1,
        "rate_fovr_jump": 1,
        "rate_frapidovr_jump": 1,
        "ovr_drop": 3,
        "fovr_drop": 3,
        "rpm_collapse": 3,
        "load_collapse": 2,
        "active_to_inactive": 2,
        "inactive_to_active": 1,
    }

    events["event_score"] = 0
    for col in events.columns:
        if col == "event_score":
            continue
        events["event_score"] += events[col].astype(int) * weights.get(col, 1)

    active_next = g["active"].shift(-1).fillna(False)
    near_active = active_now | active_prev | active_next

    base_candidate = pd.Series(False, index=g.index, dtype=bool)
    for col in [
        "execution_change",
        "mode_change",
        "active_to_inactive",
        "inactive_to_active",
        "ovr_drop",
        "fovr_drop",
        "rpm_collapse",
        "load_collapse",
    ]:
        base_candidate |= events[col] if col in events.columns else False

    numeric_candidate = pd.Series(False, index=g.index, dtype=bool)
    for col in ["rate_rpm_jump", "rate_load_jump", "rate_ovr_jump", "rate_fovr_jump", "rate_frapidovr_jump"]:
        numeric_candidate |= events[col] if col in events.columns else False

    g["intervention_candidate"] = near_active & (base_candidate | numeric_candidate)
    g["state"] = np.where(
        g["intervention_candidate"],
        "intervention_candidate",
        np.where(g["active"], "active", np.where(g["dense_idle"], "dense_idle", "idle")),
    )

    if len(g) > 0:
        first_idx = g.index[0]
        g.loc[first_idx, "intervention_candidate"] = False
        if g.loc[first_idx, "state"] == "intervention_candidate":
            g.loc[first_idx, "state"] = "active" if bool(g.loc[first_idx, "active"]) else "idle"

    g["event_score"] = events["event_score"]
    g["fired_rules"] = [build_fired_rules(events, idx) for idx in g.index]

    g.attrs["trigger_counts"] = {col: int(events[col].sum()) for col in events.columns if col != "event_score"}
    g.attrs["thresholds"] = thresholds
    return g
