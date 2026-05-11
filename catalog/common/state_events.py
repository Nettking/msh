"""Event/window and interval helpers for telemetry state analysis."""

from __future__ import annotations

import pandas as pd


def build_fired_rules(events_df: pd.DataFrame, idx, *, score_col: str = "event_score") -> str:
    """Return comma-separated event rule names that fired for one row."""
    fired = [col for col in events_df.columns if col != score_col and bool(events_df.loc[idx, col])]
    return ", ".join(fired)


def group_boolean_events(
    df: pd.DataFrame,
    event_mask,
    *,
    time_col: str = "timestamp",
    max_gap_sec: float = 10.0,
) -> pd.DataFrame:
    """Group nearby event rows into broader event windows."""
    event_rows = df.loc[event_mask].copy()
    if event_rows.empty:
        return pd.DataFrame()

    event_rows = event_rows.sort_values(time_col).reset_index()
    event_rows["dt_from_prev"] = event_rows[time_col].diff().dt.total_seconds()
    event_rows["new_group"] = (
        event_rows["dt_from_prev"].isna() | (event_rows["dt_from_prev"] > max_gap_sec)
    ).astype(int)
    event_rows["event_group"] = event_rows["new_group"].cumsum()

    grouped = (
        event_rows.groupby("event_group")
        .agg(start=(time_col, "min"), end=(time_col, "max"), n_points=("index", "size"))
        .reset_index(drop=True)
    )
    grouped["duration_sec"] = (grouped["end"] - grouped["start"]).dt.total_seconds()
    return grouped


def rows_to_state_intervals(
    g: pd.DataFrame,
    *,
    merge_gap_sec: float,
    time_col: str = "timestamp",
    machine_col: str = "machine_id",
    date_col: str = "date",
) -> pd.DataFrame:
    """Merge neighboring rows with identical state into intervals."""
    g = g.sort_values(time_col).copy()
    if len(g) == 0:
        return pd.DataFrame(columns=[machine_col, "state", "start", "end", "duration_sec", "n_points", date_col])

    rows = []
    current_state = g.iloc[0]["state"]
    start_time = g.iloc[0][time_col]
    prev_time = g.iloc[0][time_col]
    n_points = 1
    machine_id = g.iloc[0][machine_col]
    day_value = g.iloc[0][date_col]

    for i in range(1, len(g)):
        t = g.iloc[i][time_col]
        state = g.iloc[i]["state"]
        gap = (t - prev_time).total_seconds()
        if (state == current_state) and (gap <= merge_gap_sec):
            prev_time = t
            n_points += 1
            continue

        rows.append(
            {
                machine_col: machine_id,
                date_col: day_value,
                "state": current_state,
                "start": start_time,
                "end": prev_time,
                "duration_sec": (prev_time - start_time).total_seconds(),
                "n_points": n_points,
            }
        )
        current_state = state
        start_time = t
        prev_time = t
        n_points = 1

    rows.append(
        {
            machine_col: machine_id,
            date_col: day_value,
            "state": current_state,
            "start": start_time,
            "end": prev_time,
            "duration_sec": (prev_time - start_time).total_seconds(),
            "n_points": n_points,
        }
    )

    return pd.DataFrame(rows)


def extract_intervention_candidates(
    g: pd.DataFrame,
    *,
    time_col: str = "timestamp",
    date_col: str = "date",
    machine_col: str = "machine_id",
    signal_cols: tuple[str, ...] = (
        "Srpm",
        "Sload",
        "Sovr",
        "Fovr",
        "Frapidovr",
        "execution",
        "mode",
        "program",
    ),
) -> pd.DataFrame:
    """Select candidate intervention rows with a stable export schema."""
    cols = [date_col, machine_col, time_col, "state", "event_score", "fired_rules"]
    cols.extend(
        col
        for col in ("operator_intervention_candidate", "process_event_candidate")
        if col in g.columns
    )
    cols.extend([col for col in signal_cols if col in g.columns])
    out = g.loc[g["intervention_candidate"], cols].copy()
    return out.rename(columns={time_col: "timestamp"})
