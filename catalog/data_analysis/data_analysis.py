"""
Batch analysis of JSONL telemetry files.

This script performs three exploratory analyses on machine telemetry stored in
JSONL files:

1. Dataset audit
   - checks timestamp quality and ordering
   - summarizes column availability
   - identifies columns that appear numeric
   - reports frequent values for selected context fields

2. Active-segment discovery
   - uses simple heuristics to estimate active machine periods
   - segments the data into active/inactive runs
   - summarizes duration, point counts, and selected signal statistics

3. Candidate event discovery
   - detects rows that may correspond to operator interventions or notable
     state changes
   - combines multiple rule-based triggers into an event score
   - groups nearby candidate rows into broader event windows

The script is intended as an exploratory utility, not as a validated detection
pipeline. Its thresholds and rules are heuristic and should be interpreted with
care.
"""

import glob
import json
import os

import numpy as np
import pandas as pd

from catalog.common.telemetry_prep import (
    prepare_timestamp_column,
    replace_unavailable,
    to_numeric,
)

# Folder containing input JSONL files.
FOLDER = r"./data"

# File name pattern for telemetry input files.
FILE_PATTERN = "*.jsonl"

# Core numeric signals expected to be useful for machine-state interpretation.
CORE_NUMERIC_SIGNALS = [
    "Srpm", "Sovr", "Sload", "Stemp",
    "Xabs", "Yabs", "Zabs",
    "Fact", "Fovr", "Frapidovr",
]

# Context fields that may help interpret execution state.
CORE_CONTEXT_SIGNALS = [
    "execution", "mode", "program", "Tool_number", "Tool_group",
]

# Heuristics for active segment discovery.
#
# These values are not machine-independent truths; they are pragmatic defaults
# chosen for exploratory analysis of dense telemetry streams.
DENSE_DT_THRESHOLD_SEC = 5.0
LONG_IDLE_DT_THRESHOLD_SEC = 30.0
MIN_ACTIVE_POINTS = 5

# Maximum time gap for grouping nearby candidate rows into one event.
MAX_EVENT_GAP_SEC = 10.0


def print_header(title, char="="):
    """Print a full-width section header for console reports."""
    print("\n" + char * 80)
    print(title)
    print(char * 80)


def print_subheader(title):
    """Print a subsection header for console reports."""
    print("\n" + "-" * 80)
    print(title)
    print("-" * 80)


def load_jsonl(path):
    """
    Load a JSONL file into a DataFrame.

    Blank lines are skipped. Malformed JSON lines are reported and ignored
    rather than aborting the full file load.

    Parameters
    ----------
    path : str
        Path to the JSONL file.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing all successfully parsed rows.
    """
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception as e:
                print(f"[WARNING] Failed to parse line {i} in {path}: {e}")
    return pd.DataFrame(rows)


def availability_fraction(series):
    """
    Compute the fraction of non-missing values in a series.

    'UNAVAILABLE' values are treated as missing.
    """
    s = replace_unavailable(series)
    return s.notna().mean()


def group_boolean_events(df, event_mask, time_col="timestamp", max_gap_sec=10.0):
    """
    Group nearby event-marked rows into broader event windows.

    Rows marked True in ``event_mask`` are sorted by time and merged into the
    same event when the gap between successive rows does not exceed
    ``max_gap_sec``.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataframe containing timestamps.
    event_mask : pandas.Series or array-like
        Boolean mask identifying candidate event rows.
    time_col : str, default="timestamp"
        Timestamp column used for grouping.
    max_gap_sec : float, default=10.0
        Maximum allowed time gap between rows in the same grouped event.

    Returns
    -------
    pandas.DataFrame
        One row per grouped event with start, end, point count, and duration.
    """
    event_rows = df.loc[event_mask].copy()
    if event_rows.empty:
        return pd.DataFrame()

    event_rows = event_rows.sort_values(time_col).reset_index()
    event_rows["dt_from_prev"] = event_rows[time_col].diff().dt.total_seconds()
    event_rows["new_group"] = (
        event_rows["dt_from_prev"].isna()
        | (event_rows["dt_from_prev"] > max_gap_sec)
    ).astype(int)
    event_rows["event_group"] = event_rows["new_group"].cumsum()

    grouped = (
        event_rows.groupby("event_group")
        .agg(
            start=(time_col, "min"),
            end=(time_col, "max"),
            n_points=("index", "size"),
        )
        .reset_index(drop=True)
    )
    grouped["duration_sec"] = (grouped["end"] - grouped["start"]).dt.total_seconds()
    return grouped


def dataset_audit(df):
    """
    Perform a basic audit of the dataset structure and column usability.

    This analysis checks:
    - whether timestamps exist and parse cleanly
    - whether timestamps are monotonic after sorting
    - time-gap statistics between rows
    - per-column availability
    - which columns appear numerically interpretable
    - common values in selected context fields

    Returns
    -------
    dict
        Dictionary of summary objects and tables for reporting.
    """
    result = {
        "shape": df.shape,
        "timestamp_ok": False,
        "timestamp_monotonic": False,
        "dt_stats": None,
        "availability_df": None,
        "candidate_numeric_df": None,
        "execution_counts": None,
        "mode_counts": None,
        "program_counts": None,
    }

    if "timestamp" not in df.columns:
        return result

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    result["timestamp_ok"] = df["timestamp"].notna().all()

    df = prepare_timestamp_column(df, time_col="timestamp", drop_invalid=False, sort=True, reset_index=True)
    result["timestamp_monotonic"] = df["timestamp"].is_monotonic_increasing

    df["dt_sec"] = df["timestamp"].diff().dt.total_seconds()
    result["dt_stats"] = df["dt_sec"].describe(
        percentiles=[0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]
    )

    availability = []
    candidate_numeric = []

    for col in df.columns:
        s = df[col]
        avail = availability_fraction(s)
        availability.append(
            {
                "column": col,
                "available_fraction": round(avail, 4),
                "missing_fraction": round(1 - avail, 4),
                "dtype": str(s.dtype),
            }
        )

        if col != "timestamp":
            numeric = to_numeric(s)
            frac_numeric = numeric.notna().mean()
            if frac_numeric > 0:
                candidate_numeric.append(
                    {
                        "column": col,
                        "numeric_fraction": round(frac_numeric, 4),
                    }
                )

    availability_df = pd.DataFrame(availability).sort_values(
        "available_fraction", ascending=False
    )
    candidate_numeric_df = pd.DataFrame(candidate_numeric).sort_values(
        "numeric_fraction", ascending=False
    )

    result["availability_df"] = availability_df
    result["candidate_numeric_df"] = candidate_numeric_df

    if "execution" in df.columns:
        result["execution_counts"] = (
            replace_unavailable(df["execution"])
            .value_counts(dropna=False)
            .head(20)
        )

    if "mode" in df.columns:
        result["mode_counts"] = (
            replace_unavailable(df["mode"])
            .value_counts(dropna=False)
            .head(20)
        )

    if "program" in df.columns:
        result["program_counts"] = (
            replace_unavailable(df["program"])
            .value_counts(dropna=False)
            .head(20)
        )

    return result


def active_segment_analysis(df):
    """
    Estimate active machine segments using simple telemetry heuristics.

    A row is considered potentially active if at least one of the following
    holds:
    - time gaps are dense enough
    - spindle RPM is above zero
    - spindle load is above zero

    Consecutive rows with the same active/inactive state are grouped into
    segments. This is a heuristic approximation, not a validated activity model.

    Returns
    -------
    dict
        Contains:
        - all discovered segments
        - filtered active segments
        - a short numeric summary
    """
    result = {
        "segments_df": None,
        "active_segments_df": None,
        "summary": {},
    }

    if "timestamp" not in df.columns:
        return result

    df = prepare_timestamp_column(df, time_col="timestamp", drop_invalid=False, sort=True, reset_index=True)
    df["dt_sec"] = df["timestamp"].diff().dt.total_seconds()

    for col in CORE_NUMERIC_SIGNALS:
        if col in df.columns:
            df[col] = to_numeric(df[col])

    df["dense_time"] = df["dt_sec"].fillna(999999) < DENSE_DT_THRESHOLD_SEC
    df["long_idle_gap"] = df["dt_sec"].fillna(0) > LONG_IDLE_DT_THRESHOLD_SEC

    if "Srpm" in df.columns:
        df["rpm_active"] = df["Srpm"].fillna(0) > 0
    else:
        df["rpm_active"] = False

    if "Sload" in df.columns:
        df["load_active"] = df["Sload"].fillna(0) > 0
    else:
        df["load_active"] = False

    df["active"] = df["dense_time"] | df["rpm_active"] | df["load_active"]

    df["segment_break"] = (df["active"] != df["active"].shift(1)).astype(int)
    df["segment_id"] = df["segment_break"].cumsum()

    agg_dict = {
        "start": ("timestamp", "min"),
        "end": ("timestamp", "max"),
        "points": ("timestamp", "size"),
        "median_dt": ("dt_sec", "median"),
    }

    if "Srpm" in df.columns:
        agg_dict["mean_rpm"] = ("Srpm", "mean")
        agg_dict["max_rpm"] = ("Srpm", "max")
    if "Sload" in df.columns:
        agg_dict["mean_load"] = ("Sload", "mean")
        agg_dict["max_load"] = ("Sload", "max")
    if "Sovr" in df.columns:
        agg_dict["mean_ovr"] = ("Sovr", "mean")
        agg_dict["max_ovr"] = ("Sovr", "max")

    segments = (
        df.groupby(["segment_id", "active"])
        .agg(**agg_dict)
        .reset_index()
    )

    segments["duration_sec"] = (segments["end"] - segments["start"]).dt.total_seconds()

    active_segments = segments[
        (segments["active"] == True)
        & (segments["points"] >= MIN_ACTIVE_POINTS)
    ].copy()

    result["segments_df"] = segments
    result["active_segments_df"] = active_segments
    result["summary"] = {
        "n_total_segments": len(segments),
        "n_active_segments": len(active_segments),
        "total_points": len(df),
        "active_points_estimate": int(df["active"].sum()),
    }

    return result


def candidate_event_analysis(df):
    """
    Detect rows and grouped windows that may correspond to notable events.

    This function combines several heuristic trigger types:
    - state/context changes (execution, mode, program)
    - rapid numeric changes
    - abrupt override drops
    - RPM/load collapse
    - transition from active to inactive

    Triggered rules are combined into a weighted event score. Rows meeting the
    score threshold are treated as candidate event rows and may then be grouped
    into larger event windows.

    Important:
    This is a heuristic ranking mechanism for inspection, not a ground-truth
    event detector.
    """
    result = {
        "rate_summary": None,
        "thresholds": {},
        "trigger_counts": {},
        "candidate_rows_df": None,
        "grouped_events_df": None,
        "summary": {},
    }

    if "timestamp" not in df.columns:
        return result

    df = prepare_timestamp_column(df, time_col="timestamp", drop_invalid=False, sort=True, reset_index=True)
    df["dt_sec"] = df["timestamp"].diff().dt.total_seconds()

    for col in ["Srpm", "Sovr", "Sload", "Stemp", "Fovr", "Frapidovr"]:
        if col in df.columns:
            df[col] = to_numeric(df[col])

    if "execution" in df.columns:
        df["execution_clean"] = replace_unavailable(df["execution"])
    if "mode" in df.columns:
        df["mode_clean"] = replace_unavailable(df["mode"])
    if "program" in df.columns:
        df["program_clean"] = replace_unavailable(df["program"])

    # Restrict rate estimates to plausible telemetry step sizes.
    valid_dt = df["dt_sec"].between(0.1, 30)

    rpm_active = (
        df["Srpm"].fillna(0) > 0
        if "Srpm" in df.columns
        else pd.Series(False, index=df.index)
    )
    load_active = (
        df["Sload"].fillna(0) > 0
        if "Sload" in df.columns
        else pd.Series(False, index=df.index)
    )
    machining_active = rpm_active | load_active

    events = pd.DataFrame(index=df.index)

    if "execution_clean" in df.columns:
        events["execution_change"] = df["execution_clean"] != df["execution_clean"].shift(1)

    if "mode_clean" in df.columns:
        events["mode_change"] = df["mode_clean"] != df["mode_clean"].shift(1)

    if "program_clean" in df.columns:
        events["program_change"] = df["program_clean"] != df["program_clean"].shift(1)

    for col in ["Srpm", "Sovr", "Sload", "Fovr", "Frapidovr"]:
        if col in df.columns:
            df[f"d_{col}"] = df[col].diff()
            df[f"rate_{col}"] = np.where(valid_dt, df[f"d_{col}"] / df["dt_sec"], np.nan)

    rate_cols = [c for c in df.columns if c.startswith("rate_")]
    if rate_cols:
        result["rate_summary"] = df[rate_cols].describe()

    # Thresholds are adaptive and intentionally less extreme than a pure
    # tail-only strategy; this favors exploratory recall over strict precision.
    for col in ["rate_Sovr", "rate_Srpm", "rate_Sload", "rate_Fovr", "rate_Frapidovr"]:
        if col in df.columns and df[col].notna().sum() > 20:
            thr95 = df[col].abs().quantile(0.95)
            threshold = max(thr95, 1e-9)
            result["thresholds"][col] = float(threshold)
            events[f"{col}_jump"] = df[col].abs() > threshold

    if "Sovr" in df.columns:
        df["d_Sovr"] = df["Sovr"].diff()
        events["sovr_drop"] = df["d_Sovr"] <= -10

    if "Fovr" in df.columns:
        df["d_Fovr"] = df["Fovr"].diff()
        events["fovr_drop"] = df["d_Fovr"] <= -10

    if "Srpm" in df.columns:
        prev_rpm = df["Srpm"].shift(1)
        events["rpm_collapse"] = (
            machining_active.shift(1).fillna(False)
            & prev_rpm.notna()
            & df["Srpm"].notna()
            & (prev_rpm > 0)
            & (df["Srpm"] < 0.5 * prev_rpm)
        )

    if "Sload" in df.columns:
        prev_load = df["Sload"].shift(1)
        events["load_collapse"] = (
            machining_active.shift(1).fillna(False)
            & prev_load.notna()
            & df["Sload"].notna()
            & (prev_load > 0)
            & (df["Sload"] < 0.5 * prev_load)
        )

    events["active_to_inactive"] = machining_active.shift(1).fillna(False) & (~machining_active)

    events = events.fillna(False)
    if len(events) > 0:
        events.iloc[0] = False

    for col in events.columns:
        result["trigger_counts"][col] = int(events[col].sum())

    weights = {
        "execution_change": 2,
        "mode_change": 2,
        "program_change": 1,
        "sovr_drop": 3,
        "fovr_drop": 3,
        "rpm_collapse": 3,
        "load_collapse": 2,
        "active_to_inactive": 2,
    }

    events["event_score"] = 0
    for col in events.columns:
        if col == "event_score":
            continue
        w = weights.get(col, 1)
        events["event_score"] += events[col].astype(int) * w

    candidate_mask = events["event_score"] >= 2

    candidate_cols = ["timestamp", "dt_sec"]
    for col in [
        "Srpm", "Sovr", "Sload", "Fovr", "Frapidovr",
        "execution", "mode", "program",
    ]:
        if col in df.columns:
            candidate_cols.append(col)

    candidate_rows = df.loc[candidate_mask, candidate_cols].copy()
    candidate_rows["event_score"] = events.loc[candidate_mask, "event_score"].values

    fired_rules = []
    event_cols = [c for c in events.columns if c != "event_score"]
    for idx in candidate_rows.index:
        fired = [c for c in event_cols if events.loc[idx, c]]
        fired_rules.append(", ".join(fired))
    candidate_rows["fired_rules"] = fired_rules

    grouped_events = group_boolean_events(
        df,
        candidate_mask,
        time_col="timestamp",
        max_gap_sec=MAX_EVENT_GAP_SEC,
    )

    result["candidate_rows_df"] = candidate_rows
    result["grouped_events_df"] = grouped_events
    result["summary"] = {
        "n_candidate_rows": len(candidate_rows),
        "n_grouped_events": 0 if grouped_events is None or grouped_events.empty else len(grouped_events),
    }

    return result


def analyze_file(path):
    """
    Run the full analysis pipeline for one telemetry file and print a report.

    The report contains:
    1. dataset audit
    2. active-segment discovery
    3. candidate-event discovery
    4. short combined summary
    """
    print_header(f"FILE: {os.path.basename(path)}")

    try:
        df = load_jsonl(path)
    except Exception as e:
        print(f"[ERROR] Could not load file: {e}")
        return

    if df.empty:
        print("[WARNING] File is empty or unreadable.")
        return

    print_subheader("1) DATASET AUDIT")
    audit = dataset_audit(df)

    print(f"Shape: {audit['shape']}")
    print(f"Timestamp parsed fully: {audit['timestamp_ok']}")
    print(f"Timestamp monotonic after sorting: {audit['timestamp_monotonic']}")

    if audit["dt_stats"] is not None:
        print("\nTime gap statistics (seconds):")
        print(audit["dt_stats"].to_string())

    if audit["availability_df"] is not None:
        print("\nTop columns by availability:")
        print(audit["availability_df"].head(25).to_string(index=False))

    if audit["candidate_numeric_df"] is not None:
        print("\nTop numeric-like columns:")
        print(audit["candidate_numeric_df"].head(25).to_string(index=False))

    if audit["execution_counts"] is not None:
        print("\nexecution values:")
        print(audit["execution_counts"].to_string())

    if audit["mode_counts"] is not None:
        print("\nmode values:")
        print(audit["mode_counts"].to_string())

    if audit["program_counts"] is not None:
        print("\nprogram values:")
        print(audit["program_counts"].head(20).to_string())

    print_subheader("2) ACTIVE-SEGMENT DISCOVERY")
    seg = active_segment_analysis(df)

    print("Segment summary:")
    print(seg["summary"])

    if seg["segments_df"] is not None:
        print("\nAll segments:")
        print(seg["segments_df"].to_string(index=False))

    if seg["active_segments_df"] is not None and not seg["active_segments_df"].empty:
        print("\nActive segments only:")
        print(seg["active_segments_df"].to_string(index=False))
    else:
        print("\nNo active segments detected with current heuristics.")

    print_subheader("3) CANDIDATE-EVENT DISCOVERY")
    ev = candidate_event_analysis(df)

    if ev["rate_summary"] is not None:
        print("Rate summary:")
        print(ev["rate_summary"].to_string())

    print("\nEvent thresholds:")
    if ev["thresholds"]:
        for k, v in ev["thresholds"].items():
            print(f"  {k}: {v}")
    else:
        print("  No thresholds computed.")

    print("\nCandidate event summary:")
    print(ev["summary"])

    if ev["candidate_rows_df"] is not None and not ev["candidate_rows_df"].empty:
        print("\nCandidate rows:")
        print(ev["candidate_rows_df"].head(50).to_string(index=False))
    else:
        print("\nNo candidate rows detected.")

    if ev["grouped_events_df"] is not None and not ev["grouped_events_df"].empty:
        print("\nGrouped events:")
        print(ev["grouped_events_df"].to_string(index=False))
    else:
        print("\nNo grouped events detected.")

    print_subheader("4) COMBINED SHORT SUMMARY")

    usable_cols = []
    if audit["availability_df"] is not None and audit["candidate_numeric_df"] is not None:
        avail = audit["availability_df"].set_index("column")
        numlike = audit["candidate_numeric_df"].set_index("column")

        for col in CORE_NUMERIC_SIGNALS:
            if col in avail.index and col in numlike.index:
                if (
                    avail.loc[col, "available_fraction"] >= 0.8
                    and numlike.loc[col, "numeric_fraction"] >= 0.8
                ):
                    usable_cols.append(col)

    print(f"Likely usable numeric/core signals: {usable_cols}")
    print(f"Likely active segments found: {seg['summary'].get('n_active_segments', 0)}")
    print(f"Candidate grouped events found: {ev['summary'].get('n_grouped_events', 0)}")


def main():
    """
    Run the full batch analysis over all matching JSONL files in the input folder.
    """
    print_header("BATCH ANALYSIS OF JSONL TELEMETRY FILES")

    files = sorted(glob.glob(os.path.join(FOLDER, FILE_PATTERN)))
    if not files:
        print(f"No files found in folder: {FOLDER!r} with pattern {FILE_PATTERN!r}")
        return

    print(f"Found {len(files)} files.\n")
    for path in files:
        analyze_file(path)


if __name__ == "__main__":
    main()
