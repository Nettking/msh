import os
import json
import glob
import numpy as np
import pandas as pd

# ============================================================
# CONFIG
# ============================================================

FOLDER = r"./data"   # change this to your folder
FILE_PATTERN = "*.jsonl"

# Signals we will try to use if present
CORE_NUMERIC_SIGNALS = [
    "Srpm", "Sovr", "Sload", "Stemp",
    "Xabs", "Yabs", "Zabs",
    "Fact", "Fovr", "Frapidovr"
]

CORE_CONTEXT_SIGNALS = [
    "execution", "mode", "program", "Tool_number", "Tool_group"
]

# Heuristics for active segment discovery
DENSE_DT_THRESHOLD_SEC = 5.0
LONG_IDLE_DT_THRESHOLD_SEC = 30.0
MIN_ACTIVE_POINTS = 5

# Heuristics for event grouping
MAX_EVENT_GAP_SEC = 10.0


# ============================================================
# HELPERS
# ============================================================

def print_header(title, char="="):
    print("\n" + char * 80)
    print(title)
    print(char * 80)

def print_subheader(title):
    print("\n" + "-" * 80)
    print(title)
    print("-" * 80)

def load_jsonl(path):
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

def replace_unavailable_with_nan(series):
    if series.dtype == object:
        return series.replace("UNAVAILABLE", np.nan)
    return series

def try_convert_numeric(series):
    s = replace_unavailable_with_nan(series)
    return pd.to_numeric(s, errors="coerce")

def availability_fraction(series):
    s = replace_unavailable_with_nan(series)
    return s.notna().mean()

def group_boolean_events(df, event_mask, time_col="timestamp", max_gap_sec=10.0):
    """
    Groups nearby candidate rows into event windows.
    Returns a dataframe with one row per grouped event.
    """
    event_rows = df.loc[event_mask].copy()
    if event_rows.empty:
        return pd.DataFrame()

    event_rows = event_rows.sort_values(time_col).reset_index()
    event_rows["dt_from_prev"] = event_rows[time_col].diff().dt.total_seconds()
    event_rows["new_group"] = (
        event_rows["dt_from_prev"].isna() |
        (event_rows["dt_from_prev"] > max_gap_sec)
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


# ============================================================
# ANALYSIS 1: DATASET AUDIT
# ============================================================

def dataset_audit(df):
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

    df = df.sort_values("timestamp").reset_index(drop=True)
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
        availability.append({
            "column": col,
            "available_fraction": round(avail, 4),
            "missing_fraction": round(1 - avail, 4),
            "dtype": str(s.dtype),
        })

        if col != "timestamp":
            numeric = try_convert_numeric(s)
            frac_numeric = numeric.notna().mean()
            if frac_numeric > 0:
                candidate_numeric.append({
                    "column": col,
                    "numeric_fraction": round(frac_numeric, 4),
                })

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
            replace_unavailable_with_nan(df["execution"])
            .value_counts(dropna=False)
            .head(20)
        )

    if "mode" in df.columns:
        result["mode_counts"] = (
            replace_unavailable_with_nan(df["mode"])
            .value_counts(dropna=False)
            .head(20)
        )

    if "program" in df.columns:
        result["program_counts"] = (
            replace_unavailable_with_nan(df["program"])
            .value_counts(dropna=False)
            .head(20)
        )

    return result


# ============================================================
# ANALYSIS 2: ACTIVE SEGMENT DISCOVERY
# ============================================================

def active_segment_analysis(df):
    result = {
        "segments_df": None,
        "active_segments_df": None,
        "summary": {}
    }

    if "timestamp" not in df.columns:
        return result

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["dt_sec"] = df["timestamp"].diff().dt.total_seconds()

    for col in CORE_NUMERIC_SIGNALS:
        if col in df.columns:
            df[col] = try_convert_numeric(df[col])

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
        (segments["active"] == True) &
        (segments["points"] >= MIN_ACTIVE_POINTS)
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


# ============================================================
# ANALYSIS 3: CANDIDATE EVENT DISCOVERY
# ============================================================
def candidate_event_analysis(df):
    result = {  
        "rate_summary": None,
        "thresholds": {},
        "trigger_counts": {},
        "candidate_rows_df": None,
        "grouped_events_df": None,
        "summary": {}
    }

    if "timestamp" not in df.columns:
        return result

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["dt_sec"] = df["timestamp"].diff().dt.total_seconds()

    # Convert numeric signals
    for col in ["Srpm", "Sovr", "Sload", "Stemp", "Fovr", "Frapidovr"]:
        if col in df.columns:
            df[col] = try_convert_numeric(df[col])

    # Clean text/context columns
    if "execution" in df.columns:
        df["execution_clean"] = replace_unavailable_with_nan(df["execution"])
    if "mode" in df.columns:
        df["mode_clean"] = replace_unavailable_with_nan(df["mode"])
    if "program" in df.columns:
        df["program_clean"] = replace_unavailable_with_nan(df["program"])

    # Only trust dt within a reasonable range
    valid_dt = df["dt_sec"].between(0.1, 30)

    # Basic active machining approximation
    rpm_active = df["Srpm"].fillna(0) > 0 if "Srpm" in df.columns else pd.Series(False, index=df.index)
    load_active = df["Sload"].fillna(0) > 0 if "Sload" in df.columns else pd.Series(False, index=df.index)
    machining_active = rpm_active | load_active

    events = pd.DataFrame(index=df.index)

    # --------------------------------------------------------
    # 1) Context/state changes
    # --------------------------------------------------------
    if "execution_clean" in df.columns:
        events["execution_change"] = df["execution_clean"] != df["execution_clean"].shift(1)

    if "mode_clean" in df.columns:
        events["mode_change"] = df["mode_clean"] != df["mode_clean"].shift(1)

    if "program_clean" in df.columns:
        events["program_change"] = df["program_clean"] != df["program_clean"].shift(1)

    # --------------------------------------------------------
    # 2) Numeric rate changes
    # --------------------------------------------------------
    for col in ["Srpm", "Sovr", "Sload", "Fovr", "Frapidovr"]:
        if col in df.columns:
            df[f"d_{col}"] = df[col].diff()
            df[f"rate_{col}"] = np.where(valid_dt, df[f"d_{col}"] / df["dt_sec"], np.nan)

    rate_cols = [c for c in df.columns if c.startswith("rate_")]
    if rate_cols:
        result["rate_summary"] = df[rate_cols].describe()

    # Adaptive but less extreme thresholds
    for col in ["rate_Sovr", "rate_Srpm", "rate_Sload", "rate_Fovr", "rate_Frapidovr"]:
        if col in df.columns and df[col].notna().sum() > 20:
            thr95 = df[col].abs().quantile(0.95)
            thr99 = df[col].abs().quantile(0.99)
            threshold = max(thr95, 1e-9)  # less strict than 0.99
            result["thresholds"][col] = float(threshold)
            events[f"{col}_jump"] = df[col].abs() > threshold

    # --------------------------------------------------------
    # 3) Specific intervention-like patterns
    # --------------------------------------------------------

    # Sudden override drop
    if "Sovr" in df.columns:
        df["d_Sovr"] = df["Sovr"].diff()
        events["sovr_drop"] = df["d_Sovr"] <= -10

    if "Fovr" in df.columns:
        df["d_Fovr"] = df["Fovr"].diff()
        events["fovr_drop"] = df["d_Fovr"] <= -10

    # RPM collapse while machine was active
    if "Srpm" in df.columns:
        prev_rpm = df["Srpm"].shift(1)
        events["rpm_collapse"] = (
            machining_active.shift(1).fillna(False) &
            prev_rpm.notna() &
            df["Srpm"].notna() &
            (prev_rpm > 0) &
            (df["Srpm"] < 0.5 * prev_rpm)
        )

    # Load collapse while machine was active
    if "Sload" in df.columns:
        prev_load = df["Sload"].shift(1)
        events["load_collapse"] = (
            machining_active.shift(1).fillna(False) &
            prev_load.notna() &
            df["Sload"].notna() &
            (prev_load > 0) &
            (df["Sload"] < 0.5 * prev_load)
        )

    # Stop/idle transition from active state
    events["active_to_inactive"] = machining_active.shift(1).fillna(False) & (~machining_active)

    # --------------------------------------------------------
    # 4) Clean up and inspect trigger counts
    # --------------------------------------------------------
    events = events.fillna(False)
    if len(events) > 0:
        events.iloc[0] = False

    for col in events.columns:
        result["trigger_counts"][col] = int(events[col].sum())

    # Weighted score instead of simple >=2 rule
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

    # Less strict than before
    candidate_mask = events["event_score"] >= 2

    candidate_cols = ["timestamp", "dt_sec"]
    for col in [
        "Srpm", "Sovr", "Sload", "Fovr", "Frapidovr",
        "execution", "mode", "program"
    ]:
        if col in df.columns:
            candidate_cols.append(col)

    candidate_rows = df.loc[candidate_mask, candidate_cols].copy()
    candidate_rows["event_score"] = events.loc[candidate_mask, "event_score"].values

    # Add which rules fired
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
        max_gap_sec=MAX_EVENT_GAP_SEC
    )

    result["candidate_rows_df"] = candidate_rows
    result["grouped_events_df"] = grouped_events
    result["summary"] = {
        "n_candidate_rows": len(candidate_rows),
        "n_grouped_events": 0 if grouped_events is None or grouped_events.empty else len(grouped_events)
    }

    return result

# ============================================================
# PER-FILE REPORT
# ============================================================

def analyze_file(path):
    print_header(f"FILE: {os.path.basename(path)}")

    try:
        df = load_jsonl(path)
    except Exception as e:
        print(f"[ERROR] Could not load file: {e}")
        return

    if df.empty:
        print("[WARNING] File is empty or unreadable.")
        return

    # --------------------------------------------------------
    # 1) DATASET AUDIT
    # --------------------------------------------------------
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

    # --------------------------------------------------------
    # 2) ACTIVE SEGMENT DISCOVERY
    # --------------------------------------------------------
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

    # --------------------------------------------------------
    # 3) CANDIDATE EVENT DISCOVERY
    # --------------------------------------------------------
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

    # --------------------------------------------------------
    # COMBINED SUMMARY
    # --------------------------------------------------------
    print_subheader("4) COMBINED SHORT SUMMARY")

    usable_cols = []
    if audit["availability_df"] is not None and audit["candidate_numeric_df"] is not None:
        avail = audit["availability_df"].set_index("column")
        numlike = audit["candidate_numeric_df"].set_index("column")

        for col in CORE_NUMERIC_SIGNALS:
            if col in avail.index and col in numlike.index:
                if avail.loc[col, "available_fraction"] >= 0.8 and numlike.loc[col, "numeric_fraction"] >= 0.8:
                    usable_cols.append(col)

    print(f"Likely usable numeric/core signals: {usable_cols}")
    print(f"Likely active segments found: {seg['summary'].get('n_active_segments', 0)}")
    print(f"Candidate grouped events found: {ev['summary'].get('n_grouped_events', 0)}")


# ============================================================
# MAIN
# ============================================================

def main():
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