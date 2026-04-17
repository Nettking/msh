"""
Infer machine states from telemetry data and generate per-day timeline plots.

This script loads JSONL telemetry files, prepares the data, infers a simple
state model per machine and day, extracts candidate intervention rows, and
renders daily timeline visualizations.

Pipeline
--------
1. Load JSONL files from ``data/``
2. Parse timestamps and normalize selected numeric/context columns
3. Infer per-row machine states using heuristic rules
4. Convert state rows into merged time intervals
5. Extract intervention-candidate rows for inspection
6. Write candidate rows to CSV
7. Generate one timeline image per day across all machines

State model
-----------
Rows are assigned one of four states:

- ``idle``:
    no evidence of active operation and not densely sampled
- ``dense_idle``:
    densely sampled but not classified as active
- ``active``:
    dense sampling together with RPM/load evidence of activity
- ``intervention_candidate``:
    rows near activity where one or more heuristic event rules fire

Important
---------
This is an exploratory heuristic pipeline, not a validated ground-truth event
detector. Its outputs are useful for inspection and discussion, but should be
interpreted cautiously.

Outputs
-------
- ``candidate_events.csv``:
    candidate rows with timestamps, scores, and fired rule descriptions
- ``timeline_images/timeline_<date>.png``:
    one timeline plot per day showing machine states over time
"""

import glob
import json
import os

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch

# Folder containing input JSONL telemetry files.
FOLDER = r"./data"

# File pattern used to find telemetry input files.
FILE_PATTERN = "*.jsonl"

# Directory for generated timeline images.
OUTPUT_DIR = r"./timeline_images"

# Output CSV containing candidate intervention rows.
CANDIDATE_CSV = r"./candidate_events.csv"

# Candidate column names for identifying the machine/source of a row.
MACHINE_ID_CANDIDATES = [
    "machine",
    "machine_id",
    "machineId",
    "Machine",
    "MachineId",
    "machine_name",
    "resource",
    "device",
]

# Canonical signal names used in state inference and diagnostics.
RPM_COL = "Srpm"
LOAD_COL = "Sload"
OVR_COL = "Sovr"
EXEC_COL = "execution"
MODE_COL = "mode"
PROG_COL = "program"
TIME_COL = "timestamp"
FOVR_COL = "Fovr"
FRAPIDOVR_COL = "Frapidovr"

# Heuristics for dense sampling and activity detection.
DENSE_DT_SEC = 5.0
RPM_ACTIVE_THRESHOLD = 100.0
LOAD_ACTIVE_THRESHOLD = 1.0

# Quantiles used to derive rate-change thresholds. These are intentionally less
# strict than extreme-tail settings in order to favor exploratory recall.
LOAD_RATE_Q = 0.95
RPM_RATE_Q = 0.95
OVR_RATE_Q = 0.95
FOVR_RATE_Q = 0.95
FRAPIDOVR_RATE_Q = 0.95

# Explicit intervention-like pattern thresholds.
OVR_DROP_THRESHOLD = -10.0
FOVR_DROP_THRESHOLD = -10.0
RPM_COLLAPSE_RATIO = 0.5
LOAD_COLLAPSE_RATIO = 0.5

# Merge neighboring intervals with the same state if the gap is small.
MERGE_GAP_SEC = 30.0

# Minimum state duration used by the optional smoothing function.
MIN_STATE_DURATION_SEC = 5.0

# Figure settings for timeline plots.
FIG_WIDTH = 18
ROW_HEIGHT = 0.8
SAVE_FIGURES = True
SHOW_FIGURES = False


def load_jsonl(path):
    """
    Load one JSONL file into a DataFrame.

    Blank lines are skipped. Malformed JSON lines are reported and ignored.

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


def replace_unavailable(series):
    """
    Replace string-based 'UNAVAILABLE' values with NaN.

    This supports mixed telemetry columns where missingness is represented as
    a literal string rather than a real null value.
    """
    if series.dtype == object or str(series.dtype).startswith("string"):
        return series.replace("UNAVAILABLE", np.nan)
    return series


def to_numeric(series):
    """
    Convert a telemetry series to numeric values where possible.

    Non-numeric values are coerced to NaN after first normalizing
    'UNAVAILABLE' markers.
    """
    return pd.to_numeric(replace_unavailable(series), errors="coerce")


def find_machine_col(df):
    """
    Find the first plausible machine identifier column in a DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame

    Returns
    -------
    str | None
        Name of the detected machine-ID column, or None if none is found.
    """
    for col in MACHINE_ID_CANDIDATES:
        if col in df.columns:
            return col
    return None


def prepare_dataframe(df, source_name):
    """
    Prepare one raw telemetry DataFrame for downstream analysis.

    This step:
    - validates and parses timestamps
    - detects or synthesizes a machine identifier
    - normalizes selected numeric and context columns
    - adds source file and date metadata

    Parameters
    ----------
    df : pandas.DataFrame
        Raw telemetry rows.
    source_name : str
        Source filename used as fallback machine identifier if needed.

    Returns
    -------
    pandas.DataFrame | None
        Prepared DataFrame, or None if the input is unusable.
    """
    df = df.copy()

    if TIME_COL not in df.columns:
        return None

    df[TIME_COL] = pd.to_datetime(df[TIME_COL], errors="coerce")
    df = df[df[TIME_COL].notna()].copy()
    if df.empty:
        return None

    machine_col = find_machine_col(df)
    if machine_col is None:
        # Fall back to source filename when no machine identifier exists.
        df["machine_fallback"] = source_name
        machine_col = "machine_fallback"

    numeric_cols = [RPM_COL, LOAD_COL, OVR_COL, FOVR_COL, FRAPIDOVR_COL, "Stemp"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = to_numeric(df[col])

    for col in [EXEC_COL, MODE_COL, PROG_COL]:
        if col in df.columns:
            df[col] = replace_unavailable(df[col]).astype("string")

    df["machine_id"] = df[machine_col].astype(str)
    df["source_file"] = source_name
    df["date"] = df[TIME_COL].dt.date

    return df


def smooth_state_sequence(g, min_duration_sec=5.0):
    """
    Smooth very short state fragments by replacing them with neighboring states.

    Parameters
    ----------
    g : pandas.DataFrame
        Row-level state sequence for one machine/day.
    min_duration_sec : float, default=5.0
        Minimum duration below which a state fragment is treated as too short.

    Returns
    -------
    pandas.DataFrame
        Smoothed copy of the state sequence.

    Notes
    -----
    This is currently not applied in the main pipeline because the unsmoothed
    state sequence is being kept for debugging and inspection.
    """
    g = g.sort_values(TIME_COL).copy()
    if len(g) < 3:
        return g

    g["state_group"] = (g["state"] != g["state"].shift()).cumsum()

    group_info = (
        g.groupby("state_group")
        .agg(
            state=("state", "first"),
            start=(TIME_COL, "min"),
            end=(TIME_COL, "max"),
            n=("state", "size"),
        )
        .reset_index()
    )
    group_info["duration_sec"] = (group_info["end"] - group_info["start"]).dt.total_seconds()

    short_groups = group_info[group_info["duration_sec"] < min_duration_sec]["state_group"].tolist()

    for grp in short_groups:
        idx = g.index[g["state_group"] == grp].tolist()
        if not idx:
            continue

        first_pos = g.index.get_loc(idx[0])
        prev_state = None
        next_state = None

        if first_pos > 0:
            prev_state = g.iloc[first_pos - 1]["state"]

        last_pos = g.index.get_loc(idx[-1])
        if last_pos < len(g) - 1:
            next_state = g.iloc[last_pos + 1]["state"]

        replacement = None
        if prev_state is not None and next_state is not None:
            replacement = prev_state if prev_state == next_state else prev_state
        elif prev_state is not None:
            replacement = prev_state
        elif next_state is not None:
            replacement = next_state

        if replacement is not None:
            g.loc[idx, "state"] = replacement

    g = g.drop(columns=["state_group"], errors="ignore")
    return g


def build_fired_rules(events_df, idx):
    """
    Build a comma-separated description of which event rules fired at one row.

    Parameters
    ----------
    events_df : pandas.DataFrame
        Boolean event-rule table plus ``event_score``.
    idx : Any
        Row index in ``events_df``.

    Returns
    -------
    str
        Comma-separated rule names that evaluated to True for the row.
    """
    fired = [col for col in events_df.columns if col != "event_score" and bool(events_df.loc[idx, col])]
    return ", ".join(fired)


def infer_states_for_machine(g):
    """
    Infer per-row machine state and intervention-candidate flags for one machine/day.

    The logic combines:
    - dense sampling
    - RPM/load activity
    - context changes
    - rate-based numeric jumps
    - explicit override drops or collapses

    The resulting state labels are:
    - ``idle``
    - ``dense_idle``
    - ``active``
    - ``intervention_candidate``

    Parameters
    ----------
    g : pandas.DataFrame
        Rows for one machine on one day.

    Returns
    -------
    pandas.DataFrame
        Copy of the input with inferred state columns and diagnostic metadata
        attached in ``attrs``.

    Notes
    -----
    The event score is currently used for diagnostics only. Candidate selection
    is deliberately more permissive than a strict score threshold.
    """
    g = g.sort_values(TIME_COL).copy()
    g["dt_sec"] = g[TIME_COL].diff().dt.total_seconds()

    dense = g["dt_sec"].fillna(np.inf) < DENSE_DT_SEC

    rpm_active = (
        g[RPM_COL].fillna(0) > RPM_ACTIVE_THRESHOLD
        if RPM_COL in g.columns else pd.Series(False, index=g.index, dtype=bool)
    )
    load_active = (
        g[LOAD_COL].fillna(0) > LOAD_ACTIVE_THRESHOLD
        if LOAD_COL in g.columns else pd.Series(False, index=g.index, dtype=bool)
    )

    g["dense"] = dense.fillna(False)
    g["active"] = (dense & (rpm_active | load_active)).fillna(False)
    g["dense_idle"] = (dense & ~(rpm_active | load_active)).fillna(False)

    valid_dt = g["dt_sec"].between(0.1, 30.0)

    # Compute adaptive rate features for selected signals.
    rate_specs = [
        (RPM_COL, "rate_rpm", RPM_RATE_Q),
        (LOAD_COL, "rate_load", LOAD_RATE_Q),
        (OVR_COL, "rate_ovr", OVR_RATE_Q),
        (FOVR_COL, "rate_fovr", FOVR_RATE_Q),
        (FRAPIDOVR_COL, "rate_frapidovr", FRAPIDOVR_RATE_Q),
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

    if EXEC_COL in g.columns:
        events["execution_change"] = (g[EXEC_COL] != g[EXEC_COL].shift(1)).fillna(False)
    else:
        events["execution_change"] = False

    if MODE_COL in g.columns:
        events["mode_change"] = (g[MODE_COL] != g[MODE_COL].shift(1)).fillna(False)
    else:
        events["mode_change"] = False

    if PROG_COL in g.columns:
        events["program_change"] = (g[PROG_COL] != g[PROG_COL].shift(1)).fillna(False)
    else:
        events["program_change"] = False

    for _, rate_col, _ in rate_specs:
        thr = thresholds.get(rate_col, np.nan)
        if pd.notna(thr) and rate_col in g.columns:
            events[f"{rate_col}_jump"] = g[rate_col].abs() > thr
        else:
            events[f"{rate_col}_jump"] = False

    if OVR_COL in g.columns:
        g["d_ovr"] = g[OVR_COL].diff()
        events["ovr_drop"] = g["d_ovr"] <= OVR_DROP_THRESHOLD
    else:
        events["ovr_drop"] = False

    if FOVR_COL in g.columns:
        g["d_fovr"] = g[FOVR_COL].diff()
        events["fovr_drop"] = g["d_fovr"] <= FOVR_DROP_THRESHOLD
    else:
        events["fovr_drop"] = False

    if RPM_COL in g.columns:
        prev_rpm = g[RPM_COL].shift(1)
        events["rpm_collapse"] = (
            g["active"].shift(1).fillna(False)
            & prev_rpm.notna()
            & g[RPM_COL].notna()
            & (prev_rpm > 0)
            & (g[RPM_COL] < RPM_COLLAPSE_RATIO * prev_rpm)
        )
    else:
        events["rpm_collapse"] = False

    if LOAD_COL in g.columns:
        prev_load = g[LOAD_COL].shift(1)
        events["load_collapse"] = (
            g["active"].shift(1).fillna(False)
            & prev_load.notna()
            & g[LOAD_COL].notna()
            & (prev_load > 0)
            & (g[LOAD_COL] < LOAD_COLLAPSE_RATIO * prev_load)
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

    # Weighted score retained for diagnostics, not as the sole decision rule.
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
        if col in events.columns:
            base_candidate |= events[col]

    numeric_candidate = pd.Series(False, index=g.index, dtype=bool)
    for col in [
        "rate_rpm_jump",
        "rate_load_jump",
        "rate_ovr_jump",
        "rate_fovr_jump",
        "rate_frapidovr_jump",
    ]:
        if col in events.columns:
            numeric_candidate |= events[col]

    # Candidate rows are defined more permissively than the event score alone.
    g["intervention_candidate"] = near_active & (base_candidate | numeric_candidate)

    g["state"] = np.where(
        g["intervention_candidate"], "intervention_candidate",
        np.where(
            g["active"], "active",
            np.where(g["dense_idle"], "dense_idle", "idle"),
        ),
    )

    if len(g) > 0:
        first_idx = g.index[0]
        g.loc[first_idx, "intervention_candidate"] = False
        if g.loc[first_idx, "state"] == "intervention_candidate":
            g.loc[first_idx, "state"] = "active" if bool(g.loc[first_idx, "active"]) else "idle"

    g["event_score"] = events["event_score"]
    g["fired_rules"] = [build_fired_rules(events, idx) for idx in g.index]

    trigger_counts = {col: int(events[col].sum()) for col in events.columns if col != "event_score"}
    g.attrs["trigger_counts"] = trigger_counts
    g.attrs["thresholds"] = thresholds

    # Intentionally disabled during debugging to preserve raw state transitions.
    # g = smooth_state_sequence(g, min_duration_sec=MIN_STATE_DURATION_SEC)

    return g


def rows_to_intervals(g):
    """
    Merge consecutive rows with the same inferred state into time intervals.

    Neighboring rows are merged only if they share the same state and are close
    enough in time according to ``MERGE_GAP_SEC``.

    Parameters
    ----------
    g : pandas.DataFrame
        Row-level state sequence for one machine/day.

    Returns
    -------
    pandas.DataFrame
        Interval table with machine, date, state, start/end time, duration, and
        row count.
    """
    g = g.sort_values(TIME_COL).copy()
    if len(g) == 0:
        return pd.DataFrame(columns=["machine_id", "state", "start", "end", "duration_sec", "n_points", "date"])

    rows = []
    current_state = g.iloc[0]["state"]
    start_time = g.iloc[0][TIME_COL]
    prev_time = g.iloc[0][TIME_COL]
    n_points = 1
    machine_id = g.iloc[0]["machine_id"]
    day_value = g.iloc[0]["date"]

    for i in range(1, len(g)):
        t = g.iloc[i][TIME_COL]
        state = g.iloc[i]["state"]
        gap = (t - prev_time).total_seconds()

        same_block = (state == current_state) and (gap <= MERGE_GAP_SEC)

        if same_block:
            prev_time = t
            n_points += 1
        else:
            rows.append(
                {
                    "machine_id": machine_id,
                    "date": day_value,
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
            "machine_id": machine_id,
            "date": day_value,
            "state": current_state,
            "start": start_time,
            "end": prev_time,
            "duration_sec": (prev_time - start_time).total_seconds(),
            "n_points": n_points,
        }
    )

    return pd.DataFrame(rows)


def extract_candidate_rows(g):
    """
    Extract rows marked as intervention candidates for CSV export and inspection.

    Parameters
    ----------
    g : pandas.DataFrame
        Row-level state sequence with candidate flags.

    Returns
    -------
    pandas.DataFrame
        Candidate-row table including timestamps, scores, fired rules, and
        selected telemetry/context columns when available.
    """
    cols = ["date", "machine_id", TIME_COL, "state", "event_score", "fired_rules"]
    for col in [RPM_COL, LOAD_COL, OVR_COL, FOVR_COL, FRAPIDOVR_COL, EXEC_COL, MODE_COL, PROG_COL]:
        if col in g.columns:
            cols.append(col)

    out = g.loc[g["intervention_candidate"], cols].copy()
    out = out.rename(columns={TIME_COL: "timestamp"})
    return out


def plot_day_timeline(interval_df_day, output_path=None, show=False):
    """
    Plot one day's inferred machine timelines.

    Each machine is shown on a separate horizontal row. Intervals are colored
    by inferred state.

    Parameters
    ----------
    interval_df_day : pandas.DataFrame
        Interval table for one day across one or more machines.
    output_path : str | None, optional
        If provided, save the figure to this path.
    show : bool, default=False
        If True, display the figure interactively. Otherwise close it after
        saving.

    Notes
    -----
    Zero-length intervals are widened to one second for visibility in the plot.
    """
    if interval_df_day.empty:
        return

    state_colors = {
        "idle": "lightgray",
        "dense_idle": "orange",
        "active": "tab:blue",
        "intervention_candidate": "tab:red",
    }

    machines = sorted(interval_df_day["machine_id"].dropna().unique().tolist())
    fig_height = max(4, ROW_HEIGHT * len(machines) + 1.5)

    fig, ax = plt.subplots(figsize=(FIG_WIDTH, fig_height))
    y_positions = {m: i for i, m in enumerate(machines)}

    for _, row in interval_df_day.iterrows():
        y = y_positions[row["machine_id"]]
        start = row["start"]
        end = row["end"]
        color = state_colors.get(row["state"], "black")

        if start == end:
            end = start + pd.Timedelta(seconds=1)

        ax.barh(
            y=y,
            width=(end - start).total_seconds() / 86400.0,
            left=mdates.date2num(start),
            height=0.6,
            color=color,
            edgecolor="none",
        )

    ax.set_yticks(list(y_positions.values()))
    ax.set_yticklabels(list(y_positions.keys()))
    ax.set_xlabel("Time")
    ax.set_ylabel("Machine")

    day_label = str(interval_df_day["date"].iloc[0])
    ax.set_title(f"Machine timelines for {day_label}")

    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    fig.autofmt_xdate()

    legend_handles = [
        Patch(color=state_colors["idle"], label="Idle"),
        Patch(color=state_colors["dense_idle"], label="Dense idle"),
        Patch(color=state_colors["active"], label="Active"),
        Patch(color=state_colors["intervention_candidate"], label="Intervention candidate"),
    ]
    ax.legend(handles=legend_handles, loc="upper right")

    plt.tight_layout()

    if output_path is not None:
        plt.savefig(output_path, dpi=200, bbox_inches="tight")
        print(f"Saved: {output_path}")

    if show:
        plt.show()
    else:
        plt.close(fig)


# Ensure the timeline output directory exists.
os.makedirs(OUTPUT_DIR, exist_ok=True)

all_frames = []

files = sorted(glob.glob(os.path.join(FOLDER, FILE_PATTERN)))
if not files:
    raise FileNotFoundError(f"No files found in {FOLDER!r} matching {FILE_PATTERN!r}")

for path in files:
    name = os.path.basename(path)
    df = load_jsonl(path)
    prepared = prepare_dataframe(df, name)
    if prepared is not None and not prepared.empty:
        all_frames.append(prepared)

if not all_frames:
    raise ValueError("No usable data found.")

data = pd.concat(all_frames, ignore_index=True)
data = data.sort_values(["date", "machine_id", TIME_COL]).reset_index(drop=True)

state_frames = []
interval_frames = []
candidate_frames = []

print("\n=== TRIGGER DIAGNOSTICS BY DAY AND MACHINE ===")
for (day_value, machine_id), g in data.groupby(["date", "machine_id"], sort=True):
    gs = infer_states_for_machine(g)
    state_frames.append(gs)
    interval_frames.append(rows_to_intervals(gs))
    candidate_frames.append(extract_candidate_rows(gs))

    print(f"\n[{day_value}] machine={machine_id}")
    print("Trigger counts:")
    for k, v in gs.attrs.get("trigger_counts", {}).items():
        if v > 0:
            print(f"  {k}: {v}")

    print(f"  active rows: {int(gs['active'].sum())}")
    print(f"  intervention_candidate rows: {int(gs['intervention_candidate'].sum())}")

    thresholds = gs.attrs.get("thresholds", {})
    non_nan_thresholds = {k: v for k, v in thresholds.items() if pd.notna(v)}
    if non_nan_thresholds:
        print("Thresholds:")
        for k, v in non_nan_thresholds.items():
            print(f"  {k}: {v:.6f}")

state_df = pd.concat(state_frames, ignore_index=True)
interval_df = pd.concat(interval_frames, ignore_index=True)

candidate_df = pd.concat(candidate_frames, ignore_index=True) if candidate_frames else pd.DataFrame()
if not candidate_df.empty:
    candidate_df = candidate_df.sort_values(["date", "machine_id", "timestamp"]).reset_index(drop=True)
    candidate_df.to_csv(CANDIDATE_CSV, index=False)
    print(f"\nSaved candidate events to: {os.path.abspath(CANDIDATE_CSV)}")
else:
    print("\nNo candidate events found.")
    pd.DataFrame(columns=["date", "machine_id", "timestamp", "state", "event_score", "fired_rules"]).to_csv(
        CANDIDATE_CSV, index=False
    )
    print(f"Saved empty candidate file to: {os.path.abspath(CANDIDATE_CSV)}")

# Drop trivial one-point zero-length idle intervals to reduce plot clutter.
interval_df = interval_df[
    ~(
        (interval_df["state"] == "idle")
        & (interval_df["duration_sec"] == 0)
        & (interval_df["n_points"] == 1)
    )
].copy()

print("\n=== MACHINE SUMMARY BY DAY ===")
summary = (
    interval_df.groupby(["date", "machine_id", "state"])
    .agg(
        n_intervals=("state", "size"),
        total_duration_sec=("duration_sec", "sum"),
    )
    .reset_index()
    .sort_values(["date", "machine_id", "state"])
)
print(summary.to_string(index=False))

if not candidate_df.empty:
    print("\n=== FIRST CANDIDATE ROWS ===")
    print(candidate_df.head(50).to_string(index=False))

unique_days = sorted(interval_df["date"].dropna().unique().tolist())

for day_value in unique_days:
    day_intervals = interval_df[interval_df["date"] == day_value].copy()
    if day_intervals.empty:
        continue

    filename = f"timeline_{day_value}.png"
    output_path = os.path.join(OUTPUT_DIR, filename)

    plot_day_timeline(
        day_intervals,
        output_path=output_path if SAVE_FIGURES else None,
        show=SHOW_FIGURES,
    )

print(f"\nDone. Images are in: {os.path.abspath(OUTPUT_DIR)}")