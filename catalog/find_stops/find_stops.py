"""
Generate hourly stop-timeline plots from JSONL telemetry data.

This script reads telemetry files from ``data/``, identifies rows that likely
represent stopped machine states, groups nearby stop rows into stop intervals,
and saves one hourly plot per machine.

Pipeline
--------
1. Load JSONL telemetry files
2. Parse timestamps and sort rows chronologically
3. Identify likely stop rows using execution state and numeric signal values
4. Merge nearby stop rows into stop intervals
5. Group intervals by day, machine, and start hour
6. Save one plot per machine/hour

Outputs
-------
Plots are written under:

    plots/<YYYY-MM-DD>/<machine>/<HH>.png

Each plot shows stop intervals for one machine during one hour window.

Important
---------
This is an exploratory visualization utility. “Stop” is inferred heuristically
from execution state and selected numeric signals, not observed directly.
"""

import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from catalog.common.telemetry_prep import prepare_timestamp_column, to_numeric

# Directory containing input JSONL telemetry files.
DATA_DIR = Path("data")

# Base directory for generated plots.
OUTPUT_DIR = Path("plots")

# Execution states treated as explicitly stopped.
STOPPED_STATES = ["STOPPED"]

# Maximum allowed gap (seconds) between consecutive stopped rows before they are
# treated as separate stop intervals.
MAX_GAP_SECONDS = 2


def load_jsonl(file_path):
    """
    Load one JSONL file into a DataFrame.

    Malformed JSON lines are skipped. Files without any valid rows, or without a
    timestamp column, are treated as unusable and return an empty DataFrame.

    Parameters
    ----------
    file_path : pathlib.Path
        Path to the JSONL file.

    Returns
    -------
    pandas.DataFrame
        Parsed telemetry data with timestamps converted to datetime and sorted
        chronologically, or an empty DataFrame if the file is unusable.
    """
    records = []
    with open(file_path, "r") as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    if "timestamp" not in df.columns:
        return pd.DataFrame()

    df = prepare_timestamp_column(df, time_col="timestamp", drop_invalid=True, sort=True)
    return df


def find_stops(df):
    """
    Identify rows that likely correspond to stopped machine states.

    A row is marked as stopped when:
    - ``execution`` is in ``STOPPED_STATES``, and
    - at least half of the available activity-related numeric columns are zero

    Numeric columns considered when present:
    - ``Srpm``
    - ``Fact``
    - ``Xfrt``
    - ``Yfrt``
    - ``Zfrt``

    Parameters
    ----------
    df : pandas.DataFrame
        Input telemetry data.

    Returns
    -------
    pandas.DataFrame
        Subset of rows classified as stopped, containing timestamp and selected
        context columns.

    Notes
    -----
    This is a heuristic detector. It approximates stopped behavior rather than
    providing a guaranteed operational truth.
    """
    numeric_cols = ["Srpm", "Fact", "Xfrt", "Yfrt", "Zfrt"]
    available_cols = [c for c in numeric_cols if c in df.columns]

    for col in available_cols:
        df[col] = to_numeric(df[col])

    if not available_cols or "execution" not in df.columns:
        return pd.DataFrame(columns=["timestamp", "execution", "mode", "machine"])

    stopped_mask = (
        (df["execution"].isin(STOPPED_STATES))
        & ((df[available_cols] == 0).sum(axis=1) >= max(1, len(available_cols) // 2))
    )

    subset_cols = ["timestamp", "execution", "mode"]
    if "machine" in df.columns:
        subset_cols.append("machine")

    return df.loc[stopped_mask, subset_cols].copy()


def group_stops(df, max_gap_seconds=MAX_GAP_SECONDS):
    """
    Merge nearby stopped rows into stop intervals.

    Consecutive stopped rows for the same machine are combined into a single
    interval when the gap between them does not exceed ``max_gap_seconds``.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame of stop rows.
    max_gap_seconds : float, default=MAX_GAP_SECONDS
        Maximum gap allowed between rows in the same stop interval.

    Returns
    -------
    pandas.DataFrame
        Interval table with columns:
        - ``machine``
        - ``start``
        - ``end``
        - ``duration_s``

    Notes
    -----
    If no machine column is present, rows are grouped under ``"UNKNOWN"``.
    """
    if df.empty:
        return pd.DataFrame(columns=["machine", "start", "end", "duration_s"])

    df = df.sort_values("timestamp")
    df["machine"] = df.get("machine", "UNKNOWN")

    grouped = []
    for machine, mdf in df.groupby("machine"):
        start, last = None, None

        for t in mdf["timestamp"]:
            if start is None:
                start, last = t, t
                continue

            gap = (t - last).total_seconds()
            if gap <= max_gap_seconds:
                last = t
            else:
                grouped.append([machine, start, last, (last - start).total_seconds()])
                start, last = t, t

        if start is not None:
            grouped.append([machine, start, last, (last - start).total_seconds()])

    return pd.DataFrame(grouped, columns=["machine", "start", "end", "duration_s"])


def plot_hour(machine, hour_df, hour_label, out_path):
    """
    Plot stop intervals for one machine within one hour bucket.

    Parameters
    ----------
    machine : str
        Machine identifier used in the plot title.
    hour_df : pandas.DataFrame
        Stop intervals assigned to the hour bucket.
    hour_label : str
        Human-readable label for the plotted hour window.
    out_path : pathlib.Path
        Output PNG path.

    Notes
    -----
    Intervals are drawn as horizontal red line segments. This is a compact
    timeline visualization, not a duration histogram.
    """
    if hour_df.empty:
        return

    plt.figure(figsize=(10, 2))
    for _, row in hour_df.iterrows():
        plt.hlines(1, row["start"], row["end"], colors="red", linewidth=6)

    plt.title(f"{machine} – Stops at {hour_label}")
    plt.yticks([])
    plt.xlabel("Time")
    plt.tight_layout()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path)
    plt.close()


def main():
    """
    Run the full hourly stop-plot pipeline across all JSONL files in ``data/``.
    """
    all_files = sorted(DATA_DIR.glob("*.jsonl"))
    if not all_files:
        print("No JSONL files found.")
        return

    print(f"Analyzing {len(all_files)} files in {DATA_DIR}...\n")

    for file_path in all_files:
        df = load_jsonl(file_path)
        if df.empty:
            continue

        stops = find_stops(df)
        grouped = group_stops(stops)
        if grouped.empty:
            continue

        grouped["day"] = grouped["start"].dt.date
        grouped["hour"] = grouped["start"].dt.hour

        # Each interval is assigned to the hour containing its start time.
        # Intervals are not split if they cross an hour boundary.
        for (day, machine, hour), hdf in grouped.groupby(["day", "machine", "hour"]):
            day_dir = OUTPUT_DIR / str(day) / machine
            out_path = day_dir / f"{hour:02d}.png"
            label = f"{day} {hour:02d}:00–{hour + 1:02d}:00"
            plot_hour(machine, hdf, label, out_path)

        print(f"{file_path.name}: {len(grouped)} stop intervals plotted hourly.")

    print(f"\nAll hourly plots saved in: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
