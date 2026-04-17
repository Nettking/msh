"""
Analyze correlations in machine stop patterns across hourly time buckets.

This script reads JSONL telemetry files, identifies rows that likely represent
machine stops, groups nearby stopped rows into stop intervals, and then
aggregates total stop duration per machine per hour. A correlation matrix is
computed across machines based on those hourly stop-duration profiles and
visualized as a heatmap.

Pipeline:
1. Load JSONL telemetry files from ``data/``
2. Parse timestamps and sort records chronologically
3. Identify likely stop rows using execution state and numeric signal values
4. Group nearby stop rows into stop intervals
5. Aggregate total stop duration per machine per hour
6. Compute and visualize machine-to-machine correlations

Outputs:
- ``correlation_heatmap.png``: heatmap of the machine correlation matrix
- console printout of the correlation matrix

Notes:
- This is an exploratory analysis, not a validated diagnostic model.
- Correlation here reflects similarity in hourly stop-duration patterns only.
- Results depend strongly on the stop heuristic and the temporal grouping rule.
"""

import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Directory containing input JSONL telemetry files.
DATA_DIR = Path("data")

# Execution states treated as explicitly stopped.
STOPPED_STATES = ["STOPPED"]

# Maximum allowed gap (in seconds) between stopped rows before they are split
# into separate stop intervals.
MAX_GAP_SECONDS = 2


def load_jsonl(file_path):
    """
    Load one JSONL telemetry file into a DataFrame.

    Blank or malformed JSON lines are skipped. If no valid records remain, or if
    the file lacks a timestamp column, an empty DataFrame is returned.

    Parameters
    ----------
    file_path : pathlib.Path
        Path to the JSONL file.

    Returns
    -------
    pandas.DataFrame
        Parsed telemetry rows with timestamps converted to datetime and sorted
        chronologically. Returns an empty DataFrame if the file is unusable.
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

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    return df


def find_stops(df):
    """
    Identify rows that likely correspond to stopped machine states.

    A row is treated as a stop candidate when:
    - ``execution`` is in ``STOPPED_STATES``, and
    - at least half of the available numeric motion/activity signals are zero

    The heuristic uses the following numeric signals when present:
    ``Srpm``, ``Fact``, ``Xfrt``, ``Yfrt``, ``Zfrt``

    Parameters
    ----------
    df : pandas.DataFrame
        Input telemetry data.

    Returns
    -------
    pandas.DataFrame
        Subset containing timestamp and machine columns for rows classified as
        stopped. Returns an empty DataFrame if the required inputs are missing.

    Notes
    -----
    This is a heuristic stop detector. It does not guarantee that all returned
    rows correspond to true operational stops.
    """
    numeric_cols = ["Srpm", "Fact", "Xfrt", "Yfrt", "Zfrt"]
    available_cols = [c for c in numeric_cols if c in df.columns]

    for col in available_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if not available_cols or "execution" not in df.columns:
        return pd.DataFrame(columns=["timestamp", "machine"])

    stopped_mask = (
        (df["execution"].isin(STOPPED_STATES))
        & ((df[available_cols] == 0).sum(axis=1) >= max(1, len(available_cols) // 2))
    )

    subset_cols = ["timestamp"]
    if "machine" in df.columns:
        subset_cols.append("machine")

    return df.loc[stopped_mask, subset_cols].copy()


def group_stops(df, max_gap_seconds=MAX_GAP_SECONDS):
    """
    Merge nearby stopped rows into stop intervals.

    Consecutive stopped rows for the same machine are merged into one interval
    if the time gap between them does not exceed ``max_gap_seconds``.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame of stop rows containing at least a timestamp column and
        optionally a machine column.
    max_gap_seconds : float, default=MAX_GAP_SECONDS
        Maximum gap allowed between consecutive rows in the same grouped stop
        interval.

    Returns
    -------
    pandas.DataFrame
        One row per grouped stop interval with:
        - machine
        - start
        - end
        - duration_s

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


def main():
    """
    Run the full stop-correlation analysis.

    For each JSONL file:
    - load telemetry
    - detect stop rows
    - group stop rows into intervals

    Then:
    - combine all intervals
    - bucket them by hour
    - sum stop duration per machine per hour
    - compute machine correlation matrix
    - save and display a heatmap
    """
    all_files = sorted(DATA_DIR.glob("*.jsonl"))
    all_intervals = []

    for file_path in all_files:
        df = load_jsonl(file_path)
        if df.empty:
            continue

        stops = find_stops(df)
        grouped = group_stops(stops)

        if not grouped.empty:
            all_intervals.append(grouped)

    if not all_intervals:
        print("No data found.")
        return

    df = pd.concat(all_intervals, ignore_index=True)

    # Floor each stop interval start time to the containing hour so that stop
    # duration can be aggregated into hourly buckets.
    df["hour"] = df["start"].dt.floor("h")

    # Aggregate total stop time per machine per hour.
    pivot = (
        df.groupby(["hour", "machine"])["duration_s"]
        .sum()
        .unstack(fill_value=0)
    )

    # Compute correlation across machine stop-duration profiles.
    corr = pivot.corr()

    plt.figure(figsize=(6, 5))
    plt.imshow(corr, cmap="coolwarm", vmin=-1, vmax=1)
    plt.colorbar(label="Correlation coefficient")
    plt.xticks(range(len(corr.columns)), corr.columns, rotation=45)
    plt.yticks(range(len(corr.columns)), corr.index)
    plt.title("Correlation of stop patterns between machines")
    plt.tight_layout()
    plt.savefig("correlation_heatmap.png", dpi=300)
    plt.show()

    print("\nCorrelation matrix:")
    print(corr.round(2))


if __name__ == "__main__":
    main()