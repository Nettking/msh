"""
Analyze correlations in machine stop patterns across hourly time buckets.

This script reads JSONL telemetry files, identifies rows that likely represent
machine stops, groups nearby stopped rows into stop intervals, and then
aggregates total stop duration per machine per hour. A correlation matrix is
computed across machines based on those hourly stop-duration profiles.

Pipeline:
1. Load JSONL telemetry files from ``data/``
2. Parse timestamps and sort records chronologically
3. Identify likely stop rows using execution state and numeric signal values
4. Group nearby stop rows into stop intervals
5. Aggregate total stop duration per machine per hour
6. Compute machine-to-machine correlations

Outputs:
- ``correlation_matrix.csv``: machine correlation matrix
- console printout of the correlation matrix

Notes:
- This is an exploratory analysis, not a validated diagnostic model.
- Correlation here reflects similarity in hourly stop-duration patterns only.
- Results depend strongly on the stop heuristic and the temporal grouping rule.
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

from catalog.common.stops import find_stop_rows, group_stop_rows
from catalog.common.telemetry_prep import prepare_timestamp_column

# Directory containing input JSONL telemetry files.
DATA_DIR = Path("data")

# Execution states treated as explicitly stopped.
STOPPED_STATES = ["STOPPED"]

# Maximum allowed gap (in seconds) between stopped rows before they are split
# into separate stop intervals.
MAX_GAP_SECONDS = 2
OUTPUT_CORRELATION_CSV = Path("correlation_matrix.csv")


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

    return prepare_timestamp_column(df, time_col="timestamp", drop_invalid=True, sort=True)


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
    stop_rows, available_cols = find_stop_rows(
        df,
        stopped_states=STOPPED_STATES,
        numeric_cols=numeric_cols,
        execution_col="execution",
    )
    if not available_cols or "execution" not in df.columns:
        return pd.DataFrame(columns=["timestamp", "machine"])

    subset_cols = ["timestamp"]
    if "machine" in df.columns:
        subset_cols.append("machine")

    return stop_rows.loc[:, [col for col in subset_cols if col in stop_rows.columns]].copy()


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
    # Uses shared grouping helper; interval semantics remain unchanged.
    return group_stop_rows(df, max_gap_seconds=max_gap_seconds)


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
    - write the correlation matrix to CSV
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

    corr.to_csv(OUTPUT_CORRELATION_CSV, index=True)
    print(f"\nSaved correlation matrix CSV to: {OUTPUT_CORRELATION_CSV.resolve()}")

    print("\nCorrelation matrix:")
    print(corr.round(2))


if __name__ == "__main__":
    main()
