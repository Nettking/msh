"""
Generate hourly stop-timeline plots from JSONL telemetry data.

This script keeps plotting/output responsibilities local while reusing shared
loading and telemetry-preparation helpers so stop heuristics can evolve as
reusable DT foundation logic.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from catalog.common.data_loading import iter_jsonl_files, load_jsonl_dataframe
from catalog.common.stops import find_stop_rows, group_stop_rows
from catalog.common.telemetry_prep import prepare_timestamp_column

DATA_DIR = Path("data")
OUTPUT_DIR = Path("plots")
STOPPED_STATES = ["STOPPED"]
MAX_GAP_SECONDS = 2


def load_telemetry(file_path: Path) -> pd.DataFrame:
    """Load and timestamp-normalize one telemetry JSONL file."""
    df = load_jsonl_dataframe(
        file_path,
        on_malformed_json=lambda msg: print(f"[WARNING] {msg}"),
    )
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    return prepare_timestamp_column(df, time_col="timestamp", drop_invalid=True, sort=True)


def find_stops(df: pd.DataFrame) -> pd.DataFrame:
    """Identify rows likely representing machine stops."""
    numeric_cols = ["Srpm", "Fact", "Xfrt", "Yfrt", "Zfrt"]
    stop_rows, available_cols = find_stop_rows(
        df,
        stopped_states=STOPPED_STATES,
        numeric_cols=numeric_cols,
        execution_col="execution",
    )

    if not available_cols or "execution" not in df.columns:
        return pd.DataFrame(columns=["timestamp", "execution", "mode", "machine"])

    subset_cols = ["timestamp", "execution", "mode"]
    if "machine" in stop_rows.columns:
        subset_cols.append("machine")

    return stop_rows.loc[:, [col for col in subset_cols if col in stop_rows.columns]].copy()


def group_stops(df: pd.DataFrame, max_gap_seconds: float = MAX_GAP_SECONDS) -> pd.DataFrame:
    """Merge nearby stop rows into machine-specific intervals."""
    return group_stop_rows(df, max_gap_seconds=max_gap_seconds)


def plot_hour(machine, hour_df, hour_label, out_path):
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
    all_files = list(iter_jsonl_files(DATA_DIR, recursive=False))
    if not all_files:
        print("No JSONL files found.")
        return

    print(f"Analyzing {len(all_files)} files in {DATA_DIR}...\n")

    for file_path in all_files:
        df = load_telemetry(file_path)
        if df.empty:
            continue

        stops = find_stops(df)
        grouped = group_stops(stops)
        if grouped.empty:
            continue

        grouped["day"] = grouped["start"].dt.date
        grouped["hour"] = grouped["start"].dt.hour

        for (day, machine, hour), hdf in grouped.groupby(["day", "machine", "hour"]):
            day_dir = OUTPUT_DIR / str(day) / machine
            out_path = day_dir / f"{hour:02d}.png"
            label = f"{day} {hour:02d}:00–{hour + 1:02d}:00"
            plot_hour(machine, hdf, label, out_path)

        print(f"{file_path.name}: {len(grouped)} stop intervals plotted hourly.")

    print(f"\nAll hourly plots saved in: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
