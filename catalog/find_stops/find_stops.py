"""Generate hourly stop-interval summaries from JSONL telemetry data."""

from pathlib import Path

import pandas as pd

from catalog.common.data_loading import iter_jsonl_files, load_jsonl_dataframe
from catalog.common.stops import find_stop_rows, group_stop_rows
from catalog.common.telemetry_prep import prepare_timestamp_column

DATA_DIR = Path("data")
OUTPUT_DIR = Path("results")
OUTPUT_CSV = OUTPUT_DIR / "find_stops" / "hourly_stop_intervals.csv"
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


def main():
    all_files = list(iter_jsonl_files(DATA_DIR, recursive=False))
    if not all_files:
        print("No JSONL files found.")
        return

    print(f"Analyzing {len(all_files)} files in {DATA_DIR}...\n")

    interval_frames: list[pd.DataFrame] = []
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

        grouped["source_file"] = file_path.name
        interval_frames.append(grouped)
        print(f"{file_path.name}: {len(grouped)} stop intervals summarized.")

    if not interval_frames:
        print("No stop intervals found.")
        return

    summary = pd.concat(interval_frames, ignore_index=True).sort_values(
        ["day", "machine", "hour", "start", "end"]
    )
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUTPUT_CSV, index=False)
    print(f"\nHourly stop interval summary saved in: {OUTPUT_CSV.resolve()}")


if __name__ == "__main__":
    main()
