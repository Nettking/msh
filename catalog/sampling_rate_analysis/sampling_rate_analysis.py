"""Summarize average sampling rate per day from JSONL telemetry data."""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from catalog.common.data_loading import iter_records_with_parsed_timestamps

DATA_DIR = "data"
OUTPUT_CSV = "sampling_rate_summary.csv"
OUTPUT_PLOT = "daily_sampling_rate.png"
FREQUENCY_THRESHOLD = 4.9


def _warn_malformed_json(message: str) -> None:
    print(f"Error parsing line: {message}")


def _warn_invalid_timestamp(file_path: Path, raw_timestamp: object) -> None:
    print(f"Error parsing line in {file_path.name}: Invalid isoformat string: {raw_timestamp}")


def main() -> None:
    timestamps: list[pd.Timestamp] = []
    for _, entry in iter_records_with_parsed_timestamps(
        DATA_DIR,
        recursive=False,
        on_malformed_json=_warn_malformed_json,
        on_invalid_timestamp=_warn_invalid_timestamp,
    ):
        timestamps.append(entry["timestamp"])

    if not timestamps:
        raise SystemExit("No valid records found.")

    df = pd.DataFrame({"timestamp": timestamps}).sort_values("timestamp").reset_index(drop=True)
    df["time_gap_s"] = df["timestamp"].diff().dt.total_seconds()
    df["sampling_rate_hz"] = df["time_gap_s"].apply(lambda x: round(1 / x, 2) if x and x > 0 else None)
    df["date"] = df["timestamp"].dt.date

    daily_freq = df.groupby("date", as_index=False)["sampling_rate_hz"].mean()
    daily_freq.rename(columns={"sampling_rate_hz": "avg_sampling_rate_hz"}, inplace=True)
    below_threshold = daily_freq[daily_freq["avg_sampling_rate_hz"] < FREQUENCY_THRESHOLD]

    print("\nDaily average sampling rate:")
    print(daily_freq)
    if not below_threshold.empty:
        print("\nDays with low sampling rate:")
        print(below_threshold)

    daily_freq.to_csv(OUTPUT_CSV, index=False)
    print(f"\nSaved summary to: {OUTPUT_CSV}")

    plt.figure(figsize=(10, 5))
    plt.plot(daily_freq["date"], daily_freq["avg_sampling_rate_hz"], marker="o", label="Avg Frequency")
    plt.axhline(y=5.0, color="green", linestyle="--", label="Target (5 Hz)")
    plt.axhline(y=FREQUENCY_THRESHOLD, color="red", linestyle="--", label=f"Threshold ({FREQUENCY_THRESHOLD} Hz)")
    plt.title("Average Sampling Rate per Day")
    plt.xlabel("Date")
    plt.ylabel("Avg Sampling Rate (Hz)")
    plt.xticks(rotation=45, ha="right")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUTPUT_PLOT)
    plt.close()
    print(f"Saved plot to: {OUTPUT_PLOT}")


if __name__ == "__main__":
    main()
