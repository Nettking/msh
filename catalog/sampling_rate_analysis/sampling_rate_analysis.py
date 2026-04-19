"""Summarize average sampling rate per day from compact derived telemetry metrics."""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from catalog.common.basic_metrics import iter_basic_metrics_rows

DATA_DIR = Path("data")
OUTPUT_CSV = "sampling_rate_summary.csv"
OUTPUT_PLOT = "daily_sampling_rate.png"
FREQUENCY_THRESHOLD = 4.9


def main() -> None:
    prev_timestamp = None
    daily_rate_sum: defaultdict[object, float] = defaultdict(float)
    daily_rate_count: defaultdict[object, int] = defaultdict(int)
    parsed_rows = 0

    for timestamp, _, _ in iter_basic_metrics_rows(DATA_DIR):
        parsed_rows += 1
        if prev_timestamp is None:
            prev_timestamp = timestamp
            continue

        gap_seconds = (timestamp - prev_timestamp).total_seconds()
        prev_timestamp = timestamp
        if gap_seconds <= 0:
            continue

        sample_rate = 1 / gap_seconds
        day = timestamp.date()
        daily_rate_sum[day] += sample_rate
        daily_rate_count[day] += 1

    if not daily_rate_count:
        raise SystemExit("No valid records found.")

    daily_rows = []
    for day in sorted(daily_rate_count):
        avg_rate = round(daily_rate_sum[day] / daily_rate_count[day], 3)
        daily_rows.append({"date": day, "avg_sampling_rate_hz": avg_rate})

    daily_freq = pd.DataFrame(daily_rows)
    below_threshold = daily_freq[daily_freq["avg_sampling_rate_hz"] < FREQUENCY_THRESHOLD]

    print(f"Parsed {parsed_rows} rows for sampling-rate estimation.")
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

    del daily_freq
    print(f"Saved plot to: {OUTPUT_PLOT}")


if __name__ == "__main__":
    main()
