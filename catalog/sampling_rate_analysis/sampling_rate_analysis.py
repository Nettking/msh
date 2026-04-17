"""
Summarize average sampling rate per day from JSONL telemetry data.

This script reads top-level JSONL files from ``data/``, parses timestamps,
sorts records chronologically, estimates instantaneous sampling rate from the
time gap between consecutive rows, and then computes the daily average sampling
rate.

A day is flagged as low-rate if its average sampling rate falls below the
configured threshold.

Outputs
-------
- ``sampling_rate_summary.csv``:
    daily average sampling rate summary
- ``daily_sampling_rate.png``:
    line plot of average daily sampling rate with target and threshold markers

Notes
-----
- This script reads only top-level JSONL files in ``data/``.
- Rows with malformed JSON or invalid timestamps are skipped.
- The sampling rate is estimated as ``1 / time_gap_s`` between consecutive rows.
- If multiple machines or streams are mixed together, the estimated rate may not
  reflect the true sampling rate of any single source.
- The daily value is the mean of per-row instantaneous rates, which is an
  exploratory indicator rather than a strict acquisition-system metric.
"""

import json
import os
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd

# Folder containing input JSONL files.
DATA_DIR = "data"

# Output CSV containing daily average sampling-rate estimates.
OUTPUT_CSV = "sampling_rate_summary.csv"

# Output plot showing daily sampling-rate trends.
OUTPUT_PLOT = "daily_sampling_rate.png"

# Days below this daily average are highlighted as potentially problematic.
# The nominal target is 5.0 Hz.
FREQUENCY_THRESHOLD = 4.9


records = []

# Preserve original behavior by reading only top-level JSONL files.
for filename in sorted(os.listdir(DATA_DIR)):
    if filename.endswith(".jsonl"):
        filepath = os.path.join(DATA_DIR, filename)
        with open(filepath, "r") as f:
            for line in f:
                if line.strip():
                    try:
                        entry = json.loads(line)
                        entry["timestamp"] = datetime.fromisoformat(entry["timestamp"])
                        records.append(entry)
                    except Exception as e:
                        # Invalid rows are skipped instead of halting the full run.
                        print(f"Error parsing line in {filename}: {e}")

if not records:
    raise SystemExit("No valid records found.")

df = pd.DataFrame(records)
df.sort_values("timestamp", inplace=True)
df.reset_index(drop=True, inplace=True)

# Estimate instantaneous sampling rate from the time gap between consecutive rows.
# For a gap of x seconds, the estimated rate is 1 / x Hz.
df["time_gap_s"] = df["timestamp"].diff().dt.total_seconds()
df["sampling_rate_hz"] = df["time_gap_s"].apply(
    lambda x: round(1 / x, 2) if x > 0 else None
)
df["date"] = df["timestamp"].dt.date

# Aggregate to a daily average sampling-rate estimate.
daily_freq = df.groupby("date")["sampling_rate_hz"].mean().reset_index()
daily_freq.rename(columns={"sampling_rate_hz": "avg_sampling_rate_hz"}, inplace=True)

# Highlight days whose average falls below the configured threshold.
below_threshold = daily_freq[daily_freq["avg_sampling_rate_hz"] < FREQUENCY_THRESHOLD]

print("\nDaily average sampling rate:")
print(daily_freq)

if not below_threshold.empty:
    print("\nDays with low sampling rate:")
    print(below_threshold)

daily_freq.to_csv(OUTPUT_CSV, index=False)
print(f"\nSaved summary to: {OUTPUT_CSV}")

plt.figure(figsize=(10, 5))
plt.plot(
    daily_freq["date"],
    daily_freq["avg_sampling_rate_hz"],
    marker="o",
    label="Avg Frequency",
)
plt.axhline(y=5.0, color="green", linestyle="--", label="Target (5 Hz)")
plt.axhline(
    y=FREQUENCY_THRESHOLD,
    color="red",
    linestyle="--",
    label=f"Threshold ({FREQUENCY_THRESHOLD} Hz)",
)
plt.title("Average Sampling Rate per Day")
plt.xlabel("Date")
plt.ylabel("Avg Sampling Rate (Hz)")
plt.xticks(rotation=45, ha="right")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.savefig(OUTPUT_PLOT)
plt.show()

print(f"Saved plot to: {OUTPUT_PLOT}")