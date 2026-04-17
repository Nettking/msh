"""
Summarize missing sequence numbers per day for each machine.

This script reads top-level JSONL telemetry files from ``data/``, parses
timestamps, and estimates missing sequence values separately for each machine.
It then aggregates the missing counts by machine and day, writes a CSV summary,
and saves one bar chart per machine.

A sequence gap is interpreted as follows:
- gap == 1  -> no missing sequence values
- gap == n  -> ``n - 1`` missing sequence values

Important
---------
Sequence gaps are computed within each machine independently. This avoids
artificial missing counts caused by transitions between different machines in a
mixed dataset.

Outputs
-------
- ``missing_per_day_by_machine.csv``:
    daily missing-count summary per machine
- ``plots_per_machine/missing_per_day_<machine>.png``:
    one bar chart per machine

Notes
-----
- This script reads only top-level JSONL files in ``data/``.
- Rows with invalid JSON or invalid timestamps are skipped.
- The result still depends on the assumption that sequence values are
  meaningful within each machine stream after chronological sorting.
"""

import json
import os
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# Folder containing input JSONL files.
DATA_DIR = "data"

# Output CSV containing daily missing counts per machine.
OUTPUT_SUMMARY_CSV = "missing_per_day_by_machine.csv"

# Directory for per-machine plots.
OUTPUT_DIR_PLOTS = Path("plots_per_machine")
OUTPUT_DIR_PLOTS.mkdir(exist_ok=True)


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
                        # Invalid lines are skipped rather than stopping the full run.
                        print(f"Error parsing line in {filename}: {e}")

if not records:
    raise SystemExit("No valid records found in data folder.")

df = pd.DataFrame(records)

# These columns are required to compute sequence gaps per machine over time.
required_cols = {"timestamp", "sequence", "machine"}
missing_cols = required_cols - set(df.columns)
if missing_cols:
    raise ValueError(f"Missing required columns: {missing_cols}")

# Sort within machine/time order before computing sequence differences.
df.sort_values(["machine", "timestamp"], inplace=True)
df.reset_index(drop=True, inplace=True)

# Estimate missing sequence values separately for each machine.
#
# Example:
# - diff == 1  -> no missing sequence values
# - diff == 4  -> three missing sequence values
#
# Grouping by machine is essential here; otherwise cross-machine transitions
# would produce meaningless sequence gaps.
df["sequence_gap"] = df.groupby("machine")["sequence"].diff().fillna(1).astype(int)
df["missing_count"] = df["sequence_gap"].clip(lower=1) - 1

# Aggregate by machine and calendar day.
df["date"] = df["timestamp"].dt.date
missing_per_day_machine = (
    df.groupby(["machine", "date"], as_index=False)["missing_count"].sum()
    .sort_values(["machine", "date"])
)

print("\nMissing sequence numbers per day per machine:")
print(missing_per_day_machine)

missing_per_day_machine.to_csv(OUTPUT_SUMMARY_CSV, index=False)
print(f"\nSaved daily summary to: {OUTPUT_SUMMARY_CSV}")

# Save one bar chart per machine for easier comparison over time.
for machine, chunk in missing_per_day_machine.groupby("machine"):
    plt.figure(figsize=(10, 5))
    x = chunk["date"].astype(str)
    y = chunk["missing_count"]

    plt.bar(x, y)
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Missing Sequence Numbers")
    plt.title(f"Missing Sequence Numbers per Day — {machine}")
    plt.tight_layout()

    out_path = OUTPUT_DIR_PLOTS / f"missing_per_day_{machine}.png"
    plt.savefig(out_path)
    plt.show()

    print(f"Saved bar chart for {machine} to: {out_path}")