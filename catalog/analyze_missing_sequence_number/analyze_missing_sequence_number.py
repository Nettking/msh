"""
Summarize missing sequence numbers per day from JSONL telemetry data.

This script reads top-level JSONL files from ``data/``, parses timestamps,
sorts records chronologically, and estimates missing sequence values by
measuring positive gaps in the ``sequence`` column.

A gap of:
- 1 means no missing sequence values
- n > 1 means ``n - 1`` sequence values are treated as missing

The script aggregates missing counts by calendar day, prints the result,
writes a CSV summary, and saves a bar chart.

Outputs:
- ``missing_per_day.csv``: daily missing-count summary
- ``missing_per_day.png``: bar chart of missing counts by day

Notes:
- This script reads only top-level files in ``data/`` (non-recursive).
- It assumes that ``sequence`` values are meaningful across the sorted stream.
- If multiple machines are mixed in the same dataset, cross-machine sequence
  transitions may inflate the missing-count estimate unless the data is
  pre-filtered appropriately.
"""

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from catalog.common.data_loading import iter_records_with_parsed_timestamps

# Folder containing input JSONL files.
DATA_DIR = "data"

# Output files written in the current working directory.
OUTPUT_SUMMARY_CSV = "missing_per_day.csv"
OUTPUT_BAR_PLOT = "missing_per_day.png"


def _warn_malformed_json(message: str) -> None:
    """
    Report malformed JSONL lines encountered during loading.

    The shared loader skips malformed lines; this callback keeps those skips
    visible during script execution.
    """
    print(f"Error parsing line: {message}")


def _warn_invalid_timestamp(file_path: Path, raw_timestamp: object) -> None:
    """
    Report records whose timestamp cannot be parsed.
    """
    print(f"Error parsing line in {file_path.name}: Invalid isoformat string: {raw_timestamp}")


records = []

# Preserve original script behavior by scanning only top-level JSONL files.
for _, entry in iter_records_with_parsed_timestamps(
    DATA_DIR,
    recursive=False,
    allow_z_suffix=True,
    on_malformed_json=_warn_malformed_json,
    on_invalid_timestamp=_warn_invalid_timestamp,
):
    records.append(entry)

if not records:
    raise SystemExit("No valid records found in data folder.")

df = pd.DataFrame(records)
df.sort_values("timestamp", inplace=True)
df.reset_index(drop=True, inplace=True)

# Estimate missing sequence values from gaps between consecutive sequence numbers.
# Example:
# - diff == 1  -> no missing sequence values
# - diff == 4  -> three missing sequence values
df["sequence_gap"] = df["sequence"].diff().fillna(1).astype(int)
df["missing_count"] = df["sequence_gap"].apply(lambda g: max(g - 1, 0))

# Aggregate by calendar day.
df["date"] = df["timestamp"].dt.date
missing_per_day = df.groupby("date")["missing_count"].sum().reset_index()

print("\nMissing sequence numbers per day:")
print(missing_per_day)

missing_per_day.to_csv(OUTPUT_SUMMARY_CSV, index=False)
print(f"\nSaved daily summary to: {OUTPUT_SUMMARY_CSV}")

plt.figure(figsize=(10, 5))
plt.bar(
    missing_per_day["date"].astype(str),
    missing_per_day["missing_count"],
    color="steelblue",
)
plt.xticks(rotation=45, ha="right")
plt.ylabel("Missing Sequence Numbers")
plt.title("Missing Sequence Numbers per Day")
plt.tight_layout()
plt.savefig(OUTPUT_BAR_PLOT)
plt.show()

print(f"Saved bar chart to: {OUTPUT_BAR_PLOT}")
