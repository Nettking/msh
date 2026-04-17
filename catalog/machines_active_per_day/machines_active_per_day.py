"""
Summarize how many distinct machines appear in telemetry data per day.

This script reads top-level JSONL files from ``data/``, parses timestamps,
extracts the calendar date from each record, and counts how many unique machine
identifiers are present on each day.

Outputs:
- ``machines_active_per_day.csv``: daily summary of distinct machine counts
- ``machines_active_per_day.png``: bar chart of distinct machine counts by day

Notes:
- A machine is counted as "active" if it appears in at least one record on that
  day. This does not necessarily mean the machine was operational; it only means
  that telemetry was recorded for it.
- This script reads only top-level JSONL files in ``data/`` (non-recursive).
- Records with malformed JSON or invalid timestamps are skipped.
"""

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from catalog.common.data_loading import iter_records_in_dir
from catalog.common.time_utils import parse_iso_timestamp

# Folder containing input JSONL files.
DATA_DIR = "data"

# Output files written in the current working directory.
OUTPUT_SUMMARY_CSV = "machines_active_per_day.csv"
OUTPUT_PLOT = "machines_active_per_day.png"


def _warn_malformed_json(message: str) -> None:
    """
    Report malformed JSONL lines encountered during loading.

    The shared loader skips malformed lines; this callback keeps them visible
    during script execution.
    """
    print(f"Error parsing line: {message}")


records = []

# Preserve original behavior by reading only top-level JSONL files.
for file_path, entry in iter_records_in_dir(
    DATA_DIR,
    recursive=False,
    on_malformed_json=_warn_malformed_json,
):
    try:
        parsed_timestamp = parse_iso_timestamp(
            entry.get("timestamp"),
            allow_z_suffix=True,
        )
        if parsed_timestamp is None:
            raise ValueError(f"Invalid isoformat string: {entry.get('timestamp')}")

        entry["timestamp"] = parsed_timestamp
        records.append(entry)

    except Exception as e:
        # Invalid records are skipped rather than halting the whole run.
        print(f"Error parsing line in {file_path.name}: {e}")

if not records:
    raise SystemExit("No valid records found in data folder.")

df = pd.DataFrame(records)

# These columns are required to assign rows to days and machines.
required_cols = {"timestamp", "machine"}
if not required_cols.issubset(df.columns):
    raise ValueError(f"Missing required columns: {required_cols - set(df.columns)}")

# Count distinct machines present on each calendar day.
df["date"] = df["timestamp"].dt.date
machines_active_per_day = (
    df.groupby("date")["machine"].nunique().reset_index(name="machines_active")
)

print("\nMachines active per day:")
print(machines_active_per_day)

machines_active_per_day.to_csv(OUTPUT_SUMMARY_CSV, index=False)
print(f"\nSaved summary to: {OUTPUT_SUMMARY_CSV}")

plt.figure(figsize=(10, 5))
plt.bar(
    machines_active_per_day["date"].astype(str),
    machines_active_per_day["machines_active"],
    color="seagreen",
)
plt.xticks(rotation=45, ha="right")
plt.ylabel("Number of Machines Active")
plt.title("Active Machines per Day")
plt.tight_layout()
plt.savefig(OUTPUT_PLOT)
plt.show()

print(f"Saved bar chart to: {OUTPUT_PLOT}")