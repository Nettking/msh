"""
Generate per-machine, per-day time-series plots from JSONL telemetry data.

This script reads flat JSONL files from ``data/``, parses each record's timestamp,
groups records by machine and calendar day, and writes one PNG plot per numeric
signal under ``graphs/<machine>/<date>/``.

Behavior:
- Reads JSONL files only from the top level of ``data/`` (non-recursive).
- Skips malformed JSON lines, printing an error message for visibility.
- Skips records whose timestamps cannot be parsed.
- Requires at least the ``timestamp`` and ``machine`` fields.
- Plots all numeric columns except those explicitly excluded (currently ``sequence``).

Outputs:
- One PNG file per numeric column, per machine, per day.
- Output directory structure: ``graphs/<machine>/<YYYY-MM-DD>/<column>.png``

Notes:
- This is a raw visualization utility, not a statistical analysis step.
- The script loads all valid records into memory before plotting.
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

# Base output folder for generated plots.
GRAPH_BASE_DIR = Path("graphs")


def _warn_malformed_json(message: str) -> None:
    """
    Report malformed JSONL input lines during record loading.

    The shared loader skips malformed lines; this callback ensures they are still
    visible to the user during script execution.
    """
    print(f"Error parsing line: {message}")


records = []

# Read top-level JSONL files only. This preserves the script's original behavior
# and avoids unexpectedly traversing nested directories.
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
        # Invalid records are skipped rather than halting the full plotting run.
        print(f"Error parsing line in {file_path.name}: {e}")

if not records:
    raise SystemExit("No valid records found in data folder.")

df = pd.DataFrame(records)

# These columns are the minimum needed for grouping and plotting logic.
required_cols = {"timestamp", "machine"}
if not required_cols.issubset(df.columns):
    raise ValueError(f"Missing required columns: {required_cols - set(df.columns)}")

# Group by calendar day rather than full timestamp.
df["date"] = df["timestamp"].dt.date

for machine in df["machine"].unique():
    df_machine = df[df["machine"] == machine]

    for day in df_machine["date"].unique():
        df_day = df_machine[df_machine["date"] == day].sort_values("timestamp")

        # Organize plots by machine/day for easier inspection of generated outputs.
        day_dir = GRAPH_BASE_DIR / machine / str(day)
        day_dir.mkdir(parents=True, exist_ok=True)

        # Plot only numeric telemetry-style fields. Metadata and identifiers such
        # as timestamps, machine name, and date are naturally excluded here.
        numeric_cols = df_day.select_dtypes(include=["number"]).columns

        # Sequence is usually an index/counter rather than a meaningful signal.
        exclude_cols = {"sequence"}
        numeric_cols = [c for c in numeric_cols if c not in exclude_cols]

        for col in numeric_cols:
            plt.figure(figsize=(10, 4))
            plt.plot(df_day["timestamp"], df_day[col], marker=".", linestyle="-")
            plt.xlabel("Time")
            plt.ylabel(col)
            plt.title(f"{col} — {machine} — {day}")
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()

            plot_path = day_dir / f"{col}.png"
            plt.savefig(plot_path)
            plt.close()

print("Graphs generated in:", GRAPH_BASE_DIR)