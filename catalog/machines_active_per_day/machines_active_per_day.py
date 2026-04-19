"""
Summarize how many distinct machines appear in telemetry data per day.

This script reads the compact derived dataset in ``data/_derived/basic_metrics.csv``
when available (generated during orchestration) so startup avoids re-parsing every
JSONL payload repeatedly.

Outputs:
- ``machines_active_per_day.csv``: daily summary of distinct machine counts
- ``machines_active_per_day.png``: bar chart of distinct machine counts by day
"""

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
OUTPUT_SUMMARY_CSV = "machines_active_per_day.csv"
OUTPUT_PLOT = "machines_active_per_day.png"


def main() -> None:
    machines_by_day: defaultdict[object, set[str]] = defaultdict(set)
    parsed_rows = 0

    for timestamp, machine, _ in iter_basic_metrics_rows(DATA_DIR):
        if machine is None:
            continue
        machines_by_day[timestamp.date()].add(machine)
        parsed_rows += 1

    if not machines_by_day:
        raise SystemExit("No valid records with both timestamp and machine found in data folder.")

    summary_rows = [
        {"date": day, "machines_active": len(machines)}
        for day, machines in sorted(machines_by_day.items())
    ]
    machines_active_per_day = pd.DataFrame(summary_rows)

    print(f"Parsed {parsed_rows} rows for machine/day activity.")
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
    plt.close()

    del machines_active_per_day
    print(f"Saved bar chart to: {OUTPUT_PLOT}")


if __name__ == "__main__":
    main()
