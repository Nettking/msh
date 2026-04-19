"""
Summarize missing sequence numbers per day from compact derived telemetry metrics.

Outputs:
- ``missing_per_day.csv``: daily missing-count summary
- ``missing_per_day.png``: bar chart of missing counts by day
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
OUTPUT_SUMMARY_CSV = "missing_per_day.csv"
OUTPUT_BAR_PLOT = "missing_per_day.png"


def main() -> None:
    previous_sequence: int | None = None
    skipped_sequence = 0
    parsed_rows = 0
    missing_by_day: defaultdict[object, int] = defaultdict(int)

    for timestamp, _, sequence in iter_basic_metrics_rows(DATA_DIR):
        parsed_rows += 1
        if sequence is None:
            skipped_sequence += 1
            continue

        if previous_sequence is not None:
            gap = sequence - previous_sequence
            if gap > 1:
                missing_by_day[timestamp.date()] += gap - 1
        previous_sequence = sequence

    if not missing_by_day and parsed_rows == skipped_sequence:
        raise SystemExit("No valid records with timestamp+sequence found in data folder.")

    missing_rows = [
        {"date": day, "missing_count": int(count)}
        for day, count in sorted(missing_by_day.items())
    ]
    missing_per_day = pd.DataFrame(missing_rows or [{"date": None, "missing_count": 0}]).dropna()

    print(f"Parsed {parsed_rows} rows; skipped {skipped_sequence} rows missing sequence.")
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
    plt.close()

    del missing_per_day
    print(f"Saved bar chart to: {OUTPUT_BAR_PLOT}")


if __name__ == "__main__":
    main()
