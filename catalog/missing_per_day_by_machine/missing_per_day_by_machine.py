"""Summarize missing sequence numbers per day for each machine."""

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
OUTPUT_SUMMARY_CSV = "missing_per_day_by_machine.csv"
OUTPUT_DIR_PLOTS = Path("plots_per_machine")
OUTPUT_DIR_PLOTS.mkdir(exist_ok=True)


def main() -> None:
    prev_by_machine: dict[str, int] = {}
    missing_by_machine_day: defaultdict[tuple[str, object], int] = defaultdict(int)
    parsed_rows = 0

    for timestamp, machine, sequence in iter_basic_metrics_rows(DATA_DIR):
        parsed_rows += 1
        if machine is None or sequence is None:
            continue

        prev_sequence = prev_by_machine.get(machine)
        if prev_sequence is not None:
            gap = sequence - prev_sequence
            if gap > 1:
                missing_by_machine_day[(machine, timestamp.date())] += gap - 1
        prev_by_machine[machine] = sequence

    if not prev_by_machine:
        raise SystemExit("No valid records with timestamp+machine+sequence found in data folder.")

    rows = [
        {"machine": machine, "date": day, "missing_count": int(count)}
        for (machine, day), count in sorted(missing_by_machine_day.items())
    ]
    missing_per_day_machine = pd.DataFrame(rows, columns=["machine", "date", "missing_count"])

    print(f"Parsed {parsed_rows} rows.")
    print("\nMissing sequence numbers per day per machine:")
    print(missing_per_day_machine)
    missing_per_day_machine.to_csv(OUTPUT_SUMMARY_CSV, index=False)
    print(f"\nSaved daily summary to: {OUTPUT_SUMMARY_CSV}")

    for machine, chunk in missing_per_day_machine.groupby("machine"):
        plt.figure(figsize=(10, 5))
        plt.bar(chunk["date"].astype(str), chunk["missing_count"])
        plt.xticks(rotation=45, ha="right")
        plt.ylabel("Missing Sequence Numbers")
        plt.title(f"Missing Sequence Numbers per Day — {machine}")
        plt.tight_layout()
        out_path = OUTPUT_DIR_PLOTS / f"missing_per_day_{machine}.png"
        plt.savefig(out_path)
        plt.close()
        print(f"Saved bar chart for {machine} to: {out_path}")

    del missing_per_day_machine


if __name__ == "__main__":
    main()
