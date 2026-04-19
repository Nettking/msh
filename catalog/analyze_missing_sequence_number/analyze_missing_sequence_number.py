"""
Summarize missing sequence numbers per day from JSONL telemetry data.

This script reads top-level JSONL files from ``data/``, extracts only the
fields needed for this analysis, sorts by timestamp, estimates positive sequence
number gaps, and aggregates missing counts by calendar day.

Outputs:
- ``missing_per_day.csv``: daily missing-count summary
- ``missing_per_day.png``: bar chart of missing counts by day
"""

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
OUTPUT_SUMMARY_CSV = "missing_per_day.csv"
OUTPUT_BAR_PLOT = "missing_per_day.png"


def _warn_malformed_json(message: str) -> None:
    print(f"Error parsing line: {message}")


def _warn_invalid_timestamp(file_path: Path, raw_timestamp: object) -> None:
    print(f"Error parsing line in {file_path.name}: Invalid isoformat string: {raw_timestamp}")


def _to_int_sequence(raw_value: object) -> int | None:
    if raw_value is None:
        return None
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


def main() -> None:
    rows: list[tuple[pd.Timestamp, int]] = []
    skipped_sequence = 0

    for _, entry in iter_records_with_parsed_timestamps(
        DATA_DIR,
        recursive=False,
        allow_z_suffix=True,
        on_malformed_json=_warn_malformed_json,
        on_invalid_timestamp=_warn_invalid_timestamp,
    ):
        seq = _to_int_sequence(entry.get("sequence"))
        if seq is None:
            skipped_sequence += 1
            continue
        rows.append((entry["timestamp"], seq))

    if not rows:
        raise SystemExit("No valid records with timestamp+sequence found in data folder.")

    df = pd.DataFrame(rows, columns=["timestamp", "sequence"])
    df.sort_values("timestamp", inplace=True)
    df.reset_index(drop=True, inplace=True)

    df["sequence_gap"] = df["sequence"].diff().fillna(1)
    # Keep only positive forward gaps; negative/zero jumps are not "missing"
    # values in this simple stream-based estimator.
    df["missing_count"] = (df["sequence_gap"] - 1).clip(lower=0)

    df["date"] = df["timestamp"].dt.date
    missing_per_day = df.groupby("date", as_index=False)["missing_count"].sum()
    missing_per_day["missing_count"] = missing_per_day["missing_count"].astype(int)

    print(f"Parsed {len(df)} timestamp+sequence rows; skipped {skipped_sequence} rows missing sequence.")
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

    print(f"Saved bar chart to: {OUTPUT_BAR_PLOT}")


if __name__ == "__main__":
    main()
