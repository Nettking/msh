"""Summarize missing sequence numbers per day for each machine."""

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
OUTPUT_SUMMARY_CSV = "missing_per_day_by_machine.csv"
OUTPUT_DIR_PLOTS = Path("plots_per_machine")
OUTPUT_DIR_PLOTS.mkdir(exist_ok=True)


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
    rows: list[tuple[str, pd.Timestamp, int]] = []

    for _, entry in iter_records_with_parsed_timestamps(
        DATA_DIR,
        recursive=False,
        allow_z_suffix=True,
        on_malformed_json=_warn_malformed_json,
        on_invalid_timestamp=_warn_invalid_timestamp,
    ):
        machine = entry.get("machine")
        seq = _to_int_sequence(entry.get("sequence"))
        if machine is None or seq is None:
            continue
        rows.append((str(machine), entry["timestamp"], seq))

    if not rows:
        raise SystemExit("No valid records with timestamp+machine+sequence found in data folder.")

    df = pd.DataFrame(rows, columns=["machine", "timestamp", "sequence"])
    df.sort_values(["machine", "timestamp"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df["sequence_gap"] = df.groupby("machine")["sequence"].diff().fillna(1)
    df["missing_count"] = (df["sequence_gap"] - 1).clip(lower=0)
    df["date"] = df["timestamp"].dt.date

    missing_per_day_machine = (
        df.groupby(["machine", "date"], as_index=False)["missing_count"].sum().sort_values(["machine", "date"])
    )
    missing_per_day_machine["missing_count"] = missing_per_day_machine["missing_count"].astype(int)

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


if __name__ == "__main__":
    main()
