import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from catalog.common.data_loading import iter_records_in_dir
from catalog.common.time_utils import parse_iso_timestamp

# === CONFIGURATION ===
DATA_DIR = "data"  # Folder containing your .jsonl files
OUTPUT_SUMMARY_CSV = "missing_per_day.csv"
OUTPUT_BAR_PLOT = "missing_per_day.png"

# === READ AND COMBINE JSONL FILES ===
def _warn_malformed_json(message: str) -> None:
    print(f"Error parsing line: {message}")


records = []
for file_path, entry in iter_records_in_dir(DATA_DIR, recursive=False, on_malformed_json=_warn_malformed_json):
    try:
        parsed_timestamp = parse_iso_timestamp(entry.get("timestamp"), allow_z_suffix=True)
        if parsed_timestamp is None:
            raise ValueError(f"Invalid isoformat string: {entry.get('timestamp')}")
        entry["timestamp"] = parsed_timestamp
        records.append(entry)
    except Exception as e:
        print(f"Error parsing line in {file_path.name}: {e}")

if not records:
    print("No valid records found in data folder.")
    exit()

# === PARSE TO DATAFRAME ===
df = pd.DataFrame(records)
df.sort_values("timestamp", inplace=True)
df.reset_index(drop=True, inplace=True)

# === CALCULATE GAPS AND MISSING COUNTS ===
df["sequence_gap"] = df["sequence"].diff().fillna(1).astype(int)
df["missing_count"] = df["sequence_gap"].apply(lambda g: max(g - 1, 0))
df["date"] = df["timestamp"].dt.date

# === AGGREGATE MISSING PER DAY ===
missing_per_day = df.groupby("date")["missing_count"].sum().reset_index()

# === PRINT TO CONSOLE ===
print("\nMissing sequence numbers per day:")
print(missing_per_day)

# === SAVE SUMMARY TO CSV ===
missing_per_day.to_csv(OUTPUT_SUMMARY_CSV, index=False)
print(f"\nSaved daily summary to: {OUTPUT_SUMMARY_CSV}")

# === PLOT BAR CHART ===
plt.figure(figsize=(10, 5))
plt.bar(missing_per_day["date"].astype(str), missing_per_day["missing_count"], color='steelblue')
plt.xticks(rotation=45, ha='right')
plt.ylabel("Missing Sequence Numbers")
plt.title("Missing Sequence Numbers per Day")
plt.tight_layout()
plt.savefig(OUTPUT_BAR_PLOT)
plt.show()
print(f"Saved bar chart to: {OUTPUT_BAR_PLOT}")
