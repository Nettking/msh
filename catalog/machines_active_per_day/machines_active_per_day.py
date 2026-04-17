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
OUTPUT_SUMMARY_CSV = "machines_active_per_day.csv"
OUTPUT_PLOT = "machines_active_per_day.png"

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
    raise SystemExit("No valid records found in data folder.")

# === PARSE TO DATAFRAME ===
df = pd.DataFrame(records)

# Check columns
required_cols = {"timestamp", "machine"}
if not required_cols.issubset(df.columns):
    raise ValueError(f"Missing required columns: {required_cols - set(df.columns)}")

# Extract date and count distinct machines
df["date"] = df["timestamp"].dt.date
machines_active_per_day = (
    df.groupby("date")["machine"].nunique().reset_index(name="machines_active")
)

# Print results
print("\nMachines active per day:")
print(machines_active_per_day)

# Save to CSV
machines_active_per_day.to_csv(OUTPUT_SUMMARY_CSV, index=False)
print(f"\nSaved summary to: {OUTPUT_SUMMARY_CSV}")

# === PLOT BAR CHART ===
plt.figure(figsize=(10, 5))
plt.bar(machines_active_per_day["date"].astype(str),
        machines_active_per_day["machines_active"],
        color='seagreen')
plt.xticks(rotation=45, ha='right')
plt.ylabel("Number of Machines Active")
plt.title("Active Machines per Day")
plt.tight_layout()
plt.savefig(OUTPUT_PLOT)
plt.show()
print(f"Saved bar chart to: {OUTPUT_PLOT}")
