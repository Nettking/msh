import os
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# === CONFIGURATION ===
DATA_DIR = "data"  # Folder containing your .jsonl files
OUTPUT_SUMMARY_CSV = "machines_active_per_day.csv"
OUTPUT_PLOT = "machines_active_per_day.png"

# === READ AND COMBINE JSONL FILES ===
records = []
for filename in sorted(os.listdir(DATA_DIR)):
    if filename.endswith(".jsonl"):
        filepath = os.path.join(DATA_DIR, filename)
        with open(filepath, "r") as f:
            for line in f:
                if line.strip():
                    try:
                        entry = json.loads(line)
                        entry["timestamp"] = datetime.fromisoformat(entry["timestamp"])
                        records.append(entry)
                    except Exception as e:
                        print(f"Error parsing line in {filename}: {e}")

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
