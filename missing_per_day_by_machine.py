import os
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path

# === CONFIGURATION ===
DATA_DIR = "data"                          # Folder containing your .jsonl files
OUTPUT_SUMMARY_CSV = "missing_per_day_by_machine.csv"
OUTPUT_DIR_PLOTS = Path("plots_per_machine")
OUTPUT_DIR_PLOTS.mkdir(exist_ok=True)

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

# Basic sanity checks
required_cols = {"timestamp", "sequence", "machine"}
missing_cols = required_cols - set(df.columns)
if missing_cols:
    raise ValueError(f"Missing required columns: {missing_cols}")

# Sort by machine then time
df.sort_values(["machine", "timestamp"], inplace=True)
df.reset_index(drop=True, inplace=True)

# === CALCULATE GAPS AND MISSING COUNTS (PER MACHINE) ===
# Diff must be within each machine, not across all rows
df["sequence_gap"] = df.groupby("machine")["sequence"].diff().fillna(1).astype(int)
df["missing_count"] = df["sequence_gap"].clip(lower=1) - 1  # max(g-1, 0)
df["date"] = df["timestamp"].dt.date

# === AGGREGATE MISSING PER DAY & MACHINE ===
missing_per_day_machine = (
    df.groupby(["machine", "date"], as_index=False)["missing_count"].sum()
    .sort_values(["machine", "date"])
)

# === PRINT TO CONSOLE ===
print("\nMissing sequence numbers per day per machine:")
print(missing_per_day_machine)

# === SAVE SUMMARY TO CSV ===
missing_per_day_machine.to_csv(OUTPUT_SUMMARY_CSV, index=False)
print(f"\nSaved daily summary to: {OUTPUT_SUMMARY_CSV}")

# === PLOT BAR CHARTS: ONE PER MACHINE ===
for machine, chunk in missing_per_day_machine.groupby("machine"):
    plt.figure(figsize=(10, 5))
    x = chunk["date"].astype(str)
    y = chunk["missing_count"]
    plt.bar(x, y)
    plt.xticks(rotation=45, ha='right')
    plt.ylabel("Missing Sequence Numbers")
    plt.title(f"Missing Sequence Numbers per Day — {machine}")
    plt.tight_layout()
    out_path = OUTPUT_DIR_PLOTS / f"missing_per_day_{machine}.png"
    plt.savefig(out_path)
    plt.show()
    print(f"Saved bar chart for {machine} to: {out_path}")
