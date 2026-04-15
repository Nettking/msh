import os
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path

# === CONFIGURATION ===
DATA_DIR = "data"  # Folder containing your .jsonl files
GRAPH_BASE_DIR = Path("graphs")  # Output folder for all graphs

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

# Ensure we have timestamp and machine
required_cols = {"timestamp", "machine"}
if not required_cols.issubset(df.columns):
    raise ValueError(f"Missing required columns: {required_cols - set(df.columns)}")

# Add date column
df["date"] = df["timestamp"].dt.date

# === LOOP OVER MACHINES AND DAYS ===
for machine in df["machine"].unique():
    df_machine = df[df["machine"] == machine]

    for day in df_machine["date"].unique():
        df_day = df_machine[df_machine["date"] == day].sort_values("timestamp")

        # Create output directory: graphs/machine/day/
        day_dir = GRAPH_BASE_DIR / machine / str(day)
        day_dir.mkdir(parents=True, exist_ok=True)

        # Select numeric columns only (skip timestamp, machine, date, etc.)
        numeric_cols = df_day.select_dtypes(include=["number"]).columns
        exclude_cols = {"sequence"}  # optional: skip sequence if not wanted
        numeric_cols = [c for c in numeric_cols if c not in exclude_cols]

        # Plot each numeric variable for this machine/day
        for col in numeric_cols:
            plt.figure(figsize=(10, 4))
            plt.plot(df_day["timestamp"], df_day[col], marker=".", linestyle="-")
            plt.xlabel("Time")
            plt.ylabel(col)
            plt.title(f"{col} — {machine} — {day}")
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()

            # Save plot
            plot_path = day_dir / f"{col}.png"
            plt.savefig(plot_path)
            plt.close()

print("Graphs generated in:", GRAPH_BASE_DIR)
