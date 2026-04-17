import sys
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from catalog.common.data_loading import iter_records_in_dir
from catalog.common.time_utils import parse_iso_timestamp

# === CONFIGURATION ===
DATA_DIR = "data"  # Folder containing your .jsonl files
GRAPH_BASE_DIR = Path("graphs")  # Output folder for all graphs

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
