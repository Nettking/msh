import os
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# === CONFIGURATION ===
DATA_DIR = "data"  # Folder containing .jsonl files
OUTPUT_CSV = "sampling_rate_summary.csv"
OUTPUT_PLOT = "daily_sampling_rate.png"
FREQUENCY_THRESHOLD = 4.9  # Expected is 5.0 Hz

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
    print("No valid records found.")
    exit()

# === PARSE TO DATAFRAME ===
df = pd.DataFrame(records)
df.sort_values("timestamp", inplace=True)
df.reset_index(drop=True, inplace=True)

# === CALCULATE SAMPLING RATE ===
df["time_gap_s"] = df["timestamp"].diff().dt.total_seconds()
df["sampling_rate_hz"] = df["time_gap_s"].apply(lambda x: round(1 / x, 2) if x > 0 else None)
df["date"] = df["timestamp"].dt.date

# === DAILY AVERAGE FREQUENCY ===
daily_freq = df.groupby("date")["sampling_rate_hz"].mean().reset_index()
daily_freq.rename(columns={"sampling_rate_hz": "avg_sampling_rate_hz"}, inplace=True)

# === FLAG DAYS BELOW THRESHOLD ===
below_threshold = daily_freq[daily_freq["avg_sampling_rate_hz"] < FREQUENCY_THRESHOLD]

# === PRINT RESULT ===
print("\n📊 Daily average sampling rate:")
print(daily_freq)

if not below_threshold.empty:
    print("\n⚠️ Days with low sampling rate:")
    print(below_threshold)

# === SAVE CSV ===
daily_freq.to_csv(OUTPUT_CSV, index=False)
print(f"\nSaved summary to: {OUTPUT_CSV}")

# === PLOT LINE CHART ===
plt.figure(figsize=(10, 5))
plt.plot(daily_freq["date"], daily_freq["avg_sampling_rate_hz"], marker='o', label="Avg Frequency")
plt.axhline(y=5.0, color='green', linestyle='--', label="Target (5 Hz)")
plt.axhline(y=FREQUENCY_THRESHOLD, color='red', linestyle='--', label=f"Threshold ({FREQUENCY_THRESHOLD} Hz)")
plt.title("Average Sampling Rate per Day")
plt.xlabel("Date")
plt.ylabel("Avg Sampling Rate (Hz)")
plt.xticks(rotation=45, ha='right')
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.savefig(OUTPUT_PLOT)
plt.show()

print(f"Saved plot to: {OUTPUT_PLOT}")
