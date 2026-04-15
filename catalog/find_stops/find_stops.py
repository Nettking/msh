import json
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

# ------------------------------------------------------------
# SETTINGS
# ------------------------------------------------------------
DATA_DIR = Path("data")
OUTPUT_DIR = Path("plots")
STOPPED_STATES = ["STOPPED"]
MAX_GAP_SECONDS = 2  

# ------------------------------------------------------------
# LOAD JSONL FILE
# ------------------------------------------------------------
def load_jsonl(file_path):
    records = []
    with open(file_path, "r") as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    if "timestamp" not in df.columns:
        return pd.DataFrame()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    return df

# ------------------------------------------------------------
# FIND STOP STATES
# ------------------------------------------------------------
def find_stops(df):
    numeric_cols = ["Srpm", "Fact", "Xfrt", "Yfrt", "Zfrt"]
    available_cols = [c for c in numeric_cols if c in df.columns]

    for col in available_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if not available_cols or "execution" not in df.columns:
        return pd.DataFrame(columns=["timestamp", "execution", "mode", "machine"])

    stopped_mask = (
        (df["execution"].isin(STOPPED_STATES)) &
        ((df[available_cols] == 0).sum(axis=1) >= max(1, len(available_cols)//2))
    )

    subset_cols = ["timestamp", "execution", "mode"]
    if "machine" in df.columns:
        subset_cols.append("machine")

    return df.loc[stopped_mask, subset_cols].copy()

# ------------------------------------------------------------
# GROUP CONSECUTIVE STOPS INTO INTERVALS
# ------------------------------------------------------------
def group_stops(df, max_gap_seconds=MAX_GAP_SECONDS):
    if df.empty:
        return pd.DataFrame(columns=["machine", "start", "end", "duration_s"])
    df = df.sort_values("timestamp")
    df["machine"] = df.get("machine", "UNKNOWN")

    grouped = []
    for machine, mdf in df.groupby("machine"):
        start, last = None, None
        for t in mdf["timestamp"]:
            if start is None:
                start, last = t, t
                continue
            gap = (t - last).total_seconds()
            if gap <= max_gap_seconds:
                last = t
            else:
                grouped.append([machine, start, last, (last - start).total_seconds()])
                start, last = t, t
        if start is not None:
            grouped.append([machine, start, last, (last - start).total_seconds()])
    return pd.DataFrame(grouped, columns=["machine", "start", "end", "duration_s"])

# ------------------------------------------------------------
# PLOT HOURLY TIMELINE
# ------------------------------------------------------------
def plot_hour(machine, hour_df, hour_label, out_path):
    if hour_df.empty:
        return
    plt.figure(figsize=(10, 2))
    for _, row in hour_df.iterrows():
        plt.hlines(1, row["start"], row["end"], colors="red", linewidth=6)
    plt.title(f"{machine} – Stops at {hour_label}")
    plt.yticks([])
    plt.xlabel("Time")
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path)
    plt.close()

# ------------------------------------------------------------
# MAIN SCRIPT
# ------------------------------------------------------------
def main():
    all_files = sorted(DATA_DIR.glob("*.jsonl"))
    if not all_files:
        print("No JSONL files found.")
        return

    print(f"Analyzing {len(all_files)} files in {DATA_DIR}...\n")

    for file_path in all_files:
        df = load_jsonl(file_path)
        if df.empty:
            continue

        stops = find_stops(df)
        grouped = group_stops(stops)
        if grouped.empty:
            continue

        grouped["day"] = grouped["start"].dt.date
        grouped["hour"] = grouped["start"].dt.hour

        for (day, machine, hour), hdf in grouped.groupby(["day", "machine", "hour"]):
            day_dir = OUTPUT_DIR / str(day) / machine
            out_path = day_dir / f"{hour:02d}.png"
            label = f"{day} {hour:02d}:00–{hour+1:02d}:00"
            plot_hour(machine, hdf, label, out_path)

        print(f"{file_path.name}: {len(grouped)} stop intervals plotted hourly.")

    print(f"\nAll hourly plots saved in: {OUTPUT_DIR.resolve()}")

if __name__ == "__main__":
    main()
