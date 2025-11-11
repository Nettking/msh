import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import json

DATA_DIR = Path("data")
STOPPED_STATES = ["STOPPED", "READY"]
MAX_GAP_SECONDS = 120

# --------------------------------------------------------------------
# Load and reuse logic from previous scripts
# --------------------------------------------------------------------
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

def find_stops(df):
    numeric_cols = ["Srpm", "Fact", "Xfrt", "Yfrt", "Zfrt"]
    available_cols = [c for c in numeric_cols if c in df.columns]
    for col in available_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if not available_cols or "execution" not in df.columns:
        return pd.DataFrame(columns=["timestamp","machine"])
    stopped_mask = (
        (df["execution"].isin(STOPPED_STATES)) &
        ((df[available_cols] == 0).sum(axis=1) >= max(1, len(available_cols)//2))
    )
    subset_cols = ["timestamp"]
    if "machine" in df.columns:
        subset_cols.append("machine")
    return df.loc[stopped_mask, subset_cols].copy()

def group_stops(df, max_gap_seconds=MAX_GAP_SECONDS):
    if df.empty:
        return pd.DataFrame(columns=["machine","start","end","duration_s"])
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
    return pd.DataFrame(grouped, columns=["machine","start","end","duration_s"])

# --------------------------------------------------------------------
# Main correlation analysis
# --------------------------------------------------------------------
def main():
    all_files = sorted(DATA_DIR.glob("*.jsonl"))
    all_intervals = []
    for file_path in all_files:
        df = load_jsonl(file_path)
        if df.empty:
            continue
        stops = find_stops(df)
        grouped = group_stops(stops)
        if not grouped.empty:
            all_intervals.append(grouped)
    if not all_intervals:
        print("No data found.")
        return

    df = pd.concat(all_intervals, ignore_index=True)
    df["hour"] = df["start"].dt.floor("H")

    # aggregate total stop time per machine per hour
    pivot = (
        df.groupby(["hour", "machine"])["duration_s"]
        .sum()
        .unstack(fill_value=0)
    )

    # compute correlation matrix across machines
    corr = pivot.corr()

    # visualize correlation
    plt.figure(figsize=(6, 5))
    plt.imshow(corr, cmap="coolwarm", vmin=-1, vmax=1)
    plt.colorbar(label="Correlation coefficient")
    plt.xticks(range(len(corr.columns)), corr.columns, rotation=45)
    plt.yticks(range(len(corr.columns)), corr.index)
    plt.title("Correlation of stop patterns between machines")
    plt.tight_layout()
    plt.savefig("correlation_heatmap.png", dpi=300)
    plt.show()

    print("\nCorrelation matrix:")
    print(corr.round(2))

if __name__ == "__main__":
    main()
