import os
import json
from pathlib import Path

import pandas as pd

DATA_DIR = Path(r"C:\wsl\msh\data")
STATE_OUTPUT = Path("intervention_states.csv")
OVERRIDE_OUTPUT = Path("override_changes.csv")

# Only load columns we actually use
KEEP_COLS = [
    "timestamp",
    "sequence",
    "mode",
    "execution",
    "program",
    "line",
    "Tool_number",
    "Frapidovr",
    "Fovr",
    "Sovr",
]

INTERVENTION_STATES = ["FEED_HOLD", "INTERRUPTED", "PROGRAM_STOPPED"]
OVERRIDE_COLS = ["Frapidovr", "Fovr", "Sovr"]


def read_jsonl_selected(path: Path, keep_cols: list[str]) -> pd.DataFrame:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            rows.append({k: obj.get(k) for k in keep_cols})

    if not rows:
        return pd.DataFrame(columns=keep_cols)

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.sort_values(["timestamp", "sequence"], kind="stable").reset_index(drop=True)
    return df


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    # Keep only production-relevant rows
    df = df[df["mode"] == "AUTOMATIC"].copy()

    # Normalize numeric override columns
    for col in OVERRIDE_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove rows without timestamp or execution
    df = df[df["timestamp"].notna()].copy()
    return df.reset_index(drop=True)


def detect_state_episodes_fast(df: pd.DataFrame, state: str, source_file: str) -> pd.DataFrame:
    """
    Detect contiguous episodes of a given execution state using vectorized masks.
    Start = current row is 'state' and previous row is not 'state'
    End   = current row is 'state' and next row is not 'state'
    Duration = end_time - start_time
    """
    if df.empty or "execution" not in df.columns:
        return pd.DataFrame(columns=[
            "source_file", "type", "start", "end", "duration_s",
            "program", "line_start", "line_end", "tool"
        ])

    is_state = df["execution"].eq(state)

    starts = is_state & ~is_state.shift(fill_value=False)
    ends = is_state & ~is_state.shift(-1, fill_value=False)

    start_df = df.loc[starts, ["timestamp", "program", "line", "Tool_number"]].copy()
    end_df = df.loc[ends, ["timestamp", "line"]].copy()

    # Reset index so rows align by episode order
    start_df = start_df.reset_index(drop=True)
    end_df = end_df.reset_index(drop=True)

    # In a well-formed sequence these counts should match.
    # If not, trim to the shorter one to avoid crashing.
    n = min(len(start_df), len(end_df))
    start_df = start_df.iloc[:n].copy()
    end_df = end_df.iloc[:n].copy()

    out = pd.DataFrame({
        "source_file": source_file,
        "type": state,
        "start": start_df["timestamp"],
        "end": end_df["timestamp"],
        "duration_s": (end_df["timestamp"] - start_df["timestamp"]).dt.total_seconds(),
        "program": start_df["program"],
        "line_start": start_df["line"],
        "line_end": end_df["line"],
        "tool": start_df["Tool_number"],
    })

    return out


def detect_override_changes_fast(df: pd.DataFrame, col: str, source_file: str) -> pd.DataFrame:
    """
    Detect override changes as events.
    """
    if df.empty or col not in df.columns:
        return pd.DataFrame(columns=[
            "source_file", "type", "timestamp", "old", "new",
            "delta", "execution", "program", "line", "tool"
        ])

    prev = df[col].shift()
    changed = df[col].notna() & prev.notna() & df[col].ne(prev)

    out = df.loc[changed, ["timestamp", "execution", "program", "line", "Tool_number"]].copy()
    out["source_file"] = source_file
    out["type"] = col
    out["old"] = prev.loc[changed].values
    out["new"] = df.loc[changed, col].values
    out["delta"] = out["new"] - out["old"]

    out = out.rename(columns={"Tool_number": "tool"})
    out = out[[
        "source_file", "type", "timestamp", "old", "new", "delta",
        "execution", "program", "line", "tool"
    ]]

    return out.reset_index(drop=True)


def process_folder(data_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    state_parts = []
    override_parts = []

    files = sorted(data_dir.glob("*.jsonl"))

    for i, path in enumerate(files, start=1):
        print(f"[{i}/{len(files)}] Processing {path.name}")

        df = read_jsonl_selected(path, KEEP_COLS)
        if df.empty:
            continue

        df = preprocess(df)
        if df.empty:
            continue

        for state in INTERVENTION_STATES:
            state_parts.append(detect_state_episodes_fast(df, state, path.name))

        for col in OVERRIDE_COLS:
            override_parts.append(detect_override_changes_fast(df, col, path.name))

    state_df = (
        pd.concat(state_parts, ignore_index=True)
        if state_parts else
        pd.DataFrame()
    )
    override_df = (
        pd.concat(override_parts, ignore_index=True)
        if override_parts else
        pd.DataFrame()
    )

    return state_df, override_df


def add_summary_metrics(state_df: pd.DataFrame) -> pd.DataFrame:
    """
    Optional summary by intervention type.
    """
    if state_df.empty:
        return pd.DataFrame()

    summary = state_df.groupby("type")["duration_s"].agg(
        count="count",
        mean_s="mean",
        median_s="median",
        min_s="min",
        max_s="max",
    ).reset_index()

    return summary


if __name__ == "__main__":
    state_df, override_df = process_folder(DATA_DIR)

    state_df.to_csv(STATE_OUTPUT, index=False)
    override_df.to_csv(OVERRIDE_OUTPUT, index=False)

    print()
    print(f"Saved {len(state_df):,} state episodes to {STATE_OUTPUT}")
    print(f"Saved {len(override_df):,} override changes to {OVERRIDE_OUTPUT}")

    summary_df = add_summary_metrics(state_df)
    if not summary_df.empty:
        print("\nState episode summary:")
        print(summary_df.to_string(index=False))