"""
Extract intervention-state episodes and override-change events from MTConnect JSONL telemetry.

This script scans flat JSONL telemetry files, keeps only the columns needed for
intervention analysis, filters to production-relevant rows, and produces two outputs:

1. ``intervention_states.csv``
   - contiguous episodes where ``execution`` is one of the configured
     intervention-related states

2. ``override_changes.csv``
   - row-level events where one of the configured override values changes

The script is intended as a focused extraction utility for later inspection or
analysis, not as a general-purpose telemetry processing pipeline.

Notes
-----
- ``DATA_DIR`` is intentionally environment-specific in this script and may
  require local editing.
- Input files are expected to be top-level ``*.jsonl`` files in ``DATA_DIR``.
- Rows are sorted by ``timestamp`` and ``sequence`` before event extraction.
- Missing or malformed telemetry structure is handled pragmatically rather than
  strictly; the script favors extraction over hard failure.
"""

import json
from pathlib import Path

import pandas as pd

# Environment-specific input folder containing JSONL telemetry files.
DATA_DIR = Path(r"C:\wsl\msh\data")

# Output CSV for contiguous intervention-state episodes.
STATE_OUTPUT = Path("intervention_states.csv")

# Output CSV for row-level override change events.
OVERRIDE_OUTPUT = Path("override_changes.csv")

# Only load the columns required for this extraction task.
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

# Execution states treated as intervention-related episodes.
INTERVENTION_STATES = ["FEED_HOLD", "INTERRUPTED", "PROGRAM_STOPPED"]

# Override columns monitored for change events.
OVERRIDE_COLS = ["Frapidovr", "Fovr", "Sovr"]


def read_jsonl_selected(path: Path, keep_cols: list[str]) -> pd.DataFrame:
    """
    Read a JSONL file while keeping only selected columns.

    Parameters
    ----------
    path : pathlib.Path
        Input JSONL file.
    keep_cols : list[str]
        Columns to extract from each JSON object. Missing keys are recorded as None.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing only the requested columns, sorted by timestamp and
        sequence when data is available.

    Notes
    -----
    - Blank lines are skipped.
    - This function assumes each non-blank line is valid JSON and will raise if
      decoding fails.
    - ``timestamp`` is parsed to datetime with invalid values coerced to NaT.
    """
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
    """
    Filter and normalize telemetry rows before event extraction.

    This step:
    - keeps only rows in ``mode == "AUTOMATIC"``
    - converts override columns to numeric values
    - drops rows without valid timestamps

    Parameters
    ----------
    df : pandas.DataFrame
        Raw extracted telemetry rows.

    Returns
    -------
    pandas.DataFrame
        Preprocessed DataFrame suitable for episode and override-change detection.

    Notes
    -----
    The ``execution`` column is not explicitly required here, because downstream
    functions handle its absence defensively.
    """
    # Keep only production-relevant rows.
    df = df[df["mode"] == "AUTOMATIC"].copy()

    # Normalize numeric override columns.
    for col in OVERRIDE_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove rows without valid timestamps.
    df = df[df["timestamp"].notna()].copy()
    return df.reset_index(drop=True)


def detect_state_episodes_fast(df: pd.DataFrame, state: str, source_file: str) -> pd.DataFrame:
    """
    Detect contiguous episodes of one execution state.

    An episode is defined using row-to-row transitions in the ``execution`` column:

    - start: current row is ``state`` and previous row is not ``state``
    - end: current row is ``state`` and next row is not ``state``

    Parameters
    ----------
    df : pandas.DataFrame
        Preprocessed telemetry rows.
    state : str
        Execution state to extract as an episode type.
    source_file : str
        Source filename recorded in the output.

    Returns
    -------
    pandas.DataFrame
        One row per detected episode with:
        - source file
        - type
        - start/end time
        - duration
        - program
        - start/end line
        - tool

    Notes
    -----
    This logic assumes a reasonably well-formed ordered execution stream.
    If start/end counts do not match, the shorter side is used to avoid failure.
    That protects the extraction pipeline, but may hide malformed state sequences.
    """
    if df.empty or "execution" not in df.columns:
        return pd.DataFrame(
            columns=[
                "source_file",
                "type",
                "start",
                "end",
                "duration_s",
                "program",
                "line_start",
                "line_end",
                "tool",
            ]
        )

    is_state = df["execution"].eq(state)

    starts = is_state & ~is_state.shift(fill_value=False)
    ends = is_state & ~is_state.shift(-1, fill_value=False)

    start_df = df.loc[starts, ["timestamp", "program", "line", "Tool_number"]].copy()
    end_df = df.loc[ends, ["timestamp", "line"]].copy()

    # Reset index so rows align by detected episode order.
    start_df = start_df.reset_index(drop=True)
    end_df = end_df.reset_index(drop=True)

    # In a well-formed sequence these counts should match.
    # If not, trim to the shorter one to avoid crashing.
    n = min(len(start_df), len(end_df))
    start_df = start_df.iloc[:n].copy()
    end_df = end_df.iloc[:n].copy()

    out = pd.DataFrame(
        {
            "source_file": source_file,
            "type": state,
            "start": start_df["timestamp"],
            "end": end_df["timestamp"],
            "duration_s": (end_df["timestamp"] - start_df["timestamp"]).dt.total_seconds(),
            "program": start_df["program"],
            "line_start": start_df["line"],
            "line_end": end_df["line"],
            "tool": start_df["Tool_number"],
        }
    )

    return out


def detect_override_changes_fast(df: pd.DataFrame, col: str, source_file: str) -> pd.DataFrame:
    """
    Detect row-level override change events for one override column.

    A change event is emitted when the current value differs from the previous
    value and both are non-missing.

    Parameters
    ----------
    df : pandas.DataFrame
        Preprocessed telemetry rows.
    col : str
        Override column to inspect.
    source_file : str
        Source filename recorded in the output.

    Returns
    -------
    pandas.DataFrame
        One row per change event with:
        - source file
        - override type
        - timestamp
        - old/new value
        - delta
        - execution/program/line/tool context
    """
    if df.empty or col not in df.columns:
        return pd.DataFrame(
            columns=[
                "source_file",
                "type",
                "timestamp",
                "old",
                "new",
                "delta",
                "execution",
                "program",
                "line",
                "tool",
            ]
        )

    prev = df[col].shift()
    changed = df[col].notna() & prev.notna() & df[col].ne(prev)

    out = df.loc[changed, ["timestamp", "execution", "program", "line", "Tool_number"]].copy()
    out["source_file"] = source_file
    out["type"] = col
    out["old"] = prev.loc[changed].values
    out["new"] = df.loc[changed, col].values
    out["delta"] = out["new"] - out["old"]

    out = out.rename(columns={"Tool_number": "tool"})
    out = out[
        [
            "source_file",
            "type",
            "timestamp",
            "old",
            "new",
            "delta",
            "execution",
            "program",
            "line",
            "tool",
        ]
    ]

    return out.reset_index(drop=True)


def process_folder(data_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process all top-level JSONL files in a folder.

    For each file:
    - read selected columns
    - preprocess rows
    - extract intervention-state episodes
    - extract override-change events

    Parameters
    ----------
    data_dir : pathlib.Path
        Folder containing input JSONL files.

    Returns
    -------
    tuple[pandas.DataFrame, pandas.DataFrame]
        ``(state_df, override_df)`` where:
        - ``state_df`` contains intervention-state episodes
        - ``override_df`` contains override-change events
    """
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

    state_df = pd.concat(state_parts, ignore_index=True) if state_parts else pd.DataFrame()
    override_df = pd.concat(override_parts, ignore_index=True) if override_parts else pd.DataFrame()

    return state_df, override_df


def add_summary_metrics(state_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute a simple summary over extracted intervention-state episodes.

    Parameters
    ----------
    state_df : pandas.DataFrame
        Episode table returned by ``process_folder``.

    Returns
    -------
    pandas.DataFrame
        Summary by intervention type with count and duration statistics.

    Notes
    -----
    This summary is optional and used only for console reporting.
    """
    if state_df.empty:
        return pd.DataFrame()

    summary = (
        state_df.groupby("type")["duration_s"]
        .agg(
            count="count",
            mean_s="mean",
            median_s="median",
            min_s="min",
            max_s="max",
        )
        .reset_index()
    )

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