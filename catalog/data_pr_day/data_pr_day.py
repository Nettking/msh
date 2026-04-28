"""
Generate canonical machine/day CSV data from JSONL telemetry.

Behavior:
- Reads JSONL files only from the top level of ``data/`` (non-recursive).
- Skips malformed JSON lines with visible warnings.
- Requires ``timestamp`` and ``machine`` fields.

Output contract:
- Canonical CSV path:
  ``results/workflows/<session>/analyses/data_pr_day/machine_day_summary.csv``
- Canonical CSV columns (minimum): ``date``, ``machine``, ``value``
"""

import os
import sys
from pathlib import Path

import pandas as pd

SCRIPT_ROOT = Path(__file__).resolve().parents[2]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from catalog.common.data_loading import iter_records_with_parsed_timestamps

# Folder containing input JSONL files.
DATA_DIR = "data"

def _resolve_session_dir() -> Path:
    from_env = os.getenv("MSH_SESSION_DIR", "").strip()
    if from_env:
        return Path(from_env).expanduser().resolve()

    cwd = Path.cwd().resolve()
    parts = cwd.parts
    if "workflows" in parts:
        idx = parts.index("workflows")
        if idx + 1 < len(parts):
            return Path(*parts[: idx + 2])
    return cwd


def _resolve_machine_day_output_csv() -> Path:
    session_dir = _resolve_session_dir()
    target = session_dir / "analyses" / "data_pr_day" / "machine_day_summary.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def _build_machine_day_summary(frame: pd.DataFrame) -> pd.DataFrame:
    summary = (
        frame.assign(date=frame["timestamp"].dt.date.astype(str), machine=frame["machine"].astype("string"))
        .groupby(["date", "machine"], dropna=False)
        .size()
        .reset_index(name="value")
        .sort_values(["date", "machine"])
    )
    summary["machine"] = summary["machine"].fillna("unknown").astype(str)
    summary["value"] = summary["value"].astype(int)
    return summary[["date", "machine", "value"]]


def _warn_malformed_json(message: str) -> None:
    """
    Report malformed JSONL input lines during record loading.

    The shared loader skips malformed lines; this callback ensures they are still
    visible to the user during script execution.
    """
    print(f"Error parsing line: {message}")


def _warn_invalid_timestamp(file_path: Path, raw_timestamp: object) -> None:
    """
    Report records whose timestamp cannot be parsed.
    """
    print(f"Error parsing line in {file_path.name}: Invalid isoformat string: {raw_timestamp}")


records = []

# Read top-level JSONL files only. This preserves the script's original behavior
# and avoids unexpectedly traversing nested directories.
for _, entry in iter_records_with_parsed_timestamps(
    DATA_DIR,
    recursive=False,
    allow_z_suffix=True,
    on_malformed_json=_warn_malformed_json,
    on_invalid_timestamp=_warn_invalid_timestamp,
):
    records.append(entry)

if not records:
    raise SystemExit("No valid records found in data folder.")

df = pd.DataFrame(records)

# These columns are the minimum needed for grouping logic.
required_cols = {"timestamp", "machine"}
if not required_cols.issubset(df.columns):
    raise ValueError(f"Missing required columns: {required_cols - set(df.columns)}")

# Group by calendar day rather than full timestamp.
df["date"] = df["timestamp"].dt.date

summary = _build_machine_day_summary(df)
summary_path = _resolve_machine_day_output_csv()
summary.to_csv(summary_path, index=False)

print("Machine/day summary generated at:", summary_path)
