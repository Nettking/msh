from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from catalog.common.artifact_registry import read_raw_table
from catalog.common.artifact_registry import read_table_columns
from catalog.common.timeline_exports import build_state_interval_export

REQUIRED_PLAYBACK_COLUMNS = {"timestamp", "machine_id", "state"}
DEFAULT_LIVE_SIGNAL_COLUMNS = ["Srpm", "Sload", "Sovr", "Fovr", "Frapidovr"]


@dataclass
class PlaybackValidation:
    is_valid: bool
    reason: str = ""


def _has_non_empty_values(series: pd.Series) -> bool:
    cleaned = series.astype("string").str.strip()
    return cleaned.replace("", pd.NA).notna().any()



def validate_playback_source(path: str) -> PlaybackValidation:
    columns, load_error = read_table_columns(path)
    if load_error:
        return PlaybackValidation(False, f"Unable to inspect source columns: {load_error}")
    missing = sorted(REQUIRED_PLAYBACK_COLUMNS.difference(columns))
    if missing:
        return PlaybackValidation(False, f"Missing required source columns: {', '.join(missing)}")
    return PlaybackValidation(True, "")


def load_playback_frame(path: str) -> tuple[pd.DataFrame | None, str | None]:
    try:
        frame = read_raw_table(path)
    except Exception as exc:  # noqa: BLE001
        return None, f"Could not load '{path}': {exc}"
    if not isinstance(frame, pd.DataFrame):
        return None, f"Could not load '{path}': source did not produce a table."
    return frame, None


def prepare_playback_frame(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    frame["machine_id"] = frame["machine_id"].astype("string").str.strip()
    frame["state"] = frame["state"].astype("string").str.strip()
    frame = frame.dropna(subset=["timestamp", "machine_id", "state"])
    frame = frame[(frame["machine_id"] != "") & (frame["state"] != "")]
    frame["day"] = frame["timestamp"].dt.date.astype(str)
    return frame.reset_index(drop=True)


def validate_playback_frame(df: pd.DataFrame) -> PlaybackValidation:
    missing = sorted(REQUIRED_PLAYBACK_COLUMNS.difference(df.columns))
    if missing:
        return PlaybackValidation(False, f"Missing required source columns: {', '.join(missing)}")

    if not pd.to_datetime(df["timestamp"], errors="coerce").notna().any():
        return PlaybackValidation(False, "'timestamp' has no parseable values.")
    if not _has_non_empty_values(df["machine_id"]):
        return PlaybackValidation(False, "'machine_id' has no non-empty values.")
    if not _has_non_empty_values(df["state"]):
        return PlaybackValidation(False, "'state' has no non-empty values.")
    return PlaybackValidation(True, "")


def playback_subset(df: pd.DataFrame, machine_id: str, day: str) -> pd.DataFrame:
    base = prepare_playback_frame(df)
    rows = base[(base["machine_id"] == str(machine_id)) & (base["day"] == str(day))]
    return rows.sort_values("timestamp").reset_index(drop=True)


def playback_context(df: pd.DataFrame) -> dict:
    frame = prepare_playback_frame(df)
    machines = sorted(frame["machine_id"].dropna().unique().tolist())
    days = sorted(frame["day"].dropna().unique().tolist())
    return {"machines": machines, "days": days}


def playback_days_by_machine(df: pd.DataFrame) -> dict[str, list[str]]:
    frame = prepare_playback_frame(df)
    grouped = frame.groupby("machine_id", dropna=True)["day"]
    return {
        str(machine): sorted(series.dropna().unique().tolist())
        for machine, series in grouped
        if str(machine).strip()
    }


def playback_day_counts_by_machine(df: pd.DataFrame) -> dict[str, dict[str, int]]:
    frame = prepare_playback_frame(df)
    grouped = frame.groupby(["machine_id", "day"], dropna=True).size()
    day_counts: dict[str, dict[str, int]] = {}
    for (machine_id, day), count in grouped.items():
        machine_key = str(machine_id).strip()
        day_key = str(day).strip()
        if not machine_key or not day_key:
            continue
        day_counts.setdefault(machine_key, {})[day_key] = int(count)

    for machine_id in list(day_counts.keys()):
        day_counts[machine_id] = {
            day: day_counts[machine_id][day]
            for day in sorted(day_counts[machine_id].keys())
        }
    return day_counts


def interval_rows(rows: pd.DataFrame) -> list[dict]:
    if rows.empty:
        return []
    intervals = build_state_interval_export(rows)
    out = []
    for rec in intervals.to_dict("records"):
        out.append({
            "start": pd.to_datetime(rec["start"]).isoformat(),
            "end": pd.to_datetime(rec["end"]).isoformat(),
            "state": str(rec.get("state", "unknown")),
        })
    return out


def summarize_intervals(intervals: list[dict]) -> dict:
    totals: dict[str, float] = {}
    table: list[dict] = []
    for item in intervals:
        start = pd.to_datetime(item["start"], errors="coerce")
        end = pd.to_datetime(item["end"], errors="coerce")
        if pd.isna(start) or pd.isna(end):
            continue
        duration = max((end - start).total_seconds(), 0.0)
        state = str(item.get("state", "unknown"))
        totals[state] = totals.get(state, 0.0) + duration
        table.append({
            "state": state,
            "start": start.strftime("%Y-%m-%d %H:%M:%S"),
            "end": end.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_sec": round(duration, 3),
        })
    totals_rows = [{"state": k, "duration_sec": round(v, 3)} for k, v in sorted(totals.items(), key=lambda kv: kv[1], reverse=True)]
    return {"totals": totals_rows, "table": table}


def playback_field_groups(columns: list[str]) -> dict[str, list[str]]:
    lowered_to_original = {column.lower(): column for column in columns}
    grouped: dict[str, list[str]] = {
        "Signals": [],
        "State/context": [],
        "Detection/diagnostics": [],
        "Other fields": [],
    }

    signal_priority = [
        "srpm",
        "sload",
        "sovr",
        "fovr",
        "frapidovr",
        "xabs",
        "yabs",
        "zabs",
        "fact",
        "fcmd",
    ]
    state_priority = [
        "execution",
        "mode",
        "program",
        "tool_number",
        "tool_group",
        "state",
        "active",
        "dense_idle",
        "idle",
        "stopped",
    ]

    used: set[str] = set()
    for key in signal_priority:
        column = lowered_to_original.get(key)
        if column and column not in used:
            grouped["Signals"].append(column)
            used.add(column)

    for key in state_priority:
        column = lowered_to_original.get(key)
        if column and column not in used:
            grouped["State/context"].append(column)
            used.add(column)

    for column in columns:
        if column in used:
            continue
        normalized = column.lower()
        if any(token in normalized for token in ("score", "rule", "candidate", "anomaly", "warning", "stop")):
            grouped["Detection/diagnostics"].append(column)
            used.add(column)

    for column in columns:
        if column in used:
            continue
        normalized = column.lower()
        if any(token in normalized for token in ("rpm", "load", "ovr", "abs", "cmd", "act", "axis", "feed", "speed", "temp", "pressure", "power", "torque")):
            grouped["Signals"].append(column)
            used.add(column)
            continue
        if any(token in normalized for token in ("execution", "mode", "program", "tool", "state", "active", "idle", "running", "stopped", "status")):
            grouped["State/context"].append(column)
            used.add(column)

    grouped["Other fields"] = [column for column in columns if column not in used]
    return grouped


def default_live_signal_columns(df: pd.DataFrame) -> list[str]:
    selected: list[str] = []
    for column in DEFAULT_LIVE_SIGNAL_COLUMNS:
        if column not in df.columns:
            continue
        numeric_series = pd.to_numeric(df[column], errors="coerce")
        if numeric_series.notna().any():
            selected.append(column)
    return selected
