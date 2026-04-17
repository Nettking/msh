"""Shared helpers for exporting and loading playback-ready telemetry timelines."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from catalog.common.data_loading import load_jsonl_dataframe
from catalog.common.state_inference import StateInferenceConfig, infer_states_for_machine, rows_to_state_intervals
from catalog.common.telemetry_prep import (
    add_date_column,
    add_machine_id_column,
    find_machine_column,
    prepare_timestamp_column,
    replace_unavailable,
    to_numeric,
)

TIMELINE_COLUMNS = [
    "timestamp",
    "machine_id",
    "date",
    "state",
    "active",
    "dense_idle",
    "intervention_candidate",
    "stopped",
    "event_score",
    "fired_rules",
    "Srpm",
    "Sload",
    "Sovr",
    "Fovr",
    "Frapidovr",
    "execution",
    "mode",
    "program",
]

SIGNAL_COLUMNS = ["Srpm", "Sload", "Sovr", "Fovr", "Frapidovr"]
CONTEXT_COLUMNS = ["execution", "mode", "program"]
STOPPED_STATES = {"STOPPED", "PROGRAM_STOPPED", "INTERRUPTED", "STOP"}


def _normalize_machine_and_time(df: pd.DataFrame) -> pd.DataFrame:
    prepared = prepare_timestamp_column(df, time_col="timestamp", drop_invalid=True, sort=True)
    machine_col = find_machine_column(prepared, candidates=("machine_id", "machine", "resource"))
    if machine_col is None:
        prepared["machine_fallback"] = "unknown"
        machine_col = "machine_fallback"

    prepared = add_machine_id_column(prepared, source_col=machine_col, target_col="machine_id")
    prepared = add_date_column(prepared, time_col="timestamp", target_col="date")

    for col in SIGNAL_COLUMNS:
        if col in prepared.columns:
            prepared[col] = to_numeric(prepared[col])

    for col in CONTEXT_COLUMNS:
        if col in prepared.columns:
            prepared[col] = replace_unavailable(prepared[col]).astype("string")

    return prepared


def _ensure_timeline_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    for col in TIMELINE_COLUMNS:
        if col not in normalized.columns:
            normalized[col] = pd.NA
    normalized = normalized[TIMELINE_COLUMNS].copy()
    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"], errors="coerce")
    normalized = normalized[normalized["timestamp"].notna()].sort_values("timestamp")
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce").dt.date.fillna(
        normalized["timestamp"].dt.date
    )

    for col in ("active", "dense_idle", "intervention_candidate", "stopped"):
        normalized[col] = normalized[col].fillna(False).astype(bool)

    for col in SIGNAL_COLUMNS + ["event_score"]:
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce")

    normalized["fired_rules"] = normalized["fired_rules"].fillna("").astype(str)
    for col in CONTEXT_COLUMNS + ["state", "machine_id"]:
        normalized[col] = normalized[col].astype("string")

    return normalized.reset_index(drop=True)


def infer_timeline_rows(
    df: pd.DataFrame,
    *,
    machine_id: str,
    day,
    config: StateInferenceConfig = StateInferenceConfig(),
) -> pd.DataFrame:
    """Infer playback timeline rows for one machine/day slice."""
    prepared = _normalize_machine_and_time(df)
    selected = prepared[(prepared["machine_id"] == str(machine_id)) & (prepared["date"] == day)].copy()
    if selected.empty:
        return pd.DataFrame(columns=TIMELINE_COLUMNS)

    inferred = infer_states_for_machine(selected, config=config)

    if "execution" in inferred.columns:
        inferred["stopped"] = inferred["execution"].astype("string").str.upper().isin(STOPPED_STATES)
    else:
        inferred["stopped"] = False

    return _ensure_timeline_columns(inferred)


def build_timeline_rows_export(
    source_df: pd.DataFrame,
    *,
    config: StateInferenceConfig = StateInferenceConfig(),
) -> pd.DataFrame:
    """Infer playback timeline rows for all machine/day slices in a dataframe."""
    prepared = _normalize_machine_and_time(source_df)
    if prepared.empty:
        return pd.DataFrame(columns=TIMELINE_COLUMNS)

    frames: list[pd.DataFrame] = []
    for _, selected in prepared.groupby(["date", "machine_id"], sort=True):
        inferred = infer_states_for_machine(selected, config=config)
        if "execution" in inferred.columns:
            inferred["stopped"] = inferred["execution"].astype("string").str.upper().isin(STOPPED_STATES)
        else:
            inferred["stopped"] = False
        rows = _ensure_timeline_columns(inferred)
        if not rows.empty:
            frames.append(rows)

    if not frames:
        return pd.DataFrame(columns=TIMELINE_COLUMNS)

    merged = pd.concat(frames, ignore_index=True)
    return _ensure_timeline_columns(merged).sort_values(["date", "machine_id", "timestamp"]).reset_index(drop=True)


def build_state_interval_export(
    timeline_rows: pd.DataFrame,
    *,
    merge_gap_sec: float = 30.0,
) -> pd.DataFrame:
    """Build interval-level state export from row-level timeline data."""
    rows = _ensure_timeline_columns(timeline_rows)
    if rows.empty:
        return pd.DataFrame(columns=["machine_id", "date", "state", "start", "end", "duration_sec", "n_points"])

    intervals = rows_to_state_intervals(rows, merge_gap_sec=merge_gap_sec)
    if "stopped" in rows.columns:
        stopped_windows = rows_to_state_intervals(
            rows.assign(state=rows["stopped"].map({True: "stopped", False: "not_stopped"})),
            merge_gap_sec=merge_gap_sec,
        )
        stopped_windows = stopped_windows[stopped_windows["state"] == "stopped"]
        if not stopped_windows.empty:
            stopped_windows = stopped_windows.assign(state="stopped")
            intervals = pd.concat([intervals, stopped_windows], ignore_index=True)

    intervals = intervals.sort_values("start").reset_index(drop=True)
    return intervals


def load_timeline_export(path: str | Path) -> pd.DataFrame:
    """Load a timeline export from CSV/Parquet/JSONL/JSON and normalize schema."""
    source = Path(path)
    suffix = source.suffix.lower()

    if suffix == ".csv":
        df = pd.read_csv(source)
    elif suffix in {".parquet", ".pq"}:
        df = pd.read_parquet(source)
    elif suffix == ".jsonl":
        df = load_jsonl_dataframe(source)
    elif suffix == ".json":
        df = pd.read_json(source)
    else:
        raise ValueError(f"Unsupported timeline file extension: {suffix}")

    return _ensure_timeline_columns(df)


def export_timeline_for_machine_day(
    source_df: pd.DataFrame,
    *,
    machine_id: str,
    day,
    output_path: str | Path,
) -> Path:
    """Infer and export timeline rows for one machine/day to CSV."""
    rows = infer_timeline_rows(source_df, machine_id=machine_id, day=day)
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    rows.to_csv(target, index=False)
    return target


def export_timeline_rows(
    source_df: pd.DataFrame,
    *,
    output_path: str | Path,
    config: StateInferenceConfig = StateInferenceConfig(),
) -> Path:
    """Infer and export timeline rows for all machine/day slices to CSV."""
    rows = build_timeline_rows_export(source_df, config=config)
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    rows.to_csv(target, index=False)
    return target
