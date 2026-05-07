"""UI-facing validation and shaping helpers for playback timeline artifacts.

Playback views consume already-derived timeline tables rather than raw JSONL.
The helpers here enforce the minimal table contract, normalize timestamps and
machine/state fields, and create display-oriented subsets/resampled rows without
changing the underlying export files.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from catalog.common.artifact_registry import read_raw_table
from catalog.common.artifact_registry import read_table_columns
from catalog.common.timeline_exports import build_state_interval_export

REQUIRED_PLAYBACK_COLUMNS = {"timestamp", "machine_id", "state"}
DEFAULT_LIVE_SIGNAL_COLUMNS = ["Srpm", "Sload", "Sovr", "Fovr", "Frapidovr"]
PLAYBACK_TICK_FREQUENCY = "200ms"
DEFAULT_FALLBACK_PLAYBACK_DELAY_SECONDS = 0.2
DEFAULT_MAX_PLAYBACK_DELAY_SECONDS = 5.0


@dataclass
class PlaybackValidation:
    """Validation result for playback source/file contract checks."""

    is_valid: bool
    reason: str = ""


def _workflow_session_dir_for_artifact(path: str) -> Path | None:
    """Return the workflow session directory for an artifact path, when present."""
    artifact_path = Path(path)
    lowered = [part.lower() for part in artifact_path.parts]
    for idx, part in enumerate(lowered):
        if part != "workflows":
            continue
        if idx + 1 >= len(artifact_path.parts):
            return None
        return Path(*artifact_path.parts[: idx + 2])
    return None


def _session_runtime_namespace(session_dir: Path) -> str:
    """Read a session runtime namespace, defaulting older metadata to ``default``."""
    for file_name in ("session_state.json", "session.json"):
        metadata_path = session_dir / file_name
        if not metadata_path.exists():
            continue
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return "default"
        runtime_payload = metadata.get("runtime") if isinstance(metadata.get("runtime"), dict) else {}
        return str(runtime_payload.get("runtime_namespace") or "default")
    return "default"


def filter_playback_artifacts_for_runtime(
    artifacts: list[dict],
    runtime_state: dict | None,
    *,
    selected_path: str = "",
    logger=None,
) -> list[dict]:
    """Hide playback exports that do not belong to the active clean runtime.

    Workflow artifacts are always gated by ``runtime.runtime_namespace`` in their
    session metadata. Clean startup also hides playback-compatible files outside
    ``results/workflows`` from the automatic `/playback` list, because those files
    have no session namespace to prove they belong to the new runtime. A
    non-workflow artifact can still be loaded when explicitly requested by path
    from manual exploration.
    """
    state = runtime_state or {}
    active_namespace = str(state.get("active_runtime_namespace") or "default")
    startup_mode = str(state.get("startup_mode") or "")
    is_clean_startup = startup_mode == "start_clean"
    visible: list[dict] = []
    ignored_workflow: list[str] = []
    ignored_non_workflow: list[str] = []

    for artifact in artifacts:
        artifact_path = str(artifact.get("path") or "")
        session_dir = _workflow_session_dir_for_artifact(artifact_path)
        if session_dir is None:
            if is_clean_startup and artifact_path != selected_path:
                ignored_non_workflow.append(artifact_path)
                continue
            visible.append(artifact)
            continue
        artifact_namespace = _session_runtime_namespace(session_dir)
        if artifact_namespace == active_namespace:
            visible.append(artifact)
        else:
            ignored_workflow.append(artifact_path)

    if ignored_workflow and logger is not None:
        logger.info(
            "Playback runtime filter ignored %d stale workflow playback export(s) outside active runtime namespace '%s': %s",
            len(ignored_workflow),
            active_namespace,
            ", ".join(ignored_workflow[:5]) + (" ..." if len(ignored_workflow) > 5 else ""),
        )
    if ignored_non_workflow and logger is not None:
        logger.info(
            "Playback runtime filter ignored %d non-workflow playback export(s) during clean startup because they have no active runtime namespace: %s",
            len(ignored_non_workflow),
            ", ".join(ignored_non_workflow[:5]) + (" ..." if len(ignored_non_workflow) > 5 else ""),
        )
    return visible


def compute_playback_delay(
    previous_timestamp,
    current_timestamp,
    speed: float,
    fallback_delay: float = DEFAULT_FALLBACK_PLAYBACK_DELAY_SECONDS,
    max_delay: float = DEFAULT_MAX_PLAYBACK_DELAY_SECONDS,
) -> float:
    """Compute a bounded client delay between telemetry samples.

    Bad timestamps, non-positive deltas, or invalid speeds fall back to a short
    safe delay so playback remains usable instead of stalling the browser.
    """
    previous = pd.to_datetime(previous_timestamp, errors="coerce", utc=True)
    current = pd.to_datetime(current_timestamp, errors="coerce", utc=True)
    safe_fallback = fallback_delay if pd.notna(fallback_delay) and float(fallback_delay) > 0 else 0.05
    safe_max_delay = max_delay if pd.notna(max_delay) and float(max_delay) > 0 else safe_fallback
    safe_speed = float(speed) if pd.notna(speed) and float(speed) > 0 else 1.0

    if pd.isna(previous) or pd.isna(current):
        return min(safe_fallback, safe_max_delay)

    delta_seconds = (current - previous).total_seconds()
    if delta_seconds <= 0:
        return min(safe_fallback, safe_max_delay)

    scaled = delta_seconds / safe_speed
    return max(0.0, min(scaled, safe_max_delay))


def _has_non_empty_values(series: pd.Series) -> bool:
    cleaned = series.astype("string").str.strip()
    return cleaned.replace("", pd.NA).notna().any()


def validate_playback_source(path: str) -> PlaybackValidation:
    """Validate a playback file by inspecting columns before loading all rows."""
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
    """Normalize a playback table to rows with timestamp, machine, state, and day.

    Candidate-event-only legacy exports are interpreted as intervention flags
    when they use ``state == intervention_candidate`` and do not already include
    an explicit flag column.
    """
    frame = df.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    frame["machine_id"] = frame["machine_id"].astype("string").str.strip()
    frame["state"] = frame["state"].astype("string").str.strip()
    if "intervention_candidate" not in frame.columns:
        frame["intervention_candidate"] = frame["state"].str.lower().eq("intervention_candidate")
    frame = frame.dropna(subset=["timestamp", "machine_id", "state"])
    frame = frame[(frame["machine_id"] != "") & (frame["state"] != "")]
    frame["day"] = frame["timestamp"].dt.date.astype(str)
    return frame.reset_index(drop=True)


def validate_playback_frame(df: pd.DataFrame) -> PlaybackValidation:
    """Validate loaded playback rows against the minimal UI contract."""
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
    """Return source playback rows for one machine/day with duplicate timestamps collapsed."""
    base = prepare_playback_frame(df)
    rows = base[(base["machine_id"] == str(machine_id)) & (base["day"] == str(day))]
    if rows.empty:
        return rows.reset_index(drop=True)
    ordered = rows.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").copy()
    ordered["source_timestamp"] = ordered["timestamp"]
    ordered["is_synthetic_tick"] = False
    return ordered.reset_index(drop=True)


def resample_playback_timeline(df: pd.DataFrame, frequency: str = PLAYBACK_TICK_FREQUENCY) -> pd.DataFrame:
    """Build a regular playback tick grid using last-observation-carried-forward.

    Synthetic ticks are marked so the UI can distinguish actual telemetry rows
    from display interpolation.
    """
    if df.empty:
        return df.copy()

    frame = prepare_playback_frame(df)
    if frame.empty:
        return frame

    resampled_parts: list[pd.DataFrame] = []
    for machine_id, machine_rows in frame.groupby("machine_id", dropna=False):
        if pd.isna(machine_id) or str(machine_id).strip() == "":
            continue
        ordered = machine_rows.sort_values("timestamp").copy()
        ordered = ordered.drop_duplicates(subset=["timestamp"], keep="last")
        if ordered.empty:
            continue

        grid_start = ordered["timestamp"].min()
        grid_end = ordered["timestamp"].max()
        # merge_asof preserves the most recent real state for each synthetic UI
        # tick; this is display interpolation, not a replacement for raw samples.
        timeline_grid = pd.DataFrame(
            {
                "timestamp": pd.date_range(start=grid_start, end=grid_end, freq=frequency),
                "machine_id": str(machine_id),
            }
        )
        source_rows = ordered.rename(columns={"timestamp": "source_timestamp"})
        timeline_grid["machine_id"] = timeline_grid["machine_id"].astype("string")
        source_rows["machine_id"] = source_rows["machine_id"].astype("string")
        merged = pd.merge_asof(
            timeline_grid.sort_values("timestamp"),
            source_rows.sort_values("source_timestamp"),
            left_on="timestamp",
            right_on="source_timestamp",
            by="machine_id",
            direction="backward",
        )
        merged = merged.dropna(subset=["source_timestamp", "state"])
        if merged.empty:
            continue
        merged["is_synthetic_tick"] = merged["timestamp"] != merged["source_timestamp"]
        merged["day"] = merged["timestamp"].dt.date.astype(str)
        resampled_parts.append(merged)

    if not resampled_parts:
        return frame.iloc[0:0].copy()

    return pd.concat(resampled_parts, ignore_index=True).sort_values(["machine_id", "timestamp"]).reset_index(drop=True)


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
    """Convert selected playback rows into state intervals for summary tables."""
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
    """Group arbitrary export columns into UI sections using naming heuristics."""
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
