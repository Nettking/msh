"""Generate early tool-wear candidate events from normalized telemetry.

This script is intentionally conservative: it does not claim to measure tool wear
itself. It flags machine/point/channel trend measurements that drift upward from
recent local baselines under comparable synchronized source data.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd

SCRIPT_ROOT = Path(__file__).resolve().parents[2]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from catalog.common.data_loading import iter_records_with_parsed_timestamps


DEFAULT_DATA_DIR = Path("data")
DEFAULT_OUTPUT_NAME = "tool_wear_candidates.csv"
DEFAULT_SUMMARY_NAME = "tool_wear_signal_summary.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate early tool-wear candidate events from telemetry JSONL.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Directory containing MSH JSONL telemetry.")
    parser.add_argument(
        "--source",
        default="observer_phoenix",
        help="Source filter. Use 'all' to include every source with numeric value fields.",
    )
    parser.add_argument("--min-baseline-samples", type=int, default=20, help="Minimum previous samples required before scoring.")
    parser.add_argument("--baseline-window", type=int, default=120, help="Rolling baseline window in samples.")
    parser.add_argument("--slope-window", type=int, default=10, help="Samples between value and previous value used for slope.")
    parser.add_argument("--z-threshold", type=float, default=2.5, help="Minimum positive z-score for candidate samples.")
    parser.add_argument("--min-event-samples", type=int, default=3, help="Minimum candidate samples in an aggregated event.")
    parser.add_argument("--max-gap-minutes", type=float, default=15.0, help="Maximum gap between candidate samples in one event.")
    parser.add_argument("--output", type=Path, default=None, help="Candidate CSV output path.")
    parser.add_argument("--summary-output", type=Path, default=None, help="Signal summary CSV output path.")
    return parser.parse_args()


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


def _default_output_path(file_name: str) -> Path:
    session_dir = _resolve_session_dir()
    target = session_dir / "analyses" / "tool_wear_indicator" / file_name
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def _warn_malformed_json(message: str) -> None:
    print(f"Error parsing line: {message}")


def _warn_invalid_timestamp(file_path: Path, raw_timestamp: object) -> None:
    print(f"Skipping invalid timestamp in {file_path}: {raw_timestamp}")


def _first_present(record: dict[str, Any], names: list[str], default: Any = None) -> Any:
    for name in names:
        value = record.get(name)
        if value is not None and value != "":
            return value
    return default


def _load_signal_frame(data_dir: Path, source_filter: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    source_filter = source_filter.strip()
    for file_path, record in iter_records_with_parsed_timestamps(
        data_dir,
        recursive=True,
        allow_z_suffix=True,
        on_malformed_json=_warn_malformed_json,
        on_invalid_timestamp=_warn_invalid_timestamp,
    ):
        source = str(record.get("source") or "unknown")
        if source_filter.lower() != "all" and source != source_filter:
            continue

        value = pd.to_numeric(_first_present(record, ["value", "Level", "level"]), errors="coerce")
        if pd.isna(value):
            continue

        machine_id = str(_first_present(record, ["machine_id", "machine", "resource"], "unknown")).strip() or "unknown"
        point_id = str(_first_present(record, ["point_id", "PointID", "pointID"], "unknown")).strip() or "unknown"
        channel = str(_first_present(record, ["channel", "Channel"], "unknown")).strip() or "unknown"
        rows.append(
            {
                "timestamp": record["timestamp"],
                "source": source,
                "source_file": file_path.as_posix(),
                "machine_id": machine_id,
                "machine": _first_present(record, ["machine", "machine_name", "Name", "resource"], machine_id),
                "point_id": point_id,
                "point_name": _first_present(record, ["point_name", "Name", "description"], point_id),
                "channel": channel,
                "channel_name": _first_present(record, ["channel_name", "ChannelName"], channel),
                "measurement_type": _first_present(record, ["measurement_type"], "unknown"),
                "unit": _first_present(record, ["unit", "Units", "units"], ""),
                "value": float(value),
                "speed": pd.to_numeric(_first_present(record, ["speed", "Speed", "Srpm"]), errors="coerce"),
                "process": pd.to_numeric(_first_present(record, ["process", "Process"]), errors="coerce"),
                "alarm_info": record.get("alarm_info"),
                "source_record_id": record.get("source_record_id"),
            }
        )

    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["timestamp", "value"])
    return frame.sort_values(["source", "machine_id", "point_id", "channel", "timestamp"]).reset_index(drop=True)


def _score_group(group: pd.DataFrame, *, baseline_window: int, min_baseline_samples: int, slope_window: int) -> pd.DataFrame:
    group = group.sort_values("timestamp").copy()
    rolling = group["value"].rolling(window=max(2, baseline_window), min_periods=max(2, min_baseline_samples))
    group["baseline_mean"] = rolling.mean().shift(1)
    group["baseline_std"] = rolling.std(ddof=0).shift(1)
    group["baseline_std"] = group["baseline_std"].where(group["baseline_std"] > 0)
    group["z_score"] = (group["value"] - group["baseline_mean"]) / group["baseline_std"]

    previous_value = group["value"].shift(max(1, slope_window))
    previous_time = group["timestamp"].shift(max(1, slope_window))
    elapsed_hours = (group["timestamp"] - previous_time).dt.total_seconds() / 3600.0
    group["slope_per_hour"] = (group["value"] - previous_value) / elapsed_hours.where(elapsed_hours > 0)
    group["value_delta"] = group["value"] - group["baseline_mean"]
    return group


def score_signals(
    frame: pd.DataFrame,
    *,
    baseline_window: int,
    min_baseline_samples: int,
    slope_window: int,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    group_cols = ["source", "machine_id", "point_id", "channel"]
    scored = frame.groupby(group_cols, group_keys=False, dropna=False).apply(
        _score_group,
        baseline_window=baseline_window,
        min_baseline_samples=min_baseline_samples,
        slope_window=slope_window,
    )
    return scored.reset_index(drop=True)


def _risk_level(max_z: float, duration_min: float, mean_slope: float) -> str:
    if max_z >= 4.0 or (max_z >= 3.0 and duration_min >= 30 and mean_slope > 0):
        return "high"
    if max_z >= 3.0 or duration_min >= 15:
        return "elevated"
    return "watch"


def _reason(row: pd.Series) -> str:
    unit = f" {row['unit']}" if row.get("unit") else ""
    return (
        f"{row['point_name']} channel {row['channel']} reached z={row['max_z_score']:.2f}; "
        f"last value {row['last_value']:.4g}{unit}, baseline {row['baseline_mean']:.4g}{unit}."
    )


def build_candidate_events(
    scored: pd.DataFrame,
    *,
    z_threshold: float,
    min_event_samples: int,
    max_gap_minutes: float,
) -> pd.DataFrame:
    required = {"z_score", "timestamp", "value", "baseline_mean"}
    if scored.empty or not required.issubset(scored.columns):
        return pd.DataFrame()

    candidate_rows = scored[(scored["z_score"] >= z_threshold) & (scored["value_delta"] > 0)].copy()
    if candidate_rows.empty:
        return pd.DataFrame()

    events: list[dict[str, Any]] = []
    group_cols = ["source", "machine_id", "point_id", "channel"]
    for _, group in candidate_rows.groupby(group_cols, dropna=False):
        group = group.sort_values("timestamp").copy()
        gaps = group["timestamp"].diff().dt.total_seconds().div(60.0).fillna(0)
        group["event_id"] = (gaps > max_gap_minutes).cumsum()
        for _, event in group.groupby("event_id"):
            if len(event) < max(1, min_event_samples):
                continue
            start = event["timestamp"].min()
            end = event["timestamp"].max()
            duration_min = max(0.0, (end - start).total_seconds() / 60.0)
            last = event.iloc[-1]
            max_z = float(event["z_score"].max())
            mean_z = float(event["z_score"].mean())
            mean_slope = float(event["slope_per_hour"].fillna(0).mean())
            event_row = {
                "source": last["source"],
                "machine_id": last["machine_id"],
                "machine": last["machine"],
                "point_id": last["point_id"],
                "point_name": last["point_name"],
                "channel": last["channel"],
                "channel_name": last["channel_name"],
                "measurement_type": last["measurement_type"],
                "unit": last["unit"],
                "event_start": start.isoformat(),
                "event_end": end.isoformat(),
                "duration_min": round(duration_min, 2),
                "samples": int(len(event)),
                "max_z_score": round(max_z, 3),
                "mean_z_score": round(mean_z, 3),
                "baseline_mean": float(last["baseline_mean"]),
                "last_value": float(last["value"]),
                "value_delta": float(last["value_delta"]),
                "slope_per_hour": mean_slope,
                "risk_level": _risk_level(max_z, duration_min, mean_slope),
                "alarm_samples": int(event["alarm_info"].notna().sum()) if "alarm_info" in event else 0,
                "source_record_id": last.get("source_record_id"),
            }
            event_row["reason"] = _reason(pd.Series(event_row))
            events.append(event_row)

    if not events:
        return pd.DataFrame()
    return pd.DataFrame(events).sort_values(["risk_level", "event_start", "machine_id", "point_id", "channel"])


def build_signal_summary(scored: pd.DataFrame) -> pd.DataFrame:
    if scored.empty:
        return pd.DataFrame()
    group_cols = ["source", "machine_id", "machine", "point_id", "point_name", "channel", "channel_name", "unit"]
    summary = (
        scored.groupby(group_cols, dropna=False)
        .agg(
            samples=("value", "size"),
            first_timestamp=("timestamp", "min"),
            last_timestamp=("timestamp", "max"),
            mean_value=("value", "mean"),
            max_value=("value", "max"),
            max_z_score=("z_score", "max"),
            last_value=("value", "last"),
            last_z_score=("z_score", "last"),
        )
        .reset_index()
    )
    summary["first_timestamp"] = summary["first_timestamp"].apply(lambda value: value.isoformat() if pd.notna(value) else "")
    summary["last_timestamp"] = summary["last_timestamp"].apply(lambda value: value.isoformat() if pd.notna(value) else "")
    return summary.sort_values(["max_z_score", "samples"], ascending=[False, False])


def main() -> int:
    args = parse_args()
    output_path = args.output or _default_output_path(DEFAULT_OUTPUT_NAME)
    summary_path = args.summary_output or _default_output_path(DEFAULT_SUMMARY_NAME)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    frame = _load_signal_frame(args.data_dir, args.source)
    if frame.empty:
        pd.DataFrame().to_csv(output_path, index=False)
        pd.DataFrame().to_csv(summary_path, index=False)
        print(f"No numeric telemetry records found for source={args.source!r}. Empty outputs written.")
        print("Tool-wear candidates:", output_path)
        print("Signal summary:", summary_path)
        return 0

    scored = score_signals(
        frame,
        baseline_window=args.baseline_window,
        min_baseline_samples=args.min_baseline_samples,
        slope_window=args.slope_window,
    )
    candidates = build_candidate_events(
        scored,
        z_threshold=args.z_threshold,
        min_event_samples=args.min_event_samples,
        max_gap_minutes=args.max_gap_minutes,
    )
    summary = build_signal_summary(scored)

    candidates.to_csv(output_path, index=False)
    summary.to_csv(summary_path, index=False)
    print(f"Tool-wear candidates generated: {len(candidates)}")
    print("Tool-wear candidates:", output_path)
    print("Signal summary:", summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
