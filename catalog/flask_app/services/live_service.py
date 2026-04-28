from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from catalog.common.state_inference import extract_intervention_candidates, infer_states_for_machine

from .catalog_service import ArtifactCatalog

DEFAULT_MACHINES = ("QuickTurn", "IG500", "VTC")
TIMESTAMP_FIELDS = ("timestamp", "source_timestamp")
LIVE_COLUMNS = (
    "execution",
    "mode",
    "program",
    "Tool_number",
    "Tool_group",
    "Srpm",
    "Sload",
    "Sovr",
    "Fovr",
    "Frapidovr",
    "Xabs",
    "Yabs",
    "Zabs",
)
NUMERIC_HINT_COLUMNS = ("Srpm", "Sload", "Sovr", "Fovr", "Frapidovr", "Xabs", "Yabs", "Zabs")


@dataclass(frozen=True)
class LiveSnapshot:
    generated_at_iso: str
    stale_after_seconds: int
    context_note: str
    machines: list[dict[str, Any]]
    candidate_events: list[dict[str, Any]]


@dataclass
class _CacheEntry:
    value: LiveSnapshot
    built_at_mono: float
    signature: tuple[Any, ...]


class LiveTelemetryService:
    def __init__(
        self,
        *,
        refresh_ttl_seconds: float = 2.0,
        stale_after_seconds: int = 15,
        rows_per_machine: int = 300,
        max_recent_artifacts: int = 12,
    ) -> None:
        self.refresh_ttl_seconds = float(refresh_ttl_seconds)
        self.stale_after_seconds = int(stale_after_seconds)
        self.rows_per_machine = int(rows_per_machine)
        self.max_recent_artifacts = int(max_recent_artifacts)
        self._lock = threading.Lock()
        self._cache: _CacheEntry | None = None

    def snapshot(self, catalog: ArtifactCatalog) -> LiveSnapshot:
        scan = catalog.ensure_scanned()
        source_artifacts = [
            item
            for item in scan.artifacts
            if item.get("category") == "source_data"
            and item.get("status") == "ready"
            and str(item.get("path", "")).lower().endswith(".jsonl")
        ]
        signature = (
            float(scan.scanned_at_epoch),
            tuple((item.get("path"), item.get("signature")) for item in source_artifacts),
            self.stale_after_seconds,
            self.rows_per_machine,
            self.max_recent_artifacts,
        )
        now_mono = time.monotonic()
        with self._lock:
            cached = self._cache
            if cached and cached.signature == signature and (now_mono - cached.built_at_mono) <= self.refresh_ttl_seconds:
                return cached.value

        rebuilt = self._build_snapshot(source_artifacts)
        with self._lock:
            self._cache = _CacheEntry(value=rebuilt, built_at_mono=time.monotonic(), signature=signature)
        return rebuilt

    def _build_snapshot(self, source_artifacts: list[dict[str, Any]]) -> LiveSnapshot:
        now = datetime.now(timezone.utc)
        recent_rows_by_machine = _recent_rows_by_machine(
            source_artifacts,
            rows_per_machine=self.rows_per_machine,
            max_recent_artifacts=self.max_recent_artifacts,
        )

        known_machines = sorted(set(DEFAULT_MACHINES) | set(_path_machine_hints(source_artifacts)) | set(recent_rows_by_machine.keys()))
        machine_cards: list[dict[str, Any]] = []
        candidate_events: list[dict[str, Any]] = []

        for machine_name in known_machines:
            rows = recent_rows_by_machine.get(machine_name, [])
            if not rows:
                machine_cards.append(
                    {
                        "machine": machine_name,
                        "freshness": "no data",
                        "last_seen_iso": "-",
                        "last_seen_clock": "-",
                        "age_seconds": None,
                        "inferred_state": "unknown",
                        "values": {"machine": machine_name, **{col: "-" for col in LIVE_COLUMNS}},
                    }
                )
                continue

            prepared = _prepare_machine_frame(rows, machine_name=machine_name)
            latest_row = prepared.iloc[-1]
            timestamp = latest_row.get("timestamp")
            age_seconds = None if pd.isna(timestamp) else max((now - timestamp.to_pydatetime()).total_seconds(), 0.0)
            freshness = "fresh"
            if age_seconds is None or age_seconds > self.stale_after_seconds:
                freshness = "stale"

            inferred = infer_states_for_machine(prepared)
            inferred_latest = inferred.iloc[-1] if not inferred.empty else latest_row
            inferred_state = _normalized_live_state(inferred_latest)

            values = {"machine": machine_name}
            for col in LIVE_COLUMNS:
                values[col] = _safe_value(latest_row.get(col))

            machine_cards.append(
                {
                    "machine": machine_name,
                    "freshness": freshness,
                    "last_seen_iso": timestamp.isoformat() if pd.notna(timestamp) else "-",
                    "last_seen_clock": timestamp.strftime("%H:%M:%S") if pd.notna(timestamp) else "-",
                    "age_seconds": None if age_seconds is None else int(age_seconds),
                    "inferred_state": inferred_state,
                    "values": values,
                }
            )

            candidates = extract_intervention_candidates(inferred)
            if not candidates.empty:
                for _, event_row in candidates.tail(8).iterrows():
                    candidate_events.append(
                        {
                            "timestamp": event_row.get("timestamp"),
                            "machine": machine_name,
                            "event_score": int(event_row.get("event_score") or 0),
                            "fired_rules": _safe_value(event_row.get("fired_rules")),
                            "execution": _safe_value(event_row.get("execution")),
                            "mode": _safe_value(event_row.get("mode")),
                            "program": _safe_value(event_row.get("program")),
                            "Srpm": _safe_value(event_row.get("Srpm")),
                            "Sload": _safe_value(event_row.get("Sload")),
                            "Sovr": _safe_value(event_row.get("Sovr")),
                            "Fovr": _safe_value(event_row.get("Fovr")),
                        }
                    )

        candidate_events = sorted(
            candidate_events,
            key=lambda item: item.get("timestamp") if isinstance(item.get("timestamp"), pd.Timestamp) else pd.Timestamp.min,
            reverse=True,
        )[:40]
        for item in candidate_events:
            ts = item.get("timestamp")
            item["timestamp"] = ts.isoformat() if isinstance(ts, pd.Timestamp) else "-"

        return LiveSnapshot(
            generated_at_iso=now.isoformat(),
            stale_after_seconds=self.stale_after_seconds,
            context_note=(
                "Latest recorded telemetry and live inference from recent recorded-stream tails. "
                "This reflects recorder freshness, not a direct machine connection check."
            ),
            machines=machine_cards,
            candidate_events=candidate_events,
        )


def _normalized_live_state(row: pd.Series) -> str:
    raw_state = str(row.get("state") or "").strip().lower()
    execution = str(row.get("execution") or "").strip().upper()

    if raw_state == "intervention_candidate":
        return "intervention_candidate"
    if raw_state == "active":
        return "running/active"
    if "STOP" in execution:
        return "stopped"
    if raw_state in {"dense_idle", "idle"}:
        return "idle"
    return "unknown"


def _prepare_machine_frame(rows: list[dict[str, Any]], *, machine_name: str) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if "timestamp" not in frame.columns and "source_timestamp" in frame.columns:
        frame["timestamp"] = frame["source_timestamp"]
    frame["timestamp"] = pd.to_datetime(frame.get("timestamp"), errors="coerce", utc=True)
    frame = frame.dropna(subset=["timestamp"]).sort_values("timestamp")

    for col in NUMERIC_HINT_COLUMNS:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")

    for col in ("execution", "mode", "program"):
        if col not in frame.columns:
            frame[col] = ""
        frame[col] = frame[col].astype("string").fillna("").astype(str)

    frame["machine_id"] = machine_name
    frame["date"] = frame["timestamp"].dt.strftime("%Y-%m-%d")
    return frame


def _path_machine_hints(source_artifacts: list[dict[str, Any]]) -> set[str]:
    hints: set[str] = set()
    for artifact in source_artifacts:
        path = Path(str(artifact.get("path") or ""))
        if len(path.parts) >= 3 and path.parts[-3].lower() == "data":
            hints.add(path.parts[-2])
    return {item for item in hints if item}


def _recent_rows_by_machine(
    source_artifacts: list[dict[str, Any]],
    *,
    rows_per_machine: int,
    max_recent_artifacts: int,
) -> dict[str, list[dict[str, Any]]]:
    by_machine: dict[str, list[dict[str, Any]]] = {}
    sorted_artifacts = sorted(
        source_artifacts,
        key=lambda item: (str(item.get("modified_at") or ""), str(item.get("path") or "")),
        reverse=True,
    )[:max_recent_artifacts]

    for artifact in sorted_artifacts:
        path = Path(str(artifact.get("path") or ""))
        if not path.exists():
            continue
        for record in _tail_jsonl_records(path, max_records=1200):
            machine_name = str(record.get("machine") or record.get("machine_id") or "").strip()
            if not machine_name:
                machine_name = _machine_from_path(path)
            if not machine_name:
                continue
            bucket = by_machine.setdefault(machine_name, [])
            if len(bucket) >= rows_per_machine:
                continue
            bucket.append(record)
    return {machine: list(reversed(rows)) for machine, rows in by_machine.items()}


def _machine_from_path(path: Path) -> str:
    if len(path.parts) >= 3 and path.parts[-3].lower() == "data":
        return path.parts[-2]
    return ""


def _tail_jsonl_records(path: Path, *, max_records: int) -> list[dict[str, Any]]:
    if max_records <= 0:
        return []
    records: list[dict[str, Any]] = []
    with path.open("rb") as handle:
        handle.seek(0, 2)
        position = handle.tell()
        buffer = b""
        while position > 0 and len(records) < max_records:
            read_size = min(16 * 1024, position)
            position -= read_size
            handle.seek(position)
            chunk = handle.read(read_size)
            buffer = chunk + buffer
            lines = buffer.split(b"\n")
            buffer = lines[0]
            for raw in reversed(lines[1:]):
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, dict):
                    records.append(parsed)
                    if len(records) >= max_records:
                        break
        if buffer.strip() and len(records) < max_records:
            try:
                parsed = json.loads(buffer)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict):
                records.append(parsed)
    return records


def _safe_value(value: Any) -> Any:
    if value is None:
        return "-"
    if isinstance(value, str) and not value.strip():
        return "-"
    if isinstance(value, str) and value.strip().upper() == "UNAVAILABLE":
        return "-"
    if pd.isna(value):
        return "-"
    return value


_LIVE_TELEMETRY_SERVICE: LiveTelemetryService | None = None


def get_live_telemetry_service() -> LiveTelemetryService:
    global _LIVE_TELEMETRY_SERVICE
    if _LIVE_TELEMETRY_SERVICE is None:
        _LIVE_TELEMETRY_SERVICE = LiveTelemetryService()
    return _LIVE_TELEMETRY_SERVICE
