"""Read-only, telemetry-backed machine time models and what-if scenarios."""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from catalog.common.data_loading import iter_jsonl_files, iter_jsonl_records

MIN_INTERVAL_SECONDS = 0.1
MAX_INTERVAL_SECONDS = 30.0
MIN_CLASSIFIED_INTERVALS = 30
MAX_SOURCE_BYTES = 2 * 1024 * 1024 * 1024
RECOVERY_PERCENT_MAX = 50.0
_DAY_NS = 24 * 60 * 60 * 1_000_000_000

ACTIVE_EXECUTION_STATES = frozenset({"ACTIVE", "EXECUTING"})
NON_ACTIVE_EXECUTION_STATES = frozenset(
    {
        "STOPPED",
        "PROGRAM_STOPPED",
        "READY",
        "INTERRUPTED",
        "FEED_HOLD",
        "FEEDHOLD",
        "IDLE",
        "HOLD",
        "PAUSED",
    }
)
UNAVAILABLE_EXECUTION_STATES = frozenset({"", "UNAVAILABLE", "NAN", "NONE", "N/A"})


class DigitalTwinInputError(ValueError):
    """The requested telemetry scope cannot be modeled safely."""


class DigitalTwinDataError(RuntimeError):
    """Local telemetry cannot be read within the supported analysis boundary."""


@dataclass(frozen=True)
class _Snapshot:
    source_fingerprint: tuple[tuple[str, int, int], ...]
    source_file_count: int
    record_count: int
    machine_ranges: pd.DataFrame
    daily_quality: pd.DataFrame
    daily_states: pd.DataFrame


def _utc_date(value: str, *, name: str) -> str:
    if not isinstance(value, str) or len(value) != 10:
        raise DigitalTwinInputError(f"{name} must be an ISO date (YYYY-MM-DD).")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise DigitalTwinInputError(
            f"{name} must be an ISO date (YYYY-MM-DD)."
        ) from exc
    if parsed.isoformat() != value:
        raise DigitalTwinInputError(f"{name} must be an ISO date (YYYY-MM-DD).")
    return parsed.isoformat()


def _machine_identifier(record: dict[str, Any]) -> str | None:
    for field in ("machine_id", "machine", "resource"):
        value = record.get(field)
        if value is not None:
            normalized = str(value).strip()
            if normalized:
                return normalized
    return None


def _classify_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Attach a documented state label/class without treating unknown as idle."""

    prepared = frame.copy()
    execution = (
        prepared["execution"].astype("string").fillna("").str.strip().str.upper()
    )
    active = execution.isin(ACTIVE_EXECUTION_STATES)
    non_active = execution.isin(NON_ACTIVE_EXECUTION_STATES)
    unavailable = execution.isin(UNAVAILABLE_EXECUTION_STATES)

    rpm = pd.to_numeric(prepared["Srpm"], errors="coerce")
    load = pd.to_numeric(prepared["Sload"], errors="coerce")
    has_signals = rpm.notna() | load.notna()
    inferred_active = (
        rpm.gt(100).fillna(False) | load.gt(1).fillna(False)
    ) & has_signals
    inferred_non_active = ~inferred_active & has_signals
    fallback = unavailable & has_signals

    state_class = pd.Series(pd.NA, index=prepared.index, dtype="string")
    state_label = pd.Series(pd.NA, index=prepared.index, dtype="string")
    state_class.loc[active] = "active"
    state_label.loc[active] = execution.loc[active]
    state_class.loc[non_active] = "non_active"
    state_label.loc[non_active] = execution.loc[non_active]
    inferred_active_rows = fallback & inferred_active
    inferred_non_active_rows = fallback & inferred_non_active
    state_class.loc[inferred_active_rows] = "active"
    state_label.loc[inferred_active_rows] = "ACTIVE (signals inferred)"
    state_class.loc[inferred_non_active_rows] = "non_active"
    state_label.loc[inferred_non_active_rows] = "NON-ACTIVE (signals inferred)"
    prepared["state_class"] = state_class
    prepared["state_label"] = state_label
    return prepared


def _daily_aggregates(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    quality_parts: list[pd.DataFrame] = []
    state_parts: list[pd.DataFrame] = []

    for machine_id, group in frame.groupby("machine_id", sort=False):
        ordered = group.sort_values("timestamp", kind="mergesort")
        timestamp_ns = (
            ordered["timestamp"].to_numpy(dtype="datetime64[ns]").view("int64")
        )
        if len(timestamp_ns) < 2:
            continue

        start_ns = timestamp_ns[:-1]
        delta_ns = np.diff(timestamp_ns)
        positive = delta_ns > 0
        seconds = delta_ns.astype(np.float64) / 1_000_000_000.0
        usable = (
            positive
            & (seconds >= MIN_INTERVAL_SECONDS)
            & (seconds <= MAX_INTERVAL_SECONDS)
        )
        long_gap = positive & (seconds > MAX_INTERVAL_SECONDS)
        positive_indices = np.flatnonzero(positive)

        if len(positive_indices):
            positive_dates = pd.to_datetime(
                start_ns[positive_indices], unit="ns", utc=True
            ).strftime("%Y-%m-%d")
            quality_parts.append(
                pd.DataFrame(
                    {
                        "machine_id": machine_id,
                        "date": positive_dates,
                        "positive_intervals": 1,
                        "usable_intervals": usable[positive_indices].astype(np.int64),
                        "long_gaps": long_gap[positive_indices].astype(np.int64),
                    }
                )
            )

        state_classes = (
            ordered["state_class"].astype("string").to_numpy(dtype=object)[:-1]
        )
        state_labels = (
            ordered["state_label"].astype("string").to_numpy(dtype=object)[:-1]
        )
        classified_indices = np.flatnonzero(usable & pd.notna(state_classes))
        if not len(classified_indices):
            continue

        classified_starts = start_ns[classified_indices]
        classified_deltas = delta_ns[classified_indices]
        classified_seconds = seconds[classified_indices]
        day_ends = ((classified_starts // _DAY_NS) + 1) * _DAY_NS
        to_midnight = (day_ends - classified_starts).astype(
            np.float64
        ) / 1_000_000_000.0
        crosses_midnight = classified_starts + classified_deltas > day_ends
        first_seconds = np.where(crosses_midnight, to_midnight, classified_seconds)
        start_dates = pd.to_datetime(classified_starts, unit="ns", utc=True).strftime(
            "%Y-%m-%d"
        )
        state_parts.append(
            pd.DataFrame(
                {
                    "machine_id": machine_id,
                    "date": start_dates,
                    "state_class": state_classes[classified_indices],
                    "state_label": state_labels[classified_indices],
                    "seconds": first_seconds,
                    "classified_intervals": 1,
                }
            )
        )
        if crosses_midnight.any():
            extra_indices = np.flatnonzero(crosses_midnight)
            next_dates = pd.to_datetime(
                day_ends[extra_indices], unit="ns", utc=True
            ).strftime("%Y-%m-%d")
            state_parts.append(
                pd.DataFrame(
                    {
                        "machine_id": machine_id,
                        "date": next_dates,
                        "state_class": state_classes[classified_indices[extra_indices]],
                        "state_label": state_labels[classified_indices[extra_indices]],
                        "seconds": classified_seconds[extra_indices]
                        - first_seconds[extra_indices],
                        "classified_intervals": 0,
                    }
                )
            )

    quality_columns = [
        "machine_id",
        "date",
        "positive_intervals",
        "usable_intervals",
        "long_gaps",
    ]
    state_columns = [
        "machine_id",
        "date",
        "state_class",
        "state_label",
        "seconds",
        "classified_intervals",
    ]
    quality_rows = (
        pd.concat(quality_parts, ignore_index=True)
        if quality_parts
        else pd.DataFrame(columns=quality_columns)
    )
    states = (
        pd.concat(state_parts, ignore_index=True)
        if state_parts
        else pd.DataFrame(columns=state_columns)
    )
    daily_quality = (
        quality_rows.groupby(["machine_id", "date"], as_index=False, sort=True).agg(
            positive_intervals=("positive_intervals", "sum"),
            usable_intervals=("usable_intervals", "sum"),
            long_gaps=("long_gaps", "sum"),
        )
        if not quality_rows.empty
        else pd.DataFrame(columns=quality_columns)
    )
    daily_states = (
        states.groupby(
            ["machine_id", "date", "state_class", "state_label"],
            as_index=False,
            sort=True,
        ).agg(
            seconds=("seconds", "sum"),
            classified_intervals=("classified_intervals", "sum"),
        )
        if not states.empty
        else pd.DataFrame(columns=state_columns)
    )
    return daily_quality, daily_states


class DigitalTwinService:
    """Build and retain a compact daily snapshot from local JSONL telemetry."""

    def __init__(self, data_dir: Path | str = "data") -> None:
        self.data_dir = Path(data_dir).resolve()
        self._lock = threading.RLock()
        self._snapshot: _Snapshot | None = None
        self._checked_at = 0.0
        self._fingerprint_ttl_seconds = 30.0

    def _source_fingerprint(self) -> tuple[tuple[str, int, int], ...]:
        entries: list[tuple[str, int, int]] = []
        total_bytes = 0
        try:
            for path in iter_jsonl_files(self.data_dir, recursive=True):
                stat_result = path.stat()
                total_bytes += stat_result.st_size
                try:
                    relative = path.resolve().relative_to(self.data_dir).as_posix()
                except ValueError:
                    relative = path.resolve().as_posix()
                entries.append((relative, stat_result.st_mtime_ns, stat_result.st_size))
        except OSError as exc:
            raise DigitalTwinDataError(
                "The local telemetry directory could not be inspected."
            ) from exc
        if total_bytes > MAX_SOURCE_BYTES:
            raise DigitalTwinDataError(
                "The local telemetry corpus exceeds the 2 GiB v1 analysis limit."
            )
        return tuple(entries)

    def _load_snapshot(self) -> _Snapshot:
        source_fingerprint = self._source_fingerprint()
        if (
            self._snapshot is not None
            and source_fingerprint == self._snapshot.source_fingerprint
        ):
            return self._snapshot

        files = [self.data_dir / Path(item[0]) for item in source_fingerprint]
        records: list[tuple[str, Any, Any, Any, Any]] = []
        try:
            for source in files:
                for record in iter_jsonl_records(source):
                    machine_id = _machine_identifier(record)
                    if machine_id is None:
                        continue
                    records.append(
                        (
                            machine_id,
                            record.get("timestamp"),
                            record.get("execution"),
                            record.get("Srpm"),
                            record.get("Sload"),
                        )
                    )
        except OSError as exc:
            raise DigitalTwinDataError(
                "A local telemetry file could not be read."
            ) from exc

        columns = ["machine_id", "timestamp", "execution", "Srpm", "Sload"]
        frame = pd.DataFrame.from_records(records, columns=columns)
        if not frame.empty:
            frame["machine_id"] = frame["machine_id"].astype("string").str.strip()
            frame["timestamp"] = pd.to_datetime(
                frame["timestamp"], format="mixed", errors="coerce", utc=True
            )
            frame["Srpm"] = pd.to_numeric(frame["Srpm"], errors="coerce")
            frame["Sload"] = pd.to_numeric(frame["Sload"], errors="coerce")
            frame = frame.dropna(subset=["machine_id", "timestamp"])
            frame = frame[frame["machine_id"] != ""].copy()
            frame = _classify_rows(frame)

        if frame.empty:
            machine_ranges = pd.DataFrame(
                columns=["machine_id", "first_date", "last_date", "known_seconds"]
            )
            daily_quality = pd.DataFrame(
                columns=[
                    "machine_id",
                    "date",
                    "positive_intervals",
                    "usable_intervals",
                    "long_gaps",
                ]
            )
            daily_states = pd.DataFrame(
                columns=[
                    "machine_id",
                    "date",
                    "state_class",
                    "state_label",
                    "seconds",
                    "classified_intervals",
                ]
            )
        else:
            frame["utc_date"] = frame["timestamp"].dt.strftime("%Y-%m-%d")
            machine_ranges = frame.groupby("machine_id", as_index=False, sort=True).agg(
                first_date=("utc_date", "min"), last_date=("utc_date", "max")
            )
            daily_quality, daily_states = _daily_aggregates(frame)
            known_seconds = (
                daily_states.groupby("machine_id")["seconds"].sum()
                if not daily_states.empty
                else pd.Series(dtype="float64")
            )
            machine_ranges["known_seconds"] = (
                machine_ranges["machine_id"].map(known_seconds).fillna(0.0)
            )
            machine_ranges = machine_ranges.sort_values(
                ["known_seconds", "machine_id"],
                ascending=[False, True],
                kind="mergesort",
            ).reset_index(drop=True)

        snapshot = _Snapshot(
            source_fingerprint=source_fingerprint,
            source_file_count=len(files),
            record_count=len(frame),
            machine_ranges=machine_ranges,
            daily_quality=daily_quality,
            daily_states=daily_states,
        )
        self._snapshot = snapshot
        return snapshot

    def _snapshot_for_request(self) -> _Snapshot:
        with self._lock:
            now = time.monotonic()
            if (
                self._snapshot is not None
                and now - self._checked_at < self._fingerprint_ttl_seconds
            ):
                return self._snapshot
            snapshot = self._load_snapshot()
            self._checked_at = now
            return snapshot

    def analyze(
        self,
        *,
        machine_id: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        recovery_percent: float = 10.0,
    ) -> dict[str, Any]:
        """Summarize a machine/date range and apply the specified what-if."""

        try:
            recovery = float(recovery_percent)
        except (TypeError, ValueError, OverflowError) as exc:
            raise DigitalTwinInputError(
                "Recovery must be between 0 and 50 percent."
            ) from exc
        if (
            not math.isfinite(recovery)
            or not 0 <= recovery <= RECOVERY_PERCENT_MAX
            or recovery % 5 != 0
        ):
            raise DigitalTwinInputError(
                "Recovery must be between 0 and 50 percent in 5-point steps."
            )

        snapshot = self._snapshot_for_request()
        machines = [
            {
                "machine_id": str(row.machine_id),
                "first_date": str(row.first_date),
                "last_date": str(row.last_date),
                "known_seconds": float(row.known_seconds),
            }
            for row in snapshot.machine_ranges.itertuples(index=False)
        ]
        selected_id = str(machine_id).strip() if machine_id is not None else ""
        if not selected_id and machines:
            selected_id = machines[0]["machine_id"]
        if selected_id and selected_id not in {item["machine_id"] for item in machines}:
            raise DigitalTwinInputError(
                "The selected machine is not present in local telemetry."
            )

        machine_info = next(
            (item for item in machines if item["machine_id"] == selected_id), None
        )
        resolved_start = (
            _utc_date(start_date, name="Start date")
            if start_date
            else (machine_info["first_date"] if machine_info else None)
        )
        resolved_end = (
            _utc_date(end_date, name="End date")
            if end_date
            else (machine_info["last_date"] if machine_info else None)
        )
        if resolved_start and resolved_end and resolved_start > resolved_end:
            raise DigitalTwinInputError("Start date must be on or before end date.")

        result: dict[str, Any] = {
            "machines": machines,
            "machine_id": selected_id or None,
            "machine_info": machine_info,
            "start_date": resolved_start or "",
            "end_date": resolved_end or "",
            "source_file_count": snapshot.source_file_count,
            "record_count": snapshot.record_count,
            "has_data": bool(machines),
            "sufficient_data": False,
            "recovery_percent": recovery,
            "active_seconds": 0.0,
            "non_active_seconds": 0.0,
            "observed_seconds": 0.0,
            "simulated_active_seconds": 0.0,
            "recovered_seconds": 0.0,
            "observed_active_share": None,
            "simulated_active_share": None,
            "active_share_change_pp": None,
            "positive_intervals": 0,
            "usable_intervals": 0,
            "classified_intervals": 0,
            "long_gaps": 0,
            "continuity": None,
            "state_breakdown": [],
        }
        if not selected_id or not resolved_start or not resolved_end:
            return result

        state_scope = snapshot.daily_states[
            (snapshot.daily_states["machine_id"] == selected_id)
            & (snapshot.daily_states["date"] >= resolved_start)
            & (snapshot.daily_states["date"] <= resolved_end)
        ]
        quality_scope = snapshot.daily_quality[
            (snapshot.daily_quality["machine_id"] == selected_id)
            & (snapshot.daily_quality["date"] >= resolved_start)
            & (snapshot.daily_quality["date"] <= resolved_end)
        ]

        active_seconds = float(
            state_scope.loc[state_scope["state_class"] == "active", "seconds"].sum()
        )
        non_active_seconds = float(
            state_scope.loc[state_scope["state_class"] == "non_active", "seconds"].sum()
        )
        observed_seconds = active_seconds + non_active_seconds
        positive_intervals = (
            int(quality_scope["positive_intervals"].sum())
            if not quality_scope.empty
            else 0
        )
        usable_intervals = (
            int(quality_scope["usable_intervals"].sum())
            if not quality_scope.empty
            else 0
        )
        classified_intervals = (
            int(state_scope["classified_intervals"].sum())
            if not state_scope.empty
            else 0
        )
        long_gaps = (
            int(quality_scope["long_gaps"].sum()) if not quality_scope.empty else 0
        )
        recovered_seconds = non_active_seconds * recovery / 100.0
        sufficient_data = (
            classified_intervals >= MIN_CLASSIFIED_INTERVALS and observed_seconds > 0
        )
        simulated_active_seconds = (
            active_seconds + recovered_seconds if sufficient_data else None
        )

        result.update(
            {
                "active_seconds": active_seconds,
                "non_active_seconds": non_active_seconds,
                "observed_seconds": observed_seconds,
                "simulated_active_seconds": simulated_active_seconds,
                "recovered_seconds": recovered_seconds if sufficient_data else None,
                "observed_active_share": active_seconds / observed_seconds
                if observed_seconds
                else None,
                "simulated_active_share": (
                    simulated_active_seconds / observed_seconds
                    if sufficient_data and observed_seconds
                    else None
                ),
                "active_share_change_pp": (
                    (simulated_active_seconds - active_seconds)
                    / observed_seconds
                    * 100.0
                    if sufficient_data and observed_seconds
                    else None
                ),
                "positive_intervals": positive_intervals,
                "usable_intervals": usable_intervals,
                "classified_intervals": classified_intervals,
                "long_gaps": long_gaps,
                "continuity": usable_intervals / positive_intervals
                if positive_intervals
                else None,
                "state_breakdown": [
                    {"label": str(row.state_label), "seconds": float(row.seconds)}
                    for row in state_scope.groupby(
                        "state_label", as_index=False, sort=True
                    )["seconds"]
                    .sum()
                    .itertuples(index=False)
                ],
                "sufficient_data": sufficient_data,
            }
        )
        return result


_SERVICES: dict[str, DigitalTwinService] = {}
_SERVICES_LOCK = threading.Lock()


def get_digital_twin_service(data_dir: Path | str = "data") -> DigitalTwinService:
    """Return the process-local snapshot service for a telemetry root."""

    root = str(Path(data_dir).resolve())
    with _SERVICES_LOCK:
        service = _SERVICES.get(root)
        if service is None:
            service = DigitalTwinService(root)
            _SERVICES[root] = service
        return service
