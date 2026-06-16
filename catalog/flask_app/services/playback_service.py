"""UI-facing validation and shaping helpers for playback timeline artifacts.

Playback views consume already-derived timeline tables rather than raw JSONL.
The helpers here enforce the minimal table contract, normalize timestamps and
machine/state fields, and create display-oriented subsets/resampled rows without
changing the underlying export files.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from catalog.common.artifact_registry import read_raw_table
from catalog.common.artifact_registry import read_table_columns
from catalog.common.timeline_exports import build_state_interval_export, build_timeline_rows_export
from catalog.common.telemetry_cache import TelemetryCache, cached_cache_status

REQUIRED_PLAYBACK_COLUMNS = {"timestamp", "machine_id", "state"}
DEFAULT_LIVE_SIGNAL_COLUMNS = ["Srpm", "Sload", "Sovr", "Fovr", "Frapidovr"]
PLAYBACK_TICK_FREQUENCY = "200ms"
DEFAULT_FALLBACK_PLAYBACK_DELAY_SECONDS = 0.2
DEFAULT_MAX_PLAYBACK_DELAY_SECONDS = 5.0
TELEMETRY_CACHE_PLAYBACK_PATH = "telemetry-cache://timeline"


@dataclass
class PlaybackValidation:
    """Validation result for playback source/file contract checks."""

    is_valid: bool
    reason: str = ""


@dataclass(frozen=True)
class SessionNamespace:
    """Runtime namespace read from workflow session metadata."""

    value: str
    source: str
    missing: bool = False


@dataclass(frozen=True)
class PlaybackArtifactIndexEntry:
    """Lightweight machine/day index data for one playback artifact."""

    path: str
    machine_days: tuple[tuple[str, tuple[str, ...]], ...]
    machine_day_counts: tuple[tuple[str, tuple[tuple[str, int], ...]], ...]


@dataclass(frozen=True)
class PlaybackSelectionIndex:
    """Combined machine/day choices across visible playback artifacts."""

    context: dict[str, list[str]]
    machine_days: dict[str, list[str]]
    machine_day_counts: dict[str, dict[str, int]]
    entries_by_path: dict[str, PlaybackArtifactIndexEntry]


@dataclass(frozen=True)
class PlaybackSelection:
    """Resolved playback setup state for the Flask route."""

    selected_path: str
    machine: str
    day: str
    context: dict[str, list[str]]
    machine_days: dict[str, list[str]]
    machine_day_counts: dict[str, dict[str, int]]
    selected_machine_days: list[str]
    selected_machine_day_counts: dict[str, int]


def telemetry_cache_playback_artifact() -> dict[str, Any] | None:
    """Return a virtual playback artifact for a fresh telemetry cache."""

    status = cached_cache_status(Path("data"))
    if not status.exists or not status.fresh:
        return None
    return {
        "path": TELEMETRY_CACHE_PLAYBACK_PATH,
        "file_name": "Telemetry analytics cache",
        "category": "source_data",
        "status": "ready",
        "visibility": "default",
        "playback_compatible": True,
        "modified_at": status.manifest_generated_at or str(status.latest_cache_mtime or ""),
        "signature": f"telemetry-cache:{status.latest_cache_mtime}:{status.manifest_row_count}",
        "row_count": status.manifest_row_count or 0,
        "is_virtual_cache": True,
    }


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


def _safe_runtime_namespace_for_session_id(namespace: str) -> str:
    """Mirror automatic workflow session-id namespace sanitization."""
    cleaned = re.sub(r"[^a-zA-Z0-9_\-]", "_", namespace.strip())
    return cleaned[:48] if cleaned else "default"


def _namespace_from_auto_session_id(session_id: str) -> str | None:
    """Return the namespace segment embedded in an automatic workflow session id."""
    match = re.fullmatch(r"auto_(?P<namespace>.+)_\d{8}_\d{8}", session_id.strip())
    if not match:
        return None
    return match.group("namespace") or None


def _session_runtime_namespace(session_dir: Path) -> SessionNamespace:
    """Read a session runtime namespace without losing whether metadata was absent.

    Older sessions may not have a runtime namespace and are treated as the
    ``default`` namespace for compatibility, but callers still need to know the
    namespace was missing so active clean-runtime sessions can be rescued when
    their automatic session id prefix links them to the current run.
    """
    for file_name in ("session_state.json", "session.json"):
        metadata_path = session_dir / file_name
        if not metadata_path.exists():
            continue
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return SessionNamespace("default", f"{file_name}:unreadable", missing=True)
        runtime_payload = metadata.get("runtime") if isinstance(metadata.get("runtime"), dict) else {}
        namespace = runtime_payload.get("runtime_namespace")
        if namespace:
            return SessionNamespace(str(namespace), f"{file_name}:runtime.runtime_namespace", missing=False)
        return SessionNamespace("default", f"{file_name}:missing", missing=True)
    return SessionNamespace("default", "metadata_missing", missing=True)


def _session_is_linked_to_active_runtime(session_dir: Path, active_namespace: str) -> bool:
    """Return true when session naming clearly links a session to the active run.

    Runtime state only tracks the latest processed session id during catch-up.
    Clean-runtime visibility must therefore key off the active namespace-derived
    automatic session prefix, not equality with ``runtime_state["session_id"]``.
    """
    if active_namespace:
        safe_namespace = _safe_runtime_namespace_for_session_id(active_namespace)
        return session_dir.name.startswith(f"auto_{safe_namespace}_")

    return False


def filter_playback_artifacts_for_runtime(
    artifacts: list[dict],
    runtime_state: dict | None,
    *,
    selected_path: str = "",
    logger=None,
) -> list[dict]:
    """Hide workflow playback exports that do not belong to the active clean runtime.

    Workflow timeline exports are primarily gated by the namespace stored at
    ``runtime.runtime_namespace`` in ``session_state.json``/``session.json``.
    Missing namespaces are treated carefully: legacy sessions remain in the
    ``default`` namespace, while clean-start sessions generated by the current
    runtime can still be shown when their automatic session id prefix ties them
    to the active namespace. Non-workflow playback-compatible files do
    not carry workflow runtime namespace metadata; during clean startup they are
    only suppressed from automatic selection when at least one current-runtime
    workflow export is available, while explicit non-workflow selections remain
    loadable for manual exploration.
    """
    state = runtime_state or {}
    startup_mode = str(state.get("startup_mode") or "")
    is_clean_startup = startup_mode == "start_clean"
    active_namespace = str(state.get("active_runtime_namespace") or "default")
    if is_clean_startup and active_namespace == "default":
        inferred_namespace = _namespace_from_auto_session_id(str(state.get("session_id") or ""))
        if inferred_namespace:
            active_namespace = inferred_namespace
    visible: list[dict] = []

    def log_totals(stage: str, total: int) -> None:
        if logger is None:
            return
        logger.info(
            "Playback runtime filter %s total=%s active_namespace=%s startup_mode=%s state_session_id=%s",
            stage,
            total,
            active_namespace,
            startup_mode,
            str(state.get("session_id") or ""),
        )

    def log_hidden(artifact_path: str, session_dir: Path | None, session_id: str, artifact_namespace: str, reason: str) -> None:
        if logger is None:
            return
        logger.info(
            "Playback runtime filter hid workflow artifact path=%s session_dir=%s session_id=%s "
            "active_namespace=%s artifact_namespace=%s startup_mode=%s state_session_id=%s reason=%s",
            artifact_path,
            str(session_dir) if session_dir is not None else "",
            session_id,
            active_namespace,
            artifact_namespace,
            startup_mode,
            str(state.get("session_id") or ""),
            reason,
        )

    def log_hidden_non_workflow(artifact_path: str, reason: str) -> None:
        if logger is None:
            return
        logger.info(
            "Playback runtime filter hid non-workflow artifact path=%s session_dir= session_id= "
            "active_namespace=%s artifact_namespace=none startup_mode=%s state_session_id=%s reason=%s",
            artifact_path,
            active_namespace,
            startup_mode,
            str(state.get("session_id") or ""),
            reason,
        )

    def include_artifact(artifact: dict, reason: str) -> None:
        visible.append(artifact)
        if logger is None:
            return
        logger.info(
            "Playback runtime filter included artifact path=%s reason=%s",
            str(artifact.get("path") or ""),
            reason,
        )

    log_totals("before_filter", len(artifacts))

    non_workflow_artifacts: list[dict] = []
    for artifact in artifacts:
        artifact_path = str(artifact.get("path") or "")
        session_dir = _workflow_session_dir_for_artifact(artifact_path)
        if session_dir is None:
            non_workflow_artifacts.append(artifact)
            continue

        namespace = _session_runtime_namespace(session_dir)
        session_id = session_dir.name
        if namespace.value == active_namespace:
            include_artifact(artifact, "workflow_namespace_matches_active_runtime")
            continue

        if is_clean_startup and _session_is_linked_to_active_runtime(session_dir, active_namespace):
            include_artifact(artifact, "workflow_session_linked_to_active_clean_runtime")
            continue

        reason = "namespace_mismatch"
        if namespace.missing:
            reason = "missing_namespace_defaults_to_default_without_active_runtime_link"
        log_hidden(
            artifact_path,
            session_dir,
            session_id,
            f"{namespace.value} ({namespace.source})",
            reason,
        )

    has_current_workflow_export = any(
        _workflow_session_dir_for_artifact(str(item.get("path") or "")) is not None for item in visible
    )
    for artifact in non_workflow_artifacts:
        artifact_path = str(artifact.get("path") or "")
        if is_clean_startup and has_current_workflow_export and artifact_path != selected_path:
            log_hidden_non_workflow(
                artifact_path,
                "clean_startup_current_workflow_export_available_non_workflow_not_explicitly_selected",
            )
            continue
        reason = (
            "explicit_non_workflow_selection"
            if artifact_path == selected_path
            else "non_workflow_artifact_no_runtime_namespace_filter"
        )
        include_artifact(artifact, reason)

    log_totals("after_filter", len(visible))
    return visible


def _active_runtime_namespace(runtime_state: dict | None) -> str:
    state = runtime_state or {}
    active_namespace = str(state.get("active_runtime_namespace") or "default")
    if active_namespace == "default":
        inferred_namespace = _namespace_from_auto_session_id(str(state.get("session_id") or ""))
        if inferred_namespace:
            return inferred_namespace
    return active_namespace


def playback_artifact_runtime_preference(artifact: dict[str, Any], runtime_state: dict | None) -> tuple[int, str, str]:
    """Return a stable preference tuple for duplicate machine/day artifacts.

    Higher tuples are preferred. Workflow artifacts in the active runtime namespace
    outrank other artifacts; ties fall back to artifact modified time and path so
    the newest or lexicographically latest scan entry wins without exposing
    namespace internals to Flask routes.
    """
    active_namespace = _active_runtime_namespace(runtime_state)
    session_dir = _workflow_session_dir_for_artifact(str(artifact.get("path") or ""))
    active_workflow = False
    if session_dir is not None:
        active_workflow = _session_runtime_namespace(session_dir).value == active_namespace
    return (
        1 if active_workflow else 0,
        str(artifact.get("modified_at") or ""),
        str(artifact.get("path") or ""),
    )


def _artifact_index_key(artifacts: list[dict[str, Any]]) -> tuple[tuple[str, str, str, str], ...]:
    return tuple(
        sorted(
            (
                str(artifact.get("path") or ""),
                str(artifact.get("signature") or ""),
                str(artifact.get("modified_at") or ""),
                str(artifact.get("row_count") or ""),
            )
            for artifact in artifacts
            if artifact.get("path")
        )
    )


def _scope_key(scope) -> tuple[str, str, bool]:
    return (
        str(getattr(scope, "start_date", "") or ""),
        str(getattr(scope, "end_date", "") or ""),
        bool(getattr(scope, "is_active", False)),
    )


def _read_playback_index_columns(path: str) -> pd.DataFrame:
    source = Path(path)
    suffix = source.suffix.lower()
    columns = ["timestamp", "machine_id"]
    if suffix == ".csv":
        return pd.read_csv(source, usecols=columns)
    if suffix in {".parquet", ".pq"}:
        try:
            return pd.read_parquet(source, columns=columns)
        except TypeError:
            return pd.read_parquet(source)[columns]
    if suffix == ".jsonl":
        rows = []
        with source.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if isinstance(record, dict):
                    rows.append({column: record.get(column) for column in columns})
        return pd.DataFrame(rows, columns=columns)
    if suffix == ".json":
        frame = pd.read_json(source)
        return frame[columns]
    raise ValueError(f"Unsupported file extension: {suffix}")


def _playback_index_entry_for_path(
    path: str,
    scope_start: str,
    scope_end: str,
    scope_active: bool,
) -> PlaybackArtifactIndexEntry | None:
    if path == TELEMETRY_CACHE_PLAYBACK_PATH:
        return _telemetry_cache_playback_index_entry(path, scope_start, scope_end, scope_active)
    try:
        frame = _read_playback_index_columns(path)
    except Exception:  # noqa: BLE001
        return None
    missing = {"timestamp", "machine_id"}.difference(frame.columns)
    if missing:
        return None
    frame = frame.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    frame["machine_id"] = frame["machine_id"].astype("string").str.strip()
    frame = frame.dropna(subset=["timestamp", "machine_id"])
    frame = frame[frame["machine_id"] != ""]
    if frame.empty:
        return None
    frame["day"] = frame["timestamp"].dt.date.astype(str)
    if scope_active:
        frame = frame[(frame["day"] >= scope_start) & (frame["day"] <= scope_end)]
    if frame.empty:
        return None
    grouped = frame.groupby(["machine_id", "day"], dropna=True).size()
    counts_by_machine: dict[str, dict[str, int]] = {}
    for (machine_id, day), count in grouped.items():
        machine_key = str(machine_id).strip()
        day_key = str(day).strip()
        if not machine_key or not day_key:
            continue
        counts_by_machine.setdefault(machine_key, {})[day_key] = int(count)
    machine_days = tuple(
        (machine_id, tuple(sorted(day_counts.keys())))
        for machine_id, day_counts in sorted(counts_by_machine.items())
    )
    machine_day_counts = tuple(
        (machine_id, tuple((day, day_counts[day]) for day in sorted(day_counts.keys())))
        for machine_id, day_counts in sorted(counts_by_machine.items())
    )
    return PlaybackArtifactIndexEntry(path=path, machine_days=machine_days, machine_day_counts=machine_day_counts)


def _telemetry_cache_playback_index_entry(
    path: str,
    scope_start: str,
    scope_end: str,
    scope_active: bool,
) -> PlaybackArtifactIndexEntry | None:
    status = cached_cache_status(Path("data"))
    if not status.exists or not status.fresh:
        return None
    try:
        frame = TelemetryCache(status.cache_path).machine_day_row_counts(
            start_date=scope_start if scope_active else None,
            end_date=scope_end if scope_active else None,
            as_dataframe=True,
        )
    except Exception:  # noqa: BLE001
        return None
    if frame.empty or not {"date", "machine", "value"}.issubset(frame.columns):
        return None
    counts_by_machine: dict[str, dict[str, int]] = {}
    for _, row in frame.iterrows():
        machine_key = str(row.get("machine") or "").strip()
        day_key = str(row.get("date") or "").strip()[:10]
        if not machine_key or not day_key:
            continue
        try:
            count = int(row.get("value") or 0)
        except (TypeError, ValueError):
            count = 0
        if count <= 0:
            continue
        counts_by_machine.setdefault(machine_key, {})[day_key] = count
    if not counts_by_machine:
        return None
    machine_days = tuple(
        (machine_id, tuple(sorted(day_counts.keys())))
        for machine_id, day_counts in sorted(counts_by_machine.items())
    )
    machine_day_counts = tuple(
        (machine_id, tuple((day, day_counts[day]) for day in sorted(day_counts.keys())))
        for machine_id, day_counts in sorted(counts_by_machine.items())
    )
    return PlaybackArtifactIndexEntry(path=path, machine_days=machine_days, machine_day_counts=machine_day_counts)

@lru_cache(maxsize=32)
def _cached_playback_selection_index(
    artifact_key: tuple[tuple[str, str, str, str], ...],
    scope_start: str,
    scope_end: str,
    scope_active: bool,
) -> PlaybackSelectionIndex:
    entries: dict[str, PlaybackArtifactIndexEntry] = {}
    machines: set[str] = set()
    days_by_machine: dict[str, set[str]] = {}
    counts_by_machine: dict[str, dict[str, int]] = {}

    for artifact in artifact_key:
        path = artifact[0]
        entry = _playback_index_entry_for_path(path, scope_start, scope_end, scope_active)
        if entry is None:
            continue
        entries[path] = entry
        for machine_id, days in entry.machine_days:
            if not days:
                continue
            machines.add(machine_id)
            days_by_machine.setdefault(machine_id, set()).update(days)
        for machine_id, day_counts in entry.machine_day_counts:
            machine_counts = counts_by_machine.setdefault(machine_id, {})
            for day, count in day_counts:
                machine_counts[day] = machine_counts.get(day, 0) + int(count)

    machine_days = {machine_id: sorted(days) for machine_id, days in sorted(days_by_machine.items())}
    all_days = sorted({day for days in days_by_machine.values() for day in days})
    sorted_counts = {
        machine_id: {day: day_counts[day] for day in sorted(day_counts.keys())}
        for machine_id, day_counts in sorted(counts_by_machine.items())
    }
    return PlaybackSelectionIndex(
        context={"machines": sorted(machines), "days": all_days},
        machine_days=machine_days,
        machine_day_counts=sorted_counts,
        entries_by_path=entries,
    )


def playback_selection_index(artifacts: list[dict[str, Any]], scope) -> PlaybackSelectionIndex:
    """Return cached lightweight machine/day choices for playback artifacts."""
    scope_start, scope_end, scope_active = _scope_key(scope)
    return _cached_playback_selection_index(_artifact_index_key(artifacts), scope_start, scope_end, scope_active)


def _entry_machine_days(entry: PlaybackArtifactIndexEntry) -> dict[str, list[str]]:
    return {machine_id: list(days) for machine_id, days in entry.machine_days}


def _entry_machines(entry: PlaybackArtifactIndexEntry) -> list[str]:
    return [machine_id for machine_id, days in entry.machine_days if days]


def _best_artifact_for_machine_day(
    artifacts_by_path: dict[str, dict[str, Any]],
    index: PlaybackSelectionIndex,
    machine: str,
    day: str,
    runtime_state: dict | None,
) -> str:
    matching_paths = [
        path
        for path, entry in index.entries_by_path.items()
        if day in _entry_machine_days(entry).get(machine, [])
    ]
    if not matching_paths:
        return ""
    return max(
        matching_paths,
        key=lambda path: playback_artifact_runtime_preference(
            artifacts_by_path.get(path, {"path": path}),
            runtime_state,
        ),
    )


def resolve_playback_selection(
    artifacts: list[dict[str, Any]],
    runtime_state: dict | None,
    *,
    requested_path: str = "",
    requested_machine: str = "",
    requested_day: str = "",
    scope=None,
) -> PlaybackSelection:
    """Resolve playback UI machine/day state and internal artifact path.

    The returned context is built from a cached lightweight index. Full playback
    rows are intentionally not loaded here; callers should load only
    ``selected_path`` after this resolution step.
    """
    index = playback_selection_index(artifacts, scope)
    artifacts_by_path = {str(artifact.get("path") or ""): artifact for artifact in artifacts}
    visible_paths = set(artifacts_by_path.keys())
    machine = str(requested_machine or "")
    day = str(requested_day or "")
    selected_path = ""

    requested_entry = index.entries_by_path.get(requested_path) if requested_path in visible_paths else None
    if requested_entry is not None:
        selected_path = requested_path
        requested_machine_days = _entry_machine_days(requested_entry)
        requested_machines = _entry_machines(requested_entry)
        if not machine and requested_machines:
            machine = requested_machines[0]
        if machine and machine not in requested_machines:
            machine = requested_machines[0] if requested_machines else ""
        requested_days = requested_machine_days.get(machine, [])
        if day and day not in requested_days:
            day = ""
        if not day and requested_days:
            day = requested_days[0]
    else:
        if machine and machine not in index.context["machines"]:
            machine = index.context["machines"][0] if index.context["machines"] else ""
        if not machine and index.context["machines"]:
            machine = index.context["machines"][0]
        selected_machine_days_for_choice = index.machine_days.get(machine, [])
        if day and day not in selected_machine_days_for_choice:
            day = ""
        if not day and selected_machine_days_for_choice:
            day = selected_machine_days_for_choice[0]
        selected_path = (
            _best_artifact_for_machine_day(artifacts_by_path, index, machine, day, runtime_state)
            if machine and day
            else ""
        )

    selected_machine_days = index.machine_days.get(machine, [])
    selected_machine_day_counts = index.machine_day_counts.get(machine, {})
    return PlaybackSelection(
        selected_path=selected_path,
        machine=machine,
        day=day,
        context=index.context,
        machine_days=index.machine_days,
        machine_day_counts=index.machine_day_counts,
        selected_machine_days=selected_machine_days,
        selected_machine_day_counts=selected_machine_day_counts,
    )


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
    if path == TELEMETRY_CACHE_PLAYBACK_PATH:
        status = cached_cache_status(Path("data"))
        if status.exists and status.fresh:
            return PlaybackValidation(True, "")
        return PlaybackValidation(False, "Telemetry analytics cache is missing or stale.")
    columns, load_error = read_table_columns(path)
    if load_error:
        return PlaybackValidation(False, f"Unable to inspect source columns: {load_error}")
    missing = sorted(REQUIRED_PLAYBACK_COLUMNS.difference(columns))
    if missing:
        return PlaybackValidation(False, f"Missing required source columns: {', '.join(missing)}")
    return PlaybackValidation(True, "")


def load_playback_frame(path: str) -> tuple[pd.DataFrame | None, str | None]:
    if path == TELEMETRY_CACHE_PLAYBACK_PATH:
        status = cached_cache_status(Path("data"))
        if not status.exists or not status.fresh:
            return None, "Telemetry analytics cache is missing or stale."
        try:
            samples = TelemetryCache(status.cache_path).samples_by_date_range(
                "1900-01-01",
                "2999-12-31",
                as_dataframe=True,
            )
            return build_timeline_rows_export(samples), None
        except Exception as exc:  # noqa: BLE001
            return None, f"Could not load telemetry analytics cache: {exc}"
    try:
        frame = read_raw_table(path)
    except Exception as exc:  # noqa: BLE001
        return None, f"Could not load '{path}': {exc}"
    if not isinstance(frame, pd.DataFrame):
        return None, f"Could not load '{path}': source did not produce a table."
    return frame, None


def load_cached_playback_frame_for_machine_day(machine_id: str, day: str) -> tuple[pd.DataFrame | None, str | None]:
    """Load a playback-ready timeline from the telemetry cache for one machine/day."""

    status = cached_cache_status(Path("data"))
    if not status.exists or not status.fresh:
        return None, "Telemetry analytics cache is missing or stale."
    try:
        samples = TelemetryCache(status.cache_path).samples_by_date_range(day, day, as_dataframe=True)
    except Exception as exc:  # noqa: BLE001
        return None, f"Could not query telemetry analytics cache: {exc}"
    if samples.empty:
        return pd.DataFrame(columns=list(REQUIRED_PLAYBACK_COLUMNS)), None
    samples = samples[samples["machine_id"].astype("string") == str(machine_id)]
    return build_timeline_rows_export(samples), None


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
