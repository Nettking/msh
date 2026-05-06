"""Session metadata and workflow status helpers for the runner."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from catalog.runner.script_catalog import ScriptOption

SESSION_VERSION = 2
SESSION_STATE_FILE = "session_state.json"
LEGACY_SESSION_FILE = "session.json"
HEALTH_CHECK_SCRIPT_KEYS: tuple[str, ...] = (
    "machines_active_per_day",
    "analyze_missing_sequence_number",
    "missing_per_day_by_machine",
    "sampling_rate_analysis",
)
PLAYBACK_TIMELINE_SCRIPT_KEYS: tuple[str, ...] = ("data_visualizer",)
MANUAL_DEEP_SCRIPT_KEYS: tuple[str, ...] = (
    "data_analysis",
    "ml_analysis",
    "corrolation_machine_pairs",
)
AUTOMATIC_RUNTIME_SCRIPT_KEYS: tuple[str, ...] = HEALTH_CHECK_SCRIPT_KEYS + PLAYBACK_TIMELINE_SCRIPT_KEYS

WORKFLOW_STEPS: list[tuple[str, list[str]]] = [
    (
        "Step 1: Startup-safe health checks",
        list(HEALTH_CHECK_SCRIPT_KEYS),
    ),
    (
        "Step 2: Playback/timeline generation",
        list(PLAYBACK_TIMELINE_SCRIPT_KEYS),
    ),
    (
        "Step 3: Raw day aggregates (manual)",
        [
            "data_pr_day",
        ],
    ),
    (
        "Step 4: Stop detection (manual)",
        [
            "find_stops",
        ],
    ),
    (
        "Step 5: Deep/exploratory analysis (manual heavy options)",
        list(MANUAL_DEEP_SCRIPT_KEYS),
    ),
]
WORKFLOW_SCRIPT_ORDER: list[str] = [script for _, scripts in WORKFLOW_STEPS for script in scripts]


@dataclass(frozen=True)
class SessionInfo:
    """Summary of one workflow session directory."""

    session_id: str
    session_dir: Path
    metadata: dict[str, Any]


def workflow_step_for_script(script_key: str) -> str | None:
    """Resolve the workflow step label for a script key."""
    for step_name, scripts in WORKFLOW_STEPS:
        if script_key in scripts:
            return step_name
    return None


def workflow_step_status(session_metadata: dict[str, Any], step_scripts: list[str]) -> str:
    """Derive step status from script-level statuses."""
    statuses = [str(session_metadata.get("scripts", {}).get(key, {}).get("status", "not_run")) for key in step_scripts]
    if statuses and all(value == "done" for value in statuses):
        return "complete"
    if any(value == "failed" for value in statuses):
        return "failed"
    if any(value == "done" for value in statuses):
        return "partial"
    return "not_run"


def list_sessions(workflows_root: Path) -> list[SessionInfo]:
    """List known workflow sessions sorted by creation time descending."""
    if not workflows_root.exists():
        return []

    sessions: list[SessionInfo] = []
    for item in sorted(workflows_root.iterdir(), key=lambda path: path.name, reverse=True):
        if not item.is_dir():
            continue
        metadata_path = item / SESSION_STATE_FILE
        if not metadata_path.exists():
            metadata_path = item / LEGACY_SESSION_FILE
        if not metadata_path.exists():
            continue
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        sessions.append(SessionInfo(session_id=item.name, session_dir=item, metadata=metadata))
    return sessions


def initialize_session_metadata(
    session_id: str,
    start_date: date,
    end_date: date,
    *,
    start_hour: int | None,
    end_hour: int | None,
    runtime_namespace: str | None = None,
    script_options: list[ScriptOption],
) -> dict[str, Any]:
    """Build a new session metadata payload."""
    scripts: dict[str, dict[str, Any]] = {}
    for item in script_options:
        scripts[item.key] = {
            "script_name": item.key,
            "category": item.category,
            "workflow_step": workflow_step_for_script(item.key),
            "status": "not_run",
            "output_path": None,
            "last_run_at": None,
            "duration_seconds": None,
            "exit_code": None,
        }

    now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    filter_payload = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "start_hour": start_hour,
        "end_hour": end_hour,
    }
    session_config_signature = filter_signature(filter_payload)
    return {
        "version": SESSION_VERSION,
        "session_id": session_id,
        "created_at": now,
        "updated_at": now,
        "session_config_signature": session_config_signature,
        "filter": filter_payload,
        "paths": {
            "filtered_data_dir": "data",
            "runs_dir": "runs",
            "playback_exports_dir": "exports/timeline",
        },
        "filter_result": {
            "filtered_data_path": "data",
            "matched_records": None,
            "matched_files": None,
            "generated_at": None,
        },
        "runtime": {
            "runtime_namespace": runtime_namespace or "default",
        },
        "scripts": scripts,
    }


def filter_signature(filter_payload: dict[str, Any]) -> str:
    """Compute the stable config signature used for session cache reuse."""
    return hashlib.sha256(json.dumps(filter_payload, sort_keys=True).encode("utf-8")).hexdigest()[:12]


def normalize_session_metadata(
    session_dir: Path,
    metadata: dict[str, Any],
    script_options: list[ScriptOption],
) -> tuple[dict[str, Any], bool]:
    """Fill missing session fields and add new script entries for backward compatibility."""
    changed = False

    paths = metadata.setdefault("paths", {})
    if "filtered_data_dir" not in paths:
        paths["filtered_data_dir"] = "data"
        changed = True
    if "runs_dir" not in paths:
        paths["runs_dir"] = "runs"
        changed = True
    if "playback_exports_dir" not in paths:
        paths["playback_exports_dir"] = "exports/timeline"
        changed = True

    filter_payload = metadata.setdefault("filter", {})
    expected_signature = filter_signature(
        {
            "start_date": filter_payload.get("start_date"),
            "end_date": filter_payload.get("end_date"),
            "start_hour": filter_payload.get("start_hour"),
            "end_hour": filter_payload.get("end_hour"),
        }
    )
    if metadata.get("session_config_signature") != expected_signature:
        metadata["session_config_signature"] = expected_signature
        changed = True

    filter_result = metadata.setdefault("filter_result", {})
    if "filtered_data_path" not in filter_result:
        filter_result["filtered_data_path"] = "data"
        changed = True

    scripts = metadata.setdefault("scripts", {})
    for item in script_options:
        entry = scripts.get(item.key)
        if entry is None:
            scripts[item.key] = {
                "script_name": item.key,
                "category": item.category,
                "workflow_step": workflow_step_for_script(item.key),
                "status": "not_run",
                "output_path": None,
                "last_run_at": None,
                "duration_seconds": None,
                "exit_code": None,
            }
            changed = True
            continue

        defaults = {
            "script_name": item.key,
            "category": item.category,
            "workflow_step": workflow_step_for_script(item.key),
            "status": "not_run",
            "output_path": None,
            "last_run_at": None,
            "duration_seconds": None,
            "exit_code": None,
        }
        for key, value in defaults.items():
            if key not in entry:
                entry[key] = value
                changed = True

    if refresh_script_cache_status(session_dir, metadata):
        changed = True

    runtime_payload = metadata.setdefault("runtime", {})
    if "runtime_namespace" not in runtime_payload:
        runtime_payload["runtime_namespace"] = "default"
        changed = True
    return metadata, changed


def write_session_metadata(session_dir: Path, metadata: dict[str, Any]) -> None:
    """Persist session metadata atomically."""
    metadata["updated_at"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    for file_name in (SESSION_STATE_FILE, LEGACY_SESSION_FILE):
        target = session_dir / file_name
        with NamedTemporaryFile("w", encoding="utf-8", dir=session_dir, delete=False) as tmp:
            json.dump(metadata, tmp, ensure_ascii=False, indent=2, sort_keys=True)
            tmp.write("\n")
            temp_path = Path(tmp.name)
        temp_path.replace(target)


def script_output_exists(session_dir: Path, script_entry: dict[str, Any]) -> bool:
    """Return True when a script entry points at an existing output folder."""
    output_path = script_entry.get("output_path")
    if not isinstance(output_path, str) or not output_path:
        return False
    return (session_dir / output_path).exists()


def refresh_script_cache_status(session_dir: Path, metadata: dict[str, Any]) -> bool:
    """Invalidate cached script status if metadata claims done but outputs are missing."""
    changed = False
    for script_entry in metadata.get("scripts", {}).values():
        if script_entry.get("status") != "done":
            continue
        if script_output_exists(session_dir, script_entry):
            continue
        script_entry["status"] = "not_run"
        script_entry["output_path"] = None
        script_entry["exit_code"] = None
        script_entry["last_run_at"] = None
        script_entry["duration_seconds"] = None
        changed = True
    return changed
