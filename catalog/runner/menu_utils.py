"""
Utility functions for the interactive catalog runner.

This module supports the CLI runner by providing functionality for:

- discovering runnable catalog scripts
- discovering available data dates from JSONL telemetry files
- caching per-file date discovery results
- filtering source telemetry into session-scoped workspaces
- storing and updating session metadata and script execution status
- executing selected scripts in isolated per-run directories

Key behavior
------------
Date discovery:
- scans JSONL files recursively
- prefers dates parsed from record timestamps
- falls back to dates extracted from filenames only when a file has no usable
  timestamp-derived dates
- caches per-file date discovery results in a local JSON index to speed up
  repeated runs

Filtering:
- normal runs filter by date only
- same-day runs may optionally filter by whole-hour range
- hour-filtered runs require parseable timestamps and do not use filename-only
  fallback, because filenames cannot determine hour-of-day

Execution:
- sessions live under ``results/workflows/<session-id>/``
- each script execution gets its own run directory under ``runs/<script>/<timestamp>/``
- the selected script runs against the session-filtered data copy
- the catalog source is copied into each run workspace before execution

Notes
-----
The cache is used only for date discovery, not for record inclusion/exclusion
during filtering. This is an intentional safety choice to avoid skipping data
based on incomplete or ambiguous cached state.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile, mkdtemp
from time import perf_counter
from typing import Any, Iterable, Literal

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from catalog.common.data_loading import iter_jsonl_files, iter_jsonl_records
from catalog.common.time_utils import date_from_filename, parse_iso_timestamp, parse_timestamp_to_date

# Catalog folders intentionally hidden from the interactive runner.
#
# These remain documented in catalog/README.md, but they are not part of the
# default "pick script + date range" analysis flow.
RUNNER_HIDDEN_FOLDERS = {
    "runner",  # runner implementation internals
    "auto_connect",  # desktop automation helper
    "data_simulator",  # streamlit app, not a one-shot CLI analysis run
    "interventions",  # environment-specific script
    "standalone_recorder",  # ingestion tool (legacy)
    "standalone-recorder_v2",  # ingestion tool (preferred recorder)
}

# Versioned JSON cache used only for date discovery.
DATA_INDEX_VERSION = 1
DATA_INDEX_FILE = ROOT_DIR / "results" / "runner" / "data_index.json"

ToolCategory = Literal["Simple", "Advanced", "Legacy"]

# Runner-facing metadata. Keep this conservative and focused on navigation.
SCRIPT_METADATA: dict[str, dict[str, str | bool]] = {
    # Stage 1: data health checks (first-pass).
    "machines_active_per_day": {
        "category": "Simple",
        "description": "Stage 1 (Health): count distinct active machines per day.",
    },
    "analyze_missing_sequence_number": {
        "category": "Simple",
        "description": "Stage 1 (Health): summarize missing sequence numbers per day.",
    },
    "missing_per_day_by_machine": {
        "category": "Simple",
        "description": "Stage 1 (Health): per-machine missing sequence summary by day.",
    },
    "sampling_rate_analysis": {
        "category": "Simple",
        "description": "Stage 1 (Health): average telemetry sampling rate per day.",
    },
    # Stage 2: raw inspection.
    "data_pr_day": {
        "category": "Simple",
        "description": "Stage 2 (Raw): per-machine/day raw signal plots.",
    },
    # Stage 3: stop-focused inspection.
    "find_stops": {
        "category": "Simple",
        "description": "Stage 3 (Stops): stop timeline plots for day/hour windows.",
    },
    # Stage 4: deeper exploratory analysis.
    "data_visualizer": {
        "category": "Advanced",
        "description": "Stage 4 (Explore): state timelines and candidate-event export.",
    },
    "data_analysis": {
        "category": "Advanced",
        "description": "Stage 4 (Explore): deeper terminal diagnostics and exploratory summaries.",
    },
    "ml_analysis": {
        "category": "Advanced",
        "description": "Stage 4 (Explore): per-machine ML baseline for future-stop prediction.",
    },
    # Legacy / no longer a recommended main workflow.
    "corrolation_machine_pairs": {
        "category": "Legacy",
        "description": "Legacy: pairwise machine stop-correlation heatmap exploration.",
    },
}

CATEGORY_ORDER: dict[str, int] = {"Simple": 0, "Advanced": 1, "Legacy": 2}

SESSION_VERSION = 2
WORKFLOW_STEPS: list[tuple[str, list[str]]] = [
    (
        "Step 1: Health checks",
        [
            "machines_active_per_day",
            "analyze_missing_sequence_number",
            "missing_per_day_by_machine",
            "sampling_rate_analysis",
        ],
    ),
    ("Step 2: Raw inspection", ["data_pr_day"]),
    ("Step 3: Stop-focused inspection", ["find_stops"]),
    ("Step 4: Deeper exploratory analysis", ["data_visualizer", "data_analysis", "ml_analysis"]),
]
WORKFLOW_SCRIPT_ORDER: list[str] = [script for _, scripts in WORKFLOW_STEPS for script in scripts]


@dataclass(frozen=True)
class SessionInfo:
    """
    Summary of one workflow session directory.

    Attributes
    ----------
    session_id : str
        Stable session folder name.
    session_dir : pathlib.Path
        Absolute path to the session directory.
    metadata : dict[str, Any]
        Parsed session metadata payload.
    """

    session_id: str
    session_dir: Path
    metadata: dict[str, Any]


@dataclass(frozen=True)
class ScriptOption:
    """
    Description of one runnable catalog script.

    Attributes
    ----------
    number : int
        Display number used in the runner menu.
    key : str
        Script key, typically derived from the folder name.
    script_path : pathlib.Path
        Relative path to the script within the repository.
    description : str
        Short human-readable description, usually derived from the module docstring.
    category : ToolCategory
        Runner category label used for grouped presentation.
    """
    number: int
    key: str
    script_path: Path
    description: str
    category: ToolCategory


def repo_root() -> Path:
    """
    Return the repository root directory.

    Returns
    -------
    pathlib.Path
        Repository root based on the location of ``catalog/runner/menu_utils.py``.
    """
    return Path(__file__).resolve().parents[2]


def _script_description(script_path: Path, fallback: str) -> str:
    """
    Extract a short script description from the first line of a module docstring.

    Parameters
    ----------
    script_path : pathlib.Path
        Path to the candidate Python script.
    fallback : str
        Description used if no readable docstring is available.

    Returns
    -------
    str
        First line of the module docstring, or the fallback string.
    """
    try:
        source = script_path.read_text(encoding="utf-8")
        module = ast.parse(source)
        docstring = ast.get_docstring(module)
    except (OSError, SyntaxError, UnicodeDecodeError):
        docstring = None

    if not docstring:
        return fallback

    first_line = docstring.strip().splitlines()[0].strip()
    return first_line or fallback


def discover_runnable_scripts(catalog_dir: Path) -> list[ScriptOption]:
    """
    Discover runnable catalog scripts from catalog subdirectories.

    A folder is considered runnable when it contains either:
    - ``<folder_name>.py``, or
    - ``main.py``

    Parameters
    ----------
    catalog_dir : pathlib.Path
        Path to the top-level ``catalog/`` directory.

    Returns
    -------
    list[ScriptOption]
        Sorted list of runnable script definitions.

    Notes
    -----
    Certain folders are intentionally hidden via ``RUNNER_HIDDEN_FOLDERS`` to
    keep the runner focused on one-shot analysis scripts.
    """
    script_items: list[tuple[str, Path, str, ToolCategory]] = []

    for folder in sorted(catalog_dir.iterdir()):
        if not folder.is_dir():
            continue
        if folder.name in RUNNER_HIDDEN_FOLDERS:
            continue

        convention_script = folder / f"{folder.name}.py"
        main_script = folder / "main.py"

        selected_script: Path | None = None
        if convention_script.exists():
            selected_script = convention_script
            key = folder.name
        elif main_script.exists():
            selected_script = main_script
            key = folder.name
        else:
            continue

        metadata = SCRIPT_METADATA.get(key, {})
        fallback_description = key.replace("_", " ").replace("-", " ")
        description = str(metadata.get("description")) if metadata.get("description") else _script_description(
            selected_script, fallback_description
        )
        category = str(metadata.get("category", "Advanced"))
        if category not in CATEGORY_ORDER:
            category = "Advanced"
        script_items.append((key, selected_script.relative_to(repo_root()), description, category))

    script_items.sort(key=lambda item: (CATEGORY_ORDER[item[3]], item[0].lower()))
    return [
        ScriptOption(number=index, key=key, script_path=script_path, description=description, category=category)
        for index, (key, script_path, description, category) in enumerate(script_items, start=1)
    ]


def discover_available_dates(data_dir: Path) -> list[date]:
    """
    Discover all available dates present in the source dataset.

    Parameters
    ----------
    data_dir : pathlib.Path
        Root directory containing JSONL telemetry files.

    Returns
    -------
    list[date]
        Sorted list of available dates.

    Behavior
    --------
    - JSONL files are scanned recursively.
    - For unchanged files, previously cached date results are reused.
    - For new or changed files, dates are reparsed from the file.
    - Timestamp-derived dates are preferred.
    - Filename-based dates are used only when a file has no usable timestamp dates.

    Notes
    -----
    The cache is used only for date discovery, not for filtering records.
    """
    dates: set[date] = set()
    data_root = data_dir.resolve()
    index_data = _load_data_index()

    print(f"[runner] Loaded date cache from {DATA_INDEX_FILE}", flush=True)

    cached_root_entries = _load_cached_root_entries(index_data, data_root)
    updated_root_entries: dict[str, dict] = {}

    jsonl_files = list(iter_jsonl_files(data_dir, recursive=True))
    print(f"[runner] Found {len(jsonl_files)} JSONL files for date discovery", flush=True)

    reused_from_cache = 0
    reparsed_files = 0

    for file_path in jsonl_files:
        relative_path = file_path.resolve().relative_to(data_root).as_posix()
        stat_result = file_path.stat()
        file_size = stat_result.st_size
        file_mtime_ns = stat_result.st_mtime_ns
        cache_entry = cached_root_entries.get(relative_path)

        if (
            cache_entry is not None
            and cache_entry.get("size") == file_size
            and cache_entry.get("mtime_ns") == file_mtime_ns
        ):
            file_dates = _deserialize_dates(cache_entry.get("dates", []))
            date_source = str(cache_entry.get("date_source", "none"))
            has_timestamp_date = bool(cache_entry.get("has_timestamp_date", False))
            reused_from_cache += 1
        else:
            file_dates, date_source, has_timestamp_date = _discover_dates_for_file(file_path)
            reparsed_files += 1

        dates.update(file_dates)
        updated_root_entries[relative_path] = {
            "size": file_size,
            "mtime_ns": file_mtime_ns,
            "dates": _serialize_dates(file_dates),
            "date_source": date_source,
            "has_timestamp_date": has_timestamp_date,
        }

    print(
        f"[runner] Date discovery reused {reused_from_cache} cached files and reparsed {reparsed_files} files",
        flush=True,
    )

    _store_cached_root_entries(index_data, data_root, updated_root_entries)

    print(f"[runner] Writing date cache to {DATA_INDEX_FILE}", flush=True)
    _write_data_index(index_data)

    return sorted(dates)


def filter_data_by_date_range(
    source_data_dir: Path,
    destination_data_dir: Path,
    start_date: date,
    end_date: date,
    *,
    start_hour: int | None = None,
    end_hour: int | None = None,
) -> tuple[int, int]:
    """
    Filter JSONL records into a destination directory based on date or hour range.

    Parameters
    ----------
    source_data_dir : pathlib.Path
        Source directory containing JSONL files.
    destination_data_dir : pathlib.Path
        Destination directory for filtered JSONL files.
    start_date : date
        Inclusive start date.
    end_date : date
        Inclusive end date.
    start_hour : int | None, optional
        Inclusive start hour for same-day hour-filtered runs.
    end_hour : int | None, optional
        Inclusive end hour for same-day hour-filtered runs.

    Returns
    -------
    tuple[int, int]
        ``(matched_records, written_files)``

    Behavior
    --------
    Date mode:
    - records are included when their parsed date falls within the selected range
    - if a file has no parseable timestamp dates at all, filename-based date
      fallback may be used

    Hour mode:
    - enabled only when start/end date are the same and both hours are provided
    - records are included only if their parsed timestamp falls within the hour range
    - filename-only fallback is not used in hour mode

    Notes
    -----
    The full file content is read during filtering. Cache-based skipping is
    intentionally not used here for safety.
    """
    destination_data_dir.mkdir(parents=True, exist_ok=True)
    use_hour_filter = start_date == end_date and start_hour is not None and end_hour is not None

    matched_records = 0
    written_files = 0
    processed_files = 0

    source_files = list(iter_jsonl_files(source_data_dir, recursive=True))
    print(
        f"[runner] Filtering {len(source_files)} files for range {start_date.isoformat()}..{end_date.isoformat()}",
        flush=True,
    )

    for source_file in source_files:
        relative_path = source_file.relative_to(source_data_dir)
        destination_file = destination_data_dir / relative_path
        destination_file.parent.mkdir(parents=True, exist_ok=True)

        file_matched = 0
        fallback_file_date = date_from_filename(source_file)
        file_in_fallback_window = fallback_file_date is not None and start_date <= fallback_file_date <= end_date

        parsed_records: list[dict] = []
        file_has_timestamp = False

        for record in iter_jsonl_records(source_file):
            parsed_records.append(record)
            if parse_timestamp_to_date(str(record.get("timestamp", ""))) is not None:
                file_has_timestamp = True

        with destination_file.open("w", encoding="utf-8") as dst:
            for record in parsed_records:
                if use_hour_filter:
                    record_dt = _parse_timestamp_to_datetime(str(record.get("timestamp", "")))
                    if record_dt is None:
                        continue
                    if record_dt.date() != start_date:
                        continue
                    if start_hour <= record_dt.hour <= end_hour:
                        dst.write(json.dumps(record, ensure_ascii=False) + "\n")
                        matched_records += 1
                        file_matched += 1
                    continue

                record_date = parse_timestamp_to_date(str(record.get("timestamp", "")))
                if record_date is None:
                    if not file_has_timestamp and file_in_fallback_window:
                        dst.write(json.dumps(record, ensure_ascii=False) + "\n")
                        matched_records += 1
                        file_matched += 1
                    continue

                if start_date <= record_date <= end_date:
                    dst.write(json.dumps(record, ensure_ascii=False) + "\n")
                    matched_records += 1
                    file_matched += 1

        if file_matched > 0:
            written_files += 1
        else:
            destination_file.unlink(missing_ok=True)

        processed_files += 1
        if processed_files % 25 == 0:
            print(
                f"[runner] Filter progress: processed {processed_files}/{len(source_files)} files "
                f"(matched files: {written_files}, matched records: {matched_records})",
                flush=True,
            )

    print(
        f"[runner] Filter complete: processed {processed_files} files, wrote {written_files} files, "
        f"matched {matched_records} records",
        flush=True,
    )

    return matched_records, written_files


def _parse_timestamp_to_datetime(timestamp: str | None) -> datetime | None:
    """
    Parse a timestamp string into a datetime for hour-level filtering.

    Parameters
    ----------
    timestamp : str | None
        Input timestamp string.

    Returns
    -------
    datetime | None
        Parsed datetime if successful, otherwise None.

    Notes
    -----
    This helper first tries the shared ISO parser with ``allow_z_suffix=True``.
    If that fails, it falls back to extracting a basic date/time pattern from
    MTConnect-style strings.
    """
    parsed = parse_iso_timestamp(timestamp, allow_z_suffix=True)
    if parsed is not None:
        return parsed

    value = str(timestamp).strip() if timestamp is not None else ""
    match = re.search(r"(\d{4}-\d{2}-\d{2})[T ](\d{2}):(\d{2}):(\d{2})", value)
    if not match:
        return None

    try:
        return datetime(
            year=int(match.group(1)[0:4]),
            month=int(match.group(1)[5:7]),
            day=int(match.group(1)[8:10]),
            hour=int(match.group(2)),
            minute=int(match.group(3)),
            second=int(match.group(4)),
        )
    except ValueError:
        return None


def print_numbered_menu(title: str, options: Iterable[str]) -> None:
    """
    Print a numbered menu to stdout.

    Parameters
    ----------
    title : str
        Menu heading.
    options : Iterable[str]
        Menu option labels.
    """
    print(f"\n{title}", flush=True)
    for index, option in enumerate(options, start=1):
        print(f"{index}) {option}", flush=True)


def prompt_menu_choice(max_choice: int, prompt: str) -> int:
    """
    Prompt the user for a numeric menu choice.

    Parameters
    ----------
    max_choice : int
        Maximum valid menu value.
    prompt : str
        Prompt shown to the user.

    Returns
    -------
    int
        Validated menu choice in the inclusive range ``1..max_choice``.
    """
    while True:
        raw = input(prompt).strip()
        if not raw.isdigit():
            print("Please enter a number.", flush=True)
            continue

        value = int(raw)
        if 1 <= value <= max_choice:
            return value

        print(f"Please choose a value between 1 and {max_choice}.", flush=True)


def create_run_workspace(output_base_dir: Path) -> Path:
    """
    Create a temporary workspace directory for one runner execution.

    Parameters
    ----------
    output_base_dir : pathlib.Path
        Base directory under which run workspaces are created.

    Returns
    -------
    pathlib.Path
        Newly created workspace path.
    """
    output_base_dir.mkdir(parents=True, exist_ok=True)
    path = Path(mkdtemp(prefix="menu_run_", dir=output_base_dir))
    return path


def workflow_step_for_script(script_key: str) -> str | None:
    """
    Resolve the workflow step label for a script key.
    """
    for step_name, scripts in WORKFLOW_STEPS:
        if script_key in scripts:
            return step_name
    return None


def workflow_step_status(session_metadata: dict[str, Any], step_scripts: list[str]) -> str:
    """
    Derive step status from script-level statuses.
    """
    statuses = [str(session_metadata.get("scripts", {}).get(key, {}).get("status", "not_run")) for key in step_scripts]
    if statuses and all(value == "done" for value in statuses):
        return "complete"
    if any(value == "failed" for value in statuses):
        return "failed"
    if any(value == "done" for value in statuses):
        return "partial"
    return "not_run"


def list_sessions(workflows_root: Path) -> list[SessionInfo]:
    """
    List known workflow sessions sorted by creation time descending.
    """
    if not workflows_root.exists():
        return []

    sessions: list[SessionInfo] = []
    for item in sorted(workflows_root.iterdir(), key=lambda path: path.name, reverse=True):
        if not item.is_dir():
            continue
        metadata_path = item / "session.json"
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
    script_options: list[ScriptOption],
) -> dict[str, Any]:
    """
    Build a new session metadata payload.
    """
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
    session_config_signature = hashlib.sha256(
        json.dumps(filter_payload, sort_keys=True).encode("utf-8")
    ).hexdigest()[:12]
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
        },
        "filter_result": {
            "matched_records": None,
            "matched_files": None,
            "generated_at": None,
        },
        "scripts": scripts,
    }


def write_session_metadata(session_dir: Path, metadata: dict[str, Any]) -> None:
    """
    Persist session metadata atomically.
    """
    metadata["updated_at"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    target = session_dir / "session.json"
    with NamedTemporaryFile("w", encoding="utf-8", dir=session_dir, delete=False) as tmp:
        json.dump(metadata, tmp, ensure_ascii=False, indent=2, sort_keys=True)
        tmp.write("\n")
        temp_path = Path(tmp.name)
    temp_path.replace(target)


def ensure_session_filtered_data(
    *,
    source_data_dir: Path,
    session_dir: Path,
    metadata: dict[str, Any],
) -> tuple[int, int, str]:
    """
    Ensure the session-scoped filtered dataset exists, creating it once.
    """
    filtered_data_dir = session_dir / str(metadata["paths"]["filtered_data_dir"])
    filter_result = metadata.setdefault("filter_result", {})
    if (
        filtered_data_dir.exists()
        and filter_result.get("matched_records") is not None
        and filter_result.get("matched_files") is not None
    ):
        return int(filter_result["matched_records"]), int(filter_result["matched_files"]), "cached"

    if filtered_data_dir.exists():
        shutil.rmtree(filtered_data_dir)

    filter_config = metadata["filter"]
    matched_records, matched_files = filter_data_by_date_range(
        source_data_dir,
        filtered_data_dir,
        date.fromisoformat(str(filter_config["start_date"])),
        date.fromisoformat(str(filter_config["end_date"])),
        start_hour=int(filter_config["start_hour"]) if filter_config.get("start_hour") is not None else None,
        end_hour=int(filter_config["end_hour"]) if filter_config.get("end_hour") is not None else None,
    )
    filter_result["matched_records"] = matched_records
    filter_result["matched_files"] = matched_files
    filter_result["generated_at"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    write_session_metadata(session_dir, metadata)
    return matched_records, matched_files, "created"


def execute_script_for_session(
    *,
    session_dir: Path,
    metadata: dict[str, Any],
    script: ScriptOption,
    force_rerun: bool = False,
) -> tuple[str, int | None]:
    """
    Execute one script in the session and update script-level status.
    """
    script_entry = metadata.get("scripts", {}).get(script.key)
    if script_entry is None:
        return "not_tracked", None

    if script_entry.get("status") == "done" and not force_rerun:
        return "skipped_cached", int(script_entry["exit_code"]) if script_entry.get("exit_code") is not None else 0

    runs_dir = session_dir / str(metadata["paths"]["runs_dir"])
    runs_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    run_dir = runs_dir / script.key / timestamp
    run_dir.mkdir(parents=True, exist_ok=False)

    copy_repo_catalog_into_workspace(run_dir)

    session_data_dir = session_dir / str(metadata["paths"]["filtered_data_dir"])
    run_data_dir = run_dir / "data"
    try:
        run_data_dir.symlink_to(session_data_dir, target_is_directory=True)
    except OSError:
        shutil.copytree(session_data_dir, run_data_dir)

    script_to_run = run_dir / script.script_path
    started = perf_counter()
    exit_code = run_script(script_to_run, run_dir)
    duration_seconds = round(perf_counter() - started, 3)

    previous_status = str(script_entry.get("status", "not_run"))
    script_entry["status"] = "done" if exit_code == 0 else "failed"
    script_entry["output_path"] = run_dir.relative_to(session_dir).as_posix()
    script_entry["last_run_at"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    script_entry["duration_seconds"] = duration_seconds
    script_entry["exit_code"] = exit_code
    write_session_metadata(session_dir, metadata)
    if force_rerun and previous_status == "done":
        return "reran", exit_code
    return "ran", exit_code


def run_script(script_path: Path, workspace_dir: Path) -> int:
    """
    Execute a selected catalog script inside a workspace directory.

    Parameters
    ----------
    script_path : pathlib.Path
        Path to the script to execute.
    workspace_dir : pathlib.Path
        Working directory for the subprocess.

    Returns
    -------
    int
        Subprocess return code.

    Notes
    -----
    The subprocess runs with:
    - ``PYTHONUNBUFFERED=1`` for immediate output visibility
    - ``MPLBACKEND=Agg`` by default to avoid interactive Matplotlib requirements
    """
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    env.setdefault("MPLBACKEND", "Agg")

    command = [sys.executable, str(script_path)]
    print(f"\nRunning: {' '.join(command)}", flush=True)
    print(f"Working directory: {workspace_dir}", flush=True)

    completed = subprocess.run(command, cwd=workspace_dir, env=env)
    return completed.returncode


def copy_repo_catalog_into_workspace(workspace_dir: Path) -> None:
    """
    Copy the repository's ``catalog/`` directory into a run workspace.

    Parameters
    ----------
    workspace_dir : pathlib.Path
        Workspace receiving the copied catalog tree.
    """
    source_catalog = repo_root() / "catalog"
    target_catalog = workspace_dir / "catalog"

    if target_catalog.exists():
        shutil.rmtree(target_catalog)

    shutil.copytree(source_catalog, target_catalog)


def _discover_dates_for_file(file_path: Path) -> tuple[set[date], str, bool]:
    """
    Discover all dates represented by one JSONL file.

    Parameters
    ----------
    file_path : pathlib.Path
        Source JSONL file.

    Returns
    -------
    tuple[set[date], str, bool]
        ``(dates, date_source, has_timestamp_date)`` where:
        - ``dates`` is the set of discovered dates
        - ``date_source`` is ``"timestamp"``, ``"filename_fallback"``, or ``"none"``
        - ``has_timestamp_date`` indicates whether any date came from timestamps

    Behavior
    --------
    - timestamp-derived dates are preferred
    - filename fallback is used only when no timestamp-derived date exists
    """
    file_dates: set[date] = set()
    file_has_timestamp_date = False

    for record in iter_jsonl_records(file_path):
        record_date = parse_timestamp_to_date(str(record.get("timestamp", "")))
        if record_date is not None:
            file_has_timestamp_date = True
            file_dates.add(record_date)

    if file_has_timestamp_date:
        return file_dates, "timestamp", True

    fallback = date_from_filename(file_path)
    if fallback is not None:
        return {fallback}, "filename_fallback", False

    return set(), "none", False


def _serialize_dates(dates: set[date]) -> list[str]:
    """
    Convert a set of dates into sorted ISO strings for JSON cache storage.
    """
    return sorted(day.isoformat() for day in dates)


def _deserialize_dates(serialized_dates: object) -> set[date]:
    """
    Convert cached ISO date strings back into ``date`` objects.

    Parameters
    ----------
    serialized_dates : object
        JSON-decoded value expected to contain a list of ISO-format date strings.

    Returns
    -------
    set[date]
        Parsed dates. Invalid entries are ignored.
    """
    parsed_dates: set[date] = set()
    if not isinstance(serialized_dates, list):
        return parsed_dates

    for value in serialized_dates:
        if not isinstance(value, str):
            continue
        try:
            parsed_dates.add(date.fromisoformat(value))
        except ValueError:
            continue

    return parsed_dates


def _load_data_index() -> dict:
    """
    Load the runner date-discovery cache from disk.

    Returns
    -------
    dict
        Parsed cache structure, or a clean empty structure if the cache is
        missing, malformed, or version-incompatible.

    Notes
    -----
    Cache structure:

    {
      "version": int,
      "roots": {
        "<absolute_data_root>": {
          "files": {
            "<relative_path>": {
              "size": int,
              "mtime_ns": int,
              "dates": ["YYYY-MM-DD", ...],
              "date_source": "timestamp" | "filename_fallback" | "none",
              "has_timestamp_date": bool
            }
          }
        }
      }
    }
    """
    if not DATA_INDEX_FILE.exists():
        return {"version": DATA_INDEX_VERSION, "roots": {}}

    try:
        parsed = json.loads(DATA_INDEX_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"version": DATA_INDEX_VERSION, "roots": {}}

    if not isinstance(parsed, dict):
        return {"version": DATA_INDEX_VERSION, "roots": {}}
    if parsed.get("version") != DATA_INDEX_VERSION:
        return {"version": DATA_INDEX_VERSION, "roots": {}}
    if not isinstance(parsed.get("roots"), dict):
        return {"version": DATA_INDEX_VERSION, "roots": {}}

    return parsed


def _load_cached_root_entries(index_data: dict, data_root: Path) -> dict[str, dict]:
    """
    Extract cached file entries for one data root.

    Parameters
    ----------
    index_data : dict
        Loaded cache structure.
    data_root : pathlib.Path
        Absolute root directory used as the cache key.

    Returns
    -------
    dict[str, dict]
        Mapping from relative file path to cached metadata.
    """
    roots = index_data.get("roots")
    if not isinstance(roots, dict):
        return {}

    root_entry = roots.get(str(data_root))
    if not isinstance(root_entry, dict):
        return {}

    files = root_entry.get("files")
    if not isinstance(files, dict):
        return {}

    return {key: value for key, value in files.items() if isinstance(key, str) and isinstance(value, dict)}


def _store_cached_root_entries(index_data: dict, data_root: Path, file_entries: dict[str, dict]) -> None:
    """
    Store updated cached file entries for one data root.

    Parameters
    ----------
    index_data : dict
        Mutable cache structure.
    data_root : pathlib.Path
        Absolute root directory used as the cache key.
    file_entries : dict[str, dict]
        Updated per-file cache metadata.
    """
    roots = index_data.setdefault("roots", {})
    if not isinstance(roots, dict):
        index_data["roots"] = {}
        roots = index_data["roots"]

    roots[str(data_root)] = {"files": file_entries}


def _write_data_index(index_data: dict) -> None:
    """
    Write the runner date-discovery cache to disk atomically.

    Parameters
    ----------
    index_data : dict
        Cache structure to persist.

    Notes
    -----
    The cache is written via a temporary file and then replaced atomically to
    reduce the chance of corruption.
    """
    DATA_INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=DATA_INDEX_FILE.parent, delete=False) as tmp:
        json.dump(index_data, tmp, ensure_ascii=False, indent=2, sort_keys=True)
        tmp.write("\n")
        temp_path = Path(tmp.name)

    temp_path.replace(DATA_INDEX_FILE)
