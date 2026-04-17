"""
Interactive runner for catalog analysis scripts with session-based caching.

This runner keeps workflow guidance at the step level while tracking and
caching execution at the script level. Each session stores:

- selected date range (and optional hour window)
- one filtered dataset reused across script runs
- per-script status and output metadata
- derived workflow-step progress

Sessions are file-based under ``results/workflows/<session-id>/``.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from catalog.runner.data_filtering import discover_available_dates, ensure_session_filtered_data
from catalog.runner.script_catalog import ScriptOption, discover_runnable_scripts, repo_root
from catalog.runner.script_exec import copy_repo_catalog_into_workspace, execute_script_for_session
from catalog.runner.session_store import (
    WORKFLOW_SCRIPT_ORDER,
    WORKFLOW_STEPS,
    initialize_session_metadata,
    list_sessions,
    normalize_session_metadata,
    workflow_step_status,
    write_session_metadata,
)
from catalog.runner.ui import print_numbered_menu, prompt_menu_choice


def pick_date_range(available_dates):
    labels = [d.isoformat() for d in available_dates]
    print_numbered_menu("\nAvailable data dates:", labels)
    start_index = prompt_menu_choice(len(labels), "Choose start date number: ") - 1

    while True:
        end_index = prompt_menu_choice(len(labels), "Choose end date number: ") - 1
        if end_index < start_index:
            print("End date must be on or after start date.", flush=True)
            continue
        return available_dates[start_index], available_dates[end_index]


def pick_hour_range() -> tuple[int, int]:
    while True:
        raw_start = input("Choose start hour (0-23): ").strip()
        if raw_start.isdigit() and 0 <= int(raw_start) <= 23:
            start_hour = int(raw_start)
            break
        print("Please choose a number between 0 and 23.", flush=True)

    while True:
        raw_end = input("Choose end hour (0-23): ").strip()
        if not raw_end.isdigit():
            print("Please enter a number.", flush=True)
            continue
        end_hour = int(raw_end)
        if not 0 <= end_hour <= 23:
            print("Please choose a number between 0 and 23.", flush=True)
            continue
        if end_hour < start_hour:
            print("End hour must be on or after start hour.", flush=True)
            continue
        return start_hour, end_hour


def should_limit_by_hour() -> bool:
    print("\nSelected a single-day run.", flush=True)
    while True:
        answer = input("Limit this session to a specific hour range? (y/n): ").strip().lower()
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Please type y or n.", flush=True)


def summarize_session(session_id: str, metadata: dict) -> None:
    print(f"\nSession: {session_id}", flush=True)
    filter_cfg = metadata.get("filter", {})
    print(
        f"Date range: {filter_cfg.get('start_date')}..{filter_cfg.get('end_date')}",
        flush=True,
    )
    if filter_cfg.get("start_hour") is not None and filter_cfg.get("end_hour") is not None:
        print(
            f"Hour range: {int(filter_cfg['start_hour']):02d}:00-{int(filter_cfg['end_hour']):02d}:59",
            flush=True,
        )

    filter_result = metadata.get("filter_result", {})
    print(
        f"Filtered data: records={filter_result.get('matched_records')}, files={filter_result.get('matched_files')}",
        flush=True,
    )
    if metadata.get("session_config_signature"):
        print(f"Session config signature: {metadata['session_config_signature']}", flush=True)

    print("\nWorkflow progress:", flush=True)
    for step_name, step_scripts in WORKFLOW_STEPS:
        step_state = workflow_step_status(metadata, step_scripts)
        marker = {
            "complete": "✔ done",
            "partial": "◐ partial",
            "failed": "✖ failed",
            "not_run": "○ not run",
        }.get(step_state, "○ not run")
        print(f"  {step_name:<32} [{marker}]", flush=True)
        for script_key in step_scripts:
            entry = metadata.get("scripts", {}).get(script_key, {})
            status = entry.get("status", "not_run")
            last_run_at = entry.get("last_run_at")
            duration_seconds = entry.get("duration_seconds")
            output_path = entry.get("output_path")
            details = []
            if last_run_at:
                details.append(f"last_run_at={last_run_at}")
            if duration_seconds is not None:
                details.append(f"duration={duration_seconds}s")
            if output_path:
                details.append(f"output={output_path}")
            suffix = f" ({', '.join(details)})" if details else ""
            print(f"      - {script_key}: {status}{suffix}", flush=True)

    legacy_entries = [
        key
        for key, value in metadata.get("scripts", {}).items()
        if value.get("workflow_step") is None
    ]
    if legacy_entries:
        print("\nLegacy / outside workflow:", flush=True)
        for key in sorted(legacy_entries):
            status = metadata["scripts"][key].get("status", "not_run")
            print(f"  - {key}: {status}", flush=True)


def pick_script(script_options: list[ScriptOption], *, include_legacy: bool = True) -> ScriptOption:
    options = [item for item in script_options if include_legacy or item.key in WORKFLOW_SCRIPT_ORDER]
    print("\nSelect a script:", flush=True)
    current_category = None
    for idx, item in enumerate(options, start=1):
        if item.category != current_category:
            current_category = item.category
            print(f"\n[{current_category}]", flush=True)
        print(f"{idx}) {item.key} — {item.description}", flush=True)
    choice = prompt_menu_choice(len(options), f"\nEnter script number (1-{len(options)}): ")
    return options[choice - 1]


def next_workflow_step_to_run(metadata: dict) -> tuple[str, list[str]] | None:
    for step_name, step_scripts in WORKFLOW_STEPS:
        status = workflow_step_status(metadata, step_scripts)
        if status != "complete":
            return step_name, step_scripts
    return None


def run_script_batch(
    *,
    script_keys: list[str],
    script_options: list[ScriptOption],
    session_dir: Path,
    metadata: dict,
    force_rerun: bool,
    stop_on_failure: bool,
) -> bool:
    script_index = {item.key: item for item in script_options}
    for key in script_keys:
        script = script_index.get(key)
        if script is None:
            print(f"Skipping unknown script '{key}'.", flush=True)
            continue
        print(f"\n=== Running {script.key} ===", flush=True)
        state, exit_code = execute_script_for_session(
            session_dir=session_dir,
            metadata=metadata,
            script=script,
            force_rerun=force_rerun,
        )
        if state == "skipped_cached":
            print(f"Skipped {script.key}: already done (cached).", flush=True)
            continue
        if exit_code == 0:
            if state == "reran":
                print(f"Completed {script.key} (recomputed).", flush=True)
            else:
                print(f"Completed {script.key}.", flush=True)
        else:
            print(f"Failed {script.key} with exit code {exit_code}.", flush=True)
            if stop_on_failure:
                print("Stopping this batch on first failure.", flush=True)
                return False
    return True


def choose_or_create_session(
    *,
    workflows_root: Path,
    available_dates,
    script_options: list[ScriptOption],
) -> tuple[str, Path, dict]:
    sessions = list_sessions(workflows_root)
    print("\nSession options", flush=True)
    print("1) Create new session", flush=True)
    if sessions:
        print("2) Reuse existing session", flush=True)
        choice = prompt_menu_choice(2, "Choose option: ")
    else:
        choice = 1

    if choice == 2:
        labels = []
        for item in sessions:
            filter_cfg = item.metadata.get("filter", {})
            workflow_keys = set(WORKFLOW_SCRIPT_ORDER)
            completed_scripts = sum(
                1
                for key, value in item.metadata.get("scripts", {}).items()
                if key in workflow_keys and value.get("status") == "done"
            )
            updated_at = item.metadata.get("updated_at", "unknown")
            labels.append(
                f"{item.session_id} ({filter_cfg.get('start_date')}..{filter_cfg.get('end_date')}, "
                f"completed={completed_scripts}/{len(WORKFLOW_SCRIPT_ORDER)}, updated={updated_at})"
            )
        print_numbered_menu("\nAvailable sessions:", labels)
        selected = sessions[prompt_menu_choice(len(sessions), "Choose session number: ") - 1]
        normalized, changed = normalize_session_metadata(
            selected.session_dir,
            selected.metadata,
            script_options,
        )
        if changed:
            write_session_metadata(selected.session_dir, normalized)
        return selected.session_id, selected.session_dir, normalized

    start_date, end_date = pick_date_range(available_dates)
    hour_range: tuple[int, int] | None = None
    if start_date == end_date and should_limit_by_hour():
        hour_range = pick_hour_range()

    session_stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    session_id = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{session_stamp}"
    if hour_range is not None:
        session_id = (
            f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_"
            f"h{hour_range[0]:02d}-{hour_range[1]:02d}_{session_stamp}"
        )

    session_dir = workflows_root / session_id
    session_dir.mkdir(parents=True, exist_ok=False)
    metadata = initialize_session_metadata(
        session_id,
        start_date,
        end_date,
        start_hour=hour_range[0] if hour_range is not None else None,
        end_hour=hour_range[1] if hour_range is not None else None,
        script_options=script_options,
    )
    write_session_metadata(session_dir, metadata)
    return session_id, session_dir, metadata


def main() -> int:
    print("MSH session runner started", flush=True)
    print("Workflow guidance is step-based; execution cache is script-based per session.", flush=True)

    root = repo_root()
    catalog_dir = root / "catalog"
    data_dir = root / "data"
    workflows_root = root / "results" / "workflows"

    script_options = discover_runnable_scripts(catalog_dir)
    if not script_options:
        print("No runnable scripts found in catalog/.", flush=True)
        return 1

    if not data_dir.exists():
        print(f"Data directory not found: {data_dir}", flush=True)
        return 1

    available_dates = discover_available_dates(data_dir)
    if not available_dates:
        print("No dates discovered in data/.", flush=True)
        return 1

    session_id, session_dir, metadata = choose_or_create_session(
        workflows_root=workflows_root,
        available_dates=available_dates,
        script_options=script_options,
    )

    print(f"\nUsing session: {session_id}", flush=True)
    matched_records, matched_files, filter_status = ensure_session_filtered_data(
        source_data_dir=data_dir,
        session_dir=session_dir,
        metadata=metadata,
    )
    if matched_records == 0:
        print("No records found for this session filter. Nothing to run.", flush=True)
        return 0

    if filter_status == "cached":
        print(f"Using cached filtered dataset ({matched_records} records).", flush=True)
    else:
        print(f"Created filtered dataset ({matched_records} records across {matched_files} files).", flush=True)
    print(f"Session filtered data path: {session_dir / 'data'}", flush=True)
    copy_repo_catalog_into_workspace(session_dir)

    while True:
        summarize_session(session_id, metadata)
        print("\nActions", flush=True)
        print("1) Run next workflow step", flush=True)
        print("2) Run selected workflow step", flush=True)
        print("3) Run selected script", flush=True)
        print("4) Precompute workflow (Steps 1-4)", flush=True)
        print("5) Precompute workflow up to a selected step", flush=True)
        print("6) Show session status", flush=True)
        print("7) Exit", flush=True)

        action = prompt_menu_choice(7, "Choose action: ")

        if action == 1:
            next_step = next_workflow_step_to_run(metadata)
            if next_step is None:
                print("All workflow steps are complete.", flush=True)
                continue
            step_name, step_scripts = next_step
            print(f"Running next step: {step_name}", flush=True)
            run_script_batch(
                script_keys=step_scripts,
                script_options=script_options,
                session_dir=session_dir,
                metadata=metadata,
                force_rerun=False,
                stop_on_failure=True,
            )
            continue

        if action == 2:
            labels = [name for name, _ in WORKFLOW_STEPS]
            print_numbered_menu("\nWorkflow steps:", labels)
            picked_step = WORKFLOW_STEPS[prompt_menu_choice(len(WORKFLOW_STEPS), "Choose step: ") - 1]
            rerun = input("Rerun completed scripts in this step? (y/n): ").strip().lower() in {"y", "yes"}
            run_script_batch(
                script_keys=picked_step[1],
                script_options=script_options,
                session_dir=session_dir,
                metadata=metadata,
                force_rerun=rerun,
                stop_on_failure=True,
            )
            continue

        if action == 3:
            selected = pick_script(script_options, include_legacy=True)
            rerun = input("Rerun if already completed? (y/n): ").strip().lower() in {"y", "yes"}
            run_script_batch(
                script_keys=[selected.key],
                script_options=script_options,
                session_dir=session_dir,
                metadata=metadata,
                force_rerun=rerun,
                stop_on_failure=False,
            )
            continue

        if action == 4:
            rerun = input("Rerun scripts already completed? (y/n): ").strip().lower() in {"y", "yes"}
            run_script_batch(
                script_keys=WORKFLOW_SCRIPT_ORDER,
                script_options=script_options,
                session_dir=session_dir,
                metadata=metadata,
                force_rerun=rerun,
                stop_on_failure=True,
            )
            continue

        if action == 5:
            labels = [name for name, _ in WORKFLOW_STEPS]
            print_numbered_menu("\nPrecompute up to step:", labels)
            max_step_index = prompt_menu_choice(len(WORKFLOW_STEPS), "Choose step: ")
            picked: list[str] = []
            for idx, (_, step_scripts) in enumerate(WORKFLOW_STEPS, start=1):
                if idx <= max_step_index:
                    picked.extend(step_scripts)
            rerun = input("Rerun scripts already completed? (y/n): ").strip().lower() in {"y", "yes"}
            run_script_batch(
                script_keys=picked,
                script_options=script_options,
                session_dir=session_dir,
                metadata=metadata,
                force_rerun=rerun,
                stop_on_failure=True,
            )
            continue

        if action == 6:
            continue

        if action == 7:
            print(f"Session saved: {session_dir}", flush=True)
            return 0


if __name__ == "__main__":
    sys.exit(main())
