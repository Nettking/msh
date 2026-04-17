"""
Interactive runner for catalog analysis scripts.

This script provides a simple command-line interface for selecting one catalog
script, choosing a date range from the available telemetry data, optionally
narrowing a single-day run by hour, and then executing the selected script in
an isolated workspace containing only the filtered data.

Workflow
--------
1. Discover runnable catalog scripts
2. Discover available dates from telemetry data
3. Prompt the user to choose:
   - one script
   - a start and end date
   - optionally, an hour range for same-day runs
4. Filter the source dataset into a temporary run workspace
5. Copy the catalog code into that workspace
6. Run the selected script against the filtered data
7. Report the output directory for inspection

Notes
-----
- Multi-day runs use date-only filtering.
- Same-day runs may optionally use whole-hour filtering.
- The runner is intended for one-shot interactive use, not as a background service.
- Each run uses its own workspace under ``results/menu_runs/``.
"""

from __future__ import annotations

import sys

from menu_utils import (
    ScriptOption,
    copy_repo_catalog_into_workspace,
    create_run_workspace,
    discover_available_dates,
    discover_runnable_scripts,
    filter_data_by_date_range,
    print_numbered_menu,
    prompt_menu_choice,
    repo_root,
    run_script,
)


def pick_script(script_options: list[ScriptOption]) -> ScriptOption:
    """
    Prompt the user to choose one runnable catalog script.

    Parameters
    ----------
    script_options : list[ScriptOption]
        Discovered runnable script definitions.

    Returns
    -------
    ScriptOption
        The selected script entry.
    """
    labels = [f"{item.key} — {item.description}" for item in script_options]
    print_numbered_menu("Select a script to run:", labels)
    choice = prompt_menu_choice(len(script_options), f"Enter script number (1-{len(script_options)}): ")
    return script_options[choice - 1]


def pick_date_range(available_dates):
    """
    Prompt the user to choose a start and end date from discovered data dates.

    Parameters
    ----------
    available_dates : list[date]
        Sorted list of dates available in the dataset.

    Returns
    -------
    tuple[date, date]
        Selected `(start_date, end_date)` pair.

    Notes
    -----
    The end date must not be earlier than the start date.
    """
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
    """
    Prompt the user to choose a whole-hour range for a same-day run.

    Returns
    -------
    tuple[int, int]
        Selected `(start_hour, end_hour)` where both values are in ``0..23``.

    Notes
    -----
    The end hour must not be earlier than the start hour.
    """
    while True:
        raw_start = input("Choose start hour (0-23): ").strip()
        if not raw_start.isdigit():
            print("Please enter a number.", flush=True)
            continue

        start_hour = int(raw_start)
        if 0 <= start_hour <= 23:
            break

        print("Please choose a value between 0 and 23.", flush=True)

    while True:
        raw_end = input("Choose end hour (0-23): ").strip()
        if not raw_end.isdigit():
            print("Please enter a number.", flush=True)
            continue

        end_hour = int(raw_end)
        if not 0 <= end_hour <= 23:
            print("Please choose a value between 0 and 23.", flush=True)
            continue

        if end_hour < start_hour:
            print("End hour must be on or after start hour.", flush=True)
            continue

        return start_hour, end_hour


def should_limit_by_hour() -> bool:
    """
    Ask whether a same-day run should be narrowed to a specific hour range.

    Returns
    -------
    bool
        True if the user wants hour filtering, otherwise False.
    """
    print("\nSelected a single-day run.", flush=True)
    while True:
        answer = input("Limit this run to a specific hour range? (y/n): ").strip().lower()
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Please type y or n.", flush=True)


def confirm_or_exit(
    script: ScriptOption,
    start_date,
    end_date,
    hour_range: tuple[int, int] | None = None,
) -> None:
    """
    Print a short run summary and ask the user for final confirmation.

    Parameters
    ----------
    script : ScriptOption
        Selected script definition.
    start_date : date
        Start date for filtering.
    end_date : date
        End date for filtering.
    hour_range : tuple[int, int] | None, optional
        Optional whole-hour range used only for same-day runs.
    """
    print("\nSelection summary", flush=True)
    print(f"Script: {script.key}", flush=True)
    print(f"Start date: {start_date.isoformat()}", flush=True)
    print(f"End date: {end_date.isoformat()}", flush=True)

    if hour_range is not None:
        print(f"Hour range: {hour_range[0]:02d}:00-{hour_range[1]:02d}:59", flush=True)

    while True:
        answer = input("Run now? (y/n): ").strip().lower()
        if answer in {"y", "yes"}:
            return
        if answer in {"n", "no"}:
            print("Cancelled.", flush=True)
            raise SystemExit(0)
        print("Please type y or n.", flush=True)


def main() -> int:
    """
    Run the interactive catalog workflow.

    Returns
    -------
    int
        Process exit code:
        - 0 for successful completion or no matching records
        - non-zero if setup or script execution fails
    """
    print("MSH interactive runner started", flush=True)

    root = repo_root()
    catalog_dir = root / "catalog"
    data_dir = root / "data"
    output_base_dir = root / "results" / "menu_runs"

    script_options = discover_runnable_scripts(catalog_dir)
    if not script_options:
        print("No runnable scripts found in catalog/.", flush=True)
        return 1

    if not data_dir.exists():
        print(f"Data directory not found: {data_dir}", flush=True)
        print("Tip: start the project with Docker Compose so data and results are mounted automatically.", flush=True)
        print("Run: docker compose run --rm msh", flush=True)
        return 1

    available_dates = discover_available_dates(data_dir)
    if not available_dates:
        print(
            "No dates discovered in data/. Ensure records include timestamps or filenames include YYYY-MM-DD / YYYYMMDD.",
            flush=True,
        )
        return 1

    script = pick_script(script_options)
    start_date, end_date = pick_date_range(available_dates)

    hour_range: tuple[int, int] | None = None
    if start_date == end_date and should_limit_by_hour():
        hour_range = pick_hour_range()

    confirm_or_exit(script, start_date, end_date, hour_range)

    workspace = create_run_workspace(output_base_dir)
    filtered_data_dir = workspace / "data"

    matched_records, matched_files = filter_data_by_date_range(
        data_dir,
        filtered_data_dir,
        start_date,
        end_date,
        start_hour=hour_range[0] if hour_range is not None else None,
        end_hour=hour_range[1] if hour_range is not None else None,
    )

    if matched_records == 0:
        print("\nNo records found in selected date range. Nothing to run.", flush=True)
        print(f"Filtered data path: {filtered_data_dir}", flush=True)
        return 0

    # Copy the catalog code into the run workspace so the selected script can be
    # executed against the filtered dataset in isolation from the source tree.
    copy_repo_catalog_into_workspace(workspace)

    script_to_run = workspace / script.script_path

    print("\nRun configuration", flush=True)
    print(f"Selected script: {script.key}", flush=True)
    print(f"Script path: {script.script_path}", flush=True)
    print(f"Selected start date: {start_date.isoformat()}", flush=True)
    print(f"Selected end date: {end_date.isoformat()}", flush=True)

    if hour_range is not None:
        print(f"Selected hour range: {hour_range[0]:02d}:00-{hour_range[1]:02d}:59", flush=True)

    print(f"Filtered dataset path: {filtered_data_dir}", flush=True)
    print(f"Matched records: {matched_records}", flush=True)
    print(f"Matched files: {matched_files}", flush=True)
    print(f"Outputs will be written under: {workspace}", flush=True)

    exit_code = run_script(script_to_run, workspace)

    if exit_code == 0:
        print("\nScript completed successfully.", flush=True)
        print(f"Run output directory: {workspace}", flush=True)
    else:
        print(f"\nScript failed with exit code {exit_code}.", flush=True)
        print(f"Inspect run output directory: {workspace}", flush=True)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())