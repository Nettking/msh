from __future__ import annotations

import sys

from menu_utils import (
    ScriptOption,
    copy_repo_catalog_into_workspace,
    create_run_workspace,
    discover_runnable_scripts,
    discover_available_dates,
    filter_data_by_date_range,
    print_numbered_menu,
    prompt_menu_choice,
    repo_root,
    run_script,
)


def pick_script(script_options: list[ScriptOption]) -> ScriptOption:
    labels = [f"{item.key} — {item.description}" for item in script_options]
    print_numbered_menu("Select a script to run:", labels)
    choice = prompt_menu_choice(len(script_options), f"Enter script number (1-{len(script_options)}): ")
    return script_options[choice - 1]


def pick_date_range(available_dates):
    labels = [d.isoformat() for d in available_dates]
    print_numbered_menu("\nAvailable data dates:", labels)
    start_index = prompt_menu_choice(len(labels), "Choose start date number: ") - 1

    while True:
        end_index = prompt_menu_choice(len(labels), "Choose end date number: ") - 1
        if end_index < start_index:
            print("End date must be on or after start date.")
            continue
        return available_dates[start_index], available_dates[end_index]


def confirm_or_exit(script: ScriptOption, start_date, end_date) -> None:
    print("\nSelection summary")
    print(f"Script: {script.key}")
    print(f"Start date: {start_date.isoformat()}")
    print(f"End date: {end_date.isoformat()}")

    while True:
        answer = input("Run now? (y/n): ").strip().lower()
        if answer in {"y", "yes"}:
            return
        if answer in {"n", "no"}:
            print("Cancelled.")
            raise SystemExit(0)
        print("Please type y or n.")


def main() -> int:
    root = repo_root()
    catalog_dir = root / "catalog"
    data_dir = root / "data"
    output_base_dir = root / "results" / "menu_runs"

    script_options = discover_runnable_scripts(catalog_dir)
    if not script_options:
        print("No runnable scripts found in catalog/.")
        return 1

    if not data_dir.exists():
        print(f"Data directory not found: {data_dir}")
        print("Tip: start the project with Docker Compose so data and results are mounted automatically.")
        print("Run: docker compose up")
        return 1

    available_dates = discover_available_dates(data_dir)
    if not available_dates:
        print("No dates discovered in data/. Ensure records include timestamps or filenames include YYYY-MM-DD / YYYYMMDD.")
        return 1

    script = pick_script(script_options)
    start_date, end_date = pick_date_range(available_dates)
    confirm_or_exit(script, start_date, end_date)

    workspace = create_run_workspace(output_base_dir)
    filtered_data_dir = workspace / "data"

    matched_records, matched_files = filter_data_by_date_range(data_dir, filtered_data_dir, start_date, end_date)
    if matched_records == 0:
        print("\nNo records found in selected date range. Nothing to run.")
        print(f"Filtered data path: {filtered_data_dir}")
        return 0

    copy_repo_catalog_into_workspace(workspace)

    script_to_run = workspace / script.script_path
    print("\nRun configuration")
    print(f"Selected script: {script.key}")
    print(f"Script path: {script.script_path}")
    print(f"Selected start date: {start_date.isoformat()}")
    print(f"Selected end date: {end_date.isoformat()}")
    print(f"Filtered dataset path: {filtered_data_dir}")
    print(f"Matched records: {matched_records}")
    print(f"Matched files: {matched_files}")
    print(f"Outputs will be written under: {workspace}")

    exit_code = run_script(script_to_run, workspace)
    if exit_code == 0:
        print("\nScript completed successfully.")
        print(f"Run output directory: {workspace}")
    else:
        print(f"\nScript failed with exit code {exit_code}.")
        print(f"Inspect run output directory: {workspace}")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
