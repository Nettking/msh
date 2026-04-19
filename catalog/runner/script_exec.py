"""Subprocess script execution helpers for workflow sessions."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from time import perf_counter

from catalog.runner.script_catalog import ScriptOption, repo_root
from catalog.runner.session_store import script_output_exists, write_session_metadata


def create_run_workspace(output_base_dir: Path) -> Path:
    """Create a temporary workspace directory for one runner execution."""
    from tempfile import mkdtemp

    output_base_dir.mkdir(parents=True, exist_ok=True)
    path = Path(mkdtemp(prefix="menu_run_", dir=output_base_dir))
    return path


def execute_script_for_session(
    *,
    session_dir: Path,
    metadata: dict,
    script: ScriptOption,
    force_rerun: bool = False,
) -> tuple[str, int | None]:
    """Execute one script in the session and update script-level status."""
    script_entry = metadata.get("scripts", {}).get(script.key)
    if script_entry is None:
        return "not_tracked", None

    if script_entry.get("status") == "done" and not force_rerun and script_output_exists(session_dir, script_entry):
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
    """Execute a selected catalog script inside a workspace directory."""
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    env.setdefault("MPLBACKEND", "Agg")
    workspace_import_root = str(workspace_dir.resolve())
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        os.pathsep.join([workspace_import_root, existing_pythonpath])
        if existing_pythonpath
        else workspace_import_root
    )

    command = [sys.executable, str(script_path)]
    print(f"\nRunning: {' '.join(command)}", flush=True)
    print(f"Working directory: {workspace_dir}", flush=True)

    completed = subprocess.run(
        command,
        cwd=workspace_dir,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if completed.stdout:
        print("[script stdout]", flush=True)
        print(completed.stdout, end="" if completed.stdout.endswith("\n") else "\n", flush=True)
    if completed.stderr:
        print("[script stderr]", flush=True)
        print(completed.stderr, end="" if completed.stderr.endswith("\n") else "\n", flush=True)
    return completed.returncode


def copy_repo_catalog_into_workspace(workspace_dir: Path) -> None:
    """Copy the repository's ``catalog/`` directory into a run workspace."""
    source_catalog = repo_root() / "catalog"
    target_catalog = workspace_dir / "catalog"

    if target_catalog.exists():
        shutil.rmtree(target_catalog)

    shutil.copytree(source_catalog, target_catalog)
