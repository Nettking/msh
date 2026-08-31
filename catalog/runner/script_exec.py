"""Subprocess script execution helpers for workflow sessions.

Scripts are run from isolated timestamped workspaces so their relative-path
assumptions remain compatible with the old catalog layout while session metadata
keeps a durable pointer to each run.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Any

from catalog.capabilities.analysis.contracts import DEFAULT_MAX_SLICE_BYTES
from catalog.capabilities.analysis.resource_admission import (
    MAX_ANALYSIS_METADATA_BYTES,
    MAX_CATALOG_COPY_BYTES,
    MAX_CATALOG_COPY_INODES,
    MAX_SCRIPT_OUTPUT_BYTES,
    MAX_SCRIPT_OUTPUT_INODES,
    reserve_analysis_requirements,
)
from catalog.federation.host_resources import ProcessResourceAdmission
from catalog.federation.process_resource_admission import PROCESS_RESOURCE_ADMISSION
from catalog.runner.script_catalog import ScriptOption, repo_root
from catalog.runner.session_store import script_output_exists, write_session_metadata


class ScriptWorkspaceLimitExceeded(RuntimeError):
    """A selected script exceeded its bounded persistent run workspace."""

    def __init__(self, workspace_dir: Path, message: str) -> None:
        super().__init__(message)
        self.workspace_dir = workspace_dir


_DEFAULT_SCRIPT_WORKSPACE_BYTES = (
    (2 * DEFAULT_MAX_SLICE_BYTES)
    + MAX_CATALOG_COPY_BYTES
    + MAX_SCRIPT_OUTPUT_BYTES
    + MAX_ANALYSIS_METADATA_BYTES
)
_DEFAULT_SCRIPT_WORKSPACE_INODES = (
    (2 * 4096) + MAX_CATALOG_COPY_INODES + MAX_SCRIPT_OUTPUT_INODES + 24
)


def _workspace_usage(root: Path) -> tuple[int, int]:
    """Measure a run tree without following links into an external tree."""
    if not root.exists():
        return 0, 0
    total_bytes = 0
    total_inodes = 1
    for directory, dir_names, file_names in os.walk(root, followlinks=False):
        safe_dirs: list[str] = []
        for name in dir_names:
            path = Path(directory) / name
            try:
                if path.is_symlink():
                    total_inodes += 1
                else:
                    safe_dirs.append(name)
                    total_inodes += 1
            except OSError as exc:
                raise ScriptWorkspaceLimitExceeded(
                    root, "script workspace became unreadable while enforcing its bound"
                ) from exc
        dir_names[:] = safe_dirs
        for name in file_names:
            path = Path(directory) / name
            try:
                stat_result = path.lstat()
            except OSError as exc:
                raise ScriptWorkspaceLimitExceeded(
                    root, "script workspace became unreadable while enforcing its bound"
                ) from exc
            total_inodes += 1
            total_bytes += int(stat_result.st_size)
    return total_bytes, total_inodes


def enforce_workspace_limits(
    workspace_dir: Path,
    *,
    max_bytes: int,
    max_inodes: int,
) -> None:
    """Fail closed when a script's persistent tree exceeds its hard envelope."""
    used_bytes, used_inodes = _workspace_usage(workspace_dir)
    if used_bytes > max_bytes:
        raise ScriptWorkspaceLimitExceeded(
            workspace_dir,
            f"script workspace exceeds {max_bytes} bytes ({used_bytes} observed)",
        )
    if used_inodes > max_inodes:
        raise ScriptWorkspaceLimitExceeded(
            workspace_dir,
            f"script workspace exceeds {max_inodes} inodes ({used_inodes} observed)",
        )


@contextmanager
def _reserve_script_workspace(
    controller: ProcessResourceAdmission,
    session_dir: Path,
    *,
    max_bytes: int,
    max_inodes: int,
) -> Iterator[None]:
    """Hold one shared admission for the complete script workspace transaction."""
    with reserve_analysis_requirements(
        controller,
        [(session_dir, max_bytes, max_inodes)],
    ):
        yield


def create_run_workspace(
    output_base_dir: Path,
    *,
    resource_admission: ProcessResourceAdmission | None = None,
) -> Path:
    """Create a temporary workspace directory for one runner execution."""
    from tempfile import mkdtemp

    controller = resource_admission or PROCESS_RESOURCE_ADMISSION
    with reserve_analysis_requirements(
        controller,
        [(output_base_dir, 0, 2)],
    ):
        output_base_dir.mkdir(parents=True, exist_ok=True)
        path = Path(mkdtemp(prefix="menu_run_", dir=output_base_dir))
        return path


def execute_script_for_session(
    *,
    session_dir: Path,
    metadata: dict,
    script: ScriptOption,
    force_rerun: bool = False,
    resource_admission: ProcessResourceAdmission | None = None,
    admission_held: bool = False,
    max_workspace_bytes: int | None = None,
    max_workspace_inodes: int | None = None,
) -> tuple[str, int | None]:
    """Execute one script in the session and update script-level status."""
    run = _execute_script_for_session_core(
        session_dir=session_dir,
        metadata=metadata,
        script=script,
        force_rerun=force_rerun,
        capture_output=False,
        resource_admission=resource_admission,
        admission_held=admission_held,
        max_workspace_bytes=max_workspace_bytes,
        max_workspace_inodes=max_workspace_inodes,
    )
    return str(run["state"]), run["exit_code"]


def execute_script_for_session_with_logs(
    *,
    session_dir: Path,
    metadata: dict,
    script: ScriptOption,
    force_rerun: bool = False,
    resource_admission: ProcessResourceAdmission | None = None,
    admission_held: bool = False,
    max_workspace_bytes: int | None = None,
    max_workspace_inodes: int | None = None,
) -> tuple[str, int | None, str | None, str | None, str | None]:
    """Execute one script and return state + exit code + captured stdout/stderr snippets."""
    run = _execute_script_for_session_core(
        session_dir=session_dir,
        metadata=metadata,
        script=script,
        force_rerun=force_rerun,
        capture_output=True,
        resource_admission=resource_admission,
        admission_held=admission_held,
        max_workspace_bytes=max_workspace_bytes,
        max_workspace_inodes=max_workspace_inodes,
    )
    return (
        str(run["state"]),
        run["exit_code"],
        run["stdout"],
        run["stderr"],
        run["output_path"],
    )


def _execute_script_for_session_core(
    *,
    session_dir: Path,
    metadata: dict,
    script: ScriptOption,
    force_rerun: bool,
    capture_output: bool,
    resource_admission: ProcessResourceAdmission | None,
    admission_held: bool,
    max_workspace_bytes: int | None,
    max_workspace_inodes: int | None,
) -> dict[str, Any]:
    max_bytes = (
        _DEFAULT_SCRIPT_WORKSPACE_BYTES
        if max_workspace_bytes is None
        else max(int(max_workspace_bytes), 0)
    )
    max_inodes = (
        _DEFAULT_SCRIPT_WORKSPACE_INODES
        if max_workspace_inodes is None
        else max(int(max_workspace_inodes), 0)
    )
    if admission_held:
        try:
            return _execute_script_for_session_unadmitted(
                session_dir=session_dir,
                metadata=metadata,
                script=script,
                force_rerun=force_rerun,
                capture_output=capture_output,
                max_workspace_bytes=max_bytes,
                max_workspace_inodes=max_inodes,
            )
        except ScriptWorkspaceLimitExceeded as exc:
            shutil.rmtree(exc.workspace_dir, ignore_errors=True)
            raise
    controller = resource_admission or PROCESS_RESOURCE_ADMISSION
    try:
        with _reserve_script_workspace(
            controller,
            session_dir,
            max_bytes=max_bytes,
            max_inodes=max_inodes,
        ):
            return _execute_script_for_session_unadmitted(
                session_dir=session_dir,
                metadata=metadata,
                script=script,
                force_rerun=force_rerun,
                capture_output=capture_output,
                max_workspace_bytes=max_bytes,
                max_workspace_inodes=max_inodes,
            )
    except ScriptWorkspaceLimitExceeded as exc:
        # A bounded run is disposable until its status is durably committed.
        # Never leave a partial output tree that could be mistaken for a valid
        # completed script on the next invocation.
        shutil.rmtree(exc.workspace_dir, ignore_errors=True)
        raise


def _execute_script_for_session_unadmitted(
    *,
    session_dir: Path,
    metadata: dict,
    script: ScriptOption,
    force_rerun: bool,
    capture_output: bool,
    max_workspace_bytes: int,
    max_workspace_inodes: int,
) -> dict[str, Any]:
    """Run or reuse a session script and persist status side effects."""
    script_entry = metadata.get("scripts", {}).get(script.key)
    if script_entry is None:
        return {
            "state": "not_tracked",
            "exit_code": None,
            "stdout": None,
            "stderr": None,
            "output_path": None,
        }

    # Script-level cache reuse is intentionally simple: a done status plus an
    # existing output directory is considered fresh unless the caller forces rerun.
    if script_entry.get("status") == "done" and not force_rerun and script_output_exists(session_dir, script_entry):
        return {
            "state": "skipped_cached",
            "exit_code": int(script_entry["exit_code"]) if script_entry.get("exit_code") is not None else 0,
            "stdout": None,
            "stderr": None,
            "output_path": str(script_entry.get("output_path") or ""),
        }

    runs_dir = session_dir / str(metadata["paths"]["runs_dir"])
    runs_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    run_dir = runs_dir / script.key / timestamp
    run_dir.mkdir(parents=True, exist_ok=False)

    copy_repo_catalog_into_workspace(run_dir, admission_held=True)
    enforce_workspace_limits(
        run_dir,
        max_bytes=max_workspace_bytes,
        max_inodes=max_workspace_inodes,
    )

    session_data_dir = session_dir / str(metadata["paths"]["filtered_data_dir"])
    run_data_dir = run_dir / "data"
    try:
        run_data_dir.symlink_to(session_data_dir, target_is_directory=True)
    except OSError:
        # Some host/container filesystems do not allow symlinks; copying keeps
        # legacy scripts working at the cost of extra disk usage.
        shutil.copytree(session_data_dir, run_data_dir)
    enforce_workspace_limits(
        run_dir,
        max_bytes=max_workspace_bytes,
        max_inodes=max_workspace_inodes,
    )

    script_to_run = run_dir / script.script_path
    runtime_env = {
        "FCP_SESSION_ID": session_dir.name,
        "FCP_SESSION_DIR": str(session_dir.resolve()),
        "FCP_RUN_DIR": str(run_dir.resolve()),
    }
    started = perf_counter()
    if capture_output:
        exit_code, stdout_text, stderr_text = run_script_with_output(
            script_to_run,
            run_dir,
            runtime_env=runtime_env,
            admission_held=True,
            max_workspace_bytes=max_workspace_bytes,
            max_workspace_inodes=max_workspace_inodes,
        )
    else:
        exit_code = run_script(
            script_to_run,
            run_dir,
            runtime_env=runtime_env,
            admission_held=True,
            max_workspace_bytes=max_workspace_bytes,
            max_workspace_inodes=max_workspace_inodes,
        )
        stdout_text = None
        stderr_text = None
    enforce_workspace_limits(
        run_dir,
        max_bytes=max_workspace_bytes,
        max_inodes=max_workspace_inodes,
    )
    duration_seconds = round(perf_counter() - started, 3)

    previous_status = str(script_entry.get("status", "not_run"))
    output_path = run_dir.relative_to(session_dir).as_posix()
    script_entry["status"] = "done" if exit_code == 0 else "failed"
    script_entry["output_path"] = output_path
    script_entry["last_run_at"] = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    script_entry["duration_seconds"] = duration_seconds
    script_entry["exit_code"] = exit_code
    write_session_metadata(session_dir, metadata)

    state = "reran" if force_rerun and previous_status == "done" else "ran"
    return {
        "state": state,
        "exit_code": exit_code,
        "stdout": stdout_text,
        "stderr": stderr_text,
        "output_path": output_path,
    }


def run_script(
    script_path: Path,
    workspace_dir: Path,
    *,
    runtime_env: dict[str, str] | None = None,
    resource_admission: ProcessResourceAdmission | None = None,
    admission_held: bool = False,
    max_workspace_bytes: int | None = None,
    max_workspace_inodes: int | None = None,
) -> int:
    """Execute a selected catalog script inside a workspace directory."""
    completed = _run_script_with_admission(
        script_path,
        workspace_dir,
        runtime_env=runtime_env,
        resource_admission=resource_admission,
        admission_held=admission_held,
        max_workspace_bytes=max_workspace_bytes,
        max_workspace_inodes=max_workspace_inodes,
    )
    if completed.stdout:
        print("[script stdout]", flush=True)
        print(completed.stdout, end="" if completed.stdout.endswith("\n") else "\n", flush=True)
    if completed.stderr:
        print("[script stderr]", flush=True)
        print(completed.stderr, end="" if completed.stderr.endswith("\n") else "\n", flush=True)
    return completed.returncode


def run_script_with_output(
    script_path: Path,
    workspace_dir: Path,
    *,
    runtime_env: dict[str, str] | None = None,
    resource_admission: ProcessResourceAdmission | None = None,
    admission_held: bool = False,
    max_workspace_bytes: int | None = None,
    max_workspace_inodes: int | None = None,
) -> tuple[int, str | None, str | None]:
    """Execute script and return full stdout/stderr (still echoed to parent logs)."""
    completed = _run_script_with_admission(
        script_path,
        workspace_dir,
        runtime_env=runtime_env,
        resource_admission=resource_admission,
        admission_held=admission_held,
        max_workspace_bytes=max_workspace_bytes,
        max_workspace_inodes=max_workspace_inodes,
    )
    if completed.stdout:
        print("[script stdout]", flush=True)
        print(completed.stdout, end="" if completed.stdout.endswith("\n") else "\n", flush=True)
    if completed.stderr:
        print("[script stderr]", flush=True)
        print(completed.stderr, end="" if completed.stderr.endswith("\n") else "\n", flush=True)
    return completed.returncode, completed.stdout or None, completed.stderr or None


def _run_script_with_admission(
    script_path: Path,
    workspace_dir: Path,
    *,
    runtime_env: dict[str, str] | None,
    resource_admission: ProcessResourceAdmission | None,
    admission_held: bool,
    max_workspace_bytes: int | None,
    max_workspace_inodes: int | None,
) -> subprocess.CompletedProcess[str]:
    bounded_bytes = (
        _DEFAULT_SCRIPT_WORKSPACE_BYTES
        if max_workspace_bytes is None
        else max(int(max_workspace_bytes), 0)
    )
    bounded_inodes = (
        _DEFAULT_SCRIPT_WORKSPACE_INODES
        if max_workspace_inodes is None
        else max(int(max_workspace_inodes), 0)
    )
    if admission_held:
        return _run_script_subprocess(
            script_path,
            workspace_dir,
            runtime_env=runtime_env,
            max_workspace_bytes=bounded_bytes,
            max_workspace_inodes=bounded_inodes,
        )
    controller = resource_admission or PROCESS_RESOURCE_ADMISSION
    with _reserve_script_workspace(
        controller,
        workspace_dir,
        max_bytes=bounded_bytes,
        max_inodes=bounded_inodes,
    ):
        return _run_script_subprocess(
            script_path,
            workspace_dir,
            runtime_env=runtime_env,
            max_workspace_bytes=bounded_bytes,
            max_workspace_inodes=bounded_inodes,
        )


def _run_script_subprocess(
    script_path: Path,
    workspace_dir: Path,
    *,
    runtime_env: dict[str, str] | None = None,
    max_workspace_bytes: int | None = None,
    max_workspace_inodes: int | None = None,
) -> subprocess.CompletedProcess[str]:
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
    if runtime_env:
        env.update(runtime_env)

    command = [sys.executable, str(script_path)]
    print(f"\nRunning: {' '.join(command)}", flush=True)
    print(f"Working directory: {workspace_dir}", flush=True)

    process = subprocess.Popen(
        command,
        cwd=workspace_dir,
        env=env,
        # Disable stdin so Flask/Docker-triggered scripts cannot hang waiting
        # for the deprecated interactive menu.
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        while True:
            try:
                # communicate() drains both pipes while waiting, avoiding the
                # deadlock that a poll loop would create when a script emits a
                # large diagnostic stream.
                stdout, stderr = process.communicate(timeout=0.05)
                break
            except subprocess.TimeoutExpired:
                if max_workspace_bytes is not None and max_workspace_inodes is not None:
                    enforce_workspace_limits(
                        workspace_dir,
                        max_bytes=max_workspace_bytes,
                        max_inodes=max_workspace_inodes,
                    )
    except ScriptWorkspaceLimitExceeded:
        process.terminate()
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
        raise

    stdout, stderr = process.communicate()
    if max_workspace_bytes is not None and max_workspace_inodes is not None:
        enforce_workspace_limits(
            workspace_dir,
            max_bytes=max_workspace_bytes,
            max_inodes=max_workspace_inodes,
        )
    return subprocess.CompletedProcess(
        command,
        process.returncode,
        stdout,
        stderr,
    )


def copy_repo_catalog_into_workspace(
    workspace_dir: Path,
    *,
    resource_admission: ProcessResourceAdmission | None = None,
    admission_held: bool = False,
) -> None:
    """Copy the repository's ``catalog/`` directory into a run workspace."""
    if not admission_held:
        controller = resource_admission or PROCESS_RESOURCE_ADMISSION
        with reserve_analysis_requirements(
            controller,
            [(workspace_dir, MAX_CATALOG_COPY_BYTES, MAX_CATALOG_COPY_INODES + 4)],
        ):
            return copy_repo_catalog_into_workspace(
                workspace_dir,
                resource_admission=controller,
                admission_held=True,
            )
    source_catalog = repo_root() / "catalog"
    target_catalog = workspace_dir / "catalog"

    if target_catalog.exists():
        shutil.rmtree(target_catalog)
    source_bytes, source_inodes = _workspace_usage(source_catalog)
    if source_bytes > MAX_CATALOG_COPY_BYTES:
        raise ScriptWorkspaceLimitExceeded(
            workspace_dir,
            f"catalog copy exceeds {MAX_CATALOG_COPY_BYTES} bytes",
        )
    if source_inodes > MAX_CATALOG_COPY_INODES:
        raise ScriptWorkspaceLimitExceeded(
            workspace_dir,
            f"catalog copy exceeds {MAX_CATALOG_COPY_INODES} inodes",
        )
    try:
        shutil.copytree(source_catalog, target_catalog)
        copied_bytes, copied_inodes = _workspace_usage(target_catalog)
        if copied_bytes > MAX_CATALOG_COPY_BYTES or copied_inodes > MAX_CATALOG_COPY_INODES:
            raise ScriptWorkspaceLimitExceeded(
                workspace_dir,
                "catalog copy exceeded its bounded workspace envelope",
            )
    except ScriptWorkspaceLimitExceeded:
        shutil.rmtree(target_catalog, ignore_errors=True)
        raise
