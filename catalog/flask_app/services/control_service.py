from __future__ import annotations

import threading
import traceback
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from catalog.orchestrator.pipeline import get_runtime_manager
from catalog.runner.data_filtering import ensure_session_filtered_data
from catalog.runner.playback import prepare_session_playback_exports
from catalog.runner.script_catalog import ScriptOption, discover_runnable_scripts, repo_root
from catalog.runner.script_exec import execute_script_for_session_with_logs
from catalog.runner.session_store import (
    WORKFLOW_SCRIPT_ORDER,
    WORKFLOW_STEPS,
    list_sessions,
    normalize_session_metadata,
    workflow_step_status,
    write_session_metadata,
)


@dataclass
class ControlRun:
    run_id: int
    action: str
    target: str | None
    status: str
    started_at: str
    finished_at: str | None
    session_id: str | None
    output_path: str | None
    message: str
    stdout_snippet: str | None
    stderr_snippet: str | None


class ControlPanelService:
    def __init__(self) -> None:
        self.root = repo_root()
        self.workflows_root = self.root / "results" / "workflows"
        self.data_root = self.root / "data"
        self._lock = threading.Lock()
        self._active_run_id: int | None = None
        self._run_sequence = 0
        self._recent_runs: deque[ControlRun] = deque(maxlen=30)

    def snapshot(self) -> dict[str, Any]:
        runtime_state = get_runtime_manager().state_snapshot()
        script_options = discover_runnable_scripts(self.root / "catalog")
        sessions = list_sessions(self.workflows_root)
        latest_session = sessions[0] if sessions else None

        latest_metadata = None
        workflow_rows: list[dict[str, str]] = []
        script_rows: list[dict[str, Any]] = []
        if latest_session is not None:
            latest_metadata, changed = normalize_session_metadata(
                latest_session.session_dir,
                dict(latest_session.metadata),
                script_options,
            )
            if changed:
                write_session_metadata(latest_session.session_dir, latest_metadata)

            for step_name, step_scripts in WORKFLOW_STEPS:
                workflow_rows.append(
                    {
                        "step": step_name,
                        "scripts": ", ".join(step_scripts),
                        "status": workflow_step_status(latest_metadata, step_scripts),
                    }
                )

            scripts_meta = latest_metadata.get("scripts", {})
            for option in script_options:
                row = scripts_meta.get(option.key, {})
                script_rows.append(
                    {
                        "name": option.key,
                        "category": option.category,
                        "workflow_step": row.get("workflow_step") or "unassigned",
                        "status": row.get("status", "not_run"),
                        "last_run_at": row.get("last_run_at") or "n/a",
                        "exit_code": row.get("exit_code"),
                        "output_path": row.get("output_path") or "",
                    }
                )
        else:
            for option in script_options:
                script_rows.append(
                    {
                        "name": option.key,
                        "category": option.category,
                        "workflow_step": "unassigned",
                        "status": "not_run",
                        "last_run_at": "n/a",
                        "exit_code": None,
                        "output_path": "",
                    }
                )

        with self._lock:
            active_run = self._active_run_id
            recent_runs = [asdict(item) for item in self._recent_runs]

        return {
            "runtime_state": runtime_state,
            "latest_session": latest_session,
            "latest_metadata": latest_metadata,
            "workflow_rows": workflow_rows,
            "script_rows": script_rows,
            "active_run_id": active_run,
            "recent_runs": list(reversed(recent_runs)),
        }

    def trigger_action(self, action: str, *, script_key: str | None = None) -> tuple[bool, str]:
        with self._lock:
            if self._active_run_id is not None:
                return False, "A control action is already running. Wait for it to finish before starting another."
            self._run_sequence += 1
            run_id = self._run_sequence
            run = ControlRun(
                run_id=run_id,
                action=action,
                target=script_key,
                status="running",
                started_at=_utc_now_iso(),
                finished_at=None,
                session_id=None,
                output_path=None,
                message="Started",
                stdout_snippet=None,
                stderr_snippet=None,
            )
            self._active_run_id = run_id
            self._recent_runs.append(run)

        worker = threading.Thread(target=self._run_action, args=(run_id, action, script_key), daemon=True)
        worker.start()
        return True, "Action started. Refresh the page to see updated status."

    def _run_action(self, run_id: int, action: str, script_key: str | None) -> None:
        status = "ok"
        message = "Completed"
        session_id: str | None = None
        output_path: str | None = None
        stdout_snippet: str | None = None
        stderr_snippet: str | None = None

        try:
            if action == "refresh_now":
                runtime = get_runtime_manager()
                if runtime.state_snapshot().get("update_running"):
                    status = "blocked"
                    message = "Refresh already running."
                else:
                    runtime.request_refresh()
                    message = "Refresh requested through runtime manager (async single-process run)."
            elif action in {"rerun_latest_session_workflow", "rerun_bootstrap"}:
                status, message, session_id, output_path, stdout_snippet, stderr_snippet = self._rerun_scripts(WORKFLOW_SCRIPT_ORDER)
            elif action == "startup_health":
                step_scripts = WORKFLOW_STEPS[0][1]
                status, message, session_id, output_path, stdout_snippet, stderr_snippet = self._rerun_scripts(step_scripts)
            elif action == "run_script" and script_key:
                status, message, session_id, output_path, stdout_snippet, stderr_snippet = self._rerun_scripts([script_key])
            else:
                status = "failed"
                message = f"Unsupported action: {action}"
        except Exception as exc:  # noqa: BLE001
            status = "failed"
            message = f"{exc.__class__.__name__}: {exc}"
            stderr_snippet = "".join(traceback.format_exception(exc))[-800:]

        self._finish_run(
            run_id,
            status=status,
            message=message,
            session_id=session_id,
            output_path=output_path,
            stdout_snippet=stdout_snippet,
            stderr_snippet=stderr_snippet,
        )

    def _rerun_scripts(self, script_keys: list[str]) -> tuple[str, str, str | None, str | None, str | None, str | None]:
        script_options = discover_runnable_scripts(self.root / "catalog")
        script_index: dict[str, ScriptOption] = {item.key: item for item in script_options}
        sessions = list_sessions(self.workflows_root)
        if not sessions:
            return "failed", "No workflow session exists yet. Run refresh/bootstrap first.", None, None, None, None

        latest_session = sessions[0]
        metadata, changed = normalize_session_metadata(latest_session.session_dir, dict(latest_session.metadata), script_options)
        if changed:
            write_session_metadata(latest_session.session_dir, metadata)

        ensure_session_filtered_data(
            source_data_dir=self.data_root,
            session_dir=latest_session.session_dir,
            metadata=metadata,
        )

        results: list[str] = []
        final_output_path: str | None = None
        failures: list[str] = []
        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []
        for key in script_keys:
            script = script_index.get(key)
            if script is None:
                failures.append(key)
                results.append(f"{key}: not discovered")
                continue
            state, exit_code, stdout_text, stderr_text, output_path = execute_script_for_session_with_logs(
                session_dir=latest_session.session_dir,
                metadata=metadata,
                script=script,
                force_rerun=True,
            )
            final_output_path = output_path or final_output_path
            if exit_code not in (0, None):
                failures.append(key)
            if stdout_text:
                results.append(f"{key}: state={state}, exit={exit_code}, stdout={len(stdout_text)} chars")
            else:
                results.append(f"{key}: state={state}, exit={exit_code}")
            if stdout_text:
                stdout_chunks.append(f"[{key}]\n{stdout_text}")
            if stderr_text:
                stderr_chunks.append(f"[{key}]\n{stderr_text}")

        prepare_session_playback_exports(latest_session.session_dir, metadata)

        stdout_snippet = _tail_snippet("\n\n".join(stdout_chunks), limit=1200)
        stderr_snippet = _tail_snippet("\n\n".join(stderr_chunks), limit=1200)

        if failures:
            return "failed", "; ".join(results), latest_session.session_id, final_output_path, stdout_snippet, stderr_snippet
        return "ok", "; ".join(results), latest_session.session_id, final_output_path, stdout_snippet, stderr_snippet

    def _finish_run(
        self,
        run_id: int,
        *,
        status: str,
        message: str,
        session_id: str | None,
        output_path: str | None,
        stdout_snippet: str | None,
        stderr_snippet: str | None,
    ) -> None:
        with self._lock:
            for index, run in enumerate(self._recent_runs):
                if run.run_id != run_id:
                    continue
                self._recent_runs[index] = ControlRun(
                    run_id=run.run_id,
                    action=run.action,
                    target=run.target,
                    status=status,
                    started_at=run.started_at,
                    finished_at=_utc_now_iso(),
                    session_id=session_id,
                    output_path=output_path,
                    message=message,
                    stdout_snippet=stdout_snippet,
                    stderr_snippet=stderr_snippet,
                )
                break
            self._active_run_id = None


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _tail_snippet(text: str, *, limit: int) -> str | None:
    cleaned = text.strip()
    if not cleaned:
        return None
    if len(cleaned) <= limit:
        return cleaned
    return f"...\n{cleaned[-limit:]}"


_CONTROL_PANEL_SERVICE: ControlPanelService | None = None


def get_control_panel_service() -> ControlPanelService:
    global _CONTROL_PANEL_SERVICE
    if _CONTROL_PANEL_SERVICE is None:
        _CONTROL_PANEL_SERVICE = ControlPanelService()
    return _CONTROL_PANEL_SERVICE
