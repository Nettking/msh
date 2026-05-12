"""Control-panel service for manual runtime/session/script actions.

The service resolves operator-selected scope into workflow sessions, starts one
background thread per accepted action, captures recent run status/snippets in
memory, and delegates actual filtering/execution to runner/orchestrator helpers.
It is intentionally not a durable job queue.
"""

from __future__ import annotations

import threading
import traceback
from collections import deque
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any

from catalog.orchestrator.pipeline import get_runtime_manager
from catalog.runner.data_filtering import discover_available_dates, ensure_session_filtered_data
from catalog.runner.playback import (
    playback_exports_are_reusable,
    playback_readiness,
    prepare_session_playback_exports,
    session_playback_export_dir,
)
from catalog.runner.script_catalog import ScriptOption, discover_runnable_scripts, repo_root
from catalog.runner.script_exec import execute_script_for_session_with_logs
from catalog.runner.session_store import (
    WORKFLOW_SCRIPT_ORDER,
    WORKFLOW_STEPS,
    filter_signature,
    initialize_session_metadata,
    list_sessions,
    normalize_session_metadata,
    workflow_step_status,
    write_session_metadata,
)

from .workflow_session_index import get_workflow_session_index


@dataclass
class ControlRun:
    run_id: int
    action: str
    target: str | None
    status: str
    started_at: str
    finished_at: str | None
    session_id: str | None
    target_range: str | None
    output_path: str | None
    message: str
    stdout_snippet: str | None
    stderr_snippet: str | None


class ControlPanelService:
    """Resolve /control requests into session-scoped background actions."""

    def __init__(self) -> None:
        self.root = repo_root()
        self.workflows_root = self.root / "results" / "workflows"
        self.data_root = self.root / "data"
        self._lock = threading.Lock()
        self._active_run_id: int | None = None
        self._run_sequence = 0
        self._recent_runs: deque[ControlRun] = deque(maxlen=30)
        self._script_options = discover_runnable_scripts(self.root / "catalog")
        self._available_dates_cache: tuple[float, str | None, str | None] | None = None
        self._available_dates_ttl_seconds = 10.0

    def cache_signature(self) -> tuple[int | None, int, int | None]:
        with self._lock:
            active_run = self._active_run_id
            recent_count = len(self._recent_runs)
            latest_run_id = self._recent_runs[-1].run_id if self._recent_runs else None
        return active_run, recent_count, latest_run_id

    def snapshot(
        self,
        *,
        selected_session_id: str | None = None,
        runtime_state: dict[str, Any] | None = None,
        sessions: list[Any] | None = None,
    ) -> dict[str, Any]:
        """Build the control page model from runtime state, sessions, and scripts."""
        runtime_state = runtime_state or get_runtime_manager().state_snapshot()
        script_options = self._script_options
        sessions = sessions if sessions is not None else get_workflow_session_index().get_sessions(self.workflows_root).sessions
        # Session index guarantees deterministic freshness ordering (latest first).
        latest_session = sessions[0] if sessions else None

        selected_session = _resolve_selected_session(
            sessions,
            selected_session_id,
            strict=True,
        )
        selected_session_missing = bool(selected_session_id and selected_session is None)

        session_rows: list[dict[str, Any]] = []
        for session in sessions:
            session_rows.append(_session_row(session.session_id, session.session_dir, session.metadata))

        selected_metadata = None
        workflow_rows: list[dict[str, str]] = []
        script_rows: list[dict[str, Any]] = []
        if selected_session is not None:
            selected_metadata = _normalized_metadata_view(selected_session, script_options)

            for step_name, step_scripts in WORKFLOW_STEPS:
                workflow_rows.append(
                    {
                        "step": step_name,
                        "scripts": ", ".join(step_scripts),
                        "status": workflow_step_status(selected_metadata, step_scripts),
                    }
                )

            scripts_meta = selected_metadata.get("scripts", {})
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

        available_start, available_end = self._available_date_bounds()

        return {
            "runtime_state": runtime_state,
            "latest_session": latest_session,
            "selected_session": selected_session,
            "selected_session_missing": selected_session_missing,
            "selected_metadata": selected_metadata,
            "selected_dataset": _selected_dataset_summary(selected_session, selected_metadata) if selected_session else None,
            "sessions": session_rows,
            "workflow_rows": workflow_rows,
            "script_rows": script_rows,
            "active_run_id": active_run,
            "recent_runs": list(reversed(recent_runs)),
            "available_start": available_start,
            "available_end": available_end,
        }

    def _available_date_bounds(self) -> tuple[str | None, str | None]:
        now = datetime.utcnow().timestamp()
        with self._lock:
            cached = self._available_dates_cache
            if cached and (now - cached[0]) <= self._available_dates_ttl_seconds:
                return cached[1], cached[2]
        available_dates = discover_available_dates(self.data_root)
        available_start = available_dates[0].isoformat() if available_dates else None
        available_end = available_dates[-1].isoformat() if available_dates else None
        with self._lock:
            self._available_dates_cache = (datetime.utcnow().timestamp(), available_start, available_end)
        return available_start, available_end

    def trigger_action(
        self,
        action: str,
        *,
        script_key: str | None = None,
        selected_session_id: str | None = None,
        scope_mode: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> tuple[bool, str, str]:
        """Validate a control action, reserve the single worker slot, and start it."""
        resolved_target: tuple[str, Any, str] | None = None
        if action in {"startup_health", "run_selected_session_workflow", "run_script", "rebuild_playback_exports"}:
            try:
                resolved_target = self._resolve_target_session(
                    selected_session_id=selected_session_id,
                    scope_mode=scope_mode,
                    start_date=start_date,
                    end_date=end_date,
                )
            except Exception as exc:  # noqa: BLE001
                return False, str(exc), ""
        elif action in {"rerun_latest_session_workflow", "rerun_bootstrap"}:
            try:
                resolved_target = self._resolve_target_session(
                    selected_session_id=selected_session_id,
                    scope_mode="latest_existing",
                    start_date=start_date,
                    end_date=end_date,
                )
            except Exception as exc:  # noqa: BLE001
                return False, str(exc), ""

        with self._lock:
            if self._active_run_id is not None:
                return False, "A control action is already running. Wait for it to finish before starting another.", ""
            self._run_sequence += 1
            run_id = self._run_sequence
            run = ControlRun(
                run_id=run_id,
                action=action,
                target=script_key,
                status="running",
                started_at=_utc_now_iso(),
                finished_at=None,
                session_id=resolved_target[0] if resolved_target else None,
                target_range=resolved_target[2] if resolved_target else None,
                output_path=None,
                message="Started",
                stdout_snippet=None,
                stderr_snippet=None,
            )
            self._active_run_id = run_id
            self._recent_runs.append(run)

        # The MVP control plane is single-process: a daemon thread keeps Flask
        # responsive, while _active_run_id prevents overlapping script work.
        worker = threading.Thread(
            target=self._run_action,
            args=(run_id, action, script_key, selected_session_id, scope_mode, start_date, end_date, resolved_target),
            daemon=True,
        )
        worker.start()
        target_session_id = resolved_target[0] if resolved_target else (selected_session_id or "")
        return True, "Action started. Refresh the page to see updated status.", target_session_id

    def _run_action(
        self,
        run_id: int,
        action: str,
        script_key: str | None,
        selected_session_id: str | None,
        scope_mode: str | None,
        start_date: str | None,
        end_date: str | None,
        resolved_target: tuple[str, Any, str] | None,
    ) -> None:
        status = "ok"
        message = "Completed"
        session_id: str | None = None
        target_range: str | None = None
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
                target = resolved_target or self._resolve_target_session(
                    selected_session_id=selected_session_id,
                    scope_mode="latest_existing",
                    start_date=start_date,
                    end_date=end_date,
                )
                status, message, session_id, target_range, output_path, stdout_snippet, stderr_snippet = self._rerun_scripts(
                    target,
                    WORKFLOW_SCRIPT_ORDER,
                )
            elif action == "startup_health":
                step_scripts = WORKFLOW_STEPS[0][1]
                target = resolved_target or self._resolve_target_session(
                    selected_session_id=selected_session_id,
                    scope_mode=scope_mode,
                    start_date=start_date,
                    end_date=end_date,
                )
                status, message, session_id, target_range, output_path, stdout_snippet, stderr_snippet = self._rerun_scripts(
                    target,
                    step_scripts,
                )
            elif action == "run_selected_session_workflow":
                target = resolved_target or self._resolve_target_session(
                    selected_session_id=selected_session_id,
                    scope_mode=scope_mode,
                    start_date=start_date,
                    end_date=end_date,
                )
                status, message, session_id, target_range, output_path, stdout_snippet, stderr_snippet = self._rerun_scripts(
                    target,
                    WORKFLOW_SCRIPT_ORDER,
                )
            elif action == "run_script" and script_key:
                target = resolved_target or self._resolve_target_session(
                    selected_session_id=selected_session_id,
                    scope_mode=scope_mode,
                    start_date=start_date,
                    end_date=end_date,
                )
                status, message, session_id, target_range, output_path, stdout_snippet, stderr_snippet = self._rerun_scripts(
                    target,
                    [script_key],
                )
            elif action == "rebuild_playback_exports":
                target = resolved_target or self._resolve_target_session(
                    selected_session_id=selected_session_id,
                    scope_mode=scope_mode,
                    start_date=start_date,
                    end_date=end_date,
                )
                status, message, session_id, target_range, output_path, stdout_snippet, stderr_snippet = (
                    self._rebuild_playback_exports(target)
                )
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
            target_range=target_range,
            output_path=output_path,
            stdout_snippet=stdout_snippet,
            stderr_snippet=stderr_snippet,
        )

    def _resolve_target_session(
        self,
        *,
        selected_session_id: str | None,
        scope_mode: str | None,
        start_date: str | None,
        end_date: str | None,
    ) -> tuple[str, Any, str]:
        """Map operator scope controls to an existing or newly prepared session."""
        script_options = self._script_options
        sessions = list_sessions(self.workflows_root)

        if scope_mode in {None, "", "selected_session"}:
            if not selected_session_id:
                raise ValueError("Selected-session mode requires choosing an existing session.")
            session = _resolve_selected_session(sessions, selected_session_id, strict=True)
            if session is None:
                raise ValueError(f"Selected session not found: {selected_session_id}")
            metadata, changed = normalize_session_metadata(session.session_dir, dict(session.metadata), script_options)
            if changed:
                write_session_metadata(session.session_dir, metadata)
            return session.session_id, session.session_dir, _range_label(metadata)

        if scope_mode == "latest_existing":
            if not sessions:
                raise ValueError("No workflow session exists yet. Run refresh/bootstrap first.")
            session = sessions[0]
            metadata, changed = normalize_session_metadata(session.session_dir, dict(session.metadata), script_options)
            if changed:
                write_session_metadata(session.session_dir, metadata)
            return session.session_id, session.session_dir, _range_label(metadata)

        target_start, target_end = _resolve_scope_dates(
            scope_mode=scope_mode,
            start_date=start_date,
            end_date=end_date,
            data_root=self.data_root,
        )
        session_id = _manual_session_id(scope_mode, target_start, target_end)
        session_dir = self.workflows_root / session_id
        signature = filter_signature(
            {
                "start_date": target_start.isoformat(),
                "end_date": target_end.isoformat(),
                "start_hour": None,
                "end_hour": None,
            }
        )

        if session_dir.exists():
            existing = _load_normalized_session_metadata(session_dir, script_options)
            if existing and existing.get("session_config_signature") == signature:
                return session_id, session_dir, _range_label(existing)

        if not session_dir.exists():
            session_dir.mkdir(parents=True, exist_ok=True)

        metadata = initialize_session_metadata(
            session_id,
            target_start,
            target_end,
            start_hour=None,
            end_hour=None,
            script_options=script_options,
        )
        write_session_metadata(session_dir, metadata)
        return session_id, session_dir, _range_label(metadata)

    def _rerun_scripts(
        self,
        target_session: tuple[str, Any, str],
        script_keys: list[str],
    ) -> tuple[str, str, str | None, str | None, str | None, str | None, str | None]:
        session_id, session_dir, target_range = target_session
        script_options = self._script_options
        script_index: dict[str, ScriptOption] = {item.key: item for item in script_options}
        metadata = _load_normalized_session_metadata(session_dir, script_options)
        if metadata is None:
            return "failed", f"Missing session metadata for {session_id}", None, None, None, None, None

        ensure_session_filtered_data(
            source_data_dir=self.data_root,
            session_dir=session_dir,
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
                session_dir=session_dir,
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

        prepare_session_playback_exports(session_dir, metadata)

        stdout_snippet = _tail_snippet("\n\n".join(stdout_chunks), limit=1200)
        stderr_snippet = _tail_snippet("\n\n".join(stderr_chunks), limit=1200)

        if failures:
            return "failed", "; ".join(results), session_id, target_range, final_output_path, stdout_snippet, stderr_snippet
        return "ok", "; ".join(results), session_id, target_range, final_output_path, stdout_snippet, stderr_snippet

    def _rebuild_playback_exports(
        self,
        target_session: tuple[str, Any, str],
    ) -> tuple[str, str, str | None, str | None, str | None, str | None, str | None]:
        session_id, session_dir, target_range = target_session
        metadata = _load_normalized_session_metadata(session_dir, self._script_options)
        if metadata is None:
            return "failed", f"Missing session metadata for {session_id}", None, None, None, None, None

        ensure_session_filtered_data(
            source_data_dir=self.data_root,
            session_dir=session_dir,
            metadata=metadata,
        )
        export_path, export_status = prepare_session_playback_exports(session_dir, metadata)
        return (
            "ok",
            f"Playback exports {export_status} for selected dataset {session_id}.",
            session_id,
            target_range,
            str(export_path),
            None,
            None,
        )

    def _finish_run(
        self,
        run_id: int,
        *,
        status: str,
        message: str,
        session_id: str | None,
        target_range: str | None,
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
                    target_range=target_range,
                    output_path=output_path,
                    message=message,
                    stdout_snippet=stdout_snippet,
                    stderr_snippet=stderr_snippet,
                )
                break
            self._active_run_id = None
            self._available_dates_cache = None
        get_workflow_session_index().invalidate(self.workflows_root)


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _tail_snippet(text: str, *, limit: int) -> str | None:
    cleaned = text.strip()
    if not cleaned:
        return None
    if len(cleaned) <= limit:
        return cleaned
    return f"...\n{cleaned[-limit:]}"


def _resolve_selected_session(sessions: list[Any], selected_session_id: str | None, *, strict: bool = False):
    if not sessions:
        return None
    if selected_session_id:
        for session in sessions:
            if session.session_id == selected_session_id:
                return session
        if strict:
            return None
    if strict:
        return None
    return sessions[0]


def _load_normalized_session_metadata(session_dir, script_options):
    state_path = session_dir / "session_state.json"
    legacy_path = session_dir / "session.json"
    metadata_path = state_path if state_path.exists() else legacy_path
    if not metadata_path.exists():
        return None
    import json

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata, changed = normalize_session_metadata(session_dir, metadata, script_options)
    if changed:
        write_session_metadata(session_dir, metadata)
    return metadata


def _resolve_scope_dates(*, scope_mode: str | None, start_date: str | None, end_date: str | None, data_root):
    available_dates = discover_available_dates(data_root)
    if not available_dates:
        raise ValueError("No source dates discovered in data/.")

    if scope_mode == "latest_day":
        day = available_dates[-1]
        return day, day

    if scope_mode == "selected_day":
        if not start_date:
            raise ValueError("Selected-day mode requires a start date.")
        try:
            day = date.fromisoformat(start_date)
        except ValueError as exc:
            raise ValueError("Selected-day mode requires start date in YYYY-MM-DD format.") from exc
        return day, day

    if scope_mode == "custom_range":
        if not start_date or not end_date:
            raise ValueError("Custom range mode requires both start and end dates.")
        try:
            day_start = date.fromisoformat(start_date)
            day_end = date.fromisoformat(end_date)
        except ValueError as exc:
            raise ValueError("Custom range mode requires YYYY-MM-DD dates.") from exc
        if day_end < day_start:
            raise ValueError("End date must be greater than or equal to start date.")
        return day_start, day_end

    if scope_mode == "full_range":
        return available_dates[0], available_dates[-1]

    raise ValueError(f"Unsupported scope mode: {scope_mode}")


def _manual_session_id(scope_mode: str | None, start_date: date, end_date: date) -> str:
    prefix = "manual"
    if scope_mode == "full_range":
        prefix = "full"
    if scope_mode == "latest_day":
        prefix = "latest"
    if scope_mode == "selected_day":
        prefix = "day"
    return f"{prefix}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"


def _range_label(metadata: dict[str, Any]) -> str:
    filt = metadata.get("filter", {})
    start = filt.get("start_date") or "?"
    end = filt.get("end_date") or "?"
    return f"{start}..{end}"


def _session_row(session_id: str, session_dir: Any, metadata: dict[str, Any]) -> dict[str, Any]:
    status_parts: list[str] = []
    for step_name, step_scripts in WORKFLOW_STEPS:
        status_parts.append(f"{step_name}: {workflow_step_status(metadata, step_scripts)}")
    outputs = _analysis_outputs(metadata)
    return {
        "session_id": session_id,
        "range": _range_label(metadata),
        "updated_at": metadata.get("updated_at") or metadata.get("created_at") or "n/a",
        "status_summary": "; ".join(status_parts) if status_parts else "n/a",
        "outputs_summary": _outputs_summary(outputs),
        "playback_summary": _playback_summary_from_metadata(session_dir, metadata),
    }


def _analysis_outputs(metadata: dict[str, Any]) -> list[str]:
    outputs: list[str] = []
    scripts = metadata.get("scripts", {}) if isinstance(metadata.get("scripts"), dict) else {}
    for script_key, entry in sorted(scripts.items()):
        if not isinstance(entry, dict):
            continue
        if entry.get("output_path"):
            outputs.append(str(script_key))
    return outputs


def _outputs_summary(outputs: list[str]) -> str:
    if not outputs:
        return "No analysis outputs recorded"
    if len(outputs) <= 3:
        return ", ".join(outputs)
    return f"{', '.join(outputs[:3])} +{len(outputs) - 3} more"


def _playback_summary_from_metadata(session_dir: Any, metadata: dict[str, Any]) -> str:
    if playback_exports_are_reusable(session_dir, metadata):
        return "Playback exports ready"
    ready, missing = playback_readiness(session_dir, metadata)
    if ready:
        export_dir = session_playback_export_dir(session_dir, metadata)
        if export_dir.exists():
            return "Playback exports present; rebuild recommended"
        return "Ready to build playback exports"
    user_facing_missing = [item.replace("session filtered data", "dataset filtered data") for item in missing]
    return "Playback not ready: " + "; ".join(user_facing_missing)


def _selected_dataset_summary(session: Any, metadata: dict[str, Any] | None) -> dict[str, Any]:
    metadata = metadata or dict(getattr(session, "metadata", {}) or {})
    outputs = _analysis_outputs(metadata)
    return {
        "session_id": session.session_id,
        "range": _range_label(metadata),
        "updated_at": metadata.get("updated_at") or metadata.get("created_at") or "n/a",
        "status_summary": "; ".join(
            f"{step_name}: {workflow_step_status(metadata, step_scripts)}" for step_name, step_scripts in WORKFLOW_STEPS
        ),
        "outputs": outputs,
        "outputs_summary": _outputs_summary(outputs),
        "playback_summary": _playback_summary_from_metadata(session.session_dir, metadata),
    }


def _normalized_metadata_view(session: Any, script_options: list[ScriptOption]) -> dict[str, Any]:
    metadata = deepcopy(dict(session.metadata))
    normalized, _ = normalize_session_metadata(session.session_dir, metadata, script_options)
    return normalized


_CONTROL_PANEL_SERVICE: ControlPanelService | None = None


def get_control_panel_service() -> ControlPanelService:
    global _CONTROL_PANEL_SERVICE
    if _CONTROL_PANEL_SERVICE is None:
        _CONTROL_PANEL_SERVICE = ControlPanelService()
    return _CONTROL_PANEL_SERVICE
