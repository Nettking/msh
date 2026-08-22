"""Flask-first runtime orchestration for workflow sessions and playback readiness.

This module intentionally coordinates existing runner primitives instead of
implementing a separate workflow engine. Its contract is operational: start the
web UI quickly, prepare the latest day for playback, then catch up historical
days with best-effort script execution while persisting enough state for Flask
views to explain what is ready.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import threading
import traceback
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from catalog.common.artifact_registry import configured_scan_dirs, scan_artifacts
from catalog.common.basic_metrics import basic_metrics_path, build_basic_metrics_dataset
from catalog.common.data_loading import iter_jsonl_files
from catalog.runner.data_filtering import (
    date_range_source_signature,
    discover_available_dates,
    ensure_session_filtered_data,
    source_date_signatures,
)
from catalog.runner.playback import playback_readiness, prepare_session_playback_exports
from catalog.runner.script_catalog import discover_runnable_scripts, repo_root
from catalog.runner.script_exec import execute_script_for_session
from catalog.runner.session_store import (
    AUTOMATIC_RUNTIME_SCRIPT_KEYS,
    MANUAL_DEEP_SCRIPT_KEYS,
    initialize_session_metadata,
    list_sessions,
    normalize_session_metadata,
    script_output_exists,
    write_session_metadata,
)


@dataclass
class OrchestrationResult:
    session_id: str
    session_dir: Path
    artifacts: list[dict[str, Any]]
    warnings: list[str]
    script_results: list[dict[str, Any]]
    failed_scripts: list[str]


# Explicit operational policy choices for this orchestration layer.
#
# This pipeline is currently a practical wrapper around existing runner
# execution/session helpers (data filtering, script execution, session metadata).
#
# Behavioral model:
# - date policy: latest discovered day only
# - execution policy: best effort (continue after individual script failures)
# - handoff policy: start Flask even if some preparation steps failed
DATE_POLICY_BOOTSTRAP_LATEST_DAY = "latest_discovered_day_only"
EXECUTION_POLICY_BEST_EFFORT = "best_effort_continue_on_failure"
FLASK_HANDOFF_POLICY_ALWAYS = "always_handoff"
UPDATE_POLICY_INCREMENTAL = "poll_for_new_data_then_queue_new_slice"
#: Discovery creates durable federation jobs. It never decides that this machine
#: executes them; the federation scheduler selects a provider, which may or may
#: not be this node.
ANALYSIS_DISPATCH_POLICY = "federated_job_dispatch"
HISTORICAL_CATCH_UP_POLICY = "reverse_chronological_one_day_per_cycle"
BOOTSTRAP_REFRESH_POLICY = "always_refresh_latest_day_on_startup"
AUTO_COVERAGE_CONTRACT = "runtime_playback_ready_outputs"
AUTO_COVERAGE_SCRIPT_KEYS: tuple[str, ...] = AUTOMATIC_RUNTIME_SCRIPT_KEYS
BOOTSTRAP_FULL_ANALYSIS_POLICY = "latest_day_playback_ready_analysis_before_catch_up"
BOOTSTRAP_FULL_ANALYSIS_EXCLUDED_SCRIPT_KEYS: tuple[str, ...] = MANUAL_DEEP_SCRIPT_KEYS
DEFAULT_POLL_INTERVAL_SECONDS = 60
STARTUP_MODE_PENDING = "pending_choice"
STARTUP_MODE_CONTINUE = "continue_existing"
STARTUP_MODE_CLEAN = "start_clean"


class StatusPrinter:
    """Small stdout logger used before Flask logging is available."""

    def info(self, message: str) -> None:
        print(f"[orchestrator] {message}", flush=True)

    def warn(self, message: str) -> None:
        print(f"[orchestrator][warn] {message}", flush=True)


@dataclass
class RuntimeState:
    mode: str
    phase: str
    bootstrap_policy: str
    catch_up_policy: str
    app_started_at: str | None
    runtime_started_at: str | None
    discovery_started_at: str | None
    discovery_complete: bool
    bootstrap_started_at: str | None
    bootstrap_complete: bool
    bootstrap_full_analysis_started_at: str | None
    bootstrap_full_analysis_complete_at: str | None
    historical_catch_up_started_at: str | None
    historical_catch_up_complete: bool
    current_processing_phase: str
    currently_processing_date: str | None
    last_completed_step: str | None
    last_completed_date: str | None
    next_queued_date: str | None
    current_range_start: str | None
    current_range_end: str | None
    bootstrap_date: str | None
    last_bootstrap_date: str | None
    last_processed_date: str | None
    last_discovered_date: str | None
    earliest_available_source_date: str | None
    latest_available_source_date: str | None
    processed_dates: list[str]
    processed_days_count: int
    fully_processed_days_count: int
    total_available_days: int
    pending_dates_count: int
    next_planned_date: str | None
    catch_up_status: str
    catch_up_complete: bool
    last_catchup_success_at: str | None
    last_source_signature: str | None
    last_successful_refresh: str | None
    last_update_check_at: str | None
    update_running: bool
    new_data_detected: bool
    last_failure: str | None
    session_id: str | None
    failed_scripts: list[str]
    bootstrap_full_analysis_scripts: list[str]
    bootstrap_full_analysis_excluded_scripts: list[str]
    processed_dates_truth_model: str
    automatic_coverage_contract: str
    startup_mode: str
    startup_decision_source: str
    active_runtime_namespace: str
    active_execution_id: str | None
    completed_execution_id: str | None
    completed_execution_succeeded: bool | None
    # Federation job state. Discovery creates durable jobs; a provider selected by
    # the federation scheduler executes them, so runtime progress is reconciled
    # from durable job status rather than from an in-process return value.
    analysis_dispatch_mode: str
    pending_analysis_jobs: list[str]
    pending_analysis_slices: dict[str, str]
    completed_analysis_slices: dict[str, str]
    failed_analysis_jobs: dict[str, str]
    last_analysis_job_id: str | None
    last_analysis_job_status: str | None
    last_analysis_provider_id: str | None
    last_analysis_node_id: str | None


def _text_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return sorted({str(item) for item in value if isinstance(item, (str, int))})


def _date_signature_map(value: Any) -> dict[str, str]:
    """Return a persisted ``date -> source signature`` map, ignoring bad rows."""

    if not isinstance(value, dict):
        return {}
    normalized: dict[str, str] = {}
    for key, signature in value.items():
        if not isinstance(key, str) or not isinstance(signature, str):
            continue
        try:
            date.fromisoformat(key)
        except ValueError:
            continue
        normalized[key] = signature
    return normalized


def _canonical_scan_roots() -> list[str]:
    roots = configured_scan_dirs()
    preferred = ["data", "results"]
    for root in preferred:
        if root not in roots:
            roots.append(root)
    return roots


def _safe_namespace(namespace: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_\-]", "_", namespace.strip())
    return cleaned[:48] if cleaned else "default"


def _auto_session_id(start_date: str, end_date: str, *, runtime_namespace: str) -> str:
    namespace = _safe_namespace(runtime_namespace)
    return f"auto_{namespace}_{start_date.replace('-', '')}_{end_date.replace('-', '')}"


def _load_or_create_auto_session(
    *,
    workflows_root: Path,
    start_date,
    end_date,
    script_options,
    runtime_namespace: str,
):
    session_id = _auto_session_id(
        start_date.isoformat(),
        end_date.isoformat(),
        runtime_namespace=runtime_namespace,
    )
    session_dir = workflows_root / session_id
    if session_dir.exists():
        metadata_path = session_dir / "session_state.json"
        if metadata_path.exists():
            import json

            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            metadata, changed = normalize_session_metadata(
                session_dir, metadata, script_options
            )
            runtime_payload = metadata.setdefault("runtime", {})
            if runtime_payload.get("runtime_namespace") != runtime_namespace:
                runtime_payload["runtime_namespace"] = runtime_namespace
                changed = True
            if changed:
                write_session_metadata(session_dir, metadata)
            return session_id, session_dir, metadata, "reused"

    session_dir.mkdir(parents=True, exist_ok=True)
    metadata = initialize_session_metadata(
        session_id,
        start_date,
        end_date,
        start_hour=None,
        end_hour=None,
        runtime_namespace=runtime_namespace,
        script_options=script_options,
    )
    write_session_metadata(session_dir, metadata)
    return session_id, session_dir, metadata, "created"


def _blocked_by_failure_message(days: Sequence[date]) -> str | None:
    """Describe slices held back because that exact source already failed."""

    if not days:
        return None
    listed = ", ".join(day.isoformat() for day in sorted(days, reverse=True))
    return (
        f"Analysis failed for {listed}; that source will be offered again when "
        "new data arrives for it"
    )


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _source_signature(data_dir: Path) -> str:
    digest = hashlib.sha256()
    for file_path in sorted(iter_jsonl_files(data_dir, recursive=True)):
        stat_result = file_path.stat()
        digest.update(str(file_path.relative_to(data_dir)).encode("utf-8"))
        digest.update(str(stat_result.st_mtime_ns).encode("utf-8"))
        digest.update(str(stat_result.st_size).encode("utf-8"))
    return digest.hexdigest()[:16]


def _machine_day_summary_path(workflows_root: Path, session_id: str) -> Path:
    return (
        workflows_root
        / session_id
        / "analyses"
        / "data_pr_day"
        / "machine_day_summary.csv"
    )


def _machine_contract_state(
    workflows_root: Path, session_id: str | None
) -> tuple[str, str]:
    if not session_id:
        return "waiting", "Machine/day aggregation is waiting for a workflow session."
    csv_path = _machine_day_summary_path(workflows_root, session_id)
    if not csv_path.exists():
        return (
            "waiting",
            "Machine/day aggregation is manual and not generated yet for the selected session.",
        )
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                return (
                    "failed",
                    "Machine/day aggregation CSV is invalid: missing header row.",
                )
            required = {"date", "machine", "value"}
            missing = sorted(required - set(reader.fieldnames))
            if missing:
                return (
                    "failed",
                    "Machine/day aggregation CSV is invalid: missing "
                    + ", ".join(missing)
                    + ".",
                )
            first_row = next(reader, None)
            if first_row is None:
                return "failed", "Machine/day aggregation CSV is empty."
    except OSError as exc:
        return (
            "failed",
            f"Machine/day aggregation exists but could not be read: {exc.__class__.__name__}.",
        )
    return "ready", "Machine/day artifact is available for the selected session."


def _format_filter_progress_context(
    *, active_slice: date | str | None, remaining_slices: int | None
) -> str:
    """Return generic slice context for filter status logs."""
    parts = []
    if active_slice is not None:
        parts.append(
            f"active_slice={active_slice.isoformat() if isinstance(active_slice, date) else active_slice}"
        )
    if remaining_slices is not None:
        parts.append(f"remaining_slices={remaining_slices}")
    return "; " + ", ".join(parts) if parts else ""


def _run_for_date_slice(
    *,
    status: StatusPrinter,
    workflows_root: Path,
    data_dir: Path,
    script_options,
    target_day: date,
    script_keys: tuple[str, ...],
    run_label: str,
    mark_bootstrap_full_analysis_complete: bool = False,
    runtime_namespace: str,
    active_slice: date | str | None = None,
    remaining_slices: int | None = None,
) -> OrchestrationResult:
    """Prepare one single-day automatic session and run the requested script contract.

    This is the *worker-side* analysis implementation. It is invoked by the
    capability handler on whichever provider the federation scheduler selected,
    against the data slice that provider was authorized to retrieve. Discovery
    never calls it directly.

    It has side effects: creates/reuses session directories, filters data, writes
    derived metrics, updates script metadata, may create playback exports, and
    rescans artifacts. Individual script failures are captured and returned
    instead of aborting the whole cycle.
    """
    session_id, session_dir, metadata, session_mode = _load_or_create_auto_session(
        workflows_root=workflows_root,
        start_date=target_day,
        end_date=target_day,
        script_options=script_options,
        runtime_namespace=runtime_namespace,
    )
    status.info(
        f"{session_mode} bootstrap/update session: {session_id} ({target_day.isoformat()})"
    )

    matched_records, matched_files, filter_status = ensure_session_filtered_data(
        source_data_dir=data_dir,
        session_dir=session_dir,
        metadata=metadata,
        active_slice=active_slice,
        remaining_slices=remaining_slices,
    )
    filter_progress_context = _format_filter_progress_context(
        active_slice=active_slice,
        remaining_slices=remaining_slices,
    )
    if filter_status == "cached":
        status.info(
            "skipping filter step (up-to-date"
            f"{filter_progress_context}): {matched_records} records across {matched_files} files"
        )
    else:
        status.info(
            "prepared filtered session data"
            f"{filter_progress_context}: {matched_records} records across {matched_files} files"
        )

    filtered_data_dir = session_dir / str(metadata["paths"]["filtered_data_dir"])
    derived_dataset = basic_metrics_path(filtered_data_dir)
    # The derived metrics file is the startup/catch-up fast path: health scripts
    # can read timestamp/machine/sequence without each reparsing every JSONL row.
    if filter_status == "cached" and derived_dataset.exists():
        status.info(f"reusing derived metrics dataset: {derived_dataset}")
    else:
        derived_path, derived_rows = build_basic_metrics_dataset(filtered_data_dir)
        status.info(
            f"prepared derived metrics dataset: {derived_rows} rows at {derived_path}"
        )

    script_index = {item.key: item for item in script_options}
    script_results: list[dict[str, Any]] = []
    failed_scripts: list[str] = []
    for script_key in script_keys:
        script = script_index.get(script_key)
        if script is None:
            status.warn(f"configured script was not discovered: {script_key}")
            continue
        status.info(f"running analysis step: {script_key}")
        try:
            state, exit_code = execute_script_for_session(
                session_dir=session_dir,
                metadata=metadata,
                script=script,
                # A rebuilt filter invalidates analysis outputs derived from the
                # previous source signature. Preserve normal cache reuse only
                # when the filtered input itself was reused.
                force_rerun=filter_status == "created",
            )
        except Exception as exc:  # pragma: no cover - defensive logging path
            failed_scripts.append(script_key)
            script_results.append(
                {"script": script_key, "state": "crashed", "exit_code": None}
            )
            status.warn(
                f"{script_key} crashed before completion: {exc.__class__.__name__}: {exc}. "
                f"continuing due to execution policy {EXECUTION_POLICY_BEST_EFFORT}"
            )
            status.warn(
                "stack trace follows:\n" + "".join(traceback.format_exception(exc))
            )
            continue
        script_results.append(
            {"script": script_key, "state": state, "exit_code": exit_code}
        )
        if state == "skipped_cached":
            status.info(f"skipping {script_key}: output is up to date")
            continue
        if exit_code == 0:
            status.info(f"completed {script_key}")
        else:
            failed_scripts.append(script_key)
            status.warn(
                f"{script_key} failed with exit code {exit_code}; continuing due to "
                f"execution policy {EXECUTION_POLICY_BEST_EFFORT}"
            )

    # Playback readiness is intentionally modest here: filtered data must exist
    # before timeline export generation can normalize rows into playback schema.
    ready, missing = playback_readiness(session_dir, metadata)
    if ready:
        export_path, export_state = prepare_session_playback_exports(
            session_dir, metadata
        )
        if export_state == "cached":
            status.info(f"playback export already fresh: {export_path}")
        else:
            status.info(f"generated playback export: {export_path}")
    else:
        for item in missing:
            status.warn(f"playback prerequisite missing: {item}")

    if mark_bootstrap_full_analysis_complete:
        runtime_payload = metadata.setdefault("runtime", {})
        runtime_payload["latest_day_full_analysis"] = {
            "status": "complete",
            "completed_at": _utc_now_iso(),
            "target_day": target_day.isoformat(),
            "run_label": run_label,
            "script_keys": list(script_keys),
            "excluded_script_keys": list(BOOTSTRAP_FULL_ANALYSIS_EXCLUDED_SCRIPT_KEYS),
        }
        write_session_metadata(session_dir, metadata)

    artifacts, warnings = scan_artifacts(_canonical_scan_roots())
    return OrchestrationResult(
        session_id, session_dir, artifacts, warnings, script_results, failed_scripts
    )


class RuntimeOrchestrator:
    """Own the background discovery state machine used by Flask and /control.

    The orchestrator persists progress to JSON so UI views can distinguish
    availability (Flask is up) from readiness (data/session/playback artifacts are
    prepared).

    Discovery is deliberately *only* discovery. When a date slice needs analysis
    the orchestrator creates a durable federation job and hands it to the
    capability scheduler; it never decides that this machine executes the work.
    Progress is then reconciled from durable job state.
    """

    def __init__(
        self,
        *,
        poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS,
        analysis_gateway: Any | None = None,
    ) -> None:
        self.status = StatusPrinter()
        # Discovery only creates work. The gateway turns a discovered slice into a
        # durable federation job; it is injected so tests and alternative
        # deployments can supply their own without touching discovery logic.
        self._analysis_gateway = analysis_gateway
        self.root = repo_root()
        self.data_dir = self.root / "data"
        self.workflows_root = self.root / "results" / "workflows"
        self.workflows_root.mkdir(parents=True, exist_ok=True)
        self.state_path = self.workflows_root / "runtime_state.json"
        self.startup_state_path = self.workflows_root / "startup_state.json"
        self.poll_interval_seconds = max(int(poll_interval_seconds), 10)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state = self._load_state()
        env_mode = self._load_startup_mode_from_env()
        if env_mode:
            with self._lock:
                self._apply_startup_mode(env_mode, source="env")
        else:
            context = self._startup_decision_context()
            # Existing sessions/runtime state can represent valuable research artifacts.
            # Require an explicit UI/env decision before reusing or isolating them.
            if context.get("requires_choice"):
                self._state.startup_mode = STARTUP_MODE_PENDING
                self._state.startup_decision_source = "pending_user_choice"
                self._state.active_runtime_namespace = "default"
                self._persist_state()

    def _default_state(self) -> RuntimeState:
        return RuntimeState(
            mode="app_started_runtime_pending",
            phase="runtime_not_started",
            bootstrap_policy=DATE_POLICY_BOOTSTRAP_LATEST_DAY,
            catch_up_policy=HISTORICAL_CATCH_UP_POLICY,
            app_started_at=None,
            runtime_started_at=None,
            discovery_started_at=None,
            discovery_complete=False,
            bootstrap_started_at=None,
            bootstrap_complete=False,
            bootstrap_full_analysis_started_at=None,
            bootstrap_full_analysis_complete_at=None,
            historical_catch_up_started_at=None,
            historical_catch_up_complete=False,
            current_processing_phase="runtime_not_started",
            currently_processing_date=None,
            last_completed_step=None,
            last_completed_date=None,
            next_queued_date=None,
            current_range_start=None,
            current_range_end=None,
            bootstrap_date=None,
            last_bootstrap_date=None,
            last_processed_date=None,
            last_discovered_date=None,
            earliest_available_source_date=None,
            latest_available_source_date=None,
            processed_dates=[],
            processed_days_count=0,
            fully_processed_days_count=0,
            total_available_days=0,
            pending_dates_count=0,
            next_planned_date=None,
            catch_up_status="idle",
            catch_up_complete=False,
            last_catchup_success_at=None,
            last_source_signature=None,
            last_successful_refresh=None,
            last_update_check_at=None,
            update_running=False,
            new_data_detected=False,
            last_failure=None,
            session_id=None,
            failed_scripts=[],
            bootstrap_full_analysis_scripts=[],
            bootstrap_full_analysis_excluded_scripts=list(
                BOOTSTRAP_FULL_ANALYSIS_EXCLUDED_SCRIPT_KEYS
            ),
            processed_dates_truth_model="verified_session_outputs",
            automatic_coverage_contract=AUTO_COVERAGE_CONTRACT,
            startup_mode=STARTUP_MODE_CONTINUE,
            startup_decision_source="default",
            active_runtime_namespace="default",
            active_execution_id=None,
            completed_execution_id=None,
            completed_execution_succeeded=None,
            analysis_dispatch_mode=ANALYSIS_DISPATCH_POLICY,
            pending_analysis_jobs=[],
            pending_analysis_slices={},
            completed_analysis_slices={},
            failed_analysis_jobs={},
            last_analysis_job_id=None,
            last_analysis_job_status=None,
            last_analysis_provider_id=None,
            last_analysis_node_id=None,
        )

    def _load_state(self) -> RuntimeState:
        default = self._default_state()
        if not self.state_path.exists():
            return default
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return default
        if not isinstance(payload, dict):
            return default
        fields = {
            field: payload.get(field, getattr(default, field))
            for field in default.__dataclass_fields__
        }
        state = RuntimeState(**fields)
        if not isinstance(state.processed_dates, list):
            state.processed_dates = []
        state.processed_dates = sorted({str(item) for item in state.processed_dates})
        if state.bootstrap_date is None:
            state.bootstrap_date = state.last_bootstrap_date
        state.automatic_coverage_contract = AUTO_COVERAGE_CONTRACT
        state.bootstrap_full_analysis_excluded_scripts = list(
            BOOTSTRAP_FULL_ANALYSIS_EXCLUDED_SCRIPT_KEYS
        )
        # In-flight scheduler state belongs to the process that owned the
        # worker. Persisted ownership cannot survive a process restart.
        state.update_running = False
        state.active_execution_id = None
        # Federation job tracking is durable, but the JSON file is operator
        # visible. Normalize it so a hand-edited or truncated file degrades to
        # "nothing tracked" rather than crashing the discovery loop.
        state.analysis_dispatch_mode = ANALYSIS_DISPATCH_POLICY
        state.pending_analysis_jobs = _text_list(state.pending_analysis_jobs)
        state.pending_analysis_slices = _date_signature_map(
            state.pending_analysis_slices
        )
        state.completed_analysis_slices = _date_signature_map(
            state.completed_analysis_slices
        )
        state.failed_analysis_jobs = _date_signature_map(
            state.failed_analysis_jobs
        )
        return state

    def _startup_decision_context(self) -> dict[str, Any]:
        sessions = list_sessions(self.workflows_root)
        has_runtime_state = self.state_path.exists()
        return {
            "requires_choice": bool(sessions or has_runtime_state),
            "existing_sessions_count": len(sessions),
            "has_runtime_state": has_runtime_state,
            "startup_state_path": str(self.startup_state_path),
        }

    def _load_startup_mode_from_env(self) -> str | None:
        raw = str(os.getenv("FCP_STARTUP_MODE", "")).strip().lower()
        if raw in {"continue", "continue_existing", "resume"}:
            return STARTUP_MODE_CONTINUE
        if raw in {"clean", "start_clean", "fresh"}:
            return STARTUP_MODE_CLEAN
        return None

    def _apply_startup_mode(self, mode: str, *, source: str) -> None:
        """Apply continue-vs-clean startup policy and persist the operator decision."""
        namespace = "default"
        # Clean starts do not delete old sessions; they move automatic sessions
        # into a timestamped namespace so prior artifacts remain inspectable.
        if mode == STARTUP_MODE_CLEAN:
            namespace = f"clean_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"
            self.status.info(
                "clean startup selected: preserving existing workflow directories but isolating playback "
                f"to new runtime namespace '{namespace}'; stale exports from prior namespaces will be ignored"
            )

        app_started_at = self._state.app_started_at
        runtime_started_at = self._state.runtime_started_at
        base = self._default_state()
        base.app_started_at = app_started_at
        base.runtime_started_at = runtime_started_at
        base.startup_mode = mode
        base.startup_decision_source = source
        base.active_runtime_namespace = namespace

        if mode == STARTUP_MODE_CONTINUE and self.state_path.exists():
            loaded = self._load_state()
            loaded.startup_mode = mode
            loaded.startup_decision_source = source
            loaded.active_runtime_namespace = (
                loaded.active_runtime_namespace or "default"
            )
            self._state = loaded
        else:
            self._state = base

        payload = {
            "chosen_at": _utc_now_iso(),
            "mode": mode,
            "source": source,
            "active_runtime_namespace": self._state.active_runtime_namespace,
        }
        self.startup_state_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        self._persist_state()

    def startup_decision_snapshot(self) -> dict[str, Any]:
        context = self._startup_decision_context()
        state = self.state_snapshot()
        state.update(context)
        return state

    def requires_startup_choice(self) -> bool:
        with self._lock:
            if self._state.startup_mode != STARTUP_MODE_PENDING:
                return False
        return self._startup_decision_context().get("requires_choice", False)

    def choose_startup_mode(self, mode: str) -> tuple[bool, str]:
        """Resolve a UI startup choice and start background processing if valid."""
        resolved = mode.strip().lower()
        mapped = {
            "continue": STARTUP_MODE_CONTINUE,
            "continue_existing": STARTUP_MODE_CONTINUE,
            "start_clean": STARTUP_MODE_CLEAN,
            "clean": STARTUP_MODE_CLEAN,
        }.get(resolved)
        if not mapped:
            return False, "Unsupported startup mode selection."

        with self._lock:
            self._apply_startup_mode(mapped, source="ui")
        self.start_background_updates()
        return True, f"Startup mode set to {mapped.replace('_', ' ')}."

    def _persist_state(self) -> None:
        self.state_path.write_text(
            json.dumps(self._state.__dict__, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def state_snapshot(self) -> dict[str, Any]:
        with self._lock:
            snapshot = dict(self._state.__dict__)
        snapshot["view_contracts"] = self._view_contracts(snapshot)
        return snapshot

    def mark_app_started(self) -> None:
        with self._lock:
            if not self._state.app_started_at:
                self._state.app_started_at = _utc_now_iso()
            if self._state.phase == "runtime_not_started":
                self._state.mode = "webapp_available"
            self._persist_state()

    def _mark_runtime_started(self) -> None:
        with self._lock:
            if not self._state.runtime_started_at:
                self._state.runtime_started_at = _utc_now_iso()
            self._state.mode = "runtime_background_active"
            self._state.phase = "runtime_background_active"
            self._state.current_processing_phase = "discovery_pending"
            self._persist_state()

    def _view_contracts(self, snapshot: dict[str, Any]) -> dict[str, dict[str, str]]:
        running = bool(snapshot.get("runtime_started_at"))
        discovery_complete = bool(snapshot.get("discovery_complete"))
        catch_up_complete = bool(snapshot.get("historical_catch_up_complete"))
        machine_state, machine_message = _machine_contract_state(
            self.workflows_root, snapshot.get("session_id")
        )

        return {
            "status": {
                "state": "ready",
                "message": "Status page is startup-safe and available immediately.",
            },
            "control": {
                "state": "ready",
                "message": "Control page is startup-safe and available immediately.",
            },
            "machine": {
                "state": machine_state,
                "message": machine_message,
            },
            "historical_catch_up": {
                "state": (
                    "complete"
                    if catch_up_complete
                    else ("running" if running and discovery_complete else "waiting")
                ),
                "message": (
                    "Historical catch-up is complete. Runtime is polling for new days."
                    if catch_up_complete
                    else (
                        "Historical catch-up is running in the background one day at a time."
                        if discovery_complete and running
                        else "Historical catch-up will begin after latest-day playback-ready analysis finishes."
                    )
                ),
            },
        }

    def bootstrap(self) -> OrchestrationResult:
        with self._lock:
            self._state.mode = "bootstrap_running"
            self._state.phase = "bootstrap"
            self._state.update_running = True
            self._state.bootstrap_started_at = (
                self._state.bootstrap_started_at or _utc_now_iso()
            )
            self._persist_state()
        result = self._run_update(bootstrap=True)
        return result

    def start_background_updates(self) -> None:
        if self.requires_startup_choice():
            self.status.info(
                "runtime start deferred until startup mode is chosen in /startup"
            )
            return
        if self._thread is not None and self._thread.is_alive():
            return
        self._mark_runtime_started()
        self._thread = threading.Thread(
            target=self._poll_loop, name="fcp-runtime-poller", daemon=True
        )
        self._thread.start()

    def request_refresh(self, *, execution_id: str | None = None) -> bool:
        """Request an asynchronous discovery pass from the control panel.

        The pass discovers new data and queues durable analysis jobs. It does not
        execute analysis: a federation provider selected by the scheduler does.
        """
        with self._lock:
            if self._state.startup_mode == STARTUP_MODE_PENDING:
                self.status.warn(
                    "refresh request ignored: startup mode choice is still pending"
                )
                return False
            if (
                self._state.update_running
                or self._state.active_execution_id is not None
            ):
                return False
            # Reserve the scheduler before starting every requested worker,
            # including ordinary /refresh requests without an external job id.
            reserved_execution_id = execution_id or f"runtime-refresh-{uuid.uuid4().hex}"
            self._state.active_execution_id = reserved_execution_id
            try:
                self._persist_state()
            except Exception:
                self._state.active_execution_id = None
                raise
        worker = threading.Thread(
            target=self._run_requested_update,
            kwargs={"bootstrap": False, "execution_id": reserved_execution_id},
            daemon=True,
        )
        try:
            worker.start()
        except Exception:
            # A worker that never started must not leave the scheduler fenced.
            with self._lock:
                if self._state.active_execution_id == reserved_execution_id:
                    self._state.active_execution_id = None
                    self._persist_state()
            raise
        return True

    def _run_requested_update(
        self, *, bootstrap: bool, execution_id: str | None
    ) -> None:
        """Run an explicitly correlated refresh and durably close it on errors."""
        try:
            self._run_update(bootstrap=bootstrap, execution_id=execution_id)
        except Exception as exc:  # noqa: BLE001 - background boundary persists failure
            with self._lock:
                self._state.update_running = False
                self._state.last_failure = f"{exc.__class__.__name__}: {exc}"
                self._state.current_processing_phase = "failed"
                self._state.completed_execution_id = execution_id
                self._state.completed_execution_succeeded = False
                self._state.active_execution_id = None
                self._persist_state()
            self.status.warn(f"requested update failure: {exc}")

    def _poll_loop(self) -> None:
        self.status.info(
            "runtime background loop enabled "
            f"({UPDATE_POLICY_INCREMENTAL}; catch-up={HISTORICAL_CATCH_UP_POLICY}; "
            f"bootstrap={BOOTSTRAP_REFRESH_POLICY}; "
            f"auto_coverage={AUTO_COVERAGE_CONTRACT}:{','.join(AUTO_COVERAGE_SCRIPT_KEYS) or 'none'}; "
            f"interval={self.poll_interval_seconds}s)"
        )
        run_bootstrap_once = True
        while not self._stop.is_set():
            with self._lock:
                execution_reserved = self._state.active_execution_id is not None
            if execution_reserved:
                # Explicit user-triggered work owns this scheduling slot. Do not
                # let the periodic poller win the race between reservation and the
                # requested worker entering _run_update().
                self._stop.wait(1)
                continue
            try:
                self._run_update(bootstrap=run_bootstrap_once)
                run_bootstrap_once = False
            except Exception as exc:  # pragma: no cover
                with self._lock:
                    self._state.last_failure = f"{exc.__class__.__name__}: {exc}"
                    self._state.update_running = False
                    self._state.current_processing_phase = "failed"
                    self._persist_state()
                self.status.warn(f"background update loop failure: {exc}")
            self._stop.wait(1 if run_bootstrap_once else self.poll_interval_seconds)

    def _processed_dates(
        self,
        *,
        script_options,
        source_signatures: dict[str, str] | None = None,
    ) -> set[str]:
        """Return dates that are complete on disk or through a succeeded job.

        A day is complete when its analysis job succeeded — that is the durable
        record, regardless of which provider ran it — or when this device's own
        session outputs still satisfy the automatic contract for the current
        source signature.
        """

        processed = self._verified_processed_dates(
            script_options=script_options,
            source_signatures=source_signatures,
        )
        if source_signatures is None:
            return processed
        with self._lock:
            completed = dict(self._state.completed_analysis_slices)
        for day, signature in completed.items():
            try:
                target = date.fromisoformat(day)
            except ValueError:
                continue
            expected = date_range_source_signature(source_signatures, target, target)
            if expected == signature:
                processed.add(day)
        return processed

    def _verified_processed_dates(
        self,
        *,
        script_options,
        source_signatures: dict[str, str] | None = None,
    ) -> set[str]:
        """Return dates whose on-disk sessions satisfy the automatic contract.

        Runtime state can be stale after interrupted runs or manual file edits, so
        catch-up progress is reconciled against session metadata plus real output
        folders before deciding which dates are complete.
        """
        # Verification matches the bounded automatic catch-up contract only.
        verified: set[str] = set()
        sessions = list_sessions(self.workflows_root)
        for session in sessions:
            metadata, changed = normalize_session_metadata(
                session.session_dir, dict(session.metadata), script_options
            )
            if changed:
                write_session_metadata(session.session_dir, metadata)
            runtime_payload = (
                metadata.get("runtime")
                if isinstance(metadata.get("runtime"), dict)
                else {}
            )
            session_namespace = str(
                runtime_payload.get("runtime_namespace") or "default"
            )
            if session_namespace != str(
                self._state.active_runtime_namespace or "default"
            ):
                continue
            filter_payload = metadata.get("filter", {})
            start_date = filter_payload.get("start_date")
            end_date = filter_payload.get("end_date")
            if (
                not isinstance(start_date, str)
                or not isinstance(end_date, str)
                or start_date != end_date
            ):
                continue
            if source_signatures is not None:
                filter_result = metadata.get("filter_result", {})
                if not isinstance(filter_result, dict):
                    continue
                try:
                    expected_source_signature = date_range_source_signature(
                        source_signatures,
                        date.fromisoformat(start_date),
                        date.fromisoformat(end_date),
                    )
                except ValueError:
                    continue
                if (
                    str(filter_result.get("source_signature") or "")
                    != expected_source_signature
                ):
                    continue
            filtered_dir = session.session_dir / str(
                metadata.get("paths", {}).get("filtered_data_dir", "data")
            )
            if not filtered_dir.exists():
                continue
            scripts_meta = metadata.get("scripts", {})
            session_verified = True
            for script_key in AUTO_COVERAGE_SCRIPT_KEYS:
                script_entry = scripts_meta.get(script_key, {})
                if script_entry.get("status") != "done":
                    session_verified = False
                    break
                if not script_output_exists(session.session_dir, script_entry):
                    session_verified = False
                    break
            if session_verified:
                verified.add(start_date)
        return verified

    def _apply_progress_state(
        self,
        *,
        available_dates: list[date],
        verified_processed_dates: set[str],
    ) -> tuple[list[date], list[date], set[str], set[str]]:
        """Reconcile discovered dates with verified outputs and update catch-up counters."""
        state_processed = {item for item in self._state.processed_dates}
        available_iso = [item.isoformat() for item in available_dates]
        available_set = set(available_iso)
        # Truth model: processed coverage is derived strictly from verified outputs.
        # Persisted state_processed is diagnostic/history and may be stale.
        processed_set = {
            item for item in verified_processed_dates if item in available_set
        }
        dropped_unverified = {
            item
            for item in state_processed
            if item in available_set and item not in verified_processed_dates
        }
        processed_desc = sorted(processed_set, reverse=True)
        pending_desc = [
            item
            for item in reversed(available_dates)
            if item.isoformat() not in processed_set
        ]
        self._state.processed_dates = sorted(processed_set)
        self._state.processed_days_count = len(processed_set)
        self._state.fully_processed_days_count = self._state.processed_days_count
        self._state.total_available_days = len(available_dates)
        self._state.pending_dates_count = len(pending_desc)
        self._state.next_planned_date = (
            pending_desc[0].isoformat() if pending_desc else None
        )
        self._state.next_queued_date = self._state.next_planned_date
        self._state.catch_up_complete = len(pending_desc) == 0
        self._state.historical_catch_up_complete = self._state.catch_up_complete
        self._state.catch_up_status = (
            "complete" if self._state.catch_up_complete else "running"
        )
        if processed_desc:
            self._state.current_range_start = processed_desc[-1]
            self._state.current_range_end = processed_desc[0]
        else:
            self._state.current_range_start = None
            self._state.current_range_end = None
        return available_dates, pending_desc, processed_set, dropped_unverified

    def _run_update(
        self, *, bootstrap: bool, execution_id: str | None = None
    ) -> OrchestrationResult:
        """Run one bootstrap or incremental catch-up cycle.

        Bootstrap always targets the latest discovered day. Non-bootstrap cycles
        process at most one pending day so Flask remains responsive and newly
        arriving days can be picked up by later polling iterations.
        """
        with self._lock:
            if self._state.startup_mode == STARTUP_MODE_PENDING:
                return OrchestrationResult("none", self.workflows_root, [], [], [], [])
            if (
                self._state.active_execution_id is not None
                and self._state.active_execution_id != execution_id
            ):
                return OrchestrationResult("none", self.workflows_root, [], [], [], [])
            if self._state.update_running and not bootstrap:
                return OrchestrationResult("none", self.workflows_root, [], [], [], [])
            now = _utc_now_iso()
            self._state.update_running = True
            self._state.phase = "bootstrap" if bootstrap else "historical_catch_up"
            self._state.mode = (
                "bootstrap_running" if bootstrap else "incremental_refresh_running"
            )
            self._state.current_processing_phase = (
                "bootstrap_latest_day_playback_ready_analysis"
                if bootstrap
                else "historical_catch_up"
            )
            self._state.last_update_check_at = now
            self._state.discovery_started_at = self._state.discovery_started_at or now
            self._state.currently_processing_date = None
            if bootstrap:
                self._state.bootstrap_started_at = (
                    self._state.bootstrap_started_at or now
                )
            else:
                self._state.historical_catch_up_started_at = (
                    self._state.historical_catch_up_started_at or now
                )
            self._persist_state()

        artifacts, warnings = scan_artifacts(_canonical_scan_roots())
        if not self.data_dir.exists():
            self.status.warn(
                f"data directory is missing at {self.data_dir}; Flask will run in scan-only mode"
            )
            with self._lock:
                self._state.update_running = False
                self._state.phase = "idle"
                self._state.mode = "scan_only"
                self._state.discovery_complete = True
                self._state.current_processing_phase = "idle_no_data_dir"
                if execution_id is not None:
                    self._state.completed_execution_id = execution_id
                    self._state.completed_execution_succeeded = True
                    self._state.active_execution_id = None
                self._persist_state()
            return OrchestrationResult(
                "none", self.workflows_root, artifacts, warnings, [], []
            )

        available_dates = discover_available_dates(self.data_dir)
        if not available_dates:
            self.status.warn("no dates discovered in data/; skipping analysis pipeline")
            with self._lock:
                self._state.update_running = False
                self._state.phase = "idle"
                self._state.mode = "idle_no_data"
                self._state.catch_up_status = "idle"
                self._state.discovery_complete = True
                self._state.current_processing_phase = "idle_no_discovered_dates"
                if execution_id is not None:
                    self._state.completed_execution_id = execution_id
                    self._state.completed_execution_succeeded = True
                    self._state.active_execution_id = None
                self._persist_state()
            return OrchestrationResult(
                "none", self.workflows_root, artifacts, warnings, [], []
            )

        current_source_signatures = source_date_signatures(self.data_dir)
        script_options = discover_runnable_scripts(self.root / "catalog")
        if not script_options:
            self.status.warn(
                "no runnable scripts discovered; skipping analysis pipeline"
            )
            with self._lock:
                self._state.update_running = False
                self._state.phase = "idle"
                self._state.mode = "idle_no_scripts"
                self._state.discovery_complete = True
                self._state.current_processing_phase = "idle_no_scripts"
                if execution_id is not None:
                    self._state.completed_execution_id = execution_id
                    self._state.completed_execution_succeeded = False
                    self._state.active_execution_id = None
                self._persist_state()
            return OrchestrationResult(
                "none", self.workflows_root, artifacts, warnings, [], []
            )

        # Durable jobs are the unit of progress now, so fold any finished job into
        # runtime state before deciding what still needs to be queued.
        reconciled_failure = self._reconcile_analysis_jobs()

        verified_processed_dates = self._processed_dates(
            script_options=script_options,
            source_signatures=current_source_signatures,
        )
        latest = available_dates[-1]
        earliest = available_dates[0]
        source_sig = _source_signature(self.data_dir)
        with self._lock:
            self._state.last_discovered_date = latest.isoformat()
            self._state.earliest_available_source_date = earliest.isoformat()
            self._state.latest_available_source_date = latest.isoformat()
            self._state.last_source_signature = source_sig
            self._state.discovery_complete = True
            self._state.current_processing_phase = (
                "bootstrap_latest_day_playback_ready_analysis"
                if bootstrap
                else "historical_catch_up"
            )
            _, pending_desc, _, dropped_unverified = self._apply_progress_state(
                available_dates=available_dates,
                verified_processed_dates=verified_processed_dates,
            )
            self._persist_state()
        if dropped_unverified:
            self.status.warn(
                "reconciled runtime state with on-disk outputs; re-queued unverified day(s): "
                + ", ".join(sorted(dropped_unverified, reverse=True))
            )

        def _slice_signature(day: date) -> str:
            return date_range_source_signature(current_source_signatures, day, day)

        blocked_by_failure: list[date] = []

        # A day with a live durable job is already scheduled work, whatever its
        # source looked like when that job was created. A source that is still
        # growing — a recorder writing today, a federation mirror still syncing —
        # changes that day's signature on nearly every poll, so keying this on the
        # signature made the active day take every catch-up slot for itself and
        # stack a duplicate job per cycle for snapshots that were already stale.
        # Newer data for a day in flight is queued once that job settles, which
        # keeps one refresh in flight per day and lets catch-up keep advancing.
        with self._lock:
            awaiting_jobs = set(self._state.pending_analysis_slices)

        if bootstrap:
            # Latest-day first gives operators the freshest playback view quickly;
            # older source days are handled by the incremental catch-up loop.
            target_days = [latest]
            bootstrap_script_keys = self._bootstrap_full_analysis_script_keys(
                script_options
            )
            with self._lock:
                self._state.bootstrap_date = latest.isoformat()
                self._state.bootstrap_full_analysis_started_at = (
                    self._state.bootstrap_full_analysis_started_at or _utc_now_iso()
                )
                self._state.bootstrap_full_analysis_scripts = list(
                    bootstrap_script_keys
                )
                self._state.bootstrap_full_analysis_excluded_scripts = list(
                    BOOTSTRAP_FULL_ANALYSIS_EXCLUDED_SCRIPT_KEYS
                )
                self._state.current_processing_phase = (
                    "bootstrap_latest_day_playback_ready_analysis"
                )
                self._persist_state()
            self.status.info(
                "bootstrap phase: running playback-ready analysis for latest available day "
                f"{latest.isoformat()} (policy={BOOTSTRAP_FULL_ANALYSIS_POLICY})"
            )
        else:
            # Catch-up advances at most one never-analysed day per cycle rather
            # than doing a full historical recompute on every poll.
            #
            # Refreshing a day that already has an analysis is a separate lane. A
            # source that keeps growing — a recorder writing today, a federation
            # mirror still syncing a day — is the newest pending day on nearly
            # every cycle, so sharing one lane let it spend the whole catch-up
            # budget on itself while the backlog never moved. Each lane queues at
            # most one day per cycle, so neither can starve the other.
            with self._lock:
                analysed_before = set(self._state.completed_analysis_slices)
                failed_jobs = dict(self._state.failed_analysis_jobs)
                namespace = str(self._state.active_runtime_namespace or "default")
            # Re-offering work whose durable job already ended in a terminal
            # failure cannot produce a different outcome: it is the same job, so
            # the submission returns that same failed job. Left schedulable it
            # would take a lane on every cycle forever and block every other
            # pending day behind it.
            #
            # The comparison is the durable job identity, not the source alone.
            # A newer analysis contract, a changed automatic script set, a new
            # runtime namespace or a new federation session all make this
            # genuinely different work that has never been attempted, and only
            # the latest day would otherwise be rescued by bootstrap.
            blocked_by_failure = [
                day
                for day in pending_desc
                if day.isoformat() in failed_jobs
                and failed_jobs[day.isoformat()]
                == self._prospective_job_id(
                    target_day=day,
                    script_keys=AUTO_COVERAGE_SCRIPT_KEYS,
                    runtime_namespace=namespace,
                    source_signature=_slice_signature(day),
                )
            ]
            schedulable = [
                day
                for day in pending_desc
                if day.isoformat() not in awaiting_jobs
                and day not in blocked_by_failure
            ]
            backlog = [
                day for day in schedulable if day.isoformat() not in analysed_before
            ]
            refresh = [
                day for day in schedulable if day.isoformat() in analysed_before
            ]
            target_days = sorted(set(backlog[:1] + refresh[:1]))
            if target_days:
                queued_slices = ", ".join(
                    day.isoformat() for day in reversed(target_days)
                )
                self.status.info(
                    "historical catch-up phase: processing pending day(s) "
                    f"{queued_slices} "
                    f"(active_slice={target_days[-1].isoformat()}, "
                    f"new_days={len(backlog)}, refresh_days={len(refresh)}, "
                    f"remaining_slices={max(0, len(pending_desc) - len(target_days))}, "
                    f"pending_before_cycle={len(pending_desc)})"
                )
            else:
                self.status.info(
                    "historical catch-up phase: no pending days remain; cycle will idle"
                )

        with self._lock:
            self._state.new_data_detected = bool(target_days)
            self._persist_state()

        if target_days:
            self.status.info(
                "date policy applied "
                f"({DATE_POLICY_BOOTSTRAP_LATEST_DAY}): queueing {', '.join(day.isoformat() for day in target_days)}"
            )
            final_result = OrchestrationResult(
                "none", self.workflows_root, artifacts, warnings, [], []
            )
            requested_script_keys = (
                bootstrap_script_keys if bootstrap else AUTO_COVERAGE_SCRIPT_KEYS
            )
            submitted: list[str] = []
            queued_dates: list[tuple[str, str]] = []
            submission_failure: str | None = None
            for day in target_days:
                with self._lock:
                    self._state.currently_processing_date = day.isoformat()
                    namespace = str(self._state.active_runtime_namespace or "default")
                    self._persist_state()
                try:
                    job_id = self._submit_analysis_work(
                        target_day=day,
                        script_keys=requested_script_keys,
                        runtime_namespace=namespace,
                        source_signatures=current_source_signatures,
                    )
                except Exception as exc:  # noqa: BLE001 - discovery must stay alive
                    submission_failure = f"{exc.__class__.__name__}: {exc}"
                    self.status.warn(
                        f"could not create an analysis job for {day.isoformat()}: "
                        f"{submission_failure}"
                    )
                    continue
                if job_id is not None:
                    submitted.append(job_id)
                    queued_dates.append((day.isoformat(), _slice_signature(day)))
            with self._lock:
                self._state.session_id = _auto_session_id(
                    target_days[-1].isoformat(),
                    target_days[-1].isoformat(),
                    runtime_namespace=str(
                        self._state.active_runtime_namespace or "default"
                    ),
                )
                self._state.next_queued_date = target_days[-1].isoformat()
                self._state.last_analysis_job_id = (
                    submitted[-1] if submitted else self._state.last_analysis_job_id
                )
                self._state.pending_analysis_jobs = sorted(
                    set(self._state.pending_analysis_jobs) | set(submitted)
                )
                self._state.pending_analysis_slices = {
                    **self._state.pending_analysis_slices,
                    **dict(queued_dates),
                }
                completed_scripts = ",".join(requested_script_keys) or "none"
                self._state.last_completed_step = (
                    f"queued_analysis_job[{completed_scripts}] for "
                    f"{target_days[-1].isoformat()}"
                )
                if submission_failure is not None:
                    self._state.last_failure = submission_failure
                self._persist_state()
            # Scheduling and execution happen off this thread so discovery keeps
            # polling and Flask stays responsive.
            self._request_scheduling_pass()
        else:
            final_result = OrchestrationResult(
                "none", self.workflows_root, artifacts, warnings, [], []
            )
            with self._lock:
                self._state.failed_scripts = []
                # A quiet cycle clears the previous cycle's failure, but a
                # failure this cycle observed, and a slice held back because that
                # exact source already failed, are both conditions the operator
                # still has to see.
                self._state.last_failure = reconciled_failure or (
                    _blocked_by_failure_message(blocked_by_failure)
                )
                _, pending_desc, _, _ = self._apply_progress_state(
                    available_dates=available_dates,
                    verified_processed_dates=verified_processed_dates,
                )
                self.status.info(
                    "incremental progress unchanged: "
                    f"processed={self._state.processed_days_count}/{self._state.total_available_days}, "
                    f"remaining={len(pending_desc)}"
                )
                jobs_in_flight = bool(self._state.pending_analysis_jobs)
            if jobs_in_flight:
                # Nothing new to queue, but durable work is still in flight: a day
                # being analysed now, or a job a restart left behind. Keep the
                # lifecycle driver alive so those jobs still reach a terminal state.
                self._request_scheduling_pass()

        with self._lock:
            self._state.update_running = False
            self._state.phase = "idle"
            self._state.mode = "idle_incremental"
            self._state.currently_processing_date = None
            self._state.current_processing_phase = (
                "polling_new_data"
                if self._state.catch_up_complete
                else "historical_catch_up"
            )
            if execution_id is not None:
                self._state.completed_execution_id = execution_id
                self._state.completed_execution_succeeded = not bool(
                    self._state.last_failure or self._state.failed_scripts
                )
                self._state.active_execution_id = None
            self._persist_state()
        return final_result

    def _bootstrap_full_analysis_script_keys(self, script_options) -> tuple[str, ...]:
        discovered = {item.key for item in script_options}
        return tuple(key for key in AUTO_COVERAGE_SCRIPT_KEYS if key in discovered)

    # ------------------------------------------------------------------
    # Federation job handoff
    # ------------------------------------------------------------------

    def analysis_gateway(self):
        """Return the analysis work gateway, building the default one lazily."""

        if self._analysis_gateway is None:
            from .analysis_runtime import DiscoveryAnalysisGateway

            self._analysis_gateway = DiscoveryAnalysisGateway()
        return self._analysis_gateway

    def _submit_analysis_work(
        self,
        *,
        target_day: date,
        script_keys: tuple[str, ...],
        runtime_namespace: str,
        source_signatures: dict[str, str],
    ) -> str | None:
        """Create (or recognize) the durable job for one discovered date slice."""

        signature = date_range_source_signature(
            source_signatures, target_day, target_day
        )
        outcome = self.analysis_gateway().submit_date_slice(
            data_dir=self.data_dir,
            target_day=target_day,
            script_keys=script_keys,
            runtime_namespace=runtime_namespace,
            source_signature=signature,
        )
        if outcome is None:
            return None
        self.status.info(
            f"analysis job {'created' if outcome.created else 'already durable'} for "
            f"{target_day.isoformat()}: {outcome.job_id} ({outcome.status.value})"
        )
        return outcome.job_id

    def _prospective_job_id(
        self,
        *,
        target_day: date,
        script_keys: tuple[str, ...],
        runtime_namespace: str,
        source_signature: str,
    ) -> str | None:
        """Return the durable job id this day would be submitted as, if known."""

        try:
            return self.analysis_gateway().job_identity(
                target_day=target_day,
                script_keys=script_keys,
                runtime_namespace=runtime_namespace,
                source_signature=source_signature,
            )
        except Exception as exc:  # noqa: BLE001 - an unknown identity offers work
            self.status.warn(f"analysis job identity unavailable: {exc}")
            return None

    def _request_scheduling_pass(self) -> None:
        try:
            self.analysis_gateway().request_scheduling_pass()
        except Exception as exc:  # noqa: BLE001 - scheduling never blocks discovery
            self.status.warn(f"analysis scheduling pass could not start: {exc}")

    def _reconcile_analysis_jobs(self) -> str | None:
        """Fold durable job outcomes back into the runtime state the UI reads.

        Returns the failure this cycle observed, so a cycle that then finds
        nothing to queue reports that failure rather than clearing it.
        """

        with self._lock:
            pending = list(self._state.pending_analysis_jobs)
            if not pending and self._state.pending_analysis_slices:
                # No live job is left to hold a date in flight.
                self._state.pending_analysis_slices = {}
                self._persist_state()
        if not pending:
            return None
        try:
            gateway = self.analysis_gateway()
        except Exception as exc:  # noqa: BLE001 - runtime stays available
            self.status.warn(f"analysis job reconciliation unavailable: {exc}")
            return None
        still_pending: list[str] = []
        finished_dates: set[str] = set()
        succeeded_slices: dict[str, str] = {}
        failed_jobs: dict[str, str] = {}
        observed_failure: str | None = None
        for job_id in pending:
            view = gateway.job_view(job_id)
            if view is None or not view.get("terminal"):
                # Work is still in flight for this job's dates, or the coordinator
                # cannot read it yet. Neither is a finished slice, so the job stays
                # tracked and keeps holding its dates until it reaches a terminal
                # state on a later cycle.
                still_pending.append(job_id)
                continue
            completed = [str(item) for item in (view.get("target_dates") or [])]
            finished_dates.update(completed)
            signature = view.get("source_signature")
            if signature and view.get("succeeded"):
                succeeded_slices.update({item: str(signature) for item in completed})
            elif signature:
                # This exact work was attempted and ended badly. Remember the job
                # itself, so discovery stops re-offering that identical job while
                # still recognizing any genuinely different work for the day.
                failed_jobs.update({item: job_id for item in completed})
            with self._lock:
                self._state.last_analysis_job_id = job_id
                self._state.last_analysis_job_status = str(view.get("status"))
                self._state.last_analysis_provider_id = view.get("provider_id")
                self._state.last_analysis_node_id = view.get("node_id")
                if view.get("succeeded"):
                    processed = set(self._state.processed_dates) | set(completed)
                    self._state.processed_dates = sorted(processed)
                    if completed:
                        self._state.last_processed_date = max(completed)
                        self._state.last_completed_date = max(completed)
                    self._state.last_successful_refresh = _utc_now_iso()
                    if self._state.bootstrap_date in completed:
                        self._state.bootstrap_complete = True
                        self._state.last_bootstrap_date = self._state.bootstrap_date
                        self._state.bootstrap_full_analysis_complete_at = _utc_now_iso()
                    else:
                        self._state.last_catchup_success_at = _utc_now_iso()
                    self._state.failed_scripts = []
                    self._state.last_failure = None
                else:
                    observed_failure = (
                        f"Analysis job {job_id} ended as {view.get('status')}"
                        + (
                            f" ({view['error_code']})"
                            if view.get("error_code")
                            else ""
                        )
                    )
                    self._state.last_failure = observed_failure
        with self._lock:
            self._state.pending_analysis_jobs = sorted(set(still_pending))
            # A day is only "awaiting scheduling" while it has a live durable job.
            # A finished job releases it, so catch-up can move on and any data that
            # arrived for that day while the job ran is queued on the next cycle.
            self._state.pending_analysis_slices = {
                day: signature
                for day, signature in self._state.pending_analysis_slices.items()
                if day not in finished_dates
            }
            # A succeeded job is the durable record that the slice was analysed,
            # wherever the selected provider ran it.
            self._state.completed_analysis_slices = {
                **self._state.completed_analysis_slices,
                **succeeded_slices,
            }
            # A success for a day supersedes any failure recorded for it, so the
            # day is never left blocked by an outcome it has already moved past.
            self._state.failed_analysis_jobs = {
                day: failed_job_id
                for day, failed_job_id in {
                    **self._state.failed_analysis_jobs,
                    **failed_jobs,
                }.items()
                if day not in succeeded_slices
            }
            self._persist_state()
        return observed_failure


_RUNTIME_MANAGER: RuntimeOrchestrator | None = None


def get_runtime_manager() -> RuntimeOrchestrator:
    global _RUNTIME_MANAGER
    if _RUNTIME_MANAGER is None:
        poll_seconds = int(
            str(os.getenv("FCP_UPDATE_POLL_SECONDS", DEFAULT_POLL_INTERVAL_SECONDS))
        )
        _RUNTIME_MANAGER = RuntimeOrchestrator(poll_interval_seconds=poll_seconds)
    return _RUNTIME_MANAGER


def start_runtime_background() -> None:
    status = StatusPrinter()
    manager = get_runtime_manager()
    scan_roots = _canonical_scan_roots()
    status.info(f"scanning roots: {', '.join(scan_roots)}")
    status.info(
        "runtime startup policy: "
        f"date={DATE_POLICY_BOOTSTRAP_LATEST_DAY}, "
        f"bootstrap_refresh={BOOTSTRAP_REFRESH_POLICY}, "
        f"catch_up={HISTORICAL_CATCH_UP_POLICY}, "
        f"auto_coverage={AUTO_COVERAGE_CONTRACT}:{','.join(AUTO_COVERAGE_SCRIPT_KEYS) or 'none'}, "
        f"execution={EXECUTION_POLICY_BEST_EFFORT}, "
        f"handoff=webapp_first_data_later, "
        f"updates={UPDATE_POLICY_INCREMENTAL}"
    )
    manager.start_background_updates()
    status.info(f"runtime manager started in background at {_utc_now_iso()}")


def run_orchestration() -> OrchestrationResult:
    """Legacy blocking path used by CLI prep commands."""
    status = StatusPrinter()
    manager = get_runtime_manager()
    manager.mark_app_started()
    result = manager.bootstrap()
    manager.start_background_updates()
    status.info(
        f"orchestration bootstrap completed at {_utc_now_iso()} (failed scripts: {len(result.failed_scripts)})"
    )
    return result
