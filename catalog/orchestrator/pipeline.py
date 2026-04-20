from __future__ import annotations

import hashlib
import json
import os
import threading
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
import traceback

from catalog.common.artifact_registry import configured_scan_dirs, scan_artifacts
from catalog.common.basic_metrics import basic_metrics_path, build_basic_metrics_dataset
from catalog.common.data_loading import iter_jsonl_files
from catalog.runner.data_filtering import discover_available_dates, ensure_session_filtered_data
from catalog.runner.playback import playback_readiness, prepare_session_playback_exports
from catalog.runner.script_catalog import discover_runnable_scripts, repo_root
from catalog.runner.script_exec import execute_script_for_session
from catalog.runner.session_store import (
    WORKFLOW_SCRIPT_ORDER,
    WORKFLOW_STEPS,
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
UPDATE_POLICY_INCREMENTAL = "poll_for_new_data_then_process_new_slice"
HISTORICAL_CATCH_UP_POLICY = "reverse_chronological_one_day_per_cycle"
BOOTSTRAP_REFRESH_POLICY = "always_refresh_latest_day_on_startup"
AUTO_COVERAGE_CONTRACT = "startup_safe_automatic_outputs"
AUTO_COVERAGE_SCRIPT_KEYS: tuple[str, ...] = tuple(WORKFLOW_STEPS[0][1]) if WORKFLOW_STEPS else tuple(WORKFLOW_SCRIPT_ORDER)
DEFAULT_POLL_INTERVAL_SECONDS = 60


class StatusPrinter:
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
    processed_dates_truth_model: str
    automatic_coverage_contract: str


def _canonical_scan_roots() -> list[str]:
    roots = configured_scan_dirs()
    preferred = ["data", "results"]
    for root in preferred:
        if root not in roots:
            roots.append(root)
    return roots


def _auto_session_id(start_date: str, end_date: str) -> str:
    return f"auto_{start_date.replace('-', '')}_{end_date.replace('-', '')}"


def _load_or_create_auto_session(*, workflows_root: Path, start_date, end_date, script_options):
    session_id = _auto_session_id(start_date.isoformat(), end_date.isoformat())
    session_dir = workflows_root / session_id
    if session_dir.exists():
        metadata_path = session_dir / "session_state.json"
        if metadata_path.exists():
            import json

            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            metadata, changed = normalize_session_metadata(session_dir, metadata, script_options)
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
        script_options=script_options,
    )
    write_session_metadata(session_dir, metadata)
    return session_id, session_dir, metadata, "created"


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


def _run_for_date_slice(
    *,
    status: StatusPrinter,
    workflows_root: Path,
    data_dir: Path,
    script_options,
    target_day: date,
) -> OrchestrationResult:
    session_id, session_dir, metadata, session_mode = _load_or_create_auto_session(
        workflows_root=workflows_root,
        start_date=target_day,
        end_date=target_day,
        script_options=script_options,
    )
    status.info(f"{session_mode} bootstrap/update session: {session_id} ({target_day.isoformat()})")

    matched_records, matched_files, filter_status = ensure_session_filtered_data(
        source_data_dir=data_dir,
        session_dir=session_dir,
        metadata=metadata,
    )
    if filter_status == "cached":
        status.info(f"skipping filter step (up-to-date): {matched_records} records across {matched_files} files")
    else:
        status.info(f"prepared filtered session data: {matched_records} records across {matched_files} files")

    filtered_data_dir = session_dir / str(metadata["paths"]["filtered_data_dir"])
    derived_dataset = basic_metrics_path(filtered_data_dir)
    if filter_status == "cached" and derived_dataset.exists():
        status.info(f"reusing derived metrics dataset: {derived_dataset}")
    else:
        derived_path, derived_rows = build_basic_metrics_dataset(filtered_data_dir)
        status.info(f"prepared derived metrics dataset: {derived_rows} rows at {derived_path}")

    script_index = {item.key: item for item in script_options}
    script_results: list[dict[str, Any]] = []
    failed_scripts: list[str] = []
    for script_key in AUTO_COVERAGE_SCRIPT_KEYS:
        script = script_index.get(script_key)
        if script is None:
            status.warn(f"automatic coverage script not discovered: {script_key}")
            continue
        status.info(f"running analysis step: {script_key}")
        try:
            state, exit_code = execute_script_for_session(
                session_dir=session_dir,
                metadata=metadata,
                script=script,
                force_rerun=False,
            )
        except Exception as exc:  # pragma: no cover - defensive logging path
            failed_scripts.append(script_key)
            script_results.append({"script": script_key, "state": "crashed", "exit_code": None})
            status.warn(
                f"{script_key} crashed before completion: {exc.__class__.__name__}: {exc}. "
                f"continuing due to execution policy {EXECUTION_POLICY_BEST_EFFORT}"
            )
            status.warn("stack trace follows:\n" + "".join(traceback.format_exception(exc)))
            continue
        script_results.append({"script": script_key, "state": state, "exit_code": exit_code})
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

    ready, missing = playback_readiness(session_dir, metadata)
    if ready:
        export_path, export_state = prepare_session_playback_exports(session_dir, metadata)
        if export_state == "cached":
            status.info(f"playback export already fresh: {export_path}")
        else:
            status.info(f"generated playback export: {export_path}")
    else:
        for item in missing:
            status.warn(f"playback prerequisite missing: {item}")

    artifacts, warnings = scan_artifacts(_canonical_scan_roots())
    return OrchestrationResult(session_id, session_dir, artifacts, warnings, script_results, failed_scripts)


class RuntimeOrchestrator:
    """Bootstrap latest day quickly, then poll for incremental date updates."""

    def __init__(self, *, poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS) -> None:
        self.status = StatusPrinter()
        self.root = repo_root()
        self.data_dir = self.root / "data"
        self.workflows_root = self.root / "results" / "workflows"
        self.workflows_root.mkdir(parents=True, exist_ok=True)
        self.state_path = self.workflows_root / "runtime_state.json"
        self.poll_interval_seconds = max(int(poll_interval_seconds), 10)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state = self._load_state()

    def _default_state(self) -> RuntimeState:
        return RuntimeState(
            mode="bootstrap_only",
            phase="bootstrap",
            bootstrap_policy=DATE_POLICY_BOOTSTRAP_LATEST_DAY,
            catch_up_policy=HISTORICAL_CATCH_UP_POLICY,
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
            processed_dates_truth_model="verified_session_outputs",
            automatic_coverage_contract=AUTO_COVERAGE_CONTRACT,
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
        fields = {field: payload.get(field, getattr(default, field)) for field in default.__dataclass_fields__}
        state = RuntimeState(**fields)
        if not isinstance(state.processed_dates, list):
            state.processed_dates = []
        state.processed_dates = sorted({str(item) for item in state.processed_dates})
        if state.bootstrap_date is None:
            state.bootstrap_date = state.last_bootstrap_date
        return state

    def _persist_state(self) -> None:
        self.state_path.write_text(json.dumps(self._state.__dict__, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def state_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._state.__dict__)

    def bootstrap(self) -> OrchestrationResult:
        with self._lock:
            self._state.mode = "bootstrap_running"
            self._state.phase = "bootstrap"
            self._state.update_running = True
            self._persist_state()
        result = self._run_update(bootstrap=True)
        return result

    def start_background_updates(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._poll_loop, name="msh-runtime-poller", daemon=True)
        self._thread.start()

    def request_refresh(self) -> None:
        with self._lock:
            if self._state.update_running:
                return
        threading.Thread(target=self._run_update, kwargs={"bootstrap": False}, daemon=True).start()

    def _poll_loop(self) -> None:
        self.status.info(
            "incremental update loop enabled "
            f"({UPDATE_POLICY_INCREMENTAL}; catch-up={HISTORICAL_CATCH_UP_POLICY}; "
            f"bootstrap={BOOTSTRAP_REFRESH_POLICY}; "
            f"auto_coverage={AUTO_COVERAGE_CONTRACT}:{','.join(AUTO_COVERAGE_SCRIPT_KEYS) or 'none'}; "
            f"interval={self.poll_interval_seconds}s)"
        )
        while not self._stop.is_set():
            try:
                self._run_update(bootstrap=False)
            except Exception as exc:  # pragma: no cover
                with self._lock:
                    self._state.last_failure = f"{exc.__class__.__name__}: {exc}"
                    self._state.update_running = False
                    self._persist_state()
                self.status.warn(f"background update loop failure: {exc}")
            self._stop.wait(self.poll_interval_seconds)

    def _verified_processed_dates(self, *, script_options) -> set[str]:
        # Verification matches the bounded automatic catch-up contract only.
        verified: set[str] = set()
        sessions = list_sessions(self.workflows_root)
        for session in sessions:
            metadata, changed = normalize_session_metadata(session.session_dir, dict(session.metadata), script_options)
            if changed:
                write_session_metadata(session.session_dir, metadata)
            filter_payload = metadata.get("filter", {})
            start_date = filter_payload.get("start_date")
            end_date = filter_payload.get("end_date")
            if not isinstance(start_date, str) or not isinstance(end_date, str) or start_date != end_date:
                continue
            filtered_dir = session.session_dir / str(metadata.get("paths", {}).get("filtered_data_dir", "data"))
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
        state_processed = {item for item in self._state.processed_dates}
        available_iso = [item.isoformat() for item in available_dates]
        available_set = set(available_iso)
        # Truth model: processed coverage is derived strictly from verified outputs.
        # Persisted state_processed is diagnostic/history and may be stale.
        processed_set = {item for item in verified_processed_dates if item in available_set}
        dropped_unverified = {item for item in state_processed if item in available_set and item not in verified_processed_dates}
        processed_desc = sorted(processed_set, reverse=True)
        pending_desc = [item for item in reversed(available_dates) if item.isoformat() not in processed_set]
        self._state.processed_dates = sorted(processed_set)
        self._state.processed_days_count = len(processed_set)
        self._state.total_available_days = len(available_dates)
        self._state.pending_dates_count = len(pending_desc)
        self._state.next_planned_date = pending_desc[0].isoformat() if pending_desc else None
        self._state.catch_up_complete = len(pending_desc) == 0
        self._state.catch_up_status = "complete" if self._state.catch_up_complete else "running"
        if processed_desc:
            self._state.current_range_start = processed_desc[-1]
            self._state.current_range_end = processed_desc[0]
        else:
            self._state.current_range_start = None
            self._state.current_range_end = None
        return available_dates, pending_desc, processed_set, dropped_unverified

    def _run_update(self, *, bootstrap: bool) -> OrchestrationResult:
        with self._lock:
            if self._state.update_running and not bootstrap:
                return OrchestrationResult("none", self.workflows_root, [], [], [], [])
            self._state.update_running = True
            self._state.phase = "bootstrap" if bootstrap else "historical_catch_up"
            self._state.mode = "bootstrap_running" if bootstrap else "incremental_refresh_running"
            self._state.last_update_check_at = _utc_now_iso()
            self._persist_state()

        artifacts, warnings = scan_artifacts(_canonical_scan_roots())
        if not self.data_dir.exists():
            self.status.warn(f"data directory is missing at {self.data_dir}; Flask will run in scan-only mode")
            with self._lock:
                self._state.update_running = False
                self._state.phase = "idle"
                self._state.mode = "scan_only"
                self._persist_state()
            return OrchestrationResult("none", self.workflows_root, artifacts, warnings, [], [])

        available_dates = discover_available_dates(self.data_dir)
        if not available_dates:
            self.status.warn("no dates discovered in data/; skipping analysis pipeline")
            with self._lock:
                self._state.update_running = False
                self._state.phase = "idle"
                self._state.mode = "idle_no_data"
                self._state.catch_up_status = "idle"
                self._persist_state()
            return OrchestrationResult("none", self.workflows_root, artifacts, warnings, [], [])

        script_options = discover_runnable_scripts(self.root / "catalog")
        if not script_options:
            self.status.warn("no runnable scripts discovered; skipping analysis pipeline")
            with self._lock:
                self._state.update_running = False
                self._state.phase = "idle"
                self._state.mode = "idle_no_scripts"
                self._persist_state()
            return OrchestrationResult("none", self.workflows_root, artifacts, warnings, [], [])

        verified_processed_dates = self._verified_processed_dates(script_options=script_options)
        latest = available_dates[-1]
        earliest = available_dates[0]
        source_sig = _source_signature(self.data_dir)
        with self._lock:
            self._state.last_discovered_date = latest.isoformat()
            self._state.earliest_available_source_date = earliest.isoformat()
            self._state.latest_available_source_date = latest.isoformat()
            self._state.last_source_signature = source_sig
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

        if bootstrap:
            target_days = [latest]
            self.status.info(
                "bootstrap phase: refreshing latest available day "
                f"{latest.isoformat()} (policy={BOOTSTRAP_REFRESH_POLICY})"
            )
        else:
            target_days = [pending_desc[0]] if pending_desc else []
            if target_days:
                self.status.info(
                    "historical catch-up phase: processing one pending day "
                    f"{target_days[0].isoformat()} ({len(pending_desc)} pending before this cycle)"
                )
            else:
                self.status.info("historical catch-up phase: no pending days remain; cycle will idle")

        with self._lock:
            self._state.new_data_detected = bool(target_days)
            self._persist_state()

        if target_days:
            self.status.info(
                "date policy applied "
                f"({DATE_POLICY_BOOTSTRAP_LATEST_DAY}): processing {', '.join(day.isoformat() for day in target_days)}"
            )
            final_result = OrchestrationResult("none", self.workflows_root, artifacts, warnings, [], [])
            failed: list[str] = []
            for day in target_days:
                final_result = _run_for_date_slice(
                    status=self.status,
                    workflows_root=self.workflows_root,
                    data_dir=self.data_dir,
                    script_options=script_options,
                    target_day=day,
                )
                failed.extend(final_result.failed_scripts)
            with self._lock:
                processed = set(self._state.processed_dates)
                for day in target_days:
                    processed.add(day.isoformat())
                self._state.processed_dates = sorted(processed)
                self._state.session_id = final_result.session_id
                self._state.last_processed_date = target_days[-1].isoformat()
                if bootstrap:
                    self._state.bootstrap_date = target_days[-1].isoformat()
                    self._state.last_bootstrap_date = target_days[-1].isoformat()
                else:
                    self._state.last_catchup_success_at = _utc_now_iso()
                self._state.last_successful_refresh = _utc_now_iso()
                self._state.failed_scripts = failed
                self._state.last_failure = None if not failed else f"Failed scripts: {', '.join(sorted(set(failed)))}"
                verified_processed_dates = self._verified_processed_dates(script_options=script_options)
                _, pending_desc, _, dropped_unverified = self._apply_progress_state(
                    available_dates=available_dates,
                    verified_processed_dates=verified_processed_dates,
                )
                if dropped_unverified:
                    self.status.warn(
                        "post-run verification re-queued unverified day(s): "
                        + ", ".join(sorted(dropped_unverified, reverse=True))
                    )
                self.status.info(
                    "incremental progress: "
                    f"processed={self._state.processed_days_count}/{self._state.total_available_days}, "
                    f"remaining={len(pending_desc)}, next={self._state.next_planned_date or 'none'}"
                )
        else:
            final_result = OrchestrationResult("none", self.workflows_root, artifacts, warnings, [], [])
            with self._lock:
                self._state.failed_scripts = []
                self._state.last_failure = None
                _, pending_desc, _, _ = self._apply_progress_state(
                    available_dates=available_dates,
                    verified_processed_dates=verified_processed_dates,
                )
                self.status.info(
                    "incremental progress unchanged: "
                    f"processed={self._state.processed_days_count}/{self._state.total_available_days}, "
                    f"remaining={len(pending_desc)}"
                )

        with self._lock:
            self._state.update_running = False
            self._state.phase = "idle"
            self._state.mode = "idle_incremental"
            self._persist_state()
        return final_result


_RUNTIME_MANAGER: RuntimeOrchestrator | None = None


def get_runtime_manager() -> RuntimeOrchestrator:
    global _RUNTIME_MANAGER
    if _RUNTIME_MANAGER is None:
        poll_seconds = int(str(os.getenv("MSH_UPDATE_POLL_SECONDS", DEFAULT_POLL_INTERVAL_SECONDS)))
        _RUNTIME_MANAGER = RuntimeOrchestrator(poll_interval_seconds=poll_seconds)
    return _RUNTIME_MANAGER


def run_orchestration() -> OrchestrationResult:
    status = StatusPrinter()
    manager = get_runtime_manager()
    scan_roots = _canonical_scan_roots()
    status.info(f"scanning roots: {', '.join(scan_roots)}")
    status.info(
        "orchestration policy: "
        f"date={DATE_POLICY_BOOTSTRAP_LATEST_DAY}, "
        f"bootstrap_refresh={BOOTSTRAP_REFRESH_POLICY}, "
        f"catch_up={HISTORICAL_CATCH_UP_POLICY}, "
        f"auto_coverage={AUTO_COVERAGE_CONTRACT}:{','.join(AUTO_COVERAGE_SCRIPT_KEYS) or 'none'}, "
        f"execution={EXECUTION_POLICY_BEST_EFFORT}, "
        f"handoff={FLASK_HANDOFF_POLICY_ALWAYS}, "
        f"updates={UPDATE_POLICY_INCREMENTAL}"
    )
    result = manager.bootstrap()
    manager.start_background_updates()
    status.info(f"orchestration bootstrap completed at {_utc_now_iso()} (failed scripts: {len(result.failed_scripts)})")
    return result
