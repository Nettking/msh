from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from catalog.common.artifact_registry import configured_scan_dirs, scan_artifacts
from catalog.runner.data_filtering import discover_available_dates, ensure_session_filtered_data
from catalog.runner.playback import playback_readiness, prepare_session_playback_exports
from catalog.runner.script_catalog import discover_runnable_scripts, repo_root
from catalog.runner.script_exec import execute_script_for_session
from catalog.runner.session_store import (
    WORKFLOW_SCRIPT_ORDER,
    initialize_session_metadata,
    normalize_session_metadata,
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
# - date policy: full discovered source-date range
# - execution policy: best effort (continue after individual script failures)
# - handoff policy: start Flask even if some preparation steps failed
DATE_POLICY_FULL_RANGE = "full_discovered_range"
EXECUTION_POLICY_BEST_EFFORT = "best_effort_continue_on_failure"
FLASK_HANDOFF_POLICY_ALWAYS = "always_handoff"


class StatusPrinter:
    def info(self, message: str) -> None:
        print(f"[orchestrator] {message}", flush=True)

    def warn(self, message: str) -> None:
        print(f"[orchestrator][warn] {message}", flush=True)


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


def run_orchestration() -> OrchestrationResult:
    status = StatusPrinter()
    root = repo_root()
    data_dir = root / "data"
    workflows_root = root / "results" / "workflows"
    workflows_root.mkdir(parents=True, exist_ok=True)

    scan_roots = _canonical_scan_roots()
    status.info(f"scanning roots: {', '.join(scan_roots)}")
    status.info(
        "orchestration policy: "
        f"date={DATE_POLICY_FULL_RANGE}, "
        f"execution={EXECUTION_POLICY_BEST_EFFORT}, "
        f"handoff={FLASK_HANDOFF_POLICY_ALWAYS}"
    )
    artifacts, warnings = scan_artifacts(scan_roots)
    status.info(f"discovered {len(artifacts)} candidate tabular artifacts")
    if warnings:
        for warning in warnings:
            status.warn(warning)

    if not data_dir.exists():
        status.warn(f"data directory is missing at {data_dir}; Flask will start with scan-only mode")
        return OrchestrationResult("none", workflows_root, artifacts, warnings, [], [])

    status.info("discovering available source dates from data/")
    available_dates = discover_available_dates(data_dir)
    if not available_dates:
        status.warn("no dates discovered in data/; skipping analysis pipeline")
        return OrchestrationResult("none", workflows_root, artifacts, warnings, [], [])

    status.info(
        "date range policy applied "
        f"({DATE_POLICY_FULL_RANGE}): {available_dates[0].isoformat()} .. {available_dates[-1].isoformat()}"
    )
    script_options = discover_runnable_scripts(root / "catalog")
    if not script_options:
        status.warn("no runnable scripts discovered; skipping analysis pipeline")
        return OrchestrationResult("none", workflows_root, artifacts, warnings, [], [])

    session_id, session_dir, metadata, session_mode = _load_or_create_auto_session(
        workflows_root=workflows_root,
        start_date=available_dates[0],
        end_date=available_dates[-1],
        script_options=script_options,
    )
    status.info(f"{session_mode} auto session: {session_id}")

    matched_records, matched_files, filter_status = ensure_session_filtered_data(
        source_data_dir=data_dir,
        session_dir=session_dir,
        metadata=metadata,
    )
    if filter_status == "cached":
        status.info(f"skipping filter step (up-to-date): {matched_records} records across {matched_files} files")
    else:
        status.info(f"prepared filtered session data: {matched_records} records across {matched_files} files")

    script_index = {item.key: item for item in script_options}
    script_results: list[dict[str, Any]] = []
    failed_scripts: list[str] = []

    for script_key in WORKFLOW_SCRIPT_ORDER:
        script = script_index.get(script_key)
        if script is None:
            status.warn(f"workflow script not discovered: {script_key}")
            continue
        status.info(f"running analysis step: {script_key}")
        state, exit_code = execute_script_for_session(
            session_dir=session_dir,
            metadata=metadata,
            script=script,
            force_rerun=False,
        )
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

    status.info(
        f"orchestration completed at {datetime.utcnow().replace(microsecond=0).isoformat()}Z "
        f"(failed scripts: {len(failed_scripts)})"
    )
    if failed_scripts:
        status.warn(
            "handoff remains enabled despite failures "
            f"(policy={FLASK_HANDOFF_POLICY_ALWAYS}); failed scripts: {', '.join(failed_scripts)}"
        )

    return OrchestrationResult(session_id, session_dir, artifacts, warnings, script_results, failed_scripts)
