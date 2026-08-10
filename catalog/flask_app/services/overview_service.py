from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from catalog.orchestrator.pipeline import get_runtime_manager
from catalog.runner.playback import PLAYBACK_EXPORT_FILE, playback_readiness, session_playback_export_dir
from catalog.runner.session_store import list_sessions

from .catalog_service import ArtifactCatalog, ScanSnapshot


@dataclass
class OverviewSnapshot:
    headline: dict[str, Any]
    decision: dict[str, Any]
    activity: dict[str, Any]
    warnings: list[str]


def build_overview_snapshot(
    catalog: ArtifactCatalog,
    *,
    scan: ScanSnapshot | None = None,
    runtime_state: dict[str, Any] | None = None,
    sessions: list[Any] | None = None,
) -> OverviewSnapshot:
    scan = scan if scan is not None else catalog.cached_snapshot()
    runtime_state = runtime_state if runtime_state is not None else get_runtime_manager().state_snapshot()
    visible = [item for item in scan.artifacts if item.get("visibility") == "default"]
    sessions = sessions if sessions is not None else list_sessions(Path("results") / "workflows")
    session_context = _resolve_session_context(runtime_state, sessions)

    headline = _headline_summary(scan, runtime_state, visible)
    runtime = _runtime_summary(runtime_state)
    machine_activity = _machine_activity(scan, session_context)
    readiness = _view_readiness(runtime_state, visible, session_context)
    decision = _overview_decision(headline, runtime, readiness, machine_activity)

    return OverviewSnapshot(
        headline=headline,
        decision=decision,
        activity=machine_activity,
        warnings=list(scan.warnings),
    )


def _resolve_session_context(runtime_state: dict[str, Any], sessions: list[Any]) -> dict[str, Any]:
    runtime_session_id = str(runtime_state.get("session_id") or "").strip()
    if runtime_session_id:
        for item in sessions:
            if item.session_id == runtime_session_id:
                return {"session": item, "source": "runtime", "session_id": runtime_session_id}
    if sessions:
        ordered = sorted(sessions, key=_session_freshness_key, reverse=True)
        selected = ordered[0]
        return {"session": selected, "source": "fallback_latest", "session_id": selected.session_id}
    return {"session": None, "source": "none", "session_id": ""}


def _session_freshness_key(session: Any) -> tuple[pd.Timestamp, str]:
    metadata = getattr(session, "metadata", {}) or {}
    updated = pd.to_datetime(metadata.get("updated_at"), errors="coerce", utc=True)
    if pd.isna(updated):
        updated = pd.to_datetime(metadata.get("created_at"), errors="coerce", utc=True)
    if pd.isna(updated):
        filter_end = ((metadata.get("filter") or {}).get("end_date") if isinstance(metadata.get("filter"), dict) else None)
        updated = pd.to_datetime(filter_end, errors="coerce", utc=True)
    if pd.isna(updated):
        updated = pd.Timestamp(0, tz="UTC")
    return updated, str(getattr(session, "session_id", ""))


def _headline_summary(scan: ScanSnapshot, runtime_state: dict[str, Any], visible: list[dict[str, Any]]) -> dict[str, Any]:
    phase = runtime_state.get("current_processing_phase") or "runtime_not_started"
    phase_label = {
        "runtime_not_started": "Runtime not started",
        "discovery_pending": "Discovering source data",
        "bootstrap_minimal_processing": "Running bootstrap processing",
        "historical_catch_up": "Running historical catch-up",
        "polling_new_data": "Polling for new data",
        "failed": "Runtime failure",
    }.get(phase, phase.replace("_", " ").title())

    processed = runtime_state.get("fully_processed_days_count") or runtime_state.get("processed_days_count") or 0
    total = runtime_state.get("total_available_days") or 0
    catch_up_complete = bool(runtime_state.get("historical_catch_up_complete") or runtime_state.get("catch_up_complete"))
    source_artifacts = len([item for item in visible if item.get("category") == "source_data"])
    derived_artifacts = len([item for item in visible if item.get("category") == "derived_output"])
    playback_compatible_count = len([item for item in visible if item.get("playback_compatible")])
    read_error_count = len([item for item in visible if item.get("status") != "ready"])
    hidden_workflow_copy_count = len([item for item in scan.artifacts if item.get("category") == "workflow_data_copy"])
    hidden_internal_metadata_count = len([item for item in scan.artifacts if item.get("category") == "internal_metadata"])

    return {
        "phase": phase,
        "phase_label": phase_label,
        "catch_up_complete": catch_up_complete,
        "processed_days": int(processed),
        "total_days": int(total),
        "next_queued_date": runtime_state.get("next_queued_date") or runtime_state.get("next_planned_date") or "none",
        "last_failure": runtime_state.get("last_failure") or "none",
        "visible_artifact_count": len(visible),
        "source_artifacts": source_artifacts,
        "derived_artifacts": derived_artifacts,
        "playback_compatible_count": playback_compatible_count,
        "read_error_count": read_error_count,
        "hidden_workflow_copy_count": hidden_workflow_copy_count,
        "hidden_internal_metadata_count": hidden_internal_metadata_count,
        "scanned_at_epoch": scan.scanned_at_epoch,
    }


def _runtime_summary(runtime_state: dict[str, Any]) -> dict[str, Any]:
    processed = runtime_state.get("fully_processed_days_count") or runtime_state.get("processed_days_count") or 0
    total = runtime_state.get("total_available_days") or 0
    return {
        "current_phase": runtime_state.get("current_processing_phase") or "runtime_not_started",
        "currently_processing_date": runtime_state.get("currently_processing_date") or "n/a",
        "processed_days": int(processed),
        "total_days": int(total),
        "next_queued_date": runtime_state.get("next_queued_date") or runtime_state.get("next_planned_date") or "none",
        "last_completed_step": runtime_state.get("last_completed_step") or "n/a",
        "last_completed_date": runtime_state.get("last_completed_date") or "n/a",
        "last_failure": runtime_state.get("last_failure") or "none",
        "catch_up_complete": bool(runtime_state.get("historical_catch_up_complete") or runtime_state.get("catch_up_complete")),
        "discovery_complete": bool(runtime_state.get("discovery_complete")),
        "runtime_started": bool(runtime_state.get("runtime_started_at")),
    }


def _overview_decision(
    headline: dict[str, Any],
    runtime: dict[str, Any],
    readiness: list[dict[str, str]],
    activity: dict[str, Any],
) -> dict[str, Any]:
    readiness_by_view = {item["view"]: item for item in readiness}
    playback = readiness_by_view.get("/playback", {})
    machine = readiness_by_view.get("/machine", {})
    analysis = readiness_by_view.get("/analyses", {})
    visible_count = int(headline.get("visible_artifact_count") or 0)
    source_count = int(headline.get("source_artifacts") or 0)
    derived_count = int(headline.get("derived_artifacts") or 0)
    playback_count = int(headline.get("playback_compatible_count") or 0)
    read_errors = int(headline.get("read_error_count") or 0)
    runtime_started = bool(runtime.get("runtime_started"))
    last_failure = str(runtime.get("last_failure") or headline.get("last_failure") or "none")

    facts = [
        f"{visible_count} visible artifact(s): {source_count} source, {derived_count} derived.",
        f"Playback-compatible datasets: {playback_count}.",
        f"Runtime phase: {headline.get('phase_label', 'unknown')}.",
    ]
    if read_errors:
        facts.append(f"{read_errors} artifact(s) need attention during scan.")
    if activity.get("latest_known_timestamp") and activity.get("latest_known_timestamp") != "n/a":
        facts.append(f"Latest known timestamp: {activity['latest_known_timestamp']}.")

    def _decision(
        *,
        state: str,
        title: str,
        summary: str,
        next_step: str,
        primary_label: str,
        primary_href: str,
        secondary_label: str = "Open status",
        secondary_href: str = "/status",
    ) -> dict[str, Any]:
        return {
            "state": state,
            "title": title,
            "summary": summary,
            "next_step": next_step,
            "primary_action": {"label": primary_label, "href": primary_href},
            "secondary_action": {"label": secondary_label, "href": secondary_href},
            "facts": facts,
        }

    if last_failure and last_failure != "none":
        return _decision(
            state="error",
            title="Runtime needs attention",
            summary=f"The latest runtime state reports a failure: {last_failure}",
            next_step="Open Status to inspect the failure, then use Control when you are ready to restart or continue processing.",
            primary_label="Open status",
            primary_href="/status",
            secondary_label="Open control",
            secondary_href="/control",
        )

    if playback.get("state") == "ready":
        return _decision(
            state="ready",
            title="Playback is ready",
            summary=str(playback.get("message") or "A playback export is available for the selected session."),
            next_step="Open Playback to inspect the current session. Use Live if you want the latest recorded telemetry instead.",
            primary_label="Open playback",
            primary_href="/playback",
            secondary_label="Open live",
            secondary_href="/live",
        )

    if not visible_count:
        return _decision(
            state="waiting",
            title="No playback-ready data is indexed yet",
            summary=str(playback.get("message") or "The artifact scan has not indexed source, derived, or playback-ready data yet."),
            next_step="Wait for the rescan to finish, then refresh Overview. If this stays empty, run the workflow from Control before opening Playback.",
            primary_label="Open status",
            primary_href="/status",
            secondary_label="Open control",
            secondary_href="/control",
        )

    if not runtime_started and playback.get("state") != "ready":
        return _decision(
            state="waiting",
            title="Artifacts exist, but runtime is not started",
            summary="FCP can see files, but no active runtime session is driving machine or playback readiness yet.",
            next_step="Open Control to start or continue a workflow session. Use Status if you want to inspect the startup state first.",
            primary_label="Open control",
            primary_href="/control",
        )

    if playback.get("state") in {"waiting", "partial"}:
        return _decision(
            state="partial" if playback_count else "waiting",
            title="Playback is not ready yet",
            summary=str(playback.get("message") or "Playback is still waiting for session output."),
            next_step="Wait for rescan to finish, or use Control to generate playback output for the current session.",
            primary_label="Open status",
            primary_href="/status",
            secondary_label="Open control",
            secondary_href="/control",
        )

    if machine.get("state") == "ready" or analysis.get("state") == "ready":
        return _decision(
            state="partial",
            title="Analysis outputs are available",
            summary="Some views are ready, but the overview did not find a playback-ready session yet.",
            next_step="Open the ready analysis/live views, or use Control to generate playback output for the current session.",
            primary_label="Open live",
            primary_href="/live",
            secondary_label="Open control",
            secondary_href="/control",
        )

    return _decision(
        state="partial",
        title="System is partially ready",
        summary="FCP has enough information to open some views, but not all runtime outputs are available yet.",
        next_step="Use Status to inspect readiness, then continue from Control if workflow output is missing.",
        primary_label="Open status",
        primary_href="/status",
        secondary_label="Open control",
        secondary_href="/control",
    )


def _machine_activity(scan: ScanSnapshot, session_context: dict[str, Any]) -> dict[str, Any]:
    source_or_derived = [
        item
        for item in scan.artifacts
        if item.get("visibility") == "default" and item.get("status") == "ready" and item.get("category") in {"source_data", "derived_output"}
    ]
    session_id = str(session_context.get("session_id") or "")
    context_source = str(session_context.get("source") or "none")
    context_note = _context_note(context_source, session_id)
    scoped = _session_scoped_artifacts(source_or_derived, session_id)
    latest_timestamp = _latest_known_timestamp(scoped) or _latest_known_timestamp(source_or_derived)
    machine_count_hint = sum((item.get("machine_count") or 0) for item in (scoped or source_or_derived))
    if scoped and context_source == "runtime":
        summary = (
            f"Latest known activity is derived from runtime session {session_id} metadata only. "
            f"Estimated machines represented: {machine_count_hint or 'unknown'}."
        )
    elif scoped and session_id:
        summary = (
            f"Latest known activity is derived from fallback latest session {session_id} metadata only. {context_note} "
            f"Estimated machines represented: {machine_count_hint or 'unknown'}."
        )
    elif source_or_derived:
        summary = "Latest known activity is derived from historical artifact metadata only; no per-machine rows are loaded on overview."
    else:
        summary = "No source or derived data has been discovered yet."

    return {
        "latest_known_timestamp": latest_timestamp or "n/a",
        "machine_rows": [],
        "summary": summary,
    }


def _latest_known_timestamp(artifacts: list[dict[str, Any]]) -> str | None:
    candidates = [item.get("timestamp_max") for item in artifacts if item.get("timestamp_max")]
    if not candidates:
        return None
    parsed = pd.to_datetime(candidates, errors="coerce")
    parsed = parsed[parsed.notna()]
    if parsed.empty:
        return None
    return parsed.max().isoformat()


def _session_scoped_artifacts(artifacts: list[dict[str, Any]], session_id: str) -> list[dict[str, Any]]:
    if not session_id:
        return []
    marker = f"/workflows/{session_id}/"
    return [item for item in artifacts if marker in str(item.get("path") or "").replace("\\", "/")]


def _playback_state_for_overview(
    session_playback_state: str,
    session_playback_message: str,
    *,
    context_source: str,
    session_id: str,
    indexed_playback_count: int,
) -> tuple[str, str]:
    if session_playback_state == "ready" and indexed_playback_count > 0 and context_source == "runtime":
        return "ready", session_playback_message
    if session_playback_state == "ready" and indexed_playback_count > 0:
        return (
            "partial",
            f"A playback export exists for fallback latest session {session_id or 'n/a'}, but the current runtime session is not confirmed. {_context_note(context_source, session_id)}",
        )
    if session_playback_state == "ready":
        return (
            "waiting",
            "A session playback export exists, but the artifact scan has not indexed any playback-compatible exports yet. Wait for rescan to finish, then refresh Overview or Playback.",
        )
    if indexed_playback_count > 0:
        return (
            "partial",
            f"Playback is not ready for {'runtime session' if context_source == 'runtime' else 'fallback latest session'} {session_id or 'n/a'}, but {indexed_playback_count} historical playback dataset(s) exist. {_context_note(context_source, session_id)}",
        )
    return "waiting", session_playback_message


def _view_readiness(runtime_state: dict[str, Any], visible: list[dict[str, Any]], session_context: dict[str, Any]) -> list[dict[str, str]]:
    view_contracts = runtime_state.get("view_contracts") or {}
    machine_contract = view_contracts.get("machine", {})
    session = session_context.get("session")
    session_id = str(session_context.get("session_id") or "")
    context_source = str(session_context.get("source") or "none")
    context_note = _context_note(context_source, session_id)

    historical_playback_count = len([item for item in visible if item.get("playback_compatible")])
    session_artifacts = _session_scoped_artifacts(visible, session_id)
    current_derived_count = len([item for item in session_artifacts if item.get("category") == "derived_output" and item.get("status") == "ready"])
    historical_derived_ready = any(item.get("category") == "derived_output" and item.get("status") == "ready" for item in visible)
    any_artifacts = bool(visible)
    session_playback_state, session_playback_message = _current_session_playback_status(session)
    playback_state, playback_message = _playback_state_for_overview(
        session_playback_state,
        session_playback_message,
        context_source=context_source,
        session_id=session_id,
        indexed_playback_count=historical_playback_count,
    )

    return [
        {
            "view": "/machine",
            "state": machine_contract.get("state", "waiting"),
            "message": machine_contract.get("message", "Waiting for machine/day summary output."),
        },
        {
            "view": "/playback",
            "state": playback_state,
            "message": playback_message,
        },
        {
            "view": "/analyses",
            "state": "ready" if current_derived_count > 0 else ("partial" if historical_derived_ready or any_artifacts else "waiting"),
            "message": (
                f"Runtime session {session_id} has {current_derived_count} ready derived output artifact(s)."
                if current_derived_count > 0 and session_id and context_source == "runtime"
                else f"Fallback latest session {session_id} has {current_derived_count} ready derived output artifact(s). {context_note}"
                if current_derived_count > 0 and session_id
                else "Historical derived outputs exist, but none are tied to the current runtime session yet."
                if historical_derived_ready
                else "Only source artifacts are available so far."
                if any_artifacts
                else "No scanned artifacts are available yet."
            ),
        },
        {
            "view": "/status",
            "state": "ready",
            "message": "Status page is startup-safe and always available.",
        },
        {
            "view": "/live",
            "state": "ready",
            "message": "Live page shows latest recorded telemetry with inferred per-machine state and recent candidate events.",
        },
        {
            "view": "/control",
            "state": "ready",
            "message": "Control page is startup-safe and always available.",
        },
    ]


def _current_session_playback_status(session: Any) -> tuple[str, str]:
    if session is None:
        return "waiting", "No runtime/latest workflow session is available yet for playback readiness."
    ready, missing = playback_readiness(session.session_dir, session.metadata)
    if not ready:
        return "waiting", "Playback prerequisites are missing for current session: " + ", ".join(missing)
    export_dir = session_playback_export_dir(session.session_dir, session.metadata)
    export_path = export_dir / PLAYBACK_EXPORT_FILE
    if not export_path.exists():
        return "waiting", "Current session prerequisites are ready, but playback export has not been generated yet."
    return "ready", f"Playback export is ready for current session {session.session_id}."


def _context_note(context_source: str, session_id: str) -> str:
    if context_source == "runtime":
        return ""
    if context_source == "fallback_latest" and session_id:
        return "Runtime session id was unavailable/not found; using deterministic latest-session fallback."
    return "No runtime session context is available yet."
