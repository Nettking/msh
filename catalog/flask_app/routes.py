from __future__ import annotations

from pathlib import Path

import pandas as pd
from flask import Blueprint, current_app, flash, redirect, render_template, request, url_for

from catalog.common.telemetry_cache import TelemetryCache, cached_cache_status
from catalog.orchestrator.pipeline import get_runtime_manager
from catalog.runner.session_store import list_sessions

from .services.catalog_service import ArtifactCatalog, safe_load_artifact_frame
from .services.chart_service import category_columns, category_counts, histogram_data, line_or_scatter_data, machine_day_trend, numeric_columns
from .services.control_service import get_control_panel_service
from .services.live_service import get_live_telemetry_service
from .services.operator_page_cache import get_operator_page_cache
from .services.operator_scope_service import get_operator_scope_service
from .services.strategy_config_service import StrategyConfigService
from .services.playback_service import (
    TELEMETRY_CACHE_PLAYBACK_PATH,
    default_live_signal_columns,
    interval_rows,
    load_cached_playback_frame_for_machine_day,
    load_playback_frame,
    playback_field_groups,
    prepare_playback_frame,
    playback_subset,
    filter_playback_artifacts_for_runtime,
    resolve_playback_selection,
    summarize_intervals,
    validate_playback_frame,
    validate_playback_source,
    telemetry_cache_playback_artifact,
)
from .services.workflow_session_index import get_workflow_session_index

web = Blueprint("web", __name__)


@web.before_app_request
def startup_mode_gate():
    endpoint = request.endpoint or ""
    if endpoint.startswith("static"):
        return None
    allowed = {"web.startup", "web.choose_startup_mode", "web.status", "web.rescan", "web.guide"}
    if endpoint in allowed:
        return None
    if get_runtime_manager().requires_startup_choice():
        return redirect(url_for("web.startup", next=request.full_path if request.query_string else request.path))
    return None


def _telemetry_cache_status_model() -> dict[str, object]:
    status = cached_cache_status(Path("data"))
    state = "fresh" if status.fresh else ("stale" if status.exists else "missing")
    return {
        "state": state,
        "exists": status.exists,
        "fresh": status.fresh,
        "cache_path": str(status.cache_path),
        "source_file_count": status.source_file_count,
        "parquet_file_count": status.parquet_file_count,
        "cached_row_count": status.manifest_row_count,
        "manifest_source_file_count": status.manifest_source_file_count,
        "last_rebuild_time": status.manifest_generated_at,
    }


def _machine_day_detail_from_cache(scope) -> dict | None:
    status = cached_cache_status(Path("data"))
    if not status.exists or not status.fresh:
        current_app.logger.info(
            "machine day summary using session export fallback cache_exists=%s cache_fresh=%s",
            status.exists,
            status.fresh,
        )
        return None
    try:
        frame = TelemetryCache(status.cache_path).machine_day_row_counts(
            start_date=str(scope.start_date) if getattr(scope, "is_active", False) else None,
            end_date=str(scope.end_date) if getattr(scope, "is_active", False) else None,
            as_dataframe=True,
        )
    except Exception as exc:  # noqa: BLE001
        current_app.logger.warning("machine day summary DuckDB cache failed; using session export fallback error=%s", exc)
        return None
    current_app.logger.info("machine day summary using DuckDB/Parquet cache rows=%s", len(frame))
    return {
        "session_id": "telemetry_cache",
        "source_path": str(status.cache_path),
        "status": "ready" if not frame.empty else "empty_rows",
        "message": "No cached telemetry rows matched the selected scope." if frame.empty else "",
        "frame": frame,
    }

def _strategy_config_service() -> StrategyConfigService:
    return StrategyConfigService(
        strategies_path=current_app.config.get("INTERVENTION_STRATEGIES_PATH"),
        labels_path=current_app.config.get("INTERVENTION_LABELS_PATH"),
    )


def _catalog() -> ArtifactCatalog:
    return current_app.config["ARTIFACT_CATALOG"]


def _session_range(session) -> tuple[str | None, str | None]:
    metadata = getattr(session, "metadata", {}) or {}
    filter_payload = metadata.get("filter") if isinstance(metadata.get("filter"), dict) else {}
    return filter_payload.get("start_date"), filter_payload.get("end_date")


def _session_matches_scope(session, *, start_date: str, end_date: str) -> bool:
    session_start, session_end = _session_range(session)
    if not session_start or not session_end:
        return False
    return not (session_end < start_date or session_start > end_date)


def _machine_day_csv_for_session(session_id: str) -> Path:
    return Path("results") / "workflows" / session_id / "analyses" / "data_pr_day" / "machine_day_summary.csv"


def _machine_day_readiness_for_session(session_id: str) -> dict:
    csv_path = _machine_day_csv_for_session(session_id)
    base = {"session_id": session_id, "source_path": str(csv_path)}
    if not csv_path.exists():
        return {
            **base,
            "status": "missing",
            "message": "Machine/day aggregation has not been generated yet for this session.",
        }
    return {
        **base,
        "status": "artifact_present",
        "message": "",
    }


def _machine_day_detail_for_session(session_id: str) -> dict:
    csv_path = _machine_day_csv_for_session(session_id)
    base = {"session_id": session_id, "source_path": str(csv_path)}
    if not csv_path.exists():
        return {
            **base,
            "status": "missing",
            "message": "Machine/day aggregation has not been generated yet for this session.",
            "frame": None,
        }

    frame, load_error = safe_load_artifact_frame(str(csv_path))
    if frame is None:
        return {
            **base,
            "status": "invalid_csv",
            "message": f"Machine/day CSV exists but is invalid: {load_error}",
            "frame": None,
        }

    required = {"date", "machine", "value"}
    missing_columns = sorted(required - set(frame.columns))
    if missing_columns:
        return {
            **base,
            "status": "invalid_schema",
            "message": (
                "Machine/day CSV exists but is invalid: missing required columns "
                + ", ".join(missing_columns)
                + "."
            ),
            "frame": None,
        }

    prepared = frame.copy()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="coerce")
    prepared["machine"] = prepared["machine"].astype("string").fillna("unknown").astype(str)
    prepared["value"] = pd.to_numeric(prepared["value"], errors="coerce")
    prepared = prepared.dropna(subset=["date", "value"])
    if prepared.empty:
        return {
            **base,
            "status": "empty_rows",
            "message": "Machine/day CSV exists but contains no usable rows.",
            "frame": None,
        }

    return {
        **base,
        "status": "ready",
        "message": "",
        "frame": frame,
    }


def _telemetry_cache_exploration_artifact() -> dict[str, object] | None:
    status = cached_cache_status(Path("data"))
    if not status.exists or not status.fresh:
        return None
    return {
        "path": "telemetry-cache://samples",
        "file_name": "Telemetry analytics cache",
        "visibility": "default",
        "status": "ready",
        "category": "source_data",
        "row_count": status.manifest_row_count or 0,
        "modified_at": status.manifest_generated_at or "",
    }


def _load_telemetry_cache_exploration_frame(window_start: str, window_end: str) -> tuple[pd.DataFrame | None, str | None]:
    status = cached_cache_status(Path("data"))
    if not status.exists or not status.fresh:
        return None, "Telemetry analytics cache is missing or stale; choose an artifact or rebuild the cache."
    start = (window_start or "1900-01-01")[:10]
    end = (window_end or "2999-12-31")[:10]
    try:
        frame = TelemetryCache(status.cache_path).samples_by_date_range(start, end, as_dataframe=True)
    except Exception as exc:  # noqa: BLE001
        current_app.logger.warning("exploration DuckDB cache failed; use artifact fallback error=%s", exc)
        return None, f"Could not query telemetry analytics cache: {exc}"
    current_app.logger.info("exploration using DuckDB/Parquet cache rows=%s", len(frame))
    return frame, None

def _machine_day_chart_payload(frame: pd.DataFrame) -> tuple[dict, str]:
    required = {"date", "machine", "value"}
    if not required.issubset(frame.columns):
        return {"labels": [], "series": []}, "Machine/day data is missing required columns: date, machine, value."

    prepared = frame.copy()
    prepared["date"] = pd.to_datetime(prepared["date"], errors="coerce")
    prepared["machine"] = prepared["machine"].astype("string").fillna("unknown").astype(str)
    prepared["value"] = pd.to_numeric(prepared["value"], errors="coerce")
    prepared = prepared.dropna(subset=["date", "value"])
    if prepared.empty:
        return {"labels": [], "series": []}, "No machine/day data available."

    grouped = (
        prepared.groupby([prepared["date"].dt.strftime("%Y-%m-%d"), "machine"], dropna=False)["value"]
        .sum()
        .reset_index()
        .rename(columns={"date": "date"})
        .sort_values(["date", "machine"])
    )
    labels = grouped["date"].drop_duplicates().tolist()
    machines = sorted(grouped["machine"].dropna().astype(str).unique().tolist())
    series = []
    for machine in machines:
        machine_rows = grouped[grouped["machine"] == machine].set_index("date")["value"].to_dict()
        series.append({"label": machine, "data": [float(machine_rows.get(day, 0)) for day in labels]})
    return {"labels": labels, "series": []}, ""


@web.route("/")
def overview():
    route_started = pd.Timestamp.utcnow()
    overview_build_started = pd.Timestamp.utcnow()
    overview_snapshot, cache_state = get_operator_page_cache().get_overview_snapshot(_catalog())
    build_ms = max((pd.Timestamp.utcnow() - overview_build_started).total_seconds() * 1000.0, 0.0)
    total_ms = max((pd.Timestamp.utcnow() - route_started).total_seconds() * 1000.0, 0.0)
    current_app.logger.info(
        "overview GET cache=%s snapshot_ms=%.2f route_ms=%.2f",
        cache_state,
        build_ms,
        total_ms,
    )
    return render_template(
        "overview.html",
        overview=overview_snapshot,
        scan_dirs=_catalog().scan_dirs,
    )


@web.route("/guide")
def guide():
    return render_template("guide.html")


@web.route("/live")
def live():
    snapshot = get_live_telemetry_service().snapshot(_catalog())
    return render_template("live.html", live=snapshot)


@web.route("/status")
def status():
    snap = _catalog().cached_snapshot()
    runtime_state = get_runtime_manager().state_snapshot()
    operator_scope = get_operator_scope_service().get()
    internal_artifacts = [a for a in snap.artifacts if a.get("is_internal")]
    phase_messages = {
        "runtime_not_started": "Webapp started. Runtime has not started yet.",
        "discovery_pending": "Webapp started. Background discovery is running.",
        "bootstrap_latest_day_playback_ready_analysis": "Running playback-ready health and timeline analysis for latest day in the background.",
        "historical_catch_up": "Historical catch-up is running one day at a time.",
        "polling_new_data": "Historical processing is complete. Polling for newly arriving days.",
        "failed": "Background runtime encountered a failure. Check last failure details below.",
    }
    current_phase = runtime_state.get("current_processing_phase", "runtime_not_started")
    return render_template(
        "status.html",
        snapshot=snap,
        scan_dirs=_catalog().scan_dirs,
        runtime_state=runtime_state,
        internal_artifacts=internal_artifacts,
        phase_message=phase_messages.get(current_phase, "Runtime state is available below."),
        operator_scope=operator_scope,
        telemetry_cache_status=_telemetry_cache_status_model(),
    )


@web.route("/startup")
def startup():
    next_path = request.args.get("next", "/")
    startup_state = get_runtime_manager().startup_decision_snapshot()
    return render_template("startup.html", startup_state=startup_state, next_path=next_path)
