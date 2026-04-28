from __future__ import annotations

from pathlib import Path

import pandas as pd
from flask import Blueprint, current_app, flash, redirect, render_template, request, url_for

from catalog.orchestrator.pipeline import get_runtime_manager
from catalog.runner.session_store import list_sessions

from .services.catalog_service import ArtifactCatalog, safe_load_artifact_frame
from .services.chart_service import category_columns, category_counts, histogram_data, line_or_scatter_data, machine_day_trend, numeric_columns
from .services.control_service import get_control_panel_service
from .services.live_service import get_live_telemetry_service
from .services.operator_page_cache import get_operator_page_cache
from .services.operator_scope_service import get_operator_scope_service
from .services.playback_service import (
    default_live_signal_columns,
    interval_rows,
    load_playback_frame,
    playback_day_counts_by_machine,
    playback_field_groups,
    playback_context,
    playback_days_by_machine,
    prepare_playback_frame,
    playback_subset,
    summarize_intervals,
    validate_playback_frame,
    validate_playback_source,
)
from .services.workflow_session_index import get_workflow_session_index

web = Blueprint("web", __name__)


@web.before_app_request
def startup_mode_gate():
    endpoint = request.endpoint or ""
    if endpoint.startswith("static"):
        return None
    allowed = {"web.startup", "web.choose_startup_mode", "web.status", "web.rescan"}
    if endpoint in allowed:
        return None
    if get_runtime_manager().requires_startup_choice():
        return redirect(url_for("web.startup", next=request.full_path if request.query_string else request.path))
    return None


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
    return {"labels": labels, "series": series}, ""


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


@web.route("/live")
def live():
    snapshot = get_live_telemetry_service().snapshot(_catalog())
    return render_template("live.html", live=snapshot)


@web.route("/status")
def status():
    snap = _catalog().ensure_scanned()
    runtime_state = get_runtime_manager().state_snapshot()
    operator_scope = get_operator_scope_service().get()
    internal_artifacts = [a for a in snap.artifacts if a.get("is_internal")]
    phase_messages = {
        "runtime_not_started": "Webapp started. Runtime has not started yet.",
        "discovery_pending": "Webapp started. Background discovery is running.",
        "bootstrap_latest_day_full_analysis": "Running full initial analysis for latest day in the background.",
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
    )


@web.route("/startup")
def startup():
    next_path = request.args.get("next", "/")
    startup_state = get_runtime_manager().startup_decision_snapshot()
    return render_template("startup.html", startup_state=startup_state, next_path=next_path)


@web.post("/startup/choose")
def choose_startup_mode():
    mode = request.form.get("mode", "")
    next_path = request.form.get("next") or url_for("web.overview")
    ok, message = get_runtime_manager().choose_startup_mode(mode)
    flash(message, "success" if ok else "error")
    if ok:
        return redirect(next_path)
    return redirect(url_for("web.startup", next=next_path))


@web.route("/analyses")
def analyses():
    snap = _catalog().ensure_scanned()
    visible_artifacts = [a for a in snap.artifacts if a.get("visibility") == "default"]
    selected_path = request.args.get("path", "")
    selected = _catalog().artifact_by_path(selected_path) if selected_path else None
    if selected and selected.get("visibility") != "default":
        selected = None
    frame = None
    load_error = None
    trend = {"labels": [], "series": []}
    if selected:
        frame, load_error = safe_load_artifact_frame(selected_path)
        if frame is not None:
            trend = machine_day_trend(frame)
    return render_template(
        "analyses.html",
        artifacts=visible_artifacts,
        selected=selected,
        frame=frame,
        load_error=load_error,
        trend=trend,
    )


@web.route("/machine")
def machine_view():
    workflows_root = Path("results") / "workflows"
    sessions = list_sessions(workflows_root)
    runtime_state = get_runtime_manager().state_snapshot()
    readiness = [_machine_day_readiness_for_session(item.session_id) for item in sessions]
    readiness_by_session = {item["session_id"]: item for item in readiness}
    scope = get_operator_scope_service().get()
    requested_session_id = request.args.get("session_id", "").strip()
    selected_session = next((item for item in sessions if item.session_id == requested_session_id), None) if requested_session_id else None
    if not requested_session_id and scope.is_active and sessions:
        scoped_sessions = [item for item in sessions if _session_matches_scope(item, start_date=str(scope.start_date), end_date=str(scope.end_date))]
        selected_session = scoped_sessions[0] if scoped_sessions else None
    if not requested_session_id and selected_session is None and sessions:
        selected_session = sessions[0]

    trend = {"labels": [], "series": []}
    error = ""
    source_path = ""
    if requested_session_id and requested_session_id not in readiness_by_session:
        error = f"Selected session was not found: {requested_session_id}"
    elif selected_session is None:
        runtime_phase = runtime_state.get("current_processing_phase")
        if runtime_phase in {"runtime_not_started", "discovery_pending"}:
            error = "No workflow sessions yet. Webapp is up; background discovery is still running."
        else:
            error = "No workflow sessions were found yet. Background processing may still be running."
    else:
        selected_readiness = _machine_day_detail_for_session(selected_session.session_id)
        readiness_by_session[selected_session.session_id] = {
            "session_id": selected_readiness["session_id"],
            "source_path": selected_readiness["source_path"],
            "status": selected_readiness["status"],
            "message": selected_readiness["message"],
        }
        source_path = selected_readiness["source_path"]
        if selected_readiness["status"] != "ready":
            error = selected_readiness["message"]
        else:
            trend, payload_error = _machine_day_chart_payload(selected_readiness["frame"])
            error = payload_error

    chart_type = "bar" if len(trend["labels"]) <= 1 else "line"
    return render_template(
        "machine.html",
        sessions=sessions,
        selected_session=selected_session,
        readiness_by_session=readiness_by_session,
        source_path=source_path,
        trend=trend,
        error=error,
        chart_type=chart_type,
        y_axis_label="Row count",
        runtime_state=runtime_state,
        operator_scope=scope,
    )


def _serialize_playback_timestamp(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    return parsed.dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str.slice(stop=-3) + "Z"


@web.route("/playback")
def playback():
    snap = _catalog().ensure_scanned()
    playback_artifacts = [a for a in snap.artifacts if a.get("playback_compatible") and a.get("visibility") == "default"]
    selected_path = request.args.get("path", playback_artifacts[0]["path"] if playback_artifacts else "")
    machine = request.args.get("machine", "")
    day = request.args.get("day", "")
    scope = get_operator_scope_service().get()

    selected = _catalog().artifact_by_path(selected_path) if selected_path else None
    frame = None
    prepared_frame = None
    validation_reason = None
    context = {"machines": [], "days": []}
    rows = None
    intervals = []
    interval_summary = {"totals": [], "table": []}
    error = None
    machine_days: dict[str, list[str]] = {}
    machine_day_counts: dict[str, dict[str, int]] = {}
    selected_machine_days: list[str] = []
    selected_machine_day_counts: dict[str, int] = {}
    row_payload: list[dict] = []
    signal_columns: list[str] = []
    field_groups: dict[str, list[str]] = {"Signals": [], "State/context": [], "Detection/diagnostics": [], "Other fields": []}
    timeline_payload = {"labels": [], "counts": []}

    if selected:
        source_validation = validate_playback_source(selected_path)
        if not source_validation.is_valid:
            validation_reason = source_validation.reason
        else:
            frame, error = load_playback_frame(selected_path)
        if frame is not None:
            validation = validate_playback_frame(frame)
            if validation.is_valid:
                prepared_frame = prepare_playback_frame(frame)
                context = playback_context(prepared_frame)
                machine_days = playback_days_by_machine(prepared_frame)
                machine_day_counts = playback_day_counts_by_machine(prepared_frame)
                if scope.is_active:
                    context["days"] = [item for item in context["days"] if str(scope.start_date) <= item <= str(scope.end_date)]
                    machine_days = {
                        machine_id: [item for item in days if str(scope.start_date) <= item <= str(scope.end_date)]
                        for machine_id, days in machine_days.items()
                    }
                    machine_day_counts = {
                        machine_id: {
                            machine_day: count
                            for machine_day, count in day_counts.items()
                            if str(scope.start_date) <= machine_day <= str(scope.end_date)
                        }
                        for machine_id, day_counts in machine_day_counts.items()
                    }
                context["machines"] = [m for m in context["machines"] if machine_days.get(m)]
                if prepared_frame.empty:
                    validation_reason = "This playback export exists, but contains no playable rows."
                if not machine and context["machines"]:
                    machine = context["machines"][0]
                if machine and machine not in context["machines"]:
                    machine = context["machines"][0] if context["machines"] else ""
                selected_machine_days = machine_days.get(machine, [])
                selected_machine_day_counts = machine_day_counts.get(machine, {})
                if day and day not in selected_machine_days:
                    day = ""
                if not day and selected_machine_days:
                    day = selected_machine_days[0]
                if machine and day:
                    rows = playback_subset(prepared_frame, machine, day)
                    intervals = interval_rows(rows)
                    interval_summary = summarize_intervals(intervals)
                    if not rows.empty:
                        base_columns = [col for col in rows.columns if col != "day"]
                        payload_frame = rows[base_columns].copy()
                        payload_frame["timestamp"] = _serialize_playback_timestamp(payload_frame["timestamp"])
                        if "source_timestamp" in payload_frame.columns:
                            payload_frame["source_timestamp"] = _serialize_playback_timestamp(payload_frame["source_timestamp"])
                        if "is_synthetic_tick" in payload_frame.columns:
                            payload_frame["is_synthetic_tick"] = payload_frame["is_synthetic_tick"].fillna(False).astype(bool)
                        row_payload = payload_frame.fillna("").to_dict("records")
                        signal_columns = default_live_signal_columns(rows)
                        field_groups = playback_field_groups([col for col in payload_frame.columns if col != "timestamp"])
                        timeline = rows.copy()
                        timeline["timestamp"] = pd.to_datetime(timeline["timestamp"], errors="coerce")
                        timeline = timeline.dropna(subset=["timestamp"])
                        if not timeline.empty:
                            timeline["bucket"] = timeline["timestamp"].dt.floor("min")
                            grouped = timeline.groupby("bucket").size().reset_index(name="count")
                            timeline_payload = {
                                "labels": grouped["bucket"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist(),
                                "counts": grouped["count"].astype(int).tolist(),
                            }
            else:
                validation_reason = validation.reason
    elif not playback_artifacts:
        validation_reason = "No playback-ready timeline exports were found. Run or refresh the workflow to generate playback data."

    return render_template(
        "playback.html",
        playback_artifacts=playback_artifacts,
        selected_path=selected_path,
        machine=machine,
        day=day,
        context=context,
        machine_days=machine_days,
        machine_day_counts=machine_day_counts,
        selected_machine_days=selected_machine_days,
        selected_machine_day_counts=selected_machine_day_counts,
        rows=rows,
        row_payload=row_payload,
        signal_columns=signal_columns,
        field_groups=field_groups,
        intervals=intervals,
        interval_summary=interval_summary,
        timeline_payload=timeline_payload,
        operator_scope=scope,
        validation_reason=validation_reason,
        error=error,
    )


@web.route("/exploration")
def exploration():
    snap = _catalog().ensure_scanned()
    visible_artifacts = [a for a in snap.artifacts if a.get("visibility") == "default"]
    selected_path = request.args.get("path", "")
    chart_type = request.args.get("chart", "line")
    selected = _catalog().artifact_by_path(selected_path) if selected_path else None
    if selected and selected.get("visibility") != "default":
        selected = None

    frame = None
    error = None
    numeric = []
    categorical = []
    chosen_numeric = []
    hist_col = ""
    cat_col = ""
    chart_payload = {"labels": [], "datasets": [], "x_is_time": False}
    category_payload = {"labels": [], "counts": []}
    hist_payload = {"labels": [], "counts": []}
    scope = get_operator_scope_service().get()
    window_start = request.args.get("window_start", "")
    window_end = request.args.get("window_end", "")
    window_preset = request.args.get("window_preset", "full")
    aggregation = request.args.get("aggregation", "auto")
    if scope.is_active and not window_start:
        window_start = f"{scope.start_date}T00:00"
    if scope.is_active and not window_end:
        window_end = f"{scope.end_date}T23:59"

    if selected:
        frame, error = safe_load_artifact_frame(selected_path)
        if frame is not None and not frame.empty:
            numeric = numeric_columns(frame)
            categorical = category_columns(frame)
            chosen_numeric = request.args.getlist("num") or numeric[: min(3, len(numeric))]
            if chart_type in {"line", "scatter"} and chosen_numeric:
                chart_payload = line_or_scatter_data(
                    frame,
                    chosen_numeric,
                    mode=chart_type,
                    window_start=window_start or None,
                    window_end=window_end or None,
                    window_preset=window_preset,
                    aggregation=aggregation,
                )
            if chart_type == "histogram" and numeric:
                hist_col = request.args.get("hist_col", numeric[0])
                if hist_col in numeric:
                    hist_payload = histogram_data(frame, hist_col)
            if chart_type == "bar" and categorical:
                cat_col = request.args.get("cat_col", categorical[0])
                if cat_col in frame.columns:
                    category_payload = category_counts(frame, cat_col)

    return render_template(
        "exploration.html",
        artifacts=visible_artifacts,
        selected=selected,
        frame=frame,
        error=error,
        chart_type=chart_type,
        numeric=numeric,
        categorical=categorical,
        chosen_numeric=chosen_numeric,
        hist_col=hist_col,
        cat_col=cat_col,
        chart_payload=chart_payload,
        category_payload=category_payload,
        hist_payload=hist_payload,
        window_start=window_start,
        window_end=window_end,
        window_preset=window_preset,
        aggregation=aggregation,
        operator_scope=scope,
    )


@web.route("/control")
def control():
    route_started = pd.Timestamp.utcnow()
    selected_session_id = request.args.get("session_id")
    control_build_started = pd.Timestamp.utcnow()
    panel, cache_state = get_operator_page_cache().get_control_snapshot(selected_session_id=selected_session_id)
    build_ms = max((pd.Timestamp.utcnow() - control_build_started).total_seconds() * 1000.0, 0.0)
    total_ms = max((pd.Timestamp.utcnow() - route_started).total_seconds() * 1000.0, 0.0)
    current_app.logger.info(
        "control GET cache=%s snapshot_ms=%.2f route_ms=%.2f selected_session=%s",
        cache_state,
        build_ms,
        total_ms,
        selected_session_id or "",
    )
    operator_scope = get_operator_scope_service().get()
    return render_template("control.html", panel=panel, operator_scope=operator_scope)


@web.post("/control/scope")
def control_scope():
    start_date = (request.form.get("start_date") or "").strip()
    end_date = (request.form.get("end_date") or "").strip()
    selected_session_id = (request.form.get("selected_session_id") or "").strip() or None
    if not start_date or not end_date:
        get_operator_scope_service().clear()
        flash("Cleared shared operator scope.", "success")
    elif end_date < start_date:
        flash("End date must be greater than or equal to start date.", "error")
    else:
        get_operator_scope_service().set(start_date=start_date, end_date=end_date, selected_session_id=selected_session_id)
        flash(f"Shared operator scope set to {start_date}..{end_date}.", "success")
    get_operator_page_cache().invalidate_all()
    return redirect(url_for("web.control"))


@web.post("/control/action")
def control_action():
    action = request.form.get("action", "")
    selected_session_id = request.form.get("selected_session_id")
    scope_mode = request.form.get("scope_mode")
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    ok, message, target_session_id = get_control_panel_service().trigger_action(
        action,
        selected_session_id=selected_session_id,
        scope_mode=scope_mode,
        start_date=start_date,
        end_date=end_date,
    )
    get_workflow_session_index().invalidate()
    get_operator_page_cache().invalidate_all()
    flash(message, "success" if ok else "error")
    target_session = target_session_id or selected_session_id or ""
    return redirect(url_for("web.control", session_id=target_session))


@web.post("/control/script/<script_key>/run")
def run_script_control(script_key: str):
    selected_session_id = request.form.get("selected_session_id")
    scope_mode = request.form.get("scope_mode")
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    ok, message, target_session_id = get_control_panel_service().trigger_action(
        "run_script",
        script_key=script_key,
        selected_session_id=selected_session_id,
        scope_mode=scope_mode,
        start_date=start_date,
        end_date=end_date,
    )
    get_workflow_session_index().invalidate()
    get_operator_page_cache().invalidate_all()
    flash(message, "success" if ok else "error")
    target_session = target_session_id or selected_session_id or ""
    return redirect(url_for("web.control", session_id=target_session))


@web.post("/rescan")
def rescan():
    _catalog().rescan()
    get_workflow_session_index().invalidate()
    get_operator_page_cache().invalidate_overview()
    target = request.form.get("next") or url_for("web.overview")
    return redirect(target)


@web.post("/refresh")
def refresh():
    if get_runtime_manager().requires_startup_choice():
        flash("Choose startup mode (Continue vs Start clean) before running refresh.", "error")
        return redirect(url_for("web.startup", next=url_for("web.status")))
    get_runtime_manager().request_refresh()
    _catalog().rescan()
    get_workflow_session_index().invalidate()
    get_operator_page_cache().invalidate_all()
    target = request.form.get("next") or url_for("web.status")
    return redirect(target)
