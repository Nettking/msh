from __future__ import annotations

from pathlib import Path

import pandas as pd
from flask import Blueprint, current_app, flash, redirect, render_template, request, url_for

from catalog.orchestrator.pipeline import get_runtime_manager
from catalog.runner.session_store import list_sessions

from .services.catalog_service import ArtifactCatalog, safe_load_artifact_frame
from .services.chart_service import category_columns, category_counts, histogram_data, line_or_scatter_data, machine_day_trend, numeric_columns
from .services.control_service import get_control_panel_service
from .services.overview_service import build_overview_snapshot
from .services.playback_service import interval_rows, playback_context, playback_subset, summarize_intervals, validate_playback_frame, validate_playback_source

web = Blueprint("web", __name__)


def _catalog() -> ArtifactCatalog:
    return current_app.config["ARTIFACT_CATALOG"]


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
    overview_snapshot = build_overview_snapshot(_catalog())
    return render_template(
        "overview.html",
        overview=overview_snapshot,
        scan_dirs=_catalog().scan_dirs,
    )


@web.route("/status")
def status():
    snap = _catalog().ensure_scanned()
    runtime_state = get_runtime_manager().state_snapshot()
    internal_artifacts = [a for a in snap.artifacts if a.get("is_internal")]
    phase_messages = {
        "runtime_not_started": "Webapp started. Runtime has not started yet.",
        "discovery_pending": "Webapp started. Background discovery is running.",
        "bootstrap_minimal_processing": "Bootstrap/minimal processing is running in the background.",
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
    )


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
    requested_session_id = request.args.get("session_id", "").strip()
    selected_session = next((item for item in sessions if item.session_id == requested_session_id), None) if requested_session_id else None
    if not requested_session_id and sessions:
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
    )


@web.route("/playback")
def playback():
    snap = _catalog().ensure_scanned()
    playback_artifacts = [a for a in snap.artifacts if a.get("playback_compatible") and a.get("visibility") == "default"]
    selected_path = request.args.get("path", playback_artifacts[0]["path"] if playback_artifacts else "")
    machine = request.args.get("machine", "")
    day = request.args.get("day", "")

    selected = _catalog().artifact_by_path(selected_path) if selected_path else None
    frame = None
    validation_reason = None
    context = {"machines": [], "days": []}
    rows = None
    intervals = []
    interval_summary = {"totals": [], "table": []}
    error = None

    if selected:
        source_validation = validate_playback_source(selected_path)
        if not source_validation.is_valid:
            validation_reason = source_validation.reason
        else:
            frame, error = safe_load_artifact_frame(selected_path)
        if frame is not None:
            validation = validate_playback_frame(frame)
            if validation.is_valid:
                context = playback_context(frame)
                if not machine and context["machines"]:
                    machine = context["machines"][0]
                if not day and context["days"]:
                    day = context["days"][0]
                if machine and day:
                    rows = playback_subset(frame, machine, day)
                    intervals = interval_rows(rows)
                    interval_summary = summarize_intervals(intervals)
            else:
                validation_reason = validation.reason

    return render_template(
        "playback.html",
        playback_artifacts=playback_artifacts,
        selected_path=selected_path,
        machine=machine,
        day=day,
        context=context,
        rows=rows,
        intervals=intervals,
        interval_summary=interval_summary,
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

    if selected:
        frame, error = safe_load_artifact_frame(selected_path)
        if frame is not None and not frame.empty:
            numeric = numeric_columns(frame)
            categorical = category_columns(frame)
            chosen_numeric = request.args.getlist("num") or numeric[: min(3, len(numeric))]
            if chart_type in {"line", "scatter"} and chosen_numeric:
                chart_payload = line_or_scatter_data(frame, chosen_numeric, mode=chart_type)
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
    )


@web.route("/control")
def control():
    selected_session_id = request.args.get("session_id")
    panel = get_control_panel_service().snapshot(selected_session_id=selected_session_id)
    return render_template("control.html", panel=panel)


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
    flash(message, "success" if ok else "error")
    target_session = target_session_id or selected_session_id or ""
    return redirect(url_for("web.control", session_id=target_session))


@web.post("/rescan")
def rescan():
    _catalog().rescan()
    target = request.form.get("next") or url_for("web.overview")
    return redirect(target)


@web.post("/refresh")
def refresh():
    get_runtime_manager().request_refresh()
    _catalog().rescan()
    target = request.form.get("next") or url_for("web.status")
    return redirect(target)
