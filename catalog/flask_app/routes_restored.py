from __future__ import annotations

from pathlib import Path

import pandas as pd
from flask import current_app, flash, redirect, render_template, request, url_for

from catalog.runner.session_store import list_sessions

from .routes import (
    web,
    _catalog,
    _load_telemetry_cache_exploration_frame,
    _machine_day_detail_for_session,
    _machine_day_detail_from_cache,
    _machine_day_readiness_for_session,
    _session_matches_scope,
    _strategy_config_service,
    _telemetry_cache_exploration_artifact,
    _telemetry_cache_status_model,
)
from .services.catalog_service import safe_load_artifact_frame
from .services.chart_service import category_columns, category_counts, histogram_data, line_or_scatter_data, machine_day_trend, numeric_columns
from .services.control_service import get_control_panel_service
from .services.operator_page_cache import get_operator_page_cache
from .services.operator_scope_service import get_operator_scope_service
from .services.playback_service import (
    TELEMETRY_CACHE_PLAYBACK_PATH,
    default_live_signal_columns,
    filter_playback_artifacts_for_runtime,
    interval_rows,
    load_cached_playback_frame_for_machine_day,
    load_playback_frame,
    playback_field_groups,
    playback_subset,
    prepare_playback_frame,
    resolve_playback_selection,
    summarize_intervals,
    telemetry_cache_playback_artifact,
    validate_playback_frame,
    validate_playback_source,
)
from .services.workflow_session_index import get_workflow_session_index
from catalog.orchestrator.pipeline import get_runtime_manager


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
    snap = _catalog().cached_snapshot()
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
    return render_template("analyses.html", artifacts=visible_artifacts, selected=selected, frame=frame, load_error=load_error, trend=trend)


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
    cache_detail = None if requested_session_id else _machine_day_detail_from_cache(scope)
    if cache_detail is not None and cache_detail["status"] == "ready":
        source_path = cache_detail["source_path"]
        trend, error = _machine_day_chart_payload(cache_detail["frame"])
    elif requested_session_id and requested_session_id not in readiness_by_session:
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
            trend, error = _machine_day_chart_payload(selected_readiness["frame"])

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
        telemetry_cache_status=_telemetry_cache_status_model(),
    )


def _serialize_playback_timestamp(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    return parsed.dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str.slice(stop=-3) + "Z"


@web.route("/playback")
def playback():
    catalog = _catalog()
    snap = catalog.cached_snapshot()
    runtime_manager = get_runtime_manager()
    runtime_state = runtime_manager.state_snapshot() if hasattr(runtime_manager, "state_snapshot") else {}
    requested_path = request.args.get("path", "")

    discovered = [a for a in snap.artifacts if a.get("playback_compatible") and a.get("visibility") == "default"]
    cache_artifact = telemetry_cache_playback_artifact()
    if cache_artifact is not None:
        discovered.append(cache_artifact)
    playback_artifacts = filter_playback_artifacts_for_runtime(discovered, runtime_state, selected_path=requested_path, logger=current_app.logger)
    playback_artifacts.sort(key=lambda item: (0 if str(item.get("file_name", "")).lower() == "timeline_rows.csv" else 1, str(item.get("file_name", "")).lower(), str(item.get("path", ""))))

    scope = get_operator_scope_service().get()
    selection = resolve_playback_selection(
        playback_artifacts,
        runtime_state,
        requested_path=requested_path,
        requested_machine=request.args.get("machine", ""),
        requested_day=request.args.get("day", ""),
        scope=scope,
    )
    selected_path = selection.selected_path
    machine = selection.machine
    day = selection.day
    selected = telemetry_cache_playback_artifact() if selected_path == TELEMETRY_CACHE_PLAYBACK_PATH else (_catalog().artifact_by_path(selected_path) if selected_path else None)

    prepared_frame = None
    validation_reason = None
    rows = None
    intervals = []
    interval_summary = {"totals": [], "table": []}
    error = None
    row_payload: list[dict] = []
    signal_columns: list[str] = []
    field_groups: dict[str, list[str]] = {"Signals": [], "State/context": [], "Detection/diagnostics": [], "Other fields": []}
    timeline_payload = {"labels": [], "counts": []}

    if selected:
        source_validation = validate_playback_source(selected_path)
        if not source_validation.is_valid:
            validation_reason = source_validation.reason
        else:
            if selected_path == TELEMETRY_CACHE_PLAYBACK_PATH and machine and day:
                current_app.logger.info("playback using DuckDB/Parquet cache machine=%s day=%s", machine, day)
                frame, error = load_cached_playback_frame_for_machine_day(machine, day)
            else:
                current_app.logger.info("playback using session export fallback path=%s", selected_path)
                frame, error = load_playback_frame(selected_path)
            if frame is not None:
                validation = validate_playback_frame(frame)
                if validation.is_valid:
                    prepared_frame = prepare_playback_frame(frame)
                else:
                    validation_reason = validation.reason
        if prepared_frame is not None:
            if prepared_frame.empty:
                validation_reason = "This playback export exists, but contains no playable rows."
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
                        timeline_payload = {"labels": grouped["bucket"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist(), "counts": grouped["count"].astype(int).tolist()}
    elif not playback_artifacts:
        validation_reason = "No playback-ready timeline exports were found. Run or refresh the workflow to generate playback data."

    return render_template(
        "playback.html",
        playback_artifacts=playback_artifacts,
        selected_path=selected_path,
        machine=machine,
        day=day,
        context=selection.context,
        machine_days=selection.machine_days,
        machine_day_counts=selection.machine_day_counts,
        selected_machine_days=selection.selected_machine_days,
        selected_machine_day_counts=selection.selected_machine_day_counts,
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
    snap = _catalog().cached_snapshot()
    visible_artifacts = [a for a in snap.artifacts if a.get("visibility") == "default"]
    cache_exploration_artifact = _telemetry_cache_exploration_artifact()
    if cache_exploration_artifact is not None:
        visible_artifacts.insert(0, cache_exploration_artifact)
    selected_path = request.args.get("path", "")
    chart_type = request.args.get("chart", "line")
    selected = cache_exploration_artifact if selected_path == "telemetry-cache://samples" else (_catalog().artifact_by_path(selected_path) if selected_path else None)
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
        if selected_path == "telemetry-cache://samples":
            frame, error = _load_telemetry_cache_exploration_frame(window_start, window_end)
        else:
            current_app.logger.info("exploration using artifact/session fallback path=%s", selected_path)
            frame, error = safe_load_artifact_frame(selected_path)
        if frame is not None and not frame.empty:
            numeric = numeric_columns(frame)
            categorical = category_columns(frame)
            chosen_numeric = request.args.getlist("num") or numeric[: min(3, len(numeric))]
            if chart_type in {"line", "scatter"} and chosen_numeric:
                chart_payload = line_or_scatter_data(frame, chosen_numeric, mode=chart_type, window_start=window_start or None, window_end=window_end or None, window_preset=window_preset, aggregation=aggregation)
            if chart_type == "histogram" and numeric:
                hist_col = request.args.get("hist_col", numeric[0])
                if hist_col in numeric:
                    hist_payload = histogram_data(frame, hist_col)
            if chart_type == "bar" and categorical:
                cat_col = request.args.get("cat_col", categorical[0])
                if cat_col in frame.columns:
                    category_payload = category_counts(frame, cat_col)
    return render_template("exploration.html", artifacts=visible_artifacts, selected=selected, frame=frame, error=error, chart_type=chart_type, numeric=numeric, categorical=categorical, chosen_numeric=chosen_numeric, hist_col=hist_col, cat_col=cat_col, chart_payload=chart_payload, category_payload=category_payload, hist_payload=hist_payload, window_start=window_start, window_end=window_end, window_preset=window_preset, aggregation=aggregation, operator_scope=scope)


@web.get("/strategies")
def strategies():
    page = _strategy_config_service().page_model()
    return render_template("strategies.html", page=page)


@web.post("/strategies/save")
def save_strategies():
    service = _strategy_config_service()
    base_page = service.page_model()
    strategies = service.parse_form(request.form)
    validation = service.validate(strategies, labels=base_page.labels)
    if validation.errors:
        for error in validation.errors:
            flash(error, "error")
        page = base_page.__class__(strategies=strategies, labels=base_page.labels, signature=validation.signature, validation_errors=validation.errors, validation_warnings=validation.warnings, summary=service.summary(strategies, validation.signature), supported_types=base_page.supported_types, strategies_path=base_page.strategies_path, labels_path=base_page.labels_path)
        return render_template("strategies.html", page=page), 400
    signature = service.save(strategies)
    flash(f"Strategy config saved. New active strategy signature: {signature}", "success")
    flash("Strategy edits invalidate cached candidate outputs; candidate events regenerate the next time playback exports are prepared or rerun.", "info")
    return redirect(url_for("web.strategies"))


@web.route("/control")
def control():
    route_started = pd.Timestamp.utcnow()
    selected_session_id = request.args.get("session_id")
    control_build_started = pd.Timestamp.utcnow()
    panel, cache_state = get_operator_page_cache().get_control_snapshot(selected_session_id=selected_session_id)
    build_ms = max((pd.Timestamp.utcnow() - control_build_started).total_seconds() * 1000.0, 0.0)
    total_ms = max((pd.Timestamp.utcnow() - route_started).total_seconds() * 1000.0, 0.0)
    current_app.logger.info("control GET cache=%s snapshot_ms=%.2f route_ms=%.2f selected_session=%s", cache_state, build_ms, total_ms, selected_session_id or "")
    operator_scope = get_operator_scope_service().get()
    return render_template("control.html", panel=panel, operator_scope=operator_scope, telemetry_cache_status=_telemetry_cache_status_model())


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
    ok, message, target_session_id = get_control_panel_service().trigger_action(
        request.form.get("action", ""),
        selected_session_id=request.form.get("selected_session_id"),
        scope_mode=request.form.get("scope_mode"),
        start_date=request.form.get("start_date"),
        end_date=request.form.get("end_date"),
    )
    get_workflow_session_index().invalidate()
    get_operator_page_cache().invalidate_all()
    flash(message, "success" if ok else "error")
    return redirect(url_for("web.control", session_id=target_session_id or request.form.get("selected_session_id") or ""))


@web.post("/control/script/<script_key>/run")
def run_script_control(script_key: str):
    ok, message, target_session_id = get_control_panel_service().trigger_action(
        "run_script",
        script_key=script_key,
        selected_session_id=request.form.get("selected_session_id"),
        scope_mode=request.form.get("scope_mode"),
        start_date=request.form.get("start_date"),
        end_date=request.form.get("end_date"),
    )
    get_workflow_session_index().invalidate()
    get_operator_page_cache().invalidate_all()
    flash(message, "success" if ok else "error")
    return redirect(url_for("web.control", session_id=target_session_id or request.form.get("selected_session_id") or ""))


@web.post("/rescan")
def rescan():
    _catalog().rescan()
    get_workflow_session_index().invalidate()
    get_operator_page_cache().invalidate_overview()
    return redirect(request.form.get("next") or url_for("web.overview"))


@web.post("/refresh")
def refresh():
    if get_runtime_manager().requires_startup_choice():
        flash("Choose startup mode (Continue vs Start clean) before running refresh.", "error")
        return redirect(url_for("web.startup", next=url_for("web.status")))
    get_runtime_manager().request_refresh()
    _catalog().rescan()
    get_workflow_session_index().invalidate()
    get_operator_page_cache().invalidate_all()
    return redirect(request.form.get("next") or url_for("web.status"))
