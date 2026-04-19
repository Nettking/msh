from __future__ import annotations

from flask import Blueprint, current_app, redirect, render_template, request, url_for

from catalog.orchestrator.pipeline import get_runtime_manager

from .services.catalog_service import ArtifactCatalog, safe_load_artifact_frame
from .services.chart_service import category_columns, category_counts, histogram_data, line_or_scatter_data, machine_day_trend, numeric_columns
from .services.playback_service import interval_rows, playback_context, playback_subset, summarize_intervals, validate_playback_frame, validate_playback_source

web = Blueprint("web", __name__)


def _catalog() -> ArtifactCatalog:
    return current_app.config["ARTIFACT_CATALOG"]


@web.route("/")
def overview():
    snap = _catalog().ensure_scanned()
    artifacts = snap.artifacts
    overview_artifacts = [a for a in artifacts if a.get("visibility") == "default"]
    source_artifacts = [a for a in overview_artifacts if a.get("category") == "source_data"]
    derived_artifacts = [a for a in overview_artifacts if a.get("category") == "derived_output"]
    hidden_workflow_copies = len([a for a in artifacts if a.get("category") == "workflow_data_copy"])
    hidden_internal_metadata = len([a for a in artifacts if a.get("category") == "internal_metadata"])
    playback_count = len([a for a in overview_artifacts if a.get("playback_compatible")])
    read_errors = len([a for a in overview_artifacts if a.get("status") != "ready"])
    return render_template(
        "overview.html",
        artifacts=overview_artifacts[:25],
        total=len(overview_artifacts),
        source_total=len(source_artifacts),
        derived_total=len(derived_artifacts),
        hidden_workflow_copies=hidden_workflow_copies,
        hidden_internal_metadata=hidden_internal_metadata,
        playback_count=playback_count,
        read_errors=read_errors,
        warnings=snap.warnings,
        scan_dirs=_catalog().scan_dirs,
        scanned_at_epoch=snap.scanned_at_epoch,
    )


@web.route("/status")
def status():
    snap = _catalog().ensure_scanned()
    runtime_state = get_runtime_manager().state_snapshot()
    internal_artifacts = [a for a in snap.artifacts if a.get("is_internal")]
    return render_template(
        "status.html",
        snapshot=snap,
        scan_dirs=_catalog().scan_dirs,
        runtime_state=runtime_state,
        internal_artifacts=internal_artifacts,
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
    snap = _catalog().ensure_scanned()
    visible_artifacts = [a for a in snap.artifacts if a.get("visibility") == "default"]
    selected_path = request.args.get("path", "")
    selected = _catalog().artifact_by_path(selected_path) if selected_path else None
    if selected and selected.get("visibility") != "default":
        selected = None
    trend = {"labels": [], "series": []}
    error = None
    if selected:
        frame, error = safe_load_artifact_frame(selected_path)
        if frame is not None:
            trend = machine_day_trend(frame)
    return render_template("machine.html", artifacts=visible_artifacts, selected=selected, trend=trend, error=error)


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
