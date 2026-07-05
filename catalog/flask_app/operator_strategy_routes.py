from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for

from .services.operator_strategy_service import DEFAULT_TIMEZONE, OperatorStrategyError, OperatorStrategyService


operator_strategy_web = Blueprint("operator_strategy_web", __name__, url_prefix="/operator-strategies")


def _service() -> OperatorStrategyService:
    return OperatorStrategyService()


@operator_strategy_web.get("")
def index():
    service = _service()
    try:
        records = service.recent_records(limit=30)
        load_error = ""
    except OperatorStrategyError as exc:
        records = []
        load_error = str(exc)
    return render_template(
        "operator_strategies.html",
        records=records,
        load_error=load_error,
        default_timezone=DEFAULT_TIMEZONE,
        records_path=service.records_path.as_posix(),
    )


@operator_strategy_web.post("/save")
def save():
    try:
        record = _service().add_from_form(request.form)
    except OperatorStrategyError as exc:
        flash(str(exc), "error")
    else:
        flash(f"Recorded operator strategy decision at {record.decision_time}.", "success")
    return redirect(url_for("operator_strategy_web.index"))


@operator_strategy_web.post("/delete/<record_id>")
def delete(record_id: str):
    try:
        deleted = _service().delete(record_id)
    except OperatorStrategyError as exc:
        flash(str(exc), "error")
    else:
        flash("Operator strategy record deleted." if deleted else "Operator strategy record was not found.", "success" if deleted else "warning")
    return redirect(url_for("operator_strategy_web.index"))
