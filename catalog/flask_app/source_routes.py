from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for

from .services.observer_phoenix_config_service import ObserverPhoenixConfigService


source_web = Blueprint("source_web", __name__, url_prefix="/sources")


def _observer_phoenix_service() -> ObserverPhoenixConfigService:
    return ObserverPhoenixConfigService()


@source_web.get("/observer-phoenix")
def observer_phoenix():
    service = _observer_phoenix_service()
    return render_template("observer_phoenix.html", status=service.status_model())


@source_web.post("/observer-phoenix/save")
def save_observer_phoenix():
    ok, message = _observer_phoenix_service().save_from_form(request.form)
    flash(message, "success" if ok else "error")
    return redirect(url_for("source_web.observer_phoenix"))


@source_web.post("/observer-phoenix/test")
def test_observer_phoenix():
    ok, message = _observer_phoenix_service().test_connection()
    flash(message, "success" if ok else "error")
    return redirect(url_for("source_web.observer_phoenix"))


@source_web.post("/observer-phoenix/clear")
def clear_observer_phoenix():
    ok, message = _observer_phoenix_service().clear_local()
    flash(message, "success" if ok else "error")
    return redirect(url_for("source_web.observer_phoenix"))
