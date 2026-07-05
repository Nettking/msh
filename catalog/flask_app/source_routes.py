from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for

from .services.observer_phoenix_config_service import ObserverPhoenixConfigService
from .services.source_connection_test_service import SourceConnectionTestService
from .services.source_inventory_service import SourceInventoryService


source_web = Blueprint("source_web", __name__, url_prefix="/sources")


def _observer_phoenix_service() -> ObserverPhoenixConfigService:
    return ObserverPhoenixConfigService()


def _source_inventory_service() -> SourceInventoryService:
    return SourceInventoryService()


def _connection_test_service() -> SourceConnectionTestService:
    return SourceConnectionTestService(_source_inventory_service())


@source_web.get("/")
def index():
    inventory = _source_inventory_service().status_model()
    observer_status = _observer_phoenix_service().status_model()
    return render_template("sources.html", inventory=inventory, observer_status=observer_status)


@source_web.post("/machines/add")
def add_machine():
    ok, message = _source_inventory_service().add_machine_from_form(request.form)
    flash(message, "success" if ok else "error")
    return redirect(url_for("source_web.index"))


@source_web.post("/machines/<machine_id>/delete")
def delete_machine(machine_id: str):
    ok, message = _source_inventory_service().delete_machine(machine_id)
    flash(message, "success" if ok else "error")
    return redirect(url_for("source_web.index"))


@source_web.post("/machines/<machine_id>/test-mtconnect")
def test_machine_mtconnect(machine_id: str):
    result = _connection_test_service().test_mtconnect(machine_id)
    detail = f" {result.detail}" if result.detail else ""
    flash(f"{result.title}: {result.message} Target: {result.target}.{detail}", "success" if result.ok else "error")
    return redirect(url_for("source_web.index"))


@source_web.post("/machines/<machine_id>/test-vpn")
def test_machine_vpn(machine_id: str):
    result = _connection_test_service().test_vpn_network(machine_id)
    detail = f" {result.detail}" if result.detail else ""
    flash(f"{result.title}: {result.message} Target: {result.target}.{detail}", "success" if result.ok else "error")
    return redirect(url_for("source_web.index"))


@source_web.post("/vibration-sensors/add")
def add_vibration_sensor():
    ok, message = _source_inventory_service().add_vibration_sensor_from_form(request.form)
    flash(message, "success" if ok else "error")
    return redirect(url_for("source_web.index"))


@source_web.post("/vibration-sensors/<sensor_id>/delete")
def delete_vibration_sensor(sensor_id: str):
    ok, message = _source_inventory_service().delete_vibration_sensor(sensor_id)
    flash(message, "success" if ok else "error")
    return redirect(url_for("source_web.index"))


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
