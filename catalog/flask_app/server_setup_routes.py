from __future__ import annotations

from flask import Blueprint, flash, redirect, request, url_for

from catalog.orchestrator.pipeline import get_runtime_manager, start_runtime_background

from .services.server_setup_service import (
    ServerSetupError,
    load_settings,
    pull_ollama_model,
    runtime_should_start,
    save_settings,
    settings_from_form,
)


server_setup_web = Blueprint("server_setup_web", __name__)

_SETUP_PATHS = {"/startup", "/server-setup/save", "/server-setup/pull-model", "/status", "/rescan"}


def _next_path() -> str:
    return request.form.get("next") or request.args.get("next") or url_for("web.overview")


def _startup_step_url(step: str) -> str:
    return url_for("web.startup", next=_next_path(), step=step)


@server_setup_web.before_app_request
def browser_setup_gate():
    """Ensure one-command Docker startup lands in browser setup first.

    This runs before the normal web startup gate because the blueprint is
    registered before `web`. It also handles setup POSTs directly so they are not
    blocked by the older continue-vs-clean startup gate when prior runtime state
    exists.
    """

    if request.endpoint and request.endpoint.startswith("static"):
        return None
    if request.path == "/server-setup/save" and request.method == "POST":
        return _save_from_request()
    if request.path == "/server-setup/pull-model" and request.method == "POST":
        return _pull_from_request()
    if request.path in _SETUP_PATHS:
        return None
    try:
        settings = load_settings()
    except ServerSetupError as exc:
        flash(str(exc), "error")
        return redirect(url_for("web.startup", next=request.full_path if request.query_string else request.path))
    if not settings.configured:
        return redirect(url_for("web.startup", next=request.full_path if request.query_string else request.path))
    return None


def _save_from_request():
    try:
        settings = settings_from_form(request.form)
        save_settings(settings)
    except ServerSetupError as exc:
        flash(str(exc), "error")
        return redirect(_startup_step_url("review"))

    flash("Server setup saved.", "success")
    if runtime_should_start(settings):
        if get_runtime_manager().requires_startup_choice():
            flash("Choose how runtime should start next.", "info")
            return redirect(_startup_step_url("runtime"))
        start_runtime_background()
        flash("Runtime background processing is enabled.", "success")
        return redirect(_next_path())

    flash("Background orchestration is disabled for this setup mode.", "info")
    return redirect(_next_path())


def _pull_from_request():
    settings = load_settings()
    ok, message = pull_ollama_model(settings)
    flash(message, "success" if ok else "error")
    return redirect(_startup_step_url("runtime"))
