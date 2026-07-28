from __future__ import annotations

import hmac
import secrets

from flask import Blueprint, flash, jsonify, redirect, request, session, url_for

from catalog.orchestrator.pipeline import get_runtime_manager, start_runtime_background

from .services.ai_model_benchmark_service import compare_ollama_setup_models
from .services.mtconnect_discovery_service import (
    MtconnectDiscoveryError,
    get_mtconnect_discovery_service,
)
from .services.recorder_control_service import (
    RecorderControlError,
    get_recorder_control_service,
)
from .services.server_setup_service import (
    AI_MODEL_CHOICES,
    ServerSetupError,
    ai_settings_from_form,
    default_settings,
    load_settings,
    ollama_status,
    pull_ollama_model,
    role_has_recorder,
    role_uses_ai,
    runtime_should_start,
    save_settings,
    settings_from_form,
)

server_setup_web = Blueprint("server_setup_web", __name__)

_MTCONNECT_CSRF_SESSION_KEY = "mtconnect_discovery_csrf_token"

_SETUP_PATHS = {
    "/startup",
    "/server-setup/save",
    "/server-setup/pull-model",
    "/server-setup/test-ai-model",
    "/server-setup/test-ai-connection",
    "/server-setup/compare-ai-models",
    "/server-setup/recording/start",
    "/server-setup/recording/stop",
    "/status/mtconnect-scan",
    "/status/mtconnect-sources",
    "/rescan",
}


def _mtconnect_csrf_token() -> str:
    token = str(session.get(_MTCONNECT_CSRF_SESSION_KEY) or "")
    if not token:
        token = secrets.token_urlsafe(32)
        session[_MTCONNECT_CSRF_SESSION_KEY] = token
    return token


@server_setup_web.app_context_processor
def inject_mtconnect_csrf_token():
    """Expose a per-browser token only to forms rendered by this app."""

    return {"mtconnect_discovery_csrf_token": _mtconnect_csrf_token()}


def _require_setup_csrf(*, local_only: bool = False) -> None:
    """Reject cross-site or tokenless requests before changing local setup."""

    if local_only:
        request_host = request.host.casefold()
        local_host = request_host in {"localhost", "127.0.0.1", "[::1]"} or (
            request_host.startswith(("localhost:", "127.0.0.1:", "[::1]:"))
        )
        if not local_host:
            raise MtconnectDiscoveryError(
                "MTConnect network discovery can only be changed from the "
                "browser on the MSH machine."
            )
    if request.headers.get("Sec-Fetch-Site", "").casefold() == "cross-site":
        raise MtconnectDiscoveryError(
            "The setup request was blocked because it came from "
            "another site. Reload the MSH Setup page and try again."
        )
    expected = str(session.get(_MTCONNECT_CSRF_SESSION_KEY) or "")
    supplied = str(request.form.get("_csrf_token") or "")
    if not expected or not supplied or not hmac.compare_digest(expected, supplied):
        raise MtconnectDiscoveryError(
            "The setup form expired. Reload the MSH Setup page "
            "and try again."
        )


def _require_mtconnect_csrf() -> None:
    _require_setup_csrf(local_only=True)


def _next_path() -> str:
    value = request.form.get("next") or request.args.get("next") or ""
    if value.startswith("/") and not value.startswith("//"):
        return value
    return url_for("web.overview")


def _startup_step_url(step: str, *, next_path: str | None = None) -> str:
    return url_for("web.startup", next=next_path or _next_path(), step=step)


@server_setup_web.before_app_request
def browser_setup_gate():
    """Ensure one-command Docker startup lands in browser setup first.

    Setup and recorder-control POSTs are handled here before the older runtime
    choice gate. Recording is intentionally independent from the analysis-session
    continue-vs-clean decision.
    """

    if request.endpoint and request.endpoint.startswith("static"):
        return None
    if request.path == "/server-setup/save" and request.method == "POST":
        return _save_from_request()
    if request.path == "/server-setup/pull-model" and request.method == "POST":
        return _pull_from_request()
    if request.path == "/server-setup/test-ai-model" and request.method == "POST":
        return _test_ai_model_from_request()
    if request.path == "/server-setup/test-ai-connection" and request.method == "POST":
        return _test_ai_connection_from_request()
    if request.path == "/server-setup/compare-ai-models" and request.method == "POST":
        return _compare_ai_models_from_request()
    if request.path == "/server-setup/recording/start" and request.method == "POST":
        return _set_recording_from_request(True)
    if request.path == "/server-setup/recording/stop" and request.method == "POST":
        return _set_recording_from_request(False)
    if request.path in _SETUP_PATHS:
        return None

    try:
        settings = load_settings()
    except ServerSetupError as exc:
        flash(str(exc), "error")
        return redirect(
            url_for(
                "web.startup",
                next=request.full_path if request.query_string else request.path,
            )
        )
    if not settings.configured or not settings.user_setup_complete:
        return redirect(
            url_for(
                "web.startup",
                next=request.full_path if request.query_string else request.path,
            )
        )
    return None


def _set_recording_from_request(enabled: bool):
    destination = request.form.get("next") or url_for("web.startup")
    if not destination.startswith("/") or destination.startswith("//"):
        destination = url_for("web.startup")

    try:
        _require_setup_csrf()
        settings = load_settings()
        ok, message = get_recorder_control_service().set_enabled(enabled, settings)
    except (
        MtconnectDiscoveryError,
        ServerSetupError,
        RecorderControlError,
    ) as exc:
        flash(str(exc), "error")
        return redirect(destination)

    flash(message, "success" if ok else "error")
    return redirect(destination)


@server_setup_web.post("/status/mtconnect-scan")
def scan_mtconnect_network():
    wants_json = (
        request.accept_mimetypes["application/json"]
        > request.accept_mimetypes["text/html"]
    )
    try:
        _require_mtconnect_csrf()
        result = get_mtconnect_discovery_service().scan(
            str(request.form.get("cidr") or ""),
            port=str(request.form.get("port") or "5000"),
        )
    except MtconnectDiscoveryError as exc:
        message = str(exc)
        if wants_json:
            return jsonify({"ok": False, "message": message}), 400
        flash(message, "error")
        return redirect(url_for("web.status") + "#mtconnect-discovery")

    message = (
        "Network scan complete: "
        f"{result.get('machines_found', 0)} machine(s) from "
        f"{result.get('agents_found', 0)} MTConnect Agent(s)."
    )
    if wants_json:
        return jsonify({"ok": True, "scan": result, "message": message})
    flash(
        message,
        "success",
    )
    return redirect(url_for("web.status") + "#mtconnect-discovery")


@server_setup_web.post("/status/mtconnect-sources")
def save_discovered_mtconnect_sources():
    try:
        _require_mtconnect_csrf()
        settings = load_settings()
        settings = get_mtconnect_discovery_service().merge_selected_results(
            settings,
            request.form.getlist("source_id"),
        )
        save_settings(settings)
    except (MtconnectDiscoveryError, ServerSetupError) as exc:
        flash(str(exc), "error")
    else:
        flash(
            "Selected MTConnect machines were saved as recorder sources. "
            "Use Start recording when you are ready to collect data.",
            "success",
        )
    return redirect(url_for("web.status") + "#mtconnect-discovery")


def _save_from_request():
    try:
        _require_setup_csrf()
    except MtconnectDiscoveryError as exc:
        flash(str(exc), "error")
        return redirect(_startup_step_url("review"))

    try:
        previous_settings = load_settings()
        first_user_setup = not (
            previous_settings.configured and previous_settings.user_setup_complete
        )
    except ServerSetupError:
        previous_settings = default_settings(configured=False)
        first_user_setup = True

    try:
        settings = settings_from_form(request.form, previous_settings)
        selected_source_ids = request.form.getlist("source_id")
        if selected_source_ids:
            _require_mtconnect_csrf()
            settings = (
                get_mtconnect_discovery_service().merge_selected_results(
                    settings,
                    selected_source_ids,
                )
            )
        save_settings(settings)
        if (
            not role_has_recorder(settings.deployment_mode)
            or not settings.recorder_sources.strip()
        ):
            get_recorder_control_service().set_enabled(False, settings)
    except (
        MtconnectDiscoveryError,
        ServerSetupError,
        RecorderControlError,
    ) as exc:
        flash(str(exc), "error")
        return redirect(_startup_step_url("review"))

    if first_user_setup and settings.deployment_mode == "recorder-only":
        next_path = url_for("web.status") + "#recorder-status"
    elif first_user_setup:
        next_path = url_for("web.get_started")
    else:
        next_path = _next_path()
    flash("Device setup saved.", "success")
    if runtime_should_start(settings):
        if get_runtime_manager().requires_startup_choice():
            flash("Choose how runtime should start next.", "info")
            return redirect(_startup_step_url("runtime", next_path=next_path))
        start_runtime_background()
        flash("Runtime background processing is enabled.", "success")
        return redirect(next_path)

    flash("Background orchestration is disabled for this setup mode.", "info")
    return redirect(next_path)


def _settings_for_ai_form():
    settings = load_settings()
    deployment_mode = str(
        request.form.get("deployment_mode")
        or settings.deployment_mode
        or ""
    ).strip()
    if not role_uses_ai(deployment_mode):
        raise ServerSetupError(
            "Language-model actions are not available for the selected role."
        )
    return ai_settings_from_form(request.form, settings)


def _selected_ai_model_from_request(settings) -> str:
    profile = str(
        request.form.get("ai_profile")
        or settings.ai_profile
        or "laptop-standard"
    ).strip()
    if profile in AI_MODEL_CHOICES:
        return AI_MODEL_CHOICES[profile]["model"]
    return str(request.form.get("model") or settings.ai_model or "").strip()


def _legacy_shape_for_ai_page(result: dict) -> dict:
    recommendation = result.get("recommendation") or {}
    recommended_model = recommendation.get("recommended_model") or ""
    rows = result.get("rows") or []
    recommended_row = next(
        (row for row in rows if row.get("model") == recommended_model),
        None,
    )
    recommended_result = (recommended_row or {}).get("result") or {}
    result.setdefault(
        "model",
        recommended_model or _selected_ai_model_from_request(load_settings()),
    )
    result.setdefault("elapsed_ms", recommended_result.get("elapsed_ms"))
    result.setdefault(
        "assessment",
        {
            "label": (
                recommendation.get("recommended_label")
                or recommendation.get("verdict")
                or "No recommendation"
            ),
            "description": (
                recommendation.get("message")
                or result.get("message")
                or "No recommendation available."
            ),
        },
    )
    return result


def _test_ai_model_from_request():
    try:
        _require_setup_csrf()
        settings = _settings_for_ai_form()
    except (MtconnectDiscoveryError, ServerSetupError) as exc:
        return jsonify({"ok": False, "message": str(exc)}), 400

    result = _legacy_shape_for_ai_page(compare_ollama_setup_models(settings))
    return jsonify(result), 200 if result.get("ok") else 503


def _test_ai_connection_from_request():
    try:
        _require_setup_csrf()
        settings = _settings_for_ai_form()
    except (MtconnectDiscoveryError, ServerSetupError) as exc:
        return jsonify({"ok": False, "message": str(exc)}), 400

    status = ollama_status(settings, timeout_seconds=3.0)
    status["ok"] = bool(status.get("running"))
    return jsonify(status), 200 if status["ok"] else 503


def _compare_ai_models_from_request():
    try:
        _require_setup_csrf()
        settings = _settings_for_ai_form()
    except (MtconnectDiscoveryError, ServerSetupError) as exc:
        return jsonify({"ok": False, "message": str(exc)}), 400

    result = compare_ollama_setup_models(settings)
    return jsonify(result), 200 if result.get("ok") else 503


@server_setup_web.post("/server-setup/test-ai-model")
def test_ai_model():
    return _test_ai_model_from_request()


@server_setup_web.post("/server-setup/test-ai-connection")
def test_ai_connection():
    return _test_ai_connection_from_request()


@server_setup_web.post("/server-setup/compare-ai-models")
def compare_ai_models():
    return _compare_ai_models_from_request()


def _pull_from_request():
    try:
        _require_setup_csrf()
        settings = load_settings()
        if not role_uses_ai(settings.deployment_mode):
            raise ServerSetupError(
                "Language-model actions are not available for the selected role."
            )
        ok, message = pull_ollama_model(settings)
    except (MtconnectDiscoveryError, ServerSetupError) as exc:
        ok, message = False, str(exc)
    flash(message, "success" if ok else "error")
    return redirect(_startup_step_url("runtime"))
