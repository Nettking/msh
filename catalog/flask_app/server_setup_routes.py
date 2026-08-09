from __future__ import annotations

import hmac
import secrets

from flask import Blueprint, flash, jsonify, redirect, request, session, url_for

# Temporary import-only compatibility hook for CFI-6 tests and integrations that
# monkeypatch this historical module symbol. No route behavior depends on it.
from catalog.orchestrator.pipeline import get_runtime_manager  # noqa: F401

from .services.ai_model_benchmark_service import compare_ollama_setup_models
from .services.capability_config_service import (
    CapabilityConfigError,
    compatibility_settings,
    load_capability_config,
    mirror_legacy_capability_config,
    save_capability_config,
    update_language_model_config,
    update_recorder_config,
)
from .services.capability_startup_transition_service import (
    get_capability_startup_transition_service,
)
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
    load_settings,
    ollama_status,
    pull_ollama_model,
)

server_setup_web = Blueprint("server_setup_web", __name__)

_MTCONNECT_CSRF_SESSION_KEY = "mtconnect_discovery_csrf_token"


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
    """Reject cross-site or tokenless local capability-configuration requests."""

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
            "The configuration request was blocked because it came from "
            "another site. Reload MSH and try again."
        )
    expected = str(session.get(_MTCONNECT_CSRF_SESSION_KEY) or "")
    supplied = str(request.form.get("_csrf_token") or "")
    if not expected or not supplied or not hmac.compare_digest(expected, supplied):
        raise MtconnectDiscoveryError(
            "The configuration form expired. Reload MSH and try again."
        )


def _require_mtconnect_csrf() -> None:
    _require_setup_csrf(local_only=True)


def _wants_json() -> bool:
    return (
        request.accept_mimetypes["application/json"]
        > request.accept_mimetypes["text/html"]
    )


def _capability_flags() -> dict[str, object]:
    try:
        return get_capability_startup_transition_service().capability_flags()
    except Exception:  # noqa: BLE001 - operational controls fail closed
        return {
            "completed": False,
            "needs_migration": False,
            "recorder": False,
            "language_model": False,
        }


def _recorder_authorized() -> bool:
    flags = _capability_flags()
    if bool(flags.get("completed")):
        return bool(flags.get("recorder"))
    if not bool(flags.get("needs_migration")):
        return False
    try:
        settings = load_settings()
    except (OSError, ServerSetupError, TypeError, ValueError):
        return False
    return settings.deployment_mode in {"full-server", "recorder-only"}


def _persist_capability_config(config) -> None:
    save_capability_config(config)
    # Temporary one-way mirror for old runtime readers. This function never
    # changes deployment_mode, configured/user_setup_complete, or ai_enabled.
    mirror_legacy_capability_config(config)


def _settings_for_ai_form():
    config = update_language_model_config(load_capability_config(), request.form)
    # Model tests are configuration probes, not contribution activation. The
    # synthetic enabled flag only satisfies retained Ollama helper signatures.
    return compatibility_settings(
        config,
        capability="language-model",
        enabled=True,
    )


def _selected_ai_model_from_request(settings) -> str:
    profile = str(
        request.form.get("ai_profile")
        or settings.ai_profile
        or "laptop-standard"
    ).strip()
    if profile in AI_MODEL_CHOICES:
        return AI_MODEL_CHOICES[profile]["model"]
    return str(request.form.get("model") or settings.ai_model or "").strip()


def _legacy_shape_for_ai_page(result: dict, settings) -> dict:
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
        recommended_model or _selected_ai_model_from_request(settings),
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


@server_setup_web.post("/capabilities/config/language-model")
def save_language_model_capability_config():
    try:
        _require_setup_csrf()
        config = update_language_model_config(
            load_capability_config(),
            request.form,
        )
        _persist_capability_config(config)
    except (MtconnectDiscoveryError, CapabilityConfigError, ServerSetupError) as exc:
        if _wants_json():
            return jsonify({"ok": False, "message": str(exc)}), 400
        flash(str(exc), "error")
        return redirect(url_for("federation_web.overview"), code=303)

    if _wants_json():
        return jsonify(
            {
                "ok": True,
                "provider_mode": config.ai_provider_mode,
                "provider_name": config.ai_provider_name,
                "model": config.ai_model,
            }
        )
    flash("Language-model configuration saved.", "success")
    return redirect(url_for("federation_web.overview"), code=303)


@server_setup_web.post("/capabilities/config/recorder")
def save_recorder_capability_config():
    try:
        _require_setup_csrf(local_only=True)
        config = update_recorder_config(
            load_capability_config(),
            recorder_sources=str(request.form.get("recorder_sources") or ""),
            recorder_poll_interval=str(
                request.form.get("recorder_poll_interval") or "0.2"
            ),
            recorder_include_condition=(
                request.form.get("recorder_include_condition") == "on"
            ),
        )
        _persist_capability_config(config)
    except (MtconnectDiscoveryError, CapabilityConfigError, ServerSetupError) as exc:
        if _wants_json():
            return jsonify({"ok": False, "message": str(exc)}), 400
        flash(str(exc), "error")
        return redirect(url_for("web.status") + "#recorder-status", code=303)

    if _wants_json():
        return jsonify({"ok": True, "recorder_sources": config.recorder_sources})
    flash("Recorder configuration saved.", "success")
    return redirect(url_for("web.status") + "#recorder-status", code=303)


@server_setup_web.post("/server-setup/test-ai-model")
def test_ai_model():
    try:
        _require_setup_csrf()
        settings = _settings_for_ai_form()
    except (MtconnectDiscoveryError, CapabilityConfigError, ServerSetupError) as exc:
        return jsonify({"ok": False, "message": str(exc)}), 400

    result = _legacy_shape_for_ai_page(
        compare_ollama_setup_models(settings),
        settings,
    )
    return jsonify(result), 200 if result.get("ok") else 503


@server_setup_web.post("/server-setup/test-ai-connection")
def test_ai_connection():
    try:
        _require_setup_csrf()
        settings = _settings_for_ai_form()
    except (MtconnectDiscoveryError, CapabilityConfigError, ServerSetupError) as exc:
        return jsonify({"ok": False, "message": str(exc)}), 400

    status = ollama_status(settings, timeout_seconds=3.0)
    status["ok"] = bool(status.get("running"))
    return jsonify(status), 200 if status["ok"] else 503


@server_setup_web.post("/server-setup/compare-ai-models")
def compare_ai_models():
    try:
        _require_setup_csrf()
        settings = _settings_for_ai_form()
    except (MtconnectDiscoveryError, CapabilityConfigError, ServerSetupError) as exc:
        return jsonify({"ok": False, "message": str(exc)}), 400

    result = compare_ollama_setup_models(settings)
    return jsonify(result), 200 if result.get("ok") else 503


@server_setup_web.post("/server-setup/pull-model")
def pull_model():
    try:
        _require_setup_csrf()
        config = load_capability_config()
        settings = compatibility_settings(
            config,
            capability="language-model",
            enabled=True,
        )
        ok, message = pull_ollama_model(settings)
    except (MtconnectDiscoveryError, CapabilityConfigError, ServerSetupError) as exc:
        ok, message = False, str(exc)
    flash(message, "success" if ok else "error")
    return redirect(url_for("federation_web.overview"), code=303)


def _set_recording_from_request(enabled: bool):
    destination = request.form.get("next") or (url_for("web.status") + "#recorder-status")
    if not destination.startswith("/") or destination.startswith("//"):
        destination = url_for("web.status") + "#recorder-status"

    try:
        _require_setup_csrf()
        authorized = _recorder_authorized()
        if enabled and not authorized:
            raise RecorderControlError(
                "Recorder capability is not enabled by capability-first intent."
            )
        config = load_capability_config()
        settings = compatibility_settings(
            config,
            capability="recorder",
            enabled=authorized,
        )
        ok, message = get_recorder_control_service().set_enabled(enabled, settings)
    except (
        MtconnectDiscoveryError,
        CapabilityConfigError,
        ServerSetupError,
        RecorderControlError,
    ) as exc:
        flash(str(exc), "error")
        return redirect(destination, code=303)

    flash(message, "success" if ok else "error")
    return redirect(destination, code=303)


@server_setup_web.post("/server-setup/recording/start")
def start_recording():
    return _set_recording_from_request(True)


@server_setup_web.post("/server-setup/recording/stop")
def stop_recording():
    return _set_recording_from_request(False)


@server_setup_web.post("/status/mtconnect-scan")
def scan_mtconnect_network():
    wants_json = _wants_json()
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
        return redirect(url_for("web.status") + "#recorder-status", code=303)

    message = (
        "Network scan complete: "
        f"{result.get('machines_found', 0)} machine(s) from "
        f"{result.get('agents_found', 0)} MTConnect Agent(s)."
    )
    if wants_json:
        return jsonify({"ok": True, "scan": result, "message": message})
    flash(message, "success")
    return redirect(url_for("web.status") + "#recorder-status", code=303)


@server_setup_web.post("/status/mtconnect-sources")
def save_discovered_mtconnect_sources():
    try:
        _require_mtconnect_csrf()
        config = load_capability_config()
        adapter = compatibility_settings(
            config,
            capability="recorder",
            enabled=True,
        )
        merged = get_mtconnect_discovery_service().merge_selected_results(
            adapter,
            request.form.getlist("source_id"),
        )
        config = update_recorder_config(
            config,
            recorder_sources=merged.recorder_sources,
        )
        _persist_capability_config(config)
    except (
        MtconnectDiscoveryError,
        CapabilityConfigError,
        ServerSetupError,
    ) as exc:
        flash(str(exc), "error")
    else:
        flash(
            "Selected MTConnect machines were saved as recorder sources. "
            "Enable recorder contribution before starting capture.",
            "success",
        )
    return redirect(url_for("web.status") + "#recorder-status", code=303)
