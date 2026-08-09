"""Capability-first replacements for retained role-reading Flask entrypoints.

The large legacy ``routes`` module still contains compatibility implementations
that read ``ServerSetupSettings``.  This module replaces the supported product
entrypoints at application-composition time so live requests are driven by
capability configuration and contribution authority instead.  The old functions
can then be deleted separately without combining semantic migration and bulk
code removal.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from flask import (
    Flask,
    Response,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from catalog.common.telemetry_cache import cached_cache_status
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionDesiredState,
)
from catalog.orchestrator.pipeline import get_runtime_manager

from .services.capability_config_service import (
    CapabilityConfig,
    CapabilityConfigError,
    default_capability_config,
    load_capability_config,
)
from .services.capability_contribution_service import (
    get_capability_contribution_service,
)
from .services.capability_startup_transition_service import (
    get_capability_startup_transition_service,
)
from .services.recorder_control_service import get_recorder_control_service
from .services.server_setup_service import (
    AI_MODEL_CHOICES,
    AI_PROVIDER_MODES,
    DEPLOYMENT_MODES,
    ServerSetupError,
    load_settings,
)
from .services.operator_scope_service import get_operator_scope_service

_LEGACY_APP_MODULE = "catalog.flask_app.app"
_LEGACY_ROUTES_MODULE = "catalog.flask_app.routes"
_REPLACED_ENDPOINTS = (
    "web.status",
    "web.recorder_status_snapshot",
    "web.startup",
)
_ALLOWED_RUNTIME_CHOICE_ENDPOINTS = frozenset(
    {
        "web.startup",
        "web.choose_startup_mode",
        "web.status",
        "web.recorder_status_snapshot",
        "web.rescan",
        "web.guide",
        "server_setup_web.scan_mtconnect_network",
        "server_setup_web.save_discovered_mtconnect_sources",
        "ai_web.ai_page",
        "ai_web.ai_ask",
    }
)
_SAFE_PREFIXES = ("/onboarding", "/federation", "/docs", "/static")


def _capability_flags() -> dict[str, object]:
    try:
        return get_capability_startup_transition_service().capability_flags()
    except Exception:  # noqa: BLE001 - product gates fail closed
        return {
            "completed": False,
            "runtime": False,
            "recorder": False,
            "language_model": False,
            "degraded": True,
        }


def _load_product_config() -> tuple[CapabilityConfig, str]:
    try:
        return load_capability_config(), ""
    except (CapabilityConfigError, OSError, TypeError, ValueError):
        return default_capability_config(), (
            "Capability configuration could not be read safely."
        )


def _active_recorder_contribution() -> bool:
    """Project current CF4/CFI-5 authority without consulting legacy roles."""

    flags = _capability_flags()
    if not bool(flags.get("completed")):
        return False
    try:
        service = get_capability_contribution_service()
        recorder_candidates = {
            candidate.candidate_id
            for candidate in service.recommend(require_benchmark_review=False)
            if candidate.capability_type == "recorder"
        }
        return any(
            intent.candidate_id in recorder_candidates
            and intent.desired_state is ContributionDesiredState.ENABLED
            and intent.activation_state is ContributionActivationState.ACTIVE
            for intent in service.intents()
        )
    except Exception:  # noqa: BLE001 - status/control projection fails closed
        return False


def _telemetry_cache_status_model() -> dict[str, object]:
    status = cached_cache_status(Path("data"))
    state = "fresh" if status.fresh else ("stale" if status.exists else "missing")
    return {
        "state": state,
        "exists": status.exists,
        "fresh": status.fresh,
        "cache_path": str(status.cache_path),
        "source_file_count": status.source_file_count,
        "parquet_file_count": status.parquet_file_count,
        "cached_row_count": status.manifest_row_count,
        "manifest_source_file_count": status.manifest_source_file_count,
        "last_rebuild_time": status.manifest_generated_at,
    }


def capability_startup_mode_gate() -> Response | None:
    """Retain the old runtime-choice guard without device-role decisions."""

    endpoint = request.endpoint or ""
    if endpoint.startswith("static") or request.path.startswith(_SAFE_PREFIXES):
        return None
    if endpoint in _ALLOWED_RUNTIME_CHOICE_ENDPOINTS:
        return None

    flags = _capability_flags()
    if bool(flags.get("completed")):
        return None
    if get_runtime_manager().requires_startup_choice():
        next_path = request.full_path if request.query_string else request.path
        return redirect(url_for("web.startup", next=next_path))
    return None


def _status_template_adapter(*, recorder_visible: bool) -> object:
    """Temporary shape adapter for status.html; never used as authority.

    The template still names the old deployment-mode field.  Feed it a synthetic
    non-exclusive value so Diagnostics is always available while recorder UI is
    visible only when recorder configuration/capability state exists.  A later
    deletion-only cleanup can remove these template variable names.
    """

    return SimpleNamespace(
        deployment_mode="full-server" if recorder_visible else "web-workbench"
    )


def status() -> str:
    config, _config_error = _load_product_config()
    recorder_active = _active_recorder_contribution()
    recorder_status = get_recorder_control_service().status(config)
    recorder_status["ready"] = bool(recorder_status.get("ready") and recorder_active)
    recorder_visible = bool(config.recorder_sources) or recorder_active

    catalog = current_app.config["ARTIFACT_CATALOG"]
    snap = catalog.cached_snapshot()
    runtime_state = get_runtime_manager().state_snapshot()
    operator_scope = get_operator_scope_service().get()
    internal_artifacts = [
        artifact for artifact in snap.artifacts if artifact.get("is_internal")
    ]
    phase_messages = {
        "runtime_not_started": "Webapp started. Runtime has not started yet.",
        "discovery_pending": "Webapp started. Background discovery is running.",
        "bootstrap_latest_day_playback_ready_analysis": (
            "Running playback-ready health and timeline analysis for latest day "
            "in the background."
        ),
        "historical_catch_up": "Historical catch-up is running one day at a time.",
        "polling_new_data": (
            "Historical processing is complete. Polling for newly arriving days."
        ),
        "failed": (
            "Background runtime encountered a failure. Check last failure details "
            "below."
        ),
    }
    current_phase = runtime_state.get(
        "current_processing_phase",
        "runtime_not_started",
    )
    return render_template(
        "status.html",
        snapshot=snap,
        scan_dirs=catalog.scan_dirs,
        runtime_state=runtime_state,
        internal_artifacts=internal_artifacts,
        phase_message=phase_messages.get(
            current_phase,
            "Runtime state is available below.",
        ),
        operator_scope=operator_scope,
        telemetry_cache_status=_telemetry_cache_status_model(),
        recorder_status=recorder_status,
        setup_settings=_status_template_adapter(
            recorder_visible=recorder_visible,
        ),
    )


def _no_store_json(payload: dict[str, Any], status_code: int = 200):
    response = jsonify(payload)
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response, status_code


def recorder_status_snapshot():
    flags = _capability_flags()
    if not bool(flags.get("completed")):
        return _no_store_json(
            {
                "schema": "msh.recorder.web_status.v1",
                "error": "setup_required",
                "message": (
                    "Complete capability-first onboarding before reading recorder "
                    "status."
                ),
            },
            409,
        )
    if not _active_recorder_contribution():
        return _no_store_json(
            {
                "schema": "msh.recorder.web_status.v1",
                "error": "recorder_not_enabled",
                "message": "The recorder contribution is not active on this device.",
            },
            409,
        )

    config, config_error = _load_product_config()
    if config_error:
        return _no_store_json(
            {
                "schema": "msh.recorder.web_status.v1",
                "error": "recorder_config_unavailable",
                "message": config_error,
            },
            409,
        )
    return _no_store_json(get_recorder_control_service().web_status(config))


def startup() -> str:
    """Render only the explicit read-only legacy compatibility notice."""

    next_path = request.args.get("next", "/")
    startup_state = get_runtime_manager().startup_decision_snapshot()
    return render_template(
        "startup.html",
        startup_state=startup_state,
        next_path=next_path,
    )


def inject_capability_product_context() -> dict[str, object]:
    """Use capability config for product context; read legacy only on /startup."""

    config, config_error = _load_product_config()
    legacy_settings = None
    legacy_error = ""
    if request.path == "/startup":
        try:
            legacy_settings = load_settings()
        except (OSError, ServerSetupError, TypeError, ValueError) as exc:
            legacy_error = str(exc)

    return {
        "server_setup_settings": legacy_settings,
        "server_setup_error": legacy_error or config_error,
        "server_setup_modes": DEPLOYMENT_MODES,
        "server_setup_ai_choices": AI_MODEL_CHOICES,
        "server_setup_ai_provider_modes": AI_PROVIDER_MODES,
        "server_setup_ollama_status": None,
        "server_setup_recorder_status": get_recorder_control_service().status(config),
    }


def _is_named(function: object, *, module: str, name: str) -> bool:
    return (
        getattr(function, "__module__", "") == module
        and getattr(function, "__name__", "") == name
    )


def install_capability_product_routes(app: Flask) -> None:
    """Replace live role-reading handlers after the legacy blueprint is composed."""

    before = list(app.before_request_funcs.get(None, ()))
    app.before_request_funcs[None] = [
        function
        for function in before
        if not _is_named(
            function,
            module=_LEGACY_ROUTES_MODULE,
            name="startup_mode_gate",
        )
    ]
    app.before_request(capability_startup_mode_gate)

    processors = list(app.template_context_processors.get(None, ()))
    app.template_context_processors[None] = [
        function
        for function in processors
        if not _is_named(
            function,
            module=_LEGACY_APP_MODULE,
            name="inject_server_setup",
        )
    ]
    app.context_processor(inject_capability_product_context)

    replacements = {
        "web.status": status,
        "web.recorder_status_snapshot": recorder_status_snapshot,
        "web.startup": startup,
    }
    missing = [endpoint for endpoint in _REPLACED_ENDPOINTS if endpoint not in app.view_functions]
    if missing:
        raise RuntimeError(
            "Capability product routes require registered legacy endpoints: "
            + ", ".join(missing)
        )
    app.view_functions.update(replacements)


__all__ = [
    "capability_startup_mode_gate",
    "inject_capability_product_context",
    "install_capability_product_routes",
    "recorder_status_snapshot",
    "startup",
    "status",
]
