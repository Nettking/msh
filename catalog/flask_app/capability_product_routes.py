"""Capability-first Flask product entrypoints."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from flask import (
    Flask,
    abort,
    current_app,
    jsonify,
    redirect,
    render_template,
    url_for,
)

from catalog.common.telemetry_cache import cached_cache_status
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionDesiredState,
)
from catalog.orchestrator.pipeline import get_runtime_manager

from . import capability_startup_transition_routes as transition_routes
from .services.capability_config_service import (
    CapabilityConfig,
    CapabilityConfigError,
    default_capability_config,
    load_capability_config,
)
from .services.capability_contribution_service import (
    get_capability_contribution_service,
)
from .services.operator_scope_service import get_operator_scope_service
from .services.recorder_control_service import get_recorder_control_service


def get_capability_startup_transition_service():
    return transition_routes.get_capability_startup_transition_service()


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
    """Project current contribution authority without consulting legacy roles."""

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
        recorder_visible=recorder_visible,
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
                "schema": "fcp.recorder.web_status.v1",
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
                "schema": "fcp.recorder.web_status.v1",
                "error": "recorder_not_enabled",
                "message": "The recorder contribution is not active on this device.",
            },
            409,
        )

    config, config_error = _load_product_config()
    if config_error:
        return _no_store_json(
            {
                "schema": "fcp.recorder.web_status.v1",
                "error": "recorder_config_unavailable",
                "message": config_error,
            },
            409,
        )
    return _no_store_json(get_recorder_control_service().web_status(config))


def startup():
    """Backward URL handoff only; the compatibility startup UI no longer exists."""

    return redirect(url_for("capability_startup_transition_web.onboarding"), code=303)


def retired_startup_choice():
    """Make the former runtime-choice bridge unreachable in the supported product."""

    abort(404)


def inject_capability_product_context() -> dict[str, object]:
    config, config_error = _load_product_config()
    return {
        "capability_config_error": config_error,
        "server_setup_recorder_status": get_recorder_control_service().status(config),
    }


def _bind_product_route(
    app: Flask,
    *,
    rule: str,
    endpoint: str,
    view_func: Callable[..., object],
) -> None:
    if endpoint in app.view_functions:
        app.view_functions[endpoint] = view_func
        return
    app.add_url_rule(rule, endpoint=endpoint, view_func=view_func, methods=["GET"])


def install_capability_product_routes(app: Flask) -> None:
    app.context_processor(inject_capability_product_context)
    _bind_product_route(
        app,
        rule="/status",
        endpoint="web.status",
        view_func=status,
    )
    _bind_product_route(
        app,
        rule="/status/recorder.json",
        endpoint="web.recorder_status_snapshot",
        view_func=recorder_status_snapshot,
    )
    _bind_product_route(
        app,
        rule="/startup",
        endpoint="web.startup",
        view_func=startup,
    )
    if "web.choose_startup_mode" in app.view_functions:
        app.view_functions["web.choose_startup_mode"] = retired_startup_choice


__all__ = [
    "get_capability_startup_transition_service",
    "inject_capability_product_context",
    "install_capability_product_routes",
    "recorder_status_snapshot",
    "retired_startup_choice",
    "startup",
    "status",
]
