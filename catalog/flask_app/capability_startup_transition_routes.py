"""CFI-6 migration and capability-first startup transition."""

from __future__ import annotations

from collections.abc import Mapping

from flask import (
    Blueprint,
    Response,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from catalog.capabilities.benchmarking import BenchmarkingError
from catalog.capabilities.contributions import ContributionServiceError
from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.node.identity import NodeIdentityStateError

from .capability_contribution_routes import (
    _build_onboarding_with_contributions_view_model,
)
from .capability_onboarding_routes import (
    _payload,
    _safe_error_response,
    _secure_response,
    _validate_server_bound_request,
)
from .services.capability_config_migration_service import (
    persist_capability_config_from_setup,
)
from .services.capability_contribution_service import (
    get_capability_contribution_service,
)
from .services.capability_startup_transition_service import (
    get_capability_startup_transition_service,
)
from .services.startup_contribution_reconcile import (
    run_startup_contribution_reconcile,
)

capability_startup_transition_web = Blueprint(
    "capability_startup_transition_web",
    __name__,
)

_LEGACY_FALLBACK_SESSION_KEY = "capability_startup_legacy_fallback"
_ROOT_HANDOFF_SESSION_KEY = "capability_startup_root_handoff_complete"
_BASE_FIELDS = frozenset({"_csrf_token", "command_id"})
_SAFE_PREFIXES = ("/onboarding", "/federation", "/docs", "/static")
_LEGACY_CONTROL_PREFIXES = (
    "/server-setup/",
    "/status/mtconnect-",
)
_LEGACY_CONTROL_PATHS = frozenset(
    {
        "/rescan",
        "/startup/choose",
        "/status",
        "/status/recorder.json",
    }
)
_TRANSITION_ERRORS = (
    AuthenticationError,
    AuthorizationError,
    BenchmarkingError,
    ContributionServiceError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
    NodeIdentityStateError,
)


def _default_flags(*, degraded: bool = False) -> dict[str, object]:
    return {
        "workbench": False,
        "runtime": False,
        "recorder": False,
        "language_model": False,
        "compute": False,
        "storage": False,
        "completed": False,
        "needs_migration": False,
        "source": "degraded" if degraded else "unconfigured",
        "state": None,
        "degraded": degraded,
    }


@capability_startup_transition_web.app_context_processor
def inject_capability_startup_flags() -> dict[str, object]:
    try:
        flags = get_capability_startup_transition_service().capability_flags()
        flags["degraded"] = False
    except Exception as exc:  # noqa: BLE001 - navigation must fail closed
        current_app.logger.warning(
            "Capability startup flags unavailable (%s)",
            type(exc).__name__,
        )
        flags = _default_flags(degraded=True)
    return {"capability_startup_flags": flags}


def _validate_control_payload(payload: Mapping[str, object]) -> None:
    unexpected = set(payload) - _BASE_FIELDS
    if unexpected:
        raise AuthorizationError(
            "onboarding-context-override-forbidden",
            "startup transition context is bound by the server",
            min(unexpected),
        )


def _transition_message(exc: BaseException) -> str:
    messages = {
        "startup-transition-legacy-required": (
            "No supported completed legacy setup is available to migrate."
        ),
        "startup-transition-federation-required": (
            "Connect or create a federation before finishing setup."
        ),
        "startup-transition-reconnect-required": (
            "Repair the saved federation membership before migrating."
        ),
        "startup-transition-inspection-required": (
            "Inspect this device before finishing setup."
        ),
        "startup-transition-benchmarks-required": (
            "Review the current benchmark evidence before finishing setup."
        ),
        "startup-transition-contributions-required": (
            "Review every supported contribution before finishing setup."
        ),
        "startup-transition-recorder-source-required": (
            "Select an existing recorder data source before enabling recording."
        ),
        "startup-transition-capability-config-failed": (
            "Technical capability configuration could not be migrated safely."
        ),
        "candidate-selection-required": (
            "A federation was discovered. Choose and verify it before migrating."
        ),
        "onboarding-verification-failed": (
            "The federation verification code did not match."
        ),
        "startup-transition-replacement-forbidden": (
            "The saved device and federation binding cannot be replaced."
        ),
        "malformed-startup-transition": (
            "Saved capability-first startup state is damaged and was not trusted."
        ),
        "unsupported-startup-transition-schema": (
            "Saved capability-first startup state uses an unsupported schema."
        ),
    }
    return messages.get(
        getattr(exc, "code", ""),
        "The startup transition could not be completed safely.",
    )


def _degraded_view_model(view_model: dict[str, object]) -> dict[str, object]:
    view_model["overall_state"] = "repair"
    view_model["overall_state_label"] = "Startup state needs repair"
    migration = view_model.get("migration")
    if not isinstance(migration, dict):
        migration = {}
        view_model["migration"] = migration
    migration.update(
        {
            "persisted": False,
            "can_migrate": False,
            "source_kind": "degraded",
        }
    )
    finish = view_model.get("finish")
    if isinstance(finish, dict):
        finish.update(
            {
                "title": "Capability-first startup needs repair",
                "message": (
                    "The saved transition state was not trusted. Use the guided "
                    "onboarding repair flow or the explicit legacy fallback."
                ),
                "state": "degraded",
                "state_label": "Repair required",
                "transition_action": None,
                "next_action": None,
            }
        )
    actions = view_model.get("actions")
    if isinstance(actions, dict):
        actions["legacy_setup_url"] = "/startup?legacy=1&edit=1"
    return view_model


def _reconcile_contributions_once() -> None:
    def operation() -> tuple[object, ...]:
        service = get_capability_contribution_service()
        if not service.has_persisted_intents():
            return ()
        return tuple(service.reconcile())

    attempted, _result, error = run_startup_contribution_reconcile(
        current_app.extensions,
        operation,
    )
    if attempted and error is not None:
        current_app.logger.info(
            "Capability contribution startup reconcile unavailable (%s)",
            type(error).__name__,
        )


def _build_view_model() -> dict[str, object]:
    _reconcile_contributions_once()
    view_model, _benchmark_complete = (
        _build_onboarding_with_contributions_view_model()
    )
    requested_step = request.args.get("step")
    try:
        return get_capability_startup_transition_service().apply_to_onboarding_view_model(
            view_model,
            requested_step=requested_step,
        )
    except _TRANSITION_ERRORS as exc:
        current_app.logger.warning(
            "Capability startup transition unavailable (%s)",
            type(exc).__name__,
        )
        return _degraded_view_model(view_model)
    except Exception as exc:  # noqa: BLE001 - render must fail closed
        current_app.logger.warning(
            "Capability startup transition unavailable (%s)",
            type(exc).__name__,
        )
        return _degraded_view_model(view_model)


def _render_onboarding() -> Response:
    return _secure_response(
        render_template("onboarding.html", onboarding=_build_view_model())
    )


def _is_legacy_control_path() -> bool:
    return request.path in _LEGACY_CONTROL_PATHS or request.path.startswith(
        _LEGACY_CONTROL_PREFIXES
    )


@capability_startup_transition_web.before_app_request
def _startup_transition_gate_and_dispatch() -> Response | None:
    """Require onboarding until complete without replacing workbench navigation."""

    endpoint = request.endpoint or ""
    if endpoint.startswith("static"):
        return None

    if request.blueprint == capability_startup_transition_web.name:
        view = current_app.view_functions.get(endpoint)
        if view is None:
            return None
        return current_app.ensure_sync(view)(**(request.view_args or {}))

    if request.path.startswith(_SAFE_PREFIXES):
        return None

    # Compatibility controls remain callable and keep their existing local
    # authorization checks. CFI-6 changes setup authority, not the established
    # workbench entry point or its deep links.
    if _is_legacy_control_path():
        return None

    if request.path == "/startup" and request.args.get("legacy") == "1":
        session[_LEGACY_FALLBACK_SESSION_KEY] = True
        return None

    try:
        flags = get_capability_startup_transition_service().capability_flags()
    except Exception as exc:  # noqa: BLE001 - startup must fail closed
        current_app.logger.warning(
            "Capability startup gate unavailable (%s)",
            type(exc).__name__,
        )
        return redirect(
            url_for("capability_startup_transition_web.onboarding", repair="1")
        )

    if request.path == "/startup":
        if flags["completed"]:
            return redirect(url_for("web.overview"))
        if flags["needs_migration"]:
            return redirect(
                url_for(
                    "capability_startup_transition_web.onboarding",
                    step="finish",
                )
            )
        return redirect(url_for("capability_startup_transition_web.onboarding"))

    if flags["completed"]:
        if request.path == "/" and not session.get(_ROOT_HANDOFF_SESSION_KEY):
            session[_ROOT_HANDOFF_SESSION_KEY] = True
            return redirect(url_for("federation_web.overview"))
        return None

    if request.path == "/":
        if flags["needs_migration"]:
            return redirect(
                url_for(
                    "capability_startup_transition_web.onboarding",
                    step="finish",
                )
            )
        return redirect(url_for("capability_startup_transition_web.onboarding"))

    # Existing deep links retain their own compatibility gates. Their redirects
    # eventually reach /startup, where CFI-6 selects onboarding unless the
    # operator explicitly requested the legacy fallback.
    return None


@capability_startup_transition_web.get("/onboarding", strict_slashes=False)
def onboarding() -> Response:
    return _render_onboarding()


def _run_transition(*, migration: bool) -> Response:
    payload = _payload()
    try:
        _validate_control_payload(payload)
        command_id = _validate_server_bound_request(
            payload,
            require_command=True,
        )
        assert command_id is not None
        service = get_capability_startup_transition_service()
        if migration:
            legacy_loader = getattr(service, "legacy_settings", None)
            legacy_settings = legacy_loader() if callable(legacy_loader) else None
            if legacy_settings is not None:
                persist_capability_config_from_setup(legacy_settings)
            service.migrate_legacy(
                request_id=f"flask-startup-migration-{command_id}"
            )
            message = "Existing setup migrated to capability-first startup."
        else:
            service.complete_current()
            message = "Capability-first setup completed."
    except AuthorizationError as exc:
        return _safe_error_response(exc.code, 403)
    except _TRANSITION_ERRORS as exc:
        flash(_transition_message(exc), "error")
        return redirect(
            url_for(
                "capability_startup_transition_web.onboarding",
                step="finish",
            ),
            code=303,
        )
    except Exception as exc:  # noqa: BLE001 - never expose persisted internals
        current_app.logger.warning(
            "Capability startup transition failed safely (%s)",
            type(exc).__name__,
        )
        flash("The startup transition could not be completed safely.", "error")
        return redirect(
            url_for(
                "capability_startup_transition_web.onboarding",
                step="finish",
            ),
            code=303,
        )

    session.pop(_LEGACY_FALLBACK_SESSION_KEY, None)
    flash(message, "success")
    if migration:
        destination = url_for(
            "capability_startup_transition_web.onboarding",
            step="finish",
        )
    else:
        destination = url_for("federation_web.overview")
    return redirect(destination, code=303)


@capability_startup_transition_web.post("/onboarding/migrate")
def migrate_legacy() -> Response:
    return _run_transition(migration=True)


@capability_startup_transition_web.post("/onboarding/finish")
def finish_onboarding() -> Response:
    return _run_transition(migration=False)


__all__ = ["capability_startup_transition_web"]
