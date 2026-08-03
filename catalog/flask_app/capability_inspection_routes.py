"""CFI-3 Flask composition for read-only device inspection."""

from __future__ import annotations

from flask import (
    Blueprint,
    Response,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    url_for,
)

from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.node.identity import NodeIdentityStateError

from .capability_onboarding_routes import (
    _ALLOWED_STEPS,
    _command_id,
    _csrf_token,
    _payload,
    _safe_error_response,
    _secure_response,
    _validate_server_bound_request,
)
from .services.capability_inspection_service import (
    CapabilityInspectionService,
    get_capability_inspection_service,
)
from .services.capability_onboarding_service import (
    get_capability_onboarding_service,
)

capability_inspection_web = Blueprint("capability_inspection_web", __name__)


def _inspection_message(exc: BaseException) -> str:
    code = getattr(exc, "code", "")
    messages = {
        "inspection-federation-required": (
            "Connect this device to a trusted federation before inspecting it."
        ),
        "inspection-device-mismatch": (
            "Saved inspection evidence belongs to a different device identity."
        ),
        "inspection-revision-conflict": (
            "Another inspection completed first. Reload the page before trying again."
        ),
        "invalid-inspection-adapter-composition": (
            "Configured inspection adapters could not be loaded safely."
        ),
        "malformed-inspection-snapshot": (
            "Saved inspection evidence is damaged and was not trusted."
        ),
        "malformed-inspection-state": (
            "Saved inspection evidence could not be read safely."
        ),
    }
    return messages.get(
        code,
        "Device inspection could not be completed safely.",
    )


def _initialize_snapshot_schema(service: CapabilityInspectionService) -> None:
    """Create the additive table before the store acquires its write lock.

    The existing CFI-2 database may still use SQLite's default journal mode.
    Journal-mode negotiation is therefore performed outside `BEGIN IMMEDIATE`;
    the store's transactional save then sees an already initialized WAL schema.
    """

    store = service.store
    store.database.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    connection = store._connect()
    try:
        store._create_table(connection)
        connection.commit()
    finally:
        connection.close()


def _render_onboarding_with_inspection() -> Response:
    onboarding_service = get_capability_onboarding_service()
    inspection_service = get_capability_inspection_service()
    credentials = None
    saved_binding = None
    connected_binding = None
    candidates = ()
    repair_reason = None
    inspection_snapshot = None
    inspection_reason = None

    try:
        credentials = onboarding_service.identity_or_none()
        saved_binding = onboarding_service.binding_or_none()
        if credentials is not None and saved_binding is not None:
            try:
                context = onboarding_service.authorized_context()
            except (
                AuthenticationError,
                AuthorizationError,
                FederationOperationError,
                FederationValidationError,
                ProtocolCompatibilityError,
                NodeIdentityStateError,
            ) as exc:
                repair_reason = getattr(exc, "code", "reconnect-unavailable")
            else:
                connected_binding = None if context is None else context.binding
        if credentials is not None and connected_binding is None:
            try:
                candidates = onboarding_service.discover()
            except (
                AuthenticationError,
                AuthorizationError,
                FederationOperationError,
                FederationValidationError,
                ProtocolCompatibilityError,
                NodeIdentityStateError,
            ) as exc:
                current_app.logger.warning(
                    "Capability onboarding discovery unavailable (%s)",
                    type(exc).__name__,
                )
                repair_reason = getattr(exc, "code", "discovery-unavailable")
    except Exception as exc:  # noqa: BLE001 - render must fail closed
        current_app.logger.warning(
            "Capability onboarding state unavailable (%s)",
            type(exc).__name__,
        )
        repair_reason = "onboarding-state-unavailable"

    if credentials is not None:
        try:
            inspection_snapshot = inspection_service.load()
        except (
            FederationOperationError,
            FederationValidationError,
            NodeIdentityStateError,
        ) as exc:
            current_app.logger.warning(
                "Capability inspection state unavailable (%s)",
                type(exc).__name__,
            )
            inspection_reason = getattr(exc, "code", "inspection-state-unavailable")
        except Exception as exc:  # noqa: BLE001 - render must fail closed
            current_app.logger.warning(
                "Capability inspection state unavailable (%s)",
                type(exc).__name__,
            )
            inspection_reason = "inspection-state-unavailable"

    preview = onboarding_service.legacy_preview()
    requested_step = request.args.get("step")
    if requested_step not in _ALLOWED_STEPS:
        requested_step = None
    view_model = onboarding_service.build_view_model(
        credentials=credentials,
        saved_binding=saved_binding,
        connected_binding=connected_binding,
        candidates=tuple(candidates),
        preview=preview,
        csrf_token=_csrf_token(),
        command_id=_command_id(),
        requested_step=requested_step,
        repair_reason=repair_reason,
    )
    inspection_service.apply_to_onboarding_view_model(
        view_model,
        snapshot=inspection_snapshot,
        connected=connected_binding is not None,
        requested_step=requested_step,
        error_reason=inspection_reason,
    )
    return _secure_response(
        render_template("onboarding.html", onboarding=view_model)
    )


@capability_inspection_web.before_app_request
def _dispatch_before_legacy_setup_gate() -> Response | None:
    """Dispatch the CFI-3 routes before the unchanged role-first gate."""

    if request.blueprint != capability_inspection_web.name:
        return None
    endpoint = request.endpoint or ""
    view = current_app.view_functions.get(endpoint)
    if view is None:
        return None
    return current_app.ensure_sync(view)(**(request.view_args or {}))


@capability_inspection_web.get("/onboarding", strict_slashes=False)
def onboarding() -> Response:
    return _render_onboarding_with_inspection()


@capability_inspection_web.post("/onboarding/inspect")
def inspect_device() -> Response:
    payload = _payload()
    try:
        _validate_server_bound_request(payload, require_command=False)
        service = get_capability_inspection_service()
        if service.onboarding_service.authorized_context() is not None:
            _initialize_snapshot_schema(service)
        snapshot = service.run()
    except AuthorizationError as exc:
        return _safe_error_response(exc.code, 403)
    except FederationOperationError as exc:
        if (
            exc.code == "inspection-federation-required"
            and get_capability_onboarding_service().identity_or_none() is not None
        ):
            return _secure_response(
                (
                    "This step remains blocked until this device has a trusted "
                    "federation connection."
                ),
                409,
            )
        flash(_inspection_message(exc), "error")
    except (
        AuthenticationError,
        FederationValidationError,
        ProtocolCompatibilityError,
        NodeIdentityStateError,
    ) as exc:
        flash(_inspection_message(exc), "error")
    except Exception as exc:  # noqa: BLE001 - never expose adapter internals
        current_app.logger.warning(
            "Capability inspection failed safely (%s)",
            type(exc).__name__,
        )
        flash("Device inspection could not be completed safely.", "error")
    else:
        flash(
            f"Device inspection revision {snapshot.revision} is ready.",
            "success",
        )
    return redirect(
        url_for("capability_inspection_web.onboarding", step="inspect"),
        code=303,
    )


__all__ = ["capability_inspection_web"]
