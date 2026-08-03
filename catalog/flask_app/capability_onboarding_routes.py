"""CFI-2 Flask composition for identity and federation onboarding."""

from __future__ import annotations

import secrets
from typing import Any

from flask import (
    Blueprint,
    Response,
    current_app,
    flash,
    make_response,
    redirect,
    render_template,
    request,
    session,
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

from .services.capability_onboarding_service import (
    get_capability_onboarding_service,
)

capability_onboarding_web = Blueprint("capability_onboarding_web", __name__)

_CSRF_SESSION_KEY = "capability_onboarding_csrf_token"
_COMMAND_SESSION_KEY = "capability_onboarding_command_id"
_STARTUP_CHECK_EXTENSION_KEY = "capability_onboarding_startup_checked"
_FORBIDDEN_CONTEXT_FIELDS = frozenset(
    {
        "actor_node_id",
        "device_id",
        "enrollment_token",
        "federation_id",
        "internal_session_id",
        "invitation_token",
        "session_id",
        "target_node_id",
    }
)
_ALLOWED_STEPS = frozenset(
    {"identity", "federation", "inspect", "benchmarks", "contributions", "finish"}
)


def _csrf_token() -> str:
    value = session.get(_CSRF_SESSION_KEY)
    if not isinstance(value, str) or len(value) < 32:
        value = secrets.token_urlsafe(32)
        session[_CSRF_SESSION_KEY] = value
    return value


def _command_id() -> str:
    value = session.get(_COMMAND_SESSION_KEY)
    if not isinstance(value, str) or len(value) < 24:
        value = f"command-{secrets.token_urlsafe(24)}"
        session[_COMMAND_SESSION_KEY] = value
    return value


def _secure_response(value: Any, status: int | None = None) -> Response:
    response = make_response(value, status) if status is not None else make_response(value)
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "same-origin"
    return response


def _safe_error_response(code: str, status: int) -> Response:
    messages = {
        "onboarding-csrf-rejected": (
            "The onboarding form expired. Reload the page and try again."
        ),
        "onboarding-context-override-forbidden": (
            "The onboarding request attempted to replace server-bound context."
        ),
    }
    return _secure_response(
        messages.get(code, "The onboarding request was rejected safely."),
        status,
    )


def _payload() -> dict[str, Any]:
    if request.is_json:
        value = request.get_json(silent=True)
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-onboarding-request",
                "body",
                "must be a JSON object",
            )
        return value
    return request.form.to_dict(flat=True)


def _validate_server_bound_request(
    payload: dict[str, Any],
    *,
    require_command: bool,
) -> str | None:
    if _FORBIDDEN_CONTEXT_FIELDS.intersection(payload):
        raise AuthorizationError(
            "onboarding-context-override-forbidden",
            "actor, device, and federation context are bound by the server",
            "body",
        )
    supplied = payload.get("_csrf_token")
    expected = session.get(_CSRF_SESSION_KEY)
    if (
        not isinstance(supplied, str)
        or not isinstance(expected, str)
        or not secrets.compare_digest(supplied, expected)
    ):
        raise AuthorizationError(
            "onboarding-csrf-rejected",
            "the request did not carry the current onboarding CSRF token",
            "_csrf_token",
        )
    if not require_command:
        return None
    supplied_command = payload.get("command_id")
    expected_command = session.get(_COMMAND_SESSION_KEY)
    if (
        not isinstance(supplied_command, str)
        or not isinstance(expected_command, str)
        or not secrets.compare_digest(supplied_command, expected_command)
    ):
        raise AuthorizationError(
            "onboarding-command-rejected",
            "the request did not carry the current server-issued command ID",
            "command_id",
        )
    return supplied_command


def _safe_message(exc: BaseException) -> str:
    code = getattr(exc, "code", "")
    messages = {
        "candidate-selection-required": (
            "Choose one discovered federation before continuing."
        ),
        "onboarding-verification-failed": (
            "The verification code did not match. Compare it again on a trusted device."
        ),
        "unknown-discovery-candidate": (
            "That discovery result expired. Scan again before joining."
        ),
        "onboarding-discovery-unavailable": (
            "Discovery did not complete safely, so local creation remains blocked."
        ),
        "onboarding-enrollment-required": (
            "The selected federation no longer has valid enrollment authorization."
        ),
        "onboarding-invitation-required": (
            "The selected federation no longer has a valid invitation."
        ),
        "onboarding-binding-unavailable": (
            "No trusted saved federation binding is available for reconnect."
        ),
        "onboarding-identity-required": (
            "Create or load this device identity before continuing."
        ),
        "onboarding-identity-mismatch": (
            "The saved identity does not match the federation enrollment."
        ),
        "onboarding-untrusted-binding": (
            "The saved federation binding is not trusted for this device."
        ),
        "onboarding-binding-replacement-forbidden": (
            "CFI-2 cannot replace an existing federation binding."
        ),
    }
    return messages.get(
        code,
        "Federation onboarding could not be completed safely.",
    )


def _render_onboarding() -> Response:
    service = get_capability_onboarding_service()
    credentials = None
    saved_binding = None
    connected_binding = None
    candidates = ()
    repair_reason = None
    try:
        credentials = service.identity_or_none()
        saved_binding = service.binding_or_none()
        if credentials is not None and saved_binding is not None:
            try:
                context = service.authorized_context()
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
                candidates = service.discover()
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

    preview = service.legacy_preview()
    requested_step = request.args.get("step")
    if requested_step not in _ALLOWED_STEPS:
        requested_step = None
    view_model = service.build_view_model(
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
    return _secure_response(
        render_template("onboarding.html", onboarding=view_model)
    )


@capability_onboarding_web.before_app_request
def _startup_reconnect_and_dispatch() -> Response | None:
    """Revalidate saved trust once and dispatch before the legacy setup gate."""

    endpoint = request.endpoint or ""
    if not endpoint.startswith("static") and not current_app.extensions.get(
        _STARTUP_CHECK_EXTENSION_KEY
    ):
        current_app.extensions[_STARTUP_CHECK_EXTENSION_KEY] = True
        try:
            get_capability_onboarding_service().authorized_context()
        except Exception as exc:  # noqa: BLE001 - startup must remain available
            current_app.logger.info(
                "Capability onboarding startup reconnect unavailable (%s)",
                type(exc).__name__,
            )

    if request.blueprint != capability_onboarding_web.name:
        return None
    view = current_app.view_functions.get(endpoint)
    if view is None:
        return None
    return current_app.ensure_sync(view)(**(request.view_args or {}))


@capability_onboarding_web.get("/onboarding", strict_slashes=False)
def onboarding() -> Response:
    return _render_onboarding()


@capability_onboarding_web.post("/onboarding/identity")
def create_identity() -> Response:
    payload = _payload()
    try:
        _validate_server_bound_request(payload, require_command=False)
        get_capability_onboarding_service().create_identity()
    except AuthorizationError as exc:
        return _safe_error_response(exc.code, 403)
    except (
        FederationOperationError,
        FederationValidationError,
        NodeIdentityStateError,
    ) as exc:
        flash(_safe_message(exc), "error")
        return redirect(url_for("capability_onboarding_web.onboarding"), code=303)
    flash("Stable device identity is ready.", "success")
    return redirect(
        url_for("capability_onboarding_web.onboarding", step="federation"),
        code=303,
    )


@capability_onboarding_web.post("/onboarding/federation")
def federation_action() -> Response:
    payload = _payload()
    try:
        command_id = _validate_server_bound_request(
            payload,
            require_command=True,
        )
        assert command_id is not None
        intent = str(payload.get("intent") or "").strip()
        service = get_capability_onboarding_service()
        if intent == "refresh":
            service.discover()
            flash("Federation discovery refreshed.", "success")
        elif intent == "reconnect":
            service.reconnect()
            flash("Trusted federation membership reconnected.", "success")
        elif intent == "join":
            discovery_id = str(payload.get("discovery_id") or "").strip()
            verification_code = str(
                payload.get("verification_code") or ""
            ).strip()
            if len(verification_code) > 128:
                raise FederationValidationError(
                    "verification-code-too-large",
                    "verification_code",
                    "must not exceed 128 characters",
                )
            service.connect(
                request_id=f"flask-onboarding-{command_id}",
                discovery_id=discovery_id or None,
                verification_code=verification_code or None,
            )
            flash("The device joined the selected federation.", "success")
        elif intent == "create-local":
            service.connect(
                request_id=f"flask-onboarding-{command_id}",
            )
            flash("A local federation was created safely.", "success")
        else:
            raise FederationValidationError(
                "unknown-onboarding-intent",
                "intent",
                "is not supported by CFI-2",
            )
    except AuthorizationError as exc:
        return _safe_error_response(exc.code, 403)
    except (
        AuthenticationError,
        FederationOperationError,
        FederationValidationError,
        ProtocolCompatibilityError,
        NodeIdentityStateError,
    ) as exc:
        flash(_safe_message(exc), "error")
    return redirect(
        url_for("capability_onboarding_web.onboarding", step="federation"),
        code=303,
    )


@capability_onboarding_web.post("/onboarding/inspect")
@capability_onboarding_web.post("/onboarding/benchmarks/run")
@capability_onboarding_web.post("/onboarding/benchmarks/skip")
@capability_onboarding_web.post("/onboarding/contributions")
def blocked_later_step() -> Response:
    payload = _payload()
    try:
        _validate_server_bound_request(payload, require_command=False)
    except AuthorizationError as exc:
        return _safe_error_response(exc.code, 403)
    return _secure_response(
        (
            "This step remains blocked until its separate authority and "
            "persistence integration is reviewed."
        ),
        409,
    )


__all__ = ["capability_onboarding_web"]
