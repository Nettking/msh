"""Local F8.5 HTML and JSON surface for trusted provider operations."""

from __future__ import annotations

import secrets
from typing import Any

from flask import (
    Blueprint,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from catalog.capabilities.operator_surface import (
    ProviderOperatorAction,
    ProviderOperatorSurface,
)
from catalog.federation.errors import (
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)

provider_federation_web = Blueprint(
    "provider_federation_web",
    __name__,
    url_prefix="/provider-federation",
    template_folder="templates",
)

_SURFACE_CONFIG_KEY = "PROVIDER_OPERATOR_SURFACE"
_CSRF_SESSION_KEY = "provider_federation_csrf_token"
_FORBIDDEN_CONTEXT_FIELDS = frozenset({"actor_node_id", "session_id"})


def _surface() -> ProviderOperatorSurface | None:
    value = current_app.config.get(_SURFACE_CONFIG_KEY)
    return value if isinstance(value, ProviderOperatorSurface) else None


def _csrf_token() -> str:
    value = session.get(_CSRF_SESSION_KEY)
    if not isinstance(value, str) or len(value) < 32:
        value = secrets.token_urlsafe(32)
        session[_CSRF_SESSION_KEY] = value
    return value


def _payload() -> dict[str, Any]:
    if request.is_json:
        value = request.get_json(silent=True)
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise FederationValidationError(
                "invalid-operator-request",
                "body",
                "must be a JSON object",
            )
        return value
    return request.form.to_dict(flat=True)


def _wants_json() -> bool:
    if request.path.startswith("/provider-federation/api/"):
        return True
    return request.accept_mimetypes.best == "application/json"


def _safe_error(exc: BaseException, fallback: str) -> tuple[str, str | None]:
    code = getattr(exc, "code", fallback)
    field = getattr(exc, "field", None)
    return (
        code if isinstance(code, str) and code else fallback,
        field if isinstance(field, str) and field else None,
    )


def _error_response(exc: BaseException, status: int):
    code, field = _safe_error(exc, "provider-operator-failed")
    if _wants_json():
        payload: dict[str, Any] = {"ok": False, "error": {"code": code}}
        if field is not None:
            payload["error"]["field"] = field
        return jsonify(payload), status
    flash(code.replace("-", " ").capitalize(), "error")
    return redirect(url_for("provider_federation_web.index"))


def _validate_csrf(payload: dict[str, Any]) -> None:
    supplied = request.headers.get("X-CSRF-Token") or payload.get("_csrf_token")
    expected = session.get(_CSRF_SESSION_KEY)
    if (
        not isinstance(supplied, str)
        or not isinstance(expected, str)
        or not secrets.compare_digest(supplied, expected)
    ):
        raise AuthorizationError(
            "provider-operator-csrf-rejected",
            "operator control request did not carry the current CSRF token",
            "_csrf_token",
        )


def _unavailable_response():
    if _wants_json():
        return (
            jsonify(
                {
                    "ok": False,
                    "error": {"code": "provider-operator-unavailable"},
                }
            ),
            503,
        )
    return (
        render_template(
            "provider_federation.html",
            operator_view=None,
            operator_error="provider-operator-unavailable",
            provider_federation_csrf_token=_csrf_token(),
        ),
        503,
    )


@provider_federation_web.get("")
def index():
    surface = _surface()
    if surface is None:
        return _unavailable_response()
    try:
        view = surface.view()
    except (
        AuthorizationError,
        FederationOperationError,
        FederationValidationError,
        ProtocolCompatibilityError,
    ) as exc:
        code, _field = _safe_error(exc, "provider-operator-unavailable")
        return (
            render_template(
                "provider_federation.html",
                operator_view=None,
                operator_error=code,
                provider_federation_csrf_token=_csrf_token(),
            ),
            403 if isinstance(exc, AuthorizationError) else 409,
        )
    return render_template(
        "provider_federation.html",
        operator_view=view,
        operator_error="",
        provider_federation_csrf_token=_csrf_token(),
    )


@provider_federation_web.get("/api/providers")
def providers_api():
    surface = _surface()
    if surface is None:
        return _unavailable_response()
    try:
        payload = surface.view().to_dict()
    except AuthorizationError as exc:
        return _error_response(exc, 403)
    except (
        FederationOperationError,
        FederationValidationError,
        ProtocolCompatibilityError,
    ) as exc:
        return _error_response(exc, 409)
    payload["csrf_token"] = _csrf_token()
    return jsonify({"ok": True, "view": payload})


@provider_federation_web.post("/api/providers/<capability_id>/<action>")
def provider_action(capability_id: str, action: str):
    surface = _surface()
    if surface is None:
        return _unavailable_response()
    try:
        payload = _payload()
        if _FORBIDDEN_CONTEXT_FIELDS.intersection(payload):
            raise FederationValidationError(
                "operator-context-override-forbidden",
                "body",
                "actor and session are bound by the server",
            )
        _validate_csrf(payload)
        expected_revision_value = payload.get("expected_revision")
        expected_revision = (
            None
            if expected_revision_value in (None, "")
            else int(expected_revision_value)
        )
        snapshot = surface.execute(
            ProviderOperatorAction(action),
            capability_id=capability_id,
            expected_revision=expected_revision,
            reason_code=payload.get("reason_code"),
            command_id=payload.get("command_id") or None,
        )
    except AuthorizationError as exc:
        return _error_response(exc, 403)
    except (TypeError, ValueError, FederationValidationError) as exc:
        return _error_response(exc, 400)
    except (FederationOperationError, ProtocolCompatibilityError) as exc:
        return _error_response(exc, 409)

    if _wants_json():
        return jsonify({"ok": True, "provider": snapshot.to_dict()})
    flash(f"Provider action {action} accepted.", "success")
    return redirect(url_for("provider_federation_web.index"))


__all__ = ["provider_federation_web"]
