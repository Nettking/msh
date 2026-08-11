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

from .services.pending_contribution_approval import (
    get_local_provider_operator_surface,
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
    if isinstance(value, ProviderOperatorSurface):
        return value
    return get_local_provider_operator_surface()


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


def _safe_error(exc: BaseException, fallback: str) -> tuple[str, str | None]:
    code = getattr(exc, "code", fallback)
    field = getattr(exc, "field", None)
    return (
        code if isinstance(code, str) and code else fallback,
        field if isinstance(field, str) and field else None,
    )


def _json_error(exc: BaseException, status: int):
    code, field = _safe_error(exc, "provider-operator-failed")
    payload: dict[str, Any] = {"ok": False, "error": {"code": code}}
    if field is not None:
        payload["error"]["field"] = field
    return jsonify(payload), status


def _html_error(exc: BaseException):
    code, _field = _safe_error(exc, "provider-operator-failed")
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


def _unavailable_json():
    return (
        jsonify(
            {
                "ok": False,
                "error": {"code": "provider-operator-unavailable"},
            }
        ),
        503,
    )


def _unavailable_html():
    return (
        render_template(
            "provider_federation.html",
            operator_view=None,
            operator_error="provider-operator-unavailable",
            provider_federation_csrf_token=_csrf_token(),
        ),
        503,
    )


def _execute_action(capability_id: str, action: str):
    surface = _surface()
    if surface is None:
        raise FederationOperationError(
            "provider-operator-unavailable",
            "provider operator surface is not configured",
        )
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
    return surface.execute(
        ProviderOperatorAction(action),
        capability_id=capability_id,
        expected_revision=expected_revision,
        reason_code=payload.get("reason_code"),
        command_id=payload.get("command_id") or None,
    )


@provider_federation_web.get("")
def index():
    surface = _surface()
    if surface is None:
        return _unavailable_html()
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
        return _unavailable_json()
    try:
        payload = surface.view().to_dict()
    except AuthorizationError as exc:
        return _json_error(exc, 403)
    except (
        FederationOperationError,
        FederationValidationError,
        ProtocolCompatibilityError,
    ) as exc:
        return _json_error(exc, 409)
    payload["csrf_token"] = _csrf_token()
    return jsonify({"ok": True, "view": payload})


@provider_federation_web.post("/providers/<capability_id>/<action>")
def provider_action_html(capability_id: str, action: str):
    try:
        _execute_action(capability_id, action)
    except AuthorizationError as exc:
        return _html_error(exc)
    except (TypeError, ValueError, FederationValidationError) as exc:
        return _html_error(exc)
    except (FederationOperationError, ProtocolCompatibilityError) as exc:
        return _html_error(exc)
    flash(f"Provider action {action} accepted.", "success")
    return redirect(url_for("provider_federation_web.index"))


@provider_federation_web.post("/api/providers/<capability_id>/<action>")
def provider_action_api(capability_id: str, action: str):
    try:
        snapshot = _execute_action(capability_id, action)
    except AuthorizationError as exc:
        return _json_error(exc, 403)
    except (TypeError, ValueError, FederationValidationError) as exc:
        return _json_error(exc, 400)
    except FederationOperationError as exc:
        status = 503 if exc.code == "provider-operator-unavailable" else 409
        return _json_error(exc, status)
    except ProtocolCompatibilityError as exc:
        return _json_error(exc, 409)
    return jsonify({"ok": True, "provider": snapshot.to_dict()})


__all__ = ["provider_federation_web"]
