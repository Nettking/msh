"""Browser routes for signed, short-lived Federation pairing codes."""

from __future__ import annotations

import hmac
import os
from urllib.parse import urlsplit

from flask import (
    Blueprint,
    Response,
    current_app,
    flash,
    redirect,
    request,
    session,
    url_for,
)

from catalog.federation.errors import FederationOperationError

from .capability_onboarding_routes import _CSRF_SESSION_KEY
from .services.capability_onboarding_service import get_capability_onboarding_service
from .services.federation_pairing_service import (
    PairingAwareCapabilityOnboardingService,
)

federation_pairing_web = Blueprint("federation_pairing_web", __name__)


def _pairing_service() -> PairingAwareCapabilityOnboardingService:
    service = get_capability_onboarding_service()
    if not isinstance(service, PairingAwareCapabilityOnboardingService):
        raise FederationOperationError(
            "pairing-service-unavailable",
            "the authenticated pairing service is not installed",
        )
    return service


def _require_csrf() -> None:
    """Validate against the same server-issued token as the onboarding forms."""

    expected = session.get(_CSRF_SESSION_KEY)
    supplied = request.form.get("_csrf_token")
    if (
        not isinstance(expected, str)
        or not isinstance(supplied, str)
        or not hmac.compare_digest(expected, supplied)
    ):
        raise FederationOperationError(
            "invalid-onboarding-csrf",
            "the onboarding form expired; reload the page and try again",
            "_csrf_token",
        )


def _relay_url() -> str:
    configured = str(
        current_app.config.get("CAPABILITY_ONBOARDING_PAIRING_RELAY_URL")
        or os.getenv("MSH_PAIRING_RELAY_URL", "")
    ).strip()
    if configured:
        return configured
    parsed = urlsplit(f"//{request.host}")
    host = parsed.hostname or ""
    if host.casefold() in {"localhost", "127.0.0.1", "::1"}:
        raise FederationOperationError(
            "pairing-lan-address-required",
            "open MSH using the LAN or VPN address that the other machine can reach, then generate the code again",
            "relay_url",
        )
    formatted_host = f"[{host}]" if ":" in host else host
    port = int(os.getenv("MSH_RELAY_PORT", "8765"))
    return f"ws://{formatted_host}:{port}"


@federation_pairing_web.before_app_request
def _dispatch_before_legacy_setup_gate() -> Response | None:
    """Execute pairing endpoints before the retained role-first setup gate."""

    if request.blueprint != federation_pairing_web.name:
        return None
    endpoint = request.endpoint or ""
    view = current_app.view_functions.get(endpoint)
    if view is None:
        return None
    return current_app.ensure_sync(view)(**(request.view_args or {}))


@federation_pairing_web.post("/onboarding/federation/pairing-code")
def create_pairing_code():
    try:
        _require_csrf()
        code = _pairing_service().create_pairing_code(relay_url=_relay_url())
        if not code:
            raise FederationOperationError(
                "pairing-code-unavailable",
                "the pairing code could not be created",
            )
        flash(
            "Pairing code created. Copy it to the other MSH device within five minutes.",
            "success",
        )
    except FederationOperationError as exc:
        flash(exc.message, "error")
    except Exception as exc:  # noqa: BLE001 - do not expose tokens or endpoints
        current_app.logger.warning(
            "Federation pairing code generation failed (%s)",
            type(exc).__name__,
        )
        flash("The pairing code could not be created safely.", "error")
    return redirect(
        url_for("capability_onboarding_web.onboarding", step="federation"),
        code=303,
    )


@federation_pairing_web.post("/onboarding/federation/pair")
def pair_device():
    try:
        _require_csrf()
        code = str(request.form.get("pairing_code") or "").strip()
        if not code:
            raise FederationOperationError(
                "pairing-code-required",
                "paste the pairing code from the other MSH device",
                "pairing_code",
            )
        _pairing_service().redeem_pairing_code(code)
        flash("This device is now connected to the shared Federation.", "success")
        return redirect(
            url_for("capability_inspection_web.onboarding", step="inspect"),
            code=303,
        )
    except FederationOperationError as exc:
        flash(exc.message, "error")
    except Exception as exc:  # noqa: BLE001 - fail closed without token leakage
        current_app.logger.warning(
            "Federation pairing failed (%s)",
            type(exc).__name__,
        )
        flash(
            "The pairing code was invalid, expired, already used, or the other device was unreachable.",
            "error",
        )
    return redirect(
        url_for("capability_onboarding_web.onboarding", step="federation"),
        code=303,
    )


__all__ = ["federation_pairing_web"]
