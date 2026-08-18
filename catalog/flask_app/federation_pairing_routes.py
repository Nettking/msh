"""Browser and host-bridge routes for signed, short-lived pairing codes.

The host responder cannot mint membership; it can only ask this application
for one ordinary pairing grant after it has verified, through the local
tailscaled, that the connecting peer is a same-tailnet, same-owner device.
The grant itself is produced by the existing pairing authority.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from urllib.parse import urlsplit

from flask import (
    Blueprint,
    Response,
    current_app,
    flash,
    jsonify,
    redirect,
    request,
    session,
    url_for,
)

from catalog.federation.errors import FederationOperationError
from catalog.federation.tailnet_join_bridge import (
    GRANT_SCHEMA,
    SECRET_HEADER,
    auto_join_port,
    read_secret,
    secret_path,
)
from catalog.federation.tailscale_host_discovery import ADVERTISEMENT_SCHEMA

from .capability_onboarding_routes import _CSRF_SESSION_KEY
from .services.capability_onboarding_service import get_capability_onboarding_service
from .services.federation_pairing_service import (
    MAX_PAIRING_TTL_SECONDS,
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
        or os.getenv("FCP_PAIRING_RELAY_URL", "")
    ).strip()
    if configured:
        return configured
    parsed = urlsplit(f"//{request.host}")
    host = parsed.hostname or ""
    if host.casefold() in {"localhost", "127.0.0.1", "::1"}:
        raise FederationOperationError(
            "pairing-lan-address-required",
            "open FCP using the LAN or VPN address that the other machine can reach, then generate the code again",
            "relay_url",
        )
    formatted_host = f"[{host}]" if ":" in host else host
    port = int(os.getenv("FCP_RELAY_PORT", "8765"))
    return f"ws://{formatted_host}:{port}"


def _discovery_response() -> Response:
    """Advertise public-safe identity only; never mint or expose join authority."""

    try:
        service = _pairing_service()
        # A remotely paired member is not the Federation authority and must not
        # impersonate the coordinator during discovery.
        if service.remote_store.load() is not None:
            return Response(status=404)
        context = service.authorized_context()
        if context is None:
            return Response(status=404)
        federation = context.coordinator.store.get_session(
            context.binding.internal_session_id
        )
        if federation is None:
            return Response(status=404)
        relay_port = int(os.getenv("FCP_RELAY_PORT", "8765"))
        if not 1 <= relay_port <= 65_535:
            relay_port = 8765
        fingerprint = hashlib.sha256(
            (
                "fcp-tailscale-discovery-v1\0"
                + context.binding.federation_id
            ).encode("utf-8")
        ).hexdigest()[:32]
        response = jsonify(
            {
                "schema": ADVERTISEMENT_SCHEMA,
                "federation_label": str(federation.display_name),
                "federation_fingerprint": fingerprint,
                "device_name": context.credentials.identity.display_name,
                "relay_port": relay_port,
                "pairing_required": True,
                # Where a joining host asks this device's responder. Routing
                # information only; the responder still verifies identity.
                "auto_join_port": auto_join_port(),
            }
        )
    except Exception as exc:  # noqa: BLE001 - discovery endpoint fails closed
        current_app.logger.info(
            "Federation discovery advertisement unavailable (%s)",
            type(exc).__name__,
        )
        return Response(status=404)
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


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


@federation_pairing_web.get("/onboarding/federation/discovery.json")
def federation_discovery():
    return _discovery_response()


def _auto_join_secret_matches() -> bool:
    """Authenticate the host responder by a secret only this host can read."""

    expected = read_secret(secret_path())
    supplied = request.headers.get(SECRET_HEADER, "")
    if not expected or not isinstance(supplied, str) or not supplied:
        return False
    return hmac.compare_digest(expected, supplied)


@federation_pairing_web.post("/internal/federation/tailnet-join-grant")
def tailnet_join_grant():
    """Issue one pairing grant to the verified host responder.

    Peer identity was established host-side before this call. This endpoint only
    proves the caller is the local responder, then delegates entirely to the
    existing pairing authority.
    """

    if not _auto_join_secret_matches():
        current_app.logger.info("Auto-join grant request rejected: unauthenticated")
        return jsonify({"accepted": False, "error": "unauthenticated"}), 403
    try:
        service = _pairing_service()
        if service.remote_store.load() is not None:
            return jsonify({"accepted": False, "error": "not-the-coordinator"}), 409
        code = service.create_pairing_code(
            relay_url=_relay_url(),
            ttl_seconds=MAX_PAIRING_TTL_SECONDS,
            remember=False,
        )
        if not code:
            raise FederationOperationError(
                "pairing-code-unavailable",
                "the pairing code could not be created",
            )
    except FederationOperationError as exc:
        current_app.logger.info("Auto-join grant unavailable (%s)", exc.code)
        return jsonify({"accepted": False, "error": exc.code}), 503
    except Exception as exc:  # noqa: BLE001 - never leak authority details
        current_app.logger.warning(
            "Auto-join grant failed (%s)",
            type(exc).__name__,
        )
        return jsonify({"accepted": False, "error": "auto-join-unavailable"}), 503
    # The grant is deliberately absent from this log line.
    current_app.logger.info("Auto-join grant issued to the local host responder")
    response = jsonify({"schema": GRANT_SCHEMA, "accepted": True, "pairing_code": code})
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@federation_pairing_web.post("/onboarding/federation/pairing-code")
def create_pairing_code():
    try:
        _require_csrf()
        code = _pairing_service().create_pairing_code(
            relay_url=_relay_url(),
            ttl_seconds=MAX_PAIRING_TTL_SECONDS,
        )
        if not code:
            raise FederationOperationError(
                "pairing-code-unavailable",
                "the pairing code could not be created",
            )
        flash(
            "Pairing code created. It is one-use, valid for up to ten minutes, and you can create a new code at any time.",
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
                "paste the pairing code from the other FCP device",
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
