"""Human-authenticated enrollment for a fresh device discovered through Tailscale.

A second FCP device should not create another local administrator.  Instead, a
fresh device can use the public-safe Tailscale discovery snapshot to send the
browser to the existing Federation authority.  The human signs in there, an
administrator explicitly approves the new device, and the authority returns a
short-lived one-use pairing bundle to the fresh device.

Passwords never pass through the joining device.  The only cross-device secret
is the already supported one-use pairing bundle, returned in a POST body and
bound to a high-entropy browser state value.  After pairing, normal Federation
SSO creates the local shadow user on first sign-in.
"""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import os
import re
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, urlsplit, urlunsplit

from flask import (
    Blueprint,
    abort,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from catalog.federation.errors import FederationOperationError
from catalog.federation.tailscale_host_discovery import load_snapshot

from .federation import saved_remote_member
from .models import User, db
from ..services.capability_onboarding_service import get_capability_onboarding_service
from ..services.federation_pairing_service import PairingAwareCapabilityOnboardingService

federation_enrollment = Blueprint(
    "federation_enrollment",
    __name__,
    url_prefix="/federation-auth/enroll",
)

_DISCOVERY_FILE_DEFAULT = "data/federation/onboarding/tailscale_discovery.json"
_TAILSCALE_IPV4_NETWORK = ipaddress.IPv4Network("100.64.0.0/10")
_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{32}$")
_NODE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
_MAX_STATE_BYTES = 256
_MAX_RETURN_URL_BYTES = 2048
_ENROLLMENT_TTL = timedelta(minutes=10)
_STATE_KEY = "fcp_federation_enrollment_state"
_ISSUED_KEY = "fcp_federation_enrollment_issued"
_FINGERPRINT_KEY = "fcp_federation_enrollment_fingerprint"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_stamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("missing timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _pairing_service() -> PairingAwareCapabilityOnboardingService:
    service = get_capability_onboarding_service()
    if not isinstance(service, PairingAwareCapabilityOnboardingService):
        raise FederationOperationError(
            "pairing-service-unavailable",
            "the authenticated pairing service is not installed",
        )
    return service


def _is_fresh_device() -> bool:
    try:
        if saved_remote_member():
            return False
        return db.session.query(User.id).limit(1).first() is None
    except Exception:  # noqa: BLE001 - enrollment eligibility must fail closed
        return False


def _tailscale_ip(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        address = ipaddress.IPv4Address(value.strip())
    except ipaddress.AddressValueError:
        return None
    if address not in _TAILSCALE_IPV4_NETWORK:
        return None
    return str(address)


def discovered_federations() -> tuple[dict[str, object], ...]:
    """Return the already-revalidated public-safe Tailscale candidates."""

    path = os.getenv("FCP_TAILSCALE_DISCOVERY_FILE", _DISCOVERY_FILE_DEFAULT)
    snapshot = load_snapshot(path)
    raw = snapshot.get("federations")
    if not isinstance(raw, list):
        return ()
    candidates: list[dict[str, object]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        fingerprint = item.get("federation_fingerprint")
        address = _tailscale_ip(item.get("tailscale_ip"))
        port = item.get("web_port")
        if (
            not isinstance(fingerprint, str)
            or _FINGERPRINT_RE.fullmatch(fingerprint) is None
            or address is None
            or isinstance(port, bool)
            or not isinstance(port, int)
            or not 1 <= port <= 65535
        ):
            continue
        candidates.append(
            {
                "federation_label": str(item.get("federation_label") or "Federation"),
                "federation_fingerprint": fingerprint,
                "device_name": str(item.get("device_name") or "FCP device"),
                "tailscale_ip": address,
                "web_port": port,
            }
        )
    return tuple(candidates)


def fresh_discovered_federations() -> tuple[dict[str, object], ...]:
    return discovered_federations() if _is_fresh_device() else ()


def _candidate(fingerprint: str) -> dict[str, object] | None:
    if _FINGERPRINT_RE.fullmatch(fingerprint) is None:
        return None
    return next(
        (
            candidate
            for candidate in fresh_discovered_federations()
            if candidate["federation_fingerprint"] == fingerprint
        ),
        None,
    )


def _normalize_tailscale_origin(value: str) -> str:
    if not isinstance(value, str) or len(value.encode("utf-8")) > _MAX_RETURN_URL_BYTES:
        raise ValueError("invalid Tailscale origin")
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("invalid Tailscale origin") from exc
    address = _tailscale_ip(parsed.hostname)
    if (
        parsed.scheme not in {"http", "https"}
        or address is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise ValueError("invalid Tailscale origin")
    netloc = address if port is None else f"{address}:{port}"
    return urlunsplit((parsed.scheme.lower(), netloc, "", "", "")).rstrip("/")


def _normalize_return_url(value: object) -> str:
    if not isinstance(value, str) or len(value.encode("utf-8")) > _MAX_RETURN_URL_BYTES:
        raise ValueError("invalid enrollment return URL")
    parsed = urlsplit(value)
    origin = _normalize_tailscale_origin(
        urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))
    )
    expected_path = url_for("federation_enrollment.callback")
    if parsed.path != expected_path or parsed.query or parsed.fragment:
        raise ValueError("invalid enrollment return URL")
    return f"{origin}{expected_path}"


def _authority_fingerprint(service: PairingAwareCapabilityOnboardingService) -> str:
    if service.remote_store.load() is not None:
        raise FederationOperationError(
            "pairing-host-must-be-local-authority",
            "a remotely paired member cannot authorize a new device",
        )
    context = service.authorized_context()
    if context is None:
        raise FederationOperationError(
            "pairing-federation-required",
            "the Federation authority is unavailable",
        )
    return hashlib.sha256(
        ("fcp-tailscale-discovery-v1\0" + context.binding.federation_id).encode("utf-8")
    ).hexdigest()[:32]


def _authority_relay_url() -> str:
    parsed = urlsplit(f"//{request.host}")
    address = _tailscale_ip(parsed.hostname)
    if address is None:
        raise FederationOperationError(
            "pairing-tailscale-address-required",
            "open the Federation authority through its Tailscale address",
            "relay_url",
        )
    try:
        port = int(os.getenv("FCP_RELAY_PORT", "8765"))
    except ValueError as exc:
        raise FederationOperationError(
            "pairing-relay-port-invalid",
            "the configured Federation relay port is invalid",
            "relay_url",
        ) from exc
    if not 1 <= port <= 65535:
        raise FederationOperationError(
            "pairing-relay-port-invalid",
            "the configured Federation relay port is invalid",
            "relay_url",
        )
    return f"ws://{address}:{port}"


def _clear_state() -> None:
    session.pop(_STATE_KEY, None)
    session.pop(_ISSUED_KEY, None)
    session.pop(_FINGERPRINT_KEY, None)


@federation_enrollment.post("/start")
def start():
    """Begin login-first enrollment from a genuinely fresh device."""

    if not _is_fresh_device():
        return redirect(url_for("security.login"))
    fingerprint = str(request.form.get("federation_fingerprint") or "").strip()
    candidate = _candidate(fingerprint)
    if candidate is None:
        flash("That discovered Federation is no longer available. Restart discovery and try again.", "error")
        return redirect(url_for("security.login"))

    try:
        credentials = _pairing_service().create_identity()
        local_origin = _normalize_tailscale_origin(request.url_root)
    except Exception as exc:  # noqa: BLE001 - keep details out of anonymous UI
        current_app.logger.warning(
            "Federation enrollment start failed (%s)", type(exc).__name__
        )
        flash(
            "Federation sign-in must be opened through this device's Tailscale address.",
            "error",
        )
        return redirect(url_for("security.login"))

    state = secrets.token_urlsafe(32)
    session[_STATE_KEY] = state
    session[_ISSUED_KEY] = _stamp(_utc_now())
    session[_FINGERPRINT_KEY] = fingerprint

    authority_origin = f"http://{candidate['tailscale_ip']}:{candidate['web_port']}"
    return_url = f"{local_origin}{url_for('federation_enrollment.callback')}"
    query = urlencode(
        {
            "state": state,
            "return_url": return_url,
            "target_node_id": credentials.identity.node_id,
            "target_name": credentials.identity.display_name,
            "federation_fingerprint": fingerprint,
        }
    )
    return redirect(f"{authority_origin}{url_for('federation_enrollment.authorize')}?{query}")


def _authorization_fields() -> tuple[str, str, str, str, str]:
    source = request.form if request.method == "POST" else request.args
    state = str(source.get("state") or "").strip()
    return_url = _normalize_return_url(source.get("return_url"))
    target_node_id = str(source.get("target_node_id") or "").strip()
    target_name = str(source.get("target_name") or "FCP device").strip()[:256]
    fingerprint = str(source.get("federation_fingerprint") or "").strip()
    if (
        not state
        or len(state.encode("utf-8")) > _MAX_STATE_BYTES
        or _NODE_ID_RE.fullmatch(target_node_id) is None
        or _FINGERPRINT_RE.fullmatch(fingerprint) is None
    ):
        raise ValueError("invalid enrollment request")
    return state, return_url, target_node_id, target_name, fingerprint


@federation_enrollment.route("/authorize", methods=["GET", "POST"])
def authorize():
    """Let an authenticated Federation administrator approve the fresh device."""

    if not current_user.is_authenticated or not current_user.is_active:
        abort(401)
    try:
        state, return_url, target_node_id, target_name, fingerprint = (
            _authorization_fields()
        )
        service = _pairing_service()
        expected_fingerprint = _authority_fingerprint(service)
        if not hmac.compare_digest(fingerprint, expected_fingerprint):
            raise ValueError("Federation fingerprint mismatch")
    except (FederationOperationError, ValueError) as exc:
        current_app.logger.warning(
            "Federation enrollment authorization rejected (%s)", type(exc).__name__
        )
        abort(400)

    if request.method == "GET":
        return render_template(
            "auth/federation_enroll_authorize.html",
            state=state,
            return_url=return_url,
            target_node_id=target_node_id,
            target_name=target_name,
            federation_fingerprint=fingerprint,
        )

    try:
        pairing_code = service.create_pairing_code(
            relay_url=_authority_relay_url(),
            ttl_seconds=600,
        )
    except FederationOperationError as exc:
        current_app.logger.warning(
            "Federation enrollment grant failed: %s", exc.code
        )
        abort(503)

    response = render_template(
        "auth/federation_enroll_return.html",
        return_url=return_url,
        state=state,
        pairing_code=pairing_code,
    )
    return response, 200, {
        "Cache-Control": "no-store",
        "Pragma": "no-cache",
        "Referrer-Policy": "no-referrer",
    }


@federation_enrollment.post("/callback")
def callback():
    """Redeem the approved one-use pairing bundle on the fresh device."""

    if not _is_fresh_device():
        abort(409)
    expected_state = session.get(_STATE_KEY)
    issued = session.get(_ISSUED_KEY)
    expected_fingerprint = session.get(_FINGERPRINT_KEY)
    state = str(request.form.get("state") or "")
    pairing_code = str(request.form.get("pairing_code") or "").strip()
    if (
        not isinstance(expected_state, str)
        or not isinstance(expected_fingerprint, str)
        or not state
        or not secrets.compare_digest(state, expected_state)
        or not pairing_code
    ):
        _clear_state()
        flash("Federation enrollment response was missing or did not match this browser session.", "error")
        return redirect(url_for("security.login"))
    try:
        requested_at = _parse_stamp(issued)
        if _utc_now() - requested_at > _ENROLLMENT_TTL:
            raise ValueError("expired enrollment")
        service = _pairing_service()
        offer = service.pairing_codec.decode(pairing_code)
        actual_fingerprint = hashlib.sha256(
            ("fcp-tailscale-discovery-v1\0" + offer.federation_id).encode("utf-8")
        ).hexdigest()[:32]
        if not hmac.compare_digest(expected_fingerprint, actual_fingerprint):
            raise FederationOperationError(
                "pairing-federation-mismatch",
                "the returned pairing grant belongs to a different Federation",
            )
        service.redeem_pairing_code(pairing_code)
    except (FederationOperationError, ValueError) as exc:
        _clear_state()
        current_app.logger.warning(
            "Federation enrollment callback failed (%s)", type(exc).__name__
        )
        flash("The Federation enrollment could not be verified. Try signing in again.", "error")
        return redirect(url_for("security.login"))

    _clear_state()
    flash(
        "This device joined the Federation. Continue with the same Federation account; no new local password was created.",
        "success",
    )
    return redirect(url_for("security.login"))


__all__ = [
    "discovered_federations",
    "federation_enrollment",
    "fresh_discovered_federations",
]
