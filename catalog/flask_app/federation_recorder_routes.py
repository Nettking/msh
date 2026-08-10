"""Authenticated browser controls for standalone Federation recorders."""

from __future__ import annotations

import hmac

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
    FederationOperationError,
    FederationValidationError,
)

from .capability_onboarding_routes import _CSRF_SESSION_KEY, _csrf_token
from .services.federation_recorder_control_service import (
    FederationRecorderControlError,
    get_federation_recorder_control_service,
)
from .services.mtconnect_discovery_service import MtconnectDiscoveryError

federation_recorder_web = Blueprint("federation_recorder_web", __name__)


def _require_csrf() -> None:
    if request.headers.get("Sec-Fetch-Site", "").casefold() == "cross-site":
        raise FederationRecorderControlError(
            "The recorder control request came from another site. Reload FCP and try again."
        )
    expected = session.get(_CSRF_SESSION_KEY)
    supplied = request.form.get("_csrf_token")
    if (
        not isinstance(expected, str)
        or not isinstance(supplied, str)
        or not hmac.compare_digest(expected, supplied)
    ):
        raise FederationRecorderControlError(
            "The recorder control form expired. Reload the page and try again."
        )


def _destination() -> str:
    return url_for("federation_recorder_web.index")


def _error_message(exc: BaseException) -> str:
    if isinstance(exc, FederationRecorderControlError):
        return str(exc)
    if isinstance(exc, MtconnectDiscoveryError):
        return str(exc)
    if isinstance(exc, AuthenticationError):
        return "A trusted Federation membership is required to control recorders."
    if isinstance(exc, (FederationOperationError, FederationValidationError)):
        return str(getattr(exc, "message", "The recorder control request was rejected."))
    return "The recorder control request could not be completed safely."


@federation_recorder_web.before_app_request
def _dispatch_before_legacy_runtime_gate() -> Response | None:
    """Keep recorder controls reachable on every trusted Federation member."""

    if request.blueprint != federation_recorder_web.name:
        return None
    endpoint = request.endpoint or ""
    view = current_app.view_functions.get(endpoint)
    if view is None:
        return None
    return current_app.ensure_sync(view)(**(request.view_args or {}))


@federation_recorder_web.get("/federation/recorders", strict_slashes=False)
def index() -> Response:
    try:
        model = get_federation_recorder_control_service().snapshot()
    except Exception as exc:  # noqa: BLE001 - render a safe degraded view
        current_app.logger.warning(
            "Federation recorder view unavailable (%s)", type(exc).__name__
        )
        model = {
            "title": "Recorder control",
            "intro": (
                "Manage standalone MTConnect recorder sources from any trusted "
                "Federation device."
            ),
            "sections": [
                {"key": "overview", "label": "Overview", "url": "/federation"}
            ],
            "active_section": "recorders",
            "recorders": [],
            "degraded": True,
        }
    response = make_response(
        render_template(
            "federation_recorders.html",
            recorder_control=model,
            federation_csrf_token=_csrf_token(),
        )
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "same-origin"
    return response


@federation_recorder_web.post("/federation/recorders/<target_node_id>/scan")
def scan(target_node_id: str) -> Response:
    try:
        _require_csrf()
        port_text = str(request.form.get("port") or "5000").strip()
        try:
            port = int(port_text)
        except ValueError as exc:
            raise FederationRecorderControlError(
                "The MTConnect port must be a number."
            ) from exc
        get_federation_recorder_control_service().request_scan(
            target_node_id,
            cidr=str(request.form.get("cidr") or "").strip(),
            port=port,
        )
        flash(
            "Network scan requested. It will run on the recorder's private network and report discovered MTConnect machines back here.",
            "success",
        )
    except Exception as exc:  # noqa: BLE001 - never expose relay internals
        if not isinstance(
            exc,
            (
                FederationRecorderControlError,
                MtconnectDiscoveryError,
                AuthenticationError,
                FederationOperationError,
                FederationValidationError,
            ),
        ):
            current_app.logger.warning(
                "Federation recorder scan request failed (%s)", type(exc).__name__
            )
        flash(_error_message(exc), "error")
    return redirect(_destination(), code=303)


@federation_recorder_web.post("/federation/recorders/<target_node_id>/sources")
def sources(target_node_id: str) -> Response:
    try:
        _require_csrf()
        get_federation_recorder_control_service().request_source_change(
            target_node_id,
            scan_id=str(request.form.get("scan_id") or "").strip(),
            add_source_ids=request.form.getlist("add_source_id"),
            remove_source_names=request.form.getlist("remove_source_name"),
        )
        flash(
            "Recorder source change requested. The recorder will revalidate the selection locally before changing capture.",
            "success",
        )
    except Exception as exc:  # noqa: BLE001 - never expose relay internals
        if not isinstance(
            exc,
            (
                FederationRecorderControlError,
                MtconnectDiscoveryError,
                AuthenticationError,
                FederationOperationError,
                FederationValidationError,
            ),
        ):
            current_app.logger.warning(
                "Federation recorder source request failed (%s)", type(exc).__name__
            )
        flash(_error_message(exc), "error")
    return redirect(_destination(), code=303)


__all__ = ["federation_recorder_web"]
