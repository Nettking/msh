"""Read-only Federation overview integration for the supported Flask app."""

from __future__ import annotations

from flask import (
    Blueprint,
    Response,
    current_app,
    make_response,
    redirect,
    render_template,
    request,
    url_for,
)

from catalog.federation.projections import (
    FederationProjectionService,
    ProjectionAdapters,
    assert_public_projection,
)

from .services.federation_projection_service import (
    get_federation_projection_service,
)

federation_web = Blueprint("federation_web", __name__)


def _safe_overview() -> dict[str, object]:
    try:
        service = get_federation_projection_service()
        return assert_public_projection(
            service.overview(include_technical=False).to_dict()
        )
    except Exception as exc:  # noqa: BLE001 - never expose projection failures
        current_app.logger.warning(
            "Federation overview projection unavailable (%s)",
            type(exc).__name__,
        )
        fallback = FederationProjectionService(ProjectionAdapters())
        return assert_public_projection(fallback.overview().to_dict())


def _overview_response() -> Response:
    response = make_response(
        render_template(
            "federation_overview.html",
            federation_overview=_safe_overview(),
        )
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "same-origin"
    return response


@federation_web.before_app_request
def _serve_read_only_federation_before_runtime_gate() -> Response | None:
    """Dispatch GET/HEAD before the legacy runtime gate without changing it."""

    if (
        request.blueprint != federation_web.name
        or request.method not in {"GET", "HEAD"}
    ):
        return None
    endpoint = request.endpoint or ""
    view = current_app.view_functions.get(endpoint)
    if view is None:
        return None
    return current_app.ensure_sync(view)(**(request.view_args or {}))


@federation_web.get("/federation", strict_slashes=False)
def overview() -> Response:
    if request.query_string:
        return redirect(url_for("federation_web.overview"))
    return _overview_response()


__all__ = ["federation_web"]
