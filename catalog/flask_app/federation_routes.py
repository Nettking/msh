"""Read-only Federation product integration for the supported Flask app."""

from __future__ import annotations

from flask import (
    Blueprint,
    Response,
    abort,
    current_app,
    make_response,
    redirect,
    render_template,
    request,
    url_for,
)

from catalog.federation.projections import (
    FederationPage,
    FederationProjectionService,
    ProjectionAdapters,
    assert_public_projection,
)

from .services.federation_projection_service import (
    get_federation_projection_service,
)

federation_web = Blueprint("federation_web", __name__)

_PAGE_BY_PATH = {
    "device": FederationPage.THIS_DEVICE,
    "devices": FederationPage.DEVICES,
    "services": FederationPage.SERVICES,
    "benchmarks": FederationPage.BENCHMARKS,
    "storage": FederationPage.STORAGE,
    "jobs": FederationPage.JOBS,
    "activity": FederationPage.ACTIVITY,
    "settings": FederationPage.SETTINGS,
}


def _safe_projection(page: FederationPage) -> dict[str, object]:
    try:
        service = get_federation_projection_service()
        return assert_public_projection(
            service.project(page, include_technical=False).to_dict()
        )
    except Exception as exc:  # noqa: BLE001 - never expose projection failures
        current_app.logger.warning(
            "Federation %s projection unavailable (%s)",
            page.value,
            type(exc).__name__,
        )
        fallback = FederationProjectionService(ProjectionAdapters())
        return assert_public_projection(fallback.project(page).to_dict())


def _page_response(page: FederationPage) -> Response:
    template = (
        "federation_overview.html"
        if page is FederationPage.OVERVIEW
        else "federation/detail_page.html"
    )
    projection = _safe_projection(page)
    response = make_response(
        render_template(
            template,
            federation_overview=projection,
            federation_page=projection,
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
    return _page_response(FederationPage.OVERVIEW)


@federation_web.get("/federation/<page_name>", strict_slashes=False)
def detail(page_name: str) -> Response:
    page = _PAGE_BY_PATH.get(page_name)
    if page is None:
        abort(404)
    if request.query_string:
        return redirect(
            url_for("federation_web.detail", page_name=page_name)
        )
    return _page_response(page)


__all__ = ["federation_web"]
