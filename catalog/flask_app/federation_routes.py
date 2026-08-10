"""Federation product integration for the supported Flask app."""

from __future__ import annotations

import hmac

from flask import (
    Blueprint,
    Response,
    abort,
    current_app,
    flash,
    make_response,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from catalog.federation.projections import (
    FederationPage,
    FederationProjectionService,
    ProjectionAdapters,
    assert_public_projection,
)

from .capability_onboarding_routes import _CSRF_SESSION_KEY, _csrf_token
from .services.capability_benchmark_service import get_capability_benchmark_service
from .services.federation_projection_service import (
    get_federation_projection_service,
)
from .services.federation_update_service import get_federation_update_service

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


def _benchmark_item_actions(
    projection: dict[str, object],
) -> dict[str, dict[str, str]]:
    """Map visible local benchmark results to the existing bounded run action.

    The Federation GET surface remains read-only. This helper only exposes a
    form when the authenticated local benchmark service says the current
    benchmark/target pair is runnable. The POST itself stays owned by CFI-4 and
    revalidates CSRF, device identity, Federation context, inspection plan, and
    target availability before executing anything.
    """

    try:
        service = get_capability_benchmark_service()
        context = service.onboarding_service.authorized_context()
        snapshot = service.inspection_service.load()
        if context is None or snapshot is None:
            return {}
        device_id = context.credentials.identity.node_id
        if snapshot.device_id != device_id:
            return {}

        _summary, cards, _complete = service.view_model(
            snapshot,
            connected=True,
        )
        runnable: set[tuple[str, str]] = set()
        for card in cards:
            if not isinstance(card, dict) or not bool(card.get("can_run")):
                continue
            benchmark_id = card.get("benchmark_id")
            target_service_id = card.get("target_service_id")
            if isinstance(benchmark_id, str) and isinstance(target_service_id, str):
                runnable.add((benchmark_id, target_service_id))

        latest: dict[tuple[str, str], object] = {}
        for result in service.list_results():
            if result.device_id != device_id:
                continue
            key = (result.benchmark_id, result.target_service_id)
            if key not in runnable:
                continue
            previous = latest.get(key)
            if previous is None or (
                result.finished_at,
                result.run_id,
            ) > (
                previous.finished_at,
                previous.run_id,
            ):
                latest[key] = result

        visible_run_ids = {
            item.get("key")
            for item in projection.get("items", [])
            if isinstance(item, dict) and isinstance(item.get("key"), str)
        }
        actions: dict[str, dict[str, str]] = {}
        for (benchmark_id, target_service_id), result in latest.items():
            if result.run_id not in visible_run_ids:
                continue
            actions[result.run_id] = {
                "url": "/onboarding/benchmarks/run",
                "label": "Run again",
                "benchmark_id": benchmark_id,
                "target_service_id": target_service_id,
            }
        return actions
    except Exception as exc:  # noqa: BLE001 - controls fail closed
        current_app.logger.warning(
            "Federation benchmark actions unavailable (%s)",
            type(exc).__name__,
        )
        return {}


def _page_response(page: FederationPage) -> Response:
    template = (
        "federation_overview.html"
        if page is FederationPage.OVERVIEW
        else "federation/detail_page.html"
    )
    projection = _safe_projection(page)
    item_actions = (
        _benchmark_item_actions(projection)
        if page is FederationPage.BENCHMARKS
        else {}
    )
    update_status = None
    if page is FederationPage.OVERVIEW:
        try:
            update_status = get_federation_update_service().snapshot()
        except Exception as exc:  # noqa: BLE001 - safe passive degradation
            current_app.logger.warning(
                "Federation update status unavailable (%s)", type(exc).__name__
            )
            update_status = {"status": "unavailable", "devices": []}
    response = make_response(
        render_template(
            template,
            federation_overview=projection,
            federation_page=projection,
            federation_item_actions=item_actions,
            federation_csrf_token=(
                _csrf_token()
                if item_actions or page is FederationPage.OVERVIEW
                else None
            ),
            federation_update=update_status,
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


def _require_update_csrf() -> None:
    expected = session.get(_CSRF_SESSION_KEY)
    supplied = request.form.get("_csrf_token")
    if (
        not isinstance(expected, str)
        or not isinstance(supplied, str)
        or not hmac.compare_digest(expected, supplied)
    ):
        abort(403)


@federation_web.post("/federation/updates/check")
def check_updates() -> Response:
    _require_update_csrf()
    try:
        get_federation_update_service().check()
        flash(
            "Update check started. Reachable devices will report their locally "
            "verified source and running-build state.",
            "success",
        )
    except PermissionError:
        flash(
            "Only the authoritative Federation coordinator can check updates.",
            "error",
        )
    except Exception as exc:  # noqa: BLE001 - diagnostics remain server-side
        current_app.logger.warning(
            "Federation update check failed (%s)", type(exc).__name__
        )
        flash("The bounded update check could not be completed safely.", "error")
    return redirect(url_for("federation_web.overview"), code=303)


@federation_web.post("/federation/updates/apply")
def apply_updates() -> Response:
    _require_update_csrf()
    target = str(request.form.get("target_commit") or "")
    confirmation = request.form.get("confirm_update")
    if confirmation != "update-all":
        flash("Explicit Update all confirmation is required.", "error")
        return redirect(url_for("federation_web.overview"), code=303)
    try:
        get_federation_update_service().update_all(confirmed_target=target)
        flash(
            "Verified FCP update rollout started. A device reports success only "
            "after rebuild, restart, and running-commit verification.",
            "success",
        )
    except (PermissionError, ValueError, RuntimeError):
        flash(
            "The update was not started because its authority, target, freshness, "
            "or reachability check failed.",
            "error",
        )
    except Exception as exc:  # noqa: BLE001 - never expose process details
        current_app.logger.warning(
            "Federation update failed (%s)", type(exc).__name__
        )
        flash("The verified update rollout failed safely.", "error")
    return redirect(url_for("federation_web.overview"), code=303)


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
