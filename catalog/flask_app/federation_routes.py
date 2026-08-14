"""Federation product integration for the supported Flask app."""

from __future__ import annotations

import hmac
from datetime import datetime, timezone

from flask import (
    Blueprint,
    Response,
    abort,
    current_app,
    flash,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from catalog.capabilities.efficiency import learning_snapshot
from catalog.federation.errors import (
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.projections import (
    FederationPage,
    FederationProjectionService,
    ProjectionAdapters,
    assert_public_projection,
)

from .capability_onboarding_routes import _CSRF_SESSION_KEY, _csrf_token
from .services.capability_benchmark_service import get_capability_benchmark_service
from .services.capability_onboarding_service import get_capability_onboarding_service
from .services.federation_active_leader_runtime import (
    get_active_capability_request_service as get_federation_capability_request_service,
)
from .services.federation_active_leader_runtime import (
    get_active_update_service as get_federation_update_service,
)
from .services.federation_device_names import (
    FederationDeviceNamingService,
    current_federation_device_name,
    local_device_naming_available,
    local_device_self_naming_available,
)
from .services.federation_leader_authority import resolve_federation_leader
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


def _leader_authority_view() -> tuple[bool, bool, dict[str, object] | None]:
    """Return membership, active-leader control, and public-safe leadership state."""

    try:
        context = get_capability_onboarding_service().authorized_context()
        if context is None:
            return False, False, None
        authority = resolve_federation_leader(context)
        actor = context.credentials.identity.node_id
        return (
            True,
            authority.leader_node_id == actor,
            {
                "creator_node_id": authority.creator_node_id,
                "leader_node_id": authority.leader_node_id,
                "term": authority.term,
                "this_device_is_leader": authority.leader_node_id == actor,
                "creator_is_leader": (
                    authority.creator_node_id == authority.leader_node_id
                ),
            },
        )
    except Exception as exc:  # noqa: BLE001 - leader controls fail closed
        current_app.logger.warning(
            "Federation leader authority unavailable (%s)", type(exc).__name__
        )
        return False, False, None


def _update_authority_view() -> tuple[bool, bool]:
    """Compatibility helper for tests and existing route composition."""

    is_member, can_manage, _leadership = _leader_authority_view()
    return is_member, can_manage


def _page_response(page: FederationPage) -> Response:
    template = (
        "federation_overview.html"
        if page is FederationPage.OVERVIEW
        else "federation/detail_page.html"
    )
    projection = _safe_projection(page)
    item_actions = (
        _benchmark_item_actions(projection) if page is FederationPage.BENCHMARKS else {}
    )
    device_naming_available = (
        local_device_naming_available() if page is FederationPage.DEVICES else False
    )
    self_naming_available = (
        local_device_self_naming_available()
        if page is FederationPage.THIS_DEVICE
        else False
    )
    current_device_name = (
        current_federation_device_name() if page is FederationPage.THIS_DEVICE else None
    )
    update_status = None
    capability_request_status = None
    leadership_status = None
    if page is FederationPage.OVERVIEW:
        try:
            update_status = get_federation_update_service().snapshot()
        except Exception as exc:  # noqa: BLE001 - safe passive degradation
            current_app.logger.warning(
                "Federation update status unavailable (%s)", type(exc).__name__
            )
            update_status = {"status": "unavailable", "devices": []}
        try:
            capability_request_status = (
                get_federation_capability_request_service().snapshot()
            )
        except Exception as exc:  # noqa: BLE001 - safe passive degradation
            current_app.logger.warning(
                "Federation capability request status unavailable (%s)",
                type(exc).__name__,
            )
            capability_request_status = {"status": "unavailable", "devices": []}

        is_member, can_manage_updates, leadership_status = _leader_authority_view()
        update_status = {
            **update_status,
            "can_manage": can_manage_updates,
            "managed_by_coordinator": is_member and not can_manage_updates,
        }
        capability_request_status = {
            **capability_request_status,
            "can_manage": can_manage_updates,
            "managed_by_coordinator": is_member and not can_manage_updates,
        }
        if is_member and not can_manage_updates:
            update_status = {
                **update_status,
                "status": "managed_by_coordinator",
                "eligible_count": 0,
            }
            capability_request_status = {
                **capability_request_status,
                "status": "managed_by_coordinator",
            }

    response = make_response(
        render_template(
            template,
            federation_overview=projection,
            federation_page=projection,
            federation_item_actions=item_actions,
            federation_device_naming_available=device_naming_available,
            federation_device_self_naming_available=self_naming_available,
            federation_current_device_name=current_device_name,
            federation_csrf_token=(
                _csrf_token()
                if item_actions
                or page is FederationPage.OVERVIEW
                or device_naming_available
                or self_naming_available
                else None
            ),
            federation_update=update_status,
            federation_capability_request=capability_request_status,
            federation_leadership=leadership_status,
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

    if request.blueprint != federation_web.name or request.method not in {
        "GET",
        "HEAD",
    }:
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


@federation_web.post("/federation/device/name")
def rename_this_device() -> Response:
    _require_update_csrf()
    display_name = request.form.get("display_name")
    try:
        name = FederationDeviceNamingService().rename_self(display_name)
        flash(f"This device is now named {name} across the Federation.", "success")
    except AuthorizationError:
        flash(
            "A trusted Federation membership is required to name this device.", "error"
        )
    except FederationValidationError as exc:
        code = getattr(exc, "code", "invalid-device-name")
        flash(code.replace("-", " ").capitalize(), "error")
    except FederationOperationError:
        flash("This Federation device name could not be published.", "error")
    except Exception as exc:  # noqa: BLE001 - diagnostics remain server-side
        current_app.logger.warning(
            "Federation self device rename failed (%s)", type(exc).__name__
        )
        flash("This Federation device name could not be saved safely.", "error")
    return redirect(url_for("federation_web.detail", page_name="device"), code=303)


@federation_web.post("/federation/devices/name")
def rename_device() -> Response:
    _require_update_csrf()
    target_node_id = str(request.form.get("target_node_id") or "")
    display_name = request.form.get("display_name")
    try:
        name = FederationDeviceNamingService().rename(
            target_node_id=target_node_id,
            display_name=display_name,
        )
        flash(f"Federation device name saved as {name}.", "success")
    except AuthorizationError:
        flash("Only the Federation leader can name another device.", "error")
    except FederationValidationError as exc:
        code = getattr(exc, "code", "invalid-device-name")
        flash(code.replace("-", " ").capitalize(), "error")
    except FederationOperationError:
        flash("The selected Federation device could not be renamed.", "error")
    except Exception as exc:  # noqa: BLE001 - diagnostics remain server-side
        current_app.logger.warning(
            "Federation device rename failed (%s)", type(exc).__name__
        )
        flash("The Federation device name could not be saved safely.", "error")
    return redirect(url_for("federation_web.detail", page_name="devices"), code=303)


@federation_web.post("/federation/capabilities/request")
def request_member_capabilities() -> Response:
    _require_update_csrf()
    try:
        result = get_federation_capability_request_service().request_all()
        expected = result.get("expected_report_node_ids", [])
        requested_count = len(expected) if isinstance(expected, list) else 0
        if requested_count:
            flash(
                f"Asked {requested_count} reachable Federation member"
                f"{'s' if requested_count != 1 else ''} to benchmark local "
                "capabilities and contribute services allowed by their local policy.",
                "success",
            )
        else:
            flash(
                "No reachable remote Federation members were available for a "
                "capability request.",
                "error",
            )
    except PermissionError:
        flash(
            "Only the Federation leader can request member benchmarks and contributions.",
            "error",
        )
    except Exception as exc:  # noqa: BLE001 - diagnostics remain server-side
        current_app.logger.warning(
            "Federation capability request failed (%s)", type(exc).__name__
        )
        flash("The bounded capability request could not be started safely.", "error")
    return redirect(url_for("federation_web.overview"), code=303)


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
            "Only the current Federation leader can check updates.",
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
        current_app.logger.warning("Federation update failed (%s)", type(exc).__name__)
        flash("The verified update rollout failed safely.", "error")
    return redirect(url_for("federation_web.overview"), code=303)


#: Optional explicit application binding for tests/alternate compositions.
EFFICIENCY_STORE_CONFIG_KEY = "FEDERATION_EFFICIENCY_LEARNING_STORE"


def _efficiency_learning_store():
    """Resolve the store for the live authenticated federation session.

    Tests and alternate products may still bind a store explicitly through
    ``EFFICIENCY_STORE_CONFIG_KEY``. In the supported product, the durable
    analysis runtime owns the session-scoped learning store, so the route reads
    that exact store rather than a parallel database.
    """

    configured = current_app.config.get(EFFICIENCY_STORE_CONFIG_KEY)
    if configured is not None:
        return configured
    try:
        context = get_capability_onboarding_service().authorized_context()
        if context is None:
            return None
        binding = getattr(context, "binding", None)
        credentials = getattr(context, "credentials", None)
        identity = getattr(credentials, "identity", None)
        session_id = getattr(binding, "internal_session_id", None)
        node_id = getattr(identity, "node_id", None)
        if not isinstance(session_id, str) or not isinstance(node_id, str):
            return None

        # Lazy import avoids coupling Flask route import order to orchestrator
        # composition and creates no second runtime: get_analysis_runtime is the
        # existing singleton/rebind boundary used by analysis scheduling.
        from catalog.orchestrator.analysis_runtime import get_analysis_runtime

        runtime = get_analysis_runtime()
        if runtime.identity.session_id != session_id or runtime.identity.node_id != node_id:
            return None
        return runtime.efficiency.store
    except Exception as exc:  # noqa: BLE001 - observability must fail safely
        current_app.logger.warning(
            "Federation efficiency learning unavailable (%s)", type(exc).__name__
        )
        return None


@federation_web.get("/federation/efficiency.json", strict_slashes=False)
def efficiency_learning() -> Response:
    """Read-only view of what the Federation has learned about execution."""

    snapshot = learning_snapshot(
        _efficiency_learning_store(),
        now=datetime.now(timezone.utc),
    )
    response = jsonify(snapshot)
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@federation_web.get("/federation/<page_name>", strict_slashes=False)
def detail(page_name: str) -> Response:
    page = _PAGE_BY_PATH.get(page_name)
    if page is None:
        abort(404)
    if request.query_string:
        return redirect(url_for("federation_web.detail", page_name=page_name))
    return _page_response(page)


__all__ = ["EFFICIENCY_STORE_CONFIG_KEY", "federation_web"]
