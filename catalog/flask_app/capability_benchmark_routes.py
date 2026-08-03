"""CFI-4 Flask composition for bounded capability benchmarks."""

from __future__ import annotations

from collections.abc import Collection

from flask import (
    Blueprint,
    Response,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    url_for,
)

from catalog.capabilities.benchmarking import BenchmarkingError
from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.node.identity import NodeIdentityStateError

from .capability_inspection_routes import (
    _build_onboarding_with_inspection_view_model,
)
from .capability_onboarding_routes import (
    _payload,
    _safe_error_response,
    _secure_response,
    _validate_server_bound_request,
)
from .services.capability_benchmark_service import (
    get_capability_benchmark_service,
)
from .services.capability_onboarding_service import (
    get_capability_onboarding_service,
)

capability_benchmark_web = Blueprint("capability_benchmark_web", __name__)

_RUN_FIELDS = frozenset({"_csrf_token", "benchmark_id", "target_service_id"})
_SKIP_FIELDS = frozenset({"_csrf_token"})
_PREREQUISITE_CODES = frozenset(
    {
        "benchmark-federation-required",
        "benchmark-inspection-required",
        "benchmark-inspection-expired",
    }
)


def _reject_unexpected_fields(
    payload: dict[str, object],
    allowed: Collection[str],
) -> None:
    unexpected = set(payload) - set(allowed)
    if unexpected:
        raise AuthorizationError(
            "onboarding-context-override-forbidden",
            "benchmark execution context is derived from trusted server state",
            min(unexpected),
        )


def _benchmark_message(exc: BaseException) -> str:
    code = getattr(exc, "code", "")
    messages = {
        "benchmark-federation-required": (
            "Connect this device to a trusted federation before running checks."
        ),
        "benchmark-inspection-required": (
            "Run device inspection before running benchmarks."
        ),
        "benchmark-inspection-expired": (
            "The device inspection expired. Inspect the device again first."
        ),
        "benchmark-device-mismatch": (
            "Saved benchmark evidence does not belong to this device identity."
        ),
        "benchmark-target-unavailable": (
            "That benchmark target changed. Inspect the device again."
        ),
        "benchmark-already-running": (
            "That benchmark target already has an active run."
        ),
        "benchmark-not-running": (
            "There is no active run to cancel for that benchmark target."
        ),
        "unknown-benchmark-target": (
            "That benchmark target is not in the current inspection plan."
        ),
        "malformed-benchmark-state": (
            "Saved benchmark evidence is damaged and was not trusted."
        ),
    }
    return messages.get(
        code,
        "The bounded benchmark action could not be completed safely.",
    )


def _prerequisite_response(exc: FederationOperationError) -> Response | None:
    if (
        exc.code in _PREREQUISITE_CODES
        and get_capability_onboarding_service().identity_or_none() is not None
    ):
        return _secure_response(
            f"This step remains blocked. {_benchmark_message(exc)}",
            409,
        )
    return None


def _render_onboarding_with_benchmarks() -> Response:
    (
        view_model,
        inspection_snapshot,
        connected,
        requested_step,
        inspection_reason,
    ) = _build_onboarding_with_inspection_view_model()
    benchmark_service = get_capability_benchmark_service()
    benchmark_reason = inspection_reason
    if benchmark_reason is None:
        try:
            benchmark_service.apply_to_onboarding_view_model(
                view_model,
                snapshot=inspection_snapshot,
                connected=connected,
                requested_step=requested_step,
            )
        except (
            BenchmarkingError,
            FederationOperationError,
            FederationValidationError,
            NodeIdentityStateError,
        ) as exc:
            current_app.logger.warning(
                "Capability benchmark state unavailable (%s)",
                type(exc).__name__,
            )
            benchmark_reason = getattr(
                exc,
                "code",
                "benchmark-state-unavailable",
            )
        except Exception as exc:  # noqa: BLE001 - render must fail closed
            current_app.logger.warning(
                "Capability benchmark state unavailable (%s)",
                type(exc).__name__,
            )
            benchmark_reason = "benchmark-state-unavailable"
    if benchmark_reason is not None:
        benchmark_service.apply_to_onboarding_view_model(
            view_model,
            snapshot=None,
            connected=connected,
            requested_step=requested_step,
            error_reason=benchmark_reason,
        )
    return _secure_response(
        render_template("onboarding.html", onboarding=view_model)
    )


@capability_benchmark_web.before_app_request
def _dispatch_before_inspection_and_legacy_gates() -> Response | None:
    """Dispatch CFI-4 routes before CFI-3 and role-first fallback routes."""

    if request.blueprint != capability_benchmark_web.name:
        return None
    endpoint = request.endpoint or ""
    view = current_app.view_functions.get(endpoint)
    if view is None:
        return None
    return current_app.ensure_sync(view)(**(request.view_args or {}))


@capability_benchmark_web.get("/onboarding", strict_slashes=False)
def onboarding() -> Response:
    return _render_onboarding_with_benchmarks()


@capability_benchmark_web.post("/onboarding/benchmarks/run")
def run_benchmark() -> Response:
    payload = _payload()
    try:
        _reject_unexpected_fields(payload, _RUN_FIELDS)
        _validate_server_bound_request(payload, require_command=False)
        result = get_capability_benchmark_service().run(
            benchmark_id=str(payload.get("benchmark_id") or ""),
            target_service_id=str(payload.get("target_service_id") or ""),
        )
    except AuthorizationError as exc:
        return _safe_error_response(exc.code, 403)
    except FederationOperationError as exc:
        blocked = _prerequisite_response(exc)
        if blocked is not None:
            return blocked
        flash(_benchmark_message(exc), "error")
    except (
        AuthenticationError,
        BenchmarkingError,
        FederationValidationError,
        ProtocolCompatibilityError,
        NodeIdentityStateError,
    ) as exc:
        flash(_benchmark_message(exc), "error")
    except Exception as exc:  # noqa: BLE001 - never expose adapter internals
        current_app.logger.warning(
            "Capability benchmark failed safely (%s)",
            type(exc).__name__,
        )
        flash(
            "The bounded benchmark action could not be completed safely.",
            "error",
        )
    else:
        flash(
            f"Benchmark completed with state {result.state.value}.",
            "success",
        )
    return redirect(
        url_for("capability_benchmark_web.onboarding", step="benchmarks"),
        code=303,
    )


@capability_benchmark_web.post("/onboarding/benchmarks/skip")
def skip_benchmarks() -> Response:
    payload = _payload()
    try:
        _reject_unexpected_fields(payload, _SKIP_FIELDS)
        _validate_server_bound_request(payload, require_command=False)
        count = get_capability_benchmark_service().skip_all()
    except AuthorizationError as exc:
        return _safe_error_response(exc.code, 403)
    except FederationOperationError as exc:
        blocked = _prerequisite_response(exc)
        if blocked is not None:
            return blocked
        flash(_benchmark_message(exc), "error")
    except (
        AuthenticationError,
        BenchmarkingError,
        FederationValidationError,
        ProtocolCompatibilityError,
        NodeIdentityStateError,
    ) as exc:
        flash(_benchmark_message(exc), "error")
    except Exception as exc:  # noqa: BLE001 - never expose adapter internals
        current_app.logger.warning(
            "Capability benchmark skip failed safely (%s)",
            type(exc).__name__,
        )
        flash(
            "Optional benchmark checks could not be skipped safely.",
            "error",
        )
    else:
        flash(
            f"{count} optional benchmark check{'s' if count != 1 else ''} skipped. "
            "No contribution was enabled.",
            "success",
        )
    return redirect(
        url_for("capability_benchmark_web.onboarding", step="benchmarks"),
        code=303,
    )


@capability_benchmark_web.post("/onboarding/benchmarks/cancel")
def cancel_benchmark() -> Response:
    payload = _payload()
    try:
        _reject_unexpected_fields(payload, _RUN_FIELDS)
        _validate_server_bound_request(payload, require_command=False)
        get_capability_benchmark_service().cancel(
            benchmark_id=str(payload.get("benchmark_id") or ""),
            target_service_id=str(payload.get("target_service_id") or ""),
        )
    except AuthorizationError as exc:
        return _safe_error_response(exc.code, 403)
    except (
        AuthenticationError,
        BenchmarkingError,
        FederationOperationError,
        FederationValidationError,
        ProtocolCompatibilityError,
        NodeIdentityStateError,
    ) as exc:
        flash(_benchmark_message(exc), "error")
    except Exception as exc:  # noqa: BLE001 - never expose adapter internals
        current_app.logger.warning(
            "Capability benchmark cancellation failed safely (%s)",
            type(exc).__name__,
        )
        flash("The benchmark could not be cancelled safely.", "error")
    else:
        flash("Benchmark cancellation requested.", "success")
    return redirect(
        url_for("capability_benchmark_web.onboarding", step="benchmarks"),
        code=303,
    )


__all__ = ["capability_benchmark_web"]
