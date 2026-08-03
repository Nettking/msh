"""CFI-5 Flask composition for capability contribution intent."""

from __future__ import annotations

from collections.abc import Mapping

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
from catalog.capabilities.contributions import ContributionServiceError
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
from .services.capability_contribution_service import (
    get_capability_contribution_service,
)

capability_contribution_web = Blueprint(
    "capability_contribution_web",
    __name__,
)

_STARTUP_RECONCILE_EXTENSION_KEY = "capability_contribution_startup_reconciled"
_BASE_FIELDS = frozenset({"_csrf_token", "command_id"})
_CHOICE_PREFIX = "contribution."
_MAX_CHOICES = 128
_PREREQUISITE_CODES = frozenset(
    {
        "contribution-federation-required",
        "contribution-inspection-required",
        "contribution-inspection-expired",
        "contribution-benchmarks-required",
    }
)


def _contribution_message(exc: BaseException) -> str:
    code = getattr(exc, "code", "")
    messages = {
        "contribution-federation-required": (
            "Connect this device to a trusted federation before choosing contributions."
        ),
        "contribution-inspection-required": (
            "Run device inspection before choosing contributions."
        ),
        "contribution-inspection-expired": (
            "The device inspection expired. Inspect the device again first."
        ),
        "contribution-benchmarks-required": (
            "Run or skip the recommended benchmarks before choosing contributions."
        ),
        "contribution-device-mismatch": (
            "Saved contribution evidence does not belong to this device identity."
        ),
        "invalid-contribution-choice": (
            "One contribution choice was not recognized. Reload the page and try again."
        ),
        "invalid-contribution-composition": (
            "Contribution authority adapters are not configured safely."
        ),
        "malformed-contribution-state": (
            "Saved contribution intent is damaged and was not trusted."
        ),
    }
    if isinstance(exc, ContributionServiceError):
        return "That contribution candidate changed. Reload the page before trying again."
    return messages.get(
        code,
        "The contribution action could not be completed safely.",
    )


def _prerequisite_response(exc: FederationOperationError) -> Response | None:
    if exc.code in _PREREQUISITE_CODES:
        return _secure_response(
            f"This step remains blocked. {_contribution_message(exc)}",
            409,
        )
    return None


def _choice_payload(payload: Mapping[str, object]) -> dict[str, object]:
    unexpected = [
        key
        for key in payload
        if key not in _BASE_FIELDS and not key.startswith(_CHOICE_PREFIX)
    ]
    if unexpected:
        raise AuthorizationError(
            "onboarding-context-override-forbidden",
            "contribution context is derived from trusted server state",
            min(unexpected),
        )
    choices = {
        key.removeprefix(_CHOICE_PREFIX): value
        for key, value in payload.items()
        if key.startswith(_CHOICE_PREFIX)
    }
    if any(not candidate_id for candidate_id in choices):
        raise FederationValidationError(
            "invalid-contribution-choice",
            "candidate_id",
            "candidate identity is required",
        )
    if len(choices) > _MAX_CHOICES:
        raise FederationValidationError(
            "too-many-contribution-choices",
            "choices",
            f"must not exceed {_MAX_CHOICES} candidates",
        )
    return choices


def _validate_control_payload(payload: Mapping[str, object]) -> None:
    unexpected = set(payload) - _BASE_FIELDS
    if unexpected:
        raise AuthorizationError(
            "onboarding-context-override-forbidden",
            "contribution control context is bound by the server",
            min(unexpected),
        )


def _build_onboarding_with_contributions_view_model() -> tuple[
    dict[str, object],
    bool,
]:
    (
        view_model,
        inspection_snapshot,
        connected,
        requested_step,
        inspection_reason,
    ) = _build_onboarding_with_inspection_view_model()

    benchmark_service = get_capability_benchmark_service()
    benchmark_reason = inspection_reason
    benchmark_complete = False
    if benchmark_reason is None:
        try:
            _summary, _cards, benchmark_complete = benchmark_service.view_model(
                inspection_snapshot,
                connected=connected,
            )
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

    contribution_service = get_capability_contribution_service()
    contribution_reason = benchmark_reason
    if contribution_reason is None:
        try:
            contribution_service.apply_to_onboarding_view_model(
                view_model,
                snapshot=inspection_snapshot,
                connected=connected,
                benchmark_complete=benchmark_complete,
                requested_step=requested_step,
            )
        except (
            AuthenticationError,
            AuthorizationError,
            ContributionServiceError,
            FederationOperationError,
            FederationValidationError,
            ProtocolCompatibilityError,
            NodeIdentityStateError,
        ) as exc:
            current_app.logger.warning(
                "Capability contribution state unavailable (%s)",
                type(exc).__name__,
            )
            contribution_reason = getattr(
                exc,
                "code",
                "contribution-state-unavailable",
            )
        except Exception as exc:  # noqa: BLE001 - render must fail closed
            current_app.logger.warning(
                "Capability contribution state unavailable (%s)",
                type(exc).__name__,
            )
            contribution_reason = "contribution-state-unavailable"
    if contribution_reason is not None:
        contribution_service.apply_to_onboarding_view_model(
            view_model,
            snapshot=inspection_snapshot,
            connected=connected,
            benchmark_complete=benchmark_complete,
            requested_step=requested_step,
            error_reason=contribution_reason,
        )
    return view_model, benchmark_complete


def _render_onboarding_with_contributions() -> Response:
    view_model, _benchmark_complete = (
        _build_onboarding_with_contributions_view_model()
    )
    return _secure_response(
        render_template("onboarding.html", onboarding=view_model)
    )


@capability_contribution_web.before_app_request
def _startup_reconcile_and_dispatch() -> Response | None:
    """Reconcile explicit persisted intent once, then dispatch before CFI-4."""

    endpoint = request.endpoint or ""
    if not endpoint.startswith("static") and not current_app.extensions.get(
        _STARTUP_RECONCILE_EXTENSION_KEY
    ):
        current_app.extensions[_STARTUP_RECONCILE_EXTENSION_KEY] = True
        try:
            service = get_capability_contribution_service()
            if service.has_persisted_intents():
                service.reconcile()
        except Exception as exc:  # noqa: BLE001 - startup must remain available
            current_app.logger.info(
                "Capability contribution startup reconcile unavailable (%s)",
                type(exc).__name__,
            )

    if request.blueprint != capability_contribution_web.name:
        return None
    view = current_app.view_functions.get(endpoint)
    if view is None:
        return None
    return current_app.ensure_sync(view)(**(request.view_args or {}))


@capability_contribution_web.get("/onboarding", strict_slashes=False)
def onboarding() -> Response:
    return _render_onboarding_with_contributions()


@capability_contribution_web.post("/onboarding/contributions")
def save_contributions() -> Response:
    payload = _payload()
    try:
        choices = _choice_payload(payload)
        _validate_server_bound_request(payload, require_command=False)
        service = get_capability_contribution_service()
        service.recommend(require_benchmark_review=True)
        _validate_server_bound_request(payload, require_command=True)
        results = service.apply_choices(choices)
    except AuthorizationError as exc:
        return _safe_error_response(exc.code, 403)
    except FederationOperationError as exc:
        blocked = _prerequisite_response(exc)
        if blocked is not None:
            return blocked
        flash(_contribution_message(exc), "error")
    except (
        AuthenticationError,
        ContributionServiceError,
        FederationValidationError,
        ProtocolCompatibilityError,
        NodeIdentityStateError,
    ) as exc:
        flash(_contribution_message(exc), "error")
    except Exception as exc:  # noqa: BLE001 - never expose adapter internals
        current_app.logger.warning(
            "Capability contribution save failed safely (%s)",
            type(exc).__name__,
        )
        flash(
            "The contribution choices could not be saved safely.",
            "error",
        )
    else:
        active = sum(item.activation_state.value == "active" for item in results)
        pending = sum(item.activation_state.value == "pending" for item in results)
        flash(
            (
                f"Contribution choices saved: {active} active"
                + (f", {pending} pending approval" if pending else "")
                + "."
            ),
            "success",
        )
    return redirect(
        url_for("capability_contribution_web.onboarding", step="contributions"),
        code=303,
    )


@capability_contribution_web.post(
    "/onboarding/contributions/<candidate_id>/suspend"
)
def suspend_contribution(candidate_id: str) -> Response:
    payload = _payload()
    try:
        _validate_control_payload(payload)
        _validate_server_bound_request(payload, require_command=True)
        get_capability_contribution_service().suspend(candidate_id)
    except AuthorizationError as exc:
        return _safe_error_response(exc.code, 403)
    except FederationOperationError as exc:
        blocked = _prerequisite_response(exc)
        if blocked is not None:
            return blocked
        flash(_contribution_message(exc), "error")
    except (
        AuthenticationError,
        ContributionServiceError,
        FederationValidationError,
        ProtocolCompatibilityError,
        NodeIdentityStateError,
    ) as exc:
        flash(_contribution_message(exc), "error")
    except Exception as exc:  # noqa: BLE001 - never expose adapter internals
        current_app.logger.warning(
            "Capability contribution suspension failed safely (%s)",
            type(exc).__name__,
        )
        flash("The contribution could not be suspended safely.", "error")
    else:
        flash("Contribution suspended and future use fenced.", "success")
    return redirect(
        url_for("capability_contribution_web.onboarding", step="contributions"),
        code=303,
    )


@capability_contribution_web.post("/onboarding/contributions/reconcile")
def reconcile_contributions() -> Response:
    payload = _payload()
    try:
        _validate_control_payload(payload)
        _validate_server_bound_request(payload, require_command=True)
        results = get_capability_contribution_service().reconcile()
    except AuthorizationError as exc:
        return _safe_error_response(exc.code, 403)
    except FederationOperationError as exc:
        blocked = _prerequisite_response(exc)
        if blocked is not None:
            return blocked
        flash(_contribution_message(exc), "error")
    except (
        AuthenticationError,
        ContributionServiceError,
        FederationValidationError,
        ProtocolCompatibilityError,
        NodeIdentityStateError,
    ) as exc:
        flash(_contribution_message(exc), "error")
    except Exception as exc:  # noqa: BLE001 - never expose adapter internals
        current_app.logger.warning(
            "Capability contribution reconciliation failed safely (%s)",
            type(exc).__name__,
        )
        flash("Contribution intent could not be reconciled safely.", "error")
    else:
        flash(
            f"Reconciled {len(results)} persisted contribution choice"
            f"{'s' if len(results) != 1 else ''}.",
            "success",
        )
    return redirect(
        url_for("capability_contribution_web.onboarding", step="contributions"),
        code=303,
    )


__all__ = ["capability_contribution_web"]
