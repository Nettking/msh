"""Compose safe CF6 projections from existing authorized read-only services."""

from __future__ import annotations

from collections.abc import Iterable
from types import SimpleNamespace
from typing import Any

from flask import current_app

from catalog.capabilities.operator_surface import ProviderOperatorSurface
from catalog.federation.errors import FederationOperationError
from catalog.federation.onboarding_compat import federation_id_from_session_id
from catalog.federation.projections import (
    BenchmarkResultsAdapter,
    BenchmarkSnapshot,
    FederationAuthorityAdapter,
    FederationProjectionService,
    JobAuthorityAdapter,
    JobAuthoritySnapshot,
    OnboardingContractsAdapter,
    ProjectionAdapters,
    ProviderOperatorAdapter,
    ProviderSnapshot,
    StorageAuthorityAdapter,
    StorageAuthoritySnapshot,
)

from .capability_benchmark_service import get_capability_benchmark_service
from .capability_contribution_service import get_capability_contribution_service
from .capability_inspection_service import get_capability_inspection_service
from .capability_onboarding_service import (
    AuthorizedOnboardingContext,
    get_capability_onboarding_service,
)
from .federation_device_projection import CapabilityFirstFederationDeviceAdapter
from .upload_analysis_job_service import get_upload_analysis_job_service

_OPERATOR_SURFACE_CONFIG_KEY = "PROVIDER_OPERATOR_SURFACE"
_INSPECTION_CONFIG_KEY = "FEDERATION_DEVICE_INSPECTION"
_CANDIDATES_CONFIG_KEY = "FEDERATION_CONTRIBUTION_CANDIDATES"
_INTENTS_CONFIG_KEY = "FEDERATION_CONTRIBUTION_INTENTS"
_BENCHMARK_STORE_CONFIG_KEY = "FEDERATION_AUTHORIZED_BENCHMARK_STORE"
_STORAGE_STORE_CONFIG_KEY = "FEDERATION_STORAGE_AUTHORITY_STORE"
_JOB_SUPPLIER_CONFIG_KEY = "FEDERATION_AUTHORIZED_JOB_SNAPSHOT_SUPPLIER"

_EXPECTED_EMPTY_CONTRIBUTION_CODES = {
    "contribution-federation-required",
    "contribution-inspection-required",
    "contribution-inspection-expired",
}


class _AuthorizedProviderView:
    """Reuse one already-authorized operator view without re-reading authority."""

    def __init__(self, view: object) -> None:
        self._view = view

    def view(self) -> object:
        return self._view


class _StaticSnapshotAdapter:
    """Expose one already-safe immutable snapshot through the adapter protocol."""

    def __init__(self, snapshot: object) -> None:
        self._snapshot = snapshot

    def snapshot(self) -> object:
        return self._snapshot


def _private_binding_text(value: object) -> str | None:
    if not isinstance(value, str) or not value or value != value.strip():
        return None
    return value


def _configured_items(key: str) -> tuple[object, ...]:
    value = current_app.config.get(key, ())
    if value is None or isinstance(value, (str, bytes, dict)):
        return ()
    if not isinstance(value, Iterable):
        return ()
    return tuple(value)


def _empty_service() -> FederationProjectionService:
    return FederationProjectionService(ProjectionAdapters())


def _onboarding_context() -> AuthorizedOnboardingContext | None:
    try:
        return get_capability_onboarding_service().authorized_context()
    except Exception:  # noqa: BLE001 - projection authorization fails closed
        return None


def _warn_projection(name: str, exc: Exception) -> None:
    current_app.logger.warning(
        "%s projection unavailable (%s)",
        name,
        type(exc).__name__,
    )


def _inspection_state() -> tuple[object | None, bool]:
    if _INSPECTION_CONFIG_KEY in current_app.config:
        return current_app.config.get(_INSPECTION_CONFIG_KEY), False
    try:
        return get_capability_inspection_service().load(), False
    except Exception as exc:  # noqa: BLE001 - preserve binding and degrade safely
        _warn_projection("Federation inspection", exc)
        return None, True


def _benchmark_adapter(*, inspection_failed: bool) -> object:
    if inspection_failed:
        return _StaticSnapshotAdapter(
            BenchmarkSnapshot(False, "inspection-projection-failed")
        )
    if _BENCHMARK_STORE_CONFIG_KEY in current_app.config:
        store = current_app.config.get(_BENCHMARK_STORE_CONFIG_KEY)
        if store is None:
            return _StaticSnapshotAdapter(BenchmarkSnapshot(True, "not-configured"))
        return BenchmarkResultsAdapter(store)
    try:
        return BenchmarkResultsAdapter(get_capability_benchmark_service())
    except Exception as exc:  # noqa: BLE001 - keep the Federation binding visible
        _warn_projection("Federation benchmark", exc)
        return _StaticSnapshotAdapter(
            BenchmarkSnapshot(False, "benchmark-projection-failed")
        )


def _contribution_state() -> tuple[tuple[object, ...], tuple[object, ...], bool]:
    if (
        _CANDIDATES_CONFIG_KEY in current_app.config
        or _INTENTS_CONFIG_KEY in current_app.config
    ):
        return (
            _configured_items(_CANDIDATES_CONFIG_KEY),
            _configured_items(_INTENTS_CONFIG_KEY),
            False,
        )
    try:
        service = get_capability_contribution_service()
        candidates = service.recommend(require_benchmark_review=False)
        intents = service.intents()
        return tuple(candidates), tuple(intents), False
    except FederationOperationError as exc:
        if getattr(exc, "code", None) in _EXPECTED_EMPTY_CONTRIBUTION_CODES:
            return (), (), False
        _warn_projection("Federation contribution", exc)
        return (), (), True
    except Exception as exc:  # noqa: BLE001 - contribution reads fail closed
        _warn_projection("Federation contribution", exc)
        return (), (), True


def _storage_adapter(internal_session_id: str) -> object:
    store = current_app.config.get(_STORAGE_STORE_CONFIG_KEY)
    if store is None:
        return _StaticSnapshotAdapter(
            StorageAuthoritySnapshot(True, "not-configured")
        )
    return StorageAuthorityAdapter(
        store,
        internal_session_id=internal_session_id,
    )


def _job_adapter(internal_session_id: str) -> object:
    supplier: Any = current_app.config.get(_JOB_SUPPLIER_CONFIG_KEY)
    if callable(supplier):
        return JobAuthorityAdapter(supplier)
    try:
        service = get_upload_analysis_job_service()
        return JobAuthorityAdapter(
            lambda: service.snapshots(internal_session_id)
        )
    except Exception as exc:  # noqa: BLE001 - job projection must fail closed
        _warn_projection("Federation jobs", exc)
        return _StaticSnapshotAdapter(
            JobAuthoritySnapshot(False, "job-projection-failed")
        )


def get_federation_projection_service() -> FederationProjectionService:
    """Build product projections from server-bound read-only authorities.

    The existing provider operator surface remains the preferred authorization
    boundary. Otherwise the durable capability-first identity and trusted
    binding are revalidated against the existing ``SessionCoordinator``. The
    inspection, benchmark and contribution projections read the same CFI-3,
    CFI-4 and CFI-5 services used by onboarding. Browser parameters are never
    accepted as actor, session, endpoint or authority context.
    """

    surface = current_app.config.get(_OPERATOR_SURFACE_CONFIG_KEY)
    binding: object | None = None
    provider_adapter: object | None = None
    coordinator: object | None = None
    internal_session_id: str | None = None
    actor_node_id: str | None = None

    try:
        if isinstance(surface, ProviderOperatorSurface):
            authorized_view = surface.view()
            internal_session_id = _private_binding_text(
                getattr(authorized_view, "session_id", None)
            )
            actor_node_id = _private_binding_text(
                getattr(authorized_view, "actor_node_id", None)
            )
            if internal_session_id is None or actor_node_id is None:
                return _empty_service()
            binding = SimpleNamespace(
                federation_id=federation_id_from_session_id(
                    internal_session_id
                ),
                device_id=actor_node_id,
                state="connected",
                trusted=True,
            )
            enrollment = getattr(surface, "enrollment", None)
            coordinator = getattr(enrollment, "coordinator", None)
            provider_adapter = ProviderOperatorAdapter(
                _AuthorizedProviderView(authorized_view)
            )
        else:
            context = _onboarding_context()
            if context is None:
                return _empty_service()
            binding = context.binding
            internal_session_id = context.binding.internal_session_id
            actor_node_id = context.credentials.identity.node_id
            coordinator = context.coordinator

        inspection, inspection_failed = _inspection_state()
        candidates, intents, contribution_failed = _contribution_state()
        if provider_adapter is None:
            provider_adapter = _StaticSnapshotAdapter(
                ProviderSnapshot(
                    not contribution_failed,
                    (
                        "current"
                        if not contribution_failed
                        else "contribution-projection-failed"
                    ),
                )
            )

        onboarding_adapter = OnboardingContractsAdapter(
            binding=binding,
            inspection=inspection,
            candidates=candidates,
            intents=intents,
        )
        federation_adapter = FederationAuthorityAdapter(
            coordinator,
            actor_node_id=actor_node_id,
            internal_session_id=internal_session_id,
        )
        adapters = ProjectionAdapters(
            onboarding=onboarding_adapter,
            providers=provider_adapter,
            benchmarks=_benchmark_adapter(
                inspection_failed=inspection_failed
            ),
            federation=CapabilityFirstFederationDeviceAdapter(
                federation_adapter,
                onboarding_adapter,
                provider_adapter,
            ),
            storage=_storage_adapter(internal_session_id),
            jobs=_job_adapter(internal_session_id),
        )
        return FederationProjectionService(adapters)
    except Exception:  # noqa: BLE001 - authorization/projection must fail closed
        return _empty_service()


__all__ = ["get_federation_projection_service"]
