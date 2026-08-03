"""Compose safe CF6 projections from existing authorized read-only services."""

from __future__ import annotations

from collections.abc import Iterable
from types import SimpleNamespace
from typing import Any

from flask import current_app

from catalog.capabilities.operator_surface import ProviderOperatorSurface
from catalog.federation.onboarding_compat import federation_id_from_session_id
from catalog.federation.projections import (
    BenchmarkResultsAdapter,
    FederationAuthorityAdapter,
    FederationProjectionService,
    JobAuthorityAdapter,
    OnboardingContractsAdapter,
    ProjectionAdapters,
    ProviderOperatorAdapter,
    StorageAuthorityAdapter,
)

from .capability_onboarding_service import (
    AuthorizedOnboardingContext,
    get_capability_onboarding_service,
)

_OPERATOR_SURFACE_CONFIG_KEY = "PROVIDER_OPERATOR_SURFACE"
_INSPECTION_CONFIG_KEY = "FEDERATION_DEVICE_INSPECTION"
_CANDIDATES_CONFIG_KEY = "FEDERATION_CONTRIBUTION_CANDIDATES"
_INTENTS_CONFIG_KEY = "FEDERATION_CONTRIBUTION_INTENTS"
_BENCHMARK_STORE_CONFIG_KEY = "FEDERATION_AUTHORIZED_BENCHMARK_STORE"
_STORAGE_STORE_CONFIG_KEY = "FEDERATION_STORAGE_AUTHORITY_STORE"
_JOB_SUPPLIER_CONFIG_KEY = "FEDERATION_AUTHORIZED_JOB_SNAPSHOT_SUPPLIER"


class _AuthorizedProviderView:
    """Reuse one already-authorized operator view without re-reading authority."""

    def __init__(self, view: object) -> None:
        self._view = view

    def view(self) -> object:
        return self._view


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


def get_federation_projection_service() -> FederationProjectionService:
    """Build the overview projection from server-bound authorities.

    The existing provider operator surface remains the preferred authorization
    boundary. CFI-2 adds a fallback only when a durable device identity and
    trusted federation binding can be revalidated against the same existing
    ``SessionCoordinator`` authority. Request parameters are never accepted as
    actor or session context.
    """

    surface = current_app.config.get(_OPERATOR_SURFACE_CONFIG_KEY)
    binding: object | None = None
    provider_adapter: ProviderOperatorAdapter | None = None
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

        benchmark_store = current_app.config.get(_BENCHMARK_STORE_CONFIG_KEY)
        storage_store = current_app.config.get(_STORAGE_STORE_CONFIG_KEY)
        job_supplier: Any = current_app.config.get(_JOB_SUPPLIER_CONFIG_KEY)
        if not callable(job_supplier):
            job_supplier = None

        adapters = ProjectionAdapters(
            onboarding=OnboardingContractsAdapter(
                binding=binding,
                inspection=current_app.config.get(_INSPECTION_CONFIG_KEY),
                candidates=_configured_items(_CANDIDATES_CONFIG_KEY),
                intents=_configured_items(_INTENTS_CONFIG_KEY),
            ),
            providers=provider_adapter,
            benchmarks=BenchmarkResultsAdapter(benchmark_store),
            federation=FederationAuthorityAdapter(
                coordinator,
                actor_node_id=actor_node_id,
                internal_session_id=internal_session_id,
            ),
            storage=StorageAuthorityAdapter(
                storage_store,
                internal_session_id=internal_session_id,
            ),
            jobs=JobAuthorityAdapter(job_supplier),
        )
        return FederationProjectionService(adapters)
    except Exception:  # noqa: BLE001 - authorization/projection must fail closed
        return _empty_service()


__all__ = ["get_federation_projection_service"]
