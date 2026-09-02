"""Compose safe CF6 projections from existing authorized read-only services."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from types import SimpleNamespace
from typing import Any

from flask import current_app

from catalog.capabilities.operator_surface import ProviderOperatorSurface
from catalog.federation.errors import (
    AuthenticationError,
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
)
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
from .read_only_storage_authority import ReadOnlyStorageAuthorityStore
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

# Operation-level failures that mean current authoritative truth cannot be read,
# not that the persisted local identity/binding has been definitively rejected.
# Only these operation failures may fall back to read-only saved membership.
_SAVED_MEMBERSHIP_OUTAGE_CODES = {
    "authoritative-replay-incomplete",
    "onboarding-session-unavailable",
    "coordinator-unavailable",
    "relay-unavailable",
    "target-unavailable",
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


def _is_saved_membership_outage(exc: Exception) -> bool:
    """Allow display fallback only for inability to read current authority.

    A definitive authentication, authorization, or validation rejection means
    the saved binding is not current truth and must not be rendered as retained
    membership. Transport/database unavailability and explicitly bounded
    authoritative-read failures may preserve the saved identity/binding for
    display while all authority remains absent.
    """

    if isinstance(
        exc,
        (AuthenticationError, AuthorizationError, FederationValidationError),
    ):
        return False
    if isinstance(exc, FederationOperationError):
        code = str(getattr(exc, "code", ""))
        return code in _SAVED_MEMBERSHIP_OUTAGE_CODES or code.endswith("-unavailable")
    return isinstance(exc, (ConnectionError, TimeoutError, OSError, sqlite3.Error))


def _onboarding_context() -> AuthorizedOnboardingContext | None:
    try:
        return get_capability_onboarding_service().authorized_context()
    except Exception as exc:
        if _is_saved_membership_outage(exc):
            return None
        raise


def _saved_projection_binding() -> tuple[object, str] | None:
    """Recover only persisted membership metadata when live authority is down.

    This is deliberately narrower than ``authorized_context``. A saved binding
    may tell a read-only projection which trusted Federation this installation
    previously belonged to and which stable local device identity owns that
    binding. It must never be promoted into current coordinator, leader,
    provider, storage, update, or job authority.

    The identity/binding equality check prevents a stale or substituted binding
    from being displayed as this installation's membership. Any read or shape
    failure degrades to no fallback rather than guessing.
    """

    try:
        onboarding = get_capability_onboarding_service()
        credentials = onboarding.identity_or_none()
        binding = onboarding.binding_or_none()
    except Exception:  # noqa: BLE001 - persisted display state is best effort
        return None
    if credentials is None or binding is None:
        return None
    identity = getattr(credentials, "identity", None)
    actor_node_id = _private_binding_text(getattr(identity, "node_id", None))
    binding_node_id = _private_binding_text(getattr(binding, "device_id", None))
    internal_session_id = _private_binding_text(
        getattr(binding, "internal_session_id", None)
    )
    federation_id = _private_binding_text(getattr(binding, "federation_id", None))
    if (
        actor_node_id is None
        or binding_node_id != actor_node_id
        or internal_session_id is None
        or federation_id is None
        or not bool(getattr(binding, "trusted", False))
    ):
        return None
    return binding, actor_node_id


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


def _storage_adapter(internal_session_id: str, coordinator: object | None) -> object:
    # A retained local storage DB is not current write authority when the
    # coordinator that fences and renews grants is unavailable. Do not present
    # it as merely "not configured" during the exact outage the B09 surface is
    # meant to expose.
    if coordinator is None:
        return _StaticSnapshotAdapter(
            StorageAuthoritySnapshot(False, "federation-authority-unavailable")
        )

    if _STORAGE_STORE_CONFIG_KEY in current_app.config:
        store = current_app.config.get(_STORAGE_STORE_CONFIG_KEY)
        if store is None:
            return _StaticSnapshotAdapter(
                StorageAuthoritySnapshot(True, "not-configured")
            )
        return StorageAuthorityAdapter(
            store,
            internal_session_id=internal_session_id,
        )

    coordinator_store = getattr(coordinator, "store", None)
    database = getattr(coordinator_store, "database", None)
    if isinstance(database, (str, bytes)) and database:
        return StorageAuthorityAdapter(
            ReadOnlyStorageAuthorityStore(database),
            internal_session_id=internal_session_id,
        )

    return _StaticSnapshotAdapter(
        StorageAuthoritySnapshot(True, "not-configured")
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

    A provider operator surface is consumed only when it has already been
    explicitly composed. Normal Federation GETs never initialize provider
    enrollment/health authority as a side effect. Otherwise the durable
    capability-first identity and trusted binding are revalidated against the
    existing coordinator.

    If that live revalidation is unavailable, a GET may still use the persisted
    identity plus trusted binding solely to say "this known member is
    reconnecting". Definitive authentication/authorization/validation rejection
    never takes this fallback. The fallback never supplies a coordinator or any
    mutation authority; authoritative Federation/storage/job projections stay
    explicitly unavailable. Browser parameters are never accepted as actor,
    session, endpoint or authority context.
    """

    surface = current_app.config.get(_OPERATOR_SURFACE_CONFIG_KEY)
    binding: object | None = None
    provider_adapter: object | None = None
    coordinator: object | None = None
    internal_session_id: str | None = None
    actor_node_id: str | None = None
    live_authority = False

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
            live_authority = coordinator is not None
        else:
            context = _onboarding_context()
            if context is not None:
                binding = context.binding
                internal_session_id = context.binding.internal_session_id
                actor_node_id = context.credentials.identity.node_id
                coordinator = context.coordinator
                live_authority = True
            else:
                saved = _saved_projection_binding()
                if saved is None:
                    return _empty_service()
                binding, actor_node_id = saved
                internal_session_id = _private_binding_text(
                    getattr(binding, "internal_session_id", None)
                )
                if internal_session_id is None:
                    return _empty_service()
                # Deliberately no coordinator: persisted membership can explain
                # the outage, never authorize through it.
                coordinator = None
                live_authority = False

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
            storage=_storage_adapter(internal_session_id, coordinator),
            jobs=(
                _job_adapter(internal_session_id)
                if live_authority
                else _StaticSnapshotAdapter(
                    JobAuthoritySnapshot(
                        False,
                        "federation-authority-unavailable",
                    )
                )
            ),
        )
        return FederationProjectionService(adapters)
    except Exception:  # noqa: BLE001 - authorization/projection must fail closed
        return _empty_service()


__all__ = ["get_federation_projection_service"]
