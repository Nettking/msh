"""Local product composition for explicit pending provider approval."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, current_app

from catalog.capabilities.operator_surface import ProviderOperatorSurface
from catalog.capabilities.product_provider_operator import (
    CapabilityFirstProviderEnrollmentService,
    CapabilityFirstProviderOperatorSurface,
)
from catalog.capabilities.provider_enrollment import SQLiteProviderEnrollmentStore
from catalog.capabilities.provider_health import (
    FederatedProviderHealthService,
    SQLiteProviderHealthStore,
)
from catalog.federation.coordinator import SessionCoordinator

from .c03_provider_operator import (
    c03_provider_operator_surface,
    uses_c03_provider_authority,
)

_EXTENSION_KEY = "capability_first_provider_operator_surface"
_DEFAULT_ENROLLMENT_NAME = "provider_enrollment.sqlite3"
_DEFAULT_HEALTH_NAME = "provider_health.sqlite3"


def _authority_database_path(
    app: Flask,
    coordinator: SessionCoordinator,
    config_key: str,
    default_name: str,
) -> Path:
    configured = app.config.get(config_key)
    if configured:
        return Path(str(configured))
    # The supported relay already owns F8.1/F8.2 sidecar databases beside its
    # coordinator DB. Flask must reuse those exact durable stores rather than
    # create a second onboarding-local approval authority.
    return Path(coordinator.store.database).with_name(default_name)


def _selected_app(app: Flask | None) -> Flask:
    return current_app._get_current_object() if app is None else app


def _local_creator_context(
    app: Flask,
) -> tuple[object, SessionCoordinator, str, str] | None:
    """Resolve a local creator context without constructing mutation stores."""

    onboarding = app.config.get("CAPABILITY_ONBOARDING_SERVICE")
    remote_store = getattr(onboarding, "remote_store", None)
    remote_loader = getattr(remote_store, "load", None)
    if callable(remote_loader):
        try:
            if remote_loader() is not None:
                return None
        except Exception:  # noqa: BLE001 - availability fails closed
            return None

    context_loader = getattr(onboarding, "authorized_context", None)
    if not callable(context_loader):
        return None
    try:
        context = context_loader()
    except Exception:  # noqa: BLE001 - product surface must fail closed
        return None
    if context is None:
        return None

    coordinator = getattr(context, "coordinator", None)
    if not isinstance(coordinator, SessionCoordinator):
        return None
    binding = getattr(context, "binding", None)
    credentials = getattr(context, "credentials", None)
    identity = getattr(credentials, "identity", None)
    session_id = getattr(binding, "internal_session_id", None)
    actor_node_id = getattr(identity, "node_id", None)
    if not isinstance(session_id, str) or not isinstance(actor_node_id, str):
        return None
    session = coordinator.store.get_session(session_id)
    if session is None or session.created_by_node_id != actor_node_id:
        return None
    return onboarding, coordinator, session_id, actor_node_id


def local_provider_operator_available(app: Flask | None = None) -> bool:
    """Report leader approval availability without creating authority databases."""

    selected = _selected_app(app)
    if uses_c03_provider_authority(selected):
        return c03_provider_operator_surface(selected) is not None
    if isinstance(
        selected.config.get("PROVIDER_OPERATOR_SURFACE"),
        ProviderOperatorSurface,
    ):
        return True
    return _local_creator_context(selected) is not None


def _surface_paths(
    app: Flask,
    coordinator: SessionCoordinator,
    configured: ProviderOperatorSurface | None,
) -> tuple[Path, Path]:
    if configured is not None:
        enrollment_store = getattr(configured.enrollment, "store", None)
        health_store = getattr(configured.health, "store", None)
        enrollment_database = getattr(enrollment_store, "database", None)
        health_database = getattr(health_store, "database", None)
        if isinstance(enrollment_database, str) and isinstance(health_database, str):
            return Path(enrollment_database), Path(health_database)
    return (
        _authority_database_path(
            app,
            coordinator,
            "CAPABILITY_PROVIDER_ENROLLMENT_DATABASE",
            _DEFAULT_ENROLLMENT_NAME,
        ),
        _authority_database_path(
            app,
            coordinator,
            "CAPABILITY_PROVIDER_HEALTH_DATABASE",
            _DEFAULT_HEALTH_NAME,
        ),
    )


def get_local_provider_operator_surface(
    app: Flask | None = None,
) -> ProviderOperatorSurface | None:
    """Build/upgrade the durable operator surface on the local session creator.

    A previously composed generic F8.5 surface (for example from the federated AI
    bridge) is upgraded in-place semantically: the same coordinator, enrollment
    database, health database, AI authority and compute authority are retained,
    while the capability-first pending-approval rule is added. This prevents
    startup ordering from deciding whether a REGISTERING candidate can be
    reviewed.

    Remotely paired members and explicit non-owner test surfaces remain unchanged.
    """

    selected = _selected_app(app)
    if uses_c03_provider_authority(selected):
        return c03_provider_operator_surface(selected)
    configured_value = selected.config.get("PROVIDER_OPERATOR_SURFACE")
    configured = (
        configured_value
        if isinstance(configured_value, ProviderOperatorSurface)
        else None
    )
    if isinstance(configured, CapabilityFirstProviderOperatorSurface):
        return configured

    resolved = _local_creator_context(selected)
    if resolved is None:
        return configured
    onboarding, coordinator, session_id, actor_node_id = resolved

    if configured is not None and (
        configured.session_id != session_id
        or configured.actor_node_id != actor_node_id
        or configured.enrollment.coordinator.store.database
        != coordinator.store.database
    ):
        # Never transplant authority from a differently bound configured surface.
        return None

    cache = selected.extensions.get(_EXTENSION_KEY)
    cache_key = (session_id, actor_node_id, str(coordinator.store.database))
    if (
        isinstance(cache, tuple)
        and len(cache) == 2
        and cache[0] == cache_key
        and isinstance(cache[1], CapabilityFirstProviderOperatorSurface)
    ):
        selected.config["PROVIDER_OPERATOR_SURFACE"] = cache[1]
        return cache[1]

    enrollment_path, health_path = _surface_paths(
        selected,
        coordinator,
        configured,
    )
    clock = getattr(onboarding, "_clock", lambda: datetime.now(timezone.utc))
    enrollments = CapabilityFirstProviderEnrollmentService(
        coordinator,
        SQLiteProviderEnrollmentStore(enrollment_path),
        clock=clock,
    )
    health = FederatedProviderHealthService(
        enrollments,
        SQLiteProviderHealthStore(health_path),
        clock=clock,
    )
    surface = CapabilityFirstProviderOperatorSurface(
        enrollments,
        health,
        session_id=session_id,
        actor_node_id=actor_node_id,
        ai_authority=(None if configured is None else configured.ai_authority),
        compute_authority=(
            None if configured is None else configured.compute_authority
        ),
        clock=clock,
    )
    selected.extensions[_EXTENSION_KEY] = (cache_key, surface)
    selected.config["PROVIDER_OPERATOR_SURFACE"] = surface
    return surface


__all__ = [
    "CapabilityFirstProviderEnrollmentService",
    "CapabilityFirstProviderOperatorSurface",
    "get_local_provider_operator_surface",
    "local_provider_operator_available",
]
