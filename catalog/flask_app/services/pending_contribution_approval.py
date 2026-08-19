"""Leader-only product composition for capability-first pending contributions.

Capability-first contribution announcements use ``REGISTERING`` while the local
member waits for an existing runtime/control-plane authority. Core F8.1 provider
enrollment historically accepted only ``READY`` announcements, which made the
leader UI unable to record an explicit approval for those pending candidates.

This module keeps the F8.1 authority model and durability intact while adding one
narrow product rule: the session creator may approve an already-requested
``REGISTERING`` capability-first candidate. The resulting enrollment is APPROVED
but remains ineligible for resource binding until the member later advertises
``READY``.

Storage and logical storage-control capabilities stay entirely outside this
provider-enrollment authority. Their assignment and leadership are owned by the
separate storage control plane.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, current_app

from catalog.capabilities.operator_surface import (
    ProviderActivationState,
    ProviderOperatorAction,
    ProviderOperatorSurface,
)
from catalog.capabilities.provider_enrollment import (
    ProviderEnrollmentRecord,
    ProviderEnrollmentState,
    SQLiteProviderEnrollmentStore,
)
from catalog.capabilities.provider_health import (
    FederatedProviderHealthService,
    ProviderHealthState,
    SQLiteProviderHealthStore,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus

from .federation_active_leader_provider_runtime import (
    ActiveLeaderProviderEnrollmentService,
    ActiveLeaderProviderOperatorSurface,
)

_EXTENSION_KEY = "capability_first_provider_operator_surface"
_DEFAULT_ENROLLMENT_NAME = "provider_enrollment.sqlite3"
_DEFAULT_HEALTH_NAME = "provider_health.sqlite3"
_CAPABILITY_FIRST_KIND = "capability-first-candidate"
_STORAGE_SEPARATE_TYPES = frozenset({"storage", "storage-control"})


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError(
            "invalid-provider-approval-clock",
            "clock",
            "must be timezone-aware",
        )
    return value.astimezone(timezone.utc)


def _is_capability_first_pending(announcement: CapabilityAnnouncement) -> bool:
    properties = announcement.properties
    return (
        announcement.status is CapabilityStatus.REGISTERING
        and isinstance(properties, dict)
        and properties.get("kind") == _CAPABILITY_FIRST_KIND
    )


def _is_storage_separate(announcement: CapabilityAnnouncement) -> bool:
    return announcement.type.strip().casefold() in _STORAGE_SEPARATE_TYPES


class CapabilityFirstProviderEnrollmentService(ActiveLeaderProviderEnrollmentService):
    """F8.1 enrollment with explicit approval of capability-first REGISTERING.

    The exception is intentionally narrower than generic provider enrollment:
    unrelated REGISTERING announcements keep the frozen F8.1 READY-only approval
    rule. The core store also keeps ``eligible_for_resource_binding`` strict:
    APPROVED is not sufficient; the reconciled announcement must also be READY.

    Storage and storage-control are rejected here because generic provider
    enrollment is not an authority path for logical storage.
    """

    def request(
        self,
        *,
        session_id: str,
        capability_id: str,
        actor_node_id: str,
        command_id: str,
    ) -> ProviderEnrollmentRecord:
        # Requests remain member-owned. Only the leader may make approval
        # decisions, but a trusted member must still be able to request its own
        # ordinary provider enrollment through the existing F8.1 path.
        announcement = self._announcement(
            session_id=session_id,
            capability_id=capability_id,
            actor_node_id=actor_node_id,
        )
        if _is_storage_separate(announcement):
            raise FederationOperationError(
                "provider-enrollment-not-applicable",
                "storage authority is managed by the separate storage control plane",
                "capability_id",
            )
        return super().request(
            session_id=session_id,
            capability_id=capability_id,
            actor_node_id=actor_node_id,
            command_id=command_id,
        )

    def approve(
        self,
        *,
        session_id: str,
        capability_id: str,
        actor_node_id: str,
        command_id: str,
        expected_revision: int,
    ) -> ProviderEnrollmentRecord:
        self._require_session_owner(
            session_id=session_id,
            actor_node_id=actor_node_id,
        )
        announcement = self._announcement(
            session_id=session_id,
            capability_id=capability_id,
            actor_node_id=actor_node_id,
        )
        if _is_storage_separate(announcement):
            raise FederationOperationError(
                "provider-enrollment-not-applicable",
                "storage authority is managed by the separate storage control plane",
                "capability_id",
            )
        if not _is_capability_first_pending(announcement):
            return super().approve(
                session_id=session_id,
                capability_id=capability_id,
                actor_node_id=actor_node_id,
                command_id=command_id,
                expected_revision=expected_revision,
            )

        announcement = self.store._validated_announcement(announcement)
        now = _utc(self._clock())
        payload = {
            "session_id": announcement.session_id,
            "capability_id": announcement.capability_id,
            "announcement": announcement.to_dict(),
            "expected_revision": expected_revision,
            "target_state": ProviderEnrollmentState.APPROVED.value,
            "reason_code": "explicitly-approved",
        }
        fingerprint = self.store._command_fingerprint("approve", payload)

        with self.store.transaction() as database:
            replay = self.store._replay_command(
                database,
                actor_node_id=actor_node_id,
                command_id=command_id,
                operation="approve",
                fingerprint=fingerprint,
            )
            if replay is not None:
                return replay

            current = self.store._current(
                database,
                session_id=announcement.session_id,
                capability_id=announcement.capability_id,
            )
            if current is None:
                raise FederationOperationError(
                    "unknown-provider-enrollment",
                    "provider must be requested before a decision",
                    "capability_id",
                )
            self.store._assert_expected_revision(current, expected_revision)
            if current.state is ProviderEnrollmentState.REVOKED:
                raise FederationOperationError(
                    "provider-enrollment-revoked",
                    "revoked provider enrollment cannot be reopened",
                    "capability_id",
                )

            changes = self.store._announcement_changes(current, announcement)
            changes.update(
                {
                    "state": ProviderEnrollmentState.APPROVED,
                    "approved_by_node_id": actor_node_id,
                    "reason_code": "explicitly-approved",
                    "revision": current.revision + 1,
                    "updated_at": now,
                }
            )
            record = replace(current, **changes)
            self.store._write_record(database, record)
            self.store._record_command(
                database,
                actor_node_id=actor_node_id,
                command_id=command_id,
                operation="approve",
                fingerprint=fingerprint,
                record=record,
                now=now,
            )
            self.store._audit(
                database,
                operation="approve",
                outcome="accepted",
                reason_code="explicitly-approved",
                actor_node_id=actor_node_id,
                session_id=record.session_id,
                capability_id=record.capability_id,
                record=record,
                occurred_at=now,
            )
            return record


class CapabilityFirstProviderOperatorSurface(ActiveLeaderProviderOperatorSurface):
    """Keep storage status visible without exposing generic provider controls."""

    @staticmethod
    def _allowed_actions(
        *,
        is_owner: bool,
        announcement: CapabilityAnnouncement | None,
        enrollment: ProviderEnrollmentRecord | None,
    ) -> tuple[ProviderOperatorAction, ...]:
        capability_type = (
            announcement.type
            if announcement is not None
            else (None if enrollment is None else enrollment.capability_type)
        )
        if (
            isinstance(capability_type, str)
            and capability_type.strip().casefold() in _STORAGE_SEPARATE_TYPES
        ):
            return ()
        return ProviderOperatorSurface._allowed_actions(
            is_owner=is_owner,
            announcement=announcement,
            enrollment=enrollment,
        )

    def _activation(
        self,
        *,
        capability_id: str,
        capability_type: str,
        health_state: ProviderHealthState,
        enrollment: ProviderEnrollmentRecord | None,
    ) -> tuple[ProviderActivationState, str, bool | None]:
        normalized = capability_type.strip().casefold()
        if normalized in _STORAGE_SEPARATE_TYPES:
            return (
                ProviderActivationState.NOT_APPLICABLE,
                (
                    "storage-control-plane-separate"
                    if normalized == "storage"
                    else "logical-storage-authority-separate"
                ),
                None,
            )
        return super()._activation(
            capability_id=capability_id,
            capability_type=capability_type,
            health_state=health_state,
            enrollment=enrollment,
        )


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
