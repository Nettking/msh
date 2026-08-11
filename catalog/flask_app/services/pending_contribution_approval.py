"""Leader-only product composition for capability-first pending contributions.

Capability-first contribution announcements use ``REGISTERING`` while the local
member waits for an existing runtime/control-plane authority. Core F8.1 provider
enrollment historically accepted only ``READY`` announcements, which made the
leader UI unable to record an explicit approval for those pending candidates.

This module keeps the F8.1 authority model and durability intact while adding one
narrow product rule: the session creator may approve an already-requested
``REGISTERING`` capability. The resulting enrollment is APPROVED but remains
ineligible for resource binding until the member later advertises ``READY``.

No storage assignment, compute activation, provider endpoint, executable, or
other capability authority is created here.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, current_app

from catalog.capabilities.operator_surface import (
    ProviderActivationState,
    ProviderOperatorSurface,
)
from catalog.capabilities.provider_enrollment import (
    FederatedProviderEnrollmentService,
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
from catalog.federation.models import CapabilityStatus

_EXTENSION_KEY = "capability_first_provider_operator_surface"
_DEFAULT_ENROLLMENT_NAME = "provider_enrollment.sqlite3"
_DEFAULT_HEALTH_NAME = "provider_health.sqlite3"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FederationValidationError(
            "invalid-provider-approval-clock",
            "clock",
            "must be timezone-aware",
        )
    return value.astimezone(timezone.utc)


class CapabilityFirstProviderEnrollmentService(FederatedProviderEnrollmentService):
    """F8.1 enrollment with explicit approval of REGISTERING candidates.

    The core store intentionally keeps ``eligible_for_resource_binding`` strict:
    APPROVED is not sufficient; the reconciled announcement must also be READY.
    """

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
        if announcement.status is not CapabilityStatus.REGISTERING:
            return super().approve(
                session_id=session_id,
                capability_id=capability_id,
                actor_node_id=actor_node_id,
                command_id=command_id,
                expected_revision=expected_revision,
            )

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


class CapabilityFirstProviderOperatorSurface(ProviderOperatorSurface):
    """Present capability-first storage approval without inventing activation."""

    def _activation(
        self,
        *,
        capability_id: str,
        capability_type: str,
        health_state: ProviderHealthState,
        enrollment: ProviderEnrollmentRecord | None,
    ) -> tuple[ProviderActivationState, str, bool | None]:
        if capability_type != "storage":
            return super()._activation(
                capability_id=capability_id,
                capability_type=capability_type,
                health_state=health_state,
                enrollment=enrollment,
            )
        if enrollment is None:
            return (
                ProviderActivationState.UNAVAILABLE,
                "enrollment-not-requested",
                None,
            )
        if enrollment.state is not ProviderEnrollmentState.APPROVED:
            return (
                ProviderActivationState.UNAVAILABLE,
                f"enrollment-{enrollment.state.value}",
                None,
            )
        if enrollment.announcement_status is not CapabilityStatus.READY:
            return (
                ProviderActivationState.UNAVAILABLE,
                "approved-waiting-for-storage-runtime",
                None,
            )
        return (
            ProviderActivationState.NOT_APPLICABLE,
            "storage-control-plane-separate",
            None,
        )


def _database_path(app: Flask, config_key: str, default_name: str) -> Path:
    configured = app.config.get(config_key)
    if configured:
        return Path(str(configured))
    onboarding = Path(str(app.config["CAPABILITY_ONBOARDING_STATE_DATABASE"]))
    return onboarding.parent / default_name


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


def get_local_provider_operator_surface(
    app: Flask | None = None,
) -> ProviderOperatorSurface | None:
    """Build the durable operator surface only on the local session creator.

    Merely rendering normal Federation pages uses
    :func:`local_provider_operator_available` and never initializes enrollment or
    health storage. The databases below are created only when the explicit
    provider-approval surface is opened or otherwise requested.
    """

    selected = _selected_app(app)
    configured = selected.config.get("PROVIDER_OPERATOR_SURFACE")
    if isinstance(configured, ProviderOperatorSurface):
        return configured

    resolved = _local_creator_context(selected)
    if resolved is None:
        return None
    onboarding, coordinator, session_id, actor_node_id = resolved

    cache = selected.extensions.get(_EXTENSION_KEY)
    cache_key = (session_id, actor_node_id, str(coordinator.store.database))
    if (
        isinstance(cache, tuple)
        and len(cache) == 2
        and cache[0] == cache_key
        and isinstance(cache[1], ProviderOperatorSurface)
    ):
        selected.config["PROVIDER_OPERATOR_SURFACE"] = cache[1]
        return cache[1]

    clock = getattr(onboarding, "_clock", lambda: datetime.now(timezone.utc))
    enrollments = CapabilityFirstProviderEnrollmentService(
        coordinator,
        SQLiteProviderEnrollmentStore(
            _database_path(
                selected,
                "CAPABILITY_PROVIDER_ENROLLMENT_DATABASE",
                _DEFAULT_ENROLLMENT_NAME,
            )
        ),
        clock=clock,
    )
    health = FederatedProviderHealthService(
        enrollments,
        SQLiteProviderHealthStore(
            _database_path(
                selected,
                "CAPABILITY_PROVIDER_HEALTH_DATABASE",
                _DEFAULT_HEALTH_NAME,
            )
        ),
        clock=clock,
    )
    surface = CapabilityFirstProviderOperatorSurface(
        enrollments,
        health,
        session_id=session_id,
        actor_node_id=actor_node_id,
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
