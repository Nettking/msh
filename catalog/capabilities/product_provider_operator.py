"""Existing explicit product provider policy, shared by local and relay composition.

These adapters keep generic creator-pinned F8.1 behavior unchanged. Storage
assignment remains a separate authority and approval never implies execution.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from catalog.capabilities.operator_surface import (
    ProviderActivationState,
    ProviderOperatorAction,
    ProviderOperatorSurface,
)
from catalog.capabilities.provider_enrollment import (
    FederatedProviderEnrollmentService,
    ProviderEnrollmentRecord,
    ProviderEnrollmentState,
)
from catalog.capabilities.provider_health import ProviderHealthState
from catalog.federation.errors import (
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus

_CAPABILITY_FIRST_KIND = "capability-first-candidate"
_STORAGE_SEPARATE_TYPES = frozenset({"storage", "storage-control"})


class ActiveLeaderProviderEnrollmentService(FederatedProviderEnrollmentService):
    """Provider enrollment whose management authority follows the current leader."""

    def _require_session_owner(
        self,
        *,
        session_id: str,
        actor_node_id: str,
    ) -> None:
        require_leader = getattr(self.coordinator, "require_session_leader", None)
        if not callable(require_leader):
            super()._require_session_owner(
                session_id=session_id,
                actor_node_id=actor_node_id,
            )
            return
        try:
            require_leader(session_id=session_id, actor_node_id=actor_node_id)
        except AuthorizationError as exc:
            if getattr(exc, "code", None) != "federation-leader-required":
                raise
            raise AuthorizationError(
                "provider-enrollment-not-authorized",
                "only the current Federation leader may manage provider enrollment in F8.1",
                "actor_node_id",
            ) from exc


class ActiveLeaderProviderOperatorSurface(ProviderOperatorSurface):
    """Operator projection whose management actions follow the current leader."""

    def _authorized_context(self) -> tuple[tuple[Any, ...], bool]:
        announcements = self.enrollment.discover(
            session_id=self.session_id,
            actor_node_id=self.actor_node_id,
        )
        coordinator = self.enrollment.coordinator
        session = coordinator.store.get_session(self.session_id)
        if session is None:
            raise AuthorizationError(
                "unknown-session",
                "target session does not exist",
                "session_id",
            )
        leadership = getattr(coordinator, "session_leadership", None)
        if not callable(leadership):
            return announcements, session.created_by_node_id == self.actor_node_id
        leader = leadership(self.session_id)
        return announcements, leader.leader_node_id == self.actor_node_id


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
