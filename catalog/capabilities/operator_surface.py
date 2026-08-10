"""Safe F8.5 operator projection over existing provider authorities.

This module is deliberately authority-neutral. It reads coordinator discovery,
F8.1 enrollment, F8.2 health, F8.3 remote-AI eligibility, and F8.4 compute
compatibility. State-changing commands are delegated to F8.1 and never mutate
health, inventory, workers, jobs, artifacts, or storage directly.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar, Protocol

from catalog.federation.errors import (
    AuthorizationError,
    FederationOperationError,
    FederationValidationError,
    ProtocolCompatibilityError,
)
from catalog.federation.models import CapabilityAnnouncement
from catalog.federation.redaction import (
    contains_nonpublic_location,
    contains_secret_material,
)

from .provider_enrollment import (
    FederatedProviderEnrollmentService,
    ProviderEnrollmentRecord,
    ProviderEnrollmentState,
)
from .provider_health import (
    FederatedProviderHealthService,
    ProviderHealthObservation,
    ProviderHealthState,
)

PROVIDER_OPERATOR_VIEW_SCHEMA = "fcp.provider-operator-view.v1"
PROVIDER_OPERATOR_SNAPSHOT_SCHEMA = "fcp.provider-operator-snapshot.v1"
MAX_OPERATOR_REASON_BYTES = 128
MAX_OPERATOR_COMMAND_ID_BYTES = 256

_LOGICAL_ID = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _text(value: Any, field: str, *, maximum: int = 512) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 for character in value)
    ):
        raise FederationValidationError(
            "invalid-operator-text",
            field,
            "must be non-empty trimmed text without control characters",
        )
    if len(value.encode("utf-8")) > maximum:
        raise FederationValidationError(
            "operator-text-too-large",
            field,
            f"must not exceed {maximum} UTF-8 bytes",
        )
    return value


def _logical_id(value: Any, field: str) -> str:
    value = _text(value, field)
    if _LOGICAL_ID.fullmatch(value) is None:
        raise FederationValidationError(
            "invalid-operator-logical-id",
            field,
            "must be lowercase logical text separated by single hyphens",
        )
    return value


def _reason(value: Any, field: str = "reason_code") -> str:
    value = _logical_id(value, field)
    if len(value.encode("utf-8")) > MAX_OPERATOR_REASON_BYTES:
        raise FederationValidationError(
            "operator-reason-too-large",
            field,
            f"must not exceed {MAX_OPERATOR_REASON_BYTES} UTF-8 bytes",
        )
    if contains_secret_material(value) or contains_nonpublic_location(value):
        raise FederationValidationError(
            "unsafe-operator-reason",
            field,
            "must not contain secrets, endpoints, addresses, or backend paths",
        )
    return value


def _utc(value: datetime, field: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
        or value.utcoffset().total_seconds() != 0
    ):
        raise FederationValidationError(
            "invalid-operator-timestamp",
            field,
            "must be a UTC timestamp",
        )
    return value.astimezone(timezone.utc)


def _stamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _utc(value, "timestamp").isoformat().replace("+00:00", "Z")


def _safe_error_code(exc: BaseException, fallback: str) -> str:
    code = getattr(exc, "code", fallback)
    if not isinstance(code, str) or _LOGICAL_ID.fullmatch(code) is None:
        return fallback
    return code


class ProviderOperatorAction(str, Enum):
    REQUEST = "request"
    APPROVE = "approve"
    SUSPEND = "suspend"
    REVOKE = "revoke"
    RECONCILE = "reconcile"


class ProviderActivationState(str, Enum):
    ELIGIBLE = "eligible"
    UNAVAILABLE = "unavailable"
    INCOMPATIBLE = "incompatible"
    NOT_APPLICABLE = "not-applicable"


class ProviderEligibilityAuthority(Protocol):
    def current_snapshot(self, capability_id: str, **kwargs: Any) -> Any: ...


@dataclass(frozen=True)
class ProviderOperatorSnapshot:
    """One safe operator row with no provider properties or implementation data."""

    SCHEMA: ClassVar[str] = PROVIDER_OPERATOR_SNAPSHOT_SCHEMA

    session_id: str
    capability_id: str
    node_id: str
    capability_type: str
    protocol: str
    protocol_version: str
    discovered: bool
    announcement_status: str
    announced_at: datetime | None
    enrollment_state: str
    enrollment_revision: int | None
    enrollment_reason_code: str
    health_state: str
    health_reason_code: str
    provider_status: str | None
    provider_generation: int | None
    report_revision: int | None
    reported_at: datetime | None
    expires_at: datetime | None
    max_concurrent_jobs: int | None
    active_jobs: int | None
    queue_depth: int | None
    utilization_millis: int | None
    activation_state: ProviderActivationState
    activation_reason_code: str
    inventory_compatible: bool | None
    can_manage: bool
    allowed_actions: tuple[ProviderOperatorAction, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "session_id": self.session_id,
            "capability_id": self.capability_id,
            "node_id": self.node_id,
            "capability_type": self.capability_type,
            "protocol": self.protocol,
            "protocol_version": self.protocol_version,
            "discovered": self.discovered,
            "announcement_status": self.announcement_status,
            "announced_at": _stamp(self.announced_at),
            "enrollment_state": self.enrollment_state,
            "enrollment_revision": self.enrollment_revision,
            "enrollment_reason_code": self.enrollment_reason_code,
            "health_state": self.health_state,
            "health_reason_code": self.health_reason_code,
            "provider_status": self.provider_status,
            "provider_generation": self.provider_generation,
            "report_revision": self.report_revision,
            "reported_at": _stamp(self.reported_at),
            "expires_at": _stamp(self.expires_at),
            "max_concurrent_jobs": self.max_concurrent_jobs,
            "active_jobs": self.active_jobs,
            "queue_depth": self.queue_depth,
            "utilization_millis": self.utilization_millis,
            "activation_state": self.activation_state.value,
            "activation_reason_code": self.activation_reason_code,
            "inventory_compatible": self.inventory_compatible,
            "can_manage": self.can_manage,
            "allowed_actions": tuple(action.value for action in self.allowed_actions),
        }


@dataclass(frozen=True)
class ProviderOperatorView:
    """Authorized, deterministic F8.5 projection for one session and actor."""

    SCHEMA: ClassVar[str] = PROVIDER_OPERATOR_VIEW_SCHEMA

    session_id: str
    actor_node_id: str
    is_owner: bool
    generated_at: datetime
    providers: tuple[ProviderOperatorSnapshot, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.SCHEMA,
            "session_id": self.session_id,
            "actor_node_id": self.actor_node_id,
            "is_owner": self.is_owner,
            "generated_at": _stamp(self.generated_at),
            "providers": [provider.to_dict() for provider in self.providers],
        }


class ProviderOperatorSurface:
    """Bind one trusted server-side actor/session to safe status and F8.1 controls."""

    def __init__(
        self,
        enrollment: FederatedProviderEnrollmentService,
        health: FederatedProviderHealthService,
        *,
        session_id: str,
        actor_node_id: str,
        ai_authority: ProviderEligibilityAuthority | None = None,
        compute_authority: ProviderEligibilityAuthority | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if not isinstance(enrollment, FederatedProviderEnrollmentService):
            raise FederationValidationError(
                "invalid-operator-enrollment-service",
                "enrollment",
                "must be a FederatedProviderEnrollmentService",
            )
        if not isinstance(health, FederatedProviderHealthService):
            raise FederationValidationError(
                "invalid-operator-health-service",
                "health",
                "must be a FederatedProviderHealthService",
            )
        self.enrollment = enrollment
        self.health = health
        self.session_id = _text(session_id, "session_id")
        self.actor_node_id = _text(actor_node_id, "actor_node_id")
        self.ai_authority = ai_authority
        self.compute_authority = compute_authority
        self._clock = clock

    def now(self) -> datetime:
        return _utc(self._clock(), "clock")

    def _authorized_context(self) -> tuple[tuple[CapabilityAnnouncement, ...], bool]:
        announcements = self.enrollment.discover(
            session_id=self.session_id,
            actor_node_id=self.actor_node_id,
        )
        session = self.enrollment.coordinator.store.get_session(self.session_id)
        if session is None:
            raise AuthorizationError(
                "unknown-session",
                "target session does not exist",
                "session_id",
            )
        return announcements, session.created_by_node_id == self.actor_node_id

    @staticmethod
    def _allowed_actions(
        *,
        is_owner: bool,
        announcement: CapabilityAnnouncement | None,
        enrollment: ProviderEnrollmentRecord | None,
    ) -> tuple[ProviderOperatorAction, ...]:
        if not is_owner:
            return ()
        if enrollment is None:
            return (
                (ProviderOperatorAction.REQUEST,)
                if announcement is not None
                else ()
            )
        if enrollment.state is ProviderEnrollmentState.REVOKED:
            return (
                (ProviderOperatorAction.RECONCILE,)
                if announcement is not None
                else ()
            )
        actions: list[ProviderOperatorAction] = []
        if (
            announcement is not None
            and enrollment.state
            in {ProviderEnrollmentState.PENDING, ProviderEnrollmentState.SUSPENDED}
        ):
            actions.append(ProviderOperatorAction.APPROVE)
        if enrollment.state in {
            ProviderEnrollmentState.PENDING,
            ProviderEnrollmentState.APPROVED,
        }:
            actions.append(ProviderOperatorAction.SUSPEND)
        actions.append(ProviderOperatorAction.REVOKE)
        if announcement is not None:
            actions.append(ProviderOperatorAction.RECONCILE)
        return tuple(actions)

    def _health_observation(self, capability_id: str) -> ProviderHealthObservation:
        try:
            return self.health.observe(
                session_id=self.session_id,
                capability_id=capability_id,
                actor_node_id=self.actor_node_id,
            )
        except (
            AuthorizationError,
            FederationOperationError,
            FederationValidationError,
            ProtocolCompatibilityError,
        ) as exc:
            return ProviderHealthObservation(
                session_id=self.session_id,
                capability_id=capability_id,
                state=ProviderHealthState.ABSENT,
                reason_code=_safe_error_code(exc, "health-unavailable"),
                evaluated_at=self.now(),
            )

    def _activation(
        self,
        *,
        capability_id: str,
        capability_type: str,
        health_state: ProviderHealthState,
        enrollment: ProviderEnrollmentRecord | None,
    ) -> tuple[ProviderActivationState, str, bool | None]:
        if enrollment is None:
            return ProviderActivationState.UNAVAILABLE, "enrollment-not-requested", None
        if enrollment.state is not ProviderEnrollmentState.APPROVED:
            return (
                ProviderActivationState.UNAVAILABLE,
                f"enrollment-{enrollment.state.value}",
                None,
            )
        if health_state is not ProviderHealthState.CURRENT:
            return ProviderActivationState.UNAVAILABLE, "provider-health-not-current", None

        authority = (
            self.ai_authority
            if capability_type == "language-model"
            else self.compute_authority
        )
        if authority is None:
            reason = (
                "remote-ai-authority-unavailable"
                if capability_type == "language-model"
                else "compute-authority-unavailable"
            )
            return ProviderActivationState.UNAVAILABLE, reason, None
        try:
            authority.current_snapshot(capability_id)
        except (
            AuthorizationError,
            FederationOperationError,
            FederationValidationError,
            ProtocolCompatibilityError,
        ) as exc:
            code = _safe_error_code(exc, "activation-incompatible")
            state = (
                ProviderActivationState.UNAVAILABLE
                if code
                in {
                    "compute-provider-not-local",
                    "remote-provider-not-current",
                    "compute-provider-not-current",
                }
                else ProviderActivationState.INCOMPATIBLE
            )
            return state, code, False if capability_type != "language-model" else None
        return (
            ProviderActivationState.ELIGIBLE,
            (
                "remote-ai-eligible"
                if capability_type == "language-model"
                else "compute-handler-compatible"
            ),
            None if capability_type == "language-model" else True,
        )

    def _snapshot(
        self,
        *,
        capability_id: str,
        announcement: CapabilityAnnouncement | None,
        enrollment: ProviderEnrollmentRecord | None,
        is_owner: bool,
    ) -> ProviderOperatorSnapshot:
        identity = announcement or enrollment
        if identity is None:  # pragma: no cover - caller always provides one
            raise FederationOperationError(
                "provider-operator-identity-missing",
                "provider identity is unavailable",
                "capability_id",
            )
        observation = self._health_observation(capability_id)
        health_record = self.health.store.get(
            session_id=self.session_id,
            capability_id=capability_id,
        )
        report = None if health_record is None else health_record.report
        activation_state, activation_reason, inventory_compatible = self._activation(
            capability_id=capability_id,
            capability_type=(
                announcement.type
                if announcement is not None
                else enrollment.capability_type
            ),
            health_state=observation.state,
            enrollment=enrollment,
        )
        return ProviderOperatorSnapshot(
            session_id=self.session_id,
            capability_id=capability_id,
            node_id=(
                announcement.node_id if announcement is not None else enrollment.node_id
            ),
            capability_type=(
                announcement.type
                if announcement is not None
                else enrollment.capability_type
            ),
            protocol=(
                announcement.protocol
                if announcement is not None
                else enrollment.protocol
            ),
            protocol_version=(
                announcement.protocol_version
                if announcement is not None
                else enrollment.protocol_version
            ),
            discovered=announcement is not None,
            announcement_status=(
                announcement.status.value if announcement is not None else "absent"
            ),
            announced_at=(
                announcement.announced_at if announcement is not None else None
            ),
            enrollment_state=(
                enrollment.state.value if enrollment is not None else "not-requested"
            ),
            enrollment_revision=(
                enrollment.revision if enrollment is not None else None
            ),
            enrollment_reason_code=(
                enrollment.reason_code or "none"
                if enrollment is not None
                else "enrollment-not-requested"
            ),
            health_state=observation.state.value,
            health_reason_code=observation.reason_code,
            provider_status=None if report is None else report.status.value,
            provider_generation=observation.provider_generation,
            report_revision=observation.report_revision,
            reported_at=None if report is None else report.reported_at,
            expires_at=observation.expires_at,
            max_concurrent_jobs=(
                None if report is None else report.max_concurrent_jobs
            ),
            active_jobs=None if report is None else report.active_jobs,
            queue_depth=None if report is None else report.queue_depth,
            utilization_millis=(
                None if report is None else report.utilization_millis
            ),
            activation_state=activation_state,
            activation_reason_code=activation_reason,
            inventory_compatible=inventory_compatible,
            can_manage=is_owner,
            allowed_actions=self._allowed_actions(
                is_owner=is_owner,
                announcement=announcement,
                enrollment=enrollment,
            ),
        )

    def view(self) -> ProviderOperatorView:
        announcements, is_owner = self._authorized_context()
        announcement_by_id = {
            announcement.capability_id: announcement
            for announcement in announcements
        }
        enrollment_by_id = {
            record.capability_id: record
            for record in self.enrollment.store.list_records(
                session_id=self.session_id
            )
        }
        capability_ids = sorted(set(announcement_by_id) | set(enrollment_by_id))
        providers = tuple(
            self._snapshot(
                capability_id=capability_id,
                announcement=announcement_by_id.get(capability_id),
                enrollment=enrollment_by_id.get(capability_id),
                is_owner=is_owner,
            )
            for capability_id in capability_ids
        )
        return ProviderOperatorView(
            session_id=self.session_id,
            actor_node_id=self.actor_node_id,
            is_owner=is_owner,
            generated_at=self.now(),
            providers=providers,
        )

    def get(self, capability_id: str) -> ProviderOperatorSnapshot:
        capability_id = _text(capability_id, "capability_id")
        for provider in self.view().providers:
            if provider.capability_id == capability_id:
                return provider
        raise FederationOperationError(
            "unknown-operator-provider",
            "provider is not visible in the bound session",
            "capability_id",
        )

    def execute(
        self,
        action: ProviderOperatorAction | str,
        *,
        capability_id: str,
        expected_revision: int | None = None,
        reason_code: str | None = None,
        command_id: str | None = None,
    ) -> ProviderOperatorSnapshot:
        try:
            action = ProviderOperatorAction(action)
        except (TypeError, ValueError) as exc:
            raise FederationValidationError(
                "unsupported-operator-action",
                "action",
                "unknown provider operator action",
            ) from exc
        capability_id = _text(capability_id, "capability_id")
        current = self.get(capability_id)
        if not current.can_manage or action not in current.allowed_actions:
            raise AuthorizationError(
                "provider-operator-action-not-authorized",
                "action is not allowed for the current actor and provider state",
                "action",
            )
        command_id = (
            f"operator-{action.value}-{uuid.uuid4().hex}"
            if command_id is None
            else _text(
                command_id,
                "command_id",
                maximum=MAX_OPERATOR_COMMAND_ID_BYTES,
            )
        )
        common = {
            "session_id": self.session_id,
            "capability_id": capability_id,
            "actor_node_id": self.actor_node_id,
            "command_id": command_id,
        }
        if action is ProviderOperatorAction.REQUEST:
            self.enrollment.request(**common)
        else:
            if (
                isinstance(expected_revision, bool)
                or not isinstance(expected_revision, int)
                or expected_revision <= 0
            ):
                raise FederationValidationError(
                    "invalid-expected-revision",
                    "expected_revision",
                    "must be a positive integer for this action",
                )
            decision = {**common, "expected_revision": expected_revision}
            if action is ProviderOperatorAction.APPROVE:
                self.enrollment.approve(**decision)
            elif action is ProviderOperatorAction.SUSPEND:
                self.enrollment.suspend(
                    **decision,
                    reason_code=_reason(reason_code or "operator-suspended"),
                )
            elif action is ProviderOperatorAction.REVOKE:
                self.enrollment.revoke(
                    **decision,
                    reason_code=_reason(reason_code or "operator-revoked"),
                )
            else:
                self.enrollment.reconcile(**decision)
        return self.get(capability_id)


__all__ = [
    "PROVIDER_OPERATOR_SNAPSHOT_SCHEMA",
    "PROVIDER_OPERATOR_VIEW_SCHEMA",
    "ProviderActivationState",
    "ProviderOperatorAction",
    "ProviderOperatorSnapshot",
    "ProviderOperatorSurface",
    "ProviderOperatorView",
]
