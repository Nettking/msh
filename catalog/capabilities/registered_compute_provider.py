"""Compose active registered-compute contributions into the provider runtime.

This module closes the product seam between CF4 contribution activation and the
existing provider enrollment/health/scheduling lifecycle. It deliberately does
not grant provider approval, job ownership, or execution authority: the provider
may announce itself and request enrollment, while the Federation session owner
retains approval and the health service retains live-report validation.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from catalog.federation.errors import FederationValidationError
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.federation.onboarding_models import (
    ContributionActivationState,
    ContributionCandidate,
    ContributionIntent,
)

from .provider_enrollment import (
    FederatedProviderEnrollmentService,
    ProviderEnrollmentRecord,
)
from .provider_health import FederatedProviderHealthService, ProviderHealthRecord
from .provider_reports import ProviderResourceReport, ProviderStatus
from .worker_activation import LocalComputeHandlerDescriptor


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _identifier(*parts: str, prefix: str) -> str:
    digest = hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()[:32]
    return f"{prefix}-{digest}"


@dataclass(frozen=True)
class RegisteredComputeProviderBinding:
    """Stable public provider identity derived from one registered handler."""

    session_id: str
    node_id: str
    capability_id: str
    capability_type: str
    protocol: str
    protocol_version: str
    handler_id: str
    descriptor_fingerprint: str
    inspection_revision: int
    descriptor: LocalComputeHandlerDescriptor


class RegisteredComputeProviderRuntime:
    """Bridge registered-compute contribution state to provider lifecycle state."""

    def __init__(
        self,
        *,
        enrollments: FederatedProviderEnrollmentService,
        health: FederatedProviderHealthService,
        clock: Callable[[], datetime] = _utc_now,
        report_ttl_seconds: int = 30,
    ) -> None:
        if health.enrollments is not enrollments:
            raise FederationValidationError(
                "provider-runtime-service-mismatch",
                "health",
                "health and enrollment services must share one provider lifecycle",
            )
        if (
            isinstance(report_ttl_seconds, bool)
            or not isinstance(report_ttl_seconds, int)
            or not 1 <= report_ttl_seconds <= 300
        ):
            raise FederationValidationError(
                "invalid-provider-report-ttl",
                "report_ttl_seconds",
                "must be an integer between 1 and 300 seconds",
            )
        self.enrollments = enrollments
        self.health = health
        self.coordinator = enrollments.coordinator
        self._clock = clock
        self._report_ttl_seconds = report_ttl_seconds

    @staticmethod
    def binding(
        candidate: ContributionCandidate,
        intent: ContributionIntent,
        *,
        session_id: str,
        node_id: str,
    ) -> RegisteredComputeProviderBinding:
        if candidate.capability_type != "compute":
            raise FederationValidationError(
                "registered-compute-required",
                "candidate.capability_type",
                "provider runtime composition requires a compute contribution",
            )
        if candidate.candidate_id != intent.candidate_id:
            raise FederationValidationError(
                "contribution-identity-mismatch",
                "candidate_id",
                "candidate and contribution intent must refer to the same capability",
            )
        if candidate.device_id != intent.device_id or candidate.device_id != node_id:
            raise FederationValidationError(
                "contribution-device-mismatch",
                "node_id",
                "compute contribution must belong to the announcing Federation member",
            )
        envelope = candidate.capacity_envelope
        if envelope.get("kind") != "registered-compute-handler":
            raise FederationValidationError(
                "unsupported-compute-contribution",
                "candidate.capacity_envelope.kind",
                "only registered-compute handlers may enter this provider runtime",
            )
        values: dict[str, Any] = {
            "handler_id": envelope.get("handler_id"),
            "capability_type": envelope.get("handler_capability_type"),
            "protocol_version": envelope.get("protocol_version"),
            "descriptor_fingerprint": envelope.get("descriptor_fingerprint"),
        }
        for field, value in values.items():
            if not isinstance(value, str) or not value.strip():
                raise FederationValidationError(
                    "invalid-registered-compute-metadata",
                    f"candidate.capacity_envelope.{field}",
                    "must be non-empty text",
                )
        attributes = envelope.get("handler_attributes")
        if not isinstance(attributes, dict):
            raise FederationValidationError(
                "invalid-registered-compute-metadata",
                "candidate.capacity_envelope.handler_attributes",
                "must be the registered handler attribute object",
            )
        descriptor = LocalComputeHandlerDescriptor(
            handler_id=values["handler_id"],
            capability_type=values["capability_type"],
            protocol=candidate.capability_protocol,
            protocol_version=values["protocol_version"],
            attributes=attributes,
        )
        if descriptor.descriptor_fingerprint != values["descriptor_fingerprint"]:
            raise FederationValidationError(
                "compute-handler-fingerprint-mismatch",
                "candidate.capacity_envelope.descriptor_fingerprint",
                "does not match the registered handler metadata",
            )
        capability_id = _identifier(
            session_id,
            node_id,
            descriptor.handler_id,
            descriptor.descriptor_fingerprint,
            prefix="compute-provider",
        )
        return RegisteredComputeProviderBinding(
            session_id=session_id,
            node_id=node_id,
            capability_id=capability_id,
            capability_type=descriptor.capability_type,
            protocol=descriptor.protocol,
            protocol_version=descriptor.protocol_version,
            handler_id=descriptor.handler_id,
            descriptor_fingerprint=descriptor.descriptor_fingerprint,
            inspection_revision=candidate.inspection_revision,
            descriptor=descriptor,
        )

    def _fence_superseded_bindings(
        self,
        binding: RegisteredComputeProviderBinding,
        *,
        decision_revision: int,
        decision_time: datetime,
    ) -> None:
        """Disable older public identities for the same registered handler.

        The inspection revision is the monotonic ordering authority for descriptor
        replacement. An out-of-order older candidate must never fence or resurrect
        a newer binding. Equal revisions with different descriptor identities are
        rejected as inconsistent state rather than guessed through.
        """

        announcements = self.coordinator.store.list_capabilities(
            session_id=binding.session_id,
        )
        for previous in announcements:
            if (
                previous.capability_id == binding.capability_id
                or previous.node_id != binding.node_id
                or previous.properties.get("kind") != "registered-compute-handler"
                or previous.properties.get("handler_id") != binding.handler_id
            ):
                continue
            previous_revision = previous.properties.get("inspection_revision")
            if (
                isinstance(previous_revision, bool)
                or not isinstance(previous_revision, int)
                or previous_revision <= 0
            ):
                raise FederationValidationError(
                    "unversioned-registered-compute-binding",
                    "announcement.properties.inspection_revision",
                    "cannot safely order a previous registered-compute binding",
                )
            if previous_revision > binding.inspection_revision:
                raise FederationValidationError(
                    "stale-registered-compute-binding",
                    "candidate.inspection_revision",
                    "an older compute descriptor cannot replace a newer binding",
                )
            if previous_revision == binding.inspection_revision:
                raise FederationValidationError(
                    "conflicting-registered-compute-binding",
                    "candidate.inspection_revision",
                    "one inspection revision cannot name two descriptor identities",
                )
            retirement_time = max(
                decision_time,
                previous.announced_at.astimezone(timezone.utc),
            )
            disabled = CapabilityAnnouncement(
                capability_id=previous.capability_id,
                node_id=previous.node_id,
                session_id=previous.session_id,
                type=previous.type,
                protocol=previous.protocol,
                protocol_version=previous.protocol_version,
                status=CapabilityStatus.DISABLED,
                properties=dict(previous.properties),
                announced_at=retirement_time,
            )
            retirement_id = _identifier(
                previous.capability_id,
                binding.capability_id,
                str(decision_revision),
                retirement_time.isoformat(),
                prefix="compute-provider-retirement",
            )
            self.coordinator.announce_capability(
                disabled,
                actor_node_id=binding.node_id,
                request_id=retirement_id,
            )
            if self.enrollments.store.get(
                session_id=binding.session_id,
                capability_id=previous.capability_id,
            ) is not None:
                self.enrollments.request(
                    session_id=binding.session_id,
                    capability_id=previous.capability_id,
                    actor_node_id=binding.node_id,
                    command_id=_identifier(
                        retirement_id,
                        prefix="compute-provider-retirement-enrollment",
                    ),
                )

    def reconcile_contribution(
        self,
        candidate: ContributionCandidate,
        intent: ContributionIntent,
        *,
        session_id: str,
        node_id: str,
    ) -> tuple[
        RegisteredComputeProviderBinding,
        CapabilityAnnouncement,
        ProviderEnrollmentRecord | None,
    ]:
        """Publish contribution state and request enrollment when it is active.

        Reconciliation identity is derived from the durable contribution decision,
        not wall-clock execution time. Replaying the same decision after restart
        therefore replays the same coordinator/enrollment commands instead of
        creating unbounded new mutations.

        A descriptor change is fenced before the replacement identity becomes
        READY. The provider never approves itself here. A non-active contribution
        is announced as disabled; current announcement state remains part of
        enrollment eligibility.
        """

        binding = self.binding(
            candidate,
            intent,
            session_id=session_id,
            node_id=node_id,
        )
        active = intent.activation_state is ContributionActivationState.ACTIVE
        decision_time = intent.decided_at.astimezone(timezone.utc)
        self._fence_superseded_bindings(
            binding,
            decision_revision=intent.decision_revision,
            decision_time=decision_time,
        )
        announcement = CapabilityAnnouncement(
            capability_id=binding.capability_id,
            node_id=binding.node_id,
            session_id=binding.session_id,
            type=binding.capability_type,
            protocol=binding.protocol,
            protocol_version=binding.protocol_version,
            status=CapabilityStatus.READY if active else CapabilityStatus.DISABLED,
            properties={
                "kind": "registered-compute-handler",
                "handler_id": binding.handler_id,
                "descriptor_fingerprint": binding.descriptor_fingerprint,
                "inspection_revision": binding.inspection_revision,
            },
            announced_at=decision_time,
        )
        request_id = _identifier(
            binding.capability_id,
            announcement.status.value,
            str(intent.decision_revision),
            decision_time.isoformat(),
            prefix="compute-announcement",
        )
        self.coordinator.announce_capability(
            announcement,
            actor_node_id=node_id,
            request_id=request_id,
        )
        enrollment = None
        if active:
            enrollment = self.enrollments.request(
                session_id=session_id,
                capability_id=binding.capability_id,
                actor_node_id=node_id,
                command_id=_identifier(
                    request_id,
                    prefix="compute-enrollment-request",
                ),
            )
        return binding, announcement, enrollment

    def publish_health(
        self,
        binding: RegisteredComputeProviderBinding,
        *,
        status: ProviderStatus = ProviderStatus.READY,
        report_revision: int = 0,
        provider_generation: int = 1,
        max_concurrent_jobs: int = 1,
        active_jobs: int = 0,
        queue_depth: int = 0,
        utilization_millis: int = 0,
    ) -> ProviderHealthRecord:
        """Publish one validated short-lived report for an approved binding.

        Scheduler-visible capability attributes come only from the immutable local
        handler descriptor that produced the contribution candidate. Callers may
        report live capacity/status but cannot claim a different logical contract.
        """

        now = self._clock().astimezone(timezone.utc)
        report = ProviderResourceReport(
            capability_id=binding.capability_id,
            node_id=binding.node_id,
            session_id=binding.session_id,
            capability_type=binding.capability_type,
            protocol=binding.protocol,
            protocol_version=binding.protocol_version,
            status=status,
            report_revision=report_revision,
            max_concurrent_jobs=max_concurrent_jobs,
            active_jobs=active_jobs,
            queue_depth=queue_depth,
            utilization_millis=utilization_millis,
            attributes=binding.descriptor.attributes,
            reported_at=now,
            expires_at=now + timedelta(seconds=self._report_ttl_seconds),
        )
        return self.health.publish(
            report,
            actor_node_id=binding.node_id,
            command_id=_identifier(
                binding.capability_id,
                str(provider_generation),
                str(report_revision),
                now.isoformat(),
                prefix="compute-health",
            ),
            provider_generation=provider_generation,
        )
