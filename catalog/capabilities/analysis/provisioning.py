"""Make JSONL analysis a trusted, preinstalled F8 compute capability.

The node advertises this capability only when the trusted handler is genuinely
installed locally, and only through the existing F8 chain:

    capability announced (F8 coordinator registry)
        → enrollment requested (F8.1)
        → approval by the session's authority
        → health published (F8.2)
        → trusted activation (F8.4)

Startup never turns "the handler is installed" into "this provider is approved".
Approval is attempted only when this node is the session's own leader, which is
true for a device operating its own single-node session and false in a federation
somebody else leads. There, the enrollment stays pending until an operator
approves it, and the capability simply has no eligible provider report.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta

from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus

from ..lifecycle_worker import CancellableCapabilityWorker
from ..provider_reports import ProviderResourceReport, ProviderStatus
from ..worker_activation import (
    ActivatedComputeWorker,
    ComputeHandlerActivationReference,
    LocalComputeHandlerDescriptor,
    TrustedComputeWorkerBinder,
)
from .contracts import (
    ANALYSIS_CAPABILITY_TYPE,
    ANALYSIS_PROTOCOL,
    ANALYSIS_PROTOCOL_VERSION,
    analysis_provider_attributes,
)

#: Logical handler identity. It names a *preinstalled local implementation*, and
#: is never a module path, filename, or anything else that could be executed.
ANALYSIS_HANDLER_ID = "jsonl-analysis-handler"

DEFAULT_REPORT_TTL_SECONDS = 300
_ANALYSIS_DATA_OWNER_NODE_ID: ContextVar[str | None] = ContextVar(
    "fcp_analysis_data_owner_node_id",
    default=None,
)


def analysis_handler_descriptor(
    *, attributes: dict | None = None
) -> LocalComputeHandlerDescriptor:
    return LocalComputeHandlerDescriptor(
        handler_id=ANALYSIS_HANDLER_ID,
        capability_type=ANALYSIS_CAPABILITY_TYPE,
        protocol=ANALYSIS_PROTOCOL,
        protocol_version=ANALYSIS_PROTOCOL_VERSION,
        attributes=analysis_provider_attributes(**(attributes or {})),
    )


def analysis_capability_id(node_id: str) -> str:
    """Return this node's stable analysis capability identity.

    Deterministic so restarts rebind the same enrollment, and distinct from the
    node ID because ownership may never be granted by the provider receiving it.
    """

    digest = hashlib.sha256(f"analysis-capability\0{node_id}".encode()).hexdigest()
    return f"analysis-provider-{digest[:24]}"


def dispatched_data_owner_node_id(_job) -> str:
    """Return the authenticated coordinator that dispatched the current job.

    F7.5 authenticates the relay actor before the worker reaches the capability
    handler. The analysis worker carries that actor through a ``ContextVar`` so
    F6 artifact retrieval is routed to the actual data owner instead of assuming
    the executing provider also owns the JSONL.
    """

    value = _ANALYSIS_DATA_OWNER_NODE_ID.get()
    if not isinstance(value, str) or not value:
        raise FederationValidationError(
            "analysis-data-owner-unavailable",
            "data_owner_node_id",
            "analysis execution is missing its authenticated dispatcher identity",
        )
    return value


class AnalysisCancellableCapabilityWorker(CancellableCapabilityWorker):
    """F7.5 worker that exposes the authenticated dispatcher to analysis only."""

    async def handle(
        self,
        request,
        *,
        authenticated_actor: str,
        authenticated_session: str,
    ):
        token = _ANALYSIS_DATA_OWNER_NODE_ID.set(authenticated_actor)
        try:
            return await super().handle(
                request,
                authenticated_actor=authenticated_actor,
                authenticated_session=authenticated_session,
            )
        finally:
            _ANALYSIS_DATA_OWNER_NODE_ID.reset(token)


@dataclass(frozen=True)
class ProvisioningOutcome:
    """What the trust chain concluded for this node's analysis capability."""

    capability_id: str
    announced: bool
    enrollment_state: str
    approved_here: bool
    health_published: bool
    worker: ActivatedComputeWorker | None
    reason: str


class AnalysisProviderProvisioner:
    """Drive the existing F8 chain for the local analysis capability."""

    def __init__(
        self,
        *,
        coordinator,
        enrollments,
        health,
        inventory,
        binder: TrustedComputeWorkerBinder,
        session_id: str,
        node_id: str,
        clock: Callable[[], datetime],
        descriptor: LocalComputeHandlerDescriptor | None = None,
        max_concurrent_jobs: int = 1,
        active_jobs: Callable[[], int] | None = None,
        report_ttl_seconds: int = DEFAULT_REPORT_TTL_SECONDS,
        provider_generation: int = 1,
    ) -> None:
        if (
            isinstance(provider_generation, bool)
            or not isinstance(provider_generation, int)
            or provider_generation <= 0
        ):
            raise FederationValidationError(
                "invalid-provider-generation",
                "provider_generation",
                "must be a positive integer",
            )
        self.coordinator = coordinator
        self.enrollments = enrollments
        self.health = health
        self.inventory = inventory
        self.binder = binder
        self.session_id = session_id
        self.node_id = node_id
        self.clock = clock
        self.descriptor = descriptor or analysis_handler_descriptor()
        self.capability_id = analysis_capability_id(node_id)
        self.max_concurrent_jobs = max(int(max_concurrent_jobs), 1)
        self._active_jobs = active_jobs or (lambda: 0)
        self.report_ttl_seconds = int(report_ttl_seconds)
        self.provider_generation = provider_generation
        self._published_revision = 0
        self._endpoint = None
        self._worker: ActivatedComputeWorker | None = None

    # ------------------------------------------------------------------

    def install_handler(self, handler) -> None:
        """Record that the trusted implementation exists on this machine."""

        self.inventory.register(self.descriptor, handler)

    def announce(self) -> bool:
        try:
            self.coordinator.announce_capability(
                CapabilityAnnouncement(
                    capability_id=self.capability_id,
                    node_id=self.node_id,
                    session_id=self.session_id,
                    type=self.descriptor.capability_type,
                    protocol=self.descriptor.protocol,
                    protocol_version=self.descriptor.protocol_version,
                    status=CapabilityStatus.READY,
                    properties=self.descriptor.attributes,
                    announced_at=self.clock(),
                ),
                actor_node_id=self.node_id,
                request_id=f"announce-{self.capability_id}",
            )
        except (FederationValidationError, FederationOperationError):
            return False
        return True

    def _leads_session(self) -> bool:
        """Return whether this node is the authority for its own session."""

        try:
            self.coordinator.require_session_leader(
                session_id=self.session_id, actor_node_id=self.node_id
            )
        except Exception:  # noqa: BLE001 - not leading is an ordinary outcome
            return False
        return True

    def ensure_enrollment(self) -> tuple[str, bool]:
        """Request enrollment; approve only when this node leads the session."""

        record = self.enrollments.request(
            session_id=self.session_id,
            capability_id=self.capability_id,
            actor_node_id=self.node_id,
            command_id=f"request-{self.capability_id}",
        )
        state = str(getattr(record.state, "value", record.state))
        if state == "approved":
            return state, False
        if not self._leads_session():
            # Somebody else owns this federation session. Approval is theirs.
            return state, False
        approved = self.enrollments.approve(
            session_id=self.session_id,
            capability_id=self.capability_id,
            actor_node_id=self.node_id,
            command_id=f"approve-{self.capability_id}-{record.revision}",
            expected_revision=record.revision,
        )
        return str(getattr(approved.state, "value", approved.state)), True

    def _report(self, revision: int) -> ProviderResourceReport:
        now = self.clock()
        active = min(max(int(self._active_jobs()), 0), self.max_concurrent_jobs)
        return ProviderResourceReport(
            capability_id=self.capability_id,
            node_id=self.node_id,
            session_id=self.session_id,
            capability_type=self.descriptor.capability_type,
            protocol=self.descriptor.protocol,
            protocol_version=self.descriptor.protocol_version,
            status=ProviderStatus.READY,
            report_revision=revision,
            max_concurrent_jobs=self.max_concurrent_jobs,
            active_jobs=active,
            queue_depth=0,
            utilization_millis=int(1_000 * active / self.max_concurrent_jobs),
            attributes={
                **self.descriptor.attributes,
                "activation": ComputeHandlerActivationReference(
                    self.descriptor.handler_id,
                    self.descriptor.descriptor_fingerprint,
                ).to_dict(),
            },
            reported_at=now,
            expires_at=now + timedelta(seconds=self.report_ttl_seconds),
        )

    def publish_health(self, *, force: bool = False) -> bool:
        """Publish a fresh report through F8.2 when the current one is stale.

        ``report_revision`` is durable F8.2 fencing state, not process-local
        sequence state. A restarted provisioner therefore resumes after the
        persisted current revision instead of trying revision 1 again when the
        provider generation is intentionally stable.
        """

        record = self.health.store.get(
            session_id=self.session_id, capability_id=self.capability_id
        )
        if record is not None and record.provider_generation == self.provider_generation:
            self._published_revision = max(
                self._published_revision,
                int(record.report_revision),
            )
            if not force and record.is_fresh_at(self.clock()):
                return False
        revision = self._published_revision + 1
        try:
            self.health.publish(
                self._report(revision),
                actor_node_id=self.node_id,
                command_id=f"publish-{self.capability_id}-{self.provider_generation}-{revision}",
                provider_generation=self.provider_generation,
            )
        except (FederationValidationError, FederationOperationError):
            return False
        self._published_revision = revision
        return True

    def activate(self, endpoint) -> ActivatedComputeWorker | None:
        """Bind the trusted worker through F8.4 onto the lifecycle endpoint."""

        try:
            return self.binder.activate_on(endpoint, self.capability_id)
        except (FederationValidationError, FederationOperationError):
            return None

    # ------------------------------------------------------------------

    def provision(self, handler, endpoint) -> ProvisioningOutcome:
        """Run the whole chain, stopping cleanly wherever authority stops."""

        self.install_handler(handler)
        self._endpoint = endpoint
        announced = self.announce()
        if not announced:
            return ProvisioningOutcome(
                self.capability_id, False, "unannounced", False, False, None,
                "capability-not-announced",
            )
        try:
            state, approved_here = self.ensure_enrollment()
        except (FederationValidationError, FederationOperationError) as exc:
            return ProvisioningOutcome(
                self.capability_id, True, "unknown", False, False, None,
                getattr(exc, "code", "enrollment-failed"),
            )
        if state != "approved":
            # Correct and expected in a federation this node does not lead.
            return ProvisioningOutcome(
                self.capability_id, True, state, approved_here, False, None,
                "provider-awaiting-approval",
            )
        published = self.publish_health()
        self._worker = self.activate(endpoint)
        return ProvisioningOutcome(
            self.capability_id,
            True,
            state,
            approved_here,
            published,
            self._worker,
            "provider-active" if self._worker is not None else "activation-unavailable",
        )

    def refresh(self) -> bool:
        """Refresh authority and rebind after approval or a health revision change."""

        try:
            state, _approved_here = self.ensure_enrollment()
        except (FederationValidationError, FederationOperationError):
            return False
        if state != "approved":
            return False
        published = self.publish_health()
        if self._endpoint is not None and (self._worker is None or published):
            self._worker = self.activate(self._endpoint)
        return published


def lifecycle_worker_factory(registration, handler, inbox, *, clock):
    """Build the F7.5 cancellable worker F8.4 activation should wrap."""

    return AnalysisCancellableCapabilityWorker(
        registration,
        handler,
        inbox,
        clock=clock,
    )


__all__ = [
    "ANALYSIS_HANDLER_ID",
    "AnalysisCancellableCapabilityWorker",
    "AnalysisProviderProvisioner",
    "ProvisioningOutcome",
    "analysis_capability_id",
    "analysis_handler_descriptor",
    "dispatched_data_owner_node_id",
    "lifecycle_worker_factory",
]
