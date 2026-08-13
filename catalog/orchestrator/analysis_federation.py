"""Bind this device to the F8 authorities its analysis capability needs.

Two deployments, one architecture:

*Federated* — the device is enrolled in a real federation. Provider authority is
read and mutated through the product's authenticated relay, while peer dispatch
and F6 artifact frames share that same connection through one inbound-reader
chain.

*Standalone* — the device never joined a federation. It still runs the whole
durable job architecture against a device-local ``SessionCoordinator`` holding a
genuine single-node session that this device leads. Enrollment, approval, health
and F8.4 activation are the real components; only the carrier differs, because
there is no relay to carry anything.

The standalone coordinator database is deliberately separate from the product's
federation coordinator so a standalone install never manufactures phantom
sessions or members in federation views.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from catalog.capabilities.analysis.artifact_carrier import (
    LocalAnalysisArtifactCarrier,
    RelayAnalysisArtifactEndpoint,
)
from catalog.capabilities.analysis.contracts import ANALYSIS_CAPABILITY_TYPE
from catalog.capabilities.analysis.gateway import AnalysisArtifactGateway
from catalog.capabilities.analysis.provisioning import lifecycle_worker_factory
from catalog.capabilities.lifecycle_worker import SQLiteLifecycleDispatchInbox
from catalog.capabilities.local_lifecycle import LocalLifecycleTransport
from catalog.capabilities.provider_enrollment import (
    FederatedProviderEnrollmentService,
    ProviderEnrollmentRecord,
    SQLiteProviderEnrollmentStore,
)
from catalog.capabilities.provider_health import (
    FederatedProviderHealthService,
    ProviderHealthObservation,
    ProviderHealthRecord,
    ProviderHealthState,
    SQLiteProviderHealthStore,
)
from catalog.capabilities.provider_reports import ProviderResourceReport
from catalog.capabilities.relay_lifecycle import RelayLifecycleEndpoint
from catalog.capabilities.worker_activation import (
    ComputeWorkerActivationAuthority,
    LocalComputeHandlerInventory,
    TrustedComputeWorkerBinder,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.node.identity import IdentityStore


def _utc(value: datetime) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
        or value.utcoffset().total_seconds() != 0
    ):
        raise FederationValidationError(
            "invalid-analysis-federation-clock",
            "clock",
            "must return a UTC timestamp",
        )
    return value.astimezone(timezone.utc)


def _runtime_request(
    runtime: Any,
    state: Any,
    message_type: str,
    *,
    payload: dict[str, Any],
    request_id: str,
) -> dict[str, Any]:
    """Call one authenticated provider-authority command on the pairing loop."""

    async def request_authority() -> dict[str, Any]:
        await runtime._ensure_connected(state)
        client = runtime._connected_client()
        return await client.request(
            message_type,
            session_id=state.binding.internal_session_id,
            payload=payload,
            request_id=request_id,
        )

    result = runtime._submit(request_authority())
    if not isinstance(result, dict):
        raise FederationOperationError(
            "analysis-provider-authority-response-invalid",
            "provider authority did not return an object",
        )
    return result


class RelayAnalysisCoordinatorFacade:
    """Only the coordinator surface the analysis provisioner actually needs."""

    def __init__(
        self,
        runtime: Any,
        state: Any,
        *,
        session_id: str,
        node_id: str,
        local_leader: bool,
    ) -> None:
        self.runtime = runtime
        self.state = state
        self.session_id = session_id
        self.node_id = node_id
        self.local_leader = bool(local_leader)

    def announce_capability(
        self,
        announcement,
        *,
        actor_node_id: str,
        request_id: str,
    ):
        if actor_node_id != self.node_id:
            raise FederationValidationError(
                "analysis-provider-actor-mismatch",
                "actor_node_id",
                "capability announcement must use the authenticated local node",
            )
        return self.runtime.announce_capability(
            self.state,
            announcement,
            request_id=request_id,
        )

    def require_session_leader(self, *, session_id: str, actor_node_id: str) -> None:
        if (
            session_id != self.session_id
            or actor_node_id != self.node_id
            or not self.local_leader
        ):
            raise FederationOperationError(
                "session-leader-required",
                "only the session leader may approve this provider",
                "actor_node_id",
            )


class RelayAnalysisEnrollmentFacade:
    """F8.1 mutations over the already authenticated product relay."""

    def __init__(self, runtime: Any, state: Any, *, node_id: str) -> None:
        self.runtime = runtime
        self.state = state
        self.node_id = node_id

    @staticmethod
    def _record(result: dict[str, Any]) -> ProviderEnrollmentRecord:
        value = result.get("enrollment")
        if not isinstance(value, dict):
            raise FederationOperationError(
                "analysis-provider-enrollment-response-invalid",
                "provider authority did not return an enrollment record",
            )
        return ProviderEnrollmentRecord.from_dict(value)

    def request(
        self,
        *,
        session_id: str,
        capability_id: str,
        actor_node_id: str,
        command_id: str,
    ) -> ProviderEnrollmentRecord:
        if actor_node_id != self.node_id:
            raise FederationValidationError(
                "analysis-provider-actor-mismatch",
                "actor_node_id",
                "enrollment request must use the authenticated local node",
            )
        return self._record(
            _runtime_request(
                self.runtime,
                self.state,
                "provider.enrollment.request",
                payload={"capability_id": capability_id},
                request_id=command_id,
            )
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
        if actor_node_id != self.node_id:
            raise FederationValidationError(
                "analysis-provider-actor-mismatch",
                "actor_node_id",
                "approval must use the authenticated local node",
            )
        return self._record(
            _runtime_request(
                self.runtime,
                self.state,
                "provider.enrollment.approve",
                payload={
                    "capability_id": capability_id,
                    "expected_revision": int(expected_revision),
                },
                request_id=command_id,
            )
        )


class CachedRelayAnalysisHealthAuthority(FederatedProviderHealthService):
    """F8.2 current records mirrored from the authenticated relay authority.

    The subclass preserves the mature F8.4 type boundary while overriding the
    storage/service surface with a read-through relay cache. No authority is
    recreated locally: approval and health publication remain relay mutations,
    and ``provider.health.current`` already filters to currently eligible reports.
    """

    def __init__(
        self,
        runtime: Any,
        state: Any,
        *,
        session_id: str,
        actor_node_id: str,
        clock: Callable[[], datetime],
    ) -> None:
        # Deliberately do not call FederatedProviderHealthService.__init__: that
        # constructor requires local F8.1/SQLite authorities, which a remote member
        # must not manufacture. This object implements the exact read/publish
        # surface F8.4 and provider selection consume.
        self.runtime = runtime
        self.state = state
        self.session_id = session_id
        self.actor_node_id = actor_node_id
        self._clock = clock
        self._lock = threading.RLock()
        self._records: dict[str, ProviderHealthRecord] = {}
        self.store = self

    def _now(self) -> datetime:
        return _utc(self._clock())

    def _replace(self, records: tuple[ProviderHealthRecord, ...]) -> None:
        current = {
            record.capability_id: record
            for record in records
            if record.session_id == self.session_id
            and record.capability_type == ANALYSIS_CAPABILITY_TYPE
        }
        with self._lock:
            self._records = current

    def _remember(self, record: ProviderHealthRecord) -> None:
        if record.session_id != self.session_id:
            raise FederationValidationError(
                "analysis-health-session-mismatch",
                "session_id",
                "health record belongs to another federation session",
            )
        with self._lock:
            self._records[record.capability_id] = record

    def refresh_current(self) -> tuple[ProviderHealthRecord, ...]:
        result = _runtime_request(
            self.runtime,
            self.state,
            "provider.health.current",
            payload={"capability_type": ANALYSIS_CAPABILITY_TYPE},
            request_id=f"analysis-health-current-{time.time_ns()}",
        )
        values = result.get("health")
        if not isinstance(values, list):
            raise FederationOperationError(
                "analysis-provider-health-response-invalid",
                "provider authority did not return a health list",
            )
        records = tuple(
            ProviderHealthRecord.from_dict(value)
            for value in values
            if isinstance(value, dict)
        )
        self._replace(records)
        return records

    def publish(
        self,
        report: ProviderResourceReport,
        *,
        actor_node_id: str,
        command_id: str,
        provider_generation: int,
    ) -> ProviderHealthRecord:
        if actor_node_id != self.actor_node_id or report.node_id != self.actor_node_id:
            raise FederationValidationError(
                "analysis-provider-actor-mismatch",
                "actor_node_id",
                "health publication must use the authenticated local node",
            )
        result = _runtime_request(
            self.runtime,
            self.state,
            "provider.health.publish",
            payload={
                "report": report.to_dict(),
                "provider_generation": int(provider_generation),
            },
            request_id=command_id,
        )
        value = result.get("health")
        if not isinstance(value, dict):
            raise FederationOperationError(
                "analysis-provider-health-response-invalid",
                "provider authority did not return the accepted health record",
            )
        record = ProviderHealthRecord.from_dict(value)
        self._remember(record)
        return record

    def get(
        self,
        *,
        session_id: str,
        capability_id: str,
    ) -> ProviderHealthRecord | None:
        if session_id != self.session_id:
            return None
        with self._lock:
            return self._records.get(capability_id)

    def fresh_reports(
        self,
        *,
        session_id: str,
        actor_node_id: str,
        capability_type: str | None = None,
    ) -> tuple[ProviderResourceReport, ...]:
        if session_id != self.session_id or actor_node_id != self.actor_node_id:
            raise FederationValidationError(
                "analysis-health-reader-mismatch",
                "actor_node_id",
                "provider reports must be read by this authenticated session member",
            )
        records = self.refresh_current()
        now = self._now()
        reports = [
            record.report
            for record in records
            if record.is_fresh_at(now)
            and (capability_type is None or record.capability_type == capability_type)
        ]
        return tuple(sorted(reports, key=lambda report: report.capability_id))

    def observe(
        self,
        *,
        session_id: str,
        capability_id: str,
        actor_node_id: str,
        provider_generation: int | None = None,
    ) -> ProviderHealthObservation:
        now = self._now()
        record = self.get(session_id=session_id, capability_id=capability_id)
        if record is None:
            return ProviderHealthObservation(
                session_id=session_id,
                capability_id=capability_id,
                state=ProviderHealthState.ABSENT,
                reason_code="no-live-report",
                evaluated_at=now,
            )
        if actor_node_id != self.actor_node_id:
            raise FederationValidationError(
                "analysis-health-reader-mismatch",
                "actor_node_id",
                "provider health must be read by the authenticated local node",
            )
        state = ProviderHealthState.CURRENT
        reason = "provider-report-current"
        if provider_generation is not None and provider_generation != record.provider_generation:
            state = ProviderHealthState.SUPERSEDED
            reason = "provider-generation-superseded"
        elif not record.is_fresh_at(now):
            state = ProviderHealthState.EXPIRED
            reason = "provider-report-expired"
        return ProviderHealthObservation(
            session_id=session_id,
            capability_id=capability_id,
            state=state,
            reason_code=reason,
            evaluated_at=now,
            enrollment_id=record.enrollment_id,
            enrollment_revision=record.enrollment_revision,
            provider_generation=record.provider_generation,
            report_revision=record.report_revision,
            expires_at=record.report.expires_at,
        )


class ThreadsafeRelayLifecycleTransport:
    """Use an endpoint hosted on the product relay loop from scheduler threads."""

    def __init__(
        self,
        endpoint: RelayLifecycleEndpoint,
        event_loop: asyncio.AbstractEventLoop,
        *,
        timeout_seconds: float = 120.0,
        close_timeout_seconds: float = 10.0,
    ) -> None:
        self.endpoint = endpoint
        self.event_loop = event_loop
        self.timeout_seconds = float(timeout_seconds)
        self.close_timeout_seconds = float(close_timeout_seconds)
        self.relay_client = endpoint.relay_client

    async def _await_remote(self, coroutine):
        future = asyncio.run_coroutine_threadsafe(coroutine, self.event_loop)
        return await asyncio.wrap_future(future)

    async def request(self, *, target_node_id: str, request):
        return await self._await_remote(
            self.endpoint.request(target_node_id=target_node_id, request=request)
        )

    async def cancel(self, *, target_node_id: str, request):
        return await self._await_remote(
            self.endpoint.cancel(target_node_id=target_node_id, request=request)
        )

    def _on_relay_loop(self, callback):
        async def invoke():
            return callback()

        return asyncio.run_coroutine_threadsafe(
            invoke(), self.event_loop
        ).result(timeout=self.timeout_seconds)

    @property
    def workers(self):
        return self.endpoint.workers

    def register_worker(self, provider_id: str, worker) -> None:
        self._on_relay_loop(lambda: self.endpoint.register_worker(provider_id, worker))

    def unregister_worker(
        self,
        provider_id: str,
        *,
        expected_worker=None,
    ) -> bool:
        return bool(
            self._on_relay_loop(
                lambda: self.endpoint.unregister_worker(
                    provider_id,
                    expected_worker=expected_worker,
                )
            )
        )

    def replace_workers(self, workers: dict[str, Any], *, replace_provider_ids: tuple[str, ...]):
        return self._on_relay_loop(
            lambda: self.endpoint.replace_workers(
                workers,
                replace_provider_ids=replace_provider_ids,
            )
        )

    def close(self) -> None:
        """Close the wrapped endpoint from a scheduler thread, bounded.

        ``AnalysisRuntime.stop`` releases a superseded transport with a plain
        ``getattr(transport, "close", None)``. Without this method that lookup
        finds nothing, and the superseded endpoint's reader task stays parked on
        the relay loop after a runtime replacement or reconnect.

        ``RelayLifecycleEndpoint.close`` is a coroutine owned by the relay loop,
        so it is scheduled there and awaited from here. Shutdown must never
        block: a loop that is already gone is nothing to close, and a close that
        does not finish inside the budget is abandoned rather than waited on.
        """

        loop = self.event_loop
        if loop.is_closed() or not loop.is_running():
            return
        try:
            future = asyncio.run_coroutine_threadsafe(self.endpoint.close(), loop)
        except RuntimeError:
            # The loop stopped between the check above and the submission.
            return
        try:
            future.result(timeout=self.close_timeout_seconds)
        except TimeoutError:
            future.cancel()


@dataclass
class DeviceFederationAuthority:
    """The F8 authorities and carriers this device analyses JSONL with."""

    coordinator: Any | None
    enrollments: Any | None
    health: Any
    inventory: LocalComputeHandlerInventory
    binder: TrustedComputeWorkerBinder | None
    data_owner_node_id: str
    capability_root: Path
    clock: Callable[[], datetime]
    relay_client: Any | None = None
    provider_generation: int = 1
    _lifecycle_transport: Any | None = None
    _artifact_factory: Callable[[AnalysisArtifactGateway], Any] | None = None

    # ------------------------------------------------------------------

    def lifecycle_transport(self):
        """Return the F7.5 carrier: the relay when federated, local otherwise."""

        if self._lifecycle_transport is None:
            if self.relay_client is not None:
                self._lifecycle_transport = RelayLifecycleEndpoint(self.relay_client)
            else:
                self._lifecycle_transport = LocalLifecycleTransport(
                    local_node_id=self.data_owner_node_id
                )
        return self._lifecycle_transport

    def artifact_carrier(self, gateway: AnalysisArtifactGateway):
        """Return the carrier for authorized F6 artifact frames."""

        if self._artifact_factory is not None:
            return self._artifact_factory(gateway)
        if self.relay_client is not None:
            transport = self.lifecycle_transport()
            return RelayAnalysisArtifactEndpoint(
                self.relay_client,
                gateway,
                clock=self.clock,
                message_source=transport,
            )
        return LocalAnalysisArtifactCarrier(
            gateway, local_node_id=self.data_owner_node_id, clock=self.clock
        )

    # ------------------------------------------------------------------

    @classmethod
    def build(
        cls,
        *,
        coordinator: SessionCoordinator,
        capability_root: Path,
        session_id: str,
        node_id: str,
        clock: Callable[[], datetime],
        relay_client: Any | None = None,
    ) -> "DeviceFederationAuthority":
        enrollments = FederatedProviderEnrollmentService(
            coordinator,
            SQLiteProviderEnrollmentStore(
                capability_root / "provider_enrollment.sqlite3"
            ),
            clock=clock,
        )
        health = FederatedProviderHealthService(
            enrollments,
            SQLiteProviderHealthStore(capability_root / "provider_health.sqlite3"),
            clock=clock,
        )
        inventory = LocalComputeHandlerInventory()
        authority = ComputeWorkerActivationAuthority(
            health,
            inventory,
            session_id=session_id,
            local_node_id=node_id,
            clock=clock,
        )
        binder = TrustedComputeWorkerBinder(
            authority,
            lambda provider_id: SQLiteLifecycleDispatchInbox(
                capability_root / f"dispatch_inbox_{provider_id}.sqlite3"
            ),
            clock=clock,
            worker_factory=lifecycle_worker_factory,
        )
        return cls(
            coordinator=coordinator,
            enrollments=enrollments,
            health=health,
            inventory=inventory,
            binder=binder,
            data_owner_node_id=node_id,
            capability_root=capability_root,
            clock=clock,
            relay_client=relay_client,
        )

    @classmethod
    def from_authenticated_relay(
        cls,
        *,
        capability_root: Path,
        session_id: str,
        node_id: str,
        clock: Callable[[], datetime],
        runtime: Any,
        runtime_state: Any,
        event_loop: asyncio.AbstractEventLoop,
        upstream_message_source: Any,
        local_leader: bool,
    ) -> "DeviceFederationAuthority":
        """Bind to the product's one authenticated relay connection."""

        runtime.ensure_connected(runtime_state)
        client = runtime._connected_client()
        if client.node_id != node_id:
            raise FederationValidationError(
                "analysis-relay-identity-mismatch",
                "node_id",
                "product relay client belongs to another device identity",
            )
        coordinator = RelayAnalysisCoordinatorFacade(
            runtime,
            runtime_state,
            session_id=session_id,
            node_id=node_id,
            local_leader=local_leader,
        )
        enrollments = RelayAnalysisEnrollmentFacade(
            runtime,
            runtime_state,
            node_id=node_id,
        )
        health = CachedRelayAnalysisHealthAuthority(
            runtime,
            runtime_state,
            session_id=session_id,
            actor_node_id=node_id,
            clock=clock,
        )
        inventory = LocalComputeHandlerInventory()
        activation = ComputeWorkerActivationAuthority(
            health,
            inventory,
            session_id=session_id,
            local_node_id=node_id,
            clock=clock,
        )
        binder = TrustedComputeWorkerBinder(
            activation,
            lambda provider_id: SQLiteLifecycleDispatchInbox(
                capability_root / f"dispatch_inbox_{provider_id}.sqlite3"
            ),
            clock=clock,
            worker_factory=lifecycle_worker_factory,
        )
        lifecycle_endpoint = RelayLifecycleEndpoint(
            client,
            request_timeout=60.0,
            message_source=upstream_message_source,
        )
        asyncio.run_coroutine_threadsafe(
            lifecycle_endpoint.start(), event_loop
        ).result(timeout=10)
        lifecycle_transport = ThreadsafeRelayLifecycleTransport(
            lifecycle_endpoint,
            event_loop,
        )

        def artifact_factory(gateway: AnalysisArtifactGateway):
            endpoint = RelayAnalysisArtifactEndpoint(
                client,
                gateway,
                clock=clock,
                request_timeout=60.0,
                message_source=lifecycle_endpoint,
            )
            asyncio.run_coroutine_threadsafe(endpoint.start(), event_loop).result(
                timeout=10
            )
            return endpoint

        return cls(
            coordinator=coordinator,
            enrollments=enrollments,
            health=health,
            inventory=inventory,
            binder=binder,
            data_owner_node_id=node_id,
            capability_root=capability_root,
            clock=clock,
            relay_client=client,
            provider_generation=max(1, int(time.time_ns() // 1_000)),
            _lifecycle_transport=lifecycle_transport,
            _artifact_factory=artifact_factory,
        )

    @classmethod
    def without_authority(
        cls,
        *,
        capability_root: Path,
        node_id: str,
        clock: Callable[[], datetime],
    ) -> "DeviceFederationAuthority":
        """Return an authority that can host nothing and offers no provider."""

        return cls(
            coordinator=None,
            enrollments=None,
            health=_EmptyHealthAuthority(),
            inventory=LocalComputeHandlerInventory(),
            binder=None,
            data_owner_node_id=node_id,
            capability_root=capability_root,
            clock=clock,
        )

    @classmethod
    def for_device(
        cls,
        *,
        capability_root: Path,
        identity,
        clock: Callable[[], datetime],
        coordinator: SessionCoordinator | None = None,
        relay_client: Any | None = None,
    ) -> "DeviceFederationAuthority":
        """Build the authority chain, bootstrapping a single-node session if needed."""

        if coordinator is None:
            coordinator = _standalone_coordinator(
                capability_root=capability_root,
                session_id=identity.session_id,
                node_id=identity.node_id,
                clock=clock,
            )
        return cls.build(
            coordinator=coordinator,
            capability_root=capability_root,
            session_id=identity.session_id,
            node_id=identity.node_id,
            clock=clock,
            relay_client=relay_client,
        )


def _standalone_coordinator(
    *,
    capability_root: Path,
    session_id: str,
    node_id: str,
    clock: Callable[[], datetime],
) -> SessionCoordinator:
    """Return a device-local coordinator holding this device's own session."""

    capability_root.mkdir(parents=True, exist_ok=True)
    coordinator = SessionCoordinator(
        capability_root / "standalone_control.sqlite3", clock=clock
    )
    try:
        coordinator.require_active_node(node_id)
    except Exception:  # noqa: BLE001 - first run enrols this device
        credentials = IdentityStore(
            capability_root / "standalone_identity",
            display_name="This device",
        ).load_or_create(now=clock())
        if credentials.identity.node_id != node_id:
            raise FederationValidationError(
                "analysis-identity-not-enrollable",
                "node_id",
                "standalone analysis requires this device's own node identity",
            )
        token = coordinator.create_enrollment_token(ttl_seconds=300, max_uses=1)["token"]
        coordinator.enroll_node(credentials.identity, token=token)
    if session_id not in coordinator.session_ids_for_node(node_id):
        coordinator.create_session(
            actor_node_id=node_id,
            display_name="Standalone analysis",
            request_id=f"create-{session_id}",
            session_id=session_id,
        )
    return coordinator


class _EmptyHealthAuthority:
    """Health authority for a device with no federation membership at all."""

    store = None

    def fresh_reports(self, **_kwargs):
        return ()


__all__ = [
    "CachedRelayAnalysisHealthAuthority",
    "DeviceFederationAuthority",
    "RelayAnalysisCoordinatorFacade",
    "RelayAnalysisEnrollmentFacade",
    "ThreadsafeRelayLifecycleTransport",
]