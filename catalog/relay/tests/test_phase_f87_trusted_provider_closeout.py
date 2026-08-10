from __future__ import annotations

import asyncio
import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.ai.federated_runtime import TrustedFederatedLanguageModelRuntime
from catalog.ai.relay_remote import (
    BlockingRelayAIInvocationTransport,
    RelayRemoteAIEndpoint,
    RemoteAIProviderHost,
)
from catalog.ai.remote_provider import (
    RemoteAIHealthAuthority,
    TrustedRemoteLanguageModelBinder,
)
from catalog.ai.runtime_contracts import AIModality, AIRuntimeRequest
from catalog.capabilities.dispatch import (
    DispatchRequest,
    DispatchState,
    ExecutionResult,
    SQLiteDispatchInbox,
)
from catalog.capabilities.jobs import (
    ArtifactReference,
    AttemptStatus,
    CapabilityRequirement,
    JobAttempt,
    JobContract,
    JobStatus,
    RetryPolicy,
    TimeoutPolicy,
)
from catalog.capabilities.operator_surface import ProviderOperatorSurface
from catalog.capabilities.provider_enrollment import (
    FederatedProviderEnrollmentService,
    ProviderEnrollmentState,
    SQLiteProviderEnrollmentStore,
)
from catalog.capabilities.provider_health import (
    FederatedProviderHealthService,
    ProviderHealthState,
    SQLiteProviderHealthStore,
)
from catalog.capabilities.provider_reports import ProviderResourceReport, ProviderStatus
from catalog.capabilities.provider_selection import select_provider
from catalog.capabilities.relay_dispatch import RelayDispatchEndpoint
from catalog.capabilities.worker_activation import (
    ComputeHandlerActivationReference,
    ComputeWorkerActivationAuthority,
    LocalComputeHandlerDescriptor,
    LocalComputeHandlerInventory,
    TrustedComputeWorkerBinder,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.client import RelayNodeClient
from catalog.relay.service import RelayServer

NOW = datetime(2026, 8, 2, 18, tzinfo=timezone.utc)
TIMEOUT = 15.0
AI_A = "f87-ai-alpha"
AI_B = "f87-ai-beta"
COMPUTE_A = "f87-compute-alpha"
COMPUTE_B = "f87-compute-beta"


class LocalLanguageModelProvider:
    def __init__(
        self,
        *,
        capability_id: str,
        session_id: str,
        node_id: str,
        label: str,
    ) -> None:
        self.capability_id = capability_id
        self.session_id = session_id
        self.node_id = node_id
        self.display_name = label
        self.protocol = "fcp-language-model"
        self.protocol_version = "1.0"
        self.models = ("small",)
        self.modalities = ("text",)
        self.max_concurrent_jobs = 2
        self.supports_timeout = True
        self.supports_cancellation = True
        self.calls = 0

    def invoke(
        self,
        request: AIRuntimeRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event: threading.Event | None,
    ) -> str:
        del timeout_seconds
        assert cancellation_event is not None
        self.calls += 1
        return f"{self.capability_id}:{request.prompt}"


class CountingHandler:
    def __init__(self, provider_id: str) -> None:
        self.provider_id = provider_id
        self.calls = 0

    async def execute(self, job: JobContract) -> ExecutionResult:
        self.calls += 1
        return ExecutionResult(
            True,
            {"job_id": job.job_id, "provider_id": self.provider_id},
        )


async def _start_relay(database: Path, current: list[datetime]) -> RelayServer:
    relay = RelayServer(
        SessionCoordinator(database, clock=lambda: current[0]),
        host="127.0.0.1",
        port=0,
        auth_timeout_seconds=TIMEOUT,
        send_timeout_seconds=TIMEOUT,
        heartbeat_timeout_seconds=300,
        sweep_interval_seconds=300,
    )
    await relay.start()
    return relay


def _client(
    path: Path,
    relay: RelayServer,
    name: str,
    current: list[datetime],
) -> RelayNodeClient:
    return RelayNodeClient(
        state_directory=path,
        relay_url=relay.url,
        display_name=name,
        allow_insecure_local=True,
        heartbeat_interval=300,
        request_timeout=TIMEOUT,
        clock=lambda: current[0],
    )


async def _enroll(relay: RelayServer, node: RelayNodeClient) -> None:
    token = relay.coordinator.create_enrollment_token(
        ttl_seconds=60,
        max_uses=1,
    )["token"]
    await node.connect(enrollment_token=token)


async def _join(
    owner: RelayNodeClient,
    node: RelayNodeClient,
    session_id: str,
) -> None:
    invitation = await owner.create_invitation(
        session_id,
        ttl_seconds=60,
        max_uses=1,
    )
    joined = await node.join_session(str(invitation["token"]))
    assert joined["session_id"] == session_id
    await owner.request_replay(session_id)


def _descriptor(provider_id: str) -> LocalComputeHandlerDescriptor:
    return LocalComputeHandlerDescriptor(
        handler_id=f"handler-{provider_id}",
        capability_type="synthetic-compute",
        protocol="fcp-synthetic",
        protocol_version="1.0",
        attributes={"operation": "echo", "modalities": ["json"]},
    )


def _announce_and_approve(
    coordinator: SessionCoordinator,
    enrollments: FederatedProviderEnrollmentService,
    *,
    owner_node_id: str,
    provider_node_id: str,
    session_id: str,
    capability_id: str,
    capability_type: str,
    protocol: str,
    properties: dict[str, object],
    announced_at: datetime,
) -> None:
    coordinator.announce_capability(
        CapabilityAnnouncement(
            capability_id=capability_id,
            node_id=provider_node_id,
            session_id=session_id,
            type=capability_type,
            protocol=protocol,
            protocol_version="1.0",
            status=CapabilityStatus.READY,
            properties=properties,
            announced_at=announced_at,
        ),
        actor_node_id=provider_node_id,
        request_id=f"announce-{capability_id}",
    )
    requested = enrollments.request(
        session_id=session_id,
        capability_id=capability_id,
        actor_node_id=provider_node_id,
        command_id=f"request-{capability_id}",
    )
    approved = enrollments.approve(
        session_id=session_id,
        capability_id=capability_id,
        actor_node_id=owner_node_id,
        command_id=f"approve-{capability_id}",
        expected_revision=requested.revision,
    )
    assert approved.state is ProviderEnrollmentState.APPROVED


def _ai_report(
    *,
    capability_id: str,
    node_id: str,
    session_id: str,
    current: datetime,
    revision: int,
    active_jobs: int,
    max_jobs: int,
) -> ProviderResourceReport:
    return ProviderResourceReport(
        capability_id=capability_id,
        node_id=node_id,
        session_id=session_id,
        capability_type="language-model",
        protocol="fcp-language-model",
        protocol_version="1.0",
        status=ProviderStatus.READY,
        report_revision=revision,
        max_concurrent_jobs=max_jobs,
        active_jobs=active_jobs,
        queue_depth=0,
        utilization_millis=int(1000 * active_jobs / max_jobs),
        attributes={
            "label": capability_id,
            "models": ["small"],
            "modalities": ["text"],
            "features": {"timeout": True, "cancellation": True},
        },
        reported_at=current,
        expires_at=current + timedelta(seconds=30),
    )


def _compute_report(
    *,
    capability_id: str,
    node_id: str,
    session_id: str,
    descriptor: LocalComputeHandlerDescriptor,
    current: datetime,
    revision: int,
    active_jobs: int,
    max_jobs: int,
) -> ProviderResourceReport:
    return ProviderResourceReport(
        capability_id=capability_id,
        node_id=node_id,
        session_id=session_id,
        capability_type=descriptor.capability_type,
        protocol=descriptor.protocol,
        protocol_version=descriptor.protocol_version,
        status=ProviderStatus.READY,
        report_revision=revision,
        max_concurrent_jobs=max_jobs,
        active_jobs=active_jobs,
        queue_depth=0,
        utilization_millis=int(1000 * active_jobs / max_jobs),
        attributes={
            **descriptor.attributes,
            "activation": ComputeHandlerActivationReference(
                descriptor.handler_id,
                descriptor.descriptor_fingerprint,
            ).to_dict(),
        },
        reported_at=current,
        expires_at=current + timedelta(seconds=30),
    )


def _publish(
    health: FederatedProviderHealthService,
    report: ProviderResourceReport,
    *,
    generation: int,
) -> None:
    health.publish(
        report,
        actor_node_id=report.node_id,
        command_id=(
            f"health-{report.capability_id}-{generation}-{report.report_revision}"
        ),
        provider_generation=generation,
    )


def _ai_request(session_id: str, request_id: str) -> AIRuntimeRequest:
    return AIRuntimeRequest(
        request_id=request_id,
        session_id=session_id,
        idempotency_key=f"idem-{request_id}",
        model="small",
        modality=AIModality.TEXT,
        prompt="machine state",
        system_prompt="brief",
        timeout_seconds=10,
    )


def _active_job(session_id: str, job_id: str) -> JobContract:
    return JobContract(
        job_id=job_id,
        session_id=session_id,
        request_id=f"request-{job_id}",
        idempotency_key=f"{session_id}:{job_id}",
        capability=CapabilityRequirement(
            capability_type="synthetic-compute",
            protocol="fcp-synthetic",
            protocol_version="1.0",
            requirements={"operation": "echo", "modalities": ["json"]},
        ),
        inputs=(
            ArtifactReference(
                reference_id=f"input-{job_id}",
                session_id=session_id,
                schema_name="fcp.synthetic-input.v1",
                media_type="application/json",
            ),
        ),
        outputs=(
            ArtifactReference(
                reference_id=f"output-{job_id}",
                session_id=session_id,
                schema_name="fcp.synthetic-output.v1",
                media_type="application/json",
            ),
        ),
        retry_policy=RetryPolicy(2, (1,), ("worker-lost",)),
        timeout_policy=TimeoutPolicy(60, 30, 10, 30, 5),
        status=JobStatus.ACTIVE,
        attempts=(
            JobAttempt(
                f"attempt-{job_id}",
                1,
                AttemptStatus.ASSIGNED,
            ),
        ),
    )


def _dispatch_request(
    *,
    session_id: str,
    coordinator_node_id: str,
    target_node_id: str,
    provider_id: str,
    dispatch_id: str,
    job_id: str,
    current: datetime,
) -> DispatchRequest:
    job = _active_job(session_id, job_id)
    return DispatchRequest(
        dispatch_id=dispatch_id,
        session_id=session_id,
        coordinator_node_id=coordinator_node_id,
        target_node_id=target_node_id,
        provider_id=provider_id,
        job=job,
        attempt_id=job.attempts[0].attempt_id,
        attempt_number=1,
        lease_id=f"lease-{job_id}",
        lease_generation=1,
        lease_expires_at=current + timedelta(seconds=30),
        sent_at=current,
    )


def test_f87_multiple_trusted_ai_and_compute_providers_close_out_over_relay(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        current = [NOW]
        relay: RelayServer | None = None
        nodes: list[RelayNodeClient] = []
        endpoints: list[object] = []
        try:
            relay = await _start_relay(
                tmp_path / "relay" / "control.sqlite3",
                current,
            )
            owner = _client(tmp_path / "owner", relay, "Owner", current)
            ai_requester = _client(
                tmp_path / "ai-requester",
                relay,
                "AI requester",
                current,
            )
            compute_coordinator = _client(
                tmp_path / "compute-coordinator",
                relay,
                "Compute coordinator",
                current,
            )
            ai_alpha_node = _client(
                tmp_path / "ai-alpha",
                relay,
                "AI alpha",
                current,
            )
            ai_beta_node = _client(
                tmp_path / "ai-beta",
                relay,
                "AI beta",
                current,
            )
            compute_alpha_node = _client(
                tmp_path / "compute-alpha",
                relay,
                "Compute alpha",
                current,
            )
            compute_beta_node = _client(
                tmp_path / "compute-beta",
                relay,
                "Compute beta",
                current,
            )
            nodes.extend(
                (
                    owner,
                    ai_requester,
                    compute_coordinator,
                    ai_alpha_node,
                    ai_beta_node,
                    compute_alpha_node,
                    compute_beta_node,
                )
            )
            for node in nodes:
                await _enroll(relay, node)
            session = await owner.create_session(
                "F8.7 trusted provider closeout"
            )
            session_id = str(session["session_id"])
            for node in nodes[1:]:
                await _join(owner, node, session_id)

            enrollments = FederatedProviderEnrollmentService(
                relay.coordinator,
                SQLiteProviderEnrollmentStore(
                    tmp_path / "enrollment.sqlite3"
                ),
                clock=lambda: current[0],
            )
            health = FederatedProviderHealthService(
                enrollments,
                SQLiteProviderHealthStore(tmp_path / "health.sqlite3"),
                clock=lambda: current[0],
            )
            compute_alpha_descriptor = _descriptor(COMPUTE_A)
            compute_beta_descriptor = _descriptor(COMPUTE_B)
            capabilities = (
                (
                    ai_alpha_node,
                    AI_A,
                    "language-model",
                    "fcp-language-model",
                    {"models": ["small"], "modalities": ["text"]},
                ),
                (
                    ai_beta_node,
                    AI_B,
                    "language-model",
                    "fcp-language-model",
                    {"models": ["small"], "modalities": ["text"]},
                ),
                (
                    compute_alpha_node,
                    COMPUTE_A,
                    "synthetic-compute",
                    "fcp-synthetic",
                    compute_alpha_descriptor.attributes,
                ),
                (
                    compute_beta_node,
                    COMPUTE_B,
                    "synthetic-compute",
                    "fcp-synthetic",
                    compute_beta_descriptor.attributes,
                ),
            )
            for (
                node,
                capability_id,
                capability_type,
                protocol,
                properties,
            ) in capabilities:
                _announce_and_approve(
                    relay.coordinator,
                    enrollments,
                    owner_node_id=owner.node_id,
                    provider_node_id=node.node_id,
                    session_id=session_id,
                    capability_id=capability_id,
                    capability_type=capability_type,
                    protocol=protocol,
                    properties=properties,
                    announced_at=current[0],
                )

            _publish(
                health,
                _ai_report(
                    capability_id=AI_A,
                    node_id=ai_alpha_node.node_id,
                    session_id=session_id,
                    current=current[0],
                    revision=0,
                    active_jobs=1,
                    max_jobs=1,
                ),
                generation=1,
            )
            _publish(
                health,
                _ai_report(
                    capability_id=AI_B,
                    node_id=ai_beta_node.node_id,
                    session_id=session_id,
                    current=current[0],
                    revision=0,
                    active_jobs=0,
                    max_jobs=2,
                ),
                generation=1,
            )
            _publish(
                health,
                _compute_report(
                    capability_id=COMPUTE_A,
                    node_id=compute_alpha_node.node_id,
                    session_id=session_id,
                    descriptor=compute_alpha_descriptor,
                    current=current[0],
                    revision=0,
                    active_jobs=1,
                    max_jobs=1,
                ),
                generation=1,
            )
            _publish(
                health,
                _compute_report(
                    capability_id=COMPUTE_B,
                    node_id=compute_beta_node.node_id,
                    session_id=session_id,
                    descriptor=compute_beta_descriptor,
                    current=current[0],
                    revision=0,
                    active_jobs=0,
                    max_jobs=2,
                ),
                generation=1,
            )

            ai_alpha_provider = LocalLanguageModelProvider(
                capability_id=AI_A,
                session_id=session_id,
                node_id=ai_alpha_node.node_id,
                label="AI alpha",
            )
            ai_beta_provider = LocalLanguageModelProvider(
                capability_id=AI_B,
                session_id=session_id,
                node_id=ai_beta_node.node_id,
                label="AI beta",
            )
            ai_requester_endpoint = RelayRemoteAIEndpoint(
                ai_requester,
                request_timeout=TIMEOUT,
            )
            ai_alpha_endpoint = RelayRemoteAIEndpoint(
                ai_alpha_node,
                request_timeout=TIMEOUT,
            )
            ai_beta_endpoint = RelayRemoteAIEndpoint(
                ai_beta_node,
                request_timeout=TIMEOUT,
            )
            ai_alpha_endpoint.register_host(
                RemoteAIProviderHost(
                    ai_alpha_provider,
                    RemoteAIHealthAuthority(
                        health,
                        session_id=session_id,
                        actor_node_id=ai_alpha_node.node_id,
                        clock=lambda: current[0],
                    ),
                    provider_generation=1,
                    clock=lambda: current[0],
                )
            )
            ai_beta_endpoint.register_host(
                RemoteAIProviderHost(
                    ai_beta_provider,
                    RemoteAIHealthAuthority(
                        health,
                        session_id=session_id,
                        actor_node_id=ai_beta_node.node_id,
                        clock=lambda: current[0],
                    ),
                    provider_generation=1,
                    clock=lambda: current[0],
                )
            )
            endpoints.extend(
                (
                    ai_requester_endpoint,
                    ai_alpha_endpoint,
                    ai_beta_endpoint,
                )
            )
            await asyncio.gather(
                ai_requester_endpoint.start(),
                ai_alpha_endpoint.start(),
                ai_beta_endpoint.start(),
            )
            loop = asyncio.get_running_loop()
            ai_authority = RemoteAIHealthAuthority(
                health,
                session_id=session_id,
                actor_node_id=ai_requester.node_id,
                clock=lambda: current[0],
            )
            ai_binder = TrustedRemoteLanguageModelBinder(
                ai_authority,
                lambda node_id, capability_id: (
                    BlockingRelayAIInvocationTransport(
                        ai_requester_endpoint,
                        loop,
                        target_node_id=node_id,
                        capability_id=capability_id,
                    )
                ),
            )
            ai_runtime = TrustedFederatedLanguageModelRuntime(
                session_id=session_id,
                providers=ai_binder.bind_all(),
                clock=lambda: current[0],
            )
            first_ai = await asyncio.to_thread(
                ai_runtime.execute,
                _ai_request(session_id, "f87-ai-first"),
            )
            assert first_ai.provider_capability_id == AI_B
            assert first_ai.content == f"{AI_B}:machine state"
            assert ai_alpha_provider.calls == 0
            assert ai_beta_provider.calls == 1

            handlers = {
                COMPUTE_A: CountingHandler(COMPUTE_A),
                COMPUTE_B: CountingHandler(COMPUTE_B),
            }
            compute_endpoints: dict[str, RelayDispatchEndpoint] = {}
            compute_workers = {}
            for capability_id, node, descriptor in (
                (
                    COMPUTE_A,
                    compute_alpha_node,
                    compute_alpha_descriptor,
                ),
                (
                    COMPUTE_B,
                    compute_beta_node,
                    compute_beta_descriptor,
                ),
            ):
                inventory = LocalComputeHandlerInventory()
                inventory.register(descriptor, handlers[capability_id])
                binder = TrustedComputeWorkerBinder(
                    ComputeWorkerActivationAuthority(
                        health,
                        inventory,
                        session_id=session_id,
                        local_node_id=node.node_id,
                        clock=lambda: current[0],
                    ),
                    lambda provider_id, capability_id=capability_id: (
                        SQLiteDispatchInbox(
                            tmp_path
                            / f"dispatch-{capability_id}-{provider_id}.sqlite3"
                        )
                    ),
                    clock=lambda: current[0] + timedelta(seconds=1),
                )
                endpoint = RelayDispatchEndpoint(
                    node,
                    request_timeout=TIMEOUT,
                )
                compute_workers[capability_id] = binder.activate_on(
                    endpoint,
                    capability_id,
                )
                compute_endpoints[capability_id] = endpoint
                endpoints.append(endpoint)
            compute_coordinator_endpoint = RelayDispatchEndpoint(
                compute_coordinator,
                request_timeout=TIMEOUT,
            )
            endpoints.append(compute_coordinator_endpoint)
            await asyncio.gather(
                compute_coordinator_endpoint.start(),
                *(endpoint.start() for endpoint in compute_endpoints.values()),
            )

            selection = select_provider(
                _active_job(session_id, "selection-one"),
                health.fresh_reports(
                    session_id=session_id,
                    actor_node_id=compute_coordinator.node_id,
                    capability_type="synthetic-compute",
                ),
                evaluated_at=current[0],
            )
            assert selection.selected_capability_id == COMPUTE_B
            assert selection.selected_node_id == compute_beta_node.node_id
            first_dispatch = _dispatch_request(
                session_id=session_id,
                coordinator_node_id=compute_coordinator.node_id,
                target_node_id=compute_beta_node.node_id,
                provider_id=COMPUTE_B,
                dispatch_id="f87-dispatch-beta",
                job_id="f87-job-beta",
                current=current[0],
            )
            first_compute = await compute_coordinator_endpoint.request(
                target_node_id=compute_beta_node.node_id,
                request=first_dispatch,
            )
            duplicate_compute = await compute_coordinator_endpoint.request(
                target_node_id=compute_beta_node.node_id,
                request=first_dispatch,
            )
            assert first_compute.state is DispatchState.SUCCEEDED
            assert duplicate_compute == first_compute
            assert handlers[COMPUTE_A].calls == 0
            assert handlers[COMPUTE_B].calls == 1

            operator_view = ProviderOperatorSurface(
                enrollments,
                health,
                session_id=session_id,
                actor_node_id=owner.node_id,
                ai_authority=RemoteAIHealthAuthority(
                    health,
                    session_id=session_id,
                    actor_node_id=owner.node_id,
                    clock=lambda: current[0],
                ),
                clock=lambda: current[0],
            ).view()
            assert operator_view.is_owner
            assert {
                provider.capability_id for provider in operator_view.providers
            } == {AI_A, AI_B, COMPUTE_A, COMPUTE_B}
            serialized_view = json.dumps(
                operator_view.to_dict(),
                sort_keys=True,
            ).casefold()
            for forbidden in (
                "properties",
                "attributes",
                "handler_id",
                "descriptor_fingerprint",
                "report_fingerprint",
                "enrollment_id",
                "models",
                "modalities",
                "base_url",
                "credential",
                "backend_path",
            ):
                assert forbidden not in serialized_view

            current[0] += timedelta(seconds=1)
            _publish(
                health,
                _ai_report(
                    capability_id=AI_A,
                    node_id=ai_alpha_node.node_id,
                    session_id=session_id,
                    current=current[0],
                    revision=1,
                    active_jobs=0,
                    max_jobs=2,
                ),
                generation=1,
            )
            ai_beta_enrollment = enrollments.store.get(
                session_id=session_id,
                capability_id=AI_B,
            )
            assert ai_beta_enrollment is not None
            enrollments.suspend(
                session_id=session_id,
                capability_id=AI_B,
                actor_node_id=owner.node_id,
                command_id="suspend-f87-ai-beta",
                expected_revision=ai_beta_enrollment.revision,
                reason_code="operator-maintenance",
            )
            updated_ai_runtime = TrustedFederatedLanguageModelRuntime(
                session_id=session_id,
                providers=ai_binder.bind_all(),
                clock=lambda: current[0],
            )
            second_ai = await asyncio.to_thread(
                updated_ai_runtime.execute,
                _ai_request(session_id, "f87-ai-second"),
            )
            assert second_ai.provider_capability_id == AI_A
            assert ai_alpha_provider.calls == 1
            assert ai_beta_provider.calls == 1

            current[0] += timedelta(seconds=1)
            _publish(
                health,
                _compute_report(
                    capability_id=COMPUTE_A,
                    node_id=compute_alpha_node.node_id,
                    session_id=session_id,
                    descriptor=compute_alpha_descriptor,
                    current=current[0],
                    revision=1,
                    active_jobs=0,
                    max_jobs=2,
                ),
                generation=1,
            )
            compute_beta_enrollment = enrollments.store.get(
                session_id=session_id,
                capability_id=COMPUTE_B,
            )
            assert compute_beta_enrollment is not None
            enrollments.revoke(
                session_id=session_id,
                capability_id=COMPUTE_B,
                actor_node_id=owner.node_id,
                command_id="revoke-f87-compute-beta",
                expected_revision=compute_beta_enrollment.revision,
                reason_code="operator-revoked",
            )
            stale_beta = await compute_coordinator_endpoint.request(
                target_node_id=compute_beta_node.node_id,
                request=_dispatch_request(
                    session_id=session_id,
                    coordinator_node_id=compute_coordinator.node_id,
                    target_node_id=compute_beta_node.node_id,
                    provider_id=COMPUTE_B,
                    dispatch_id="f87-dispatch-beta-revoked",
                    job_id="f87-job-beta-revoked",
                    current=current[0],
                ),
            )
            assert stale_beta.state is DispatchState.FAILED
            assert (
                stale_beta.events[-1].reason_code
                == "compute-activation-not-current"
            )
            assert handlers[COMPUTE_B].calls == 1

            alpha_endpoint = compute_endpoints[COMPUTE_A]
            alpha_old_worker = compute_workers[COMPUTE_A]
            TrustedComputeWorkerBinder.deactivate_from(
                alpha_endpoint,
                alpha_old_worker,
            )
            alpha_inventory = LocalComputeHandlerInventory()
            alpha_inventory.register(
                compute_alpha_descriptor,
                handlers[COMPUTE_A],
            )
            alpha_binder = TrustedComputeWorkerBinder(
                ComputeWorkerActivationAuthority(
                    health,
                    alpha_inventory,
                    session_id=session_id,
                    local_node_id=compute_alpha_node.node_id,
                    clock=lambda: current[0],
                ),
                lambda provider_id: SQLiteDispatchInbox(
                    tmp_path / f"dispatch-alpha-current-{provider_id}.sqlite3"
                ),
                clock=lambda: current[0] + timedelta(seconds=1),
            )
            alpha_binder.activate_on(alpha_endpoint, COMPUTE_A)
            updated_selection = select_provider(
                _active_job(session_id, "selection-two"),
                health.fresh_reports(
                    session_id=session_id,
                    actor_node_id=compute_coordinator.node_id,
                    capability_type="synthetic-compute",
                ),
                evaluated_at=current[0],
            )
            assert updated_selection.selected_capability_id == COMPUTE_A
            second_compute = await compute_coordinator_endpoint.request(
                target_node_id=compute_alpha_node.node_id,
                request=_dispatch_request(
                    session_id=session_id,
                    coordinator_node_id=compute_coordinator.node_id,
                    target_node_id=compute_alpha_node.node_id,
                    provider_id=COMPUTE_A,
                    dispatch_id="f87-dispatch-alpha-current",
                    job_id="f87-job-alpha-current",
                    current=current[0],
                ),
            )
            assert second_compute.state is DispatchState.SUCCEEDED
            assert handlers[COMPUTE_A].calls == 1

            current[0] += timedelta(seconds=31)
            expired = health.observe(
                session_id=session_id,
                capability_id=AI_A,
                actor_node_id=owner.node_id,
            )
            assert expired.state is ProviderHealthState.EXPIRED
            durable_approval = enrollments.store.get(
                session_id=session_id,
                capability_id=AI_A,
            )
            assert durable_approval is not None
            assert durable_approval.state is ProviderEnrollmentState.APPROVED
        finally:
            if endpoints:
                await asyncio.gather(
                    *(endpoint.close() for endpoint in endpoints),
                    return_exceptions=True,
                )
            for node in reversed(nodes):
                await node.disconnect()
            if relay is not None:
                await relay.stop()

    asyncio.run(scenario())
