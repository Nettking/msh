from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.ai.relay_remote import (
    RelayRemoteAIEndpoint,
    RemoteAIProviderHost,
)
from catalog.ai.remote_contracts import (
    RemoteAIInvocationOutcome,
    RemoteAIInvocationRequest,
)
from catalog.ai.remote_provider import RemoteAIHealthAuthority
from catalog.ai.runtime_contracts import AIModality, AIRuntimeRequest
from catalog.capabilities.provider_enrollment import (
    FederatedProviderEnrollmentService,
    SQLiteProviderEnrollmentStore,
)
from catalog.capabilities.provider_health import (
    FederatedProviderHealthService,
    SQLiteProviderHealthStore,
)
from catalog.capabilities.provider_reports import (
    ProviderResourceReport,
    ProviderStatus,
)
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.node.client import RelayNodeClient
from catalog.relay.service import RelayServer

NOW = datetime(2026, 8, 2, 14, tzinfo=timezone.utc)
TIMEOUT = 15.0
CAPABILITY_ID = "remote-relay-model"


class LocalLanguageModelProvider:
    def __init__(self, *, session_id: str, node_id: str) -> None:
        self.session_id = session_id
        self.node_id = node_id
        self.capability_id = CAPABILITY_ID
        self.display_name = "Relay model"
        self.protocol = "msh-language-model"
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
        return f"remote:{request.prompt}"


async def start_relay(
    database: Path,
    current: list[datetime],
) -> RelayServer:
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


def client(
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


async def enroll(relay: RelayServer, node: RelayNodeClient) -> None:
    token = relay.coordinator.create_enrollment_token(
        ttl_seconds=60,
        max_uses=1,
    )["token"]
    await node.connect(enrollment_token=token)


async def join(
    owner: RelayNodeClient,
    provider: RelayNodeClient,
    session_id: str,
) -> None:
    invitation = await owner.create_invitation(
        session_id,
        ttl_seconds=60,
        max_uses=1,
    )
    joined = await provider.join_session(str(invitation["token"]))
    assert joined["session_id"] == session_id
    await owner.request_replay(session_id)


def runtime_request(session_id: str, request_id: str) -> AIRuntimeRequest:
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


def health_report(
    *,
    session_id: str,
    provider_node_id: str,
    revision: int,
    reported_at: datetime,
) -> ProviderResourceReport:
    return ProviderResourceReport(
        capability_id=CAPABILITY_ID,
        node_id=provider_node_id,
        session_id=session_id,
        capability_type="language-model",
        protocol="msh-language-model",
        protocol_version="1.0",
        status=ProviderStatus.READY,
        report_revision=revision,
        max_concurrent_jobs=2,
        active_jobs=0,
        queue_depth=0,
        utilization_millis=0,
        attributes={
            "label": "Relay model",
            "models": ["small"],
            "modalities": ["text"],
            "features": {
                "timeout": True,
                "cancellation": True,
            },
        },
        reported_at=reported_at,
        expires_at=reported_at + timedelta(seconds=30),
    )


def test_f83_remote_provider_invokes_once_over_authenticated_relay(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        current = [NOW]
        relay: RelayServer | None = None
        owner: RelayNodeClient | None = None
        provider_node: RelayNodeClient | None = None
        endpoints: list[RelayRemoteAIEndpoint] = []
        try:
            relay = await start_relay(
                tmp_path / "relay" / "control.sqlite3",
                current,
            )
            owner = client(tmp_path / "owner", relay, "Owner", current)
            provider_node = client(
                tmp_path / "provider",
                relay,
                "Provider",
                current,
            )
            await enroll(relay, owner)
            await enroll(relay, provider_node)
            session = await owner.create_session("F8.3 relay AI")
            session_id = str(session["session_id"])
            await join(owner, provider_node, session_id)

            relay.coordinator.announce_capability(
                CapabilityAnnouncement(
                    capability_id=CAPABILITY_ID,
                    node_id=provider_node.node_id,
                    session_id=session_id,
                    type="language-model",
                    protocol="msh-language-model",
                    protocol_version="1.0",
                    status=CapabilityStatus.READY,
                    properties={
                        "label": "Relay model",
                        "models": ["small"],
                        "modalities": ["text"],
                    },
                    announced_at=current[0],
                ),
                actor_node_id=provider_node.node_id,
                request_id="announce-f83-relay-model",
            )
            enrollments = FederatedProviderEnrollmentService(
                relay.coordinator,
                SQLiteProviderEnrollmentStore(
                    tmp_path / "provider-enrollment.sqlite3"
                ),
                clock=lambda: current[0],
            )
            requested = enrollments.request(
                session_id=session_id,
                capability_id=CAPABILITY_ID,
                actor_node_id=provider_node.node_id,
                command_id="request-f83-relay-model",
            )
            enrollments.approve(
                session_id=session_id,
                capability_id=CAPABILITY_ID,
                actor_node_id=owner.node_id,
                command_id="approve-f83-relay-model",
                expected_revision=requested.revision,
            )
            health = FederatedProviderHealthService(
                enrollments,
                SQLiteProviderHealthStore(tmp_path / "provider-health.sqlite3"),
                clock=lambda: current[0],
            )
            health.publish(
                health_report(
                    session_id=session_id,
                    provider_node_id=provider_node.node_id,
                    revision=0,
                    reported_at=current[0],
                ),
                actor_node_id=provider_node.node_id,
                command_id="publish-f83-generation-one",
                provider_generation=1,
            )

            local_provider = LocalLanguageModelProvider(
                session_id=session_id,
                node_id=provider_node.node_id,
            )
            provider_authority = RemoteAIHealthAuthority(
                health,
                session_id=session_id,
                actor_node_id=provider_node.node_id,
                clock=lambda: current[0],
            )
            host = RemoteAIProviderHost(
                local_provider,
                provider_authority,
                provider_generation=1,
                clock=lambda: current[0],
            )
            owner_endpoint = RelayRemoteAIEndpoint(
                owner,
                request_timeout=TIMEOUT,
            )
            provider_endpoint = RelayRemoteAIEndpoint(
                provider_node,
                request_timeout=TIMEOUT,
            )
            provider_endpoint.register_host(host)
            endpoints.extend((owner_endpoint, provider_endpoint))
            await asyncio.gather(*(endpoint.start() for endpoint in endpoints))

            invocation = RemoteAIInvocationRequest(
                invocation_id="rai-relay-one",
                session_id=session_id,
                requester_node_id=owner.node_id,
                provider_node_id=provider_node.node_id,
                capability_id=CAPABILITY_ID,
                provider_generation=1,
                health_report_revision=0,
                request=runtime_request(session_id, "relay-one"),
                sent_at=current[0],
                expires_at=current[0] + timedelta(seconds=10),
            )
            response = await owner_endpoint.request(
                target_node_id=provider_node.node_id,
                request=invocation,
            )
            duplicate = await owner_endpoint.request(
                target_node_id=provider_node.node_id,
                request=invocation,
            )

            assert response.outcome is RemoteAIInvocationOutcome.SUCCEEDED
            assert response.content == "remote:machine state"
            assert duplicate == response
            assert local_provider.calls == 1

            current[0] += timedelta(seconds=1)
            health.publish(
                health_report(
                    session_id=session_id,
                    provider_node_id=provider_node.node_id,
                    revision=0,
                    reported_at=current[0],
                ),
                actor_node_id=provider_node.node_id,
                command_id="publish-f83-generation-two",
                provider_generation=2,
            )
            stale = RemoteAIInvocationRequest(
                invocation_id="rai-relay-stale-generation",
                session_id=session_id,
                requester_node_id=owner.node_id,
                provider_node_id=provider_node.node_id,
                capability_id=CAPABILITY_ID,
                provider_generation=1,
                health_report_revision=0,
                request=runtime_request(session_id, "relay-stale"),
                sent_at=current[0],
                expires_at=current[0] + timedelta(seconds=10),
            )
            stale_response = await owner_endpoint.request(
                target_node_id=provider_node.node_id,
                request=stale,
            )
            assert stale_response.outcome is RemoteAIInvocationOutcome.FAILED
            assert stale_response.error_code == "provider-unavailable"
            assert local_provider.calls == 1
        finally:
            if endpoints:
                await asyncio.gather(
                    *(endpoint.close() for endpoint in endpoints),
                    return_exceptions=True,
                )
            if provider_node is not None:
                await provider_node.disconnect()
            if owner is not None:
                await owner.disconnect()
            if relay is not None:
                await relay.stop()

    asyncio.run(scenario())
