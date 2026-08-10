from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from catalog.ai.federated_runtime import TrustedFederatedLanguageModelRuntime
from catalog.ai.remote_contracts import (
    RemoteAIInvocationRequest,
    RemoteAIInvocationResponse,
)
from catalog.ai.remote_provider import (
    RemoteAIHealthAuthority,
    RemoteLanguageModelProvider,
    TrustedRemoteLanguageModelBinder,
)
from catalog.ai.runtime import AIProviderInvocationError
from catalog.ai.runtime_contracts import (
    AIModality,
    AIProviderFailureClass,
    AIRuntimeRequest,
)
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
from catalog.node.identity import IdentityStore, NodeCredentials

NOW = datetime(2026, 8, 2, 13, tzinfo=timezone.utc)
SESSION_ID = "session-f83"


def credentials(tmp_path: Path, name: str) -> NodeCredentials:
    return IdentityStore(
        tmp_path / f"identity-{name.casefold().replace(' ', '-')}",
        display_name=name,
    ).create(now=NOW)


def enroll(coordinator: SessionCoordinator, node: NodeCredentials) -> None:
    token = coordinator.create_enrollment_token(
        ttl_seconds=60,
        max_uses=1,
    )["token"]
    coordinator.enroll_node(node.identity, token=token)


def join(
    coordinator: SessionCoordinator,
    *,
    owner: NodeCredentials,
    node: NodeCredentials,
    suffix: str,
) -> None:
    invitation = coordinator.create_invitation(
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        ttl_seconds=60,
        max_uses=1,
        request_id=f"invite-{suffix}",
    )
    coordinator.join_session(
        node_id=node.identity.node_id,
        token=invitation["token"],
        request_id=f"join-{suffix}",
        expected_session_id=SESSION_ID,
    )


def environment(
    tmp_path: Path,
) -> tuple[
    SessionCoordinator,
    FederatedProviderEnrollmentService,
    FederatedProviderHealthService,
    NodeCredentials,
    NodeCredentials,
    NodeCredentials,
    list[datetime],
]:
    current = [NOW]
    coordinator = SessionCoordinator(
        tmp_path / "coordinator.sqlite3",
        clock=lambda: current[0],
    )
    owner = credentials(tmp_path, "Owner")
    first = credentials(tmp_path, "First Provider")
    second = credentials(tmp_path, "Second Provider")
    for node in (owner, first, second):
        enroll(coordinator, node)
    coordinator.create_session(
        actor_node_id=owner.identity.node_id,
        display_name="F8.3 remote AI",
        request_id="create-f83-session",
        session_id=SESSION_ID,
    )
    join(coordinator, owner=owner, node=first, suffix="first")
    join(coordinator, owner=owner, node=second, suffix="second")
    enrollments = FederatedProviderEnrollmentService(
        coordinator,
        SQLiteProviderEnrollmentStore(tmp_path / "enrollment.sqlite3"),
        clock=lambda: current[0],
    )
    health = FederatedProviderHealthService(
        enrollments,
        SQLiteProviderHealthStore(tmp_path / "health.sqlite3"),
        clock=lambda: current[0],
    )
    return coordinator, enrollments, health, owner, first, second, current


def announce_and_approve(
    coordinator: SessionCoordinator,
    enrollments: FederatedProviderEnrollmentService,
    *,
    owner: NodeCredentials,
    provider: NodeCredentials,
    capability_id: str,
) -> None:
    announcement = CapabilityAnnouncement(
        capability_id=capability_id,
        node_id=provider.identity.node_id,
        session_id=SESSION_ID,
        type="language-model",
        protocol="fcp-language-model",
        protocol_version="1.0",
        status=CapabilityStatus.READY,
        properties={
            "label": capability_id,
            "models": ["small"],
            "modalities": ["text"],
        },
        announced_at=NOW,
    )
    coordinator.announce_capability(
        announcement,
        actor_node_id=provider.identity.node_id,
        request_id=f"announce-{capability_id}",
    )
    requested = enrollments.request(
        session_id=SESSION_ID,
        capability_id=capability_id,
        actor_node_id=provider.identity.node_id,
        command_id=f"request-{capability_id}",
    )
    enrollments.approve(
        session_id=SESSION_ID,
        capability_id=capability_id,
        actor_node_id=owner.identity.node_id,
        command_id=f"approve-{capability_id}",
        expected_revision=requested.revision,
    )


def publish_health(
    health: FederatedProviderHealthService,
    *,
    provider: NodeCredentials,
    capability_id: str,
    generation: int,
    revision: int = 0,
    active_jobs: int = 0,
    queue_depth: int = 0,
    status: ProviderStatus = ProviderStatus.READY,
    reported_at: datetime = NOW,
    expires_at: datetime | None = None,
    label: str | None = None,
) -> ProviderResourceReport:
    report = ProviderResourceReport(
        capability_id=capability_id,
        node_id=provider.identity.node_id,
        session_id=SESSION_ID,
        capability_type="language-model",
        protocol="fcp-language-model",
        protocol_version="1.0",
        status=status,
        report_revision=revision,
        max_concurrent_jobs=4,
        active_jobs=active_jobs,
        queue_depth=queue_depth,
        utilization_millis=int(1000 * active_jobs / 4),
        attributes={
            "label": label or capability_id,
            "models": ["small"],
            "modalities": ["text"],
            "features": {
                "timeout": True,
                "cancellation": True,
            },
        },
        reported_at=reported_at,
        expires_at=expires_at or reported_at + timedelta(seconds=30),
    )
    health.publish(
        report,
        actor_node_id=provider.identity.node_id,
        command_id=f"health-{capability_id}-{generation}-{revision}",
        provider_generation=generation,
    )
    return report


def runtime_request(*, request_id: str = "remote-request") -> AIRuntimeRequest:
    return AIRuntimeRequest(
        request_id=request_id,
        session_id=SESSION_ID,
        idempotency_key=f"idem-{request_id}",
        model="small",
        modality=AIModality.TEXT,
        prompt="Explain the current machine state.",
        system_prompt="Answer briefly.",
        timeout_seconds=10,
    )


class RecordingTransport:
    def __init__(
        self,
        *,
        content: str = "remote answer",
        error_code: str | None = None,
        failure_class: AIProviderFailureClass = AIProviderFailureClass.TRANSIENT,
        retryable: bool = True,
    ) -> None:
        self.content = content
        self.error_code = error_code
        self.failure_class = failure_class
        self.retryable = retryable
        self.requests: list[RemoteAIInvocationRequest] = []

    def invoke(
        self,
        request: RemoteAIInvocationRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event: threading.Event | None,
    ) -> RemoteAIInvocationResponse:
        del timeout_seconds, cancellation_event
        self.requests.append(request)
        if self.error_code is None:
            return RemoteAIInvocationResponse.succeeded(
                request,
                content=self.content,
                completed_at=request.sent_at + timedelta(seconds=1),
            )
        return RemoteAIInvocationResponse.failed(
            request,
            error_code=self.error_code,
            failure_class=self.failure_class,
            retryable=self.retryable,
            completed_at=request.sent_at + timedelta(seconds=1),
        )


def bind(
    health: FederatedProviderHealthService,
    owner: NodeCredentials,
    current: list[datetime],
    capability_id: str,
    transport: RecordingTransport,
) -> RemoteLanguageModelProvider:
    authority = RemoteAIHealthAuthority(
        health,
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        clock=lambda: current[0],
    )
    binder = TrustedRemoteLanguageModelBinder(
        authority,
        lambda _node_id, _capability_id: transport,
    )
    return binder.bind(capability_id)


def test_remote_contract_roundtrip_contains_only_logical_route_fields() -> None:
    request = runtime_request()
    frame = RemoteAIInvocationRequest(
        invocation_id="rai-contract",
        session_id=SESSION_ID,
        requester_node_id="node-ABCDEFGHIJKLMNOPQRSTUVWX",
        provider_node_id="node-ZYXWVUTSRQPONMLKJIHGFEDC",
        capability_id="provider-contract",
        provider_generation=2,
        health_report_revision=7,
        request=request,
        sent_at=NOW,
        expires_at=NOW + timedelta(seconds=10),
    )
    restored = RemoteAIInvocationRequest.from_json(frame.to_json())
    response = RemoteAIInvocationResponse.succeeded(
        restored,
        content="safe answer",
        completed_at=NOW + timedelta(seconds=1),
    )

    assert restored == frame
    assert RemoteAIInvocationResponse.from_json(response.to_json()) == response
    serialized = frame.to_json() + response.to_json()
    for forbidden in (
        "base_url",
        "endpoint",
        "credential",
        "token",
        "127.0.0.1",
        "11434",
        "authority",
        "handler",
        "command",
    ):
        assert forbidden not in serialized


def test_binder_creates_existing_runtime_provider_without_private_endpoint(
    tmp_path: Path,
) -> None:
    coordinator, enrollments, health, owner, first, _second, current = (
        environment(tmp_path)
    )
    announce_and_approve(
        coordinator,
        enrollments,
        owner=owner,
        provider=first,
        capability_id="remote-first",
    )
    publish_health(
        health,
        provider=first,
        capability_id="remote-first",
        generation=1,
        label="Trusted remote model",
    )
    transport = RecordingTransport(content="bound answer")
    provider = bind(health, owner, current, "remote-first", transport)

    assert provider.capability_id == "remote-first"
    assert provider.node_id == first.identity.node_id
    assert provider.models == ("small",)
    assert provider.modalities == ("text",)
    assert provider.max_concurrent_jobs == 4
    assert provider.invoke(
        runtime_request(),
        timeout_seconds=10,
        cancellation_event=None,
    ) == "bound answer"
    sent = transport.requests[0]
    assert sent.requester_node_id == owner.identity.node_id
    assert sent.provider_node_id == first.identity.node_id
    assert sent.provider_generation == 1
    assert sent.health_report_revision == 0
    assert "base_url" not in vars(provider)
    assert "credentials" not in vars(provider)


def test_expiry_and_higher_generation_fence_old_adapter_without_revoking(
    tmp_path: Path,
) -> None:
    coordinator, enrollments, health, owner, first, _second, current = (
        environment(tmp_path)
    )
    announce_and_approve(
        coordinator,
        enrollments,
        owner=owner,
        provider=first,
        capability_id="remote-fenced",
    )
    publish_health(
        health,
        provider=first,
        capability_id="remote-fenced",
        generation=1,
        expires_at=NOW + timedelta(seconds=5),
    )
    old_transport = RecordingTransport()
    old = bind(health, owner, current, "remote-fenced", old_transport)

    current[0] = NOW + timedelta(seconds=6)
    assert old.resource_report(current[0]) is None
    with pytest.raises(AIProviderInvocationError) as expired:
        old.invoke(
            runtime_request(request_id="after-expiry"),
            timeout_seconds=5,
            cancellation_event=None,
        )
    assert expired.value.code == "provider-unavailable"
    approval = enrollments.store.get(
        session_id=SESSION_ID,
        capability_id="remote-fenced",
    )
    assert approval is not None and approval.state.value == "approved"

    publish_health(
        health,
        provider=first,
        capability_id="remote-fenced",
        generation=2,
        revision=0,
        reported_at=current[0],
        expires_at=current[0] + timedelta(seconds=30),
    )
    assert old.resource_report(current[0]) is None
    fresh_transport = RecordingTransport(content="generation two")
    fresh = bind(health, owner, current, "remote-fenced", fresh_transport)
    assert fresh.provider_generation == 2
    assert fresh.invoke(
        runtime_request(request_id="generation-two"),
        timeout_seconds=5,
        cancellation_event=None,
    ) == "generation two"


def test_federated_runtime_uses_current_remote_capacity_for_selection(
    tmp_path: Path,
) -> None:
    coordinator, enrollments, health, owner, first, second, current = (
        environment(tmp_path)
    )
    for node, capability in (
        (first, "remote-full"),
        (second, "remote-available"),
    ):
        announce_and_approve(
            coordinator,
            enrollments,
            owner=owner,
            provider=node,
            capability_id=capability,
        )
    publish_health(
        health,
        provider=first,
        capability_id="remote-full",
        generation=1,
        active_jobs=4,
        queue_depth=3,
    )
    publish_health(
        health,
        provider=second,
        capability_id="remote-available",
        generation=1,
        active_jobs=1,
    )
    transports = {
        "remote-full": RecordingTransport(content="wrong provider"),
        "remote-available": RecordingTransport(content="selected provider"),
    }
    authority = RemoteAIHealthAuthority(
        health,
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        clock=lambda: current[0],
    )
    providers = TrustedRemoteLanguageModelBinder(
        authority,
        lambda _node_id, capability_id: transports[capability_id],
    ).bind_all()
    runtime = TrustedFederatedLanguageModelRuntime(
        session_id=SESSION_ID,
        providers=providers,
        clock=lambda: current[0],
    )

    result = runtime.execute(runtime_request(request_id="capacity-selection"))

    assert result.content == "selected provider"
    assert result.provider_capability_id == "remote-available"
    assert transports["remote-full"].requests == []
    assert len(transports["remote-available"].requests) == 1
    status = {
        item["capability_id"]: item for item in runtime.provider_status()
    }
    assert status["remote-full"]["active_jobs"] == 4
    assert status["remote-full"]["queue_depth"] == 3


def test_existing_fallback_policy_applies_to_two_remote_providers(
    tmp_path: Path,
) -> None:
    coordinator, enrollments, health, owner, first, second, current = (
        environment(tmp_path)
    )
    for node, capability in (
        (first, "remote-primary"),
        (second, "remote-fallback"),
    ):
        announce_and_approve(
            coordinator,
            enrollments,
            owner=owner,
            provider=node,
            capability_id=capability,
        )
    publish_health(
        health,
        provider=first,
        capability_id="remote-primary",
        generation=1,
        active_jobs=0,
    )
    publish_health(
        health,
        provider=second,
        capability_id="remote-fallback",
        generation=1,
        active_jobs=1,
    )
    transports = {
        "remote-primary": RecordingTransport(
            error_code="provider-unavailable",
            failure_class=AIProviderFailureClass.TRANSIENT,
            retryable=True,
        ),
        "remote-fallback": RecordingTransport(content="fallback answer"),
    }
    authority = RemoteAIHealthAuthority(
        health,
        session_id=SESSION_ID,
        actor_node_id=owner.identity.node_id,
        clock=lambda: current[0],
    )
    providers = TrustedRemoteLanguageModelBinder(
        authority,
        lambda _node_id, capability_id: transports[capability_id],
    ).bind_all()
    runtime = TrustedFederatedLanguageModelRuntime(
        session_id=SESSION_ID,
        providers=providers,
        clock=lambda: current[0],
    )

    result = runtime.execute(runtime_request(request_id="remote-fallback-test"))

    assert result.content == "fallback answer"
    assert result.provider_capability_id == "remote-fallback"
    assert [attempt.capability_id for attempt in result.attempts] == [
        "remote-primary",
        "remote-fallback",
    ]
