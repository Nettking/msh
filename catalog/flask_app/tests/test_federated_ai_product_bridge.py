from __future__ import annotations

import tempfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.ai.remote_contracts import RemoteAIInvocationRequest, RemoteAIInvocationResponse
from catalog.ai.remote_provider import RemoteLanguageModelProvider
from catalog.ai.runtime_contracts import AIModality, AIRuntimeRequest
from catalog.capabilities.provider_enrollment import ProviderEnrollmentRecord
from catalog.capabilities.provider_health import (
    ProviderHealthRecord,
    SQLiteProviderHealthStore,
)
from catalog.capabilities.provider_reports import ProviderResourceReport, ProviderStatus
from catalog.federation.models import CapabilityStatus
from catalog.flask_app.services.federated_ai_product_bridge import (
    CachedFederatedAIHealthAuthority,
    FederationHostedLocalProvider,
    WorkbenchRemoteProvider,
)

NOW = datetime.now(timezone.utc)
SESSION_ID = "session-federated-ai"
PROVIDER_NODE = "node-Provider_0123456789abcdef"
REQUESTER_NODE = "node-Requester_0123456789abcdef"
CAPABILITY_ID = "ollama-configured"


class LocalProvider:
    session_id = "local-ai"
    node_id = "node-localruntime0123456789"
    capability_id = CAPABILITY_ID
    display_name = "AI Explainer — This computer"
    protocol = "msh-language-model"
    protocol_version = "1.0"
    models = ("llama3.2:3b",)
    modalities = ("text",)
    max_concurrent_jobs = 1
    supports_timeout = True
    supports_cancellation = False

    def __init__(self) -> None:
        self.seen_session = ""

    def invoke(self, request, *, timeout_seconds, cancellation_event):
        del timeout_seconds, cancellation_event
        self.seen_session = request.session_id
        return "local answer"


class RecordingTransport:
    def __init__(self) -> None:
        self.request: RemoteAIInvocationRequest | None = None

    def invoke(self, request, *, timeout_seconds, cancellation_event):
        del timeout_seconds, cancellation_event
        self.request = request
        return RemoteAIInvocationResponse.succeeded(
            request,
            content="remote answer",
            completed_at=NOW + timedelta(milliseconds=1),
        )


def runtime_request(session_id: str) -> AIRuntimeRequest:
    return AIRuntimeRequest(
        request_id="request-product-bridge",
        session_id=session_id,
        idempotency_key="request-product-bridge",
        model="llama3.2:3b",
        modality=AIModality.TEXT,
        prompt="Explain the flow",
        system_prompt="Use repository context.",
        timeout_seconds=30,
    )


def health_record() -> ProviderHealthRecord:
    report = ProviderResourceReport(
        capability_id=CAPABILITY_ID,
        node_id=PROVIDER_NODE,
        session_id=SESSION_ID,
        capability_type="language-model",
        protocol="msh-language-model",
        protocol_version="1.0",
        status=ProviderStatus.READY,
        report_revision=0,
        max_concurrent_jobs=1,
        active_jobs=0,
        queue_depth=0,
        utilization_millis=0,
        attributes={
            "label": "AI Explainer — Provider",
            "models": ["llama3.2:3b"],
            "modalities": ["text"],
            "features": {"timeout": True, "cancellation": False},
        },
        reported_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )
    enrollment = ProviderEnrollmentRecord(
        enrollment_id="enrollment-product-bridge",
        session_id=SESSION_ID,
        capability_id=CAPABILITY_ID,
        node_id=PROVIDER_NODE,
        capability_type="language-model",
        protocol="msh-language-model",
        protocol_version="1.0",
        protocol_major=1,
        state="approved",
        revision=1,
        announcement_status=CapabilityStatus.READY,
        announcement_announced_at=NOW,
        announcement_fingerprint="sha256:" + "a" * 64,
        requested_by_node_id=PROVIDER_NODE,
        approved_by_node_id=REQUESTER_NODE,
        reason_code=None,
        created_at=NOW,
        updated_at=NOW,
    )
    # Build the record through the actual F8.2 store so fingerprint and revision
    # fields match the production contract rather than duplicating private helpers.
    with tempfile.TemporaryDirectory() as directory:
        store = SQLiteProviderHealthStore(Path(directory) / "health.sqlite3")
        return store.publish(
            report,
            enrollment,
            actor_node_id=PROVIDER_NODE,
            command_id="health-product-bridge",
            provider_generation=1,
            now=NOW,
        )


def test_provider_side_bridge_translates_federation_request_to_local_runtime() -> None:
    local = LocalProvider()
    hosted = FederationHostedLocalProvider(
        local,
        session_id=SESSION_ID,
        node_id=PROVIDER_NODE,
        capability_id=CAPABILITY_ID,
    )

    answer = hosted.invoke(
        runtime_request(SESSION_ID),
        timeout_seconds=30,
        cancellation_event=threading.Event(),
    )

    assert answer == "local answer"
    assert local.seen_session == "local-ai"
    assert hosted.session_id == SESSION_ID
    assert hosted.node_id == PROVIDER_NODE


def test_consumer_bridge_translates_workbench_request_to_federation_session() -> None:
    record = health_record()
    authority = CachedFederatedAIHealthAuthority(
        session_id=SESSION_ID,
        actor_node_id=REQUESTER_NODE,
    )
    authority.replace_records((record,))
    transport = RecordingTransport()
    remote = RemoteLanguageModelProvider(
        authority,  # type: ignore[arg-type]
        transport,
        capability_id=CAPABILITY_ID,
        provider_generation=1,
    )
    workbench = WorkbenchRemoteProvider(remote, workbench_session="local-ai")

    answer = workbench.invoke(
        runtime_request("local-ai"),
        timeout_seconds=30,
        cancellation_event=threading.Event(),
    )

    assert answer == "remote answer"
    assert workbench.session_id == "local-ai"
    assert transport.request is not None
    assert transport.request.session_id == SESSION_ID
    assert transport.request.request.session_id == SESSION_ID
    assert transport.request.requester_node_id == REQUESTER_NODE
    assert transport.request.provider_node_id == PROVIDER_NODE
