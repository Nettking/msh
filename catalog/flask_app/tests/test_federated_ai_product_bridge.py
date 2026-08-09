from __future__ import annotations

import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from catalog.ai.remote_contracts import RemoteAIInvocationRequest, RemoteAIInvocationResponse
from catalog.ai.remote_provider import RemoteLanguageModelProvider
from catalog.ai.runtime import LanguageModelRuntime
from catalog.ai.runtime_contracts import AIModality, AIRuntimeRequest
from catalog.ai.shared_capacity import SharedCapacityLanguageModelProvider
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


class BlockingLocalProvider:
    session_id = "local-ai"
    node_id = "node-local-capacity"
    capability_id = "ollama-shared-capacity"
    display_name = "Shared capacity provider"
    protocol = "msh-language-model"
    protocol_version = "1.0"
    models = ("llama3.2:3b",)
    modalities = ("text",)
    max_concurrent_jobs = 1
    supports_timeout = True
    supports_cancellation = False

    def __init__(self) -> None:
        self.first_entered = threading.Event()
        self.release_first = threading.Event()
        self._lock = threading.Lock()
        self._invocations = 0
        self._active = 0
        self.max_seen_active = 0

    def invoke(self, request, *, timeout_seconds, cancellation_event):
        del request, timeout_seconds, cancellation_event
        with self._lock:
            self._invocations += 1
            invocation = self._invocations
            self._active += 1
            self.max_seen_active = max(self.max_seen_active, self._active)
        try:
            if invocation == 1:
                self.first_entered.set()
                assert self.release_first.wait(timeout=5)
            return f"answer-{invocation}"
        finally:
            with self._lock:
                self._active -= 1


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


def runtime_request(session_id: str, suffix: str = "product-bridge") -> AIRuntimeRequest:
    return AIRuntimeRequest(
        request_id=f"request-{suffix}",
        session_id=session_id,
        idempotency_key=f"request-{suffix}",
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


def test_local_and_federation_invocations_share_provider_capacity() -> None:
    provider = BlockingLocalProvider()
    shared = SharedCapacityLanguageModelProvider(provider)
    runtime = LanguageModelRuntime(
        session_id="local-ai",
        providers=(shared,),
    )
    hosted = FederationHostedLocalProvider(
        shared,
        session_id="session-shared-capacity",
        node_id="node-federation-capacity",
        capability_id="federation-shared-capacity",
    )
    results: dict[str, str] = {}
    errors: list[BaseException] = []

    def run_local() -> None:
        try:
            results["local"] = runtime.execute(
                runtime_request("local-ai", "shared-local")
            ).content
        except BaseException as exc:  # noqa: BLE001 - surfaced after join
            errors.append(exc)

    def run_remote() -> None:
        try:
            results["remote"] = hosted.invoke(
                runtime_request("session-shared-capacity", "shared-remote"),
                timeout_seconds=5,
                cancellation_event=threading.Event(),
            )
        except BaseException as exc:  # noqa: BLE001 - surfaced after join
            errors.append(exc)

    local_thread = threading.Thread(target=run_local)
    local_thread.start()
    assert provider.first_entered.wait(timeout=2)

    remote_thread = threading.Thread(target=run_remote)
    remote_thread.start()

    deadline = time.monotonic() + 2
    snapshot = shared.capacity_snapshot()
    while snapshot != (1, 1) and time.monotonic() < deadline:
        time.sleep(0.01)
        snapshot = shared.capacity_snapshot()

    assert snapshot == (1, 1)
    assert provider.max_seen_active == 1

    provider.release_first.set()
    local_thread.join(timeout=5)
    remote_thread.join(timeout=5)

    assert not local_thread.is_alive()
    assert not remote_thread.is_alive()
    assert errors == []
    assert results == {"local": "answer-1", "remote": "answer-2"}
    assert provider.max_seen_active == 1
    assert shared.capacity_snapshot() == (0, 0)


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
