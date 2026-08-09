from __future__ import annotations

import threading
import time

from catalog.ai.runtime import LanguageModelRuntime
from catalog.ai.runtime_contracts import AIModality, AIRuntimeRequest
from catalog.ai.shared_capacity import SharedCapacityLanguageModelProvider
from catalog.flask_app.services.federated_ai_product_bridge import (
    FederationHostedLocalProvider,
)


LOCAL_SESSION = "local-ai"
FEDERATION_SESSION = "session-shared-capacity"
CAPABILITY_ID = "ollama-shared-capacity"
PROVIDER_NODE = "node-local-capacity"
FEDERATION_NODE = "node-federation-capacity"


class BlockingProvider:
    session_id = LOCAL_SESSION
    node_id = PROVIDER_NODE
    capability_id = CAPABILITY_ID
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


def _request(session_id: str, suffix: str) -> AIRuntimeRequest:
    return AIRuntimeRequest(
        request_id=f"request-{suffix}",
        session_id=session_id,
        idempotency_key=f"request-{suffix}",
        model="llama3.2:3b",
        modality=AIModality.TEXT,
        prompt="Explain shared capacity.",
        system_prompt="Use repository context.",
        timeout_seconds=5,
    )


def test_local_and_federation_invocations_share_one_capacity_domain() -> None:
    provider = BlockingProvider()
    shared = SharedCapacityLanguageModelProvider(provider)
    runtime = LanguageModelRuntime(
        session_id=LOCAL_SESSION,
        providers=(shared,),
    )
    hosted = FederationHostedLocalProvider(
        shared,
        session_id=FEDERATION_SESSION,
        node_id=FEDERATION_NODE,
        capability_id="federation-shared-capacity",
    )
    results: dict[str, str] = {}
    errors: list[BaseException] = []

    def run_local() -> None:
        try:
            results["local"] = runtime.execute(_request(LOCAL_SESSION, "local")).content
        except BaseException as exc:  # noqa: BLE001 - surfaced after thread join
            errors.append(exc)

    def run_remote() -> None:
        try:
            results["remote"] = hosted.invoke(
                _request(FEDERATION_SESSION, "remote"),
                timeout_seconds=5,
                cancellation_event=threading.Event(),
            )
        except BaseException as exc:  # noqa: BLE001 - surfaced after thread join
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
