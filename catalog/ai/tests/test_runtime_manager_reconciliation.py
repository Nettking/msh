from __future__ import annotations

from dataclasses import dataclass
import threading

from catalog.ai.runtime import (
    LANGUAGE_MODEL_PROTOCOL,
    LANGUAGE_MODEL_PROTOCOL_VERSION,
)
from catalog.ai.runtime_contracts import AIModality, AIRuntimeRequest
from catalog.ai.runtime_manager import ConfiguredLanguageModelRuntimeManager

SESSION_ID = "session-f86-manager"


@dataclass
class FakeProvider:
    capability_id: str
    node_id: str
    response: str
    session_id: str = SESSION_ID
    display_name: str = "F8.6 provider"
    models: tuple[str, ...] = ("small",)
    modalities: tuple[str, ...] = (AIModality.TEXT.value,)
    max_concurrent_jobs: int = 1
    supports_timeout: bool = True
    supports_cancellation: bool = True
    protocol: str = LANGUAGE_MODEL_PROTOCOL
    protocol_version: str = LANGUAGE_MODEL_PROTOCOL_VERSION

    def invoke(
        self,
        request: AIRuntimeRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event: threading.Event | None,
    ) -> str:
        del request, timeout_seconds, cancellation_event
        return self.response


def test_f86_replaces_only_reconciler_owned_ai_providers() -> None:
    manager = ConfiguredLanguageModelRuntimeManager(session_id=SESSION_ID)
    unrelated = FakeProvider("provider-unrelated", "node-unrelated", "unrelated")
    previous = FakeProvider("provider-reconciled", "node-reconciled", "previous")
    replacement = FakeProvider(
        "provider-reconciled",
        "node-reconciled",
        "replacement",
    )
    manager.register(unrelated)
    manager.register(previous)

    rolled_back = manager.replace_registered(
        (replacement,),
        replace_capability_ids=(replacement.capability_id,),
    )

    assert rolled_back == (previous,)
    assert manager.additional_providers() == (replacement, unrelated)

    manager.replace_registered(
        rolled_back,
        replace_capability_ids=(replacement.capability_id,),
    )

    assert manager.additional_providers() == (previous, unrelated)
