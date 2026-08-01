"""Long-lived configured F7.7 runtime for concurrent local web requests."""

from __future__ import annotations

import threading
from collections.abc import Callable

from catalog.federation.errors import FederationValidationError

from .ollama_client import chat
from .ollama_provider import OllamaLanguageModelProvider
from .runtime import AIRuntimePolicy, LanguageModelProvider, LanguageModelRuntime
from .runtime_contracts import _logical_id, _text


class ConfiguredLanguageModelRuntimeManager:
    """Retain queue/capacity state while setup or providers remain unchanged."""

    def __init__(
        self,
        *,
        session_id: str,
        policy: AIRuntimePolicy | None = None,
    ) -> None:
        self.session_id = _logical_id(session_id, "session_id")
        self.policy = policy or AIRuntimePolicy()
        if not isinstance(self.policy, AIRuntimePolicy):
            raise FederationValidationError(
                "invalid-ai-runtime-policy", "policy", "must be AIRuntimePolicy"
            )
        self._lock = threading.RLock()
        self._additional: dict[str, LanguageModelProvider] = {}
        self._configuration: tuple[str, str, str, int] | None = None
        self._runtime: LanguageModelRuntime | None = None

    def register(self, provider: LanguageModelProvider) -> None:
        capability_id = _logical_id(
            getattr(provider, "capability_id", None), "capability_id"
        )
        provider_session = _logical_id(
            getattr(provider, "session_id", None), "provider.session_id"
        )
        if provider_session != self.session_id:
            raise FederationValidationError(
                "cross-session-ai-provider",
                "provider.session_id",
                "provider must belong to the managed runtime session",
            )
        with self._lock:
            if capability_id in self._additional:
                raise FederationValidationError(
                    "duplicate-ai-provider",
                    "capability_id",
                    "provider is already registered",
                )
            self._additional[capability_id] = provider
            self._runtime = None

    def unregister(self, capability_id: str) -> bool:
        capability_id = _logical_id(capability_id, "capability_id")
        with self._lock:
            removed = self._additional.pop(capability_id, None) is not None
            if removed:
                self._runtime = None
            return removed

    def runtime_for(
        self,
        *,
        model: str,
        base_url: str,
        provider_name: str,
        chat_callable: Callable[..., str] = chat,
    ) -> LanguageModelRuntime:
        model = _text(model, "model", maximum=256)
        base_url = _text(base_url, "base_url", maximum=2048)
        provider_name = _text(provider_name, "provider_name", maximum=80)
        configuration = (model, base_url, provider_name, id(chat_callable))
        with self._lock:
            if self._runtime is not None and self._configuration == configuration:
                return self._runtime
            configured = OllamaLanguageModelProvider(
                session_id=self.session_id,
                display_name=provider_name,
                base_url=base_url,
                models=(model,),
                chat_callable=chat_callable,
            )
            providers: dict[str, LanguageModelProvider] = {
                configured.capability_id: configured
            }
            for capability_id, provider in sorted(self._additional.items()):
                if capability_id in providers:
                    raise FederationValidationError(
                        "configured-ai-provider-conflict",
                        "capability_id",
                        "configured and registered providers share one logical ID",
                    )
                providers[capability_id] = provider
            self._runtime = LanguageModelRuntime(
                session_id=self.session_id,
                providers=tuple(providers.values()),
                policy=self.policy,
            )
            self._configuration = configuration
            return self._runtime

    def additional_provider_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._additional))
