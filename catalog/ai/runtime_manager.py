"""Long-lived configured F7.7 runtime for concurrent local web requests."""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import replace

from catalog.federation.errors import FederationValidationError

from .ollama_client import chat
from .ollama_provider import OllamaLanguageModelProvider
from .runtime import AIRuntimePolicy, LanguageModelProvider, LanguageModelRuntime
from .runtime_contracts import AIRuntimeRequest, _logical_id, _text

MAX_CONFIGURED_AI_MODELS = 128


class _ManagedOllamaLanguageModelProvider:
    """Share one provider identity and capacity counter across requested models."""

    def __init__(
        self,
        *,
        session_id: str,
        display_name: str,
        base_url: str,
        model: str,
        chat_callable: Callable[..., str],
    ) -> None:
        self._lock = threading.RLock()
        self._provider = OllamaLanguageModelProvider(
            session_id=session_id,
            display_name=display_name,
            base_url=base_url,
            models=(model,),
            chat_callable=chat_callable,
        )

    @property
    def capability_id(self) -> str:
        return self._provider.capability_id

    @property
    def node_id(self) -> str:
        return self._provider.node_id

    @property
    def session_id(self) -> str:
        return self._provider.session_id

    @property
    def display_name(self) -> str:
        return self._provider.display_name

    @property
    def protocol(self) -> str:
        return self._provider.protocol

    @property
    def protocol_version(self) -> str:
        return self._provider.protocol_version

    @property
    def models(self) -> tuple[str, ...]:
        with self._lock:
            return self._provider.models

    @property
    def modalities(self) -> tuple[str, ...]:
        return self._provider.modalities

    @property
    def max_concurrent_jobs(self) -> int:
        return self._provider.max_concurrent_jobs

    @property
    def supports_timeout(self) -> bool:
        return self._provider.supports_timeout

    @property
    def supports_cancellation(self) -> bool:
        return self._provider.supports_cancellation

    def add_model(self, model: str) -> None:
        model = _text(model, "model", maximum=256)
        with self._lock:
            if model in self._provider.models:
                return
            if len(self._provider.models) >= MAX_CONFIGURED_AI_MODELS:
                raise FederationValidationError(
                    "too-many-configured-ai-models",
                    "model",
                    f"configured provider must not exceed {MAX_CONFIGURED_AI_MODELS} models",
                )
            self._provider = replace(
                self._provider,
                models=(*self._provider.models, model),
            )

    def invoke(
        self,
        request: AIRuntimeRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event: threading.Event | None,
    ) -> str:
        with self._lock:
            provider = self._provider
        return provider.invoke(
            request,
            timeout_seconds=timeout_seconds,
            cancellation_event=cancellation_event,
        )


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
        self._configuration: tuple[str, str, int] | None = None
        self._configured_provider: _ManagedOllamaLanguageModelProvider | None = None
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
            self._configured_provider = None
            self._runtime = None

    def unregister(self, capability_id: str) -> bool:
        capability_id = _logical_id(capability_id, "capability_id")
        with self._lock:
            removed = self._additional.pop(capability_id, None) is not None
            if removed:
                self._configured_provider = None
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
        configuration = (base_url, provider_name, id(chat_callable))
        with self._lock:
            if self._runtime is not None and self._configuration == configuration:
                if self._configured_provider is None:
                    raise RuntimeError("configured AI provider is missing")
                self._configured_provider.add_model(model)
                return self._runtime
            configured = _ManagedOllamaLanguageModelProvider(
                session_id=self.session_id,
                display_name=provider_name,
                base_url=base_url,
                model=model,
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
            self._configured_provider = configured
            return self._runtime

    def additional_provider_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._additional))
