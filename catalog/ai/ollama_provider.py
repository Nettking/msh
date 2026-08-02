"""Private Ollama adapter for the F7.7 logical language-model runtime."""

from __future__ import annotations

import hashlib
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass, field

from catalog.federation.errors import FederationValidationError

from .ollama_client import OllamaError, chat
from .runtime import (
    AIProviderInvocationError,
    LANGUAGE_MODEL_PROTOCOL,
    LANGUAGE_MODEL_PROTOCOL_VERSION,
)
from .runtime_contracts import (
    AIModality,
    AIProviderFailureClass,
    AIRuntimeRequest,
    _logical_id,
    _text,
)

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def logical_provider_id(provider_name: str, *, mode: str = "ollama") -> str:
    """Create a safe stable logical ID without embedding the provider endpoint."""
    provider_name = _text(provider_name, "provider_name", maximum=80)
    mode = _logical_id(mode, "mode")
    slug = _SLUG_RE.sub("-", provider_name.casefold()).strip("-") or "provider"
    digest = hashlib.sha256(provider_name.encode("utf-8")).hexdigest()[:10]
    return _logical_id(f"{mode}-{slug[:48]}-{digest}", "capability_id")


@dataclass(frozen=True)
class OllamaLanguageModelProvider:
    """Keep the Ollama URL private while advertising logical capabilities only."""

    session_id: str
    display_name: str
    base_url: str
    models: tuple[str, ...]
    capability_id: str | None = None
    node_id: str | None = None
    max_concurrent_jobs: int = 1
    modalities: tuple[str, ...] = (AIModality.TEXT.value,)
    protocol: str = LANGUAGE_MODEL_PROTOCOL
    protocol_version: str = LANGUAGE_MODEL_PROTOCOL_VERSION
    supports_timeout: bool = True
    supports_cancellation: bool = False
    chat_callable: Callable[..., str] = field(default=chat, compare=False, repr=False)

    def __post_init__(self) -> None:
        session_id = _logical_id(self.session_id, "session_id")
        object.__setattr__(self, "session_id", session_id)
        display_name = _text(self.display_name, "display_name", maximum=80)
        object.__setattr__(self, "display_name", display_name)
        object.__setattr__(self, "base_url", _text(self.base_url, "base_url", maximum=2048))
        models = tuple(
            _text(item, f"models[{index}]", maximum=256)
            for index, item in enumerate(self.models)
        )
        if not models or len(models) != len(set(models)):
            raise FederationValidationError(
                "invalid-ollama-models",
                "models",
                "must contain unique model names",
            )
        object.__setattr__(self, "models", models)
        modalities = tuple(
            _logical_id(item, f"modalities[{index}]")
            for index, item in enumerate(self.modalities)
        )
        if not modalities or len(modalities) != len(set(modalities)):
            raise FederationValidationError(
                "invalid-ollama-modalities",
                "modalities",
                "must contain unique modalities",
            )
        object.__setattr__(self, "modalities", modalities)
        capability_id = self.capability_id or logical_provider_id(display_name)
        object.__setattr__(
            self, "capability_id", _logical_id(capability_id, "capability_id")
        )
        node_id = self.node_id or f"node-{self.capability_id}"
        object.__setattr__(self, "node_id", _logical_id(node_id, "node_id"))
        if (
            isinstance(self.max_concurrent_jobs, bool)
            or not isinstance(self.max_concurrent_jobs, int)
            or self.max_concurrent_jobs <= 0
        ):
            raise FederationValidationError(
                "invalid-provider-capacity",
                "max_concurrent_jobs",
                "must be a positive integer",
            )

    def invoke(
        self,
        request: AIRuntimeRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event: threading.Event | None,
    ) -> str:
        if request.session_id != self.session_id:
            raise AIProviderInvocationError(
                "provider-authorization-error",
                "The selected provider does not belong to the request session.",
                failure_class=AIProviderFailureClass.AUTHORIZATION,
            )
        if request.model not in self.models:
            raise AIProviderInvocationError(
                "provider-model-unavailable",
                "The selected provider does not advertise the requested model.",
                failure_class=AIProviderFailureClass.PROTOCOL,
            )
        if request.modality.value not in self.modalities:
            raise AIProviderInvocationError(
                "provider-modality-unavailable",
                "The selected provider does not advertise the requested modality.",
                failure_class=AIProviderFailureClass.PROTOCOL,
            )
        if cancellation_event is not None and cancellation_event.is_set():
            raise AIProviderInvocationError(
                "provider-request-cancelled",
                "The provider request was cancelled before execution.",
                failure_class=AIProviderFailureClass.CANCELLED,
            )
        try:
            return self.chat_callable(
                prompt=request.prompt,
                system_prompt=request.system_prompt,
                model=request.model,
                base_url=self.base_url,
                timeout_seconds=timeout_seconds,
            )
        except OllamaError as error:
            code = getattr(error, "code", "ollama-request-failed")
            mappings = {
                "ollama-timeout": (
                    "provider-timeout",
                    AIProviderFailureClass.TIMEOUT,
                    True,
                ),
                "ollama-unavailable": (
                    "provider-unavailable",
                    AIProviderFailureClass.TRANSIENT,
                    True,
                ),
                "ollama-overloaded": (
                    "provider-overloaded",
                    AIProviderFailureClass.OVERLOADED,
                    True,
                ),
                "ollama-authorization": (
                    "provider-authorization-error",
                    AIProviderFailureClass.AUTHORIZATION,
                    False,
                ),
                "ollama-protocol-error": (
                    "provider-protocol-error",
                    AIProviderFailureClass.PROTOCOL,
                    False,
                ),
                "ollama-invalid-response": (
                    "provider-invalid-response",
                    AIProviderFailureClass.INVALID_RESPONSE,
                    False,
                ),
                "ollama-model-unavailable": (
                    "provider-model-unavailable",
                    AIProviderFailureClass.PROTOCOL,
                    False,
                ),
            }
            runtime_code, failure_class, retryable = mappings.get(
                code,
                (
                    "provider-internal-error",
                    AIProviderFailureClass.INTERNAL,
                    False,
                ),
            )
            raise AIProviderInvocationError(
                runtime_code,
                str(error),
                failure_class=failure_class,
                retryable=retryable,
            ) from error
