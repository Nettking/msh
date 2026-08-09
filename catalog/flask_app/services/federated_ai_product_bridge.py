"""Product composition for trusted remote AI contributions.

Capability-first contribution intent remains separate from F8 provider authority:
READY metadata requests enrollment but never approves itself. The Federation
owner approves providers through the existing F8.5 operator surface. Only an
approved provider may publish short-lived F8.2 health, and only current health is
bound into the local AI workbench through the existing authenticated F8.3 relay
invocation path.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import time
from contextlib import suppress
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

from flask import Flask

from catalog.ai.relay_remote import (
    BlockingRelayAIInvocationTransport,
    RelayRemoteAIEndpoint,
    RemoteAIProviderHost,
)
from catalog.ai.remote_provider import (
    RemoteAIHealthAuthority,
    RemoteLanguageModelProvider,
    RemoteLanguageModelSnapshot,
)
from catalog.ai.runtime import LANGUAGE_MODEL_CAPABILITY, LANGUAGE_MODEL_PROTOCOL
from catalog.ai.runtime_contracts import AIRuntimeRequest, _logical_id, _text
from catalog.ai.shared_capacity import SharedCapacityLanguageModelProvider
from catalog.capabilities.operator_surface import ProviderOperatorSurface
from catalog.capabilities.provider_health import ProviderHealthRecord
from catalog.capabilities.provider_reports import ProviderResourceReport, ProviderStatus
from catalog.federation.coordinator import SessionCoordinator
from catalog.federation.errors import FederationOperationError
from catalog.federation.models import CapabilityAnnouncement, CapabilityStatus
from catalog.relay.provider_service import build_provider_authorities

from .federation_pairing_service import RemotePairingState

_PROVIDER_SURFACE_CONFIG_KEY = "PROVIDER_OPERATOR_SURFACE"
_REMOTE_AI_REQUEST_TIMEOUT_SECONDS = 120.0
_HEALTH_TTL_SECONDS = 60


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _semantic_id(prefix: str, value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return prefix + hashlib.sha256(encoded).hexdigest()


def _session_owner(status: dict[str, Any], session_id: str) -> str | None:
    sessions = status.get("sessions")
    if not isinstance(sessions, list):
        return None
    for value in sessions:
        if not isinstance(value, dict) or value.get("session_id") != session_id:
            continue
        owner = value.get("created_by_node_id")
        return owner if isinstance(owner, str) and owner else None
    return None


def _announcement(
    status: dict[str, Any],
    *,
    session_id: str,
    node_id: str,
    capability_id: str,
) -> CapabilityAnnouncement | None:
    """Find this device's public announcement for one local candidate identity."""

    values = status.get("capabilities")
    if not isinstance(values, list):
        return None
    fallback: dict[str, Any] | None = None
    for value in values:
        if (
            not isinstance(value, dict)
            or value.get("session_id") != session_id
            or value.get("node_id") != node_id
        ):
            continue
        if value.get("capability_id") == capability_id:
            return CapabilityAnnouncement.from_dict(value)
        properties = value.get("properties")
        if (
            isinstance(properties, dict)
            and properties.get("kind") == "capability-first-candidate"
            and properties.get("candidate_id") == capability_id
        ):
            fallback = value
    return None if fallback is None else CapabilityAnnouncement.from_dict(fallback)


def _runtime_provider_id(node_id: str, capability_id: str) -> str:
    digest = hashlib.sha256(f"{node_id}\0{capability_id}".encode()).hexdigest()
    return f"federated-ai-{digest[:24]}"


def _runtime_node_id(node_id: str, capability_id: str) -> str:
    digest = hashlib.sha256(
        f"runtime\0{node_id}\0{capability_id}".encode()
    ).hexdigest()
    return f"node-federated-{digest[:32]}"


class CachedFederatedAIHealthAuthority:
    """Current F8.2 records received from the authenticated relay authority."""

    def __init__(self, *, session_id: str, actor_node_id: str) -> None:
        self.session_id = _logical_id(session_id, "session_id")
        self.actor_node_id = _text(actor_node_id, "actor_node_id", maximum=256)
        self._lock = threading.RLock()
        self._records: dict[str, ProviderHealthRecord] = {}

    def replace_records(self, records: tuple[ProviderHealthRecord, ...]) -> None:
        current: dict[str, ProviderHealthRecord] = {}
        for record in records:
            if record.session_id == self.session_id:
                current[record.capability_id] = record
        with self._lock:
            self._records = current

    def now(self) -> datetime:
        return _utc_now()

    def current_record(
        self,
        capability_id: str,
        *,
        provider_generation: int | None = None,
        report_revision: int | None = None,
    ) -> ProviderHealthRecord:
        capability_id = _logical_id(capability_id, "capability_id")
        with self._lock:
            record = self._records.get(capability_id)
        if record is None:
            raise FederationOperationError(
                "remote-provider-health-missing",
                "current provider health record is unavailable",
                "capability_id",
            )
        if record.session_id != self.session_id or not record.is_fresh_at(self.now()):
            raise FederationOperationError(
                "remote-provider-not-current",
                "remote provider health is not current",
                "capability_id",
            )
        if (
            provider_generation is not None
            and record.provider_generation != provider_generation
        ):
            raise FederationOperationError(
                "remote-provider-generation-mismatch",
                "remote provider generation is no longer current",
                "provider_generation",
            )
        if report_revision is not None and record.report_revision != report_revision:
            raise FederationOperationError(
                "remote-provider-report-revision-mismatch",
                "remote provider report revision is no longer current",
                "report_revision",
            )
        if (
            record.capability_type != LANGUAGE_MODEL_CAPABILITY
            or record.report.protocol != LANGUAGE_MODEL_PROTOCOL
            or record.report.status is not ProviderStatus.READY
        ):
            raise FederationOperationError(
                "remote-provider-not-current",
                "remote provider is not a ready language-model provider",
                "capability_id",
            )
        return record

    def current_snapshot(
        self,
        capability_id: str,
        *,
        provider_generation: int | None = None,
        report_revision: int | None = None,
    ) -> RemoteLanguageModelSnapshot:
        record = self.current_record(
            capability_id,
            provider_generation=provider_generation,
            report_revision=report_revision,
        )
        attributes = record.report.attributes
        models_value = attributes.get("models")
        modalities_value = attributes.get("modalities")
        if not isinstance(models_value, list) or not models_value:
            raise FederationOperationError(
                "remote-provider-models-missing",
                "current provider health does not advertise models",
                "attributes.models",
            )
        if not isinstance(modalities_value, list) or not modalities_value:
            raise FederationOperationError(
                "remote-provider-modalities-missing",
                "current provider health does not advertise modalities",
                "attributes.modalities",
            )
        models = tuple(
            _text(item, f"attributes.models[{index}]", maximum=256)
            for index, item in enumerate(models_value)
        )
        modalities = tuple(
            _logical_id(item, f"attributes.modalities[{index}]")
            for index, item in enumerate(modalities_value)
        )
        features = attributes.get("features", {})
        if not isinstance(features, dict):
            features = {}
        label = attributes.get("label", capability_id)
        display_name = _text(label, "attributes.label", maximum=80)
        return RemoteLanguageModelSnapshot(
            record=record,
            display_name=display_name,
            models=models,
            modalities=modalities,
            supports_timeout=bool(features.get("timeout", True)),
            supports_cancellation=bool(features.get("cancellation", False)),
        )


class FederationHostedLocalProvider:
    """Expose one local workbench provider under its Federation identity."""

    def __init__(
        self,
        provider: object,
        *,
        session_id: str,
        node_id: str,
        capability_id: str,
    ) -> None:
        self._provider = provider
        self.session_id = session_id
        self.node_id = node_id
        self.capability_id = capability_id
        self.display_name = str(provider.display_name)
        self.protocol = str(provider.protocol)
        self.protocol_version = str(provider.protocol_version)
        self.models = tuple(provider.models)
        self.modalities = tuple(provider.modalities)
        self.max_concurrent_jobs = int(provider.max_concurrent_jobs)
        self.supports_timeout = bool(provider.supports_timeout)
        self.supports_cancellation = bool(provider.supports_cancellation)

    def invoke(
        self,
        request: AIRuntimeRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event: threading.Event | None,
    ) -> str:
        local_request = replace(
            request,
            session_id=str(self._provider.session_id),
        )
        return self._provider.invoke(
            local_request,
            timeout_seconds=timeout_seconds,
            cancellation_event=cancellation_event,
        )


class WorkbenchRemoteProvider:
    """Adapt an F8.3 Federation provider to the local workbench session."""

    def __init__(
        self,
        provider: RemoteLanguageModelProvider,
        *,
        workbench_session: str,
    ) -> None:
        self._provider = provider
        self.session_id = workbench_session
        self.capability_id = _runtime_provider_id(
            provider.node_id, provider.capability_id
        )
        self.node_id = _runtime_node_id(provider.node_id, provider.capability_id)
        label = f"{provider.display_name} — Federation"
        self.display_name = label if len(label) <= 80 else label[:80].rstrip()
        self.protocol = provider.protocol
        self.protocol_version = provider.protocol_version

    @property
    def models(self) -> tuple[str, ...]:
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

    def invoke(
        self,
        request: AIRuntimeRequest,
        *,
        timeout_seconds: float | None,
        cancellation_event: threading.Event | None,
    ) -> str:
        federation_request = replace(request, session_id=self._provider.session_id)
        return self._provider.invoke(
            federation_request,
            timeout_seconds=timeout_seconds,
            cancellation_event=cancellation_event,
        )


class FederatedAIProductBridge:
    """Synchronize explicit F8 authority and remote AI runtime bindings."""

    def __init__(self, app: Flask, onboarding_service: object) -> None:
        self.app = app
        self.onboarding_service = onboarding_service
        self._generation = max(1, int(time.time_ns() // 1_000))
        self._report_revision = 0
        self._endpoint_lock = threading.RLock()
        self._endpoint: RelayRemoteAIEndpoint | None = None
        self._endpoint_client_id: int | None = None
        self._endpoint_loop: asyncio.AbstractEventLoop | None = None
        self._authority: CachedFederatedAIHealthAuthority | None = None
        self._remote_runtime_ids: set[str] = set()
        self._owner_session_id: str | None = None
        self._capacity_wrappers: dict[str, SharedCapacityLanguageModelProvider] = {}

    @staticmethod
    def _manager():
        from catalog.flask_app.ai_routes import AI_RUNTIME_MANAGER

        return AI_RUNTIME_MANAGER

    def _runtime_request(
        self,
        runtime: object,
        state: RemotePairingState,
        message_type: str,
        *,
        payload: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        async def request_authority() -> dict[str, Any]:
            await runtime._ensure_connected(state)
            client = runtime._connected_client()
            return await client.request(
                message_type,
                session_id=state.binding.internal_session_id,
                payload=payload,
                request_id=request_id,
            )

        result = runtime._submit(request_authority())
        if not isinstance(result, dict):
            raise FederationOperationError(
                "provider-authority-response-invalid",
                "provider authority did not return an object",
            )
        return result

    def _ensure_owner_surface(
        self,
        context: object,
        *,
        session_id: str,
        node_id: str,
        owner_node_id: str,
    ) -> None:
        if node_id != owner_node_id:
            return
        coordinator = getattr(context, "coordinator", None)
        if not isinstance(coordinator, SessionCoordinator):
            return
        if self._owner_session_id == session_id and isinstance(
            self.app.config.get(_PROVIDER_SURFACE_CONFIG_KEY),
            ProviderOperatorSurface,
        ):
            return
        enrollment, health = build_provider_authorities(coordinator)
        ai_authority = RemoteAIHealthAuthority(
            health,
            session_id=session_id,
            actor_node_id=node_id,
        )
        self.app.config[_PROVIDER_SURFACE_CONFIG_KEY] = ProviderOperatorSurface(
            enrollment,
            health,
            session_id=session_id,
            actor_node_id=node_id,
            ai_authority=ai_authority,
        )
        self._owner_session_id = session_id

    def _shared_local_provider(self, provider: object) -> object:
        capability_id = str(provider.capability_id)
        if isinstance(provider, SharedCapacityLanguageModelProvider):
            self._capacity_wrappers[capability_id] = provider
            return provider
        current = self._capacity_wrappers.get(capability_id)
        if current is not None and current.wraps(provider):
            return current
        shared = SharedCapacityLanguageModelProvider(provider)  # type: ignore[arg-type]
        self._manager().replace_registered(
            (shared,),
            replace_capability_ids=(capability_id,),
        )
        self._capacity_wrappers[capability_id] = shared
        return shared

    def _local_provider(
        self,
        announcement: CapabilityAnnouncement | None,
    ) -> object | None:
        if announcement is None:
            return None
        local_candidate_id = announcement.properties.get("candidate_id")
        if not isinstance(local_candidate_id, str) or not local_candidate_id:
            local_candidate_id = announcement.capability_id
        for provider in self._manager().additional_providers():
            if provider.capability_id in self._remote_runtime_ids:
                continue
            if provider.capability_id == local_candidate_id:
                return self._shared_local_provider(provider)
        return None

    def _request_enrollment(
        self,
        runtime: object,
        state: RemotePairingState,
        announcement: CapabilityAnnouncement,
    ) -> None:
        request_id = _semantic_id(
            "capability-provider-request-",
            announcement.to_dict(),
        )
        self._runtime_request(
            runtime,
            state,
            "provider.enrollment.request",
            payload={"capability_id": announcement.capability_id},
            request_id=request_id,
        )

    def _publish_health(
        self,
        runtime: object,
        state: RemotePairingState,
        announcement: CapabilityAnnouncement,
        provider: object,
        *,
        session_id: str,
        node_id: str,
    ) -> None:
        now = _utc_now()
        max_concurrent_jobs = int(getattr(provider, "max_concurrent_jobs", 1))
        active_jobs = 0
        queue_depth = 0
        if isinstance(provider, SharedCapacityLanguageModelProvider):
            active_jobs, queue_depth = provider.capacity_snapshot()
        utilization_millis = min(
            1000,
            int(1000 * active_jobs / max(1, max_concurrent_jobs)),
        )
        report = ProviderResourceReport(
            capability_id=announcement.capability_id,
            node_id=node_id,
            session_id=session_id,
            capability_type=LANGUAGE_MODEL_CAPABILITY,
            protocol=LANGUAGE_MODEL_PROTOCOL,
            protocol_version=announcement.protocol_version,
            status=ProviderStatus.READY,
            report_revision=self._report_revision,
            max_concurrent_jobs=max_concurrent_jobs,
            active_jobs=active_jobs,
            queue_depth=queue_depth,
            utilization_millis=utilization_millis,
            attributes={
                "label": str(provider.display_name),
                "models": list(provider.models),
                "modalities": list(provider.modalities),
                "features": {
                    "timeout": bool(getattr(provider, "supports_timeout", True)),
                    "cancellation": bool(
                        getattr(provider, "supports_cancellation", False)
                    ),
                },
            },
            reported_at=now,
            expires_at=now + timedelta(seconds=_HEALTH_TTL_SECONDS),
        )
        self._runtime_request(
            runtime,
            state,
            "provider.health.publish",
            payload={
                "report": report.to_dict(),
                "provider_generation": self._generation,
            },
            request_id=(
                f"provider-health-{self._generation}-{self._report_revision}"
            ),
        )
        self._report_revision += 1

    def _current_health(
        self,
        runtime: object,
        state: RemotePairingState,
    ) -> tuple[ProviderHealthRecord, ...]:
        result = self._runtime_request(
            runtime,
            state,
            "provider.health.current",
            payload={"capability_type": LANGUAGE_MODEL_CAPABILITY},
            request_id=f"provider-health-current-{time.time_ns()}",
        )
        values = result.get("health")
        if not isinstance(values, list):
            raise FederationOperationError(
                "provider-health-response-invalid",
                "provider authority did not return a health list",
            )
        records = []
        for value in values:
            if isinstance(value, dict):
                records.append(ProviderHealthRecord.from_dict(value))
        return tuple(records)

    def _transport_context(
        self,
        runtime: object,
        state: RemotePairingState,
    ) -> tuple[RelayRemoteAIEndpoint, asyncio.AbstractEventLoop]:
        runtime.ensure_connected(state)
        client = runtime._connected_client()
        loop = runtime._start_loop()
        client_id = id(client)
        with self._endpoint_lock:
            if self._endpoint is not None and self._endpoint_client_id == client_id:
                return self._endpoint, loop
            previous = self._endpoint
            previous_loop = self._endpoint_loop
            endpoint = RelayRemoteAIEndpoint(
                client,
                request_timeout=_REMOTE_AI_REQUEST_TIMEOUT_SECONDS,
            )
            future = asyncio.run_coroutine_threadsafe(endpoint.start(), loop)
            future.result(timeout=10)
            self._endpoint = endpoint
            self._endpoint_client_id = client_id
            self._endpoint_loop = loop
        if previous is not None and previous_loop is not None:
            with suppress(Exception):
                asyncio.run_coroutine_threadsafe(
                    previous.close(), previous_loop
                ).result(timeout=5)
        return endpoint, loop

    def _configure_host(
        self,
        endpoint: RelayRemoteAIEndpoint,
        loop: asyncio.AbstractEventLoop,
        host: RemoteAIProviderHost | None,
    ) -> None:
        async def configure() -> None:
            endpoint.hosts.clear()
            if host is not None:
                endpoint.register_host(host)
            await endpoint.start()

        asyncio.run_coroutine_threadsafe(configure(), loop).result(timeout=10)

    def _bind_remote_providers(
        self,
        records: tuple[ProviderHealthRecord, ...],
        *,
        local_node_id: str,
        endpoint: RelayRemoteAIEndpoint,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        manager = self._manager()
        authority = self._authority
        if authority is None:
            return
        replacements: list[WorkbenchRemoteProvider] = []
        new_ids: set[str] = set()
        for record in records:
            if record.node_id == local_node_id:
                continue
            transport = BlockingRelayAIInvocationTransport(
                endpoint,
                loop,
                target_node_id=record.node_id,
                capability_id=record.capability_id,
            )
            remote = RemoteLanguageModelProvider(
                authority,  # type: ignore[arg-type]
                transport,
                capability_id=record.capability_id,
                provider_generation=record.provider_generation,
            )
            adapted = WorkbenchRemoteProvider(
                remote,
                workbench_session=manager.session_id,
            )
            replacements.append(adapted)
            new_ids.add(adapted.capability_id)
        owned = self._remote_runtime_ids | new_ids
        if owned:
            manager.replace_registered(
                replacements,
                replace_capability_ids=owned,
            )
        self._remote_runtime_ids = new_ids

    def sync(self, runtime_state: RemotePairingState, context: object) -> None:
        """Perform one bounded product-composition pass for trusted remote AI."""

        binding = getattr(context, "binding", None)
        credentials = getattr(context, "credentials", None)
        identity = getattr(credentials, "identity", None)
        session_id = getattr(binding, "internal_session_id", None)
        node_id = getattr(identity, "node_id", None)
        if not isinstance(session_id, str) or not isinstance(node_id, str):
            raise FederationOperationError(
                "federated-ai-context-incomplete",
                "trusted Federation context is incomplete",
            )
        runtime = self.onboarding_service.relay_runtime
        status = runtime.coordinator_status()
        owner_node_id = _session_owner(status, session_id)
        if owner_node_id is None:
            raise FederationOperationError(
                "federated-ai-owner-missing",
                "Federation owner identity is unavailable",
            )
        self._ensure_owner_surface(
            context,
            session_id=session_id,
            node_id=node_id,
            owner_node_id=owner_node_id,
        )

        manager = self._manager()
        local_announcement = None
        for provider in manager.additional_providers():
            if provider.capability_id in self._remote_runtime_ids:
                continue
            candidate = _announcement(
                status,
                session_id=session_id,
                node_id=node_id,
                capability_id=provider.capability_id,
            )
            if (
                candidate is not None
                and candidate.type == LANGUAGE_MODEL_CAPABILITY
            ):
                local_announcement = candidate
                break
        local_provider = self._local_provider(local_announcement)

        if local_announcement is not None:
            self._request_enrollment(runtime, runtime_state, local_announcement)
            if (
                local_provider is not None
                and local_announcement.status is CapabilityStatus.READY
            ):
                try:
                    self._publish_health(
                        runtime,
                        runtime_state,
                        local_announcement,
                        local_provider,
                        session_id=session_id,
                        node_id=node_id,
                    )
                except FederationOperationError as exc:
                    # Pending/suspended provider approval is expected and must
                    # not make Federation membership unhealthy.
                    if exc.code != "provider-enrollment-ineligible":
                        raise

        records = self._current_health(runtime, runtime_state)
        if self._authority is None or self._authority.session_id != session_id:
            self._authority = CachedFederatedAIHealthAuthority(
                session_id=session_id,
                actor_node_id=node_id,
            )
        self._authority.replace_records(records)
        endpoint, loop = self._transport_context(runtime, runtime_state)

        host = None
        if local_provider is not None and local_announcement is not None:
            own_record = next(
                (
                    record
                    for record in records
                    if record.node_id == node_id
                    and record.capability_id == local_announcement.capability_id
                ),
                None,
            )
            if own_record is not None:
                hosted = FederationHostedLocalProvider(
                    local_provider,
                    session_id=session_id,
                    node_id=node_id,
                    capability_id=own_record.capability_id,
                )
                host = RemoteAIProviderHost(
                    hosted,  # type: ignore[arg-type]
                    self._authority,  # type: ignore[arg-type]
                    provider_generation=own_record.provider_generation,
                )
        self._configure_host(endpoint, loop, host)
        self._bind_remote_providers(
            records,
            local_node_id=node_id,
            endpoint=endpoint,
            loop=loop,
        )


__all__ = [
    "CachedFederatedAIHealthAuthority",
    "FederatedAIProductBridge",
    "FederationHostedLocalProvider",
    "WorkbenchRemoteProvider",
]
